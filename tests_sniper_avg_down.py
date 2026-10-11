#!/usr/bin/env python3
"""The below-average rule (SNIPER_AVG_DOWN, the frozen spec since 2026-10-11):
after a side's first fill in a market, its next buy only below that side's
average price, so every add-on buy lowers the average.

    python tests_sniper_avg_down.py

The real Sniper, PaperBroker and Ledger (the tests_sniper_ioc harness), with
the rule ON. Prices are checked against values computed here, never with the
function under test.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import time
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402  (Market, PaperIOCBase, settings)
from accounting.ledger import Ledger  # noqa: E402


class BelowAverage(unittest.TestCase):
    def test_the_highest_tick_strictly_below(self):
        for avg, tick, want in ((0.789, 0.01, 0.78), (0.78, 0.01, 0.77), (0.7799999, 0.01, 0.77),
                                (0.5825, 0.01, 0.58), (0.051, 0.01, 0.05), (0.5, 0.001, 0.499), (0.3, 0.01, 0.29)):
            with self.subTest(avg=avg, tick=tick):
                self.assertAlmostEqual(S.below_average(avg, tick), want, places=9)


class AvgDownPaper(I.PaperIOCBase):
    """One market, the real paper broker: buys on a side after its first fill."""

    def setUp(self):
        super().setUp()
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_AVG_DOWN="1"))
        self.sn._write = self.rows.append

    def again(self, side, decision, match=None):
        self.sn.last_fire_by_side[side] = -1e18                     # the cooldown is not under test
        return self.fire(side, decision, match)

    def avg(self, side):
        rd = self.sn.rounds[self.m.window_end]
        return rd[side + "_notional"] / rd[side]

    def lots(self, token):
        return [(float(l.shares), float(l.price)) for l in self.broker.ledger.positions[token].lots]

    def test_the_first_buy_of_a_side_is_not_limited(self):
        r = self.fire("UP", [(0.60, 100)])
        self.assertEqual((r["result"], r["cap"]), ("filled", 0.65))
        self.assertNotIn("side_avg", r)

    def test_a_rebuy_at_or_above_the_average_is_skipped(self):
        self.fire("UP", [(0.60, 100)])
        r = self.again("UP", [(0.60, 100)])                         # at the average
        self.assertEqual((r["result"], r["reason"]), ("skipped", "not below this side's average"))
        self.assertAlmostEqual(r["side_avg"], 0.60)
        self.assertAlmostEqual(r["side_shares"], 10.0)
        # above it: the same skip again within seconds is counted (the journal coalesces repeats)
        self.sn.last_fire_by_side["UP"] = -1e18
        self.m.set(I.UP, [(0.65, 100)])
        asyncio.run(self.sn._fire("UP", 1.0, time.monotonic() * 1000.0))
        self.assertEqual(self.sn.n["skip: not below this side's average"], 2)
        self.assertAlmostEqual(self.held(I.UP), 10.0, places=6)     # nothing more was bought
        self.assertEqual(self.sn.n["orders"], 1)

    def test_a_rebuy_below_the_average_is_capped_under_it(self):
        self.fire("UP", [(0.60, 100)])                              # 10 @ 0.60
        r = self.again("UP", [(0.55, 5), (0.58, 100), (0.62, 100)])
        self.assertEqual(r["result"], "filled")
        self.assertAlmostEqual(r["cap"], 0.59)                      # min(0.55 + 0.05, the tick under 0.60)
        self.assertTrue(all(px <= 0.59 + 1e-9 for _sh, px in self.lots(I.UP)[1:]), self.lots(I.UP))
        # 10 @ 0.60, then 5 @ 0.55 + 5 @ 0.58: the average falls to 11.65 / 20
        self.assertAlmostEqual(self.held(I.UP), 20.0, places=6)
        self.assertAlmostEqual(self.avg("UP"), (6.0 + 2.75 + 2.90) / 20.0, places=9)

    def test_the_average_only_ever_falls(self):
        self.fire("UP", [(0.70, 100)])
        seen = [self.avg("UP")]
        for ask in (0.66, 0.66, 0.61, 0.64, 0.55):
            r = self.again("UP", [(ask, 100)])
            if r["result"] == "filled":
                self.assertLess(self.avg("UP"), seen[-1])          # every add-on lowers it
                seen.append(self.avg("UP"))
            else:
                self.assertEqual(r["reason"], "not below this side's average")
        # averages 0.70 -> 0.68 -> 0.6733 -> 0.6575 -> 0.654 -> 0.6367: each ask is under the
        # tick below the average then (0.69, 0.67, 0.67, 0.65, 0.65), so all six fill
        self.assertEqual(len(seen), 6, seen)
        self.assertAlmostEqual(seen[-1], (7.0 + 6.6 + 6.6 + 6.1 + 6.4 + 5.5) / 60.0, places=9)
        self.assertTrue(all(b < a for a, b in zip(seen, seen[1:])), seen)

    def test_the_other_sides_first_buy_is_free(self):
        self.fire("UP", [(0.60, 100)])
        r = self.fire("DOWN", [(0.45, 100)])
        self.assertEqual((r["result"], r["cap"]), ("filled", 0.50))
        self.assertNotIn("side_avg", r)
        r = self.again("DOWN", [(0.45, 100)])                       # DOWN's own average now applies
        self.assertEqual(r["reason"], "not below this side's average")

    def test_an_unknown_average_never_guesses(self):
        rd = self.sn.rounds[self.m.window_end]
        rd["UP"] = 10.0                                             # shares held, but no price recorded
        r = self.fire("UP", [(0.40, 100)])
        self.assertEqual((r["result"], r["reason"]), ("skipped", "side average unknown"))

    def test_a_restart_keeps_the_average_from_the_ledger(self):
        self.fire("UP", [(0.60, 100)])
        led2 = Ledger(path=self.broker.ledger.path)
        self.assertTrue(led2.load())
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_AVG_DOWN="1"))
        self.sn._write = self.rows.append
        self.assertEqual(self.sn.seed_rounds(led2), 1)
        r = self.fire("UP", [(0.60, 100)])
        self.assertEqual((r["result"], r["reason"]), ("skipped", "not below this side's average"))
        self.assertAlmostEqual(r["side_avg"], 0.60)
        r = self.again("UP", [(0.55, 100)])
        self.assertEqual((r["result"], r["cap"]), ("filled", 0.59))

    def test_switched_off_the_rebuy_goes_out(self):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_AVG_DOWN="0"))
        self.sn._write = self.rows.append
        self.fire("UP", [(0.60, 100)])
        r = self.again("UP", [(0.60, 100)])
        self.assertEqual((r["result"], r["cap"]), ("filled", 0.65))


class AvgDownLive(unittest.TestCase):
    """The live path's own fills feed the average (FakeLive: no venue)."""

    def test_a_live_fill_sets_the_average_the_next_decision_checks(self):
        import time
        import types
        m = I.Market(time.time() + 120)
        shim = types.SimpleNamespace(min_buy_price=0.05, max_buy_price=0.95, trade_window_seconds=300.0,
                                     min_seconds_to_expiry=10.0, own_fill_ttl_s=0,
                                     _net_of_own_fills=lambda b: b, has_recent_own_fill=lambda t: False)
        live = I.FakeLive()
        sn = S.Sniper(m, shim, settings=I.settings(SNIPER_AVG_DOWN="1"), live=live)
        rows = []
        sn._write = rows.append
        live.reply = {"kind": "MATCHED", "order_id": "o1", "fill": I.live_fill(10.0)}
        import asyncio
        asyncio.run(sn._fire("UP", 1.0, time.monotonic() * 1000.0))
        rd = sn.rounds[m.window_end]
        self.assertGreater(rd["UP"], 0)
        avg = rd["UP_notional"] / rd["UP"]
        self.assertGreater(avg, 0)
        sn.last_fire_by_side["UP"] = -1e18
        m.set(m.up_token, [(round(avg, 2) + 0.01, 100)])            # an ask above the live average
        asyncio.run(sn._fire("UP", 1.0, time.monotonic() * 1000.0))
        self.assertEqual(rows[-1].get("reason"), "not below this side's average", rows[-1])


class AvgDownSettings(unittest.TestCase):
    def env(self, **kw):
        base = {k: v for k, v in os.environ.items() if not k.startswith("SNIPER_")}
        return patch.dict(os.environ, {**base, **kw}, clear=True)

    def test_on_by_default_and_the_frozen_spec(self):
        with self.env(SNIPER_ONLY="1"):
            s = S.Settings()
        self.assertTrue(s.avg_down)
        self.assertEqual(dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)["avg_down"], True)
        self.assertIn("it buys only below its average", s.lines()[0])
        self.assertIn("it buys only below its average", s.lines(live=True)[0])
        with self.env(SNIPER_ONLY="1"):
            self.assertFalse(any("add-on" in v for v in X.spec_violations(S.Settings(), 25)))

    def test_live_refuses_it_switched_off_and_names_it(self):
        with self.env(SNIPER_ONLY="1", SNIPER_AVG_DOWN="0"):
            bad = X.spec_violations(S.Settings(), 25)
        self.assertTrue(any("add-on buys" in v and "SNIPER_AVG_DOWN" in v for v in bad), bad)

    def test_a_bad_value_stops_the_start(self):
        for raw in ("maybe", "2", "yes please"):
            with self.subTest(raw), self.env(SNIPER_AVG_DOWN=raw), self.assertRaisesRegex(ValueError, "SNIPER_AVG_DOWN"):
                S.Settings()
        for raw, want in (("1", True), ("on", True), ("0", False), ("off", False), ("", True)):
            with self.subTest(raw), self.env(SNIPER_AVG_DOWN=raw):
                self.assertEqual(S.Settings().avg_down, want)


if __name__ == "__main__":
    unittest.main(verbosity=2)
