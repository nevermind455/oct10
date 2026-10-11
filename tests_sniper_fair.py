#!/usr/bin/env python3
"""The fair-value gate (SNIPER_FAIR, the frozen spec since 2026-10-11): every
buy only at or under what the share is worth now, less the taker fee and a
margin; the limit is capped there too.

    python tests_sniper_fair.py

Expected values are computed here from the formulas themselves (math.erf,
brute force over the tick grid), never with the functions under test. The
order path runs on the real Sniper, PaperBroker and Ledger (tests_sniper_ioc).
"""
from __future__ import annotations

import json
import math
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

RATE, MARGIN, TICK = 0.07, 0.01, 0.01


def phi(z):                                   # the oracle's own normal CDF
    return 0.5 * math.erfc(-z / math.sqrt(2.0))


def oracle_limit(fair, margin=MARGIN, rate=RATE):
    """Brute force: the highest cent p with p + rate p (1-p) + margin <= fair."""
    best = 0.0
    for c in range(1, 100):
        p = c / 100.0
        if p + rate * p * (1 - p) + margin <= fair + 1e-12:
            best = p
    return best


def oracle_fair_up(s, s0, sigma, tau):
    return phi(math.log(s / s0) / (sigma * math.sqrt(tau)))


# =========================================================================== the math
class Math(unittest.TestCase):
    def test_normal_cdf(self):
        for z, want in ((0.0, 0.5), (1.0, 0.8413447460685429), (-1.96, 0.024997895148220435), (3.0, 0.9986501019683699)):
            self.assertAlmostEqual(S.norm_cdf(z), want, places=12)

    def test_the_venue_fee_curve(self):
        self.assertAlmostEqual(S.taker_fee(0.5, RATE), 0.0175)
        self.assertAlmostEqual(S.taker_fee(0.9, RATE), 0.0063)

    def test_the_limit_is_the_highest_cent_that_keeps_fee_and_margin(self):
        fair = 0.02
        while fair < 1.0:
            with self.subTest(fair=fair):
                self.assertAlmostEqual(S.fair_limit(fair, MARGIN, TICK, RATE), oracle_limit(fair), places=9)
            fair = round(fair + 0.0037, 6)
        self.assertAlmostEqual(S.fair_limit(0.62, MARGIN, TICK, RATE), 0.59)            # 0.59 + 1.69c + 1c <= 0.62
        self.assertEqual(S.fair_limit(0.015, MARGIN, TICK, RATE), 0.0)                  # nothing is cheap enough


# =========================================================================== the model
class Model(unittest.TestCase):
    def test_the_start_price_is_the_one_standing_at_the_rounds_first_instant(self):
        f = S.FairValue()
        rs = 1_791_729_600
        f.observe(rs - 2.0, 0.0, 60_000.0)
        f.observe(rs - 0.4, 0.5, 60_010.0)                     # the last price before the boundary
        f.observe(rs + 0.7, 1.0, 60_020.0)                     # the round's first tick
        self.assertEqual(f.starts[rs], 60_010.0)
        g = S.FairValue()
        g.observe(rs + 3.0, 0.0, 60_050.0)                     # no earlier price: its first tick, within 5s
        self.assertEqual(g.starts[rs], 60_050.0)
        h = S.FairValue()
        h.observe(rs + 90.0, 0.0, 60_050.0)                    # joined mid-round: unknown, never guessed
        self.assertIsNone(h.starts[rs])
        k = S.FairValue()
        k.observe(rs - 45.0, 0.0, 60_000.0)                    # a price 45s old at the boundary is no start
        k.observe(rs + 20.0, 1.0, 60_005.0)
        self.assertIsNone(k.starts[rs])
        for i in range(8):                                     # only the last four rounds are kept
            k.observe(rs + 300 * (i + 1) + 1, 2.0 + i, 60_000.0)
        self.assertEqual(len(k.starts), 4)

    def test_volatility_per_sqrt_second_from_one_sample_a_second(self):
        f = S.FairValue(min_samples=60)
        v = 0.8e-4                                             # 0.8bp steps
        mono, x = 0.0, math.log(60_000.0)
        for i in range(200):
            for j in range(4):                                 # ticks every 0.25s: one sample a second counts
                f.observe(1_791_729_700.0 + mono, mono, math.exp(x))
                mono += 0.25
            x += v if i % 2 else -v
        self.assertAlmostEqual(f.vol(), v, delta=v * 0.02)
        g = S.FairValue(min_samples=60)
        for i in range(30):
            g.observe(1_791_729_700.0 + i, float(i), 60_000.0)
        self.assertIsNone(g.vol())                             # under a minute of samples
        for i in range(30, 120):
            g.observe(1_791_729_700.0 + i, float(i), 60_000.0)
        self.assertAlmostEqual(g.vol(), S.FAIR_MIN_VOL_BP / 1e4)   # flat: the floor, never zero

    def test_the_window_forgets_old_volatility(self):
        f = S.FairValue(vol_window_s=60, min_samples=30)
        for i in range(120):                                   # a wild first minute
            f.observe(1_791_729_700.0 + i, float(i), 60_000.0 * (1.001 if i % 2 else 1.0))
        for i in range(120, 200):                              # then a calm one
            f.observe(1_791_729_700.0 + i, float(i), 60_000.0 * (1.0001 if i % 2 else 1.0))
        self.assertAlmostEqual(f.vol(), math.log(1.0001), delta=1e-6)

    def test_fair_up_against_the_formula(self):
        f = S.FairValue(min_samples=10)
        rs, v = 1_791_729_600, 1e-4
        f.observe(rs - 1.0, 0.0, 60_000.0)
        for i in range(1, 40):
            f.observe(rs + i, float(i), 60_000.0 * math.exp(v if i % 2 else 0.0))
        sigma = f.vol()
        self.assertAlmostEqual(sigma, v, delta=1e-9)
        p, info = f.fair_up(60_030.0, rs + 180.0, rs + 300)
        self.assertAlmostEqual(p, oracle_fair_up(60_030.0, 60_000.0, v, 120.0), places=12)
        self.assertEqual((info["start_px"], round(info["tau_s"], 6)), (60_000.0, 120.0))
        self.assertAlmostEqual(f.fair_up(60_000.0, rs + 180.0, rs + 300)[0], 0.5)   # at the start price: 50/50
        self.assertLess(f.fair_up(59_970.0, rs + 180.0, rs + 300)[0], 0.5)
        self.assertEqual(f.fair_up(60_030.0, rs + 300.0, rs + 300), (None, "round over"))
        self.assertEqual(f.fair_up(60_030.0, rs + 400.0, rs + 600), (None, "no start price"))
        g = S.FairValue()
        g.observe(rs - 1.0, 0.0, 60_000.0)
        g.observe(rs + 0.5, 0.5, 60_000.0)                      # the round has begun: its start is set
        self.assertEqual(g.fair_up(60_030.0, rs + 10.0, rs + 300), (None, "volatility warming up"))


# =========================================================================== the order path
class Gate(I.PaperIOCBase):
    """The real Sniper and paper broker with SNIPER_FAIR=1; the fair model fed
    a known history: start price S0, steps of exactly v, then the price now."""

    V = 1e-4                                                    # 1bp per sqrt(second)

    def setUp(self):
        super().setUp()
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_FAIR="1"))
        self.sn._write = self.rows.append

    def prime(self, s0, now_mid, warm=70):
        rs = int(self.m.window_end) - S.ROUND_S
        f = self.sn.fair_model
        f.observe(rs - 0.5, 0.0, s0)
        for i in range(1, warm + 1):
            f.observe(rs + 1.0, float(i), s0 * math.exp(self.V if i % 2 else 0.0))
        f.observe(rs + 1.0, warm + 0.5, now_mid)                # under 1s later: the price now, no new sample
        return s0

    def expect(self, row, side, s0, s):
        """The fair value and limit the row must carry, from its own recorded tau."""
        p_up = oracle_fair_up(s, s0, self.V, row["tau_s"])
        fair = p_up if side == "UP" else 1 - p_up
        self.assertAlmostEqual(row["fair"], fair, places=3)
        # the journalled fair is rounded to 4 places: the limit is the oracle's on either side of it
        self.assertTrue(oracle_limit(row["fair"] - 2e-4) - 1e-9 <= row["fair_limit"] <= oracle_limit(row["fair"] + 2e-4) + 1e-9,
                        (row["fair"], row["fair_limit"]))
        self.assertEqual(row["start_px"], s0)
        return row["fair_limit"]

    def test_an_ask_under_the_fair_limit_buys_with_the_limit_capped_there(self):
        s0 = self.prime(60_000.0, 60_090.0)                     # +15bp: UP is the favourite
        r = self.fire("UP", [(0.55, 100)])
        limit = self.expect(r, "UP", s0, 60_090.0)
        self.assertEqual(r["result"], "filled")
        self.assertAlmostEqual(r["cap"], min(0.60, limit))
        self.assertLessEqual(r["price"], limit + 1e-9)

    def test_an_ask_over_the_fair_limit_is_skipped(self):
        s0 = self.prime(60_000.0, 59_990.0)                     # a little under the start: UP < 0.5
        r = self.fire("UP", [(0.60, 100)])
        self.expect(r, "UP", s0, 59_990.0)
        self.assertEqual((r["result"], r["reason"]), ("skipped", "ask not below fair value"))
        self.assertEqual(self.held(I.UP), 0.0)

    def test_down_is_worth_one_minus_up(self):
        s0 = self.prime(60_000.0, 59_910.0)                     # -15bp: DOWN is the favourite
        r = self.fire("DOWN", [(0.55, 100)])
        limit = self.expect(r, "DOWN", s0, 59_910.0)
        self.assertEqual(r["result"], "filled")
        self.assertAlmostEqual(r["cap"], min(0.60, limit))

    def test_add_on_buys_pass_the_same_gate(self):
        self.prime(60_000.0, 60_090.0)
        first = self.fire("UP", [(0.55, 100)])
        self.assertEqual(first["result"], "filled")
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.56, 100)])                      # a higher ask, still under the fair limit
        self.assertEqual(r["result"], "filled")                 # no below-average rule any more
        rs = int(self.m.window_end) - S.ROUND_S
        self.sn.fair_model.observe(rs + 1.0, 70.7, 59_990.0)    # BTC falls under the start: UP worth < 0.5
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.56, 100)])
        self.assertEqual(r["reason"], "ask not below fair value")

    def test_no_start_price_or_no_history_skips(self):
        r = self.fire("UP", [(0.30, 100)])                      # nothing observed at all
        self.assertEqual((r["result"], r["reason"]), ("skipped", "fair value unknown: no BTC price"))
        rs = int(self.m.window_end) - S.ROUND_S
        self.sn.fair_model.observe(rs + 120.0, 0.0, 60_000.0)  # joined two minutes in
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.30, 100)])
        self.assertEqual(r["reason"], "fair value unknown: no start price")
        sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_FAIR="1"))
        rows = []
        sn._write = rows.append
        sn.fair_model.observe(rs - 0.5, 0.0, 60_000.0)          # a start price, but no minute of samples
        sn.fair_model.observe(rs + 1.0, 0.4, 60_000.0)
        self.sn = sn
        self.rows = rows
        r = self.fire("UP", [(0.30, 100)])
        self.assertEqual(r["reason"], "fair value unknown: volatility warming up")

    def test_switched_off_it_buys_at_ask_plus_slip(self):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_FAIR="0"))
        self.sn._write = self.rows.append
        r = self.fire("UP", [(0.95, 100)])
        self.assertEqual((r["result"], r["cap"]), ("filled", 0.99))
        self.assertNotIn("fair", r)


class Feed(unittest.TestCase):
    def test_the_trigger_feed_feeds_the_model(self):
        with patch.dict(os.environ, {"SNIPER_FAIR": "1", "SNIPER_LATENCY_MS": "200"}):
            sn = S.Sniper(None, None, settings=S.Settings())
        sn._fire_now = lambda *a, **k: None
        for i, mid in enumerate((60_000.0, 60_001.0, 60_002.0)):
            sn.on_coinbase(json.dumps({"type": "ticker", "sequence": i + 1, "product_id": "BTC-USD",
                                       "best_bid": f"{mid - 0.01:.2f}", "best_ask": f"{mid + 0.01:.2f}"}))
        self.assertAlmostEqual(sn.fair_model.last_mid, 60_002.0)
        self.assertTrue(sn.fair_model.starts)
        self.assertIn("fair", sn.summary())


class Dashboard(unittest.TestCase):
    def text(self, fair):
        import tests_dashboard as td
        from dashboard import build
        from dashboard.theme import UNICODE
        snap = td.populated("PAPER")
        if fair is not None:
            snap["trigger"]["fair"] = fair
        return "\n".join("".join(seg[0] for seg in line) for line in build(snap, 160, 48, UNICODE))

    def test_the_trigger_panel_shows_fair_value_or_why_not(self):
        on = self.text({"up": 0.6234, "why": None, "vol_bp": 0.61, "start": 60_100.0})
        self.assertIn("UP 0.62  DN 0.38  vol 0.61bp", on)
        self.assertIn("unknown: no start price - no buys", self.text({"up": None, "why": "no start price"}))
        off = self.text(None)
        self.assertNotIn("fair ", off.split("ORDERS")[0])
        for t in (on, off):                                     # the order / latency line still fits
            self.assertIn("order     FAK", t)
            self.assertIn("last -1.20bp", t)


class Settings(unittest.TestCase):
    def env(self, **kw):
        base = {k: v for k, v in os.environ.items() if not k.startswith("SNIPER_")}
        return patch.dict(os.environ, {**base, **kw}, clear=True)

    def test_on_by_default_and_in_the_frozen_spec(self):
        with self.env(SNIPER_ONLY="1"):
            s = S.Settings()
            self.assertEqual(X.spec_violations(s, 25), [])
        self.assertEqual((s.fair, s.fair_margin, s.fair_vol_s, s.fair_fee_rate), (True, 0.01, 300.0, 0.07))
        spec = dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)
        self.assertEqual((spec["fair"], spec["fair_margin"]), (True, 0.01))
        self.assertIn("every buy at or under fair value - fee - $0.01", s.lines()[0])
        self.assertIn("every buy at or under fair value - fee - $0.01", s.lines(live=True)[0])

    def test_live_refuses_it_off_or_with_another_margin(self):
        for kw, word in (({"SNIPER_FAIR": "0"}, "fair value"), ({"SNIPER_FAIR_MARGIN": "0.02"}, "fair margin")):
            with self.subTest(kw), self.env(SNIPER_ONLY="1", **kw):
                bad = X.spec_violations(S.Settings(), 25)
            self.assertTrue(any(word in v for v in bad), bad)

    def test_bad_values_stop_the_start(self):
        for kw in ({"SNIPER_FAIR": "maybe"}, {"SNIPER_FAIR_MARGIN": "-0.01"}, {"SNIPER_FAIR_MARGIN": "0.6"},
                   {"SNIPER_FAIR_VOL_S": "30"}, {"SNIPER_FAIR_FEE_RATE": "0.5"}):
            with self.subTest(kw), self.env(**kw), self.assertRaises(ValueError):
                S.Settings()


if __name__ == "__main__":
    unittest.main(verbosity=2)
