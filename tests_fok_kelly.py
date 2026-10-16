#!/usr/bin/env python3
"""SNIPER_FOK_KELLY (2026-10-16, the operator's paper experiment): a FOK sized
as a FAK is - fair value, half Kelly, share sizing - so the two differ in the
order type only. SNIPER_KELLY_BUY_MIN (same day): Kelly under the venue's
5-share minimum buys the minimum instead of skipping. No network.

    python tests_fok_kelly.py
"""
from __future__ import annotations

import asyncio
import math
import pathlib
import sys
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import tests_sniper_fair as F  # noqa: E402


class FokKelly(unittest.TestCase):
    def gate(self, bankroll=40.0, **env):
        g = F.Gate("test_down_moves_buy_down_when_its_ask_lags")
        g.setUp()
        self.addCleanup(g.tearDown)
        g.make(**env)
        g.pin_time_left(120.0)
        g.sn._read_bankroll = lambda: bankroll                 # Kelly well inside the side's 200-share room
        g.sn._bankroll = None
        return g

    def fire(self, g, decision, match):
        """One UP signal: the decision sees `decision`, the order meets `match`."""
        token = g.m.up_token
        g.mids[token] = 0.40
        g.m.set(token, decision, match)
        g.sn.last_fire_by_side["UP"] = -1e18
        before = len(g.rows)
        tick_ms = time.monotonic() * 1000.0
        g.sn.trigger.ref = (tick_ms - 250.0, 60_000.0)
        asyncio.run(g.sn._fire("UP", 5.0, tick_ms))
        rows = [r for r in g.rows[before:] if r.get("result") in ("filled", "refused", "skipped", "error")]
        self.assertEqual(len(rows), 1, g.rows[before:])
        return rows[0]

    def test_fok_with_the_switch_is_sized_by_the_fair_value_and_kelly(self):
        g = self.gate(SNIPER_ORDER_TYPE="FOK", SNIPER_FOK_KELLY="1", SNIPER_ORDER_SIZING="shares")
        self.assertIn("Kelly", g.sn.s.lines()[0])
        r = self.fire(g, [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "filled")
        self.assertIsNotNone(r.get("kelly_shares"))
        self.assertEqual(r["target_shares"], r["kelly_shares"])                   # Kelly's shares, not the clip
        self.assertLess(r["kelly_shares"], 200)
        limit = math.floor(r["cap"] * 100 + 1e-9) / 100
        self.assertAlmostEqual(r["amount"], r["order_shares"] * limit, places=6)   # share sizing, as FAK
        self.assertAlmostEqual(r["shares"], r["amount"] / 0.41, places=3)          # the whole order at the ask

    def test_a_thin_book_is_no_fill_never_a_partial(self):
        g = self.gate(SNIPER_ORDER_TYPE="FOK", SNIPER_FOK_KELLY="1", SNIPER_ORDER_SIZING="shares")
        r = self.fire(g, [(0.41, 1000)], [(0.41, 3)])                             # 3 shares there at the match
        self.assertEqual(r["result"], "refused")
        self.assertIn("FOK", str(r.get("reason")))
        self.assertFalse(r.get("partial"))

    def test_without_the_switch_fok_keeps_its_fixed_clip(self):
        g = self.gate(SNIPER_ORDER_TYPE="FOK")
        r = self.fire(g, [(0.41, 1000)], [(0.41, 1000)])
        self.assertIsNone(r.get("kelly_shares"))
        self.assertAlmostEqual(r["amount"], 10 * 0.41, places=6)                   # SNIPER_SHARES x ask, as before
        self.assertNotIn("Kelly", g.sn.s.lines()[0])

    def test_fak_is_unchanged_by_the_switch(self):
        a = self.fire(self.gate(SNIPER_ORDER_SIZING="shares"), [(0.41, 1000)], [(0.41, 1000)])
        b = self.fire(self.gate(SNIPER_ORDER_SIZING="shares", SNIPER_FOK_KELLY="1"), [(0.41, 1000)], [(0.41, 1000)])
        for k in ("kelly_shares", "target_shares", "order_shares", "amount", "cap"):
            self.assertEqual(a[k], b[k], k)


class KellyBuyMin(unittest.TestCase):
    """SNIPER_KELLY_BUY_MIN (2026-10-16, paper): Kelly under the venue's 5-share
    minimum buys the minimum instead of skipping - while Kelly wants to add
    shares and cash pays for them."""
    FOK = dict(SNIPER_ORDER_TYPE="FOK", SNIPER_FOK_KELLY="1", SNIPER_ORDER_SIZING="shares")
    gate, fire = FokKelly.gate, FokKelly.fire
    SMALL = 2.5                     # Kelly ~1 share here; 5 sh x the cap is about $2.30

    def test_kelly_under_five_buys_five(self):
        g = self.gate(self.SMALL, SNIPER_KELLY_BUY_MIN="1", **self.FOK)
        self.assertIn("under 5 sh: buys 5", g.sn.s.lines()[0])
        r = self.fire(g, [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "filled", r.get("reason"))
        self.assertLess(r["kelly_shares"], 5)                                   # what Kelly said
        self.assertTrue(r["kelly_buy_min"])
        self.assertEqual(r["target_shares"], 5)
        self.assertEqual(r["order_shares"], 5)
        limit = math.floor(r["cap"] * 100 + 1e-9) / 100
        self.assertAlmostEqual(r["amount"], 5 * limit, places=6)

    def test_without_the_switch_it_still_skips(self):
        r = self.fire(self.gate(self.SMALL, **self.FOK), [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "skipped")
        self.assertEqual(r["reason"], "kelly size under the 5-share minimum")
        self.assertNotIn("kelly_buy_min", r)

    def test_cash_under_the_minimum_skips(self):
        r = self.fire(self.gate(1.5, SNIPER_KELLY_BUY_MIN="1", **self.FOK), [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "skipped")
        self.assertEqual(r["reason"], "kelly size under the 5-share minimum")

    def test_no_second_minimum_once_kelly_is_held(self):
        g = self.gate(self.SMALL, SNIPER_KELLY_BUY_MIN="1", **self.FOK)
        g.sn._kelly_inventory = lambda *a, **k: (5.0, 0.0)                      # the first minimum, held
        r = self.fire(g, [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "skipped")
        self.assertEqual(r["reason"], "kelly size under the 5-share minimum")

    def test_the_room_still_bounds_it(self):
        g = self.gate(self.SMALL, SNIPER_KELLY_BUY_MIN="1", SNIPER_MAX_SIDE_SHARES="5", **self.FOK)
        self.assertEqual(g.sn.s.max_side_shares, 5)                              # 5 sh of room: 5.6 at the ask won't fit
        r = self.fire(g, [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "skipped")
        self.assertEqual(r["reason"], "kelly room under venue minimum at price cap")

    def test_fak_takes_the_switch_too(self):
        r = self.fire(self.gate(self.SMALL, SNIPER_KELLY_BUY_MIN="1", SNIPER_ORDER_SIZING="shares"),
                      [(0.41, 1000)], [(0.41, 1000)])
        self.assertEqual(r["result"], "filled", r.get("reason"))
        self.assertEqual(r["order_shares"], 5)

    def test_live_refuses_it(self):
        import exec_latency
        on = self.gate(SNIPER_KELLY_BUY_MIN="1").sn.s
        off = self.gate().sn.s
        self.assertTrue(any("SNIPER_KELLY_BUY_MIN" in v for v in exec_latency.spec_violations(on)))
        self.assertFalse(any("SNIPER_KELLY_BUY_MIN" in v for v in exec_latency.spec_violations(off)))
        self.assertTrue(any("buy 5" in ln for ln in exec_latency.effective_config_lines(on, live=False)))

    def test_kelly_wanted_is_kelly_shares_before_rounding(self):
        import sniper_mode as S
        for held, opp in ((0.0, 0.0), (3.0, 0.0), (0.0, 4.0)):
            w = S.kelly_wanted(0.62, 0.41, 0.07, 0.5, 40.0, held_shares=held, opposite_shares=opp)
            sh, _f = S.kelly_shares(0.62, 0.41, 0.07, 0.5, 40.0, 1000.0, held_shares=held, opposite_shares=opp)
            self.assertEqual(sh, max(0.0, math.floor(w + 1e-9)))
        self.assertEqual(S.kelly_wanted(0.40, 0.41, 0.07, 0.5, 40.0), 0.0)        # no edge: nothing


if __name__ == "__main__":
    unittest.main()
