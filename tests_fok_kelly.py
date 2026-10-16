#!/usr/bin/env python3
"""SNIPER_FOK_KELLY (2026-10-16, the operator's paper experiment): a FOK sized
as a FAK is - fair value, half Kelly, share sizing - so the two differ in the
order type only. No network.

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


if __name__ == "__main__":
    unittest.main()
