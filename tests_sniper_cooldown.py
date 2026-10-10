#!/usr/bin/env python3
"""The sniper's cooldown: 300ms, per side, started when an order is submitted.

    python tests_sniper_cooldown.py

Drives the real Sniper._signal gate on a controlled monotonic clock, with
the order path replaced by a recorder (and, in the last tests, the real
Coinbase and Binance tick handlers). No network.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import types
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S  # noqa: E402

UP_MOVE = types.SimpleNamespace(update=lambda t, mid: (1, 0.6))
DOWN_MOVE = types.SimpleNamespace(update=lambda t, mid: (-1, -0.6))


class Clock:
    def __init__(self, ms=10_000.0):
        self.ms = ms

    def __call__(self):
        return self.ms / 1000.0


def make(env=None, submit_ms=0.0, sent=True):
    """A Sniper whose order path records each order and takes `submit_ms` to
    submit it (the clock moves on), then resolves (busy cleared)."""
    base = {"SNIPER_ONLY": "1", "SNIPER_LATENCY_MS": "50", "SNIPER_SHADOW_MS": ""}
    with patch.dict(os.environ, {**base, **(env or {})}):
        sn = S.Sniper(None, None, settings=S.Settings())
    clock = Clock()
    orders = []

    def fire(side, r_bp, t_ms, t_pc=None, feed=None):
        clock.ms += submit_ms
        orders.append((side, round(clock.ms - 10_000.0, 1)))
        sn.busy = False                   # the order resolved: only the cooldown gates the next one
        return object() if sent else None
    sn._fire_now = fire
    return sn, clock, orders


def signal(sn, clock, move, at_ms, feed=None):
    """One signal `at_ms` after the start of the test clock, from `feed`
    (None: the trigger feed, SNIPER_PRIMARY_FEED)."""
    clock.ms = 10_000.0 + at_ms
    sn._signal(feed or sn.s.primary_feed, move, 60_000.0, clock.ms, 0)


class CooldownTests(unittest.TestCase):
    def run_signals(self, sn, clock, steps):
        with patch.object(S.time, "monotonic", clock):
            for move, at in steps:
                signal(sn, clock, move, at)

    def test_default_is_300ms(self):
        with patch.dict(os.environ, {"SNIPER_ONLY": "1"}, clear=True):
            self.assertEqual(S.Settings().cooldown_ms, 300)
        with patch.dict(os.environ, {"SNIPER_ONLY": "1", "SNIPER_LATENCY_MS": "50"}, clear=True):
            self.assertIn("cooldown 300ms per side", S.Settings().lines()[0])

    def test_same_side_signals_inside_300ms_make_no_order(self):
        sn, clock, orders = make()
        self.run_signals(sn, clock, [(UP_MOVE, 0), (UP_MOVE, 100), (UP_MOVE, 299), (UP_MOVE, 300)])
        self.assertEqual(orders, [("UP", 0.0), ("UP", 300.0)])     # 100 and 299 fall in UP's cooldown
        self.assertEqual(sn.n["skip: cooldown"], 2)

    def test_an_up_order_does_not_block_down(self):
        sn, clock, orders = make()
        self.run_signals(sn, clock, [(UP_MOVE, 0), (DOWN_MOVE, 50), (UP_MOVE, 100), (DOWN_MOVE, 150)])
        self.assertEqual(orders, [("UP", 0.0), ("DOWN", 50.0)])    # UP@100 and DOWN@150 each in their own side's cooldown
        self.assertEqual(sn.n["skip: cooldown"], 2)

    def test_a_down_order_does_not_block_up(self):
        sn, clock, orders = make()
        self.run_signals(sn, clock, [(DOWN_MOVE, 0), (UP_MOVE, 1), (DOWN_MOVE, 250), (UP_MOVE, 302)])
        self.assertEqual(orders, [("DOWN", 0.0), ("UP", 1.0), ("UP", 302.0)])
        self.assertEqual(sn.last_fire_by_side, {"UP": 10_302.0, "DOWN": 10_000.0})

    def test_the_cooldown_starts_when_the_order_is_submitted(self):
        # deciding, signing and sending take 40ms: the cooldown runs from the
        # submission (t=40), not from the BTC tick (t=0)
        sn, clock, orders = make(submit_ms=40.0)
        self.run_signals(sn, clock, [(UP_MOVE, 0), (UP_MOVE, 320), (UP_MOVE, 340)])
        self.assertEqual(orders, [("UP", 40.0), ("UP", 380.0)])    # 320 is only 280ms after the submission
        self.assertEqual(sn.n["skip: cooldown"], 1)

    def test_a_signal_that_sends_no_order_starts_no_cooldown(self):
        sn, clock, orders = make(sent=False)                      # every signal skipped by a later gate
        self.run_signals(sn, clock, [(UP_MOVE, 0), (UP_MOVE, 10), (UP_MOVE, 20)])
        self.assertEqual(len(orders), 3)
        self.assertEqual(sn.n["skip: cooldown"], 0)
        self.assertEqual(sn.last_fire_by_side, {"UP": -1e18, "DOWN": -1e18})

    def test_the_setting_still_applies_per_side(self):
        sn, clock, orders = make({"SNIPER_COOLDOWN_MS": "1000"})
        self.run_signals(sn, clock, [(UP_MOVE, 0), (DOWN_MOVE, 10), (UP_MOVE, 900), (UP_MOVE, 1000)])
        self.assertEqual(orders, [("UP", 0.0), ("DOWN", 10.0), ("UP", 1000.0)])

    def test_a_second_feed_on_the_same_move_is_still_one_order(self):
        # Binance as the trigger, Coinbase as the second one (the primary is never its own second trigger)
        sn, clock, orders = make({"SNIPER_PRIMARY_FEED": "binance", "SNIPER_EXTRA_FEEDS": "coinbase"})
        with patch.object(S.time, "monotonic", clock):
            signal(sn, clock, UP_MOVE, 0, feed="coinbase")
            signal(sn, clock, UP_MOVE, 5, feed="binance")
            signal(sn, clock, DOWN_MOVE, 20, feed="binance")
        self.assertEqual(orders, [("UP", 0.0), ("DOWN", 20.0)])
        self.assertEqual(sn.n["skip: cooldown"], 1)

    def test_one_order_in_flight_per_side(self):
        # While an order awaits its outcome its SIDE waits; the other side does not.
        sn, clock, orders = make()

        def fire(side, *a, **k):
            orders.append((side, None))
            sn.inflight[side] = {"end": None}           # stays in flight, like a real order
            return object()
        sn._fire_now = fire
        self.run_signals(sn, clock, [(UP_MOVE, 0), (DOWN_MOVE, 50), (UP_MOVE, 400), (DOWN_MOVE, 450)])
        self.assertEqual(orders, [("UP", None), ("DOWN", None)])
        self.assertEqual(sn.n["skip: previous order in flight"], 2)     # UP@400 and DOWN@450, each its own side
        self.assertEqual(sn.n["skip: cooldown"], 0)

    def test_last_fire_compatibility(self):
        sn, clock, orders = make()
        self.run_signals(sn, clock, [(UP_MOVE, 0)])
        self.assertEqual(sn.last_fire, 10_000.0)
        sn.last_fire = -1e18                                      # clears both sides
        self.assertEqual(sn.last_fire_by_side, {"UP": -1e18, "DOWN": -1e18})

    def test_real_binance_ticks_fire_up_then_down_inside_300ms(self):
        sn, clock, orders = make({"SNIPER_K_BP": "0.5", "SNIPER_BINANCE_FRESHNESS": "0", "SNIPER_PRIMARY_FEED": "binance"})
        with patch.object(S.time, "monotonic", clock):
            # the rule compares with the price at least 250ms old: +0.67bp at 260ms
            # (UP), then -0.67bp at 300ms against that same 0ms price (DOWN, 40ms later)
            for at, mid in ((0, 60_000.0), (260, 60_004.0), (300, 59_996.0)):
                clock.ms = 10_000.0 + at
                sn.on_tick(json.dumps({"b": f"{mid - 0.01:.2f}", "a": f"{mid + 0.01:.2f}"}))
        self.assertEqual([side for side, _ in orders], ["UP", "DOWN"])

    def test_real_coinbase_tickers_fire_up_then_down_inside_300ms(self):
        """The same moves as Coinbase BTC-USD tickers - the trigger by default."""
        sn, clock, orders = make({"SNIPER_K_BP": "0.5"})
        self.assertEqual(sn.s.primary_feed, "coinbase")
        with patch.object(S.time, "monotonic", clock):
            for seq, (at, mid) in enumerate(((0, 60_000.0), (260, 60_004.0), (300, 59_996.0)), 1):
                clock.ms = 10_000.0 + at
                sn.on_coinbase(json.dumps({"type": "ticker", "sequence": seq, "product_id": "BTC-USD",
                                           "best_bid": f"{mid - 0.01:.2f}", "best_ask": f"{mid + 0.01:.2f}"}))
        self.assertEqual([side for side, _ in orders], ["UP", "DOWN"])
        self.assertEqual((sn.n["signals"], sn.n["coinbase ticks"]), (2, 3))     # unsuffixed: it is the trigger


if __name__ == "__main__":
    unittest.main(verbosity=2)
