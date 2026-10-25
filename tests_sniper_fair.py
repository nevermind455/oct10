#!/usr/bin/env python3
"""The fair-value gate and Kelly size (SNIPER_FAIR / SNIPER_KELLY, the frozen
spec since 2026-10-11).

    python tests_sniper_fair.py

The fair value is MARKET-ANCHORED: the market's own mid for the side just
before the move, read as a probability, moved by the BTC move over the
volatility and time left. Every buy must be at or under it less the taker fee
and a margin, and FAK orders buy full Kelly on it: from 5 shares, no per-order
cap - only what is left of the side's per-round cap (live: the canary's too).

Expected values are computed here from the formulas themselves (math.erf, a
bisection inverse, brute force over the tick grid), never with the functions
under test. The order path runs on the real Sniper, PaperBroker and Ledger
(tests_sniper_ioc).
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import pathlib
import sys
import time
from decimal import Decimal
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402  (Market, PaperIOCBase, settings, View)

RATE, MARGIN = 0.07, 0.01


def phi(z):                                   # the oracle's own normal CDF
    return 0.5 * math.erfc(-z / math.sqrt(2.0))


def phi_inv(p):                               # ... and its inverse, by bisection
    lo, hi = -10.0, 10.0
    for _ in range(200):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if phi(mid) < p else (lo, mid)
    return (lo + hi) / 2


def oracle_fair(mid, move_bp, sigma, tau):
    m = min(max(mid, 0.005), 0.995)
    return phi(phi_inv(m) + (abs(move_bp) / 1e4) / (sigma * math.sqrt(tau)))


def oracle_limit(fair, margin=MARGIN, rate=RATE):
    """Brute force: the highest cent p with p + rate p (1-p) + margin <= fair."""
    best = 0.0
    for c in range(1, 100):
        p = c / 100.0
        if p + rate * p * (1 - p) + margin <= fair + 1e-12:
            best = p
    return best


def oracle_kelly(fair, ask, bankroll, fraction=0.25, cap=20, rate=RATE):
    p = ask + rate * ask * (1 - ask)
    f = (fair - p) / (1 - p)
    return (0.0 if f <= 0 else float(min(math.floor(fraction * f * bankroll / p + 1e-9), cap))), f


# =========================================================================== the math
class Math(unittest.TestCase):
    def test_the_normal_distribution(self):
        for z, want in ((0.0, 0.5), (1.0, 0.8413447460685429), (-1.96, 0.024997895148220435)):
            self.assertAlmostEqual(S.norm_cdf(z), want, places=12)

    def test_the_venue_fee_curve(self):
        self.assertAlmostEqual(S.taker_fee(0.5, RATE), 0.0175)
        self.assertAlmostEqual(S.taker_fee(0.9, RATE), 0.0063)

    def test_anchored_fair_against_the_formula(self):
        for mid, move, sigma, tau in ((0.22, 0.68, 0.45e-4, 147.0), (0.30, 1.35, 0.458e-4, 139.0),
                                      (0.50, 0.5, 1e-4, 60.0), (0.85, 2.0, 0.6e-4, 20.0), (0.05, 0.7, 0.4e-4, 240.0)):
            with self.subTest(mid=mid, move=move):
                self.assertAlmostEqual(S.anchored_fair(mid, move, sigma, tau), oracle_fair(mid, move, sigma, tau),
                                       places=9)
        self.assertAlmostEqual(S.anchored_fair(0.22, 0.68, 0.45e-4, 147.0), 0.2586, places=3)  # the 16:57:33 trade

    def test_the_market_sets_the_level_and_the_move_only_adds(self):
        self.assertGreater(S.anchored_fair(0.40, 1.0, 1e-4, 100), 0.40)       # a move toward the side: worth more
        self.assertAlmostEqual(S.anchored_fair(0.40, 0.0, 1e-4, 100), 0.40)   # no move: exactly the market
        small, late = S.anchored_fair(0.40, 0.5, 1e-4, 200), S.anchored_fair(0.40, 0.5, 1e-4, 20)
        self.assertLess(small, late)                                         # the same move matters more late
        self.assertLess(S.anchored_fair(0.40, 0.5, 2e-4, 100), S.anchored_fair(0.40, 0.5, 1e-4, 100))   # ... less when wild

    def test_the_limit_is_the_highest_cent_that_keeps_fee_and_margin(self):
        fair = 0.02
        while fair < 1.0:
            with self.subTest(fair=fair):
                self.assertAlmostEqual(S.fair_limit(fair, MARGIN, 0.01, RATE), oracle_limit(fair), places=9)
            fair = round(fair + 0.0037, 6)
        self.assertEqual(S.fair_limit(0.015, MARGIN, 0.01, RATE), 0.0)

    def test_kelly_shares(self):
        for fair, ask, bank in ((0.35, 0.31, 1000), (0.2586, 0.23, 1000), (0.62, 0.58, 1000), (0.62, 0.58, 150),
                                (0.2586, 0.23, 40), (0.55, 0.53, 300), (0.9, 0.86, 1000)):
            with self.subTest(fair=fair, ask=ask, bank=bank):
                got = S.kelly_shares(fair, ask, RATE, 0.25, bank, 20)
                want = oracle_kelly(fair, ask, bank)
                self.assertEqual(got[0], want[0])
                self.assertAlmostEqual(got[1], want[1], places=12)
        self.assertEqual(S.kelly_shares(0.30, 0.31, RATE, 0.25, 1000, 20)[0], 0.0)   # no edge, no shares
        self.assertEqual(S.kelly_shares(0.35, 0.31, RATE, 0.25, 0, 20), (0.0, 0.0))  # no cash
        self.assertEqual(S.kelly_shares(0.35, 0.31, RATE, 0.25, 1000, 20)[0], 20.0)  # Kelly 28 -> the cap


class Vol(unittest.TestCase):
    def test_volatility_per_sqrt_second_from_one_sample_a_second(self):
        f = S.Volatility(min_samples=60)
        v, mono, x = 0.8e-4, 0.0, math.log(60_000.0)
        for i in range(200):
            for _ in range(4):                                  # ticks every 0.25s: one sample a second counts
                f.observe(mono, math.exp(x))
                mono += 0.25
            x += v if i % 2 else -v
        self.assertAlmostEqual(f.vol(), v, delta=v * 0.02)
        g = S.Volatility(min_samples=60)
        for i in range(30):
            g.observe(float(i), 60_000.0)
        self.assertIsNone(g.vol())                              # under a minute of samples
        for i in range(30, 120):
            g.observe(float(i), 60_000.0)
        self.assertAlmostEqual(g.vol(), S.FAIR_MIN_VOL_BP / 1e4)   # flat: the floor, never zero

    def test_the_window_forgets_old_volatility(self):
        f = S.Volatility(window_s=60, min_samples=30)
        for i in range(120):
            f.observe(float(i), 60_000.0 * (1.001 if i % 2 else 1.0))
        for i in range(120, 200):
            f.observe(float(i), 60_000.0 * (1.0001 if i % 2 else 1.0))
        self.assertAlmostEqual(f.vol(), math.log(1.0001), delta=1e-6)


# =========================================================================== the order path
class Gate(I.PaperIOCBase):
    """The real Sniper and paper broker with SNIPER_FAIR=1 / SNIPER_KELLY=1: a
    known volatility (steps of exactly V), the market's mid set per token."""

    V = 1e-4                                                    # 1bp per sqrt(second)

    def setUp(self):
        super().setUp()
        self.mids = {}
        market = self.m
        test = self

        def view(token):
            v = I.View(market.decision[str(token)])
            v.mid = test.mids.get(str(token), v.mid)
            return v
        self.m.book = type("Book", (), {"view": staticmethod(view),
                                        "view_at_mono": staticmethod(lambda token, mono: view(token))})()
        from paper_trade import MarketRules
        self.broker._rules_fetch = lambda cid: MarketRules(
            cid, Decimal(str(RATE)), 1, min_order_size=Decimal("5"), tick_size=Decimal("0.01"),
            up_token_id=self.m.up_token, down_token_id=self.m.down_token)
        self.broker._rules(self.m.condition_id)  # production warms rules before evaluating a signal
        self.make()

    def make(self, **env):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(**{
            "SNIPER_FAIR": "1", "SNIPER_FAIR_MARGIN": str(MARGIN), "SNIPER_KELLY": "1",
            "SNIPER_KELLY_FRACTION": "0.5", **env}))
        self.sn._write = self.rows.append
        for i in range(70):                                     # a known minute: steps of exactly V
            self.sn.vol_model.observe(float(i), 60_000.0 * math.exp(self.V if i % 2 else 0.0))

    def pin_time_left(self, seconds: float = 120.0) -> None:
        """Fix the round's time left (tau). The fair value - and so Kelly's size -
        depends on it; the current round's end left it anywhere in 20..300s by
        when the suite ran, so a size-dependent test flaked (2026-10-14 repair)."""
        self.m.window_end = time.time() + seconds

    def kelly_wants_the_room(self, room: float) -> None:
        """The premise of a room-trim test: unbounded, Kelly wants more than the
        room. Asserted, so a model change fails here instead of flaking."""
        bank = self.sn._bankroll_now()
        tau = self.m.window_end - time.time()
        fair = oracle_fair(0.40, 5.0, self.V, tau)
        want, _f = oracle_kelly(fair, 0.41, bank, fraction=self.sn.s.kelly_fraction, cap=10_000)
        self.assertGreater(want, room, f"premise: Kelly wants {want} > the {room} room at tau={tau:.0f}s")

    def order(self, side, ask_levels, mid, move_bp):
        token = self.m.up_token if side == "UP" else self.m.down_token
        self.mids[token] = mid
        self.m.set(token, ask_levels)
        self.sn.last_fire_by_side[side] = -1e18
        before = len(self.rows)
        tick_ms = time.monotonic() * 1000.0
        self.sn.trigger.ref = (tick_ms - 250.0, 60_000.0)
        asyncio.run(self.sn._fire(side, move_bp, tick_ms))
        rows = [r for r in self.rows[before:] if r.get("result") in ("filled", "refused", "skipped", "error")]
        self.assertEqual(len(rows), 1, self.rows[before:])
        return rows[0]

    def check_fair(self, r, mid, move):
        want = oracle_fair(mid, move, self.V, r["tau_s"])
        self.assertAlmostEqual(r["fair"], want, places=3)
        self.assertEqual(r["market_mid"], mid)
        self.assertTrue(oracle_limit(r["fair"] - 2e-4) - 1e-9 <= r["fair_limit"] <= oracle_limit(r["fair"] + 2e-4) + 1e-9)

    def test_an_ask_under_the_anchored_limit_buys_kelly_shares(self):
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.check_fair(r, 0.40, 5.0)
        self.assertEqual(r["result"], "filled")
        self.assertAlmostEqual(r["cap"], min(0.46, r["fair_limit"]))
        want, f = oracle_kelly(r["fair"], 0.41, 1000.0, fraction=self.sn.s.kelly_fraction, cap=200)
        self.assertLessEqual(abs(r["target_shares"] - want), 1.0)    # the row's fair is rounded to 4 places
        self.assertEqual((r["bankroll"], r["carry_in"]), (1000.0, 0.0))
        self.assertAlmostEqual(r["kelly_f"], f, places=3)

    def test_the_market_level_rules_not_a_start_price(self):
        """The 16:57 trades: the market said UP 0.22-0.30 while the old model said
        0.43-0.54. Anchored, a +0.68bp move is worth a few cents over the
        market - not 20 - and an ask well over the market is refused."""
        r = self.order("UP", [(0.31, 200)], mid=0.25, move_bp=0.68)
        self.check_fair(r, 0.25, 0.68)
        self.assertLess(r["fair"], 0.30)
        self.assertEqual((r["result"], r["reason"]), ("skipped", "ask not below fair value"))

    def test_down_moves_buy_down_when_its_ask_lags(self):
        r = self.order("DOWN", [(0.73, 200)], mid=0.72, move_bp=-6.0)
        self.check_fair(r, 0.72, 6.0)
        self.assertEqual(r["result"], "filled")
        self.assertEqual(r["side"], "DOWN")

    def test_no_volatility_or_no_market_price_skips(self):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_FAIR="1", SNIPER_KELLY="1"))
        self.sn._write = self.rows.append
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["reason"], "fair value unknown: volatility warming up")
        self.make()
        r = self.order("UP", [(0.41, 200)], mid=None, move_bp=5.0)
        self.assertEqual(r["reason"], "fair value unknown: no market price")

    def test_too_little_cash_for_the_venue_minimum_skips(self):
        self.sn._read_bankroll = lambda: 2.0                     # $2: even full Kelly is a share or two
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.assertEqual((r["result"], r["reason"]), ("skipped", "kelly size under the 5-share minimum"))
        self.sn._read_bankroll = lambda: None
        self.sn._bankroll = None
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=6.0)
        self.assertEqual(r["reason"], "kelly size unknown: no cash reading")

    def test_no_per_order_cap_only_the_sides_room_in_the_round(self):
        self.pin_time_left(120.0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertGreater(r["target_shares"], 20)                   # the old 20-share cap is gone
        self.assertLessEqual(r["target_shares"], 200)
        rd = self.sn.rounds[self.m.window_end]
        rd["UP"] = 180.0                                             # 180 already held this round
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertLessEqual(r["target_shares"], 20.0)               # trimmed to the 20 left, not skipped
        rd["UP"] = 198.0                                             # 2 left: under the 5-share minimum
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["reason"], "kelly size under the 5-share minimum")

    def test_live_keeps_the_canary_per_order_cap(self):
        import types
        self.sn.live = types.SimpleNamespace(balance=1000.0, sender=types.SimpleNamespace(
            limits=types.SimpleNamespace(max_order_shares=25.0)))
        self.mids[self.m.up_token] = 0.40
        self.m.set(self.m.up_token, [(0.41, 1000)])
        plan = self.sn._decide("UP", {"bp": 5.0, "window_ms": 250.0}, time.monotonic() * 1000.0)
        self.assertIsNotNone(plan)
        self.assertLessEqual(plan["target"], 25.0)

    def test_thin_best_ask_trims_stake_to_round_room_instead_of_missing_the_order(self):
        self.make(SNIPER_SLIP="0.02")
        self.pin_time_left(120.0)
        self.kelly_wants_the_room(20.0)
        rd = self.sn.rounds[self.m.window_end]
        rd["UP"] = 180.0
        # Kelly wants the 20 shares left. Their ladder cost is $8.58, but
        # the unchanged round guard conservatively counts $8.58/.41 > 20.
        r = self.order("UP", [(0.41, 1), (0.43, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled")
        self.assertEqual((r["target_shares"], r["amount"], r["cap"]), (20.0, 8.20, 0.43))
        self.assertLessEqual(r["amount"] / r["ask_seen"], 20.0)
        self.assertGreater(r["shares"], 18.0)
        self.assertLessEqual(rd["UP"], self.sn.s.max_side_shares)
        self.assertEqual(self.sn.carry["UP"], 0.0)

    def test_cent_rounding_floors_stake_to_fractional_round_room(self):
        self.make(SNIPER_SLIP="0.02")
        self.pin_time_left(120.0)
        self.kelly_wants_the_room(19.99)
        rd = self.sn.rounds[self.m.window_end]
        rd["UP"] = 180.01
        # 19.99 * .41 = $8.1959. Rounding UP to $8.20 would count
        # 20 shares and miss a valid smaller order; $8.19 stays within room.
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual((r["result"], r["amount"]), ("filled", 8.19))
        self.assertLessEqual(r["amount"] / r["ask_seen"], 19.99)
        self.assertLessEqual(rd["UP"], self.sn.s.max_side_shares)

    def test_minimum_at_the_signed_limit_cannot_enlarge_stake_past_round_room(self):
        self.make(SNIPER_SLIP="0.02")
        self.pin_time_left(120.0)
        self.kelly_wants_the_room(5.0)
        rd = self.sn.rounds[self.m.window_end]
        rd["UP"] = 195.0
        # $2.05 buys five at .41, but the sender would need $2.15 to
        # sign five at .43. That enlargement would violate the same guard.
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual((r["result"], r["reason"]),
                         ("skipped", "kelly room under venue minimum at price cap"))
        self.assertEqual(self.broker.filled_orders, 0)
        self.assertEqual(rd["UP"], 195.0)

    def test_thin_best_ask_keeps_the_live_per_order_room_conservative(self):
        import types
        self.make(SNIPER_SLIP="0.02")
        self.sn.live = types.SimpleNamespace(balance=1000.0, sender=types.SimpleNamespace(
            limits=types.SimpleNamespace(max_order_shares=20.0)))
        self.mids[self.m.up_token] = 0.40
        self.m.set(self.m.up_token, [(0.41, 1), (0.43, 1000)])
        plan = self.sn._decide("UP", {"bp": 5.0, "window_ms": 250.0}, time.monotonic() * 1000.0)
        self.assertIsNotNone(plan)
        self.assertEqual((plan["target"], plan["amount"]), (20.0, 8.20))
        self.assertLessEqual(plan["amount"] / plan["view"].best_ask, 20.0)

    def anchored_order(self, then, now, move_bp=5.0, ask=0.41):
        """An UP order whose book stood at mid `then` when the move began
        (window_ms before the tick) and stands at `now` when it signals."""
        token = self.m.up_token
        t_ms = time.monotonic() * 1000.0
        seen = []

        def view_at_mono(tok, mono):
            seen.append((tok, mono))
            v = I.View(self.m.decision[str(tok)])
            v.mid = then
            return v
        self.m.book.view_at_mono = view_at_mono
        self.sn.trigger.ref = (t_ms - 300.0, 60_000.0)                  # the move's reference, 300ms back
        self.mids[token] = now
        self.m.set(token, [(ask, 200)])
        self.sn.last_fire_by_side["UP"] = -1e18
        before = len(self.rows)
        asyncio.run(self.sn._fire("UP", move_bp, t_ms))
        rows = [r for r in self.rows[before:] if r.get("result") in ("filled", "refused", "skipped", "error")]
        self.assertEqual(len(rows), 1, self.rows[before:])
        self.assertEqual(seen[-1], (str(token), (t_ms - 300.0) / 1000.0))
        return rows[0]

    def test_the_anchor_is_the_book_from_before_the_move(self):
        """The book had already moved .40 -> .44 by the tick: anchoring on .44
        added the move to a price that partly held it already."""
        r = self.anchored_order(then=0.40, now=0.44)
        self.assertEqual((r["fair_anchor"], r["market_mid"], r["market_mid_now"]), ("before move", 0.40, 0.44))
        self.check_fair(r, 0.40, 5.0)
        self.assertLess(r["fair"], oracle_fair(0.44, 5.0, self.V, r["tau_s"]))

    def test_the_anchor_is_never_above_the_mid_now(self):
        r = self.anchored_order(then=0.45, now=0.40)                    # the book fell meanwhile: the lower one
        self.assertEqual((r["fair_anchor"], r["market_mid"]), ("now", 0.40))
        self.check_fair(r, 0.40, 5.0)

    def test_no_book_history_refuses_to_guess_a_pre_move_price(self):
        r = self.anchored_order(then=None, now=0.40)
        self.assertEqual((r["result"], r["reason"]),
                         ("skipped", "fair value unknown: no pre-move market history"))
        self.assertNotIn("fair", r)
        self.assertNotIn("market_mid_now", r)

    def test_back_to_back_orders_size_from_the_cash_left(self):
        r1 = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        r2 = self.order("DOWN", [(0.41, 1000)], mid=0.40, move_bp=-5.0)   # before the next cash reading
        self.assertEqual(r1["bankroll"], 1000.0)
        # Before the next cash refresh, paper holds the same conservative
        # all-in stake plus fee bound as live, not only the signed principal.
        self.assertAlmostEqual(r2["bankroll"], 1000.0 - r1["amount"] * (1 + r1["fee_rate"]), places=2)

    def test_a_cap_at_the_price_floor_is_skipped_not_sent(self):
        """10-12 VPS run: 14 orders with ask 0.05 = MIN_BUY_PRICE and cap 0.05
        were refused by the broker ("order price cap 0.05 is at or below the
        floor 0.05"); live would have bought at the floor."""
        self.make(SNIPER_FAIR="0", SNIPER_SLIP="0")
        self.broker.min_buy_price = 0.05
        r = self.order("UP", [(0.05, 200)], mid=0.05, move_bp=5.0)
        self.assertEqual((r["result"], r["reason"], r["cap"]), ("skipped", "price cap at the price floor", 0.05))
        r = self.order("UP", [(0.06, 200)], mid=0.06, move_bp=5.0)
        self.assertEqual(r["result"], "filled")

    def test_a_repeat_signal_does_not_repeat_the_same_outcome_kelly_bet(self):
        self.m.window_end = time.time() + 100.0
        self.sn._read_bankroll = lambda: 100.0
        first = self.order("UP", [(0.50, 1000)], mid=0.49, move_bp=3.0)
        self.assertEqual(first["result"], "filled")
        second = self.order("UP", [(0.50, 1000)], mid=0.49, move_bp=3.0)
        self.assertEqual((second["result"], second["reason"]),
                         ("skipped", "kelly size under the 5-share minimum"))
        self.assertAlmostEqual(second["kelly_held_shares"], first["shares"], places=3)
        self.assertEqual(self.broker.filled_orders, 1)

    def test_market_fees_override_an_understated_environment_fee(self):
        from paper_trade import MarketRules
        self.m.window_end = time.time() + 100.0
        self.sn.s.fair_fee_rate = 0.0
        self.broker._rules_cache[self.m.condition_id] = (MarketRules(
            self.m.condition_id, Decimal(".14"), 1, min_order_size=Decimal("5"),
            tick_size=Decimal(".01"), up_token_id=self.m.up_token, down_token_id=self.m.down_token), time.time())
        row = self.order("UP", [(0.58, 1000)], mid=0.49, move_bp=3.0)
        self.assertEqual((row["result"], row["reason"]), ("skipped", "ask not below fair value"))
        self.assertEqual((row["fee_rate"], row["fee_exponent"]), (.14, 1))
        self.assertEqual(self.broker.filled_orders, 0)

    def test_kelly_orders_carry_nothing(self):
        r = self.order("UP", [(0.41, 3)], mid=0.40, move_bp=5.0)        # only 3 shares at the ask, then nothing
        self.assertIn(r["result"], ("filled", "refused"))
        self.assertEqual(self.sn.carry["UP"], 0.0)                     # the unfilled rest is not carried
        r2 = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.assertEqual(r2["carry_in"], 0.0)

    def test_kelly_off_keeps_the_fixed_size(self):
        self.make(SNIPER_KELLY="0")
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.assertEqual((r["result"], r["target_shares"]), ("filled", 10.0))
        self.assertNotIn("kelly_f", r)

    def test_fair_off_buys_at_ask_plus_slip(self):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_FAIR="0"))
        self.sn._write = self.rows.append
        r = self.order("UP", [(0.95, 100)], mid=0.10, move_bp=1.0)
        self.assertEqual((r["result"], r["cap"]), ("filled", 0.99))
        self.assertNotIn("fair", r)


class Bankroll(unittest.TestCase):
    def test_it_is_read_off_the_order_path_and_never_guessed(self):
        with patch.dict(os.environ, {"SNIPER_FAIR": "1", "SNIPER_KELLY": "1", "SNIPER_LATENCY_MS": "200"}):
            reads = []

            class Broker:
                def cash_balance(self):
                    reads.append(1)
                    return 512.5
            sn = S.Sniper(None, Broker(), settings=S.Settings())
        self.assertEqual(sn._bankroll_now(), 512.5)
        self.assertEqual(sn._bankroll_now(), 512.5)
        self.assertEqual(len(reads), 1)                          # cached: no ledger walk per decision
        sn._bankroll_at -= S.Sniper.BANKROLL_STALE_S + 1
        sn._bankroll_now()
        self.assertEqual(len(reads), 2)                          # stale: read again
        live = type("Live", (), {"balance": 77.0, "attach": lambda self, s: None})()
        with patch.dict(os.environ, {"SNIPER_FAIR": "1", "SNIPER_KELLY": "1"}):
            sn2 = S.Sniper(None, None, settings=S.Settings(), live=live)
        self.assertEqual(sn2._bankroll_now(), 77.0)              # live: the canary's collateral reading
        live.reserved = 30.0                                     # an order in flight holds $30 of it
        self.assertEqual(sn2._bankroll_now(), 47.0)
        live.balance, live.reserved = 50.0, 0.0                  # the sender booked it: read on every decision
        self.assertEqual(sn2._bankroll_now(), 50.0)

    def test_paper_spends_leave_the_cash_at_once_until_a_reading_holds_them(self):
        with patch.dict(os.environ, {"SNIPER_FAIR": "1", "SNIPER_KELLY": "1", "SNIPER_LATENCY_MS": "200"}):
            sn = S.Sniper(None, type("B", (), {"cash_balance": lambda self: 1000.0})(), settings=S.Settings())
        self.assertEqual(sn._bankroll_now(), 1000.0)
        sn._bankroll_spent(60.0)
        self.assertEqual(sn._bankroll_now(), 940.0)                 # at once, not at the next reading
        now = time.monotonic()
        sn._set_bankroll(1000.0, now)                            # a reading the fill had not reached yet
        self.assertEqual(sn._bankroll, 940.0)
        sn._set_bankroll(940.0, now + S.Sniper.BANKROLL_SETTLE_S + 0.1)   # a later one that holds it
        self.assertEqual(sn._bankroll, 940.0)
        self.assertEqual(sn._spends, [])
        sn._set_bankroll(None, now + 10)
        self.assertIsNone(sn._bankroll)                          # no reading: Kelly skips, never guesses


class Feed(unittest.TestCase):
    def test_the_trigger_feed_feeds_the_volatility(self):
        with patch.dict(os.environ, {"SNIPER_FAIR": "1", "SNIPER_LATENCY_MS": "200"}):
            sn = S.Sniper(None, None, settings=S.Settings())
        sn._fire_now = lambda *a, **k: None
        for i, mid in enumerate((60_000.0, 60_001.0, 60_002.0)):
            sn.on_coinbase(json.dumps({"type": "ticker", "sequence": i + 1, "product_id": "BTC-USD",
                                       "best_bid": f"{mid - 0.01:.2f}", "best_ask": f"{mid + 0.01:.2f}"}))
        self.assertAlmostEqual(sn.vol_model.last_mid, 60_002.0)
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

    def test_the_trigger_panel_shows_the_last_decision_or_why_not(self):
        on = self.text({"vol_bp": 0.45, "why": None,
                        "last": {"side": "UP", "market_mid": 0.22, "fair": 0.2586, "limit": 0.23, "shares": 20.0}})
        self.assertIn("vol 0.45bp  UP .22->.26 <=.23 20sh", on)
        self.assertIn("unknown: volatility warming up - no buys", self.text({"vol_bp": None,
                                                                            "why": "volatility warming up"}))
        off = self.text(None)
        for t in (on, off):                                     # the order / latency line still fits
            self.assertIn("order     FAK", t)


class Settings(unittest.TestCase):
    def env(self, **kw):
        base = {k: v for k, v in os.environ.items() if not k.startswith("SNIPER_")}
        return patch.dict(os.environ, {**base, **kw}, clear=True)

    def test_on_by_default_and_in_the_frozen_spec(self):
        with self.env(SNIPER_ONLY="1"):
            s = S.Settings()
            self.assertEqual(X.spec_violations(s, 25), [])
        self.assertEqual((s.fair, s.fair_margin, s.kelly, s.kelly_fraction, s.max_target_shares),
                         (True, 0.01, True, 0.25, 20.0))
        self.assertTrue(s.hybrid_sizing)
        self.assertFalse(s.kelly_min_entry)       # off by default since 2026-10-25 (AUDIT_20261023.md M3)
        self.assertEqual((s.hybrid_policy.risk_fraction, s.hybrid_policy.max_order_usd), (0, 0))
        spec = dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)
        self.assertEqual((spec["fair"], spec["fair_margin"], spec["kelly"], spec["kelly_fraction"]),
                         (True, 0.01, True, 0.25))
        for line in (s.lines()[0], s.lines(live=True)[0]):
            self.assertIn("market-anchored fair value - fee - $0.01", line)
            self.assertIn("size 0.25 Kelly", line)

    def test_live_refuses_them_off_or_changed(self):
        for kw, word in (({"SNIPER_FAIR": "0"}, "fair value"), ({"SNIPER_FAIR_MARGIN": "0.02"}, "fair margin"),
                         ({"SNIPER_KELLY": "0"}, "kelly size"), ({"SNIPER_KELLY_FRACTION": "1"}, "kelly fraction"),
                         ({"SNIPER_KELLY_FRACTION": "0.5"}, "kelly fraction"),
                         ({"SNIPER_HYBRID_SIZING": "0"}, "hybrid sizing")):
            with self.subTest(kw), self.env(SNIPER_ONLY="1", **kw):
                bad = X.spec_violations(S.Settings(), 25)
            self.assertTrue(any(word in v for v in bad), bad)

    def test_bad_values_stop_the_start(self):
        for kw in ({"SNIPER_FAIR": "maybe"}, {"SNIPER_FAIR_MARGIN": "-0.01"}, {"SNIPER_FAIR_VOL_S": "30"},
                   {"SNIPER_FAIR_FEE_RATE": "0.5"}, {"SNIPER_KELLY": "2"}, {"SNIPER_KELLY_FRACTION": "0"},
                   {"SNIPER_KELLY_FRACTION": "1.5"}):
            with self.subTest(kw), self.env(**kw), self.assertRaises(ValueError):
                S.Settings()


if __name__ == "__main__":
    unittest.main(verbosity=2)
