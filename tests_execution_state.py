#!/usr/bin/env python3
"""The execution fixes after the live audit:

  1. an exact 0.50bp move triggers (float tolerance, threshold unchanged)
  2. the stake is signed in whole cents (the SDK no longer floors $5.10 to $5.09)
  3. one order in flight PER SIDE: an outstanding UP order does not hold an
     independent DOWN signal, and each fill stays with its own order
  4. window_ms and fill confirmation timing on every order row

    python tests_execution_state.py

Paper cases run the real Sniper on the real PaperBroker; live cases the real
LiveExecution and LeanSender against a fake CLOB (tests_audit_live's
harness). No network.
"""
from __future__ import annotations

import asyncio
import collections
import json
import pathlib
import sys
import threading
import time
import types
import unittest
from decimal import Decimal as D
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_audit_live as A  # noqa: E402  (live harness)
import tests_sniper_ioc as I  # noqa: E402  (paper harness: Market, PaperIOCBase)
import tests_sniper_live as T  # noqa: E402


# =========================================================================== 1. the 0.5bp boundary
class BoundaryTests(unittest.TestCase):
    def move(self, ref, mid):
        t = S.Trigger(0.5, 250)
        t.update(0.0, ref)
        return t.update(260.0, mid)

    def test_exact_half_bp_moves_trigger_both_ways(self):
        for ref, mid, want in ((60_000.0, 60_003.0, 1), (60_000.0, 59_997.0, -1),
                               (48_740.0, 48_742.437, 1), (139_100.0, 139_106.955, 1)):
            self.assertEqual(self.move(ref, mid)[0], want, (ref, mid))

    def test_a_genuinely_smaller_move_still_does_not(self):
        # one 0.005 mid step below an exact 0.50bp move is 0.4999...bp by 8e-4bp, not 1e-12
        for ref, mid in ((60_000.0, 60_002.995), (60_000.0, 59_997.005), (100_000.0, 100_004.995)):
            d, r = self.move(ref, mid)
            self.assertIsNone(d, (ref, mid, r))
        self.assertLess(S.K_BP_TOLERANCE, 1e-6)

    def test_the_sweep_has_no_misses(self):
        import random
        rng, missed = random.Random(7), 0
        for _ in range(200_000):
            ref = D(rng.randrange(3_000_000, 15_000_000)) / D(100)
            for sign in (1, -1):
                mid = ref + sign * ref * D("0.00005")
                if mid != mid.quantize(D("0.005")):
                    continue
                if self.move(float(ref), float(mid))[0] != sign:
                    missed += 1
        self.assertEqual(missed, 0)


# =========================================================================== 2. the signed stake
class SignedStakeTests(unittest.TestCase):
    def amounts(self, stake, cap, tick):
        from py_clob_client_v2.order_builder.builder import ROUNDING_CONFIG, OrderBuilder
        from py_clob_client_v2.order_builder.constants import BUY
        _side, maker, taker = OrderBuilder.get_market_order_amounts(
            None, BUY, S.signing_amount(stake), S.signing_price(cap, tick), ROUNDING_CONFIG[tick])
        return D(maker), D(taker)

    def test_whole_cents_are_signed_exactly_and_never_past_the_ceiling(self):
        for tick in ("0.01", "0.001"):
            for cents in range(5, 96):
                ask = cents / 100
                cap = round(ask + 0.05, 4)
                for target in range(10, 21):
                    stake = S.Sniper.ioc_spend([(ask, 1000)], target, ask, cap)
                    maker, taker = self.amounts(stake, cap, tick)
                    self.assertEqual(maker, D(str(stake)) * 1_000_000, (tick, ask, target, stake))
                    self.assertLess(maker / taker, D(str(cap)) + D(tick))          # never the next tick
                    shares_at_ask = maker / D(1_000_000) / D(str(ask))
                    self.assertGreaterEqual(shares_at_ask, D(target), (ask, target))   # the target is reached
                    self.assertLess(shares_at_ask, D(target) + D("0.01") / D(str(ask)) + D("1e-9"))

    def test_float_noise_lands_on_the_intended_cent(self):
        for raw in (5.1, 5.1000000000000005, 5.0999999999999996, 4.85, 7.7, 1.14, 2.55):
            want = D(str(round(raw, 2)))
            maker, _ = self.amounts(raw, 0.60, "0.01")
            self.assertEqual(maker, want * 1_000_000, raw)

    def test_live_posts_the_whole_stake(self):
        async def go():
            case = A.LivePathBase()
            case.ASK = 0.51                                   # bid 0.50 under it, not the default 0.54
            await case.setup_live()
            try:
                case.reply()
                await case.tick(60_000.0, 60_003.6)
                body, _ = case.last_post()
                self.assertEqual(D(str(body["order"]["makerAmount"])), D("5100000"))   # 10 x 0.51, not 5.09
            finally:
                await case.teardown_live()
        asyncio.run(go())


# =========================================================================== 3. per-side execution state
class PaperPerSideTests(I.PaperIOCBase):
    """Real PaperBroker; latency long enough that two orders overlap."""

    def setUp(self):
        super().setUp()
        self.sn.s.fixed_latency, self.sn.s.latency_ms = True, 200.0

    def signals(self, steps):
        """(side, ms after start) signals through the real _signal gate; waits for the orders."""
        up = types.SimpleNamespace(update=lambda t, m: (1, 0.6), ref=None, q=collections.deque())
        down = types.SimpleNamespace(update=lambda t, m: (-1, -0.6), ref=None, q=collections.deque())

        async def go():
            t0 = time.monotonic()
            for side, at in steps:
                await asyncio.sleep(max(0.0, t0 + at / 1000.0 - time.monotonic()))
                self.sn._signal("binance", up if side == "UP" else down, 60_000.0, time.monotonic() * 1000.0, 0)
            for _ in range(300):
                if not self.sn.busy:
                    break
                await asyncio.sleep(0.01)
            return time.monotonic() - t0
        return asyncio.run(go())

    def orders(self):
        return [r for r in self.rows if r.get("result") in ("filled", "refused")]

    def test_up_in_flight_does_not_hold_down_and_fills_stay_with_their_order(self):
        self.m.set(I.UP, [(0.47, 6), (0.60, 100)])
        self.m.set(I.DOWN, [(0.47, 7), (0.60, 100)])
        took = self.signals([("UP", 0), ("DOWN", 20)])
        rows = {r["side"]: r for r in self.orders()}
        self.assertEqual(set(rows), {"UP", "DOWN"})
        self.assertLess(took, 0.39, "the two 200ms orders did not overlap")
        self.assertAlmostEqual(rows["UP"]["shares"], 6.0)               # each row has its own order's fill
        self.assertAlmostEqual(rows["DOWN"]["shares"], 7.0)
        self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 3.0})
        self.assertAlmostEqual(self.held(I.UP), 6.0)
        self.assertAlmostEqual(self.held(I.DOWN), 7.0)
        self.assertEqual(self.sn.rounds[self.m.window_end]["fills"], 2)
        self.assertNotEqual(rows["UP"]["order_id"], rows["DOWN"]["order_id"])

    def test_the_same_side_still_waits_for_its_own_order(self):
        self.sn.s.cooldown_ms = 0                             # isolate the in-flight gate from the cooldown
        self.signals([("UP", 0), ("UP", 50), ("DOWN", 60)])
        sides = sorted(r["side"] for r in self.orders())
        self.assertEqual(sides, ["DOWN", "UP"])
        self.assertEqual(self.sn.n["skip: previous order in flight"], 1)

    def test_the_broker_refuses_a_same_token_duplicate_but_not_the_other_token(self):
        results, barrier = {}, threading.Barrier(3)

        def place(name, side):
            barrier.wait()
            ok = self.broker.place_trade(side, 4.7, I.UP, I.DOWN, self.m.condition_id, self.m.window_end, 0.52, None,
                                         latency_ms=150.0, fak=True)
            results[name] = (ok, self.broker.last_error, (self.broker.last_fill or {}).get("token_id"))
        threads = [threading.Thread(target=place, args=a) for a in (("up1", "UP"), ("up2", "UP"), ("down", "DOWN"))]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        up = [results["up1"], results["up2"]]
        self.assertEqual(sorted(ok for ok, _e, _t in up), [False, True])              # one UP refused as a duplicate
        self.assertTrue(any("in flight on this token" in str(e) for ok, e, _t in up if not ok))
        self.assertTrue(results["down"][0], results["down"])                          # DOWN overlapped, filled
        self.assertEqual(results["down"][2], I.DOWN)                                  # its own outcome, on its thread
        self.assertEqual([t for ok, _e, t in up if ok], [I.UP])

    def test_the_round_fill_cap_counts_the_other_sides_order_in_flight(self):
        self.sn.rounds[self.m.window_end]["fills"] = self.sn.s.max_fills_round - 1
        self.signals([("UP", 0), ("DOWN", 20)])
        self.assertEqual([r["side"] for r in self.orders()], ["UP"])
        self.assertEqual(self.sn.n["skip: round cap"], 1)


class LivePerSideTests(A.LivePathBase):
    def test_down_while_up_quantity_pending_each_fill_to_its_order(self):
        async def go():
            await self.setup_live()
            try:
                up_oid, up_tid = self.reply()
                await self.tick(60_000.0, 60_003.6)                       # UP accepted, quantity pending
                self.assertIn(up_oid, self.sn._fak_pending)
                await self.tick(60_000.0, 60_003.6)                       # UP again: its own FAK holds it
                self.assertEqual(self.fake.hits[("POST", "/order")], 1)
                self.assertEqual(self.rows("skipped")[-1]["reason"], "awaiting FAK fill size")
                with self.assertRaises(L.Refused):                       # the executor refuses that side too
                    self.ex.prepare(dict(T.live_plan(self.book.view(T.UP), cap=0.6, amount=5.5,
                                                     end=self.hub.window_end), order_type="FAK"))
                dn_oid, dn_tid = self.reply()
                await self.tick(60_000.0, 59_996.4)                       # DOWN: free
                self.assertEqual(self.fake.hits[("POST", "/order")], 2)
                self.trade(dn_tid, dn_oid, 7, 0.51, token=T.DN)           # trades arrive out of order
                self.trade(up_tid, up_oid, 6, 0.55)
                self.sn._sync_live_fak()
                upd = {r["side"]: r for r in self.rows("live_fill_update")}
                self.assertEqual((upd["UP"]["order_id"], upd["UP"]["shares"]), (up_oid, 6.0))
                self.assertEqual((upd["DOWN"]["order_id"], upd["DOWN"]["shares"]), (dn_oid, 7.0))
                self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 3.0})
                self.assertEqual(self.ex.reserved, 0.0)
                self.assertEqual(self.ex.sender.limits.orders, 2)
            finally:
                await self.teardown_live()
        self.run_async(go())


# =========================================================================== 4. observability
class TimingTests(A.LivePathBase):
    def test_window_and_the_live_latency_split(self):
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)                       # reference 300ms before the tick
                r = self.rows("pending")[-1]
                self.assertAlmostEqual(r["window_ms"], 300.0, delta=5.0)
                self.assertEqual(r["ref_mid"], 60_000.0)
                for k in ("t_signal_to_send_ms", "t_send_to_response_ms", "t_signal_to_response_ms"):
                    self.assertIsInstance(r[k], float, k)
                self.assertAlmostEqual(r["t_signal_to_response_ms"],
                                       r["t_signal_to_send_ms"] + r["t_send_to_response_ms"], delta=0.05)
                self.assertNotIn("fill_confirmed_ms", r)                   # quantity not known yet
                await asyncio.sleep(0.15)                                 # the private trade comes 150ms later
                self.trade(tid, oid, 10, 0.55)
                await asyncio.sleep(0.05)                                 # ... and the bot books it 50ms after that
                self.sn._sync_live_fak()
                u = self.rows("live_fill_update")[-1]
                A.say(f"window {r['window_ms']}ms | signal->send {r['t_signal_to_send_ms']}ms | send->response "
                      f"{r['t_send_to_response_ms']}ms | signal->fill confirmed {u['fill_confirmed_ms']}ms | "
                      f"send->fill confirmed {u['t_send_to_fill_confirmed_ms']}ms | booked at "
                      f"{u['t_signal_to_fill_known_ms']}ms")
                self.assertGreater(u["fill_confirmed_ms"], r["t_signal_to_response_ms"] + 140)
                self.assertAlmostEqual(u["t_send_to_fill_confirmed_ms"],
                                       u["fill_confirmed_ms"] - r["t_signal_to_send_ms"], delta=0.05)
                self.assertGreater(u["t_signal_to_fill_known_ms"], u["fill_confirmed_ms"] + 40)
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_a_no_match_is_confirmed_by_the_reply(self):
        async def go():
            await self.setup_live()
            try:
                self.reply(status=400, body={"success": False, "errorMsg": "no orders found to match with FAK order"})
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("refused")[-1]
                self.assertEqual(r["fill_confirmed_ms"], r["t_signal_to_response_ms"])
                self.assertIn("window_ms", r)
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_paper_rows_carry_the_same_split(self):
        case = I.PaperIOCTests("test_full_fill")
        case.setUp()
        try:
            t = S.Trigger(case.sn.s.k_bp, case.sn.s.window_ms)
            now = time.monotonic() * 1000.0
            t.update(now - 300.0, 60_000.0)
            case.sn.trigger = t
            t.update(now, 60_003.6)                                        # leaves the reference in t.ref
            r = case.fire("UP", [(0.47, 100)])
            self.assertAlmostEqual(r["window_ms"], 300.0, delta=50.0)
            self.assertIsInstance(r["t_signal_to_send_ms"], float)
            self.assertGreaterEqual(r["t_send_to_fill_confirmed_ms"], 20.0 - 1.0)    # the modelled 20ms flight
            self.assertAlmostEqual(r["fill_confirmed_ms"], r["t_signal_to_send_ms"] + r["t_send_to_fill_confirmed_ms"],
                                   delta=0.05)
        finally:
            case.tearDown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
