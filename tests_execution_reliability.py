#!/usr/bin/env python3
"""Execution reliability fixes (2026-10-23, audit AUDIT_20261023.md H1 H2 H3 M1 M2).

H1  An unknown-outcome FAK (a send timeout) is settled from the account's own trade
    record of its market once the market has closed - no trade of the order, or only
    FAILED ones: nothing executed; its trades: it executed. Before, only get_order
    counted, which the CLOB answers null for FAKs: 5 timeouts of 10-16..10-20 held
    $27.10 of the loss budget for good and halted every restart for 5 minutes.
H2  An accepted FAK whose fill size is seconds away blocks only its token; all orders
    pause only when it is still unknown after PENDING_FAK_HALT_S (20 global 5-minute
    pauses, 100 min, 10-15..10-21, while sizes arrived in p50 1.0s, max 9s).
H3  Slow venue replies (> 1.5s) pause new orders after 3 within a rolling 30 minutes
    (the operator's 2026-10-23 choice); one reply over 5s still pauses at once. One
    slow reply used to pause: 101 pauses, 8.4 hours, 10-15..10-21.
M1  SNIPER_SEND_TIMEOUT_S (default 10s), SNIPER_GUARD_STRIKES, SNIPER_GUARD_WINDOW_S.
M2  An unexpected order-path failure is a visible, journalled pause - never a silent
    stop of all decisions; a failed dispatch is held as an unknown outcome (reserved,
    token blocked, reconciled - never resent).

Loopback fake CLOB and throwaway signatures only: no wallet, no venue.

    python tests_execution_reliability.py
"""
from __future__ import annotations

import asyncio
import inspect
import json
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import Mock

import testing_env  # noqa: F401 - never load keys or contact a venue
import polymarket_trade as pt
import sniper_live as L
import tests_execution_weakness_fixes as W
import tests_hybrid_strategy as HS
import tests_live_20261014_incident as Inc
import tests_live_20261016_cash_freeze as CF
import tests_sniper_live as T
from accounting import Ledger
from feeds.poly_user import FillStore


def trade_row(order_hash, *, tid="t-h1", token=None, market=None, status="CONFIRMED", size="4", price="0.33",
              maker=False):
    row = {"id": tid, "market": market or T.COND, "asset_id": token or T.UP, "side": "BUY", "price": price,
           "size": size, "status": status, "fee_rate_bps": "0", "maker_orders": [],
           "taker_order_id": "0x" + "ee" * 32 if maker else order_hash, "trader_side": "TAKER"}
    if maker:
        row["maker_orders"] = [{"order_id": order_hash, "asset_id": token or T.UP, "price": price,
                                "matched_amount": size, "side": "SELL"}]
    return row


# =========================================================================== H1: the evidence
class TradeHistoryVerdict(unittest.TestCase):
    HASH = "0x" + "ab" * 32

    def ex(self, rows):
        ex = L.LiveExecution.__new__(L.LiveExecution)
        self.calls = []

        def account_trades(market, after):
            self.calls.append((market, after))
            if isinstance(rows, Exception):
                raise rows
            return rows
        ex.account_trades = account_trades
        return ex

    def verdict(self, rows, *, closed_ago=600.0, sent_ago=900.0):
        now = time.time()
        return self.ex(rows)._trade_history_verdict(order_hash=self.HASH, token=T.UP, condition=T.COND,
                                                    end=now - closed_ago, sent_wall=now - sent_ago)

    def test_nothing_settles_before_the_market_has_closed_long_enough(self):
        self.assertIsNone(self.verdict([], closed_ago=10.0))
        self.assertEqual(self.calls, [])                                  # not even asked

    def test_no_trade_of_the_order_after_the_close_is_zero_execution(self):
        self.assertEqual(self.verdict([trade_row("0x" + "11" * 32)]), (L.NO_MATCH, []))   # another order's trade
        market, after = self.calls[0]
        self.assertEqual(market, T.COND)
        self.assertLessEqual(after, time.time() - 900.0 - 60.0)           # the record from before the send

    def test_only_failed_trades_of_the_order_is_zero_execution(self):
        kind, rows = self.verdict([trade_row(self.HASH, status="FAILED")])
        self.assertEqual(kind, L.NO_MATCH)

    def test_a_trade_of_the_order_is_an_execution(self):
        kind, rows = self.verdict([trade_row(self.HASH), trade_row("0x" + "11" * 32, tid="other")])
        self.assertEqual((kind, [r["id"] for r in rows]), (L.MATCHED, ["t-h1"]))

    def test_a_trade_of_the_order_on_another_token_or_as_a_maker_is_a_conflict(self):
        with self.assertRaises(RuntimeError):
            self.verdict([trade_row(self.HASH, token=T.DN)])
        with self.assertRaises(RuntimeError):
            self.verdict([trade_row(self.HASH, maker=True)])              # a FAK never rests on the book

    def test_a_failed_or_malformed_lookup_is_never_evidence(self):
        with self.assertRaises(ConnectionError):
            self.verdict(ConnectionError("venue unreachable"))
        with self.assertRaises(TypeError):
            self.verdict({"data": []})


# =========================================================================== H1: this run
class TimeoutThisRun(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan, trade = Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan, Inc.Live.trade

    def arm(self, trades):
        ex = self.ex
        ex.sender.timeout_s = 0.2
        ex.order_status = lambda h: None                                  # the CLOB's null for a FAK
        ex.ambiguity_delays_s = (0.01,)
        ex.HISTORY_AFTER_END_S = -3600.0                                  # the market's record is complete now
        ex.HISTORY_RETRY_S = 0.01
        self.calls = []

        def account_trades(market, after):
            self.calls.append((market, after))
            value = trades(len(self.calls)) if callable(trades) else trades
            if isinstance(value, Exception):
                raise value
            return value
        ex.account_trades = account_trades

    async def timeout(self):
        self.fake.script.append(("hang", 1.0))                           # no reply within the timeout
        p = self.ex.prepare(self.plan(3.0))
        self.hash = p.order_hash
        row = await self.ex.send(p)
        self.assertEqual(row["kind"], L.AMBIGUOUS)
        self.assertIn(p.client_order_id, self.ex._risk_orders)
        return p

    async def settle(self):
        await asyncio.wait_for(asyncio.gather(*list(self.ex._order_tasks), return_exceptions=True), 10)

    def by_hand(self):
        return [r for r in self.ex.kill.reasons if "reconcile by hand" in r]

    async def test_a_timeout_with_no_trade_in_the_record_releases_its_reservation(self):
        self.arm([])
        p = await self.timeout()
        await self.settle()
        self.assertNotIn(p.client_order_id, self.ex._risk_orders)
        self.assertEqual(self.ex.n["unknown outcome resolved NO_MATCH"], 1)
        self.assertEqual(self.by_hand(), [])
        self.assertEqual(self.ex.sender.unresolved, {})
        self.assertFalse(pt._ambiguous_blocks(p.condition, p.token))
        self.assertEqual(self.ex._risk_budget_used(), 0.0)

    async def test_lookups_that_fail_first_are_retried_until_one_answers(self):
        self.arm(lambda n: ConnectionError("venue unreachable") if n <= 2 else [])
        p = await self.timeout()
        await self.settle()
        self.assertEqual(self.ex.n["trade history errors"], 2)
        self.assertNotIn(p.client_order_id, self.ex._risk_orders)

    async def test_a_partial_fill_found_in_the_record_is_authorized_with_its_exact_size(self):
        self.arm(lambda n: [trade_row(self.hash, size="4", price="0.33")])
        p = await self.timeout()
        await self.settle()
        a = self.ex.awaiting[self.hash]
        self.assertEqual((a["trade_ids"], a["final_size"]), ({"t-h1"}, 4.0))
        self.assertIn("t-h1", self.store.fills)                          # in the fill store for the ledger
        fill = self.ex.fill_for(self.hash)
        self.assertEqual((fill["shares"], fill["statuses"]), (4.0, ["CONFIRMED"]))
        self.assertEqual(self.ex.n["unknown outcome resolved MATCHED"], 1)

    async def test_a_record_that_never_answers_keeps_the_reservation_and_says_so(self):
        self.ex.HISTORY_ATTEMPTS = 3
        self.arm(ConnectionError("venue unreachable"))
        p = await self.timeout()
        await self.settle()
        self.assertEqual(self.ex.n["trade history errors"], 3)
        self.assertIn(p.client_order_id, self.ex._risk_orders)            # still reserved
        self.assertTrue(self.by_hand())

    async def test_a_conflicting_record_is_never_settled_automatically(self):
        self.arm(lambda n: [trade_row(self.hash, token=T.DN)])
        p = await self.timeout()
        await self.settle()
        self.assertIn(p.client_order_id, self.ex._risk_orders)
        self.assertTrue(self.by_hand())


# =========================================================================== H1: after a restart
class TimeoutAfterRestart(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, reboot, HASH, crashed_run = (W.DurableWarmDispatch.asyncSetUp, W.DurableWarmDispatch.reboot,
                                            W.PreviousRunReconciliation.HASH, W.PreviousRunReconciliation.crashed_run)
    asyncTearDown = W.PreviousRunReconciliation.asyncTearDown

    def execution(self, session, limits, kill, trades):
        ex = W.PreviousRunReconciliation.execution(self, session, limits, kill, lambda h: None)   # venue null
        ex.fills, ex.HISTORY_AFTER_END_S = FillStore(), -3600.0
        ex.account_trades = lambda market, after: trades
        return ex

    async def test_an_old_timeout_with_no_trade_is_resolved_and_the_start_up_halt_lifts(self):
        p, session, limits, kill = self.crashed_run()
        self.assertTrue(any(r.startswith(L.CanarySession.PREVIOUS_RUN) for r in kill.reasons))
        ex = self.execution(session, limits, kill, [])
        await ex._reconcile_previous_run()
        self.assertEqual(session.unresolved(), [])
        self.assertFalse(kill.tripped, kill.reasons)
        self.assertEqual(ex.n["previous run order resolved NO_MATCH"], 1)
        session.flush()
        _, _, kill2 = self.reboot()                                       # and the next start is clean
        self.assertFalse(kill2.tripped, kill2.reasons)

    async def test_an_old_order_the_record_shows_executed_stays_for_the_operator(self):
        p, session, limits, kill = self.crashed_run()
        ex = self.execution(session, limits, kill, [trade_row(self.HASH)])
        await ex._reconcile_previous_run()
        self.assertEqual(len(session.unresolved()), 1)                    # never authorized: by hand
        self.assertTrue(any("executed" in text for text, _level in ex.events), ex.events)

    async def test_ledger_lots_without_a_venue_trade_are_a_conflict_not_a_zero(self):
        p, session, limits, kill = self.crashed_run()
        self.ledger.authorize_order(self.HASH, {"condition_id": T.COND, "token_id": T.UP,
                                                "window_end": T.round_end(), "requested_notional": 5,
                                                "fee_rate": .07, "fee_exponent": 1})
        self.assertTrue(self.ledger.record_fill("t-x", T.UP, shares=9.0, price=.5, condition_id=T.COND,
                                                status="CONFIRMED", order_id=self.HASH))
        ex = self.execution(session, limits, kill, [])
        await ex._reconcile_previous_run()
        self.assertEqual(len(session.unresolved()), 1)


# =========================================================================== H2
class PendingFakIsTokenLocal(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan, trade = Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan, Inc.Live.trade
    matched = CF.CashFreeze.matched

    async def pending(self, oid="0xf1"):
        self.matched(oid)                                                 # totals, no trade ids: size pending
        p = self.ex.prepare(self.plan(CF.AMOUNT, cap=CF.CAP, ask=CF.PRICE))
        await self.ex.send(p)
        self.assertIn(oid, self.ex.fak_orders)
        self.assertIsNone(self.ex.fill_for(oid))
        return p

    async def test_a_fill_size_seconds_away_blocks_only_its_own_token(self):
        p = await self.pending()
        self.assertTrue(self.ex.blocks_pending_fak("0xf1"))
        self.assertFalse(self.ex.kill.tripped, self.ex.kill.reasons)      # no global pause
        with self.assertRaisesRegex(L.Refused, "awaiting its fill size"):
            self.ex.prepare(self.plan(3.0, side="UP", cap=CF.CAP, ask=CF.PRICE))
        q = self.ex.prepare(self.plan(3.0, side="DOWN", cap=CF.CAP, ask=CF.PRICE))   # the other token trades
        # ... with the pending order's worst case still in the cash and the loss budget
        self.assertIn(p.client_order_id, self.ex._risk_orders)
        self.assertGreaterEqual(self.ex._risk_budget_used(), p.reserve - 1e-9)
        self.assertAlmostEqual(self.ex.reserved, q.reserve, places=9)

    async def test_still_unknown_after_the_limit_pauses_all_trading_once(self):
        await self.pending()
        self.ex.fak_orders["0xf1"]["wall"] -= self.ex.PENDING_FAK_HALT_S + 1
        self.assertTrue(self.ex.blocks_pending_fak("0xf1"))
        self.assertTrue(self.ex.kill.tripped)
        reasons = list(self.ex.kill.reasons)
        self.assertTrue(self.ex.blocks_pending_fak("0xf1"))
        self.assertEqual(self.ex.kill.reasons, reasons)                   # once

    async def test_the_complete_fill_lifts_the_block_at_once_and_a_duplicate_counts_once(self):
        await self.pending()
        self.trade("t1", "0xf1", 6.0, CF.PRICE)
        self.assertTrue(self.ex.blocks_pending_fak("0xf1"))               # 6 of 10 shares: still pending
        self.trade("t2", "0xf1", 4.0, CF.PRICE, source="rest")            # the rest arrives after a reconnect
        self.trade("t2", "0xf1", 4.0, CF.PRICE)                           # and again on the socket
        self.assertFalse(self.ex.blocks_pending_fak("0xf1"))
        self.assertEqual(self.ex.fill_for("0xf1")["shares"], 10.0)

    async def test_pending_orders_in_several_markets_each_block_only_their_own(self):
        for i in (1, 2):
            self.ex.fak_orders[f"0xm{i}"] = {"wall": time.time(), "token": f"token-{i}", "condition": f"market-{i}",
                                             "client_order_id": f"k{i}", "kind": L.MATCHED, "amount": 3.0}
        self.assertTrue(self.ex.blocks_pending_fak("0xm1") and self.ex.blocks_pending_fak("0xm2"))
        self.assertFalse(self.ex.kill.tripped)
        self.ex.prepare(self.plan(3.0, side="UP", cap=CF.CAP, ask=CF.PRICE))   # a third market's token

    async def test_an_unknown_outcome_keeps_its_token_blocked_after_its_pause(self):
        oid = "0x" + "cd" * 32
        self.assertTrue(self.ex.blocks_pending_fak(oid, client_order_id="k-unknown"))
        for reason in list(self.ex.kill.reasons):                         # its pause has run out
            self.ex.kill.clear(reason)
        self.assertTrue(self.ex.blocks_pending_fak(oid, client_order_id="k-unknown"))
        self.assertFalse(self.ex.kill.tripped)                            # not paused again


# =========================================================================== H3
class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class SlowReplyStrikes(unittest.TestCase):
    def guard(self, **kw):
        self.clock, self.rows = Clock(), []
        return L.VenueGuard(L.KillSwitch(mono=self.clock), mono=self.clock, journal=self.rows.append, **kw)

    def allowed(self, g):
        try:
            g.check()
            return True
        except L.Refused:
            return False

    def test_defaults_three_slow_replies_within_thirty_minutes_pause_five(self):
        g = self.guard()
        self.assertEqual((g.strikes_to_long_pause, g.strike_window_s, g.long_pause_s), (3, 1800.0, 300.0))

    def test_zero_one_and_two_slow_replies_keep_trading(self):
        g = self.guard()
        self.assertEqual(g.on_reply(300), g.HEALTHY)
        self.assertTrue(self.allowed(g))
        for _ in range(2):
            self.clock.t += 60
            g.on_reply(1650)
            self.assertTrue(self.allowed(g), g.status())
        self.assertEqual(g.state, g.DEGRADED)

    def test_the_third_within_the_window_pauses_five_minutes_then_counts_afresh(self):
        g = self.guard()
        for _ in range(3):
            self.clock.t += 400
            g.on_reply(1650)
        self.assertFalse(self.allowed(g))
        self.assertEqual([(r["from"], r["to"]) for r in self.rows], [("HEALTHY", "DEGRADED"), ("DEGRADED", "PAUSED")])
        self.clock.t += 299
        self.assertFalse(self.allowed(g))
        self.clock.t += 1
        self.assertTrue(self.allowed(g))
        self.clock.t += 10
        g.on_reply(1650)                                                  # a fresh first strike
        self.assertTrue(self.allowed(g))

    def test_strikes_older_than_the_window_expire(self):
        g = self.guard()
        for at in (0, 1000, 1801):                                        # the first one is 1801s old
            self.clock.t = 1000.0 + at
            g.on_reply(1650)
        self.assertTrue(self.allowed(g), g.status())
        self.clock.t = 1000.0 + 1802
        g.on_reply(1650)                                                  # 1000, 1801, 1802: three
        self.assertFalse(self.allowed(g))

    def test_degraded_returns_to_healthy_when_its_strikes_expire(self):
        g = self.guard()
        g.on_reply(1650)
        self.assertEqual(g.state, g.DEGRADED)
        self.clock.t += 1801
        self.assertEqual(g.status()["state"], g.HEALTHY)

    def test_one_extreme_reply_still_pauses_at_once(self):
        g = self.guard()
        self.assertEqual(g.on_reply(5200), g.PAUSED)

    def test_simultaneous_replies_pause_once(self):
        g = self.guard()
        for _ in range(4):
            g.on_reply(1650)
        self.assertEqual(sum(1 for r in self.rows if r["to"] == "PAUSED"), 1)

    def test_a_restart_starts_with_no_strikes(self):
        g = self.guard()
        g.on_reply(1650)
        g.on_reply(1650)
        g = self.guard()                                                  # a new process: strikes are in memory
        g.on_reply(1650)
        self.assertTrue(self.allowed(g))

    def test_unknown_outcomes_are_not_slow_replies(self):
        kill = L.KillSwitch()
        sender = L.LeanSender.__new__(L.LeanSender)
        sender.pt, sender.kill, sender.guard = pt, kill, L.VenueGuard(kill)
        sender.limits, sender.session, sender.journal = L.CanaryLimits(), None, lambda row: None
        sender.rules = types.SimpleNamespace(invalidate=lambda *a: None)
        sender.consecutive_errors, sender.max_consecutive_errors, sender.ambiguity_resolver = 0, 3, None
        p = W.prepared()
        now = time.perf_counter_ns()
        st = {"prepare_pc": now, "net_send_start_pc": now, "response_pc": now + 2_000_000_000}   # 2s
        try:
            sender._after(p, {"kind": L.AMBIGUOUS, "error": "TimeoutError", "http_status": None,
                              "new_connection": False, "response": None}, dict(st))
            self.assertEqual(sender.guard.strikes, type(sender.guard.strikes)())
            sender._after(p, {"kind": L.NO_MATCH, "error": "no orders found to match", "http_status": 400,
                              "new_connection": False, "response": None}, dict(st))
            self.assertEqual(len(sender.guard.strikes), 1)
            self.assertNotEqual(sender.guard.state, sender.guard.PAUSED)   # one slow reply: no pause
        finally:
            pt._resolve_ambiguous(p.condition, p.token)


# =========================================================================== M1
class ExecutionSettings(unittest.TestCase):
    def test_defaults(self):
        self.assertEqual(L.execution_settings_from_env({}),
                         {"send_timeout_s": 10.0, "guard_strikes": 3, "guard_window_s": 1800.0})

    def test_bad_values_are_refused_with_their_name(self):
        for name, bad in (("SNIPER_SEND_TIMEOUT_S", "0"), ("SNIPER_SEND_TIMEOUT_S", "nan"),
                          ("SNIPER_SEND_TIMEOUT_S", "31"), ("SNIPER_GUARD_STRIKES", "0"),
                          ("SNIPER_GUARD_STRIKES", "2.5"), ("SNIPER_GUARD_WINDOW_S", "10"),
                          ("SNIPER_GUARD_WINDOW_S", "abc")):
            with self.assertRaisesRegex(ValueError, name):
                L.execution_settings_from_env({name: bad})

    def test_the_live_build_uses_them(self):
        src = inspect.getsource(L.build_live_execution)
        self.assertIn("execution_settings_from_env", src)
        self.assertIn("account_trades", src)
        example = (Path(__file__).resolve().parent / ".env.example").read_text(encoding="utf-8")
        for name in ("SNIPER_SEND_TIMEOUT_S", "SNIPER_GUARD_STRIKES", "SNIPER_GUARD_WINDOW_S"):
            self.assertIn(name, example)


# =========================================================================== M2
class ExecutionFaultsAreVisible(unittest.TestCase):
    def setUp(self):
        self.fixture = HS.HybridStrategy()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.enable_small_entry()
        self.sn = self.fixture.sn
        self.events = []
        self.sn.on_event = lambda src, text, level="info": self.events.append((text, level))
        self.plan, self.row = self.fixture.plan()
        self.assertIsNotNone(self.plan, self.row)
        self.key = self.plan["minimum_entry_key"]
        self.now = time.perf_counter_ns()

    def fire(self):
        return self.sn._fire_live("UP", 5, self.row, self.plan, self.now, self.now)

    def faults(self):
        return [r for r in self.fixture.rows if r.get("result") == "execution_fault"]

    def test_an_unexpected_preparation_failure_pauses_visibly_and_does_not_stop_decisions(self):
        self.sn.live = types.SimpleNamespace(prepare=Mock(side_effect=RuntimeError("builder failed")),
                                             kill=types.SimpleNamespace(trip=Mock()), status=lambda: {})
        with self.assertRaisesRegex(RuntimeError, "builder failed"):
            self.fire()
        self.assertFalse(self.sn._closing)                                # decisions continue after the pause
        self.assertNotIn(self.key, self.sn._minimum_entry_keys)
        self.assertEqual(self.sn.n["orders"], 0)
        (reason,), _ = self.sn.live.kill.trip.call_args
        self.assertIn("order preparation failed", reason)
        self.assertEqual([r["stage"] for r in self.faults()], ["prepare"])
        self.assertTrue(any(level == "bad" and "EXECUTION FAULT" in text for text, level in self.events))
        self.assertEqual(self.sn.summary_snapshot()["execution_faults"]["count"], 1)

    def test_a_failed_dispatch_is_held_as_an_unknown_order_never_resent(self):
        prepared = types.SimpleNamespace(send_guard=None)
        self.sn.live = types.SimpleNamespace(
            prepare=Mock(return_value=prepared), start_send=Mock(side_effect=OSError("write outcome unknown")),
            dispatch_unknown=Mock(), kill=types.SimpleNamespace(trip=Mock()))
        with self.assertRaisesRegex(OSError, "write outcome unknown"):
            self.fire()
        self.assertFalse(self.sn._closing)
        self.sn.live.dispatch_unknown.assert_called_once()
        self.assertIs(self.sn.live.dispatch_unknown.call_args[0][0], prepared)
        self.assertIn(self.key, self.sn._minimum_entry_keys)              # its claim is kept
        self.assertEqual(self.sn.n["orders"], 1)
        self.sn.live.start_send.assert_called_once()                      # sent at most once
        self.assertEqual([r["stage"] for r in self.faults()], ["dispatch"])

    def test_a_stop_without_a_kill_switch_is_visible(self):
        self.sn.live = None
        self.sn._stop("order worker failed: reconcile before trading")
        self.assertTrue(self.sn._closing)
        self.assertEqual(self.sn.summary_snapshot()["stopped"]["reason"],
                         "order worker failed: reconcile before trading")
        self.assertTrue(any(level == "bad" and "STOPPED" in text for text, level in self.events))


class DispatchUnknown(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan = Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan

    async def test_the_order_is_reserved_blocked_and_reconciled_and_the_cash_is_not_frozen(self):
        ex = self.ex
        ex.order_status, ex.account_trades = (lambda h: None), (lambda market, after: [])
        ex.ambiguity_delays_s, ex.HISTORY_AFTER_END_S, ex.HISTORY_RETRY_S = (0.01,), 3600.0, 0.01
        before = ex.balance
        p = ex.prepare(self.plan(3.0))
        ex.dispatch_unknown(p, OSError("write outcome unknown"))
        self.assertEqual(ex.reserved, 0.0)                               # not left "on the wire" (C1's ratchet)
        self.assertIn(p.client_order_id, ex._risk_orders)                # but its worst case is held
        self.assertAlmostEqual(ex.balance, before - p.reserve, places=9)
        self.assertTrue(pt._ambiguous_blocks(p.condition, p.token))
        self.assertTrue(ex.kill.tripped)
        self.assertIn(p.client_order_id, ex.sender.unresolved)
        await ex._read_balance()
        self.assertEqual(ex.last_balance_read["rule"], "held")           # wallet less the hold, not in_flight


if __name__ == "__main__":
    unittest.main()
