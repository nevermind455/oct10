"""Durable live dispatch, canary reservations and prospective loss regressions.

Uses actual execution classes; socket integration cases use only loopback and
throwaway unfunded signatures. No wallet credentials or venue traffic.
"""
import testing_env  # noqa: F401 - isolate the bot's environment before imports

import asyncio
import json
import os
import stat
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import fast_h2 as H
import polymarket_trade as pt
import sniper_live as L
import tests_sniper_live as T
from accounting import Ledger


def prepared(key="intent-1", side="UP", amount=5.0):
    token = T.UP if side == "UP" else T.DN
    rules = dict(T.rules_fetch(T.COND, T.UP, T.DN), up_token=T.UP, down_token=T.DN)
    p = L.Prepared(side, token, T.COND, time.time() + 120, amount, .52, .52, "0.01", key,
                   json.dumps({"order": {"salt": key, "signature": "throwaway-public-signature"}}),
                   {}, rules, {"prepare_pc": time.perf_counter_ns()}, "FAK")
    p.reserve = amount * 1.07
    return p


def durable(session) -> dict:
    """What a restart would read now: the snapshot plus every journal record
    (outcomes are written by the session's writer thread - wait for it)."""
    session.flush()
    return L.CanarySession.durable_view(session.path)


class WarmMemoryVenue:
    """The synchronous transport boundary, with real Stream/Resp semantics."""
    connected = True

    def __init__(self, response=None, observe=None):
        self.response = T.MATCHED_REPLY if response is None else response
        self.observe = observe or (lambda: None)
        self.orders = []

    def post_now(self, url, body, headers):
        self.observe()
        self.orders.append(json.loads(body))
        future = asyncio.get_running_loop().create_future()
        stream = H.Stream(len(self.orders), future)
        now = time.perf_counter_ns()
        stream.events = [("http2.send_request_headers.started", now),
                         ("http2.send_request_body.complete", now),
                         ("http2.receive_response_headers.complete", now)]
        future.set_result(H.Resp(200, [], json.dumps(self.response).encode()))
        return stream


class DurableWarmDispatch(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        T.reset_ambiguity()
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(path=str(Path(self.tmp.name) / "ledger.json"))
        self.path = Path(self.tmp.name) / "session.json"
        self.limits = L.CanaryLimits(max_daily_loss_usd=100)
        self.kill = L.KillSwitch()
        self.session = L.CanarySession(self.path, "offline-wallet")
        self.session.open(self.limits, self.kill, self.ledger)
        self.venue = WarmMemoryVenue()
        self.sender = L.LeanSender.__new__(L.LeanSender)
        self.sender.pt, self.sender.http = pt, self.venue
        self.sender.url = "http://memory-venue.invalid/order"
        self.sender.limits, self.sender.kill = self.limits, self.kill
        self.sender.session = self.session
        self.sender.guard = L.VenueGuard(self.kill)
        self.sender.clock = L.ServerClock()
        now = time.time()
        self.sender.clock.add_sample(now, now, now, 0)
        self.sender.rules = types.SimpleNamespace(get=lambda *args: prepared().rules)
        self.sender.realized_loss = None
        self.sender.timeout_s = 1
        self.sender.consecutive_errors = 0
        self.sender.max_consecutive_errors = 3
        self.sender.journal = lambda row: None

    async def asyncTearDown(self):
        self.session.close()
        self.tmp.cleanup()
        T.reset_ambiguity()

    def reboot(self, *, new=False):
        limits, kill = L.CanaryLimits(), L.KillSwitch()
        session = L.CanarySession(self.path, "offline-wallet")
        self.session.flush()
        session.open(limits, kill, self.ledger, new_session=new)
        session.close()
        return session, limits, kill

    async def test_intent_is_durable_before_write_and_crash_restart_halts(self):
        observed = []

        def at_write():
            state = L.CanarySession.durable_view(self.path)        # the write-ahead record, read as a restart would
            observed.append(state)
            self.assertEqual(state["intents"]["intent-1"]["status"], "DISPATCHING")
            self.assertEqual((state["orders"], state["pending_fills"], state["exposure_usd"]), (1, 1, 5))
            self.assertEqual(state["intents"]["intent-1"]["reserve"], 5.35)

        self.venue.observe = at_write
        self.sender.start(prepared())  # simulate process loss before its response waiter runs
        _, limits, kill = self.reboot()
        self.assertEqual((limits.orders, limits.pending_fills, limits.exposure_usd), (1, 1, 5))
        self.assertTrue(kill.tripped)
        self.assertEqual(len(observed), 1)
        with self.assertRaises(L.Halted):
            kill.check()
        with self.assertRaisesRegex(RuntimeError, "unresolved"):
            self.reboot(new=True)

    async def test_fsync_stall_expiring_guard_sends_nothing_and_resolves_unsent_intent(self):
        p = prepared()
        original_append = self.session._durable_append

        def delayed_flush(line):
            result = original_append(line)
            p.end = time.time() - 1  # signal expires while the disk operation is outstanding
            return result

        with patch.object(self.session, "_durable_append", side_effect=delayed_flush):
            result = await self.sender.send(p, self.sender.start(p))
        self.assertEqual(result["kind"], L.NOT_SENT)
        self.assertEqual(self.venue.orders, [])
        self.assertEqual((self.limits.pending_fills, self.limits.exposure_usd), (0, 0))
        self.assertEqual(durable(self.session)["intents"][p.client_order_id]["status"], L.NOT_SENT)
        self.assertFalse(self.reboot()[2].tripped)

    @unittest.skipIf(os.name == "nt", "POSIX directory durability")
    async def test_directory_fsync_failure_halts_before_transport_write(self):
        original_fsync = os.fsync

        def fail_directory(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode):
                raise OSError("directory flush failed")
            return original_fsync(fd)

        # The segment an intent is appended to is created - its directory entry
        # flushed - before any intent can use it. A session whose new segment
        # cannot be made durable never opens: nothing can be sent at all.
        fresh = L.CanarySession(Path(self.tmp.name) / "other.json", "offline-wallet")
        with patch.object(L.os, "fsync", side_effect=fail_directory):
            with self.assertRaisesRegex(RuntimeError, "cannot be written"):
                fresh.open(L.CanaryLimits(), L.KillSwitch(), self.ledger)
        # With no durable segment (one lost, or never created), the first intent
        # creates one: a failed directory flush then halts before any write.
        self.session.flush()
        self.session._segment_path(self.session._segment).unlink()
        p = prepared()
        with patch.object(L.os, "fsync", side_effect=fail_directory):
            result = await self.sender.send(p, self.sender.start(p))
        self.assertEqual(result["kind"], L.NOT_SENT)
        self.assertEqual(self.venue.orders, [])
        self.assertTrue(self.kill.tripped)

    async def test_overlapping_sides_reserve_the_session_fill_cap_at_dispatch(self):
        self.limits.max_fills = 1
        first = prepared("first", "UP")
        stream = self.sender.start(first)
        second = prepared("second", "DOWN")
        refused = await self.sender.send(second, self.sender.start(second))
        accepted = await self.sender.send(first, stream)
        self.assertEqual(refused["kind"], L.NOT_SENT)
        self.assertEqual(accepted["kind"], L.MATCHED)
        self.assertEqual(len(self.venue.orders), 1)
        self.assertEqual((self.limits.fills, self.limits.pending_fills), (1, 0))

    async def test_definitive_miss_releases_potential_fill_and_survives_restart(self):
        self.venue.response = {"success": False, "errorMsg": "FOK orders are fully filled or killed"}
        p = prepared()
        result = await self.sender.send(p, self.sender.start(p))
        self.assertEqual(result["kind"], L.NO_MATCH)
        _, limits, kill = self.reboot()
        self.assertEqual((limits.pending_fills, limits.fills, limits.exposure_usd), (0, 0, 0))
        self.assertFalse(kill.tripped)

    async def test_snake_case_acknowledgement_retains_accepted_order_identifier(self):
        self.venue.response = {**T.MATCHED_REPLY, "order_id": "snake-order"}
        del self.venue.response["orderID"]
        p = prepared()
        result = await self.sender.send(p, self.sender.start(p))
        self.assertEqual((result["kind"], result["order_id"]), (L.MATCHED, "snake-order"))
        intent = durable(self.session)["intents"][p.client_order_id]
        self.assertEqual((intent["status"], intent["order_id"]), ("ACCEPTED", "snake-order"))

    async def test_legacy_unconfirmed_authorizations_halt_when_upgraded(self):
        self.ledger.authorize_order("legacy-pending", {
            "condition_id": T.COND, "token_id": T.UP, "window_end": T.round_end(),
            "requested_notional": 5, "fee_rate": .07, "fee_exponent": 1,
        })
        self.session.close()
        state = json.loads(self.path.read_text())
        state["version"] = 1
        state.pop("intents")
        state.pop("pending_fills")
        self.path.write_text(json.dumps(state))
        _, limits, kill = self.reboot()
        self.assertEqual((limits.orders, limits.exposure_usd), (1, 5))
        self.assertTrue(kill.tripped)
        # upgraded to the journal format; the file as it was is kept beside it
        self.assertEqual(json.loads(self.path.read_text())["version"], L.CanarySession.VERSION)
        self.assertTrue(self.path.with_name(self.path.name + ".v1.bak").exists())


class PreviousRunReconciliation(unittest.IsolatedAsyncioTestCase):
    """The restart halt: a previous run's unresolved order is settled by the
    venue's record of its own order hash - proven or still halted."""

    asyncSetUp, reboot = DurableWarmDispatch.asyncSetUp, DurableWarmDispatch.reboot

    async def asyncTearDown(self):
        # The restart sessions keep their journal segment open: close them before
        # the temporary directory goes (Windows cannot delete an open file, and
        # addCleanup callbacks run only after asyncTearDown).
        for session in getattr(self, "restarts", []):
            session.close()
        await DurableWarmDispatch.asyncTearDown(self)

    HASH = "0x" + "ab" * 32

    def crashed_run(self, response=None):
        """An order written to the wire, then the process lost before its reply;
        the restart's session, limits and kill switch (left open)."""
        p = prepared()
        p.order_hash = self.HASH
        self.sender.start(p)
        self.session.flush()
        limits, kill = L.CanaryLimits(max_daily_loss_usd=100), L.KillSwitch()
        session = L.CanarySession(self.path, "offline-wallet")
        session.open(limits, kill, self.ledger)
        self.restarts = getattr(self, "restarts", []) + [session]
        return p, session, limits, kill

    def execution(self, session, limits, kill, venue_order):
        ex = L.LiveExecution.__new__(L.LiveExecution)
        ex.session, ex.ledger = session, self.ledger
        ex.sender = types.SimpleNamespace(limits=limits, kill=kill)          # LiveExecution.kill is the sender's
        ex.order_status = venue_order if callable(venue_order) else (lambda h: venue_order)
        ex.n = __import__("collections").Counter()
        ex.events = []
        ex.on_event = lambda src, text, level: ex.events.append((text, level))
        ex.previous_run_delays_s = (0.0,)
        return ex

    def record(self, **kw):
        return {"id": self.HASH, "asset_id": T.UP, "market": T.COND, "side": "BUY", "status": "canceled",
                "size_matched": "0", "associate_trades": [], **kw}

    async def test_a_proven_zero_fill_lifts_the_halt_and_stays_lifted(self):
        p, session, limits, kill = self.crashed_run()
        self.assertTrue(any(r.startswith(L.CanarySession.PREVIOUS_RUN) for r in kill.reasons))
        ex = self.execution(session, limits, kill, self.record())
        await ex._reconcile_previous_run()
        self.assertFalse(kill.tripped, kill.reasons)
        self.assertEqual(session.unresolved(), [])
        self.assertEqual((limits.pending_fills, limits.exposure_usd), (0, 0))
        self.assertIn("halt lifted", ex.events[-1][0])
        session.flush()
        _, limits2, kill2 = self.reboot()                                 # and the next start is clean
        self.assertFalse(kill2.tripped, kill2.reasons)

    async def test_a_match_booked_under_its_authorization_is_confirmed(self):
        p, session, limits, kill = self.crashed_run()
        self.ledger.authorize_order(self.HASH, {
            "condition_id": T.COND, "token_id": T.UP, "window_end": T.round_end(),
            "requested_notional": 5, "fee_rate": .07, "fee_exponent": 1, "trade_ids": ["t-prev-1"]})
        self.assertTrue(self.ledger.record_fill("t-prev-1", T.UP, shares=9.0, price=.5, condition_id=T.COND,
                                                fee=.1, order_id=self.HASH))
        ex = self.execution(session, limits, kill, self.record(status="matched", size_matched="9",
                                                               associate_trades=["t-prev-1"]))
        await ex._reconcile_previous_run()
        self.assertFalse(kill.tripped, kill.reasons)
        self.assertEqual(session.intents[p.client_order_id]["status"], "CONFIRMED")
        self.assertAlmostEqual(session.intents[p.client_order_id]["spent"], 4.5)

    async def test_a_match_not_yet_booked_stays_halted(self):
        p, session, limits, kill = self.crashed_run()
        ex = self.execution(session, limits, kill, self.record(status="matched", size_matched="9",
                                                               associate_trades=["t-prev-1"]))
        await ex._reconcile_previous_run()
        self.assertTrue(kill.tripped)
        self.assertEqual(len(session.unresolved()), 1)
        self.assertIn("not in the ledger yet", ex.events[-1][0])

    async def test_a_record_of_another_order_or_none_is_never_evidence(self):
        for venue, why in ((self.record(asset_id=T.DN), "does not match"),
                           ({}, "no terminal venue record"),
                           (self.record(status="live"), "no terminal venue record")):
            with self.subTest(why=why):
                p, session, limits, kill = self.crashed_run()
                ex = self.execution(session, limits, kill, venue)
                await ex._reconcile_previous_run()
                self.assertTrue(kill.tripped)
                self.assertIn(why, ex.events[-1][0])
                session.close()
                self.path.unlink()                                       # a fresh session for the next case
                for seg in self.path.parent.glob(self.path.name + ".*"):
                    seg.unlink()
                self.session = L.CanarySession(self.path, "offline-wallet")
                self.session.open(self.limits, L.KillSwitch(), self.ledger)
                self.sender.session = self.session

    async def test_an_expired_market_alone_never_releases_an_unresolved_order(self):
        """Scenario 15: the round of an order whose outcome is unknown has ended.
        Expiry is no evidence: without the venue's terminal record it stays
        unresolved, its worst case reserved, the start-up halt in place."""
        p, session, limits, kill = self.crashed_run()
        for key, _intent in session.unresolved():
            session.intents[key]["end"] = time.time() - 600                    # the market expired 10 min ago
        exposure = limits.exposure_usd
        ex = self.execution(session, limits, kill, {})                          # the venue has no record (yet)
        await ex._reconcile_previous_run()
        self.assertTrue(kill.tripped)
        self.assertEqual(len(session.unresolved()), 1)
        self.assertEqual(limits.exposure_usd, exposure)                         # nothing released by the clock

    async def test_nothing_is_sent_while_reconciling(self):
        p, session, limits, kill = self.crashed_run()
        before = len(self.venue.orders)
        ex = self.execution(session, limits, kill, self.record())
        await ex._reconcile_previous_run()
        self.assertEqual(len(self.venue.orders), before)


class LiveVenueAndRisk(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        T.reset_ambiguity()
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()
        self.ex, self.ledger, self.store, _, _ = await T.make_canary(self.fake, self.tmp.name)
        self.path = Path(self.tmp.name) / "session.json"
        self.ex.session = L.CanarySession(self.path, "offline-wallet")
        self.ex.session.open(self.ex.sender.limits, self.ex.kill, self.ledger)
        self.ex.sender.session = self.ex.session

    async def asyncTearDown(self):
        self.ex.session.close()
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        T.reset_ambiguity()

    def plan(self, *, side="UP", order_type="FAK", amount=5):
        token = T.UP if side == "UP" else T.DN
        book = T.live_book()
        return dict(T.live_plan(book.view(token), side=side, amount=amount), order_type=order_type)

    def trade(self, *, oid="0xorder1", tid="trade-1", shares=10, price=.5):
        self.store.record_trade(tid, order_id=oid, asset_id=T.UP, market=T.COND,
                                side="BUY", price=price, size=shares, status="CONFIRMED")

    async def test_actual_loopback_send_has_intent_while_response_is_outstanding(self):
        self.fake.script.append(("sleep", .15, ("json", 200, T.MATCHED_REPLY)))
        p = self.ex.prepare(self.plan())
        waiter = asyncio.create_task(self.ex.send(p))
        for _ in range(50):
            if self.fake.orders:
                break
            await asyncio.sleep(.005)
        self.assertEqual(len(self.fake.orders), 1)
        state = durable(self.ex.session)
        self.assertEqual(state["intents"][p.client_order_id]["status"], "DISPATCHING")
        self.assertEqual(state["pending_fills"], 1)
        limits, kill = L.CanaryLimits(), L.KillSwitch()
        self.ex.session.flush()
        L.CanarySession(self.path, "offline-wallet").open(limits, kill, self.ledger)
        self.assertTrue(kill.tripped)
        result = await waiter
        self.assertEqual(result["kind"], L.MATCHED)

    async def test_pending_fok_reserves_one_slot_and_confirmation_keeps_exactly_one(self):
        self.fake.script.append(("json", 200, {"success": True, "status": "delayed", "orderID": "0xorder1"}))
        result = await self.ex.send(self.ex.prepare(self.plan(order_type="FOK")))
        self.assertEqual(result["kind"], L.PENDING)
        self.assertEqual((self.ex.sender.limits.fills, self.ex.sender.limits.pending_fills), (1, 0))
        self.trade()
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        self.assertEqual(self.ex.sender.limits.fills, 1)
        self.assertEqual(self.ex.awaiting, {})
        self.assertEqual(next(iter(self.ex.session.intents.values()))["status"], "CONFIRMED")

    async def test_loss_budget_counts_fees_and_prepared_opposite_order(self):
        self.ex.sender.limits.max_daily_loss_usd = 10
        self.ex.prepare(self.plan())  # full all-in reservation is 5.35
        with self.assertRaisesRegex(L.Refused, "worst-case loss"):
            self.ex.prepare(self.plan(side="DOWN"))
        self.assertEqual(self.fake.orders, [])

    async def test_partial_confirmation_does_not_double_count_booked_basis(self):
        self.fake.script.append(("json", 200, {**T.MATCHED_REPLY, "tradeIDs": ["trade-1", "trade-2"]}))
        p = self.ex.prepare(self.plan())
        await self.ex.send(p)
        self.trade(shares=3)
        self.ledger.ingest_fill_store(self.store)
        self.assertAlmostEqual(self.ex._risk_budget_used(), p.reserve)
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), 10)
        self.ex.reconcile_once()
        self.assertIn("0xorder1", self.ex.awaiting)

    async def test_complete_confirmation_releases_unused_risk_and_settlement_releases_open_basis(self):
        p = self.ex.prepare(self.plan())
        await self.ex.send(p)
        self.trade(shares=3)
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        self.assertAlmostEqual(self.ex._risk_budget_used(), self.ledger.positions[T.UP].cost)
        self.assertEqual(self.ex._risk_orders, {})
        position = self.ledger.positions[T.UP]
        position.settled, position.settled_wall, position.realized = True, time.time(), 3 - position.cost
        self.assertEqual(self.ex._risk_budget_used(), 0)
        position.realized = -position.cost
        self.assertAlmostEqual(self.ex._risk_budget_used(), position.cost)

    async def test_fee_rule_change_after_fair_decision_refuses_before_signing(self):
        plan = self.plan()
        plan.update(fair_fee_rate=.25, fair_fee_exponent=1)
        with self.assertRaisesRegex(L.Refused, "fee rules changed"):
            self.ex.prepare(plan)
        self.assertEqual(self.fake.orders, [])

    async def test_late_execution_evidence_conflict_blocks_an_already_prepared_order(self):
        p = self.ex.prepare(self.plan())
        self.ledger.integrity_error = "trade cost disagrees with durable execution basis"
        row = await self.ex.send(p)
        self.assertEqual(row["kind"], L.NOT_SENT)
        self.assertEqual(self.fake.orders, [])
        self.assertTrue(self.ex.kill.tripped)
        self.assertTrue(any("execution evidence" in reason for reason in self.ex.kill.reasons))
        with self.assertRaises(L.Halted):
            self.ex.reconcile_once()

    async def test_authoritative_zero_fill_remains_refunded_and_resolved_after_restart(self):
        self.fake.script.append(("json", 200, {"success": True, "status": "delayed", "orderID": "0xorder1"}))
        await self.ex.send(self.ex.prepare(self.plan()))
        self.ex.awaiting["0xorder1"]["wall"] -= 35
        self.ex.order_status = lambda oid: {
            "id": oid, "asset_id": T.UP, "market": T.COND, "side": "BUY",
            "status": "CANCELED", "size_matched": "0", "associate_trades": [],
        }
        self.ex.reconcile_once()
        limits, kill = L.CanaryLimits(), L.KillSwitch()
        self.ex.session.flush()
        L.CanarySession(self.path, "offline-wallet").open(limits, kill, self.ledger)
        self.assertEqual((limits.orders, limits.fills, limits.pending_fills, limits.exposure_usd), (1, 0, 0, 0))
        self.assertFalse(kill.tripped)

    async def test_partial_unused_spend_remains_refunded_after_restart(self):
        await self.ex.send(self.ex.prepare(self.plan()))
        self.trade(shares=3)
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        limits, kill = L.CanaryLimits(), L.KillSwitch()
        self.ex.session.flush()
        L.CanarySession(self.path, "offline-wallet").open(limits, kill, self.ledger)
        self.assertAlmostEqual(limits.exposure_usd, 1.5)
        self.assertEqual((limits.fills, limits.pending_fills), (1, 0))
        self.assertFalse(kill.tripped)


if __name__ == "__main__":
    unittest.main()
