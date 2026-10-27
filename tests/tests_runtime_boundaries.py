"""Offline regressions for cash, durable counters, disk locks and timing."""
import testing_env  # noqa: F401 - never load keys or contact a venue

import asyncio
import concurrent.futures
import json
from pathlib import Path
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import AsyncMock, patch

from accounting.ledger import Ledger
from feeds.poly_user import FillStore
import exec_latency as X
import sniper_live as L
import sniper_mode as S
import tests_execution_weakness_fixes as E
import tests_live_20261014_incident as Inc


def execution(ledger, store=None):
    sender = types.SimpleNamespace(kill=L.KillSwitch(), rules=types.SimpleNamespace(),
                                   guard=types.SimpleNamespace(), limits=L.CanaryLimits())
    shim = types.SimpleNamespace(invalidate=lambda *_: None)
    return L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=lambda _: True,
                           balance_fetch=lambda: {"balance": 100, "allowance": 100},
                           clock_url="unused", fills=store)


def intent(key):
    return L.Prepared("UP", "up", "condition", time.time() + 120, 5, .5, .5, ".01", key,
                      json.dumps({"order": {"salt": key}}), {}, {}, {}, "FAK")


class CashReservations(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ex = execution(Ledger("unused-ledger.invalid"))
        self.ex.balance = 100
        self.ex.balance_at = time.monotonic()
        self.p = intent("uncertain")
        self.p.reserve = 11
        self.ex.reserved = 11
        self.ex._risk_orders[self.p.client_order_id] = {
            "reserve": 11, "order_id": None, "token": "up", "condition": "condition", "shares": 20}

    async def test_ambiguous_order_cash_survives_repeated_wallet_refreshes(self):
        self.ex.sender.send = AsyncMock(return_value={"kind": L.AMBIGUOUS, "error": "unknown"})
        await self.ex.send(self.p)
        self.assertEqual((self.ex.balance, self.ex.reserved), (89, 0))
        for _ in range(3):
            await self.ex._read_balance()
            self.assertEqual(self.ex.balance, 89)
            self.assertIn(self.p.client_order_id, self.ex._risk_orders)

    async def test_interrupted_order_cash_survives_wallet_refresh(self):
        self.ex.sender.send = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await self.ex.send(self.p)
        await self.ex._read_balance()
        self.assertEqual(self.ex.balance, 89)

    async def test_payout_can_raise_cash_without_refunding_uncertain_order(self):
        self.ex.sender.send = AsyncMock(return_value={"kind": L.AMBIGUOUS, "error": "unknown"})
        await self.ex.send(self.p)
        self.ex.balance_fetch = lambda: {"balance": 130, "allowance": 130}
        await self.ex._read_balance()
        self.assertEqual(self.ex.balance, 119)

    async def test_multiple_orders_without_venue_ids_each_keep_their_reservation(self):
        self.ex.reserved = 0
        self.ex.balance = 78
        self.ex._risk_orders["another"] = dict(self.ex._risk_orders[self.p.client_order_id])
        await self.ex._read_balance()
        self.assertEqual(self.ex.balance, 78)

    async def test_authoritative_resolution_releases_cash_on_next_refresh(self):
        self.ex.reserved = 0
        self.ex.balance = 89
        self.ex._resolve_risk({"client_order_id": self.p.client_order_id}, filled=False)
        await self.ex._read_balance()
        self.assertEqual(self.ex.balance, 100)


class DurableState(unittest.TestCase):
    def test_periodic_state_cannot_overwrite_a_newer_intents_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session.json"
            session = L.CanarySession(path, "offline-wallet")
            limits, kill = L.CanaryLimits(), L.KillSwitch()
            ledger = Ledger(str(Path(directory) / "ledger.json"))
            session.open(limits, kill, ledger)
            self.addCleanup(session.close)
            first = intent("first")
            session.before_send(first, limits, kill)
            limits.record_outcome(L.NO_MATCH, 5)
            session.outcome(first, {"kind": L.NO_MATCH}, limits, kill)
            session.flush()
            captured, release, sent = threading.Event(), threading.Event(), threading.Event()
            persisted, errors = [], []
            original = session._counters

            def counters(value):
                result = original(value)
                if threading.current_thread().name == "persist-race":
                    captured.set()
                    if not release.wait(2):
                        raise AssertionError("counter race was not released")
                return result

            def send():
                session.before_send(intent("second"), limits, kill)
                sent.set()

            def persist():
                try:
                    persisted.append(session.persist(limits, kill))
                except BaseException as exc:
                    errors.append(exc)

            with patch.object(session, "_counters", side_effect=counters):
                with concurrent.futures.ThreadPoolExecutor(thread_name_prefix="send") as pool:
                    writer = threading.Thread(target=persist, name="persist-race")
                    writer.start()
                    try:
                        self.assertTrue(captured.wait(2))
                        sending = pool.submit(send)
                        sent.wait(.1)
                    finally:
                        release.set()
                        writer.join(3)
                    self.assertFalse(writer.is_alive())
                    self.assertFalse(errors, errors)
                    self.assertEqual(persisted, [True])
                    sending.result(timeout=3)
            session.flush()
            restored = L.CanarySession.durable_view(path)
            self.assertEqual((restored["orders"], restored["exposure_usd"]), (2, 5))
            session.close()
            reboot = L.CanarySession(path, "offline-wallet")
            recovered, restarted_kill = L.CanaryLimits(), L.KillSwitch()
            try:
                reboot.open(recovered, restarted_kill, ledger)
                self.assertEqual((recovered.orders, recovered.exposure_usd), (2, 5))
            finally:
                reboot.close()

    def test_authorization_disk_flush_does_not_block_inventory_reads(self):
        for existing_snapshot in (False, True):
            with self.subTest(existing_snapshot=existing_snapshot):
                self.check_inventory_during_flush(existing_snapshot)

    def check_inventory_during_flush(self, existing_snapshot):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(str(Path(directory) / "ledger.json"))
            if existing_snapshot:
                self.assertTrue(ledger.save())
            entered, release = threading.Event(), threading.Event()
            operation = "_append_journal" if existing_snapshot else "_write_payload"
            append = getattr(ledger, operation)

            def delayed(entry):
                entered.set()
                if not release.wait(2):
                    raise AssertionError("disk flush was not released")
                return append(entry)

            ex = execution(ledger)
            with patch.object(ledger, operation, side_effect=delayed), \
                    concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                worker = pool.submit(ledger.authorize_order_durable, "order-1")
                self.assertTrue(entered.wait(2))
                reader = pool.submit(ex.own_risk_shares, "up", "condition")
                try:
                    self.assertEqual(reader.result(timeout=.2), 0)
                finally:
                    release.set()
                self.assertTrue(worker.result(timeout=3))
            self.assertIn("order-1", Ledger(ledger.path).authorized_orders)


class Timing(unittest.TestCase):
    def test_decision_freezes_the_original_dispatch_deadline(self):
        import tests_hybrid_strategy as H
        fixture = H.HybridStrategy()
        fixture.setUp()
        try:
            fixture.refresh()
            plan, row = fixture.plan()
            self.assertIsNotNone(plan, row)
            self.assertEqual(plan["dispatch_deadline"], plan["end"] - fixture.sn._window()[1])
        finally:
            fixture.tearDown()

    def test_ack_budget_includes_both_network_legs(self):
        lat = {"feed_to_trigger": {"p50": 1}, "trigger_to_send": {"p50": 2},
               "send_to_ack": {"p50": 300, "simulated": False}}
        result = X.latency_budget({"coinbase": 50}, "coinbase", lat, 100)
        self.assertEqual(result["totals"]["coinbase"], 353)
        from dashboard.sniper_layout import _budget_body
        from dashboard.theme import UNICODE
        text = "\n".join("".join(run[0] for run in row) for row in _budget_body(result, 90, UNICODE))
        self.assertIn("Network < CLOB", text)
        self.assertIn("353ms", text)

    def test_simulated_match_budget_does_not_add_a_return_leg(self):
        lat = {"trigger_to_send": {"p50": 3}, "send_to_ack": {"p50": 300, "simulated": True}}
        result = X.latency_budget({"coinbase": 50}, "coinbase", lat, 100)
        self.assertEqual(result["totals"]["coinbase"], 353)

    def test_retrying_trade_uses_when_execution_became_known(self):
        store = FillStore()
        ex = execution(Ledger("unused-ledger.invalid"), store)
        ex.fak_orders["order-1"] = {"condition": "condition", "token": "up", "amount": 5,
                                    "kind": L.MATCHED, "trade_ids": {"trade-1"},
                                    "cap": .55, "fee_rate": .07, "fee_exponent": 1}
        fields = dict(order_id="order-1", asset_id="up", market="condition", side="BUY", size=10, price=.5)
        with patch("feeds.poly_user.time.monotonic", return_value=10):
            store.record_trade("trade-1", status="RETRYING", **fields)
        self.assertIsNone(ex.fill_for("order-1"))
        with patch("feeds.poly_user.time.monotonic", return_value=15):
            store.record_trade("trade-1", status="MATCHED", **fields)
        self.assertEqual(ex.fill_for("order-1")["seen_mono"], 15)
        with patch("feeds.poly_user.time.monotonic", return_value=20):
            store.record_trade("trade-1", status="CONFIRMED", **fields)
        self.assertEqual(ex.fill_for("order-1")["seen_mono"], 15)
        ex.fak_orders.clear()  # FOK/private-channel observations use the same clock.
        self.assertEqual(ex.fill_for("order-1")["seen_mono"], 15)

    def test_partial_trade_record_is_timestamped_when_quantity_arrives(self):
        store = FillStore()
        fields = dict(order_id="order-1", asset_id="up", market="condition", side="BUY", price=.5)
        with patch("feeds.poly_user.time.monotonic", return_value=10):
            store.record_trade("trade-1", status="MATCHED", **fields)
        self.assertIsNone(store.fills["trade-1"].execution_seen)
        with patch("feeds.poly_user.time.monotonic", return_value=15):
            store.record_trade("trade-1", size=10, status="MATCHED", **fields)
        self.assertEqual(store.fills["trade-1"].execution_seen, 15)

    def test_dispatch_cannot_outlive_the_original_trading_window(self):
        sn = object.__new__(S.Sniper)
        sn._closing = False
        sn.s = types.SimpleNamespace(stale_ms=2000)
        sn._window = lambda: (300, 10)
        plan = {"end": 1000, "dispatch_deadline": 990, "view": types.SimpleNamespace(updated_mono=50)}
        with patch("sniper_mode.time.monotonic", return_value=50.1), \
                patch("sniper_mode.time.time", return_value=990.1):
            with self.assertRaisesRegex(L.Refused, "window"):
                sn._check_dispatch_age(plan, None, None)


class DispatchWrites(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = E.DurableWarmDispatch.asyncSetUp
    asyncTearDown = E.DurableWarmDispatch.asyncTearDown

    def guard(self, p, clock):
        sn = object.__new__(S.Sniper)
        sn._closing = False
        sn.s = types.SimpleNamespace(stale_ms=2000)
        plan = {"end": p.end, "dispatch_deadline": clock[0] + .1,
                "view": types.SimpleNamespace(updated_mono=time.monotonic())}
        p.send_guard = lambda: sn._check_dispatch_age(plan, None, None)

    async def test_intent_flush_that_outlasts_window_prevents_warm_socket_write(self):
        p, clock = E.prepared(), [time.time()]
        self.guard(p, clock)
        before_send = self.session.before_send

        def delayed(*args):
            before_send(*args)
            clock[0] += .2

        with patch("sniper_mode.time.time", side_effect=lambda: clock[0]), \
                patch.object(self.session, "before_send", side_effect=delayed):
            stream = self.sender.start(p)
            result = await self.sender.send(p, stream)
        self.assertEqual(result["kind"], L.NOT_SENT)
        self.assertIn("window", result["error"])
        self.assertEqual(self.venue.orders, [])

    async def test_cold_connection_that_outlasts_window_prevents_socket_write(self):
        p, clock = E.prepared(), [time.time()]
        self.guard(p, clock)

        async def post(*args, **kwargs):
            clock[0] += .2
            kwargs["extensions"]["before_write"]()
            self.fail("transport must refuse before writing")

        self.sender.http = types.SimpleNamespace(post=post)
        with patch("sniper_mode.time.time", side_effect=lambda: clock[0]):
            result = await self.sender.send(p)
        self.assertEqual(result["kind"], L.NOT_SENT)
        self.assertIn("window", result["error"])
        self.assertEqual(self.venue.orders, [])


class LiveExecutionWindow(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan = Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan

    async def test_live_guard_independently_enforces_the_saved_decision_deadline(self):
        plan = self.plan(5, cap=.55, ask=.50)
        plan["dispatch_deadline"] = time.time() + .1
        p = self.ex.prepare(plan)
        with patch("sniper_live.time.time", return_value=plan["dispatch_deadline"] + .01):
            with self.assertRaisesRegex(L.Refused, "original decision window"):
                p.send_guard()
        self.assertEqual(self.fake.orders, [])

    async def test_live_guard_preserves_the_strategys_minimum_time_left(self):
        self.ex._sniper_settings = types.SimpleNamespace(min_left_s=10)
        plan = self.plan(5, cap=.55, ask=.50)
        plan["end"] = time.time() + 12
        p = self.ex.prepare(plan)
        with patch("sniper_live.time.time", return_value=plan["end"] - 9):
            with self.assertRaisesRegex(L.Refused, "expiry window"):
                p.send_guard()
        self.assertEqual(self.fake.orders, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
