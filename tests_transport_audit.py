"""Offline transport, collateral-race, freshness and shutdown regressions."""
import testing_env  # noqa: F401 - isolate credentials before bot imports

import asyncio
import json
import pathlib
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import patch

import h2.config
import h2.connection
import h2.errors
import h2.events

import fast_h2 as H
import sniper_live as L
import sniper_mode as S
import tests_sniper_live as T
import durable_io


class MemoryTransport:
    def __init__(self, fail_write=False):
        self.writes = []
        self.closed = False
        self.fail_write = fail_write

    def write(self, data):
        self.writes.append(data)
        if self.fail_write:
            raise OSError("write failed after bytes were handed off")

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed


class ProtocolAudit(unittest.IsolatedAsyncioTestCase):
    def proto(self):
        client = H.FastH2("http://127.0.0.1:12345")
        p = H._Proto(client)
        p.connection_made(MemoryTransport())
        client._proto = p
        return p

    async def test_timeout_releases_stream_without_retry(self):
        p = self.proto()
        s = p.request("POST", "/order", {}, b"{}")
        with self.assertRaises(asyncio.TimeoutError):
            await s.response(0.005)
        self.assertEqual(p.streams, {})
        self.assertTrue(s.future.cancelled())
        self.assertTrue(p.alive)
        self.assertEqual(len(p.transport.writes), 3)  # preface, POST, RST_STREAM

    async def test_cancellation_releases_stream(self):
        p = self.proto()
        s = p.request("POST", "/order", {}, b"{}")
        task = asyncio.create_task(s.response(None))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(p.streams, {})
        self.assertTrue(s.future.cancelled())

    async def test_write_failure_is_ambiguous_and_cannot_be_retried(self):
        p = self.proto()
        p.transport.fail_write = True
        s = p.request("POST", "/order", {}, b"{}")
        with self.assertRaises(ConnectionError):
            await s.response(0.1)
        self.assertFalse(p.alive)
        self.assertTrue(p.transport.closed)
        self.assertTrue(any(n.endswith("send_request_headers.started") for n, _ in s.events))
        with self.assertRaises(H.NotConnected):
            p.request("POST", "/order", {}, b"{}")

    async def test_goaway_settles_all_waiters_with_correct_uncertainty(self):
        p = self.proto()
        one = p.request("POST", "/order", {}, b"{}")
        two = p.request("POST", "/order", {}, b"{}")
        server = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False))
        server.initiate_connection()
        server.close_connection(last_stream_id=one.sid)
        p.data_received(server.data_to_send())
        with self.assertRaises(ConnectionError) as older:
            await one.response(0.1)
        self.assertNotIsInstance(older.exception, H.NotProcessed)
        with self.assertRaises(H.NotProcessed):
            await two.response(0.1)
        self.assertEqual(p.streams, {})
        self.assertTrue(p.transport.closed)

    async def test_peer_stream_limit_is_definitely_not_sent(self):
        p = self.proto()
        p.conn.remote_settings.max_concurrent_streams = 0
        p.conn.remote_settings.acknowledge()
        writes = len(p.transport.writes)
        with self.assertRaises(H.NotProcessed):
            p.request("POST", "/order", {}, b"{}")
        self.assertEqual(len(p.transport.writes), writes)
        self.assertEqual(p.streams, {})

    async def test_malformed_response_status_fails_all_waiters(self):
        p = self.proto()
        s = p.request("POST", "/order", {}, b"{}")
        ev = h2.events.ResponseReceived(stream_id=s.sid, headers=[(":status", "broken")])
        with patch.object(p.conn, "receive_data", return_value=[ev]):
            p.data_received(b"response")
        with self.assertRaises(ConnectionError):
            await s.response(0.1)
        self.assertEqual(p.streams, {})
        self.assertFalse(p.alive)
        self.assertTrue(p.transport.closed)

    async def test_control_write_failure_fails_pending_order(self):
        p = self.proto()
        s = p.request("POST", "/order", {}, b"{}")
        p.transport.fail_write = True
        with patch.object(p.conn, "receive_data", return_value=[]), \
                patch.object(p.conn, "data_to_send", return_value=b"settings ack"):
            p.data_received(b"settings")
        with self.assertRaises(ConnectionError):
            await s.response(0.1)
        self.assertFalse(p.alive)
        self.assertTrue(p.transport.closed)

    async def test_exhausted_flow_control_window_is_definitely_not_sent(self):
        p = self.proto()
        p.conn.outbound_flow_control_window = 0
        writes = len(p.transport.writes)
        with self.assertRaises(H.NotProcessed):
            p.request("POST", "/order", {}, b"{}")
        self.assertEqual(len(p.transport.writes), writes)
        self.assertEqual(p.streams, {})

    async def test_origin_checks_include_scheme_default_port_and_credentials(self):
        h = H.FastH2("https://exchange.test:8443")
        for url in ("http://exchange.test:8443/order", "https://exchange.test/order",
                    "https://user@exchange.test:8443/order", "//other.test/order"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                h._path(url)
        self.assertEqual(h._path("https://exchange.test:8443/order"), "/order")

    async def test_close_during_connect_does_not_publish_new_connection(self):
        h = H.FastH2("http://127.0.0.1:12345")
        loop = asyncio.get_running_loop()
        original = loop.create_connection
        transport = MemoryTransport()

        async def connect(*args, **kwargs):
            await h.aclose()
            return transport, types.SimpleNamespace()

        loop.create_connection = connect
        try:
            with self.assertRaises(RuntimeError):
                await h._connect()
            self.assertTrue(transport.closed)
            self.assertFalse(h.connected)
        finally:
            loop.create_connection = original


class SenderAudit(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake = await T.FakeH2Clob().start()
        self.sender, self.rows = await T.make_fast(self.fake, limits=L.CanaryLimits(max_order_shares=25))

    async def asyncTearDown(self):
        await self.sender.http.aclose()
        await self.fake.stop()
        T.reset_ambiguity()

    def prepare(self):
        return self.sender.prepare(side="UP", token=T.UP, up=T.UP, down=T.DN,
                                   condition=T.COND, end=T.round_end(), amount=5, cap=0.52)

    async def test_unknown_start_exception_never_falls_back_to_second_post(self):
        p = self.prepare()
        calls = []

        def uncertain(*args):
            calls.append(1)
            raise OSError("uncertain write")

        self.sender.http.post_now = uncertain
        started = self.sender.start(p)
        self.assertIsNotNone(started)
        row = await self.sender.send(p, started)
        self.assertEqual(row["kind"], L.AMBIGUOUS)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.fake.orders, [])
        self.assertEqual(self.sender.limits.orders, 1)
        self.assertTrue(self.sender.kill.tripped)

    async def test_two_prepared_cold_orders_cannot_exceed_canary_order_cap(self):
        self.sender.limits.max_orders = 1
        p, other = self.prepare(), self.prepare()
        # Force the async dispatch fallback without changing the actual HTTP/2 transport.
        first = await self.sender.send(p)
        second = await self.sender.send(other)
        self.assertIn(first["kind"], (L.MATCHED, L.NO_MATCH))
        self.assertEqual(second["kind"], L.NOT_SENT)
        self.assertEqual(len(self.fake.orders), 1)
        self.assertEqual(self.sender.limits.orders, 1)

    async def test_known_unsent_start_does_not_refund_or_consume_another_order_budget(self):
        p = self.prepare()
        self.sender.limits.exposure_usd = 3
        self.sender.http._proto.conn.outbound_flow_control_window = 0
        row = await self.sender.send(p, self.sender.start(p))
        self.assertEqual(row["kind"], L.NOT_SENT)
        self.assertEqual(self.fake.orders, [])
        self.assertEqual(self.sender.limits.orders, 0)
        self.assertEqual(self.sender.limits.exposure_usd, 3)

    async def test_refreshed_rules_refuse_prepared_payload_before_wire(self):
        for field, value in (("tick", "0.001"), ("minimum", 10), ("taker_delay", True)):
            with self.subTest(field=field):
                original, at = self.sender.rules._rules[T.COND]
                p = self.prepare()
                if field == "taker_delay":
                    value = not bool(original.get(field))
                refreshed = dict(original, **{field: value})
                self.sender.rules._rules[T.COND] = (refreshed, at)
                row = await self.sender.send(p, self.sender.start(p))
                self.assertEqual(row["kind"], L.NOT_SENT)
                self.assertEqual(self.fake.orders, [])
                self.sender.rules._rules[T.COND] = (original, at)
                self.sender.consecutive_errors = 0

    async def test_trace_callback_failure_after_write_releases_stream(self):
        http = self.sender.http
        before = len(self.fake.orders)

        async def failed_trace(name, info):
            if name.endswith("send_request_headers.started"):
                raise RuntimeError("diagnostic callback failed")

        with self.assertRaises(RuntimeError):
            await http.post(self.sender.url, content=b"{}", extensions={"trace": failed_trace})
        self.assertEqual(http._proto.streams, {})
        # Cleanup cancels only the stream; it never repeats the POST.
        await asyncio.sleep(0.01)
        self.assertLessEqual(len(self.fake.orders) - before, 1)

    async def test_expired_prepared_order_is_refused_before_wire(self):
        p = self.prepare()
        p.end = time.time() - 1
        row = await self.sender.send(p, self.sender.start(p))
        self.assertEqual(row["kind"], L.NOT_SENT)
        self.assertEqual(self.fake.orders, [])
        self.assertEqual(self.sender.limits.orders, 0)

    async def test_guard_is_rechecked_after_async_connection_wait(self):
        p = self.prepare()
        real = self.sender.http._connect
        old = self.sender.http._proto
        old.transport.close()
        self.sender.http._proto = None
        gate = [True]
        p.send_guard = lambda: gate[0] or (_ for _ in ()).throw(L.Refused("coverage lost"))

        async def slow_connect():
            await real()
            gate[0] = False

        self.sender.http._connect = slow_connect
        row = await self.sender.send(p)
        self.assertEqual(row["kind"], L.NOT_SENT)
        self.assertEqual(self.fake.orders, [])
        self.assertEqual(self.sender.limits.exposure_usd, 0)

    async def test_failed_keep_warm_probe_reconnects_without_waiting_normal_interval(self):
        rows, recovered = [], asyncio.Event()
        calls = [0]

        async def get(*args, **kwargs):
            calls[0] += 1
            if calls[0] == 1:
                raise ConnectionError("idle connection lost")
            return types.SimpleNamespace(text=str(time.time()))

        def observe(row):
            rows.append(row)
            if row.get("ok"):
                recovered.set()

        with patch("feeds.supervisor.backoff_delay", return_value=0.001):
            task = asyncio.create_task(L.keep_warm(types.SimpleNamespace(get=get), L.ServerClock(),
                                                  "unused", every_s=30, on_event=observe))
            try:
                await asyncio.wait_for(recovered.wait(), 0.2)
            finally:
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
        self.assertEqual(calls[0], 2)
        self.assertIn("error", rows[0])
        self.assertTrue(rows[1]["ok"])


def execution(fetch):
    sender = types.SimpleNamespace(rules=types.SimpleNamespace(), guard=types.SimpleNamespace(),
                                   kill=L.KillSwitch(None))
    ex = L.LiveExecution(sender=sender, shim=types.SimpleNamespace(invalidate=lambda c: None),
                         ledger=None, receipt_sink=lambda r: True, balance_fetch=fetch, clock_url="unused")
    ex.balance, ex.balance_at = 100.0, time.monotonic()
    return ex


class CollateralAudit(unittest.IsolatedAsyncioTestCase):
    async def test_balance_read_cannot_refund_concurrent_order_debit(self):
        entered, release = threading.Event(), threading.Event()

        def read():
            entered.set()
            release.wait(2)
            return {"balance": 100, "allowance": 100}

        ex = execution(read)
        task = asyncio.create_task(ex._read_balance())
        while not entered.is_set():
            await asyncio.sleep(0.001)
        ex.balance -= 10
        ex._balance_revision += 1
        release.set()
        await task
        self.assertEqual(ex.balance, 90)

    async def test_unconfirmed_order_blocks_balance_refund(self):
        ex = execution(lambda: {"balance": 1000, "allowance": 1000})
        ex.awaiting["pending"] = {}
        await ex._read_balance()
        self.assertEqual(ex.balance, 100)

    async def test_invalid_balance_does_not_refresh_freshness(self):
        for invalid in (float("nan"), float("inf"), -1, "broken"):
            ex = execution(lambda: {"balance": invalid, "allowance": 100})
            at = ex.balance_at
            await ex._read_balance()
            self.assertEqual((ex.balance, ex.balance_at), (100, at))
            self.assertEqual(ex.n["balance read errors"], 1)

    async def test_cancelled_order_keeps_cash_debit_and_pauses_five_minutes(self):
        ex = execution(lambda: {})
        mono = types.SimpleNamespace(now=1000.0)
        ex.kill.mono = lambda: mono.now

        async def send(*args):
            await asyncio.Event().wait()

        ex.sender.send = send
        ex.reserved = 5
        p = types.SimpleNamespace(amount=5, reserve=5, client_order_id="cancelled-order")
        task = asyncio.create_task(ex.send(p))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(ex.balance, 95)
        self.assertEqual(ex.reserved, 0)
        self.assertTrue(ex.kill.tripped)
        mono.now += 300
        ex.kill.check()
        self.assertEqual(ex.balance, 95)


class DataAudit(unittest.TestCase):
    def test_concurrent_canary_save_cannot_overwrite_final_risk_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            target = pathlib.Path(directory) / "canary.json"
            session = L.CanarySession(target, "offline-wallet")
            session.session_id, session.started_wall = "offline-session", time.time()
            limits, kill = L.CanaryLimits(orders=1), L.KillSwitch(None)
            entered, release, finished = threading.Event(), threading.Event(), threading.Event()
            original_replace, calls, outcomes = durable_io.replace_atomic, [], []

            def delayed_replace(source, destination):
                calls.append(1)
                if len(calls) == 1:
                    entered.set()
                    release.wait(2)
                original_replace(source, destination)

            def save(final=False):
                outcomes.append(session.save(limits, kill))
                if final:
                    finished.set()

            with patch("durable_io.replace_atomic", side_effect=delayed_replace):
                first = threading.Thread(target=save)
                first.start()
                self.assertTrue(entered.wait(2))
                limits.orders = 2
                last = threading.Thread(target=save, args=(True,))
                last.start()
                try:
                    self.assertFalse(finished.wait(0.02))
                finally:
                    release.set()
                    first.join(2)
                    last.join(2)
            self.assertEqual(outcomes, [True, True])
            self.assertEqual(json.loads(target.read_text())["orders"], 2)

    def test_atomic_replace_retries_only_known_transient_windows_errors(self):
        temporary = PermissionError("sharing conflict")
        temporary.winerror = 32
        with patch("durable_io.os.replace", side_effect=[temporary, temporary, None]) as replace, \
                patch("durable_io.time.sleep") as sleep:
            durable_io.replace_atomic("source", "destination")
            self.assertEqual(replace.call_count, 3)
            self.assertEqual([c.args[0] for c in sleep.call_args_list], [0.005, 0.01])

    def test_persistent_atomic_replace_error_still_fails_closed(self):
        denied = PermissionError("persistent access denial")
        denied.winerror = 5
        with patch("durable_io.os.replace", side_effect=denied) as replace, patch("durable_io.time.sleep"):
            with self.assertRaises(PermissionError):
                durable_io.replace_atomic("source", "destination")
            self.assertEqual(replace.call_count, 4)
        with patch("durable_io.os.replace", side_effect=OSError("disk full")) as replace:
            with self.assertRaises(OSError):
                durable_io.replace_atomic("source", "destination")
            self.assertEqual(replace.call_count, 1)

    def test_modern_websockets_backlog_is_observed(self):
        ws = types.SimpleNamespace(recv_messages=types.SimpleNamespace(frames=[None] * 31))
        self.assertEqual(S.rx_backlog(ws), 31)
        fresh = S.BinanceFreshness(1000, 5000, 20)
        fresh.enabled = True
        fresh.on_connect()
        fresh.note_message(time.monotonic(), S.rx_backlog(ws))
        self.assertEqual(fresh.why_not(time.monotonic()), "binance receive backlog")

    def test_malformed_current_timestamp_invalidates_prior_freshness(self):
        f = S.BinanceFreshness(1000, 5000, 20, name="coinbase")
        f.enabled = True
        f.on_connect()
        now = time.monotonic()
        f.note_trade(time.time() * 1000 - 10, time.time() * 1000, now)
        self.assertIsNone(f.why_not(now))
        f.note_trade(None, time.time() * 1000, now)
        self.assertEqual(f.why_not(now), "coinbase age unknown")

    def test_naive_exchange_timestamp_is_not_trusted(self):
        self.assertIsNone(S._exchange_ms("2026-10-11T12:00:00"))
        self.assertIsNotNone(S._exchange_ms("2026-10-11T12:00:00Z"))

    def test_invalid_binance_quotes_never_trigger(self):
        settings = S.Settings()
        settings.primary_feed = "binance"
        sn = S.Sniper(None, None, settings=settings)
        fired = []
        sn._signal = lambda *args: fired.append(1)
        for bid, ask in ((True, 60000), (-1, 60000), (60000, 0), (60001, 60000), ("nan", 60000)):
            sn.on_tick(json.dumps({"b": bid, "a": ask}))
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["binance parse errors"], 5)


class ShutdownAudit(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_cancels_auxiliary_feeds_and_drains_orders(self):
        sn = object.__new__(S.Sniper)
        sn._tasks, sn._order_tasks, sn._closing = set(), set(), False
        events = []

        async def auxiliary():
            try:
                await asyncio.Event().wait()
            finally:
                events.append("feed stopped")

        async def order():
            await asyncio.sleep(0.01)
            events.append("fill booked")
            sn._spawn(asyncio.sleep(100))  # shutdown suppresses late markout tasks

        sn._spawn(auxiliary())
        sn._spawn(order(), order=True)
        await asyncio.sleep(0)
        await sn.stop()
        events.append("final snapshot")
        self.assertEqual(events, ["feed stopped", "fill booked", "final snapshot"])
        self.assertEqual(sn._tasks, set())


if __name__ == "__main__":
    unittest.main()
