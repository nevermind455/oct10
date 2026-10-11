"""Offline feed regressions: real loopback sockets and deterministic faults.

Run: python -m pytest -q tests_connection_audit.py
No exchange connections, credentials, orders, or durable bot state are used.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import statistics
import threading
import time
import unittest
from unittest import mock

import testing_env  # noqa: F401 - do not read production .env
import websockets

from feeds.binance import BinanceTradeFeed
from feeds.book import BookState
from feeds.health import DISCONNECTED, LIVE, STALE, UNSYNCED
from feeds.hub import FeedHub
from feeds.poly_market import PolyMarketFeed
from feeds.poly_user import PolyUserFeed


async def until(predicate, timeout=1.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition did not become true inside timeout")


class BookValidation(unittest.TestCase):
    def book(self):
        state = BookState(stale_after=0.01)
        state.connected = True
        state.set_active(["A"])
        self.assertTrue(state.apply_snapshot("A", [(0.4, 3)], [(0.6, 4)],
                                              ts_ms=int(time.time() * 1000)))
        return state

    def test_invalid_snapshot_timestamp_desyncs_without_parser_crash(self):
        for timestamp in (None, True, float("inf"), float("nan"), "x", 0):
            state = self.book()
            self.assertFalse(state.apply_snapshot("A", [], [], ts_ms=timestamp,
                                                   require_exchange_ts=True))
            self.assertEqual(state.view("A").status, UNSYNCED, timestamp)

    def test_malformed_level_collections_desync(self):
        for levels in (0, 5, "", "0.5", {}, {"price": "0.5"}, None):
            state = self.book()
            self.assertFalse(state.apply_snapshot("A", levels, []))
            self.assertEqual(state.view("A").status, UNSYNCED, levels)

    def test_malformed_delta_collections_desync(self):
        for changes in (1, "", "x", {}, {"asset_id": "A"}, None):
            state = self.book()
            self.assertFalse(state.apply_price_changes(changes,
                                                        ts_ms=int(time.time() * 1000)))
            self.assertEqual(state.view("A").status, UNSYNCED, changes)

    def test_duplicate_depth_overflow_is_rejected(self):
        state = self.book()
        self.assertFalse(state.apply_snapshot("A", [(0.4, 1e308), (0.4, 1e308)], []))
        self.assertEqual(state.view("A").status, UNSYNCED)

    def test_boolean_and_unrepresentable_numeric_depth_are_rejected(self):
        for value in (True, 10 ** 1000):
            state = self.book()
            self.assertFalse(state.apply_snapshot("A", [(0.4, value)], []))
            state = self.book()
            self.assertFalse(state.apply_price_changes(
                [{"asset_id": "A", "side": "SELL", "price": 0.6, "size": value}],
                ts_ms=int(time.time() * 1000)))
            self.assertEqual(state.view("A").status, UNSYNCED)

    def test_pre_gap_rest_snapshot_cannot_sync_a_later_session(self):
        state = self.book()
        old = state.view("A")
        state.desync_all("disconnect")
        state.desync_all("connect")
        self.assertFalse(state.apply_snapshot("A", [(0.3, 4)], [(0.7, 8)],
                                               only_if_unsynced=True,
                                               expected_generation=old.generation,
                                               expected_updates=old.updates))
        self.assertEqual(state.view("A").status, UNSYNCED)

    def test_stale_book_can_be_revalidated_by_rest(self):
        state = self.book()
        with state._lock:
            state._meta["A"]["updated"] -= 1
        view = state.view("A")
        self.assertEqual(view.status, STALE)
        self.assertEqual(state.needs_resync(include_stale=True), ["A"])
        self.assertTrue(state.apply_snapshot("A", [(0.3, 4)], [(0.7, 8)],
                                              only_if_unsynced=True,
                                              expected_generation=view.generation,
                                              expected_updates=view.updates))
        self.assertEqual(state.view("A").status, LIVE)

    def test_rest_revalidation_never_overwrites_newer_ws_update(self):
        state = self.book()
        old = state.view("A")
        self.assertTrue(state.apply_price_change("A", 0.61, 2, "SELL"))
        with state._lock:
            state._meta["A"]["updated"] -= 1
        self.assertFalse(state.apply_snapshot("A", [(0.3, 4)], [(0.7, 8)],
                                               only_if_unsynced=True,
                                               expected_generation=old.generation,
                                               expected_updates=old.updates))
        self.assertIn((0.61, 2.0), state.view("A").asks)

    def test_untrusted_hwm_does_not_advance_history(self):
        state = self.book()
        before = state.ts_hwm
        state.note_exchange_ts(int(time.time() * 1000) + 3_600_000)
        self.assertEqual(state.ts_hwm, before)


class BinanceValidation(unittest.TestCase):
    def test_disconnect_withholds_a_recent_price(self):
        feed = BinanceTradeFeed()
        feed.price = 60000
        feed.price_mono = time.monotonic()
        feed.health.status = DISCONNECTED
        self.assertIsNone(feed.fresh_price())

    def test_bad_timestamp_and_sequence_do_not_crash_handler(self):
        feed = BinanceTradeFeed()
        for timestamp in (0, float("inf"), float("nan"), "x", -1):
            feed._handle(json.dumps({"p": "60000", "T": timestamp}))
        for sequence in (float("inf"), float("nan"), "x", -1):
            feed._handle(json.dumps({"p": "60000", "T": int(time.time() * 1000), "t": sequence}))
        self.assertIsNone(feed.price)
        self.assertEqual(feed.accepted_trades, 0)


class CoinbaseHistory(unittest.TestCase):
    def test_unsafe_non_signalling_tick_is_never_a_future_reference_or_volatility_sample(self):
        import sniper_mode as S
        import tests_sniper_coinbase as T

        for unsafe_lag in (1200.0, None):
            with self.subTest(lag=unsafe_lag):
                sn, fired = T.make()
                sn.cb_fresh.enabled = sn.cb_fresh.connected = True
                clock = [1000.0]
                sn.cb_fresh.note_message(clock[0], 0)
                with mock.patch.object(S.time, "monotonic", lambda: clock[0]):
                    sn.on_coinbase(T.ticker(60000.0, seq=1, lag_ms=80))
                    clock[0] = 1001.1
                    sn.on_coinbase(T.ticker(60000.3, seq=2, lag_ms=unsafe_lag))
                    self.assertEqual(list(sn.trigger.q), [])
                    self.assertEqual(sn.vol_model.last_mid, 60000.0)
                    self.assertEqual(len(sn.vol_model._incs), 0)
                    clock[0] = 1001.4
                    sn.on_coinbase(T.ticker(60003.4, seq=3, lag_ms=80))
                    self.assertEqual(fired, [], "first fresh price cannot trade against a stale reference")
                    clock[0] = 1001.7
                    sn.on_coinbase(T.ticker(60006.5, seq=4, lag_ms=80))
                    self.assertEqual([item[0] for item in fired], ["UP"])


class FeedConnections(unittest.IsolatedAsyncioTestCase):
    async def test_quiet_book_refreshes_before_configured_sniper_freshness_expires(self):
        calls = []

        def fetch(token):
            calls.append((token, time.monotonic()))
            return [(0.4, 3)], [(0.6, 4)]

        with mock.patch.dict("os.environ", {"SNIPER_STALE_MS": "400"}):
            hub = FeedHub(rest_book_fetch=fetch, book_stale_after=8)
        self.assertAlmostEqual(hub.book_refresh_after, 0.3)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        for token in ("A", "B"):
            hub.book.apply_snapshot(token, [(0.4, 3)], [(0.6, 4)])
        started = time.monotonic()
        task = asyncio.create_task(hub._resync_loop())
        try:
            await until(lambda: hub.rest_resyncs == 2, timeout=0.4)
            self.assertTrue(all(stamp - started < 0.4 for _, stamp in calls))
            self.assertTrue(all(hub.book.view(token).book_age_ms() < 100 for token in ("A", "B")))
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_frequently_updated_books_make_no_refresh_requests(self):
        calls = []
        hub = FeedHub(rest_book_fetch=lambda token: calls.append(token), book_refresh_after=0.12)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        for token in ("A", "B"):
            hub.book.apply_snapshot(token, [(0.4, 3)], [(0.6, 4)])
        task = asyncio.create_task(hub._resync_loop())
        try:
            for _ in range(10):
                for token in ("A", "B"):
                    hub.book.apply_snapshot(token, [(0.4, 3)], [(0.6, 4)])
                await asyncio.sleep(0.01)
            self.assertEqual(calls, [])
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_preemptive_rest_refresh_cannot_overwrite_a_ws_change(self):
        started, release = threading.Event(), threading.Event()

        def fetch(token):
            started.set()
            release.wait(2)
            return [(0.3, 3)], [(0.7, 4)]

        hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=0.1)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        hub.book.apply_snapshot("A", [(0.4, 3)], [(0.6, 4)])
        with hub.book._lock:
            hub.book._meta["A"]["updated"] -= 0.2
        self.assertEqual(hub.book.view("A").status, LIVE)
        task = asyncio.create_task(hub._resync_token("A", asyncio.Semaphore(1)))
        try:
            await until(started.is_set)
            hub.book.apply_price_change("A", 0.59, 2, "SELL")
            release.set()
            await task
            self.assertEqual(hub.book.view("A").best_ask, 0.59)
            self.assertEqual(hub.rest_resyncs, 0)
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_failed_rest_refreshes_are_rate_limited(self):
        calls = []

        def fetch(token):
            calls.append((token, time.monotonic()))
            raise ConnectionError("injected fast failure")

        hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=0.03)
        hub.set_round("A", "B", "condition")
        task = asyncio.create_task(hub._resync_loop())
        try:
            await until(lambda: len(calls) == 2)
            await asyncio.sleep(0.1)
            self.assertEqual(len(calls), 2, "fast HTTP failures must not create an API hot loop")
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def server(self):
        frames = []
        live = set()

        async def handle(ws):
            live.add(ws)
            try:
                async for raw in ws:
                    if raw == "PING":
                        await ws.send("PONG")
                    else:
                        frames.append((time.perf_counter(), json.loads(raw)))
            except websockets.ConnectionClosed:
                pass
            finally:
                live.discard(ws)

        server = await websockets.serve(handle, "127.0.0.1", 0, compression=None)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        return url, frames, live

    async def test_market_worker_rotation_has_no_polling_delay(self):
        url, frames, _ = await self.server()
        feed = PolyMarketFeed(url, ping_every=1)
        feed.set_tokens(["initial"])
        feed.start()
        self.addAsyncCleanup(feed.stop)
        await until(lambda: bool(frames))
        latencies = []
        for n in range(20):
            token = f"round-{n}"
            started = []

            def rotate():
                started.append(time.perf_counter())
                feed.set_tokens([token])

            await asyncio.to_thread(rotate)
            await until(lambda: any(token in item.get("assets_ids", [])
                                   and item.get("operation") == "subscribe"
                                   for _, item in frames), timeout=0.15)
            received = next(stamp for stamp, item in frames
                            if token in item.get("assets_ids", []))
            latencies.append((received - started[0]) * 1000)
        print(f"market subscription -> local server: n=20 p50={statistics.median(latencies):.3f}ms "
              f"max={max(latencies):.3f}ms")
        self.assertEqual(feed.book.active, ("round-19",))

    async def test_user_initial_heartbeat_and_worker_rotation_are_immediate(self):
        url, frames, _ = await self.server()
        feed = PolyUserFeed({"apiKey": "offline", "secret": "offline", "passphrase": "offline"},
                            url, markets=["initial"], ping_every=10)
        started = time.perf_counter()
        feed.start()
        self.addAsyncCleanup(feed.stop)
        await until(lambda: feed.ready_for_market("initial"), timeout=0.5)
        readiness_ms = (time.perf_counter() - started) * 1000
        latencies = []
        for n in range(20):
            market = f"round-{n}"
            started = []

            def rotate():
                started.append(time.perf_counter())
                feed.set_markets([market])

            await asyncio.to_thread(rotate)
            await until(lambda: any(market in item.get("markets", [])
                                   and item.get("operation") == "subscribe"
                                   for _, item in frames), timeout=0.15)
            received = next(stamp for stamp, item in frames
                            if market in item.get("markets", []))
            latencies.append((received - started[0]) * 1000)
        print(f"private initial readiness={readiness_ms:.3f}ms; subscription -> local server: "
              f"n=20 p50={statistics.median(latencies):.3f}ms max={max(latencies):.3f}ms")
        self.assertTrue(feed.ready_for_market("round-19"))

    async def test_initial_subscription_failures_clean_up_all_state(self):
        class FailingSocket:
            async def send(self, payload):
                raise ConnectionError("injected initial subscribe failure")

        class Session:
            async def __aenter__(self):
                return FailingSocket()

            async def __aexit__(self, *args):
                return False

        market = PolyMarketFeed("ws://offline")
        market.set_tokens(["A"])
        user = PolyUserFeed({"apiKey": "offline", "secret": "offline", "passphrase": "offline"},
                            "ws://offline", markets=["A"])
        for feed in (market, user):
            with mock.patch("websockets.connect", return_value=Session()):
                with self.assertRaises(ConnectionError):
                    await feed._session()
            self.assertIsNone(feed._ws)
            self.assertEqual(feed._sent, set())
            self.assertEqual(feed.refresh_status(), DISCONNECTED)
        self.assertFalse(market.book.connected)

    async def test_private_heartbeat_timeout_reconnects_and_recovers(self):
        count = 0

        async def handle(ws):
            nonlocal count
            count += 1
            connection = count
            try:
                async for raw in ws:
                    if raw == "PING" and connection > 1:
                        await ws.send("PONG")
            except websockets.ConnectionClosed:
                pass

        server = await websockets.serve(handle, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        feed = PolyUserFeed({"apiKey": "offline", "secret": "offline", "passphrase": "offline"},
                            url, markets=["A"], ping_every=0.03, recv_timeout=1)
        started = time.perf_counter()
        feed.start()
        self.addAsyncCleanup(feed.stop)
        await until(lambda: count >= 2 and feed.ready_for_market("A"), timeout=2)
        self.assertGreaterEqual(feed.health.reconnect_count, 1)
        self.assertGreaterEqual(feed.subscription_generation, 2)
        print(f"private dropped heartbeat -> ready after reconnect: "
              f"{(time.perf_counter() - started) * 1000:.3f}ms "
              f"connections={count} reconnects={feed.health.reconnect_count}")

    async def test_slow_rest_token_does_not_block_sibling_resync(self):
        slow_started = threading.Event()
        release = threading.Event()

        def fetch(token):
            if token == "A":
                slow_started.set()
                release.wait(2)
            return [(0.4, 3)], [(0.6, 4)]

        hub = FeedHub(rest_book_fetch=fetch)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        task = asyncio.create_task(hub._resync_loop())
        try:
            await until(lambda: slow_started.is_set())
            await until(lambda: hub.book.view("B").status == LIVE, timeout=0.3)
            self.assertEqual(hub.book.view("A").status, UNSYNCED)
            hub.book.desync_all("reconnected while REST was in flight")
            release.set()
            await until(lambda: not hub._resync_event.is_set())
            await asyncio.sleep(0.02)
            self.assertEqual(hub.book.view("A").status, UNSYNCED)
        finally:
            release.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


if __name__ == "__main__":
    unittest.main()
