"""Offline regressions for REST/WS recovery races and ordered resubscription."""
from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from unittest import mock

import testing_env  # noqa: F401 - isolate credentials and block venue traffic
import websockets

from feeds.book import BookState
from feeds.health import DISCONNECTED, LIVE, UNSYNCED
from feeds.hub import FeedHub
from feeds.poly_market import PolyMarketFeed


async def until(predicate, timeout=1.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.002)
    raise AssertionError("condition did not become true inside timeout")


def delta(*rows, timestamp=None):
    return {"event_type": "price_change", "timestamp": timestamp or int(time.time() * 1000),
            "price_changes": list(rows)}


def ask(token, price, size=10):
    return {"asset_id": token, "side": "SELL", "price": str(price), "size": str(size)}


def snapshot(token, price, timestamp=None):
    return {"event_type": "book", "asset_id": token,
            "timestamp": timestamp or int(time.time() * 1000),
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": str(price), "size": "10"}]}


class ReceiveRevisions(unittest.TestCase):
    def book(self):
        state = BookState()
        state.connected = True
        state.set_active(["A", "B"])
        return state

    def test_unsynced_event_fences_rest_without_inventing_updates_or_history(self):
        state = self.book()
        before = state.view("A")
        self.assertTrue(state.apply_price_changes([ask("A", .5, 0), ask("A", .6)],
                                                  ts_ms=int(time.time() * 1000)))
        after = state.view("A")
        self.assertEqual(after.generation, before.generation)
        self.assertEqual(after.updates, 0)
        self.assertEqual(after.receive_revision, before.receive_revision + 1)
        self.assertEqual(after.status, UNSYNCED)
        self.assertEqual(state.view("B").receive_revision, 0)
        self.assertIsNone(state.view_at_mono("A", time.monotonic()))
        self.assertFalse(state.apply_snapshot("A", [(0.4, 10)], [(0.5, 10)],
                                              expected_generation=before.generation,
                                              expected_updates=before.updates,
                                              expected_receive_revision=before.receive_revision))

    def test_single_ignored_delta_also_fences_rest(self):
        state = self.book()
        self.assertFalse(state.apply_price_change("A", .6, 10, "SELL"))
        self.assertEqual(state.view("A").receive_revision, 1)
        self.assertEqual(state.view("A").updates, 0)

    def test_malformed_or_refused_events_fence_pending_books(self):
        cases = [None, [None], [{"side": "SELL", "price": ".6", "size": "10"}],
                 [ask("A", "garbage")]]
        for rows in cases:
            with self.subTest(rows=rows):
                state = self.book()
                state.apply_price_changes(rows, ts_ms=int(time.time() * 1000))
                self.assertEqual(state.view("A").receive_revision, 1)
                self.assertEqual(state.view("A").updates, 0)
                self.assertEqual(state.view("A").status, UNSYNCED)
        state = self.book()
        self.assertFalse(state.apply_price_changes([ask("A", .6)], ts_ms="bad"))
        self.assertEqual(state.view("A").receive_revision, 1)

    def test_older_ws_snapshot_cannot_erase_an_observed_pending_delta(self):
        state = self.book()
        now = int(time.time() * 1000)
        state.apply_price_changes([ask("A", .6)], ts_ms=now)
        self.assertFalse(state.apply_snapshot("A", [(0.4, 10)], [(0.5, 10)],
                                              ts_ms=now - 1, require_exchange_ts=True))
        self.assertEqual(state.view("A").status, UNSYNCED)
        self.assertEqual(state.view("A").updates, 0)
        self.assertTrue(state.apply_snapshot("A", [(0.4, 10)], [(0.6, 10)],
                                             ts_ms=now, require_exchange_ts=True))

    def test_receive_revision_survives_cached_status_replacement(self):
        state = self.book()
        state.apply_snapshot("A", [(0.4, 10)], [(0.6, 10)])
        view = state.view("A")
        state.connected = False
        self.assertEqual(state.view("A").status, DISCONNECTED)
        self.assertEqual(state.view("A").receive_revision, view.receive_revision)


class RecoveryRaces(unittest.IsolatedAsyncioTestCase):
    async def raced_fetch(self, dispatch, *, refresh_after=.1):
        started, release = threading.Event(), threading.Event()

        def fetch(token):
            started.set()
            if not release.wait(2):
                raise TimeoutError("fixture response was not released")
            return [(0.4, 10)], [(0.5, 10)]

        hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=refresh_after)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        hub.market._handle(json.dumps(snapshot("B", .7)))
        task = asyncio.create_task(hub._resync_token("A", asyncio.Semaphore(1)))
        try:
            await until(started.is_set)
            dispatch(hub)
            release.set()
            await task
            return hub
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_actual_ws_dispatch_during_rest_keeps_sibling_live(self):
        def dispatch(hub):
            hub.market._handle(json.dumps(delta(ask("A", .5, 0), ask("A", .6),
                                                 ask("B", .7, 0), ask("B", .71))))

        hub = await self.raced_fetch(dispatch)
        self.assertEqual(hub.book.view("A").status, UNSYNCED)
        self.assertEqual(hub.book.view("A").updates, 0)
        self.assertEqual(hub.book.view("B").status, LIVE)
        self.assertEqual(hub.book.view("B").best_ask, .71)
        self.assertEqual(hub.book.view("B").updates, 2)
        self.assertEqual(hub.rest_resyncs, 0)
        self.assertEqual(hub.market._snapshot_requests, {"A": hub.book.view("A").generation})

    async def test_disconnect_and_rotation_reject_old_rest_without_resubscription(self):
        for transition in ("disconnect", "rotation", "remove-readd"):
            with self.subTest(transition=transition):
                def dispatch(hub):
                    if transition == "disconnect":
                        hub.book.connected = False
                        hub.book.desync_all("disconnect")
                        hub.book.connected = True
                        hub.book.desync_all("connect")
                    else:
                        hub.set_round("C", "D", "new-condition")
                        if transition == "remove-readd":
                            hub.set_round("A", "B", "condition")

                hub = await self.raced_fetch(dispatch)
                self.assertNotEqual(hub.book.view("A").status, LIVE)
                self.assertEqual(hub.rest_resyncs, 0)
                self.assertEqual(hub.market._snapshot_requests, {})

    async def test_ws_snapshot_fallback_is_rate_limited(self):
        hub = FeedHub(rest_book_fetch=lambda token: None, book_refresh_after=.01)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True

        async def raced_response(fn, token):
            hub.market._handle(json.dumps(delta(ask("A", .6))))
            return [(0.4, 10)], [(0.5, 10)]

        with mock.patch("feeds.hub.asyncio.to_thread", raced_response), \
                mock.patch.object(hub.market, "request_snapshot", wraps=hub.market.request_snapshot) as request:
            for _ in range(5):
                hub._next_book_fetch_at.clear()
                await hub._resync_token("A", asyncio.Semaphore(1))
            self.assertEqual(request.call_count, 1)
        self.assertEqual(hub.book.view("A").status, UNSYNCED)
        self.assertEqual(hub.rest_resyncs, 0)

    async def test_queued_snapshot_request_is_dropped_after_rotation(self):
        state = BookState()
        state.connected = True
        feed = PolyMarketFeed(book=state)
        feed.set_tokens(["A", "B"])
        generation = state.view("A").generation
        feed._ws = mock.AsyncMock()
        feed._sent = {"A", "B"}
        self.assertTrue(feed.request_snapshot("A", expected_generation=generation))
        feed.set_tokens(["C", "D"])
        await feed._sync_subscriptions()
        frames = [json.loads(call.args[0]) for call in feed._ws.send.call_args_list]
        self.assertFalse(any(frame.get("operation") == "subscribe" and "A" in frame["assets_ids"]
                             for frame in frames))
        self.assertFalse(feed.request_snapshot("A", expected_generation=generation))
        feed.set_tokens(["A", "B"])
        self.assertFalse(feed.request_snapshot("A", expected_generation=generation))

    async def test_snapshot_arriving_during_unsubscribe_is_invalidated_before_resubscribe(self):
        state = BookState()
        state.connected = True
        feed = PolyMarketFeed(book=state)
        feed.set_tokens(["A", "B"])
        feed._sent = {"A", "B"}
        state.apply_snapshot("B", [(0.4, 10)], [(0.7, 10)])
        original = state.view("A")
        sibling = state.view("B")

        async def send(raw):
            message = json.loads(raw)
            if message["operation"] == "unsubscribe":
                feed._handle(json.dumps(snapshot("A", .5)))
                self.assertEqual(state.view("A").status, LIVE)
            else:
                self.assertEqual(state.view("A").status, UNSYNCED)
                self.assertGreater(state.view("A").generation, original.generation)
                feed._handle(json.dumps(snapshot("A", .6)))

        feed._ws = mock.AsyncMock()
        feed._ws.send.side_effect = send
        self.assertTrue(feed.request_snapshot("A", expected_generation=original.generation))
        await feed._sync_subscriptions()
        self.assertEqual(state.view("A").best_ask, .6)
        self.assertEqual(state.view("A").status, LIVE)
        self.assertEqual(feed._sent, {"A", "B"})
        self.assertEqual(state.view("B"), sibling)

    async def test_continuous_feed_recovers_from_ordered_ws_snapshot_on_same_socket(self):
        frames, recovered = [], asyncio.Event()
        fetch_started, release = threading.Event(), threading.Event()
        fetch_mono = []

        async def server_session(ws):
            emitter = None

            async def emit():
                while not recovered.is_set():
                    await ws.send(json.dumps(delta(ask("A", .5, 0), ask("A", .6))))
                    await asyncio.sleep(.004)

            try:
                async for raw in ws:
                    if raw == "PING":
                        await ws.send("PONG")
                        continue
                    frame = json.loads(raw)
                    frames.append(frame)
                    if frame.get("type") == "market":
                        await ws.send(json.dumps(snapshot("B", .7)))
                        emitter = asyncio.create_task(emit())
                    elif frame.get("operation") == "subscribe" and frame["assets_ids"] == ["A"]:
                        # Cancel the producer before sending the recovery
                        # snapshot, then continue the ordered stream with a
                        # newer delta. No quiet REST window was available.
                        if emitter:
                            emitter.cancel()
                            await asyncio.gather(emitter, return_exceptions=True)
                        await ws.send(json.dumps(snapshot("A", .6)))
                        await ws.send(json.dumps(delta(ask("A", .6, 0), ask("A", .65))))
                        recovered.set()
            except websockets.ConnectionClosed:
                pass
            finally:
                if emitter:
                    emitter.cancel()
                    await asyncio.gather(emitter, return_exceptions=True)

        def fetch(token):
            fetch_mono.append(time.monotonic())
            fetch_started.set()
            if not release.wait(2):
                raise TimeoutError("fixture response was not released")
            return [(0.4, 10)], [(0.5, 10)]

        async with websockets.serve(server_session, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=.02,
                          urls={"poly_market": f"ws://127.0.0.1:{port}"})
            hub.set_round("A", "B", "condition")
            hub.market.start()
            fetch_task = None
            try:
                await until(lambda: hub.book.view("B").status == LIVE
                            and hub.book.view("A").receive_revision >= 3)
                socket = hub.market._ws
                sibling_generation = hub.book.view("B").generation
                fetch_task = asyncio.create_task(hub._resync_token("A", asyncio.Semaphore(1)))
                await until(fetch_started.is_set)
                revision = hub.book.view("A").receive_revision
                await until(lambda: hub.book.view("A").receive_revision >= revision + 3)
                release.set()
                await fetch_task
                await until(lambda: hub.book.view("A").status == LIVE
                            and hub.book.view("A").best_ask == .65)
                self.assertEqual(hub.rest_resyncs, 0)
                self.assertIs(hub.market._ws, socket)
                self.assertEqual(hub.book.view("A").updates, 2)
                self.assertEqual(hub.book.view("B").generation, sibling_generation)
                self.assertEqual(hub.book.view("B").status, LIVE)
                self.assertIsNone(hub.book.view_at_mono("A", fetch_mono[0]))
                self.assertEqual(hub.book.view_at_mono("A", time.monotonic()).best_ask, .65)
                recovery_frames = [f for f in frames if "operation" in f]
                self.assertEqual(recovery_frames, [
                    {"assets_ids": ["A"], "operation": "unsubscribe"},
                    {"assets_ids": ["A"], "operation": "subscribe", "custom_feature_enabled": True},
                ])
            finally:
                release.set()
                if fetch_task:
                    fetch_task.cancel()
                    await asyncio.gather(fetch_task, return_exceptions=True)
                await hub.market.stop()


if __name__ == "__main__":
    unittest.main()
