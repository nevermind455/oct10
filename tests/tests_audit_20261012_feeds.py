"""Offline feed audit regressions, stress checks, and reproducible timings.

Run ``python -m pytest -q -s tests_audit_20261012_feeds.py``. No credentials,
exchange connections, production journals, or real orders are accessed.
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

import testing_env  # noqa: F401 - prevent credentials and venue network access

from feeds.book import BookState
from feeds.health import LIVE, UNSYNCED
from feeds.hub import FeedHub
from feeds.poly_market import PolyMarketFeed
from feeds.poly_user import FillStore
import orderbook


def state():
    book = BookState()
    book.connected = True
    book.set_active(["A", "B"])
    now = int(time.time() * 1000)
    for token in book.active:
        assert book.apply_snapshot(token, [(0.4, 10)], [(0.6, 10)], ts_ms=now)
    return book, now


async def until(predicate, timeout=1.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        await asyncio.sleep(.002)
    raise AssertionError("condition did not become true inside timeout")


class FeedOrdering(unittest.TestCase):
    def test_rest_revalidation_preserves_timestamp_fence_for_later_deltas(self):
        book, now = state()
        with book._lock:
            book._meta["A"]["updated"] -= 2
        before = book.view("A")
        self.assertTrue(book.apply_snapshot(
            "A", [(0.4, 10)], [(0.61, 10)], only_if_unsynced=True,
            expected_generation=before.generation, expected_updates=before.updates,
            expected_receive_revision=before.receive_revision,
            refresh_if_older_than=1))
        self.assertFalse(book.apply_price_changes([
            {"asset_id": "A", "price": ".5", "size": "10", "side": "SELL"}],
            ts_ms=now - 1))
        self.assertEqual(book.view("A").best_ask, .61)

    def test_single_delta_also_preserves_observed_watermark_after_rest(self):
        book, now = state()
        before = book.view("A")
        self.assertTrue(book.apply_snapshot(
            "A", [(0.4, 10)], [(0.61, 10)], expected_generation=before.generation,
            expected_updates=before.updates, expected_receive_revision=before.receive_revision))
        self.assertFalse(book.apply_price_change("A", .5, 10, "SELL", ts_ms=now - 1))
        self.assertEqual(book.view("A").best_ask, .61)

    def test_rest_without_exchange_timestamp_cannot_invent_past_paper_book(self):
        book, now = state()
        book.apply_snapshot("A", [(0.4, 10)], [(0.61, 10)])
        book.note_exchange_ts(now + 1)
        self.assertIsNone(book.view_at("A", now)[0])
        self.assertEqual(book.view_at_mono("A", time.monotonic()).best_ask, .61)

    def test_history_waits_until_exchange_time_is_strictly_passed(self):
        book, now = state()
        self.assertIsNone(book.view_at("A", now)[0],
                          "another message may carry the same millisecond timestamp")
        book.note_exchange_ts(now + 1)
        self.assertEqual(book.view_at("A", now)[0].best_ask, .6)

    def test_out_of_order_tick_change_cannot_restore_an_old_tick(self):
        book, now = state()
        feed = PolyMarketFeed(book=book)
        feed.set_tokens(["A", "B"])
        for timestamp, tick in ((now + 2, ".001"), (now + 1, ".01")):
            feed._handle(json.dumps({"event_type": "tick_size_change", "asset_id": "A",
                                     "timestamp": timestamp, "new_tick_size": tick}))
        self.assertEqual(book.view("A").tick_size, .001)

    def test_invalid_timestamp_last_trade_never_replaces_latest_price(self):
        book, now = state()
        feed = PolyMarketFeed(book=book)
        feed.set_tokens(["A", "B"])
        feed._handle(json.dumps({"event_type": "last_trade_price", "asset_id": "A",
                                "timestamp": now, "price": ".6"}))
        for invalid in (None, True, float("inf"), "garbage", now + 60_000):
            feed._handle(json.dumps({"event_type": "last_trade_price", "asset_id": "A",
                                    "timestamp": invalid, "price": ".7"}))
        self.assertEqual(feed.last_trade_price["A"], .6)


class PublicBookValidation(unittest.TestCase):
    def test_supplied_preflight_book_cannot_bypass_level_validation(self):
        bad_books = [
            ([{"price": ".4", "size": "10"}],
             [{"price": "NaN", "size": "10"}, {"price": ".6", "size": "10"}]),
            ([{"price": ".7", "size": "10"}], [{"price": ".6", "size": "10"}]),
            ([{"price": ".4", "size": "10"}],
             [{"price": ".6", "size": "-1"}, {"price": ".7", "size": "100"}]),
        ]
        for book in bad_books:
            with self.subTest(book=book), self.assertRaises(ValueError):
                orderbook.validate_buy_liquidity("1", 1, .8, .5, book=book)

    def test_rest_response_requires_level_arrays(self):
        now = time.time()
        for invalid in (None, "", {}, 0):
            with self.subTest(levels=invalid), self.assertRaises(ValueError):
                orderbook.parse_orderbook({"asset_id": "1", "timestamp": int(now * 1000),
                                           "bids": invalid,
                                           "asks": [{"price": ".6", "size": "10"}]},
                                          "1", now=now)


class IndependentRecovery(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_token_cannot_starve_subsequent_sibling_refresh(self):
        release = threading.Event()
        started = threading.Event()
        calls = []

        def fetch(token):
            calls.append(token)
            if token == "A":
                started.set()
                release.wait(2)
            return [(0.4, 10)], [(0.6, 10)]

        hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=.04)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        task = asyncio.create_task(hub._resync_loop())
        try:
            await until(started.is_set)
            await until(lambda: hub.book.view("B").status == LIVE)
            hub.book.desync_all("injected later gap")
            started_at = time.perf_counter()
            await until(lambda: hub.book.view("B").status == LIVE, timeout=.6)
            print(f"blocked REST A -> second recovery B: "
                  f"{(time.perf_counter() - started_at) * 1000:.3f}ms")
            self.assertEqual(calls.count("A"), 1, "only one worker per token may be in flight")
        finally:
            release.set()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_repeated_async_timeouts_keep_one_physical_worker_and_fence_its_result(self):
        release = threading.Event()
        calls = []

        def fetch(token):
            calls.append(token)
            release.wait(2)
            return [(0.4, 10)], [(0.5, 10)]

        hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=.01, rest_book_timeout=.01)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        try:
            for _ in range(3):
                hub._next_book_fetch_at.clear()
                await hub._resync_token("A", asyncio.Semaphore(1))
            self.assertEqual(calls, ["A"])
            self.assertEqual(hub.market._snapshot_requests, {"A": hub.book.view("A").generation})
            hub.book.apply_snapshot("A", [(0.4, 10)], [(0.7, 10)], ts_ms=int(time.time() * 1000))
            release.set()
            await hub._book_fetches["A"][0]
            hub._next_book_fetch_at.clear()
            with hub.book._lock:
                hub.book._meta["A"]["updated"] -= 1
            await hub._resync_token("A", asyncio.Semaphore(1))
            self.assertEqual(hub.book.view("A").best_ask, .7)
            self.assertEqual(hub.rest_resyncs, 0)
        finally:
            release.set()
            await asyncio.gather(*(task for task, _ in hub._book_fetches.values()),
                                 return_exceptions=True)

    async def test_http_worker_timeout_can_retry_after_completion(self):
        calls = []

        def fetch(token):
            calls.append(token)
            if len(calls) == 1:
                raise TimeoutError("HTTP read timeout")
            return [(0.4, 10)], [(0.6, 10)]

        hub = FeedHub(rest_book_fetch=fetch, rest_book_timeout=.1)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        await hub._resync_token("A", asyncio.Semaphore(1))
        hub._next_book_fetch_at.clear()
        await hub._resync_token("A", asyncio.Semaphore(1))
        self.assertEqual(calls, ["A", "A"])
        self.assertEqual(hub.book.view("A").status, LIVE)


class FeedStress(unittest.TestCase):
    def test_atomic_multi_token_updates_and_cached_views_under_contention(self):
        book, now = state()
        finished = threading.Event()
        failures = []
        checks = 0

        def write():
            for n in range(2000):
                size = 10 + n
                if not book.apply_price_changes([
                        {"asset_id": token, "price": ".6", "size": size, "side": "SELL"}
                        for token in ("A", "B")], ts_ms=now):
                    failures.append(n)
            finished.set()

        thread = threading.Thread(target=write)
        thread.start()
        try:
            while not finished.is_set() or checks < 2000:
                views = book.selected_views(("A", "B"))
                self.assertEqual(views["A"].best_ask_size, views["B"].best_ask_size)
                self.assertEqual(views["A"].status, LIVE)
                checks += 1
        finally:
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(book.view("A").updates, 2001)
        print(f"atomic book stress: 2000 events, {checks} snapshots, 0 lost/torn updates")

    def test_fill_summary_remains_correct_after_every_mutation(self):
        store = FillStore()
        self.assertEqual(store.summary()["seen"], 0)
        store.record_trade("trade", asset_id="A", market="M", side="BUY", price=.6,
                           size=10, status="MATCHED")
        self.assertEqual(store.summary()["pending"], 1)
        store.record_trade("trade", asset_id="A", market="M", side="BUY", price=.6,
                           size=10, status="CONFIRMED")
        self.assertEqual(store.summary()["fills"], 1)
        store.record_trade("trade", asset_id="A", market="M", side="BUY", price=.7,
                           size=10, status="CONFIRMED")
        self.assertEqual(store.summary()["fills"], 0)
        store.record_order("order", asset_id="A", side="BUY", price=.6, state="PLACEMENT")
        self.assertEqual(store.summary()["open_orders"], 1)
        store.record_order("order", state="CANCELLATION")
        self.assertEqual(store.summary()["cancelled"], 1)
        store.note_invalid()
        self.assertEqual(store.summary()["invalid_updates"], 1)
        altered = store.summary()
        altered["seen"] = 1000
        self.assertEqual(store.summary()["seen"], 1)


def timings(n=2000, fills=10000):
    """Repeated read cost under an unchanged completed-fill history.

    Measures real public APIs with valid payloads; setup and timestamp/string
    construction are outside the timed receive samples.
    """
    store = FillStore()
    for k in range(fills):
        store.record_trade(f"trade-{k}", asset_id="A", market="M", side="BUY",
                           price=.6, size=10, status="CONFIRMED")
    store.summary()
    summary = []
    for _ in range(n):
        start = time.perf_counter_ns()
        store.summary()
        summary.append((time.perf_counter_ns() - start) / 1e6)
    book, now = state()
    feed = PolyMarketFeed(book=book)
    feed.set_tokens(["A", "B"])
    raw = json.dumps({"event_type": "price_change", "timestamp": now,
                      "price_changes": [{"asset_id": "A", "price": ".6",
                                         "size": "10", "side": "SELL"}]})
    receive = []
    for _ in range(n):
        start = time.perf_counter_ns()
        feed._handle(raw)
        book.view("A")
        receive.append((time.perf_counter_ns() - start) / 1e6)

    def stats(xs):
        ordered = sorted(xs)
        return {"n": len(xs), "p50_ms": statistics.median(xs),
                "p99_ms": ordered[min(len(xs) - 1, int(len(xs) * .99))],
                "max_ms": max(xs)}

    return {"fill_summary_10000": stats(summary), "market_receive_plus_view": stats(receive),
            "accepted_events": book.view("A").updates - 1,
            "rejected_events": feed.invalid_messages}


if __name__ == "__main__":
    # run_tests.py runs this file directly: the regression cases must run here,
    # not only the timing printout (which reported PASS with no case executed).
    import sys
    if "--timings" in sys.argv:
        print(json.dumps(timings(), indent=2))
    else:
        unittest.main()
