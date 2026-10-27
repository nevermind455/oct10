"""Offline regressions for executable REST recovery and historical evidence."""
from __future__ import annotations

import asyncio
import dataclasses
from decimal import Decimal
import time
import types
import unittest
from unittest import mock

import testing_env  # noqa: F401 - no .env, wallet, or venue traffic
import orderbook
from feeds.hub import FeedHub
from paper_trade import PaperRejected, snapshot_from_book_view


def payload(token="1", *, timestamp=None, ask="0.51"):
    return {"asset_id": token, "timestamp": int(time.time() * 1000) if timestamp is None else timestamp,
            "bids": [{"price": "0.49", "size": "50"}],
            "asks": [{"price": ask, "size": "30"}], "tick_size": "0.01", "hash": "rest-hash"}


async def inline_worker(fn, *args, **kwargs):
    # Exercise the real HTTP/parser/hub/book path with deterministic receive
    # races. No external I/O or executor teardown is needed for these tests.
    return fn(*args, **kwargs)


class RestMetadata(unittest.IsolatedAsyncioTestCase):
    def hub(self):
        hub = FeedHub(rest_book_fetch=orderbook.get_orderbook_snapshot, book_refresh_after=.01)
        hub.set_round("1", "2", "condition")
        hub.book.connected = True
        return hub

    async def refresh(self, hub, data, *, during_fetch=None, during_decode=None, json_delay=0):
        def get(*_args, **_kwargs):
            if during_fetch:
                during_fetch()

            def json_data():
                if during_decode:
                    during_decode()
                if json_delay:
                    time.sleep(json_delay)
                return data

            return types.SimpleNamespace(raise_for_status=lambda: None, json=json_data)

        with mock.patch("orderbook.http_pool.get", side_effect=get), \
                mock.patch("feeds.hub.asyncio.to_thread", new=inline_worker):
            await hub._resync_token("1", asyncio.Semaphore(1))

    async def test_quiet_refresh_remains_executable_without_a_new_ws_event(self):
        hub = self.hub()
        old = int(time.time() * 1000) - 100
        hub.book.apply_snapshot("1", [(0.49, 50)], [(0.50, 30)], ts_ms=old)
        snapshot_from_book_view(hub.book.view("1"), expected_token="1")
        await asyncio.sleep(.015)
        data = payload()
        await self.refresh(hub, data)

        view = hub.book.view("1")
        self.assertEqual((hub.rest_resyncs, view.status, view.source), (1, "LIVE", "rest"))
        self.assertEqual(view.exchange_ts_ms, data["timestamp"])
        self.assertEqual(view.hash, "rest-hash")
        self.assertLess(view.book_age_ms(), 100)
        # This is the same conversion LiveExecution.prepare calls immediately
        # before sizing/signing; the former refresh raised PaperRejected here.
        executable = snapshot_from_book_view(view, expected_token="1")
        self.assertEqual(executable.timestamp, str(data["timestamp"]))
        self.assertEqual(executable.tick_size, Decimal("0.01"))
        self.assertEqual(executable.asks, ((Decimal("0.51"), Decimal("30.0")),))

    async def test_rest_snapshot_preserves_earlier_history_without_certifying_venue_time(self):
        hub = self.hub()
        now = int(time.time() * 1000)
        hub.book.apply_snapshot("1", [(0.49, 50)], [(0.50, 30)], ts_ms=now - 400)
        hub.book.note_exchange_ts(now - 300)
        await asyncio.sleep(.015)
        await self.refresh(hub, payload(timestamp=now - 100))
        view = hub.book.view("1")
        self.assertEqual(hub.book.ts_hwm, now - 300, "REST does not advance the WS stream watermark")
        earlier, _ = hub.book.view_at("1", now - 350)
        self.assertEqual(earlier.best_ask, .50, "new REST levels must not rewrite the pre-move book")
        self.assertEqual(earlier.source, "ws")
        local = hub.book.view_at_mono("1", time.monotonic())
        self.assertEqual(local, dataclasses.replace(view, updates=0, receive_revision=0, hash=None))
        snapshot_from_book_view(local, expected_token="1")

        for quiet in (False, True):
            hub.book.note_exchange_ts(now + 100)
            historical, why = hub.book.view_at("1", now - 50, allow_quiet=quiet)
            self.assertIsNone(historical)
            self.assertEqual(why["why"], "REST snapshot is not stream-ordered history")
        hub.book.apply_price_change("1", .52, 20, "SELL", ts_ms=now + 101)
        hub.book.note_exchange_ts(now + 103)
        restored, _ = hub.book.view_at("1", now + 102)
        self.assertEqual(restored.source, "ws")
        snapshot_from_book_view(restored, expected_token="1")

    async def test_rest_receipt_age_includes_response_decoding(self):
        hub = self.hub()
        await self.refresh(hub, payload(), json_delay=.03)
        self.assertEqual(hub.rest_resyncs, 1)
        self.assertGreaterEqual(hub.book.view("1").book_age_ms(), 25)
        executable = snapshot_from_book_view(hub.book.view("1"), expected_token="1")
        self.assertGreaterEqual(time.time() - executable.received_wall, .025)

    async def test_rest_local_history_starts_at_installation_while_freshness_starts_at_receipt(self):
        hub = self.hub()
        now = int(time.time() * 1000)
        hub.book.apply_snapshot("1", [(0.49, 50)], [(0.51, 30)], ts_ms=now - 100)
        await asyncio.sleep(.015)
        during = {}

        def query_during_decode():
            during["mono"] = time.monotonic()
            during["ask"] = hub.book.view_at_mono("1", during["mono"]).best_ask

        await self.refresh(hub, payload(timestamp=now, ask="0.71"),
                           during_decode=query_during_decode, json_delay=.03)
        self.assertEqual(hub.rest_resyncs, 1)
        self.assertEqual(during["ask"], .51)
        self.assertEqual(hub.book.view_at_mono("1", during["mono"]).best_ask, .51,
                         "installing REST must not rewrite the book previously held during JSON decoding")
        after = hub.book.view_at_mono("1", time.monotonic())
        current = hub.book.view("1")
        self.assertEqual((after.best_ask, after.source), (.71, "rest"))
        self.assertEqual(after.updated_mono, current.updated_mono,
                         "historical snapshots retain the HTTP receipt age")
        self.assertLessEqual(after.updated_mono, during["mono"])
        self.assertGreaterEqual(after.book_age_ms(), 25)
        executable = snapshot_from_book_view(after, expected_token="1")
        self.assertGreaterEqual(time.time() - executable.received_wall, .025)

    async def test_rest_venue_timestamp_fences_delayed_ws_deltas_and_tick_events(self):
        for event in ("single", "batch", "tick"):
            with self.subTest(event=event):
                hub = self.hub()
                now = int(time.time() * 1000)
                hub.book.apply_snapshot("1", [(0.49, 50)], [(0.50, 30)], ts_ms=now - 200)
                await asyncio.sleep(.015)
                await self.refresh(hub, payload(timestamp=now))
                if event == "single":
                    applied = hub.book.apply_price_change("1", .505, 40, "SELL", ts_ms=now - 100)
                elif event == "batch":
                    applied = hub.book.apply_price_changes(
                        [{"asset_id": "1", "side": "SELL", "price": ".505", "size": "40"}],
                        ts_ms=now - 100)
                else:
                    applied = hub.book.set_tick_size("1", .001, ts_ms=now - 100)
                self.assertFalse(applied)
                current = hub.book.view("1")
                self.assertEqual((current.best_ask, current.tick_size, current.exchange_ts_ms, current.source),
                                 (.51, .01, now, "rest"))

    async def test_wrong_token_and_missing_or_invalid_timestamp_never_recover_the_book(self):
        cases = [payload("2"), payload(timestamp="bad"), payload(timestamp=0),
                 payload(timestamp=int((time.time() + 30) * 1000))]
        no_timestamp = payload()
        no_timestamp.pop("timestamp")
        cases.append(no_timestamp)
        for data in cases:
            with self.subTest(data=data):
                hub = self.hub()
                await self.refresh(hub, data)
                self.assertEqual(hub.rest_resyncs, 0)
                self.assertEqual(hub.book.view("1").status, "UNSYNCED")

    async def test_structured_wrong_token_is_also_rejected_at_the_hub_boundary(self):
        hub = self.hub()
        result = orderbook.parse_orderbook_snapshot(payload("2"), "2")
        hub._rest_book_fetch = lambda _token: result
        with mock.patch("feeds.hub.asyncio.to_thread", new=inline_worker):
            await hub._resync_token("1", asyncio.Semaphore(1))
        self.assertEqual(hub.rest_resyncs, 0)
        self.assertEqual(hub.book.view("1").status, "UNSYNCED")

    async def test_ws_receive_and_generation_changes_fence_structured_rest_responses(self):
        for change in ("delta", "disconnect", "rotation"):
            with self.subTest(change=change):
                hub = self.hub()

                def during_fetch():
                    if change == "delta":
                        hub.book.apply_price_changes(
                            [{"asset_id": "1", "side": "SELL", "price": ".6", "size": "10"}],
                            ts_ms=int(time.time() * 1000))
                    elif change == "disconnect":
                        hub.book.desync_all("disconnect")
                    else:
                        hub.set_round("3", "4", "next-condition")

                await self.refresh(hub, payload(), during_fetch=during_fetch)
                self.assertEqual(hub.rest_resyncs, 0)
                self.assertNotEqual(hub.book.view("1").status, "LIVE")

    async def test_legacy_levels_do_not_invent_exchange_timestamp(self):
        hub = self.hub()
        hub._rest_book_fetch = lambda _token: ([(.49, 50)], [(.51, 30)])
        with mock.patch("feeds.hub.asyncio.to_thread", new=inline_worker):
            await hub._resync_token("1", asyncio.Semaphore(1))
        view = hub.book.view("1")
        self.assertEqual((view.status, view.source, view.exchange_ts_ms), ("LIVE", "rest", None))
        with self.assertRaisesRegex(PaperRejected, "omitted exchange timestamp"):
            snapshot_from_book_view(view, expected_token="1")


class ProductionWiring(unittest.TestCase):
    def test_paper_runner_uses_the_metadata_preserving_public_fetch(self):
        import run
        with mock.patch("run._print_log", return_value=None):
            hub, _ = run.build_hub(paper=True)
        self.assertIs(hub._rest_book_fetch, orderbook.get_orderbook_snapshot)

    def test_legacy_get_orderbook_still_returns_two_level_lists(self):
        data = payload()
        response = types.SimpleNamespace(raise_for_status=lambda: None, json=lambda: data)
        with mock.patch("orderbook.http_pool.get", return_value=response):
            bids, asks = orderbook.get_orderbook("1")
        self.assertEqual(bids, [{"price": "0.49", "size": "50"}])
        self.assertEqual(asks, [{"price": "0.51", "size": "30"}])


if __name__ == "__main__":
    unittest.main()
