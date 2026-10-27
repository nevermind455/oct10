#!/usr/bin/env python3
"""Three bugs reported 2026-10-27, each reproduced here first.

1  Inventory undercount: an order whose reply (or private-channel trades) shows 20
   shares delivered counted as its 10-share sizing estimate in Kelly's inventory
   until the ledger booked it CONFIRMED - so a new signal could buy 7 more.
2  Book recovery: a REST snapshot that failed fast skipped the WebSocket
   resubscribe fallback, leaving the token UNSYNCED and untradeable.
3  Restart: with exits off (allow_sells=False) a valid ledger whose history holds
   SELL lots refused to load, so the bot could not start.

Found while fixing them:
- a replayed sale that closed a position was re-dated to the restart, moving its
  P/L into a later loss-budget window (realized_since);
- the resolver's per-market API throttle grew by one entry per round, forever.

No network, no wallet.

    python tests/tests_fixes_20261027.py
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import tests_live_20261014_incident as Inc  # noqa: E402
import tests_live_20261016_cash_freeze as CF  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from book_snapshot import RestBookSnapshot  # noqa: E402
from feeds.health import LIVE, UNSYNCED  # noqa: E402
from feeds.hub import FeedHub  # noqa: E402


# =========================================================================== 1
class InventoryBeforeConfirmation(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan, trade, reply = (Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan,
                                                     Inc.Live.trade, Inc.Live.reply)
    matched = CF.CashFreeze.matched

    async def send(self, oid):
        p = self.ex.prepare(self.plan(CF.AMOUNT, cap=CF.CAP, ask=CF.PRICE))
        await self.ex.send(p)
        return p

    async def test_the_replys_delivered_shares_count_before_the_ledger_books_them(self):
        T = self.T
        self.matched("0xa1", making=str(CF.AMOUNT), taking="20")          # 20 shares for $9: a cheaper fill
        await self.send("0xa1")
        self.assertEqual(self.ex.ledger.open_leg_shares(T.COND, T.UP), 0)   # nothing CONFIRMED yet
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), 20.0, places=6)

    async def test_private_channel_trades_count_before_they_are_confirmed(self):
        T = self.T
        self.reply("0xa2", tids=("t-a2",))                                # matched, trade ids, no totals
        await self.send("0xa2")
        self.trade("t-a2", "0xa2", 20.0, 0.45, status="MATCHED")          # provisional: the ledger waits
        self.assertEqual(self.ex.ledger.open_leg_shares(T.COND, T.UP), 0)
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), 20.0, places=6)

    async def test_a_retrying_trade_still_counts_and_a_failed_one_does_not(self):
        T = self.T
        self.reply("0xa5", tids=("t-a5", "t-a6"))
        p = await self.send("0xa5")
        estimate = self.ex._risk_orders[p.client_order_id]["shares"]
        self.trade("t-a5", "0xa5", 12.0, 0.45, status="RETRYING")        # may still be delivered
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), max(estimate, 12.0), places=6)
        self.trade("t-a6", "0xa5", 30.0, 0.30, status="FAILED")          # never delivered
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), max(estimate, 12.0), places=6)

    async def test_once_booked_the_shares_count_once(self):
        T = self.T
        self.matched("0xa3", making=str(CF.AMOUNT), taking="20")
        await self.send("0xa3")
        self.trade("t-a3", "0xa3", 20.0, 0.45, status="CONFIRMED")
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), 20.0, places=6)   # seen, not yet booked
        self.ex.ledger.ingest_fill_store(self.store)
        self.assertAlmostEqual(self.ex.ledger.open_leg_shares(T.COND, T.UP), 20.0, places=6)
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), 20.0, places=6)   # booked: not twice

    async def test_an_estimate_larger_than_the_fill_still_binds_until_resolved(self):
        T = self.T
        self.reply("0xa4", tids=("t-a4",))
        p = await self.send("0xa4")
        estimate = self.ex._risk_orders[p.client_order_id]["shares"]
        self.trade("t-a4", "0xa4", 4.0, 0.90, status="MATCHED")           # only part seen so far
        self.assertAlmostEqual(self.ex.own_risk_shares(T.UP, T.COND), estimate, places=6)


# =========================================================================== 2
def http_503(_token):
    raise ConnectionError("503 Service Unavailable")                     # fails at once, no timeout


async def inline(fn, *args, **kwargs):
    return fn(*args, **kwargs)


class FailedRestRecoveryFallsBackToTheSocket(unittest.IsolatedAsyncioTestCase):
    def hub(self, fetch):
        hub = FeedHub(rest_book_fetch=fetch, book_refresh_after=.01)
        hub.set_round("A", "B", "condition")
        hub.book.connected = True
        self.assertEqual(hub.book.view("A").status, UNSYNCED)
        return hub

    async def recover(self, hub, times=1):
        with mock.patch("feeds.hub.asyncio.to_thread", new=inline), \
                mock.patch.object(hub.market, "request_snapshot", wraps=hub.market.request_snapshot) as request:
            for _ in range(times):
                hub._next_book_fetch_at.clear()                          # the REST retry gate only
                await hub._resync_token("A", asyncio.Semaphore(1))
        return request

    async def test_a_fast_http_error_requests_the_ordered_ws_snapshot(self):
        hub = self.hub(http_503)
        generation = hub.book.view("A").generation
        request = await self.recover(hub)
        request.assert_called_once_with("A", expected_generation=generation)
        self.assertEqual(hub.market._snapshot_requests, {"A": generation})

    async def test_a_refused_rest_book_requests_it_too(self):
        now_ms = int(time.time() * 1000)
        refused = {
            "crossed": ([(0.6, 10)], [(0.5, 10)]),
            "no venue timestamp": RestBookSnapshot("A", [(0.4, 10)], [(0.5, 10)], 0, time.monotonic()),
            "another token": RestBookSnapshot("B", [(0.4, 10)], [(0.5, 10)], now_ms, time.monotonic()),
        }
        for name, result in refused.items():
            with self.subTest(name):
                hub = self.hub(lambda _token, result=result: result)
                request = await self.recover(hub)
                self.assertEqual(hub.rest_resyncs, 0)
                self.assertEqual(request.call_count, 1)

    async def test_the_token_is_tradeable_once_the_snapshot_lands(self):
        hub = self.hub(http_503)
        await self.recover(hub)
        feed = hub.market
        feed._sent = set(feed._want)

        async def send(raw):
            if json.loads(raw).get("operation") == "subscribe":
                feed._handle(json.dumps({"event_type": "book", "asset_id": "A",
                                         "timestamp": int(time.time() * 1000),
                                         "bids": [{"price": "0.4", "size": "10"}],
                                         "asks": [{"price": "0.55", "size": "10"}]}))

        feed._ws = mock.AsyncMock()
        feed._ws.send.side_effect = send
        await feed._sync_subscriptions()
        self.assertEqual(hub.book.view("A").status, LIVE)
        self.assertEqual(hub.book.view("A").best_ask, 0.55)

    async def test_repeated_fast_errors_stay_rate_limited(self):
        hub = self.hub(http_503)
        request = await self.recover(hub, times=5)
        self.assertEqual(request.call_count, 1)

    async def test_no_resubscribe_churn_while_the_snapshot_is_on_its_way(self):
        # REST hard down and the venue slow to answer the resubscribe: the next fast
        # failure must not unsubscribe the token again before its snapshot can land.
        hub = self.hub(http_503)
        generation = hub.book.view("A").generation
        await self.recover(hub)
        feed = hub.market
        feed._sent = set(feed._want)
        feed._ws = mock.AsyncMock()                                      # no snapshot comes back yet
        await feed._sync_subscriptions()
        frames = [json.loads(c.args[0]) for c in feed._ws.send.call_args_list]
        self.assertEqual([f["operation"] for f in frames], ["unsubscribe", "subscribe"])
        self.assertEqual(hub.book.view("A").generation, generation)      # an UNSYNCED gap keeps it
        request = await self.recover(hub, times=3)
        request.assert_not_called()
        self.assertEqual(feed._snapshot_requests, {})

    async def test_a_live_book_is_never_resubscribed(self):
        hub = self.hub(http_503)
        hub.book.apply_snapshot("A", [(0.4, 10)], [(0.5, 10)])
        await asyncio.sleep(.03)                                         # older than the refresh budget
        request = await self.recover(hub)
        request.assert_not_called()
        self.assertEqual(hub.book.view("A").status, LIVE)


# =========================================================================== 3
class SellHistoryLoadsWithExitsOff(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "live_ledger.json")

    def tearDown(self):
        self.dir.cleanup()

    def trade_with_exits_on(self, *, sell=4.0, snapshot=False, sold_at=None):
        led = Ledger(path=self.path, allow_sells=True)
        self.assertTrue(led.save())                                     # the file exists before the fills
        bought_at = None if sold_at is None else sold_at - 60
        self.assertTrue(led.record_fill_durable("b1", "UP", shares=10.0, price=0.40, condition_id="C",
                                                fee=0.0, wall=bought_at))
        self.assertTrue(led.record_fill_durable("s1", "UP", shares=sell, price=0.70, side="SELL",
                                                condition_id="C", fee=0.0, wall=sold_at))
        if snapshot:
            self.assertTrue(led.save())
        else:
            self.assertEqual(led.journal_pending(), 2)                  # the sale lives in the journal
        return led

    def restart(self, allow_sells=False):
        return Ledger(path=self.path, allow_sells=allow_sells)

    def test_a_journalled_sale_replays_with_exits_off(self):
        before = self.trade_with_exits_on()
        after = self.restart()                                          # raised: refusing to start
        pos = after.positions["UP"]
        self.assertAlmostEqual(pos.shares, 6.0, places=9)
        self.assertAlmostEqual(pos.realized_from_sales, before.positions["UP"].realized_from_sales, places=9)
        self.assertEqual(after.journal_replayed, 2)
        self.assertEqual(after.skipped_side, 0)

    def test_a_snapshotted_sale_loads_with_exits_off(self):
        self.trade_with_exits_on(snapshot=True)
        self.assertAlmostEqual(self.restart().positions["UP"].shares, 6.0, places=9)

    def test_new_sales_are_still_refused_with_exits_off(self):
        self.trade_with_exits_on()
        after = self.restart()
        self.assertFalse(after.record_fill("s2", "UP", shares=1.0, price=0.70, side="SELL",
                                           condition_id="C", fee=0.0))
        self.assertEqual(after.skipped_side, 1)
        self.assertAlmostEqual(after.positions["UP"].shares, 6.0, places=9)

    def test_a_sale_that_closed_the_position_keeps_its_own_date(self):
        sold_at = time.time() - 3600
        self.trade_with_exits_on(sell=10.0, sold_at=sold_at)
        for allow_sells in (True, False):
            with self.subTest(allow_sells=allow_sells):
                after = self.restart(allow_sells)
                pos = after.positions["UP"]
                self.assertTrue(pos.settled)
                self.assertEqual(pos.settled_wall, sold_at)             # was the restart's time
                self.assertEqual(after.realized_since(time.time() - 60), 0.0)
                self.assertAlmostEqual(after.realized_since(sold_at), 3.0, places=9)


# ================================================================ found in the sweep
class ResolverThrottleIsBounded(unittest.TestCase):
    """The per-market API throttle grew by one entry per round for as long as the bot ran."""

    def test_old_markets_are_dropped_and_the_latest_stays_throttled(self):
        from accounting import resolution as R
        now = {"t": 0.0}
        r = R.Resolver(api_every_s=5.0, clock=lambda: now["t"])
        asked = []

        def mirrors(cid, timeout, chain=True):
            asked.append(cid)
            return R.Resolution(cid, R.PENDING, {}, detail="CLOB market still open")

        with mock.patch.object(R, "fetch", mirrors), mock.patch.dict(os.environ, {"SETTLE_ONCHAIN": "0"}):
            cids = [f"0x{i:064x}" for i in range(1000)]
            for cid in cids:
                self.assertEqual(r(cid).status, R.PENDING)
            self.assertLessEqual(len(r._api_at), 256)
            self.assertEqual(len(asked), 1000)
            r(cids[-1])                                                  # inside api_every_s: not asked again
            self.assertEqual(len(asked), 1000)


if __name__ == "__main__":
    unittest.main(verbosity=1)
