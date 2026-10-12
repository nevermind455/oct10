"""Offline accounting, paper execution and manager regressions for the deep audit.

No credentials or venue traffic. ``--benchmark`` measures real journal fsyncs
and a paper run against a fixed public-book fixture.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import io
import json
import pathlib
import statistics
import sys
import tempfile
import time
import types
import unittest
from decimal import Decimal as D
from unittest.mock import patch

import testing_env  # noqa: F401 - must precede bot imports
from accounting.ledger import Ledger
from accounting.resolution import Resolution, RESOLVED
from paper_trade import PaperBroker, BookSnapshot, MarketRules
import position_manager as PM

CID = "0x" + "ab" * 32


def book(token="111"):
    return BookSnapshot(token, ((D("0.40"), D("1000000")),),
                        min_order_size=D("5"), tick_size=D("0.01"),
                        timestamp=str(int(time.time() * 1000)),
                        received_wall=time.time(), best_bid=D("0.39"))


def make_broker(ledger, observer=None, balance=1000.0):
    return PaperBroker(
        ledger, market_context=lambda: {"condition_id": CID,
                                         "up_token_id": "111", "down_token_id": "222"},
        host="https://example.invalid", max_buy_price=0.99,
        start_balance=balance, trade_window_seconds=300,
        book_fetch=book, on_event=observer,
        rules_fetch=lambda _: MarketRules(CID, D("0.07"), 1,
                                            min_order_size=D("5"), tick_size=D("0.01"),
                                            up_token_id="111", down_token_id="222"))


def place(broker, **kw):
    return broker.place_trade("UP", 2.0, "111", "222", CID,
                              window_end=time.time() + 200, **kw)


def broken_observer(*_):
    raise RuntimeError("observer unavailable")


def manager(ledger, observer=None, mono=time.monotonic):
    view = types.SimpleNamespace(updated_mono=100.0, best_ask=0.4,
                                 best_bid=0.39, asks=((0.4, 100.0),), status="LIVE")
    hub = types.SimpleNamespace(up_token="111", down_token="222", condition_id=CID,
                                window_end=time.time() + 200,
                                book=types.SimpleNamespace(view=lambda _: view))
    result = PM.PositionManager(PM.PMConfig(enabled=True), hub=hub, ledger=ledger,
                                 broker=types.SimpleNamespace(), on_event=observer, mono=mono)
    result.hist.condition = CID
    return result


def decision():
    result = PM.Decision("UP", action="BUY", classification="SIDE_AVG_IMPROVEMENT",
                          size=5.0, limit=0.45, stake=2.0)
    result.fill = PM.Fill(qty=5.0, notional=2.0, fees=0.084, vwap=0.4)
    return result


class DurableAccounting(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.tmp.name) / "ledger.json")
        self.ledger = Ledger(self.path, allow_sells=True)

    def tearDown(self):
        self.tmp.cleanup()

    def fill(self, tid, **kw):
        return self.ledger.record_fill_durable(tid, "111", shares=5,
                                                price=0.4, fee=0.084,
                                                condition_id=CID, **kw)

    def test_buy_rollback_preserves_lots_and_reopened_settlement(self):
        self.fill("first")
        self.ledger.settle(Resolution(CID, RESOLVED, {"111": 1.0, "222": 0.0}))
        before = self.ledger._payload_locked()
        original_lot = self.ledger.positions["111"].lots[0]
        with patch.object(self.ledger, "_persist_locked", return_value=False):
            with self.assertRaises(RuntimeError):
                self.fill("failed")
        self.assertEqual(self.ledger._payload_locked(), before)
        self.assertNotIn("failed", self.ledger.seen)
        self.assertEqual(self.ledger.positions["111"].lots[0].consumed, 0)
        self.assertEqual(original_lot.consumed, 0)
        self.assertTrue(Ledger(self.path, allow_sells=True).positions["111"].settled)

    def test_sell_rollback_restores_fifo_consumption_and_cash(self):
        self.fill("first")
        self.fill("second")
        before = self.ledger._payload_locked()
        with patch.object(self.ledger, "_persist_locked", return_value=False):
            with self.assertRaises(RuntimeError):
                self.ledger.record_fill_durable("failed-sale", "111", shares=7,
                                                price=0.5, fee=0.1, side="SELL", condition_id=CID)
        self.assertEqual(self.ledger._payload_locked(), before)
        self.assertEqual([lot.consumed for lot in self.ledger.positions["111"].lots], [0, 0])
        self.assertTrue(self.ledger.record_fill_durable("sale", "111", shares=7,
                                                       price=0.5, fee=0.1, side="SELL", condition_id=CID))
        reloaded = Ledger(self.path, allow_sells=True)
        self.assertEqual(reloaded.positions["111"].shares, 3)
        self.assertAlmostEqual(reloaded.positions["111"].realized_from_sales, 0.4824)

    def test_concurrent_duplicate_fills_commit_once_and_replay(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            outcomes = list(pool.map(lambda _: self.fill("same"), range(128)))
        self.assertEqual(sum(outcomes), 1)
        self.assertEqual(self.ledger.positions["111"].shares, 5)
        self.assertEqual(Ledger(self.path, allow_sells=True).positions["111"].shares, 5)


class ObserverFailures(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(str(pathlib.Path(self.tmp.name) / "ledger.json"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_paper_committed_fill_survives_observer_failure(self):
        broker = make_broker(self.ledger, broken_observer)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertTrue(place(broker))
        self.assertEqual(broker.filled_orders, 1)
        self.assertEqual(len(self.ledger.seen), 1)
        self.assertEqual(broker.last_fill["status"], "FILLED")
        self.assertIsNone(broker.last_error)
        self.assertIn("observer unavailable", broker.last_callback_error)

    def test_paper_rejection_survives_observer_failure(self):
        broker = make_broker(self.ledger, broken_observer)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertFalse(broker.place_trade("BAD", 2.0))
        self.assertEqual(broker.rejected_orders, 1)
        self.assertEqual(len(self.ledger.seen), 0)
        self.assertIn("invalid side", broker.last_error)

    def test_manager_reservation_survives_observer_failure(self):
        mgr = manager(self.ledger, broken_observer)
        order = mgr.reserve(decision())
        self.assertEqual(order.status, "PENDING")
        mgr.settle_order(order, ok=False, fill=None, error="cancelled before send")
        self.assertTrue(order.finished)
        self.assertFalse(mgr.hist.sides["UP"].in_flight)
        self.assertEqual(mgr.hist.spend, 0)
        self.assertIn("observer unavailable", mgr.last_callback_error)

    def test_manager_state_changes_when_cooldown_expires_without_book_update(self):
        now = [100.0]
        mgr = manager(self.ledger, mono=lambda: now[0])
        mgr.hist.sides["UP"].last_done_mono = now[0]
        blocked_version = mgr._state_version()
        now[0] += mgr.cfg.cooldown_ms / 1000.0 + 0.001
        self.assertNotEqual(blocked_version, mgr._state_version())

    def test_manager_state_changes_when_sniper_quiet_expires(self):
        now = [100.0]
        mgr = manager(self.ledger, mono=lambda: now[0])
        mgr.sniper = types.SimpleNamespace(inflight={}, last_fire_by_side={"UP": now[0] * 1000})
        blocked_version = mgr._state_version()
        now[0] += mgr.cfg.sniper_quiet_ms / 1000.0 + 0.001
        self.assertNotEqual(blocked_version, mgr._state_version())


def quantiles(samples):
    rows = sorted(samples)
    return {"n": len(rows), "median_ms": statistics.median(rows),
            "p95_ms": rows[min(len(rows) - 1, int(len(rows) * 0.95))], "max_ms": max(rows)}


def benchmark():
    result = {"scope": "offline real local journal fsync; fixed paper books; no venue latency"}
    with tempfile.TemporaryDirectory() as tmp:
        ledger = Ledger(str(pathlib.Path(tmp) / "ledger.json"))
        for i in range(5000):
            ledger.record_fill(str(i), "111", shares=5, price=0.4, fee=0.084, condition_id=CID)
        ledger.save()
        ledger.JOURNAL_COMPACT_EVERY = 100000
        samples = []
        for i in range(40):
            start = time.perf_counter_ns()
            ledger.record_fill_durable("bench-" + str(i), "111", shares=5, price=0.4,
                                        fee=0.084, condition_id=CID)
            samples.append((time.perf_counter_ns() - start) / 1e6)
        result["durable_fill_5000_existing_lots"] = quantiles(samples)
        reloaded = Ledger(ledger.path)
        result["durable_replay_lots"] = len(reloaded.positions["111"].lots)
    with tempfile.TemporaryDirectory() as tmp:
        ledger = Ledger(str(pathlib.Path(tmp) / "ledger.json"))
        broker = make_broker(ledger, balance=100000)
        samples, success, misses = [], 0, 0
        with contextlib.redirect_stdout(io.StringIO()):
            for _ in range(80):
                start = time.perf_counter_ns()
                if place(broker):
                    success += 1
                else:
                    misses += 1
                samples.append((time.perf_counter_ns() - start) / 1e6)
        result["paper_fixed_book"] = {**quantiles(samples), "success": success,
                                       "missed": misses, "success_rate": success / 80}
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    if "--benchmark" in sys.argv:
        benchmark()
    else:
        unittest.main()
