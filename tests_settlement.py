#!/usr/bin/env python3
"""Settlement: every market's lifecycle from expiry to realized P/L.

    python tests_settlement.py

The 17 required cases are test_01 .. test_17 (named after the case); the rest
cover the resolver (chain first), late fills, timeouts and the hot path. The
real Ledger, SettlementWorker, PaperBroker, run.DashView and dashboard build()
run against a fake clock and a fake result source. No network.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import onchain_resolution  # noqa: E402
import run  # noqa: E402
import tests_paper as TP  # noqa: E402  (PaperBroker fixtures: _broker, book, rules)
from accounting import resolution as R  # noqa: E402
from accounting import settlement as S  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from dashboard import build  # noqa: E402
from dashboard.theme import UNICODE  # noqa: E402

END = 1_791_374_100                     # a 5-minute boundary (exchange time)
A, B = "0x" + "aa" * 32, "0x" + "bb" * 32
A_UP, A_DN, B_UP, B_DN = "1001", "1002", "2001", "2002"


def resolved(cid, up, down, winner="Up"):
    pay = {up: 1.0, down: 0.0} if winner.lower() == "up" else {up: 0.0, down: 1.0}
    return R.Resolution(cid, R.RESOLVED, pay, winning_label=winner, detail="on-chain ConditionResolution")


def waiting(cid):
    return R.Resolution(cid, R.PENDING, {}, detail="not resolved on chain yet")


def frame_text(snap, cols=120, rows=40) -> str:
    return "\n".join("".join(t for t, _ in row) for row in build(snap, cols, rows, UNICODE))


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        self.broker = TP._broker(self.tmp.name, balance=100)
        self.led = self.broker.ledger
        self.clock = {"t": END - 60.0}
        self.events: list[str] = []
        self.results: dict = {}           # cid -> Resolution (or Exception) the fake source answers
        self.fetches: list = []

    def tearDown(self):
        self.tmp.cleanup()

    # ---- fixtures
    def fill(self, tid, token, cid, shares, price, fee=0.0):
        self.assertTrue(self.led.record_fill_durable(tid, token, shares=shares, price=price, condition_id=cid,
                                                     fee=fee, source="test"))

    def fetch(self, cid):
        self.fetches.append((cid, threading.current_thread().name))
        r = self.results.get(cid)
        if isinstance(r, Exception):
            raise r
        if callable(r):
            return r(cid)
        return r or waiting(cid)

    def worker(self, **kw):
        kw.setdefault("interval", 0.01)
        kw.setdefault("tick_s", 0.005)
        kw.setdefault("poll_active", False)
        kw.setdefault("paper", True)
        return S.SettlementWorker(self.led, fetch=self.fetch, clock=lambda: self.clock["t"],
                                  on_event=lambda _s, text, _lv: self.events.append(text),
                                  journal_path=self.dir / "ledger_settlements.jsonl", **kw)

    def track(self, w, cid, end, up, down):
        return w.track(cid, end - 300, end, up_token=up, down_token=down, slug=f"btc-updown-5m-{end - 300}")

    def names(self, cid=None):
        out = []
        for text in self.events:
            if cid is None or f"btc-updown-5m-{self._start(cid)}" in text:
                out.append(text.split(" | ")[0])
        return out

    def _start(self, cid):
        return {A: END - 300, B: END}[cid]

    async def until(self, cond, timeout=3.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if cond():
                return True
            await asyncio.sleep(0.005)
        return cond()

    def run_async(self, coro):
        return asyncio.run(coro)

    def realized_total(self):
        return self.led.summary()["realized_pnl"]

    async def settle_one(self, cid, up, down, winner, end=END):
        """Track, expire and settle one market through the running worker."""
        w = self.worker()
        self.track(w, cid, end, up, down)
        w.start()
        try:
            self.clock["t"] = end + 0.5
            ok = await self.until(lambda: w.records[cid].state == S.PENDING)
            self.assertTrue(ok, w.records[cid].state)
            self.results[cid] = resolved(cid, up, down, winner)
            self.assertTrue(await self.until(lambda: w.records[cid].state == S.SETTLED), w.records[cid])
        finally:
            await w.stop()
        return w, w.records[cid]


# ============================================================== the 17 cases
class RequiredCases(Base):
    def test_01_normal_up_settlement(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w, rec = await self.settle_one(A, A_UP, A_DN, "Up")
            self.assertEqual(rec.winner, "UP")
            self.assertAlmostEqual(rec.realized, 10.0 - 4.5)
            self.assertEqual(self.names(A), ["MARKET_EXPIRED", "SETTLEMENT_PENDING", "RESULT_RECEIVED",
                                             "ACCOUNTING_SETTLED"])
            self.assertTrue(self.led.positions[A_UP].settled)
            self.assertEqual(self.led.positions[A_UP].payout_per_share, 1.0)
        self.run_async(go())

    def test_02_normal_down_settlement(self):
        self.fill("t1", A_DN, A, 10.0, 0.40)

        async def go():
            w, rec = await self.settle_one(A, A_UP, A_DN, "Down")
            self.assertEqual(rec.winner, "DOWN")
            self.assertAlmostEqual(rec.realized, 10.0 - 4.0)
            self.assertIn("winner=DOWN", next(e for e in self.events if e.startswith("RESULT_RECEIVED")))
        self.run_async(go())

    def test_03_bot_owns_winning_side(self):
        self.fill("t1", A_UP, A, 10.0, 0.45, fee=0.05)
        cash_before = self.broker.cash_balance()

        async def go():
            w, rec = await self.settle_one(A, A_UP, A_DN, "Up")
            view = rec.view(self.clock["t"])
            self.assertEqual(view["settlement_price"], 1.0)
            self.assertAlmostEqual(rec.realized, 10.0 - 4.5 - 0.05)
            # the winning payout ($1/share) reaches paper cash exactly once, at settlement
            self.assertAlmostEqual(self.broker.cash_balance(), cash_before + 10.0)
            self.assertEqual(rec.redeem, S.REDEEM_PAPER)
        self.run_async(go())

    def test_04_bot_owns_losing_side(self):
        self.fill("t1", A_UP, A, 10.0, 0.45, fee=0.05)
        cash_before = self.broker.cash_balance()

        async def go():
            w, rec = await self.settle_one(A, A_UP, A_DN, "Down")
            self.assertAlmostEqual(rec.realized, -(4.5 + 0.05))
            self.assertEqual(self.led.positions[A_UP].payout_per_share, 0.0)
            self.assertAlmostEqual(self.broker.cash_balance(), cash_before)      # cost was paid at the fill
            self.assertEqual(rec.state, S.SETTLED)
        self.run_async(go())

    def test_05_bot_owns_both_sides(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        self.fill("t2", A_DN, A, 8.0, 0.50)

        async def go():
            w, rec = await self.settle_one(A, A_UP, A_DN, "Up")
            self.assertTrue(self.led.positions[A_UP].settled and self.led.positions[A_DN].settled)
            self.assertAlmostEqual(rec.realized, 10.0 - 4.5 - 4.0)
            self.assertAlmostEqual(self.led.positions[A_DN].realized, -4.0)
            # both legs in ONE atomic ledger write: one settlement time for both
            self.assertEqual(self.led.positions[A_UP].settled_wall, self.led.positions[A_DN].settled_wall)
            self.assertEqual(sum(1 for e in self.events if e.startswith("ACCOUNTING_SETTLED")), 1)
        self.run_async(go())

    def test_06_partial_fill_positions(self):
        # two partial IOC fills (5.435 of 10, then 3.2 of a carried 14) on the same side
        self.fill("t1", A_UP, A, 5.435, 0.46, fee=0.0175)
        self.fill("t2", A_UP, A, 3.2, 0.47, fee=0.0099)

        async def go():
            w, rec = await self.settle_one(A, A_UP, A_DN, "Up")
            cost = 5.435 * 0.46 + 0.0175 + 3.2 * 0.47 + 0.0099
            self.assertAlmostEqual(self.led.positions[A_UP].shares, 8.635)
            self.assertAlmostEqual(rec.realized, 8.635 - cost, places=9)
            self.assertIn("UP 8.635sh", next(e for e in self.events if e.startswith("ACCOUNTING_SETTLED")))
        self.run_async(go())

    def test_07_result_delayed_several_seconds(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            w.start()
            try:
                ages = []
                for t in (0.5, 2.0, 4.0, 6.0):
                    self.clock["t"] = END + t
                    await asyncio.sleep(0.05)
                    rec = w.records[A]
                    self.assertEqual(rec.state, S.PENDING)
                    self.assertFalse(self.led.positions[A_UP].settled)        # never settled on a timer
                    ages.append(w.panel(self.clock["t"])["subject"]["age_s"])
                self.assertEqual(ages, sorted(ages))                            # the age keeps counting
                self.assertAlmostEqual(ages[-1], 6.0)
                self.assertGreater(len([f for f in self.fetches if f[0] == A]), 4)
                self.clock["t"] = END + 6.5
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                self.assertAlmostEqual(w.records[A].view(self.clock["t"])["settlement_s"], 6.5)
            finally:
                await w.stop()
        self.run_async(go())

    def test_08_api_temporarily_fails(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        calls = {"n": 0}

        def flaky(cid):
            calls["n"] += 1
            if calls["n"] <= 3:
                raise ConnectionError("polygon rpc timed out")
            if calls["n"] <= 5:
                return R.Resolution(cid, R.PENDING, {}, detail="CLOB resolution fetch failed: SSLError")
            return resolved(cid, A_UP, A_DN, "Up")

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = flaky
            self.clock["t"] = END + 1
            w.start()
            try:
                self.assertTrue(await self.until(lambda: calls["n"] >= 2))
                self.assertEqual(w.health_status(), "ERROR")                    # visible while it fails
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                self.assertEqual(w.health_status(), "OK")                       # and clears once it works
                api = [e for e in self.events if e.startswith("RESULT_API_ERROR")]
                self.assertEqual(len(api), 2, api)                             # once per distinct failure, not per retry
                self.assertEqual(self.led.summary()["settled_positions"], 1)
            finally:
                await w.stop()
        self.run_async(go())

    def test_09_duplicate_result_notification(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        cash_before = self.broker.cash_balance()

        def slow_resolved(cid):
            time.sleep(0.05)                                                    # two checks overlap
            return resolved(cid, A_UP, A_DN, "Up")

        async def go():
            w = self.worker(max_inflight=2, interval=0.01)
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = slow_resolved
            self.clock["t"] = END + 1
            w.start()
            try:
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                await asyncio.sleep(0.1)
                # a repeated notification of the same final result
                self.assertEqual(await w._apply_result(w.records[A], resolved(A, A_UP, A_DN, "Up")), 0)
            finally:
                await w.stop()
            self.assertGreaterEqual(w.duplicate_results, 1)
            self.assertEqual(sum(1 for e in self.events if e.startswith("RESULT_RECEIVED")), 1)
            self.assertEqual(sum(1 for e in self.events if e.startswith("ACCOUNTING_SETTLED")), 1)
            self.assertAlmostEqual(self.broker.cash_balance(), cash_before + 10.0)
        self.run_async(go())

    def test_10_settlement_function_called_twice(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        cash_before = self.broker.cash_balance()
        res = resolved(A, A_UP, A_DN, "Up")

        async def go():
            w = self.worker()
            rec = self.track(w, A, END, A_UP, A_DN)
            rec.state, rec.resolution, rec.winner, rec.payouts = S.PENDING, res, "UP", dict(res.payouts)
            first = await w._settle(rec)
            second = await w._settle(rec)
            self.assertEqual((first, second), (1, 0))
            self.assertEqual(self.led.settle(res), [])                        # the ledger itself too
            self.assertEqual(await w.run_once(), 0)                             # and a whole extra pass
            await w.stop()
            self.assertAlmostEqual(rec.realized, 5.5)
            self.assertAlmostEqual(self.realized_total(), 5.5)
            self.assertAlmostEqual(self.broker.cash_balance(), cash_before + 10.0)
        self.run_async(go())

    def test_11_new_market_starts_while_previous_pending(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            w.start()
            try:
                self.clock["t"] = END + 1.0
                self.assertTrue(await self.until(lambda: w.records[A].state == S.PENDING))
                self.track(w, B, END + 300, B_UP, B_DN)                         # rotation: the next round
                self.fill("t2", B_DN, B, 6.0, 0.52)                             # ... and it trades
                await asyncio.sleep(0.1)
                self.assertEqual(w.records[B].state, S.ACTIVE)
                self.assertEqual(w.records[A].state, S.PENDING)
                p = w.panel(self.clock["t"], active_condition=B)
                self.assertEqual(p["active"]["condition_id"], B)
                self.assertEqual([v["condition_id"] for v in p["pending"]], [A])
                self.assertFalse(any(cid == B for cid, _ in self.fetches))     # an active market is never asked
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                self.assertFalse(self.led.positions[B_DN].settled)             # the new market's position is untouched
                self.assertEqual(w.records[B].state, S.ACTIVE)
            finally:
                await w.stop()
        self.run_async(go())

    def test_12_two_markets_pending_simultaneously(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        self.fill("t2", B_DN, B, 4.0, 0.30)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.track(w, B, END + 300, B_UP, B_DN)
            w.start()
            try:
                self.clock["t"] = END + 300 + 1.0
                self.assertTrue(await self.until(lambda: all(r.state == S.PENDING for r in w.records.values())))
                p = w.panel(self.clock["t"])
                ages = {v["condition_id"]: v["age_s"] for v in p["pending"]}
                self.assertAlmostEqual(ages[A], 301.0)
                self.assertAlmostEqual(ages[B], 1.0)
                self.assertEqual(p["subject"]["condition_id"], A)              # the oldest is shown first
                self.results[B] = resolved(B, B_UP, B_DN, "Down")               # the NEWER one resolves first
                self.assertTrue(await self.until(lambda: w.records[B].state == S.SETTLED))
                self.assertEqual(w.records[A].state, S.PENDING)
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                self.assertAlmostEqual(w.records[A].realized, 5.5)
                self.assertAlmostEqual(w.records[B].realized, 4.0 - 1.2)
            finally:
                await w.stop()
        self.run_async(go())

    def test_13_settlement_while_a_new_sniper_order_is_executing(self):
        """Market A settles while a paper order on market B is mid-execution
        (blocked inside its book read on another thread): neither waits for
        the other, and the books add up afterwards."""
        ctx = {"condition_id": "cond", "up_token_id": "up", "down_token_id": "down"}
        release, entered = threading.Event(), threading.Event()

        def blocking_book(token):
            entered.set()
            release.wait(5)
            return TP.book(token)

        broker = TP._broker(self.tmp.name + "/b13", balance=100, context=ctx)
        broker._book_fetch = blocking_book
        self.broker, self.led = broker, broker.ledger
        self.fill("t1", A_UP, A, 10.0, 0.45)
        cash0 = broker.cash_balance()
        outcome = {}
        order = threading.Thread(target=lambda: outcome.setdefault("ok", TP.paper_order(broker, "UP", 2)),
                                 name="order")

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = resolved(A, A_UP, A_DN, "Up")
            order.start()
            self.assertTrue(entered.wait(2))
            self.clock["t"] = END + 1
            w.start()
            try:
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                self.assertTrue(order.is_alive())                               # settled while the order was in flight
            finally:
                await w.stop()
            release.set()
            await asyncio.to_thread(order.join, 5)
        self.run_async(go())
        self.assertTrue(outcome.get("ok"), broker.last_error)
        new = broker.ledger.positions["up"]
        self.assertFalse(new.settled)
        self.assertTrue(self.led.positions[A_UP].settled)
        self.assertAlmostEqual(broker.cash_balance(), cash0 + 10.0 - new.cost)

    def test_14_dashboard_active_to_pending_to_settled(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        def snap(w, active):
            p = w.panel(self.clock["t"], active_condition=active)
            for v in [p["active"], p["subject"], p["last_settled"]] + p["pending"] + p["previous"]:
                if v is not None:
                    v["label"] = "14:00-14:05" if v["condition_id"] == A else "14:05-14:10"
            return {"mode": "PAPER", "settlement": p, "market": {"slug": "btc-updown-5m-x", "seconds_left": 30.0},
                    "account": {"kind": "PAPER", "cash": 95.5, "equity": 100.0, "pnl": 0.0}}

        def status_row(text):
            return next(ln for ln in text.splitlines() if "MARKET STATUS" in ln)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            w.start()
            try:
                await asyncio.sleep(0.03)
                active = frame_text(snap(w, A))
                self.assertIn("MARKET STATUS ACTIVE", " ".join(status_row(active).split()))
                self.assertIn(" ACTIVE ", active.splitlines()[0])               # header badge
                self.clock["t"] = END + 1.4
                self.assertTrue(await self.until(lambda: w.records[A].state == S.PENDING))
                pending = frame_text(snap(w, None))
                self.assertIn("PENDING SETTLEMENT", status_row(pending))
                self.assertIn("WAITING", pending)
                self.assertRegex(pending, r"PENDING AGE\s+1\.4s")
                self.assertIn("1 PENDING SETTLEMENT", pending.splitlines()[0])
                self.clock["t"] = END + 2.8
                self.assertRegex(frame_text(snap(w, None)), r"PENDING AGE\s+2\.8s")   # keeps counting
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                settled = frame_text(snap(w, None))
                self.assertIn("SETTLED", status_row(settled))
                self.assertRegex(settled, r"WINNER\s+UP")
                self.assertRegex(settled, r"SETTLE PRICE\s+\$1\.00")
                self.assertRegex(settled, r"REALIZED P/L\s+\+\$5\.50")
                self.assertRegex(settled, r"SETTLE TIME\s+2\.8s")
                for text in (active, pending, settled):                        # never blank
                    self.assertNotRegex(status_row(text), r"MARKET STATUS\s+│")
            finally:
                await w.stop()
        self.run_async(go())

    # ---- 15 / 16 through run.DashView: the dashboard's own account math
    def dash(self, w, hub):
        rt = run.Runtime(paper=True, hub=hub, cfg=None)
        rt.ledger, rt.broker, rt.settler = self.led, self.broker, w
        return run.DashView(rt)

    def hub(self, cid, up, down, bid_up=None, bid_down=None):
        bids = {up: bid_up, down: bid_down}

        def view(token):
            bid = bids.get(str(token))
            return types.SimpleNamespace(best_bid=bid, best_ask=None if bid is None else bid + 0.02,
                                         status="LIVE" if bid is not None else "STALE", updated_mono=time.monotonic())
        return types.SimpleNamespace(condition_id=cid, up_token=up, down_token=down, window_start=END - 300,
                                     window_end=END, book=types.SimpleNamespace(view=view), health=lambda: {})

    def account_ok(self, a):
        self.assertAlmostEqual(a["cash"] + a["position_value"], a["equity"])
        self.assertAlmostEqual(a["realized"] + a["unrealized"], a["total"])
        self.assertAlmostEqual(a["equity"] - a["start"], a["total"])

    def test_15_pnl_moves_from_unrealized_to_realized_exactly_once(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            view = self.dash(w, self.hub(A, A_UP, A_DN, bid_up=0.60, bid_down=0.38))
            before = view._accounting_nowait()
            self.assertAlmostEqual(before["unrealized"], 10 * 0.60 - 4.5)
            self.assertAlmostEqual(before["realized"], 0.0)
            w.start()
            try:
                self.clock["t"] = END + 1
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                for _ in range(5):                                              # more passes change nothing
                    self.assertEqual(await w.run_once(), 0)
                    await asyncio.sleep(0.02)
            finally:
                await w.stop()
            view.rt.hub = self.hub(B, B_UP, B_DN)                               # the next round trades now
            after = view._accounting_nowait()
            self.assertAlmostEqual(after["realized"], 5.5)
            self.assertAlmostEqual(after["unrealized"], 0.0)
            self.assertEqual(self.led.summary()["settled_positions"], 1)
            self.assertEqual(sum(1 for e in self.events if e.startswith("ACCOUNTING_SETTLED")), 1)
        self.run_async(go())

    def test_16_equity_remains_consistent_through_settlement(self):
        self.fill("t1", A_UP, A, 10.0, 0.45, fee=0.05)
        start = self.broker.start_balance

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            view = self.dash(w, self.hub(A, A_UP, A_DN, bid_up=0.60))
            live = view._accounting_nowait()                                    # ACTIVE: marked to the bid
            self.account_ok(live)
            self.assertAlmostEqual(live["position_value"], 6.0)
            # expired: the round's tokens are gone from the book; held at cost, P/L flat
            view.rt.hub = self.hub(B, B_UP, B_DN)
            w.start()
            try:
                self.clock["t"] = END + 1
                self.assertTrue(await self.until(lambda: w.records[A].state == S.PENDING))
                pending = view._accounting_nowait()
                self.account_ok(pending)
                self.assertAlmostEqual(pending["position_value"], 4.55)
                self.assertAlmostEqual(pending["equity"], start)
                self.assertEqual(view._positions["pending"]["markets"], 1)
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
            finally:
                await w.stop()
            done = view._accounting_nowait()
            self.account_ok(done)
            self.assertAlmostEqual(done["position_value"], 0.0)
            self.assertAlmostEqual(done["equity"], start - 4.55 + 10.0)
            self.assertAlmostEqual(done["total"], done["realized"])
            self.assertAlmostEqual(done["realized"], 10.0 - 4.55)
        self.run_async(go())

    def test_17_redemption_states(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        self.fill("t2", B_UP, B, 5.0, 0.40)                                      # B: a losing position only
        balances = {A_UP: [10.0, 10.0, 0.0]}
        reads = []

        def balance(token):
            reads.append(token)
            seq = balances.get(token, [0.0])
            value = seq.pop(0) if len(seq) > 1 else seq[0]
            if value is None:
                return None, "polygon rpc unavailable (test)"
            return value, "on-chain balanceOf"

        async def go():
            w = self.worker(paper=False, redeem_check=balance, redeem_every_s=0.02)
            self.track(w, A, END, A_UP, A_DN)
            self.track(w, B, END, B_UP, B_DN)
            w.start()
            try:
                self.clock["t"] = END + 1
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                self.results[B] = resolved(B, B_UP, B_DN, "Down")
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED
                                                 and w.records[B].state == S.SETTLED))
                self.assertEqual(w.records[A].redeem, S.REDEEM_PENDING)        # settled is NOT redeemed
                self.assertEqual(w.records[B].redeem, S.REDEEM_NOT_NEEDED)
                self.assertTrue(await self.until(lambda: w.panel(self.clock["t"])["redeem_pending"] == 10.0))
                self.assertAlmostEqual(w.panel(self.clock["t"])["redeem_pending"], 10.0)
                self.assertTrue(await self.until(lambda: w.records[A].redeem == S.REDEEM_UNVERIFIED))
                self.assertGreaterEqual(len(reads), 3)                          # token absence does not prove cash
                self.assertAlmostEqual(w.panel(self.clock["t"])["redeem_pending"], 0.0)
                self.assertAlmostEqual(w.records[A].redeem_unverified_amount, 10.0)
                self.assertIsNone(w.records[A].redeemed_at)
            finally:
                await w.stop()
            names = [e.split(" | ")[0] for e in self.events]
            self.assertLess(names.index("ACCOUNTING_SETTLED"), names.index("REDEEM_PENDING"))
            self.assertLess(names.index("REDEEM_PENDING"), names.index("REDEEM_BALANCE"))
            self.assertNotIn("REDEEMED", names)
            self.assertIn("collateral proceeds are not verified",
                          next(e for e in self.events if "unverified=$10.00" in e))
        self.run_async(go())

    def test_17b_redeem_check_failure_is_reported_not_assumed(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker(paper=False, redeem_check=lambda _t: (None, "polygon rpc unavailable (test)"),
                            redeem_every_s=0.02)
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = resolved(A, A_UP, A_DN, "Up")
            self.clock["t"] = END + 1
            w.start()
            try:
                self.assertTrue(await self.until(lambda: any(e.startswith("REDEEM_FAILED") for e in self.events)))
                await asyncio.sleep(0.1)
                self.assertEqual(w.records[A].redeem, S.REDEEM_PENDING)
            finally:
                await w.stop()
            self.assertEqual(sum(1 for e in self.events if e.startswith("REDEEM_FAILED")), 1)
        self.run_async(go())


# ================================================================ the rest
class MoreCases(Base):
    def test_a_late_fill_after_settlement_is_settled_with_the_same_result(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = resolved(A, A_UP, A_DN, "Up")
            self.clock["t"] = END + 1
            w.start()
            try:
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                self.fill("t2", A_UP, A, 2.0, 0.50)                              # confirmed after settlement
                self.assertTrue(await self.until(lambda: any(e.startswith("LATE_FILL") for e in self.events)))
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED
                                                 and self.led.positions[A_UP].settled))
            finally:
                await w.stop()
            self.assertAlmostEqual(self.led.positions[A_UP].realized, 12.0 - 4.5 - 1.0)
            self.assertAlmostEqual(w.records[A].realized, 6.5)
        self.run_async(go())

    def test_a_position_left_from_a_previous_run_is_adopted_and_settled(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        self.led.positions[A_UP].lots[0].wall = END - 200                        # bought in A's round

        async def go():
            w = self.worker()                                                    # nothing tracked: a restart
            self.results[A] = resolved(A, A_UP, A_DN, "Up")
            self.clock["t"] = END + 30
            w.start()
            try:
                self.assertTrue(await self.until(lambda: A in w.records and w.records[A].state == S.SETTLED))
            finally:
                await w.stop()
            rec = w.records[A]
            self.assertEqual((rec.source, rec.window_end), ("ledger", END))
            self.assertEqual(rec.side_of(A_UP), "UP")
        self.run_async(go())

    def test_timeout_is_reported_once_and_checking_continues(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker(timeout_s=10.0)
            self.track(w, A, END, A_UP, A_DN)
            w.start()
            try:
                self.clock["t"] = END + 11
                self.assertTrue(await self.until(lambda: w.records[A].timed_out))
                await asyncio.sleep(0.05)
                self.assertEqual(sum(1 for e in self.events if e.startswith("SETTLEMENT_TIMEOUT")), 1)
                self.results[A] = resolved(A, A_UP, A_DN, "Up")
                w.records[A].next_check = 0.0
                self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
            finally:
                await w.stop()
        self.run_async(go())

    def test_accounting_error_is_reported_and_retried_with_the_same_result(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        real, real_append = self.led._write_payload, self.led._append_journal
        fails = {"n": 2}

        def flaky_write(payload):
            if fails["n"] > 0:
                fails["n"] -= 1
                return False
            return real(payload)

        def flaky_append(entry):                    # the journal is the settlement's durable write now
            if fails["n"] > 0:
                fails["n"] -= 1
                return False
            return real_append(entry)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = resolved(A, A_UP, A_DN, "Up")
            self.clock["t"] = END + 1
            with (mock.patch.object(self.led, "_write_payload", flaky_write),
                  mock.patch.object(self.led, "_append_journal", flaky_append)):
                w.start()
                try:
                    self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED))
                finally:
                    await w.stop()
            self.assertEqual(sum(1 for e in self.events if e.startswith("ACCOUNTING_ERROR")), 1)
            self.assertEqual(sum(1 for e in self.events if e.startswith("RESULT_RECEIVED")), 1)
            self.assertTrue(self.led.positions[A_UP].settled)
        self.run_async(go())

    def test_a_result_that_is_not_final_never_settles(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = R.Resolution(A, R.PENDING, {}, detail="oracle resolution status is proposed")
            self.clock["t"] = END + 1
            w.start()
            try:
                self.assertTrue(await self.until(lambda: any(e.startswith("RESULT_NOT_FINAL") for e in self.events)))
                await asyncio.sleep(0.05)
            finally:
                await w.stop()
            self.assertFalse(self.led.positions[A_UP].settled)
            self.assertEqual(w.records[A].state, S.PENDING)
        self.run_async(go())

    def test_the_settlement_journal_has_every_event_with_timestamps(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)

        async def go():
            await self.settle_one(A, A_UP, A_DN, "Up")
        self.run_async(go())
        import json
        rows = [json.loads(line) for line in (self.dir / "ledger_settlements.jsonl").read_text().splitlines()]
        self.assertEqual([r["event"] for r in rows], ["MARKET_EXPIRED", "SETTLEMENT_PENDING", "RESULT_RECEIVED",
                                                      "ACCOUNTING_SETTLED"])
        self.assertTrue(all(r["condition_id"] == A and r["window_end"] == END and "clock" in r for r in rows))
        self.assertEqual(rows[-1]["pnl"], "+5.50")


class HotPath(Base):
    """Settlement never adds a blocking call, sleep or lock wait to the loop
    that runs the sniper (Binance tick -> signal -> sign -> send)."""

    def test_checks_and_ledger_writes_run_on_the_workers_own_pool(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        threads = []
        real_settle = self.led.settle

        def settle(res):
            threads.append(threading.current_thread().name)
            return real_settle(res)

        async def go():
            with mock.patch.object(self.led, "settle", settle):
                await self.settle_one(A, A_UP, A_DN, "Up")
        self.run_async(go())
        self.assertTrue(self.fetches and all(name.startswith("settle-net") for _c, name in self.fetches),
                        self.fetches)
        self.assertTrue(threads and all(name.startswith("settle-io") for name in threads), threads)

    def test_hung_result_reads_never_hold_up_another_markets_bookkeeping(self):
        """Every network thread stuck on a hung RPC: another market still
        expires on time, and its result, once read, is booked at once."""
        self.fill("t1", A_UP, A, 10.0, 0.45)
        self.fill("t2", B_UP, B, 4.0, 0.50)
        hang = threading.Event()

        def hung(cid):
            hang.wait(5)
            return waiting(cid)

        async def go():
            w = self.worker(workers=2, max_inflight=2)
            self.track(w, A, END, A_UP, A_DN)
            self.track(w, B, END + 300, B_UP, B_DN)
            self.results[A] = hung
            w.start()
            try:
                self.clock["t"] = END + 1
                self.assertTrue(await self.until(lambda: w.records[A].inflight == 2))   # both net threads stuck
                self.clock["t"] = END + 301
                self.assertTrue(await self.until(lambda: w.records[B].state == S.PENDING, timeout=1.0))
                self.assertIn("positions=UP 4.000sh", next(e for e in self.events
                                                          if e.startswith("MARKET_EXPIRED") and "1791374100" in e))
                rec = w.records[B]
                rec.resolution, rec.winner = resolved(B, B_UP, B_DN, "Up"), "UP"
                t0 = time.perf_counter()
                self.assertEqual(await w._settle(rec), 1)                            # accounting pool is free
                self.assertLess(time.perf_counter() - t0, 1.0)
            finally:
                hang.set()
                await w.stop()
        self.run_async(go())

    def test_a_slow_result_source_and_a_slow_ledger_write_never_stall_the_event_loop(self):
        self.fill("t1", A_UP, A, 10.0, 0.45)
        real, real_append = self.led._write_payload, self.led._append_journal

        def slow_write(payload):
            time.sleep(0.3)                                                     # a slow fsync (OneDrive, a busy disk)
            return real(payload)

        def slow_append(entry):
            time.sleep(0.3)
            return real_append(entry)

        def slow_fetch(cid):
            time.sleep(0.3)                                                     # a slow RPC
            return resolved(cid, A_UP, A_DN, "Up")

        async def go():
            gaps = []

            async def ticker():
                last = time.perf_counter()
                while True:
                    await asyncio.sleep(0.005)
                    now = time.perf_counter()
                    gaps.append(now - last)
                    last = now

            w = self.worker()
            self.track(w, A, END, A_UP, A_DN)
            self.results[A] = slow_fetch
            self.clock["t"] = END + 1
            probe = asyncio.create_task(ticker())
            with (mock.patch.object(self.led, "_write_payload", slow_write),
                  mock.patch.object(self.led, "_append_journal", slow_append)):
                w.start()
                try:
                    self.assertTrue(await self.until(lambda: w.records[A].state == S.SETTLED, timeout=5))
                finally:
                    await w.stop()
            probe.cancel()
            self.assertLess(max(gaps), 0.1, f"the loop stalled {max(gaps) * 1000:.0f}ms")
        self.run_async(go())

    def test_the_paper_book_netting_never_waits_for_the_broker_lock(self):
        """The sniper nets its own recent fills off the book on the event loop.
        A booking holds the broker lock while the ledger fsyncs - and behind a
        settlement's fsync - so this read must not take that lock."""
        broker = self.broker
        broker.own_fill_ttl_s = 2.0
        broker._own_fills["up"] = [(time.monotonic(), TP.D("0.40"), TP.D("2"))]
        held, release = threading.Event(), threading.Event()

        def hold():
            with broker._lock:
                held.set()
                release.wait(2)

        t = threading.Thread(target=hold)
        t.start()
        try:
            self.assertTrue(held.wait(1))
            t0 = time.perf_counter()
            netted = broker._net_of_own_fills(TP.book("up"))
            took = time.perf_counter() - t0
        finally:
            release.set()
            t.join()
        self.assertLess(took, 0.05)
        self.assertEqual(netted.asks[0], (TP.D("0.40"), TP.D("1")))              # our 2 shares are off the level


class ResolverCases(unittest.TestCase):
    """resolution.Resolver: the chain first (outcome order read once), the
    CLOB + Gamma mirrors only when the chain cannot answer."""
    CID = "0x" + "cd" * 32

    def clob(self, *, closed=False):
        return {"condition_id": self.CID, "closed": closed, "market_slug": "btc-updown-5m-1791373800",
                "tokens": [{"token_id": "111", "outcome": "Up", "price": 0.5, "winner": False},
                           {"token_id": "222", "outcome": "Down", "price": 0.5, "winner": False}]}

    def setUp(self):
        self.gets = []
        self.chain = {"answer": (None, "not resolved on chain yet")}
        response = types.SimpleNamespace(status_code=200, raise_for_status=lambda: None, json=lambda: self.clob())

        def get(url, **_kw):
            self.gets.append(url)
            return response

        # the outcome-slot proof (audit F5): by default the chain proves slot 0 = "111"
        self.proof = {"answer": ({"111": 0, "222": 1}, "outcome slots proven on chain (test)")}
        self.slot_calls = []

        def payouts_for(cid, tokens):
            self.slot_calls.append(list(tokens))                 # the order the chain is asked in
            ans = self.chain["answer"]
            if ans[0] is None:
                return ans
            by_slot = list(ans[0].values())                      # the chain's vector, slot order
            return {tokens[0]: by_slot[0], tokens[1]: by_slot[1]}, ans[1]

        self.patches = [mock.patch.object(R.http_pool, "get", get),
                        mock.patch.object(onchain_resolution, "payouts_for", payouts_for),
                        mock.patch.object(onchain_resolution, "slot_positions",
                                          lambda cid, tokens: self.proof["answer"]),
                        mock.patch.dict(os.environ, {"SETTLE_ONCHAIN": "1"})]
        for p in self.patches:
            p.start()
        self.now = {"t": 0.0}
        self.r = R.Resolver(api_every_s=5.0, clock=lambda: self.now["t"])

    def tearDown(self):
        for p in reversed(self.patches):
            p.stop()

    def test_the_outcome_order_is_read_once_then_each_poll_is_one_chain_read(self):
        for _ in range(5):
            self.assertEqual(self.r(self.CID).status, R.PENDING)
        self.assertEqual(len(self.gets), 1)                                     # one CLOB GET, no Gamma
        self.chain["answer"] = ({"111": 0.0, "222": 1.0}, "on-chain ConditionResolution")
        res = self.r(self.CID)
        self.assertTrue(res.resolved)
        self.assertEqual((res.winning_label, res.payouts), ("Down", {"111": 0.0, "222": 1.0}))
        self.assertEqual(len(self.gets), 1)

    def test_an_unreachable_chain_falls_back_to_the_mirrors_at_most_every_api_every_s(self):
        self.chain["answer"] = (None, "polygon rpc unavailable (ConnectError)")
        for t in (0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0):
            self.now["t"] = t
            res = self.r(self.CID)
            self.assertFalse(res.resolved)
            self.assertIn("polygon rpc unavailable", res.detail)
        mirror_gets = [u for u in self.gets]
        # 1 slot read + the mirrors at t=0 and t=5 (each a CLOB GET; the CLOB says still open)
        self.assertEqual(len(mirror_gets), 3, mirror_gets)

    def test_the_chain_off_switch_uses_the_mirrors_only(self):
        with mock.patch.dict(os.environ, {"SETTLE_ONCHAIN": "0"}):
            self.assertFalse(self.r.prepare(self.CID))
            res = self.r(self.CID)
        self.assertEqual(res.detail, "CLOB market still open")

    # ---- audit F5: which token each payout slot pays is proven, not assumed
    def test_a_proven_reversed_slot_order_maps_payouts_by_the_proof(self):
        self.proof["answer"] = ({"222": 0, "111": 1}, "outcome slots proven on chain (test)")   # CLOB lists them reversed
        self.chain["answer"] = ({"s0": 1.0, "s1": 0.0}, "on-chain ConditionResolution")         # slot 0 won
        res = self.r(self.CID)
        self.assertTrue(res.resolved)
        self.assertEqual(self.slot_calls[-1], ["222", "111"])                   # asked in proven slot order
        self.assertEqual((res.payouts, res.winning_label), ({"222": 1.0, "111": 0.0}, "Down"))
        self.assertIn("proven", res.detail)

    def test_an_unproven_mapping_waits_for_the_mirrors_and_never_settles_alone(self):
        self.proof["answer"] = (None, "polygon rpc unavailable (test)")
        self.chain["answer"] = ({"s0": 1.0, "s1": 0.0}, "on-chain ConditionResolution")
        res = self.r(self.CID)                                                   # mirrors: still open
        self.assertEqual(res.status, R.PENDING)
        self.assertIn("not proven", res.detail)

    def mirrors(self, up_wins: bool):
        clob = self.clob(closed=True)
        clob["tokens"][0].update(price=1.0 if up_wins else 0.0, winner=up_wins)
        clob["tokens"][1].update(price=0.0 if up_wins else 1.0, winner=not up_wins)
        gamma = {"slug": "btc-updown-5m-1791373800", "markets": [{
            "conditionId": self.CID, "closed": True, "umaResolutionStatus": "resolved",
            "clobTokenIds": '["111", "222"]', "outcomes": '["Up", "Down"]',
            "outcomePrices": '["1", "0"]' if up_wins else '["0", "1"]'}]}

        def get(url, **_kw):
            self.gets.append(url)
            data = gamma if "gamma" in url else clob
            return types.SimpleNamespace(status_code=200, raise_for_status=lambda: None, json=lambda: data)
        return mock.patch.object(R.http_pool, "get", get)

    def test_an_unproven_mapping_settles_when_the_mirrors_agree(self):
        self.proof["answer"] = (None, "no known collateral maps these tokens (test)")
        self.chain["answer"] = ({"s0": 1.0, "s1": 0.0}, "on-chain ConditionResolution")
        self.r.prepare(self.CID)
        with self.mirrors(up_wins=True):
            res = self.r(self.CID)
        self.assertTrue(res.resolved)
        self.assertEqual(res.payouts, {"111": 1.0, "222": 0.0})
        self.assertIn("confirmed by the CLOB + Gamma mirrors", res.detail)

    def test_conflicting_sources_are_result_mismatch_never_settled(self):
        self.proof["answer"] = (None, "no known collateral maps these tokens (test)")
        self.chain["answer"] = ({"s0": 1.0, "s1": 0.0}, "on-chain ConditionResolution")      # chain: "111" won
        self.r.prepare(self.CID)
        with self.mirrors(up_wins=False):                                                   # mirrors: "222" won
            res = self.r(self.CID)
        self.assertFalse(res.resolved)
        self.assertEqual(res.status, R.UNKNOWN)
        self.assertIn("RESULT_MISMATCH", res.detail)

    def test_slot_rows_refuse_anything_but_a_clean_up_down_pair(self):
        good = self.clob()
        self.assertEqual(R.slot_rows(good, self.CID), [("111", "Up"), ("222", "Down")])
        self.assertIsNone(R.slot_rows(dict(good, condition_id="0x" + "ee" * 32), self.CID))
        bad = dict(good, tokens=[{"token_id": "111", "outcome": "Up"}, {"token_id": "111", "outcome": "Down"}])
        self.assertIsNone(R.slot_rows(bad, self.CID))
        bad = dict(good, tokens=[{"token_id": "111", "outcome": "Yes"}, {"token_id": "222", "outcome": "No"}])
        self.assertIsNone(R.slot_rows(bad, self.CID))

    def test_chain_is_on_by_default_and_can_be_turned_off(self):
        with mock.patch.dict(os.environ, {"SETTLE_ONCHAIN": ""}):
            self.assertTrue(onchain_resolution.enabled())
        for off in ("0", "false", "off", "no"):
            with mock.patch.dict(os.environ, {"SETTLE_ONCHAIN": off}):
                self.assertFalse(onchain_resolution.enabled())


class ChainReads(unittest.TestCase):
    """onchain_resolution._read_payouts: one batch, or plain calls without batching."""

    def contract(self, values, *, batch_ok=True):
        calls = []

        class Fn:
            def __init__(self, name, *args):
                self.name, self.args = name, args

            def call(self):
                calls.append(self.name)
                return values[(self.name,) + self.args]

        class Batch:
            def __init__(self):
                self.items = []

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def add(self, fn):
                self.items.append(fn)

            def execute(self):
                if not batch_ok:
                    raise ValueError("batch requests are not supported")
                calls.append("batch")
                return [values[(f.name,) + f.args] for f in self.items]

        functions = types.SimpleNamespace(
            payoutDenominator=lambda key: Fn("payoutDenominator"),
            payoutNumerators=lambda key, i: Fn("payoutNumerators", i))
        return types.SimpleNamespace(functions=functions, w3=types.SimpleNamespace(batch_requests=Batch)), calls

    VALUES = {("payoutDenominator",): 1, ("payoutNumerators", 0): 1, ("payoutNumerators", 1): 0}

    def test_one_batch_round_trip(self):
        c, calls = self.contract(self.VALUES)
        self.assertEqual(onchain_resolution._read_payouts(c, b"k"), (1, [1, 0]))
        self.assertEqual(calls, ["batch"])

    def test_plain_calls_when_the_rpc_cannot_batch(self):
        c, calls = self.contract(self.VALUES, batch_ok=False)
        self.assertEqual(onchain_resolution._read_payouts(c, b"k"), (1, [1, 0]))
        self.assertEqual(calls, ["payoutDenominator", "payoutNumerators", "payoutNumerators"])

    def test_an_unresolved_condition_costs_one_plain_call(self):
        c, calls = self.contract({("payoutDenominator",): 0}, batch_ok=False)
        self.assertEqual(onchain_resolution._read_payouts(c, b"k"), (0, [0, 0]))
        self.assertEqual(calls, ["payoutDenominator"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
