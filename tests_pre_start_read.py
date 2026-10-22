#!/usr/bin/env python3
"""The cash is read just before every market starts (2026-10-23, the operator's
choice "read + orders wait"): a read in the 5s before each round begins,
retried until one succeeds, and no order in a round until there is a good
reading from 10s before its start or later. The 30s loop's read counts when it
falls there. No network, no SDK.

    python tests_pre_start_read.py
"""
from __future__ import annotations

import asyncio
import inspect
import tempfile
import time
import types
import unittest
from pathlib import Path

import testing_env  # noqa: F401 - never load keys or contact a venue
from accounting.ledger import Ledger
import sniper_live as L


class Wallet:
    """The collateral reads in turn (None: a failed read); the last one repeats."""

    def __init__(self, *readings):
        self.readings, self.calls = list(readings), 0

    def __call__(self):
        self.calls += 1
        v = self.readings.pop(0) if len(self.readings) > 1 else self.readings[0]
        return None if v is None else {"balance": v, "allowance": v}


def execution(ledger, wallet):
    sender = types.SimpleNamespace(kill=L.KillSwitch(), rules=types.SimpleNamespace(),
                                   guard=types.SimpleNamespace(check=lambda: None), limits=L.CanaryLimits())
    shim = types.SimpleNamespace(invalidate=lambda *_: None, cached_rules=lambda _cond: None)
    return L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=lambda _: True,
                           balance_fetch=wallet, clock_url="unused", fills=None)


def read_ago(ex, seconds, cash=30.0):
    ex.balance, ex.balance_at = cash, time.monotonic() - seconds


def plan(end):
    return {"token": "up", "cond": "condition", "end": end,
            "send": ("UP", 5.0, "up", "down", "condition", end, 0.5, "up", "0.01", 1.0)}


class PreStartRead(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.wallet = Wallet(40.0)
        self.ex = execution(Ledger(str(Path(self.tmp.name) / "ledger.json")), self.wallet)
        self.rows = []
        self.ex._journal = self.rows.append
        self.ex.PRE_START_RETRY_S = 0.02

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def run_loop(self, hub, until=lambda: False, for_s=0.3):
        task = asyncio.get_running_loop().create_task(self.ex._pre_start_read_loop(hub))
        try:
            deadline = time.monotonic() + for_s
            while time.monotonic() < deadline and not until():
                await asyncio.sleep(0.01)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def reads(self):
        return [r for r in self.rows if r.get("result") == "balance_read"]

    async def test_one_read_in_the_last_seconds_before_a_round_starts(self):
        read_ago(self.ex, 60)                                          # fresh (under 120s), but before the round
        start = time.time() + 3                                        # the next market starts in 3s
        await self.run_loop(types.SimpleNamespace(window_end=start))
        self.assertEqual(self.wallet.calls, 1)
        self.assertEqual(self.ex.balance, 40.0)
        (row,) = self.reads()
        self.assertEqual(row["round_start"], start)
        self.assertTrue(2.0 < row["before_start_s"] <= 3.0, row["before_start_s"])
        self.assertEqual((row["before"], row["wallet"], row["applied"], row["rule"], row["kelly_cash"]),
                         (30.0, 40.0, 40.0, "wallet", 40.0))
        self.assertEqual(self.ex.n["pre-start balance reads"], 1)

    async def test_a_read_already_taken_in_the_window_counts(self):
        read_ago(self.ex, 2)                                           # the 30s loop, 2s ago
        await self.run_loop(types.SimpleNamespace(window_end=time.time() + 4))
        self.assertEqual((self.wallet.calls, self.reads()), (0, []))

    async def test_no_extra_read_in_the_middle_of_a_round(self):
        read_ago(self.ex, 20)                                          # read after this round began
        await self.run_loop(types.SimpleNamespace(window_end=time.time() + 60))
        self.assertEqual(self.wallet.calls, 0)

    async def test_a_failed_read_is_retried_until_one_succeeds(self):
        self.wallet.readings = [None, None, 40.0]
        read_ago(self.ex, 60)
        await self.run_loop(types.SimpleNamespace(window_end=time.time() + 3), until=self.reads, for_s=2.0)
        self.assertEqual(self.wallet.calls, 3)
        self.assertEqual(self.ex.n["pre-start balance read retries"], 2)
        self.assertEqual(len(self.reads()), 1)
        self.assertEqual(self.ex.balance, 40.0)

    async def test_a_bot_started_mid_round_reads_at_once(self):
        self.ex.balance = self.ex.balance_at = None
        await self.run_loop(types.SimpleNamespace(window_end=time.time() + 200), until=self.reads)
        (row,) = self.reads()
        self.assertLess(row["before_start_s"], 0)                      # after the round's start
        self.assertEqual(self.ex.balance, 40.0)

    def test_the_loop_runs_with_the_bot(self):
        self.assertIn("self._pre_start_read_loop(hub)", inspect.getsource(L.LiveExecution.start))


class OrdersWait(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ex = execution(Ledger(str(Path(self.tmp.name) / "ledger.json")), Wallet(40.0))

    def tearDown(self):
        self.tmp.cleanup()

    def refused(self, end):
        with self.assertRaises(L.Refused) as caught:
            self.ex.prepare(plan(end))
        return str(caught.exception)

    def test_an_order_waits_for_a_read_from_just_before_its_round(self):
        end = time.time() + 270                                        # this round began 30s ago
        read_ago(self.ex, 50)                                          # 20s before it began: too early
        self.assertEqual(self.refused(end), "cash not read for this round yet")
        read_ago(self.ex, 35)                                          # 5s before it began
        self.assertEqual(self.refused(end), "market rules not loaded for this round yet")   # past the cash check

    def test_the_window_is_ten_seconds_before_the_start(self):
        end = time.time() + 300                                        # the round starts now
        read_ago(self.ex, 12)
        self.assertEqual(self.refused(end), "cash not read for this round yet")
        read_ago(self.ex, 8)
        self.assertNotEqual(self.refused(end), "cash not read for this round yet")

    def test_a_stale_reading_is_still_refused_first(self):
        read_ago(self.ex, 200)
        self.assertEqual(self.refused(time.time() + 100), "collateral balance is stale")


if __name__ == "__main__":
    unittest.main()
