#!/usr/bin/env python3
"""Overlapping collateral reads apply in the order they began (2026-10-26).

LiveExecution._read_balance runs from the 30s loop, the read before each round,
the two reads after each settlement and the guard's recovery checks - they
overlap. Each applied its reply whenever it came back: an older read whose reply
landed after a newer one's put back the cash from before it (a payout, a
deposit), and stamped it as read just now. Kelly sizes from that figure on every
decision (Sniper._read_bankroll: balance - reserved). Now every read is numbered
when it begins; a reply older than the one already applied is dropped, and a
reading is dated from its request, never from its reply. No network, no wallet.

    python tests_balance_race.py
"""
from __future__ import annotations

import asyncio
import pathlib
import random
import sys
import threading
import time
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402


def execution(fetch, cash=100.0):
    sender = types.SimpleNamespace(rules=types.SimpleNamespace(), guard=types.SimpleNamespace(),
                                   kill=L.KillSwitch(None))
    ex = L.LiveExecution(sender=sender, shim=types.SimpleNamespace(invalidate=lambda c: None),
                         ledger=None, receipt_sink=lambda r: True, balance_fetch=fetch, clock_url="unused")
    ex.balance, ex.balance_at = cash, time.monotonic()
    return ex


class Gate:
    """A collateral read that blocks until released, then answers `value`."""

    def __init__(self, value):
        self.value, self.entered, self.release = value, threading.Event(), threading.Event()
        self.entered_mono = self.entered_wall = None

    def __call__(self):
        self.entered_mono, self.entered_wall = time.monotonic(), time.time()
        self.entered.set()
        self.release.wait(5)
        return {"balance": self.value, "allowance": 1e9}


async def until(event: threading.Event):
    while not event.is_set():
        await asyncio.sleep(0.001)


class Routed:
    """Each call goes to the next fetch in the list - one per read, in call order."""

    def __init__(self, *fetches):
        self.fetches, self.lock = list(fetches), threading.Lock()

    def __call__(self):
        with self.lock:
            fetch = self.fetches.pop(0)
        return fetch()


class OlderReplyLandingLast(unittest.IsolatedAsyncioTestCase):
    async def test_it_cannot_put_back_the_cash_from_before_a_payout(self):
        old = Gate(100.0)                                       # asked before the payout landed
        ex = execution(Routed(old, lambda: {"balance": 120.0, "allowance": 1e9}))
        first = asyncio.create_task(ex._read_balance())
        await until(old.entered)
        await ex._read_balance()                                # asked after it: 120, applied
        self.assertEqual(ex.balance, 120.0)
        old.release.set()
        await first                                             # the older reply lands last
        self.assertEqual(ex.balance, 120.0)
        self.assertEqual(ex.last_balance_read["wallet"], 120.0)
        self.assertEqual(ex.n["balance stale reads dropped"], 1)

    async def test_nor_raise_it_back_after_a_debit_the_wallet_already_shows(self):
        old = Gate(100.0)                                       # before a confirmed trade settled
        ex = execution(Routed(old, lambda: {"balance": 91.0, "allowance": 1e9}))
        first = asyncio.create_task(ex._read_balance())
        await until(old.entered)
        await ex._read_balance()
        old.release.set()
        await first
        self.assertEqual(ex.balance, 91.0)

    async def test_replies_in_order_apply_in_order(self):
        ex = execution(Routed(lambda: {"balance": 110.0, "allowance": 1e9},
                              lambda: {"balance": 130.0, "allowance": 1e9}))
        await ex._read_balance()
        self.assertEqual(ex.balance, 110.0)
        await ex._read_balance()
        self.assertEqual(ex.balance, 130.0)
        self.assertEqual(ex.n["balance stale reads dropped"], 0)

    async def test_any_completion_order_ends_on_the_newest_read(self):
        rnd = random.Random(20261026)
        for trial in range(20):
            delays = [rnd.uniform(0, 0.03) for _ in range(6)]

            def fetch_for(i):
                def fetch():
                    time.sleep(delays[i])
                    return {"balance": 100.0 + i, "allowance": 1e9}
                return fetch
            ex = execution(Routed(*(fetch_for(i) for i in range(6))))
            tasks = []
            for _ in range(6):
                tasks.append(asyncio.create_task(ex._read_balance()))
                await asyncio.sleep(0.002)                     # each begins after the one before
            await asyncio.gather(*tasks)
            with self.subTest(trial=trial):
                self.assertEqual(ex.balance, 105.0)            # the read begun last
                self.assertEqual(ex.last_balance_read["wallet"], 105.0)


class ReadingIsDatedFromItsRequest(unittest.IsolatedAsyncioTestCase):
    async def test_a_slow_reply_is_not_fresher_than_its_request(self):
        slow = Gate(100.0)
        ex = execution(slow)
        task = asyncio.create_task(ex._read_balance())
        await until(slow.entered)
        await asyncio.sleep(0.2)                                # the reply takes a while
        slow.release.set()
        await task
        self.assertLessEqual(ex.balance_at, slow.entered_mono)

    async def test_a_read_asked_before_a_rounds_window_does_not_count_for_it(self):
        slow = Gate(100.0)
        ex = execution(slow)
        ex.balance = ex.balance_at = None
        ex.PRE_START_OK_S = 0.1
        task = asyncio.create_task(ex._read_balance())
        await until(slow.entered)
        await asyncio.sleep(0.3)
        slow.release.set()
        await task
        start = slow.entered_wall + 0.15                        # the round began while it was asked
        self.assertFalse(ex._cash_read_for(start))              # asked 0.15s before: older than 0.1s
        self.assertTrue(ex._cash_read_for(slow.entered_wall))

    async def test_a_dropped_reply_changes_nothing(self):
        old = Gate(100.0)
        ex = execution(Routed(old, lambda: {"balance": 120.0, "allowance": 1e9}))
        first = asyncio.create_task(ex._read_balance())
        await until(old.entered)
        await ex._read_balance()
        applied = (ex.balance, ex.balance_at, dict(ex.last_balance_read))
        old.release.set()
        await first
        self.assertEqual((ex.balance, ex.balance_at, ex.last_balance_read), applied)

    async def test_the_in_flight_rule_still_binds_the_newest_read(self):
        old = Gate(100.0)
        new = Gate(100.0)
        ex = execution(Routed(old, new))
        first = asyncio.create_task(ex._read_balance())
        await until(old.entered)
        second = asyncio.create_task(ex._read_balance())
        await until(new.entered)
        ex.balance -= 10                                        # an order's debit while both are out
        ex._balance_revision += 1
        new.release.set()
        await second
        old.release.set()
        await first
        self.assertEqual(ex.balance, 90.0)                      # neither reply refunds the debit
        self.assertEqual(ex.last_balance_read["rule"], "in_flight")


if __name__ == "__main__":
    unittest.main(verbosity=1)
