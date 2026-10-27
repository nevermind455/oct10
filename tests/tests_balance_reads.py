#!/usr/bin/env python3
"""Live cash after every market, and an alarm when it stays stuck (2026-10-17).

    python tests_balance_reads.py

On 10-15/16 the cash Kelly sized from drifted down and only came back on a
restart (17.75 -> 43.68, 16.76 -> 65.15, 13.86 -> 47.35). cf63872 removed the
main cause; this adds:
  1  a balance read 90s and 180s after every round ends (payouts land about a
     minute after settlement), each journalled as a balance_read row;
  2  a cash_stuck alarm when the cash stays more than $1 under the wallet's
     own figure for 10 minutes, repeated every 10 minutes until it clears.
No network, no wallet (the incident tests' fake CLOB).
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import time
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402
import tests_live_20261014_incident as Inc  # noqa: E402
import tests_live_20261016_cash_freeze as CF  # noqa: E402


class BalanceReads(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan, trade = (Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan,
                                              Inc.Live.trade)
    matched, send, read = CF.CashFreeze.matched, CF.CashFreeze.send, CF.CashFreeze.read

    def journal(self):
        rows, events = [], []
        self.ex._journal = rows.append
        self.ex.on_event = lambda src, msg, level="info": events.append((msg, level))
        return rows, events

    # ---------------------------------------------------------------- 1: after every market
    async def test_two_reads_after_each_round_end_each_journalled(self):
        rows, _ = self.journal()
        self.ex.SETTLE_READ_AFTER_S = (0.05, 0.15)
        ended = time.time() + 0.1                                      # this round ends in 0.1s
        hub = types.SimpleNamespace(window_end=ended)
        task = asyncio.get_running_loop().create_task(self.ex._settle_read_loop(hub))
        try:
            await asyncio.sleep(0.1)
            self.ex.balance_fetch = lambda: {"balance": 120.0, "allowance": 1e9}    # the payout came in
            hub.window_end = ended + 300                                               # the next round began
            deadline = time.monotonic() + 3.5
            while len([r for r in rows if r.get("result") == "balance_read"]) < 2 and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        reads = [r for r in rows if r.get("result") == "balance_read"]
        self.assertEqual([r["after_end_s"] for r in reads], [0.05, 0.15])
        first = reads[0]
        self.assertEqual((first["wallet"], first["applied"], first["rule"], first["awaiting"]), (120.0, 120.0, "wallet", 0))
        self.assertEqual(first["kelly_cash"], 120.0)
        self.assertEqual(first["before"], 100.0)                       # the fixture's cash before the payout
        self.assertEqual(self.ex.balance, 120.0)
        self.assertEqual(self.ex.n["settlement balance reads"], 2)

    async def test_no_read_before_the_first_round_ends(self):
        rows, _ = self.journal()
        self.ex.SETTLE_READ_AFTER_S = (0.01,)
        hub = types.SimpleNamespace(window_end=time.time() + 300)
        task = asyncio.get_running_loop().create_task(self.ex._settle_read_loop(hub))
        await asyncio.sleep(1.2)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertFalse([r for r in rows if r.get("result") == "balance_read"])

    # ---------------------------------------------------------------- 2: the alarm
    async def test_cash_held_under_the_wallet_alarms_after_ten_minutes_then_every_ten(self):
        rows, events = self.journal()
        # anchored to the real clock: the fixture's balance reading carries a real
        # monotonic stamp, which on Windows counts from boot (a fixed fake time
        # made the reading look stale right after a reboot)
        base = time.monotonic()
        clock = [base]
        with mock.patch.object(L.time, "monotonic", lambda: clock[0]):
            p = await self.send()                                     # unconfirmed: the cash is held
            self.assertAlmostEqual(await self.read(130.0), 130.0 - p.reserve, places=6)
            self.assertEqual(self.ex.last_balance_read["rule"], "held")
            for t in (300.0, 599.0):                                  # under ten minutes: quiet
                clock[0] = base + t
                await self.read(130.0)
            self.assertFalse(events)
            clock[0] = base + 601.0
            await self.read(130.0)
            self.assertEqual(len(events), 1)
            msg, level = events[0]
            self.assertEqual(level, "bad")
            self.assertIn(f"${p.reserve:.2f} below the wallet ($130.00)", msg)
            self.assertIn("1 unconfirmed", msg)
            stuck = [r for r in rows if r.get("result") == "cash_stuck"]
            self.assertEqual(len(stuck), 1)
            self.assertEqual((stuck[0]["wallet"], stuck[0]["rule"], stuck[0]["awaiting"]), (130.0, "held", ["0xf1"]))
            clock[0] = base + 900.0
            await self.read(130.0)
            self.assertEqual(len(events), 1)                          # not again within ten minutes
            clock[0] = base + 1205.0
            await self.read(130.0)
            self.assertEqual(len(events), 2)                          # and again after ten more
            self.assertEqual(self.ex.n["stuck cash alarms"], 2)

    async def test_the_alarm_clears_when_the_cash_meets_the_wallet(self):
        _rows, events = self.journal()
        clock = [1000.0]
        with mock.patch.object(L.time, "monotonic", lambda: clock[0]):
            self.ex._check_stuck(130.0, 100.0, "strict")
            clock[0] = 1700.0
            self.ex._check_stuck(130.0, 129.5, "wallet")              # within $1: cleared
            self.assertIsNone(self.ex._stuck_since)
            clock[0] = 1800.0
            self.ex._check_stuck(130.0, 100.0, "strict")              # held again: a new ten minutes starts
            self.assertFalse(events)
            clock[0] = 2401.0
            self.ex._check_stuck(130.0, 100.0, "strict")
            self.assertEqual(len(events), 1)
            self.assertIn("(strict; 0 unconfirmed: none)", events[0][0])

    async def test_an_unheld_reading_records_the_wallet_rule(self):
        await self.read(77.0)
        self.assertEqual(self.ex.last_balance_read, {"wallet": 77.0, "applied": 77.0, "rule": "wallet",
                                                     "hold": None, "awaiting": 0})


if __name__ == "__main__":
    unittest.main(verbosity=1)
