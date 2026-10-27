#!/usr/bin/env python3
"""The cash reservation goes back to exactly 0 when no order is on the wire
(2026-10-23). Two overlapping orders ($3.638 then $3.40) left 4.4e-16 in
`reserved`; above 0, every read took the in_flight rule - min() with the bot's
own figure - so the cash could only fall: on 2026-10-22 Kelly sized from $0.17
of a $40.60 wallet for about 21 hours (128 cash_stuck alarms). No network, no SDK.

    python tests_reserved_residue.py
"""
from __future__ import annotations

import json
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

import testing_env  # noqa: F401 - never load keys or contact a venue
from accounting.ledger import Ledger
import sniper_live as L

WALLET, FROZEN = 40.60371, 0.173508


def execution(ledger):
    sender = types.SimpleNamespace(kill=L.KillSwitch(), rules=types.SimpleNamespace(),
                                   guard=types.SimpleNamespace(), limits=L.CanaryLimits(),
                                   send=AsyncMock(return_value={"kind": L.NO_MATCH}))
    shim = types.SimpleNamespace(invalidate=lambda *_: None)
    return L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=lambda _: True,
                           balance_fetch=lambda: {"balance": WALLET, "allowance": WALLET},
                           clock_url="unused", fills=None)


def prepared(key, reserve):
    p = L.Prepared("UP", "up", "condition", time.time() + 120, 5, .5, .5, ".01", key,
                   json.dumps({"order": {"salt": key}}), {}, {}, {}, "FAK")
    p.reserve = reserve
    return p


class ReservedResidue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ex = execution(Ledger(str(Path(self.tmp.name) / "ledger.json")))
        self.ex.balance, self.ex.balance_at = FROZEN, time.monotonic()
        self.a, self.b = prepared("a", 3.638), prepared("b", 3.4)
        self.ex.reserved += self.a.reserve                             # both prepared, as prepare() adds them
        self.ex.reserved += self.b.reserve

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_two_overlapping_orders_leave_exactly_zero_and_the_wallet_is_read_again(self):
        await self.ex.send(self.a)
        await self.ex.send(self.b)
        self.assertEqual(self.ex.reserved, 0.0)                        # not 4.4e-16
        await self.ex._read_balance()
        self.assertEqual(self.ex.last_balance_read["rule"], "wallet")
        self.assertAlmostEqual(self.ex.balance, WALLET, places=6)      # not stuck at $0.17

    async def test_an_order_still_on_the_wire_keeps_the_in_flight_rule(self):
        await self.ex.send(self.a)
        self.assertAlmostEqual(self.ex.reserved, 3.4, places=9)
        await self.ex._read_balance()
        self.assertEqual(self.ex.last_balance_read["rule"], "in_flight")
        self.assertAlmostEqual(self.ex.balance, FROZEN, places=6)      # a reading may lower it, never raise it


if __name__ == "__main__":
    unittest.main()
