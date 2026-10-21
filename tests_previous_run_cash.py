#!/usr/bin/env python3
"""A previous run's unresolved orders do not hold cash (2026-10-22). After the
restart on dcecad8 + a562d69, five AMBIGUOUS sends of 10-16..10-20 (reserve
$27.09) held back cash: the wallet read $40.04, the bot used $12.95, and Kelly
sized from that. Whatever such an order took, the wallet already shows; it
still counts in the loss budget. This run's uncertain orders keep holding cash.
No network, no SDK.

    python tests_previous_run_cash.py
"""
from __future__ import annotations

import tempfile
import time
import types
import unittest
from pathlib import Path

import testing_env  # noqa: F401 - never load keys or contact a venue
from accounting.ledger import Ledger
import sniper_live as L


def execution(ledger, wallet):
    sender = types.SimpleNamespace(kill=L.KillSwitch(), rules=types.SimpleNamespace(),
                                   guard=types.SimpleNamespace(), limits=L.CanaryLimits())
    shim = types.SimpleNamespace(invalidate=lambda *_: None)
    ex = L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=lambda _: True,
                         balance_fetch=lambda: {"balance": wallet, "allowance": wallet},
                         clock_url="unused", fills=None)
    ex.balance, ex.balance_at = 12.949791, time.monotonic()      # what the bot used on the VPS
    return ex


OLD = {"condition": "condition", "token": "up", "order_id": "0xde512509", "shares": 10.0}


class PreviousRunCash(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ex = execution(Ledger(str(Path(self.tmp.name) / "ledger.json")), 40.042191)
        for i, reserve in enumerate((5.99, 5.59, 1.09, 10.19, 4.24)):            # the VPS's five, $27.10
            self.ex._risk_orders[f"old-{i}"] = dict(OLD, reserve=reserve, order_id=f"0xold{i}")
        self.ex._previous_run_keys = {f"old-{i}" for i in range(5)}

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_the_wallet_is_the_cash_and_the_loss_budget_still_counts_them(self):
        await self.ex._read_balance()
        self.assertAlmostEqual(self.ex.balance, 40.042191, places=6)           # not 12.95
        self.assertEqual(self.ex.last_balance_read["rule"], "wallet")
        self.assertGreaterEqual(self.ex._risk_budget_used(), 27.09)            # worst case unchanged

    async def test_this_runs_uncertain_order_still_holds_beside_them(self):
        self.ex._risk_orders["this-run"] = dict(OLD, reserve=5.0, order_id=None)   # no venue id: uncertain
        await self.ex._read_balance()
        self.assertAlmostEqual(self.ex.balance, 40.042191 - 5.0, places=6)
        self.assertEqual(self.ex.last_balance_read["rule"], "held")

    async def test_without_a_previous_run_every_reservation_holds_as_before(self):
        self.ex._previous_run_keys = set()
        await self.ex._read_balance()
        self.assertAlmostEqual(self.ex.balance, 40.042191 - 27.10, places=6)


if __name__ == "__main__":
    unittest.main()
