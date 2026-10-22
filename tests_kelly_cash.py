#!/usr/bin/env python3
"""C1 regression coverage (2026-10-23): the cash Kelly sizes from.

d1ab2ae set `reserved` back to exactly 0 when no order is on the wire: float residue
from two overlapping orders kept every wallet read on the in_flight rule and Kelly
sized from $0.17 of a $40.60 wallet for about 21 hours on 10-22. These pin the rest:
any mix of overlapping orders, the wallet against Kelly's cash, repeated partial fills,
a restart, and drawdown sizing from that cash. No network, no SDK.

    python tests_kelly_cash.py
"""
from __future__ import annotations

import json
import random
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

import testing_env  # noqa: F401 - never load keys or contact a venue
from accounting.ledger import Ledger
from hybrid_sizing import HybridPolicy, sizing_factors
import sniper_live as L
import sniper_mode as S

WALLET = 40.60371
COND, UP = "condition", "up-token"


def execution(ledger, wallet=WALLET):
    sender = types.SimpleNamespace(kill=L.KillSwitch(), rules=types.SimpleNamespace(),
                                   guard=types.SimpleNamespace(), limits=L.CanaryLimits(),
                                   send=AsyncMock(return_value={"kind": L.NO_MATCH}))
    shim = types.SimpleNamespace(invalidate=lambda *_: None)
    ex = L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=lambda _: True,
                         balance_fetch=lambda: {"balance": wallet, "allowance": wallet},
                         clock_url="unused", fills=None)
    ex.balance, ex.balance_at = wallet, time.monotonic()
    return ex


def prepared(key, reserve):
    p = L.Prepared("UP", UP, COND, time.time() + 120, 5, .5, .5, ".01", key,
                   json.dumps({"order": {"salt": key}}), {}, {}, {}, "FAK")
    p.reserve = reserve
    return p


def bankroll(ex) -> float:
    """What Kelly sizes from: Sniper._read_bankroll on this live execution."""
    return S.Sniper._read_bankroll(types.SimpleNamespace(live=ex, broker=None))


class KellyCash(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(str(Path(self.tmp.name) / "ledger.json"))
        self.ex = execution(self.ledger)

    async def asyncTearDown(self):
        self.tmp.cleanup()

    async def test_any_mix_of_overlapping_orders_returns_to_exactly_zero(self):
        rng = random.Random(20261023)
        for trial in range(300):
            orders = [prepared(f"o{trial}-{i}", round(rng.uniform(1.0, 12.0), 4)) for i in range(rng.randint(2, 5))]
            for p in orders:                                             # all on the wire at once
                self.ex.reserved += p.reserve
            rng.shuffle(orders)                                          # replies in any order
            for p in orders:
                await self.ex.send(p)
            self.assertEqual(self.ex.reserved, 0.0, f"trial {trial}")
        await self.ex._read_balance()
        self.assertEqual((self.ex.last_balance_read["rule"], self.ex.balance), ("wallet", WALLET))

    async def test_kelly_cash_is_the_wallet_less_what_is_on_the_wire(self):
        a, b = prepared("a", 3.638), prepared("b", 3.4)
        self.ex.reserved += a.reserve
        self.assertAlmostEqual(bankroll(self.ex), WALLET - 3.638, places=9)
        self.ex.reserved += b.reserve
        self.assertAlmostEqual(bankroll(self.ex), WALLET - 3.638 - 3.4, places=9)
        await self.ex.send(a)
        await self.ex.send(b)
        self.assertEqual(bankroll(self.ex), WALLET)
        # the residue itself is invisible in a $40 figure; what froze the cash was the
        # next reads taking the in_flight rule (min with our own, falling figure)
        self.ex.balance = 0.173508                                        # the 10-22 frozen figure
        await self.ex._read_balance()
        self.assertEqual((self.ex.last_balance_read["rule"], self.ex.balance), ("wallet", WALLET))

    async def test_repeated_partial_fills_hold_only_what_is_not_booked_yet(self):
        oid, reserve = "0xpartial", 9.63
        self.ledger.authorize_order(oid, {"condition_id": COND, "token_id": UP,
                                          "window_end": (int(time.time()) // 300 + 1) * 300,
                                          "requested_notional": 9.0, "fee_rate": .07, "fee_exponent": 1})
        self.ex._risk_orders["k-partial"] = {"condition": COND, "token": UP, "reserve": reserve, "order_id": oid,
                                             "shares": 10.0}
        self.ex.awaiting[oid] = {"wall": time.time(), "condition": COND, "token": UP, "amount": 9.0,
                                 "kind": L.MATCHED, "client_order_id": "k-partial", "authorized": True}
        booked = 0.0
        for i, (shares, price) in enumerate(((4.0, 0.90), (3.0, 0.91), (3.0, 0.92))):
            self.assertTrue(self.ledger.record_fill(f"t{i}", UP, shares=shares, price=price, condition_id=COND,
                                                    status="CONFIRMED", order_id=oid, fee=0.0))
            booked += shares * price
            await self.ex._read_balance()
            self.assertEqual(self.ex.last_balance_read["rule"], "held")
            self.assertAlmostEqual(self.ex.last_balance_read["hold"], round(reserve - booked, 6), places=6)
            self.assertAlmostEqual(self.ex.balance, WALLET - (reserve - booked), places=6)   # never twice
        self.ex._risk_orders.pop("k-partial")                            # complete: reconciled
        self.ex.awaiting.pop(oid)
        await self.ex._read_balance()
        self.assertEqual((self.ex.last_balance_read["rule"], self.ex.balance), ("wallet", WALLET))

    async def test_a_restart_starts_from_the_wallet(self):
        fresh = execution(self.ledger)
        fresh.balance = fresh.balance_at = None
        self.assertEqual(fresh.reserved, 0.0)
        fresh._risk_orders["old"] = {"condition": COND, "token": UP, "reserve": 5.99, "order_id": "0xold",
                                     "shares": 10.0}
        fresh._previous_run_keys = {"old"}                               # a previous run's unknown order
        await fresh._read_balance()
        self.assertEqual((fresh.last_balance_read["rule"], fresh.balance), ("wallet", WALLET))
        self.assertEqual(bankroll(fresh), WALLET)
        self.assertGreaterEqual(fresh._risk_budget_used(), 5.99)          # still in the loss budget

    async def test_drawdown_scales_kelly_from_the_live_cash(self):
        policy = HybridPolicy(drawdown_stop=0.20)
        cash = bankroll(self.ex)
        full = sizing_factors(policy, sigma=1e-4, equity=100.0, high_water=100.0)["effective_fraction"]
        half = sizing_factors(policy, sigma=1e-4, equity=90.0, high_water=100.0)["effective_fraction"]
        stop = sizing_factors(policy, sigma=1e-4, equity=80.0, high_water=100.0)["effective_fraction"]
        self.assertAlmostEqual(half, full / 2, places=12)                # 10% down: half the fraction
        self.assertEqual(stop, 0.0)
        big, _ = S.kelly_shares(0.75, 0.60, 0.07, full, cash * 10, 10_000)
        small, _ = S.kelly_shares(0.75, 0.60, 0.07, half, cash * 10, 10_000)
        none, _ = S.kelly_shares(0.75, 0.60, 0.07, stop, cash * 10, 10_000)
        self.assertGreater(big, small)
        self.assertLessEqual(abs(small - big / 2), 1)                    # whole shares
        self.assertEqual(none, 0.0)
        frozen, _ = S.kelly_shares(0.75, 0.60, 0.07, full, 0.17, 10_000)  # the 10-22 frozen cash
        self.assertEqual(frozen, 0.0)


if __name__ == "__main__":
    unittest.main()
