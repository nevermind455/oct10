#!/usr/bin/env python3
"""The 2026-10-16 live cash freeze. No network, no wallet (a fake CLOB,
throwaway keys).

    python tests_live_20261016_cash_freeze.py

Order 0x0d0ce8b4 (a $14.88 FAK, 16.533334 shares at 0.90) was answered
"matched", but its only trade FAILED on settlement: nothing was bought. It
stayed `awaiting`, and while anything awaits, a balance reading could only
lower the bot's cash - so for an hour the cash read $13.86 (real ~$47), every
payout stayed invisible and Kelly skipped nearly every signal.

  1  with orders unconfirmed, the cash is the reading less what they could
     still take (reserve less booked cost): payouts show, nothing counts twice;
     a read during a send still only lowers it
  2  a FAK whose every trade FAILED, covering its whole reply, nothing booked,
     for FAILED_RELEASE_S, is released as a zero fill; anything less is not
  3  an order past its deadline is warned about every UNRESOLVED_WARN_S
  4  reconcile_order.py reports every-trade-FAILED as a zero fill and --settle
     records it NO_MATCH
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402
import tests_live_20261014_incident as Inc  # noqa: E402

# the VPS order's prices; scaled under the test canary's $15 loss budget ($14.88 + fees is over it)
AMOUNT, SHARES, PRICE, CAP = 9.0, 10.0, 0.90, 0.93


class CashFreeze(unittest.IsolatedAsyncioTestCase):
    # the incident tests' live fixture (a canary on a fake CLOB, $100 of cash), not its tests
    asyncSetUp, asyncTearDown, plan, trade = (Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan,
                                              Inc.Live.trade)

    def matched(self, oid, making=str(AMOUNT), taking=str(SHARES)):
        # as the CLOB V2 answered: matched, its totals, no trade ids
        self.fake.script.append(("json", 200, {"success": True, "status": "matched", "orderID": oid,
                                               "makingAmount": making, "takingAmount": taking,
                                               "transactionsHashes": ["0x" + "ab" * 32], "errorMsg": ""}))

    async def send(self, oid="0xf1"):
        self.matched(oid)
        p = self.ex.prepare(self.plan(AMOUNT, cap=CAP, ask=PRICE))
        await self.ex.send(p)
        self.assertIn(oid, self.ex.awaiting)
        return p

    async def read(self, cash):
        self.ex.balance_fetch = lambda: {"balance": cash, "allowance": 1e9}
        await self.ex._read_balance()
        return self.ex.balance

    # ---------------------------------------------------------------- 1: the reading
    async def test_payouts_show_while_an_order_is_unconfirmed(self):
        p = await self.send()
        frozen = self.ex.balance                                          # $100 less the reservation
        self.assertAlmostEqual(frozen, 100.0 - p.reserve, places=6)
        # a payout came in: the old rule kept min(reading, our figure) - frozen
        self.assertAlmostEqual(await self.read(130.0), 130.0 - p.reserve, places=6)
        self.assertGreater(self.ex.balance, frozen)

    async def test_the_hold_never_counts_a_booked_cost_twice(self):
        T = self.T
        p = await self.send()
        self.trade("t1", "0xf1", SHARES, PRICE, status="CONFIRMED")
        self.ledger.ingest_fill_store(self.store)                          # booked: the reading shows its cost
        _r, _o, booked = self.ledger.risk_view(self.ex.session_start, [("0xf1", T.UP)])
        self.assertGreater(booked["0xf1"], 0)
        self.assertAlmostEqual(await self.read(90.0), 90.0 - (p.reserve - booked["0xf1"]), places=6)

    async def test_a_read_during_a_send_still_only_lowers(self):
        await self.send()
        before = self.ex.balance
        self.ex.reserved = 3.0                                             # another order on the wire
        try:
            self.assertEqual(await self.read(130.0), before)
            self.assertAlmostEqual(await self.read(10.0), 10.0, places=6)   # lowering is always taken
        finally:
            self.ex.reserved = 0.0

    async def test_with_nothing_awaiting_the_reading_is_the_cash(self):
        self.assertEqual(await self.read(77.0), 77.0)

    # ---------------------------------------------------------------- 2: every trade FAILED
    async def test_a_fak_whose_only_trade_failed_is_released_as_a_zero_fill(self):
        await self.send()
        lim = self.ex.sender.limits
        exposure, fills = lim.exposure_usd, lim.fills
        rows = []
        self.ex._journal = rows.append
        self.trade("t1", "0xf1", SHARES, PRICE, status="FAILED")
        a = self.ex.fak_orders["0xf1"]
        self.ex._reconcile_fak("0xf1", a, a["wall"] + 5)
        self.assertIn("0xf1", self.ex.awaiting)                            # not before FAILED_RELEASE_S
        self.assertTrue(any("FAILED" in r for r in self.ex.kill.reasons))  # the pause still alerts at once
        self.ex._reconcile_fak("0xf1", a, a["wall"] + 6 + self.ex.FAILED_RELEASE_S)
        self.assertNotIn("0xf1", self.ex.awaiting)
        self.assertEqual(self.ex._risk_orders, {})                         # its worst case no longer counted
        self.assertAlmostEqual(lim.exposure_usd, exposure - AMOUNT, places=6)
        self.assertEqual(lim.fills, max(0, fills - 1))
        self.assertEqual(rows[-1]["result"], "live_failed_release")
        self.assertEqual(await self.read(100.0), 100.0)                    # its $14.88 is cash again

    async def _never_released(self):
        a = self.ex.fak_orders["0xf1"]
        self.ex._reconcile_fak("0xf1", a, a["wall"] + 5)
        self.ex._reconcile_fak("0xf1", a, a["wall"] + 10 + self.ex.FAILED_RELEASE_S)
        self.assertIn("0xf1", self.ex.awaiting)

    async def test_a_failed_and_a_confirmed_trade_is_not_released(self):
        await self.send()
        self.trade("t1", "0xf1", 6, PRICE, status="FAILED")
        self.trade("t2", "0xf1", SHARES - 6, PRICE, status="CONFIRMED")
        await self._never_released()

    async def test_failed_trades_short_of_the_reply_are_not_released(self):
        await self.send()
        self.trade("t1", "0xf1", 6, PRICE, status="FAILED")              # 6 of the reply's 10 shares
        await self._never_released()

    async def test_a_booked_lot_blocks_the_release(self):
        await self.send()
        self.trade("t1", "0xf1", SHARES, PRICE, status="FAILED")
        self.ex._order_booked = lambda oid, token: True
        await self._never_released()

    async def test_a_failed_status_that_changes_restarts_the_wait(self):
        await self.send()
        self.trade("t1", "0xf1", SHARES, PRICE, status="FAILED")
        a = self.ex.fak_orders["0xf1"]
        self.ex._reconcile_fak("0xf1", a, a["wall"] + 5)
        self.assertIn("failed_since", a)
        self.trade("t2", "0xf1", 1, PRICE, status="MATCHED")              # a new trade turns up
        self.ex._reconcile_fak("0xf1", a, a["wall"] + 10 + self.ex.FAILED_RELEASE_S)
        self.assertIn("0xf1", self.ex.awaiting)
        self.assertNotIn("failed_since", a)

    async def test_an_unconfirmed_order_without_a_known_reservation_keeps_the_strict_rule(self):
        await self.send()
        before = self.ex.balance
        self.ex.awaiting["0xunknown"] = {}                                  # what it may take: unknown
        self.assertEqual(await self.read(130.0), before)

    async def test_a_failed_hold_lookup_keeps_the_strict_rule(self):
        await self.send()
        before = self.ex.balance

        def broken():
            raise RuntimeError("ledger unavailable")
        self.ex._awaiting_hold = broken
        self.assertEqual(await self.read(130.0), before)                    # never "no hold"
        self.assertEqual(self.ex.n["balance hold errors"], 1)

    # ---------------------------------------------------------------- 2b: a previous run's FAILED order
    def intent(self, **kw):
        T = self.T
        return dict({"order_hash": "0xprev", "kind": L.MATCHED, "token": T.UP, "condition": T.COND,
                     "amount": AMOUNT, "trade_ids": [], "reply_totals": [AMOUNT, SHARES]}, **kw)

    async def test_a_previous_runs_failed_order_is_resolved_at_restart(self):
        self.trade("tp", "0xprev", SHARES, PRICE, status="FAILED", source="reconcile")   # the start-up audit
        self.assertEqual(self.ex._previous_run_verdict(self.intent(), None), (L.NO_MATCH, 0.0))
        self.assertEqual(self.ex._previous_run_failed(self.intent(trade_ids=["tp"], reply_totals=None)),
                         (L.NO_MATCH, 0.0))

    async def test_a_previous_runs_order_without_failed_proof_stays(self):
        self.trade("tp", "0xprev", 6, PRICE, status="FAILED", source="reconcile")
        self.assertIsNone(self.ex._previous_run_failed(self.intent()))                 # short of the totals
        self.assertIsNone(self.ex._previous_run_failed(self.intent(reply_totals=None)))  # nothing to cover
        self.trade("tq", "0xprev", SHARES - 6, PRICE, status="MATCHED", source="reconcile")
        self.assertIsNone(self.ex._previous_run_failed(self.intent()))                 # not all FAILED
        self.assertIsNone(self.ex._previous_run_failed(self.intent(kind=L.NO_MATCH)))  # never accepted

    async def test_a_previous_runs_failed_order_the_ledger_booked_stays(self):
        self.trade("tp", "0xprev", SHARES, PRICE, status="FAILED", source="reconcile")
        self.ex._order_booked = lambda oid, token: True
        self.assertIsNone(self.ex._previous_run_failed(self.intent()))

    # ---------------------------------------------------------------- 3: warned while unresolved
    async def test_an_unresolved_order_is_warned_about_every_five_minutes(self):
        await self.send()
        rows, events = [], []
        self.ex._journal = rows.append
        self.ex.on_event = lambda *args, **kw: events.append(args)
        t0 = self.ex.awaiting["0xf1"]["wall"]
        self.ex._warn_unresolved(t0 + 60)                                  # inside the deadline: quiet
        self.assertEqual(rows, [])
        self.ex._warn_unresolved(t0 + self.ex.confirm_within_s + 1)
        self.ex._warn_unresolved(t0 + self.ex.confirm_within_s + 100)
        self.assertEqual(len(rows), 1)
        self.ex._warn_unresolved(t0 + self.ex.confirm_within_s + 2 + self.ex.UNRESOLVED_WARN_S)
        self.assertEqual([r["result"] for r in rows], ["live_unresolved"] * 2)
        self.assertIn("reconcile_order.py", events[-1][1])


# -------------------------------------------------------------------- 4: the tool
class ToolZeroFill(unittest.TestCase):
    incident_state, run_tool = Inc.ReconcileTool.incident_state, Inc.ReconcileTool.run_tool

    def failed_trade(self, oid):
        from tests_sniper_live import COND, UP
        return {"id": "tr-1", "taker_order_id": oid, "asset_id": UP, "market": COND, "side": "BUY",
                "status": "FAILED", "size": str(Inc.INCIDENT_SHARES), "price": str(Inc.INCIDENT_PRICE)}

    def test_every_trade_failed_is_a_zero_fill_and_settles_no_match(self):
        oid = "0x" + "a1fa8479" + "0" * 56
        with tempfile.TemporaryDirectory() as tmp:
            _oid, canary, client, env = self.incident_state(tmp, with_lot=False, reply_tids=(),
                                                            totals=("3.74", str(Inc.INCIDENT_SHARES)),
                                                            trades=[self.failed_trade(oid)])
            code, text = self.run_tool(client, env)
            self.assertEqual(code, 0, text)
            self.assertIn("every trade FAILED", text)
            self.assertIn("as a zero fill (NO_MATCH)", text)
            code, text = self.run_tool(client, env, "--settle")
            self.assertEqual(code, 0, text)
            intent = L.CanarySession.durable_view(canary)["intents"].get("key-1")
            self.assertTrue(intent is None or intent["status"] == L.NO_MATCH, intent)

    def test_a_failed_trade_the_ledger_booked_is_not_settled(self):
        oid = "0x" + "a1fa8479" + "0" * 56
        with tempfile.TemporaryDirectory() as tmp:
            _oid, canary, client, env = self.incident_state(tmp, with_lot=True, trades=[self.failed_trade(oid)])
            code, text = self.run_tool(client, env, "--settle")
            self.assertEqual(code, 1, text)
            self.assertIn("whose every trade FAILED", text)
            self.assertEqual(L.CanarySession.durable_view(canary)["intents"]["key-1"]["status"], "ACCEPTED")


if __name__ == "__main__":
    unittest.main()
