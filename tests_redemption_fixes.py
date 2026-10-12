#!/usr/bin/env python3
"""Offline regression cases for wallet redemption observations and restart."""
from __future__ import annotations

import asyncio
import pathlib
import tempfile
import unittest

import testing_env  # noqa: F401 - prevent .env loading and forbid venue traffic
from accounting.ledger import Ledger
from accounting.resolution import RESOLVED, Resolution
from accounting import settlement as S


CID = "0x" + "ab" * 32
UP, DOWN = "1001", "1002"
END = 1_791_374_100


class RedemptionFixes(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(pathlib.Path(self.tmp.name) / "ledger.json")
        self.ledger = Ledger(self.path)
        self.result = Resolution(CID, RESOLVED, {UP: 1.0, DOWN: 0.0},
                                 winning_label="Up", detail="offline final result")

    def tearDown(self):
        self.tmp.cleanup()

    def fill(self, token=UP, shares=10, cid=CID, trade_id="fill"):
        self.assertTrue(self.ledger.record_fill_durable(
            trade_id, token, shares=shares, price=0.4, condition_id=cid, fee=0))

    def worker(self, check, **kwargs):
        return S.SettlementWorker(self.ledger, fetch=lambda _cid: self.result,
                                  paper=False, redeem_check=check, **kwargs)

    async def settled(self, worker):
        rec = worker.track(CID, END - 300, END, up_token=UP, down_token=DOWN)
        await worker._apply_result(rec, self.result)
        return rec

    def test_partial_token_removal_retains_nine_shares_and_never_claims_cash(self):
        self.fill()

        async def go():
            worker = self.worker(lambda _token: (9.0, "balanceOf"))
            try:
                rec = await self.settled(worker)
                await worker._check_redeem(rec)
                self.assertEqual(rec.redeem, S.REDEEM_PENDING)
                self.assertEqual(rec.redeem_shares, {UP: 10})
                self.assertEqual(rec.redeem_remaining_shares, {UP: 9})
                self.assertEqual(rec.redeem_remaining_amount, 9)
                self.assertEqual(rec.redeem_unverified_amount, 1)
                self.assertEqual(rec.redeem_amount, 10)
                self.assertEqual(worker.panel()["redeem_pending"], 9)
                self.assertIsNone(rec.redeemed_at)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_zero_tokens_is_unverified_and_not_added_to_cash_equity(self):
        self.fill()

        async def go():
            worker = self.worker(lambda _token: (0.0, "balanceOf"))
            try:
                rec = await self.settled(worker)
                await worker._check_redeem(rec)
                self.assertEqual(rec.redeem, S.REDEEM_UNVERIFIED)
                self.assertEqual(worker.panel()["redeem_pending"], 0)
                self.assertEqual(worker.panel()["redeem_unverified"], 10)
                self.assertIsNone(rec.redeemed_at)
                self.assertFalse(any(row["event"] == "REDEEMED" for row in worker._journal))
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_unknown_invalid_or_failed_reads_leave_equity_unknown(self):
        self.fill()

        async def go():
            for value in (None, float("nan"), float("inf"), -1, "invalid", RuntimeError("RPC failed")):
                with self.subTest(value=value):
                    def check(_token):
                        if isinstance(value, Exception):
                            raise value
                        return value, "offline RPC unavailable"

                    worker = self.worker(check)
                    try:
                        rec = await self.settled(worker)
                        await worker._check_redeem(rec)
                        self.assertEqual(rec.redeem, S.REDEEM_PENDING)
                        self.assertIsNone(rec.redeem_remaining_amount)
                        self.assertIsNone(worker.panel()["redeem_pending"])
                        self.assertTrue(worker.panel()["redeem_unknown"])
                        self.assertIn("wallet balance unknown", rec.redeem_error)
                    finally:
                        await worker.stop()

        asyncio.run(go())

    def test_a_failed_read_invalidates_the_previous_equity_observation(self):
        self.fill()
        balances = iter([9.0, None, 8.0])

        async def go():
            worker = self.worker(lambda _token: (next(balances), "balanceOf"))
            try:
                rec = await self.settled(worker)
                await worker._check_redeem(rec)
                self.assertEqual(worker.panel()["redeem_pending"], 9)
                await worker._check_redeem(rec)
                self.assertIsNone(worker.panel()["redeem_pending"])
                await worker._check_redeem(rec)
                self.assertEqual(worker.panel()["redeem_pending"], 8)
                self.assertIsNone(rec.redeem_error)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_restart_restores_durable_winners_without_settling_them_twice(self):
        self.fill()
        self.fill(token=DOWN, shares=5, trade_id="loser")
        self.ledger.settle(self.result)
        settled_wall = self.ledger.positions[UP].settled_wall
        # Reload the actual snapshot/journal as a new process would.
        self.ledger = Ledger(self.path)
        sequence_before = self.ledger.journal_seq
        realized_before = self.ledger.summary()["realized_pnl"]

        async def go():
            worker = self.worker(lambda _token: (9.0, "balanceOf"), max_age_s=1)
            try:
                await worker._refresh_from_ledger()
                rec = worker.records[CID]
                self.assertEqual(rec.state, S.SETTLED)
                self.assertEqual(rec.payouts, {UP: 1.0, DOWN: 0.0})
                self.assertEqual(rec.settled_at, settled_wall)
                self.assertEqual(rec.realized, realized_before)
                self.assertIsNone(worker.panel()["redeem_pending"])
                await worker._check_redeem(rec)
                self.assertEqual(worker.panel()["redeem_pending"], 9)
                self.assertEqual(await worker._check(rec), 0)
                self.assertEqual(self.ledger.journal_seq, sequence_before)
                self.assertEqual(self.ledger.summary()["realized_pnl"], realized_before)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_already_redeemed_historical_positions_do_not_inflate_restart_equity(self):
        self.fill()
        self.ledger.settle(self.result)
        self.ledger = Ledger(self.path)

        async def go():
            worker = self.worker(lambda _token: (0.0, "old winner balance"))
            try:
                await worker._refresh_from_ledger()
                rec = worker.records[CID]
                await worker._check_redeem(rec)
                self.assertEqual(worker.panel()["redeem_pending"], 0)
                self.assertEqual(rec.redeem, S.REDEEM_UNVERIFIED)
                self.assertEqual(rec.redeem_amount, 10)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_extra_historical_wallet_tokens_are_capped_at_bot_holdings(self):
        self.fill()
        balances = iter([19.0, 9.0])

        async def go():
            worker = self.worker(lambda _token: (next(balances), "balanceOf"))
            try:
                rec = await self.settled(worker)
                await worker._check_redeem(rec)
                self.assertEqual(rec.redeem_remaining_shares, {UP: 10})
                self.assertEqual(worker.panel()["redeem_pending"], 10)
                await worker._check_redeem(rec)
                self.assertEqual(rec.redeem, S.REDEEM_PENDING)
                self.assertEqual(worker.panel()["redeem_pending"], 9)
                self.assertIsNone(rec.redeemed_at)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_repeated_settlement_does_not_reset_the_partial_balance(self):
        self.fill()

        async def go():
            worker = self.worker(lambda _token: (9.0, "balanceOf"))
            try:
                rec = await self.settled(worker)
                await worker._check_redeem(rec)
                self.assertEqual(await worker._apply_result(rec, self.result), 0)
                worker._begin_redemption(rec)
                await worker._refresh_from_ledger()
                self.assertEqual(worker.panel()["redeem_pending"], 9)
                self.assertEqual(worker.settled, 1)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_two_half_payout_tokens_use_the_remaining_quantities(self):
        self.fill()
        self.fill(token=DOWN, shares=4, trade_id="second")
        self.result = Resolution(CID, RESOLVED, {UP: 0.5, DOWN: 0.5}, winning_label="50/50")

        async def go():
            worker = self.worker(lambda token: ({UP: 9.0, DOWN: 1.0}[token], "balanceOf"))
            try:
                rec = await self.settled(worker)
                await worker._check_redeem(rec)
                self.assertEqual(rec.redeem_amount, 7)
                self.assertEqual(worker.panel()["redeem_pending"], 5)
                self.assertEqual(rec.redeem_unverified_amount, 2)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_inflight_old_balance_cannot_overwrite_a_late_fill(self):
        self.fill()

        async def go():
            worker = self.worker(lambda _token: (0.0, "balanceOf"))
            try:
                rec = await self.settled(worker)
                entered, release = asyncio.Event(), asyncio.Event()
                pool = worker._in_pool

                async def blocked(fn, *args, net=False):
                    if net:
                        entered.set()
                        await release.wait()
                        return 0.0, "snapshot before late fill"
                    return await pool(fn, *args, net=net)

                worker._in_pool = blocked
                check = asyncio.create_task(worker._check_redeem(rec))
                await entered.wait()
                self.fill(shares=2, trade_id="late")
                await worker._refresh_from_ledger()
                await worker._check(rec)
                release.set()
                await check
                self.assertEqual(rec.redeem_shares, {UP: 12})
                self.assertEqual(rec.redeem, S.REDEEM_PENDING)
                self.assertIsNone(rec.redeem_remaining_amount)
                self.assertIsNone(rec.redeemed_at)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_late_fill_of_an_unseen_token_refreshes_the_authoritative_result(self):
        self.fill()
        self.ledger.settle(self.result)
        self.ledger = Ledger(self.path)

        async def go():
            worker = self.worker(lambda _token: (10.0, "balanceOf"))
            try:
                await worker._refresh_from_ledger()
                rec = worker.records[CID]
                self.assertEqual(rec.payouts, {UP: 1})
                self.fill(token=DOWN, shares=2, trade_id="late-other-side")
                await worker._refresh_from_ledger()
                self.assertEqual(rec.state, S.PENDING)
                self.assertIsNone(rec.resolution)
                self.assertEqual(await worker._check(rec), 1)
                self.assertTrue(self.ledger.positions[DOWN].settled)
                self.assertEqual(rec.state, S.SETTLED)
                self.assertEqual(rec.payouts, {UP: 1, DOWN: 0})
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_late_same_token_fill_after_restart_refetches_a_complete_payout_vector(self):
        self.fill()
        self.ledger.settle(self.result)
        self.ledger = Ledger(self.path)

        async def go():
            worker = self.worker(lambda _token: (12.0, "balanceOf"))
            try:
                await worker._refresh_from_ledger()
                rec = worker.records[CID]
                self.assertEqual(rec.payouts, {UP: 1})
                self.fill(shares=2, trade_id="late-same-token")
                await worker._refresh_from_ledger()
                self.assertIsNone(rec.resolution)
                self.assertEqual(await worker._check(rec), 1)
                self.assertEqual(rec.payouts, {UP: 1, DOWN: 0})
                self.assertEqual(rec.redeem_shares, {UP: 12})
                self.assertEqual(rec.state, S.SETTLED)
            finally:
                await worker.stop()

        asyncio.run(go())

    def test_zero_balance_history_is_bounded_and_not_immediately_readopted(self):
        second_cid, second_token = "0x" + "cd" * 32, "2001"
        self.fill()
        self.fill(token=second_token, shares=2, cid=second_cid, trade_id="other")
        self.ledger.settle(self.result)
        self.ledger.settle(Resolution(second_cid, RESOLVED, {second_token: 1.0, "2002": 0.0}))

        async def go():
            worker = self.worker(lambda _token: (0.0, "balanceOf"), history=1)
            try:
                await worker._refresh_from_ledger()
                for rec in worker.records.values():
                    await worker._check_redeem(rec)
                worker._trim(0)
                self.assertEqual(len(worker.records), 1)
                self.assertEqual(len(worker._retired_redemptions), 1)
                await worker._refresh_from_ledger()
                self.assertEqual(len(worker.records), 1)
                # A later fill changes its ledger version and must be restored.
                retired_cid = next(iter(worker._retired_redemptions))
                token = UP if retired_cid == CID else second_token
                self.fill(token=token, shares=1, cid=retired_cid, trade_id="new-after-trim")
                await worker._refresh_from_ledger()
                self.assertIn(retired_cid, worker.records)
                self.assertTrue(worker.records[retired_cid].unsettled())
            finally:
                await worker.stop()

        asyncio.run(go())


if __name__ == "__main__":
    unittest.main(verbosity=2)
