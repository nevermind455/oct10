"""Hybrid sizing's durable realized-equity accounting, entirely offline."""
from __future__ import annotations

import concurrent.futures
import json
import pathlib
import tempfile
import types
import unittest
from unittest import mock

import testing_env  # noqa: F401 - never load credentials or external services
from accounting.ledger import Ledger


class HybridLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = pathlib.Path(self.temp.name) / "ledger.json"
        self.ledger = Ledger(str(self.path), allow_sells=True)

    def tearDown(self):
        self.temp.cleanup()

    def buy(self, token, *, shares=10, price=0.5, fee=0.0):
        self.assertTrue(self.ledger.record_fill_durable(
            "buy-" + token, token, shares=shares, price=price, side="BUY",
            condition_id="condition-" + token, status="CONFIRMED", fee=fee))

    def settle(self, token, payout):
        self.ledger.settle(types.SimpleNamespace(
            resolved=True, condition_id="condition-" + token,
            payouts={token: payout, "opposite-" + token: 1 - payout}))

    def restart(self):
        self.ledger = Ledger(str(self.path), allow_sells=True)
        return self.ledger

    def test_failed_first_snapshot_does_not_advance_the_accounting_sequence(self):
        before = self.ledger.journal_seq
        with mock.patch.object(self.ledger, '_write_payload', return_value=False):
            with self.assertRaisesRegex(RuntimeError, 'could not be persisted'):
                self.ledger.record_fill_durable(
                    'failed-initial-snapshot', 'token', shares=10, price=.5,
                    condition_id='condition-token', status='CONFIRMED', fee=0)
        self.assertEqual(self.ledger.journal_seq, before)
        self.assertEqual(self.ledger.open_leg_shares('condition-token', 'token'), 0)

    def test_initialization_needs_explicit_positive_finite_numeric_capital(self):
        for reference in (None, 0, -1, True, "100", float("inf"), float("nan")):
            with self.subTest(reference=reference), self.assertRaises(ValueError):
                self.ledger.hybrid_risk_view(reference)
        self.assertIsNone(self.ledger.hybrid_risk)
        self.assertFalse(self.path.exists())

    def test_new_epoch_is_durable_and_returned_metadata_cannot_mutate_it(self):
        view = self.ledger.hybrid_risk_view(100)
        self.assertEqual(view["equity"], 100)
        self.assertEqual(view["high_water"], 100)
        self.assertEqual(view["pnl_baseline"], 0)
        view["high_water"] = 999
        detached = self.ledger.hybrid_risk
        detached["reference_equity"] = 999
        self.assertEqual(self.ledger.hybrid_risk_view()["reference_equity"], 100)
        self.assertEqual(self.restart().hybrid_risk_view()["high_water"], 100)

    def test_reference_cannot_be_rebased_after_initialization(self):
        self.ledger.hybrid_risk_view(100)
        self.assertEqual(self.ledger.hybrid_risk_view(100)["equity"], 100)
        with self.assertRaisesRegex(ValueError, "refusing to rebase"):
            self.ledger.hybrid_risk_view(101)

    def test_purchase_and_cash_marks_do_not_create_drawdown(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a", shares=50, price=0.5, fee=0.5)
        self.ledger.mark_balance(74.5)
        view = self.ledger.hybrid_risk_view()
        self.assertEqual(view["equity"], 100)
        self.assertEqual(view["high_water"], 100)
        self.assertEqual(view["realized_total"], 0)
        self.ledger.mark_balance(500)  # external cash does not create profits
        self.assertEqual(self.ledger.hybrid_risk_view()["equity"], 100)

    def test_resolved_losses_and_wins_preserve_peak_through_restart(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("win", fee=0.1)
        self.settle("win", 1)
        view = self.ledger.hybrid_risk_view()
        self.assertAlmostEqual(view["equity"], 104.9)
        self.assertAlmostEqual(view["high_water"], 104.9)
        self.buy("loss", fee=0.2)
        self.settle("loss", 0)
        self.assertAlmostEqual(self.ledger.hybrid_risk_view()["equity"], 99.7)
        view = self.restart().hybrid_risk_view()
        self.assertAlmostEqual(view["equity"], 99.7)
        self.assertAlmostEqual(view["high_water"], 104.9)
        self.assertEqual(view["reference_equity"], 100)

    def test_historical_profit_at_initialization_is_baselined_once(self):
        self.buy("old")
        self.settle("old", 1)
        view = self.ledger.hybrid_risk_view(100)
        self.assertEqual(view["realized_total"], 5)
        self.assertEqual(view["pnl_baseline"], 5)
        self.assertEqual(view["equity"], 100)
        self.buy("new")
        self.settle("new", 0)
        self.assertEqual(self.ledger.hybrid_risk_view()["equity"], 95)

    def test_partial_sales_count_once_before_and_after_resolution(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a", price=0.4, fee=0.1)
        self.assertTrue(self.ledger.record_fill_durable(
            "sell-a", "a", shares=4, price=0.6, side="SELL",
            condition_id="condition-a", status="CONFIRMED", fee=0.02))
        view = self.ledger.hybrid_risk_view()
        self.assertAlmostEqual(view["equity"], 100.74)
        self.assertAlmostEqual(view["high_water"], 100.74)
        self.restart()
        self.settle("a", 0)
        view = self.ledger.hybrid_risk_view()
        self.assertAlmostEqual(view["realized_total"], -1.72)
        self.assertAlmostEqual(view["equity"], 98.28)
        self.assertAlmostEqual(view["high_water"], 100.74)

    def test_unconfirmed_fill_never_changes_equity(self):
        self.ledger.hybrid_risk_view(100)
        self.assertFalse(self.ledger.record_fill_durable(
            "pending", "a", shares=10, price=0.5, side="BUY",
            condition_id="condition-a", status="MATCHED", fee=0))
        self.assertEqual(self.ledger.hybrid_risk_view()["equity"], 100)

    def test_nonpositive_equity_keeps_positive_high_water(self):
        self.ledger.hybrid_risk_view(1)
        self.buy("loss")
        self.settle("loss", 0)
        view = self.ledger.hybrid_risk_view()
        self.assertEqual(view["equity"], -4)
        self.assertEqual(view["high_water"], 1)
        self.assertEqual(self.restart().hybrid_risk_view()["equity"], -4)

    def test_unchanged_reads_do_not_write_snapshot_or_journal(self):
        self.ledger.hybrid_risk_view(100)
        with mock.patch.object(self.ledger, "_write_payload") as write, \
                mock.patch.object(self.ledger, "_append_journal") as append:
            self.ledger.hybrid_risk_view()
            self.ledger.hybrid_risk_view(100)
        write.assert_not_called()
        append.assert_not_called()

    def test_profit_in_replayed_journal_keeps_epoch_and_updates_peak(self):
        self.ledger.hybrid_risk_view(100)
        identity = self.ledger.ledger_id
        self.buy("a")
        self.settle("a", 1)
        self.assertGreater(self.ledger.journal_pending(), 0)
        self.restart()
        self.assertEqual(self.ledger.ledger_id, identity)
        self.assertGreater(self.ledger.journal_replayed, 0)
        view = self.ledger.hybrid_risk_view()
        self.assertEqual(view["equity"], 105)
        self.assertEqual(view["high_water"], 105)
        self.assertEqual(self.restart().hybrid_risk_view()["high_water"], 105)

    def test_failed_settlement_does_not_create_drawdown(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a")
        with mock.patch.object(self.ledger, "_append_journal", return_value=False):
            with self.assertRaises(RuntimeError):
                self.settle("a", 0)
        view = self.ledger.hybrid_risk_view()
        self.assertEqual(view["equity"], 100)
        self.assertEqual(view["high_water"], 100)

    def test_failed_hwm_persistence_halts_and_never_publishes_new_peak(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a")
        # Plain record_fill is explicitly in-memory; its peak is not covered
        # by a successful journal transaction and still needs a snapshot.
        self.assertTrue(self.ledger.record_fill(
            "unsaved-sale", "a", shares=10, price=1, side="SELL",
            condition_id="condition-a", status="CONFIRMED", fee=0))
        self.assertEqual(self.ledger.hybrid_risk["high_water"], 100)
        with mock.patch.object(self.ledger, "_write_payload", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "could not be persisted"):
                self.ledger.hybrid_risk_view()
        self.assertEqual(self.ledger.hybrid_risk["high_water"], 100)
        self.assertIsNotNone(self.ledger.persistence_halt)
        with self.assertRaisesRegex(RuntimeError, "healthy durable accounting"):
            self.ledger.hybrid_risk_view()
        # The unsaved sale was never acknowledged as durable or published.
        self.assertEqual(self.restart().hybrid_risk_view()["high_water"], 100)

    def test_initialization_sequence_rejects_intervening_purchase(self):
        sequence = self.ledger.journal_seq
        cash_before = 100
        self.buy("concurrent", shares=20)
        _realized, basis_after, _booked = self.ledger.risk_view(0, ())
        # Mixed reads would manufacture reference capital 100 + 10 = 110.
        with self.assertRaisesRegex(ValueError, "changed during reference reading"):
            self.ledger.hybrid_risk_view(cash_before + basis_after,
                                         expected_journal_seq=sequence)
        self.assertIsNone(self.ledger.hybrid_risk)
        self.assertEqual(self.ledger.hybrid_risk_view(
            100, expected_journal_seq=self.ledger.journal_seq)["reference_equity"], 100)

    def test_initialized_epoch_ignores_unrelated_sequence_changes(self):
        self.ledger.hybrid_risk_view(100, expected_journal_seq=0)
        self.buy("a")
        self.assertEqual(self.ledger.hybrid_risk_view(expected_journal_seq=0)["equity"], 100)
        for invalid in (True, -1, 1.5, "0"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.ledger.hybrid_risk_view(expected_journal_seq=invalid)

    def test_peak_between_refreshes_survives_loss_and_journal_replay(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("gain", shares=40)
        self.settle("gain", 1)
        self.buy("loss", shares=40)
        self.settle("loss", 0)
        # No hybrid_risk_view call occurs between these independent outcomes.
        self.assertEqual(self.ledger.hybrid_risk["high_water"], 120)
        self.assertGreater(self.ledger.journal_pending(), 0)
        view = self.restart().hybrid_risk_view()
        self.assertEqual(view["equity"], 100)
        self.assertEqual(view["high_water"], 120)
        self.assertAlmostEqual((view["high_water"] - view["equity"]) / view["high_water"], 1 / 6)

    def test_partial_sale_peak_between_refreshes_survives_journal_replay(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a", price=.4, fee=.1)
        self.assertTrue(self.ledger.record_fill_durable(
            "sell-a", "a", shares=4, price=.6, side="SELL",
            condition_id="condition-a", status="CONFIRMED", fee=.02))
        self.settle("a", 0)
        view = self.restart().hybrid_risk_view()
        self.assertAlmostEqual(view["equity"], 98.28)
        self.assertAlmostEqual(view["high_water"], 100.74)

    def test_failed_profitable_sale_rolls_back_position_cache_and_peak(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a")
        with mock.patch.object(self.ledger, "_append_journal", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "fill could not be persisted"):
                self.ledger.record_fill_durable(
                    "failed-sale", "a", shares=10, price=1, side="SELL",
                    condition_id="condition-a", status="CONFIRMED", fee=0)
        self.assertEqual(self.ledger.hybrid_risk["high_water"], 100)
        self.assertEqual(self.ledger._hybrid_realized_total, 0)
        self.assertEqual(self.ledger.positions["a"].shares, 10)
        self.assertNotIn("failed-sale", self.ledger.seen)
        self.assertEqual(self.restart().hybrid_risk_view()["high_water"], 100)

    def test_failed_profitable_settlement_cannot_publish_phantom_peak(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a")
        with mock.patch.object(self.ledger, "_append_journal", return_value=False):
            with self.assertRaises(RuntimeError):
                self.settle("a", 1)
        self.assertEqual(self.ledger.hybrid_risk_view()["high_water"], 100)
        self.assertEqual(self.ledger._hybrid_realized_total, 0)
        self.settle("a", 0)
        self.assertEqual(self.restart().hybrid_risk_view()["high_water"], 100)

    def test_settlement_combines_both_hedge_legs_before_observing_peak(self):
        self.ledger.hybrid_risk_view(100)
        for token in ("up", "down"):
            self.assertTrue(self.ledger.record_fill_durable(
                "buy-" + token, token, shares=10, price=.5, condition_id="hedged",
                status="CONFIRMED", fee=0))
        self.ledger.settle(types.SimpleNamespace(
            resolved=True, condition_id="hedged", payouts={"up": 1, "down": 0}))
        view = self.restart().hybrid_risk_view()
        self.assertEqual((view["equity"], view["high_water"]), (100, 100))

    def test_late_fill_reopen_and_failed_reopen_preserve_correct_realized_delta(self):
        self.ledger.hybrid_risk_view(100)
        self.buy("a")
        self.settle("a", 1)
        self.assertEqual(self.ledger.hybrid_risk["high_water"], 105)
        with mock.patch.object(self.ledger, "_append_journal", return_value=False):
            with self.assertRaises(RuntimeError):
                self.ledger.record_fill_durable(
                    "failed-late-buy", "a", shares=1, price=.5,
                    condition_id="condition-a", status="CONFIRMED", fee=0)
        self.assertTrue(self.ledger.positions["a"].settled)
        self.assertEqual(self.ledger._hybrid_realized_total, 5)
        self.restart()
        self.assertTrue(self.ledger.record_fill_durable(
            "late-buy", "a", shares=1, price=.5,
            condition_id="condition-a", status="CONFIRMED", fee=0))
        view = self.ledger.hybrid_risk_view()
        self.assertEqual((view["equity"], view["high_water"]), (100, 105))
        view = self.restart().hybrid_risk_view()
        self.assertEqual((view["equity"], view["high_water"]), (100, 105))

    def test_realized_transitions_use_existing_journal_without_extra_snapshots(self):
        self.ledger.hybrid_risk_view(100)
        with mock.patch.object(self.ledger, "_write_payload", wraps=self.ledger._write_payload) as write:
            self.buy("gain")
            self.settle("gain", 1)
            self.buy("loss")
            self.settle("loss", 0)
            view = self.ledger.hybrid_risk_view()
        self.assertEqual(view["high_water"], 105)
        write.assert_not_called()

    def test_bad_metadata_is_rejected_without_reset_or_partial_reload(self):
        self.ledger.hybrid_risk_view(100)
        payload = json.loads(self.path.read_text())
        variants = [None, {}, {**payload["hybrid_risk"], "ledger_id": "a" * 32},
                    {**payload["hybrid_risk"], "version": True},
                    {**payload["hybrid_risk"], "reference_equity": 0},
                    {**payload["hybrid_risk"], "high_water": 99},
                    {**payload["hybrid_risk"], "pnl_baseline": "0"}]
        original = self.ledger.hybrid_risk
        for metadata in variants:
            with self.subTest(metadata=metadata):
                self.path.write_text(json.dumps({**payload, "hybrid_risk": metadata}))
                self.assertFalse(self.ledger.load())
                self.assertEqual(self.ledger.hybrid_risk, original)
                with self.assertRaises(RuntimeError):
                    Ledger(str(self.path))

    def test_missing_legacy_metadata_can_initialize_without_losing_positions(self):
        self.buy("legacy")
        self.restart()
        self.assertIsNone(self.ledger.hybrid_risk)
        self.assertEqual(self.ledger.hybrid_risk_view(100)["equity"], 100)
        self.assertEqual(self.ledger.positions["legacy"].shares, 10)

    def test_concurrent_initializers_preserve_one_epoch(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda _: self.ledger.hybrid_risk_view(100), range(8)))
        self.assertTrue(all(v["reference_equity"] == 100 for v in values))
        self.assertEqual(self.restart().hybrid_risk_view()["high_water"], 100)


if __name__ == "__main__":
    unittest.main()
