"""Actual multi-level fill accounting and durable replay regressions, offline."""
import concurrent.futures
import json
import pathlib
import tempfile
import types
import unittest
from decimal import Decimal, ROUND_HALF_UP

import testing_env  # noqa: F401 - isolate credentials and external networking first
from accounting.ledger import Ledger
from feeds.poly_user import Fill, FillStore


CID = "0x" + "ab" * 32
REAL_LEGS = [
    {"asset_id": "1002", "matched_amount": "3.22", "price": "0.91"},
    {"asset_id": "1002", "matched_amount": "7", "price": "0.90"},
    {"asset_id": "1002", "matched_amount": "0.102", "price": "0.90"},
]


class FillAccountingFixes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = pathlib.Path(self.temp.name) / "ledger.json"
        self.ledger = Ledger(str(self.path))

    def tearDown(self):
        self.temp.cleanup()

    def authorize(self, amount=1.0, exponent=1):
        self.ledger.authorize_order("order", {
            "condition_id": CID, "token_id": "1001", "window_end": 1800000000,
            "requested_notional": amount, "fee_rate": 0.07, "fee_exponent": exponent,
        })

    def store(self, trade="trade", size="10.322", price="0.10", legs=REAL_LEGS):
        store = FillStore()
        store.record_trade(trade, order_id="order", asset_id="1001", market=CID,
                           side="BUY", price=price, size=size, status="CONFIRMED",
                           maker_orders=legs)
        return store

    def test_real_multilevel_fill_books_cash_and_each_leg_fee(self):
        self.authorize()
        store = self.store()
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        position = self.ledger.positions["1001"]
        lot = position.lots[0]
        self.assertEqual(store.fills["trade"].paid, 1.0)
        self.assertEqual(lot.reported_price, 0.1)
        self.assertAlmostEqual(lot.notional, 1.0)
        self.assertAlmostEqual(position.fees, 0.06320)
        self.assertAlmostEqual(position.cost, 1.06320)
        self.ledger.settle(types.SimpleNamespace(resolved=True, condition_id=CID,
                                               payouts={"1001": 1.0, "1002": 0.0}))
        self.assertAlmostEqual(position.realized, 10.322 - 1.06320)

    def test_nonlinear_curve_is_applied_at_each_execution_price(self):
        self.authorize(exponent=2)
        legs = [{"asset_id": "1001", "matched_amount": "1", "price": price}
                for price in ("0.1", "0.9")]
        self.assertEqual(self.ledger.ingest_fill_store(
            self.store(size="2", price="0.9", legs=legs)), 1)
        expected = sum((Decimal("0.07") * (p * (1 - p)) ** 2).quantize(
            Decimal("0.00001"), rounding=ROUND_HALF_UP)
                       for p in (Decimal("0.1"), Decimal("0.9")))
        position = self.ledger.positions["1001"]
        self.assertAlmostEqual(position.avg_price, 0.5)
        self.assertAlmostEqual(position.fees, float(expected))
        self.assertAlmostEqual(position.fees, 0.00114)

    def test_duplicate_full_fill_does_not_reapply_authorization(self):
        self.authorize()
        store = self.store()
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        before = self.ledger.positions["1001"].cost
        for _ in range(3):
            self.assertEqual(self.ledger.ingest_fill_store(store), 0)
        self.assertEqual(self.ledger.skipped_authorization_mismatch, 0)
        self.assertEqual(self.ledger.positions["1001"].cost, before)
        self.assertEqual(len(self.ledger.positions["1001"].lots), 1)

    def test_partial_trades_use_cash_for_cumulative_budget(self):
        self.authorize()
        first = self.store("first", size="5.1", legs=[
            {"asset_id": "1001", "matched_amount": "1", "price": "0.09"},
            {"asset_id": "1001", "matched_amount": "4.1", "price": "0.10"}])
        second = self.store("second", size="3", price="0.2", legs=[
            {"asset_id": "1001", "matched_amount": "1", "price": "0.1"},
            {"asset_id": "1001", "matched_amount": "2", "price": "0.2"}])
        self.assertEqual(self.ledger.ingest_fill_store(first), 1)
        first.fills.update(second.fills)
        self.assertEqual(self.ledger.ingest_fill_store(first), 1)
        self.assertEqual(self.ledger.ingest_fill_store(first), 0)
        self.assertEqual(self.ledger.skipped_authorization_mismatch, 0)
        self.assertAlmostEqual(self.ledger.confirmed_notional_for_condition(CID), 1.0)
        extra = self.store("overfill", size="0.3", legs=None)
        self.assertEqual(self.ledger.ingest_fill_store(extra), 0)
        self.assertEqual(self.ledger.skipped_authorization_mismatch, 1)

    def test_concurrent_partial_ingestion_cannot_exceed_order_budget(self):
        self.authorize()
        stores = [self.store(name, size="1.2", price="0.5", legs=None)
                  for name in ("first", "second")]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            counts = list(pool.map(self.ledger.ingest_fill_store, stores))
        self.assertEqual(sum(counts), 1)
        self.assertAlmostEqual(self.ledger.confirmed_notional_for_condition(CID), 0.6)

    def test_cost_and_headline_survive_journal_and_snapshot_restart(self):
        self.authorize()
        self.assertTrue(self.ledger.save())
        store = self.store()
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        self.assertEqual(self.ledger.journal_pending(), 1)
        restored = Ledger(str(self.path))
        self.assertEqual(restored.journal_replayed, 1)
        lot = restored.positions["1001"].lots[0]
        self.assertAlmostEqual(lot.notional, 1.0)
        self.assertAlmostEqual(lot.fee, 0.06320)
        self.assertEqual(lot.reported_price, 0.1)
        self.assertTrue(restored.save())
        restored = Ledger(str(self.path))
        self.assertEqual(restored.journal_replayed, 0)
        self.assertAlmostEqual(restored.positions["1001"].cost, 1.06320)
        self.assertEqual(restored.ingest_fill_store(store), 0)
        self.assertEqual(restored.skipped_authorization_mismatch, 0)
        self.assertTrue(restored.rest_trade_is_booked({
            "id": "trade", "asset_id": "1001", "market": CID,
            "taker_order_id": "order", "side": "BUY", "price": "0.1",
            "size": "10.322", "status": "CONFIRMED"}))

    def test_changed_headline_replay_still_fails_closed(self):
        self.authorize()
        self.ledger.ingest_fill_store(self.store())
        changed = self.store(price="0.11")
        with self.assertRaisesRegex(RuntimeError, "price"):
            self.ledger.ingest_fill_store(changed)
        self.assertAlmostEqual(self.ledger.positions["1001"].cost, 1.06320)

    def test_legacy_journal_without_headline_field_remains_readable(self):
        self.assertTrue(self.ledger.save())
        self.assertTrue(self.ledger.record_fill_durable(
            "legacy", "1001", shares=2, price=0.5, fee=0.1, condition_id=CID))
        journal = pathlib.Path(self.ledger.journal_path)
        entry = json.loads(journal.read_text())
        entry.pop("reported_price")
        journal.write_text(json.dumps(entry) + "\n")
        restored = Ledger(str(self.path))
        self.assertAlmostEqual(restored.positions["1001"].cost, 1.1)
        self.assertIsNone(restored.positions["1001"].lots[0].reported_price)

    def test_invalid_persisted_headline_is_rejected(self):
        self.authorize()
        self.ledger.ingest_fill_store(self.store())
        self.assertTrue(self.ledger.save())
        payload = json.loads(self.path.read_text())
        payload["positions"]["1001"]["lots"][0]["reported_price"] = True
        self.path.write_text(json.dumps(payload))
        with self.assertRaisesRegex(RuntimeError, "unreadable or invalid"):
            Ledger(str(self.path))

    def test_omitted_legs_retain_existing_execution_evidence(self):
        store = self.store()
        store.record_trade("trade", order_id="order", asset_id="1001", market=CID,
                           side="BUY", price="0.1", size="10.322", status="CONFIRMED")
        self.assertEqual(store.fills["trade"].paid, 1.0)
        self.assertEqual(len(store.fills["trade"].maker_legs), 3)
        self.authorize()
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        self.assertAlmostEqual(self.ledger.positions["1001"].cost, 1.06320)

    def test_reordered_maker_legs_are_the_same_trade(self):
        store = self.store()
        store.record_trade("trade", order_id="order", asset_id="1001", market=CID,
                           side="BUY", price="0.1", size="10.322", status="CONFIRMED",
                           maker_orders=list(reversed(REAL_LEGS)))
        self.assertTrue(store.fills["trade"].counts)

    def test_changed_maker_cash_is_a_conflict(self):
        store = self.store()
        changed = [dict(leg, price="0.8") for leg in REAL_LEGS]
        store.record_trade("trade", order_id="order", asset_id="1001", market=CID,
                           side="BUY", price="0.1", size="10.322", status="CONFIRMED",
                           maker_orders=changed)
        self.assertEqual(store.fills["trade"].status, "CONFLICT")
        self.assertFalse(store.fills["trade"].counts)

    def test_paid_only_uses_cash_weighted_price(self):
        self.authorize()
        fill = Fill("trade", asset_id="1001", market=CID, side="BUY", price=0.9,
                    size=2, status="CONFIRMED", order_id="order", paid=1.0)
        self.assertEqual(self.ledger.ingest_fill_store(
            types.SimpleNamespace(fills={"trade": fill})), 1)
        self.assertAlmostEqual(self.ledger.positions["1001"].avg_price, 0.5)
        self.assertAlmostEqual(self.ledger.positions["1001"].fees, 0.035)

    def test_no_legs_or_paid_keeps_headline_fallback(self):
        self.authorize()
        self.assertEqual(self.ledger.ingest_fill_store(
            self.store(size="2", price="0.5", legs=None)), 1)
        self.assertAlmostEqual(self.ledger.positions["1001"].cost, 1.035)

    def test_late_maker_evidence_cannot_silently_change_booked_cash_and_fees(self):
        self.authorize(amount=2)
        store = self.store(legs=None)
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        before = self.ledger.positions["1001"].cost
        store.record_trade("trade", order_id="order", asset_id="1001", market=CID,
                           side="BUY", price="0.1", size="10.322", status="CONFIRMED",
                           maker_orders=REAL_LEGS)
        with self.assertRaisesRegex(RuntimeError, "paid cash.*maker-leg fee"):
            self.ledger.ingest_fill_store(store)
        self.assertIsNotNone(self.ledger.integrity_error)
        self.assertEqual(self.ledger.positions["1001"].cost, before)

    def test_same_cash_late_legs_with_different_fee_fail_closed(self):
        self.authorize()
        store = self.store(size="2", price="0.5", legs=None)
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        legs = [{"asset_id": "1001", "matched_amount": "1", "price": price}
                for price in ("0.1", "0.9")]
        store.record_trade("trade", order_id="order", asset_id="1001", market=CID,
                           side="BUY", price="0.5", size="2", status="CONFIRMED",
                           maker_orders=legs)
        with self.assertRaisesRegex(RuntimeError, "maker-leg fee"):
            self.ledger.ingest_fill_store(store)
        self.assertNotIn("paid cash", self.ledger.integrity_error)
        self.assertAlmostEqual(self.ledger.positions["1001"].fees, 0.035)

    def test_exact_execution_replay_passes_ws_and_rest_after_restart(self):
        self.authorize()
        store = self.store()
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        self.assertTrue(self.ledger.save())
        restored = Ledger(str(self.path))
        self.assertEqual(restored.ingest_fill_store(store), 0)
        self.assertTrue(restored.rest_trade_is_booked({
            "id": "trade", "asset_id": "1001", "market": CID,
            "taker_order_id": "order", "side": "BUY", "price": "0.1",
            "size": "10.322", "status": "CONFIRMED", "maker_orders": REAL_LEGS}))
        self.assertIsNone(restored.integrity_error)

    def test_rest_known_trade_suppression_rejects_late_execution_evidence(self):
        from feeds.reconcile import RestReconciler
        self.authorize(amount=2)
        self.assertEqual(self.ledger.ingest_fill_store(self.store(legs=None)), 1)
        reconcile = RestReconciler(FillStore(), known_trade=self.ledger.rest_trade_is_booked)
        # This is the predicate evaluated before REST rows can be suppressed.
        with self.assertRaisesRegex(RuntimeError, "paid cash"):
            reconcile._known_trade({
                "id": "trade", "asset_id": "1001", "market": CID,
                "taker_order_id": "order", "side": "BUY", "price": "0.1",
                "size": "10.322", "status": "CONFIRMED", "maker_orders": REAL_LEGS})
        self.assertIsNotNone(self.ledger.integrity_error)

    def test_malformed_supplied_rest_execution_evidence_is_not_suppressed(self):
        self.authorize()
        self.assertEqual(self.ledger.ingest_fill_store(self.store()), 1)
        for evidence in ([], {}, [{"asset_id": "1002", "matched_amount": "bad", "price": "0.9"}]):
            with self.subTest(evidence=evidence):
                with self.assertRaisesRegex(RuntimeError, "malformed supplied maker orders"):
                    self.ledger.rest_trade_is_booked({"id": "trade", "maker_orders": evidence})
        self.assertIsNotNone(self.ledger.integrity_error)

    def test_paid_only_late_change_is_not_suppressed(self):
        self.authorize()
        store = self.store(size="2", price="0.5", legs=None)
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        store.fills["trade"].paid = 0.9
        with self.assertRaisesRegex(RuntimeError, "paid cash"):
            self.ledger.ingest_fill_store(store)

    def test_integrity_failure_is_sticky_and_survives_snapshot_restart(self):
        self.authorize()
        store = self.store()
        self.ledger.ingest_fill_store(store)
        with self.assertRaises(RuntimeError):
            self.ledger.rest_trade_is_booked({"id": "trade", "paid": 0.9})
        initial_error = self.ledger.integrity_error
        self.assertEqual(self.ledger.ingest_fill_store(store), 0)
        self.assertEqual(self.ledger.integrity_error, initial_error)
        self.assertTrue(self.ledger.save())
        restored = Ledger(str(self.path))
        self.assertEqual(restored.integrity_error, initial_error)
        self.assertEqual(restored.ingest_fill_store(store), 0)
        self.assertEqual(restored.integrity_error, initial_error)

    def test_late_feed_conflict_on_a_booked_trade_is_an_integrity_failure(self):
        self.authorize()
        store = self.store()
        self.assertEqual(self.ledger.ingest_fill_store(store), 1)
        changed = [dict(leg, price="0.8") for leg in REAL_LEGS]
        store.record_trade("trade", order_id="order", asset_id="1001", market=CID,
                           side="BUY", price="0.1", size="10.322", status="CONFIRMED",
                           maker_orders=changed)
        self.assertEqual(store.fills["trade"].status, "CONFLICT")
        with self.assertRaisesRegex(RuntimeError, "terminal status"):
            self.ledger.ingest_fill_store(store)
        self.assertIsNotNone(self.ledger.integrity_error)


if __name__ == "__main__":
    unittest.main()
