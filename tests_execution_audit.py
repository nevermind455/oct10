"""Regression tests for evidence accounting and deterministic event replay."""
import json
import io
import tarfile
from pathlib import Path
import tempfile
import unittest
from decimal import Decimal as D
from types import SimpleNamespace

import testing_env  # first: never load trading secrets
import execution_audit as A
import execution_replay as R
import guard_eval as G


def event(seq, at, kind="watermark", **kw):
    return dict(token="t", seq=seq, ts_ms=at, kind=kind, **kw)


def book():
    return event(1, 0, "snapshot", asks=[[".50", "10"], [".55", "5"]], bids=[[".49", "10"]])


def order(oid="o", **kw):
    return dict(id=oid, token="t", submit_ms=1, latency_ms=1, kind="FAK", cap=".50",
                shares="10", fee_rate=".07", fee_exponent=1, tick=".01", min_shares="5") | kw


class Replay(unittest.TestCase):
    def replay(self, events=None, orders=None):
        return R.Replay().run(events or [book(), event(2, 100)], orders or [order()])

    def test_latency_meets_new_price_and_never_chases_cap(self):
        events = [book(), event(2, 2, "delta", book_side="ASK", price=".50", size="0"), event(3, 100)]
        result = self.replay(events)
        self.assertEqual(result[0]["status"], "KILLED")
        self.assertEqual(result[0]["fills"], [])

    def test_partial_fak_and_atomic_fok(self):
        orders = [order("fok", shares="15", kind="FOK"), order("fak", submit_ms=3, shares="15")]
        fok, fak = self.replay(orders=orders)
        self.assertEqual(fok["status"], "KILLED")
        self.assertEqual(fak["status"], "PARTIAL_KILLED")
        self.assertEqual(fak["fills"][0]["shares"], D(10))
        self.assertEqual(fak["fills"][0]["fee"], D(".17500"))

    def test_no_depth_resurrection_on_identical_snapshot(self):
        events = [book(), dict(book(), seq=2, ts_ms=3), event(3, 100)]
        first, second = self.replay(events, [order("one"), order("two", submit_ms=4)])
        self.assertEqual(first["status"], "FILLED")
        self.assertEqual(second["status"], "KILLED")

    def test_new_liquidity_after_level_disappears(self):
        events = [book(), event(2, 3, "delta", book_side="ASK", price=".50", size=0),
                  event(3, 4, "delta", book_side="ASK", price=".50", size=8), event(4, 100)]
        one, two = self.replay(events, [order("one"), order("two", submit_ms=5)])
        self.assertEqual(two["fills"][0]["shares"], D(8))
        self.assertEqual(two["status"], "PARTIAL_KILLED")

    def test_competing_trade_consumes_asks_before_the_book_delta_arrives(self):
        events = [book(), event(2, 1, "trade", aggressor="BUY", price=".50", size=10, trade_id="x"),
                  event(3, 3, "delta", book_side="ASK", price=".50", size=0), event(4, 100)]
        self.assertEqual(self.replay(events)[0]["status"], "KILLED")

    def test_a_new_absolute_book_update_does_not_double_deduct_the_print(self):
        events = [book(), event(2, 1, "trade", aggressor="BUY", price=".50", size=5, trade_id="x"),
                  event(3, 1.5, "delta", book_side="ASK", price=".50", size=5), event(4, 100)]
        result = self.replay(events)[0]
        self.assertEqual(result["fills"][0]["shares"], D(5))

    def test_missing_sequence_and_end_of_coverage_are_unknown(self):
        result = self.replay([book(), event(3, 1), event(4, 100)])
        self.assertEqual(result[0]["status"], "UNKNOWN_BOOK")
        self.assertEqual(self.replay(orders=[order(submit_ms=101)])[0]["status"], "UNKNOWN_COVERAGE")

    def test_full_snapshot_recovers_book_but_not_existing_queue_rank(self):
        events = [book(), event(3, 3), dict(book(), seq=4, ts_ms=4), event(5, 100)]
        one, two = self.replay(events, [order("maker", kind="POST_ONLY", cap=".49"), order("taker", submit_ms=5)])
        self.assertEqual(one["status"], "UNKNOWN_AFTER_GAP")
        self.assertEqual(two["status"], "FILLED")

    def test_price_touch_and_bid_cancellations_do_not_fill_maker(self):
        events = [book(), event(2, 3, "delta", book_side="BID", price=".49", size=0),
                  event(3, 4, "trade", aggressor="SELL", price=".49", size=10, trade_id="x"), event(4, 100)]
        result = self.replay(events, [order(kind="POST_ONLY", cap=".49")])[0]
        self.assertEqual(result["fills"], [])
        self.assertEqual(result["ahead"], D(0))

    def test_cancellation_latency_allows_fill_until_effective(self):
        events = [book(), event(2, 3, "trade", aggressor="SELL", price=".49", size=12, trade_id="x"),
                  event(3, 4, "trade", aggressor="SELL", price=".49", size=20, trade_id="y"), event(4, 100)]
        result = self.replay(events, [order(kind="POST_ONLY", cap=".49", cancel_ms=3.5)])[0]
        self.assertEqual(result["status"], "CANCELLED")
        self.assertEqual(sum(f["shares"] for f in result["fills"]), D(2))
        self.assertEqual(result["fills"][0]["fee"], D(0))

    def test_duplicate_prints_and_two_makers_do_not_double_consume_volume(self):
        events = [book(), event(2, 3, "trade", aggressor="SELL", price=".49", size=17, trade_id="x"),
                  event(3, 4, "trade", aggressor="SELL", price=".49", size=17, trade_id="x"), event(4, 100)]
        one, two = self.replay(events, [order("one", kind="POST_ONLY", shares=5, cap=".49"),
                                       order("two", kind="POST_ONLY", submit_ms=1.5, shares=5, cap=".49")])
        self.assertEqual(sum(f["shares"] for f in one["fills"]), D(5))
        self.assertEqual(sum(f["shares"] for f in two["fills"]), D(2))

    def test_post_only_crossing_is_rejected(self):
        self.assertEqual(self.replay(orders=[order(kind="POST_ONLY")])[0]["status"], "REJECTED_POST_ONLY_OR_SIZE")

    def test_size_and_fee_rules_must_be_known(self):
        with self.assertRaises(KeyError):
            spec = order()
            del spec["fee_rate"]
            self.replay(orders=[spec])
        with self.assertRaises(ValueError):
            self.replay(orders=[order(cap=".505")])
        self.assertEqual(self.replay(orders=[order(shares=2)])[0]["status"], "REJECTED_SIZE")

    def test_dollar_amount_is_not_a_share_count_and_fees_use_each_level(self):
        spec = order(cap=".55")
        del spec["shares"]
        spec["amount"] = "6"
        result = self.replay(orders=[spec])[0]
        fills = result["fills"]
        self.assertEqual(sum(f["shares"] * f["price"] for f in fills), D(6))
        self.assertEqual(sum(f["fee"] for f in fills), D(".20650"))


class Evidence(unittest.TestCase):
    def reconciliation_archive(self, members):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "records.tgz"
        with tarfile.open(path, "w:gz") as archive:
            for name, content in members:
                raw = content.encode()
                info = tarfile.TarInfo(name)
                info.size = len(raw)
                archive.addfile(info, io.BytesIO(raw))
        return path

    def test_compacted_zero_evidence_survives_snapshot_rotation_and_repeated_archives(self):
        intent = {"status": "NO_MATCH", "order_id": "failed-then-released", "spent": 0.0,
                  "reserve": 15.92, "signed_order": "PRIVATE_ARCHIVED_BODY"}
        record = json.dumps({"key": "client", "session_id": "old", "intent": intent}) + "\n"
        path = self.reconciliation_archive([
            ("sniperbtc/live_canary_state.json", json.dumps({"intents": {}})),
            ("sniperbtc/live_canary_state.json.archive.jsonl", record + record),
        ])
        ids, provenance = A.reconciled_zeros(path)
        self.assertEqual(ids, {"failed-then-released"})
        self.assertEqual(len(provenance), 2)
        self.assertNotIn("PRIVATE_ARCHIVED_BODY", json.dumps(provenance))

    def test_archived_zero_requires_explicit_numeric_zero_spend(self):
        records = [{"intent": {"status": "NO_MATCH", "order_id": str(i), "spent": spent}}
                   for i, spent in enumerate([None, False, "0", 1, float("nan"), 0])]
        path = self.reconciliation_archive([("sniperbtc/live_canary_state.json.archive.jsonl",
                                             "".join(json.dumps(r) + "\n" for r in records))])
        self.assertEqual(A.reconciled_zeros(path)[0], {"5"})

    def test_duplicate_archive_member_is_rejected(self):
        name = "sniperbtc/live_canary_state.json.archive.jsonl"
        with self.assertRaisesRegex(ValueError, "invalid canary reconciliation"):
            A.reconciled_zeros(self.reconciliation_archive([(name, ""), (name, "")]))

    def test_malformed_archived_intent_is_rejected(self):
        path = self.reconciliation_archive([("sniperbtc/live_canary_state.json.archive.jsonl", '{"intent": []}\n')])
        with self.assertRaisesRegex(ValueError, "invalid archived canary intent"):
            A.reconciled_zeros(path)

    def test_terminal_canary_zero_keeps_original_reservation_but_no_signed_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "records.tgz"
            state = {"intents": {"a": {"order_id": "o", "status": "NO_MATCH", "reserve": 17, "spent": 0,
                                        "signed_order": "PRIVATE_PAYLOAD"},
                                 "b": {"order_id": "bad", "status": "NO_MATCH", "spent": 1}}}
            data = json.dumps(state).encode()
            with tarfile.open(path, "w:gz") as t:
                info = tarfile.TarInfo("sniperbtc/live_canary_state.json")
                info.size = len(data)
                t.addfile(info, io.BytesIO(data))
            ids, provenance = A.reconciled_zeros(path)
            self.assertEqual(ids, {"o"})
            self.assertNotIn("PRIVATE_PAYLOAD", json.dumps(provenance))

    def test_grid_has_no_fabricated_exact_instant_or_tail(self):
        shadow = {"L": {"300": {"sh": 10}, "400": {"miss": "FAK no-fill"}}}
        self.assertEqual(G.entry_at(shadow, 399), {})
        self.assertEqual(G.entry_at(shadow, 400), shadow["L"]["400"])
        self.assertEqual(G.entry_at(shadow, None), {})
        self.assertIsNone(G.delay_of({}))
        self.assertEqual(A.snapshot(shadow, 399, "floor")[0], 300)
        self.assertEqual(A.snapshot(shadow, 399, "next")[0], 400)
        self.assertEqual(A.snapshot(shadow, 450, "next"), (None, None))

    def test_unknown_book_is_not_a_failed_match(self):
        self.assertIsNone(A.quote({"miss": "unknown book state"}))
        self.assertIsNone(A.quote({"miss": "FOK no-fill: partial liquidity is unknown"}))
        self.assertEqual(A.quote({"miss": "FAK no-fill: no liquidity"}), ())
        self.assertIsNone(A.fill_pnl(None, 1))
        self.assertEqual(A.fill_pnl((), None), 0)
        self.assertFalse(G.quote_known({"miss": "unknown book state"}))
        self.assertFalse(G.quote_known({"miss": "FOK no-fill"}))
        self.assertTrue(G.quote_known({"miss": "FOK no-fill"}, fok=True))
        self.assertTrue(G.quote_known({"miss": "FOK no-fill", "fak": {"miss": "FAK no-fill"}}))

    def test_split_settlement_does_not_pay_the_whole_winner(self):
        self.assertAlmostEqual(G.pnl((10, .40, .1), "UP", "50/50"), .9)
        self.assertAlmostEqual(G.pnl((10, .40, .1), "DOWN", {"DOWN": .25}), -1.6)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.json"
            path.write_text(json.dumps({"positions": {"t": {"token_id": "t", "condition_id": "c", "settled": True, "payout_per_share": .5}}}))
            self.assertEqual(G.winners(tmp, str(path), {"t": "UP"})["c"], {"UP": .5, "DOWN": .5})

    def test_receipt_is_not_a_confirmed_fill_and_missing_ids_never_join(self):
        rows = [{"id": "a", "wall": 1, "result": "filled", "side": "UP", "bp": 1, "live": True,
                 "kind": "MATCHED", "client_order_id": "x", "condition_id": "c", "order_id": "o", "shares": 20, "price": .5},
                {"result": "live_send", "client_order_id": "x", "order_id": "o", "kind": "MATCHED"}]
        ledger = {"positions": {"t": {"lots": [{"status": "FAILED", "side": "BUY", "order_id": "o"}]}}}
        orders, _, _ = A.orders_from(rows, ledger, [], True)
        self.assertIsNone(orders[0]["actual"])
        orders, _, _ = A.orders_from(rows, ledger, [], True, {"o"})
        self.assertEqual(orders[0]["actual"], ())
        self.assertEqual(orders[0]["truth"], "RECONCILED_ZERO_CANARY")
        paper = [{"id": "b", "wall": 1, "result": "refused", "side": "UP", "bp": 1, "condition_id": "c"}]
        ledger["positions"]["t"]["lots"] = [{"status": "CONFIRMED", "side": "BUY", "order_id": None, "trade_id": "paper-other", "shares": 10, "price": .5, "fee": .1}]
        orders, _, unmatched = A.orders_from(paper, ledger, [], False)
        self.assertEqual(orders[0]["actual"], ())
        self.assertEqual(unmatched, 1)

    def test_guard_evaluation_does_not_count_a_provisional_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            row = {"id": "a", "wall": 1, "result": "filled", "side": "UP", "bp": 1,
                   "live": True, "kind": "MATCHED", "order_id": "o", "shares": 10, "price": .5, "condition_id": "c"}
            (root / "live.jsonl").write_text(json.dumps(row) + "\n")
            (root / "ledger.json").write_text('{"positions": {}}')
            order = G.load(tmp, str(root / "live.jsonl"), str(root / "ledger.json"), None)[0]
            self.assertIsNone(order["actual"])
            self.assertFalse(order["actual_known"])

    def test_corrupt_journal_does_not_disappear_from_report_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.jsonl"
            path.write_text('{"result":"start"}\n{broken\n', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "malformed journal record"):
                G._rows(str(path))

    def test_arrival_diagnostics_use_fak_for_partial_orders(self):
        import paper_trade as P
        from unittest.mock import Mock
        rules = P.MarketRules("c", D(".07"), 1, D(5), D(".01"))
        book = P.BookSnapshot("t", ((D(".50"), D(4)),), best_bid=D(".49"))
        broker = SimpleNamespace(_net_of_own_fills=lambda b: b, _check_book=Mock(), max_spread=.1, _quote=P.PaperBroker._quote)
        fak = P.PaperBroker._quote_outcome(broker, book, "UP", 5, .50, rules, 0, fak=True)
        fok = P.PaperBroker._quote_outcome(broker, book, "UP", 5, .50, rules, 0, fak=False)
        self.assertTrue(fak["fill"])
        self.assertFalse(fok["fill"])


if __name__ == "__main__":
    unittest.main()
