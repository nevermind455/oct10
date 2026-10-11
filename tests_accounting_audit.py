"""Accounting and discovery regressions; offline, with fake venue responses."""
from __future__ import annotations

import asyncio
import copy
import contextlib
import io
import pathlib
import socket
import tempfile
import unittest
from unittest.mock import Mock, patch

import testing_env  # noqa: F401 - isolate before importing bot configuration
import durable_io
import market_discovery as discovery
import run_tests
from accounting import resolution as R
from accounting.ledger import Ledger, _pos_dict, _validate_loaded_position
from accounting.settlement import SettlementWorker

CID = "0x" + "ab" * 32
ROWS = [{"token_id": "1001", "outcome": "Up", "price": "0.5", "winner": False},
        {"token_id": "1002", "outcome": "Down", "price": "0.5", "winner": False}]


def response(data):
    return Mock(status_code=200, json=Mock(return_value=data))


class ResolutionAudit(unittest.TestCase):
    def test_direct_chain_fetch_proves_slot_order(self):
        market = {"condition_id": CID, "closed": False, "tokens": ROWS}
        with patch.object(R.http_pool, "get", return_value=response(market)), \
                patch.object(R.onchain_resolution, "enabled", return_value=True), \
                patch.object(R.onchain_resolution, "slot_positions", return_value=(
                    {"1001": 1, "1002": 0}, "proven")), \
                patch.object(R.onchain_resolution, "payouts_for", return_value=(
                    {"1001": 1.0, "1002": 0.0}, "chain")) as payouts:
            result = R.fetch(CID)
        payouts.assert_called_once_with(CID, ["1002", "1001"])
        self.assertTrue(result.resolved)
        self.assertEqual(result.winning_label, "Up")

    def test_unproven_chain_order_cannot_settle(self):
        market = {"condition_id": CID, "closed": False, "tokens": ROWS}
        for proof in (None, {"1001": 0, "1002": 0}, {"1001": False, "1002": 1}):
            with self.subTest(proof=proof), \
                    patch.object(R.http_pool, "get", return_value=response(market)), \
                    patch.object(R.onchain_resolution, "enabled", return_value=True), \
                    patch.object(R.onchain_resolution, "slot_positions", return_value=(proof, "unproven")), \
                    patch.object(R.onchain_resolution, "payouts_for") as payouts:
                self.assertFalse(R.fetch(CID).resolved)
                payouts.assert_not_called()

    def test_malformed_or_mismatched_clob_response_fails_closed(self):
        for data in (None, [], "bad", {"condition_id": "0x" + "cd" * 32}):
            with self.subTest(data=data), patch.object(R.http_pool, "get", return_value=response(data)):
                self.assertEqual(R.fetch(CID).status, R.UNKNOWN)

    def test_gamma_rejects_boolean_huge_integer_and_object_payouts(self):
        market = {"conditionId": CID, "closed": True, "resolved": True,
                  "clobTokenIds": ["1001", "1002"], "outcomes": ["Up", "Down"],
                  "outcomePrices": [True, False]}
        for prices in ([True, False], [10 ** 1000, 0], {"a": 1, "b": 0}):
            with self.subTest(prices_type=type(prices).__name__):
                self.assertEqual(R.parse_market({**market, "outcomePrices": prices}).status, R.UNKNOWN)
        self.assertEqual(R.parse_market(None).status, R.UNKNOWN)
        self.assertEqual(R.parse_market({**market, "clobTokenIds": [None, "1002"]}).status, R.UNKNOWN)

    def test_clob_rejects_boolean_and_huge_integer_prices(self):
        for price in (True, 10 ** 1000):
            rows = copy.deepcopy(ROWS)
            rows[0]["price"] = price
            self.assertEqual(R.parse_clob_market({"condition_id": CID, "closed": True,
                                                  "tokens": rows}, CID).status, R.UNKNOWN)

    def test_resolver_cache_views_cannot_change_slot_mapping(self):
        resolver = R.Resolver()
        resolver._slots[CID] = [("1001", "Up"), ("1002", "Down")]
        resolver._proof[CID] = {"1001": 0, "1002": 1}
        resolver.slots(CID).reverse()
        resolver.proof(CID)["1001"] = 1
        self.assertEqual(resolver.slots(CID)[0], ("1001", "Up"))
        self.assertEqual(resolver.proof(CID), {"1001": 0, "1002": 1})


class LedgerAudit(unittest.TestCase):
    def ledger(self):
        return Ledger(path="unused-audit-ledger.invalid", allow_sells=True)

    def test_rejected_sell_does_not_create_or_reopen_position(self):
        led = self.ledger()
        self.assertFalse(led.record_fill("no-stock", "1001", shares=1e-10, price=0.5,
                                         condition_id=CID, side="SELL", fee=0))
        self.assertEqual(led.positions, {})
        self.assertTrue(led.record_fill("buy", "1001", shares=10, price=0.5, condition_id=CID, fee=0))
        position = led.positions["1001"]
        position.settled, position.payout_per_share = True, 1.0
        position.realized, position.settled_wall = 5.0, 1.0
        before = _pos_dict(position)
        self.assertFalse(led.record_fill("oversell", "1001", shares=11, price=0.5,
                                         condition_id=CID, side="SELL", fee=0))
        self.assertEqual(_pos_dict(position), before)

    def test_fill_time_exponent_and_aggregate_overflow_are_rejected(self):
        led = self.ledger()
        for wall in (float("nan"), float("inf"), -1, "invalid", 10 ** 1000):
            self.assertFalse(led.record_fill("bad-wall", "1001", shares=10, price=0.5,
                                             condition_id=CID, wall=wall, fee=0))
        self.assertFalse(led.record_fill("bad-exponent", "1001", shares=10, price=0.5,
                                         condition_id=CID, fee_exponent=0))
        self.assertFalse(led.record_fill("too-big", "1001", shares=10 ** 1000, price=0.5,
                                         condition_id=CID, fee=0))
        self.assertFalse(led.record_fill("fee-overflow", "1001", shares=1e308, price=0.5,
                                         condition_id=CID))
        self.assertTrue(led.record_fill("huge", "1001", shares=1e308, price=0.5, condition_id=CID, fee=0))
        before = _pos_dict(led.positions["1001"])
        self.assertFalse(led.record_fill("overflow", "1001", shares=1e308, price=0.5, condition_id=CID, fee=0))
        self.assertEqual(_pos_dict(led.positions["1001"]), before)

    def test_sell_overflow_leaves_fifo_unchanged(self):
        led = self.ledger()
        self.assertTrue(led.record_fill("buy", "1001", shares=1e308, price=0.1, condition_id=CID, fee=0))
        pos = led.positions["1001"]
        pos.realized_from_sales = 1.7e308
        before = _pos_dict(pos)
        self.assertFalse(led.record_fill("overflow", "1001", shares=1e308, price=1,
                                         condition_id=CID, side="SELL", fee=0))
        self.assertEqual(_pos_dict(pos), before)

    def test_reconciliation_includes_net_sell_proceeds(self):
        led = self.ledger()
        led.record_fill("buy", "1001", shares=10, price=0.5, condition_id=CID, fee=0.1, wall=10)
        led.record_fill("sell", "1001", shares=5, price=0.6, condition_id=CID,
                        side="SELL", fee=0.05, wall=11)
        led.balance_marks = [(9, 100), (12, 97.85)]
        result = led.reconcile_balance()
        self.assertEqual(result["status"], "OK")
        self.assertAlmostEqual(result["expected_delta"], -2.15)
        self.assertAlmostEqual(result["confirmed_sell_proceeds_in_window"], 2.95)

    def test_snapshot_validation_rejects_sale_before_purchase(self):
        led = self.ledger()
        led.record_fill("buy", "1001", shares=10, price=0.5, condition_id=CID, fee=0)
        led.record_fill("sell", "1001", shares=5, price=0.6, condition_id=CID, side="SELL", fee=0)
        pos = copy.deepcopy(led.positions["1001"])
        _validate_loaded_position("1001", pos)
        pos.lots.reverse()
        with self.assertRaises(ValueError):
            _validate_loaded_position("1001", pos)

    def test_snapshot_validation_rejects_unrecorded_sell_fees(self):
        led = self.ledger()
        led.record_fill("buy", "1001", shares=10, price=0.5, condition_id=CID, fee=0)
        led.record_fill("sell", "1001", shares=5, price=0.6, condition_id=CID, side="SELL", fee=0.1)
        pos = copy.deepcopy(led.positions["1001"])
        pos.sell_fees += 1
        with self.assertRaises(ValueError):
            _validate_loaded_position("1001", pos)

    def test_ledger_save_retries_windows_sharing_error(self):
        with tempfile.TemporaryDirectory() as directory:
            led = Ledger(path=str(pathlib.Path(directory) / "ledger.json"))
            original_replace = durable_io.os.replace
            error = PermissionError("transient sharing lock")
            error.winerror = 32
            calls = []

            def replace(source, target):
                calls.append((source, target))
                if len(calls) == 1:
                    raise error
                return original_replace(source, target)

            with patch.object(durable_io.os, "replace", side_effect=replace), \
                    patch.object(durable_io.time, "sleep") as sleep:
                self.assertTrue(led.save(), led.last_persistence_error)
            self.assertEqual(len(calls), 2)
            sleep.assert_called_once_with(0.005)
            self.assertEqual(Ledger(path=led.path).ledger_id, led.ledger_id)

    def test_persistent_sharing_error_preserves_prior_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            led = Ledger(path=str(pathlib.Path(directory) / "ledger.json"))
            self.assertTrue(led.save())
            before = pathlib.Path(led.path).read_bytes()
            error = PermissionError("persistent sharing lock")
            error.winerror = 5
            with patch.object(durable_io.os, "replace", side_effect=error) as replace, \
                    patch.object(durable_io.time, "sleep") as sleep:
                self.assertFalse(led.save())
            self.assertEqual(replace.call_count, 4)
            self.assertEqual(sleep.call_count, 3)
            self.assertEqual(pathlib.Path(led.path).read_bytes(), before)
            self.assertIn("persistent sharing lock", led.last_persistence_error)
            self.assertEqual(list(pathlib.Path(directory).glob("*.tmp")), [])


class InfrastructureAudit(unittest.TestCase):
    def test_discovery_response_that_crosses_expiry_is_rejected(self):
        with patch.object(discovery, "_current_5m_window_start_unix", side_effect=[300, 600]), \
                patch.object(discovery, "_fetch_slug", return_value={}), \
                patch.object(discovery, "_parse_event") as parse:
            self.assertIsNone(discovery.get_btc_5m_tokens(300))
            parse.assert_not_called()

    def test_socket_audit_blocks_external_connect_and_dns_before_io(self):
        with socket.socket() as sock:
            with self.assertRaises(PermissionError):
                sock.connect(("192.0.2.1", 443))
        with self.assertRaises(PermissionError):
            socket.getaddrinfo("venue.invalid", 443)
        self.assertTrue(socket.getaddrinfo("localhost", 443))

    def test_unknown_suite_is_a_failure(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(run_tests.main(["does-not-exist"]), 2)

    def test_runner_continues_after_suite_timeout(self):
        import subprocess
        suites = [pathlib.Path("tests_a.py"), pathlib.Path("tests_b.py")]
        passed = subprocess.CompletedProcess([], 0, stdout="OK\n", stderr="")
        with patch.object(run_tests.subprocess, "run", side_effect=[
                subprocess.TimeoutExpired([], 1200), passed]) as run, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(run_tests._run_suites(suites, {}), 1)
        self.assertEqual(run.call_count, 2)

    def test_failed_settlement_journal_flush_keeps_events(self):
        async def run():
            worker = SettlementWorker(Ledger(path="unused-audit-ledger.invalid"),
                                      journal_path="unused-audit-journal.invalid", fetch=lambda _: None)
            worker._journal = [{"event": "RESULT_RECEIVED"}]

            async def fail():
                raise PermissionError("temporarily locked")

            worker._in_pool = lambda *_args: fail()
            await worker._flush_journal()
            self.assertEqual(worker._journal, [{"event": "RESULT_RECEIVED"}])
            self.assertIsNotNone(worker.journal_error)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main(verbosity=2)
