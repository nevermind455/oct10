"""Offline regressions for order publication, freshness and outcome races."""
from __future__ import annotations

import asyncio
import concurrent.futures
import io
import tempfile
import threading
import time
import types
import unittest
from contextlib import ExitStack, redirect_stdout
from dataclasses import replace
from unittest.mock import patch

import testing_env  # noqa: F401 - never read deployment credentials
import paper_trade as paper
import polymarket_trade as live
import position_manager as manager
import tests_paper as fixtures
import tests_position_manager as pmfixtures


class PaperOrderAudit(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.broker = fixtures._broker(self.temp.name)
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()

    def tearDown(self):
        self.output.__exit__(None, None, None)
        self.temp.cleanup()

    def order(self, **kw):
        return self.broker.place_trade("UP", 2, "up", "down", "cond", fixtures.round_end(), **kw)

    def test_slot_cannot_be_pruned_before_claim(self):
        for i in range(65):
            self.broker._execution_locks[(f"old-{i}", "sniper")] = threading.Lock()
        key = ("retired", "sniper")
        observed = []

        def execute(*args, **kwargs):
            observed.append(key in self.broker._execution_locks)
            observed.append(self.broker._execution_locks[key].locked())
            # A second caller must see the same claimed slot, even if the token
            # was removed from current market context before worker dispatch.
            observed.append(self.broker.place_trade("UP", 2, "retired", "down", "cond",
                                                    fixtures.round_end(), tag="sniper"))
            return True

        with patch.object(self.broker, "_place_trade", execute):
            self.assertTrue(self.broker.place_trade("UP", 2, "retired", "down", "cond",
                                                   fixtures.round_end(), tag="sniper"))
        self.assertEqual(observed, [True, True, False])
        self.assertFalse(self.broker._execution_locks[key].locked())

    def test_wrong_token_at_decision_cannot_fill(self):
        self.broker._book_fetch = lambda token: fixtures.book("down")
        self.assertFalse(self.order())
        self.assertIn("token does not match", self.broker.last_error)
        self.assertFalse(self.broker.ledger.seen)

    def test_wrong_token_at_arrival_cannot_fill(self):
        books = iter([fixtures.book("up"), fixtures.book("down")])
        self.broker._book_fetch = lambda token: next(books)
        self.assertFalse(self.order())
        self.assertIn("token does not match", self.broker.last_error)
        self.assertFalse(self.broker.ledger.seen)

    def test_market_rotation_during_delay_cannot_book(self):
        context = {"condition_id": "cond", "up_token_id": "up", "down_token_id": "down"}
        self.broker.market_context = lambda: dict(context)
        with patch.object(paper.time, "sleep", lambda seconds: context.update(condition_id="next")):
            self.assertFalse(self.order(latency_ms=1))
        self.assertIn("market changed", self.broker.last_error)
        self.assertFalse(self.broker.ledger.seen)

    def test_future_receipt_timestamp_rejected(self):
        snap = replace(fixtures.book(), received_wall=time.time() + 100)
        self.broker._book_fetch = lambda token: snap
        self.assertFalse(self.order())
        self.assertIn("receipt is future-dated", self.broker.last_error)

    def test_exit_rejects_missing_exchange_timestamp(self):
        snap = replace(fixtures.book(), timestamp=None, bids=((fixtures.D("0.39"), fixtures.D("10")),))
        self.broker._book_fetch = lambda token: snap
        self.assertEqual(self.broker.sell_shares("up", 1), 0)
        self.assertIn("omitted exchange timestamp", self.broker.last_error)

    def test_new_order_clears_old_fill_before_fatal_failure(self):
        self.assertTrue(self.order())
        self.broker._rules_fetch = lambda token: (_ for _ in ()).throw(RuntimeError("offline failure"))
        self.broker._rules_cache.clear()
        self.assertFalse(self.order())
        self.assertIsNone(self.broker.last_fill)


class PositionOutcomeAudit(unittest.TestCase):
    def setUp(self):
        self.world = pmfixtures.World()
        self.pm = self.world.pm
        self.pm.hist = self.pm._hist_for(pmfixtures.COND)
        self.decision = manager.Decision("UP", action="BUY", classification="SIDE_AVG_IMPROVEMENT",
                                        fill=manager.Fill(qty=5, notional=2, vwap=.4), stake=2, limit=.42)

    def tearDown(self):
        self.world.close()

    def test_missing_and_future_book_age_never_authorize(self):
        now = self.pm.mono()
        for stamp in (None, float("nan"), float("inf"), now + 1, now - 10):
            view = types.SimpleNamespace(status="LIVE", updated_mono=stamp)
            with patch.object(self.world.book, "view", return_value=view):
                self.assertFalse(self.pm._book_ok(pmfixtures.UP_T), stamp)

    def test_cancelled_executor_future_keeps_reservation(self):
        order = self.pm.reserve(self.decision)
        fut = concurrent.futures.Future()
        fut.cancel()
        self.pm._done(order, fut)
        self.assertEqual(order.status, "UNKNOWN")
        self.assertFalse(order.finished)
        self.assertTrue(self.pm.hist.sides["UP"].in_flight)
        self.assertEqual(self.pm.hist.spend, 2)

    def test_accepted_without_fill_is_unknown(self):
        order = self.pm.reserve(self.decision)
        self.pm.settle_order(order, ok=True, fill=None, error=None)
        self.assertEqual(order.status, "UNKNOWN")
        self.assertFalse(order.finished)
        self.assertEqual(self.pm.hist.spend, 2)

    def test_late_outcome_after_cancellation_is_booked_once(self):
        order = self.pm.reserve(self.decision)
        fut = concurrent.futures.Future()
        fut.cancel()
        self.pm._done(order, fut)
        outcome = {"order_id": "late", "shares": 5, "average_price": .4, "fee": .03, "total_cost": 2.03}
        for _ in range(2):
            self.pm.settle_order(order, ok=True, fill=outcome, error=None)
        self.assertEqual((order.status, self.pm.n["filled"]), ("FILLED", 1))
        self.assertAlmostEqual(self.pm.hist.spend, 2.03)

    def test_executor_submission_failure_releases_unsent_order(self):
        self.pm._snap_version = self.pm._state_version()
        self.pm._executor = types.SimpleNamespace(submit=lambda *args: (_ for _ in ()).throw(RuntimeError("closed")))
        loop = types.SimpleNamespace(call_later=lambda *args: types.SimpleNamespace(cancel=lambda: None),
                                     create_future=lambda: None)
        with self.assertRaises(RuntimeError):
            self.pm._dispatch(self.decision, loop)
        order = next(iter(self.pm.orders.values()))
        self.assertEqual(order.status, "CANCELLED")
        self.assertEqual(self.pm.hist.spend, 0)
        self.assertFalse(self.pm.hist.sides["UP"].in_flight)

    def test_cancelled_waiter_worker_finishes_and_shutdown_drains_before_journal_close(self):
        broker = pmfixtures.SlowBroker(self.world.led, shares=5, price=.4)
        self.pm.broker = broker
        closed_after = []

        async def exercise():
            loop = asyncio.get_running_loop()
            self.pm._snap_version = self.pm._state_version()
            order = self.pm._dispatch(self.decision, loop)
            for _ in range(100):
                if broker.calls:
                    break
                await asyncio.sleep(.001)
            self.assertEqual(broker.calls, 1)
            raw = next(iter(self.pm._workers))
            waiter = asyncio.wrap_future(raw)
            waiter.cancel()
            await asyncio.sleep(0)
            stopper = asyncio.create_task(self.pm.stop())
            await asyncio.sleep(.01)
            self.assertFalse(stopper.done())
            self.assertIsNone(self.pm._dispatch(self.decision, loop))
            self.assertEqual(closed_after, [])
            broker.release.set()
            await asyncio.wait_for(stopper, 2)
            self.assertEqual((order.status, order.finished), ("FILLED", True))
            self.assertFalse(self.pm.hist.sides["UP"].in_flight)
            self.assertEqual(self.pm.n["filled"], 1)
            self.assertEqual(self.pm.hist.spend, 2)
            self.assertFalse(self.pm._workers)

        original_close = self.pm.journal.close
        with patch.object(self.pm.journal, "close", side_effect=lambda: closed_after.append(self.pm.n["filled"])):
            asyncio.run(exercise())
        self.assertEqual(closed_after, [1])
        original_close()

    def test_post_commit_audit_failure_preserves_actual_fill(self):
        def place(*args, **kw):
            self.assertTrue(kw["pre_submit_guard"]())
            self.pm.broker.last_fill = {"order_id": "committed", "shares": 5, "average_price": .4,
                                        "fee": .03, "total_cost": 2.03, "tag": "manager",
                                        "condition_id": pmfixtures.COND, "token_id": pmfixtures.UP_T}
            raise RuntimeError("audit failed after durable ledger commit")

        self.pm.broker = types.SimpleNamespace(place_trade=place, last_fill=None)
        order = self.pm.reserve(self.decision)
        self.pm._execute_paper(self.decision, pmfixtures.COND, pmfixtures.UP_T, pmfixtures.DN_T,
                               self.world.hub.window_end, order)
        self.pm._finish(order)
        self.assertEqual((order.status, order.finished), ("FILLED", True))
        self.assertAlmostEqual(self.pm.hist.spend, 2.03)

    def test_priority_rechecked_at_booking_guard(self):
        sniper = pmfixtures.fake_sniper()
        self.pm.sniper = sniper
        seen = []

        def place(*args, **kw):
            sniper.inflight["DOWN"] = {"order": "sniper"}
            seen.append(kw["pre_submit_guard"]())
            return False

        self.pm.broker = types.SimpleNamespace(place_trade=place, last_fill=None, last_error="guard rejected")
        order = self.pm.reserve(self.decision)
        self.pm._execute_paper(self.decision, pmfixtures.COND, pmfixtures.UP_T, pmfixtures.DN_T,
                               self.world.hub.window_end, order)
        self.assertEqual(seen, [False])
        self.assertFalse(order.booking)

    def test_exception_after_booking_without_outcome_remains_unknown(self):
        def place(*args, **kw):
            self.assertTrue(kw["pre_submit_guard"]())
            raise RuntimeError("durability unknown")

        self.pm.broker = types.SimpleNamespace(place_trade=place, last_fill=None)
        order = self.pm.reserve(self.decision)
        self.pm._execute_paper(self.decision, pmfixtures.COND, pmfixtures.UP_T, pmfixtures.DN_T,
                               self.world.hub.window_end, order)
        self.pm._finish(order)
        self.assertEqual(order.status, "UNKNOWN")
        self.assertEqual(self.pm.hist.spend, 2)
        self.assertFalse(order.finished)


class LiveSubmissionAudit(unittest.TestCase):
    def test_collateral_decimal_overflow_is_unknown_not_infinite_balance(self):
        for value in ("1e400", "1e999999999999999999999", "Infinity", "NaN"):
            self.assertIsNone(live._pusd_amount(value), value)
            self.assertIsNone(live._balance_from_response({"balance": value, "allowance": "1000000"}), value)
        self.assertEqual(live._pusd_amount("1234567"), 1.234567)

    def test_uint256_token_validation_never_raises_on_bad_integer_strings(self):
        malformed = ("²", "١١١", "9" * 5000, str(1 << 256), 10 ** 5000, "0", "-1")
        for value in malformed:
            self.assertFalse(live._valid_token(value))
        with patch.object(live, "_live_disabled", False), patch.object(live, "_journal_fault", None), \
                patch.object(live, "_order_observer", lambda row: True), patch.object(live, "_get_client") as client:
            for value in malformed:
                self.assertFalse(live.place_trade("UP", 1, value, "222", "0x" + "ab" * 32, 1200))
                self.assertEqual(live.sell_shares(value, 1), 0)
                info = {"t": [{"o": "Up", "t": value}, {"o": "Down", "t": "222"}]}
                with self.assertRaises(RuntimeError):
                    live._validate_market_mapping(info, "fixture", "111", "222")
                with self.assertRaises(RuntimeError):
                    live._validate_market_mapping(info, "fixture", value, "222")
            client.assert_not_called()
        for value in ("111", str((1 << 256) - 1), "0" * 5000 + "111"):
            self.assertTrue(live._valid_token(value))

    def test_entry_numeric_overflow_is_refused_before_client_access(self):
        with patch.object(live, "_live_disabled", False), patch.object(live, "_journal_fault", None), \
                patch.object(live, "_order_observer", lambda row: True), patch.object(live, "_get_client") as client:
            self.assertFalse(live.place_trade("UP", 10 ** 1000, "111", "222", "0x" + "ab" * 32, 1200))
            client.assert_not_called()
        with self.assertRaises(RuntimeError):
            live._validate_round_end(10 ** 1000)

    def test_paper_firewall_waits_for_exit_that_already_owns_client(self):
        signing = threading.Event()
        release = threading.Event()
        disabled = threading.Event()
        results = []
        errors = []

        def sign(*args):
            signing.set()
            if not release.wait(2):
                raise RuntimeError("test signing release timeout")
            return "signed"

        client = types.SimpleNamespace(create_market_order=sign, post_order=unittest.mock.Mock(return_value={
            "success": True, "status": "matched", "orderID": "firewall-exit", "tradeIDs": ["firewall-trade"]}))

        def sell():
            try:
                results.append(live.sell_shares("111", 5))
            except BaseException as exc:
                errors.append(exc)

        def disable():
            try:
                live.disable_live_execution()
                disabled.set()
            except BaseException as exc:
                errors.append(exc)

        with patch.object(live, "_live_disabled", False), patch.object(live, "_client", client), \
                patch.object(live, "_ambiguous_exit_tokens", set()), redirect_stdout(io.StringIO()):
            seller = threading.Thread(target=sell)
            disabler = threading.Thread(target=disable)
            seller.start()
            try:
                self.assertTrue(signing.wait(1))
                disabler.start()
                self.assertFalse(disabled.wait(.03), "firewall returned while a live exit could still POST")
            finally:
                release.set()
                seller.join(2)
                if disabler.ident is not None:
                    disabler.join(2)
            self.assertFalse(seller.is_alive())
            self.assertFalse(disabler.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(disabled.is_set())
            self.assertEqual(results, [5])
            self.assertEqual(live.sell_shares("111", 5), 0)
            self.assertEqual(client.post_order.call_count, 1)

    def test_exit_numeric_overflow_is_refused_before_client_access(self):
        with patch.object(live, "_live_disabled", False), patch.object(live, "_get_client") as client:
            self.assertEqual(live.sell_shares("111", 10 ** 1000), 0)
            self.assertEqual(live.sell_shares("111", 1, min_price=10 ** 1000), 0)
            client.assert_not_called()

    def test_exit_timeout_never_allows_duplicate_post(self):
        client = types.SimpleNamespace(create_market_order=lambda *args: "signed",
                                       post_order=unittest.mock.Mock(side_effect=TimeoutError("uncertain POST; no match in diagnostic")))
        with patch.object(live, "_live_disabled", False), patch.object(live, "_get_client", return_value=client), \
                patch.object(live, "_ambiguous_exit_tokens", set()), redirect_stdout(io.StringIO()):
            self.assertEqual(live.sell_shares("111", 5), 0)
            self.assertEqual(live.sell_shares("111", 5), 0)
            self.assertEqual(client.post_order.call_count, 1)
            self.assertTrue(live._ambiguous_blocks("fixture", "111"))

    def test_exit_that_expires_during_signing_is_not_posted(self):
        client = types.SimpleNamespace(create_market_order=lambda *args: "signed", post_order=unittest.mock.Mock())
        with patch.object(live, "_live_disabled", False), patch.object(live, "_get_client", return_value=client), \
                patch.object(live, "_ambiguous_exit_tokens", set()), redirect_stdout(io.StringIO()), \
                patch.object(live, "_validate_round_end", side_effect=[1200, RuntimeError("expired during signing")]):
            self.assertEqual(live.sell_shares("111", 5, window_end=1200), 0)
            client.post_order.assert_not_called()
            self.assertNotIn("111", live._ambiguous_exit_tokens)

    def test_accepted_pending_exit_blocks_repeat_submission(self):
        client = types.SimpleNamespace(create_market_order=lambda *args: "signed", post_order=unittest.mock.Mock(
            return_value={"success": True, "orderID": "pending-exit", "status": "delayed"}))
        with patch.object(live, "_live_disabled", False), patch.object(live, "_get_client", return_value=client), \
                patch.object(live, "_ambiguous_exit_tokens", set()), redirect_stdout(io.StringIO()):
            self.assertEqual(live.sell_shares("111", 5), 5)
            self.assertEqual(live.sell_shares("111", 5), 0)
            self.assertEqual(client.post_order.call_count, 1)

    def test_client_initializes_once_across_concurrent_callers(self):
        created = []
        barrier = threading.Barrier(8)
        clients = []

        def construct(**kwargs):
            client = object()
            created.append(client)
            time.sleep(.02)
            return client

        def get():
            barrier.wait()
            clients.append(live._get_client())

        with patch.object(live, "_client", None), patch.object(live, "_live_disabled", False), \
                patch.object(live, "ClobClient", construct), patch.object(live, "_install_api_creds"), \
                patch.object(live, "_apply_clob_http_timeout"), patch.object(live, "_normalize_private_key", return_value="fixture"), \
                patch.object(live.config, "POLY_FUNDER", None), redirect_stdout(io.StringIO()):
            threads = [threading.Thread(target=get) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(2)
        self.assertEqual(len(created), 1)
        self.assertEqual(len(clients), 8)
        self.assertTrue(all(client is created[0] for client in clients))

    def test_nonfinite_or_fractional_round_end_rejected(self):
        for end in (float("nan"), float("inf"), 1200.5):
            with self.assertRaises(RuntimeError), patch.object(live.time, "time", return_value=1100):
                live._validate_round_end(end)

    def stub(self, replies, *, advance_balance=None):
        client = types.SimpleNamespace(
            get_clob_market_info=lambda cid: {"t": [{"o": "Up", "t": "111"}, {"o": "Down", "t": "222"}],
                                             "mos": "1", "mts": "0.01", "nr": False,
                                             "fd": {"r": "0.07", "e": 1, "to": True}, "itode": False},
            create_market_order=lambda *args, **kw: "signed")
        replies = iter(replies)
        client.post_order = unittest.mock.Mock(side_effect=lambda *args: next(replies))
        stack = ExitStack()
        for target, name, value in ((live, "_live_disabled", False), (live, "_journal_fault", None),
                                     (live, "_ambiguous_condition", None), (live, "_order_observer", lambda row: True)):
            stack.enter_context(patch.object(target, name, value))
        stack.enter_context(patch.object(live, "_get_client", return_value=client))
        stack.enter_context(patch.object(live, "_validate_round_end", return_value=1200))
        stack.enter_context(patch.object(live, "_read_balance", side_effect=advance_balance or
                                         (lambda c: {"balance": 100, "allowance": 100})))
        stack.enter_context(patch.object(live.config, "TICK_SIZE", None))
        stack.enter_context(patch.object(live.config, "NEG_RISK", None))
        stack.enter_context(patch.object(live.orderbook, "validate_buy_liquidity", return_value=(
            [{"price": "0.49", "size": "100"}], [{"price": "0.50", "size": "100"}])))
        stack.enter_context(redirect_stdout(io.StringIO()))
        return stack, client

    def test_explicit_no_fill_retry_has_no_fixed_half_second_delay(self):
        stack, client = self.stub([{"success": False, "errorMsg": "no match"},
                                   {"success": True, "status": "matched", "orderID": "fixture-order", "tradeIDs": ["fixture-trade"]}])
        with stack, patch.object(live.time, "sleep") as sleep:
            self.assertTrue(live.place_trade("UP", 2, "111", "222", "0x" + "ab" * 32, 1200))
            sleep.assert_not_called()
            self.assertEqual(client.post_order.call_count, 2)

    def test_slow_balance_cannot_post_using_stale_priced_book(self):
        clock = [10.0]

        def balance(client):
            clock[0] += 2
            return {"balance": 100, "allowance": 100}

        stack, client = self.stub([], advance_balance=balance)
        with stack, patch.object(live.time, "monotonic", side_effect=lambda: clock[0]), \
                patch.object(live.config, "ORDERBOOK_MAX_AGE_SECONDS", 1):
            self.assertFalse(live.place_trade("UP", 2, "111", "222", "0x" + "ab" * 32, 1200))
            client.post_order.assert_not_called()
            self.assertIn("became stale", live.last_order_error)

    def test_no_match_text_in_transport_error_is_never_retried(self):
        stack, client = self.stub([])
        client.post_order.side_effect = TimeoutError("timeout: no match in cached diagnostic")
        with stack, patch.object(live, "_ambiguous_tokens", set()):
            self.assertFalse(live.place_trade("UP", 2, "111", "222", "0x" + "ab" * 32, 1200))
            self.assertEqual(client.post_order.call_count, 1)
            self.assertIn("ambiguous submission", live.last_order_error)


if __name__ == "__main__":
    unittest.main(verbosity=2)
