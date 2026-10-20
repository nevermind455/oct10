"""Paper dispatch cash and fail-closed regressions; no SDK or network."""
from __future__ import annotations

import asyncio
import time
import unittest
from unittest.mock import patch

import testing_env  # noqa: F401
import tests_hybrid_strategy as H
import tests_sniper_ioc as I
import sniper_mode as S


class PaperDispatchAudit(unittest.TestCase):
    def setUp(self):
        self.fixture = H.HybridStrategy(methodName='test_real_cash_initializes_durable_equity_and_neutral_confidence')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def test_cap_price_fill_reserves_fees_before_the_next_side_decides(self):
        c = self.fixture
        c.enable_small_entry(cash=4.77)
        c.m.set(I.UP, [(.41, 1000)], [(.46, 1000)])

        async def run():
            loop = asyncio.get_running_loop()

            def match_inline(_executor, callback, *args):
                future = loop.create_future()
                try:
                    future.set_result(callback(*args))
                except Exception as exc:
                    future.set_exception(exc)
                return future

            now_ms = time.monotonic() * 1000
            c.sn.trigger.ref = (now_ms - 250, 60_000)
            with patch.object(loop, 'run_in_executor', side_effect=match_inline):
                await c.sn._fire('UP', 5, now_ms)
            self.assertEqual(c.sn.n['filled'], 1)
            self.assertAlmostEqual(c.broker.cash_balance(), 2.38306)
            self.assertAlmostEqual(c.sn._bankroll_now(), 2.309)
            self.assertLessEqual(c.sn._bankroll_now(), c.broker.cash_balance())
            c.m.set(I.DOWN, [(.41, 1000)], [(.46, 1000)])
            now_ms = time.monotonic() * 1000
            c.sn.trigger.ref = (now_ms - 250, 60_000)
            row = {'bp': -5, 'window_ms': 250}
            self.assertIsNone(c.sn._decide('DOWN', row, now_ms))
            self.assertIn('minimum', row['reason'])
            self.assertEqual(c.sn.n['orders'], 1)

        asyncio.run(run())

    def test_worker_accounting_error_blocks_further_orders(self):
        c = self.fixture
        c.refresh()

        async def run():
            plan, row = c.plan()
            self.assertIsNotNone(plan, row)
            future = asyncio.get_running_loop().create_future()
            future.set_exception(OSError('injected ledger fsync failure'))
            stamp = time.perf_counter_ns()
            await c.sn._finish('UP', 5, row, plan, future, stamp, stamp, stamp)
            self.assertTrue(c.sn._closing)
            self.assertEqual(c.sn.n['errors'], 1)
            self.assertIn('fsync failure', c.rows[-1]['reason'])
            self.assertIsNone(c.sn._fire_now('DOWN', -5, time.monotonic() * 1000))

        asyncio.run(run())

    def test_interrupted_worker_wait_blocks_further_orders(self):
        c = self.fixture
        c.refresh()

        async def run():
            plan, row = c.plan()
            future = asyncio.get_running_loop().create_future()
            future.cancel()
            stamp = time.perf_counter_ns()
            with self.assertRaises(asyncio.CancelledError):
                await c.sn._finish('UP', 5, row, plan, future, stamp, stamp, stamp)
            self.assertTrue(c.sn._closing)

        asyncio.run(run())

    def test_telemetry_error_after_worker_completed_does_not_halt_trading(self):
        c = self.fixture
        c.refresh()

        async def run():
            plan, row = c.plan()
            plan['result'] = {'fill': None, 'error': 'FAK no-fill'}
            future = asyncio.get_running_loop().create_future()
            future.set_result((False, None, None, {}))
            stamp = time.perf_counter_ns()
            with patch.object(c.sn, '_stages', side_effect=RuntimeError('telemetry failed')):
                await c.sn._finish('UP', 5, row, plan, future, stamp, stamp, stamp)
            self.assertFalse(c.sn._closing)
            self.assertEqual(c.sn.n['errors'], 1)

        asyncio.run(run())

    def test_pending_worker_cash_does_not_expire_after_two_seconds(self):
        sn = self.fixture.sn
        now = time.monotonic()
        sn._set_bankroll(20, now)
        plan = {}
        sn._bankroll_spent(6, plan=plan)
        sn._set_bankroll(20, now + sn.BANKROLL_SETTLE_S + 3)
        self.assertEqual(sn._bankroll, 14)
        self.assertEqual(len(sn._spends), 1)
        self.assertEqual(len(sn._paper_pending_spends), 1)

    def test_completion_keeps_cash_reserved_for_an_overlapping_cash_read(self):
        sn = self.fixture.sn
        now = time.monotonic()
        sn._set_bankroll(20, now)
        plan = {}
        with patch.object(S.time, 'monotonic', return_value=now):
            sn._bankroll_spent(6, plan=plan)
        with patch.object(S.time, 'monotonic', return_value=now + 8):
            sn._bankroll_completed(plan)
        self.assertFalse(sn._paper_pending_spends)
        # This cash snapshot began before the worker completed.
        sn._set_bankroll(20, now + 7)
        self.assertEqual(sn._bankroll, 14)
        # A later snapshot can safely include the outcome (a no-fill here).
        sn._set_bankroll(20, now + 11)
        self.assertEqual(sn._bankroll, 20)
        self.assertEqual(sn._spends, [])

    def test_executor_submission_failure_stops_new_orders_and_retains_cash(self):
        c = self.fixture
        c.enable_small_entry(cash=20)

        async def run():
            loop = asyncio.get_running_loop()
            now_ms = time.monotonic() * 1000
            c.sn.trigger.ref = (now_ms - 250, 60_000)
            with patch.object(loop, 'run_in_executor', side_effect=RuntimeError('executor shut down')):
                self.assertIsNone(c.sn._fire_now('UP', 5, now_ms))
            self.assertTrue(c.sn._closing)
            self.assertEqual(len(c.sn._paper_pending_spends), 1)
            self.assertLess(c.sn._bankroll_now(), 20)
            self.assertEqual(len(c.sn._minimum_entry_keys), 1)
            self.assertIsNone(c.sn._fire_now('DOWN', -5, now_ms))

        asyncio.run(run())


if __name__ == '__main__':
    unittest.main()
