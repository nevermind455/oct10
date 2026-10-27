"""Runtime and decision-age regressions; offline and credential-free."""
import testing_env  # noqa: F401
import asyncio
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

import run
import sniper_mode as S
from sniper_live import Refused


def sniper():
    with patch.dict('os.environ', {'SNIPER_LATENCY_MODE': 'fixed', 'SNIPER_FAIR': '0'}):
        settings = S.Settings()
    hub = types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_end=None, book=None)
    sn = S.Sniper(hub, types.SimpleNamespace(), settings=settings, journal_path=None)
    sn.on_event = Mock()
    return sn


class RuntimeFailures(unittest.IsolatedAsyncioTestCase):
    async def test_dead_accounting_service_stops_runtime_immediately(self):
        async def die():
            await asyncio.sleep(0.01)
            raise ValueError('ledger worker failed')
        active = asyncio.create_task(asyncio.Event().wait(), name='sniper')
        failed = asyncio.create_task(die(), name='ledger')
        rt = types.SimpleNamespace(stop=threading.Event(), sniper_task=active, tasks=[active, failed])
        started = time.perf_counter()
        try:
            exc = await asyncio.wait_for(run._wait_until_stopped(rt), 0.2)
            self.assertIsInstance(exc, ValueError)
            self.assertLess(time.perf_counter() - started, 0.15)
        finally:
            active.cancel()
            await asyncio.gather(active, failed, return_exceptions=True)

    async def test_order_task_failure_is_observed_and_blocks_dispatch(self):
        sn = sniper()
        async def die():
            raise RuntimeError('unexpected order failure')
        task = sn._spawn(die(), order=True)
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)
        self.assertEqual(sn.n['task errors'], 1)
        self.assertTrue(sn._closing)
        sn.on_event.assert_called()


class DispatchAge(unittest.TestCase):
    def test_shutdown_signal_never_decides_or_claims_a_side(self):
        sn = sniper()
        sn._closing = True
        sn.trigger.update(0, 60000)
        sn._fire_now = Mock()
        sn._signal(sn.s.primary_feed, sn.trigger, 60010, 260, 1)
        sn._fire_now.assert_not_called()
        self.assertFalse(sn.busy)

    def test_shutdown_direct_fire_never_decides(self):
        sn = sniper()
        sn._closing = True
        sn._decide = Mock()
        self.assertIsNone(sn._fire_now('UP', 1, 10))
        sn._decide.assert_not_called()

    def test_original_book_cannot_be_refreshed_by_a_newer_snapshot(self):
        sn = sniper()
        sn.s.stale_ms = 2000
        plan = {'view': types.SimpleNamespace(updated_mono=7.9), 't_ms': 10000,
                'dispatch_deadline': time.time() + 120}
        with patch.object(S.time, 'monotonic', return_value=10):
            with self.assertRaisesRegex(Refused, 'decision book'):
                sn._check_dispatch_age(plan, None, None)

    def test_fresh_followup_ticks_cannot_revive_old_signal(self):
        sn = sniper()
        sn.cb_fresh.enabled = True
        sn.cb_fresh.connected = True
        sn.cb_fresh.last_trade_mono = 10
        sn.cb_fresh.last_excess = 0
        plan = {'view': types.SimpleNamespace(updated_mono=10), 't_ms': 8900,
                'dispatch_deadline': time.time() + 120}
        with patch.object(S.time, 'monotonic', return_value=10):
            with self.assertRaisesRegex(Refused, 'signal became stale'):
                sn._check_dispatch_age(plan, sn.cb_fresh, 0)

    def test_original_excess_counts_against_existing_feed_age_budget(self):
        sn = sniper()
        sn.cb_fresh.enabled = sn.cb_fresh.connected = True
        sn.cb_fresh.last_trade_mono = 10
        sn.cb_fresh.last_excess = 0
        plan = {'view': types.SimpleNamespace(updated_mono=10), 't_ms': 9900,
                'dispatch_deadline': time.time() + 120}
        with patch.object(S.time, 'monotonic', return_value=10):
            with self.assertRaises(Refused):
                sn._check_dispatch_age(plan, sn.cb_fresh, 950)

    def test_fresh_plan_passes_and_disabled_feed_gate_stays_disabled(self):
        sn = sniper()
        plan = {'view': types.SimpleNamespace(updated_mono=9.9), 't_ms': 9000,
                'dispatch_deadline': time.time() + 120}
        with patch.object(S.time, 'monotonic', return_value=10):
            sn._check_dispatch_age(plan, sn.cb_fresh, None)


if __name__ == '__main__':
    unittest.main()
