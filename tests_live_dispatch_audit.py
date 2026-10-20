"""No-network regressions for unsent preparation and uncertain dispatch failures."""
import time
import types
import unittest
from unittest.mock import Mock

import testing_env  # noqa: F401
import tests_hybrid_strategy as H
import sniper_live as L


class LiveDispatchAudit(unittest.TestCase):
    def setUp(self):
        self.fixture = H.HybridStrategy()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.fixture.enable_small_entry()
        self.sn = self.fixture.sn
        self.plan, self.row = self.fixture.plan()
        self.assertIsNotNone(self.plan, self.row)
        self.assertTrue(self.plan['minimum_entry'])
        self.key = self.plan['minimum_entry_key']
        self.now = time.perf_counter_ns()

    def fire_live(self):
        return self.sn._fire_live('UP', 5, self.row, self.plan, self.now, self.now)

    def test_unexpected_prepare_failure_releases_unused_claim_and_stops(self):
        self.sn.live = types.SimpleNamespace(prepare=Mock(side_effect=RuntimeError('builder failed')))
        with self.assertRaisesRegex(RuntimeError, 'builder failed'):
            self.fire_live()
        self.assertNotIn(self.key, self.sn._minimum_entry_keys)
        self.assertEqual(self.sn.n['orders'], 0)
        self.assertTrue(self.sn._closing)
        self.assertIsNone(self.sn._fire_now('UP', 5, time.monotonic() * 1000))

    def test_dispatch_failure_retains_claim_and_halts_for_reconciliation(self):
        prepared = types.SimpleNamespace(send_guard=None)
        self.sn.live = types.SimpleNamespace(
            prepare=Mock(return_value=prepared),
            start_send=Mock(side_effect=OSError('write outcome unknown')),
            kill=types.SimpleNamespace(trip=Mock()))
        with self.assertRaisesRegex(OSError, 'write outcome unknown'):
            self.fire_live()
        self.assertIn(self.key, self.sn._minimum_entry_keys)
        self.assertEqual(self.sn.n['orders'], 1)
        self.assertTrue(self.sn._closing)
        self.sn.live.kill.trip.assert_called_once_with('order dispatch failed: reconcile before trading')

    def test_expected_prepare_refusal_remains_recoverable_and_unsent(self):
        self.sn.live = types.SimpleNamespace(prepare=Mock(side_effect=L.Refused('book became stale')))
        self.assertIsNone(self.fire_live())
        self.assertFalse(self.sn._closing)
        self.assertNotIn(self.key, self.sn._minimum_entry_keys)
        self.assertEqual(self.sn.n['orders'], 0)
        self.assertEqual(self.row['reason'], 'live refused')


if __name__ == '__main__':
    unittest.main(verbosity=2)
