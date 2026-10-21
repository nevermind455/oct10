#!/usr/bin/env python3
"""SNIPER_HYBRID_DRAWDOWN_PAUSE_S (2026-10-22, the operator's choice): at the
hybrid drawdown stop, no orders for the pause (default 600s), then a new hybrid
epoch from the current equity - capital = equity, P&L from now, peak = equity -
so full sizing resumes. Live sat at 23.6% drawdown (equity $40.04, peak $52.44)
from 10-21 05:23 with no way back. 0 keeps the old stop. No network.

    python tests_hybrid_drawdown_pause.py
"""
from __future__ import annotations

import collections
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - never load credentials or external services
import sniper_mode as S  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from hybrid_sizing import HybridPolicy, at_drawdown_stop  # noqa: E402


class StopBoundary(unittest.TestCase):
    def test_the_stop_is_sizing_factors_own_exact_test(self):
        p = HybridPolicy(drawdown_stop=0.20)
        self.assertTrue(at_drawdown_stop(p, equity=98.76, high_water=123.45))     # exactly 20%
        self.assertFalse(at_drawdown_stop(p, equity=98.77, high_water=123.45))
        self.assertTrue(at_drawdown_stop(p, equity=40.04, high_water=52.44))      # the live case, 23.6%


class LedgerRebase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = pathlib.Path(self.temp.name) / "ledger.json"
        self.ledger = Ledger(str(self.path), allow_sells=True)

    def tearDown(self):
        self.temp.cleanup()

    def lose(self, token, shares=10, price=0.5):
        self.assertTrue(self.ledger.record_fill_durable(
            "buy-" + token, token, shares=shares, price=price, side="BUY",
            condition_id="condition-" + token, status="CONFIRMED", fee=0.0))
        self.ledger.settle(types.SimpleNamespace(resolved=True, condition_id="condition-" + token,
                                                 payouts={token: 0.0, "opposite-" + token: 1.0}))

    def test_a_reset_starts_a_new_epoch_from_the_equity_and_survives_a_restart(self):
        self.ledger.hybrid_risk_view(100)
        self.lose("a", shares=50, price=0.5)                                       # -$25: 25% down
        view = self.ledger.hybrid_risk_view()
        self.assertAlmostEqual(view["equity"], 75.0)
        self.assertEqual(view["high_water"], 100)
        fresh = self.ledger.hybrid_risk_view(None, rebase=True)
        self.assertAlmostEqual(fresh["equity"], 75.0)
        self.assertAlmostEqual(fresh["high_water"], 75.0)                          # drawdown 0
        self.assertAlmostEqual(fresh["reference_equity"], 75.0)
        again = Ledger(str(self.path), allow_sells=True).hybrid_risk_view()        # durable
        self.assertAlmostEqual(again["high_water"], 75.0)
        self.assertAlmostEqual(again["equity"], 75.0)
        self.lose("b", shares=10, price=0.5)                                       # later losses count from 75
        self.assertAlmostEqual(self.ledger.hybrid_risk_view()["equity"], 70.0)

    def test_no_reset_without_an_epoch_or_with_a_new_reference(self):
        with self.assertRaises(ValueError):
            self.ledger.hybrid_risk_view(None, rebase=True)
        self.ledger.hybrid_risk_view(100)
        with self.assertRaises(ValueError):
            self.ledger.hybrid_risk_view(100, rebase=True)


def sniper(pause=600.0):
    return types.SimpleNamespace(
        s=types.SimpleNamespace(hybrid_dd_pause_s=pause, hybrid_policy=HybridPolicy(drawdown_stop=0.20)),
        _dd_stop_since=None, n=collections.Counter(), rows=[], events=[],
        _write=None, on_event=None)


class SniperPause(unittest.TestCase):
    STOPPED = {"equity": 40.04, "high_water": 52.44}
    FRESH = {"equity": 40.04, "high_water": 40.04}

    def setUp(self):
        self.sn = sniper()
        self.sn._write = self.sn.rows.append
        self.sn.on_event = lambda *a: self.sn.events.append(a)
        self.calls = []

    def read(self, reference, **kw):
        self.calls.append(kw)
        return dict(self.FRESH)

    def step(self, at, snap=None):
        with mock.patch.object(S.time, "monotonic", return_value=at):
            return S.Sniper._hybrid_drawdown_pause(self.sn, self.read, dict(snap or self.STOPPED))

    def test_pause_then_a_fresh_start(self):
        self.assertEqual(self.step(1000.0)["high_water"], 52.44)                   # the stop: no orders
        self.assertEqual(self.sn.rows[-1]["result"], "hybrid_drawdown_pause")
        self.assertEqual(self.step(1000.0 + 599)["high_water"], 52.44)             # still inside the 10 min
        self.assertEqual(self.calls, [])
        out = self.step(1000.0 + 600)
        self.assertEqual(out["high_water"], 40.04)                                 # fresh start
        self.assertEqual(self.calls, [{"rebase": True}])
        self.assertEqual(self.sn.rows[-1]["result"], "hybrid_drawdown_reset")
        self.assertIsNone(self.sn._dd_stop_since)
        self.assertEqual(self.sn.n["hybrid drawdown resets"], 1)

    def test_a_recovery_before_the_pause_ends_cancels_it(self):
        self.step(1000.0)
        self.step(1100.0, {"equity": 45.0, "high_water": 52.44})                   # 14% down: trading again
        self.assertIsNone(self.sn._dd_stop_since)
        self.step(2000.0)                                                         # a new stop starts a new pause
        self.assertEqual(self.calls, [])

    def test_zero_keeps_the_old_stop(self):
        self.sn.s.hybrid_dd_pause_s = 0.0
        for at in (1000.0, 5000.0, 90000.0):
            self.assertEqual(self.step(at)["high_water"], 52.44)
        self.assertEqual((self.calls, self.sn.rows), ([], []))

    def test_a_failed_reset_keeps_the_stop_and_retries(self):
        def broken(reference, **kw):
            raise ValueError("equity not positive")
        self.step(1000.0)
        with mock.patch.object(S.time, "monotonic", return_value=1700.0):
            out = S.Sniper._hybrid_drawdown_pause(self.sn, broken, dict(self.STOPPED))
        self.assertEqual(out["high_water"], 52.44)
        self.assertEqual(self.sn.n["hybrid drawdown reset errors"], 1)
        self.assertEqual(self.step(1705.0)["high_water"], 40.04)                   # next refresh resets


class Setting(unittest.TestCase):
    def test_default_and_limits(self):
        with mock.patch.dict(os.environ, {"SNIPER_HYBRID_DRAWDOWN_PAUSE_S": ""}):
            self.assertEqual(S.Settings().hybrid_dd_pause_s, 600.0)
        for bad in ("-1", "nan", "90000"):
            with mock.patch.dict(os.environ, {"SNIPER_HYBRID_DRAWDOWN_PAUSE_S": bad}), self.assertRaises(ValueError):
                S.Settings()


if __name__ == "__main__":
    unittest.main()
