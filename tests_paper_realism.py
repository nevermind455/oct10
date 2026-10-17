#!/usr/bin/env python3
"""SNIPER_PAPER_EXTRA_MS: optional paper arrival-delay sensitivity.
An extra delay is an assumption; matching aggregate fill rates is not proof
of a measured feed lag or correct per-order fills. See EXECUTION_AUDIT.md.
Paper only. No network.

    python tests_paper_realism.py
"""
from __future__ import annotations

import os
import pathlib
import random
import sys
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S  # noqa: E402


def settings(**env):
    base = {"SNIPER_LATENCY_MS": ""}                     # the live-like / live-sampled paths, not fixed
    with mock.patch.dict(os.environ, {**base, **env}):
        return S.Settings()


def plan(s, *, delays=(), live=None, rtt=()):
    sn = types.SimpleNamespace(s=s, _live_delays=list(delays), _rng=random.Random(1), live=live,
                               net_rtt=list(rtt))
    return S.Sniper.latency_plan(sn)


class PaperExtraDelay(unittest.TestCase):
    def test_off_by_default_and_unchanged(self):
        s = settings()
        self.assertEqual(s.paper_extra_ms, 0.0)
        lat, parts = plan(s, delays=[300.0])
        self.assertEqual(lat, 300.0)
        self.assertNotIn("extra_ms", parts)
        self.assertNotIn("calibration", " ".join(s.lines()))

    def test_a_live_sampled_delay_gets_the_calibration(self):
        lat, parts = plan(settings(SNIPER_PAPER_EXTRA_MS="200"), delays=[300.0])
        self.assertEqual(lat, 500.0)
        self.assertEqual((parts["delay_ms"], parts["extra_ms"]), (300.0, 200.0))   # both journalled

    def test_the_live_like_model_gets_it_too(self):
        base, _ = plan(settings(), rtt=[20.0])
        lat, parts = plan(settings(SNIPER_PAPER_EXTRA_MS="200"), rtt=[20.0])
        self.assertAlmostEqual(lat, base + 200.0, places=9)
        self.assertEqual(parts["extra_ms"], 200.0)

    def test_a_live_sniper_never_adds_it(self):
        lat, parts = plan(settings(SNIPER_PAPER_EXTRA_MS="200"), delays=[300.0], live=object())
        self.assertEqual(lat, 300.0)
        self.assertNotIn("extra_ms", parts)

    def test_a_fixed_latency_stays_fixed(self):
        s = settings(SNIPER_PAPER_EXTRA_MS="200", SNIPER_LATENCY_MS="20")
        self.assertEqual(plan(s, delays=[300.0]), (20.0, {"latency_mode": "fixed"}))

    def test_invalid_values_refuse_to_start(self):
        for bad in ("-1", "nan", "5001"):
            with self.assertRaises(ValueError, msg=bad):
                settings(SNIPER_PAPER_EXTRA_MS=bad)

    def test_the_paper_line_says_so(self):
        self.assertIn("+ 200ms calibration", " ".join(settings(SNIPER_PAPER_EXTRA_MS="200").lines()))


if __name__ == "__main__":
    unittest.main()
