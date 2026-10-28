#!/usr/bin/env python3
"""SNIPER_PAPER_CASH_FROM - paper sizes from the LIVE bot's cash and drawdown
(2026-10-29, the operator's request).

    python tests/tests_paper_cash_mirror.py

paper50 had about $354 of paper cash while live had about $53, so paper sent
78-share orders where live sent 8 - nothing it did could be compared with
live. With SNIPER_PAPER_CASH_FROM=<the live journal>:
  1  the live journal's END is read (never the whole file): the newest Kelly
     cash (balance_read's kelly_cash, or a decision's bankroll) and the newest
     hybrid equity / high-water (a drawdown reset starts afresh)
  2  Kelly sizes from live's cash - paper's own if smaller, since it pays
  3  the hybrid drawdown scale is live's, not paper's
  4  no live reading for SNIPER_PAPER_CASH_MAX_AGE_S (live stopped?): no size
  5  an unreadable journal stops the start; live refuses the setting
The real Sniper and paper broker (tests_sniper_fair.Gate). No network.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402


def journal(path, rows, tail=b""):
    with open(path, "wb") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")).encode() + b"\n")
        fh.write(tail)


class Tail(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.p = os.path.join(self.tmp.name, "live_sniper.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def test_the_newest_cash_and_drawdown_across_chunk_boundaries(self):
        filler = [{"wall": 1.0 + i, "result": "skipped", "reason": "ask not below fair value", "pad": "x" * 40}
                  for i in range(200)]
        rows = ([{"wall": 300, "result": "balance_read", "kelly_cash": 40.0, "wallet": 41.0}] + filler[:50]
                + [{"wall": 400, "result": "filled", "bankroll": 38.5, "hybrid": {"equity": 45.0, "high_water": 60.0}}]
                + filler[50:])
        journal(self.p, rows, tail=b'{"wall":999,"result":"balance_read","kelly_ca')     # being written
        for chunk in (64, 1000, 262_144):
            m = S.live_account_tail(self.p, chunk=chunk)
            self.assertEqual((m["cash"], m["cash_wall"], m["equity"], m["high_water"], m["hybrid_wall"]),
                             (38.5, 400.0, 45.0, 60.0, 400.0), chunk)

    def test_a_newer_balance_read_and_a_drawdown_reset_win(self):
        journal(self.p, [{"wall": 400, "result": "skipped", "bankroll": 38.5,
                          "hybrid": {"equity": 45.0, "high_water": 60.0}},
                         {"wall": 500, "result": "hybrid_drawdown_reset", "old_high_water": 60.0, "equity": 44.0},
                         {"wall": 600, "result": "balance_read", "kelly_cash": 44.25}])
        m = S.live_account_tail(self.p, chunk=50)
        self.assertEqual((m["cash"], m["cash_wall"], m["equity"], m["high_water"]), (44.25, 600.0, 44.0, 44.0))

    def test_nothing_found_and_the_byte_limit(self):
        journal(self.p, [{"wall": 1, "result": "gc"}] * 50)
        self.assertEqual(S.live_account_tail(self.p)["cash"], None)
        journal(self.p, [{"wall": 1, "result": "balance_read", "kelly_cash": 9.0}] + [{"wall": 2, "result": "gc"}] * 500)
        self.assertIsNone(S.live_account_tail(self.p, max_bytes=1000, chunk=500)["cash"])   # older than the limit
        self.assertEqual(S.live_account_tail(self.p)["cash"], 9.0)


class Paper(F.Gate):
    """Share sizing, as live and paper50 run it."""

    def setUp(self):
        self.ltmp = tempfile.TemporaryDirectory()
        self.lj = os.path.join(self.ltmp.name, "live_sniper.jsonl")
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.ltmp.cleanup()

    def live_says(self, cash, age_s=5.0, equity=None, high_water=None):
        row = {"wall": time.time() - age_s, "result": "balance_read", "kelly_cash": cash}
        rows = [row] + ([{"wall": time.time() - age_s, "result": "skipped",
                          "hybrid": {"equity": equity, "high_water": high_water}}] if equity is not None else [])
        journal(self.lj, rows)

    def signal(self, **env):
        self.make(SNIPER_ORDER_SIZING="shares", **env)
        self.pin_time_left()
        return self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)

    def test_kelly_sizes_from_lives_cash(self):
        self.live_says(20.0)
        r = self.signal(SNIPER_PAPER_CASH_FROM=self.lj)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual((r["bankroll"], r["live_cash"]), (20.0, 20.0))
        self.assertIn("sized from LIVE's cash and drawdown", self.sn.s.fair_text())
        own = self.signal()                                             # the same signal, paper's own cash
        self.assertGreater(own["bankroll"], 500)                       # paper's own $1000
        self.assertNotIn("live_cash", own)
        self.assertLess(r["kelly_shares"], own["kelly_shares"])

    def test_paper_never_sizes_above_its_own_cash(self):
        self.live_says(5_000.0)
        r = self.signal(SNIPER_PAPER_CASH_FROM=self.lj)
        self.assertLess(r["bankroll"], 1_001)
        self.assertEqual(r["live_cash"], 5000.0)

    def test_an_old_live_reading_sizes_nothing(self):
        self.live_says(20.0, age_s=2_000)
        r = self.signal(SNIPER_PAPER_CASH_FROM=self.lj)
        self.assertEqual((r["result"], r["reason"]), ("skipped", "kelly size unknown: no live cash reading"))
        self.assertIn("is live running", r["detail"])
        self.live_says(20.0, age_s=2_000)
        r = self.signal(SNIPER_PAPER_CASH_FROM=self.lj, SNIPER_PAPER_CASH_MAX_AGE_S="3000")
        self.assertEqual(r["result"], "filled", r)

    def test_the_hybrid_drawdown_is_lives(self):
        self.live_says(40.0, equity=40.0, high_water=50.0)
        self.make(SNIPER_PAPER_CASH_FROM=self.lj, SNIPER_HYBRID_SIZING="1")
        self.sn._hybrid_risk = {"equity": 500.0, "high_water": 500.0, "observed_mono": time.monotonic()}
        h = self.sn._hybrid_factors()
        self.assertEqual((h["equity"], h["high_water"], h["live_mirror"], h["paper_equity"]), (40.0, 50.0, True, 500.0))
        self.make(SNIPER_HYBRID_SIZING="1")
        self.sn._hybrid_risk = {"equity": 500.0, "high_water": 500.0, "observed_mono": time.monotonic()}
        self.assertEqual(self.sn._hybrid_factors()["equity"], 500.0)                  # off: paper's own
        self.live_says(40.0)                                                           # no hybrid row
        self.make(SNIPER_PAPER_CASH_FROM=self.lj, SNIPER_HYBRID_SIZING="1")
        self.sn._hybrid_risk = {"equity": 500.0, "high_water": 500.0, "observed_mono": time.monotonic()}
        with self.assertRaises(ValueError):
            self.sn._hybrid_factors()

    def test_an_unreadable_journal_stops_the_start(self):
        with self.assertRaises(ValueError) as cm:
            self.make(SNIPER_PAPER_CASH_FROM=os.path.join(self.ltmp.name, "missing.jsonl"))
        self.assertIn("SNIPER_PAPER_CASH_FROM", str(cm.exception))


# run only this file's tests, not every inherited tests_sniper_fair.Gate test again
for _name in [n for n in dir(F.Gate) if n.startswith("test") and n not in Paper.__dict__]:
    setattr(Paper, _name, None)


class LiveRefuses(unittest.TestCase):
    def test_live_refuses_it(self):
        bad = X.spec_violations(I.settings(SNIPER_PAPER_CASH_FROM="/opt/sniperbtc/live_sniper.jsonl", SNIPER_FAIR="1"))
        self.assertTrue(any("paper cash mirror" in v for v in bad), bad)
        self.assertFalse(any("paper cash mirror" in v for v in X.spec_violations(I.settings(SNIPER_FAIR="1"))))

    def test_bad_age_stops_the_start(self):
        for v in ("0", "-5", "abc"):
            with self.assertRaises(ValueError, msg=v):
                I.settings(SNIPER_PAPER_CASH_MAX_AGE_S=v)


if __name__ == "__main__":
    unittest.main(verbosity=1)
