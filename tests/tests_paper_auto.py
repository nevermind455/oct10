#!/usr/bin/env python3
"""Paper as real as live, by default (2026-10-29, the operator's request).

    python tests/tests_paper_auto.py

run.py --paper in the live bot's folder, with nothing set:
  1  SNIPER_DELAY_SOURCE=auto: each order's delay is drawn from the live
     journal's last SNIPER_DELAY_DAYS (3) days when they hold >= 30 orders,
     else the model - and the settings line says which; no live journal there:
     the model, as always
  2  SNIPER_PAPER_CASH_FROM unset (auto): live's cash and drawdown while live
     runs (a reading <= SNIPER_PAPER_CASH_MAX_AGE_S old), else paper's own -
     never a skip for it
  3  explicit settings still win: a path is strict, off / model the old paper
  4  live is unaffected: only an explicit path is refused
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
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402

NOW = time.time()


def journal(path, rows):
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
    return path


def order(wall, reply_ms):
    return {"wall": wall, "live": True, "kind": "NO_MATCH", "result": "refused", "t_wire_to_reply_ms": reply_ms}


def summary(wall, rtt):
    return {"wall": wall, "result": "summary", "live": {"clock": {"rtt_ms_p50": rtt}}}


class Window(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_only_the_newest_days_and_undated_rows(self):
        old, new = NOW - 5 * 86400, NOW - 3600
        p = journal(os.path.join(self.tmp.name, "j.jsonl"),
                    [summary(old, 100.0)] + [order(old, 600.0)] * 10
                    + [summary(new, 24.0)] + [order(new, 212.0)] * 4
                    + [{"live": True, "kind": "MATCHED", "result": "filled", "t_wire_to_reply_ms": 112.0}])
        self.assertEqual(S.live_delays(p, days=3), ([200.0] * 4 + [100.0], 12.0))      # an undated row stays
        everything, one_way = S.live_delays(p)
        self.assertEqual((len(everything), one_way), (15, 31.0))                     # the old way: all of it


class Delays(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.p = os.path.join(self.tmp.name, "live_sniper.jsonl")

    def tearDown(self):
        self.tmp.cleanup()

    def sniper(self, **env):
        with patch.dict(os.environ, {"SNIPER_DELAY_SOURCE": "", "SNIPER_DELAY_JOURNAL": self.p,
                                     "SNIPER_LATENCY_MS": "", "SNIPER_PAPER_EXTRA_MS": "0", **env}):
            return S.Sniper(None, None, settings=S.Settings())

    def test_auto_draws_the_last_three_days_of_live_orders(self):
        journal(self.p, [summary(NOW - 60, 24.0)] + [order(NOW - 9 * 86400, 2000.0)] * 50
                + [order(NOW - 60, 112.0 + i) for i in range(40)])
        sn = self.sniper()
        self.assertEqual(sn.s.delay_source, "auto")
        self.assertEqual(len(sn._live_delays), 40)                                   # not the 50 old slow ones
        self.assertEqual(sn.latency_plan()[1]["latency_mode"], "live-sampled")
        self.assertIn("drawn from 40 live orders", sn.lines()[0])
        self.assertIn("last 3 days", sn.lines()[0])

    def test_too_few_recent_orders_is_the_model_and_says_so(self):
        journal(self.p, [summary(NOW - 60, 24.0)] + [order(NOW - 60, 212.0)] * 5)
        sn = self.sniper()                                                           # no refusal: auto
        self.assertEqual(sn._live_delays, [])
        self.assertEqual(sn.latency_plan()[1]["latency_mode"], "live-like")
        self.assertIn("only 5 live orders in the last 3 days", sn.lines()[0])

    def test_no_live_journal_is_the_model_as_always(self):
        sn = self.sniper()
        self.assertEqual((sn._live_delays, sn.s.delay_note), ([], ""))

    def test_explicit_settings_still_win(self):
        journal(self.p, [summary(NOW - 60, 24.0)] + [order(NOW - 60, 212.0)] * 5)
        with self.assertRaises(ValueError):                                          # live: strict, as before
            self.sniper(SNIPER_DELAY_SOURCE="live")
        self.assertEqual(self.sniper(SNIPER_DELAY_SOURCE="model")._live_delays, [])
        self.assertEqual(self.sniper(SNIPER_LATENCY_MS="200")._live_delays, [])        # a fixed delay: no auto


class Cash(F.Gate):
    """Share sizing, as live runs it."""

    def setUp(self):
        self.ltmp = tempfile.TemporaryDirectory()
        self.lj = os.path.join(self.ltmp.name, "live_sniper.jsonl")
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.ltmp.cleanup()

    def live_says(self, cash, age_s=5.0, equity=None, high_water=None):
        rows = [{"wall": time.time() - age_s, "result": "balance_read", "kelly_cash": cash}]
        if equity is not None:
            rows.append({"wall": time.time() - age_s, "result": "skipped",
                         "hybrid": {"equity": equity, "high_water": high_water}})
        journal(self.lj, rows)

    def signal(self, **env):
        self.make(**{"SNIPER_ORDER_SIZING": "shares", "SNIPER_PAPER_CASH_FROM": "", "LIVE_SNIPER_JOURNAL": self.lj,
                     **env})
        self.pin_time_left()
        return self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)

    def test_while_live_runs_paper_sizes_from_its_cash(self):
        self.live_says(20.0)
        r = self.signal()
        self.assertEqual((r["result"], r["bankroll"], r["live_cash"], r["cash_source"]), ("filled", 20.0, 20.0, "live"))
        self.assertIn("sized from LIVE's cash and drawdown while live runs", self.sn.lines()[0])

    def test_with_live_stopped_paper_uses_its_own_cash_and_still_trades(self):
        self.live_says(20.0, age_s=2_000)
        r = self.signal()
        self.assertEqual(r["result"], "filled", r)
        self.assertGreater(r["bankroll"], 500)                                       # paper's own $1000
        self.assertEqual(r["cash_source"], "paper (no fresh live reading)")
        self.assertNotIn("live_cash", r)

    def test_no_live_journal_is_the_old_paper(self):
        r = self.signal()                                                            # self.lj never written
        self.assertGreater(r["bankroll"], 500)
        self.assertNotIn("cash_source", r)
        self.assertNotIn("sized from LIVE", self.sn.lines()[0])

    def test_the_drawdown_is_lives_while_it_runs_else_papers(self):
        self.live_says(40.0, equity=40.0, high_water=50.0)
        self.make(SNIPER_PAPER_CASH_FROM="", LIVE_SNIPER_JOURNAL=self.lj, SNIPER_HYBRID_SIZING="1")
        self.sn._hybrid_risk = {"equity": 500.0, "high_water": 500.0, "observed_mono": time.monotonic()}
        self.assertEqual((self.sn._hybrid_factors()["equity"], self.sn._hybrid_factors()["live_mirror"]), (40.0, True))
        self.live_says(40.0, age_s=2_000, equity=40.0, high_water=50.0)
        self.make(SNIPER_PAPER_CASH_FROM="", LIVE_SNIPER_JOURNAL=self.lj, SNIPER_HYBRID_SIZING="1")
        self.sn._hybrid_risk = {"equity": 500.0, "high_water": 500.0, "observed_mono": time.monotonic()}
        h = self.sn._hybrid_factors()                                                # live stopped: no error
        self.assertEqual((h["equity"], h["live_mirror"]), (500.0, False))

    def test_off_is_the_old_paper(self):
        self.live_says(20.0)
        r = self.signal(SNIPER_PAPER_CASH_FROM="off")
        self.assertGreater(r["bankroll"], 500)
        self.assertNotIn("cash_source", r)


# run only this file's tests, not every inherited tests_sniper_fair.Gate test again
for _name in [n for n in dir(F.Gate) if n.startswith("test") and n not in Cash.__dict__]:
    setattr(Cash, _name, None)


class Modes(unittest.TestCase):
    def test_modes_and_live(self):
        for raw, mode in (("", "auto"), ("auto", "auto"), ("off", "off"), ("0", "off"), ("/x/live.jsonl", "path")):
            s = I.settings(SNIPER_PAPER_CASH_FROM=raw, SNIPER_FAIR="1")
            self.assertEqual(s.paper_cash_mode, mode, raw)
            refused = any("paper cash mirror" in v for v in X.spec_violations(s))
            self.assertEqual(refused, mode == "path", raw)                           # live refuses only a path
        self.assertEqual(I.settings(SNIPER_DELAY_SOURCE="").delay_source, "auto")
        for v in ("0", "-1", "abc"):
            with self.assertRaises(ValueError, msg=v):
                I.settings(SNIPER_DELAY_DAYS=v)
        with self.assertRaises(ValueError):
            I.settings(SNIPER_DELAY_SOURCE="sometimes")


if __name__ == "__main__":
    unittest.main(verbosity=1)
