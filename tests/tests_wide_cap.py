#!/usr/bin/env python3
"""SNIPER_WIDE_CAP_C - a PAPER experiment (2026-10-28, the operator's request).

    python tests/tests_wide_cap.py

Live missed ~79% of its orders because the asks were gone by the match. Priced
at the reply-time book, buying those misses anyway would have made +$167 on
1,236 orders (10-18..21, t=1.5 - unproven), so paper tests it:
  1  the price limit is ask + SNIPER_WIDE_CAP_C cents, whatever the fair value
     says, never past the account's MAX_BUY_PRICE
  2  an ask above the fair limit (normally "ask not below fair value") still
     buys a fixed minimum-size order - Kelly says 0 there
  3  an ask under the fair limit keeps its Kelly size, with the wide limit
  4  off by default: every decision as before
  5  LIVE refuses it (exec_latency.spec_violations); bad values stop the start
The real Sniper and paper broker (tests_sniper_fair.Gate). No network.
"""
from __future__ import annotations

import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402


class WideCap(F.Gate):
    """Share sizing, as live and paper50 run it (the frozen spec)."""

    def test_an_ask_above_the_fair_limit_buys_a_fixed_minimum(self):
        self.make(SNIPER_WIDE_CAP_C="15", SNIPER_ORDER_SIZING="shares")
        self.pin_time_left()
        r = self.order("UP", [(0.31, 200)], mid=0.25, move_bp=0.68)      # refused by the fair value when off
        self.assertGreater(0.31, r["fair_limit"])
        self.assertEqual((r["wide_cap_c"], r["wide_cap_mode"]), (15.0, "above_fair_fixed"))
        self.assertEqual(r["result"], "filled", r)
        self.assertAlmostEqual(r["cap"], 0.46)                          # ask + 15c, past the fair limit
        self.assertEqual(r["wide_cap_fixed_shares"], 5.0)
        self.assertIsNone(r["kelly_shares"])
        self.assertEqual(r["order_shares"], 5)

    def test_an_ask_under_the_fair_limit_keeps_kelly_with_the_wide_limit(self):
        self.make(SNIPER_WIDE_CAP_C="15", SNIPER_ORDER_SIZING="shares")
        self.pin_time_left()
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.assertEqual((r["result"], r["wide_cap_mode"]), ("filled", "kelly"))
        self.assertAlmostEqual(r["cap"], 0.56)                          # not min(ask + 5c, fair limit)
        self.assertGreater(r["cap"], r["fair_limit"])
        self.assertGreater(r["kelly_shares"], 5)

    def test_the_limit_never_passes_the_accounts_max_buy_price(self):
        self.make(SNIPER_WIDE_CAP_C="15", SNIPER_ORDER_SIZING="shares")
        self.pin_time_left()
        r = self.order("UP", [(0.90, 200)], mid=0.88, move_bp=5.0)
        self.assertEqual(r["cap"], 0.95)

    def test_off_by_default_nothing_changes(self):
        self.pin_time_left()
        r = self.order("UP", [(0.31, 200)], mid=0.25, move_bp=0.68)
        self.assertEqual((r["result"], r["reason"]), ("skipped", "ask not below fair value"))
        self.assertNotIn("wide_cap_c", r)
        r = self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)
        self.assertAlmostEqual(r["cap"], min(0.46, r["fair_limit"]))
        self.assertNotIn("wide_cap_mode", r)


# run only this file's tests, not every inherited tests_sniper_fair.Gate test again
for _name in [n for n in dir(F.Gate) if n.startswith("test") and n not in WideCap.__dict__]:
    setattr(WideCap, _name, None)


class LiveAndValues(unittest.TestCase):
    def test_live_refuses_the_experiment_and_paper_says_so(self):
        s = I.settings(SNIPER_WIDE_CAP_C="15", SNIPER_FAIR="1")
        bad = X.spec_violations(s)
        self.assertTrue(any("wide cap experiment" in v for v in bad), bad)
        self.assertIn("WIDE CAP EXPERIMENT (paper only): limit ask+15c", s.fair_text())
        self.assertFalse(any("wide cap" in v for v in X.spec_violations(I.settings(SNIPER_FAIR="1"))))

    def test_bad_values_stop_the_start(self):
        for v in ("-1", "51", "abc"):
            with self.assertRaises(ValueError, msg=v):
                I.settings(SNIPER_WIDE_CAP_C=v)


if __name__ == "__main__":
    unittest.main(verbosity=1)
