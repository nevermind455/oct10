#!/usr/bin/env python3
"""SNIPER_WIDE_CAP_LIVE - the wide limit on LIVE, as a small capped test (2026-10-29,
the operator's request: "test the wide limit with live small money 50 dollar").

    python tests/tests_wide_cap_live.py

Paper's wide-limit run (0.7bp, ask+15c) filled 96% of its orders where live fills
~21-26%, so only live can say what the wide limit does. The operator chose: the
wallet holds ~$50 (the hard cap) AND the test stops itself after a set loss; the
paper run's trigger (0.7bp) and limit (ask+15c).
  1  live refuses SNIPER_WIDE_CAP_C unless SNIPER_WIDE_CAP_LIVE=1 is set too; the
     opt-in allows exactly the wide limit and a 0.5-1.0bp trigger - every other
     frozen setting stays frozen
  2  once the realized P&L since the test started reaches -SNIPER_WIDE_CAP_LIVE_STOP_USD
     (default $25) the wide limit is off for good (ask + slip and the fair limit
     again): journalled once, announced, kept off across restarts by a state file
     beside the live ledger; a smaller loss keeps it on; an unreadable state fails off
  3  paper is unchanged
No network, no wallet.
"""
from __future__ import annotations

import inspect
import json
import pathlib
import sys
import tempfile
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_hybrid_strategy as HS  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402

TEST = dict(SNIPER_FAIR="1", SNIPER_WIDE_CAP_C="15", SNIPER_WIDE_CAP_LIVE="1", SNIPER_K_BP="0.7")


class LiveOptIn(unittest.TestCase):
    def base(self):
        return X.spec_violations(I.settings(SNIPER_FAIR="1"))

    def test_live_refuses_the_wide_limit_without_the_opt_in(self):
        bad = X.spec_violations(I.settings(SNIPER_FAIR="1", SNIPER_WIDE_CAP_C="15"))
        self.assertTrue(any("wide cap" in v for v in bad), bad)
        bad = X.spec_violations(I.settings(SNIPER_FAIR="1", SNIPER_K_BP="0.7"))
        self.assertTrue(any("trigger" in v for v in bad), bad)

    def test_the_opt_in_allows_exactly_the_wide_limit_and_the_tested_trigger(self):
        self.assertEqual(X.spec_violations(I.settings(**TEST)), self.base())
        self.assertEqual(X.spec_violations(I.settings(**dict(TEST, SNIPER_K_BP="0.5"))), self.base())

    def test_the_opt_in_keeps_every_other_frozen_setting(self):
        for name, value, label in (("SNIPER_K_BP", "1.2", "trigger"), ("SNIPER_K_BP", "0.4", "trigger"),
                                   ("SNIPER_SLIP", "0.10", "slip"), ("SNIPER_KELLY_FRACTION", "0.5", "kelly fraction")):
            bad = X.spec_violations(I.settings(**dict(TEST, **{name: value})))
            self.assertTrue(any(v.startswith(label) for v in bad), (name, value, bad))

    def test_the_opt_in_without_a_wide_limit_is_refused(self):
        bad = X.spec_violations(I.settings(SNIPER_FAIR="1", SNIPER_WIDE_CAP_LIVE="1"))
        self.assertTrue(any("SNIPER_WIDE_CAP_LIVE" in v for v in bad), bad)

    def test_the_live_settings_line_names_the_test_and_its_stop(self):
        line = I.settings(**TEST).lines(live=True)[0]
        self.assertIn("WIDE CAP LIVE TEST: limit ask+15c", line)
        self.assertIn("stops after a $25 realized loss", line)
        self.assertNotIn("paper only", line)
        self.assertIn("WIDE CAP EXPERIMENT (paper only)", I.settings(**TEST).lines(live=False)[0])

    def test_the_stop_must_be_a_positive_dollar_amount(self):
        for v in ("0", "-5", "abc", "nan"):
            with self.assertRaises(ValueError, msg=v):
                I.settings(**dict(TEST, SNIPER_WIDE_CAP_LIVE_STOP_USD=v))
        self.assertEqual(I.settings(**dict(TEST, SNIPER_WIDE_CAP_LIVE_STOP_USD="40")).wide_cap_live_stop_usd, 40.0)


class LiveStop(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name) / "live_ledger.json"
        self.ledger = Ledger(str(self.path))
        self.fixture = HS.HybridStrategy()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def sniper(self, **env):
        self.fixture.make(**dict(TEST, **env))
        sn = self.fixture.sn
        sn.live = types.SimpleNamespace(ledger=self.ledger, status=lambda: {})
        sn.events = []
        sn.on_event = lambda src, text, level="info": sn.events.append((text, level))
        return sn

    def lose(self, name, dollars):
        self.assertTrue(self.ledger.record_fill_durable(f"t-{name}", f"tok-{name}", shares=2 * dollars, price=0.5,
                                                        condition_id=f"c-{name}", status="CONFIRMED", fee=0.0))
        self.ledger.settle(types.SimpleNamespace(resolved=True, condition_id=f"c-{name}",
                                                 payouts={f"tok-{name}": 0.0, f"other-{name}": 1.0}))

    def offs(self):
        return [r for r in self.fixture.rows if r.get("result") == "wide_cap_off"]

    def state(self):
        return json.loads((self.path.parent / S.Sniper.WIDE_CAP_STATE).read_text(encoding="utf-8"))

    def test_the_wide_limit_runs_until_the_stop_then_is_off_for_good(self):
        sn = self.sniper()
        self.assertEqual(sn._wide_cap_now(), 15.0)
        self.assertFalse(self.state()["off"])
        self.lose("a", 30)                                             # -$30 since the test began
        sn._wide_cap_checked = -1e18
        self.assertEqual(sn._wide_cap_now(), 0.0)
        self.assertEqual(sn._wide_cap_now(), 0.0)
        (row,) = self.offs()                                           # journalled once
        self.assertAlmostEqual(row["realized"], -30.0)
        self.assertTrue(any(level == "bad" and "WIDE CAP TEST stopped" in text for text, level in sn.events))
        self.assertTrue(self.state()["off"])
        self.assertTrue(sn.summary_snapshot()["wide_cap_live"]["off"])

    def test_a_restart_keeps_it_off(self):
        sn = self.sniper()
        sn._wide_cap_now()
        self.lose("a", 30)
        sn._wide_cap_checked = -1e18
        sn._wide_cap_now()
        again = self.sniper()                                          # a new process, the same ledger folder
        self.assertEqual(again._wide_cap_now(), 0.0)

    def test_a_smaller_loss_keeps_it_on(self):
        sn = self.sniper()
        sn._wide_cap_now()
        self.lose("a", 10)
        sn._wide_cap_checked = -1e18
        self.assertEqual(sn._wide_cap_now(), 15.0)
        self.assertEqual(self.offs(), [])
        self.assertAlmostEqual(sn.summary_snapshot()["wide_cap_live"]["realized"], -10.0)

    def test_losses_before_the_test_began_do_not_count(self):
        self.lose("old", 40)
        sn = self.sniper()
        self.assertEqual(sn._wide_cap_now(), 15.0)

    def test_an_unreadable_state_fails_off(self):
        (self.path.parent / S.Sniper.WIDE_CAP_STATE).write_text("{not json", encoding="utf-8")
        sn = self.sniper()
        self.assertEqual(sn._wide_cap_now(), 0.0)
        self.assertTrue(any("WIDE CAP" in text for text, level in sn.events))

    def test_live_without_the_opt_in_never_trades_it(self):
        sn = self.sniper(SNIPER_WIDE_CAP_LIVE="0")
        self.assertEqual(sn._wide_cap_now(), 0.0)

    def test_paper_is_unchanged(self):
        sn = self.sniper()
        sn.live = None
        self.lose("a", 30)
        self.assertEqual(sn._wide_cap_now(), 15.0)

    def test_the_decision_uses_it(self):
        self.assertIn("self._wide_cap_now()", inspect.getsource(S.Sniper._decide))


if __name__ == "__main__":
    unittest.main(verbosity=1)
