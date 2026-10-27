#!/usr/bin/env python3
"""Canary limits after a restart (2026-10-18). No network, no wallet.

    python tests_canary_limits.py

The live restart kept a $50 loss cap from the saved canary session after .env
was lowered to $30, and a shell's exported SNIPER_CANARY_MAX_SHARES=200 beat
.env's 25 without a word. Now:
  1  a restored budget never exceeds this start's configured limits: each
     ceiling and step is the lower of the saved value and the configured one;
     a saved budget that is tighter stays tighter
  2  every start names a SNIPER_* setting whose shell value overrides .env
"""
from __future__ import annotations

import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402


def limits(**kw):
    base = dict(max_order_shares=25, max_orders=1000, max_fills=1000, max_rounds=1000,
                max_exposure_usd=500.0, max_daily_loss_usd=30.0)
    return L.CanaryLimits(**{**base, **kw})


def saved(**ceilings):
    """A budget_state() as a session saved it, with the given ceilings (steps = the original values)."""
    old = limits(max_daily_loss_usd=50.0, max_exposure_usd=1e5, max_orders=10 ** 5, max_fills=10 ** 5,
                 max_rounds=10 ** 5)
    state = old.budget_state()
    state["ceilings"].update(ceilings)
    return state


class RestoredBudget(unittest.TestCase):
    def test_a_lowered_env_limit_wins_over_the_saved_session(self):
        lim = limits()                                         # .env now: $30 loss, $500 spend
        lim.restore_budget(saved(max_daily_loss_usd=100.0))    # saved: $50 + one $50 step earned after a pause
        self.assertEqual(lim.max_daily_loss_usd, 30.0)
        self.assertEqual(lim.max_exposure_usd, 500.0)
        self.assertEqual(lim.max_orders, 1000)
        self.assertEqual(lim.budget_state()["step"]["max_daily_loss_usd"], 30.0)   # a later step is +$30, not +$50

    def test_a_tighter_saved_budget_stays_tighter(self):
        lim = limits(max_daily_loss_usd=80.0)
        state = limits(max_daily_loss_usd=20.0).budget_state()
        lim.restore_budget(state)
        self.assertEqual(lim.max_daily_loss_usd, 20.0)
        self.assertEqual(lim.budget_state()["step"]["max_daily_loss_usd"], 20.0)

    def test_the_per_order_share_cap_is_never_restored(self):
        lim = limits(max_order_shares=25)
        lim.restore_budget(saved())
        self.assertEqual(lim.max_order_shares, 25)             # always this start's own

    def test_pending_caps_survive_and_a_malformed_budget_is_still_refused(self):
        lim = limits()
        state = saved()
        state["pending"] = {"max_daily_loss_usd": "canary daily loss $31.00 >= $30.00"}
        lim.restore_budget(state)
        self.assertEqual(lim.budget_state()["pending"], state["pending"])
        bad = saved()
        bad["ceilings"]["max_daily_loss_usd"] = -1
        with self.assertRaises(ValueError):
            limits().restore_budget(bad)


class ShadowedSettings(unittest.TestCase):
    def test_a_shell_value_that_overrides_env_is_named(self):
        import run
        out = run.shadowed_settings(env={"SNIPER_CANARY_MAX_SHARES": "200", "SNIPER_K_BP": "1.0",
                                         "POLY_PRIVATE_KEY": "0xsecret"},
                                    file_values={"SNIPER_CANARY_MAX_SHARES": "25", "SNIPER_K_BP": "1.0",
                                                 "POLY_PRIVATE_KEY": "0xother"})
        self.assertEqual(len(out), 1)
        self.assertIn("SNIPER_CANARY_MAX_SHARES=200 comes from the shell and overrides .env's "
                      "SNIPER_CANARY_MAX_SHARES=25", out[0])
        self.assertNotIn("secret", " ".join(out))              # only SNIPER_* settings, never credentials

    def test_nothing_to_say_when_they_agree_or_the_shell_has_none(self):
        import run
        self.assertEqual(run.shadowed_settings(env={"SNIPER_K_BP": " 1.0 "}, file_values={"SNIPER_K_BP": "1.0"}), [])
        self.assertEqual(run.shadowed_settings(env={}, file_values={"SNIPER_K_BP": "1.0"}), [])

    def test_tests_never_read_the_bots_env_file(self):
        import dotenv
        self.assertEqual(dotenv.dotenv_values("anything"), {})


if __name__ == "__main__":
    unittest.main(verbosity=1)
