"""Strict comparison uses current live controls without sending orders."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import time
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import testing_env  # noqa: E402,F401
import paper_parity as P  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_hybrid_strategy as H  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402


class StrictPaper(unittest.TestCase):
    def setUp(self):
        self.fixture = H.HybridStrategy()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "live.jsonl"

    def settings(self, **over):
        return I.settings(**{
            "SNIPER_PAPER_LIVE_PARITY": "1", "SNIPER_DELAY_SOURCE": "auto", "SNIPER_LATENCY_MS": "",
            "SNIPER_PAPER_CASH_FROM": "auto", "LIVE_SNIPER_JOURNAL": str(self.path),
            "SNIPER_DELAY_JOURNAL": str(self.path), "SNIPER_FAIR": "1", "SNIPER_KELLY": "1",
            "SNIPER_KELLY_FRACTION": ".25", "SNIPER_HYBRID_SIZING": "1", "SNIPER_ORDER_SIZING": "shares",
            **over})

    def reference(self, settings=None, **over):
        settings = settings or self.settings()
        broker = self.fixture.broker
        return {"result": "execution_reference", "version": 1, "live": True, "wall": time.time(),
                "config": P.comparison_config(settings, (broker.min_buy_price, broker.max_buy_price), broker),
                "cash": 1000.0, "hybrid": {"equity": 1000.0, "high_water": 1000.0, "wall": time.time()},
                "max_order_shares": 25.0, "venue_min_shares": 0.0, "pauses": [],
                "wide_active": False, "wide_ready": True, "market": P.market_key(self.fixture.m),
                "readiness": dict.fromkeys(P.READINESS_FIELDS, True), "valid_until": time.time() + 14, **over}

    def journal(self, reference=None, age=5):
        now = time.time()
        rows = [{"wall": now - age, "result": "summary", "live": {"clock": {"rtt_ms_p50": 24}}}]
        rows += [{"wall": now - age, "live": True, "kind": "NO_MATCH", "result": "refused",
                  "t_wire_to_reply_ms": 112 + i} for i in range(40)]
        if reference is not None:
            rows.append(reference)
        self.path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")

    def sniper(self, settings=None):
        sn = S.Sniper(self.fixture.m, self.fixture.broker, settings=settings or self.settings())
        sn._write = self.fixture.rows.append
        sn.vol_model.vol = lambda: .0001
        sn._hybrid_risk = {"equity": 1000, "high_water": 1000, "observed_mono": time.monotonic()}
        sn._set_bankroll(self.fixture.broker.cash_balance(), time.monotonic())
        self.fixture.sn = sn
        return sn

    def test_default_is_strict_and_live_configuration_is_not_rewritten(self):
        with patch.dict(os.environ):
            os.environ.pop("SNIPER_PAPER_LIVE_PARITY", None)
            self.assertTrue(S.Settings().paper_live_parity)
        self.assertEqual(self.settings(SNIPER_PAPER_LIVE_PARITY="0").paper_cash_mode, "auto")

    def test_missing_live_data_skips_instead_of_trading_papers_own_cash(self):
        sn = self.sniper()
        plan, row = self.fixture.plan()
        self.assertIsNone(plan)
        self.assertEqual(row["reason"], "paper/live comparison unavailable")
        self.assertEqual(sn.latency_plan()[1]["latency_mode"], "unavailable")
        self.assertIn("simulated fills remain unvalidated", " ".join(sn.lines()))

    def test_fresh_reference_caps_the_real_order_plan_at_lives_share_limit(self):
        self.journal(self.reference(cash=1000, max_order_shares=7))
        sn = self.sniper()
        plan, row = self.fixture.plan(move=10)
        self.assertIsNotNone(plan, row)
        self.assertEqual(row["order_shares"], 7)
        self.assertEqual(sn._bankroll_now(), 1000)
        self.assertIn("strict comparison", row["execution_model"])

    def test_missing_then_arriving_live_reference_recovers_without_restart(self):
        sn = self.sniper()
        self.assertIsNotNone(sn._paper_parity_problem())
        self.journal(self.reference(cash=40))
        sn._refresh_paper_delays()
        sn._refresh_live_mirror()
        self.assertIsNone(sn._paper_parity_problem())
        self.assertEqual(sn._bankroll_now(), 40)

    def test_stale_cash_reference_or_drawdown_or_paused_live_blocks_decisions(self):
        for over in ({"wall": time.time() - 20}, {"wall": time.time() + 60},
                     {"hybrid": {"equity": 1000, "high_water": 1000, "wall": time.time() - 60}},
                     {"pauses": ["canary loss budget"]}):
            with self.subTest(over=over):
                self.journal(self.reference(**over))
                sn = self.sniper()
                self.assertIsNotNone(sn._paper_parity_problem())
                self.assertIsNone(sn._bankroll_now())

    def test_changed_settings_pause_instead_of_comparing_different_strategies(self):
        ref = self.reference()
        ref["config"]["k_bp"] = .7
        self.journal(ref)
        sn = self.sniper()
        self.assertIn("settings differ", sn._paper_parity_problem())

    def test_previous_market_reference_cannot_authorize_the_new_round(self):
        ref = self.reference()
        ref["market"] = ["previous-condition", "previous-up", "previous-down", self.fixture.m.window_end - 300]
        self.journal(ref)
        self.assertIsNotNone(self.sniper()._paper_parity_problem())

    def test_rotation_invalidates_the_cached_reference_before_the_next_refresh(self):
        self.journal(self.reference())
        sn = self.sniper()
        self.assertIsNone(sn._paper_parity_problem())
        self.fixture.m.condition_id = "next-condition"
        self.assertIn("market differs", sn._paper_parity_problem())

    def test_unready_live_execution_cannot_authorize_paper(self):
        for name in ("collateral", "round_cash", "private_fills", "rules", "clock", "venue_guard"):
            with self.subTest(name=name):
                ref = self.reference()
                ref["readiness"] = {key: key != name for key in (
                    "collateral", "round_cash", "private_fills", "rules", "clock", "venue_guard")}
                self.journal(ref)
                self.assertIsNotNone(self.sniper()._paper_parity_problem())

    def test_a_new_pause_invalidates_a_previously_good_reference(self):
        self.journal(self.reference())
        sn = self.sniper()
        self.assertIsNone(sn._paper_parity_problem())
        self.journal(self.reference(pauses=["guard paused"]))
        sn._refresh_live_mirror()
        self.assertIn("guard paused", sn._paper_parity_problem())

    def test_old_or_undated_delay_samples_cannot_pass_strict_comparison(self):
        self.journal(self.reference(), age=4 * 86400)
        sn = self.sniper()
        self.assertIsNotNone(sn._paper_parity_problem())
        self.assertEqual(sn._live_delays, [])
        self.journal(self.reference())
        sn._refresh_paper_delays()
        self.assertEqual(len(sn._live_delays), 40)
        self.assertIsNone(sn._paper_parity_problem())
        sn._paper_delays_read_at -= 2 * sn.PAPER_DELAY_REFRESH_S + 1
        self.assertEqual(sn._paper_parity_problem(), "live delay refresh stale")

    def test_expired_and_undated_rows_are_excluded_with_a_wall_clock_window(self):
        self.journal(age=4 * 86400)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"live": True, "kind": "NO_MATCH", "result": "refused",
                                     "t_wire_to_reply_ms": 212}) + "\n")
        self.assertEqual(S.live_delays(self.path, days=3, now=time.time())[0], [])

    def test_malformed_delay_rows_do_not_crash_refresh_or_poison_the_rtt(self):
        self.journal(self.reference())
        rows = [{"wall": time.time(), "result": "summary", "live": {"clock": clock}}
                for clock in (["bad clock"], {"rtt_ms_p50": float("inf")}, {"rtt_ms_p50": True})]
        rows.append({"wall": 10 ** 400, "live": True, "result": "refused", "kind": "NO_MATCH",
                     "t_wire_to_reply_ms": 112})
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write("".join(json.dumps(row) + "\n" for row in rows))
        delays, one_way = S.live_delays(self.path, days=3, now=time.time())
        self.assertEqual((len(delays), one_way, delays[0]), (40, 12, 100))

    def test_wide_cap_and_slip_follow_the_live_stop_without_writing_live_state(self):
        settings = self.settings(SNIPER_WIDE_CAP_C="15", SNIPER_WIDE_CAP_LIVE="1", SNIPER_SLIP=".08")
        self.journal(self.reference(settings, wide_active=True))
        sn = self.sniper(settings)
        self.assertEqual((sn._wide_cap_now(), sn._slip_now()), (15, .08))
        self.journal(self.reference(settings, wide_active=False))
        before = self.path.read_bytes()
        sn._refresh_live_mirror()
        self.assertEqual((sn._wide_cap_now(), sn._slip_now()), (0, .05))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertFalse((self.path.parent / sn.WIDE_CAP_STATE).exists())

    def test_live_venue_minimum_fallback_is_shared(self):
        settings = self.settings(SNIPER_VENUE_MIN_SHARES="1")
        self.journal(self.reference(settings, venue_min_shares=0))
        self.assertEqual(self.sniper(settings).s.venue_min_shares, 0)

    def test_live_drawdown_stop_blocks_the_paper_order_even_if_paper_equity_is_high(self):
        self.journal(self.reference(hybrid={"equity": 800, "high_water": 1000, "wall": time.time()}))
        self.sniper()
        plan, row = self.fixture.plan(move=10)
        self.assertIsNone(plan)
        self.assertEqual(row["reason"], "hybrid drawdown stop")

    def test_missing_test_state_and_foreign_delay_files_do_not_silently_run(self):
        settings = self.settings(SNIPER_WIDE_CAP_C="15", SNIPER_WIDE_CAP_LIVE="1")
        self.journal(self.reference(settings, wide_ready=False))
        self.assertIn("not initialized", self.sniper(settings)._paper_parity_problem())
        with self.assertRaisesRegex(ValueError, "LIVE_SNIPER_JOURNAL"):
            self.sniper(self.settings(SNIPER_DELAY_JOURNAL=str(self.path.parent / "old.jsonl")))

    def test_model_only_options_and_live_refused_settings_need_explicit_opt_out(self):
        for over in ({"SNIPER_DELAY_SOURCE": "model"}, {"SNIPER_LATENCY_MS": "200"},
                     {"SNIPER_PAPER_CASH_FROM": "off"}, {"SNIPER_KELLY_FRACTION": ".5"},
                     {"SNIPER_BOOK_QUIET": "1"}, {"SNIPER_BOOK_CLOCK": "wake"}):
            with self.subTest(over=over), self.assertRaises(ValueError):
                self.sniper(self.settings(**over))

    def test_reader_ignores_partial_append_and_validator_rejects_invalid_limits(self):
        ref = self.reference()
        self.journal(ref)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(self.reference(cash=999)))  # not committed with a newline
        self.assertEqual(P.read_reference(self.path)["cash"], ref["cash"])
        for cap in (None, True, float("nan"), 0, -1, 10 ** 400):
            bad = {**ref, "max_order_shares": cap}
            with self.subTest(cap=cap), self.assertRaises(ValueError):
                P.validate_reference(bad, ref["config"])

    def live_execution(self):
        clock = types.SimpleNamespace(now_s=time.time, mono=time.monotonic, at=time.monotonic(), max_age_s=120)
        return types.SimpleNamespace(
            balance=40, reserved=3, balance_at=time.monotonic(), balance_max_age_s=120,
            kill=types.SimpleNamespace(active=lambda: []), user_ready=lambda _condition: True,
            rules_ready=tuple(P.market_key(self.fixture.m)[:3]), _cash_read_for=lambda _start: True,
            shim=types.SimpleNamespace(cached_rules=lambda _condition: object()),
            sender=types.SimpleNamespace(limits=types.SimpleNamespace(max_order_shares=25), clock=clock,
                                         guard=types.SimpleNamespace(state="HEALTHY")), attach=lambda _sn: None)

    def publish(self, live):
        sn = S.Sniper(self.fixture.m, self.fixture.broker, live=live, settings=self.settings())
        sn._hybrid_risk = {"equity": 50, "high_water": 60, "observed_mono": time.monotonic()}
        rows = []
        sn._write = rows.append
        sn._publish_execution_reference()
        row, = rows
        return row, sn

    def test_live_publishes_cached_controls_without_ledger_access_or_orders(self):
        row, sn = self.publish(self.live_execution())
        P.validate_reference(row, sn._comparison_config)
        self.assertEqual((row["cash"], row["max_order_shares"], row["hybrid"]["equity"]), (37, 25, 50))

    def test_publisher_marks_missing_live_prerequisites_unready(self):
        for name in P.READINESS_FIELDS:
            with self.subTest(name=name):
                live = self.live_execution()
                if name == "collateral":
                    live.balance_at -= 121
                elif name == "round_cash":
                    live._cash_read_for = lambda _start: False
                elif name == "private_fills":
                    live.user_ready = lambda _condition: False
                elif name == "rules":
                    live.rules_ready = None
                elif name == "clock":
                    live.sender.clock.at -= 121
                else:
                    live.sender.guard.state = "PAUSED"
                row, sn = self.publish(live)
                self.assertIs(row["readiness"][name], False)
                with self.assertRaisesRegex(ValueError, "live execution not ready"):
                    P.validate_reference(row, sn._comparison_config)

    def test_known_balance_and_clock_expiry_outlive_neither_cached_reference(self):
        for name in ("collateral", "clock"):
            with self.subTest(name=name):
                live = self.live_execution()
                if name == "collateral":
                    live.balance_at -= 119
                else:
                    live.sender.clock.at -= 119
                row, sn = self.publish(live)
                with self.assertRaisesRegex(ValueError, "readiness reference expired"):
                    P.validate_reference(row, sn._comparison_config, now=row["wall"] + 2)


class ComparisonReport(unittest.TestCase):
    def report(self, *, failed=False, tie=False, pending=False, ambiguous=False):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            now = time.time() - 60
            common = {"id": "signal", "wall": now, "condition_id": "condition", "side": "UP", "bp": 1,
                      "shares": 5, "price": .4, "fee": .1, "order_shares": 5, "ask_seen": .4, "bankroll": 20}
            live = [dict(common, live=True, result="pending", kind="MATCHED", shares=None, cost=None)]
            if ambiguous:
                live[0].update(result="refused", kind="AMBIGUOUS")
            if not pending:
                update = dict(common, wall=now + 20, live=True, result="live_fill_update", kind="MATCHED",
                              resolved_result="refused" if failed else "filled", shares=0 if failed else 5,
                              cost=0 if failed else 2)
                live += [update, update]  # repeated observations must not duplicate the order
            paper = [dict(common, result="filled", cost=2.1)]
            for path, rows in (("live_sniper.jsonl", live), ("paper_orders_sniper.jsonl", paper)):
                rows = [{"wall": now - 1, "result": "start", "settings": "spot mid >= 0.5bp"}, *rows,
                        {"wall": now + 30, "result": "summary"}]
                (folder / path).write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
            (folder / "live_ledger_settlements.jsonl").write_text(json.dumps(
                {"event": "RESULT_RECEIVED", "condition_id": "condition", "winner": "50/50" if tie else "UP"}
            ) + '\n["RESULT_RECEIVED", "not an object"]\n', encoding="utf-8")
            helper = Path(__file__).resolve().parent.parent / "handoff_scripts" / "pvl.py"
            proc = subprocess.run([sys.executable, "-I", str(helper), tmp, tmp],
                                  capture_output=True, text=True, timeout=20)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            return proc.stdout

    def test_fees_count_once_and_late_updates_keep_the_original_signal_time(self):
        out = self.report()
        self.assertIn("decided within 1s): 1 pairs", out)
        self.assertRegex(out, r"both filled\s+1\s+live P&L \$\s*\+2\.90\s+paper P&L \$\s*\+2\.90")

    def test_failed_and_unresolved_live_outcomes_are_not_successful_fills(self):
        self.assertRegex(self.report(failed=True), r"PAPER ONLY filled\s+1")
        self.assertRegex(self.report(pending=True), r"unresolved\s+1")
        self.assertRegex(self.report(pending=True, ambiguous=True), r"unresolved\s+1")

    def test_fractional_payout_is_counted(self):
        self.assertRegex(self.report(tie=True), r"both filled\s+1\s+live P&L \$\s*\+0\.40\s+paper P&L \$\s*\+0\.40")


if __name__ == "__main__":
    unittest.main()
