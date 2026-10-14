#!/usr/bin/env python3
"""The 25 mandatory failure / stress scenarios (repair 2026-10-14, Phase F),
each mapped to the automated tests that exercise it - and run.

    python run_scenarios.py              # every scenario, one line each
    python run_scenarios.py 7 12 21      # only these
    python run_scenarios.py --list       # the mapping, nothing run
    python run_scenarios.py --quick      # leave out the two 10,000-case stress tests (~4 min)

No network, no wallet, no real order: simulated venue replies and fault
injection only (the suites never read .env, see testing_env.py). Each test
module runs in its own process, as run_tests.py does. Exit code 0 only when
every mapped test exists and passed.

The full suite (run_tests.py) runs all of these and many more; this file
answers "which test proves scenario N" and runs exactly those.
"""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent

SCENARIOS = [
    (1, "Fresh session and a session with 10,000 prior orders", [
        "tests_repair_20261014:test_a_session_with_10000_prior_orders_sends_like_a_fresh_one",
        "tests_repair_20261014:test_each_send_appends_one_record_whatever_the_history",
        "tests_repair_20261014:test_migration_from_v2_keeps_a_backup_and_archives_finished_intents"]),
    (2, "Large historical ledger", [
        "tests_repair_20261014:test_indexed_reads_match_a_full_recomputation",
        "tests_repair_20261014:test_order_path_reads_do_not_wait_for_a_fill_flush",
        "tests_f_fixes:test_booking_does_not_grow_with_the_ledger"]),
    (3, "Rapid Coinbase / Binance feed bursts", [
        "tests_repair_20261014:test_default_count_gate_still_refuses_a_fresh_burst_and_counts_it",
        "tests_repair_20261014:test_age_gate_passes_a_fresh_burst_but_not_a_stall",
        "tests_binance_freshness:test_backlog_skips",
        "tests_sniper_coinbase:test_a_receive_backlog_is_skipped"]),
    (4, "Simultaneous UP and DOWN signals", [
        "tests_f1_share_sized:test_up_and_down_in_flight_each_reserve_their_own_worst_case",
        "tests_audit_live:test_live_up_and_down_carry_isolation",
        "tests_latency:test_1000_triggers_rapid_up_down_switching_and_opposite_pairs"]),
    (5, "Multiple qualifying signals during one order", [
        "tests_sniper_cooldown:test_one_order_in_flight_per_side",
        "tests_sniper_binance_fut:test_one_move_seen_by_both_feeds_is_one_order",
        "tests_audit_live:test_live_rapid_signals_one_order"]),
    (6, "Normal order acceptance", [
        "tests_f1_share_sized:test_signs_exactly_the_shares_and_reserves_the_worst_case",
        "tests_audit_live:test_FIXED_journal_records_signal_to_fill_confirmed",
        "tests_f1_share_sized:test_a_fill_at_a_better_price_counts_its_real_cost",
        "tests_execution_weakness_fixes:test_snake_case_acknowledgement_retains_accepted_order_identifier"]),
    (7, "FAK partial fill", [
        "tests_f1_share_sized:test_a_partial_fill_is_kept_with_the_venues_fee_curve",
        "tests_sniper_ioc:test_partial_fill_is_kept_and_its_remainder_carried",
        "tests_execution_weakness_fixes:test_partial_confirmation_does_not_double_count_booked_basis",
        "tests_sniper_fak:test_canceled_remainder_keeps_partial_fill_and_its_cost"]),
    (8, "Accepted order without trade IDs", [
        "tests_repair_20261014:test_m6_a_fak_reply_without_trade_ids_resolves_within_a_second",
        "tests_repair_20261014:test_m6_unknown_venue_status_keeps_the_side_blocked"]),
    (9, "Zero fill and rejection", [
        "tests_f1_share_sized:test_a_zero_fill_releases_its_reservation_and_risk",
        "tests_audit_live:test_live_zero_fill_is_no_fill_and_carries_the_target",
        "tests_audit_live:test_live_rejection_leaves_carry_and_counts_nothing",
        "tests_sniper_recovery:test_order_timed_out_500_is_a_rejection_not_unknown"]),
    (10, "Polymarket order-response timeout", [
        "tests_final_audit:test_timeout_unknown_keeps_the_reservation_until_the_late_outcome",
        "tests_transport_audit:test_timeout_releases_stream_without_retry",
        "tests_f1_share_sized:test_a_delayed_reply_keeps_the_order_at_risk_until_its_outcome"]),
    (11, "Connection GOAWAY and disconnect", [
        "tests_sniper_recovery:test_cold_http2_refused_and_goaway_are_not_sent_but_reset_is_unknown",
        "tests_repair_20261014:test_m7_a_reset_after_the_write_is_reconciled_as_matched_without_resending",
        "tests_connection_audit:test_private_heartbeat_timeout_reconnects_and_recovers",
        "tests_sniper_binance_fut:test_a_silent_socket_reconnects"]),
    (12, "Lost websocket fill notification", [
        "tests_f1_share_sized:test_a_lost_fill_notification_keeps_the_risk_and_halts_at_the_deadline",
        "tests_repair_20261014:test_m7_no_venue_record_in_time_trips_the_kill_switch_as_before",
        "tests_connection_audit:test_rest_revalidation_never_overwrites_newer_ws_update"]),
    (13, "Duplicate fill notification", [
        "tests_audit_20261012_accounting:test_concurrent_duplicate_fills_commit_once_and_replay",
        "tests_fill_accounting_fixes:test_duplicate_full_fill_does_not_reapply_authorization",
        "tests_final_audit:test_fill_feed_integrity_duplicates_out_of_order_conflicts"]),
    (14, "Market transition during an active order", [
        "tests_order_audit:test_market_rotation_during_delay_cannot_book",
        "tests_repair_20261014:test_the_prepared_round_is_promoted_before_discovery_returns",
        "tests_repair_20261014:test_rules_prefetched_for_the_prepared_round_are_ready_at_promotion",
        "tests_final_audit:test_carry_resets_on_market_change"]),
    (15, "Expired market with unresolved orders", [
        "tests_execution_weakness_fixes:test_an_expired_market_alone_never_releases_an_unresolved_order",
        "tests_final_audit:test_api_failure_and_not_final_never_settle",
        "tests_f_fixes:test_result_mismatch_is_raised_and_never_settled"]),
    (16, "Process crash before durable intent completion", [
        "tests_repair_20261014:test_a_failed_append_halts_and_nothing_may_be_sent",
        "tests_repair_20261014:test_the_intent_is_durable_before_the_call_returns",
        "tests_repair_20261014:test_torn_final_line_is_dropped_and_damage_elsewhere_fails_closed",
        "tests_execution_weakness_fixes:test_directory_fsync_failure_halts_before_transport_write"]),   # POSIX only
    (17, "Crash after intent persistence but before send", [
        "tests_execution_weakness_fixes:test_intent_is_durable_before_write_and_crash_restart_halts",
        "tests_execution_weakness_fixes:test_fsync_stall_expiring_guard_sends_nothing_and_resolves_unsent_intent",
        "tests_execution_weakness_fixes:test_nothing_is_sent_while_reconciling"]),
    (18, "Crash after send but before response", [
        "tests_repair_20261014:test_crash_with_an_unresolved_intent_halts_the_next_start",
        "tests_execution_weakness_fixes:test_a_proven_zero_fill_lifts_the_halt_and_stays_lifted",
        "tests_execution_weakness_fixes:test_a_match_booked_under_its_authorization_is_confirmed"]),
    (19, "Restart with open positions", [
        "tests_f_fixes:test_normal_restart_restores_every_counter",
        "tests_repair_20261014:test_archived_confirmed_spend_is_used_on_restart",
        "tests_settlement:test_a_position_left_from_a_previous_run_is_adopted_and_settled",
        "tests_redemption_fixes:test_restart_restores_durable_winners_without_settling_them_twice"]),
    (20, "Ledger snapshot and journal recovery", [
        "tests_f_fixes:test_torn_corrupt_gapped_and_orphaned_journals",
        "tests_f_fixes:test_a_crash_between_snapshot_and_truncation_never_double_applies",
        "tests_repair_20261014:test_authorizations_are_journaled_and_replayed",
        "tests_repair_20261014:test_compaction_racing_sends_loses_nothing",
        "tests_fill_accounting_fixes:test_cost_and_headline_survive_journal_and_snapshot_restart"]),
    (21, "Disk errors, slow fsync and corrupted journal tail", [
        "tests_repair_20261014:test_torn_final_line_is_dropped_and_damage_elsewhere_fails_closed",
        "tests_repair_20261014:test_a_write_failing_after_the_fill_was_visible_halts_live",
        "tests_execution_weakness_fixes:test_fsync_stall_expiring_guard_sends_nothing_and_resolves_unsent_intent",
        "tests_settlement:test_a_slow_result_source_and_a_slow_ledger_write_never_stall_the_event_loop"]),
    (22, "Insufficient available balance", [
        "tests_f1_share_sized:test_up_and_down_in_flight_each_reserve_their_own_worst_case",
        "tests_audit_live:test_live_rejection_leaves_carry_and_counts_nothing",
        "tests_transport_audit:test_balance_read_cannot_refund_concurrent_order_debit",
        "tests_order_audit:test_collateral_decimal_overflow_is_unknown_not_infinite_balance"]),
    (23, "Position-cap boundary and simultaneous reservations", [
        "tests_final_audit:test_every_canary_cap_holds_at_its_boundary",
        "tests_execution_weakness_fixes:test_overlapping_sides_reserve_the_session_fill_cap_at_dispatch",
        "tests_f1_share_sized:test_reservation_is_exactly_the_signed_worst_case_on_random_orders",
        "tests_audit_live:test_live_20_share_cap"]),
    (24, "Dashboard load during active execution", [
        "tests_repair_20261014:test_summary_is_computed_off_the_event_loop_by_the_dashboard",
        "tests_final_audit:test_dashboard_accounting_runs_off_the_event_loop",
        "tests_latency:test_no_disk_io_no_dashboard_between_trigger_and_send"]),
    (25, "Continuous operation with sustained feed and order workloads", [
        "tests_stress:test_10000_full_system_scenarios",
        "tests_stress:test_10000_concurrent_order_races",
        "tests_transport_audit:test_concurrent_canary_save_cannot_overwrite_final_risk_counters"]),
]
SLOW = {"tests_stress:test_10000_full_system_scenarios", "tests_stress:test_10000_concurrent_order_races"}


def _child(module: str, names: list) -> int:
    """Run the named test methods of one module; print one RESULT line."""
    sys.path.insert(0, str(ROOT))
    import testing_env  # noqa: F401 - first: tests never read the bot's .env
    mod = __import__(module)
    found = {}

    def walk(suite):
        for t in suite:
            if isinstance(t, unittest.TestSuite):
                walk(t)
            else:
                found.setdefault(t.id().rsplit(".", 1)[-1], t)
    walk(unittest.defaultTestLoader.loadTestsFromModule(mod))
    out = {}
    for name in names:
        test = found.get(name)
        if test is None:
            out[name] = {"status": "MISSING", "s": 0.0}
            continue
        result = unittest.TestResult()
        t0 = time.monotonic()
        unittest.TestSuite([test]).run(result)
        bad = result.failures + result.errors
        status = "PASS" if result.wasSuccessful() and not result.skipped else (
            "SKIP" if result.skipped and not bad else "FAIL")
        detail = (bad[0][1].strip().splitlines()[-1] if bad else
                  f"skipped on this platform: {result.skipped[0][1]}" if result.skipped else "")
        out[name] = {"status": status, "s": round(time.monotonic() - t0, 2), "detail": detail[:300]}
    print("RESULT " + json.dumps(out), flush=True)
    return 0


def main(argv) -> int:
    if argv[:1] == ["--child"]:
        return _child(argv[1], argv[2:])
    listing, quick = "--list" in argv, "--quick" in argv
    wanted = {int(a) for a in argv if a.isdigit()}
    chosen = [s for s in SCENARIOS if not wanted or s[0] in wanted]
    if listing:
        for n, title, tests in chosen:
            print(f"{n:2d}. {title}")
            for t in tests:
                print(f"      {t.replace(':', '  ')}")
        return 0
    by_module: dict = {}
    for _n, _title, tests in chosen:
        for t in tests:
            if quick and t in SLOW:
                continue
            module, name = t.split(":")
            by_module.setdefault(module, [])
            if name not in by_module[module]:
                by_module[module].append(name)
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    results: dict = {}
    for module, names in sorted(by_module.items()):
        try:
            p = subprocess.run([sys.executable, str(ROOT / "run_scenarios.py"), "--child", module, *names], cwd=ROOT,
                               env=env, capture_output=True, text=True, encoding="utf-8", errors="replace",
                               timeout=1800)
            line = next((ln for ln in reversed(p.stdout.splitlines()) if ln.startswith("RESULT ")), None)
            got = json.loads(line[7:]) if line else {}
            tail = (p.stderr or p.stdout).strip().splitlines()[-1:] or [""]
        except subprocess.TimeoutExpired:
            got, tail = {}, ["module timed out"]
        for name in names:
            results[f"{module}:{name}"] = got.get(name) or {"status": "FAIL", "s": 0.0, "detail": tail[0][:300]}
    ok_all, n_pass, n_skip, n_total = True, 0, 0, 0
    for n, title, tests in chosen:
        rows = [(t, results.get(t, {"status": "NOT RUN (--quick)", "s": 0.0})) for t in tests]
        ran = [r for _t, r in rows if not r["status"].startswith("NOT RUN")]
        # a platform skip (e.g. POSIX-only directory fsync on Windows) is reported,
        # not counted as a pass; the scenario still needs every other test to pass
        good = all(r["status"] in ("PASS", "SKIP") for r in ran) and any(r["status"] == "PASS" for r in ran)
        ok_all &= bool(good)
        n_pass += sum(r["status"] == "PASS" for r in ran)
        n_skip += sum(r["status"] == "SKIP" for r in ran)
        n_total += len(ran)
        print(f"{'PASS' if good else 'FAIL'}  {n:2d}. {title}")
        for t, r in rows:
            print(f"        {r['status']:<5} {r['s']:7.2f}s  {t.replace(':', '  ')}"
                  + (f"\n              {r.get('detail')}" if r["status"] not in ("PASS",) and r.get("detail") else ""))
    print(f"\n{len(chosen)} scenarios, {n_pass}/{n_total} mapped tests passed"
          + (f", {n_skip} skipped on this platform" if n_skip else "") + ("" if ok_all else " - FAILURES above"))
    return 0 if ok_all else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
