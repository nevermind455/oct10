"""Offline regressions for the actual sniper_why command's printed verdict."""
from __future__ import annotations

import contextlib
import io
import json
import pathlib
import tempfile
import unittest
from unittest import mock

import testing_env  # noqa: F401 - before imports that may read the bot's .env
import sniper_why as why


class Verdicts(unittest.TestCase):
    NOW = 1_700_000_000.0

    def row(self, result, ago=10, **fields):
        return {"wall": self.NOW - ago, "result": result, **fields}

    def summary(self, ago=20, **fields):
        values = dict(signals=50, orders=10, filled=3, refused=7,
                      errors=2, skips={"live halted": 30})
        values.update(fields)
        return self.row("summary", ago, **values)

    def output(self, rows, *, live=True):
        start = self.row("start", 1000, live=live, code="test", settings={})
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "sniper.jsonl"
            journal = path if live else path.with_name(f"{path.stem}_sniper.jsonl")
            journal.write_text("".join(json.dumps(r) + "\n" for r in [start, *rows]), encoding="utf-8")
            out = io.StringIO()
            argv = ["sniper_why.py", "--env", str(pathlib.Path(tmp) / "absent.env")]
            if live:
                argv.append("--live")
            with mock.patch("sys.argv", argv), mock.patch.object(why.sc, "state_path", return_value=path), \
                    mock.patch.object(why.time, "time", return_value=self.NOW), contextlib.redirect_stdout(out):
                result = why.main()
        self.assertEqual(result, 0, out.getvalue())
        return out.getvalue(), out.getvalue().split("\nVERDICT:\n", 1)[1]

    def test_halt_then_matching_resume_then_fill_is_trading(self):
        output, verdict = self.output([
            self.row("error", 100, reason="old signing failure"),
            self.row("skipped", 90, reason="live halted", detail="reconciliation delayed"),
            self.summary(80, live={"halted": ["reconciliation delayed"]}),
            self.row("resume", 15, reason="reconciliation delayed", paused_s=300),
            self.row("filled", 5, live=True, shares=10),
        ])
        self.assertIn("it IS trading", verdict)
        self.assertNotIn("IS HALTED", verdict)
        self.assertNotIn("FAILING", verdict)
        self.assertNotIn("restart needed", output)
        self.assertNotIn("until it is restarted", output)

    def test_latest_explicit_status_overrides_historical_halt_and_counts(self):
        _, verdict = self.output([
            self.row("skipped", 100, reason="live halted", detail="old uncertainty"),
            self.summary(live={"halted": []}),
        ])
        self.assertIn("it IS trading", verdict)
        self.assertNotIn("IS HALTED", verdict)
        self.assertNotIn("FAILING", verdict)

    def test_resume_clears_only_its_own_active_cause(self):
        _, verdict = self.output([
            self.summary(60, live={"halted": ["cause A", "cause B"]}),
            self.row("resume", 10, reason="cause A", paused_s=300),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("IS HALTED", verdict)
        self.assertIn("cause B", verdict)
        self.assertNotIn("cause A", verdict)

    def test_separate_pause_events_and_unrelated_resume_preserve_active_causes(self):
        _, verdict = self.output([
            self.row("pause", 60, reason="cause A"),
            self.row("halt", 50, reason="cause B"),
            self.row("resume", 10, reason="unrelated cause"),
        ])
        self.assertIn("IS HALTED", verdict)
        self.assertIn("cause A", verdict)
        self.assertIn("cause B", verdict)

    def test_joined_skip_after_complete_status_does_not_create_phantom_cause(self):
        _, verdict = self.output([
            self.summary(60, live={"halted": ["cause A", "cause B"]}),
            self.row("skipped", 50, reason="live halted", detail="cause A; cause B"),
            self.row("resume", 15, reason="cause A"),
            self.row("resume", 10, reason="cause B"),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("it IS trading", verdict)

    def test_opaque_joined_halt_is_not_cleared_by_partial_resume(self):
        _, verdict = self.output([
            self.row("skipped", 50, reason="live halted", detail="cause A; cause B"),
            self.row("resume", 10, reason="cause A"),
        ])
        self.assertIn("IS HALTED", verdict)
        self.assertIn("cause B", verdict)

    def test_producer_truncated_joined_skip_does_not_leave_phantom_halt(self):
        causes = ["cause A: " + "a" * 140, "cause B: " + "b" * 140]
        self.assertGreater(len("; ".join(causes)), 200)
        _, verdict = self.output([
            self.summary(60, live={"halted": causes}),
            self.row("skipped", 50, reason="live halted", detail="; ".join(causes)[:200]),
            self.row("resume", 15, reason=causes[0][:200], paused_s=300),
            self.row("resume", 10, reason=causes[1][:200], paused_s=300),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("it IS trading", verdict)
        self.assertNotIn("IS HALTED", verdict)

    def test_producer_truncated_single_long_reason_resumes_uniquely(self):
        cause = "long reconciliation cause: " + "x" * 240
        _, verdict = self.output([
            self.summary(60, live={"halted": [cause]}),
            self.row("skipped", 50, reason="live halted", detail=cause[:200]),
            self.row("resume", 10, reason=cause[:200], paused_s=300),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("it IS trading", verdict)
        self.assertNotIn("IS HALTED", verdict)

    def test_unique_truncated_resume_preserves_an_unrelated_cause(self):
        cause = "long reconciliation cause: " + "x" * 240
        _, verdict = self.output([
            self.summary(60, live={"halted": [cause, "other cause"]}),
            self.row("resume", 10, reason=cause[:200], paused_s=300),
        ])
        self.assertIn("IS HALTED", verdict)
        self.assertIn("other cause", verdict)
        self.assertNotIn("long reconciliation cause", verdict)

    def test_ambiguous_truncated_resume_reports_unknown(self):
        prefix = "shared uncertainty: " + "x" * 200
        causes = [prefix + " cause A", prefix + " cause B"]
        _, verdict = self.output([
            self.summary(60, live={"halted": causes}),
            self.row("skipped", 50, reason="live halted", detail="; ".join(causes)[:200]),
            self.row("resume", 10, reason=causes[0][:200], paused_s=300),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("status is UNKNOWN", verdict)
        self.assertNotIn("IS HALTED", verdict)
        self.assertNotIn("it IS trading", verdict)

    def test_exact_200_character_reason_cannot_disambiguate_a_shared_prefix(self):
        prefix = "x" * 200
        _, verdict = self.output([
            self.summary(60, live={"halted": [prefix, prefix + " longer cause"]}),
            self.row("resume", 10, reason=prefix, paused_s=300),
        ])
        self.assertIn("status is UNKNOWN", verdict)

    def test_complete_status_resolves_an_ambiguous_resume(self):
        prefix = "x" * 200
        _, verdict = self.output([
            self.summary(60, live={"halted": [prefix + " A", prefix + " B"]}),
            self.row("resume", 30, reason=prefix, paused_s=300),
            self.summary(20, live={"halted": []}),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("it IS trading", verdict)
        self.assertNotIn("IS HALTED", verdict)

    def test_opaque_200_character_skip_cannot_prove_all_causes_resumed(self):
        causes = ["a" * 220, "another still-active cause"]
        _, verdict = self.output([
            self.row("skipped", 50, reason="live halted", detail="; ".join(causes)[:200]),
            self.row("resume", 10, reason=causes[0][:200], paused_s=300),
            self.row("filled", 5, live=True),
        ])
        self.assertIn("status is UNKNOWN", verdict)
        self.assertNotIn("IS HALTED", verdict)
        self.assertNotIn("it IS trading", verdict)

    def test_complete_status_can_resolve_opaque_truncated_skip(self):
        cause = "a" * 220
        _, verdict = self.output([
            self.row("skipped", 60, reason="live halted", detail=cause[:200]),
            self.row("resume", 30, reason=cause[:200], paused_s=300),
            self.summary(20, live={"halted": []}, filled=0),
            self.row("filled", 5, live=True, order_id="new-filled-order"),
        ])
        self.assertIn("it IS trading: 1 recorded fill(s)", verdict)

    def test_failed_fak_update_and_new_summary_retract_old_fill_evidence(self):
        _, verdict = self.output([
            self.row("filled", 100, live=True, order_id="failed-order", shares=10),
            self.row("live_fill_update", 30, live=True, order_id="failed-order",
                     resolved_result="refused", shares=0, retracted_shares=10,
                     reason="venue reported FAILED execution"),
            self.summary(20, live={"halted": []}, filled=0, refused=1, orders=1),
        ])
        self.assertNotIn("it IS trading", verdict)
        self.assertIn("no fills currently recorded", verdict)
        self.assertNotIn("price was gone", verdict)

    def test_failed_fak_update_supersedes_an_older_summary_fill_counter(self):
        _, verdict = self.output([
            self.row("filled", 100, live=True, order_id="failed-order", shares=10),
            self.summary(80, live={"halted": []}, filled=1, orders=1),
            self.row("live_fill_update", 10, live=True, order_id="failed-order",
                     resolved_result="refused", shares=0, retracted_shares=10),
        ])
        self.assertNotIn("it IS trading", verdict)
        self.assertIn("no fills currently recorded", verdict)

    def test_failed_fak_update_only_retracts_its_own_fill(self):
        _, verdict = self.output([
            self.row("filled", 120, live=True, order_id="good-order", shares=5),
            self.row("filled", 100, live=True, order_id="failed-order", shares=10),
            self.summary(80, live={"halted": []}, filled=2, orders=2),
            self.row("live_fill_update", 10, live=True, order_id="failed-order",
                     resolved_result="refused", shares=0, retracted_shares=10),
        ])
        self.assertIn("it IS trading: 1 recorded fill(s)", verdict)

    def test_failed_fak_update_matches_client_and_decision_identifiers(self):
        for field in ("client_order_id", "id"):
            with self.subTest(field=field):
                _, verdict = self.output([
                    self.row("filled", 100, live=True, shares=10, **{field: "stable-identity"}),
                    self.summary(80, live={"halted": []}, filled=1, orders=1),
                    self.row("live_fill_update", 10, live=True, order_id="venue-order",
                             resolved_result="refused", shares=0, retracted_shares=10,
                             **{field: "stable-identity"}),
                ])
                self.assertNotIn("it IS trading", verdict)
                self.assertIn("no fills currently recorded", verdict)

    def test_multiple_identifiers_merge_order_histories(self):
        _, verdict = self.output([
            self.row("filled", 120, live=True, client_order_id="client-order", shares=10),
            self.row("filled", 100, live=True, order_id="venue-order", shares=10),
            self.summary(80, live={"halted": []}, filled=1, orders=1),
            self.row("live_fill_update", 10, live=True, order_id="venue-order",
                     client_order_id="client-order", resolved_result="refused", shares=0,
                     retracted_shares=10),
        ])
        self.assertNotIn("it IS trading", verdict)

    def test_new_fill_update_increments_older_summary_once(self):
        _, verdict = self.output([
            self.row("pending", 100, live=True, order_id="delayed-order"),
            self.summary(80, live={"halted": []}, filled=0, orders=1),
            self.row("live_fill_update", 15, live=True, order_id="delayed-order",
                     resolved_result="filled", shares=5),
            self.row("live_fill_update", 10, live=True, order_id="delayed-order",
                     resolved_result="filled", shares=5),
        ])
        self.assertIn("it IS trading: 1 recorded fill(s)", verdict)

    def test_stale_status_is_unknown_despite_recent_fill(self):
        for halted in ([], ["old cause"]):
            with self.subTest(halted=halted):
                _, verdict = self.output([
                    self.summary(500, live={"halted": halted}),
                    self.row("filled", 5, live=True),
                ])
                self.assertIn("status is UNKNOWN", verdict)
                self.assertNotIn("IS HALTED", verdict)

    def test_resume_does_not_refresh_another_stale_cause(self):
        _, verdict = self.output([
            self.summary(500, live={"halted": ["cause A", "cause B"]}),
            self.row("resume", 10, reason="cause B"),
        ])
        self.assertIn("status is UNKNOWN", verdict)
        self.assertNotIn("IS HALTED", verdict)

    def test_incomplete_status_never_clears_fresh_recorded_halt(self):
        for live in ({}, {"halted": None}, {"halted": False}, {"halted": "cause"},
                     {"halted": ["cause", None]}, True):
            with self.subTest(live=live):
                _, verdict = self.output([
                    self.row("skipped", 50, reason="live halted", detail="active cause"),
                    self.summary(live=live),
                ])
                self.assertIn("IS HALTED", verdict)
                self.assertIn("active cause", verdict)

    def test_cumulative_halt_skips_without_current_evidence_are_unknown(self):
        _, verdict = self.output([self.summary(live={"counts": {"pauses ended": 9}})])
        self.assertIn("status is UNKNOWN", verdict)
        self.assertNotIn("IS HALTED", verdict)

    def test_future_status_and_malformed_resume_cannot_establish_recovery(self):
        _, verdict = self.output([
            self.summary(-10, live={"halted": []}),
            self.row("resume", 5, reason=["not a reason"]),
        ])
        self.assertIn("status is UNKNOWN", verdict)

    def test_historical_error_does_not_override_later_paper_fill(self):
        _, verdict = self.output([
            self.row("error", 100, reason="old transient failure"),
            self.summary(),
            self.row("filled", 5),
        ], live=False)
        self.assertIn("it IS trading", verdict)
        self.assertNotIn("FAILING", verdict)

    def test_latest_order_error_is_described_as_an_observation(self):
        _, verdict = self.output([
            self.summary(live={"halted": []}),
            self.row("error", 5, reason="failed request construction"),
        ])
        self.assertIn("latest order ended with an error", verdict)
        self.assertNotIn("orders are FAILING", verdict)

    def test_pre_restart_halt_is_not_current_state(self):
        _, verdict = self.output([
            self.row("skipped", 100, reason="live halted", detail="old process cause"),
            self.row("start", 60, live=True, code="restart", settings={}),
            self.summary(live={"halted": []}),
        ])
        self.assertNotIn("IS HALTED", verdict)
        self.assertNotIn("old process cause", verdict)


class ScopedJournal(unittest.TestCase):
    """sniper_check.jsonl_scoped (2026-10-27): the reports read the journal line by line and keep
    only their scope - the whole file as dicts got live_check/live_reconcile killed on a 2 GB VPS."""

    def journal(self, rows, compact=True):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = pathlib.Path(tmp.name) / "j.jsonl"
        sep = (",", ":") if compact else None
        path.write_text("".join(json.dumps(r, separators=sep) + "\n" for r in rows) + "not json\n",
                        encoding="utf-8")
        return path

    ROWS = [{"wall": 1, "result": "start", "code": "a"}, {"wall": 2, "result": "skipped", "reason": "x"},
            {"wall": 3, "result": "start", "code": "b"}, {"wall": 4, "result": "book_trace", "flow": [1] * 50},
            {"wall": 5, "result": "filled", "order_id": "0x1", "live": True}, {"wall": 6, "result": "summary"}]

    def test_last_start_keeps_only_the_current_run_and_drops_book_traces(self):
        for compact in (True, False):
            rows, total, start = why.sc.jsonl_scoped(self.journal(self.ROWS, compact), last_start=True)
            self.assertEqual([r["wall"] for r in rows], [3, 5, 6])
            self.assertEqual((total, start["code"]), (7, "b"))

    def test_since_wall_keeps_the_window_and_still_reports_the_last_start(self):
        rows, _total, start = why.sc.jsonl_scoped(self.journal(self.ROWS), since_wall=4.5)
        self.assertEqual([r["wall"] for r in rows], [5, 6])
        self.assertEqual(start["code"], "b")

    def test_need_reads_only_matching_lines_and_nothing_dropped_reads_all(self):
        rows, _total, _start = why.sc.jsonl_scoped(self.journal(self.ROWS), need='"order_id"')
        self.assertEqual([r["wall"] for r in rows], [5])
        rows, _total, _start = why.sc.jsonl_scoped(self.journal(self.ROWS), drop=())
        self.assertEqual(len(rows), 6)
        self.assertEqual(why.sc.jsonl_scoped(pathlib.Path("/nonexistent/j.jsonl")), ([], 0, None))


if __name__ == "__main__":
    unittest.main()
