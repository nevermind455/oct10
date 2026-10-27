#!/usr/bin/env python3
"""refresh_delays.py: the paper delay sample rebuilt from the live journal (2026-10-25).

The shipped sample is the canary of 2026-10-04..06 (p90 480ms); live replies on 10-22 ran
p90 966ms. The tool keeps the recent orders paper's own reader takes, strips everything but
the venue answer and the reply time (no ids, prices, amounts, times or addresses), refuses
a sample paper would refuse, and never touches the journal. Temp files only, no network.

    python tests_refresh_delays.py
"""
from __future__ import annotations

import contextlib
import io
import json
import pathlib
import sys
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import refresh_delays as R  # noqa: E402
import sniper_mode as S  # noqa: E402

ORDER_KEYS = {"live", "kind", "result", "t_wire_to_reply_ms"}


def order(wall, ms, kind="NO_MATCH", result="refused", **extra):
    return {"wall": wall, "live": True, "kind": kind, "result": result, "t_wire_to_reply_ms": ms,
            "order_id": "0x" + "ab" * 32, "token": "123456789", "price": 0.53, "amount": 3.18,
            "client_order_id": "4471", "condition": "0x" + "cd" * 32, **extra}


def journal(tmp, rows, extra_lines=()):
    path = pathlib.Path(tmp) / "live_sniper.jsonl"
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
        for line in extra_lines:
            fh.write(line + "\n")
    return path


def run(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = R.main(list(argv))
    return code, out.getvalue() + err.getvalue()


class RefreshDelays(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        now = time.time()
        self.recent = [order(now - 3600 + i, 300.0 + 10 * i, *(("MATCHED", "filled") if i % 4 == 0 else
                                                                ("NO_MATCH", "refused"))) for i in range(40)]
        self.old = [order(now - 10 * 86400 + i, 150.0) for i in range(50)]          # outside 7 days
        self.noise = [
            order(now - 60, 999.0, result="live_fill_update"),                     # not one row per order
            order(now - 60, 999.0, kind="AMBIGUOUS", result="pending"),            # no venue answer
            order(now - 60, "n/a"),                                                # no reply time
            {"wall": now - 100, "result": "summary", "live": {"clock": {"rtt_ms_p50": 21.5}}, "orders": 12},
            {"wall": now - 9 * 86400, "result": "summary", "live": {"clock": {"rtt_ms_p50": 90.0}}},
            {"wall": now - 50, "result": "skip", "reason": "cooldown"},
        ]
        self.path = journal(self.tmp.name, self.old + self.recent + self.noise, extra_lines=("{not json",))
        self.out = pathlib.Path(self.tmp.name) / "data" / "live_delays_test.jsonl"
        self.current = ROOT / "data" / "live_delays_2026-10-04.jsonl"

    def refresh(self, *extra):
        return run("--journal", str(self.path), "--out", str(self.out), "--current", str(self.current), *extra)

    def test_recent_orders_only_in_the_shipped_format(self):
        code, text = self.refresh()
        self.assertEqual(code, 0, text)
        rows = [json.loads(line) for line in self.out.read_text(encoding="utf-8").splitlines()]
        orders = [r for r in rows if r.get("live") is True]
        self.assertEqual(len(orders), 40)                                        # the last 7 days only
        for r in orders:
            self.assertEqual(set(r), ORDER_KEYS)                                 # no ids, prices, times
        summaries = [r for r in rows if r.get("result") == "summary"]
        self.assertEqual(summaries, [{"result": "summary", "live": {"clock": {"rtt_ms_p50": 21.5}}}])
        delays, one_way = S.live_delays(self.out)                               # exactly as paper reads it
        self.assertEqual(len(delays), 40)
        self.assertAlmostEqual(one_way, 21.5 / 2.0)
        self.assertAlmostEqual(min(delays), 300.0 - 10.75)
        self.assertIn("SNIPER_DELAY_JOURNAL=", text)

    def test_the_shipped_file_has_the_same_shape(self):
        shipped = [json.loads(line) for line in self.current.read_text(encoding="utf-8").splitlines()]
        for r in shipped:
            self.assertTrue(set(r) == ORDER_KEYS or r.get("result") == "summary", r)

    def test_too_few_orders_is_refused_and_nothing_is_written(self):
        code, text = self.refresh("--days", "0.0001")                           # ~9 seconds: none
        self.assertEqual(code, 1)
        self.assertIn("REFUSED", text)
        self.assertFalse(self.out.exists())

    def test_a_dry_run_writes_nothing(self):
        code, text = self.refresh("--dry-run")
        self.assertEqual(code, 0, text)
        self.assertFalse(self.out.exists())
        self.assertIn("40 orders", text)

    def test_the_journal_is_never_touched(self):
        before = self.path.read_bytes()
        self.refresh()
        self.assertEqual(self.path.read_bytes(), before)

    def test_a_missing_journal_or_bad_days_fail_cleanly(self):
        self.assertEqual(run("--journal", str(self.path) + ".missing", "--out", str(self.out))[0], 1)
        for bad in ("0", "-1", "nan"):
            self.assertEqual(self.refresh("--days", bad)[0], 2, bad)
        self.assertFalse(self.out.exists())


if __name__ == "__main__":
    unittest.main()
