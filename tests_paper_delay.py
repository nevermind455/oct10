"""Paper orders that wait what live orders really waited (SNIPER_DELAY_SOURCE=live).

    python -m unittest -v tests_paper_delay
"""
import json
import os
import tempfile
import unittest
from unittest.mock import patch

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S


def journal(rows):
    fh = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False, encoding="utf-8")
    for r in rows:
        fh.write(json.dumps(r) + "\n")
    fh.close()
    return fh.name


def order(kind, result, reply_ms, **kw):
    return {"live": True, "kind": kind, "result": result, "t_wire_to_reply_ms": reply_ms, **kw}


SUMMARY = {"result": "summary", "live": {"clock": {"rtt_ms_p50": 24.0}}}


class LiveDelays(unittest.TestCase):
    def test_reads_one_delay_per_answered_order_minus_one_way(self):
        path = journal([SUMMARY,
                        order("MATCHED", "filled", 262.0),
                        order("NO_MATCH", "refused", 112.0),
                        order("MATCHED", "pending", 512.0),
                        order("MATCHED", "live_fill_update", 262.0),     # a FAK's later size: same order
                        {"result": "live_send", "kind": "MATCHED", "t_wire_to_reply_ms": 262.0},
                        order("AMBIGUOUS", "refused", 900.0),            # never answered
                        order("REJECTED", "refused", 40.0),              # not the matching engine
                        order("NO_MATCH", "refused", None)])
        delays, one_way = S.live_delays(path)
        self.assertEqual(one_way, 12.0)
        self.assertEqual(sorted(delays), [100.0, 250.0, 500.0])

    def test_without_a_summary_the_lab_one_way_is_used(self):
        delays, one_way = S.live_delays(journal([order("NO_MATCH", "refused", 113.3)]))
        self.assertEqual(one_way, 13.3)
        self.assertAlmostEqual(delays[0], 100.0)


class PaperSniper(unittest.TestCase):
    def env(self, path, **extra):
        return patch.dict(os.environ, {"SNIPER_DELAY_SOURCE": "live", "SNIPER_DELAY_JOURNAL": path,
                                       "SNIPER_LATENCY_MS": "", **extra})

    def test_each_paper_order_draws_a_real_live_delay(self):
        ms = [100.0 + 10 * i for i in range(40)]
        path = journal([SUMMARY] + [order("NO_MATCH", "refused", m + 12.0) for m in ms])
        with self.env(path):
            sn = S.Sniper(None, None, settings=S.Settings())
        got = [sn.latency_plan() for _ in range(300)]
        self.assertTrue(all(d in ms for d, _ in got))
        self.assertGreater(len({d for d, _ in got}), 20)              # drawn, not one fixed number
        self.assertEqual(got[0][1]["latency_mode"], "live-sampled")
        self.assertEqual(got[0][1]["delay_n"], 40)
        self.assertIn("drawn from 40 live orders", sn.lines()[0])

    def test_too_few_live_orders_refuses_to_start(self):
        path = journal([SUMMARY] + [order("NO_MATCH", "refused", 200.0)] * (S.MIN_LIVE_DELAYS - 1))
        with self.env(path), self.assertRaises(ValueError):
            S.Sniper(None, None, settings=S.Settings())

    def test_missing_journal_refuses_to_start(self):
        with self.env(os.path.join(tempfile.mkdtemp(), "nope.jsonl")), self.assertRaises(ValueError):
            S.Sniper(None, None, settings=S.Settings())

    def test_fixed_latency_and_live_delays_are_exclusive(self):
        with patch.dict(os.environ, {"SNIPER_DELAY_SOURCE": "live", "SNIPER_LATENCY_MS": "200"}), \
                self.assertRaises(ValueError):
            S.Settings()

    def test_default_stays_the_model(self):
        with patch.dict(os.environ, {"SNIPER_DELAY_SOURCE": "", "SNIPER_LATENCY_MS": ""}):
            st = S.Settings()
            sn = S.Sniper(None, None, settings=st)
        d, parts = sn.latency_plan()
        self.assertEqual(parts["latency_mode"], "live-like")
        self.assertAlmostEqual(d, st.net_default_ms + st.hold_ms + st.server_ms)


if __name__ == "__main__":
    unittest.main()
