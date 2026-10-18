#!/usr/bin/env python3
"""Liquidity disappearance attribution - measurement only (2026-10-18).

    python tests_liquidity_attribution.py

  1  Sniper.attribute_window: gross ask reductions/additions under the original
     cap from the book's own states, matched against public BUY prints; feed
     coverage; conservative labels; None (never 0) for what was not observed
  2  Sniper.print_side_check: does a print's side match which book side shrank
  3  feeds.print_log.PrintLog: bounded, non-blocking, persisted, pruned
  4  PolyMarketFeed: every print logged with its receive time; out-of-order
     prints kept as trades; refused prints logged with the reason
  5  Sniper._attribution on a fake hub, and liquidity_report on its rows
No network, no wallet; nothing here sends or changes an order.
"""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S  # noqa: E402

A = S.Sniper.attribute_window
CAP = 0.28


def st(t, asks, bids=None):
    return (t, dict(bids or {0.20: 100.0}), None if asks is None else dict(asks))


def pr(t, price, size, side="BUY"):
    return {"recv_mono": t, "price": price, "size": size, "side": side}


GAP = (None, None, None)


class Attribution(unittest.TestCase):
    def test_01_a_verified_buy_explains_the_drop(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [pr(0.04, 0.25, 100.0)], CAP, 0, 0.1)
        self.assertEqual((w["observed_gross_reductions"], w["matched_trade_depletion_estimate"],
                          w["unexplained_reduction_estimate"], w["trade_attribution_ratio"]), (100.0, 100.0, 0.0, 1.0))
        self.assertEqual((w["attribution_classification"], w["feed_coverage_status"]),
                         ("TRADE_DOMINATED", "COMPLETE_OBSERVED"))

    def test_02_a_drop_with_no_trades_is_non_trade_dominated_not_a_proven_cancel(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [], CAP, 0, 0.1)
        self.assertEqual((w["unexplained_reduction_estimate"], w["attribution_classification"]),
                         (100.0, "NON_TRADE_DOMINATED"))

    def test_03_new_liquidity_during_the_drop_is_counted_apart(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {0.26: 50.0})], [], CAP, 0, 0.1)
        self.assertEqual((w["observed_gross_reductions"], w["observed_gross_additions"], w["net_depth_change"]),
                         (100.0, 50.0, -50.0))
        self.assertEqual(w["levels"], {"0.25": [100.0, 0.0], "0.26": [0.0, 50.0]})

    def test_04_several_trades_on_one_level(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [pr(0.01, 0.25, 30.0), pr(0.02, 0.25, 30.0),
                                                     pr(0.03, 0.25, 40.0)], CAP, 0, 0.1)
        self.assertEqual((w["relevant_public_trade_volume"], w["trade_event_count"], w["trade_attribution_ratio"]),
                         (100.0, 3, 1.0))

    def test_05_one_trade_over_several_book_updates(self):
        w = A([st(0, {0.25: 100.0}), st(0.02, {0.25: 60.0}), st(0.04, {})], [pr(0.01, 0.25, 100.0)], CAP, 0, 0.1)
        self.assertEqual((w["observed_gross_reductions"], w["book_event_count"], w["matched_trade_depletion_estimate"]),
                         (100.0, 2, 100.0))

    def test_06_07_identical_prints_are_all_kept_and_matching_never_exceeds_the_reductions(self):
        # a replay and two legitimate identical trades look the same: none is deleted,
        # and the match is capped at what the book really lost
        p = pr(0.01, 0.25, 100.0)
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [p, dict(p)], CAP, 0, 0.1)
        self.assertEqual((w["trade_event_count"], w["relevant_public_trade_volume"],
                          w["matched_trade_depletion_estimate"]), (2, 200.0, 100.0))

    def test_08_a_print_received_after_the_window_does_not_count_strictly(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [pr(0.12, 0.25, 100.0)], CAP, 0, 0.1)
        self.assertEqual((w["relevant_public_trade_volume"], w["attribution_classification"]),
                         (0.0, "NON_TRADE_DOMINATED"))

    def test_09_10_at_the_cap_counts_above_does_not(self):
        cap = round(0.23 + 0.05, 4)
        w = A([st(0, {0.28: 50.0, 0.29: 50.0}), st(0.05, {})],
              [pr(0.01, 0.04 + 0.24, 50.0), pr(0.02, 0.29, 50.0)], cap, 0, 0.1)
        self.assertEqual((w["initial_depth_le_cap"], w["observed_gross_reductions"],
                          w["relevant_public_trade_volume"]), (50.0, 50.0, 50.0))

    def test_12_a_sell_print_does_not_consume_asks(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [pr(0.01, 0.25, 100.0, "SELL")], CAP, 0, 0.1)
        self.assertEqual((w["relevant_public_trade_volume"], w["sell_side_volume"], w["attribution_classification"]),
                         (0.0, 100.0, "NON_TRADE_DOMINATED"))

    def test_13_14_a_gap_or_resync_makes_it_inconclusive(self):
        w = A([st(0, {0.25: 100.0}), (0.02, None, None), st(0.03, {0.25: 10.0}), st(0.05, {})], [], CAP, 0, 0.1)
        self.assertEqual((w["feed_coverage_status"], w["attribution_classification"], w["resets"]),
                         ("GAPPED", "INCONCLUSIVE", 1))
        self.assertEqual(w["observed_gross_reductions"], 10.0)        # never across the gap
        g = A([(0.0, None, None), st(0.05, {})], [], CAP, 0.01, 0.1)
        self.assertEqual(g["feed_coverage_status"], "GAPPED")

    def test_partial_history_and_no_history(self):
        w = A([st(0.05, {0.25: 10.0})], [], CAP, 0, 0.1)
        self.assertEqual((w["feed_coverage_status"], w["attribution_classification"]), ("PARTIAL", "INCONCLUSIVE"))
        n = A([], [], CAP, 0, 0.1)
        self.assertEqual((n["feed_coverage_status"], n["attribution_classification"]), ("UNKNOWN", "INCONCLUSIVE"))
        self.assertNotIn("observed_gross_reductions", n)               # unknown, never a zero

    def test_unknown_side_prints_make_it_inconclusive(self):
        w = A([st(0, {0.25: 100.0}), st(0.05, {})], [pr(0.01, 0.25, 50.0, None)], CAP, 0, 0.1)
        self.assertEqual((w["unknown_side_volume"], w["attribution_classification"]), (50.0, "INCONCLUSIVE"))

    def test_mixed_and_the_thresholds(self):
        states = [st(0, {0.25: 100.0}), st(0.05, {})]
        prints = [pr(0.01, 0.25, 50.0)]
        self.assertEqual(A(states, prints, CAP, 0, 0.1)["attribution_classification"], "MIXED")
        self.assertEqual(A(states, prints, CAP, 0, 0.1, hi=0.5)["attribution_classification"], "TRADE_DOMINATED")

    def test_17_20_snapshot_only_and_zero_initial_depth(self):
        w = A([st(0, {0.30: 100.0})], [], CAP, 0, 0.1)
        self.assertEqual((w["initial_depth_le_cap"], w["book_event_count"], w["trade_attribution_ratio"],
                          w["attribution_classification"]), (0.0, 0, None, "NO_DEPLETION"))


class SideCheck(unittest.TestCase):
    def test_buy_prints_followed_by_ask_drops_and_sells_by_bid_drops(self):
        states = [st(0, {0.25: 100.0}, {0.24: 80.0}), st(0.02, {0.25: 60.0}, {0.24: 80.0}),
                  st(0.04, {0.25: 60.0}, {0.24: 30.0})]
        tally = S.Sniper.print_side_check(states, [pr(0.01, 0.25, 40.0, "BUY"), pr(0.03, 0.24, 50.0, "SELL"),
                                                   pr(0.5, 0.25, 1.0, "BUY")])
        self.assertEqual(tally["BUY"], {"ask_drop": 1, "bid_drop": 0, "neither": 0, "no_state": 1})
        self.assertEqual(tally["SELL"], {"ask_drop": 0, "bid_drop": 1, "neither": 0, "no_state": 0})


class PrintLogTests(unittest.TestCase):
    def test_written_bounded_and_pruned(self):
        from feeds.print_log import PrintLog
        with tempfile.TemporaryDirectory() as d:
            old = pathlib.Path(d) / "market_prints-20000101.jsonl"
            old.write_text("{}\n")
            log = PrintLog(d, max_queue=2, keep_days=3)
            log.record("trade", asset_id="up", price="0.25", size="10", side="BUY", venue_ms=1)
            log.record("connect", conn=1, tokens=["up"])
            log.record("trade", asset_id="up")                         # the queue is full: dropped, counted
            self.assertEqual(log.flush(), 2)
            self.assertEqual(log.stats()["dropped"], 1)
            files = list(pathlib.Path(d).glob("market_prints-*.jsonl"))
            self.assertEqual(len(files), 1)                            # the 2000 file was pruned
            lines = [json.loads(x) for x in files[0].read_text().splitlines()]
            self.assertEqual([(x["v"], x["seq"], x["t"]) for x in lines], [(1, 1, "trade"), (1, 2, "connect")])
            self.assertEqual(lines[0]["price"], "0.25")                # raw strings kept for Decimal use
            self.assertIn("mono_ns", lines[0])

    def test_a_write_failure_is_counted_never_raised(self):
        from feeds.print_log import PrintLog
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name                                              # a file where the folder should be
        try:
            log = PrintLog(path)
            log.record("trade", asset_id="up")
            log.flush()
            self.assertEqual(log.stats()["errors"], 1)
        finally:
            os.unlink(path)


class FeedPrints(unittest.TestCase):
    def feed(self, log):
        from feeds.book import BookState
        from feeds.poly_market import PolyMarketFeed
        book = BookState()
        book.connected = True
        book.set_active(["A", "B"])
        now = int(time.time() * 1000)
        for token in book.active:
            book.apply_snapshot(token, [(0.4, 10)], [(0.6, 10)], ts_ms=now)
        f = PolyMarketFeed(book=book, print_log=log)
        f.set_tokens(["A", "B"])
        return f, now

    def test_prints_logged_kept_out_of_order_and_refusals_explained(self):
        from feeds.print_log import PrintLog
        with tempfile.TemporaryDirectory() as d:
            log = PrintLog(d)
            f, now = self.feed(log)
            send = lambda **m: f._handle(json.dumps({"event_type": "last_trade_price", **m}))  # noqa: E731
            send(asset_id="A", timestamp=now + 5, price="0.5", size="7", side="BUY")
            send(asset_id="A", timestamp=now + 1, price="0.5", size="3", side="SELL")   # older stamp
            send(asset_id="A", price="0.5", size="9", side="BUY")                        # no timestamp
            send(asset_id="Z", timestamp=now + 6, price="0.5", size="1", side="BUY")     # other token
            got = f.trades_received("A", 0, time.monotonic() + 1)
            self.assertEqual([(x["size"], x["side"]) for x in got], [(7.0, "BUY"), (3.0, "SELL")])
            self.assertEqual(f.last_trade_price["A"], 0.5)
            self.assertEqual(f.trades_received("B", 0, time.monotonic() + 1), [])     # the other outcome
            log.flush()
            rows = [json.loads(x) for p in pathlib.Path(d).glob("*.jsonl") for x in p.read_text().splitlines()]
            self.assertEqual([(r["accepted"], (r["why"] or "")[:12]) for r in rows],
                             [(True, ""), (True, "out of order"), (False, "invalid pric"), (False, "inactive tok")])
            self.assertTrue(all(r["t"] == "trade" and "recv_mono_ns" in r for r in rows))


class EndToEnd(unittest.TestCase):
    def test_attribution_on_a_fake_hub_and_the_report(self):
        t0 = 1000.0
        hist = {"up": [(1, t0 - 0.5, 0, 1, {0.2: 50.0}, {0.25: 300.0, 0.27: 100.0}, 0.01),
                       (2, t0 + 0.03, 0, 2, {0.2: 50.0}, {0.25: 200.0, 0.27: 100.0}, 0.01),
                       (3, t0 + 0.12, 0, 3, {0.2: 50.0}, {0.27: 100.0}, 0.01),
                       (4, t0 + 0.30, 0, 4, {0.2: 50.0}, {0.30: 80.0}, 0.01)]}
        prints = [{"ts": 0, "recv_mono": t0 + 0.02, "price": 0.25, "size": 100.0, "side": "BUY"}]
        hub = types.SimpleNamespace(book=types.SimpleNamespace(_hist=hist), up_token="up", down_token="down",
                                    condition_id="c", window_end=time.time() + 60,
                                    market=types.SimpleNamespace(trades_received=lambda tok, lo, hi: [
                                        x for x in prints if lo < x["recv_mono"] <= hi]))
        with unittest.mock.patch.dict(os.environ, {"SNIPER_SHADOW_MS": "", "SNIPER_GUARD_DIAG": "0"}):
            sn = S.Sniper(hub, types.SimpleNamespace(), settings=S.Settings(), journal_path=None)
        att = sn._attribution("up", 0.28, t0, t0 + 0.4, 0.0)
        span = att["spans"]["signal-reply"]
        self.assertEqual((span["observed_gross_reductions"], span["matched_trade_depletion_estimate"],
                          span["unexplained_reduction_estimate"]), (400.0, 100.0, 300.0))
        # 100 of 400 matched = 0.25 <= lo 0.3
        self.assertEqual((span["attribution_classification"], att["classification"]),
                         ("NON_TRADE_DOMINATED", "NON_TRADE_DOMINATED"))
        self.assertIn("0-50", att["spans"])
        self.assertEqual(att["spans"]["0-50"]["attribution_classification"], "TRADE_DOMINATED")
        self.assertEqual(att["side_check"]["BUY"]["ask_drop"], 1)
        self.assertEqual(att["prints"][0][2:], [0.25, 100.0, "BUY"])
        with tempfile.TemporaryDirectory() as d:
            j = pathlib.Path(d) / "j.jsonl"
            j.write_text("\n".join(json.dumps(r) for r in (
                {"wall": time.time(), "result": "refused", "id": "o1", "dec_quote_age_ms": 3.0, "dec_spread": 0.01},
                {"wall": time.time(), "result": "book_trace", "id": "o1", "side": "UP", "token": "up", "cap": 0.28,
                 "amount": 2.8, "kind": "NO_FILL", "ask_seen": 0.25,
                 "points": {"0": {"depth_cap_sh": 400.0}, "response": {"depth_cap_sh": 0.0, "at_ms": 400.0,
                                                                       "asks": [[0.30, 80.0]]}},
                 "attribution": att},
                {"wall": time.time(), "result": "book_trace", "id": "o2", "side": "DOWN", "token": "dn", "cap": 0.5,
                 "kind": "FILLED", "points": {}})) + "\n")
            out = subprocess.run([sys.executable, str(ROOT / "liquidity_report.py"), str(j), "--csv",
                                  str(pathlib.Path(d) / "o.csv")], capture_output=True, text=True, timeout=60)
            self.assertEqual(out.returncode, 0, out.stderr)
            self.assertIn("2 traced orders", out.stdout)
            self.assertIn("without (historical, NOT zero trades): 1", out.stdout)
            self.assertIn("NO_TELEMETRY", out.stdout)
            self.assertIn("SENSITIVITY", out.stdout)
            self.assertTrue((pathlib.Path(d) / "o.csv").exists())


if __name__ == "__main__":
    import unittest.mock  # noqa: F401
    unittest.main(verbosity=1)
