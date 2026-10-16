#!/usr/bin/env python3
"""exec_eval.py (execution policies A/B/C/D on identical signals) and the
shadow's fair-value-cap quote it reads. No network.

    python tests_exec_eval.py
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import pathlib
import sys
import tempfile
import time
import unittest
from decimal import Decimal as D

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_eval as E  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402


class ShadowFairCap(I.PaperIOCBase):
    """The shadow prices the same order at the fair-value cap (FOK and what FAK
    keeps), with the order's dollars and with whole shares x the fair cap."""

    def test_the_fair_cap_quote(self):
        import tests_sniper_live as T
        from paper_trade import MarketRules
        book = T.live_book(asks=((0.50, 4), (0.55, 100)), bids=((0.49, 50),))
        rules = MarketRules(T.COND, D("0"), 1, min_order_size=D("5"), tick_size=D("0.01"),
                            up_token_id=T.UP, down_token_id=T.DN)
        row = self.sn._shadow_fill(book.view(T.UP), T.UP, 5.2, 0.52, rules, 0.56, 0.0, None, None, (0.58, 5.8))
        self.assertIn("miss", row)                                # FOK at 0.52: only $2 there
        self.assertAlmostEqual(row["fak"]["sh"], 4.0, places=6)   # FAK keeps the 4 at 0.50
        fv = row["fv"]
        self.assertEqual(fv["cap"], 0.58)
        self.assertAlmostEqual(fv["same"]["sh"], 4 + 3.2 / 0.55, places=3)    # $5.20 at <= 0.58: FOK full
        self.assertAlmostEqual(fv["own"]["sh"], 4 + 3.8 / 0.55, places=3)     # 10 sh x 0.58 = $5.80
        self.assertNotIn("fv", self.sn._shadow_fill(book.view(T.UP), T.UP, 5.2, 0.52, rules, 0.56, 0.0))


def journal_rows(now):
    """Three submitted orders + the decision rows' shadows, two runs' summaries."""
    rows = [{"wall": now - 900, "result": "start"},
            {"wall": now - 800, "result": "summary", "skips": {"ask not below fair value": 10, "book stale": 2}},
            {"wall": now - 700, "result": "start"},
            {"wall": now - 600, "result": "summary", "skips": {"ask not below fair value": 5}}]
    # 1: the caps coincide (fair limit under ask + 5c): FOK fills - A = B = C = D
    rows += [{"wall": now - 500, "id": "o1", "result": "filled", "side": "UP", "bp": 1.0, "cap": 0.52,
              "fair_limit": 0.52, "amount": 5.2, "condition_id": "c1", "latency_ms": 300, "shares": 10.0,
              "price": 0.50, "fee": 0.0},
             {"wall": now - 499, "id": "o1", "result": "shadow", "side": "UP", "token": "u1", "other_token": "d1",
              "L": {"300": {"sh": 10.4, "px": 0.5, "fee": 0.0, "mk5": 2.0}}}]
    # 2: ask + 5c bound the cap; FOK misses at 0.52, FAK keeps 4; at the fair cap FOK fills
    rows += [{"wall": now - 400, "id": "o2", "result": "refused", "side": "UP", "bp": 2.0, "cap": 0.52,
              "fair_limit": 0.58, "amount": 5.2, "condition_id": "c2", "latency_ms": 300},
             {"wall": now - 399, "id": "o2", "result": "shadow", "side": "UP", "token": "u2", "other_token": "d2",
              "L": {"300": {"miss": "FOK no-fill: only $2.000000 available at or below 0.52",
                            "fak": {"sh": 4.0, "px": 0.5, "fee": 0.0, "mk5": 1.0},
                            "fv": {"cap": 0.58, "same": {"spend": 5.2, "sh": 9.8182, "px": 0.5296, "fee": 0.0},
                                   "own": {"spend": 5.8, "sh": 10.909, "px": 0.5317, "fee": 0.0}}}}}]
    # 3: it would differ but an older build did not price the fair cap: left out of every policy
    rows += [{"wall": now - 300, "id": "o3", "result": "refused", "side": "DOWN", "bp": -2.0, "cap": 0.40,
              "fair_limit": 0.47, "amount": 4.0, "condition_id": "c3", "latency_ms": 300},
             {"wall": now - 299, "id": "o3", "result": "shadow", "side": "DOWN", "token": "d3", "other_token": "u3",
              "L": {"300": {"miss": "FAK no-fill: nothing at or below 0.4"}}}]
    return rows


class Report(unittest.TestCase):
    def run_eval(self, *argv):
        with tempfile.TemporaryDirectory() as tmp:
            now = time.time()
            with open(os.path.join(tmp, "paper_orders_sniper.jsonl"), "w", encoding="utf-8") as fh:
                fh.write("\n".join(json.dumps(r) for r in journal_rows(now)) + "\n")
            with open(os.path.join(tmp, "paper_ledger_settlements.jsonl"), "w", encoding="utf-8") as fh:
                for c, w in (("c1", "UP"), ("c2", "UP"), ("c3", "DOWN")):
                    fh.write(json.dumps({"event": "RESULT_RECEIVED", "condition_id": c, "winner": w}) + "\n")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                E.main(["--dir", tmp, "--min-n", "1", *argv])
            return out.getvalue()

    def test_identical_signals_and_the_four_policies(self):
        text = self.run_eval()
        self.assertIn("REJECTED BEFORE SUBMISSION (exact counters): 17", text)      # 12 + 5, both runs
        self.assertIn("SUBMITTED ORDERS (eligible signals): 3", text)
        self.assertIn("2 identical eligible signals (1 left out", text)
        same = text[text.index("SAME DOLLARS"):text.index("AS SIZED")]
        table = {ln[2]: ln for ln in same.splitlines() if ln[:3] in ("  A", "  B", "  C", "  D") and "vs A" not in ln}
        # A: o1 full (10.4 sh at 0.50, won) + o2's FAK 4 at 0.50 (won); B (FOK): o1 only
        self.assertIn(f"{10.4 * 0.5 + 4 * 0.5:+8.2f}", table["A"])
        self.assertIn(f"{10.4 * 0.5:+8.2f}", table["B"])
        # C/D at the fair cap: o2's $5.20 fills whole (9.8182 sh at 0.5296) - FOK too
        c_pnl = 10.4 * 0.5 + 9.8182 * (1 - 0.5296)
        self.assertIn(f"{c_pnl:+8.2f}", table["C"])
        self.assertIn(f"{c_pnl:+8.2f}", table["D"])
        self.assertIn("B vs A: -100.00c per signal", same)                        # (5.20 - 7.20) / 2 signals

    def test_as_sized_uses_whole_shares_at_the_fair_cap(self):
        text = self.run_eval()
        tail = text[text.index("AS SIZED"):]
        c_line = next(ln for ln in tail.splitlines() if ln.startswith("  C "))
        self.assertIn(f"{10.4 * 0.5 + 10.909 * (1 - 0.5317):+8.2f}", c_line)


if __name__ == "__main__":
    unittest.main()
