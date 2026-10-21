#!/usr/bin/env python3
"""Offline regressions for compact-feed replay and locked settlement cash."""
from __future__ import annotations

import asyncio
import gzip
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import testing_env  # noqa: F401 - never read deployment settings/credentials
import replay_ab as R
from accounting.ledger import Ledger
from accounting.resolution import RESOLVED, Resolution
from feeds.book import BookState


class ReplayWeaknessFixes(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="replay_regression_")
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)

    def recording(self, rows, name="fixture"):
        path = self.root / f"lab_{name}.jsonl.gz"
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
        return str(path)

    def market_fixture(self):
        # A minute of tiny changes warms the REAL volatility model. A later
        # 0.8bp burst crosses the frozen 0.5bp threshold. Polymarket's quote
        # already exists before the move; no fair/Kelly/connection gates are
        # disabled or mocked. Only venue network lookup is absent.
        w = 1_800_000_000
        info = {"window_start": w, "condition_id": "fixture-condition",
                "up_token_id": "fixture-up", "down_token_id": "fixture-down"}
        rows = [["round", 0, w * 10**9, info],
                ["aid", 0, w * 10**9, "fixture-up", 0, "UP"],
                ["aid", 0, w * 10**9, "fixture-down", 1, "DOWN"]]
        for i in range(611):
            wall = w + i / 10
            ns = round(wall * 10**9)
            if i % 10 == 0:
                for token_index in (0, 1):
                    rows.append(["pmb", ns, ns, int(wall * 1000), token_index,
                                 [[0.49, 1000]], [[0.51, 1000]]])
            mid = 60000.0 + (0.005 if i % 2 else 0.0)
            rows.append(["xb", ns, ns, R.COINBASE_CODE, wall * 1000 - 80, mid - 0.01, mid + 0.01])
        for dt, mid in ((61.1, 60001.2), (61.2, 60004.8), (61.4, 60004.8), (62.0, 60004.8),
                        (302.0, 60004.8)):
            wall = w + dt
            ns = round(wall * 10**9)
            rows.append(["xb", ns, ns, R.COINBASE_CODE, wall * 1000 - 80, mid - 0.01, mid + 0.01])
        return self.recording(rows), {info["condition_id"]: {"payouts": {"fixture-up": 1, "fixture-down": 0},
                                                             "winner": "UP"}}

    def replay(self, path, results, **kwargs):
        rounds = R.scan_rounds([path])
        # Keep run journals inside the test directory so the regression leaves
        # no durable trading state and cannot read an existing replay journal.
        original = tempfile.mkdtemp
        with patch.object(R.tempfile, "mkdtemp", side_effect=lambda **kw: original(dir=self.root, **kw)):
            return asyncio.run(R.run_variant([path], rounds, results, with_pm=kwargs.pop("with_pm", False),
                pm_cfg=R.PMM.PMConfig.from_env({}, global_max_buy_price=0.95, global_max_side_shares=200),
                latency_ms=170, start_cash=100, label="fixture", **kwargs))

    def test_default_multisocket_current_strategy_has_signals_orders_and_fills(self):
        path, results = self.market_fixture()
        # Network is forbidden even if a future replay accidentally starts a
        # rule prefetch/resolver. Current rules are warmed from recorded-model
        # constants and all decision/fill/accounting code is production code.
        with patch("http_pool.get", side_effect=AssertionError("replay must stay offline")), \
             patch.dict(R.os.environ, {"SNIPER_COINBASE_CONNS": "2"}):
            a = self.replay(path, results)
            b = self.replay(path, results, with_pm=True)
        self.assertGreater(a["sniper_counters"].get("signals", 0), 0)
        self.assertGreater(a["sniper_orders"], 0, a["sniper_counters"])
        self.assertGreater(a["sniper_fills"], 0, a["sniper_rows"])
        filled = [r for r in a["sniper_rows"] if r["result"] == "filled"]
        self.assertTrue(all(r["fair"] is not None and r["fair_anchor"] == "before move" for r in filled), filled)
        self.assertTrue(all(r["kelly_held_shares"] is not None and r["fee_rate"] == 0.07 for r in filled), filled)
        self.assertEqual(a["trigger_status"], {"tick": 616})
        self.assertNotIn("coinbase malformed", a["sniper_counters"])
        self.assertEqual(a["input_diagnostics"]["sequence_source:synthetic"], 616)
        self.assertGreater(a["total_pnl"], 0)
        self.assertEqual(a["realized_pnl"], 0)
        self.assertGreater(a["unsettled_cost"], 0)
        self.assertAlmostEqual(a["end_available_cash"], 100 - a["capital_deployed"])
        self.assertEqual(R.compare_sniper(a, b)["different_outcome"], 0)
        self.assertGreater(b["sniper_fills"], 0, b["sniper_counters"])

    def test_recorded_sequences_deduplicate_and_legacy_sequences_are_deterministic(self):
        w = 1_800_000_000
        rows = [["xb", i, (w + i / 10) * 10**9, R.COINBASE_CODE, (w + i / 10) * 1000 - 80,
                 60000, 60000.02, seq] for i, seq in enumerate((700, 700, 701))]
        rows.append(["xb", 3, (w + 0.3) * 10**9, R.COINBASE_CODE, None, 60000, 60000.02])
        path = self.recording(rows)
        first = list(R.events([path]))
        self.assertEqual(first, list(R.events([path])))
        self.assertEqual([e[4]["sequence"] for e in first], [700, 700, 701, 702])
        self.assertEqual(first[0][4]["exchange_ms"], w * 1000 - 80)
        self.assertEqual(first[-1][4]["exchange_time_source"], "receive_time_model")
        out = self.replay(path, {})
        self.assertEqual(out["trigger_status"], {"tick": 3, "late": 1})
        self.assertEqual(out["input_diagnostics"]["sequence_source:recorded"], 3)
        self.assertTrue(any("no signals" in warning for warning in out["warnings"]))

    def test_malformed_empty_replay_reports_failure_instead_of_quiet_success(self):
        path = self.recording([["xb", 1, 1_800_000_000 * 10**9, R.COINBASE_CODE, None, -1, 60000]])
        out = self.replay(path, {})
        self.assertEqual(out["input_diagnostics"]["malformed_trigger_rows"], 1)
        self.assertTrue(any("no trigger ticks accepted" in warning for warning in out["warnings"]))
        with patch.object(R, "outcomes", side_effect=AssertionError("empty replay must not resolve over network")):
            self.assertEqual(R.main(["--data", str(self.root)]), 1)

    def broker(self, cash=10, redemption_delay=600):
        clock = R.Clock()
        clock.now = 100
        book = BookState(stale_after=10000)
        book.connected = True
        book.set_active(["up", "other"])
        ledger = Ledger(path=str(self.root / "broker-ledger.json"))
        broker = R.ReplayBroker(ledger, clock, book, start_cash=cash, latency_ms=170,
                               redemption_delay_s=redemption_delay)
        with patch("feeds.book.time", clock):
            for token in ("up", "other"):
                self.assertTrue(book.apply_snapshot(token, [[0.49, 100]], [[0.50, 100]], ts_ms=100000,
                                                    require_exchange_ts=False))
        return broker, clock, ledger

    def test_settled_winnings_cannot_fund_an_order_until_redemption(self):
        broker, clock, ledger = self.broker(cash=5.2)
        ledger.record_fill("first", "up", shares=10, price=0.5, fee=0.175, condition_id="one")
        ledger.settle(Resolution("one", RESOLVED, {"up": 1, "down": 0}))
        broker.mark_settled("one", 100)
        clock.now = 699.7
        self.assertAlmostEqual(broker.cash_balance(), 0.025)
        ev, slot = broker._enqueue("UP", 2.5, "other", "down", "two", 1000, 0.5, "sniper", 0.5, fok=False)
        clock.now += 1
        # This request arrived BEFORE redemption even though the next recording
        # message follows it. The matching clock must use actual arrival.
        with patch("feeds.book.time", clock):
            broker.resolve_due()
        self.assertFalse(slot["ok"])
        self.assertIn("collateral", slot["error"])
        self.assertAlmostEqual(broker.cash_balance(), 10.025)
        ev, slot = broker._enqueue("UP", 2.5, "other", "down", "two", 1000, 0.5, "sniper", 0.5, fok=False)
        clock.now += 1
        with patch("feeds.book.time", clock):
            broker.resolve_due()
        self.assertTrue(slot["ok"], slot)

    def test_setup_uses_main_and_preserves_branch_override(self):
        text = (R.ROOT / "setup.sh").read_text(encoding="utf-8")
        self.assertIn('BRANCH="${BRANCH:-main}"', text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
