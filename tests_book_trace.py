#!/usr/bin/env python3
"""Book trace and honest fill latency (2026-10-16, the operator's request).

    python tests_book_trace.py

- every order sent gets one book_trace row: the executable asks and the depth
  at or under its cap at the BTC tick + 0/50/100/150/250ms and at the venue's
  reply, with the order type, the reply's status and the order id;
- a zero-fill order has an ACK but no fill time (TRIG>FILL reads N/A);
- SNIPER_BOOK_TRACE_MS="" turns the trace off and the orders are unchanged.
The live path (reply trade ids, user_trades from the authenticated user
channel) is tested in tests_latency.LiveFast against the fake HTTP/2 CLOB.
No network, no wallet.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S  # noqa: E402

ASKS = ((0.52, 100.0), (0.53, 40.0), (0.60, 900.0))


def _settings(**env):
    base = {"SNIPER_ONLY": "1", "SNIPER_LATENCY_MS": "200", "SNIPER_SLIP": "0.02", "SNIPER_FAIR": "0",
            "SNIPER_KELLY": "0", "SNIPER_SHADOW_MS": "", "SNIPER_GUARD_DIAG": "0"}
    with patch.dict(os.environ, {**base, **env}):
        return S.Settings()


class View:
    status, asks, bids, best_ask, mid, tick_size = "LIVE", ASKS, ((0.50, 100.0),), 0.52, 0.51, 0.01
    exchange_ts_ms = 1_700_000_000_000

    def __init__(self):
        self.updated_mono = time.monotonic()


class Hub:
    up_token, down_token, condition_id = "up", "down", "cond"

    def __init__(self):
        self.window_end = time.time() + 120
        self.book = types.SimpleNamespace(view=lambda token: View())


class Broker:
    """Fills the first order, matches nothing for the second."""

    def __init__(self):
        self.last_fill = self.last_error = self.last_timing = None
        self.n = 0

    def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None, **kw):
        self.n += 1
        self.last_timing = {"matched_pc": time.perf_counter_ns()}
        if self.n == 1:
            self.last_fill = {"tag": tag, "order_id": f"o-{side}", "shares": 10.0, "average_price": 0.52,
                              "fee": 0.17, "total_cost": 5.37}
            self.last_error = None
            return True
        self.last_fill, self.last_error = None, "FAK no-fill: nothing at or below 0.54"
        return False


def run_two(**env):
    tmp = tempfile.TemporaryDirectory()
    journal = pathlib.Path(tmp.name) / "paper_orders_sniper.jsonl"
    sn = S.Sniper(Hub(), Broker(), settings=_settings(**env), journal_path=str(journal))

    async def go():
        await sn._fire("UP", 1.2, time.monotonic() * 1000.0)
        await sn._fire("DOWN", -1.3, time.monotonic() * 1000.0)
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and any(not t.done() for t in sn._tasks):
            await asyncio.sleep(0.02)
    asyncio.run(go())
    rows = [json.loads(x) for x in journal.read_text(encoding="utf-8").splitlines()]
    summary = sn.summary()
    tmp.cleanup()
    return rows, summary


class PaperBookTrace(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows, cls.summary = run_two()
        cls.orders = [r for r in cls.rows if r.get("result") in ("filled", "refused")]
        cls.traces = {r["id"]: r for r in cls.rows if r.get("result") == "book_trace"}

    def test_one_trace_per_order_at_every_instant_and_the_reply(self):
        self.assertEqual([o["result"] for o in self.orders], ["filled", "refused"])
        self.assertEqual(set(self.traces), {o["id"] for o in self.orders})
        for o in self.orders:
            t = self.traces[o["id"]]
            self.assertEqual(list(t["points"]), ["0", "50", "100", "150", "250", "response"])
            self.assertEqual(t["order_type"], o.get("order_type", "FAK"))
            self.assertEqual(t["source"], "SIMULATED")
            self.assertEqual(t["cap"], o["cap"])
            for name, pt in t["points"].items():
                self.assertEqual(pt["asks"][0], [0.52, 100.0], name)
                self.assertEqual(pt["bid"], 0.50)
                self.assertEqual(pt["book_ts"], View.exchange_ts_ms)
                under = [(p, q) for p, q in ASKS if p <= o["cap"] + 1e-9]
                self.assertAlmostEqual(pt["depth_cap_sh"], sum(q for _p, q in under))
                self.assertAlmostEqual(pt["depth_cap_usd"], sum(p * q for p, q in under), places=4)
            self.assertIn("at_ms", t["points"]["response"])

    def test_the_reply_status_and_order_id(self):
        filled, missed = (self.traces[o["id"]] for o in self.orders)
        self.assertEqual((filled["kind"], filled["order_id"]), ("FILLED", "o-UP"))
        self.assertEqual(missed["kind"], "NO_FILL")
        self.assertNotIn("order_id", missed)
        self.assertIn("FAK no-fill", missed["error"])

    def test_a_zero_fill_has_an_ack_but_no_fill_time(self):
        filled, missed = self.orders
        self.assertIn("trigger_to_fill", filled["lat"])
        self.assertEqual(filled["lat_fill_source"], "SIMULATED")
        self.assertIn("trigger_to_ack", missed["lat"])
        self.assertNotIn("trigger_to_fill", missed["lat"])
        self.assertNotIn("ack_to_fill", missed["lat"])
        self.assertNotIn("lat_fill_source", missed)
        lat = self.summary["exec_latency"]
        self.assertEqual(lat["trigger_to_ack"]["n"], 2)
        self.assertEqual(lat["trigger_to_fill"]["n"], 1)        # only the order that filled

    def test_off_switch_writes_no_trace_and_the_same_orders(self):
        rows, _ = run_two(SNIPER_BOOK_TRACE_MS="")
        self.assertFalse([r for r in rows if r.get("result", "").startswith("book_trace")])
        off = [r for r in rows if r.get("result") in ("filled", "refused")]
        keys = ("side", "result", "shares", "price", "fee", "cost", "cap", "amount")
        self.assertEqual([{k: o.get(k) for k in keys} for o in off],
                         [{k: o.get(k) for k in keys} for o in self.orders])


class FillLatencyMath(unittest.TestCase):
    def test_with_fill_drops_t7_for_zero_shares(self):
        import exec_latency as X
        lat = {"trigger_to_ack": 300.0, "trigger_to_fill": 300.0, "ack_to_fill": 0.0}
        self.assertEqual(X.with_fill(lat, t_ms=0, t0=None, t1=None, seen_mono=None, filled=False),
                         {"trigger_to_ack": 300.0})
        self.assertEqual(X.with_fill({"trigger_to_ack": 300.0}, t_ms=0, t0=None, t1=None, seen_mono=None),
                         {"trigger_to_ack": 300.0, "trigger_to_fill": 300.0, "ack_to_fill": 0.0})

    def test_the_dashboard_reads_na_without_a_fill(self):
        from dashboard.sniper_layout import _latency_body
        rows = _latency_body({"trigger_to_ack": {"n": 3, "p50": 300.0, "last": 300.0}}, 90)
        text = "\n".join("".join(r[0] if isinstance(r, tuple) else getattr(r, "text", str(r)) for r in row)
                         if isinstance(row, list) else str(row) for row in rows)
        line = next((ln for ln in text.splitlines() if "TRIG>FILL" in ln), "")
        self.assertIn("N/A", line, text)


if __name__ == "__main__":
    unittest.main(verbosity=1)
