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



class TradePrints(unittest.TestCase):
    """feeds.poly_market keeps each token's trade prints (venue ms, price, size, side)."""

    def feed(self):
        from feeds.book import BookState
        from feeds.poly_market import PolyMarketFeed
        book = BookState()
        book.connected = True
        book.set_active(["A", "B"])
        now = int(time.time() * 1000)
        for token in book.active:
            book.apply_snapshot(token, [(0.4, 10)], [(0.6, 10)], ts_ms=now)
        feed = PolyMarketFeed(book=book)
        feed.set_tokens(["A", "B"])
        return feed, now

    def test_prints_are_kept_with_size_and_side_and_pruned_with_their_token(self):
        feed, now = self.feed()
        for dt_, price, size, side in ((1, ".45", "5", "BUY"), (2, ".46", "3.5", "SELL"), (3, ".47", None, "BUY")):
            feed._handle(json.dumps({"event_type": "last_trade_price", "asset_id": "A", "timestamp": now + dt_,
                                     "price": price, "size": size, "side": side}))
        got = feed.trades_between("A", now, now + 10)
        self.assertEqual([(t["ts"], t["price"], t["size"], t["side"]) for t in got],
                         [(now + 1, .45, 5.0, "BUY"), (now + 2, .46, 3.5, "SELL"), (now + 3, .47, None, "BUY")])
        self.assertEqual(feed.trades_between("A", now + 2, now + 2)[0]["side"], "SELL")
        self.assertEqual(feed.trades_between("B", now, now + 10), [])
        feed.set_tokens(["B", "C"])
        self.assertEqual(feed.trades_between("A", now, now + 10), [])

    def test_a_rejected_print_is_not_kept(self):
        feed, now = self.feed()
        feed._handle(json.dumps({"event_type": "last_trade_price", "asset_id": "A", "timestamp": now + 5,
                                 "price": ".5", "size": "5", "side": "BUY"}))
        feed._handle(json.dumps({"event_type": "last_trade_price", "asset_id": "A", "timestamp": now + 1,
                                 "price": ".5", "size": "7", "side": "BUY"}))      # out of order: rejected
        self.assertEqual([t["size"] for t in feed.trades_between("A", now, now + 10)], [5.0])


class VenueMatch(unittest.TestCase):
    """Sniper._venue_match: our trade found among the market's prints."""

    def sniper(self, prints):
        hub = Hub()
        hub.market = types.SimpleNamespace(
            trades_between=lambda token, lo, hi: [p for p in prints if lo <= p["ts"] <= hi])
        return S.Sniper(hub, Broker(), settings=_settings(), journal_path=None)

    MINE = [{"trade_id": "t1", "status": "CONFIRMED", "price": 0.45, "size": 5.0, "maker_legs": [[3.0, 0.45], [2.0, 0.44]]}]

    def test_the_earliest_print_is_only_an_unverified_candidate(self):
        prints = [{"ts": 10_050, "price": 0.45, "size": 7.0, "side": "BUY"},     # someone else's size
                  {"ts": 10_080, "price": 0.45, "size": 5.0, "side": "SELL"},    # wrong side
                  {"ts": 10_120, "price": 0.44, "size": 2.0, "side": "BUY"},     # our second leg
                  {"ts": 10_121, "price": 0.45, "size": 3.0, "side": "BUY"},     # our first leg
                  {"ts": 9_000 - 1, "price": 0.45, "size": 5.0, "side": "BUY"}]  # before the window
        out = self.sniper(prints)._venue_match("tok", self.MINE, tick_ms=10_000.0, reply_ms=10_300.0)
        self.assertEqual(out["candidate_first_venue_ms"], 10_120)
        self.assertEqual(out["candidate_after_tick_ms"], 120.0)
        self.assertEqual(out["ack_after_candidate_ms"], 180.0)
        self.assertEqual(out["match_identity"], "UNVERIFIED_PRICE_SIZE")
        self.assertNotIn("match_after_tick_ms", out)
        self.assertEqual((out["match_prints_seen"], out["match_prints_hit"]), (4, 2))

    def test_a_size_and_price_from_different_legs_do_not_match(self):
        prints = [{"ts": 10_100, "price": .44, "size": 3, "side": "BUY"},
                  {"ts": 10_120, "price": .45, "size": 5, "side": None}]
        out = self.sniper(prints)._venue_match("tok", self.MINE, 10_000., 10_300.)
        self.assertEqual(out["match_prints_hit"], 0)
        self.assertNotIn("venue_match_ms", out)

    def test_no_print_no_match_and_failed_trades_are_ignored(self):
        sn = self.sniper([{"ts": 10_100, "price": 0.45, "size": 5.0, "side": "BUY"}])
        failed = [dict(self.MINE[0], status="FAILED")]
        self.assertEqual(sn._venue_match("tok", failed, 10_000.0, 10_300.0), {})
        self.assertEqual(self.sniper([])._venue_match("tok", self.MINE, 10_000.0, 10_300.0),
                         {"match_prints_seen": 0, "match_prints_hit": 0})
        no_feed = S.Sniper(Hub(), Broker(), settings=_settings(), journal_path=None)
        self.assertEqual(no_feed._venue_match("tok", self.MINE, 10_000.0, 10_300.0), {})

    def test_the_dashboard_shows_trig_match_and_keeps_trig_fill_in_eight_rows(self):
        from dashboard.sniper_layout import _latency_body
        lat = {k: {"n": 2, "p50": v, "last": v} for k, v in
               (("trigger_to_send", 1.1), ("trigger_to_match", 120.0), ("trigger_to_ack", 300.0),
                ("trigger_to_fill", 310.0), ("feed_to_trigger", 0.02), ("send_to_ack", 299.0))}
        rows = _latency_body(lat, 60, max_rows=6)[:6]
        text = "\n".join("".join(r[0] if isinstance(r, tuple) else getattr(r, "text", str(r)) for r in row)
                         if isinstance(row, list) else str(row) for row in rows)
        for label in ("TRIG>SEND", "TRIG>MATCH", "TRIG>ACK", "TRIG>FILL"):
            self.assertIn(label, text, text)


class DecisionBook(unittest.TestCase):
    """dec_* on every order row: the decision's own book, measurement only."""

    def test_paper_order_rows_carry_the_decision_book(self):
        rows, _ = run_two()
        for o in [r for r in rows if r.get("result") in ("filled", "refused")]:
            self.assertEqual((o["dec_bid"], o["dec_ask"], o["dec_spread"]), (0.50, 0.52, 0.02))
            self.assertEqual(o["dec_cap"], o["cap"])
            under = [(px, q) for px, q in ASKS if px <= o["cap"] + 1e-9]
            self.assertEqual(o["dec_levels_under_cap"], len(under))
            self.assertAlmostEqual(o["dec_depth_sh"], sum(q for _px, q in under))
            self.assertAlmostEqual(o["dec_depth_usd"], sum(px * q for px, q in under), places=4)
            self.assertGreaterEqual(o["dec_quote_age_ms"], 0.0)
            self.assertIn("dec_venue_age_ms", o)
            self.assertNotIn("dec_error", o)

    REPLY_BOOK = ((0.25, 5.82), (0.27, 23.51), (0.28, 25.25))     # the 2026-10-17 reply snapshot

    def decide(self, cap, max_buy=0.95):
        sn = S.Sniper(Hub(), Broker(), settings=_settings(), journal_path=None)
        sn.broker.max_buy_price = max_buy
        view = types.SimpleNamespace(asks=self.REPLY_BOOK, bids=((0.24, 10.0),), best_bid=0.24, best_ask=0.25,
                                     updated_mono=time.monotonic(), exchange_ts_ms=None)
        row = {"wall": time.time()}
        sn._decision_book(row, {"view": view, "cap": cap, "token": "up", "decided_mono": time.monotonic()})
        return row

    def test_a_level_exactly_at_the_cap_counts_even_with_float_noise(self):
        row = self.decide(round(0.23 + 0.05, 4))                       # how _decide builds ask + slip
        self.assertEqual((row["dec_cap"], row["dec_levels_under_cap"]), (0.28, 3))
        self.assertAlmostEqual(row["dec_depth_sh"], 54.58, places=6)
        noisy = 0.04 + 0.24                                            # 0.27999999999999997: just under
        self.assertNotEqual(noisy, 0.28)
        for cap in (noisy, 0.28 - 1e-12):                              # noise either side still counts
            self.assertEqual(self.decide(cap)["dec_levels_under_cap"], 3)

    def test_a_lower_cap_or_account_ceiling_leaves_the_028_level_out(self):
        for row in (self.decide(0.27), self.decide(0.2799), self.decide(0.28, max_buy=0.27)):
            self.assertAlmostEqual(row["dec_depth_sh"], 29.33, places=6)   # 5.82 + 23.51: the report's figure
            self.assertEqual(row["dec_levels_under_cap"], 2)
        self.assertEqual(self.decide(0.28, max_buy=0.27)["dec_cap"], 0.27)

    def test_book_trace_points_use_the_same_inclusive_boundary(self):
        sn = S.Sniper(Hub(), Broker(), settings=_settings(), journal_path=None)
        view = types.SimpleNamespace(asks=self.REPLY_BOOK, bids=((0.24, 10.0),), status="LIVE", exchange_ts_ms=None)
        self.assertAlmostEqual(sn._book_point(view, round(0.23 + 0.05, 4))["depth_cap_sh"], 54.58, places=6)
        self.assertAlmostEqual(sn._book_point(view, 0.27)["depth_cap_sh"], 29.33, places=6)

    def test_a_broken_view_is_reported_never_raised(self):
        sn = S.Sniper(Hub(), Broker(), settings=_settings(), journal_path=None)
        row = {}
        sn._decision_book(row, {"view": types.SimpleNamespace(asks=(("x", 1),)), "cap": 0.3, "token": "up"})
        self.assertIn("dec_error", row)
        self.assertEqual(sn.n["decision book errors"], 1)

if __name__ == "__main__":
    unittest.main(verbosity=1)
