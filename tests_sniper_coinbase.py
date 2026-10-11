#!/usr/bin/env python3
"""The BTC sniper triggered by Coinbase's BTC-USD ticker - the default trigger
and the frozen spec's feed since 2026-10-10 (SNIPER_PRIMARY_FEED=coinbase).

    python tests_sniper_coinbase.py

The protocol shapes below are the ones Coinbase Exchange's public WebSocket
sent on 2026-10-10 (a 45s read-only probe: the subscriptions ack, 103 tickers
with every field listed in TICKER_FIELDS, 45 heartbeats, sequences rising).
No real socket: the loop tests run against a local server speaking them.
"""
from __future__ import annotations

import asyncio
import gzip
import itertools
import json
import os
import pathlib
import sys
import tempfile
import time
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import websockets  # noqa: E402

import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402

TICKER_FIELDS = ("best_ask", "best_ask_size", "best_bid", "best_bid_size", "high_24h", "last_size", "low_24h",
                 "open_24h", "price", "product_id", "sequence", "side", "time", "trade_id", "type", "volume_24h",
                 "volume_30d")
ACK = {"type": "subscriptions", "channels": [{"name": "ticker", "product_ids": ["BTC-USD"], "account_ids": None},
                                             {"name": "heartbeat", "product_ids": ["BTC-USD"], "account_ids": None}]}
CB = {"SNIPER_LATENCY_MS": "200"}                 # a fixed latency: no order signing; the feed is the default
_SEQ = itertools.count(1_000)


def iso(ms: float | None = None) -> str:
    t = time.time() if ms is None else ms / 1000.0
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def ticker(mid: float, *, seq=None, product="BTC-USD", lag_ms: float | None = 80.0, spread=0.02, **extra) -> str:
    """One Coinbase ticker as the venue sends it; lag_ms: how old its match time is (None: no time field)."""
    d = {"type": "ticker", "sequence": next(_SEQ) if seq is None else seq, "product_id": product,
         "price": f"{mid:.2f}", "open_24h": "60125.01", "volume_24h": "6112.31524733", "low_24h": "59511.2",
         "high_24h": "60850", "volume_30d": "275519.46224301", "best_bid": f"{mid - spread / 2:.2f}",
         "best_bid_size": "0.08217445", "best_ask": f"{mid + spread / 2:.2f}", "best_ask_size": "0.31221",
         "side": "buy", "trade_id": 812345678, "last_size": "0.00012"}
    if lag_ms is not None:
        d["time"] = iso(time.time() * 1000.0 - lag_ms)
    d.update(extra)
    return json.dumps(d)


def heartbeat() -> str:
    return json.dumps({"type": "heartbeat", "last_trade_id": 812345678, "product_id": "BTC-USD",
                       "sequence": next(_SEQ), "time": iso()})


def env(**kw):
    """The environment with every SNIPER_* variable removed, then kw set."""
    base = {k: v for k, v in os.environ.items() if not k.startswith("SNIPER_") and k != "BOT_LOCK_PATH"}
    base.update(kw)
    return patch.dict(os.environ, base, clear=True)


def make(extra_env=None, **sniper_kw):
    """(Sniper, fired): the order path replaced by a recorder."""
    with env(**{**CB, **(extra_env or {})}):
        sn = S.Sniper(None, None, settings=S.Settings(), **sniper_kw)
    fired = []

    def fire(side, r_bp, t_ms, t_pc=None, feed=None):
        fired.append((side, round(r_bp, 2), feed))
        sn.busy = False
        return object()
    sn._fire_now = fire
    sn._write = lambda row: None
    return sn, fired


def age(sn, ms: float) -> None:
    """Make every price the rule holds `ms` older (the next ticker is then measured against it)."""
    sn.trigger.q = type(sn.trigger.q)((t - ms, m) for t, m in sn.trigger.q)


# =========================================================================== parsing
class ParseTests(unittest.TestCase):
    def test_a_real_ticker_shape_gives_the_mid(self):
        d = json.loads(ticker(60_000.0))
        self.assertEqual(tuple(sorted(d)), TICKER_FIELDS)
        self.assertAlmostEqual(S.coinbase_mid(d), 60_000.0)

    def test_anything_but_a_well_formed_btc_usd_ticker_is_no_price(self):
        good = json.loads(ticker(60_000.0))
        for label, over in (("another product", {"product_id": "ETH-USD"}), ("no product", {"product_id": None}),
                            ("crossed", {"best_bid": "60001", "best_ask": "60000"}), ("zero bid", {"best_bid": "0"}),
                            ("negative ask", {"best_ask": "-1"}), ("nan", {"best_bid": "nan"}),
                            ("inf", {"best_ask": "inf"}), ("text", {"best_bid": "abc"}), ("null", {"best_ask": None}),
                            ("bool", {"best_bid": True}), ("list", {"best_ask": ["60000"]})):
            with self.subTest(label):
                self.assertIsNone(S.coinbase_mid({**good, **over}))
        self.assertIsNone(S.coinbase_mid({k: v for k, v in good.items() if k != "best_bid"}))
        self.assertIsNone(S.coinbase_mid(["not", "a", "dict"]))
        self.assertAlmostEqual(S.coinbase_mid({**good, "best_bid": 59_999.5, "best_ask": 60_000.5}), 60_000.0)

    def test_the_ack_must_cover_ticker_and_heartbeat_for_btc_usd(self):
        S.coinbase_ack(ACK)
        for label, chans in (
                ("no heartbeat", [{"name": "ticker", "product_ids": ["BTC-USD"]}]),
                ("no ticker", [{"name": "heartbeat", "product_ids": ["BTC-USD"]}]),
                ("another product", [{"name": "ticker", "product_ids": ["ETH-USD"]},
                                     {"name": "heartbeat", "product_ids": ["ETH-USD"]}]),
                ("empty", []), ("malformed", ["ticker", {"name": "heartbeat"}])):
            with self.subTest(label), self.assertRaises(S.CoinbaseSubscribeError):
                S.coinbase_ack({"type": "subscriptions", "channels": chans})
        with self.assertRaises(S.CoinbaseSubscribeError):
            S.coinbase_ack({"type": "subscriptions"})

    def test_the_subscription_is_ticker_and_heartbeat_for_btc_usd(self):
        self.assertEqual(S.COINBASE_SUBSCRIBE,
                         {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker", "heartbeat"]})
        self.assertEqual(S.EXTRA_FEEDS["coinbase"], (S.COINBASE_WS_URL, S.COINBASE_SUBSCRIBE))

    def test_only_coinbase_exchange_public_websocket(self):
        for raw in (None, "", "  ", S.COINBASE_WS_URL):
            self.assertEqual(S.coinbase_url(raw), S.COINBASE_WS_URL)
        for ok in ("wss://ws-feed.exchange.coinbase.com:443", "wss://ws-feed.exchange.coinbase.com/"):
            self.assertEqual(S.coinbase_url(ok), ok)
        for bad in ("ws://ws-feed.exchange.coinbase.com", "wss://evil.example.com", "wss://ws-feed.exchange.coinbase.com:80",
                    "wss://ws-feed.exchange.coinbase.com/other", "wss://ws-feed.exchange.coinbase.com?x=1",
                    "wss://u:p@ws-feed.exchange.coinbase.com", "wss://ws-feed.exchange.coinbase.com#f",
                    "wss://advanced-trade-ws.coinbase.com"):
            with self.subTest(bad), self.assertRaisesRegex(ValueError, "SNIPER_COINBASE_URL"):
                S.coinbase_url(bad)


# =========================================================================== the handler
class HandlerTests(unittest.TestCase):
    def test_what_each_message_is(self):
        sn, fired = make()
        self.assertEqual(sn.on_coinbase(json.dumps(ACK)), "subscribed")
        self.assertEqual(sn.on_coinbase(heartbeat()), "heartbeat")
        self.assertEqual(sn.on_coinbase(json.dumps({"type": "status", "products": []})), "other")
        self.assertEqual(sn.on_coinbase("not json"), "malformed")
        self.assertEqual(sn.on_coinbase("[1, 2]"), "malformed")
        self.assertEqual(sn.on_coinbase(ticker(60_000.0, best_bid="abc")), "malformed")
        self.assertEqual(sn.on_coinbase(ticker(60_000.0, seq=5)), "tick")
        self.assertEqual(sn.on_coinbase(ticker(60_000.0, seq=5)), "late")
        self.assertEqual((sn.n["coinbase malformed"], sn.n["coinbase heartbeats"], sn.n["coinbase ticks"],
                          sn.n["coinbase out of order"]), (3, 1, 1, 1))
        self.assertEqual(fired, [])

    def test_an_error_or_a_wrong_ack_raises(self):
        sn, _ = make()
        with self.assertRaisesRegex(S.CoinbaseSubscribeError, "Failed to subscribe"):
            sn.on_coinbase(json.dumps({"type": "error", "message": "Failed to subscribe", "reason": "BTC-XYZ is not a valid product"}))
        with self.assertRaises(S.CoinbaseSubscribeError):
            sn.on_coinbase(json.dumps({"type": "subscriptions", "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]}))

    def test_up_and_down_fire_tagged_coinbase_with_the_unsuffixed_counters(self):
        sn, fired = make()
        sn.on_coinbase(ticker(60_000.0))
        age(sn, 300)
        sn.on_coinbase(ticker(60_006.0))                                    # +1bp against the price 300ms ago
        sn.last_fire = -1e18
        age(sn, 300)
        sn.on_coinbase(ticker(59_994.0))                                    # -2bp against 60006
        self.assertEqual([(f[0], f[2]) for f in fired], [("UP", "coinbase"), ("DOWN", "coinbase")])
        self.assertEqual(fired[0][1], 1.0)
        self.assertEqual(sn.n["signals"], 2)
        self.assertNotIn("signals coinbase", sn.n)
        self.assertEqual(sn.summary()["fired_by_feed"], {"coinbase": 2})
        self.assertAlmostEqual(sn.last_mid, 59_994.0)

    def test_the_spec_threshold_is_the_rule(self):
        k = dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)["k_bp"]       # 0.3bp since 2026-10-11
        sn, fired = make()
        sn.on_coinbase(ticker(60_000.0, spread=0.002))
        age(sn, 260)
        sn.on_coinbase(ticker(round(60_000.0 * (1 + (k - 0.01) / 1e4), 2), spread=0.0))   # 0.01bp under: no signal
        self.assertEqual(fired, [])
        sn.trigger.q.clear()
        sn.on_coinbase(ticker(60_000.0, spread=0.0))
        age(sn, 260)
        sn.on_coinbase(ticker(round(60_000.0 * (1 + k / 1e4), 2), spread=0.0))           # exactly k
        self.assertEqual([f[0] for f in fired], ["UP"])

    def test_a_heartbeat_does_not_carry_the_price_forward(self):
        """The ticker speaks on matches: the book can move between them unseen. A
        quiet spell past the 2s max gap restarts the rule, heartbeats or not."""
        clock = types.SimpleNamespace(s=1_000.0)
        sn, fired = make()
        with patch.object(S.time, "monotonic", lambda: clock.s):
            sn.on_coinbase(ticker(60_000.0))
            for _ in range(5):                                              # 2.5s of heartbeats, no match
                clock.s += 0.5
                sn.on_coinbase(heartbeat())
            clock.s += 0.2
            sn.on_coinbase(ticker(60_012.0))                                # +2bp, but 2.7s after the last price
            self.assertEqual(fired, [])
            self.assertEqual(len(sn.trigger.q), 1)                          # the rule restarted on this price
            self.assertEqual(sn.n["coinbase gaps over max gap"], 1)
            clock.s += 0.3
            sn.on_coinbase(ticker(60_024.0))                                # +2bp within 300ms: fires
        self.assertEqual([f[0] for f in fired], ["UP"])
        self.assertAlmostEqual(sn.coinbase_gap_max, 2700.0, places=3)

    def test_kraken_heartbeats_still_hold_their_price(self):                 # the contrast, unchanged
        with env(SNIPER_PRIMARY_FEED="kraken", SNIPER_LATENCY_MS="200"):
            sn = S.Sniper(None, None, settings=S.Settings())
        clock = types.SimpleNamespace(s=1_000.0)
        with patch.object(S.time, "monotonic", lambda: clock.s):
            sn.trigger.update(clock.s * 1000.0, 3000.0)
            clock.s += 1.5
            sn.on_kraken('{"channel":"heartbeat"}')
        self.assertEqual(len(sn.trigger.q), 2)

    def test_an_out_of_order_ticker_moves_nothing(self):
        sn, fired = make()
        sn.on_coinbase(ticker(60_000.0, seq=100))
        age(sn, 300)
        self.assertEqual(sn.on_coinbase(ticker(60_100.0, seq=99)), "late")  # older than one already used
        self.assertEqual(fired, [])
        self.assertEqual([m for _, m in sn.trigger.q], [60_000.0])
        self.assertEqual(sn.on_coinbase(ticker(60_100.0, seq=101)), "tick")
        self.assertEqual([f[0] for f in fired], ["UP"])

    def test_a_ticker_without_a_sequence_is_still_a_price(self):            # replays write none
        sn, _ = make()
        self.assertEqual(sn.on_coinbase(json.dumps({"type": "ticker", "product_id": "BTC-USD",
                                                     "best_bid": "60000", "best_ask": "60000.02"})), "tick")
        self.assertEqual(sn.cb_fresh.n["trade without E"], 1)              # no match time either

    def test_binance_ticks_do_not_move_a_coinbase_trigger(self):
        sn, fired = make()
        sn.on_tick(json.dumps({"b": "60000.0", "a": "60000.2"}))
        age(sn, 300)
        sn.on_tick(json.dumps({"b": "60100.0", "a": "60100.2"}))
        self.assertEqual(fired, [])
        self.assertEqual(len(sn.trigger.q), 0)
        self.assertEqual(sn.n["binance ticks ignored (not the trigger)"], 2)

    def test_as_a_second_trigger_it_keeps_its_own_rule_and_suffixed_counters(self):
        sn, fired = make({"SNIPER_PRIMARY_FEED": "binance", "SNIPER_EXTRA_FEEDS": "coinbase"})
        self.assertIsNot(sn.triggers["coinbase"], sn.trigger)
        sn.on_coinbase(ticker(60_000.0))
        sn.triggers["coinbase"].q = type(sn.triggers["coinbase"].q)((t - 300, m) for t, m in sn.triggers["coinbase"].q)
        sn.on_coinbase(ticker(60_012.0))
        self.assertEqual([(f[0], f[2]) for f in fired], [("UP", "coinbase")])
        self.assertEqual((sn.n["signals"], sn.n["signals coinbase"]), (0, 1))
        self.assertEqual(len(sn.trigger.q), 0)                              # Binance's rule untouched
        self.assertIsNone(sn.last_mid)                                      # the dashboard shows the trigger's mid

    def test_the_match_time_is_measured(self):
        sn, _ = make()
        sn.on_coinbase(ticker(60_000.0, lag_ms=120.0))
        self.assertEqual(sn.cb_fresh.n["trades"], 1)
        self.assertAlmostEqual(sn.cb_fresh.lag[-1], 120.0, delta=50.0)


# =========================================================================== the freshness gate
class GateTests(unittest.TestCase):
    def armed(self, backlog=0):
        sn, fired = make()
        rows = []
        sn._write = rows.append
        sn.cb_fresh.enabled = sn.cb_fresh.connected = True
        sn.cb_fresh.note_message(time.monotonic(), backlog)
        return sn, fired, rows

    def move(self, sn, lag_ms=80.0, frm=60_000.0, to=60_012.0):
        sn.on_coinbase(ticker(frm, lag_ms=80.0))                           # the baseline: an 80ms path
        age(sn, 300)
        sn.on_coinbase(ticker(to, lag_ms=lag_ms))

    def test_a_current_ticker_fires(self):
        sn, fired, _ = self.armed()
        self.move(sn)
        self.assertEqual([f[0] for f in fired], ["UP"])
        self.assertEqual(sn.cb_fresh.status(), "LIVE")

    def test_a_ticker_far_behind_its_usual_is_skipped_and_restarts_the_rule(self):
        sn, fired, rows = self.armed()
        self.move(sn, lag_ms=1_600.0)                                       # 1.5s worse than the 80ms baseline
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["skip: coinbase feed behind"], 1)
        self.assertEqual(len(sn.trigger.q), 0)
        row = rows[-1]
        self.assertEqual((row["feed"], row["reason"]), ("coinbase", "coinbase feed behind"))
        self.assertGreater(row["coinbase_excess_ms"], 1_000.0)
        self.assertIn("coinbase_backlog", row)

    def test_a_receive_backlog_is_skipped(self):
        sn, fired, _ = self.armed(backlog=50)
        self.move(sn)
        self.assertEqual((fired, sn.n["skip: coinbase receive backlog"]), ([], 1))

    def test_a_disconnected_feed_is_skipped(self):
        sn, fired, _ = self.armed()
        sn.cb_fresh.on_disconnect()
        self.move(sn)
        self.assertEqual((fired, sn.n["skip: coinbase disconnected"]), ([], 1))

    def test_no_match_time_is_unknown_age(self):
        sn, fired, _ = self.armed()
        sn.on_coinbase(ticker(60_000.0, lag_ms=None))
        age(sn, 300)
        sn.on_coinbase(ticker(60_012.0, lag_ms=None))
        self.assertEqual((fired, sn.n["skip: coinbase age unknown"]), ([], 1))

    def test_the_gate_is_off_unless_the_socket_loop_turns_it_on(self):     # replays, benches
        sn, fired = make()
        self.assertFalse(sn.cb_fresh.enabled)
        sn.on_coinbase(ticker(60_000.0, lag_ms=None))
        age(sn, 300)
        sn.on_coinbase(ticker(60_012.0, lag_ms=None))
        self.assertEqual([f[0] for f in fired], ["UP"])

    def test_binance_freshness_does_not_gate_coinbase(self):
        sn, fired, _ = self.armed()
        sn.fresh.enabled, sn.fresh.connected = True, False                 # Binance "disconnected": not the trigger
        self.move(sn)
        self.assertEqual([f[0] for f in fired], ["UP"])

    def test_the_decision_row_carries_the_coinbase_excess(self):
        with env(**CB):
            sn = S.Sniper(types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_end=None),
                          None, settings=S.Settings())
        rows = []
        sn._write = rows.append
        sn.cb_fresh.last_excess = 12.34
        sn._fire_now("UP", 1.5, time.monotonic() * 1000.0)                  # feed None: the trigger feed
        self.assertEqual((rows[0]["feed"], rows[0]["coinbase_excess_ms"], rows[0]["reason"]),
                         ("coinbase", 12.3, "no current round"))
        self.assertNotIn("binance_excess_ms", rows[0])


# =========================================================================== settings, summary, live
class SettingsTests(unittest.TestCase):
    def test_coinbase_is_the_default_and_the_frozen_spec(self):
        with env(**CB):
            st = S.Settings()
        self.assertEqual(st.primary_feed, "coinbase")
        self.assertEqual(dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)["primary_feed"], "coinbase")
        self.assertIn("[triggered by Coinbase BTC-USD ticker]", st.lines()[0])
        self.assertIn("[triggered by Coinbase BTC-USD ticker]", st.lines(live=True)[0])
        self.assertEqual((st.coinbase_url, st.coinbase_freshness, st.coinbase_stale_ms, st.coinbase_unknown_ms,
                          st.coinbase_backlog_max, st.coinbase_silent_s),
                         (S.COINBASE_WS_URL, True, 1000.0, 5000.0, 20, 5.0))
        with env(SNIPER_ONLY="1"):
            self.assertFalse(any("trigger feed" in v for v in X.spec_violations(S.Settings(), 25)))

    def test_the_primary_is_never_its_own_second_trigger(self):
        with env(**CB, SNIPER_EXTRA_FEEDS="coinbase"):
            st = S.Settings()
        self.assertEqual(st.extra_feeds, ())
        self.assertNotIn("second trigger", st.lines()[0])
        with env(**CB, SNIPER_EXTRA_FEEDS="coinbase", SNIPER_PRIMARY_FEED="binance"):
            self.assertEqual(S.Settings().extra_feeds, ("coinbase",))

    def test_coinbase_settings_are_validated(self):
        for var in ("SNIPER_COINBASE_STALE_MS", "SNIPER_COINBASE_UNKNOWN_MS", "SNIPER_COINBASE_SILENT_S"):
            with self.subTest(var), env(**CB, **{var: "0"}), self.assertRaisesRegex(ValueError, "SNIPER_COINBASE"):
                S.Settings()
        with env(**CB, SNIPER_COINBASE_BACKLOG_MAX="-1"), self.assertRaisesRegex(ValueError, "SNIPER_COINBASE"):
            S.Settings()
        with env(**CB, SNIPER_COINBASE_URL="wss://evil.example.com"), self.assertRaisesRegex(ValueError, "SNIPER_COINBASE_URL"):
            S.Settings()
        with env(**CB, SNIPER_COINBASE_FRESHNESS="0"):
            self.assertFalse(S.Settings().coinbase_freshness)
        with env(**CB, SNIPER_COINBASE_URL="wss://ws-feed.exchange.coinbase.com:443"):
            self.assertIn("Coinbase via wss://ws-feed.exchange.coinbase.com:443", S.Settings().lines()[0])

    def test_the_summary_has_the_coinbase_block(self):
        sn, _ = make()
        sn.on_coinbase(heartbeat())
        sn.on_coinbase(ticker(60_000.0))
        s = sn.summary()
        c = s["coinbase"]
        for k in ("status", "excess_ms", "age_ms", "baseline_ms", "clock", "backlog_max", "ticks", "heartbeats",
                  "malformed", "out_of_order", "reconnects", "gaps_over_max_gap", "gap_ms_max"):
            self.assertIn(k, c)
        self.assertEqual((c["ticks"], c["heartbeats"]), (1, 1))
        self.assertNotIn("kraken", s)
        self.assertEqual(s["binance"]["reconnects"], 0)                     # not the trigger socket
        with env(**CB, SNIPER_PRIMARY_FEED="binance"):
            self.assertNotIn("coinbase", S.Sniper(None, None, settings=S.Settings()).summary())

    def test_the_live_canary_may_trigger_from_coinbase(self):
        with env(**CB):
            st = S.Settings()
            sn = S.Sniper(None, None, settings=st, live=types.SimpleNamespace(attach=lambda s: None))
        self.assertIsNotNone(sn.live)

    def test_run_routes_to_the_coinbase_loop(self):
        sn, _ = make()
        called = []

        async def fake_loop():
            called.append("coinbase")

        async def fake_kraken():
            called.append("kraken")
        sn._coinbase_loop, sn._kraken_loop = fake_loop, fake_kraken

        async def go():
            await sn.run()
            for t in list(sn._tasks):
                t.cancel()
            await asyncio.gather(*sn._tasks, return_exceptions=True)
        asyncio.run(go())
        self.assertEqual(called, ["coinbase"])


# =========================================================================== the socket loop
class CoinbaseSocketTests(unittest.IsolatedAsyncioTestCase):
    """_coinbase_loop against a local server speaking Coinbase's shapes."""

    async def asyncSetUp(self):
        self.client_msgs, self.conns, self.task, self.server = [], 0, None, None
        self.patches = [patch("feeds.supervisor.backoff_delay", lambda attempt, *a, **k: 0.05)]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for p in self.patches:
            p.stop()

    async def start(self, script, extra_env=None):
        async def handler(ws):
            n = self.conns
            self.conns += 1
            self.client_msgs.append(("sub", n, await ws.recv()))
            try:
                await script(n, ws)
            except websockets.exceptions.ConnectionClosed:
                pass
        self.server = await websockets.serve(handler, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.sn, self.fired = make(extra_env)
        self.sn.s.coinbase_url = f"ws://127.0.0.1:{port}"                  # a local stand-in; wss is checked above
        self.events = []
        self.sn.on_event = lambda *a: self.events.append(a)
        self.task = asyncio.create_task(self.sn._coinbase_loop())

    async def until(self, cond, timeout=5.0):
        end = asyncio.get_running_loop().time() + timeout
        while not cond():
            if asyncio.get_running_loop().time() > end:
                self.fail(f"timed out; events={self.events} n={dict(self.sn.n)}")
            await asyncio.sleep(0.01)

    async def test_it_subscribes_with_the_documented_message_and_reports_connected(self):
        async def script(n, ws):
            await ws.send(json.dumps(ACK))
            await ws.send(ticker(60_000.0))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: any("ticker connected" in e[1] for e in self.events))
        self.assertEqual(json.loads(self.client_msgs[0][2]), S.COINBASE_SUBSCRIBE)
        self.assertIn(("sniper", "Coinbase BTC-USD ticker connected (freshness gate on)", "good"), self.events)
        self.assertAlmostEqual(self.sn.last_mid, 60_000.0)
        self.assertTrue(self.sn.cb_fresh.enabled and self.sn.cb_fresh.connected)

    async def test_a_move_inside_one_connection_signals_through_the_gate(self):
        async def script(n, ws):
            await ws.send(json.dumps(ACK))
            await ws.send(ticker(60_000.0))
            await asyncio.sleep(0.3)
            await ws.send(ticker(60_012.0))                                 # +2bp, a current match
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.fired)
        self.assertEqual((self.fired[0][0], self.fired[0][2]), ("UP", "coinbase"))

    async def test_a_stale_ticker_on_the_socket_is_not_traded(self):
        async def script(n, ws):
            await ws.send(ticker(60_000.0, lag_ms=80.0))
            await asyncio.sleep(0.3)
            await ws.send(ticker(60_012.0, lag_ms=3_000.0))                 # a 3s-old match
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.sn.n["skip: coinbase feed behind"] == 1)
        self.assertEqual(self.fired, [])

    async def test_a_price_from_before_a_reconnect_is_no_reference(self):
        async def script(n, ws):
            if n == 0:
                await ws.send(ticker(60_000.0))
                await asyncio.sleep(0.3)
                await ws.close()
            else:
                await ws.send(ticker(60_060.0))                             # +10bp against the OLD connection's price
                await asyncio.sleep(0.5)
        await self.start(script)
        await self.until(lambda: self.conns >= 2 and self.sn.last_mid == 60_060.0)
        await asyncio.sleep(0.1)
        self.assertEqual(self.fired, [])
        self.assertEqual(self.sn.n["signals"], 0)
        self.assertGreaterEqual(self.sn.n["reconnects"], 1)
        self.assertEqual(len(self.sn.trigger.q), 1)
        self.assertEqual(self.sn.coinbase_gap_max, 0.0)                     # the outage is not a market gap

    async def test_the_trigger_object_is_replaced_on_every_connect(self):
        seen = []

        async def script(n, ws):
            seen.append(self.sn.trigger)
            await ws.send(ticker(60_000.0))
            await asyncio.sleep(0.05)
            await ws.close()
        await self.start(script)
        await self.until(lambda: len(seen) >= 3)
        self.assertEqual(len({id(t) for t in seen}), len(seen))
        self.assertIs(self.sn.triggers["coinbase"], self.sn.trigger)

    async def test_a_sequence_from_an_old_connection_does_not_block_a_new_one(self):
        async def script(n, ws):
            await ws.send(ticker(60_000.0, seq=500 if n == 0 else 10))      # a new connection may restart lower
            await asyncio.sleep(0.05 if n == 0 else 0.5)
            if n == 0:
                await ws.close()
        await self.start(script)
        await self.until(lambda: self.conns >= 2 and self.sn.n["coinbase ticks"] >= 2)
        self.assertEqual(self.sn.n["coinbase out of order"], 0)

    async def test_an_error_reply_reconnects_and_never_reports_connected(self):
        async def script(n, ws):
            await ws.send(json.dumps({"type": "error", "message": "Failed to subscribe",
                                      "reason": "BTC-USD is delisted"}))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.sn.n["reconnects"] >= 2)
        self.assertFalse(any("connected" in e[1] for e in self.events), self.events)
        self.assertTrue(any("delisted" in e[1] for e in self.events), self.events)

    async def test_an_ack_without_the_heartbeat_reconnects(self):
        async def script(n, ws):
            await ws.send(json.dumps({"type": "subscriptions",
                                      "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]}))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.sn.n["reconnects"] >= 1)
        self.assertTrue(any("missing heartbeat" in e[1] for e in self.events), self.events)

    async def test_heartbeats_keep_a_quiet_socket_alive(self):
        async def script(n, ws):
            await ws.send(ticker(60_000.0))
            for _ in range(12):
                await ws.send(heartbeat())
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.2)
        await self.start(script, {"SNIPER_COINBASE_SILENT_S": "0.3"})
        await self.until(lambda: self.sn.n["coinbase heartbeats"] >= 12)
        self.assertEqual((self.sn.n["reconnects"], self.conns), (0, 1))

    async def test_a_silent_socket_is_a_dead_socket(self):
        async def script(n, ws):
            await ws.send(ticker(60_000.0))
            await asyncio.sleep(5.0)                                        # then nothing, not even a heartbeat
        await self.start(script, {"SNIPER_COINBASE_SILENT_S": "0.2"})
        await self.until(lambda: self.conns >= 2)
        self.assertGreaterEqual(self.sn.n["reconnects"], 1)
        self.assertTrue(any("silent 0.2s" in e[1] and "reconnecting" in e[1] for e in self.events), self.events)

    async def test_heartbeats_without_tickers_are_a_lost_subscription(self):
        async def script(n, ws):
            await ws.send(json.dumps(ACK))
            await ws.send(ticker(60_000.0))
            for _ in range(60):
                await ws.send(heartbeat())
                await asyncio.sleep(0.05)
        with patch.object(S, "COINBASE_STARVED_S", 0.3):
            await self.start(script)
            await self.until(lambda: self.conns >= 2)
        self.assertTrue(any("subscription lost" in e[1] for e in self.events), self.events)

    async def test_freshness_off_is_announced(self):
        async def script(n, ws):
            await ws.send(ticker(60_000.0))
            await asyncio.sleep(0.5)
        await self.start(script, {"SNIPER_COINBASE_FRESHNESS": "0"})
        await self.until(lambda: any("ticker connected" in e[1] for e in self.events))
        self.assertTrue(any("NOT checked for age" in e[1] and e[2] == "warn" for e in self.events), self.events)
        self.assertFalse(self.sn.cb_fresh.enabled)


# =========================================================================== bench_network: the race and the budget
class BenchNetworkTests(unittest.TestCase):
    def test_one_move_is_one_episode_per_side(self):
        import bench_network as N
        sig = [(0, 1), (40, 1), (290, 1), (300, 1), (310, -1), (320, -1), (900, 1)]
        self.assertEqual(N.episodes(sig, 300.0), [(0, 1), (300, 1), (310, -1), (900, 1)])

    def test_episodes_pair_by_side_within_a_second(self):
        import bench_network as N
        cb = [(1_000, 1), (5_000, -1), (9_000, 1), (20_000, 1)]
        bn = [(1_040, 1), (4_970, -1), (9_100, -1), (30_000, 1)]
        m = N.match_episodes(cb, bn, 1000.0)
        self.assertEqual((m["matched"], m["a_first"], m["b_first"], m["only_a"], m["only_b"]), (2, 1, 1, 2, 2))
        self.assertEqual((m["a_lead_ms"]["p50"], m["b_lead_ms"]["p50"]), (40.0, 30.0))
        m2 = N.match_episodes([(0, 1)], [(10, 1), (500, 1)], 1000.0)                # the nearest one, used once
        self.assertEqual((m2["matched"], m2["only_b"], m2["lead_ms"]["p50"]), (1, 1, 10.0))

    def test_the_budget_adds_the_measured_parts(self):
        import bench_network as N
        res = {"coinbase": {"match_to_receipt_ms": {"p50": 100.0}}, "clob_warm": {"warm_rtt_ms": {"p50": 20.0}},
               "coinbase_clock": {"offset_ms": -5.0}}
        b = N.budget(res)
        self.assertEqual((b["feed_ms"], b["one_way_ms"]), (95.0, 10.0))
        self.assertAlmostEqual(b["signal_to_match_ms"], 0.4 + 10.0 + 150.0 + 25.0)
        self.assertAlmostEqual(b["btc_to_match_ms"], 95.0 + 0.4 + 10.0 + 150.0 + 25.0)
        self.assertIsNone(N.budget({"coinbase": {"error": "x"}}))

    def test_the_report_renders_every_section_and_errors(self):
        import bench_network as N
        st = {"n": 3, "avg": 1.0, "p50": 1.0, "p95": 2.0, "p99": 2.0, "max": 3.0}
        res = {"host": "vps", "when": "now", "clob_handshake": {"error": "gaierror"}, "clob_warm": {"warm_rtt_ms": st},
               "coinbase_clock": {"offset_ms": 1.5, "offset_uncertainty_ms": 40.0, "rtt_ms": st},
               "coinbase": {"seconds": 60, "tickers": 120, "per_s": 2.0, "heartbeats": 60, "gaps_over_2s": 3,
                            "match_to_receipt_ms": st, "jitter_above_min_ms": st, "gap_ms": st, "connect_ms": 50.0},
               "binance": {"error": "blocked"},
               "race": {"seconds": 120, "k_bp": 0.5, "window_ms": 250.0, "ticks": {"coinbase": 200, "binance": 900},
                        "episodes": {"coinbase": 4, "binance": 6}, "matched": 3, "coinbase_first": 2, "binance_first": 1,
                        "tie": 0, "only_coinbase": 1, "only_binance": 3, "coinbase_lead_ms": st, "binance_lead_ms": st}}
        res["budget"] = N.budget(res)
        text = N.table(res)
        for part in ("clock offset to Coinbase +1.5ms", "Coinbase match -> receipt", "3 gaps over 2s",
                     "Coinbase first 2, Binance first 1", "binance: blocked", "latency budget", "from our signal"):
            self.assertIn(part, text)


# =========================================================================== the A/B replay
class ReplayTests(unittest.TestCase):
    def recording(self, rows) -> str:
        d = tempfile.mkdtemp(prefix="cb_replay_")
        path = os.path.join(d, "lab_test.jsonl.gz")
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")
        return path

    def test_the_replay_drives_the_trigger_feed_it_is_asked_for(self):
        import replay_ab
        from latency_lab import feeds as XF
        w = 1_791_374_100 * 10 ** 9
        path = self.recording([
            ["bsb", 1, w + 1, 7, "60000.1", "1", "60000.3", "1"],
            ["xb", 2, w + 2, XF.CODE["coinbase"], 1.0, 60001.0, 60001.02],
            ["xb", 3, w + 3, XF.CODE["kraken"], 1.0, 60002.0, 60002.02],
        ])
        self.assertEqual(replay_ab.SPEC_FEED, "coinbase")
        cb = [e for e in replay_ab.events([path], "coinbase") if e[0] == "btc"]
        bn = [e for e in replay_ab.events([path], "binance") if e[0] == "btc"]
        self.assertEqual([(e[2], e[3]) for e in cb], [(60001.0, 60001.02)])  # Coinbase only, never Kraken
        self.assertEqual([(e[2], e[3]) for e in bn], [(60000.1, 60000.3)])
        self.assertEqual(replay_ab.scan_rounds([path], "coinbase")["__btc_ticks__"], 1)
        with self.assertRaises(ValueError):
            list(replay_ab.events([path], "kraken"))

    def test_the_replayed_message_is_one_the_handler_takes(self):
        import replay_ab
        sn, _ = make()
        self.assertEqual(sn.on_coinbase(replay_ab.btc_message("coinbase", 60001.0, 60001.02)), "tick")
        self.assertAlmostEqual(sn.last_mid, 60001.01)
        self.assertEqual(json.loads(replay_ab.btc_message("binance", 60000.1, 60000.3)), {"b": "60000.10", "a": "60000.30"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
