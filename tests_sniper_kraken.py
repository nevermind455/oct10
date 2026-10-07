"""The BTC sniper triggered by Kraken's BTC/USD ticker (SNIPER_PRIMARY_FEED=kraken):
parsing, the rule, feed quality, the socket (a local WebSocket server stands in for
Kraken) - PAPER only - and the Binance default unchanged.

    python -m unittest -v tests_sniper_kraken
"""
import asyncio
import json
import math
import os
import types
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import websockets

import sniper_mode as S

KRK = {"SNIPER_PRIMARY_FEED": "kraken", "SNIPER_LATENCY_MS": "200"}       # a fixed latency: no order signing
BTC = {"SNIPER_LATENCY_MS": "200"}
# Kraken's own acknowledgement, as the live server sent it (2026-10-06)
ACK = {"method": "subscribe", "req_id": 1, "success": True, "time_in": "2026-10-06T16:25:53.837172Z",
       "time_out": "2026-10-06T16:25:53.837216Z",
       "result": {"channel": "ticker", "event_trigger": "bbo", "snapshot": True, "symbol": "BTC/USD"}}


def env(**kw):
    """The process environment with every SNIPER_* variable removed, then kw set - so a
    developer's or the VPS's own .env cannot change what a test sees."""
    base = {k: v for k, v in os.environ.items() if not k.startswith("SNIPER_") and k != "BOT_LOCK_PATH"}
    base.update(kw)
    return patch.dict(os.environ, base, clear=True)


def tick(mid, typ="update", symbol="BTC/USD", spread=0.02, **row_extra):
    row = {"symbol": symbol, "bid": round(mid - spread / 2, 4), "bid_qty": 4.0, "ask": round(mid + spread / 2, 4),
           "ask_qty": 3.5, "last": mid, "volume": 1.0, "vwap": mid, "low": mid - 5, "high": mid + 5,
           "change": 0.0, "change_pct": 0.0, "timestamp": "2026-10-06T12:00:00.123456Z", **row_extra}
    return json.dumps({"channel": "ticker", "type": typ, "data": [row]})


def make(extra_env=None, base=KRK, **sniper_kw):
    """(Sniper, fired): a sniper whose order path is replaced by a recorder, so a test sees
    exactly which decisions the feed produced."""
    with env(**{**base, **(extra_env or {})}):
        sn = S.Sniper(None, None, settings=S.Settings(), **sniper_kw)
    fired = []

    def fire(side, r_bp, t_ms, t_pc=None, feed="binance"):
        fired.append((side, round(r_bp, 2), feed))
        sn.busy = False
        return object()                      # not None: an order was started, so the cooldown starts
    sn._fire_now = fire
    return sn, fired


class Clock:
    """A monotonic clock the test moves by hand (seconds, as time.monotonic)."""

    def __init__(self):
        self.ms = 10_000.0

    def __call__(self):
        return self.ms / 1000.0


class KrakenUrlTests(unittest.TestCase):
    def test_default_and_accepted_forms(self):
        self.assertEqual(S.kraken_url(None), "wss://ws.kraken.com/v2")
        self.assertEqual(S.kraken_url("  "), "wss://ws.kraken.com/v2")
        self.assertEqual(S.kraken_url("wss://ws.kraken.com/v2"), "wss://ws.kraken.com/v2")
        self.assertEqual(S.kraken_url("wss://ws.kraken.com:443/v2"), "wss://ws.kraken.com:443/v2")

    def test_nothing_but_krakens_public_v2_socket(self):
        for bad in ("ws://ws.kraken.com/v2",                    # not TLS
                    "wss://ws-auth.kraken.com/v2",              # the private host
                    "wss://api.kraken.com/v2", "wss://kraken.com/v2",
                    "wss://ws.kraken.com.evil.example/v2", "wss://evil.example/ws.kraken.com/v2",
                    "wss://ws.kraken.com/", "wss://ws.kraken.com/v1", "wss://ws.kraken.com/V2",
                    "wss://ws.kraken.com/v2/", "wss://ws.kraken.com/v2?x=1", "wss://ws.kraken.com/v2#f",
                    "wss://ws.kraken.com:9443/v2", "wss://user:pw@ws.kraken.com/v2", "wss://user@ws.kraken.com/v2",
                    "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"):
            with self.subTest(bad), self.assertRaisesRegex(ValueError, "SNIPER_KRAKEN_URL"):
                S.kraken_url(bad)

    def test_settings_validate_it(self):
        with env(**KRK, SNIPER_KRAKEN_URL="wss://evil.example/v2"), self.assertRaisesRegex(ValueError, "KRAKEN"):
            S.Settings()

    def test_the_subscription_is_the_documented_ticker_bbo_for_btc_usd_only(self):
        sub = S.KRAKEN_SUBSCRIBE
        self.assertEqual(sub["method"], "subscribe")
        self.assertEqual(sub["params"], {"channel": "ticker", "symbol": ["BTC/USD"], "event_trigger": "bbo",
                                         "snapshot": True})


class KrakenMidTests(unittest.TestCase):
    def good(self, **kw):
        return {"symbol": "BTC/USD", "bid": 3000.0, "ask": 3000.5, **kw}

    def test_mid_is_the_average_of_best_bid_and_ask(self):
        self.assertAlmostEqual(S.kraken_mid(self.good()), 3000.25)
        self.assertAlmostEqual(S.kraken_mid(self.good(bid=3000, ask=3000)), 3000.0)        # locked: ok
        self.assertAlmostEqual(S.kraken_mid(self.good(bid=1, ask=2)), 1.5)                 # ints

    def test_anything_else_is_not_a_price(self):
        for name, row in (("another symbol", self.good(symbol="ETH/USD")), ("no symbol", {"bid": 1, "ask": 2}),
                          ("missing bid", {"symbol": "BTC/USD", "ask": 3000.5}),
                          ("missing ask", {"symbol": "BTC/USD", "bid": 3000.0}),
                          ("string bid", self.good(bid="3000.0")), ("bool bid", self.good(bid=True)),
                          ("null ask", self.good(ask=None)), ("zero bid", self.good(bid=0)),
                          ("negative ask", self.good(ask=-1.0)), ("nan bid", self.good(bid=math.nan)),
                          ("inf ask", self.good(ask=math.inf)), ("crossed", self.good(bid=3001.0, ask=3000.0)),
                          ("not a dict", [3000.0, 3000.5]), ("none", None), ("string", "BTC/USD")):
            with self.subTest(name):
                self.assertIsNone(S.kraken_mid(row))


class KrakenMessageTests(unittest.TestCase):
    """Parsing: what each kind of Kraken message does, and that nothing malformed gets near the rule."""

    def setUp(self):
        self.sn, self.fired = make()

    def test_snapshot_and_update_rows_are_prices(self):
        self.assertEqual(self.sn.on_kraken(tick(3000.0, typ="snapshot")), "tick")
        self.assertEqual(self.sn.on_kraken(tick(3000.5, typ="update")), "tick")
        self.assertAlmostEqual(self.sn.last_mid, 3000.5)
        self.assertEqual(len(self.sn.trigger.q), 2)
        self.assertEqual(self.sn.n["kraken malformed"], 0)

    def test_a_heartbeat_carries_the_price_forward_and_never_signals(self):
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            self.sn.on_kraken(tick(3000.0))
            for _ in range(5):
                clock.ms += 1000
                self.assertEqual(self.sn.on_kraken('{"channel":"heartbeat"}'), "heartbeat")
        q = list(self.sn.trigger.q)
        self.assertTrue(all(p == q[0][1] for _, p in q), q)                    # the same price, carried
        self.assertEqual(q[-1][0], clock.ms)                                     # ... as of the last heartbeat
        self.assertEqual(self.sn.n["kraken heartbeats"], 5)
        self.assertEqual(self.sn.n["signals"], 0)
        self.assertEqual(self.fired, [])

    def test_a_quiet_book_with_heartbeats_is_no_gap(self):
        # live 10-06: quiet spells of up to 7.7s, with heartbeats; the first move out of calm must fire
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            self.sn.on_kraken(tick(3000.0))
            for _ in range(5):
                clock.ms += 1000
                self.sn.on_kraken('{"channel":"heartbeat"}')
            clock.ms += 100
            self.sn.on_kraken(tick(3000.6))                                      # +2bp
        self.assertEqual(self.fired, [("UP", 2.0, "kraken")])
        self.assertEqual(self.sn.n["kraken gaps over max gap"], 0)
        self.assertAlmostEqual(self.sn.kraken_gap_max, 5100.0)

    def test_a_stall_without_heartbeats_still_ages_the_reference_out(self):
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            self.sn.on_kraken(tick(3000.0))
            clock.ms += 2500                                                    # silence, not even a heartbeat
            self.sn.on_kraken('{"channel":"heartbeat"}')                         # too late to vouch for the price
            clock.ms += 100
            self.sn.on_kraken(tick(3000.6))
        self.assertEqual(self.fired, [])
        self.assertEqual(self.sn.n["kraken gaps over max gap"], 1)

    def test_acks_status_and_pongs_never_reach_the_rule(self):
        self.assertEqual(self.sn.on_kraken(json.dumps(ACK)), "subscribed")
        status = {"channel": "status", "type": "update",
                  "data": [{"version": "2.0.10", "system": "online", "api_version": "v2", "connection_id": 1}]}
        self.assertEqual(self.sn.on_kraken(json.dumps(status)), "status")
        self.assertEqual(self.sn.kraken_system, "online")
        for msg in ({"method": "pong", "req_id": 1, "time_in": "x", "time_out": "y"},
                    {"channel": "ohlc", "type": "update", "data": []}):
            self.assertEqual(self.sn.on_kraken(json.dumps(msg)), "other", msg)
        self.assertEqual(self.sn.n["kraken malformed"], 0)
        self.assertEqual(len(self.sn.trigger.q), 0)

    def test_an_ack_for_anything_but_ticker_btc_usd_bbo_is_refused(self):
        # a bbo subscription silently downgraded to "trades" would miss most top-of-book moves
        for name, change in (("trades trigger", {"event_trigger": "trades"}), ("no trigger echoed", {"event_trigger": None}),
                             ("another symbol", {"symbol": "ETH/USD"}), ("another channel", {"channel": "book"})):
            with self.subTest(name), self.assertRaises(S.KrakenSubscribeError):
                self.sn.on_kraken(json.dumps({**ACK, "result": {**ACK["result"], **change}}))
        for ack in ({**ACK, "result": None}, {k: v for k, v in ACK.items() if k != "success"}):
            with self.assertRaises(S.KrakenSubscribeError):
                self.sn.on_kraken(json.dumps(ack))

    def test_a_refused_subscription_raises_so_the_loop_reconnects(self):
        refusal = {"method": "subscribe", "success": False, "error": "Currency pair not supported BTC/USD"}
        with self.assertRaisesRegex(S.KrakenSubscribeError, "not supported"):
            self.sn.on_kraken(json.dumps(refusal))

    def test_malformed_input_is_counted_and_dropped(self):
        for raw in ("", "not json", "{", "[]", "[1,2]", "null", "3000.5", '"ticker"',
                    '{"channel":"ticker","type":"update"}',                    # no data
                    '{"channel":"ticker","type":"update","data":null}',
                    '{"channel":"ticker","type":"update","data":{}}',          # not a list
                    '{"channel":"ticker","type":"update","data":[]}',          # empty
                    '{"channel":"ticker","type":"update","data":[null]}',
                    '{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":"x","ask":1}]}',
                    tick(3000.0, symbol="ETH/USD"),
                    tick(3000.0, bid=3001.0, ask=3000.0),                      # crossed
                    tick(3000.0, bid=None), tick(3000.0, ask=0),
                    '{"channel":"ticker","type":"update","data":[{"symbol":"BTC/USD","bid":NaN,"ask":NaN}]}',
                    b"\xff\xfe"):
            with self.subTest(raw=raw):
                self.assertEqual(self.sn.on_kraken(raw), "malformed")
        self.assertEqual(len(self.sn.trigger.q), 0)               # nothing reached the rule
        self.assertIsNone(self.sn.last_mid)
        self.assertEqual(self.sn.n["signals"], 0)
        self.assertGreaterEqual(self.sn.n["kraken malformed"], 20)

    def test_a_message_with_several_rows_feeds_only_its_last_good_one(self):
        rows = [{"symbol": "BTC/USD", "bid": 3000.0, "ask": 3000.2}, {"symbol": "ETH/USD", "bid": 1, "ask": 2},
                {"symbol": "BTC/USD", "bid": 3001.0, "ask": 3001.2}, {"symbol": "BTC/USD", "bid": "bad", "ask": 1}]
        self.assertEqual(self.sn.on_kraken(json.dumps({"channel": "ticker", "type": "update", "data": rows})), "tick")
        self.assertEqual(len(self.sn.trigger.q), 1)                # one timestamp, never two alike
        self.assertAlmostEqual(self.sn.last_mid, 3001.1)
        self.assertEqual(self.sn.n["kraken malformed"], 2)

    def test_bytes_messages_are_accepted(self):
        self.assertEqual(self.sn.on_kraken(tick(3000.0).encode()), "tick")


class KrakenSignalTests(unittest.TestCase):
    """The rule on Kraken's mid: >= SNIPER_K_BP within SNIPER_WINDOW_MS, same gates as Binance."""

    def run_ticks(self, steps, extra_env=None):
        sn, fired = make(extra_env)
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            for dt, mid in steps:
                clock.ms += dt
                sn.on_kraken(tick(mid))
        return sn, fired

    def test_a_rise_buys_up_and_a_fall_buys_down(self):
        sn, fired = self.run_ticks([(0, 3000.0), (100, 3000.0), (200, 3000.5)])          # +1.67bp in 300ms
        self.assertEqual(fired, [("UP", 1.67, "kraken")])
        sn, fired = self.run_ticks([(0, 3000.0), (100, 3000.0), (200, 2999.5)])
        self.assertEqual(fired, [("DOWN", -1.67, "kraken")])

    def test_a_move_under_the_threshold_is_no_signal(self):
        sn, fired = self.run_ticks([(0, 3000.0), (100, 3000.0), (200, 3000.25)])         # +0.83bp
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["signals"], 0)

    def test_k_bp_is_the_threshold(self):
        steps = [(0, 3000.0), (100, 3000.0), (200, 3000.25)]
        self.assertEqual(self.run_ticks(steps, {"SNIPER_K_BP": "0.7"})[1], [("UP", 0.83, "kraken")])
        self.assertEqual(self.run_ticks(steps, {"SNIPER_K_BP": "0.9"})[1], [])

    def test_the_primary_feed_keeps_the_unsuffixed_counters_the_reports_read(self):
        sn, fired = self.run_ticks([(0, 3000.0), (300, 3000.6), (100, 3001.2)], {"SNIPER_COOLDOWN_MS": "1000"})
        self.assertEqual(len(fired), 1)
        self.assertEqual(sn.n["signals"], 2)                      # not "signals kraken"
        self.assertEqual(sn.n["skip: cooldown"], 1)               # inside the cooldown the first order started
        self.assertEqual(sn.n["fired kraken"], 1)
        self.assertNotIn("signals kraken", sn.n)
        self.assertEqual(sn.summary()["fired_by_feed"], {"kraken": 1})
        self.assertEqual(sn.summary()["skips"], {"cooldown": 1})

    def test_one_order_in_flight(self):
        sn, fired = make()
        sn._fire_now = lambda *a, **k: fired.append(1) or object()      # leaves busy set, like a real order
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            for dt, mid in ((0, 3000.0), (300, 3000.6), (1500, 3003.0), (300, 3006.0)):
                clock.ms += dt
                sn.on_kraken(tick(mid))
        self.assertEqual(len(fired), 1)
        self.assertGreaterEqual(sn.n["skip: previous order in flight"], 1)

    def test_a_stalled_feed_is_no_reference_at_the_default_gap(self):
        # the only earlier price is 2.5s old: the default 2s limit drops it, so the first price after the gap is a start
        sn, fired = self.run_ticks([(0, 3000.0), (2500, 3003.0)])
        self.assertEqual(fired, [])

    def test_heartbeats_between_ticks_do_not_change_the_answer(self):
        sn, fired = make()
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            sn.on_kraken(tick(3000.0))
            for _ in range(3):
                clock.ms += 100
                sn.on_kraken('{"channel":"heartbeat"}')
            clock.ms += 100
            sn.on_kraken(tick(3000.6))
        self.assertEqual(fired, [("UP", 2.0, "kraken")])


class FeedQualityTests(unittest.TestCase):
    """Telemetry measured after each decision: arrival lag and gaps between updates."""

    def test_arrival_lag_is_receipt_minus_krakens_own_timestamp(self):
        sn, _ = make()
        stamp = datetime.fromtimestamp(1_000_000 - 0.25, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        with patch.object(S.time, "time", lambda: 1_000_000.0):
            self.assertEqual(sn.on_kraken(tick(3000.0, timestamp=stamp)), "tick")
        self.assertAlmostEqual(sn.kraken_lag[-1], 250.0, places=3)
        self.assertEqual(sn.summary()["kraken"]["lag_ms"]["p50"], 250.0)

    def test_a_missing_or_bad_timestamp_still_trades_but_measures_nothing(self):
        sn, _ = make()
        for ts in (None, "", "yesterday", 12345):
            self.assertEqual(sn.on_kraken(tick(3000.0, timestamp=ts)), "tick")
        self.assertEqual(len(sn.kraken_lag), 0)
        self.assertEqual(sn.n["kraken ticks"], 4)

    def test_gaps_longer_than_the_max_gap_are_counted(self):
        sn, _ = make()
        clock = Clock()
        with patch.object(S.time, "monotonic", clock):
            for dt in (0, 100, 2500, 300):
                clock.ms += dt
                sn.on_kraken(tick(3000.0))
        self.assertEqual(sn.n["kraken gaps over max gap"], 1)
        self.assertAlmostEqual(sn.kraken_gap_max, 2500.0)
        k = sn.summary()["kraken"]
        self.assertEqual((k["ticks"], k["gaps_over_max_gap"], k["gap_ms_max"]), (4, 1, 2500.0))


class KrakenSocketTests(unittest.IsolatedAsyncioTestCase):
    """_kraken_loop against a local server speaking Kraken's v2 shapes."""

    async def asyncSetUp(self):
        self.client_msgs, self.conns, self.task, self.server = [], 0, None, None
        self.patches = [patch.object(S, "KRAKEN_BACKOFF_S", 0.05)]
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
                pass                                  # the client hung up on purpose (a test of exactly that)
        self.server = await websockets.serve(handler, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.sn, self.fired = make(extra_env)
        self.sn.s.kraken_url = f"ws://127.0.0.1:{port}/v2"          # a local stand-in; wss is checked in KrakenUrlTests
        self.events = []
        self.sn.on_event = lambda *a: self.events.append(a)
        self.task = asyncio.create_task(self.sn._kraken_loop())

    async def until(self, cond, timeout=5.0):
        end = asyncio.get_running_loop().time() + timeout
        while not cond():
            if asyncio.get_running_loop().time() > end:
                self.fail(f"timed out; events={self.events} n={dict(self.sn.n)}")
            await asyncio.sleep(0.01)

    async def test_it_subscribes_with_the_documented_message_and_reports_connected(self):
        async def script(n, ws):
            await ws.send(json.dumps(ACK))
            await ws.send(tick(3000.0, typ="snapshot"))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: any("ticker connected" in e[1] for e in self.events))
        sub = json.loads(self.client_msgs[0][2])
        self.assertEqual(sub, S.KRAKEN_SUBSCRIBE)
        self.assertIn(("sniper", "Kraken BTC/USD ticker connected", "good"), self.events)
        self.assertAlmostEqual(self.sn.last_mid, 3000.0)

    async def test_a_move_inside_one_connection_signals(self):             # the control for the reset test below
        async def script(n, ws):
            await ws.send(tick(3000.0))
            await asyncio.sleep(0.3)
            await ws.send(tick(3030.0))                                    # +100bp
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.fired)
        self.assertEqual(self.fired[0][0], "UP")
        self.assertEqual(self.fired[0][2], "kraken")

    async def test_a_price_from_before_a_reconnect_is_no_reference(self):
        async def script(n, ws):
            if n == 0:
                await ws.send(tick(3000.0))
                await asyncio.sleep(0.3)                  # older than the 250ms lookback when the next price lands
                await ws.close()
            else:
                await ws.send(tick(3030.0))               # +100bp against the OLD connection's price
                await asyncio.sleep(0.5)
        await self.start(script)
        await self.until(lambda: self.conns >= 2 and self.sn.last_mid == 3030.0)
        await asyncio.sleep(0.1)
        self.assertEqual(self.fired, [])
        self.assertEqual(self.sn.n["signals"], 0)
        self.assertGreaterEqual(self.sn.n["reconnects"], 1)
        self.assertEqual(len(self.sn.trigger.q), 1)                          # a fresh rule: only the new price
        self.assertEqual(self.sn.kraken_gap_max, 0.0)                        # the outage is not a market gap

    async def test_the_trigger_object_is_replaced_on_every_connect(self):
        seen = []

        async def script(n, ws):
            seen.append(self.sn.trigger)
            await ws.send(tick(3000.0))
            await asyncio.sleep(0.05)
            await ws.close()
        await self.start(script)
        await self.until(lambda: len(seen) >= 3)
        self.assertEqual(len({id(t) for t in seen}), len(seen))
        self.assertIs(self.sn.triggers["kraken"], self.sn.trigger)

    async def test_a_refused_subscription_reconnects_and_never_reports_connected(self):
        async def script(n, ws):
            await ws.send(json.dumps({"method": "subscribe", "success": False, "error": "Currency pair not supported"}))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.sn.n["reconnects"] >= 2)
        self.assertFalse(any("connected" in e[1] for e in self.events), self.events)
        self.assertTrue(any("not supported" in e[1] for e in self.events), self.events)

    async def test_heartbeats_keep_a_quiet_socket_alive(self):
        async def script(n, ws):
            for _ in range(12):
                await ws.send('{"channel":"heartbeat"}')
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.2)
        with patch.object(S, "KRAKEN_RECV_TIMEOUT_S", 0.3):
            await self.start(script)
            await self.until(lambda: self.sn.n["kraken heartbeats"] >= 12)
        self.assertEqual(self.sn.n["reconnects"], 0)
        self.assertEqual(self.conns, 1)

    async def test_a_silent_socket_is_a_dead_socket(self):
        async def script(n, ws):
            await ws.send(tick(3000.0))
            await asyncio.sleep(5.0)                              # then nothing, not even a heartbeat
        with patch.object(S, "KRAKEN_RECV_TIMEOUT_S", 0.2):
            await self.start(script)
            await self.until(lambda: self.conns >= 2)
        self.assertGreaterEqual(self.sn.n["reconnects"], 1)
        self.assertTrue(any("TimeoutError" in e[1] and "reconnecting" in e[1] for e in self.events), self.events)

    async def test_heartbeats_without_updates_are_a_lost_subscription(self):
        async def script(n, ws):
            await ws.send(json.dumps(ACK))
            await ws.send(tick(3000.0))
            for _ in range(60):                                   # alive, but the ticker has gone quiet
                await ws.send('{"channel":"heartbeat"}')
                await asyncio.sleep(0.05)
        with patch.object(S, "KRAKEN_STARVED_S", 0.3):
            await self.start(script)
            await self.until(lambda: self.conns >= 2)
        self.assertTrue(any("subscription lost" in e[1] for e in self.events), self.events)

    async def test_a_venue_status_other_than_online_is_reported(self):
        async def script(n, ws):
            await ws.send(json.dumps({"channel": "status", "type": "update",
                                      "data": [{"system": "maintenance", "api_version": "v2"}]}))
            await ws.send(tick(3000.0))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: any("maintenance" in e[1] for e in self.events))
        self.assertEqual(self.sn.kraken_system, "maintenance")

    async def test_a_wrong_ack_is_never_traded_on(self):
        async def script(n, ws):
            await ws.send(json.dumps({**ACK, "result": {**ACK["result"], "event_trigger": "trades"}}))
            await ws.send(tick(3000.0))
            await asyncio.sleep(0.3)
            await ws.send(tick(3030.0))
            await asyncio.sleep(1.0)
        await self.start(script)
        await self.until(lambda: self.sn.n["reconnects"] >= 2)
        self.assertEqual(self.fired, [])
        self.assertTrue(any("acknowledged" in e[1] for e in self.events), self.events)

    async def test_no_compression_is_negotiated(self):
        seen = []

        async def script(n, ws):
            request = getattr(ws, "request", None)
            headers = request.headers if request is not None else getattr(ws, "request_headers", {})
            seen.append(headers.get("Sec-WebSocket-Extensions"))
            await ws.send(tick(3000.0))
            await asyncio.sleep(0.5)
        await self.start(script)
        await self.until(lambda: seen)
        self.assertNotIn("permessage-deflate", str(seen[0] or ""))

    async def test_it_sends_an_application_level_ping(self):
        async def script(n, ws):
            await ws.send(tick(3000.0))
            async for msg in ws:
                self.client_msgs.append(("msg", n, msg))
        with patch.object(S, "KRAKEN_PING_S", 0.1):
            await self.start(script)
            await self.until(lambda: any(m[0] == "msg" and json.loads(m[2]).get("method") == "ping"
                                         for m in self.client_msgs))


class SettingsTests(unittest.TestCase):
    def test_binance_is_the_default_and_its_start_line_is_unchanged(self):
        with env(**BTC):
            st = S.Settings()
        self.assertEqual(st.primary_feed, "binance")
        self.assertTrue(st.lines()[0].startswith("SNIPER paper-only: spot mid >= 1.0bp within 250ms"), st.lines()[0])
        self.assertNotIn("Kraken", st.lines()[0])

    def test_the_kraken_start_line_names_the_trigger(self):
        with env(**KRK):
            st = S.Settings()
        self.assertEqual((st.primary_feed, st.kraken_url), ("kraken", "wss://ws.kraken.com/v2"))
        self.assertTrue(st.lines()[0].startswith(
            "SNIPER paper-only [triggered by Kraken BTC/USD ticker (bbo), not Binance]: spot mid >= 1.0bp"),
            st.lines()[0])

    def test_only_binance_or_kraken(self):
        for bad in ("coinbase", "binance_tr", "krakenx"):
            with self.subTest(bad), env(**BTC, SNIPER_PRIMARY_FEED=bad), \
                    self.assertRaisesRegex(ValueError, "SNIPER_PRIMARY_FEED"):
                S.Settings()
        with env(**BTC, SNIPER_PRIMARY_FEED=" Kraken "):
            self.assertEqual(S.Settings().primary_feed, "kraken")

    def test_kraken_is_paper_only_in_the_sniper_itself(self):
        with env(**KRK):
            st = S.Settings()
            with self.assertRaisesRegex(ValueError, "paper-only"):
                S.Sniper(None, None, settings=st, live=types.SimpleNamespace(attach=lambda s: None))

    def test_kraken_live_needs_the_explicit_switch(self):
        with env(**KRK, SNIPER_KRAKEN_LIVE="1"):
            st = S.Settings()
            sn = S.Sniper(None, None, settings=st, live=types.SimpleNamespace(attach=lambda s: None))
        self.assertIsNotNone(sn.live)
        self.assertIn("[triggered by Kraken BTC/USD ticker (bbo), not Binance]", st.lines(live=True)[0])


class SummaryTests(unittest.TestCase):
    def test_the_kraken_summary_has_its_feed_block_and_the_default_has_none(self):
        sn, _ = make()
        sn.on_kraken(tick(3000.0))
        k = sn.summary()["kraken"]
        self.assertEqual((k["ticks"], k["malformed"]), (1, 0))
        sn_b, _ = make(base=BTC)
        self.assertNotIn("kraken", sn_b.summary())

    def test_binance_ticks_still_fire_with_unsuffixed_counters_by_default(self):
        sn, fired = make(base=BTC)
        sn.on_tick(json.dumps({"u": 1, "s": "BTCUSDT", "b": "100000.0", "B": "1", "a": "100000.2", "A": "1"}))
        sn.trigger.q[0] = (sn.trigger.q[0][0] - 300.0, sn.trigger.q[0][1])          # 300ms ago
        sn.on_tick(json.dumps({"u": 2, "s": "BTCUSDT", "b": "100020.0", "B": "1", "a": "100020.2", "A": "1"}))
        self.assertEqual([f[0] for f in fired], ["UP"])
        self.assertEqual(sn.n["signals"], 1)


class RunGateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import run
        except Exception as exc:  # noqa: BLE001 - optional SDKs missing on this machine
            raise unittest.SkipTest(f"run is not importable here: {type(exc).__name__}: {exc}")
        cls.rf = run

    def test_kraken_refuses_live(self):
        with env(SNIPER_PRIMARY_FEED="kraken", SNIPER_ONLY="1", SNIPER_LIVE_CANARY="1"), \
                self.assertRaisesRegex(RuntimeError, "PAPER only"):
            self.rf._live_sniper_gate(paper=False)

    def test_kraken_live_with_the_explicit_switch_is_refused_by_the_frozen_spec(self):
        # The opt-in still clears the Kraken check; the frozen spec (exec_latency.FROZEN_SPEC)
        # then refuses LIVE on any trigger feed but Binance.
        with env(SNIPER_PRIMARY_FEED="kraken", SNIPER_KRAKEN_LIVE="1", SNIPER_ONLY="1", SNIPER_LIVE_CANARY="1",
                 SNIPER_K_BP="1.0", SNIPER_SLIP="0.04"), \
                self.assertRaisesRegex(RuntimeError, "(?s)frozen spec.*trigger feed: resolved kraken"):
            self.rf._live_sniper_gate(paper=False)

    def test_kraken_paper_passes(self):
        with env(SNIPER_PRIMARY_FEED="kraken", SNIPER_ONLY="1"):
            self.assertFalse(self.rf._live_sniper_gate(paper=True))


if __name__ == "__main__":
    unittest.main()
