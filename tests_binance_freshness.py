"""Binance freshness gate (sniper_mode.BinanceFreshness): no signal is acted on
unless the Binance feed is shown current. Run: python -m pytest -q tests_binance_freshness.py"""
import collections
import json
import time
import types
import unittest

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S


def trade(E_ms):
    return json.dumps({"stream": "btcusdt@trade", "data": {"e": "trade", "E": E_ms, "T": E_ms, "p": "60000"}})


def book(mid, wrapped=True):
    d = {"u": 1, "s": "BTCUSDT", "b": f"{mid - 0.005:.3f}", "B": "1", "a": f"{mid + 0.005:.3f}", "A": "1"}
    return json.dumps({"stream": "btcusdt@bookTicker", "data": d} if wrapped else d)


def sniper(enabled=True):
    sn = S.Sniper(None, None, settings=S.Settings())
    sn.fresh.enabled = enabled
    sn.fresh.connected = enabled
    sn.fresh.note_message(time.monotonic(), 0)       # what run() stamps on every message
    fired = []

    def fake_fire(side, r, t, t_pc=None, feed="binance"):
        fired.append(side)
        sn.busy = False
        return True
    sn._fire_now = fake_fire
    sn._write = lambda row: None
    return sn, fired


def move(sn, frm=60000.0, to=60010.0):
    """A 1.67bp rise against the price 300ms ago."""
    sn.trigger.q.append((time.monotonic() * 1000.0 - 300, frm))
    sn.on_tick(book(to))


class Gate(unittest.TestCase):
    def test_fresh_feed_fires(self):
        sn, fired = sniper()
        sn.on_tick(trade(time.time() * 1000.0 - 40))
        move(sn)
        self.assertEqual(fired, ["UP"])
        self.assertEqual(sn.fresh.status(), "LIVE")

    def test_feed_behind_skips_and_restarts_the_rule(self):
        sn, fired = sniper()
        now = time.time() * 1000.0
        sn.on_tick(trade(now - 40))          # the best path: 40ms
        sn.on_tick(trade(now - 1300))        # 1.26s worse than that: a stalled feed
        move(sn)
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["skip: binance feed behind"], 1)
        self.assertEqual(len(sn.trigger.q), 0, "the compressed move must not stay a reference")
        self.assertEqual(sn.fresh.status(), "DEGRADED")

    def test_trade_pipeline_jitter_does_not_block(self):
        # Binance's trade events trail its bookTicker by up to ~400ms on their
        # own (measured): that alone must not block a fresh tick.
        sn, fired = sniper()
        now = time.time() * 1000.0
        sn.on_tick(trade(now - 40))
        sn.on_tick(trade(now - 440))
        move(sn)
        self.assertEqual(fired, ["UP"])

    def test_clock_offset_does_not_matter(self):
        # A clock 2s behind Binance makes every age negative; the excess is the same.
        for offset in (0.0, -2000.0, 5000.0):
            f = S.BinanceFreshness(1000, 5000, 20)
            m = 100.0
            for lag in (40, 45, 41, 300):
                f.note_trade(1_000_000.0, 1_000_000.0 + lag + offset, m)
                m += 0.1
            self.assertAlmostEqual(f.last_excess, 260.0)

    def test_baseline_forgets_old_minimum(self):
        f = S.BinanceFreshness(150, 5000, 20, baseline_s=10)
        f.note_trade(0, 20, 0.0)             # a 20ms best, long ago
        f.note_trade(0, 100, 20.0)           # 20s later the route is 100ms
        self.assertEqual(f.last_excess, 0.0)

    def test_backlog_skips(self):
        sn, fired = sniper()
        sn.on_tick(trade(time.time() * 1000.0 - 40))
        sn.fresh.note_message(time.monotonic(), 500)
        move(sn)
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["skip: binance receive backlog"], 1)
        sn.fresh.note_message(time.monotonic(), 0)      # drained: acts again
        move(sn)
        self.assertEqual(fired, ["UP"])
        self.assertEqual(sn.fresh.summary()["backlog_max"], 500)

    def test_no_trade_timestamp_is_unknown_age(self):
        sn, fired = sniper()
        move(sn)
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["skip: binance age unknown"], 1)

    def test_old_trade_timestamp_is_unknown_age(self):
        sn, fired = sniper()
        sn.fresh.note_trade(time.time() * 1000.0 - 40, time.time() * 1000.0, time.monotonic() - 6)
        move(sn)
        self.assertEqual(sn.n["skip: binance age unknown"], 1)

    def test_disconnected_skips_and_reconnect_needs_new_timing(self):
        sn, fired = sniper()
        sn.on_tick(trade(time.time() * 1000.0 - 40))
        sn.fresh.on_disconnect()
        move(sn)
        self.assertEqual(sn.n["skip: binance disconnected"], 1)
        sn.fresh.on_connect()
        move(sn)
        self.assertEqual(sn.n["skip: binance age unknown"], 1, "the old socket's timing vouches for nothing")
        sn.on_tick(trade(time.time() * 1000.0 - 40))
        move(sn)
        self.assertEqual(fired, ["UP"])

    def test_gate_off_for_replays(self):
        sn, fired = sniper(enabled=False)
        move(sn)                              # no trades, no socket: the old behaviour
        self.assertEqual(fired, ["UP"])
        self.assertEqual(sn.fresh.status(), "OFF")

    def test_unwrapped_book_ticker_still_parses(self):
        sn, fired = sniper(enabled=False)
        sn.trigger.q.append((time.monotonic() * 1000.0 - 300, 60000.0))
        sn.on_tick(book(60010.0, wrapped=False))
        self.assertEqual(fired, ["UP"])


class Parsing(unittest.TestCase):
    def test_trade_never_moves_the_rule(self):
        sn, fired = sniper()
        sn.on_tick(trade(time.time() * 1000.0))
        self.assertEqual(len(sn.trigger.q), 0)
        self.assertEqual(sn.fresh.n["trades"], 1)

    def test_bad_messages_are_counted_not_used(self):
        sn, fired = sniper()
        for raw in ("not json", json.dumps({"stream": "x", "data": {"b": "nan", "a": "nan"}}),
                    json.dumps({"stream": "x", "data": {"b": "1"}}), json.dumps([1, 2])):
            sn.on_tick(raw)
        self.assertEqual(sn.n["binance parse errors"], 4)
        self.assertEqual(len(sn.trigger.q), 0)

    def test_trade_without_event_time(self):
        f = S.BinanceFreshness(150, 5000, 20)
        f.note_trade(None, 1.0, 1.0)
        f.note_trade("x", 1.0, 1.0)
        self.assertEqual(f.n["trade without E"], 2)
        self.assertIsNone(f.last_trade_mono)


class Plumbing(unittest.TestCase):
    def test_stream_url_keeps_the_route(self):
        self.assertEqual(S.binance_stream_url(S.BOOK_TICKER_URL),
                         "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@trade")
        self.assertEqual(S.binance_stream_url("wss://data-stream.binance.vision/ws/btcusdt@bookTicker"),
                         "wss://data-stream.binance.vision/stream?streams=btcusdt@bookTicker/btcusdt@trade")

    def test_rx_backlog_both_websockets_generations(self):
        import queue
        q = queue.SimpleQueue()
        for _ in range(3):
            q.put(1)
        self.assertEqual(S.rx_backlog(types.SimpleNamespace(recv_messages=types.SimpleNamespace(frames=q))), 3)
        self.assertEqual(S.rx_backlog(types.SimpleNamespace(messages=collections.deque([1, 2]))), 2)
        self.assertIsNone(S.rx_backlog(object()))

    def test_settings_validate(self):
        import os
        from unittest import mock
        with mock.patch.dict(os.environ, {"SNIPER_BINANCE_STALE_MS": "0"}):
            with self.assertRaises(ValueError):
                S.Settings()
        with mock.patch.dict(os.environ, {"SNIPER_BINANCE_FRESHNESS": "0"}):
            self.assertFalse(S.Settings().binance_freshness)

    def test_summary_has_the_binance_block(self):
        sn, _ = sniper()
        sn.on_tick(trade(time.time() * 1000.0 - 40))
        b = sn.summary()["binance"]
        for k in ("status", "excess_ms", "age_ms", "baseline_ms", "clock", "backlog_max",
                  "reconnects", "parse_errors"):
            self.assertIn(k, b)


if __name__ == "__main__":
    unittest.main()
