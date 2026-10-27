#!/usr/bin/env python3
"""Binance USD-M perpetual BTCUSDT as a SECOND trigger beside the Coinbase
ticker (SNIPER_EXTRA_FEEDS=binance_fut) - paper since 2026-10-13, live allowed
since 2026-10-13 (exec_latency.LIVE_EXTRA_FEEDS).

    python tests_sniper_binance_fut.py

The message shape is Binance's documented futures bookTicker (e, u, s, b, B,
a, A, T, E). No real socket: the loop test runs against a local server.
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import sys
import time
import types
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import websockets  # noqa: E402

import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
from tests_sniper_coinbase import age, env, make, ticker  # noqa: E402

DUAL = {"SNIPER_EXTRA_FEEDS": "binance_fut"}


def fut(mid: float, *, lag_ms: float = 80.0, symbol: str = "BTCUSDT", spread: float = 0.1, **extra) -> str:
    """One futures bookTicker as Binance sends it; lag_ms: how old its event time is."""
    e = int(time.time() * 1000 - lag_ms)
    d = {"e": "bookTicker", "u": 400900217, "s": symbol, "b": f"{mid - spread / 2:.2f}", "B": "31.21",
         "a": f"{mid + spread / 2:.2f}", "A": "40.66", "T": e, "E": e}
    d.update(extra)
    return json.dumps(d)


def age_fut(sn, ms: float) -> None:
    trig = sn.triggers["binance_fut"]
    trig.q = type(trig.q)((t - ms, m) for t, m in trig.q)


def armed(extra_env=None, keep_in_flight=False):
    """(Sniper, fired): Coinbase primary + Binance futures second trigger."""
    sn, fired = make({**DUAL, **(extra_env or {})})
    if keep_in_flight:
        def fire(side, r_bp, t_ms, t_pc=None, feed=None):          # the order stays in flight
            fired.append((side, round(r_bp, 2), feed))
            return object()
        sn._fire_now = fire
    return sn, fired


class SettingsTests(unittest.TestCase):
    def test_it_is_a_second_trigger_live_may_add(self):
        with env(SNIPER_LATENCY_MS="200", **DUAL):
            st = S.Settings()
        self.assertEqual((st.primary_feed, st.extra_feeds), ("coinbase", ("binance_fut",)))
        self.assertIn("second trigger: binance_fut", st.lines()[0])
        self.assertFalse(any("second trigger" in v for v in X.spec_violations(st)))
        with env(SNIPER_LATENCY_MS="200"):
            self.assertFalse(any("second trigger" in v for v in X.spec_violations(S.Settings())))
        other = types.SimpleNamespace(**{**vars(st), "extra_feeds": ("kraken",)})
        self.assertTrue(any("second trigger: SNIPER_EXTRA_FEEDS=kraken" in v for v in X.spec_violations(other)))
        with env(SNIPER_LATENCY_MS="200", SNIPER_EXTRA_FEEDS="binance_perp"), \
                self.assertRaisesRegex(ValueError, "binance_fut"):
            S.Settings()                                                   # a typo names the supported feeds

    def test_live_starts_with_or_without_it(self):
        import run
        for extra in (DUAL, {}):
            with self.subTest(extra=extra), env(SNIPER_LATENCY_MS="200", SNIPER_ONLY="1", SNIPER_LIVE_CANARY="1",
                                                **extra):
                self.assertTrue(run._live_sniper_gate(paper=False))
                lines = X.effective_config_lines(S.Settings(), live=True)
                second = next(x for x in lines if "2nd trigger" in x)
                self.assertIn("binance_fut" if extra else "none", second)
                self.assertNotIn("not accepted live", second)


class HandlerTests(unittest.TestCase):
    def test_what_each_message_is(self):
        sn, fired = armed()
        self.assertEqual(sn.on_binance_fut(fut(60_000.0)), "tick")
        self.assertEqual(sn.on_binance_fut(json.dumps({"result": None, "id": 1})), "other")
        self.assertEqual(sn.on_binance_fut("not json"), "malformed")
        self.assertEqual(sn.on_binance_fut(fut(60_000.0, symbol="ETHUSDT")), "malformed")
        self.assertEqual(sn.on_binance_fut(fut(60_000.0, b="60001", a="60000")), "malformed")     # crossed
        self.assertEqual(sn.on_binance_fut(fut(60_000.0, b=True)), "malformed")
        self.assertEqual((sn.n["binance_fut ticks"], sn.n["binance_fut malformed"]), (1, 4))
        self.assertEqual(sn.bf_fresh.n["trades"], 1)                                 # its event time is noted
        self.assertEqual(fired, [])

    def test_a_futures_move_fires_tagged_binance_fut(self):
        sn, fired = armed()
        sn.on_binance_fut(fut(60_000.0))
        age_fut(sn, 300)
        sn.on_binance_fut(fut(60_012.0))                                             # +2bp in 300ms
        self.assertEqual(fired, [("UP", 2.0, "binance_fut")])
        self.assertEqual(sn.n["signals binance_fut"], 1)
        self.assertEqual(sn.n["signals"], 0)                                         # the primary's own count
        self.assertEqual(len(sn.trigger.q), 0)                                       # Coinbase's rule untouched
        self.assertIsNone(sn.last_mid)                                               # nor its mid
        self.assertEqual(sn.vol_model.last_mid, None)                                # nor the volatility

    def test_one_move_seen_by_both_feeds_is_one_order(self):
        sn, fired = armed(keep_in_flight=True)
        sn.on_binance_fut(fut(60_000.0))
        sn.on_coinbase(ticker(60_000.0))
        age_fut(sn, 300)
        age(sn, 300)
        sn.on_binance_fut(fut(60_012.0))                                             # futures first ...
        sn.on_coinbase(ticker(60_012.0))                                             # ... the ticker 150ms later
        self.assertEqual(fired, [("UP", 2.0, "binance_fut")])
        # the ticker's signal is stopped by the side's cooldown (checked before in-flight)
        self.assertEqual(sn.n["skip: cooldown"] + sn.n["skip: previous order in flight"], 1)

    def test_a_stale_futures_tick_is_not_traded(self):
        sn, fired = armed()
        g = sn.bf_fresh
        g.enabled = g.connected = True
        g.note_message(time.monotonic(), 0)
        sn.on_binance_fut(fut(60_000.0, lag_ms=80.0))
        age_fut(sn, 300)
        sn.on_binance_fut(fut(60_012.0, lag_ms=2_000.0))                             # 2s behind its usual
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["skip: binance_fut feed behind (binance_fut)"]
                         + sn.n["skip: binance_fut feed behind"], 1)

    def test_the_summary_and_dashboard_show_it(self):
        sn, _ = armed()
        sn.on_binance_fut(fut(60_000.0))
        s = sn.summary()
        self.assertEqual((s["binance_fut"]["ticks"], s["binance_fut"]["malformed"]), (1, 0))
        self.assertIn("binance_fut", s["signals_by_feed"])
        with env(SNIPER_LATENCY_MS="200"):
            self.assertNotIn("binance_fut", S.Sniper(None, None, settings=S.Settings()).summary())
        import run
        rt = types.SimpleNamespace(sniper=sn, paper=True, reconciler=None, settler=None, ledger=None,
                                   clock_ok=True, clock_detail="ok",
                                   hub=types.SimpleNamespace(fill_store=None))
        view = run.DashView.__new__(run.DashView)
        view.rt = rt
        rows = {name: (state, detail) for name, state, detail in view._health_rows(s, {})}
        self.assertIn("BIN FUT", rows)
        self.assertIn("2nd trigger", rows["BIN FUT"][1])


class SocketTests(unittest.IsolatedAsyncioTestCase):
    """_extra_feed("binance_fut") against a local server speaking the futures stream."""

    async def asyncSetUp(self):
        self.received, self.conns, self.server, self.task = [], 0, None, None
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
            try:
                await script(n, ws)
            except websockets.exceptions.ConnectionClosed:
                pass
        self.server = await websockets.serve(handler, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.sn, self.fired = armed(extra_env)
        self.events = []
        self.sn.on_event = lambda *a: self.events.append(a)
        self.patches.append(patch.dict(S.EXTRA_FEEDS, {"binance_fut": (f"ws://127.0.0.1:{port}", None)}))
        self.patches[-1].start()
        self.task = asyncio.create_task(self.sn._extra_feed("binance_fut"))

    async def until(self, cond, timeout=5.0):
        end = asyncio.get_running_loop().time() + timeout
        while not cond():
            if asyncio.get_running_loop().time() > end:
                self.fail(f"timed out; events={self.events} n={dict(self.sn.n)}")
            await asyncio.sleep(0.01)

    async def test_it_streams_without_subscribing_and_signals_through_the_gate(self):
        sent = []

        async def script(n, ws):
            async def listen():
                async for m in ws:
                    sent.append(m)
            listener = asyncio.create_task(listen())
            await ws.send(fut(60_000.0))
            await asyncio.sleep(0.3)
            await ws.send(fut(60_012.0))
            await asyncio.sleep(1.0)
            listener.cancel()
        await self.start(script)
        await self.until(lambda: self.fired)
        self.assertEqual(self.fired, [("UP", 2.0, "binance_fut")])
        self.assertEqual(sent, [])                                     # the stream is in the URL: nothing sent
        self.assertTrue(self.sn.bf_fresh.enabled and self.sn.bf_fresh.connected)
        self.assertTrue(any("binance_fut ticker connected (second trigger)" in e[1] for e in self.events))

    async def test_a_silent_socket_reconnects(self):
        async def script(n, ws):
            await ws.send(fut(60_000.0))
            await asyncio.sleep(5.0)
        await self.start(script, {"SNIPER_BINANCE_SILENT_S": "0.2"})
        await self.until(lambda: self.conns >= 2)
        self.assertGreaterEqual(self.sn.n["binance_fut reconnects"], 1)


class CheckReportTests(unittest.TestCase):
    def test_orders_are_split_by_the_feed_that_fired_them(self):
        import sniper_check as C
        fills = [{"order_id": f"o{i}", "feed": feed, "round_end": 100 + i, "shares": 10.0, "cost": 4.0}
                 for i, feed in enumerate(["coinbase", "coinbase", "binance_fut", "binance_fut", "binance_fut"])]
        refused = [{"feed": "binance_fut"}, {"feed": "coinbase"}, {"feed": "coinbase"}]
        lot_pay = {f"o{i}": (True, 1.0 if i % 2 else 0.0) for i in range(5)}
        marks = {f"o{i}": {"markout_5s_c": 0.5} for i in range(5)}
        text = "\n".join(C.by_feed_lines(fills, refused, marks, lot_pay))
        self.assertIn("by triggering feed", text)
        cb = next(ln for ln in text.splitlines() if ln.strip().startswith("coinbase"))
        bf = next(ln for ln in text.splitlines() if ln.strip().startswith("binance_fut"))
        self.assertEqual(cb.split()[1:4], ["4", "2", "50%"])
        self.assertEqual(bf.split()[1:4], ["4", "3", "75%"])
        self.assertEqual(C.by_feed_lines(fills[:2], refused[1:], marks, lot_pay), [])   # one feed: nothing


if __name__ == "__main__":
    unittest.main()
