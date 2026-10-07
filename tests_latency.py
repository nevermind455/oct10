#!/usr/bin/env python3
"""The low-latency execution audit: what the live path does between the
trigger and the send, measured and guarded.

    python tests_latency.py

Runs the PRODUCTION live path (Sniper -> LiveExecution -> LeanSender with the
FastOrderBuilder -> FastH2.post_now on a warm HTTP/2 connection) against a
local HTTP/2 fake CLOB, plus the paper path and the frozen-spec gate. The
strategy-equivalence test replays one deterministic event stream through the
strategy layer before this change (git 5b36ea5) and after it. No network,
no wallet (orders are signed with a throwaway key made in memory).

The cases the audit lists that earlier suites already cover are named in
the final report: tests_audit_live, tests_sniper_ioc, tests_sniper_cooldown,
tests_execution_state, tests_settlement.
"""
from __future__ import annotations

import asyncio
import builtins
import importlib.util
import json
import os
import pathlib
import random
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import bench_execution as B  # noqa: E402  (the production-path harness)
import exec_latency as X  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_cooldown as C  # noqa: E402  (the cooldown clock harness)
import tests_sniper_live as T  # noqa: E402

OLD_COMMIT = "5b36ea5"          # the strategy layer before the latency work
SPEC_ENV = dict(B.BENCH_ENV, SNIPER_MAX_FILLS_ROUND="20", SNIPER_MAX_SIDE_SHARES="200")
NO_MATCH = B.NO_MATCH


def move_mid(ref: float, bp: float) -> float:
    return round(ref * (1 + bp / 1e4), 3)


# =========================================================================== trigger
class TriggerBoundaries(unittest.TestCase):
    def fire(self, bp: float, age_ms: float = 260.0):
        t = S.Trigger(0.5, 250)
        t.update(0.0, 60_000.0)
        return t.update(age_ms, move_mid(60_000.0, bp))[0]

    def test_049_no_050_and_051_trigger_both_ways(self):
        for bp, want in ((0.49, None), (0.50, 1), (0.51, 1), (-0.49, None), (-0.50, -1), (-0.51, -1)):
            self.assertEqual(self.fire(bp), want, bp)

    def test_the_250ms_window(self):
        t = S.Trigger(0.5, 250)
        t.update(0.0, 60_000.0)
        self.assertIsNone(t.update(200.0, move_mid(60_000.0, 0.6))[0])      # no price 250ms old yet
        t = S.Trigger(0.5, 250)
        t.update(0.0, 60_000.0)
        t.update(100.0, move_mid(60_000.0, 0.6))
        self.assertIsNone(t.update(400.0, move_mid(60_000.0, 0.6))[0])      # moved >250ms ago, flat since


# =========================================================================== cooldown
class CooldownBoundary(unittest.TestCase):
    def run_steps(self, steps):
        sn, clock, orders = C.make()
        with patch.object(S.time, "monotonic", clock):
            for move, at in steps:
                C.signal(sn, clock, move, at)
        return orders

    def test_same_side_299ms_blocked_300ms_allowed(self):
        self.assertEqual(self.run_steps([(C.UP_MOVE, 0), (C.UP_MOVE, 299)]), [("UP", 0.0)])
        self.assertEqual(self.run_steps([(C.UP_MOVE, 0), (C.UP_MOVE, 300)]), [("UP", 0.0), ("UP", 300.0)])

    def test_opposite_side_is_evaluated_independently_during_a_cooldown(self):
        self.assertEqual(self.run_steps([(C.UP_MOVE, 0), (C.DOWN_MOVE, 1), (C.UP_MOVE, 150)]),
                         [("UP", 0.0), ("DOWN", 1.0)])


# =========================================================================== config fail-fast
class FrozenSpec(unittest.TestCase):
    FROZEN = {"SNIPER_ONLY": "1", "SNIPER_LIVE_CANARY": "1", "SNIPER_PRIMARY_FEED": "binance", "SNIPER_K_BP": "0.5",
              "SNIPER_WINDOW_MS": "250", "SNIPER_ORDER_TYPE": "FAK", "SNIPER_SLIP": "0.05",
              "SNIPER_COOLDOWN_MS": "300", "SNIPER_SHARES": "10", "SNIPER_MAX_TARGET_SHARES": "20",
              "SNIPER_CANARY_MAX_SHARES": "25"}

    def gate(self, **over):
        import run
        env = {k: v for k, v in {**self.FROZEN, **over}.items() if v is not None}
        with patch.dict(os.environ, env, clear=True):
            return run._live_sniper_gate(paper=False)

    def test_the_frozen_spec_starts_live(self):
        self.assertTrue(self.gate())

    def test_an_old_env_value_refuses_live_and_names_it(self):
        for var, value, word in (("SNIPER_K_BP", "1.0", "trigger"), ("SNIPER_SLIP", "0.02", "slip"),
                                 ("SNIPER_ORDER_TYPE", "FOK", "execution"), ("SNIPER_COOLDOWN_MS", "1000", "cooldown"),
                                 ("SNIPER_WINDOW_MS", "500", "window"), ("SNIPER_SHARES", "5", "base_target"),
                                 ("SNIPER_MAX_TARGET_SHARES", "30", "max_target")):
            with self.assertRaises(RuntimeError) as ctx:
                self.gate(**{var: value})
            self.assertIn(word, str(ctx.exception))
            self.assertIn(var, str(ctx.exception))

    def test_a_missing_value_falling_back_to_an_old_default_refuses_live(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.gate(SNIPER_K_BP=None, SNIPER_SLIP=None)            # code defaults: 1.0bp, $0.02
        msg = str(ctx.exception)
        self.assertIn("SNIPER_K_BP unset (code default)", msg)
        self.assertIn("SNIPER_SLIP unset (code default)", msg)

    def test_a_canary_cap_below_the_carried_target_refuses_live(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.gate(SNIPER_CANARY_MAX_SHARES="10")
        self.assertIn("canary_cap", str(ctx.exception))

    def test_paper_only_warns_and_the_config_lines_hold_no_secret(self):
        import run
        env = dict(self.FROZEN, SNIPER_K_BP="1.0", POLY_PRIVATE_KEY="0x" + "ab" * 32, POLY_FUNDER="0x" + "cd" * 20)
        with patch.dict(os.environ, env, clear=True):
            self.assertFalse(run._live_sniper_gate(paper=True))
            s = S.Settings()
            lines = X.effective_config_lines(s, live=True, canary_max_shares=25)
            bad = X.spec_violations(s, 25)
        text = "\n".join(lines)
        self.assertIn("trigger", text)
        self.assertIn("1.00 bp", text)
        self.assertIn("<- frozen spec 0.50 bp", text)
        self.assertNotIn("ab" * 32, text)
        self.assertNotIn("cd" * 20, text)
        self.assertEqual(len(bad), 1)


# =========================================================================== the production live path
class LiveFast(unittest.TestCase):
    """Sniper + LiveExecution + LeanSender(FastOrderBuilder) + FastH2.post_now
    against a local HTTP/2 fake CLOB, as bench_execution runs it."""

    def run_async(self, coro):
        return asyncio.run(coro)

    async def setup_live(self, env=None):
        self.tmp = tempfile.mkdtemp(prefix="tests_latency_")
        self.fake, self.ex, self.store = await B.make_live(self.tmp)
        self.book = T.live_book(asks=((0.55, 500), (0.56, 500)), bids=((0.54, 500),))
        self.book.apply_snapshot(T.DN, [(0.44, 500)], [(0.45, 500), (0.46, 500)], ts_ms=int(time.time() * 1000),
                                 require_exchange_ts=False)
        self.hub = types.SimpleNamespace(up_token=T.UP, down_token=T.DN, condition_id=T.COND,
                                         window_end=T.round_end(min_left=60.0), book=self.book)
        self.journal = os.path.join(self.tmp, "live_sniper.jsonl")
        with B._env(dict(SPEC_ENV, **(env or {}))):
            settings = S.Settings()
        self.sn = S.Sniper(self.hub, self.ex.shim, journal_path=self.journal, settings=settings, trade_rows=[],
                           own_rounds=True, live=self.ex)
        self.sn.fresh.enabled = False
        self.n_order = 0

    async def teardown_live(self):
        for task in list(self.sn._tasks):
            task.cancel()
        await asyncio.gather(*self.sn._tasks, return_exceptions=True)
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        T.reset_ambiguity()

    def script(self, kind="no_match", delay_s=0.0, body=None, status=None):
        self.n_order += 1
        oid, tid = f"0xorder{self.n_order}", f"trade-{self.n_order}"
        if body is None:
            body, status = ((NO_MATCH, 400) if kind == "no_match" else
                            ({"success": True, "status": "matched", "orderID": oid, "tradeIDs": [tid],
                              "errorMsg": ""}, 200))
        action = ("json", status, body)
        self.fake.script.append(("sleep", delay_s, action) if delay_s else action)
        return oid, tid

    def raw(self, side: str, bp: float = 0.6) -> str:
        mid = move_mid(60_000.0, bp if side == "UP" else -bp)
        return json.dumps({"b": f"{mid - 0.005:.3f}", "a": f"{mid + 0.005:.3f}"})

    def arm(self, clear_cooldown=True):
        if clear_cooldown:
            self.sn.last_fire = -1e18
        self.sn.trigger.q.clear()
        self.sn.trigger.q.append((time.monotonic() * 1000.0 - 300.0, 60_000.0))

    async def settle(self, side, timeout=2.0):
        deadline = time.monotonic() + timeout
        while self.sn.inflight.get(side) is not None and time.monotonic() < deadline:
            await asyncio.sleep(0.001)

    def rows(self, result=None):
        out = [json.loads(x) for x in open(self.journal, encoding="utf-8")] if os.path.exists(self.journal) else []
        return [r for r in out if result is None or r.get("result") == result]

    def posted(self):
        return [o[0] for o in self.fake.orders]

    # ---- T0..T7
    def test_every_order_carries_t0_to_t7_with_its_sources(self):
        async def go():
            await self.setup_live()
            try:
                self.script("no_match")
                self.arm()
                self.sn.on_tick(self.raw("UP"))
                await self.settle("UP")
                oid, tid = self.script("matched")
                self.arm()
                self.sn.on_tick(self.raw("DOWN"))
                await self.settle("DOWN")
                self.store.record_trade(tid, order_id=oid, asset_id=T.DN, market=T.COND, side="BUY", price=0.45,
                                        size=5.0, status="MATCHED")
                self.sn._sync_live_fak()
                miss = self.rows("refused")[-1]
                upd = self.rows("live_fill_update")[-1]
                for r, fill_src in ((miss, "ACK"), (upd, "PRIVATE_WS")):
                    lat = r["lat"]
                    for k in X.STAGES + X.TOTALS:
                        self.assertIn(k, lat, (k, lat))
                    parts = lat["trigger_to_decision"] + lat["build"] + lat["sign"] + lat["dispatch"]
                    self.assertAlmostEqual(parts, lat["trigger_to_send"], delta=0.002)
                    self.assertGreaterEqual(lat["trigger_to_ack"], lat["trigger_to_send"])
                    self.assertGreaterEqual(lat["trigger_to_fill"], lat["trigger_to_ack"])
                    self.assertEqual(r["lat_ack_source"], "CLOB")
                    self.assertEqual(r["lat_fill_source"], fill_src)
                self.assertEqual(miss["lat"]["ack_to_fill"], 0.0)              # the reply said: nothing filled
                q = self.sn.summary()["exec_quality"]
                self.assertTrue(upd["partial"])                              # 5 of a 10+ share target
                self.assertEqual((q["attempted"], q["sent"], q["acked"], q["zero_fill"], q["partial"], q["pending"]),
                                 (2, 2, 2, 1, 1, 0))
                lat = self.sn.summary()["exec_latency"]
                self.assertEqual(lat["trigger_to_send"]["n"], 2)
                self.assertFalse(lat["trigger_to_send"]["simulated"])
            finally:
                await self.teardown_live()
        self.run_async(go())

    # ---- nothing between the trigger and the send but the order itself
    def test_no_disk_io_no_dashboard_between_trigger_and_send(self):
        async def go():
            await self.setup_live()
            events = []
            real_open, real_post = builtins.open, self.ex.sender.http.post_now

            def spy_open(file, *a, **k):
                events.append(("open", str(file)))
                return real_open(file, *a, **k)

            def spy_post(*a, **k):
                events.append(("post", None))
                return real_post(*a, **k)

            def spy_build(*a, **k):
                events.append(("dashboard", None))
            try:
                self.script("no_match")
                self.arm()
                self.ex.sender.http.post_now = spy_post
                with patch.object(builtins, "open", spy_open), \
                        patch("dashboard.sniper_layout.build", spy_build), patch("dashboard.build", spy_build):
                    self.sn.on_tick(self.raw("UP"))                    # trigger -> decide -> sign -> write: synchronous
                self.assertIn(("post", None), events)
                before = events[:events.index(("post", None))]
                self.assertEqual(before, [], before)
                await self.settle("UP")
            finally:
                self.ex.sender.http.post_now = real_post
                await self.teardown_live()
        self.run_async(go())

    # ---- one signal, one order
    def test_a_duplicate_callback_makes_one_order(self):
        async def go():
            await self.setup_live()
            try:
                self.script("no_match")
                self.script("no_match")
                self.arm()
                raw = self.raw("UP")
                self.sn.on_tick(raw)
                self.sn.on_tick(raw)                                   # the same message delivered twice
                trig = S.Trigger(0.5, 250)                             # and a second feed seeing the same move
                trig.update(time.monotonic() * 1000.0 - 300.0, 60_000.0)
                self.sn._signal("coinbase", trig, move_mid(60_000.0, 0.6), time.monotonic() * 1000.0,
                                time.perf_counter_ns())
                await self.settle("UP")
                await asyncio.sleep(0.05)
                self.assertEqual(len(self.posted()), 1, self.posted())
                self.assertEqual(self.sn.n["orders"], 1)
            finally:
                await self.teardown_live()
        self.run_async(go())

    # ---- an outstanding UP order does not hold a DOWN signal 150ms later
    def test_up_outstanding_down_at_150ms_submits_and_fills_stay_with_their_orders(self):
        async def go():
            await self.setup_live()
            try:
                up_oid, up_tid = self.script("matched", delay_s=0.4)        # UP's ACK is slow
                self.arm()
                self.sn.on_tick(self.raw("UP"))
                await asyncio.sleep(0.15)
                self.assertIsNotNone(self.sn.inflight["UP"])                # UP still waiting
                dn_oid, dn_tid = self.script("matched")
                self.arm(clear_cooldown=False)
                self.sn.on_tick(self.raw("DOWN"))                            # DOWN: its own cooldown is clear
                await self.settle("DOWN")
                await self.settle("UP", timeout=3.0)
                sides = [json.loads(json.dumps(o))["order"]["tokenId"] for o in self.posted()]
                self.assertEqual(sides, [T.UP, T.DN])                       # both sent, UP first
                self.store.record_trade(dn_tid, order_id=dn_oid, asset_id=T.DN, market=T.COND, side="BUY",
                                        price=0.45, size=6.0, status="MATCHED")
                self.store.record_trade(up_tid, order_id=up_oid, asset_id=T.UP, market=T.COND, side="BUY",
                                        price=0.55, size=4.0, status="MATCHED")
                self.sn._sync_live_fak()
                upd = {r["side"]: r for r in self.rows("live_fill_update")}
                self.assertEqual((upd["UP"]["order_id"], upd["UP"]["shares"]), (up_oid, 4.0))
                self.assertEqual((upd["DOWN"]["order_id"], upd["DOWN"]["shares"]), (dn_oid, 6.0))
                carry = self.sn.carry_view()
                self.assertGreater(carry["UP"], 0)                         # each side's carry from its own fill
                self.assertGreater(carry["DOWN"], 0)
                self.assertNotEqual(carry["UP"], carry["DOWN"])
            finally:
                await self.teardown_live()
        self.run_async(go())

    # ---- a delayed ACK
    def test_delayed_ack_holds_its_own_side_only_and_is_measured(self):
        async def go():
            await self.setup_live()
            try:
                self.script("no_match", delay_s=0.25)
                self.arm()
                self.sn.on_tick(self.raw("UP"))
                await asyncio.sleep(0.05)
                self.script("no_match")
                self.arm()
                self.sn.on_tick(self.raw("UP"))                              # same side, in flight: no order
                self.assertEqual(self.sn.n["skip: previous order in flight"], 1)
                self.arm(clear_cooldown=False)
                self.sn.on_tick(self.raw("DOWN"))                            # other side: sent
                await self.settle("DOWN")
                await self.settle("UP", timeout=3.0)
                self.assertEqual(len(self.posted()), 2)
                up = next(r for r in self.rows("refused") if r["side"] == "UP")
                self.assertGreaterEqual(up["lat"]["send_to_ack"], 240.0)
            finally:
                await self.teardown_live()
        self.run_async(go())

    # ---- a rejection
    def test_a_rejected_order_is_counted_and_never_retried(self):
        async def go():
            await self.setup_live()
            try:
                self.script(body={"success": False, "errorMsg": "order rejected: market is closed"}, status=400)
                self.arm()
                self.sn.on_tick(self.raw("UP"))
                await self.settle("UP")
                await asyncio.sleep(0.05)
                r = self.rows("refused")[-1]
                self.assertEqual(r["kind"], "REJECTED")
                self.assertEqual(len(self.posted()), 1)                       # no retry
                q = self.sn.summary()["exec_quality"]
                self.assertEqual((q["rejected"], q["zero_fill"], q["filled"]), (1, 0, 0))
            finally:
                await self.teardown_live()
        self.run_async(go())

    # ---- the ceiling stays anchored; a moving book makes a new reference, never a chase
    def test_moving_book_new_reference_no_chase(self):
        async def go():
            await self.setup_live()
            try:
                self.script("no_match")
                self.arm()
                self.sn.on_tick(self.raw("UP"))
                await self.settle("UP")
                now_ms = int(time.time() * 1000)
                self.book.apply_snapshot(T.UP, [(0.57, 500)], [(0.58, 500), (0.59, 500)], ts_ms=now_ms,
                                         require_exchange_ts=False)
                self.script("no_match")
                self.arm()
                self.sn.on_tick(self.raw("UP"))
                await self.settle("UP")
                caps = [r["cap"] for r in self.rows("refused")]
                self.assertEqual(caps, [0.6, 0.63])                          # ask + 0.05 at each decision
                for body, cap in zip(self.posted(), caps):
                    o = body["order"]
                    price = int(o["makerAmount"]) / int(o["takerAmount"])
                    # shares are rounded down, so maker/taker can sit a hair over the
                    # cap - never up to the next 1c tick, so no level past the cap fills
                    self.assertLess(price, cap + 0.01 - 1e-6)
                self.assertEqual(len(self.posted()), 2)                       # one POST per signal
            finally:
                await self.teardown_live()
        self.run_async(go())

    # ---- connectivity
    def test_disconnect_gap_stale_and_reconnect(self):
        async def go():
            await self.setup_live()
            try:
                def attempt():
                    self.arm()
                    self.sn.on_tick(self.raw("UP"))
                self.book.connected = False                                   # socket down
                attempt()
                self.assertEqual(self.sn.n["skip: book not live"], 1)
                self.book.connected = True
                self.book.desync_all("reconnect")                             # reconnected, not resynced
                attempt()
                self.assertEqual(self.sn.n["skip: book not live"], 2)
                self.book.apply_snapshot(T.UP, [(0.54, 500)], [(0.55, 500)], ts_ms=int(time.time() * 1000),
                                         require_exchange_ts=False)            # resynced
                self.book._meta[T.UP]["synced"] = False                      # a sequence gap desyncs it again
                attempt()
                self.assertEqual(self.sn.n["skip: book not live"], 3)
                self.book.apply_snapshot(T.UP, [(0.54, 500)], [(0.55, 500)], ts_ms=int(time.time() * 1000),
                                         require_exchange_ts=False)
                self.book._meta[T.UP]["updated"] = time.monotonic() - 100.0   # quiet past stale_after
                attempt()
                self.assertGreaterEqual(self.sn.n["skip: book not live"] + self.sn.n["skip: book stale"], 4)
                self.assertEqual(self.posted(), [])                           # nothing sent on any of them
                self.book.apply_snapshot(T.UP, [(0.54, 500)], [(0.55, 500)], ts_ms=int(time.time() * 1000),
                                         require_exchange_ts=False)
                self.script("no_match")
                attempt()                                                      # healthy again: it trades
                await self.settle("UP")
                self.assertEqual(len(self.posted()), 1)
            finally:
                await self.teardown_live()
        self.run_async(go())


# =========================================================================== performance
class Performance(unittest.TestCase):
    def test_10000_market_updates(self):
        res = B.bench_market_data(10_000)
        self.assertEqual(res["invalid"], 0)
        self.assertEqual(res["book_update_us"]["n"], 10_000)
        self.assertLess(res["book_update_us"]["p99"], 1000.0)

    def test_1000_triggers_rapid_up_down_switching_and_opposite_pairs(self):
        res = asyncio.run(B.bench_live(1000))
        self.assertEqual(res["orders_sent"], 1100)                            # 900 singles + 100 UP+DOWN pairs
        self.assertEqual(res["posted"], 1100)                                 # no duplicate POST
        self.assertEqual(res["skips"], {})
        self.assertEqual(res["matched_unresolved"], 0)
        self.assertEqual((res["connections"], res["new_connection_on_hot_path"]), (1, 0))
        self.assertLess(res["stages_ms"]["trigger_to_send"]["p99"], 20.0)     # generous: CI boxes vary


# =========================================================================== statistics
class Stats(unittest.TestCase):
    def test_latency_stats_and_na(self):
        st = X.LatencyStats(keep=100)
        self.assertEqual(st.summary(), {})
        for v in range(1, 101):
            st.add({"trigger_to_send": float(v)})
        d = st.summary()["trigger_to_send"]
        self.assertEqual((d["n"], d["last"], d["p50"], d["p95"], d["p99"], d["max"]), (100, 100.0, 51.0, 96.0, 100.0, 100.0))
        self.assertAlmostEqual(d["avg"], 50.5)
        st.add({"trigger_to_ack": 300.0}, simulated=("trigger_to_ack",))
        self.assertTrue(st.summary()["trigger_to_ack"]["simulated"])
        st.add({"trigger_to_ack": 40.0})
        self.assertFalse(st.summary()["trigger_to_ack"]["simulated"])         # measured wins over modelled

    def test_quality_counts(self):
        q = X.ExecQuality()
        q.observe({"live": True, "kind": "NO_MATCH", "result": "refused"})
        q.observe({"live": True, "kind": "MATCHED", "result": "pending"})
        q.resolve({"shares": 4.0, "partial": True, "price": 0.56, "ask_seen": 0.55})
        q.observe({"live": True, "kind": "REJECTED", "result": "refused", "reason": "market closed"})
        q.observe({"live": True, "kind": "AMBIGUOUS", "result": "refused"})
        s = q.summary()
        self.assertEqual((s["attempted"], s["sent"], s["acked"], s["zero_fill"], s["partial"], s["rejected"],
                          s["failed"], s["pending"]), (4, 4, 3, 1, 1, 1, 1, 0))
        self.assertAlmostEqual(s["avg_slip_c"], 1.0)


# =========================================================================== strategy equivalence
def load_old_sniper_mode():
    try:
        out = subprocess.run(["git", "show", f"{OLD_COMMIT}:sniper_mode.py"], cwd=ROOT, capture_output=True,
                             timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    path = pathlib.Path(tempfile.mkdtemp(prefix="old_strategy_")) / "sniper_mode_old.py"
    path.write_bytes(out.stdout)
    spec = importlib.util.spec_from_file_location("sniper_mode_old", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeTime:
    """time for one sniper_mode module: monotonic and time() are the replay's
    clock; everything else (perf counters, sleep) is real."""

    def __init__(self):
        self.mono, self.wall = 1_000.0, 1_791_374_000.0

    def __getattr__(self, name):
        return getattr(time, name)

    def monotonic(self):
        return self.mono

    def time(self):
        return self.wall


class ReplayBroker:
    """A paper broker answering from a script: the n-th order fills a fixed
    fraction of its target (full, partial, nothing, ...)."""
    FRACTIONS = (1.0, 0.6, 0.0, 0.3, 1.0, 0.0)
    trade_window_seconds = 300.0
    min_seconds_to_expiry = 10.0
    min_buy_price = 0.05
    max_buy_price = 0.95
    own_fill_ttl_s = 0

    def __init__(self):
        self.n = 0
        import threading
        self._local = threading.local()

    @property
    def last_fill(self):
        return getattr(self._local, "fill", None)

    @property
    def last_error(self):
        return getattr(self._local, "error", None)

    def place_trade(self, side, amount, up, down, cond, end, cap, _x, latency_ms=None, tag=None, **kw):
        f = self.FRACTIONS[self.n % len(self.FRACTIONS)]
        self.n += 1
        if f == 0:
            self._local.fill, self._local.error = None, "FAK no-fill: nothing at or below the cap"
            return False
        price = round(cap - 0.03, 4)
        shares = round(amount * f / price, 4)
        self._local.error = None
        self._local.fill = {"tag": tag, "order_id": f"r{self.n}", "shares": shares, "average_price": price,
                            "total_cost": round(shares * price, 4), "fee": 0.0, "partial": f < 1.0}
        return True


async def replay(mod, steps, env) -> list:
    """Feed one module's Sniper the same ticks and books; return its decisions."""
    from feeds.book import BookState
    fake = FakeTime()
    with patch.dict(os.environ, env):
        settings = mod.Settings()
    book = BookState(history_s=30)
    book.connected = True
    book.set_active([T.UP, T.DN])
    hub = types.SimpleNamespace(up_token=T.UP, down_token=T.DN, condition_id=T.COND, window_end=None, book=book)
    journal = pathlib.Path(tempfile.mkdtemp(prefix="replay_")) / "j.jsonl"
    with patch.object(mod, "time", fake):
        sn = mod.Sniper(hub, ReplayBroker(), journal_path=journal, settings=settings, trade_rows=[], own_rounds=True)
        sn.fresh.enabled = False
        for dt_ms, mid, ask_up, ask_dn, end in steps:
            fake.mono += dt_ms / 1000.0
            fake.wall += dt_ms / 1000.0
            hub.window_end = end
            hub.condition_id = f"{T.COND[:-6]}{end % 1_000_000:06d}"
            book.apply_snapshot(T.UP, [(round(ask_up - 0.01, 2), 500)], [(ask_up, 40), (round(ask_up + 0.01, 2), 500)],
                                ts_ms=int(fake.wall * 1000), require_exchange_ts=False)
            book.apply_snapshot(T.DN, [(round(ask_dn - 0.01, 2), 500)], [(ask_dn, 40), (round(ask_dn + 0.01, 2), 500)],
                                ts_ms=int(fake.wall * 1000), require_exchange_ts=False)
            with patch("feeds.book.time", fake):
                sn.on_tick(json.dumps({"b": f"{mid - 0.005:.3f}", "a": f"{mid + 0.005:.3f}"}))
                for _ in range(400):                                         # this order resolves before the next tick
                    if all(v is None for v in sn.inflight.values()):
                        break
                    await asyncio.sleep(0.002)
        for task in list(sn._tasks):
            task.cancel()
        await asyncio.gather(*sn._tasks, return_exceptions=True)
    keys = ("result", "reason", "side", "bp", "ask_seen", "cap", "amount", "shares", "target_shares", "carry_in",
            "carry_out", "round_end")
    rows = [json.loads(x) for x in journal.read_text(encoding="utf-8").splitlines()]
    return [{k: r.get(k) for k in keys} for r in rows if r.get("result") in ("filled", "refused", "skipped", "error")]


def replay_steps(seed: int, n: int):
    rng = random.Random(seed)
    mid, steps = 60_000.0, []
    base_end = 1_791_374_100
    for i in range(n):
        dt = rng.choice((40, 80, 120, 200, 260, 320))
        jump = rng.random()
        if jump < 0.15:
            mid *= 1 + rng.choice((-1, 1)) * rng.uniform(0.45, 1.2) / 1e4     # a move near the threshold
        else:
            mid *= 1 + rng.uniform(-0.05, 0.05) / 1e4
        ask_up = round(rng.choice((0.30, 0.45, 0.52, 0.55, 0.61, 0.97)), 2)
        ask_dn = round(min(0.99, 1.02 - ask_up), 2)
        end = base_end + 300 * (i // (n // 3 + 1))                            # three rounds: the carry must reset
        steps.append((dt, round(mid, 3), ask_up, ask_dn, end))
    return steps


class StrategyEquivalence(unittest.TestCase):
    def test_old_and_new_strategy_layers_decide_identically(self):
        old = load_old_sniper_mode()
        if old is None:
            self.skipTest(f"git history with {OLD_COMMIT} is not available")
        steps = replay_steps(7, 1500)
        # the replay's clock: each round's trade window is open (300s..10s left) for most steps
        steps = [(dt, mid, au, ad, end) for dt, mid, au, ad, end in steps]
        for mode_env in ({"SNIPER_LATENCY_MS": "5"}, {"SNIPER_LATENCY_MS": ""}):
            env = dict(SPEC_ENV, SNIPER_SHADOW_MS="", **mode_env)
            a = asyncio.run(replay(old, steps, env))
            b = asyncio.run(replay(S, steps, env))
            self.assertGreater(sum(1 for r in a if r["result"] in ("filled", "refused")), 20, "too few orders replayed")
            self.assertEqual(len(a), len(b))
            for i, (x, y) in enumerate(zip(a, b)):
                self.assertEqual(x, y, f"decision {i} differs ({mode_env})")


if __name__ == "__main__":
    unittest.main(verbosity=2)
