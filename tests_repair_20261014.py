#!/usr/bin/env python3
"""Regressions for the 2026-10-14 repair (audit C1, C2, H3 and the fill-store
index). No network, no wallet, temp directories only.

    python tests_repair_20261014.py

C1  the canary's send intents: one appended, fsynced line before each send
    (cost independent of the session's history), finished intents archived,
    v1/v2 state files migrated, crash recovery from the journal, a torn final
    line dropped, damage elsewhere failing closed.
C2  the ledger's indexed reads agree with a full recomputation through random
    fills, sells, settlements, late fills and failed writes; the order path's
    reads do not wait for a fill's disk flush; a write that fails after its
    fill was visible halts live trading; authorizations replay from the journal.
H3  the summary's percentiles equal the old full sort exactly; the dashboard
    computes the summary off the event loop.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import pathlib
import random
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from feeds.poly_user import FillStore  # noqa: E402

COND = "0x" + "ab" * 32


def intent(i: int, status: str = "NO_MATCH", order_id=None, spent=None) -> dict:
    d = {"status": status, "kind": status, "wall": time.time() - 60 + i * 1e-3, "end": time.time() + 200,
         "condition": COND, "token": "11", "side": "UP", "amount": 5.0, "reserve": 5.35, "order_type": "FAK",
         "price": 0.5, "body_sha256": "e" * 64, "signed_order": {"salt": i, "signature": "0x" + "c" * 130},
         "order_id": order_id}
    if spent is not None:
        d["spent"] = spent
    return d


def prepared(key: str, side: str = "UP", amount: float = 5.0) -> L.Prepared:
    p = L.Prepared(side, "11", COND, time.time() + 120, amount, .52, .52, "0.01", key,
                   json.dumps({"order": {"salt": key, "signature": "throwaway"}}), {}, {}, {}, "FAK")
    p.reserve = amount * 1.07
    return p


class Workspace(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        self.state = self.dir / "live_canary_state.json"
        self.ledger = Ledger(path=str(self.dir / "live_ledger.json"))
        self.ledger.save()
        self.sessions = []

    def tearDown(self):
        for s in self.sessions:
            s.close()
        self.tmp.cleanup()

    def boot(self, *, new=False):
        limits, kill = L.CanaryLimits(max_orders=10 ** 9, max_fills=10 ** 9, max_rounds=10 ** 9,
                                      max_exposure_usd=1e12, max_daily_loss_usd=1e12), L.KillSwitch()
        s = L.CanarySession(self.state, "wallet")
        s.open(limits, kill, self.ledger, new_session=new)
        self.sessions.append(s)
        return s, limits, kill

    def write_v2(self, intents: dict, **extra) -> None:
        state = {"version": 2, "session_id": "s" * 32, "wallet": "wallet", "started_wall": time.time() - 3600,
                 "orders": len(intents), "fills": 0, "exposure_usd": 0.0, "rounds": [COND], "kill_reasons": [],
                 "pending_fills": 0, "intents": intents, **extra}
        self.state.write_text(json.dumps(state), encoding="utf-8")


# =========================================================================== C1
class C1WriteAhead(Workspace):
    def test_each_send_appends_one_record_whatever_the_history(self):
        self.write_v2({f"old-{i}": intent(i) for i in range(3000)})
        s, limits, kill = self.boot()
        self.assertEqual(s.intents, {}, "finished intents leave the hot state at open")
        self.assertEqual(len(s.archive_path.read_text().splitlines()), 3000)
        seg = s._segment_path(s._segment)
        appended = []
        for k in range(5):
            before = seg.stat().st_size if seg.exists() else 0
            with mock.patch.object(L.CanarySession, "state", side_effect=AssertionError("no full state on send")):
                s.before_send(prepared(f"new-{k}"), limits, kill)
            appended.append(seg.stat().st_size - before)
        self.assertTrue(all(0 < b < 2000 for b in appended), appended)
        # one short record each (a few bytes vary: timestamps), with 3,000 orders of history behind it
        self.assertLess(max(appended) - min(appended), 64, appended)

    def test_the_intent_is_durable_before_the_call_returns(self):
        s, limits, kill = self.boot()
        synced = []
        real = L.CanarySession._sync
        with mock.patch.object(L.CanarySession, "_sync", side_effect=lambda fd: (synced.append(fd), real(fd))):
            s.before_send(prepared("k1"), limits, kill)
        self.assertEqual(len(synced), 1)
        view = L.CanarySession.durable_view(self.state)
        self.assertEqual(view["intents"]["k1"]["status"], "DISPATCHING")
        self.assertEqual((view["orders"], view["pending_fills"]), (1, 1))

    def test_a_failed_append_halts_and_nothing_may_be_sent(self):
        s, limits, kill = self.boot()
        with mock.patch.object(L.os, "write", side_effect=OSError("disk full")):
            with self.assertRaises(L.Halted):
                s.before_send(prepared("k1"), limits, kill)
        self.assertTrue(kill.tripped)

    def test_duplicate_intent_key_halts(self):
        s, limits, kill = self.boot()
        s.before_send(prepared("k1"), limits, kill)
        with self.assertRaises(L.Halted):
            s.before_send(prepared("k1"), limits, kill)

    def test_crash_with_an_unresolved_intent_halts_the_next_start(self):
        s, limits, kill = self.boot()
        s.before_send(prepared("k1"), limits, kill)          # the process dies here (no outcome)
        _s2, limits2, kill2 = self.boot()
        self.assertTrue(kill2.tripped)
        self.assertEqual((limits2.orders, limits2.pending_fills), (1, 1))
        with self.assertRaisesRegex(RuntimeError, "unresolved"):
            self.boot(new=True)

    def test_outcomes_and_resolutions_survive_a_restart(self):
        s, limits, kill = self.boot()
        p = prepared("k1")
        s.before_send(p, limits, kill)
        limits.record_outcome(L.NO_MATCH, p.amount)
        s.outcome(p, {"kind": L.NO_MATCH, "order_id": None}, limits, kill)
        p2 = prepared("k2", "DOWN")
        s.before_send(p2, limits, kill)
        limits.record_outcome(L.MATCHED, p2.amount)
        s.outcome(p2, {"kind": L.MATCHED, "order_id": "0xo2"}, limits, kill)
        s.resolve("k2", "CONFIRMED", limits, kill, spent=4.0)
        s.flush()
        _s2, limits2, kill2 = self.boot()
        self.assertFalse(kill2.tripped)
        self.assertEqual((limits2.orders, limits2.fills, limits2.pending_fills), (2, 1, 0))

    def test_a_lost_async_outcome_leaves_the_intent_unresolved(self):
        s, limits, kill = self.boot()
        p = prepared("k1")
        s.before_send(p, limits, kill)
        with mock.patch.object(s, "_submit"):                # the writer never got to it: a crash
            s.outcome(p, {"kind": L.NO_MATCH, "order_id": None}, limits, kill)
        _s2, _l2, kill2 = self.boot()
        self.assertTrue(kill2.tripped, "a record lost in a crash can only make the restart halt")

    def test_torn_final_line_is_dropped_and_damage_elsewhere_fails_closed(self):
        s, limits, kill = self.boot()
        s.before_send(prepared("k1"), limits, kill)
        seg = s._segment_path(s._segment)
        with open(seg, "ab") as fh:
            fh.write(b'{"seq": 99, "op": "intent", "key": "k2", "int')     # the append that never returned
        s2, _l, kill2 = self.boot()
        self.assertIn("k1", s2.intents)
        self.assertNotIn("k2", s2.intents)
        # damage in a line that is not the last: refuse to start
        seg2 = s2._segment_path(s2._segment)
        s2.before_send(prepared("k3"), _l, kill2)
        raw = seg2.read_bytes()
        seg2.write_bytes(b"{garbage}\n" + raw)
        with self.assertRaises(RuntimeError):
            self.boot()

    def test_replayed_records_are_idempotent(self):
        s, limits, kill = self.boot()
        s.before_send(prepared("k1"), limits, kill)
        seg = s._segment_path(s._segment)
        seg.write_bytes(seg.read_bytes() * 2)                  # every record twice
        s2, limits2, _k = self.boot()
        self.assertEqual(limits2.orders, 1)
        self.assertEqual(list(s2.intents), ["k1"])

    def test_migration_from_v2_keeps_a_backup_and_archives_finished_intents(self):
        intents = {f"old-{i}": intent(i) for i in range(10)}
        intents["open-1"] = intent(99, status="ACCEPTED", order_id="0xopen")
        self.write_v2(intents)
        s, limits, kill = self.boot()
        self.assertEqual(s.migrated_from, 2)
        self.assertTrue(self.state.with_name(self.state.name + ".v2.bak").exists())
        self.assertEqual(json.loads(self.state.read_text())["version"], L.CanarySession.VERSION)
        self.assertEqual(list(s.intents), ["open-1"])
        self.assertTrue(kill.tripped, "the accepted-but-unconfirmed intent still halts")
        self.assertEqual(len(s.archive_path.read_text().splitlines()), 10)

    def test_archived_confirmed_spend_is_used_on_restart(self):
        end = (int(time.time()) // 300 + 1) * 300             # a round end (the validator requires it)
        self.ledger.authorize_order("0xf", {"condition_id": COND, "token_id": "11", "window_end": end,
                                            "requested_notional": 5.0})
        self.ledger.record_fill_durable("t1", "11", shares=4.0, price=0.5, condition_id=COND, fee=0.0, order_id="0xf")
        self.ledger.save()
        self.write_v2({"k1": intent(1, status="CONFIRMED", order_id="0xf", spent=2.0)})
        s, limits, _k = self.boot()                             # migration archives it
        self.assertEqual(s.intents, {})
        _s2, limits2, _k2 = self.boot()                         # it is read back from the archive
        self.assertAlmostEqual(limits2.exposure_usd, 2.0)

    def test_compaction_racing_sends_loses_nothing(self):
        s, limits, kill = self.boot()
        stop = threading.Event()

        def compact():
            while not stop.is_set():
                s.save(limits, kill)
        t = threading.Thread(target=compact)
        t.start()
        try:
            for i in range(200):
                p = prepared(f"k{i}", "UP" if i % 2 else "DOWN")
                s.before_send(p, limits, kill)
                limits.record_outcome(L.NO_MATCH, p.amount)
                s.outcome(p, {"kind": L.NO_MATCH, "order_id": None}, limits, kill)
        finally:
            stop.set()
            t.join()
        s.flush()
        _s2, limits2, kill2 = self.boot()
        self.assertEqual((limits2.orders, limits2.pending_fills), (200, 0))
        self.assertFalse(kill2.tripped)
        archived = {json.loads(x)["key"] for x in s.archive_path.read_text().splitlines()}
        self.assertEqual(len(archived | set(_s2.intents)), 200)

    def test_halts_recorded_by_persist_survive(self):
        s, limits, kill = self.boot()
        kill.trip("canary daily loss $25.00 >= $20.00")
        self.assertTrue(s.persist(limits, kill))
        _s2, _l2, kill2 = self.boot()
        self.assertEqual(kill2.reasons, ["canary daily loss $25.00 >= $20.00"])

    def test_persist_writes_only_when_something_changed(self):
        s, limits, kill = self.boot()
        seg = s._segment_path(s._segment)
        size = seg.stat().st_size if seg.exists() else 0
        for _ in range(5):
            self.assertTrue(s.persist(limits, kill))
        self.assertEqual(seg.stat().st_size if seg.exists() else 0, size)


# =========================================================================== C2
def brute(led: Ledger, since: float, orders):
    """The reads the order path makes, recomputed the slow way."""
    pos = list(led.positions.values())
    realized = sum(float(p.realized or 0) for p in pos if p.settled and float(p.settled_wall or 0) >= since)
    open_basis = sum(float(p.cost) for p in pos if not p.settled and p.shares > 0)
    booked = {}
    for oid, _tok in orders:
        if oid and oid not in booked:
            booked[oid] = sum(float(l.cost) for p in pos for l in p.lots
                              if l.order_id == oid and str(l.side).upper() == "BUY")
    return realized, open_basis, booked


class C2Indexes(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "l.json")

    def tearDown(self):
        self.tmp.cleanup()

    def check_all(self, led: Ledger, since: float):
        conds = {p.condition_id for p in led.positions.values()}
        tokens = list(led.positions)
        orders = [(l.order_id, l.token_id) for p in led.positions.values() for l in p.lots if l.order_id]
        r, o, b = led.risk_view(since, orders)
        R, O, B = brute(led, since, orders)
        self.assertTrue(math.isclose(r, R, abs_tol=1e-9) and math.isclose(o, O, abs_tol=1e-9), (r, R, o, O))
        self.assertEqual(set(b), set(B))
        for k in b:
            self.assertTrue(math.isclose(b[k], B[k], abs_tol=1e-9))
        self.assertTrue(math.isclose(led.realized_since(since), R, abs_tol=1e-9))
        self.assertTrue(math.isclose(led.unsettled_cost(), O, abs_tol=1e-9))
        for t in tokens:
            p = led.positions[t]
            want = float(p.shares) if (not p.settled and p.shares > 0) else 0.0
            self.assertEqual(led.open_leg_shares(p.condition_id, t), want)
        for c in conds:
            self.assertEqual(led.held_tokens_for_condition(c),
                             frozenset(p.token_id for p in led.positions.values()
                                       if p.condition_id == c and p.shares > 0 and not p.settled))
            want = sum(l.cost for p in led.positions.values() if p.condition_id == c for l in p.lots)
            self.assertTrue(math.isclose(led.confirmed_cost_for_condition(c), want, abs_tol=1e-9))
        s = led.summary()
        settled = [p for p in led.positions.values() if p.settled]
        self.assertTrue(math.isclose(s["realized_pnl"], sum(p.realized or 0 for p in settled), abs_tol=1e-9))
        self.assertEqual(s["open_positions"], sum(1 for p in led.positions.values() if not p.settled and p.shares > 0))
        self.assertTrue(math.isclose(s["total_cost"], sum(p.cost for p in led.positions.values()), abs_tol=1e-9))

    def test_indexed_reads_match_a_full_recomputation(self):
        from accounting.resolution import RESOLVED, Resolution
        rng = random.Random(7)
        led = Ledger(path=self.path, allow_sells=True)
        led.save()
        since = time.time() - 1
        conds = [f"0x{k:064x}" for k in range(1, 7)]
        n = 0
        for step in range(400):
            c = rng.choice(conds)
            tok = f"{conds.index(c) * 2 + rng.randint(1, 2)}"
            act = rng.random()
            n += 1
            try:
                if act < 0.55:
                    led.record_fill_durable(f"t{n}", tok, shares=rng.uniform(1, 20), price=rng.uniform(0.05, 0.95),
                                            condition_id=c, fee=rng.uniform(0, 0.1),
                                            order_id=f"o{tok}-{n % 5}")     # an order fills one token only
                elif act < 0.65 and tok in led.positions and led.positions[tok].shares > 1:
                    led.record_fill_durable(f"t{n}", tok, shares=min(led.positions[tok].shares, rng.uniform(0.5, 3)),
                                            price=rng.uniform(0.05, 0.95), side="SELL", condition_id=c, fee=0.01)
                elif act < 0.80:
                    held = {p.token_id for p in led.positions.values() if p.condition_id == c}
                    toks = {f"{conds.index(c) * 2 + 1}", f"{conds.index(c) * 2 + 2}"} | held
                    win = rng.choice(sorted(toks))
                    led.settle(Resolution(c, RESOLVED, {t: 1.0 if t == win else 0.0 for t in toks}))
                elif act < 0.90:
                    with mock.patch.object(led, "_persist_locked", return_value=False):
                        with self.assertRaises(RuntimeError):
                            led.record_fill_durable(f"t{n}", tok, shares=2.0, price=0.5, condition_id=c, fee=0.0)
                    led.persistence_halt = None
                else:
                    # a late fill on a settled position reopens it
                    led.record_fill_durable(f"t{n}", tok, shares=1.0, price=0.5, condition_id=c, fee=0.0)
            except (ValueError, RuntimeError):
                pass
            if step % 20 == 0:
                self.check_all(led, since)
        self.check_all(led, since)
        again = Ledger(path=self.path, allow_sells=True)            # journal replay rebuilds the same indexes
        self.check_all(again, since)

    def test_positions_changed_behind_the_ledger_are_reindexed(self):
        led = Ledger(path=self.path)
        led.record_fill("t1", "11", shares=5, price=0.5, condition_id=COND, fee=0.0)
        from accounting.ledger import Position
        led.positions["22"] = Position(token_id="22", condition_id=COND, shares=3.0, cost=1.5)
        self.assertEqual(led.open_leg_shares(COND, "22"), 3.0)
        self.assertTrue(math.isclose(led.unsettled_cost(), 2.5 + 1.5))
        led.positions["11"].settled, led.positions["11"].settled_wall, led.positions["11"].realized = True, time.time(), 1.0
        self.assertEqual(led.open_leg_shares(COND, "11"), 0.0)
        self.assertTrue(math.isclose(led.realized_since(0), 1.0))

    def test_order_path_reads_do_not_wait_for_a_fill_flush(self):
        led = Ledger(path=self.path)
        led.save()
        entered, release = threading.Event(), threading.Event()
        real = led._append_journal

        def slow(entry):
            entered.set()
            release.wait(5)
            return real(entry)
        with mock.patch.object(led, "_append_journal", side_effect=slow):
            t = threading.Thread(target=led.record_fill_durable, args=("t1", "11"),
                                 kwargs=dict(shares=5, price=0.5, condition_id=COND, fee=0.0))
            t.start()
            self.assertTrue(entered.wait(5))
            t0 = time.perf_counter()
            led.open_leg_shares(COND, "11")
            led.risk_view(0.0, [])
            waited = time.perf_counter() - t0
            release.set()
            t.join()
        self.assertLess(waited, 0.5, "the reads must not wait for the disk flush")

    def test_a_write_failing_after_the_fill_was_visible_halts_live(self):
        led = Ledger(path=self.path)
        led.save()
        with mock.patch.object(led, "_persist_locked", return_value=False):
            with self.assertRaises(RuntimeError):
                led.record_fill_durable("t1", "11", shares=5, price=0.5, condition_id=COND, fee=0.0)
        self.assertNotIn("t1", led.seen)
        self.assertTrue(led.persistence_halt)
        ex = types.SimpleNamespace(ledger=led, kill=L.KillSwitch(), _announced=set(), on_event=lambda *a: None)
        ex._trip = lambda reason: L.LiveExecution._trip(ex, reason)
        with self.assertRaises(L.Halted):
            L.LiveExecution._check_accounting_integrity(ex)

    def test_authorizations_are_journaled_and_replayed(self):
        led = Ledger(path=self.path)
        led.save()
        size = os.path.getsize(self.path)
        receipt = {"condition_id": COND, "token_id": "11", "window_end": (int(time.time()) // 300 + 1) * 300,
                   "requested_notional": 5.0, "fee_rate": 0.07, "fee_exponent": 1}
        self.assertTrue(led.authorize_order_durable("0xa", receipt, venue_min_shares=5.0, price_cap=0.95))
        self.assertEqual(os.path.getsize(self.path), size, "no full rewrite for an authorization")
        again = Ledger(path=self.path)
        self.assertEqual(again.authorized_orders["0xa"], led.authorized_orders["0xa"])
        self.assertFalse(led.authorize_order_durable("bad id with spaces", receipt))

    def test_a_damaged_authorization_entry_fails_the_load(self):
        led = Ledger(path=self.path)
        led.save()
        with open(led.journal_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"seq": 1, "op": "authorize", "order_id": "0xa", "metadata": "nope"}) + "\n")
        with self.assertRaises(RuntimeError):
            Ledger(path=self.path)


# =========================================================================== H3 + fill store
class H3Summary(unittest.TestCase):
    def test_quantiles_equal_the_old_full_sort(self):
        rng = random.Random(3)
        for n in (1, 2, 7, 100, 2047, 2048, 5000, 20000):
            xs = [rng.choice((rng.gauss(46, 8), 46.0, 0.0)) for _ in range(n)]
            ys = sorted(xs)
            q = lambda p: round(ys[min(n - 1, int(n * p / 100))], 1)   # noqa: E731 - the old _q
            want = {"p50": q(50), "p90": q(90), "p95": q(95), "p99": q(99), "max": round(ys[-1], 1), "n": n}
            self.assertEqual(S.quantiles(xs), want)
            self.assertEqual(S.BinanceFreshness._q(xs), want)
        self.assertIsNone(S.quantiles([]))

    def test_summary_is_computed_off_the_event_loop_by_the_dashboard(self):
        import run
        threads = []
        sn = types.SimpleNamespace()
        sn.summary_snapshot = lambda: {"x": 1}

        def summary_from(snap):
            threads.append(threading.get_ident())
            return {"signals": 1}
        sn.summary_from = summary_from
        sn.summary = lambda: (_ for _ in ()).throw(AssertionError("summary() on the loop"))
        rt = types.SimpleNamespace(sniper=sn, hub=types.SimpleNamespace(health=lambda: {}), ledger=None,
                                   journal_path=os.devnull, started=time.time())

        async def go():
            view = run.DashView(rt, accounting_every_s=0.0, accounting_off_loop=True)
            view._refresh_slow()
            await asyncio.sleep(0.05)
            view._slow_at = -1e9
            view._refresh_slow()
            return view._slow["summary"]
        out = asyncio.run(go())
        self.assertEqual(out, {"signals": 1})
        self.assertTrue(threads and threads[0] != threading.main_thread().ident)


class H4BacklogGate(unittest.TestCase):
    """The receive-backlog gate: unchanged by default, measured, and the
    age gate / coalescing behind flags that LIVE refuses."""

    def armed(self, **env):
        from tests_sniper_binance_fut import armed
        sn, fired = armed(extra_env=env)
        g = sn.bf_fresh
        g.enabled = g.connected = True
        return sn, fired, g

    def burst_move(self, sn, g, lag_ms):
        from tests_sniper_binance_fut import age_fut, fut
        g.note_message(time.monotonic(), 0)
        sn.on_binance_fut(fut(60_000.0, lag_ms=80.0))
        age_fut(sn, 300)
        g.note_message(time.monotonic(), g.backlog_max + 30)        # 50 frames queued behind this one
        sn.on_binance_fut(fut(60_012.0, lag_ms=lag_ms))               # +2bp in 300ms

    def test_default_count_gate_still_refuses_a_fresh_burst_and_counts_it(self):
        sn, fired, g = self.armed()
        self.assertEqual(g.mode, "count")
        self.burst_move(sn, g, lag_ms=80.0)
        self.assertEqual(fired, [])
        self.assertEqual(sn.n["backlog refusals with fresh data binance_fut"], 1)

    def test_age_gate_passes_a_fresh_burst_but_not_a_stall(self):
        sn, fired, g = self.armed(SNIPER_BACKLOG_GATE="age")
        self.burst_move(sn, g, lag_ms=80.0)
        self.assertEqual(fired, [("UP", 2.0, "binance_fut")])
        sn2, fired2, g2 = self.armed(SNIPER_BACKLOG_GATE="age")
        self.burst_move(sn2, g2, lag_ms=700.0)                         # frames that waited: still refused
        self.assertEqual(fired2, [])
        self.assertEqual(sn2.n["backlog refusals stale binance_fut"], 1)

    def test_coalesced_frames_are_history_and_only_the_newest_is_evaluated(self):
        from tests_sniper_binance_fut import age_fut, fut
        sn, fired, g = self.armed(SNIPER_FEED_COALESCE="1")
        g.enabled = False
        sn.on_binance_fut(fut(60_000.0))
        age_fut(sn, 300)
        sn.on_binance_fut(fut(60_012.0), evaluate=False)              # queued behind a newer frame
        self.assertEqual(fired, [])
        self.assertEqual(len(sn.triggers["binance_fut"].q), 2)        # folded into the history
        sn.on_binance_fut(fut(60_013.0))                              # the newest: evaluated
        self.assertEqual(len(fired), 1)

    def test_live_refuses_both_options_until_approved(self):
        import exec_latency as X
        from tests_sniper_coinbase import env
        for extra in ({"SNIPER_BACKLOG_GATE": "age"}, {"SNIPER_FEED_COALESCE": "1"}):
            with env(SNIPER_LATENCY_MS="200", **extra):
                bad = X.spec_violations(S.Settings())
            self.assertTrue(any("approved" in v for v in bad), bad)
        with env(SNIPER_LATENCY_MS="200"):
            self.assertFalse(any("approved" in v for v in X.spec_violations(S.Settings())))

    def test_an_out_of_order_update_id_is_counted(self):
        from tests_sniper_binance_fut import fut
        sn, _fired, _g = self.armed()
        sn.on_binance_fut(fut(60_000.0, u=10))
        sn.on_binance_fut(fut(60_000.0, u=9))
        self.assertEqual(sn.n["binance_fut out of order"], 1)


class L8CoinbaseSockets(unittest.TestCase):
    def test_a_lagging_copy_socket_does_not_veto_the_socket_that_delivers_the_tick(self):
        """Audit L8, re-read: each socket records its own backlog immediately
        before its message is handled, so the gate judges the delivering
        socket. No code change; this pins the behaviour."""
        from tests_sniper_coinbase import age, make, ticker
        sn, fired = make()
        g = sn.cb_fresh
        g.enabled = g.connected = True
        g.note_message(time.monotonic(), 0)
        sn.on_coinbase(ticker(60_000.0))
        age(sn, 300)
        g.note_message(time.monotonic(), 500)                         # socket 2: far behind (its copies are late)
        g.note_message(time.monotonic(), 0)                           # socket 1: its own message, read now
        sn.on_coinbase(ticker(60_012.0))
        self.assertEqual(len(fired), 1)


class FillStoreIndex(unittest.TestCase):
    def test_fills_for_order_matches_a_full_scan(self):
        rng = random.Random(5)
        st = FillStore()
        for i in range(300):
            oid = f"o{rng.randint(0, 20)}"
            st.record_trade(f"t{rng.randint(0, 250)}", order_id=oid if rng.random() < 0.8 else None, asset_id="11",
                            market=COND, side="BUY", price=0.5, size=1.0,
                            status=rng.choice(("MATCHED", "CONFIRMED")))
            if i == 150:
                st.fills.pop(next(iter(st.fills)))                 # changed behind the store's back
        for k in range(21):
            oid = f"o{k}"
            want = sorted(f.trade_id for f in st.fills.values() if f.order_id == oid)
            self.assertEqual(sorted(f.trade_id for f in st.fills_for_order(oid)), want)


if __name__ == "__main__":
    unittest.main(verbosity=1)
