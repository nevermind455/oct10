#!/usr/bin/env python3
"""Stress, race and event-order fuzzing of the whole paper bot (no network).

    python tests_stress.py                 # full size: 10,000 races + 10,000 scenarios
    STRESS_SCALE=0.1 python tests_stress.py   # a tenth, for a quick look

A. BROKER RACES (10,000 iterations). The real PaperBroker and Ledger, four
   orders at once on one market - sniper UP + DOWN (FAK) and manager UP + DOWN
   (FOK) - with random books, random flight times, a settlement holding the
   ledger lock (its fsync) and a dashboard reading it without waiting. Checked
   against an oracle after every iteration: no displayed share bought twice,
   cash = start - sum(cost) and never negative, one ledger lot per accepted
   order, no duplicate trade id, nothing booked into another market, FOK fills
   are whole, every fill at or under its cap, no thread left hanging.

B. FULL SYSTEM (10,000 scenarios). The real Sniper (its _signal gates,
   decision, IOC sizing, carry, cooldown), PaperBroker, PositionManager (paper,
   enabled), SettlementWorker and BookState, on one event loop as in the bot,
   driven by random event sequences: book snapshots and deltas (some invalid
   or crossed), disconnects and resyncs, UP/DOWN signals, manager evaluations
   and sends, market switches with fills still in flight, results (some
   duplicated, some failing) and settlement. After every scenario the counts
   the audit asked for are taken (see COUNTS) and must be zero.

C. RESOURCES. One bot through 600 market rotations: threads, tasks, memory,
   per-market maps and file handles at the start and the end.
"""
from __future__ import annotations

import asyncio
import collections
import gc
import math
import os
import pathlib
import random
import sys
import tempfile
import threading
import time
import tracemalloc
import types
import unittest
from dataclasses import replace
from decimal import Decimal as D
from fractions import Fraction as F
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import position_manager as PM  # noqa: E402
import sniper_mode as S  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from accounting import settlement as ST  # noqa: E402
from accounting.resolution import PENDING, RESOLVED, Resolution  # noqa: E402
from feeds.book import BookState  # noqa: E402
from paper_trade import BookSnapshot, MarketRules, PaperBroker, snapshot_from_book_view  # noqa: E402

SCALE = float(os.environ.get("STRESS_SCALE", "1") or 1)
N_RACES = max(1, int(10_000 * SCALE))
N_SCENARIOS = max(1, int(10_000 * SCALE))
FEE_RATE = D("0.07")
QUIET = open(os.devnull, "w")

COUNTS = ("crashes", "uncaught_exceptions", "deadlocks", "duplicate_sniper_orders", "duplicate_manager_orders",
          "duplicate_fills_applied", "negative_shares", "negative_pending", "exposure_violations",
          "pair_threshold_violations", "cross_market_corruptions", "stale_book_orders", "accounting_mismatches",
          "settlement_double_counts", "manager_caused_sniper_misses", "real_manager_orders_sent",
          "depth_used_twice", "fill_above_cap", "partial_fok")


def rules_for(ctx):
    return lambda cid: MarketRules(cid, FEE_RATE, 1, min_order_size=D("5"), tick_size=D("0.01"),
                                   up_token_id=ctx["up"], down_token_id=ctx["down"])


def no_live_client(*_a, **_k):
    raise AssertionError("a live client was requested in paper")


# =========================================================================== A. broker races
class BrokerRaces(unittest.TestCase):
    def test_10000_concurrent_order_races(self):
        rng = random.Random(1007)
        counts = collections.Counter()
        start_cash = 100_000.0
        per_broker = 250
        orders_total = 0
        t0 = time.perf_counter()
        for batch in range(0, N_RACES, per_broker):
            with tempfile.TemporaryDirectory() as tmp:
                tmp = pathlib.Path(tmp)
                ctx = {"cond": None, "up": None, "down": None, "books": {}}
                led = Ledger(path=str(tmp / "l.json"))
                br = PaperBroker(
                    led, market_context=lambda: {"condition_id": ctx["cond"], "up_token_id": ctx["up"],
                                                 "down_token_id": ctx["down"]},
                    host="https://public.invalid", max_buy_price=0.95, min_buy_price=0.05,
                    start_balance=start_cash, account_path=tmp / "a.json", audit_path=tmp / "o.jsonl",
                    book_fetch=lambda tok: ctx["books"][str(tok)], rules_fetch=rules_for(ctx),
                    min_seconds_to_expiry=0, trade_window_seconds=300, own_fill_ttl_s=2.0)
                br._audit = lambda row: None                       # the audit file is not under test
                cond_tokens = {}
                for it in range(batch, min(batch + per_broker, N_RACES)):
                    cid = "0x" + f"{it + 1:064x}"
                    up, dn = f"{10 * it + 1}", f"{10 * it + 2}"
                    ctx.update(cond=cid, up=up, down=dn)
                    cond_tokens[up] = cond_tokens[dn] = cid
                    books = {}
                    for tok in (up, dn):
                        ask0 = D(rng.randrange(10, 85)) / 100
                        levels = []
                        for k in range(rng.randrange(1, 5)):
                            levels.append((ask0 + D(k) / 100, D(rng.randrange(5, 3000)) / 100))
                        books[tok] = BookSnapshot(tok, tuple(levels), min_order_size=D("5"), tick_size=D("0.01"),
                                                  timestamp=str(int(time.time() * 1000)), book_hash="h",
                                                  received_wall=time.time(), best_bid=ask0 - D("0.01"))
                    ctx["books"] = books
                    end = time.time() + 200
                    jobs, results = [], {}
                    for tag, side in (("sniper", "UP"), ("sniper", "DOWN"), ("manager", "UP"), ("manager", "DOWN")):
                        if rng.random() < 0.25:
                            continue
                        tok = up if side == "UP" else dn
                        ask0 = books[tok].asks[0][0]
                        fak = tag == "sniper"
                        cap = float(ask0 + (D("0.05") if fak else D("0.02")))
                        amount = round(rng.uniform(1.0, 12.0), 2)
                        lat = rng.choice((0.0, 0.0, 0.5, 1.0, 3.0))
                        hold = rng.choice((0.0, 0.0, 0.001, 0.002))

                        def guard(hold=hold):
                            if hold:
                                time.sleep(hold)
                            return True

                        def run(tag=tag, side=side, tok=tok, fak=fak, cap=cap, amount=amount, lat=lat, guard=guard):
                            try:
                                ok = br.place_trade(side, amount, up, dn, cid, end, cap, None, latency_ms=lat,
                                                    tag=tag, fak=fak, pre_submit_guard=guard)
                                results[(tag, side)] = (ok, br.last_fill if ok else None, br.last_error, cap, fak,
                                                        amount, tok)
                            except Exception as exc:                  # noqa: BLE001 - counted
                                results[(tag, side)] = ("EXC", f"{type(exc).__name__}: {exc}")
                        jobs.append(threading.Thread(target=run))
                    if rng.random() < 0.3:                               # a settlement's fsync holds the ledger
                        hold_s = rng.choice((0.001, 0.003))

                        def settle_hold(hold_s=hold_s):
                            with led._save_lock, led._lock:
                                time.sleep(hold_s)
                        jobs.append(threading.Thread(target=settle_hold))
                    if rng.random() < 0.3:                               # the dashboard never waits
                        def dash():
                            if led._lock.acquire(blocking=False):
                                try:
                                    br.cash_balance()
                                finally:
                                    led._lock.release()
                        jobs.append(threading.Thread(target=dash))
                    with patch("sys.stdout", QUIET):
                        rng.shuffle(jobs)
                        for j in jobs:
                            j.start()
                        for j in jobs:
                            j.join(10)
                    if any(j.is_alive() for j in jobs):
                        counts["deadlocks"] += 1
                        continue
                    # ---- the oracle
                    used = collections.Counter()
                    for key, res in results.items():
                        if res[0] == "EXC":
                            counts["crashes"] += 1
                            continue
                        ok, fill, err, cap, fak, amount, tok = res
                        orders_total += 1
                        if ok:
                            if fill is None or fill.get("tag") != key[0] or fill.get("token_id") != tok:
                                counts["duplicate_fills_applied"] += 1          # a fill read from another order
                                continue
                            for p, sh, _n in fill["levels"]:
                                used[(tok, round(p, 4))] += F(str(sh))
                                if p > cap + 1e-9:
                                    counts["fill_above_cap"] += 1
                            if not fak:
                                # FOK: never partial, never less than the stake; raised above it only
                                # as far as 5 shares AT THE LIMIT (the venue's minimum on the order as
                                # signed: size = stake / limit)
                                spent = F(str(fill["requested_amount"]))
                                stake = F(str(round(amount, 2)))
                                left, alone = stake, F(0)
                                for px, q in ctx["books"][tok].asks:
                                    if float(px) > cap + 1e-9 or left <= 0:
                                        break
                                    take = min(left, F(str(px)) * F(str(q)))
                                    alone += take / F(str(px))
                                    left -= take
                                if (fill.get("partial") or spent < stake - F(1, 200)
                                        or fill["shares"] < 5 - 1e-6
                                        or spent > max(stake, F(math.ceil(5 * cap * 100 - 1e-9), 100))
                                        + F(1, 100)):
                                    counts["partial_fok"] += 1
                                    if os.environ.get("STRESS_DEBUG"):
                                        print("FOKDBG", amount, cap, fill["requested_amount"], fill["shares"],
                                              fill.get("partial"), float(alone),
                                              [(float(a), float(b)) for a, b in ctx["books"][tok].asks],
                                              file=sys.stderr)
                    for (tok, p), sh in used.items():
                        shown = sum(F(str(q)) for px, q in ctx["books"][tok].asks if round(float(px), 4) == p)
                        if sh > shown + F(1, 10 ** 9):
                            counts["depth_used_twice"] += 1
                    accepted = collections.Counter(res[6] for res in results.values() if res[0] is True)
                    for tok in (up, dn):
                        pos = led.positions.get(tok)
                        lots = len(pos.lots) if pos else 0
                        if lots != accepted[tok]:
                            counts["duplicate_fills_applied"] += 1
                        if pos and pos.condition_id != cid:
                            counts["cross_market_corruptions"] += 1
                        if pos and (pos.shares < 0 or pos.cost < 0):
                            counts["negative_shares"] += 1
                # ---- cash, once per broker: every lot ever booked, from scratch
                spent = sum(F(str(l.shares)) * F(str(l.price)) + F(str(l.fee))
                            for p in led.positions.values() for l in p.lots)
                if abs(float(F(str(start_cash)) - spent) - br.cash_balance()) > 1e-6 or br.cash_balance() < 0:
                    counts["accounting_mismatches"] += 1
                lots = sum(len(p.lots) for p in led.positions.values())
                if len(led.seen) != lots:
                    counts["duplicate_fills_applied"] += 1
                for tok, p in led.positions.items():
                    if cond_tokens.get(tok) != p.condition_id:
                        counts["cross_market_corruptions"] += 1
        took = time.perf_counter() - t0
        print(f"\n  A. broker races: {N_RACES} iterations, {orders_total} orders, {took:.0f}s: "
              f"{dict(counts) or 'no violations'}")
        self.assertEqual({k: v for k, v in counts.items() if v}, {})


# =========================================================================== B. the whole system
class Hub:
    """What the Sniper and the manager read: the round and a real BookState."""

    def __init__(self):
        self.book = BookState(stale_after=8.0)
        self.book.connected = True
        self.up_token = self.down_token = self.condition_id = None
        self.window_start = self.window_end = None


class Bot:
    """One paper bot: real components, a harness clock-pump for the exchange-time book."""

    def __init__(self, tmp: pathlib.Path, rng: random.Random, counts: collections.Counter):
        self.tmp, self.rng, self.counts = tmp, rng, counts
        self.hub = Hub()
        self.ledger = Ledger(path=str(tmp / "l.json"))
        self.start_cash = 1_000_000.0
        hub = self.hub
        self.ctx = {"up": None, "down": None}
        self.broker = PaperBroker(
            self.ledger, market_context=lambda: {"condition_id": hub.condition_id, "up_token_id": hub.up_token,
                                                 "down_token_id": hub.down_token},
            host="https://public.invalid", max_buy_price=0.95, min_buy_price=0.05, start_balance=self.start_cash,
            account_path=tmp / "a.json", audit_path=tmp / "o.jsonl",
            book_fetch=lambda tok: snapshot_from_book_view(hub.book.view(str(tok)), expected_token=str(tok)),
            rules_fetch=rules_for(self.ctx), min_seconds_to_expiry=0, trade_window_seconds=300,
            own_fill_ttl_s=2.0)
        self.broker._audit = lambda row: None
        env = {"SNIPER_ONLY": "1", "SNIPER_ORDER_TYPE": "FAK", "SNIPER_SLIP": "0.05", "SNIPER_SHARES": "10",
               "SNIPER_MAX_TARGET_SHARES": "20", "SNIPER_K_BP": "0.5", "SNIPER_WINDOW_MS": "250",
               "SNIPER_COOLDOWN_MS": "300", "SNIPER_LATENCY_MS": str(rng.choice((1, 2, 4))),
               "SNIPER_SHADOW_MS": "", "SNIPER_MIN_LEFT_S": "10", "SNIPER_MAX_FILLS_ROUND": "20",
               "SNIPER_MAX_SIDE_SHARES": "60", "SNIPER_BOOK_WAIT_MS": "50"}
        saved = {k: os.environ.get(k) for k in env}
        os.environ.update(env)
        try:
            self.sn = S.Sniper(hub, self.broker, settings=S.Settings())
        finally:
            for k, v in saved.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)
        self.sn.fresh.why_not = lambda t: None                       # the Binance feed is not under test here
        self.rows = []
        self.sn._write = self.rows.append
        self.pm_cfg = replace(PM.PMConfig(enabled=True), cooldown_ms=0.0, min_left_s=30.0, order_timeout_s=2.0,
                              sniper_quiet_ms=0.0, side_avg_min_gain_per_usd=0.0, side_avg_max_added_loss_usd=5.0)
        self.pm = PM.PositionManager(self.pm_cfg, hub=hub, ledger=self.ledger, broker=self.broker, sniper=self.sn)
        self.results = {}                                            # condition -> winner index
        self.resolver_fail = set()
        self.settler = ST.SettlementWorker(self.ledger, interval=0.02, fetch=self._resolve, paper=True,
                                           poll_active=False, tick_s=0.01)
        self.tokens_of = {}                                          # token -> condition
        self.market_tokens = {}                                      # condition -> (up, down)
        self.n_market = 0
        self.match_calls = collections.Counter()
        self.why = collections.Counter()
        self.pm_sends = collections.Counter()
        self.decisions = []
        self.plans_checked = 0
        self._wrap()
        self.stop = threading.Event()
        self.pump = threading.Thread(target=self._pump, daemon=True)
        self.pump.start()

    # -- instrumentation (wrappers around the real methods; nothing replaced)
    def _wrap(self):
        sn, pm, hub, counts = self.sn, self.pm, self.hub, self.counts
        real_decide, real_match = sn._decide, sn._match

        def decide(side, row, t_ms):
            plan = real_decide(side, row, t_ms)
            if plan is not None:
                self.plans_checked += 1
                plan["_audit_seq"] = self.plans_checked                # id() is reused after GC
                v = hub.book.view(plan["token"])
                age = None if v.updated_mono is None else (time.monotonic() - v.updated_mono) * 1000.0
                if v.status != "LIVE" or age is None or age > sn.s.stale_ms:
                    counts["stale_book_orders"] += 1
                if self.tokens_of.get(plan["token"]) != plan["cond"]:
                    counts["cross_market_corruptions"] += 1
                # the round cap's contract: shares already filled this round on this side
                # + this order's estimate (stake / reference ask) never above the cap
                rd = sn.rounds.get(plan["end"]) or {}
                if rd.get(side, 0) + float(row.get("shares") or 0) > sn.s.max_side_shares + 1e-3:
                    counts["exposure_violations"] += 1
                    self.why["exposure: sniper round cap at decide"] += 1
            return plan

        def match(plan, sign_ms, st):
            key = plan.get("_audit_seq")
            self.match_calls[key] += 1
            if self.match_calls[key] > 1:
                counts["duplicate_sniper_orders"] += 1
            return real_match(plan, sign_ms, st)
        sn._decide, sn._match = decide, match
        real_exec = pm._execute_paper

        def execute(d, cond, up, down, end, o=None):
            key = getattr(o, "reservation_id", None)
            self.pm_sends[key] += 1
            if self.pm_sends[key] > 1:
                counts["duplicate_manager_orders"] += 1
            return real_exec(d, cond, up, down, end, o=o)
        pm._execute_paper = execute
        real_reserve = pm.reserve

        def reserve(d):
            self.decisions.append(d)
            m = d.metrics
            if (m.get("new_pairs") or 0) > PM.EPS and d.classification != "EXPOSURE_IMPROVEMENT":
                limit = 1.0 - self.pm_cfg.min_pair_improvement * (m.get("threshold_multiplier") or 1.0)
                if m.get("incremental_pair_cost") is None or m["incremental_pair_cost"] > limit + 1e-9:
                    counts["pair_threshold_violations"] += 1
            cap = self.pm_cfg.max_up_shares if d.side == "UP" else self.pm_cfg.max_down_shares
            if (m.get("current_shares") or 0) + d.fill.qty > cap + 1e-9:
                counts["exposure_violations"] += 1
                self.why["exposure: pm side cap at reserve"] += 1
            return real_reserve(d)
        pm.reserve = reserve

    def _pump(self):
        """The feed's exchange clock: what a live socket's messages carry."""
        while not self.stop.is_set():
            self.hub.book.note_exchange_ts(int(time.time() * 1000) + 5)
            time.sleep(0.002)

    def _resolve(self, cid):
        if cid in self.resolver_fail:
            raise RuntimeError("rpc unreachable (injected)")
        win = self.results.get(cid)
        if win is None:
            return Resolution(cid, PENDING, {}, detail="not resolved on chain yet")
        up, dn = self.market_tokens[cid]
        return Resolution(cid, RESOLVED, {up: float(win == 0), dn: float(win == 1)}, "Up" if win == 0 else "Down")

    # -- events
    def new_market(self):
        self.n_market += 1
        n = self.n_market
        cid = "0x" + f"{(id(self) % 10 ** 6) * 10 ** 6 + n:064x}"
        up, dn = f"{id(self) % 10 ** 6}{n:06d}1", f"{id(self) % 10 ** 6}{n:06d}2"
        old = self.hub.condition_id
        if old is not None:
            # the old market ends now: the settlement tracks it from here
            self.settler.track(old, int(time.time()) - 301, int(time.time()) - 1,
                               up_token=self.hub.up_token, down_token=self.hub.down_token)
        hub = self.hub
        hub.book.set_active([up, dn])
        end = int(time.time()) + 200
        hub.up_token, hub.down_token, hub.condition_id = up, dn, cid
        hub.window_start, hub.window_end = end - 300, end
        self.ctx.update(up=up, down=dn)
        self.tokens_of[up] = self.tokens_of[dn] = cid
        self.market_tokens[cid] = (up, dn)
        self.settler.track(cid, end - 300, end, up_token=up, down_token=dn)
        for tok in (up, dn):
            self.snapshot(tok)
        self.broker._rules(cid)                       # the round-start prefetch (Sniper._prefetch_rules)

    def snapshot(self, tok, crossed=False):
        rng = self.rng
        ask0 = rng.randrange(20, 80) / 100
        asks = [(round(ask0 + k / 100, 2), round(rng.uniform(0.5, 60), 2)) for k in range(rng.randrange(1, 6))]
        bids = [(round(ask0 - 0.01 - k / 100, 2), round(rng.uniform(1, 60), 2)) for k in range(rng.randrange(1, 3))]
        if crossed:
            bids = [(round(ask0 + 0.02, 2), 5.0)]
        self.hub.book.apply_snapshot(tok, bids, asks, ts_ms=int(time.time() * 1000), require_exchange_ts=True)

    def delta(self, tok):
        rng = self.rng
        v = self.hub.book.view(tok)
        if v.status != "LIVE" or not v.asks:
            return
        if rng.random() < 0.1:
            p = round((v.best_bid or 0.3) + 0.01, 2)                 # may cross: must desync, never stay LIVE
            change = {"asset_id": tok, "price": str(p), "size": "7", "side": "BUY"}
        else:
            p = round(v.best_ask + rng.choice((0, 0, 0.01, 0.02)), 2)
            change = {"asset_id": tok, "price": str(p), "size": str(rng.choice((0, round(rng.uniform(1, 40), 2)))),
                      "side": "SELL"}
        self.hub.book.apply_price_changes([change], ts_ms=int(time.time() * 1000))

    def signal(self, side):
        # Scenarios run in milliseconds; a real signal on a side usually comes more than
        # 300ms after that side's last order. Most signals model that gap; the rest hit
        # the real cooldown (its exact boundary is tested in tests_final_audit).
        if self.rng.random() < 0.7:
            self.sn.last_fire_by_side[side] = -1e18
        d = 1 if side == "UP" else -1
        trig = types.SimpleNamespace(update=lambda t, m: (d, 0.6 * d), q=collections.deque(), ref=None)
        self.sn._signal("binance", trig, 60_000.0, time.monotonic() * 1000.0, time.perf_counter_ns())

    def manager(self, loop, stats=None):
        decisions = self.pm.evaluate()
        if not decisions:
            return
        if stats is not None:
            for d in decisions.values():
                stats[f"pm {d.action} {d.reason or d.classification}"] += 1
        d = self.pm.choose(decisions)
        if d is not None:
            self.pm._dispatch(d, loop)

    def resolve_old(self, duplicate=False):
        pending = [c for c, r in self.settler.records.items() if r.state in (ST.PENDING, ST.SETTLING)]
        if not pending:
            return
        cid = self.rng.choice(pending)
        self.results.setdefault(cid, self.rng.choice((0, 1)))
        if duplicate:
            up, dn = self.market_tokens[cid]
            win = self.results[cid]
            self.ledger.settle(Resolution(cid, RESOLVED, {up: float(win == 0), dn: float(win == 1)}, "x"))

    # -- invariants
    async def quiesce(self, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            # an order's side is released in _finish's finally: after its outcome is journalled
            # (markout tasks run on for seconds and are not orders)
            sn_busy = any(v is not None for v in self.sn.inflight.values())
            pm_busy = any(o.status in ("PENDING", "UNKNOWN") and not o.finished for o in self.pm.orders.values())
            if not sn_busy and not pm_busy:
                return True
            await asyncio.sleep(0.005)
        return False

    def check(self):
        counts, led = self.counts, self.ledger
        with led._lock:
            positions = list(led.positions.values())
            lots = sum(len(p.lots) for p in positions)
            if len(led.seen) != lots:
                counts["duplicate_fills_applied"] += 1
            for p in positions:
                if p.shares < -1e-9 or p.cost < -1e-9:
                    counts["negative_shares"] += 1
                if self.tokens_of.get(p.token_id) != p.condition_id:
                    counts["cross_market_corruptions"] += 1
                if any(l.condition_id != p.condition_id for l in p.lots):
                    counts["cross_market_corruptions"] += 1
            # oracle: cash = start - every cost + every settled payout; realized per settled position
            spent = sum(F(str(l.shares)) * F(str(l.price)) + F(str(l.fee)) for p in positions for l in p.lots)
            paid, realized = F(0), F(0)
            for p in positions:
                if p.settled and p.shares > 0:
                    win = self.results.get(p.condition_id)
                    if win is None:
                        counts["accounting_mismatches"] += 1                 # settled without a result
                        continue
                    up, _dn = self.market_tokens[p.condition_id]
                    pay = F(int((p.token_id == up) == (win == 0)))
                    cost = sum(F(str(l.shares)) * F(str(l.price)) + F(str(l.fee)) for l in p.lots)
                    paid += F(str(p.shares)) * pay
                    realized += F(str(p.shares)) * pay - cost
                    if p.payout_per_share != float(pay):
                        counts["settlement_double_counts"] += 1
            # read under the SAME lock as the oracle: a settlement running on the
            # worker pool must not land between the two reads (it did once in
            # 10,000 scenarios when these two read the ledger after the lock)
            cash = self.broker.cash_balance()
            realized_led = led.summary()["realized_pnl"]
        if abs(float(F(str(self.start_cash)) - spent + paid) - cash) > 1e-6:
            counts["accounting_mismatches"] += 1
            self.why[f"mismatch cash: oracle {float(F(str(self.start_cash)) - spent + paid):.6f} vs {cash:.6f}"] += 1
        if abs(realized_led - float(realized)) > 1e-6:
            counts["accounting_mismatches"] += 1
            self.why[f"mismatch realized: oracle {float(realized):.6f} vs {realized_led:.6f}"] += 1
        for o in self.pm.orders.values():
            if o.requested_qty < 0 or o.reserved_usd < 0 or o.filled_qty < 0:
                counts["negative_pending"] += 1
        for h in self.pm.histories.values():
            if h.spend < -1e-6 or h.orders < 0:
                counts["negative_pending"] += 1
            if h.spend > self.pm_cfg.max_market_usd + 1e-6 or h.orders > self.pm_cfg.max_orders:
                counts["exposure_violations"] += 1
                self.why["exposure: pm market spend/orders"] += 1
        for end, rd in self.sn.rounds.items():
            # Measured, not a violation: a dollar-denominated IOC buys more shares than its
            # estimate when the ask falls before the match, so a round can end above the
            # cap by that one order's price improvement (reported in the audit).
            over = max(rd["UP"], rd["DOWN"]) - self.sn.s.max_side_shares
            if over > 1e-9:
                self.why["measured: round above side cap by price improvement"] += 1
                self.over_max = max(getattr(self, "over_max", 0.0), over)
            if rd["fills"] > self.sn.s.max_fills_round:
                counts["exposure_violations"] += 1
                self.why["exposure: sniper rd fills"] += 1
        for side, c in self.sn.carry.items():
            if not 0 <= c <= self.sn.s.max_target_shares + 1e-9:
                counts["exposure_violations"] += 1
                self.why["exposure: carry range"] += 1
        for r in self.rows:
            if r.get("result") == "filled" and r.get("target_shares") and r["shares"] > r["target_shares"] + 1e-6:
                self.why["measured: fills above target (ask fell before the match)"] += 1
            if r.get("result") == "refused" and "own order on this token" in str(r.get("reason") or ""):
                counts["manager_caused_sniper_misses"] += 1
            if r.get("result") == "error":
                counts["crashes"] += 1
        self.rows.clear()
        if self.sn.n["errors"]:
            counts["crashes"] += self.sn.n["errors"]
            self.sn.n["errors"] = 0
        if self.pm.n["errors"]:
            counts["crashes"] += self.pm.n["errors"]
            self.pm.n["errors"] = 0

    async def close(self):
        # Exercise the real component lifecycle, including manager-owned
        # executors and order waiters, before closing the harness clock.
        try:
            await self.sn.stop()
            await self.pm.stop()
            await self.settler.stop()
        finally:
            self.stop.set()
            self.pump.join(2)


class FullSystem(unittest.TestCase):
    def test_10000_full_system_scenarios(self):
        counts = collections.Counter()
        stats = collections.Counter()
        t0 = time.perf_counter()

        def on_loop_error(loop, context):
            counts["uncaught_exceptions"] += 1
            stats["uncaught: " + str(context.get("message"))[:60]] += 1

        async def main():
            loop = asyncio.get_running_loop()
            loop.set_exception_handler(on_loop_error)
            rng = random.Random(2026)
            per_bot = 500
            done = 0
            while done < N_SCENARIOS:
                with tempfile.TemporaryDirectory() as tmp:
                    bot = Bot(pathlib.Path(tmp), rng, counts)
                    bot.new_market()
                    try:
                        for _ in range(min(per_bot, N_SCENARIOS - done)):
                            await self.scenario(bot, rng, loop, stats)
                            if not await bot.quiesce():
                                counts["deadlocks"] += 1
                            bot.check()
                            done += 1
                    finally:
                        await bot.close()
                        stats["fills"] += sum(len(p.lots) for p in bot.ledger.positions.values())
                        stats["settled_positions"] += sum(1 for p in bot.ledger.positions.values() if p.settled)
                        stats["manager_orders"] += len(bot.pm.orders)
                        stats["markets"] += bot.n_market
                        stats["sniper_decisions_checked"] += bot.plans_checked
                        stats.update(bot.why)
                        stats["measured: max shares above side cap"] = max(
                            stats["measured: max shares above side cap"], round(getattr(bot, "over_max", 0.0), 2))

        with patch("sys.stdout", QUIET), patch("polymarket_trade._get_client", no_live_client):
            asyncio.run(main())
        took = time.perf_counter() - t0
        print(f"\n  B. full system: {N_SCENARIOS} scenarios in {took:.0f}s; " + ", ".join(
            f"{k} {v}" for k, v in sorted(stats.items())))
        print("     " + ", ".join(f"{k}={counts.get(k, 0)}" for k in COUNTS))
        self.assertEqual({k: v for k, v in counts.items() if v}, {})

    async def scenario(self, bot, rng, loop, stats):
        for _ in range(rng.randrange(4, 14)):
            r = rng.random()
            tok = rng.choice((bot.hub.up_token, bot.hub.down_token))
            try:
                if r < 0.20:
                    bot.signal(rng.choice(("UP", "DOWN")))
                    stats["signals"] += 1
                elif r < 0.30:
                    bot.signal("UP"); bot.signal("DOWN")                     # opposite sides at once
                    stats["signals"] += 2
                elif r < 0.45:
                    bot.delta(tok)
                elif r < 0.52:
                    bot.snapshot(tok, crossed=rng.random() < 0.1)
                elif r < 0.55:
                    bot.hub.book.desync_all("disconnect (injected)")         # then a resync snapshot later
                elif r < 0.68:
                    bot.manager(loop, stats)
                    stats["manager_evaluations"] += 1
                elif r < 0.72:
                    bot.new_market()                                         # orders may still be in flight
                elif r < 0.78:
                    bot.resolve_old(duplicate=rng.random() < 0.3)
                elif r < 0.80:
                    pending = list(bot.settler.records)
                    if pending:
                        c = rng.choice(pending)
                        (bot.resolver_fail.add if rng.random() < 0.5 else bot.resolver_fail.discard)(c)
                elif r < 0.88:
                    await bot.settler.tick()
                else:
                    await asyncio.sleep(rng.choice((0, 0.001, 0.003, 0.006)))
            except Exception as exc:                                         # noqa: BLE001 - counted
                self.fail_count = getattr(self, "fail_count", 0) + 1
                stats[f"harness exception {type(exc).__name__}: {exc}"[:90]] += 1
                bot.counts["crashes"] += 1
        await bot.settler.tick()


# =========================================================================== C. resources
class Resources(unittest.TestCase):
    def test_long_run_resources_stay_bounded(self):
        import psutil
        proc = psutil.Process()
        rotations = max(20, int(600 * SCALE))

        async def main():
            rng = random.Random(9)
            counts = collections.Counter()
            with tempfile.TemporaryDirectory() as tmp:
                bot = Bot(pathlib.Path(tmp), rng, counts)
                try:
                    bot.new_market()
                    loop = asyncio.get_running_loop()

                    def executor_pools():
                        pools = {}
                        if loop._default_executor is not None:
                            pools["sniper"] = loop._default_executor
                        if bot.pm._executor is not None:
                            pools["manager"] = bot.pm._executor
                        pools.update({f"settle-{name}": pool for name, pool in bot.settler._pools.items()})
                        return pools

                    async def warm_pool(pool):
                        # ThreadPoolExecutor starts workers lazily. Twenty rotations
                        # need not use every slot, so comparing cold thread counts
                        # confuses later bounded allocation with a resource leak.
                        # Hold one job per configured slot until every worker exists;
                        # all trading/settlement methods remain the real methods.
                        ready = asyncio.Event()
                        release = threading.Event()
                        lock = threading.Lock()
                        started = 0

                        def occupy():
                            nonlocal started
                            with lock:
                                started += 1
                                if started == pool._max_workers:
                                    loop.call_soon_threadsafe(ready.set)
                            release.wait()

                        futures = []
                        try:
                            for _ in range(pool._max_workers):
                                futures.append(pool.submit(occupy))
                            await asyncio.wait_for(ready.wait(), timeout=10.0)
                            self.assertEqual(len(pool._threads), pool._max_workers)
                        finally:
                            release.set()
                            await asyncio.gather(*(asyncio.wrap_future(f) for f in futures))

                    async def rotate(n):
                        for _ in range(n):
                            for _ in range(6):
                                bot.signal(rng.choice(("UP", "DOWN")))
                                bot.manager(loop)
                                bot.delta(rng.choice((bot.hub.up_token, bot.hub.down_token)))
                                await asyncio.sleep(0.002)
                            await bot.quiesce()
                            bot.resolve_old()
                            await bot.settler.tick()
                            bot.new_market()
                        await bot.quiesce()
                    await rotate(20)                                             # warm up
                    for pool in executor_pools().values():
                        await warm_pool(pool)
                    await asyncio.sleep(5.5)          # markout tasks live 5s by design: not a leak, let them end
                    gc.collect()
                    before_pools = executor_pools()
                    before_threads = set(threading.enumerate())
                    tracemalloc.start()
                    m0 = tracemalloc.take_snapshot()
                    before = {"threads": threading.active_count(), "tasks": len(asyncio.all_tasks()),
                              "handles": proc.num_handles() if hasattr(proc, "num_handles") else proc.num_fds(),
                              "settle_records": len(bot.settler.records), "pm_histories": len(bot.pm.histories),
                              "book_tokens": len(bot.hub.book._meta), "book_history": len(bot.hub.book._hist),
                              "exec_locks": len(bot.broker._execution_locks), "own_fills": len(bot.broker._own_fills),
                              "rules_cache": len(bot.broker._rules_cache), "sniper_rounds": len(bot.sn.rounds),
                              "pm_orders": len(bot.pm.orders)}
                    t_rot = time.monotonic()
                    await rotate(rotations)
                    self.elapsed = time.monotonic() - t_rot
                    await asyncio.sleep(5.5)
                    gc.collect()
                    m1 = tracemalloc.take_snapshot()
                    grown = sum(s.size_diff for s in m1.compare_to(m0, "filename"))
                    tracemalloc.stop()
                    after = {"threads": threading.active_count(), "tasks": len(asyncio.all_tasks()),
                             "handles": proc.num_handles() if hasattr(proc, "num_handles") else proc.num_fds(),
                             "settle_records": len(bot.settler.records), "pm_histories": len(bot.pm.histories),
                             "book_tokens": len(bot.hub.book._meta), "book_history": len(bot.hub.book._hist),
                             "exec_locks": len(bot.broker._execution_locks), "own_fills": len(bot.broker._own_fills),
                             "rules_cache": len(bot.broker._rules_cache), "sniper_rounds": len(bot.sn.rounds),
                             "pm_orders": len(bot.pm.orders)}
                    after_pools = executor_pools()
                    after_threads = set(threading.enumerate())
                    owned_threads = {bot.pump} | {thread for pool in after_pools.values() for thread in pool._threads}
                    # A manager executor may only be needed after the measured
                    # rotations start; every such new pool still has its explicit
                    # configured bound. Existing warmed pools must be reused.
                    expected_limits = {"manager": 2, "settle-net": bot.settler.workers, "settle-io": 2}
                    new_capacity = 0
                    for name, pool in after_pools.items():
                        self.assertLessEqual(len(pool._threads), pool._max_workers, name)
                        if name in before_pools:
                            self.assertIs(pool, before_pools[name], name)
                        else:
                            self.assertIn(name, expected_limits)
                            self.assertEqual(pool._max_workers, expected_limits[name], name)
                            new_capacity += pool._max_workers
                    self.assertEqual(set(before_pools) - set(after_pools), set())
                    self.assertFalse(after_threads - before_threads - owned_threads,
                                     "rotation spawned threads outside the bot's bounded executors")
                    self.assertLessEqual(after["threads"], before["threads"] + new_capacity)
                    return before, after, grown, counts, owned_threads
                finally:
                    if tracemalloc.is_tracing():
                        tracemalloc.stop()
                    await bot.close()

        with patch("sys.stdout", QUIET):
            before, after, grown, counts, owned_threads = asyncio.run(main())
        # asyncio.run has also shut down the sniper's default executor. The
        # settlement shutdown is non-blocking, so give any final already-running
        # local worker a bounded opportunity to exit and verify ownership ends.
        deadline = time.monotonic() + 2.0
        for thread in owned_threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        self.assertFalse([thread.name for thread in owned_threads if thread.is_alive()],
                         "bot-owned workers survived shutdown")
        print(f"\n  C. resources over {rotations} market rotations: memory {grown / 1024:+.0f} KiB")
        for k in before:
            print(f"     {k:16s} {before[k]:>7} -> {after[k]:>7}")
        self.assertEqual({k: v for k, v in counts.items() if v}, {})
        self.assertLessEqual(after["tasks"], before["tasks"] + 5)
        self.assertLessEqual(after["handles"], before["handles"] + 20)
        for bounded in ("settle_records", "pm_histories", "book_tokens", "rules_cache", "own_fills", "exec_locks",
                        "sniper_rounds"):
            self.assertLessEqual(after[bounded], max(before[bounded], 70), bounded)
        self.assertLessEqual(after["pm_orders"], PM.PositionManager.KEEP_FINISHED + 10)
        # book history of old tokens is kept history_s (30s) then dropped at a rotation: bounded by
        # time, not by count - this test rotates every ~0.1s, a live bot every 300s
        # (+ slack: the window's edges and the 5.5s waits around the measurement)
        self.assertLessEqual(after["book_history"], 20 + 2 * 36 * rotations / max(1.0, self.elapsed))
        self.assertLess(grown, 40 * 1024 * 1024)
        self.results = (before, after, grown)


if __name__ == "__main__":
    unittest.main(verbosity=1)
