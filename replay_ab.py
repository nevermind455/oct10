#!/usr/bin/env python3
"""A/B replay: the sniper alone (A) against the sniper + Position Manager (B),
on identical recorded markets. Paper only - no orders, no wallet.

    python replay_ab.py                          # every recording in LAB_DATA / --data
    python replay_ab.py --data DIR --data DIR2 --json out.json
    python replay_ab.py --feed binance           # replay the Binance trigger instead of Coinbase

What runs: the REAL sniper_mode.Sniper (its trigger, gates, sizing, carry,
cooldown - the production settings in exec_latency.FROZEN_SPEC) and, in B,
the REAL position_manager.PositionManager, both on a virtual clock driven by
recorded messages (latency_lab recorder: the trigger feed - Coinbase BTC-USD
ticker by default, the frozen spec's; --feed binance replays Binance spot
bookTicker - and Polymarket books). Every round in the recordings is used -
none picked or dropped - and is scored on its authoritative venue result
(accounting.resolution, cached). During trading, settlement defaults to 60s
after expiry and redemption to another 600s; payouts cannot be reused before
then. These are configurable assumptions, not measured venue timings.

How an order fills: it reaches the venue latency_ms after the decision (the
modelled taker hold + network) and is matched against the recorded book at
that instant, up to its limit, minus what our own orders took from that book
in the last 2s (sniper and manager compete for the same depth). The sniper's
orders are FAK (whatever is there); the manager's are FOK (all of it, or
nothing).
Fees: the venue curve, shares * 0.07 * p(1-p) (the rate on every recorded
paper fill). Decisions only see the past; fills see the book at arrival.

Limits of the method (stated, not hidden): the recorded book holds the top 5
levels within 5c of the best; our orders do not move the recorded market; a
paper fill is not a live fill. The recorder writes a Coinbase ticker only when
its best bid or ask changed, so a run of matches at an unchanged quote is one
recorded tick: a quiet spell can exceed the rule's max gap in the replay where
live heard a ticker inside it (the replay's rule restarts a little more often).
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import glob
import json
import math
import os
import pathlib
import sys
import tempfile
import threading
import time
import types
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import exec_latency  # noqa: E402
import position_manager as PMM  # noqa: E402
import sniper_mode as S  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from feeds.book import BookState  # noqa: E402
from latency_lab import feeds as XF  # noqa: E402
from latency_lab.load import Book, _lines  # noqa: E402
from paper_trade import MarketRules  # noqa: E402

FEE_RATE, FEE_EXP = 0.07, 1
OWN_FILL_TTL_S = 2.0
SETTLEMENT_DELAY_S = 60.0
REDEMPTION_DELAY_S = 600.0


# =========================================================================== recordings
def recording_files(dirs) -> list[str]:
    out = []
    for d in dirs:
        out += glob.glob(os.path.join(d, "lab_*.jsonl.gz"))
    return sorted(set(out), key=lambda p: os.path.basename(p))


REPLAY_FEEDS = ("coinbase", "binance")
COINBASE_CODE = XF.CODE["coinbase"]


def events(paths, feed: str = "coinbase", diagnostics=None):
    """("round", wall, info) / ("aid", wall, asset, idx, side) / ("btc", wall, bid, ask) /
    ("pm", wall, {asset: (bids, asks)}) in recording order. Books rebuilt per
    asset from snapshots and level changes (latency_lab.load.Book). Coinbase
    "btc" events also carry sequence/exchange-time metadata in slot 4.
    "btc" is
    the trigger feed's best bid/ask: Coinbase's ticker ("xb" rows of its code)
    or Binance's bookTicker ("bsb" rows) - only that one feed, so a replay on
    one feed is not timed by the other's messages."""
    if feed not in REPLAY_FEEDS:
        raise ValueError(f"replay feed {feed!r}: {' or '.join(REPLAY_FEEDS)}")
    books = collections.defaultdict(Book)
    idx_asset: dict = {}
    sequence = 0
    diagnostics = diagnostics if diagnostics is not None else collections.Counter()
    for path in paths:
        for line in _lines(path):
            try:
                r = json.loads(line)
                if not isinstance(r, list) or len(r) < 3:
                    raise ValueError("not a recording row")
                k = r[0]
                wall = float(r[2]) / 1e9
                if not math.isfinite(wall):
                    raise ValueError("nonfinite receive time")
            except (ValueError, TypeError):
                diagnostics["malformed_rows"] += 1
                continue
            if k == "bsb" or k == "xb":
                try:
                    selected = ((k == "bsb" and feed == "binance") or
                                (k == "xb" and feed == "coinbase" and r[3] == COINBASE_CODE))
                    if not selected:
                        continue
                    bid, ask = float(r[4 if k == "bsb" else 5]), float(r[6])
                    if not (math.isfinite(bid) and math.isfinite(ask) and 0 < bid <= ask):
                        raise ValueError("invalid best bid/ask")
                    if k == "bsb":
                        yield ("btc", wall, bid, ask)
                        continue
                    # Legacy compact xb rows discarded Coinbase's sequence. Give
                    # them increasing, run-local identities; newer rows may retain
                    # it in an optional trailing integer. Preserve duplicates so
                    # the production multi-socket deduplicator still handles them.
                    recorded_seq = r[7] if len(r) > 7 else None
                    has_seq = isinstance(recorded_seq, int) and not isinstance(recorded_seq, bool)
                    if len(r) > 7 and not has_seq:
                        raise ValueError("invalid recorded sequence")
                    seq = recorded_seq if has_seq else sequence + 1
                    sequence = max(sequence, seq)
                    ex = float(r[4]) if r[4] is not None else wall * 1000.0
                    if not math.isfinite(ex):
                        raise ValueError("nonfinite exchange time")
                    yield ("btc", wall, bid, ask, {"sequence": seq, "exchange_ms": ex,
                           "sequence_source": "recorded" if has_seq else "synthetic",
                           "exchange_time_source": "recorded" if r[4] is not None else "receive_time_model"})
                except (IndexError, ValueError, TypeError):
                    diagnostics["malformed_trigger_rows"] += 1
                    continue
            elif k == "round":
                yield ("round", wall, r[3])
            elif k == "aid":
                idx_asset[r[4]] = r[3]
                yield ("aid", wall, r[3], r[4], r[5])
            elif k == "pmb":
                b = books[r[4]]
                b.snapshot(r[5], r[6])
                a = idx_asset.get(r[4])
                if a is not None:
                    asks, bids = b.ladder()
                    yield ("pm", wall, {a: (bids, asks)})
            elif k == "pmc":
                touched = {}
                for idx, p, s, sd, bb, ba in r[4]:
                    books[idx].change(p, s, sd, bb, ba)
                    touched[idx] = True
                upd = {}
                for idx in touched:
                    a = idx_asset.get(idx)
                    if a is not None:
                        asks, bids = books[idx].ladder()
                        upd[a] = (bids, asks)
                if upd:
                    yield ("pm", wall, upd)


def scan_rounds(paths, feed: str = "coinbase") -> dict:
    """window_start -> round info, and how many book updates each round has.
    Also fixes the replay's end ("__until__"): a recording still being written
    grows between the two runs, and A and B must see identical events.
    "__btc_ticks__": how many trigger-feed ticks the recordings hold."""
    rounds, counts = {}, collections.Counter()
    asset_round = {}
    last = 0.0
    ticks = 0
    diagnostics = collections.Counter()
    for ev in events(paths, feed, diagnostics):
        last = max(last, ev[1])
        if ev[0] == "btc":
            ticks += 1
        if ev[0] == "round":
            info = ev[2]
            rounds[int(info["window_start"])] = info
            asset_round[str(info["up_token_id"])] = int(info["window_start"])
            asset_round[str(info["down_token_id"])] = int(info["window_start"])
        elif ev[0] == "pm":
            for a in ev[2]:
                w = asset_round.get(str(a))
                if w is not None:
                    counts[w] += 1
    out = {w: dict(info, book_updates=counts[w]) for w, info in rounds.items()}
    out["__until__"] = last
    out["__btc_ticks__"] = ticks
    out["__input_diagnostics__"] = dict(diagnostics)
    return out


def outcomes(rounds: dict, cache_path: pathlib.Path) -> dict:
    """condition id -> {"payouts": {token: 0/1}, "winner": ...} from the venue
    (chain first, the API mirrors after; accounting.resolution.Resolver)."""
    cache = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except ValueError:
            cache = {}
    from accounting import resolution
    os.environ.setdefault("SETTLE_ONCHAIN", "1")
    resolver = resolution.Resolver(api_every_s=0.0)
    for info in rounds.values():
        cid = info["condition_id"]
        if cid in cache:
            continue
        r = resolver(cid)
        if r.resolved:
            cache[cid] = {"payouts": dict(r.payouts), "winner": r.winning_label, "source": r.detail}
    cache_path.write_text(json.dumps(cache, indent=1), encoding="utf-8")
    return cache


# =========================================================================== the virtual clock
class Clock:
    def __init__(self):
        self.now = 0.0

    def __getattr__(self, name):
        return getattr(time, name)

    def time(self):
        return self.now

    def monotonic(self):
        return self.now


# =========================================================================== the venue
class ReplayBroker:
    """FAK fills against the recorded book at each order's arrival. The
    sniper calls place_trade from its executor thread (it blocks until the
    replay clock reaches the arrival); the manager submits from the driver."""
    supports_size_at_limit = False
    supports_arrival_book = False
    supports_share_sized = True       # F1: a share-sized FAK buys at most its shares
    own_fill_ttl_s = 0

    def __init__(self, ledger: Ledger, clock: Clock, book, *, start_cash: float, latency_ms: float,
                 window_s: float = 300.0, min_left_s: float = 10.0, min_price: float = 0.05,
                 max_price: float = 0.95, redemption_delay_s: float = REDEMPTION_DELAY_S) -> None:
        self.ledger, self.clock, self.book = ledger, clock, book
        self.start_balance = start_cash
        self.latency_s = latency_ms / 1000.0
        self.trade_window_seconds, self.min_seconds_to_expiry = window_s, min_left_s
        self.min_buy_price, self.max_buy_price = min_price, max_price
        self._rules_cache: dict = {}
        self.redemption_delay_s = redemption_delay_s
        self.redemption_at: dict[str, float] = {}
        self.pending: list = []
        self.received = 0
        self.lock = threading.Lock()
        self._local = threading.local()
        self.own: dict = collections.defaultdict(list)      # token -> [(time, price, shares)]
        self.n = 0
        self.orders: list = []                              # every resolved order, both sources

    @property
    def last_fill(self):
        return getattr(self._local, "fill", None)

    @property
    def last_error(self):
        return getattr(self._local, "error", None)

    def rules(self, cond):
        hit = self._rules_cache.get(cond)
        if hit is None:
            hit = self._rules_cache[cond] = (MarketRules(cond, Decimal(str(FEE_RATE)), FEE_EXP, Decimal("5"),
                                                         Decimal("0.01")), self.clock.now)
        return hit[0]

    def _enqueue(self, side, amount, up, down, cond, end, cap, tag, ref_ask, callback=None, fok=False,
                 shares=None):
        ev, slot = threading.Event(), {}
        token = up if side == "UP" else down
        with self.lock:
            self.received += 1
            self.pending.append({"arrival": self.clock.now + self.latency_s, "side": side, "amount": float(amount),
                                 "token": str(token), "cond": str(cond), "end": float(end), "cap": float(cap),
                                 "tag": tag, "ref_ask": ref_ask, "decided": self.clock.now, "ev": ev, "slot": slot,
                                 "callback": callback, "fok": bool(fok),
                                 "shares": None if shares is None else float(shares)})
        return ev, slot

    def place_trade(self, side, amount, up, down, cond, end, max_price, min_price=None, *, latency_ms=None,
                    tag=None, fak=False, shares=None, **_kw) -> bool:
        view = self.book.view(str(up if side == "UP" else down))
        ev, slot = self._enqueue(side, amount, up, down, cond, end, max_price, tag or "sniper", view.best_ask,
                                 fok=not fak, shares=shares)
        ev.wait(120)
        self._local.fill, self._local.error = slot.get("fill"), slot.get("error")
        return bool(slot.get("ok"))

    def submit(self, side, amount, up, down, cond, end, cap, *, tag, callback, fok=True):
        view = self.book.view(str(up if side == "UP" else down))
        self._enqueue(side, amount, up, down, cond, end, cap, tag, view.best_ask, callback, fok=fok)

    def cash_balance(self) -> float:
        with self.ledger._lock:
            spent = sum(p.cost for p in self.ledger.positions.values())
            payouts = sum(p.shares * float(p.payout_per_share or 0) for p in self.ledger.positions.values()
                          if p.settled and self.redemption_at.get(p.condition_id, math.inf) <= self.clock.now)
        return self.start_balance - spent + payouts

    def mark_settled(self, condition_id: str, settled_at: float) -> None:
        self.redemption_at[str(condition_id)] = settled_at + self.redemption_delay_s

    def _asks_now(self, token):
        view = self.book.view(token)
        now = self.clock.now
        used = collections.Counter()
        keep = []
        for t, px, sh in self.own[token]:
            if now - t < OWN_FILL_TTL_S:
                used[px] += sh
                keep.append((t, px, sh))
        self.own[token] = keep
        out = []
        for p, q in (view.asks or ()):
            left = float(q) - used.get(round(float(p), 4), 0.0)
            if left > 1e-9:
                out.append((float(p), left))
        return out, view

    def resolve_due(self) -> list:
        """Fill every order whose arrival time has come, against the book now."""
        now = self.clock.now
        with self.lock:
            due = [o for o in self.pending if o["arrival"] <= now]
            self.pending = [o for o in self.pending if o["arrival"] > now]
        for o in sorted(due, key=lambda o: o["arrival"]):
            # No recorded event between arrival and now changed the book. Match
            # at arrival, including the cutoff and own-depth TTL, rather than
            # rejecting a valid order because the next message arrived late.
            self.clock.now = o["arrival"]
            self._fill(o)
        self.clock.now = now
        return due

    def _fill(self, o) -> None:
        slot, tok = o["slot"], o["token"]
        asks, view = self._asks_now(tok)
        err = None
        if self.clock.now >= o["end"] - self.min_seconds_to_expiry:
            err = "paper quote reached the round cutoff"
        elif str(getattr(view, "status", "")) != "LIVE":
            err = "unknown book state at arrival"
        if err is None:
            cap = math.floor(o["cap"] * 100 + 1e-9) / 100
            spend, shares, notional, fee, used = o["amount"], 0.0, 0.0, 0.0, []
            want = o.get("shares")            # F1: a share-sized order stops at its shares
            for p, q in asks:
                if p > cap + 1e-9 or spend <= 1e-9 or (want is not None and shares >= want - 1e-9):
                    break
                take = min(q, spend / p) if want is None else min(q, want - shares)
                shares += take
                notional += take * p
                fee += take * FEE_RATE * (p * (1 - p)) ** FEE_EXP
                spend -= take * p
                used.append((p, take))
            if shares <= 1e-9:
                err = f"FAK no-fill: nothing at or below {cap}"
            elif o["fok"] and spend > 1e-6:
                # FOK: all of it at or under the cap, or nothing
                err = f"FOK no-fill: only ${o['amount'] - spend:.6f} available at or below {cap}"
            elif notional + fee > self.cash_balance() + 1e-9:
                err = "insufficient available collateral including fees"
        if err is not None:
            slot.update(ok=False, error=err, fill=None)
        else:
            self.n += 1
            oid = f"{o['tag']}-{self.n}"
            vwap = notional / shares
            self.ledger.record_fill(oid, tok, shares=shares, price=vwap, condition_id=o["cond"], fee=fee,
                                    source=f"replay-{o['tag']}")
            for p, take in used:
                self.own[tok].append((self.clock.now, round(p, 4), take))
            fill = {"tag": o["tag"], "order_id": oid, "shares": shares, "average_price": vwap, "fee": fee,
                    "total_cost": notional + fee,
                    "partial": (notional < o["amount"] - 0.01) if o.get("shares") is None
                    else shares < o["shares"] - 1e-9}
            slot.update(ok=True, error=None, fill=fill)
        self.orders.append({"tag": o["tag"], "side": o["side"], "cond": o["cond"], "decided": o["decided"],
                            "ref_ask": o["ref_ask"], "cap": o["cap"], "amount": o["amount"], "shares": o.get("shares"),
                            "fill": slot.get("fill"), "error": slot.get("error")})
        if o["callback"] is not None:
            o["callback"](slot.get("ok", False), slot.get("fill"), slot.get("error"))
        o["ev"].set()


# =========================================================================== one variant
SNIPER_ENV = {k: v for k, v in (
    ("SNIPER_ONLY", "1"), ("SNIPER_SHADOW_MS", ""), ("SNIPER_STALL_MS", "0"), ("SNIPER_MIN_LEFT_S", "10"),
    ("SNIPER_MAX_FILLS_ROUND", "20"), ("SNIPER_MAX_SIDE_SHARES", "200"), ("SNIPER_BINANCE_FRESHNESS", "0"),
    ("SNIPER_COINBASE_CONNS", "2"))}
for _attr, _var, _val, _l, _f in exec_latency.FROZEN_SPEC:
    SNIPER_ENV[_var] = str(_val)
SPEC_FEED = SNIPER_ENV["SNIPER_PRIMARY_FEED"]
# no extra trigger in a replay (a deployment's .env may name one): the recordings
# replay one feed
SNIPER_ENV["SNIPER_EXTRA_FEEDS"] = ""


def btc_message(feed: str, bid: float, ask: float, *, sequence: int = 1, exchange_ms: float = 0.0) -> str:
    """One recorded best bid/ask as the trigger feed's own message."""
    if feed == "coinbase":
        return json.dumps({"type": "ticker", "product_id": S.COINBASE_PRODUCT, "best_bid": repr(bid),
                           "best_ask": repr(ask), "sequence": sequence,
                           "time": datetime.fromtimestamp(exchange_ms / 1000.0, timezone.utc).isoformat()})
    if feed != "binance":
        raise ValueError(f"unsupported replay feed {feed!r}")
    return json.dumps({"b": f"{bid:.2f}", "a": f"{ask:.2f}"})


async def run_variant(paths, rounds, results, *, with_pm: bool, pm_cfg: PMM.PMConfig, latency_ms: float,
                      start_cash: float, label: str, feed: str = SPEC_FEED,
                      settlement_delay_s: float = SETTLEMENT_DELAY_S,
                      redemption_delay_s: float = REDEMPTION_DELAY_S) -> dict:
    for name, value in (("settlement_delay_s", settlement_delay_s), ("redemption_delay_s", redemption_delay_s)):
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be finite and nonnegative")
    until = rounds.get("__until__", math.inf)
    rounds = {w: i for w, i in rounds.items() if not str(w).startswith("__")}
    clock = Clock()
    tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"replay_{label}_"))
    ledger = Ledger(path=str(tmp / "ledger.json"))
    book = BookState(stale_after=8.0, history_s=5.0)
    book.connected = True
    hub = types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_start=None,
                                window_end=None, book=book)
    broker = ReplayBroker(ledger, clock, book, start_cash=start_cash, latency_ms=latency_ms,
                          redemption_delay_s=redemption_delay_s)
    env = dict(SNIPER_ENV, SNIPER_LATENCY_MS=str(latency_ms), SNIPER_PRIMARY_FEED=feed)
    metrics = {"label": label, "max_open_cost": 0.0, "max_worst_loss": 0.0, "max_one_side": 0.0}
    input_diagnostics = collections.Counter()
    trigger_status = collections.Counter()
    asset_side = {}
    latest = {}                                            # asset -> (bids, asks) latest recorded ladder
    settled_rounds, pm_last = set(), -1e9
    equity_curve = [0.0]
    with patch.dict(os.environ, env), patch.object(S, "time", clock), patch("feeds.book.time", clock):
        sn = S.Sniper(hub, broker, journal_path=tmp / "sniper.jsonl", settings=S.Settings(), trade_rows=[],
                      own_rounds=True)
        sn.fresh.enabled = sn.cb_fresh.enabled = False
        on_btc = sn.on_coinbase if feed == "coinbase" else sn.on_tick
        pm = None
        if with_pm:
            pm = PMM.PositionManager(replace(pm_cfg, enabled=True), hub=hub, ledger=ledger, broker=broker, sniper=sn,
                                     journal_path=tmp / "pm.jsonl", wall=clock.time, mono=clock.monotonic)

        def exposure_sample():
            with ledger._lock:
                by = collections.defaultdict(lambda: [0.0, 0.0, 0.0])         # cond -> [cost, up, down]
                for p in ledger.positions.values():
                    if p.settled or p.shares <= 0:
                        continue
                    rec = by[p.condition_id]
                    rec[0] += p.cost
                    rec[1 if asset_side.get(str(p.token_id)) == "UP" else 2] += p.shares
            cost = sum(v[0] for v in by.values())
            worst = sum(max(0.0, v[0] - min(v[1], v[2])) for v in by.values())
            # cost tied up in UNPAIRED shares (the part a single outcome decides), at each side's average
            one = sum((v[0] / (v[1] + v[2])) * abs(v[1] - v[2]) for v in by.values() if v[1] + v[2] > 0)
            metrics["max_open_cost"] = max(metrics["max_open_cost"], cost)
            metrics["max_worst_loss"] = max(metrics["max_worst_loss"], worst)
            metrics["max_one_side"] = max(metrics["max_one_side"], one)

        async def drain(expect_orders=None, sides=()):
            # Deterministic: the virtual clock does not move until the sniper's worker
            # has reached the broker, or the filled sides' _finish has released them.
            # (A bounded number of loop turns was not enough under thread scheduling,
            # and left a side "in flight" for the next tick in one run but not the other.)
            deadline = time.perf_counter() + 10.0
            while time.perf_counter() < deadline:
                if expect_orders is not None and broker.received >= expect_orders:
                    return
                if expect_orders is None and all(sn.inflight.get(s) is None for s in sides):
                    return
                await asyncio.sleep(0.0002)
            raise RuntimeError(f"replay stalled waiting for the sniper (orders {broker.received}/{expect_orders}, "
                               f"sides {sides}, inflight {sn.inflight})")

        def rotate(now):
            w = int(now // 300 * 300)
            if hub.window_start == w:
                return
            info = rounds.get(w)
            if info is None:
                hub.up_token = hub.down_token = hub.condition_id = None
                hub.window_start, hub.window_end = w, w + 300
                return
            up, down = str(info["up_token_id"]), str(info["down_token_id"])
            hub.up_token, hub.down_token, hub.condition_id = up, down, info["condition_id"]
            hub.window_start, hub.window_end = w, w + 300
            asset_side[up], asset_side[down] = "UP", "DOWN"
            book.set_active([up, down])
            broker.rules(info["condition_id"])
            for a in (up, down):
                if a in latest:
                    bids, asks = latest[a]
                    book.apply_snapshot(a, bids, asks, ts_ms=int(now * 1000), require_exchange_ts=False)

        def settle_due(now):
            for w, info in rounds.items():
                cid = info["condition_id"]
                settled_at = w + 300 + settlement_delay_s
                if settled_at <= now and w not in settled_rounds and cid in results:
                    from accounting.resolution import RESOLVED, Resolution
                    r = results[cid]
                    done = ledger.settle(Resolution(cid, RESOLVED, {k: float(v) for k, v in r["payouts"].items()},
                                                    winning_label=r.get("winner")))
                    settled_rounds.add(w)
                    broker.mark_settled(cid, settled_at)
                    if done:
                        equity_curve.append(sum(float(p.realized or 0) for p in ledger.positions.values()
                                                if p.settled))

        for ev in events(paths, feed, input_diagnostics):
            kind, now = ev[0], ev[1]
            if now > until:
                break                                       # identical input for A and B
            clock.now = now
            if broker.pending:
                due = broker.resolve_due()
                if due:
                    await drain(sides={o["side"] for o in due if o["tag"] == "sniper"})
                    exposure_sample()
            rotate(now)
            settle_due(now)
            if kind == "pm":
                for a, ladder in ev[2].items():
                    latest[a] = ladder
                    if a in (hub.up_token, hub.down_token):
                        book.apply_snapshot(a, ladder[0], ladder[1], ts_ms=int(now * 1000), require_exchange_ts=False)
                if pm is not None and now - pm_last >= pm.cfg.poll_ms / 1000.0 and hub.condition_id:
                    pm_last = now
                    decisions = pm.evaluate()
                    if decisions:
                        for d in decisions.values():
                            pm._log_skip(d)
                        d = pm.choose(decisions)
                        if d is not None and not pm._sniper_busy():
                            o = pm.reserve(d)
                            broker.submit(d.side, d.stake, hub.up_token, hub.down_token, hub.condition_id,
                                          hub.window_end, d.limit, tag="manager",
                                          callback=lambda ok, fill, err, o=o: pm.settle_order(o, ok=ok, fill=fill,
                                                                                            error=err))
            elif kind == "btc":
                before, received = sn.n["orders"], broker.received      # both counted BEFORE the tick
                metadata = ev[4] if len(ev) > 4 else {}
                for key in ("sequence_source", "exchange_time_source"):
                    if key in metadata:
                        input_diagnostics[f"{key}:{metadata[key]}"] += 1
                trigger_status[on_btc(btc_message(feed, ev[2], ev[3],
                                sequence=metadata.get("sequence", 1),
                                exchange_ms=metadata.get("exchange_ms", now * 1000.0))) or "tick"] += 1
                if sn.n["orders"] > before:
                    await drain(expect_orders=received + (sn.n["orders"] - before))
        # Finish orders already dispatched, without granting future payouts.
        if broker.pending:
            clock.now = max(clock.now, max(o["arrival"] for o in broker.pending))
        due = broker.resolve_due()
        await drain(sides={o["side"] for o in due if o["tag"] == "sniper"})
        settle_due(clock.now)
        for task in list(sn._tasks):
            task.cancel()
        await asyncio.gather(*sn._tasks, return_exceptions=True)
        if pm is not None:
            pm.journal.close()
    out = collect(label, ledger, broker, sn, pm, metrics, equity_curve, asset_side, rounds, results, tmp)
    out["input_diagnostics"] = dict(input_diagnostics)
    out["trigger_status"] = dict(trigger_status)
    out["sniper_counters"] = dict(sn.n)
    out["settlement_delay_s"] = settlement_delay_s
    out["redemption_delay_s"] = redemption_delay_s
    out["replay_end_wall"] = clock.now
    out["freshness_enabled"] = False
    out["warnings"] = replay_warnings(out)
    return out


def collect(label, ledger, broker, sn, pm, m, curve, asset_side, rounds, results, tmp) -> dict:
    lots = [(p, lot) for p in ledger.positions.values() for lot in p.lots]
    by_src = collections.defaultdict(lambda: {"cost": 0.0, "fees": 0.0, "pnl": 0.0, "shares": 0.0, "orders": set()})
    side_tot = {"UP": [0.0, 0.0], "DOWN": [0.0, 0.0]}
    for p, lot in lots:
        src = "manager" if lot.trade_id.startswith("manager-") else "sniper"
        rec = by_src[src]
        rec["cost"] += lot.cost
        rec["fees"] += lot.fee
        rec["shares"] += lot.shares
        rec["orders"].add(lot.trade_id)
        payout = (p.payout_per_share if p.settled else
                  results.get(p.condition_id, {}).get("payouts", {}).get(str(p.token_id)))
        if payout is not None:
            rec["pnl"] += lot.shares * float(payout) - lot.cost
        st = side_tot[asset_side.get(str(p.token_id), "UP")]
        st[0] += lot.cost
        st[1] += lot.shares
    per_market = collections.defaultdict(lambda: {"UP": [0.0, 0.0], "DOWN": [0.0, 0.0]})
    for p in ledger.positions.values():
        s = asset_side.get(str(p.token_id), "UP")
        rec = per_market[p.condition_id][s]
        rec[0] += sum(l.cost for l in p.lots)
        rec[1] += sum(l.shares for l in p.lots)
    paired_cost_num = paired_n = unpaired = 0.0
    for v in per_market.values():
        up, dn = v["UP"], v["DOWN"]
        pairs = min(up[1], dn[1])
        unpaired += abs(up[1] - dn[1])
        if pairs > 0:
            paired_cost_num += pairs * (up[0] / up[1] + dn[0] / dn[1])
            paired_n += pairs
    peak, dd = 0.0, 0.0
    for x in curve:
        peak = max(peak, x)
        dd = max(dd, peak - x)
    slip = [((o["fill"]["average_price"] - o["ref_ask"]) * 100.0, o["tag"]) for o in broker.orders
            if o["fill"] and o["ref_ask"] is not None]
    realized = sum(float(p.realized or 0) for p in ledger.positions.values() if p.settled)
    unsettled = sum(p.cost for p in ledger.positions.values() if not p.settled and p.shares > 0)
    known_unsettled_pnl = 0.0
    for p in ledger.positions.values():
        payout = results.get(p.condition_id, {}).get("payouts", {}).get(str(p.token_id))
        if not p.settled and payout is not None:
            known_unsettled_pnl += p.shares * float(payout) - p.cost
    pending_redemption = sum(p.shares * float(p.payout_per_share or 0) for p in ledger.positions.values()
                             if p.settled and broker.redemption_at.get(p.condition_id, math.inf) > broker.clock.now)
    rows = [json.loads(x) for x in (tmp / "sniper.jsonl").read_text(encoding="utf-8").splitlines()] \
        if (tmp / "sniper.jsonl").exists() else []
    sniper_orders = [r for r in rows if r.get("result") in ("filled", "refused", "error")]
    n_markets = len({r["condition_id"] for r in sniper_orders if r.get("condition_id")}) or 0
    out = {
        "label": label, "rounds_with_result": sum(1 for i in rounds.values() if i["condition_id"] in results),
        "total_pnl": realized + known_unsettled_pnl, "realized_pnl": realized, "unsettled_cost": unsettled,
        "known_outcome_unsettled_pnl": known_unsettled_pnl,
        "end_available_cash": broker.cash_balance(), "pending_redemption_payout": pending_redemption,
        "fees": sum(r["fees"] for r in by_src.values()),
        "capital_deployed": sum(r["cost"] for r in by_src.values()),
        "max_exposure": m["max_open_cost"], "max_worst_case_loss": m["max_worst_loss"],
        "max_one_sided_exposure": m["max_one_side"], "max_drawdown": dd,
        "avg_up_cost": side_tot["UP"][0] / side_tot["UP"][1] if side_tot["UP"][1] else None,
        "avg_down_cost": side_tot["DOWN"][0] / side_tot["DOWN"][1] if side_tot["DOWN"][1] else None,
        "paired_cost": paired_cost_num / paired_n if paired_n else None, "paired_shares": paired_n,
        "unpaired_shares": unpaired,
        "sniper_orders": len(sniper_orders), "sniper_fills": sum(1 for r in sniper_orders if r["result"] == "filled"),
        "markets_traded": n_markets,
        "slippage_c_avg": sum(x for x, _ in slip) / len(slip) if slip else None,
        "by_source": {k: {kk: (len(vv) if isinstance(vv, set) else vv) for kk, vv in v.items()} for k, v in by_src.items()},
        "sniper_rows": [{k: r.get(k) for k in ("wall", "side", "result", "reason", "ask_seen", "cap", "amount",
                                               "shares", "price", "target_shares", "carry_in", "fair", "fair_anchor",
                                               "kelly_held_shares", "kelly_opposite_shares", "fee_rate",
                                               "fee_exponent")} for r in sniper_orders],
        "manager": None if pm is None else {
            "orders": len(pm.orders),
            "by_class": dict(collections.Counter(o.classification for o in pm.orders.values())),
            "by_status": dict(collections.Counter(o.status for o in pm.orders.values())),
            "skips": {k[5:]: v for k, v in pm.n.items() if k.startswith("skip ")},
            "slippage_c_avg": (lambda xs: sum(xs) / len(xs) if xs else None)([x for x, t in slip if t == "manager"]),
            # orders whose NEW pairs cost more than the pair threshold at their decision (must be 0)
            "bad_pairs": sum(1 for o in pm.orders.values()
                             if (o.reason_row.get("new_pairs") or 0) > 1e-9
                             and o.reason_row.get("incremental_pair_cost") is not None
                             and o.reason_row["incremental_pair_cost"]
                             > 1.0 - float(o.reason_row.get("threshold_pair") or 0.0) + 1e-9),
            "eval_us": pm.summary()["eval_us"]},
    }
    return out


def replay_warnings(out: dict) -> list[str]:
    """Zero activity has different meanings; expose its inputs and gates."""
    warnings = []
    malformed = sum(v for k, v in out.get("input_diagnostics", {}).items() if k.startswith("malformed"))
    malformed += out.get("trigger_status", {}).get("malformed", 0)
    if malformed:
        warnings.append(f"{malformed} malformed input rows/messages; inspect input_diagnostics and trigger_status")
    accepted = out.get("trigger_status", {}).get("tick", 0)
    if not accepted:
        warnings.append("no trigger ticks accepted; this run cannot validate strategy behavior")
    elif not out.get("sniper_counters", {}).get("signals", 0):
        warnings.append("no signals on accepted ticks; no trading decisions were exercised")
    elif not out.get("sniper_orders", 0):
        warnings.append("signals produced no orders; inspect sniper_counters for the rejecting gates")
    elif not out.get("sniper_fills", 0):
        warnings.append("orders produced no fills; this run did not exercise fill accounting or settlement")
    return warnings


def compare_sniper(a: dict, b: dict) -> dict:
    """Every sniper decision of A against B, matched by time and side."""
    ka = {(round(r["wall"], 3), r["side"]): r for r in a["sniper_rows"]}
    kb = {(round(r["wall"], 3), r["side"]): r for r in b["sniper_rows"]}
    missed = [k for k in ka if k not in kb]
    extra = [k for k in kb if k not in ka]
    changed = [k for k in ka if k in kb and (ka[k]["result"], round(float(ka[k]["shares"] or 0), 4))
               != (kb[k]["result"], round(float(kb[k]["shares"] or 0), 4))]
    return {"sniper_orders_A": len(ka), "sniper_orders_B": len(kb), "missed_in_B": len(missed),
            "extra_in_B": len(extra), "different_outcome": len(changed),
            "examples": [{"key": k, "A": ka[k], "B": kb.get(k)} for k in (missed + changed)[:5]]}


def report(a: dict, b: dict, cmp: dict) -> str:
    f = lambda v, n=2: "--" if v is None else f"{v:,.{n}f}"  # noqa: E731
    rows = [("known-outcome P/L $", "total_pnl", 2), ("settled P/L $", "realized_pnl", 2), ("fees $", "fees", 2),
            ("end available cash $", "end_available_cash", 2),
            ("pending redemption payout $", "pending_redemption_payout", 2),
            ("unsettled cost $", "unsettled_cost", 2),
            ("avg slippage c/share", "slippage_c_avg", 2), ("capital deployed $", "capital_deployed", 2),
            ("max exposure (open cost) $", "max_exposure", 2), ("max one-side exposure $", "max_one_sided_exposure", 2),
            ("max worst-case loss $", "max_worst_case_loss", 2),
            ("max drawdown $", "max_drawdown", 2), ("avg UP cost", "avg_up_cost", 4),
            ("avg DOWN cost", "avg_down_cost", 4), ("paired cost (UP+DOWN)", "paired_cost", 4),
            ("paired shares", "paired_shares", 2), ("unpaired shares", "unpaired_shares", 2),
            ("sniper orders", "sniper_orders", 0), ("sniper fills", "sniper_fills", 0),
            ("markets traded", "markets_traded", 0)]
    out = [f"{'':32s} {'A sniper only':>16s} {'B + manager':>16s} {'B - A':>12s}"]
    for name, k, n in rows:
        va, vb = a.get(k), b.get(k)
        diff = None if va is None or vb is None else vb - va
        out.append(f"{name:32s} {f(va, n):>16s} {f(vb, n):>16s} {f(diff, n):>12s}")
    mg = b.get("manager") or {}
    src = b["by_source"].get("manager", {})
    extra_cap = b["capital_deployed"] - a["capital_deployed"]
    dpl = b["total_pnl"] - a["total_pnl"]
    out += ["", f"rounds with a venue result: {a['rounds_with_result']}",
            f"cash model: settlement {a.get('settlement_delay_s', SETTLEMENT_DELAY_S):g}s after expiry, "
            f"redemption {a.get('redemption_delay_s', REDEMPTION_DELAY_S):g}s after settlement; "
            "known-outcome P/L uses cached final results and does not imply spendable cash",
            "feed freshness gates disabled for compact recordings; recorded timestamps retained, "
            "missing sequences/times synthesized and counted",
            f"A trigger status {a.get('trigger_status')} input diagnostics {a.get('input_diagnostics')}",
            f"B trigger status {b.get('trigger_status')} input diagnostics {b.get('input_diagnostics')}",
            f"manager orders {mg.get('orders')} by class {mg.get('by_class')} by status {mg.get('by_status')}",
            f"manager orders creating pairs above the pair threshold: {mg.get('bad_pairs')}",
            f"manager capital ${f(src.get('cost', 0.0))}  manager fees ${f(src.get('fees', 0.0))}  "
            f"manager-attributable P/L ${f(src.get('pnl', 0.0))}  manager slippage c {f(mg.get('slippage_c_avg'))}",
            f"additional capital deployed ${f(extra_cap)}  P/L change ${f(dpl)}  "
            f"P/L change per additional $1: {f(dpl / extra_cap, 4) if abs(extra_cap) > 1e-9 else '--'}",
            f"incremental max drawdown ${f(b['max_drawdown'] - a['max_drawdown'])}  incremental max worst-case "
            f"loss ${f(b['max_worst_case_loss'] - a['max_worst_case_loss'])}",
            f"return on capital: A {f(a['total_pnl'] / a['capital_deployed'] * 100 if a['capital_deployed'] else None)}%"
            f"  B {f(b['total_pnl'] / b['capital_deployed'] * 100 if b['capital_deployed'] else None)}%",
            f"P/L per $ of max worst-case loss: A {f(a['total_pnl'] / a['max_worst_case_loss'] if a['max_worst_case_loss'] else None, 3)}"
            f"  B {f(b['total_pnl'] / b['max_worst_case_loss'] if b['max_worst_case_loss'] else None, 3)}",
            f"sniper decisions: A {cmp['sniper_orders_A']} B {cmp['sniper_orders_B']}; missed in B "
            f"{cmp['missed_in_B']}, extra in B {cmp['extra_in_B']}, different outcome {cmp['different_outcome']}",
            f"manager skips (sampled): {mg.get('skips')}", f"manager evaluation us: {mg.get('eval_us')}"]
    for variant in (a, b):
        out.extend(f"WARNING {variant['label']}: {warning}" for warning in variant.get("warnings", ()))
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--data", action="append", default=None, help="a recordings folder (repeatable)")
    ap.add_argument("--latency-ms", type=float, default=170.0, help="decision -> venue match (150ms hold + network)")
    ap.add_argument("--cash", type=float, default=1000.0)
    ap.add_argument("--settlement-delay-s", type=float, default=SETTLEMENT_DELAY_S,
                    help="assumed resolution delay after expiry (default: 60s)")
    ap.add_argument("--redemption-delay-s", type=float, default=REDEMPTION_DELAY_S,
                    help="additional delay before settled payouts become collateral (default: 600s)")
    ap.add_argument("--json", default=None)
    ap.add_argument("--feed", choices=REPLAY_FEEDS, default=SPEC_FEED,
                    help=f"the trigger feed to replay (default: the frozen spec's, {SPEC_FEED})")
    args = ap.parse_args(argv)
    if any(not math.isfinite(v) or v < 0 for v in (args.settlement_delay_s, args.redemption_delay_s)):
        ap.error("settlement and redemption delays must be finite and nonnegative")
    dirs = args.data or [os.environ.get("LAB_DATA", str(ROOT / "latency_lab" / "data"))]
    paths = recording_files(dirs)
    if not paths:
        print("no recordings found in", dirs)
        return 1
    rounds = scan_rounds(paths, args.feed)
    until = rounds.pop("__until__")
    ticks = rounds.pop("__btc_ticks__")
    diagnostics = rounds.pop("__input_diagnostics__")
    if not ticks:
        print(f"the recordings hold no {args.feed} ticks: nothing to trigger on (recorded with --feeds none? "
              f"try --feed {'binance' if args.feed == 'coinbase' else 'coinbase'})")
        print("input diagnostics:", diagnostics)
        return 1
    res = outcomes(rounds, pathlib.Path(dirs[-1]) / "replay_outcomes.json")
    rounds["__until__"] = until
    pm_cfg = PMM.PMConfig.from_env({}, global_max_buy_price=0.95, global_max_side_shares=200)
    t0 = time.time()
    a = asyncio.run(run_variant(paths, rounds, res, with_pm=False, pm_cfg=pm_cfg, latency_ms=args.latency_ms,
                                start_cash=args.cash, label="A", feed=args.feed,
                                settlement_delay_s=args.settlement_delay_s, redemption_delay_s=args.redemption_delay_s))
    b = asyncio.run(run_variant(paths, rounds, res, with_pm=True, pm_cfg=pm_cfg, latency_ms=args.latency_ms,
                                start_cash=args.cash, label="B", feed=args.feed,
                                settlement_delay_s=args.settlement_delay_s, redemption_delay_s=args.redemption_delay_s))
    cmp = compare_sniper(a, b)
    rounds.pop("__until__", None)
    print(f"{len(paths)} recordings, {len(rounds)} rounds ({sum(1 for i in rounds.values() if i['condition_id'] in res)}"
          f" with a venue result), {ticks} {args.feed} ticks, replayed twice in {time.time() - t0:.0f}s\n")
    print(report(a, b, cmp))
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps({"A": a, "B": b, "compare": cmp, "files": paths,
                                                       "latency_ms": args.latency_ms, "feed": args.feed},
                                                      indent=1, default=str),
                                           encoding="utf-8")
    return 0 if all(v["trigger_status"].get("tick", 0) for v in (a, b)) else 2


if __name__ == "__main__":
    raise SystemExit(main())
