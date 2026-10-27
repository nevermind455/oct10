#!/usr/bin/env python3
"""Execution latency benchmark - the bot's REAL code paths, no venue, no wallet.

    python bench_execution.py                    # default sizes, table + JSON
    python bench_execution.py --orders 1000 --updates 10000 --json out.json

What runs (all in this process, on one event loop, as in the bot):

  1. market data   N Polymarket book messages through PolyMarketFeed._handle
                   (the socket's own parser and BookState), and N messages
                   of the trigger feed (the frozen spec's: Coinbase BTC-USD
                   tickers through Sniper.on_coinbase; also Binance bookTicker
                   through Sniper.on_tick) that do not trigger: parse + the
                   250ms / k_bp rule (the frozen spec's).
  2. live orders   M triggers through the LIVE path end to end:
                   trigger-feed message -> its handler -> Trigger -> per-side
                   gates -> book -> IOC sizing -> LiveExecution.prepare ->
                   LeanSender (FastOrderBuilder: real EIP-712 signing with a
                   throwaway key made in memory, real POST body + L2 HMAC
                   headers) -> FastH2.post_now on a warm HTTP/2 connection ->
                   a fake CLOB on 127.0.0.1 -> reply classified -> journal row.
                   Sides alternate; a share of orders get MATCHED replies whose
                   private trades are delivered right after (fill path).
                   Includes rapid opposite-side pairs (UP then DOWN at once).
  3. signing       the order builder alone through the production
                   eth_account.LocalAccount.sign_typed_data path. The
                   underlying ECDSA backend is not independently verified;
                   no C-versus-pure-Python comparison is reported.
  4. journal       one Sniper._write (open/append/close) in the system temp
                   directory and next to the bot.

Every stage is perf_counter_ns inside the process. The fake CLOB is local:
"send -> ACK" here is loopback + HTTP/2 framing + our reply handling, NOT the
network or Polymarket's matching (150ms taker hold on crypto markets) - those
only a real order (or bench_network.py for the network part) can measure.
"""
from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import pathlib
import platform
import sys
import tempfile
import time
import types

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - never reads the bot's .env; the benchmark has no secrets
import exec_latency as X  # noqa: E402
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_live as T  # noqa: E402  (FakeH2Clob, live_book, rules_fetch, paper_rules, UP/DN/COND)

SPEC_FEED = str(dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)["primary_feed"])
BENCH_ENV = {"SNIPER_ONLY": "1", "SNIPER_K_BP": str(dict((a, v) for a, _n, v, _l, _f in X.FROZEN_SPEC)["k_bp"]),
             "SNIPER_WINDOW_MS": "250", "SNIPER_ORDER_TYPE": "FAK",
             "SNIPER_SLIP": "0.05", "SNIPER_SHARES": "10", "SNIPER_MAX_TARGET_SHARES": "20",
             "SNIPER_COOLDOWN_MS": "300", "SNIPER_SHADOW_MS": "", "SNIPER_STALL_MS": "0",
             "SNIPER_PRIMARY_FEED": SPEC_FEED, "SNIPER_EXTRA_FEEDS": "",
             # no round-start price or volatility history in a benchmark: the fair-value
             # gate is a strategy rule, not latency - off here like the round caps
             "SNIPER_FAIR": "0",
             # the per-round caps would stop a 1000-order run after 20 fills; they are
             # risk limits, not latency - lifted for the benchmark only
             "SNIPER_MAX_FILLS_ROUND": "1000000", "SNIPER_MAX_SIDE_SHARES": "100000000"}
NO_MATCH = {"success": False, "errorMsg": "no orders found to match with FAK order"}
_SEQ = itertools.count(1)


def tick_raw(feed: str, mid: float, *, stamp: bool = True) -> str:
    """One BTC best bid/ask (mid -/+ 0.005) as the trigger feed's own wire
    message. Coinbase: a full ticker - every field the venue sends, a rising
    sequence and (stamp) the match time - so parsing costs what it costs live."""
    bid, ask = f"{mid - 0.005:.3f}", f"{mid + 0.005:.3f}"
    if feed != "coinbase":
        return json.dumps({"b": bid, "a": ask})
    import datetime as _dt
    msg = {"type": "ticker", "sequence": next(_SEQ), "product_id": S.COINBASE_PRODUCT, "price": ask,
           "open_24h": "60125.01", "volume_24h": "6112.31524733", "low_24h": "59511.2", "high_24h": "60850",
           "volume_30d": "275519.46224301", "best_bid": bid, "best_bid_size": "0.08217445", "best_ask": ask,
           "best_ask_size": "0.31221000", "side": "buy", "trade_id": 812345678, "last_size": "0.00012"}
    if stamp:
        msg["time"] = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return json.dumps(msg)


def feed_tick(sn, raw: str):
    """Hand one trigger-feed message to the handler its socket would call."""
    return (sn.on_coinbase if sn.s.primary_feed == "coinbase" else sn.on_tick)(raw)


def stats(xs) -> dict | None:
    xs = sorted(float(x) for x in xs if x is not None)
    if not xs:
        return None
    q = lambda p: xs[min(len(xs) - 1, int(len(xs) * p / 100))]  # noqa: E731
    return {"n": len(xs), "last": None, "avg": sum(xs) / len(xs), "p50": q(50), "p95": q(95), "p99": q(99),
            "max": xs[-1]}


def stats_last(xs) -> dict | None:
    s = stats(xs)
    if s is not None:
        s["last"] = float([x for x in xs if x is not None][-1])
    return s


# --------------------------------------------------------------------------- 1. market data
def bench_market_data(n: int) -> dict:
    from feeds.book import BookState
    from feeds.poly_market import PolyMarketFeed
    book = BookState(history_s=30)
    book.connected = True
    feed = PolyMarketFeed(book=book)
    book.set_active([T.UP, T.DN])
    now_ms = int(time.time() * 1000)
    snap_ms = now_ms - n - 1                  # the snapshot predates every delta (older deltas are skipped)
    book.apply_snapshot(T.UP, [(0.49, 50)], [(0.50, 30), (0.51, 40)], ts_ms=snap_ms, require_exchange_ts=False)
    book.apply_snapshot(T.DN, [(0.49, 50)], [(0.51, 30), (0.52, 40)], ts_ms=snap_ms, require_exchange_ts=False)
    msgs = []
    first_ms = now_ms - n                     # in the past, one ms apart: never ahead of our clock
    for i in range(n):
        token = T.UP if i % 2 else T.DN
        sell = bool(i % 3)                    # asks 0.50..0.56, bids 0.43..0.48: the book never crosses
        px = f"{0.50 + (i % 7) * 0.01:.2f}" if sell else f"{0.43 + (i % 6) * 0.01:.2f}"
        msgs.append(json.dumps({"event_type": "price_change", "market": T.COND, "timestamp": str(first_ms + i),
                                "price_changes": [{"asset_id": token, "price": px, "size": str(10 + i % 50),
                                                   "side": "SELL" if sell else "BUY", "hash": f"h{i}",
                                                   "best_bid": "0.49", "best_ask": "0.50"}]}))
    per = []
    for m in msgs:
        t = time.perf_counter_ns()
        feed._handle(m)
        per.append((time.perf_counter_ns() - t) / 1000.0)
    return {"book_update_us": stats(per), "invalid": feed.invalid_messages}


def bench_ticks(n: int) -> dict:
    """The trigger feed's handler on n messages that do not trigger - the
    frozen spec's feed, and Binance's for comparison."""
    out = {"feed": SPEC_FEED}
    for feed in dict.fromkeys((SPEC_FEED, "binance")):
        with _env(dict(BENCH_ENV, SNIPER_PRIMARY_FEED=feed)):
            settings = S.Settings()
        hub = types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_end=None, book=None)
        sn = S.Sniper(hub, types.SimpleNamespace(), settings=settings, trade_rows=None)
        sn.fresh.enabled = False
        per = []
        mid = 60_000.0
        for i in range(n):
            mid += 0.01 if i % 2 else -0.01                   # tiny moves: the rule runs, never fires
            raw = tick_raw(feed, mid)
            t = time.perf_counter_ns()
            feed_tick(sn, raw)
            per.append((time.perf_counter_ns() - t) / 1000.0)
        key = "tick_no_trigger_us" if feed == SPEC_FEED else f"{feed}_tick_no_trigger_us"
        out[key] = stats(per)
        out["signals" if feed == SPEC_FEED else f"{feed}_signals"] = sn.n["signals"]
    return out


# --------------------------------------------------------------------------- 2. the live path
class _env:
    def __init__(self, values):
        self.values, self.saved = values, {}

    def __enter__(self):
        for k, v in self.values.items():
            self.saved[k] = os.environ.get(k)
            os.environ[k] = v

    def __exit__(self, *a):
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


async def make_live(tmp: str, *, ledger_src: str | None = None, session_start: float | None = None,
                    durable: bool = False):
    """The production sender (FastOrderBuilder + FastH2 on a warm connection)
    and LiveExecution, against a local HTTP/2 fake CLOB.

    ledger_src: a COPY of a real ledger (its .journal too) - the order path's
    loss-budget read visits the positions settled since session_start, so an
    empty ledger hides what a long-running canary pays (2026-10-25: 3.9ms
    decided -> wire on 10-15, 8.3ms on 10-25). durable: a real CanarySession,
    so every order writes and fsyncs its intent as live does."""
    import base64
    import secrets
    import uuid
    from accounting import Ledger
    from fast_h2 import FastH2
    from feeds.poly_user import FillStore
    from clob_sdk import ApiCreds
    from clob_signing import OrderBuilder
    fake = await T.FakeH2Clob().start()
    T.reset_ambiguity()
    signer = L.fast_signer("0x" + secrets.token_hex(32), 137)          # throwaway, never funded or sent anywhere
    creds = ApiCreds(api_key=str(uuid.uuid4()), api_secret=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
                     api_passphrase=secrets.token_hex(16))
    http = FastH2(fake.url, timeout_s=3.0)
    clock = L.ServerClock()
    rules = L.RoundRules(lambda c, u, d: T.rules_fetch(c, u, d))
    limits = L.CanaryLimits(max_order_shares=25, max_orders=10 ** 9, max_fills=10 ** 9, max_rounds=10 ** 9,
                            max_exposure_usd=1e12, max_daily_loss_usd=1e12)
    builder = S.FastOrderBuilder(OrderBuilder(signer))
    prove = getattr(builder.b, "prove_fast_signing", None)             # as build_live_execution does at start
    if prove is not None:                                              # (absent before 2026-10-25: compare runs)
        prove()
    sender = L.LeanSender(builder=builder, signer=signer, creds=creds,
                          host=fake.url, http=http, clock=clock, rules=rules, limits=limits, kill=L.KillSwitch(None),
                          journal=lambda row: None, timeout_s=3.0)
    await clock.refresh(http, fake.url + "/time")                      # opens the connection, off the hot path
    await rules.prefetch(T.COND, T.UP, T.DN)
    if ledger_src:
        import shutil
        for suffix in ("", ".journal"):
            src = pathlib.Path(ledger_src + suffix)
            if src.exists():
                shutil.copy(src, os.path.join(tmp, "ledger.json" + suffix))
    ledger = Ledger(path=os.path.join(tmp, "ledger.json"))

    def sink(receipt):
        # as run.make_order_observer: one fsynced journal line per accepted order, not
        # a rewrite of the whole ledger (which, from a real ledger, is ~1.7MB a time)
        if not ledger.authorize_order_durable(receipt["order_id"], receipt, venue_min_shares=5.0, price_cap=0.95):
            raise RuntimeError("accepted order id cannot be authorized in the bench ledger")
        return True
    shim = L.LiveBrokerShim(lambda cid: T.paper_rules(), min_buy_price=0.05, max_buy_price=0.95, max_spread=0.25,
                            trade_window_seconds=600.0, min_seconds_to_expiry=10.0)
    shim._rules(T.COND)
    store = FillStore()
    ex = L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=sink,
                         balance_fetch=lambda: {"balance": 1e9, "allowance": 1e9}, clock_url=fake.url + "/time",
                         fills=store, assumed_delay_s=1.0)
    await ex._read_balance()
    if session_start is not None:
        ex.session_start = float(session_start)
    if durable:
        session = L.CanarySession(pathlib.Path(tmp) / "canary_state.json", "bench-wallet")
        session.open(limits, sender.kill, ledger, new_session=True)     # a deliberate fresh budget
        sender.session = ex.session = session
    return fake, ex, store


async def bench_live(m: int, matched_every: int = 5, with_pm: bool = False, pm_stress: bool = False,
                     with_fair: bool = False, ledger_src: str | None = None, session_start: float | None = None,
                     durable: bool = False) -> dict:
    tmp = tempfile.mkdtemp(prefix="bench_live_")
    fake, ex, store = await make_live(tmp, ledger_src=ledger_src, session_start=session_start, durable=durable)
    if with_pm:
        # inventory to manage, so the manager does full evaluations (both sides, every size)
        ex.ledger.record_fill("seed-up", T.UP, shares=20, price=0.45, condition_id=T.COND, fee=0.16)
        ex.ledger.record_fill("seed-dn", T.DN, shares=5, price=0.50, condition_id=T.COND, fee=0.04)
    book = T.live_book(asks=((0.55, 500), (0.56, 500), (0.57, 500)), bids=((0.54, 500),))
    book.apply_snapshot(T.DN, [(0.44, 500)], [(0.45, 500), (0.46, 500)], ts_ms=int(time.time() * 1000),
                        require_exchange_ts=False)
    hub = types.SimpleNamespace(up_token=T.UP, down_token=T.DN, condition_id=T.COND,
                                window_end=T.round_end(min_left=200.0), book=book)
    journal = os.path.join(tmp, "live_sniper.jsonl")
    with _env(dict(BENCH_ENV, SNIPER_FAIR="1" if with_fair else "0")):
        settings = S.Settings()
    sn = S.Sniper(hub, ex.shim, journal_path=journal, settings=settings, trade_rows=[], own_rounds=True, live=ex)
    sn.fresh.enabled = False
    ex.attach(sn) if hasattr(ex, "attach") else None
    if with_fair:
        # Valid synthetic history lets the actual fair-value and Kelly gates
        # execute. Nothing in the production rule or its defaults is changed.
        now = time.monotonic()
        for i in range(301):
            sn.vol_model.observe(now - 300 + i, 60_000.0)
        # The fair value anchors on the book as it stood when the move began
        # (300ms before each tick here); without that history every order is
        # skipped ("no pre-move market history"). A real round's book has it.
        await asyncio.sleep(0.35)
    pm = None
    if with_pm:
        import position_manager as PMM
        from dataclasses import replace as _replace
        pm = PMM.PositionManager(_replace(PMM.PMConfig(enabled=True), poll_ms=5.0), hub=hub, ledger=ex.ledger,
                                 broker=ex.shim, sniper=sn, live_mode=True)       # live: shadow, never orders
        if pm_stress:
            pm._state_version = lambda: object()          # a full evaluation on every 5ms poll
        pm.start()
    n_order = 0
    wall0 = time.perf_counter()

    async def fire(side: str):
        nonlocal n_order
        n_order += 1
        oid, tid = f"0xorder{n_order}", f"trade-{n_order}"
        if n_order % matched_every == 0:
            fake.script.append(("json", 200, {"success": True, "status": "matched", "orderID": oid,
                                               "tradeIDs": [tid], "errorMsg": ""}))
        else:
            fake.script.append(("json", 400, NO_MATCH))
        sn.last_fire = -1e18                                          # the benchmark is not about the cooldown
        now = time.monotonic() * 1000.0
        sn.trigger.q.clear()
        ref = 60_000.0
        sn.trigger.q.append((now - 300.0, ref))
        move = 2.0 if with_fair else 0.6  # enough synthetic edge to pass the real fair-value gate
        mid = ref * (1 + (move if side == "UP" else -move) / 1e4)
        feed_tick(sn, tick_raw(sn.s.primary_feed, mid))
        return oid, tid, n_order % matched_every == 0

    async def settle_side(side, oid, tid, matched):
        """Wait for the reply; for a MATCHED order deliver its private trade
        (the venue's user channel, here immediately) and let the bot read it."""
        deadline = time.monotonic() + 2.0
        while sn.inflight.get(side) is not None and time.monotonic() < deadline:
            await asyncio.sleep(0.0005)
        if matched:
            token = T.UP if side == "UP" else T.DN
            store.record_trade(tid, order_id=oid, asset_id=token, market=T.COND, side="BUY", price=0.55 if side == "UP"
                               else 0.45, size=5.0, status="MATCHED")
            sn._sync_live_fak()

    sides = ("UP", "DOWN")
    pairs = 0
    for i in range(m):
        if i % 50 == 0:                                   # a live feed keeps the book fresh
            now_ms = int(time.time() * 1000)
            book.apply_snapshot(T.UP, [(0.54, 500)], [(0.55, 500), (0.56, 500), (0.57, 500)], ts_ms=now_ms,
                                require_exchange_ts=False)
            book.apply_snapshot(T.DN, [(0.44, 500)], [(0.45, 500), (0.46, 500)], ts_ms=now_ms,
                                require_exchange_ts=False)
        if i % 200 == 0:
            await ex.sender.clock.refresh(ex.sender.http, fake.url + "/time")
            await ex._read_balance()
        if i % 10 == 9:
            # rapid opposite-side pair: UP and DOWN in the same instant (per-side state)
            a = await fire("UP")
            b = await fire("DOWN")
            await settle_side("UP", *a)
            await settle_side("DOWN", *b)
            pairs += 1
            continue
        side = sides[i % 2]
        r = await fire(side)
        await settle_side(side, *r)
    await asyncio.sleep(0.05)
    elapsed = time.perf_counter() - wall0
    if pm is not None:
        await pm.stop()
    for task in list(sn._tasks):
        task.cancel()
    await asyncio.gather(*sn._tasks, return_exceptions=True)
    await ex.sender.http.aclose()
    await fake.stop()
    with open(journal, encoding="utf-8") as journal_file:
        rows = [json.loads(x) for x in journal_file]
    # the Sniper's own order rows (the sender's "live_send" rows and later
    # fill updates repeat an order; they are not new orders)
    orders = [r for r in rows if r.get("live") and r.get("result") in ("filled", "refused", "pending", "error")]
    out = {"orders_sent": len(orders), "pairs": pairs, "elapsed_s": round(elapsed, 2), "fair_value_enabled": with_fair,
           "connections": fake.conns, "halted": list(ex.kill.reasons),
           "persistence_error": ex.ledger.last_persistence_error,
           "skips": {k[6:]: v for k, v in sn.n.items() if k.startswith("skip: ") and v}}

    def us(r, a, b):
        ts = r.get("live_ts_us") or {}
        return (ts[b] - ts[a]) / 1000.0 if a in ts and b in ts else None
    out["stages_ms"] = {
        "tick_to_decided": stats_last([r.get("t_tick_to_decided_ms") for r in orders]),
        "decided_to_prepare": stats_last([us(r, "decided", "prepare") for r in orders]),
        "prepare_checks": stats_last([us(r, "prepare", "build") for r in orders]),
        "build_and_sign": stats_last([us(r, "build", "signed") for r in orders]),
        "body_and_headers": stats_last([us(r, "signed", "ready") for r in orders]),
        "ready_to_write": stats_last([us(r, "ready", "net_send_start") for r in orders]),
        "write_syscall": stats_last([us(r, "net_send_start", "net_send_done") for r in orders]),
        "trigger_to_send": stats_last([r.get("t_signal_to_send_ms") for r in orders]),
        "send_to_ack_loopback": stats_last([r.get("t_send_to_response_ms") for r in orders]),
        "trigger_to_ack_loopback": stats_last([r.get("t_signal_to_response_ms") for r in orders]),
        "trigger_to_fill_known_loopback": stats_last([r.get("fill_confirmed_ms") for r in rows
                                                      if r.get("result") in ("filled", "live_fill_update")
                                                      and r.get("fill_confirmed_ms") is not None]),
    }
    out["new_connection_on_hot_path"] = sum(1 for r in orders if r.get("new_connection"))
    out["matched_unresolved"] = len(sn._fak_pending)
    out["posted"] = sum(1 for o in fake.orders)                     # POSTs the fake CLOB received
    lat = [r.get("lat") or {} for r in orders]
    out["lat_ms"] = {k: stats_last([x.get(k) for x in lat]) for k in ("feed_to_trigger", "trigger_to_decision",
                                                                     "trigger_to_send", "trigger_to_ack")}
    if pm is not None:
        out["pm"] = pm.summary()["eval_us"]
        out["pm_counts"] = dict(pm.n)
        out["pm_last"] = {s: (d.action, d.reason, d.classification) for s, d in pm.last.items()}
    return out


# --------------------------------------------------------------------------- 3. signing
def bench_signing(n: int) -> dict:
    import secrets
    from clob_sdk import CreateOrderOptions, MarketOrderArgsV2, OrderType
    from clob_signing import OrderBuilder
    from clob_signing import BUY
    raw = secrets.token_hex(32)
    # The production builder, proven at start as live and paper prove it: with
    # SNIPER_FAST_SIGN on (default) and the proof passed, orders sign their directly
    # encoded EIP-712 digest with the held key; otherwise sign_typed_data.
    signer = S.fast_signer("0x" + raw, 137)
    builder = S.FastOrderBuilder(OrderBuilder(signer))
    prove = getattr(builder.b, "prove_fast_signing", None)
    note = prove() if prove is not None else ""
    direct = getattr(builder.b, "fast_sign", None) is True
    out = {"signing_path": ("direct EIP-712 digest + held libsecp256k1 key"
                            if direct and getattr(signer, "secp_key", None) is not None
                            else "direct EIP-712 digest + eth_keys key" if direct
                            else "eth_account.LocalAccount.sign_typed_data"),
           "fast_sign_note": note, "default_backend": "unverified", "backend_verified": False}
    per = []
    for i in range(n):
        args = MarketOrderArgsV2(token_id=T.UP, amount=S.signing_amount(5.0 + (i % 50) / 10),
                                 side=BUY, price=S.signing_price(0.6, "0.01"), order_type=OrderType.FAK)
        t = time.perf_counter_ns()
        builder.build_market_order(args, CreateOrderOptions(tick_size="0.01", neg_risk=False), version=2)
        per.append((time.perf_counter_ns() - t) / 1e6)
    out["default_build_and_sign_ms"] = stats(per)
    return out


# --------------------------------------------------------------------------- 4. journal
def bench_journal(n: int) -> dict:
    out = {}
    row = {"wall": time.time(), "id": "x", "side": "UP", "bp": 0.61, "feed": "binance", "result": "skipped",
           "reason": "cooldown", "round_end": 1, "left_s": 100.0, "condition_id": T.COND}
    for label, d in (("temp_dir", tempfile.mkdtemp(prefix="bench_j_")), ("bot_dir", str(ROOT))):
        path = os.path.join(d, f"bench_journal_{os.getpid()}.jsonl")
        # Sniper._write appends through a kept-open descriptor since audit L9: the
        # journal path's own methods and recheck interval, on a bare instance.
        sn = S.Sniper.__new__(S.Sniper)
        sn.journal, sn.n = path, {"journal errors": 0}
        per = []
        for _ in range(n):
            t = time.perf_counter_ns()
            S.Sniper._write(sn, row)
            per.append((time.perf_counter_ns() - t) / 1e6)
        sn._journal_close()
        try:
            os.unlink(path)
        except OSError:
            pass
        out[f"{label}_write_ms"] = stats(per)
    return out


# --------------------------------------------------------------------------- 5. packet -> handler
async def bench_wakeup(n: int, *, dashboard: bool, switch_ms: float | None = None, gap_ms: float = 3.0,
                       frame_every_s: float = 0.0) -> dict:
    """A thread writes n small packets to a loopback socket, each stamped with
    perf_counter_ns at the write; the event loop's protocol stamps when its
    handler runs. The difference is what no stamp inside on_tick can see: the
    kernel hop plus the wait for the loop and the GIL. With dashboard=True a
    thread builds full dashboard frames back to back (the worst case of the
    real --dash, which builds 2 per second on a worker thread)."""
    import socket
    import threading
    loop = asyncio.get_running_loop()
    got: list = []
    done = asyncio.Event()

    class P(asyncio.Protocol):
        buf = b""

        def data_received(self, data):
            now = time.perf_counter_ns()
            self.buf += data
            *lines, self.buf = self.buf.split(b"\n")
            for ln in lines:
                got.append((now - int(ln)) / 1000.0)
            if len(got) >= n:
                done.set()
    server = await loop.create_server(P, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    stop = threading.Event()
    old_switch = sys.getswitchinterval()
    if switch_ms is not None:
        sys.setswitchinterval(switch_ms / 1000.0)

    def frames():
        import tests_dashboard as td
        from dashboard import build
        from dashboard.theme import UNICODE
        snap = td.populated("LIVE")
        while not stop.is_set():
            build(snap, 160, 48, UNICODE)
            if frame_every_s > 0:
                stop.wait(frame_every_s)

    def sender():
        c = socket.create_connection(("127.0.0.1", port))
        c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for _ in range(n):
            c.sendall(f"{time.perf_counter_ns()}\n".encode())
            time.sleep(gap_ms / 1000.0)
        c.close()
    if dashboard:
        threading.Thread(target=frames, daemon=True).start()
    t = threading.Thread(target=sender, daemon=True)
    t.start()
    try:
        await asyncio.wait_for(done.wait(), timeout=n * max(gap_ms, 16.0) / 1000.0 * 2 + 10)  # Windows sleeps >= 15.6ms
    finally:
        stop.set()
        sys.setswitchinterval(old_switch)
        server.close()
    return stats(got)


# --------------------------------------------------------------------------- main
def table(res: dict) -> str:
    lines = []
    f = lambda v: "--" if v is None else (f"{v:.3f}" if v < 10 else f"{v:.1f}")  # noqa: E731

    def add(name, s, unit):
        if s:
            lines.append(f"  {name:34s} n={s['n']:<6d} avg {f(s['avg'])}  p50 {f(s['p50'])}  p95 {f(s['p95'])}  "
                         f"p99 {f(s['p99'])}  max {f(s['max'])} {unit}")
    lines.append("market data")
    add("book update (PolyMarketFeed._handle)", res["market"]["book_update_us"], "us")
    add(f"{res['ticks'].get('feed', 'binance')} tick, no trigger", res["ticks"]["tick_no_trigger_us"], "us")
    add("binance tick, no trigger (on_tick)", res["ticks"].get("binance_tick_no_trigger_us"), "us")
    lines.append(f"live path ({res['live']['orders_sent']} orders, {res['live']['pairs']} UP+DOWN pairs, "
                 f"{res['live']['connections']} connection(s), new conns on hot path: "
                 f"{res['live']['new_connection_on_hot_path']})")
    for k, s in res["live"]["stages_ms"].items():
        add(k, s, "ms")
    lines.append(f"signing ({res['signing']['signing_path']}; backend unverified)")
    add("build+sign, production typed account", res["signing"]["default_build_and_sign_ms"], "ms")
    lines.append("packet -> handler (loopback socket to the event loop)")
    for k, s in res.get("wakeup_us", {}).items():
        add(k, s, "us")
    lines.append("journal row (open/append/close)")
    add("system temp dir", res["journal"]["temp_dir_write_ms"], "ms")
    add("bot dir", res["journal"]["bot_dir_write_ms"], "ms")
    return "\n".join(lines)


async def main_async(args) -> dict:
    res = {"when": time.strftime("%Y-%m-%d %H:%M:%S"), "host": platform.node(), "python": sys.version.split()[0],
           "platform": platform.platform()}
    res["market"] = bench_market_data(args.updates)
    res["ticks"] = bench_ticks(args.updates)
    res["position_manager"] = args.pm
    res["live"] = await bench_live(args.orders, with_pm=args.pm != "off", pm_stress=args.pm == "stress",
                                   with_fair=args.fair, ledger_src=args.ledger, session_start=args.session_start,
                                   durable=args.durable)
    res["signing"] = bench_signing(args.sign)
    res["journal"] = bench_journal(args.journal)
    res["wakeup_us"] = {"idle": await bench_wakeup(args.wakeup, dashboard=False),
                        "dashboard at 2 frames/s": await bench_wakeup(args.wakeup, dashboard=True,
                                                                      frame_every_s=0.5),
                        "dashboard thread, 5ms GIL switch": await bench_wakeup(args.wakeup, dashboard=True),
                        "dashboard thread, 1ms GIL switch": await bench_wakeup(args.wakeup, dashboard=True,
                                                                               switch_ms=1.0)}
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--orders", type=int, default=1000)
    ap.add_argument("--updates", type=int, default=10000)
    ap.add_argument("--sign", type=int, default=300)
    ap.add_argument("--journal", type=int, default=300)
    ap.add_argument("--wakeup", type=int, default=1000, help="packets per packet->handler run")
    ap.add_argument("--json", default=None, help="also write the results here")
    ap.add_argument("--fair", action="store_true", help="exercise real fair-value and Kelly gates on synthetic history")
    ap.add_argument("--pm", choices=("off", "on", "stress"), default="off",
                    help="Position Manager beside the sniper: off, on (5ms polls, an evaluation whenever the state changed), "
                         "stress (a full evaluation every 5ms). Use a fresh process per setting.")
    ap.add_argument("--ledger", default=None,
                    help="a real ledger to COPY into the run (e.g. the VPS's live_ledger.json): the order path's "
                         "loss-budget read grows with the positions settled since --session-start")
    ap.add_argument("--session-start", type=float, default=None,
                    help="the canary session's start (unix seconds; live_canary_state.json started_wall)")
    ap.add_argument("--durable", action="store_true",
                    help="a real canary session: every order writes and fsyncs its send intent, as live does")
    ap.add_argument("--uvloop", action="store_true",
                    help="run on uvloop (Linux/macOS, pip install uvloop) - compare with a run without it "
                         "before setting SNIPER_UVLOOP=1 (audit L10)")
    args = ap.parse_args(argv)
    if args.uvloop:
        import uvloop                                    # ImportError: not installed / not supported here
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            res = runner.run(main_async(args))
        res["event_loop"] = "uvloop " + getattr(uvloop, "__version__", "?")
    else:
        res = asyncio.run(main_async(args))
        res["event_loop"] = "asyncio"
    print(table(res))
    print(f"event loop: {res['event_loop']}")
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
        print(f"\nwritten {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
