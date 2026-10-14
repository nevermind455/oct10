#!/usr/bin/env python3
"""Live order path as the canary's history and the ledger grow - the costs
bench_execution.py cannot see (it runs with no canary session and a 2-position
ledger). No venue, no wallet, no network: a fake HTTP/2 CLOB on 127.0.0.1 and
a throwaway signing key made in memory.

    python bench_live_state.py                  # the full sweep, one fresh process per case
    python bench_live_state.py --quick          # fewer orders per case
    python bench_live_state.py --feed-burst     # Binance futures bursts beside the dashboard
    python bench_live_state.py --json out.json

Each case runs the bot's REAL objects together, as `run.py --live --dash` does:

  orders      the sniper's live path end to end (trigger -> decision -> prepare
              -> sign -> durable send intent -> HTTP/2 write -> reply -> journal),
              every 5th order MATCHED with a CONFIRMED private trade
  session     a CanarySession restored with N earlier (resolved) send intents
  ledger      a live ledger holding P earlier positions (3 lots each)
  ledger loop run._ledger_loop booking the confirmed trades on a worker
  dashboard   run.DashView.snapshot() twice a second on the event loop and the
              frame built on a worker, as _dashboard_frames does
  loop probe  a thread writes a stamped packet every 5ms to a loopback socket the
              event loop reads (as a feed's socket): packet written -> handler ran

Reported per case: signal -> wire (p50/p95/p99/max), the pre-send durable
intent write, the ledger reads the order path makes, event-loop lag, and the
Python heap growth per 1,000 orders (tracemalloc).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import tracemalloc
import types

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - never reads the bot's .env

CASES = [
    {"intents": 0, "positions": 100},
    {"intents": 1000, "positions": 100},
    {"intents": 3000, "positions": 100},
    {"intents": 10000, "positions": 100},
    {"intents": 0, "positions": 1000},
    {"intents": 0, "positions": 5000},
    {"intents": 0, "positions": 20000},
    {"intents": 3000, "positions": 5000},
]


def pct(xs) -> dict | None:
    xs = sorted(float(x) for x in xs if x is not None)
    if not xs:
        return None
    q = lambda p: xs[min(len(xs) - 1, int(len(xs) * p / 100))]  # noqa: E731
    return {"n": len(xs), "p50": round(q(50), 3), "p95": round(q(95), 3), "p99": round(q(99), 3),
            "max": round(xs[-1], 3)}


def _old_intent(i: int) -> dict:
    """One resolved send intent as the session file holds it (a FAK that missed)."""
    order = {"salt": 100000 + i, "maker": "0x" + "a" * 40, "signer": "0x" + "b" * 40, "taker": "0x" + "0" * 40,
             "tokenId": "1" * 77, "makerAmount": "8200000", "takerAmount": "19116279", "side": "BUY",
             "expiration": "0", "nonce": "0", "feeRateBps": "0", "signatureType": 2,
             "signature": "0x" + "c" * 130, "timestamp": "1791885079"}
    return {"status": "NO_MATCH", "kind": "NO_MATCH", "wall": 1.79e9 + i, "end": 1.79e9 + i,
            "condition": "0x" + "d" * 64, "token": "1" * 77, "side": "UP", "amount": 8.2, "reserve": 8.6,
            "order_type": "FAK", "price": 0.47, "body_sha256": "e" * 64, "signed_order": order, "order_id": None}


def _seed_session_file(path: pathlib.Path, n: int) -> None:
    """A version-2 canary state file with n resolved intents - the format every
    existing install has. The repaired code migrates it on open()."""
    now = time.time()
    state = {"version": 2, "session_id": "b" * 32, "wallet": "bench-wallet", "started_wall": now - 3600,
             "orders": n, "fills": 0, "exposure_usd": 0.0, "rounds": [], "kill_reasons": [],
             "pending_fills": 0, "intents": {f"old-{i}": _old_intent(i) for i in range(n)}}
    path.write_text(json.dumps(state), encoding="utf-8")


def _seed_ledger(ledger, n: int) -> None:
    for i in range(n):
        cid = "0x" + f"{i // 2 + 1:064x}"
        tok = f"9{i:08d}"
        for j in range(3):
            ledger.record_fill(f"seed-{i}-{j}", tok, shares=10.0, price=0.47, condition_id=cid, fee=0.01,
                               order_id=f"seed-order-{i}")
        pos = ledger.positions[tok]
        pos.settled, pos.settled_wall, pos.payout_per_share = True, time.time() - 7200, 0.0
        pos.realized = -pos.cost
    ledger.save()


class Timed:
    """Wrap an instance's method and collect its call times (ms)."""

    def __init__(self, obj, name: str):
        self.obj, self.name, self.ms = obj, name, []
        self.orig = getattr(obj, name)

        def wrapper(*a, **k):
            t = time.perf_counter_ns()
            try:
                return self.orig(*a, **k)
            finally:
                self.ms.append((time.perf_counter_ns() - t) / 1e6)
        setattr(obj, name, wrapper)


async def one_case(intents: int, positions: int, orders: int) -> dict:
    import bench_execution as B
    import polymarket_trade as pt
    import run
    import sniper_live as L
    import sniper_mode as S
    import tests_sniper_live as T
    from dashboard import build, glyphs
    pt.get_balance_allowance = lambda: None                    # the ledger loop's balance read: no network
    tmp = tempfile.mkdtemp(prefix="bench_state_")
    try:
        fake, ex, store = await B.make_live(tmp)
        ledger = ex.ledger
        await asyncio.to_thread(_seed_ledger, ledger, positions)
        # the production observer that makes each accepted order's authorization durable
        observer = getattr(run, "make_order_observer", None)
        if observer is not None:
            ex.receipt_sink = observer(ledger, venue_min_shares=5.0, price_cap=0.95)
        state = pathlib.Path(tmp) / "live_canary_state.json"
        if intents:
            _seed_session_file(state, intents)
        session = L.CanarySession(state, "bench-wallet")
        session.open(ex.sender.limits, ex.kill, ledger)
        ex.session, ex.sender.session, ex.session_start = session, session, session.started_wall
        book = T.live_book(asks=((0.55, 500), (0.56, 500), (0.57, 500)), bids=((0.54, 500),))
        book.apply_snapshot(T.DN, [(0.44, 500)], [(0.45, 500), (0.46, 500)], ts_ms=int(time.time() * 1000),
                            require_exchange_ts=False)
        hub = types.SimpleNamespace(up_token=T.UP, down_token=T.DN, condition_id=T.COND, window_start=None,
                                    window_end=T.round_end(min_left=200.0), book=book, fill_store=store,
                                    health=lambda: {}, prepared_round=None)
        journal = os.path.join(tmp, "live_sniper.jsonl")
        with B._env(dict(B.BENCH_ENV, SNIPER_FAIR="1")):        # fair value + Kelly, as the frozen spec runs
            settings = S.Settings()
        sn = S.Sniper(hub, ex.shim, journal_path=journal, settings=settings, trade_rows=[], own_rounds=True, live=ex)
        sn.fresh.enabled = False
        ex.attach(sn)
        now = time.monotonic()
        for i in range(301):                                  # valid history: the real fair/Kelly gates run
            sn.vol_model.observe(now - 300 + i, 60_000.0)
        await asyncio.sleep(0.35)                             # the book's pre-move history
        stop = asyncio.Event()
        rt = types.SimpleNamespace(paper=False, mode="LIVE", hub=hub, sniper=sn, ledger=ledger, broker=None,
                                   live_exec=ex, settler=None, pm=None, journal_path=journal, started=time.time(),
                                   trade_rows=[], stop=stop)
        view = run.DashView(rt, accounting_every_s=1.0, accounting_off_loop=True)
        g = glyphs()
        lags: list = []
        timers = {"pre_send_intent": Timed(session, "before_send"),
                  "risk_budget": Timed(ex, "_risk_budget_used"),
                  "own_risk_shares": Timed(ex, "own_risk_shares"),
                  "open_leg_shares": Timed(ledger, "open_leg_shares")}

        import socket
        import threading
        loop = asyncio.get_running_loop()

        class Probe(asyncio.Protocol):
            buf = b""

            def data_received(self, data):
                now = time.perf_counter_ns()
                self.buf += data
                *lines, self.buf = self.buf.split(b"\n")
                for ln in lines:
                    lags.append((now - int(ln)) / 1e6)
        probe_srv = await loop.create_server(Probe, "127.0.0.1", 0)
        probe_port = probe_srv.sockets[0].getsockname()[1]
        probe_stop = threading.Event()

        def probe_sender():
            c = socket.create_connection(("127.0.0.1", probe_port))
            c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            while not probe_stop.is_set():
                c.sendall(f"{time.perf_counter_ns()}\n".encode())
                probe_stop.wait(0.005)
            c.close()
        threading.Thread(target=probe_sender, daemon=True).start()

        async def dashboard():
            while not stop.is_set():
                snap = view.snapshot()
                await asyncio.to_thread(build, snap, 200, 50, g)
                await asyncio.sleep(0.5)

        tasks = [asyncio.create_task(dashboard()),
                 asyncio.create_task(run._ledger_loop(hub, ledger, stop))]
        tracemalloc.start()
        mem0 = tracemalloc.get_traced_memory()[0]
        n_order = 0

        async def fire(side: str):
            nonlocal n_order
            n_order += 1
            oid, tid = f"0xorder{n_order}", f"trade-{n_order}"
            matched = n_order % 5 == 0
            if matched:
                fake.script.append(("json", 200, {"success": True, "status": "matched", "orderID": oid,
                                                   "tradeIDs": [tid], "errorMsg": ""}))
            else:
                fake.script.append(("json", 400, B.NO_MATCH))
            sn.last_fire = -1e18
            now = time.monotonic() * 1000.0
            sn.trigger.q.clear()
            sn.trigger.q.append((now - 300.0, 60_000.0))
            mid = 60_000.0 * (1 + (2.0 if side == "UP" else -2.0) / 1e4)   # enough edge for the real fair gate
            B.feed_tick(sn, B.tick_raw(sn.s.primary_feed, mid))
            deadline = time.monotonic() + 3.0
            while sn.inflight.get(side) is not None and time.monotonic() < deadline:
                await asyncio.sleep(0.0005)
            if matched:
                token = T.UP if side == "UP" else T.DN
                store.record_trade(tid, order_id=oid, asset_id=token, market=T.COND, side="BUY",
                                   price=0.55 if side == "UP" else 0.45, size=5.0, status="CONFIRMED")
                sn._sync_live_fak()

        wall0 = time.perf_counter()
        fresh_at = -1e9
        for i in range(orders):
            if time.monotonic() - fresh_at > 0.4:                 # a live market feed keeps the book fresh
                fresh_at = time.monotonic()
                now_ms = int(time.time() * 1000)
                book.apply_snapshot(T.UP, [(0.54, 500)], [(0.55, 500), (0.56, 500), (0.57, 500)], ts_ms=now_ms,
                                    require_exchange_ts=False)
                book.apply_snapshot(T.DN, [(0.44, 500)], [(0.45, 500), (0.46, 500)], ts_ms=now_ms,
                                    require_exchange_ts=False)
                await ex.sender.clock.refresh(ex.sender.http, fake.url + "/time")
                await ex._read_balance()
            await fire(("UP", "DOWN")[i % 2])
            await asyncio.sleep(0.02)                         # orders arrive apart, as signals do
        elapsed = time.perf_counter() - wall0
        mem1 = tracemalloc.get_traced_memory()[0]
        tracemalloc.stop()
        probe_stop.set()
        probe_srv.close()
        stop.set()
        await asyncio.sleep(0.05)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for task in list(sn._tasks):
            task.cancel()
        await asyncio.gather(*sn._tasks, return_exceptions=True)
        stopper = getattr(session, "close", None)
        if callable(stopper):
            stopper()
        await ex.sender.http.aclose()
        await fake.stop()
        rows = [json.loads(x) for x in open(journal, encoding="utf-8")]
        sent = [r for r in rows if r.get("live") and r.get("result") in ("filled", "refused", "pending", "error")]
        return {"intents": intents, "positions": positions, "orders_sent": len(sent), "elapsed_s": round(elapsed, 2),
                "halted": list(ex.kill.reasons), "skips": {k[6:]: v for k, v in sn.n.items()
                                                            if k.startswith("skip: ") and v},
                "signal_to_wire_ms": pct([r.get("t_signal_to_send_ms") for r in sent]),
                "pre_send_intent_ms": pct(timers["pre_send_intent"].ms),
                "ledger_reads_ms": {k: pct(timers[k].ms) for k in ("risk_budget", "own_risk_shares", "open_leg_shares")},
                "event_loop_lag_ms": pct(lags),
                "heap_growth_kb_per_1000_orders": round((mem1 - mem0) / 1024 / max(1, len(sent)) * 1000, 1),
                "session_file_kb": round(state.stat().st_size / 1024, 1) if state.exists() else None}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------------- feed bursts (H3/H4)
async def feed_burst(seconds: float, coalesce: str, gate: str) -> dict:
    """Binance futures bookTicker frames from a local websocket - steady 400/s
    with a 3,000/s burst of 200ms every second, every frame stamped fresh -
    through Sniper._extra_feed, while DashView.snapshot() runs twice a second
    on the loop with full (20,000-sample) feed histories, as after an hour live."""
    import random
    import websockets
    import run
    import sniper_mode as S
    os.environ.update({"SNIPER_ONLY": "1", "SNIPER_EXTRA_FEEDS": "binance_fut", "SNIPER_FAIR": "0",
                       "SNIPER_FEED_COALESCE": coalesce, "SNIPER_BACKLOG_GATE": gate})
    settings = S.Settings()
    hub = types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_end=None, book=None,
                                health=lambda: {}, prepared_round=None)
    sn = S.Sniper(hub, types.SimpleNamespace(), settings=settings, trade_rows=None)
    for fr in (sn.cb_fresh, sn.bf_fresh):                    # an hour of history, as a running bot holds
        for _ in range(20000):
            fr.lag.append(random.gauss(46, 8))
            fr.excess.append(abs(random.gauss(3, 10)))
    for _ in range(5000):
        sn.cb_behind.append(abs(random.gauss(3, 5)))
    stop = asyncio.Event()

    def frame(mid):
        e = int(time.time() * 1000)
        return json.dumps({"e": "bookTicker", "u": 1, "s": "BTCUSDT", "b": f"{mid - 0.05:.2f}", "B": "1",
                           "a": f"{mid + 0.05:.2f}", "A": "1", "T": e, "E": e})

    async def server(ws):
        mid, t0 = 60_000.0, time.monotonic()
        while not stop.is_set():
            el = time.monotonic() - t0
            burst = (el % 1.0) < 0.2
            rate = 3000 if burst else 400
            # a 1bp step every 0.5s: the rule fires; the rest is noise below the threshold
            mid = 60_000.0 * (1 + (1e-4 if int(el * 2) % 2 else 0))
            for _ in range(max(1, rate // 100)):
                await ws.send(frame(mid + random.uniform(-0.2, 0.2)))
            await asyncio.sleep(0.01)
    srv = await websockets.serve(server, "127.0.0.1", 0, compression=None)
    port = srv.sockets[0].getsockname()[1]
    S.EXTRA_FEEDS["binance_fut"] = (f"ws://127.0.0.1:{port}", None)
    rt = types.SimpleNamespace(paper=True, mode="PAPER", hub=hub, sniper=sn, ledger=None, broker=None, live_exec=None,
                               settler=None, pm=None, journal_path=os.devnull, started=time.time(), trade_rows=[],
                               stop=stop)
    view = run.DashView(rt, accounting_every_s=1.0, accounting_off_loop=True)

    async def dashboard():
        while not stop.is_set():
            view.snapshot()
            await asyncio.sleep(0.5)
    feed = asyncio.create_task(sn._extra_feed("binance_fut"))
    dash = asyncio.create_task(dashboard())
    await asyncio.sleep(seconds)
    stop.set()
    for t in (feed, dash):
        t.cancel()
    await asyncio.gather(feed, dash, return_exceptions=True)
    srv.close()
    signals = sn.n["signals binance_fut"]
    return {"seconds": seconds, "coalesce": coalesce, "gate": gate, "ticks": sn.n["binance_fut ticks"],
            "signals": signals, "backlog_refusals": sn.n["skip: binance_fut receive backlog"],
            "fresh_backlog_refusals": sn.n.get("backlog refusals with fresh data binance_fut", None),
            "backlog_max": sn.bf_fresh.backlog_max_seen}


# --------------------------------------------------------------------------- driver
def _child(spec: dict) -> dict:
    if spec.get("feed_burst"):
        return asyncio.run(feed_burst(spec["seconds"], spec.get("coalesce", "0"), spec.get("gate", "count")))
    return asyncio.run(one_case(spec["intents"], spec["positions"], spec["orders"]))


def run_child(spec: dict, root: pathlib.Path) -> dict:
    p = subprocess.run([sys.executable, str(root / "bench_live_state.py"), "--one", json.dumps(spec)],
                       cwd=str(root), capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=3600)
    for line in reversed(p.stdout.splitlines()):
        if line.startswith("RESULT "):
            return json.loads(line[7:])
    return {"error": (p.stderr or p.stdout)[-2000:], **spec}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--orders", type=int, default=300)
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--feed-burst", action="store_true")
    ap.add_argument("--seconds", type=float, default=20.0)
    ap.add_argument("--root", default=str(ROOT), help="run the cases against the code in this directory")
    ap.add_argument("--json", default=None)
    ap.add_argument("--one", default=None, help=argparse.SUPPRESS)
    args = ap.parse_args(argv)
    if args.one:
        print("RESULT " + json.dumps(_child(json.loads(args.one))), flush=True)
        return 0
    root = pathlib.Path(args.root).resolve()
    res = {"when": time.strftime("%Y-%m-%d %H:%M:%S"), "host": platform.node(), "python": sys.version.split()[0],
           "platform": platform.platform(), "code": root.name, "cases": []}
    if args.feed_burst:
        for coalesce, gate in (("0", "count"), ("0", "age"), ("1", "count")):
            r = run_child({"feed_burst": True, "seconds": args.seconds, "coalesce": coalesce, "gate": gate}, root)
            res["cases"].append(r)
            print(json.dumps(r), flush=True)
    else:
        for case in CASES:
            n = args.orders if not args.quick else 60
            if case["intents"] >= 10000:
                n = min(n, 100)
            r = run_child({**case, "orders": n}, root)
            res["cases"].append(r)
            s, w, lag = r.get("signal_to_wire_ms") or {}, r.get("pre_send_intent_ms") or {}, r.get("event_loop_lag_ms") or {}
            print(f"intents {case['intents']:>6} positions {case['positions']:>6}: signal->wire p50 {s.get('p50')} "
                  f"p99 {s.get('p99')} max {s.get('max')} ms | intent write p50 {w.get('p50')} p99 {w.get('p99')} | "
                  f"loop lag p99 {lag.get('p99')} max {lag.get('max')} | heap +{r.get('heap_growth_kb_per_1000_orders')}"
                  f"KB/1k | {r.get('error', '')[:200]}", flush=True)
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(res, indent=1), encoding="utf-8")
        print(f"written {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
