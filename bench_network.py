#!/usr/bin/env python3
"""Network benchmark: this box -> the endpoints the bot uses. Read-only.

    python bench_network.py                 # 50 samples each, table
    python bench_network.py --n 200 --json net.json

No wallet, no credentials, no orders: only public requests (GET /time on the
CLOB, the public market and Binance sockets). Run it ON THE VPS that trades -
numbers from any other machine say nothing about the bot's network path.

Measured, per endpoint:
  dns        name resolution (the OS resolver, as the bot uses it)
  tcp        TCP connect
  tls        TLS handshake (ALPN h2 for the CLOB)
  cold GET   a fresh connection per request: DNS + TCP + TLS + request
  warm GET   requests on ONE kept-alive HTTP/2 connection (fast_h2.FastH2,
             the client the live sender writes orders on): the network round
             trip + the CLOB edge answering /time. A live order's send -> ACK
             is this plus Polymarket's matching, which on crypto markets
             includes the venue's 150ms taker hold (changelog, 2026-09-04).
  binance    websocket connect; each trade's exchange event time E against
             this box's receipt time. That includes this box's clock offset
             to Binance (keep the VPS NTP-synced); the spread above the
             minimum is the feed's jitter whatever the offset.
  poly ws    the public market socket (subscribed to the current round's
             two tokens, as the bot is): PING -> PONG round trip
"""
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import socket
import ssl
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

CLOB = "https://clob.polymarket.com"
POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/btcusdt@trade"


def stats(xs) -> dict | None:
    xs = sorted(float(x) for x in xs if x is not None)
    if not xs:
        return None
    q = lambda p: xs[min(len(xs) - 1, int(len(xs) * p / 100))]  # noqa: E731
    return {"n": len(xs), "avg": sum(xs) / len(xs), "p50": q(50), "p95": q(95), "p99": q(99), "max": xs[-1]}


def host_steps(host: str, port: int = 443, alpn=("h2",)) -> dict:
    t0 = time.perf_counter()
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    t1 = time.perf_counter()
    addr = infos[0][4]
    s = socket.create_connection(addr[:2], timeout=10)
    t2 = time.perf_counter()
    ctx = ssl.create_default_context()
    ctx.set_alpn_protocols(list(alpn))
    ss = ctx.wrap_socket(s, server_hostname=host)
    t3 = time.perf_counter()
    proto = ss.selected_alpn_protocol()
    ss.close()
    return {"dns_ms": (t1 - t0) * 1000, "tcp_ms": (t2 - t1) * 1000, "tls_ms": (t3 - t2) * 1000,
            "ip": addr[0], "alpn": proto}


async def warm_gets(n: int) -> dict:
    from fast_h2 import FastH2
    h = FastH2(CLOB, timeout_s=10)
    rtts = []
    try:
        t = time.perf_counter()
        r = await h.get(CLOB + "/time")                 # opens the connection
        first = (time.perf_counter() - t) * 1000
        for _ in range(n):
            t = time.perf_counter()
            await h.get(CLOB + "/time")
            rtts.append((time.perf_counter() - t) * 1000)
            await asyncio.sleep(0.05)
        connects = h.connects
    finally:
        await h.aclose()
    return {"first_request_ms": first, "warm_rtt_ms": stats(rtts), "connections": connects}


async def cold_gets(n: int) -> dict:
    from fast_h2 import FastH2
    out = []
    for _ in range(n):
        h = FastH2(CLOB, timeout_s=10)
        t = time.perf_counter()
        try:
            await h.get(CLOB + "/time")
            out.append((time.perf_counter() - t) * 1000)
        finally:
            await h.aclose()
        await asyncio.sleep(0.1)
    return {"cold_request_ms": stats(out)}


async def binance(n: int) -> dict:
    import websockets
    t = time.perf_counter()
    lags, ticks, gaps = [], 0, []
    async with websockets.connect(BINANCE_WS, open_timeout=10, compression=None) as ws:
        connect_ms = (time.perf_counter() - t) * 1000
        last = None
        while len(lags) < n:
            raw = await asyncio.wait_for(ws.recv(), 20)
            now = time.time()
            d = json.loads(raw).get("data") or {}
            mono = time.perf_counter()
            if last is not None:
                gaps.append((mono - last) * 1000)
            last = mono
            if d.get("e") == "trade" and d.get("E"):
                lags.append(now * 1000 - float(d["E"]))
            else:
                ticks += 1
    floor = min(lags) if lags else 0.0
    return {"connect_ms": connect_ms, "trade_event_to_receipt_ms": stats(lags),
            "jitter_above_min_ms": stats([x - floor for x in lags]), "bookticker_msgs": ticks,
            "inter_message_gap_ms": stats(gaps),
            "note": "event->receipt includes this box's clock offset to Binance; the jitter does not"}


async def poly_ws(n: int) -> dict:
    import websockets
    t = time.perf_counter()
    rtts = []
    import market_discovery
    import timer
    tokens = await asyncio.to_thread(market_discovery.get_tokens_for_current_round, timer.window_start())
    if not tokens:
        raise RuntimeError("could not discover the current round's tokens to subscribe to")
    async with websockets.connect(POLY_WS, open_timeout=10, ping_interval=None, compression=None) as ws:
        connect_ms = (time.perf_counter() - t) * 1000
        await ws.send(json.dumps({"assets_ids": [tokens["up_token_id"], tokens["down_token_id"]], "type": "market"}))
        for _ in range(n):
            t = time.perf_counter()
            await ws.send("PING")
            while True:
                msg = await asyncio.wait_for(ws.recv(), 10)
                if str(msg).strip().upper() == "PONG":
                    break
            rtts.append((time.perf_counter() - t) * 1000)
            await asyncio.sleep(0.05)
    return {"connect_ms": connect_ms, "ping_pong_ms": stats(rtts)}


async def main_async(n: int) -> dict:
    res = {"when": time.strftime("%Y-%m-%d %H:%M:%S %z"), "host": socket.gethostname()}
    for name, host in (("clob", "clob.polymarket.com"), ("poly_ws", "ws-subscriptions-clob.polymarket.com"),
                       ("binance", "stream.binance.com")):
        try:
            res[f"{name}_handshake"] = await asyncio.to_thread(
                host_steps, host, 9443 if name == "binance" else 443, ("h2",) if name == "clob" else ("http/1.1",))
        except Exception as exc:  # noqa: BLE001 - reported, the rest still runs
            res[f"{name}_handshake"] = {"error": f"{type(exc).__name__}: {exc}"}
    for name, fn in (("clob_warm", lambda: warm_gets(n)), ("clob_cold", lambda: cold_gets(max(5, n // 5))),
                     ("poly_ws", lambda: poly_ws(min(n, 50)))):
        try:
            res[name] = await fn()
        except Exception as exc:  # noqa: BLE001
            res[name] = {"error": f"{type(exc).__name__}: {exc}"}
    try:
        res["binance"] = await binance(n)
    except Exception as exc:  # noqa: BLE001
        res["binance"] = {"error": f"{type(exc).__name__}: {exc}"}
    return res


def table(res: dict) -> str:
    f = lambda v: "--" if v is None else f"{v:.1f}"  # noqa: E731
    out = [f"network from {res['host']} at {res['when']}"]
    for name in ("clob", "poly_ws", "binance"):
        h = res.get(f"{name}_handshake") or {}
        out.append(f"  {name:8s} dns {f(h.get('dns_ms'))}ms  tcp {f(h.get('tcp_ms'))}ms  tls {f(h.get('tls_ms'))}ms"
                   f"  ip {h.get('ip')} alpn {h.get('alpn')}" if "error" not in h else f"  {name:8s} {h['error']}")

    def line(label, s):
        if s:
            out.append(f"  {label:34s} n={s['n']:<4d} avg {f(s['avg'])}  p50 {f(s['p50'])}  p95 {f(s['p95'])}  "
                       f"p99 {f(s['p99'])}  max {f(s['max'])} ms")
    w = res.get("clob_warm") or {}
    if "error" in w:
        out.append(f"  clob warm: {w['error']}")
    else:
        line("CLOB warm HTTP/2 GET /time", w.get("warm_rtt_ms"))
        out.append(f"  (first request incl. connect {f(w.get('first_request_ms'))}ms; connections {w.get('connections')})")
    c = res.get("clob_cold") or {}
    line("CLOB cold (new connection each)", c.get("cold_request_ms")) if "error" not in c else out.append(
        f"  clob cold: {c['error']}")
    p = res.get("poly_ws") or {}
    line("Polymarket market WS ping->pong", p.get("ping_pong_ms")) if "error" not in p else out.append(
        f"  poly ws: {p['error']}")
    b = res.get("binance") or {}
    if "error" in b:
        out.append(f"  binance: {b['error']}")
    else:
        line("Binance trade event -> receipt", b.get("trade_event_to_receipt_ms"))
        line("  jitter above the minimum", b.get("jitter_above_min_ms"))
        out.append("  (event -> receipt includes this box's clock offset to Binance; the jitter does not)")
        line("Binance inter-message gap", b.get("inter_message_gap_ms"))
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--json", default=None)
    args = ap.parse_args(argv)
    res = asyncio.run(main_async(args.n))
    print(table(res))
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
