#!/usr/bin/env python3
"""Network benchmark: this box -> the endpoints the bot uses. Read-only.

    python bench_network.py                 # 50 samples each, 60s Coinbase, 120s race, table
    python bench_network.py --n 200 --feed-s 120 --race-s 600 --json net.json

No wallet, no credentials, no orders: only public requests (GET /time on the
CLOB and on Coinbase, the public market, Coinbase and Binance sockets). Run it
ON THE VPS that trades - numbers from any other machine say nothing about the
bot's network path.

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
  coinbase   the trigger feed (SNIPER_PRIMARY_FEED=coinbase): each ticker's
             match time against this box's receipt, the gaps between tickers
             (a gap over 2s restarts the rule), and this box's clock offset to
             Coinbase (GET /time on one kept-alive connection) to correct it
  race       Coinbase and Binance at once, each through the sniper's own rule
             (sniper_mode.Trigger, the frozen spec's k_bp / 250ms) on this
             box's receipt clock: which feed signals a move first, by how much
  budget     BTC match on Coinbase -> our order matched at Polymarket: feed
             delay + in-process (~0.4ms, bench_execution.py) + one-way to the
             CLOB + the venue's taker hold + the paper model's server allowance
"""
from __future__ import annotations

import argparse
import asyncio
import collections
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
BINANCE_BOOK_WS = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
COINBASE_HOST = "api.exchange.coinbase.com"
# The venue's taker hold on crypto up/down markets (Polymarket changelog: 150ms
# since 2026-09-04); the paper model's default SNIPER_HOLD_MS is 250.
VENUE_HOLD_MS = 150.0


def progress(msg: str) -> None:
    print(f"  ... {msg}", file=sys.stderr, flush=True)


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


def coinbase_clock(n: int = 10) -> dict:
    """This box's clock against Coinbase's: GET /time (epoch with ms) on one
    kept-alive connection. offset = Coinbase time - this box's time at the
    middle of the round trip, from the sample with the shortest round trip;
    it is uncertain by up to half that round trip. A Coinbase receipt delay
    read on this clock is corrected by adding the offset."""
    import http.client
    conn = http.client.HTTPSConnection(COINBASE_HOST, timeout=10)
    samples = []
    try:
        for i in range(n + 1):
            wall0, t0 = time.time(), time.perf_counter()
            conn.request("GET", "/time", headers={"User-Agent": "sniperbtc-bench", "Accept": "application/json"})
            body = conn.getresponse().read()
            rtt = (time.perf_counter() - t0) * 1000
            if i == 0:
                continue                                    # the first request also opened the connection
            samples.append((rtt, float(json.loads(body)["epoch"]) * 1000 - (wall0 * 1000 + rtt / 2)))
            time.sleep(0.05)
    finally:
        conn.close()
    rtt, offset = min(samples)
    return {"rtt_ms": stats([s[0] for s in samples]), "offset_ms": offset, "offset_uncertainty_ms": rtt / 2}


async def coinbase_feed(seconds: float) -> dict:
    """The trigger feed for `seconds`: ticker match time -> receipt on this
    box's clock, the gaps between tickers, heartbeats. The first ticker after
    subscribing repeats the LAST match (possibly seconds old) and is skipped."""
    import websockets
    import sniper_mode as S
    lags, gaps = [], []
    heartbeats = tickers = 0
    t = time.perf_counter()
    async with websockets.connect(S.COINBASE_WS_URL, open_timeout=10, compression=None) as ws:
        connect_ms = (time.perf_counter() - t) * 1000
        await ws.send(json.dumps(S.COINBASE_SUBSCRIBE))
        end, last, first = time.perf_counter() + seconds, None, True
        while time.perf_counter() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), max(0.05, end - time.perf_counter()))
            except asyncio.TimeoutError:
                break
            wall, mono = time.time(), time.perf_counter()
            d = json.loads(raw)
            kind = d.get("type")
            if kind == "heartbeat":
                heartbeats += 1
                continue
            if kind == "error":
                raise RuntimeError(f"Coinbase: {d.get('message')}: {d.get('reason')}")
            if kind != "ticker" or S.coinbase_mid(d) is None:
                continue
            if first:
                first, last = False, mono
                continue
            ex = S._exchange_ms(d.get("time"))
            if ex is not None:
                lags.append(wall * 1000 - ex)
            gaps.append((mono - last) * 1000)
            last, tickers = mono, tickers + 1
    floor = min(lags) if lags else 0.0
    return {"connect_ms": connect_ms, "seconds": seconds, "tickers": tickers, "per_s": tickers / seconds,
            "heartbeats": heartbeats, "match_to_receipt_ms": stats(lags),
            "jitter_above_min_ms": stats([x - floor for x in lags]), "gap_ms": stats(gaps),
            "gaps_over_2s": sum(1 for g in gaps if g > 2000.0),
            "note": "match -> receipt is on this box's clock: add the clock offset for the true delay"}


def episodes(signals, cooldown_ms: float = 300.0) -> list:
    """One move, one episode: the first signal of a direction, then no other
    of that direction until cooldown_ms after it (the sniper's per-side
    cooldown). signals: [(t_ms, direction)] in time order."""
    out, last = [], {}
    for t, d in signals:
        if t - last.get(d, -1e18) >= cooldown_ms:
            out.append((t, d))
            last[d] = t
    return out


def match_episodes(a, b, window_ms: float = 1000.0) -> dict:
    """Pair each episode of feed a with the nearest same-direction episode of
    feed b within window_ms (each b episode used once). lead_ms > 0: a
    signalled first, by that much."""
    used, leads = set(), []
    for ta, da in a:
        best = None
        for j, (tb, db) in enumerate(b):
            if j in used or db != da or abs(tb - ta) > window_ms:
                continue
            if best is None or abs(tb - ta) < abs(b[best][0] - ta):
                best = j
        if best is not None:
            used.add(best)
            leads.append(b[best][0] - ta)
    return {"matched": len(leads), "a_first": sum(1 for x in leads if x > 0), "b_first": sum(1 for x in leads if x < 0),
            "tie": sum(1 for x in leads if x == 0), "only_a": len(a) - len(leads), "only_b": len(b) - len(used),
            "a_lead_ms": stats([x for x in leads if x > 0]), "b_lead_ms": stats([-x for x in leads if x < 0]),
            "lead_ms": stats(leads)}


async def race(seconds: float) -> dict:
    """Coinbase's ticker and Binance's book ticker side by side for `seconds`,
    each through the sniper's own rule (the frozen spec's k and window) on
    this box's receipt clock - the clock the bot decides on. Each feed's
    signals become episodes (one per move and side, 300ms cooldown) and the
    two are paired within 1s."""
    import websockets
    import exec_latency
    import sniper_mode as S
    spec = {a: v for a, _n, v, _l, _f in exec_latency.FROZEN_SPEC}
    k, window, cooldown = float(spec["k_bp"]), float(spec["window_ms"]), float(spec["cooldown_ms"])
    trig = {"coinbase": S.Trigger(k, window), "binance": S.Trigger(k, window)}
    sig = {"coinbase": [], "binance": []}
    ticks = collections.Counter()
    end = time.perf_counter() + seconds

    def feed(name, t_ms, mid):
        ticks[name] += 1
        d, _r = trig[name].update(t_ms, mid)
        if d is not None:
            sig[name].append((t_ms, d))

    async def pump(name, url, sub, parse):
        async with websockets.connect(url, open_timeout=10, compression=None) as ws:
            if sub is not None:
                await ws.send(json.dumps(sub))
            while time.perf_counter() < end:
                try:
                    raw = await asyncio.wait_for(ws.recv(), max(0.05, end - time.perf_counter()))
                except asyncio.TimeoutError:
                    break
                t_ms = time.monotonic() * 1000.0
                mid = parse(json.loads(raw))
                if mid is not None:
                    feed(name, t_ms, mid)

    def binance_mid(d):
        try:
            return (float(d["b"]) + float(d["a"])) / 2.0
        except (KeyError, TypeError, ValueError):
            return None

    def coinbase_mid(d):
        return S.coinbase_mid(d) if d.get("type") == "ticker" else None

    await asyncio.gather(pump("coinbase", S.COINBASE_WS_URL, S.COINBASE_SUBSCRIBE, coinbase_mid),
                         pump("binance", BINANCE_BOOK_WS, None, binance_mid))
    cb, bn = episodes(sig["coinbase"], cooldown), episodes(sig["binance"], cooldown)
    m = match_episodes(cb, bn)
    return {"seconds": seconds, "k_bp": k, "window_ms": window, "ticks": dict(ticks),
            "signals": {n: len(v) for n, v in sig.items()}, "episodes": {"coinbase": len(cb), "binance": len(bn)},
            "matched": m["matched"], "coinbase_first": m["a_first"], "binance_first": m["b_first"], "tie": m["tie"],
            "only_coinbase": m["only_a"], "only_binance": m["only_b"],
            "coinbase_lead_ms": m["a_lead_ms"], "binance_lead_ms": m["b_lead_ms"]}


def budget(res: dict, hold_ms: float = VENUE_HOLD_MS, server_ms: float = 25.0, in_process_ms: float = 0.4) -> dict | None:
    """BTC match on Coinbase -> our order matched at Polymarket, from the
    measured parts (p50s). None when a part is missing."""
    feed = ((res.get("coinbase") or {}).get("match_to_receipt_ms") or {}).get("p50")
    rtt = (((res.get("clob_warm") or {}).get("warm_rtt_ms")) or {}).get("p50")
    if feed is None or rtt is None:
        return None
    offset = (res.get("coinbase_clock") or {}).get("offset_ms")
    feed_true = feed + offset if offset is not None else feed
    one_way = rtt / 2.0
    return {"feed_ms": feed_true, "feed_raw_ms": feed, "clock_offset_ms": offset, "in_process_ms": in_process_ms,
            "one_way_ms": one_way, "hold_ms": hold_ms, "server_ms": server_ms,
            "signal_to_match_ms": in_process_ms + one_way + hold_ms + server_ms,
            "btc_to_match_ms": feed_true + in_process_ms + one_way + hold_ms + server_ms}


async def main_async(n: int, feed_s: float = 60.0, race_s: float = 120.0) -> dict:
    res = {"when": time.strftime("%Y-%m-%d %H:%M:%S %z"), "host": socket.gethostname()}
    progress("handshakes")
    for name, host in (("clob", "clob.polymarket.com"), ("poly_ws", "ws-subscriptions-clob.polymarket.com"),
                       ("binance", "stream.binance.com"), ("coinbase", "ws-feed.exchange.coinbase.com")):
        try:
            res[f"{name}_handshake"] = await asyncio.to_thread(
                host_steps, host, 9443 if name == "binance" else 443, ("h2",) if name == "clob" else ("http/1.1",))
        except Exception as exc:  # noqa: BLE001 - reported, the rest still runs
            res[f"{name}_handshake"] = {"error": f"{type(exc).__name__}: {exc}"}
    for name, fn in (("clob_warm", lambda: warm_gets(n)), ("clob_cold", lambda: cold_gets(max(5, n // 5))),
                     ("poly_ws", lambda: poly_ws(min(n, 50))),
                     ("coinbase_clock", lambda: asyncio.to_thread(coinbase_clock, 10)),
                     ("coinbase", lambda: coinbase_feed(feed_s)), ("binance", lambda: binance(n))):
        progress(name + (f" ({feed_s:g}s)" if name == "coinbase" else ""))
        try:
            res[name] = await fn()
        except Exception as exc:  # noqa: BLE001
            res[name] = {"error": f"{type(exc).__name__}: {exc}"}
    if race_s > 0:
        progress(f"race: Coinbase vs Binance through the sniper's rule ({race_s:g}s)")
        try:
            res["race"] = await race(race_s)
        except Exception as exc:  # noqa: BLE001
            res["race"] = {"error": f"{type(exc).__name__}: {exc}"}
    res["budget"] = budget(res)
    return res


def table(res: dict) -> str:
    f = lambda v: "--" if v is None else f"{v:.1f}"  # noqa: E731
    out = [f"network from {res['host']} at {res['when']}"]
    for name in ("clob", "poly_ws", "binance", "coinbase"):
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
    out.append("coinbase (the trigger feed)")
    k = res.get("coinbase_clock") or {}
    if "error" in k:
        out.append(f"  clock: {k['error']}")
    elif k:
        out.append(f"  clock offset to Coinbase {k['offset_ms']:+.1f}ms (+/- {k['offset_uncertainty_ms']:.1f}ms); "
                   f"GET /time rtt p50 {f((k.get('rtt_ms') or {}).get('p50'))}ms")
    c = res.get("coinbase") or {}
    if "error" in c:
        out.append(f"  coinbase: {c['error']}")
    elif c:
        line("Coinbase match -> receipt (this clock)", c.get("match_to_receipt_ms"))
        line("  jitter above the minimum", c.get("jitter_above_min_ms"))
        line("Coinbase gap between tickers", c.get("gap_ms"))
        out.append(f"  {c['tickers']} tickers in {c['seconds']:g}s ({c['per_s']:.1f}/s), {c['heartbeats']} heartbeats, "
                   f"{c['gaps_over_2s']} gaps over 2s (each restarts the rule); connect {f(c.get('connect_ms'))}ms")
    r = res.get("race")
    if r and "error" in r:
        out.append(f"race: {r['error']}")
    elif r:
        out.append(f"race ({r['seconds']:g}s, {r['k_bp']:g}bp / {r['window_ms']:g}ms on each feed, this box's clock)")
        out.append(f"  ticks: coinbase {r['ticks'].get('coinbase', 0)}, binance {r['ticks'].get('binance', 0)}; "
                   f"moves (episodes): coinbase {r['episodes']['coinbase']}, binance {r['episodes']['binance']}")
        out.append(f"  same move on both: {r['matched']} - Coinbase first {r['coinbase_first']}, Binance first "
                   f"{r['binance_first']}, tie {r['tie']}; only Coinbase {r['only_coinbase']}, only Binance {r['only_binance']}")
        line("  Coinbase ahead by", r.get("coinbase_lead_ms"))
        line("  Binance ahead by", r.get("binance_lead_ms"))
    bu = res.get("budget")
    if bu:
        out.append("latency budget (p50s): BTC match on Coinbase -> our order matched at Polymarket")
        out.append(f"  feed {f(bu['feed_ms'])}ms (raw {f(bu['feed_raw_ms'])} + clock offset {f(bu['clock_offset_ms'])})"
                   f" + in-process {bu['in_process_ms']:g}ms + one-way to CLOB {f(bu['one_way_ms'])}ms"
                   f" + venue hold {bu['hold_ms']:g}ms + server {bu['server_ms']:g}ms")
        out.append(f"  = {f(bu['btc_to_match_ms'])}ms from the BTC trade; {f(bu['signal_to_match_ms'])}ms from our signal "
                   f"(paper's default SNIPER_HOLD_MS is 250: {f(bu['signal_to_match_ms'] - bu['hold_ms'] + 250)}ms)")
    else:
        out.append("latency budget: needs both the Coinbase feed and the CLOB warm round trip")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--feed-s", type=float, default=60.0, help="seconds of the Coinbase feed to measure")
    ap.add_argument("--race-s", type=float, default=120.0, help="seconds of the Coinbase vs Binance race (0 = skip)")
    ap.add_argument("--json", default=None)
    args = ap.parse_args(argv)
    res = asyncio.run(main_async(args.n, args.feed_s, args.race_s))
    print(table(res))
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(res, indent=1, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
