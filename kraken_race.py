"""Kraken vs Binance, live: which one tells THIS machine first that BTC moved?

    .venv/bin/python kraken_race.py                  # 10 minutes, the bot's rule (1bp within 250ms)
    .venv/bin/python kraken_race.py --seconds 1800 --k-bp 0.7

Read-only: two public sockets, no keys, no orders. Safe beside a running bot.

Both feeds run the sniper's own rule (sniper_mode.Trigger: mid moved >= k bp
against the last price at or before the lookback, 1s cooldown; Kraken's
heartbeats carry its price forward exactly as the bot does). Each "fire" is
stamped on this machine's monotonic clock the moment its message is read, so
the race needs no exchange clock. Fires in the same direction within
--pair-ms of each other are the same BTC move:

  lead     Kraken fire time minus Binance fire time on the same move (ms);
           negative = Kraken told us first
  only     moves only one feed fired on (a move the other never showed as
           >= k bp in time, or that feed's own noise)

Also: update rates, quiet gaps, and Kraken's arrival lag on price-changing
updates (receipt wall minus Kraken's timestamp - rough: needs this clock NTP
synced, and Kraken's timestamp is not always the moment the bbo changed).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import websockets  # noqa: E402

import sniper_mode as S  # noqa: E402

BINANCE = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"


class Feed:
    def __init__(self, name, k_bp, window_ms, cooldown_ms):
        self.name = name
        self.trigger = S.Trigger(k_bp, window_ms)
        self.cooldown_ms = cooldown_ms
        self.last_fire = -1e18
        self.fires = []                # (mono ms, direction, bp)
        self.updates = self.heartbeats = 0
        self.prev_t = None
        self.gaps = []
        self.lag = []
        self.prev_px = None
        self.connect_ms = None

    def tick(self, t, mid):
        self.updates += 1
        if self.prev_t is not None:
            self.gaps.append(t - self.prev_t)
        self.prev_t = t
        d, r = self.trigger.update(t, mid)
        if d is not None and t - self.last_fire >= self.cooldown_ms:
            self.last_fire = t
            self.fires.append((t, d, r))


async def run_binance(f: Feed, stop):
    t0 = time.perf_counter()
    async with websockets.connect(BINANCE, open_timeout=10, ping_interval=20, ping_timeout=20) as ws:
        f.connect_ms = (time.perf_counter() - t0) * 1000.0
        while time.monotonic() < stop:
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
            t = time.monotonic() * 1000.0
            d = json.loads(raw)
            f.tick(t, (float(d["b"]) + float(d["a"])) / 2.0)


async def run_kraken(f: Feed, stop):
    t0 = time.perf_counter()
    async with websockets.connect(S.KRAKEN_TICKER_URL, open_timeout=10, ping_interval=None,
                                  compression=None) as ws:
        f.connect_ms = (time.perf_counter() - t0) * 1000.0
        await ws.send(json.dumps(S.KRAKEN_SUBSCRIBE))
        while time.monotonic() < stop:
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            t = time.monotonic() * 1000.0
            wall = time.time() * 1000.0
            d = json.loads(raw)
            if d.get("channel") == "heartbeat":
                f.heartbeats += 1
                f.trigger.hold(t)
                continue
            if d.get("channel") != "ticker":
                continue
            for row in d.get("data") or ():
                mid = S.kraken_mid(row)
                if mid is None:
                    continue
                px = (row.get("bid"), row.get("ask"))
                ex = S._exchange_ms(row.get("timestamp"))
                if ex is not None and f.prev_px is not None and px != f.prev_px:
                    f.lag.append(wall - ex)              # only price-changing updates
                f.prev_px = px
                f.tick(t, mid)


def q(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else float("nan")


def report(b: Feed, k: Feed, seconds, pair_ms) -> None:
    print(f"\nKRAKEN vs BINANCE - {seconds / 60:.0f} min, rule {b.trigger.k_bp:g}bp within "
          f"{b.trigger.window_ms:g}ms, 1s cooldown, same move = same direction within {pair_ms:g}ms")
    for f in (b, k):
        print(f"  {f.name:8s} connect {f.connect_ms or float('nan'):6.0f}ms | {f.updates / seconds:5.1f} updates/s"
              + (f", {f.heartbeats} heartbeats" if f.heartbeats else "")
              + f" | quiet gap p50 {q(f.gaps, .5):.0f}ms p99 {q(f.gaps, .99):.0f}ms max {max(f.gaps or [0]):.0f}ms"
              + f" | fires {len(f.fires)}")
    if k.lag:
        print(f"  Kraken arrival lag on price changes (receipt - Kraken timestamp): p10 {q(k.lag, .1):.0f}ms "
              f"p50 {q(k.lag, .5):.0f}ms p90 {q(k.lag, .9):.0f}ms (n={len(k.lag)})")
    used, pairs = set(), []
    for tb, db, rb in b.fires:
        best = None
        for i, (tk, dk, rk) in enumerate(k.fires):
            if i in used or dk != db or abs(tk - tb) > pair_ms:
                continue
            if best is None or abs(tk - tb) < abs(k.fires[best][0] - tb):
                best = i
        if best is not None:
            used.add(best)
            pairs.append(k.fires[best][0] - tb)
    only_b = len(b.fires) - len(pairs)
    only_k = len(k.fires) - len(pairs)
    print(f"\n  same moves (both fired): {len(pairs)} | Binance only: {only_b} | Kraken only: {only_k}")
    if pairs:
        first = sum(1 for x in pairs if x < 0) / len(pairs)
        print(f"  lead = Kraken minus Binance (ms; negative = Kraken first): p10 {q(pairs, .1):+.0f} "
              f"p50 {statistics.median(pairs):+.0f} p90 {q(pairs, .9):+.0f} | Kraken first on {first:.0%} of moves")
    print("\n  read it: a Kraken trigger only helps if Kraken is first on most SAME moves by tens of ms, without many")
    print("  Kraken-only fires (moves the other exchanges and Polymarket may never make). Few fires = quiet market:")
    print("  run longer (--seconds 1800) or in a busier hour.")


async def main_async(args):
    b = Feed("Binance", args.k_bp, args.window_ms, 1000.0)
    k = Feed("Kraken", args.k_bp, args.window_ms, 1000.0)
    stop = time.monotonic() + args.seconds
    res = await asyncio.gather(run_binance(b, stop), run_kraken(k, stop), return_exceptions=True)
    for name, r in zip(("Binance", "Kraken"), res):
        if isinstance(r, Exception):
            print(f"  {name} socket ended early: {type(r).__name__}: {r}")
    report(b, k, args.seconds, args.pair_ms)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seconds", type=float, default=600.0)
    ap.add_argument("--k-bp", type=float, default=1.0)
    ap.add_argument("--window-ms", type=float, default=250.0)
    ap.add_argument("--pair-ms", type=float, default=1000.0)
    args = ap.parse_args()
    print(f"racing Kraken BTC/USD against Binance BTCUSDT for {args.seconds:g}s (read-only)...", flush=True)
    asyncio.run(main_async(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
