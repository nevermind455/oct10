#!/usr/bin/env python3
"""What the event loop (and the order path's worker) pays as the ledger grows.

    python bench_loop_costs.py            # ledgers of 100 .. 20,000 positions

The ledger keeps every position the bot ever held (two per 5-minute market
traded, ~576 a day). Measured on each size, with the bot's real objects:

  loop  DashView accounting refresh   (run.DashView._accounting_nowait, 1/s)
  loop  Position Manager state capture (portfolio + cash, every evaluation)
  loop  Sniper decision on a token with a recent own fill (_visible_ask)
  wkr   paper fill booking            (Ledger.record_fill_durable: rewrite + fsync)
  wkr   settlement ledger scan        (SettlementWorker._ledger_scan)

No network. Paper only. Prints a table; --json writes it.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import statistics
import sys
import tempfile
import time
import tracemalloc
import types
from decimal import Decimal as D

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - never reads the bot's .env
import position_manager as PM  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from accounting import settlement as ST  # noqa: E402
from paper_trade import MarketRules, PaperBroker  # noqa: E402


def build(n_positions: int, led: Ledger):
    """Fill `led` with n positions: every market settled but the last (current) one."""
    for i in range(n_positions // 2):
        cid = "0x" + f"{i + 1:064x}"
        for k in (1, 2):
            tok = f"{i}{k}"
            led.record_fill(f"t{i}-{k}", tok, shares=10.0, price=0.47, condition_id=cid, fee=0.02)
        if i < n_positions // 2 - 1:                                  # every market but the current one settled
            led.positions[f"{i}1"].settled = led.positions[f"{i}2"].settled = True
            led.positions[f"{i}1"].payout_per_share, led.positions[f"{i}2"].payout_per_share = 1.0, 0.0
            led.positions[f"{i}1"].realized, led.positions[f"{i}2"].realized = 5.28, -4.72
            led.positions[f"{i}1"].settled_wall = led.positions[f"{i}2"].settled_wall = time.time()
    led.save()
    last = n_positions // 2 - 1
    return "0x" + f"{last + 1:064x}", f"{last}1", f"{last}2"


def timed(fn, n=50):
    xs = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        xs.append((time.perf_counter() - t) * 1000.0)
    xs.sort()
    return {"p50": round(xs[len(xs) // 2], 3), "p99": round(xs[min(len(xs) - 1, int(len(xs) * 0.99))], 3),
            "max": round(xs[-1], 3)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="100,1000,5000,20000")
    ap.add_argument("--json", default=None)
    args = ap.parse_args(argv)
    import run
    from tests_sniper_live import live_book
    out = {}
    for n in [int(x) for x in args.sizes.split(",")]:
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            led = Ledger(path=str(tmp / "l.json"))
            last = n // 2 - 1
            cid, up, dn = "0x" + f"{last + 1:064x}", f"{last}1", f"{last}2"
            hub = types.SimpleNamespace(up_token=up, down_token=dn, condition_id=cid, window_end=time.time() + 200,
                                        window_start=time.time() - 100, book=live_book())
            rules = MarketRules(cid, D("0.07"), 1, D("5"), D("0.01"), up_token_id=up, down_token_id=dn)
            broker = PaperBroker(led, market_context=lambda: {"condition_id": cid, "up_token_id": up,
                                                              "down_token_id": dn},
                                 host="https://public.invalid", max_buy_price=0.95, start_balance=1e9, account_path=tmp / "a.json",
                                 audit_path=tmp / "o.jsonl", rules_fetch=lambda c: rules, own_fill_ttl_s=2.0)
            broker._rules(cid)
            build(n, led)                                  # the account exists before the fills, as in the bot
            rt = types.SimpleNamespace(ledger=led, broker=broker, hub=hub, live_exec=None, started=time.time())
            view = run.DashView.__new__(run.DashView)
            view.rt = rt
            pm = PM.PositionManager(PM.PMConfig(enabled=True), hub=hub, ledger=led, broker=broker)
            seq = [0]

            def book_one():
                seq[0] += 1
                led.record_fill_durable(f"b{seq[0]}", up, shares=1.0, price=0.47, condition_id=cid, fee=0.0)
            w = ST.SettlementWorker(led, fetch=lambda c: None)
            off = run.DashView.__new__(run.DashView)
            off.__dict__.update(rt=rt, off_loop=True, _acct_future=None, _acct={}, _slow={}, _slow_at=-1e9,
                                _positions={}, every=0.0)

            async def refresh_on_loop():
                """What the bot's refresh holds the loop for now: start the worker job."""
                xs = []
                for _ in range(50):
                    off._slow_at = -1e9
                    t = time.perf_counter()
                    off._refresh_slow()
                    xs.append((time.perf_counter() - t) * 1000.0)
                    while off._acct_future is not None and not off._acct_future.done():
                        await asyncio.sleep(0.001)
                    off._collect_accounting()
                xs.sort()
                return {"p50": round(xs[25], 3), "p99": round(xs[-1], 3), "max": round(xs[-1], 3)}

            def capture_miss():
                pm._cash_cache = None
                return pm.portfolio(cid, up, dn), pm._cash_free()
            row = {
                "accounting pass itself (now on a worker)": timed(lambda: view._accounting_nowait()),
                "loop: dashboard refresh (off-loop accounting)": asyncio.run(refresh_on_loop()),
                "loop: manager capture": timed(lambda: (pm.portfolio(cid, up, dn), pm._cash_free())),
                "loop: manager capture, cash recomputed": timed(capture_miss),
                "loop: paper cash_balance": timed(lambda: broker.cash_balance()),
                "worker: fill booking (fsync)": timed(book_one, n=20),
                "worker: settlement scan": timed(lambda: w._ledger_scan(frozenset({cid})), n=20),
                "ledger file KiB": round(os.path.getsize(led.path) / 1024, 1),
            }
            t = time.perf_counter()
            again = Ledger(path=led.path)
            again.load()
            row["restart: Ledger.load"] = {"p50": round((time.perf_counter() - t) * 1000.0, 3)}
            tracemalloc.start()
            snap0 = tracemalloc.take_snapshot()
            probe = Ledger(path=led.path)
            probe.load()
            grown = sum(x.size_diff for x in tracemalloc.take_snapshot().compare_to(snap0, "filename"))
            tracemalloc.stop()
            row["ledger in memory MiB"] = round(grown / 2 ** 20, 1)
            del probe, again
            pm.journal.close()
            out[n] = row
            print(f"{n:>6} positions  " + "  ".join(
                f"{k} p50 {v['p50']:.3f}ms" if isinstance(v, dict) else f"{k} {v}" for k, v in row.items()))
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
