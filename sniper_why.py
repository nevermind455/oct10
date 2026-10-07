"""Why is the sniper not trading?  Read-only, one screen.

    .venv/bin/python sniper_why.py            # the paper bot (default)
    .venv/bin/python sniper_why.py --live     # the live canary's journal

From the bot's own journal since its last start: is it running, did BTC
give it any signal, which check stopped each signal, and what happened to
the orders it sent. The counters come from the bot's 5-minute summary rows
(exact - journal skip rows are written at most once a second per reason).
"""
from __future__ import annotations

import argparse
import collections
import time
from pathlib import Path

import sniper_check as sc

EXPLAIN = {
    "no current round": "the bot has no current 5-minute market (market discovery failing or between rounds)",
    "before trade window": "the market is open but TRADE_LAST_SECONDS has not been reached yet",
    "too late in round": "less than MIN_SECONDS_TO_EXPIRY / SNIPER_MIN_LEFT_S left in the market",
    "book not live": "Polymarket's order book feed is disconnected or resyncing - no trustworthy price",
    "book stale": "Polymarket's book has not updated within SNIPER_STALE_MS",
    "no asks on this side": "nothing offered on the side BTC moved toward (a market all but decided)",
    "ask outside price band": "the ask is outside MIN_BUY_PRICE..MAX_BUY_PRICE",
    "round cap": "SNIPER_MAX_FILLS_ROUND / SNIPER_MAX_SIDE_SHARES reached in this market",
    "no asks left after our own recent fills": "our own recent fills used up the visible asks",
    "cooldown": "an order on the same side was submitted within SNIPER_COOLDOWN_MS",
    "previous order in flight": "the previous order was still waiting for its match",
    "awaiting FAK fill size": "a live FAK order's fill size has not arrived yet",
    "live halted": "the live canary is HALTED (kill switch) - restart needed",
    "live refused": "the live canary refused the order (see the detail)",
    "binance feed behind": "Binance's trade timestamps were more than SNIPER_BINANCE_STALE_MS behind their 5-min "
                           "best - a stalled feed; the move may already be priced",
    "binance receive backlog": "more than SNIPER_BINANCE_BACKLOG_MAX Binance messages were queued (the bot fell "
                               "behind): a burst of old ticks looks like a fast move",
    "binance age unknown": "no Binance trade timestamp within SNIPER_BINANCE_UNKNOWN_MS, so tick age could not be shown",
    "binance disconnected": "the Binance socket was down",
}


def ago(t: float) -> str:
    s = time.time() - t
    return f"{s / 60:.0f} min ago" if s < 7200 else f"{s / 3600:.1f} h ago"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default=str(sc.ROOT / ".env"))
    ap.add_argument("--live", action="store_true", help="the live canary's journal instead of paper's")
    args = ap.parse_args()
    env = sc.read_env(Path(args.env))
    if args.live:
        path = sc.state_path(env, "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl")
    else:
        audit = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
        path = audit.with_name(f"{audit.stem}_sniper.jsonl")
    rows = sc.jsonl(path)
    print(f"journal: {path}  ({len(rows)} rows)")
    if not rows:
        print("VERDICT: the journal is empty - the bot has never run with this journal (or writes elsewhere).")
        return 1
    starts = [i for i, r in enumerate(rows) if r.get("result") == "start"]
    if not starts:
        print("VERDICT: no start row - this journal was not written by the current sniper.")
        return 1
    st = rows[starts[-1]]
    since = rows[starts[-1]:]
    last_wall = max(float(r.get("wall") or 0) for r in since)
    up_min = (last_wall - float(st["wall"])) / 60
    print(f"last start : {time.strftime('%m-%d %H:%M:%S', time.gmtime(float(st['wall'])))} UTC ({ago(float(st['wall']))}),"
          f" code {st.get('code')}")
    print(f"settings   : {st.get('settings')}")
    print(f"last row   : {ago(last_wall)}  (the bot writes a summary every 5 min and a gc row every market)")
    alive = time.time() - last_wall < 420
    if not alive:
        print(f"\nVERDICT: the bot is NOT RUNNING - nothing written for {ago(last_wall)}. Start it again "
              f"(tmux ls; then the start command), and if it exits, read the error in its tmux window.")
        return 1

    sums = [r for r in since if r.get("result") == "summary"]
    s = sums[-1] if sums else {}
    signals, orders = int(s.get("signals") or 0), int(s.get("orders") or 0)
    filled, refused, errors = int(s.get("filled") or 0), int(s.get("refused") or 0), int(s.get("errors") or 0)
    skips = s.get("skips") or {}
    if not sums:
        print("\n(no 5-minute summary yet - wait until the bot has run 5 minutes for exact counters; "
              "showing journal rows only)")
    else:
        print(f"\nsince start ({up_min:.0f} min, summary {ago(float(s['wall']))}): BTC signals {signals}, orders {orders}, "
              f"filled {filled}, refused {refused}, errors {errors}")
    kq = s.get("kraken")
    if isinstance(kq, dict):
        lag = kq.get("lag_ms") or {}
        print(f"  Kraken feed: {kq.get('ticks')} updates, {kq.get('heartbeats')} heartbeats, "
              f"{kq.get('reconnects')} reconnects, {kq.get('malformed')} malformed; longest gap "
              f"{kq.get('gap_ms_max')}ms, {kq.get('gaps_over_max_gap')} longer than 2s (each resets the rule); "
              f"arrival lag p50 {lag.get('p50')}ms p99 {lag.get('p99')}ms; system {kq.get('system')}")
    if skips:
        print("  signals stopped by:")
        for why, n in sorted(skips.items(), key=lambda kv: -kv[1]):
            base = why.split(" (")[0]
            print(f"    {n:6d}  {why:40s} {EXPLAIN.get(base, '')}")
    rows_by = collections.Counter((r.get("result"), r.get("reason")) for r in since
                                  if r.get("result") in ("skipped", "refused", "error"))
    if rows_by and not skips:
        print("  journal rows (skips throttled to 1/s per reason):")
        for (res, why), n in rows_by.most_common(12):
            print(f"    {n:6d}  {res:8s} {str(why)[:90]}")
    halted = [r for r in since if r.get("result") == "skipped" and r.get("reason") == "live halted" and r.get("detail")]
    for why in ("live halted", "live refused"):
        rr = [r for r in since if r.get("result") == "skipped" and r.get("reason") == why and r.get("detail")]
        if rr:
            print(f"  {why} - latest reason: {str(rr[-1]['detail'])[:220]}")
    band = [r for r in since if r.get("result") == "skipped" and r.get("reason") == "ask outside price band"]
    if band:
        try:
            from dotenv import dotenv_values
            ev = dotenv_values(args.env) or {}
        except Exception:  # noqa: BLE001 - no python-dotenv: show the asks only
            ev = {}
        lo, hi = ev.get("MIN_BUY_PRICE", "?"), ev.get("MAX_BUY_PRICE", "?")
        asks = ", ".join(f"{float(r['ask_seen']):.2f} ({r.get('side')}, {float(r.get('left_s') or 0):.0f}s left)"
                         for r in band[-6:] if r.get("ask_seen") is not None)
        print(f"  price band MIN_BUY_PRICE={lo} MAX_BUY_PRICE={hi}; latest asks outside it: {asks}")
    bnl = collections.Counter(r.get("book_status") for r in since
                              if r.get("result") == "skipped" and r.get("reason") == "book not live")
    if bnl:
        early = sum(1 for r in since if r.get("result") == "skipped" and r.get("reason") == "book not live"
                    and float(r.get("left_s") or 0) > 290)
        print("  book not live, by feed state (journal rows): "
              + ", ".join(f"{k or 'not recorded'} {v}" for k, v in bnl.most_common())
              + f"; {early} in a market's first 10s")
    refused_why = collections.Counter(str(r.get("reason"))[:90] for r in since if r.get("result") == "refused")
    if refused_why:
        print("  orders not filled, by reason:")
        for why, n in refused_why.most_common(6):
            print(f"    {n:6d}  {why}")
    errs = [r for r in since if r.get("result") == "error"]
    for r in errs[-3:]:
        print(f"  ERROR {ago(float(r.get('wall') or 0))}: {str(r.get('reason'))[:160]}")

    print("\nVERDICT:")
    top = max(skips.items(), key=lambda kv: kv[1])[0] if skips else None
    if halted:
        print(f"  the LIVE CANARY IS HALTED - no orders until it is restarted: {str(halted[-1]['detail'])[:200]}")
    elif errors or errs:
        print(f"  orders are FAILING with errors ({errors or len(errs)}) - see the ERROR lines above; paste them.")
    elif sums and signals == 0:
        print(f"  no BTC signal in {up_min:.0f} min: BTC never moved >= the trigger (SNIPER_K_BP) within the lookback.")
        print("  Quiet market, or the Binance feed is down (dashboard: 'Binance book ticker connected'?).")
    elif sums and orders == 0 and top:
        print(f"  BTC gave {signals} signal(s) but every one was stopped - mostly by '{top}': "
              f"{EXPLAIN.get(top.split(' (')[0], '')}.")
    elif orders and not filled:
        print(f"  {orders} order(s) sent, none filled - the price was gone past the limit (see reasons above).")
    elif filled:
        print(f"  it IS trading: {filled} fill(s) of {orders} order(s) since the start.")
    else:
        print("  too early to tell - run again after the first 5-minute summary.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
