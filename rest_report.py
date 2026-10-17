#!/usr/bin/env python3
"""Would leaving a missed order resting at its limit have paid? Read-only.
Needs rest_shadow rows (code since 2026-10-17) and the settlement log.

    python rest_report.py                                   # paper journal + paper settlements
    python rest_report.py live_sniper.jsonl live_ledger_settlements.jsonl --hours 12

For every fully missed order, a bid for the same shares resting at its limit
from the instant it would have matched: filled only by real SELL prints at or
under the limit, after the shares already bid at that price (conservative -
it ignores bids ahead that cancel). Each fill is a maker fill: bought at the
limit, no taker fee, settled with the round's real winner. By resting time and
by BTC move. A shadow estimate, not a promise of live fills.
"""
from __future__ import annotations

import argparse
import collections
import json
import time


def winners_from(path: str) -> dict:
    out = {}
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                if '"RESULT_RECEIVED"' in line:
                    r = json.loads(line)
                    if r.get("winner"):
                        out[r["condition_id"]] = r["winner"]
    except FileNotFoundError:
        pass
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("journal", nargs="?", default="paper_orders_sniper.jsonl")
    ap.add_argument("settlements", nargs="?", default="paper_ledger_settlements.jsonl")
    ap.add_argument("--hours", type=float, default=None)
    args = ap.parse_args()
    since = time.time() - args.hours * 3600 if args.hours else 0.0
    win = winners_from(args.settlements)
    rows = []
    with open(args.journal, encoding="utf-8") as fh:
        for line in fh:
            if '"rest_shadow"' in line:
                r = json.loads(line)
                if r.get("result") == "rest_shadow" and r.get("wall", 0) >= since:
                    rows.append(r)
    if not rows:
        print("no rest_shadow rows yet (needs code from 2026-10-17 and missed orders)")
        return 1
    settled = [r for r in rows if r.get("condition_id") in win]
    print(f"{len(rows)} missed orders shadowed, {len(settled)} with the round's winner known ({args.journal})")
    horizons = list(dict.fromkeys(h for r in rows for h in (r.get("horizons") or {})))
    for label, keep in (("all moves", lambda r: True), ("move < 1bp", lambda r: abs(float(r.get("bp") or 0)) < 1),
                        ("move >= 1bp", lambda r: abs(float(r.get("bp") or 0)) >= 1)):
        group = [r for r in settled if keep(r)]
        if not group:
            continue
        print(f"\n{label}: {len(group)} missed orders")
        print("  rest until   filled orders   shares    cost $   payout $     P&L $   won/filled")
        for h in horizons:
            n = sh = cost = pay = 0.0
            won = 0
            for r in group:
                x = (r.get("horizons") or {}).get(h) or {}
                f = float(x.get("filled_sh") or 0)
                if f <= 0:
                    continue
                n += 1
                sh += f
                cost += f * float(r["cap"])
                hit = win[r["condition_id"]] == r.get("side")
                pay += f if hit else 0.0
                won += int(hit)
            print(f"  {h + ('s' if h != 'end' else ''):10s} {int(n):8d}/{len(group):<5d} {sh:8.1f} {cost:9.2f} {pay:10.2f} "
                  f"{pay - cost:+9.2f}   {won}/{int(n)}")
    q = sorted(float(r.get("queue_ahead_sh") or 0) for r in rows)
    print(f"\nqueue already at our price when resting started: median {q[len(q) // 2]:.0f} shares; "
          f"bids above our price: median {sorted(float(r.get('better_bids_sh') or 0) for r in rows)[len(rows) // 2]:.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
