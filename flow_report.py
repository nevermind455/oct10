#!/usr/bin/env python3
"""Where the cheap asks went after our signals: bought by takers or cancelled.
Read-only, one screen. Needs book_trace rows with a flow (code since 2026-10-17).

    python flow_report.py                                  # the paper bot's journal
    python flow_report.py live_sniper.jsonl --hours 6      # live, last 6 hours

For each traced signal, between consecutive book snapshots (0, 50, ... ms after
the BTC tick, then the venue's reply): the depth at or under our price cap that
disappeared, the part public BUY prints account for (taken by other takers),
and the rest - AT LEAST that much was cancelled (new asks in between only hide
cancels). Mostly cancels: the sellers pull their asks when BTC moves, and no
taker order can beat that through the venue's taker delay. Mostly trades:
faster takers took them, and a faster signal could win some of those.
"""
from __future__ import annotations

import argparse
import collections
import json
import time


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("journal", nargs="?", default="paper_orders_sniper.jsonl")
    ap.add_argument("--hours", type=float, default=None)
    args = ap.parse_args()
    since = time.time() - args.hours * 3600 if args.hours else 0.0
    rows = []
    with open(args.journal, encoding="utf-8") as fh:
        for line in fh:
            if '"book_trace"' not in line or '"flow"' not in line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("result") == "book_trace" and r.get("flow") is not None and r.get("wall", 0) >= since:
                rows.append(r)
    if not rows:
        print("no book_trace rows with a flow yet (needs code from 2026-10-17 and some orders)")
        return 1
    drop = sum(r["flow_drop_sh"] for r in rows)
    traded = sum(r["flow_traded_sh"] for r in rows)
    cancel = sum(r["flow_cancel_min_sh"] for r in rows)
    unknown = sum(r.get("flow_unknown_side_sh", 0.0) for r in rows)
    print(f"{len(rows)} traced signals ({args.journal})")
    print(f"depth under our cap that disappeared: {drop:,.0f} shares")
    if drop:
        print(f"  bought by other takers (BUY prints): {traded:,.0f}  ({100 * traded / drop:.0f}%)")
        print(f"  cancelled, at least:                 {cancel:,.0f}  ({100 * cancel / drop:.0f}%)")
        if unknown:
            print(f"  prints without a side:               {unknown:,.0f}  ({100 * unknown / drop:.0f}%)")
    steps = collections.OrderedDict()
    for r in rows:
        for st in r["flow"]:
            k = f"{st['from']}->{st['to']}"
            d = steps.setdefault(k, [0.0, 0.0, 0.0])
            d[0] += max(0.0, st["depth_from"] - st["depth_to"])
            d[1] += st["traded_sh"]
            d[2] += st["cancel_min_sh"]
    print("\nby step (ms after the BTC tick):   gone     traded   cancelled>=")
    for k, (g, t, c) in steps.items():
        print(f"  {k:24s} {g:9,.0f} {t:9,.0f} {c:11,.0f}")
    by = collections.defaultdict(lambda: [0, 0.0, 0.0, 0.0])
    for r in rows:
        k = r.get("kind") or "?"
        b = by[k]
        b[0] += 1
        b[1] += r["flow_drop_sh"]
        b[2] += r["flow_traded_sh"]
        b[3] += r["flow_cancel_min_sh"]
    print("\nby outcome:          signals      gone    traded  cancelled>=")
    for k, (n, g, t, c) in sorted(by.items()):
        print(f"  {k:18s} {n:8d} {g:9,.0f} {t:9,.0f} {c:11,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
