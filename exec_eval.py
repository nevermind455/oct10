#!/usr/bin/env python3
"""Execution policies A/B/C/D on the SAME eligible signals. Read-only.

    .venv/bin/python exec_eval.py                          # the paper journal
    .venv/bin/python exec_eval.py --live                   # the live canary's journal + ledger
    .venv/bin/python exec_eval.py --since "2026-10-16 12:00"
    .venv/bin/python exec_eval.py --fetch-winners          # ask the chain / API for unknown round results

The bot's decision, signal and Kelly size are unchanged; only how the order
executes differs:

    A  FAK  cap min(ask + 5c, fair - fee - 1c)   the current bot
    B  FOK  the same cap                         all of it at acceptable prices, or nothing
    C  FAK  cap fair - fee - 1c                  no ask + 5c bound
    D  FOK  cap fair - fee - 1c

Each policy is priced on each submitted order's own venue-time book: the bot's
shadow at that order's delay (paper's modelled one, live's measured one). C/D
differ from A/B only where ask + 5c bound the cap; those orders need shadow rows
from a build that prices the fair-value cap ("fv", 2026-10-16). An older order
that would differ is left out of every policy, so all four see identical signals.

Two sizings, to keep execution apart from allocation:
  same dollars  every policy spends the order's own signed amount (order type and cap only)
  as sized      C/D spend whole shares x their own cap (the bot's share sizing at that cap)
Later orders are NOT resized (Kelly would size them a little differently after a
different fill): the comparison isolates execution.

Metric: settled P&L, fees included, per eligible signal (an order the bot sent),
plus fills, cost, P&L per dollar, 5s markout and the worst-case loss of a round.
Detail (same dollars): full vs partial fills, slippage against the ask the bot
saw, the model's edge at the actual fill price, UP/DOWN and latency bands,
added / lost / changed fills against A, max drawdown and peak open exposure
(each round settles at its end), and a 95% bootstrap range of the difference
against A (resampling whole rounds: orders in one round are not independent).
Signals rejected before submission are counted apart from submitted orders that
did not fill. PASS (vs A) needs a positive difference in EVERY UTC day and every
6-hour block with at least --min-n signals; fewer is INSUFFICIENT.
"""
from __future__ import annotations

import argparse
import calendar
import collections
import json
import os
import time

import guard_eval as G

POLICIES = ("A", "B", "C", "D")
LABEL = {"A": "FAK  min(ask+5c, fair limit)  (now)", "B": "FOK  min(ask+5c, fair limit)",
         "C": "FAK  fair limit", "D": "FOK  fair limit"}


def _fill(q, fok: bool):
    """(shares, price, fee, mk5, partial) of a shadow quote; FAK falls back to
    what it keeps (a partial fill: the whole order was not there)."""
    if q and q.get("sh"):
        return float(q["sh"]), float(q["px"]), float(q["fee"]), q.get("mk5"), False
    if fok:
        return None
    k = (q or {}).get("fak") or {}
    if k.get("sh"):
        return float(k["sh"]), float(k["px"]), float(k["fee"]), k.get("mk5"), True
    return None


BANDS = ((0, 250, "<250ms"), (250, 400, "250-400ms"), (400, 800, "400-800ms"), (800, 1e12, "800ms+"))


def _band(ms: float) -> str:
    return next(lbl for lo, hi, lbl in BANDS if lo <= ms < hi)


def drawdown_and_exposure(items, p: str) -> tuple[float, float]:
    """Max drawdown of settled P&L (each round settles at its end) and the peak
    cost of positions open at once (bought, round not yet over)."""
    by_end, events = collections.defaultdict(float), []
    for it in items:
        d, f = it["o"]["d"], it["fills"][p]
        end = float(d.get("round_end") or d.get("wall") or 0)
        by_end[end] += it["pnl"][p]
        if f:
            cost = f[0] * f[1] + f[2]
            events += [(float(d.get("wall") or end), cost), (end, -cost)]
    peak = cum = dd = 0.0
    for end in sorted(by_end):
        cum += by_end[end]
        peak = max(peak, cum)
        dd = max(dd, peak - cum)
    open_cost = top = 0.0
    for _t, c in sorted(events, key=lambda e: (e[0], e[1])):
        open_cost += c
        top = max(top, open_cost)
    return dd, top


def bootstrap(items, p: str, draws: int = 2000, seed: int = 20261016) -> tuple[float, float]:
    """95% range of (p - A) cents per signal, resampling whole rounds."""
    import random
    rounds = collections.defaultdict(list)
    for it in items:
        rounds[it["o"]["cond"]].append(it["pnl"][p] - it["pnl"]["A"])
    groups = list(rounds.values())
    if len(groups) < 2:
        return float("nan"), float("nan")
    rnd, out = random.Random(seed), []
    for _ in range(draws):
        pick = [groups[rnd.randrange(len(groups))] for _ in groups]
        n = sum(len(g) for g in pick)
        out.append(100 * sum(sum(g) for g in pick) / n)
    out.sort()
    return out[int(0.025 * draws)], out[int(0.975 * draws) - 1]


def details(items) -> None:
    n = len(items)
    print(f"  {'policy':6s} {'full':>5s} {'partial':>7s} {'slip c':>7s} {'edge@fill c':>11s} "
          f"{'max DD $':>9s} {'peak open $':>11s}")
    for p in POLICIES:
        f = [(i, i["fills"][p]) for i in items if i["fills"][p]]
        slip = [100 * (x[1] - float(i["o"]["d"]["ask_seen"])) for i, x in f if i["o"]["d"].get("ask_seen") is not None]
        edge = [100 * (float(i["o"]["d"]["fair"]) - x[1] - x[2] / x[0]) for i, x in f
                if i["o"]["d"].get("fair") is not None and x[0] > 0]
        dd, top = drawdown_and_exposure(items, p)
        avg = lambda xs: sum(xs) / len(xs) if xs else float("nan")  # noqa: E731
        num = lambda v, w: f"{v:+{w}.2f}" if v == v else f"{'-':>{w}s}"  # noqa: E731 - nan -> "-"
        print(f"  {p:6s} {sum(1 for _i, x in f if not x[4]):5d} {sum(1 for _i, x in f if x[4]):7d} "
              f"{num(avg(slip), 7)} {num(avg(edge), 11)} {dd:9.2f} {top:11.2f}")
    for title, key in (("by side", lambda i: i["o"]["d"]["side"]),
                       ("by latency band", lambda i: _band(G.delay_of(i["o"]["d"])))):
        groups = collections.defaultdict(list)
        for i in items:
            groups[key(i)].append(i)
        cells = []
        for k in sorted(groups, key=lambda k: [lbl for _lo, _hi, lbl in BANDS].index(k)
                        if k in [lbl for _lo, _hi, lbl in BANDS] else k):
            g = groups[k]
            cells.append(f"{k} n={len(g)} " + " ".join(
                f"{p} {100 * sum(i['pnl'][p] for i in g) / len(g):+.1f}c/{sum(1 for i in g if i['fills'][p]) / len(g):.0%}"
                for p in POLICIES))
        print(f"  {title} (P&L c/signal / fill%): " + " | ".join(cells))
    for p in ("B", "C", "D"):
        added = lost = changed = 0
        for i in items:
            a, x = i["fills"]["A"], i["fills"][p]
            if x and not a:
                added += 1
            elif a and not x:
                lost += 1
            elif a and x and (abs(a[0] - x[0]) > 1e-9 or abs(a[1] - x[1]) > 1e-9):
                changed += 1
        lo, hi = bootstrap(items, p)
        diff = 100 * sum(i["pnl"][p] - i["pnl"]["A"] for i in items) / n
        print(f"  {p} vs A: added {added}, lost {lost}, changed price/size {changed}; "
              f"{diff:+.2f}c per signal, 95% range [{lo:+.2f}, {hi:+.2f}]"
              + ("  (the range spans 0: not shown to differ)" if lo <= 0 <= hi else ""))


def miss_reason(text) -> str:
    t = str(text or "").lower()
    if "unknown book" in t:
        return "book unknown at the match"
    if "no asks" in t or "one-sided" in t:
        return "no asks at the match"
    if "only $" in t or "fully" in t:
        return "not enough size at the cap"
    if "nothing at or below" in t:
        return "price moved past the cap"
    if "spread" in t or "not live" in t:
        return "book not tradable"
    return t[:40] or "unknown"


def policies_for(o: dict, sizing: str):
    """{policy: fill or None} for one order, or None when C/D cannot be priced."""
    d, s = o["d"], o["shadow"]
    if not s:
        return None
    e = G.entry_at(s, G.delay_of(d))
    if not e or ("sh" not in e and "miss" not in e):
        return None
    out = {"A": _fill(e, False), "B": _fill(e, True)}
    fl, cap = d.get("fair_limit"), d.get("cap")
    differs = fl is not None and cap is not None and float(fl) > float(cap) + 1e-9
    if not differs:
        out["C"], out["D"] = out["A"], out["B"]          # the same cap: the same execution
        return out
    fv = e.get("fv") or {}
    q = fv.get("same" if sizing == "same" else "own") or (fv.get("same") if sizing != "same" else None)
    if not q:
        return None                                      # differs, but this build did not price it
    out["C"], out["D"] = _fill(q, False), _fill(q, True)
    return out


def worst_rounds(items, p: str) -> list[float]:
    by = collections.defaultdict(lambda: {"UP": [0.0, 0.0], "DOWN": [0.0, 0.0]})
    for it in items:
        f = it["fills"][p]
        if f:
            b = by[it["o"]["cond"]][it["o"]["d"]["side"]]
            b[0] += f[0]
            b[1] += f[0] * f[1] + f[2]
    return [v["UP"][1] + v["DOWN"][1] - min(v["UP"][0], v["DOWN"][0]) for v in by.values()]


def pre_submission(rows, since) -> collections.Counter:
    """Signals rejected before submission: the exact 5-minute summary counters,
    the last summary of each run summed (journal skip rows are written at most
    once a second per reason, so they undercount)."""
    out, last = collections.Counter(), None
    for r in rows:
        if since and float(r.get("wall") or 0) < since:
            continue
        if r.get("result") == "start":
            if last:
                out.update(last)
            last = None
        elif r.get("result") == "summary" and isinstance(r.get("skips"), dict):
            last = r["skips"]
    if last:
        out.update(last)
    return out


def fetch_winners(orders, cache_path):
    from accounting.resolution import Resolver
    cache = {}
    if cache_path and os.path.exists(cache_path):
        try:
            cache = json.load(open(cache_path, encoding="utf-8"))
        except (OSError, ValueError):
            cache = {}
    res = Resolver(api_every_s=0.0)
    for o in orders:
        if o["win"] or not o["cond"]:
            continue
        c = o["cond"]
        if c not in cache:
            try:
                r = res(c)
            except Exception:  # noqa: BLE001 - a lookup that failed is not a result
                continue
            if not getattr(r, "resolved", False):
                continue
            s = o["shadow"] or {}
            tok, other = str(s.get("token") or ""), str(s.get("other_token") or "")
            side = o["d"]["side"]
            if tok and r.payout(tok) is not None:
                cache[c] = side if float(r.payout(tok)) >= 0.5 else ("DOWN" if side == "UP" else "UP")
            elif other and r.payout(other) is not None:
                cache[c] = ("DOWN" if side == "UP" else "UP") if float(r.payout(other)) >= 0.5 else side
        if c in cache:
            o["win"] = cache[c]
    if cache_path:
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(cache, fh)


def report(orders, rows, since, min_n: int) -> int:
    skips = pre_submission(rows, since)
    sent = len(orders)
    print(f"SIGNALS REJECTED BEFORE SUBMISSION (exact counters): {sum(skips.values())}")
    for k, v in skips.most_common(10):
        print(f"  {v:8d}  {k}")
    print(f"\nSUBMITTED ORDERS (eligible signals): {sent}; with a known round winner: "
          f"{sum(1 for o in orders if o['win'])}")
    live_kinds = collections.Counter(o["d"].get("kind") for o in orders if o["d"].get("live"))
    if live_kinds:
        print("  live replies: " + ", ".join(f"{k} {v}" for k, v in live_kinds.most_common()))
    for sizing, title in (("same", "SAME DOLLARS (order type and cap only)"),
                          ("own", "AS SIZED (C/D at whole shares x their own cap)")):
        items, unpriced = [], 0
        for o in orders:
            if o["win"] is None:
                continue
            fills = policies_for(o, sizing)
            if fills is None:
                unpriced += 1
                continue
            items.append({"o": o, "fills": fills,
                          "pnl": {p: G.pnl(f[:3], o["d"]["side"], o["win"]) if f else 0.0 for p, f in fills.items()}})
        n = len(items)
        print(f"\n{title}: {n} identical eligible signals" + (f" ({unpriced} left out: C/D not priced by this build)"
                                                            if unpriced else ""))
        if not n:
            continue
        print(f"  {'policy':41s} {'fills':>6s} {'fill%':>6s} {'cost $':>8s} {'P&L $':>8s} {'c/signal':>9s} "
              f"{'per $1':>7s} {'5s mk c':>8s} {'worst round $':>13s}")
        for p in POLICIES:
            f = [i["fills"][p] for i in items if i["fills"][p]]
            cost = sum(x[0] * x[1] + x[2] for x in f)
            pl = sum(i["pnl"][p] for i in items)
            mk = [x[3] for x in f if x[3] is not None]
            wr = worst_rounds(items, p)
            print(f"  {p}  {LABEL[p]:38s} {len(f):6d} {len(f) / n:6.0%} {cost:8.2f} {pl:+8.2f} {100 * pl / n:+9.2f} "
                  f"{(pl / cost if cost else 0):+7.1%} {(sum(mk) / len(mk) if mk else float('nan')):+8.2f} "
                  f"{(max(wr) if wr else 0):13.2f}")
        a_miss = collections.Counter()
        for i in items:
            if not i["fills"]["A"]:
                e = G.entry_at(i["o"]["shadow"], G.delay_of(i["o"]["d"]))
                a_miss[miss_reason((e.get("fak") or {}).get("miss") or e.get("miss"))] += 1
        if a_miss:
            print("  submitted but not filled under A: " + ", ".join(f"{k} {v}" for k, v in a_miss.most_common()))
        if sizing == "same":
            details(items)
        print("  effects:  order type = B-A (cap held), D-C | cap = C-A (type held), D-B")
        for p in ("B", "C", "D"):
            verdicts = []
            for idx in (0, 1):
                per = collections.defaultdict(list)
                for i in items:
                    per[G.periods(float(i["o"]["d"]["wall"]))[idx]].append(i)
                cells = [(k, len(v), 100 * sum(i["pnl"][p] - i["pnl"]["A"] for i in v) / len(v))
                         for k, v in sorted(per.items())]
                enough = [c for c in cells if c[1] >= min_n]
                verdicts.append("INSUFFICIENT" if len(enough) < 2 else
                                "PASS" if all(c[2] > 0 for c in enough) else "FAIL")
            diff = 100 * sum(i["pnl"][p] - i["pnl"]["A"] for i in items) / n
            verdict = ("PASS" if all(v == "PASS" for v in verdicts) else
                       "INSUFFICIENT" if "INSUFFICIENT" in verdicts else "FAIL")
            print(f"  {p} vs A: {diff:+.2f}c per signal -> {verdict} (days {verdicts[0]}, 6h blocks {verdicts[1]})")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--live", action="store_true", help="the live canary's journal and ledger")
    ap.add_argument("--dir", default=".", help="folder with the journal, ledger and settlement files")
    ap.add_argument("--journal", default=None)
    ap.add_argument("--ledger", default=None)
    ap.add_argument("--since", default=None, help='UTC start, "YYYY-MM-DD HH:MM"')
    ap.add_argument("--min-n", type=int, default=20, help="signals a period needs to count")
    ap.add_argument("--fetch-winners", action="store_true",
                    help="look up unknown round results (chain, then API); network, read-only")
    ap.add_argument("--winners-cache", default="exec_eval_winners.json",
                    help="with --fetch-winners: results kept here between runs")
    args = ap.parse_args(argv)
    journal = args.journal or os.path.join(args.dir, "live_sniper.jsonl" if args.live else "paper_orders_sniper.jsonl")
    ledger = args.ledger or os.path.join(args.dir, "live_ledger.json" if args.live else "paper_ledger.json")
    since = calendar.timegm(time.strptime(args.since, "%Y-%m-%d %H:%M")) if args.since else None
    print(f"journal {journal}" + (f", since {args.since} UTC" if args.since else ", all rows"))
    orders = G.load(args.dir, journal, ledger, since)
    if args.fetch_winners:
        fetch_winners(orders, os.path.join(args.dir, args.winners_cache))
    return report(orders, G._rows(journal), since, args.min_n)


if __name__ == "__main__":
    raise SystemExit(main())
