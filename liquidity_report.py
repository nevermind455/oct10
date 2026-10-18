#!/usr/bin/env python3
"""Liquidity disappearance attribution - offline, read-only, MEASUREMENT ONLY.

    python liquidity_report.py                                  # the paper journal
    python liquidity_report.py live_sniper.jsonl --hours 24     # live, last 24 hours
    python liquidity_report.py live_sniper.jsonl --csv out.csv --json out.json

When the bot saw executable asks under its price cap after a BTC signal, were
they consumed by public trades, withdrawn without trades, or is the evidence
insufficient? Reads the book_trace rows' `attribution` (code since 2026-10-18):
per step and span, the gross ask reductions/additions under the ORIGINAL cap
from the bot's own installed book states, the public BUY prints received in
the window, matched depletion (min of the two), unexplained reductions, feed
coverage and a diagnostic label. Older rows have no attribution: they are
reported as NO_TELEMETRY, never as zero trades.

Labels are diagnostics with configurable thresholds (SNIPER_ATTRIB_HI/LO in
the bot; --hi/--lo here for the sensitivity table), never execution gates.
NON_TRADE_DOMINATED means displayed asks shrank much more than observed
executions explain - consistent with cancels or repricing, not proof of any
particular maker's cancel. The print side is assumed to be the taker's; the
side check below tests that against the book.
"""
from __future__ import annotations

import argparse
import collections
import csv
import json
import statistics
import time

LABELS = ("TRADE_DOMINATED", "NON_TRADE_DOMINATED", "MIXED", "NO_DEPLETION", "INCONCLUSIVE")


def med(xs):
    xs = [x for x in xs if x is not None]
    return None if not xs else round(statistics.median(xs), 4)


def classify(red, matched, unknown, coverage, hi, lo, min_sh=1.0, unknown_frac=0.2):
    if coverage != "COMPLETE_OBSERVED":
        return "INCONCLUSIVE"
    if red is None or red < min_sh:
        return "NO_DEPLETION"
    if unknown > unknown_frac * red:
        return "INCONCLUSIVE"
    ratio = matched / red
    return "TRADE_DOMINATED" if ratio >= hi else "NON_TRADE_DOMINATED" if ratio <= lo else "MIXED"


def tolerant(att: dict, cap: float, reply_ms: float, tol_ms: float, hi: float, lo: float):
    """signal->reply label with prints counted up to tol_ms outside the window
    (receive-time alignment sensitivity). Reductions are the stored level changes."""
    span = att["spans"]["signal-reply"]
    red = sum(-d for t, p, d in att.get("level_changes") or () if d < 0 and 0 < t <= reply_ms)
    buy = unknown = 0.0
    for t, _vt, price, size, side in att.get("prints") or ():
        if price is None or size is None or float(price) > cap + 1e-9:
            continue
        if -tol_ms < t <= reply_ms + tol_ms:
            if side == "BUY":
                buy += float(size)
            elif side not in ("SELL",):
                unknown += float(size)
    return classify(red, min(buy, red), unknown, span["feed_coverage_status"], hi, lo)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("journal", nargs="?", default="paper_orders_sniper.jsonl")
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--csv", default=None)
    ap.add_argument("--json", default=None)
    ap.add_argument("--hi", type=float, default=None, help="TRADE_DOMINATED threshold for the sensitivity table")
    ap.add_argument("--lo", type=float, default=None, help="NON_TRADE_DOMINATED threshold for the sensitivity table")
    args = ap.parse_args()
    since = time.time() - args.hours * 3600 if args.hours else 0.0
    orders, traces, sends = {}, {}, {}
    with open(args.journal, encoding="utf-8") as fh:
        for line in fh:
            if not any(k in line for k in ('"book_trace"', '"filled"', '"refused"', '"pending"', '"live_send"')):
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("wall", 0) < since:
                continue
            res = r.get("result")
            if res == "book_trace" and r.get("id"):
                traces[r["id"]] = r
            elif res in ("filled", "refused", "pending") and r.get("id"):
                orders[r["id"]] = r
            elif res == "live_send" and r.get("client_order_id"):
                sends[r["client_order_id"]] = r
    rows, side_tally = [], collections.defaultdict(lambda: collections.Counter())
    for oid, t in traces.items():
        o = orders.get(oid, {})
        att = t.get("attribution")
        pts = t.get("points") or {}
        kind = t.get("kind") or o.get("kind") or ("FILLED" if o.get("result") == "filled" else o.get("result"))
        resp = pts.get("response") or {}
        row = {"order_id": t.get("client_order_id") or o.get("order_id") or oid, "decision_id": oid,
               "outcome": t.get("side"), "token": t.get("token"), "original_cap": t.get("cap"), "result": kind,
               "quote_age_ms": o.get("dec_quote_age_ms"), "spread": o.get("dec_spread"),
               "reply_latency_ms": resp.get("at_ms"),
               "order_shares": round(float(t["amount"]) / float(t["cap"]), 4) if t.get("amount") and t.get("cap") else None,
               "depth_at_0": (pts.get("0") or {}).get("depth_cap_sh"),
               "depth_at_50": (pts.get("50") or {}).get("depth_cap_sh"),
               "depth_at_150": (pts.get("150") or {}).get("depth_cap_sh"),
               "depth_at_250": (pts.get("250") or {}).get("depth_cap_sh"),
               "depth_at_reply": resp.get("depth_cap_sh"),
               "ask_seen": t.get("ask_seen"), "best_ask_at_reply": (resp.get("asks") or [[None]])[0][0]}
        if att:
            span = att["spans"]["signal-reply"]
            row.update(relevant_trade_volume=span.get("relevant_public_trade_volume"),
                       trade_associated_depletion=span.get("matched_trade_depletion_estimate"),
                       unexplained_reduction=span.get("unexplained_reduction_estimate"),
                       gross_reductions=span.get("observed_gross_reductions"),
                       gross_additions=span.get("observed_gross_additions"),
                       trade_attribution_ratio=span.get("trade_attribution_ratio"),
                       feed_coverage=span.get("feed_coverage_status"), coverage_reason=span.get("coverage_reason"),
                       classification=span.get("attribution_classification"),
                       confidence=span.get("attribution_confidence"),
                       class_0_50=(att["spans"].get("0-50") or {}).get("attribution_classification"),
                       class_0_150=(att["spans"].get("0-150") or {}).get("attribution_classification"),
                       class_0_250=(att["spans"].get("0-250") or {}).get("attribution_classification"),
                       _att=att)
            for side, c in (att.get("side_check") or {}).items():
                side_tally[side].update(c)
        else:
            row.update(feed_coverage="UNKNOWN", classification="NO_TELEMETRY", confidence="NONE")
        rows.append(row)
    if not rows:
        print("no book_trace rows in this journal/window")
        return 1
    n = len(rows)
    tel = [r for r in rows if r["classification"] != "NO_TELEMETRY"]
    complete = [r for r in tel if r.get("feed_coverage") == "COMPLETE_OBSERVED"]
    print(f"{n} traced orders ({args.journal}); with attribution telemetry: {len(tel)}; "
          f"without (historical, NOT zero trades): {n - len(tel)}")
    print(f"feed coverage of the signal->reply window: "
          f"{dict(collections.Counter(r.get('feed_coverage') for r in tel))}")
    for title, group in (("ALL TRACED (denominator = every traced order)", rows),
                         ("WITH TELEMETRY", tel), ("COMPLETE COVERAGE ONLY", complete)):
        if not group:
            continue
        c = collections.Counter(r["classification"] for r in group)
        print(f"\n{title}: n={len(group)}")
        for lab in LABELS + ("NO_TELEMETRY",):
            if c.get(lab):
                print(f"   {lab:20s} {c[lab]:5d}  ({100 * c[lab] / len(group):.0f}%)")
    if tel:
        red = sum(r.get("gross_reductions") or 0 for r in tel)
        mt = sum(r.get("trade_associated_depletion") or 0 for r in tel)
        print(f"\nsignal->reply, all telemetry: ask reductions under the cap {red:,.0f} shares; "
              f"matched to public BUY prints {mt:,.0f} ({100 * mt / red if red else 0:.0f}%); "
              f"unexplained {red - mt:,.0f}")
        print("\nby span (orders labelled; complete coverage only):")
        for span in ("class_0_50", "class_0_150", "class_0_250", "classification"):
            c = collections.Counter(r.get(span) for r in complete if r.get(span))
            print(f"   {span:16s} {dict(c)}")
        print("\nMATCHED vs NO_MATCH (medians, telemetry rows):")
        for name, key in (("depth at signal", "depth_at_0"), ("depth at 250ms", "depth_at_250"),
                          ("depth at reply", "depth_at_reply"), ("trade-associated depletion", "trade_associated_depletion"),
                          ("unexplained reductions", "unexplained_reduction"), ("reply latency ms", "reply_latency_ms"),
                          ("quote age ms", "quote_age_ms"), ("spread", "spread")):
            filled = [r.get(key) for r in tel if r["result"] in ("FILLED", "MATCHED", "filled")]
            missed = [r.get(key) for r in tel if r["result"] in ("NO_FILL", "NO_MATCH", "refused")]
            print(f"   {name:28s} filled {med(filled)!s:>10}   missed {med(missed)!s:>10}")
        jump = lambda r: None if r.get("best_ask_at_reply") is None or r.get("ask_seen") is None else \
            100 * (float(r["best_ask_at_reply"]) - float(r["ask_seen"]))  # noqa: E731
        print(f"   {'best-ask move to reply (c)':28s} filled "
              f"{med([jump(r) for r in tel if r['result'] in ('FILLED', 'MATCHED', 'filled')])!s:>10}   missed "
              f"{med([jump(r) for r in tel if r['result'] in ('NO_FILL', 'NO_MATCH', 'refused')])!s:>10}")
        print("   depth/order size at signal: filled "
              f"{med([r['depth_at_0'] / r['order_shares'] for r in tel if r.get('depth_at_0') is not None and r.get('order_shares') and r['result'] in ('FILLED', 'MATCHED', 'filled')])}"
              "   missed "
              f"{med([r['depth_at_0'] / r['order_shares'] for r in tel if r.get('depth_at_0') is not None and r.get('order_shares') and r['result'] in ('NO_FILL', 'NO_MATCH', 'refused')])}")
        print("\nby outcome:", {side: dict(collections.Counter(r["classification"] for r in tel if r["outcome"] == side))
                                for side in ("UP", "DOWN")})
        print("\nSIDE CHECK (first book state after each print): does a print's side match which side of the book shrank?")
        for side, c in sorted(side_tally.items()):
            tot = sum(v for k, v in c.items() if k != "no_state")
            if tot:
                print(f"   {side:5s} prints {tot:5d}: ask drop {100 * c['ask_drop'] / tot:.0f}%, "
                      f"bid drop {100 * c['bid_drop'] / tot:.0f}%, neither {100 * c['neither'] / tot:.0f}%"
                      f"  (no state: {c['no_state']})")
        print("   if BUY prints mostly show ask drops, side = the taker's; if not, read every label as uncertain")
        hi = args.hi if args.hi is not None else (tel[0]["_att"]["thresholds"]["hi"] if tel else 0.7)
        lo = args.lo if args.lo is not None else (tel[0]["_att"]["thresholds"]["lo"] if tel else 0.3)
        print(f"\nSENSITIVITY (signal->reply; prints counted up to the tolerance outside the window; hi={hi} lo={lo}):")
        for tol in (0, 25, 50, 100, 250):
            c = collections.Counter(tolerant(r["_att"], float(r["original_cap"]), float(r["reply_latency_ms"] or 0),
                                             tol, hi, lo) for r in tel if r.get("reply_latency_ms") is not None)
            print(f"   tolerance {tol:4d} ms: {dict(c)}")
    out_rows = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
    if args.csv:
        keys = list(dict.fromkeys(k for r in out_rows for k in r))
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=keys)
            w.writeheader()
            w.writerows(out_rows)
        print(f"\nwrote {args.csv}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump({"orders": out_rows, "levels": {r["decision_id"]: r["_att"]["spans"]["signal-reply"].get("levels")
                                                      for r in tel}}, fh, indent=1, default=str)
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
