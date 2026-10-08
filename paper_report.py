#!/usr/bin/env python3
"""Real-time PAPER run report - per market, per source, with invariant checks.

    python paper_report.py --dir ~/sniperbtc-realtime            # table + checks
    python paper_report.py --dir DIR --json report.json          # also as JSON

Read-only. Everything comes from the run's own files: the paper ledger (its
snapshot + journal), the paper order audit (every simulated fill and refusal,
tagged sniper / manager), the sniper journal (orders, round summaries,
latency), the manager journal (decisions, results, per-market summaries) and
the settlement journal (results from the venue, settlement times).

All fills here are SIMULATED PAPER FILLS against real Polymarket books; prices,
books, round timing and results are real. Nothing here is an exchange fill.
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import os
import pathlib
import statistics
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from accounting.ledger import current_payload  # noqa: E402

SOURCES = ("sniper", "manager")


def rows(path: pathlib.Path) -> list[dict]:
    out = []
    if not path.exists():
        return out
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if isinstance(r, dict):
                out.append(r)
    return out


def files(d: pathlib.Path) -> dict:
    ledger = d / "paper_ledger.json"
    return {"ledger": ledger, "audit": d / "paper_orders.jsonl", "sniper": d / "paper_orders_sniper.jsonl",
            "manager": d / "paper_orders_sniper_manager.jsonl",
            "settle": ledger.with_name(f"{ledger.stem}_settlements.jsonl"), "monitor": d / "monitor.jsonl"}


def q(xs, p):
    xs = sorted(x for x in xs if x is not None and math.isfinite(x))
    return None if not xs else round(xs[min(len(xs) - 1, int(len(xs) * p / 100))], 3)


def stats(xs) -> dict | None:
    xs = [x for x in xs if x is not None and math.isfinite(x)]
    return None if not xs else {"n": len(xs), "p50": q(xs, 50), "p95": q(xs, 95), "p99": q(xs, 99),
                                "max": round(max(xs), 3)}


def source_of(src: str) -> str:
    s = str(src or "")
    return s.split("/", 1)[1] if s.startswith("paper_live_book/") else ("untagged" if s == "paper_live_book" else "other")


def build(d: pathlib.Path) -> dict:
    f = files(d)
    ledger = current_payload(str(f["ledger"])) if f["ledger"].exists() else {"positions": {}, "seen": []}
    audit, sniper, manager, settle = rows(f["audit"]), rows(f["sniper"]), rows(f["manager"]), rows(f["settle"])
    markets: dict = collections.OrderedDict()

    def mk(cid, end=None):
        m = markets.get(cid)
        if m is None:
            m = markets[cid] = {"condition_id": cid, "slug": None, "window_end": end, "signals": None,
                                "signals_up": None, "signals_down": None, "sniper_orders": 0, "full": 0,
                                "partial": 0, "zero": 0, "over_target": 0, "rejected": 0, "pm_evaluations": None,
                                "pm_opportunities": None, "pm_orders": 0, "pm_fills": 0, "pm_zero": 0,
                                "pm_blocked": {}, "spend": {s: 0.0 for s in SOURCES}, "fees": {s: 0.0 for s in SOURCES},
                                "shares": {}, "winner": None, "result_s": None, "settled_s": None,
                                "pnl": {s: None for s in SOURCES}, "pnl_total": None, "state": "OPEN"}
        if end and not m["window_end"]:
            m["window_end"] = end
        return m
    end_to_cid = {}
    for r in sniper:
        cid, end = r.get("condition_id"), r.get("round_end")
        if cid and end:
            end_to_cid[int(float(end))] = cid
        if r.get("result") in ("filled", "refused", "error") and cid:
            m = mk(cid, int(float(end)) if end else None)
            m["sniper_orders"] += 1
            if r["result"] == "filled":
                tgt = float(r.get("target_shares") or 0)
                got = float(r.get("shares") or 0)
                if r.get("partial") and got + 1e-6 < tgt:
                    m["partial"] += 1
                else:
                    m["full"] += 1
                if r.get("over_target"):
                    m["over_target"] += 1
            elif "no-fill" in str(r.get("reason") or "").lower():
                m["zero"] += 1
            else:
                m["rejected"] += 1
    for r in sniper:
        if r.get("result") == "round_summary":
            cid = end_to_cid.get(int(float(r.get("round_end") or 0)))
            if cid:
                m = mk(cid)
                m["signals"], m["signals_up"], m["signals_down"] = r.get("signals"), r.get("signals_up"), r.get("signals_down")
    for r in manager:
        ev, cid = str(r.get("event") or ""), r.get("condition")
        if not cid:
            continue
        if ev.startswith("POSITION_MANAGER BUY"):
            mk(cid)["pm_orders"] += 1
        elif ev.startswith("POSITION_MANAGER RESULT"):
            if r.get("status") in ("FILLED", "PARTIAL"):
                mk(cid)["pm_fills"] += 1
            elif r.get("status") == "ZERO_FILL":
                mk(cid)["pm_zero"] += 1
        elif ev == "POSITION_MANAGER MARKET SUMMARY":
            m = mk(cid)
            m["pm_evaluations"], m["pm_opportunities"] = r.get("evaluations"), r.get("opportunities")
            m["pm_blocked"] = r.get("blocked") or {}
    for r in audit:
        if not r.get("order_id") or not r.get("condition_id") or r.get("status") != "FILLED":
            continue
        src = str(r.get("tag") or "sniper")
        m = mk(r["condition_id"])
        if src in SOURCES:
            m["spend"][src] += float(r.get("total_cost") or 0.0)
            m["fees"][src] += float(r.get("fee") or 0.0)
    for r in settle:
        cid = r.get("condition_id")
        if not cid:
            continue
        m = mk(cid, r.get("window_end"))
        m["slug"] = m["slug"] or r.get("slug")
        if r.get("event") == "RESULT_RECEIVED":
            m["winner"] = r.get("winner")
            m["result_s"] = round(float(r.get("clock") or 0) - float(r.get("window_end") or 0), 1) if r.get("window_end") else None
        elif r.get("event") == "ACCOUNTING_SETTLED":
            m["settled_s"] = round(float(r.get("clock") or 0) - float(r.get("window_end") or 0), 1) if r.get("window_end") else None
    # positions and P/L from the ledger itself (actual fills, settled payouts)
    for tok, p in (ledger.get("positions") or {}).items():
        cid = p.get("condition_id")
        if not cid:
            continue
        m = mk(cid)
        side = m["shares"].setdefault(tok, {"shares": 0.0, "cost": 0.0, "by": {}})
        side["shares"] += float(p.get("shares") or 0.0)
        side["cost"] += float(p.get("cost") or 0.0)
        for lot in p.get("lots") or []:
            if str(lot.get("side") or "").upper() != "BUY":
                continue
            s = source_of(lot.get("source"))
            cost = float(lot["shares"]) * float(lot["price"]) + float(lot.get("fee") or 0.0)
            side["by"][s] = side["by"].get(s, 0.0) + float(lot["shares"])
            if p.get("settled"):
                pnl = float(lot["shares"]) * float(p.get("payout_per_share") or 0.0) - cost
                m["pnl"][s] = (m["pnl"].get(s) or 0.0) + pnl
        if p.get("settled"):
            m["state"] = "SETTLED"
        elif m["state"] != "SETTLED":
            m["state"] = "PENDING" if m.get("window_end") and time.time() > float(m["window_end"]) else "OPEN"
    for m in markets.values():
        if any(v is not None for v in m["pnl"].values()):
            m["pnl_total"] = sum(v or 0.0 for v in m["pnl"].values())
        m["slug"] = m["slug"] or (f"btc-updown-5m-{int(m['window_end']) - 300}" if m.get("window_end") else None)
    totals = aggregate(markets, sniper, manager, audit, ledger)
    checks = check(d, ledger, audit, sniper, manager, settle, markets)
    return {"generated": time.strftime("%Y-%m-%d %H:%M:%S"), "dir": str(d), "markets": list(markets.values()),
            "totals": totals, "checks": checks, "latency": latency(sniper), "monitor": monitor(f["monitor"])}


def aggregate(markets, sniper, manager, audit, ledger) -> dict:
    ms = list(markets.values())
    done = [m for m in ms if m["state"] == "SETTLED"]
    t = {"markets_seen": len(ms), "markets_settled": len(done),
         "markets_pending": sum(1 for m in ms if m["state"] == "PENDING"),
         "signals": sum(m["signals"] or 0 for m in ms), "sniper_orders": sum(m["sniper_orders"] for m in ms),
         "sniper_full": sum(m["full"] for m in ms), "sniper_partial": sum(m["partial"] for m in ms),
         "sniper_zero": sum(m["zero"] for m in ms), "sniper_over_target": sum(m["over_target"] for m in ms),
         "manager_evaluations": sum(m["pm_evaluations"] or 0 for m in ms),
         "manager_opportunities": sum(m["pm_opportunities"] or 0 for m in ms),
         "manager_orders": sum(m["pm_orders"] for m in ms), "manager_fills": sum(m["pm_fills"] for m in ms),
         "manager_zero_fills": sum(m["pm_zero"] for m in ms)}
    for s in SOURCES:
        t[f"{s}_spend"] = round(sum(m["spend"][s] for m in ms), 4)
        t[f"{s}_fees"] = round(sum(m["fees"][s] for m in ms), 4)
        t[f"{s}_pnl"] = round(sum(m["pnl"].get(s) or 0.0 for m in done), 4)
        wins = sum(1 for m in done if (m["pnl"].get(s) or 0) > 0)
        losses = sum(1 for m in done if m["pnl"].get(s) is not None and m["pnl"][s] <= 0)
        t[f"{s}_wins"], t[f"{s}_losses"] = wins, losses
    t["combined_pnl"] = round(t["sniper_pnl"] + t["manager_pnl"], 4)
    t["fees"] = round(t["sniper_fees"] + t["manager_fees"], 4)
    t["capital_deployed"] = round(t["sniper_spend"] + t["manager_spend"], 4)
    # exposure over time from the fills (open cost until each market settled) and the
    # realized-equity drawdown (settlement by settlement)
    events = []
    for tok, p in (ledger.get("positions") or {}).items():
        for lot in p.get("lots") or []:
            events.append((float(lot["wall"]), float(lot["shares"]) * float(lot["price"]) + float(lot.get("fee") or 0)))
        if p.get("settled") and p.get("settled_wall"):
            events.append((float(p["settled_wall"]), -float(p.get("cost") or 0.0)))
    open_cost, peak = 0.0, 0.0
    for _w, delta in sorted(events):
        open_cost += delta
        peak = max(peak, open_cost)
    t["max_exposure"] = round(peak, 4)
    equity, high, dd = 0.0, 0.0, 0.0
    for m in sorted(done, key=lambda m: m.get("window_end") or 0):
        equity += m["pnl_total"] or 0.0
        high = max(high, equity)
        dd = max(dd, high - equity)
    t["max_drawdown_realized"] = round(dd, 4)
    t["pnl_per_manager_dollar"] = (round(t["manager_pnl"] / t["manager_spend"], 4) if t["manager_spend"] else None)
    return t


def check(d, ledger, audit, sniper, manager, settle, markets) -> dict:
    """The run's invariants; every count must be 0 except the measured ones."""
    c = collections.Counter()
    lots = [l for p in (ledger.get("positions") or {}).values() for l in p.get("lots") or []]
    ids = [l["trade_id"] for l in lots]
    c["duplicate_fills"] = len(ids) - len(set(ids))
    c["seen_mismatch"] = int(set(ids) != set(ledger.get("seen") or []))
    for p in (ledger.get("positions") or {}).values():
        if float(p.get("shares") or 0) < -1e-9 or float(p.get("cost") or 0) < -1e-9:
            c["negative_shares_or_cost"] += 1
        if any(l.get("condition_id") != p.get("condition_id") for l in p.get("lots") or []):
            c["cross_market_corruption"] += 1
    fills = [r for r in audit if r.get("status") == "FILLED" and r.get("order_id")]
    c["audit_vs_ledger_fill_mismatch"] = abs(len(fills) - len([l for l in lots if source_of(l.get("source")) in SOURCES]))
    for r in fills:
        if r.get("cash_after") is not None and float(r["cash_after"]) < -1e-9:
            c["negative_cash"] += 1
        if r.get("tag") == "manager" and r.get("partial"):
            c["manager_fok_partial"] += 1
    for r in manager:
        if str(r.get("event") or "").startswith("POSITION_MANAGER BUY"):
            if (r.get("new_pairs") or 0) > 1e-9 and r.get("classification") != "EXPOSURE_IMPROVEMENT":
                lim = 1.0 - float(r.get("threshold_pair") or 0.0)
                if r.get("incremental_pair_cost") is None or float(r["incremental_pair_cost"]) > lim + 1e-9:
                    c["pair_threshold_violation"] += 1
    done = collections.Counter(r.get("condition_id") for r in settle if r.get("event") == "ACCOUNTING_SETTLED"
                               and int(r.get("settled_now") or 0) > 0)
    c["double_settlement"] = sum(1 for v in done.values() if v > 1)
    now = time.time()
    for m in markets.values():
        if m["state"] == "PENDING" and m.get("window_end") and now - float(m["window_end"]) > 900:
            c["missing_settlement_15min"] += 1
    for r in sniper:
        if r.get("result") == "error":
            c["sniper_errors"] += 1
        if r.get("result") == "stall":
            c["measured_event_loop_stalls"] += 1
    c["measured_fills_above_target"] = sum(m["over_target"] for m in markets.values())
    return dict(c)


def latency(sniper) -> dict:
    orders = [r for r in sniper if r.get("result") in ("filled", "refused") and isinstance(r.get("lat"), dict)]
    lat = [r["lat"] for r in orders]
    out = {k: stats([x.get(k) for x in lat]) for k in ("feed_to_trigger", "trigger_to_decision", "trigger_to_send",
                                                       "send_to_ack", "trigger_to_ack")}
    out["note"] = "paper: send_to_ack / trigger_to_ack are the MODELLED match (simulated), never a CLOB ACK"
    out["binance_excess_ms"] = stats([r.get("binance_excess_ms") for r in orders])
    out["loop_lag_ms"] = stats([r.get("loop_lag_ms") for r in orders])
    return out


def monitor(path: pathlib.Path) -> dict:
    rs = rows(path)
    res = [r for r in rs if r.get("kind") == "resources"]
    if not res:
        return {}
    first, last = res[0], res[-1]
    return {"samples": len(res), "rss_mb_first": first.get("rss_mb"), "rss_mb_last": last.get("rss_mb"),
            "rss_mb_max": max(r.get("rss_mb") or 0 for r in res), "cpu_pct_p50": q([r.get("cpu_pct") for r in res], 50),
            "cpu_pct_max": max(r.get("cpu_pct") or 0 for r in res), "threads_last": last.get("threads"),
            "handles_first": first.get("handles"), "handles_last": last.get("handles"),
            "restarts": sum(1 for r in rs if r.get("kind") == "restart"),
            "alerts": [r for r in rs if r.get("kind") == "alert"][-20:]}


def table(rep: dict) -> str:
    out = ["SIMULATED PAPER FILLS on real Polymarket books; real BTC feed, real rounds and results.",
           f"{'market (window end UTC)':24s} {'sig':>4s} {'ord':>4s} {'full':>4s} {'part':>4s} {'zero':>4s} "
           f"{'pm ev':>6s} {'opp':>4s} {'pm o':>4s} {'pm f':>4s} {'sn $':>8s} {'pm $':>8s} {'fees':>6s} "
           f"{'win':>5s} {'sn P/L':>8s} {'pm P/L':>8s} {'P/L':>8s} {'state':>8s}"]
    f2 = lambda v: "--" if v is None else f"{v:+.2f}"  # noqa: E731
    for m in rep["markets"]:
        end = m.get("window_end")
        label = time.strftime("%m-%d %H:%M", time.gmtime(float(end))) if end else (m["condition_id"] or "")[:12]
        out.append(f"{label:24s} {str(m['signals'] if m['signals'] is not None else '--'):>4s} {m['sniper_orders']:>4d} "
                   f"{m['full']:>4d} {m['partial']:>4d} {m['zero']:>4d} "
                   f"{str(m['pm_evaluations'] if m['pm_evaluations'] is not None else '--'):>6s} "
                   f"{str(m['pm_opportunities'] if m['pm_opportunities'] is not None else '--'):>4s} "
                   f"{m['pm_orders']:>4d} {m['pm_fills']:>4d} {m['spend']['sniper']:>8.2f} {m['spend']['manager']:>8.2f} "
                   f"{m['fees']['sniper'] + m['fees']['manager']:>6.2f} {str(m['winner'] or '--'):>5s} "
                   f"{f2(m['pnl'].get('sniper')):>8s} {f2(m['pnl'].get('manager')):>8s} {f2(m['pnl_total']):>8s} "
                   f"{m['state']:>8s}")
    t = rep["totals"]
    out += ["", "TOTALS " + json.dumps(t), "CHECKS " + json.dumps(rep["checks"]), "LATENCY " + json.dumps(rep["latency"]),
            "MONITOR " + json.dumps({k: v for k, v in rep["monitor"].items() if k != "alerts"})]
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--dir", default=os.path.expanduser("~/sniperbtc-realtime"))
    ap.add_argument("--json", default=None)
    args = ap.parse_args(argv)
    rep = build(pathlib.Path(args.dir))
    print(table(rep))
    if args.json:
        pathlib.Path(args.json).write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
