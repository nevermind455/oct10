#!/usr/bin/env python3
"""Credential-free, reproducible analysis of a downloaded VPS records archive.

python execution_audit.py --archive PATH --out data/execution_audit

Reads named journals, ledgers and reconciliation state; never extracts tar paths or imports trading
configuration. No network, no orders. Output contains private trading data:
keep it local. Shadow values are counterfactuals, never actual executions.
"""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import math
import random
from pathlib import Path
import statistics
import tarfile
from datetime import datetime, timezone


def utc(wall):
    return datetime.fromtimestamp(wall, timezone.utc).isoformat()


def stats(values):
    a = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))
    if not a:
        return {"n": 0}
    def percentile(p):
        i = (len(a) - 1) * p
        lo = int(i)
        return a[lo] + (a[min(lo + 1, len(a) - 1)] - a[lo]) * (i - lo)
    return {"n": len(a), "mean": statistics.mean(a), "p50": percentile(.5),
            "p95": percentile(.95), "p99": percentile(.99), "max": a[-1]}


def snapshot(shadow, delay, method):
    """An explicitly labelled grid sensitivity, NOT an interpolated fill.

    floor reproduces the old bug. next is another sampled scenario, not a
    conservative bound (books are not monotonic). Out-of-grid => unknown.
    """
    grid = sorted((float(k), v) for k, v in (shadow or {}).get("L", {}).items())
    if not grid or delay is None or not math.isfinite(delay) or not grid[0][0] <= delay <= grid[-1][0]:
        return None, None
    points = [(k, v) for k, v in grid if k <= delay] if method == "floor" else [
        (k, v) for k, v in grid if k >= delay]
    return (points[-1] if method == "floor" else points[0])


def quote(row):
    """None = missing evidence, () = model no-fill, tuple = model fill."""
    if row is None:
        return None
    q = row if row.get("sh") else (row.get("fak") or row)
    if q.get("sh"):
        return float(q["sh"]), float(q["px"]), float(q["fee"])
    why = str(q.get("miss", "")).lower()
    if why.startswith("fak no-fill"):
        return ()
    return None


def payouts(rows, ledger):
    result = {}
    for r in rows:
        if r.get("event") != "RESULT_RECEIVED":
            continue
        w = r.get("winner")
        if w in ("UP", "DOWN", "50/50"):
            result[r["condition_id"]] = {s: .5 if w == "50/50" else float(s == w) for s in ("UP", "DOWN")}
    by_token = {}
    for p in ledger.get("positions", {}).values():
        pay = p.get("payout_per_share")
        if p.get("settled") and pay is not None:
            by_token[p["token_id"]] = float(pay)
    return result, by_token


def fill_pnl(fill, pay):
    if fill == ():
        return 0.0
    if fill is None or pay is None:
        return None
    return 0.0 if not fill else fill[0] * (pay - fill[1]) - fill[2]


def read_archive(path):
    """Allowlisted names only; reject duplicate members / malformed records."""
    runs = {}
    provenance = {}
    with tarfile.open(path, "r:gz") as archive:
        counts = collections.Counter(m.name for m in archive.getmembers())
        for directory, journal, ledger in (
            ("sniperbtc", "live_sniper.jsonl", "live_ledger.json"),
            ("sniperbtc", "paper_orders_sniper.jsonl", "paper_ledger.json"),
            ("sniperbtc-paper50", "paper_orders_sniper.jsonl", "paper_ledger.json"),
        ):
            data = []
            journal_member = f"{directory}/{ledger}.journal"
            if counts[journal_member] > 1:
                raise ValueError("duplicate ledger journal")
            if counts[journal_member] and archive.getmember(journal_member).size:
                raise ValueError("unapplied ledger journal: materialize a reconciled snapshot first")
            for name in (journal, ledger, ledger.replace(".json", "_settlements.jsonl")):
                member = f"{directory}/{name}"
                if counts[member] != 1:
                    raise ValueError(f"missing or duplicate archive member: {member}")
                info = archive.getmember(member)
                if not info.isfile() or info.size > 256 * 1024 * 1024:
                    raise ValueError(f"invalid archive member: {member}")
                raw = archive.extractfile(member).read()
                provenance[member] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
                if name.endswith(".jsonl"):
                    data.append([json.loads(line) for line in raw.splitlines() if line.strip()])
                else:
                    data.append(json.loads(raw))
            key = "live" if journal.startswith("live") else "paper50" if directory.endswith("paper50") else "paper_old"
            runs[key] = data
    return runs, provenance


def reconciled_zeros(path):
    """Sanitize durable canary outcomes; never export signed order bodies.

    This is the bot's persisted reconciliation evidence, not a fresh venue
    lookup. A confirmed ledger lot always takes precedence over this state.
    """
    member = "sniperbtc/live_canary_state.json"
    with tarfile.open(path, "r:gz") as t:
        matches = [m for m in t.getmembers() if m.name == member]
        if not matches:
            return set(), {}
        if len(matches) != 1 or not matches[0].isfile() or matches[0].size > 8 * 1024 * 1024:
            raise ValueError("invalid canary snapshot member")
        raw = t.extractfile(matches[0]).read()
        state = json.loads(raw)
    ids = set()
    for intent in state.get("intents", {}).values():
        if not isinstance(intent, dict) or intent.get("status") != "NO_MATCH" or not intent.get("order_id"):
            continue
        # reserve is the original signed reservation, retained for audit even
        # after release. The terminal state and explicit spent=0 prove this
        # bot reconciliation, not a zero in that historical reserve field.
        spent = intent.get("spent")
        if isinstance(spent, (int, float)) and not isinstance(spent, bool) and math.isfinite(spent) and spent == 0:
            ids.add(str(intent["order_id"]))
    return ids, {member: {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                         "evidence": "durable bot reconciliation; signed order bodies excluded from outputs"}}


def orders_from(rows, ledger, settlements, live, zero_ids=()):
    if any(float(p.get("realized_from_sales") or 0) or float(p.get("sell_fees") or 0)
           or any(float(l.get("consumed") or 0) for l in p.get("lots", []))
           for p in ledger.get("positions", {}).values()):
        raise ValueError("sale history needs cash-flow replay; BUY-only audit cannot silently ignore it")
    decisions, shadows, sends = {}, {}, {}
    runs = []
    for r in rows:
        typ = r.get("result")
        if typ == "start":
            runs.append({"wall": r["wall"], "code": r.get("code"), "settings": r.get("settings")})
        if typ == "live_send":
            key = str(r["client_order_id"])
            if key in sends:
                raise ValueError("duplicate live submission identity")
            sends[key] = r
        if typ == "shadow":
            if r["id"] in shadows:
                raise ValueError("duplicate shadow identity")
            shadows[r["id"]] = r
        if typ in ("filled", "refused", "pending") and "bp" in r:
            if r["id"] in decisions:
                raise ValueError("duplicate decision identity")
            decisions[r["id"]] = r
    market_pay, token_pay = payouts(settlements, ledger)
    lots = collections.defaultdict(list)
    for pos in ledger.get("positions", {}).values():
        for lot in pos.get("lots", []):
            if lot.get("status") == "CONFIRMED" and lot.get("side") == "BUY":
                key = lot.get("order_id") or (lot.get("trade_id") if not live else None)
                if key:
                    lots[key].append(lot)
    orders = []
    for rid, d in sorted(decisions.items(), key=lambda x: x[1]["wall"]):
        send = sends.get(str(d.get("client_order_id")), {})
        oid = send.get("order_id") or d.get("order_id")
        ll = lots.get(oid, [])
        if ll:
            shares = sum(float(l["shares"]) for l in ll)
            principal = sum(float(l["shares"]) * float(l["price"]) for l in ll)
            actual = (shares, principal / shares, sum(float(l["fee"]) for l in ll))
            truth = "CONFIRMED"
        elif send.get("kind") == "NO_MATCH" or (not live and d.get("result") == "refused"):
            actual, truth = (), "ZERO"
        elif oid and oid in zero_ids:
            actual, truth = (), "RECONCILED_ZERO_CANARY"
        else:
            actual, truth = None, "UNRESOLVED_NOT_IN_LEDGER"
        shadow = shadows.get(rid)
        pay = token_pay.get(str((shadow or {}).get("token")))
        if pay is None:
            pay = market_pay.get(d.get("condition_id"), {}).get(d["side"])
        ack = d.get("t_signal_to_response_ms")
        # Paper latency is an input; live ACK is only a proxy, not venue match.
        latency = ack if ack is not None else d.get("latency_ms")
        delay = float(latency) if latency is not None else None
        orders.append({"id": rid, "order_id": oid, "d": d, "send": send, "shadow": shadow,
                       "actual": actual, "truth": truth, "pay": pay, "delay": delay,
                       "pnl": fill_pnl(actual, pay)})
    if live and len(sends) != len(orders):
        raise ValueError("live sends and decisions do not reconcile")
    matched = {o["order_id"] for o in orders}
    unmatched = set(lots) - matched
    return orders, runs, len(unmatched)


def actual_summary(orders):
    fills = [o for o in orders if o["actual"]]
    resolved = [o for o in orders if o["pnl"] is not None]
    return {"submissions": len(orders), "confirmed_filled_orders": len(fills),
            "fill_rate": len(fills) / len(orders) if orders else None,
            "truth": dict(collections.Counter(o["truth"] for o in orders)),
            "settled_pnl_orders": len(resolved), "net_pnl": sum(o["pnl"] for o in resolved),
            "principal": sum(o["actual"][0] * o["actual"][1] for o in fills),
            "fees": sum(o["actual"][2] for o in fills),
            "unknown_payout_fills": sum(o["pay"] is None for o in fills)}


def scenario(orders, extra=0, method="floor"):
    included = []
    skipped = collections.Counter()
    for o in orders:
        grid_ms, row = snapshot(o["shadow"], None if o["delay"] is None else o["delay"] + extra, method)
        f = quote(row)
        if f is None:
            skipped["out_of_grid_or_unknown_book"] += 1
            continue
        if o["actual"] is None or o["pay"] is None:
            skipped["unknown_actual_or_payout"] += 1
            continue
        included.append((o, f, grid_ms))
    n = len(included)
    predicted = [bool(f) for o, f, k in included]
    observed = [bool(o["actual"]) for o, f, k in included]
    return {"n": n, "excluded": dict(skipped), "method": method, "extra_ms": extra,
            "warning": "sampled snapshot sensitivity; neither an exact fill nor a bound; ACK != match",
            "predicted_fills": sum(predicted), "actual_fills_same_orders": sum(observed),
            "false_fill_predictions": sum(a and not b for a, b in zip(predicted, observed)),
            "missed_fill_predictions": sum(b and not a for a, b in zip(predicted, observed)),
            "classification_errors": sum(a != b for a, b in zip(predicted, observed)),
            "model_pnl": sum(fill_pnl(f, o["pay"]) for o, f, k in included),
            "actual_pnl_same_orders": sum(o["pnl"] for o, f, k in included),
            "mean_grid_displacement_ms": statistics.mean(k - o["delay"] - extra for o, f, k in included) if n else None}


def holdout(orders, cutoff):
    """Train before UTC Oct 16, test afterwards; whole conditions never split.

    Predeclared candidates scored on binary fill error, not profits. A common
    support set prevents a longer delay winning by hiding difficult orders.
    This is a retrospective validation, not untouched future evidence.
    """
    cond_first = {}
    cond_end = {}
    for o in orders:
        c = o["d"]["condition_id"]
        cond_first[c] = min(cond_first.get(c, math.inf), o["d"]["wall"])
        cond_end[c] = max(cond_end.get(c, 0), o["d"].get("round_end", o["d"]["wall"]))
    candidates = [0, 50, 100, 150, 200, 300, 400]
    support = [o for o in orders if o["actual"] is not None and o["pay"] is not None
               and o["delay"] is not None
               and all(quote(snapshot(o["shadow"], o["delay"] + e, "next")[1]) is not None for e in candidates)
               and quote(snapshot(o["shadow"], o["delay"], "floor")[1]) is not None]
    train = [o for o in support if cond_end[o["d"]["condition_id"]] <= cutoff - 300]
    test = [o for o in support if cond_first[o["d"]["condition_id"]] >= cutoff]
    fit = [scenario(train, e, "next") for e in candidates]
    best = min(fit, key=lambda s: (s["classification_errors"], s["extra_ms"])) if train else None
    # Paired resampling of 30-minute time blocks; not independent orders.
    blocks = collections.defaultdict(list)
    for o in test:
        floor = quote(snapshot(o["shadow"], o["delay"], "floor")[1])
        nxt = quote(snapshot(o["shadow"], o["delay"], "next")[1])
        actual = bool(o["actual"])
        blocks[int(o["d"]["round_end"]) // 1800].append(int(bool(floor) != actual) - int(bool(nxt) != actual))
    rng, intervals = random.Random(0), []
    values = list(blocks.values())
    if values:
        for _ in range(2000):
            sample = [v for _ in values for v in rng.choice(values)]
            intervals.append(statistics.mean(sample))
        intervals.sort()
    return {"cutoff_utc": utc(cutoff), "support_orders": len(support),
            "excluded_common_support": len(orders) - len(support),
            "embargo_ms": 300_000,
            "embargo_orders": len(support) - len(train) - len(test),
            "train_conditions": len({o["d"]["condition_id"] for o in train}),
            "test_conditions": len({o["d"]["condition_id"] for o in test}),
            "training_candidates": fit, "selected_extra_ms": best["extra_ms"] if best else None,
            "test_baseline": scenario(test, 0, "next"),
            "test_old_floor": scenario(test, 0, "floor"),
            "floor_minus_next_error_rate_block_ci95": [intervals[49], intervals[1949]] if intervals else None,
            "test_selected": scenario(test, best["extra_ms"], "next") if best else None,
            "limitation": "retrospective, previously inspected archive; delay absorbs unknown mechanisms; not production calibration"}


def audit(path, holdout_utc="2026-10-16T00:00:00+00:00"):
    raw, files = read_archive(path)
    zero_ids, zero_files = reconciled_zeros(path)
    files.update(zero_files)
    result = {"archive_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(), "files": files, "runs": {}}
    normalized = []
    for name, (rows, ledger, settlements) in raw.items():
        orders, runs, unmatched = orders_from(rows, ledger, settlements, name == "live", zero_ids if name == "live" else ())
        entry = {"from_utc": utc(min(r["wall"] for r in rows)), "to_utc": utc(max(r["wall"] for r in rows)),
                 "versions": runs, "event_counts": dict(collections.Counter(r.get("result") for r in rows)),
                 "actual": actual_summary(orders), "unmatched_ledger_order_ids": unmatched,
                 "feeds": {}, "latency_ms": {}, "markout_cents": {}}
        for feed in sorted({o["d"].get("feed", "unknown") for o in orders}):
            entry["feeds"][feed] = actual_summary([o for o in orders if o["d"].get("feed", "unknown") == feed])
        for k in ("t_signal_to_send_ms", "t_wire_to_reply_ms", "t_signal_to_response_ms", "loop_lag_ms"):
            entry["latency_ms"][k] = stats(o["d"].get(k) for o in orders)
        for label, start, end in (("sign", "sign_start", "sign_end"), ("build_to_ready", "build", "ready"),
                                  ("ready_to_wire", "ready", "net_send_start")):
            entry["latency_ms"][label] = stats((s[end] - s[start]) / 1000 for o in orders
                                              if (s := o["send"].get("ts_us", {})) and start in s and end in s)
        markouts = {r["order_id"]: r for r in rows if r.get("result") == "markout" and r.get("order_id")}
        for k in ("markout_1s_c", "markout_5s_c"):
            entry["markout_cents"][k] = stats(markouts.get(o["order_id"], {}).get(k) for o in orders if o["actual"])
        entry["buckets"] = {}
        for label, bucket in (
            ("move_bp", lambda o: "<1" if abs(o["d"]["bp"]) < 1 else ">=1"),
            ("ack_ms", lambda o: "unknown" if o["delay"] is None else "<400" if o["delay"] < 400 else ">=400"),
            ("decision_fee_adjusted_edge", lambda o: "<2c" if float(o["d"].get("fair", 0)) - float(o["d"].get("ask_seen", 0))
             - float(o["d"].get("fee_rate", 0)) * (float(o["d"].get("ask_seen", 0))
             * (1 - float(o["d"].get("ask_seen", 0)))) ** int(o["d"].get("fee_exponent", 1)) < .02 else ">=2c"),
        ):
            groups = collections.defaultdict(list)
            for o in orders:
                groups[bucket(o)].append(o)
            entry["buckets"][label] = {k: actual_summary(v) for k, v in groups.items()}
        if name == "live":
            entry["shadow_sensitivities"] = [scenario(orders, e, m) for e in (0, 200) for m in ("floor", "next")]
            cutoff = datetime.fromisoformat(holdout_utc)
            if cutoff.tzinfo is None:
                raise ValueError("holdout timestamp requires UTC offset")
            entry["retrospective_holdout"] = holdout(orders, cutoff.timestamp())
            fills = [o for o in orders if o["actual"]]
            entry["slippage_cents"] = stats(100 * (o["actual"][1] - o["d"]["ask_seen"]) for o in fills)
            entry["fill_net_edge_cents"] = stats(100 * (o["d"]["fair"] - o["actual"][1] - o["actual"][2] / o["actual"][0]) for o in fills)
            entry["above_cap_confirmed"] = sum(o["actual"][1] > o["d"]["cap"] + 1e-5 for o in fills)
            entry["reply_http_status"] = dict(collections.Counter(str(o["send"].get("http_status")) for o in orders))
            entry["new_connections"] = sum(bool(o["send"].get("new_connection")) for o in orders)
            entry["confirmed_notional_partials"] = sum(o["actual"][0] * o["actual"][1] < float(o["d"]["amount"]) - 1e-5 for o in fills)
        result["runs"][name] = entry
        for o in orders:
            normalized.append({"run": name, "id": o["id"], "order_id": o["order_id"], "wall": o["d"]["wall"],
                               "side": o["d"]["side"], "condition": o["d"]["condition_id"], "truth": o["truth"],
                               "shares": o["actual"][0] if o["actual"] else None,
                               "price": o["actual"][1] if o["actual"] else None,
                               "fee": o["actual"][2] if o["actual"] else None,
                               "payout": o["pay"], "pnl": o["pnl"], "signal_to_ack_ms": o["delay"]})
    return result, normalized


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--holdout-utc", default="2026-10-16T00:00:00+00:00", help="freeze before evaluating test outcomes")
    args = ap.parse_args(argv)
    result, rows = audit(args.archive, args.holdout_utc)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "metrics.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    with (args.out / "orders.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({k: v["actual"] for k, v in result["runs"].items()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
