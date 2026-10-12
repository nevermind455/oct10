"""Is the paper sniper working correctly?  Read-only health check.

    .venv/bin/python sniper_check.py            # paths from .env, like the bot
    .venv/bin/python sniper_check.py --hours 6  # only the last 6 hours

Reads the sniper journal (<audit>_sniper.jsonl), the paper audit file and the
paper ledger. Touches nothing, sends nothing. From .env it reads only the
PAPER_*_PATH and SNIPER_* lines.

It checks the plumbing (is it running, does every journalled fill exist in the
audit file and ledger with the sniper tag, 200ms latency, price <= cap, side ==
BTC direction, caps respected, nothing else trading) and then compares the
numbers with the latency lab: ~1 in 3 orders filled, ~+1c/share net markout
5s after the fill, ~7 fills an hour when it may trade the whole round.
"""
from __future__ import annotations


import argparse
import collections
import json
import math
import os
import statistics
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
from accounting.ledger import current_payload  # noqa: E402

LAB_FILL_RATE = 1 / 3
LAB_MARKOUT_5S_C = 1.06
LAB_FILLS_PER_HOUR = 135 / 18


def read_env(path: Path) -> dict:
    keep = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip().removeprefix("export ").strip()
            if k.startswith("SNIPER_") or k in ("PAPER_AUDIT_PATH", "PAPER_LEDGER_PATH"):
                keep[k] = v.split(" #", 1)[0].strip().strip("'\"")
    except OSError:
        pass
    return keep


def state_path(env: dict, name: str, default: str) -> Path:
    raw = os.environ.get(name) or env.get(name) or default
    p = Path(raw)
    return p if p.is_absolute() else ROOT / p


def jsonl(path: Path) -> list[dict]:
    rows = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    rows.append(json.loads(line))
                except ValueError:
                    pass
    except OSError:
        pass
    return rows


def _git_head() -> str | None:
    try:
        import subprocess
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True, timeout=3)
        return out.stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


def order_slip_c(rows) -> float:
    """The price limit the orders were sent with, in cents above the ask they
    saw (SNIPER_SLIP; the median, as MAX_BUY_PRICE clips it for high asks)."""
    xs = [round((float(r["cap"]) - float(r["ask_seen"])) * 100, 1) for r in rows
          if r.get("cap") is not None and r.get("ask_seen") is not None]
    return statistics.median(xs) if xs else 2.0


def refusal_kind(reason) -> str:
    r = str(reason or "").lower()
    if "fok no-fill" in r or "fak no-fill" in r:          # FAK (IOC): nothing left at or below the cap
        return "price ran past the cap (normal)"
    if "unknown book state" in r:
        return "book at the modelled arrival not seen yet (paper measurement gap, not a price move)"
    if "execution interval" in r or "cutoff" in r or "time remaining" in r:
        return "outside trade window (skipped instead since f8bbb2e)"
    if "order price cap" in r and "floor" in r:
        return "price cap at the price floor (skipped instead since 2026-10-12)"
    if "best ask" in r and "min_buy_price" in r:
        # the decision already skips an ask outside the band (6ab06b6): this one
        # fell under MIN_BUY_PRICE during the order's flight
        return "the ask fell under MIN_BUY_PRICE by arrival (normal)"
    if "floor" in r or "min_buy_price" in r or "maximum buy price" in r:
        return "outside price band (skipped instead since 6ab06b6)"
    if "order signing failed" in r:
        return "live library refused to build the order"
    if "marketable buy" in r:
        return "under the venue's $1 minimum (sized up instead since the $1 fix)"
    if "spread" in r:
        return "spread too wide (paper guard)"
    if "stale" in r or "not changed" in r:
        return "book stale (paper guard)"
    if "cash" in r:
        return "out of paper cash"
    if "in flight" in r:
        return "another order in flight"
    return str(reason or "no reason")[:60]


def cluster_ratio(items):
    """Per-share mean of x over s from (round, x, s) rows, with a standard
    error clustered by round: fills in one round share its outcome and its
    BTC path, so counting them as independent overstates the evidence."""
    X, S = collections.defaultdict(float), collections.defaultdict(float)
    for g, x, sh in items:
        X[g] += x
        S[g] += sh
    tot = sum(S.values())
    if not X or tot <= 0:
        return None, None, 0
    r = sum(X.values()) / tot
    if len(X) < 2:
        return r, None, len(X)
    v = len(X) / (len(X) - 1) * sum((X[g] - r * S[g]) ** 2 for g in X)
    return r, math.sqrt(v) / tot, len(X)


def fmt_ratio(items) -> str:
    r, se, g = cluster_ratio(items)
    if r is None:
        return "-"
    return f"{r:+.2f}" if not se else f"{r:+.2f} +/-{se:.2f} (t={r / se:.1f})"


def verdict_line(rep, label, r, se, n_rounds, lab=None):
    t = r / se if se else float("nan")
    sig = "clearly above zero" if t > 2 else ("clearly below zero" if t < -2 else "NOT distinguishable from zero")
    rep.line("PASS" if t > 2 else "WARN",
             f"{label} {r:+.2f}c/share +/-{se:.2f} over {n_rounds} rounds (t={t:.1f}: {sig})"
             + (f"  lab +{lab:.2f}c" if lab is not None else ""))


class Report:
    def __init__(self):
        self.bad = self.warn = 0

    def line(self, status, text):
        if status == "FAIL":
            self.bad += 1
        elif status == "WARN":
            self.warn += 1
        print(f"  [{status:4}] {text}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=None, help="only look at the last N hours")
    ap.add_argument("--all", action="store_true",
                    help="the whole history (default: since the bot's last restart)")
    ap.add_argument("--env", default=str(ROOT / ".env"))
    args = ap.parse_args()

    env = read_env(Path(args.env))
    audit_path = state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
    ledger_path = state_path(env, "PAPER_LEDGER_PATH", "paper_ledger.json")
    journal_path = audit_path.with_name(f"{audit_path.stem}_sniper.jsonl")
    latency_ms = float(env.get("SNIPER_LATENCY_MS") or 200)
    shares_setting = float(env.get("SNIPER_SHARES") or 10)
    min_left = float(env.get("SNIPER_MIN_LEFT_S") or 10)
    max_fills_round = int(float(env.get("SNIPER_MAX_FILLS_ROUND") or 20))
    max_side = float(env.get("SNIPER_MAX_SIDE_SHARES") or 200)
    only = str(env.get("SNIPER_ONLY", "")).lower() in {"1", "true", "yes", "on"}

    print(f"journal : {journal_path}")
    print(f"audit   : {audit_path}")
    print(f"ledger  : {ledger_path}")
    print(f"mode    : {'SNIPER_ONLY' if only else 'sniper beside the main strategy'}"
          + (f"  (fixed latency {latency_ms:.0f}ms" if env.get("SNIPER_LATENCY_MS") else "  (live-like latency")
          + f", {shares_setting:.0f} sh)")

    journal = jsonl(journal_path)
    starts = [r for r in journal if r.get("result") == "start"]
    if not journal:
        print("\nNo sniper journal rows. Is the bot running with SNIPER_ONLY=1 (or SNIPER_ENABLED=1) "
              "in --paper mode, and is PAPER_AUDIT_PATH the same as the bot's?")
        return 2
    if args.hours:
        since, scope = time.time() - args.hours * 3600, f"the last {args.hours:g}h"
    elif args.all or not starts:
        since, scope = 0, "the whole history"
    else:
        since, scope = float(starts[-1]["wall"]) - 1, "since the bot's last restart (--all for everything)"
    print(f"scope   : {scope}")
    journal = [r for r in journal if float(r.get("wall") or 0) >= since]
    audit = [r for r in jsonl(audit_path) if float(r.get("wall") or 0) >= since]
    try:
        ledger = current_payload(str(ledger_path))     # the snapshot plus its journal
    except (OSError, ValueError, RuntimeError):
        ledger = {}

    decisions = [r for r in journal if r.get("result") in ("filled", "refused", "skipped", "error")]
    marks = {r.get("order_id"): r for r in journal if r.get("result") == "markout"}
    fills = [r for r in decisions if r["result"] == "filled"]
    refused = [r for r in decisions if r["result"] == "refused"]
    errors = [r for r in decisions if r["result"] == "error"]
    skipped = [r for r in decisions if r["result"] == "skipped"]
    orders = len(fills) + len(refused)
    if not decisions:
        print("\nNo sniper decisions in this period.")
        return 2
    t0 = min(float(r["wall"]) for r in decisions)
    t1 = max(float(r["wall"]) for r in decisions)
    hours = max((t1 - t0) / 3600, 1e-9)

    rep = Report()
    print(f"\nperiod  : {time.strftime('%m-%d %H:%M', time.gmtime(t0))} -> "
          f"{time.strftime('%m-%d %H:%M', time.gmtime(t1))} UTC ({hours:.1f}h)")

    # ------------------------------------------------------------ running
    print("\n1. RUNNING")
    # Any row proves life - the 5-minute summary rows keep coming when BTC is
    # too quiet for a decision, so a calm hour is not reported as a stop.
    last_any = max(float(r.get("wall") or 0) for r in journal)
    age_min = (time.time() - last_any) / 60
    rep.line("PASS" if age_min < 10 else "FAIL",
             f"last sniper activity {age_min:.1f} min ago" + ("" if age_min < 10 else
             " - the sniper looks stopped (bot down, or Binance socket not connected?)"))
    if starts:
        running = starts[-1].get("code")
        on_disk = _git_head()
        started = time.strftime("%m-%d %H:%M", time.gmtime(float(starts[-1]["wall"])))
        if running and on_disk and running != on_disk:
            rep.line("WARN", f"the bot has run code {running} since {started} UTC, but the code on disk is "
                             f"{on_disk} - restart the bot to use it")
        else:
            rep.line("PASS", f"bot started {started} UTC, running code {running or '?'}"
                             + (" (the latest on disk)" if running and running == on_disk else ""))
    rep.line("PASS" if not errors else "FAIL", f"{len(errors)} errors" + "".join(
        f"\n           {e.get('reason')}" for e in errors[-3:]))

    # ------------------------------------------------------------ funnel
    print("\n2. FUNNEL (signals that reached a decision)")
    print(f"         decisions {len(decisions)}  skipped {len(skipped)}  orders {orders}  "
          f"filled {len(fills)}  refused {len(refused)}")
    for why, n in collections.Counter(r.get("reason") for r in skipped).most_common():
        print(f"         skip  {n:6d}  {why}")
    for why, n in collections.Counter(refusal_kind(r.get("reason")) for r in refused).most_common():
        print(f"         refuse{n:6d}  {why}")
    odd = [r for r in refused if "normal" not in refusal_kind(r.get("reason"))
           and "since" not in refusal_kind(r.get("reason"))
           and "measurement gap" not in refusal_kind(r.get("reason"))]
    old = [r for r in refused if "since" in refusal_kind(r.get("reason"))]
    if old:
        rep.line("WARN", f"{len(old)} refusals the current code skips instead - pull + restart if they are recent "
                         f"(last {(time.time() - max(float(r['wall']) for r in old)) / 60:.0f} min ago)")
    gaps = sum(1 for r in refused if "measurement gap" in refusal_kind(r.get("reason")))
    rep.line("PASS" if not odd else "WARN",
             f"{len(odd)} refusals for reasons other than price moving" if odd else
             "every refusal is the price moving past the cap, as the lab expects"
             + (f" ({gaps} more: paper could not see the book at the modelled arrival)" if gaps else ""))

    # ------------------------------------------------------------ plumbing
    print("\n3. EVERY FILL IS WHAT IT SHOULD BE")
    by_id = {r["order_id"]: r for r in audit if r.get("order_id") and r.get("status") == "FILLED"}
    missing = [f for f in fills if f.get("order_id") not in by_id]
    untagged_link = [f for f in fills if by_id.get(f.get("order_id"), {}).get("tag") != "sniper"
                     and f.get("order_id") in by_id]
    rep.line("PASS" if not missing and not untagged_link else "FAIL",
             f"{len(fills) - len(missing)}/{len(fills)} journalled fills found in the audit file, tagged sniper"
             + (f"; {len(missing)} missing" if missing else "")
             + (f"; {len(untagged_link)} without the tag" if untagged_link else ""))
    # Each order carries the latency it planned (live-like mode varies it with
    # the measured network); older rows fall back to the fixed setting.
    linked = [f for f in fills if f.get("order_id") in by_id]
    planned = {f["order_id"]: float(f.get("latency_ms") or latency_ms) for f in linked}
    wrong_lat = [f for f in linked
                 if abs(float(by_id[f["order_id"]].get("assumed_latency_ms") or -1) - planned[f["order_id"]]) > 0.05]
    rep.line("PASS" if linked and not wrong_lat else ("WARN" if not linked else "FAIL"),
             "every fill waited exactly the latency the sniper planned for it" if linked and not wrong_lat else
             f"{len(wrong_lat)} fills matched with a different latency than planned")
    if linked:
        early = [f for f in linked if (float(by_id[f["order_id"]].get("wall") or 0) - float(f["wall"])) * 1000
                 < planned[f["order_id"]] * 0.95]
        gap = statistics.median((float(by_id[f["order_id"]].get("wall") or 0) - float(f["wall"])) * 1000
                                for f in linked)
        rep.line("PASS" if not early else "FAIL",
                 f"decision -> fill median {gap:.0f}ms, none sooner than planned (the fill waits, it never "
                 f"uses the book it decided on)" if not early else
                 f"{len(early)} fills matched sooner than their planned latency")
    live_like = [f for f in fills if f.get("latency_mode") == "live-like"]
    if live_like:
        med = lambda k: statistics.median(float(f[k]) for f in live_like if f.get(k) is not None)
        signed = [f for f in live_like if f.get("sign_ms") is not None]
        measured = sum(1 for f in live_like if f.get("net_measured"))
        print(f"         live-like fills {len(live_like)}: latency median {med('latency_ms'):.0f}ms = one-way "
              f"{med('net_ms'):.1f}ms ({measured}/{len(live_like)} measured) + hold {med('hold_ms'):.0f} + server "
              f"{med('server_ms'):.0f}; signing median "
              + (f"{statistics.median(float(f['sign_ms']) for f in signed):.1f}ms" if signed else "n/a (not signed)"))
    timed = [f for f in fills if f.get("match_ms") is not None]
    if timed:
        match = statistics.median(float(f["match_ms"]) for f in timed)
        plan = statistics.median(float(f.get("latency_ms") or latency_ms) for f in timed)
        lag = statistics.median(float(f.get("loop_lag_ms") or 0) for f in timed)
        extra = match - plan
        rep.line("PASS" if extra <= 50 else "WARN",
                 f"BTC tick -> order met the book: median {match:.0f}ms = planned {plan:.0f}ms + {extra:.0f}ms "
                 f"of our own process (event loop {lag:.0f}ms, thread, signing)"
                 + ("" if extra <= 50 else " - the VPS is slowing orders down; live would suffer the same"))
    staged = [r for r in fills + refused if r.get("t_total_ms") is not None]
    for r in staged:
        # Rows from before the fast path (a9be5de) timed the broker's checks and
        # the delay separately and in sequence; together they are "in flight".
        if r.get("t_wire_ms") is None and r.get("t_delay_ms") is not None:
            r["t_wire_ms"] = float(r.get("t_checks_ms") or 0) + float(r["t_delay_ms"])
    if staged:
        names = (("t_loop_ms", "event loop"), ("t_decide_ms", "decide"), ("t_handoff_ms", "thread handoff"),
                 ("t_sign_ms", "signing"), ("t_wire_ms", "in flight (network+hold+server)"))
        # Builds differ in which stages exist (since the sign-on-loop build the
        # thread handoff happens after sending, inside the in-flight time), so
        # only stages present are shown and our own part is total - in flight.
        med = {k: statistics.median(float(r[k]) for r in staged if r.get(k) is not None)
               for k, _ in names if any(r.get(k) is not None for r in staged)}
        own = {k: v for k, v in med.items() if k != "t_wire_ms"}
        worst = max(own, key=own.get)
        own_ms = statistics.median(float(r["t_total_ms"]) - float(r["t_wire_ms"])
                                   for r in staged if r.get("t_wire_ms") is not None)
        inside = [float(r["t_inflight_handoff_ms"]) for r in staged if r.get("t_inflight_handoff_ms") is not None]
        print(f"         where the time goes (median ms over {len(staged)} orders): "
              + " | ".join(f"{label} {med[k]:.1f}" for k, label in names if k in med)
              + f" | total {statistics.median(float(r['t_total_ms']) for r in staged):.0f}")
        print(f"         our own part (tick until the order is sent): {own_ms:.1f}ms - "
              f"the biggest piece is {dict(names)[worst]} ({own[worst]:.1f}ms)"
              + (f"; thread handoff {statistics.median(inside):.1f}ms now runs inside the in-flight time"
                 if inside else ""))
    sign_refused = [r for r in refused if "order signing failed" in str(r.get("reason"))]
    if sign_refused:
        rep.line("WARN", f"{len(sign_refused)} orders the live library refused to build, e.g. "
                         f"{sign_refused[-1].get('reason')}")
    over_cap = [f for f in fills if float(f.get("price") or 0) > float(f.get("cap") or 1) + 1e-9]
    rep.line("PASS" if not over_cap else "FAIL", "no fill above its price cap" if not over_cap
             else f"{len(over_cap)} fills above the cap")
    wrong_side = [r for r in fills + refused
                  if (r.get("side") == "UP") != (float(r.get("bp") or 0) > 0)]
    rep.line("PASS" if not wrong_side else "FAIL", "every order bought the side BTC moved toward"
             if not wrong_side else f"{len(wrong_side)} orders on the wrong side")
    late = [f for f in fills if float(f.get("left_s") or 999) < min_left]
    rep.line("PASS" if not late else "FAIL", f"no fill with under {min_left:.0f}s left" if not late
             else f"{len(late)} fills too late in the round")
    per_round = collections.defaultdict(lambda: {"n": 0, "UP": 0.0, "DOWN": 0.0})
    for f in fills:
        pr = per_round[f.get("round_end")]
        pr["n"] += 1
        pr[f.get("side")] += float(f.get("shares") or 0)
    cap_bust = [k for k, v in per_round.items()
                if v["n"] > max_fills_round or max(v["UP"], v["DOWN"]) > max_side + 1e-9]
    rep.line("PASS" if not cap_bust else "FAIL", f"round caps held ({max_fills_round} fills, "
             f"{max_side:.0f} sh/side)" if not cap_bust else f"{len(cap_bust)} rounds over the caps")
    both = sum(1 for v in per_round.values() if v["UP"] and v["DOWN"])
    print(f"         {len(per_round)} rounds traded, {both} with both sides bought (natural pairs, not forced)")
    if only:
        others = [r for r in audit if r.get("status") == "FILLED" and r.get("tag") != "sniper"
                  and float(r.get("wall") or 0) >= t0]
        rep.line("PASS" if not others else "FAIL", "no other strategy filled anything (SNIPER_ONLY holds)"
                 if not others else f"{len(others)} non-sniper fills while SNIPER_ONLY - the main strategy traded")

    # ------------------------------------------------------------ vs the lab
    print("\n4. NUMBERS vs THE LATENCY LAB")
    old_marks = sum(1 for f in fills if "loop_lag_ms" not in f)
    if old_marks:
        rep.line("WARN", f"{old_marks} fills are from before the markout fix: their 1s markouts may belong to "
                         f"another fill - use --hours to look only at the time since the restart")
    if orders:
        fr = len(fills) / orders
        rep.line("PASS" if fr >= 0.2 else "WARN",
                 f"fill rate {fr:.0%} of {orders} orders (lab ~{LAB_FILL_RATE:.0%})")
    rate = len(fills) / hours
    print(f"         {rate:.1f} fills/hour (lab ~{LAB_FILLS_PER_HOUR:.1f} when it may trade the whole round "
          f"and asks 0.05-0.95 - a narrower window or band means proportionally fewer)")
    slip = [(float(f.get("price") or 0) - float(f.get("ask_seen") or 0)) * 100 for f in fills]
    if slip:
        print(f"         paid {statistics.fmean(slip):+.2f}c vs the ask seen at the decision "
              f"(cap allows +{order_slip_c(fills):g}c)")
    for k, lab in (("markout_1s_c", None), ("markout_5s_c", LAB_MARKOUT_5S_C)):
        items = [(f.get("round_end"), float(marks[f["order_id"]][k]) * float(f.get("shares") or 0),
                  float(f.get("shares") or 0))
                 for f in fills if marks.get(f.get("order_id"), {}).get(k) is not None]
        r, se, g = cluster_ratio(items)
        if r is None or se is None:
            print(f"         {k[8:10]} markout: not enough data yet")
            continue
        verdict_line(rep, f"{k[8:10]} net markout", r, se, g, lab)

    # ------------------------------------------------------------ settlement
    print("\n5. SETTLED P&L (noisy: each share pays $1 or $0)")
    lot_pay = {}
    for pos in (ledger.get("positions") or {}).values():
        for lot in pos.get("lots") or []:
            lot_pay[lot.get("trade_id")] = (pos.get("settled"), pos.get("payout_per_share"))
    settled, pnl, wins, sh = 0, 0.0, 0, 0.0
    not_in_ledger = 0
    items, blocks = [], collections.defaultdict(lambda: [0.0, 0.0, 0])
    for f in fills:
        s = lot_pay.get(f.get("order_id"))
        if s is None:
            not_in_ledger += 1
            continue
        if not s[0] or s[1] is None:
            continue
        settled += 1
        shares = float(f.get("shares") or 0)
        p = shares * float(s[1]) - float(f.get("cost") or 0)
        pnl += p
        wins += float(s[1]) > 0.5
        sh += shares
        items.append((f.get("round_end"), p * 100, shares))
        b = blocks[int((float(f["wall"]) - t0) // (6 * 3600))]
        b[0] += p
        b[1] += shares
        b[2] += 1
    rep.line("PASS" if not not_in_ledger else "FAIL",
             f"{len(fills) - not_in_ledger}/{len(fills)} fills are in the ledger")
    if settled:
        print(f"         {settled} settled fills: P&L ${pnl:+.2f} ({pnl / sh * 100:+.2f}c/share), "
              f"won {wins}/{settled} ({wins / settled:.0%})")
        r, se, g = cluster_ratio(items)
        if se:
            verdict_line(rep, "settled P&L", r, se, g)
        print("         by 6h block: " + "  ".join(
            f"${v[0]:+.0f} ({v[0] / v[1] * 100:+.1f}c, {v[2]})" for _, v in sorted(blocks.items()) if v[1]))
    vol = sum(float(f.get("cost") or 0) for f in fills)
    fees = sum(float(f.get("fee") or 0) for f in fills)
    print(f"         volume ${vol:,.0f}, taker fees paid ${fees:,.2f} (already inside the P&L)")
    print(f"         {len(fills) - not_in_ledger - settled} fills still waiting for their round to settle")
    if items:
        # Busy rounds (BTC swinging) take many fills; do they pay?
        by_round = collections.defaultdict(lambda: [0, 0.0, 0.0])   # fills, P&L cents, shares
        for g, p_c, shs in items:
            br = by_round[g]
            br[0] += 1
            br[1] += p_c
            br[2] += shs
        counts = sorted(v[0] for v in by_round.values())
        at_cap = sum(1 for c in counts if c >= max_fills_round)
        print(f"         fills per round: median {statistics.median(counts):.0f}, 90% of rounds <= "
              f"{counts[int(0.9 * (len(counts) - 1))]}, max {counts[-1]}; "
              f"{at_cap} of {len(counts)} rounds hit the {max_fills_round}-fill cap")
        edges = [b for b in (1, 4, 8, 13) if b < max_fills_round] + [max_fills_round]
        spans = [(edges[i], edges[i + 1] - 1) for i in range(len(edges) - 1)] + [(max_fills_round, None)]
        print(f"         {'fills in round':>16}  {'rounds':>6}  {'P&L':>9}  settled c/share")
        for lo, hi in spans:
            sel = [v for v in by_round.values() if v[0] >= lo and (hi is None or v[0] <= hi)]
            if not sel:
                continue
            label = f"{lo}-{hi}" if hi is not None else f"{lo} (cap)"
            money = f"${sum(v[1] for v in sel) / 100:+.2f}"
            print(f"         {label:>16}  {len(sel):>6}  {money:>9}  "
                  f"{fmt_ratio([(i, v[1], v[2]) for i, v in enumerate(sel)])}")

    # ------------------------------------------------------------ latency curve
    print("\n6. LATENCY CURVE (the same orders priced at other latencies - arithmetic only, nothing sent)")
    shadows = [r for r in journal if r.get("result") == "shadow"]
    shadow_errors = sum(1 for r in journal if r.get("result") == "shadow_error")
    if not shadows:
        print("         no shadow rows yet - pull + restart to get the build with shadow fills")
    else:
        pay = {}
        for pos in (ledger.get("positions") or {}).values():
            if pos.get("settled") and pos.get("payout_per_share") is not None:
                pay[pos.get("token_id")] = float(pos["payout_per_share"])
        print(f"         {len(shadows)} orders shadowed"
              + (f", {shadow_errors} shadow errors" if shadow_errors else "")
              + "; the edge at each latency tells how fast a live order must be")
        print(f"         {'latency':>8}  {'filled':>6}  {'5s markout c/share':<24}  {'settled c/share':<24}  timer late")
        for L in sorted({int(k) for r in shadows for k in (r.get("L") or {})}):
            n_fill, mk, st, late = 0, [], [], []
            for r in shadows:
                v = (r.get("L") or {}).get(str(L))
                if not v:
                    continue
                late.append(float(v.get("late_ms") or 0))
                if "px" not in v:
                    continue
                n_fill += 1
                sh, g = float(v["sh"]), r.get("round_end")
                if v.get("mk5") is not None:
                    mk.append((g, float(v["mk5"]) * sh, sh))
                o = r.get("other_token")
                pay_t = pay.get(r.get("token"), (1.0 - pay[o]) if o in pay else None)
                if pay_t is not None:
                    st.append((g, (sh * pay_t - sh * float(v["px"]) - float(v["fee"])) * 100, sh))
            print(f"         {L:>6}ms  {n_fill / len(shadows):>6.0%}  {fmt_ratio(mk):<24}  {fmt_ratio(st):<24}  "
                  f"{statistics.median(late) if late else 0:+.0f}ms")

        # -------------------------------------------------------- wider cap
        # Same orders, same latency, but a higher price limit: do the misses
        # it would turn into fills make or lose money?
        planned_lat = [float(f["latency_ms"]) for f in fills if f.get("latency_ms")]
        target = statistics.median(planned_lat) if planned_lat else 200.0
        Ls = sorted({int(k) for r in shadows for k in (r.get("L") or {})})
        Lc = min(Ls, key=lambda x: abs(x - target))
        slips = sorted({k for r in shadows
                        for k in (((r.get("L") or {}).get(str(Lc)) or {}).get("alt") or {})}, key=float)
        if slips:
            print(f"\n7. WIDER PRICE LIMIT (the same orders at the {Lc}ms shadow with a higher limit - "
                  f"do the extra fills pay?)")
            print(f"         {'limit':>8}  {'filled':>6}  {'5s markout c/share':<24}  {'settled c/share':<24}")
            for slip in ["base"] + slips:
                n_fill, mk, st = 0, [], []
                for r in shadows:
                    v = (r.get("L") or {}).get(str(Lc))
                    if not v:
                        continue
                    # An order whose wider limit would pass 0.99 has no entry
                    # for it: its cap is already as wide as it gets.
                    q = v if slip == "base" else (v.get("alt") or {}).get(slip, v)
                    if "px" not in q:
                        continue
                    n_fill += 1
                    sh, g = float(q["sh"]), r.get("round_end")
                    if q.get("mk5") is not None:
                        mk.append((g, float(q["mk5"]) * sh, sh))
                    o = r.get("other_token")
                    pay_t = pay.get(r.get("token"), (1.0 - pay[o]) if o in pay else None)
                    if pay_t is not None:
                        st.append((g, (sh * pay_t - sh * float(q["px"]) - float(q["fee"])) * 100, sh))
                label = f"ask+{order_slip_c(fills + refused):g}c" if slip == "base" else f"ask+{slip}c"
                print(f"         {label:>8}  {n_fill / len(shadows):>6.0%}  {fmt_ratio(mk):<24}  {fmt_ratio(st):<24}")
            print("         a wider limit is only worth it if its markout holds up while the fill rate rises")

    print(f"\nVERDICT: {rep.bad} FAIL, {rep.warn} WARN - "
          + ("plumbing broken, see FAIL lines" if rep.bad else
             "working as designed" + ("; see WARN lines" if rep.warn else "")))
    return 1 if rep.bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
