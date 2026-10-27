"""What the sniper's LIVE CANARY measured - the numbers paper cannot give:

    .venv/bin/python live_check.py              # since the last start
    .venv/bin/python live_check.py --hours 24
    .venv/bin/python live_check.py --all

Reads the live journal (LIVE_SNIPER_JOURNAL, default live_sniper.jsonl):
orders sent and what the venue answered, the real fill rate beside the paper
model's own shadow fills on the SAME orders (the calibration this canary
exists for), the price really paid against the ask seen, markouts from the
real fill price, the latency from BTC tick to wire to reply, and halts.
"""
from __future__ import annotations

import argparse
import collections
import statistics
import time
from pathlib import Path

import sniper_check as sc


def q(xs, p):
    xs = sorted(x for x in xs if x is not None)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))] if xs else None


def dist(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    return f"p50 {q(xs, 50):.1f} p90 {q(xs, 90):.1f} max {max(xs):.1f} (n={len(xs)})"


def cause(detail) -> str:
    """One live refusal / halt detail -> its cause."""
    d = str(detail or "").lower()
    if "latency guard paused" in d:
        return "latency pause"
    if "latency guard" in d or "replies slower" in d or ("send -> response" in d and "exceeded" in d):
        return "latency hard halt"
    if "outcome unknown" in d or "ambiguous" in d:
        return "unknown order outcome"
    if "collateral balance is stale" in d or "collateral reading stale" in d:
        return "stale balance"
    if "collateral" in d:
        return "insufficient collateral"
    if "fill channel" in d or "rules" in d or "no current round" in d:
        return "channel/rules not ready"
    return "other"


def replay_guard(rows, probe_s: float = 2.0) -> None:
    """Today's real venue reply times run through the new VenueGuard, its
    recovery checks assumed healthy (one every probe_s while PAUSED): what
    the session would have done, and which decisions the old permanent
    latency halt refused that the new guard would have let through."""
    import sniper_live as SL
    clock = {"t": 0.0}
    rows = sorted(rows, key=lambda r: float(r.get("wall") or 0))
    trans = []
    kill = SL.KillSwitch(None)
    g = SL.VenueGuard(kill, journal=lambda r: trans.append((clock["t"], r["from"], r["to"], r["detail"])),
                      mono=lambda: clock["t"])
    allowed = paused_refused = still_halted = 0
    replies = 0
    for r in rows:
        t = float(r.get("wall") or 0)
        if g.state == g.PAUSED and g.paused_at is not None:      # recovery checks between events
            n = int((t - g.paused_at) // probe_s)
            for i in range(min(n, g.healthy_checks)):
                clock["t"] = g.paused_at + (i + 1) * probe_s
                g.on_probe(True, "assumed healthy")
                if g.state != g.PAUSED:
                    break
        clock["t"] = t
        if r.get("live") and r.get("t_wire_to_reply_ms") is not None and r.get("kind") != "AMBIGUOUS":
            g.on_reply(float(r["t_wire_to_reply_ms"]))
            replies += 1
        elif r.get("result") == "skipped" and r.get("reason") == "live halted" and cause(r.get("detail")) == \
                "latency hard halt":
            if g.state == g.HEALTHY:
                allowed += 1
            elif g.state == g.PAUSED:
                paused_refused += 1
            else:
                still_halted += 1
    print(f"\nGUARD REPLAY - {replies} real venue replies through the new guard (recovery checks assumed healthy):")
    for t, a, b, d in trans:
        print(f"  {time.strftime('%m-%d %H:%M:%S', time.gmtime(t))} {a} -> {b}: {d[:90]}")
    print(f"  final state: {g.state}")
    print(f"  decisions the old permanent halt refused: now SENT {allowed}, refused during a pause {paused_refused}, "
          f"still halted {still_halted}")


def mean(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    m = statistics.fmean(xs)
    se = statistics.stdev(xs) / len(xs) ** 0.5 if len(xs) > 1 else None
    return f"{m:+.2f}" + (f" +-{se:.2f} (t={m / se:+.1f})" if se else "") + f" n={len(xs)}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--env", default=str(sc.ROOT / ".env"))
    ap.add_argument("--replay-guard", action="store_true",
                    help="run the journal's real venue reply times through the new latency guard")
    args = ap.parse_args()
    env = sc.read_env(Path(args.env))
    path = sc.state_path(env, "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl")
    # Line by line, only the rows in scope (the whole journal as dicts got the tool killed on the
    # 2 GB VPS, 2026-10-27); --all still reads everything.
    if args.all:
        rows, _total, start = sc.jsonl_scoped(path, drop=())
    elif args.hours:
        rows, _total, start = sc.jsonl_scoped(path, since_wall=time.time() - args.hours * 3600)
    else:
        rows, _total, start = sc.jsonl_scoped(path, last_start=True)
    print(f"journal : {path}")
    if not rows and start is None:
        print("no live journal rows yet (is the bot running with --live, SNIPER_ONLY=1, SNIPER_LIVE_CANARY=1?)")
        return 0
    starts = [start] if start is not None else []
    since = (time.time() - args.hours * 3600 if args.hours else 0.0 if args.all or not starts
             else float(starts[-1]["wall"]))
    rows = [r for r in rows if float(r.get("wall") or 0) >= since]
    if starts:
        s = starts[-1]
        print(f"start   : {time.strftime('%m-%d %H:%M:%S', time.gmtime(float(s['wall'])))} UTC, code {s.get('code')}, "
              f"live={s.get('live')}\n          {s.get('settings')}")
    span_h = (max(float(r.get("wall") or 0) for r in rows) - min(float(r.get("wall") or 0) for r in rows)) / 3600 \
        if rows else 0.0
    print(f"scope   : {'all' if args.all else f'last {args.hours:g}h' if args.hours else 'since the last start'}"
          f" ({span_h:.1f}h of rows)")

    if args.replay_guard:
        replay_guard(rows)
        return 0
    dec = [r for r in rows if r.get("live") and r.get("result") in ("filled", "refused", "pending", "error")]
    skips = [r for r in rows if r.get("result") == "skipped"]
    kinds = collections.Counter(r.get("kind") or r.get("result") for r in dec)
    fills = {r.get("order_id"): r.get("fill") for r in rows if r.get("result") == "live_fill"}
    # venue-delayed orders that filled later (a MATCHED FAK waiting for its size is already counted)
    pend_filled = sum(1 for r in dec if r.get("result") == "pending" and r.get("kind") != "MATCHED"
                      and fills.get(r.get("order_id")))
    sent = sum(v for k, v in kinds.items() if k not in ("NOT_SENT", "error"))
    filled = kinds.get("MATCHED", 0) + pend_filled
    print(f"\nORDERS SENT {sent}: " + ", ".join(f"{k} {v}" for k, v in kinds.most_common()))
    otypes = collections.Counter(r.get("order_type") or "FOK" for r in dec if r.get("kind") not in (None, "NOT_SENT"))
    if set(otypes) - {"FOK"}:
        print("  order types: " + ", ".join(f"{k} {v}" for k, v in otypes.most_common()))
    if pend_filled:
        print(f"  pending orders that filled later: {pend_filled}")
    print(f"  LIVE FILL RATE {filled}/{sent} = {filled / sent:.0%}" if sent else "  LIVE FILL RATE -")
    print("  (venue-time paper model on the lab recordings: 25% at a 150ms hold, 33% at 100ms, 22% at 200ms)")
    per = collections.Counter(r.get("feed") or "binance" for r in dec
                              if r.get("kind") not in (None, "NOT_SENT"))
    per_fill = collections.Counter(r.get("feed") or "binance" for r in dec
                                   if r.get("kind") == "MATCHED" or fills.get(r.get("order_id")))
    if set(per) - {"binance"}:
        print("  by the feed that fired: " + ", ".join(f"{f} {per_fill[f]}/{n} filled" for f, n in per.items())
)

    # the paper model's shadow fills on the same orders
    shadows = {r.get("id"): r.get("L") or {} for r in rows if r.get("result") == "shadow"}
    sent_rows = [r for r in dec if (r.get("kind") or "") not in ("NOT_SENT",) and r.get("result") != "error"]
    if shadows and sent_rows:
        print("\nPAPER SHADOW ON THE SAME ORDERS (arithmetic fill at L ms after the decision, venue-time book):")
        print(f"  {'L':>6s} {'shadow fill':>11s} {'live fill':>9s}   both  live-only  shadow-only  neither")
        for L in sorted({k for s in shadows.values() for k in s}, key=lambda x: float(x)):
            pairs = []
            for r in sent_rows:
                sh = shadows.get(r.get("id"), {}).get(L)
                if sh is None:
                    continue
                live = r.get("kind") == "MATCHED" or bool(fills.get(r.get("order_id")))
                pairs.append((live, "sh" in sh))
            if not pairs:
                continue
            n = len(pairs)
            c = collections.Counter(pairs)
            print(f"  {L:>5s}ms {sum(b for _a, b in pairs) / n:11.0%} {sum(a for a, _b in pairs) / n:9.0%}   "
                  f"{c[(True, True)]:4d} {c[(True, False)]:10d} {c[(False, True)]:12d} {c[(False, False)]:8d}")
        print("  the L whose shadow fill rate matches the live one is the venue's real delay as paper sees it")
        # The same live orders priced at wider caps by the shadow (venue-time
        # book, arithmetic only): live evidence for a cap change, beside what
        # the orders actually did. No other trader is modelled here.
        live_fill = [r for r in sent_rows if r.get("kind") == "MATCHED" or fills.get(r.get("order_id"))]
        marks5 = {r.get("order_id"): r.get("markout_5s_c") for r in rows if r.get("result") == "markout"}
        live_mk = [marks5.get(r.get("order_id")) for r in live_fill]
        print(f"\n  SHADOW PRICE LIMITS on the same {len(sent_rows)} live orders (5s markout c/share; actual live: "
              f"fill {len(live_fill) / max(1, len(sent_rows)):.0%}, 5s {mean(live_mk)})")
        for L in ("200", "300"):
            cells = []
            for name, key in (("+2c", None), ("FAK+2c", "fak"), ("+3c", "3"), ("+4c", "4"), ("+6c", "6")):
                qs = []
                for r in sent_rows:
                    s0 = shadows.get(r.get("id"), {}).get(L)
                    if not s0 or ("sh" not in s0 and "miss" not in s0 and not s0.get("alt")):
                        continue
                    if key == "fak":       # FAK = the FOK fill when it filled, else what FAK would have kept
                        qv = s0 if "sh" in s0 else (s0.get("fak") or {"miss": "not priced"})
                    else:
                        qv = s0 if key is None else (s0.get("alt") or {}).get(key)
                    if qv is not None:
                        qs.append(qv)
                got = [qv for qv in qs if "sh" in qv]
                mk = [qv.get("mk5") for qv in got if qv.get("mk5") is not None]
                avg = f"{sum(mk) / len(mk):+.2f}" if mk else "-"
                cells.append(f"{name} {len(got) / max(1, len(qs)):4.0%} {avg:>6s}")
            print(f"    at {L}ms:  " + "   ".join(cells))
        print("    a wider cap earns a live test only if its markout stays clearly positive here AND in the replay")

        # Live against its own shadow, order by order: is a markout gap
        # SELECTION (live fills different orders than the shadow) or PRICE
        # (on orders both fill, live pays more / marks out worse)?
        def live_px(r):
            f = fills.get(r.get("order_id"))
            if f and f.get("price") is not None:
                return float(f["price"])
            return float(r["price"]) if r.get("price") is not None and r.get("price_source") != \
                "decision-book estimate" else None
        print("\n  LIVE vs ITS OWN SHADOW, order by order (5s markout c/share):")
        for L in ("200", "300"):
            both, live_only, shadow_only = [], [], []
            for r in sent_rows:
                s0 = shadows.get(r.get("id"), {}).get(L)
                if not s0:
                    continue
                lf = r.get("kind") == "MATCHED" or bool(fills.get(r.get("order_id")))
                if lf and "sh" in s0:
                    both.append((r, s0))
                elif lf:
                    live_only.append(marks5.get(r.get("order_id")))
                elif "sh" in s0:
                    shadow_only.append(s0.get("mk5"))
            dpx = [(live_px(r) - s0["px"]) * 100 for r, s0 in both if live_px(r) is not None]
            print(f"    at {L}ms: both filled {len(both)}: live {mean([marks5.get(r.get('order_id')) for r, _ in both])}"
                  f" vs shadow {mean([s0.get('mk5') for _, s0 in both])}; live paid {mean(dpx)}c vs the shadow's price")
            print(f"              live-only fills {len(live_only)}: {mean(live_only)} | "
                  f"shadow-only fills {len(shadow_only)}: {mean(shadow_only)}")
        print("    gap mostly in 'both' = PRICE (we pay more / mark out worse on the same fills); gap from live-only bad"
              " and shadow-only good = SELECTION (competitors take the good fills)")
        # Why live missed the orders the shadow filled: the venue answered late
        # (its slow tail - nothing to fix here) or on time (someone else's order
        # took the shares first - speed relative to them matters).
        groups = {"both filled": [], "shadow-only": [], "neither": []}
        for r in sent_rows:
            s0 = shadows.get(r.get("id"), {}).get("200")
            if not s0:
                continue
            lf = r.get("kind") == "MATCHED" or bool(fills.get(r.get("order_id")))
            g = "both filled" if lf and "sh" in s0 else "shadow-only" if "sh" in s0 else "neither" if not lf else None
            if g:
                groups[g].append(r)
        print("\n  WHY LIVE MISSED THE SHADOW'S FILLS (orders by their 200ms shadow):")
        print(f"    {'group':12s} {'orders':>6s} {'venue reply ms p50':>18s} {'p90':>6s} {'BTC move bp':>12s} "
              f"{'by feed':>20s}")
        for g, rs in groups.items():
            rep_ms = sorted(float(r["t_wire_to_reply_ms"]) for r in rs if r.get("t_wire_to_reply_ms") is not None)
            p = (lambda k: rep_ms[min(len(rep_ms) - 1, int(len(rep_ms) * k / 100))] if rep_ms else float("nan"))
            bp = [abs(float(r.get("bp") or 0)) for r in rs]
            feeds_ = collections.Counter(r.get("feed") or "binance" for r in rs)
            print(f"    {g:12s} {len(rs):>6d} {p(50):18.0f} {p(90):6.0f} "
                  f"{(sum(bp) / len(bp)) if bp else float('nan'):12.2f} {dict(feeds_)!s:>20s}")
        print("    shadow-only replies as fast as the rest = someone else got there first (competition);"
              " clearly slower = the venue's slow tail")

    # a FAK whose quantity arrived after the reply: its update row carries the fill
    updates = {r.get("order_id"): r for r in rows if r.get("result") == "live_fill_update"}
    got = [r if r.get("result") == "filled" else updates[r.get("order_id")] for r in dec
           if r.get("result") == "filled" or (updates.get(r.get("order_id")) or {}).get("resolved_result") == "filled"]
    if got:
        print("\nFILLS")
        fak = [r for r in got if r.get("order_type") == "FAK"]
        if fak:
            sh = [float(r.get("shares") or 0) for r in fak]
            part = sum(1 for r in fak if r.get("partial"))
            print(f"  FAK fills {len(fak)}: partial {part}, full {len(fak) - part}; shares per fill "
                  f"{sum(sh) / len(sh):.2f} (min {min(sh):.2f}, max {max(sh):.2f})")
        src = collections.Counter(r.get("price_source") for r in got)
        print("  price source: " + ", ".join(f"{k} {v}" for k, v in src.items()))
        real = []
        for r in got:
            f = fills.get(r.get("order_id"))
            px = (f or {}).get("price") or (r.get("price") if r.get("price_source") != "decision-book estimate"
                                            else None)
            if px is not None and r.get("ask_seen") is not None:
                real.append((float(px) - float(r["ask_seen"])) * 100)
        print(f"  price paid - ask seen at the decision (c/share): {mean(real)}")
    marks = [r for r in rows if r.get("result") == "markout"]
    if marks:
        print(f"  markout 1s (c/share, real fill price where known): {mean([m.get('markout_1s_c') for m in marks])}")
        print(f"  markout 5s: {mean([m.get('markout_5s_c') for m in marks])}")

    print("\nLATENCY (ms)")
    print(f"  BTC tick -> decision : {dist([r.get('t_tick_to_decided_ms') for r in dec])}")
    print(f"  loop lag before tick : {dist([r.get('loop_lag_ms') for r in dec])}")
    print(f"  decision -> on wire  : {dist([r.get('t_decided_to_wire_ms') for r in dec])}")
    print(f"  on wire -> reply     : {dist([r.get('t_wire_to_reply_ms') for r in dec])}")
    stages = (("pre-send checks (size, FOK, balance)", "decided", "prepare"),
              ("sender checks (kill, clock, rules, caps)", "prepare", "build"),
              ("signing", "build", "signed"),
              ("order JSON + L2 HMAC", "signed", "ready"),
              ("ready -> write starts", "ready", "post_call"),
              ("write (HEADERS+DATA to the socket)", "post_call", "net_send_done"),
              ("network + venue (incl. its taker hold)", "net_send_start", "response"))
    print("  stage by stage (each order's own transport stamps):")
    for name, a, b in stages:
        xs = [(r["live_ts_us"][b] - r["live_ts_us"][a]) / 1000.0 for r in dec
              if a in (r.get("live_ts_us") or {}) and b in (r.get("live_ts_us") or {})]
        print(f"    {name:42s} {dist(xs)}")
    by_kind = collections.defaultdict(list)
    for r in dec:
        by_kind[r.get("kind")].append(r.get("t_wire_to_reply_ms"))
    for k, xs in sorted(by_kind.items(), key=lambda kv: str(kv[0])):
        print(f"    {str(k):10s} reply: {dist(xs)}")
    print(f"  new connections on the hot path: {sum(1 for r in dec if r.get('new_connection'))}")

    if skips:
        why = collections.Counter(r.get("reason") for r in skips)
        print("\nSKIPPED " + ", ".join(f"{k} {v}" for k, v in why.most_common(8)))
        live_sk = [r for r in skips if r.get("reason") in ("live refused", "live halted")]
        if live_sk:
            by = collections.Counter(cause(r.get("detail")) for r in live_sk)
            print("  live refused / halted by cause: " + ", ".join(f"{k} {v}" for k, v in by.most_common()))
            det = collections.Counter(str(r.get("detail"))[:90] for r in live_sk)
            for k, v in det.most_common(6):
                print(f"  {v:5d}  {k}")
    trans = [r for r in rows if r.get("result") == "guard"]
    if trans:
        probes = [r for r in rows if r.get("result") == "guard_probe"]
        print()
        print(f"VENUE LATENCY GUARD: {len(trans)} transitions, {len(probes)} recovery checks "
              f"({sum(1 for r in probes if r.get('ok'))} healthy)")
        for r in trans[-12:]:
            print(f"  {time.strftime('%m-%d %H:%M:%S', time.gmtime(float(r['wall'])))} {r.get('from')} -> "
                  f"{r.get('to')}: {str(r.get('detail'))[:100]}")
    bad = [r for r in dec if r.get("kind") in ("REJECTED", "AMBIGUOUS", "NOT_SENT") or r.get("result") == "error"]
    for r in bad[-5:]:
        print(f"  {time.strftime('%H:%M:%S', time.gmtime(float(r['wall'])))} {r.get('kind') or r.get('result')}: "
              f"{str(r.get('error') or r.get('reason'))[:110]}")
    summ = [r for r in rows if r.get("result") == "summary" and r.get("live")]
    if summ:
        st = summ[-1]["live"]
        print(f"\nCANARY STATUS (last 5-min summary): balance ${st.get('balance')}, orders {st.get('orders')}, "
              f"fills {st.get('fills')}, spend ${st.get('spend_usd')}, rounds {st.get('rounds')}, "
              f"awaiting ledger {st.get('awaiting_confirmation')}, realized loss ${st.get('session_loss_usd')}")
        print("  HALTED: " + "; ".join(st["halted"]) if st.get("halted") else "  not halted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
