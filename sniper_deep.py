"""Where does the paper sniper's edge come from, and would a simple rule do
better?  Read-only, like sniper_check.py.

    .venv/bin/python sniper_deep.py          # since the bot's last restart
    .venv/bin/python sniper_deep.py --all    # the whole history
    .venv/bin/python sniper_deep.py --hours 72   # only the last 3 days (fresh data
                                                 # for a rule found in an older report)
    .venv/bin/python sniper_deep.py --daily  # the whole journal, one row per UTC day
    .venv/bin/python sniper_deep.py --hourly # the last 48h per UTC hour, beside BTC's own volatility
    .venv/bin/python sniper_deep.py --gates --hours 72   # every skip reason in detail + the
                                                 # effective settings recorded at each restart
    .venv/bin/python sniper_deep.py --by-feed    # orders / fills / markouts split by the feed
                                                 # that fired them (SNIPER_EXTRA_FEEDS); --live
                                                 # reads the live canary's journal instead

Every breakdown is shown separately for the first and the second half of the
rounds (A | B). A pattern that shows up in only one half is noise. One that
holds in both is worth a closer look - and still has to hold in fresh data
before anything is changed, because trying many cuts on one sample will
always find some that look good by luck.

Columns: 5s = net markout 5s after the fill (mid - price - fee, c/share; less
noisy), settled = what the fill actually made at settlement (c/share), with
round-clustered t values.
"""
from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

import sniper_check as sc


def load(env_path: str, whole: bool, hours: float | None = None):
    env = sc.read_env(Path(env_path))
    audit_path = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
    ledger_path = sc.state_path(env, "PAPER_LEDGER_PATH", "paper_ledger.json")
    journal = sc.jsonl(audit_path.with_name(f"{audit_path.stem}_sniper.jsonl"))
    starts = [r for r in journal if r.get("result") == "start"]
    if hours:
        import time
        since, scope = time.time() - hours * 3600, f"the last {hours:g}h"
    elif whole or not starts:
        since, scope = 0.0, "the whole history"
    else:
        since, scope = float(starts[-1]["wall"]) - 1, "since the bot's last restart (--all for everything)"
    journal = [r for r in journal if float(r.get("wall") or 0) >= since]
    try:
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        ledger = {}
    pay = {}
    for pos in (ledger.get("positions") or {}).values():
        if pos.get("settled") and pos.get("payout_per_share") is not None:
            for lot in pos.get("lots") or []:
                pay[lot.get("trade_id")] = float(pos["payout_per_share"])
    marks = {r.get("order_id"): r for r in journal if r.get("result") == "markout"}
    fills = []
    for r in journal:
        if r.get("result") != "filled" or r.get("order_id") not in pay:
            continue
        sh = float(r.get("shares") or 0)
        if sh <= 0:
            continue
        p = pay[r["order_id"]]
        mk = (marks.get(r["order_id"]) or {}).get("markout_5s_c")
        fills.append({
            "g": r.get("round_end"), "wall": float(r["wall"]), "side": r.get("side"),
            "ask": float(r.get("ask_seen") or r.get("price") or 0), "bp": abs(float(r.get("bp") or 0)),
            "left": float(r.get("left_s") or 0), "sh": sh,
            "pnl_c": (sh * p - float(r.get("cost") or 0)) * 100,
            "mk5": None if mk is None else float(mk),
            "match_ms": r.get("match_ms"),
        })
    fills.sort(key=lambda f: f["wall"])
    # Position in the round, the round's previous side, gap since the last fill.
    last_side, count, prev_wall = {}, collections.Counter(), None
    for f in fills:
        count[f["g"]] += 1
        f["k"] = count[f["g"]]
        f["flip"] = f["g"] in last_side and last_side[f["g"]] != f["side"]
        last_side[f["g"]] = f["side"]
        f["gap"] = None if prev_wall is None else f["wall"] - prev_wall
        prev_wall = f["wall"]
    rounds = sorted({f["g"] for f in fills})
    half = set(rounds[: len(rounds) // 2])
    for f in fills:
        f["half"] = "A" if f["g"] in half else "B"
    return fills, scope, rounds


def daily(env_path: str) -> int:
    """One row per UTC day over the whole journal: which paper model ran,
    how active BTC was (signals/h), how many orders went out, how many
    filled, and what the fills made - so a change in daily P&L can be split
    into fewer orders, fewer fills, and less per fill."""
    import time
    env = sc.read_env(Path(env_path))
    audit_path = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
    ledger_path = sc.state_path(env, "PAPER_LEDGER_PATH", "paper_ledger.json")
    journal = sc.jsonl(audit_path.with_name(f"{audit_path.stem}_sniper.jsonl"))
    try:
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        ledger = {}
    pay = {}
    for pos in (ledger.get("positions") or {}).values():
        if pos.get("settled") and pos.get("payout_per_share") is not None:
            for lot in pos.get("lots") or []:
                pay[lot.get("trade_id")] = float(pos["payout_per_share"])
    marks = {r.get("order_id"): r for r in journal if r.get("result") == "markout"}
    days = collections.OrderedDict()
    prev = None

    def day_of(w):
        return time.strftime("%m-%d %a", time.gmtime(w))
    for r in journal:
        w = float(r.get("wall") or 0)
        if w <= 0:
            continue
        D = days.setdefault(day_of(w), {"orders": 0, "fills": 0, "venue": 0, "mk": [], "pnl_c": 0.0, "sh": 0.0,
                                        "settled": 0, "sum_sh": 0.0, "signals": 0, "summaries": 0, "code": [],
                                        "rounds": set(), "fill_rounds": collections.Counter()})
        res = r.get("result")
        if res == "start":
            prev = None
            if r.get("code") and r.get("code") not in D["code"]:
                D["code"].append(r.get("code"))
        elif res == "summary":
            D["summaries"] += 1
            if prev is not None:
                D["signals"] += max(0, int(r.get("signals") or 0) - int(prev.get("signals") or 0))
            prev = r
        elif res in ("filled", "refused") and not r.get("live"):
            D["orders"] += 1
            D["rounds"].add(r.get("round_end"))
            if isinstance(r.get("arrival"), dict):
                D["venue"] += 1
            if res == "filled":
                D["fills"] += 1
                D["fill_rounds"][r.get("round_end")] += 1
                sh = float(r.get("shares") or 0)
                D["sum_sh"] += sh
                mk = (marks.get(r.get("order_id")) or {}).get("markout_5s_c")
                if mk is not None and sh > 0:
                    D["mk"].append((float(mk), sh))
                p = pay.get(r.get("order_id"))
                if p is not None and sh > 0:
                    D["pnl_c"] += (sh * p - float(r.get("cost") or 0)) * 100
                    D["sh"] += sh
                    D["settled"] += 1
    if not days:
        print("no journal rows")
        return 2
    print("DAY BY DAY (UTC), whole journal. model: old = paper filled on the book in our hands; venue = on "
          "Polymarket's book at the match instant (7dc208b, 10-02)")
    print(f"  {'day':10s} {'model':6s} {'up h':>5s} {'signals/h':>9s} {'orders':>6s} {'ord/h':>6s} {'fills':>5s} "
          f"{'fill%':>5s} {'fills/round':>11s} {'5s c/sh':>8s} {'settled $':>9s} {'c/sh':>6s}  code")
    per_model = collections.defaultdict(lambda: collections.Counter())
    for d, D in days.items():
        up = D["summaries"] / 12.0
        model = "venue" if D["orders"] and D["venue"] > D["orders"] / 2 else ("old" if D["orders"] else "-")
        mk = (sum(m * s for m, s in D["mk"]) / sum(s for _m, s in D["mk"])) if D["mk"] else None
        cps = D["pnl_c"] / D["sh"] if D["sh"] else None
        fr = sorted(D["fill_rounds"].values())
        med = fr[len(fr) // 2] if fr else 0
        print(f"  {d:10s} {model:6s} {up:5.1f} {D['signals'] / up if up else 0:9.0f} {D['orders']:6d} "
              f"{D['orders'] / up if up else 0:6.1f} {D['fills']:5d} {100 * D['fills'] / max(1, D['orders']):5.0f} "
              f"{med:11d} {'' if mk is None else f'{mk:+.2f}':>8s} {D['pnl_c'] / 100:+9.2f} "
              f"{'' if cps is None else f'{cps:+.2f}':>6s}  {','.join(c for c in D['code'] if c)}")
        if model != "-":
            M = per_model[model]
            for k in ("orders", "fills", "settled"):
                M[k] += D[k]
            M["up"] += up
            M["pnl_c"] += D["pnl_c"]
            M["sh"] += D["sh"]
            M["sum_sh"] += D["sum_sh"]
            M["signals"] += D["signals"]
    print("\nWHERE THE DAILY $ COMES FROM:  $/day = orders/day x fill rate x shares/fill x settled c/share")
    for model, M in per_model.items():
        if not M["up"]:
            continue
        opd = M["orders"] / M["up"] * 24
        fr = M["fills"] / max(1, M["orders"])
        spf = M["sum_sh"] / max(1, M["fills"])
        cps = M["pnl_c"] / M["sh"] if M["sh"] else 0.0
        print(f"  {model:5s} ({M['up']:5.1f}h up): {opd:7.0f} orders/day x {fr:4.0%} x {spf:4.1f} sh x "
              f"{cps:+5.2f}c = ${opd * fr * spf * cps / 100:+8.2f}/day   (signals/h {M['signals'] / M['up']:.0f})")
    return 0


def btc_minutes(start_s: float, end_s: float) -> dict:
    """{UTC hour start: [1m range in bp, ...]} from Binance's public klines,
    so the bot's signal count can be set beside what BTC actually did."""
    import time
    import urllib.request
    out = collections.defaultdict(list)
    t = int(start_s * 1000)
    while t < end_s * 1000:
        url = f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&startTime={t}&limit=1000"
        batch = json.loads(urllib.request.urlopen(url, timeout=20).read())
        if not batch:
            break
        for k in batch:
            o, h, lo = float(k[1]), float(k[2]), float(k[3])
            out[int(k[0] / 1000) // 3600 * 3600].append((h - lo) / o * 1e4)
        t = batch[-1][0] + 60_000
        time.sleep(0.2)
    return out


def hourly(env_path: str, last_h: float = 48.0, live: bool = False) -> int:
    """Per UTC hour: BTC's own 1-minute volatility (Binance) beside the
    sniper's signals, cooldown skips, decisions and their skip reasons,
    orders, fills - with restarts marked. A pipeline that broke shows as
    signals stepping down at a restart while BTC kept moving; a quiet market
    shows both falling together."""
    import time
    env = sc.read_env(Path(env_path))
    if live:                                            # the live canary's own journal
        journal = sc.jsonl(sc.state_path(env, "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl"))
    else:
        audit_path = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
        journal = sc.jsonl(audit_path.with_name(f"{audit_path.stem}_sniper.jsonl"))
    since = time.time() - last_h * 3600
    H = collections.OrderedDict()
    prev = None
    for r in journal:
        w = float(r.get("wall") or 0)
        res = r.get("result")
        if res == "start":
            prev = None
        if w < since:
            if res == "summary":
                prev = r
            continue
        h = int(w) // 3600 * 3600
        B = H.setdefault(h, {"signals": 0, "cooldown": 0, "inflight": 0, "summaries": 0, "orders": 0, "fills": 0,
                             "skips": collections.Counter(), "restart": []})
        if res == "start":
            B["restart"].append(str(r.get("code") or "?"))
        elif res == "summary":
            B["summaries"] += 1
            if prev is not None:
                B["signals"] += max(0, int(r.get("signals") or 0) - int(prev.get("signals") or 0))
                sk, pk = r.get("skips") or {}, prev.get("skips") or {}
                B["cooldown"] += max(0, int(sk.get("cooldown") or 0) - int(pk.get("cooldown") or 0))
                B["inflight"] += max(0, int(sk.get("previous order in flight") or 0)
                                     - int(pk.get("previous order in flight") or 0))
            prev = r
        elif res == "skipped":
            B["skips"][str(r.get("reason"))] += 1
        elif res in ("filled", "refused", "pending") and bool(r.get("live")) == live:
            B["orders"] += 1
            B["fills"] += res == "filled"
    if not H:
        print("no journal rows in the last", last_h, "hours")
        return 2
    try:
        vol = btc_minutes(min(H), max(H) + 3600)
    except Exception as exc:  # noqa: BLE001 - the table still prints without it
        print(f"(Binance klines unavailable: {type(exc).__name__}; BTC columns blank)")
        vol = {}
    print("HOUR BY HOUR (UTC): BTC's own 1-minute volatility (Binance) beside the sniper's pipeline")
    print("  signals = BTC moves >=1bp in 250ms seen by the bot; cooldown = signals inside 1s of the last decision;"
          " decisions = reached the order checks")
    print("  the hour with a restart undercounts signals and cooldown (the counters start again from zero)")
    print(f"  {'hour':11s} {'BTC 1m bp':>9s} {'min>=5bp':>8s} {'signals':>8s} {'cooldown':>8s} {'in-flight':>9s} "
          f"{'decisions':>9s} {'orders':>6s} {'fills':>5s} {'fill%':>5s}  skips (journalled)  restart")
    for h, B in H.items():
        v = vol.get(h, [])
        rng = sum(v) / len(v) if v else None
        big = sum(1 for x in v if x >= 5) if v else None
        dec = B["orders"] + sum(B["skips"].values())
        sk = ", ".join(f"{k} {n}" for k, n in B["skips"].most_common(3))
        print(f"  {time.strftime('%m-%d %H:00', time.gmtime(h))} {'' if rng is None else f'{rng:.2f}':>9s} "
              f"{'' if big is None else big:>8} {B['signals']:8d} {B['cooldown']:8d} {B['inflight']:9d} {dec:9d} "
              f"{B['orders']:6d} {B['fills']:5d} {100 * B['fills'] / max(1, B['orders']):5.0f}  {sk[:60]:60s}"
              f"  {'<- restart ' + ','.join(B['restart']) if B['restart'] else ''}")
    return 0


def gates(env_path: str, hours: float | None) -> int:
    """The bot's own gate decisions in detail, from its journal: per skip
    reason, when in the round it happened (seconds left), how far outside
    the price band, how BTC moved; and the EFFECTIVE settings each run
    started with (the settings line the process itself wrote), so a changed
    runtime value shows even when the strategy replay is identical."""
    import time
    env = sc.read_env(Path(env_path))
    audit_path = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
    journal = sc.jsonl(audit_path.with_name(f"{audit_path.stem}_sniper.jsonl"))
    since = time.time() - hours * 3600 if hours else 0.0
    rows = [r for r in journal if float(r.get("wall") or 0) >= since]
    print("EFFECTIVE SETTINGS at each start (written by the running process):")
    last = None
    for r in [r for r in journal if r.get("result") == "start"]:
        st = r.get("settings")
        mark = "" if st == last else "   <- CHANGED" if last else ""
        print(f"  {time.strftime('%m-%d %H:%M', time.gmtime(float(r['wall'])))}  code {str(r.get('code')):9s} "
              f"switch {r.get('switch_ms', '-')}ms live={r.get('live', False)}{mark}")
        if st != last:
            print(f"      {st}")
        last = st
    sk = [r for r in rows if r.get("result") == "skipped"]
    dec = [r for r in rows if r.get("result") in ("skipped", "filled", "refused")]
    print(f"\nGATES in scope: {len(dec)} decisions, {len(sk)} skipped, "
          f"{len(dec) - len(sk)} orders")
    reasons = collections.Counter(r.get("reason") for r in sk)
    edges = [0, 10, 30, 60, 120, 180, 240, 290, 301]
    print(f"  {'reason':34s} {'n':>5s}   by seconds left in the round: " +
          " ".join(f"{a}-{b}" for a, b in zip(edges, edges[1:])))
    for why, n in reasons.most_common():
        hist = collections.Counter()
        for r in sk:
            if r.get("reason") == why and r.get("left_s") is not None:
                for a, b in zip(edges, edges[1:]):
                    if a <= float(r["left_s"]) < b:
                        hist[a] += 1
        print(f"  {str(why)[:34]:34s} {n:>5d}   " + " ".join(f"{hist.get(a, 0):>{len(f'{a}-{b}')}d}"
                                                       for a, b in zip(edges, edges[1:])))
    band = [r for r in sk if r.get("reason") == "ask outside price band" and r.get("ask_seen") is not None]
    if band:
        asks = collections.Counter()
        for r in band:
            a = float(r["ask_seen"])
            asks["< 0.02" if a < 0.02 else "0.02-0.05" if a < 0.05 else "0.95-0.98" if a <= 0.98 else "> 0.98"] += 1
        print("  price band skips by ask: " + ", ".join(f"{k} {v}" for k, v in sorted(asks.items())))
    cap = [r for r in sk if r.get("reason") == "round cap"]
    if cap:
        per_round = collections.Counter(r.get("round_end") for r in cap)
        print(f"  round cap: {len(cap)} skips in {len(per_round)} rounds (max {max(per_round.values())} in one round)")
    bnl = [r for r in sk if r.get("reason") == "book not live"]
    if bnl:
        per_round = collections.Counter(r.get("round_end") for r in bnl)
        early = sum(1 for r in bnl if r.get("left_s") is not None and float(r["left_s"]) > 290)
        print(f"  book not live: {len(bnl)} skips in {len(per_round)} rounds; {early} in a round's first 10s "
              f"(the new round's book not yet in); max {max(per_round.values())} in one round")
    bp = collections.Counter()
    for r in dec:
        b = abs(float(r.get("bp") or 0))
        bp["1-1.5bp" if b < 1.5 else "1.5-2bp" if b < 2 else "2-3bp" if b < 3 else "3bp+"] += 1
    print("  decisions by BTC move: " + ", ".join(f"{k} {v}" for k, v in sorted(bp.items())))
    return 0


def by_feed(env_path: str, hours: float | None, live: bool = False) -> int:
    """Does the second trigger add good orders or noise? Orders, fill rate
    and markouts per feed that fired them, plus the paper shadow at 100ms
    and 200ms on the same orders, and the signal counts per feed."""
    import math
    import statistics
    import time
    env = sc.read_env(Path(env_path))
    if live:
        path = sc.state_path(env, "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl")
    else:
        audit_path = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
        path = audit_path.with_name(f"{audit_path.stem}_sniper.jsonl")
    journal = sc.jsonl(path)
    starts = [r for r in journal if r.get("result") == "start"]
    since = (time.time() - hours * 3600 if hours else float(starts[-1]["wall"]) if starts else 0.0)
    rows = [r for r in journal if float(r.get("wall") or 0) >= since]
    print(f"journal : {path}" + chr(10) + f"scope   : {'last %gh' % hours if hours else 'since the last start'}"
          + (chr(10) + f"settings: {starts[-1].get('settings')}" if starts else ""))
    marks = {r.get("order_id"): r for r in rows if r.get("result") == "markout"}
    shadows = {r.get("id"): r.get("L") or {} for r in rows if r.get("result") == "shadow"}
    dec = [r for r in rows if r.get("result") in ("filled", "refused", "pending")]

    def stat(xs):
        xs = [x for x in xs if x is not None]
        if not xs:
            return "-"
        m = statistics.fmean(xs)
        if len(xs) < 2:
            return f"{m:+.2f} (n=1)"
        se = statistics.stdev(xs) / math.sqrt(len(xs))
        return f"{m:+.2f} +-{se:.2f} t={m / se if se else 0:+.1f} n={len(xs)}"
    print(chr(10) + f"BY FEED ({len(dec)} orders)")
    print(f"  {'feed':9s} {'orders':>6s} {'filled':>6s} {'fill':>5s}  {'markout 1s c/sh':24s} {'markout 5s c/sh':24s} "
          f"{'shadow 100ms fill':>17s} {'shadow 200ms fill':>17s}")
    feeds = collections.defaultdict(list)
    for r in dec:
        feeds[r.get("feed") or "binance"].append(r)
    for feed, rs in sorted(feeds.items()):
        filled = [r for r in rs if r.get("result") == "filled"]
        m1 = [(marks.get(r.get("order_id")) or {}).get("markout_1s_c") for r in filled]
        m5 = [(marks.get(r.get("order_id")) or {}).get("markout_5s_c") for r in filled]

        def sh(L):
            got = [shadows[r["id"]].get(L) for r in rs if r.get("id") in shadows and shadows[r["id"]].get(L)]
            return f"{100 * sum('sh' in g for g in got) / len(got):.0f}% of {len(got)}" if got else "-"
        print(f"  {feed:9s} {len(rs):>6d} {len(filled):>6d} {100 * len(filled) / max(1, len(rs)):4.0f}%  "
              f"{stat(m1):24s} {stat(m5):24s} {sh('100'):>17s} {sh('200'):>17s}")
    summ = [r for r in rows if r.get("result") == "summary"]
    if summ:
        last = summ[-1]
        print(chr(10) + f"  signals by feed (since the bot started): {last.get('signals_by_feed') or {'binance': last.get('signals')}}")
        print(f"  fired by feed: {last.get('fired_by_feed')}; cooldown skips: "
              f"{ {k: v for k, v in (last.get('skips') or {}).items() if k.startswith('cooldown')} }")
    print(chr(10) + "  read it as: the second feed earns its place only if its own orders' markouts are positive in a"
          " weekday sample, without lowering Binance's - orders it fires are moves Binance had not signalled yet.")
    return 0


def ratio(items):
    r, se, _g = sc.cluster_ratio(items)
    if r is None:
        return "      -      "
    return f"{r:+5.1f} (t{r / se:+.1f})" if se else f"{r:+5.1f}        "


def cols(sub):
    out = []
    for h in ("A", "B"):
        part = [f for f in sub if f["half"] == h]
        out.append((ratio([(f["g"], f["mk5"] * f["sh"], f["sh"]) for f in part if f["mk5"] is not None]),
                    ratio([(f["g"], f["pnl_c"], f["sh"]) for f in part])))
    return out


def table(title, fills, key, order):
    print(f"\n{title}")
    print(f"  {'':<14}{'fills':>6}   {'5s  A':<14}{'5s  B':<14}   {'settled A':<14}{'settled B':<14}{'P&L':>8}")
    groups = collections.defaultdict(list)
    for f in fills:
        b = key(f)
        if b is not None:
            groups[b].append(f)
    for label in order:
        sub = groups.get(label)
        if not sub:
            continue
        (ma, sa), (mb, sb) = cols(sub)
        money = f"${sum(f['pnl_c'] for f in sub) / 100:+.0f}"
        print(f"  {label:<14}{len(sub):>6}   {ma:<14}{mb:<14}   {sa:<14}{sb:<14}{money:>8}")


def bucket(x, edges, fmt):
    for lo, hi in zip(edges, edges[1:]):
        if lo <= x < hi:
            return fmt(lo, hi)
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true", help="the whole history (default: since the last restart)")
    ap.add_argument("--hours", type=float, default=None, help="only the last N hours")
    ap.add_argument("--env", default=str(sc.ROOT / ".env"))
    ap.add_argument("--daily", action="store_true", help="one row per UTC day over the whole journal")
    ap.add_argument("--hourly", action="store_true",
                    help="per UTC hour (last 48h, or --hours N) beside BTC's own volatility")
    ap.add_argument("--gates", action="store_true",
                    help="every skip reason in detail + the effective settings at each start")
    ap.add_argument("--by-feed", action="store_true",
                    help="orders, fills and markouts split by the feed that fired them")
    ap.add_argument("--live", action="store_true", help="with --by-feed or --hourly: the live canary's journal")
    args = ap.parse_args()
    if args.by_feed:
        return by_feed(args.env, args.hours, args.live)
    if args.gates:
        return gates(args.env, args.hours)
    if args.daily:
        return daily(args.env)
    if args.hourly:
        return hourly(args.env, args.hours or 48.0, args.live)
    fills, scope, rounds = load(args.env, args.all, args.hours)
    if len(rounds) < 20:
        print(f"only {len(fills)} settled fills in {len(rounds)} rounds - let it run longer")
        return 2
    a = [f for f in fills if f["half"] == "A"]
    b = [f for f in fills if f["half"] == "B"]
    print(f"scope   : {scope}")
    print(f"fills   : {len(fills)} settled in {len(rounds)} rounds - half A {len(a)} fills, half B {len(b)} fills")
    print(f"total   : ${sum(f['pnl_c'] for f in fills) / 100:+.2f}   A ${sum(f['pnl_c'] for f in a) / 100:+.2f}"
          f"   B ${sum(f['pnl_c'] for f in b) / 100:+.2f}")

    price_edges = [0.0, 0.20, 0.35, 0.50, 0.65, 0.80, 1.01]
    table("ENTRY PRICE (the ask when it decided)", fills,
          lambda f: bucket(f["ask"], price_edges, lambda lo, hi: f"{lo:.2f}-{min(hi, 1):.2f}"),
          [f"{lo:.2f}-{min(hi, 1):.2f}" for lo, hi in zip(price_edges, price_edges[1:])])
    table("FAVOURITE OR UNDERDOG", fills,
          lambda f: "favourite >0.5" if f["ask"] > 0.5 else "underdog <=0.5", ["underdog <=0.5", "favourite >0.5"])
    left_edges = [0, 60, 120, 180, 240, 301]
    table("SECONDS LEFT IN THE ROUND", fills,
          lambda f: bucket(f["left"], left_edges, lambda lo, hi: f"{lo}-{min(hi, 300)}s"),
          [f"{lo}-{min(hi, 300)}s" for lo, hi in zip(left_edges, left_edges[1:])])
    bp_edges = [0, 1.25, 1.5, 2.0, 3.0, 1e9]
    table("SIZE OF THE BTC MOVE (bp within 250ms)", fills,
          lambda f: bucket(f["bp"], bp_edges, lambda lo, hi: f"{lo:g}-{hi:g}bp" if hi < 1e9 else f"{lo:g}+bp"),
          [f"{lo:g}-{hi:g}bp" if hi < 1e9 else f"{lo:g}+bp" for lo, hi in zip(bp_edges, bp_edges[1:])])
    k_edges = [1, 2, 4, 8, 13, 21]
    table("WHICH FILL OF THE ROUND", fills,
          lambda f: bucket(f["k"], k_edges, lambda lo, hi: f"#{lo}" if hi - lo == 1 else f"#{lo}-{hi - 1}"),
          [f"#{lo}" if hi - lo == 1 else f"#{lo}-{hi - 1}" for lo, hi in zip(k_edges, k_edges[1:])])
    table("SAME SIDE OR FLIP (vs the round's previous fill)", fills,
          lambda f: "first of round" if f["k"] == 1 else ("flip" if f["flip"] else "same side"),
          ["first of round", "same side", "flip"])
    gap_edges = [0, 3, 10, 30, 1e9]
    table("SECONDS SINCE THE PREVIOUS FILL", fills,
          lambda f: None if f["gap"] is None else bucket(
              f["gap"], gap_edges, lambda lo, hi: f"{lo:g}-{hi:g}s" if hi < 1e9 else f"{lo:g}s+"),
          [f"{lo:g}-{hi:g}s" if hi < 1e9 else f"{lo:g}s+" for lo, hi in zip(gap_edges, gap_edges[1:])])
    ms_edges = [0, 205, 220, 240, 270, 1e9]
    table("OUR SPEED (BTC tick -> order met the book)", [f for f in fills if f["match_ms"] is not None],
          lambda f: bucket(float(f["match_ms"]), ms_edges,
                           lambda lo, hi: f"<{hi:g}ms" if lo == 0 else (f"{lo:g}ms+" if hi >= 1e9 else f"{lo:g}-{hi:g}ms")),
          [f"<{hi:g}ms" if lo == 0 else (f"{lo:g}ms+" if hi >= 1e9 else f"{lo:g}-{hi:g}ms")
           for lo, hi in zip(ms_edges, ms_edges[1:])])

    # ---------------------------------------------------------------- rules
    rules = [
        ("max 4 fills per round", lambda f: f["k"] > 4),
        ("max 8 fills per round", lambda f: f["k"] > 8),
        ("max 12 fills per round", lambda f: f["k"] > 12),
        ("max 16 fills per round", lambda f: f["k"] > 16),
        ("skip asks below 0.20", lambda f: f["ask"] < 0.20),
        ("skip asks above 0.80", lambda f: f["ask"] > 0.80),
        ("skip the underdog (<=0.5)", lambda f: f["ask"] <= 0.5),
        ("skip the favourite (>0.5)", lambda f: f["ask"] > 0.5),
        ("skip the last 60s", lambda f: f["left"] < 60),
        ("skip the first 60s", lambda f: f["left"] >= 240),
        ("only BTC moves >= 1.5bp", lambda f: f["bp"] < 1.5),
        ("never flip sides in a round", lambda f: f["flip"]),
        ("wait 3s after a fill", lambda f: f["gap"] is not None and f["gap"] < 3),
    ]
    print("\nWHAT-IF RULES - the fills a rule would have skipped, and what they made")
    print("  (a rule helps only if the skipped fills LOST money in BOTH halves; the c/share columns")
    print("   show how strongly - a weak one is more likely luck)")
    print(f"  {'rule':<28}{'skips':>6}  {'skipped A':>10}{'skipped B':>11}   {'their 5s c/sh':<14}"
          f"{'their settled':<14} verdict")
    for name, drop in rules:
        gone = [f for f in fills if drop(f)]
        if not gone:
            continue
        da = sum(f["pnl_c"] for f in gone if f["half"] == "A") / 100
        db = sum(f["pnl_c"] for f in gone if f["half"] == "B") / 100
        verdict = ("HELPS in both halves - watch it" if da < 0 and db < 0 else
                   "hurts in both halves" if da > 0 and db > 0 else "mixed - noise so far")
        m5 = ratio([(f["g"], f["mk5"] * f["sh"], f["sh"]) for f in gone if f["mk5"] is not None])
        st = ratio([(f["g"], f["pnl_c"], f["sh"]) for f in gone])
        print(f"  {name:<28}{len(gone):>6}  {'$' + format(da, '+.2f'):>10}{'$' + format(db, '+.2f'):>11}   "
              f"{m5:<14} {st:<14} {verdict}")
    print("\nA rule that helps in both halves is a candidate, not a change: it has to keep")
    print("helping on the days AFTER this report before it earns a place in the bot.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
