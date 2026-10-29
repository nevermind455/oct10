import bisect, collections, json, math, os, re, statistics, sys, time
# Read-only: paper50 vs live on the SAME hours and the SAME signals. Stdlib only. On the VPS:
#   python3 -I /tmp/pvl.py [hours]          (default: the whole overlap of the two journals)
# Local: python3 -I pvl.py <live dir> <paper dir> [hours]
args = sys.argv[1:]
HOURS = float(args.pop()) if args and args[-1].replace(".", "", 1).isdigit() else None
LIVE_DIR, PAPER_DIR = args if len(args) == 2 else ("/opt/sniperbtc", "/opt/sniperbtc")
LJ, PJ = f"{LIVE_DIR}/live_sniper.jsonl", f"{PAPER_DIR}/paper_orders_sniper.jsonl"
t = lambda w: time.strftime("%m-%d %H:%M", time.gmtime(w))
q = lambda xs, p: sorted(xs)[min(len(xs) - 1, int(p * len(xs)))] if xs else float("nan")


def rows(p):
    with open(p, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if '"result"' not in line:
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if (isinstance(r, dict) and isinstance(r.get("wall"), (int, float))
                    and not isinstance(r["wall"], bool) and math.isfinite(r["wall"])):
                yield r


def winners(path):
    out = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "RESULT_RECEIVED" in line:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if (isinstance(r, dict) and isinstance(r.get("condition_id"), str)
                            and r.get("winner") in ("UP", "DOWN", "50/50")):
                        out[r["condition_id"]] = r["winner"]
    return out


def load(path, live):
    orders, skips, starts, span = {}, collections.defaultdict(list), [], [None, None]
    for r in rows(path):
        w, res = float(r["wall"]), r.get("result")
        span[0] = min(span[0], w) if span[0] is not None else w
        span[1] = max(span[1], w) if span[1] is not None else w
        update = live and res == "live_fill_update"
        if update:
            res = r.get("resolved_result")
        if res == "start":
            s = str(r.get("settings") or "")
            m = re.search(r"spot mid >= ([0-9.]+)bp", s)
            x = re.search(r"\+ (\d+)ms calibration", s)
            starts.append((w, m.group(1) if m else "?", ("delays from live" if "drawn from" in s else "model delay")
                           + (f" +{x.group(1)}ms" if x else "")))
        elif res == "skipped" and r.get("side"):
            skips[r["side"]].append((w, str(r.get("reason") or "?")[:48]))
        elif res in ("filled", "refused", "pending") and r.get("bp") is not None and r.get("side"):
            if live:
                if not update and r.get("kind") not in ("MATCHED", "NO_MATCH", "PENDING", "AMBIGUOUS"):
                    continue                              # not answered by the engine (pre-send refusals, unknowns)
            elif res == "refused" and not str(r.get("reason") or "").startswith(("FAK no-fill", "FOK no-fill")):
                continue                                  # paper's pre-send refusals: not an order
            key = r.get("id") or r.get("client_order_id") or r.get("order_id") or (w, r["side"])
            old = orders.get(key)
            shares, fee = float(r.get("shares") or 0), float(r.get("fee") or 0)
            # Paper's cost already includes its fee; normalize both to principal.
            cost = (shares * float(r["price"]) if r.get("price") is not None and shares > 0 else
                    float(r.get("cost") or 0) - (fee if not live else 0))
            orders[key] = {"w": old["w"] if old else w, "side": r["side"],
                           "cond": r.get("condition_id"),
                           "fill": None if res == "pending" or (live and not update and
                               r.get("kind") in ("PENDING", "AMBIGUOUS") and res != "filled") else res == "filled",
                           "sh": shares, "cost": cost,
                           "fee": float(r.get("fee") or 0), "ask": r.get("ask_seen"), "cap": r.get("cap"),
                           "osh": r.get("order_shares"), "bank": r.get("bankroll"), "bp": abs(float(r["bp"])),
                           "fair": r.get("fair"), "lat": r.get("t_wire_to_reply_ms") if live else r.get("latency_ms")}
    for v in skips.values():
        v.sort()
    return list(orders.values()), skips, starts, span


lo, so, ls, lspan = load(LJ, True)
po, sp, ps, pspan = load(PJ, False)
if None in lspan + pspan:
    print(f"empty journal: live {LJ} {lspan}, paper {PJ} {pspan}")
    sys.exit()
win = winners(f"{LIVE_DIR}/live_ledger_settlements.jsonl")
win.update(winners(f"{PAPER_DIR}/paper_ledger_settlements.jsonl"))
a, b = max(lspan[0], pspan[0]), min(lspan[1], pspan[1])
if HOURS:
    a = max(a, b - HOURS * 3600)
if a >= b:
    print(f"no overlap: live {t(lspan[0])}..{t(lspan[1])}, paper {t(pspan[0])}..{t(pspan[1])}")
    sys.exit()
lo = [o for o in lo if a <= o["w"] <= b]
po = [o for o in po if a <= o["w"] <= b]
print(f"== paper vs live, {t(a)} .. {t(b)} UTC ({(b - a) / 3600:.1f}h overlap)")
print("Journal outcome comparison, not a confirmed-ledger reconciliation or proof of live profitability.")


for name, st_ in (("live", ls), ("paper", ps)):
    run = [x for x in st_ if a <= x[0] <= b] + [x for x in st_ if x[0] < a][-1:]
    seen = sorted({(k, e) for _w, k, e in run})
    print(f"  {name:5s} settings in the window: " + "; ".join(
        f"{k}bp" + (f", {e}" if name == "paper" else "") for k, e in seen))


def pnl(o):
    if not o["fill"] or o["cond"] not in win or o["sh"] <= 0:
        return None
    winner = win[o["cond"]]
    if winner not in ("UP", "DOWN", "50/50"):
        return None
    payout = .5 if winner == "50/50" else float(winner == o["side"])
    return o["sh"] * payout - o["cost"] - o["fee"]


def line(name, g):
    f = [o for o in g if o["fill"]]
    s = [(o, pnl(o)) for o in f if pnl(o) is not None]
    p, spent, sh = sum(x for _, x in s), sum(o["cost"] + o["fee"] for o, _ in s), sum(o["sh"] for o, _ in s)
    paid = [100 * (o["cost"] / o["sh"] - float(o["ask"])) for o in f if o["sh"] > 0 and o["ask"] is not None]
    wins = sum(1 for o, _ in s if win[o["cond"]] == o["side"])
    exp = sum(float(o["fair"]) for o, _ in s if o["fair"] is not None)
    print(f"  {name:22s} orders {len(g):5d} filled {len(f):5d} ({100 * len(f) / len(g) if g else 0:4.1f}%)"
          f"  P&L ${p:+8.2f} ({100 * p / spent if spent else 0:+5.1f}%/$, {100 * p / sh if sh else 0:+5.1f}c/sh)"
          f"  wins {wins}/{exp:.1f} fair  paid {statistics.mean(paid) if paid else 0:+.1f}c vs ask"
          f"  cash~${q([float(o['bank']) for o in g if o['bank'] is not None], .5):.0f}"
          f"  order~{q([float(o['osh']) for o in g if o['osh'] is not None], .5):.0f}sh"
          f"  unresolved {sum(o['fill'] is None for o in g)}")


print("ALL ORDERS each bot sent (P&L on settled rounds):")
line("live", lo)
line("paper", po)
ld = [float(o["lat"]) for o in lo if o["lat"] is not None]
pd = [float(o["lat"]) for o in po if o["lat"] is not None]
print(f"  reply/model delay (ACK is not a match timestamp): live wire->reply p50 {q(ld, .5):.0f} p90 {q(ld, .9):.0f}ms | "
      f"paper modelled p50 {q(pd, .5):.0f} p90 {q(pd, .9):.0f}ms")

# the SAME signal: same round and side, sent within 1s of each other
by = collections.defaultdict(list)
for o in po:
    by[(o["cond"], o["side"])].append(o)
pairs, used, lonly = [], set(), []
for o in lo:
    cands = [p for p in by.get((o["cond"], o["side"]), []) if id(p) not in used and abs(p["w"] - o["w"]) <= 1.0]
    if cands:
        p = min(cands, key=lambda p: abs(p["w"] - o["w"]))
        used.add(id(p))
        pairs.append((o, p))
    else:
        lonly.append(o)
ponly = [p for p in po if id(p) not in used]
print(f"APPROXIMATE SAME SIGNAL (same round + side, decided within 1s): {len(pairs)} pairs")
if not pairs:
    print("NO MATCHED SIGNALS: the separate P&L totals cannot validate paper/live execution agreement.")
cats = collections.defaultdict(list)
for o, p in pairs:
    cats[("unresolved" if o["fill"] is None or p["fill"] is None else
          "both filled" if o["fill"] and p["fill"] else "PAPER ONLY filled" if p["fill"] else
          "live only filled" if o["fill"] else "neither filled")].append((o, p))
for c in ("both filled", "PAPER ONLY filled", "live only filled", "neither filled", "unresolved"):
    g = cats.get(c, [])
    lp = [pnl(o) for o, _ in g if pnl(o) is not None]
    pp = [pnl(p) for _, p in g if pnl(p) is not None]
    pps = [pnl(p) / p["sh"] * 100 for _, p in g if pnl(p) is not None and p["sh"] > 0]
    print(f"  {c:18s} {len(g):5d}  live P&L ${sum(lp):+8.2f}   paper P&L ${sum(pp):+8.2f}"
          f" ({statistics.mean(pps) if pps else 0:+.1f}c/sh)")
known = [(o, p) for o, p in pairs if o["fill"] is not None and p["fill"] is not None]
if known:
    pairs = known
    lf = sum(o["fill"] for o, _ in pairs) / len(pairs)
    pf = sum(p["fill"] for _, p in pairs) / len(pairs)
    print(f"  on the same signals: live filled {100 * lf:.0f}%, paper {100 * pf:.0f}%"
          f" -> paper agrees with live on {100 * sum(o['fill'] == p['fill'] for o, p in pairs) / len(pairs):.0f}%")
    ratio = [float(p["osh"]) / float(o["osh"]) for o, p in pairs if o["osh"] and p["osh"]]
    print(f"  order size paper/live: median x{q(ratio, .5):.2f} (p10 x{q(ratio, .1):.2f}, p90 x{q(ratio, .9):.2f})")
    po_ = [(o, p) for o, p in cats.get("PAPER ONLY filled", []) if o["lat"] is not None]
    if po_:
        print(f"  live's reply on the paper-only fills: p50 {q([float(o['lat']) for o, _ in po_], .5):.0f}ms"
              f" vs paper's modelled {q([float(p['lat']) for _, p in po_ if p['lat'] is not None], .5):.0f}ms")


def why(skips, side, w):
    v = skips.get(side, [])
    i = bisect.bisect_left(v, (w - 1.0, ""))
    near = [r for x, r in v[i:i + 20] if abs(x - w) <= 1.0]
    return near[0] if near else "no row within 1s"


for name, g, other in (("live sent, paper did not", lonly, sp), ("paper sent, live did not", ponly, so)):
    reasons = collections.Counter(why(other, o["side"], o["w"]) for o in g)
    pl = [pnl(o) for o in g if pnl(o) is not None]
    print(f"{name.upper()}: {len(g)} orders (their own P&L ${sum(pl):+.2f}); the other bot's reason:")
    for r_, n in reasons.most_common(6):
        print(f"    {n:5d}  {r_}")
