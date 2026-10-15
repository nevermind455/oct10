#!/usr/bin/env python3
"""Where the live sniper's money went, trade by trade. READ-ONLY.

    .venv/bin/python live_pnl.py              # the live ledger + live journal
    .venv/bin/python live_pnl.py --paper      # the same report for paper
    .venv/bin/python live_pnl.py --hours 24   # only trades from the last 24h

For every order that bought something it joins what the bot decided (the
journal: BTC move, ask, fair value, cap, Kelly size) with what happened (the
ledger: shares, price, fee, and - once the market resolved - won or lost),
and the price 5s after the fill (markout). Then:

  - PnL: total, by side, by entry price;
  - CALIBRATION: the model said each bought side wins with probability `fair`;
    the sum of those is the wins it expected. Fewer real wins than that,
    beyond normal luck (2 sigma), means the fair value is too optimistic;
  - EDGE: fair - (price paid + fee per share), what the model thought each
    share was worth above its cost; MARKOUT: did the market agree 5s later;
  - the worst trades.

It changes nothing and prints no key, credential or address.
"""
from __future__ import annotations

import argparse
import collections
import json
import math
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def state_path(env: str, default: str) -> Path:
    raw = os.environ.get(env) or default
    p = Path(raw)
    return p if p.is_absolute() else ROOT / p


def rows_of(path: Path) -> list:
    out = []
    if not path.exists():
        return out
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            try:
                out.append(json.loads(line))
            except ValueError:
                pass
    return out


def f(x, nd=3):
    return "-" if x is None else f"{x:.{nd}f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--paper", action="store_true", help="the paper ledger and journal instead of live")
    ap.add_argument("--hours", type=float, default=None, help="only orders from the last N hours")
    ap.add_argument("--list", type=int, default=25, help="how many trades to list (newest first)")
    args = ap.parse_args()
    import config  # noqa: F401 - loads .env into the environment, as the bot does
    from accounting.ledger import Ledger

    if args.paper:
        audit = state_path("PAPER_AUDIT_PATH", "paper_orders.jsonl")
        journal = audit.with_name(f"{audit.stem}_sniper.jsonl")
        ledger_path = state_path("PAPER_LEDGER_PATH", "paper_ledger.json")
    else:
        journal = state_path("LIVE_SNIPER_JOURNAL", "live_sniper.jsonl")
        ledger_path = state_path("LEDGER_PATH", "ledger.json")
    rows = rows_of(journal)
    since = time.time() - args.hours * 3600 if args.hours else 0.0
    decided, marks = {}, {}
    for r in rows:
        oid = r.get("order_id")
        if not oid:
            continue
        if r.get("result") in ("filled", "pending") and r.get("side") and r.get("bp") is not None:
            decided.setdefault(oid, r)
        elif r.get("result") == "markout":
            marks[oid] = r

    led = Ledger(path=str(ledger_path), category="crypto")
    if not led.load():
        print(f"ledger {ledger_path.name} did not load ({led.last_persistence_error})")
        return 1
    trades = collections.OrderedDict()
    with led._lock:
        for pos in led.positions.values():
            for lot in pos.lots:
                # live lots carry the venue order id; a paper fill's id is its lot's trade id
                key = getattr(lot, "order_id", None) or (lot.trade_id if args.paper else None)
                if str(lot.side).upper() != "BUY" or not key:
                    continue
                t = trades.setdefault(key, {"shares": 0.0, "notional": 0.0, "fee": 0.0, "wall": lot.wall,
                                                     "settled": pos.settled, "payout": pos.payout_per_share,
                                                     "token": pos.token_id})
                t["shares"] += float(lot.shares)
                t["notional"] += float(lot.shares) * float(lot.price)
                t["fee"] += float(lot.fee or 0.0)
                t["wall"] = min(t["wall"], lot.wall)
    out = []
    for oid, t in trades.items():
        d = decided.get(oid, {})
        wall = float(d.get("wall") or t["wall"] or 0)
        if wall < since or (not args.paper and not d.get("live", True)):
            continue
        cost = t["notional"] + t["fee"]
        price = t["notional"] / t["shares"] if t["shares"] else None
        fee_ps = t["fee"] / t["shares"] if t["shares"] else 0.0
        fair = d.get("fair")
        won = None if not t["settled"] or t["payout"] is None else float(t["payout"]) >= 0.5
        pnl = (t["shares"] * float(t["payout"]) - cost) if won is not None else None
        out.append({"oid": oid, "wall": wall, "side": d.get("side", "?"), "bp": d.get("bp"),
                    "ask": d.get("ask_seen"), "cap": d.get("cap"), "fair": fair, "mid": d.get("market_mid"),
                    "left": d.get("left_s"), "target": d.get("target_shares"), "shares": t["shares"],
                    "price": price, "cost": cost, "won": won, "pnl": pnl,
                    "edge": (fair - price - fee_ps) if fair is not None and price else None,
                    "m5": (marks.get(oid) or {}).get("markout_5s_c"), "feed": d.get("feed")})
    out.sort(key=lambda x: x["wall"])
    kind = "PAPER" if args.paper else "LIVE"
    print(f"{kind}: journal {journal.name}, ledger {ledger_path.name}"
          + (f", last {args.hours:g}h" if args.hours else ", all time"))
    if not out:
        print("no filled orders found")
        return 0
    done = [x for x in out if x["won"] is not None]
    total_cost = sum(x["cost"] for x in out)
    pnl = sum(x["pnl"] for x in done)
    wins = sum(1 for x in done if x["won"])
    print(f"\nTRADES {len(out)} filled, {len(done)} resolved ({len(out) - len(done)} still open), "
          f"spent ${total_cost:.2f}")
    if done:
        print(f"  PnL on resolved: ${pnl:+.2f}  ({pnl / sum(x['cost'] for x in done):+.1%} of what they cost); "
              f"won {wins}/{len(done)} = {wins / len(done):.0%}")
        for side in ("UP", "DOWN"):
            xs = [x for x in done if x["side"] == side]
            if xs:
                print(f"  {side:4s}: {len(xs)} trades, won {sum(1 for x in xs if x['won'])}, "
                      f"PnL ${sum(x['pnl'] for x in xs):+.2f}")
        buckets = collections.defaultdict(list)
        for x in done:
            if x["price"] is not None:
                buckets[min(int(x["price"] * 10), 9)].append(x)
        print("  by price paid:")
        for b in sorted(buckets):
            xs = buckets[b]
            print(f"    {b / 10:.1f}-{(b + 1) / 10:.1f}: {len(xs):3d} trades, won {sum(1 for x in xs if x['won']):3d} "
                  f"({sum(1 for x in xs if x['won']) / len(xs):.0%}; break-even ~{statistics_mean([x['price'] for x in xs]):.0%}), "
                  f"PnL ${sum(x['pnl'] for x in xs):+.2f}")
        cal = [x for x in done if x["fair"] is not None]
        if cal:
            exp = sum(x["fair"] for x in cal)
            sd = math.sqrt(sum(x["fair"] * (1 - x["fair"]) for x in cal)) or 1.0
            act = sum(1 for x in cal if x["won"])
            z = (act - exp) / sd
            print(f"\nCALIBRATION on {len(cal)} resolved trades with a fair value:")
            print(f"  the model expected {exp:.1f} wins (avg fair {exp / len(cal):.0%}); actual {act} "
                  f"({act / len(cal):.0%}); difference {z:+.1f} sigma")
            print("  " + ("within normal luck for this many trades" if abs(z) < 2 else
                          "fewer wins than the model's fair value can explain: it is too optimistic" if z < 0 else
                          "more wins than the model expected"))
            avg_paid = statistics_mean([x["price"] for x in cal if x["price"]])
            print(f"  avg fair {exp / len(cal):.3f} vs avg price paid {avg_paid:.3f}: the model's claimed edge "
                  f"{statistics_mean([x['edge'] for x in cal if x['edge'] is not None]) * 100:+.1f}c/share")
    if done:
        breakdown("BY CLAIMED EDGE (fair - price - fee, c/share)", done,
                  lambda x: None if x["edge"] is None else ("<2c" if x["edge"] < 0.02 else "2-5c" if x["edge"] < 0.05
                                                            else "5-10c" if x["edge"] < 0.10 else "10c+"),
                  ("<2c", "2-5c", "5-10c", "10c+"))
        breakdown("BY BTC MOVE", done,
                  lambda x: None if x["bp"] is None else ("<1bp" if abs(x["bp"]) < 1 else "1-2bp" if abs(x["bp"]) < 2
                                                          else "2bp+"),
                  ("<1bp", "1-2bp", "2bp+"))
        breakdown("BY TIME LEFT IN THE ROUND", done,
                  lambda x: None if x["left"] is None else ("<60s" if x["left"] < 60 else "60-150s" if x["left"] < 150
                                                            else "150s+"),
                  ("<60s", "60-150s", "150s+"))
        print("  a group whose wins fall well under 'expected' (sigma <= -2) is where the fair value overrates its edge")
    m5 = [x["m5"] for x in out if x["m5"] is not None]
    if m5:
        print(f"\nMARKOUT 5s after the fill (market mid - price - fee, c/share): avg {statistics_mean(m5):+.1f}, "
              f"positive on {sum(1 for v in m5 if v > 0)}/{len(m5)}")
        print("  negative = the market moved against the buy right after it (we bought late / at a stale price)")
    worst = sorted([x for x in done], key=lambda x: x["pnl"])[:5]
    if worst:
        print("\nWORST:")
        for x in worst:
            print(f"  {time.strftime('%m-%d %H:%M:%S', time.gmtime(x['wall']))} {x['side']:4s} "
                  f"BTC {f(x['bp'], 2)}bp ask {f(x['ask'], 2)} fair {f(x['fair'], 3)} paid {f(x['price'], 3)} "
                  f"x{x['shares']:.2f} -> {'WON' if x['won'] else 'lost'} ${x['pnl']:+.2f}")
    print(f"\nLAST {min(args.list, len(out))} (UTC):")
    print(f"  {'time':14s} {'side':4s} {'BTC bp':>6s} {'ask':>5s} {'fair':>6s} {'paid':>6s} {'edge c':>6s} "
          f"{'shares':>7s} {'cost $':>7s} {'5s mk':>6s} {'result':>7s} {'PnL $':>7s}")
    for x in out[-args.list:][::-1]:
        res = "open" if x["won"] is None else ("WON" if x["won"] else "lost")
        edge = "-" if x["edge"] is None else f"{x['edge'] * 100:.1f}"
        pnl_txt = "-" if x["pnl"] is None else f"{x['pnl']:+.2f}"
        when = time.strftime("%m-%d %H:%M:%S", time.gmtime(x["wall"]))
        print(f"  {when:14s} {x['side']:4s} {f(x['bp'], 2):>6s} {f(x['ask'], 2):>5s} {f(x['fair'], 3):>6s} "
              f"{f(x['price'], 3):>6s} {edge:>6s} {x['shares']:7.2f} {x['cost']:7.2f} {f(x['m5'], 1):>6s} "
              f"{res:>7s} {pnl_txt:>7s}")
    return 0


def breakdown(title, done, key, order) -> None:
    """Resolved trades grouped by `key`: wins vs the model's expected wins, PnL,
    return per dollar and the 5s markout of each group."""
    groups = collections.defaultdict(list)
    for x in done:
        k = key(x)
        if k is not None:
            groups[k].append(x)
    if not groups:
        return
    print(f"\n{title}:")
    print(f"  {'group':>8s} {'trades':>6s} {'won':>5s} {'expected':>8s} {'sigma':>6s} {'PnL $':>8s} "
          f"{'per $1':>7s} {'5s mk c':>7s}")
    for label in order:
        xs = groups.get(label)
        if not xs:
            continue
        fx = [x for x in xs if x["fair"] is not None]
        exp = sum(x["fair"] for x in fx)
        sd = math.sqrt(sum(x["fair"] * (1 - x["fair"]) for x in fx)) or 1.0
        act = sum(1 for x in fx if x["won"])
        pnl = sum(x["pnl"] for x in xs)
        cost = sum(x["cost"] for x in xs) or 1.0
        m5 = statistics_mean([x["m5"] for x in xs])
        print(f"  {label:>8s} {len(xs):6d} {sum(1 for x in xs if x['won']):5d} {exp:8.1f} "
              f"{(act - exp) / sd if fx else float('nan'):+6.1f} {pnl:+8.2f} {pnl / cost:+7.1%} "
              f"{'-' if m5 != m5 else f'{m5:+.1f}':>7s}")


def statistics_mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else float("nan")


if __name__ == "__main__":
    raise SystemExit(main())
