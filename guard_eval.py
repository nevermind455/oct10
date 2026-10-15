#!/usr/bin/env python3
"""Forward-only comparison of guard and limit alternatives against the
unchanged bot. Read-only: it sends nothing and changes no file.

    .venv/bin/python guard_eval.py                        # the paper journal
    .venv/bin/python guard_eval.py --live                 # the live canary's journal + ledger
    .venv/bin/python guard_eval.py --since "2026-10-15 12:00"   # orders sent from then on (UTC)
    .venv/bin/python guard_eval.py --dir /path/to/copy    # a downloaded copy of the files

The metric (settlement P&L, fees included):

    net incremental P&L per eligible signal
        = (alternative total P&L - baseline total P&L) / eligible signals

Alternatives:
  guards - drop the sent orders the guard would have rejected (from the bot's
           guard_diag rows; later orders are NOT resized - Kelly would size them
           a little differently):
    c1          the newly paired opposite-side shares cost C >= $1 (sure loss on them),
                against this round's CONFIRMED fills
    c1_pending  the same, counting opposite orders still in flight / awaiting
                their size (a whipsaw inside one reply time)
    cd500       an order within 500 ms of an opposite FILL, same round
    cd1000      the same, 1000 ms
    cdo500      an order within 500 ms of the opposite side's last ORDER, same round
    cdo1000     the same, 1000 ms
    net     netting the BTC moves (when the market had not repriced between the
            two decisions) leaves the ask above the fair limit
  limits - the shadow at each order's own delay, model against model:
    tight   ask + 3c, still under the fair limit (is the room above it worth having?)
    wide3   ask + 3c even past the fair limit, only where the real limit missed

Each is reported overall and per independent period (UTC day, 6-hour blocks).
PASS needs a positive result in EVERY period with at least --min-n eligible
signals; fewer is INSUFFICIENT, not a pass.
"""
from __future__ import annotations

import argparse
import collections
import glob
import json
import os
import time

GUARDS = ("c1", "c1_pending", "cd500", "cd1000", "cdo500", "cdo1000", "net")
LIMITS = ("tight", "wide3")


def _rows(path):
    out = []
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out


def winners(directory: str, ledger_path: str | None, token_side: dict) -> dict:
    """condition id -> winning side, from settlement journals and the ledger."""
    win = {}
    for p in glob.glob(os.path.join(directory, "*settlements*.jsonl")):
        for r in _rows(p):
            if r.get("event") == "RESULT_RECEIVED" and r.get("winner") and r.get("condition_id"):
                win[r["condition_id"]] = r["winner"]
    if ledger_path and os.path.exists(ledger_path):
        try:
            led = json.load(open(ledger_path, encoding="utf-8"))
        except (OSError, ValueError):
            led = {}
        for pos in (led.get("positions") or {}).values():
            if not isinstance(pos, dict) or not pos.get("settled"):
                continue
            tok, cond, pay = str(pos.get("token_id")), pos.get("condition_id"), pos.get("payout_per_share")
            if tok in token_side and cond and pay is not None:
                side = token_side[tok]
                win.setdefault(cond, side if float(pay) >= 0.5 else ("DOWN" if side == "UP" else "UP"))
    return win


def entry_at(shadow: dict, L: float) -> dict:
    keys = sorted(int(k) for k in shadow.get("L", {}))
    if not keys:
        return {}
    use = [k for k in keys if k <= L] or keys[:1]
    return shadow["L"][str(use[-1])]


def quote_fill(q: dict | None):
    """(shares, price, fee) of a shadow quote, FOK first, else what FAK kept."""
    if not q:
        return None
    if q.get("sh"):
        return float(q["sh"]), float(q["px"]), float(q["fee"])
    fak = q.get("fak") or {}
    if fak.get("sh"):
        return float(fak["sh"]), float(fak["px"]), float(fak["fee"])
    return None


def pnl(fill, side, win):
    sh, px, fee = fill
    return sh * ((1.0 if side == win else 0.0) - px) - fee


def load(directory, journal, ledger, since):
    rows = _rows(journal)
    dec, final, shadow, diag = {}, {}, {}, {}
    for r in rows:
        rid, res = r.get("id"), r.get("result")
        if not rid:
            continue
        if res in ("filled", "refused", "pending", "live_fill_update") and r.get("side"):
            if rid not in dec and r.get("bp") is not None:
                dec[rid] = r
            if res == "live_fill_update" and r.get("resolved_result"):
                final[rid] = dict(r, result=r["resolved_result"])
            elif res in ("filled", "refused"):
                final[rid] = r
        elif res == "shadow":
            shadow[rid] = r
        elif res == "guard_diag":
            diag[rid] = r
    token_side = {}
    for s in shadow.values():
        if s.get("token") and s.get("side"):
            token_side[str(s["token"])] = s["side"]
            if s.get("other_token"):
                token_side[str(s["other_token"])] = "DOWN" if s["side"] == "UP" else "UP"
    win = winners(directory, ledger, token_side)
    orders = []
    for rid, d in dec.items():
        if since and float(d.get("wall") or 0) < since:
            continue
        f = final.get(rid, {})
        actual = None
        if f.get("result") == "filled" and f.get("shares") and f.get("price") is not None:
            actual = (float(f["shares"]), float(f["price"]), float(f.get("fee") or 0.0))
        orders.append({"id": rid, "d": d, "actual": actual, "shadow": shadow.get(rid), "diag": diag.get(rid),
                       "cond": d.get("condition_id"), "win": win.get(d.get("condition_id"))})
    return orders


def delay_of(d: dict) -> float:
    for k in ("t_signal_to_response_ms", "latency_ms", "delay_ms"):
        if d.get(k) is not None:
            return float(d[k])
    return 300.0


def periods(wall: float) -> tuple[str, str]:
    g = time.gmtime(wall)
    return time.strftime("%Y-%m-%d", g), f"{time.strftime('%Y-%m-%d', g)} {g.tm_hour // 6 * 6:02d}-{g.tm_hour // 6 * 6 + 6:02d}h"


def compare(orders, name):
    """Per eligible order: (baseline P&L, alternative P&L, extra info)."""
    out = []
    if name == "tight" and not any("tight" in e for o in orders if o["shadow"]
                                   for e in (o["shadow"].get("L") or {}).values()):
        return out                      # a journal from before the tighter limit was priced
    for o in orders:
        if o["win"] is None:
            continue
        d, side = o["d"], o["d"]["side"]
        if name in GUARDS:
            if not o["diag"]:
                continue
            base = pnl(o["actual"], side, o["win"]) if o["actual"] else 0.0
            blocked = bool((o["diag"].get("guards") or {}).get(name))
            out.append({"o": o, "base": base, "alt": 0.0 if blocked else base, "blocked": blocked,
                        "fee": o["actual"][2] if o["actual"] else 0.0})
        else:
            s = o["shadow"]
            if not s:
                continue
            e = entry_at(s, delay_of(d))
            if not e:
                continue
            bf = quote_fill(e if (e.get("sh") or e.get("fak")) else None)
            base = pnl(bf, side, o["win"]) if bf else 0.0
            if name == "tight":
                t = e.get("tight")
                af = quote_fill(t) if t else bf                  # no tighter limit: the same policy
            else:
                af = bf if bf else quote_fill((e.get("alt") or {}).get("3"))
            alt = pnl(af, side, o["win"]) if af else 0.0
            out.append({"o": o, "base": base, "alt": alt, "blocked": False, "bfill": bf is not None,
                        "afill": af is not None})
    return out


def worst_case(items, use_alt: bool) -> list[float]:
    by = collections.defaultdict(lambda: {"UP": [0.0, 0.0], "DOWN": [0.0, 0.0]})
    for it in items:
        o = it["o"]
        if not o["actual"] or (use_alt and it["blocked"]):
            continue
        sh, px, fee = o["actual"]
        b = by[o["cond"]][o["d"]["side"]]
        b[0] += sh
        b[1] += sh * px + fee
    return [v["UP"][1] + v["DOWN"][1] - min(v["UP"][0], v["DOWN"][0]) for v in by.values()]


def report(orders, min_n: int) -> int:
    print(f"orders with a known round winner: {sum(1 for o in orders if o['win'])} of {len(orders)} sent")
    for name in GUARDS + LIMITS:
        items = compare(orders, name)
        print()
        if not items:
            print(f"{name}: no eligible signals (needs guard_diag / shadow rows from a build that writes them)")
            continue
        n = len(items)
        B, A = sum(i["base"] for i in items), sum(i["alt"] for i in items)
        print(f"{name}: eligible {n}, baseline P&L ${B:+.2f}, alternative ${A:+.2f}, "
              f"NET INCREMENTAL {100 * (A - B) / n:+.2f}c per signal")
        if name in GUARDS:
            bl = [i for i in items if i["blocked"]]
            won = sum(1 for i in bl if i["o"]["actual"] and i["base"] > 0)
            lost = sum(1 for i in bl if i["o"]["actual"] and i["base"] <= 0)
            wb, wa = worst_case(items, False), worst_case(items, True)
            print(f"   rejected {len(bl)} orders: fills among them {sum(1 for i in bl if i['o']['actual'])} "
                  f"(missed winners {won}, losers avoided {lost}); fees saved ${sum(i['fee'] for i in bl):.2f}")
            if wb and wa:
                print(f"   worst-case round loss: baseline mean ${sum(wb) / len(wb):.2f} max ${max(wb):.2f}"
                      f" | with the guard mean ${sum(wa) / len(wa):.2f} max ${max(wa):.2f}")
        else:
            print(f"   fills: baseline {sum(1 for i in items if i['bfill'])}, alternative {sum(1 for i in items if i['afill'])}")
        verdicts = []
        for idx, label in ((0, "UTC day"), (1, "6-hour block")):
            per = collections.defaultdict(list)
            for i in items:
                per[periods(float(i["o"]["d"]["wall"]))[idx]].append(i)
            cells = []
            for k in sorted(per):
                v = per[k]
                inc = 100 * (sum(i["alt"] for i in v) - sum(i["base"] for i in v)) / len(v)
                cells.append((k, len(v), inc))
            print(f"   by {label}: " + "; ".join(f"{k} n={c} {inc:+.2f}c" for k, c, inc in cells))
            enough = [c for c in cells if c[1] >= min_n]
            if len(enough) < 2:
                verdicts.append("INSUFFICIENT")
            elif all(inc > 0 for _k, _c, inc in enough):
                verdicts.append("PASS")
            else:
                verdicts.append("FAIL")
        verdict = "PASS" if all(v == "PASS" for v in verdicts) else (
            "INSUFFICIENT" if "INSUFFICIENT" in verdicts else "FAIL")
        print(f"   verdict: {verdict} (positive in every period with >= {min_n} signals; days: {verdicts[0]}, "
              f"6h blocks: {verdicts[1]})")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--live", action="store_true", help="the live canary's journal and ledger")
    ap.add_argument("--dir", default=".", help="folder with the journal, ledger and settlement files")
    ap.add_argument("--journal", default=None)
    ap.add_argument("--ledger", default=None)
    ap.add_argument("--since", default=None, help='UTC start, "YYYY-MM-DD HH:MM" (the forward test start)')
    ap.add_argument("--min-n", type=int, default=20, help="signals a period needs to count")
    args = ap.parse_args(argv)
    journal = args.journal or os.path.join(args.dir, "live_sniper.jsonl" if args.live else "paper_orders_sniper.jsonl")
    ledger = args.ledger or os.path.join(args.dir, "live_ledger.json" if args.live else "paper_ledger.json")
    since = None
    if args.since:
        import calendar
        since = calendar.timegm(time.strptime(args.since, "%Y-%m-%d %H:%M"))
    print(f"journal {journal}" + (f", since {args.since} UTC" if args.since else ", all rows"))
    return report(load(args.dir, journal, ledger, since), args.min_n)


if __name__ == "__main__":
    raise SystemExit(main())
