"""Why did the live ledger refuse a fill? Read-only.

    .venv/bin/python live_reconcile.py            # every refused fill and every unconfirmed live order
    .venv/bin/python live_reconcile.py --hours 48

The canary halts when a fill on the wallet is refused by the live ledger
(no authorized order explains it, or it does not match the order: other
token or market, not a BUY, or more notional than the order asked for) and
when a MATCHED order never turns up confirmed. This prints, for each such
trade and order, the venue's own trade record (GET /data/trades, read-only)
beside what the ledger authorized, and names the check that failed.

It sends no order and changes no file. It prints trade and order ids,
sizes and prices only - never keys, API owner ids or wallet addresses.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

TOL_USD = 0.02          # the ledger's notional tolerance (accounting.ledger._fill_matches_authorization)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def state_path(env_file: Path, name: str, default: str) -> Path:
    """A state file the way run.py resolves it: the shell's value, else the
    .env's (any key - sniper_check.read_env keeps only PAPER_*/SNIPER_* lines,
    so LEDGER_PATH set in .env was missed), else the default; relative paths
    are relative to the code."""
    import os
    raw = os.environ.get(name)
    if not raw:
        try:
            from dotenv import dotenv_values
            raw = (dotenv_values(env_file) or {}).get(name)
        except Exception:  # noqa: BLE001 - no python-dotenv: the default
            raw = None
    path = Path(raw or default)
    return path if path.is_absolute() else ROOT / path


def load_ledger(path: Path) -> dict:
    from accounting.ledger import current_payload
    data = current_payload(str(path))                   # the snapshot plus its journal
    pos = data.get("positions") or {}
    lots = []
    for p in pos.values():
        p = json.loads(p) if isinstance(p, str) else p
        lots += p.get("lots") or []
    seen = data.get("seen") or []
    if isinstance(seen, str):
        seen = json.loads(seen)
    return {"authorized": data.get("authorized_orders") or {}, "seen": set(seen),
            "unauthorized": set(data.get("reported_unauthorized") or []),
            "mismatch": set(data.get("reported_mismatch") or []),
            "booked_orders": {lot.get("order_id") for lot in lots if lot.get("order_id")},
            "booked_trades": {lot.get("trade_id") for lot in lots}}


def verdict(t: dict, ours: dict, led: dict) -> list[str]:
    """The ledger's own checks on one venue trade record, in its order."""
    oid = str(t.get("taker_order_id") or "")
    auth = led["authorized"].get(oid)
    out = []
    if t.get("trader_side") and str(t.get("trader_side")).upper() != "TAKER":
        out.append(f"we are the {t.get('trader_side')} on this trade - the ledger keys fills by the TAKER order id, "
                   f"so it cannot find our order")
    if not oid or auth is None:
        mine = [m.get("order_id") for m in (t.get("maker_orders") or []) if m.get("order_id") in ours]
        out.append("UNAUTHORIZED: the taker order id is not an order the ledger authorized"
                   + (f" (our order {str(mine[0])[:12]}.. is among the MAKER orders)" if mine else ""))
        return out
    if str(t.get("asset_id") or "") != str(auth.get("token_id") or ""):
        out.append("MISMATCH: token differs from the authorized order's")
    if str(t.get("market") or "") != str(auth.get("condition_id") or ""):
        out.append("MISMATCH: market differs from the authorized order's")
    if str(t.get("side") or "").upper() != "BUY":
        out.append(f"MISMATCH: side {t.get('side')} (the ledger books BUYs only)")
    size, price, req = _f(t.get("size")), _f(t.get("price")), _f(auth.get("requested_notional"))
    if None not in (size, price, req):
        if size * price > req + TOL_USD:
            out.append(f"MISMATCH: size x price ${size * price:.4f} > the order's ${req:.4f} + ${TOL_USD} "
                       f"(over by ${size * price - req:.4f})")
    if not out:
        out.append("passes every ledger check on its own - refused only if another trade of this order "
                   "already used the notional")
    return out


def show(t: dict, ours: dict, led: dict) -> None:
    size, price = _f(t.get("size")), _f(t.get("price"))
    makers = t.get("maker_orders") or []
    m_sh = sum(_f(m.get("matched_amount")) or 0 for m in makers)
    m_usd = sum((_f(m.get("matched_amount")) or 0) * (_f(m.get("price")) or 0) for m in makers)
    oid = str(t.get("taker_order_id") or "")
    auth = led["authorized"].get(oid) or {}
    print(f"  trade ...{str(t.get('id'))[-10:]}  status {t.get('status')}  {t.get('trader_side') or '?'} "
          f"{t.get('side')}  size {t.get('size')} @ {t.get('price')}  = ${(size or 0) * (price or 0):.4f}")
    print(f"    taker order {oid[:14]}.. {'(ours)' if oid in ours else '(NOT one of our journalled orders)'}; "
          f"authorized notional {auth.get('requested_notional', '-')}; booked in ledger: "
          f"{'yes' if str(t.get('id')) in led['booked_trades'] else 'no'}")
    if makers:
        levels = ", ".join(f"{m.get('matched_amount')}@{m.get('price')}" for m in makers[:8])
        total = f" -> {m_sh:.4f} sh, ${m_usd:.4f}, average {m_usd / m_sh:.4f}" if m_sh else ""
        print(f"    {len(makers)} maker order(s): {levels}{' ...' if len(makers) > 8 else ''}{total}")
    for v in verdict(t, ours, led):
        print(f"    -> {v}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default=str(ROOT / ".env"))
    ap.add_argument("--hours", type=float, default=36.0, help="live orders from the last N hours (default 36)")
    args = ap.parse_args()
    import sniper_check as sc
    ledger_path = state_path(Path(args.env), "LEDGER_PATH", "ledger.json")
    if not ledger_path.exists():
        found = sorted(str(x.name) for x in ROOT.glob("*ledger*.json"))
        print(f"no live ledger at {ledger_path} (LEDGER_PATH). Ledger files here: {', '.join(found) or 'none'}.")
        print("If the live bot never ran with this checkout's settings, there is nothing to reconcile; "
              "otherwise run again with LEDGER_PATH=<that file> in front of the command.")
        return 1
    led = load_ledger(ledger_path)
    since = time.time() - args.hours * 3600
    rows = [r for r in sc.jsonl(state_path(Path(args.env), "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl"))
            if r.get("live") and r.get("order_id") and float(r.get("wall") or 0) >= since]
    ours = {r["order_id"]: r for r in rows}
    matched = [r for r in rows if r.get("kind") == "MATCHED"]
    unconfirmed = [r for r in matched if r["order_id"] not in led["booked_orders"]]
    refused = (led["unauthorized"] | led["mismatch"]) - led["seen"]
    print(f"ledger {ledger_path.name}: {len(led['authorized'])} authorized orders, refused trades not booked since: "
          f"{len(refused)} ({len(led['unauthorized'] - led['seen'])} unauthorized, "
          f"{len(led['mismatch'] - led['seen'])} mismatch)")
    print(f"live journal, last {args.hours:g}h: {len(rows)} orders, {len(matched)} MATCHED, "
          f"{len(unconfirmed)} MATCHED but not booked in the ledger")
    if not refused and not unconfirmed:
        print("nothing to reconcile")
        return 0

    import config  # noqa: F401 - loads .env for the client
    import polymarket_trade as pt
    from clob_sdk import TradeParams
    client = pt._get_client()

    print("\nREFUSED TRADES (the venue's record)")
    for tid in sorted(refused):
        try:
            got = client.get_trades(TradeParams(id=tid), only_first_page=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  trade ...{tid[-10:]}: lookup failed {type(exc).__name__}")
            continue
        if not got:
            print(f"  trade ...{tid[-10:]}: the venue returned no record")
        for t in got:
            show(t, ours, led)

    print("\nMATCHED ORDERS NOT BOOKED (every trade of the wallet in that market around the order)")
    for r in unconfirmed:
        oid = r["order_id"]
        auth = led["authorized"].get(oid) or {}
        cond = auth.get("condition_id")
        print(f"\n  order {oid[:14]}..  {time.strftime('%m-%d %H:%M:%S', time.gmtime(float(r['wall'])))} UTC  "
              f"{r.get('side')}  ask seen {r.get('ask_seen')}  cap {r.get('cap')}  amount {r.get('amount')}  "
              f"authorized: {'yes' if auth else 'NO'} (notional {auth.get('requested_notional', '-')})")
        if not cond:
            print("    no authorization on record: the ledger never learned this order's market")
            continue
        try:
            got = client.get_trades(TradeParams(market=cond, after=int(float(r["wall"])) - 60), only_first_page=True)
        except Exception as exc:  # noqa: BLE001
            print(f"    trade lookup failed {type(exc).__name__}")
            continue
        mine = [t for t in got if str(t.get("taker_order_id")) == oid
                or any(m.get("order_id") == oid for m in (t.get("maker_orders") or []))]
        if not mine:
            print(f"    no trade of this order among {len(got)} wallet trade(s) in that market")
        for t in mine:
            show(t, ours, led)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
