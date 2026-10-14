#!/usr/bin/env python3
"""One live order, checked against Polymarket before a halt is cleared.
READ-ONLY: no order, no cancel, no state written. Stop the bot first.

    .venv/bin/python reconcile_order.py 0xa1fa8479          # an order id or its prefix
    .venv/bin/python reconcile_order.py 0xa1fa8479 --no-position

What it compares, for that order:
  journal   live_sniper.jsonl: what was decided and signed (amount, cap, shares)
  venue     the CLOB's own record of the order (status, size_matched, trades)
            and each of its trades (deduplicated by trade id)
  ledger    the live ledger's authorization and the lots booked for it
  canary    the canary session's send intent (live_canary_state.json)
  position  the wallet's position in that outcome (Polymarket data API)

Then a verdict: does what Polymarket filled equal what the ledger booked,
was the spend within the signed USDC amount, and is the position at least
what this order bought. Prints no key, credential or full address.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))


def state_path(env: str, default: str) -> Path:
    raw = os.environ.get(env) or default
    p = Path(raw)
    return p if p.is_absolute() else ROOT / p


def journal_rows(prefix: str) -> tuple[str | None, list]:
    path = state_path("LIVE_SNIPER_JOURNAL", "live_sniper.jsonl")
    if not path.exists():
        return None, []
    full, rows = None, []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if prefix.lower() not in line.lower():
                continue
            try:
                r = json.loads(line)
            except ValueError:
                continue
            oid = str(r.get("order_id") or "")
            if oid.lower().startswith(prefix.lower()):
                full = full or oid
                rows.append(r)
    return full, rows


def d(x) -> Decimal | None:
    try:
        v = Decimal(str(x))
        return v if v.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("order", help="the order id (0x...) or a prefix of it as the halt showed it")
    ap.add_argument("--no-position", action="store_true", help="skip the data-API position lookup")
    args = ap.parse_args()
    import config  # noqa: F401 - loads .env
    from feeds.poly_user import _legs_paid, taker_legs

    full, rows = journal_rows(args.order)
    oid = full or args.order
    if len(oid) < 66:
        print(f"order {args.order}: not found in the live journal; pass the full 0x... id")
        return 2
    print(f"order ...{oid[-12:]}")
    signed = next((r for r in rows if r.get("amount") is not None and r.get("cap") is not None), {})
    token, cond = signed.get("token") or signed.get("token_id"), signed.get("condition_id") or signed.get("cond")
    amount = d(signed.get("amount"))
    print(f"  journal : {len(rows)} row(s); results {[r.get('result') for r in rows]}; signed amount "
          f"${signed.get('amount')} cap {signed.get('cap')} order_shares {signed.get('order_shares')} "
          f"target {signed.get('target_shares') or signed.get('kelly_shares')}")

    import polymarket_trade
    from py_clob_client_v2 import TradeParams
    client = polymarket_trade._get_client()
    order = client.get_order(oid) or {}
    if str(order.get("id")) != oid:
        print(f"  venue   : no record for this order ({str(order)[:120]})")
        return 1
    token = token or str(order.get("asset_id"))
    cond = cond or str(order.get("market"))
    tids = list(order.get("associate_trades") or [])
    print(f"  venue   : status {order.get('status')}, side {order.get('side')}, price {order.get('price')}, "
          f"original_size {order.get('original_size')}, size_matched {order.get('size_matched')}, "
          f"{len(tids)} trade id(s)")
    trades = {}
    for row in client.get_trades(TradeParams(market=cond, after=int(time.time()) - 7 * 86400)) or []:
        if isinstance(row, dict) and (str(row.get("taker_order_id")) == oid or str(row.get("id")) in tids):
            trades[str(row.get("id"))] = row                       # deduplicated by trade id
    v_shares = v_paid = Decimal(0)
    for tid, t in sorted(trades.items()):
        legs = taker_legs(str(t.get("asset_id")), t.get("maker_orders"), t.get("size"))
        paid = _legs_paid(legs)
        size, price = d(t.get("size")) or Decimal(0), d(t.get("price")) or Decimal(0)
        cost = Decimal(str(paid)) if paid is not None else size * price
        v_shares += size
        v_paid += cost
        print(f"    trade ...{tid[-10:]}: {t.get('status')} {size} sh, taker price {price}, paid ${cost:.6f}"
              + (f" ({len(legs)} maker leg(s))" if legs else ""))
    missing = [t for t in tids if t not in trades]
    if missing:
        print(f"    {len(missing)} trade id(s) the order names were not returned by get_trades")
    print(f"  venue total: {v_shares} shares for ${v_paid:.6f}")

    from accounting.ledger import Ledger
    led = Ledger(path=str(state_path("LEDGER_PATH", "ledger.json")), category="crypto")
    if not led.load():
        print(f"  ledger  : did not load ({led.last_persistence_error})")
        return 1
    with led._lock:
        authorized = oid in led.authorized_orders
        lots = [(lot, pos) for pos in led.positions.values() for lot in pos.lots
                if getattr(lot, "order_id", None) == oid]
        booked_ids = {str(lot.trade_id) for lot, _ in lots}
        l_shares = sum((Decimal(str(lot.shares)) for lot, _ in lots), Decimal(0))
        l_cost = sum((Decimal(str(lot.shares)) * Decimal(str(lot.price)) for lot, _ in lots), Decimal(0))
        held = sum((Decimal(str(pos.shares)) for pos in led.positions.values()
                    if str(getattr(pos, "token_id", "")) == str(token) and not pos.settled), Decimal(0))
    print(f"  ledger  : authorized {authorized}; {len(lots)} lot(s) {l_shares} shares ${l_cost:.6f}; "
          f"booked trade ids {'match' if booked_ids == set(trades) else sorted(i[-8:] for i in booked_ids)}")

    from sniper_live import CanarySession
    canary = state_path("SNIPER_CANARY_STATE_PATH", str(Path(led.path).with_name("live_canary_state.json")))
    try:
        data = CanarySession.durable_view(canary) or {}
    except Exception as exc:  # noqa: BLE001
        data = {}
        print(f"  canary  : unreadable ({type(exc).__name__})")
    intent = next((i for i in (data.get("intents") or {}).values()
                   if str(i.get("order_hash") or "") == oid or str(i.get("order_id") or "") == oid), None)
    print(f"  canary  : intent {intent.get('status') if intent else 'not in the hot state (finished/archived)'}; "
          f"halts {data.get('kill_reasons') or []}")

    pos_size = None
    if not args.no_position:
        holder = getattr(client.builder, "funder", None) or client.signer.address()
        q = urllib.parse.urlencode({"user": holder, "market": cond, "sizeThreshold": 0})
        try:
            with urllib.request.urlopen(f"https://data-api.polymarket.com/positions?{q}", timeout=10) as r:
                rows_ = json.loads(r.read().decode())
            pos_size = sum((d(p.get("size")) or Decimal(0) for p in rows_ if str(p.get("asset")) == str(token)),
                           Decimal(0))
            print(f"  position: wallet ...{str(holder)[-6:]} holds {pos_size} shares of this outcome "
                  f"(ledger, open: {held})")
        except Exception as exc:  # noqa: BLE001
            print(f"  position: lookup failed ({type(exc).__name__}); check it on polymarket.com")

    problems = []
    if not trades and Decimal(str(order.get("size_matched") or 0)) > 0:
        problems.append("the venue says it matched but returned no trades for it")
    if d(order.get("size_matched")) is not None and abs(d(order.get("size_matched")) - v_shares) > Decimal("0.000001"):
        problems.append(f"size_matched {order.get('size_matched')} != the trades' {v_shares}")
    if amount is not None and v_paid > amount + Decimal("0.000001"):
        problems.append(f"spent ${v_paid} above the signed ${amount}")
    if not authorized:
        problems.append("the ledger has no authorization for this order")
    if abs(l_shares - v_shares) > Decimal("0.000001") or booked_ids != set(trades):
        problems.append(f"the ledger booked {l_shares} shares, the venue filled {v_shares}")
    if pos_size is not None and pos_size + Decimal("0.000001") < v_shares and held > 0:
        problems.append(f"the wallet holds {pos_size}, less than this order bought ({v_shares})")
    print("VERDICT: " + ("consistent - what Polymarket filled is what the ledger booked and the wallet holds; "
                         "the halt may be cleared (DEPLOY, 'Clearing the halt')"
                         if not problems else "NOT YET:\n  - " + "\n  - ".join(problems)))
    if intent and intent.get("status") not in ("CONFIRMED", "NO_MATCH", "REJECTED", "NOT_SENT"):
        print(f"note: the canary send intent is still {intent.get('status')}; the first restart settles it "
              f"by the order hash (then --new-canary-session clears the halt)")
    return 0 if not problems else 1


if __name__ == "__main__":
    raise SystemExit(main())
