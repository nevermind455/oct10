#!/usr/bin/env python3
"""One live order, checked against Polymarket before a halt is cleared.
Stop the bot first.

    .venv/bin/python reconcile_order.py 0xa1fa8479            # read-only check (an id or its prefix)
    .venv/bin/python reconcile_order.py 0xa1fa8479 --settle   # check, then settle its canary intent

What it compares, for that order:
  journal   live_sniper.jsonl: what was signed (amount, cap) and the reply's trade ids
  venue     the CLOB's record of the order, when it keeps one (it answered null for
            an executed FAK on 2026-10-15), and the order's trades from GET
            /data/trades - taker_order_id = this order - deduplicated by trade id
  ledger    the live ledger's authorization and the lots booked for the order
  canary    the canary session's send intent (live_canary_state.json)
  position  the wallet's position in that outcome (Polymarket data API)

Consistent when the venue's trades are exactly the reply's (or the order
record's) trades, all CONFIRMED, booked by the ledger under the order's
authorization and nothing else, the spend is within the signed dollars,
and the wallet holds at least what the order bought.

--settle (only after a consistent check, with the bot stopped - it takes the
ledger's process lock): records the order's canary intent as CONFIRMED with
its real spend, through the canary session's own code. Nothing is sent to
the venue. Then `run.py --live --new-canary-session` clears the halt.
Prints no key, credential or full address.
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
TOL = Decimal("0.000001")


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


def first(rows, *keys):
    for r in rows:
        for k in keys:
            if r.get(k) not in (None, ""):
                return r[k]
    return None


def reply_trade_ids(rows) -> list:
    out = []
    for r in rows:
        resp = r.get("response") if isinstance(r.get("response"), dict) else {}
        for tids in (resp.get("tradeIDs"), resp.get("trade_ids"), r.get("trade_ids")):
            if isinstance(tids, list):
                out += [str(t) for t in tids if t]
    return sorted(set(out))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("order", help="the order id (0x...) or a prefix of it as the halt showed it")
    ap.add_argument("--no-position", action="store_true", help="skip the data-API position lookup")
    ap.add_argument("--settle", action="store_true",
                    help="after a consistent check: record the order's canary intent as CONFIRMED")
    args = ap.parse_args()
    import config  # noqa: F401 - loads .env
    from feeds.poly_user import _legs_paid, taker_legs

    full, rows = journal_rows(args.order)
    oid = full or args.order
    if len(oid) < 66:
        print(f"order {args.order}: not found in the live journal; pass the full 0x... id")
        return 2
    amount = d(first(rows, "amount"))
    cap = first(rows, "cap")
    cond = first(rows, "condition_id", "cond", "market")
    token = first(rows, "token", "token_id", "asset_id")
    reply_tids = reply_trade_ids(rows)
    print(f"order ...{oid[-12:]}")
    print(f"  journal : {len(rows)} row(s) {[r.get('result') for r in rows]}; signed ${amount} at cap {cap}; "
          f"the reply named {len(reply_tids)} trade(s)")

    import polymarket_trade
    from py_clob_client_v2 import TradeParams
    client = polymarket_trade._get_client()
    raw = client.get_order(oid)
    order = raw if isinstance(raw, dict) and str(raw.get("id")) == oid else None
    if order is None:
        print(f"  venue   : no order record (the CLOB answered {type(raw).__name__} {str(raw)[:40]!r}); "
              f"using the order's trades")
        order_tids = []
    else:
        order_tids = [str(t) for t in order.get("associate_trades") or []]
        token, cond = token or str(order.get("asset_id")), cond or str(order.get("market"))
        print(f"  venue   : status {order.get('status')}, size_matched {order.get('size_matched')}, "
              f"original_size {order.get('original_size')}, {len(order_tids)} trade id(s)")
    if not cond:
        print("  no market id in the journal or the venue: cannot look the trades up")
        return 1
    expected = set(order_tids or reply_tids)
    trades = {}
    for row in client.get_trades(TradeParams(market=cond, after=int(time.time()) - 7 * 86400)) or []:
        if isinstance(row, dict) and (str(row.get("taker_order_id")) == oid or str(row.get("id")) in expected):
            trades[str(row.get("id"))] = row                         # deduplicated by trade id
    v_shares = v_paid = Decimal(0)
    statuses = set()
    for tid, t in sorted(trades.items()):
        token = token or str(t.get("asset_id"))
        legs = taker_legs(str(t.get("asset_id")), t.get("maker_orders"), t.get("size"))
        paid = _legs_paid(legs)
        size, price = d(t.get("size")) or Decimal(0), d(t.get("price")) or Decimal(0)
        cost = Decimal(str(paid)) if paid is not None else size * price
        v_shares += size
        v_paid += cost
        statuses.add(str(t.get("status") or "").upper())
        print(f"    trade ...{tid[-10:]}: {t.get('status')} {size} sh, price {price}, paid ${cost:.6f}")
    print(f"  venue   : {len(trades)} trade(s), {v_shares} shares for ${v_paid:.6f}")

    from accounting.ledger import Ledger
    led_path = state_path("LEDGER_PATH", "ledger.json")
    led = Ledger(path=str(led_path), category="crypto")
    if not led.load():
        print(f"  ledger  : did not load ({led.last_persistence_error})")
        return 1
    with led._lock:
        authorized = oid in led.authorized_orders
        lots = [lot for pos in led.positions.values() for lot in pos.lots
                if getattr(lot, "order_id", None) == oid and str(lot.side).upper() == "BUY"]
        booked = {str(lot.trade_id) for lot in lots}
        l_shares = sum((Decimal(str(lot.shares)) for lot in lots), Decimal(0))
        l_cost = sum((Decimal(str(lot.shares)) * Decimal(str(lot.price)) for lot in lots), Decimal(0))
        held = sum((Decimal(str(p.shares)) for p in led.positions.values()
                    if str(p.token_id) == str(token) and not p.settled), Decimal(0))
    print(f"  ledger  : authorized {authorized}; {len(lots)} lot(s), {l_shares} shares for ${l_cost:.6f}")

    from sniper_live import CanarySession
    canary = state_path("SNIPER_CANARY_STATE_PATH", str(led_path.with_name("live_canary_state.json")))
    try:
        data = CanarySession.durable_view(canary) or {}
    except Exception as exc:  # noqa: BLE001
        data = {}
        print(f"  canary  : unreadable ({type(exc).__name__})")
    key, intent = next(((k, i) for k, i in (data.get("intents") or {}).items()
                        if oid in (str(i.get("order_hash") or ""), str(i.get("order_id") or ""))), (None, None))
    print(f"  canary  : intent {intent.get('status') if intent else 'not in the hot state'}; "
          f"halts {len(data.get('kill_reasons') or [])}")

    pos_size = None
    if not args.no_position:
        holder = getattr(client.builder, "funder", None) or client.signer.address()
        q = urllib.parse.urlencode({"user": holder, "market": cond, "sizeThreshold": 0})
        try:
            with urllib.request.urlopen(f"https://data-api.polymarket.com/positions?{q}", timeout=10) as r:
                rows_ = json.loads(r.read().decode())
            pos_size = sum((d(p.get("size")) or Decimal(0) for p in rows_ if str(p.get("asset")) == str(token)),
                           Decimal(0))
            print(f"  position: the wallet holds {pos_size} shares of this outcome (ledger, open: {held})")
        except Exception as exc:  # noqa: BLE001
            print(f"  position: lookup failed ({type(exc).__name__}); check it on polymarket.com")

    problems = []
    if not trades:
        problems.append("no trade of this order on the venue - a missing order record never proves a zero fill")
    if expected and set(trades) != expected:
        problems.append(f"the venue's trades {sorted(t[-8:] for t in trades)} are not the order's "
                        f"{sorted(t[-8:] for t in expected)}")
    if not expected:
        problems.append("neither the reply nor an order record lists the order's trades: completeness unproven")
    if order is not None and d(order.get("size_matched")) is not None \
            and abs(d(order.get("size_matched")) - v_shares) > TOL:
        problems.append(f"size_matched {order.get('size_matched')} != the trades' {v_shares}")
    if statuses - {"CONFIRMED"}:
        problems.append(f"not every trade is CONFIRMED yet ({sorted(statuses)})")
    if amount is not None and v_paid > amount + TOL:
        problems.append(f"spent ${v_paid} above the signed ${amount}")
    if not authorized:
        problems.append("the ledger has no authorization for this order")
    if booked != set(trades) or abs(l_shares - v_shares) > TOL:
        problems.append(f"the ledger booked {l_shares} shares ({len(booked)} trade(s)); the venue filled {v_shares}")
    if pos_size is not None and held > 0 and pos_size + TOL < v_shares:
        problems.append(f"the wallet holds {pos_size}, less than this order bought ({v_shares})")
    print("VERDICT: " + ("consistent - what Polymarket filled is what the ledger booked and the wallet holds"
                         if not problems else "NOT YET:\n  - " + "\n  - ".join(problems)))
    if problems:
        return 1
    if intent is None or intent.get("status") in CanarySession.FINAL:
        print("canary: nothing to settle; start with --new-canary-session to clear the halt")
        return 0
    if not args.settle:
        print(f"canary: the send intent is {intent.get('status')}; re-run with --settle to record it CONFIRMED "
              f"(${l_cost:.6f}), then start with --new-canary-session")
        return 0
    return settle(canary, data, key, intent, led, led_path, float(l_cost), client)


def settle(canary, data, key, intent, led, led_path, spent: float, client) -> int:
    """The canary intent -> CONFIRMED with its real spend, as a restart that
    found this proof would record it. Takes the ledger's process lock."""
    import run
    from sniper_live import MATCHED, PENDING, CanarySession, KillSwitch, limits_from_env
    holder = getattr(client.builder, "funder", None) or client.signer.address()
    wallet = CanarySession.wallet_key_for(holder)
    if data.get("wallet") != wallet:
        print("settle: the canary state belongs to another wallet: refusing")
        return 1
    lock = run._ProcessLock(Path(str(led_path) + ".lock"))
    try:
        lock.acquire()
    except RuntimeError:
        print("settle: a bot process holds the ledger: stop it first")
        return 1
    try:
        session, limits, kill = CanarySession(canary, wallet), limits_from_env(), KillSwitch()
        session.open(limits, kill, led)
        amount = float(intent.get("amount") or 0.0)
        if intent.get("kind") not in (MATCHED, PENDING):
            limits.record_outcome(MATCHED, amount)
        limits.resolve_accepted(max(0.0, amount - spent))
        session.resolve(key, "CONFIRMED", limits, kill, spent=min(spent, amount))
        ok = session.flush() and session.save(limits, kill)
        session.close()
    finally:
        lock.release()
    after = CanarySession.durable_view(canary) or {}
    status = ((after.get("intents") or {}).get(key) or {}).get("status", "archived (finished)")
    print(f"settle: canary intent -> {status}" + ("" if ok else " (NOT durable - check the disk)"))
    print("next: start live with --new-canary-session")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
