#!/usr/bin/env python3
"""Offline BUY execution replay; no credentials, clocks, network or live orders.

python execution_replay.py input.json output.json

Input: {events: [...], orders: [...]}. All times are venue-domain milliseconds.
Events require token, seq, ts_ms, kind (snapshot/delta/trade/gap/watermark). Snapshot:
asks/bids [[price,size],...]; delta: book_side ASK/BID, price, size (absolute);
trade: aggressor BUY/SELL, price, size, trade_id. seq must be contiguous per
token; gaps invalidate the book until a new full snapshot. This requires an
upstream recorder that detects connection gaps; inventing seq is not proof.

Orders: id, token, submit_ms, latency_ms, kind FAK/FOK/POST_ONLY, cap,
and exactly one of amount (dollars, taker only) or shares. Fee parameters
fee_rate/fee_exponent, tick, min_shares MUST be supplied. Optional cancel_ms
is the effective cancellation time (already includes cancellation latency).

Maker queue: all displayed bids at our price are ahead. Only unique SELL
prints at that price consume that queue; cancellations never improve it.
This is a conservative FIFO SCENARIO, not measured queue identity. Taker
fills walk remaining displayed asks only, under the original cap. Virtual
consumption persists across identical snapshots; a level must disappear
before that level's consumption is reset. Never fill on price touch alone.

Results are model executions. L2 cannot reveal hidden liquidity, competing
takers, actual FIFO rank, missing messages or counterfactual market impact.
Market events precede our actions at equal timestamps (explicit convention).
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
import json
import math
from pathlib import Path

D = Decimal
ZERO = D(0)


def number(value, name, *, positive=False):
    value = D(str(value))
    if not value.is_finite() or (value <= 0 if positive else value < 0):
        raise ValueError(f"invalid {name}")
    return value


@dataclass
class Book:
    asks: dict = field(default_factory=dict)
    bids: dict = field(default_factory=dict)
    debt: dict = field(default_factory=dict)
    print_debt: dict = field(default_factory=dict)
    seq: int | None = None
    valid: bool = False
    trades: set = field(default_factory=set)


class Replay:
    def __init__(self):
        self.books = {}
        self.orders = {}

    def invalidate(self, token):
        self.books.setdefault(token, Book()).valid = False
        for o in self.orders.values():
            if o["token"] == token and o["status"] == "WORKING":
                o["status"] = "UNKNOWN_AFTER_GAP"

    def market(self, event):
        token, kind = event["token"], event["kind"]
        b = self.books.setdefault(token, Book())
        seq = event.get("seq")
        if not isinstance(seq, int) or isinstance(seq, bool):
            raise ValueError("events need integer sequence evidence")
        if kind == "gap":
            self.invalidate(token)
            b.seq = seq
            return
        if b.seq is not None and seq <= b.seq:
            raise ValueError("duplicate or backward event sequence")
        if kind != "snapshot" and (b.seq is None or seq != b.seq + 1):
            self.invalidate(token)
        b.seq = seq
        if kind == "snapshot":
            if b.valid and any(o["token"] == token and o["status"] == "WORKING" for o in self.orders.values()):
                # A full refresh does not prove maker priority survived it.
                self.invalidate(token)
            def ladder(key):
                out = {}
                for p, q in event[key]:
                    px, size = number(p, "price", positive=True), number(q, "size")
                    if px >= 1 or px in out:
                        raise ValueError("invalid or duplicate level")
                    if size:
                        out[px] = size
                return out
            asks, bids = ladder("asks"), ladder("bids")
            if asks and bids and min(asks) <= max(bids):
                self.invalidate(token)
                return
            b.debt = {p: q for p, q in b.debt.items() if p in asks}
            b.print_debt.clear()  # full snapshot is authoritative after prints
            b.asks, b.bids, b.valid = asks, bids, True
            return
        if not b.valid:
            return
        if kind == "watermark":
            return
        if kind == "delta":
            if event["book_side"] not in ("ASK", "BID"):
                raise ValueError("invalid book side")
            ladder = b.asks if event["book_side"] == "ASK" else b.bids
            px, size = number(event["price"], "price", positive=True), number(event["size"], "size")
            if px >= 1:
                raise ValueError("invalid price")
            b.print_debt.pop((event["book_side"], px), None)
            if size:
                ladder[px] = size
            else:
                ladder.pop(px, None)
                if event["book_side"] == "ASK":
                    b.debt.pop(px, None)
            if b.asks and b.bids and min(b.asks) <= max(b.bids):
                self.invalidate(token)
        elif kind == "trade":
            tid = event.get("trade_id")
            if not tid:
                self.invalidate(token)
                return
            if tid in b.trades:
                return
            b.trades.add(tid)
            aggressor = event.get("aggressor")
            if aggressor not in ("BUY", "SELL"):
                self.invalidate(token)
                return
            px, remaining = number(event["price"], "price", positive=True), number(event["size"], "size", positive=True)
            if px >= 1:
                raise ValueError("invalid trade price")
            key = ("ASK" if aggressor == "BUY" else "BID", px)
            b.print_debt[key] = b.print_debt.get(key, ZERO) + remaining
            if aggressor != "SELL":
                return
            # Earlier simulated makers share the queue, never the same print twice.
            working = [o for o in self.orders.values() if o["token"] == token and o["status"] == "WORKING" and o["cap"] == px]
            if working:
                front = working[0]
                ahead = min(front["ahead"], remaining)
                remaining -= ahead
                for o in working:
                    o["ahead"] = max(ZERO, o["ahead"] - ahead)
                for o in working:
                    take = min(o["remaining"], remaining)
                    if take:
                        self.fill(o, take, px, event["ts_ms"], maker=True)
                        remaining -= take
                        for later in working[working.index(o) + 1:]:
                            later["ahead"] = max(ZERO, later["ahead"] - take)
                    if not remaining:
                        break
        else:
            raise ValueError(f"unknown market event {kind}")

    @staticmethod
    def fill(o, size, price, at, maker=False):
        fee = ZERO if maker else (size * o["fee_rate"] * (price * (1-price)) ** o["fee_exponent"]).quantize(D(".00001"), rounding=ROUND_HALF_UP)
        o["fills"].append({"shares": size, "price": price, "fee": fee, "ts_ms": at})
        o["remaining"] -= size
        if o["remaining"] <= D(".000000001"):
            o["status"] = "FILLED"

    def arrive(self, spec, at):
        oid = spec["id"]
        if oid in self.orders:
            raise ValueError("duplicate order ID")
        kind = spec["kind"]
        if kind not in ("FAK", "FOK", "POST_ONLY"):
            raise ValueError("unsupported order kind")
        cap = number(spec["cap"], "cap", positive=True)
        tick = number(spec["tick"], "tick", positive=True)
        rate = number(spec["fee_rate"], "fee rate")
        exponent = spec["fee_exponent"]
        minimum = number(spec["min_shares"], "minimum shares", positive=True)
        if cap >= 1 or cap % tick or rate > 1 or isinstance(exponent, bool) or not isinstance(exponent, int) or not 1 <= exponent <= 8:
            raise ValueError("invalid order rules")
        if ("shares" in spec) == ("amount" in spec):
            raise ValueError("supply shares or amount, exclusively")
        dollar = "amount" in spec
        wanted = number(spec["amount" if dollar else "shares"], "order size", positive=True)
        o = {"id": oid, "token": spec["token"], "kind": kind, "cap": cap, "remaining": wanted,
             "fee_rate": rate, "fee_exponent": exponent, "fills": [], "status": "WORKING",
             "arrival_ms": at, "evidence": "L2_SCENARIO_NOT_OBSERVED_EXECUTION"}
        self.orders[oid] = o
        b = self.books.get(spec["token"])
        if not b or not b.valid:
            o["status"] = "UNKNOWN_BOOK"
            return
        if kind == "POST_ONLY":
            if dollar or wanted < minimum or (b.asks and min(b.asks) <= cap):
                o["status"] = "REJECTED_POST_ONLY_OR_SIZE"
                return
            o["ahead"] = max(ZERO, b.bids.get(cap, ZERO) - b.print_debt.get(("BID", cap), ZERO)) + sum(x["remaining"] for x in self.orders.values()
                if x is not o and x["token"] == o["token"] and x["cap"] == cap and x["status"] == "WORKING")
            return
        if (dollar and (wanted < 1 or wanted / cap < minimum)) or (not dollar and (wanted < minimum or wanted * cap < 1)):
            o["status"] = "REJECTED_SIZE"
            return
        needed, levels = wanted, []
        for px, visible in sorted(b.asks.items()):
            if px > cap:
                break
            size = max(ZERO, visible - b.debt.get(px, ZERO) - b.print_debt.get(("ASK", px), ZERO))
            take = min(size, needed / px if dollar else needed)
            if take:
                levels.append((px, take))
                needed -= take * px if dollar else take
        if kind == "FOK" and needed > D(".000000001"):
            o["status"] = "KILLED"
            return
        o["remaining"] = sum(q for p, q in levels) + (needed / cap if dollar else needed)
        for px, take in levels:
            self.fill(o, take, px, at)
            b.debt[px] = b.debt.get(px, ZERO) + take
        o["status"] = "PARTIAL_KILLED" if levels and needed > D(".000000001") else "FILLED" if levels else "KILLED"

    def run(self, events, orders):
        schedule = []
        coverage = {}
        for i, e in enumerate(events):
            schedule.append((float(e["ts_ms"]), 0, i, "market", e))
            coverage[e["token"]] = max(coverage.get(e["token"], -math.inf), float(e["ts_ms"]))
        for i, o in enumerate(orders):
            latency = float(o["latency_ms"])
            submit = float(o["submit_ms"])
            if not all(math.isfinite(v) and v >= 0 for v in (latency, submit)):
                raise ValueError("invalid order latency/time")
            schedule.append((submit + latency, 1, i, "arrive", o))
            if "cancel_ms" in o:
                cancel = float(o["cancel_ms"])
                if not math.isfinite(cancel) or cancel < submit + latency:
                    raise ValueError("cancel must take effect after arrival")
                schedule.append((cancel, 2, i, "cancel", o))
        for at, priority, index, kind, event in sorted(schedule):
            if not math.isfinite(at) or at < 0:
                raise ValueError("invalid event time")
            if kind == "market":
                self.market(event)
            elif kind == "arrive":
                covered = at <= coverage.get(event["token"], -math.inf)
                if not covered:
                    self.invalidate(event["token"])
                self.arrive(event, at)
                if not covered:
                    # Do not carry an old book past the end of the recording.
                    o = self.orders[event["id"]]
                    o["status"], o["fills"] = "UNKNOWN_COVERAGE", []
            else:
                o = self.orders[event["id"]]
                if o["status"] == "WORKING":
                    o["status"] = "CANCELLED"
                    for later in self.orders.values():
                        if later is not o and later["token"] == o["token"] and later["cap"] == o["cap"] and later["status"] == "WORKING" and later["arrival_ms"] >= o["arrival_ms"]:
                            later["ahead"] = max(ZERO, later["ahead"] - o["remaining"])
        # Unfilled remainder is not a loss or a pretend fill. End of recording
        # cannot prove whether a still-working order would fill afterwards.
        return list(self.orders.values())


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("input", type=Path)
    ap.add_argument("output", type=Path)
    args = ap.parse_args()
    data = json.loads(args.input.read_text(encoding="utf-8"))
    result = Replay().run(data["events"], data["orders"])
    args.output.write_text(json.dumps(result, default=str, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
