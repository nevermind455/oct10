#!/usr/bin/env python3
"""Deterministic safety, fill, persistence, fee, and settlement tests.

    python tests_paper.py

No network is used.  All public books and market rules are fixed fixtures.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import pathlib
import shutil
import sys
import tempfile
import time
import types
from decimal import Decimal

ROOT = pathlib.Path(__file__).parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
from accounting.ledger import Ledger  # noqa: E402
from accounting.resolution import PENDING, RESOLVED, Resolution  # noqa: E402
from accounting.settlement import SettlementWorker  # noqa: E402
from paper_trade import (BookSnapshot, MarketRules, PaperBroker, PaperRejected,  # noqa: E402
                         curve_fee, estimate_fok, fetch_executable_book,
                         install_paper_execution, parse_book, parse_market_rules,
                         size_to_venue_minimum, snapshot_from_book_view)


PASS = FAIL = 0
FAILURES: list[str] = []
D = Decimal


def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL {name} {detail}")


def approx(a, b, tolerance=1e-8):
    return a is not None and abs(float(a) - float(b)) <= tolerance


def book(token="up", *, minimum=None):
    return BookSnapshot(
        token,
        ((D("0.40"), D("3")), (D("0.50"), D("10")),
         (D("0.995"), D("100"))),
        min_order_size=D(str(minimum)) if minimum is not None else None,
        tick_size=D("0.01"), timestamp=str(int(time.time() * 1000)),
        book_hash="abc", received_wall=time.time(), best_bid=D("0.39"))


def rules(*, minimum=None, rate="0.07", exponent=1):
    return MarketRules(
        "cond", D(rate), exponent,
        min_order_size=D(str(minimum)) if minimum is not None else None,
        tick_size=D("0.01"), source="venue",
        up_token_id="up", down_token_id="down")


def round_end():
    return (int(time.time()) // 300 + 1) * 300


def paper_order(broker, side="UP", amount=2, up="up", down="down",
                condition="cond", end=None, pre_submit_guard=None):
    return broker.place_trade(side, amount, up, down, condition,
                              round_end() if end is None else end,
                              pre_submit_guard=pre_submit_guard)


def expect_reject(name, fn, contains):
    try:
        fn()
    except PaperRejected as exc:
        check(name, contains.lower() in str(exc).lower(), str(exc))
    except Exception as exc:
        check(name, False, f"wrong exception {type(exc).__name__}: {exc}")
    else:
        check(name, False, "did not reject")


def t_book_parser_is_strict_and_sorted():
    parsed = parse_book({
        "asset_id": "up", "timestamp": int(time.time() * 1000),
        "min_order_size": "5", "bids": [{"price": "0.39", "size": "4"}],
        "asks": [
            {"price": "0.5", "size": "2"},
            {"price": "0.4", "size": "3"},
            {"price": "0.5", "size": "4"},
        ],
    }, "up")
    check("asks sorted best first", parsed.asks[0] == (D("0.4"), D("3")),
          str(parsed.asks))
    check("duplicate ask prices aggregated",
          parsed.asks[1] == (D("0.5"), D("6")), str(parsed.asks))
    check("minimum size parsed", parsed.min_order_size == D("5"))
    expect_reject("wrong book token rejected",
                  lambda: parse_book({"asset_id": "down", "asks": []}, "up"),
                  "does not match")
    expect_reject(
        "one malformed level rejects the whole snapshot",
        lambda: parse_book({"asset_id": "up", "asks": [
            {"price": "0.4", "size": "3"}, {"price": "bad", "size": "10"}
        ]}, "up"), "malformed ask")
    expect_reject(
        "zero-size level rejects the whole snapshot",
        lambda: parse_book({"asset_id": "up", "asks": [
            {"price": "0.3", "size": "0"}
        ]}, "up"), "out-of-range ask")


def t_fok_walks_depth_with_level_fees():
    quote = estimate_fok(book(), 2, 0.99, rules())
    check("FOK spends full requested amount", quote.notional == D("2.000000"),
          str(quote.notional))
    check("multi-level shares correct", approx(quote.shares, 4.6), str(quote.shares))
    check("VWAP correct", approx(quote.average_price, 2 / 4.6),
          str(quote.average_price))
    check("worst consumed ask correct", quote.worst_price == D("0.50"))
    # 3sh @ .40 = .05040 and 1.6sh @ .50 = .02800
    check("fees summed at each price level", quote.fee == D("0.07840"),
          str(quote.fee))
    check("total cost includes taker fee", quote.total_cost == D("2.078400"),
          str(quote.total_cost))


def t_fok_rejects_partial_cap_empty_and_minimum():
    expect_reject("insufficient depth is no fill",
                  lambda: estimate_fok(book(), 9, 0.50, rules()), "FOK no-fill")
    expect_reject("price cap is enforced",
                  lambda: estimate_fok(book(), 2, 0.45, rules()), "FOK no-fill")
    expect_reject("empty book is rejected",
                  lambda: estimate_fok(BookSnapshot("up", ()), 2, .99, rules()),
                  "no asks")
    expect_reject("venue minimum shares enforced",
                  lambda: estimate_fok(book(minimum=5), 2, .99, rules()),
                  "below venue minimum")
    only_fractional_cap = BookSnapshot(
        "up", ((D("0.995"), D("100")),), tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)), best_bid=D("0.99"))
    expect_reject(
        "paper cap is floored to the venue tick exactly like live",
        lambda: estimate_fok(only_fractional_cap, 2, .995, rules()),
        "FOK no-fill")
    expect_reject(
        "book/rules tick mismatch is rejected",
        lambda: estimate_fok(
            BookSnapshot("up", ((D("0.4"), D("10")),),
                         tick_size=D("0.001")), 2, .99, rules()),
        "disagree on tick")


def t_fok_rejects_asks_outside_min_max_band():
    cheap = BookSnapshot(
        "up", ((D("0.08"), D("100")),), tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)), best_bid=D("0.07"))
    expect_reject(
        "best ask below 0.20 is refused",
        lambda: estimate_fok(cheap, 5, 0.90, rules(), min_price=0.20),
        "below MIN_BUY_PRICE")
    rich = BookSnapshot(
        "up", ((D("0.95"), D("100")),), tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)), best_bid=D("0.94"))
    expect_reject(
        "best ask above 0.90 is refused",
        lambda: estimate_fok(rich, 5, 0.90, rules(), min_price=0.20),
        "FOK no-fill")
    mid = BookSnapshot(
        "up", ((D("0.40"), D("100")),), tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)), best_bid=D("0.39"))
    quote = estimate_fok(mid, 5, 0.90, rules(), min_price=0.20)
    check("ask inside 0.20-0.90 fills", quote.worst_price == D("0.40"), str(quote))


def t_paper_sizes_up_to_venue_minimum():
    snap = BookSnapshot(
        "up", ((D("0.99"), D("100")),), min_order_size=D("5"),
        tick_size=D("0.01"), timestamp=str(int(time.time() * 1000)),
        book_hash="min", received_wall=time.time(), best_bid=D("0.98"))
    sized = size_to_venue_minimum(2, snap, rules(minimum=5), 0.99)
    check("two dollars is raised to five shares at 0.99",
          sized == D("4.95"), str(sized))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap, selected_rules=rules(minimum=5))
        check("sized paper FOK fills", paper_order(broker, amount=2) is True)
        check("fill notional meets the minimum",
              approx(broker.last_fill["requested_amount"], 4.95),
              str(broker.last_fill))


def t_paper_refuses_marketable_buys_under_one_dollar():
    # 10 shares at 5c is $0.50: enough shares for the 5-share minimum, but the
    # venue refuses any marketable BUY under $1, so paper must too.
    snap = BookSnapshot(
        "up", ((D("0.05"), D("1000")),), min_order_size=D("5"),
        tick_size=D("0.01"), timestamp=str(int(time.time() * 1000)),
        book_hash="usd", received_wall=time.time(), best_bid=D("0.04"))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap, selected_rules=rules(minimum=5))
        check("a $0.50 paper buy is refused like the venue refuses it",
              paper_order(broker, amount=0.5) is False
              and "min size: $1" in str(broker.last_error), str(broker.last_error))
        check("a $1.00 paper buy at 5c fills (20 shares)",
              paper_order(broker, amount=1.0) is True
              and approx(broker.last_fill["shares"], 20.0), str(broker.last_error))


def t_paper_refreshes_rules_when_the_tick_size_changes():
    import dataclasses
    snap = BookSnapshot(
        "up", ((D("0.035"), D("100")),), min_order_size=D("5"),
        tick_size=D("0.001"), timestamp=str(int(time.time() * 1000)),
        book_hash="tick", received_wall=time.time(), best_bid=D("0.034"))
    for still_stale in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            broker = _broker(tmp, selected_book=snap)
            calls = []

            def fetch(cid):
                calls.append(cid)
                r = rules(minimum=5)
                return r if (len(calls) == 1 or still_stale) else dataclasses.replace(r, tick_size=D("0.001"))
            broker._rules_fetch = fetch
            ok = paper_order(broker, amount=1.0)
            if not still_stale:
                check("cached 0.01 rules vs a 0.001 book: refreshed once, and the order fills",
                      ok is True and len(calls) == 2, f"{ok} {broker.last_error} {calls}")
            else:
                check("if the venue really disagrees after the refresh, it is still refused",
                      ok is False and "tick size" in str(broker.last_error), str(broker.last_error))


def t_paper_tick_refresh_other_directions_and_failures():
    import dataclasses

    def book_at(price, tick):
        return BookSnapshot(
            "up", ((D(price), D("100")),), min_order_size=D("5"), tick_size=D(tick),
            timestamp=str(int(time.time() * 1000)), book_hash="t", received_wall=time.time(),
            best_bid=D(price) - D(tick))

    # 0.001 -> 0.01: the cache still says 0.001, the venue moved back to 0.01.
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=book_at("0.50", "0.01"))
        calls = []

        def fetch(cid):
            calls.append(cid)
            r = rules(minimum=5)
            return dataclasses.replace(r, tick_size=D("0.001")) if len(calls) == 1 else r
        broker._rules_fetch = fetch
        check("cached 0.001 rules vs a 0.01 book: refreshed once, and the order fills",
              paper_order(broker, amount=5.0) is True and len(calls) == 2, f"{broker.last_error} {calls}")
    # The market disappears while refreshing: refuse safely, no retry loop.
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=book_at("0.035", "0.001"))
        calls = []

        def gone(cid):
            calls.append(cid)
            if len(calls) > 1:
                raise ConnectionError("market not found")
            return rules(minimum=5)
        broker._rules_fetch = gone
        ok = paper_order(broker, amount=1.0)
        check("market gone during the refresh: refused, rules fetched exactly twice",
              ok is False and "market rules" in str(broker.last_error) and len(calls) == 2,
              f"{ok} {broker.last_error} {calls}")
    # Rounding to the tick must never lift the price above the cap: a cap of
    # 0.5499 on a 0.01 tick is 0.54, so an ask of 0.55 cannot fill...
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=book_at("0.55", "0.01"), selected_rules=rules(minimum=5))
        ok = broker.place_trade("UP", 5.5, "up", "down", "cond", round_end(), 0.5499, None)
        check("an off-tick cap is floored, never rounded up past the cap",
              ok is False and "no-fill" in str(broker.last_error).lower(), str(broker.last_error))
    # ...while an ask on the floored cap still fills at it.
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=book_at("0.54", "0.01"), selected_rules=rules(minimum=5))
        ok = broker.place_trade("UP", 5.4, "up", "down", "cond", round_end(), 0.5499, None)
        check("...and an ask at the floored cap fills at exactly that price",
              ok is True and approx(broker.last_fill["average_price"], 0.54), str(broker.last_fill))


def t_sniper_signing_price_never_exceeds_the_cap_and_keeps_the_tick():
    import math
    from decimal import Decimal as Dc
    import sniper_mode
    from py_clob_client_v2.order_builder.helpers import round_down
    plain_losses, bad = [], []
    for tick, digits in (("0.01", 2), ("0.001", 3), ("0.0001", 4)):
        step = Dc(tick)
        n = int(1 / step)
        for i in range(1, n):
            on = step * i
            for cap in (float(on), float(on + step / 2)):          # on the tick, and half a tick above
                want = float((Dc(str(cap)) / step).to_integral_value(rounding="ROUND_FLOOR") * step)
                got = round_down(sniper_mode.signing_price(cap, tick), digits)
                if abs(got - want) > 1e-12 or got > cap + 1e-12:
                    bad.append((tick, cap, got, want))
            if round_down(float(on), digits) < float(on) - 1e-12:
                plain_losses.append((tick, float(on)))
    check("the signed price is exactly the cap floored to the tick, never above it, at every price",
          not bad, str(bad[:5]))
    check("(the library's own float floor loses a tick for some prices, e.g. 0.57 -> 0.56)",
          ("0.01", 0.57) in plain_losses, str(plain_losses[:5]))


def t_sniper_stallwatch_names_what_blocked_the_loop_and_stays_quiet_otherwise():
    import sniper_mode

    def blocking_ledger_like_write():
        time.sleep(0.3)

    def spin_on_the_loop():
        t = time.perf_counter()
        while time.perf_counter() - t < 0.2:
            pass

    async def go():
        rows = []
        loop = asyncio.get_running_loop()
        w = sniper_mode.StallWatch(loop, rows.append, threshold_ms=50, poll_s=0.01, sample_ms=50).start()
        await asyncio.sleep(0.4)
        quiet = list(rows)
        loop.call_soon(blocking_ledger_like_write)
        await asyncio.sleep(0.5)
        loop.call_soon(spin_on_the_loop)
        await asyncio.sleep(0.4)
        w.stop()
        return quiet, rows
    quiet, rows = asyncio.run(go())
    stalls = [r for r in rows if r.get("result") == "stall"]
    blocked = [r for r in stalls if any("blocking_ledger_like_write" in fr for s in r["samples"]
                                        for k, frames in s["threads"].items() if k.endswith("*") for fr in frames)]
    spun = [r for r in stalls if any("spin_on_the_loop" in fr for s in r["samples"]
                                     for k, frames in s["threads"].items() if k.endswith("*") for fr in frames)]
    check("no stall rows while the loop is responsive", not quiet, str(quiet)[:200])
    check("a 300ms blocking call on the loop is caught, ~300ms long, with the culprit on the loop thread's stack",
          len(blocked) == 1 and 250 <= blocked[0]["ms"] <= 450, str([(r["ms"], r["samples"][0]) for r in stalls])[:600])
    check("a 200ms busy loop is caught too, with CPU time to match", len(spun) == 1 and spun[0]["cpu_ms"] >= 100,
          str([(r["ms"], r["cpu_ms"]) for r in spun]))


def t_book_history_reads_the_book_as_it_stood_at_a_venue_time():
    from feeds.book import BookState
    T = 1_790_000_000_000
    b = BookState(history_s=30)
    b.connected = True
    b.set_active(["up", "down"])
    b.apply_snapshot("up", [(0.49, 50)], [(0.50, 10), (0.53, 50)], ts_ms=T, require_exchange_ts=False)
    b.apply_price_changes([{"asset_id": "up", "price": 0.50, "size": 0, "side": "SELL"}], ts_ms=T + 100,
                          require_exchange_ts=False)
    asks = lambda t: (lambda v: v[0].asks if v[0] is not None else v[1]["why"])(b.view_at("up", t))
    check("before the change: the snapshot's book; at/after it: the changed book",
          asks(T + 50) == ((0.50, 10.0), (0.53, 50.0)) and asks(T + 100) == ((0.53, 50.0),), f"{asks(T + 50)} {asks(T + 100)}")
    check("a time the feed has not yet passed is not answered (a later change could still arrive)",
          "not yet passed" in str(asks(T + 150)), str(asks(T + 150)))
    b.note_exchange_ts(T + 200)                                   # e.g. a trade message on the same feed
    check("...once anything stamped later arrives, it is", asks(T + 150) == ((0.53, 50.0),), str(asks(T + 150)))
    check("before the history starts: unknown", "does not reach back" in str(asks(T - 10)), str(asks(T - 10)))
    first = b.view_at("up", T + 50)[0]
    b.apply_price_changes([{"asset_id": "up", "price": 0.53, "size": 1, "side": "SELL"}], ts_ms=T + 300,
                          require_exchange_ts=False)
    check("later updates never alter an earlier state", b.view_at("up", T + 50)[0].asks == first.asks,
          str(b.view_at("up", T + 50)[0].asks))
    b.desync_all("disconnect")                                    # gap stamped at the newest time seen (T+300)
    check("a feed gap makes the time after it unknown", "gap" in str(asks(T + 300)), str(asks(T + 300)))
    check("...but the time before it is still known", asks(T + 250) == ((0.53, 50.0),), str(asks(T + 250)))
    b.apply_snapshot("up", [(0.40, 5)], [(0.45, 5)], ts_ms=T + 250, require_exchange_ts=False)
    b.note_exchange_ts(T + 400)
    check("a resync snapshot answers from the gap on, never before it",
          asks(T + 350) == ((0.45, 5.0),) and asks(T + 250) == ((0.53, 50.0),), f"{asks(T + 350)} {asks(T + 250)}")
    v_mono = b.view_at_mono("up", time.monotonic())
    check("view_at_mono gives the book in our hands at a local instant", v_mono is not None and v_mono.asks == ((0.45, 5.0),),
          str(v_mono))
    b.apply_price_changes([{"asset_id": "down", "price": 0.6, "size": 1, "side": "SELL"}], ts_ms=T + 500,
                          require_exchange_ts=False)
    check("feed age is recorded per applied price change (not for a book still waiting for its snapshot)",
          b.feed_age_summary()["n"] == 2, str(b.feed_age_summary()))


def _arrival_broker(tmp, books):
    """A paper broker whose live book changes between reads: books[0] at the
    decision, books[1] when the timer wakes."""
    broker = _broker(tmp)
    seq = list(books)
    broker._book_fetch = lambda token: seq.pop(0) if len(seq) > 1 else seq[0]
    return broker


def _ask_book(price, size=50, bid=None):
    return BookSnapshot("up", ((D(str(price)), D(str(size))),), tick_size=D("0.01"),
                        timestamp=str(int(time.time() * 1000)), book_hash="h", received_wall=time.time(),
                        best_bid=D(str(bid if bid is not None else round(price - 0.01, 2))))


def t_paper_fak_keeps_what_is_left_at_the_cap_where_fok_misses():
    from paper_trade import estimate_fak, estimate_fok

    def thin(levels):
        return BookSnapshot("up", tuple((D(str(p)), D(str(q))) for p, q in levels), tick_size=D("0.01"),
                            timestamp=str(int(time.time() * 1000)), book_hash="t", received_wall=time.time(),
                            best_bid=D("0.49"))
    decision = _ask_book(0.50, size=50)                      # the full stake fills at the decision
    at_match = thin([(0.50, 4), (0.51, 3), (0.60, 50)])     # 7 shares left at or under 0.52
    out = {}
    for mode in ("FOK", "FAK"):
        with tempfile.TemporaryDirectory() as tmp:
            broker = _arrival_broker(tmp, [decision, decision])
            ok = broker.place_trade("UP", 5.0, "up", "down", "cond", round_end(), 0.52, latency_ms=1,
                                    arrival_book=lambda token: (at_match, {"arrival_wall": time.time() - 0.01}),
                                    **({"fak": True} if mode == "FAK" else {}))
            pos = sum(p_.shares for p_ in broker.ledger.positions.values())
            out[mode] = (ok, dict(broker.last_fill or {}) if ok else None, str(broker.last_error or ""), pos,
                         broker.cash_balance())
    ok, fill, err, pos, cash = out["FOK"]
    check("FOK: 7 shares left at the cap for a $5 stake -> no fill, nothing booked",
          ok is False and "fok no-fill" in err.lower() and pos == 0, str(out["FOK"]))
    ok, fill, err, pos, cash = out["FAK"]
    check("FAK: the 7 shares at or under the cap are taken (4 @ 0.50 + 3 @ 0.51), flagged partial, booked",
          ok is True and fill and fill.get("partial") is True and abs(fill["shares"] - 7) < 1e-9
          and abs(fill["requested_amount"] - 3.53) < 1e-9 and abs(fill["stake"] - 5.0) < 1e-9
          and abs(pos - 7) < 1e-9, str(out["FAK"]))
    check("FAK: cash pays only what was filled (+ its fee), not the stake",
          abs((100 - cash) - (3.53 + fill["fee"])) < 1e-6, f"{100 - cash} vs {3.53 + fill['fee']}")
    with tempfile.TemporaryDirectory() as tmp:
        broker = _arrival_broker(tmp, [decision, decision])
        ok = broker.place_trade("UP", 5.0, "up", "down", "cond", round_end(), 0.52, latency_ms=1, fak=True,
                                arrival_book=lambda token: (thin([(0.60, 50)]), {"arrival_wall": time.time() - 0.01}))
        check("FAK: nothing at or under the cap -> a clean miss ('FAK no-fill'), nothing booked",
              ok is False and "fak no-fill" in str(broker.last_error).lower() and not broker.ledger.positions,
              str(broker.last_error))
    full = thin([(0.50, 50)])
    a = estimate_fok(full, 5.0, 0.52, rules())
    b = estimate_fak(full, 5.0, 0.52, rules())
    check("FAK on a book that fills the whole stake is exactly the FOK quote", a == b, f"{a} vs {b}")


def t_paper_sniper_sizes_at_the_limit_and_fak_presend_takes_partial_depth():
    """A wide limit: 10 shares' worth at the ask can be under the venue's 5
    shares AT THE LIMIT, and a big move then left a full fill under 5 shares,
    which paper refused ('below venue minimum') though the venue would not -
    the minimum applies to the order as placed. The sniper sizes as its live
    sender: stake >= 5 x limit."""
    from paper_trade import minimum_spend_at_limit

    def lv(levels, bid):
        return BookSnapshot("up", tuple((D(str(p)), D(str(q))) for p, q in levels), tick_size=D("0.01"),
                            timestamp=str(int(time.time() * 1000)), book_hash="w", received_wall=time.time(),
                            best_bid=D(str(bid)))
    r5 = rules(minimum=5)
    check("minimum stake at a 0.95 limit with a 5-share venue minimum is $4.75",
          minimum_spend_at_limit(lv([(0.30, 50)], 0.29), r5, 0.95) == D("4.75"))
    decision, moved = lv([(0.30, 50)], 0.29), lv([(0.90, 50)], 0.89)      # the ask ran 0.30 -> 0.90
    out = {}
    for flag in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            broker = _broker(tmp, selected_rules=r5)
            broker._book_fetch = lambda token: decision
            ok = broker.place_trade("UP", 3.0, "up", "down", "cond", round_end(), 0.95, latency_ms=1, fak=True,
                                    size_at_limit=flag,
                                    arrival_book=lambda token: (moved, {"arrival_wall": time.time() - 0.01}))
            out[flag] = (ok, dict(broker.last_fill or {}) if ok else None, str(broker.last_error or ""))
    ok, fill, err = out[False]
    check("old sizing: $3 at 0.90 is 3.33 shares -> refused as below the venue minimum",
          ok is False and "below venue minimum" in err, str(out[False]))
    ok, fill, err = out[True]
    check("sized at the limit: $4.75 at 0.90 = 5.28 shares, filled",
          ok is True and fill and abs(fill["requested_amount"] - 4.75) < 1e-9 and fill["shares"] >= 5, str(out[True]))
    thin = lv([(0.50, 4), (0.51, 3)], 0.49)                               # 7 shares, $3.53, for a $5 stake
    for fak in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            broker = _broker(tmp)
            broker._book_fetch = lambda token: thin
            ok = broker.place_trade("UP", 5.0, "up", "down", "cond", round_end(), 0.52, latency_ms=1, fak=fak,
                                    arrival_book=lambda token: (thin, {"arrival_wall": time.time() - 0.01}))
            out[("fak", fak)] = (ok, str(broker.last_error or ""))
    check("pre-send on a thin decision book: FOK refused, FAK sent and partially filled (as live)",
          out[("fak", False)][0] is False and "fok no-fill" in out[("fak", False)][1].lower()
          and out[("fak", True)][0] is True, str(out))


def t_sniper_binance_url_only_spot_btcusdt_bookticker_routes():
    import sniper_mode
    check("default route unchanged (settings line unchanged)",
          _fixed_sniper_settings(SNIPER_BINANCE_URL="").binance_url == sniper_mode.BOOK_TICKER_URL
          and "Binance via" not in _fixed_sniper_settings(SNIPER_BINANCE_URL="").lines()[0])
    for ok_url in ("wss://stream.binance.com:443/ws/btcusdt@bookTicker",
                   "wss://data-stream.binance.vision/ws/btcusdt@bookTicker"):
        st = _fixed_sniper_settings(SNIPER_BINANCE_URL=ok_url)
        check(f"accepted route {ok_url}", st.binance_url == ok_url and "Binance via" in st.lines()[0], st.lines()[0])
    for bad in ("wss://fstream.binance.com/ws/btcusdt@bookTicker",       # the perp: a different market
                "wss://stream.binance.com:9443/ws/ethusdt@bookTicker",   # another coin
                "wss://stream.binance.com:9443/ws/btcusdt@trade",        # another stream
                "ws://stream.binance.com:9443/ws/btcusdt@bookTicker",    # no TLS
                "wss://evil.example/ws/btcusdt@bookTicker"):
        try:
            _fixed_sniper_settings(SNIPER_BINANCE_URL=bad)
            check(f"refused route {bad}", False, "accepted")
        except ValueError:
            check(f"refused route {bad}", True)


def t_sniper_shadow_prices_fak_when_the_fok_misses():
    import sniper_mode
    sn = sniper_mode.Sniper(None, None, settings=_fixed_sniper_settings())
    sn.broker = types.SimpleNamespace(max_spread=0.25, max_buy_price=0.95, min_buy_price=0.05)
    from feeds.book import BookView
    thin = BookView("up", 1, "LIVE", bids=((0.49, 50.0),), asks=((0.50, 4.0), (0.51, 3.0), (0.60, 50.0)),
                    updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)
    full = BookView("up", 1, "LIVE", bids=((0.49, 50.0),), asks=((0.50, 50.0),),
                    updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)
    r_thin = sn._shadow_fill(thin, "up", 5.0, 0.52, rules(), 0.55, 0.0)
    r_full = sn._shadow_fill(full, "up", 5.0, 0.52, rules(), 0.55, 0.0)
    check("FOK misses a thin book; the shadow records what FAK keeps (7 shares) and its 5s markout",
          "miss" in r_thin and abs(r_thin["fak"]["sh"] - 7) < 1e-9 and "mk5" in r_thin["fak"], str(r_thin))
    check("a full FOK fill needs no separate FAK quote", "sh" in r_full and "fak" not in r_full, str(r_full))


def t_sniper_order_type_setting_label_and_live_guard():
    import sniper_mode
    st = _fixed_sniper_settings(SNIPER_ORDER_TYPE="fak")
    check("SNIPER_ORDER_TYPE=fak -> FAK, named in the settings line", st.order_type == "FAK"
          and "-> FAK " in st.lines()[0], st.lines()[0])
    check("default stays FOK (settings line unchanged)", _fixed_sniper_settings(SNIPER_ORDER_TYPE="").order_type
          == "FOK" and "-> FOK " in _fixed_sniper_settings(SNIPER_ORDER_TYPE="").lines()[0])
    try:
        _fixed_sniper_settings(SNIPER_ORDER_TYPE="GTC")
        check("an unknown order type is refused", False, "accepted")
    except ValueError:
        check("an unknown order type is refused", True)
    attached = []
    live = types.SimpleNamespace(attach=attached.append)
    sn_live = sniper_mode.Sniper(None, None, settings=st, live=live)
    check("FAK can attach to live execution; partial-fill accounting has separate regressions",
          attached == [sn_live] and sn_live.s.order_type == "FAK")
    label = sniper_mode.Sniper._outcome(False, "FAK no-fill: nothing at or below 0.52",
                                        {"cap": 0.52, "arrival": {"best_ask": 0.60}})
    check("a FAK miss is labelled like a FOK miss (PRICE_CAP_MISS), not a technical error",
          label == "PRICE_CAP_MISS", label)
    calls = []

    class Spy:
        supports_arrival_book = False

        def place_trade(self, *a, **k):
            calls.append(k.get("fak"))
            self.last_fill, self.last_timing = None, {}
            return False
    sn = sniper_mode.Sniper(None, Spy(), settings=_fixed_sniper_settings(SNIPER_ORDER_TYPE="FAK"))
    sn._match({"send": ("UP", 5.0, "u", "d", "c", round_end(), 0.52, "u", "0.01", 1.0), "sent_wall": time.time(),
               "sent_mono": time.monotonic()}, None, {"sent_pc": time.perf_counter_ns()})
    sn2 = sniper_mode.Sniper(None, Spy(), settings=_fixed_sniper_settings())
    sn2._match({"send": ("UP", 5.0, "u", "d", "c", round_end(), 0.52, "u", "0.01", 1.0), "sent_wall": time.time(),
                "sent_mono": time.monotonic()}, None, {"sent_pc": time.perf_counter_ns()})
    check("the sniper hands fak=True to the paper broker only when FAK is set", calls == [True, None], str(calls))


def t_paper_fills_against_the_book_at_the_modelled_arrival_not_at_wake():
    good, gone = _ask_book(0.50), _ask_book(0.60)
    cases = {}
    for name, wake, at_arrival in (("late wake saw a worse book", gone, good),
                                   ("late wake saw a better book", good, gone)):
        with tempfile.TemporaryDirectory() as tmp:
            broker = _arrival_broker(tmp, [good, wake])
            ok = broker.place_trade("UP", 2, "up", "down", "cond", round_end(), 0.55, latency_ms=1,
                                    arrival_book=lambda token, b=at_arrival: (b, {"arrival_wall": time.time() - 0.01}))
            cases[name] = (ok, broker.last_timing.get("arrival", {}).get("wake", {}).get("fill"),
                           str(broker.last_error or ""))
    check("the arrival book decides: a fill though the late wake saw the ask gone (old rule: false miss)",
          cases["late wake saw a worse book"][:2] == (True, False), str(cases))
    check("...and a miss though the late wake saw a cheap ask (old rule: false fill)",
          cases["late wake saw a better book"][:2] == (False, True)
          and "fok no-fill" in cases["late wake saw a better book"][2].lower(), str(cases))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _arrival_broker(tmp, [good, good])
        ok = broker.place_trade("UP", 2, "up", "down", "cond", round_end(), 0.55, latency_ms=1,
                                arrival_book=lambda token: (None, {"why": "the feed has not yet passed that time"}))
        check("an unknown arrival book is refused as unknown - never filled, even when the wake book would fill",
              ok is False and "unknown book state" in str(broker.last_error).lower()
              and broker.last_timing["arrival"]["wake"]["fill"] is True, str(broker.last_error))
        check("...and nothing reached the ledger", not broker.ledger.positions, str(broker.ledger.positions))


def t_sniper_arrival_book_waits_for_the_feed_then_answers_or_says_unknown():
    import threading
    import sniper_mode
    from feeds.book import BookState
    now_ms = time.time() * 1000.0
    b = BookState()
    b.connected = True
    b.set_active(["up", "down"])
    b.apply_snapshot("up", [(0.49, 50)], [(0.50, 10)], ts_ms=int(now_ms - 1000), require_exchange_ts=False)
    hub = types.SimpleNamespace(book=b)
    with tempfile.TemporaryDirectory() as tmp:
        sn = sniper_mode.Sniper(hub, _broker(tmp), settings=sniper_mode.Settings())
        sn.s.book_wait_ms = 300
        arrival = now_ms + 50
        threading.Timer(0.12, lambda: b.apply_price_changes(
            [{"asset_id": "up", "price": 0.50, "size": 0, "side": "SELL"},
             {"asset_id": "up", "price": 0.52, "size": 10, "side": "SELL"}],
            ts_ms=int(arrival + 30), require_exchange_ts=False)).start()
        t = time.perf_counter()
        snap, info = sn._arrival_book("up", arrival, time.monotonic() + 0.05)
        waited = (time.perf_counter() - t) * 1000
        check("waits until the feed carries a change stamped after the arrival, then gives the book AT the arrival "
              "(0.50, not the 0.52 that came after)",
              snap is not None and float(snap.asks[0][0]) == 0.50 and 100 <= waited < 300 and info["book_wait_ms"] >= 100,
              f"{snap} {info} waited {waited:.0f}ms")
        snap2, info2 = sn._arrival_book("up", time.time() * 1000.0 + 5000, time.monotonic())
        check("a feed that never passes the arrival within the wait: unknown, with the reason",
              snap2 is None and "not yet passed" in info2.get("why", ""), str(info2))


def t_sniper_labels_every_outcome():
    import sniper_mode
    o = sniper_mode.Sniper._outcome
    cases = [o(True, None, {}),
             o(False, "unknown book state at the modelled arrival: feed gap", {}),
             o(False, "FOK no-fill: only $1 available at or below 0.52", {"cap": 0.52, "arrival": {"best_ask": 0.55}}),
             o(False, "FOK no-fill: only $1 available at or below 0.52", {"cap": 0.52, "arrival": {"best_ask": 0.51}}),
             o(False, "FOK no-fill: only $1 available at or below 0.52", {"cap": 0.52}),
             o(False, "FOK no-fill: only $1 available at or below 0.52", {"cap": 0.52}, False),
             o(False, "public order book is stale in hand (held=9s)", {}),
             o(False, "order size 4.9 shares is below venue minimum 5", {}),
             o(False, "something else", {})]
    check("outcomes: FILLED / UNKNOWN / PRICE_CAP_MISS / INSUFFICIENT_LIQUIDITY / pre-send miss / old-rule miss / "
          "STALE / MIN / TECHNICAL",
          cases == ["FILLED", "UNKNOWN_BOOK_STATE", "PRICE_CAP_MISS", "INSUFFICIENT_LIQUIDITY", "MISS_AT_DECISION",
                    "FOK_MISS_AT_WAKE", "STALE_BOOK", "BELOW_VENUE_MINIMUM", "TECHNICAL_ERROR"], str(cases))


def t_sniper_gc_runs_only_in_the_rounds_no_trade_seconds():
    import sniper_mode
    rows = []

    class Hub:
        window_end = time.time() + 6.0            # 6s left: inside (2, min_left - 2) = (2, 8)
    old = gc_threshold = __import__("gc").get_threshold()

    async def go():
        sn = sniper_mode.Sniper(Hub(), None, settings=_fixed_sniper_settings())
        sn._write = rows.append
        task = asyncio.ensure_future(sn._gc_schedule())
        await asyncio.sleep(1.2)
        Hub.window_end = time.time() + 120.0     # mid-round: must not collect
        await asyncio.sleep(1.2)
        task.cancel()
        return sn
    try:
        sn = asyncio.run(go())
        th = __import__("gc").get_threshold()
    finally:
        __import__("gc").set_threshold(*old)
    gcs = [r for r in rows if r.get("result") == "gc"]
    check("one scheduled full collection in the no-trade seconds, none mid-round; gen-2 made rare",
          len(gcs) == 1 and 2.0 < gcs[0]["left_s"] < 8.0 and th[2] == 1000, f"{gcs} {th}")


def t_rescore_walks_the_recorded_book_like_a_venue_fok():
    import math
    import numpy as np
    import sniper_rescore as R
    nan = float("nan")
    cases = {
        "fills at the best ask": R.fok([(0.50, 10), (0.51, 50)], 5.0, 0.52)[:3] == ("fill", 10.0, 0.5),
        "walks levels up to the cap": (lambda x: x[0] == "fill" and abs(x[2] - (2.5 + 2.55) / (5 + 5)) < 1e-9)(
            R.fok([(0.50, 5), (0.51, 5), (0.60, 50)], 5.05, 0.52)),
        "best ask over the cap: miss (cap)": R.fok([(0.53, 100)], 5.0, 0.52) == ("miss", "cap"),
        "not enough under the cap: miss (depth)": R.fok([(0.50, 2), (0.53, 100)], 5.0, 0.52)
        == ("miss", "depth under cap"),
        "recorded depth ends under the cap: unknown": R.fok([(0.50, 2), (nan, nan)], 5.0, 0.52)[0] == "unknown",
        "under 5 shares: miss": R.fok([(0.50, 100)], 2.0, 0.52) == ("miss", "below venue minimum"),
        "fee 0.07*p*(1-p) per share": abs(R.fok([(0.50, 10)], 5.0, 0.52)[3] - 0.175) < 1e-12,
    }
    v = {"asks": np.array([[[nan, nan]], [[nan, nan]]]), "ba": np.array([0.44, 1.0])}
    cases["no ask levels on record but a best ask under $1: unknown, not a miss"] = \
        R.walk(v, 0, 5.0, 0.46)[0] == "unknown" and R.walk(v, 1, 5.0, 1.0) == ("miss", "no asks")
    m, se, n = R.clustered([1.0, 3.0, 2.0, 6.0], ["a", "a", "b", "b"])
    cases["round-clustered error: two rounds (2, 4) around mean 3 -> se 1"] = \
        abs(m - 3.0) < 1e-12 and abs(se - math.sqrt((4 + 4) / 16 * 2)) < 1e-12 and n == 4
    bad = [k for k, ok in cases.items() if not ok]
    check("sniper_rescore: FOK walk, missing data and clustered errors", not bad, str(bad))


def t_sniper_visible_ask_fast_path_gives_the_same_answer():
    import sniper_mode
    from feeds.book import BookView
    view = BookView("up", 1, "LIVE", bids=((0.51, 50.0),), asks=((0.52, 12.0), (0.53, 100.0)),
                    updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)
    snap = snapshot_from_book_view(view, expected_token="up")
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap, selected_rules=rules(minimum=5))
        broker.own_fill_ttl_s = 0.3
        sn = sniper_mode.Sniper(None, broker, settings=_fixed_sniper_settings())

        def slow(token):
            book = broker._net_of_own_fills(snapshot_from_book_view(view, expected_token=token))
            return float(book.asks[0][0]) if book.asks else None
        cases = []
        cases.append(("no fills yet", sn._visible_ask("up", view), slow("up"), broker.has_recent_own_fill("up")))
        paper_order(broker, amount=6.24)                      # takes 12 shares at 0.52: the level is gone
        cases.append(("own fill inside the window", sn._visible_ask("up", view), slow("up"),
                      broker.has_recent_own_fill("up")))
        time.sleep(0.35)
        cases.append(("own fill expired", sn._visible_ask("up", view), slow("up"),
                      broker.has_recent_own_fill("up")))
    check("fast and full paths agree in every state (raw 0.52, netted 0.53, expired 0.52)",
          [(c[1], c[2]) for c in cases] == [(0.52, 0.52), (0.53, 0.53), (0.52, 0.52)]
          and [c[3] for c in cases] == [False, True, False], str(cases))


def t_sniper_prefetches_market_rules_once_per_round():
    import asyncio
    import sniper_mode

    class Broker:
        def __init__(self):
            self.calls = []

        def _rules(self, cond):
            self.calls.append(cond)

    class Hub:
        condition_id = "cond-1"

    hub, broker = Hub(), Broker()
    sn = sniper_mode.Sniper(hub, broker, settings=_fixed_sniper_settings())

    async def go():
        task = asyncio.get_running_loop().create_task(sn._prefetch_rules())
        await asyncio.sleep(1.2)
        hub.condition_id = "cond-2"
        await asyncio.sleep(1.2)
        task.cancel()
    asyncio.run(go())
    check("market rules fetched once at each round start, not per order",
          broker.calls == ["cond-1", "cond-2"], str(broker.calls))


def t_paper_sizes_up_when_top_of_book_cannot_fill_the_minimum():
    """$2.50 at a 0.50 best ask is 5 shares only if that level has the size.

    A 3-share top and the rest at 0.51 is the live btc-updown-5m case that
    produced `order size 4.959885 shares is below venue minimum 5`.
    """
    thin = BookSnapshot(
        "up", ((D("0.50"), D("3")), (D("0.51"), D("10"))),
        min_order_size=D("5"), tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)),
        book_hash="thin", received_wall=time.time(), best_bid=D("0.49"))
    import config
    saved = config.VENUE_MIN_PRICE_HEADROOM
    try:
        config.VENUE_MIN_PRICE_HEADROOM = 0.0
        sized = size_to_venue_minimum(2.50, thin, rules(minimum=5), 0.90)
    finally:
        config.VENUE_MIN_PRICE_HEADROOM = saved
    check("thin top of book sizes up past best-ask notional",
          sized == D("2.52"), str(sized))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=thin, selected_rules=rules(minimum=5))
        check("thin-book FOK fills after sizing up",
              paper_order(broker, amount=2.50) is True, broker.last_error)
        check("filled shares meet the venue minimum",
              broker.last_fill["shares"] >= 5,
              str(broker.last_fill))


def t_paper_uses_live_ws_asks_when_rest_is_empty():
    view = types.SimpleNamespace(
        token="up", status="LIVE", asks=((0.76, 50.0),),
        best_bid=0.68, exchange_ts_ms=int(time.time() * 1000),
        tick_size=0.01, hash="ws")
    snap = snapshot_from_book_view(view, expected_token="up")
    check("websocket view carries asks", snap.asks[0][0] == D("0.76"), str(snap.asks))
    preferred = fetch_executable_book(
        "up", host="https://public.invalid", ws_view=view)
    check("LIVE websocket book is preferred over REST",
          preferred.asks[0][0] == D("0.76"), str(preferred.asks))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap, selected_rules=rules(minimum=5))
        check("paper FOK fills from the websocket-shaped book",
              paper_order(broker, amount=5) is True)


def t_paper_ws_book_without_exchange_ts_falls_back_to_rest():
    """Receipt time is not exchange time.

    Stamping `now` on a socket book that carried no exchange timestamp makes a
    silently gapped feed look perfectly fresh, which defeats the staleness
    check the whole FOK path depends on. The snapshot refuses it, and the
    executable path falls through to the REST book, which carries the venue's
    own timestamp.
    """
    import paper_trade

    view = types.SimpleNamespace(
        token="up", status="LIVE", asks=((0.55, 80.0),),
        best_bid=0.54, exchange_ts_ms=None,
        tick_size=0.01, hash="ws-no-ts")
    expect_reject(
        "a socket book with no exchange timestamp is refused",
        lambda: snapshot_from_book_view(view, expected_token="up"),
        "exchange timestamp")

    rest = BookSnapshot(token_id="up", asks=((D("0.60"), D("40")),),
                        best_bid=D("0.59"), tick_size=D("0.01"),
                        timestamp=str(int(time.time() * 1000)),
                        book_hash="rest", received_wall=time.time())
    original = paper_trade.fetch_public_book
    paper_trade.fetch_public_book = lambda token, *, host, timeout=8.0: rest
    try:
        preferred = fetch_executable_book(
            "up", host="https://public.invalid", ws_view=view)
    finally:
        paper_trade.fetch_public_book = original
    check("the untimestamped socket book is not used for the fill",
          preferred.asks[0][0] == D("0.60"), str(preferred.asks))
    check("the REST book's own venue timestamp is kept",
          preferred.timestamp == rest.timestamp, str(preferred.timestamp))


def t_paper_quiet_book_is_not_mistaken_for_a_stale_one():
    """Paper FOK must use the same held/quiet split as the live book parser.

    The previous check treated exchange-timestamp age as copy freshness, so a
    book the venue had not changed for more than 8s was refused even though
    REST had just returned it. On btc-updown-5m, 33s+ gaps between changes
    are ordinary.
    """
    from dataclasses import replace

    import timer

    feeds = pathlib.Path(__file__).with_name("run.py").read_text(encoding="utf-8")
    check("runner wires the quiet bound into paper",
          "max_quiet_s=config.ORDERBOOK_MAX_QUIET_SECONDS" in feeds)
    check("runner wires the future-dated bound into paper",
          "future_tol_s=config.ORDERBOOK_FUTURE_TOLERANCE_SECONDS" in feeds)

    now = timer.wall()

    def snap(*, quiet_s=1.0, held_s=0.05):
        return replace(
            book(),
            timestamp=str(int((now - quiet_s) * 1000)),
            received_wall=now - held_s,
        )

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap(quiet_s=95.0))
        check("a book unchanged for 95s still fills",
              paper_order(broker) is True, broker.last_error)

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap(held_s=40.0))
        check("a copy held 40s is refused",
              paper_order(broker) is False, broker.last_error)
        check("held refusal names the copy, not quiet time",
              "stale in hand" in (broker.last_error or ""), broker.last_error)

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap(quiet_s=1200.0))
        check("a book unchanged past the frozen-venue bound is refused",
              paper_order(broker) is False, broker.last_error)
        check("the frozen-venue refusal names the cause",
              "not changed" in (broker.last_error or ""), broker.last_error)

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap(quiet_s=-30.0))
        check("a book dated well ahead of us is refused",
              paper_order(broker) is False, broker.last_error)
        check("the future refusal names the clock",
              "future-dated" in (broker.last_error or ""), broker.last_error)

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap(quiet_s=-2.0))
        check("a book inside the future tolerance still fills",
              paper_order(broker) is True, broker.last_error)


def t_paper_ws_held_age_uses_socket_receipt_not_conversion_time():
    """Stamping ``now`` at conversion would hide an already-old LIVE book."""
    import timer

    now_ms = int(timer.wall() * 1000)
    view = types.SimpleNamespace(
        token="up", status="LIVE", asks=((0.55, 80.0),),
        best_bid=0.54, exchange_ts_ms=now_ms - 95_000,
        tick_size=0.01, hash="ws-quiet",
        updated_mono=time.monotonic() - 40.0,
    )
    snap = snapshot_from_book_view(view, expected_token="up")
    held = timer.wall() - snap.received_wall
    check("socket receipt age is preserved on the snapshot",
          39.0 <= held <= 41.0, str(held))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap)
        check("an old socket copy is refused as stale in hand",
              paper_order(broker) is False, broker.last_error)
        check("the refusal is held-age, not quiet-age",
              "stale in hand" in (broker.last_error or ""), broker.last_error)


def t_dynamic_v2_fee_curve_and_missing_fee_rejection():
    metadata = {
        "condition_id": "cond", "seconds_delay": 0,
        "tokens": [{"outcome": "Up", "token_id": "up"},
                   {"outcome": "Down", "token_id": "down"}],
    }
    parsed = parse_market_rules(
        {"mos": 5, "mts": .01, "fd": {"r": .25, "e": 2, "to": True},
         "t": [{"o": "Up", "t": "up"}, {"o": "Down", "t": "down"}],
         "itode": True},
        "cond", market_data=metadata)
    check("V2 fee rate parsed", parsed.fee_rate == D("0.25"), str(parsed))
    check("V2 fee exponent parsed", parsed.fee_exponent == 2, str(parsed))
    check("V2 exponent applied",
          curve_fee(D("100"), D("0.5"), parsed) == D("1.56250"),
          str(curve_fee(D("100"), D("0.5"), parsed)))
    expect_reject(
        "missing venue fee cannot produce exact paper PnL",
        lambda: parse_market_rules(
            {"mos": 5,
             "t": [{"o": "Up", "t": "up"}, {"o": "Down", "t": "down"}]},
            "cond", category="crypto", market_data=metadata),
        "fee parameters")
    # The venue marks whether the published curve is actually charged to the
    # taker. Without that flag the rate is not known to apply, and a paper
    # fill that guesses either way misstates PnL.
    expect_reject(
        "fee data without the venue taker flag is not simulated",
        lambda: parse_market_rules(
            {"mos": 5, "mts": .01, "fd": {"r": .07, "e": 1},
             "t": [{"o": "Up", "t": "up"}, {"o": "Down", "t": "down"}]},
            "cond", category="crypto", market_data=metadata),
        "fee parameters")
    check("actual public seconds_delay is used, not itode guesswork",
          parsed.taker_delay_ms == 0.0, str(parsed.taker_delay_ms))

    unknown_delay = parse_market_rules(
        {"mos": 5, "mts": .01, "fd": {"r": .07, "e": 1, "to": True}, "itode": True,
         "t": [{"o": "Up", "t": "up"}, {"o": "Down", "t": "down"}]},
        "cond")
    check("itode alone never invents a delay duration",
          unknown_delay.taker_delay_ms is None, str(unknown_delay.taker_delay_ms))


def _broker(tmp, *, balance=100, selected_book=None, selected_rules=None,
            context=None):
    tmp = pathlib.Path(tmp)
    ledger = Ledger(path=str(tmp / "paper_ledger.json"))
    ctx = context or {"condition_id": "cond", "up_token_id": "up",
                      "down_token_id": "down"}
    broker = PaperBroker(
        ledger, market_context=lambda: dict(ctx), host="https://public.invalid",
        max_buy_price=.99, start_balance=balance,
        account_path=tmp / "paper_account.json",
        audit_path=tmp / "paper_orders.jsonl",
        book_fetch=lambda token: selected_book or book(token),
        rules_fetch=lambda cid: selected_rules or rules(),
        min_seconds_to_expiry=0, trade_window_seconds=300,
    )
    return broker


def t_broker_records_only_realistic_fills_and_cash():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        check("paper FOK reports filled", paper_order(broker))
        summary = broker.summary(mark=lambda _token: .39)
        check("one fill persisted", summary["fills_counted"] == 1, str(summary))
        check("paper cash deducts notional plus fee",
              approx(summary["cash"], 100 - 2.0784), str(summary["cash"]))
        check("open position marked to bid",
              approx(summary["unrealized_mark_to_bid"], 4.6 * .39 - 2.0784),
              str(summary))
        rows = [json.loads(line) for line in
                (pathlib.Path(tmp) / "paper_orders.jsonl").read_text().splitlines()]
        check("full execution audit written", rows[0]["status"] == "FILLED" and
              len(rows[0]["levels"]) == 2, str(rows))


def t_paper_pre_submit_guard_runs_after_latency_and_quote():
    import paper_trade as paper

    events = []
    allowed = {"value": True}
    original_sleep = paper.time.sleep
    original_estimate = paper.estimate_fok

    def tracked_estimate(*args, **kwargs):
        quote = original_estimate(*args, **kwargs)
        events.append("quote")
        allowed["value"] = False
        return quote

    def guard():
        events.append("guard")
        return allowed["value"]

    paper.time.sleep = lambda _seconds: events.append("latency")
    paper.estimate_fok = tracked_estimate
    try:
        with tempfile.TemporaryDirectory() as tmp:
            broker = _broker(tmp)
            broker.latency_ms = 25.0
            accepted = paper_order(broker, pre_submit_guard=guard)
            check("paper guard runs after modeled latency and final quote",
                  events == ["quote", "latency", "quote", "guard"], str(events))
            check("signal flip during paper latency/quote rejects the fill",
                  accepted is False and not broker.ledger.seen,
                  f"accepted={accepted} seen={broker.ledger.seen}")
            check("paper guard rejection is explicit",
                  "pre-submit guard rejected" in (broker.last_error or "").lower(),
                  str(broker.last_error))

            def broken_guard():
                raise RuntimeError("do-not-persist-this-detail")

            accepted = paper_order(broker, pre_submit_guard=broken_guard)
            audit = (pathlib.Path(tmp) / "paper_orders.jsonl").read_text()
            check("paper guard exception fails closed without a fill",
                  accepted is False and not broker.ledger.seen,
                  f"accepted={accepted} seen={broker.ledger.seen}")
            check("paper guard exception is clear but redacted in durable audit",
                  "pre-submit guard failed closed: RuntimeError" in audit
                  and "do-not-persist-this-detail" not in audit,
                  audit)
    finally:
        paper.estimate_fok = original_estimate
        paper.time.sleep = original_sleep


def t_unmarkable_open_position_never_fakes_total_equity():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        paper_order(broker)
        summary = broker.summary(mark=lambda _token: None)
        check("unmarkable paper position is flagged",
              summary["unmarkable_positions"] == 1, str(summary))
        check("unmarkable paper position withholds equity",
              summary["equity"] is None and summary["total_pnl"] is None,
              str(summary))


def t_rejections_never_create_positions():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=book(minimum=100))
        check("unfillable paper order returns false",
              paper_order(broker) is False)
        check("rejection creates no ledger fill",
              broker.ledger.summary()["fills_counted"] == 0,
              str(broker.ledger.summary()))
        check("rejection reason retained",
              "minimum" in (broker.last_error or "").lower()
              or "no-fill" in (broker.last_error or "").lower()
              or "no fill" in (broker.last_error or "").lower(),
              str(broker.last_error))


def t_cash_and_round_mapping_are_fail_closed():
    with tempfile.TemporaryDirectory() as tmp:
        poor = _broker(tmp, balance=1)
        check("insufficient paper cash rejects",
              paper_order(poor) is False)
        check("cash reject records no fill", not poor.ledger.seen)
    with tempfile.TemporaryDirectory() as tmp:
        mismatch = _broker(tmp)
        check("wrong-round token rejects",
              paper_order(mismatch, up="old-up") is False)
        check("wrong-round token never reaches ledger", not mismatch.ledger.seen)
    with tempfile.TemporaryDirectory() as tmp:
        missing = _broker(tmp, context={"up_token_id": "up", "down_token_id": "down"})
        check("missing condition rejects because PnL could not settle",
              paper_order(missing) is False)
        check("missing condition creates no fill", not missing.ledger.seen)


def t_official_resolution_credits_cash_and_pnl_once():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        paper_order(broker)
        before = broker.cash_balance()
        done = broker.ledger.settle(Resolution(
            "cond", RESOLVED, {"up": 1.0, "down": 0.0}, winning_label="Up"))
        after = broker.cash_balance()
        check("venue resolution settles paper position", len(done) == 1)
        check("winner payout credited to paper cash once",
              approx(after, before + 4.6), f"{before} -> {after}")
        check("realized PnL includes exact fee",
              approx(broker.summary()["realized_pnl"], 4.6 - 2.0784),
              str(broker.summary()))
        broker.ledger.settle(Resolution(
            "cond", RESOLVED, {"up": 1.0, "down": 0.0}, winning_label="Up"))
        check("repeat resolution cannot double-credit", approx(broker.cash_balance(), after))


def t_background_settlement_credits_cash_once_and_survives_reload():
    async def scenario(tmp):
        broker = _broker(tmp)
        paper_order(broker)
        before = broker.cash_balance()
        fetches = []

        def fetch(cid):
            fetches.append(cid)
            if len(fetches) == 1:
                return Resolution(cid, PENDING, {}, detail="venue still open")
            return Resolution(cid, RESOLVED, {"up": 1.0, "down": 0.0},
                              winning_label="Up")

        worker = SettlementWorker(broker.ledger, fetch=fetch, interval=0.01)
        worker.start()
        try:
            deadline = time.monotonic() + 1.0
            while (time.monotonic() < deadline
                   and not broker.ledger.positions["up"].settled):
                await asyncio.sleep(0.005)
            after = broker.cash_balance()
            check("background settlement observes pending before resolved",
                  len(fetches) >= 2, str(fetches))
            check("background worker settles the paper position",
                  broker.ledger.positions["up"].settled, worker.summary())
            check("background settlement credits the exact winner payout once",
                  approx(after, before + 4.6), f"{before} -> {after}")
            surfaced = broker.get_balance_allowance()
            check("settled payout is immediately visible through the balance API",
                  approx(surfaced["balance"], after)
                  and approx(surfaced["allowance"], after),
                  str(surfaced))
            check("settled position leaves no work for another poll",
                  await worker.run_once() == 0, str(worker.summary()))
            check("repeat polling cannot double-credit background settlement",
                  approx(broker.cash_balance(), after), str(broker.cash_balance()))
        finally:
            await worker.stop()

        reloaded = _broker(tmp, balance=999999)
        check("background settlement survives ledger reload",
              reloaded.ledger.positions["up"].settled,
              str(reloaded.ledger.positions["up"]))
        check("reloaded broker reconstructs the credited cash exactly",
              approx(reloaded.cash_balance(), after),
              f"{reloaded.cash_balance()} vs {after}")

    with tempfile.TemporaryDirectory() as tmp:
        asyncio.run(scenario(tmp))


def t_execution_window_covers_phase2_and_refuses_earlier_orders():
    """The broker's execution interval matches the configured phase-2 reach."""
    import config

    check("the execution window covers phase 2",
          config.EXECUTION_WINDOW_SECONDS == config.TRADE_LAST_SECONDS,
          str(config.EXECUTION_WINDOW_SECONDS))

    import timer as timer_mod
    from dataclasses import replace

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        # Size the interval the way run.py does.
        broker.trade_window_seconds = float(config.EXECUTION_WINDOW_SECONDS)
        end = round_end()
        original_unix = timer_mod.unix
        original_wall = timer_mod.wall
        # Stand just inside phase 2's opening boundary.
        sampled = end - config.TRADE_LAST_SECONDS + 1.0
        timer_mod.unix = lambda *_a, **_k: sampled
        timer_mod.wall = lambda *_a, **_k: sampled
        try:
            # Interval uses Unix; book freshness uses CLOB wall. Keep them
            # on the same synthetic clock so this is an interval test.
            broker._book_fetch = lambda token: replace(
                book(),
                timestamp=str(int(sampled * 1000)),
                received_wall=sampled,
            )
            accepted = paper_order(broker, amount=2.5, end=end)
        finally:
            timer_mod.unix = original_unix
            timer_mod.wall = original_wall
        check("a phase-2 timestamp is not refused as out-of-interval",
              "outside the current round execution interval"
              not in (broker.last_error or ""), str(broker.last_error))
        check("a phase-2 order fills against a contemporaneous book",
              accepted is True, broker.last_error)

        # The guard still bites before phase 2 opens.
        timer_mod.unix = lambda *_a, **_k: end - config.TRADE_LAST_SECONDS - 1.0
        try:
            paper_order(broker, amount=2.5, end=end)
        finally:
            timer_mod.unix = original_unix
        check("a timestamp before phase 2 still is refused",
              "outside the current round execution interval"
              in (broker.last_error or ""), str(broker.last_error))
    tree = ast.parse((ROOT / "run.py").read_text(encoding="utf-8"))
    runner = next((node for node in ast.walk(tree)
                   if isinstance(node, ast.AsyncFunctionDef)
                   and node.name == "run_runtime"), None)
    check("feed runner still owns settlement startup", runner is not None)
    if runner is None:
        return

    # Scan the whole runner module, not just `_run_inner`: startup is split
    # across helpers it calls, and the invariant is that run.py builds one
    # worker at one second - not which function holds the call.
    constructors = [node for node in ast.walk(tree)
                    if isinstance(node, ast.Call)
                    and ast.unparse(node.func) == "SettlementWorker"]
    check("feed runner constructs exactly one settlement worker",
          len(constructors) == 1, str(len(constructors)))
    if len(constructors) != 1:
        return

    interval = next((kw.value for kw in constructors[0].keywords
                     if kw.arg == "interval"), None)
    check("production settlement polling is explicitly one second",
          isinstance(interval, ast.Constant) and interval.value == 1.0,
          "missing interval" if interval is None else ast.unparse(interval))


def t_directory_fsync_permission_denied_does_not_block_persist():
    import os
    from accounting.ledger import _fsync_directory
    with tempfile.TemporaryDirectory() as tmp:
        real_open = os.open

        def deny_dir(path, flags, *args, **kwargs):
            if flags == os.O_RDONLY and pathlib.Path(path).is_dir():
                raise PermissionError(13, "Permission denied", path)
            return real_open(path, flags, *args, **kwargs)

        os.open = deny_dir
        try:
            _fsync_directory(tmp)
            broker = _broker(tmp)
            check("paper account created without directory fsync",
                  (pathlib.Path(tmp) / "paper_account.json").exists())
            check("ledger save survives directory PermissionError",
                  broker.ledger.save())
        finally:
            os.open = real_open
    with tempfile.TemporaryDirectory() as tmp:
        first = _broker(tmp, balance=100)
        paper_order(first)
        cash = first.cash_balance()
        second_ledger = Ledger(path=str(pathlib.Path(tmp) / "paper_ledger.json"))
        second = PaperBroker(
            second_ledger,
            market_context=lambda: {"condition_id": "cond", "up_token_id": "up",
                                    "down_token_id": "down"},
            host="https://public.invalid", max_buy_price=.99,
            start_balance=999999,
            account_path=pathlib.Path(tmp) / "paper_account.json",
            audit_path=pathlib.Path(tmp) / "paper_orders.jsonl",
            book_fetch=lambda token: book(token), rules_fetch=lambda cid: rules(),
            min_seconds_to_expiry=0, trade_window_seconds=300)
        check("saved starting balance wins after restart", second.start_balance == 100,
              str(second.start_balance))
        check("restart reconstructs identical cash", approx(second.cash_balance(), cash),
              f"{second.cash_balance()} vs {cash}")
        check("restart preserves deduplicated fills", len(second.ledger.seen) == 1)


def t_missing_account_file_cannot_silently_reset_existing_ledger():
    with tempfile.TemporaryDirectory() as tmp:
        first = _broker(tmp, balance=100)
        paper_order(first)
        (pathlib.Path(tmp) / "paper_account.json").unlink()
        loaded = Ledger(path=str(pathlib.Path(tmp) / "paper_ledger.json"))
        try:
            PaperBroker(
                loaded,
                market_context=lambda: {"condition_id": "cond", "up_token_id": "up",
                                        "down_token_id": "down"},
                host="https://public.invalid", max_buy_price=.99,
                start_balance=100,
                account_path=pathlib.Path(tmp) / "paper_account.json",
                audit_path=pathlib.Path(tmp) / "paper_orders.jsonl",
                book_fetch=lambda token: book(token), rules_fetch=lambda cid: rules(),
                min_seconds_to_expiry=0, trade_window_seconds=300)
        except RuntimeError as exc:
            check("existing ledger without account fails closed",
                  "contains fills" in str(exc), str(exc))
        else:
            check("existing ledger without account fails closed", False,
                  "paper account was silently recreated")


def t_paper_account_and_ledger_have_one_durable_identity():
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        broker = _broker(root, balance=123)
        account = json.loads((root / "paper_account.json").read_text())
        ledger = json.loads((root / "paper_ledger.json").read_text())
        check("new paper account uses portable identity schema",
              account.get("version") == 2, str(account))
        check("new paper ledger identity is durable before the first fill",
              ledger.get("version") == 4 and
              ledger.get("ledger_id") == account.get("ledger_id") == broker.ledger.ledger_id,
              f"account={account.get('ledger_id')} ledger={ledger.get('ledger_id')}")


def t_paper_state_directory_move_migrates_legacy_pair_without_reset():
    with tempfile.TemporaryDirectory() as tmp:
        base = pathlib.Path(tmp)
        source = base / "old-location"
        source.mkdir()
        first = _broker(source, balance=100)
        check("legacy-move fixture has a durable fill", paper_order(first))
        cash_before = first.cash_balance()

        # Recreate the exact pre-fix V1/V3 schemas, including the absolute old
        # path that used to make a whole-directory move unbootable.
        account_path = source / "paper_account.json"
        account = json.loads(account_path.read_text())
        account["version"] = 1
        account.pop("ledger_id", None)
        account_path.write_text(json.dumps(account), encoding="utf-8")
        ledger_path = source / "paper_ledger.json"
        ledger = json.loads(ledger_path.read_text())
        ledger["version"] = 3
        ledger.pop("ledger_id", None)
        ledger_path.write_text(json.dumps(ledger), encoding="utf-8")

        destination = base / "new-location"
        shutil.move(str(source), str(destination))
        reopened = _broker(destination, balance=999999)
        migrated_account = json.loads(
            (destination / "paper_account.json").read_text())
        migrated_ledger = json.loads(
            (destination / "paper_ledger.json").read_text())
        check("moved legacy state keeps its original starting cash",
              reopened.start_balance == 100, str(reopened.start_balance))
        check("moved legacy state keeps every fill and exact cash",
              len(reopened.ledger.seen) == 1 and
              approx(reopened.cash_balance(), cash_before),
              f"fills={reopened.ledger.seen} cash={reopened.cash_balance()}")
        check("legacy account and ledger migrate to one V2/V4 identity",
              migrated_account.get("version") == 2 and
              migrated_ledger.get("version") == 4 and
              migrated_account.get("ledger_id") == migrated_ledger.get("ledger_id"),
              f"account={migrated_account} ledger_id={migrated_ledger.get('ledger_id')}")
        check("migrated account records its current diagnostic path",
              pathlib.Path(migrated_account["ledger_path"]).resolve()
              == (destination / "paper_ledger.json").resolve(),
              migrated_account["ledger_path"])


def t_missing_or_replaced_paper_ledger_cannot_reset_pnl():
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        first = _broker(root, balance=100)
        check("missing-ledger fixture has a fill", paper_order(first))
        account_before = (root / "paper_account.json").read_bytes()
        (root / "paper_ledger.json").unlink()
        try:
            _broker(root, balance=999999)
        except RuntimeError as exc:
            check("account without its ledger fails closed",
                  "ledger file is missing" in str(exc), str(exc))
        else:
            check("account without its ledger fails closed", False,
                  "PnL silently reset to an empty ledger")
        check("failed recovery does not rewrite the surviving account",
              (root / "paper_account.json").read_bytes() == account_before)

    with tempfile.TemporaryDirectory() as tmp:
        base = pathlib.Path(tmp)
        left, right = base / "left", base / "right"
        left.mkdir()
        right.mkdir()
        _broker(left, balance=100)
        _broker(right, balance=200)
        shutil.copyfile(left / "paper_account.json", right / "paper_account.json")
        try:
            _broker(right, balance=200)
        except RuntimeError as exc:
            check("account paired with a replacement ledger fails closed",
                  "identities do not match" in str(exc), str(exc))
        else:
            check("account paired with a replacement ledger fails closed", False,
                  "mismatched state pair was accepted")


def t_paper_install_blocks_live_client_and_rebinds_every_action():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        fake_live = types.SimpleNamespace(_client="old", last_order_error=None)
        fake_live._get_client = lambda: "DANGEROUS"
        def disable_live_execution():
            fake_live._client = None
        fake_live.disable_live_execution = disable_live_execution
        fake_main = types.SimpleNamespace(
            execution_mode="LIVE", _paper_broker=None,
            get_balance_allowance=lambda: None,
            cancel_all_open_orders=lambda: False,
            place_trade=lambda *_a: False,
            TRADE_LOG=pathlib.Path("live.csv"))
        previous = sys.modules.get("polymarket_trade")
        sys.modules["polymarket_trade"] = fake_live
        try:
            install_paper_execution(fake_main, broker, log_path=pathlib.Path(tmp) / "paper.csv")
            check("paper mode banner state installed", fake_main.execution_mode == "PAPER")
            check("balance rebound to paper account",
                  fake_main.get_balance_allowance.__self__ is broker)
            check("cancel rebound to paper engine",
                  fake_main.cancel_all_open_orders.__self__ is broker)
            check("order rebound to paper engine", fake_main.place_trade.__self__ is broker)
            try:
                fake_live._get_client()
            except RuntimeError as exc:
                check("live client gateway poisoned",
                      "disabled by --paper" in str(exc), str(exc))
            else:
                check("live client gateway poisoned", False, "guard did not raise")
            check("cached live client erased", fake_live._client is None)
        finally:
            if previous is None:
                sys.modules.pop("polymarket_trade", None)
            else:
                sys.modules["polymarket_trade"] = previous


def t_runner_paper_build_never_derives_credentials():
    import run

    original = run.derive_creds
    calls = {"n": 0}

    def forbidden():
        calls["n"] += 1
        raise AssertionError("credential derivation reached")

    run.derive_creds = forbidden
    try:
        _hub, cfg = run.build_hub(paper=True)
        check("paper hub never derives credentials", calls["n"] == 0, str(calls))
        check("paper user websocket forced off", cfg.user_ws == "off", cfg.describe())
        check("paper wallet reconcile forced off", cfg.reconcile == "off", cfg.describe())
        _hub, cfg = run.build_hub(read_only=True)
        check("health hub never derives credentials either", calls["n"] == 0, str(calls))
    finally:
        run.derive_creds = original


def t_paper_dashboard_renders_exact_geometry():
    from dashboard import build, glyphs

    snap = {"mode": "PAPER",
            "account": {"kind": "PAPER", "cash": 987.654321, "equity": 988.40, "pnl": -0.50,
                        "realized": -1.25, "open": 1, "pending_cost": 11.10, "fees": 0.2,
                        "wins": 2, "losses": 1, "start": 1000.0}}
    for cols, rows in ((200, 60), (120, 40), (80, 24), (40, 12), (20, 4)):
        frame = build(snap, cols, rows, glyphs())
        check(f"paper dashboard row count {cols}x{rows}", len(frame) == rows,
              str(len(frame)))
        widths = [sum(len(text) for text, _style in row) for row in frame]
        check(f"paper dashboard column count {cols}x{rows}",
              all(width == cols for width in widths), str(widths))
    full_text = "\n".join(
        "".join(text for text, _style in row)
        for row in build(snap, 120, 40, glyphs()))
    check("paper dashboard visibly identifies mode", "PAPER" in full_text, full_text)
    check("paper dashboard exposes P&L", "TOTAL P/L" in full_text and "-$0.50" in full_text, full_text)
    check("paper dashboard shows cash", "$987.65" in full_text, full_text)


def t_fill_delay_probes_record_later_quotes_without_touching_cash():
    """Probes re-quote the same order later and change nothing about the fill.

    Paper matched every archived order after exactly PAPER_LATENCY_MS while
    live won 10.5 points less often at the same prices. The probes measure how
    that gap opens with delay, and they are only trustworthy if they are pure
    observation - so this pins that they never alter cash or the ledger.
    """
    import paper_trade as paper

    scheduled = []
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        broker.fill_delay_probes = (1.0, 3.0)
        broker._schedule_probe = lambda wait, fn: scheduled.append((wait, fn))
        check("a probed order still fills", paper_order(broker))
        cash_after_fill = broker.cash_balance()
        seen_after_fill = len(broker.ledger.seen)
        check("one probe is scheduled per configured delay",
              [round(w, 3) for w, _ in scheduled] == [1.0, 3.0], str(scheduled))
        if len(scheduled) == 2:
            scheduled[0][1]()                    # 1s later: same book, fills
            original = paper.estimate_fok
            paper.estimate_fok = lambda *a, **k: (_ for _ in ()).throw(
                paper.PaperRejected("FOK no-fill: price moved past the cap"))
            try:
                scheduled[1][1]()                # 3s later: the price has run
            finally:
                paper.estimate_fok = original

        journal = pathlib.Path(tmp) / "paper_orders_fill_delay.jsonl"
        rows = ([json.loads(line) for line in journal.read_text().splitlines()]
                if journal.exists() else [])
        by_delay = {r["delay_s"]: r for r in rows
                    if r.get("reason") != "actual paper fill"}
        check("the actual fill is recorded as the baseline row",
              sum(r.get("reason") == "actual paper fill" for r in rows) == 1
              and rows[0].get("fillable") is True, str(rows))
        check("a later book that still fills is recorded as fillable",
              by_delay.get(1.0, {}).get("fillable") is True
              and bool(by_delay.get(1.0, {}).get("average_price")), str(rows))
        check("a later book that would miss is a no-fill, not an error",
              by_delay.get(3.0, {}).get("fillable") is False
              and "no-fill" in by_delay.get(3.0, {}).get("reason", ""), str(rows))
        check("probes never move cash",
              approx(broker.cash_balance(), cash_after_fill),
              f"{broker.cash_balance()} vs {cash_after_fill}")
        check("probes never write to the ledger",
              len(broker.ledger.seen) == seen_after_fill, str(broker.ledger.seen))


def t_fill_delay_probe_failures_never_reach_the_fill_path():
    """A broken probe must not stop the bot.

    place_trade treats an unexpected exception as fatal and re-raises it, so
    a scheduler or book-read failure escaping the probe code would halt
    trading over a diagnostic.
    """
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        broker.fill_delay_probes = (2.0,)

        def exploding_scheduler(_wait, _fn):
            raise RuntimeError("timer thread unavailable")

        broker._schedule_probe = exploding_scheduler
        check("a failing scheduler still lets the fill complete",
              paper_order(broker) is True, str(broker.last_error))

        captured = []
        broker._schedule_probe = lambda _wait, fn: captured.append(fn)
        paper_order(broker)
        check("the next fill schedules its probe", bool(captured), str(captured))
        raised = None
        if captured:
            broker._book_fetch = lambda _token: (_ for _ in ()).throw(
                TimeoutError("book read timed out"))
            try:
                captured[0]()
            except Exception as exc:
                raised = exc
        check("a probe whose book read fails does not raise", raised is None,
              repr(raised))
        journal = pathlib.Path(tmp) / "paper_orders_fill_delay.jsonl"
        rows = ([json.loads(line) for line in journal.read_text().splitlines()]
                if journal.exists() else [])
        errored = [r for r in rows if r.get("fillable") is None]
        check("a failed probe is recorded as an error, kept out of statistics",
              bool(errored) and "probe error" in errored[-1].get("reason", ""),
              str(rows))


def t_fill_delay_probes_are_off_by_default():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        check("no probes are configured by default", broker.fill_delay_probes == ())
        paper_order(broker)
        check("and no delay journal is written",
              not (pathlib.Path(tmp) / "paper_orders_fill_delay.jsonl").exists())


def t_live_fill_timing_journal_records_post_and_response():
    """Live must record when an order was POSTed and when the venue answered.

    Receipts carried no timestamps, so the real submit-to-match time - the
    number that decides whether paper can predict live - was never measured.
    """
    import os
    import polymarket_trade as trade

    rows = []
    swallowed = False
    with tempfile.TemporaryDirectory() as tmp:
        target = pathlib.Path(tmp) / "timing.jsonl"
        saved = os.environ.get("LIVE_FILL_TIMING_PATH")
        os.environ["LIVE_FILL_TIMING_PATH"] = str(target)
        try:
            trade._record_fill_timing("oid-1", "matched", None, 100.0, 100.25,
                                      token_id="up", condition_id="cond")
            rows = [json.loads(line) for line in target.read_text().splitlines()]
            # A directory is not appendable: the write must fail quietly.
            os.environ["LIVE_FILL_TIMING_PATH"] = tmp
            trade._record_fill_timing("oid-2", "matched", None, 1.0, 2.0,
                                      token_id="up", condition_id="cond")
            swallowed = True
        except Exception:
            swallowed = False
        finally:
            if saved is None:
                os.environ.pop("LIVE_FILL_TIMING_PATH", None)
            else:
                os.environ["LIVE_FILL_TIMING_PATH"] = saved
    check("a POST is recorded with both timestamps",
          bool(rows) and rows[0]["order_id"] == "oid-1"
          and rows[0]["submitted_wall"] == 100.0
          and abs(rows[0]["post_seconds"] - 0.25) < 1e-9, str(rows))
    check("an unwritable journal never raises into the order path", swallowed)


def _book_at(best, token="up"):
    """A one-level book at ``best`` so a fill's price is unambiguous."""
    return BookSnapshot(
        token, ((D(str(best)), D("50")),),
        min_order_size=None, tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)), book_hash="abc",
        received_wall=time.time(), best_bid=D(str(round(best - 0.01, 2))))


def _inflight_broker(tmp, books, minimum=None):
    """A broker whose book changes between the decision and the arrival."""
    tmp = pathlib.Path(tmp)
    ledger = Ledger(path=str(tmp / "paper_ledger.json"))
    return PaperBroker(
        ledger,
        market_context=lambda: {"condition_id": "cond", "up_token_id": "up",
                                "down_token_id": "down"},
        host="https://public.invalid", max_buy_price=.99, start_balance=100,
        account_path=tmp / "paper_account.json",
        audit_path=tmp / "paper_orders.jsonl",
        book_fetch=lambda _token: books.pop(0) if books else _book_at(0.40),
        rules_fetch=lambda _cid: rules(minimum=minimum),
        min_seconds_to_expiry=0, trade_window_seconds=300,
        latency_ms=1.0)


def t_paper_fills_on_the_price_limit_exactly_as_a_live_fok_does():
    """A FOK cares about the limit price, not which way the ask moved.

    Paper used to refuse a fill whenever the ask ticked up, however far the
    order's cap actually was - so it kept only the fills where the price had
    moved in its favour and booked those improved prices as edge. Live has no
    such rule: a signed order fills at any price up to its limit and refuses
    past it.
    """
    with tempfile.TemporaryDirectory() as tmp:
        risen = _inflight_broker(tmp, [_book_at(0.40), _book_at(0.45)])
        check("an ask that rose but stayed under the cap still fills",
              paper_order(risen, amount=2), str(risen.last_error))
        check("and it is booked at the price actually paid, not the one seen",
              abs(risen.last_fill["average_price"] - 0.45) < 1e-9,
              str(risen.last_fill["average_price"]))

    with tempfile.TemporaryDirectory() as tmp:
        # 0.995 is above the broker's 0.99 ceiling; 0.95 would still fill.
        past_cap = _inflight_broker(tmp, [_book_at(0.40), _book_at(0.995)])
        check("an ask that rose past the cap does not fill",
              paper_order(past_cap, amount=2) is False, str(past_cap.last_error))

    with tempfile.TemporaryDirectory() as tmp:
        fallen = _inflight_broker(tmp, [_book_at(0.40), _book_at(0.35)])
        check("an ask that fell still fills - the adverse half is kept too",
              paper_order(fallen, amount=2), str(fallen.last_error))


def t_paper_requires_a_submittable_decision_book():
    """Later liquidity cannot turn a live preflight rejection into a paper fill."""
    from dataclasses import replace

    with tempfile.TemporaryDirectory() as tmp:
        shallow = replace(_book_at(0.50), asks=((D("0.50"), D("1")),))
        broker = _inflight_broker(tmp, [shallow, _book_at(0.50)])
        check("paper refuses a stake with insufficient decision-book depth",
              paper_order(broker, amount=2) is False
              and not broker.ledger.seen
              and "no-fill" in (broker.last_error or "").lower(),
              str(broker.last_error))

    with tempfile.TemporaryDirectory() as tmp:
        wide = replace(_book_at(0.50), best_bid=D("0.10"))
        broker = _inflight_broker(tmp, [wide, _book_at(0.50)])
        check("paper refuses a decision spread live would reject",
              paper_order(broker, amount=2) is False
              and not broker.ledger.seen
              and "spread" in (broker.last_error or "").lower(),
              str(broker.last_error))


def t_paper_fixes_the_stake_before_the_delay_like_a_signed_order():
    """Live sizes and signs in preflight; the amount cannot change in flight.

    Paper sized AFTER its simulated latency, against the very book it was
    about to fill on, so it could tune its stake to liquidity a real order
    had not seen yet. Here the venue minimum is 5 shares: $2.00 buys them at
    the decision price of 0.40, but not at the arrival price of 0.60. A fixed
    stake must therefore be refused for being under the minimum. Re-sizing
    after the delay would quietly raise the stake to $3.00 and fill.
    """
    with tempfile.TemporaryDirectory() as tmp:
        moved = _inflight_broker(tmp, [_book_at(0.40), _book_at(0.60)],
                                 minimum=5)
        filled = paper_order(moved, amount=2)
        check("the stake is not re-sized against the arrival book",
              filled is False, str(moved.last_error))
        check("and it is refused for the venue minimum, not for the price",
              "minimum" in (moved.last_error or "").lower(),
              str(moved.last_error))

    with tempfile.TemporaryDirectory() as tmp:
        steady = _inflight_broker(tmp, [_book_at(0.40), _book_at(0.40)],
                                  minimum=5)
        check("an unchanged book still fills at the fixed stake",
              paper_order(steady, amount=2), str(steady.last_error))
        # The stake carries headroom above the minimum, so an unchanged book
        # buys a little more than 5 shares: (5 x 0.40 + 5 x h) / 0.40.
        import config
        expected = (5 * 0.40 + 5 * config.VENUE_MIN_PRICE_HEADROOM) / 0.40
        check("an unchanged book buys the minimum plus its headroom",
              abs(steady.last_fill["shares"] - expected) < 1e-6,
              f"{steady.last_fill['shares']} vs {expected}")

    # The bug this exists for. Sized to EXACTLY the minimum, one tick of rise
    # before matching left the same dollars short of 5 shares and the order
    # was rejected - and the ask rises when the trade is right, so it
    # rejected winners. Measured: 3 of 5 paper fills failed this way when
    # re-quoted 1s later. With headroom a one-tick rise must still fill.
    with tempfile.TemporaryDirectory() as tmp:
        ticked = _inflight_broker(tmp, [_book_at(0.40), _book_at(0.41)],
                                  minimum=5)
        check("a one-tick rise before matching no longer rejects the order",
              paper_order(ticked, amount=2), str(ticked.last_error))


def t_paper_refuses_to_size_an_order_from_a_stale_book():
    """The book an order is SIZED from must be as fresh as the one it fills on.

    Fixing the stake at decision time introduced a second book read, and it
    was validated only for shape - not for age. Live gets that check for
    free: every read goes through parse_orderbook, which enforces
    ORDERBOOK_MAX_AGE_SECONDS, so live can never size off a book paper would
    happily have used.
    """
    stale = BookSnapshot(
        "up", ((D("0.40"), D("50")),), tick_size=D("0.01"),
        timestamp=str(int(time.time() * 1000)), book_hash="abc",
        received_wall=time.time() - 40.0, best_bid=D("0.39"))

    with tempfile.TemporaryDirectory() as tmp:
        broker = _inflight_broker(tmp, [stale, _book_at(0.40)])
        check("a stale decision book is refused before any sizing",
              paper_order(broker, amount=2) is False, str(broker.last_error))
        check("and it says the book was stale, not that the price was wrong",
              "stale in hand" in (broker.last_error or ""), str(broker.last_error))

    with tempfile.TemporaryDirectory() as tmp:
        arrival = _inflight_broker(tmp, [_book_at(0.40), stale])
        check("a stale arrival book is still refused too",
              paper_order(arrival, amount=2) is False, str(arrival.last_error))


def t_per_order_latency_override_and_tag():
    """The sniper passes its own decision-to-match time and a tag; leaving both
    out must keep the broker's configured behaviour exactly."""
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        check("default order still fills", paper_order(broker))
        check("default order carries no tag", "tag" not in broker.last_fill, str(broker.last_fill))
        check("default latency is the configured one (0 here)",
              broker.last_fill["assumed_latency_ms"] == 0, str(broker.last_fill["assumed_latency_ms"]))
        t0 = time.monotonic()
        ok = broker.place_trade("UP", 2, "up", "down", "cond", round_end(), latency_ms=50, tag="sniper")
        waited = time.monotonic() - t0
        check("override order fills", ok, str(broker.last_error))
        check("override latency is recorded", broker.last_fill["assumed_latency_ms"] == 50,
              str(broker.last_fill["assumed_latency_ms"]))
        check("the broker really waited the override before re-reading", waited >= 0.045, f"{waited:.3f}s")
        check("tag is copied to the audit row", broker.last_fill.get("tag") == "sniper", str(broker.last_fill))
        rows = [json.loads(line) for line in
                (pathlib.Path(tmp) / "paper_orders.jsonl").read_text().splitlines()]
        check("audit file holds the tagged row", rows[-1].get("tag") == "sniper", str(rows[-1]))
        check("a nonsensical override is refused",
              broker.place_trade("UP", 2, "up", "down", "cond", round_end(), latency_ms=-5) is False,
              str(broker.last_error))


def t_sniper_trigger_fires_only_on_a_real_move():
    import sniper_mode
    trig = sniper_mode.Trigger(k_bp=1.0, window_ms=250)
    check("first tick has no reference", trig.update(0, 100.0)[0] is None)
    check("a 0.5bp move is not a signal", trig.update(100, 100.005)[0] is None)
    d, r = trig.update(260, 100.011)
    check("1.1bp above the price 250ms ago fires Up", d == 1 and 1.09 < r < 1.11, f"{d} {r}")
    d, r = trig.update(600, 99.99)
    check("a fall of >= 1bp fires Down", d == -1, f"{d} {r}")
    trig2 = sniper_mode.Trigger(k_bp=1.0, window_ms=250)
    trig2.update(0, 100.0)
    check("no reference older than the window yet -> no signal", trig2.update(200, 100.05)[0] is None)


def t_sniper_coinbase_second_trigger_shares_one_cooldown():
    import json as _json
    import types as _types
    import sniper_mode
    # a deployment's .env may set SNIPER_EXTRA_FEEDS (config loads it into the environment): clear it here
    check("no second trigger unless asked", _fixed_sniper_settings(SNIPER_EXTRA_FEEDS="").extra_feeds == ())
    st = _fixed_sniper_settings(SNIPER_EXTRA_FEEDS="coinbase")
    check("SNIPER_EXTRA_FEEDS=coinbase parsed; the settings line says so",
          st.extra_feeds == ("coinbase",) and "second trigger: coinbase" in st.lines()[0], st.lines()[0])
    try:
        _fixed_sniper_settings(SNIPER_EXTRA_FEEDS="nosuchfeed")
        check("an unknown extra feed is refused", False, "accepted")
    except ValueError:
        check("an unknown extra feed is refused", True)
    sn = sniper_mode.Sniper(None, None, settings=st)
    fired = []

    def fake_fire(side, r, t, t_pc=None, feed="binance"):
        fired.append((side, feed))
        sn.busy = False
        return True  # emulate a prepared attempt (None means a skipped signal)
    sn._fire_now = fake_fire
    now = time.monotonic() * 1000.0
    sn.on_coinbase(_json.dumps({"type": "subscriptions", "channels": []}))
    sn.on_coinbase(_json.dumps({"type": "heartbeat", "sequence": 1}))
    check("acks and heartbeats are not prices", not sn.triggers["coinbase"].q, str(sn.triggers["coinbase"].q))
    sn.triggers["coinbase"].q.append((now - 300, 60000.0))
    sn.on_coinbase(_json.dumps({"type": "ticker", "best_bid": "60008.5", "best_ask": "60009.5"}))
    check("a 1.5bp Coinbase move fires Up, tagged coinbase", fired == [("UP", "coinbase")], str(fired))
    sn.trigger.q.append((now - 300, 60100.0))
    sn.on_tick(_json.dumps({"b": "60109", "a": "60111"}))
    check("Binance's signal on the same move hits the SHARED cooldown: one move, one order",
          fired == [("UP", "coinbase")] and sn.n["skip: cooldown"] == 1 and sn.n["signals"] == 1
          and sn.n["signals coinbase"] == 1, f"{fired} {dict(sn.n)}")
    check("each feed keeps its own price reference (Coinbase 60009 never compared with Binance 60110)",
          sn.trigger.q[-1][1] == 60110.0 and sn.triggers["coinbase"].q[-1][1] == 60009.0)
    sn.last_fire = -1e18
    sn.trigger.q.clear()
    sn.trigger.q.append((time.monotonic() * 1000.0 - 300, 60110.0))
    sn.on_tick(_json.dumps({"b": "60099", "a": "60101"}))
    check("after the cooldown Binance fires on its own, tagged binance", fired[-1] == ("DOWN", "binance"),
          str(fired))
    summ = sn.summary()
    check("summary splits signals and fires by feed",
          summ["signals_by_feed"] == {"binance": 2, "coinbase": 1}
          and summ["fired_by_feed"] == {"coinbase": 1, "binance": 1}, str(summ))
    # the decision row names the feed that fired it
    rows = []
    sn2 = sniper_mode.Sniper(_types.SimpleNamespace(up_token=None, down_token=None, condition_id=None,
                                                    window_end=None), None, settings=st)
    sn2._write = rows.append
    sn2._fire_now("UP", 1.5, time.monotonic() * 1000.0, feed="coinbase")
    check("the journalled decision carries feed=coinbase", rows and rows[0].get("feed") == "coinbase"
          and rows[0].get("reason") == "no current round", str(rows))


def _fixed_sniper_settings(**env):
    """Old fixed-delay mode: these tests use fake token ids ("up") that the
    real order signer would rightly refuse to build. Their caps are priced at
    ask + 2c, set here (the code default is the spec's 4c)."""
    import sniper_mode
    env = {"SNIPER_SLIP": "0.02", **env}
    keys = ("SNIPER_LATENCY_MS", *env)
    saved = {k: os.environ.get(k) for k in keys}
    os.environ["SNIPER_LATENCY_MS"] = "200"
    os.environ.update(env)
    try:
        return sniper_mode.Settings()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def t_sniper_order_path_uses_latency_cap_tag_and_caps():
    import asyncio
    import sniper_mode

    class View:
        def __init__(self, ask, status="LIVE", age=0.0):
            self.status, self.asks = status, ((ask, 100.0),) if ask else ()
            self.best_ask, self.mid = ask, (ask - 0.005) if ask else None
            self.updated_mono = time.monotonic() - age

    class Book:
        def __init__(self):
            self.views = {"up": View(0.52), "down": View(0.49)}

        def view(self, token):
            return self.views[token]

    class Hub:
        up_token, down_token, condition_id = "up", "down", "cond"

        def __init__(self):
            self.window_end = time.time() + 120
            self.book = Book()

    class Broker:
        def __init__(self):
            self.calls, self.last_fill, self.last_error = [], None, None

        def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None):
            self.calls.append(dict(side=side, amount=amount, cap=max_price, latency=latency_ms, tag=tag))
            self.last_fill = {"tag": tag, "order_id": f"o{len(self.calls)}", "shares": 10.0,
                              "average_price": 0.52, "fee": 0.17, "total_cost": 5.37}
            return True

    s = _fixed_sniper_settings(SNIPER_MAX_FILLS_ROUND="2")
    hub, broker = Hub(), Broker()
    sn = sniper_mode.Sniper(hub, broker, settings=s)

    async def go():
        for _ in range(3):
            await sn._fire("UP", 1.2, 0.0)
        hub.book.views["down"] = View(0.49, status="STALE")
        await sn._fire("DOWN", -1.3, 0.0)
        hub.window_end = time.time() + 5
        await sn._fire("UP", 1.5, 0.0)
    asyncio.run(go())
    c = broker.calls
    check("two orders reach the broker, the third hits the round cap", len(c) == 2, str(c))
    check("side, 200ms latency and the sniper tag are passed", c and c[0]["side"] == "UP"
          and c[0]["latency"] == 200 and c[0]["tag"] == "sniper", str(c[:1]))
    check("price cap = ask seen + 2c", c and abs(c[0]["cap"] - 0.54) < 1e-9, str(c[:1]))
    check("stake is 10 shares at the seen ask", c and abs(c[0]["amount"] - 5.2) < 1e-9, str(c[:1]))

    # Under 10c the 10-share clip would be under the venue's $1 floor.
    import paper_trade
    check("the sniper's $1 floor is the paper broker's", sniper_mode.MIN_ORDER_USD
          == float(paper_trade.MIN_MARKETABLE_BUY_USD))
    cheap_hub, cheap_broker = Hub(), Broker()
    cheap_hub.book.views["up"] = View(0.05)
    asyncio.run(sniper_mode.Sniper(cheap_hub, cheap_broker, settings=s)._fire("UP", 1.2, 0.0))
    cc = cheap_broker.calls
    check("at a 5c ask the order is sized up to $1 (20 shares), not $0.50",
          cc and abs(cc[0]["amount"] - 1.0) < 1e-9, str(cc))
    check("round cap, stale book and late-round skips are counted",
          sn.n["skip: round cap"] == 1 and sn.n["skip: book not live"] == 1
          and sn.n["skip: too late in round"] == 1, str(dict(sn.n)))


def t_sniper_orders_reach_the_dashboard_trades_table():
    import asyncio
    import sniper_mode

    class View:
        status, asks, best_ask, mid = "LIVE", ((0.52, 100.0),), 0.52, 0.515

        def __init__(self):
            self.updated_mono = time.monotonic()

    class Book:
        def view(self, token):
            return View()

    class Hub:
        up_token, down_token, condition_id = "up", "down", "cond"

        def __init__(self):
            self.window_end = time.time() + 120
            self.book = Book()

    class Broker:
        def __init__(self):
            self.ok, self.last_fill, self.last_error = True, None, None

        def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None):
            self.last_fill = {"tag": tag, "order_id": "o", "shares": 10.0,
                              "average_price": 0.52, "fee": 0.17, "total_cost": 5.37} if self.ok else None
            self.last_error = None if self.ok else "price ran past the cap"
            return self.ok

    hub, broker = Hub(), Broker()
    rows = [{"phase": "phase2", "side": "UP", "result": "ok"}]
    sn = sniper_mode.Sniper(hub, broker, settings=_fixed_sniper_settings(), trade_rows=rows, own_rounds=True)

    async def go():
        await sn._fire("UP", 1.2, 0.0)
        broker.ok = False
        await sn._fire("DOWN", -1.3, 0.0)
        hub.window_end += 300
        broker.ok = True
        await sn._fire("DOWN", -1.1, 0.0)
    asyncio.run(go())
    check("new round: the sniper empties the table when it owns rounds",
          len(rows) == 1 and rows[0]["side"] == "DOWN" and rows[0]["phase"] == "sniper", str(rows))

    rows[:] = []
    hub.window_end += 300
    sn2 = sniper_mode.Sniper(hub, broker, settings=_fixed_sniper_settings(), trade_rows=rows)
    broker.ok = True
    asyncio.run(sn2._fire("UP", 1.2, 0.0))
    broker.ok = False
    asyncio.run(sn2._fire("DOWN", -1.3, 0.0))
    check("a fill and a refusal both land in the table",
          [r["result"] for r in rows] == ["filled", "rejected"], str(rows))
    miss_broker = Broker()
    miss_broker.min_buy_price, miss_broker.max_buy_price = 0.0, 1.0

    def killed(*a, **k):
        miss_broker.last_fill = None
        miss_broker.last_error = "FOK no-fill: only $0.000000 available at or below 0.54"
        return False
    miss_broker.place_trade = killed
    miss_rows = []
    asyncio.run(sniper_mode.Sniper(hub, miss_broker, settings=_fixed_sniper_settings(),
                                   trade_rows=miss_rows)._fire("UP", 1.2, 0.0))
    r = miss_rows[-1]["result"] if miss_rows else None
    check("a FOK killed because the price moved is a MISS, not a REJECT",
          r == "missed", str(miss_rows))
    check("a fill is recorded as filled, a refusal as rejected (sent, not skipped)",
          rows[0]["result"] == "filled" and rows[1]["result"] == "rejected", str(rows))
    check("row carries cost and an HH:MM:SS time the table can slice",
          abs(rows[0]["amount"] - 5.37) < 1e-9 and rows[0]["time_et"][-11:-3].count(":") == 2, str(rows[0]))
    rows.insert(0, {"phase": "phase2", "side": "UP", "result": "ok"})
    hub.window_end += 300
    broker.ok = True
    asyncio.run(sn2._fire("UP", 1.2, 0.0))
    check("without own_rounds the sniper never clears the strategy's rows",
          rows[0]["phase"] == "phase2" and len(rows) == 4, str(rows))

    # The round turns over with no new sniper order yet: the table must not
    # keep showing the last round's trades.
    own_rows = [{"phase": "sniper", "side": "UP", "result": "filled"}]
    watcher = sniper_mode.Sniper(hub, broker, settings=_fixed_sniper_settings(), trade_rows=own_rows,
                                 own_rounds=True)
    watcher.rows_round = hub.window_end
    watcher.roll_rounds()
    check("same round: the watcher leaves the table alone", len(own_rows) == 1, str(own_rows))
    hub.window_end += 300
    watcher.roll_rounds()
    check("new round: the table empties at once, before any new trade", own_rows == [], str(own_rows))
    sn2.rows_round = hub.window_end - 300
    sn2.roll_rounds()
    check("without own_rounds the watcher never clears", len(rows) == 4, str(rows))

    # A 3c ask sits below the account floor: the broker would refuse it, so
    # the sniper must skip it without sending or adding a REJECT row.
    broker.min_buy_price, broker.max_buy_price = 0.20, 0.90
    calls = []
    broker.place_trade = lambda *a, **k: calls.append(a) or True
    View.best_ask, View.asks = 0.03, ((0.03, 100.0),)
    before = len(rows)
    asyncio.run(sn2._fire("UP", 1.2, 0.0))
    View.best_ask, View.asks = 0.97, ((0.97, 100.0),)
    asyncio.run(sn2._fire("DOWN", -1.2, 0.0))
    check("asks outside MIN/MAX_BUY_PRICE are skipped, not sent",
          not calls and len(rows) == before and sn2.n["skip: ask outside price band"] == 2,
          f"{calls} {dict(sn2.n)}")

    # The default account window (120s..60s left) is narrower than the lab
    # plan: outside it the sniper skips instead of sending to be refused.
    View.best_ask, View.asks = 0.52, ((0.52, 100.0),)
    broker.trade_window_seconds, broker.min_seconds_to_expiry = 120.0, 60.0
    gaps = sn2.plan_gaps()
    check("a narrower window and band are announced at startup",
          len(gaps) == 2 and "TRADE_LAST_SECONDS=300" in gaps[0] and "MIN_BUY_PRICE=0.05" in gaps[1], str(gaps))
    hub.window_end = time.time() + 200
    asyncio.run(sn2._fire("UP", 1.2, 0.0))
    hub.window_end = time.time() + 60.1
    asyncio.run(sn2._fire("UP", 1.2, 0.0))
    check("before the window and inside the cutoff+latency are skipped, not sent",
          not calls and sn2.n["skip: before trade window"] == 1
          and sn2.n["skip: too late in round"] == 1, f"{calls} {dict(sn2.n)}")
    hub.window_end = time.time() + 90
    asyncio.run(sn2._fire("UP", 1.2, 0.0))
    check("inside the window the order is sent", len(calls) == 1, str(calls))
    broker.trade_window_seconds, broker.min_seconds_to_expiry = 300.0, 10.0
    broker.min_buy_price, broker.max_buy_price = 0.05, 0.95
    check("lab-plan settings clear the warning", sn2.plan_gaps() == [], str(sn2.plan_gaps()))


def t_sniper_shadow_fills_price_the_same_order_at_other_latencies():
    import asyncio
    import json
    import tempfile
    import sniper_mode
    from feeds.book import BookView

    start = time.monotonic()

    class Book:
        # The ask runs from 0.50 to 0.55 100ms after the decision.
        def view(self, token):
            moved = time.monotonic() - start > 0.10
            a = 0.55 if moved else 0.50
            return BookView(str(token), 1, "LIVE", bids=((round(a - 0.01, 2), 50.0),), asks=((a, 50.0),),
                            updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000),
                            tick_size=0.01)

    class Hub:
        up_token, down_token, condition_id = "up", "down", "cond"

        def __init__(self):
            self.window_end = time.time() + 120
            self.book = Book()

    class Broker:
        max_spread, min_buy_price, max_buy_price = 0.05, 0.05, 0.95

        def __init__(self):
            self.calls, self.last_error = 0, None
            self.last_fill = None

        def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None):
            self.calls += 1
            self.last_fill = {"tag": tag, "order_id": "o1", "shares": 10.0,
                              "average_price": 0.50, "fee": 0.175, "total_cost": 5.175}
            return True

        def _rules(self, cond):
            return rules()

    s = _fixed_sniper_settings()
    s.shadow_ms = (0, 20, 250)
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "j.jsonl")
        broker = Broker()
        sn = sniper_mode.Sniper(Hub(), broker, settings=s, journal_path=path)
        sn.SHADOW_MARK_S = 0.2

        async def go():
            await sn._fire("UP", 1.2, start * 1000.0)
            await asyncio.sleep(0.8)
        asyncio.run(go())
        rows = [json.loads(x) for x in open(path, encoding="utf-8")]
    shadow = [r for r in rows if r.get("result") == "shadow"]
    filled = [r for r in rows if r.get("result") == "filled"]
    check("one shadow row per order, linked to it by id",
          len(shadow) == 1 and filled and shadow[0]["id"] == filled[0]["id"], str(rows))
    L = shadow[0]["L"] if shadow else {}
    check("before the ask runs, the shadow fills at 0.50 within the cap",
          L.get("0", {}).get("px") == 0.5 and L.get("20", {}).get("px") == 0.5, str(L))
    check("after the ask runs past the cap (0.52), the late shadow misses",
          "miss" in L.get("250", {}) and "px" not in L.get("250", {}), str(L))
    fee_c = 0.07 * 0.5 * 0.5 * 100
    check("shadow 5s markout = later mid - price - fee per share",
          abs(L.get("0", {}).get("mk5", 99) - (54.5 - 50.0 - fee_c)) < 1e-6, str(L))
    alt = L.get("250", {}).get("alt", {})
    check("wider limits are priced on the same book: +3c/+4c still miss 0.55, +6c fills it",
          sorted(alt) == ["3", "4", "6"] and "miss" in alt["3"] and "miss" in alt["4"]
          and alt["6"].get("px") == 0.55, str(alt))
    check("before the move a wider limit fills at the same 0.50",
          L.get("0", {}).get("alt", {}).get("3", {}).get("px") == 0.5, str(L.get("0")))
    check("shadows send nothing: the broker saw exactly one order", broker.calls == 1, str(broker.calls))
    check("the timer lateness is recorded for every shadow",
          all("late_ms" in v for v in L.values()), str(L))


def t_sniper_live_like_mode_signs_and_uses_measured_latency():
    import asyncio
    import sniper_mode
    from feeds.book import BookView

    up_tok = "71321045679252212594626385532706912750332728571942532289631379312455583992563"
    dn_tok = "52114319501245915516055106046884209969926127482827954674443846427813813222426"

    class Book:
        tick = 0.01

        def view(self, token):
            return BookView(str(token), 1, "LIVE", bids=((0.51, 50.0),), asks=((0.52, 50.0),),
                            updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000),
                            tick_size=self.tick)

    class Hub:
        up_token, down_token, condition_id = up_tok, dn_tok, "cond"

        def __init__(self):
            self.window_end = time.time() + 120
            self.book = Book()

    class Broker:
        def __init__(self):
            self.calls, self.last_error, self.last_fill = [], None, None

        def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None,
                        latency_from_pc=None):
            self.calls.append(latency_ms)
            self.sent_pc = latency_from_pc
            self.last_fill = {"tag": tag, "order_id": "o", "shares": 10.0, "average_price": 0.52,
                              "fee": 0.17, "total_cost": 5.37}
            return True

    saved = os.environ.pop("SNIPER_LATENCY_MS", None)
    try:
        s = sniper_mode.Settings()
    finally:
        if saved is not None:
            os.environ["SNIPER_LATENCY_MS"] = saved
    s.shadow_ms = ()
    check("SNIPER_LATENCY_MS unset -> live-like mode", not s.fixed_latency and "live-like" in s.lines()[0])
    hub, broker = Hub(), Broker()
    sn = sniper_mode.Sniper(hub, broker, settings=s)
    check("before any probe the lab's 14ms one-way is used",
          abs(sn.latency_plan()[0] - (14 + 250 + 25)) < 1e-9, str(sn.latency_plan()))
    sn.net_rtt.extend([30.0, 90.0, 40.0])
    lat, parts = sn.latency_plan()
    check("latency = half the median round trip + 250ms hold (documented) + 25ms server",
          abs(lat - (20 + 250 + 25)) < 1e-9 and parts["net_measured"], str((lat, parts)))
    if sn._builder is None:
        check("py_clob_client_v2 is importable in the test environment", False)
        return
    rows = []
    sn._write = rows.append
    asyncio.run(sn._fire("UP", 1.2, time.monotonic() * 1000))
    check("the broker is told the live-like latency", broker.calls == [295.0], str(broker.calls))
    check("...and when the order left (after signing), so its delay runs from there",
          isinstance(getattr(broker, "sent_pc", None), int), str(getattr(broker, "sent_pc", None)))
    check("the order was really signed and its time recorded",
          rows and rows[-1].get("sign_ms", 0) > 0 and rows[-1].get("result") == "filled", str(rows))
    # A tick the library does not know: live could not build this order.
    hub.book.tick = 0.5
    asyncio.run(sn._fire("UP", 1.2, time.monotonic() * 1000))
    check("an order the live library refuses to build is refused before the broker",
          len(broker.calls) == 1 and rows[-1].get("result") == "refused"
          and "order signing failed" in str(rows[-1].get("reason")), str(rows[-1:]))


def t_sniper_review_fixes():
    import asyncio
    import sniper_mode
    import run
    from feeds.book import BookView
    from paper_trade import BookSnapshot as Snap

    # --- markouts: each fill reports its own, even when fills overlap -------
    mids = {"a": 0.60, "b": 0.40}

    class Book:
        def view(self, token):
            m = mids[token]
            return BookView(token, 1, "LIVE", bids=((round(m - 0.005, 3), 50.0),),
                            asks=((round(m + 0.005, 3), 50.0),), updated_mono=time.monotonic(),
                            exchange_ts_ms=int(time.time() * 1000), tick_size=0.001)

    class Hub:
        book = Book()

    sn = sniper_mode.Sniper(Hub(), None, settings=_fixed_sniper_settings())
    sn.MARKOUT_S = (0.05, 0.15)
    rows = []
    sn._write = rows.append

    async def overlap():
        await asyncio.gather(
            sn._markout({"order_id": "A", "price": 0.50, "fee": 0.0, "shares": 10}, "a"),
            sn._markout({"order_id": "B", "price": 0.50, "fee": 0.0, "shares": 10}, "b"))
    asyncio.run(overlap())
    got = {r["order_id"]: r for r in rows}
    check("overlapping fills each journal their own markout (A +10c, B -10c)",
          abs(got["A"]["markout_1s_c"] - 10) < 1e-6 and abs(got["B"]["markout_1s_c"] + 10) < 1e-6
          and abs(got["A"]["markout_5s_c"] - 10) < 1e-6 and abs(got["B"]["markout_5s_c"] + 10) < 1e-6,
          str(rows))

    class NoMid:
        def view(self, token):
            return BookView(token, 1, "LIVE", bids=(), asks=((0.5, 1.0),))

    sn.hub = type("H", (), {"book": NoMid()})()
    rows.clear()
    asyncio.run(sn._markout({"order_id": "C", "price": 0.5, "fee": 0.0, "shares": 10}, "a"))
    check("no mid -> no markout, not the previous fill's",
          rows and rows[0]["markout_1s_c"] is None and rows[0]["markout_5s_c"] is None, str(rows))

    # --- network probe: first (connection set-up) sample dropped ------------
    probe = sniper_mode.Sniper(None, None)
    probe.NET_PROBE_S = 0.01
    calls = []
    probe._rtt_once = lambda url: calls.append(url) or (200, 400.0 if len(calls) == 1 else 30.0)

    async def run_probe():
        task = asyncio.get_running_loop().create_task(probe._net_probe())
        await asyncio.sleep(0.15)
        task.cancel()
    asyncio.run(run_probe())
    check("the probe keeps warm samples only (cold first one dropped)",
          len(calls) >= 3 and list(probe.net_rtt) and all(x == 30.0 for x in probe.net_rtt),
          f"{len(calls)} {list(probe.net_rtt)}")

    # --- the ask seen is net of our own recent fills -------------------------
    class NetBroker:
        own_fill_ttl_s, min_buy_price, max_buy_price = 2.0, 0.05, 0.95

        def __init__(self, drop):
            self.drop, self.caps, self.last_fill, self.last_error = drop, [], None, "x"

        def _net_of_own_fills(self, book):
            return Snap(book.token_id, book.asks[self.drop:], best_bid=book.best_bid,
                        timestamp=book.timestamp, received_wall=book.received_wall)

        def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None):
            self.caps.append(max_price)
            return False

    class Book2:
        def view(self, token):
            return BookView(str(token), 1, "LIVE", bids=((0.49, 50.0),), asks=((0.50, 10.0), (0.53, 50.0)),
                            updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)

    class Hub2:
        up_token, down_token, condition_id = "up", "down", "cond"

        def __init__(self):
            self.window_end = time.time() + 120
            self.book = Book2()

    nb = NetBroker(drop=1)
    asyncio.run(sniper_mode.Sniper(Hub2(), nb, settings=_fixed_sniper_settings())._fire("UP", 1.2, 0.0))
    check("our own fill took the 0.50 level: the cap is set off 0.53 like live",
          nb.caps and abs(nb.caps[0] - 0.55) < 1e-9, str(nb.caps))
    gone = NetBroker(drop=2)
    sg = sniper_mode.Sniper(Hub2(), gone, settings=_fixed_sniper_settings())
    asyncio.run(sg._fire("UP", 1.2, 0.0))
    check("every level taken by our own fills -> skipped, nothing sent",
          not gone.caps and sg.n["skip: no asks left after our own recent fills"] == 1, str(dict(sg.n)))

    # --- tick -> match time, and a finished round's row is ignored -----------
    class MatchBroker(NetBroker):
        own_fill_ttl_s = 0

        def place_trade(self, *a, **k):
            self.last_fill = {"tag": "sniper", "order_id": "m", "shares": 10.0, "average_price": 0.5,
                              "fee": 0.1, "total_cost": 5.1, "matched_wall": time.time() + 0.2}
            return True

    jr = []
    sm = sniper_mode.Sniper(Hub2(), MatchBroker(0), settings=_fixed_sniper_settings())
    sm._write = jr.append
    sm.MARKOUT_S = (0.0, 0.0)
    asyncio.run(sm._fire("UP", 1.2, time.monotonic() * 1000.0 - 30))
    fr = [r for r in jr if r.get("result") == "filled"]
    check("the journal records loop lag and tick -> match time",
          fr and fr[0]["loop_lag_ms"] >= 30 and 220 <= fr[0]["match_ms"] < 400, str(fr))
    table = [{"phase": "sniper"}]
    st = sniper_mode.Sniper(Hub2(), None, settings=_fixed_sniper_settings(), trade_rows=table, own_rounds=True)
    st.rows_round = 2000
    st._table_row("UP", 5.0, "filled", 1700)
    check("a row for a round that already ended leaves the new round's table alone",
          table == [{"phase": "sniper"}] and st.rows_round == 2000, str(table))

    # --- a typo in PAPER_OWN_FILL_TTL_S no longer stops the bot ---------------
    saved = os.environ.get("PAPER_OWN_FILL_TTL_S")
    try:
        results = []
        for raw in ("abc", "-1", "0", "1.5", None):
            if raw is None:
                os.environ.pop("PAPER_OWN_FILL_TTL_S", None)
            else:
                os.environ["PAPER_OWN_FILL_TTL_S"] = raw
            results.append(run._env_seconds("PAPER_OWN_FILL_TTL_S", 2.0))
        check("bad values fall back to the default, good ones are used",
              results == [2.0, 2.0, 0.0, 1.5, 2.0], str(results))
    finally:
        if saved is None:
            os.environ.pop("PAPER_OWN_FILL_TTL_S", None)
        else:
            os.environ["PAPER_OWN_FILL_TTL_S"] = saved


def t_sniper_end_to_end_on_the_real_paper_broker():
    """Real Sniper -> real PaperBroker -> real ledger and audit, with real
    signing: catches any mismatch between the two that fakes would hide."""
    import asyncio
    import json
    import sniper_mode
    from feeds.book import BookView

    UP = "71321045679252212594626385532706912750332728571942532289631379312455583992563"
    DN = "52114319501245915516055106046884209969926127482827954674443846427813813222426"
    end = round_end()
    if end - time.time() < 20:
        time.sleep(end - time.time() + 0.5)
        end = round_end()

    class Book:
        def view(self, token):
            return BookView(str(token), 1, "LIVE", bids=((0.51, 100.0),), asks=((0.52, 12.0), (0.53, 100.0)),
                            updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000),
                            tick_size=0.01)

    class Hub:
        up_token, down_token, condition_id, window_end = UP, DN, "cond", end
        book = Book()

    hub = Hub()
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(
            tmp, context={"condition_id": "cond", "up_token_id": UP, "down_token_id": DN},
            selected_rules=MarketRules("cond", D("0.07"), 1, min_order_size=D("5"), tick_size=D("0.01"),
                                       up_token_id=UP, down_token_id=DN))
        broker._book_fetch = lambda token: snapshot_from_book_view(hub.book.view(token), expected_token=token)
        broker.own_fill_ttl_s = 5.0
        saved = os.environ.pop("SNIPER_LATENCY_MS", None)
        try:
            s = sniper_mode.Settings()
        finally:
            if saved is not None:
                os.environ["SNIPER_LATENCY_MS"] = saved
        s.hold_ms, s.server_ms, s.shadow_ms = 0.0, 0.0, (0, 30)
        journal = os.path.join(tmp, "j.jsonl")
        table = []
        sn = sniper_mode.Sniper(hub, broker, settings=s, journal_path=journal, trade_rows=table, own_rounds=True)
        sn.SHADOW_MARK_S, sn.MARKOUT_S = 0.1, (0.05, 0.1)

        async def go():
            await sn._fire("UP", 1.2, time.monotonic() * 1000.0)
            await sn._fire("UP", 1.3, time.monotonic() * 1000.0)
            await asyncio.sleep(0.4)
        asyncio.run(go())
        rows = [json.loads(x) for x in open(journal, encoding="utf-8")]
        audit = [json.loads(x) for x in (pathlib.Path(tmp) / "paper_orders.jsonl").read_text().splitlines()]
    fills = [r for r in rows if r.get("result") == "filled"]
    check("both orders fill through the real broker, signed for real",
          len(fills) == 2 and all(f.get("sign_ms", 0) > 0 for f in fills), str(rows))
    check("the first takes 10 of the 12 shares at 0.52",
          fills and abs(fills[0]["price"] - 0.52) < 1e-9, str(fills[:1]))
    check("the second sees only 2 left there: ask seen 0.52 net of our fill, pays into 0.53",
          len(fills) == 2 and 0.52 < fills[1]["price"] <= 0.53, str(fills[1:]))
    check("every fill is in the audit file, tagged, with the planned latency and its match time",
          sum(1 for a in audit if a.get("tag") == "sniper" and a.get("matched_wall")
              and abs(a["assumed_latency_ms"] - 14.0) < 1e-9) == 2, str(audit))
    check("tick -> match is recorded and at least the planned latency",
          all(f.get("match_ms", 0) >= 14 for f in fills), str([f.get("match_ms") for f in fills]))
    check("shadows and markouts were journalled for both orders",
          sum(1 for r in rows if r.get("result") == "shadow") == 2
          and sum(1 for r in rows if r.get("result") == "markout") == 2, str([r.get("result") for r in rows]))
    check("the dashboard table shows both as FILL rows of this round",
          [r["result"] for r in table] == ["filled", "filled"], str(table))
    stages = ("t_loop_ms", "t_decide_ms", "t_sign_ms", "t_request_ms", "t_wire_ms")
    check("every stage from BTC tick to match is timed",
          fills and all(all(f.get(k) is not None and f[k] >= 0 for k in stages + ("t_total_ms",)) for f in fills),
          str([{k: f.get(k) for k in stages} for f in fills]))
    check("in flight = the planned latency (14ms here) from when it was signed, not more",
          fills and all(13.5 <= f["t_wire_ms"] <= 40 for f in fills), str([f.get("t_wire_ms") for f in fills]))
    check("the decision runs in the tick handler: no event-loop wait before it",
          fills and all(f["t_loop_ms"] < 1.0 for f in fills), str([f.get("t_loop_ms") for f in fills]))
    order = ("tick", "decide", "decided", "built", "sign", "signed", "sent", "handoff", "thread",
             "enter", "delay", "matched", "ledger_start", "ledger_done", "audit_done")
    check("every raw stamp is recorded, in the order the path runs (send before the worker handoff)",
          fills and all(list(f.get("ts_us", {})) and all(k in f["ts_us"] for k in order)
                        and [f["ts_us"][k] for k in order] == sorted(f["ts_us"][k] for k in order)
                        for f in fills), str([f.get("ts_us") for f in fills]))
    check("the real POST /order request (body + HMAC headers) is built before 'sent'",
          fills and all(f.get("t_request_ms", 0) > 0 for f in fills), str([f.get("t_request_ms") for f in fills]))
    check("the ledger write happens after the book read, never before it",
          fills and all(f["ts_us"]["ledger_start"] >= f["ts_us"]["matched"] for f in fills))
    check("the stages add up to the total",
          fills and all(abs(sum(f[k] for k in stages) - f["t_total_ms"]) < 0.05 for f in fills),
          str([(sum(f[k] for k in stages), f["t_total_ms"]) for f in fills]))


def t_dashboard_never_waits_for_a_ledger_write():
    import threading
    import run

    class Book:
        def view(self, token):
            return types.SimpleNamespace(status="UNSYNCED", best_bid=None)

    class Hub:
        up_token, down_token, condition_id = "up", "down", "cond"
        book = Book()

    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        ledger = broker.ledger
        rt = types.SimpleNamespace(ledger=ledger, broker=broker, hub=Hub(), live_exec=None,
                                   journal_path=None, started=time.time())
        view = run.DashView(rt)
        got = view._accounting_nowait()
        check("free lock: the frame gets the paper account",
              got is not None and got.get("kind") == "PAPER" and got.get("cash") is not None, str(got))
        held, release = threading.Event(), threading.Event()

        def writer():
            with ledger._lock:          # what a fill or settlement does while it writes
                held.set()
                release.wait(2)
        th = threading.Thread(target=writer)
        th.start()
        held.wait(2)
        t = time.perf_counter()
        busy = view._accounting_nowait()
        waited = time.perf_counter() - t
        release.set()
        th.join()
        check("a write in progress: the frame returns at once instead of freezing the loop",
              busy is None and waited < 0.05, f"{busy} {waited:.3f}s")


def t_sniper_journals_its_start_and_code_version():
    import asyncio
    import sniper_mode

    rows = []
    sn = sniper_mode.Sniper(None, None, settings=_fixed_sniper_settings())
    sn._write = rows.append
    real_connect = sniper_mode.websockets.connect

    def refuse(*a, **k):
        raise OSError("offline test")
    sniper_mode.websockets.connect = refuse

    async def briefly():
        task = asyncio.get_running_loop().create_task(sn.run())
        await asyncio.sleep(0.05)
        task.cancel()
    try:
        asyncio.run(briefly())
    finally:
        sniper_mode.websockets.connect = real_connect
    check("the first journal row marks the restart, with the code version",
          rows and rows[0]["result"] == "start" and "code" in rows[0] and "settings" in rows[0], str(rows[:1]))


def t_paper_fill_records_when_it_met_the_book():
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp)
        before = time.time()
        check("paper FOK fills", paper_order(broker))
        m = broker.last_fill.get("matched_wall")
        check("the fill records when it met the book, before the ledger write finished",
              m is not None and before <= m <= broker.last_fill["wall"], str(broker.last_fill))


def t_paper_own_fills_use_up_the_book():
    snap = BookSnapshot(
        "up", ((D("0.50"), D("12")), (D("0.51"), D("100"))), min_order_size=D("5"),
        tick_size=D("0.01"), timestamp=str(int(time.time() * 1000)),
        book_hash="own", received_wall=time.time(), best_bid=D("0.49"))
    with tempfile.TemporaryDirectory() as tmp:
        broker = _broker(tmp, selected_book=snap, selected_rules=rules(minimum=5))
        check("off by default in the broker", broker.own_fill_ttl_s == 0)
        broker.own_fill_ttl_s = 0.3
        paper_order(broker, amount=5.0)
        first = broker.last_fill["average_price"]
        paper_order(broker, amount=5.0)
        second = broker.last_fill["average_price"]
        check("first buy takes 10 of the 12 shares at 0.50", approx(first, 0.50), str(first))
        check("the next buy sees only 2 left at 0.50 and walks into 0.51",
              0.50 < second < 0.51, str(second))
        time.sleep(0.35)
        paper_order(broker, amount=5.0)
        check("after the ttl the maker is assumed to have re-quoted",
              approx(broker.last_fill["average_price"], 0.50), str(broker.last_fill["average_price"]))


def t_sniper_is_off_unless_enabled():
    import sniper_mode
    # SNIPER_ONLY also turns the sniper on (enabled() = SNIPER_ENABLED or
    # SNIPER_ONLY) and a deployment's .env sets it - clear both, or the test
    # reads the machine's own settings instead of its cases.
    saved = {k: os.environ.pop(k, None) for k in ("SNIPER_ENABLED", "SNIPER_ONLY")}
    try:
        check("unset -> off", sniper_mode.enabled() is False)
        os.environ["SNIPER_ENABLED"] = "0"
        check("0 -> off", sniper_mode.enabled() is False)
        os.environ["SNIPER_ENABLED"] = "1"
        check("1 -> on", sniper_mode.enabled() is True)
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v


def t_sniper_only_mode_switches_and_live_refusal():
    import asyncio
    import threading
    import sniper_mode
    import run
    saved = {k: os.environ.pop(k, None) for k in ("SNIPER_ENABLED", "SNIPER_ONLY", "SNIPER_LIVE_CANARY",
                                                  "SNIPER_PRIMARY_FEED", "SNIPER_KRAKEN_LIVE")}
    try:
        check("SNIPER_ONLY unset -> sniper_mode says so", sniper_mode.only() is False)
        try:
            run._live_sniper_gate(True)
            check("this build refuses to run without SNIPER_ONLY=1", False, "it was allowed")
        except RuntimeError as exc:
            check("this build refuses to run without SNIPER_ONLY=1 (even in paper)",
                  "SNIPER_ONLY=1" in str(exc), str(exc))
        os.environ["SNIPER_ONLY"] = "1"
        check("SNIPER_ONLY=1 turns the sniper on by itself", sniper_mode.enabled() and sniper_mode.only())
        try:
            asyncio.run(run.run(paper=False))
            check("LIVE with SNIPER_ONLY refuses to start", False, "it started")
        except RuntimeError as exc:
            check("LIVE with SNIPER_ONLY (no canary flag) refuses to start, before touching anything",
                  "refusing to start LIVE" in str(exc) and "SNIPER_LIVE_CANARY" in str(exc), str(exc))
        os.environ["SNIPER_LIVE_CANARY"] = "1"
        frozen = {"SNIPER_K_BP": "0.5", "SNIPER_WINDOW_MS": "250", "SNIPER_ORDER_TYPE": "FAK", "SNIPER_SLIP": "0.04", "SNIPER_COOLDOWN_MS": "300", "SNIPER_SHARES": "10", "SNIPER_MAX_TARGET_SHARES": "20", "SNIPER_CANARY_MAX_SHARES": "25"}                       # --live also needs the frozen spec (exec_latency)
        before = {k: os.environ.get(k) for k in frozen}
        os.environ.update(frozen)
        try:
            check("LIVE + SNIPER_ONLY + SNIPER_LIVE_CANARY is the live canary", run._live_sniper_gate(False) is True)
        finally:
            for k, v in before.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        check("SNIPER_LIVE_CANARY is ignored in paper", run._live_sniper_gate(True) is False)
        os.environ.pop("SNIPER_ONLY")
        try:
            run._live_sniper_gate(False)
            check("the live canary without SNIPER_ONLY refuses", False, "it was allowed")
        except RuntimeError as exc:
            check("the live canary without SNIPER_ONLY refuses (it must be the only thing that trades)",
                  "SNIPER_ONLY" in str(exc), str(exc))
        os.environ.pop("SNIPER_LIVE_CANARY")
        try:
            run._live_sniper_gate(False)
            check("plain LIVE (no sniper flags) refuses", False, "it was allowed")
        except RuntimeError as exc:
            check("plain LIVE (no sniper flags) refuses: nothing else in this build can trade",
                  "SNIPER_ONLY" in str(exc), str(exc))
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v

    rt = types.SimpleNamespace(stop=threading.Event(), sniper_task=None)

    async def go():
        task = asyncio.create_task(run._wait_until_stopped(rt))
        await asyncio.sleep(0.05)
        alive = not task.done()
        rt.stop.set()
        result = await asyncio.wait_for(task, timeout=2)
        return alive and result is None
    check("the runner idles until the stop signal, then ends", asyncio.run(go()))


def main() -> int:
    for name, test in sorted(globals().items()):
        if not name.startswith("t_") or not callable(test):
            continue
        try:
            test()
        except Exception as exc:
            global FAIL
            FAIL += 1
            FAILURES.append(f"{name} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{PASS} passed, {FAIL} failed")
    for failure in FAILURES[:30]:
        print("  -", failure)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
