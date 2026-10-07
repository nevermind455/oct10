#!/usr/bin/env python3
"""Accounting tests.

    python tests_accounting.py
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import tempfile
import time
import types

ROOT = pathlib.Path(__file__).parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
from accounting import fees  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from accounting.resolution import (PENDING, RESOLVED, UNKNOWN, Resolution,  # noqa: E402
                                   parse_clob_market, parse_market)
from accounting.settlement import SettlementWorker  # noqa: E402

PASS = FAIL = 0
FAILURES: list[str] = []
UP_TOK, DN_TOK, COND = "tok-up", "tok-down", "0xcond"


def _auth(token=UP_TOK, condition=COND, notional=2.0, window_end=300):
    return {
        "condition_id": condition,
        "token_id": token,
        "requested_notional": notional,
        "window_end": window_end,
        "fee_rate": 0.07,
        "fee_exponent": 1,
        "validation": "public-market-preflight",
    }


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL {name} {detail}")


def approx(a, b, tol=1e-9):
    return a is not None and abs(a - b) <= tol


# ------------------------------------------------------------------- fees --
def t_fee_matches_published_schedule():
    # Polymarket's own worked example: 100 shares, crypto, $0.50 -> $1.75
    check("crypto 100sh @0.50 = $1.75", approx(fees.taker_fee(100, 0.50), 1.75, 1e-9),
          str(fees.taker_fee(100, 0.50)))
    check("peak is at 0.50",
          fees.taker_fee(100, 0.50) > fees.taker_fee(100, 0.40) and
          fees.taker_fee(100, 0.50) > fees.taker_fee(100, 0.60))
    check("symmetric around 0.50",
          approx(fees.taker_fee(100, 0.30), fees.taker_fee(100, 0.70)))
    check("zero at the extremes", approx(fees.taker_fee(100, 1.0), 0.0) and
          approx(fees.taker_fee(100, 0.0), 0.0))
    check("geopolitics is free", approx(fees.taker_fee(100, 0.5, category="geopolitics"), 0.0))
    # Sports moved to the published 0.03 schedule; the old 0.05 overstated
    # paper cost. 100 shares at 0.50 -> 100 * 0.03 * 0.25.
    check("sports uses the published 0.03 rate",
          approx(fees.taker_fee(100, 0.5, category="sports"), 0.75),
          str(fees.taker_fee(100, 0.5, category="sports")))
    check("economics uses the published 0.05 rate",
          approx(fees.taker_fee(100, 0.5, category="economics"), 1.25))
    check("fees are rounded to five decimals",
          fees.taker_fee(1, 0.333333, 0.07) == round(fees.taker_fee(1, 0.333333, 0.07), 5))
    check("makers pay nothing", fees.maker_fee(100, 0.5) == 0.0)


def t_fixed_notional_closed_form():
    for p in (0.05, 0.25, 0.5, 0.75, 0.99):
        shares = 2.0 / p
        check(f"notional form matches share form @{p}",
              approx(fees.fee_for_notional(2.0, p), fees.taker_fee(shares, p), 1e-12),
              f"{fees.fee_for_notional(2.0, p)} vs {fees.taker_fee(shares, p)}")
    check("cheap fills cost MORE on a fixed stake",
          fees.fee_for_notional(2.0, 0.30) > fees.fee_for_notional(2.0, 0.90))


def t_breakeven_and_drag():
    check("breakeven @0.50 is 51.75%", approx(fees.breakeven_win_rate(0.50), 0.5175, 1e-9),
          str(fees.breakeven_win_rate(0.50)))
    check("breakeven always above the fill price",
          all(fees.breakeven_win_rate(p) > p for p in (0.1, 0.3, 0.5, 0.7, 0.9)))
    check("drag in bps", approx(fees.fee_drag_bps(0.50), 350.0, 1e-6),
          str(fees.fee_drag_bps(0.50)))


def t_unknown_fee_is_never_free():
    """An unknown fee is not a free fee — that silently overstates PnL."""
    check("unknown category falls back to a real rate",
          fees.theta("no-such-category") == fees.FALLBACK_THETA > 0,
          str(fees.theta("no-such-category")))
    check("live value wins when supplied", approx(fees.theta("crypto", live=0.123), 0.123))
    check("live nonsense is ignored", approx(fees.theta("crypto", live="abc"), 0.07))
    os.environ["POLY_FEE_THETA"] = "0.05"
    try:
        check("env override works", approx(fees.theta("crypto"), 0.05))
    finally:
        os.environ.pop("POLY_FEE_THETA")
    check("bps field is converted",
          approx(fees.live_theta_from_market({"fee_rate_bps": 700}), 0.07))
    check("no fee field returns None, not zero",
          fees.live_theta_from_market({"question": "x"}) is None)


# ------------------------------------------------------------- resolution --
def _market(closed=True, outcomes=("Up", "Down"), prices=("1", "0"),
            tokens=(UP_TOK, DN_TOK), status="resolved"):
    market = {"conditionId": COND, "closed": closed,
              "clobTokenIds": json.dumps(list(tokens)),
              "outcomes": json.dumps(list(outcomes)),
              "outcomePrices": json.dumps(list(prices))}
    if status is not None:
        market["umaResolutionStatus"] = status
    return market


def t_resolution_never_guesses():
    r = parse_market(_market(closed=False))
    check("open market is PENDING", r.status == PENDING, r.status)
    check("pending pays nothing", r.payout(UP_TOK) is None)

    r = parse_market(_market(closed="false"))
    check("string false is not treated as closed", r.status == PENDING, r.status)

    r = parse_market(_market(status=None))
    check("closed plus 1/0 without oracle status stays PENDING",
          r.status == PENDING, f"{r.status} {r.detail}")

    r = parse_market(_market(status="proposed"))
    check("proposed oracle outcome stays PENDING",
          r.status == PENDING, f"{r.status} {r.detail}")

    r = parse_market(_market(prices=("0.999", "0.001")))
    check("near-final market prices never become payouts",
          r.status == PENDING, f"{r.status} {r.detail}")

    r = parse_market(_market(prices=("0.5", "0.5")))
    check("official Unknown result settles both outcomes at 0.5",
          r.status == RESOLVED and approx(r.payout(UP_TOK), 0.5)
          and approx(r.payout(DN_TOK), 0.5), f"{r.status} {r.detail}")

    r = parse_market(_market(prices=("1", "1")))
    check("outcomes not summing to 1 stay PENDING", r.status == PENDING, r.detail)

    r = parse_market(_market(tokens=(UP_TOK,)))
    check("shape mismatch is UNKNOWN not a guess", r.status == UNKNOWN, r.detail)

    r = parse_market(_market(prices=("x", "0")))
    check("unparsable price is UNKNOWN", r.status == UNKNOWN, r.detail)

    r = parse_market(_market(prices=("1.1", "-0.1")))
    check("out-of-range payouts are UNKNOWN", r.status == UNKNOWN, r.detail)


def t_resolution_maps_by_label_not_index():
    """H5: clobTokenIds[0] is not guaranteed to be Up."""
    normal = parse_market(_market(outcomes=("Up", "Down"), prices=("1", "0")))
    check("normal order resolves", normal.status == RESOLVED, normal.detail)
    check("up token wins", approx(normal.payout(UP_TOK), 1.0))
    check("down token loses", approx(normal.payout(DN_TOK), 0.0))

    flipped = parse_market(_market(outcomes=("Down", "Up"), prices=("0", "1"),
                                   tokens=(DN_TOK, UP_TOK)))
    check("reversed listing still pays the up token",
          approx(flipped.payout(UP_TOK), 1.0), str(flipped.payouts))
    check("reversed listing still zeroes the down token",
          approx(flipped.payout(DN_TOK), 0.0))
    check("winner label recorded", flipped.winning_label == "Up", str(flipped.winning_label))

    weird = parse_market(_market(outcomes=("Alpha", "Beta")))
    check("unrecognised labels are UNKNOWN", weird.status == UNKNOWN, weird.detail)


def t_clob_resolution_requires_final_winner_flags():
    condition = "0x" + "1" * 64
    up, down = "101", "202"
    base = {
        "condition_id": condition, "closed": True,
        "tokens": [
            {"token_id": up, "outcome": "Up", "price": 1, "winner": True},
            {"token_id": down, "outcome": "Down", "price": 0, "winner": False},
        ],
    }
    r = parse_clob_market(base, condition)
    check("CLOB final winner surface resolves", r.status == RESOLVED, r.detail)
    check("CLOB token payout is mapped by token", r.payout(up) == 1.0,
          str(r.payouts))
    inconsistent = json.loads(json.dumps(base))
    inconsistent["tokens"][0]["winner"] = False
    r = parse_clob_market(inconsistent, condition)
    check("1/0 prices without a winner flag remain pending", r.status == PENDING,
          r.detail)
    half = json.loads(json.dumps(base))
    for row in half["tokens"]:
        row["price"] = 0.5
        row["winner"] = False
    r = parse_clob_market(half, condition)
    check("0.5/0.5 prices alone do not invent official settlement",
          r.status == PENDING, r.detail)
    half["is_50_50_outcome"] = True
    r = parse_clob_market(half, condition)
    check("explicit CLOB 50/50 outcome flag authorizes half payouts",
          r.status == RESOLVED and r.payout(up) == r.payout(down) == 0.5,
          r.detail)


# ----------------------------------------------------------------- ledger --
def _ledger():
    fd, path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    os.unlink(path)
    return Ledger(path=path), path


def t_ledger_counts_each_fill_once():
    led, path = _ledger()
    try:
        ok = led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        check("first fill recorded", ok)
        for st in ("MATCHED", "MINED", "CONFIRMED"):
            check(f"{st} replay suppressed",
                  not led.record_fill("t1", UP_TOK, shares=4.0, price=0.50,
                                      condition_id=COND, status=st))
        s = led.summary()
        check("one fill counted", s["fills_counted"] == 1, str(s))
        check("duplicates counted", s["duplicates_suppressed"] == 3, str(s))
        pos = led.positions[UP_TOK]
        check("shares not accumulated", approx(pos.shares, 4.0), str(pos.shares))
        check("fee charged", approx(pos.fees, fees.taker_fee(4.0, 0.50)), str(pos.fees))
        check("cost is notional plus fee", approx(pos.cost, 2.0 + fees.taker_fee(4.0, 0.50)),
              str(pos.cost))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_ledger_rejects_non_fills():
    led, path = _ledger()
    try:
        check("FAILED is not a fill",
              not led.record_fill("f1", UP_TOK, shares=4, price=0.5,
                                  condition_id=COND, status="FAILED"))
        check("RETRYING is not a fill yet",
              not led.record_fill("f2", UP_TOK, shares=4, price=0.5,
                                  condition_id=COND, status="RETRYING"))
        check("MATCHED is still pending",
              not led.record_fill("f5", UP_TOK, shares=4, price=0.5,
                                  condition_id=COND, status="MATCHED"))
        check("MINED is still pending",
              not led.record_fill("f6", UP_TOK, shares=4, price=0.5,
                                  condition_id=COND, status="MINED"))
        check("zero shares rejected",
              not led.record_fill("f3", UP_TOK, shares=0, price=0.5,
                                  condition_id=COND))
        check("price above 1 rejected",
              not led.record_fill("f4", UP_TOK, shares=4, price=1.5,
                                  condition_id=COND))
        check("nothing was booked", led.summary()["fills_counted"] == 0, str(led.summary()))
        check("skips are counted", led.summary()["skipped_not_a_fill"] == 4, str(led.summary()))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_provisional_trade_never_becomes_phantom_pnl():
    led, path = _ledger()
    try:
        fill = types.SimpleNamespace(
            trade_id="pending-fails", asset_id=UP_TOK, market=COND,
            side="BUY", size=4.0, price=0.5, status="MATCHED",
            sources={"user_ws"}, fee_rate_bps=700, order_id="bot-order",
        )
        led.authorize_order("bot-order", _auth())
        store = types.SimpleNamespace(fills={fill.trade_id: fill})
        check("MATCHED is not ingested", led.ingest_fill_store(store) == 0)
        fill.status = "MINED"
        check("MINED is not ingested", led.ingest_fill_store(store) == 0)
        fill.status = "FAILED"
        check("later FAILED leaves no position", led.ingest_fill_store(store) == 0)
        check("failed lifecycle booked no inventory", UP_TOK not in led.positions)

        fill.trade_id = "eventually-confirms"
        store.fills = {fill.trade_id: fill}
        fill.status = "MATCHED"
        check("second pending trade waits", led.ingest_fill_store(store) == 0)
        fill.status = "CONFIRMED"
        check("CONFIRMED is ingested exactly once", led.ingest_fill_store(store) == 1)
        check("live fee bps reaches the ledger",
              approx(led.positions[UP_TOK].fees,
                     fees.taker_fee(4.0, 0.5, 0.07)),
              str(led.positions[UP_TOK].fees))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_recovery_conditions_are_recent_durable_and_stable():
    led, path = _ledger()
    now = 900_000.0
    try:
        # Insert out of chronological order to prove the query order comes
        # from durable round time, not dictionary insertion order.
        led.authorize_order(
            "newer", _auth(token="tok-new", condition="0xnew",
                            window_end=int(now + 300)))
        led.authorize_order(
            "recent-1", _auth(token="tok-r1", condition="0xrecent",
                              window_end=int(now - 600)))
        led.authorize_order(
            "expired", _auth(token="tok-old", condition="0xexpired",
                             window_end=int(now - 7_500)))
        led.authorize_order(
            "boundary", _auth(token="tok-edge", condition="0xboundary",
                              window_end=int(
                                  now - Ledger.RECOVERY_LOOKBACK_SECONDS
                                  + Ledger.MARKET_WINDOW_SECONDS)))
        led.authorize_order(
            "outside-rest-after",
            _auth(token="tok-outside", condition="0xoutside",
                  window_end=int(now - Ledger.RECOVERY_LOOKBACK_SECONDS)))
        led.authorize_order(
            "recent-2", _auth(token="tok-r2", condition="0xrecent",
                              window_end=int(now - 300)))
        led.authorize_order("legacy-without-metadata")

        expected = ("0xboundary", "0xrecent", "0xnew")
        found = led.recovery_conditions(now=now)
        check("recovery conditions are stable, bounded, and deduplicated",
              found == expected, str(found))
        check("two-hour recovery expires older authorizations",
              "0xexpired" not in found and "0xoutside" not in found,
              str(found))
        check("metadata-free legacy authorization cannot broaden recovery",
              len(found) == 3, str(found))

        check("recovery authorization snapshot saves", led.save())
        reborn = Ledger(path=path)
        check("recovery filters survive a ledger restart",
              reborn.recovery_conditions(now=now) == expected,
              str(reborn.recovery_conditions(now=now)))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_fill_authorization_fails_closed_and_survives_restart():
    led, path = _ledger()
    try:
        def fill(trade_id="trade", *, order_id="order", token=UP_TOK,
                 condition=COND, size=4.0, price=0.5):
            return types.SimpleNamespace(
                trade_id=trade_id, asset_id=token, market=condition,
                side="BUY", size=size, price=price, status="CONFIRMED",
                sources={"user_ws"}, fee_rate_bps=0, order_id=order_id,
            )

        legacy = fill("legacy", order_id="legacy-order")
        led.authorize_order("legacy-order")
        check("metadata-free legacy authorization is rejected",
              led.ingest_fill_store(types.SimpleNamespace(fills={"legacy": legacy})) == 0)
        check("legacy rejection is classified as mismatch",
              led.skipped_authorization_mismatch == 1)

        led.authorize_order("order", _auth())
        wrong_token = fill("wrong-token", token=DN_TOK)
        wrong_market = fill("wrong-market", condition="0xother")
        overfill = fill("overfill", size=5.0)
        store = types.SimpleNamespace(fills={
            "wrong-token": wrong_token,
            "wrong-market": wrong_market,
            "overfill": overfill,
        })
        check("wrong token, market, and overfill all rejected",
              led.ingest_fill_store(store) == 0)
        before = led.skipped_authorization_mismatch
        check("repeat mismatch poll is deduplicated",
              led.ingest_fill_store(store) == 0 and
              led.skipped_authorization_mismatch == before)

        good = fill("good")
        check("matching confirmed fill is accepted once",
              led.ingest_fill_store(types.SimpleNamespace(fills={"good": good})) == 1)
        check("authorization notional restored for round",
              approx(led.authorized_notional_for_window(0), 2.0))
        check("legacy fee metadata restores conservative all-in exposure",
              approx(led.authorized_cost_for_window(0), 2.14),
              str(led.authorized_cost_for_window(0)))
        check("live accepted token leg is restored for round",
              led.authorized_tokens_for_window(0) == frozenset({UP_TOK}),
              str(led.authorized_tokens_for_window(0)))
        check("paper confirmed token leg is restored by condition",
              led.held_tokens_for_condition(COND) == frozenset({UP_TOK}),
              str(led.held_tokens_for_condition(COND)))
        check("authorization ledger saved", led.save())
        reborn = Ledger(path=path)
        check("authorization metadata survives restart",
              reborn.authorized_orders["order"]["token_id"] == UP_TOK)
        check("restart restores accepted exposure",
              approx(reborn.authorized_notional_for_window(0), 2.0))
        check("restart restores legacy all-in accepted exposure",
              approx(reborn.authorized_cost_for_window(0), 2.14),
              str(reborn.authorized_cost_for_window(0)))
        check("restart restores live complement guard",
              reborn.authorized_tokens_for_window(0) == frozenset({UP_TOK}),
              str(reborn.authorized_tokens_for_window(0)))
        check("restart restores paper complement guard",
              reborn.held_tokens_for_condition(COND) == frozenset({UP_TOK}),
              str(reborn.held_tokens_for_condition(COND)))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_durable_all_in_exposure_counts_both_legs_and_fees():
    led, path = _ledger()
    try:
        # PAPER inventory is real fill cost, not stake/notional. Complementary
        # tokens are independent lots and both remain gross exposure.
        check("paper UP fill recorded",
              led.record_fill("paper-up", UP_TOK, shares=4.0, price=0.50,
                              fee=0.03, condition_id=COND))
        check("paper DOWN fill recorded",
              led.record_fill("paper-down", DN_TOK, shares=5.0, price=0.40,
                              fee=0.02, condition_id=COND))
        expected_paper = 4.05
        check("paper raw notional excludes fees for legacy reporting",
              approx(led.confirmed_notional_for_condition(COND), 4.0))
        check("paper risk exposure includes both legs and both fees",
              approx(led.confirmed_cost_for_condition(COND), expected_paper),
              str(led.confirmed_cost_for_condition(COND)))
        check("paper replay cannot inflate all-in exposure",
              not led.record_fill("paper-down", DN_TOK, shares=5.0, price=0.40,
                                  fee=0.02, condition_id=COND)
              and approx(led.confirmed_cost_for_condition(COND), expected_paper))

        # These are accepted/pending LIVE orders: no fill is needed before the
        # cap reserves their full worst-case cash. The 5-share minimum at a
        # $0.50 cap dominates each $2 requested stake.
        up_auth = _auth(token=UP_TOK)
        up_auth["estimated_fee"] = 0.04
        down_auth = _auth(token=DN_TOK)
        down_auth["estimated_fee"] = 0.04
        led.authorize_order("pending-up", up_auth,
                            venue_min_shares=5, price_cap=0.50)
        led.authorize_order("pending-down", down_auth,
                            venue_min_shares=5, price_cap=0.50)
        expected_live = 2 * (5 * 0.50 * 1.07)
        check("pending complementary orders reserve both all-in costs",
              approx(led.authorized_cost_for_window(0), expected_live),
              str(led.authorized_cost_for_window(0)))
        check("pending reservation exists before any live fill",
              set(led.authorized_orders) == {"pending-up", "pending-down"}
              and len(led.positions) == 2, str(led.authorized_orders))
        check("fee boundary cannot use raw notional to admit another leg",
              led.authorized_notional_for_window(0) < expected_live - 0.01
              < led.authorized_cost_for_window(0))
        check("both accepted tokens remain independently durable",
              led.authorized_tokens_for_window(0)
              == frozenset({UP_TOK, DN_TOK}))
        check("reservation metadata snapshots minimum, cap, and fee",
              all(meta.get("venue_min_shares") == 5.0
                      and meta.get("price_cap") == 0.5
                      and meta.get("fee_rate") == 0.07
                      and approx(meta.get("reserved_total_cost"), 2.675)
                  for meta in led.authorized_orders.values()),
              str(led.authorized_orders))

        check("all-in exposure snapshot saved", led.save())
        reborn = Ledger(path=path)
        check("paper all-in exposure survives restart",
              approx(reborn.confirmed_cost_for_condition(COND), expected_paper))
        check("pending live all-in exposure survives restart",
              approx(reborn.authorized_cost_for_window(0), expected_live),
              str(reborn.authorized_cost_for_window(0)))
        reborn.settle(parse_market(_market()))
        summary = reborn.summary()
        check("both legs still settle as one deduplicated market",
              summary["settled_positions"] == 2
              and summary["settled_markets"] == 1
              and summary["fills_counted"] == 2, str(summary))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_active_authorization_without_fee_bound_fails_closed():
    led, path = _ledger()
    try:
        insufficient = _auth()
        insufficient.pop("fee_rate")
        insufficient.pop("fee_exponent")
        led.authorize_order("active-without-fee", insufficient)
        try:
            led.authorized_cost_for_window(0)
        except RuntimeError:
            check("active authorization without all-in bound fails closed", True)
        else:
            check("active authorization without all-in bound fails closed", False)

        check("insufficient legacy marker can still be preserved", led.save())
        reborn = Ledger(path=path)
        try:
            reborn.authorized_cost_for_window(0)
        except RuntimeError:
            check("insufficient active metadata remains fail-closed after restart", True)
        else:
            check("insufficient active metadata remains fail-closed after restart", False)

        try:
            led.authorize_order(
                "new-without-fee", insufficient,
                venue_min_shares=5, price_cap=0.5)
        except ValueError:
            check("new reservation requires durable fee metadata", True)
        else:
            check("new reservation requires durable fee metadata", False)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_corrupt_persisted_accounting_fails_closed():
    led, path = _ledger()
    try:
        led.record_fill("valid", UP_TOK, shares=4.0, price=0.5,
                        condition_id=COND)
        check("valid fixture saved before corruption", led.save())
        valid_data = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        data = json.loads(json.dumps(valid_data))
        data["positions"][UP_TOK]["cost"] = -1.0
        pathlib.Path(path).write_text(json.dumps(data), encoding="utf-8")
        try:
            Ledger(path=path)
        except RuntimeError:
            check("negative persisted cost refuses startup", True)
        else:
            check("negative persisted cost refuses startup", False)

        pathlib.Path(path).write_text(
            '{"version":3,"opened_wall":NaN,"seen":[],"positions":{},'
            '"authorized_orders":{},"balance_marks":[]}', encoding="utf-8")
        try:
            Ledger(path=path)
        except RuntimeError:
            check("non-finite persisted JSON refuses startup", True)
        else:
            check("non-finite persisted JSON refuses startup", False)

        invalid_identity = json.loads(json.dumps(valid_data))
        invalid_identity["ledger_id"] = "not-a-ledger-identity"
        pathlib.Path(path).write_text(
            json.dumps(invalid_identity), encoding="utf-8")
        try:
            Ledger(path=path)
        except RuntimeError:
            check("malformed durable ledger identity refuses startup", True)
        else:
            check("malformed durable ledger identity refuses startup", False)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_ledger_save_reuses_unchanged_positions_never_stale_ones():
    from accounting.ledger import _pos_dict
    led, path = _ledger()
    try:
        led.record_fill_durable("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        led.record_fill_durable("t2", DN_TOK, shares=6.0, price=0.40, condition_id=COND)
        led.record_fill_durable("t3", "other-tok", shares=5.0, price=0.30, condition_id="cond-other")
        led.settle(parse_market(_market()))

        def on_disk():
            return json.loads(pathlib.Path(path).read_text(encoding="utf-8"))

        def full_encoding():
            with led._lock:
                p = led._payload_locked()
                p["positions"] = {k: _pos_dict(v) for k, v in led.positions.items()}
                p["seen"] = sorted(led.seen)
            return json.loads(json.dumps(p))
        check("the saved file is exactly what a full re-encode would write", on_disk() == full_encoding())
        with led._lock:
            old_style = led._payload_locked()
            old_style["positions"] = {k: _pos_dict(v) for k, v in led.positions.items()}
            old_style["seen"] = sorted(led.seen)
        check("...byte for byte, key order included (the pre-cache encoder's exact output)",
              pathlib.Path(path).read_text(encoding="utf-8") == json.dumps(old_style, allow_nan=False))
        # A wallet written by the 8846d31 build put "positions" last; it must still load.
        moved = dict(old_style)
        moved["positions"] = moved.pop("positions")
        pathlib.Path(path).write_text(json.dumps(moved, allow_nan=False), encoding="utf-8")
        check("a wallet saved with positions last still loads to the same positions",
              {k: _pos_dict(v) for k, v in Ledger(path=path).positions.items()} == old_style["positions"])
        led.save()
        settled_text = led._pos_text[UP_TOK][1]
        led.record_fill_durable("t4", "other-tok", shares=1.0, price=0.31, condition_id="cond-other")
        check("an unchanged settled position's text is reused, not re-encoded",
              led._pos_text[UP_TOK][1] is settled_text)
        check("the changed open position is saved fresh",
              approx(on_disk()["positions"]["other-tok"]["shares"], 6.0), str(on_disk()["positions"]["other-tok"]))
        reborn = Ledger(path=path)
        check("the file reloads to the same positions",
              {k: _pos_dict(v) for k, v in reborn.positions.items()}
              == {k: _pos_dict(v) for k, v in led.positions.items()})
        # The cached `seen` list must follow a rolled-back fill: a failed save
        # discards its trade id, and a different fill then restores the size.
        real_write = led._write_payload
        led._write_payload = lambda payload: False
        try:
            led.record_fill_durable("t-rolled-back", "other-tok", shares=1.0, price=0.32, condition_id="cond-other")
            check("a failed save raises", False)
        except RuntimeError:
            pass
        led._write_payload = real_write
        led.record_fill_durable("t-after", "other-tok", shares=1.0, price=0.33, condition_id="cond-other")
        seen_on_disk = on_disk()["seen"]
        check("after a rollback the saved trade ids hold the new fill, never the rolled-back one",
              "t-after" in seen_on_disk and "t-rolled-back" not in seen_on_disk
              and seen_on_disk == sorted(led.seen), str(seen_on_disk))
        # Direct edits below are deliberately inconsistent (the loader would
        # refuse them); they only prove that any change defeats the cache.
        led.positions[UP_TOK].lots[0].consumed = 1.0
        led.save()
        check("a lot's consumed change reaches the file",
              on_disk()["positions"][UP_TOK]["lots"][0]["consumed"] == 1.0)
        led.positions[DN_TOK].realized = 123.0
        led.save()
        check("a scalar change on a settled position reaches the file",
              on_disk()["positions"][DN_TOK]["realized"] == 123.0)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_ledger_settles_only_on_venue_resolution():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        cost = led.positions[UP_TOK].cost

        led.settle(parse_market(_market(closed=False)))
        check("pending resolution settles nothing", not led.positions[UP_TOK].settled)
        check("no realized pnl while pending", led.summary()["realized_pnl"] == 0.0)
        check("pending cost reported separately",
              approx(led.summary()["pending_cost"], cost), str(led.summary()))

        led.settle(parse_market(_market()))
        pos = led.positions[UP_TOK]
        check("resolved settles", pos.settled)
        check("realized = shares*payout - cost", approx(pos.realized, 4.0 - cost),
              f"{pos.realized} vs {4.0 - cost}")
        check("winner is profitable at 0.50", pos.realized > 0)
        s = led.summary()
        check("win counted", s["wins"] == 1 and s["losses"] == 0, str(s))
        check("win rate computed", approx(s["win_rate"], 1.0))

        led2, p2 = _ledger()
        led2.record_fill("t9", DN_TOK, shares=4.0, price=0.50, condition_id=COND)
        led2.settle(parse_market(_market()))
        loser = led2.positions[DN_TOK]
        check("loser realizes the full cost", approx(loser.realized, -loser.cost),
              str(loser.realized))
        check("loss counted", led2.summary()["losses"] == 1)
        os.path.exists(p2) and os.unlink(p2)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_unrealized_marks_to_bid_and_flags_unmarkable():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        cost = led.positions[UP_TOK].cost
        u, un = led.unrealized(lambda t: 0.60)
        check("marked to the given bid", approx(u, 4.0 * 0.60 - cost), str(u))
        check("nothing unmarkable", un == 0)
        u, un = led.unrealized(lambda t: None)
        check("no bid means unmarkable, not zero", un == 1 and u == 0.0, f"{u} {un}")
        check("unmarkable position withholds total equity",
              led.summary(mark=lambda t: None)["equity_pnl"] is None)
        s = led.summary(mark=lambda t: 0.60)
        check("equity = realized + unrealized",
              approx(s["equity_pnl"], s["realized_pnl"] + s["unrealized_mark_to_bid"]))
        check("unrealized is labelled as a mark", "unrealized_mark_to_bid" in s)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_ledger_survives_restart_without_recounting():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        led.record_fill("t2", DN_TOK, shares=2.0, price=0.40, condition_id=COND)
        led.settle(parse_market(_market()))
        before = led.summary()
        check("both outcome positions grade as one market",
              before["settled_positions"] == 2 and before["settled_markets"] == 1,
              str(before))
        check("market win/loss is based on net round PnL",
              before["wins"] + before["losses"] == 1, str(before))
        check("saved", led.save())

        reborn = Ledger(path=path)
        after = reborn.summary()
        check("realized survives restart",
              approx(after["realized_pnl"], before["realized_pnl"]), str(after))
        check("fees survive restart", approx(after["fees_paid"], before["fees_paid"]))
        check("settled flag survives", reborn.positions[UP_TOK].settled)
        check("lots survive", len(reborn.positions[UP_TOK].lots) == 1)

        # this is the whole point: a reconcile poll after restart must not
        # re-book trades the socket already delivered
        check("known trade still deduped after restart",
              not reborn.record_fill("t1", UP_TOK, shares=4.0, price=0.50))
        check("counts unchanged after the replay",
              approx(reborn.summary()["realized_pnl"], before["realized_pnl"]))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_balance_reconciliation_catches_a_wrong_ledger():
    led, path = _ledger()
    try:
        check("no data before two reads", led.reconcile_balance()["status"] == "NO_DATA")
        led.mark_balance(100.0)
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        cost = led.positions[UP_TOK].cost

        led.mark_balance(100.0 - cost)          # cash left, payout not in yet
        r = led.reconcile_balance()
        check("open position accounted for", r["status"] == "OK", str(r))
        check("confirmed buy cost reported",
              approx(r["confirmed_buy_cost_in_window"], cost), str(r))

        led.settle(parse_market(_market()))
        # Resolution changes token value but does not itself redeem into pUSD.
        led.mark_balance(100.0 - cost)
        r = led.reconcile_balance()
        check("resolution without redemption reconciles", r["status"] == "OK", str(r))
        check("difference is ~0", abs(r["difference"]) < 0.02, str(r))

        led.mark_balance(100.0 - cost + 4.0)    # an external redemption occurred
        r = led.reconcile_balance()
        check("untracked redemption is surfaced", r["status"] == "MISMATCH", str(r))
        check("mismatch names external activity",
              "redemption" in r["detail"], r["detail"])
    finally:
        os.path.exists(path) and os.unlink(path)


def t_never_settles_on_anything_but_resolved():
    """The `resolved` guard is load-bearing, not decoration.

    PENDING happens to carry empty payouts, so dropping the guard looks
    harmless. UNKNOWN does NOT - it carries the payouts it managed to parse.
    Settling on those means booking PnL from a market whose outcome labels we
    could not even understand.
    """
    from accounting.resolution import Resolution
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)

        unknown = Resolution(COND, UNKNOWN, {UP_TOK: 1.0, DN_TOK: 0.0},
                             detail="unrecognised outcome labels")
        check("UNKNOWN carries payouts (the trap)", unknown.payout(UP_TOK) == 1.0)
        done = led.settle(unknown)
        check("UNKNOWN settles nothing", done == [] and not led.positions[UP_TOK].settled,
              str(done))
        check("no realized pnl from an UNKNOWN market",
              led.summary()["realized_pnl"] == 0.0, str(led.summary()))

        pending_with_payouts = Resolution(COND, PENDING, {UP_TOK: 1.0, DN_TOK: 0.0})
        check("PENDING settles nothing even with payouts present",
              led.settle(pending_with_payouts) == [])

        check("RESOLVED does settle", len(led.settle(parse_market(_market()))) == 1)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_late_fill_after_settlement_reopens():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        led.settle(parse_market(_market()))
        check("settled first", led.positions[UP_TOK].settled)
        led.record_fill("t2", UP_TOK, shares=1.0, price=0.50, condition_id=COND)
        check("late fill reopens rather than being folded in silently",
              not led.positions[UP_TOK].settled)
        check("realized cleared pending re-settle", led.positions[UP_TOK].realized is None)
    finally:
        os.path.exists(path) and os.unlink(path)


def t_reload_rejects_fabricated_realized_from_sales():
    """A sell-to-close position must cross-check realized_from_sales vs lots.

    Reload validation used to check a settled sell-to-close position (payout
    is None) only against ITSELF - realized == realized_from_sales - never
    against what its recorded SELL lots actually paid out. A hand-edited or
    corrupted ledger.json could carry any matching pair of fabricated
    realized/realized_from_sales values and load cleanly. This builds a real
    buy-then-fully-sell position, tampers with both fields on disk (keeping
    them equal to each other, exactly as the old check required), and expects
    the reload to now fail closed.
    """
    led, path = _ledger()
    try:
        led.allow_sells = True
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        led.record_fill("t2", UP_TOK, shares=4.0, price=0.60, side="SELL",
                        condition_id=COND)
        pos = led.positions[UP_TOK]
        check("fully sold position closes itself", pos.settled and pos.shares == 0.0)
        real_realized = pos.realized_from_sales
        check("test setup actually banked a nonzero realized PnL",
              abs(real_realized) > 1e-6, str(real_realized))
        check("ledger saves", led.save())

        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        entry = data["positions"][UP_TOK]
        fabricated = real_realized + 1.0
        entry["realized_from_sales"] = fabricated
        entry["realized"] = fabricated
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

        try:
            Ledger(path=path)
            rejected = False
        except RuntimeError:
            rejected = True
        check("a fabricated-but-self-consistent realized_from_sales is rejected",
              rejected)
    finally:
        os.path.exists(path) and os.unlink(path)


# ------------------------------------------------------------- settlement --
async def t_settlement_worker():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        calls = {"n": 0}

        def fetch_pending(cid):
            calls["n"] += 1
            return parse_market(_market(closed=False))

        w = SettlementWorker(led, fetch=fetch_pending, interval=0.05)
        await w.run_once()
        check("polls the open condition", calls["n"] == 1, str(calls))
        check("nothing settled while pending", led.summary()["settled_positions"] == 0)
        check("pending reason recorded", COND in w.pending_reasons, str(w.pending_reasons))

        w._fetch = lambda cid: parse_market(_market())
        n = await w.run_once()
        check("settles when resolved", n == 1, str(n))
        check("pending reason cleared", COND not in w.pending_reasons)

        n = await w.run_once()
        check("does not settle twice", n == 0, str(n))
        check("nothing left awaiting", w.summary()["awaiting"] == 0, str(w.summary()))
    finally:
        os.path.exists(path) and os.unlink(path)


async def t_settlement_survives_a_broken_endpoint():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)

        def boom(cid):
            raise RuntimeError("gamma down")

        w = SettlementWorker(led, fetch=boom, interval=0.05)
        await w.run_once()
        check("fetch failure does not crash", True)
        check("nothing settled on failure", led.summary()["settled_positions"] == 0)
        check("failure recorded as a reason", COND in w.pending_reasons, str(w.pending_reasons))
    finally:
        os.path.exists(path) and os.unlink(path)


async def t_settlement_clears_error_after_a_successful_recovery_cycle():
    led, path = _ledger()
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)

        def boom(_cid):
            raise RuntimeError("temporary resolution outage")

        w = SettlementWorker(led, fetch=boom, interval=0.05)
        await w.run_once()
        check("failed settlement cycle records its error",
              "temporary resolution outage" in (w.last_error or ""),
              str(w.summary()))

        # A binary market resolves both outcomes. Handing the ledger only the
        # winner leaves the loser unsettled forever, so the shape is rejected.
        w._fetch = lambda cid: Resolution(cid, RESOLVED, {UP_TOK: 1.0, DN_TOK: 0.0})
        n = await w.run_once()
        check("recovery cycle settles the position", n == 1, str(w.summary()))
        check("fully successful recovery clears the previous error",
              w.last_error is None, str(w.summary()))
    finally:
        os.path.exists(path) and os.unlink(path)


async def t_settlement_keeps_an_error_when_a_later_condition_succeeds():
    led, path = _ledger()
    other_token, other_condition = "tok-other", "0xcond-other"
    try:
        led.record_fill("t1", UP_TOK, shares=4.0, price=0.50, condition_id=COND)
        led.record_fill("t2", other_token, shares=2.0, price=0.50,
                        condition_id=other_condition)
        calls = []

        def mixed_fetch(cid):
            calls.append(cid)
            if cid == COND:
                raise RuntimeError("first condition unavailable")
            return Resolution(cid, PENDING, {}, detail="second condition still open")

        w = SettlementWorker(led, fetch=mixed_fetch, interval=0.05)
        await w.run_once()
        # Both are asked in the same pass, concurrently (one slow market no
        # longer holds up the next), so the order they are asked in is free.
        check("mixed cycle checks both open conditions",
              sorted(calls) == sorted([COND, other_condition]), str(calls))
        check("later successful fetch does not erase an earlier cycle error",
              "first condition unavailable" in (w.last_error or ""),
              str(w.summary()))
        check("mixed cycle retains both pending explanations",
              set(w.pending_reasons) == {COND, other_condition},
              str(w.pending_reasons))
    finally:
        os.path.exists(path) and os.unlink(path)


def t_old_unresolved_positions_are_not_abandoned():
    led, path = _ledger()
    try:
        led.record_fill("old", UP_TOK, shares=4.0, price=0.5, condition_id=COND)
        led.positions[UP_TOK].lots[0].wall = time.time() - 10 * 24 * 3600
        check("default worker keeps polling old unresolved positions",
              COND in SettlementWorker(led).open_conditions())
        check("an explicit age cap still works",
              COND not in SettlementWorker(led, max_age_s=3600).open_conditions())
    finally:
        os.path.exists(path) and os.unlink(path)


# -------------------------------------------------------------------- main --
def main() -> int:
    sync = [v for k, v in sorted(globals().items())
            if k.startswith("t_") and not asyncio.iscoroutinefunction(v)]
    aio = [v for k, v in sorted(globals().items())
           if k.startswith("t_") and asyncio.iscoroutinefunction(v)]
    for t in sync:
        try:
            t()
        except Exception as exc:
            globals()["FAIL"] += 1
            FAILURES.append(f"{t.__name__} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {t.__name__}: {type(exc).__name__}: {exc}")
    for t in aio:
        try:
            asyncio.run(t())
        except Exception as exc:
            globals()["FAIL"] += 1
            FAILURES.append(f"{t.__name__} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {t.__name__}: {type(exc).__name__}: {exc}")
    print(f"\n{PASS} passed, {FAIL} failed")
    for f in FAILURES[:20]:
        print("  -", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
