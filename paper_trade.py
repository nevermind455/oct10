"""Credential-free paper execution against the live public CLOB book.

Paper mode deliberately lives outside :mod:`polymarket_trade`.  It never
constructs a ``ClobClient``, reads a private key, derives API credentials,
creates a signature, or calls an order/cancel/account endpoint.

The simulator models the live bot's FOK market BUY:

* fetch the selected outcome's public order book at submission time;
* walk asks from best to worst, enforcing ``MIN_BUY_PRICE`` and ``MAX_BUY_PRICE``;
* fill the whole dollar amount or reject it (no partial FOK fills);
* enforce the venue-reported minimum order size;
* apply the venue's public fee curve at every consumed price level;
* record only successful simulated fills in the persistent Ledger;
* let the normal SettlementWorker settle from Polymarket's resolution.

This is realistic paper execution, not a promise that a live order would get
the same fill.  Network latency and queue movement between observation and a
real matcher acknowledgement cannot be recreated without placing an order.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, ROUND_UP
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

import requests

import http_pool
import orderbook
import timer
from accounting.ledger import Ledger, _fsync_directory


MONEY = Decimal("0.000001")
FEE_PRECISION = Decimal("0.00001")
ZERO = Decimal("0")
ONE = Decimal("1")
# Venue floor on the dollar amount of a marketable BUY, separate from the
# share minimum in the market rules.
MIN_MARKETABLE_BUY_USD = Decimal("1")


@dataclass(frozen=True)
class BookSnapshot:
    token_id: str
    asks: tuple[tuple[Decimal, Decimal], ...]  # price, available shares
    min_order_size: Decimal | None = None
    tick_size: Decimal | None = None
    timestamp: str | None = None
    book_hash: str | None = None
    received_wall: float = 0.0
    best_bid: Decimal | None = None
    # The full bid ladder, best first. An exit walks this the way an entry
    # walks asks; `best_bid` alone cannot price anything past the top level.
    bids: tuple[tuple[Decimal, Decimal], ...] = ()


@dataclass(frozen=True)
class MarketRules:
    condition_id: str
    fee_rate: Decimal
    fee_exponent: int = 1
    min_order_size: Decimal | None = None
    tick_size: Decimal | None = None
    source: str = "venue"
    up_token_id: str | None = None
    down_token_id: str | None = None
    # None means the venue says a delay feature exists but did not disclose
    # the actual delay. Paper execution must then fail closed instead of
    # inventing a duration.
    taker_delay_ms: float | None = 0.0
    # True when the venue flags a taker matching delay (itode). It can do this
    # while publishing seconds_delay=0, so a 0 above is not evidence that live
    # matches instantly - see PaperBroker's flagged_delay_ms.
    taker_delay_flagged: bool = False


@dataclass(frozen=True)
class FillQuote:
    notional: Decimal
    shares: Decimal
    average_price: Decimal
    worst_price: Decimal
    fee: Decimal
    levels: tuple[tuple[Decimal, Decimal, Decimal], ...]  # price, shares, notional

    @property
    def total_cost(self) -> Decimal:
        return self.notional + self.fee


@dataclass(frozen=True)
class SellQuote:
    """One simulated FAK exit. Unlike a FOK buy, a partial fill is a success.

    An exit that insists on all-or-nothing is not an exit: the book we are
    selling into is thin by definition - it is thin *because* the position has
    moved against us - so refusing a partial leaves the whole position on.
    """
    shares: Decimal                 # shares actually sold
    proceeds: Decimal               # gross USDC before fees
    average_price: Decimal
    worst_price: Decimal
    fee: Decimal
    levels: tuple[tuple[Decimal, Decimal, Decimal], ...]
    unfilled: Decimal               # shares the book could not absorb

    @property
    def net_proceeds(self) -> Decimal:
        return self.proceeds - self.fee


class PaperRejected(RuntimeError):
    """A paper FOK that would not be accepted/filled under the model."""


def _pre_submit_guard_error(pre_submit_guard) -> str | None:
    """Return a stable rejection reason unless an optional guard says True."""
    if pre_submit_guard is None:
        return None
    if not callable(pre_submit_guard):
        return "pre-submit guard is not callable"
    try:
        allowed = pre_submit_guard()
    except Exception as exc:
        # The callback may close over credentials or remote messages.  Its
        # exception text does not belong in the durable paper audit journal.
        return f"pre-submit guard failed closed: {type(exc).__name__}"
    from polymarket_trade import GuardRejection
    if isinstance(allowed, GuardRejection):
        return f"pre-submit guard rejected order: {allowed.reason}"
    if allowed is not True:
        return "pre-submit guard rejected order"
    return None


def _require_https_origin(host: str) -> None:
    parsed = urlsplit(str(host or ""))
    if (parsed.scheme != "https" or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        raise PaperRejected("public CLOB host must be a clean HTTPS origin")


def _decimal(value, *, name: str) -> Decimal:
    try:
        out = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PaperRejected(f"invalid {name}: {value!r}") from exc
    if not out.is_finite():
        raise PaperRejected(f"invalid {name}: {value!r}")
    return out


def parse_book(data: dict, expected_token: str, *,
               received_wall: float | None = None) -> BookSnapshot:
    """Validate and normalise one public ``GET /book`` response."""
    if not isinstance(data, dict):
        raise PaperRejected("public order book returned a non-object")
    if data.get("asset_id") is None:
        raise PaperRejected("public order book omitted asset_id")
    token = str(data.get("asset_id"))
    if token != str(expected_token):
        raise PaperRejected("public order book token does not match this order")

    aggregated: dict[Decimal, Decimal] = {}
    for raw in data.get("asks") or ():
        try:
            price = _decimal(raw.get("price") if isinstance(raw, dict) else raw[0],
                             name="ask price")
            size = _decimal(raw.get("size") if isinstance(raw, dict) else raw[1],
                            name="ask size")
        except (PaperRejected, IndexError, KeyError, TypeError) as exc:
            raise PaperRejected("public order book has a malformed ask level") from exc
        if not ZERO < price < ONE or size <= ZERO:
            raise PaperRejected("public order book has an out-of-range ask level")
        aggregated[price] = aggregated.get(price, ZERO) + size
    asks = tuple(sorted(aggregated.items(), key=lambda level: level[0]))

    bid_levels: dict[Decimal, Decimal] = {}
    for raw in data.get("bids") or ():
        try:
            price = _decimal(raw.get("price") if isinstance(raw, dict) else raw[0],
                             name="bid price")
            size = _decimal(raw.get("size") if isinstance(raw, dict) else raw[1],
                            name="bid size")
        except (PaperRejected, IndexError, KeyError, TypeError) as exc:
            raise PaperRejected("public order book has a malformed bid level") from exc
        if not ZERO < price < ONE or size <= ZERO:
            raise PaperRejected("public order book has an out-of-range bid level")
        bid_levels[price] = bid_levels.get(price, ZERO) + size
    bids = tuple(sorted(bid_levels.items(), key=lambda level: -level[0]))
    best_bid = bids[0][0] if bids else None
    if asks and best_bid is not None and best_bid >= asks[0][0]:
        raise PaperRejected("public order book is crossed or locked")

    def optional_decimal(*names):
        for name in names:
            value = data.get(name)
            if value not in (None, ""):
                parsed = _decimal(value, name=name)
                if parsed <= ZERO:
                    raise PaperRejected(f"public order book has invalid {name}")
                return parsed
        return None

    timestamp = data.get("timestamp")
    try:
        timestamp_ms = int(timestamp)
    except (TypeError, ValueError) as exc:
        raise PaperRejected("public order book omitted a valid exchange timestamp") from exc
    if timestamp_ms <= 0:
        raise PaperRejected("public order book has an invalid exchange timestamp")

    return BookSnapshot(
        token_id=token,
        asks=asks,
        best_bid=best_bid,
        bids=bids,
        min_order_size=optional_decimal("min_order_size", "minimum_order_size"),
        tick_size=optional_decimal("tick_size", "minimum_tick_size"),
        timestamp=str(timestamp_ms),
        book_hash=str(data.get("hash")) if data.get("hash") is not None else None,
        received_wall=_receipt_wall(received_wall),
    )


def _receipt_wall(received_wall: float | None = None, *,
                  updated_mono: float | None = None) -> float:
    """Wall time when this copy of the book arrived in our hands.

    For a websocket view, convert monotonic receipt age onto CLOB-aligned
    wall time so the later held-age check measures the same interval as
    ``BookView.book_age_ms``. Stamping ``now`` here would make a quiet but
    already-old socket book look freshly fetched.
    """
    if received_wall is not None:
        try:
            stamp = float(received_wall)
        except (TypeError, ValueError) as exc:
            raise PaperRejected("public order book has an invalid receipt time") from exc
        if not math.isfinite(stamp) or stamp <= 0:
            raise PaperRejected("public order book has an invalid receipt time")
        return stamp
    if updated_mono is not None:
        try:
            age_s = time.monotonic() - float(updated_mono)
        except (TypeError, ValueError) as exc:
            raise PaperRejected("websocket order book has an invalid receipt time") from exc
        if not math.isfinite(age_s) or age_s < 0 or age_s >= 86_400:
            raise PaperRejected("websocket order book has an invalid receipt time")
        return timer.wall() - age_s
    return timer.wall()


def fetch_public_book(token_id: str, *, host: str, timeout: float = 8.0) -> BookSnapshot:
    """Read the unauthenticated CLOB book. No wallet material is involved."""
    _require_https_origin(host)
    response = http_pool.get(f"{host.rstrip('/')}/book",
                             params={"token_id": str(token_id)}, timeout=timeout)
    # Stamp arrival before parsing so held-age is the copy we hold, not
    # however long decoding took. Same split as orderbook.get_orderbook.
    received_wall = timer.wall()
    response.raise_for_status()
    return parse_book(response.json(), str(token_id), received_wall=received_wall)


def snapshot_from_book_view(view, *, expected_token: str | None = None) -> BookSnapshot:
    """Turn a LIVE websocket ``BookView`` into the paper FOK snapshot."""
    token = str(getattr(view, "token", "") or "")
    if expected_token is not None and token != str(expected_token):
        raise PaperRejected("public order book token does not match this order")
    aggregated: dict[Decimal, Decimal] = {}
    for level in getattr(view, "asks", ()) or ():
        try:
            price = _decimal(level[0], name="ask price")
            size = _decimal(level[1], name="ask size")
        except (IndexError, TypeError) as exc:
            raise PaperRejected("public order book has a malformed ask level") from exc
        if not ZERO < price < ONE or size <= ZERO:
            raise PaperRejected("public order book has an out-of-range ask level")
        aggregated[price] = aggregated.get(price, ZERO) + size
    asks = tuple(sorted(aggregated.items(), key=lambda item: item[0]))
    bid_agg: dict[Decimal, Decimal] = {}
    for level in getattr(view, "bids", ()) or ():
        try:
            price = _decimal(level[0], name="bid price")
            size = _decimal(level[1], name="bid size")
        except (IndexError, TypeError) as exc:
            raise PaperRejected("public order book has a malformed bid level") from exc
        if not ZERO < price < ONE or size <= ZERO:
            raise PaperRejected("public order book has an out-of-range bid level")
        bid_agg[price] = bid_agg.get(price, ZERO) + size
    bids = tuple(sorted(bid_agg.items(), key=lambda item: -item[0]))
    best_bid_raw = getattr(view, "best_bid", None)
    best_bid = (_decimal(best_bid_raw, name="bid")
                if best_bid_raw is not None else None)
    if best_bid is not None and not ZERO < best_bid < ONE:
        raise PaperRejected("public order book has an out-of-range bid")
    if asks and best_bid is not None and best_bid >= asks[0][0]:
        raise PaperRejected("public order book is crossed or locked")
    ts = getattr(view, "exchange_ts_ms", None)
    if ts is None:
        raise PaperRejected("websocket order book omitted exchange timestamp")
    try:
        ts = int(ts)
    except (TypeError, ValueError) as exc:
        raise PaperRejected("websocket order book has an invalid exchange timestamp") from exc
    if ts <= 0:
        raise PaperRejected("websocket order book has an invalid exchange timestamp")
    tick_raw = getattr(view, "tick_size", None)
    tick = (_decimal(tick_raw, name="tick")
            if tick_raw not in (None, "") else None)
    if tick is not None and not ZERO < tick < ONE:
        raise PaperRejected("websocket order book has an invalid tick size")
    digest = getattr(view, "hash", None)
    return BookSnapshot(
        token_id=token,
        asks=asks,
        best_bid=best_bid,
        bids=bids,
        tick_size=tick,
        timestamp=str(int(ts)),
        book_hash=str(digest) if digest else None,
        received_wall=_receipt_wall(
            updated_mono=getattr(view, "updated_mono", None)),
    )


def fetch_executable_book(token_id: str, *, host: str, ws_view=None,
                          timeout: float = 8.0) -> BookSnapshot:
    """Prefer a LIVE websocket book that still has asks; else public REST."""
    token = str(token_id)
    if ws_view is not None and getattr(ws_view, "asks", None):
        status = str(getattr(ws_view, "status", "") or "")
        if status == "LIVE":
            try:
                snap = snapshot_from_book_view(ws_view, expected_token=token)
                if snap.asks:
                    return snap
            except PaperRejected:
                pass
    return fetch_public_book(token, host=host, timeout=timeout)


def estimate_sell_fak(book: BookSnapshot, shares, min_price,
                      rules: MarketRules) -> SellQuote:
    """Walk live bids downward and return what an exit would actually get.

    ``min_price`` is a floor, not a target: levels below it are left alone, so
    a stop can decline to dump into an empty book rather than accept any price
    at all. Whatever the book cannot absorb above the floor comes back as
    ``unfilled`` for the caller to retry or abandon.
    """
    wanted = _decimal(shares, name="shares")
    floor = _decimal(min_price, name="minimum sell price")
    if wanted <= ZERO:
        raise PaperRejected("paper sell size must be positive")
    if floor < ZERO or floor >= ONE:
        raise PaperRejected("minimum sell price must be in [0, 1)")
    if not book.bids:
        raise PaperRejected("cannot sell: no bids on the live book")

    remaining = wanted
    sold = proceeds = total_fee = ZERO
    used: list[tuple[Decimal, Decimal, Decimal]] = []
    worst = ZERO
    for price, available in book.bids:
        if price < floor or remaining <= ZERO:
            break
        take = min(remaining, available)
        if take <= ZERO:
            continue
        notional = take * price
        used.append((price, take, notional))
        sold += take
        proceeds += notional
        total_fee += curve_fee(take, price, rules)
        remaining -= take
        worst = price

    if sold <= ZERO:
        best = book.bids[0][0]
        raise PaperRejected(
            f"cannot sell: best bid {best} is below the floor {floor}")
    total_fee = total_fee.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP)
    return SellQuote(sold, proceeds, proceeds / sold, worst, total_fee,
                     tuple(used), remaining)


def parse_market_rules(data: dict, condition_id: str, *, category: str = "crypto",
                       market_data: dict | None = None) -> MarketRules:
    """Parse public V2 CLOB parameters; unknown fees are not simulated."""
    if not isinstance(data, dict):
        raise PaperRejected("CLOB market rules returned a non-object")
    fd = data.get("fd") if isinstance(data.get("fd"), dict) else {}
    source = "venue"
    try:
        rate = _decimal(fd.get("r"), name="fee rate")
        exponent_raw = fd.get("e", 1)
        if isinstance(exponent_raw, bool):
            raise ValueError
        exponent_decimal = _decimal(exponent_raw, name="fee exponent")
        if exponent_decimal != exponent_decimal.to_integral_value():
            raise ValueError
        exponent = int(exponent_decimal)
        if (rate <= ZERO or rate > ONE or not 1 <= exponent <= 8
                or fd.get("to") is not True):
            raise ValueError
    except (PaperRejected, TypeError, ValueError) as exc:
        raise PaperRejected("CLOB market omitted valid live fee parameters") from exc

    def optional(key):
        if data.get(key) in (None, ""):
            return None
        out = _decimal(data.get(key), name=key)
        if out <= ZERO:
            raise PaperRejected(f"CLOB market has invalid {key}")
        return out

    token_rows = data.get("t")
    if not isinstance(token_rows, list) or len(token_rows) != 2:
        raise PaperRejected("CLOB market rules do not describe a binary market")
    labelled = {}
    for row in token_rows:
        if not isinstance(row, dict):
            raise PaperRejected("invalid CLOB token mapping")
        label = str(row.get("o") or "").strip().lower()
        if label in labelled:
            raise PaperRejected("duplicate CLOB outcome label")
        labelled[label] = str(row.get("t") or "")
    if (set(labelled) != {"up", "down"} or not all(labelled.values())
            or labelled["up"] == labelled["down"]):
        raise PaperRejected("CLOB outcomes are not exactly UP and DOWN")
    taker_delay_ms: float | None
    if "itode" in data and not isinstance(data.get("itode"), bool):
        raise PaperRejected("CLOB market has invalid matching-delay flag")
    if market_data is None:
        taker_delay_ms = None if data.get("itode") is True else 0.0
    else:
        returned_condition = str(market_data.get("condition_id") or "")
        if returned_condition and returned_condition != str(condition_id):
            raise PaperRejected("public market metadata condition mismatch")
        rows = market_data.get("tokens")
        if not isinstance(rows, list) or len(rows) != 2:
            raise PaperRejected("public market metadata is not binary")
        metadata_tokens = {
            str(row.get("outcome") or "").strip().lower(): str(row.get("token_id") or "")
            for row in rows if isinstance(row, dict)
        }
        if metadata_tokens != labelled:
            raise PaperRejected("public market endpoints disagree on UP/DOWN mapping")
        try:
            if isinstance(market_data.get("seconds_delay"), bool):
                raise TypeError
            seconds_delay = float(market_data.get("seconds_delay"))
        except (TypeError, ValueError) as exc:
            raise PaperRejected("public market metadata omitted the matching delay") from exc
        if not math.isfinite(seconds_delay) or not 0 <= seconds_delay <= 60:
            raise PaperRejected("public market metadata has an invalid matching delay")
        taker_delay_ms = seconds_delay * 1000.0

    minimum, tick = optional("mos"), optional("mts")
    if minimum is None or tick is None or tick >= ONE:
        raise PaperRejected("CLOB market omitted valid minimum-size/tick rules")

    return MarketRules(
        str(condition_id), rate, exponent,
        min_order_size=minimum, tick_size=tick, source=source,
        up_token_id=labelled["up"], down_token_id=labelled["down"],
        taker_delay_ms=taker_delay_ms,
        taker_delay_flagged=data.get("itode") is True,
    )


def fetch_market_rules(condition_id: str, *, host: str, category: str = "crypto",
                       timeout: float = 8.0) -> MarketRules:
    """Read and cross-check both unauthenticated market parameter surfaces."""
    _require_https_origin(host)
    response = http_pool.get(
        f"{host.rstrip('/')}/clob-markets/{condition_id}", timeout=timeout)
    response.raise_for_status()
    metadata = http_pool.get(
        f"{host.rstrip('/')}/markets/{condition_id}", timeout=timeout)
    metadata.raise_for_status()
    return parse_market_rules(
        response.json(), str(condition_id), category=category,
        market_data=metadata.json())


def curve_fee(shares: Decimal, price: Decimal, rules: MarketRules) -> Decimal:
    """V2 fee curve: shares * rate * (price * (1-price)) ** exponent."""
    raw = shares * rules.fee_rate * (price * (ONE - price)) ** rules.fee_exponent
    return raw.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP)


def size_to_venue_minimum(amount, book: BookSnapshot, rules: MarketRules,
                          max_price) -> Decimal:
    """Raise a dollar stake just enough to buy the venue's minimum shares.

    The ask-ladder walk itself lives in orderbook.venue_minimum_stake,
    shared with polymarket_trade.py's live order sizing, so a correction
    only ever needs to land once.
    """
    wanted = _decimal(amount, name="amount").quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    minimums = [v for v in (book.min_order_size, rules.min_order_size) if v is not None]
    if not minimums or not book.asks:
        return wanted
    minimum = max(minimums)
    tick = rules.tick_size
    if tick is None or not ZERO < tick < ONE:
        return wanted
    configured_cap = _decimal(max_price, name="maximum buy price")
    cap = min(configured_cap, ONE - tick)
    cap = (cap / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    return orderbook.venue_minimum_stake(wanted, book.asks, minimum, cap)


def minimum_spend_at_limit(book: BookSnapshot, rules: MarketRules, max_price) -> Decimal:
    """The smallest stake whose order, as signed, reaches the venue's share
    minimum: the SDK signs a BUY's size at its LIMIT price (amount / limit),
    so with a wide limit 10 shares' worth at the ask can fall under 5 shares
    at the limit. Mirrors sniper_live's pre-send sizing."""
    minimums = [v for v in (book.min_order_size, rules.min_order_size) if v is not None]
    tick = rules.tick_size
    if not minimums or tick is None or not ZERO < tick < ONE:
        return ZERO
    cap = min(_decimal(max_price, name="maximum buy price"), ONE - tick)
    cap = (cap / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    return (max(minimums) * cap).quantize(Decimal("0.01"), rounding=ROUND_CEILING)


def estimate_fok(book: BookSnapshot, amount, max_price,
                 rules: MarketRules, min_price=0) -> FillQuote:
    """Walk live asks and return a full-fill quote or raise ``PaperRejected``."""
    wanted = _decimal(amount, name="amount").quantize(MONEY, rounding=ROUND_HALF_UP)
    configured_cap = _decimal(max_price, name="maximum buy price")
    configured_floor = _decimal(min_price, name="minimum buy price")
    if wanted <= ZERO:
        raise PaperRejected("paper amount must be positive")
    if not ZERO < configured_cap < ONE:
        raise PaperRejected("maximum buy price must be between 0 and 1")
    if configured_floor < ZERO or configured_floor >= configured_cap:
        raise PaperRejected("minimum buy price must be below the maximum buy price")
    tick = rules.tick_size
    if tick is None or not ZERO < tick < ONE:
        raise PaperRejected("paper market has no valid venue tick size")
    if book.tick_size is not None and book.tick_size != tick:
        raise PaperRejected("public book and market rules disagree on tick size")
    cap = min(configured_cap, ONE - tick)
    cap = (cap / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    if cap <= ZERO:
        raise PaperRejected("maximum buy price is below one venue tick")
    floor = ZERO if configured_floor <= ZERO else (
        (configured_floor / tick).to_integral_value(rounding=ROUND_CEILING) * tick)
    if not book.asks:
        raise PaperRejected("cannot FOK buy: no asks on the live book")
    if book.asks[0][0] < floor:
        raise PaperRejected(
            f"best ask {book.asks[0][0]} is below MIN_BUY_PRICE {floor}")

    remaining = wanted
    total_shares = ZERO
    total_fee = ZERO
    used: list[tuple[Decimal, Decimal, Decimal]] = []
    worst = ZERO
    for price, available_shares in book.asks:
        if price > cap:
            break
        available_notional = price * available_shares
        spend = min(remaining, available_notional)
        if spend <= ZERO:
            continue
        shares = spend / price
        used.append((price, shares, spend))
        total_shares += shares
        total_fee += curve_fee(shares, price, rules)
        remaining -= spend
        worst = price
        if remaining <= MONEY / Decimal("2"):
            remaining = ZERO
            break

    if remaining > ZERO:
        available = wanted - remaining
        raise PaperRejected(
            f"FOK no-fill: only ${available:.6f} available at or below {cap}")

    minimums = [v for v in (book.min_order_size, rules.min_order_size) if v is not None]
    minimum = max(minimums) if minimums else None
    if minimum is not None and total_shares < minimum:
        raise PaperRejected(
            f"order size {total_shares:.6f} shares is below venue minimum {minimum}")

    total_fee = total_fee.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP)
    return FillQuote(wanted, total_shares, wanted / total_shares, worst,
                     total_fee, tuple(used))


def estimate_fak(book: BookSnapshot, amount, max_price, rules: MarketRules, min_price=0,
                 min_partial_shares=0) -> FillQuote:
    """Fill-and-kill BUY: estimate_fok's walk, but whatever the levels at or
    under the cap hold is taken and the rest cancelled. A full fill is
    exactly estimate_fok's quote. A partial returns what was SPENT as its
    notional. Raises PaperRejected("FAK no-fill ...") only when nothing is
    there (or less than min_partial_shares); every other refusal is
    estimate_fok's own. The venue's share/$ minimums are checked on the
    order as placed (the pre-send check), not on each partial match."""
    try:
        return estimate_fok(book, amount, max_price, rules, min_price=min_price)
    except PaperRejected as exc:
        if "fok no-fill" not in str(exc).lower():
            raise
    wanted = _decimal(amount, name="amount").quantize(MONEY, rounding=ROUND_HALF_UP)
    tick = rules.tick_size
    cap = min(_decimal(max_price, name="maximum buy price"), ONE - tick)
    cap = (cap / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    remaining, spent, shares, fee, worst = wanted, ZERO, ZERO, ZERO, ZERO
    used: list[tuple[Decimal, Decimal, Decimal]] = []
    for price, available_shares in book.asks:
        if price > cap:
            break
        spend = min(remaining, price * available_shares)
        if spend <= ZERO:
            continue
        got = spend / price
        used.append((price, got, spend))
        shares += got
        fee += curve_fee(got, price, rules)
        spent += spend
        remaining -= spend
        worst = price
    if shares <= ZERO:
        raise PaperRejected(f"FAK no-fill: nothing at or below {cap}")
    if min_partial_shares and shares < Decimal(str(min_partial_shares)):
        raise PaperRejected(f"FAK no-fill: partial {shares:.6f} shares is below {min_partial_shares}")
    return FillQuote(spent, shares, spent / shares, worst,
                     fee.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP), tuple(used))


def _start_daemon_timer(delay_s: float, fn: Callable[[], None]) -> None:
    """Run ``fn`` once after ``delay_s`` seconds on a daemon thread."""
    handle = threading.Timer(max(0.0, float(delay_s)), fn)
    handle.daemon = True
    handle.start()


class PaperBroker:
    """Persistent paper cash, fills and venue-resolution PnL."""

    supports_arrival_book = True     # place_trade(arrival_book=...) - see its docstring
    supports_size_at_limit = True    # place_trade(size_at_limit=...) - the sniper sizes as its live sender

    mode = "PAPER"

    def __init__(
        self,
        ledger: Ledger,
        *,
        market_context: Callable[[], dict],
        host: str,
        max_buy_price: float,
        min_buy_price: float = 0.0,
        start_balance: float = 1000.0,
        account_path: str | os.PathLike | None = None,
        audit_path: str | os.PathLike | None = None,
        book_fetch: Callable[[str], BookSnapshot] | None = None,
        rules_fetch: Callable[[str], MarketRules] | None = None,
        category: str = "crypto",
        latency_ms: float = 0.0,
        max_book_age_s: float = 8.0,
        max_quiet_s: float = 900.0,
        future_tol_s: float = 5.0,
        max_spread: float = 0.25,
        min_seconds_to_expiry: float = 1.0,
        trade_window_seconds: float = 60.0,
        on_event=None,
        fill_delay_probes: tuple = (),
        probe_scheduler: Callable[[float, Callable[[], None]], None] | None = None,
        flagged_delay_ms: float = 0.0,
        own_fill_ttl_s: float = 0.0,
    ) -> None:
        self.ledger = ledger
        # How long our own paper fills keep their shares out of the public
        # book (0 = off). See _net_of_own_fills.
        self.own_fill_ttl_s = max(0.0, float(own_fill_ttl_s))
        self._own_fills: dict[str, list] = {}
        # Matching delay to assume when the venue flags one (itode) but
        # publishes seconds_delay=0. Trusting that 0 is what let paper match
        # 0.15s after deciding while live orders sat through the real delay.
        self.flagged_delay_ms = max(0.0, float(flagged_delay_ms))
        self.market_context = market_context
        self.host = host
        self.max_buy_price = float(max_buy_price)
        self.min_buy_price = float(min_buy_price)
        self.category = category
        self.latency_ms = float(latency_ms)
        self.max_book_age_s = float(max_book_age_s)
        self.max_quiet_s = float(max_quiet_s)
        self.future_tol_s = float(future_tol_s)
        self.max_spread = float(max_spread)
        self.min_seconds_to_expiry = float(min_seconds_to_expiry)
        self.trade_window_seconds = float(trade_window_seconds)
        if (not 0 <= self.latency_ms < 60_000 or self.max_book_age_s <= 0
                or self.max_quiet_s <= 0
                or not 0 <= self.future_tol_s < 86_400
                or not 0 < self.max_spread <= 1
                or not 0 <= self.min_seconds_to_expiry < 300
                or not self.min_seconds_to_expiry < self.trade_window_seconds <= 300
                or not 0 <= self.min_buy_price < self.max_buy_price < 1):
            raise ValueError("invalid paper latency/book-age/spread configuration")
        self.fill_delay_probes = tuple(float(d) for d in (fill_delay_probes or ()))
        if any(not math.isfinite(d) or not 0 < d <= 60 for d in self.fill_delay_probes):
            raise ValueError("fill delay probes must be between 0 and 60 seconds")
        self.on_event = on_event
        base = Path(ledger.path).resolve().parent
        self.account_path = Path(account_path or base / "paper_account.json")
        self.audit_path = Path(audit_path or base / "paper_orders.jsonl")
        # Diagnostic journal for fill-delay probes. Named from the audit file
        # so each paper profile keeps its own, and deliberately NOT part of the
        # state set below: it holds no cash or positions and may be deleted.
        self.delay_journal_path = self.audit_path.with_name(
            f"{self.audit_path.stem}_fill_delay.jsonl")
        # Probes run on their own timers, never on the trading thread - a
        # 5-second probe inline would stall the loop 5 seconds per fill.
        self._schedule_probe = probe_scheduler or _start_daemon_timer
        self._probe_lock = threading.Lock()
        state_paths = {
            Path(self.ledger.path).resolve(), self.account_path.resolve(),
            self.audit_path.resolve(),
        }
        if len(state_paths) != 3:
            raise ValueError("paper ledger, account, and audit paths must be distinct")
        self._book_fetch = book_fetch or (
            lambda token: fetch_public_book(token, host=self.host))
        self._rules_fetch = rules_fetch or (
            lambda cid: fetch_market_rules(cid, host=self.host, category=self.category))
        self._lock = threading.RLock()
        # One paper ENTRY per token at a time: a second order on the same
        # token while one is in flight is refused as a duplicate. Orders on
        # different tokens (a market's UP and DOWN) may be in flight together;
        # each books atomically under _lock (cash re-checked against its own
        # actual cost there), so they cannot overspend between them.
        self._execution_locks: dict[str, threading.Lock] = {}
        # last_fill / last_error / last_timing per thread too: a caller
        # matching two orders at once reads ITS order's outcome on the thread
        # that placed it (other readers see the latest, as before).
        self._results = threading.local()
        # Exits get their own lock. The execution lock exists to stop two
        # concurrent ENTRIES becoming a duplicate order; an exit is not a
        # duplicate entry, and sharing the lock meant a stop firing rejected
        # that cycle's buy outright - which main_bot then followed with a full
        # TRADE_INTERVAL_SECONDS cooldown, losing the cycle entirely. The
        # ledger's own lock still serialises the critical section, so cash and
        # inventory stay consistent across the two paths.
        self._exit_lock = threading.Lock()
        # condition_id -> (MarketRules, fetched_wall). See _rules().
        self._rules_cache: dict[str, tuple] = {}
        self._rules_cache_ttl = 300.0
        self.last_error: str | None = None
        self.last_fill: dict | None = None
        self.filled_orders = 0
        self.rejected_orders = 0
        self.started_wall = time.time()
        self.start_balance = self._load_or_create_account(start_balance)

    # --------------------------------------------------------- account
    def _load_or_create_account(self, requested) -> float:
        requested_d = _decimal(requested, name="paper starting balance")
        if requested_d <= ZERO:
            raise ValueError("PAPER_START_BALANCE must be positive")
        try:
            data = json.loads(self.account_path.read_text(encoding="utf-8"))
            if (not isinstance(data, dict)
                    or type(data.get("version")) is not int
                    or data.get("version") not in (1, 2)):
                raise ValueError("unsupported paper account schema")
            created = float(data["created_at"])
            if not math.isfinite(created) or created <= 0:
                raise ValueError("invalid paper account creation timestamp")
            saved = _decimal(data["starting_balance"], name="saved paper starting balance")
            if saved <= ZERO:
                raise ValueError("invalid saved paper starting balance")

            # An account file proves that a ledger previously existed.  If the
            # ledger is now absent, accepting a newly constructed empty Ledger
            # would erase every fill and make cash jump back to starting cash.
            if not self.ledger.loaded_from_disk:
                raise ValueError(
                    "paper account exists but its ledger file is missing; "
                    "restore the matching ledger")

            if data["version"] == 1:
                self._validate_legacy_account_pair(data, created)
                self._ensure_ledger_identity_durable()
                self._atomic_json(self._account_payload(created, saved))
            else:
                saved_id = str(data.get("ledger_id") or "")
                if (self.ledger.schema_version != 4
                        or saved_id != self.ledger.ledger_id):
                    raise ValueError(
                        "paper account and ledger identities do not match; "
                        "restore them as a pair")
                # ledger_path is diagnostic in V2; identity, not location,
                # owns the pairing.  Refresh it after a safe directory move.
                if str(data.get("ledger_path") or "") != self._ledger_path_text():
                    self._atomic_json(self._account_payload(created, saved))
            return float(saved)
        except FileNotFoundError:
            data = None
        except Exception as exc:
            raise RuntimeError(
                f"invalid paper state pair at {self.account_path}: {exc}") from exc
        if (self.ledger.seen or self.ledger.positions
                or self.ledger.authorized_orders):
            raise RuntimeError(
                "paper ledger contains fills but paper_account.json is missing; "
                "restore the account file instead of guessing a starting balance")
        self._ensure_ledger_identity_durable()
        self._atomic_json(self._account_payload(time.time(), requested_d))
        return float(requested_d)

    def _ledger_path_text(self) -> str:
        return str(Path(self.ledger.path).resolve())

    def _account_payload(self, created, balance) -> dict:
        return {
            "version": 2,
            "created_at": float(created),
            "starting_balance": float(balance),
            "ledger_id": self.ledger.ledger_id,
            # Kept for operator diagnostics only.  The random ledger_id is the
            # durable pairing key, so moving this whole directory is safe.
            "ledger_path": self._ledger_path_text(),
            "note": "Cash is derived from ledger fills and venue-settled payouts.",
        }

    def _ensure_ledger_identity_durable(self) -> None:
        if (self.ledger.schema_version == 4
                and Path(self.ledger.path).is_file()):
            return
        if not self.ledger.save():
            detail = self.ledger.last_persistence_error or "unknown persistence failure"
            raise RuntimeError(f"paper ledger identity could not be saved: {detail}")

    def _validate_legacy_account_pair(self, data: dict, created: float) -> None:
        """Safely bind a path-based V1 account once, then migrate it to V2.

        A matching old path is the normal upgrade.  If the whole state
        directory was moved before upgrade, creation times prove an empty
        ledger pair; a non-empty ledger additionally has to match the durable
        FILLED audit IDs.  Ambiguous mixes fail closed for operator recovery.
        """
        saved_ledger = Path(str(data["ledger_path"])).resolve()
        current_ledger = Path(self.ledger.path).resolve()
        account_parent = self.account_path.resolve().parent
        creation_gap = created - float(self.ledger.opened_wall)
        if not 0 <= creation_gap <= 2.0:
            raise ValueError(
                "legacy paper account creation does not match this ledger")
        if (saved_ledger != current_ledger
                and (saved_ledger.name != current_ledger.name
                     or account_parent != current_ledger.parent)):
            raise ValueError(
                "legacy paper account points at another ledger and cannot be "
                "safely migrated")
        if self.ledger.seen:
            try:
                rows = [json.loads(line) for line in
                        self.audit_path.read_text(encoding="utf-8").splitlines()
                        if line.strip()]
                fill_ids = [str(row.get("order_id") or "") for row in rows
                            if isinstance(row, dict) and row.get("status") == "FILLED"]
            except Exception as exc:
                raise ValueError(
                    "legacy moved ledger has no valid matching paper audit") from exc
            if (len(fill_ids) != len(set(fill_ids))
                    or set(fill_ids) != set(self.ledger.seen)):
                raise ValueError(
                    "legacy moved ledger does not match the paper audit")

    def _atomic_json(self, payload: dict) -> None:
        self.account_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.account_path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True, allow_nan=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.account_path)
            _fsync_directory(self.account_path.parent)
        except Exception:
            try:
                os.unlink(tmp)
            except OSError as cleanup_exc:
                self.last_error = (
                    f"paper account temp cleanup failed: {type(cleanup_exc).__name__}")
            raise

    def cash_balance(self) -> float:
        """Starting cash - open cost + payouts + PnL already banked by selling.

        Selling reduces ``position.cost`` by the basis of the shares that left,
        which returns that basis to cash on its own. The gain or loss ON the
        sale is not in ``cost`` at all - it lives in ``realized_from_sales`` -
        so without this third term an exit would credit only what the shares
        originally cost and silently discard the result of selling them.
        """
        with self.ledger._lock:
            spent = sum(position.cost for position in self.ledger.positions.values())
            payouts = sum(
                position.shares * float(position.payout_per_share or 0.0)
                for position in self.ledger.positions.values() if position.settled)
            banked = sum(position.realized_from_sales
                         for position in self.ledger.positions.values())
        return self.start_balance - spent + payouts + banked

    def get_balance_allowance(self) -> dict:
        cash = self.cash_balance()
        return {"balance": cash, "allowance": cash, "paper": True}

    def cancel_all_open_orders(self) -> bool:
        # Every simulated order is FOK and therefore terminal immediately.
        self.last_error = None
        print("[PAPER] No resting orders to cancel (paper orders are immediate FOK).")
        return True

    # --------------------------------------------------------- execution
    def _context(self) -> tuple[str, str | None, str | None]:
        context = self.market_context() or {}
        condition = str(context.get("condition_id") or "")
        if not condition:
            raise PaperRejected("missing current condition id; cannot settle paper PnL")
        up = str(context.get("up_token_id")) if context.get("up_token_id") else None
        down = str(context.get("down_token_id")) if context.get("down_token_id") else None
        return condition, up, down

    def _rules(self, condition_id: str) -> MarketRules:
        """Venue parameters for one market, cached for the market's lifetime.

        Every field on MarketRules - fee rate and exponent, minimum size, tick,
        taker delay - is fixed when the market is created, yet this used to be
        re-fetched on every fill: two HTTP calls per order, six per cycle once
        the multi-signal legs are on, all returning identical bytes. Measured
        against the live venue those calls run 0.5s normally but spike past
        8653ms, and the fetch's own timeout is 8.0s - so the spike surfaced as
        "cannot read live market rules: ReadTimeout" and cost the order.

        The cache is per condition and time-bounded. A five-minute market never
        outlives one entry, and the TTL means a genuinely changed parameter is
        picked up rather than pinned for the life of the process.
        """
        key = str(condition_id or "")
        now = time.time()
        with self._lock:
            hit = self._rules_cache.get(key)
            if hit is not None and now - hit[1] <= self._rules_cache_ttl:
                return hit[0]
        try:
            rules = self._rules_fetch(condition_id)
        except Exception as exc:
            # Fail closed on unknown V2 market parameters. A guessed fee or
            # minimum size would make the paper PnL look more exact than it is.
            raise PaperRejected(f"cannot read live market rules: {type(exc).__name__}") from exc
        if not isinstance(rules, MarketRules):
            raise PaperRejected("invalid live market rules response")
        with self._lock:
            self._rules_cache[key] = (rules, now)
            if len(self._rules_cache) > 64:
                for stale in sorted(self._rules_cache,
                                    key=lambda k: self._rules_cache[k][1])[:32]:
                    del self._rules_cache[stale]
        return rules

    def sell_shares(self, token_id: str, shares: float, *,
                    min_price: float = 0.0,
                    condition_id: str | None = None,
                    window_end: float | None = None,
                    exit_cutoff_seconds: float = 0.0) -> float:
        """Simulate one FAK exit. Returns the shares actually sold, 0.0 if none.

        Exits get their own, later cutoff than entries. The entry cutoff exists
        to stop us BUYING into resolution; applying it to sells would trap the
        position exactly when a stop is supposed to fire. A partial fill is a
        success and is reported as such - the book we sell into is thin
        precisely because the position has already moved against us.
        """
        if not self._exit_lock.acquire(blocking=False):
            self.last_error = "another paper exit is already in flight"
            return 0.0
        try:
            token = str(token_id or "")
            if not token:
                self.last_error = "missing token id"
                return 0.0
            try:
                want = float(shares)
            except (TypeError, ValueError):
                self.last_error = "invalid sell size"
                return 0.0
            if not math.isfinite(want) or want <= 0:
                self.last_error = "invalid sell size"
                return 0.0
            try:
                if window_end is not None:
                    end = float(window_end)
                    if timer.unix() >= end - float(exit_cutoff_seconds):
                        raise PaperRejected("exit cutoff reached")
                context_condition, _u, _d = self._context()
                if condition_id and str(condition_id) != context_condition:
                    raise PaperRejected("condition does not belong to the current paper round")
                condition_id = context_condition
                rules = self._rules(condition_id)
                # An exit is a taker order and eats the same delay an entry
                # does: our own latency plus the venue matching delay. Filling
                # against the book as it looked BEFORE that delay would make a
                # stop appear to escape at a price it never had - the exact
                # measurement this is being built to produce.
                if rules.taker_delay_ms is None:
                    raise PaperRejected(
                        "venue matching delay is unknown; realistic paper exit "
                        "is impossible")
                assumed_latency_ms = self.latency_ms + self._taker_delay_ms(rules)
                if window_end is not None:
                    cutoff = float(window_end) - float(exit_cutoff_seconds)
                    if timer.unix() + assumed_latency_ms / 1000.0 >= cutoff:
                        raise PaperRejected(
                            "not enough time remaining for exit latency before "
                            "the cutoff")
                if assumed_latency_ms:
                    time.sleep(assumed_latency_ms / 1000.0)
                if window_end is not None and timer.unix() >= cutoff:
                    raise PaperRejected("exit latency reached the cutoff")
                # Re-read AFTER the delay: whatever the book did while the
                # order was in flight is what the exit actually gets.
                book = self._book_fetch(token)
                if not isinstance(book, BookSnapshot):
                    raise PaperRejected("invalid public order book response")
                if not math.isfinite(book.received_wall) or book.received_wall <= 0:
                    raise PaperRejected("public order book omitted receipt time")
                if timer.wall() - book.received_wall > self.max_book_age_s:
                    raise PaperRejected("public order book is stale in hand")
                quote = estimate_sell_fak(book, want, min_price, rules)
                with self._lock:
                    held = self.ledger.positions.get(token)
                    if held is None or held.shares + 1e-9 < float(quote.shares):
                        raise PaperRejected("cannot sell more than the position holds")
                    order_id = f"paper-sell-{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
                    inserted = self.ledger.record_fill_durable(
                        order_id, token, shares=float(quote.shares),
                        price=float(quote.average_price), side="SELL",
                        condition_id=condition_id, status="CONFIRMED",
                        source="paper_live_book", fee=float(quote.fee))
                    if not inserted:
                        raise PaperRejected("paper sell was not recorded")
                self.last_error = None
                print(f"[PAPER] FAK sold {float(quote.shares):.6f} sh @ "
                      f"{float(quote.average_price):.6f} | fee "
                      f"${float(quote.fee):.5f} | unfilled "
                      f"{float(quote.unfilled):.6f} | cash "
                      f"${self.cash_balance():.2f}")
                return float(quote.shares)
            except PaperRejected as exc:
                self.last_error = str(exc)
                print(f"[PAPER] sell refused: {exc}")
                return 0.0
        finally:
            self._exit_lock.release()

    def _taker_delay_ms(self, rules: MarketRules) -> float:
        """The matching delay a live taker order would sit through.

        The venue's published delay wins when it is non-zero. When it flags a
        delay (itode) but publishes 0, the 0 is not evidence of instant
        matching, so the configured fallback applies instead.
        """
        published = float(rules.taker_delay_ms or 0.0)
        if rules.taker_delay_flagged and published <= 0.0:
            return self.flagged_delay_ms
        return published

    def place_trade(self, side: str, amount: float,
                    up_token_id: str | None = None,
                    down_token_id: str | None = None,
                    condition_id: str | None = None,
                    window_end: float | None = None,
                    max_price: float | None = None,
                    min_price: float | None = None, *,
                    pre_submit_guard=None, latency_ms: float | None = None,
                    tag: str | None = None, latency_from_pc: int | None = None,
                    arrival_book=None, fak: bool = False, size_at_limit: bool = False) -> bool:
        """Run one simulated FOK; reject concurrent duplicate submissions.

        `fak=True` matches it as fill-and-kill instead: at the match instant
        whatever the book holds at or under the cap is taken, the rest
        cancelled (estimate_fak). The pre-send check on the decision book then
        accepts partial depth, as the live sniper's does.

        `size_at_limit=True` (the sniper, as its live sender): the stake is
        raised so the order as signed - size = stake / limit - reaches the
        venue's share minimum at the limit price.

        `max_price` caps this order only. A caller trading a price band needs
        the walk to stop at the band's top, not at the account-wide ceiling:
        a thin best level would otherwise fill outside the band being traded.
        `min_price` is the same argument for the band's BOTTOM, and may only
        raise the broker's floor. Without it the band was half-enforced: the
        walk stopped at the band's top but could still fill all the way down
        to MIN_BUY_PRICE, which is not the range the experiment measures.
        An optional `pre_submit_guard` must return literal ``True`` after the
        modeled delay and quote, immediately before the durable paper fill.

        `latency_ms` replaces the modelled decision-to-match time for this
        order only - the caller's own measured figure, which must already
        include the venue's taker hold. `latency_from_pc` (perf_counter_ns)
        says when the order left the caller: the delay then runs from that
        instant, so this broker's own checks overlap it rather than add to it
        (a live order on the wire is not waiting for them). `tag` is copied into the audit row so
        one strategy's fills can be told from another's in a shared ledger.
        Leaving both unset keeps the broker's configured behaviour.

        `arrival_book(token) -> (BookSnapshot | None, info)` replaces the
        read-the-book-when-the-timer-wakes rule: it returns the book as it
        stood at the modelled match instant, so a late wake-up can no longer
        decide the fill. None means that book cannot be known: the order is
        refused as "unknown book state", never filled. What the old rule
        (and any alternative books in info["alternatives"]) would have done
        is recorded in last_timing["arrival"].
        """
        if latency_ms is not None:
            latency_ms = float(latency_ms)
            if not math.isfinite(latency_ms) or not 0 <= latency_ms < 60_000:
                return self._reject(side, amount, "invalid per-order latency")
        # perf_counter_ns stamps of this order's path, for callers that
        # measure where their milliseconds go: entered, delay started, met
        # the book. Published only once this order owns the broker.
        timing = {"enter_pc": time.perf_counter_ns()}
        token = up_token_id if str(side).upper() == "UP" else down_token_id
        # One order in flight per token PER SOURCE (tag): the sniper's slot on a
        # token is never taken by another strategy's order (the position
        # manager's), so it can neither refuse nor delay the sniper. Booking
        # stays atomic under self._lock either way.
        with self._lock:
            entry_lock = self._execution_locks.setdefault((str(token), str(tag or "")), threading.Lock())
            if len(self._execution_locks) > 64:
                # One lock per token and source, four per five-minute market: drop
                # the idle ones of tokens that are no longer this round's (they
                # grew without bound - 370 after 600 rotations in tests_stress).
                try:
                    current = {str(t) for t in self._context()[1:] if t}
                except PaperRejected:                   # no current round: keep only busy locks
                    current = set()
                for key, lock in list(self._execution_locks.items()):
                    if key[0] not in current and not lock.locked():
                        del self._execution_locks[key]
        if not entry_lock.acquire(blocking=False):
            return self._reject(side, amount, "another paper order is already in flight on this token")
        try:
            self.last_timing = timing
            return self._place_trade(side, amount, up_token_id, down_token_id,
                                     condition_id, window_end, max_price,
                                     min_price,
                                     pre_submit_guard=pre_submit_guard,
                                     latency_ms=latency_ms, tag=tag, timing=timing,
                                     latency_from_pc=latency_from_pc, arrival_book=arrival_book,
                                     fak=fak, size_at_limit=size_at_limit)
        finally:
            entry_lock.release()

    # -- per-order outcome: see _results in __init__
    def _result(self, name):
        local = self.__dict__.get("_results")
        if local is not None and name in local.__dict__:
            return local.__dict__[name]
        return self.__dict__.get("_latest_" + name)

    def _set_result(self, name, value) -> None:
        local = self.__dict__.get("_results")
        if local is not None:
            local.__dict__[name] = value
        self.__dict__["_latest_" + name] = value

    last_fill = property(lambda self: self._result("last_fill"),
                         lambda self, value: self._set_result("last_fill", value))
    last_error = property(lambda self: self._result("last_error"),
                          lambda self, value: self._set_result("last_error", value))
    last_timing = property(lambda self: self._result("last_timing"),
                           lambda self, value: self._set_result("last_timing", value))

    def _place_trade(self, side: str, amount: float,
                     up_token_id: str | None = None,
                     down_token_id: str | None = None,
                     condition_id: str | None = None,
                     window_end: float | None = None,
                     max_price: float | None = None,
                     min_price: float | None = None, *,
                     pre_submit_guard=None, latency_ms: float | None = None,
                     tag: str | None = None, timing: dict | None = None,
                     latency_from_pc: int | None = None, arrival_book=None,
                     fak: bool = False, size_at_limit: bool = False) -> bool:
        side = str(side or "").upper()
        if side not in ("UP", "DOWN"):
            return self._reject(side, amount, "invalid side")
        token_id = up_token_id if side == "UP" else down_token_id
        if not token_id:
            return self._reject(side, amount, "missing token id")

        try:
            try:
                end = float(window_end)
            except (TypeError, ValueError) as exc:
                raise PaperRejected("missing current round end timestamp") from exc
            now = timer.unix()
            if not end - self.trade_window_seconds <= now < end - self.min_seconds_to_expiry:
                raise PaperRejected("paper order is outside the current round execution interval")
            context_condition, context_up, context_down = self._context()
            if condition_id and str(condition_id) != context_condition:
                raise PaperRejected("condition does not belong to the current paper round")
            condition_id = context_condition
            expected = context_up if side == "UP" else context_down
            if expected and str(token_id) != expected:
                raise PaperRejected("token does not belong to the current paper round")
            rules = self._rules(condition_id)
            if (rules.up_token_id != context_up or rules.down_token_id != context_down):
                raise PaperRejected("Gamma and CLOB disagree on UP/DOWN token mapping")
            if rules.taker_delay_ms is None:
                raise PaperRejected(
                    "venue matching delay is unknown; realistic paper fill is impossible")
            assumed_latency_ms = (self.latency_ms + self._taker_delay_ms(rules)
                                  if latency_ms is None else latency_ms)
            cutoff = end - self.min_seconds_to_expiry
            if timer.unix() + assumed_latency_ms / 1000.0 >= cutoff:
                raise PaperRejected("not enough time remaining for paper latency before cutoff")
            # An order-level cap may tighten the account ceiling but never
            # loosen it: a caller cannot buy above what the account allows.
            cap = self.max_buy_price
            if max_price is not None:
                cap = min(cap, float(max_price))
                if cap <= self.min_buy_price:
                    raise PaperRejected(
                        f"order price cap {cap} is at or below the floor "
                        f"{self.min_buy_price}")
            floor = self.min_buy_price
            if min_price is not None:
                # Raise only, mirroring the cap's tighten-only rule.
                floor = max(floor, Decimal(str(min_price)))

            # THE STAKE IS FIXED HERE, from the book as it is at the decision,
            # because that is when a live order's size is fixed too: live
            # sizes and signs in preflight and cannot revise the amount once
            # the order is in flight. Sizing after the delay let paper choose
            # its stake against the very book it was about to fill on, which
            # is information a real order never has.
            decision_book = self._net_of_own_fills(self._book_fetch(str(token_id)))
            rules = self._rules_matching(condition_id, rules, decision_book)
            self._check_book(decision_book, side)
            decision_bid = decision_book.best_bid
            if decision_bid is None:
                raise PaperRejected("public order book has no bid; spread is unknown")
            decision_spread = decision_book.asks[0][0] - decision_bid
            if decision_spread > Decimal(str(self.max_spread)):
                raise PaperRejected(
                    f"spread {decision_spread:.6f} exceeds paper limit {self.max_spread:.6f}")
            spend = size_to_venue_minimum(amount, decision_book, rules, cap)
            if size_at_limit:
                spend = max(spend, minimum_spend_at_limit(decision_book, rules, cap))
            # The venue refuses a marketable BUY under $1 whatever the share
            # count ("invalid amount for a marketable BUY order ($0.5), min
            # size: $1"). Without this, paper filled 10 shares at 5c - $0.50
            # orders live could never place.
            if spend < MIN_MARKETABLE_BUY_USD:
                raise PaperRejected(
                    f"invalid amount for a marketable BUY order (${spend:.2f}), "
                    f"min size: ${MIN_MARKETABLE_BUY_USD}")
            # Live checks that the signed stake can fill against the decision
            # book before signing. An arrival book must not rescue an order
            # live would never have submitted.
            (estimate_fak if fak else estimate_fok)(decision_book, spend, cap, rules, min_price=floor)
            if spend > Decimal(str(amount)):
                print(
                    f"[PAPER] Sizing ${float(amount):.2f} up to ${float(spend):.2f} "
                    f"to meet the venue minimum"
                )
            if timing is not None:
                timing["delay_pc"] = time.perf_counter_ns()
            wait_s = assumed_latency_ms / 1000.0
            if latency_from_pc is not None:
                wait_s -= (time.perf_counter_ns() - int(latency_from_pc)) / 1e9
            if wait_s > 0:
                time.sleep(wait_s)
            if timer.unix() >= cutoff:
                raise PaperRejected("paper latency reached the round cutoff")
            if arrival_book is None:
                raw_book = self._book_fetch(str(token_id))
                book = self._net_of_own_fills(raw_book)
                # The instant the order met the book: everything after this (the
                # durable ledger write) cannot change the price it got.
                matched_wall = time.time()
                if timing is not None:
                    timing["matched_pc"] = time.perf_counter_ns()
            else:
                # The timer may have woken late: the book is the one that
                # stood at the modelled match instant, not the one now. The old
                # rule's answer is kept beside it, for comparison only.
                if timing is not None:
                    timing["matched_pc"] = time.perf_counter_ns()          # the timer woke
                try:
                    woke = self._book_fetch(str(token_id))
                except PaperRejected:
                    woke = None
                woke = self._quote_outcome(woke, side, spend, cap, rules, floor)
                arrived, info = arrival_book(str(token_id))
                info = dict(info or {})
                info["wake"] = woke
                for name, alt in (info.pop("alternatives", None) or {}).items():
                    info[name] = self._quote_outcome(alt, side, spend, cap, rules, floor)
                if timing is not None:
                    timing["book_ready_pc"] = time.perf_counter_ns()
                    timing["arrival"] = info
                if arrived is None:
                    raise PaperRejected(
                        f"unknown book state at the modelled arrival: {info.get('why')}")
                raw_book = arrived
                book = self._net_of_own_fills(arrived)
                matched_wall = float(info.get("arrival_wall") or time.time())
            if fak and isinstance(book, BookSnapshot) and not book.asks:
                # An IOC that meets an empty book matches nothing - the venue's
                # NO_MATCH, a zero fill (its whole target is carried), exactly
                # as when every ask sits above the cap. It used to be refused
                # as "no asks on the live book", which left the carry untouched.
                raise PaperRejected("FAK no-fill: no asks on the book at the match")
            rules = self._rules_matching(condition_id, rules, book)
            quiet_s, held_s = self._check_book(book, side)
            book_age_s = quiet_s
            # A real executable spread requires both sides of the same,
            # timestamped venue snapshot.
            best_bid = book.best_bid
            if best_bid is None:
                raise PaperRejected("public order book has no bid; spread is unknown")
            spread = book.asks[0][0] - best_bid
            if spread > Decimal(str(self.max_spread)):
                raise PaperRejected(
                    f"spread {spread:.6f} exceeds paper limit {self.max_spread:.6f}")
            # The fixed stake meets the arrival book. Whether the ask rose
            # or fell in between decides only the price paid: a FOK fills at
            # any price up to the cap and refuses past it, which is exactly
            # what the venue does with a signed limit order.
            quote = (estimate_fak if fak else estimate_fok)(book, spend, cap, rules, min_price=floor)
            partial = bool(fak and quote.notional < spend.quantize(MONEY, rounding=ROUND_HALF_UP))
            if timer.unix() >= end - self.min_seconds_to_expiry:
                raise PaperRejected("paper quote reached the round cutoff")
            with self._lock:
                # Another of our orders on this token (the other source's slot -
                # the Position Manager's) may have booked since the quote above:
                # the book is netted again here, where bookings are serialized,
                # and the order re-quoted on what is left. Quoting both on the
                # same displayed depth bought it twice (20 shares off a level
                # showing 10). A no-fill here refuses the order, as live would.
                again = self._net_of_own_fills(raw_book)
                if again.asks != book.asks:
                    book = again
                    if not book.asks:
                        raise PaperRejected(f"{'FAK' if fak else 'FOK'} no-fill: our own order on this "
                                            f"token took the depth first")
                    quote = (estimate_fak if fak else estimate_fok)(book, spend, cap, rules, min_price=floor)
                    partial = bool(fak and quote.notional < spend.quantize(MONEY, rounding=ROUND_HALF_UP))
                if self.cash_balance() + 1e-9 < float(quote.total_cost):
                    raise PaperRejected(
                        f"paper cash ${self.cash_balance():.6f} is below total cost "
                        f"${quote.total_cost:.6f}")
                guard_error = _pre_submit_guard_error(pre_submit_guard)
                if guard_error is not None:
                    raise PaperRejected(guard_error)
                if timer.unix() >= end - self.min_seconds_to_expiry:
                    raise PaperRejected("pre-submit guard reached the round cutoff")
                order_id = f"paper-{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
                if timing is not None:
                    timing["ledger_start_pc"] = time.perf_counter_ns()
                inserted = self.ledger.record_fill_durable(
                    order_id, str(token_id), shares=float(quote.shares),
                    price=float(quote.average_price), side="BUY",
                    condition_id=condition_id, status="CONFIRMED",
                    source="paper_live_book", fee=float(quote.fee))
                if not inserted:
                    raise PaperRejected("paper fill id collision")
                if timing is not None:
                    timing["ledger_done_pc"] = time.perf_counter_ns()
                if self.own_fill_ttl_s:
                    now_mono = time.monotonic()
                    self._own_fills.setdefault(str(token_id), []).extend(
                        (now_mono, p, sh) for p, sh, _n in quote.levels)
                    if len(self._own_fills) > 16:
                        # A token's list is pruned when that token is read again;
                        # an old round's never is. Drop tokens whose newest fill
                        # has expired (they no longer net anything).
                        for tok, fills in list(self._own_fills.items()):
                            if not fills or now_mono - fills[-1][0] >= self.own_fill_ttl_s:
                                del self._own_fills[tok]
                self.filled_orders += 1
                self.last_error = None
                self.last_fill = {
                    "order_id": order_id,
                    "status": "FILLED",
                    "side": side,
                    "token_id": str(token_id),
                    "condition_id": condition_id,
                    "requested_amount": float(quote.notional),
                    **({"partial": True, "stake": float(spend)} if partial else {}),
                    "shares": float(quote.shares),
                    "average_price": float(quote.average_price),
                    "worst_price": float(quote.worst_price),
                    "fee": float(quote.fee),
                    "total_cost": float(quote.total_cost),
                    "fee_rate": float(rules.fee_rate),
                    "fee_exponent": rules.fee_exponent,
                    "fee_source": rules.source,
                    "book_timestamp": book.timestamp,
                    "book_hash": book.book_hash,
                    "book_age_ms": book_age_s * 1000.0,
                    "book_held_ms": held_s * 1000.0,
                    "assumed_latency_ms": assumed_latency_ms,
                    "matched_wall": matched_wall,
                    **({"tag": str(tag)} if tag else {}),
                    "cash_after": self.cash_balance(),
                    "levels": [[float(p), float(sh), float(n)]
                               for p, sh, n in quote.levels],
                    "wall": time.time(),
                }
                self._audit(self.last_fill)
                if timing is not None:
                    timing["audit_done_pc"] = time.perf_counter_ns()
            self._publish_error(None)
            self._schedule_fill_delay_probes(
                order_id=order_id, token_id=str(token_id),
                condition_id=condition_id, side=side, spend=spend, cap=cap,
                floor=floor, rules=rules, fill=quote,
                latency_s=assumed_latency_ms / 1000.0, window_end=end)
            print(
                f"[PAPER] {'FAK filled (partial)' if partial else 'FAK filled' if fak else 'FOK filled'}: "
                f"{side} ${float(quote.notional):.2f} | "
                f"{float(quote.shares):.6f} shares @ {float(quote.average_price):.6f} | "
                f"fee ${float(quote.fee):.5f} | cash ${self.cash_balance():.2f}")
            if self.on_event:
                self.on_event("paper", f"FILL {side} {float(quote.shares):.4f} @ "
                              f"{float(quote.average_price):.4f}", "good")
            return True
        except PaperRejected as exc:
            return self._reject(side, amount, str(exc))
        except Exception as exc:
            # Persistence/invariant failures are not ordinary FOK rejects.
            # Let the exception stop the runner so it cannot continue from
            # accounting state whose durability is uncertain.
            self.last_error = f"paper engine fatal: {type(exc).__name__}: {exc}"
            self._publish_error(self.last_error)
            print(f"[PAPER] FATAL: {self.last_error}")
            raise

    def _schedule_fill_delay_probes(self, *, order_id, token_id, condition_id,
                                    side, spend, cap, floor, rules, fill,
                                    latency_s, window_end) -> None:
        """Record what this same order would have cost after a longer delay.

        Paper matches as soon as its modelled latency elapses. Live waits for
        the network and for the venue's own taker matching delay, which the
        venue flags (itode) but never quantifies. Every archived paper fill
        used exactly PAPER_LATENCY_MS, so re-quoting the same order at later
        offsets is the only way to see how much of paper's edge depends on
        filling faster than live can - and gate2.py (git history; removed
        2026-10-02) put that edge at about a tick, which a slower fill erases.

        Diagnostic only, and fenced accordingly: nothing here places an order
        or touches cash or the ledger, and every error is swallowed. An
        exception escaping into place_trade would reach its fatal handler and
        stop the bot over a measurement.
        """
        if not self.fill_delay_probes:
            return
        try:
            base = {
                "order_id": order_id, "token_id": token_id,
                "condition_id": condition_id, "side": side,
                "stake": float(spend), "cap": float(cap), "floor": float(floor),
                "fill_wall": time.time(), "window_end": float(window_end),
            }
            self._write_delay_row({
                **base, "delay_s": round(float(latency_s), 3), "fillable": True,
                "average_price": float(fill.average_price),
                "total_cost": float(fill.total_cost),
                "reason": "actual paper fill",
            })
            for delay_s in self.fill_delay_probes:
                if delay_s <= latency_s:
                    continue
                self._schedule_probe(
                    delay_s - latency_s,
                    lambda d=delay_s: self._run_delay_probe(
                        base, d, spend, cap, floor, rules))
        except Exception:
            pass

    def _run_delay_probe(self, base, delay_s, spend, cap, floor, rules) -> None:
        """Quote the same FOK against the book as it stands now."""
        try:
            row = {**base, "delay_s": round(float(delay_s), 3),
                   "probed_wall": time.time(), "average_price": None,
                   "total_cost": None}
            # fillable=None marks a probe that measured nothing. It is kept
            # out of the statistics rather than miscounted as a miss.
            if timer.unix() >= base["window_end"] - self.min_seconds_to_expiry:
                # Live could not have filled here either, and the book of a
                # closed round is empty - that is not evidence of a miss.
                row.update(fillable=None, reason="round closed before probe")
                self._write_delay_row(row)
                return
            try:
                book = self._book_fetch(base["token_id"])
            except Exception as exc:
                # A stale or unreadable book says nothing about the price.
                row.update(fillable=None,
                           reason=f"probe error: {type(exc).__name__}: {str(exc)[:120]}")
                self._write_delay_row(row)
                return
            row["book_timestamp"] = getattr(book, "timestamp", None)
            try:
                quote = estimate_fok(book, spend, cap, rules, min_price=floor)
                row.update(fillable=True,
                           average_price=float(quote.average_price),
                           total_cost=float(quote.total_cost), reason="")
            except PaperRejected as exc:
                # A genuine no-fill: at this delay the order would have missed.
                row.update(fillable=False, reason=str(exc)[:160])
            except Exception as exc:
                row.update(fillable=None,
                           reason=f"probe error: {type(exc).__name__}")
            self._write_delay_row(row)
        except Exception:
            pass

    def _write_delay_row(self, row: dict) -> None:
        # Appended without fsync: this is a diagnostic, and on a one-vCPU host
        # a synced write per probe would compete with the trading loop for IO.
        try:
            with self._probe_lock:
                self.delay_journal_path.parent.mkdir(parents=True, exist_ok=True)
                with self.delay_journal_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(row, sort_keys=True, default=str) + "\n")
        except Exception:
            pass

    def _rules_matching(self, condition_id, rules, book):
        """Rules whose tick size agrees with this book, refreshing them once.

        Polymarket moves a market's tick between 0.01 and 0.001 as its prices
        near 0 or 1. Cached rules then disagree with the live book and every
        order was refused - orders the venue would take at the new tick. A
        real disagreement after the refresh still refuses (estimate_fok).
        """
        if not isinstance(book, BookSnapshot) or book.tick_size is None or rules.tick_size == book.tick_size:
            return rules
        with self._lock:
            self._rules_cache.pop(str(condition_id or ""), None)
        return self._rules(condition_id)

    def has_recent_own_fill(self, token_id) -> bool:
        """Would _net_of_own_fills change this token's book right now? A cheap
        check for the sniper's hot path (no lock: a list append and a read of
        its last item are atomic, and a stale answer only costs the slow path)."""
        if not self.own_fill_ttl_s:
            return False
        fills = self._own_fills.get(str(token_id))
        return bool(fills) and time.monotonic() - fills[-1][0] < self.own_fill_ttl_s

    def _net_of_own_fills(self, book):
        """The book as a live account would see it after its own recent buys.

        A paper buy never reaches the venue, so the public book still shows
        the shares paper 'bought'. Live, those shares are gone, and the next
        order a second later walks past them to a worse price or fails its
        FOK. Our recent fills are taken off the levels they hit for
        own_fill_ttl_s - roughly how long a maker takes to re-quote.
        """
        if not self.own_fill_ttl_s or not isinstance(book, BookSnapshot):
            return book
        now = time.monotonic()
        # Read without the broker lock: the sniper calls this on the event loop,
        # and a booking holds that lock while the ledger fsyncs (or waits for a
        # settlement's fsync), which froze the loop for the write. list() of a
        # list is one atomic copy under the GIL; appends happen under the lock.
        live = [f for f in list(self._own_fills.get(book.token_id, ()))
                if now - f[0] < self.own_fill_ttl_s]
        if self._lock.acquire(blocking=False):      # prune only when it costs no wait
            try:
                current = self._own_fills.get(book.token_id)
                if current is not None:
                    kept = [f for f in current if now - f[0] < self.own_fill_ttl_s]
                    if kept:
                        self._own_fills[book.token_id] = kept
                    else:
                        self._own_fills.pop(book.token_id, None)
            finally:
                self._lock.release()
        if not live:
            return book
        used: dict = {}
        for _t, price, shares in live:
            used[price] = used.get(price, ZERO) + shares
        asks = tuple((p, sz - used.get(p, ZERO)) for p, sz in book.asks
                     if sz - used.get(p, ZERO) > ZERO)
        return dataclass_replace(book, asks=asks)

    def _quote_outcome(self, book, side, spend, cap, rules, floor) -> dict:
        """Would this book have filled the order? A counterfactual only:
        {"fill": True, "px": ...} or {"fill": False, "why": ...}; fill None
        when there was no book to ask."""
        if book is None:
            return {"fill": None, "why": "no book"}
        try:
            book = self._net_of_own_fills(book)
            self._check_book(book, side)
            if book.best_bid is None:
                raise PaperRejected("public order book has no bid")
            if book.asks[0][0] - book.best_bid > Decimal(str(self.max_spread)):
                raise PaperRejected("spread exceeds paper limit")
            q = estimate_fok(book, spend, cap, rules, min_price=floor)
        except PaperRejected as exc:
            return {"fill": False, "why": str(exc)[:80],
                    "ask": float(book.asks[0][0]) if getattr(book, "asks", None) else None}
        return {"fill": True, "px": round(float(q.average_price), 4)}

    def _check_book(self, book, side: str) -> tuple:
        """Validate a book and return (quiet_s, held_s). Raises otherwise.

        Both the book an order is SIZED from and the book it FILLS against
        have to pass this. Live gets that for free - every read goes through
        parse_orderbook, which enforces ORDERBOOK_MAX_AGE_SECONDS - so a
        paper path that skipped it on either read would size or fill from
        data live would have refused outright.
        """
        if not isinstance(book, BookSnapshot):
            raise PaperRejected("invalid public order book response")
        if book.timestamp is None:
            raise PaperRejected("public order book omitted exchange timestamp")
        if not math.isfinite(book.received_wall) or book.received_wall <= 0:
            raise PaperRejected("public order book omitted receipt time")
        try:
            book_ts_s, _unit = timer.parse_exchange_ts(book.timestamp)
        except ValueError as exc:
            raise PaperRejected(
                "public order book has an invalid timestamp") from exc
        now_wall = timer.wall()
        # Quiet = how long since the venue last changed the book.
        # Held = how long this copy has been in our hands. The live
        # orderbook parser already splits these; conflating them with
        # ORDERBOOK_MAX_AGE_SECONDS (8s) refused ordinary quiet
        # btc-updown-5m books (33s+ between changes) as "stale".
        quiet_s = now_wall - book_ts_s
        held_s = now_wall - book.received_wall
        if quiet_s < -self.future_tol_s:
            raise PaperRejected(
                f"public order book is future-dated (age={quiet_s:.3f}s)")
        if held_s > self.max_book_age_s:
            raise PaperRejected(
                f"public order book is stale in hand (held={held_s:.3f}s)")
        if quiet_s > self.max_quiet_s:
            raise PaperRejected(
                f"public order book has not changed for {quiet_s:.3f}s")
        if not book.asks:
            raise PaperRejected(
                f"cannot FOK buy {side}: no asks on the live book "
                "(liquidity pulled or one-sided)")
        return quiet_s, held_s

    def _reject(self, side, amount, reason: str) -> bool:
        reason = reason or "paper order rejected"
        with self._lock:
            self.rejected_orders += 1
            self.last_error = reason
            self.last_fill = None
            self._audit({
                "status": "REJECTED", "side": side,
                "requested_amount": amount, "reason": reason,
                "wall": time.time(), "cash_after": self.cash_balance(),
            })
        self._publish_error(reason)
        print(f"[PAPER] FOK rejected: {reason}")
        if self.on_event:
            self.on_event("paper", f"REJECT {reason}", "warn")
        return False

    @staticmethod
    def _publish_error(reason: str | None) -> None:
        # main_bot historically reads this module attribute after a failure.
        # Updating it preserves that error display while the bound live order
        # function itself remains replaced by PaperBroker.place_trade.
        import polymarket_trade
        polymarket_trade.last_order_error = reason

    def _audit(self, row: dict) -> None:
        try:
            self.audit_path.parent.mkdir(parents=True, exist_ok=True)
            with self.audit_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
        except Exception as exc:
            raise RuntimeError(f"paper audit log could not be saved: {exc}") from exc

    # --------------------------------------------------------- reporting
    def summary(self, mark=None) -> dict:
        ledger = self.ledger.summary(mark=mark)
        cash = self.cash_balance()
        unreal = ledger.get("unrealized_mark_to_bid")
        if ledger.get("open_positions", 0) == 0:
            equity = cash
        elif unreal is None or ledger.get("unmarkable_positions", 0):
            equity = None
        else:
            equity = cash + ledger["pending_cost"] + unreal
        return {
            "mode": "PAPER",
            "starting_balance": self.start_balance,
            "cash": cash,
            "equity": equity,
            "total_pnl": None if equity is None else equity - self.start_balance,
            "filled_orders_session": self.filled_orders,
            "rejected_orders_session": self.rejected_orders,
            "ledger_path": str(Path(self.ledger.path).resolve()),
            "audit_path": str(self.audit_path.resolve()),
            **ledger,
        }


def install_paper_execution(main_bot, broker: PaperBroker, *, log_path=None) -> None:
    """Bind the bot to paper execution and poison the live-client gateway."""
    import polymarket_trade

    # Defense in depth: even an overlooked direct live-function call cannot
    # obtain a client, sign, cancel, query a wallet, or post an order.
    def blocked_live_client(*_args, **_kwargs):
        raise RuntimeError("live CLOB client is disabled by --paper")

    polymarket_trade.disable_live_execution()
    polymarket_trade._get_client = blocked_live_client
    main_bot.execution_mode = "PAPER"
    main_bot._paper_broker = broker
    main_bot.get_balance_allowance = broker.get_balance_allowance
    main_bot.cancel_all_open_orders = broker.cancel_all_open_orders
    main_bot.place_trade = broker.place_trade
    if log_path is not None:
        main_bot.TRADE_LOG = Path(log_path)
