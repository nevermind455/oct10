"""Validated public CLOB order-book reads used by the decision path."""
import math
import os
import time
from decimal import ROUND_UP, Decimal, InvalidOperation

import requests

import config
import http_pool
import timer
from book_snapshot import RestBookSnapshot

BOOK_URL = "https://clob.polymarket.com/book"

# Opt-in per-read timestamp logging. Off by default so a healthy run
# stays quiet; set ORDERBOOK_TS_LOG=1 to see every accepted book.
_TS_LOG = (os.environ.get("ORDERBOOK_TS_LOG", "") or "").strip().lower() in (
    "1", "true", "yes", "on")


def _number(name, value, *, minimum=None, maximum=None, inclusive_min=False):
    try:
        if isinstance(value, bool):
            raise ValueError("boolean numeric value")
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    if minimum is not None:
        valid = parsed >= minimum if inclusive_min else parsed > minimum
        if not valid:
            op = ">=" if inclusive_min else ">"
            raise ValueError(f"{name} must be {op} {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} must be <= {maximum}")
    return parsed


def _level(raw, side):
    if not isinstance(raw, dict):
        raise ValueError(f"invalid {side} level")
    try:
        price = Decimal(str(raw.get("price")))
        size = Decimal(str(raw.get("size")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid {side} level") from exc
    if not price.is_finite() or not size.is_finite():
        raise ValueError(f"non-finite {side} level")
    if not Decimal(0) < price < Decimal(1) or size <= 0:
        raise ValueError(f"out-of-range {side} level")
    return price, size


def _levels(raw_levels, side):
    """Aggregate duplicate price rows so malformed depth cannot be double-counted."""
    if not isinstance(raw_levels, (list, tuple)):
        raise ValueError(f"invalid {side} level collection")
    aggregated: dict[Decimal, Decimal] = {}
    for raw in raw_levels or ():
        price, size = _level(raw, side)
        aggregated[price] = aggregated.get(price, Decimal(0)) + size
    reverse = side == "bid"
    return [
        {"price": str(price), "size": str(aggregated[price])}
        for price in sorted(aggregated, reverse=reverse)
    ]


# The last completed freshness decision, kept so a caller or the dashboard can
# show why a book was taken or refused without re-deriving it.
LAST_TIMESTAMP_REPORT: dict | None = None


def _format_report(r: dict) -> str:
    return (f"exchange_ts={r['exchange_ts_raw']!r} ({r['unit']}, "
            f"{r['exchange_ts_s']:.3f}s) "
            f"local_ts={r['local_ts_s']:.3f}s "
            f"clock_offset={r['clock_offset_s']:+.3f}s "
            f"quiet={r['quiet_s']:.3f}s (max {r['max_quiet_s']:.1f}s) "
            f"held={r['held_s']:.3f}s (max {r['max_age_s']:.1f}s) "
            f"future_tolerance={r['future_tol_s']:.1f}s "
            f"source={r['source']}")


def _timestamp_report(data, *, now, received_at, max_age_s, max_quiet_s,
                      future_tol_s, source):
    """Everything the freshness decision rests on, in one place.

    Two independent quantities, which the old check conflated into one:

      quiet - how long since the VENUE last changed the book. On a quiet
              market this grows without bound and says nothing about whether
              our copy is current.
      held  - how long since WE received this response. This is the real
              staleness of the data in our hands.
    """
    raw = data.get("timestamp")
    try:
        ts_s, unit = timer.parse_exchange_ts(raw)
    except ValueError as exc:
        raise ValueError(
            f"CLOB book has no valid exchange timestamp: {exc} "
            f"(got {raw!r}, local_ts={now:.3f}s)") from exc
    return {
        "exchange_ts_raw": raw,
        "exchange_ts_s": ts_s,
        "unit": unit,
        "local_ts_s": now,
        "clock_offset_s": timer.clock_offset(),
        "received_at_s": received_at,
        "quiet_s": now - ts_s,
        "held_s": now - received_at,
        "max_age_s": max_age_s,
        "max_quiet_s": max_quiet_s,
        "future_tol_s": future_tol_s,
        "source": source,
    }


def parse_orderbook(data, token_id, *, max_age_s=None, now=None,
                    received_at=None, max_quiet_s=None, future_tol_s=None,
                    source="clob-rest"):
    global LAST_TIMESTAMP_REPORT
    if not isinstance(data, dict):
        raise ValueError("CLOB book response is not an object")
    expected = str(token_id)
    asset = data.get("asset_id")
    if asset is None or str(asset) != expected:
        raise ValueError("CLOB book asset_id does not match the requested token")
    wall = timer.wall() if now is None else _number(
        "now", now, minimum=0, inclusive_min=True)
    limit = _number(
        "max_age_s",
        config.ORDERBOOK_MAX_AGE_SECONDS if max_age_s is None else max_age_s,
        minimum=0,
    )
    quiet_limit = _number(
        "max_quiet_s",
        config.ORDERBOOK_MAX_QUIET_SECONDS if max_quiet_s is None else max_quiet_s,
        minimum=0,
    )
    future_tol = _number(
        "future_tol_s",
        (config.ORDERBOOK_FUTURE_TOLERANCE_SECONDS
         if future_tol_s is None else future_tol_s),
        minimum=0, inclusive_min=True,
    )
    held_at = wall if received_at is None else _number(
        "received_at", received_at, minimum=0, inclusive_min=True)
    report = _timestamp_report(
        data, now=wall, received_at=held_at, max_age_s=limit,
        max_quiet_s=quiet_limit, future_tol_s=future_tol, source=source)
    LAST_TIMESTAMP_REPORT = report

    # A book dated ahead of us is a clock or unit fault, never a real book.
    if report["quiet_s"] < -future_tol:
        raise ValueError(
            "CLOB book is future-dated - check the clock and the timestamp "
            f"unit: {_format_report(report)}")
    # Held time is the freshness of the copy we are about to trade on.
    if report["held_s"] > limit:
        raise ValueError(
            f"CLOB book response is stale in hand: {_format_report(report)}")
    # Quiet time only catches a venue serving a frozen or cached book. It is
    # deliberately generous: an unchanged book is still the current book.
    if report["quiet_s"] > quiet_limit:
        raise ValueError(
            "CLOB book has not changed for longer than the venue-frozen "
            f"bound: {_format_report(report)}")
    if _TS_LOG:
        print(f"[BOOK-TS] accepted {_format_report(report)}", flush=True)

    bids = _levels(data.get("bids"), "bid")
    asks = _levels(data.get("asks"), "ask")
    if not bids and not asks:
        raise ValueError("CLOB book is empty")
    if bids and asks and float(bids[0]["price"]) >= float(asks[0]["price"]):
        raise ValueError("CLOB book is crossed or locked")
    return bids, asks


def parse_orderbook_snapshot(data, token_id, *, received_at=None, received_mono=None):
    """Keep REST metadata for the hub without changing the legacy levels API.

    Receipt time is captured before JSON parsing by the HTTP caller. The
    venue timestamp comes only from the validated response, never our clock.
    """
    bids, asks = parse_orderbook(data, token_id, received_at=received_at)
    timestamp = int(round(timer.parse_exchange_ts(data["timestamp"])[0] * 1000))
    receipt = time.monotonic() if received_mono is None else _number(
        "received_mono", received_mono, minimum=0, inclusive_min=True)
    if receipt > time.monotonic():
        raise ValueError("CLOB book receipt time is in the future")
    tick_raw = data.get("tick_size", data.get("minimum_tick_size"))
    tick = None if tick_raw in (None, "") else _number("tick_size", tick_raw, minimum=0)
    if tick is not None and tick >= 1:
        raise ValueError("tick_size must be below 1")
    return RestBookSnapshot(
        token_id=str(token_id), bids=bids, asks=asks, exchange_ts_ms=timestamp,
        received_mono=receipt, tick_size=tick,
        book_hash=str(data["hash"]) if data.get("hash") is not None else None,
    )


def _get_orderbook(token_id, timeout=8.0, *, with_metadata=False):
    token = str(token_id or "")
    if not token.isdigit() or int(token) <= 0:
        raise ValueError("invalid CLOB token id")
    timeout = _number("timeout", timeout, minimum=0)
    last_error = None
    for attempt in range(2):
        try:
            resp = http_pool.get(BOOK_URL, params={"token_id": token}, timeout=timeout)
            # Stamp arrival before any parsing, so `held` measures the age of
            # the data we hold rather than however long decoding took.
            received_at = timer.wall()
            received_mono = time.monotonic()
            resp.raise_for_status()
            data = resp.json()
            if with_metadata:
                return parse_orderbook_snapshot(data, token, received_at=received_at,
                                                received_mono=received_mono)
            return parse_orderbook(data, token, received_at=received_at)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
        except requests.HTTPError as exc:
            last_error = exc
            status = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
            if status != 429 and not 500 <= status <= 599:
                raise
        if attempt == 0:
            # The read runs in a worker thread, so a short bounded backoff does
            # not stall the asyncio loop.  Final freshness/expiry checks in the
            # caller still fail closed if the retry takes too long.
            time.sleep(0.1)
    if last_error is not None:
        raise last_error
    raise RuntimeError("CLOB book request failed without an error")


def get_orderbook(token_id, timeout=8.0):
    """Legacy levels-only public read used by liquidity callers."""
    return _get_orderbook(token_id, timeout)


def get_orderbook_snapshot(token_id, timeout=8.0):
    """Public read for book recovery, preserving validated venue metadata."""
    return _get_orderbook(token_id, timeout, with_metadata=True)


def liquidity_signal(bids, asks):
    def volume(levels):
        total, count = 0.0, 0
        for level in levels or ():
            try:
                size = float(level.get("size"))
            except (AttributeError, TypeError, ValueError):
                continue
            if math.isfinite(size) and size > 0:
                candidate = total + size
                if not math.isfinite(candidate):
                    return 0.0, 0
                total = candidate
                count += 1
        return total, count

    bid_volume, bid_count = volume(bids)
    ask_volume, ask_count = volume(asks)
    # A one-sided book is not a depth comparison.  Near expiry the winning
    # token keeps only bids and the losing token only asks, so an empty side
    # would otherwise cast a confident vote for the token that cannot be
    # bought at all.  Abstain and let the other signals decide.
    if not bid_count or not ask_count:
        return None
    return "UP" if bid_volume >= ask_volume else "DOWN"


def validate_buy_liquidity(token_id, amount, max_price, max_spread, min_price=0.0,
                           book=None):
    """Fail closed before signing when the selected token is unfillable/unsafe.

    `book` is an already-fetched (bids, asks) for THIS token. The caller has
    usually just read it for the final-validation liquidity signal, and a
    second fetch of the same leg costs a full venue round trip (measured
    371-763ms) for a book that cannot have moved in the microseconds since.
    The caller owns freshness: it must not pass a book read for a different
    token, nor one that predates blocking work. None keeps the old behaviour
    of fetching here.
    """
    amount = _number("amount", amount, minimum=0)
    max_price = _number("max_price", max_price, minimum=0, maximum=1)
    min_price = _number(
        "min_price", min_price, minimum=0, maximum=1, inclusive_min=True)
    max_spread = _number("max_spread", max_spread, minimum=0, maximum=1)
    if min_price >= max_price:
        raise ValueError("min_price must be below max_price")
    bids, asks = get_orderbook(token_id) if book is None else book
    # Cached reads must satisfy the same safeguards as public REST reads.
    # Sorting also prevents an unsorted caller's first row hiding the real
    # best price. No second network request is needed for validation.
    bids, asks = _levels(bids, "bid"), _levels(asks, "ask")
    if not asks:
        raise ValueError("selected token has no asks")
    best_ask = float(asks[0]["price"])
    if best_ask > max_price:
        raise ValueError(f"best ask {best_ask} exceeds MAX_BUY_PRICE {max_price}")
    if best_ask < min_price:
        raise ValueError(f"best ask {best_ask} is below MIN_BUY_PRICE {min_price}")
    if not bids:
        raise ValueError("selected token has no bids; spread cannot be validated")
    spread = best_ask - float(bids[0]["price"])
    if spread <= 0:
        raise ValueError("selected token book is crossed or locked")
    if spread > max_spread:
        raise ValueError(f"spread {spread:.6f} exceeds limit {max_spread:.6f}")
    available = sum(
        float(level["price"]) * float(level["size"])
        for level in asks if float(level["price"]) <= max_price
    )
    if not math.isfinite(available):
        raise ValueError("FOK preflight depth is non-finite")
    if available + 1e-9 < amount:
        raise ValueError(
            f"FOK preflight: only ${available:.6f} available at or below {max_price}")
    return bids, asks


def venue_minimum_stake(wanted: Decimal, asks, minimum: Decimal, cap: Decimal,
                        *, price_headroom=None) -> Decimal:
    """Raise a dollar stake just enough to buy the venue's minimum shares.

    Best-ask * minimum under-sizes whenever the top of book alone cannot
    fill the venue's minimum share count - a thin top level makes a $2.50
    FOK walk into 0.51 and land at 4.96 shares. This walks the same
    executable asks the fill will actually consume. Shared by paper_trade.py
    (paper FOK sizing) and polymarket_trade.py (live order sizing) so a
    correction to this walk only ever needs to land once, instead of the two
    hand-written copies this replaced silently drifting apart.

    `asks` is an iterable of (price, size) Decimal pairs, best (lowest ask)
    first. A level whose price or size is not strictly positive is skipped
    rather than treated as the end of the book, unless its price exceeds
    `cap` - past that nothing on the book is buyable at all, so the walk
    stops there exactly as the caller's own FOK/FAK order would. Returns
    `wanted` unchanged whenever it cannot verify a raise is needed (no
    minimum, no asks, an invalid cap, or the ladder cannot fill the minimum
    even at the cap), so an unreadable market can never silently enlarge an
    order.
    """
    if minimum <= 0 or cap <= 0:
        return wanted
    asks = list(asks)
    if not asks:
        return wanted
    best_price, _best_size = asks[0]
    if best_price <= 0 or best_price > cap:
        return wanted
    remaining = minimum
    notional = Decimal("0")
    for price, size in asks:
        if price > cap:
            break
        if price <= 0 or size <= 0:
            continue
        take = remaining if remaining <= size else size
        notional += take * price
        remaining -= take
        if remaining <= 0:
            break
    if remaining > 0:
        return wanted
    # Leave room for the ask to rise before the order matches. Without it a
    # stake that buys exactly the minimum is rejected by a single tick, and
    # the tick goes up precisely when the trade was right - see
    # config.VENUE_MIN_PRICE_HEADROOM. Never plan past the cap: nothing above
    # it is buyable, so headroom there would only enlarge the order.
    headroom = Decimal(str(config.VENUE_MIN_PRICE_HEADROOM
                           if price_headroom is None else price_headroom))
    average = notional / minimum
    room = min(headroom, max(Decimal("0"), cap - average))
    required = (notional + minimum * room).quantize(
        Decimal("0.01"), rounding=ROUND_UP)
    return wanted if wanted >= required else required
