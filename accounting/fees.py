"""Polymarket taker fees.

    fee = shares * theta * price * (1 - price)          (takers only)
    makers pay 0

For a FIXED notional the shares cancel out:

    shares = notional / p
    fee    = notional * theta * (1 - p)

which is why a CHEAP fill costs this bot more in fees than an expensive one.

The bot sends FOK market orders. A market order is always a taker, so every
fill it produces pays. There is no maker path in this build to be free.

Rates below are the July 2026 schedule. **A fee schedule is a snapshot** —
crypto went from free to 0.072 in January 2026 and to 0.07 in July 2026. Set
POLY_FEE_THETA to override without a code change, and prefer the live value
when the venue supplies one.
"""
from __future__ import annotations

import os
import math
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

FEE_PRECISION = Decimal("0.00001")

# Category -> theta. Only `crypto` and the zero-rated categories are used by
# this bot; the rest are here so a wrong default is visible rather than silent.
CATEGORY_THETA = {
    "crypto": 0.07,
    # Polymarket's current public schedule is 0.03 for fee-enabled sports.
    # Keeping the older 0.05 value overstates paper costs and understates PnL.
    "sports": 0.03,
    "culture": 0.05,
    "weather": 0.05,
    "politics": 0.04,
    "finance": 0.04,
    "tech": 0.04,
    "mentions": 0.04,
    "geopolitics": 0.0,
    "economics": 0.05,
    "other": 0.05,
    "general": 0.05,
}

DEFAULT_CATEGORY = "crypto"          # BTC 5m up/down
# An unknown fee is not a free fee. Falling back to 0 silently overstates PnL,
# which is exactly the bug this module exists to stop.
FALLBACK_THETA = CATEGORY_THETA["crypto"]


def theta(category: str | None = None, live: float | None = None) -> float:
    """Resolve the fee rate. Priority: live venue value > env > table."""
    if live is not None:
        try:
            v = float(live)
            # A V2 user-trade event may report legacy fee_rate_bps=0 even on
            # a fee-enabled market.  Zero is therefore not authoritative for
            # this crypto bot; the public CLOB fee details/category schedule
            # are the safe fallback.
            if math.isfinite(v) and 0 < v <= 1:
                return v
        except (TypeError, ValueError):
            v = 0.0
    env = os.environ.get("POLY_FEE_THETA")
    if env not in (None, ""):
        try:
            value = float(env)
            if math.isfinite(value) and 0 < value <= 1:
                return value
        except ValueError:
            value = 0.0
    name = category if isinstance(category, str) else DEFAULT_CATEGORY
    return CATEGORY_THETA.get((name or DEFAULT_CATEGORY).lower(), FALLBACK_THETA)


def taker_fee(shares: float, price: float, th: float | None = None,
              category: str | None = None, exponent: int = 1) -> float:
    """Fee on a taker fill of `shares` at `price`."""
    if shares is None or price is None:
        return 0.0
    th = theta(category, live=th)
    try:
        sh = Decimal(str(shares))
        p = Decimal(str(price))
        rate = Decimal(str(th))
        exp = int(exponent)
    except (InvalidOperation, TypeError, ValueError):
        return 0.0
    if (not all(value.is_finite() for value in (sh, p, rate))
            or sh <= 0 or rate < 0 or not 0 <= p <= 1
            or not 1 <= exp <= 8):
        return 0.0
    value = sh * rate * (p * (Decimal(1) - p)) ** exp
    return float(value.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP))


def fee_for_notional(notional: float, price: float, th: float | None = None,
                     category: str | None = None) -> float:
    """Fee when spending a fixed dollar amount. shares cancel: N*theta*(1-p)."""
    if not notional or price in (None, 0):
        return 0.0
    th = theta(category, live=th)
    try:
        n = Decimal(str(notional))
        p = Decimal(str(price))
        rate = Decimal(str(th))
    except (InvalidOperation, TypeError, ValueError):
        return 0.0
    if (not all(value.is_finite() for value in (n, p, rate))
            or n <= 0 or rate < 0 or not 0 < p <= 1):
        return 0.0
    value = n * rate * (Decimal(1) - p)
    return float(value.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP))


def maker_fee(*_a, **_kw) -> float:
    return 0.0


def breakeven_win_rate(price: float, th: float | None = None,
                       category: str | None = None) -> float:
    """Win rate needed to break even after fees at this fill price.

        cost  = notional + fee = N(1 + theta(1-p))
        shares= N/p, payout = shares on a win
        w*N/p = N(1 + theta(1-p))  ->  w = p + theta*p*(1-p)
    """
    th = theta(category, live=th)
    p = max(0.0, min(1.0, float(price)))
    return p + th * p * (1.0 - p)


def fee_drag_bps(price: float, th: float | None = None,
                 category: str | None = None) -> float:
    """Fee as basis points of notional."""
    th = theta(category, live=th)
    p = max(0.0, min(1.0, float(price)))
    return th * (1.0 - p) * 10_000.0


def live_theta_from_market(market: dict | None) -> float | None:
    """Pull the fee rate out of a venue market object if it carries one.

    Field naming is not pinned across SDK versions, so probe the plausible
    names and return None rather than guessing zero.
    """
    if not isinstance(market, dict):
        return None
    for key in ("taker_fee_rate", "takerFeeRate", "fee_rate", "feeRate", "theta"):
        if key in market and market[key] is not None:
            try:
                value = float(market[key])
                return value if math.isfinite(value) and 0 < value <= 1 else None
            except (TypeError, ValueError):
                continue
    for key in ("taker_base_fee", "fee_rate_bps", "takerFeeBps"):
        if key in market and market[key] is not None:
            try:
                value = float(market[key]) / 10_000.0
                return value if math.isfinite(value) and 0 < value <= 1 else None
            except (TypeError, ValueError):
                continue
    return None
