"""Pure hybrid Kelly sizing and optional conservative order-risk limits.

Confidence remains neutral until an independently validated historical estimate
exists. A recent trading win rate is not a predictive-accuracy calibration.
These functions perform no account, network, file or clock access.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, localcontext
import math


def _decimal(value, name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"{name} must be a finite number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        constraint = "positive" if positive else "nonnegative"
        raise ValueError(f"{name} must be finite and {constraint}")
    return result


def _number(value, name: str, *, positive: bool = False) -> float:
    decimal = _decimal(value, name, positive=positive)
    result = float(decimal)
    if not math.isfinite(result) or (decimal > 0 and result <= 0):
        raise ValueError(f"{name} must be a finite representable number")
    return result


@dataclass(frozen=True)
class HybridPolicy:
    """Volatility baseline is basis points per sqrt(second).

    A zero risk_fraction or max_order_usd disables that individual hard cap.
    Both caps default to disabled so an operator can choose limits later.
    """

    vol_baseline_bp: float = 1.0
    drawdown_stop: float = 0.20
    risk_fraction: float = 0.0
    max_order_usd: float = 0.0

    def __post_init__(self) -> None:
        for name in ("vol_baseline_bp", "drawdown_stop", "risk_fraction", "max_order_usd"):
            value = _number(getattr(self, name), name,
                            positive=name in ("vol_baseline_bp", "drawdown_stop"))
            if name in ("drawdown_stop", "risk_fraction") and value > 1:
                raise ValueError(f"{name} must be at most 1")
            object.__setattr__(self, name, value)


def at_drawdown_stop(policy: HybridPolicy, *, equity, high_water) -> bool:
    """sizing_factors' drawdown stop alone (no volatility needed): the loss from
    the peak at or beyond drawdown_stop of it - the same exact Decimal test."""
    if not isinstance(policy, HybridPolicy):
        raise ValueError("policy must be a HybridPolicy")
    equity = _number(equity, "equity")
    high_water = _number(high_water, "high_water", positive=True)
    with localcontext() as context:
        context.prec = 50
        peak = Decimal(str(high_water))
        return peak - Decimal(str(equity)) >= peak * Decimal(str(policy.drawdown_stop))


def sizing_factors(policy: HybridPolicy, *, sigma, equity, high_water,
                   base_fraction=0.25) -> dict:
    """Return target-allocation factors, not a scale for incremental orders.

    Pass effective_fraction into portfolio Kelly *before* subtracting existing
    holdings. Multiplying an incremental order afterward allows repeat signals
    to keep rebuilding the larger, unscaled target.
    """
    if not isinstance(policy, HybridPolicy):
        raise ValueError("policy must be a HybridPolicy")
    sigma = _number(sigma, "sigma", positive=True)
    equity = _number(equity, "equity")
    high_water = _number(high_water, "high_water", positive=True)
    base_fraction = _number(base_fraction, "base_fraction")
    if high_water < equity:
        raise ValueError("high_water must be at least equity")
    if base_fraction > 1:
        raise ValueError("base_fraction must be at most 1")

    volatility_scale = min(1.0, (policy.vol_baseline_bp / 1e4) / sigma)
    # Compare monetary values before converting to a floating-point scale.
    # For example, 123.45 -> 98.76 is exactly 20% drawdown, but float division
    # leaves a tiny positive fraction. Minimum-entry sizing must not turn that
    # residue into a full venue-minimum order at the drawdown stop.
    with localcontext() as context:
        context.prec = 50
        peak = Decimal(str(high_water))
        loss = peak - Decimal(str(equity))
        stop_loss = peak * Decimal(str(policy.drawdown_stop))
        drawdown = float(loss / peak)
        drawdown_scale = 0.0 if loss >= stop_loss else float(Decimal(1) - loss / stop_loss)
    budgets = []
    if policy.risk_fraction > 0:
        budgets.append(equity * policy.risk_fraction)
    if policy.max_order_usd > 0:
        budgets.append(policy.max_order_usd)
    return {
        "volatility_scale": volatility_scale,
        "drawdown": drawdown,
        "drawdown_scale": drawdown_scale,
        "confidence_scale": 1.0,
        "confidence_status": "unvalidated_neutral",
        "effective_fraction": base_fraction * volatility_scale * drawdown_scale,
        "risk_budget_usd": min(budgets) if budgets else None,
        "hard_cap_enabled": bool(budgets),
    }


def risk_total(spend, fee_rate, quoted_fee=0) -> Decimal:
    """Conservative all-in cost, using the larger quoted or rate-bound fee.

    The exchange fee curve is bounded above by spend * fee_rate. Upward Decimal
    rounding keeps this a bound even for unusually long numeric inputs.
    """
    spend = _decimal(spend, "spend")
    fee_rate = _decimal(fee_rate, "fee_rate")
    quoted_fee = _decimal(quoted_fee, "quoted_fee")
    with localcontext() as context:
        context.prec = 50
        context.rounding = ROUND_CEILING
        return spend + max(quoted_fee, spend * fee_rate)


def shares_within_budget(budget, price_cap, fee_rate) -> int:
    """Whole shares whose cap-price cost and conservative fee fit the budget."""
    budget = _decimal(budget, "budget")
    price_cap = _decimal(price_cap, "price_cap", positive=True)
    fee_rate = _decimal(fee_rate, "fee_rate")
    with localcontext() as context:
        context.prec = 50
        context.rounding = ROUND_CEILING
        per_share = price_cap * (Decimal(1) + fee_rate)
        context.rounding = ROUND_FLOOR
        return int((budget / per_share).to_integral_value(rounding=ROUND_FLOOR))


def enforce_order_budget(spend, fee_rate, budget, quoted_fee=0) -> Decimal:
    """Return all-in cost, or refuse an invalid/oversized final order."""
    budget = _decimal(budget, "budget")
    total = risk_total(spend, fee_rate, quoted_fee)
    if total > budget:
        raise ValueError(f"order risk {total} exceeds hybrid budget {budget}")
    return total


def minimum_entry_shares(price_cap, venue_min_shares=5, min_order_usd=1) -> int:
    """Smallest whole-share order accepted by the current whole-cent sender.

    Fine price ticks can require a share multiple: five shares at .501 cost
    2.505, so this sender needs ten shares for an exact whole-cent stake.
    """
    price = _decimal(price_cap, "price_cap", positive=True)
    venue_min = _decimal(venue_min_shares, "venue_min_shares", positive=True)
    minimum_usd = _decimal(min_order_usd, "min_order_usd", positive=True)
    if price >= 1:
        raise ValueError("price_cap must be below 1")
    with localcontext() as context:
        context.prec = 50
        context.rounding = ROUND_CEILING
        minimum = max(int(venue_min.to_integral_value(rounding=ROUND_CEILING)),
                      int((minimum_usd / price).to_integral_value(rounding=ROUND_CEILING)))
        _numerator, multiple = (price * 100).as_integer_ratio()
        return ((minimum + multiple - 1) // multiple) * multiple


def cent_compatible_shares(target, price_cap) -> int:
    """Round a target down so its signed limit stake is whole cents."""
    target = _decimal(target, "target")
    price = _decimal(price_cap, "price_cap", positive=True)
    if price >= 1:
        raise ValueError("price_cap must be below 1")
    _numerator, multiple = (price * 100).as_integer_ratio()
    shares = int(target.to_integral_value(rounding=ROUND_FLOOR))
    return shares // multiple * multiple
