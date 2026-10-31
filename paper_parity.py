"""Public, cached live execution references for strict paper comparison.

No wallet, SDK client, orders or account writes. References describe live state;
they do not establish that a simulated fill would execute at the venue.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import math
import os
import time

REFERENCE_EVERY_S = 5.0
REFERENCE_MAX_AGE_S = 15.0
READINESS_FIELDS = ("collateral", "round_cash", "private_fills", "rules", "clock", "venue_guard")
CONFIG_FIELDS = (
    "primary_feed", "extra_feeds", "k_bp", "window_ms", "shares", "slip", "order_type",
    "order_sizing", "max_target_shares", "cooldown_ms", "min_left_s", "max_fills_round",
    "max_side_shares", "fair", "fair_margin", "fair_vol_s", "kelly", "kelly_fraction",
    "hybrid_sizing", "kelly_min_entry", "min_entry_max_drawdown", "wide_cap_c", "wide_cap_live",
    "wide_cap_live_stop_usd", "wide_cap_dir", "dir_add", "dir_add_live", "ptb_max_gap_s", "ptb_margin_bp", "ptb_min_checks",
    "venue_min_shares", "backlog_gate", "feed_coalesce",
    "stale_ms", "backlog_fresh_ms", "hybrid_dd_pause_s", "coinbase_freshness",
    "coinbase_stale_ms", "coinbase_unknown_ms", "coinbase_backlog_max", "coinbase_conns",
    "coinbase_silent_s", "binance_freshness", "binance_stale_ms", "binance_unknown_ms",
    "binance_backlog_max", "binance_silent_s",
)


def comparison_config(settings, price_band, broker=None):
    # Round-trip tuples to lists so a loaded reference compares identically.
    config = {key: getattr(settings, key, None) for key in CONFIG_FIELDS}
    config["hybrid_policy"] = asdict(settings.hybrid_policy)
    config["price_band"] = list(price_band)
    config["execution_rules"] = {name: float(getattr(broker, name, 0.0) or 0.0)
                                 for name in ("max_spread", "trade_window_seconds", "min_seconds_to_expiry")}
    return json.loads(json.dumps(config, allow_nan=False))


def number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("live reference contains an invalid number")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError("live reference contains an invalid number") from exc
    if not math.isfinite(result):
        raise ValueError("live reference contains an invalid number")
    return result


def market_key(hub):
    values = [getattr(hub, name, None) for name in ("condition_id", "up_token", "down_token", "window_end")]
    if not all(values):
        return None
    try:
        return [str(value) for value in values[:3]] + [number(values[3])]
    except ValueError:
        return None


def execution_state(live, hub, *, now=None, mono=None):
    """Read the same cached prerequisites as live preparation, without advancing guards."""
    wall = time.time() if now is None else now
    monotonic = time.monotonic() if mono is None else mono
    market = market_key(hub)
    ready = dict.fromkeys(READINESS_FIELDS, False)
    expires = wall + REFERENCE_MAX_AGE_S
    try:
        age = monotonic - number(live.balance_at)
        max_age = number(live.balance_max_age_s)
        ready["collateral"] = (0 <= age <= max_age and
                               0 <= number(live.reserved) <= number(live.balance))
        expires = min(expires, wall + max_age - age)
    except (AttributeError, ValueError):
        pass
    if market is not None:
        condition, _up, _down, end = market
        for name, check in (
            ("round_cash", lambda: live._cash_read_for(min(end - 300, wall))),
            ("private_fills", lambda: live.user_ready is None or live.user_ready(condition)),
            ("rules", lambda: live.rules_ready == tuple(market[:3]) and live.shim.cached_rules(condition) is not None),
        ):
            try:
                ready[name] = bool(check())
            except Exception:  # cache/readiness failure must block comparison, not stop live
                pass
    try:
        clock = live.sender.clock
        clock.now_s()  # read-only trust/age validation, never a refresh
        age = clock.mono() - number(clock.at)
        max_age = number(clock.max_age_s)
        ready["clock"] = 0 <= age <= max_age
        expires = min(expires, wall + max_age - age)
    except Exception:
        pass
    try:
        # guard.check() may resume live trading, so this observer reads its state only.
        ready["venue_guard"] = live.sender.guard.state in ("HEALTHY", "DEGRADED", "RECOVERED")
    except AttributeError:
        pass
    return {"market": market, "readiness": ready, "valid_until": expires}


def read_reference(path, *, max_bytes=4_000_000):
    """Newest complete reference in a bounded tail; never use a partial record."""
    with open(path, "rb") as stream:
        size = stream.seek(0, os.SEEK_END)
        start = max(0, size - max_bytes)
        stream.seek(start)
        lines = stream.read(max_bytes).split(b"\n")
    lines.pop()  # the final unterminated row is not an observation yet
    if start and lines:
        lines.pop(0)  # the first row may start before our read
    for line in reversed(lines):
        if b'"execution_reference"' not in line:
            continue
        try:
            row = json.loads(line)
        except ValueError as exc:
            raise ValueError("malformed live execution reference") from exc
        if isinstance(row, dict) and row.get("result") == "execution_reference":
            return row
    raise ValueError("no live execution reference; update/start live in this folder")


def validate_reference(row, config, *, market=None, now=None):
    now = time.time() if now is None else now
    if not isinstance(row, dict) or type(row.get("version")) is not int or row["version"] != 1 or row.get("live") is not True:
        raise ValueError("invalid live execution reference")
    age = now - number(row.get("wall"))
    if not 0 <= age <= REFERENCE_MAX_AGE_S:
        raise ValueError("live execution reference stale or future-dated")
    if row.get("config") != config:
        raise ValueError("paper/live strategy settings differ; restart both with matching settings")
    observed_market = row.get("market")
    if (not isinstance(observed_market, list) or len(observed_market) != 4 or
            not all(isinstance(x, str) and x for x in observed_market[:3])):
        raise ValueError("live market reference unavailable")
    if number(observed_market[3]) <= now:
        raise ValueError("live market reference expired")
    if market is not None and observed_market != market:
        raise ValueError("paper/live market differs; waiting for this round's live reference")
    ready = row.get("readiness")
    if not isinstance(ready, dict) or any(ready.get(name) is not True for name in READINESS_FIELDS):
        missing = [name for name in READINESS_FIELDS if not isinstance(ready, dict) or ready.get(name) is not True]
        raise ValueError("live execution not ready: " + ", ".join(missing))
    deadline = number(row.get("valid_until"))
    if not now <= deadline <= row["wall"] + REFERENCE_MAX_AGE_S:
        raise ValueError("live readiness reference expired")
    number(row.get("cash"))
    cap = number(row.get("max_order_shares"))
    minimum = number(row.get("venue_min_shares"))
    if cap <= 0 or minimum < 0 or minimum > 5 or minimum != int(minimum):
        raise ValueError("invalid live share limits")
    if not isinstance(row.get("pauses"), list) or not all(isinstance(x, str) for x in row["pauses"]):
        raise ValueError("live pause state unavailable")
    if row["pauses"]:
        raise ValueError("live paused: " + "; ".join(row["pauses"]))
    if not isinstance(row.get("wide_active"), bool) or not isinstance(row.get("wide_ready"), bool):
        raise ValueError("live test state unavailable")
    if config.get("wide_cap_live") and not row["wide_ready"]:
        raise ValueError("live test state not initialized yet")
    hybrid = row.get("hybrid")
    if config.get("hybrid_sizing"):
        if not isinstance(hybrid, dict):
            raise ValueError("live drawdown reference unavailable")
        equity, high = number(hybrid.get("equity")), number(hybrid.get("high_water"))
        if equity < 0 or high < equity or not 0 <= now - number(hybrid.get("wall")) <= 30:
            raise ValueError("live drawdown reference stale or invalid")
    return row
