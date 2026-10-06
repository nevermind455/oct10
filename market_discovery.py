"""Exact discovery for the current BTC five-minute Up/Down market."""
import json
import math
import re
import time
from datetime import UTC, datetime
from urllib.parse import quote

import requests

import http_pool
import timer

GAMMA = "https://gamma-api.polymarket.com/events/slug"
WINDOW_SECONDS = 300
CONDITION_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
TOKEN_RE = re.compile(r"^[0-9]+$")
EXPECTED_CRYPTO_ASSET = "btc"
EXPECTED_CRYPTO_DURATION = "5m"
EXPECTED_TWAP_LOOKBACK_SECONDS = 60


def _current_5m_window_start_unix(now: float | None = None):
    current = int(timer.unix() if now is None else now)
    return current - current % WINDOW_SECONDS


def _fetch_slug(slug):
    """Fetch one exact event by its canonical path endpoint."""
    url = f"{GAMMA}/{quote(str(slug), safe='-')}"
    last_exc = None
    for attempt in range(2):
        resp = None
        try:
            resp = http_pool.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, dict) and data.get("slug") == slug else None
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
        except requests.HTTPError as exc:
            last_exc = exc
            status = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
            if status != 429 and not 500 <= status <= 599:
                print(f"[MARKET] API error (slug={slug}, status={status or 'unknown'}): HTTPError")
                return None
        except Exception as exc:
            print(
                f"[MARKET] API error (slug={slug}): "
                f"{type(exc).__name__}: {str(exc)[:120]}"
            )
            return None
        if attempt == 0:
            retry_after = None
            headers = getattr(resp, "headers", None)
            if headers:
                try:
                    retry_after = float(headers.get("Retry-After"))
                except (TypeError, ValueError):
                    retry_after = None
            delay = retry_after if retry_after is not None and math.isfinite(retry_after) else 0.25
            time.sleep(min(1.0, max(0.05, delay)))
    detail = type(last_exc).__name__ if last_exc is not None else "unknown"
    print(f"[MARKET] API error (slug={slug}): {detail}")
    return None


def _as_list(value):
    """Gamma sometimes serialises list-valued fields as JSON strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _truthy(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _parse_iso(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    except (OverflowError, TypeError, ValueError):
        return None


def _tradeable(market):
    """Require the explicit current Gamma flags used for order eligibility."""
    return (
        not _truthy(market.get("closed"))
        and _truthy(market.get("active"))
        and _truthy(market.get("acceptingOrders"))
        and _truthy(market.get("enableOrderBook"))
    )


def _valid_crypto_market_config(market):
    """Require the venue to declare the exact oracle model the bot trades.

    ``ChainlinkStrike`` deliberately consumes the BTC/USD 60-second TWAP.
    Gamma configures this per market, so discovery must fail closed if an
    active event rotates to spot, another asset/window, or another lookback.
    Otherwise valid-looking token IDs would be traded with signals for a
    different resolution contract.
    """
    config = market.get("cryptoMarketConfig")
    if not isinstance(config, dict):
        return False
    asset = config.get("asset")
    duration = config.get("duration")
    lookback = config.get("twapLookbackSeconds")
    return (
        isinstance(asset, str)
        and asset.strip().lower() == EXPECTED_CRYPTO_ASSET
        and isinstance(duration, str)
        and duration.strip().lower() == EXPECTED_CRYPTO_DURATION
        and config.get("twapEnabled") is True
        and type(lookback) in (int, float)
        and math.isfinite(float(lookback))
        and float(lookback) == EXPECTED_TWAP_LOOKBACK_SECONDS
    )


def _valid_window(market, expected_window):
    start = _parse_iso(market.get("eventStartTime") or market.get("startTime"))
    end = _parse_iso(market.get("endDate") or market.get("endDateIso"))
    if start is None or end is None:
        return False
    return (abs(start - expected_window) <= 1.0
            and abs(end - (expected_window + WINDOW_SECONDS)) <= 1.0)


def _parse_event(event, expected_window=None):
    if not isinstance(event, dict):
        return None
    expected = (_current_5m_window_start_unix() if expected_window is None
                else int(expected_window))
    expected_slug = f"btc-updown-5m-{expected}"
    if event.get("slug") != expected_slug:
        return None
    if _truthy(event.get("closed")) or not _truthy(event.get("active")):
        return None

    matches = []
    for market in _as_list(event.get("markets")):
        if (not isinstance(market, dict) or not _tradeable(market)
                or not _valid_crypto_market_config(market)
                or not _valid_window(market, expected)):
            continue
        token_ids = _as_list(market.get("clobTokenIds"))
        outcomes = _as_list(market.get("outcomes"))
        if len(token_ids) != 2 or len(outcomes) != 2:
            continue
        labels = [str(label).strip().lower() for label in outcomes]
        if sorted(labels) != ["down", "up"]:
            continue
        tokens = [str(token).strip() for token in token_ids]
        if (tokens[0] == tokens[1] or any(not TOKEN_RE.fullmatch(token) for token in tokens)
                or any(int(token) <= 0 for token in tokens)):
            continue
        condition = str(market.get("conditionId") or market.get("condition_id") or "")
        market_id = str(market.get("id") or "")
        if (not CONDITION_RE.fullmatch(condition) or not market_id.isdigit()
                or int(market_id) <= 0):
            continue
        labelled = dict(zip(labels, tokens))
        matches.append({
            "up_token_id": labelled["up"],
            "down_token_id": labelled["down"],
            "orderbook_token_id": labelled["up"],
            "condition_id": condition,
            "market_id": market_id,
            "slug": expected_slug,
            "window_start": expected,
            "window_end": expected + WINDOW_SECONDS,
        })
    # More than one eligible binary market is ambiguous; never pick by order.
    return matches[0] if len(matches) == 1 else None


def get_btc_5m_tokens(window_start_unix=None):
    current = _current_5m_window_start_unix()
    try:
        window = current if window_start_unix is None else int(window_start_unix)
    except (OverflowError, TypeError, ValueError):
        return None
    if window % WINDOW_SECONDS:
        return None
    # This function serves only the active round and one-round prewarming.
    # Even if Gamma's flags lag after expiry, a caller cannot retrieve or trade
    # the previous round through this gateway.
    if window < current or window > current + WINDOW_SECONDS:
        return None
    slug = f"btc-updown-5m-{window}"
    # There is deliberately no previous-round fallback.
    return _parse_event(_fetch_slug(slug), expected_window=window)


def get_tokens_for_current_round(window_start_unix=None):
    return get_btc_5m_tokens(window_start_unix)
