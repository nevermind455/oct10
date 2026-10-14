"""Polymarket CLOB V2 live execution with strict FOK acknowledgement rules."""
import math
import os
import pathlib
import re
import threading
import uuid
import time
from decimal import Decimal, DecimalException, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from urllib.parse import urlsplit

import config  # noqa: F401 — loads .env first
import orderbook

from py_clob_client_v2 import (
    AssetType,
    BalanceAllowanceParams,
    ClobClient,
    MarketOrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
)

_client = None
_live_disabled = False
_execution_lock = threading.Lock()
# Exits take their own lock for the same reason PAPER does: an exit blocking an
# entry costs the whole trade cycle, and the two are not duplicates of one
# another.
_exit_lock = threading.Lock()
_state_lock = threading.RLock()
_client_lock = threading.RLock()
_order_observer = None
_journal_fault = None
_market_fee_by_token: dict[str, tuple[float, int]] = {}
_MAX_FEE_CACHE_TOKENS = 4096
_ambiguous_condition = None
_ambiguous_until = 0.0
_ambiguous_tokens: set[str] = set()
_ambiguous_all_tokens = False
_ambiguous_exit_tokens: set[str] = set()
last_order_error = None
last_order_status = None
last_order_receipt = None
PUSD_BASE_UNITS = Decimal("1000000")
CONDITION_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
# Diagnostics that must survive stdout buffering.
_DIAG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "live_diagnostics.log")

HEX_SECRET_RE = re.compile(r"0x[0-9a-fA-F]{64}")
FEE_PRECISION = Decimal("0.00001")
_API_LOCK_WAIT_SECONDS = 10.0
_CLOB_HTTP_TIMEOUT_SECONDS = 20.0
# httpx defaults keepalive_expiry to 5s. Orders go out every
# TRADE_INTERVAL_SECONDS (12s by default), so the pooled connection was always
# expired by the time the next order needed it and every live order paid a
# fresh TCP+TLS handshake. Measured against the live CLOB on a warm link that
# is +86ms per order; on a cold link it is seconds. Held across a whole round
# instead. The server may still close an idle connection - httpx reopens it,
# which is exactly the old behaviour, so a long expiry costs nothing.
_CLOB_KEEPALIVE_SECONDS = 300.0
_CREDENTIAL_DERIVE_ATTEMPTS = 3
_CREDENTIAL_DERIVE_BACKOFF_SECONDS = (1.0, 2.0)
_clob_http_timeout_applied = False


def _safe_error(exc) -> str:
    text = str(exc or "")
    for name in (
        "POLY_PRIVATE_KEY", "PRIVATE_KEY", "POLY_API_KEY", "CLOB_API_KEY",
        "POLY_SECRET", "CLOB_SECRET", "POLY_PASSPHRASE",
        "CLOB_PASSPHRASE", "CLOB_PASS_PHRASE",
    ):
        secret = os.environ.get(name)
        if secret:
            text = text.replace(secret, "<redacted>")
    return HEX_SECRET_RE.sub("0x<redacted>", text)[:300]


def _pusd_amount(value) -> float | None:
    """Convert six-decimal base units; malformed values stay unknown."""
    try:
        out = Decimal(str(value)) / PUSD_BASE_UNITS
    except (DecimalException, TypeError, ValueError, OverflowError):
        return None
    if not out.is_finite() or out < 0:
        return None
    try:
        amount = float(out)
    except (TypeError, ValueError, OverflowError):
        return None
    return amount if math.isfinite(amount) else None


def disable_live_execution() -> None:
    """Irreversible process-local paper-mode firewall."""
    global _live_disabled, _client
    with _execution_lock:
        # An exit already owns its client while it signs/posts. Wait for that
        # action to finish before declaring the paper firewall installed. Lock
        # order agrees with entries (execution -> client) and exits (exit -> client).
        with _exit_lock:
            with _client_lock:
                _live_disabled = True
                _client = None


def live_execution_disabled() -> bool:
    return _live_disabled


def set_order_observer(callback) -> None:
    """Install the live accounting journal callback (never used in paper)."""
    global _order_observer
    with _state_lock:
        _order_observer = callback


def market_fee_rate(token_id) -> float | None:
    with _state_lock:
        params = _market_fee_by_token.get(str(token_id))
    return None if params is None else params[0]


def market_fee_parameters(token_id) -> dict | None:
    with _state_lock:
        params = _market_fee_by_token.get(str(token_id))
    return None if params is None else {"rate": params[0], "exponent": params[1]}


def reset_client():
    global _client
    with _execution_lock:
        with _client_lock:
            _client = None


def _validate_live_host(host: str) -> str:
    value = str(host or "").rstrip("/")
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in ("", "/")):
        raise RuntimeError("authenticated CLOB host must be a clean HTTPS origin")
    return value


def _normalize_private_key(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.strip().strip('"').strip("'").replace(" ", "").replace("\n", "")
    if not key:
        return None
    if not key.startswith("0x"):
        key = "0x" + key
    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", key):
        raise RuntimeError("live private key must be exactly 32 bytes of hexadecimal")
    return key


def _apply_clob_http_timeout() -> None:
    """The SDK's shared httpx client defaults to a 5s read timeout.

    L2 create/derive is two authenticated round trips. A single slow CLOB
    read used to abort live startup as "credentials could not be derived".
    """
    global _clob_http_timeout_applied
    if _clob_http_timeout_applied:
        return
    try:
        import httpx
        import py_clob_client_v2.http_helpers.helpers as helpers
    except ImportError:
        return
    previous = getattr(helpers, "_http_client", None)
    helpers._http_client = httpx.Client(
        http2=True, timeout=httpx.Timeout(_CLOB_HTTP_TIMEOUT_SECONDS),
        limits=httpx.Limits(keepalive_expiry=_CLOB_KEEPALIVE_SECONDS))
    if previous is not None:
        try:
            previous.close()
        except Exception:
            pass
    _clob_http_timeout_applied = True


def _is_transient_clob_error(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and status < 500:
        return False
    text = str(exc or "").lower()
    name = type(exc).__name__.lower()
    return (
        "timeout" in text or "timed out" in text or "timeout" in name
        or "connect" in name or "connection" in text or "network" in text
        or "request exception" in text
    )


def _install_api_creds(client) -> None:
    last_error = None
    for attempt in range(_CREDENTIAL_DERIVE_ATTEMPTS):
        try:
            creds = client.create_or_derive_api_key()
            if creds is None:
                raise RuntimeError("credential derivation returned no credentials")
            client.set_api_creds(creds)
            return
        except Exception as exc:
            last_error = exc
            if (attempt + 1 >= _CREDENTIAL_DERIVE_ATTEMPTS
                    or not _is_transient_clob_error(exc)):
                break
            delay = _CREDENTIAL_DERIVE_BACKOFF_SECONDS[
                min(attempt, len(_CREDENTIAL_DERIVE_BACKOFF_SECONDS) - 1)]
            print(
                f"[LIVE] L2 credential derivation failed "
                f"({type(exc).__name__}: {_safe_error(exc)}); "
                f"retry {attempt + 2}/{_CREDENTIAL_DERIVE_ATTEMPTS} in {delay:.0f}s"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"could not derive L2 API credentials: {_safe_error(last_error)}"
    ) from None


def _get_client() -> ClobClient:
    # Entries, exits and startup/read-only probes use different execution
    # locks. Credential derivation and replacing the SDK HTTP pool must happen
    # once; a competing initialization used to close another caller's pool.
    with _client_lock:
        return _get_client_unlocked()


def _get_client_unlocked() -> ClobClient:
    global _client
    if _live_disabled:
        raise RuntimeError("live CLOB client is disabled by paper mode")
    if _client is not None:
        return _client

    key = _normalize_private_key(os.environ.get("POLY_PRIVATE_KEY") or
                                 os.environ.get("PRIVATE_KEY"))
    if not key:
        raise RuntimeError("live trading requires POLY_PRIVATE_KEY or PRIVATE_KEY")
    funder = config.POLY_FUNDER
    if funder and not re.fullmatch(r"0x[0-9a-fA-F]{40}", str(funder)):
        raise RuntimeError("POLY_FUNDER is not a valid EVM address")

    client_kwargs = {
        "host": _validate_live_host(config.CLOB_HOST),
        "chain_id": config.CHAIN_ID,
        "key": key,
        "use_server_time": True,
    }
    # Preserve the SDK's EOA defaults instead of overriding them with None.
    if config.POLY_SIGNATURE_TYPE is not None:
        client_kwargs["signature_type"] = config.POLY_SIGNATURE_TYPE
    if funder:
        client_kwargs["funder"] = funder
    _apply_clob_http_timeout()
    client = ClobClient(**client_kwargs)
    _install_api_creds(client)

    _client = client
    print("[LIVE] CLOB V2 client initialized (wallet identifiers redacted).")
    return _client


def _balance_from_response(raw) -> dict | None:
    if not isinstance(raw, dict):
        return None
    balance = _pusd_amount(raw.get("balance"))
    allowance_raw = raw.get("allowance")
    if allowance_raw is not None:
        allowance = _pusd_amount(allowance_raw)
    elif isinstance(raw.get("allowances"), dict):
        vals = [_pusd_amount(v) for v in raw["allowances"].values()]
        # A maximum hides a zero approval on one required exchange.  The
        # conservative minimum is the only safe aggregate.
        allowance = min(vals) if vals and all(v is not None for v in vals) else None
    else:
        allowance = None
    if balance is None or allowance is None:
        return None
    return {"balance": balance, "allowance": allowance}


def _read_balance(client) -> dict | None:
    raw = client.get_balance_allowance(
        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    return _balance_from_response(raw)


def get_balance_allowance() -> dict | None:
    global last_order_error
    # Non-blocking: a queued wait here used to acquire in the gap between
    # MULTI legs and make the second FOK fail as "already in flight".
    if not _execution_lock.acquire(blocking=False):
        return None
    try:
        result = _read_balance(_get_client())
        if result is None:
            raise RuntimeError("malformed balance/allowance response")
        return result
    except Exception as exc:
        last_order_error = _safe_error(exc)
        print(f"[LIVE] Balance/allowance check failed: {last_order_error}")
        return None
    finally:
        _execution_lock.release()


def cancel_all_open_orders() -> bool:
    global last_order_error
    if not _execution_lock.acquire(blocking=False):
        last_order_error = "cannot cancel while another live API action is in flight"
        return False
    try:
        return _cancel_all_open_orders()
    finally:
        _execution_lock.release()


def _cancel_all_open_orders() -> bool:
    global last_order_error
    try:
        resp = _get_client().cancel_all()
        if not isinstance(resp, dict):
            raise RuntimeError("malformed cancel-all response")
        canceled = resp.get("canceled", resp.get("cancelled"))
        not_canceled = resp.get("not_canceled", resp.get("notCancelled"))
        if not isinstance(canceled, list) or not isinstance(not_canceled, dict):
            raise RuntimeError("cancel-all response is missing result fields")
        if not_canceled:
            raise RuntimeError(f"venue could not cancel {len(not_canceled)} order(s)")
        last_order_error = None
        print(f"[LIVE] Cancelled {len(canceled)} open order(s).")
        return True
    except Exception as exc:
        last_order_error = _safe_error(exc)
        print(f"[LIVE] Cancel all error: {last_order_error}")
        return False


def _is_no_match(err: str) -> bool:
    e = (err or "").lower()
    return ("no match" in e or "couldn't be fully filled" in e
            or "fully filled or killed" in e or "fok orders are filled or killed" in e or "status unmatched" in e
            or "fok_order_not_filled" in e
            # FAK with nothing at or under its limit (documented 400 reply)
            or "no orders found to match" in e or "partially filled or killed" in e)


def _explicit_no_match_exception(exc: Exception) -> bool:
    # An exception string is not venue rejection evidence: a transport timeout
    # can contain an old response body or diagnostic text. Retry only a
    # documented client-error response from the venue.
    return getattr(exc, "status_code", None) in (400, 422) and _is_no_match(str(exc))


_FAILED_ORDER_STATUSES = {
    "failed", "rejected", "cancelled", "canceled", "unmatched", "error",
}


def _accepted_order_response(resp) -> tuple[str | None, str | None, str | None]:
    """Accept only a documented FOK ``matched`` acknowledgement.

    This is placement evidence, not final position evidence.  The ledger still
    waits for the user channel/REST trade to reach terminal ``CONFIRMED``.
    """
    if not isinstance(resp, dict):
        return None, None, "unaccepted non-object order response"
    oid = resp.get("orderID") or resp.get("order_id")
    status = str(resp.get("status") or "").strip()
    status_l = status.lower()
    error = resp.get("errorMsg") or resp.get("error")
    if resp.get("success") is False or resp.get("ok") is False:
        return None, status or None, str(error or resp.get("message") or "venue rejected order")
    if error not in (None, ""):
        return None, status or None, str(error)
    if status_l in _FAILED_ORDER_STATUSES:
        return None, status or None, f"FOK order status {status or 'missing'}"
    if status_l != "matched":
        return None, status or None, f"FOK response is not matched (status={status or 'missing'})"
    if resp.get("success") is not True and resp.get("ok") is not True:
        return None, status, "FOK response has no explicit success flag"
    if not oid:
        return None, status, "matched FOK response has no order id"
    if not _valid_response_id(oid):
        return None, status, "matched FOK response has an invalid order id"
    trades = resp.get("tradeIDs") or resp.get("trade_ids") or []
    transactions = resp.get("transactionsHashes") or resp.get("transaction_hashes") or []
    if not isinstance(trades, list) or not isinstance(transactions, list):
        return None, status, "matched FOK response has malformed trade evidence"
    if not trades and not transactions:
        return None, status, "matched FOK response has no trade ids or transaction hashes"
    if (any(not _valid_response_id(value) for value in trades + transactions)
            or len(set(map(str, trades))) != len(trades)
            or len(set(map(str, transactions))) != len(transactions)):
        return None, status, "matched FOK response has invalid trade evidence"
    # Current CLOB FOK matched replies often omit makingAmount/takingAmount
    # even when they include orderID + tradeIDs. Those amounts are fill size,
    # not placement evidence; inventing them would be a guess, and treating
    # the POST as unknown would retry or block a round that already matched.
    return str(oid), status, None


def _accepted_pending_response(resp) -> tuple[str | None, str | None, str | None]:
    """Recognize a successful placement that is not yet validated as matched.

    ``delayed`` is an official successful order status. It must never be
    counted as a fill, but its order ID must be journaled so a later CONFIRMED
    user-stream trade can enter accounting. Other explicit-success responses
    with an order ID are retained as unverified rather than mislabeled failed.
    """
    if not isinstance(resp, dict):
        return None, None, None
    if resp.get("success") is not True and resp.get("ok") is not True:
        return None, None, None
    error = resp.get("errorMsg") or resp.get("error")
    if error not in (None, ""):
        return None, None, None
    oid = resp.get("orderID") or resp.get("order_id")
    status = str(resp.get("status") or "").strip()
    if not _valid_response_id(oid) or status.lower() in _FAILED_ORDER_STATUSES:
        return None, status or None, None
    return str(oid), status or None, (
        "DELAYED_PENDING_OUTCOME" if status.lower() == "delayed" else
        "UNEXPECTED_LIVE_PENDING" if status.lower() == "live" else
        "MATCHED_UNVERIFIED_PENDING_CONFIRMATION" if status.lower() == "matched" else
        "ACCEPTED_UNVERIFIED_PENDING"
    )


def _definitive_rejection(resp) -> bool:
    if not isinstance(resp, dict):
        return False
    status = str(resp.get("status") or "").strip().lower()
    # Conflicting venue evidence is an unknown outcome. Releasing exposure or
    # retrying a no-match message alongside trade evidence can buy twice.
    if (resp.get("success") is True or resp.get("ok") is True
            or status in ("matched", "delayed", "live")
            or any(resp.get(k) for k in ("tradeIDs", "trade_ids", "transactionsHashes", "transaction_hashes"))):
        return False
    return (resp.get("success") is False or resp.get("ok") is False
            or status in _FAILED_ORDER_STATUSES)


def _valid_response_id(value) -> bool:
    text = str(value or "")
    return (0 < len(text) <= 256 and text.isprintable()
            and not any(char.isspace() for char in text))


def _valid_execution_amounts(resp: dict) -> bool:
    try:
        making = Decimal(str(resp["makingAmount"]))
        taking = Decimal(str(resp["takingAmount"]))
    except (KeyError, InvalidOperation, TypeError, ValueError):
        return False
    return (making.is_finite() and taking.is_finite()
            and making > 0 and taking > 0
            and making == making.to_integral_value()
            and taking == taking.to_integral_value())


def _build_receipt(resp: dict, oid: str, status: str | None, *, condition_id,
                   token_id, window_end, amount, estimated_fee,
                   fee_rate, fee_exponent, validation: str) -> dict:
    making = taking = None
    if _valid_execution_amounts(resp):
        making = str(resp["makingAmount"])
        taking = str(resp["takingAmount"])
    return {
        "order_id": oid,
        "status": status,
        "validation": validation,
        "trade_ids": list(resp.get("tradeIDs") or resp.get("trade_ids") or []),
        "transaction_hashes": list(
            resp.get("transactionsHashes") or resp.get("transaction_hashes") or []),
        "making_amount_base_units": making,
        "taking_amount_base_units": taking,
        "condition_id": str(condition_id),
        "token_id": token_id,
        "window_end": window_end,
        "requested_notional": amount,
        "estimated_fee": float(estimated_fee),
        "fee_rate": float(fee_rate),
        "fee_exponent": int(fee_exponent),
    }


def _size_to_venue_minimum(amount, asks, rules, cap) -> float:
    """Raise a dollar stake just enough to buy the venue's minimum shares.

    The ask-ladder walk itself lives in orderbook.venue_minimum_stake,
    shared with paper_trade.size_to_venue_minimum's paper sizing, so a
    correction only ever needs to land once. Returns the stake unchanged
    whenever it cannot verify a raise is needed, so an unreadable market can
    never silently enlarge a live order.
    """
    try:
        wanted = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        minimum = Decimal(str(rules["minimum"]))
        ceiling = Decimal(str(cap))
        normalized = [(Decimal(str(level["price"])), Decimal(str(level["size"])))
                     for level in asks]
        return float(orderbook.venue_minimum_stake(wanted, normalized, minimum, ceiling))
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return float(amount)


def _validate_market_mapping(info, condition_id, up_token_id, down_token_id):
    if not isinstance(info, dict):
        raise RuntimeError("CLOB market-info response is not an object")
    if not _valid_token(up_token_id) or not _valid_token(down_token_id):
        raise RuntimeError("invalid expected UP/DOWN token ids")
    returned = str(info.get("condition_id") or info.get("conditionId") or "")
    # The current CLOB V2 response is keyed by condition in the URL but does
    # not echo it.  If a future response does echo it, verify it.
    if returned and returned != str(condition_id):
        raise RuntimeError("CLOB market-info condition id mismatch")
    rows = info.get("t")
    if not isinstance(rows, list) or len(rows) != 2:
        raise RuntimeError("CLOB market is not binary")
    labelled = {}
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("malformed CLOB token mapping")
        raw_token = row.get("t")
        if not _valid_token(raw_token):
            raise RuntimeError("malformed CLOB token mapping")
        labelled[str(row.get("o") or "").strip().lower()] = str(raw_token)
    if (set(labelled) != {"up", "down"} or not all(_valid_token(v) for v in labelled.values())
            or labelled["up"] == labelled["down"]):
        raise RuntimeError("CLOB outcomes are not exactly UP and DOWN")
    if (labelled["up"] != str(up_token_id)
            or labelled["down"] != str(down_token_id)):
        raise RuntimeError("Gamma and CLOB disagree on UP/DOWN token mapping")
    try:
        minimum = Decimal(str(info.get("mos")))
        tick = Decimal(str(info.get("mts")))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError("CLOB market has invalid minimum-size/tick rules") from exc
    if (not minimum.is_finite() or minimum <= 0 or not tick.is_finite()
            or tick <= 0 or tick >= 1):
        raise RuntimeError("CLOB market has invalid minimum-size/tick rules")
    fd = info.get("fd")
    if not isinstance(fd, dict):
        raise RuntimeError("CLOB market omitted fee details")
    try:
        fee_rate = Decimal(str(fd.get("r")))
        exponent_raw = fd.get("e")
        if isinstance(exponent_raw, bool):
            raise ValueError
        exponent_decimal = Decimal(str(exponent_raw))
        if (not exponent_decimal.is_finite()
                or exponent_decimal != exponent_decimal.to_integral_value()):
            raise ValueError
        fee_exponent = int(exponent_decimal)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RuntimeError("CLOB market has invalid fee details") from exc
    if (not fee_rate.is_finite() or not 0 < fee_rate <= 1
            or not 1 <= fee_exponent <= 8):
        raise RuntimeError("CLOB market has invalid fee details")
    if fd.get("to") is not True:
        raise RuntimeError("CLOB market fee details are not explicitly taker-only")
    neg_risk = info.get("nr", False)
    taker_delay = info.get("itode", False)
    if not isinstance(neg_risk, bool) or not isinstance(taker_delay, bool):
        raise RuntimeError("CLOB market has invalid boolean execution rules")
    if taker_delay and config.ASSUMED_MATCH_DELAY_SECONDS <= 0:
        # This endpoint exposes only that a delay is enabled, not its duration.
        # Submitting near a hard expiry cutoff without the duration would guess.
        # Set ASSUMED_MATCH_DELAY_SECONDS to trade anyway, outside a window of
        # that many seconds from the round end; the caller enforces it, because
        # only the caller knows how much of the round is left.
        raise RuntimeError("CLOB market has an undisclosed matching delay")
    fee_params = (float(fee_rate), fee_exponent)
    with _state_lock:
        _market_fee_by_token[labelled["up"]] = fee_params
        _market_fee_by_token[labelled["down"]] = fee_params
        while len(_market_fee_by_token) > _MAX_FEE_CACHE_TOKENS:
            _market_fee_by_token.pop(next(iter(_market_fee_by_token)))
    return {
        "taker_delay": bool(taker_delay),
        "minimum": minimum,
        "tick": tick,
        "neg_risk": neg_risk,
        "fee_rate": fee_rate,
        "fee_exponent": fee_exponent,
    }


def _valid_token(token) -> bool:
    try:
        text = str(token or "")
    except (TypeError, ValueError, OverflowError):
        return False
    if not text or not text.isascii() or not text.isdecimal():
        return False
    # Venue token IDs are uint256 decimal values. Strip harmless leading zeros
    # before bounding/parsing so even enormous strings cannot hit int()'s limit.
    digits = text.lstrip("0")
    return bool(digits) and len(digits) <= 78 and int(digits) <= (1 << 256) - 1


def _round_limit(max_price, tick: Decimal) -> Decimal:
    cap = min(Decimal(str(max_price)), Decimal(1) - tick)
    rounded = (cap / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    if rounded <= 0 or rounded >= 1:
        raise RuntimeError("MAX_BUY_PRICE cannot be represented at the venue tick size")
    return rounded


def _round_floor(min_price, tick: Decimal) -> Decimal:
    floor = max(Decimal(str(min_price)), tick)
    rounded = (floor / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    if rounded <= 0 or rounded >= 1:
        raise RuntimeError("MIN_BUY_PRICE cannot be represented at the venue tick size")
    return rounded


def _quote_fok(asks, amount: Decimal, limit: Decimal,
               fee_rate: Decimal, fee_exponent: int) -> tuple[Decimal, Decimal]:
    """Return (shares, fee) for the same best-to-worst FOK walk as paper."""
    remaining = amount
    shares = Decimal(0)
    fee = Decimal(0)
    for level in asks:
        price = Decimal(str(level["price"]))
        available_shares = Decimal(str(level["size"]))
        if price > limit:
            break
        spend = min(remaining, price * available_shares)
        if spend <= 0:
            continue
        level_shares = spend / price
        shares += level_shares
        fee += level_shares * fee_rate * (price * (Decimal(1) - price)) ** fee_exponent
        remaining -= spend
        if remaining <= Decimal("0.0000005"):
            remaining = Decimal(0)
            break
    if remaining > 0:
        raise RuntimeError(
            f"FOK preflight has only ${amount - remaining:.6f} executable liquidity")
    return shares, fee.quantize(FEE_PRECISION, rounding=ROUND_HALF_UP)


def _validate_round_end(window_end) -> float:
    try:
        end = float(window_end)
    except (TypeError, ValueError, OverflowError) as exc:
        raise RuntimeError("missing current round end timestamp") from exc
    now = time.time()
    if (not math.isfinite(end) or end != int(end) or int(end) % 300 != 0
            or not end - config.EXECUTION_WINDOW_SECONDS <= now
            or not now < end - config.MIN_SECONDS_TO_EXPIRY):
        raise RuntimeError("order is outside the current round execution interval")
    return end


def _mark_ambiguous(condition_id: str, window_end: float,
                    token_id: str | None = None) -> None:
    """Block resubmission of an unknown POST, scoped to the token when known.

    MULTI places both outcomes in one cycle. A condition-wide lock after the
    first leg's unclear reply is what skipped the other side even when the
    first FOK had already matched.
    """
    global _ambiguous_condition, _ambiguous_until, _ambiguous_all_tokens
    with _state_lock:
        cid = str(condition_id)
        end = float(window_end)
        if _ambiguous_condition != cid or time.time() >= _ambiguous_until:
            _ambiguous_tokens.clear()
            _ambiguous_all_tokens = False
        _ambiguous_condition = cid
        _ambiguous_until = end
        if token_id:
            _ambiguous_tokens.add(str(token_id))
        else:
            _ambiguous_all_tokens = True
            _ambiguous_tokens.clear()


def _resolve_ambiguous(condition_id: str, token_id: str) -> None:
    """Lift one token's resubmission block after its unknown order was
    reconciled with the venue (sniper_live, audit M7). Other blocks stay."""
    global _ambiguous_condition, _ambiguous_until, _ambiguous_all_tokens
    with _state_lock:
        if _ambiguous_condition != str(condition_id):
            return
        _ambiguous_tokens.discard(str(token_id))
        if not _ambiguous_tokens and not _ambiguous_all_tokens:
            _ambiguous_condition, _ambiguous_until = None, 0.0


def _ambiguous_blocks(condition_id: str, token_id: str) -> bool:
    with _state_lock:
        if str(token_id) in _ambiguous_exit_tokens:
            return True
        if _ambiguous_condition != str(condition_id):
            return False
        if time.time() >= _ambiguous_until:
            return False
        if _ambiguous_all_tokens:
            return True
        return str(token_id) in _ambiguous_tokens


attempt_log_failures = 0
# Directory the live diagnostic journals write into. Set by run_feeds ONLY when
# it starts LIVE; left None everywhere else, so nothing is written by default.
# The old default wrote unless argv[0] looked like a test, and fixtures still
# reached live_fill_timing.jsonl - every row in it was synthetic, in the one
# file meant to calibrate paper against live. Opt-in cannot leak that way.
_telemetry_dir: "pathlib.Path | None" = None


def _telemetry_path(env_name: str, filename: str) -> "str | None":
    """Where a live journal goes, or None when this is not a live run."""
    explicit = os.environ.get(env_name)
    if explicit:
        return explicit
    if _telemetry_dir is None:
        return None
    return str(_telemetry_dir / filename)
# Set by run_feeds to feeds.adapters.last_book_meta. Left None when no feed
# is installed, in which case book ages are recorded as unavailable rather
# than as zero.
book_meta_provider = None


def _book_meta(token_id):
    """Timestamps for the book the order path just priced against."""
    provider = book_meta_provider
    if provider is None:
        return None
    try:
        return provider(token_id)
    except Exception:
        return None


def _book_age_s(meta, *, at_mono=None):
    """Age of the served book in seconds, monotonic only, or None.

    Never mixes clocks: updated_mono and the reading below both come from
    time.monotonic(). A book served from REST carries no update stamp, so
    its age is unavailable - which is recorded as None, never as 0.
    """
    if not meta or meta.get("updated_mono") is None:
        return None
    now = time.monotonic() if at_mono is None else at_mono
    age = now - float(meta["updated_mono"])
    return age if age >= 0 else None


def _attempt_log(row: dict) -> None:
    """Append one row to the append-only live attempt journal.

    Every INTENDED live order must appear here, including ones that never
    reach the venue, are rejected, or never fill. Without the orders that did
    NOT fill there is no live fill rate, and with no live fill rate PAPER
    cannot be calibrated against LIVE at all - which is the whole reason a
    profitable paper run kept losing money live.

    Rows are never rewritten. One attempt emits several events and its history
    is reconstructed by joining them on attempt_id, so a crash mid-order
    leaves evidence rather than a gap.

    A logging failure must never stop trading, but it must not pass silently
    either: it increments attempt_log_failures for the runner to surface.
    """
    global attempt_log_failures
    try:
        import json as _json
        target = _telemetry_path("LIVE_ATTEMPTS_PATH", "live_attempts.jsonl")
        if not target:
            return
        with open(target, "a", encoding="utf-8") as fh:
            fh.write(_json.dumps(row, sort_keys=True, default=str) + "\n")
    except Exception:
        attempt_log_failures += 1


def _record_fill_timing(order_id, status, error, submitted_wall, responded_wall,
                        *, token_id, condition_id) -> None:
    """Append when a live order was POSTed and when the venue answered.

    Live receipts carried no timestamps at all, so the one number that decides
    whether paper can predict live - how long a real order takes to match -
    had never been measured. Paper assumes the venue matches as soon as
    PAPER_LATENCY_MS elapses. These rows, joined by order id to the confirmed
    lots in the ledger, give the real submit -> response -> confirm timeline.

    Diagnostic only: written outside the accounting journal, never read by any
    decision, and every failure is swallowed. Test runs write nothing unless
    LIVE_FILL_TIMING_PATH is set, so fixtures never masquerade as venue data.
    """
    try:
        import json as _json
        target = _telemetry_path("LIVE_FILL_TIMING_PATH", "live_fill_timing.jsonl")
        if not target:
            return
        row = {
            "order_id": order_id,
            "status": None if status is None else str(status),
            "error": _safe_error(error)[:160] if error else None,
            "token_id": str(token_id),
            "condition_id": str(condition_id),
            "submitted_wall": float(submitted_wall),
            "responded_wall": float(responded_wall),
            "post_seconds": float(responded_wall) - float(submitted_wall),
        }
        with open(target, "a", encoding="utf-8") as fh:
            fh.write(_json.dumps(row, sort_keys=True, default=str) + "\n")
    except Exception:
        pass


def _journal_receipt(receipt: dict) -> bool:
    with _state_lock:
        callback = _order_observer
    if callback is None:
        return False
    try:
        return callback(receipt) is not False
    except Exception as exc:
        print(f"[LIVE] CRITICAL: matched-order accounting journal failed: {type(exc).__name__}")
        return False


class GuardRejection:
    """An explicit, non-authorizing rejection with a caller-supplied safe reason."""

    def __init__(self, reason: str):
        self.reason = reason


def _pre_submit_guard_error(pre_submit_guard) -> str | None:
    """Run an optional last-moment execution guard and fail closed.

    The callback is intentionally argument-free: callers bind the exact side,
    round, and observation they authorized in a closure.  Only the literal
    boolean ``True`` permits submission; truthy objects or callback failures
    are unsafe at an irreversible order boundary.
    """
    if pre_submit_guard is None:
        return None
    if not callable(pre_submit_guard):
        return "pre-submit guard is not callable"
    try:
        allowed = pre_submit_guard()
    except Exception as exc:
        return f"pre-submit guard failed closed: {type(exc).__name__}: {_safe_error(exc)}"
    if isinstance(allowed, GuardRejection):
        return f"pre-submit guard rejected order: {allowed.reason}"
    if allowed is not True:
        return "pre-submit guard rejected order"
    return None


def sell_shares(token_id: str, shares: float, *, min_price: float = 0.0,
                condition_id: str | None = None,
                window_end: float | None = None) -> float:
    """Live FAK exit. Returns shares submitted, or 0.0 if nothing was sent.

    FAK, not FOK: an exit fills against whatever the book holds and cancels the
    rest. FOK would refuse the whole order whenever the full size is not
    resting at once, which is the normal state of the book a stop sells into -
    it is thin precisely because the position has already gone wrong.

    This deliberately does NOT reuse the entry preflight. That path exists to
    stop us BUYING into resolution and checks USDC balance, round window and
    price ceilings; none of it protects an exit, and some of it would block one
    at exactly the moment a stop needs to act.
    """
    global last_order_error
    if _live_disabled:
        last_order_error = "live execution is disabled by paper mode"
        return 0.0
    if not _valid_token(token_id):
        last_order_error = "invalid token id"
        return 0.0
    token = str(token_id)
    try:
        size = float(shares)
        floor = float(min_price)
    except (TypeError, ValueError, OverflowError):
        last_order_error = "invalid sell size or floor"
        return 0.0
    if not math.isfinite(size) or size <= 0 or not 0.0 <= floor < 1.0:
        last_order_error = "invalid sell size or floor"
        return 0.0
    if not _exit_lock.acquire(timeout=_API_LOCK_WAIT_SECONDS):
        last_order_error = "timed out waiting for the live API"
        return 0.0
    try:
        with _state_lock:
            if token in _ambiguous_exit_tokens:
                last_order_error = "prior live exit has an ambiguous result; token blocked until reconciliation"
                return 0.0
        try:
            if window_end is not None:
                _validate_round_end(window_end)
            client = _get_client()
            mo = MarketOrderArgs(
                token_id=token,
                amount=size,             # SELL sizes in SHARES, not USDC
                side=Side.SELL,
                price=floor,
                order_type=OrderType.FAK,
            )
            # No PartialCreateOrderOptions: the entry path derives tick/neg_risk
            # from a validated UP/DOWN mapping it already holds, and an exit has
            # only the one token. Letting the client resolve them from the token
            # is correct here and avoids asserting a mapping we cannot check.
            signed = client.create_market_order(mo)
            # Signing can fetch tick/neg-risk metadata and take long enough to
            # cross the expiry boundary. Preserve this helper's existing
            # eligibility check at the irreversible boundary as well.
            if window_end is not None:
                _validate_round_end(window_end)
        except Exception as exc:
            last_order_error = _safe_error(exc)
            print(f"[LIVE] Sell failed before submission: {last_order_error}")
            return 0.0
        try:
            resp = client.post_order(signed, OrderType.FAK)
        except Exception as exc:
            last_order_error = _safe_error(exc)
            if not _explicit_no_match_exception(exc):
                with _state_lock:
                    _ambiguous_exit_tokens.add(token)
                last_order_error = f"ambiguous live exit submission; token blocked until reconciliation: {last_order_error}"
            print(f"[LIVE] Sell failed: {last_order_error}")
            return 0.0
        oid, status, err = _accepted_order_response(resp)
        if err is not None:
            last_order_error = _safe_error(err)
            pending_oid, _status, pending_kind = _accepted_pending_response(resp)
            if pending_oid and pending_kind:
                with _state_lock:
                    _ambiguous_exit_tokens.add(token)
                last_order_error = "accepted live exit awaiting authoritative outcome; token blocked until reconciliation"
                # This is a submission, not evidence that every requested
                # share sold. The helper's documented return is size submitted.
                return size
            if not _definitive_rejection(resp):
                with _state_lock:
                    _ambiguous_exit_tokens.add(token)
                last_order_error = f"ambiguous live exit response; token blocked until reconciliation: {last_order_error}"
            print(f"[LIVE] Sell rejected: {last_order_error}")
            return 0.0
        last_order_error = None
        print(f"[LIVE] Sell submitted: {size:.6f} sh floor {floor:.4f} "
              f"status={status} id={str(oid)[-12:]}")
        return size
    finally:
        _exit_lock.release()


def place_trade(side: str, amount: float, up_token_id: str | None = None,
                down_token_id: str | None = None,
                condition_id: str | None = None,
                window_end: float | None = None,
                max_price: float | None = None,
                min_price: float | None = None, *,
                pre_submit_guard=None) -> bool:
    """Serialize live submissions so two callers cannot duplicate an entry.

    `max_price` caps this order alone, and may only tighten MAX_BUY_PRICE.
    `min_price` is its mirror: a floor for this order alone, which may only
    RAISE MIN_BUY_PRICE. A band caller needs both. Passing only the ceiling
    let a band's walk stop at the band's top but still fill anywhere down to
    the account-wide floor - outside the band being measured, and into a book
    that has moved against the side being bought.
    `pre_submit_guard`, when supplied, must return literal ``True`` immediately
    before each signing/POST boundary.  It may reject, but never changes side.
    """
    global last_order_error
    # Wait out a short in-flight balance/cancel HTTP. Failing immediately made
    # MULTI drop the complementary FOK when a ledger poll held this lock.
    if not _execution_lock.acquire(timeout=_API_LOCK_WAIT_SECONDS):
        last_order_error = "timed out waiting for the live API"
        return False
    try:
        return _place_trade(side, amount, up_token_id, down_token_id,
                            condition_id, window_end, max_price, min_price,
                            pre_submit_guard=pre_submit_guard)
    finally:
        _execution_lock.release()


def _place_trade(side: str, amount: float, up_token_id: str | None = None,
                 down_token_id: str | None = None,
                 condition_id: str | None = None,
                 window_end: float | None = None,
                 max_price: float | None = None,
                 min_price: float | None = None, *,
                 pre_submit_guard=None) -> bool:
    global last_order_error, last_order_status, last_order_receipt, _journal_fault
    global attempt_log_failures
    last_order_error = None
    last_order_status = None
    last_order_receipt = None
    if _live_disabled:
        last_order_error = "live execution is disabled by paper mode"
        return False
    if _journal_fault:
        last_order_error = _journal_fault
        return False
    with _state_lock:
        observer_missing = _order_observer is None
    if observer_missing:
        last_order_error = (
            "durable live order observer is not installed; refusing submission")
        return False
    side = str(side or "").upper()
    if side not in ("UP", "DOWN"):
        last_order_error = f"invalid side {side}"
        return False
    try:
        amount = round(float(amount), 2)
    except (TypeError, ValueError, OverflowError):
        last_order_error = "invalid order amount"
        return False
    if not math.isfinite(amount) or amount <= 0:
        last_order_error = "order amount must be finite and positive"
        return False
    if (not _valid_token(up_token_id) or not _valid_token(down_token_id)
            or str(up_token_id) == str(down_token_id)):
        last_order_error = "missing or invalid UP/DOWN token ids"
        return False
    if not condition_id or not CONDITION_RE.fullmatch(str(condition_id)):
        last_order_error = "missing or invalid current condition id"
        return False
    token_id = str(up_token_id if side == "UP" else down_token_id)
    # Minted BEFORE any network call, so an order that dies in preflight, is
    # rejected, or never fills still has an identity in the telemetry.
    attempt_id = uuid.uuid4().hex
    decision_wall = time.time()
    decision_mono = time.monotonic()

    try:
        end = _validate_round_end(window_end)
        if _ambiguous_blocks(condition_id, token_id):
            raise RuntimeError(
                "prior submission of this outcome has an ambiguous result; "
                "it is blocked to prevent duplicates")
        client = _get_client()
        rules = _validate_market_mapping(
            client.get_clob_market_info(str(condition_id)), condition_id,
            up_token_id, down_token_id)
        if rules.get("taker_delay"):
            # The venue will not state the delay, so treat it as at most the
            # assumed value and refuse inside that window - an order sent later
            # than this could match after the round has already resolved, which
            # is the whole reason the flag is dangerous. Guessing LOW is the
            # unsafe direction: if the real delay exceeds the assumption, an
            # order accepted just outside the window still lands too late.
            assumed = float(config.ASSUMED_MATCH_DELAY_SECONDS)
            remaining = end - time.time()
            if remaining <= assumed:
                raise RuntimeError(
                    f"undisclosed matching delay: {remaining:.1f}s left is "
                    f"inside the assumed {assumed:.1f}s delay window")
        if config.TICK_SIZE is not None and Decimal(str(config.TICK_SIZE)) != rules["tick"]:
            raise RuntimeError("configured TICK_SIZE disagrees with the current market")
        if config.NEG_RISK is not None and bool(config.NEG_RISK) != rules["neg_risk"]:
            raise RuntimeError("configured NEG_RISK disagrees with the current market")
        ceiling = config.MAX_BUY_PRICE
        if max_price is not None:
            # Tighten only: an order cap can never raise the account ceiling.
            ceiling = min(ceiling, float(max_price))
        limit = _round_limit(ceiling, rules["tick"])
        base_floor = config.MIN_BUY_PRICE
        if min_price is not None:
            # Raise only, mirroring the ceiling's tighten-only rule: an order
            # floor can never drop below the account-wide minimum.
            base_floor = max(base_floor, float(min_price))
        floor = _round_floor(base_floor, rules["tick"])
        if floor > limit:
            raise RuntimeError(
                "the effective price floor is above the order's price cap")
        book_read_wall = time.time()
        book_read_mono = time.monotonic()
        _bids, asks = orderbook.validate_buy_liquidity(
            token_id, amount, float(limit), config.MAX_ALLOWED_SPREAD,
            min_price=float(floor))
        # Raise the stake just enough to buy the venue's minimum shares, the
        # same way PAPER's size_to_venue_minimum does. Without this, a fixed
        # BET_SIZE can only fill at asks <= BET_SIZE / minimum - $2.50 stops
        # filling above 0.50 - so live refused most of the prices paper had
        # been filling, and the two modes measured different strategies.
        # The order costs more than BET_SIZE at those prices; that is already
        # true in paper and is what the startup sizing NOTE describes.
        amount = _size_to_venue_minimum(amount, asks, rules, limit)
        shares, estimated_fee = _quote_fok(
            asks, Decimal(str(amount)), limit,
            rules["fee_rate"], rules["fee_exponent"])
        if shares < rules["minimum"]:
            raise RuntimeError(
                f"executable size {shares:.6f} is below venue minimum "
                f"{rules['minimum']} even after sizing up to ${amount:.2f}")
        funds = _read_balance(client)
        if funds is None:
            raise RuntimeError("malformed balance/allowance response")
        total_cost = float(Decimal(str(amount)) + estimated_fee)
        if funds["balance"] + 1e-9 < total_cost:
            raise RuntimeError(
                f"insufficient pUSD balance (${funds['balance']:.6f} < ${total_cost:.6f} incl fee)")
        if funds["allowance"] + 1e-9 < total_cost:
            raise RuntimeError(
                f"insufficient pUSD allowance (${funds['allowance']:.6f} < ${total_cost:.6f} incl fee)")
        _validate_round_end(end)
    except Exception as exc:
        last_order_error = _safe_error(exc)
        # An intended order that never reached the venue is still an attempt.
        _attempt_log({
            "attempt_id": attempt_id, "event": "blocked", "wall": time.time(),
            "decision_wall": decision_wall,
            "elapsed_ms": (time.monotonic() - decision_mono) * 1000.0,
            "condition_id": str(condition_id), "token_id": str(token_id),
            "side": side, "order_side": "BUY", "window_end": window_end,
            "requested_notional": amount, "final_status": "BLOCKED",
            "reason": last_order_error[:200],
        })
        print(f"[LIVE] Order preflight failed: {last_order_error}")
        return False

    # Everything the venue was asked for, and what the book said it should
    # get, recorded BEFORE the first submission. Joined later to the confirmed
    # lots in the ledger by order id.
    try:
        _meta = _book_meta(token_id)
        _best_ask = float(asks[0]["price"]) if asks else None
        _best_bid = float(_bids[0]["price"]) if _bids else None
        _exp_shares = float(shares)
        _exp_vwap = float(amount) / _exp_shares if _exp_shares > 0 else None
        _attempt_log({
            "attempt_id": attempt_id, "event": "attempt", "wall": time.time(),
            "decision_wall": decision_wall, "book_read_wall": book_read_wall,
            # The venue's own book timestamp is not exposed at this layer.
            # What IS proven is that parse_orderbook accepted the book inside
            # ORDERBOOK_MAX_AGE_SECONDS, so this is an upper bound, not the
            # measured age. Closing that gap is a named follow-up.
            # The limit parse_orderbook enforced. NOT a measurement, and
            # deliberately unchanged in meaning.
            "book_age_bound_s": float(config.ORDERBOOK_MAX_AGE_SECONDS),
            # Measured, when the feed served a websocket book. A REST book
            # carries no update stamp, so these stay null rather than 0.
            "book_source": (_meta or {}).get("source"),
            "book_source_ts_ms": (_meta or {}).get("exchange_ts_ms"),
            "book_updated_mono": (_meta or {}).get("updated_mono"),
            "book_read_wall": book_read_wall,
            "book_age_at_decision_s": _book_age_s(
                _meta, at_mono=book_read_mono),
            "condition_id": str(condition_id), "token_id": str(token_id),
            "side": side, "order_side": "BUY", "order_type": "FOK",
            "window_end": window_end,
            "best_bid": _best_bid, "best_ask": _best_ask,
            "spread": (_best_ask - _best_bid
                       if _best_ask is not None and _best_bid is not None
                       else None),
            "requested_notional": float(amount),
            "limit_price": float(limit), "floor_price": float(floor),
            "expected_shares": _exp_shares,
            "expected_vwap": _exp_vwap,
            "expected_fee": float(estimated_fee),
            "expected_slippage": (_exp_vwap - _best_ask
                                  if _exp_vwap is not None
                                  and _best_ask is not None else None),
        })
    except Exception:
        attempt_log_failures += 1

    # Retry only an explicit FOK no-fill.  Every retry rebuilds and re-signs;
    # transport timeouts remain ambiguous and are never retried.
    for attempt in range(3):
        guard_error = _pre_submit_guard_error(pre_submit_guard)
        if guard_error is not None:
            last_order_error = guard_error
            print(f"[LIVE] Order blocked before signing: {last_order_error}")
            return False
        if attempt:
            try:
                _validate_round_end(end)
                # The stake and price cap were fixed for the first attempt.
                # Recheck that this exact order is still executable after the
                # no-fill response; do not size it up or widen its limit.
                book_read_mono = time.monotonic()
                _bids, asks = orderbook.validate_buy_liquidity(
                    token_id, amount, float(limit), config.MAX_ALLOWED_SPREAD,
                    min_price=float(floor))
                shares, estimated_fee = _quote_fok(
                    asks, Decimal(str(amount)), limit,
                    rules["fee_rate"], rules["fee_exponent"])
                if shares < rules["minimum"]:
                    raise RuntimeError(
                        f"retry size {shares:.6f} is below venue minimum "
                        f"{rules['minimum']}")
                funds = _read_balance(client)
                if funds is None:
                    raise RuntimeError("malformed balance/allowance response")
                total_cost = float(Decimal(str(amount)) + estimated_fee)
                if funds["balance"] + 1e-9 < total_cost:
                    raise RuntimeError("insufficient pUSD balance for FOK retry")
                if funds["allowance"] + 1e-9 < total_cost:
                    raise RuntimeError("insufficient pUSD allowance for FOK retry")
                _validate_round_end(end)
            except Exception as exc:
                last_order_error = f"FOK retry preflight failed: {_safe_error(exc)}"
                _attempt_log({
                    "attempt_id": attempt_id, "event": "retry_blocked",
                    "try": attempt, "wall": time.time(),
                    "condition_id": str(condition_id), "token_id": token_id,
                    "side": side, "order_side": "BUY",
                    "requested_notional": amount, "final_status": "BLOCKED",
                    "reason": last_order_error[:200],
                })
                print(f"[LIVE] {last_order_error}")
                return False
        try:
            _validate_round_end(end)
            if time.monotonic() - book_read_mono > config.ORDERBOOK_MAX_AGE_SECONDS:
                raise RuntimeError("priced order book became stale during preflight")
            options = PartialCreateOrderOptions(
                tick_size=str(rules["tick"]), neg_risk=rules["neg_risk"])
            mo = MarketOrderArgs(
                token_id=token_id,
                amount=amount,
                side=Side.BUY,
                price=float(limit),
                order_type=OrderType.FOK,
                user_usdc_balance=funds["balance"],
            )
            signed = client.create_market_order(mo, options=options)
            _validate_round_end(end)
        except Exception as exc:
            last_order_error = _safe_error(exc)
            print(f"[LIVE] Order signing failed: {last_order_error}")
            return False
        guard_error = _pre_submit_guard_error(pre_submit_guard)
        if guard_error is not None:
            last_order_error = guard_error
            print(f"[LIVE] Order blocked before submission: {last_order_error}")
            return False
        # Balance/network checks and signing can outlive the priced snapshot.
        # A newer background book does not freshen the book used for this order.
        if time.monotonic() - book_read_mono > config.ORDERBOOK_MAX_AGE_SECONDS:
            last_order_error = "priced order book became stale before submission"
            return False
        submitted_wall = time.time()
        # Age of the same book at the moment of submission, monotonic only.
        submit_book_age_s = _book_age_s(_book_meta(token_id))
        try:
            resp = client.post_order(signed, OrderType.FOK)
        except Exception as exc:
            err = _safe_error(exc)
            _record_fill_timing(None, "post_raised", err, submitted_wall,
                                time.time(), token_id=token_id,
                                condition_id=condition_id)
            _attempt_log({
                "attempt_id": attempt_id, "event": "post", "try": attempt,
                "wall": time.time(), "submitted_wall": submitted_wall,
                "post_seconds": time.time() - submitted_wall,
                "order_id": None, "status": "POST_RAISED",
                "final_status": "SUBMIT_FAILED", "reason": err[:200],
            })
            known_no_fill = _explicit_no_match_exception(exc)
            if known_no_fill and attempt < 2:
                continue
            if not known_no_fill:
                _mark_ambiguous(condition_id, end, token_id)
                err = f"ambiguous submission; outcome blocked: {err}"
            last_order_error = err
            print(f"[LIVE] Place order error: {last_order_error}")
            return False

        responded_wall = time.time()
        oid, status, err = _accepted_order_response(resp)
        _record_fill_timing(oid, status, err, submitted_wall, responded_wall,
                            token_id=token_id, condition_id=condition_id)
        _attempt_log({
            "attempt_id": attempt_id, "event": "post", "try": attempt,
            "wall": responded_wall, "submitted_wall": submitted_wall,
            "responded_wall": responded_wall,
            "post_seconds": responded_wall - submitted_wall,
            "order_id": oid, "status": None if status is None else str(status),
            "final_status": ("ACCEPTED" if err is None else "REJECTED"),
            "reason": (_safe_error(err)[:200] if err else None),
            "book_age_at_submit_s": submit_book_age_s,
        })
        if err is not None:
            err = _safe_error(err)
            known_no_fill = _is_no_match(err) and _definitive_rejection(resp)
            if known_no_fill and attempt < 2:
                continue
            pending_oid, pending_status, pending_kind = _accepted_pending_response(resp)
            if pending_oid and pending_kind:
                last_order_status = pending_kind
                last_order_receipt = _build_receipt(
                    resp, pending_oid, pending_status,
                    condition_id=condition_id, token_id=token_id,
                    window_end=end, amount=amount, estimated_fee=estimated_fee,
                    fee_rate=rules["fee_rate"],
                    fee_exponent=rules["fee_exponent"],
                    validation=pending_kind,
                )
                journaled = _journal_receipt(last_order_receipt)
                last_order_receipt["accounting_journaled"] = journaled
                if not journaled:
                    _mark_ambiguous(condition_id, end)
                    _journal_fault = (
                        "CRITICAL: accepted order could not be durably journaled; "
                        "all further live submissions are disabled")
                    last_order_error = _journal_fault
                    last_order_status = f"{pending_kind}_JOURNAL_FAULT"
                    print(f"[LIVE] {_journal_fault}")
                    # The order was accepted. Returning False could make the
                    # caller retry an already-live action; return True while
                    # the process-wide fault blocks every later submission.
                    return True
                # A documented delayed placement has a known order ID and is
                # followed through the user channel. Any other incomplete
                # success blocks another submission of this token because its
                # terminal outcome is not yet known. The other outcome of the
                # same market stays placeable so MULTI can still complete a pair.
                if pending_kind != "DELAYED_PENDING_OUTCOME":
                    _mark_ambiguous(condition_id, end, token_id)
                last_order_error = None
                print(
                    f"[LIVE] FOK accepted: {side} ${amount:.2f} "
                    f"orderID={pending_oid} status={pending_status or 'missing'} | "
                    "no fill recorded; waiting for CONFIRMED trade status"
                )
                return True
            # A response received after POST that is not an explicit no-fill
            # may describe an accepted order incompletely. Block this token
            # only: the complementary MULTI leg is a different order.
            if not _definitive_rejection(resp):
                _mark_ambiguous(condition_id, end, token_id)
                # Say WHAT came back, not just that it was unclassifiable.
                # A venue with a taker matching delay can acknowledge an order
                # before it has any trade evidence, and the parser requires
                # tradeIDs or transactionsHashes - so a perfectly ordinary
                # accepted order used to read as ambiguous and skip the other
                # MULTI leg. That lock is now per-outcome, not per-round.
                # Field names and status only; no amounts, ids or wallet data.
                shape = "non-dict"
                if isinstance(resp, dict):
                    present = sorted(k for k, v in resp.items()
                                     if v not in (None, "", [], {}))
                    shape = (f"status={str(resp.get('status') or 'missing')!r} "
                             f"fields={present}")
                print(f"[LIVE] unclassified response shape: {shape}")
                # Also write it to disk. stdout is block-buffered when it is
                # not a console, so the one line that identifies this failure
                # can sit unseen in a buffer for the whole run.
                try:
                    # The suites drive place_trade with stubbed clients and
                    # deliberately malformed replies. Writing those to the same
                    # file makes test fixtures indistinguishable from venue
                    # behaviour - which already sent one investigation down a
                    # false trail. Record only from a real run.
                    if _telemetry_dir is None:
                        raise RuntimeError("not a live run")
                    import datetime as _dt
                    with open(_DIAG_PATH, "a", encoding="utf-8") as _fh:
                        _fh.write(
                            f"[{_dt.datetime.now():%Y-%m-%d %H:%M:%S}] "
                            f"unclassified response: {shape}\n"
                            f"    err={err}\n")
                        _fh.flush()
                        os.fsync(_fh.fileno())
                except Exception:
                    pass
                err = f"ambiguous order response; outcome blocked: {err}"
            last_order_error = err
            print(f"[LIVE] Place order error: {last_order_error}")
            return False

        last_order_status = "MATCHED_PENDING_CONFIRMATION"
        last_order_receipt = _build_receipt(
            resp, oid, status, condition_id=condition_id, token_id=token_id,
            window_end=end, amount=amount, estimated_fee=estimated_fee,
            fee_rate=rules["fee_rate"], fee_exponent=rules["fee_exponent"],
            validation="MATCHED_WITH_TRADE_EVIDENCE",
        )
        journaled = _journal_receipt(last_order_receipt)
        last_order_receipt["accounting_journaled"] = journaled
        if not journaled:
            _mark_ambiguous(condition_id, end)
            _journal_fault = (
                "CRITICAL: matched order could not be durably journaled; "
                "all further live submissions are disabled")
            last_order_error = _journal_fault
            last_order_status = "MATCHED_PENDING_CONFIRMATION_JOURNAL_FAULT"
            print(f"[LIVE] {_journal_fault}")
            # POST succeeded, so this remains a submitted action.  The fault
            # gate above prevents a duplicate or any subsequent live order.
            return True
        print(
            f"[LIVE] FOK matched: {side} ${amount:.2f} orderID={oid} | "
            "position waits for CONFIRMED trade status"
        )
        return True

    last_order_error = "FOK no-fill after 3 freshly signed attempts"
    return False
