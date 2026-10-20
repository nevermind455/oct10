"""Small bot-facing adapter for the supported ``polymarket`` SDK.

Only this module bridges the SDK's typed account responses to the dictionaries
consumed by the existing execution and reconciliation code. Order submission
uses ``post_order`` once; the SDK's allowance-recovery placement helpers are
deliberately not used.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
import re
import threading
import time

from clob_signing import ApiCreds, OrderBuilder, Signer


class AssetType:
    COLLATERAL = "COLLATERAL"
    CONDITIONAL = "CONDITIONAL"
    CONDITIONAL_V2 = "CONDITIONAL-V2"


@dataclass
class BalanceAllowanceParams:
    asset_type: str = AssetType.COLLATERAL
    token_id: str | None = None
    signature_type: int | None = None


@dataclass
class TradeParams:
    id: str | None = None
    maker_address: str | None = None
    market: str | None = None
    asset_id: str | None = None
    before: int | str | None = None
    after: int | str | None = None


@dataclass
class OpenOrderParams:
    id: str | None = None
    market: str | None = None
    asset_id: str | None = None


def _value(value):
    return getattr(value, "value", value)


def _wire(value):
    """Retain decimal precision and the legacy REST epoch timestamp format."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite amount in CLOB response")
        return format(value, "f")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    if isinstance(value, dict):
        return {str(k): _wire(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_wire(v) for v in value]
    if hasattr(value, "model_dump"):
        return _wire(value.model_dump(mode="python"))
    return value


def _account_row(value):
    row = _wire(value)
    if not isinstance(row, dict):
        raise ValueError("CLOB account response is not an object")
    # Typed SDK names differ from the legacy account-trade REST field names.
    for typed, wire in (("matched_at", "match_time"), ("updated_at", "last_update"),
                        ("expires_at", "expiration")):
        if typed in row:
            row[wire] = row.pop(typed)
    if "expiration" in row and row["expiration"] is None:
        row["expiration"] = "0"
    status = row.get("status")
    if isinstance(status, str):
        for prefix in ("TRADE_STATUS_", "ORDER_STATUS_"):
            if status.startswith(prefix):
                row["status"] = status[len(prefix):]
                break
    return row


def _order_response(response):
    """SDK execution amounts already use dollars/shares, not micro-units."""
    def amount(value):
        text = _wire(value)
        # Legacy consumers infer units from textual representation. Preserve
        # the SDK's explicit decimal-unit contract even for whole amounts.
        return text + ".0" if isinstance(text, str) and "." not in text else text

    if getattr(response, "ok", None) is True:
        return {
            "success": True,
            "errorMsg": "",
            "orderID": str(response.order_id),
            "status": response.status,
            "makingAmount": amount(response.making_amount),
            "takingAmount": amount(response.taking_amount),
            "tradeIDs": list(response.trade_ids),
            "transactionsHashes": list(response.transactions_hashes),
        }
    if getattr(response, "ok", None) is False:
        code = str(response.code)
        if code not in ("unmatched", "market_not_ready", "not_enough_balance", "invalid_nonce",
                        "invalid_expiration", "post_only_would_cross", "post_only_mode",
                        "fok_not_filled", "fak_not_filled"):
            # The SDK discards the raw order id and success flag for unknown
            # response shapes. That cannot establish a safe no-fill outcome.
            raise ValueError("CLOB order outcome is unknown; reconcile before retrying")
        return {
            "success": False,
            "errorMsg": str(response.message),
            "orderID": "",
            "status": "unmatched" if code in ("unmatched", "fak_not_filled", "fok_not_filled") else "",
            "code": code,
            "makingAmount": "0",
            "takingAmount": "0",
            "tradeIDs": [],
            "transactionsHashes": [],
        }
    raise ValueError("unrecognized typed CLOB order response")


class ClobClient:
    """Preserve this bot's API while using the unified SDK underneath.

    Production Polygon and existing EOA/proxy/Safe/deposit wallets are supported. An
    unknown wallet is rejected rather than selecting the SDK's session-key
    fallback. Wallet deployment is intentionally outside this adapter.
    """

    def __init__(self, host, chain_id=137, key=None, signature_type=None,
                 funder=None, use_server_time=False, creds=None,
                 timeout_s=20.0, keepalive_s=300.0):
        from polymarket.environments import PRODUCTION
        from polymarket._internal.environment import get_environment_config
        from polymarket._internal.wallet import try_classify_wallet_type, signature_type_for

        self.host = str(host).rstrip("/")
        if self.host != "https://clob.polymarket.com" or chain_id != 137:
            raise ValueError("unified SDK adapter requires the production CLOB origin and Polygon chain 137")
        self.signer = Signer(key, chain_id)
        wallet = funder or self.signer.address()
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", str(wallet)):
            raise ValueError("invalid CLOB funding wallet")
        environment_config = get_environment_config(PRODUCTION)
        wallet_type = try_classify_wallet_type(
            signer=self.signer.address(), wallet=wallet,
            config=environment_config.wallet_derivation,
        )
        if wallet_type is None:
            raise ValueError("funding wallet does not belong to the configured signer; session keys are unsupported")
        wanted = 0 if signature_type is None else signature_type
        if type(wanted) is not int or wanted not in (0, 1, 2, 3):
            raise ValueError("signature type must be 0 (EOA), 1 (proxy), 2 (Safe), or 3 (deposit)")
        if wanted != signature_type_for(wallet_type):
            raise ValueError("configured signature type does not match the funding wallet")
        self.builder = OrderBuilder(self.signer, wanted, funder)
        self.creds = None
        self._secure = None
        self._environment = PRODUCTION
        self._wallet = str(wallet)
        self._wallet_type = wallet_type
        for label, number in (("timeout_s", timeout_s), ("keepalive_s", keepalive_s)):
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number <= 0:
                raise ValueError(f"{label} must be a positive finite number")
        self._timeout_s = float(timeout_s)
        self._keepalive_s = float(keepalive_s)
        self._use_server_time = bool(use_server_time)
        self._clock_server = None
        self._clock_mono = None
        self._lock = threading.RLock()
        if creds is not None:
            self.set_api_creds(creds)

    @staticmethod
    def _compat_creds(creds):
        if isinstance(creds, ApiCreds):
            return creds
        return ApiCreds(api_key=creds.key, api_secret=creds.secret, api_passphrase=creds.passphrase)

    def _configure_transport(self, secure):
        """Configure only this SDK instance; never replace an SDK global pool."""
        import httpx
        from polymarket.clients._transport import SyncTransport, TransportOptions
        from polymarket._internal.hmac import build_hmac_signature

        options = TransportOptions(
            timeout=httpx.Timeout(self._timeout_s),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20,
                                keepalive_expiry=self._keepalive_s),
            http2=True,
        )
        ctx = secure._ctx
        public = SyncTransport(base_url=self.host, options=options)

        def resolve(method, path, body):
            if self._use_server_time:
                timestamp = self._server_timestamp(public)
            else:
                timestamp = int(time.time())
            credentials = ctx.credentials
            return {
                "POLY_ADDRESS": str(ctx.signer.address),
                "POLY_API_KEY": credentials.key,
                "POLY_PASSPHRASE": credentials.passphrase,
                "POLY_TIMESTAMP": str(timestamp),
                "POLY_SIGNATURE": build_hmac_signature(
                    secret=credentials.secret, timestamp=timestamp,
                    method=method, path=path, body=body,
                ),
            }

        private = SyncTransport(base_url=self.host, options=options, header_resolver=resolve)
        try:
            if self._use_server_time:
                self._server_timestamp(public)
            secure._ctx = replace(ctx, clob=public, secure_clob=private)
        except BaseException:
            public.close()
            private.close()
            raise
        ctx.clob.close()
        ctx.secure_clob.close()

    def _server_timestamp(self, public):
        with self._lock:
            now = time.monotonic()
            if self._clock_mono is None or now - self._clock_mono >= 60.0:
                started = time.monotonic()
                value = public.get_json("/time")
                finished = time.monotonic()
                if isinstance(value, dict):
                    value = value.get("server_time")
                if isinstance(value, bool):
                    raise ValueError("invalid CLOB server timestamp")
                try:
                    stamp = float(value)
                except (TypeError, ValueError, OverflowError) as exc:
                    raise ValueError("invalid CLOB server timestamp") from exc
                if not math.isfinite(stamp) or not 946684800 <= stamp < 4102444800:
                    raise ValueError("invalid CLOB server timestamp")
                self._clock_server = stamp
                self._clock_mono = (started + finished) / 2
            return int(self._clock_server + time.monotonic() - self._clock_mono)

    def _initialize(self, creds=None):
        from polymarket import SecureClient
        from polymarket.models.clob import ApiKeyCreds

        class ExistingWalletClient(SecureClient):
            def _deploy_default_deposit_wallet(self):
                raise RuntimeError("an existing deployed wallet is required; automatic wallet deployment is disabled")

        with self._lock:
            if self._secure is not None:
                return self._secure
            credentials = None if creds is None else ApiKeyCreds(
                key=creds.api_key, secret=creds.api_secret, passphrase=creds.api_passphrase,
            )
            # Explicit wallet preserves the configured funding account. The
            # override prevents the SDK from deploying an undeployed deposit
            # wallet as a side effect of credential initialization.
            secure = ExistingWalletClient.create(
                private_key=self.signer.private_key, wallet=self._wallet,
                environment=self._environment, credentials=credentials,
            )
            try:
                if str(secure.wallet).lower() != self._wallet.lower() or secure.wallet_type != self._wallet_type:
                    raise ValueError("SDK resolved an unexpected trading wallet")
                self._configure_transport(secure)
            except BaseException:
                secure.close()
                raise
            self._secure = secure
            self.creds = self._compat_creds(secure.credentials)
            return secure

    def create_or_derive_api_key(self):
        self._initialize()
        return self.creds

    def set_api_creds(self, creds):
        with self._lock:
            incoming = self._compat_creds(creds)
            if self._secure is not None:
                if incoming != self.creds:
                    raise ValueError("API credentials cannot change on an initialized client; create a new client")
                return
            self._initialize(incoming)

    def get_balance_allowance(self, params=None):
        params = params or BalanceAllowanceParams()
        if params.signature_type is not None and params.signature_type != self.builder.signature_type:
            raise ValueError("balance request signature type differs from trading wallet")
        result = self._initialize().get_balance_allowance(
            asset_type=_value(params.asset_type), token_id=params.token_id,
        )
        # BalanceAllowance is explicitly denominated in integer base units;
        # the caller's existing six-decimal conversion remains authoritative.
        return {"balance": str(result.balance),
                "allowances": {str(k): str(v) for k, v in result.allowances.items()}}

    def get_clob_market_info(self, condition_id):
        condition = str(condition_id)
        if not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition):
            raise ValueError("invalid CLOB condition id")
        value = self._initialize()._ctx.clob.get_json(f"/clob-markets/{condition}")
        if not isinstance(value, dict):
            raise ValueError("CLOB market metadata is not an object")
        return value

    def get_order(self, order_id):
        from polymarket._internal.actions.account import build_get_order_request, parse_open_order

        identity = str(order_id)
        if not identity or len(identity) > 256 or not re.fullmatch(r"[A-Za-z0-9_-]+", identity):
            raise ValueError("invalid CLOB order id")
        path, params = build_get_order_request(order_id=identity)
        value = self._initialize()._ctx.secure_clob.get_json(path, params=params)
        # A venue null is used by existing recovery code only in conjunction
        # with its independent complete trade evidence. Malformed objects
        # still fail typed validation and can never become a missing order.
        return None if value is None else _account_row(parse_open_order(value))

    @staticmethod
    def _rows(paginator, only_first_page=False):
        if only_first_page:
            return [_account_row(v) for v in paginator.first_page().items]
        result = []
        for page in paginator:
            if page.limit_reached:
                raise ValueError("CLOB account pagination reached a depth limit; completeness is unknown")
            result.extend(_account_row(v) for v in page.items)
        return result

    def get_trades(self, params=None, only_first_page=False):
        params = params or TradeParams()
        paginator = self._initialize().list_account_trades(
            id=params.id, market=params.market, asset_id=params.asset_id,
            maker_address=params.maker_address,
            after=None if params.after is None else str(params.after),
            before=None if params.before is None else str(params.before),
        )
        return self._rows(paginator, only_first_page)

    def get_orders(self, params=None, only_first_page=False):
        params = params or OpenOrderParams()
        paginator = self._initialize().list_open_orders(
            id=params.id, market=params.market, asset_id=params.asset_id,
        )
        return self._rows(paginator, only_first_page)

    def cancel_all(self):
        result = self._initialize().cancel_all()
        return {"canceled": list(result.canceled), "not_canceled": dict(result.not_canceled)}

    def create_market_order(self, args, options=None):
        if options is not None:
            return self.builder.build_market_order(args, options)
        # Exit callers do not have a validated binary-market mapping. Resolve
        # the token's actual metadata through the SDK rather than guessing it.
        side = _value(args.side)
        kwargs = {"token_id": str(args.token_id), "side": side,
                  "order_type": _value(args.order_type)}
        # Legacy price=0 means resolve the market price, including the
        # default unrestricted SELL exit. The unified SDK requires every
        # explicitly supplied price bound to be strictly positive.
        try:
            if isinstance(args.price, bool):
                raise ValueError("market price bound must be finite and nonnegative")
            price = Decimal(str(args.price))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("market price bound must be finite and nonnegative") from exc
        if not price.is_finite() or price < 0:
            raise ValueError("market price bound must be finite and nonnegative")
        if side == "BUY":
            kwargs["amount"] = Decimal(str(args.amount))
            if price > 0:
                kwargs["max_price"] = price
        elif side == "SELL":
            kwargs["shares"] = Decimal(str(args.amount))
            if price > 0:
                kwargs["min_price"] = price
        else:
            raise ValueError("order side must be BUY or SELL")
        builder_code = getattr(args, "builder_code", None)
        if builder_code and str(builder_code) != "0x" + "0" * 64:
            kwargs["builder_code"] = builder_code
        return self._initialize().create_market_order(**kwargs)

    def create_order(self, args, options=None):
        if options is not None:
            return self.builder.build_order(args, options)
        kwargs = {"token_id": str(args.token_id), "side": _value(args.side),
                  "price": Decimal(str(args.price)), "size": Decimal(str(args.size))}
        if getattr(args, "expiration", 0):
            kwargs["expiration"] = int(args.expiration)
        builder_code = getattr(args, "builder_code", None)
        if builder_code and str(builder_code) != "0x" + "0" * 64:
            kwargs["builder_code"] = builder_code
        return self._initialize().create_limit_order(**kwargs)

    def post_order(self, signed_order, order_type="GTC", post_only=False):
        from polymarket._internal.actions.orders.post import build_post_order_request
        from polymarket.models.clob.order_response import RawOrderResponse

        if _value(order_type) not in ("GTC", "GTD", "FAK", "FOK"):
            raise ValueError("unknown CLOB order type")
        if type(post_only) is not bool:
            raise ValueError("post_only must be a boolean")
        signed = getattr(signed_order, "sdk_order", signed_order)
        signed = replace(signed, order_type=_value(order_type), post_only=bool(post_only))
        secure = self._initialize()
        path, payload = build_post_order_request(signed, owner_api_key=secure.credentials.key)
        # Use the official SDK's payload, authentication, and native transport
        # exactly once. Its high-level response normalizer discards raw order
        # IDs and trade evidence on unknown/conflicting responses; retaining
        # those fields lets the bot preserve exposure and reconcile safely.
        value = secure._ctx.secure_clob.post_json(path, json=payload)
        if not isinstance(value, dict):
            raise ValueError("CLOB order response is not an object; reconcile before retrying")
        validation = dict(value)
        # Real matched FOK replies can omit totals. Absence is preserved in
        # the returned raw response; zeros here only permit SDK shape checks.
        validation.setdefault("makingAmount", "0")
        validation.setdefault("takingAmount", "0")
        RawOrderResponse.parse_response(validation)
        return value

    def close(self):
        with self._lock:
            if self._secure is not None:
                self._secure.close()
                self._secure = None


__all__ = ["ClobClient", "AssetType", "BalanceAllowanceParams", "TradeParams", "OpenOrderParams"]
