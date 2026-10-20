"""Offline order construction using the pinned official unified Polymarket SDK.

The bot supplies its already-validated market rules. No metadata lookup or
network request is performed while building or signing an order. SDK imports
are lazy so arithmetic-only paper runs do not require signing dependencies.
"""
from __future__ import annotations

import dataclasses
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_FLOOR
from typing import Any

BUY, SELL = "BUY", "SELL"
POST_ORDER = "/order"
BYTES32_ZERO = "0x" + "00" * 32


class OrderType:
    GTC = "GTC"
    GTD = "GTD"
    FAK = "FAK"
    FOK = "FOK"


class Side:
    BUY = BUY
    SELL = SELL


@dataclass(frozen=True)
class RoundingConfig:
    amount: int
    price: int
    size: int
    tick_size: str


ROUNDING_CONFIG = {
    tick: RoundingConfig(amount=amount, price=price, size=2, tick_size=tick)
    for tick, amount, price in (("0.1", 3, 1), ("0.01", 4, 2), ("0.005", 5, 3),
                                ("0.0025", 6, 4), ("0.001", 5, 3), ("0.0001", 6, 4))
}


def round_down(value, decimals: int) -> float:
    from polymarket._internal.actions.orders.math import round_down as sdk_round_down

    return float(sdk_round_down(Decimal(str(value)), decimals))


@dataclass
class ApiCreds:
    api_key: str
    api_secret: str = field(repr=False)
    api_passphrase: str = field(repr=False)

    @property
    def key(self) -> str:
        return self.api_key

    @property
    def secret(self) -> str:
        return self.api_secret

    @property
    def passphrase(self) -> str:
        return self.api_passphrase


@dataclass
class RequestArgs:
    method: str
    request_path: str
    body: Any = None
    serialized_body: str | None = None


@dataclass
class CreateOrderOptions:
    tick_size: str
    neg_risk: bool


@dataclass
class PartialCreateOrderOptions:
    tick_size: str | None = None
    neg_risk: bool | None = None


@dataclass
class MarketOrderArgsV2:
    token_id: str
    amount: float
    side: str
    price: float = 0
    order_type: str = OrderType.FOK
    builder_code: str = BYTES32_ZERO
    metadata: str = BYTES32_ZERO
    user_usdc_balance: float = 0


@dataclass
class OrderArgsV2:
    token_id: str
    price: float
    size: float
    side: str
    expiration: int = 0
    builder_code: str = BYTES32_ZERO
    metadata: str = BYTES32_ZERO


MarketOrderArgs = MarketOrderArgsV2
OrderArgs = OrderArgsV2


class Signer:
    """Parsed local key with the small interface used by the execution code."""

    def __init__(self, private_key: str, chain_id: int) -> None:
        from eth_account import Account

        self.account = Account.from_key(private_key)
        self.private_key = private_key
        self.chain_id = int(chain_id)

    def address(self) -> str:
        return self.account.address

    def get_chain_id(self) -> int:
        return self.chain_id

    def sign(self, message_hash: str | bytes) -> str:
        """Sign a digest, retaining support for a cached eth_keys key."""
        raw = bytes.fromhex(message_hash.removeprefix("0x")) if isinstance(message_hash, str) else message_hash
        if len(raw) != 32:
            raise ValueError("a signing digest must contain exactly 32 bytes")
        if hasattr(self.private_key, "sign_msg_hash"):
            signature = self.private_key.sign_msg_hash(raw)
            encoded = (signature.r.to_bytes(32, "big") + signature.s.to_bytes(32, "big")
                       + bytes([signature.v + 27]))
        else:
            encoded = bytes(self.account.unsafe_sign_hash(raw).signature)
        return "0x" + encoded.hex()

    def sign_typed_data(self, *, full_message: dict):
        return self.account.sign_typed_data(full_message=full_message)


class SignedOrder:
    """SDK signed value with legacy field aliases used by durable journalling."""

    def __init__(self, sdk_order, unsigned_order, order_hash: str) -> None:
        self.sdk_order = sdk_order
        self.unsigned_order = unsigned_order
        self.order_hash = order_hash

    @property
    def order(self):
        return self

    def __getattr__(self, name: str):
        aliases = {"tokenId": "token_id", "makerAmount": "maker_amount",
                   "takerAmount": "taker_amount", "signatureType": "signature_type"}
        if name == "side":
            return 0 if self.sdk_order.side == BUY else 1
        return getattr(self.sdk_order, aliases.get(name, name))


def _positive_decimal(name: str, value) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive finite number")
    number = Decimal(str(value))
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return number


class OrderBuilder:
    """Use official SDK amounts, exchange routing, typed data and signatures."""

    def __init__(self, signer: Signer, signature_type: int = 0, funder: str | None = None) -> None:
        if type(signature_type) is not int or signature_type not in (0, 1, 2, 3):
            raise ValueError("signature_type must be 0, 1, 2 or 3")
        self.signer = signer
        self.signature_type = signature_type
        self.funder = funder or signer.address()
        self.last_order_hash = None
        self.last_sign_ns = None
        self._wallet_classification = None

    def _environment(self):
        from polymarket._internal.environment import PRODUCTION_CONFIG

        if self.signer.get_chain_id() != PRODUCTION_CONFIG.chain_id:
            raise ValueError("the unified SDK adapter supports the production Polygon chain only")
        return PRODUCTION_CONFIG

    def get_order_amounts(self, side, size, price, rounding):
        from polymarket._internal.actions.orders.limit import _compute_limit_order_amounts
        from polymarket._internal.actions.orders.math import round_normal

        if side not in (BUY, SELL):
            raise ValueError("side must be BUY or SELL")
        tick = Decimal(rounding.tick_size)
        maker, taker = _compute_limit_order_amounts(
            price=round_normal(_positive_decimal("price", price), rounding.price),
            size=_positive_decimal("size", size), side=side, tick_size=tick)
        return (0 if side == BUY else 1), maker, taker

    def get_market_order_amounts(self, side, amount, price, rounding):
        from polymarket._internal.actions.orders.market import _compute_market_order_amounts

        if side not in (BUY, SELL):
            raise ValueError("side must be BUY or SELL")
        maker, taker = _compute_market_order_amounts(
            amount=_positive_decimal("amount", amount), price=_positive_decimal("price", price),
            side=side, tick_size=Decimal(rounding.tick_size))
        return (0 if side == BUY else 1), maker, taker

    def _wallet(self):
        from polymarket._internal.wallet import classify_account, signature_type_for

        if self._wallet_classification is None:
            account = classify_account(signer=self.signer.address(), wallet=self.funder,
                                       config=self._environment().wallet_derivation)
            if account.signer_type != "OWNER":
                raise ValueError("session-key signing is unsupported by this bot")
            if signature_type_for(account.wallet_type) != self.signature_type:
                raise ValueError("configured signature_type does not match the signer/funder wallet")
            self._wallet_classification = account
        return self._wallet_classification

    def _build(self, args, options, *, market: bool, version: int):
        from polymarket._internal.actions.exchange_asset import resolve_asset_id
        from polymarket._internal.actions.orders.context import (
            MIN_SUPPORTED_TICK_SIZE, resolve_order_exchange_address,
            resolve_rounding_config, validate_price_on_tick_grid,
        )
        from polymarket._internal.actions.orders.limit import _compute_limit_order_amounts
        from polymarket._internal.actions.orders.market import _compute_market_order_amounts
        from polymarket._internal.actions.orders.orders import create_signed_order, create_unsigned_order
        from polymarket._internal.actions.orders.typed_data import build_order_signature, build_order_typed_data
        from polymarket._internal.actions.orders.types import OrderDraft
        from polymarket._internal.validation import validate_builder_code
        from polymarket._internal.wallet import wrap_deposit_wallet_signature
        from polymarket.types import EvmAddress, HexString

        if version != 2:
            raise ValueError("legacy protocol-v1 signing is unsupported by the unified SDK")
        if args.side not in (BUY, SELL):
            raise ValueError("side must be BUY or SELL")
        if options is None or type(options.neg_risk) is not bool or options.tick_size is None:
            raise ValueError("offline signing requires explicit tick_size and neg_risk")
        config = self._environment()
        wallet = self._wallet()
        tick = _positive_decimal("tick_size", options.tick_size)
        resolve_rounding_config(tick)
        # The legacy callers add a sub-tick float nudge. Remove it by flooring
        # to the actual grid, exactly as their pre-send price validation does.
        price = (_positive_decimal("price", args.price) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
        validate_price_on_tick_grid(price, tick, "price")
        asset = resolve_asset_id(asset_id=None, token_id=str(args.token_id))
        amount = _positive_decimal("amount" if market else "size", args.amount if market else args.size)
        if market:
            if args.order_type not in (OrderType.FOK, OrderType.FAK):
                raise ValueError("a market order must use FOK or FAK")
            offered, requested = _compute_market_order_amounts(amount=amount, price=price, side=args.side,
                                                              tick_size=tick)
            order_type = args.order_type
            expiration = 0
        else:
            offered, requested = _compute_limit_order_amounts(price=price, size=amount, side=args.side,
                                                             tick_size=tick)
            expiration = args.expiration
            if type(expiration) is not int or expiration < 0:
                raise ValueError("expiration must be a non-negative integer")
            order_type = OrderType.GTD if expiration else OrderType.GTC
        if offered <= 0 or requested <= 0:
            raise ValueError("rounding produced a zero order amount")
        if market and args.side == BUY:
            # Match the SDK's protected-BUY check. Rounding shares down can
            # raise maker/taker above the cap; it must stay strictly below
            # the next ask even if the exchange makes its tick finer later.
            price_numerator, price_denominator = price.as_integer_ratio()
            tick_numerator, tick_denominator = MIN_SUPPORTED_TICK_SIZE.as_integer_ratio()
            next_price_numerator = (
                price_numerator * tick_denominator + tick_numerator * price_denominator)
            next_price_denominator = price_denominator * tick_denominator
            if offered * next_price_denominator >= requested * next_price_numerator:
                raise ValueError(
                    "Cannot preserve max_price with this BUY amount and tick precision; "
                    "increase amount or choose a different max_price")
        metadata = getattr(args, "metadata", BYTES32_ZERO)
        if metadata != BYTES32_ZERO:
            raise ValueError("the unified SDK currently supports zero order metadata only")
        draft = OrderDraft(chain_id=config.chain_id,
                           exchange_address=resolve_order_exchange_address(config, asset_id=asset,
                                                                           neg_risk=options.neg_risk),
                           expiration=expiration, funder_address=EvmAddress(self.funder),
                           offered_amount=offered, order_type=order_type, side=args.side,
                           signer=EvmAddress(self.signer.address()), requested_amount=requested,
                           asset_id=asset, builder_code=validate_builder_code(args.builder_code))
        unsigned = create_unsigned_order(draft, wallet=EvmAddress(self.funder), wallet_type=wallet.wallet_type)
        typed_data = build_order_typed_data(unsigned)
        start = time.perf_counter_ns()
        raw_signature = self.signer.sign_typed_data(full_message=typed_data).signature.hex()
        signature = HexString(raw_signature if raw_signature.startswith("0x") else "0x" + raw_signature)
        signature = wrap_deposit_wallet_signature(
            signer=self.signer.address(), signer_type=wallet.signer_type,
            signature=build_order_signature(unsigned, signature))
        self.last_sign_ns = (start, time.perf_counter_ns())
        sdk_order = create_signed_order(unsigned, signature)
        self.last_order_hash = _unsigned_order_hash(unsigned)
        return SignedOrder(sdk_order, unsigned, self.last_order_hash)

    def build_market_order(self, order_args, options, version: int = 2, fee_rate_bps=None):
        return self._build(order_args, options, market=True, version=version)

    def build_order(self, order_args, options, version: int = 2, fee_rate_bps=None):
        return self._build(order_args, options, market=False, version=version)


def _unsigned_order_hash(unsigned) -> str:
    from eth_utils.crypto import keccak
    from polymarket._internal.actions.orders.typed_data import _app_domain_separator, _order_contents_hash

    domain = _app_domain_separator(unsigned, protocol_version=unsigned.protocol_version)
    digest = keccak(b"\x19\x01" + bytes.fromhex(domain[2:]) + _order_contents_hash(unsigned))
    return "0x" + digest.hex()


def order_digest_v2(domain_separator: bytes | str, order) -> bytes:
    """Compatibility helper using the SDK's canonical Order struct hashing."""
    from eth_utils.crypto import keccak
    from polymarket._internal.actions.orders.typed_data import _order_contents_hash

    if isinstance(order, SignedOrder):
        order = order.unsigned_order
    if isinstance(domain_separator, str):
        domain_separator = bytes.fromhex(domain_separator.removeprefix("0x"))
    if len(domain_separator) != 32:
        raise ValueError("an EIP-712 domain separator must contain exactly 32 bytes")
    return keccak(b"\x19\x01" + domain_separator + _order_contents_hash(order))


def order_hash_v2(signed, *, neg_risk: bool, chain_id: int, signer=None) -> str | None:
    """Canonical venue order ID, including the current asset's protocol domain."""
    if isinstance(signed, SignedOrder):
        unsigned = signed.unsigned_order
        if int(chain_id) != unsigned.chain_id:
            return None
        return _unsigned_order_hash(unsigned)
    return None


def order_to_json_v2(signed, owner: str, order_type: str, post_only: bool = False) -> dict:
    from polymarket._internal.actions.orders.post import build_post_order_request

    if order_type not in (OrderType.GTC, OrderType.GTD, OrderType.FAK, OrderType.FOK):
        raise ValueError("unknown order type")
    sdk_order = signed.sdk_order if isinstance(signed, SignedOrder) else signed
    sdk_order = dataclasses.replace(sdk_order, order_type=order_type, post_only=post_only)
    _, payload = build_post_order_request(sdk_order, owner_api_key=owner)
    return payload


def create_level_2_headers(signer, creds, request_args: RequestArgs, timestamp: int | None = None) -> dict:
    from polymarket._internal.hmac import build_hmac_signature

    stamp = int(time.time()) if timestamp is None else timestamp
    if type(stamp) is not int or stamp <= 0:
        raise ValueError("authentication timestamp must be a positive integer in seconds")
    body = request_args.serialized_body
    if body is None and request_args.body is not None:
        body = json.dumps(request_args.body, separators=(",", ":"), ensure_ascii=False)
    signature = build_hmac_signature(secret=creds.api_secret, timestamp=stamp,
                                     method=request_args.method, path=request_args.request_path, body=body)
    return {"POLY_ADDRESS": signer.address(), "POLY_API_KEY": creds.api_key,
            "POLY_PASSPHRASE": creds.api_passphrase, "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": str(stamp)}


__all__ = ["ApiCreds", "BUY", "SELL", "Side", "OrderType", "POST_ORDER", "BYTES32_ZERO",
           "RequestArgs", "CreateOrderOptions", "PartialCreateOrderOptions", "MarketOrderArgsV2",
           "MarketOrderArgs", "OrderArgsV2", "OrderArgs", "Signer", "OrderBuilder", "SignedOrder",
           "create_level_2_headers", "order_to_json_v2", "order_hash_v2", "order_digest_v2",
           "ROUNDING_CONFIG", "RoundingConfig", "round_down"]
