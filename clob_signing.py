"""Offline order construction using the pinned official unified Polymarket SDK.

The bot supplies its already-validated market rules. No metadata lookup or
network request is performed while building or signing an order. SDK imports
are lazy so arithmetic-only paper runs do not require signing dependencies.
"""
from __future__ import annotations

import dataclasses
import json
import os
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
        self.secp_key = None         # fast_signer: a held libsecp256k1 key (sign() below)

    def address(self) -> str:
        return self.account.address

    def get_chain_id(self) -> int:
        return self.chain_id

    def sign(self, message_hash: str | bytes) -> str:
        """Sign a digest, retaining support for a cached eth_keys key."""
        raw = bytes.fromhex(message_hash.removeprefix("0x")) if isinstance(message_hash, str) else message_hash
        if len(raw) != 32:
            raise ValueError("a signing digest must contain exactly 32 bytes")
        secp = getattr(self, "secp_key", None)
        if secp is not None:
            # the call eth_keys' coincurve backend makes, on a key built once instead of per signature
            recoverable = secp.sign_recoverable(raw, hasher=None)
            encoded = recoverable[:64] + bytes([recoverable[64] + 27])
        elif hasattr(self.private_key, "sign_msg_hash"):
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
        # The direct EIP-712 path (order_typed_digest + the cached key): None until the
        # first eligible order proves it byte-identical to sign_typed_data, then True
        # (used) or False (never again in this process). SNIPER_FAST_SIGN=0 turns it off.
        self.fast_sign: bool | None = None
        self.fast_sign_note = ""

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
        from polymarket._internal.actions.orders.orders import create_signed_order
        from polymarket._internal.actions.orders.typed_data import build_order_signature, build_order_typed_data
        from polymarket._internal.wallet import wrap_deposit_wallet_signature
        from polymarket.types import HexString

        unsigned, wallet = self._unsigned(args, options, market=market, version=version)
        start = time.perf_counter_ns()
        raw_signature, order_hash = self._sign_unsigned(unsigned, build_order_typed_data)
        signature = HexString(raw_signature if raw_signature.startswith("0x") else "0x" + raw_signature)
        signature = wrap_deposit_wallet_signature(
            signer=self.signer.address(), signer_type=wallet.signer_type,
            signature=build_order_signature(unsigned, signature))
        self.last_sign_ns = (start, time.perf_counter_ns())
        sdk_order = create_signed_order(unsigned, signature)
        self.last_order_hash = order_hash or _unsigned_order_hash(unsigned)
        return SignedOrder(sdk_order, unsigned, self.last_order_hash)

    def _unsigned(self, args, options, *, market: bool, version: int):
        """(the SDK's unsigned order, the wallet classification): every amount,
        routing and validation step of an order, without its signature."""
        from polymarket._internal.actions.exchange_asset import resolve_asset_id
        from polymarket._internal.actions.orders.context import (
            MIN_SUPPORTED_TICK_SIZE, resolve_order_exchange_address,
            resolve_rounding_config, validate_price_on_tick_grid,
        )
        from polymarket._internal.actions.orders.limit import _compute_limit_order_amounts
        from polymarket._internal.actions.orders.market import _compute_market_order_amounts
        from polymarket._internal.actions.orders.orders import create_unsigned_order
        from polymarket._internal.actions.orders.types import OrderDraft
        from polymarket._internal.validation import validate_builder_code
        from polymarket.types import EvmAddress

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
        return unsigned, wallet

    def _sign_unsigned(self, unsigned, build_order_typed_data) -> tuple[str, str | None]:
        """(signature hex, order hash or None). The standard order typed data
        (signature types 0-2) is signed from its digest, encoded directly
        (order_typed_digest) - the bytes sign_typed_data produces at about a tenth of
        its cost (2026-10-25: ~1.0ms of the VPS's 8ms decided -> wire). Until one
        order of this builder has been signed both ways and compared byte for byte,
        and for anything else (type 3, SNIPER_FAST_SIGN=0, an encoding the direct
        path does not take, a failed comparison), the SDK's own path signs."""
        digest = None
        if (self.fast_sign is not False and _fast_sign_enabled()
                and getattr(unsigned, "signature_type", None) in (0, 1, 2)):
            try:
                digest = order_typed_digest(unsigned)
            except Exception as exc:  # noqa: BLE001 - the SDK path below decides this order
                self.fast_sign_note = f"direct digest unavailable ({type(exc).__name__}); SDK signing"[:160]
        if digest is not None and self.fast_sign:
            return self.signer.sign(digest), "0x" + digest.hex()
        raw = self.signer.sign_typed_data(full_message=build_order_typed_data(unsigned)).signature.hex()
        raw = raw if raw.startswith("0x") else "0x" + raw
        if digest is not None:
            self._prove_fast_sign(unsigned, digest, raw)
        return raw, None

    def _prove_fast_sign(self, unsigned, digest: bytes, raw: str) -> None:
        """First use: the direct digest must equal the SDK's order hash and the
        cached key's signature of it the exact string sign_typed_data returned."""
        try:
            same_hash = "0x" + digest.hex() == _unsigned_order_hash(unsigned)
            same_sig = self.signer.sign(digest) == raw
        except Exception as exc:  # noqa: BLE001 - unproven means unused
            same_hash = same_sig = False
            detail = f"proof raised {type(exc).__name__}"
        else:
            detail = (f"digest {'ok' if same_hash else 'differs'}, "
                      f"signature {'ok' if same_sig else 'differs'} from sign_typed_data")
        self.fast_sign = same_hash and same_sig
        self.fast_sign_note = ("direct EIP-712 signing verified byte-identical to sign_typed_data"
                               if self.fast_sign else f"direct EIP-712 signing DISABLED: {detail}")

    def build_market_order(self, order_args, options, version: int = 2, fee_rate_bps=None):
        return self._build(order_args, options, market=True, version=version)

    def prove_fast_signing(self) -> str:
        """Settle the direct signing path at start-up, so the first live order does
        not pay for the proof - without the key signing any order. An order of this
        wallet (maker, signer, signature type, exchange) is built and NOT signed: its
        direct digest must equal the SDK's order hash and eth_account's digest of its
        typed data (what sign_typed_data signs). The held key's signature of a digest
        must equal sign_typed_data's on a self-test message no contract accepts (no
        chain id, no verifying contract). Returns what was settled (start-up log)."""
        if self.fast_sign is not None or not _fast_sign_enabled():
            return self.fast_sign_note or ("" if _fast_sign_enabled() else
                                           "direct EIP-712 signing off (SNIPER_FAST_SIGN=0)")
        try:
            unsigned, _wallet = self._unsigned(
                MarketOrderArgsV2(token_id=PROOF_TOKEN_ID, amount=1.0, side=BUY, price=0.5, order_type=OrderType.FAK),
                CreateOrderOptions(tick_size="0.01", neg_risk=False), market=True, version=2)
        except Exception as exc:  # noqa: BLE001 - the SDK path stays; a real order raises its own error
            self.fast_sign_note = f"start-up signing proof failed ({type(exc).__name__}); the first order proves it"
            return self.fast_sign_note
        if unsigned.signature_type not in (0, 1, 2):
            self.fast_sign = False
            self.fast_sign_note = "signature type 3 signs its nested TypedDataSign: SDK signing"
            return self.fast_sign_note
        try:
            from eth_account.messages import encode_typed_data
            from eth_utils.crypto import keccak
            from polymarket._internal.actions.orders.typed_data import build_order_typed_data

            def eip712_digest(typed: dict) -> bytes:
                message = encode_typed_data(full_message=typed)
                return keccak(b"\x19" + message.version + message.header + message.body)
            digest = order_typed_digest(unsigned)
            same_hash = "0x" + digest.hex() == _unsigned_order_hash(unsigned)
            same_typed = digest == eip712_digest(build_order_typed_data(unsigned))
            probe = {"types": {"EIP712Domain": [{"name": "name", "type": "string"}],
                               "SigningSelfTest": [{"name": "digest", "type": "bytes32"}]},
                     "primaryType": "SigningSelfTest", "domain": {"name": SELF_TEST_DOMAIN},
                     "message": {"digest": digest}}
            reference = self.signer.sign_typed_data(full_message=probe).signature.hex()
            reference = reference if reference.startswith("0x") else "0x" + reference
            same_sig = self.signer.sign(eip712_digest(probe)) == reference
        except Exception as exc:  # noqa: BLE001 - unproven means unused
            same_hash = same_typed = same_sig = False
            detail = f"proof raised {type(exc).__name__}"
        else:
            detail = (f"digest {'ok' if same_hash and same_typed else 'differs'}, "
                      f"signature {'ok' if same_sig else 'differs'} from sign_typed_data")
        self.fast_sign = same_hash and same_typed and same_sig
        self.fast_sign_note = ("direct EIP-712 signing verified at start (digest = SDK order hash = eth_account "
                               "typed-data digest; held-key signature = sign_typed_data)" if self.fast_sign
                               else f"direct EIP-712 signing DISABLED: {detail}")
        return self.fast_sign_note

    def build_order(self, order_args, options, version: int = 2, fee_rate_bps=None):
        return self._build(order_args, options, market=False, version=version)


def _fast_sign_enabled() -> bool:
    """SNIPER_FAST_SIGN=0 keeps every order on the SDK's sign_typed_data path."""
    return str(os.environ.get("SNIPER_FAST_SIGN", "1")).strip().lower() not in ("0", "false", "no", "off")


# The start-up proof's token: a legacy-namespace position id (bits 40-103 set), so its
# unsigned order routes to the same exchange and protocol version as the live 5-minute
# markets. No registered token; the order is never signed or sent.
PROOF_TOKEN_ID = str((1 << 255) | (0x5EED << 64) | 1)
# The self-test message's EIP-712 domain: a name only - no chain id, no verifying
# contract - so its signature authorizes nothing anywhere.
SELF_TEST_DOMAIN = "sniperBTC signing self-test - authorizes nothing"

_DOMAIN_SEPARATORS: dict = {}
_CHECKED_ADDRESSES: dict = {}


def _word_uint(value, bits: int = 256) -> bytes:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected an integer, got {type(value).__name__}")
    if not 0 <= value < (1 << bits):
        raise ValueError(f"uint{bits} out of range")
    return value.to_bytes(32, "big")


def _word_address(value) -> bytes:
    """An address word exactly as eth_abi encodes it: 20 bytes, left-padded (eth_abi
    takes what eth_utils.is_address takes; so does this)."""
    if isinstance(value, (bytes, bytearray)):
        if len(value) != 20:
            raise ValueError("invalid address")
        return bytes(value).rjust(32, b"\x00")
    text = str(value)
    word = _CHECKED_ADDRESSES.get(text)
    if word is None:
        from eth_utils import is_address
        if not is_address(text):
            raise ValueError("invalid address")
        raw = bytes.fromhex(text[2:] if text[:2] in ("0x", "0X") else text)
        if len(raw) != 20:
            raise ValueError("invalid address")
        word = raw.rjust(32, b"\x00")
        if len(_CHECKED_ADDRESSES) < 64:            # a wallet's own two addresses, in practice
            _CHECKED_ADDRESSES[text] = word
    return word


def _word_bytes32(value) -> bytes:
    """Only the canonical form - "0x" and 64 hex digits, as the SDK validates a builder
    code and writes zero metadata - which the SDK's hash and eth_account both take
    alike; any other form is left to the SDK path to accept or refuse."""
    if not isinstance(value, str) or len(value) != 66 or not value.startswith("0x"):
        raise ValueError("bytes32 field is not 0x + 64 hex digits")
    return bytes.fromhex(value[2:])


def order_typed_digest(unsigned) -> bytes:
    """The EIP-712 digest of a standard order's typed data - keccak(0x1901 ||
    domain separator || hashStruct(Order)) - with the struct encoded directly:
    every Order field is one static 32-byte word, so eth_abi's generic encoder
    adds only cost. The domain separator is the SDK's own, kept per exchange.
    Signature type 3 (deposit wallets) signs a nested TypedDataSign: not this."""
    from eth_utils.crypto import keccak
    from polymarket._internal.actions.orders import typed_data as td
    if unsigned.signature_type not in (0, 1, 2):
        raise ValueError("only signature types 0-2 sign the standard order typed data")
    version = unsigned.protocol_version
    key = (version, unsigned.chain_id, str(unsigned.exchange_address))
    domain = _DOMAIN_SEPARATORS.get(key)
    if domain is None:
        domain = bytes.fromhex(td._app_domain_separator(unsigned, protocol_version=version)[2:])
        _DOMAIN_SEPARATORS[key] = domain
    struct = keccak(b"".join((
        td._ORDER_TYPE_HASH, _word_uint(unsigned.salt), _word_address(unsigned.maker),
        _word_address(unsigned.signer), _word_uint(int(unsigned.token_id)), _word_uint(unsigned.maker_amount),
        _word_uint(unsigned.taker_amount), _word_uint(td._encode_side(unsigned.side), 8),
        _word_uint(unsigned.signature_type, 8), _word_uint(unsigned.timestamp),
        _word_bytes32(unsigned.metadata), _word_bytes32(unsigned.builder))))
    return keccak(b"\x19\x01" + domain + struct)


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
