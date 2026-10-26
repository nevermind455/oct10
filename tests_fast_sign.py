#!/usr/bin/env python3
"""Direct EIP-712 order signing (2026-10-25): the same bytes as the SDK's sign_typed_data.

clob_signing.order_typed_digest encodes the standard Order struct directly (every field
is one static 32-byte word) and the held libsecp256k1 key signs that digest: build+sign
~0.1ms instead of ~0.5ms here. A builder signs one order both ways and compares before
it trusts the direct path - live and paper do that at start-up on a throwaway order -
and anything it does not take (signature type 3, SNIPER_FAST_SIGN=0, an encoding it
refuses, a failed comparison) signs on the SDK's own path. These checks hold the direct
path to the SDK's order hash, to eth_account's typed-data digest computed independently,
and to the SDK path's complete signed order, wire payload included. Throwaway keys, no
network.

    python tests_fast_sign.py
"""
from __future__ import annotations

import contextlib
import dataclasses
import inspect
import os
import pathlib
import random
import secrets
import statistics
import sys
import time
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import clob_signing as C  # noqa: E402
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402

from eth_account import Account  # noqa: E402
from eth_account.messages import _hash_eip191_message, encode_typed_data  # noqa: E402
from eth_utils import to_checksum_address  # noqa: E402
from polymarket._internal.actions.orders import typed_data as TD  # noqa: E402
from polymarket._internal.actions.orders.types import BYTES32_ZERO, UnsignedOrder  # noqa: E402
from polymarket._internal.environment import PRODUCTION_CONFIG  # noqa: E402
from polymarket._internal.protocol import is_v2_position_id  # noqa: E402
from polymarket._internal.wallet import (  # noqa: E402
    derive_proxy_wallet_address, derive_safe_wallet_address, derive_uups_deposit_wallet_address)

LIVE_TOKEN = "92012601006026812704111674518766648628897961687330612274342782385608689495840"
EXCHANGES = (PRODUCTION_CONFIG.standard_exchange, PRODUCTION_CONFIG.neg_risk_exchange,
             PRODUCTION_CONFIG.exchange_v3)
U256 = (1 << 256) - 1


def eth_account_digest(unsigned) -> bytes:
    """What eth_account's sign_typed_data signs: computed without clob_signing or the SDK hash."""
    return bytes(_hash_eip191_message(encode_typed_data(full_message=TD.build_order_typed_data(unsigned))))


def random_address(rnd) -> str:
    raw = "0x" + rnd.getrandbits(160).to_bytes(20, "big").hex()
    return to_checksum_address(raw) if rnd.random() < 0.6 else raw          # checksummed or all lowercase


def random_unsigned(rnd, **fixed) -> UnsignedOrder:
    edge = rnd.random()
    fields = dict(
        chain_id=rnd.choice([137, 137, 137, 80002]),
        builder=rnd.choice([BYTES32_ZERO, "0x" + secrets.token_hex(32)]),
        exchange_address=rnd.choice(EXCHANGES + (random_address(rnd),)),
        expiration=rnd.choice([0, 1_900_000_000]),
        maker=random_address(rnd),
        maker_amount=(U256 if edge < 0.05 else 0 if edge < 0.1 else rnd.randrange(1, 10 ** 12)),
        metadata=rnd.choice([BYTES32_ZERO, "0x" + secrets.token_hex(32)]),
        order_type=rnd.choice(["FAK", "FOK", "GTC"]),
        salt=(U256 if edge < 0.05 else 0 if edge < 0.1 else rnd.getrandbits(rnd.choice([53, 64, 256]))),
        side=rnd.choice(["BUY", "SELL"]),
        signature_type=rnd.choice([0, 1, 2]),
        signer=random_address(rnd),
        taker_amount=(U256 if edge < 0.05 else rnd.randrange(1, 10 ** 12)),
        timestamp=rnd.choice([0, 1_792_710_675_000 + rnd.randrange(10 ** 9), U256]),
        token_id=str(rnd.choice([rnd.getrandbits(256), rnd.getrandbits(40), int(LIVE_TOKEN), 0, U256])),
        protocol_version=rnd.choice(["2", "3"]))
    fields.update(fixed)
    return UnsignedOrder(**fields)


def builder_for(sig_type: int, *, fast_key: bool = True, key: str | None = None):
    key = key or "0x" + secrets.token_hex(32)
    signer = S.fast_signer(key, 137) if fast_key else C.Signer(key, 137)
    funder = (None if sig_type == 0 else
              (derive_proxy_wallet_address if sig_type == 1 else derive_safe_wallet_address)(
                  signer.address(), PRODUCTION_CONFIG.wallet_derivation))
    return C.OrderBuilder(signer, sig_type, funder), key


def market_args(rnd, token=None):
    tick = rnd.choice(["0.01", "0.001", "0.0001"])
    return (C.MarketOrderArgsV2(token_id=token or str(rnd.choice([rnd.randrange(10 ** 70, 10 ** 77), int(LIVE_TOKEN)])),
                                amount=round(rnd.uniform(1, 50), 2), side=rnd.choice([C.BUY, C.SELL]),
                                price=round(rnd.uniform(0.05, 0.95), len(tick) - 2),
                                order_type=rnd.choice([C.OrderType.FAK, C.OrderType.FOK])),
            C.CreateOrderOptions(tick_size=tick, neg_risk=rnd.random() < 0.3))


def reference_signature(order, key: str) -> str:
    """The SDK path's signature for exactly this order, made with eth_account directly."""
    typed = TD.build_order_typed_data(order.unsigned_order)
    raw = Account.from_key(key).sign_message(encode_typed_data(full_message=typed)).signature.hex()
    return TD.build_order_signature(order.unsigned_order, "0x" + raw.removeprefix("0x"))


class CountingSigner:
    """Counts which signing call the builder makes; the signature itself is the real signer's."""

    def __init__(self, signer):
        self.inner, self.typed, self.digests, self.primary = signer, 0, 0, []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def sign_typed_data(self, *, full_message):
        self.typed += 1
        self.primary.append(full_message.get("primaryType"))
        return self.inner.sign_typed_data(full_message=full_message)

    def sign(self, message_hash):
        self.digests += 1
        return self.inner.sign(message_hash)


def counted(builder):
    builder.signer = CountingSigner(builder.signer)
    return builder.signer


@contextlib.contextmanager
def fast_sign_env(value):
    with patch.dict(os.environ, {"SNIPER_FAST_SIGN": value}):
        yield


# ============================================================ the digest itself
class DigestMatchesTheSdkAndEthAccount(unittest.TestCase):
    def test_random_orders_including_edge_values_and_every_domain(self):
        rnd = random.Random(20261025)
        n = 0
        for _ in range(600):
            u = random_unsigned(rnd)
            got = C.order_typed_digest(u)
            self.assertEqual("0x" + got.hex(), C._unsigned_order_hash(u), u)
            self.assertEqual(got, eth_account_digest(u), u)
            n += 1
        self.assertEqual(n, 600)

    def test_v2_position_ids_route_to_the_v3_domain_and_still_match(self):
        rnd = random.Random(7)
        token = str(rnd.getrandbits(40))
        self.assertTrue(is_v2_position_id(token))
        u = random_unsigned(rnd, token_id=token, exchange_address=PRODUCTION_CONFIG.exchange_v3, protocol_version="3")
        self.assertEqual(C.order_typed_digest(u), eth_account_digest(u))

    def test_domain_cache_is_keyed_by_version_chain_and_exact_exchange(self):
        rnd = random.Random(11)
        u = random_unsigned(rnd, chain_id=137, protocol_version="2",
                            exchange_address=PRODUCTION_CONFIG.standard_exchange)
        first = C.order_typed_digest(u)
        for change in (dict(chain_id=80002), dict(protocol_version="3"),
                       dict(exchange_address=PRODUCTION_CONFIG.neg_risk_exchange)):
            other = dataclasses.replace(u, **change)
            self.assertNotEqual(C.order_typed_digest(other), first, change)
            self.assertEqual(C.order_typed_digest(other), eth_account_digest(other), change)
        lower = dataclasses.replace(u, exchange_address=PRODUCTION_CONFIG.standard_exchange.lower())
        self.assertEqual(C.order_typed_digest(lower), eth_account_digest(lower))
        self.assertEqual(C.order_typed_digest(u), first)                       # the cache gave the same bytes

    def test_malformed_fields_never_get_a_digest_the_sdk_path_would_not_sign(self):
        """Wherever the direct encoder gives a digest, the SDK's order hash and eth_account's
        typed-data digest (what the SDK path signs) give the same one; wherever the SDK
        refuses, so does the direct encoder. Odd forms neither the SDK's builder-code check
        nor its zero metadata can produce are left to the SDK path."""
        rnd = random.Random(3)
        base = random_unsigned(rnd, signature_type=0)
        good = to_checksum_address("0x" + "ab" * 20)
        cases = [dict(maker=good[:2] + good[2:].swapcase()), dict(maker=good.lower()[2:]), dict(maker=bytes(20)),
                 dict(signer="0x1234"), dict(maker_amount=U256 + 1), dict(salt=-1), dict(taker_amount=True),
                 dict(timestamp=1.5), dict(metadata="0x" + "11" * 33), dict(metadata="0x" + "11" * 31),
                 dict(metadata="0x" + "1" * 63), dict(builder="0X" + "22" * 32), dict(token_id=str(U256 + 1)),
                 dict(token_id="0x1f"), dict(token_id=" 12 "), dict(side="sell"), dict(builder="0x" + "AB" * 32)]

        def outcome(fn, u):
            try:
                return fn(u)
            except Exception:  # noqa: BLE001 - refused, by whichever encoder
                return None
        accepted = 0
        for change in cases:
            bad = dataclasses.replace(base, **change)
            with self.subTest(change=change):
                direct = outcome(C.order_typed_digest, bad)
                sdk = outcome(lambda u: bytes.fromhex(C._unsigned_order_hash(u)[2:]), bad)
                signed = outcome(eth_account_digest, bad)
                if sdk is None:
                    self.assertIsNone(direct)
                if direct is not None:
                    accepted += 1
                    self.assertEqual(direct, sdk)
                    self.assertEqual(direct, signed)
        self.assertGreaterEqual(accepted, 5)

    def test_signature_type_3_is_not_the_standard_order_digest(self):
        u = dataclasses.replace(random_unsigned(random.Random(5)), signature_type=3)
        with self.assertRaisesRegex(ValueError, "signature types 0-2"):
            C.order_typed_digest(u)


# ============================================================ whole orders, both paths
class SignedOrdersAreByteIdentical(unittest.TestCase):
    def test_same_order_in_gives_the_same_signed_order_and_wire_payload_out(self):
        rnd = random.Random(20261026)
        checked = 0
        for i in range(90):
            sig_type = (0, 1, 2)[i % 3]
            key = "0x" + secrets.token_hex(32)
            fast, _ = builder_for(sig_type, fast_key=i % 2 == 0, key=key)
            sdk, _ = builder_for(sig_type, fast_key=i % 2 == 1, key=key)
            self.assertIn("verified", fast.prove_fast_signing())
            fast_counts, sdk_counts = counted(fast), counted(sdk)
            args, opts = market_args(rnd)
            salt, stamp = rnd.getrandbits(53), 1_792_710_675_000 + i
            with patch("polymarket._internal.actions.orders.orders._generate_salt", return_value=salt), \
                 patch("polymarket._internal.actions.orders.orders._current_timestamp_ms", return_value=stamp):
                a = fast.build_market_order(args, opts, version=2)
                with fast_sign_env("0"):
                    b = sdk.build_market_order(args, opts, version=2)
            self.assertEqual((fast_counts.typed, fast_counts.digests), (0, 1), "direct path, no typed data")
            self.assertEqual((sdk_counts.typed, sdk_counts.digests), (1, 0), "SDK path")
            self.assertEqual(a.sdk_order, b.sdk_order)
            self.assertEqual(a.order_hash, b.order_hash)
            self.assertEqual(a.order_hash, C._unsigned_order_hash(a.unsigned_order))
            self.assertEqual(C.order_to_json_v2(a, "owner-key", args.order_type),
                             C.order_to_json_v2(b, "owner-key", args.order_type))
            self.assertEqual(a.signature, reference_signature(a, key))
            checked += 1
        self.assertEqual(checked, 90)

    def test_one_proven_builder_signs_a_long_run_directly_and_correctly(self):
        rnd = random.Random(99)
        for sig_type in (0, 1, 2):
            for fast_key in (True, False):
                with self.subTest(sig_type=sig_type, fast_key=fast_key):
                    builder, key = builder_for(sig_type, fast_key=fast_key)
                    builder.prove_fast_signing()
                    counts = counted(builder)
                    owner = Account.from_key(key).address
                    for _ in range(40):
                        args, opts = market_args(rnd, token=LIVE_TOKEN)
                        got = builder.build_market_order(args, opts, version=2)
                        self.assertEqual(got.signature, reference_signature(got, key))
                        self.assertEqual(got.order_hash, C._unsigned_order_hash(got.unsigned_order))
                        typed = encode_typed_data(full_message=TD.build_order_typed_data(got.unsigned_order))
                        self.assertEqual(Account.recover_message(typed, signature=got.signature), owner)
                        self.assertEqual(builder.last_order_hash, got.order_hash)
                    self.assertEqual((counts.typed, counts.digests), (0, 40))

    def test_limit_orders_take_the_direct_path_too(self):
        builder, key = builder_for(0)
        builder.prove_fast_signing()
        counts = counted(builder)
        got = builder.build_order(C.OrderArgsV2(token_id=LIVE_TOKEN, price=0.53, size=10.0, side=C.BUY),
                                  C.CreateOrderOptions(tick_size="0.01", neg_risk=False))
        self.assertEqual(got.signature, reference_signature(got, key))
        self.assertEqual((counts.typed, counts.digests), (0, 1))

    def test_the_first_order_of_an_unproven_builder_is_the_proof(self):
        builder, key = builder_for(0)
        counts = counted(builder)
        rnd = random.Random(1)
        first = builder.build_market_order(*market_args(rnd, token=LIVE_TOKEN))
        self.assertTrue(builder.fast_sign)
        self.assertEqual((counts.typed, counts.digests), (1, 1))                 # signed both ways, compared
        self.assertEqual(first.signature, reference_signature(first, key))
        second = builder.build_market_order(*market_args(rnd, token=LIVE_TOKEN))
        self.assertEqual((counts.typed, counts.digests), (1, 2))
        self.assertEqual(second.signature, reference_signature(second, key))


# ============================================================ when it is not used
class FallsBackToTheSdkPath(unittest.TestCase):
    def test_switch_off_keeps_every_order_on_sign_typed_data(self):
        with fast_sign_env("0"):
            builder, key = builder_for(0)
            self.assertIn("SNIPER_FAST_SIGN=0", builder.prove_fast_signing())
            counts = counted(builder)
            rnd = random.Random(2)
            for _ in range(5):
                got = builder.build_market_order(*market_args(rnd, token=LIVE_TOKEN))
                self.assertEqual(got.signature, reference_signature(got, key))
            self.assertIsNone(builder.fast_sign)
            self.assertEqual((counts.typed, counts.digests), (5, 0))

    def test_a_digest_that_differs_disables_the_direct_path_for_good(self):
        builder, key = builder_for(1)
        real = C.order_typed_digest
        calls = []

        def wrong(unsigned):
            calls.append(1)
            d = real(unsigned)
            return bytes([d[0] ^ 1]) + d[1:]
        with patch.object(C, "order_typed_digest", wrong):
            note = builder.prove_fast_signing()
            self.assertIs(builder.fast_sign, False)
            self.assertIn("DISABLED", note)
            self.assertIn("digest differs", note)
            counts = counted(builder)
            rnd = random.Random(4)
            for _ in range(4):
                got = builder.build_market_order(*market_args(rnd, token=LIVE_TOKEN))
                self.assertEqual(got.signature, reference_signature(got, key))
                self.assertEqual(got.order_hash, C._unsigned_order_hash(got.unsigned_order))
        self.assertEqual(len(calls), 1, "never computed again once disabled")
        self.assertEqual((counts.typed, counts.digests), (4, 0))

    def test_a_signer_whose_digest_signature_differs_disables_it(self):
        builder, key = builder_for(0)
        inner = builder.signer

        class Skewed(CountingSigner):
            def sign(self, message_hash):
                sig = inner.sign(message_hash)
                return sig[:-2] + ("1c" if sig.endswith("1b") else "1b")              # the other recovery id
        builder.signer = Skewed(inner)
        self.assertIn("signature differs", builder.prove_fast_signing())
        self.assertIs(builder.fast_sign, False)
        got = builder.build_market_order(*market_args(random.Random(6), token=LIVE_TOKEN))
        self.assertEqual(got.signature, reference_signature(got, key))

    def test_a_proof_that_raises_disables_it_and_the_order_still_signs(self):
        builder, key = builder_for(0)
        inner = builder.signer

        class Broken(CountingSigner):
            def sign(self, message_hash):
                raise RuntimeError("no digest signing here")
        builder.signer = Broken(inner)
        got = builder.build_market_order(*market_args(random.Random(8), token=LIVE_TOKEN))
        self.assertEqual(got.signature, reference_signature(got, key))
        self.assertIs(builder.fast_sign, False)
        self.assertIn("proof raised RuntimeError", builder.fast_sign_note)

    def test_an_order_the_direct_encoder_refuses_goes_the_sdk_way_and_the_next_does_not(self):
        builder, key = builder_for(0)
        builder.prove_fast_signing()
        counts = counted(builder)
        real = C.order_typed_digest
        state = {"n": 0}

        def flaky(unsigned):
            state["n"] += 1
            if state["n"] == 1:
                raise RuntimeError("unexpected shape")
            return real(unsigned)
        rnd = random.Random(10)
        with patch.object(C, "order_typed_digest", flaky):
            a = builder.build_market_order(*market_args(rnd, token=LIVE_TOKEN))
            b = builder.build_market_order(*market_args(rnd, token=LIVE_TOKEN))
        for got in (a, b):
            self.assertEqual(got.signature, reference_signature(got, key))
        self.assertTrue(builder.fast_sign)
        self.assertEqual((counts.typed, counts.digests), (1, 1))

    def test_type_3_signs_on_the_sdk_path_even_when_proven(self):
        builder, _key = builder_for(0)
        builder.prove_fast_signing()
        counts = counted(builder)
        u3 = dataclasses.replace(random_unsigned(random.Random(12), chain_id=137), signature_type=3,
                                 signer=builder.signer.address())
        raw, order_hash = builder._sign_unsigned(u3, TD.build_order_typed_data)
        self.assertIsNone(order_hash)
        self.assertEqual((counts.typed, counts.digests), (1, 0))
        want = builder.signer.inner.account.sign_typed_data(full_message=TD.build_order_typed_data(u3)).signature.hex()
        self.assertEqual(raw, "0x" + want.removeprefix("0x"))

    def test_objects_without_the_sdk_shape_take_the_sdk_path(self):
        builder, _key = builder_for(0)
        builder.fast_sign = True
        seen = []

        class Stub:
            address = staticmethod(lambda: "0x" + "11" * 20)

            @staticmethod
            def sign_typed_data(*, full_message):
                seen.append(full_message)
                return type("Signed", (), {"signature": b"\x22" * 65})()
        builder.signer = Stub()
        raw, order_hash = builder._sign_unsigned(object(), lambda unsigned: {"typed": "official"})
        self.assertEqual((raw, order_hash, seen), ("0x" + "22" * 65, None, [{"typed": "official"}]))


# ============================================================ start-up proof and wiring
class StartUpProof(unittest.TestCase):
    def test_prove_settles_it_and_the_key_signs_no_order(self):
        for sig_type in (0, 1, 2):
            with self.subTest(sig_type=sig_type):
                builder, _key = builder_for(sig_type)
                counts = counted(builder)
                note = builder.prove_fast_signing()
                self.assertTrue(builder.fast_sign)
                self.assertIn("verified", note)
                self.assertEqual(counts.primary, ["SigningSelfTest"])           # the self-test message, no Order
                self.assertEqual((counts.typed, counts.digests), (1, 1))
                self.assertIsNone(builder.last_order_hash)
                self.assertIsNone(builder.last_sign_ns)
                builder.prove_fast_signing()                                   # settled: nothing signed again
                self.assertEqual((counts.typed, counts.digests), (1, 1))

    def test_the_self_test_message_authorizes_nothing(self):
        builder, key = builder_for(0)
        counts = counted(builder)
        seen = []
        real = counts.sign_typed_data

        def keep(*, full_message):
            seen.append(full_message)
            return real(full_message=full_message)
        counts.sign_typed_data = keep
        builder.prove_fast_signing()
        (probe,) = seen
        self.assertEqual(probe["domain"], {"name": C.SELF_TEST_DOMAIN})          # no chainId, no verifyingContract
        self.assertEqual([f["name"] for f in probe["types"]["EIP712Domain"]], ["name"])
        self.assertNotIn("Order", probe["types"])

    def test_an_order_typed_data_mismatch_at_start_disables_it(self):
        builder, key = builder_for(0)
        real = TD.build_order_typed_data

        def skewed(unsigned, **kw):
            typed = real(unsigned, **kw)
            typed["message"] = {**typed["message"], "salt": typed["message"]["salt"] + 1}
            return typed
        with patch.object(TD, "build_order_typed_data", skewed):
            note = builder.prove_fast_signing()
        self.assertIs(builder.fast_sign, False)
        self.assertIn("digest differs", note)
        got = builder.build_market_order(*market_args(random.Random(14), token=LIVE_TOKEN))
        self.assertEqual(got.signature, reference_signature(got, key))

    def test_a_deposit_wallet_settles_on_the_sdk_path(self):
        signer = S.fast_signer("0x" + secrets.token_hex(32), 137)
        funder = derive_uups_deposit_wallet_address(signer.address(), PRODUCTION_CONFIG.wallet_derivation)
        builder = C.OrderBuilder(signer, 3, funder)
        self.assertIn("signature type 3", builder.prove_fast_signing())
        self.assertIs(builder.fast_sign, False)
        counts = counted(builder)
        got = builder.build_market_order(*market_args(random.Random(15), token=LIVE_TOKEN))
        self.assertEqual(got.signature_type, 3)
        self.assertEqual((counts.typed, counts.digests), (1, 0))

    def test_the_proof_token_routes_like_the_live_markets_and_is_no_v2_position(self):
        self.assertFalse(is_v2_position_id(C.PROOF_TOKEN_ID))
        self.assertFalse(is_v2_position_id(LIVE_TOKEN))
        self.assertGreater(int(C.PROOF_TOKEN_ID), 10 ** 76)

    def test_a_builder_that_cannot_sign_reports_it_and_stays_unproven(self):
        signer = S.fast_signer("0x" + secrets.token_hex(32), 137)
        builder = C.OrderBuilder(signer, 2, "0x" + "12" * 20)                  # funder is not this key's safe
        note = builder.prove_fast_signing()
        self.assertIsNone(builder.fast_sign)
        self.assertIn("start-up signing proof failed", note)

    def test_live_and_paper_prove_before_their_first_order(self):
        src = inspect.getsource(L.build_live_execution)
        self.assertIn("prove_fast_signing()", src)
        self.assertIn("LIVE signing:", src)
        builder, clob = S.Sniper._make_builder()
        self.assertTrue(builder.b.fast_sign, builder.b.fast_sign_note)
        self.assertIn("signer", clob)


# ============================================================ the held key, and the gain
class HeldKeyAndCost(unittest.TestCase):
    def test_the_held_libsecp256k1_key_signs_what_eth_keys_and_eth_account_sign(self):
        key = "0x" + secrets.token_hex(32)
        held = S.fast_signer(key, 137)
        self.assertIsNotNone(held.secp_key)
        eth_keys_only = S.fast_signer(key, 137)
        eth_keys_only.secp_key = None
        plain = C.Signer(key, 137)
        for _ in range(300):
            digest = secrets.token_bytes(32)
            want = "0x" + bytes(Account.from_key(key).unsafe_sign_hash(digest).signature).hex()
            self.assertEqual(held.sign(digest), want)
            self.assertEqual(eth_keys_only.sign(digest), want)
            self.assertEqual(plain.sign(digest), want)
            self.assertEqual(held.sign("0x" + digest.hex()), want)

    def test_the_direct_path_costs_well_under_the_sdk_path(self):
        def median_us(fast: bool) -> float:
            with fast_sign_env("1" if fast else "0"):
                builder, _key = builder_for(0)
                builder.prove_fast_signing()
                rnd, times = random.Random(13), []
                for _ in range(160):
                    args, opts = market_args(rnd, token=LIVE_TOKEN)
                    t = time.perf_counter_ns()
                    builder.build_market_order(args, opts, version=2)
                    times.append((time.perf_counter_ns() - t) / 1000)
                return statistics.median(times[20:])
        sdk, fast = median_us(False), median_us(True)
        print(f"build+sign median: SDK path {sdk:.0f}us, direct path {fast:.0f}us", file=sys.stderr)
        self.assertLess(fast, sdk * 0.6)


if __name__ == "__main__":
    unittest.main(verbosity=1)
