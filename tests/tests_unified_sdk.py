"""Offline unified-SDK migration checks.

The unit tests use small typed-response stand-ins and explicit fake SDK
modules; they do not validate native cryptography or perform HTTP requests.
The final tests exercise the actual pinned SDK when it is installed and are
reported as skipped, rather than simulated, when it is unavailable.
"""
import testing_env  # noqa: F401 — isolate .env and forbid external network first

import ast
import base64
import dataclasses
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import importlib.metadata
import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

import clob_client as C
import clob_signing as S


ROOT = Path(__file__).resolve().parent.parent


class TypedValue:
    """Only the typed SDK model_dump protocol; never an SDK substitute."""

    def __init__(self, **fields):
        self.fields = fields

    def model_dump(self, *, mode):
        if mode != "python":
            raise AssertionError("decimal/timestamp types must remain available")
        return dict(self.fields)


def accepted(**changes):
    fields = dict(ok=True, order_id="order-1", status="matched",
                  making_amount=Decimal("5.10"), taking_amount=Decimal("10.20"),
                  trade_ids=("trade-1",), transactions_hashes=())
    fields.update(changes)
    return types.SimpleNamespace(**fields)


def client_for(secure):
    """Bypass credentials entirely and test the response bridge only."""
    client = object.__new__(C.ClobClient)
    client._initialize = Mock(return_value=secure)
    client.builder = types.SimpleNamespace(signature_type=0)
    return client


def post_fixture(response):
    """Mock only the SDK transport/helper boundary, without opening sockets."""
    build = Mock(return_value=("/order", {"official": "order-payload"}))
    validate = Mock()
    post_module = types.ModuleType("polymarket._internal.actions.orders.post")
    post_module.build_post_order_request = build
    response_module = types.ModuleType("polymarket.models.clob.order_response")
    response_module.RawOrderResponse = types.SimpleNamespace(parse_response=validate)
    secure = Mock()
    secure.credentials.key = "offline-key"
    secure._ctx.secure_clob.post_json.return_value = response
    return secure, build, validate, {post_module.__name__: post_module, response_module.__name__: response_module}


@dataclasses.dataclass(frozen=True)
class WireOrder:
    order_type: str = "GTC"
    post_only: bool = False
    side: str = "BUY"
    maker_amount: int = 5_100_000
    taker_amount: int = 10_200_000
    token_id: str = "123"
    signature_type: int = 0


class ResponseBridge(unittest.TestCase):
    def test_default_sell_exit_omits_zero_price_floor(self):
        secure = types.SimpleNamespace(create_market_order=Mock(return_value=object()))
        client = client_for(secure)
        result = client.create_market_order(S.MarketOrderArgs(
            token_id="123", amount=10, side=S.SELL, order_type=S.OrderType.FAK))
        self.assertIs(result, secure.create_market_order.return_value)
        secure.create_market_order.assert_called_once_with(
            token_id="123", side="SELL", order_type="FAK", shares=Decimal("10"))

    def test_default_buy_omits_zero_price_ceiling(self):
        secure = types.SimpleNamespace(create_market_order=Mock(return_value=object()))
        client_for(secure).create_market_order(S.MarketOrderArgs(
            token_id="123", amount=5, side=S.BUY, order_type=S.OrderType.FOK))
        secure.create_market_order.assert_called_once_with(
            token_id="123", side="BUY", order_type="FOK", amount=Decimal("5"))

    def test_explicit_market_price_bounds_are_preserved(self):
        for side, bound, amount_name in ((S.SELL, "min_price", "shares"),
                                        (S.BUY, "max_price", "amount")):
            with self.subTest(side=side):
                secure = types.SimpleNamespace(create_market_order=Mock(return_value=object()))
                client_for(secure).create_market_order(S.MarketOrderArgs(
                    token_id="123", amount=10, side=side, price=.25, order_type=S.OrderType.FAK))
                secure.create_market_order.assert_called_once_with(
                    token_id="123", side=side, order_type="FAK",
                    **{amount_name: Decimal("10"), bound: Decimal("0.25")})

    def test_invalid_market_price_bound_fails_before_sdk_or_submission(self):
        for side in (S.BUY, S.SELL):
            for price in (True, False, -.01, float("nan"), float("inf"),
                          float("-inf"), None, "invalid"):
                with self.subTest(side=side, price=price):
                    secure = types.SimpleNamespace(create_market_order=Mock())
                    client = client_for(secure)
                    with self.assertRaisesRegex(ValueError, "finite and nonnegative"):
                        client.create_market_order(S.MarketOrderArgs(
                            token_id="123", amount=10, side=side, price=price))
                    client._initialize.assert_not_called()
                    secure.create_market_order.assert_not_called()

    def test_collateral_remains_integer_base_units(self):
        secure = types.SimpleNamespace(get_balance_allowance=Mock(return_value=types.SimpleNamespace(
            balance=5_100_001, allowances={"exchange": 9_999_999_999})))
        response = client_for(secure).get_balance_allowance(C.BalanceAllowanceParams())
        self.assertEqual(response, {"balance": "5100001", "allowances": {"exchange": "9999999999"}})
        self.assertEqual(Decimal(response["balance"]) / 1_000_000, Decimal("5.100001"))
        secure.get_balance_allowance.assert_called_once_with(asset_type="COLLATERAL", token_id=None)

    def test_typed_execution_amounts_remain_dollars_and_shares(self):
        response = C._order_response(accepted())
        self.assertEqual(Decimal(response["makingAmount"]), Decimal("5.10"))
        self.assertEqual(Decimal(response["takingAmount"]), Decimal("10.20"))
        self.assertEqual(response["tradeIDs"], ["trade-1"])
        self.assertTrue(response["success"])

    def test_large_integral_typed_amounts_cannot_be_guessed_as_micro_units(self):
        from sniper_live import reply_totals

        response = C._order_response(accepted(making_amount=Decimal("1000"),
                                              taking_amount=Decimal("1250")))
        self.assertEqual(reply_totals(response), (1000.0, 1250.0))

    def test_account_precision_status_and_timestamp_aliases(self):
        when = datetime(2026, 10, 19, 12, 0, 1, 123456, tzinfo=timezone.utc)
        row = C._account_row(TypedValue(status="TRADE_STATUS_CONFIRMED", price=Decimal("0.50123456789"),
                                       size=Decimal("10.200001"), matched_at=when, updated_at=when,
                                       expires_at=None, maker_orders=(TypedValue(matched_amount=Decimal("1.25")),)))
        self.assertEqual(row["price"], "0.50123456789")
        self.assertEqual(row["size"], "10.200001")
        self.assertEqual(row["status"], "CONFIRMED")
        self.assertEqual(row["match_time"], when.timestamp())
        self.assertEqual(row["last_update"], when.timestamp())
        self.assertEqual(row["expiration"], "0")
        self.assertEqual(row["maker_orders"], [{"matched_amount": "1.25"}])

    def test_naive_account_timestamps_are_treated_as_utc(self):
        stamp = datetime(2026, 10, 19)
        self.assertEqual(C._wire(stamp), stamp.replace(tzinfo=timezone.utc).timestamp())

    def test_nonfinite_typed_amounts_are_refused(self):
        for number in (Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            with self.subTest(number=number), self.assertRaises(ValueError):
                C._order_response(accepted(making_amount=number))

    def test_explicit_no_fill_codes_are_the_only_unmatched_codes(self):
        for code in ("unmatched", "fak_not_filled", "fok_not_filled", "not_enough_balance"):
            with self.subTest(code=code):
                result = C._order_response(types.SimpleNamespace(ok=False, code=code, message="refused"))
                self.assertFalse(result["success"])
                self.assertEqual(result["status"], "unmatched" if "filled" in code or code == "unmatched" else "")
                self.assertEqual(result["tradeIDs"], [])

    def test_unknown_typed_outcome_is_not_a_definite_zero(self):
        for response in (object(), types.SimpleNamespace(ok=False, code="unknown", message="Unknown order failure")):
            with self.subTest(response=response), self.assertRaises(ValueError):
                C._order_response(response)

    def test_single_post_preserves_pending_and_never_mutates_original_order(self):
        response = {"success": True, "errorMsg": "", "orderID": "order-1", "status": "delayed"}
        secure, build, validate, modules = post_fixture(response)
        order = WireOrder()
        with patch.dict(sys.modules, modules):
            result = client_for(secure).post_order(order, "FAK")
        self.assertIs(result, response)
        self.assertEqual(result["status"], "delayed")
        secure._ctx.secure_clob.post_json.assert_called_once_with("/order", json={"official": "order-payload"})
        build.assert_called_once()
        submitted = build.call_args.args[0]
        self.assertEqual(submitted.order_type, "FAK")
        self.assertEqual(order.order_type, "GTC")
        self.assertIsNot(order, submitted)
        self.assertNotIn("makingAmount", result)
        self.assertEqual(validate.call_args.args[0]["makingAmount"], "0")

    def test_malformed_post_result_raises_without_retry_or_recovery_transaction(self):
        secure, _build, _validate, modules = post_fixture(None)
        with patch.dict(sys.modules, modules), self.assertRaises(ValueError):
            client_for(secure).post_order(WireOrder(), "FAK")
        self.assertEqual([call[0] for call in secure.mock_calls], ["_ctx.secure_clob.post_json"])

    def test_conflicting_raw_post_evidence_remains_ambiguous(self):
        import polymarket_trade as pt
        import sniper_live as L

        response = {"success": False, "errorMsg": "refused", "orderID": "order-1", "status": "matched",
                    "tradeIDs": ["trade-1"], "takingAmount": "10.20", "makingAmount": "5.10"}
        secure, _build, _validate, modules = post_fixture(response)
        with patch.dict(sys.modules, modules):
            result = client_for(secure).post_order(WireOrder(), "FAK")
        classifier = object.__new__(L.LeanSender)
        classifier.pt = pt
        self.assertIs(result, response)
        self.assertEqual(classifier._classify(200, result, "")[0], L.AMBIGUOUS)
        self.assertEqual([call[0] for call in secure.mock_calls], ["_ctx.secure_clob.post_json"])

    def test_unknown_accepted_status_keeps_order_id_for_reconciliation(self):
        import polymarket_trade as pt
        import sniper_live as L

        response = {"success": True, "errorMsg": "", "orderID": "order-1", "status": "future-status"}
        secure, _build, _validate, modules = post_fixture(response)
        with patch.dict(sys.modules, modules):
            result = client_for(secure).post_order(WireOrder(), "FAK")
        classifier = object.__new__(L.LeanSender)
        classifier.pt = pt
        self.assertEqual(classifier._classify(200, result, "")[0], L.PENDING)
        self.assertEqual(result["orderID"], "order-1")
        self.assertNotIn("tradeIDs", result)

    def test_failed_sdk_shape_validation_never_repeats_post(self):
        secure, _build, validate, modules = post_fixture({"status": "malformed"})
        validate.side_effect = ValueError("unexpected response")
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(ValueError, "unexpected response"):
            client_for(secure).post_order(WireOrder(), "FAK")
        self.assertEqual([call[0] for call in secure.mock_calls], ["_ctx.secure_clob.post_json"])

    def test_truncated_account_history_cannot_be_used_as_complete_evidence(self):
        page = types.SimpleNamespace(items=(TypedValue(id="trade-1"),), limit_reached=True)
        with self.assertRaisesRegex(ValueError, "completeness is unknown"):
            C.ClobClient._rows(iter((page,)))

    def test_all_history_pages_and_typed_aliases_are_retained(self):
        pages = [types.SimpleNamespace(items=(TypedValue(id=f"trade-{i}", status="TRADE_STATUS_FAILED"),),
                                       limit_reached=False) for i in range(2)]
        self.assertEqual(C.ClobClient._rows(iter(pages)),
                         [{"id": "trade-0", "status": "FAILED"},
                          {"id": "trade-1", "status": "FAILED"}])

    def test_explicit_first_page_is_distinct_from_complete_history(self):
        paginator = types.SimpleNamespace(first_page=Mock(return_value=types.SimpleNamespace(
            items=(TypedValue(id="one"),), limit_reached=True)))
        self.assertEqual(C.ClobClient._rows(paginator, only_first_page=True), [{"id": "one"}])

    def test_reconciliation_filters_and_cutoffs_are_preserved(self):
        secure = types.SimpleNamespace(list_account_trades=Mock(return_value=iter(())))
        self.assertEqual(client_for(secure).get_trades(C.TradeParams(
            id="trade", maker_address="wallet", market="condition", asset_id="token", before=30, after=20)), [])
        secure.list_account_trades.assert_called_once_with(
            id="trade", maker_address="wallet", market="condition", asset_id="token", before="30", after="20")


def fake_sdk_modules():
    """Explicit unit-only stand-ins; they never claim native SDK parity."""
    calls = types.SimpleNamespace()
    config = types.SimpleNamespace(chain_id=137, wallet_derivation="wallet-rules")
    wallet = types.SimpleNamespace(wallet_type="EOA", signer_type="OWNER")
    unsigned = types.SimpleNamespace(chain_id=137, protocol_version="3")
    modules = {}

    def module(name, **attributes):
        value = types.ModuleType(name)
        value.__dict__.update(attributes)
        modules[name] = value
        return value

    for name in ("polymarket", "polymarket._internal", "polymarket._internal.actions", "polymarket._internal.actions.orders"):
        module(name, __path__=[])
    calls.asset = Mock(return_value="123")
    calls.exchange = Mock(return_value="exchange-v3")
    calls.rounding = Mock()
    calls.grid = Mock()
    calls.market_amounts = Mock(return_value=(5_100_000, 10_200_000))
    calls.limit_amounts = Mock(return_value=(2_550_000, 5_100_000))
    calls.unsigned = Mock(return_value=unsigned)
    calls.typed_data = Mock(return_value={"typed": "official-sdk"})
    calls.signature = Mock(side_effect=lambda order, signature: signature)
    calls.wrap = Mock(side_effect=lambda **kwargs: kwargs["signature"])
    calls.signed = Mock(return_value=WireOrder())
    calls.classify = Mock(return_value=wallet)
    module("polymarket._internal.environment", PRODUCTION_CONFIG=config)
    module("polymarket._internal.actions.exchange_asset", resolve_asset_id=calls.asset)
    module("polymarket._internal.actions.orders.context", resolve_order_exchange_address=calls.exchange,
           resolve_rounding_config=calls.rounding, validate_price_on_tick_grid=calls.grid,
           MIN_SUPPORTED_TICK_SIZE=Decimal("0.0001"))
    module("polymarket._internal.actions.orders.market", _compute_market_order_amounts=calls.market_amounts)
    module("polymarket._internal.actions.orders.limit", _compute_limit_order_amounts=calls.limit_amounts)
    module("polymarket._internal.actions.orders.orders", create_unsigned_order=calls.unsigned,
           create_signed_order=calls.signed)
    module("polymarket._internal.actions.orders.typed_data", build_order_typed_data=calls.typed_data,
           build_order_signature=calls.signature)
    module("polymarket._internal.actions.orders.types", OrderDraft=lambda **kwargs: types.SimpleNamespace(**kwargs))
    module("polymarket._internal.validation", validate_builder_code=lambda code: code)
    module("polymarket._internal.wallet", classify_account=calls.classify,
           signature_type_for=lambda _wallet: 0, wrap_deposit_wallet_signature=calls.wrap)
    module("polymarket.types", EvmAddress=str, HexString=str)
    return modules, calls


class SigningDelegationUnit(unittest.TestCase):
    def signer(self):
        return types.SimpleNamespace(address=lambda: "0x" + "11" * 20, get_chain_id=lambda: 137,
                                     sign_typed_data=Mock(return_value=types.SimpleNamespace(signature=b"\x22" * 65)))

    def test_market_buy_and_sell_use_official_amounts_asset_and_signature_helpers(self):
        for side in (S.BUY, S.SELL):
            with self.subTest(side=side):
                modules, calls = fake_sdk_modules()
                signer = self.signer()
                builder = S.OrderBuilder(signer)
                with patch.dict(sys.modules, modules), patch.object(S, "_unsigned_order_hash", return_value="0xhash"):
                    signed = builder.build_market_order(
                        S.MarketOrderArgsV2("123", amount=5.10, side=side, price=.500000001, order_type="FAK"),
                        S.CreateOrderOptions("0.01", False))
                calls.asset.assert_called_once_with(asset_id=None, token_id="123")
                calls.market_amounts.assert_called_once_with(amount=Decimal("5.1"), price=Decimal("0.50"),
                                                             side=side, tick_size=Decimal("0.01"))
                calls.limit_amounts.assert_not_called()
                draft = calls.unsigned.call_args.args[0]
                self.assertEqual((draft.offered_amount, draft.requested_amount, draft.exchange_address),
                                 (5_100_000, 10_200_000, "exchange-v3"))
                signer.sign_typed_data.assert_called_once_with(full_message={"typed": "official-sdk"})
                calls.signature.assert_called_once()
                calls.wrap.assert_called_once()
                calls.signed.assert_called_once()
                self.assertEqual(signed.order_hash, "0xhash")

    def test_limit_amounts_and_expiration_use_official_helpers(self):
        modules, calls = fake_sdk_modules()
        builder = S.OrderBuilder(self.signer())
        with patch.dict(sys.modules, modules), patch.object(S, "_unsigned_order_hash", return_value="0xhash"):
            builder.build_order(S.OrderArgsV2("123", .5, 5.1, "BUY", expiration=1_900_000_000),
                                S.CreateOrderOptions("0.01", True))
        calls.limit_amounts.assert_called_once_with(price=Decimal("0.5"), size=Decimal("5.1"),
                                                    side="BUY", tick_size=Decimal("0.01"))
        calls.market_amounts.assert_not_called()
        draft = calls.unsigned.call_args.args[0]
        self.assertEqual((draft.expiration, draft.order_type), (1_900_000_000, "GTD"))
        self.assertTrue(calls.exchange.call_args.kwargs["neg_risk"])

    def test_incompatible_wallet_type_cannot_sign(self):
        modules, calls = fake_sdk_modules()
        builder = S.OrderBuilder(self.signer(), signature_type=2)
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(ValueError, "does not match"):
            builder.build_order(S.OrderArgsV2("123", .5, 5, "BUY"), S.CreateOrderOptions("0.01", False))
        calls.unsigned.assert_not_called()

    def test_zero_rounded_amount_cannot_produce_a_signed_order(self):
        modules, calls = fake_sdk_modules()
        calls.market_amounts.return_value = (1, 0)
        builder = S.OrderBuilder(self.signer())
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(ValueError, "zero order amount"):
            builder.build_market_order(S.MarketOrderArgsV2("123", 0.000001, "BUY", price=.5),
                                       S.CreateOrderOptions("0.01", False))
        calls.unsigned.assert_not_called()

    def test_coarse_tick_buy_cannot_cross_a_finer_ask_after_share_rounding(self):
        # These are the SDK's rounded amounts for $4.56 at .9 on a .1 tick:
        # 4.56 / 5.066 = .900118..., which could lift a later .9001 ask.
        modules, calls = fake_sdk_modules()
        calls.market_amounts.return_value = (4_560_000, 5_066_000)
        signer = self.signer()
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(ValueError, "Cannot preserve max_price"):
            S.OrderBuilder(signer).build_market_order(
                S.MarketOrderArgsV2("123", 4.56, "BUY", price=.9, order_type="FOK"),
                S.CreateOrderOptions("0.1", False))
        calls.unsigned.assert_not_called()
        signer.sign_typed_data.assert_not_called()

    def test_market_buy_exactly_at_next_finest_ask_is_rejected(self):
        modules, calls = fake_sdk_modules()
        calls.market_amounts.return_value = (9_001_000, 10_000_000)
        signer = self.signer()
        with patch.dict(sys.modules, modules), self.assertRaisesRegex(ValueError, "Cannot preserve max_price"):
            S.OrderBuilder(signer).build_market_order(
                S.MarketOrderArgsV2("123", 9.001, "BUY", price=.9),
                S.CreateOrderOptions("0.1", False))
        calls.unsigned.assert_not_called()
        signer.sign_typed_data.assert_not_called()

    def test_market_buy_strictly_below_next_finest_ask_can_sign(self):
        modules, calls = fake_sdk_modules()
        calls.market_amounts.return_value = (9_000_999, 10_000_000)
        signer = self.signer()
        with patch.dict(sys.modules, modules), patch.object(S, "_unsigned_order_hash", return_value="0xhash"):
            S.OrderBuilder(signer).build_market_order(
                S.MarketOrderArgsV2("123", 9.000999, "BUY", price=.9),
                S.CreateOrderOptions("0.1", False))
        calls.unsigned.assert_called_once()
        signer.sign_typed_data.assert_called_once()

    def test_order_wire_is_delegated_and_original_frozen_order_is_untouched(self):
        official = Mock(return_value=("/order", {"official": "payload"}))
        module = types.ModuleType("polymarket._internal.actions.orders.post")
        module.build_post_order_request = official
        raw = WireOrder()
        wrapped = S.SignedOrder(raw, object(), "0xhash")
        with patch.dict(sys.modules, {module.__name__: module}):
            self.assertEqual(S.order_to_json_v2(wrapped, "api-key", "FAK"), {"official": "payload"})
        official.assert_called_once()
        self.assertEqual(official.call_args.kwargs, {"owner_api_key": "api-key"})
        self.assertEqual(official.call_args.args[0].order_type, "FAK")
        self.assertEqual(raw.order_type, "GTC")


class AuthenticationBridgeUnit(unittest.TestCase):
    def test_cached_server_stamp_and_exact_serialized_body_reach_official_hmac(self):
        secret = base64.urlsafe_b64encode(b"offline-secret-only").decode().rstrip("=")
        body = '{"label":"é","amount":"5.10"}'
        stamp = 1_900_000_000
        expected = base64.urlsafe_b64encode(hmac.new(b"offline-secret-only",
            f"{stamp}POST/order{body}".encode(), hashlib.sha256).digest()).decode()
        official = Mock(return_value=expected)
        module = types.ModuleType("polymarket._internal.hmac")
        module.build_hmac_signature = official
        with patch.dict(sys.modules, {module.__name__: module}):
            headers = S.create_level_2_headers(types.SimpleNamespace(address=lambda: "offline-wallet"),
                S.ApiCreds("offline-key", secret, "offline-passphrase"),
                S.RequestArgs("POST", "/order", body={"wrong": "body"}, serialized_body=body), stamp)
        official.assert_called_once_with(secret=secret, timestamp=stamp, method="POST", path="/order", body=body)
        self.assertEqual(headers["POLY_SIGNATURE"], expected)
        self.assertEqual(headers["POLY_TIMESTAMP"], str(stamp))

    def test_invalid_clock_stamp_never_reaches_authentication(self):
        official = Mock()
        module = types.ModuleType("polymarket._internal.hmac")
        module.build_hmac_signature = official
        for stamp in (True, False, 0, -1, 1.25, float("nan")):
            with self.subTest(stamp=stamp), patch.dict(sys.modules, {module.__name__: module}), self.assertRaises(ValueError):
                S.create_level_2_headers(types.SimpleNamespace(address=lambda: "offline-wallet"),
                    S.ApiCreds("offline-key", "c2VjcmV0", "offline-passphrase"), S.RequestArgs("POST", "/order"), stamp)
        official.assert_not_called()


class DependencyContract(unittest.TestCase):
    def test_supported_sdk_is_pinned_without_legacy_dependency(self):
        requirements = (ROOT / "requirements.txt").read_text()
        lock = (ROOT / "requirements.lock.txt").read_text()
        active = "\n".join(line.split("#", 1)[0].strip() for line in requirements.splitlines())
        self.assertIn("polymarket-client==0.13.0", active)
        self.assertIn("polymarket-client==0.13.0", lock)
        self.assertNotIn("py-clob-client", active)
        self.assertIn("websockets>=13.0,<16", active)

    def test_production_modules_do_not_import_the_retired_sdk(self):
        old_imports = []
        for path in sorted(ROOT.rglob("*.py")):
            if (path.name.startswith("tests_") or any(part in {"data", "review_artifacts", ".venv", ".git", "__pycache__"}
                                                       for part in path.relative_to(ROOT).parts)):
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                names = ([node.module or ""] if isinstance(node, ast.ImportFrom)
                         else [item.name for item in node.names] if isinstance(node, ast.Import) else [])
                if any(name == "py_clob_client_v2" or name.startswith("py_clob_client_v2.") for name in names):
                    old_imports.append(f"{path.relative_to(ROOT)}:{node.lineno}")
        self.assertEqual(old_imports, [])


def actual_sdk_available():
    if importlib.util.find_spec("polymarket") is None:
        return False
    try:
        return importlib.metadata.version("polymarket-client") == "0.13.0"
    except importlib.metadata.PackageNotFoundError:
        return False


@unittest.skipUnless(actual_sdk_available(), "actual polymarket-client==0.13.0 is not installed; native SDK parity is unverified")
class ActualSdkOfflineParity(unittest.TestCase):
    """Real pinned SDK and crypto dependencies; still no HTTP or credentials."""

    def unsigned(self, *, protocol_version="2"):
        from polymarket._internal.actions.orders.types import UnsignedOrder

        return UnsignedOrder(chain_id=137, builder=S.BYTES32_ZERO, exchange_address="0x" + "33" * 20,
                             expiration=0, maker="0x" + "11" * 20, maker_amount=5_100_000,
                             metadata=S.BYTES32_ZERO, order_type="FAK", salt=42, side="BUY", signature_type=0,
                             signer="0x" + "11" * 20, taker_amount=10_200_000, timestamp=1_900_000_000_123,
                             token_id="123", protocol_version=protocol_version)

    def test_actual_sdk_signed_order_wire_contract(self):
        from polymarket._internal.actions.orders.orders import create_signed_order

        sdk_order = create_signed_order(self.unsigned(), "0x" + "22" * 65)
        payload = S.order_to_json_v2(sdk_order, "offline-api-key", "FAK")
        self.assertEqual(payload, {"deferExec": False, "owner": "offline-api-key", "orderType": "FAK", "order": {
            "builder": S.BYTES32_ZERO, "expiration": "0", "maker": "0x" + "11" * 20,
            "makerAmount": "5100000", "metadata": S.BYTES32_ZERO, "salt": 42, "side": "BUY",
            "signature": "0x" + "22" * 65, "signatureType": 0, "signer": "0x" + "11" * 20,
            "takerAmount": "10200000", "timestamp": "1900000000123", "tokenId": "123"}})

    def test_actual_sdk_v2_and_v3_hash_match_eth_account_typed_data(self):
        from eth_account.messages import encode_typed_data
        from eth_utils.crypto import keccak
        from polymarket._internal.actions.orders.typed_data import build_order_typed_data

        hashes = []
        for version in ("2", "3"):
            with self.subTest(protocol_version=version):
                unsigned = self.unsigned(protocol_version=version)
                message = encode_typed_data(full_message=build_order_typed_data(unsigned))
                expected = "0x" + keccak(b"\x19" + message.version + message.header + message.body).hex()
                self.assertEqual(S._unsigned_order_hash(unsigned), expected)
                hashes.append(expected)
        self.assertNotEqual(*hashes)

    def test_actual_sdk_hmac_matches_independent_stdlib_oracle(self):
        secret = base64.urlsafe_b64encode(b"offline-secret-only").decode().rstrip("=")
        body = '{"label":"é","amount":"5.10"}'
        args = S.RequestArgs("POST", "/order", body={"ignored": True}, serialized_body=body)
        creds = S.ApiCreds("offline-key", secret, "offline-passphrase")
        stamp = 1_900_000_000
        expected = base64.urlsafe_b64encode(hmac.new(b"offline-secret-only",
            f"{stamp}POST/order{body}".encode(), hashlib.sha256).digest()).decode()
        result = S.create_level_2_headers(types.SimpleNamespace(address=lambda: "offline-wallet"), creds, args, stamp)
        self.assertEqual(result["POLY_SIGNATURE"], expected)
        self.assertEqual(result["POLY_TIMESTAMP"], str(stamp))


if __name__ == "__main__":
    unittest.main()
