#!/usr/bin/env python3
"""FAK orders sized from whole shares times their signed limit.

    python tests_f1_share_sized.py

The recorded Oct 14 incident bought 12.896552 shares at .29 for an order
sized as 11 shares times a .34 limit ($3.74). The signed maker stake is the
principal bound; the taker amount is not a maximum share fill. Price
improvement may increase quantity, and only a dollar-budget overfill halts.

The pure paper quote tests compare the maker-budget walk against a separate
oracle. Signing/live fixture cases additionally need the installed native
SDK, cryptographic dependencies and local test sockets. They make no venue
calls. tests_fak_budget_parity.py covers the incident with SDK-free fixtures.
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import random
import secrets
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from decimal import Decimal as D

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from paper_trade import (BookSnapshot, MarketRules, PaperRejected, estimate_fak_shares)  # noqa: E402


class SignedAmounts(unittest.TestCase):
    def test_whole_shares_at_a_cent_price_sign_exact_amounts(self):
        """Every share count 5..300 at every cent price: takerAmount is the shares and
        makerAmount is shares x price, to the micro-dollar - the library's own rounding."""
        from clob_signing import ROUNDING_CONFIG, OrderBuilder
        from clob_signing import BUY
        bad = []
        for shares in range(5, 301):
            for cents in range(1, 100):
                price = float(S.signing_price(cents / 100, "0.01"))
                _side, maker, taker = OrderBuilder.get_order_amounts(None, BUY, float(shares), price,
                                                                      ROUNDING_CONFIG["0.01"])
                if (int(taker), int(maker)) != (shares * 1_000_000, shares * cents * 10_000):
                    bad.append((shares, cents, maker, taker))
        self.assertEqual(bad, [])

    def test_the_fast_builder_signs_the_librarys_limit_order(self):
        from eth_account import Account
        from clob_sdk import CreateOrderOptions, OrderArgsV2
        from clob_signing import OrderBuilder
        from clob_signing import BUY
        from eth_account.messages import encode_typed_data
        from eth_utils import keccak
        from polymarket._internal.actions.orders.typed_data import build_order_signature, build_order_typed_data
        from polymarket._internal.environment import PRODUCTION_CONFIG
        from polymarket._internal.wallet import derive_proxy_wallet_address, derive_safe_wallet_address
        from clob_signing import Signer
        rnd = random.Random(20261014)
        for i in range(60):
            key = "0x" + secrets.token_hex(32)
            signer = L.fast_signer(key, 137) if i % 2 == 0 else Signer(key, 137)        # both signer kinds
            sig_type = rnd.choice([0, 1, 2])
            funder = (None if sig_type == 0 else
                      (derive_proxy_wallet_address if sig_type == 1 else derive_safe_wallet_address)(
                          signer.address(), PRODUCTION_CONFIG.wallet_derivation))
            lib = OrderBuilder(signer, sig_type, funder)
            fast = S.FastOrderBuilder(OrderBuilder(signer, sig_type, funder))
            args = OrderArgsV2(token_id=str(rnd.randrange(10 ** 70, 10 ** 77)), price=rnd.randrange(5, 96) / 100,
                               size=float(rnd.randrange(5, 200)), side=BUY)
            opts = CreateOrderOptions(tick_size="0.01", neg_risk=rnd.random() < 0.3)
            got = fast.build_order(args, opts, version=2)
            ref = lib.build_order(args, opts, version=2)
            for f in ("maker", "signer", "tokenId", "makerAmount", "takerAmount", "side", "signatureType",
                      "metadata", "builder", "expiration"):
                self.assertEqual(getattr(got, f), getattr(ref, f), (i, f))
            typed = build_order_typed_data(got.unsigned_order)
            message = encode_typed_data(full_message=typed)
            raw = signer.account.sign_message(message).signature.hex()
            want = build_order_signature(got.unsigned_order, "0x" + raw.removeprefix("0x"))
            self.assertEqual(got.signature, want, i)
            digest = "0x" + keccak(b"\x19" + message.version + message.header + message.body).hex()
            self.assertEqual(fast.last_order_hash, digest, i)
            self.assertEqual(Account.recover_message(message, signature=got.signature).lower(),
                             signer.address().lower())

    def test_all_limit_orders_delegate_to_the_sdk_adapter(self):
        from clob_sdk import CreateOrderOptions, OrderArgsV2
        from clob_signing import OrderBuilder
        from clob_signing import BUY
        calls = []

        class Spy(OrderBuilder):
            def build_order(self, *a, **k):
                calls.append(k.get("version"))
                return super().build_order(*a, **k)
        signer = L.fast_signer("0x" + secrets.token_hex(32), 137)
        args = OrderArgsV2(token_id="123", price=0.5, size=10.0, side=BUY)
        opts = CreateOrderOptions(tick_size="0.01", neg_risk=False)
        S.FastOrderBuilder(Spy(signer)).build_order(args, opts, version=2)
        try:
            S.FastOrderBuilder(Spy(signer, 3, "0x" + secrets.token_hex(20))).build_order(args, opts, version=2)
        except Exception:  # noqa: BLE001 - whatever the library does with it, it was the library
            pass
        self.assertEqual(calls, [2, 2])


class LiveShareSized(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import tests_sniper_live as T
        self.T = T
        T.reset_ambiguity()
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()

    async def asyncTearDown(self):
        ex = getattr(self, "ex", None)
        if ex is not None:
            for task in list(ex._order_tasks):
                task.cancel()
            await asyncio.gather(*ex._order_tasks, return_exceptions=True)
            await ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        self.T.reset_ambiguity()

    async def canary(self, **kw):
        self.ex, self.ledger, self.store, _, _ = await self.T.make_canary(self.fake, self.tmp.name, **kw)
        return self.ex

    def plan(self, shares, side="UP", cap=0.52, asks=((0.50, 30),)):
        T = self.T
        book = T.live_book(asks=asks)
        view = book.view(T.UP if side == "UP" else T.DN)
        return dict(T.live_plan(view, side=side, cap=cap, amount=shares * 0.5), order_type="FAK", shares=shares)

    async def test_signs_exactly_the_shares_and_reserves_the_worst_case(self):
        ex = await self.canary()
        p = ex.prepare(self.plan(10))
        self.assertEqual((p.shares, p.amount, p.order_type), (10.0, 5.2, "FAK"))
        order = json.loads(p.body)["order"]
        self.assertEqual((int(order["takerAmount"]), int(order["makerAmount"])), (10_000_000, 5_200_000))
        self.assertAlmostEqual(p.reserve, 5.2 + 5.2 * 0.07, places=9)       # the whole signed spend + a fee bound
        self.assertAlmostEqual(ex.reserved, p.reserve, places=9)
        self.assertEqual(ex._risk_orders[p.client_order_id]["shares"], 10.4)

    async def test_the_fast_builder_path_signs_the_same_order(self):
        from clob_signing import OrderBuilder
        ex = await self.canary()
        ex.sender.builder = S.FastOrderBuilder(OrderBuilder(ex.sender.signer))
        p = ex.prepare(self.plan(9, cap=0.55))
        order = json.loads(p.body)["order"]
        self.assertEqual((int(order["takerAmount"]), int(order["makerAmount"])), (9_000_000, 4_950_000))
        self.assertEqual(p.order_hash, ex.sender.builder.last_order_hash)
        self.assertIsNotNone(p.stamps.get("sign_start_pc"))

    async def test_refuses_fractional_sub_minimum_and_over_cap_sizes(self):
        ex = await self.canary(limits=L.CanaryLimits(max_order_shares=8.0))
        for shares, why in ((10.5, "whole shares"), (4, "minimum"), (10, "canary cap")):
            with self.assertRaises(L.Refused) as cm:
                ex.prepare(self.plan(shares))
            self.assertIn(why, str(cm.exception), shares)
        self.assertEqual(ex.reserved, 0.0)
        self.assertEqual(self.fake.hits[("POST", "/order")], 0)

    async def test_price_improvement_above_the_sizing_target_is_accepted(self):
        T = self.T
        ex = await self.canary()
        self.fake.script.append(("json", 200, T.MATCHED_REPLY))
        await ex.send(ex.prepare(self.plan(10)))
        # 11 shares at 0.40 = $4.40: inside the dollar budget, above the target.
        self.store.record_trade("trade-1", order_id="0xorder1", asset_id=T.UP, market=T.COND, side="BUY",
                                price=0.40, size=11, status="MATCHED")
        fill = ex.fill_for("0xorder1")
        self.assertIsNotNone(fill)
        self.assertEqual((fill['shares'], fill['cost']), (11, 4.4))
        self.assertFalse(ex.kill.tripped, ex.kill.reasons)

    async def test_a_fill_at_a_better_price_counts_its_real_cost(self):
        T = self.T
        ex = await self.canary()
        self.fake.script.append(("json", 200, T.MATCHED_REPLY))
        await ex.send(ex.prepare(self.plan(10)))
        self.store.record_trade("trade-1", order_id="0xorder1", asset_id=T.UP, market=T.COND, side="BUY",
                                price=0.45, size=10, status="MATCHED")
        fill = ex.fill_for("0xorder1")
        self.assertIsNotNone(fill)
        self.assertEqual((fill["shares"], round(fill["cost"], 6)), (10.0, 4.5))
        self.assertFalse(ex.kill.tripped)

    async def test_a_partial_fill_is_kept_with_the_venues_fee_curve(self):
        T = self.T
        ex = await self.canary()
        self.fake.script.append(("json", 200, T.MATCHED_REPLY))
        await ex.send(ex.prepare(self.plan(10)))
        self.store.record_trade("trade-1", order_id="0xorder1", asset_id=T.UP, market=T.COND, side="BUY",
                                price=0.50, size=6, status="MATCHED")
        fill = ex.fill_for("0xorder1")
        self.assertEqual((fill["shares"], round(fill["cost"], 6)), (6.0, 3.0))        # 6 of 10: kept, not refused
        self.assertAlmostEqual(fill["fee"], 6 * 0.07 * 0.5 * 0.5, places=6)          # shares x rate x p(1-p)
        self.assertFalse(ex.kill.tripped)

    async def test_a_zero_fill_releases_its_reservation_and_risk(self):
        ex = await self.canary()
        self.fake.script.append(("json", 400, {"success": False, "errorMsg": "no orders found to match with FAK "
                                               "order. FAK orders are partially filled or killed if no match "
                                               "is found."}))
        p = ex.prepare(self.plan(10))
        res = await ex.send(p)
        self.assertEqual(res["kind"], L.NO_MATCH)
        self.assertEqual((ex.reserved, ex._risk_orders), (0.0, {}))
        self.assertFalse(ex.kill.tripped)

    async def test_a_lost_fill_notification_keeps_the_risk_and_halts_at_the_deadline(self):
        """Scenario 12: the venue says MATCHED with a trade id; the private
        channel never delivers that trade. Nothing is released on a guess: the
        order's worst case stays reserved, and the reconciler halts at the
        confirmation deadline for a reconcile by hand."""
        T = self.T
        ex = await self.canary()
        ex.order_status = None                                   # no venue status record either
        before = ex.balance
        self.fake.script.append(("json", 200, T.MATCHED_REPLY))
        p = ex.prepare(self.plan(10))
        res = await ex.send(p)
        self.assertEqual(res["kind"], L.MATCHED)
        for _ in range(300):                                     # the receipt is authorized on a worker
            a = ex.awaiting.get("0xorder1")
            if a is not None and a.get("authorized") is not False:
                break
            await asyncio.sleep(0.01)
        self.assertIsNotNone(ex.awaiting.get("0xorder1"))
        self.assertIsNone(ex.fill_for("0xorder1"))               # no trade has arrived
        ex.reconcile_once()
        self.assertFalse(ex.kill.tripped)                        # inside the deadline: waiting, at risk
        self.assertIn(p.client_order_id, ex._risk_orders)
        self.assertAlmostEqual(ex.balance, before - p.reserve, places=9)        # its worst case is spent, or may be
        ex.awaiting["0xorder1"]["wall"] -= ex.confirm_within_s + 1
        ex.reconcile_once()
        self.assertTrue(ex.kill.tripped)
        self.assertIn("not completely confirmed", "; ".join(ex.kill.reasons))
        self.assertIn(p.client_order_id, ex._risk_orders)        # still counted after the halt
        self.assertAlmostEqual(ex.balance, before - p.reserve, places=9)
        self.assertEqual(self.fake.hits[("POST", "/order")], 1)  # and nothing re-sent

    async def test_a_delayed_reply_keeps_the_order_at_risk_until_its_outcome(self):
        ex = await self.canary()
        before = ex.balance
        self.fake.script.append(("json", 200, {"success": True, "status": "delayed", "orderID": "0xdelayed",
                                               "errorMsg": ""}))
        p = ex.prepare(self.plan(10))
        res = await ex.send(p)
        self.assertEqual(res["kind"], L.PENDING)
        self.assertIn(p.client_order_id, ex._risk_orders)                             # its worst case still counts
        self.assertAlmostEqual(ex.balance, before - p.reserve, places=9)               # spent, or may be
        self.assertFalse(ex.kill.tripped)

    async def test_a_tenth_of_a_cent_tick_signs_whole_cents_or_refuses(self):
        T = self.T
        ex = await self.canary()
        s = ex.sender
        rules = dict(T.rules_fetch(T.COND, T.UP, T.DN, tick="0.001"))
        kw = dict(side="UP", token=T.UP, up=T.UP, down=T.DN, condition=T.COND, end=T.round_end(),
                  amount=0.0, cap=0.055, order_type="FAK")
        with patch.object(s.rules, "get", lambda *a, **k: rules):
            p = s.prepare(shares=10, **kw)                                             # 10 x 0.055 = $0.55
            order = json.loads(p.body)["order"]
            self.assertEqual((int(order["takerAmount"]), int(order["makerAmount"])), (10_000_000, 550_000))
            with self.assertRaisesRegex(L.Refused, "whole-cent"):
                s.prepare(shares=7, **kw)                                              # $0.385: never signed
        self.assertEqual(self.fake.hits[("POST", "/order")], 0)

    async def test_reservation_is_exactly_the_signed_worst_case_on_random_orders(self):
        """Signed amounts and reservation, over many sizes and limits: the venue's
        contract can take at most makerAmount (+ its fee) from a BUY - which is
        exactly shares x limit, and the reservation covers it plus a fee bound."""
        ex = await self.canary(limits=L.CanaryLimits(max_order_shares=200.0, max_exposure_usd=1e6, max_orders=10_000),
                               balance=10_000.0)                 # caps out of the way: only the amounts
        ex.sender.limits.max_daily_loss_usd = 10_000.0
        rng = random.Random(20261014)
        for i in range(60):
            cap = rng.randint(6, 94) / 100
            # the decision's floors: the venue's 5 shares and its $1 marketable minimum
            shares = max(rng.randint(5, 60), int((D(1) / D(str(cap))).to_integral_value(rounding="ROUND_CEILING")))
            ask = max(0.05, round(cap - 0.03, 2))
            book = self.T.live_book(asks=((ask, 500),), bids=((round(ask - 0.01, 2), 500),))
            p = ex.prepare(dict(self.T.live_plan(book.view(self.T.UP), side="UP", cap=cap, amount=shares * ask),
                                order_type="FAK", shares=shares))
            order = json.loads(p.body)["order"]
            spend = D(shares) * D(str(cap))
            self.assertEqual(int(order["takerAmount"]), shares * 1_000_000, i)
            self.assertEqual(D(order["makerAmount"]) / D(1_000_000), spend, i)
            fee_bound = max(D(str(p.estimate["fee"])), spend * D("0.07"))
            self.assertAlmostEqual(p.reserve, float(spend + fee_bound), places=9)
            # the venue's own fee on a BUY is rate x p(1-p) per share <= rate x p: inside the bound
            self.assertLessEqual(D(shares) * D("0.07") * D(str(cap)) * (1 - D(str(cap))), fee_bound)
            ex.reserved -= p.reserve                                    # next order from a clean slate
            ex._risk_orders.pop(p.client_order_id, None)
        self.assertEqual(self.fake.hits[("POST", "/order")], 0)

    async def test_up_and_down_in_flight_each_reserve_their_own_worst_case(self):
        ex = await self.canary(balance=12.0)
        up = ex.prepare(self.plan(10, side="UP", cap=0.52))
        self.assertAlmostEqual(ex.reserved, 5.2 * 1.07, places=9)
        down = ex.prepare(dict(self.plan(10, side="DOWN", cap=0.53, asks=((0.50, 30),)), order_type="FAK"))
        self.assertAlmostEqual(ex.reserved, up.reserve + down.reserve, places=9)
        # Each side's inventory estimate is its own signed stake at its own best
        # ask: UP $5.20 / 0.50 and DOWN $5.30 / 0.51 (live_book's DOWN asks 0.51).
        got = sorted(r["shares"] for r in ex._risk_orders.values())
        want = sorted([5.2 / 0.50, 5.3 / 0.51])
        self.assertEqual(len(got), 2)
        for g, w in zip(got, want):
            self.assertAlmostEqual(g, w, places=9)
        # $12 holds both worst cases ($11.24) but not a third
        with self.assertRaises(L.Refused) as cm:
            ex.prepare(self.plan(5, side="UP", cap=0.52))
        self.assertIn("collateral", str(cm.exception))


def brute(asks, shares, cap):
    left, got, cost = D(shares) * cap, D(0), D(0)
    for price, size in sorted(asks):
        if price > cap or left <= 0:
            break
        spend = min(left, size * price)
        got, cost, left = got + spend / price, cost + spend, left - spend
    return got, cost


class PaperShareEstimate(unittest.TestCase):
    RULES = MarketRules("c", D("0"), 1, min_order_size=D("5"), tick_size=D("0.01"), up_token_id="u",
                        down_token_id="d")

    def book(self, asks):
        return BookSnapshot(token_id="u", bids=((D("0.01"), D("100")),), asks=tuple(sorted(asks)),
                            tick_size=D("0.01"), min_order_size=D("5"))

    def test_budget_and_price_cap_hold_on_random_books(self):
        rnd = random.Random(14)
        for _ in range(3000):
            n = rnd.randint(0, 8)
            prices = rnd.sample(range(2, 99), n)
            asks = [(D(p) / 100, D(rnd.choice((1, 2, 3, 5, 7, 11, 40, 500)))) for p in prices]
            shares, cap = rnd.randint(5, 60), D(rnd.randint(3, 98)) / 100
            want_shares, want_cost = brute(asks, shares, cap)
            try:
                q = estimate_fak_shares(self.book(asks), shares, cap, self.RULES)
            except PaperRejected as exc:
                self.assertEqual(want_shares, 0, (asks, shares, cap))
                self.assertIn("FAK no-fill", str(exc))
                continue
            self.assertAlmostEqual(q.shares, want_shares, places=20, msg=str((asks, shares, cap)))
            self.assertEqual(q.notional, want_cost, (asks, shares, cap))
            self.assertLessEqual(q.notional, D(shares) * cap)
            self.assertLessEqual(q.worst_price, cap)
            self.assertEqual([p for p, *_ in q.levels], sorted(p for p, *_ in q.levels))   # cheapest first

    def test_rejects_what_estimate_fok_rejects(self):
        book = self.book([(D("0.50"), D("100"))])
        for kw in ({"shares": 0, "max_price": 0.6}, {"shares": 10, "max_price": 1.0},
                   {"shares": 10, "max_price": 0.6, "min_price": 0.6}):
            with self.assertRaises(PaperRejected):
                estimate_fak_shares(book, kw["shares"], kw["max_price"], self.RULES, kw.get("min_price", 0))
        with self.assertRaises(PaperRejected):                                       # best ask under the floor
            estimate_fak_shares(book, 10, 0.6, self.RULES, min_price=0.51)


class PaperShareSized(I.PaperIOCBase):
    def test_the_sniper_sends_the_shares_book_cost_never_a_share_sized_order(self):
        sent = []
        place = self.broker.place_trade
        self.broker.place_trade = lambda *a, **k: (sent.append(k), place(*a, **k))[1]
        r = self.fire("UP", [(0.47, 5), (0.50, 100)])
        self.assertEqual(r["result"], "filled")
        self.assertAlmostEqual(r["amount"], 4.85, places=6)              # 5 x 0.47 + 5 x 0.50: the book cost
        self.assertNotIn("shares", sent[-1])
        self.assertNotIn("order_shares", r)

    def test_the_broker_refuses_a_share_sized_fok_and_fractional_shares(self):
        m = self.m
        ok = self.broker.place_trade("UP", 5.0, m.up_token, m.down_token, m.condition_id, m.window_end, 0.55,
                                     fak=False, shares=10)
        self.assertFalse(ok)
        self.assertIn("fill-and-kill", str(self.broker.last_error))
        m.set(m.up_token, [(0.50, 100)])
        ok = self.broker.place_trade("UP", 5.0, m.up_token, m.down_token, m.condition_id, m.window_end, 0.55,
                                     fak=True, shares=10.5)
        self.assertFalse(ok)
        self.assertIn("whole shares", str(self.broker.last_error))


class FrozenSpec(unittest.TestCase):
    def test_live_requires_share_sizing(self):
        """2026-10-15, the operator's choice: share sizing (whole shares x the cap, sent
        as a dollar order - the venue fills by dollars, so a cheaper fill buys more)."""
        self.assertEqual(I.settings(SNIPER_ORDER_SIZING="").order_sizing, "shares")      # the code default
        self.assertEqual([v for v in X.spec_violations(I.settings(SNIPER_ORDER_SIZING="shares"))
                          if "order sizing" in v], [])
        dollars = [v for v in X.spec_violations(I.settings(SNIPER_ORDER_SIZING="dollars")) if "order sizing" in v]
        self.assertEqual(len(dollars), 1, dollars)                                         # paper A/B only

    def test_an_unknown_sizing_is_refused_at_startup(self):
        with self.assertRaises(ValueError):
            I.settings(SNIPER_ORDER_SIZING="notional")


if __name__ == "__main__":
    unittest.main()
