#!/usr/bin/env python3
"""F1 (2026-10-14 repair): share-sized FAK orders. No network, no wallet.

    python tests_f1_share_sized.py

A FAK is a limit order for whole shares at the cap (takerAmount = shares,
makerAmount = shares x limit), so no fill can exceed its share count, the
side's cap or the canary's per-order cap; price improvement lowers the cost,
never raises the size. Every dollar limit reserves the order's maximum spend.

  - the signed amounts are exact for every whole share count and cent price
    (the library's own get_order_amounts);
  - the fast builder's limit order equals the library's, its hash the venue's id;
  - live: signs exactly the shares, reserves shares x limit + a fee bound, refuses
    fractional / sub-minimum / over-cap sizes, trips on a fill above its size;
    UP and DOWN in flight together each reserve their own worst case;
  - paper: the share estimate never exceeds its shares or the cap (random books
    against a brute-force oracle); a deeper, cheaper book at the match buys no more;
  - live refuses SNIPER_ORDER_SIZING=dollars (the frozen spec).
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import pathlib
import random
import secrets
import sys
import tempfile
import time
import unittest
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
        from py_clob_client_v2.order_builder.builder import ROUNDING_CONFIG, OrderBuilder
        from py_clob_client_v2.order_builder.constants import BUY
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
        from py_clob_client_v2.clob_types import CreateOrderOptions, OrderArgsV2
        from py_clob_client_v2.config import get_contract_config
        from py_clob_client_v2.order_builder.builder import OrderBuilder
        from py_clob_client_v2.order_builder.constants import BUY
        from py_clob_client_v2.order_utils import ExchangeOrderBuilderV2
        from py_clob_client_v2.order_utils.model.order_data_v2 import OrderV2
        from py_clob_client_v2.signer import Signer
        rnd = random.Random(20261014)
        for i in range(60):
            key = "0x" + secrets.token_hex(32)
            signer = L.fast_signer(key, 137) if i % 2 == 0 else Signer(key, 137)        # both signer kinds
            sig_type = rnd.choice([0, 1, 2])
            funder = None if sig_type == 0 else "0x" + secrets.token_hex(20)
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
            cc = get_contract_config(137)
            eb = ExchangeOrderBuilderV2(cc.neg_risk_exchange_v2 if opts.neg_risk else cc.exchange_v2, 137, signer)
            order = OrderV2(**{k: v for k, v in dataclasses.asdict(got).items() if k != "signature"})
            typed = eb.build_order_typed_data(order)
            self.assertEqual(got.signature, eb.build_order_signature(typed), i)          # byte-identical
            digest = eb.build_order_hash(typed)
            self.assertEqual(fast.last_order_hash, digest, i)                            # the venue's order id
            self.assertEqual(Account._recover_hash(bytes.fromhex(digest[2:]),
                                                   signature=bytes.fromhex(got.signature[2:])).lower(),
                             signer.address().lower())

    def test_poly_1271_limit_orders_go_to_the_library(self):
        from py_clob_client_v2.clob_types import CreateOrderOptions, OrderArgsV2
        from py_clob_client_v2.order_builder.builder import OrderBuilder
        from py_clob_client_v2.order_builder.constants import BUY
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
        self.assertEqual(calls, [2])


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
        self.assertEqual(ex._risk_orders[p.client_order_id]["shares"], 10.0)

    async def test_the_fast_builder_path_signs_the_same_order(self):
        from py_clob_client_v2.order_builder.builder import OrderBuilder
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

    async def test_a_fill_above_the_signed_size_trips_the_kill_switch(self):
        T = self.T
        ex = await self.canary()
        self.fake.script.append(("json", 200, T.MATCHED_REPLY))
        await ex.send(ex.prepare(self.plan(10)))
        # 11 shares at 0.40 = $4.40: inside the dollar budget, above the 10 signed shares
        self.store.record_trade("trade-1", order_id="0xorder1", asset_id=T.UP, market=T.COND, side="BUY",
                                price=0.40, size=11, status="MATCHED")
        self.assertIsNone(ex.fill_for("0xorder1"))
        self.assertTrue(ex.kill.tripped)
        self.assertIn("signed size", "; ".join(ex.kill.reasons))

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

    async def test_up_and_down_in_flight_each_reserve_their_own_worst_case(self):
        ex = await self.canary(balance=12.0)
        up = ex.prepare(self.plan(10, side="UP", cap=0.52))
        self.assertAlmostEqual(ex.reserved, 5.2 * 1.07, places=9)
        down = ex.prepare(dict(self.plan(10, side="DOWN", cap=0.53, asks=((0.50, 30),)), order_type="FAK"))
        self.assertAlmostEqual(ex.reserved, up.reserve + down.reserve, places=9)
        self.assertEqual({r["shares"] for r in ex._risk_orders.values()}, {10.0})
        # $12 holds both worst cases ($11.24) but not a third
        with self.assertRaises(L.Refused) as cm:
            ex.prepare(self.plan(5, side="UP", cap=0.52))
        self.assertIn("collateral", str(cm.exception))


def brute(asks, shares, cap):
    left, got, cost = D(shares), D(0), D(0)
    for price, size in sorted(asks):
        if price > cap or left <= 0:
            break
        take = min(left, size)
        got, cost, left = got + take, cost + take * price, left - take
    return got, cost


class PaperShareEstimate(unittest.TestCase):
    RULES = MarketRules("c", D("0"), 1, min_order_size=D("5"), tick_size=D("0.01"), up_token_id="u",
                        down_token_id="d")

    def book(self, asks):
        return BookSnapshot(token_id="u", bids=((D("0.01"), D("100")),), asks=tuple(sorted(asks)),
                            tick_size=D("0.01"), min_order_size=D("5"))

    def test_never_more_than_its_shares_or_above_its_cap_on_random_books(self):
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
            self.assertEqual((q.shares, q.notional), (want_shares, want_cost), (asks, shares, cap))
            self.assertLessEqual(q.shares, shares)
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
    def test_a_deeper_cheaper_book_at_the_match_buys_no_more(self):
        r = self.fire("UP", [(0.50, 100)], [(0.20, 1000), (0.30, 1000)])
        self.assertEqual((r["result"], r["order_shares"], r["shares"]), ("filled", 10, 10.0))
        self.assertAlmostEqual(r["price"], 0.20, places=9)

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
    def test_live_requires_share_sized_orders(self):
        dollars = [v for v in X.spec_violations(I.settings(SNIPER_ORDER_SIZING="dollars")) if "order sizing" in v]
        shares = [v for v in X.spec_violations(I.settings()) if "order sizing" in v]
        self.assertEqual(len(dollars), 1, dollars)
        self.assertIn("SNIPER_ORDER_SIZING", dollars[0])
        self.assertEqual(shares, [])

    def test_an_unknown_sizing_is_refused_at_startup(self):
        with self.assertRaises(ValueError):
            I.settings(SNIPER_ORDER_SIZING="notional")


if __name__ == "__main__":
    unittest.main()
