"""Capped partial-fill regressions. Local HTTP server, throwaway unfunded key.

    python -m unittest -v tests_sniper_fak
"""
import asyncio
import json
import os
import tempfile
import time
import types
import unittest
from decimal import Decimal as D
from unittest.mock import patch

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L
import sniper_mode as S
import tests_sniper_live as T


class FAKTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        T.reset_ambiguity()
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()
        self.ex, self.ledger, self.store, self.receipts, _ = await T.make_canary(self.fake, self.tmp.name)
        self.mono = T.FakeMono()
        self.ex.kill.mono = self.mono
        self.sniper = None

    async def asyncTearDown(self):
        if self.sniper:
            tasks = list(self.sniper._tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        T.reset_ambiguity()

    def plan(self, asks=((0.50, 30),), **kw):
        return dict(T.live_plan(T.live_book(asks=asks).view(T.UP), **kw), order_type="FAK")

    def trade(self, tid="trade-1", qty=3, px=0.51, status="CONFIRMED", oid="0xorder1", **kw):
        fields = dict(order_id=oid, asset_id=T.UP, market=T.COND, side="BUY", price=px, size=qty, status=status)
        fields.update(kw)
        self.store.record_trade(tid, **fields)

    async def send(self, plan=None, response=None):
        if response is not None:
            self.fake.script.append(("json", 200, response))
        p = self.ex.prepare(self.plan() if plan is None else plan)
        return p, await self.ex.send(p)

    def terminal(self, size, tids=(), status="CANCELED"):
        return dict(id="0xorder1", asset_id=T.UP, market=T.COND, side="BUY", order_type="FAK",
                    status=status, size_matched=str(size), associate_trades=list(tids))

    async def test_thin_book_posts_fak_and_books_three_shares_below_order_minimum(self):
        self.trade()
        p, row = await self.send(self.plan(asks=((0.50, 3), (0.53, 100))))
        posted = self.fake.orders[0][0]
        self.assertEqual(posted["orderType"], "FAK")
        self.assertEqual(p.price, 0.52)
        self.assertEqual(p.amount, 5)
        self.assertGreaterEqual(D(posted["order"]["takerAmount"]) / D(1e6), 5)
        self.assertEqual(row["fill"]["shares"], 3)
        self.assertAlmostEqual(row["fill"]["cost"], 1.53)
        self.assertTrue(row["fill"]["partial"])
        self.assertEqual(self.ledger.ingest_fill_store(self.store), 1)
        pos = self.ledger.positions[T.UP]
        self.assertEqual(pos.shares, 3)
        self.assertAlmostEqual(pos.cost, 1.53 + row["fill"]["fee"], places=5)
        self.assertEqual(self.ledger.ingest_fill_store(self.store), 0)
        self.ex.reconcile_once()
        self.assertAlmostEqual(self.ex.sender.limits.exposure_usd, 1.53)
        self.ex.reconcile_once()
        self.assertAlmostEqual(self.ex.sender.limits.exposure_usd, 1.53)
        self.assertFalse(self.ex.kill.tripped)

    async def test_full_fill_at_original_price_uses_actual_ten_shares(self):
        self.trade(qty=10, px=0.50)
        _, row = await self.send()
        self.assertEqual(row["fill"]["shares"], 10)
        self.assertEqual(row["fill"]["cost"], 5)
        self.assertFalse(row["fill"]["partial"])

    async def test_price_moved_inside_cap_still_buys_available_shares(self):
        self.trade(qty=9, px=0.52)
        _, row = await self.send()
        self.assertEqual(row["fill"]["shares"], 9)
        self.assertEqual(row["fill"]["price"], 0.52)
        self.assertAlmostEqual(row["fill"]["cost"], 4.68)

    async def test_multilevel_fill_uses_each_trade_fee_and_waits_for_all_trades(self):
        reply = dict(T.MATCHED_REPLY, tradeIDs=["trade-1", "trade-2"])
        self.trade(qty=2, px=0.50)
        _, row = await self.send(response=reply)
        self.assertIsNone(row["fill"])
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        self.assertIn("0xorder1", self.ex.awaiting)
        self.assertEqual(self.ex.sender.limits.exposure_usd, 5)
        with self.assertRaisesRegex(L.Refused, "awaiting its fill size"):    # its token only (H2, 2026-10-23)
            self.ex.prepare(self.plan())
        self.trade(tid="trade-2", qty=1, px=0.52)
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        fill = self.ex.fill_for("0xorder1")
        self.assertEqual(fill["shares"], 3)
        self.assertAlmostEqual(fill["cost"], 1.52)
        self.assertAlmostEqual(fill["fee"], self.ledger.positions[T.UP].fees, places=5)
        self.assertNotIn("0xorder1", self.ex.awaiting)
        self.assertAlmostEqual(self.ex.sender.limits.exposure_usd, 1.52)

    async def test_partial_snapshot_reserves_full_budget_and_fee_bound(self):
        plan = self.plan(asks=((0.50, 3), (0.53, 100)))
        self.ex.balance = 2
        with self.assertRaisesRegex(L.Refused, "collateral"):
            self.ex.prepare(plan)
        self.assertEqual(len(self.fake.orders), 0)
        self.ex.balance = 100
        p = self.ex.prepare(plan)
        self.assertAlmostEqual(p.reserve, 5 * (1 + float(T.paper_rules().fee_rate)))
        self.assertEqual(p.estimate["shares"], 3)
        self.trade(qty=10, px=0.50)          # new liquidity at arrival fills the whole request
        row = await self.ex.send(p)
        self.assertEqual(row["fill"]["shares"], 10)
        self.assertEqual(row["fill"]["cost"], 5)

    async def test_no_liquidity_inside_cap_does_not_send(self):
        with self.assertRaisesRegex(L.Refused, "no-fill"):
            self.ex.prepare(self.plan(asks=((0.53, 100),)))
        self.assertEqual(len(self.fake.orders), 0)
        self.assertEqual(self.ex.reserved, 0)

    async def test_price_moves_past_cap_no_fill_releases_spend_without_retry(self):
        no_match = {"success": False, "errorMsg": "no orders found to match with FAK order. "
                    "FAK orders are partially filled or killed if no match is found."}
        self.fake.script.append(("json", 500, no_match))
        _, row = await self.send()
        self.assertEqual(row["kind"], L.NO_MATCH)
        self.assertEqual(len(self.fake.orders), 1)
        self.assertEqual(self.ex.sender.limits.exposure_usd, 0)
        self.assertFalse(self.ex.kill.tripped)
        self.trade(oid="0xnext")
        _, fresh = await self.send(response=dict(T.MATCHED_REPLY, orderID="0xnext"))
        self.assertEqual(fresh["fill"]["shares"], 3)
        self.assertEqual(len(self.fake.orders), 2)

    async def test_post_amounts_are_not_guessed_or_replaced_with_full_size(self):
        _, row = await self.send(response=T.AMOUNTS_REPLY)
        self.assertIsNone(row["fill"])
        self.assertEqual(self.ex.sender.limits.exposure_usd, 5)
        # 2026-10-23 (H2): the unknown size holds its own token, it no longer pauses all orders
        with self.assertRaisesRegex(L.Refused, "awaiting its fill size"):
            self.ex.prepare(self.plan())
        self.assertFalse(self.ex.kill.tripped)
        self.assertEqual(len(self.fake.orders), 1)
        self.trade(qty=3, px=0.51)
        self.assertEqual(self.ex.fill_for("0xorder1")["shares"], 3)
        self.ex.prepare(self.plan())                                      # its size is known: it trades again

    async def test_provisional_fill_does_not_release_spend_before_confirmation(self):
        self.trade(status="MATCHED")
        _, row = await self.send()
        self.assertEqual(row["fill"]["shares"], 3)
        self.assertEqual(self.ledger.ingest_fill_store(self.store), 0)
        self.ex.reconcile_once()
        self.assertEqual(self.ex.sender.limits.exposure_usd, 5)
        self.assertIn("0xorder1", self.ex.awaiting)
        self.trade(status="CONFIRMED")
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        self.assertAlmostEqual(self.ex.sender.limits.exposure_usd, 1.53)

    async def test_canceled_remainder_keeps_partial_fill_and_its_cost(self):
        pending = dict(success=True, status="delayed", orderID="0xorder1", errorMsg="")
        _, row = await self.send(response=pending)
        self.assertEqual(row["kind"], L.PENDING)
        self.assertEqual(self.ex.sender.limits.fills, 1)
        self.ex.awaiting["0xorder1"]["wall"] -= 35
        self.ex.order_status = lambda oid: self.terminal(3, ["trade-1"])
        self.ex.reconcile_once()
        self.assertEqual(self.ex.sender.limits.exposure_usd, 5)
        self.assertIn("0xorder1", self.ex.awaiting)
        self.trade()
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        self.assertEqual(self.ex.fill_for("0xorder1")["shares"], 3)
        self.assertAlmostEqual(self.ex.sender.limits.exposure_usd, 1.53)
        self.assertEqual(self.ex.sender.limits.fills, 1)

    async def test_cancellation_without_size_never_refunds_the_whole_order(self):
        await self.send(response=dict(success=True, status="delayed", orderID="0xorder1", errorMsg=""))
        self.ex.awaiting["0xorder1"]["wall"] -= 35
        order = self.terminal(0)
        del order["size_matched"]
        self.ex.order_status = lambda oid: order
        self.ex.reconcile_once()
        self.assertEqual(self.ex.sender.limits.exposure_usd, 5)
        self.assertIsNone(self.ex.fill_for("0xorder1"))

    async def test_delayed_terminal_zero_releases_reservation_and_fill_slot(self):
        await self.send(response=dict(success=True, status="delayed", orderID="0xorder1", errorMsg=""))
        self.ex.awaiting["0xorder1"]["wall"] -= 35
        self.ex.order_status = lambda oid: self.terminal(0)
        self.ex.reconcile_once()
        self.assertEqual(self.ex.fill_for("0xorder1")["shares"], 0)
        self.assertEqual(self.ex.sender.limits.exposure_usd, 0)
        self.assertEqual(self.ex.sender.limits.fills, 0)
        self.assertNotIn("0xorder1", self.ex.awaiting)

    async def test_unsafe_private_fills_halt_without_releasing_reserved_spend(self):
        await self.send()
        for fields in (dict(px=0.53), dict(qty=11, px=0.50), dict(asset_id=T.DN),
                       dict(status="FAILED"), dict(status="CONFLICT")):
            with self.subTest(fields=fields):
                self.store.fills.clear()
                self.ex.sender.kill = L.KillSwitch(mono=self.mono)
                self.ex._tripped_once.clear()              # each field is an independent incident
                self.trade(**fields)
                self.assertIsNone(self.ex.fill_for("0xorder1"))
                self.assertTrue(self.ex.kill.tripped)
                self.assertEqual(self.ex.sender.limits.exposure_usd, 5)

    async def test_ambiguous_server_error_keeps_reservation_and_never_retries(self):
        self.fake.script.append(("json", 500, {"error": "unknown server failure"}))
        _, row = await self.send()
        self.assertEqual(row["kind"], L.AMBIGUOUS)
        self.assertTrue(self.ex.kill.tripped)
        self.assertEqual(self.ex.sender.limits.exposure_usd, 5)
        self.assertEqual(len(self.fake.orders), 1)

    def make_sniper(self, *, cap=200, fills=20):
        with patch.dict(os.environ, {"SNIPER_ONLY": "1", "SNIPER_ORDER_TYPE": "FAK",
                                     "SNIPER_SHADOW_MS": ""}):
            settings = S.Settings()
        settings.max_side_shares, settings.max_fills_round = cap, fills
        hub = types.SimpleNamespace(book=T.live_book(), up_token=T.UP, down_token=T.DN,
                                    condition_id=T.COND, window_end=T.round_end())
        self.ex.shim.trade_window_seconds = 600
        self.sniper = S.Sniper(hub, self.ex.shim, settings=settings, live=self.ex, trade_rows=[])
        self.rows = []
        self.sniper._write = lambda row: self.rows.append(dict(row))
        self.ex.attach(self.sniper)
        return self.sniper

    async def test_sniper_updates_pending_row_and_round_shares_once_when_quantity_arrives(self):
        sniper = self.make_sniper(cap=12)
        await sniper._fire("UP", 2, time.monotonic() * 1000)
        self.assertEqual(sniper.n["errors"], 0)
        pending = next(r for r in self.rows if r.get("result") == "pending")
        self.assertIsNone(pending["shares"])
        rd = sniper.rounds[sniper.hub.window_end]
        self.assertEqual(rd, {"fills": 1, "UP": 0, "DOWN": 0})
        self.assertIsNone(sniper._decide("UP", {}, time.monotonic() * 1000))
        self.assertEqual(len(self.fake.orders), 1)
        self.trade()
        # A 3-share partial + a new requested 10 would exceed this 12-share cap.
        self.assertIsNone(sniper._decide("UP", {}, time.monotonic() * 1000))
        sniper._sync_live_fak()
        self.assertEqual(rd, {"fills": 1, "UP": 3, "DOWN": 0})
        self.assertEqual(sniper.n["filled"], 1)
        self.assertEqual(len(sniper.trade_rows), 1)
        self.assertEqual(sniper.trade_rows[0]["result"], "filled")
        resolved = [r for r in self.rows if r.get("result") == "live_fill_update"]
        self.assertEqual(len(resolved), 1)
        self.assertAlmostEqual(resolved[0]["cost"], 1.53)

    async def test_sniper_immediate_partial_consumes_one_round_slot_and_actual_shares(self):
        sniper = self.make_sniper(fills=1)
        self.trade()
        await sniper._fire("UP", 2, time.monotonic() * 1000)
        self.assertEqual(sniper.n["errors"], 0)
        rd = sniper.rounds[sniper.hub.window_end]
        self.assertEqual(rd, {"fills": 1, "UP": 3, "DOWN": 0})
        row = next(r for r in self.rows if r.get("result") == "filled")
        self.assertEqual(row["shares"], 3)
        self.assertEqual(row["order_type"], "FAK")
        self.assertAlmostEqual(row["cost"], 1.53)
        self.assertIsNone(sniper._decide("UP", {}, time.monotonic() * 1000))
        self.assertEqual(len(self.fake.orders), 1)

    async def test_sniper_delayed_zero_releases_round_slot_without_counting_a_fill(self):
        sniper = self.make_sniper(fills=1)
        self.fake.script.append(("json", 200, dict(success=True, status="delayed", orderID="0xorder1", errorMsg="")))
        await sniper._fire("UP", 2, time.monotonic() * 1000)
        self.ex.awaiting["0xorder1"]["wall"] -= 35
        self.ex.order_status = lambda oid: self.terminal(0)
        self.ex.reconcile_once()
        sniper._sync_live_fak()
        self.assertEqual(sniper.rounds[sniper.hub.window_end], {"fills": 0, "UP": 0, "DOWN": 0})
        self.assertEqual(sniper.n["filled"], 0)
        self.assertEqual(len(sniper.trade_rows), 1)
        self.assertEqual(sniper.trade_rows[0]["result"], "missed")

    async def test_session_fill_limit_still_blocks_after_partial(self):
        self.ex.sender.limits.max_fills = 1
        self.trade()
        await self.send()
        self.ledger.ingest_fill_store(self.store)
        self.ex.reconcile_once()
        with self.assertRaises(L.Halted):
            self.ex.prepare(self.plan())
        self.assertEqual(len(self.fake.orders), 1)


class FAKConfigurationTests(unittest.TestCase):
    def test_sniper_only_defaults_fak_and_explicit_fok_remains_available(self):
        with patch.dict(os.environ, {"SNIPER_ONLY": "1"}, clear=True):
            settings = S.Settings()
            self.assertEqual(settings.order_type, "FAK")
            # the cooldown default is 300ms per side (was one shared 1000ms)
            self.assertEqual((settings.k_bp, settings.window_ms, settings.slip, settings.cooldown_ms),
                             (0.5, 250, 0.05, 300))
            os.environ["SNIPER_ORDER_TYPE"] = "FOK"
            self.assertEqual(S.Settings().order_type, "FOK")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(S.Settings().order_type, "FOK")

    def test_paper_signing_builds_fak_request_too(self):
        with patch.dict(os.environ, {"SNIPER_ONLY": "1", "SNIPER_ORDER_TYPE": "FAK"}, clear=True):
            sniper = S.Sniper(types.SimpleNamespace(), types.SimpleNamespace(), settings=S.Settings())
        sniper._builder, sniper._clob = sniper._make_builder()
        self.assertIsNotNone(sniper._builder)
        error, signed = sniper._sign(T.UP, 5, 0.52, "0.01")
        self.assertIsNone(error)
        real = sniper._clob["order_to_json"]
        with patch.dict(sniper._clob, {"order_to_json": lambda obj, key, kind:
                                      self.assert_fak_payload(real(obj, key, kind))}):
            self.assertIsNone(sniper._prepare_request(signed))

    def assert_fak_payload(self, payload):
        self.assertEqual(payload["orderType"], "FAK")
        return payload


if __name__ == "__main__":
    unittest.main()
