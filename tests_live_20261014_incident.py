#!/usr/bin/env python3
"""The 2026-10-14 live halt and the fixes before live restarts. No network,
no wallet (a fake CLOB, throwaway keys).

    python tests_live_20261014_incident.py

The incident: a share-sized FAK (11 shares, makerAmount $3.74 at the 0.34
cap) was filled by Polymarket for its whole $3.74 at 0.29 - 12.896552 shares.
The share check halted it ("filled above its signed size"), and 180s later
the same order halted again ("not completely confirmed").

  1  sizing: the order's dollars are the Kelly shares' cost on the visible
     book (levels up to the cap, fees outside the stake)
  2  the cap never passes the fair limit (fair - taker fee - margin)
  3  cheaper executions that buy more shares are not halted; only spending
     above the signed USDC is
  4  one order halts once; trades are deduplicated by trade id; partial FAK
     fills; delayed confirmations
  5  the incident replayed, lower-price fills, partials, rejections, delayed
     confirmation, duplicate private events, UP and DOWN together, and paper
     and live signing the same dollars for the same decision
  6  reconcile_order.py: the read-only check run before a halt is cleared
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
import math
import os
import pathlib
import sys
import tempfile
import time
import types
import unittest
from decimal import Decimal as D
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402

INCIDENT_AMOUNT, INCIDENT_SHARES, INCIDENT_PRICE, INCIDENT_CAP = 3.74, 12.896552, 0.29, 0.34


class Live(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import tests_sniper_live as T
        self.T = T
        T.reset_ambiguity()
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()
        self.ex, self.ledger, self.store, _, _ = await T.make_canary(
            self.fake, self.tmp.name, limits=L.CanaryLimits(max_order_shares=25.0))
        self.ex.fast_fak_delays_s = (0.05, 0.1, 0.2)

    async def asyncTearDown(self):
        for task in list(self.ex._order_tasks):
            task.cancel()
        await asyncio.gather(*self.ex._order_tasks, return_exceptions=True)
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        self.T.reset_ambiguity()

    def plan(self, amount, *, side="UP", cap=INCIDENT_CAP, ask=INCIDENT_PRICE):
        T = self.T
        book = T.live_book(asks=((ask, 500),), bids=((round(ask - 0.01, 2), 500),))
        view = book.view(T.UP if side == "UP" else T.DN)
        return dict(T.live_plan(view, side=side, cap=cap, amount=amount), order_type="FAK")

    def reply(self, oid, tids=None):
        body = {"success": True, "status": "matched", "orderID": oid, "transactionsHashes": [], "errorMsg": ""}
        if tids is not None:
            body["tradeIDs"] = list(tids)
        self.fake.script.append(("json", 200, body))

    def trade(self, tid, oid, size, price, status="MATCHED", token=None, source="ws"):
        T = self.T
        return self.store.record_trade(tid, order_id=oid, asset_id=token or T.UP, market=T.COND, side="BUY",
                                       price=price, size=size, status=status, source=source)

    async def send(self, amount, oid, tids=("trade-1",), **kw):
        self.reply(oid, tids)
        p = self.ex.prepare(self.plan(amount, **kw))
        await self.ex.send(p)
        return p

    # ---------------------------------------------------------------- 1, 3, 5: the incident replayed
    async def test_the_incident_fill_is_accepted_not_halted(self):
        p = await self.send(INCIDENT_AMOUNT, "0xa1fa8479")
        order = json.loads(p.body)["order"]
        self.assertEqual(int(order["makerAmount"]), 3_740_000)              # the dollars are what is signed
        self.assertIsNone(getattr(p, "shares", None))                        # not a share-sized order
        self.trade("trade-1", "0xa1fa8479", INCIDENT_SHARES, INCIDENT_PRICE)
        fill = self.ex.fill_for("0xa1fa8479")
        self.assertIsNotNone(fill)
        self.assertAlmostEqual(fill["shares"], INCIDENT_SHARES, places=6)   # more shares, same dollars: fine
        self.assertAlmostEqual(fill["cost"], INCIDENT_AMOUNT, places=5)
        self.assertFalse(self.ex.kill.tripped, self.ex.kill.reasons)

    async def test_the_same_kelly_order_now_signs_its_book_cost(self):
        # Kelly's 11 shares at the 0.29 book: $3.19 signed, not 11 x the 0.34 cap ($3.74)
        p = await self.send(round(11 * INCIDENT_PRICE, 2), "0xorder1")
        self.assertEqual(int(json.loads(p.body)["order"]["makerAmount"]), 3_190_000)
        self.trade("trade-1", "0xorder1", 11, INCIDENT_PRICE)
        fill = self.ex.fill_for("0xorder1")
        self.assertEqual((fill["shares"], round(fill["cost"], 6)), (11.0, 3.19))
        self.assertFalse(self.ex.kill.tripped)

    async def test_lower_price_fills_buy_more_shares_without_a_halt(self):
        await self.send(5.0, "0xorder1", cap=0.55, ask=0.50, tids=("t1", "t2"))
        self.trade("t1", "0xorder1", 5, 0.40)                              # $2.00
        self.trade("t2", "0xorder1", 7.5, 0.40)                            # $3.00 -> 12.5 shares for $5
        fill = self.ex.fill_for("0xorder1")
        self.assertEqual((fill["shares"], round(fill["cost"], 6)), (12.5, 5.0))
        self.assertFalse(self.ex.kill.tripped)

    async def test_spending_above_the_signed_dollars_halts_once(self):
        await self.send(INCIDENT_AMOUNT, "0xorder1")
        self.trade("trade-1", "0xorder1", 13, INCIDENT_PRICE)              # $3.77 > the signed $3.74
        self.assertIsNone(self.ex.fill_for("0xorder1"))
        self.assertEqual(self.ex.kill.reasons, ["FAK order 0xorder1 filled above its budget: reconcile"])
        a = self.ex.fak_orders["0xorder1"]
        self.ex._reconcile_fak("0xorder1", a, a["wall"] + self.ex.confirm_within_s + 60)
        self.assertEqual(len(self.ex.kill.reasons), 1, self.ex.kill.reasons)   # no second, 180s halt
        self.assertIn("0xorder1", self.ex.awaiting)                         # its reservation is kept

    async def test_an_order_with_missing_trades_still_times_out(self):
        await self.send(INCIDENT_AMOUNT, "0xorder1")
        a = self.ex.fak_orders["0xorder1"]
        self.ex._reconcile_fak("0xorder1", a, a["wall"] + self.ex.confirm_within_s + 1)
        self.assertEqual(len(self.ex.kill.reasons), 1)
        self.assertIn("not completely confirmed", self.ex.kill.reasons[0])

    # ---------------------------------------------------------------- 4, 5: partial, rejected, delayed, duplicate
    async def test_a_partial_fak_fill_is_kept(self):
        await self.send(3.19, "0xorder1")
        self.trade("trade-1", "0xorder1", 6, INCIDENT_PRICE)
        fill = self.ex.fill_for("0xorder1")
        self.assertEqual((fill["shares"], round(fill["cost"], 6), fill["partial"]), (6.0, 1.74, True))
        self.assertFalse(self.ex.kill.tripped)

    async def test_a_rejected_fak_releases_its_reservation(self):
        self.fake.script.append(("json", 400, {"success": False,
                                               "errorMsg": "no orders found to match with FAK order"}))
        p = self.ex.prepare(self.plan(3.19))
        self.assertGreater(self.ex.reserved, 0)
        res = await self.ex.send(p)
        self.assertEqual(res["kind"], L.NO_MATCH)
        self.assertEqual(self.ex.reserved, 0.0)
        self.assertFalse(self.ex.kill.tripped)
        self.assertEqual(self.fake.hits[("POST", "/order")], 1)              # never resent

    async def test_a_delayed_confirmation_resolves_without_a_halt(self):
        T = self.T
        self.ex.order_status = lambda oid: {"id": oid, "asset_id": T.UP, "market": T.COND, "side": "BUY",
                                            "status": "matched", "size_matched": "11",
                                            "associate_trades": ["trade-1"]}
        await self.send(3.19, "0xorder1", tids=None)                        # the reply names no trades
        self.trade("trade-1", "0xorder1", 11, INCIDENT_PRICE)
        for _ in range(100):
            if self.ex.fill_for("0xorder1") is not None:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(self.ex.fill_for("0xorder1")["shares"], 11.0)
        a = self.ex.fak_orders["0xorder1"]
        self.ex._reconcile_fak("0xorder1", a, a["wall"] + 60)               # still MATCHED, not yet 180s
        self.assertFalse(self.ex.kill.tripped)
        self.trade("trade-1", "0xorder1", 11, INCIDENT_PRICE, status="CONFIRMED")
        self.ledger.ingest_fill_store(self.store)
        self.ex._reconcile_fak("0xorder1", a, a["wall"] + 90)
        self.assertNotIn("0xorder1", self.ex.awaiting)                      # confirmed and booked
        self.assertFalse(self.ex.kill.tripped)

    async def test_duplicate_private_events_count_once(self):
        await self.send(3.19, "0xorder1")
        self.assertTrue(self.trade("trade-1", "0xorder1", 11, INCIDENT_PRICE))
        self.assertFalse(self.trade("trade-1", "0xorder1", 11, INCIDENT_PRICE))                # socket repeat
        self.assertFalse(self.trade("trade-1", "0xorder1", 11, INCIDENT_PRICE, source="reconcile"))  # REST
        self.assertEqual(len(self.store.fills_for_order("0xorder1")), 1)
        self.assertEqual(self.ex.fill_for("0xorder1")["shares"], 11.0)

    async def test_up_and_down_together(self):
        T = self.T
        up = self.ex.prepare(self.plan(3.19, side="UP"))
        down = self.ex.prepare(self.plan(5.10, side="DOWN", cap=0.56, ask=0.51))
        self.assertAlmostEqual(self.ex.reserved, up.reserve + down.reserve, places=9)
        self.reply("0xup", ["t-up"])
        await self.ex.send(up)
        self.reply("0xdown", ["t-down"])
        await self.ex.send(down)
        self.trade("t-up", "0xup", 12.0, 0.2658)                            # cheaper: more shares, same $3.19
        self.trade("t-down", "0xdown", 10, 0.51, token=T.DN)
        self.assertAlmostEqual(self.ex.fill_for("0xup")["cost"], 3.19, places=3)
        self.assertEqual(self.ex.fill_for("0xdown")["shares"], 10.0)
        self.assertFalse(self.ex.kill.tripped)


# ---------------------------------------------------------------------------- 1, 2: the decision
class Decision(unittest.TestCase):
    def gate(self, bankroll=None):
        import tests_sniper_fair as F
        g = F.Gate("test_down_moves_buy_down_when_its_ask_lags")
        g.setUp()
        self.addCleanup(g.tearDown)
        g.pin_time_left(120.0)
        if bankroll is not None:
            g.sn._read_bankroll = lambda: bankroll
            g.sn._bankroll = None
        return g, F

    def test_kelly_shares_are_sent_as_their_book_cost(self):
        g, _F = self.gate(bankroll=40.0)                                    # Kelly well inside the side's room
        levels = [(0.41, 3), (0.42, 4), (0.44, 1000)]
        r = g.order("UP", levels, mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled")
        self.assertLess(r["target_shares"], g.sn.s.max_side_shares)
        n, cost = r["target_shares"], 0.0
        for price, size in levels:
            if price > r["cap"] + 1e-9 or n <= 0:
                break
            take = min(n, size)
            cost, n = cost + take * price, n - take
        cost += max(0.0, n) * r["ask_seen"]                                # beyond the cap: at the reference ask
        room = math.floor(g.sn.s.max_side_shares * r["ask_seen"] * 100 + 1e-9) / 100   # the Kelly room bound
        self.assertAlmostEqual(r["amount"], min(math.ceil(cost * 100 - 1e-6) / 100, room), places=6)
        self.assertLess(r["amount"], r["target_shares"] * r["cap"])         # never shares x cap

    def test_the_cap_never_passes_the_fair_limit_net_of_fee_and_margin(self):
        bound = 0
        for ask, mid, move in ((0.41, 0.40, 5.0), (0.33, 0.30, 3.0), (0.36, 0.33, 2.5), (0.55, 0.52, 8.0)):
            g, F = self.gate()                                              # a fresh account per case
            r = g.order("UP", [(ask, 1000)], mid=mid, move_bp=move)
            self.assertIn("fair_limit", r, r)
            fl = r["fair_limit"]
            # the fair limit itself: the price plus the venue's taker fee plus the margin stays under fair
            self.assertLessEqual(fl + F.RATE * fl * (1 - fl) + g.sn.s.fair_margin, r["fair"] + 2e-4)
            if r["result"] == "skipped":
                self.assertIn(r["reason"], ("ask not below fair value", "kelly size under the 5-share minimum"))
                continue
            self.assertLessEqual(r["cap"], fl + 1e-9)                      # never above the fair limit
            self.assertLessEqual(r["cap"], ask + g.sn.s.slip + 1e-9)        # nor above ask + slip
            bound += r["cap"] < ask + g.sn.s.slip - 1e-9                    # the fair limit was the binding one
        self.assertGreater(bound, 0, "no case where the fair limit bound the cap below ask + slip")


# ---------------------------------------------------------------------------- 5: paper and live sign the same dollars
class PaperLiveSizing(I.PaperIOCBase):
    def test_paper_spends_what_live_signs(self):
        import tests_sniper_live as T
        m = self.m
        # the sniper never sizes under $1 (MIN_ORDER_USD); the venue minimums lift the rest alike
        cases = [(a, ask) for a in (1.0, 2.37, 3.19, 3.74, 5.0) for ask in (0.08, 0.15, 0.29, 0.5, 0.9)]

        async def live_amounts():
            fake = await T.FakeClob().start()
            tmp = tempfile.TemporaryDirectory()
            try:
                ex, *_ = await T.make_canary(fake, tmp.name, balance=1000.0,
                                             limits=L.CanaryLimits(max_order_shares=1000.0, max_exposure_usd=1000.0))
                out = []
                for amount, ask in cases:
                    book = T.live_book(asks=((ask, 5000),), bids=((round(ask - 0.01, 2), 500),))
                    p = ex.prepare(dict(T.live_plan(book.view(T.UP), cap=round(ask + 0.05, 2), amount=amount),
                                        order_type="FAK"))
                    out.append(int(json.loads(p.body)["order"]["makerAmount"]) / 1e6)
                    with ex._risk_lock:
                        ex._risk_orders.clear()
                    ex.reserved = 0.0
                return out
            finally:
                await fake.stop()
                tmp.cleanup()
        signed = asyncio.run(live_amounts())
        for (amount, ask), live in zip(cases, signed):
            m.set(m.up_token, [(ask, 5000)])
            ok = self.broker.place_trade("UP", amount, m.up_token, m.down_token, m.condition_id, m.window_end,
                                         round(ask + 0.05, 2), fak=True, size_at_limit=True)
            self.assertTrue(ok, (amount, ask, self.broker.last_error))
            paid = float(self.broker.last_fill["shares"]) * float(self.broker.last_fill["average_price"])
            self.assertAlmostEqual(paid, live, places=6, msg=(amount, ask))


# ---------------------------------------------------------------------------- 6: reconcile_order.py
class ReconcileTool(unittest.TestCase):
    def test_a_booked_fill_is_reported_consistent_and_nothing_is_written(self):
        import reconcile_order as R
        from accounting.ledger import Ledger
        from tests_sniper_live import COND, UP
        oid = "0x" + "a1fa8479" + "0" * 56
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(path=os.path.join(tmp, "ledger.json"))
            import polymarket_trade as pt
            receipt = pt._build_receipt({"tradeIDs": ["tr-1"]}, oid, "matched", condition_id=COND, token_id=UP,
                                        window_end=(int(time.time()) // 300 + 1) * 300, amount=INCIDENT_AMOUNT,
                                        estimated_fee=0.0, fee_rate=0.07, fee_exponent=1,
                                        validation="MATCHED_WITH_TRADE_EVIDENCE")
            led.authorize_order(oid, receipt, venue_min_shares=5.0, price_cap=0.95)
            led.record_fill("tr-1", UP, shares=INCIDENT_SHARES, price=INCIDENT_PRICE, condition_id=COND,
                            order_id=oid, status="CONFIRMED", source="user_ws")
            led.save()
            journal = os.path.join(tmp, "live_sniper.jsonl")
            with open(journal, "w", encoding="utf-8") as fh:
                fh.write(json.dumps({"result": "pending", "order_id": oid, "amount": INCIDENT_AMOUNT,
                                     "cap": INCIDENT_CAP, "condition_id": COND}) + "\n")
            before = {p.name: p.stat().st_mtime_ns for p in pathlib.Path(tmp).iterdir()}
            trade = {"id": "tr-1", "taker_order_id": oid, "asset_id": UP, "market": COND, "side": "BUY",
                     "status": "CONFIRMED", "size": str(INCIDENT_SHARES), "price": str(INCIDENT_PRICE)}
            client = types.SimpleNamespace(
                get_order=lambda o: {"id": o, "asset_id": UP, "market": COND, "side": "BUY", "status": "MATCHED",
                                     "price": "0.34", "original_size": "11", "size_matched": str(INCIDENT_SHARES),
                                     "associate_trades": ["tr-1"]},
                get_trades=lambda params: [trade, dict(trade)],          # the same trade twice: counted once
                builder=types.SimpleNamespace(funder=None), signer=types.SimpleNamespace(address=lambda: "0x" + "1" * 40))
            out = io.StringIO()
            env = {"LIVE_SNIPER_JOURNAL": journal, "LEDGER_PATH": os.path.join(tmp, "ledger.json"),
                   "SNIPER_CANARY_STATE_PATH": os.path.join(tmp, "live_canary_state.json")}
            with mock.patch.dict(os.environ, env), mock.patch("polymarket_trade._get_client", lambda: client), \
                    mock.patch.object(sys, "argv", ["reconcile_order.py", "0xa1fa8479", "--no-position"]), \
                    contextlib.redirect_stdout(out):
                code = R.main()
            text = out.getvalue()
            after = {p.name: p.stat().st_mtime_ns for p in pathlib.Path(tmp).iterdir()}
        self.assertEqual(code, 0, text)
        self.assertIn("VERDICT: consistent", text)
        self.assertIn(f"1 trade(s), {INCIDENT_SHARES} shares", text)
        self.assertEqual(before, after)                                     # read-only

    def incident_state(self, tmp, *, with_lot=True, reply_tids=("tr-1",), trades=None):
        """A live ledger, journal and canary state as the halted VPS had them:
        the order accepted (its canary intent ACCEPTED), its trade booked."""
        import polymarket_trade as pt
        from accounting.ledger import Ledger
        from tests_sniper_live import COND, UP
        oid = "0x" + "a1fa8479" + "0" * 56
        led_path = os.path.join(tmp, "ledger.json")
        led = Ledger(path=led_path)
        end = (int(time.time()) // 300 + 1) * 300
        receipt = pt._build_receipt({"tradeIDs": list(reply_tids)}, oid, "matched", condition_id=COND, token_id=UP,
                                    window_end=end, amount=INCIDENT_AMOUNT, estimated_fee=0.0, fee_rate=0.07,
                                    fee_exponent=1, validation="MATCHED_WITH_TRADE_EVIDENCE")
        led.authorize_order(oid, receipt, venue_min_shares=5.0, price_cap=0.95)
        if with_lot:
            led.record_fill("tr-1", UP, shares=INCIDENT_SHARES, price=INCIDENT_PRICE, condition_id=COND,
                            order_id=oid, status="CONFIRMED", source="user_ws")
        led.save()
        with open(os.path.join(tmp, "live_sniper.jsonl"), "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"result": "live_send", "order_id": oid, "amount": INCIDENT_AMOUNT,
                                 "cap": INCIDENT_CAP, "condition_id": COND, "token": UP,
                                 "response": {"success": True, "status": "matched", "orderID": oid,
                                              "tradeIDs": list(reply_tids)}}) + "\n")
        holder = "0x" + "1" * 40
        canary = os.path.join(tmp, "live_canary_state.json")
        session = L.CanarySession(canary, L.CanarySession.wallet_key_for(holder))
        limits, kill = L.CanaryLimits(), L.KillSwitch()
        session.open(limits, kill, Ledger(path=os.path.join(tmp, "empty.json")))
        p = L.Prepared("UP", UP, COND, float(end), INCIDENT_AMOUNT, INCIDENT_CAP, INCIDENT_CAP, "0.01", "key-1",
                       json.dumps({"order": {"salt": "1", "signature": "throwaway"}}), {}, {}, {}, "FAK")
        p.reserve, p.order_hash = INCIDENT_AMOUNT * 1.07, oid
        session.before_send(p, limits, kill)
        session.outcome(p, {"kind": L.MATCHED, "order_id": oid}, limits, kill)     # the old code kept no trade ids
        kill.trip("FAK order " + oid + " filled above its signed size: reconcile")
        session.persist(limits, kill)
        session.close()
        trade = {"id": "tr-1", "taker_order_id": oid, "asset_id": UP, "market": COND, "side": "BUY",
                 "status": "CONFIRMED", "size": str(INCIDENT_SHARES), "price": str(INCIDENT_PRICE)}
        client = types.SimpleNamespace(get_order=lambda o: None,              # the CLOB answered null
                                       get_trades=lambda params: [trade] if trades is None else trades,
                                       builder=types.SimpleNamespace(funder=None),
                                       signer=types.SimpleNamespace(address=lambda: holder))
        env = {"LIVE_SNIPER_JOURNAL": os.path.join(tmp, "live_sniper.jsonl"), "LEDGER_PATH": led_path,
               "SNIPER_CANARY_STATE_PATH": canary}
        return oid, canary, client, env

    def run_tool(self, client, env, *argv):
        import reconcile_order as R
        out = io.StringIO()
        with mock.patch.dict(os.environ, env), mock.patch("polymarket_trade._get_client", lambda: client), \
                mock.patch.object(sys, "argv", ["reconcile_order.py", "0xa1fa8479", "--no-position", *argv]), \
                contextlib.redirect_stdout(out):
            code = R.main()
        return code, out.getvalue()

    def test_no_venue_order_record_settles_from_the_replys_booked_trades(self):
        with tempfile.TemporaryDirectory() as tmp:
            oid, canary, client, env = self.incident_state(tmp)
            code, text = self.run_tool(client, env)
            self.assertEqual(code, 0, text)
            self.assertIn("no order record", text)
            self.assertIn("VERDICT: consistent", text)
            self.assertIn("re-run with --settle", text)
            self.assertEqual(L.CanarySession.durable_view(canary)["intents"]["key-1"]["status"], "ACCEPTED")
            code, text = self.run_tool(client, env, "--settle")
            self.assertEqual(code, 0, text)
            data = L.CanarySession.durable_view(canary)
            intent = data["intents"].get("key-1")
            self.assertTrue(intent is None or intent["status"] == "CONFIRMED", intent)
            self.assertEqual(len(data["kill_reasons"]), 1)                  # the halt stays until a new session
            from accounting.ledger import Ledger
            led = Ledger(path=env["LEDGER_PATH"])
            led.load()
            line = L.CanarySession(canary, data["wallet"]).open(L.CanaryLimits(), L.KillSwitch(), led,
                                                                new_session=True)
            self.assertIn("new canary session", line)                        # no unresolved intent left

    def test_a_missing_record_without_trades_is_never_settled(self):
        with tempfile.TemporaryDirectory() as tmp:
            oid, canary, client, env = self.incident_state(tmp, with_lot=False, trades=[])
            code, text = self.run_tool(client, env, "--settle")
            self.assertEqual(code, 1, text)
            self.assertIn("never proves a zero fill", text)
            self.assertEqual(L.CanarySession.durable_view(canary)["intents"]["key-1"]["status"], "ACCEPTED")

    def test_a_trade_the_reply_did_not_name_blocks_the_verdict(self):
        from tests_sniper_live import COND, UP
        oid = "0x" + "a1fa8479" + "0" * 56
        base = {"id": "tr-1", "taker_order_id": oid, "asset_id": UP, "market": COND, "side": "BUY",
                "status": "CONFIRMED", "size": str(INCIDENT_SHARES), "price": "0.29"}
        extra = dict(base, id="tr-2", size="1")
        with tempfile.TemporaryDirectory() as tmp:
            _oid, canary, client, env = self.incident_state(tmp, trades=[base, extra])
            code, text = self.run_tool(client, env, "--settle")
            self.assertEqual(code, 1, text)
            self.assertIn("are not the order's", text)
            self.assertEqual(L.CanarySession.durable_view(canary)["intents"]["key-1"]["status"], "ACCEPTED")


class RestartEvidence(unittest.IsolatedAsyncioTestCase):
    """A restart settles an accepted order the venue keeps no record of only from
    the reply's trade ids, each booked under the order's authorization."""

    async def asyncSetUp(self):
        import tests_sniper_live as T
        self.T = T
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()
        self.ex, self.ledger, self.store, _, _ = await T.make_canary(self.fake, self.tmp.name)

    async def asyncTearDown(self):
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()

    def book(self, oid, tids, lots):
        import polymarket_trade as pt
        T = self.T
        receipt = pt._build_receipt({"tradeIDs": list(tids)}, oid, "matched", condition_id=T.COND, token_id=T.UP,
                                    window_end=(int(time.time()) // 300 + 1) * 300, amount=INCIDENT_AMOUNT,
                                    estimated_fee=0.0, fee_rate=0.07, fee_exponent=1,
                                    validation="MATCHED_WITH_TRADE_EVIDENCE")
        self.ledger.authorize_order(oid, receipt, venue_min_shares=5.0, price_cap=0.95)
        for tid, shares in lots:
            self.ledger.record_fill(tid, T.UP, shares=shares, price=INCIDENT_PRICE, condition_id=T.COND,
                                    order_id=oid, status="CONFIRMED", source="user_ws")

    def intent(self, oid, tids):
        T = self.T
        return {"order_hash": oid, "order_id": oid, "kind": L.MATCHED, "status": "ACCEPTED", "token": T.UP,
                "condition": T.COND, "amount": INCIDENT_AMOUNT, "trade_ids": list(tids)}

    async def test_booked_reply_trades_settle_a_missing_record(self):
        oid = "0x" + "ab" * 32
        self.book(oid, ["t1"], [("t1", INCIDENT_SHARES)])
        verdict = self.ex._previous_run_verdict(self.intent(oid, ["t1"]), None)
        self.assertEqual(verdict[0], "CONFIRMED")
        self.assertAlmostEqual(verdict[1], INCIDENT_AMOUNT, places=5)

    async def test_no_trade_ids_an_unbooked_or_a_foreign_trade_is_not_evidence(self):
        oid = "0x" + "cd" * 32
        self.book(oid, ["t1"], [("t1", 6.0), ("t9", 1.0)])
        self.assertIsNone(self.ex._previous_run_verdict(self.intent(oid, []), {}))             # nothing named
        self.assertIsNone(self.ex._previous_run_verdict(self.intent(oid, ["t1", "t2"]), {}))  # t2 not booked
        self.assertIsNone(self.ex._previous_run_verdict(self.intent(oid, ["t1"]), {}))        # t9 is foreign

    async def test_the_canary_intent_keeps_the_replys_trade_ids(self):
        from accounting.ledger import Ledger
        T = self.T
        path = os.path.join(self.tmp.name, "canary.json")
        session, limits, kill = L.CanarySession(path, "w"), L.CanaryLimits(), L.KillSwitch()
        session.open(limits, kill, Ledger(path=os.path.join(self.tmp.name, "e.json")))
        p = L.Prepared("UP", T.UP, T.COND, time.time() + 120, 3.19, 0.34, 0.34, "0.01", "k",
                       json.dumps({"order": {"salt": "1", "signature": "x"}}), {}, {}, {}, "FAK")
        p.reserve, p.order_hash = 3.5, "0x" + "ef" * 32
        session.before_send(p, limits, kill)
        session.outcome(p, {"kind": L.MATCHED, "order_id": p.order_hash,
                            "response": {"tradeIDs": ["t1", "t2"]}}, limits, kill)
        session.close()
        self.assertEqual(L.CanarySession.durable_view(path)["intents"]["k"]["trade_ids"], ["t1", "t2"])


if __name__ == "__main__":
    unittest.main()
