#!/usr/bin/env python3
"""SNIPER_VENUE_MIN_SHARES - the share minimum an order is held to (2026-10-29,
the operator's request).

    python tests/tests_venue_min.py

The market's published minimum (`mos`, 5 on the BTC 5-minute markets) skipped
every signal Kelly sized under 5 shares. Whether the venue really refuses a
marketable FAK BUY under 5 shares - its documented floor for one is $1 - is
unproven, so the operator tests it live:
  1  off by default: every minimum as before (the published 5)
  2  a whole number 1-5 replaces it in the decision, the paper broker, the
     live pre-send check and the signing sender alike
  3  the $1 marketable-BUY floor always holds: at 0.50 the smallest order is
     2 shares, at 0.30 four
  4  bad values, or dollar sizing, stop the start; live allows the setting
     (the venue's reply judges it)
The real Sniper and paper broker (tests_sniper_fair.Gate), and the live canary
against the local fake CLOB (tests_sniper_live). No network.
"""
from __future__ import annotations

import asyncio
import json
import pathlib
import sys
import tempfile
import unittest
from decimal import Decimal as D

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import sniper_live as L  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402


class Decision(F.Gate):
    """Share sizing, as live and paper50 run it (the frozen spec)."""

    def small_order(self, **env):
        # a side's room of 3 shares: Kelly wants more, so it sizes 3
        self.make(SNIPER_ORDER_SIZING="shares", SNIPER_MAX_SIDE_SHARES="3", **env)
        self.pin_time_left()
        self.kelly_wants_the_room(3)
        return self.order("UP", [(0.41, 200)], mid=0.40, move_bp=5.0)

    def test_off_by_default_a_kelly_size_under_5_is_skipped(self):
        r = self.small_order()
        self.assertEqual((r["result"], r["reason"]), ("skipped", "kelly size under the 5-share minimum"))

    def test_with_the_setting_the_same_signal_buys_3_shares(self):
        r = self.small_order(SNIPER_VENUE_MIN_SHARES="1")
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual(r["order_shares"], 3)
        self.assertGreaterEqual(r["order_shares"] * r["cap"], 1.0)        # still at least $1 at the limit

    def test_the_floor_keeps_the_dollar_minimum(self):
        cond, view = self.m.condition_id, I.View(self.m.decision[str(self.m.up_token)])
        self.make(SNIPER_ORDER_SIZING="shares", SNIPER_VENUE_MIN_SHARES="1")
        floor = lambda p: self.sn._share_order_floor(cond, view, D(p))
        self.assertEqual((floor("0.50"), floor("0.30"), floor("0.20"), floor("0.90")), (2, 4, 5, 2))
        self.make(SNIPER_ORDER_SIZING="shares", SNIPER_VENUE_MIN_SHARES="3")
        self.assertEqual((floor("0.50"), floor("0.20")), (3, 5))
        self.make(SNIPER_ORDER_SIZING="shares")
        self.assertEqual((floor("0.50"), floor("0.90")), (5, 5))                     # the published 5


    def test_the_venue_refusing_turns_it_off_for_the_run(self):
        self.make(SNIPER_ORDER_SIZING="shares", SNIPER_VENUE_MIN_SHARES="1")
        cond, view = self.m.condition_id, I.View(self.m.decision[str(self.m.up_token)])
        self.sn._venue_min_off({"error": "Size (2) lower than the minimum: 5", "client_order_id": "c1"})
        self.assertEqual(self.sn.s.venue_min_shares, 0)
        self.assertEqual(self.sn._share_order_floor(cond, view, D("0.50")), 5)    # the published 5 again
        self.assertIn("Kelly from 5 sh", self.sn.s.fair_text())
        offs = [r for r in self.rows if r.get("result") == "venue_min_off"]
        self.assertEqual((len(offs), offs[0]["was"], offs[0]["client_order_id"]), (1, 1.0, "c1"))
        self.sn._venue_min_off({"error": "again"})                                 # already off: nothing more
        self.assertEqual(len([r for r in self.rows if r.get("result") == "venue_min_off"]), 1)

    def test_the_live_reply_handler_hands_the_refusal_over(self):
        self.make(SNIPER_ORDER_SIZING="shares", SNIPER_VENUE_MIN_SHARES="1")
        reply = {"kind": "REJECTED", "venue_min_refused": True, "error": "Size (2) lower than the minimum: 5",
                 "client_order_id": "c2", "ts_us": {}}

        class Stub:
            async def send(self, p, started=None):
                return reply
        self.sn.live = Stub()
        p = type("P", (), {"amount": 1.1, "cap": 0.55, "price": 0.55, "order_type": "FAK", "client_order_id": "c2",
                           "order_hash": None, "stamps": {}, "estimate": None, "rules": {}})()
        plan = {"end": self.m.window_end, "rd": None, "token": self.m.up_token, "decided_pc": 0, "minimum_entry": False}
        try:
            asyncio.run(self.sn._finish_live("UP", 1.0, {}, plan, p, 0))
        except Exception:  # noqa: BLE001 - the stub carries only what the hand-off needs
            pass
        self.assertEqual(self.sn.s.venue_min_shares, 0)
        self.assertTrue(any(r.get("result") == "venue_min_off" for r in self.rows), self.rows[-3:])


# run only this file's tests, not every inherited tests_sniper_fair.Gate test again
for _name in [n for n in dir(F.Gate) if n.startswith("test") and n not in Decision.__dict__]:
    setattr(Decision, _name, None)


class PaperBroker(I.PaperIOCBase):
    def place(self, shares, **kw):
        m = self.m
        m.set(m.up_token, [(0.50, 100)])
        ok = self.broker.place_trade("UP", 1.0, m.up_token, m.down_token, m.condition_id, m.window_end, 0.55,
                                     fak=True, shares=shares, **kw)
        return ok, str(self.broker.last_error)

    def test_2_shares_fill_only_with_the_setting(self):
        ok, why = self.place(2)
        self.assertFalse(ok)
        self.assertIn("below venue minimum 5", why)
        ok, why = self.place(2, venue_min_shares=2)
        self.assertTrue(ok, why)
        self.assertGreaterEqual(float(self.broker.last_fill["shares"]), 2.0)

    def test_the_dollar_minimum_still_refuses_1_share(self):
        ok, why = self.place(1, venue_min_shares=1)
        self.assertFalse(ok)
        self.assertIn("min size: $1", why)


class Live(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import tests_sniper_live as T
        self.T = T
        T.reset_ambiguity()
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()
        self.ex, *_ = await T.make_canary(self.fake, self.tmp.name)

    async def asyncTearDown(self):
        for task in list(self.ex._order_tasks):
            task.cancel()
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        self.T.reset_ambiguity()

    def plan(self, shares, **kw):
        T = self.T
        view = T.live_book(asks=((0.50, 30),)).view(T.UP)
        return dict(T.live_plan(view, side="UP", cap=0.55, amount=shares * 0.5), order_type="FAK",
                    shares=shares, **kw)

    async def test_the_pre_send_check_and_the_signer_take_2_shares_with_the_setting(self):
        with self.assertRaises(L.Refused) as cm:
            self.ex.prepare(self.plan(2))
        self.assertIn("minimum", str(cm.exception))
        p = self.ex.prepare(self.plan(2, venue_min_shares=2))
        self.assertEqual((p.shares, p.amount, p.order_type), (2.0, 1.1, "FAK"))
        order = json.loads(p.body)["order"]
        self.assertEqual((int(order["takerAmount"]), int(order["makerAmount"])), (2_000_000, 1_100_000))
        self.assertEqual(self.fake.hits[("POST", "/order")], 0)                      # prepared, never sent

    async def test_the_dollar_minimum_still_refuses_1_share(self):
        with self.assertRaises(L.Refused) as cm:
            self.ex.prepare(self.plan(1, venue_min_shares=1))
        self.assertIn("min size: $1", str(cm.exception))
        self.assertEqual(self.ex.reserved, 0.0)

    async def test_the_signer_alone_holds_the_published_minimum_without_it(self):
        T, pl = self.T, self.plan(2)
        args = dict(side="UP", token=T.UP, up=T.UP, down=T.DN, condition=pl["cond"], end=pl["end"],
                    amount=1.1, cap=0.55, order_type="FAK", shares=2)
        with self.assertRaises(L.Refused) as cm:
            self.ex.sender.prepare(**args)
        self.assertIn("below the venue minimum 5", str(cm.exception))
        self.assertEqual(self.ex.sender.prepare(**args, min_shares=2).shares, 2.0)


    async def test_a_size_refusal_is_the_tests_answer_not_an_order_error(self):
        for _ in range(3):                                  # 3 in a row used to pause every order for 5 min
            self.fake.script.append(("json", 400, {"error": "Size (2) lower than the minimum: 5"}))
            row = await self.ex.send(self.ex.prepare(self.plan(2, venue_min_shares=2)))
            self.assertEqual(row["kind"], L.REJECTED)
            self.assertTrue(row.get("venue_min_refused"), row)
            self.assertNotIn("rules_invalidated", row)       # the rules were right: no reload, no gap
        self.assertEqual(self.ex.sender.consecutive_errors, 0)
        self.assertFalse(self.ex.kill.tripped, self.ex.kill.reasons)

    async def test_other_refusals_still_count(self):
        self.fake.script.append(("json", 400, {"error": "invalid order payload"}))        # not about its size
        row = await self.ex.send(self.ex.prepare(self.plan(2, venue_min_shares=2)))
        self.assertNotIn("venue_min_refused", row)
        self.assertEqual(self.ex.sender.consecutive_errors, 1)
        self.fake.script.append(("json", 400, {"error": "Size (6) lower than the minimum: 5"}))
        row = await self.ex.send(self.ex.prepare(self.plan(6)))                          # not under the published 5
        self.assertNotIn("venue_min_refused", row)
        self.assertEqual(self.ex.sender.consecutive_errors, 2)


class SettingsLine(unittest.TestCase):
    def test_values_and_the_settings_line(self):
        s = I.settings(SNIPER_VENUE_MIN_SHARES="1", SNIPER_FAIR="1", SNIPER_KELLY="1", SNIPER_ORDER_TYPE="FAK",
                       SNIPER_ORDER_SIZING="shares")
        self.assertEqual(s.venue_min_shares, 1)
        self.assertIn("Kelly from 1 sh and $1 (SNIPER_VENUE_MIN_SHARES", s.fair_text())
        self.assertIn("Kelly from 5 sh", I.settings(SNIPER_FAIR="1", SNIPER_KELLY="1",
                                                    SNIPER_ORDER_TYPE="FAK").fair_text())
        self.assertEqual(I.settings().venue_min_shares, 0)                          # off by default

    def test_bad_values_and_dollar_sizing_stop_the_start(self):
        for v in ("-1", "6", "2.5", "abc"):
            with self.assertRaises(ValueError, msg=v):
                I.settings(SNIPER_VENUE_MIN_SHARES=v, SNIPER_ORDER_SIZING="shares")
        with self.assertRaises(ValueError) as cm:
            I.settings(SNIPER_VENUE_MIN_SHARES="2", SNIPER_ORDER_SIZING="dollars")
        self.assertIn("SNIPER_ORDER_SIZING=shares", str(cm.exception))

    def test_live_allows_it(self):
        bad = X.spec_violations(I.settings(SNIPER_VENUE_MIN_SHARES="1", SNIPER_FAIR="1",
                                              SNIPER_ORDER_SIZING="shares"))
        self.assertFalse(any("VENUE_MIN" in v or "venue_min" in v for v in bad), bad)


if __name__ == "__main__":
    unittest.main(verbosity=1)
