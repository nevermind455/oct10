"""Regression tests for missed live-sniper orders. Loopback only; no wallet.

    python -m unittest -v tests_sniper_recovery
"""
import asyncio
import dataclasses
import tempfile
import threading
import time
import types
import unittest
from decimal import Decimal as D

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L
import sniper_mode as S
import tests_sniper_live as T


class RecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fake = await T.FakeClob().start()
        self.sender, self.rows = await T.make(self.fake)

    async def asyncTearDown(self):
        await self.sender.http.aclose()
        await self.fake.stop()
        T.reset_ambiguity()

    async def test_http500_fok_miss_releases_money_and_next_signal_sends(self):
        self.fake.script.append(("json", 500, {"error": "FOK orders are filled or killed"}))
        result = await self.sender.send(T.order(self.sender))
        self.assertEqual(result["kind"], L.NO_MATCH)
        self.assertEqual(self.sender.limits.exposure_usd, 0)
        self.assertFalse(self.sender.kill.tripped)
        self.assertEqual(self.fake.hits[("POST", "/order")], 1)  # never chase/retry
        self.assertEqual((await self.sender.send(T.order(self.sender)))["kind"], L.MATCHED)
        self.assertEqual(self.fake.hits[("POST", "/order")], 2)

    async def test_fak_no_match_reply_from_the_docs_is_a_miss(self):
        msg = ("no orders found to match with FAK order. FAK orders are partially filled or killed "
               "if no match is found.")
        self.assertEqual(self.sender._classify(400, {"error": msg}, "")[0], L.NO_MATCH)
        self.assertEqual(self.sender._classify(400, {"errorMsg": "No orders found to match with FAK order"}, "")[0],
                         L.NO_MATCH)
        self.fake.script.append(("json", 400, {"error": msg}))
        result = await self.sender.send(T.order(self.sender))
        self.assertEqual(result["kind"], L.NO_MATCH)
        self.assertEqual(self.sender.limits.exposure_usd, 0)
        self.assertFalse(self.sender.kill.tripped)
        self.assertEqual(self.sender.consecutive_errors, 0)

    async def test_order_timed_out_500_is_a_rejection_not_unknown(self):
        self.assertEqual(self.sender._classify(500, {"error": "order timed out"}, "")[0], L.REJECTED)
        self.assertEqual(self.sender._classify(500, {**T.MATCHED_REPLY, "error": "order timed out"}, "")[0],
                         L.AMBIGUOUS)
        self.fake.script.append(("json", 500, {"error": "order timed out"}))
        result = await self.sender.send(T.order(self.sender))
        self.assertEqual(result["kind"], L.REJECTED)
        self.assertEqual(self.sender.limits.exposure_usd, 0)
        self.assertFalse(self.sender.kill.tripped)

    async def test_a_400_fok_miss_echoing_its_order_id_is_a_miss_not_unknown(self):
        # The live canary halted on exactly this (10-06): HTTP 400, the documented
        # FOK no-fill message, plus the order's own id echoed back.
        msg = "order couldn't be fully filled. FOK orders are fully filled or killed."
        for payload in ({"errorMsg": msg, "orderID": "0x" + "ab" * 32, "success": False},
                        {"errorMsg": msg, "orderID": "0x" + "ab" * 32, "success": False,
                         "makingAmount": "5.2", "takingAmount": "10", "status": "", "transactionsHashes": []},
                        {"error": msg, "orderID": "0x" + "ab" * 32}):
            with self.subTest(payload=payload):
                self.assertEqual(self.sender._classify(400, payload, "")[0], L.NO_MATCH)
        self.assertEqual(self.sender._classify(500, {"error": "FOK orders are filled or killed",
                                                     "orderID": "0x" + "cd" * 32}, "")[0], L.NO_MATCH)
        self.assertEqual(self.sender._classify(503, {"code": "post_only_mode", "error": "post-only mode",
                                                     "orderID": "0x" + "ef" * 32}, "")[0], L.REJECTED)
        self.fake.script.append(("json", 400, {"errorMsg": msg, "orderID": "0x" + "ab" * 32, "success": False}))
        result = await self.sender.send(T.order(self.sender))
        self.assertEqual(result["kind"], L.NO_MATCH)
        self.assertFalse(self.sender.kill.tripped)
        self.assertEqual(self.sender.limits.exposure_usd, 0)

    async def test_5xx_unknown_and_conflicting_replies_stay_ambiguous(self):
        for status, payload in (
            (500, {"error": "internal error"}), (503, {"error": "unavailable"}),
            (500, {**T.MATCHED_REPLY, "error": "FOK orders are filled or killed"}),
            (400, {"success": True, "error": "FOK orders are filled or killed"}),
            (500, {"takingAmount": "10000000", "error": "FOK orders are filled or killed"}),
            (503, {"code": "post_only_mode", "status": "live"}),
        ):
            with self.subTest(status=status, payload=payload):
                self.assertEqual(self.sender._classify(status, payload, "")[0], L.AMBIGUOUS)
        self.fake.script.append(("json", 500, {"error": "internal error"}))
        result = await self.sender.send(T.order(self.sender))
        self.assertEqual(result["kind"], L.AMBIGUOUS)
        self.assertTrue(self.sender.kill.tripped)
        self.assertGreater(self.sender.limits.exposure_usd, 0)
        with self.assertRaises(L.Halted):
            T.order(self.sender)

    async def test_post_only_waits_for_deadline_and_health_then_fresh_signal(self):
        now = [100.0]
        self.sender.guard.mono = lambda: now[0]
        self.fake.script.append(("json", 503, {"code": "post_only_mode", "error": "post-only mode",
                                              "retry_after_seconds": 79}))
        result = await self.sender.send(T.order(self.sender))
        self.assertEqual(result["kind"], L.REJECTED)
        self.assertEqual(result["venue_retry_after_s"], 79)
        self.assertFalse(self.sender.kill.tripped)
        self.assertEqual(self.sender.limits.exposure_usd, 0)
        self.assertEqual(self.sender.consecutive_errors, 0)
        for stamp in (101, 140, 178.9):
            now[0] = stamp
            self.sender.guard.on_probe(True, "healthy")
            with self.assertRaises(L.Refused):
                T.order(self.sender)
        self.assertEqual(self.fake.hits[("POST", "/order")], 1)
        now[0] = 179
        for _ in range(self.sender.guard.healthy_checks):
            self.sender.guard.on_probe(True, "healthy")
        self.assertEqual((await self.sender.send(T.order(self.sender)))["kind"], L.MATCHED)
        self.assertEqual(self.fake.hits[("POST", "/order")], 2)

    async def test_retry_header_supports_both_transports(self):
        for headers in ({"Retry-After": "91"}, [("retry-after", "91")]):
            self.assertEqual(self.sender._retry_after(types.SimpleNamespace(headers=headers)), "91")

    async def test_cold_http2_refused_and_goaway_are_not_sent_but_reset_is_unknown(self):
        for action, expected in (("refuse", L.NOT_SENT), ("goaway", L.NOT_SENT), ("reset", L.AMBIGUOUS)):
            with self.subTest(action=action):
                fake = await T.FakeH2Clob().start()
                sender, _ = await T.make_fast(fake)
                try:
                    await sender.http.aclose()
                    from fast_h2 import FastH2
                    sender.http = FastH2(fake.url)  # fresh transport: actual cold/connect path
                    fake.script.append((action,))
                    p = T.order(sender)
                    self.assertIsNone(sender.start(p))
                    result = await sender.send(p)
                    self.assertEqual(result["kind"], expected)
                    self.assertEqual(sender.kill.tripped, expected == L.AMBIGUOUS)
                    self.assertEqual(sender.limits.exposure_usd > 0, expected == L.AMBIGUOUS)
                    self.assertEqual(len(fake.orders), 1)
                finally:
                    await sender.http.aclose()
                    await fake.stop()
                    T.reset_ambiguity()

    async def test_invalidation_during_prefetch_cannot_restore_old_rules(self):
        entered, release = threading.Event(), threading.Event()
        def fetch(*args):
            entered.set()
            release.wait(2)
            return T.rules_fetch(*args)
        rules = L.RoundRules(fetch)
        task = asyncio.create_task(rules.prefetch(T.COND, T.UP, T.DN))
        self.assertTrue(await asyncio.to_thread(entered.wait, 2))
        rules.invalidate(T.COND)
        release.set()
        with self.assertRaises(L.Refused):
            await task
        with self.assertRaises(L.Refused):
            rules.get(T.COND, T.UP, T.DN)


class ExecutionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fake = await T.FakeClob().start()
        self.ex, *_ = await T.make_canary(self.fake, self.tmp.name)
        self.hub = types.SimpleNamespace(condition_id=T.COND, up_token=T.UP, down_token=T.DN)
        self.task = None

    async def asyncTearDown(self):
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        T.reset_ambiguity()

    async def wait_loaded(self, count):
        async def wait():
            while self.ex.n["rules loaded"] < count:
                await asyncio.sleep(0.01)
        await asyncio.wait_for(wait(), 2.0)

    async def start_rules(self):
        self.task = asyncio.create_task(self.ex._rules_loop(self.hub))
        await self.wait_loaded(1)

    async def test_venue_rule_rejection_refreshes_both_caches_within_one_second(self):
        await self.start_rules()
        self.fake.script.append(("json", 400, {"error": "invalid tick size"}))
        p = self.ex.prepare(T.live_plan(T.live_book().view(T.UP)))
        result = await self.ex.send(p)
        self.assertTrue(result["rules_invalidated"])
        self.assertIsNone(self.ex.shim.cached_rules(T.COND))
        started = time.monotonic()
        await self.wait_loaded(2)
        self.assertLess(time.monotonic() - started, 1.0)
        p = self.ex.prepare(T.live_plan(T.live_book().view(T.UP)))
        self.assertEqual((await self.ex.send(p))["kind"], L.MATCHED)
        self.assertEqual(self.fake.hits[("POST", "/order")], 2)

    async def test_book_tick_change_requests_and_recovers_both_rule_caches(self):
        await self.start_rules()
        self.ex.sender.rules.fetch = lambda *args: T.rules_fetch(*args, tick="0.001")
        self.ex.shim._fetch = lambda cid: dataclasses.replace(T.paper_rules(), tick_size=D("0.001"))
        view = dataclasses.replace(T.live_book().view(T.UP), tick_size="0.001")
        with self.assertRaisesRegex(L.Refused, "background refresh requested"):
            self.ex.prepare(T.live_plan(view))
        self.assertIsNone(self.ex.shim.cached_rules(T.COND))
        await self.wait_loaded(2)
        p = self.ex.prepare(T.live_plan(view))
        self.assertEqual(p.tick, "0.001")
        self.assertEqual((await self.ex.send(p))["kind"], L.MATCHED)

    async def test_periodic_refresh_bypasses_600_second_sizing_cache(self):
        await self.start_rules()
        self.ex.sender.rules.fetch = lambda *args: T.rules_fetch(*args, tick="0.001")
        self.ex.shim._fetch = lambda cid: dataclasses.replace(T.paper_rules(), tick_size=D("0.001"))
        self.ex.rules_refresh_s = 0.01
        await self.wait_loaded(2)
        self.assertEqual(self.ex.shim.cached_rules(T.COND).tick_size, D("0.001"))
        self.assertEqual(self.ex.sender.rules.get(T.COND, T.UP, T.DN)["tick"], D("0.001"))

    async def test_changed_token_mapping_halts_instead_of_rebinding_orders(self):
        self.hub.up_token = "wrong-token"
        self.task = asyncio.create_task(self.ex._rules_loop(self.hub))
        await asyncio.sleep(0.02)
        self.assertTrue(self.ex.kill.tripped)
        self.assertEqual(self.ex.n["rules loaded"], 0)

    async def test_wide_slippage_small_order_meets_signed_minimum_and_cash_limit(self):
        view = T.live_book().view(T.UP)
        p = self.ex.prepare(T.live_plan(view, amount=2.5, cap=0.56))
        self.assertEqual(p.amount, 2.8)
        self.assertGreaterEqual(D(str(p.amount)) / D("0.56"), D("5"))
        self.assertLessEqual(p.price, 0.56)
        self.ex.reserved = 0
        self.ex.balance = 2.75
        with self.assertRaisesRegex(L.Refused, "collateral"):
            self.ex.prepare(T.live_plan(view, amount=2.5, cap=0.56))


class SignalTests(unittest.TestCase):
    def test_repeated_skips_keep_exact_counters_without_writing_each_tick(self):
        from unittest.mock import patch
        with patch.dict("os.environ", {"SNIPER_LATENCY_MS": "200"}):
            sn = S.Sniper(None, None, settings=S.Settings())
        rows = []
        sn._write = rows.append
        with patch.object(S, "time", types.SimpleNamespace(monotonic=lambda: 100)):
            for _ in range(100):
                sn._skip("book stale", {"side": "UP", "round_end": 300})
            sn._skip("too late in round", {"side": "UP", "round_end": 300})
        self.assertEqual(sn.n["skip: book stale"], 100)
        self.assertEqual(len(rows), 2)

    def test_skipped_signal_does_not_consume_cooldown_next_eligible_signal_does(self):
        from unittest.mock import patch
        # Binance the trigger, a Coinbase signal as a second feed's (suffixed counters)
        with patch.dict("os.environ", {"SNIPER_LATENCY_MS": "200", "SNIPER_COOLDOWN_MS": "1000",
                                       "SNIPER_PRIMARY_FEED": "binance"}):
            sn = S.Sniper(None, None, settings=S.Settings())
        called = []
        clock = {"ms": 1000.0}
        def fire(*args, **kwargs):
            called.append(args)
            sn.busy = False
            clock["ms"] += 3.0                  # deciding and sending take a moment
            return None if len(called) == 1 else object()
        sn._fire_now = fire
        trigger = types.SimpleNamespace(update=lambda t, mid: (1, 2))
        with patch.object(S.time, "monotonic", lambda: clock["ms"] / 1000.0):
            sn._signal("binance", trigger, 60000, 1000, 1)
            clock["ms"] = 1050.0
            sn._signal("binance", trigger, 60000, 1050, 2)
            clock["ms"] = 1100.0
            sn._signal("coinbase", trigger, 60000, 1100, 3)
        self.assertEqual(len(called), 2)
        # the cooldown started when the order was submitted (1053), on its side only
        self.assertAlmostEqual(sn.last_fire_by_side["UP"], 1053.0)
        self.assertEqual(sn.last_fire_by_side["DOWN"], -1e18)
        self.assertEqual(sn.n["fired binance"], 1)
        self.assertEqual(sn.n["skip: cooldown (coinbase)"], 1)

    def test_stale_feed_reference_and_invalid_prices_never_fire(self):
        trigger = S.Trigger(1, 250)
        trigger.update(0, 100)
        self.assertIsNone(trigger.update(5000, 101)[0])
        self.assertIsNone(trigger.update(5100, 101)[0])
        self.assertIsNone(trigger.update(5250, 101)[0])
        self.assertEqual(trigger.update(5300, 101.1)[0], 1)
        for price in (0, float("nan"), float("inf"), -1):
            self.assertIsNone(trigger.update(5400, price)[0])
        self.assertIsNone(trigger.update(5500, 102)[0])

    def test_quiet_ticker_is_no_gap_the_first_move_out_of_calm_fires(self):
        # the book ticker is silent while the top of the book is unchanged:
        # 1.5s of silence means the price 250ms ago was still 100
        trigger = S.Trigger(1, 250)
        trigger.update(0, 100)
        self.assertEqual(trigger.update(1500, 100.02)[0], 1)
        stalled = S.Trigger(1, 250)
        stalled.update(0, 100)
        self.assertIsNone(stalled.update(S.MAX_GAP_MS + 1, 100.02)[0])

    def test_sizing_inflight_refresh_cannot_resurrect_invalidated_cache(self):
        entered, release = threading.Event(), threading.Event()
        def fetch(cid):
            entered.set()
            release.wait(2)
            return T.paper_rules()
        shim = L.LiveBrokerShim(fetch, min_buy_price=.2, max_buy_price=.9, max_spread=.25,
                                trade_window_seconds=120, min_seconds_to_expiry=60)
        errors = []
        def worker():
            try:
                shim._rules(T.COND)
            except L.Refused as exc:
                errors.append(exc)
        thread = threading.Thread(target=worker)
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            shim.invalidate(T.COND)
        finally:
            release.set()
            thread.join(2)
        self.assertEqual(len(errors), 1)
        self.assertIsNone(shim.cached_rules(T.COND))


if __name__ == "__main__":
    unittest.main(verbosity=2)
