"""Execution regressions, using only synthetic clocks and an offline venue."""
import testing_env  # noqa: F401

import asyncio
import json
import threading
import time
import types
import unittest

import fast_h2 as H
import polymarket_trade as pt
import sniper_live as L
import tests_sniper_live as T
from tests_execution_weakness_fixes import WarmMemoryVenue, prepared


def memory_sender():
    T.reset_ambiguity()
    sender = L.LeanSender.__new__(L.LeanSender)
    sender.pt, sender.http = pt, WarmMemoryVenue()
    sender.url = "http://offline.invalid/order"
    sender.limits, sender.kill = L.CanaryLimits(max_orders=1000), L.KillSwitch()
    sender.session = None
    sender.guard = L.VenueGuard(sender.kill)
    sender.clock = L.ServerClock()
    now = time.time()
    sender.clock.add_sample(now, now, now, 0)
    sender.rules = types.SimpleNamespace(get=lambda *args: prepared().rules)
    sender.realized_loss = None
    sender.timeout_s, sender.consecutive_errors, sender.max_consecutive_errors = 1, 0, 3
    sender.rows = []
    sender.journal = sender.rows.append
    return sender


class OneShotDispatch(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self):
        T.reset_ambiguity()

    async def test_repeated_start_writes_once(self):
        sender, p = memory_sender(), prepared()
        first = sender.start(p)
        second = sender.start(p)
        self.assertIs(first, second)
        result = await sender.send(p, first)
        self.assertEqual(result["kind"], L.MATCHED)
        self.assertEqual(len(sender.http.orders), 1)

    async def test_send_without_started_argument_does_not_resubmit_started_order(self):
        sender, p = memory_sender(), prepared()
        sender.start(p)
        result = await sender.send(p)
        self.assertEqual(result["kind"], L.MATCHED)
        self.assertEqual(len(sender.http.orders), 1)

    async def test_concurrent_and_repeated_waiters_account_once(self):
        sender, p = memory_sender(), prepared()
        stream = sender.start(p)
        rows = await asyncio.gather(*(sender.send(p, stream) for _ in range(200)))
        rows.append(await sender.send(p, stream))
        self.assertTrue(all(row is rows[0] for row in rows))
        self.assertEqual((len(sender.http.orders), len(sender.rows), sender.limits.fills), (1, 1, 1))


class TrustedClock(unittest.TestCase):
    def test_invalid_samples_do_not_poison_valid_clock(self):
        mono, wall = [100.0], [1_800_000_000.0]
        clock = L.ServerClock(mono=lambda: mono[0], wall=lambda: wall[0])
        self.assertTrue(clock.add_sample(wall[0], wall[0], wall[0], 0))
        before = clock.now_s()
        for sample in ((float("nan"), wall[0], wall[0], 1),
                       (float("inf"), wall[0], wall[0], 1),
                       (wall[0], wall[0], wall[0], -1),
                       (wall[0], wall[0], wall[0], float("nan")),
                       (wall[0], wall[0], wall[0], float("inf")),
                       (wall[0], wall[0], wall[0] - 10, 1),
                       (True, wall[0], wall[0], 1)):
            self.assertFalse(clock.add_sample(*sample), sample)
            self.assertEqual(clock.now_s(), before)

    def test_wall_clock_jump_cannot_change_authentication_time(self):
        mono, wall = [100.0], [1_800_000_000.0]
        clock = L.ServerClock(mono=lambda: mono[0], wall=lambda: wall[0])
        clock.add_sample(wall[0], wall[0], wall[0], 0)
        before = clock.now_s()
        mono[0] += 2
        wall[0] += 1000
        self.assertEqual(clock.now_s(), before + 2)


class ResponseUncertainty(unittest.TestCase):
    def test_conflicting_rejection_cannot_release_order_exposure(self):
        sender = memory_sender()
        for status, response in (
                (200, {"success": False, "status": "rejected", "tradeIDs": ["trade"]}),
                (429, {**T.MATCHED_REPLY}),
                (400, {"success": False, "errorMsg": "no match", "status": "matched", "tradeIDs": ["trade"]})):
            with self.subTest(status=status, response=response):
                self.assertEqual(sender._classify(status, response, json.dumps(response))[0], L.AMBIGUOUS)
                self.assertFalse(pt._definitive_rejection(response))


class RulesRefresh(unittest.IsolatedAsyncioTestCase):
    async def test_older_slow_refresh_cannot_overwrite_newer_rules(self):
        entered, release = threading.Event(), threading.Event()
        calls = 0

        def fetch(*args):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                if not release.wait(2):
                    raise RuntimeError("test release missing")
                return {"tick": "old"}
            return {"tick": "new"}

        rules = L.RoundRules(fetch)
        old = asyncio.create_task(rules.prefetch(T.COND, T.UP, T.DN))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 1))
            await rules.prefetch(T.COND, T.UP, T.DN)
        finally:
            release.set()
        await asyncio.gather(old, return_exceptions=True)
        self.assertEqual(rules.get(T.COND, T.UP, T.DN)["tick"], "new")


if __name__ == "__main__":
    unittest.main(verbosity=2)
