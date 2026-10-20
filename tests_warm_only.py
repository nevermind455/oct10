"""Warm-only sender behavior, with in-memory transport and no venue traffic."""
import testing_env  # noqa: F401

import asyncio
import time
import types
import unittest

import sniper_live as L
import tests_latency_target as T


class WarmOnly(unittest.IsolatedAsyncioTestCase):
    def fixture(self, warm=False):
        sender, prepared, events, journal, response = T.SenderTelemetry().fixture()
        sender.warm_only = True
        sender.http.connected = warm

        async def received(_timeout):
            return response

        def post_now(_url, _body, _headers):
            events.append("post_now")
            now = time.perf_counter_ns()
            return types.SimpleNamespace(response=received, events=[
                ("send_request_headers.started", now),
                ("send_request_body.complete", now + 1000),
                ("receive_response_headers.complete", now + 2000)])

        sender.http.post_now = post_now
        return sender, prepared, events, journal

    def test_mode_defaults_off_and_requires_explicit_zero_or_one(self):
        self.assertFalse(L.warm_only_from_env({}))
        self.assertFalse(L.warm_only_from_env({"SNIPER_WARM_ONLY": "0"}))
        self.assertTrue(L.warm_only_from_env({"SNIPER_WARM_ONLY": "1"}))
        for value in ("", "yes", "true", " 1 ", 1, True, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                L.warm_only_from_env({"SNIPER_WARM_ONLY": value})

    def test_cold_prepare_is_refused_before_signing_or_reserving(self):
        sender, _p, events, _journal = self.fixture()
        sender.guard = L.VenueGuard(sender.kill)
        with self.assertRaisesRegex(L.Refused, "warm-only"):
            sender.prepare(side="UP", token="up", up="up", down="down", condition="condition",
                           end=time.time() + 100, amount=5, cap=.5)
        self.assertEqual(sender.limits.orders, 0)
        self.assertNotIn("durable", events)

    def test_httpx_conflict_is_rejected_before_credential_derivation(self):
        with self.assertRaisesRegex(ValueError, "requires the fast HTTP/2"):
            L.build_live_execution(ledger=None, env={"SNIPER_WARM_ONLY": "1", "SNIPER_HTTP": "httpx"})

    async def test_cold_send_has_no_network_intent_or_budget_change(self):
        sender, p, events, _journal = self.fixture()
        result = await sender.send(p)
        self.assertEqual(result["kind"], L.NOT_SENT)
        self.assertTrue(result["warm_connection_unavailable"])
        self.assertNotIn("post", events)
        self.assertNotIn("post_now", events)
        self.assertNotIn("durable", events)
        self.assertFalse(p.stamps.get("budget_recorded"))
        self.assertEqual(sender.limits.orders, 0)
        self.assertEqual(sender.limits.pending_fills, 0)

    async def test_skipped_order_is_never_sent_after_connection_recovers(self):
        sender, p, events, _journal = self.fixture()
        first = await sender.send(p)
        sender.http.connected = True
        self.assertIs(await sender.send(p), first)
        self.assertIsNone(sender.start(p))
        self.assertNotIn("post_now", events)
        self.assertNotIn("post", events)

    async def test_direct_warm_send_uses_synchronous_write_after_durable_intent(self):
        sender, p, events, _journal = self.fixture(warm=True)
        result = await sender.send(p)
        self.assertEqual(result["kind"], L.NO_MATCH)
        self.assertEqual(result["dispatch_mode"], "synchronous")
        self.assertLess(events.index("durable"), events.index("post_now"))
        self.assertNotIn("post", events)
        self.assertEqual(events.count("post_now"), 1)

    async def test_repeated_started_send_writes_and_accounts_once(self):
        sender, p, events, _journal = self.fixture(warm=True)
        stream = sender.start(p)
        first = await sender.send(p, stream)
        self.assertIs(await sender.send(p), first)
        self.assertIs(sender.start(p), stream)
        self.assertEqual(events.count("durable"), 1)
        self.assertEqual(events.count("post_now"), 1)
        self.assertEqual(sender.limits.orders, 1)
        self.assertEqual(sender.limits.pending_fills, 0)

    async def test_warm_skip_does_not_reset_or_increment_existing_venue_errors(self):
        sender, _p, events, _journal = self.fixture()
        sender.consecutive_errors = 2
        for _ in range(4):
            _other, p, _events, _journal = self.fixture()
            result = await sender.send(p)
            self.assertEqual(result["kind"], L.NOT_SENT)
            self.assertEqual(sender.consecutive_errors, 2)
        self.assertFalse(sender.kill.tripped)
        self.assertNotIn("post", events)

    async def test_unsent_after_intent_releases_pending_spend_without_reconnecting(self):
        sender, p, events, _journal = self.fixture()

        def disconnected_after_intent(prepared):
            sender._record_send(prepared)
            return None

        sender.start = disconnected_after_intent
        result = await sender.send(p)
        self.assertEqual(result["kind"], L.NOT_SENT)
        self.assertEqual(sender.limits.pending_fills, 0)
        self.assertEqual(sender.limits.exposure_usd, 0)
        self.assertNotIn("post", events)
        self.assertNotIn("post_now", events)

    async def test_warm_mode_does_not_bypass_expired_signing_clock(self):
        sender, p, _events, _journal = self.fixture(warm=True)
        sender.guard = L.VenueGuard(sender.kill)
        sender.clock = L.ServerClock()
        with self.assertRaisesRegex(L.Halted, "never synced"):
            L.LeanSender._check_send(sender, p)


if __name__ == "__main__":
    unittest.main()
