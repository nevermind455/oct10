"""Durable dispatch telemetry, offline and SDK-free."""
import testing_env  # noqa: F401 - no wallet configuration or venue traffic

import asyncio
import time
import types
import unittest

import sniper_live as L


class SenderTelemetry(unittest.TestCase):
    def fixture(self, *, fail_intent=False):
        events, journal = [], []
        sender = object.__new__(L.LeanSender)
        sender.url, sender.timeout_s = "https://example.invalid/order", 3.0
        sender.limits, sender.kill = L.CanaryLimits(), L.KillSwitch()
        sender.guard = types.SimpleNamespace(slow_ms=1500, on_reply=lambda _rtt: "NORMAL")
        sender.consecutive_errors, sender.max_consecutive_errors = 0, 3
        sender.journal = journal.append
        sender._check_send = lambda *_args, **_kw: events.append("guard")
        sender._classify = lambda *_args: (L.NO_MATCH, "FAK no-fill")

        def before_send(p, limits, kill):
            self.assertIn("intent_start_pc", p.stamps)
            self.assertNotIn("intent_end_pc", p.stamps)
            events.append("durable")
            if fail_intent:
                raise L.Halted("intent persistence failed")
            limits.record_send(p.amount, p.condition)
            p.stamps["budget_recorded"] = True

        sender.session = types.SimpleNamespace(before_send=before_send,
                                               outcome=lambda *_args: events.append("outcome"))
        p = types.SimpleNamespace(client_order_id="synthetic", side="UP", token="token", condition="condition",
                                  end=time.time() + 100, cap=.5, price=.5, tick=".01", amount=5,
                                  order_type="FAK", body="{}", headers={},
                                  stamps={"prepare_pc": time.perf_counter_ns()})
        response = types.SimpleNamespace(status_code=400, http_version="HTTP/2", headers={},
                                         json=lambda: {}, text="FAK no-fill")

        class FakeHTTP:
            async def post(self, _url, **kwargs):
                self_test.assertIn("intent_end_pc", p.stamps)
                events.append("post")
                kwargs["extensions"]["before_write"]()
                for name in ("send_request_headers.started", "send_request_body.complete",
                             "receive_response_headers.complete"):
                    await kwargs["extensions"]["trace"](name, {"timestamp_ns": time.perf_counter_ns()})
                return response

        self_test = self
        sender.http = FakeHTTP()
        return sender, p, events, journal, response

    def test_durable_timing_is_published_before_write_and_does_not_repeat_the_intent(self):
        sender, p, events, _journal, _response = self.fixture()
        row = asyncio.run(sender._send_once(p))
        self.assertLess(events.index("durable"), events.index("post"))
        self.assertEqual(row["dispatch_mode"], "async")
        self.assertEqual(row["http_version"], "HTTP/2")
        self.assertTrue(row["http_transport"].endswith(".FakeHTTP"))
        self.assertLessEqual(row["ts_us"]["intent_start"], row["ts_us"]["intent_end"])
        self.assertLessEqual(row["ts_us"]["intent_end"], row["ts_us"]["net_send_start"])
        stamps = dict(p.stamps)
        sender._record_send(p)
        self.assertEqual(events.count("durable"), 1)
        self.assertEqual(p.stamps, stamps)

    def test_failed_persistence_still_records_elapsed_intent_time_and_raises(self):
        sender, p, events, _journal, _response = self.fixture(fail_intent=True)
        with self.assertRaisesRegex(L.Halted, "persistence failed"):
            sender._record_send(p)
        self.assertGreaterEqual(p.stamps["intent_end_pc"], p.stamps["intent_start_pc"])
        self.assertNotIn("post", events)
        self.assertFalse(p.stamps.get("budget_recorded"))

    def test_synchronous_dispatch_is_reported_without_an_additional_post(self):
        sender, p, events, _journal, response = self.fixture()
        sender._record_send(p)
        now = time.perf_counter_ns()

        async def received(_timeout):
            return response

        stream = types.SimpleNamespace(response=received, events=[("send_request_headers.started", now),
            ("send_request_body.complete", now + 1000), ("receive_response_headers.complete", now + 2000)])
        row = asyncio.run(sender._send_once(p, started=stream))
        self.assertEqual(row["dispatch_mode"], "synchronous")
        self.assertEqual(row["http_version"], "HTTP/2")
        self.assertNotIn("post", events)
        self.assertEqual(events.count("durable"), 1)


if __name__ == "__main__":
    unittest.main()
