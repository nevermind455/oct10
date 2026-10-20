"""ACK-target evidence and durable dispatch telemetry, offline and SDK-free."""
import testing_env  # noqa: F401 - no wallet configuration or venue traffic

import asyncio
import contextlib
import io
import time
import types
import unittest
from unittest.mock import patch

import bench_network as B
import sniper_live as L


class AckPlanning(unittest.TestCase):
    def test_only_valid_public_time_responses_qualify_as_rtt_samples(self):
        for payload in (1790000000, "1790000000", {"server_time": 1790000000}):
            B.check_time_reply(types.SimpleNamespace(status_code=200, json=lambda: payload))
        for status, payload in ((403, 1790000000), (503, 1790000000), (200, "challenge"),
                                (200, {}), (200, None), (200, False), (200, float("nan"))):
            with self.subTest(status=status, payload=payload), self.assertRaises(ValueError):
                B.check_time_reply(types.SimpleNamespace(status_code=status, json=lambda: payload))

    def test_failed_public_probe_cannot_produce_a_measured_or_successful_ack(self):
        async def unavailable(_n):
            raise ValueError("CLOB public /time returned HTTP 503")

        with patch.object(B, "warm_gets", new=unavailable), contextlib.redirect_stderr(io.StringIO()):
            result = asyncio.run(B.main_async(3, clob_only=True))
        self.assertIn("503", result["clob_warm"]["error"])
        result["ack_budget"] = B.ack_budget(result)
        self.assertEqual(result["ack_budget"]["scenario_ack_ms"], {})
        self.assertIn("unknown", B.table(result))

    def test_ack_uses_return_path_and_never_claims_an_order_was_measured(self):
        result = B.ack_budget({"clob_warm": {"warm_rtt_ms": {"p50": 33, "p95": 44, "p99": 48}}})
        self.assertEqual(result["scenario_ack_ms"], {"p50": 213.0, "p95": 224.0, "p99": 228.0})
        self.assertEqual(result["warm_rtt_budget_ms"], 20.0)
        self.assertFalse(result["measured_order_ack"])
        self.assertIn("queueing", result["limitation"])

    def test_missing_failed_or_invalid_get_samples_leave_feasibility_unknown(self):
        for data in ({}, {"clob_warm": {"error": "unavailable"}},
                     {"clob_warm": {"warm_rtt_ms": {"p50": float("nan"), "p95": -1, "p99": True}}}):
            with self.subTest(data=data):
                result = B.ack_budget(data)
                self.assertEqual(result["scenario_ack_ms"], {})
                self.assertIn("unknown", "\n".join(B.ack_table(result)))

    def test_allowances_are_explicit_and_impossible_budget_is_not_hidden(self):
        result = B.ack_budget({}, target_ms=100, in_process_ms=5, hold_ms=150, server_ms=25)
        self.assertEqual(result["warm_rtt_budget_ms"], -80)
        for field, value in (("target_ms", 0), ("target_ms", float("nan")),
                             ("hold_ms", -1), ("server_ms", float("inf")), ("in_process_ms", True)):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                B.ack_budget({}, **{field: value})

    def test_clob_only_command_runs_only_the_public_warm_probe(self):
        calls = []

        async def warm(n):
            calls.append(n)
            return {"warm_rtt_ms": {"n": n, "p50": 20, "p95": 30, "p99": 40}, "connections": 1}

        with patch.object(B, "warm_gets", new=warm), \
             patch.object(B, "host_steps", side_effect=AssertionError("unexpected handshake probe")), \
             patch.object(B, "coinbase_feed", side_effect=AssertionError("unexpected feed")), \
             contextlib.redirect_stderr(io.StringIO()):
            result = asyncio.run(B.main_async(3, clob_only=True))
        self.assertEqual(calls, [3])
        result["ack_budget"] = B.ack_budget(result)
        text = B.table(result)
        self.assertIn("public GET /time", text)
        self.assertIn("not measured order execution", text)
        self.assertNotIn("Coinbase", text)
        self.assertNotIn("needs both", text)

    def test_invalid_cli_budget_is_rejected_before_any_network_work(self):
        for args in (["--clob-only", "--n", "0"], ["--clob-only", "--target-ack-ms", "nan"],
                     ["--clob-only", "--server-ms", "-1"],
                     ["--clob-only", "--feed-race-only", "--feed-race-s", "1"]):
            with self.subTest(args=args), patch.object(B, "main_async") as work, \
                 contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as exc:
                B.main(args)
            self.assertEqual(exc.exception.code, 2)
            work.assert_not_called()


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
