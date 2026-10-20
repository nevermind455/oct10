"""Offline clock and keep-warm response validation; no SDK, wallet or sockets."""
import testing_env  # noqa: F401

import asyncio
import json
import types
import unittest
from unittest.mock import patch

import sniper_live as L


def response(payload, status=200):
    return types.SimpleNamespace(status_code=status, json=lambda: payload, text=json.dumps(payload))


class TimeReply(unittest.TestCase):
    def test_supported_numeric_and_object_replies(self):
        for payload in (1_800_000_000, 1_800_000_000.25, "1800000000",
                        {"time": 1_800_000_000}, {"timestamp": "1800000000"},
                        {"server_time": 1_800_000_000}):
            with self.subTest(payload=payload):
                self.assertEqual(L.parse_server_time_reply(response(payload)), float(
                    next(iter(payload.values())) if isinstance(payload, dict) else payload))

    def test_plain_numeric_text_is_supported(self):
        def not_json():
            raise ValueError("plain text reply")

        reply = types.SimpleNamespace(status_code=200, json=not_json, text=" 1800000000\n")
        self.assertEqual(L.parse_server_time_reply(reply), 1_800_000_000)

    def test_http_errors_with_numeric_bodies_are_not_clock_samples(self):
        for status in (301, 403, 429, 500, 503):
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, f"HTTP {status}"):
                L.parse_server_time_reply(response(1_800_000_000, status))

    def test_invalid_and_boolean_timestamps_are_rejected(self):
        for value in (True, False, None, {}, [], "challenge", 0, -1, "0", "NaN",
                      float("nan"), float("inf"), "1e10000"):
            for payload in (value, {"time": value}, {"timestamp": value}, {"server_time": value}):
                with self.subTest(payload=payload), self.assertRaises(ValueError):
                    L.parse_server_time_reply(response(payload))


class ClockResponses(unittest.IsolatedAsyncioTestCase):
    def clock(self):
        mono, wall = [100.0], [1_800_000_000.0]
        clock = L.ServerClock(mono=lambda: mono[0], wall=lambda: wall[0])
        clock.add_sample(wall[0], wall[0], wall[0], 0)
        mono[0] += 5
        wall[0] += 5
        return clock, mono, wall

    async def test_invalid_refresh_cannot_replace_or_refresh_trusted_clock(self):
        clock, _mono, _wall = self.clock()
        before = (clock.offset, clock.at, clock._server_at, clock.now_s(), list(clock.samples))
        for reply in (response(1_900_000_000, 503), response(True), response({"time": "NaN"})):
            async def get(_url):
                return reply

            self.assertFalse(await clock.refresh(types.SimpleNamespace(get=get), "unused"))
            self.assertEqual((clock.offset, clock.at, clock._server_at, clock.now_s(), list(clock.samples)), before)
        self.assertEqual(clock.rejected, 3)

    async def test_refresh_accepts_each_supported_object_key(self):
        for key in ("time", "timestamp", "server_time"):
            with self.subTest(key=key):
                clock, mono, wall = self.clock()

                async def get(_url):
                    return response({key: wall[0]})

                self.assertTrue(await clock.refresh(types.SimpleNamespace(get=get), "unused"))
                self.assertEqual(clock.at, mono[0])

    async def test_failed_keep_warm_reply_preserves_clock_and_retries_as_failure(self):
        clock, _mono, wall = self.clock()
        before = (clock.offset, clock.at, clock._server_at, list(clock.samples))
        replies = iter((response(wall[0] + 1_000, 503), response({"server_time": wall[0]})))
        rows = []

        async def get(_url, **_kwargs):
            return next(replies)

        class ProbeDone(Exception):
            pass

        def observe(row):
            rows.append(row)
            if len(rows) == 1:
                self.assertEqual((clock.offset, clock.at, clock._server_at, list(clock.samples)), before)
            else:
                raise ProbeDone

        with patch("feeds.supervisor.backoff_delay", return_value=0) as backoff:
            with self.assertRaises(ProbeDone):
                await asyncio.wait_for(L.keep_warm(types.SimpleNamespace(get=get), clock, "unused",
                                                  every_s=30, on_event=observe), 0.5)
        self.assertIn("HTTP 503", rows[0]["error"])
        self.assertNotIn("ok", rows[0])
        self.assertTrue(rows[1]["ok"])
        self.assertEqual(clock.rejected, 1)
        backoff.assert_called_once_with(0)


if __name__ == "__main__":
    unittest.main()
