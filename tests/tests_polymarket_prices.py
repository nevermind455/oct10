"""Offline regressions for the Polymarket reference/trigger price mismatch."""
import asyncio
import copy
import json
import pathlib
import sys
import time
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import testing_env  # noqa: E402,F401
import polymarket_prices as P  # noqa: E402
import run  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from dashboard import build, plain_line  # noqa: E402
from dashboard.theme import UNICODE  # noqa: E402

T0 = 1_800_000_000


def message(stamp=T0, value="80000.12345678", **payload):
    return {"channel": "price.crypto.twap", "payload": {
        "symbol": "btcusd", "timestamp": stamp * 1000, "value": 1,
        "full_accuracy_value": value, "window_seconds": 60, "source": "chainlink", **payload}}


def event(start=T0, opening=None, final=None):
    from datetime import datetime, timezone
    iso = lambda t: datetime.fromtimestamp(t, timezone.utc).isoformat()
    return {"slug": f"btc-updown-5m-{start}",
            "eventMetadata": {"priceToBeat": opening, "finalPrice": final},
            "markets": [{"eventStartTime": iso(start), "endDate": iso(start + 300),
                         "cryptoMarketConfig": {"asset": "btc", "duration": "5m",
                                                "twapEnabled": True, "twapLookbackSeconds": 60}}]}


class Prices(unittest.TestCase):
    def setUp(self):
        self.hub = types.SimpleNamespace(window_end=T0 + 300)
        self.ref = P.PolymarketPrices(self.hub)
        self.ref.connected = True

    def ingest(self, msg, wall=T0 + 1, mono=50):
        self.ref.ingest(msg, wall=wall, mono=mono)

    def snapshot(self, wall=T0 + 1, mono=51, end=T0 + 300):
        return self.ref.snapshot(end, wall=wall, mono=mono)

    def test_boundary_uses_source_time_and_full_accuracy_usd(self):
        self.ingest({**message(), "ts": (T0 + 7) * 1000})
        snap = self.snapshot()
        self.assertEqual(snap["current"], 80000.12345678)
        self.assertEqual(snap["price_to_beat"], 80000.12345678)
        self.assertEqual(snap["status"], "LIVE")

    def test_legacy_twap_is_e18_and_spot_is_never_used(self):
        legacy = {"topic": "crypto_prices_twap_sixty", "payload": {
            "symbol": "btc/usd", "timestamp": T0 * 1000,
            "window_s": 60, "value": 999,
            "full_accuracy_value": "80000123456780000000000"}}
        self.ingest(legacy)
        self.assertEqual(self.snapshot()["current"], 80000.12345678)
        self.ingest({**message(T0 + 1, "99999"), "channel": "price.crypto"})
        self.assertEqual(self.snapshot()["current"], 80000.12345678)
        del legacy["payload"]["full_accuracy_value"]
        legacy["payload"]["timestamp"] += 1000
        self.ingest(legacy)
        self.assertEqual(self.snapshot()["current"], 80000.12345678)

    def test_wrong_asset_source_window_and_bad_prices_are_rejected(self):
        for attrs in ({"symbol": "ethusd"}, {"source": "pyth"}, {"window_seconds": 30},
                      {"full_accuracy_value": True}, {"full_accuracy_value": "NaN"},
                      {"full_accuracy_value": "Infinity"}, {"full_accuracy_value": 0},
                      {"full_accuracy_value": -1}, {"timestamp": True}, {"timestamp": None}):
            with self.subTest(attrs=attrs):
                self.ingest(message(**attrs))
                self.assertIsNone(self.ref.latest)

    def test_missing_boundary_never_uses_first_tick_or_previous_round(self):
        self.ingest(message(T0 + 1))
        self.assertIsNone(self.snapshot()["price_to_beat"])
        self.ingest(message())
        self.assertIsNotNone(self.snapshot()["price_to_beat"])
        snap = self.snapshot(wall=T0 + 301, mono=351, end=T0 + 600)
        self.assertIsNone(snap["price_to_beat"])
        self.assertIsNone(snap["current"])

    def test_staleness_uses_source_and_receipt_times_and_connection(self):
        self.ingest(message())
        self.assertIsNone(self.snapshot(wall=T0 + 11)["current"])
        self.assertIsNone(self.snapshot(mono=61)["current"])
        self.ref.connected = False
        self.assertIsNone(self.snapshot()["current"])
        self.assertIsNotNone(self.snapshot()["price_to_beat"])
        self.ingest(message(T0 + 5), wall=T0 + 1)
        self.assertEqual(self.ref.latest[0], T0)

    def test_snapshot_history_recovers_boundary_but_old_ticks_do_not_rewind_current(self):
        msg = message()
        msg["payload"]["data"] = [message(T0 + 20, "80020")["payload"], message()["payload"]]
        self.ingest(msg, wall=T0 + 21)
        self.assertEqual(self.ref.latest[1], 80020)
        self.assertEqual(self.ref.price_to_beat(T0 + 300), 80000.12345678)
        self.ingest(message(T0 + 10, "70000"), wall=T0 + 21)
        self.assertEqual(self.ref.latest[1], 80020)

    def test_gamma_recovers_midround_and_overrides_stream_boundary(self):
        self.ref._fetch_event = lambda slug: event(opening=80010.25)
        asyncio.run(self.ref.metadata_once())
        self.ingest(message())
        self.assertEqual(self.ref.price_to_beat(T0 + 300), 80010.25)
        self.assertEqual(self.snapshot()["ptb_source"], "Gamma priceToBeat")

    def test_contiguous_previous_final_can_recover_the_open(self):
        events = {f"btc-updown-5m-{T0}": event(),
                  f"btc-updown-5m-{T0-300}": event(T0 - 300, final=80001.50)}
        self.ref._fetch_event = events.get
        asyncio.run(self.ref.metadata_once())
        self.assertEqual(self.ref.price_to_beat(T0 + 300), 80001.50)
        self.assertEqual(self.snapshot()["ptb_source"], "Gamma previous finalPrice")

    def test_metadata_rejects_other_rounds_or_oracle_models(self):
        good = event(opening=80000)
        wrong_config = copy.deepcopy(good)
        wrong_config["markets"][0]["cryptoMarketConfig"]["twapLookbackSeconds"] = 30
        for bad in (event(T0 - 300, opening=70000), wrong_config, {**good, "markets": []},
                    {**good, "markets": good["markets"] * 2}, event(opening=True)):
            self.ref._fetch_event = lambda slug, bad=bad: bad
            asyncio.run(self.ref.metadata_once())
            self.assertIsNone(self.ref.price_to_beat(T0 + 300))

    def test_round_rotation_during_fetch_cannot_install_old_strike_in_new_round(self):
        def fetch(slug):
            self.hub.window_end = T0 + 600
            return event(opening=80000)
        self.ref._fetch_event = fetch
        asyncio.run(self.ref.metadata_once())
        self.assertEqual(self.ref.price_to_beat(T0 + 300), 80000)
        self.assertIsNone(self.ref.price_to_beat(self.hub.window_end))

    def test_metadata_failures_are_visible_and_do_not_log_exception_secrets(self):
        events = []
        self.ref._event = lambda *args: events.append(args)

        def fail(slug):
            raise RuntimeError("exception fixture secret must stay private")

        self.ref._fetch_event = fail
        asyncio.run(self.ref.metadata_once())
        asyncio.run(self.ref.metadata_once())
        snap = self.snapshot()
        self.assertEqual(snap["ptb_detail"], "PTB metadata read failed (RuntimeError)")
        self.assertEqual(len(events), 1)
        self.assertNotIn("fixture secret", str(events) + str(snap))

    def test_unpublished_metadata_is_distinct_from_a_connection_error(self):
        self.ref._fetch_event = lambda slug: event(int(slug.rsplit("-", 1)[1]))
        asyncio.run(self.ref.metadata_once())
        self.assertIn("has not published", self.snapshot()["ptb_detail"])

    def test_legacy_error_frames_do_not_get_silently_ignored(self):
        for msg in ({"op": "error", "code": "auth_required"},
                    {"type": "error", "payload": {"code": "auth_failed"}}):
            with self.assertRaises(P.ReferenceFeedError) as error:
                P._check_error(msg)
            self.assertTrue(error.exception.auth)

    def test_storage_stays_bounded(self):
        for n in range(20):
            self.ingest(message(T0 + n * 300), wall=T0 + n * 300 + 1)
        self.assertEqual(len(self.ref.beats), 8)

    def test_direction_uses_polymarket_for_both_values_even_if_exchange_disagrees(self):
        sn = S.Sniper(I.Market(T0 + 300), None, settings=I.settings())
        sn.reference_prices = self.ref
        sn.ptb_rounds[T0] = {"est": 70000}
        sn.primary_mid = (90000, time.monotonic())
        sn.ptb_calib.errors.extend([0.2] * 6)
        self.assertIsNone(sn.price_to_beat(T0 + 300))
        self.assertIsNone(sn._ptb_view("UP", T0 + 300)[0])
        self.ingest(message(value="80000"))
        self.ingest(message(T0 + 1, "80024"))
        with patch.object(P.time, "time", return_value=T0 + 1), patch.object(P.time, "monotonic", return_value=51):
            self.assertTrue(sn._ptb_view("UP", T0 + 300)[0])
            self.assertFalse(sn._ptb_view("DOWN", T0 + 300)[0])
            self.assertEqual(sn.price_to_beat(T0 + 300), 80000)
            sn.ptb_calib.errors.append(4.0)
            self.assertIsNone(sn._ptb_view("UP", T0 + 300)[0])
            sn.ptb_calib.errors.clear()
            self.assertIsNone(sn._ptb_view("UP", T0 + 300)[0])
        sn.ptb_calib.errors.extend([0.2] * 6)
        with patch.object(P.time, "time", return_value=T0 + 5), patch.object(P.time, "monotonic", return_value=51):
            self.assertIsNotNone(self.ref.snapshot(T0 + 300)["current"])    # visible, aged on the dashboard
            self.assertIsNone(sn._ptb_view("UP", T0 + 300)[0])             # too old for the add-on


class Stream(unittest.IsolatedAsyncioTestCase):
    async def test_public_paper_stream_has_no_auth_and_cancels_its_heartbeat(self):
        sent, ready, cleaned = [], asyncio.Event(), asyncio.Event()
        frames = [{"topic": "crypto_prices_twap_sixty", "type": "update", "payload": {
            "symbol": "btc/usd", "timestamp": T0 * 1000, "window_s": 60,
            "value": 80000.12, "full_accuracy_value": "80000123456780000000000"}}]

        class Socket:
            async def send(self, raw):
                sent.append(raw)

            async def recv(self):
                sub = json.loads(sent[0])
                # The simulated venue accepts the SDK's update subscription.
                # The old wildcard + filter must fail this end-to-end check.
                if sub != {"action": "subscribe", "subscriptions": [
                        {"topic": "crypto_prices_twap_sixty", "type": "update"}]}:
                    return json.dumps({"type": "error", "code": "invalid_type"})
                if frames:
                    return json.dumps(frames.pop(0))
                ready.set()
                await asyncio.Future()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        ref = P.PolymarketPrices(types.SimpleNamespace(window_end=T0 + 300))

        async def heartbeat(ws):
            try:
                await ws.send("PING")
                await asyncio.Future()
            finally:
                cleaned.set()

        ref._legacy_ping = heartbeat
        with patch.object(P.websockets, "connect", return_value=Socket()) as connect, \
                patch.object(P, "ws_ssl", return_value=None), patch.object(P.time, "time", return_value=T0 + 1):
            task = asyncio.create_task(ref.stream())
            await asyncio.wait_for(ready.wait(), 1)
            self.assertEqual(ref.snapshot(T0 + 300)["current"], 80000.12345678)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(connect.call_args.args[0], P.RTDS)
        sub = json.loads(sent[0])
        self.assertNotIn("auth", sub)
        self.assertEqual(sub["subscriptions"][0]["topic"], "crypto_prices_twap_sixty")
        self.assertEqual(sub["subscriptions"][0]["type"], "update")
        self.assertNotIn("filters", sub["subscriptions"][0])
        self.assertIn("PING", sent)
        self.assertTrue(cleaned.is_set())
        self.assertFalse(ref.connected)

    async def test_auth_rejection_surfaces_without_logging_server_text_or_credentials(self):
        warned = asyncio.Event()
        events = []

        def on_event(*args):
            events.append(args)
            if args[-1] == "warn":
                warned.set()

        class Socket:
            async def send(self, raw):
                pass

            async def recv(self):
                return json.dumps({"op": "error", "code": "auth_failed", "message": "server-fixture-secret"})

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        creds = {"apiKey": "credential-fixture", "secret": "credential-fixture", "passphrase": "credential-fixture"}
        ref = P.PolymarketPrices(types.SimpleNamespace(window_end=T0 + 300), creds=creds, on_event=on_event)
        with patch.object(P.websockets, "connect", return_value=Socket()), patch.object(P, "ws_ssl", return_value=None):
            task = asyncio.create_task(ref.stream())
            await asyncio.wait_for(warned.wait(), 1)
            snap = ref.snapshot(T0 + 300)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(snap["status"], "AUTH REQUIRED")
        self.assertIn("valid L2", snap["detail"])
        self.assertNotIn("server-fixture-secret", str(events) + str(snap))
        self.assertNotIn("credential-fixture", str(events) + str(snap))

    async def test_polybolt_auth_then_subscribe_and_cancel_cleans_connection(self):
        sent = []
        frames = [{"op": "authed"}, message()]
        ready = asyncio.Event()

        class Socket:
            async def send(self, raw):
                sent.append(json.loads(raw))

            async def recv(self):
                if frames:
                    return json.dumps(frames.pop(0))
                ready.set()
                await asyncio.Future()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        creds = {"apiKey": "fixture", "secret": "fixture", "passphrase": "fixture"}
        ref = P.PolymarketPrices(types.SimpleNamespace(window_end=T0 + 300), creds=creds)
        with patch.object(P.websockets, "connect", return_value=Socket()) as connect, \
                patch.object(P, "ws_ssl", return_value=None), patch.object(P.time, "time", return_value=T0 + 1):
            task = asyncio.create_task(ref.stream())
            await asyncio.wait_for(ready.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(connect.call_args.args[0], P.POLYBOLT)
        self.assertEqual([s["op"] for s in sent], ["auth", "subscribe"])
        self.assertEqual(sent[1]["rid"], "reference-prices")
        self.assertEqual(sent[1]["subscriptions"][0]["channel"], "price.crypto.twap")
        self.assertNotIn("fixture", repr(ref))
        self.assertFalse(ref.connected)


class Display(unittest.TestCase):
    def test_waiting_reason_is_visible_in_the_panel_and_plain_status(self):
        snap = {"polymarket": {"current": None, "price_to_beat": None, "status": "ERROR",
                               "detail": "public RTDS connection failed (TimeoutError)",
                               "ptb_detail": "Gamma has not published this round's price to beat"}}
        for cols, rows in ((80, 24), (120, 40), (200, 40), (56, 24), (3, 2)):
            frame = build(snap, cols, rows, UNICODE)
            text = "\n".join("".join(s for s, _ in row) for row in frame)
            self.assertNotIn("layout error", text)
            self.assertEqual(len(frame), rows)
            if cols >= 80:
                self.assertIn("public RTDS", text)
                self.assertIn("TimeoutError", text)
                self.assertIn("ERROR", text)
        self.assertIn("poly_detail=public RTDS", plain_line(snap))

    def test_new_panel_keeps_settlement_and_execution_panels_visible(self):
        from tests_dashboard import populated
        snap = populated("LIVE")
        snap["polymarket"] = {"current": 80024.12, "price_to_beat": 80000.34, "status": "LIVE"}
        for cols, rows in ((80, 24), (120, 40), (140, 52), (160, 60)):
            frame = build(snap, cols, rows, UNICODE)
            text = "\n".join("".join(s for s, _ in row) for row in frame)
            self.assertEqual(len(frame), rows)
            self.assertTrue(all(sum(len(s) for s, _ in row) == cols for row in frame))
            self.assertIn("POLYMARKET BTC", text)
            self.assertIn("SETTLEMENT", text)
            if rows >= 52:
                self.assertIn("SNIPER EXECUTION LATENCY", text)
                self.assertIn("EXECUTION QUALITY", text)

    def test_polymarket_has_its_own_section_and_cent_precision(self):
        snap = {"polymarket": {"current": 80024.12, "price_to_beat": 80000.34,
                               "distance_bp": 2.97, "status": "LIVE", "age_s": 0.5}}
        for cols, rows in ((80, 24), (112, 40), (120, 40), (160, 48), (56, 24)):
            text = "\n".join("".join(s for s, _ in row) for row in build(snap, cols, rows, UNICODE))
            self.assertIn("POLYMARKET BTC", text)
            self.assertIn("$80,024.12", text)
            self.assertIn("$80,000.34", text)
            self.assertIn("Price to Beat", text)
            self.assertIn("LIVE", text)
            self.assertIn("+2.97bp", text)
        self.assertIn("poly_btc=80024.12 ptb=80000.34", plain_line(snap))

    def test_dashboard_never_substitutes_trigger_or_previous_round_values(self):
        hub = types.SimpleNamespace(up_token=None, down_token=None, condition_id=None,
                                    window_start=T0, window_end=T0 + 300, health=lambda: {},
                                    book=types.SimpleNamespace(), fill_store=types.SimpleNamespace(summary=lambda: {}))
        ref = P.PolymarketPrices(hub)
        rt = types.SimpleNamespace(mode="PAPER", paper=True, hub=hub, sniper=None, reference_prices=ref,
                                   ledger=None, broker=None, live_exec=None, reconciler=None, settler=None,
                                   journal_path=None, clock_ok=True, clock_detail="fixture", started=0)
        view = run.DashView(rt)
        view._btc_start = 99999       # old dashboard fallback must never leak
        self.assertIsNone(view.snapshot()["market"]["btc_start"])
        ref._remember(T0, 80000.34, "Gamma priceToBeat")
        self.assertEqual(view.snapshot()["market"]["btc_start"], 80000.34)
        hub.window_start, hub.window_end = T0 + 300, T0 + 600
        snap = view.snapshot()
        self.assertIsNone(snap["market"]["btc_start"])
        self.assertIsNone(snap["polymarket"]["price_to_beat"])


if __name__ == "__main__":
    unittest.main()
