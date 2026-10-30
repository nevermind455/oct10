"""Polymarket's BTC/USD reference prices, separate from the sniper trigger.

The current five-minute markets use Chainlink's 60-second TWAP. Consume that
published stream directly; averaging Coinbase/Binance prices cannot reproduce
it. Only a report stamped exactly at the boundary, or Gamma's published value
for the exact round, supplies the price to beat. No nearest-tick guess.

Protocol: https://docs.polymarket.com/api-reference/wss/polybolt
Legacy encoding: polymarket-client 0.13.0 models/rtds_events.py (E18 TWAP).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import math
import time
from decimal import Decimal, InvalidOperation

import websockets

from feeds.tls import ws_ssl

POLYBOLT = "wss://ws-live-v2.polymarket.com/ws"
RTDS = "wss://ws-live-data.polymarket.com"
ROUND_S = 300
STALE_S = 10.0
META_POLL_S = 10.0


def _positive(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


class PolymarketPrices:
    def __init__(self, hub, *, creds=None, on_event=None, fetch_event=None):
        self.hub = hub
        self._creds = dict(creds or {})
        self._event = on_event or (lambda *args: None)
        self._fetch_event = fetch_event
        self.latest = None                 # (source timestamp seconds, USD, receipt monotonic)
        self.beats = {}                    # round start -> (USD, provenance)
        self.connected = False
        self.detail = "waiting for Polymarket's Chainlink TWAP"

    def __repr__(self):
        return "PolymarketPrices()"       # credentials never reach logs

    def _remember(self, start, price, source):
        self.beats[start] = (price, source)
        for old in sorted(self.beats)[:-8]:
            del self.beats[old]

    def ingest(self, message, *, wall=None, mono=None):
        """Validate source, symbol, window, units and source time before using a price."""
        if not isinstance(message, dict):
            return
        legacy = message.get("topic") == "crypto_prices_twap_sixty"
        if not legacy and message.get("channel") != "price.crypto.twap":
            return
        payload = message.get("payload")
        if not isinstance(payload, dict):
            return
        if payload.get("symbol") != ("btc/usd" if legacy else "btcusd"):
            return
        if payload.get("window_s" if legacy else "window_seconds") != 60:
            return
        if payload.get("source", "chainlink") != "chainlink":
            return
        wall = time.time() if wall is None else wall
        mono = time.monotonic() if mono is None else mono
        points = payload.get("data") if isinstance(payload.get("data"), list) else [payload]
        for point in points:
            if not isinstance(point, dict):
                continue
            stamp = _positive(point.get("timestamp"))
            if stamp is None:
                continue
            stamp /= 1000.0             # source time, never envelope/receipt time
            if stamp > wall + 2.0 or stamp < wall - 4 * ROUND_S:
                continue
            raw = point.get("full_accuracy_value", point.get("value"))
            if legacy:
                # RTDS's full_accuracy_value is an E18 integer. Its float value
                # is not a substitute; PolyBolt's decimal string is already USD.
                raw = point.get("full_accuracy_value")
                try:
                    raw = Decimal(str(raw)) / Decimal(10**18)
                except (InvalidOperation, ValueError):
                    continue
            price = _positive(raw)
            if price is None:
                continue
            if self.latest is None or stamp > self.latest[0]:
                self.latest = (stamp, price, mono)
            if stamp % ROUND_S == 0:
                # Published metadata takes precedence over a boundary report.
                if not str(self.beats.get(stamp, (None, ""))[1]).startswith("Gamma"):
                    self._remember(stamp, price, "Chainlink TWAP at round start")

    def price_to_beat(self, end):
        return self.beats.get(float(end) - ROUND_S, (None, None))[0] if end else None

    def snapshot(self, end, *, wall=None, mono=None):
        wall = time.time() if wall is None else wall
        mono = time.monotonic() if mono is None else mono
        beat, source = self.beats.get(float(end) - ROUND_S, (None, None)) if end else (None, None)
        latest = self.latest
        age = max(wall - latest[0], mono - latest[2], 0.0) if latest else None
        fresh = (self.connected and age is not None and age <= STALE_S
                 and end is not None and float(end) - ROUND_S <= latest[0] < float(end))
        current = latest[1] if fresh else None
        return {"current": current, "price_to_beat": beat, "ptb_source": source,
                "status": "LIVE" if fresh else "STALE" if latest else "WAITING",
                "age_s": age, "source": "Chainlink BTC/USD 60s TWAP",
                "distance_bp": (current - beat) / beat * 1e4 if current and beat else None,
                "detail": self.detail if not fresh else ""}

    async def metadata_once(self):
        """Read exact-round metadata, including after a mid-round restart.

        The preceding contiguous round's final TWAP is the current opening
        TWAP. Require its exact identity, window and oracle config as well.
        Results stay keyed by the requested start if discovery rotates in flight.
        """
        import market_discovery as discovery
        import price_to_beat

        end = getattr(self.hub, "window_end", None)
        if end is None or not math.isfinite(float(end)) or float(end) % ROUND_S:
            return
        start = int(end) - ROUND_S
        if self.beats.get(start, (None, None))[1] == "Gamma priceToBeat":
            return
        fetch = self._fetch_event or discovery._fetch_slug
        for boundary in (start, start - ROUND_S):
            if boundary != start and start in self.beats:
                break
            slug = f"btc-updown-5m-{boundary}"
            event = await asyncio.to_thread(fetch, slug)
            if not isinstance(event, dict) or event.get("slug") != slug:
                continue
            markets = [m for m in discovery._as_list(event.get("markets"))
                       if isinstance(m, dict) and discovery._valid_window(m, boundary)
                       and discovery._valid_crypto_market_config(m)]
            if len(markets) != 1:
                continue
            opening, final = price_to_beat.official_from_event(event)
            value = opening if boundary == start else final
            if value is not None:
                self._remember(start, value, "Gamma priceToBeat" if boundary == start else "Gamma previous finalPrice")
                break

    async def metadata_loop(self):
        while True:
            try:
                await self.metadata_once()
            except Exception:
                # The read is retried; no exception text can disclose credentials.
                pass
            await asyncio.sleep(META_POLL_S)

    async def _legacy_ping(self, ws):
        while True:
            await asyncio.sleep(5)
            await ws.send("PING")

    async def _receive(self, ws, authed):
        heartbeat = None if authed else asyncio.create_task(self._legacy_ping(ws))
        started = time.monotonic()
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), 30)
                if raw in ("PING", "PONG", "ping", "pong", ""):
                    if raw in ("PING", "ping"):
                        await ws.send("PONG" if raw == "PING" else "pong")
                else:
                    message = json.loads(raw)
                    if isinstance(message, dict) and message.get("op") == "error":
                        raise ValueError("reference subscription refused")
                    self.ingest(message)
                if time.monotonic() - max(started, self.latest[2] if self.latest else started) > 30:
                    raise TimeoutError("reference stream stale")
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat

    async def stream(self):
        """Reference data only; never creates credentials, signs or places orders.

        Live reuses its existing CLOB credentials for PolyBolt. Paper can use
        explicitly supplied L2 credentials, or the legacy public TWAP feed while
        Polymarket keeps it available. A failure is visible without stopping orders.
        """
        authed = all(self._creds.get(k) for k in ("apiKey", "secret", "passphrase"))
        url = POLYBOLT if authed else RTDS
        retry = 1.0
        while True:
            try:
                async with websockets.connect(url, ssl=ws_ssl(url), open_timeout=10,
                                              close_timeout=2, ping_interval=20,
                                              max_size=262144) as ws:
                    if authed:
                        await ws.send(json.dumps({"op": "auth", "rid": "reference-auth", "auth": self._creds}))
                        reply = json.loads(await asyncio.wait_for(ws.recv(), 10))
                        if not isinstance(reply, dict) or reply.get("op") != "authed":
                            raise ValueError("reference authentication not accepted")
                        sub = {"op": "subscribe", "subscriptions": [{"channel": "price.crypto.twap",
                               "filter": {"symbol": "btcusd", "window_seconds": 60, "provider": "chainlink"}}]}
                    else:
                        sub = {"action": "subscribe", "subscriptions": [{"topic": "crypto_prices_twap_sixty",
                               "type": "*", "filters": json.dumps({"symbol": "btc/usd"})}]}
                    await ws.send(json.dumps(sub))
                    self.latest = None          # a reconnect needs a new report/snapshot
                    self.connected = True
                    self.detail = "waiting for a fresh Chainlink TWAP report"
                    self._event("poly_price", "connected to Polymarket Chainlink 60s TWAP", "info")
                    await self._receive(ws, authed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.detail = f"Polymarket feed reconnecting ({type(exc).__name__})"
                if not authed:
                    self.detail += "; PolyBolt requires L2 API credentials if public RTDS is unavailable"
                self._event("poly_price", self.detail, "warn")
            finally:
                self.connected = False
            await asyncio.sleep(retry)
            retry = min(30.0, retry * 2)
