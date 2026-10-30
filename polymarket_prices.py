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


class ReferenceFeedError(RuntimeError):
    """Only fixed, credential-free diagnostics may be displayed or logged."""
    def __init__(self, detail, *, auth=False):
        super().__init__(detail)
        self.auth = auth


def _check_error(message):
    if not isinstance(message, dict):
        return
    if message.get("op") != "error" and message.get("type") != "error":
        return
    payload = message.get("payload") if isinstance(message.get("payload"), dict) else {}
    code = message.get("code") or payload.get("code")
    if code in ("auth_required", "auth_failed", "invalid_credentials", "unauthorized"):
        raise ReferenceFeedError("Polymarket requires valid L2 API credentials", auth=True)
    raise ReferenceFeedError("Polymarket rejected the price subscription")


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
        self.state = "CONNECTING"
        self.transport = "PolyBolt" if all(self._creds.get(k) for k in ("apiKey", "secret", "passphrase")) else "public RTDS"
        self.detail = f"connecting to {self.transport}"
        self.metadata_detail = "waiting for round metadata"
        self._metadata_warning = None
        self.frames = 0
        self.accepted = 0

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
            self.accepted += 1
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
        status = ("LIVE" if fresh else self.state if self.state in ("ERROR", "AUTH REQUIRED")
                  else "STALE" if latest else "WAITING" if self.connected else "CONNECTING")
        return {"current": current, "price_to_beat": beat, "ptb_source": source,
                "status": status,
                "age_s": age, "source": "Chainlink BTC/USD 60s TWAP",
                "distance_bp": (current - beat) / beat * 1e4 if current and beat else None,
                "detail": self.detail if not fresh else "",
                "ptb_detail": self.metadata_detail if beat is None else "",
                "transport": self.transport, "frames": self.frames, "accepted": self.accepted}

    def _metadata_failed(self, detail):
        self.metadata_detail = detail
        if self._metadata_warning != detail:
            self._event("poly_price", detail, "warn")
            self._metadata_warning = detail

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
            self.metadata_detail = "waiting for market discovery"
            return
        start = int(end) - ROUND_S
        if self.beats.get(start, (None, None))[1] == "Gamma priceToBeat":
            self.metadata_detail = ""
            return
        fetch = self._fetch_event or discovery._fetch_slug
        problem = None
        for boundary in (start, start - ROUND_S):
            if boundary != start and start in self.beats:
                break
            slug = f"btc-updown-5m-{boundary}"
            try:
                event = await asyncio.to_thread(fetch, slug)
            except Exception as exc:
                problem = f"PTB metadata read failed ({type(exc).__name__})"
                continue
            if not isinstance(event, dict) or event.get("slug") != slug:
                problem = problem or "Gamma round metadata unavailable"
                continue
            markets = [m for m in discovery._as_list(event.get("markets"))
                       if isinstance(m, dict) and discovery._valid_window(m, boundary)
                       and discovery._valid_crypto_market_config(m)]
            if len(markets) != 1:
                problem = problem or "Gamma oracle/window validation failed"
                continue
            opening, final = price_to_beat.official_from_event(event)
            value = opening if boundary == start else final
            if value is not None:
                self._remember(start, value, "Gamma priceToBeat" if boundary == start else "Gamma previous finalPrice")
                self.metadata_detail = ""
                self._metadata_warning = None
                break
        if start not in self.beats:
            if problem:
                self._metadata_failed(problem)
            else:
                self.metadata_detail = "Gamma has not published this round's price to beat"
                self._metadata_warning = None

    async def metadata_loop(self):
        while True:
            try:
                await self.metadata_once()
            except Exception as exc:
                self._metadata_failed(f"PTB metadata read failed ({type(exc).__name__})")
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
                try:
                    raw = await asyncio.wait_for(ws.recv(), 30)
                except asyncio.TimeoutError:
                    raise ReferenceFeedError(f"no BTC/USD TWAP data in 30s ({self.frames} frames received)") from None
                if raw in ("PING", "PONG", "ping", "pong", ""):
                    if raw in ("PING", "ping"):
                        await ws.send("PONG" if raw == "PING" else "pong")
                else:
                    message = json.loads(raw)
                    self.frames += 1
                    _check_error(message)
                    previous = self.accepted
                    self.ingest(message)
                    if self.accepted > previous and self.state != "LIVE":
                        self.state = "LIVE"
                        self.detail = ""
                        self._event("poly_price", f"receiving BTC/USD 60s TWAP via {self.transport}", "good")
                if time.monotonic() - max(started, self.latest[2] if self.latest else started) > 30:
                    raise ReferenceFeedError(f"no fresh BTC/USD TWAP in 30s ({self.frames} frames received)")
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
                if self.state not in ("ERROR", "AUTH REQUIRED"):
                    self.state = "CONNECTING"
                    self.detail = f"connecting to {self.transport}"
                async with websockets.connect(url, ssl=ws_ssl(url), open_timeout=10,
                                              close_timeout=2, ping_interval=20,
                                              max_size=262144) as ws:
                    if authed:
                        await ws.send(json.dumps({"op": "auth", "rid": "reference-auth", "auth": self._creds}))
                        deadline = time.monotonic() + 10
                        while True:
                            reply = json.loads(await asyncio.wait_for(ws.recv(), max(0.01, deadline - time.monotonic())))
                            _check_error(reply)
                            if isinstance(reply, dict) and reply.get("op") == "authed":
                                break
                            if time.monotonic() >= deadline:
                                raise ReferenceFeedError("Polymarket authentication was not accepted", auth=True)
                        sub = {"op": "subscribe", "rid": "reference-prices", "subscriptions": [{"channel": "price.crypto.twap",
                               "filter": {"symbol": "btcusd", "window_seconds": 60, "provider": "chainlink"}}]}
                    else:
                        # Match polymarket-client's RtdsServerSubscription exactly.
                        # Subscribe to updates for all symbols, filter BTC locally.
                        # A wildcard type / server-side symbol filter can leave
                        # this legacy TWAP topic connected without any updates.
                        sub = {"action": "subscribe", "subscriptions": [{"topic": "crypto_prices_twap_sixty",
                               "type": "update"}]}
                    await ws.send(json.dumps(sub))
                    self.latest = None          # a reconnect needs a new report/snapshot
                    self.connected = True
                    self.state = "WAITING"
                    self.frames = self.accepted = 0
                    self.detail = f"{self.transport}: subscription sent; waiting for BTC/USD TWAP"
                    self._event("poly_price", self.detail, "info")
                    await self._receive(ws, authed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state = "AUTH REQUIRED" if isinstance(exc, ReferenceFeedError) and exc.auth else "ERROR"
                self.detail = str(exc) if isinstance(exc, ReferenceFeedError) else f"{self.transport} connection failed ({type(exc).__name__})"
                if not authed:
                    self.detail += "; if RTDS is retired, configure L2 price-feed credentials"
                self._event("poly_price", self.detail, "warn")
            finally:
                self.connected = False
            await asyncio.sleep(retry)
            retry = min(30.0, retry * 2)
