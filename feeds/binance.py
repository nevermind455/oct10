"""Binance BTC/USDT trade stream.

Binance sends a WS ping every 20s and disconnects if it does not get a pong
within 60s; the `websockets` library answers those automatically. We also send
our own client pings, which is what gives us a real latency_ms and detects a
half-open socket that a `recv()` alone would sit on forever.

Three independent liveness guards, because they fail differently:
  * `ping_timeout`  - the peer stopped answering pings (half-open socket)
  * receive timeout - the peer answers pings but sends no data
  * staleness       - data is arriving but the last PRICE is older than the
                      window (e.g. a stream that goes quiet mid-round)
"""
from __future__ import annotations

import asyncio
import json
import math
import time

import websockets

import timer
from .health import DISCONNECTED, LIVE, STALE
from .supervisor import SupervisedFeed
from .tls import ws_ssl

DEFAULT_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
# Market-data-only mirror; no user streams. Useful when the main host is
# blocked or rate-limited.
FALLBACK_URL = "wss://data-stream.binance.vision/ws/btcusdt@trade"


class BinanceTradeFeed(SupervisedFeed):
    name = "binance"

    def __init__(self, url: str = DEFAULT_URL, *, stale_after: float = 3.0,
                 recv_timeout: float = 10.0, ping_interval: float = 15.0,
                 ping_timeout: float = 10.0, max_session: float = 23 * 3600,
                 on_price=None, on_event=None, urls: tuple[str, ...] | None = None) -> None:
        super().__init__(on_event=on_event)
        self.urls = urls or (url, FALLBACK_URL)
        self._url_i = 0
        self._dry_sessions = 0
        self.stale_after = stale_after
        self.recv_timeout = recv_timeout
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_session = max_session      # rotate before Binance's 24h cut
        self._on_price = on_price

        self.price: float | None = None
        self.price_mono: float | None = None
        self.trade_ms: int | None = None    # exchange-side timestamp
        self.trade_id: int | None = None    # monotonically increasing per symbol
        self.skew_ms: float | None = None   # our clock vs exchange
        self.invalid_messages = 0
        self.accepted_trades = 0

    # ------------------------------------------------------------- status
    def refresh_status(self) -> str:
        h = self.health
        if h.status == DISCONNECTED:
            return DISCONNECTED
        age = self.price_age_ms
        h.status = LIVE if (age is not None and age <= self.stale_after * 1000.0) else STALE
        return h.status

    @property
    def price_age_ms(self) -> float | None:
        return None if self.price_mono is None else (time.monotonic() - self.price_mono) * 1000.0

    def fresh_price(self) -> float | None:
        """The price ONLY if it is inside the freshness window.

        Callers that must never act on an old print use this. `self.price` is
        the last value regardless of age and is what the display shows,
        clearly labelled with its age.
        """
        age = self.price_age_ms
        if (self.refresh_status() != LIVE or age is None
                or age > self.stale_after * 1000.0):
            return None
        return self.price

    # ------------------------------------------------------------ session
    async def _session(self) -> None:
        url = self.urls[self._url_i % len(self.urls)]
        host = url.split("/", 3)[2] if "/" in url else "<invalid-url>"
        self.event(f"connecting {host}")
        got = self.accepted_trades              # valid prices for THIS attempt
        try:
            async with websockets.connect(
                url, ssl=ws_ssl(url),
                # The explicit latency task below already sends protocol
                # pings and closes half-open sockets. Avoid two ping timers.
                ping_interval=None,
                close_timeout=2,
                max_size=2 ** 20,
                open_timeout=10,
                compression=None,
            ) as ws:
                self.health.mark_connected()
                self.price_mono = None          # a prior socket cannot prove freshness
                self.health.status = STALE      # not LIVE until data arrives
                self.health.subscribed = ("btcusdt@trade",)
                self.event("connected", "good")
                deadline = time.monotonic() + self.max_session
                latency_task = asyncio.create_task(
                    self._latency_loop(ws), name="feed:binance-latency")
                try:
                    while not self.stopping:
                        if time.monotonic() > deadline:
                            self.event("session age limit - rotating connection", "warn")
                            return
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout)
                        except asyncio.TimeoutError:
                            # Connected, pings answered, but no data. Treat as
                            # dead rather than sitting on a silent socket.
                            self.health.status = STALE
                            self.event(
                                f"no data for {self.recv_timeout:.0f}s - reconnecting", "warn")
                            return
                        self._handle(raw)
                        valid_at = self.price_mono or self.health.connected_at
                        if (valid_at is not None
                                and time.monotonic() - valid_at > self.recv_timeout):
                            raise ConnectionError("no valid price inside receive timeout")
                finally:
                    latency_task.cancel()
                    results = await asyncio.gather(latency_task, return_exceptions=True)
                    err = results[0] if results else None
                    if isinstance(err, Exception) and not isinstance(err, asyncio.CancelledError):
                        self.health.mark_error(err)
                        self.event(f"latency task failed: {type(err).__name__}: {err}", "warn")
        finally:
            # A connection refusal/HTTP 429 happens before entering the socket
            # context.  It still has to advance the endpoint ladder; otherwise
            # a blocked primary makes the configured mirror unreachable.
            if not self.stopping:
                if self.accepted_trades > got:
                    self._dry_sessions = 0
                else:
                    self._dry_sessions += 1
                    if self._dry_sessions >= 2:
                        self._url_i = (self._url_i + 1) % len(self.urls)
                        self._dry_sessions = 0
                        self.event("switching endpoint after 2 dry attempts", "warn")

    def _handle(self, raw: str | bytes) -> None:
        """Receive callback. Parse and assign only.

        No REST, no disk, no formatting, no rendering - anything expensive
        here shows up as book/price lag on every single message.
        """
        self.health.mark_message()
        try:
            msg = json.loads(raw)
        except Exception as exc:
            self.invalid_messages += 1
            self.health.detail = f"invalid Binance JSON: {type(exc).__name__}"
            return
        if not isinstance(msg, dict):
            self.invalid_messages += 1
            self.health.detail = "invalid Binance message: expected object"
            return
        if msg.get("e") == "serverShutdown":
            self.event("serverShutdown received - reconnecting", "warn")
            raise ConnectionError("serverShutdown")
        p = msg.get("p")
        if p is None:
            return
        try:
            if isinstance(p, bool):
                raise ValueError("boolean price")
            price = float(p)
        except (TypeError, ValueError, OverflowError):
            self.invalid_messages += 1
            self.health.detail = "invalid Binance trade price"
            return
        if not math.isfinite(price) or price <= 0:
            self.invalid_messages += 1
            self.health.detail = "out-of-range Binance trade price"
            return
        now = time.monotonic()
        ts = msg.get("T") or msg.get("E")
        try:
            trade_ms = int(ts)
        except (TypeError, ValueError, OverflowError):
            self.invalid_messages += 1
            self.health.detail = "invalid Binance exchange timestamp"
            return
        raw_trade_id = msg.get("t")
        try:
            if isinstance(raw_trade_id, bool):
                raise ValueError("boolean sequence id")
            trade_id = int(raw_trade_id) if raw_trade_id is not None else None
        except (TypeError, ValueError, OverflowError):
            self.invalid_messages += 1
            self.health.detail = "invalid Binance trade sequence id"
            return
        if trade_id is not None and trade_id < 0:
            self.invalid_messages += 1
            self.health.detail = "invalid Binance trade sequence id"
            return
        if (trade_id is not None and self.trade_id is not None
                and trade_id <= self.trade_id):
            self.invalid_messages += 1
            self.health.detail = "rejected duplicate/out-of-order Binance trade id"
            return
        if self.trade_ms is not None and trade_ms < self.trade_ms:
            self.invalid_messages += 1
            self.health.detail = "rejected out-of-order exchange timestamp"
            return
        try:
            skew_ms = timer.exchange_age_s(trade_ms) * 1000.0
        except (ValueError, OverflowError):
            self.invalid_messages += 1
            self.health.detail = "invalid Binance exchange timestamp"
            return
        # Receipt freshness is monotonic (fresh_price / stale_after). Exchange
        # age only rejects garbage stamps: CLOB time and Binance time are not
        # the same clock, and a 3s cross-venue offset must not drop a live tape.
        if not math.isfinite(skew_ms) or skew_ms < -5_000 or skew_ms > 60_000:
            self.invalid_messages += 1
            self.health.detail = f"rejected exchange timestamp skew {skew_ms:.0f}ms"
            return
        self.price = price
        self.price_mono = now
        self.trade_ms = trade_ms
        if trade_id is not None:
            self.trade_id = trade_id
        self.skew_ms = skew_ms
        self.accepted_trades += 1
        self.health.status = LIVE
        if self._on_price is not None:
            self._on_price(price, now, trade_ms)

    async def _latency_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            t0 = time.monotonic()
            try:
                pong = await ws.ping()
                await asyncio.wait_for(pong, timeout=self.ping_timeout)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.mark_error(exc)
                self.event(f"latency heartbeat failed: {type(exc).__name__}: {exc}", "warn")
                try:
                    await ws.close(code=1011, reason="latency heartbeat failed")
                except Exception as close_exc:
                    self.health.mark_error(close_exc)
                return
            self.health.latency_ms = (time.monotonic() - t0) * 1000.0
