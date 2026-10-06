"""Polymarket CLOB market channel.

    wss://ws-subscriptions-clob.polymarket.com/ws/market

Public, no auth. Subscribe by asset id (token id):

    {"assets_ids": [...], "type": "market", "custom_feature_enabled": true}

The server drops a connection that does not send an application-level `PING`
roughly every 10 seconds and replies `PONG`. That is not the WebSocket ping
frame - a client that only relies on protocol pings gets disconnected about
every 10s and looks like a flaky venue.

Round rotation uses `{"assets_ids": [...], "operation": "subscribe"}` /
`"unsubscribe"` on the open socket, so a new 5-minute market does not cost a
reconnect (and therefore does not cost a resync of the other token).
"""
from __future__ import annotations

import asyncio
import json
import math
import threading
import time

import websockets

from .book import BookState
from .health import UNSYNCED
from .supervisor import SupervisedFeed
from .tls import ws_ssl

MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_EVERY = 10.0


class PolyMarketFeed(SupervisedFeed):
    name = "poly_market"

    def __init__(self, url: str = MARKET_WS, *, book: BookState | None = None,
                 stale_after: float = 8.0, recv_timeout: float = 25.0,
                 request_resync=None, on_event=None,
                 custom_features: bool = True, ping_every: float = PING_EVERY) -> None:
        super().__init__(on_event=on_event)
        self.url = url
        self.book = book or BookState(stale_after=stale_after)
        self.recv_timeout = recv_timeout
        self.custom_features = custom_features
        self.ping_every = ping_every
        self._ws = None
        self._sub_lock = threading.RLock()
        self._want: list[str] = []
        self._sent: set[str] = set()
        self._resync = request_resync      # called with a token list, off-thread
        self.last_trade_price: dict[str, float] = {}
        self.pong_at: float | None = None
        self._ping_sent: float | None = None
        self._pong_event = asyncio.Event()
        self.invalid_messages = 0
        self.last_trade_ts: dict[str, int] = {}

    # ----------------------------------------------------------- rotation
    def set_tokens(self, tokens) -> None:
        """Point the feed at this round's UP/DOWN tokens.

        Safe to call from the strategy thread: it only records intent and
        wakes the socket task. No network happens on the caller.
        """
        tokens = [str(t) for t in tokens if t]
        with self._sub_lock:
            if tokens != self._want:
                self._want = tokens
                keep = set(tokens)
                self.last_trade_price = {
                    token: price for token, price in self.last_trade_price.items()
                    if token in keep
                }
                self.last_trade_ts = {
                    token: timestamp for token, timestamp in self.last_trade_ts.items()
                    if token in keep
                }
        # Reconcile BookState even when subscription intent is unchanged. An
        # interrupted older transition can leave `_want` correct while the
        # active set is empty or stale. BookState.set_active is idempotent: a
        # healthy repeat neither resets generations nor emits a rotate event.
        added, removed = self.book.set_active(tokens)
        if added or removed:
            self.event(f"rotate +{len(added)} -{len(removed)} -> {[t[-6:] for t in tokens]}")

    # ------------------------------------------------------------- status
    def refresh_status(self, tokens=None) -> str:
        self.health.status = self.book.status(tokens)
        with self._sub_lock:
            self.health.subscribed = tuple(sorted(self._sent))
        return self.health.status

    # ------------------------------------------------------------ session
    async def _session(self) -> None:
        async with websockets.connect(
            self.url, ssl=ws_ssl(self.url), ping_interval=None,      # this venue wants app-level PING
            close_timeout=2, max_size=4 * 2 ** 20, open_timeout=10,
        ) as ws:
            self._ws = ws
            with self._sub_lock:
                self._sent.clear()
            # Reset heartbeat state on every socket. A disconnect between PING
            # and PONG must not poison every later reconnect.
            self._ping_sent = None
            self.pong_at = time.monotonic()
            self._pong_event.set()
            self.book.connected = True
            # Everything we knew is now a guess: deltas during the gap are lost.
            self.book.desync_all("connect")
            self.health.mark_connected()
            self.health.status = UNSYNCED
            self.event("connected", "good")

            await self._sync_subscriptions(initial=True)
            ping_task = asyncio.create_task(self._ping_loop(ws))
            sub_task = asyncio.create_task(self._subscription_loop(ws))
            try:
                while not self.stopping:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout)
                    except asyncio.TimeoutError:
                        self.event(f"silent {self.recv_timeout:.0f}s - reconnecting", "warn")
                        return
                    self._handle(raw)
            finally:
                ping_task.cancel()
                sub_task.cancel()
                await asyncio.gather(ping_task, sub_task, return_exceptions=True)
                self._ws = None
                self.book.connected = False
                self.book.desync_all("disconnect")

    async def _sync_subscriptions(self, initial: bool = False) -> None:
        ws = self._ws
        if ws is None:
            return
        with self._sub_lock:
            want = set(self._want)
            sent = set(self._sent)
        if initial:
            if not want:
                return
            msg = {"assets_ids": sorted(want), "type": "market"}
            if self.custom_features:
                msg["custom_feature_enabled"] = True
            await ws.send(json.dumps(msg))
            with self._sub_lock:
                self._sent = set(want)
            return
        add = sorted(want - sent)
        drop = sorted(sent - want)
        # Subscribe first.  The active BookState already rejects old-token
        # messages, while missing the new token's one-time initial snapshot
        # would leave it UNSYNCED until REST recovery.
        if add:
            msg = {"assets_ids": add, "operation": "subscribe"}
            if self.custom_features:
                msg["custom_feature_enabled"] = True
            await ws.send(json.dumps(msg))
            with self._sub_lock:
                self._sent |= set(add)
        if drop:
            await ws.send(json.dumps({"assets_ids": drop, "operation": "unsubscribe"}))
            with self._sub_lock:
                self._sent -= set(drop)

    async def _subscription_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(0.25)
            with self._sub_lock:
                changed = set(self._want) != self._sent
            if changed:
                try:
                    await self._sync_subscriptions()
                except Exception as exc:
                    self.health.mark_error(exc)
                    try:
                        await ws.close(code=1011, reason="subscription update failed")
                    except Exception as close_exc:
                        self.health.mark_error(close_exc)
                    return

    async def _ping_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.ping_every)
            try:
                self._ping_sent = time.monotonic()
                self._pong_event.clear()
                await ws.send("PING")
                await asyncio.wait_for(self._pong_event.wait(), timeout=self.ping_every)
            except asyncio.TimeoutError:
                self.health.mark_error("application heartbeat response timeout")
                await ws.close(code=1011, reason="heartbeat response timeout")
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.mark_error(exc)
                try:
                    await ws.close(code=1011, reason="heartbeat failed")
                except Exception as close_exc:
                    self.health.mark_error(close_exc)
                return

    # ------------------------------------------------------------- receive
    def _handle(self, raw) -> None:
        """Parse and update state. Nothing else.

        No REST call, no file write, no rendering - a resync is requested by
        flipping a flag that a separate task acts on.
        """
        self.health.mark_message()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        text = raw.strip()
        if text == "PONG" or text == "pong":
            self.pong_at = time.monotonic()
            if self._ping_sent is not None:
                self.health.latency_ms = (self.pong_at - self._ping_sent) * 1000.0
            self._pong_event.set()
            return
        try:
            msg = json.loads(text)
        except Exception as exc:
            self.invalid_messages += 1
            self.health.detail = f"invalid websocket JSON: {type(exc).__name__}"
            return
        for item in (msg if isinstance(msg, list) else [msg]):
            if isinstance(item, dict):
                self._dispatch(item)
            else:
                self.invalid_messages += 1
                self.health.detail = "invalid market event: expected object"

    def _dispatch(self, m: dict) -> None:
        et = m.get("event_type")
        if et == "book":
            applied = self.book.apply_snapshot(
                str(m.get("asset_id")), m.get("bids"), m.get("asks"),
                ts_ms=m.get("timestamp"), hash_=m.get("hash"),
                require_exchange_ts=True)
            if not applied:
                self.invalid_messages += 1
                self.health.detail = "rejected invalid/stale book snapshot"
        elif et == "price_change":
            ts, h = m.get("timestamp"), m.get("hash")
            applied = self.book.apply_price_changes(
                m.get("price_changes"), ts_ms=ts, hash_=h,
                require_exchange_ts=True)
            if not applied:
                self.invalid_messages += 1
                self.health.detail = "rejected invalid/stale price-change event"
        elif et == "tick_size_change":
            tok = str(m.get("asset_id"))
            self.book.set_tick_size(tok, m.get("new_tick_size"))
            self.event(f"tick {m.get('old_tick_size')} -> {m.get('new_tick_size')} "
                       f"on {tok[-6:]}", "warn")
        elif et == "last_trade_price":
            token = str(m.get("asset_id") or "")
            if token not in self.book.active:
                self.book.note_inactive_drop()
                return
            try:
                price = float(m.get("price"))
                timestamp = int(m.get("timestamp")) if m.get("timestamp") is not None else None
            except (TypeError, ValueError):
                self.invalid_messages += 1
                self.health.detail = "rejected invalid last-trade price"
                return
            if not math.isfinite(price) or not 0 < price < 1:
                self.invalid_messages += 1
                self.health.detail = "rejected out-of-range last-trade price"
                return
            self.book.note_exchange_ts(timestamp)
            with self._sub_lock:
                previous = self.last_trade_ts.get(token)
                if timestamp is not None and previous is not None and timestamp < previous:
                    self.invalid_messages += 1
                    self.health.detail = "rejected out-of-order last-trade price"
                    return
                self.last_trade_price[token] = price
                if timestamp is not None:
                    self.last_trade_ts[token] = timestamp
        elif et == "best_bid_ask":
            # top-of-book only; the full book is authoritative here - but its
            # time still shows how far the feed has got
            self.book.note_exchange_ts(m.get("timestamp"))
        elif et == "market_resolved":
            self.event(f"market resolved: winner {m.get('winning_outcome')}", "warn")
        else:
            self.invalid_messages += 1
            self.health.detail = f"ignored unknown market event {str(et)[:40]!r}"
