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
import collections
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


TRADE_PRINTS_KEPT = 2000          # per token: minutes of prints even in a busy round


def _finite(v) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


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
        self._snapshot_requests: dict[str, int] = {}
        self._subscription_send_lock = asyncio.Lock()
        self._resync = request_resync      # called with a token list, off-thread
        self.last_trade_price: dict[str, float] = {}
        self.pong_at: float | None = None
        self._ping_sent: float | None = None
        self._pong_event = asyncio.Event()
        self.invalid_messages = 0
        self.last_trade_ts: dict[str, int] = {}
        # Every accepted trade print per token (venue ms, price, size, taker side),
        # newest last: lets the sniper find its own trade's venue match time.
        self.trades: dict[str, collections.deque] = {}

    def trades_between(self, token, lo_ms: float, hi_ms: float) -> list[dict]:
        """Copies of this token's trade prints stamped lo_ms..hi_ms (venue ms)."""
        with self._sub_lock:
            q = list(self.trades.get(str(token)) or ())
        return [{"ts": t, "price": p, "size": s, "side": side} for t, p, s, side in q if lo_ms <= t <= hi_ms]

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
                self.trades = {token: q for token, q in self.trades.items() if token in keep}
                self.last_trade_ts = {
                    token: timestamp for token, timestamp in self.last_trade_ts.items()
                    if token in keep
                }
            # Intent and the active set are one serialized transition.
            # Same-state calls can still repair an interrupted transition.
            added, removed = self.book.set_active(tokens)
            self._snapshot_requests = {token: generation
                                       for token, generation in self._snapshot_requests.items()
                                       if token in self._want}
        self._wake_intent()
        if added or removed:
            self.event(f"rotate +{len(added)} -{len(removed)} -> {[t[-6:] for t in tokens]}")

    def request_snapshot(self, token: str, *, expected_generation: int) -> bool:
        """Request an ordered WS snapshot without disrupting sibling books.

        A REST response has no venue sequence to align with concurrent deltas.
        Re-subscribing an UNSYNCED token gets a full snapshot on the same
        ordered stream, so recovery does not require a quiet market. The hub
        rate-limits this fallback. No network runs on the caller's thread.
        """
        with self._sub_lock:
            view = self.book.view(token)
            if (token not in self._want or view.generation != expected_generation
                    or view.status != UNSYNCED):
                return False
            self._snapshot_requests[token] = expected_generation
        self._wake_intent()
        return True

    # ------------------------------------------------------------- status
    def refresh_status(self, tokens=None) -> str:
        self.health.status = self.book.status(tokens)
        with self._sub_lock:
            self.health.subscribed = tuple(sorted(self._sent))
        return self.health.status

    # ------------------------------------------------------------ session
    async def _session(self) -> None:
        self._bind_intent_loop()
        async with websockets.connect(
            self.url, ssl=ws_ssl(self.url), ping_interval=None,      # this venue wants app-level PING
            close_timeout=2, max_size=4 * 2 ** 20, open_timeout=10,
            compression=None,
        ) as ws:
            self._ws = ws
            with self._sub_lock:
                self._sent.clear()
                self._snapshot_requests.clear()
            # Reset heartbeat state on every socket. A disconnect between PING
            # and PONG must not poison every later reconnect.
            self._ping_sent = None
            self.pong_at = time.monotonic()
            self._pong_event.set()
            self.book.connected = True
            # Everything we knew is now a guess: deltas during the gap are lost.
            self.book.desync_all("connect")
            self._request_resync()
            self.health.mark_connected()
            self.health.status = UNSYNCED
            self.event("connected", "good")

            tasks = []
            try:
                await self._sync_subscriptions(initial=True)
                tasks = [asyncio.create_task(self._ping_loop(ws)),
                         asyncio.create_task(self._subscription_loop(ws))]
                while not self.stopping:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout)
                    except asyncio.TimeoutError:
                        self.event(f"silent {self.recv_timeout:.0f}s - reconnecting", "warn")
                        return
                    self._handle(raw)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._ws = None
                with self._sub_lock:
                    self._sent.clear()
                    self._snapshot_requests.clear()
                self.book.connected = False
                self.book.desync_all("disconnect")

    async def _sync_subscriptions(self, initial: bool = False) -> None:
        async with self._subscription_send_lock:
            await self._send_subscriptions(initial)

    async def _send_subscriptions(self, initial: bool = False) -> None:
        ws = self._ws
        if ws is None:
            return
        with self._sub_lock:
            want = set(self._want)
            sent = set(self._sent)
            requests = dict(self._snapshot_requests)
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
        for token, generation in requests.items():
            with self._sub_lock:
                view = self.book.view(token)
                valid = (token in self._want and token in self._sent
                         and token not in add and view.status == UNSYNCED
                         and view.generation == generation)
                if self._snapshot_requests.get(token) == generation:
                    self._snapshot_requests.pop(token, None)
            if not valid:
                continue
            # Same socket, unsubscribe then subscribe: the returned `book`
            # event and subsequent deltas have one authoritative order.
            await ws.send(json.dumps({"assets_ids": [token], "operation": "unsubscribe"}))
            with self._sub_lock:
                self._sent.discard(token)
                # Intent can change from the strategy thread during send.
                refreshed_generation = self.book.subscription_gap(token, expected_generation=generation)
                valid = token in self._want and refreshed_generation is not None
            if valid:
                msg = {"assets_ids": [token], "operation": "subscribe"}
                if self.custom_features:
                    msg["custom_feature_enabled"] = True
                await ws.send(json.dumps(msg))
                with self._sub_lock:
                    self._sent.add(token)

    async def _subscription_loop(self, ws) -> None:
        while True:
            # Clear before reading state: a worker notification during send
            # remains pending, and changed intent is reconciled immediately.
            self._intent_event.clear()
            with self._sub_lock:
                changed = set(self._want) != self._sent or bool(self._snapshot_requests)
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
                continue
            await self._intent_event.wait()

    async def _ping_loop(self, ws) -> None:
        while True:
            cycle_started = time.monotonic()
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
            # Send immediately after subscription, then on a fixed cadence.
            # Waiting a full interval after PONG made every RTT add heartbeat
            # drift and delayed initial connection proof by ten seconds.
            await asyncio.sleep(max(0.0, cycle_started + self.ping_every - time.monotonic()))

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
                self._request_resync()
        elif et == "price_change":
            ts, h = m.get("timestamp"), m.get("hash")
            applied = self.book.apply_price_changes(
                m.get("price_changes"), ts_ms=ts, hash_=h,
                require_exchange_ts=True)
            if not applied:
                self.invalid_messages += 1
                self.health.detail = "rejected invalid/stale price-change event"
                self._request_resync()
        elif et == "tick_size_change":
            tok = str(m.get("asset_id"))
            if self.book.set_tick_size(tok, m.get("new_tick_size"),
                                       ts_ms=m.get("timestamp"),
                                       require_exchange_ts=m.get("timestamp") is not None):
                self.event(f"tick {m.get('old_tick_size')} -> {m.get('new_tick_size')} "
                           f"on {tok[-6:]}", "warn")
            else:
                self.invalid_messages += 1
                self.health.detail = "rejected invalid/stale tick-size event"
        elif et == "last_trade_price":
            token = str(m.get("asset_id") or "")
            if token not in self.book.active:
                self.book.note_inactive_drop()
                return
            try:
                if isinstance(m.get("price"), bool) or isinstance(m.get("timestamp"), bool):
                    raise ValueError("boolean last-trade value")
                price = float(m.get("price"))
                timestamp = int(m.get("timestamp"))
            except (TypeError, ValueError, OverflowError):
                self.invalid_messages += 1
                self.health.detail = "rejected invalid last-trade price"
                return
            if (not math.isfinite(price) or not 0 < price < 1
                    or not self.book._fresh_exchange_ts(timestamp)):
                self.invalid_messages += 1
                self.health.detail = "rejected out-of-range/stale last-trade price"
                return
            self.book.note_exchange_ts(timestamp)
            with self._sub_lock:
                # Discovery can rotate tokens between parsing and assignment.
                if token not in self._want:
                    self.book.note_inactive_drop()
                    return
                previous = self.last_trade_ts.get(token)
                if timestamp is not None and previous is not None and timestamp < previous:
                    self.invalid_messages += 1
                    self.health.detail = "rejected out-of-order last-trade price"
                    return
                self.last_trade_price[token] = price
                if timestamp is not None:
                    self.last_trade_ts[token] = timestamp
                size = _finite(m.get("size"))
                self.trades.setdefault(token, collections.deque(maxlen=TRADE_PRINTS_KEPT)).append(
                    (timestamp, price, size, str(m.get("side") or "").upper() or None))
        elif et == "best_bid_ask":
            # top-of-book only; the full book is authoritative here - but its
            # time still shows how far the feed has got
            if str(m.get("asset_id") or "") in self.book.active:
                self.book.note_exchange_ts(m.get("timestamp"))
        elif et == "market_resolved":
            self.event(f"market resolved: winner {m.get('winning_outcome')}", "warn")
        else:
            self.invalid_messages += 1
            self.health.detail = f"ignored unknown market event {str(et)[:40]!r}"

    def _request_resync(self) -> None:
        if self._resync is not None:
            self._resync(self.book.needs_resync())
