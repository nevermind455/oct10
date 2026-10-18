"""Feed hub and the atomic market snapshot.

The strategy reads ONE object. Everything in it was copied under a single
lock acquisition, so a reader can never see this round's bid against last
round's ask, or a book that is half-way through a delta.

    BINANCE WS  -> btc
    POLY MKT WS -> up / down books
    POLY USER WS-> fills
    CHAINLINK   -> oracle (unchanged; still a REST call on the bot's schedule)
              |
              v
        MarketSnapshot   (frozen)
              |
              v
        EXISTING STRATEGY
"""
from __future__ import annotations

import asyncio
import math
import os
import threading
import time
from dataclasses import dataclass

from .binance import BinanceTradeFeed
from .book import BookState, BookView
from .health import LIVE, UNSYNCED, worst
from .poly_market import PolyMarketFeed
from .poly_user import FillStore, PolyUserFeed

DEFAULT_BINANCE = "wss://stream.binance.com:9443/ws/btcusdt@trade"
DEFAULT_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
DEFAULT_USER = "wss://ws-subscriptions-clob.polymarket.com/ws/user"


@dataclass(frozen=True)
class MarketSnapshot:
    """One consistent read of the world. Frozen on purpose."""
    taken_mono: float
    taken_wall: float

    # BTC
    btc_price: float | None
    btc_price_age_ms: float | None
    btc_status: str
    btc_fresh: float | None            # None when older than the freshness window

    # books
    up_token: str | None
    down_token: str | None
    up: BookView | None
    down: BookView | None
    book_status: str

    # order state
    fills: dict
    user_status: str

    # aggregate
    overall: str
    generation: int

    def book_age_ms(self, side: str = "UP") -> float | None:
        v = self.up if side.upper() == "UP" else self.down
        return None if v is None else v.book_age_ms()

    def as_rest(self, side: str = "UP"):
        v = self.up if side.upper() == "UP" else self.down
        return ([], []) if v is None else v.as_rest()

    @property
    def tradeable(self) -> bool:
        """Every feed the decision depends on is LIVE. Advisory - nothing in
        this package acts on it; it is here so a caller can."""
        return self.btc_status == LIVE and self.book_status == LIVE


class FeedHub:
    """Owns every feed, supervises them, and publishes snapshots."""

    def __init__(self, *, creds: dict | None = None, on_event=None,
                 btc_stale_after: float = 3.0, book_stale_after: float = 8.0,
                 rest_book_fetch=None, urls: dict | None = None,
                 book_refresh_after: float | None = None,
                 rest_book_timeout: float = 10.0, print_log=None) -> None:
        urls = urls or {}
        self._lock = threading.RLock()
        self._on_event = on_event
        self.book = BookState(stale_after=book_stale_after)
        if book_refresh_after is None:
            try:
                sniper_budget = float(os.environ.get("SNIPER_STALE_MS", "2000")) / 1000.0
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("SNIPER_STALE_MS must be a finite positive number") from exc
            if not math.isfinite(sniper_budget) or sniper_budget <= 0:
                raise ValueError("SNIPER_STALE_MS must be a finite positive number")
            book_refresh_after = 0.75 * min(book_stale_after, sniper_budget)
        if (not math.isfinite(book_refresh_after) or book_refresh_after <= 0):
            raise ValueError("book refresh age must be finite and positive")
        self.book_refresh_after = book_refresh_after
        self._book_refresh_check_s = min(0.25, book_refresh_after / 4.0)
        if not math.isfinite(rest_book_timeout) or rest_book_timeout <= 0:
            raise ValueError("REST book timeout must be finite and positive")
        self.rest_book_timeout = float(rest_book_timeout)
        self.fill_store = FillStore()

        kw = {"on_event": on_event}
        self.binance = BinanceTradeFeed(
            urls.get("binance") or DEFAULT_BINANCE,
            stale_after=btc_stale_after, on_price=self._on_price, **kw)
        self.market = PolyMarketFeed(urls.get("poly_market", DEFAULT_MARKET),
                                     book=self.book, stale_after=book_stale_after,
                                     request_resync=self._request_resync, print_log=print_log, **kw)
        self.user = PolyUserFeed(creds, urls.get("poly_user", DEFAULT_USER),
                                 store=self.fill_store, **kw)

        self.up_token: str | None = None
        self.down_token: str | None = None
        self.condition_id: str | None = None
        self.window_start: int | None = None
        self.window_end: int | None = None
        self.prepared_round: dict | None = None
        self._user_markets: list[str] = []
        self.generation = 0

        self._rest_book_fetch = rest_book_fetch   # callable(token) -> (bids, asks)
        self._resync_wanted: set[str] = set()
        self._next_book_fetch_at: dict[str, tuple[int, float]] = {}
        self._next_ws_snapshot_at: dict[str, tuple[int, float]] = {}
        # One background recovery per token, and one physical HTTP worker
        # per token even if an async waiter times out or the hub restarts.
        self._book_recovery_tasks: dict[str, asyncio.Task] = {}
        self._book_fetches: dict[str, tuple[asyncio.Task, BookView]] = {}
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._resync_event = asyncio.Event()
        self._resync_loop_owner: asyncio.AbstractEventLoop | None = None
        self.rest_resyncs = 0
        self.rest_fallbacks = 0

    # -------------------------------------------------------------- price
    def _on_price(self, price: float, mono: float, exchange_ts_ms: int | None = None) -> None:
        """Receive-path callback. Assignment only."""
        return None

    # ------------------------------------------------------------ rotation
    def _desired_market_tokens_locked(self) -> list[str]:
        """Return the public subscriptions implied by the locked hub state."""
        tokens = [t for t in (self.up_token, self.down_token) if t]
        if self.prepared_round:
            tokens.extend((self.prepared_round["up_token_id"],
                           self.prepared_round["down_token_id"]))
        return list(dict.fromkeys(tokens))

    def _reconcile_round_consumers_locked(self) -> None:
        """Make downstream subscription intent match the locked hub state.

        ``set_round`` is called both on the event-loop thread and from market
        discovery workers.  The state mutation and these in-memory setters
        must therefore be one serialized transition.  Applying them after
        releasing ``_lock`` lets an older transition overwrite a newer one.

        Both setters are idempotent: a same-state call repairs drift without
        sending a subscription update or resetting a healthy book.
        """
        desired_tokens = self._desired_market_tokens_locked()
        active_before = set(self.book.active)
        self.market.set_tokens(desired_tokens)
        # A same-state reconciliation may have had to re-add a missing token.
        # It now needs the same REST-resync treatment as a normal transition.
        self._resync_wanted.update(set(desired_tokens) - active_before)
        # The prepared next round's market too: its private fill channel is
        # subscribed (and proven by heartbeat) before its boundary, so the first
        # order of the round is not refused for coverage (audit M5).
        markets = list(self._user_markets)
        prepared = (self.prepared_round or {}).get("condition_id")
        if prepared and prepared not in markets:
            markets.append(prepared)
        self.user.set_markets(tuple(markets))

    def set_round(self, up_token, down_token, condition_id=None,
                  window_start=None, window_end=None) -> bool:
        """Point every feed at this round's market. Returns True on change.

        Called from the strategy path; it does no network. Hub state and feed
        intent change atomically here, then feed tasks send any wire updates.
        """
        up, down = (str(up_token) if up_token else None,
                    str(down_token) if down_token else None)
        condition = str(condition_id) if condition_id else None
        start = int(window_start) if window_start is not None else None
        end = int(window_end) if window_end is not None else None
        with self._lock:
            target = (up, down, condition, start, end)
            current = (self.up_token, self.down_token, self.condition_id,
                       self.window_start, self.window_end)
            changed = target != current
            if changed:
                self.up_token, self.down_token = up, down
                self.condition_id = condition
                self.window_start, self.window_end = start, end
                if (self.prepared_round
                        and (up, down) == (self.prepared_round["up_token_id"],
                                           self.prepared_round["down_token_id"])):
                    self.prepared_round = None
                self.generation += 1
                if condition:
                    if condition in self._user_markets:
                        self._user_markets.remove(condition)
                    self._user_markets.append(condition)
                    # Keep two hours of recent conditions for delayed trade
                    # lifecycle updates and the REST reconciliation window.
                    self._user_markets = self._user_markets[-24:]
                self._resync_wanted.update(t for t in (up, down) if t)
            # Do not early-return on equal hub fields: a prior interrupted or
            # buggy transition may have left a downstream consumer divergent.
            self._reconcile_round_consumers_locked()
            self._wake_resync()
            return changed

    def prepare_round(self, tokens: dict) -> bool:
        """Warm the next round's two public books without promoting it."""
        if not isinstance(tokens, dict):
            return False
        up = str(tokens.get("up_token_id") or "")
        down = str(tokens.get("down_token_id") or "")
        condition = str(tokens.get("condition_id") or "")
        if not up or not down or up == down or not condition:
            return False
        prepared = {
            "up_token_id": up, "down_token_id": down,
            "condition_id": condition,
            "window_start": tokens.get("window_start"),
            "window_end": tokens.get("window_end"),
        }
        with self._lock:
            changed = prepared != self.prepared_round
            if changed:
                self.prepared_round = prepared
                self._resync_wanted.update((up, down))
            # As with set_round, equal state must still be able to repair a
            # downstream subscription that was interrupted or corrupted.
            self._reconcile_round_consumers_locked()
            self._wake_resync()
            return changed

    def recent_markets(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._user_markets)

    # ------------------------------------------------------------ snapshot
    def snapshot(self) -> MarketSnapshot:
        """Atomic. One lock, everything copied, nothing live-referenced."""
        with self._lock:
            up_t, down_t, gen = self.up_token, self.down_token, self.generation
            # A market event can update both outcome tokens under one book
            # lock.  Read both under that same lock or a writer between two
            # view() calls would produce a torn cross-token snapshot.
            views = self.book.selected_views((up_t, down_t))
            up = views.get(up_t) if up_t else None
            down = views.get(down_t) if down_t else None
            btc_status = self.binance.refresh_status()
            book_status = self.market.refresh_status([up_t, down_t])
            user_status = self.user.refresh_status()
            return MarketSnapshot(
                taken_mono=time.monotonic(), taken_wall=time.time(),
                btc_price=self.binance.price,
                btc_price_age_ms=self.binance.price_age_ms,
                btc_status=btc_status,
                btc_fresh=self.binance.fresh_price(),
                up_token=up_t, down_token=down_t, up=up, down=down,
                book_status=book_status,
                fills=self.fill_store.summary(),
                user_status=user_status,
                overall=worst(btc_status, book_status),
                generation=gen,
            )

    def health(self) -> dict:
        self.binance.refresh_status()
        with self._lock:
            current_tokens = [t for t in (self.up_token, self.down_token) if t]
            prepared = dict(self.prepared_round) if self.prepared_round else None
        self.market.refresh_status(current_tokens)
        self.user.refresh_status()
        out = {f.name: f.health.as_dict() for f in (self.binance, self.market, self.user)}
        out["book"] = {
            "status": self.book.status(current_tokens),
            "active": self.book.active,
            "dropped_inactive": self.book.dropped_inactive,
            "needs_resync": self.book.needs_resync(),
            "rest_resyncs": self.rest_resyncs,
            "prepared_round": prepared,
        }
        return out

    # --------------------------------------------------------------- run
    def start(self, *, user: bool = True, binance: bool = True) -> list[asyncio.Task]:
        """Start public feeds and, only when requested, the private user feed.

        Passing ``user=False`` is stronger than constructing the user feed
        without credentials: no private-channel supervisor task exists and no
        connection attempt can be scheduled later in the process.
        """
        if any(not task.done() for task in self._tasks):
            return list(self._tasks)
        self._stop.clear()
        self._tasks = [self.market.start(),
                       asyncio.create_task(self._resync_loop(), name="feed:resync")]
        if binance:
            self._tasks.insert(0, self.binance.start())
        if user:
            self._tasks.append(self.user.start())
        return self._tasks

    async def stop(self) -> None:
        self._stop.set()
        for f in (self.binance, self.market, self.user):
            await f.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _resync_loop(self) -> None:
        """The ONLY place REST /book is used for the book.

        Runs on its own task so the receive callbacks stay allocation-only:
        a REST call inside a message handler stalls every later message
        behind it.
        """
        self._resync_loop_owner = asyncio.get_running_loop()
        limiter = asyncio.Semaphore(4)
        try:
            while not self._stop.is_set():
                try:
                    self._resync_event.clear()
                    for token, task in tuple(self._book_recovery_tasks.items()):
                        if task.done():
                            self._book_recovery_tasks.pop(token, None)
                            if not task.cancelled() and task.exception() is not None:
                                self.market.health.mark_error(task.exception())
                    with self._lock:
                        need = set(self.book.needs_resync(include_stale=True)) | set(self._resync_wanted)
                    for token, view in self.book.views().items():
                        age = view.book_age_ms()
                        if age is not None and age >= self.book_refresh_after * 1000.0:
                            need.add(token)
                    active = set(self.book.active)
                    need &= active
                    for token, (fetch_task, _view) in tuple(self._book_fetches.items()):
                        if token not in self._book_recovery_tasks and fetch_task.done():
                            # A timed-out request may finish after WS recovery
                            # or rotation. Consume its exception and discard;
                            # it has no authority to update a newer book.
                            if token not in need:
                                if not fetch_task.cancelled():
                                    fetch_task.exception()
                                self._book_fetches.pop(token, None)
                    self._next_book_fetch_at = {token: deadline
                                               for token, deadline in self._next_book_fetch_at.items()
                                               if token in active}
                    self._next_ws_snapshot_at = {token: deadline
                                                for token, deadline in self._next_ws_snapshot_at.items()
                                                if token in active}
                    if self._rest_book_fetch is not None:
                        for token in sorted(need):
                            if token not in self._book_recovery_tasks:
                                task = asyncio.create_task(self._resync_token(token, limiter),
                                                           name=f"feed:resync:{token[-6:]}")
                                self._book_recovery_tasks[token] = task
                    try:
                        await asyncio.wait_for(self._resync_event.wait(), timeout=self._book_refresh_check_s)
                    except asyncio.TimeoutError:
                        pass
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.market.health.mark_error(exc)
                    self._event("resync", f"loop error: {type(exc).__name__}: {exc}", "warn")
                    await asyncio.sleep(1.0)
        finally:
            pending = list(self._book_recovery_tasks.values())
            self._book_recovery_tasks.clear()
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def _resync_token(self, token: str, limiter: asyncio.Semaphore) -> None:
        async with limiter:
            if self._stop.is_set():
                return
            now = time.monotonic()
            view = self.book.view(token)
            prior_attempt = self._next_book_fetch_at.get(token)
            if (prior_attempt is not None and prior_attempt[0] == view.generation
                    and now < prior_attempt[1]):
                return
            age = view.book_age_ms()
            if (view.status == LIVE and age is not None
                    and age < self.book_refresh_after * 1000.0):
                with self._lock:
                    self._resync_wanted.discard(token)
                return
            # Also rate-limit failed and WS-raced refreshes. A fast HTTP error
            # must not turn the background deadline check into an API hot loop.
            self._next_book_fetch_at[token] = (view.generation, now + max(0.25, self.book_refresh_after))
            try:
                pending = self._book_fetches.get(token)
                if pending is None:
                    fetch_task = asyncio.create_task(asyncio.to_thread(self._rest_book_fetch, token),
                                                     name=f"feed:book-http:{token[-6:]}")
                    self._book_fetches[token] = (fetch_task, view)
                else:
                    fetch_task, view = pending
                bids, asks = await asyncio.wait_for(asyncio.shield(fetch_task),
                                                  timeout=self.rest_book_timeout)
                self._book_fetches.pop(token, None)
            except asyncio.TimeoutError as exc:
                # A Python worker cannot be killed. Keep its handle so no
                # replacement HTTP call accumulates behind a hung request.
                # A timeout raised BY a completed HTTP worker is different:
                # it can be discarded and retried after the rate-limit gate.
                if fetch_task.done():
                    self._book_fetches.pop(token, None)
                self.market.health.mark_error("REST book snapshot timed out")
                self._event("resync", f"REST snapshot timed out for {token[-6:]}", "warn")
                current = self.book.view(token)
                self._request_ws_snapshot(token, current)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._book_fetches.pop(token, None)
                self.market.health.mark_error(exc)
                self._event("resync", f"REST snapshot failed for {token[-6:]}: "
                            f"{type(exc).__name__}: {exc}", "warn")
                return
            if self.book.apply_snapshot(
                    token, bids, asks, only_if_unsynced=True,
                    expected_generation=view.generation, expected_updates=view.updates,
                    expected_receive_revision=view.receive_revision,
                    refresh_if_older_than=self.book_refresh_after):
                self.rest_resyncs += 1
                with self._lock:
                    self._resync_wanted.discard(token)
                self._event("resync", f"REST snapshot {token[-6:]}", "info")
            elif self.book.view(token).status == LIVE:
                with self._lock:
                    self._resync_wanted.discard(token)
            else:
                current = self.book.view(token)
                # Ignored UNSYNCED deltas now fence REST. On a busy market
                # every HTTP response can race; use a stream-ordered snapshot
                # instead of waiting indefinitely for a quiet fetch interval.
                if (current.status == UNSYNCED and current.generation == view.generation
                        and current.receive_revision != view.receive_revision):
                    self._request_ws_snapshot(token, current)

    def _request_ws_snapshot(self, token: str, view: BookView) -> bool:
        prior = self._next_ws_snapshot_at.get(token)
        now = time.monotonic()
        if (view.status == UNSYNCED
                and (prior is None or prior[0] != view.generation or now >= prior[1])
                and self.market.request_snapshot(token, expected_generation=view.generation)):
            self._next_ws_snapshot_at[token] = (
                view.generation, now + max(1.0, self.book_refresh_after))
            self._event("resync", f"WS snapshot requested {token[-6:]} after REST recovery delay", "info")
            return True
        return False

    def _wake_resync(self) -> None:
        loop = self._resync_loop_owner
        if loop is not None and not loop.is_closed():
            try:
                loop.call_soon_threadsafe(self._resync_event.set)
            except RuntimeError:
                pass

    def _request_resync(self, tokens) -> None:
        with self._lock:
            self._resync_wanted.update(tokens)
        self._wake_resync()

    def _event(self, source: str, text: str, level: str) -> None:
        if self._on_event:
            try:
                self._on_event(source, text, level)
            except Exception as exc:
                self.market.health.mark_error(
                    f"event callback failed: {type(exc).__name__}: {exc}")
