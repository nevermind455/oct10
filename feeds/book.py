"""Local CLOB order book.

Built from a `book` snapshot then maintained with `price_change` deltas.
A `price_change` with size "0" removes the level.

Two rules that matter more than the arithmetic:

1. The CLOB does not replay deltas you missed. After any disconnect the local
   book is UNSYNCED and stays that way until a fresh snapshot lands (WS `book`
   event or a REST /book resync). Serving a gapped book as LIVE is how you end
   up trading against a book that stopped existing.

2. Tokens rotate every 5 minutes. Each token carries a generation; a message
   whose asset_id is not in the current active set is dropped and counted, so
   an in-flight update for the previous round can never land in this round's
   book.
"""
from __future__ import annotations

import collections
import threading
import time
import math
from dataclasses import dataclass

import config
import timer
from .health import DISCONNECTED, LIVE, STALE, UNSYNCED

Level = tuple[float, float]      # (price, size)


@dataclass(frozen=True)
class BookView:
    """Immutable point-in-time view. Safe to hand to any reader."""
    token: str
    generation: int
    status: str
    bids: tuple[Level, ...] = ()
    asks: tuple[Level, ...] = ()
    updated_mono: float | None = None
    exchange_ts_ms: int | None = None
    tick_size: float | None = None
    hash: str | None = None
    updates: int = 0
    # Includes refused/ignored venue messages, unlike `updates` which counts
    # installed book states. REST recovery must race against both.
    receive_revision: int = 0

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_bid_size(self) -> float | None:
        return self.bids[0][1] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def best_ask_size(self) -> float | None:
        return self.asks[0][1] if self.asks else None

    @property
    def spread(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        return None if (b is None or a is None) else a - b

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        return None if (b is None or a is None) else (a + b) / 2.0

    @property
    def depth_bid(self) -> float:
        return sum(s for _, s in self.bids)

    @property
    def depth_ask(self) -> float:
        return sum(s for _, s in self.asks)

    def book_age_ms(self) -> float | None:
        if self.updated_mono is None:
            return None
        return (time.monotonic() - self.updated_mono) * 1000.0

    def as_rest(self) -> tuple[list[dict], list[dict]]:
        """Same shape `orderbook.get_orderbook` returns, so any existing
        consumer works unchanged."""
        return ([{"price": str(p), "size": str(s)} for p, s in self.bids],
                [{"price": str(p), "size": str(s)} for p, s in self.asks])


class BookState:
    """All books for the currently active tokens."""

    def __init__(self, stale_after: float = 8.0, history_s: float = 30.0) -> None:
        self._lock = threading.RLock()
        self.stale_after = stale_after
        # Event-time sanity bounds. Separate from `stale_after`, which
        # governs liveness measured from receipt.
        self.future_tolerance = float(
            getattr(config, 'ORDERBOOK_FUTURE_TOLERANCE_SECONDS', 5.0))
        self.max_quiet = float(
            getattr(config, 'ORDERBOOK_MAX_QUIET_SECONDS', 900.0))
        self._bids: dict[str, dict[float, float]] = {}
        self._asks: dict[str, dict[float, float]] = {}
        self._meta: dict[str, dict] = {}
        self._active: set[str] = set()
        self._generation: dict[str, int] = {}
        self._cache: dict[str, tuple[int, BookView]] = {}
        self._dirty: dict[str, int] = {}
        self.dropped_inactive = 0
        # Deltas/snapshots refused on a SYNCED book (crossed result, malformed
        # values, an untrustworthy timestamp): each one desyncs its tokens.
        self.rejected_updates = 0
        self.connected = False
        # The last history_s seconds of each book, to read it as it stood at
        # a given VENUE time (the paper sniper's modelled match instant) no
        # matter how late a timer woke or how long the event loop stalled.
        # Writers never mutate a level dict once stored - every update swaps
        # in a new one - so an entry is just references: (key_ms, applied
        # monotonic, applied wall, exchange ts, bids, asks, tick). key_ms is
        # the exchange ts, never decreasing per token; a feed gap is an entry
        # with bids None. ts_hwm is the newest exchange time any message on
        # the feed carried: once it passes T, every change stamped <= T has
        # arrived (the feed is one ordered connection).
        self.history_s = float(history_s)
        self._hist: dict[str, collections.deque] = {}
        self.ts_hwm: int | None = None
        # Applied wall time minus exchange time of each price change (ms):
        # how old the venue's book is by the time it is in ours.
        self.feed_age_ms: collections.deque = collections.deque(maxlen=5000)

    # ------------------------------------------------------------ history
    def _note_ts(self, ts) -> None:
        if ts is not None and (self.ts_hwm is None or ts > self.ts_hwm):
            self.ts_hwm = ts

    def note_exchange_ts(self, ts_ms) -> None:
        """Any feed message's exchange time (trades too): advances ts_hwm."""
        ts = _int(ts_ms)
        if not self._fresh_exchange_ts(ts):
            return
        with self._lock:
            self._note_ts(ts)

    def _record(self, token: str, ts, *, gap: bool = False, mono=None, wall=None) -> None:
        if self.history_s <= 0:
            return
        h = self._hist.get(token)
        if h is None:
            h = self._hist[token] = collections.deque(maxlen=20000)
        prev = h[-1][0] if h else None
        key = ts if (ts is not None and not gap) else self.ts_hwm
        if key is None:
            key = prev if prev is not None else 0
        if prev is not None and key < prev:
            key = prev
        mono = time.monotonic() if mono is None else mono
        wall = time.time() if wall is None else wall
        if gap:
            h.append((key, mono, wall, None, None, None, None))
        else:
            h.append((key, mono, wall, ts, self._bids[token], self._asks[token],
                      self._meta[token].get("tick")))
        cutoff = mono - self.history_s
        while len(h) > 1 and h[1][1] < cutoff:      # keep the state in force at the window start
            h.popleft()

    def _entry_view(self, token: str, e) -> BookView:
        return BookView(token=token, generation=self._generation.get(token, 0), status=LIVE,
                        bids=tuple(sorted(e[4].items(), key=lambda kv: kv[0], reverse=True)),
                        asks=tuple(sorted(e[5].items(), key=lambda kv: kv[0])),
                        updated_mono=e[1], exchange_ts_ms=e[3], tick_size=e[6])

    def view_at(self, token: str, venue_ms: float):
        """(BookView, info) of the book as it stood at venue time venue_ms, or
        (None, info) with info["why"] when that cannot be known: the feed has
        not yet carried anything stamped after it, the history does not reach
        back that far, or the feed had a gap (disconnect/resync) then."""
        token = str(token)
        with self._lock:
            info = {"hwm_ms": self.ts_hwm}
            if self.ts_hwm is None or self.ts_hwm < venue_ms:
                info["why"] = "the feed has not yet passed that time"
                return None, info
            h = self._hist.get(token)
            if not h:
                info["why"] = "no book history for this token"
                return None, info
            for e in reversed(h):
                if e[0] <= venue_ms:
                    break
            else:
                info["why"] = "the book history does not reach back that far"
                return None, info
            if e[4] is None:
                info["why"] = "feed gap (disconnect or resync) at that time"
                return None, info
            info.update(book_ts=e[3], applied_wall=e[2])
            return self._entry_view(token, e), info

    def view_at_mono(self, token: str, mono: float):
        """The book as it stood in OUR hands at local monotonic time mono
        (what a reader at that instant saw); None if unknown."""
        with self._lock:
            h = self._hist.get(str(token))
            for e in reversed(h or ()):
                if e[1] <= mono:
                    return (None if e[4] is None else self._entry_view(str(token), e))
            return None

    def feed_age_summary(self):
        with self._lock:
            xs = sorted(self.feed_age_ms)
        if not xs:
            return None
        q = lambda p: round(xs[min(len(xs) - 1, int(len(xs) * p / 100))], 1)
        return {"p50": q(50), "p90": q(90), "p99": q(99), "max": round(xs[-1], 1), "n": len(xs)}

    # ------------------------------------------------------------ rotation
    def set_active(self, tokens) -> tuple[list[str], list[str]]:
        """Swap the active token set. Returns (added, removed).

        Removed tokens are erased outright - keeping them "just in case" is
        exactly how last round's book gets read as this round's.
        """
        tokens = [t for t in tokens if t]
        with self._lock:
            new = set(tokens)
            added = sorted(new - self._active)
            removed = sorted(self._active - new)
            for t in removed:
                self._bids.pop(t, None)
                self._asks.pop(t, None)
                self._meta.pop(t, None)
                self._cache.pop(t, None)
                self._dirty.pop(t, None)
            # A removed token's history is kept for history_s (an order's
            # markout may still read it), then dropped at a later rotation.
            cutoff = time.monotonic() - self.history_s
            for t in [t for t, h in self._hist.items() if t not in new and (not h or h[-1][1] < cutoff)]:
                del self._hist[t]
            for t in added:
                self._generation[t] = self._generation.get(t, 0) + 1
                self._bids[t] = {}
                self._asks[t] = {}
                self._meta[t] = {"synced": False, "updated": None, "ts": None,
                                 "tick": None, "hash": None, "updates": 0,
                                 "receive_revision": 0, "seen_ts": None}
                self._dirty[t] = 0
            self._active = new
            return added, removed

    @property
    def active(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._active))

    def note_inactive_drop(self) -> None:
        with self._lock:
            self.dropped_inactive += 1

    def desync_all(self, reason: str = "reconnect") -> None:
        """Mark every book untrustworthy. Called on disconnect: the deltas
        that happened while we were away are gone for good."""
        with self._lock:
            for t, m in self._meta.items():
                # An in-flight REST read from before the gap must not sync
                # the new socket session with a pre-gap snapshot.
                self._generation[t] = self._generation.get(t, 0) + 1
                m["synced"] = False
                m["desync_reason"] = reason
                self._dirty[t] = self._dirty.get(t, 0) + 1
                self._record(t, None, gap=True)

    def _desync_locked(self, tokens, reason: str) -> None:
        """A message for these tokens was refused: our copy no longer matches
        the venue's (an update is missing from it), so it is not LIVE any more.
        UNSYNCED until a fresh snapshot (the hub's REST resync picks it up).
        A refused delta used to leave the book LIVE without that change - a
        level the venue no longer had stayed best ask, and the sniper's cap
        and paper fills were set from it. Caller holds the lock."""
        for token in dict.fromkeys(tokens):
            m = self._meta.get(token)
            if token not in self._active or m is None or not m.get("synced"):
                continue
            m["synced"] = False
            self._generation[token] = self._generation.get(token, 0) + 1
            m["desync_reason"] = reason
            self._dirty[token] = self._dirty.get(token, 0) + 1
            self._record(token, None, gap=True)
            self.rejected_updates += 1

    # -------------------------------------------------------------- writes
    def _observe_locked(self, tokens, ts=None) -> None:
        """Fence in-flight REST even when a venue update cannot be applied.

        Do not add a history state or change the applied-update count: while
        UNSYNCED the levels still are unknown. Caller holds the book lock.
        """
        for token in set(tokens) & self._active:
            m = self._meta[token]
            m["receive_revision"] = m.get("receive_revision", 0) + 1
            if ts is not None and (m.get("seen_ts") is None or ts > m["seen_ts"]):
                m["seen_ts"] = ts
            self._dirty[token] = self._dirty.get(token, 0) + 1

    def subscription_gap(self, token: str, *, expected_generation: int) -> int | None:
        """Invalidate a token after unsubscribe, before requesting its snapshot.

        A previously queued WS snapshot can recover it while the unsubscribe
        send yields. Fence REST and mark that state unknown through the gap;
        the next subscription's full snapshot will recover it again.
        """
        with self._lock:
            if token not in self._active or self._generation.get(token) != expected_generation:
                return None
            self._observe_locked((token,))
            self._desync_locked((token,), "snapshot resubscription")
            return self._generation[token]

    def apply_snapshot(self, token: str, bids, asks, *, ts_ms=None, hash_=None,
                       tick=None, only_if_unsynced: bool = False,
                       expected_generation: int | None = None,
                       expected_updates: int | None = None,
                       expected_receive_revision: int | None = None,
                       refresh_if_older_than: float | None = None,
                       require_exchange_ts: bool = False) -> bool:
        """Full replace. Returns False if the token is not active."""
        with self._lock:
            if token not in self._active:
                self.dropped_inactive += 1
                return False
            m = self._meta[token]
            if expected_generation is not None and self._generation.get(token) != expected_generation:
                return False
            if expected_updates is not None and m.get("updates", 0) != expected_updates:
                return False
            if (expected_receive_revision is not None
                    and m.get("receive_revision", 0) != expected_receive_revision):
                return False
            incoming_ts = _int(ts_ms)
            # A REST recovery is guarded by its request's receive revision;
            # it is not itself an observed venue event.
            if expected_receive_revision is None:
                self._observe_locked((token,), incoming_ts if self._fresh_exchange_ts(incoming_ts) else None)
            if only_if_unsynced and m.get("synced") and self._status(token) != STALE:
                # A quiet but connected book may hit a consumer's shorter
                # freshness budget before the feed's own stale threshold.
                # Revalidation is safe only when no WS update or session gap
                # happened since the REST fetch began.
                updated = m.get("updated")
                if (refresh_if_older_than is None or expected_generation is None
                        or expected_updates is None or updated is None
                        or time.monotonic() - updated < refresh_if_older_than):
                    return False
            if require_exchange_ts and not self._fresh_exchange_ts(incoming_ts):
                self._desync_locked((token,), "snapshot timestamp refused")
                return False
            previous_ts = m.get("ts")
            seen_ts = m.get("seen_ts")
            if seen_ts is not None and (previous_ts is None or seen_ts > previous_ts):
                previous_ts = seen_ts
            if (incoming_ts is not None and previous_ts is not None
                    and incoming_ts < previous_ts):
                return False
            new_bids, valid_bids = _to_map(bids)
            new_asks, valid_asks = _to_map(asks)
            if not valid_bids or not valid_asks:
                self._desync_locked((token,), "invalid snapshot")
                return False
            if new_bids and new_asks and max(new_bids) >= min(new_asks):
                self._desync_locked((token,), "crossed snapshot")
                return False
            self._bids[token] = new_bids
            self._asks[token] = new_asks
            m.update(synced=True, updated=time.monotonic(), ts=incoming_ts,
                     hash=hash_, updates=m.get("updates", 0) + 1)
            if tick is not None:
                m["tick"] = tick
            self._dirty[token] = self._dirty.get(token, 0) + 1
            self._note_ts(incoming_ts)
            self._record(token, incoming_ts, mono=m["updated"])
            return True

    def apply_price_change(self, token: str, price, size, side, *, ts_ms=None,
                           hash_=None, require_exchange_ts: bool = False) -> bool:
        """One delta. size == 0 removes the level."""
        with self._lock:
            if token not in self._active:
                self.dropped_inactive += 1
                return False
            m = self._meta[token]
            incoming_ts = _int(ts_ms)
            self._observe_locked((token,), incoming_ts if self._fresh_exchange_ts(incoming_ts) else None)
            if not m.get("synced"):
                # A delta on an unsynced book cannot be trusted: we do not
                # know what it is being applied to. Wait for the snapshot.
                return False
            try:
                if isinstance(price, bool) or isinstance(size, bool):
                    raise ValueError("boolean delta value")
                p, s = float(price), float(size)
            except (TypeError, ValueError, OverflowError):
                self._desync_locked((token,), "invalid delta")
                return False
            side = str(side or "").upper()
            if (side not in ("BUY", "BID", "SELL", "ASK")
                    or not math.isfinite(p) or not math.isfinite(s)
                    or not 0 < p < 1 or s < 0):
                self._desync_locked((token,), "invalid delta")
                return False
            if require_exchange_ts and not self._fresh_exchange_ts(incoming_ts):
                self._desync_locked((token,), "delta timestamp refused")
                return False
            previous_ts = m.get("ts")
            if (incoming_ts is not None and previous_ts is not None
                    and incoming_ts < previous_ts):
                return False
            bids = dict(self._bids[token])
            asks = dict(self._asks[token])
            book = bids if side in ("BUY", "BID") else asks
            if s <= 0:
                book.pop(p, None)
            else:
                book[p] = s
            if bids and asks and max(bids) >= min(asks):
                self._desync_locked((token,), "delta crossed the book")
                return False
            self._bids[token], self._asks[token] = bids, asks
            m.update(updated=time.monotonic(), ts=incoming_ts,
                     updates=m.get("updates", 0) + 1)
            if hash_:
                m["hash"] = hash_
            self._dirty[token] = self._dirty.get(token, 0) + 1
            self._note_ts(incoming_ts)
            wall = time.time()
            if incoming_ts is not None:
                self.feed_age_ms.append(wall * 1000.0 - incoming_ts)
            self._record(token, incoming_ts, mono=m["updated"], wall=wall)
            return True

    def apply_price_changes(self, changes, *, ts_ms=None, hash_=None,
                            require_exchange_ts: bool = True) -> bool:
        """Apply one WS price-change event atomically across all its tokens.

        A multi-level event can momentarily look crossed if applied one row at
        a time. Readers must see either the complete old event or complete new
        event, never that intermediate state.
        """
        incoming_ts = _int(ts_ms)
        with self._lock:
            if not isinstance(changes, (list, tuple)):
                self._observe_locked(self._active)
                self._desync_locked(self._active, "malformed price-change collection")
                return False
            named = [str(c.get("asset_id") or "") for c in (changes or ()) if isinstance(c, dict)]
            # An unidentifiable row might belong to any active token. Refuse
            # it conservatively, including on books already awaiting recovery.
            malformed = any(not isinstance(c, dict) or not c.get("asset_id") for c in changes)
            observed = self._active if malformed else named
            self._observe_locked(observed, incoming_ts if self._fresh_exchange_ts(incoming_ts) else None)
            if malformed:
                self._desync_locked(observed, "malformed event")
                return False
            if require_exchange_ts and not self._fresh_exchange_ts(incoming_ts):
                self._desync_locked(named, "event timestamp refused")
                return False
            staged: dict[str, tuple[dict[float, float], dict[float, float]]] = {}
            token_hashes: dict[str, str] = {}
            touched: set[str] = set()
            skipped_pending = False
            for change in changes or ():
                if not isinstance(change, dict):
                    self._desync_locked(named, "malformed event")
                    return False
                token = str(change.get("asset_id") or "")
                if token not in self._active:
                    self.dropped_inactive += 1
                    continue
                meta = self._meta[token]
                if not meta.get("synced"):
                    # A sibling token still waiting for its snapshot must not
                    # discard deltas for a token that is already live.
                    skipped_pending = True
                    continue
                previous_ts = meta.get("ts")
                if (incoming_ts is not None and previous_ts is not None
                        and incoming_ts < previous_ts):
                    continue
                try:
                    if isinstance(change.get("price"), bool) or isinstance(change.get("size"), bool):
                        raise ValueError("boolean delta value")
                    price = float(change.get("price"))
                    size = float(change.get("size"))
                except (TypeError, ValueError, OverflowError):
                    self._desync_locked(named, "invalid delta")
                    return False
                side = str(change.get("side") or "").upper()
                if (side not in ("BUY", "BID", "SELL", "ASK")
                        or not math.isfinite(price) or not math.isfinite(size)
                        or not 0 < price < 1 or size < 0):
                    self._desync_locked(named, "invalid delta")
                    return False
                if token not in staged:
                    staged[token] = (dict(self._bids[token]), dict(self._asks[token]))
                bids, asks = staged[token]
                book = bids if side in ("BUY", "BID") else asks
                if size == 0:
                    book.pop(price, None)
                else:
                    book[price] = size
                touched.add(token)
                if change.get("hash"):
                    token_hashes[token] = change["hash"]

            for bids, asks in staged.values():
                if bids and asks and max(bids) >= min(asks):
                    self._desync_locked(list(staged), "delta crossed the book")
                    return False
            now_mono = time.monotonic()
            now_wall = time.time()
            self._note_ts(incoming_ts)
            if touched and incoming_ts is not None:
                self.feed_age_ms.append(now_wall * 1000.0 - incoming_ts)
            for token in touched:
                bids, asks = staged[token]
                self._bids[token], self._asks[token] = bids, asks
                meta = self._meta[token]
                meta.update(updated=now_mono, ts=incoming_ts,
                            updates=meta.get("updates", 0) + 1)
                token_hash = token_hashes.get(token)
                if token_hash or hash_:
                    meta["hash"] = token_hash or hash_
                self._dirty[token] = self._dirty.get(token, 0) + 1
                self._record(token, incoming_ts, mono=now_mono, wall=now_wall)
            return bool(touched) or skipped_pending

    def set_tick_size(self, token: str, tick) -> None:
        with self._lock:
            if token in self._meta:
                try:
                    if isinstance(tick, bool):
                        raise ValueError("boolean tick")
                    parsed = float(tick)
                except (TypeError, ValueError, OverflowError):
                    return
                if not math.isfinite(parsed) or parsed <= 0 or parsed >= 1:
                    return
                self._meta[token]["tick"] = parsed
                # Must invalidate: view() is cached on this counter, and a
                # stale tick size gets orders rejected by the venue.
                self._dirty[token] = self._dirty.get(token, 0) + 1
                if self._meta[token].get("synced"):
                    self._record(token, None)

    # --------------------------------------------------------------- reads
    def view(self, token: str) -> BookView:
        """Immutable snapshot of one book. Cached until the next write."""
        with self._lock:
            gen = self._generation.get(token, 0)
            if token not in self._active:
                return BookView(token=token, generation=gen, status=DISCONNECTED)
            dirty = self._dirty.get(token, 0)
            hit = self._cache.get(token)
            if hit and hit[0] == dirty:
                cached = hit[1]
                return cached._replace_status(self._status(token)) \
                    if cached.status != self._status(token) else cached
            m = self._meta.get(token, {})
            bids = tuple(sorted(self._bids.get(token, {}).items(),
                                key=lambda kv: kv[0], reverse=True))
            asks = tuple(sorted(self._asks.get(token, {}).items(),
                                key=lambda kv: kv[0]))
            v = BookView(token=token, generation=gen, status=self._status(token),
                         bids=bids, asks=asks, updated_mono=m.get("updated"),
                         exchange_ts_ms=m.get("ts"), tick_size=m.get("tick"),
                         hash=m.get("hash"), updates=m.get("updates", 0),
                         receive_revision=m.get("receive_revision", 0))
            self._cache[token] = (dirty, v)
            return v

    def views(self) -> dict[str, BookView]:
        with self._lock:
            return {t: self.view(t) for t in sorted(self._active)}

    def selected_views(self, tokens) -> dict[str, BookView]:
        """Read several books under one lock acquisition.

        ``price_change`` events may update both outcome tokens atomically.  A
        caller that invokes ``view(up)`` and ``view(down)`` separately can see
        the old UP side and new DOWN side if a writer lands between calls.
        This method preserves the event boundary for cross-token snapshots.
        """
        selected = tuple(dict.fromkeys(str(t) for t in tokens if t))
        with self._lock:
            return {token: self.view(token) for token in selected}

    def _status(self, token: str) -> str:
        if not self.connected:
            return DISCONNECTED
        m = self._meta.get(token)
        if not m or not m.get("synced"):
            return UNSYNCED
        upd = m.get("updated")
        if upd is None:
            return UNSYNCED
        return LIVE if (time.monotonic() - upd) <= self.stale_after else STALE

    def status(self, tokens=None) -> str:
        with self._lock:
            selected = set(self._active if tokens is None else (str(t) for t in tokens if t))
            if not selected:
                return UNSYNCED if self.connected else DISCONNECTED
            from .health import worst
            return worst(*[self._status(t) for t in selected])

    def needs_resync(self, *, include_stale: bool = False) -> list[str]:
        with self._lock:
            return [t for t in sorted(self._active)
                    if (not self._meta.get(t, {}).get("synced")
                        or (include_stale and self._status(t) == STALE))]

    def _fresh_exchange_ts(self, ts_ms: int | None) -> bool:
        """Sanity-check an event timestamp. Liveness is NOT measured here.

        The arrival of this message is what makes it current; the timestamp
        says when the venue last changed the book, which on a quiet market is
        far older. Bounding it by ``stale_after`` refused the resubscribe
        snapshot of any book that had not traded in the last few seconds, and
        the token then never synced at all. Liveness is measured from receipt
        in ``status()``, against the same ``stale_after``.

        What is still refused: a timestamp we cannot read, one dated ahead of
        our clock (a clock or unit fault), and one so old the venue must be
        serving a frozen book.
        """
        if ts_ms is None:
            return False
        try:
            age_s = timer.exchange_age_s(ts_ms)
        except (ValueError, OverflowError):
            return False
        return -self.future_tolerance <= age_s <= self.max_quiet


def _to_map(levels) -> tuple[dict[float, float], bool]:
    out: dict[float, float] = {}
    if not isinstance(levels, (list, tuple)):
        return {}, False
    for lv in levels or ():
        try:
            if isinstance(lv, dict):
                if isinstance(lv.get("price"), bool) or isinstance(lv.get("size"), bool):
                    return {}, False
                p, s = float(lv.get("price")), float(lv.get("size"))
            else:
                if isinstance(lv[0], bool) or isinstance(lv[1], bool):
                    return {}, False
                p, s = float(lv[0]), float(lv[1])
        except (IndexError, KeyError, TypeError, ValueError, OverflowError):
            return {}, False
        if math.isfinite(p) and math.isfinite(s) and 0 < p < 1 and s > 0:
            out[p] = out.get(p, 0.0) + s
            if not math.isfinite(out[p]):
                return {}, False
        else:
            return {}, False
    return out, True


def _int(v):
    if isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError, OverflowError):
        return None


def _replace_status(self: BookView, status: str) -> BookView:
    return BookView(token=self.token, generation=self.generation, status=status,
                    bids=self.bids, asks=self.asks, updated_mono=self.updated_mono,
                    exchange_ts_ms=self.exchange_ts_ms, tick_size=self.tick_size,
                    hash=self.hash, updates=self.updates,
                    receive_revision=self.receive_revision)


BookView._replace_status = _replace_status
