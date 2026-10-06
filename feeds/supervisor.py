"""Supervised async feed.

Each feed is an independent task. A feed that raises logs, backs off and
reconnects; it never propagates the exception, so one venue going down cannot
take the bot with it.

Backoff is 0.25 -> 0.5 -> 1 -> 2 -> 4 -> 8 (cap), each with +/- jitter so a
fleet of bots does not resynchronise into a thundering herd after a venue
blip. The ladder resets only after a connection has been healthy for
`reset_after` seconds - resetting on connect alone turns a connect/drop loop
into a 0.25s hot loop against the venue.
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Callable

from .health import DISCONNECTED, FeedHealth, safe_log_text

BACKOFF_LADDER = (0.25, 0.5, 1.0, 2.0, 4.0, 8.0)
JITTER = 0.25          # +/-25%


def backoff_delay(attempt: int, ladder=BACKOFF_LADDER, jitter: float = JITTER,
                  rng: random.Random | None = None) -> float:
    if not ladder:
        raise ValueError("backoff ladder cannot be empty")
    base = ladder[min(max(0, int(attempt)), len(ladder) - 1)]
    r = (rng or random).uniform(-jitter, jitter)
    return max(0.0, base * (1.0 + r))


class SupervisedFeed:
    """Base class. Subclasses implement `_session()` - one connection's life."""

    name = "feed"
    reset_after = 30.0          # seconds connected before the ladder resets

    def __init__(self, on_event: Callable[[str, str, str], None] | None = None) -> None:
        self.health = FeedHealth(name=self.name)
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._attempt = 0
        self._on_event = on_event

    # ---------------------------------------------------------------- api
    def start(self) -> asyncio.Task:
        # Lifecycle retries must not create two sockets/subscriptions writing
        # into the same mutable feed state.
        if self._task is not None and not self._task.done():
            return self._task
        self._stop.clear()
        self._task = asyncio.create_task(self._supervise(), name=f"feed:{self.name}")
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                self.health.mark_error(exc)
                self.event(f"stop failed: {type(exc).__name__}: {exc}", "bad")

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def event(self, text: str, level: str = "info") -> None:
        """Queue a line for whoever is displaying feeds. Cheap by contract:
        callers must not do I/O here."""
        if self._on_event:
            try:
                self._on_event(self.name, safe_log_text(text), level)
            except Exception as exc:
                # Telemetry must never crash a feed, but its own failure must
                # remain visible in health instead of disappearing.
                self.health.detail = safe_log_text(
                    f"event callback failed: {type(exc).__name__}: {exc}", limit=200)

    # ------------------------------------------------------------ internals
    async def _supervise(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:                      # never escapes
                self.health.mark_error(exc)
                self.event(f"{type(exc).__name__}: {exc}", "bad")
            finally:
                self.health.status = DISCONNECTED

            if self._stop.is_set():
                break
            if time.monotonic() - started >= self.reset_after:
                self._attempt = 0
            delay = backoff_delay(self._attempt)
            self._attempt += 1
            self.health.mark_reconnect()
            self.event(f"reconnect in {delay:.2f}s (attempt {self._attempt})", "warn")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue

    async def _session(self) -> None:                     # pragma: no cover
        raise NotImplementedError
