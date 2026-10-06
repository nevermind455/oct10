"""Settlement task — polls for resolution and settles the ledger.

Runs on its own task. Never called from a websocket receive path, never in
the order path. A market that is not resolved yet is left PENDING; this loop
simply asks again later.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time

from . import resolution as res_mod
from .ledger import Ledger


class SettlementWorker:
    def __init__(self, ledger: Ledger, *, interval: float = 20.0,
                 fetch=None, on_event=None, max_age_s: float | None = None) -> None:
        self.ledger = ledger
        self.interval = float(interval)
        if not math.isfinite(self.interval) or self.interval <= 0:
            raise ValueError("settlement interval must be finite and positive")
        self._fetch = fetch or res_mod.fetch
        self._on_event = on_event
        self.max_age_s = None if max_age_s is None else float(max_age_s)
        if (self.max_age_s is not None
                and (not math.isfinite(self.max_age_s) or self.max_age_s <= 0)):
            raise ValueError("settlement max age must be finite and positive")
        self._stop = asyncio.Event()
        self._task = None
        self._run_lock = asyncio.Lock()
        self.polls = 0
        self.settled = 0
        self.pending_reasons: dict = {}
        self.last_error: str | None = None
        self.last_callback_error: str | None = None

    def start(self):
        if self._task is not None and not self._task.done():
            return self._task
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(), name="settlement")
        return self._task

    async def stop(self):
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.last_error = f"stop failed: {type(exc).__name__}: {exc}"[:200]

    def _emit(self, topic: str, message: str, level: str) -> None:
        if not self._on_event:
            return
        try:
            self._on_event(topic, message, level)
            self.last_callback_error = None
        except Exception as exc:
            self.last_callback_error = (
                f"settlement event callback failed: {type(exc).__name__}: {exc}")[:200]
            logging.getLogger(__name__).exception(self.last_callback_error)

    def open_conditions(self) -> list:
        out, now = [], time.time()
        with self.ledger._lock:
            for p in self.ledger.positions.values():
                if p.settled or p.shares <= 0 or not p.condition_id:
                    continue
                newest = max((l.wall for l in p.lots), default=0)
                if self.max_age_s is not None and now - newest > self.max_age_s:
                    continue          # give up polling ancient rounds
                if p.condition_id not in out:
                    out.append(p.condition_id)
        return out

    async def run_once(self) -> int:
        async with self._run_lock:
            n = 0
            cycle_error: str | None = None
            conditions = self.open_conditions()
            active = set(conditions)
            for stale in set(self.pending_reasons) - active:
                self.pending_reasons.pop(stale, None)
            for cid in conditions:
                if self._stop.is_set():
                    break
                self.polls += 1
                try:
                    r = await asyncio.to_thread(self._fetch, cid)
                    if not hasattr(r, "resolved"):
                        raise TypeError("resolution fetch returned an invalid object")
                except Exception as exc:
                    detail = f"{type(exc).__name__}: {exc}"[:200]
                    self.pending_reasons[cid] = detail
                    cycle_error = detail
                    self._emit("settle", f"fetch failed for {cid[-8:]}: {detail}",
                               "warn")
                    continue
                if not r.resolved:
                    detail = str(getattr(r, "detail", "")
                                 or getattr(r, "status", "UNKNOWN"))[:200]
                    previous = self.pending_reasons.get(cid)
                    self.pending_reasons[cid] = detail
                    if detail != previous:
                        self._emit("settle",
                                   f"awaiting {cid[-8:]}: {detail}", "info")
                    continue
                self.pending_reasons.pop(cid, None)
                try:
                    # On a worker thread: settle() rewrites and fsyncs the whole
                    # ledger file, which on the event loop froze every feed,
                    # order and the dashboard for the length of the write - at
                    # every round boundary, growing with the ledger.
                    settled = await asyncio.to_thread(self.ledger.settle, r)
                except Exception as exc:
                    detail = f"{type(exc).__name__}: {exc}"[:200]
                    self.pending_reasons[cid] = detail
                    cycle_error = detail
                    self._emit("settle", f"settlement failed: {detail}", "bad")
                    continue
                for pos in settled:
                    n += 1
                    self.settled += 1
                    sign = "+" if (pos.realized or 0) >= 0 else ""
                    self._emit("settle",
                               f"{pos.token_id[-6:]} {sign}{pos.realized:.4f} "
                               f"({pos.shares:.2f} sh @ {pos.payout_per_share:.2f})",
                               "good" if (pos.realized or 0) >= 0 else "bad")
            # `last_error` is health state, not an append-only incident log.
            self.last_error = cycle_error
            return n

    async def _loop(self):
        while not self._stop.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"[:200]
                self._emit("settle", self.last_error, "bad")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                return
            except asyncio.TimeoutError:
                pass

    def summary(self) -> dict:
        return {"polls": self.polls, "settled": self.settled,
                "awaiting": len(self.open_conditions()),
                "pending_reasons": dict(list(self.pending_reasons.items())[:5]),
                "last_error": self.last_error,
                "callback_error": self.last_callback_error}

    def health_status(self) -> str:
        """Worker health; unresolved venue outcomes are normal work, not failure."""
        if self.last_error or self.last_callback_error:
            return "ERROR"
        if self._task is not None and self._task.done() and not self._stop.is_set():
            return "ERROR"
        return "OK"
