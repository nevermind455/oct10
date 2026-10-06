"""REST reconcile — the backup path for fills.

Worth stating plainly: this tree has no existing reconcile process to keep as
a backup. `trade_log.csv` records that an order was *sent*, not that it
filled, and there is no fill price, share count or order id in it. So this is
new, not preserved.

What it does: it polls when the user WebSocket is unhealthy, immediately after
each wire-level subscription/session generation, and periodically even while
the socket looks healthy.  The last two paths close gaps where a brief outage
reconnects before the normal outage poll or where a venue trade becomes visible
only after the first REST response.  Results feed the SAME FillStore. Because
the store is keyed by trade id, a trade the socket already delivered is
recognised and suppressed rather than counted twice.

Only runs on its own task. Never called from a receive callback.
"""
from __future__ import annotations

import asyncio
import math
import time

from .health import DISCONNECTED, STALE, redact
from .poly_user import FillStore


class RestReconciler:
    def __init__(self, store: FillStore, fetch_trades=None, *,
                 user_feed=None, trigger_after: float = 20.0,
                 interval: float = 15.0, fetch_timeout: float = 20.0,
                 healthy_audit_interval: float = 120.0,
                 known_trade=None, on_event=None) -> None:
        try:
            interval = float(interval)
            trigger_after = float(trigger_after)
            fetch_timeout = float(fetch_timeout)
            healthy_audit_interval = float(healthy_audit_interval)
        except (TypeError, ValueError) as exc:
            raise ValueError("reconcile intervals/timeouts must be finite") from exc
        if (not all(math.isfinite(value) for value in (
                    interval, trigger_after, fetch_timeout,
                    healthy_audit_interval))
                or interval <= 0 or trigger_after < 0 or fetch_timeout <= 0
                or healthy_audit_interval <= 0):
            raise ValueError("reconcile intervals/timeouts must be positive")
        self.store = store
        self._fetch = fetch_trades          # callable() -> list[dict]
        self.user = user_feed
        self.trigger_after = trigger_after
        self.interval = interval
        self.fetch_timeout = fetch_timeout
        self.healthy_audit_interval = healthy_audit_interval
        self._known_trade = known_trade
        self._on_event = on_event
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.polls = 0
        self.recovered = 0                  # fills the socket never delivered
        self.suppressed = 0                 # already known, correctly ignored
        self.last_error: str | None = None
        self.last_run_mono: float | None = None
        self._audited_subscription_generation: int | None = None
        self._last_completed_subscription_generation: int | None = None
        self._run_lock = asyncio.Lock()
        self._fetch_task: asyncio.Task | None = None
        self._fetch_subscription_generation: int | None = None

    # -------------------------------------------------------------- state
    @property
    def armed(self) -> bool:
        """True when the socket cannot be relied on right now."""
        if self.user is None:
            return True
        status = self.user.health.status
        if status in (DISCONNECTED, STALE) or status not in ("LIVE",):
            return True
        age = self.user.pong_age()
        return age is None or age > self.trigger_after

    def summary(self) -> dict:
        return {"armed": self.armed, "polls": self.polls,
                "recovered": self.recovered, "suppressed": self.suppressed,
                "last_error": self.last_error,
                "subscription_generation": self._subscription_generation(),
                "audited_subscription_generation":
                    self._audited_subscription_generation,
                "age_s": None if self.last_run_mono is None
                else time.monotonic() - self.last_run_mono}

    def _subscription_generation(self) -> int | None:
        """Read an optional wire-level generation without trusting booleans."""
        raw = getattr(self.user, "subscription_generation", None)
        if isinstance(raw, bool):
            return None
        try:
            generation = int(raw)
        except (TypeError, ValueError):
            return None
        return generation if generation >= 0 else None

    def _healthy_audit_due(self) -> bool:
        return (self.last_run_mono is None
                or time.monotonic() - self.last_run_mono
                >= self.healthy_audit_interval)

    # ---------------------------------------------------------------- run
    def start(self) -> asyncio.Task:
        if self._task is not None and not self._task.done():
            return self._task
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(), name="feed:reconcile")
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
                self.last_error = f"stop failed: {type(exc).__name__}: {exc}"[:200]

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
                return
            except asyncio.TimeoutError:
                generation = self._subscription_generation()
                generation_due = (
                    generation is not None
                    and generation != self._audited_subscription_generation)
                if (self._fetch is not None
                        and (self.armed or generation_due
                             or self._healthy_audit_due())):
                    try:
                        await self.run_once()
                        # A timed-out fetch may have been created for an older
                        # subscription scope. Acknowledge only the generation
                        # that the completed request was actually created for;
                        # any newer wire subscription remains due.
                        completed_generation = (
                            self._last_completed_subscription_generation)
                        if (self.last_error is None and generation is not None
                                and completed_generation is not None
                                and completed_generation >= generation):
                            self._audited_subscription_generation = (
                                completed_generation)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self._error("reconcile loop", exc)

    async def run_once(self) -> int:
        """Poll and merge. Returns how many fills were genuinely new."""
        if self._fetch is None:
            return 0
        async with self._run_lock:
            try:
                if self._fetch_task is None:
                    self._fetch_subscription_generation = (
                        self._subscription_generation())
                    self._fetch_task = asyncio.create_task(
                        asyncio.to_thread(self._fetch), name="feed:reconcile-fetch")
                fetch_task = self._fetch_task
                trades = await asyncio.wait_for(
                    asyncio.shield(fetch_task), timeout=self.fetch_timeout)
                fetch_generation = self._fetch_subscription_generation
                self._fetch_task = None
                self._fetch_subscription_generation = None
            except asyncio.TimeoutError as exc:
                # Keep the still-running task.  A later cycle may consume its
                # result, but no second blocked HTTP request is launched.
                self._error("REST reconcile timed out", exc)
                return 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._fetch_task = None
                self._fetch_subscription_generation = None
                self._error("REST reconcile fetch failed", exc)
                return 0
            self.polls += 1
            new = 0
            response_failed = False
            try:
                rows = trades or ()
                if isinstance(rows, (str, bytes, dict)):
                    raise TypeError("trade endpoint returned a non-list payload")
                for t in rows:
                    if not isinstance(t, dict):
                        self.store.note_invalid()
                        continue
                    trade_id = t.get("id") or t.get("trade_id")
                    if trade_id and self._known_trade is not None:
                        try:
                            already_booked = self._known_trade(t)
                        except Exception as exc:
                            # One contradictory historical row must alarm but
                            # must not starve later genuinely missing fills in
                            # the same REST response.
                            self.store.note_invalid()
                            self._error("durable REST replay conflict", exc)
                            response_failed = True
                            continue
                        if already_booked:
                            # FillStore is intentionally ephemeral, while
                            # Ledger's seen registry survives restart. Do not
                            # relabel an already-booked durable fill as new.
                            self.suppressed += 1
                            continue
                    is_new = self.store.record_trade(
                        trade_id,
                        order_id=t.get("taker_order_id") or t.get("takerOrderId"),
                        asset_id=t.get("asset_id"), market=t.get("market"),
                        side=t.get("side"), price=t.get("price"), size=t.get("size"),
                        # Never invent terminal success.  A REST row without an
                        # explicit lifecycle state remains non-counting until an
                        # authoritative copy supplies one.
                        status=t.get("status") or "", source="reconcile",
                        fee_rate_bps=t.get("fee_rate_bps", t.get("feeRateBps")),
                        maker_orders=t.get("maker_orders"))
                    if is_new:
                        new += 1
                    else:
                        self.suppressed += 1
            except Exception as exc:
                self._error("REST reconcile response failed validation", exc)
                return new
            self.recovered += new
            if new:
                self._event(f"recovered {new} trade record(s) the socket missed", "warn")
            if response_failed:
                return new
            self.last_error = None
            self.last_run_mono = time.monotonic()
            self._last_completed_subscription_generation = fetch_generation
            return new

    def _event(self, text: str, level: str) -> None:
        if self._on_event:
            try:
                self._on_event("reconcile", text, level)
            except Exception as exc:
                self.last_error = f"event callback failed: {type(exc).__name__}: {exc}"[:200]

    def _error(self, context: str, exc: BaseException) -> None:
        self.last_error = redact(f"{context}: {type(exc).__name__}: {exc}")[:200]
        self._event(self.last_error, "warn")
