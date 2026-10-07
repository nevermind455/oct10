"""Settlement - every market's lifecycle from expiry to realized P/L.

    ACTIVE -> EXPIRED -> PENDING SETTLEMENT -> SETTLING -> SETTLED
                                       (LIVE, winning shares held) -> REDEEM PENDING -> REDEEMED

One record per market, keyed by its condition id, so any number of markets
can wait for their results at once and each settles on its own; the market
the sniper trades now is never held up by an older one still pending.

Only an authoritative venue result settles anything (accounting.resolution:
the Conditional Tokens payout vector on Polygon first, the CLOB + Gamma
mirrors as fallback). A clock reaching zero only moves a market to PENDING
SETTLEMENT; a BTC price never decides a winner.

Settled is not redeemed. Settlement books the payout in the ledger the moment
the result is known. LIVE, a winning share is still an outcome token in the
wallet until it is redeemed for collateral, so LIVE tracks REDEEM PENDING and
proves REDEEMED from the wallet's on-chain balance, never from the winner.
PAPER has no redemption step: its cash is credited at settlement.

Off the hot path by construction: this runs as its own task, every network
read happens on this worker's own network pool and every ledger read/write and
journal write on its own accounting pool (never the default executor the order
path uses; bookkeeping never queues behind a slow RPC), and nothing here takes
the ledger lock on the event loop.
"""
from __future__ import annotations

import asyncio
import functools
import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from . import resolution as res_mod
from .ledger import Ledger

ACTIVE = "ACTIVE"
EXPIRED = "EXPIRED"
PENDING = "PENDING SETTLEMENT"
SETTLING = "SETTLING"
SETTLED = "SETTLED"
STATES = (ACTIVE, EXPIRED, PENDING, SETTLING, SETTLED)

REDEEM_PENDING = "REDEEM PENDING"
REDEEMED = "REDEEMED"
REDEEM_PAPER = "N/A (PAPER)"            # paper cash is credited at settlement
REDEEM_NOT_NEEDED = "NOT NEEDED"        # no winning shares held: nothing to redeem

# Pending details that only mean "not resolved yet" - no event, no error.
_PLAIN_WAIT = ("not resolved on chain yet", "still open", "status is missing", "status is none",
               "awaiting the api mirrors")
_API_FAILURE = ("fetch failed", "unavailable", "read failed", "unreachable", "timed out", "timeout")


def _num(value) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0
    return f if math.isfinite(f) else 0.0


@dataclass
class MarketRecord:
    condition_id: str
    window_start: int | None = None
    window_end: int | None = None
    up_token: str | None = None
    down_token: str | None = None
    slug: str | None = None
    source: str = "rotation"                       # "rotation" (discovered) or "ledger" (adopted)
    state: str = ACTIVE
    positions: list = field(default_factory=list)  # ledger rows of this market, refreshed off-loop
    expired_at: float | None = None                # clock (exchange unix) when seen expired
    result_at: float | None = None                 # ... when the authoritative result was received
    settled_at: float | None = None                # ... when the ledger durably recorded it
    processing_ms: float | None = None             # result received -> accounting recorded
    result_pc: float | None = None                 # perf_counter at result receipt
    check_ms: float | None = None                  # how long the last result read took
    resolution: object = None
    winner: str | None = None
    payouts: dict | None = None
    result_source: str = ""
    realized: float | None = None
    detail: str = ""                               # why it is still pending
    error: str | None = None
    checks: int = 0
    inflight: int = 0
    next_check: float = 0.0                        # monotonic
    timed_out: bool = False
    prepared: bool = False
    prepare_at: float = 0.0
    redeem: str | None = None
    redeem_amount: float | None = None
    redeem_shares: dict = field(default_factory=dict)  # winning token -> shares the ledger holds
    redeemed_at: float | None = None
    redeem_error: str | None = None
    redeem_next: float = 0.0
    redeem_inflight: bool = False
    finished_mono: float | None = None
    _last_event: dict = field(default_factory=dict)

    @property
    def market(self) -> str:
        return self.slug or f"cond..{self.condition_id[-8:]}"

    def held(self) -> list:
        """Rows that hold shares (settled or not)."""
        return [r for r in self.positions if _num(r.get("shares")) > 1e-9]

    def unsettled(self) -> list:
        return [r for r in self.positions if _num(r.get("shares")) > 1e-9 and not r.get("settled")]

    def side_of(self, token: str) -> str:
        if token and token == self.up_token:
            return "UP"
        if token and token == self.down_token:
            return "DOWN"
        if self.payouts and self.winner in ("UP", "DOWN") and token in self.payouts:
            won = abs(_num(self.payouts.get(token)) - 1.0) <= 1e-9
            return self.winner if won else ("DOWN" if self.winner == "UP" else "UP")
        return "?"

    def positions_text(self) -> str:
        rows = self.held()
        if not rows:
            return "none"
        return ", ".join(f"{self.side_of(r['token_id'])} {_num(r['shares']):.3f}sh ${_num(r['cost']):.2f}"
                         for r in rows)

    def view(self, now: float) -> dict:
        """Plain values for the dashboard and the report."""
        end = self.window_end
        age = None if end is None or now < end else now - end
        pay = None
        if self.payouts and self.winner in ("UP", "DOWN"):
            pay = 1.0
        elif self.payouts and self.winner == "50/50":
            pay = 0.5
        return {
            "condition_id": self.condition_id, "slug": self.slug, "source": self.source,
            "window_start": self.window_start, "window_end": end, "state": self.state,
            "age_s": age, "winner": self.winner, "settlement_price": pay,
            "realized": self.realized, "result_source": self.result_source,
            "result_s": None if self.result_at is None or end is None else self.result_at - end,
            "settlement_s": None if self.settled_at is None or end is None else self.settled_at - end,
            "processing_ms": self.processing_ms, "check_ms": self.check_ms, "checks": self.checks,
            "redeem": self.redeem, "redeem_amount": self.redeem_amount, "redeem_error": self.redeem_error,
            "redeemed_s": None if self.redeemed_at is None or end is None else self.redeemed_at - end,
            "detail": self.detail, "error": self.error, "timed_out": self.timed_out,
            "has_position": bool(self.held()),
            "positions": [dict(r, side=self.side_of(r["token_id"])) for r in self.positions],
        }


class SettlementWorker:
    """Tracks markets by condition id and settles each from its own result.

    ``interval`` is how often one pending market is asked for its result.
    ``poll_active`` (default True, the original contract) also asks markets
    whose window has not ended - every open ledger position is polled. The bot
    passes False: a market cannot resolve before it ends, so asking earlier
    only spends requests. ``fetch(condition_id) -> Resolution`` defaults to
    a chain-first resolution.Resolver.
    """

    def __init__(self, ledger: Ledger, *, interval: float = 20.0,
                 fetch=None, on_event=None, max_age_s: float | None = None,
                 journal_path=None, paper: bool = True, redeem_check=None,
                 clock=time.time, poll_active: bool = True, prepare=None,
                 tick_s: float = 0.25, timeout_s: float = 900.0, history: int = 8,
                 max_inflight: int = 2, redeem_every_s: float = 30.0,
                 workers: int = 6) -> None:
        self.ledger = ledger
        self.interval = float(interval)
        if not math.isfinite(self.interval) or self.interval <= 0:
            raise ValueError("settlement interval must be finite and positive")
        if fetch is None:
            fetch = res_mod.Resolver()
        self._fetch = fetch
        self._prepare = prepare if prepare is not None else getattr(fetch, "prepare", None)
        self._on_event = on_event
        self.max_age_s = None if max_age_s is None else float(max_age_s)
        if (self.max_age_s is not None
                and (not math.isfinite(self.max_age_s) or self.max_age_s <= 0)):
            raise ValueError("settlement max age must be finite and positive")
        self.journal_path = Path(journal_path) if journal_path else None
        self.paper = bool(paper)
        self._redeem_check = redeem_check
        self._clock = clock
        self.poll_active = bool(poll_active)
        self.tick_s = min(float(tick_s), self.interval)
        self.timeout_s = float(timeout_s)
        self.history = max(1, int(history))
        self.max_inflight = max(1, int(max_inflight))
        self.redeem_every_s = float(redeem_every_s)
        self.adopt_every_s = min(1.0, self.interval)
        self.workers = max(1, int(workers))
        self.records: dict[str, MarketRecord] = {}
        self._pools: dict[str, ThreadPoolExecutor] = {}
        self._stop = asyncio.Event()
        self._task = None
        self._tasks: set = set()
        self._run_lock = asyncio.Lock()
        self._adopting = False
        self._adopted_mono = -1e18
        self._scan_clock: float | None = None      # clock when the newest completed ledger scan began
        self._journal: list[dict] = []
        self._flushing = False
        self.polls = 0
        self.settled = 0
        self.duplicate_results = 0
        self.pending_reasons: dict = {}
        self._loop_error: str | None = None
        self.last_callback_error: str | None = None
        self.journal_error: str | None = None

    # ------------------------------------------------------------ lifecycle
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
                pass
            except Exception as exc:
                self._loop_error = f"stop failed: {type(exc).__name__}: {exc}"[:200]
        pending = [t for t in self._tasks if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if self._journal:
            await self._flush_journal()
        for pool in self._pools.values():
            pool.shutdown(wait=False, cancel_futures=True)
        self._pools.clear()

    @property
    def last_error(self) -> str | None:
        """The newest unresolved failure: a market whose last check or
        accounting failed, or the loop itself. Cleared when that market's
        next check succeeds - health, not an incident log."""
        if self._loop_error:
            return self._loop_error
        for rec in self.records.values():
            if rec.error and rec.state in (ACTIVE, EXPIRED, PENDING, SETTLING):
                return rec.error
        return None

    @last_error.setter
    def last_error(self, value) -> None:
        self._loop_error = value

    def _in_pool(self, fn, *args, net: bool = False):
        """Run `fn` on the worker's network pool (result and balance reads) or
        its accounting pool (ledger scans and writes, the journal)."""
        name = "net" if net else "io"
        pool = self._pools.get(name)
        if pool is None:
            pool = self._pools[name] = ThreadPoolExecutor(
                max_workers=self.workers if net else 2, thread_name_prefix=f"settle-{name}")
        return asyncio.get_running_loop().run_in_executor(pool, functools.partial(fn, *args))

    def _spawn(self, coro, name: str) -> None:
        task = asyncio.create_task(coro, name=f"settle-{name}")
        self._tasks.add(task)
        task.add_done_callback(self._task_done)

    def _task_done(self, task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:            # never swallowed: health shows it and the event says what
            self._loop_error = f"{task.get_name()}: {type(exc).__name__}: {exc}"[:200]
            self._emit("settle", f"SETTLEMENT_ERROR | {self._loop_error}", "bad")

    # ---------------------------------------------------------------- events
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

    def _event(self, name: str, rec: MarketRecord | None, level: str, *, once_per: str | None = None,
               **fields) -> None:
        """One explicit lifecycle event: `NAME | market=... | k=v`, to the
        event ring and the settlement journal. `once_per` drops a repeat of
        the same event with the same key (a failure retried every second)."""
        if rec is not None and once_per is not None:
            if rec._last_event.get(name) == once_per:
                return
            rec._last_event[name] = once_per
        parts = [name] + ([f"market={rec.market}"] if rec is not None else [])
        parts += [f"{k}={v}" for k, v in fields.items() if v is not None]
        self._emit("settle", " | ".join(parts), level)
        row = {"wall": time.time(), "clock": self._clock(), "event": name}
        if rec is not None:
            row.update(condition_id=rec.condition_id, slug=rec.slug, window_end=rec.window_end, state=rec.state)
        row.update({k: v for k, v in fields.items() if v is not None})
        self._journal.append(row)

    async def _flush_journal(self) -> None:
        if self.journal_path is None:
            self._journal.clear()
            return
        rows, self._journal = self._journal, []
        self._flushing = True
        try:
            await self._in_pool(self._write_journal, rows)
            self.journal_error = None
        except Exception as exc:
            self.journal_error = f"settlement journal write failed: {type(exc).__name__}: {exc}"[:200]
            self._emit("settle", f"SETTLEMENT_ERROR | {self.journal_error}", "warn")
        finally:
            self._flushing = False

    def _write_journal(self, rows: list) -> None:
        self.journal_path.parent.mkdir(parents=True, exist_ok=True)
        with self.journal_path.open("a", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")

    # --------------------------------------------------------------- markets
    def track(self, condition_id, window_start=None, window_end=None, *, up_token=None,
              down_token=None, slug=None) -> MarketRecord | None:
        """Register a market the bot discovered. Cheap (no I/O): call it from
        the rotation loop; it starts ACTIVE and is checked only once it ends."""
        cid = str(condition_id or "")
        if not cid:
            return None
        rec = self.records.get(cid)
        if rec is None:
            rec = self.records[cid] = MarketRecord(cid)
        rec.window_start = int(window_start) if window_start is not None else rec.window_start
        rec.window_end = int(window_end) if window_end is not None else rec.window_end
        rec.up_token = str(up_token) if up_token else rec.up_token
        rec.down_token = str(down_token) if down_token else rec.down_token
        rec.slug = slug or rec.slug or (f"btc-updown-5m-{rec.window_start}" if rec.window_start else None)
        rec.source = "rotation"
        return rec

    def open_conditions(self) -> list:
        """Conditions holding unsettled shares (reads the ledger under its lock:
        call it from a worker thread, not the event loop)."""
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

    def _ledger_scan(self, wanted: frozenset) -> tuple[dict, dict]:
        """Worker thread: this worker's view of the ledger - every position of
        a tracked market, and every unsettled position - plus the window end
        a live authorization recorded for each of those markets."""
        rows: dict[str, list] = {}
        with self.ledger._lock:
            for p in self.ledger.positions.values():
                cid = p.condition_id
                if not cid or not (cid in wanted or (not p.settled and p.shares > 0)):
                    continue
                rows.setdefault(cid, []).append(_position_row(p))
            ends = {}
            for meta in self.ledger.authorized_orders.values():
                if isinstance(meta, dict) and meta.get("condition_id") in rows and meta.get("window_end"):
                    ends[meta["condition_id"]] = int(meta["window_end"])
        return rows, ends

    def _rows_for(self, cid: str) -> list:
        with self.ledger._lock:
            return [_position_row(p) for p in self.ledger.positions.values() if p.condition_id == cid]

    async def _refresh_from_ledger(self) -> None:
        started = self._clock()
        try:
            rows, ends = await self._in_pool(self._ledger_scan, frozenset(self.records))
        finally:
            self._adopting = False
            self._adopted_mono = time.monotonic()
        self._scan_clock = started
        wall = time.time()
        for cid, these in rows.items():
            rec = self.records.get(cid)
            if rec is None:
                open_rows = [r for r in these if not r["settled"] and r["shares"] > 1e-9]
                if not open_rows:
                    continue
                newest = max((r["newest_wall"] or 0.0) for r in open_rows)
                if self.max_age_s is not None and wall - newest > self.max_age_s:
                    continue
                end = ends.get(cid) or (int(newest // 300) * 300 + 300 if newest else None)
                rec = self.records[cid] = MarketRecord(
                    cid, window_start=None if end is None else end - 300, window_end=end, source="ledger")
            rec.positions = these
            if rec.state == EXPIRED and rec.finished_mono is not None and rec.unsettled():
                # Given up as position-less, yet it holds shares: never abandon them.
                rec.state, rec.finished_mono, rec.detail, rec.next_check = PENDING, None, "", 0.0
                self._event("SETTLEMENT_PENDING", rec, "warn", positions=rec.positions_text(),
                            detail="shares found in a market given up as position-less; checking again")
            if rec.state == SETTLED and rec.unsettled():
                # A fill confirmed after the market settled (the ledger reopens
                # the position). Settle it with the same authoritative result.
                rec.state = PENDING
                rec.finished_mono = None
                rec.next_check = 0.0
                self._event("LATE_FILL", rec, "warn", positions=rec.positions_text(),
                            detail="a fill confirmed after settlement; re-settling with the confirmed result")

    # ------------------------------------------------------------------ loop
    async def _loop(self):
        while not self._stop.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._loop_error = f"{type(exc).__name__}: {exc}"[:200]
                self._emit("settle", f"SETTLEMENT_ERROR | {self._loop_error}", "bad")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.tick_s)
                return
            except asyncio.TimeoutError:
                pass

    async def tick(self) -> None:
        """One pass on the event loop: cheap state changes only. Every check,
        settlement and journal write it starts runs as its own task on the
        worker's pool, so one slow RPC never delays another market."""
        now, mono = self._clock(), time.monotonic()
        # A market that just ended is reported with the positions it ended
        # with: read the ledger once more first (a few ms, on the pool).
        ending = any(r.state == ACTIVE and r.window_end is not None and now >= r.window_end
                     and not self._scanned_after(r.window_end) for r in self.records.values())
        if not self._adopting and (ending or mono - self._adopted_mono >= self.adopt_every_s):
            self._adopting = True
            self._spawn(self._refresh_from_ledger(), "ledger")
        for rec in list(self.records.values()):
            self._advance(rec, now, mono)
            if self._check_due(rec, mono):
                rec.inflight += 1
                rec.next_check = mono + self._poll_every(rec)
                self._spawn(self._check(rec, reserved=True), "check")
            if (rec.state == ACTIVE and self._prepare is not None and not rec.prepared
                    and mono >= rec.prepare_at):
                rec.prepare_at = mono + 30.0
                self._spawn(self._prepare_market(rec), "prepare")
            if (rec.redeem == REDEEM_PENDING and self._redeem_check is not None
                    and not rec.redeem_inflight and mono >= rec.redeem_next):
                rec.redeem_inflight = True
                rec.redeem_next = mono + self.redeem_every_s
                self._spawn(self._check_redeem(rec), "redeem")
        self._trim(mono)
        if self._journal and not self._flushing:
            self._spawn(self._flush_journal(), "journal")

    def _scanned_after(self, when: float) -> bool:
        return self._scan_clock is not None and self._scan_clock >= when

    def _advance(self, rec: MarketRecord, now: float, mono: float) -> None:
        if (rec.state == ACTIVE and rec.window_end is not None and now >= rec.window_end
                and (self._scanned_after(rec.window_end) or now - rec.window_end > 5.0)):
            rec.state = EXPIRED
            rec.expired_at = now
            self._event("MARKET_EXPIRED", rec, "info", positions=rec.positions_text(),
                        source=None if rec.source == "rotation" else "adopted from the ledger")
            rec.state = PENDING
            rec.next_check = 0.0
            self._event("SETTLEMENT_PENDING", rec, "warn" if rec.held() else "info",
                        age=f"{max(0.0, now - rec.window_end) * 1000.0:.0f}ms")
        if (rec.state == PENDING and not rec.timed_out and rec.window_end is not None
                and now - rec.window_end > self.timeout_s):
            rec.timed_out = True
            if rec.held():
                self._event("SETTLEMENT_TIMEOUT", rec, "bad", age=f"{now - rec.window_end:.0f}s",
                            detail=rec.error or rec.detail or "no result yet",
                            action="still checking, every 5s")
            else:
                rec.state = EXPIRED                        # nothing held: stop asking
                rec.detail = f"no result after {self.timeout_s:.0f}s; no position, not checked further"
                rec.finished_mono = mono
                self._event("SETTLEMENT_TIMEOUT", rec, "warn", age=f"{now - rec.window_end:.0f}s",
                            detail="no position held; not checked further")

    def _check_due(self, rec: MarketRecord, mono: float) -> bool:
        if rec.inflight >= self.max_inflight or mono < rec.next_check:
            return False
        if rec.state == PENDING:
            return True
        return rec.state == ACTIVE and self.poll_active and bool(rec.unsettled())

    def _poll_every(self, rec: MarketRecord) -> float:
        return max(self.interval, 5.0) if rec.timed_out else self.interval

    def _trim(self, mono: float) -> None:
        """Keep every market still owed something; of the finished ones, the newest `history`."""
        finished = [r for r in self.records.values() if r.finished_mono is not None
                    and r.state in (SETTLED, EXPIRED) and r.redeem != REDEEM_PENDING]
        if len(finished) <= self.history:
            return
        finished.sort(key=lambda r: (r.window_end or 0, r.finished_mono or 0))
        for rec in finished[:len(finished) - self.history]:
            self.records.pop(rec.condition_id, None)

    async def _prepare_market(self, rec: MarketRecord) -> None:
        """Read the market's outcome-slot order while it still trades, so the
        first check after expiry is a single chain read."""
        try:
            rec.prepared = bool(await self._in_pool(self._prepare, rec.condition_id, net=True))
        except Exception as exc:  # noqa: BLE001 - retried in 30s; a check reads it itself if needed
            rec.prepared = False
            rec.detail = f"outcome order not read yet: {type(exc).__name__}"

    # ---------------------------------------------------------------- checks
    async def _check(self, rec: MarketRecord, *, reserved: bool = False) -> int:
        """Ask for this market's result once; settle it if it is final."""
        if not reserved:
            rec.inflight += 1
        try:
            if rec.resolution is not None:
                # Result already received: an earlier accounting attempt failed,
                # or a late fill reopened the market. Settle with that result.
                return await self._settle(rec)
            self.polls += 1
            rec.checks += 1
            t0 = time.perf_counter()
            try:
                r = await self._in_pool(self._fetch, rec.condition_id, net=True)
                if not hasattr(r, "resolved"):
                    raise TypeError("resolution fetch returned an invalid object")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                rec.check_ms = (time.perf_counter() - t0) * 1000.0
                self._pending(rec, f"{type(exc).__name__}: {exc}"[:200], "api")
                return 0
            rec.check_ms = (time.perf_counter() - t0) * 1000.0
            if not r.resolved:
                detail = str(getattr(r, "detail", "") or getattr(r, "status", "UNKNOWN"))[:240]
                low = detail.lower()
                if any(m in low for m in _API_FAILURE):
                    kind = "api"
                elif getattr(r, "status", "") == res_mod.UNKNOWN:
                    kind = "bad"
                elif any(m in low for m in _PLAIN_WAIT):
                    kind = "wait"
                else:
                    kind = "not_final"
                self._pending(rec, detail, kind)
                return 0
            return await self._apply_result(rec, r)
        finally:
            rec.inflight -= 1

    def _pending(self, rec: MarketRecord, detail: str, kind: str) -> None:
        rec.detail = detail
        self.pending_reasons[rec.condition_id] = detail
        if kind == "wait":
            rec.error = None
            return
        if kind == "api":
            rec.error = detail
            self._event("RESULT_API_ERROR", rec, "warn", once_per=detail, error=detail,
                        action=f"retry in {self._poll_every(rec):g}s")
            return
        # A result surface answered with something that is not a final payout
        # (proposed / disputed / malformed / disagreeing): never settled on.
        rec.error = detail if kind == "bad" else None
        self._event("RESULT_NOT_FINAL", rec, "bad" if kind == "bad" else "warn", once_per=detail,
                    detail=detail)

    async def _apply_result(self, rec: MarketRecord, r) -> int:
        """An authoritative final result arrived. A second copy (two checks in
        flight, a repeated notification) is counted and ignored."""
        if rec.resolution is not None or rec.state in (SETTLING, SETTLED):
            self.duplicate_results += 1
            return 0
        rec.resolution = r
        rec.payouts = dict(getattr(r, "payouts", {}) or {})
        label = str(getattr(r, "winning_label", "") or "").strip()
        rec.winner = label.upper() if label.lower() in ("up", "down") else (label or None)
        rec.result_source = str(getattr(r, "detail", "") or "")[:120]
        rec.result_at = self._clock()
        rec.result_pc = time.perf_counter()
        rec.detail, rec.error = "", None
        delay = None if rec.window_end is None else f"{(rec.result_at - rec.window_end) * 1000.0:.0f}ms"
        self._event("RESULT_RECEIVED", rec, "info", winner=rec.winner or "?", delay=delay,
                    source=rec.result_source or None)
        return await self._settle(rec)

    async def _settle(self, rec: MarketRecord) -> int:
        """Book the result in the ledger (on the pool: settle() rewrites and
        fsyncs the ledger file). Idempotent: the ledger settles a position
        only once, and a market only reaches SETTLED from here."""
        if rec.state == SETTLING:
            return 0
        if rec.state == SETTLED and not rec.unsettled():
            self.duplicate_results += 1
            return 0
        rec.state = SETTLING
        try:
            done, rows = await self._in_pool(self._settle_sync, rec.resolution, rec.condition_id)
        except asyncio.CancelledError:
            rec.state = PENDING
            raise
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"[:200]
            rec.state, rec.error = PENDING, detail
            self.pending_reasons[rec.condition_id] = detail
            self._event("ACCOUNTING_ERROR", rec, "bad", once_per=detail, error=detail,
                        action=f"retry in {self._poll_every(rec):g}s with the same result")
            return 0
        rec.positions = rows
        rec.settled_at = self._clock()
        rec.processing_ms = (None if rec.result_pc is None
                             else (time.perf_counter() - rec.result_pc) * 1000.0)
        rec.realized = sum(_num(r["realized"]) for r in rows if r["settled"] and r["realized"] is not None)
        rec.state, rec.error, rec.detail = SETTLED, None, ""
        rec.finished_mono = time.monotonic()
        self.pending_reasons.pop(rec.condition_id, None)
        self.settled += len(done)
        delay = None if rec.window_end is None else f"{(rec.settled_at - rec.window_end) * 1000.0:.0f}ms"
        self._event("ACCOUNTING_SETTLED", rec, "good" if rec.realized >= 0 else "bad",
                    winner=rec.winner or "?", pnl=f"{rec.realized:+.2f}", delay=delay,
                    processing=None if rec.processing_ms is None else f"{rec.processing_ms:.1f}ms",
                    positions=rec.positions_text(), settled_now=len(done))
        self._begin_redemption(rec)
        return len(done)

    def _settle_sync(self, resolution, cid: str) -> tuple[list, list]:
        done = self.ledger.settle(resolution)
        return done, self._rows_for(cid)

    # ------------------------------------------------------------ redemption
    def _begin_redemption(self, rec: MarketRecord) -> None:
        winners = {r["token_id"]: _num(r["shares"]) for r in rec.positions
                   if r["settled"] and _num(r["shares"]) > 1e-9 and _num(r["payout"]) > 0}
        amount = sum(_num(r["shares"]) * _num(r["payout"]) for r in rec.positions
                     if r["settled"] and r["token_id"] in winners)
        if self.paper:
            rec.redeem = REDEEM_PAPER
            return
        if not winners:
            rec.redeem = REDEEM_NOT_NEEDED
            return
        rec.redeem, rec.redeem_amount, rec.redeem_shares = REDEEM_PENDING, amount, winners
        rec.redeem_next = time.monotonic()
        self._event("REDEEM_PENDING", rec, "warn", amount=f"${amount:.2f}",
                    detail=("winning shares are still outcome tokens in the wallet: not spendable "
                            "collateral until redeemed (claim on polymarket.com)")
                    + ("" if self._redeem_check is not None else "; not tracked (no wallet address)"))

    async def _check_redeem(self, rec: MarketRecord) -> None:
        try:
            remaining = {}
            for token, held in rec.redeem_shares.items():
                balance, detail = await self._in_pool(self._redeem_check, token, net=True)
                if balance is None:
                    rec.redeem_error = f"wallet balance unknown: {detail}"
                    self._event("REDEEM_FAILED", rec, "warn", once_per=detail,
                                detail=f"redemption status could not be read ({detail}); retry in "
                                       f"{self.redeem_every_s:g}s")
                    return
                if balance >= held - 1e-6:
                    remaining[token] = held
            rec.redeem_error = None
            if remaining:
                return
            rec.redeem, rec.redeemed_at = REDEEMED, self._clock()
            self._event("REDEEMED", rec, "good", amount=f"${_num(rec.redeem_amount):.2f}",
                        detail="the winning shares have left the wallet (redeemed for collateral)")
        finally:
            rec.redeem_inflight = False

    # ------------------------------------------------------------- one pass
    async def run_once(self) -> int:
        """Check every market that holds unsettled shares now, together, and
        settle what has resolved. Returns the number of positions settled."""
        async with self._run_lock:
            self._adopting = True
            await self._refresh_from_ledger()
            conditions = await self._in_pool(self.open_conditions)
            for stale in set(self.pending_reasons) - set(conditions):
                self.pending_reasons.pop(stale, None)
            recs = [self.records[c] for c in conditions if c in self.records]
            if self._stop.is_set() or not recs:
                return 0
            counts = await asyncio.gather(*(self._check(rec) for rec in recs))
            if self._journal and not self._flushing:
                await self._flush_journal()
            return int(sum(counts))

    # --------------------------------------------------------------- reading
    def panel(self, now: float | None = None, *, active_condition: str | None = None,
              settled_hold_s: float = 60.0) -> dict:
        """What the dashboard's SETTLEMENT / MARKETS panels show (plain values).

        `subject` is the market the SETTLEMENT block is about: the oldest one
        still waiting for its result, else one that settled in the last
        `settled_hold_s`, else the market trading now (ACTIVE)."""
        now = self._clock() if now is None else now
        views = sorted((r.view(now) for r in self.records.values()),
                       key=lambda v: -(v["window_end"] or 0))
        active = next((v for v in views if v["condition_id"] == active_condition), None)
        if active is None:
            active = next((v for v in views if v["state"] == ACTIVE and v["source"] == "rotation"), None)
        waiting = [v for v in views if v["state"] in (EXPIRED, PENDING, SETTLING) and v is not active
                   and not (v["state"] == EXPIRED and v["detail"])]
        previous = [v for v in views if v is not active and v["state"] != ACTIVE]
        settled = [v for v in views if v["state"] == SETTLED]
        last = max(settled, key=lambda v: v["window_end"] or 0, default=None)
        if waiting:
            subject = min(waiting, key=lambda v: v["window_end"] or 0)
        elif last is not None and last["settlement_s"] is not None and last["window_end"] is not None \
                and now - (last["window_end"] + last["settlement_s"]) <= settled_hold_s:
            subject = last
        else:
            subject = active
        return {"active": active, "subject": subject, "pending": waiting, "previous": previous,
                "last_settled": last, "pending_count": len(waiting),
                "redeem_pending": sum(_num(v["redeem_amount"]) for v in views if v["redeem"] == REDEEM_PENDING)}

    def latency(self) -> dict:
        def stats(values):
            values = sorted(v for v in values if v is not None)
            if not values:
                return None
            return {"n": len(values), "p50": round(values[len(values) // 2], 3), "max": round(values[-1], 3)}
        recs = list(self.records.values())
        return {"result_after_expiry_s": stats(r.result_at - r.window_end for r in recs
                                               if r.result_at is not None and r.window_end is not None),
                "settled_after_expiry_s": stats(r.settled_at - r.window_end for r in recs
                                                if r.settled_at is not None and r.window_end is not None),
                "processing_ms": stats(r.processing_ms for r in recs),
                "check_ms": stats(r.check_ms for r in recs)}

    def summary(self) -> dict:
        now = self._clock()
        try:
            awaiting = len(self.open_conditions())
        except Exception as exc:  # noqa: BLE001 - a report must not fail on this
            awaiting = f"unknown: {type(exc).__name__}"
        return {"polls": self.polls, "settled": self.settled,
                "awaiting": awaiting,
                "pending_reasons": dict(list(self.pending_reasons.items())[:5]),
                "duplicate_results": self.duplicate_results,
                "last_error": self.last_error,
                "callback_error": self.last_callback_error,
                "journal": None if self.journal_path is None else str(self.journal_path),
                "journal_error": self.journal_error,
                "latency": self.latency(),
                "markets": [{k: v for k, v in r.view(now).items() if k != "positions"}
                            for r in sorted(self.records.values(), key=lambda r: -(r.window_end or 0))]}

    def health_status(self) -> str:
        """Worker health; unresolved venue outcomes are normal work, not failure."""
        if self.last_error or self.last_callback_error:
            return "ERROR"
        if self._task is not None and self._task.done() and not self._stop.is_set():
            return "ERROR"
        return "OK"


def _position_row(p) -> dict:
    return {"token_id": p.token_id, "condition_id": p.condition_id, "shares": float(p.shares),
            "cost": float(p.cost), "fees": float(p.fees), "avg": p.avg_price, "settled": bool(p.settled),
            "realized": p.realized, "payout": p.payout_per_share,
            "newest_wall": max((lot.wall for lot in p.lots), default=None)}
