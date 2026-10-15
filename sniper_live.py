"""Lean LIVE sender for the sniper - CANARY ONLY. Nothing here runs unless a
live canary is explicitly started, and nothing here changes a trading
decision: the Sniper decides exactly as in paper; this module only executes.

It holds no credentials of its own. It takes the audited, already
authenticated ClobClient (polymarket_trade._get_client()) and reuses that
client's own OrderBuilder (signer, signature type, funder) and API creds, so
an order is signed and authenticated exactly as the audited live path does -
without that path's per-order network calls.

Hot path, signal -> network send:
    exact price (sniper_mode.signing_price) -> sign (the client's builder) ->
    order JSON + L2 HMAC headers stamped with a CACHED server time ->
    durable local intent/reservation -> recheck dispatch guards ->
    POST /order on an already-open HTTP/2 connection.
No GET /time, no market metadata, no balance read, no book fetch, no retry.

Off the hot path:
    ServerClock   GET /time every few seconds, offset by the round-trip
                  midpoint. An order is refused - never sent with a guessed
                  time - once the offset is older than max_age_s.
    keep_warm     runs those refreshes on the SAME connection the orders use
                  (make_http), so a quiet spell between orders never costs a
                  new TCP+TLS handshake, and a dead connection is found by a
                  /time request instead of by an order.
    RoundRules    tick, neg_risk, minimum size and the UP/DOWN mapping per
                  condition, fetched at round start through the audited
                  validator (polymarket_trade._validate_market_mapping).
    Canary        hard limits; a kill switch that stops NEW orders on any
                  unexplained state and never stops reconciliation.

Every send is timed from the transport itself (httpcore trace events):
network_send_start is when the request headers start going to the socket,
not when the order was signed or queued, and a new TCP/TLS connection on the
hot path is detected and reported.
"""
from __future__ import annotations

import asyncio
import collections
import copy
import json
import math
import os
import re
import statistics
import time
import types
import dataclasses
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

from sniper_mode import FastOrderBuilder, fast_signer, signing_amount, signing_price  # noqa: F401 - fast_signer: build the live signer with it

# Result kinds of one send. Only MATCHED and PENDING mean the venue took the
# order; AMBIGUOUS means nobody can know without reconciling.
MATCHED, PENDING, NO_MATCH, REJECTED, NOT_SENT, AMBIGUOUS = (
    "MATCHED", "PENDING", "NO_MATCH", "REJECTED", "NOT_SENT", "AMBIGUOUS")


class Halted(Exception):
    """New orders are stopped (kill switch). Reconciliation continues."""


class Refused(Exception):
    """This one order must not be sent; trading may continue."""


def short_id(oid) -> str:
    """An order id as halt and event text shows it: 0x + 64 hex becomes
    0xa1fa8479...72449d. The dashboard hides any raw 32-byte hex (a private key
    looks the same), which left a halted order unidentifiable; the prefix is
    what reconcile_order.py takes. Anything else is returned unchanged."""
    s = str(oid or "")
    return f"{s[:10]}...{s[-6:]}" if re.fullmatch(r"0x[0-9a-fA-F]{64}", s) else s


def reply_totals(resp) -> tuple[float, float] | None:
    """(USDC spent, shares received) from an accepted BUY reply's makingAmount /
    takingAmount - the venue's totals for the whole execution - or None. The
    CLOB has sent them as decimals ("3.74") and, in older replies, as 6-decimal
    base units ("3740000"); a whole number >= 1000 is read as base units (no
    canary order spends $1000 or buys 1000 shares)."""
    if not isinstance(resp, dict):
        return None
    out = []
    for key in ("makingAmount", "takingAmount"):
        raw = resp.get(key)
        if raw is None or isinstance(raw, bool):
            return None
        try:
            v = Decimal(str(raw).strip())
        except (ArithmeticError, ValueError):
            return None
        if not v.is_finite() or v <= 0:
            return None
        if "." not in str(raw) and v >= 1000:
            v = v / Decimal(1_000_000)
        out.append(float(v))
    return out[0], out[1]


class KillSwitch:
    """Pause new orders for five minutes; reconciliation keeps running.

    Repeated reports do not extend an active pause. Persistent conditions use
    trip_once so they cannot continually pause trading. STOP_LIVE is held only
    while the operator's file exists. pause_s=None retains a manual latch.
    """

    def __init__(self, stop_file: str | None = None, *, pause_s: float | None = 300.0,
                 mono=time.monotonic) -> None:
        import threading
        self.stop_file = Path(stop_file) if stop_file else None
        self.reasons: list[str] = []
        self.since: dict = {}                   # reason -> when it tripped (monotonic)
        self._deadlines: dict[str, float | None] = {}
        self.pause_s, self.mono = pause_s, mono
        self.resumed: list = []                 # (reason, seconds paused): the history
        self.on_resume = None                   # callback(reason, seconds) when a pause ends
        self._lock = threading.RLock()
        self._once: set[str] = set()

    def _add_locked(self, reason: str) -> None:
        if reason not in self.reasons:
            self.reasons.append(reason)
            self.since[reason] = self.mono()
            self._deadlines[reason] = None if self.pause_s is None else time.time() + self.pause_s

    def trip(self, reason: str) -> None:
        # A fresh incident must not be mistaken for an expired pause whose
        # removal has not yet been observed by a check or status read.
        self._expire()
        with self._lock:
            self._add_locked(reason)

    def trip_once(self, key, reason: str, *, aliases=()) -> None:
        """Pause once per incident, accepting its older durable identities."""
        self._expire()
        with self._lock:
            identity = repr(key)
            seen = identity in self._once or any(repr(alias) in self._once for alias in aliases)
            self._once.add(identity)
            if not seen:
                self._add_locked(reason)

    def clear(self, reason: str) -> None:
        with self._lock:
            if reason in self.reasons:
                self.reasons.remove(reason)
            self.since.pop(reason, None)
            self._deadlines.pop(reason, None)

    def _expire(self) -> None:
        now, done = self.mono(), []
        with self._lock:
            stop_reason = f"stop file present: {self.stop_file}"
            stopped = self.stop_file is not None and self.stop_file.exists()
            if stopped:
                self._add_locked(stop_reason)
            else:
                self.clear(stop_reason)
            for reason in list(self.reasons):
                started = self.since.setdefault(reason, now)   # set from outside: its pause starts now
                if reason == stop_reason or self.pause_s is None:
                    continue
                if now - started >= self.pause_s:
                    self.reasons.remove(reason)
                    self.since.pop(reason, None)
                    self._deadlines.pop(reason, None)
                    done.append((reason, now - started))
            self.resumed.extend(done)
        for reason, seconds in done:
            callback = self.on_resume
            if callback is not None:
                try:
                    callback(reason, seconds)
                except Exception:  # noqa: BLE001 - announcing must never block a resume
                    pass

    def check(self) -> None:
        reasons = self.active()
        if reasons:
            raise Halted("; ".join(reasons))

    def active(self) -> list:
        """The reasons pausing new orders now (expired ones dropped first)."""
        self._expire()
        with self._lock:
            return list(self.reasons)

    @property
    def tripped(self) -> bool:
        return bool(self.active())

    def snapshot(self) -> dict:
        self._expire()
        with self._lock:
            return {"pauses": [{"reason": r, "deadline_wall": self._deadlines.setdefault(
                                r, None if self.pause_s is None else
                                time.time() + max(0.0, self.pause_s - (self.mono() - self.since[r])))}
                               for r in self.reasons if not r.startswith("stop file present:")],
                    "once": sorted(self._once)}

    def restore(self, payload: dict) -> None:
        if (not isinstance(payload, dict) or not isinstance(payload.get("pauses"), list)
                or not isinstance(payload.get("once"), list)
                or any(not isinstance(k, str) for k in payload["once"])):
            raise ValueError("malformed live pause state")
        for item in payload["pauses"]:
            if (not isinstance(item, dict) or not isinstance(item.get("reason"), str) or not item["reason"]
                    or "deadline_wall" not in item):
                raise ValueError("malformed live pause record")
            deadline = item["deadline_wall"]
            if deadline is not None and (isinstance(deadline, bool) or not isinstance(deadline, (float, int))
                                         or not math.isfinite(deadline)):
                raise ValueError("invalid live pause deadline")
        with self._lock:
            now, wall = self.mono(), time.time()
            self._once.update(payload.get("once", []))
            for item in payload.get("pauses", []):
                reason, deadline = item["reason"], item["deadline_wall"]
                if deadline is not None and deadline <= wall:
                    continue
                self.trip(reason)
                self._deadlines[reason] = deadline
                if deadline is not None and self.pause_s is not None:
                    self.since[reason] = now - self.pause_s + min(self.pause_s, deadline - wall)


class VenueGuard:
    """Slow replies pause new signals for five minutes and never hard-stop.

    Health probes remain diagnostic. A continuing bad probe cannot extend a
    pause; a fresh slow order reply after recovery can start another pause.
    """
    HEALTHY, DEGRADED, PAUSED, RECOVERED, HARD_HALTED = "HEALTHY", "DEGRADED", "PAUSED", "RECOVERED", "HARD_HALTED"
    _LEVEL = {"HEALTHY": "good", "RECOVERED": "good", "DEGRADED": "warn", "PAUSED": "warn", "HARD_HALTED": "bad"}

    def __init__(self, kill: KillSwitch, *, slow_ms: float = 1500.0, extreme_ms: float = 5000.0,
                 strikes_to_long_pause: int = 3, strike_window_s: float = 300.0, healthy_checks: int = 3,
                 max_pause_s: float = 300.0, long_pause_s: float = 300.0, journal=None, on_event=None,
                 mono=time.monotonic) -> None:
        self.kill = kill
        self.slow_ms, self.extreme_ms = slow_ms, extreme_ms
        self.strikes_to_long_pause, self.strike_window_s = strikes_to_long_pause, strike_window_s
        self.long_pause_s = long_pause_s
        self.healthy_checks, self.max_pause_s = healthy_checks, max_pause_s
        self.journal = journal or (lambda row: None)
        self.on_event = on_event or (lambda *a, **k: None)
        self.mono = mono
        self.state = self.HEALTHY
        self.strikes: collections.deque = collections.deque()
        self.paused_at: float | None = None
        self.retry_not_before = 0.0
        self.healthy_streak = 0
        self.reason = ""
        self.history: list = []

    def _to(self, new: str, detail: str) -> None:
        old, self.state = self.state, new
        row = {"wall": time.time(), "result": "guard", "from": old, "to": new, "detail": str(detail)[:200],
               "strikes": len(self.strikes)}
        self.history.append((old, new))
        try:
            self.journal(row)
        except Exception:  # noqa: BLE001 - journalling must never break the guard
            pass
        try:
            self.on_event("sniper", f"LIVE latency guard {old} -> {new}: {detail}"[:220], self._LEVEL[new])
        except Exception:  # noqa: BLE001 - reporting must not interrupt order accounting or recovery
            pass

    def on_reply(self, rtt_ms: float) -> str:
        """One definite order reply's send -> response time; returns the state."""
        self._resume_if_due()
        if rtt_ms > self.slow_ms:
            now = self.mono()
            self.strikes.append(now)
            while self.strikes and now - self.strikes[0] > self.strike_window_s:
                self.strikes.popleft()
            detail = f"reply {rtt_ms:.0f}ms > {self.slow_ms:.0f}ms (strike {len(self.strikes)})"
            if self.state != self.PAUSED:
                self._to(self.DEGRADED, detail)
                self.pause_for(self.long_pause_s, detail)
        return self.state

    def _resume_if_due(self) -> None:
        if self.state == self.PAUSED and self.mono() >= self.retry_not_before:
            self._to(self.RECOVERED, f"pause completed; {self.healthy_streak} healthy probes")
            self.paused_at, self.healthy_streak, self.reason = None, 0, ""
            self.retry_not_before = 0.0
            self._to(self.HEALTHY, "new orders resume")

    def check(self) -> None:
        """The order path: refused (journalled by the caller) while PAUSED."""
        self._resume_if_due()
        if self.state == self.PAUSED:
            started = self.paused_at if self.paused_at is not None else self.mono()
            raise Refused(f"latency guard PAUSED for {self.mono() - started:.0f}s "
                          f"({self.reason}); {self.healthy_streak}/{self.healthy_checks} healthy checks")

    def pause_for(self, seconds: float, reason: str) -> None:
        """A definitive venue rejection: wait once, then allow new signals.
        This never resends the rejected order; only a new signal can trade."""
        self._resume_if_due()
        if self.state == self.PAUSED:
            return
        now = self.mono()
        self.retry_not_before = now + seconds
        self.paused_at, self.healthy_streak, self.reason = now, 0, reason
        self._to(self.PAUSED, f"{reason}; retry no earlier than {seconds:g}s")

    def on_probe(self, ok: bool, detail: str = "") -> str:
        """One recovery health check while PAUSED; returns the state."""
        if self.state != self.PAUSED:
            return self.state
        self.healthy_streak = self.healthy_streak + 1 if ok else 0
        self._resume_if_due()
        return self.state

    def hard_halt(self, reason: str) -> None:
        # Compatibility for callers of the former hard-stop API.
        self.pause_for(self.long_pause_s, reason)

    def status(self) -> dict:
        self._resume_if_due()
        return {"state": self.state, "strikes": len(self.strikes), "healthy_streak": self.healthy_streak,
                "paused_s": None if self.paused_at is None else round(self.mono() - self.paused_at, 1),
                "reason": self.reason}


class ServerClock:
    """Polymarket server time as local wall time + a measured offset.

    The venue's /time is whole seconds, so one sample pins the offset to
    +-0.5s plus half the round trip; the L2 HMAC timestamp is whole seconds
    too. The median of recent samples is used; a sample with a slow round
    trip is discarded; the offset is refused once older than max_age_s.
    """

    def __init__(self, max_age_s: float = 60.0, max_rtt_ms: float = 500.0, keep: int = 50,
                 mono=time.monotonic, wall=time.time) -> None:
        self.max_age_s, self.max_rtt_ms = max_age_s, max_rtt_ms
        self.mono, self.wall = mono, wall
        self.samples: collections.deque = collections.deque(maxlen=keep)   # (mono, offset_s, rtt_ms)
        self.offset: float | None = None
        self.at: float | None = None
        self._server_at: float | None = None
        self.rejected = 0

    def add_sample(self, server_s: float, sent_wall: float, recv_wall: float, rtt_ms: float) -> bool:
        values = (server_s, sent_wall, recv_wall, rtt_ms)
        try:
            valid = (not any(isinstance(v, bool) for v in values)
                     and all(math.isfinite(float(v)) for v in values)
                     and float(server_s) > 0 and 0 <= float(rtt_ms) <= self.max_rtt_ms
                     and float(recv_wall) >= float(sent_wall)
                     and abs((float(recv_wall) - float(sent_wall)) * 1000 - float(rtt_ms)) <= 1000)
        except (TypeError, ValueError, OverflowError):
            valid = False
        if not valid:
            self.rejected += 1
            return False
        # A whole-second reply means the server was somewhere in [s, s+1):
        # take the middle, or the offset runs half a second slow.
        mid = float(server_s) + (0.5 if float(server_s).is_integer() else 0.0)
        self.samples.append((self.mono(), mid - (sent_wall + recv_wall) / 2.0, rtt_ms))
        self.offset = statistics.median(s[1] for s in list(self.samples)[-5:])
        self.at = self.mono()
        # Advance authentication time on the monotonic clock. A local NTP or
        # manual wall-clock step must not invalidate every order's HMAC.
        self._server_at = float(recv_wall) + self.offset
        return True

    def now_s(self) -> int:
        """The timestamp to sign with, or Halted if the clock cannot be trusted."""
        if self.offset is None or self.at is None:
            raise Halted("server clock never synced")
        age = self.mono() - self.at
        if age < 0 or age > self.max_age_s:
            raise Halted(f"server clock stale ({age:.0f}s > {self.max_age_s:.0f}s)")
        return int(self._server_at + age)

    async def refresh(self, http, url: str) -> bool:
        sent = self.wall()
        t = time.perf_counter()
        r = await http.get(url)
        rtt = (time.perf_counter() - t) * 1000.0
        recv = self.wall()
        if r.status_code != 200:
            self.rejected += 1
            return False
        try:
            data = r.json()
        except ValueError:
            data = r.text
        server = data.get("time") or data.get("timestamp") if isinstance(data, dict) else data
        return self.add_sample(float(server), sent, recv, rtt)

    def status(self) -> dict:
        s = list(self.samples)
        drift = None
        if len(s) >= 2 and s[-1][0] - s[0][0] > 60:
            drift = (s[-1][1] - s[0][1]) / ((s[-1][0] - s[0][0]) / 3600.0)     # s per hour
        return {"offset_s": self.offset, "age_s": None if self.at is None else self.mono() - self.at,
                "samples": len(s), "rejected": self.rejected,
                "rtt_ms_p50": statistics.median(x[2] for x in s) if s else None,
                "drift_s_per_hour": drift}


class RoundRules:
    """Per-condition order rules, fetched off the hot path."""

    def __init__(self, fetch, max_age_s: float = 600.0, mono=time.monotonic) -> None:
        self.fetch, self.max_age_s, self.mono = fetch, max_age_s, mono
        self._rules: dict = {}
        self.fetches = 0
        self._generation: dict = {}
        self._refresh_generation: dict = {}
        self.on_invalidate = None

    async def prefetch(self, condition: str, up: str, down: str) -> dict:
        key = str(condition)
        generation = self._generation.get(key, 0)
        refresh = self._refresh_generation.get(key, 0) + 1
        self._refresh_generation[key] = refresh
        rules = await asyncio.to_thread(self.fetch, condition, up, down)
        self.fetches += 1
        if generation != self._generation.get(key, 0):
            raise Refused("rules invalidated during refresh; waiting for a new fetch")
        if refresh != self._refresh_generation.get(key):
            raise Refused("a newer market-rules refresh superseded this fetch")
        self._rules[key] = (dict(rules, up_token=str(up), down_token=str(down)), self.mono())
        return rules

    def get(self, condition: str, up: str, down: str) -> dict:
        hit = self._rules.get(str(condition))
        if hit is None:
            raise Refused("market rules not prefetched for this round")
        rules, at = hit
        if self.mono() - at > self.max_age_s:
            raise Refused("market rules expired; waiting for the refresh")
        if rules["up_token"] != str(up) or rules["down_token"] != str(down):
            raise Halted("token mapping changed against the prefetched market rules")
        return rules

    def invalidate(self, condition: str) -> None:
        key = str(condition)
        self._generation[key] = self._generation.get(key, 0) + 1
        self._rules.pop(key, None)
        if self.on_invalidate is not None:
            self.on_invalidate(key)


def make_http(timeout_s: float = 3.0, origin: str | None = None):
    """The sender's own HTTP/2 client.

    With an origin (the live canary) it is fast_h2.FastH2: the order's bytes
    are written in the tick handler itself (LeanSender.start), with no
    event-loop turn - httpx yields 4-5 times before its headers reach the
    socket. SNIPER_HTTP=httpx goes back to httpx. Without an origin (tests,
    probes against a local fake venue) it is httpx, as before.

    httpx closes any connection idle for more than 5s by default
    (Limits.keepalive_expiry=5.0) - sniper orders are minutes apart, so on the
    defaults nearly every order would pay a new TCP+TLS handshake (72ms vs a
    36ms warm round trip, measured from the VPS). Here idle connections are
    kept; keep_warm keeps them alive and proves them.
    """
    if origin and str(os.environ.get("SNIPER_HTTP", "fast")).strip().lower() != "httpx":
        from fast_h2 import FastH2
        return FastH2(origin, timeout_s=timeout_s)
    import httpx
    return httpx.AsyncClient(http2=True, timeout=timeout_s,
                             limits=httpx.Limits(max_connections=4, max_keepalive_connections=4,
                                                 keepalive_expiry=None))


async def keep_warm(http, clock: ServerClock, url: str, every_s: float = 10.0, on_event=None) -> None:
    """Refresh the server clock on the order connection every every_s, until
    cancelled. on_event(dict) gets each round: whether a new connection had to
    be opened (the old one died while idle) and any error. A failed refresh
    just ages the clock; ServerClock refuses orders once it is too old."""
    from feeds.supervisor import backoff_delay
    attempt = 0
    while True:
        events = []

        async def trace(name, info, ev=events):
            ev.append(name)
        row = {"wall": time.time()}
        try:
            sent = clock.wall()
            t = time.perf_counter()
            r = await http.get(url, extensions={"trace": trace})
            rtt = (time.perf_counter() - t) * 1000.0
            row["ok"] = clock.add_sample(float(r.text.strip().strip('"')), sent, clock.wall(), rtt)
            row["rtt_ms"] = round(rtt, 2)
        except Exception as exc:  # noqa: BLE001 - keep trying; the clock ages meanwhile
            row["error"] = f"{type(exc).__name__}: {exc}"[:160]
        row["new_connection"] = any("connect_tcp" in n or "start_tls" in n for n in events)
        if on_event:
            on_event(row)
        if row.get("ok"):
            attempt = 0
            delay = every_s
        else:
            # A failed idle probe must not leave the order connection down for
            # the whole keep-warm interval. Only public probes are retried.
            delay = backoff_delay(attempt)
            attempt += 1
        await asyncio.sleep(delay)


@dataclass
class CanaryLimits:
    """Cumulative budgets gain one original increment after each pause.

    The per-order share bound remains an order sizing rule.
    """
    max_order_shares: float = 10.0
    max_orders: int = 40
    max_fills: int = 20
    max_rounds: int = 6
    max_exposure_usd: float = 30.0
    max_daily_loss_usd: float = 15.0
    orders: int = 0
    fills: int = 0
    pending_fills: int = 0                  # every dispatched order may fill
    exposure_usd: float = 0.0
    rounds: set = field(default_factory=set)

    def __post_init__(self):
        import threading
        self._lock = threading.RLock()
        # a cap reached pauses 5 minutes (the kill switch), then allows one more
        # step of the same size - the operator's choice (2026-10-15)
        self._step = {"max_orders": self.max_orders, "max_fills": self.max_fills, "max_rounds": self.max_rounds,
                      "max_exposure_usd": self.max_exposure_usd, "max_daily_loss_usd": self.max_daily_loss_usd}
        self._pending_caps: dict[str, str] = {}

    def _advance(self, kill: KillSwitch) -> None:
        kill.check()
        for name, reason in list(self._pending_caps.items()):
            if reason not in kill.reasons:
                setattr(self, name, getattr(self, name) + self._step[name])
                self._pending_caps.pop(name)

    def _pause_cap(self, name: str, reason: str, kill: KillSwitch) -> None:
        if name not in self._pending_caps:
            self._pending_caps[name] = reason
            kill.trip(reason)

    def check_loss_budget(self, risk: float, kill: KillSwitch) -> None:
        with self._lock:
            self._advance(kill)
            if not math.isfinite(risk) or risk < 0:
                raise Refused("live worst-case loss cannot be calculated")
            if risk > self.max_daily_loss_usd + 1e-9:
                self._pause_cap("max_daily_loss_usd",
                                f"worst-case loss ${risk:.6f} incl. fees exceeds the canary loss budget "
                                f"${self.max_daily_loss_usd:.2f}", kill)
            kill.check()

    def budget_state(self) -> dict:
        with self._lock:
            return {"step": dict(self._step),
                    "ceilings": {name: getattr(self, name) for name in self._step},
                    "pending": dict(self._pending_caps)}

    def restore_budget(self, payload: dict) -> None:
        if not isinstance(payload, dict):
            raise ValueError("malformed canary budget state")
        steps, ceilings, pending = (payload.get(k) for k in ("step", "ceilings", "pending"))
        names = set(self._step)
        if (not isinstance(steps, dict) or not isinstance(ceilings, dict) or not isinstance(pending, dict)
                or set(steps) != names or set(ceilings) != names or not set(pending) <= names):
            raise ValueError("malformed canary budget fields")
        for name in names:
            values = (steps[name], ceilings[name])
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0
                   for v in values):
                raise ValueError("invalid canary budget value")
            if name in ("max_orders", "max_fills", "max_rounds") and any(int(v) != v for v in values):
                raise ValueError("invalid canary count budget")
            if ceilings[name] < steps[name]:
                raise ValueError("canary budget smaller than its increment")
        if any(not isinstance(r, str) or not r for r in pending.values()):
            raise ValueError("invalid pending canary cap")
        with self._lock:
            self._step = dict(steps)
            self._pending_caps = dict(pending)
            for name, value in ceilings.items():
                setattr(self, name, value)

    def check(self, amount: float, shares: float, condition: str, kill: KillSwitch, realized_loss_usd=None):
        with self._lock:
            self._advance(kill)
            orders, potential_fills = self.orders, self.fills + self.pending_fills
            rounds, exposure = set(self.rounds), self.exposure_usd
            if shares > self.max_order_shares + 1e-9:
                raise Refused(f"order of {shares:.2f} shares exceeds the canary cap {self.max_order_shares}")
            if orders >= self.max_orders:
                self._pause_cap("max_orders", f"canary order cap reached ({self.max_orders})", kill)
            if potential_fills >= self.max_fills:
                self._pause_cap("max_fills", f"canary fill cap reached ({self.max_fills})", kill)
            if condition not in rounds and len(rounds) >= self.max_rounds:
                self._pause_cap("max_rounds", f"canary round cap reached ({self.max_rounds})", kill)
            if exposure + amount > self.max_exposure_usd + 1e-9:
                self._pause_cap("max_exposure_usd", f"canary exposure cap ${self.max_exposure_usd:.2f} would be exceeded", kill)
            if realized_loss_usd is not None and realized_loss_usd >= self.max_daily_loss_usd:
                self._pause_cap("max_daily_loss_usd",
                                f"canary daily loss ${realized_loss_usd:.2f} >= ${self.max_daily_loss_usd:.2f}", kill)
            kill.check()

    def record_send(self, amount: float, condition: str) -> None:
        with self._lock:
            self.orders += 1
            self.pending_fills += 1
            self.rounds.add(condition)
            self.exposure_usd += amount       # counted at send: an unknown order is exposure

    def record_outcome(self, kind: str, amount: float) -> None:
        with self._lock:
            if kind in (MATCHED, PENDING):
                self.pending_fills = max(0, self.pending_fills - 1)
                self.fills += 1
            elif kind in (NO_MATCH, REJECTED, NOT_SENT):
                self.pending_fills = max(0, self.pending_fills - 1)
                if kind == NOT_SENT:
                    self.orders = max(0, self.orders - 1)
                self.exposure_usd = max(0.0, self.exposure_usd - amount)   # definitely not spent
        # AMBIGUOUS keeps its exposure until reconciled

    def resolve_accepted(self, unused: float, *, zero_fill: bool = False) -> None:
        """Serialize worker reconciliation with event-loop send counters."""
        with self._lock:
            self.exposure_usd = max(0.0, self.exposure_usd - unused)
            if zero_fill:
                self.fills = max(0, self.fills - 1)


class CanarySession:
    """Durable send intents and session budgets (write-ahead, append-only).

    A signed order's intent and reservation are on disk BEFORE its first
    possible socket write: one appended, fsynced journal line - never a
    rewrite of the whole state, so the cost per order does not grow with the
    session's history. A crash with an unresolved intent pauses the next run;
    its reservation remains counted after the pause, and neither a restart
    nor a new budget silently retries that order.
    Definitive no-fill replies and complete ledger confirmations resolve
    intents; halts are recorded too.

    On disk (``path`` is the state file the operator knows):
      path                      snapshot: counters, rounds, halts and the
                                intents still open when it was taken
                                (journal_seq says which records it includes)
      path.journal.<k>          append-only segments, one JSON line per intent
                                / outcome / resolution / counter change, each
                                with a rising seq
      path.archive.jsonl        finished intents moved out of the hot state at
                                compaction - the audit trail, read only at start
    open() = snapshot + every newer record from every segment, in seq order. A
    torn final line (an append that never returned, so its order was never
    written) is dropped; damage anywhere else fails closed. Compaction (save(),
    on a worker) archives finished intents, starts a new segment, writes a new
    snapshot and deletes the segments it includes; the order path never waits
    for it beyond the instant the segment is switched.

    Only the pre-send intent is written in the caller's step. Outcomes and
    resolutions go to a writer thread (ordered by seq); a crash before one of
    them reached the disk leaves its intent unresolved - the next start pauses
    and reconciles, as a crash before the reply always did.
    """
    VERSION = 3
    UNRESOLVED = {"DISPATCHING", "ACCEPTED", "AMBIGUOUS"}
    FINAL = {NO_MATCH, REJECTED, NOT_SENT, "CONFIRMED"}
    RECENT_S = 86_400.0
    # Older snapshots store only reasons. Ignore their derived restart reason
    # and derive it again; newer kill_state retains its deadline and once key
    # so restarting cannot restart a pause that has already ended.
    PREVIOUS_RUN = "unresolved live orders from a previous run"
    TRANSIENT = ("shutting down", PREVIOUS_RUN)   # halts that are not persisted (see above)

    def unresolved(self) -> list:
        """[(client order id, intent copy)] still awaiting their outcome."""
        with self._lock:
            return [(k, dict(i)) for k, i in self.intents.items() if i.get("status") in self.UNRESOLVED]
    COMPACT_RECORDS = 256                   # journal records between compactions
    COMPACT_FINISHED = 128                  # finished intents held before they are archived

    def __init__(self, path, wallet_key: str) -> None:
        import threading
        self.path = Path(path)
        self.wallet_key = str(wallet_key)
        self.session_id: str | None = None
        self.started_wall: float | None = None
        self.last_error: str | None = None
        self.restored: dict = {}
        self.intents: dict = {}             # open intents + finished ones not archived yet
        self._save_lock = threading.RLock()     # one compaction at a time
        self._lock = threading.RLock()          # in-memory state and seq
        self._journal_lock = threading.Lock()   # the active segment's fd
        self._seq = 0                       # last record seq assigned
        self._snap_seq = 0                  # last record the snapshot on disk includes
        self._segment = 0                   # active segment number
        self._since = 0                     # records since the last compaction
        self._marker = None                 # counters + halts as last recorded
        self._writer = None
        self.gaps = 0                       # records missing on replay (a crash lost an async record)
        self.migrated_from: int | None = None

    # ------------------------------------------------------------ files
    @staticmethod
    def wallet_key_for(address: str) -> str:
        """A stable, non-reversible account id for the file (never the address)."""
        import hashlib
        return hashlib.sha256(str(address or "").strip().lower().encode()).hexdigest()[:16]

    def _segment_path(self, k: int) -> Path:
        return self.path.with_name(f"{self.path.name}.journal.{int(k)}")

    @property
    def archive_path(self) -> Path:
        return self.path.with_name(f"{self.path.name}.archive.jsonl")

    def _segments(self) -> list[int]:
        prefix = f"{self.path.name}.journal."
        out = []
        for p in self.path.parent.glob(f"{self.path.name}.journal.*"):
            tail = p.name[len(prefix):]
            if tail.isdigit():
                out.append(int(tail))
        return sorted(out)

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        if os.name == "nt":
            return                          # Windows: the file flush is what exists
        fd = os.open(str(directory), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _sync(fd: int) -> None:
        (getattr(os, "fdatasync", None) or os.fsync)(fd)

    def _create_segment(self, k: int) -> int:
        """A segment whose directory entry is durable, open for appends (the
        caller closes it). No file is held open between appends: Windows
        cannot rename or delete an open file, and a restart, a new session or
        compaction must be able to."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self._segment_path(k)), os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0),
                     0o600)
        try:
            self._fsync_dir(self.path.parent)
        except BaseException:
            os.close(fd)
            raise
        return fd

    # ------------------------------------------------------------ reading
    @classmethod
    def _validate_intent(cls, key, intent) -> None:
        if (not isinstance(key, str) or not key or not isinstance(intent, dict)
                or intent.get("status") not in cls.UNRESOLVED | cls.FINAL
                or not isinstance(intent.get("condition"), str) or not intent["condition"]
                or not isinstance(intent.get("token"), str) or not intent["token"]):
            raise ValueError("malformed canary send intent")
        for field_name in ("amount", "reserve", "wall", "end"):
            value = intent.get(field_name)
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value < 0):
                raise ValueError("malformed canary intent reservation")
        if intent["reserve"] < intent["amount"]:
            raise ValueError("canary intent reserve below notional")
        spent = intent.get("spent")
        if spent is not None and (isinstance(spent, bool) or not isinstance(spent, (int, float))
                                  or not math.isfinite(spent) or not 0 <= spent <= intent["amount"] + 1e-6):
            raise ValueError("invalid resolved canary spend")
        shares = intent.get("shares")
        if shares is not None and (isinstance(shares, bool) or not isinstance(shares, (int, float))
                                   or not math.isfinite(shares) or shares <= 0):
            raise ValueError("invalid canary intent shares")
        tids = intent.get("trade_ids")
        if tids is not None and (not isinstance(tids, list)
                                 or any(not isinstance(t, str) or not t for t in tids)):
            raise ValueError("invalid canary intent trade ids")
        totals = intent.get("reply_totals")
        if totals is not None and (not isinstance(totals, list) or len(totals) != 2
                                   or any(isinstance(v, bool) or not isinstance(v, (int, float))
                                          or not math.isfinite(v) or v <= 0 for v in totals)):
            raise ValueError("invalid canary intent reply totals")

    @staticmethod
    def _validate_counters(c) -> dict:
        if not isinstance(c, dict):
            raise ValueError("malformed canary counters")
        for k in ("orders", "fills", "pending_fills", "exposure_usd"):
            v = c.get(k)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
                raise ValueError("malformed canary counters")
            if k != "exposure_usd" and v != int(v):
                raise ValueError("malformed canary counters")
        return c

    def _read(self) -> dict | None:
        """The snapshot on disk (version 1, 2 or 3), validated; None if there is none."""
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text(encoding="utf-8"), parse_constant=lambda c: float("nan"))
        ok = (isinstance(data, dict) and data.get("version") in (1, 2, self.VERSION)
              and isinstance(data.get("session_id"), str) and data["session_id"]
              and isinstance(data.get("wallet"), str)
              and isinstance(data.get("rounds"), list) and isinstance(data.get("kill_reasons"), list))
        if ok:
            for k in ("started_wall", "orders", "fills", "exposure_usd"):
                v = data.get(k)
                if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0:
                    ok = False
                elif k in ("orders", "fills") and v != int(v):
                    ok = False
        if not ok:
            raise ValueError("malformed canary session state")
        for field_name in ("kill_state", "limit_state"):
            if field_name in data and not isinstance(data[field_name], dict):
                raise ValueError(f"malformed canary {field_name}")
        pending = data.get("pending_fills", 0)
        intents = data.get("intents", {})
        if data["version"] >= 2 and ("pending_fills" not in data or "intents" not in data):
            raise ValueError("missing durable canary reservations")
        if (isinstance(pending, bool) or not isinstance(pending, int) or pending < 0
                or not isinstance(intents, dict)):
            raise ValueError("malformed canary send reservations")
        for key, intent in intents.items():
            self._validate_intent(key, intent)
        if data["version"] == self.VERSION:
            for k in ("journal_seq", "segment"):
                v = data.get(k)
                if isinstance(v, bool) or not isinstance(v, int) or v < 0:
                    raise ValueError("malformed canary journal position")
        return data

    def _records(self, after_seq: int) -> list[dict]:
        """Every journal record newer than after_seq, from every segment, in
        seq order. A torn final line of the last segment is dropped; any other
        damaged line fails closed."""
        segments = self._segments()
        out = []
        for i, k in enumerate(segments):
            raw = self._segment_path(k).read_bytes()
            lines = raw.split(b"\n")
            torn = lines.pop()
            if torn.strip() and i != len(segments) - 1:
                raise ValueError(f"canary journal segment {k} ends with an incomplete record")
            # (a torn final line of the last segment: an append that never
            # completed, so it was never acknowledged and its order never left)
            for line in lines:
                if not line.strip():
                    raise ValueError(f"blank line inside canary journal segment {k}")
                rec = json.loads(line.decode("utf-8"), parse_constant=lambda c: float("nan"))
                if (not isinstance(rec, dict) or isinstance(rec.get("seq"), bool)
                        or not isinstance(rec.get("seq"), int) or rec["seq"] <= 0
                        or rec.get("op") not in ("intent", "outcome", "resolve", "state")):
                    raise ValueError(f"malformed canary journal record in segment {k}")
                if rec["seq"] > after_seq:
                    out.append(rec)
        out.sort(key=lambda r: r["seq"])
        return out

    def _apply(self, data: dict, rec: dict) -> None:
        """One journal record onto a snapshot-shaped dict (open() and durable_view())."""
        op, intents = rec["op"], data.setdefault("intents", {})
        if "c" in rec:
            c = self._validate_counters(rec["c"])
            data.update(orders=int(c["orders"]), fills=int(c["fills"]), pending_fills=int(c["pending_fills"]),
                        exposure_usd=float(c["exposure_usd"]))
        if op == "intent":
            self._validate_intent(rec.get("key"), rec.get("intent"))
            intents[rec["key"]] = dict(rec["intent"])
            if isinstance(rec.get("round"), str) and rec["round"] not in data["rounds"]:
                data["rounds"].append(rec["round"])
        elif op in ("outcome", "resolve"):
            intent = intents.get(rec.get("key"))
            if intent is not None:          # (else archived by a later compaction: nothing to update)
                for k in ("status", "kind", "order_id", "spent", "trade_ids", "reply_totals"):
                    if k in rec:
                        intent[k] = rec[k]
                self._validate_intent(rec["key"], intent)
        elif op == "state":
            reasons = rec.get("kill_reasons")
            if not isinstance(reasons, list):
                raise ValueError("malformed canary halt record")
            data["kill_reasons"] = [str(r) for r in reasons]
            for field_name in ("kill_state", "limit_state"):
                if field_name in rec:
                    if not isinstance(rec[field_name], dict):
                        raise ValueError(f"malformed canary {field_name}")
                    data[field_name] = copy.deepcopy(rec[field_name])
        data["journal_seq"] = rec["seq"]

    def _load_durable(self) -> dict | None:
        """Snapshot + journal as one state dict (the version it was read as is kept)."""
        data = self._read()
        if data is None:
            if self._segments():
                raise ValueError("canary journal exists without its snapshot")
            return None
        data.setdefault("journal_seq", 0)
        data.setdefault("segment", 0)
        if data["version"] == self.VERSION:
            expected = None
            for rec in self._records(int(data["journal_seq"])):
                if expected is not None and rec["seq"] != expected:
                    self.gaps += rec["seq"] - expected
                self._apply(data, rec)
                expected = rec["seq"] + 1
        return data

    @classmethod
    def durable_view(cls, path) -> dict | None:
        """What a restart would read - snapshot + journal - without opening a
        session (tools, tests). Archived intents are not included."""
        return cls(path, "")._load_durable()

    def _archived(self, since_wall: float) -> dict:
        """Finished intents from the archive file (for the ledger proof at start)."""
        out = {}
        p = self.archive_path
        if not p.exists():
            return out
        lines = p.read_bytes().split(b"\n")
        lines.pop()                         # a torn last line: an archive append that never completed
        for line in lines:
            if not line.strip():
                continue
            try:
                rec = json.loads(line.decode("utf-8"))
            except ValueError:
                raise ValueError("damaged canary archive line") from None
            if not isinstance(rec, dict) or not isinstance(rec.get("intent"), dict):
                raise ValueError("damaged canary archive line")
            if float(rec["intent"].get("wall") or 0) >= since_wall - self.RECENT_S:
                out[str(rec.get("key"))] = rec["intent"]
        return out

    # ------------------------------------------------------------ open
    @staticmethod
    def _ledger_view(ledger, since_wall: float, intents: dict | None = None) -> dict:
        """What the live ledger's durable authorizations prove since since_wall."""
        orders, spend, rounds, filled = 0, 0.0, set(), set()
        by_order = {str(intent.get("order_id") or intent.get("order_hash")): intent
                    for intent in (intents or {}).values() if intent.get("order_id") or intent.get("order_hash")}
        with ledger._lock:
            ids = set()
            for oid, meta in ledger.authorized_orders.items():
                if not isinstance(meta, dict) or float(meta.get("window_end") or 0) < since_wall:
                    continue
                orders += 1
                ids.add(oid)
                if meta.get("condition_id"):
                    rounds.add(str(meta["condition_id"]))
            for pos in ledger.positions.values():
                for lot in pos.lots:
                    if lot.order_id in ids:
                        filled.add(lot.order_id)
            no_fill = {oid for oid in ids if by_order.get(oid, {}).get("status") in (NO_MATCH, REJECTED, NOT_SENT)} - filled
            for oid in ids - no_fill:
                meta, intent = ledger.authorized_orders[oid], by_order.get(oid, {})
                if intent.get("status") == "CONFIRMED" and intent.get("spent") is not None:
                    spend += float(intent["spent"])
                else:
                    spend += float(meta.get("requested_notional") or 0.0)
            ids -= no_fill
            complete = {oid for oid in ids
                        if by_order.get(str(oid), {}).get("status") == "CONFIRMED"
                        or (oid in filled and ledger.authorized_orders[oid].get("trade_ids")
                            and set(ledger.authorized_orders[oid]["trade_ids"]) <= set(ledger.seen))}
        return {"orders": orders, "spend": spend, "rounds": rounds, "fills": len(filled),
                "confirmed": complete, "unconfirmed": ids - complete}

    def _recover_authorizations(self, ledger, unconfirmed: set, known: dict) -> None:
        """Old state formats may have an authorization without a send intent.

        Rebuild a durable, unresolved reservation rather than allowing that
        accepted order to disappear from the loss budget after the pause. An
        old authorization without fee data reserves the full possible fee
        rate, so missing historical metadata cannot reduce its risk.
        """
        represented = {str(i.get("order_hash") or i.get("order_id")) for i in known.values()
                       if i.get("order_hash") or i.get("order_id")}
        with ledger._lock:
            missing = [(str(oid), dict(ledger.authorized_orders[oid]))
                       for oid in unconfirmed if str(oid) not in represented]
        for oid, meta in missing:
            amount = float(meta.get("requested_notional") or 0.0)
            principal = max(amount, float(meta.get("venue_min_shares") or 0.0)
                            * float(meta.get("price_cap") or 0.0))
            rate = float(meta.get("fee_rate") if meta.get("fee_rate") is not None else 1.0)
            reserve = max(principal * (1.0 + rate), amount + float(meta.get("estimated_fee") or 0.0),
                          float(meta.get("reserved_total_cost") or 0.0))
            key = f"recovered-authorization:{oid}"
            intent = {"status": "DISPATCHING", "wall": self.started_wall, "end": float(meta["window_end"]),
                      "condition": str(meta["condition_id"]), "token": str(meta["token_id"]), "side": "BUY",
                      "amount": amount, "reserve": reserve, "order_hash": oid, "order_id": oid,
                      "recovered_authorization": True}
            self._validate_intent(key, intent)
            self.intents[key] = intent

    def open(self, limits: "CanaryLimits", kill: KillSwitch, ledger, *, new_session: bool = False,
             now: float | None = None) -> str:
        """Restore this wallet's session (or start one). Raises RuntimeError:
        the caller must not trade then. Returns a one-line description."""
        import shutil
        import uuid
        now = time.time() if now is None else float(now)
        try:
            data = self._load_durable()
        except Exception as exc:  # noqa: BLE001 - any unreadable state fails closed
            raise RuntimeError(f"canary session state {self.path} is unreadable ({type(exc).__name__}): "
                               f"restore it, or start a NEW budget deliberately with --new-canary-session") from None
        if data is not None and data["wallet"] != self.wallet_key:
            raise RuntimeError(f"canary session state {self.path} belongs to another wallet: refusing to start")
        if data is not None:
            # A single booked lot cannot prove an accepted FAK's complete
            # quantity. Only an explicitly persisted terminal resolution can
            # clear its intent; an interrupted reconciliation stays halted.
            if new_session and any(i["status"] in self.UNRESOLVED for i in data.get("intents", {}).values()):
                raise RuntimeError("unresolved live send intent: reconcile the venue before starting a new canary budget")
        if new_session and data is not None:
            # The old session, consolidated (snapshot + journal), stays beside the new one.
            stamp = int(now)
            self.path.with_name(f"{self.path.name}.{stamp}.bak").write_text(
                json.dumps({**data, "version": max(int(data["version"]), 2)}, sort_keys=True), encoding="utf-8")
            self.path.unlink()
            for k in self._segments():
                self._segment_path(k).replace(self.path.with_name(f"{self.path.name}.journal.{k}.{stamp}.bak"))
            data = None
        if data is not None and data["version"] < self.VERSION:
            # Format upgrade: the file as it was stays beside the new one.
            self.migrated_from = int(data["version"])
            shutil.copy2(self.path, self.path.with_name(f"{self.path.name}.v{data['version']}.bak"))
        if data is None:
            recent = self._ledger_view(ledger, now - self.RECENT_S)
            if recent["orders"] and not new_session:
                raise RuntimeError(f"canary session state {self.path} is missing but the live ledger shows "
                                   f"{recent['orders']} live order(s) in the last day: refusing to start a fresh "
                                   f"budget silently - pass --new-canary-session to start one deliberately")
            data = {"version": self.VERSION, "session_id": uuid.uuid4().hex, "wallet": self.wallet_key,
                    "started_wall": now, "orders": 0, "fills": 0, "exposure_usd": 0.0, "rounds": [],
                    "kill_reasons": [], "pending_fills": 0, "intents": {}, "journal_seq": 0, "segment": 0}
            how = "new canary session"
        else:
            how = "canary session restored"
        self.session_id, self.started_wall = data["session_id"], float(data["started_wall"])
        self.intents = copy.deepcopy(data.get("intents", {}))
        self._seq = self._snap_seq = int(data.get("journal_seq", 0))
        self._segment = max([int(data.get("segment", 0))] + self._segments())
        try:
            archived = self._archived(self.started_wall)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"canary archive {self.archive_path} is unreadable ({type(exc).__name__})") from None
        proof = self._ledger_view(ledger, self.started_wall, {**archived, **self.intents})
        self._recover_authorizations(ledger, proof["unconfirmed"], {**archived, **self.intents})
        limits.orders = max(int(data["orders"]), proof["orders"])
        limits.fills = max(int(data["fills"]), proof["fills"])
        accepted_ids = {i.get("order_id") for i in self.intents.values() if i.get("kind") in (MATCHED, PENDING)}
        limits.pending_fills = max(int(data.get("pending_fills", 0)),
                                   len(proof["unconfirmed"] - accepted_ids),
                                   sum(i["status"] in self.UNRESOLVED and i.get("kind") not in (MATCHED, PENDING)
                                       for i in self.intents.values()))
        limits.exposure_usd = max(float(data["exposure_usd"]), proof["spend"])
        limits.rounds = set(map(str, data["rounds"])) | proof["rounds"]
        try:
            if "limit_state" in data:
                limits.restore_budget(data["limit_state"])
            if "kill_state" in data:
                kill.restore(data["kill_state"])
            else:
                for reason in data["kill_reasons"]:
                    if not str(reason).startswith(self.TRANSIENT):
                        kill.trip(str(reason))
        except (TypeError, ValueError, KeyError):
            raise RuntimeError(f"canary session state {self.path} contains malformed pause or budget state") from None
        unresolved = [key for key, intent in self.intents.items() if intent["status"] in self.UNRESOLVED]
        if unresolved or proof["unconfirmed"]:
            reason = (f"{self.PREVIOUS_RUN} ({len(unresolved)} send intents, "
                      f"{len(proof['unconfirmed'])} unconfirmed authorizations): reconciling in background")
            for key in unresolved:
                kill.trip_once(("order-uncertain", str(key)), reason,
                               aliases=((self.PREVIOUS_RUN, key),))
        self.restored = {"orders": limits.orders, "fills": limits.fills, "spend": round(limits.exposure_usd, 4),
                         "rounds": len(limits.rounds), "halts": list(kill.reasons)}
        self._marker = None
        if not self.save(limits, kill):
            raise RuntimeError(f"canary session state {self.path} cannot be written ({self.last_error}): "
                               f"refusing to trade without a durable budget")
        self._start_writer(kill)
        return (f"{how} {self.session_id[:8]} (since {time.strftime('%m-%d %H:%M', time.localtime(self.started_wall))}): "
                f"{limits.orders} orders, {limits.fills} fills, ${limits.exposure_usd:.2f} spent, "
                f"{len(limits.rounds)} rounds" + (f"; HALTED: {'; '.join(kill.reasons)}" if kill.reasons else "")
                + (f"; state file upgraded from format {self.migrated_from}" if self.migrated_from else ""))

    # ------------------------------------------------------------ state
    @staticmethod
    def _counters(limits: "CanaryLimits") -> dict:
        with limits._lock:
            return {"orders": int(limits.orders), "fills": int(limits.fills),
                    "pending_fills": int(limits.pending_fills), "exposure_usd": float(limits.exposure_usd)}

    @staticmethod
    def _controls(limits: "CanaryLimits", kill: KillSwitch) -> dict:
        """Budget steps and pause deadlines survive a restart unchanged."""
        kill_state = kill.snapshot()
        kill_state["pauses"] = [p for p in kill_state["pauses"]
                                 if not str(p["reason"]).startswith("shutting down")]
        return {"kill_state": kill_state, "limit_state": limits.budget_state()}

    @staticmethod
    def _state_marker(counters: dict, reasons: list, rounds: int, controls: dict) -> tuple:
        return (tuple(sorted(counters.items())), tuple(reasons), rounds,
                json.dumps(controls, sort_keys=True, separators=(",", ":")))

    def state(self, limits: "CanaryLimits", kill: KillSwitch) -> dict:
        """The snapshot as it would be written now: counters, rounds, halts and
        every intent still in the hot state - the open ones, and finished ones
        a compaction has not archived yet (one that finished while a compaction
        ran is in neither its archive batch nor, were it left out here, any
        file at all once the older segments are deleted)."""
        with limits._lock:
            counters = self._counters(limits)
            rounds = sorted(map(str, limits.rounds))
        with self._lock:
            intents = {k: dict(v) for k, v in self.intents.items()}
            seq = self._seq
        return {"version": self.VERSION, "session_id": self.session_id, "wallet": self.wallet_key,
                "started_wall": self.started_wall, **counters, "rounds": rounds, "intents": intents,
                "kill_reasons": [r for r in kill.reasons if not str(r).startswith(self.TRANSIENT)],
                **self._controls(limits, kill), "journal_seq": seq, "segment": self._segment}

    def _next(self) -> int:
        self._seq += 1
        self._since += 1
        return self._seq

    @staticmethod
    def _line(rec: dict) -> bytes:
        return (json.dumps(rec, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")

    # ------------------------------------------------------------ the order path
    def before_send(self, p: "Prepared", limits: "CanaryLimits", kill: KillSwitch) -> None:
        """Persist risk and signed identity before the transport may write: one
        fsynced journal line. Raises Halted (nothing may be written) when it
        cannot be made durable."""
        import hashlib
        with self._lock:
            key = p.client_order_id
            if key in self.intents:
                kill.trip("duplicate durable live send intent")
                raise Halted("duplicate durable live send intent")
            payload = json.loads(p.body)
            limits.record_send(p.amount, p.condition)
            p.stamps["budget_recorded"] = True
            intent = {"status": "DISPATCHING", "wall": time.time(), "end": p.end,
                      "condition": p.condition, "token": p.token, "side": p.side,
                      "amount": p.amount, "reserve": float(getattr(p, "reserve", p.amount)),
                      "order_type": p.order_type, "price": p.price,
                      "body_sha256": hashlib.sha256(p.body.encode()).hexdigest(),
                      "signed_order": payload.get("order"), "order_id": None}
            order_hash = getattr(p, "order_hash", None)
            if order_hash:
                intent["order_hash"] = str(order_hash)
            if getattr(p, "shares", None) is not None:
                intent["shares"] = float(p.shares)
            self.intents[key] = intent
            line = self._line({"seq": self._next(), "op": "intent", "key": key, "intent": intent,
                               "round": p.condition, "c": self._counters(limits)})
        if not self._durable_append(line):
            kill.trip_once(("session-disk", str(self.path)),
                           f"live send intent cannot be persisted ({self.last_error})")
            raise Halted("live send intent cannot be persisted; order not sent")

    def _durable_append(self, line: bytes) -> bool:
        """Append one record to the active segment and flush it to the disk.
        True only when it is durable."""
        try:
            with self._journal_lock:
                try:
                    fd = os.open(str(self._segment_path(self._segment)),
                                 os.O_WRONLY | os.O_APPEND | getattr(os, "O_BINARY", 0))
                except FileNotFoundError:
                    fd = self._create_segment(self._segment)     # its directory entry made durable first
                try:
                    view = memoryview(line)
                    while view:
                        n = os.write(fd, view)
                        view = view[n:]
                    self._sync(fd)
                finally:
                    os.close(fd)
            self.last_error = None
            return True
        except Exception as exc:  # noqa: BLE001 - reported; the caller halts
            self.last_error = f"{type(exc).__name__}: {exc}"[:200]
            return False

    # ------------------------------------------------------------ after the send (never the order path)
    def _start_writer(self, kill: KillSwitch) -> None:
        import queue
        import threading
        if self._writer is not None:
            return
        q: "queue.Queue" = queue.Queue()

        def loop():
            while True:
                item = q.get()
                try:
                    if item is None:
                        return
                    line, why = item
                    if not self._durable_append(line):
                        kill.trip_once(("session-disk", str(self.path)),
                                       f"{why} cannot be persisted ({self.last_error})")
                finally:
                    q.task_done()
        t = threading.Thread(target=loop, name="canary-journal", daemon=True)
        t.start()
        self._writer = (q, t)

    def _submit(self, line: bytes, why: str, kill: KillSwitch) -> None:
        if self._writer is None:
            if not self._durable_append(line):
                kill.trip_once(("session-disk", str(self.path)),
                               f"{why} cannot be persisted ({self.last_error})")
            return
        self._writer[0].put((line, why))

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until every queued record is on disk (tests, shutdown)."""
        if self._writer is None:
            return True
        q = self._writer[0]
        deadline = time.monotonic() + timeout
        while q.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.001)
        return not q.unfinished_tasks

    def close(self) -> None:
        """Drain the writer and stop it."""
        self.flush()
        if self._writer is not None:
            self._writer[0].put(None)
            self._writer[1].join(timeout=5)
            self._writer = None

    def outcome(self, p: "Prepared", row: dict, limits: "CanaryLimits", kill: KillSwitch) -> None:
        with self._lock:
            intent = self.intents.get(p.client_order_id)
            if intent is None:
                return
            kind = row["kind"]
            status = "ACCEPTED" if kind in (MATCHED, PENDING) else kind
            intent.update(status=status, kind=kind, order_id=row.get("order_id"))
            rec = {"seq": self._next(), "op": "outcome", "key": p.client_order_id, "status": status,
                   "kind": kind, "order_id": row.get("order_id"), "c": self._counters(limits)}
            # The reply's trade ids: a FAK's complete matching. A restart can settle
            # the order from them when the venue keeps no record of its id.
            resp = row.get("response") if isinstance(row.get("response"), dict) else {}
            tids = resp.get("tradeIDs") or resp.get("trade_ids")
            if isinstance(tids, list) and tids and all(isinstance(t, str) and t for t in tids):
                intent["trade_ids"] = rec["trade_ids"] = list(tids)
            totals = reply_totals(resp) if kind in (MATCHED, PENDING) else None
            if totals is not None:
                intent["reply_totals"] = rec["reply_totals"] = list(totals)
            line = self._line(rec)
        self._submit(line, "live send outcome", kill)

    def resolve(self, client_order_id: str | None, status: str, limits: "CanaryLimits", kill: KillSwitch,
                *, spent: float | None = None) -> None:
        with self._lock:
            intent = self.intents.get(client_order_id)
            if intent is None:
                return
            intent["status"] = status
            rec = {"seq": self._next(), "op": "resolve", "key": client_order_id, "status": status,
                   "c": self._counters(limits)}
            if spent is not None:
                intent["spent"] = float(spent)
                rec["spent"] = float(spent)
            line = self._line(rec)
        self._submit(line, "live send reconciliation", kill)

    def persist(self, limits: "CanaryLimits", kill: KillSwitch) -> bool:
        """Periodic (a worker): record counter or halt changes not tied to an
        intent - one small line, only when something changed - and compact
        once the journal has grown. True when the disk holds the current state."""
        counters = self._counters(limits)
        reasons = [r for r in kill.reasons if not str(r).startswith(self.TRANSIENT)]
        controls = self._controls(limits, kill)
        marker = self._state_marker(counters, reasons, len(limits.rounds), controls)
        ok = True
        if marker != self._marker:
            with self._lock:
                line = self._line({"seq": self._next(), "op": "state", "kill_reasons": reasons,
                                   "c": counters, **controls})
            ok = self._durable_append(line)
            if ok:
                self._marker = marker
        with self._lock:
            finished = sum(1 for v in self.intents.values() if v.get("status") in self.FINAL)
            due = self._since >= self.COMPACT_RECORDS or finished >= self.COMPACT_FINISHED
        if due:
            ok = self.save(limits, kill) and ok
        return ok

    # ------------------------------------------------------------ compaction
    def save(self, limits: "CanaryLimits", kill: KillSwitch) -> bool:
        """Compaction - a durable checkpoint (open, shutdown, a grown journal;
        a worker thread): archive finished intents, switch to a new segment,
        write the snapshot, delete the segments it includes. True when the file
        holds the current state."""
        with self._save_lock:
            return self._save_unlocked(limits, kill)

    def _save_unlocked(self, limits: "CanaryLimits", kill: KillSwitch) -> bool:
        import tempfile
        from durable_io import replace_atomic
        tmp = None
        new_fd = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                finished = {k: dict(v) for k, v in self.intents.items() if v.get("status") in self.FINAL}
            if finished:
                # The audit trail first: once archived, a finished intent may leave the hot state.
                with open(self.archive_path, "ab") as fh:
                    created = fh.tell() == 0
                    for k, v in finished.items():
                        fh.write(self._line({"key": k, "session_id": self.session_id, "intent": v}))
                    fh.flush()
                    os.fsync(fh.fileno())
                if created:
                    self._fsync_dir(self.path.parent)
            new_fd = self._create_segment(self._segment + 1)
            os.close(new_fd)
            new_fd = None
            with self._journal_lock, self._lock:
                # Every record up to this seq is in the old segments; every later one goes to the new.
                state = self.state(limits, kill)
                state["segment"] = self._segment + 1
                self._segment += 1
                for k, v in finished.items():
                    if self.intents.get(k, {}).get("status") == v.get("status"):
                        del self.intents[k]
                self._since = 0
            fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(json.dumps({**state, "updated_wall": time.time()}, sort_keys=True))
                fh.flush()
                os.fsync(fh.fileno())
            replace_atomic(tmp, self.path)
            tmp = None
            self._fsync_dir(self.path.parent)
            self._snap_seq = state["journal_seq"]
            for k in self._segments():
                if k < state["segment"]:
                    try:
                        self._segment_path(k).unlink()
                    except OSError:
                        pass                # replayed again at the next start: harmless, older than the snapshot
            self._marker = self._state_marker(self._counters(limits), state["kill_reasons"], len(limits.rounds),
                                              {k: state[k] for k in ("kill_state", "limit_state")})
            self.last_error = None
            return True
        except Exception as exc:  # noqa: BLE001 - reported; the caller halts on repeated failure
            self.last_error = f"{type(exc).__name__}: {exc}"[:200]
            if new_fd is not None:
                os.close(new_fd)
            if tmp is not None:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
            return False


@dataclass
class Prepared:
    side: str
    token: str
    condition: str
    end: float
    amount: float
    cap: float
    price: float            # the limit actually signed (cap floored to the tick)
    tick: str
    client_order_id: str
    body: str
    headers: dict
    rules: dict
    stamps: dict
    order_type: str = "FOK"     # FOK (fill-or-kill) or FAK (fill-and-kill: keep what fills, cancel the rest)


async def _once(p: Prepared, attribute: str, action, *args):
    """One outcome per prepared intent; repeated waiters share it.

    These calls run on the sender's event loop, with no yield between checking
    and claiming the future. Await the first action inline so dispatch incurs
    no additional scheduling turn. Cancelling a duplicate waiter cannot cancel
    the original order, its reconciliation or its accounting.
    """
    existing = getattr(p, attribute, None)
    if existing is not None:
        return await asyncio.shield(existing)
    outcome = asyncio.get_running_loop().create_future()
    setattr(p, attribute, outcome)
    try:
        result = await action(*args)
    except BaseException as exc:
        outcome.set_exception(exc)
        outcome.exception()
        raise
    outcome.set_result(result)
    return result


class LeanSender:
    """Prepare (sync, on the event loop) and send (async) one sniper order."""

    def __init__(self, *, builder, signer, creds, host: str, http, clock: ServerClock, rules: RoundRules,
                 limits: CanaryLimits, kill: KillSwitch, version: int = 2, journal=None,
                 timeout_s: float = 3.0, latency_halt_ms: float = 1500.0, max_consecutive_errors: int = 3,
                 latency_extreme_ms: float = 5000.0, guard: "VenueGuard | None" = None,
                 realized_loss=None) -> None:
        import polymarket_trade as pt
        from py_clob_client_v2.clob_types import (CreateOrderOptions, MarketOrderArgsV2, OrderArgsV2, OrderType,
                                                  RequestArgs)
        from py_clob_client_v2.endpoints import POST_ORDER
        from py_clob_client_v2.headers.headers import create_level_2_headers
        from py_clob_client_v2.order_builder.constants import BUY
        from py_clob_client_v2.order_utils.model.order_data_v2 import order_to_json_v2
        self.pt = pt
        self.builder, self.signer, self.creds = builder, signer, creds
        self.url = f"{host.rstrip('/')}{POST_ORDER}"
        self.http, self.clock, self.rules, self.limits, self.kill = http, clock, rules, limits, kill
        self.version, self.journal = version, journal or (lambda row: None)
        self.timeout_s, self.latency_halt_ms = timeout_s, latency_halt_ms
        # Slow venue replies pause new orders and recover automatically; only
        # repeated or extreme ones halt (VenueGuard). One 1.67s reply used to
        # trip the kill switch and stop the canary for the rest of the session.
        self.guard = guard or VenueGuard(kill, slow_ms=latency_halt_ms, extreme_ms=latency_extreme_ms)
        self.max_consecutive_errors, self.consecutive_errors = max_consecutive_errors, 0
        self.realized_loss = realized_loss
        self.session: CanarySession | None = None
        # Audit M7: orders whose outcome is unknown and being reconciled with
        # the venue by their hash. While any is, no new order is sent.
        self.unresolved: dict = {}
        self.ambiguity_resolver = None
        self._lib = dict(CreateOrderOptions=CreateOrderOptions, MarketOrderArgsV2=MarketOrderArgsV2,
                         OrderArgsV2=OrderArgsV2,
                         FOK=OrderType.FOK, FAK=OrderType.FAK, RequestArgs=RequestArgs, POST_ORDER=POST_ORDER,
                         l2=create_level_2_headers, BUY=BUY, to_json=order_to_json_v2)

    # ------------------------------------------------------------ hot path, synchronous
    def prepare(self, *, side: str, token: str, up: str, down: str, condition: str, end: float,
                amount: float, cap: float, decided_pc: int | None = None, order_type: str = "FOK",
                shares: float | None = None) -> Prepared:
        st = {"decided_pc": decided_pc, "prepare_pc": time.perf_counter_ns()}
        if order_type not in ("FOK", "FAK"):
            raise Refused(f"unknown order type {order_type!r}")
        self.kill.check()
        self.guard.check()                            # Refused while the venue guard has new orders paused
        for client_id in list(getattr(self, "unresolved", {}) or {}):
            self.kill.trip_once(("order-uncertain", str(client_id)),
                                f"order {client_id} outcome unknown: reconciling with the venue")
        self.kill.check()
        ts = self.clock.now_s()                       # Halted before any work if the time is not trusted
        rules = self.rules.get(condition, up, down)
        if self.pt._ambiguous_blocks(str(condition), str(token)):
            client_id = getattr(self, "_ambiguous_pauses", {}).get((str(condition), str(token)))
            key = (("order-uncertain", str(client_id)) if client_id is not None else
                   ("ambiguous-token", str(condition), str(token)))
            self.kill.trip_once(key, "an earlier submission on this token has an unknown outcome")
            self.kill.check()
        tick = f"{Decimal(str(rules['tick'])).normalize():f}"
        step = Decimal(tick)
        exact = (Decimal(str(cap)) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        if not (step <= exact <= 1 - step and exact <= Decimal(str(cap))):
            raise Refused(f"capped price {exact} outside the tradable range for tick {tick}")
        size = None
        if shares is not None:
            # F1: a share-sized FAK - a limit order for exactly these whole
            # shares at the cap. Its USDC amount (the most it can spend) is
            # shares x limit: whole cents, as every signed stake is.
            if order_type != "FAK":
                raise Refused("a share-sized order is a fill-and-kill")
            size = Decimal(str(shares))
            if size != size.to_integral_value() or size <= 0:
                raise Refused("a share-sized order is whole shares")
            maker = size * exact
            if maker != maker.quantize(Decimal("0.01")):
                raise Refused(f"{size} shares at {exact} is not a whole-cent amount")
            amount = float(maker)
        n_shares = size if size is not None else Decimal(str(amount)) / exact
        if n_shares < Decimal(str(rules["minimum"])):
            raise Refused(f"{n_shares:.4f} shares at {exact} is below the venue minimum {rules['minimum']}")
        self.limits.check(float(amount), float(n_shares), str(condition), self.kill,
                          self.realized_loss() if self.realized_loss else None)
        lib = self._lib
        st["build_pc"] = time.perf_counter_ns()
        if hasattr(self.builder, "last_sign_ns"):
            self.builder.last_sign_ns = None
        if size is not None:
            signed = self.builder.build_order(
                lib["OrderArgsV2"](token_id=str(token), price=signing_price(cap, tick), size=float(size),
                                   side=lib["BUY"]),
                lib["CreateOrderOptions"](tick_size=tick, neg_risk=bool(rules["neg_risk"])), version=self.version)
        else:
            signed = self.builder.build_market_order(
                lib["MarketOrderArgsV2"](token_id=str(token), amount=signing_amount(amount), side=lib["BUY"],
                                         price=signing_price(cap, tick), order_type=lib[order_type]),
                lib["CreateOrderOptions"](tick_size=tick, neg_risk=bool(rules["neg_risk"])), version=self.version)
        st["signed_pc"] = time.perf_counter_ns()
        if getattr(self.builder, "last_sign_ns", None):
            st["sign_start_pc"], st["sign_end_pc"] = self.builder.last_sign_ns     # T3, T4
        order_hash = getattr(self.builder, "last_order_hash", None)
        if order_hash is None and self.version == 2:
            from sniper_mode import order_hash_v2
            chain = getattr(self.signer, "get_chain_id", lambda: 137)()
            order_hash = order_hash_v2(getattr(signed, "order", signed), neg_risk=bool(rules["neg_risk"]),
                                       chain_id=chain, signer=self.signer)
        payload = lib["to_json"](signed, self.creds.api_key, lib[order_type])
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        # No Connection header: HTTP/2 forbids connection-specific headers
        # (RFC 9113 8.2.2) and a front end may reset the stream for one -
        # after the headers went out, which reads as an AMBIGUOUS order.
        headers = {"User-Agent": "py_clob_client_v2", "Accept": "*/*", "Content-Type": "application/json"}
        headers.update(lib["l2"](self.signer, self.creds,
                                 lib["RequestArgs"](method="POST", request_path=lib["POST_ORDER"],
                                                    body=payload, serialized_body=body), timestamp=ts))
        st["ready_pc"] = time.perf_counter_ns()       # REQUEST READY (not sent)
        salt = getattr(getattr(signed, "order", signed), "salt", None) or payload.get("order", {}).get("salt")
        p = Prepared(side, str(token), str(condition), float(end), float(amount), float(cap),
                     float(exact), tick, str(salt), body, headers, dict(rules), st, order_type)
        p.order_hash = order_hash
        p.shares = float(size) if size is not None else None
        return p

    # ------------------------------------------------------------ the write, synchronous
    def _check_send(self, p: Prepared, *, budget: bool = True) -> None:
        self.kill.check()
        self.guard.check()
        for client_id in list(getattr(self, "unresolved", {}) or {}):
            self.kill.trip_once(("order-uncertain", str(client_id)),
                                f"order {client_id} outcome unknown: reconciling with the venue")
        self.kill.check()
        self.clock.now_s()
        current_rules = self.rules.get(p.condition, p.rules["up_token"], p.rules["down_token"])
        if current_rules != p.rules:
            raise Refused("market rules changed after the order was prepared")
        if p.end <= time.time():
            raise Refused("market expired before dispatch")
        check = getattr(p, "send_guard", None)
        if check is not None:
            check()
        if budget and not p.stamps.get("budget_recorded"):
            self.limits.check(p.amount, p.amount / p.price, p.condition, self.kill,
                              self.realized_loss() if self.realized_loss else None)

    def _record_send(self, p: Prepared) -> None:
        if p.stamps.get("budget_recorded"):
            return
        if self.session is not None:
            self.session.before_send(p, self.limits, self.kill)
            return
        self.limits.record_send(p.amount, p.condition)
        p.stamps["budget_recorded"] = True

    def start(self, p: Prepared):
        """Write the order to the wire NOW - inside the caller's own step, no
        event-loop turn - when the HTTP client has a live HTTP/2 connection
        (fast_h2.FastH2.post_now). Returns the in-flight stream for send(), or
        None when it cannot (httpx, or no live connection): send() then goes
        the async way, which connects first. Nothing is written on None."""
        # Repeated start/send calls must never write the same signed order or
        # release its cash/fill reservation twice, including after an ACK.
        if hasattr(p, "_dispatch_stream"):
            return p._dispatch_stream
        if getattr(p, "_sender_outcome", None) is not None:
            return None
        post_now = getattr(self.http, "post_now", None)
        if post_now is None or not getattr(self.http, "connected", False):
            return None
        p.stamps["post_call_pc"] = time.perf_counter_ns()
        try:
            self._check_send(p)
            self._record_send(p)               # durable intent before the first possible write
            self._check_send(p, budget=False)  # disk flush may have outlived the signal/rules/expiry
            stream = post_now(self.url, p.body.encode("utf-8"), p.headers)
        except Exception as exc:
            from fast_h2 import NotConnected, Stream
            if not isinstance(exc, NotConnected):
                # An arbitrary transport failure cannot prove nothing left the
                # process. Pass it to send() as a failed attempt, never retry it.
                stream = Stream(-1, asyncio.get_running_loop().create_future())
                from fast_h2 import NotProcessed
                stream.future.set_exception(NotProcessed(str(exc)) if isinstance(exc, (Halted, Refused)) else exc)
                stream.future.exception()
                if not isinstance(exc, (Halted, Refused, NotProcessed)):
                    self._record_send(p)
                p._dispatch_stream = stream
                return stream
            return None
        p._dispatch_stream = stream
        return stream

    # ------------------------------------------------------------ the send, async
    async def send(self, p: Prepared, started=None) -> dict:
        return await _once(p, "_sender_outcome", self._send_once, p, started)

    async def _send_once(self, p: Prepared, started=None) -> dict:
        if hasattr(p, "_dispatch_stream"):
            started = p._dispatch_stream
        row = {"client_order_id": p.client_order_id, "side": p.side, "token": p.token,
               "condition": p.condition, "round_end": p.end, "cap": p.cap, "price": p.price, "tick": p.tick, "amount": p.amount,
               "order_type": p.order_type}
        kind, resp, err, status = None, None, None, None
        st = p.stamps
        if started is not None:                         # already on the wire (start): await the reply
            events = started.events
            try:
                r = await started.response(self.timeout_s)
                st["response_body_pc"] = time.perf_counter_ns()
                status = r.status_code
                row["retry_after"] = self._retry_after(r)
                try:
                    resp = r.json()
                except ValueError:
                    resp = None
                kind, err = self._classify(status, resp, r.text)
            except BaseException as exc:  # noqa: BLE001 - the bytes went out: unknown unless refused
                from fast_h2 import NotProcessed
                err = f"{type(exc).__name__}: {exc}"[:200]
                kind = NOT_SENT if isinstance(exc, NotProcessed) else AMBIGUOUS
                if not isinstance(exc, Exception):
                    self._finish(p, row, st, events, kind, resp, err, status)
                    raise
            return self._finish(p, row, st, events, kind, resp, err, status)
        events = []
        st["post_call_pc"] = time.perf_counter_ns()

        async def trace(name, info):
            if name.endswith("send_request_headers.started") and not hasattr(self.http, "post_now"):
                self._check_send(p, budget=False)
            events.append((name, info.get("timestamp_ns", time.perf_counter_ns())))
        try:
            self._check_send(p)
            self._record_send(p)
            r = await self.http.post(self.url, content=p.body.encode("utf-8"), headers=p.headers,
                                     extensions={"trace": trace,
                                                 "before_write": lambda: self._check_send(p, budget=False)},
                                     timeout=self.timeout_s)
            st["response_body_pc"] = time.perf_counter_ns()
            status = r.status_code
            row["retry_after"] = self._retry_after(r)
            try:
                resp = r.json()
            except ValueError:
                resp = None
            kind, err = self._classify(status, resp, r.text)
        except BaseException as exc:  # noqa: BLE001 - transport failure: sent or not decides everything
            from fast_h2 import NotProcessed
            err = f"{type(exc).__name__}: {exc}"[:200]
            sent = any(n.endswith("send_request_headers.started") for n, _ in events)
            kind = NOT_SENT if isinstance(exc, NotProcessed) or not sent else AMBIGUOUS
            if not isinstance(exc, Exception):          # cancelled / interrupted: record it, then let it go on
                self._finish(p, row, st, events, kind, resp, err, status)
                raise
        return self._finish(p, row, st, events, kind, resp, err, status)

    def _finish(self, p, row, st, events, kind, resp, err, status) -> dict:
        for name, t in events:                          # the transport's own stamps
            if name.endswith("send_request_headers.started"):
                st.setdefault("net_send_start_pc", t)
            elif name.endswith("send_request_body.complete"):
                st["net_send_done_pc"] = t
            elif name.endswith("receive_response_headers.complete"):
                st["response_pc"] = t
        new_conn = any("connect_tcp" in n or "start_tls" in n for n, _ in events)
        row.update(kind=kind, http_status=status, error=err, new_connection=new_conn,
                   order_id=((resp or {}).get("orderID") or (resp or {}).get("order_id"))
                   if isinstance(resp, dict) else None,
                   venue_status=(resp or {}).get("status") if isinstance(resp, dict) else None,
                   response=resp if isinstance(resp, dict) else None)
        self._after(p, row, st)
        return row

    @staticmethod
    def _retry_after(response):
        # Both httpx.Headers and FastH2's list of pairs are supported.
        headers = getattr(response, "headers", {})
        pairs = headers.items() if hasattr(headers, "items") else headers
        for key, value in pairs:
            if str(key).lower() == "retry-after":
                return str(value)
        return None

    @staticmethod
    def _has_acceptance_evidence(resp: dict, status: int | None = None) -> bool:
        """Signs that the venue took (part of) the order, beside a no-fill message.

        An order id is not one: the venue can echo the order's hash on a refusal
        (the client computes it anyway). Counting it turned the first live FOK
        miss after the change - HTTP 400 "order couldn't be fully filled" - into
        an unknown outcome and halted the canary, where the old check had read
        the same reply as a miss on every earlier live order. On a 4xx, a request
        the venue refused, echoed amounts do not count either; only an explicit
        success flag, trade ids / transaction hashes or a matched / delayed /
        live status do."""
        if resp.get("success") is True or resp.get("ok") is True:
            return True
        if any(resp.get(k) for k in ("tradeIDs", "trade_ids", "transactionsHashes", "transaction_hashes")):
            return True
        if str(resp.get("status", "")).strip().lower() in ("matched", "delayed", "live"):
            return True
        if status is not None and 400 <= status < 500:
            return False
        return any(resp.get(k) not in (None, "", 0, "0", "0.0") for k in ("makingAmount", "takingAmount"))

    def _classify(self, status: int, resp, text: str):
        pt = self.pt
        # Some documented FOK misses are HTTP 500. Only a clear no-fill
        # payload without conflicting placement evidence is safe to release.
        error = (resp.get("errorMsg") or resp.get("error") or resp.get("message")) if isinstance(resp, dict) else None
        known_miss = isinstance(error, str) and pt._is_no_match(error)
        post_only = status == 503 and isinstance(resp, dict) and resp.get("code") == "post_only_mode"
        # Documented 500: "rejected before reaching the order book and can be
        # safely resubmitted" - a rejection, not an unknown outcome.
        timed_out = status == 500 and isinstance(error, str) and "order timed out" in error.lower()
        if known_miss or post_only or timed_out:
            if self._has_acceptance_evidence(resp, status):
                return AMBIGUOUS, "conflicting rejection and acceptance evidence"
            if known_miss:
                return NO_MATCH, error
            if timed_out:
                return REJECTED, f"{error} (rejected before the book)"
            return REJECTED, str(error or "post_only_mode")
        if (isinstance(resp, dict) and self._has_acceptance_evidence(resp, status)
                and (status >= 400 or resp.get("success") is False or resp.get("ok") is False
                     or str(resp.get("status") or "").strip().lower() in pt._FAILED_ORDER_STATUSES)):
            return AMBIGUOUS, "conflicting rejection and acceptance evidence"
        if status == 429:
            return REJECTED, "rate limited (429)"
        if status >= 500:
            return AMBIGUOUS, f"server error {status}"     # the venue may have taken it
        if not isinstance(resp, dict):
            return (REJECTED, f"HTTP {status}: {text[:120]}") if 400 <= status < 500 else (
                AMBIGUOUS, f"unparseable response (HTTP {status})")
        oid, _st, err = pt._accepted_order_response(resp)
        if err is None:
            return MATCHED, None
        if pt._is_no_match(err):
            return NO_MATCH, err
        pending_oid, _pst, pending_kind = pt._accepted_pending_response(resp)
        if pending_oid and pending_kind:
            return PENDING, pending_kind
        if pt._definitive_rejection(resp) or 400 <= status < 500:
            return REJECTED, err
        return AMBIGUOUS, f"unclassified response: {err}"

    # Rejections that mean something is wrong with this bot, not with this
    # one order: stop new orders. (Rate limits and closed markets are not.)
    HALT_ON = {"timestamp": "request time", "nonce": "request time", "expired": "request time",
               "order_version_mismatch": "order version", "balance": "collateral", "allowance": "collateral",
               "duplicat": "duplicate order", "api key": "credentials", "unauthorized": "credentials",
               "signature": "signature"}
    # Rejections that mean the round's prefetched rules are stale: drop them
    # so the next order is refused until a background refetch.
    STALE_RULES_ON = ("tick", "min size", "minimum", "min_size")

    def _after(self, p: Prepared, row: dict, st: dict) -> None:
        kind = row["kind"]
        if st.get("budget_recorded"):
            self.limits.record_outcome(kind, p.amount)
        if kind == AMBIGUOUS:
            self.pt._mark_ambiguous(p.condition, p.end, p.token)
            self._ambiguous_pauses = getattr(self, "_ambiguous_pauses", {})
            self._ambiguous_pauses[(str(p.condition), str(p.token))] = p.client_order_id
            self.kill.trip_once(("order-uncertain", str(p.client_order_id)),
                                f"order {p.client_order_id} outcome unknown ({row['error']}): reconciling with the venue")
            resolver = getattr(self, "ambiguity_resolver", None)
            if resolver is not None and getattr(p, "order_hash", None) and p.order_type == "FAK":
                # Keep reconciling its own hash throughout the one pause. The
                # old order and its reservation remain after trading resumes.
                self.unresolved = getattr(self, "unresolved", None) or {}
                self.unresolved[p.client_order_id] = p
                row["auto_reconcile"] = True
                resolver(p, dict(row))
        text = str(row["error"]).lower()
        if kind == REJECTED:
            hit = next((why for w, why in self.HALT_ON.items() if w in text), None)
            if hit or row["http_status"] in (401, 403):
                self.kill.trip(f"venue rejected the order ({hit or 'HTTP %s' % row['http_status']}): {row['error']}")
            if any(w in text for w in self.STALE_RULES_ON):
                self.rules.invalidate(p.condition)
                row["rules_invalidated"] = True
        temporary = (kind == REJECTED and row["http_status"] == 503
                     and (row.get("response") or {}).get("code") == "post_only_mode")
        if temporary:
            waits = []
            for value in (row["response"].get("retry_after_seconds"), row.get("retry_after")):
                try:
                    seconds = float(value)
                    if math.isfinite(seconds) and seconds >= 0:
                        waits.append(seconds)
                except (TypeError, ValueError):
                    pass
            delay = max(waits, default=30.0)
            self.guard.pause_for(300.0, "venue post-only mode")
            row["venue_retry_after_s"] = delay
        if row["new_connection"]:
            row["warning"] = "a new TCP/TLS connection was opened on the hot path"
        if kind in (MATCHED, PENDING, NO_MATCH) or temporary:
            self.consecutive_errors = 0
        else:
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.max_consecutive_errors:
                self.kill.trip(f"{self.consecutive_errors} consecutive order errors")
                self.consecutive_errors = 0         # after the 5-minute pause, count afresh
        if "net_send_start_pc" in st and "response_pc" in st and kind != AMBIGUOUS:
            rtt = (st["response_pc"] - st["net_send_start_pc"]) / 1e6
            row["reply_ms"] = round(rtt, 1)
            if rtt > self.guard.slow_ms:
                row["slow_reply"] = True
            row["guard"] = self.guard.on_reply(rtt)
        base = st.get("decided_pc") or st["prepare_pc"]
        row["ts_us"] = {k[:-3]: round((v - base) / 1000) for k, v in st.items()
                        if k.endswith("_pc") and isinstance(v, int)}
        row["wall"] = time.time()
        if self.session is not None:
            self.session.outcome(p, row, self.limits, self.kill)
        self.journal(row)


# =========================================================================== the live canary
# Everything below wires the LeanSender into the bot for a LIVE CANARY: the
# Sniper's decisions unchanged, real orders, every accepted order authorized in
# the live ledger before its fills can count, and a reconciler that stops new
# orders on anything it cannot explain. Off unless SNIPER_LIVE_CANARY=1 with
# --live and SNIPER_ONLY=1 (run_feeds enforces all three).

def lean_view(view, cap: float):
    """The book view cut to what the pre-send check reads: the asks up to the
    cap (the FOK walk and the venue-minimum sizing both stop there) and the
    best bid (the spread). Converting every level of both sides to Decimal
    cost ~2ms of the live order path; the checks' results are identical
    (tests_sniper_live compares them on random books). Keeps at least the
    best ask, so a book whose best ask is above the cap is refused exactly
    as before."""
    if not dataclasses.is_dataclass(view):
        return view
    asks = view.asks
    n = 0
    while n < len(asks) and asks[n][0] <= cap + 1e-12:
        n += 1
    return dataclasses.replace(view, asks=asks[:max(n, 1)], bids=view.bids[:1])


def limits_from_env(env=None) -> CanaryLimits:
    """Canary caps from the environment. Defaults are deliberately small:
    spend is counted at send and never given back for a fill, so
    SNIPER_CANARY_MAX_SPEND_USD bounds what the whole session can lose.

    A set value must be a finite number >= 0 (a whole number for the counts),
    else ValueError: these are the live safety caps, so a typo must stop the
    start, never quietly become the default. "nan" used to pass and switched
    its cap off entirely (every comparison with NaN is false), and "3O" became
    the default 25 shares."""
    import os
    env = os.environ if env is None else env

    def num(name, default, cast=float):
        raw = env.get(name)
        if raw is None or not str(raw).strip():
            return cast(default)
        try:
            value = float(str(raw).strip())
        except ValueError:
            raise ValueError(f"{name}={raw!r} is not a number") from None
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name}={raw!r} must be a finite number >= 0")
        if cast is int and value != int(value):
            raise ValueError(f"{name}={raw!r} must be a whole number")
        return cast(value)
    return CanaryLimits(max_order_shares=num("SNIPER_CANARY_MAX_SHARES", 25),
                        max_orders=num("SNIPER_CANARY_MAX_ORDERS", 40, int),
                        max_fills=num("SNIPER_CANARY_MAX_FILLS", 20, int),
                        max_rounds=num("SNIPER_CANARY_MAX_ROUNDS", 12, int),
                        max_exposure_usd=num("SNIPER_CANARY_MAX_SPEND_USD", 60),
                        max_daily_loss_usd=num("SNIPER_CANARY_MAX_LOSS_USD", 20))


class LiveBrokerShim:
    """The broker surface the Sniper reads, for a live canary (there is no
    paper broker in live): the round's trade window, the account's price band
    and spread limit, and the market rules an order is sized with - the same
    paper_trade functions, so live sizes exactly as paper sizes. No own-fill
    netting: live, our fills are already in the real book."""
    mode = "LIVE"
    own_fill_ttl_s = 0.0
    supports_arrival_book = False

    def __init__(self, rules_fetch, *, min_buy_price: float, max_buy_price: float, max_spread: float,
                 trade_window_seconds: float, min_seconds_to_expiry: float, ttl_s: float = 600.0) -> None:
        import threading
        self._fetch, self.ttl_s = rules_fetch, ttl_s
        self.min_buy_price, self.max_buy_price, self.max_spread = min_buy_price, max_buy_price, max_spread
        self.trade_window_seconds, self.min_seconds_to_expiry = trade_window_seconds, min_seconds_to_expiry
        self._cache: dict = {}
        self._lock = threading.Lock()
        self._generation: dict = {}
        self.last_fill = self.last_error = None
        self.last_timing: dict = {}

    def cached_rules(self, condition_id):
        """The rules if already loaded and fresh, else None. Never does I/O:
        this is what the order path reads, on the event loop."""
        with self._lock:
            hit = self._cache.get(str(condition_id))
        if hit is None or time.time() - hit[1] > self.ttl_s:
            return None
        return hit[0]

    def invalidate(self, condition_id):
        key = str(condition_id)
        with self._lock:
            self._cache.pop(key, None)
            self._generation[key] = self._generation.get(key, 0) + 1

    def _rules(self, condition_id, *, force=False):
        """Fetch-through (worker threads only: round-start prefetch, shadows)."""
        key = str(condition_id)
        hit = self.cached_rules(key)
        if hit is not None and not force:
            return hit
        with self._lock:
            generation = self._generation.get(key, 0)
        rules = self._fetch(key)
        with self._lock:
            if generation != self._generation.get(key, 0):
                raise Refused("sizing rules invalidated during refresh")
            self._cache[key] = (rules, time.time())
            while len(self._cache) > 64:
                self._cache.pop(next(iter(self._cache)))
        return rules

    def has_recent_own_fill(self, token) -> bool:
        return False

    def _net_of_own_fills(self, book):
        return book


class LiveExecution:
    """One live canary: sizing exactly as paper sizes, the LeanSender for the
    send, the live ledger's authorization for every accepted order, and a
    reconciler. Anything unexplained trips the kill switch, which stops NEW
    orders only - reconciliation keeps running."""

    def __init__(self, *, sender: LeanSender, shim: LiveBrokerShim, ledger, receipt_sink, balance_fetch,
                 clock_url: str, order_status=None, user_ready=None, fills=None, on_event=None,
                 assumed_delay_s: float = 0.0, keep_warm_s: float = 10.0, balance_every_s: float = 30.0,
                 balance_max_age_s: float = 120.0, reconcile_every_s: float = 5.0,
                 confirm_within_s: float = 180.0, unexplained_grace_s: float = 20.0,
                 rules_refresh_s: float = 240.0) -> None:
        import polymarket_trade as pt
        self.pt = pt
        self.sender, self.shim, self.ledger = sender, shim, ledger
        # A venue tick/minimum rejection invalidates BOTH views of the rules.
        sender.rules.on_invalidate = shim.invalidate
        self.receipt_sink, self.balance_fetch, self.order_status = receipt_sink, balance_fetch, order_status
        # user_ready(condition) -> bool: the private fill channel covers that
        # market. An order whose fills cannot arrive would only trip the
        # confirmation deadline later.
        self.user_ready, self.fills = user_ready, fills
        self.clock_url = clock_url
        self.on_event = on_event or (lambda *a, **k: None)
        self.assumed_delay_s = float(assumed_delay_s)
        self.keep_warm_s, self.balance_every_s, self.balance_max_age_s = keep_warm_s, balance_every_s, balance_max_age_s
        self.reconcile_every_s, self.confirm_within_s = reconcile_every_s, confirm_within_s
        self.unexplained_grace_s, self.rules_refresh_s = unexplained_grace_s, rules_refresh_s
        self.balance: float | None = None
        self.balance_at: float | None = None
        self.reserved = 0.0
        import threading
        self._risk_lock = threading.RLock()
        self._risk_orders: dict = {}        # client id -> full all-in reservation until fully reconciled
        self._balance_revision = 0
        self.awaiting: dict = {}            # order id -> what we expect the ledger to confirm
        self.fak_orders: dict = {}          # retained metadata for validating partial-fill updates
        self._sync_fak = lambda: None       # event-loop callback, never invoked from a worker
        self.session_start = time.time()
        self.session_loss = 0.0
        self.baseline_unexplained: set = set()
        self._unexplained_since: dict = {}
        self.rules_ready = None             # (condition, up, down) the order rules were last loaded for
        self.n = collections.Counter()
        self._tasks: list = []
        self._announced: set = set()
        self._tripped_once: set = set()     # a lasting condition pauses once, then trading resumes
        if hasattr(self.sender, "kill") and hasattr(self.sender.kill, "on_resume"):
            self.sender.kill.on_resume = self._on_resume
        self.hub = None
        self.guard_probe_s = 2.0
        self.session: CanarySession | None = None     # set by build_live_execution (audit F3)
        self.persist_every_s = 0.5
        self._persist_failures = 0
        sender.realized_loss = lambda: self.session_loss
        sender.guard.on_event = self.on_event
        # M6 / M7: per-order reconciliation tasks (fast FAK resolution, unknown outcomes)
        self._order_tasks: set = set()
        self.fast_fak_delays_s = (0.25, 0.5, 1.0, 2.0, 4.0)
        self.ambiguity_delays_s = (0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0)
        # A previous run's unresolved orders: looked up again after each delay
        # (the ledger books their trades as the reconciler catches up).
        # a previous run's unresolved orders: looked up for about an hour (the
        # start-up pause ends after 5 minutes; their worst case stays reserved)
        self.previous_run_delays_s = (0.0, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0) + (300.0,) * 12
        sender.ambiguity_resolver = self._schedule_ambiguity

    @property
    def kill(self) -> KillSwitch:
        return self.sender.kill

    def attach(self, sniper) -> None:
        """The Sniper's journal receives every send row, and every venue-guard
        transition and recovery check."""
        self.sender.journal = lambda row: sniper._write({**row, "result": "live_send"})
        self.sender.guard.journal = sniper._write
        self._journal = sniper._write
        self._sync_fak = sniper._sync_live_fak
        self._sniper_settings = sniper.s

    def limits_line(self) -> str:
        lim = self.sender.limits
        return (f"SNIPER LIVE CANARY limits: <= {lim.max_order_shares:g} sh/order, {lim.max_orders} orders, "
                f"{lim.max_fills} fills, {lim.max_rounds} rounds, ${lim.max_exposure_usd:g} total spend, "
                f"${lim.max_daily_loss_usd:g} worst-case loss incl. open orders; stop file {self.kill.stop_file}")

    # ------------------------------------------------------------ start / stop / background
    async def start(self, hub) -> None:
        """Sync the clock on the order connection and read the collateral
        before the first order; then keep both fresh, load each round's
        rules and reconcile. Raises if it cannot arm: the caller must not
        trade then."""
        if not await self.sender.clock.refresh(self.sender.http, self.clock_url):
            raise RuntimeError("could not sync the CLOB clock over the order connection")
        await self._read_balance()
        if self.balance is None:
            raise RuntimeError("could not read the collateral balance/allowance")
        # Recovery cannot distinguish a manual trade from an orphaned bot
        # write. Never silently exempt recovered unexplained fills.
        self.baseline_unexplained = set()
        for tid in await asyncio.to_thread(self._unexplained_now):
            self._trip_once(("unexplained", tid),
                            f"unexplained recovered wallet fill {short_id(tid)}: reconciling in background")
        self.hub = hub
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(keep_warm(self.sender.http, self.sender.clock, self.clock_url,
                                                  self.keep_warm_s, self._warm_event)),
                       loop.create_task(self._balance_loop()), loop.create_task(self._reconcile_loop()),
                       loop.create_task(self._rules_loop(hub)), loop.create_task(self._guard_loop())]
        if self.session is not None:
            self._tasks.append(loop.create_task(self._session_loop()))
            # A previous run's unresolved orders: their worst case counts against the loss
            # budget until the venue proves them (the start-up pause ends after 5 minutes)
            previous = self.session.unresolved()
            self._previous_run_keys = {key for key, _intent in previous}
            with self._risk_lock:
                for key, intent in previous:
                    self._risk_orders.setdefault(key, {
                        "condition": str(intent.get("condition")), "token": str(intent.get("token")),
                        "reserve": float(intent.get("reserve") or intent.get("amount") or 0.0),
                        "order_id": intent.get("order_hash") or intent.get("order_id"),
                        # A BUY cap is a maximum price, not a maximum quantity.
                        # Share-sized intents carry their exact size; an old
                        # amount-sized intent uses the venue's minimum tick.
                        "shares": float(intent.get("shares") or float(intent.get("amount") or 0.0) / 0.0001)})
            self._tasks.append(loop.create_task(self._reconcile_previous_run()))
        if self.kill.stop_file is not None and self.kill.stop_file.exists():
            self.on_event("sniper", f"LIVE CANARY: stop file {self.kill.stop_file} present - no orders "
                                    f"(remove it to release this stop)", "bad")
        self.on_event("sniper", f"LIVE CANARY armed: collateral ${self.balance:.2f}; " + self.limits_line(), "warn")

    async def _session_loop(self) -> None:
        """Persist the canary's counters and halts within persist_every_s of a
        change, on a worker - one small journal line when something changed,
        a compaction when the journal has grown. Three failed writes in a row
        halt new orders: a budget that cannot be made durable is not a budget."""
        while True:
            await asyncio.sleep(self.persist_every_s)
            ok = await asyncio.to_thread(self.session.persist, self.sender.limits, self.kill)
            self._persist_failures = 0 if ok else self._persist_failures + 1
            if self._persist_failures >= 3:
                self._trip_once(("session-disk", str(self.session.path)),
                                f"canary session state cannot be written ({self.session.last_error})")

    async def stop(self) -> None:
        for t in list(self._tasks) + list(self._order_tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, *self._order_tasks, return_exceptions=True)
        self._tasks = []
        self._order_tasks = set()
        if self.session is not None:
            await asyncio.to_thread(self.session.flush)
            await asyncio.to_thread(self.session.save, self.sender.limits, self.kill)
            await asyncio.to_thread(self.session.close)
        try:
            await self.sender.http.aclose()
        except Exception:  # noqa: BLE001 - shutting down
            pass

    def _warm_event(self, row: dict) -> None:
        if row.get("new_connection"):
            self.n["keep-warm reconnects"] += 1
        if row.get("error"):
            self.n["keep-warm errors"] += 1

    async def _read_balance(self) -> None:
        revision = self._balance_revision
        bal = await asyncio.to_thread(self.balance_fetch)
        if isinstance(bal, dict) and bal.get("balance") is not None and bal.get("allowance") is not None:
            # Spendable is the smaller of what we hold and what the exchange may move.
            try:
                values = (float(bal["balance"]), float(bal["allowance"]))
                if not all(math.isfinite(v) and v >= 0 for v in values):
                    raise ValueError("invalid collateral reading")
            except (TypeError, ValueError, OverflowError):
                self.n["balance read errors"] += 1
                return
            available = min(values)
            if (self.balance is not None and (revision != self._balance_revision or self.reserved > 0
                                              or self.awaiting)):
                # A read concurrent with an order can return its pre-fill cash.
                # Until confirmation, it may lower our bound but never refund it.
                available = min(available, self.balance)
            self.balance = available
            self.balance_at = time.monotonic()
        else:
            self.n["balance read errors"] += 1

    async def _balance_loop(self) -> None:
        while True:
            await asyncio.sleep(self.balance_every_s)
            try:
                await self._read_balance()
            except Exception:  # noqa: BLE001 - the age check in prepare refuses orders if this keeps failing
                self.n["balance read errors"] += 1

    async def _rules_loop(self, hub) -> None:
        """Load the round's order rules (tick, minimum, neg-risk, fees and the
        UP/DOWN mapping, through the audited validator) as soon as the hub is
        on a new round, and again before they expire - never on the order path."""
        done, at = None, 0.0
        prepared_done, prepared_at = None, 0.0
        while True:
            key = (getattr(hub, "condition_id", None), getattr(hub, "up_token", None),
                   getattr(hub, "down_token", None))
            missing = False
            if all(key):
                try:
                    self.sender.rules.get(*(str(x) for x in key))
                except Refused:
                    missing = True
                except Halted as exc:
                    self._trip_once(("rules-mapping", *(str(x) for x in key)), str(exc))
                    self.sender.rules.invalidate(str(key[0]))
                    missing = True
                missing = missing or self.shim.cached_rules(str(key[0])) is None
            if all(key) and not missing and key != done and key == prepared_done:
                # Loaded before the boundary for exactly this round (audit M5):
                # ready at once - no invalidation, no refetch, no gap.
                done, at, self.rules_ready = key, prepared_at, key
                self.n["rules ready at the boundary (prefetched)"] += 1
            if all(key) and (missing or key != done or time.monotonic() - at > self.rules_refresh_s):
                cond, up, down = (str(x) for x in key)
                self.rules_ready = None
                try:
                    self.sender.rules.invalidate(cond)
                    await self.sender.rules.prefetch(cond, up, down)
                    sizing = await asyncio.to_thread(self.shim._rules, cond, force=True)
                    signing = self.sender.rules.get(cond, up, down)
                    if sizing.tick_size != signing["tick"] or sizing.min_order_size != signing["minimum"]:
                        self.sender.rules.invalidate(cond)
                        raise Refused("signing and sizing rules changed during refresh")
                    done, at, self.rules_ready = key, time.monotonic(), key
                    self.n["rules loaded"] += 1
                except Exception as exc:  # noqa: BLE001 - orders are refused until this succeeds
                    self.n["rules load errors"] += 1
                    msg = f"round rules not loaded: {type(exc).__name__}: {exc}"[:200]
                    if msg not in self._announced:
                        self._announced.add(msg)
                        self.on_event("sniper", f"LIVE CANARY {msg}", "warn")
                    await asyncio.sleep(2.0)
                    continue
            # The next round, prepared before its boundary: its order rules
            # (the audited CLOB mapping check) and sizing rules, loaded now.
            prepared = getattr(hub, "prepared_round", None) or {}
            pkey = tuple(str(prepared.get(k) or "") for k in ("condition_id", "up_token_id", "down_token_id"))
            if all(pkey) and pkey != prepared_done and pkey != done:
                try:
                    await self.sender.rules.prefetch(*pkey)
                    sizing = await asyncio.to_thread(self.shim._rules, pkey[0], force=True)
                    signing = self.sender.rules.get(*pkey)
                    if sizing.tick_size != signing["tick"] or sizing.min_order_size != signing["minimum"]:
                        self.sender.rules.invalidate(pkey[0])
                        raise Refused("signing and sizing rules disagree for the next round")
                    prepared_done, prepared_at = pkey, time.monotonic()
                    self.n["next round rules prefetched"] += 1
                except Exception as exc:  # noqa: BLE001 - retried; the boundary then loads them the old way
                    self.n["next round rules prefetch errors"] += 1
                    msg = f"next round rules not prefetched: {type(exc).__name__}: {exc}"[:200]
                    if msg not in self._announced:
                        self._announced.add(msg)
                        self.on_event("sniper", f"LIVE CANARY {msg}", "info")
            # Poll fast around a boundary (promotion is then seen within 50ms).
            end = getattr(hub, "window_end", None)
            try:
                left = float(end) - time.time() if end else None
            except (TypeError, ValueError):
                left = None
            await asyncio.sleep(0.05 if left is None or left < 2.0 or left > 298.0 else 0.5)

    async def health_probe(self) -> tuple[bool, str]:
        """One recovery check of everything an order needs, off the order
        path: the order connection answers fast with a fresh server time, the
        clock is trusted, this round's order + sizing rules are loaded, the
        private fill channel covers the market, the collateral reading is
        fresh. (ok, what failed - or a one-line summary)."""
        bad, ms = [], None
        t = time.perf_counter()
        try:
            ok = await asyncio.wait_for(self.sender.clock.refresh(self.sender.http, self.clock_url), 5.0)
            ms = (time.perf_counter() - t) * 1000.0
            if not ok:
                bad.append("clock sample rejected (slow round trip)")
            elif ms > self.sender.guard.slow_ms:
                bad.append(f"order connection slow ({ms:.0f}ms)")
        except Exception as exc:  # noqa: BLE001 - a dead venue fails the check
            bad.append(f"order connection: {type(exc).__name__}")
        try:
            self.sender.clock.now_s()
        except Halted as exc:
            bad.append(str(exc))
        hub = self.hub
        cond, up, down = (getattr(hub, "condition_id", None), getattr(hub, "up_token", None),
                          getattr(hub, "down_token", None))
        if not (cond and up and down):
            bad.append("no current round")
        else:
            if self.user_ready is not None and not self.user_ready(str(cond)):
                bad.append("private fill channel not ready")
            try:
                self.sender.rules.get(str(cond), str(up), str(down))
            except (Refused, Halted) as exc:
                bad.append(f"order rules: {exc}")
            if self.shim.cached_rules(str(cond)) is None:
                bad.append("sizing rules not loaded")
        try:
            await self._read_balance()
        except Exception as exc:  # noqa: BLE001
            bad.append(f"balance read: {type(exc).__name__}")
        if self.balance_at is None or time.monotonic() - self.balance_at > self.balance_max_age_s:
            bad.append("collateral reading stale")
        if bad:
            return False, "; ".join(bad)
        return True, f"order connection {ms:.0f}ms; channel, rules, clock, collateral ok"

    async def _guard_loop(self) -> None:
        """While the venue guard has orders PAUSED: one health probe every
        guard_probe_s, each journalled; enough healthy ones in a row resume."""
        guard = self.sender.guard
        while True:
            await asyncio.sleep(self.guard_probe_s)
            if guard.state != guard.PAUSED:
                continue
            try:
                ok, detail = await self.health_probe()
            except Exception as exc:  # noqa: BLE001 - a broken probe is an unhealthy one
                ok, detail = False, f"probe error {type(exc).__name__}: {exc}"[:160]
            state = guard.on_probe(ok, detail)
            journal = getattr(self, "_journal", None)
            if journal is not None:
                journal({"wall": time.time(), "result": "guard_probe", "ok": ok, "detail": detail[:200],
                         "streak": guard.healthy_streak, "state": state})

    async def _reconcile_loop(self) -> None:
        while True:
            await asyncio.sleep(self.reconcile_every_s)
            try:
                await asyncio.to_thread(self.reconcile_once)
                self._sync_fak()
            except Exception as exc:  # noqa: BLE001 - an unexplained state stops new orders
                self._trip_once(("reconciliation-error", type(exc).__name__),
                                f"reconciliation failed: {type(exc).__name__}: {exc}")

    def _trip_fak(self, a: dict, reason: str) -> None:
        """A FAK's own evidence halts trading: the order keeps its reservation
        and stays awaiting, and is not timed out again for the same order."""
        a.setdefault("halted_for", reason)
        self._trip_once(("order-uncertain", str(a.get("client_order_id") or id(a))), reason)

    def _trip(self, reason: str) -> None:
        self.kill.trip(reason)
        if reason not in self._announced:
            self._announced.add(reason)
            pause = getattr(self.kill, "pause_s", None)
            self.on_event("sniper", f"LIVE CANARY PAUSED{f' {pause / 60:g} min' if pause else ''}: {reason}", "bad")

    def _trip_once(self, key, reason: str) -> None:
        """A condition that stays true pauses once (its 5 minutes), not on every check."""
        if key in self._tripped_once:
            return
        self._tripped_once.add(key)
        self.kill.trip_once(key, reason)
        if reason in self.kill.reasons and reason not in self._announced:
            self._announced.add(reason)
            self.on_event("sniper", f"LIVE CANARY PAUSED 5 min: {reason}", "bad")

    def _on_resume(self, reason: str, seconds: float) -> None:
        self._announced.discard(reason)
        self.n["pauses ended"] += 1
        self.on_event("sniper", f"LIVE CANARY resumes after a {seconds / 60:.0f}-minute pause ({reason[:120]})", "warn")
        journal = getattr(self, "_journal", None)
        if journal is not None:
            journal({"wall": time.time(), "result": "resume", "reason": str(reason)[:200], "paused_s": round(seconds, 1)})

    # ------------------------------------------------------------ ledger views (worker thread)
    def _unexplained_now(self) -> set:
        """Confirmed fills on this wallet the ledger refused because no order
        of ours explains them (or they do not match the order), and that it
        has not booked since. A fill that arrived a moment before its order's
        authorization is booked on the next pass and drops out of this set."""
        led = self.ledger
        with led._lock:
            seen = led.seen
            return {t for group in (getattr(led, "reported_unauthorized", ()), getattr(led, "reported_mismatch", ()))
                    for t in group if t not in seen}

    def _order_booked(self, oid, token) -> bool:
        """Any fill of this order booked in the ledger (on its own token)."""
        has = getattr(self.ledger, "order_has_lots", None)
        if callable(has):
            return has(oid, token)
        with self.ledger._lock:
            return any(lot.order_id == oid for pos in self.ledger.positions.values() for lot in pos.lots)

    def _trades_booked(self, trade_ids) -> bool:
        booked = getattr(self.ledger, "trades_booked", None)
        if callable(booked):
            return booked(trade_ids)
        with self.ledger._lock:
            return all(t in self.ledger.seen for t in trade_ids)

    def _session_realized(self) -> float:
        since = getattr(self.ledger, "realized_since", None)
        if callable(since):
            return since(self.session_start)
        led = self.ledger
        with led._lock:
            return sum(float(getattr(p, "realized", 0) or 0) for p in led.positions.values()
                       if getattr(p, "settled", False) and float(getattr(p, "settled_wall", 0) or 0) >= self.session_start)

    def _risk_budget_used(self) -> float:
        """Loss already realized plus conservative open and uncertain basis.

        Unconfirmed quantities never earn a hedge credit. A partially booked
        order retains its unused full reservation until its complete trade
        set is confirmed, without counting booked basis twice.
        """
        with self._risk_lock:
            reservations = [dict(a) for a in self._risk_orders.values()]
        view = getattr(self.ledger, "risk_view", None)
        if callable(view):
            # O(open positions + this session's settlements + the reserved
            # orders' lots) under the ledger lock - never its whole history.
            realized, open_basis, booked = view(self.session_start,
                                                [(a.get("order_id"), a.get("token")) for a in reservations])
        else:
            with self.ledger._lock:
                positions = list(self.ledger.positions.values())
                realized = sum(float(getattr(p, "realized", 0) or 0) for p in positions
                               if getattr(p, "settled", False)
                               and float(getattr(p, "settled_wall", 0) or 0) >= self.session_start)
                open_basis = sum(float(p.cost) for p in positions if not p.settled and p.shares > 0)
                booked = collections.defaultdict(float)
                for pos in positions:
                    for lot in pos.lots:
                        if not getattr(lot, "order_id", None) or str(lot.side).upper() != "BUY":
                            continue
                        booked[lot.order_id] += float(lot.cost)
        # Settlement of a partially booked position is not proof that the
        # unresolved remainder never executed. Retain that remaining risk.
        uncertain = sum(max(0.0, a["reserve"] - booked.get(a.get("order_id"), 0.0)) for a in reservations)
        value = max(0.0, -realized) + open_basis + uncertain
        if not math.isfinite(value) or value < 0:
            self._trip_once("invalid reservation", "invalid live worst-case loss reservation")
            self.kill.check()
        return value

    def _check_loss_budget(self, additional: float = 0.0) -> None:
        self._check_accounting_integrity()
        risk = self._risk_budget_used() + additional
        self.sender.limits.check_loss_budget(risk, self.kill)

    def _check_accounting_integrity(self) -> None:
        error = getattr(self.ledger, "integrity_error", None)
        if error:
            self._trip_once(("integrity", str(error)), f"durable fill execution evidence conflicts with the ledger: {error}")
            self.kill.check()
        halt = getattr(self.ledger, "persistence_halt", None)
        if halt:
            self._trip_once(("persistence", str(halt)), f"the live ledger could not make booked accounting durable: {halt}")
            self.kill.check()

    def own_risk_shares(self, token: str, condition: str) -> float:
        """Booked plus uncertain own-side inventory; never hedge credit."""
        with self._risk_lock:
            reservations = [dict(a) for a in self._risk_orders.values()
                            if a["token"] == str(token) and a["condition"] == str(condition)]
        with self.ledger._lock:
            # positions are keyed by their token: one lookup, not a walk of every position held
            pos = self.ledger.positions.get(str(token))
            positions = ([pos] if pos is not None and str(pos.token_id) == str(token)
                         and str(pos.condition_id) == str(condition) and not pos.settled else [])
            held = sum(float(p.shares) for p in positions)
            booked = collections.defaultdict(float)
            for pos in positions:
                for lot in pos.lots:
                    if str(lot.side).upper() == "BUY" and getattr(lot, "order_id", None):
                        booked[lot.order_id] += float(lot.open_shares)
        return held + sum(max(0.0, a["shares"] - booked.get(a.get("order_id"), 0.0)) for a in reservations)

    def _resolve_risk(self, a: dict, *, filled: bool, spent: float | None = None) -> None:
        key = a.get("client_order_id")
        with self._risk_lock:
            self._risk_orders.pop(key, None)
        session = self.session
        if session is not None:
            session.resolve(key, "CONFIRMED" if filled else NO_MATCH, self.sender.limits, self.kill,
                            spent=spent if filled else 0.0)

    def reconcile_once(self) -> None:
        """Every accepted order must turn up CONFIRMED in the ledger; no fill
        may stay unexplained past a short grace; the session's realized loss
        feeds the loss cap. Runs on a worker thread."""
        self._check_accounting_integrity()
        now = time.time()
        unexplained = self._unexplained_now() - self.baseline_unexplained
        for tid in [t for t in self._unexplained_since if t not in unexplained]:
            self._unexplained_since.pop(tid, None)
        for tid in unexplained:
            first = self._unexplained_since.setdefault(tid, now)
            if now - first > self.unexplained_grace_s:
                self._trip_once(("unexplained", tid), f"a confirmed fill on this wallet that no order of ours "
                                                      f"explains (trade ...{str(tid)[-10:]}): reconcile by hand")
        for oid, a in list(self.awaiting.items()):
            if a.get("authorized") is False:
                if now - a["wall"] > self.confirm_within_s:
                    self._trip_once(("order-uncertain", str(a.get("client_order_id") or oid)),
                                    f"order {short_id(oid)} authorization incomplete: reconcile")
                continue
            if a.get("order_type") == "FAK":
                self._reconcile_fak(oid, a, now)
                continue
            if self._order_booked(oid, a.get("token")):
                complete = not a.get("trade_ids") or self._trades_booked(a["trade_ids"])
                if complete:
                    self._resolve_risk(a, filled=True)
                    self.awaiting.pop(oid, None)
                    self.n["confirmed in ledger"] += 1
                    continue
            age = now - a["wall"]
            if (self.order_status is not None and age > 30 and not a.get("venue_matched")
                    and now - a.get("checked_at", 0.0) > 15):
                a["checked_at"] = now
                try:
                    order = self.order_status(oid) or {}
                    status = str(order.get("status") or "").lower()
                except Exception:  # noqa: BLE001 - counted; the deadline below still applies
                    status, order = "", {}
                    self.n["order status errors"] += 1
                a["venue_status"] = status
                if status in ("canceled", "cancelled", "unmatched", "killed", "expired"):
                    # Cancellation alone cannot release collateral: require
                    # this exact order's authoritative zero execution evidence.
                    try:
                        zero = (str(order.get("id")) == str(oid) and str(order.get("asset_id")) == a["token"]
                                and str(order.get("market")) == a["condition"] and order.get("side") == "BUY"
                                and Decimal(str(order.get("size_matched"))) == 0
                                and order.get("associate_trades") == [])
                    except (ArithmeticError, TypeError, ValueError):
                        zero = False
                    if not zero:
                        if age > self.confirm_within_s:
                            self._trip_once(("order-uncertain", str(a.get("client_order_id") or oid)),
                                            f"FOK order {short_id(oid)} canceled without "
                                            f"complete zero-fill proof: reconcile")
                        continue
                    self.awaiting.pop(oid, None)
                    self.sender.limits.resolve_accepted(a["amount"], zero_fill=True)
                    self._resolve_risk(a, filled=False)
                    self.n["accepted but never filled"] += 1
                    continue
                a["venue_matched"] = status == "matched"
            if age > self.confirm_within_s:
                self._trip_once(("order-uncertain", str(a.get("client_order_id") or oid)),
                                f"order {short_id(oid)} ({a['kind']}, venue status {a.get('venue_status') or 'unknown'}) "
                                f"not confirmed in the ledger after {self.confirm_within_s:.0f}s: reconcile")
        self.session_loss = max(0.0, -self._session_realized())

    def _fak_status(self, oid, a, now) -> None:
        """Ask the venue for an accepted FAK's final size and trade ids (its
        status record). Only complete, identity-matched evidence is kept."""
        a["checked_at"] = now
        try:
            order = self.order_status(oid) or {}
        except Exception:
            order = {}
            self.n["order status errors"] += 1
        if not order:
            return
        if (str(order.get("id")) != str(oid) or str(order.get("asset_id")) != a["token"]
                or str(order.get("market")) != a["condition"] or order.get("side") != "BUY"):
            self._trip_fak(a, f"FAK order {short_id(oid)} status identity mismatch: reconcile")
            return
        status = str(order.get("status") or "").lower()
        a["venue_status"] = status
        if status in ("matched", "canceled", "cancelled", "unmatched", "killed", "expired"):
            try:
                size = Decimal(str(order["size_matched"]))
                tids = order.get("associate_trades")
                if not size.is_finite() or size < 0 or not isinstance(tids, list):
                    raise ValueError("missing final trade evidence")
                if any(not isinstance(t, str) or not t for t in tids):
                    raise ValueError("invalid trade ids")
                if size == 0 and status == "matched":
                    raise ValueError("matched is not evidence of zero execution")
                a["trade_ids"] = set(tids)
                a["final_size"] = float(size)
                a.setdefault("final_mono", time.monotonic())   # when the final size became known
            except (KeyError, ArithmeticError, TypeError, ValueError):
                pass                          # incomplete evidence keeps the full reservation

    def _reconcile_fak(self, oid, a, now):
        """A canceled FAK can still have fills. Resolve all trades before
        releasing unused spend; never treat cancellation alone as zero."""
        age = now - a["wall"]
        if (not a.get("trade_ids") and a.get("final_size") is None and self.order_status is not None and age > 1
                and now - a.get("checked_at", 0) > self.reconcile_every_s):
            self._fak_status(oid, a, now)
        fill = self.fill_for(oid)
        if fill is not None:
            if fill["shares"] == 0:
                self.sender.limits.resolve_accepted(a["amount"], zero_fill=True)
                self._resolve_risk(a, filled=False)
                self.awaiting.pop(oid, None)
                self.n["accepted but never filled"] += 1
                return
            booked = self._trades_booked(fill["trade_ids"])
            if fill["statuses"] == ["CONFIRMED"] and booked:
                # Only the proven unused notional is returned to the session cap.
                unused = max(0.0, a["amount"] - fill["cost"])
                self.sender.limits.resolve_accepted(unused)
                self._resolve_risk(a, filled=True, spent=fill["cost"])
                self.awaiting.pop(oid, None)
                self.n["confirmed in ledger"] += 1
                return
        if age > self.confirm_within_s and not a.get("halted_for"):
            # An order whose own evidence already halted trading is not timed out
            # a second time: one halt, with its real reason (2026-10-14).
            self._trip_fak(a, f"FAK order {short_id(oid)} not completely confirmed after {self.confirm_within_s:.0f}s: reconcile")

    def fill_for(self, order_id) -> dict | None:
        """Our fills of one order as the private channel reported them
        (MATCHED is provisional; only CONFIRMED is booked by the ledger).
        For FAK, require the complete trade set and validate its quantities."""
        store = self.fills
        if store is None or not order_id:
            return None
        for_order = getattr(store, "fills_for_order", None)
        if callable(for_order):
            rows = for_order(order_id)               # copies of this order's fills only
        else:
            lock = getattr(store, "_lock", None)
            if lock is None:
                rows = [copy.copy(f) for f in store.fills.values()]
            else:
                with lock:
                    rows = [copy.copy(f) for f in store.fills.values()]
            rows = [f for f in rows if str(getattr(f, "order_id", "") or "") == str(order_id)]
        a = self.fak_orders.get(order_id)
        if a is not None:
            return self._fak_fill(order_id, a, rows)
        mine = [(float(f.size or 0), float(f.price or 0), str(f.status or ""), getattr(f, "paid", None))
                for f in rows if str(getattr(f, "order_id", "") or "") == str(order_id)]
        mine = [m for m in mine if m[0] > 0 and m[1] > 0
                and m[2] in ("MATCHED", "MATCHED_NOT_BROADCASTED", "MINED", "CONFIRMED")]
        if not mine:
            return None
        shares = sum(m[0] for m in mine)
        # the maker legs' real cost when known (the headline price is not their average)
        cost = sum(m[3] if m[3] is not None else m[0] * m[1] for m in mine)
        return {"shares": round(shares, 6), "price": round(cost / shares, 6), "cost": round(cost, 6),
                "trades": len(mine), "statuses": sorted({m[2] for m in mine}), "source": "user channel"}

    def _fak_fill(self, oid, a, rows):
        from paper_trade import MarketRules, curve_fee
        expected = a.get("trade_ids") or set()
        if a.get("final_size") == 0:
            if rows or expected or a["kind"] == MATCHED:
                self._trip_fak(a, f"FAK order {short_id(oid)} has contradictory zero-fill evidence: reconcile")
                return None
            a["quantity_resolved"] = True
            return {"shares": 0.0, "cost": 0.0, "fee": 0.0, "price": None,
                    "source": "terminal order status", "statuses": [], "trade_ids": [],
                    "seen_mono": a.get("final_mono")}
        actual_ids = {str(f.trade_id) for f in rows}
        if expected and actual_ids - expected:
            self._trip_fak(a, f"FAK order {short_id(oid)} has unexpected trades: reconcile")
            return None
        if not rows or (expected and actual_ids != expected):
            return None
        if not expected and a.get("final_size") is None:
            return None                            # one private trade need not be the entire fill
        shares = cost = fee = Decimal(0)
        rules = MarketRules(a["condition"], Decimal(str(a["fee_rate"])), a["fee_exponent"])
        for f in rows:
            status = str(f.status or "").upper()
            if status in ("FAILED", "CONFLICT"):
                self._trip_fak(a, f"FAK order {short_id(oid)} has a {status} trade: reconcile")
                return None
            if status not in ("MATCHED", "MATCHED_NOT_BROADCASTED", "MINED", "CONFIRMED"):
                return None
            try:
                qty, px = Decimal(str(f.size)), Decimal(str(f.price))
                valid = (qty.is_finite() and px.is_finite() and qty > 0 and 0 < px <= Decimal(str(a["cap"]))
                         and str(f.asset_id) == a["token"] and str(f.market) == a["condition"] and f.side == "BUY")
                if not valid:
                    raise ValueError("identity, price or quantity")
            except (ArithmeticError, TypeError, ValueError):
                self._trip_fak(a, f"FAK order {short_id(oid)} has an invalid fill: reconcile")
                return None
            shares += qty
            paid = getattr(f, "paid", None)
            trade_cost = Decimal(str(paid)) if paid is not None else qty * px
            legs = getattr(f, "maker_legs", None)
            if legs is not None:
                try:
                    parsed_legs = [(Decimal(str(q)), Decimal(str(price))) for q, price in legs]
                    if (not parsed_legs or any(not q.is_finite() or not price.is_finite() or q <= 0
                                              or not 0 < price <= Decimal(str(a["cap"]))
                                              for q, price in parsed_legs)
                            or abs(sum(q for q, _ in parsed_legs) - qty) > Decimal("0.000001")
                            or abs(sum(q * price for q, price in parsed_legs) - trade_cost) > Decimal("0.000001")):
                        raise ValueError("invalid maker execution legs")
                except (ArithmeticError, TypeError, ValueError):
                    self._trip_fak(a, f"FAK order {short_id(oid)} has invalid maker execution legs: reconcile")
                    return None
                fee += sum(curve_fee(q, price, rules) for q, price in parsed_legs)
            else:
                fee += curve_fee(qty, trade_cost / qty, rules)
            cost += trade_cost
        if cost > Decimal(str(a["amount"])) + Decimal("0.000001"):
            self._trip_fak(a, f"FAK order {short_id(oid)} filled above its budget: reconcile")
            return None
        if a.get("size") is not None and shares > Decimal(str(a["size"])) + Decimal("0.000001"):
            self._trip_fak(a, f"FAK order {short_id(oid)} filled above its signed size: reconcile")
            return None
        if a.get("final_size") is not None and abs(float(shares) - a["final_size"]) > 1e-6:
            return None                            # not all final fills have arrived yet
        a["quantity_resolved"] = True
        # When the full quantity became knowable (time.monotonic, the ticks'
        # clock): the last of the order's trades arriving on the private
        # channel - or, when the reply named no trade ids, the order status
        # that fixed the final size, if that came later.
        seen = max(float(getattr(f, "first_seen", 0.0) or 0.0) for f in rows)
        if not expected and a.get("final_mono") is not None:
            seen = max(seen, float(a["final_mono"]))
        return {"shares": float(shares), "price": float(cost / shares), "cost": float(cost), "fee": float(fee),
                "partial": cost < Decimal(str(a["amount"])) - Decimal("0.01"),
                "trades": len(rows), "trade_ids": sorted(actual_ids),
                "statuses": sorted({str(f.status).upper() for f in rows}), "source": "user channel",
                "seen_mono": seen or None}

    def blocks_pending_fak(self, oid, *, client_order_id=None) -> bool:
        """Pause once for an unknown quantity, keeping its evidence and risk.

        The decision layer supplies the original client id for an ambiguous
        order whose venue authorization is still being looked up.
        """
        a = self.fak_orders.get(oid)
        if a is not None:
            if a.get("quantity_resolved") or self.fill_for(oid) is not None:
                return False
            client_order_id = a.get("client_order_id") or client_order_id
        identity = str(client_order_id or oid)
        self._trip_once(("order-uncertain", identity),
                        f"FAK order {short_id(oid)} is still awaiting complete fill quantities")
        return self.kill.tripped

    # ------------------------------------------------------------ one order (event loop)
    def prepare(self, plan: dict, decided_pc: int | None = None) -> Prepared:
        """Size and check the order exactly as the paper broker does (same
        functions, on the book the decision used), then sign it. Raises
        Refused (this order not sent) or Halted (new orders paused). No I/O."""
        from paper_trade import (MIN_MARKETABLE_BUY_USD, PaperRejected, estimate_fok, estimate_fak, estimate_fak_shares,
                                 size_to_venue_minimum, snapshot_from_book_view)
        self.kill.check()
        order_type = str(plan.get("order_type", "FOK")).upper()
        if order_type not in ("FOK", "FAK"):
            raise Refused(f"unknown order type {order_type!r}")
        token, cond, end = plan["token"], plan["cond"], float(plan["end"])
        for oid, a in list(self.fak_orders.items()):
            # Its incomplete quantity pauses once, while its reservation and
            # reconciliation remain even after that pause ends.
            if a.get("token") != token:
                continue
            if self.blocks_pending_fak(oid, client_order_id=a.get("client_order_id")):
                self.kill.check()
        self.sender.guard.check()                       # Refused (journalled) while the venue guard has orders paused
        side, amount, up, down = plan["send"][:4]
        if self.balance_at is None or time.monotonic() - self.balance_at > self.balance_max_age_s:
            raise Refused("collateral balance is stale")
        if self.user_ready is not None and not self.user_ready(cond):
            raise Refused("the private fill channel is not subscribed to this market yet")
        rules = self.shim.cached_rules(cond)
        if rules is None:
            raise Refused("market rules not loaded for this round yet")
        order_rules = self.sender.rules.get(cond, str(up), str(down))
        if plan.get("fair_fee_rate") is not None:
            if (Decimal(str(plan["fair_fee_rate"])) != rules.fee_rate
                    or int(plan.get("fair_fee_exponent", 1)) != rules.fee_exponent
                    or Decimal(str(order_rules["fee_rate"])) != rules.fee_rate
                    or int(order_rules["fee_exponent"]) != rules.fee_exponent):
                raise Refused("fee rules changed after the fair-value decision")
        if order_rules.get("taker_delay") and end - time.time() <= self.assumed_delay_s:
            raise Refused(f"inside the venue's assumed {self.assumed_delay_s:g}s matching-delay window")
        # As paper: an order's cap may tighten the account ceiling, never loosen it.
        cap = min(float(plan["cap"]), float(self.shim.max_buy_price))
        try:
            book = snapshot_from_book_view(lean_view(plan["view"], cap), expected_token=token)
            if (rules.tick_size != order_rules["tick"] or rules.min_order_size != order_rules["minimum"]
                    or (book.tick_size is not None and book.tick_size != rules.tick_size)):
                self.sender.rules.invalidate(cond)
                self.rules_ready = None
                self.n["rules refresh requested"] += 1
                raise PaperRejected("market rules changed; background refresh requested")
            if not book.asks or book.best_bid is None:
                raise PaperRejected("one-sided book")
            if book.asks[0][0] - book.best_bid > Decimal(str(self.shim.max_spread)):
                raise PaperRejected("spread exceeds the limit")
            price = (Decimal(str(cap)) / rules.tick_size).to_integral_value(rounding=ROUND_FLOOR) * rules.tick_size
            size = None
            if plan.get("shares") is not None and order_type == "FAK":
                # F1: exactly these whole shares at the limit, never more
                size = Decimal(str(plan["shares"]))
                minimums = [v for v in (book.min_order_size, rules.min_order_size) if v is not None]
                if minimums and size < max(minimums):
                    raise PaperRejected(f"order size {size} shares is below venue minimum {max(minimums)}")
                spend = size * price
                if spend != spend.quantize(Decimal("0.01")):
                    raise PaperRejected(f"{size} shares at {price} is not a whole-cent amount")
                if spend < MIN_MARKETABLE_BUY_USD:
                    raise PaperRejected(f"invalid amount for a marketable BUY order (${spend:.2f}), min size: $1")
                quote = estimate_fak_shares(book, size, float(price), rules, min_price=self.shim.min_buy_price)
            else:
                spend = size_to_venue_minimum(amount, book, rules, cap)
                # The SDK signs BUY size at the limit, not the visible average.
                # Reserve enough for the venue minimum at that exact limit even
                # when slippage is wider than the generic sizing headroom.
                minimum_spend = (rules.min_order_size * price).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
                spend = max(spend, minimum_spend)
                if spend < MIN_MARKETABLE_BUY_USD:
                    raise PaperRejected(f"invalid amount for a marketable BUY order (${spend:.2f}), min size: $1")
                quote_fn = estimate_fak if order_type == "FAK" else estimate_fok
                quote = quote_fn(book, spend, float(price), rules, min_price=self.shim.min_buy_price)
        except PaperRejected as exc:
            raise Refused(f"pre-send check: {exc}") from exc
        except Exception as exc:  # noqa: BLE001 - e.g. a malformed book: do not send
            raise Refused(f"pre-send check failed: {type(exc).__name__}: {exc}") from exc
        spend, fee = float(spend), float(quote.fee)
        # More liquidity can arrive after our snapshot. Reserve for the whole
        # signed spend and an upper fee bound, not just the visible partial.
        reserve_fee = max(fee, spend * float(rules.fee_rate))
        free = (self.balance or 0.0) - self.reserved
        if spend + reserve_fee > free + 1e-9:
            raise Refused(f"collateral ${free:.2f} is below the order ${spend + reserve_fee:.2f} incl. fee")
        self._check_loss_budget(spend + reserve_fee)
        p = self.sender.prepare(side=side, token=token, up=str(up), down=str(down), condition=cond, end=end,
                                amount=spend, cap=cap, decided_pc=decided_pc, order_type=order_type,
                                shares=float(size) if size is not None else None)
        p.reserve = spend + reserve_fee
        p.estimate = {"shares": float(quote.shares), "price": float(quote.average_price), "fee": fee}
        def send_guard():
            self._check_loss_budget()
            if self.balance_at is None or time.monotonic() - self.balance_at > self.balance_max_age_s:
                raise Refused("collateral balance became stale before dispatch")
            if self.balance is None or self.reserved > self.balance + 1e-9:
                raise Refused("collateral fell below outstanding reservations before dispatch")
            if self.user_ready is not None and not self.user_ready(cond):
                raise Refused("private fill coverage lost before dispatch")
            left = end - time.time()
            cutoff = max(float(self.shim.min_seconds_to_expiry),
                         self.assumed_delay_s if order_rules.get("taker_delay") else 0.0)
            if left <= cutoff:
                raise Refused("inside the matching-delay/expiry window before dispatch")
            if self.hub is not None:
                key = (str(getattr(self.hub, "condition_id", "")),
                       str(getattr(self.hub, "up_token", "")), str(getattr(self.hub, "down_token", "")))
                if key != (str(cond), str(up), str(down)):
                    raise Refused("market changed before dispatch")
                latest = self.hub.book.view(token)
                from feeds.health import LIVE
                age = latest.book_age_ms()
                max_age = getattr(getattr(self, "_sniper_settings", None), "stale_ms", 2000.0)
                if latest.status != LIVE or age is None or not math.isfinite(age) or age < 0 or age > max_age:
                    raise Refused("book became stale before dispatch")
        p.send_guard = send_guard
        with self._risk_lock:
            self._risk_orders[p.client_order_id] = {"condition": str(cond), "token": str(token),
                                                   "reserve": p.reserve, "order_id": None,
                                                   # share-sized: exactly what it can add (F1)
                                                   "shares": (float(size) if size is not None else
                                                              max(float(quote.shares), spend / float(book.asks[0][0])))}
        self.reserved += p.reserve
        self._balance_revision += 1
        return p

    def start_send(self, p: Prepared):
        """The order's bytes onto the warm connection now (see LeanSender.start)."""
        return self.sender.start(p)

    async def send(self, p: Prepared, started=None) -> dict:
        return await _once(p, "_execution_outcome", self._send_once, p, started)

    async def _send_once(self, p: Prepared, started=None) -> dict:
        reserve = getattr(p, "reserve", p.amount)
        try:
            row = await self.sender.send(p, started)
        except BaseException:
            # Cancellation after a write is still an unknown order. Retain its
            # cash bound even though the waiter cannot return its outcome.
            if self.balance is not None:
                self.balance -= reserve
            self._trip_once(("order-uncertain", str(p.client_order_id)),
                            "order waiter interrupted: reconcile before trading")
            raise
        finally:
            self.reserved = max(0.0, self.reserved - reserve)
            self._balance_revision += 1
        kind = row["kind"]
        if kind in (NO_MATCH, REJECTED, NOT_SENT):
            with self._risk_lock:
                self._risk_orders.pop(p.client_order_id, None)
        if kind in (MATCHED, PENDING, AMBIGUOUS) and self.balance is not None:
            self.balance -= reserve                         # spent, or may be; the next read corrects it
        if kind in (MATCHED, PENDING) and row.get("order_id"):
            if kind == PENDING and row.get("error") != "DELAYED_PENDING_OUTCOME":
                # As the audited path: only a documented delayed placement may
                # be followed; any other incomplete success blocks this token.
                self.pt._mark_ambiguous(p.condition, p.end, p.token)
                self.sender._ambiguous_pauses = getattr(self.sender, "_ambiguous_pauses", {})
                self.sender._ambiguous_pauses[(str(p.condition), str(p.token))] = p.client_order_id
            await asyncio.to_thread(self._authorize, p, row)
        elif kind in (MATCHED, PENDING):
            self._trip_once(("order-uncertain", str(p.client_order_id)),
                            f"accepted order with no order id ({row.get('venue_status')}): reconcile")
        if kind == AMBIGUOUS and not row.get("auto_reconcile"):
            self._trip_once(("order-uncertain", str(p.client_order_id)),
                            f"order outcome unknown ({row.get('error')}): check the venue and the ledger by hand")
        if kind in (MATCHED, PENDING) and p.order_type == "FAK":
            # Do not infer response amount units. Private trade records give
            # decimal quantities and the complete trade set.
            row["fill"] = self.fill_for(row.get("order_id"))
            a = self.fak_orders.get(row.get("order_id"))
            if (row["fill"] is None and a is not None and not a.get("trade_ids")
                    and self.order_status is not None):
                # M6: the reply named no trades - ask the venue for the order's
                # final size now, not at the next reconcile (this side waits on it).
                self._track(asyncio.get_running_loop().create_task(self._fast_fak(row["order_id"])))
        elif kind == MATCHED:
            row["fill"] = self._fill_from_response(row) or self.fill_for(row.get("order_id"))
        self.n[kind] += 1
        return row

    def _track(self, task) -> None:
        self._order_tasks.add(task)
        task.add_done_callback(self._order_tasks.discard)

    async def _fast_fak(self, oid) -> None:
        """M6: resolve an accepted FAK's quantity within about a second."""
        for delay in self.fast_fak_delays_s:
            await asyncio.sleep(delay)
            a = self.fak_orders.get(oid)
            if a is None or a.get("quantity_resolved") or oid not in self.awaiting:
                return
            if a.get("trade_ids") or a.get("final_size") is not None:
                break                               # the evidence is in; the trades complete it
            await asyncio.to_thread(self._fak_status, oid, a, time.time())
        self.n["fast FAK lookups"] += 1
        self._sync_fak()

    # ------------------------------------------------------------ unknown outcomes (audit M7)
    def _schedule_ambiguity(self, p, row) -> None:
        if self.order_status is None:
            self._trip_once(("order-uncertain", str(p.client_order_id)),
                            f"order {p.client_order_id} outcome unknown ({row.get('error')}): reconcile before trading")
            return
        self._track(asyncio.get_running_loop().create_task(self._resolve_ambiguity(p, row)))

    def _ambiguity_evidence(self, order, p):
        """(kind, size, trade ids) from the venue's record of this order, or
        None while that record does not settle it (absent, not terminal,
        incomplete). An identity mismatch is not evidence: it halts."""
        if not isinstance(order, dict) or not order:
            return None
        if (str(order.get("id")) != str(p.order_hash) or str(order.get("asset_id")) != str(p.token)
                or str(order.get("market")) != str(p.condition) or order.get("side") != "BUY"):
            raise RuntimeError("the venue's record of this order hash does not match it")
        status = str(order.get("status") or "").lower()
        if status not in ("matched", "canceled", "cancelled", "unmatched", "killed", "expired"):
            return None
        try:
            size = Decimal(str(order["size_matched"]))
            tids = order.get("associate_trades")
            if not size.is_finite() or size < 0 or not isinstance(tids, list):
                return None
            if any(not isinstance(t, str) or not t for t in tids):
                return None
        except (KeyError, ArithmeticError, TypeError, ValueError):
            return None
        if size == 0:
            if tids or status == "matched":
                return None                         # not proof of zero execution
            return NO_MATCH, 0.0, []
        return MATCHED, float(size), list(tids)

    async def _resolve_ambiguity(self, p, row) -> None:
        """An order whose outcome is unknown: the venue's record of its hash
        decides - matched (authorized, its fills then count as any other's)
        or proven zero execution (its reservation released). Nothing is ever
        sent again. No record in time: the kill switch, as before."""
        evidence = None
        why = "the venue has no terminal record of it"
        for delay in self.ambiguity_delays_s:
            await asyncio.sleep(delay)
            try:
                order = await asyncio.to_thread(self.order_status, p.order_hash)
                evidence = self._ambiguity_evidence(order, p)
            except RuntimeError as exc:
                why = str(exc)
                break
            except Exception:  # noqa: BLE001 - a lookup that failed is not evidence
                self.n["order status errors"] += 1
                continue
            if evidence is not None:
                break
        limits = self.sender.limits
        if evidence is None:
            self._trip_once(("order-uncertain", str(p.client_order_id)),
                            f"order {p.client_order_id} outcome unknown ({row.get('error')}); {why}: reconcile by hand")
            return
        kind, size, tids = evidence
        oid = str(p.order_hash)
        limits.record_outcome(kind, p.amount)
        if self.session is not None:
            self.session.outcome(p, {"kind": kind, "order_id": oid}, limits, self.kill)
        if kind == MATCHED:
            accepted = dict(row, kind=MATCHED, order_id=oid, venue_status="matched",
                            response={"orderID": oid, "status": "matched", "tradeIDs": tids})
            await asyncio.to_thread(self._authorize, p, accepted)
            a = self.awaiting.get(oid)
            if a is not None:
                a["trade_ids"] = set(tids)
                a["final_size"] = size
                a.setdefault("final_mono", time.monotonic())
        else:
            with self._risk_lock:
                self._risk_orders.pop(p.client_order_id, None)
            if self.session is not None:
                self.session.resolve(p.client_order_id, NO_MATCH, limits, self.kill, spent=0.0)
            # the sniper holds this order as a pending FAK under its hash: a
            # proven zero fill releases its round slot (fill_for -> 0 shares)
            rules = getattr(p, "rules", None) or {}
            self.fak_orders[oid] = {"wall": time.time(), "condition": p.condition, "token": p.token,
                                    "amount": p.amount, "kind": NO_MATCH, "client_order_id": p.client_order_id,
                                    "order_type": "FAK", "cap": p.price, "trade_ids": set(), "final_size": 0.0,
                                    "fee_rate": float(rules.get("fee_rate") or 0),
                                    "fee_exponent": int(rules.get("fee_exponent") or 1),
                                    "final_mono": time.monotonic()}
        self.pt._resolve_ambiguous(p.condition, p.token)
        self.sender.unresolved.pop(p.client_order_id, None)
        self._sync_fak()
        self.n[f"unknown outcome resolved {kind}"] += 1
        journal = getattr(self, "_journal", None)
        if journal is not None:
            journal({"wall": time.time(), "result": "outcome_reconciled", "client_order_id": p.client_order_id,
                     "order_id": oid, "kind": kind, "shares": size, "trades": len(tids),
                     "was": row.get("error")})
        self.on_event("sniper", f"LIVE order {p.client_order_id[-8:]} outcome reconciled with the venue: "
                                f"{kind} ({size:g} shares)", "warn")

    # ------------------------------------------------------------ a previous run's orders (restart halt)
    def _previous_run_by_trades(self, intent: dict) -> tuple | None:
        """CONFIRMED when the venue has no record of the order but its accepted
        reply proves the complete execution, and the ledger booked exactly that
        under the order's authorization (the ledger books CONFIRMED trades
        only): the reply's trade ids, every one booked and no other trade; or,
        when it named none, the reply's own totals (USDC spent, shares
        received) equal to the order's booked lots. None otherwise: a missing
        record is never evidence of zero execution."""
        tids, totals = intent.get("trade_ids"), intent.get("reply_totals")
        h = str(intent.get("order_hash") or intent.get("order_id") or "")
        if (not tids and not totals) or not h or intent.get("kind") not in (MATCHED, PENDING):
            return None
        led = self.ledger
        with led._lock:
            if h not in led.authorized_orders:
                return None
            lots = [lot for pos in led.positions.values() for lot in pos.lots
                    if getattr(lot, "order_id", None) == h and str(lot.side).upper() == "BUY"]
            if not lots:
                return None
            shares = sum(float(lot.shares) for lot in lots)
            spent = sum(float(lot.shares) * float(lot.price) for lot in lots)
            if tids:
                if not set(tids) <= set(led.seen) or {str(lot.trade_id) for lot in lots} != set(tids):
                    return None
            elif abs(shares - float(totals[1])) > 1e-6 or abs(spent - float(totals[0])) > 1e-4:
                return None
            if not math.isfinite(spent) or spent > float(intent.get("amount") or 0.0) + 1e-6:
                return None
        return "CONFIRMED", spent

    def _previous_run_verdict(self, intent: dict, order) -> tuple | None:
        """(status, spent) settling one of a previous run's unresolved send
        intents from the venue's record of its own order hash, or None while
        nothing proves it: NO_MATCH = terminal with zero execution; CONFIRMED =
        matched, its authorization in the ledger and every one of its trades
        booked there. An identity mismatch raises (it is never evidence)."""
        h = str(intent["order_hash"])
        if not isinstance(order, dict) or not order:
            # The CLOB answered null for the order's id (seen live for an executed
            # FAK, 2026-10-15): settled only by the reply's own trade list - every
            # one booked under the order's authorization, and no other trade.
            return self._previous_run_by_trades(intent)
        if (str(order.get("id")) != h or str(order.get("asset_id")) != str(intent.get("token"))
                or str(order.get("market")) != str(intent.get("condition")) or order.get("side") != "BUY"):
            raise RuntimeError(f"the venue's record of order hash ...{h[-10:]} does not match its intent")
        p = types.SimpleNamespace(order_hash=h, token=intent.get("token"), condition=intent.get("condition"))
        evidence = self._ambiguity_evidence(order, p)
        if evidence is None:
            return None
        kind, size, tids = evidence
        if kind == NO_MATCH:
            return NO_MATCH, 0.0
        led = self.ledger
        with led._lock:
            if h not in led.authorized_orders or not set(tids) <= set(led.seen):
                return None                         # matched, but not (yet) booked under its authorization
            lots = [lot for pos in led.positions.values() for lot in pos.lots
                    if getattr(lot, "order_id", None) == h and str(lot.side).upper() == "BUY"]
            if (abs(sum(float(lot.shares) for lot in lots) - size) > 1e-6
                    or (tids and {str(lot.trade_id) for lot in lots} != set(tids))):
                return None
            spent = sum(float(lot.shares) * float(lot.price) for lot in lots)
            if not math.isfinite(spent) or spent > float(intent.get("amount") or 0.0) + 1e-6:
                return None
        return "CONFIRMED", spent

    async def _reconcile_previous_run(self) -> None:
        """Every send intent a previous run
        left unresolved is looked up on the venue by its own order hash - the
        same evidence rules as an unknown outcome in this run (M7). Proven zero
        execution, or a match fully booked under its authorization, resolves
        it; nothing is ever sent again. Reconciliation continues independently
        of the five-minute start-up pause. New intents from this run are never
        mistaken for prior-run intents after the bot resumes."""
        session = self.session
        if session is None:
            return
        previous_keys = getattr(self, "_previous_run_keys", None)
        if previous_keys is None:
            previous_keys = self._previous_run_keys = {key for key, _intent in session.unresolved()}
        if not previous_keys:
            return
        limits, notes = self.sender.limits, {}
        for delay in self.previous_run_delays_s:
            await asyncio.sleep(delay)
            for key, intent in session.unresolved():
                if key not in previous_keys:
                    continue
                if not intent.get("order_hash") or self.order_status is None:
                    notes[key] = "no order hash recorded (sent by an older version): reconcile by hand"
                    continue
                try:
                    order = await asyncio.to_thread(self.order_status, intent["order_hash"])
                    verdict = self._previous_run_verdict(intent, order)
                except RuntimeError as exc:
                    notes[key] = str(exc)
                    continue
                except Exception as exc:  # noqa: BLE001 - a lookup that failed is not evidence
                    self.n["order status errors"] += 1
                    notes[key] = f"venue lookup failed ({type(exc).__name__})"
                    continue
                if verdict is None:
                    notes[key] = "no terminal venue record, or its trades are not in the ledger yet"
                    continue
                status, spent = verdict
                accepted = intent.get("kind") in (MATCHED, PENDING)
                amount = float(intent.get("amount") or 0.0)
                if status == NO_MATCH:
                    if accepted:
                        limits.resolve_accepted(amount, zero_fill=True)
                    else:
                        limits.record_outcome(NO_MATCH, amount)
                else:
                    if not accepted:
                        limits.record_outcome(MATCHED, amount)
                    limits.resolve_accepted(max(0.0, amount - spent))
                session.resolve(key, status, limits, self.kill, spent=spent)
                with self._risk_lock:
                    self._risk_orders.pop(key, None)
                notes.pop(key, None)
                self.n[f"previous run order resolved {status}"] += 1
                self.on_event("sniper", f"LIVE previous run's order {key[-8:]} reconciled with the venue: "
                                        f"{status}" + (f" (${spent:.2f})" if spent else ""), "warn")
            if not any(key in previous_keys for key, _intent in session.unresolved()):
                for reason in list(self.kill.reasons):
                    if str(reason).startswith(session.PREVIOUS_RUN):
                        self.kill.clear(reason)
                session.persist(limits, self.kill)
                self.on_event("sniper", "LIVE previous run's orders all reconciled with the venue: start-up "
                                        "halt lifted" + ("" if not self.kill.reasons else
                                                         f" (still halted: {'; '.join(self.kill.reasons)})"),
                              "good" if not self.kill.reasons else "warn")
                return
        for key, why in notes.items():
            self.on_event("sniper", f"LIVE previous run's order {key[-8:]} still unresolved: {why}", "bad")

    @staticmethod
    def _fill_from_response(row: dict) -> dict | None:
        """Shares and price from the reply's base-unit amounts, when present
        (a BUY gives makingAmount USDC for takingAmount shares). Matched FOK
        replies often omit them; then the private channel tells."""
        resp = row.get("response") or {}
        try:
            making, taking = Decimal(str(resp["makingAmount"])), Decimal(str(resp["takingAmount"]))
        except (KeyError, ArithmeticError, TypeError, ValueError):
            return None
        if not (making.is_finite() and taking.is_finite() and making > 0 and taking > 0
                and making == making.to_integral_value() and taking == taking.to_integral_value()):
            return None
        shares, cost = float(taking) / 1e6, float(making) / 1e6
        return {"shares": round(shares, 6), "price": round(cost / shares, 6), "cost": round(cost, 6),
                "source": "order reply"}

    def _authorize(self, p: Prepared, row: dict) -> None:
        """Worker thread: hand the receipt to the live ledger's authorization
        (the same observer the audited live path uses). Until this lands, the
        order's fills are held back as unauthorized - then counted."""
        rules, oid = p.rules, row["order_id"]
        est = getattr(p, "estimate", None) or {}
        receipt = self.pt._build_receipt(
            row.get("response") or {}, oid, row.get("venue_status"), condition_id=p.condition, token_id=p.token,
            window_end=p.end, amount=p.amount, estimated_fee=float(est.get("fee") or 0.0),
            fee_rate=float(rules.get("fee_rate") or 0), fee_exponent=int(rules.get("fee_exponent") or 1),
            validation="MATCHED_WITH_TRADE_EVIDENCE" if row["kind"] == MATCHED else (row.get("error") or "PENDING"))
        resp = row.get("response") or {}
        a = {"wall": time.time(), "condition": p.condition, "token": p.token, "amount": p.amount,
             "kind": row["kind"], "client_order_id": p.client_order_id, "authorized": False,
             "trade_ids": set(resp.get("tradeIDs") or resp.get("trade_ids") or [])}
        self.awaiting[oid] = a
        with self._risk_lock:
            if p.client_order_id in self._risk_orders:
                self._risk_orders[p.client_order_id]["order_id"] = oid
        if p.order_type == "FAK":
            a.update(order_type="FAK", cap=p.price, fee_rate=float(rules.get("fee_rate") or 0),
                     fee_exponent=int(rules.get("fee_exponent") or 1),
                     size=getattr(p, "shares", None))
            self.fak_orders[oid] = a
        try:
            ok = self.receipt_sink(receipt)
        except Exception as exc:  # noqa: BLE001
            ok, row["authorize_error"] = False, f"{type(exc).__name__}: {exc}"[:160]
        a["authorized"] = bool(ok)
        if not ok:
            row.setdefault("authorize_error", getattr(self.ledger, "last_persistence_error", None)
                           or "receipt sink refused authorization")
            self._trip_once(("order-uncertain", str(p.client_order_id)),
                            f"order {short_id(oid)} could not be authorized in the live ledger: reconcile")

    def status(self) -> dict:
        lim = self.sender.limits
        active = self.kill.active() if hasattr(self.kill, "active") else list(self.kill.reasons)
        return {"halted": active, "balance": None if self.balance is None else round(self.balance, 2),
                "orders": lim.orders, "fills": lim.fills, "potential_fills": lim.pending_fills,
                "spend_usd": round(lim.exposure_usd, 2), "worst_case_loss_usd": round(self._risk_budget_used(), 6),
                "rounds": len(lim.rounds), "awaiting_confirmation": len(self.awaiting),
                "session_loss_usd": round(self.session_loss, 2), "clock": self.sender.clock.status(),
                "rules_ready": bool(self.rules_ready), "guard": self.sender.guard.status(), "counts": dict(self.n)}


def build_live_execution(*, ledger, on_event=None, user_ready=None, fills=None, env=None,
                         new_session: bool = False) -> LiveExecution:
    """The canary, built on the audited live client: its L2 credentials, its
    signature type and funder, its market-rules validation and its order
    observer. The private key is read from the environment, never logged.
    Blocking (derives the client's credentials): call it from a worker thread."""
    import os
    import config
    import paper_trade
    import polymarket_trade as pt
    from py_clob_client_v2.order_builder.builder import OrderBuilder
    env = os.environ if env is None else env
    if float(config.ASSUMED_MATCH_DELAY_SECONDS) <= 0:
        raise RuntimeError("set ASSUMED_MATCH_DELAY_SECONDS (e.g. 1): the BTC 5-minute markets flag Polymarket's "
                           "taker matching delay, and the live market-rules check refuses every round without it")
    client = pt._get_client()                                   # derives the L2 credentials
    key = pt._normalize_private_key(env.get("POLY_PRIVATE_KEY") or env.get("PRIVATE_KEY"))
    if not key:
        raise RuntimeError("live canary needs POLY_PRIVATE_KEY")
    signer = fast_signer(key, config.CHAIN_ID)
    if signer.address().lower() != client.signer.address().lower():
        raise RuntimeError("canary signer does not match the live client's signer")
    # the library's builder with the EIP-712 digest computed directly (byte-identical signatures)
    builder = FastOrderBuilder(OrderBuilder(signer, client.builder.signature_type, client.builder.funder))
    host = config.CLOB_HOST.rstrip("/")
    rules = RoundRules(lambda cond, up, down: pt._validate_market_mapping(client.get_clob_market_info(cond),
                                                                          cond, up, down))
    stop_file = env.get("SNIPER_CANARY_STOP_FILE") or str(Path(__file__).resolve().parent / "STOP_LIVE")
    sender = LeanSender(builder=builder, signer=signer, creds=client.creds, host=host, http=make_http(origin=host),
                        clock=ServerClock(), rules=rules, limits=limits_from_env(env), kill=KillSwitch(stop_file))
    shim = LiveBrokerShim(lambda cid: paper_trade.fetch_market_rules(cid, host=host),
                          min_buy_price=float(config.MIN_BUY_PRICE), max_buy_price=float(config.MAX_BUY_PRICE),
                          max_spread=float(config.MAX_ALLOWED_SPREAD),
                          trade_window_seconds=float(config.EXECUTION_WINDOW_SECONDS),
                          min_seconds_to_expiry=float(config.MIN_SECONDS_TO_EXPIRY))
    ex = LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=pt._journal_receipt,
                       balance_fetch=pt.get_balance_allowance, clock_url=f"{host}/time",
                       order_status=lambda oid: client.get_order(oid), user_ready=user_ready, fills=fills,
                       on_event=on_event, assumed_delay_s=float(config.ASSUMED_MATCH_DELAY_SECONDS))
    # The session budget survives restarts (audit F3): restored, reconciled
    # with the ledger's authorizations, or refused - before anything trades.
    holder = client.builder.funder or signer.address()
    state_path = env.get("SNIPER_CANARY_STATE_PATH") or str(Path(ledger.path).with_name("live_canary_state.json"))
    ex.session = CanarySession(state_path, CanarySession.wallet_key_for(holder))
    line = ex.session.open(sender.limits, sender.kill, ledger, new_session=new_session)
    sender.session = ex.session
    ex.session_start = ex.session.started_wall
    (on_event or (lambda *a: None))("sniper", f"LIVE CANARY {line}", "warn")
    return ex
