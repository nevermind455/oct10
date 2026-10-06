"""Lean LIVE sender for the sniper - CANARY ONLY. Nothing here runs unless a
live canary is explicitly started, and nothing here changes a trading
decision: the Sniper decides exactly as in paper; this module only executes.

It holds no credentials of its own. It takes the audited, already
authenticated ClobClient (polymarket_trade._get_client()) and reuses that
client's own OrderBuilder (signer, signature type, funder) and API creds, so
an order is signed and authenticated exactly as the audited live path does -
without that path's per-order network calls.

Hot path, signal -> network send, and nothing else:
    exact price (sniper_mode.signing_price) -> sign (the client's builder) ->
    order JSON + L2 HMAC headers stamped with a CACHED server time ->
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
import statistics
import time
import dataclasses
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path

from sniper_mode import FastOrderBuilder, fast_signer, signing_price  # noqa: F401 - fast_signer: build the live signer with it

# Result kinds of one send. Only MATCHED and PENDING mean the venue took the
# order; AMBIGUOUS means nobody can know without reconciling.
MATCHED, PENDING, NO_MATCH, REJECTED, NOT_SENT, AMBIGUOUS = (
    "MATCHED", "PENDING", "NO_MATCH", "REJECTED", "NOT_SENT", "AMBIGUOUS")


class Halted(Exception):
    """New orders are stopped (kill switch). Reconciliation continues."""


class Refused(Exception):
    """This one order must not be sent; trading may continue."""


class KillSwitch:
    """Once tripped, every new order is refused until a human clears it."""

    def __init__(self, stop_file: str | None = None) -> None:
        self.stop_file = Path(stop_file) if stop_file else None
        self.reasons: list[str] = []

    def trip(self, reason: str) -> None:
        if reason not in self.reasons:
            self.reasons.append(reason)

    def check(self) -> None:
        if self.stop_file is not None and self.stop_file.exists():
            self.trip(f"stop file present: {self.stop_file}")
        if self.reasons:
            raise Halted("; ".join(self.reasons))

    @property
    def tripped(self) -> bool:
        return bool(self.reasons)


class VenueGuard:
    """Venue latency guard with automatic recovery: it stops NEW orders while
    the venue answers slowly and lets them resume once everything an order
    needs checks healthy again - but repeated trouble still hard-halts.

    Each DEFINITE order reply reports its send -> response time:
      <= slow_ms          healthy
      slow_ms..extreme_ms a strike: HEALTHY -> DEGRADED (warning) -> PAUSED;
                          new orders are refused (and journalled) until
                          `healthy_checks` consecutive health probes pass:
                          PAUSED -> RECOVERED -> HEALTHY
      > extreme_ms        HARD_HALTED at once
    `strikes_to_halt` strikes within `strike_window_s`, or a pause that has not
    recovered within `max_pause_s`, also HARD_HALT - through the kill switch,
    which only a restart clears. Unknown outcomes (timeouts, resets) never come
    here: they still halt through the kill switch directly, as before.
    Every transition is journalled (result "guard") and announced."""
    HEALTHY, DEGRADED, PAUSED, RECOVERED, HARD_HALTED = "HEALTHY", "DEGRADED", "PAUSED", "RECOVERED", "HARD_HALTED"
    _LEVEL = {"HEALTHY": "good", "RECOVERED": "good", "DEGRADED": "warn", "PAUSED": "warn", "HARD_HALTED": "bad"}

    def __init__(self, kill: KillSwitch, *, slow_ms: float = 1500.0, extreme_ms: float = 5000.0,
                 strikes_to_halt: int = 3, strike_window_s: float = 1800.0, healthy_checks: int = 3,
                 max_pause_s: float = 300.0, journal=None, on_event=None, mono=time.monotonic) -> None:
        self.kill = kill
        self.slow_ms, self.extreme_ms = slow_ms, extreme_ms
        self.strikes_to_halt, self.strike_window_s = strikes_to_halt, strike_window_s
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
        self.on_event("sniper", f"LIVE latency guard {old} -> {new}: {detail}"[:220], self._LEVEL[new])

    def on_reply(self, rtt_ms: float) -> str:
        """One definite order reply's send -> response time; returns the state."""
        if self.state == self.HARD_HALTED:
            return self.state
        if rtt_ms > self.extreme_ms:
            self.hard_halt(f"send -> response {rtt_ms:.0f}ms exceeded {self.extreme_ms:.0f}ms")
        elif rtt_ms > self.slow_ms:
            now = self.mono()
            self.strikes.append(now)
            while self.strikes and now - self.strikes[0] > self.strike_window_s:
                self.strikes.popleft()
            if len(self.strikes) >= self.strikes_to_halt:
                self.hard_halt(f"{len(self.strikes)} replies slower than {self.slow_ms:.0f}ms within "
                               f"{self.strike_window_s / 60:.0f} min (last {rtt_ms:.0f}ms)")
            else:
                detail = f"reply {rtt_ms:.0f}ms > {self.slow_ms:.0f}ms (strike {len(self.strikes)}/{self.strikes_to_halt})"
                if self.state != self.PAUSED:
                    self._to(self.DEGRADED, detail)
                    self._to(self.PAUSED, "new orders paused until the venue checks healthy")
                self.paused_at, self.healthy_streak, self.reason = now, 0, detail
        return self.state

    def check(self) -> None:
        """The order path: refused (journalled by the caller) while PAUSED."""
        if self.state == self.PAUSED:
            raise Refused(f"latency guard PAUSED for {self.mono() - (self.paused_at or self.mono()):.0f}s "
                          f"({self.reason}); {self.healthy_streak}/{self.healthy_checks} healthy checks")

    def pause_for(self, seconds: float, reason: str) -> None:
        """A definitive venue rejection: wait, then require healthy probes.
        This never resends the rejected order; only a new signal can trade."""
        if self.state == self.HARD_HALTED:
            return
        now = self.mono()
        self.retry_not_before = max(self.retry_not_before, now + seconds)
        self.paused_at, self.healthy_streak, self.reason = now, 0, reason
        self._to(self.PAUSED, f"{reason}; retry no earlier than {seconds:g}s")

    def on_probe(self, ok: bool, detail: str = "") -> str:
        """One recovery health check while PAUSED; returns the state."""
        if self.state != self.PAUSED:
            return self.state
        if self.mono() < self.retry_not_before:
            self.healthy_streak = 0
            return self.state
        if ok:
            self.healthy_streak += 1
            if self.healthy_streak >= self.healthy_checks:
                self._to(self.RECOVERED, f"{self.healthy_streak} healthy checks in a row ({detail})")
                self.paused_at, self.healthy_streak, self.reason = None, 0, ""
                self.retry_not_before = 0.0
                self._to(self.HEALTHY, "new orders resume")
                return self.state
        else:
            self.healthy_streak = 0
        if self.paused_at is not None and self.mono() - max(self.paused_at, self.retry_not_before) > self.max_pause_s:
            self.hard_halt(f"not healthy within {self.max_pause_s:.0f}s of the pause (last check: {detail})")
        return self.state

    def hard_halt(self, reason: str) -> None:
        if self.state != self.HARD_HALTED:
            self._to(self.HARD_HALTED, reason)
        self.kill.trip(f"latency guard: {reason}")

    def status(self) -> dict:
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
        self.rejected = 0

    def add_sample(self, server_s: float, sent_wall: float, recv_wall: float, rtt_ms: float) -> bool:
        if not rtt_ms <= self.max_rtt_ms:
            self.rejected += 1
            return False
        # A whole-second reply means the server was somewhere in [s, s+1):
        # take the middle, or the offset runs half a second slow.
        mid = float(server_s) + (0.5 if float(server_s).is_integer() else 0.0)
        self.samples.append((self.mono(), mid - (sent_wall + recv_wall) / 2.0, rtt_ms))
        self.offset = statistics.median(s[1] for s in list(self.samples)[-5:])
        self.at = self.mono()
        return True

    def now_s(self) -> int:
        """The timestamp to sign with, or Halted if the clock cannot be trusted."""
        if self.offset is None or self.at is None:
            raise Halted("server clock never synced")
        age = self.mono() - self.at
        if age > self.max_age_s:
            raise Halted(f"server clock stale ({age:.0f}s > {self.max_age_s:.0f}s)")
        return int(self.wall() + self.offset)

    async def refresh(self, http, url: str) -> bool:
        sent = self.wall()
        t = time.perf_counter()
        r = await http.get(url)
        rtt = (time.perf_counter() - t) * 1000.0
        recv = self.wall()
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
        self.on_invalidate = None

    async def prefetch(self, condition: str, up: str, down: str) -> dict:
        key = str(condition)
        generation = self._generation.get(key, 0)
        rules = await asyncio.to_thread(self.fetch, condition, up, down)
        self.fetches += 1
        if generation != self._generation.get(key, 0):
            raise Refused("rules invalidated during refresh; waiting for a new fetch")
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
        await asyncio.sleep(every_s)


@dataclass
class CanaryLimits:
    """Hard caps for a measurement canary. Exceeding one refuses the order;
    the loss and exposure caps also trip the kill switch."""
    max_order_shares: float = 10.0
    max_orders: int = 40
    max_fills: int = 20
    max_rounds: int = 6
    max_exposure_usd: float = 30.0
    max_daily_loss_usd: float = 15.0
    orders: int = 0
    fills: int = 0
    exposure_usd: float = 0.0
    rounds: set = field(default_factory=set)

    def check(self, amount: float, shares: float, condition: str, kill: KillSwitch, realized_loss_usd=None):
        if shares > self.max_order_shares + 1e-9:
            raise Refused(f"order of {shares:.2f} shares exceeds the canary cap {self.max_order_shares}")
        if self.orders >= self.max_orders:
            kill.trip(f"canary order cap reached ({self.max_orders})")
        if self.fills >= self.max_fills:
            kill.trip(f"canary fill cap reached ({self.max_fills})")
        if condition not in self.rounds and len(self.rounds) >= self.max_rounds:
            kill.trip(f"canary round cap reached ({self.max_rounds})")
        if self.exposure_usd + amount > self.max_exposure_usd + 1e-9:
            kill.trip(f"canary exposure cap ${self.max_exposure_usd:.2f} would be exceeded")
        if realized_loss_usd is not None and realized_loss_usd >= self.max_daily_loss_usd:
            kill.trip(f"canary daily loss ${realized_loss_usd:.2f} >= ${self.max_daily_loss_usd:.2f}")
        kill.check()

    def record_send(self, amount: float, condition: str) -> None:
        self.orders += 1
        self.rounds.add(condition)
        self.exposure_usd += amount           # counted at send: an unknown order is exposure

    def record_outcome(self, kind: str, amount: float) -> None:
        if kind in (MATCHED, PENDING):
            self.fills += kind == MATCHED
        elif kind in (NO_MATCH, REJECTED, NOT_SENT):
            self.exposure_usd = max(0.0, self.exposure_usd - amount)   # definitely not spent
        # AMBIGUOUS keeps its exposure until reconciled


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


class LeanSender:
    """Prepare (sync, on the event loop) and send (async) one sniper order."""

    def __init__(self, *, builder, signer, creds, host: str, http, clock: ServerClock, rules: RoundRules,
                 limits: CanaryLimits, kill: KillSwitch, version: int = 2, journal=None,
                 timeout_s: float = 3.0, latency_halt_ms: float = 1500.0, max_consecutive_errors: int = 3,
                 latency_extreme_ms: float = 5000.0, guard: "VenueGuard | None" = None,
                 realized_loss=None) -> None:
        import polymarket_trade as pt
        from py_clob_client_v2.clob_types import CreateOrderOptions, MarketOrderArgsV2, OrderType, RequestArgs
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
        self._lib = dict(CreateOrderOptions=CreateOrderOptions, MarketOrderArgsV2=MarketOrderArgsV2,
                         FOK=OrderType.FOK, FAK=OrderType.FAK, RequestArgs=RequestArgs, POST_ORDER=POST_ORDER,
                         l2=create_level_2_headers, BUY=BUY, to_json=order_to_json_v2)

    # ------------------------------------------------------------ hot path, synchronous
    def prepare(self, *, side: str, token: str, up: str, down: str, condition: str, end: float,
                amount: float, cap: float, decided_pc: int | None = None, order_type: str = "FOK") -> Prepared:
        st = {"decided_pc": decided_pc, "prepare_pc": time.perf_counter_ns()}
        if order_type not in ("FOK", "FAK"):
            raise Refused(f"unknown order type {order_type!r}")
        self.kill.check()
        self.guard.check()                            # Refused while the venue guard has new orders paused
        ts = self.clock.now_s()                       # Halted before any work if the time is not trusted
        rules = self.rules.get(condition, up, down)
        if self.pt._ambiguous_blocks(str(condition), str(token)):
            raise Refused("an earlier submission on this token has an unknown outcome")
        tick = f"{Decimal(str(rules['tick'])).normalize():f}"
        step = Decimal(tick)
        exact = (Decimal(str(cap)) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        if not (step <= exact <= 1 - step and exact <= Decimal(str(cap))):
            raise Refused(f"capped price {exact} outside the tradable range for tick {tick}")
        shares = Decimal(str(amount)) / exact
        if shares < Decimal(str(rules["minimum"])):
            raise Refused(f"{shares:.4f} shares at {exact} is below the venue minimum {rules['minimum']}")
        self.limits.check(float(amount), float(shares), str(condition), self.kill,
                          self.realized_loss() if self.realized_loss else None)
        lib = self._lib
        st["build_pc"] = time.perf_counter_ns()
        signed = self.builder.build_market_order(
            lib["MarketOrderArgsV2"](token_id=str(token), amount=float(amount), side=lib["BUY"],
                                     price=signing_price(cap, tick), order_type=lib[order_type]),
            lib["CreateOrderOptions"](tick_size=tick, neg_risk=bool(rules["neg_risk"])), version=self.version)
        st["signed_pc"] = time.perf_counter_ns()
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
        return Prepared(side, str(token), str(condition), float(end), float(amount), float(cap),
                        float(exact), tick, str(salt), body, headers, rules, st, order_type)

    # ------------------------------------------------------------ the write, synchronous
    def start(self, p: Prepared):
        """Write the order to the wire NOW - inside the caller's own step, no
        event-loop turn - when the HTTP client has a live HTTP/2 connection
        (fast_h2.FastH2.post_now). Returns the in-flight stream for send(), or
        None when it cannot (httpx, or no live connection): send() then goes
        the async way, which connects first. Nothing is written on None."""
        post_now = getattr(self.http, "post_now", None)
        if post_now is None or not getattr(self.http, "connected", False):
            return None
        p.stamps["post_call_pc"] = time.perf_counter_ns()
        try:
            stream = post_now(self.url, p.body.encode("utf-8"), p.headers)
        except Exception:  # noqa: BLE001 - raised before any byte is written: the async path decides
            return None
        self.limits.record_send(p.amount, p.condition)
        return stream

    # ------------------------------------------------------------ the send, async
    async def send(self, p: Prepared, started=None) -> dict:
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
            events.append((name, time.perf_counter_ns()))
        self.limits.record_send(p.amount, p.condition)
        try:
            r = await self.http.post(self.url, content=p.body.encode("utf-8"), headers=p.headers,
                                     extensions={"trace": trace}, timeout=self.timeout_s)
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
                   order_id=(resp or {}).get("orderID") if isinstance(resp, dict) else None,
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
        self.limits.record_outcome(kind, p.amount)
        if kind == AMBIGUOUS:
            self.pt._mark_ambiguous(p.condition, p.end, p.token)
            self.kill.trip(f"order {p.client_order_id} outcome unknown ({row['error']}): reconcile before trading")
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
            self.guard.pause_for(delay, "venue post-only mode")
            row["venue_retry_after_s"] = delay
        if row["new_connection"]:
            row["warning"] = "a new TCP/TLS connection was opened on the hot path"
        if kind in (MATCHED, PENDING, NO_MATCH) or temporary:
            self.consecutive_errors = 0
        else:
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.max_consecutive_errors:
                self.kill.trip(f"{self.consecutive_errors} consecutive order errors")
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
    SNIPER_CANARY_MAX_SPEND_USD bounds what the whole session can lose."""
    import os
    env = os.environ if env is None else env

    def num(name, default, cast=float):
        try:
            return cast(env.get(name, default))
        except (TypeError, ValueError):
            return cast(default)
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
        self.hub = None
        self.guard_probe_s = 2.0
        sender.realized_loss = lambda: self.session_loss
        sender.guard.on_event = self.on_event

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

    def limits_line(self) -> str:
        lim = self.sender.limits
        return (f"SNIPER LIVE CANARY limits: <= {lim.max_order_shares:g} sh/order, {lim.max_orders} orders, "
                f"{lim.max_fills} fills, {lim.max_rounds} rounds, ${lim.max_exposure_usd:g} total spend, "
                f"${lim.max_daily_loss_usd:g} realized loss; stop file {self.kill.stop_file}")

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
        # Fills this wallet already had that no order explains are the
        # operator's to know about, not a reason to halt this session.
        self.baseline_unexplained = await asyncio.to_thread(self._unexplained_now)
        self.hub = hub
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(keep_warm(self.sender.http, self.sender.clock, self.clock_url,
                                                  self.keep_warm_s, self._warm_event)),
                       loop.create_task(self._balance_loop()), loop.create_task(self._reconcile_loop()),
                       loop.create_task(self._rules_loop(hub)), loop.create_task(self._guard_loop())]
        if self.kill.stop_file is not None and self.kill.stop_file.exists():
            self.on_event("sniper", f"LIVE CANARY: stop file {self.kill.stop_file} present - no orders "
                                    f"(remove it and restart to trade)", "bad")
        self.on_event("sniper", f"LIVE CANARY armed: collateral ${self.balance:.2f}; " + self.limits_line(), "warn")

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
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
        bal = await asyncio.to_thread(self.balance_fetch)
        if isinstance(bal, dict) and bal.get("balance") is not None and bal.get("allowance") is not None:
            # Spendable is the smaller of what we hold and what the exchange may move.
            self.balance = min(float(bal["balance"]), float(bal["allowance"]))
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
                    self._trip(str(exc))
                    await asyncio.sleep(0.5)
                    continue
                missing = missing or self.shim.cached_rules(str(key[0])) is None
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
            await asyncio.sleep(0.5)

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
                self._trip(f"reconciliation failed: {type(exc).__name__}: {exc}")

    def _trip(self, reason: str) -> None:
        self.kill.trip(reason)
        if reason not in self._announced:
            self._announced.add(reason)
            self.on_event("sniper", f"LIVE CANARY HALTED: {reason}", "bad")

    # ------------------------------------------------------------ ledger views (worker thread)
    def _unexplained_now(self) -> set:
        """Confirmed fills on this wallet the ledger refused because no order
        of ours explains them (or they do not match the order), and that it
        has not booked since. A fill that arrived a moment before its order's
        authorization is booked on the next pass and drops out of this set."""
        led = self.ledger
        with led._lock:
            return ((set(getattr(led, "reported_unauthorized", ())) | set(getattr(led, "reported_mismatch", ())))
                    - set(led.seen))

    def _confirmed_orders(self) -> set:
        led = self.ledger
        with led._lock:
            return {lot.order_id for pos in led.positions.values() for lot in getattr(pos, "lots", ())
                    if getattr(lot, "order_id", None)}

    def _session_realized(self) -> float:
        led = self.ledger
        with led._lock:
            return sum(float(getattr(p, "realized", 0) or 0) for p in led.positions.values()
                       if getattr(p, "settled", False) and float(getattr(p, "settled_wall", 0) or 0) >= self.session_start)

    def reconcile_once(self) -> None:
        """Every accepted order must turn up CONFIRMED in the ledger; no fill
        may stay unexplained past a short grace; the session's realized loss
        feeds the loss cap. Runs on a worker thread."""
        now = time.time()
        unexplained = self._unexplained_now() - self.baseline_unexplained
        for tid in [t for t in self._unexplained_since if t not in unexplained]:
            self._unexplained_since.pop(tid, None)
        for tid in unexplained:
            first = self._unexplained_since.setdefault(tid, now)
            if now - first > self.unexplained_grace_s:
                self._trip(f"a confirmed fill on this wallet that no order of ours explains "
                           f"(trade ...{str(tid)[-10:]}): reconcile by hand")
        confirmed = self._confirmed_orders()
        for oid, a in list(self.awaiting.items()):
            if a.get("order_type") == "FAK":
                self._reconcile_fak(oid, a, now)
                continue
            if oid in confirmed:
                self.awaiting.pop(oid, None)
                self.n["confirmed in ledger"] += 1
                continue
            age = now - a["wall"]
            if (self.order_status is not None and age > 30 and not a.get("venue_matched")
                    and now - a.get("checked_at", 0.0) > 15):
                a["checked_at"] = now
                try:
                    status = str((self.order_status(oid) or {}).get("status") or "").lower()
                except Exception:  # noqa: BLE001 - counted; the deadline below still applies
                    status = ""
                    self.n["order status errors"] += 1
                a["venue_status"] = status
                if status in ("canceled", "cancelled", "unmatched", "killed", "expired"):
                    # A delayed FOK the venue then killed: no fill will come.
                    self.awaiting.pop(oid, None)
                    self.sender.limits.exposure_usd = max(0.0, self.sender.limits.exposure_usd - a["amount"])
                    self.n["accepted but never filled"] += 1
                    continue
                a["venue_matched"] = status == "matched"
            if age > self.confirm_within_s:
                self._trip(f"order {oid} ({a['kind']}, venue status {a.get('venue_status') or 'unknown'}) not "
                           f"confirmed in the ledger after {self.confirm_within_s:.0f}s: reconcile")
        self.session_loss = max(0.0, -self._session_realized())

    def _reconcile_fak(self, oid, a, now):
        """A canceled FAK can still have fills. Resolve all trades before
        releasing unused spend; never treat cancellation alone as zero."""
        age = now - a["wall"]
        if (not a.get("trade_ids") and self.order_status is not None and age > 1
                and now - a.get("checked_at", 0) > self.reconcile_every_s):
            a["checked_at"] = now
            try:
                order = self.order_status(oid) or {}
            except Exception:
                order = {}
                self.n["order status errors"] += 1
            if order:
                if (str(order.get("id")) != str(oid) or str(order.get("asset_id")) != a["token"]
                        or str(order.get("market")) != a["condition"] or order.get("side") != "BUY"):
                    self._trip(f"FAK order {oid} status identity mismatch: reconcile")
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
                    except (KeyError, ArithmeticError, TypeError, ValueError):
                        pass                          # incomplete evidence keeps the full reservation
        fill = self.fill_for(oid)
        if fill is not None:
            if fill["shares"] == 0:
                self.sender.limits.exposure_usd = max(0.0, self.sender.limits.exposure_usd - a["amount"])
                self.sender.limits.fills -= 1     # release this accepted FAK's reserved fill slot
                self.awaiting.pop(oid, None)
                self.n["accepted but never filled"] += 1
                return
            with self.ledger._lock:
                booked = set(fill["trade_ids"]) <= set(self.ledger.seen)
            if fill["statuses"] == ["CONFIRMED"] and booked:
                # Only the proven unused notional is returned to the session cap.
                unused = max(0.0, a["amount"] - fill["cost"])
                self.sender.limits.exposure_usd = max(0.0, self.sender.limits.exposure_usd - unused)
                self.awaiting.pop(oid, None)
                self.n["confirmed in ledger"] += 1
                return
        if age > self.confirm_within_s:
            self._trip(f"FAK order {oid} not completely confirmed after {self.confirm_within_s:.0f}s: reconcile")

    def fill_for(self, order_id) -> dict | None:
        """Our fills of one order as the private channel reported them
        (MATCHED is provisional; only CONFIRMED is booked by the ledger).
        For FAK, require the complete trade set and validate its quantities."""
        store = self.fills
        if store is None or not order_id:
            return None
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
                self._trip(f"FAK order {oid} has contradictory zero-fill evidence: reconcile")
                return None
            a["quantity_resolved"] = True
            return {"shares": 0.0, "cost": 0.0, "fee": 0.0, "price": None,
                    "source": "terminal order status", "statuses": [], "trade_ids": []}
        actual_ids = {str(f.trade_id) for f in rows}
        if expected and actual_ids - expected:
            self._trip(f"FAK order {oid} has unexpected trades: reconcile")
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
                self._trip(f"FAK order {oid} has a {status} trade: reconcile")
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
                self._trip(f"FAK order {oid} has an invalid fill: reconcile")
                return None
            shares += qty
            paid = getattr(f, "paid", None)
            cost += Decimal(str(paid)) if paid is not None else qty * px
            fee += curve_fee(qty, px, rules)
        if cost > Decimal(str(a["amount"])) + Decimal("0.000001"):
            self._trip(f"FAK order {oid} filled above its budget: reconcile")
            return None
        if a.get("final_size") is not None and abs(float(shares) - a["final_size"]) > 1e-6:
            return None                            # not all final fills have arrived yet
        a["quantity_resolved"] = True
        return {"shares": float(shares), "price": float(cost / shares), "cost": float(cost), "fee": float(fee),
                "partial": cost < Decimal(str(a["amount"])) - Decimal("0.01"),
                "trades": len(rows), "trade_ids": sorted(actual_ids),
                "statuses": sorted({str(f.status).upper() for f in rows}), "source": "user channel"}

    # ------------------------------------------------------------ one order (event loop)
    def prepare(self, plan: dict, decided_pc: int | None = None) -> Prepared:
        """Size and check the order exactly as the paper broker does (same
        functions, on the book the decision used), then sign it. Raises
        Refused (this order not sent) or Halted (no orders until a human
        restarts the canary). No I/O."""
        from paper_trade import (MIN_MARKETABLE_BUY_USD, PaperRejected, estimate_fok, estimate_fak, size_to_venue_minimum,
                                 snapshot_from_book_view)
        self.kill.check()
        order_type = str(plan.get("order_type", "FOK")).upper()
        if order_type not in ("FOK", "FAK"):
            raise Refused(f"unknown order type {order_type!r}")
        for oid, a in list(self.fak_orders.items()):
            if not a.get("quantity_resolved") and self.fill_for(oid) is None:
                self.kill.check()
                raise Refused("accepted FAK order is still awaiting complete fill quantities")
        self.sender.guard.check()                       # Refused (journalled) while the venue guard has orders paused
        token, cond, end = plan["token"], plan["cond"], float(plan["end"])
        side, amount, up, down = plan["send"][:4]
        if self.balance_at is None or time.monotonic() - self.balance_at > self.balance_max_age_s:
            raise Refused("collateral balance is stale")
        if self.user_ready is not None and not self.user_ready(cond):
            raise Refused("the private fill channel is not subscribed to this market yet")
        rules = self.shim.cached_rules(cond)
        if rules is None:
            raise Refused("market rules not loaded for this round yet")
        order_rules = self.sender.rules.get(cond, str(up), str(down))
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
            spend = size_to_venue_minimum(amount, book, rules, cap)
            # The SDK signs BUY size at the limit, not the visible average.
            # Reserve enough for the venue minimum at that exact limit even
            # when slippage is wider than the generic sizing headroom.
            price = (Decimal(str(cap)) / rules.tick_size).to_integral_value(rounding=ROUND_FLOOR) * rules.tick_size
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
        reserve_fee = max(fee, spend * float(rules.fee_rate)) if order_type == "FAK" else fee
        free = (self.balance or 0.0) - self.reserved
        if spend + reserve_fee > free + 1e-9:
            raise Refused(f"collateral ${free:.2f} is below the order ${spend + reserve_fee:.2f} incl. fee")
        p = self.sender.prepare(side=side, token=token, up=str(up), down=str(down), condition=cond, end=end,
                                amount=spend, cap=cap, decided_pc=decided_pc, order_type=order_type)
        p.reserve = spend + reserve_fee
        p.estimate = {"shares": float(quote.shares), "price": float(quote.average_price), "fee": fee}
        self.reserved += p.reserve
        return p

    def start_send(self, p: Prepared):
        """The order's bytes onto the warm connection now (see LeanSender.start)."""
        return self.sender.start(p)

    async def send(self, p: Prepared, started=None) -> dict:
        reserve = getattr(p, "reserve", p.amount)
        try:
            row = await self.sender.send(p, started)
        finally:
            self.reserved = max(0.0, self.reserved - reserve)
        kind = row["kind"]
        if kind in (MATCHED, PENDING, AMBIGUOUS) and self.balance is not None:
            self.balance -= reserve                         # spent, or may be; the next read corrects it
        if kind in (MATCHED, PENDING) and row.get("order_id"):
            if kind == PENDING and row.get("error") != "DELAYED_PENDING_OUTCOME":
                # As the audited path: only a documented delayed placement may
                # be followed; any other incomplete success blocks this token.
                self.pt._mark_ambiguous(p.condition, p.end, p.token)
            await asyncio.to_thread(self._authorize, p, row)
        elif kind in (MATCHED, PENDING):
            self._trip(f"accepted order with no order id ({row.get('venue_status')}): reconcile")
        if kind == AMBIGUOUS:
            self._trip(f"order outcome unknown ({row.get('error')}): check the venue and the ledger by hand")
        if kind in (MATCHED, PENDING) and p.order_type == "FAK":
            # Do not infer response amount units. Private trade records give
            # decimal quantities and the complete trade set.
            row["fill"] = self.fill_for(row.get("order_id"))
        elif kind == MATCHED:
            row["fill"] = self._fill_from_response(row) or self.fill_for(row.get("order_id"))
        self.n[kind] += 1
        return row

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
        try:
            ok = self.receipt_sink(receipt)
        except Exception as exc:  # noqa: BLE001
            ok, row["authorize_error"] = False, f"{type(exc).__name__}: {exc}"[:160]
        self.awaiting[oid] = {"wall": time.time(), "condition": p.condition, "token": p.token,
                              "amount": p.amount, "kind": row["kind"]}
        if p.order_type == "FAK":
            resp = row.get("response") or {}
            tids = resp.get("tradeIDs") or resp.get("trade_ids") or []
            a = self.awaiting[oid]
            a.update(order_type="FAK", cap=p.price, fee_rate=float(rules.get("fee_rate") or 0),
                     fee_exponent=int(rules.get("fee_exponent") or 1),
                     trade_ids=set(tids) if row["kind"] == MATCHED else set())
            self.fak_orders[oid] = a
            if row["kind"] == PENDING:
                self.sender.limits.fills += 1     # pending FAK also reserves a session fill slot
        if not ok:
            self._trip(f"order {oid} could not be authorized in the live ledger: reconcile")

    def status(self) -> dict:
        lim = self.sender.limits
        return {"halted": list(self.kill.reasons), "balance": None if self.balance is None else round(self.balance, 2),
                "orders": lim.orders, "fills": lim.fills, "spend_usd": round(lim.exposure_usd, 2),
                "rounds": len(lim.rounds), "awaiting_confirmation": len(self.awaiting),
                "session_loss_usd": round(self.session_loss, 2), "clock": self.sender.clock.status(),
                "rules_ready": bool(self.rules_ready), "guard": self.sender.guard.status(), "counts": dict(self.n)}


def build_live_execution(*, ledger, on_event=None, user_ready=None, fills=None, env=None) -> LiveExecution:
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
    return LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=pt._journal_receipt,
                         balance_fetch=pt.get_balance_allowance, clock_url=f"{host}/time",
                         order_status=lambda oid: client.get_order(oid), user_ready=user_ready, fills=fills,
                         on_event=on_event, assumed_delay_s=float(config.ASSUMED_MATCH_DELAY_SECONDS))
