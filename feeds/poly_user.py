"""Polymarket CLOB user channel — real-time fills, order status, cancels.

    wss://ws-subscriptions-clob.polymarket.com/ws/user
    {"auth": {"apiKey":..., "secret":..., "passphrase":...},
     "markets": ["0xCONDITION_ID"], "type": "user"}

Deduplication is the whole point of this module. Three separate ways this
feed will hand you the same fill twice:

  1. Status progression. One trade arrives as MATCHED, then MINED, then
     CONFIRMED. Three messages, one fill. Counting messages triples volume.
  2. Reconnect replay. After a drop the venue may re-send recent trades.
     Same trade id, already counted.
  3. The REST reconcile backup. It reports the same trades the socket already
     delivered.

All three land in one store keyed by trade id, so a fill is recorded once no
matter which path found it. `order` UPDATE events carry a CUMULATIVE
`size_matched`; summing those is the same double-count in a different costume,
so the record keeps the maximum instead.
"""
from __future__ import annotations

import asyncio
import copy
import json
import math
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal

import websockets

from .health import LIVE, STALE, UNSYNCED
from .supervisor import SupervisedFeed
from .tls import ws_ssl

USER_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
PING_EVERY = 10.0

TERMINAL = ("CONFIRMED", "FAILED", "CONFLICT")
STATUS_RANK = {
    "RETRYING": 0,
    "MATCHED": 1,
    "MATCHED_NOT_BROADCASTED": 1,
    "MINED": 2,
    "CONFIRMED": 3,
    "FAILED": 4,
}


@dataclass
class Fill:
    trade_id: str
    asset_id: str | None = None
    market: str | None = None
    side: str | None = None
    price: float | None = None
    size: float = 0.0
    status: str = ""
    fee_rate_bps: float | None = None
    first_seen: float = 0.0
    last_seen: float = 0.0
    sources: set = field(default_factory=set)
    revisions: int = 0
    terminal_conflicts: int = 0
    order_id: str | None = None
    conflict_reason: str | None = None
    # USDC the taker side really exchanged, from the trade's maker legs (see
    # taker_paid). The venue's headline `price` is one number for a match that
    # can cross several levels, so price x size can overstate it: a $1.00 buy
    # of 10.322 shares from makers at 0.09/0.10 reported price 0.10 ($1.0322).
    # None when the legs were not in the message. The headline price remains
    # available for identity checks; accounting uses the cash-weighted basis.
    paid: float | None = None
    # Matched shares and prices expressed in the taker's token. Keeping every
    # leg lets accounting apply the nonlinear fee curve at each execution price.
    maker_legs: tuple[tuple[float, float], ...] | None = None
    # First complete execution observation, distinct from seeing a RETRYING
    # trade id or a partial record. Later confirmations do not backdate it.
    execution_seen: float | None = None

    @property
    def execution_known(self) -> bool:
        return (self.status in ("MATCHED", "MATCHED_NOT_BROADCASTED", "MINED", "CONFIRMED")
                and self.order_id is not None and self.asset_id is not None
                and self.market is not None and self.side in ("BUY", "SELL")
                and self.price is not None and math.isfinite(self.price) and 0 < self.price < 1
                and math.isfinite(self.size) and self.size > 0)

    @property
    def notional(self) -> float:
        return (self.price or 0.0) * self.size

    @property
    def paid_notional(self) -> float:
        """What this trade cost the taker: its maker legs when known, else price x size."""
        return self.paid if self.paid is not None else self.notional

    @property
    def counts(self) -> bool:
        """Only a complete, internally consistent terminal-success record counts."""
        return (self.status == "CONFIRMED"
                and self.asset_id is not None
                and self.market is not None
                and self.side in ("BUY", "SELL")
                and self.price is not None and math.isfinite(self.price)
                and 0 < self.price < 1
                and math.isfinite(self.size) and self.size > 0)


@dataclass
class OrderRec:
    order_id: str
    asset_id: str | None = None
    side: str | None = None
    price: float | None = None
    original_size: float = 0.0
    size_matched: float = 0.0        # cumulative; take max, never add
    state: str = ""                  # PLACEMENT | UPDATE | CANCELLATION
    last_seen: float = 0.0
    conflict_reason: str | None = None


class FillStore:
    """One store, many sources. Idempotent by construction."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.fills: dict[str, Fill] = {}
        self.orders: dict[str, OrderRec] = {}
        self.duplicates = 0
        self.late_status = 0
        self.invalid_updates = 0
        self.identity_conflicts = 0
        self._summary_cache: dict | None = None
        # order id -> its trade ids, so one order's fills are found without
        # copying every fill this process has seen (the live order path asks
        # while a FAK is unresolved). Rebuilt if `fills` was changed directly.
        self._by_order: dict[str, list[str]] = {}
        self._by_order_n = 0

    def _index_order_locked(self, rec) -> None:
        if rec.order_id:
            ids = self._by_order.setdefault(rec.order_id, [])
            if rec.trade_id not in ids:
                ids.append(rec.trade_id)

    def fills_for_order(self, order_id) -> list:
        """Copies of this order's fills (any status), in the order first seen."""
        oid = str(order_id or "")
        with self._lock:
            if self._by_order_n != len(self.fills):
                self._by_order = {}
                for rec in self.fills.values():
                    self._index_order_locked(rec)
                self._by_order_n = len(self.fills)
            return [copy.copy(self.fills[t]) for t in self._by_order.get(oid, ())
                    if t in self.fills and str(self.fills[t].order_id or "") == oid]

    def record_trade(self, trade_id, *, order_id=None, asset_id=None, market=None, side=None,
                     price=None, size=None, status="", source="ws",
                     fee_rate_bps=None, maker_orders=None) -> bool:
        """Returns True only the first time this trade id is seen."""
        if not trade_id or not str(trade_id).strip():
            return False
        tid = str(trade_id).strip()
        now = time.monotonic()
        status = _status(status)
        incoming = {
            "order_id": _s(order_id),
            "asset_id": _s(asset_id),
            "market": _s(market),
            "side": _side(side),
            "price": _price(price),
            "size": _size(size),
            "fee_rate_bps": _nonnegative(fee_rate_bps),
        }
        legs = taker_legs(incoming["asset_id"], maker_orders, incoming["size"])
        paid = _legs_paid(legs)
        with self._lock:
            self._summary_cache = None
            rec = self.fills.get(tid)
            if rec is None:
                self.fills[tid] = Fill(
                    trade_id=tid, order_id=incoming["order_id"],
                    asset_id=incoming["asset_id"], market=incoming["market"],
                    side=incoming["side"], price=incoming["price"],
                    size=incoming["size"] or 0.0,
                    status=status, first_seen=now, last_seen=now,
                    sources={str(source)}, fee_rate_bps=incoming["fee_rate_bps"],
                    paid=paid, maker_legs=legs)
                if self.fills[tid].execution_known:
                    self.fills[tid].execution_seen = now
                if self._by_order_n == len(self.fills) - 1:
                    self._index_order_locked(self.fills[tid])
                    self._by_order_n = len(self.fills)
                if (_has_invalid_numeric(price, incoming["price"], size, incoming["size"],
                                         fee_rate_bps, incoming["fee_rate_bps"])
                        or (side is not None and incoming["side"] is None)):
                    self.invalid_updates += 1
                return True
            self.duplicates += 1
            rec.last_seen = now
            rec.sources.add(str(source))
            rec.revisions += 1
            if (_has_invalid_numeric(price, incoming["price"], size, incoming["size"],
                                     fee_rate_bps, incoming["fee_rate_bps"])
                    or (side is not None and incoming["side"] is None)):
                self.invalid_updates += 1

            conflicts = []
            for key in ("order_id", "asset_id", "market", "side"):
                old, new = getattr(rec, key), incoming[key]
                if old is not None and new is not None and old != new:
                    conflicts.append(key)
            if (rec.price is not None and incoming["price"] is not None
                    and not math.isclose(rec.price, incoming["price"], rel_tol=0.0,
                                         abs_tol=1e-12)):
                conflicts.append("price")
            if (rec.size > 0 and incoming["size"] is not None
                    and not math.isclose(rec.size, incoming["size"], rel_tol=0.0,
                                         abs_tol=1e-12)):
                conflicts.append("size")
            if (rec.paid is not None and paid is not None
                    and not math.isclose(rec.paid, paid, rel_tol=0.0, abs_tol=1e-6)):
                conflicts.append("paid")
            if rec.maker_legs is not None and legs is not None and rec.maker_legs != legs:
                conflicts.append("maker legs")
            if conflicts:
                rec.status = "CONFLICT"
                rec.conflict_reason = "immutable fields changed: " + ",".join(conflicts)
                rec.terminal_conflicts += 1
                self.identity_conflicts += 1
                return False
            # Only move forward through the lifecycle; a replayed MATCHED
            # after CONFIRMED must not undo the confirmation.
            if rec.status in TERMINAL:
                if status in TERMINAL and status != rec.status:
                    # Two contradictory terminal states are not a fill. A
                    # malformed/out-of-order feed must fail closed instead of
                    # leaving a potentially failed trade in confirmed PnL.
                    rec.status = "CONFLICT"
                    self.late_status += 1
                    rec.terminal_conflicts += 1
                elif status and status != rec.status:
                    self.late_status += 1
            elif STATUS_RANK.get(status, -1) > STATUS_RANK.get(rec.status, -1):
                rec.status = status
            elif status and status != rec.status:
                self.late_status += 1
            if rec.price is None:
                rec.price = incoming["price"]
            if rec.paid is None and paid is not None:
                rec.paid = paid
            if rec.maker_legs is None and legs is not None:
                rec.maker_legs = legs
            rec.asset_id = rec.asset_id or incoming["asset_id"]
            if not rec.order_id and incoming["order_id"]:
                rec.order_id = incoming["order_id"]
                self._index_order_locked(rec)
            rec.market = rec.market or incoming["market"]
            rec.side = rec.side or incoming["side"]
            if rec.fee_rate_bps is None:
                rec.fee_rate_bps = incoming["fee_rate_bps"]
            if incoming["size"] is not None and rec.size <= 0:
                rec.size = incoming["size"]
            if rec.execution_seen is None and rec.execution_known:
                rec.execution_seen = now
            return False

    def record_order(self, order_id, *, asset_id=None, side=None, price=None,
                     original_size=None, size_matched=None, state="") -> None:
        if not order_id or not str(order_id).strip():
            return
        oid = str(order_id).strip()
        with self._lock:
            self._summary_cache = None
            rec = self.orders.get(oid)
            if rec is None:
                rec = self.orders[oid] = OrderRec(order_id=oid)
            incoming_asset = _s(asset_id)
            incoming_side = _side(side)
            incoming_price = _price(price)
            conflicts = []
            if rec.asset_id and incoming_asset and rec.asset_id != incoming_asset:
                conflicts.append("asset_id")
            if rec.side and incoming_side and rec.side != incoming_side:
                conflicts.append("side")
            if (rec.price is not None and incoming_price is not None
                    and not math.isclose(rec.price, incoming_price, rel_tol=0.0,
                                         abs_tol=1e-12)):
                conflicts.append("price")
            if conflicts:
                rec.state = "CONFLICT"
                rec.conflict_reason = "immutable fields changed: " + ",".join(conflicts)
                self.identity_conflicts += 1
                return
            rec.asset_id = incoming_asset or rec.asset_id
            rec.side = incoming_side or rec.side
            rec.price = incoming_price if incoming_price is not None else rec.price
            os_ = _size_or_zero(original_size)
            if os_ is not None:
                rec.original_size = max(rec.original_size, os_)
            sm = _size_or_zero(size_matched)
            if sm is not None:
                rec.size_matched = max(rec.size_matched, sm)   # cumulative
            if state:
                new_state = str(state).upper()
                if rec.state not in ("CANCELLATION", "CONFLICT"):
                    if new_state in ("PLACEMENT", "UPDATE", "CANCELLATION"):
                        rec.state = new_state
                    else:
                        self.invalid_updates += 1
            rec.last_seen = time.monotonic()

    # ------------------------------------------------------------- reads
    def summary(self) -> dict:
        with self._lock:
            if self._summary_cache is not None:
                return dict(self._summary_cache)
            counted = [f for f in self.fills.values() if f.counts]
            self._summary_cache = {
                "fills": len(counted),
                "seen": len(self.fills),
                "duplicates_suppressed": self.duplicates,
                "shares": sum(f.size for f in counted),
                "notional": sum(f.notional for f in counted),
                "open_orders": sum(1 for o in self.orders.values()
                                   if o.state == "PLACEMENT"),
                "cancelled": sum(1 for o in self.orders.values()
                                 if o.state == "CANCELLATION"),
                "pending": sum(1 for f in self.fills.values()
                               if f.status not in TERMINAL),
                "invalid_updates": self.invalid_updates,
                "identity_conflicts": self.identity_conflicts,
            }
            return dict(self._summary_cache)

    def recent(self, n: int = 20) -> list[Fill]:
        with self._lock:
            return sorted(self.fills.values(), key=lambda f: f.first_seen)[-n:]

    def trade_counts(self, trade_id) -> bool:
        with self._lock:
            rec = self.fills.get(str(trade_id))
            return bool(rec and rec.counts)

    def note_invalid(self) -> None:
        with self._lock:
            self.invalid_updates += 1
            self._summary_cache = None


class PolyUserFeed(SupervisedFeed):
    name = "poly_user"

    def __init__(self, creds: dict | None, url: str = USER_WS, *,
                 store: FillStore | None = None, markets=None,
                 stale_after: float = 90.0, recv_timeout: float = 45.0,
                 on_event=None, ping_every: float = PING_EVERY) -> None:
        super().__init__(on_event=on_event)
        self.url = url
        self._creds = creds or {}
        self.store = store or FillStore()
        self.stale_after = stale_after
        self.recv_timeout = recv_timeout
        self.ping_every = ping_every
        self._sub_lock = threading.RLock()
        self._want: list[str] = list(markets or [])
        self._sent: set[str] = set()
        # Monotonic wire-level generation.  Hub state can change before a
        # subscribe frame is sent, so reconciliation must key off this value,
        # which advances only after a subscription send succeeds.  A fresh
        # session advances it even when the requested markets are unchanged.
        self._subscription_generation = 0
        self._ws = None
        self._ping_sent: float | None = None
        self._pong_at: float | None = None
        self._pong_event = asyncio.Event()
        self.invalid_messages = 0

    def __repr__(self) -> str:                 # creds never reach a log line
        return f"<PolyUserFeed markets={len(self._want)} status={self.health.status}>"

    @property
    def authed(self) -> bool:
        return bool(self._creds.get("apiKey") and self._creds.get("secret")
                    and self._creds.get("passphrase"))

    @property
    def subscription_generation(self) -> int:
        with self._sub_lock:
            return self._subscription_generation

    def ready_for_market(self, condition_id) -> bool:
        """True only after this session sent the filter and proved heartbeat."""
        condition = str(condition_id or "")
        if not condition or not self.authed or self._ws is None:
            return False
        with self._sub_lock:
            subscribed = condition in self._sent
        if not subscribed:
            return False
        # Do not trust a stale cached health label. Recompute it from the
        # application PONG timestamp so a quiet-but-dead private stream cannot
        # authorize order submission.
        return self.refresh_status() == LIVE

    def ready_reason_for_market(self, condition_id) -> str:
        """Why ready_for_market would refuse, or "" when it would allow.

        Mirrors ready_for_market's checks in the same order WITHOUT being the
        gate: the gate stays exactly as it was, and this only explains it, so
        telemetry can say which readiness input blocked a decision instead of
        recording an undifferentiated False.
        """
        condition = str(condition_id or "")
        if not condition:
            return "no condition id"
        if not self.authed:
            return "private stream not authenticated"
        if self._ws is None:
            return "private stream socket is down"
        with self._sub_lock:
            subscribed = condition in self._sent
        if not subscribed:
            return "condition not subscribed on the private stream"
        status = self.refresh_status()
        if status != LIVE:
            return f"private stream status is {status}, not LIVE"
        return ""

    def set_markets(self, markets) -> None:
        markets = [str(m) for m in markets if m]
        with self._sub_lock:
            if markets == self._want:
                return
            self._want = markets
        self._wake_intent()
        self.event(f"markets -> {len(markets)}")

    def refresh_status(self) -> str:
        h = self.health
        if self._ws is None:
            h.status = "DISCONNECTED"
            return h.status
        # A quiet user channel is normal - no fills means no messages. Only
        # the heartbeat proves liveness, so staleness is measured against
        # PONG, not against trades.
        if self.pong_age() is None or self.pong_age() > self.stale_after:
            h.status = STALE
        else:
            h.status = LIVE
        with self._sub_lock:
            h.subscribed = tuple(sorted(self._sent))
        return h.status

    def pong_age(self) -> float | None:
        p = getattr(self, "_pong_at", None)
        return None if p is None else time.monotonic() - p

    # ------------------------------------------------------------ session
    async def _session(self) -> None:
        self._bind_intent_loop()
        if not self.authed:
            self.health.status = UNSYNCED
            self.health.detail = "no L2 credentials"
            self.event("no L2 API credentials - user channel idle", "warn")
            await asyncio.sleep(30)
            return
        # An omitted market filter subscribes to all activity for the API key.
        # Wait until discovery provides an explicit bot market instead of
        # contaminating this ledger with unrelated/manual trades.
        while not self.stopping:
            self._intent_event.clear()
            with self._sub_lock:
                has_markets = bool(self._want)
            if has_markets:
                break
            self.health.status = UNSYNCED
            self.health.detail = "waiting for explicit market filter"
            await self._intent_event.wait()
        if self.stopping:
            return
        async with websockets.connect(
            self.url, ssl=ws_ssl(self.url), ping_interval=None, close_timeout=2,
            max_size=2 ** 20, open_timeout=10, compression=None,
        ) as ws:
            self._ws = ws
            self.health.mark_connected()
            # TCP/TLS connect and a successful subscribe send are necessary
            # but not sufficient proof that the private channel is alive. The
            # first application PONG below is what makes execution ready.
            self._pong_at = None
            self._ping_sent = None
            self._pong_event.clear()
            self.health.status = UNSYNCED
            with self._sub_lock:
                wanted = list(self._want)
            sub = {"auth": {"apiKey": self._creds["apiKey"],
                            "secret": self._creds["secret"],
                            "passphrase": self._creds["passphrase"]},
                   "type": "user"}
            if wanted:
                sub["markets"] = wanted
            tasks = []
            try:
                await ws.send(json.dumps(sub))
                with self._sub_lock:
                    self._sent = set(wanted)
                    self._subscription_generation += 1
                self.event("connected + subscribed", "good")   # never logs the payload
                tasks = [asyncio.create_task(self._ping_loop(ws)),
                         asyncio.create_task(self._sub_loop(ws))]
                while not self.stopping:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.recv_timeout)
                    except asyncio.TimeoutError:
                        self.event("silent - reconnecting", "warn")
                        return
                    self._handle(raw)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._ws = None
                self._pong_at = None
                with self._sub_lock:
                    self._sent.clear()

    async def _sub_loop(self, ws) -> None:
        while True:
            self._intent_event.clear()
            with self._sub_lock:
                want = set(self._want)
                sent = set(self._sent)
            add = sorted(want - sent)
            if add:
                try:
                    await ws.send(json.dumps({"markets": add, "operation": "subscribe"}))
                    with self._sub_lock:
                        self._sent |= set(add)
                        self._subscription_generation += 1
                except Exception as exc:
                    self.health.mark_error(exc)
                    try:
                        await ws.close(code=1011, reason="subscription update failed")
                    except Exception as close_exc:
                        self.health.mark_error(close_exc)
                    return
            drop = sorted(sent - want)
            if drop:
                try:
                    await ws.send(json.dumps({"markets": drop,
                                              "operation": "unsubscribe"}))
                    with self._sub_lock:
                        self._sent -= set(drop)
                except Exception as exc:
                    self.health.mark_error(exc)
                    try:
                        await ws.close(code=1011, reason="subscription update failed")
                    except Exception as close_exc:
                        self.health.mark_error(close_exc)
                    return
            if add or drop:
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
            await asyncio.sleep(max(0.0, cycle_started + self.ping_every - time.monotonic()))

    def _handle(self, raw) -> None:
        self.health.mark_message()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        text = raw.strip()
        if text.upper() == "PONG":
            self._pong_at = time.monotonic()
            if self._ping_sent is not None:
                self.health.latency_ms = (self._pong_at - self._ping_sent) * 1000.0
            self._pong_event.set()
            return
        try:
            msg = json.loads(text)
        except Exception as exc:
            self.invalid_messages += 1
            self.health.detail = f"invalid websocket JSON: {type(exc).__name__}"
            return
        for item in (msg if isinstance(msg, list) else [msg]):
            if not isinstance(item, dict):
                self.invalid_messages += 1
                self.health.detail = "invalid user event: expected object"
                continue
            et = item.get("event_type")
            market = str(item.get("market") or "")
            with self._sub_lock:
                allowed = market in set(self._want)
            if et in ("trade", "order") and not allowed:
                self.invalid_messages += 1
                self.health.detail = "rejected user event outside subscribed markets"
                continue
            if et == "trade":
                trade_id = item.get("id")
                counted_before = self.store.trade_counts(trade_id)
                new = self.store.record_trade(
                    trade_id, order_id=item.get("taker_order_id"),
                    asset_id=item.get("asset_id"),
                    market=item.get("market"), side=item.get("side"),
                    price=item.get("price"), size=item.get("size"),
                    status=item.get("status"), source="user_ws",
                    fee_rate_bps=item.get("fee_rate_bps", item.get("feeRateBps")),
                    maker_orders=item.get("maker_orders"))
                counted_after = self.store.trade_counts(trade_id)
                if counted_after and not counted_before:
                    self.event(f"FILL {item.get('side')} {item.get('size')} @ "
                               f"{item.get('price')} [{item.get('status')}]", "good")
                elif new:
                    self.event(f"trade pending {str(trade_id)[:16]} "
                               f"[{item.get('status')}]", "info")
            elif et == "order":
                self.store.record_order(
                    item.get("id"), asset_id=item.get("asset_id"),
                    side=item.get("side"), price=item.get("price"),
                    original_size=item.get("original_size"),
                    size_matched=item.get("size_matched"),
                    state=item.get("type"))
                if str(item.get("type", "")).upper() == "CANCELLATION":
                    self.event(f"order cancelled {str(item.get('id'))[:12]}", "warn")
            else:
                self.invalid_messages += 1
                self.health.detail = f"ignored unknown user event {str(et)[:40]!r}"


def taker_paid(asset_id, maker_orders, size=None) -> float | None:
    """USDC the taker side of one trade exchanged, from its maker legs: each
    leg's matched shares at the price of the TAKER's token. A maker on the same
    token sold it at its price; a maker on the complementary token (a BUY of
    the other outcome, matched by minting) prices the other side, so ours is
    1 - its price. None when anything needed is missing or the legs do not add
    up to the trade's size - the caller then falls back to price x size."""
    return _legs_paid(taker_legs(asset_id, maker_orders, size))


def taker_legs(asset_id, maker_orders, size=None) -> tuple[tuple[float, float], ...] | None:
    """Validated executions in the taker's token, independent of maker ordering."""
    if not asset_id or not isinstance(maker_orders, list) or not maker_orders:
        return None
    shares = total = 0.0
    legs = []
    for m in maker_orders:
        if not isinstance(m, dict) or not m.get("asset_id"):
            return None
        sh, p = _f(m.get("matched_amount")), _price(m.get("price"))
        if sh is None or sh <= 0 or p is None:
            return None
        shares += sh
        # Decimal subtraction preserves a literal 0.90 maker price as 0.10
        # on the complementary token, avoiding fee-rounding boundary errors.
        taker_price = (p if str(m["asset_id"]) == str(asset_id)
                       else float(Decimal(1) - Decimal(str(p))))
        total += sh * taker_price
        legs.append((sh, taker_price))
        if not math.isfinite(shares) or not math.isfinite(total):
            return None
    if size is not None and not math.isclose(shares, float(size), rel_tol=1e-6, abs_tol=1e-6):
        return None
    return tuple(sorted(legs))


def _legs_paid(legs) -> float | None:
    if legs is None:
        return None
    total = sum(Decimal(str(sh)) * Decimal(str(price)) for sh, price in legs)
    return round(float(total), 6)


def _f(v):
    if isinstance(v, bool):
        return None
    try:
        parsed = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _price(v):
    parsed = _f(v)
    return parsed if parsed is not None and 0 < parsed < 1 else None


def _size(v):
    parsed = _f(v)
    return parsed if parsed is not None and parsed > 0 else None


def _size_or_zero(v):
    parsed = _f(v)
    return parsed if parsed is not None and parsed >= 0 else None


def _nonnegative(v):
    parsed = _f(v)
    return parsed if parsed is not None and parsed >= 0 else None


def _s(v):
    if v is None:
        return None
    out = str(v).strip()
    return out or None


def _side(v):
    side = str(v or "").upper()
    return side if side in ("BUY", "SELL") else None


def _has_invalid_numeric(price_raw, price, size_raw, size, fee_raw, fee) -> bool:
    return ((price_raw is not None and price is None)
            or (size_raw is not None and size is None)
            or (fee_raw is not None and fee is None))


def _status(v):
    status = str(v or "").upper()
    prefix = "TRADE_STATUS_"
    return status[len(prefix):] if status.startswith(prefix) else status
