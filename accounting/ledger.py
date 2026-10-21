"""Position ledger and PnL.

Everything here is built from fills the VENUE reported (user socket, or the
REST reconcile backup), never from what the bot intended to do. An order that
was sent is not a fill; a fill at the limit price is not a fill at the fill
price.

Accuracy rules:

* Realized PnL exists only for markets the venue has RESOLVED. Everything
  else is PENDING and reported separately. No settling on a BTC close.
* Unrealized is marked to the live BID (what you could actually get out at),
  not the mid, and is labelled as a mark rather than a result.
* Fees are charged on every fill, taker rate. A missing fee is not a zero fee.
* The ledger persists, so a restart does not lose dedup state and make the
  reconcile backup re-report every historical trade as newly recovered.
* `reconcile_balance()` checks the whole thing against the venue's own USDC
  balance movement. That is the only claim here that does not rest on our own
  bookkeeping being right.
"""
from __future__ import annotations

import json
import math
import operator
import os
import bisect
import copy
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field, fields
from decimal import Decimal, DecimalException
from types import SimpleNamespace

from . import fees as fee_mod
from durable_io import replace_atomic

# MATCHED and MINED are non-terminal. They can still transition to FAILED,
# so inventory and P&L must only use the terminal successful state.
COUNTED_STATUSES = ("CONFIRMED",)
RECOVERY_LOOKBACK_SECONDS = 2 * 60 * 60
MARKET_WINDOW_SECONDS = 5 * 60


def _fsync_directory(path) -> None:
    """Flush directory metadata after ``os.replace``.

    POSIX can open a directory fd and fsync it. Windows, and synced folders
    such as OneDrive, reject that open with PermissionError. The payload
    file is already fsynced and replaced; skip directory fsync there.
    """
    try:
        directory_fd = os.open(os.fspath(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    except OSError:
        return
    finally:
        os.close(directory_fd)


@dataclass
class Lot:
    trade_id: str
    token_id: str
    condition_id: str | None
    side: str
    shares: float
    price: float
    fee: float
    status: str
    wall: float
    source: str = ""
    order_id: str | None = None
    # Shares of this BUY lot already consumed by a later SELL. The lot itself
    # is never mutated away or removed: `lots` stays an append-only journal of
    # what actually happened, and FIFO basis is derived from what is left open.
    consumed: float = 0.0
    # The venue's headline price identifies a trade across WS/REST replays.
    # `price` is its cash-weighted execution price and drives inventory basis.
    reported_price: float | None = None

    @property
    def open_shares(self) -> float:
        return max(0.0, self.shares - self.consumed)

    @property
    def notional(self) -> float:
        return self.shares * self.price

    @property
    def cost(self) -> float:
        return self.notional + self.fee


@dataclass
class Position:
    token_id: str
    condition_id: str | None = None
    shares: float = 0.0
    cost: float = 0.0                 # notional + fees
    fees: float = 0.0
    lots: list = field(default_factory=list)
    settled: bool = False
    payout_per_share: float | None = None
    realized: float | None = None
    settled_wall: float | None = None
    # PnL already banked by selling before resolution, net of the fee paid on
    # the way out. Settlement adds this to the payout on whatever is still
    # held, so a partially-sold position reports one honest number.
    realized_from_sales: float = 0.0
    sell_fees: float = 0.0

    @property
    def avg_price(self) -> float | None:
        return (self.cost - self.fees) / self.shares if self.shares else None


class Ledger:
    # Must stay aligned with the explicit ``after`` bound in the CLOB REST
    # reconciliation query. This is a recent-loss recovery window, not an
    # unbounded historical wallet scan.
    RECOVERY_LOOKBACK_SECONDS = RECOVERY_LOOKBACK_SECONDS
    MARKET_WINDOW_SECONDS = MARKET_WINDOW_SECONDS

    def __init__(self, path: str | None = None, category: str = "crypto",
                 fee_resolver=None, allow_sells: bool = False) -> None:
        self._lock = threading.RLock()
        self._save_lock = threading.Lock()
        self._ingest_lock = threading.Lock()
        self.path = path or os.environ.get("LEDGER_PATH", "ledger.json")
        self.category = category
        self.fee_resolver = fee_resolver
        # Exits are opt-in. With this off the ledger keeps its original
        # contract - buy-and-hold only - so every existing caller behaves
        # exactly as before and a stray SELL still cannot enter PnL.
        self.allow_sells = bool(allow_sells)
        self.positions: dict[str, Position] = {}
        # key -> (fingerprint, JSON text) of each position as last saved; see
        # _position_texts_locked.
        self._pos_text: dict = {}
        # The sorted, encoded `seen` list, keyed by the set's identity, size and
        # a revision bumped on every add/discard (size alone is not enough: a
        # rolled-back fill followed by a different one leaves the size equal).
        self._seen_rev = 0
        self._seen_text = None
        self.seen: set[str] = set()          # trade ids, survives restart
        self.skipped_status: int = 0
        self.skipped_side: int = 0
        self.skipped_unauthorized: int = 0
        self.skipped_authorization_mismatch: int = 0
        self.duplicates: int = 0
        # order id -> non-secret placement metadata.  The metadata both
        # prevents a mismapped user-stream fill entering PnL and preserves
        # accepted round exposure across a restart.
        self.authorized_orders: dict[str, dict] = {}
        self.reported_unauthorized: set[str] = set()
        self.reported_mismatch: set[str] = set()
        self.opened_wall = time.time()
        # A path is only a location, not an identity.  Keep a random identity
        # inside the ledger so a paired paper account remains valid when its
        # directory is moved, while a missing/replaced ledger still fails
        # closed instead of silently resetting PnL.
        self.ledger_id = uuid.uuid4().hex
        # Hybrid sizing's realized-equity high-water mark belongs to the
        # account ledger, rather than a process or a resettable canary budget.
        # A worker establishes the epoch; durable P&L transitions retain its
        # peaks using the existing fill/settlement journal.
        self._hybrid_risk: dict | None = None
        self._hybrid_persisted: dict | None = None
        self._hybrid_realized_total: float | None = None
        self.schema_version: int | None = None
        self.loaded_from_disk = False
        self.balance_marks: list = []        # (wall, balance) from the venue
        self.last_persistence_error: str | None = None
        self.last_ingest_error: str | None = None
        self.integrity_error: str | None = None
        self.last_mark_error: str | None = None
        # Append-only journal beside the snapshot (audit F2). A fill or a
        # settlement appends one fsynced line instead of rewriting every
        # position the ledger ever held (188ms a fill at 50,000 positions).
        # The snapshot records the journal entry it includes (journal_seq);
        # load() replays the newer ones. Every JOURNAL_COMPACT_EVERY entries,
        # and on save(), a full snapshot is written and the journal emptied.
        self.journal_path = self.path + ".journal"
        self.journal_seq = 0                 # last journal entry applied
        self._journal_since = 0              # entries since the last snapshot
        self._journal_dirty = False          # a failed append: next write is a snapshot
        self.journal_replayed = 0            # entries replayed by the last load()
        # Indexes for the reads on the order path (audit C2): every one of
        # them used to walk every position the ledger ever held, under the
        # lock fills are booked with. Maintained by record_fill / settle /
        # load; rebuilt in full whenever `positions` was replaced or resized
        # behind the ledger's back (a test, a tool).
        #   _open_pos       token -> position that may still be open (settled
        #                   ones are dropped lazily, on the next read)
        #   _settled        (settled_wall, n, position), sorted: realized P/L
        #                   since a time without visiting older history
        #   _by_condition   condition -> its tokens
        #   _lot_by_trade   trade id -> lot
        self._open_pos: dict[str, Position] = {}
        self._settled: list = []
        self._settled_n = 0
        self._by_condition: dict[str, list[str]] = {}
        self._lot_by_trade: dict[str, Lot] = {}
        self._indexed = None
        # A fill or settlement whose journal write failed AFTER other threads
        # could see it (the state lock is not held across the disk flush):
        # the books may have been read in a state that was then rolled back.
        # LIVE halts new orders on it, like an integrity error.
        self.persistence_halt: str | None = None
        if not os.path.exists(self.path) and _journal_has_entries(self.journal_path):
            raise RuntimeError(
                f"ledger file is missing but its journal {self.journal_path!r} holds fills; "
                f"refusing to start with partial accounting (restore {self.path!r})")
        if os.path.exists(self.path) and not self.load():
            raise RuntimeError(
                f"ledger {self.path!r} exists but is unreadable or invalid; "
                "refusing to start with empty accounting")

    def authorize_order(self, order_id, receipt: dict | None = None, *,
                        venue_min_shares: float | None = None,
                        price_cap: float | None = None) -> bool:
        """Authorize one bot order and retain its non-secret risk metadata.

        ``venue_min_shares`` and ``price_cap`` snapshot the sizing limits used
        by the strategy.  Supplying them adds a conservative all-in cash
        reservation to the durable authorization, so a restart restores the
        same gross risk bound even while the accepted order is still pending.
        """
        oid = str(order_id or "")
        if not _safe_identifier(oid):
            return False
        metadata = _authorization_metadata(receipt)
        if venue_min_shares is not None or price_cap is not None:
            metadata = _with_authorization_reservation(
                metadata, venue_min_shares=venue_min_shares,
                price_cap=price_cap)
        _validate_authorization_metadata(metadata)
        with self._lock:
            return self._apply_authorization_locked(oid, metadata)

    def _apply_authorization_locked(self, oid: str, metadata: dict) -> bool:
        existing = self.authorized_orders.get(oid)
        if existing is not None and metadata and existing and existing != metadata:
            raise RuntimeError("order authorization metadata changed")
        was_new = existing is None
        if existing is None or (not existing and metadata):
            self.authorized_orders[oid] = metadata
        return was_new

    def authorize_order_durable(self, order_id, receipt: dict | None = None, *,
                                venue_min_shares: float | None = None,
                                price_cap: float | None = None) -> bool:
        """authorize_order, made durable by one fsynced journal line - not a
        rewrite of the whole ledger (which grew with every position ever
        held, under the lock the order path reads with). False when the order
        id is not safe to store; raises when the line cannot be written."""
        oid = str(order_id or "")
        with self._save_lock:
            self.authorize_order(order_id, receipt, venue_min_shares=venue_min_shares, price_cap=price_cap)
            with self._lock:
                metadata = self.authorized_orders.get(oid)
                if metadata is None:
                    return False
                entry = {"op": "authorize", "order_id": oid, "metadata": dict(metadata)}
            # Serialize writers, while inventory readers remain independent of
            # journal fsync and first-write/compaction snapshot I/O.
            if not self._persist_locked(entry):
                raise RuntimeError("authorized order could not be persisted")
            return True

    def authorized_notional_for_window(self, window_start: int) -> float:
        """Raw accepted order notional (legacy reporting compatibility)."""
        end = int(window_start) + 300
        with self._lock:
            total = sum(
                float(meta.get("requested_notional") or 0.0)
                for meta in self.authorized_orders.values()
                if isinstance(meta, dict) and int(meta.get("window_end") or 0) == end
            )
            if not math.isfinite(total) or total < 0:
                raise RuntimeError("authorized order exposure is invalid")
            return total

    def authorized_cost_for_window(self, window_start: int) -> float:
        """Conservative all-in accepted exposure, including pending orders.

        New authorizations carry an explicit reservation based on the venue
        minimum, the strategy price cap, and the venue's fee rate.  Older V3/V4
        authorizations remain usable when their durable fee rate proves a safe
        upper bound.  A row for this window without either proof fails closed;
        counting only its requested notional could admit another order after a
        restart even though fees have already consumed the remaining budget.
        """
        end = int(window_start) + self.MARKET_WINDOW_SECONDS
        with self._lock:
            total = 0.0
            for metadata in self.authorized_orders.values():
                if (not isinstance(metadata, dict)
                        or int(metadata.get("window_end") or 0) != end):
                    continue
                total += _conservative_authorization_cost(metadata)
            if not math.isfinite(total) or total < 0:
                raise RuntimeError("authorized order all-in exposure is invalid")
            return total

    def authorized_tokens_for_window(self, window_start: int) -> frozenset[str]:
        """Token legs accepted in a live round, including pending fills."""
        end = int(window_start) + self.MARKET_WINDOW_SECONDS
        with self._lock:
            return frozenset(
                str(metadata["token_id"])
                for metadata in self.authorized_orders.values()
                if (metadata
                    and int(metadata.get("window_end") or 0) == end
                    and _safe_identifier(metadata.get("token_id")))
            )

    def confirmed_notional_for_condition(self, condition_id: str | None) -> float:
        condition = str(condition_id or "")
        if not condition:
            return 0.0
        with self._lock:
            return sum(
                lot.notional for position in self._condition_positions_locked(condition)
                for lot in position.lots
            )

    def confirmed_cost_for_condition(self, condition_id: str | None) -> float:
        """Actual confirmed PAPER cash exposure, including every fill fee."""
        condition = str(condition_id or "")
        if not condition:
            return 0.0
        with self._lock:
            total = sum(
                lot.cost for position in self._condition_positions_locked(condition)
                for lot in position.lots
            )
            if not math.isfinite(total) or total < 0:
                raise RuntimeError("confirmed all-in exposure is invalid")
            return total

    def unsettled_cost(self) -> float:
        """Cash committed to positions the venue has not resolved yet.

        This is capital that is neither spendable nor lost - it returns in
        full at settlement, or converts to a payout. It spans every open
        round, which is exactly what per-round exposure cannot see: when
        venue resolution stalls, each new round passes its own budget check
        while the total frozen across rounds keeps climbing. Observed doing
        precisely that - 20 open rounds holding 103% of a $300 wallet while
        settlement was backed up upstream.
        """
        with self._lock:
            total = sum(position.cost for position in self._open_positions_locked()
                        if position.shares > 0)
        if not math.isfinite(total) or total < 0:
            raise RuntimeError("unsettled exposure is invalid")
        return float(total)

    def held_tokens_for_condition(self, condition_id: str | None) -> frozenset[str]:
        """Unsettled paper inventory legs for one discovered condition."""
        condition = str(condition_id or "")
        if not condition:
            return frozenset()
        with self._lock:
            return frozenset(
                position.token_id for position in self._open_positions_locked()
                if position.condition_id == condition and position.shares > 0
            )

    def open_leg_shares(self, condition_id: str | None,
                        token_id: str | None) -> float:
        """Confirmed open shares of one leg, or 0.0.

        An exit has to be sized from what is actually owned. Selling a
        requested or authorized quantity instead is how a paper run invents
        inventory it never held and a live one is rejected by the venue.
        """
        condition = str(condition_id or "")
        token = str(token_id or "")
        if not condition or not token:
            return 0.0
        with self._lock:
            position = self.positions.get(token)       # positions are keyed by their token
            if (position is None or position.condition_id != condition
                    or position.token_id != token or position.settled or position.shares <= 0):
                return 0.0
            shares = float(position.shares)
            return shares if math.isfinite(shares) and shares > 0 else 0.0

    def open_leg_basis(self, condition_id: str | None,
                       token_id: str | None) -> tuple[float, float] | None:
        """Entry price and paid fee-per-share for one unsettled leg.

        Returns ``(avg_price, fee_per_share)`` with the price excluding fees,
        or ``None`` when this condition holds no open shares of that token.
        The pair-lock guard needs both halves: what the held leg cost and the
        fee already sunk into it, because a matched pair only redeems for
        $1.00 and every cent paid on either leg comes out of that.
        """
        condition = str(condition_id or "")
        token = str(token_id or "")
        if not condition or not token:
            return None
        with self._lock:
            for position in (self.positions.get(token),):     # positions are keyed by their token
                if (position is None or position.condition_id != condition
                        or position.token_id != token
                        or position.settled or position.shares <= 0):
                    continue
                price = position.avg_price
                fee_per_share = position.fees / position.shares
                # A basis the guard cannot trust must read as "no basis", never
                # as a cheap one: the caller treats None as a refusal.
                if (price is None or not math.isfinite(price) or price <= 0
                        or not math.isfinite(fee_per_share)
                        or fee_per_share < 0):
                    return None
                return float(price), float(fee_per_share)
        return None

    def recovery_conditions(self, *, now: float | None = None,
                            lookback_s: float = RECOVERY_LOOKBACK_SECONDS,
                            ) -> tuple[str, ...]:
        """Return recent condition IDs that this ledger can authenticate.

        The feed hub only remembers markets discovered in the current process.
        After a restart that list begins empty, but V3+ order authorizations
        durably retain the condition whose fills the bot is allowed to count.
        Those conditions are therefore the authoritative restart-recovery
        filters. The default horizon is explicitly two hours, matching the
        venue query in ``run_feeds.fetch_trades``. A condition is retained only
        while its full five-minute order window fits inside that lookback;
        older authorizations expire from this recovery path. Metadata-free
        legacy authorizations are deliberately skipped: they cannot prove a
        market boundary and must not broaden a wallet query.
        """
        sampled = time.time() if now is None else float(now)
        horizon = float(lookback_s)
        if (not math.isfinite(sampled) or sampled <= 0
                or not math.isfinite(horizon) or horizon <= 0):
            raise ValueError("recovery time and lookback must be finite and positive")
        # REST's `after` is measured from trade time, while authorization keeps
        # round end. Require round_start = window_end - 300 to be within the
        # same strict lookback so selecting a condition never promises rows the
        # query is already incapable of returning.
        cutoff = sampled - horizon + self.MARKET_WINDOW_SECONDS
        with self._lock:
            candidates = []
            for metadata in self.authorized_orders.values():
                if not metadata:
                    # V2 authorizations contain only the order id.  Querying
                    # without a condition would admit unrelated wallet trades.
                    continue
                condition = metadata.get("condition_id")
                window_end = metadata.get("window_end")
                if (not _safe_identifier(condition)
                        or isinstance(window_end, bool)
                        or not isinstance(window_end, (int, float))
                        or not math.isfinite(float(window_end))
                        or float(window_end) < cutoff):
                    continue
                candidates.append((float(window_end), str(condition)))

        # Sorting makes the REST filter set stable across JSON reloads and
        # concurrent authorization insertion order.  Deduplicate conditions
        # because one order can have multiple venue fill records.
        ordered: list[str] = []
        seen: set[str] = set()
        for _window_end, condition in sorted(candidates):
            if condition not in seen:
                seen.add(condition)
                ordered.append(condition)
        return tuple(ordered)

    def has_seen_trade(self, trade_id) -> bool:
        """Thread-safe durable dedup check for restart reconciliation."""
        tid = str(trade_id or "")
        with self._lock:
            return tid in self.seen

    def rest_trade_is_booked(self, row: dict) -> bool:
        """Verify that a REST replay agrees with its durable ledger lot.

        Returning ``True`` lets reconciliation suppress an ordinary restart
        replay. A reused trade ID with contradictory terminal status or
        identity is accounting corruption and raises instead of being hidden
        by ID-only deduplication.
        """
        if not isinstance(row, dict):
            return False
        trade_id = str(row.get("id") or row.get("trade_id") or "")
        if not trade_id:
            return False
        with self._lock:
            lot = self._lot_for_trade_locked(trade_id, row.get("asset_id"))
            authorization = copy.deepcopy(self.authorized_orders.get(lot.order_id, {})) if lot else {}
        if lot is None:
            return False

        conflicts = []

        def compare_text(label, raw, expected, *, upper=False):
            if raw in (None, ""):
                return
            actual = str(raw)
            wanted = str(expected or "")
            if upper:
                actual, wanted = actual.upper(), wanted.upper()
            if actual != wanted:
                conflicts.append(label)

        compare_text("token", row.get("asset_id"), lot.token_id)
        compare_text("condition", row.get("market"), lot.condition_id)
        compare_text("side", row.get("side"), lot.side, upper=True)
        compare_text("order", row.get("taker_order_id")
                     or row.get("takerOrderId"), lot.order_id)
        status = str(row.get("status") or "").upper()
        if status.endswith("FAILED") or status.endswith("CONFLICT"):
            conflicts.append("terminal status")
        for label, raw, expected in (
                ("price", row.get("price"),
                 lot.price if lot.reported_price is None else lot.reported_price),
                ("size", row.get("size"), lot.shares)):
            if raw in (None, ""):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError, OverflowError):
                conflicts.append(label)
                continue
            if (not math.isfinite(value)
                    or not math.isclose(value, float(expected),
                                        rel_tol=1e-9, abs_tol=1e-9)):
                conflicts.append(label)
        try:
            conflicts.extend(_replay_execution_conflicts(row, lot, authorization, self.category))
        except (TypeError, ValueError, OverflowError, DecimalException) as exc:
            conflicts.append(f"invalid execution evidence: {exc}")
        if conflicts:
            message = (f"REST replay conflicts with durable fill {trade_id[-12:]} "
                       f"({', '.join(conflicts)})")[:1000]
            with self._lock:
                # Successful later reads cannot erase an unresolved discrepancy.
                self.integrity_error = self.integrity_error or message
            raise RuntimeError(message)
        return True

    # -------------------------------------------------------------- ingest
    def record_fill(self, trade_id, token_id, *, shares, price, side="BUY",
                    condition_id=None, status="CONFIRMED", theta=None,
                    source="user_ws", fee=None, fee_exponent: int = 1,
                    order_id=None, wall: float | None = None,
                    reported_price: float | None = None) -> bool:
        """Idempotent. Returns True only if this trade id was new. `wall` is
        the lot's time; only a journal replay passes it (the original one)."""
        try:
            lot_wall = time.time() if wall is None else float(wall)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(lot_wall) or lot_wall <= 0:
            return False
        tid = str(trade_id or "")
        token = str(token_id or "")
        condition = _s(condition_id)
        order_side = str(side or "").upper()
        source_text = str(source or "")
        order_text = _s(order_id)
        if (not _safe_identifier(tid) or not _safe_identifier(token)
                or not _safe_identifier(condition)
                or len(source_text) > 256 or not source_text.isprintable()
                or (order_text is not None and not _safe_identifier(order_text))):
            return False
        st = str(status or "").upper()
        with self._lock:
            self._check_index_locked()
            if tid in self.seen:
                self.duplicates += 1
                return False
            if st not in COUNTED_STATUSES:
                # RETRYING is not a fill yet; FAILED never was one.
                self.skipped_status += 1
                return False
            if order_side == "SELL":
                if not self.allow_sells:
                    self.skipped_side += 1
                    return False
            elif order_side != "BUY":
                # Treating an unrelated order as newly acquired long shares
                # inverts PnL, so anything that is not a recognised side is
                # refused outright.
                self.skipped_side += 1
                return False
            try:
                sh, px = float(shares), float(price)
            except (TypeError, ValueError, OverflowError, DecimalException):
                return False
            if not math.isfinite(sh) or not math.isfinite(px) or sh <= 0 or not (0.0 < px <= 1.0):
                return False
            try:
                headline = None if reported_price is None else float(reported_price)
            except (TypeError, ValueError, OverflowError):
                return False
            if (isinstance(reported_price, bool)
                    or (headline is not None
                        and (not math.isfinite(headline) or not 0 < headline <= 1))):
                return False
            try:
                if fee is None and (type(fee_exponent) is not int or not 1 <= fee_exponent <= 8):
                    return False
                f = (fee_mod.taker_fee(
                    sh, px, theta, self.category, exponent=fee_exponent)
                     if fee is None else float(fee))
            except (TypeError, ValueError, OverflowError, DecimalException):
                return False
            if not math.isfinite(f) or f < 0:
                return False
            pos = self.positions.get(token)
            # A rejected exit must not create an empty position or reopen a
            # settled one. Check inventory before changing either state.
            if order_side == "SELL":
                available = (sum(l.open_shares for l in pos.lots
                                 if str(l.side or "").upper() == "BUY") if pos else 0.0)
                if (pos is None or available <= 0 or pos.shares <= 0
                        or sh > available + 1e-9 or sh > pos.shares + 1e-9):
                    self.skipped_side += 1
                    return False
                notional_basis, fee_basis, shortfall = _consume_fifo(pos, sh, consume=False)
                basis = notional_basis + fee_basis
                proceeds = sh * px - f
                if shortfall > 1e-9 or any(not math.isfinite(value) for value in (
                        proceeds, basis, pos.shares - sh, pos.cost - basis,
                        pos.fees - fee_basis, pos.sell_fees + f,
                        pos.realized_from_sales + proceeds - basis)):
                    return False
            elif (not math.isfinite(sh * px + f)
                  or (pos is not None and any(not math.isfinite(value) for value in
                      (pos.shares + sh, pos.cost + sh * px + f, pos.fees + f)))):
                return False
            if pos is None:
                pos = self.positions[token] = Position(
                    token_id=token, condition_id=condition)
            if pos.condition_id and pos.condition_id != condition:
                return False
            if not pos.condition_id:
                pos.condition_id = condition
            hybrid_before = _hybrid_position_realized(pos) if self._hybrid_risk is not None else 0.0
            if pos.settled:
                # A fill arriving after settlement means our settlement was
                # premature. Surface it rather than folding it in silently.
                pos.settled = False
                pos.payout_per_share = None
                pos.realized = None
                pos.settled_wall = None
            if order_side == "SELL":
                # An exit can only ever reduce inventory we actually hold.
                # Probing FIFO first means a short is refused before any lot
                # is mutated, so a rejected sell leaves the ledger untouched.
                notional_basis, fee_basis, shortfall = _consume_fifo(pos, sh)
                if shortfall > 1e-9:
                    # Unreachable given the check above; refuse rather than
                    # book a partially-consumed sale if it ever is reached.
                    self.skipped_side += 1
                    return False
                basis = notional_basis + fee_basis
                proceeds = sh * px - f
                pos.lots.append(Lot(tid, token, condition, order_side,
                                    sh, px, f, st, lot_wall, source_text,
                                    order_text, reported_price=headline))
                pos.shares -= sh
                pos.cost -= basis
                pos.fees -= fee_basis
                pos.sell_fees += f
                pos.realized_from_sales += proceeds - basis
                if pos.shares <= 1e-9:
                    # Nothing left to resolve. Close it here, or settlement -
                    # which only visits positions still holding shares - would
                    # never report the PnL banked by the sales.
                    pos.shares = 0.0
                    pos.cost = 0.0
                    pos.fees = 0.0
                    pos.settled = True
                    pos.payout_per_share = None
                    pos.realized = pos.realized_from_sales
                    pos.settled_wall = time.time()
                self.seen.add(tid)
                self._seen_rev += 1
                self._index_fill_locked(pos)
                self._observe_hybrid_delta_locked(_hybrid_position_realized(pos) - hybrid_before)
                return True
            pos.lots.append(Lot(tid, token, condition, order_side,
                                sh, px, f, st, lot_wall, source_text,
                                order_text, reported_price=headline))
            pos.shares += sh
            pos.fees += f
            pos.cost += sh * px + f
            self.seen.add(tid)
            self._seen_rev += 1
            self._index_fill_locked(pos)
            self._observe_hybrid_delta_locked(_hybrid_position_realized(pos) - hybrid_before)
            return True

    def record_fill_durable(self, trade_id, token_id, **kwargs) -> bool:
        """Record one fill and its dedup key as a single durable transaction.

        A confirmed venue fill must not exist only in RAM: a crash in that
        state causes the REST/user-stream replay to be counted differently
        after restart.  The save lock is acquired before the ledger lock, the
        same order used by :meth:`save` and :meth:`settle`, so persistence,
        fills, and settlement cannot interleave into an impossible snapshot.
        """
        token = str(token_id or "")
        tid = str(trade_id or "")
        with self._save_lock:
            with self._lock:
                self._check_index_locked()
                position = self.positions.get(token)
                # A BUY changes scalar fields and appends one lot; it never
                # changes earlier lots. Deep-copying the entire history here
                # made each journal append O(history), holding the ledger and
                # the GIL for tens of milliseconds. Keep an exact undo record
                # instead. SELL additionally changes FIFO consumption, which
                # is restored separately if the durable write fails.
                previous = None if position is None else position.__dict__.copy()
                previous_hybrid = (None if self._hybrid_risk is None else dict(self._hybrid_risk))
                previous_hybrid_realized = self._hybrid_realized_total
                previous_length = 0 if position is None else len(position.lots)
                consumed = []
                if position is not None and str(kwargs.get("side", "BUY")).upper() == "SELL":
                    consumed = [(lot, lot.consumed) for lot in position.lots
                                if str(lot.side or "").upper() == "BUY" and lot.open_shares > 0]
                inserted = self.record_fill(trade_id, token_id, **kwargs)
                if not inserted:
                    return False
                lot = self.positions[token].lots[-1]
                entry = {
                    "op": "fill", "trade_id": lot.trade_id, "token_id": lot.token_id,
                    "condition_id": lot.condition_id, "side": lot.side, "shares": lot.shares,
                    "price": lot.price, "fee": lot.fee, "status": lot.status, "wall": lot.wall,
                    "source": lot.source, "order_id": lot.order_id,
                    "reported_price": lot.reported_price}
            # The disk flush runs with only the save lock held - every other
            # writer (fills, settlement, snapshots) still waits for it, but the
            # order path's reads do not wait for the disk (audit C2).
            if self._persist_locked(entry):
                return True
            with self._lock:
                if previous is not None:
                    del position.lots[previous_length:]
                    for old_lot, old_consumed in consumed:
                        old_lot.consumed = old_consumed
                    position.__dict__.update(previous)
                else:
                    self.positions.pop(token, None)
                self.seen.discard(tid)
                self._seen_rev += 1
                self._reindex_locked()
                self._hybrid_risk = previous_hybrid
                self._hybrid_realized_total = previous_hybrid_realized
                # Readers could see this fill before the write failed.
                self.persistence_halt = self.persistence_halt or (
                    f"fill {tid[-12:]} could not be made durable after it was booked: "
                    f"{self.last_persistence_error or 'journal write failed'}")[:300]
            raise RuntimeError("confirmed fill could not be persisted")

    def ingest_fill_store(self, store) -> int:
        """Pull everything from the feeds FillStore. Safe to call repeatedly."""
        # Cumulative order limits and the subsequent durable fill must observe
        # one ingestion at a time, including concurrent reconciliation callers.
        with self._ingest_lock:
            return self._ingest_fill_store(store)

    def _ingest_fill_store(self, store) -> int:
        n = 0
        store_lock = getattr(store, "_lock", None)
        if store_lock is None:
            fills = list(store.fills.values())
        else:
            with store_lock:
                # Copy the records, not just the dict values: the feed mutates
                # lifecycle objects in place during reconnect/reconcile.
                fills = [copy.deepcopy(fill) for fill in store.fills.values()]
        for fill in fills:
            trade_id = str(getattr(fill, "trade_id", None) or "")
            order_id = str(getattr(fill, "order_id", None) or "")
            if self.has_seen_trade(trade_id):
                # Verify identity before suppressing a durable replay. Adding
                # the same fill to cumulative authorization a second time used
                # to falsely reject every fully filled order on the next poll.
                self.rest_trade_is_booked({
                    "id": trade_id, "taker_order_id": order_id,
                    "asset_id": fill.asset_id, "market": fill.market,
                    "side": fill.side, "price": fill.price,
                    "size": fill.size, "status": fill.status,
                    "paid": getattr(fill, "paid", None),
                    "maker_legs": getattr(fill, "maker_legs", None)})
                continue
            # Pending new trades do not count. A durable trade is checked
            # above even if a later feed copy has become FAILED or CONFLICT.
            if str(fill.status or "").upper() not in COUNTED_STATUSES:
                continue
            with self._lock:
                authorization = copy.deepcopy(self.authorized_orders.get(order_id))
            if not order_id or authorization is None:
                with self._lock:
                    if trade_id not in self.reported_unauthorized:
                        self.skipped_unauthorized += 1
                        self.reported_unauthorized.add(trade_id)
                continue
            if not self._fill_matches_authorization(fill, order_id, authorization):
                with self._lock:
                    if trade_id not in self.reported_mismatch:
                        self.skipped_authorization_mismatch += 1
                        self.reported_mismatch.add(trade_id)
                continue
            # V2's trade ``fee_rate_bps`` is a legacy/base-fee field and is
            # commonly zero even when the public market ``fd`` curve is
            # enabled. Only the CLOB market-info cache is authoritative.
            live_theta = authorization.get("fee_rate") if authorization else None
            fee_exponent = int((authorization or {}).get("fee_exponent") or 1)
            if live_theta is None and self.fee_resolver is not None:
                try:
                    resolved = self.fee_resolver(fill.asset_id)
                    if isinstance(resolved, dict):
                        live_theta = resolved.get("rate")
                        fee_exponent = int(resolved.get("exponent") or 1)
                    elif isinstance(resolved, (tuple, list)) and len(resolved) >= 2:
                        live_theta, fee_exponent = resolved[0], int(resolved[1])
                    else:
                        live_theta = resolved
                    self.last_ingest_error = None
                except Exception as exc:
                    self.last_ingest_error = (
                        f"fee resolver failed for token {str(fill.asset_id)[-12:]}: "
                        f"{type(exc).__name__}: {exc}")[:300]
            price, actual_fee = _fill_execution_values(
                fill, live_theta, self.category, fee_exponent)
            if self.record_fill_durable(
                    fill.trade_id, fill.asset_id, shares=fill.size,
                    price=price, reported_price=fill.price, side=fill.side,
                    condition_id=fill.market, status=fill.status,
                    fee=actual_fee, theta=live_theta, fee_exponent=fee_exponent,
                    order_id=order_id,
                    source="+".join(sorted(fill.sources)) or "user_ws"):
                n += 1
        return n

    def _fill_matches_authorization(self, fill, order_id: str,
                                    authorization: dict) -> bool:
        """Reject wrong-market/token and overfilled lifecycle records."""
        # Old v2 ledgers contain only an order id and cannot prove which
        # market/token/notional the bot authorized.  Accepting such a fill can
        # turn an unrelated wallet trade into bot inventory, so fail closed.
        if not authorization:
            return False
        if (str(fill.asset_id or "") != str(authorization.get("token_id") or "")
                or str(fill.market or "") != str(authorization.get("condition_id") or "")
                or str(fill.side or "").upper() != "BUY"):
            return False
        try:
            requested = float(authorization["requested_notional"])
            # What the trade really cost, from its maker legs when the feed carried
            # them: one taker match can cross several price levels and the venue's
            # headline price is not their average, so price x size overstated a
            # $1.00 buy of 10.322 shares as $1.0322 and refused our own fill.
            paid = getattr(fill, "paid", None)
            candidate = float(paid) if paid is not None else float(fill.size) * float(fill.price)
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
        with self._lock:
            # an order's lots live in its own token's position
            position = self.positions.get(str(authorization.get("token_id") or ""))
            already = sum(
                lot.notional for lot in (position.lots if position is not None else ())
                if lot.order_id == order_id
            )
        return (math.isfinite(candidate) and candidate > 0
                and already + candidate <= requested + 0.02)

    # ------------------------------------------------------------- settle
    def settle(self, resolution) -> list:
        """Apply a venue resolution. Only RESOLVED settles anything."""
        if not getattr(resolution, "resolved", False):
            return []
        condition, payouts = _validated_resolution(resolution)
        done: list[Position] = []
        # Save lock first is the global lock order. Holding the ledger lock
        # through the atomic replace prevents a late fill from reopening a
        # position between mutation and persistence.
        with self._save_lock:
            with self._lock:
                targets = [
                    pos for pos in self.positions.values()
                    if not pos.settled and pos.shares > 0
                    and pos.condition_id == condition
                ]
                if not targets:
                    return []
                missing = [pos.token_id for pos in targets if pos.token_id not in payouts]
                if missing:
                    raise ValueError(
                        f"resolution omitted held token(s): {', '.join(missing[:3])}")
                settled_wall = time.time()
                previous_hybrid = (None if self._hybrid_risk is None else dict(self._hybrid_risk))
                previous_hybrid_realized = self._hybrid_realized_total
                hybrid_before = math.fsum(_hybrid_position_realized(pos) for pos in targets)
                for pos in targets:
                    self._settle_position_locked(pos, payouts[pos.token_id], settled_wall)
                    self._note_settled_locked(pos)
                    done.append(pos)
                # A resolution settles all legs atomically: a winning hedge
                # leg alone must not create an intermediate equity peak.
                self._observe_hybrid_delta_locked(
                    math.fsum(_hybrid_position_realized(pos) for pos in done) - hybrid_before)
                if not self._persist_locked({
                        "op": "settle", "condition_id": condition, "wall": settled_wall,
                        "payouts": {pos.token_id: payouts[pos.token_id] for pos in done},
                        "tokens": [pos.token_id for pos in done]}):
                    # Do not leave volatile state claiming settlement that was
                    # not durably recorded. The worker retries the resolution.
                    for pos in done:
                        pos.payout_per_share = None
                        pos.realized = None
                        pos.settled = False
                        pos.settled_wall = None
                        self._open_pos[pos.token_id] = pos
                    self._hybrid_risk = previous_hybrid
                    self._hybrid_realized_total = previous_hybrid_realized
                    raise RuntimeError("settlement could not be persisted")
        return done

    @staticmethod
    def _settle_position_locked(pos, pay: float, settled_wall: float) -> None:
        pos.payout_per_share = pay
        pos.realized = (pos.shares * pay - pos.cost + pos.realized_from_sales)
        pos.settled = True
        pos.settled_wall = settled_wall

    # ------------------------------------------------------------- journal
    JOURNAL_COMPACT_EVERY = 500

    def _persist_locked(self, entry: dict) -> bool:
        """Make one mutation durable; the caller holds _save_lock (every writer
        does, so seq and the journal are its alone) - and may hold _lock, or
        not. One journal line when a snapshot is on disk to append to, else
        (the ledger's first write, or after a failed append) a full snapshot."""
        if not self.loaded_from_disk or self._journal_dirty or not os.path.exists(self.path):
            # Snapshot-backed mutations need a revision too. Without this,
            # an initial cash/reference read cannot detect an intervening
            # first fill merely because that fill created the snapshot.
            previous_seq = self.journal_seq
            self.journal_seq += 1
            if self._snapshot_locked():
                return True
            self.journal_seq = previous_seq
            return False
        entry = {"seq": self.journal_seq + 1, **entry}
        if not self._append_journal(entry):
            self._journal_dirty = True
            return False
        self.journal_seq += 1
        self._journal_since += 1
        with self._lock:
            self._hybrid_persisted = (None if self._hybrid_risk is None else dict(self._hybrid_risk))
        if self._journal_since >= self.JOURNAL_COMPACT_EVERY:
            self._snapshot_locked()          # best effort: the journal still holds everything
        return True

    def _append_journal(self, entry: dict) -> bool:
        try:
            line = (json.dumps(entry, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")
            created = not os.path.exists(self.journal_path)
            fd = os.open(self.journal_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0),
                         0o600)
            try:
                view = memoryview(line)
                while view:
                    n = os.write(fd, view)
                    view = view[n:]
                os.fsync(fd)
            finally:
                os.close(fd)
            if created:
                _fsync_directory(os.path.dirname(os.path.abspath(self.journal_path)) or ".")
            self.last_persistence_error = None
            return True
        except Exception as exc:
            self.last_persistence_error = (
                f"ledger journal append failed: {type(exc).__name__}: {exc}")[:300]
            return False

    def _snapshot_locked(self) -> bool:
        """A full snapshot (it includes every journal entry so far), then an
        empty journal. A crash between the two is harmless: load() replays
        only entries newer than the snapshot's journal_seq. The caller holds
        _save_lock; the state lock is taken only to build the payload."""
        with self._lock:
            payload = self._payload_locked()
        if not self._write_payload(payload):
            return False
        self._truncate_journal()
        return True

    def _truncate_journal(self) -> None:
        try:
            if os.path.exists(self.journal_path):
                with open(self.journal_path, "r+b") as fh:
                    fh.truncate(0)
                    fh.flush()
                    os.fsync(fh.fileno())
            self._journal_since = 0
            self._journal_dirty = False
        except OSError as exc:
            self._journal_dirty = True         # stale entries are covered by journal_seq anyway
            self.last_persistence_error = (
                f"ledger journal truncate failed: {type(exc).__name__}: {exc}")[:300]

    def journal_pending(self) -> int:
        return self._journal_since

    def _replay_journal(self, after_seq: int) -> int:
        """Apply the journal's entries newer than the snapshot. Every entry is
        parsed and checked first; a damaged entry anywhere but a torn final
        line (an append that never completed, so was never acknowledged)
        fails the load - the caller then refuses to start."""
        if not os.path.exists(self.journal_path):
            return 0
        with open(self.journal_path, "rb") as fh:
            raw = fh.read()
        lines = raw.split(b"\n")
        torn = lines.pop()                    # b"" when the file ends with a newline
        entries = []
        for i, line in enumerate(lines):
            if not line.strip():
                raise ValueError("blank line inside the ledger journal")
            entries.append(_journal_entry(json.loads(line.decode("utf-8"), parse_constant=_reject_json_constant)))
        if torn.strip():
            try:
                json.loads(torn.decode("utf-8"))
                raise ValueError("ledger journal ends without a newline")
            except ValueError as exc:
                if "ends without" in str(exc):
                    raise
                # a torn final append: never acknowledged, dropped
        newer = [e for e in entries if e["seq"] > after_seq]
        expected = after_seq + 1
        for e in newer:
            if e["seq"] != expected:
                raise ValueError(f"ledger journal gap: entry {e['seq']} where {expected} was expected")
            expected += 1
        applied = 0
        for e in newer:
            if e["op"] == "authorize":
                metadata = _authorization_metadata(e["metadata"])
                _validate_authorization_metadata(metadata)
                if not _safe_identifier(e["order_id"]):
                    raise ValueError(f"ledger journal authorization {e['seq']} has an unsafe order id")
                self._apply_authorization_locked(e["order_id"], metadata)
            elif e["op"] == "fill":
                if not self.record_fill(e["trade_id"], e["token_id"], shares=e["shares"], price=e["price"],
                                        side=e["side"], condition_id=e["condition_id"], status=e["status"],
                                        source=e["source"], fee=e["fee"], order_id=e["order_id"],
                                        wall=e["wall"], reported_price=e.get("reported_price")):
                    raise ValueError(f"ledger journal fill {e['seq']} does not apply")
            else:
                targets = [p for p in self.positions.values()
                           if not p.settled and p.shares > 0 and p.condition_id == e["condition_id"]]
                if sorted(p.token_id for p in targets) != sorted(e["tokens"]):
                    raise ValueError(f"ledger journal settlement {e['seq']} does not match the positions")
                hybrid_before = math.fsum(_hybrid_position_realized(p) for p in targets)
                for p in targets:
                    self._settle_position_locked(p, float(e["payouts"][p.token_id]), float(e["wall"]))
                    self._note_settled_locked(p)
                self._observe_hybrid_delta_locked(
                    math.fsum(_hybrid_position_realized(p) for p in targets) - hybrid_before)
            self.journal_seq = e["seq"]
            applied += 1
        return applied

    # -------------------------------------------------------------- indexes
    def _reindex_locked(self) -> None:
        """Rebuild every index from `positions` (load, a rollback, or state
        changed behind the ledger's back)."""
        self._open_pos, self._settled, self._settled_n = {}, [], 0
        self._by_condition, self._lot_by_trade = {}, {}
        for key, p in self.positions.items():
            self._by_condition.setdefault(str(p.condition_id), []).append(key)
            if p.settled:
                self._note_settled_locked(p)
            else:
                self._open_pos[key] = p
            for lot in p.lots:
                self._lot_by_trade[lot.trade_id] = lot
        self._indexed = (id(self.positions), len(self.positions))

    def _check_index_locked(self) -> None:
        if self._indexed != (id(self.positions), len(self.positions)):
            self._reindex_locked()

    def _index_fill_locked(self, pos: Position) -> None:
        """record_fill just booked pos.lots[-1] (and may have created, reopened
        or closed pos)."""
        key = pos.token_id
        tokens = self._by_condition.setdefault(str(pos.condition_id), [])
        if key not in tokens:
            tokens.append(key)
        if pos.settled:
            self._note_settled_locked(pos)
        else:
            self._open_pos[key] = pos
        lot = pos.lots[-1]
        self._lot_by_trade[lot.trade_id] = lot
        self._indexed = (id(self.positions), len(self.positions))

    def _note_settled_locked(self, pos: Position) -> None:
        wall = pos.settled_wall
        try:
            wall = float(wall)
        except (TypeError, ValueError):
            return
        self._settled_n += 1
        bisect.insort(self._settled, (wall, self._settled_n, pos), key=lambda e: (e[0], e[1]))

    def _open_positions_locked(self) -> list:
        """The positions still unsettled. Settled ones leave the index here,
        however they were settled."""
        self._check_index_locked()
        out, gone = [], []
        for key, p in self._open_pos.items():
            if p.settled:
                gone.append(key)
            else:
                out.append(p)
        for key in gone:
            p = self._open_pos.pop(key)
            if self.positions.get(key) is p:
                self._note_settled_locked(p)
        return out

    def _condition_positions_locked(self, condition: str) -> list:
        self._check_index_locked()
        return [self.positions[t] for t in self._by_condition.get(condition, ())
                if t in self.positions and self.positions[t].condition_id == condition]

    def _lot_for_trade_locked(self, trade_id: str, token=None):
        """The booked lot of one trade: its token's position first, then the
        trade index, then (state changed behind the ledger's back) a full scan."""
        position = self.positions.get(str(token)) if token else None
        if position is not None:
            for lot in position.lots:
                if lot.trade_id == trade_id:
                    return lot
        self._check_index_locked()
        lot = self._lot_by_trade.get(trade_id)
        if lot is not None:
            holder = self.positions.get(lot.token_id)
            if holder is not None and any(candidate is lot for candidate in holder.lots):
                return lot
        return next((candidate for position in self.positions.values()
                     for candidate in position.lots if candidate.trade_id == trade_id), None)

    def realized_since(self, since_wall: float) -> float:
        """Realized P/L of the positions settled at or after since_wall."""
        with self._lock:
            return self._realized_since_locked(since_wall)

    def _realized_since_locked(self, since_wall: float) -> float:
        self._open_positions_locked()           # index anything settled since the last read
        total, counted = 0.0, set()
        start = bisect.bisect_left(self._settled, (since_wall, -1), key=lambda e: (e[0], e[1]))
        for wall, _n, p in self._settled[start:]:
            # an entry is current only while the position is still settled at that time
            if id(p) in counted or not p.settled or p.settled_wall != wall:
                continue
            counted.add(id(p))
            total += float(getattr(p, "realized", 0) or 0)
        return total

    def risk_view(self, since_wall: float, orders) -> tuple[float, float, dict]:
        """One consistent read for the live loss budget (audit C2):
        (realized P/L of positions settled since since_wall, the cost basis
        of every open position, {order id: its booked BUY cost}) for orders =
        [(order_id, token), ...]. Visits the open positions, the positions
        settled since since_wall and the lots of the given orders' tokens -
        never the whole history."""
        with self._lock:
            realized = self._realized_since_locked(since_wall)
            open_basis = sum(float(p.cost) for p in self._open_positions_locked() if p.shares > 0)
            booked: dict = {}
            for order_id, token in orders:
                if not order_id or order_id in booked:
                    continue
                position = self.positions.get(str(token))
                booked[order_id] = sum(float(lot.cost) for lot in (position.lots if position is not None else ())
                                       if lot.order_id == order_id and str(lot.side).upper() == "BUY")
            return realized, open_basis, booked

    @property
    def hybrid_risk(self) -> dict | None:
        """A detached initialized risk epoch, or None for a legacy ledger."""
        with self._lock:
            return (None if self._hybrid_persisted is None else
                    _validated_hybrid_risk(self._hybrid_persisted, self.ledger_id))

    def _observe_hybrid_delta_locked(self, delta: float) -> None:
        """Observe one complete P&L transition without another history scan.

        Its enclosing fill/settlement transaction supplies durability and
        rollback. Journal replay applies the same transitions in order, so
        peaks between background refreshes survive restarts without an extra
        file write per trade. Plain record_fill remains an in-memory API;
        hybrid_risk_view snapshots an unsaved peak before publishing it.
        """
        if self._hybrid_risk is None:
            return
        total = self._hybrid_realized_total
        if total is None:
            self.persistence_halt = self.persistence_halt or "hybrid realized accounting cache unavailable"
            return
        total += delta
        state = self._hybrid_risk
        equity = state["reference_equity"] + total - state["pnl_baseline"]
        if not math.isfinite(total) or not math.isfinite(equity):
            self.persistence_halt = self.persistence_halt or "non-finite hybrid realized accounting"
            return
        self._hybrid_realized_total = total
        if equity > state["high_water"]:
            self._hybrid_risk = {**state, "high_water": equity}

    def hybrid_risk_view(self, reference_equity=None, *, expected_journal_seq=None, rebase=False) -> dict:
        """Durable realized-equity state for hybrid sizing; call on a worker.

        Buying inventory and waiting for redemption do not change realized
        equity. The first call establishes an explicit positive capital base
        and the current confirmed realized-P&L baseline. Later calls preserve
        that epoch, including across process/canary restarts. Partial-sale P&L
        counts immediately, and settled P&L already includes it exactly once.

        Durable fills/settlements record peaks through their existing journal.
        Initialization or an unsaved in-memory peak needs a snapshot before
        publication. No network work occurs here; unchanged reads do not write.

        rebase=True (2026-10-22, the operator's drawdown pause): a new epoch from
        the current realized equity - capital = equity, P&L counted from now,
        peak = equity - durable like any epoch. Needs an established epoch and
        positive equity.
        """
        reference = None
        if (expected_journal_seq is not None and
                (type(expected_journal_seq) is not int or expected_journal_seq < 0)):
            raise ValueError("invalid expected hybrid accounting sequence")
        if reference_equity is not None:
            reference = _hybrid_number(reference_equity, "reference equity")
            if reference <= 0:
                raise ValueError("hybrid reference equity must be positive")
        with self._save_lock:
            with self._lock:
                if self.integrity_error or self.persistence_halt:
                    raise RuntimeError("hybrid sizing requires healthy durable accounting")
                state = (None if self._hybrid_risk is None else
                         _validated_hybrid_risk(self._hybrid_risk, self.ledger_id))
                values = []
                for position in self.positions.values():
                    value = (position.realized if position.settled
                             else position.realized_from_sales)
                    values.append(_hybrid_number(value, "realized P&L"))
                realized = math.fsum(values)
                if not math.isfinite(realized):
                    raise ValueError("non-finite hybrid realized P&L")
                if state is None:
                    if reference is None:
                        raise ValueError("first hybrid risk reading needs positive reference equity")
                    if expected_journal_seq is not None and expected_journal_seq != self.journal_seq:
                        raise ValueError("hybrid accounting changed during reference reading; retry initialization")
                    state = {"version": 1, "ledger_id": self.ledger_id,
                             "reference_equity": reference, "pnl_baseline": realized,
                             "high_water": reference}
                elif (reference is not None and
                      not math.isclose(reference, state["reference_equity"], rel_tol=0, abs_tol=1e-9)):
                    raise ValueError("hybrid reference equity is already established; refusing to rebase")
                equity = state["reference_equity"] + realized - state["pnl_baseline"]
                if not math.isfinite(equity):
                    raise ValueError("non-finite hybrid equity")
                if rebase:
                    if reference is not None or not equity > 0:
                        raise ValueError("a hybrid epoch reset needs an established epoch and positive equity")
                    state = {**state, "reference_equity": equity, "pnl_baseline": realized, "high_water": equity}
                state["high_water"] = max(state["high_water"], equity)
                changed = state != self._hybrid_persisted
                if changed:
                    payload = self._payload_locked()
                    payload["hybrid_risk"] = dict(state)
            if changed:
                # All durable ledger writers serialize on _save_lock, so the
                # snapshot and its journal position remain consistent while
                # other threads can still read the last published state.
                if not self._write_payload(payload):
                    with self._lock:
                        self.persistence_halt = self.persistence_halt or (
                            "hybrid risk state could not be persisted: "
                            f"{self.last_persistence_error or 'ledger save failed'}")[:300]
                    raise RuntimeError("hybrid risk state could not be persisted")
                with self._lock:
                    self._hybrid_risk = dict(state)
                    self._hybrid_realized_total = realized
                self._truncate_journal()
            else:
                with self._lock:
                    self._hybrid_realized_total = realized
            return {**state, "equity": equity, "realized_total": realized}

    def order_has_lots(self, order_id, token) -> bool:
        """Whether any fill of this order (on its token) is booked."""
        with self._lock:
            position = self.positions.get(str(token))
            return position is not None and any(lot.order_id == order_id for lot in position.lots)

    def trades_booked(self, trade_ids) -> bool:
        """Whether every one of these trade ids is booked (no copy of `seen`)."""
        with self._lock:
            return all(str(t) in self.seen for t in trade_ids)

    def open_positions(self) -> list:
        """Snapshot of the open positions' figures (dashboard)."""
        with self._lock:
            return [(p.token_id, p.condition_id, float(p.shares), p.avg_price, float(p.cost))
                    for p in self._open_positions_locked() if p.shares > 0]

    # -------------------------------------------------------------- reads
    def unrealized(self, mark) -> tuple[float, int]:
        """Mark open positions to `mark(token_id) -> exit price or None`.

        Marked to the bid: the price you could actually exit at. Positions
        with no usable bid are counted as unmarkable rather than assumed
        worthless or assumed whole.
        """
        total, unmarkable = 0.0, 0
        with self._lock:
            open_snapshot = [
                (pos.token_id, pos.shares, pos.cost)
                for pos in self._open_positions_locked()
                if pos.shares > 0
            ]
        cycle_error = None
        for token, shares, cost in open_snapshot:
            px, error = _safe_mark(mark, token)
            if error:
                cycle_error = error
            if px is None:
                unmarkable += 1
                continue
            total += shares * px - cost
        with self._lock:
            self.last_mark_error = cycle_error
        return total, unmarkable

    def summary(self, mark=None) -> dict:
        # The lock is held for a reference copy of the positions and the open
        # ones' figures - not for a pass over every position the ledger ever
        # held (that pass blocked the order path's reads for its length).
        # Settled positions never change again (a late fill reopens one: it
        # is then open on the next summary), so they are read outside it.
        with self._lock:
            everything = list(self.positions.values())
            open_ = [p for p in self._open_positions_locked() if p.shares > 0]
            open_ids = {id(p) for p in open_}
            open_rows = [(p.token_id, p.condition_id, p.shares, p.avg_price, p.cost, p.fees,
                          max((lot.wall for lot in p.lots), default=None)) for p in open_]
            counters = (len(self.seen), self.duplicates, self.skipped_status, self.skipped_side,
                        self.skipped_unauthorized, self.skipped_authorization_mismatch,
                        self.last_persistence_error, self.last_ingest_error, self.integrity_error)
        if True:
            settled = [p for p in everything if id(p) not in open_ids and p.settled]
            realized = sum(p.realized or 0.0 for p in settled)
            # Grade one market once. If the strategy bought both outcomes in
            # a round, counting the winner token as a win and the loser token
            # as a loss makes the reported win rate meaningless; the round's
            # net PnL is the result that matters.
            market_results: dict[str, float] = {}
            for p in settled:
                key = p.condition_id or f"token:{p.token_id}"
                market_results[key] = market_results.get(key, 0.0) + (p.realized or 0.0)
            wins = sum(1 for pnl in market_results.values() if pnl > 0)
            losses = sum(1 for pnl in market_results.values() if pnl <= 0)
            open_cost = {id(p): row[4] for p, row in zip(open_, open_rows)}
            open_fees = {id(p): row[5] for p, row in zip(open_, open_rows)}
            staked = sum(open_cost.get(id(p), p.cost) for p in everything)
            (fills_counted, duplicates, skipped_status, skipped_side, skipped_unauthorized,
             skipped_mismatch, persistence_error, ingest_error, integrity_error) = counters
            out = {
                "realized_pnl": realized,
                "settled_positions": len(settled),
                "settled_markets": len(market_results),
                "open_positions": len(open_),
                "pending_cost": sum(row[4] for row in open_rows),
                "wins": wins,
                "losses": losses,
                "win_rate": (wins / (wins + losses)) if (wins + losses) else None,
                "fees_paid": sum(open_fees.get(id(p), p.fees) for p in everything),
                "total_cost": staked,
                "fills_counted": fills_counted,
                "duplicates_suppressed": duplicates,
                "skipped_not_a_fill": skipped_status,
                "skipped_non_buy": skipped_side,
                "skipped_unauthorized": skipped_unauthorized,
                "skipped_authorization_mismatch": skipped_mismatch,
                "persistence_error": persistence_error,
                "ingest_error": ingest_error,
                "integrity_error": integrity_error,
                "open_position_details": [
                    {
                        "token_id": token,
                        "condition_id": condition,
                        "shares": shares,
                        "average_entry_price": avg,
                        "cost": cost,
                        "fees": fees,
                        "latest_fill_wall": latest,
                    }
                    for token, condition, shares, avg, cost, fees, latest in open_rows
                ],
            }
            out["realized_after_fees"] = realized   # fees already inside cost
        if mark is not None:
            u, unmarkable, cycle_error = 0.0, 0, None
            # Sample every token exactly once. Calling a moving book twice made
            # the aggregate equity disagree with its displayed components.
            for detail in out["open_position_details"]:
                bid, error = _safe_mark(mark, detail["token_id"])
                if error:
                    cycle_error = error
                detail["mark_bid"] = bid
                detail["unrealized_to_bid"] = (
                    None if bid is None else
                    detail["shares"] * bid - detail["cost"])
                if bid is None:
                    unmarkable += 1
                else:
                    u += detail["unrealized_to_bid"]
            with self._lock:
                self.last_mark_error = cycle_error
            out["unrealized_mark_to_bid"] = u
            out["unmarkable_positions"] = unmarkable
            # A partial mark is not total equity. Returning a number here
            # silently treated every unmarkable position as exactly flat.
            out["equity_pnl"] = None if unmarkable else out["realized_pnl"] + u
            out["mark_error"] = cycle_error
        else:
            out["unrealized_mark_to_bid"] = None
            out["equity_pnl"] = None
            out["mark_error"] = None
        return out

    # --------------------------------------------------- accuracy check
    def mark_balance(self, balance) -> None:
        try:
            b = float(balance)
        except (TypeError, ValueError):
            return
        if not math.isfinite(b) or b < 0:
            return
        with self._lock:
            self.balance_marks.append((time.time(), b))
            if len(self.balance_marks) > 500:
                del self.balance_marks[:250]

    BALANCE_CHECK_WINDOW_S = 900.0   # the wallet movement checked: the last 15 minutes of reads
    REDEEM_LAG_S = 600.0             # a payout may reach the wallet this long after its settlement

    def reconcile_balance(self, tolerance: float = 0.02, window_s: float | None = None) -> dict:
        """Compare our books against the venue's own USDC movement.

        Over the balance reads of the last window_s: the wallet's change must lie
        between what confirmed BUY fills less net SELL proceeds explain, and that
        plus every payout that may have landed in it - positions this ledger
        settled from REDEEM_LAG_S before the window to its end. Resolution alone
        moves no cash, but this account's winnings come back as cash within about
        a minute of settlement (redeemed automatically, seen live 2026-10-16): the
        old check, over every read since start and without payouts, showed each
        payout - and any deposit, for the rest of the run - as a MISMATCH. Missing
        fills (the wallet fell more) and deposits, manual or unrelated activity
        (it rose more) still are; a deposit drops out once the window passes it.
        """
        try:
            tolerance = float(tolerance)
            window = self.BALANCE_CHECK_WINDOW_S if window_s is None else float(window_s)
        except (TypeError, ValueError) as exc:
            raise ValueError("balance reconciliation tolerance is invalid") from exc
        if not math.isfinite(tolerance) or tolerance < 0 or not math.isfinite(window) or window <= 0:
            raise ValueError("balance reconciliation tolerance is invalid")
        with self._lock:
            if len(self.balance_marks) < 2:
                return {"status": "NO_DATA",
                        "detail": "need at least two balance reads"}
            t1, b1 = self.balance_marks[-1]
            inside = [m for m in self.balance_marks[:-1] if m[0] >= t1 - window]
            t0, b0 = inside[0] if inside else self.balance_marks[-2]
            # lots are append-only and never change once booked: walk them
            # outside the lock, over reference copies taken inside it
            lots = [list(position.lots) for position in self.positions.values()]
            payouts = [(float(p.settled_wall), float(p.shares) * float(p.payout_per_share))
                       for p in self.positions.values()
                       if p.settled and p.settled_wall is not None and p.payout_per_share and p.shares > 0]
        if True:
            observed = b1 - b0
            purchased = sum(
                lot.cost for group in lots
                for lot in group if lot.side == "BUY" and t0 <= lot.wall <= t1
            )
            sold = sum(
                lot.notional - lot.fee for group in lots
                for lot in group if lot.side == "SELL" and t0 <= lot.wall <= t1
            )
            expected = sold - purchased
            payout_room = sum(amount for wall, amount in payouts if t0 - self.REDEEM_LAG_S <= wall <= t1)
            tol = max(tolerance, abs(expected) * 0.01)
            diff = observed - expected
            low, high = observed < expected - tol, observed > expected + payout_room + tol
            ok = not (low or high)
            return {
                "status": "OK" if ok else "MISMATCH",
                "window_s": t1 - t0,
                "balance_observed_delta": observed,
                "expected_delta": expected,
                "confirmed_buy_cost_in_window": purchased,
                "confirmed_sell_proceeds_in_window": sold,
                "payouts_possible_in_window": payout_room,
                "difference": diff,
                "detail": "" if ok else
                          ("the wallet fell more than our confirmed fills explain: missing fills, a withdrawal "
                           "or unrelated activity; investigate" if low else
                           "the wallet rose more than our fills and payouts explain: a deposit, a redemption "
                           "the ledger does not know, or unrelated activity; investigate"),
            }

    # ---------------------------------------------------------- persistence
    def _payload_locked(self) -> dict:
        """Build an ownership-independent JSON snapshot under ``_lock``."""
        return {
            "version": 4,
            "ledger_id": self.ledger_id,
            **({"hybrid_risk": dict(self._hybrid_risk)}
               if self._hybrid_risk is not None else {}),
            "opened_wall": self.opened_wall,
            "category": self.category,
            "seen": self._seen_json(),
            "duplicates": self.duplicates,
            "skipped_status": self.skipped_status,
            "skipped_side": self.skipped_side,
            "skipped_unauthorized": self.skipped_unauthorized,
            "skipped_authorization_mismatch": self.skipped_authorization_mismatch,
            "integrity_error": self.integrity_error,
            "reported_unauthorized": sorted(self.reported_unauthorized),
            "reported_mismatch": sorted(self.reported_mismatch),
            # A copy of the registry, so json.dumps after _lock is released
            # cannot see it change size. Its metadata dicts are never mutated
            # in place (authorize replaces the value), so a shallow copy is a
            # snapshot - a deep copy of every authorization the ledger ever
            # held cost O(history) under the lock on every save (audit C2).
            "authorized_orders": dict(self.authorized_orders),
            "positions": self._position_texts_locked(),
            "balance_marks": list(self.balance_marks[-50:]),
            "journal_seq": self.journal_seq,
        }

    def _position_texts_locked(self) -> "_PositionTexts":
        """Every position as JSON text, re-encoding only those that changed.

        Every fill and every settlement rewrites the whole file, and encoding
        thousands of settled positions that never change again held the GIL
        for longer each day - over 100ms a write on the VPS after four days,
        freezing the event loop each time. A position's text is reused while
        its fingerprint is unchanged. The fingerprint covers everything this
        module ever mutates on a position: its scalar fields, the number of
        lots (lots are append-only) and each lot's `consumed`, plus the
        identities of the objects, so a replaced position is always re-encoded.
        """
        out, keep = _PositionTexts(), {}
        now = time.time()
        for k, v in self.positions.items():
            lots = v.lots
            hit = self._pos_text.get(k)
            if hit is not None and hit[2] is v and v.settled and len(lots) == hit[3]:
                # Settled long ago: nothing in this module changes it again (a
                # late fill reopens it first, and adds a lot) - the text stands
                # without re-fingerprinting every lot under the lock (audit C2:
                # 28-50ms a snapshot at 20,000 positions, the order path waiting).
                keep[k] = hit
                out.append(hit[1])
                continue
            fp = (id(v), id(lots), _get_scalars(v), len(lots), tuple(map(_get_consumed, lots)))
            entry = (hit[1] if hit is not None and hit[0] == fp
                     else f"{json.dumps(k)}: {json.dumps(_pos_dict(v), allow_nan=False)}")
            frozen = (v if v.settled and v.settled_wall is not None
                      and now - float(v.settled_wall) > self.FREEZE_SETTLED_AFTER_S else None)
            keep[k] = (fp, entry, frozen, len(lots))
            out.append(entry)
        self._pos_text = keep
        return out

    FREEZE_SETTLED_AFTER_S = 600.0

    def _seen_json(self) -> "_RawJSON":
        """json.dumps(sorted(self.seen)), re-done only when the set changed."""
        key = (id(self.seen), len(self.seen), self._seen_rev)
        if self._seen_text is None or self._seen_text[0] != key:
            self._seen_text = (key, _RawJSON(json.dumps(sorted(self.seen))))
        return self._seen_text[1]

    def _write_payload(self, payload: dict) -> bool:
        """Write one already-snapshotted payload; caller owns ``_save_lock``."""
        tmp = None
        try:
            d = os.path.dirname(os.path.abspath(self.path)) or "."
            os.makedirs(d, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
            # dumps, not dump: json.dump streams through the pure-Python
            # encoder, several times slower than the C one dumps uses, and
            # every fill rewrites the whole ledger - so the save grew with
            # every lot and froze the process (same bytes either way).
            text = _encode_payload(payload)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            replace_atomic(tmp, self.path)
            tmp = None
            _fsync_directory(d)
            with self._lock:
                self._hybrid_persisted = (dict(payload["hybrid_risk"])
                                          if "hybrid_risk" in payload else None)
            self.schema_version = 4
            self.loaded_from_disk = True
            self.last_persistence_error = None
            return True
        except Exception as exc:
            self.last_persistence_error = (
                f"ledger save failed: {type(exc).__name__}: {exc}")[:300]
            if tmp is not None:
                try:
                    os.unlink(tmp)
                except OSError as cleanup_exc:
                    self.last_persistence_error += (
                        f"; temp cleanup failed: {type(cleanup_exc).__name__}: "
                        f"{cleanup_exc}")[:200]
            return False

    def save(self) -> bool:
        """Atomic write. A half-written ledger is worse than a stale one."""
        with self._save_lock:
            with self._lock:
                payload = self._payload_locked()
            if not self._write_payload(payload):
                return False
            self._truncate_journal()             # the snapshot now holds every journal entry
            return True

    def load(self) -> bool:
        try:
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(
                    fh,
                    parse_constant=_reject_json_constant,
                )
        except Exception as exc:
            self.last_persistence_error = (
                f"ledger load failed: {type(exc).__name__}: {exc}")[:300]
            return False
        try:
            if (not isinstance(data, dict)
                    or type(data.get("version")) is not int
                    or data.get("version") not in (2, 3, 4)):
                raise ValueError("unsupported ledger schema version")
            schema_version = int(data["version"])
            if schema_version == 4:
                ledger_id = str(data.get("ledger_id") or "")
                if not _valid_ledger_id(ledger_id):
                    raise ValueError("invalid ledger identity")
            else:
                # V2/V3 ledgers predate durable identity.  Retain the random
                # ID allocated by __init__; the next successful save upgrades
                # the file atomically to V4.
                ledger_id = self.ledger_id
            hybrid_risk = (_validated_hybrid_risk(data["hybrid_risk"], ledger_id)
                           if "hybrid_risk" in data else None)
            opened_wall = float(data.get("opened_wall", self.opened_wall))
            integrity_error = data.get("integrity_error")
            if (integrity_error is not None
                    and (not isinstance(integrity_error, str) or not integrity_error
                         or len(integrity_error) > 1024 or not integrity_error.isprintable())):
                raise ValueError("invalid ledger integrity error")
            category = data.get("category", self.category)
            if (not math.isfinite(opened_wall) or opened_wall <= 0
                    or not isinstance(category, str) or not category.strip()
                    or len(category) > 64):
                raise ValueError("invalid ledger header")

            raw_seen = data.get("seen") or []
            if not isinstance(raw_seen, list):
                raise ValueError("invalid ledger seen registry")
            seen_list = [str(value) for value in raw_seen]
            if (len(seen_list) != len(set(seen_list))
                    or any(not _safe_identifier(value) for value in seen_list)):
                raise ValueError("invalid ledger seen registry")
            seen = set(seen_list)

            def counter(name):
                value = int(data.get(name) or 0)
                if value < 0:
                    raise ValueError(f"negative ledger counter {name}")
                return value

            duplicates = counter("duplicates")
            skipped_status = counter("skipped_status")
            skipped_side = counter("skipped_side")
            skipped_unauthorized = counter("skipped_unauthorized")
            skipped_authorization_mismatch = counter(
                "skipped_authorization_mismatch")

            def identifier_set(name):
                raw = data.get(name) or []
                if not isinstance(raw, list):
                    raise ValueError(f"invalid {name}")
                values = {str(value) for value in raw}
                if len(values) != len(raw) or any(
                        not _safe_identifier(value) for value in values):
                    raise ValueError(f"invalid {name}")
                return values

            reported_unauthorized = identifier_set("reported_unauthorized")
            reported_mismatch = identifier_set("reported_mismatch")

            raw_authorized = data.get("authorized_orders") or {}
            authorized_orders: dict[str, dict] = {}
            if isinstance(raw_authorized, list):
                for oid in raw_authorized:
                    order_id = str(oid or "")
                    if not _safe_identifier(order_id) or order_id in authorized_orders:
                        raise ValueError("invalid legacy authorized order entry")
                    authorized_orders[order_id] = {}
            elif isinstance(raw_authorized, dict):
                for oid, meta in raw_authorized.items():
                    order_id = str(oid or "")
                    if not _safe_identifier(order_id) or not isinstance(meta, dict):
                        raise ValueError("invalid authorized order entry")
                    normalized = _authorization_metadata(meta)
                    required = {
                        "condition_id", "token_id", "window_end",
                        "requested_notional",
                    }
                    # Incomplete v2 legacy metadata stays a non-authorizing
                    # marker. New malformed v3 metadata is corruption.
                    if normalized and not required.issubset(normalized):
                        if data.get("version") == 2:
                            normalized = {}
                        else:
                            raise ValueError("incomplete authorization risk metadata")
                    _validate_authorization_metadata(normalized)
                    authorized_orders[order_id] = normalized
            else:
                raise ValueError("invalid authorized order registry")

            balance_marks = []
            raw_marks = data.get("balance_marks") or []
            if not isinstance(raw_marks, list) or len(raw_marks) > 500:
                raise ValueError("invalid balance mark registry")
            for mark in raw_marks:
                if not isinstance(mark, (list, tuple)) or len(mark) != 2:
                    raise ValueError("invalid balance mark")
                wall, balance = float(mark[0]), float(mark[1])
                if (not math.isfinite(wall) or wall <= 0
                        or not math.isfinite(balance) or balance < 0):
                    raise ValueError("invalid balance mark")
                if balance_marks and wall < balance_marks[-1][0]:
                    raise ValueError("balance marks are out of order")
                balance_marks.append((wall, balance))

            raw_positions = data.get("positions") or {}
            if not isinstance(raw_positions, dict):
                raise ValueError("invalid position registry")
            positions: dict[str, Position] = {}
            all_lot_ids: list[str] = []
            for k, v in raw_positions.items():
                if not isinstance(v, dict):
                    raise ValueError("invalid position entry")
                position_data = dict(v)
                raw_lots = position_data.pop("lots", [])
                if not isinstance(raw_lots, list):
                    raise ValueError("invalid position lots")
                lots = [Lot(**lot) for lot in raw_lots]
                position = Position(lots=lots, **position_data)
                key = str(k)
                _validate_loaded_position(key, position)
                positions[key] = position
                all_lot_ids.extend(lot.trade_id for lot in lots)
            if len(all_lot_ids) != len(set(all_lot_ids)):
                raise ValueError("duplicate trade id appears in multiple positions")
            if set(all_lot_ids) != seen:
                raise ValueError("ledger seen set does not match persisted lots")
            journal_seq = data.get("journal_seq", 0)
            if type(journal_seq) is not int or journal_seq < 0:
                raise ValueError("invalid ledger journal position")

            # Commit only after the complete file passes validation. Explicit
            # reloads therefore cannot partially destroy a running ledger.
            with self._lock:
                self.opened_wall = opened_wall
                self.ledger_id = ledger_id
                self._hybrid_risk = hybrid_risk
                self._hybrid_persisted = (None if hybrid_risk is None else dict(hybrid_risk))
                self._hybrid_realized_total = (None if hybrid_risk is None else math.fsum(
                    _hybrid_position_realized(position) for position in positions.values()))
                self.schema_version = schema_version
                self.loaded_from_disk = True
                self.category = category
                self.seen = seen
                self.duplicates = duplicates
                self.skipped_status = skipped_status
                self.skipped_side = skipped_side
                self.skipped_unauthorized = skipped_unauthorized
                self.skipped_authorization_mismatch = skipped_authorization_mismatch
                self.reported_unauthorized = reported_unauthorized
                self.reported_mismatch = reported_mismatch
                self.authorized_orders = authorized_orders
                self.balance_marks = balance_marks
                self.positions = positions
                self._pos_text = {}
                self._reindex_locked()
                self.journal_seq = journal_seq
                self.journal_replayed = self._replay_journal(journal_seq)
                self._hybrid_persisted = (None if self._hybrid_risk is None else dict(self._hybrid_risk))
                self._journal_since = self.journal_replayed
                self.last_persistence_error = None
                self.integrity_error = self.integrity_error or integrity_error
            return True
        except Exception as exc:
            self.last_persistence_error = (
                f"ledger validation failed: {type(exc).__name__}: {exc}")[:300]
            return False


def _journal_has_entries(path: str) -> bool:
    try:
        return os.path.getsize(path) > 0
    except OSError:
        return False


_JOURNAL_FILL = {"seq": int, "op": str, "trade_id": str, "token_id": str, "condition_id": (str, type(None)),
                 "side": str, "shares": (int, float), "price": (int, float), "fee": (int, float), "status": str,
                 "wall": (int, float), "source": str, "order_id": (str, type(None))}
_JOURNAL_SETTLE = {"seq": int, "op": str, "condition_id": str, "wall": (int, float), "payouts": dict,
                   "tokens": list}
_JOURNAL_AUTHORIZE = {"seq": int, "op": str, "order_id": str, "metadata": dict}


def _journal_entry(e) -> dict:
    """One journal line, checked field by field (a malformed one fails the load)."""
    if not isinstance(e, dict) or e.get("op") not in ("fill", "settle", "authorize"):
        raise ValueError("invalid ledger journal entry")
    shape = {"fill": _JOURNAL_FILL, "settle": _JOURNAL_SETTLE, "authorize": _JOURNAL_AUTHORIZE}[e["op"]]
    if e["op"] == "fill" and "reported_price" in e:
        # Old journals predate a separate headline price and remain readable.
        shape = {**shape, "reported_price": (int, float, type(None))}
    if set(e) != set(shape):
        raise ValueError("ledger journal entry has unexpected fields")
    for k, t in shape.items():
        if not isinstance(e[k], t) or isinstance(e[k], bool):
            raise ValueError(f"ledger journal field {k} has the wrong type")
    if e["seq"] <= 0:
        raise ValueError("invalid ledger journal sequence")
    if e["op"] == "settle":
        if (sorted(e["payouts"]) != sorted(e["tokens"]) or not e["tokens"]
                or any(not isinstance(v, (int, float)) or isinstance(v, bool)
                       or not 0.0 <= float(v) <= 1.0 for v in e["payouts"].values())):
            raise ValueError("invalid ledger journal settlement")
    return e


def current_payload(path: str) -> dict:
    """The ledger as a plain dict, journal included - for read-only tools
    (sniper_check, live_reconcile, ...). Loads it without writing anything."""
    led = Ledger(path=path)
    with led._lock:
        return json.loads(_encode_payload(led._payload_locked()))


def _consume_fifo(pos, shares: float, *, consume: bool = True):
    """Consume `shares` from the oldest open BUY lots, oldest first.

    Returns ``(notional_basis, fee_basis, shortfall)``. A non-zero shortfall
    means the position does not hold that many shares, and the caller must
    refuse the fill rather than book a short: this ledger has no concept of
    negative inventory and inventing one would invert PnL.
    """
    remaining = float(shares)
    notional = fees = 0.0
    for lot in pos.lots:
        if remaining <= 1e-12:
            break
        if str(lot.side or "").upper() != "BUY":
            continue
        open_sh = lot.open_shares
        if open_sh <= 1e-12:
            continue
        take = min(open_sh, remaining)
        fee_per_share = (lot.fee / lot.shares) if lot.shares else 0.0
        notional += take * lot.price
        fees += take * fee_per_share
        if consume:
            lot.consumed += take
        remaining -= take
    return notional, fees, remaining


def _pos_dict(p: Position) -> dict:
    # Same dict asdict() built, without its recursive deep copy: every field
    # but `lots` is an immutable scalar and Lot has no mutable fields, so
    # fresh dicts and a fresh list are an equally safe snapshot, ~5x faster.
    d = {name: getattr(p, name) for name in _POSITION_FIELDS}
    d["lots"] = [{name: getattr(l, name) for name in _LOT_FIELDS} for l in p.lots]
    return d


_POSITION_FIELDS = tuple(f.name for f in fields(Position))
_POS_SCALARS = tuple(n for n in _POSITION_FIELDS if n != "lots")
_get_scalars = operator.attrgetter(*_POS_SCALARS)
_get_consumed = operator.attrgetter("consumed")


class _PositionTexts(list):
    """'"key": {...}' entries of the payload's "positions" object, already encoded."""


class _RawJSON(str):
    """A payload value that is already JSON text."""


def _encode_payload(payload: dict) -> str:
    """Byte for byte what json.dumps(payload, allow_nan=False) writes, with
    positions that are already encoded spliced in where they belong.

    The encoder is compositional: an object is "{" + '"key": value' pairs
    joined by ", " + "}" in insertion order, so encoding each top-level value
    on its own and joining them with the same separators reproduces the
    single-call output exactly - same keys, same order, same bytes."""
    parts = []
    for key, value in payload.items():
        if isinstance(value, _PositionTexts):
            text = "{" + ", ".join(value) + "}"
        elif isinstance(value, _RawJSON):
            text = str(value)
        else:
            text = json.dumps(value, allow_nan=False)
        parts.append(f"{json.dumps(key)}: {text}")
    return "{" + ", ".join(parts) + "}"


_LOT_FIELDS = tuple(f.name for f in fields(Lot))


def _s(v):
    return None if v is None else str(v)


def _fill_execution_values(fill, theta, category, exponent) -> tuple[float, float]:
    """Cash-weighted basis and fees at each actual execution price.

    Some lifecycle copies omit maker legs. Actual paid cash still proves the
    average basis when available; otherwise the headline is the only evidence.
    """
    if type(exponent) is not int or not 1 <= exponent <= 8:
        raise ValueError("invalid fill fee exponent")
    shares, headline = float(fill.size), float(fill.price)
    paid_raw = getattr(fill, "paid", None)
    paid = shares * headline if paid_raw is None else float(paid_raw)
    if (isinstance(fill.size, bool) or isinstance(fill.price, bool)
            or isinstance(paid_raw, bool)
            or not all(math.isfinite(value) for value in (shares, headline, paid))
            or shares <= 0 or paid <= 0 or not 0 < headline <= 1):
        raise ValueError("invalid fill execution amounts")
    price = paid / shares
    if not math.isfinite(price) or not 0 < price <= 1:
        raise ValueError("invalid fill execution price")
    legs = getattr(fill, "maker_legs", None)
    if legs is None:
        return price, fee_mod.taker_fee(shares, price, theta, category, exponent=exponent)
    if not isinstance(legs, (list, tuple)) or not legs:
        raise ValueError("invalid fill maker legs")
    quantities, cash, fees = [], [], []
    for leg in legs:
        if (not isinstance(leg, (list, tuple)) or len(leg) != 2
                or any(isinstance(value, bool) for value in leg)):
            raise ValueError("invalid fill maker leg")
        quantity, leg_price = map(float, leg)
        if (not math.isfinite(quantity) or quantity <= 0
                or not math.isfinite(leg_price) or not 0 < leg_price < 1):
            raise ValueError("invalid fill maker leg")
        quantities.append(quantity)
        cash.append(quantity * leg_price)
        fees.append(Decimal(str(fee_mod.taker_fee(
            quantity, leg_price, theta, category, exponent=exponent))))
    if (not math.isclose(math.fsum(quantities), shares, rel_tol=1e-6, abs_tol=1e-6)
            or not math.isclose(math.fsum(cash), paid, rel_tol=1e-6, abs_tol=1e-6)):
        raise ValueError("fill maker legs do not match its shares/cash")
    return price, float(sum(fees))


def _replay_execution_conflicts(row, lot, authorization, category) -> list[str]:
    """Available execution evidence must agree before a booked ID is suppressed."""
    paid, legs = row.get("paid"), row.get("maker_legs")
    maker_orders = row.get("maker_orders")
    if maker_orders is not None:
        from feeds.poly_user import taker_legs, taker_paid
        parsed_legs = taker_legs(lot.token_id, maker_orders, lot.shares)
        parsed_paid = taker_paid(lot.token_id, maker_orders, lot.shares)
        if parsed_legs is None or parsed_paid is None:
            raise ValueError("malformed supplied maker orders")
        if (paid is not None and (isinstance(paid, bool)
                or not math.isclose(float(paid), parsed_paid, rel_tol=1e-9, abs_tol=1e-6))):
            raise ValueError("maker orders disagree with paid cash")
        if legs is not None and legs != parsed_legs:
            raise ValueError("maker execution surfaces disagree")
        paid, legs = parsed_paid, parsed_legs
    conflicts = []
    if paid is not None:
        value = float(paid)
        if isinstance(paid, bool) or not math.isfinite(value) or value <= 0:
            raise ValueError("invalid paid cash")
        if not math.isclose(value, lot.notional, rel_tol=1e-9, abs_tol=1e-6):
            conflicts.append("paid cash")
    if legs is not None:
        evidence = SimpleNamespace(
            size=lot.shares,
            price=lot.price if lot.reported_price is None else lot.reported_price,
            paid=paid, maker_legs=legs)
        # Missing paid cash is reconstructed from the supplied executions;
        # the headline cannot prove the average price of a multi-level match.
        if paid is None:
            if not isinstance(legs, (list, tuple)) or not legs:
                raise ValueError("invalid supplied maker legs")
            evidence.paid = math.fsum(float(qty) * float(price) for qty, price in legs)
        price, fee = _fill_execution_values(
            evidence, authorization.get("fee_rate"), category,
            authorization.get("fee_exponent") or 1)
        if not math.isclose(price * lot.shares, lot.notional, rel_tol=1e-9, abs_tol=1e-6):
            if "paid cash" not in conflicts:
                conflicts.append("paid cash")
        if not math.isclose(fee, lot.fee, rel_tol=1e-9, abs_tol=1e-9):
            conflicts.append("maker-leg fee")
    return conflicts


def _safe_identifier(value, *, max_length: int = 256) -> bool:
    return (isinstance(value, str) and 0 < len(value) <= max_length
            and value.isprintable() and not any(ch.isspace() for ch in value))


def _valid_ledger_id(value: str) -> bool:
    """Return True only for the canonical random identity written by V4."""
    if not isinstance(value, str) or len(value) != 32:
        return False
    try:
        return uuid.UUID(hex=value).hex == value
    except (ValueError, AttributeError):
        return False


def _hybrid_number(value, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid hybrid {name}")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"invalid hybrid {name}") from exc
    if not math.isfinite(result):
        raise ValueError(f"invalid hybrid {name}")
    return result


def _hybrid_position_realized(position) -> float:
    return _hybrid_number(position.realized if position.settled
                          else position.realized_from_sales, "realized P&L")


def _validated_hybrid_risk(value, ledger_id: str) -> dict:
    required = {"version", "ledger_id", "reference_equity", "pnl_baseline", "high_water"}
    if (not isinstance(value, dict) or set(value) != required
            or type(value.get("version")) is not int or value["version"] != 1
            or value.get("ledger_id") != ledger_id or not _valid_ledger_id(ledger_id)):
        raise ValueError("invalid hybrid risk state or ledger identity")
    reference = _hybrid_number(value["reference_equity"], "reference equity")
    baseline = _hybrid_number(value["pnl_baseline"], "P&L baseline")
    high_water = _hybrid_number(value["high_water"], "high-water mark")
    if reference <= 0 or high_water < reference:
        raise ValueError("invalid hybrid reference equity or high-water mark")
    return {"version": 1, "ledger_id": ledger_id, "reference_equity": reference,
            "pnl_baseline": baseline, "high_water": high_water}


def _authorization_metadata(receipt: dict | None) -> dict:
    if not isinstance(receipt, dict):
        return {}
    out = {}
    for key in ("condition_id", "token_id", "validation"):
        if receipt.get(key) not in (None, ""):
            value = str(receipt[key])
            if not _safe_identifier(value):
                raise ValueError(f"invalid authorization {key}")
            out[key] = value
    for key in (
            "window_end", "requested_notional", "estimated_fee", "fee_rate",
            "fee_exponent", "venue_min_shares", "price_cap",
            "reserved_total_cost"):
        if receipt.get(key) not in (None, ""):
            if isinstance(receipt[key], bool):
                raise ValueError(f"invalid authorization {key}")
            value = float(receipt[key])
            if not math.isfinite(value):
                raise ValueError(f"non-finite authorization {key}")
            if key in ("window_end", "fee_exponent"):
                if not value.is_integer():
                    raise ValueError(f"non-integral authorization {key}")
                out[key] = int(value)
            else:
                out[key] = value
    return out


def _validate_authorization_metadata(metadata: dict) -> None:
    if not metadata:
        return
    required = {"condition_id", "token_id", "window_end", "requested_notional"}
    if not required.issubset(metadata):
        raise ValueError("incomplete authorization risk metadata")
    has_minimum = "venue_min_shares" in metadata
    has_cap = "price_cap" in metadata
    estimated_fee = metadata.get("estimated_fee", 0.0)
    reserved = metadata.get("reserved_total_cost")
    if (not _safe_identifier(metadata["condition_id"])
            or not _safe_identifier(metadata["token_id"])
            or metadata["requested_notional"] <= 0
            or metadata["window_end"] <= 0
            or metadata["window_end"] % 300
            or estimated_fee < 0
            or ("fee_rate" in metadata
                and not 0 < metadata["fee_rate"] <= 1)
            or ("fee_exponent" in metadata
                and not 1 <= metadata["fee_exponent"] <= 8)
            or has_minimum != has_cap
            or (has_minimum and metadata["venue_min_shares"] <= 0)
            or (has_cap and not 0 < metadata["price_cap"] <= 1)
            or (reserved is not None
                and (not has_minimum or "fee_rate" not in metadata))
            or (reserved is not None
                and reserved + 1e-9 < _minimum_authorization_reservation(metadata))):
        raise ValueError("invalid authorization risk metadata")


def _minimum_authorization_reservation(metadata: dict) -> float:
    """Return the lowest all-in reserve justified by durable metadata."""
    requested = float(metadata["requested_notional"])
    required = requested + float(metadata.get("estimated_fee") or 0.0)
    fee_rate = metadata.get("fee_rate")
    if fee_rate is not None:
        principal = requested
        if ("venue_min_shares" in metadata and "price_cap" in metadata):
            principal = max(
                principal,
                float(metadata["venue_min_shares"]) * float(metadata["price_cap"]),
            )
        # For fee = shares * rate * (p * (1-p)) ** exponent, fee/notional
        # is rate * (1-p) ** exponent and can never exceed ``rate``.
        required = max(required, principal * (1.0 + float(fee_rate)))
    if not math.isfinite(required) or required <= 0:
        raise ValueError("invalid authorization reservation inputs")
    return required


def _with_authorization_reservation(metadata: dict, *,
                                    venue_min_shares, price_cap) -> dict:
    """Snapshot strategy limits and their conservative cash reservation."""
    if not metadata:
        raise ValueError("cannot reserve exposure without authorization metadata")
    if venue_min_shares is None or price_cap is None:
        raise ValueError("venue minimum and price cap must be supplied together")
    if "fee_rate" not in metadata:
        raise ValueError("cannot reserve exposure without a durable fee rate")
    try:
        minimum = float(venue_min_shares)
        cap = float(price_cap)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid authorization reservation inputs") from exc
    if (not math.isfinite(minimum) or minimum <= 0
            or not math.isfinite(cap) or not 0 < cap <= 1):
        raise ValueError("invalid authorization reservation inputs")
    out = dict(metadata)
    out["venue_min_shares"] = minimum
    out["price_cap"] = cap
    out["reserved_total_cost"] = _minimum_authorization_reservation(out)
    return out


def _conservative_authorization_cost(metadata: dict) -> float:
    """Recover one accepted order's safe gross cost or fail closed."""
    if "fee_rate" not in metadata:
        raise RuntimeError(
            "active authorization lacks conservative all-in reservation metadata")
    reserved = metadata.get("reserved_total_cost")
    if reserved is not None:
        value = float(reserved)
    else:
        # Backward-compatible V3/V4 path. Live market BUYs post exactly the
        # requested dollar amount (they reject, rather than resize, below the
        # venue minimum), so requested * (1 + rate) safely bounds their cost.
        value = _minimum_authorization_reservation(metadata)
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError("authorized order all-in exposure is invalid")
    return value


def _safe_mark(mark, token_id: str) -> tuple[float | None, str | None]:
    try:
        raw = mark(token_id)
    except Exception as exc:
        return None, (
            f"mark failed for {token_id[-12:]}: {type(exc).__name__}: {exc}")[:300]
    if raw is None:
        return None, None
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        return None, f"invalid mark for {token_id[-12:]}: {type(exc).__name__}"[:300]
    if not math.isfinite(value) or not 0 <= value <= 1:
        return None, f"out-of-range mark for {token_id[-12:]}: {raw!r}"[:300]
    return value, None


def _validated_resolution(resolution) -> tuple[str, dict[str, float]]:
    condition = str(getattr(resolution, "condition_id", "") or "")
    raw = getattr(resolution, "payouts", None)
    if not _safe_identifier(condition) or not isinstance(raw, dict) or len(raw) != 2:
        raise ValueError("resolved market has invalid condition/payout shape")
    payouts: dict[str, float] = {}
    for token, payout in raw.items():
        token_id = str(token or "")
        if not _safe_identifier(token_id) or token_id in payouts:
            raise ValueError("resolved market has invalid payout token mapping")
        try:
            value = float(payout)
        except (TypeError, ValueError) as exc:
            raise ValueError("resolved market has unparsable payout") from exc
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError("resolved market has out-of-range payout")
        payouts[token_id] = value
    values = sorted(payouts.values())
    final_binary = (abs(values[0]) <= 1e-9 and abs(values[1] - 1) <= 1e-9)
    final_half = all(abs(value - 0.5) <= 1e-9 for value in values)
    if abs(sum(values) - 1) > 1e-9 or not (final_binary or final_half):
        raise ValueError("resolved market payouts are not final binary/50-50 values")
    return condition, payouts


def _reject_json_constant(value: str):
    raise ValueError(f"non-finite JSON constant {value}")


def _replay_realized_from_sales(position: "Position") -> float:
    """Independently recompute realized_from_sales from the lot journal.

    `Position.realized_from_sales` is trusted at load time rather than
    derived, so a corrupted or hand-edited file can carry any number there as
    long as it stays self-consistent with `realized` - a settled sell-to-close
    position, for example, only checked `realized == realized_from_sales`
    against itself, never against what the recorded SELL lots actually paid
    out. This replays the same oldest-lot-first FIFO consumption
    `_consume_fifo` performs live, off a private per-lot open-shares counter
    so it never mutates the lots it is validating, and returns the total
    proceeds-minus-basis a faithful replay would produce.
    """
    open_shares: dict[int, float] = {}
    realized = 0.0
    for index, lot in enumerate(position.lots):
        if str(lot.side or "").upper() == "BUY":
            open_shares[index] = lot.shares
            continue
        remaining = lot.shares
        notional = fees = 0.0
        for j, buy_lot in enumerate(position.lots[:index]):
            if remaining <= 1e-12:
                break
            if str(buy_lot.side or "").upper() != "BUY":
                continue
            avail = open_shares.get(j, 0.0)
            if avail <= 1e-12:
                continue
            take = min(avail, remaining)
            fee_per_share = (buy_lot.fee / buy_lot.shares) if buy_lot.shares else 0.0
            notional += take * buy_lot.price
            fees += take * fee_per_share
            open_shares[j] = avail - take
            remaining -= take
        if remaining > 1e-9:
            raise ValueError("persisted SELL exceeds inventory available at that time")
        basis = notional + fees
        proceeds = lot.shares * lot.price - lot.fee
        realized += proceeds - basis
    for index, lot in enumerate(position.lots):
        if str(lot.side or "").upper() == "BUY" and abs(
                lot.consumed - (lot.shares - open_shares[index])) > 1e-8:
            raise ValueError("persisted FIFO consumption does not match SELL lots")
    return realized


def _validate_loaded_position(key: str, position: Position) -> None:
    """Reject corrupted state instead of letting NaN/negative cash bypass risk."""
    if (not _safe_identifier(key) or str(position.token_id) != key
            or not _safe_identifier(position.condition_id)
            or not isinstance(position.settled, bool)):
        raise ValueError("invalid position identity")
    for value in (position.shares, position.cost, position.fees):
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("invalid position amount")
    if position.fees > position.cost + 1e-9:
        raise ValueError("position fees exceed cost")
    seen_lots: set[str] = set()
    shares = cost = fees = 0.0
    for lot in position.lots:
        lot_side = str(lot.side).upper()
        if (not lot.trade_id or lot.trade_id in seen_lots
                or not _safe_identifier(lot.trade_id)
                or str(lot.token_id) != key
                or lot.condition_id != position.condition_id
                or lot_side not in ("BUY", "SELL")
                or str(lot.status).upper() != "CONFIRMED"):
            raise ValueError("invalid persisted lot identity")
        if (not isinstance(lot.source, str) or len(lot.source) > 256
                or not lot.source.isprintable()
                or (lot.order_id is not None and not _safe_identifier(lot.order_id))):
            raise ValueError("invalid persisted lot metadata")
        seen_lots.add(lot.trade_id)
        numeric = (lot.shares, lot.price, lot.fee, lot.wall, lot.consumed)
        if (any(not isinstance(value, (int, float)) or not math.isfinite(value)
                for value in numeric)
                or lot.shares <= 0 or not 0 < lot.price <= 1
                or lot.fee < 0 or lot.wall <= 0
                or lot.consumed < 0 or lot.consumed > lot.shares + 1e-9):
            raise ValueError("invalid persisted lot amount")
        if (lot.reported_price is not None
                and (isinstance(lot.reported_price, bool)
                     or not isinstance(lot.reported_price, (int, float))
                     or not math.isfinite(lot.reported_price)
                     or not 0 < lot.reported_price <= 1)):
            raise ValueError("invalid persisted headline price")
        if lot_side == "SELL":
            # A sale is journalled but holds nothing: its PnL was banked into
            # realized_from_sales when it happened, and the BUY lots it
            # consumed already carry the reduction. Counting it into the
            # aggregates here would double-count the exit.
            if lot.consumed:
                raise ValueError("a SELL lot cannot itself be consumed")
            continue
        open_shares = lot.shares - lot.consumed
        fee_per_share = (lot.fee / lot.shares) if lot.shares else 0.0
        open_fee = fee_per_share * open_shares
        shares += open_shares
        fees += open_fee
        cost += open_shares * lot.price + open_fee
    tolerance = 1e-8
    if (abs(position.shares - shares) > tolerance
            or abs(position.fees - fees) > tolerance
            or abs(position.cost - cost) > tolerance):
        raise ValueError("position aggregates do not match lots")
    if abs(position.realized_from_sales
           - _replay_realized_from_sales(position)) > tolerance:
        raise ValueError("realized_from_sales does not match the recorded SELL lots")
    for value in (position.realized_from_sales, position.sell_fees):
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError("invalid position sales amount")
    if position.sell_fees < 0:
        raise ValueError("invalid position sales amount")
    if abs(position.sell_fees - sum(lot.fee for lot in position.lots
                                   if str(lot.side or "").upper() == "SELL")) > tolerance:
        raise ValueError("sell fees do not match the recorded SELL lots")
    if position.settled:
        payout = position.payout_per_share
        realized = position.realized
        if (not isinstance(realized, (int, float)) or not math.isfinite(realized)
                or not isinstance(position.settled_wall, (int, float))
                or not math.isfinite(position.settled_wall)
                or position.settled_wall <= 0):
            raise ValueError("invalid settled position")
        if payout is None:
            # Closed by selling out rather than by resolution: there is no
            # payout, and everything realised came from the sales.
            if (position.shares > tolerance
                    or abs(realized - position.realized_from_sales) > tolerance):
                raise ValueError("invalid settled position")
        elif (not isinstance(payout, (int, float)) or not math.isfinite(payout)
                or not 0 <= payout <= 1
                or abs(realized - (position.shares * payout - position.cost
                                   + position.realized_from_sales)) > tolerance):
            raise ValueError("invalid settled position")
    elif (position.payout_per_share is not None or position.realized is not None
          or position.settled_wall is not None):
        raise ValueError("unsettled position carries settlement values")
