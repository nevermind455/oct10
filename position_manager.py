"""Dynamic Position Manager - an optional portfolio layer beside the sniper.

It watches both sides of the market the sniper trades (inventory from the
ledger's CONFIRMED fills, both executable books, its own pending orders) and
buys only when a purchase measurably improves the position:

    PAIR_COST_IMPROVEMENT   new matched UP+DOWN shares whose incremental cost,
                            fees and expected slippage included, is below $1 by
                            a margin (one UP + one DOWN pays $1 at resolution,
                            whichever side wins; 0.50/0.50 on a 50-50 result)
    SIDE_AVG_IMPROVEMENT    a lower average on a side already held, by a margin
    EXPOSURE_IMPROVEMENT    less worst-case loss, at a pair cost the operator
                            allows (off by default: hedging is not free)

and then only if the extra capital and risk pass a second, independent check.

Manager orders are FOK (fill-or-kill): the whole order at or under its limit,
or nothing - never a partial position. A candidate exists only for a size the
book can fill completely within the order's price boundary.
"The price went down" is never a reason to buy. A lower average with a larger
possible loss is AVG_IMPROVES_BUT_RISK_REJECTED.

Never in the sniper's path. It runs as its own task, reads state without
waiting for a lock, yields to the sniper (no order while a sniper order is in
flight or right after one), keeps a cash reserve for the sniper, and in paper
uses its own execution slot in the broker (tag "manager"). Off by default:

    POSITION_MANAGER_ENABLED=1        calculations, dashboard, paper orders
    POSITION_MANAGER_LIVE_ENABLED=1   real manager orders - NOT implemented until
                                      the replay results are reviewed: --live
                                      refuses it. Live with the engine on is
                                      shadow-only (decisions logged, no orders).

Formulas (all prices per share, costs include fees):
    avg            = cost / shares
    paired         = min(UP shares, DOWN shares)        (filled + pending)
    unpaired_up    = max(UP - DOWN, 0), unpaired_down likewise
    pnl if UP wins = UP shares - total cost; if DOWN wins = DOWN shares - total cost
    worst-case loss (exposure) = max(0, -min of the two)
    a candidate buy of side S: expected fill walked over the book up to
        min(best ask + max price over best, MAX_BUY_PRICE): qty, VWAP, worst
        price, fees (the venue's curve), slippage = VWAP - best ask
    projected avg  = (cost_S + expected notional + expected fees) / (shares_S + expected qty)
    new pairs      = paired after - paired before
    incremental pair cost = (expected notional + fees) / expected qty + avg of the other side
"""
from __future__ import annotations

import asyncio
import collections
import json
import math
import os
import queue
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path

ACTIONS = ("BUY", "BLOCKED", "WAIT")
CLASSES = ("PAIR_COST_IMPROVEMENT", "SIDE_AVG_IMPROVEMENT", "EXPOSURE_IMPROVEMENT", "NO_IMPROVEMENT",
           "AVG_IMPROVES_BUT_RISK_REJECTED")
REASONS = ("PRICE_NOT_BETTER", "IMPROVEMENT_TOO_SMALL", "PAIR_ECONOMICS_BAD", "AVG_IMPROVES_BUT_RISK_REJECTED",
           "MAX_EXPOSURE", "MAX_MANAGER_SPEND", "MAX_ORDERS", "MAX_SIDE_INVENTORY", "INSUFFICIENT_LIQUIDITY",
           "SLIPPAGE_TOO_HIGH", "TOO_CLOSE_TO_EXPIRY", "ORDER_PENDING", "COOLDOWN", "REARM_REQUIRED", "STALE_BOOK",
           "MARKET_INACTIVE", "SNIPER_PRIORITY", "INSUFFICIENT_BALANCE", "NOTHING_TO_MANAGE", "RULES_NOT_LOADED",
           "DISABLED")
SIDES = ("UP", "DOWN")
EPS = 1e-9


def _flag(env, name) -> bool:
    return str(env.get(name, "")).strip().lower() in ("1", "true", "yes", "on")


# ============================================================================ configuration
@dataclass(frozen=True)
class PMConfig:
    enabled: bool = False
    live_enabled: bool = False
    max_order_usd: float = 10.0               # one manager order
    max_market_usd: float = 25.0              # manager spend in one market
    max_up_shares: float = 60.0               # side inventory, all sources, filled + pending
    max_down_shares: float = 60.0
    max_total_exposure_usd: float = 60.0      # market cost basis, all sources, filled + pending
    max_orders: int = 6                       # manager orders in one market
    max_orders_per_side: int = 3
    min_avg_improvement: float = 0.03         # $/share
    min_pair_improvement: float = 0.02        # a new pair must cost <= 1 - this, fees in
    max_slippage: float = 0.02                # expected VWAP - best ask
    max_price_over_best: float = 0.02         # how far up the book one order may walk
    min_liquidity_shares: float = 5.0         # expected fill below this: INSUFFICIENT_LIQUIDITY
    rearm_price_step: float = 0.03            # a second same-side average-down needs the ask this much lower
    cooldown_ms: float = 2000.0               # per side, after a manager order completes
    min_left_s: float = 30.0                  # no manager order with less time left
    # (at least this many seconds left, multiplier on the improvement thresholds)
    expiry_tiers: tuple = ((120.0, 1.0), (60.0, 1.5), (30.0, 2.5))
    side_avg_max_added_loss_usd: float = 1.0  # stage 2: worst-case loss a SIDE_AVG buy may add
    side_avg_min_gain_per_usd: float = 0.002  # stage 2: $/share avg improvement per $ deployed
    pair_max_added_loss_usd: float = 0.0      # stage 2: a pair buy may not raise the worst-case loss
    exposure_max_pair_cost: float = 0.0       # EXPOSURE_IMPROVEMENT up to this pair cost; 0 = off (opt in)
    exposure_min_loss_cut_usd: float = 1.0    # ... and must cut the worst-case loss by at least this
    min_new_pairs: float = 1.0
    sniper_quiet_ms: float = 300.0            # no manager order this soon after a sniper order
    sniper_cash_reserve_usd: float = 25.0     # cash kept for the sniper
    venue_min_shares: float = 5.0
    venue_min_usd: float = 1.0
    max_buy_price: float = 0.95               # never above the account's MAX_BUY_PRICE
    score_risk_weight: float = 1.0
    score_avg_weight: float = 0.25
    score_capital_weight: float = 0.01
    poll_ms: float = 100.0
    skip_log_s: float = 5.0

    @classmethod
    def from_env(cls, env=None, *, global_max_buy_price: float | None = None,
                 global_max_side_shares: float | None = None,
                 global_max_unsettled_usd: float | None = None) -> "PMConfig":
        """POSITION_MANAGER_* settings, clamped so no manager limit is larger
        than the matching global one (the manager is subordinate to them)."""
        env = os.environ if env is None else env
        base = cls()
        names = {
            "max_order_usd": "MAX_ORDER_USD", "max_market_usd": "MAX_MARKET_USD", "max_up_shares": "MAX_UP_SHARES",
            "max_down_shares": "MAX_DOWN_SHARES", "max_total_exposure_usd": "MAX_TOTAL_EXPOSURE",
            "max_orders": "MAX_ORDERS", "max_orders_per_side": "MAX_ORDERS_PER_SIDE",
            "min_avg_improvement": "MIN_AVG_IMPROVEMENT", "min_pair_improvement": "MIN_PAIR_IMPROVEMENT",
            "max_slippage": "MAX_SLIPPAGE", "max_price_over_best": "MAX_PRICE_OVER_BEST",
            "min_liquidity_shares": "MIN_LIQUIDITY", "rearm_price_step": "MIN_PRICE_IMPROVEMENT_BETWEEN_ENTRIES",
            "cooldown_ms": "COOLDOWN_MS", "min_left_s": "MIN_LEFT_S",
            "side_avg_max_added_loss_usd": "SIDE_AVG_MAX_ADDED_LOSS_USD",
            "side_avg_min_gain_per_usd": "SIDE_AVG_MIN_GAIN_PER_USD",
            "pair_max_added_loss_usd": "PAIR_MAX_ADDED_LOSS_USD", "exposure_max_pair_cost": "EXPOSURE_MAX_PAIR_COST",
            "exposure_min_loss_cut_usd": "EXPOSURE_MIN_LOSS_CUT_USD", "sniper_quiet_ms": "SNIPER_QUIET_MS",
            "sniper_cash_reserve_usd": "SNIPER_CASH_RESERVE_USD",
        }
        values, bad = {}, []
        for attr, suffix in names.items():
            raw = env.get(f"POSITION_MANAGER_{suffix}")
            if raw in (None, ""):
                continue
            try:
                v = float(raw)
            except ValueError:
                bad.append(f"POSITION_MANAGER_{suffix}={raw!r}")
                continue
            if not math.isfinite(v) or v < 0:
                bad.append(f"POSITION_MANAGER_{suffix}={raw!r}")
                continue
            values[attr] = int(v) if isinstance(getattr(base, attr), int) else v
        raw = env.get("POSITION_MANAGER_EXPIRY_TIERS")
        if raw:
            try:
                tiers = tuple(sorted(((float(a), float(b)) for a, b in
                                      (part.split(":") for part in raw.split(",") if part.strip())), reverse=True))
                if not tiers or any(m < 1 or s < 0 for s, m in tiers):
                    raise ValueError
                values["expiry_tiers"] = tiers
            except ValueError:
                bad.append(f"POSITION_MANAGER_EXPIRY_TIERS={raw!r} (want 'seconds:multiplier,...', multipliers >= 1)")
        if bad:
            raise ValueError("invalid POSITION_MANAGER settings: " + "; ".join(bad))
        cfg = replace(base, enabled=_flag(env, "POSITION_MANAGER_ENABLED"),
                      live_enabled=_flag(env, "POSITION_MANAGER_LIVE_ENABLED"), **values)
        clamp = {}
        if global_max_buy_price is not None:
            clamp["max_buy_price"] = min(cfg.max_buy_price, float(global_max_buy_price))
        if global_max_side_shares is not None:
            clamp["max_up_shares"] = min(cfg.max_up_shares, float(global_max_side_shares))
            clamp["max_down_shares"] = min(cfg.max_down_shares, float(global_max_side_shares))
        if global_max_unsettled_usd:                                    # 0 = no global cap configured
            clamp["max_total_exposure_usd"] = min(cfg.max_total_exposure_usd, float(global_max_unsettled_usd))
        return replace(cfg, **clamp)

    def lines(self, *, live_mode: bool) -> list[str]:
        engine = "ENABLED" if self.enabled else "DISABLED"
        orders = ("ENABLED (refused: not implemented until the replay results are reviewed)" if self.live_enabled
                  else "DISABLED") if live_mode else "paper only" if self.enabled else "DISABLED"
        out = ["POSITION MANAGER", f"  Engine:       {engine}", f"  Live Orders:  {orders}",
               "  Execution:    FOK (all of the order at or under its limit, or nothing)"]
        if self.enabled:
            out.append(f"  limits: order ${self.max_order_usd:g}, market ${self.max_market_usd:g}, side "
                       f"{self.max_up_shares:g}/{self.max_down_shares:g} sh, exposure ${self.max_total_exposure_usd:g}, "
                       f"{self.max_orders} orders ({self.max_orders_per_side}/side); improvement >= "
                       f"{self.min_avg_improvement:g} avg / {self.min_pair_improvement:g} pair; slippage <= "
                       f"{self.max_slippage:g}; cooldown {self.cooldown_ms:g}ms; >= {self.min_left_s:g}s left")
        return out


# ============================================================================ state
@dataclass
class SideState:
    shares: float = 0.0
    cost: float = 0.0                # incl. fees
    fees: float = 0.0
    pending_shares: float = 0.0
    pending_usd: float = 0.0
    best_bid: float | None = None
    best_ask: float | None = None
    asks: tuple = ()                 # ((price, size), ...) best first, net of our own recent fills

    @property
    def avg(self) -> float | None:
        return self.cost / self.shares if self.shares > EPS else None

    @property
    def total_shares(self) -> float:   # filled + pending
        return self.shares + self.pending_shares

    @property
    def total_cost(self) -> float:
        return self.cost + self.pending_usd

    @property
    def unrealized(self) -> float | None:
        if self.shares <= EPS:
            return 0.0
        return None if self.best_bid is None else self.shares * self.best_bid - self.cost


@dataclass
class Portfolio:
    up: SideState
    down: SideState
    realized: float = 0.0

    def side(self, s: str) -> SideState:
        return self.up if s == "UP" else self.down

    @property
    def paired(self) -> float:
        return min(self.up.total_shares, self.down.total_shares)

    @property
    def unpaired_up(self) -> float:
        return max(self.up.total_shares - self.down.total_shares, 0.0)

    @property
    def unpaired_down(self) -> float:
        return max(self.down.total_shares - self.up.total_shares, 0.0)

    @property
    def total_cost(self) -> float:
        return self.up.total_cost + self.down.total_cost

    @property
    def filled_cost(self) -> float:
        return self.up.cost + self.down.cost

    @property
    def pending_usd(self) -> float:
        return self.up.pending_usd + self.down.pending_usd

    @property
    def fees(self) -> float:
        return self.up.fees + self.down.fees

    @property
    def combined_avg(self) -> float | None:
        return None if self.up.avg is None or self.down.avg is None else self.up.avg + self.down.avg

    @property
    def value(self) -> float | None:
        vals = [s.shares * s.best_bid if s.shares > EPS and s.best_bid is not None else (0.0 if s.shares <= EPS else None)
                for s in (self.up, self.down)]
        return None if None in vals else sum(vals)

    @property
    def unrealized(self) -> float | None:
        u = [self.up.unrealized, self.down.unrealized]
        return None if None in u else sum(u)

    def worst_case(self, up_add=0.0, down_add=0.0, cost_add=0.0) -> float:
        """Worst settlement P/L (filled + pending, plus a candidate)."""
        c = self.total_cost + cost_add
        return min(self.up.total_shares + up_add - c, self.down.total_shares + down_add - c)

    def exposure(self, **kw) -> float:
        return max(0.0, -self.worst_case(**kw))


@dataclass
class Fill:
    qty: float = 0.0
    notional: float = 0.0
    fees: float = 0.0
    vwap: float | None = None
    worst: float | None = None
    best: float | None = None
    boundary: float | None = None

    @property
    def cost(self) -> float:
        return self.notional + self.fees

    @property
    def slippage(self) -> float | None:
        return None if self.vwap is None or self.best is None else self.vwap - self.best


def fee_curve(rate: float, exponent: int):
    """The venue's taker fee per fill: shares * rate * (p * (1 - p)) ** exponent."""
    def fee(shares: float, price: float) -> float:
        return float(shares) * rate * (price * (1.0 - price)) ** exponent
    return fee


def expected_fill(asks, want_shares: float, boundary: float, fee) -> Fill:
    """Walk the asks (best first) up to `boundary`, as a FAK at that limit
    would fill: never the displayed best price for the whole order."""
    out = Fill(best=asks[0][0] if asks else None, boundary=boundary)
    need = float(want_shares)
    for price, size in asks:
        if price > boundary + EPS or need <= EPS:
            break
        take = min(need, float(size))
        if take <= EPS:
            continue
        out.qty += take
        out.notional += take * price
        out.fees += fee(take, price)
        out.worst = price
        need -= take
    if out.qty > EPS:
        out.vwap = out.notional / out.qty
    return out


# ============================================================================ decisions
@dataclass
class Decision:
    side: str
    action: str = "WAIT"                     # BUY / BLOCKED / WAIT
    classification: str = "NO_IMPROVEMENT"
    reason: str | None = None
    size: float = 0.0                        # shares the order asks for
    limit: float | None = None               # FAK price limit
    stake: float = 0.0                       # $ signed (notional the walk expects)
    fill: Fill | None = None
    metrics: dict = field(default_factory=dict)
    score: float = 0.0
    score_parts: dict = field(default_factory=dict)
    fingerprint: tuple = ()

    def row(self) -> dict:
        f = self.fill or Fill()
        return {"side": self.side, "action": self.action, "classification": self.classification,
                "reason": self.reason, "size": round(self.size, 4), "limit": self.limit, "stake": round(self.stake, 4),
                "expected_qty": round(f.qty, 4), "expected_vwap": None if f.vwap is None else round(f.vwap, 4),
                "worst_fill": f.worst, "expected_fees": round(f.fees, 5),
                "expected_slippage": None if f.slippage is None else round(f.slippage, 4),
                "score": round(self.score, 5), "score_parts": {k: round(v, 5) for k, v in self.score_parts.items()},
                **{k: (round(v, 5) if isinstance(v, float) else v) for k, v in self.metrics.items()}}


@dataclass
class SideHistory:
    orders: int = 0
    last_buy_price: float | None = None
    last_done_mono: float = -1e18
    in_flight: bool = False


@dataclass
class MarketHistory:
    condition: str | None = None
    orders: int = 0
    spend: float = 0.0
    sides: dict = field(default_factory=lambda: {s: SideHistory() for s in SIDES})


class Engine:
    """The decision: pure, no I/O, no clock of its own - every input passed in."""

    def __init__(self, cfg: PMConfig) -> None:
        self.cfg = cfg

    def multiplier(self, left_s: float) -> float | None:
        if left_s < self.cfg.min_left_s:
            return None
        for min_left, mult in sorted(self.cfg.expiry_tiers, reverse=True):
            if left_s >= min_left:
                return float(mult)
        return float(max(m for _s, m in self.cfg.expiry_tiers)) if self.cfg.expiry_tiers else 1.0

    def evaluate(self, side: str, port: Portfolio, *, fee, left_s: float, book_ok: bool, active: bool,
                 hist: MarketHistory, now_mono: float, sniper_busy: bool, cash_free: float | None) -> Decision:
        cfg = self.cfg
        d = Decision(side)
        s, o = port.side(side), port.side("DOWN" if side == "UP" else "UP")
        h = hist.sides[side]
        unpaired_other = port.unpaired_down if side == "UP" else port.unpaired_up
        d.metrics.update(current_shares=s.total_shares, current_avg=s.avg, best_ask=s.best_ask, best_bid=s.best_bid,
                         paired_before=port.paired, unpaired_other=unpaired_other, left_s=round(left_s, 1))
        # ---- what there is to manage
        if s.shares <= EPS and unpaired_other <= EPS:
            d.reason = "NOTHING_TO_MANAGE"
            return d
        # ---- hard gates, cheapest first
        for blocked, reason in ((not active, "MARKET_INACTIVE"), (not book_ok or s.best_ask is None, "STALE_BOOK"),
                                (h.in_flight, "ORDER_PENDING"), (left_s < cfg.min_left_s, "TOO_CLOSE_TO_EXPIRY"),
                                (now_mono - h.last_done_mono < cfg.cooldown_ms / 1000.0, "COOLDOWN"),
                                (hist.orders >= cfg.max_orders or h.orders >= cfg.max_orders_per_side, "MAX_ORDERS"),
                                (hist.spend >= cfg.max_market_usd - EPS, "MAX_MANAGER_SPEND")):
            if blocked:
                return self._block(d, reason)
        mult = self.multiplier(left_s) or 1.0
        need_avg, need_pair = cfg.min_avg_improvement * mult, cfg.min_pair_improvement * mult
        d.metrics.update(threshold_multiplier=mult)
        boundary = min(s.best_ask + cfg.max_price_over_best, cfg.max_buy_price, 0.99)
        side_cap = cfg.max_up_shares if side == "UP" else cfg.max_down_shares
        room = min(side_cap - s.total_shares,
                   (min(cfg.max_order_usd, cfg.max_market_usd - hist.spend)) / max(s.best_ask, 0.01))
        sizes = self._sizes(unpaired_other, room)
        candidates = [self._candidate(side, port, s, o, q, boundary, fee, need_avg, need_pair, hist, cash_free)
                      for q in sizes]
        for c in candidates:                         # every logged decision says which thresholds applied
            c.metrics.update(threshold_multiplier=mult, left_s=round(left_s, 1))
        if not candidates:
            return self._block(d, "MAX_SIDE_INVENTORY" if side_cap - s.total_shares < cfg.venue_min_shares
                               else "MAX_MANAGER_SPEND")
        passing = [c for c in candidates if c.action == "BUY"]
        if passing:
            best = max(passing, key=lambda c: (round(c.score, 9), -c.size))
            if sniper_busy:                                         # the sniper owns this instant
                best.action, best.reason = "BLOCKED", "SNIPER_PRIORITY"
            return best
        # nothing passes: the most informative reason - a stage-2 refusal over a stage-1 one
        stage2 = [c for c in candidates if c.metrics.get("stage1")]
        pick = stage2[0] if stage2 else candidates[0]
        return pick

    def _sizes(self, unpaired_other: float, room: float) -> list[float]:
        cfg = self.cfg
        if room < cfg.venue_min_shares - EPS:
            return []
        out = set()
        if unpaired_other > EPS:
            out.add(round(max(cfg.venue_min_shares, min(unpaired_other, room)), 2))      # exactly what pairs
        q = cfg.venue_min_shares
        while q <= room + EPS and len(out) < 12:
            out.add(round(q, 2))
            q += cfg.venue_min_shares
        return sorted(x for x in out if x <= room + EPS)

    def _block(self, d: Decision, reason: str) -> Decision:
        d.action, d.reason = "BLOCKED", reason
        if reason == "AVG_IMPROVES_BUT_RISK_REJECTED":
            d.classification = reason
        return d

    def _candidate(self, side, port, s, o, q, boundary, fee, need_avg, need_pair, hist, cash_free) -> Decision:
        cfg = self.cfg
        d = Decision(side, size=q, limit=round(boundary, 4))
        f = expected_fill(s.asks, q, boundary, fee)
        d.fill = f
        up_add, dn_add = (f.qty, 0.0) if side == "UP" else (0.0, f.qty)
        unpaired_other = port.unpaired_down if side == "UP" else port.unpaired_up
        loss_before = port.exposure()
        loss_after = port.exposure(up_add=up_add, down_add=dn_add, cost_add=f.cost)
        paired_after = min(port.up.total_shares + up_add, port.down.total_shares + dn_add)
        new_pairs = paired_after - port.paired
        avg_after = (s.cost + s.pending_usd + f.cost) / (s.total_shares + f.qty) if s.total_shares + f.qty > EPS else None
        avg_before = (s.cost + s.pending_usd) / s.total_shares if s.total_shares > EPS else None
        unit = f.cost / f.qty if f.qty > EPS else None
        other_avg = (o.cost + o.pending_usd) / o.total_shares if o.total_shares > EPS else None
        inc_pair = unit + other_avg if (unit is not None and other_avg is not None and new_pairs > EPS) else None
        comb_after = None
        if avg_after is not None and other_avg is not None:
            comb_after = avg_after + other_avg if side == "UP" else other_avg + avg_after
        # FOK: the stake is the cost of exactly these shares, in whole cents (as the
        # venue quantizes it), and the book must hold all of it within the limit.
        d.stake = round(max(math.ceil(f.notional * 100 - 1e-9) / 100, cfg.venue_min_usd), 2)
        capital_max = d.stake + (f.fees * d.stake / f.notional if f.notional > EPS else 0.0)
        d.metrics.update(capital_max=capital_max, order_type="FOK")
        d.metrics.update(
            current_shares=s.total_shares, current_avg=avg_before, best_ask=s.best_ask, best_bid=s.best_bid,
            expected_qty=f.qty, projected_avg=avg_after,
            avg_improvement=None if avg_before is None or avg_after is None else avg_before - avg_after,
            paired_before=port.paired, paired_after=paired_after, new_pairs=new_pairs,
            unpaired_other_before=unpaired_other,
            pair_cost_before=port.combined_avg, pair_cost_after=comb_after, incremental_pair_cost=inc_pair,
            exposure_before=loss_before, exposure_after=loss_after, added_loss=loss_after - loss_before,
            capital=f.cost, threshold_avg=need_avg, threshold_pair=need_pair)
        # ---- liquidity first: a candidate the book cannot fill is not a candidate.
        # FOK: all of the size (and all of the stake, after rounding to the cent).
        room_usd = sum(p * q for p, q in s.asks if p <= boundary + EPS)
        if (f.qty < q - 1e-6 or d.stake > room_usd + 1e-9
                or f.qty < max(cfg.min_liquidity_shares, cfg.venue_min_shares) - EPS
                or f.notional < cfg.venue_min_usd):
            return self._block(d, "INSUFFICIENT_LIQUIDITY")
        if f.slippage is not None and f.slippage > cfg.max_slippage + EPS:
            return self._block(d, "SLIPPAGE_TOO_HIGH")
        # ---- STAGE 1: is there an economic benefit at all?
        cls, why = None, None
        avg_gain = d.metrics["avg_improvement"]
        if inc_pair is not None and new_pairs >= cfg.min_new_pairs - EPS:
            if inc_pair <= 1.0 - need_pair + EPS:
                cls = "PAIR_COST_IMPROVEMENT"
            elif (cfg.exposure_max_pair_cost > 0
                  # stricter near expiry, like every other threshold
                  and inc_pair <= cfg.exposure_max_pair_cost - (need_pair - cfg.min_pair_improvement) + EPS
                  and loss_before - loss_after >= cfg.exposure_min_loss_cut_usd * (need_pair / max(cfg.min_pair_improvement, EPS)) - EPS):
                cls = "EXPOSURE_IMPROVEMENT"
            else:
                why = "PAIR_ECONOMICS_BAD"
        if cls is None and avg_gain is not None and s.total_shares > EPS:
            if s.best_ask >= (avg_before or 0) - EPS:
                why = why or "PRICE_NOT_BETTER"
            elif avg_gain >= need_avg - EPS:
                cls = "SIDE_AVG_IMPROVEMENT"
            else:
                why = why or "IMPROVEMENT_TOO_SMALL"
        if cls is None:
            d.classification = "NO_IMPROVEMENT"
            return self._block(d, why or ("PRICE_NOT_BETTER" if s.total_shares > EPS else "PAIR_ECONOMICS_BAD"))
        d.classification = cls
        d.metrics["stage1"] = cls
        # ---- STAGE 2: does the benefit justify the added capital and risk?
        side_cap = cfg.max_up_shares if side == "UP" else cfg.max_down_shares
        if s.total_shares + f.qty > side_cap + EPS:
            return self._block(d, "MAX_SIDE_INVENTORY")
        if capital_max > cfg.max_order_usd + EPS or hist.spend + capital_max > cfg.max_market_usd + EPS:
            return self._block(d, "MAX_MANAGER_SPEND")
        if port.total_cost + capital_max > cfg.max_total_exposure_usd + EPS:
            return self._block(d, "MAX_EXPOSURE")
        if cash_free is not None and cash_free - cfg.sniper_cash_reserve_usd < capital_max - EPS:
            return self._block(d, "INSUFFICIENT_BALANCE")
        added = loss_after - loss_before
        if cls == "SIDE_AVG_IMPROVEMENT":
            per_usd = (avg_gain or 0.0) / f.cost if f.cost > EPS else 0.0
            d.metrics["avg_gain_per_usd"] = per_usd
            if added > cfg.side_avg_max_added_loss_usd + EPS or per_usd < cfg.side_avg_min_gain_per_usd - EPS:
                return self._block(d, "AVG_IMPROVES_BUT_RISK_REJECTED")
            h = hist.sides[side]
            if h.last_buy_price is not None and s.best_ask > h.last_buy_price - cfg.rearm_price_step + EPS:
                return self._block(d, "REARM_REQUIRED")       # not a new opportunity: no ladder down the book
        elif cls == "PAIR_COST_IMPROVEMENT":
            if added > cfg.pair_max_added_loss_usd + EPS:
                return self._block(d, "AVG_IMPROVES_BUT_RISK_REJECTED")
        # ---- an explainable score: what each part contributes, in dollars
        locked = max(0.0, new_pairs) * (1.0 - inc_pair) if inc_pair is not None else 0.0
        parts = {"locked_pair_profit": locked if cls != "EXPOSURE_IMPROVEMENT" else 0.0,
                 "pair_cost_locked": min(0.0, locked) if cls == "EXPOSURE_IMPROVEMENT" else 0.0,
                 "risk_change": -cfg.score_risk_weight * added,
                 "avg_improvement_value": cfg.score_avg_weight * max(0.0, avg_gain or 0.0) * s.total_shares,
                 "capital": -cfg.score_capital_weight * f.cost}
        d.score_parts, d.score = parts, sum(parts.values())
        d.action, d.reason = "BUY", None
        d.fingerprint = (side, cls, round(s.best_ask, 4), round(unpaired_other, 2), round(f.qty, 2))
        return d


# ============================================================================ the runtime
class AsyncJournal:
    """JSON lines written by a background thread: write() never blocks."""

    def __init__(self, path) -> None:
        self.path = Path(path) if path else None
        self.q: queue.SimpleQueue = queue.SimpleQueue()
        self.errors = 0
        self._t = None
        if self.path is not None:
            self._t = threading.Thread(target=self._run, name="pm-journal", daemon=True)
            self._t.start()

    def write(self, row: dict) -> None:
        if self.path is not None:
            self.q.put(row)

    def _run(self) -> None:
        while True:
            row = self.q.get()
            if row is None:
                return
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")
                    while True:                                      # drain what queued meanwhile
                        try:
                            nxt = self.q.get_nowait()
                        except queue.Empty:
                            break
                        if nxt is None:
                            return
                        fh.write(json.dumps(nxt, separators=(",", ":"), default=str) + "\n")
            except OSError:
                self.errors += 1

    def close(self, timeout: float = 2.0) -> None:
        if self._t is not None:
            self.q.put(None)
            self._t.join(timeout)


@dataclass
class ManagerOrder:
    order_id: str | None
    source: str
    side: str
    classification: str
    requested_qty: float
    reserved_usd: float
    expected_price: float | None
    limit: float | None
    status: str = "PENDING"
    filled_qty: float = 0.0
    actual_price: float | None = None
    fees: float = 0.0
    cost: float = 0.0
    error: str | None = None
    t_submit: float = 0.0
    t_done: float | None = None
    condition: str | None = None
    reason_row: dict = field(default_factory=dict)

    @property
    def remaining_qty(self) -> float:
        return max(0.0, self.requested_qty - self.filled_qty)


class PositionManager:
    """Reads the hub's books, the ledger's confirmed fills and the sniper's
    state; decides with Engine; in paper, executes through the broker's own
    'manager' slot. One instance per bot."""

    def __init__(self, cfg: PMConfig, *, hub, ledger, broker, sniper=None, on_event=None, journal_path=None,
                 live_mode: bool = False, wall=time.time, mono=time.monotonic) -> None:
        self.cfg = cfg
        self.engine = Engine(cfg)
        self.hub, self.ledger, self.broker, self.sniper = hub, ledger, broker, sniper
        self.on_event = on_event or (lambda *a, **k: None)
        self.journal = AsyncJournal(journal_path)
        self.live_mode = bool(live_mode)
        self.shadow = self.live_mode                 # live: decisions only, never orders (see the module doc)
        self.wall, self.mono = wall, mono
        self.hist = MarketHistory()
        self.orders: dict[int, ManagerOrder] = {}
        self._seq = 0
        self.manager_order_ids: set[str] = set()
        self.last: dict = {}
        self.last_port: Portfolio | None = None
        self.n = collections.Counter()
        self._skip_logged: dict = {}
        self._version = None
        self._task = None
        self.eval_us: collections.deque = collections.deque(maxlen=2000)

    # ---------------------------------------------------------------- state
    def _rules_fee(self, cond: str):
        """The market's fee curve from the broker's cache - never a fetch here."""
        cached = getattr(self.broker, "cached_rules", None)              # the live shim
        if cached is not None:
            rules = cached(str(cond))
        else:
            hit = (getattr(self.broker, "_rules_cache", None) or {}).get(str(cond))   # the paper broker
            rules = hit[0] if hit else None
        if rules is None:
            return None, None
        return fee_curve(float(rules.fee_rate), int(rules.fee_exponent)), rules

    def portfolio(self, cond: str, up: str, down: str) -> Portfolio | None:
        """Confirmed fills of this market from the ledger (never requested
        quantities) + this manager's pending reservations + the books. None
        while a fill or settlement holds the ledger lock - never waits."""
        led = self.ledger
        lock = getattr(led, "_lock", None)
        if lock is not None and not lock.acquire(blocking=False):
            return None
        try:
            rows = {}
            for pos in led.positions.values():
                if pos.condition_id == cond and not pos.settled and pos.shares > 0:
                    rows[str(pos.token_id)] = (float(pos.shares), float(pos.cost), float(pos.fees))
            realized = sum(float(p.realized or 0) for p in led.positions.values() if p.settled)
        finally:
            if lock is not None:
                lock.release()
        port = Portfolio(SideState(), SideState(), realized=realized)
        for side, token in (("UP", up), ("DOWN", down)):
            st = port.side(side)
            st.shares, st.cost, st.fees = rows.get(str(token), (0.0, 0.0, 0.0))
            for o in self.orders.values():
                if o.side == side and o.status == "PENDING" and o.condition == cond:
                    st.pending_shares += o.requested_qty
                    st.pending_usd += o.reserved_usd
            view = self.hub.book.view(str(token))
            st.best_bid, st.best_ask = view.best_bid, view.best_ask
            st.asks = self._visible_asks(str(token), view)
        return port

    def _visible_asks(self, token: str, view) -> tuple:
        sn = self.sniper
        if sn is not None and hasattr(sn, "_visible_levels"):
            try:
                return tuple(sn._visible_levels(token, view))          # net of our own recent paper fills
            except Exception:  # noqa: BLE001 - fall back to the raw book
                pass
        return tuple((float(p), float(q)) for p, q in (view.asks or ()))

    def _book_ok(self, token: str) -> bool:
        view = self.hub.book.view(str(token))
        if str(getattr(view, "status", "")) != "LIVE":
            return False
        upd = getattr(view, "updated_mono", None)
        stale_ms = float(getattr(getattr(self.sniper, "s", None), "stale_ms", 2000.0) or 2000.0)
        return upd is None or (self.mono() - float(upd)) * 1000.0 <= stale_ms

    def _sniper_busy(self) -> bool:
        sn = self.sniper
        if sn is None:
            return False
        if any(v is not None for v in getattr(sn, "inflight", {}).values()):
            return True
        last = max(getattr(sn, "last_fire_by_side", {"_": -1e18}).values())
        return self.mono() * 1000.0 - last < self.cfg.sniper_quiet_ms

    def _cash_free(self) -> float | None:
        b = self.broker
        if not hasattr(b, "cash_balance"):
            return None
        led_lock = getattr(self.ledger, "_lock", None)
        if led_lock is not None and not led_lock.acquire(blocking=False):
            return 0.0                                 # cannot tell right now: assume none (no order)
        try:
            return float(b.cash_balance()) - sum(o.reserved_usd for o in self.orders.values() if o.status == "PENDING")
        finally:
            if led_lock is not None:
                led_lock.release()

    # ---------------------------------------------------------------- one evaluation
    def evaluate(self) -> dict | None:
        """Both sides, independently, on the current state. No I/O."""
        t0 = time.perf_counter_ns()
        hub = self.hub
        up, down, cond, end = hub.up_token, hub.down_token, hub.condition_id, hub.window_end
        if not (up and down and cond and end):
            self.last = {s: Decision(s, reason="MARKET_INACTIVE", action="BLOCKED") for s in SIDES}
            return None
        if self.hist.condition != cond:                                  # a new market: fresh limits
            self.hist = MarketHistory(condition=cond)
        fee, _rules = self._rules_fee(cond)
        port = self.portfolio(cond, up, down)
        if port is None:
            return None                                                  # ledger busy: next poll
        self.last_port = port
        left = float(end) - self.wall()
        sniper_busy = self._sniper_busy()
        cash = self._cash_free()
        out = {}
        for side, token in (("UP", up), ("DOWN", down)):
            if fee is None:
                out[side] = Decision(side, action="BLOCKED", reason="RULES_NOT_LOADED",
                                     metrics={"detail": "market fee rules not loaded yet"})
                continue
            out[side] = self.engine.evaluate(side, port, fee=fee, left_s=left, book_ok=self._book_ok(token),
                                             active=left > 0, hist=self.hist, now_mono=self.mono(),
                                             sniper_busy=sniper_busy, cash_free=cash)
        self.last = out
        self.eval_us.append((time.perf_counter_ns() - t0) / 1000.0)
        return out

    def _log_skip(self, d: Decision) -> None:
        if d.action != "BLOCKED" or d.reason in (None, "NOTHING_TO_MANAGE"):
            return
        key = (d.side, d.reason)
        now = self.mono()
        if now - self._skip_logged.get(key, -1e18) < self.cfg.skip_log_s:
            return
        self._skip_logged[key] = now
        self.n[f"skip {d.reason}"] += 1
        self.journal.write({"wall": self.wall(), "event": "POSITION_MANAGER SKIP", "condition": self.hist.condition,
                            **d.row()})

    # ---------------------------------------------------------------- orders
    def choose(self, decisions: dict) -> Decision | None:
        buys = [d for d in decisions.values() if d.action == "BUY"]
        return max(buys, key=lambda d: d.score) if buys else None

    def reserve(self, d: Decision) -> ManagerOrder:
        """Reserve the order's exposure before it is sent (FILLED + PENDING)."""
        self._seq += 1
        reserve = float(d.metrics.get("capital_max") or d.fill.cost)
        o = ManagerOrder(None, "POSITION_MANAGER", d.side, d.classification, d.fill.qty, reserve,
                         d.fill.vwap, d.limit, t_submit=self.wall(), condition=self.hist.condition,
                         reason_row=d.row())
        self.orders[self._seq] = o
        h = self.hist.sides[d.side]
        h.in_flight = True
        self.hist.orders += 1
        h.orders += 1
        self.hist.spend += reserve
        self.n["orders"] += 1
        self.n[f"orders {d.classification}"] += 1
        self.journal.write({"wall": self.wall(), "event": f"POSITION_MANAGER BUY {d.side}",
                            "condition": self.hist.condition, "seq": self._seq, **d.row()})
        self.on_event("manager", f"BUY {d.side} {d.fill.qty:.2f} @<= {d.limit:.2f} ({d.classification})", "info")
        return o

    def settle_order(self, o: ManagerOrder, *, ok: bool, fill: dict | None, error: str | None) -> None:
        """The order's actual outcome (never its request) releases the reservation."""
        h = self.hist.sides[o.side]
        h.in_flight = False
        h.last_done_mono = self.mono()
        o.t_done = self.wall()
        if ok and fill:
            o.order_id = fill.get("order_id")
            o.filled_qty = float(fill.get("shares") or 0.0)
            o.actual_price = fill.get("average_price")
            o.fees = float(fill.get("fee") or 0.0)
            o.cost = float(fill.get("total_cost") or 0.0)
            # FOK: the whole order executed. Fewer shares than planned means the book
            # moved up (still inside the limit) before it arrived - a worse price, not a
            # partial fill; requested_qty/filled_qty in the journal show it.
            o.status = "PARTIAL" if fill.get("partial") else "FILLED"
            if o.order_id:
                self.manager_order_ids.add(str(o.order_id))
            if o.actual_price is not None:
                h.last_buy_price = float(o.actual_price)
            self.n["filled" if o.status == "FILLED" else "partial"] += 1
        else:
            o.error = str(error or "no fill")[:200]
            o.status = "ZERO_FILL" if "no-fill" in o.error.lower() else "REJECTED"   # FOK: killed, nothing bought
            self.n["zero_fill" if o.status == "ZERO_FILL" else "rejected"] += 1
        # spend counts what was actually spent, not the reservation
        self.hist.spend += o.cost - o.reserved_usd
        self.journal.write({"wall": self.wall(), "event": f"POSITION_MANAGER RESULT {o.side}", "status": o.status,
                            "order_id": o.order_id, "classification": o.classification,
                            "requested_qty": round(o.requested_qty, 4), "filled_qty": round(o.filled_qty, 4),
                            "expected_price": o.expected_price, "actual_price": o.actual_price, "fees": o.fees,
                            "cost": o.cost, "error": o.error, "condition": o.condition})

    def _execute_paper(self, d: Decision, cond, up, down, end) -> tuple:
        """Worker thread: one FAK in the PAPER broker's 'manager' slot (the
        sniper's slot on the same token is untouched). Never in live mode."""
        if self.shadow or self.live_mode:
            raise RuntimeError("the Position Manager places no live orders (shadow-only)")
        lat = None
        if self.sniper is not None and hasattr(self.sniper, "latency_plan"):
            try:
                lat = float(self.sniper.latency_plan()[0])
            except Exception:  # noqa: BLE001
                lat = None
        guard = lambda: (self.hub.condition_id == cond and float(end) - self.wall() >= self.cfg.min_left_s)  # noqa: E731
        # FOK (fak=False): every share at or under the limit, or nothing at all
        ok = self.broker.place_trade(d.side, d.stake, up, down, cond, end, d.limit, None,
                                     latency_ms=lat, tag="manager", fak=False, pre_submit_guard=guard)
        return ok, getattr(self.broker, "last_fill", None), getattr(self.broker, "last_error", None)

    # ---------------------------------------------------------------- the task
    def start(self):
        if self._task is None and self.cfg.enabled:
            self._task = asyncio.get_running_loop().create_task(self.run(), name="position-manager")
        return self._task

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.journal.close()

    def _state_version(self):
        hub = self.hub
        vs = []
        for t in (hub.up_token, hub.down_token):
            if t:
                v = hub.book.view(str(t))
                vs.append((getattr(v, "updated_mono", None), v.best_ask))
        return (hub.condition_id, tuple(vs), len(getattr(self.ledger, "seen", ())), self._seq)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(self.cfg.poll_ms / 1000.0)
            try:
                ver = self._state_version()
                if ver == self._version:
                    continue                                   # nothing new: nothing to evaluate
                self._version = ver
                decisions = self.evaluate()
                if decisions is None:
                    continue
                for d in decisions.values():
                    self._log_skip(d)
                d = self.choose(decisions)
                if d is None:
                    continue
                if self.shadow:
                    self.n["shadow buys"] += 1
                    self.journal.write({"wall": self.wall(), "event": f"POSITION_MANAGER SHADOW BUY {d.side}",
                                        **d.row()})
                    continue
                if self._sniper_busy():                        # re-checked at the send
                    d.action, d.reason = "BLOCKED", "SNIPER_PRIORITY"
                    self._log_skip(d)
                    continue
                hub = self.hub
                cond, up, down, end = hub.condition_id, hub.up_token, hub.down_token, hub.window_end
                o = self.reserve(d)
                fut = loop.run_in_executor(None, self._execute_paper, d, cond, up, down, end)
                fut.add_done_callback(lambda f, o=o: self._done(o, f))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the manager never stops the bot; it says why
                self.n["errors"] += 1
                self.on_event("manager", f"error: {type(exc).__name__}: {exc}", "bad")

    def _done(self, o: ManagerOrder, fut) -> None:
        try:
            ok, fill, err = fut.result()
        except Exception as exc:  # noqa: BLE001
            ok, fill, err = False, None, f"{type(exc).__name__}: {exc}"
        self.settle_order(o, ok=bool(ok), fill=fill, error=err)

    # ---------------------------------------------------------------- reading
    def snapshot(self) -> dict:
        """What the dashboard shows - plain values, cheap (the last evaluation)."""
        port = self.last_port
        out = {"engine": self.cfg.enabled, "live_orders": self.cfg.live_enabled and not self.shadow,
               "shadow": self.shadow, "orders": self.hist.orders, "spend": round(self.hist.spend, 4),
               "max_orders": self.cfg.max_orders, "max_spend": self.cfg.max_market_usd, "sides": {}, "portfolio": {}}
        for side in SIDES:
            st = port.side(side) if port else SideState()
            d = self.last.get(side)
            state = "WAIT" if d is None or d.action == "WAIT" else "IMPROVE" if d.action == "BUY" else "BLOCKED"
            out["sides"][side] = {
                "shares": st.shares, "avg": st.avg, "cost": st.cost, "best_bid": st.best_bid, "best_ask": st.best_ask,
                "pending": st.pending_shares,
                "new_avg_if_buy": (d.metrics.get("projected_avg") if d is not None else None),
                "state": state, "reason": None if d is None else (d.classification if d.action == "BUY" else d.reason)}
        if port is not None:
            out["portfolio"] = {"combined_avg": port.combined_avg, "paired": port.paired,
                                "unpaired_up": port.unpaired_up, "unpaired_down": port.unpaired_down,
                                "total_cost": port.filled_cost, "pending": port.pending_usd, "fees": port.fees,
                                "exposure": port.exposure(), "unrealized": port.unrealized}
        return out

    def summary(self) -> dict:
        xs = sorted(self.eval_us)
        return {"enabled": self.cfg.enabled, "shadow": self.shadow, "counts": dict(self.n),
                "eval_us": None if not xs else {"p50": xs[len(xs) // 2], "p99": xs[int(len(xs) * 0.99)],
                                                "max": xs[-1], "n": len(xs)},
                "orders": [{"side": o.side, "classification": o.classification, "status": o.status,
                            "requested": o.requested_qty, "filled": o.filled_qty, "price": o.actual_price,
                            "cost": o.cost, "order_id": o.order_id} for o in list(self.orders.values())[-20:]]}
