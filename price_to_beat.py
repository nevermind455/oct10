"""Published reference parsing and legacy exchange-TWAP calibration.

A btc-updown-5m market resolves Up when Chainlink's BTC/USD 60-second TWAP at the
round's end is at or above the same TWAP at its start: that start value is the
price to beat (Gamma's `cryptoMarketConfig` declares the TWAP, and discovery
refuses any other lookback: market_discovery.EXPECTED_TWAP_LOOKBACK_SECONDS).
The runner uses polymarket_prices.PolymarketPrices for the actual Chainlink
stream and published opening reference. The exchange-feed calculation here
remains useful for diagnostics and historical calibration:

  1. twap(): the trigger feed's own 60s time-weighted mid ending at the round's
     start - the same calculation on a different venue's prices;
  2. Calibration: once Polymarket publishes a round's value, the difference
     between it and our TWAP (the basis) corrects the next estimates, and the
     estimate's error is measured. The add-on retains its configured warmup
     and historical error margin when using the official reference as well.

Pure functions and a small state class: no network, clock or file access.
"""
from __future__ import annotations

import collections
import json
import math
import statistics

TWAP_S = 60            # = market_discovery.EXPECTED_TWAP_LOOKBACK_SECONDS (tests check they agree)
BASIS_ROUNDS = 12      # the basis is the median over this many published rounds
ERROR_ROUNDS = 24      # the margin is the largest estimate error over this many
MIN_BASIS_FOR_ERROR = 3  # an estimate's error counts once its basis had this many rounds


def twap(samples, start: float, *, span: float = TWAP_S, max_gap: float) -> tuple[float | None, str | None]:
    """The time-weighted mean of (wall, price) samples over [start - span, start], each
    price standing until the next sample; (None, why) when the samples do not cover
    that window: none at or before its beginning, or a gap longer than max_gap between
    samples in it (or from the last one to the start)."""
    begin = start - span
    prior, inside = None, []
    for t, p in samples:                       # oldest first
        if t <= begin:
            prior = (t, p)
        elif t <= start:
            inside.append((t, p))
    if prior is None:
        return None, f"no trigger price {span:g}s before the round's start (started or reconnected too late)"
    times = [prior[0]] + [t for t, _p in inside] + [start]
    gap = max(b - a for a, b in zip(times, times[1:]))
    if gap > max_gap + 1e-9:
        return None, f"a {gap:.1f}s gap in the trigger feed around the {span:g}s window"
    points = [(begin, prior[1])] + inside
    total = sum(p * (t1 - t0) for (t0, p), t1 in zip(points, [t for t, _p in points[1:]] + [start]))
    return total / span, None


def official_from_event(event) -> tuple[float | None, float | None]:
    """(priceToBeat, finalPrice) from a Gamma event's eventMetadata (a dict or a JSON
    string); None for a value that is missing, not a number, or not positive."""
    meta = event.get("eventMetadata") if isinstance(event, dict) else None
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except ValueError:
            meta = None
    if not isinstance(meta, dict):
        return None, None

    def num(v):
        if isinstance(v, bool):
            return None
        try:
            f = float(v)
        except (TypeError, ValueError, OverflowError):
            return None
        return f if math.isfinite(f) and f > 0 else None
    return num(meta.get("priceToBeat")), num(meta.get("finalPrice"))


def metadata_keys(event) -> list[str]:
    """What eventMetadata held, for a journal row when priceToBeat was not there."""
    meta = event.get("eventMetadata") if isinstance(event, dict) else None
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except ValueError:
            return ["<unparsable string>"]
    return sorted(str(k) for k in meta) if isinstance(meta, dict) else []


class Calibration:
    """Polymarket's published prices to beat against our own TWAPs.

    basis: median(official - our TWAP) over the last BASIS_ROUNDS published rounds;
    errors: |official - the estimate we used then| in bp, for estimates whose basis
    already had MIN_BASIS_FOR_ERROR rounds; margin_bp: the largest of the last
    ERROR_ROUNDS of those, never under the operator's floor."""

    def __init__(self) -> None:
        self.diffs: collections.deque = collections.deque(maxlen=BASIS_ROUNDS)
        self.errors: collections.deque = collections.deque(maxlen=ERROR_ROUNDS)
        self.seen: set = set()

    @property
    def basis(self) -> float:
        return statistics.median(self.diffs) if self.diffs else 0.0

    @property
    def checks(self) -> int:
        return len(self.errors)

    def estimate(self, raw: float | None) -> tuple[float | None, int]:
        """Our TWAP corrected by the basis, and how many rounds that basis rests on."""
        return (None if raw is None else raw + self.basis), len(self.diffs)

    def margin_bp(self, floor_bp: float) -> float:
        return max([floor_bp] + list(self.errors))

    def add(self, start: float, raw: float | None, est: float | None, basis_rounds: int, official: float) -> dict:
        """One published round. Returns its journal fields (err_bp None when not counted)."""
        if start in self.seen:
            return {}
        self.seen.add(start)
        err = None
        if est is not None and basis_rounds >= MIN_BASIS_FOR_ERROR:
            err = abs(official - est) / official * 1e4
            self.errors.append(err)
        if raw is not None:
            self.diffs.append(official - raw)
        return {"official": official, "raw": raw, "est": est, "basis_rounds": basis_rounds,
                "err_bp": None if err is None else round(err, 3), "basis_now": round(self.basis, 4),
                "checks": self.checks}
