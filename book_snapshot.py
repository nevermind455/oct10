"""Metadata exchanged between public REST book reads and feed recovery."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RestBookSnapshot:
    """Validated REST levels and their original venue/receipt metadata."""
    token_id: str
    bids: list[dict]
    asks: list[dict]
    exchange_ts_ms: int
    received_mono: float
    tick_size: float | None = None
    book_hash: str | None = None
