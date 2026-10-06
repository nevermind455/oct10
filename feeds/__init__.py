"""Hardened WebSocket / data-feed layer.

Read-only with respect to trading logic: nothing in this package decides a
side, a size, a price or a time. It maintains state and publishes an atomic
snapshot. Substitution into the bot's read points lives in `adapters.py` and
is env-gated, off by default.
"""
from .binance import BinanceTradeFeed
from .book import BookState, BookView
from .health import DISCONNECTED, LIVE, STALE, UNSYNCED, FeedHealth, redact, worst
from .hub import FeedHub, MarketSnapshot
from .poly_market import PolyMarketFeed
from .poly_user import Fill, FillStore, PolyUserFeed
from .reconcile import RestReconciler
from .supervisor import BACKOFF_LADDER, SupervisedFeed, backoff_delay

__all__ = [
    "BinanceTradeFeed", "BookState", "BookView", "FeedHealth", "FeedHub",
    "MarketSnapshot", "PolyMarketFeed", "PolyUserFeed", "Fill", "FillStore",
    "RestReconciler", "SupervisedFeed", "backoff_delay", "BACKOFF_LADDER",
    "redact", "worst", "LIVE", "STALE", "UNSYNCED", "DISCONNECTED",
]
