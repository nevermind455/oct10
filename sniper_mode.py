"""BTC-tick sniper inside the bot - paper, or a capped LIVE CANARY.

Paper by default. With --live, SNIPER_ONLY=1 and SNIPER_LIVE_CANARY=1 (all
three, or run_feeds refuses to start) the same decisions go to the venue as
real FOK orders through sniper_live.LiveExecution, under hard canary limits;
nothing below changes what or when it trades, only who executes.

The latency lab's frozen plan, run on the bot's own paper broker, ledger and
settlement so its fills land in the same account:

  * when the BTC mid of the trigger feed (SNIPER_PRIMARY_FEED: Coinbase's
    BTC-USD ticker by default, or Binance's BTCUSDT book ticker, or Kraken's
    BTC/USD bbo) moves >= SNIPER_K_BP within SNIPER_WINDOW_MS,
    buy the side BTC moved toward (Up on a rise, Down on a fall)
  * fill-or-kill, SNIPER_SHARES shares, price cap = the ask seen + SNIPER_SLIP
  * live-like execution (the default): every order is really built and
    signed, exactly as live would sign it but with a throwaway key made in
    memory (no wallet, nothing sent), so it costs the same CPU and fails
    wherever the live library would refuse it. It is then matched after the
    measured one-way time to the CLOB (a round trip is probed every 5s) +
    Polymarket's taker hold (SNIPER_HOLD_MS=250) + SNIPER_SERVER_MS=25 for the
    venue's own processing - the one part only a real order could measure.
    The broker fills against the book as it is THEN, net of our own recent
    fills, refusing if the price ran past the cap.
    SNIPER_LATENCY_MS=<ms> instead gives the old fixed delay with no signing.

It fires from its own trigger socket, not from the strategy's decision loop:
the edge is gone within ~300ms of the BTC move and the loop runs about once a
second. Every paper-broker guard still applies (price band,
MIN_SECONDS_TO_EXPIRY, spread, cash, round mapping), so it is a slightly
stricter version of the lab plan, never a looser one.

Settings come from the environment, read here so the audited config.py is
untouched:
    SNIPER_ENABLED=1            off unless set; ignored (with a warning) in LIVE
    SNIPER_ONLY=1               the sniper is the ONLY strategy: the main strategy
                                loop is not started (implies SNIPER_ENABLED);
                                LIVE refuses it unless SNIPER_LIVE_CANARY=1
    SNIPER_LIVE_CANARY=1        LIVE only, with SNIPER_ONLY: real orders under the
                                SNIPER_CANARY_* limits (see sniper_live.limits_from_env)
    SNIPER_K_BP=0.5             SNIPER_WINDOW_MS=250
    SNIPER_SHARES=10            SNIPER_SLIP=0.05
    SNIPER_LATENCY_MS=(unset)   SNIPER_COOLDOWN_MS=300 (per side, from the order's submission)
    SNIPER_MIN_LEFT_S=10        SNIPER_MAX_FILLS_ROUND=20
    SNIPER_MAX_SIDE_SHARES=200  SNIPER_STALE_MS=2000
    SNIPER_PRIMARY_FEED=coinbase  the trigger feed (exec_latency.FROZEN_SPEC):
                                coinbase | binance | kraken (kraken: paper only
                                unless SNIPER_KRAKEN_LIVE=1)
    SNIPER_COINBASE_FRESHNESS=1 no Coinbase signal is acted on unless the feed is
                                shown current, from each ticker's own match time
                                (the same gate as Binance's, below):
    SNIPER_COINBASE_STALE_MS=1000 / _UNKNOWN_MS=5000 / _BACKLOG_MAX=20
    SNIPER_COINBASE_SILENT_S=5  no message that long (heartbeats come every
                                second): reconnect (0.25s..8s, jittered)
    SNIPER_BINANCE_FRESHNESS=1  the Binance socket also carries btcusdt@trade (it has
                                Binance's event time; bookTicker has none) and no
                                signal is acted on unless the feed is shown current:
    SNIPER_BINANCE_STALE_MS=1000  trade age above its rolling 5-min minimum (clock-offset
                                  free) -> "binance feed behind". A gross-stall detector
                                  only: measured 2026-10-06, Binance's trade events trail
                                  its bookTicker by up to ~400ms on their own (late trades
                                  matched the earlier quote 15% of the time vs 46% on
                                  time), so a finer threshold would block fresh ticks
    SNIPER_BINANCE_BACKLOG_MAX=20 messages queued behind the one being read
                                  -> "binance receive backlog"
    SNIPER_BINANCE_UNKNOWN_MS=5000 no trade timestamp that recent -> "binance age unknown"
    SNIPER_BINANCE_SILENT_S=5   no message that long: reconnect (0.25s..8s, jittered)
    SNIPER_EXTRA_FEEDS=         off; "coinbase" adds Coinbase's BTC-USD ticker as a
                                SECOND TRIGGER (when it is not the primary already):
                                the same rule on its own mid, and
                                whichever feed signals first fires (one shared
                                cooldown and in-flight flag: one move, one order).
                                Every decision is journalled with the feed that fired.
                                "binance_fut" adds Binance's USD-M perpetual BTCUSDT
                                book ticker the same way, gated on its own event times
                                (SNIPER_BINANCE_* limits). PAPER ONLY: LIVE refuses any
                                second trigger (exec_latency.spec_violations).
Shadow fills (SNIPER_SHADOW_MS=100,200,300,400,500; empty turns them off):
for every order it also prices, on paper arithmetic only, what the same FOK
would have got at each of those latencies after the decision, plus the mid 5s
later. Nothing is sent and nothing touches the ledger; it gives the edge-vs-
latency curve on today's market from the sniper's own decisions.

Each decision and its outcome goes to a journal beside the paper audit file;
fills are also tagged "sniper" in the audit file itself. Every order it sends
(filled or refused) is also added to the dashboard's RECENT TRADES table.
"""
from __future__ import annotations

import asyncio
import collections
import gc
import json
import math
import os
import statistics
import time
from datetime import datetime
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from pathlib import Path

import websockets

import exec_latency
from timer import now_et

try:
    import orjson
    _loads = orjson.loads
except ImportError:
    _loads = json.loads

BOOK_TICKER_URL = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
# SNIPER_BINANCE_URL may pick another route to the SAME feed (binance_race.py
# measures which reaches this machine first): only Binance spot hosts, only the
# BTCUSDT bookTicker stream - the signal itself can never change this way.
BINANCE_SPOT_HOSTS = ("stream.binance.com", "data-stream.binance.vision")


def binance_url(raw: str | None) -> str:
    if not raw or not str(raw).strip():
        return BOOK_TICKER_URL
    from urllib.parse import urlsplit
    u = urlsplit(str(raw).strip())
    if (u.scheme != "wss" or u.hostname not in BINANCE_SPOT_HOSTS or u.port not in (None, 443, 9443)
            or u.path.lower() != "/ws/btcusdt@bookticker" or u.query):
        raise ValueError(f"SNIPER_BINANCE_URL must be a Binance spot BTCUSDT bookTicker stream "
                         f"(wss://<{'|'.join(BINANCE_SPOT_HOSTS)}>[:443|:9443]/ws/btcusdt@bookTicker), got {raw!r}")
    return str(raw).strip()


def binance_stream_url(book_ticker_url: str) -> str:
    """The same host and port as the bookTicker route, as a combined stream that
    also carries btcusdt@trade. bookTicker has no exchange timestamp; the trade
    events on the SAME connection do (E), and one connection is delivered in
    order, so a trade's age bounds how far behind the book ticks around it are."""
    from urllib.parse import urlsplit
    u = urlsplit(book_ticker_url)
    return f"wss://{u.netloc}/stream?streams=btcusdt@bookTicker/btcusdt@trade"


def rx_backlog(ws) -> int | None:
    """Messages already received and waiting behind the one just read, or None
    when this websockets version does not say. After an event-loop stall the
    transport hands over everything the kernel buffered at once: that burst is
    old data delivered in a few milliseconds, which would read as a fast move."""
    rm = getattr(ws, "recv_messages", None)                     # websockets >= 14 (asyncio)
    frames = getattr(rm, "frames", None)
    if frames is not None and hasattr(frames, "qsize"):
        return frames.qsize()
    if frames is not None and hasattr(frames, "__len__"):
        return len(frames)                                  # websockets 14-15 SimpleQueue
    msgs = getattr(ws, "messages", None)                         # legacy implementation (12-13)
    return len(msgs) if msgs is not None and hasattr(msgs, "__len__") else None


def quantiles(xs, digits: int | None = 1) -> dict | None:
    """p50/p90/p95/p99/max/n of a list exactly as sorted()[min(n-1, int(n*p/100))]
    reads them - the same values the old full sort gave - by selecting only
    those order statistics (numpy partition, which releases the GIL on a large
    array). Called on a worker for the dashboard and the journal summary rows;
    the event loop only copies the samples."""
    n = len(xs)
    if not n:
        return None
    idx = {p: min(n - 1, int(n * p / 100)) for p in (50, 90, 95, 99)}
    if n >= 2048:
        import numpy as np
        a = np.asarray(xs, dtype=float)
        kth = sorted(set(idx.values()) | {n - 1})
        part = np.partition(a, kth)
        pick = {p: float(part[i]) for p, i in idx.items()}
        top = float(part[n - 1])
    else:
        ys = sorted(xs)
        pick = {p: ys[i] for p, i in idx.items()}
        top = ys[-1]
    r = (lambda v: round(v, digits)) if digits is not None else (lambda v: v)
    return {"p50": r(pick[50]), "p90": r(pick[90]), "p95": r(pick[95]), "p99": r(pick[99]), "max": r(top), "n": n}


class BinanceFreshness:
    """Is the Binance feed current enough to act on? Fail closed: no new signal
    unless every answer is yes. Only the live socket enables it (run()); replays
    and benches that call on_tick directly keep their old behaviour.

    Freshness is measured without trusting the wall clock: each trade's age on
    arrival (receive wall - Binance E) includes a constant clock offset, so the
    rolling MINIMUM over baseline_s is that offset plus the best network path,
    and the excess over it is delay - ours, the network's, or Binance's own:
    its trade events can trail the bookTicker on the same socket by a few
    hundred ms (measured), so the excess bounds a tick's age only coarsely and
    the default threshold (1s) catches stalls, not jitter. The receive backlog
    is the exact check for our own lag. The minimum itself is reported as a
    clock check, never used to gate.

    The Coinbase trigger has its own (name="coinbase"): there the exchange
    time is each ticker's own - the match it reports - so the excess bounds
    the very tick that signals, not a neighbour's."""

    def __init__(self, stale_ms: float, unknown_ms: float, backlog_max: int, baseline_s: float = 300.0,
                 name: str = "binance") -> None:
        self.stale_ms, self.unknown_ms, self.backlog_max = stale_ms, unknown_ms, backlog_max
        self.baseline_s = baseline_s
        self.name = name
        self._why = {k: f"{name} {k}" for k in ("disconnected", "receive backlog", "age unknown", "feed behind")}
        self.enabled = False
        self.connected = False
        self._min: collections.deque = collections.deque()      # (mono, lag) increasing lag: rolling min
        self.last_trade_mono: float | None = None
        self.last_msg_mono: float | None = None
        self.last_excess: float | None = None
        self.backlog = 0
        self.backlog_max_seen = 0
        self.lag: collections.deque = collections.deque(maxlen=20000)
        self.excess: collections.deque = collections.deque(maxlen=20000)
        self.n = collections.Counter()
        self.mode = "count"                 # Settings.backlog_gate
        self.fresh_ms = 100.0               # Settings.backlog_fresh_ms

    def fresh_burst(self) -> bool:
        """A receive backlog whose newest-read tick is still within fresh_ms of
        the feed's usual delay: frames that arrived together, not a stall."""
        return (self.backlog > self.backlog_max and self.last_excess is not None
                and self.last_excess <= self.fresh_ms)

    def on_connect(self) -> None:
        self.connected = True
        self.last_trade_mono = self.last_excess = None          # nothing from the old socket vouches for this one

    def on_disconnect(self) -> None:
        self.connected = False

    def note_message(self, mono: float, backlog: int | None) -> None:
        self.last_msg_mono = mono
        if backlog is not None:
            self.backlog = backlog
            if backlog > self.backlog_max_seen:
                self.backlog_max_seen = backlog

    def note_trade(self, event_ms, recv_wall_ms: float, mono: float) -> None:
        try:
            lag = recv_wall_ms - float(event_ms)
        except (TypeError, ValueError, OverflowError):
            self.n["trade without E"] += 1
            self.last_trade_mono = self.last_excess = None
            return
        if not math.isfinite(lag):
            self.n["trade without E"] += 1
            self.last_trade_mono = self.last_excess = None
            return
        q = self._min
        while q and q[-1][1] >= lag:
            q.pop()
        q.append((mono, lag))
        cut = mono - self.baseline_s
        while q[0][0] < cut:
            q.popleft()
        self.last_excess = lag - q[0][1]
        self.last_trade_mono = mono
        self.lag.append(lag)
        self.excess.append(self.last_excess)
        self.n["trades"] += 1

    def why_not(self, now_mono: float) -> str | None:
        """None when a signal may be acted on; else the reason it must not."""
        if not self.enabled:
            return None
        if not self.connected:
            return self._why["disconnected"]
        if self.backlog > self.backlog_max and not (self.mode == "age" and self.fresh_burst()):
            return self._why["receive backlog"]
        if self.last_trade_mono is None or (now_mono - self.last_trade_mono) * 1000.0 > self.unknown_ms:
            return self._why["age unknown"]
        if self.last_excess is not None and self.last_excess > self.stale_ms:
            return self._why["feed behind"]
        return None

    def status(self, now_mono: float | None = None) -> str:
        if not self.enabled:
            return "OFF"
        now = time.monotonic() if now_mono is None else now_mono
        if not self.connected:
            return "DISCONNECTED"
        if self.last_msg_mono is None or (now - self.last_msg_mono) * 1000.0 > self.unknown_ms:
            return "STALE"
        return "LIVE" if self.why_not(now) is None else "DEGRADED"

    @staticmethod
    def _q(xs):
        return quantiles(list(xs))

    def snapshot(self) -> dict:
        """What summary() reads, copied (event loop: list() of a deque is one C call)."""
        return {"status": self.status(), "trades": self.n["trades"], "backlog_max": self.backlog_max_seen,
                "excess": list(self.excess), "lag": list(self.lag),
                "base": self._min[0][1] if self._min else None}

    def summary(self, snap: dict | None = None) -> dict:
        snap = self.snapshot() if snap is None else snap
        base = snap["base"]
        return {"status": snap["status"], "trades": snap["trades"], "backlog_max": snap["backlog_max"],
                "excess_ms": quantiles(snap["excess"]), "age_ms": quantiles(snap["lag"]),
                # receive wall - Binance E at its best: one-way network + clock offset.
                # Negative = this clock is behind Binance's by at least that much.
                "baseline_ms": round(base, 1) if base is not None else None,
                "clock": None if base is None else ("SUSPECT" if base < -5 or base > 1000 else "OK"),
                "stale_ms": self.stale_ms, "unknown_ms": self.unknown_ms, "backlog_limit": self.backlog_max}


# Kraken trigger (SNIPER_PRIMARY_FEED=kraken, PAPER only): Kraken's public v2
# WebSocket, ticker channel, BTC/USD only. event_trigger "bbo" pushes on every
# change of the best bid/offer price levels (the default, "trades", would only
# speak on a trade); v2's book channel starts at depth 10, so there is no
# depth-1 alternative. Heartbeats ({"channel":"heartbeat"}, ~1/s while nothing
# else is sent) are the liveness signal: a silent socket is a dead one.
# Measured from the PC 2026-10-06: ~12 BTC/USD updates a second, longest gap 1.4s.
PRIMARY_FEEDS = ("coinbase", "binance", "kraken")
KRAKEN_TICKER_URL = "wss://ws.kraken.com/v2"
KRAKEN_HOSTS = ("ws.kraken.com",)
KRAKEN_SYMBOL = "BTC/USD"


def kraken_subscribe(symbol: str = KRAKEN_SYMBOL) -> dict:
    return {"method": "subscribe", "params": {"channel": "ticker", "symbol": [symbol],
                                              "event_trigger": "bbo", "snapshot": True}, "req_id": 1}


KRAKEN_SUBSCRIBE = kraken_subscribe()
KRAKEN_RECV_TIMEOUT_S = 10.0      # heartbeats arrive about every second: ten of silence is a dead socket
KRAKEN_PING_S = 20.0              # application-level ping ({"method":"ping"}); the pong is ignored
KRAKEN_BACKOFF_S = 1.0            # first reconnect delay; doubles to 30s until a usable update arrives
# Heartbeats only come "in the absence of any other channel updates": a socket that
# sends them but no BTC/USD best bid/offer for this long has lost its subscription.
KRAKEN_STARVED_S = 60.0


def kraken_url(raw: str | None) -> str:
    """Only Kraken's public v2 WebSocket - mirrors binance_url(): the signal
    itself can never change by pointing this at another host."""
    if not raw or not str(raw).strip():
        return KRAKEN_TICKER_URL
    from urllib.parse import urlsplit
    u = urlsplit(str(raw).strip())
    if (u.scheme != "wss" or u.hostname not in KRAKEN_HOSTS or u.port not in (None, 443)
            or u.username or u.password or u.path != "/v2" or u.query or u.fragment):
        raise ValueError(f"SNIPER_KRAKEN_URL must be Kraken's public v2 WebSocket "
                         f"(wss://{KRAKEN_HOSTS[0]}[:443]/v2), got {raw!r}")
    return str(raw).strip()


class KrakenFeedError(RuntimeError):
    """The Kraken socket is open but is not carrying the feed asked for: reconnect."""


class KrakenSubscribeError(KrakenFeedError):
    """Kraken refused the subscription, or acknowledged a different one (another
    channel, symbol or event trigger): reconnect rather than trade on it."""


def kraken_ack(d: dict, symbol: str = KRAKEN_SYMBOL) -> None:
    """Accept only an acknowledgement of exactly ticker / <symbol> / bbo. A bbo
    subscription quietly downgraded to the default "trades" trigger would speak
    only on trades and miss most top-of-book moves."""
    if d.get("success") is not True:
        raise KrakenSubscribeError(str(d.get("error") or "subscription refused")[:120])
    res = d.get("result") if isinstance(d.get("result"), dict) else {}
    got = (res.get("channel"), res.get("symbol"), res.get("event_trigger"))
    if got != ("ticker", symbol, "bbo"):
        raise KrakenSubscribeError(f"acknowledged {got}, not ticker/{symbol}/bbo")


def _exchange_ms(ts) -> float | None:
    """A Kraken RFC 3339 timestamp ("2026-10-06T14:17:44.145473Z") as epoch ms, or None."""
    if not isinstance(ts, str):
        return None
    try:
        stamp = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return stamp.timestamp() * 1000.0 if stamp.tzinfo is not None else None
    except (ValueError, OverflowError, OSError):
        return None


def _num(v) -> float | None:
    """A finite positive JSON number (never a bool or a string), else None."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    try:
        return float(v) if math.isfinite(v) and v > 0 else None
    except OverflowError:
        return None


def kraken_mid(row, symbol: str = KRAKEN_SYMBOL) -> float | None:
    """(best bid + best ask) / 2 of one ticker row, or None for anything but a
    well-formed row of that symbol: another symbol, a missing/zero/negative/
    non-numeric side, a crossed book."""
    if not isinstance(row, dict) or row.get("symbol") != symbol:
        return None
    bid, ask = _num(row.get("bid")), _num(row.get("ask"))
    if bid is None or ask is None or bid > ask:
        return None
    return (bid + ask) / 2.0


# Coinbase trigger (SNIPER_PRIMARY_FEED=coinbase - the default, and the frozen
# spec's feed since 2026-10-10): Coinbase Exchange's public WebSocket, BTC-USD.
# Its engine is ~40ms from Amsterdam against Binance's ~127ms, so a BTC move
# that starts in the US reaches the VPS sooner through it. The "ticker" channel
# speaks on every match (a cascade of matches comes as one message) and
# carries the book's best bid/ask as of that match - on trades, NOT on every
# top-of-book change. The "heartbeat" channel (one a second, whatever the
# market does) proves a quiet socket alive.
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_HOSTS = ("ws-feed.exchange.coinbase.com",)
COINBASE_PRODUCT = "BTC-USD"
COINBASE_CHANNELS = ("ticker", "heartbeat")


def coinbase_subscribe(product: str = COINBASE_PRODUCT) -> dict:
    return {"type": "subscribe", "product_ids": [product], "channels": list(COINBASE_CHANNELS)}


COINBASE_SUBSCRIBE = coinbase_subscribe()
# Heartbeats with no BTC-USD ticker this long: the subscription is lost (BTC-USD
# trades many times a second; a minute without one is not a quiet market).
COINBASE_STARVED_S = 60.0


def coinbase_url(raw: str | None) -> str:
    """Only Coinbase Exchange's public market-data WebSocket - mirrors
    binance_url(): the signal itself can never change by pointing this at
    another host."""
    if not raw or not str(raw).strip():
        return COINBASE_WS_URL
    from urllib.parse import urlsplit
    u = urlsplit(str(raw).strip())
    if (u.scheme != "wss" or u.hostname not in COINBASE_HOSTS or u.port not in (None, 443)
            or u.username or u.password or u.path not in ("", "/") or u.query or u.fragment):
        raise ValueError(f"SNIPER_COINBASE_URL must be Coinbase Exchange's public WebSocket "
                         f"(wss://{COINBASE_HOSTS[0]}[:443]), got {raw!r}")
    return str(raw).strip()


class CoinbaseFeedError(RuntimeError):
    """The Coinbase socket is open but is not carrying the feed asked for: reconnect."""


class CoinbaseSubscribeError(CoinbaseFeedError):
    """Coinbase answered the subscription with an error, or acknowledged a
    different one (another product, a channel missing): reconnect rather than
    trade on it."""


def coinbase_ack(d: dict, product: str = COINBASE_PRODUCT) -> None:
    """Accept only an acknowledgement that covers ticker AND heartbeat for the
    product. Without the heartbeat a quiet minute would read as a dead socket;
    without the ticker there is no price."""
    got = {}
    for ch in d.get("channels") if isinstance(d.get("channels"), list) else ():
        if isinstance(ch, dict) and isinstance(ch.get("product_ids"), list):
            got[ch.get("name")] = set(ch["product_ids"])
    missing = [c for c in COINBASE_CHANNELS if product not in got.get(c, ())]
    if missing:
        raise CoinbaseSubscribeError(f"acknowledged {sorted((k, sorted(v)) for k, v in got.items() if k)}"[:160]
                                     + f", missing {'/'.join(missing)} for {product}")


def _dec(v) -> float | None:
    """A Coinbase price - a decimal string, or a JSON number - as a finite
    positive float, else None (never a bool)."""
    if isinstance(v, bool) or not isinstance(v, (str, int, float)):
        return None
    try:
        x = float(v)
    except (ValueError, OverflowError):
        return None
    return x if math.isfinite(x) and x > 0 else None


def coinbase_mid(d, product: str = COINBASE_PRODUCT) -> float | None:
    """(best bid + best ask) / 2 of one ticker message, or None for anything
    but a well-formed ticker of that product: another product, a missing/zero/
    negative/non-numeric side, a crossed book."""
    if not isinstance(d, dict) or d.get("product_id") != product:
        return None
    bid, ask = _dec(d.get("best_bid")), _dec(d.get("best_ask"))
    if bid is None or ask is None or bid > ask:
        return None
    return (bid + ask) / 2.0


# Binance USD-M perpetual BTCUSDT book ticker: a second trigger (PAPER only -
# the frozen spec has none). Its bookTicker carries the event time E, so its
# freshness gate times the very tick that signals. Raced on 2026-10-12 against
# the Coinbase ticker for 30 minutes from Dublin and from Amsterdam: first on
# 80% / 72% of the shared moves, by a median 149 / 130ms, although its data
# travels ~105-115ms from Tokyo against Coinbase's ~48ms.
BINANCE_FUT_WS_URL = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
BINANCE_FUT_SYMBOL = "BTCUSDT"

# Second-trigger feeds (SNIPER_EXTRA_FEEDS): name -> (url, subscribe message or
# None when the stream is named in the URL). A feed is never its own second
# trigger: Settings drops the primary from it.
EXTRA_FEEDS = {
    "coinbase": (COINBASE_WS_URL, COINBASE_SUBSCRIBE),
    "binance_fut": (BINANCE_FUT_WS_URL, None),
}
MIN_ORDER_USD = 1.0   # venue floor on a marketable BUY (paper_trade.MIN_MARKETABLE_BUY_USD)


def _env_float(name: str, default: float) -> float:
    """A SNIPER_* number: the default when unset or blank, else the value -
    which must parse and be finite (ValueError otherwise). A typo used to
    become the default without a word: SNIPER_MAX_SIDE_SHARES=5O ran at 200,
    SNIPER_K_BP=1.0bp at 0.5. A bad value stops the start instead."""
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return float(default)
    try:
        v = float(str(raw).strip())
    except ValueError:
        raise ValueError(f"invalid {name}={raw!r}: not a number") from None
    if not math.isfinite(v):
        raise ValueError(f"invalid {name}={raw!r}: must be finite")
    return v


def _env_bool(name: str, default: bool) -> bool:
    """A SNIPER_* switch: the default when unset or blank, else 1/true/yes/on
    or 0/false/no/off - anything else stops the start (as _env_float)."""
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"invalid {name}={raw!r}: 1 or 0")


# ---------------------------------------------------------------- fair value and Kelly size
FAIR_MIN_SAMPLES = 60         # 1s volatility samples before a fair value is trusted (a minute)
FAIR_MIN_VOL_BP = 0.2         # volatility floor, bp per sqrt(second): a dead-calm minute is not zero risk
FAIR_MID_CLAMP = 0.005        # a market mid is read as a probability inside [0.005, 0.995]
KELLY_MIN_SHARES = 5          # the venue's minimum order size
_NORMAL = statistics.NormalDist()


def norm_cdf(z: float) -> float:
    return _NORMAL.cdf(z)


def taker_fee(price: float, rate: float, exponent: int = 1) -> float:
    """The venue's taker fee per share under the market's fee curve."""
    return rate * (price * (1.0 - price)) ** exponent


def anchored_fair(market_mid: float, move_bp: float, sigma: float, tau: float) -> float:
    """What the side BTC moved toward is worth right after the move, ANCHORED to
    the market's own price for it just before (the operator's model since
    2026-10-11): the market's mid read as a probability, as a z-score, moved by
    the BTC move over the volatility left in the round, and back:

        fair = Phi( Phi^-1(mid) + |move| / (sigma * sqrt(tau)) )

    The market sets the level - where BTC stands against the round's strike, the
    start price, momentum: everything it knows better than one feed. The model
    adds only what a move of this size should do to it, which the market has
    not priced yet. sigma per sqrt(second), tau seconds left, move in bp."""
    m = min(max(float(market_mid), FAIR_MID_CLAMP), 1.0 - FAIR_MID_CLAMP)
    dz = (abs(float(move_bp)) / 1e4) / (float(sigma) * math.sqrt(float(tau)))
    return _NORMAL.cdf(_NORMAL.inv_cdf(m) + dz)


def signed_anchored_fair(market_mid: float, move_bp: float, sigma: float, tau: float) -> float:
    """anchored_fair with a SIGNED move: positive moves toward the side, negative
    against it (a reversal netted against an earlier move can leave either)."""
    m = min(max(float(market_mid), FAIR_MID_CLAMP), 1.0 - FAIR_MID_CLAMP)
    dz = (float(move_bp) / 1e4) / (float(sigma) * math.sqrt(float(tau)))
    return _NORMAL.cdf(_NORMAL.inv_cdf(m) + dz)


UNREPRICED_TOL = 0.02     # this side's anchor within 2c of 1 - the opposite decision's anchor


def whipsaw_diag(*, side: str, ask: float, shares: float, fee_rate: float, fee_exponent: int, now_wall: float,
                 mine: dict | None, opp: dict | None, prev_opp: dict | None, market_mid: float | None,
                 sigma: float | None, tau: float | None, bp: float, tick: float, margin: float,
                 opp_pending: dict | None = None) -> dict:
    """What opposite-side guards would have said about one order - DIAGNOSTICS ONLY.

    mine / opp: this round's confirmed fills per side, {"shares", "cost" (incl.
    fees), "last_wall"}; opp_pending: the opposite side's orders still in flight
    or awaiting their fill size, {"shares", "cost" (the order amount)} - a
    whipsaw inside one reply time pairs with an order not yet confirmed;
    prev_opp: the opposite side's last sent decision in this round, {"wall",
    "bp", "market_mid", "feed"}. The order is assumed to fill its shares at the
    ask seen (the decision's own estimate).

    - pair cost C = the opposite side's average cost + ask + the fee per share:
      shares this order would pair with C >= 1 settle at a sure loss;
    - worst-case round loss before / after (cost - the smaller payout);
    - cooldowns since the last opposite FILL (cd500 / cd1000) and since the last
      opposite ORDER was sent (cdo500 / cdo1000);
    - netted fair: when this side's market anchor still equals 1 - the opposite
      decision's anchor (the market has not repriced between the two), the BTC
      moves are netted (prior + this, signed) instead of counting only this one.
    """
    fee_ps = fee_rate * (ask * (1.0 - ask)) ** fee_exponent
    m_sh, m_cost = (float(mine["shares"]), float(mine["cost"])) if mine else (0.0, 0.0)
    o_sh, o_cost = (float(opp["shares"]), float(opp["cost"])) if opp else (0.0, 0.0)
    paired = min(float(shares), max(0.0, o_sh - m_sh)) if o_sh > 0 else 0.0
    out = {"held_same": round(m_sh, 4), "held_opp": round(o_sh, 4), "paired_shares": round(paired, 4)}
    guards = {"c1": False, "c1_pending": False, "cd500": False, "cd1000": False, "cdo500": False,
              "cdo1000": False, "net": False}
    if paired > 0:
        c = o_cost / o_sh + ask + fee_ps
        out.update(pair_cost=round(c, 4), locked_loss=round(max(0.0, c - 1.0) * paired, 4))
        guards["c1"] = c >= 1.0
    p_sh, p_cost = (float(opp_pending["shares"]), float(opp_pending["cost"])) if opp_pending else (0.0, 0.0)
    if p_sh > 0:
        pp = p_cost / p_sh
        p_cost += p_sh * fee_rate * (pp * (1.0 - pp)) ** fee_exponent if 0 < pp < 1 else 0.0
        out["opp_pending"] = round(p_sh, 4)
    tot_sh = o_sh + p_sh
    paired_all = min(float(shares), max(0.0, tot_sh - m_sh)) if tot_sh > 0 else 0.0
    if paired_all > 0:
        c_all = (o_cost + p_cost) / tot_sh + ask + fee_ps
        out.update(paired_incl_pending=round(paired_all, 4), pair_cost_incl_pending=round(c_all, 4))
        guards["c1_pending"] = c_all >= 1.0
    new_cost = float(shares) * (ask + fee_ps)
    pos = {side: (m_sh, m_cost), ("DOWN" if side == "UP" else "UP"): (o_sh, o_cost)}
    before = sum(cst for _s, cst in pos.values()) - min(s for s, _c in pos.values())
    pos[side] = (m_sh + float(shares), m_cost + new_cost)
    after = sum(cst for _s, cst in pos.values()) - min(s for s, _c in pos.values())
    out.update(wc_before=round(before, 4), wc_after=round(after, 4))
    if opp and opp.get("last_wall") is not None:
        dt = (now_wall - float(opp["last_wall"])) * 1000.0
        out["dt_opp_fill_ms"] = round(dt, 1)
        guards["cd500"], guards["cd1000"] = dt < 500.0, dt < 1000.0
    if prev_opp is not None:
        dto = (now_wall - float(prev_opp["wall"])) * 1000.0
        out["dt_opp_decision_ms"] = round(dto, 1)
        guards["cdo500"], guards["cdo1000"] = dto < 500.0, dto < 1000.0
        out["prev_opp"] = {k: prev_opp.get(k) for k in ("bp", "market_mid", "feed")}
        pm = prev_opp.get("market_mid")
        if market_mid is not None and pm is not None:
            unrepriced = abs(float(market_mid) - (1.0 - float(pm))) <= UNREPRICED_TOL
            out["unrepriced"] = unrepriced
            if unrepriced and sigma and tau and sigma > 0 and tau > 0:
                net = float(prev_opp.get("bp") or 0.0) + float(bp)        # signed BTC bp: up +, down -
                toward = net if side == "UP" else -net
                nf = signed_anchored_fair(float(market_mid), toward, sigma, tau)
                lim = fair_limit(nf, margin, tick, fee_rate, fee_exponent)
                out.update(netted_fair=round(nf, 4), netted_limit=lim)
                guards["net"] = ask > lim + 1e-9
    out["guards"] = guards
    return out


def fair_limit(fair: float, margin: float, tick: float, fee_rate: float, fee_exponent: int = 1) -> float:
    """The highest tick price p that still leaves the margin after its fee:
    p + fee(p) + margin <= fair. 0.0 when none does."""
    p = math.floor((fair - margin) / tick + 1e-9) * tick
    while p > 0 and p + taker_fee(p, fee_rate, fee_exponent) + margin > fair + 1e-12:
        p -= tick
    return round(max(p, 0.0), 4)


def kelly_shares(fair: float, ask: float, fee_rate: float, fraction: float, bankroll: float,
                 max_shares: float, *, held_shares: float = 0.0, opposite_shares: float = 0.0,
                 fee_exponent: int = 1) -> tuple[float, float]:
    """Incremental binary shares, accounting for the existing outcome payouts.

    Cash C and holdings U/D have terminal wealth C+U or C+D. At effective
    share cost p, full Kelly maximizes q*log(C+U+(1-p)x) +
    (1-q)*log(C+D-p*x). Fractional Kelly targets that fraction of the optimal
    total directional position, then subtracts the position already held;
    repeated signals cannot keep adding the same fractional bet.
    The second return value remains the standalone Kelly cash fraction.
    """
    p = float(ask) + taker_fee(float(ask), fee_rate, fee_exponent)
    if not 0.0 < p < 1.0 or not bankroll or bankroll <= 0:
        return 0.0, 0.0
    f = (float(fair) - p) / (1.0 - p)
    if f <= 0:
        return 0.0, f
    held, opposite = float(held_shares), float(opposite_shares)
    if not all(math.isfinite(v) and v >= 0 for v in (held, opposite, float(max_shares))):
        return 0.0, f
    wealth = bankroll + p * held + (1.0 - p) * opposite
    wanted = fraction * (float(fair) - p) * wealth / (p * (1.0 - p)) - (held - opposite)
    # A hedge may increase the optimum, but it cannot spend unavailable cash.
    return float(max(0, min(math.floor(wanted + 1e-9), math.floor(bankroll / p + 1e-9), max_shares))), f


class Volatility:
    """The trigger feed's realized volatility per sqrt(second): one sample a
    second, over the last window_s, floored at FAIR_MIN_VOL_BP; None with fewer
    than min_samples steps (a bot that just started does not guess)."""

    def __init__(self, window_s: float = 300.0, min_samples: int = FAIR_MIN_SAMPLES,
                 min_vol_bp: float = FAIR_MIN_VOL_BP) -> None:
        self.window_s, self.min_samples, self.min_vol = float(window_s), int(min_samples), min_vol_bp / 1e4
        self.last_mid: float | None = None
        self._last: tuple[float, float] | None = None       # (mono, ln mid) of the last 1s sample
        self._incs: collections.deque = collections.deque()  # (mono, dx^2, dt) per sample step
        self._sq = self._dt = 0.0
        self._steps = 0

    def observe(self, mono: float, mid: float) -> None:
        """One trigger-feed price. Cheap: called on every tick, before the rule."""
        if isinstance(mid, bool) or not isinstance(mid, (int, float)) or not (math.isfinite(mid) and mid > 0):
            return
        mid = float(mid)
        self.last_mid = mid
        last = self._last
        if last is not None and mono - last[0] < 1.0:
            return
        x = math.log(mid)
        if last is not None:
            dx2, dt = (x - last[1]) ** 2, mono - last[0]
            self._incs.append((mono, dx2, dt))
            self._sq += dx2
            self._dt += dt
            self._steps += 1
            cut = mono - self.window_s
            while self._incs and self._incs[0][0] < cut:
                _m, a, b = self._incs.popleft()
                self._sq -= a
                self._dt -= b
            if self._steps % 600 == 0:              # re-add exactly now and then: no float drift
                self._sq = sum(i[1] for i in self._incs)
                self._dt = sum(i[2] for i in self._incs)
        self._last = (mono, x)

    def vol(self) -> float | None:
        """sigma per sqrt(second), or None with fewer than min_samples steps."""
        if len(self._incs) < self.min_samples or self._dt <= 0:
            return None
        return max(math.sqrt(max(self._sq, 0.0) / self._dt), self.min_vol)


def signing_price(cap: float, tick: str) -> float:
    """The limit price to sign: the cap floored exactly to the tick, plus a
    nudge far below any tick.

    The order library floors the price with floats (round_down: floor(x*10^d)),
    and 0.57*100 is 56.999... in binary, so a cap of 0.57 was signed as 0.56 -
    a live order one tick stricter than the paper broker's exact cap. The
    nudge makes the library land on the intended tick, and because the value
    is floored to the tick first it can never round ABOVE the cap.
    """
    t = Decimal(str(tick))
    on_tick = (Decimal(str(cap)) / t).to_integral_value(rounding=ROUND_FLOOR) * t
    return float(on_tick) + 1e-9


def signing_amount(amount: float) -> float:
    """The BUY stake to sign: whole cents, plus a nudge far below a cent.

    The order library floors the dollar amount with floats as well
    (round_down: floor(x*100)/100), and 5.10*100 is 509.999... in binary, so
    a $5.10 stake was signed as $5.09: a live order buying fewer shares than
    its target and a cent less than the paper broker, which rounds the same
    stake in Decimal. Same remedy as signing_price: the stake is first set to
    whole cents exactly (half-up, as the paper broker rounds it), and the
    nudge only makes the library land on that cent - it cannot reach the
    next one. The stake never moves the price limit: that is signed apart.
    """
    cents = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(cents) + 1e-9


def fast_signer(private_key: str, chain_id: int):
    """The order library's Signer, with its key parsed once.

    The library hands the hex key to eth_account on every signature, which
    re-derives the public key (an elliptic-curve multiply) and, when the
    optional C backend coincurve is not installed, retries importing it -
    twice per order. Holding a parsed key with its backend chosen up front
    leaves only the signature's own multiply. The signed bytes are identical
    (RFC 6979 signatures are deterministic); tests_sniper_live checks it.
    """
    from eth_keys import keys
    from eth_keys.backends import get_backend
    from py_clob_client_v2.signer import Signer
    signer = Signer(private_key, chain_id)
    raw = private_key[2:] if private_key.startswith("0x") else private_key
    signer.private_key = keys.PrivateKey(bytes.fromhex(raw), backend=get_backend())
    return signer


class FastOrderBuilder:
    """OrderBuilder.build_market_order (v2) with the signature's EIP-712
    digest computed directly.

    The library builds a typed-data dict for every order, runs the generic
    encoder over it and makes a new exchange builder (re-hashing the domain)
    each time - most of what signing costs once coincurve makes the ECDSA
    step cheap. Here the digest is keccak(0x1901 || domain || hashStruct),
    the same formula the library itself applies for POLY_1271 orders, with
    the domain hash kept per exchange contract. Everything else - amounts,
    salt, timestamp, the signer check, the order object - is the library's
    own code. Signatures are byte-identical (tests_sniper_live checks it on
    random orders against the library's own signing of the same order).
    v1 and POLY_1271 orders go to the library unchanged."""

    def __init__(self, builder) -> None:
        self.b = builder
        self.signer, self.signature_type, self.funder = builder.signer, builder.signature_type, builder.funder
        self._exchange: dict = {}
        self._key = None
        self.last_sign_ns = None          # (T3, T4) of the newest order built here
        self.last_order_hash = None       # the EIP-712 digest just signed = the venue's order id

    def _pk(self):
        if self._key is None:
            pk = self.b.signer.private_key
            if not hasattr(pk, "sign_msg_hash"):                    # a plain library Signer: parse once
                from eth_keys import keys
                from eth_keys.backends import get_backend
                raw = str(pk)[2:] if str(pk).startswith("0x") else str(pk)
                pk = keys.PrivateKey(bytes.fromhex(raw), backend=get_backend())
            self._key = pk
        return self._key

    def build_market_order(self, order_args, options, version: int = 2, fee_rate_bps=None):
        import dataclasses
        from py_clob_client_v2.order_builder import builder as bm
        b = self.b
        if version != 2 or b.signature_type == bm.SignatureTypeV2.POLY_1271:
            return b.build_market_order(order_args, options, version=version, fee_rate_bps=fee_rate_bps)
        side, maker_amount, taker_amount = b.get_market_order_amounts(
            order_args.side, order_args.amount, order_args.price, bm.ROUNDING_CONFIG[options.tick_size])
        chain = b.signer.get_chain_id()
        cc = bm.get_contract_config(chain)
        exchange = cc.neg_risk_exchange_v2 if options.neg_risk else cc.exchange_v2
        eb = self._exchange.get(exchange)
        if eb is None:
            eb = self._exchange[exchange] = bm.ExchangeOrderBuilderV2(exchange, chain, b.signer)
        order = eb.build_order(bm.OrderDataV2(
            maker=b.funder, tokenId=order_args.token_id, makerAmount=str(maker_amount),
            takerAmount=str(taker_amount), side=side, signer=b._v2_order_signer(), signatureType=b.signature_type,
            timestamp=str(time.time_ns() // 1_000_000), metadata=getattr(order_args, "metadata", bm.BYTES32_ZERO),
            builder=order_args.builder_code))
        fields = dataclasses.asdict(order)
        # T3 (order constructed) and T4 (signed) for the latency report.
        t3 = time.perf_counter_ns()
        signature = self.sign(eb, order)
        self.last_sign_ns = (t3, time.perf_counter_ns())
        return bm.SignedOrderV2(**{**fields, "signature": signature})

    def build_order(self, order_args, options, version: int = 2, fee_rate_bps=None):
        """OrderBuilder.build_order (a v2 limit order, F1's share-sized FAK)
        with the fast digest; amounts, salt and fields are the library's own."""
        import dataclasses
        from py_clob_client_v2.order_builder import builder as bm
        b = self.b
        if version != 2 or b.signature_type == bm.SignatureTypeV2.POLY_1271:
            return b.build_order(order_args, options, version=version, fee_rate_bps=fee_rate_bps)
        side, maker_amount, taker_amount = b.get_order_amounts(
            order_args.side, order_args.size, order_args.price, bm.ROUNDING_CONFIG[options.tick_size])
        chain = b.signer.get_chain_id()
        cc = bm.get_contract_config(chain)
        exchange = cc.neg_risk_exchange_v2 if options.neg_risk else cc.exchange_v2
        eb = self._exchange.get(exchange)
        if eb is None:
            eb = self._exchange[exchange] = bm.ExchangeOrderBuilderV2(exchange, chain, b.signer)
        order = eb.build_order(bm.OrderDataV2(
            maker=b.funder, tokenId=order_args.token_id, makerAmount=str(maker_amount),
            takerAmount=str(taker_amount), side=side, signer=b._v2_order_signer(), signatureType=b.signature_type,
            timestamp=str(time.time_ns() // 1_000_000), metadata=getattr(order_args, "metadata", bm.BYTES32_ZERO),
            builder=order_args.builder_code, expiration=str(getattr(order_args, "expiration", 0))))
        fields = dataclasses.asdict(order)
        t3 = time.perf_counter_ns()
        signature = self.sign(eb, order)
        self.last_sign_ns = (t3, time.perf_counter_ns())
        return bm.SignedOrderV2(**{**fields, "signature": signature})

    def sign(self, eb, order) -> str:
        """The order's EIP-712 signature: '0x' + r || s || v (v = 27/28), as
        eth_account's sign_message returns it."""
        digest = order_digest_v2(eb.app_domain_separator, order)
        # The digest signed IS the order's hash - the library's build_order_hash
        # and the id the venue knows the order by (M7: an order whose reply was
        # lost is looked up by it, never sent again).
        self.last_order_hash = "0x" + digest.hex()
        sig = self._pk().sign_msg_hash(digest)
        return "0x" + (sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big") + bytes([sig.v + 27])).hex()


def order_digest_v2(domain_separator: bytes, order) -> bytes:
    """keccak(0x1901 || domain || hashStruct(order)): what a v2 order's
    signature signs, and the order's hash - the id the venue knows it by."""
    from eth_utils import keccak
    from py_clob_client_v2.order_utils import exchange_order_builder_v2 as ev2

    def word(n):
        return int(n).to_bytes(32, "big")

    def addr(a):
        return bytes(12) + bytes.fromhex(str(a)[2:])
    struct = keccak(ev2.ORDER_TYPE_HASH + word(order.salt) + addr(order.maker) + addr(order.signer)
                    + word(order.tokenId) + word(order.makerAmount) + word(order.takerAmount)
                    + word(int(order.side)) + word(int(order.signatureType)) + word(order.timestamp)
                    + ev2._bytes32(order.metadata) + ev2._bytes32(order.builder))
    return keccak(b"\x19\x01" + domain_separator + struct)


def order_hash_v2(signed, *, neg_risk: bool, chain_id: int, signer=None) -> str | None:
    """The order hash of a signed v2 order built by any builder (the fast one
    hands it over directly; this recomputes it). None if it cannot be had."""
    try:
        from py_clob_client_v2.order_builder import builder as bm
        cc = bm.get_contract_config(int(chain_id))
        eb = bm.ExchangeOrderBuilderV2(cc.neg_risk_exchange_v2 if neg_risk else cc.exchange_v2, int(chain_id), signer)
        return "0x" + order_digest_v2(eb.app_domain_separator, signed).hex()
    except Exception:  # noqa: BLE001 - no hash: an unknown outcome then halts, as before
        return None


def code_version() -> str | None:
    """Short git hash of the checkout this process started from, so a report
    can tell when the running bot is older than the code on disk."""
    try:
        import subprocess
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             cwd=os.path.dirname(os.path.abspath(__file__)),
                             capture_output=True, text=True, timeout=3)
        return out.stdout.strip() or None
    except Exception:  # noqa: BLE001 - no git, no version; nothing else depends on it
        return None


def _flag(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def only() -> bool:
    """SNIPER_ONLY: the sniper is the bot's only strategy - the main strategy
    loop is not started. LIVE refuses it unless SNIPER_LIVE_CANARY is set too."""
    return _flag("SNIPER_ONLY")


def kraken_live() -> bool:
    """SNIPER_KRAKEN_LIVE=1: the operator's explicit choice to let the live
    canary trigger from Kraken (SNIPER_PRIMARY_FEED=kraken). Without it a
    Kraken-triggered bot is paper only."""
    return _flag("SNIPER_KRAKEN_LIVE")


def live_canary() -> bool:
    """SNIPER_LIVE_CANARY: with --live and SNIPER_ONLY, the sniper sends real
    orders under the canary limits. Ignored in paper."""
    return _flag("SNIPER_LIVE_CANARY")


def enabled() -> bool:
    return _flag("SNIPER_ENABLED") or only()


MIN_LIVE_DELAYS = 30


def live_delays(path) -> tuple[list[float], float]:
    """(order-out -> match ms of every live order the matching engine
    answered - MATCHED or NO_MATCH - and the one-way taken off each reply).

    A reply comes back one network leg after the match, so the match was
    reply - one-way after the first byte left; one-way is half the median of
    the canary's own warm round trips (13.3ms until a summary lands)."""
    import statistics
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                rows.append(json.loads(line))
            except ValueError:
                continue
    rtts = [((r.get("live") or {}).get("clock") or {}).get("rtt_ms_p50") for r in rows
            if r.get("result") == "summary" and isinstance(r.get("live"), dict)]
    rtts = [float(x) for x in rtts if isinstance(x, (int, float)) and x > 0]
    one_way = statistics.median(rtts) / 2.0 if rtts else 13.3
    out = []
    for r in rows:
        if (r.get("live") is True and r.get("kind") in ("MATCHED", "NO_MATCH")
                and r.get("result") in ("filled", "refused", "pending")):   # one row per order
            try:
                ms = float(r["t_wire_to_reply_ms"]) - one_way
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(ms) and 0 <= ms < 60_000:
                out.append(ms)
    return out, one_way


class Settings:
    def __init__(self) -> None:
        # the frozen spec's trigger (exec_latency.FROZEN_SPEC): a 0.5bp move
        # (0.3bp for part of 2026-10-11)
        self.k_bp = _env_float("SNIPER_K_BP", 0.5)
        self.window_ms = _env_float("SNIPER_WINDOW_MS", 250)
        self.shares = _env_float("SNIPER_SHARES", 10)
        # the frozen spec's limit (exec_latency.FROZEN_SPEC): the ask seen + $0.05 at most
        self.slip = _env_float("SNIPER_SLIP", 0.05)
        # Set -> the old fixed delay, no signing. Unset -> live-like.
        self.fixed_latency = bool(str(os.environ.get("SNIPER_LATENCY_MS", "")).strip())
        self.latency_ms = _env_float("SNIPER_LATENCY_MS", 200)
        # Polymarket's documented taker delay on up/down markets flagged itode:
        # "The order is held for 250 ms, then validation runs again" (docs,
        # concepts/order-lifecycle). NOTE: the docs changelog says crypto is
        # 150ms since 2026-09-04 14:00 UTC (50ms from 2026-08-17). The default
        # stays 250: it also sets the end-of-round cutoff (_window), a strategy
        # decision. SNIPER_HOLD_MS=150 models the venue as the changelog has it.
        self.hold_ms = _env_float("SNIPER_HOLD_MS", 250)
        self.server_ms = _env_float("SNIPER_SERVER_MS", 25)
        self.net_default_ms = _env_float("SNIPER_NET_MS", 14)
        # Paper only - where each paper order's flight time (order out ->
        # matched) comes from. "model": measured one-way + SNIPER_HOLD_MS +
        # SNIPER_SERVER_MS, the same number for every order. "live": one delay
        # drawn per order from the live canary's own measured order-out ->
        # match times (SNIPER_DELAY_JOURNAL, default the live journal), slow
        # tail included - what live orders really waited.
        self.delay_source = str(os.environ.get("SNIPER_DELAY_SOURCE", "model")).strip().lower() or "model"
        if self.delay_source not in ("model", "live"):
            raise ValueError(f"invalid SNIPER_DELAY_SOURCE {self.delay_source!r}: model or live")
        if self.delay_source == "live" and self.fixed_latency:
            raise ValueError("SNIPER_DELAY_SOURCE=live and SNIPER_LATENCY_MS are exclusive: unset SNIPER_LATENCY_MS")
        self.delay_journal = (os.environ.get("SNIPER_DELAY_JOURNAL") or os.environ.get("LIVE_SNIPER_JOURNAL")
                              or "live_sniper.jsonl")
        # Paper realism (2026-10-16, calibrated on 1,154 live orders): the recorded book lags what
        # the venue matched - faster takers ahead of us in the hold are not in it yet - so paper
        # filled 31% of live's own orders (live 21%) for +$210.68 (live +$1.42), +$202 of it on
        # fills live never got. Meeting the book 200ms later than each order's measured delay
        # matched live: 21% vs 21%, +$7.00 vs +$1.42. Paper only (live's orders meet the real venue).
        self.paper_extra_ms = _env_float("SNIPER_PAPER_EXTRA_MS", 0.0)
        if not math.isfinite(self.paper_extra_ms) or not 0.0 <= self.paper_extra_ms <= 5000.0:
            raise ValueError(f"invalid SNIPER_PAPER_EXTRA_MS {self.paper_extra_ms!r}: 0..5000 ms")
        self.delay_note = ""                   # set by a paper Sniper once the live delays are loaded
        # Per side: an order on one side starts that side's cooldown only.
        self.cooldown_ms = _env_float("SNIPER_COOLDOWN_MS", 300)
        self.min_left_s = _env_float("SNIPER_MIN_LEFT_S", 10)
        self.max_fills_round = int(_env_float("SNIPER_MAX_FILLS_ROUND", 20))
        self.max_side_shares = _env_float("SNIPER_MAX_SIDE_SHARES", 200)
        # FAK (IOC) only: what an order did not fill is added to the next
        # same-side order in the same market, the target never above this.
        self.max_target_shares = _env_float("SNIPER_MAX_TARGET_SHARES", 20)
        # Fair value (the frozen spec since 2026-10-11, the operator's rule): every buy
        # only at or under the MARKET-ANCHORED fair value (anchored_fair: the market's
        # own mid for the side just before the move, moved by what a BTC move of this
        # size should do with the volatility and time left) less the taker fee and
        # SNIPER_FAIR_MARGIN; the limit is capped there too. Under a minute of price
        # history, or no market mid, skips: never guessed.
        self.fair = _env_bool("SNIPER_FAIR", True)
        self.fair_margin = _env_float("SNIPER_FAIR_MARGIN", 0.01)
        self.fair_vol_s = _env_float("SNIPER_FAIR_VOL_S", 300)
        # Retained for environment compatibility; decisions use the prefetched
        # market's actual fee rate/exponent, never this former model assumption.
        self.fair_fee_rate = _env_float("SNIPER_FAIR_FEE_RATE", 0.07)
        # Kelly size (the frozen spec since 2026-10-11, with the fair value): FAK orders
        # target SNIPER_KELLY_FRACTION of the optimal total directional position,
        # accounting for cash and both outcome holdings, in whole shares. There is
        # no per-order cap, only what is left of the side's
        # SNIPER_MAX_SIDE_SHARES in the round (live: also the canary's per-order cap);
        # under the venue's 5-share minimum it skips. Replaces the fixed SNIPER_SHARES + carry.
        self.kelly = _env_bool("SNIPER_KELLY", True)
        self.kelly_fraction = _env_float("SNIPER_KELLY_FRACTION", 0.5)       # half Kelly since 2026-10-13 (full before; 0.25 earlier on 10-11)
        self.stale_ms = _env_float("SNIPER_STALE_MS", 2000)
        # Which book a paper order fills against. "exchange": the book as it
        # stood at the modelled match instant on the venue's own clock (the
        # feed's timestamps), waiting up to book_wait_ms for the feed to pass
        # that instant, else UNKNOWN. "local": the book in our hands at the
        # modelled instant. "wake": the old rule - whatever book is there when
        # the timer wakes, however late.
        self.book_clock = str(os.environ.get("SNIPER_BOOK_CLOCK", "exchange")).strip().lower()
        if self.book_clock not in ("exchange", "local", "wake"):
            self.book_clock = "exchange"
        self.book_wait_ms = _env_float("SNIPER_BOOK_WAIT_MS", 500)
        # Audit P2 (opt-in, off): when the feed has carried nothing past the
        # match instant by the end of the wait, a connected, gap-free, LIVE
        # feed's last book is taken as the book then - inferred, labelled
        # (arrival.evidence = "quiet") and counted. Off: UNKNOWN, as before.
        self.book_quiet = str(os.environ.get("SNIPER_BOOK_QUIET", "0")).strip().lower() in ("1", "true", "yes", "on")
        self.venue_offset_ms = _env_float("SNIPER_VENUE_CLOCK_OFFSET_MS", 0)
        raw = os.environ.get("SNIPER_SHADOW_MS", "100,200,300,400,500")
        try:
            self.shadow_ms = tuple(sorted({int(float(x)) for x in raw.split(",") if x.strip()}))
        except ValueError:
            self.shadow_ms = ()
        self.shadow_ms = tuple(x for x in self.shadow_ms if 0 <= x <= 5000)
        # Book trace (diagnostics only): the executable asks and depth at these
        # ms after the BTC tick, and at the venue's reply, for every order sent.
        # Empty turns it off. Read from the book history; nothing is sent.
        raw = os.environ.get("SNIPER_BOOK_TRACE_MS", "0,50,100,150,250")
        try:
            self.book_trace_ms = tuple(sorted({int(float(x)) for x in raw.split(",") if x.strip()}))
        except ValueError:
            self.book_trace_ms = ()
        self.book_trace_ms = tuple(x for x in self.book_trace_ms if 0 <= x <= 5000)
        self.book_trace_levels = max(1, min(20, int(_env_float("SNIPER_BOOK_TRACE_LEVELS", 5))))
        raw = os.environ.get("SNIPER_SHADOW_SLIPS", "0.03,0.04,0.06")
        try:
            self.shadow_slips = tuple(sorted({round(float(x), 4) for x in raw.split(",") if x.strip()}))
        except ValueError:
            self.shadow_slips = ()
        self.shadow_slips = tuple(x for x in self.shadow_slips if 0 < x < 0.5)
        # The shadow also prices a TIGHTER limit, ask + this, kept under the real
        # cap (fair value included): is the room above it worth having? 0 = off.
        tight = _env_float("SNIPER_SHADOW_TIGHT", 0.03)
        self.shadow_tight = tight if 0 < tight < 0.5 else 0.0
        # Diagnostics only (guard_diag rows): what a whipsaw / opposite-side
        # guard would have done with each sent order. Never changes an order.
        self.guard_diag = str(os.environ.get("SNIPER_GUARD_DIAG", "1")).strip().lower() not in ("0", "false", "no", "off")
        # Sniper-only defaults to keeping partial fills inside the same cap.
        # An explicit .env/shell choice still wins; legacy combined mode is FOK.
        default_type = "FAK" if only() else "FOK"
        self.order_type = str(os.environ.get("SNIPER_ORDER_TYPE", default_type)).strip().upper() or default_type
        if self.order_type not in ("FOK", "FAK"):
            raise ValueError(f"invalid SNIPER_ORDER_TYPE {self.order_type!r}: FOK or FAK")
        # How much a FAK spends for its share target (Kelly's, or the IOC clip +
        # carry). Polymarket fills a FAK BUY by its DOLLARS (2026-10-14: $3.74
        # signed for 11 shares at the 0.34 cap bought 12.896552 at 0.29).
        #   "shares" (default; the operator's choice 2026-10-15): the whole shares
        #       x the cap's cent limit - under the cap the saving buys extra shares,
        #       journalled as over_target, never a halt;
        #   "dollars": what the shares cost on the visible book (paper A/B).
        # Either is sent as an ordinary dollar order; the gates judge the book
        # cost either way, so the same signals qualify.
        self.order_sizing = str(os.environ.get("SNIPER_ORDER_SIZING", "shares")).strip().lower() or "shares"
        if self.order_sizing not in ("shares", "dollars"):
            raise ValueError(f"invalid SNIPER_ORDER_SIZING {self.order_sizing!r}: shares or dollars")
        self.binance_url = binance_url(os.environ.get("SNIPER_BINANCE_URL"))
        # Binance freshness gate (BinanceFreshness): on by default; 0 restores the
        # bare bookTicker socket with no exchange timestamps and no gate.
        self.binance_freshness = str(os.environ.get("SNIPER_BINANCE_FRESHNESS", "1")).strip() not in ("0", "false", "off")
        self.binance_stale_ms = _env_float("SNIPER_BINANCE_STALE_MS", 1000)
        self.binance_unknown_ms = _env_float("SNIPER_BINANCE_UNKNOWN_MS", 5000)
        self.binance_backlog_max = int(_env_float("SNIPER_BINANCE_BACKLOG_MAX", 20))
        self.binance_silent_s = _env_float("SNIPER_BINANCE_SILENT_S", 5)
        # Receive-backlog gate (audit H4). "count" - the default, unchanged: a
        # signal is refused while more than the feed's BACKLOG_MAX frames wait
        # behind the one read. "age": refused only when that tick is also more
        # than SNIPER_BACKLOG_FRESH_MS older than the feed's usual delay - frames
        # that arrived together in one network read are a burst, not a stalled
        # feed. It changes which signals pass, so it is off until approved
        # (exec_latency refuses LIVE with it).
        self.backlog_gate = str(os.environ.get("SNIPER_BACKLOG_GATE", "count")).strip().lower() or "count"
        if self.backlog_gate not in ("count", "age"):
            raise ValueError(f"invalid SNIPER_BACKLOG_GATE {self.backlog_gate!r}: count or age")
        self.backlog_fresh_ms = _env_float("SNIPER_BACKLOG_FRESH_MS", 100)
        # Latest-frame coalescing for the Binance futures trigger (audit H4):
        # frames already queued behind the one read are folded into the rule's
        # price history and only the newest is evaluated. It changes which ticks
        # the rule evaluates, so it is off until approved (LIVE refuses it).
        self.feed_coalesce = str(os.environ.get("SNIPER_FEED_COALESCE", "0")).strip().lower() in (
            "1", "true", "yes", "on")
        # Which feed triggers the sniper: Coinbase's BTC-USD ticker (the default,
        # and the frozen spec's feed since 2026-10-10), Binance's BTCUSDT book
        # ticker, or Kraken's BTC/USD best bid/offer - paper only unless
        # SNIPER_KRAKEN_LIVE=1 (Sniper and run.py refuse a live canary on it).
        # LIVE also refuses any feed but the frozen spec's (exec_latency).
        self.primary_feed = str(os.environ.get("SNIPER_PRIMARY_FEED", "coinbase")).strip().lower() or "coinbase"
        if self.primary_feed not in PRIMARY_FEEDS:
            raise ValueError(f"invalid SNIPER_PRIMARY_FEED {self.primary_feed!r}: {' or '.join(PRIMARY_FEEDS)}")
        self.kraken_url = kraken_url(os.environ.get("SNIPER_KRAKEN_URL"))
        self.coinbase_url = coinbase_url(os.environ.get("SNIPER_COINBASE_URL"))
        # Coinbase freshness gate (BinanceFreshness on each ticker's match time):
        # on by default; 0 acts on every ticker whatever its age.
        self.coinbase_freshness = str(os.environ.get("SNIPER_COINBASE_FRESHNESS", "1")).strip() not in ("0", "false", "off")
        self.coinbase_stale_ms = _env_float("SNIPER_COINBASE_STALE_MS", 1000)
        self.coinbase_unknown_ms = _env_float("SNIPER_COINBASE_UNKNOWN_MS", 5000)
        self.coinbase_backlog_max = int(_env_float("SNIPER_COINBASE_BACKLOG_MAX", 20))
        self.coinbase_silent_s = _env_float("SNIPER_COINBASE_SILENT_S", 5)
        # Coinbase sockets carrying the same ticker at once (the trigger feed only):
        # each ticker is used from whichever delivers it first (by sequence), so a
        # stalled or dropped socket costs nothing while another is up.
        self.coinbase_conns = int(_env_float("SNIPER_COINBASE_CONNS", 2))
        raw = os.environ.get("SNIPER_EXTRA_FEEDS", "")
        self.extra_feeds = tuple(dict.fromkeys(x.strip().lower() for x in raw.split(",") if x.strip()))
        unknown = [x for x in self.extra_feeds if x not in EXTRA_FEEDS]
        if unknown:
            raise ValueError(f"invalid SNIPER_EXTRA_FEEDS {unknown}: supported {sorted(EXTRA_FEEDS)}")
        # The primary is never also its own second trigger (an older .env may still
        # name coinbase there): one socket, one rule.
        self.extra_feeds = tuple(x for x in self.extra_feeds if x != self.primary_feed)
        bad = [text for ok, text in (
            (self.k_bp > 0, f"SNIPER_K_BP={self.k_bp:g} (must be > 0)"),
            (self.window_ms > 0, f"SNIPER_WINDOW_MS={self.window_ms:g} (must be > 0)"),
            (self.shares >= 5, f"SNIPER_SHARES={self.shares:g} (venue minimum is 5)"),
            (0 <= self.slip < 0.5, f"SNIPER_SLIP={self.slip:g} (must be 0 to 0.49; the price limit is also "
                                   f"capped at MAX_BUY_PRICE, so 0.45 already means 'up to MAX_BUY_PRICE' for "
                                   f"asks from 0.50)"),
            (0 <= self.latency_ms < 60_000, f"SNIPER_LATENCY_MS={self.latency_ms:g} (0 to 60000)"),
            (self.max_fills_round > 0, f"SNIPER_MAX_FILLS_ROUND={self.max_fills_round} (must be > 0)"),
            (self.max_target_shares >= self.shares,
             f"SNIPER_MAX_TARGET_SHARES={self.max_target_shares:g} (must be >= SNIPER_SHARES={self.shares:g})"),
            (self.cooldown_ms >= 0, f"SNIPER_COOLDOWN_MS={self.cooldown_ms:g} (must be >= 0)"),
            (self.min_left_s >= 0, f"SNIPER_MIN_LEFT_S={self.min_left_s:g} (must be >= 0)"),
            (self.max_side_shares > 0, f"SNIPER_MAX_SIDE_SHARES={self.max_side_shares:g} (must be > 0)"),
            (self.stale_ms > 0, f"SNIPER_STALE_MS={self.stale_ms:g} (must be > 0)"),
            (self.book_wait_ms >= 0, f"SNIPER_BOOK_WAIT_MS={self.book_wait_ms:g} (must be >= 0)"),
            (min(self.hold_ms, self.server_ms, self.net_default_ms) >= 0,
             "SNIPER_HOLD_MS / SNIPER_SERVER_MS / SNIPER_NET_MS must be >= 0"),
            (self.binance_stale_ms > 0 and self.binance_unknown_ms > 0 and self.binance_backlog_max >= 0
             and self.binance_silent_s > 0,
             "SNIPER_BINANCE_STALE_MS / _UNKNOWN_MS / _SILENT_S must be > 0, _BACKLOG_MAX >= 0"),
            (self.coinbase_stale_ms > 0 and self.coinbase_unknown_ms > 0 and self.coinbase_backlog_max >= 0
             and self.coinbase_silent_s > 0,
             "SNIPER_COINBASE_STALE_MS / _UNKNOWN_MS / _SILENT_S must be > 0, _BACKLOG_MAX >= 0"),
            (1 <= self.coinbase_conns <= 4, f"SNIPER_COINBASE_CONNS={self.coinbase_conns} (1 to 4)"),
            (0 <= self.fair_margin < 0.5, f"SNIPER_FAIR_MARGIN={self.fair_margin:g} (must be 0 to 0.49)"),
            (self.fair_vol_s >= 60, f"SNIPER_FAIR_VOL_S={self.fair_vol_s:g} (must be >= 60)"),
            (0 <= self.fair_fee_rate < 0.25, f"SNIPER_FAIR_FEE_RATE={self.fair_fee_rate:g} (must be 0 to 0.25)"),
            (0 < self.kelly_fraction <= 1, f"SNIPER_KELLY_FRACTION={self.kelly_fraction:g} (must be above 0, at most 1)")) if not ok]
        if bad:
            raise ValueError("invalid SNIPER_* settings: " + "; ".join(bad))

    def feeds_text(self) -> str:
        route = "" if self.binance_url == BOOK_TICKER_URL else f"; Binance via {self.binance_url}"
        if self.primary_feed == "kraken" and self.kraken_url != KRAKEN_TICKER_URL:
            route += f"; Kraken via {self.kraken_url}"
        if self.primary_feed == "coinbase" and self.coinbase_url != COINBASE_WS_URL:
            route += f"; Coinbase via {self.coinbase_url}"
        return route + (("; second trigger: " + ",".join(self.extra_feeds)
                         + " (whichever feed signals first; each side's cooldown is shared)") if self.extra_feeds else "")

    def lead_text(self) -> str:
        """Names the trigger on every start line when it is not Binance, so a
        journal says which feed decided its orders."""
        if self.primary_feed == "coinbase":
            return f" [triggered by Coinbase {COINBASE_PRODUCT} ticker]"
        return f" [triggered by Kraken {KRAKEN_SYMBOL} ticker (bbo), not Binance]" if self.primary_feed == "kraken" else ""

    def carry_text(self) -> str:
        return (f" (+ same-side unfilled carry, <= {self.max_target_shares:g} sh)"
                if self.order_type == "FAK" else "")

    def sizing_text(self) -> str:
        """How much a FAK spends for its shares (the venue fills a FAK BUY by its dollars)."""
        if self.order_type != "FAK":
            return ""
        return (" (sized as whole shares x the cap: under the cap the saving buys extra shares)"
                if self.order_sizing == "shares" else " (sent as their book cost: more shares only if the ask falls)")

    def extra_text(self) -> str:
        return (f" + {self.paper_extra_ms:.0f}ms calibration (the book's lag behind the venue, from live)"
                if self.paper_extra_ms else "")

    def fair_text(self) -> str:
        if not self.fair:
            return ""
        return (f"; every buy at or under the market-anchored fair value - fee - ${self.fair_margin:.2f} "
                f"(the market's mid moved by the BTC move over {self.fair_vol_s:.0f}s volatility and the time left)"
                + (f"; size {self.kelly_fraction:g} Kelly from {KELLY_MIN_SHARES} sh, no per-order cap "
                   f"(<= {self.max_side_shares:.0f} sh/side per round)"
                   if self.kelly and self.order_type == "FAK" else ""))

    def lines(self, live: bool = False) -> list[str]:
        if live:
            return [f"SNIPER LIVE CANARY{self.lead_text()}: spot mid >= {self.k_bp}bp within {self.window_ms:.0f}ms -> real {self.order_type} "
                    f"{self.shares:.0f} sh{self.carry_text()} at ask+{self.slip:.2f}{self.sizing_text()}, "
                    f"cooldown {self.cooldown_ms:.0f}ms per side, "
                    f">= {self.min_left_s:.0f}s left, <= {self.max_fills_round} fills and "
                    f"{self.max_side_shares:.0f} sh/side per round" + self.fair_text()
                    + (f"; shadow fills at {','.join(map(str, self.shadow_ms))}ms (arithmetic only)"
                       if self.shadow_ms else "") + self.feeds_text()]
        return [f"SNIPER paper-only{self.lead_text()}: spot mid >= {self.k_bp}bp within {self.window_ms:.0f}ms -> {self.order_type} "
                f"{self.shares:.0f} sh{self.carry_text()} at ask+{self.slip:.2f}{self.sizing_text()}, "
                + (f"matched {self.latency_ms:.0f}ms after deciding (fixed); " if self.fixed_latency else
                   f"real order signing + {self.delay_note}{self.extra_text()}; " if self.delay_note else
                   f"live-like: real order signing + measured one-way to the CLOB + {self.hold_ms:.0f}ms hold "
                   f"+ {self.server_ms:.0f}ms server allowance{self.extra_text()}; ")
                + f"cooldown {self.cooldown_ms:.0f}ms per side, >= {self.min_left_s:.0f}s left, "
                f"<= {self.max_fills_round} fills and {self.max_side_shares:.0f} sh/side per round"
                + self.fair_text()
                + (f"; shadow fills at {','.join(map(str, self.shadow_ms))}ms (arithmetic only)"
                   if self.shadow_ms else "")
                + ("" if self.fixed_latency else
                   f"; paper fills against the book at the modelled match instant ({self.book_clock} clock"
                   + (", a quiet feed's last book inferred" if self.book_quiet else "") + ")")
                + self.feeds_text()]


class StallWatch:
    """Who held up the event loop, and why. Diagnostics only.

    A daemon thread posts a callback to the loop every poll_s. If it has not
    run within threshold_ms the loop is stalled: the thread snapshots every
    busy thread's stack, again every sample_ms until the callback runs, and
    records, over the stall:
      cpu_ms       this process's CPU time (all threads)
      loop_cpu_ms  the loop thread's own CPU time         } Linux only, from
      loop_wait_ms the loop thread runnable but NOT run   } /proc schedstat
      steal_ms     CPU the hypervisor gave elsewhere (whole VPS)
      gc           garbage collections (gen 1-2) that overlapped
    loop_cpu ~ stall: the loop itself was busy (its stack says where).
    loop_wait ~ stall: the OS did not run it (another process, our own other
    threads at OS level, or steal). Neither: the loop was blocked - waiting
    for the GIL another thread held, or in a blocking call (stacks say which).
    Rows go to `write` from the loop itself, after the stall.
    """

    IDLE = ("threading.py", "selectors.py", "queue.py")

    def __init__(self, loop, write, threshold_ms: float = 50.0, poll_s: float = 0.02,
                 sample_ms: float = 100.0, max_per_min: int = 20) -> None:
        import threading
        self.loop, self.write = loop, write
        self.threshold_s, self.poll_s, self.sample_s = threshold_ms / 1000.0, poll_s, sample_ms / 1000.0
        self.max_per_min = max_per_min
        self.loop_ident = threading.get_ident()            # constructed on the loop thread
        self.loop_native = getattr(threading, "get_native_id", lambda: None)()
        self.gcs = collections.deque(maxlen=64)            # (start_pc, ms, generation)
        self._gc_start = {}
        self.stalls = self.suppressed = 0
        self._minute, self._in_minute = 0, 0
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="sniper-stallwatch", daemon=True)

    def start(self):
        import gc
        gc.callbacks.append(self._gc_cb)
        self.thread.start()
        return self

    def stop(self) -> None:
        import gc
        self._stop.set()
        if self._gc_cb in gc.callbacks:
            gc.callbacks.remove(self._gc_cb)

    def _gc_cb(self, phase, info) -> None:
        gen = info.get("generation", 0)
        if gen < 1:
            return
        if phase == "start":
            self._gc_start[gen] = time.perf_counter()
        elif gen in self._gc_start:
            t = self._gc_start.pop(gen)
            self.gcs.append((t, (time.perf_counter() - t) * 1000.0, gen))

    def _schedstat(self):
        try:
            with open(f"/proc/self/task/{self.loop_native}/schedstat") as fh:
                cpu_ns, wait_ns = fh.read().split()[:2]
            return int(cpu_ns), int(wait_ns)
        except (OSError, ValueError, TypeError):
            return None

    @staticmethod
    def _steal():
        try:
            with open("/proc/stat") as fh:
                f = fh.readline().split()
            return int(f[8]) * 10.0 if len(f) > 8 else None      # jiffies (10ms) -> ms
        except (OSError, ValueError, IndexError):
            return None

    def _snapshot(self) -> dict:
        import sys
        import threading
        import traceback
        names = {t.ident: t.name for t in threading.enumerate()}
        me = threading.get_ident()
        out = {}
        for ident, frame in sys._current_frames().items():
            if ident == me:
                continue
            stack = traceback.extract_stack(frame)[-6:]
            top = os.path.basename(stack[-1].filename) if stack else ""
            loop = ident == self.loop_ident
            if not loop and (top in self.IDLE or (stack and stack[-1].name == "_worker")):
                continue                                    # parked, waiting for work
            out[names.get(ident, str(ident)) + ("*" if loop else "")] = [
                f"{self._where(fs.filename)}:{fs.lineno} {fs.name}" for fs in stack]
        return out

    _STDLIB = os.path.dirname(os.__file__)

    @classmethod
    def _where(cls, filename: str) -> str:
        """Basename, tagged lib/ (third-party) or py/ (standard library); our own code untagged."""
        base = os.path.basename(filename)
        if "site-packages" in filename or "dist-packages" in filename:
            return "lib/" + base
        if filename.startswith(cls._STDLIB) or filename.startswith("<frozen"):
            return "py/" + base
        return base

    def _run(self) -> None:
        import threading
        while not self._stop.is_set():
            done = threading.Event()
            t0, cpu0 = time.perf_counter(), time.process_time()
            try:
                self.loop.call_soon_threadsafe(done.set)
            except RuntimeError:                            # loop closed
                return
            if not done.wait(self.threshold_s):
                self._stalled(done, t0, cpu0)
            self._stop.wait(self.poll_s)

    def _stalled(self, done, t0, cpu0) -> None:
        sched0, steal0 = self._schedstat(), self._steal()
        at = time.perf_counter()
        samples = [{"at_ms": round((at - t0) * 1000.0), "threads": self._snapshot()}]
        while not done.wait(self.sample_s):
            if len(samples) < 6:
                samples.append({"at_ms": round((time.perf_counter() - t0) * 1000.0), "threads": self._snapshot()})
            if time.perf_counter() - t0 > 60:
                break
        end, cpu1 = time.perf_counter(), time.process_time()
        sched1, steal1 = self._schedstat(), self._steal()
        ms = (end - t0) * 1000.0
        self.stalls += 1
        minute = int(time.time() // 60)
        if minute != self._minute:
            self._minute, self._in_minute = minute, 0
        self._in_minute += 1
        if self._in_minute > self.max_per_min and ms < 200:
            self.suppressed += 1
            return
        for s in samples[1:]:                               # keep only what changed
            s["threads"] = {k: v for k, v in s["threads"].items() if samples[0]["threads"].get(k) != v}
        row = {"wall": time.time(), "result": "stall", "ms": round(ms, 1),
               "cpu_ms": round((cpu1 - cpu0) * 1000.0, 1),
               "gc": [[g, round(d, 1)] for t, d, g in list(self.gcs) if t + d / 1000.0 >= t0 and t <= end],
               "suppressed": self.suppressed, "samples": samples}
        if sched0 and sched1:
            # measured from when the stall was seen, not from t0: scale is close enough
            row["loop_cpu_ms"] = round((sched1[0] - sched0[0]) / 1e6, 1)
            row["loop_wait_ms"] = round((sched1[1] - sched0[1]) / 1e6, 1)
            row["seen_ms"] = round((end - at) * 1000.0, 1)
        if steal0 is not None and steal1 is not None:
            row["steal_ms"] = round(steal1 - steal0, 1)
        self.suppressed = 0
        try:
            self.loop.call_soon_threadsafe(self.write, row)
        except RuntimeError:
            pass


MAX_GAP_MS = 2000.0
# The move is (mid / ref - 1) * 1e4 in floats, and a move of exactly K bp can
# come out a hair under K: 60000 -> 59997 is -0.4999999999999bp, 48740 ->
# 48742.437 is +0.4999999999988bp - a threshold move that was not sent. The
# comparison allows this much, far below the smallest step a BTC mid can make
# (0.005 on 150,000 is 3.3e-4 bp), so it admits exact-threshold moves and
# nothing genuinely smaller. The threshold itself is unchanged.
K_BP_TOLERANCE = 1e-9


class Trigger:
    """Pure signal logic: feed (monotonic ms, mid); returns +1/-1 when the mid
    has moved >= k_bp since the last price at or before window_ms ago.

    Binance's book ticker only speaks when the top of the book changes, so the
    last price heard IS the price until the next message: a quiet half second
    is no gap. A reference older than max_gap_ms is: a stalled feed must not
    turn seconds of drift into a 250ms move. On the lab's Binance recordings a
    2s limit keeps every signal the unlimited rule had (16 of 16) while the
    old 2 x lookback limit (500ms) dropped a quarter (12) - the first move out
    of a calm spell, measured against the calm price."""

    def __init__(self, k_bp: float, window_ms: float, max_gap_ms: float = MAX_GAP_MS) -> None:
        self.k_bp, self.window_ms = k_bp, window_ms
        self.max_gap_ms = max(float(max_gap_ms), float(window_ms))
        self.q: collections.deque = collections.deque()
        # (time ms, mid) of the reference the last measured move was taken
        # against: the price standing window_ms before that tick.
        self.ref: tuple[float, float] | None = None

    def hold(self, t_ms: float) -> None:
        """No change as of t_ms. A feed that only speaks when its top of book
        changes (Kraken's bbo ticker) and sent a heartbeat has proved the last
        price is still the price: carried forward, a quiet book is no gap. A
        real stall - no heartbeat either - still ages the reference out (the
        heartbeat must come within max_gap_ms of the last sample). Never a signal."""
        q = self.q
        if not q or not math.isfinite(t_ms) or t_ms <= q[-1][0] or t_ms - q[-1][0] > self.max_gap_ms:
            return
        q.append((t_ms, q[-1][1]))
        cut = t_ms - self.window_ms
        while len(q) >= 2 and q[1][0] <= cut:
            q.popleft()

    def observe(self, t_ms: float, mid: float) -> None:
        """A price folded into the history exactly as update() folds it - never
        evaluated (SNIPER_FEED_COALESCE: a frame with newer ones queued)."""
        q = self.q
        if not math.isfinite(t_ms) or not math.isfinite(mid) or mid <= 0:
            q.clear()
            return
        if q and (t_ms <= q[-1][0] or t_ms - q[-1][0] > self.max_gap_ms):
            q.clear()
        q.append((t_ms, mid))
        cut = t_ms - self.window_ms
        while len(q) >= 2 and q[1][0] <= cut:
            q.popleft()

    def update(self, t_ms: float, mid: float):
        q = self.q
        if not math.isfinite(t_ms) or not math.isfinite(mid) or mid <= 0:
            q.clear()
            return None, 0.0
        # Discrete ticks may straddle the lookback, but a stalled feed must
        # not turn seconds of movement into a 250ms move.
        if q and (t_ms <= q[-1][0] or t_ms - q[-1][0] > self.max_gap_ms):
            q.clear()
        q.append((t_ms, mid))
        cut = t_ms - self.window_ms
        while len(q) >= 2 and q[1][0] <= cut:
            q.popleft()
        if q[0][0] > cut or t_ms - q[0][0] > self.max_gap_ms:
            return None, 0.0
        self.ref = q[0]
        r = (mid / q[0][1] - 1.0) * 1e4
        if r >= self.k_bp - K_BP_TOLERANCE:
            return 1, r
        if r <= -self.k_bp + K_BP_TOLERANCE:
            return -1, r
        return None, r


class Sniper:
    SHADOW_MARK_S = 5.0   # shadow markout horizon; the checker's 5s markout
    MARKOUT_S = (1.0, 5.0)  # real-fill markout horizons, journalled as 1s / 5s
    NET_PROBE_S = 5.0     # seconds between CLOB round-trip probes
    LIVE_OUTCOME = {"MATCHED": "FILLED", "PENDING": "PENDING", "NO_MATCH": "LIVE_NO_MATCH",
                    "REJECTED": "LIVE_REJECTED", "NOT_SENT": "NOT_SENT", "AMBIGUOUS": "AMBIGUOUS"}

    def __init__(self, hub, broker, *, on_event=None, journal_path=None, settings: Settings | None = None,
                 trade_rows: list | None = None, own_rounds: bool = False, live=None) -> None:
        self.hub = hub
        # live: a sniper_live.LiveExecution - real orders (LIVE CANARY). The
        # broker is then its LiveBrokerShim: round window, price band, rules.
        self.live = live
        self.broker = broker
        self.on_event = on_event or (lambda *a, **k: None)
        self.s = settings or Settings()
        if live is not None and self.s.primary_feed == "kraken" and not kraken_live():
            raise ValueError("SNIPER_PRIMARY_FEED=kraken is paper-only: the live canary triggers from "
                             "Kraken only with SNIPER_KRAKEN_LIVE=1 set")
        self.trigger = Trigger(self.s.k_bp, self.s.window_ms)       # the primary feed's mid
        self.last_mid: float | None = None         # the Kraken / Coinbase mid last heard
        # Kraken feed quality (summary rows): arrival lag, gaps, the venue's own status.
        self.kraken_lag: collections.deque = collections.deque(maxlen=3000)
        self.kraken_gap_max = 0.0
        self.kraken_system: str | None = None
        self._kraken_prev_t: float | None = None   # None after a (re)connect: an outage is not a market gap
        # One trigger per second-trigger feed: each feed's own mid (Coinbase
        # BTC-USD and Binance BTCUSDT sit at different levels).
        self.triggers = {name: Trigger(self.s.k_bp, self.s.window_ms) for name in self.s.extra_feeds}
        self.triggers[self.s.primary_feed] = self.trigger
        self.fresh = BinanceFreshness(self.s.binance_stale_ms, self.s.binance_unknown_ms, self.s.binance_backlog_max)
        # Coinbase: its freshness gate (enabled by _coinbase_loop only, like Binance's
        # by run()), the last ticker sequence used (an older one is out of order) and
        # feed-quality figures for the summary rows.
        # The trigger feed's volatility for the fair value (SNIPER_FAIR): fed every price.
        self.vol_model = Volatility(window_s=self.s.fair_vol_s)
        # Kelly sizing reads the account's cash from here: refreshed off the order path
        # (the paper broker sums every ledger position for it) - see _bankroll_now.
        self._bankroll: float | None = None
        self._bankroll_at = -1e18
        self._spends: list = []                     # paper: (mono, $) of orders sent - see _set_bankroll
        self.last_fair: dict | None = None          # the last fair-value decision (the dashboard)
        self.cb_fresh = BinanceFreshness(self.s.coinbase_stale_ms, self.s.coinbase_unknown_ms,
                                         self.s.coinbase_backlog_max, name="coinbase")
        self._coinbase_seq: int | None = None
        self._coinbase_prev_t: float | None = None  # None after a (re)connect: an outage is not a market gap
        # SNIPER_COINBASE_CONNS > 1: the sockets up now, when each recent sequence was
        # first heard, and how far behind the other socket(s) delivered the same ticker.
        self._cb_up: set = set()
        self._cb_first: dict = {}
        self._cb_late_seq: int | None = None
        self.cb_behind: collections.deque = collections.deque(maxlen=5000)
        self.coinbase_gap_max = 0.0
        # Binance futures (second trigger, SNIPER_EXTRA_FEEDS=binance_fut): its own
        # freshness gate on each book ticker's event time, Binance's limits.
        self.bf_fresh = BinanceFreshness(self.s.binance_stale_ms, self.s.binance_unknown_ms,
                                         self.s.binance_backlog_max, name="binance_fut")
        self._bf_last_u = None
        for gate in (self.fresh, self.cb_fresh, self.bf_fresh):
            gate.mode, gate.fresh_ms = self.s.backlog_gate, self.s.backlog_fresh_ms
        self.journal = Path(journal_path) if journal_path else None
        # When each side's last order was submitted (monotonic ms, the ticks'
        # clock). An UP order cools down UP only; DOWN stays free, and back.
        self.last_fire_by_side = {"UP": -1e18, "DOWN": -1e18}
        # The order each side has in flight (None: that side is free). An UP
        # order waiting for its venue answer holds UP only; DOWN may trade -
        # its fills are told apart by order id (live) / per-order result (paper).
        self.inflight = {"UP": None, "DOWN": None}
        self.rounds = collections.defaultdict(lambda: {"fills": 0, "UP": 0.0, "DOWN": 0.0})
        # signals per round by side, and how many each gate stopped (journalled as
        # a round_summary row once the round has ended; no decision reads it)
        self.round_signals = collections.defaultdict(collections.Counter)
        # token id -> (shares, fills) the ledger already held when this process
        # started (seed_rounds). A round's caps count them from its first order
        # on: a restart must not reset a market's caps to zero.
        self.round_baseline: dict = {}
        self._fak_pending: dict = {}        # accepted orders awaiting actual trade quantities
        # FAK (IOC): shares an order did not fill, per side, for the next
        # same-side order in the same market. Never moved across sides;
        # cleared when the market changes (carry_market).
        self.carry = {"UP": 0.0, "DOWN": 0.0}
        self.carry_market = None            # (condition id, round end) the carry belongs to
        # Guard diagnostics (guard_diag rows, never the order path): this round's
        # confirmed fills and last sent decision per side, keyed (round end, side).
        self._round_fills: dict = {}
        self._round_decisions: dict = {}
        self.n = collections.Counter()
        # T0..T7 per order (exec_latency) and the outcome counts: filled after
        # the send, read by the dashboard and the summary.
        self.latency = exec_latency.LatencyStats()
        self.quality = exec_latency.ExecQuality()
        self._skip_logged: dict = {}
        self.marks = {"1s": [], "5s": []}
        self.net_rtt: collections.deque = collections.deque(maxlen=5)
        self.loop_lag: collections.deque = collections.deque(maxlen=3000)
        # Paper signs every order with a throwaway key so signing costs what
        # it costs live; live signs with the real one, in LiveExecution.
        self._builder, self._clob = ((None, {}) if self.s.fixed_latency or live is not None
                                     else self._make_builder())
        # The dashboard's RECENT TRADES list (main_bot.session_trades). The main
        # strategy empties it at each round boundary; with SNIPER_ONLY that loop
        # is not running, so own_rounds makes the sniper empty it instead.
        self.trade_rows = trade_rows
        self.own_rounds = own_rounds
        self.rows_round = None
        self._tasks: set = set()
        self._order_tasks: set = set()
        self._closing = False
        self.code = code_version()
        self._live_delays: list = []
        if live is None and self.s.delay_source == "live":
            self._load_live_delays()
        if live is not None:
            live.attach(self)

    def seed_rounds(self, ledger) -> int:
        """Count what the ledger already holds into the per-round caps: every
        unsettled position's confirmed shares and fills (one per lot) by token.
        Called once at startup, before the first order. Without it a restart
        mid-round counted that round from zero: max_side_shares 20 held 30.
        Conservative: the ledger cannot tell a sniper fill from another source's
        in the same market, so every fill counts. Returns the tokens seeded."""
        base = {}
        with ledger._lock:
            for pos in ledger.positions.values():
                if pos.settled or pos.shares <= 0:
                    continue
                fills = sum(1 for lot in pos.lots if str(lot.side or "").upper() == "BUY")
                base[str(pos.token_id)] = (float(pos.shares), int(fills))
        self.round_baseline = base
        return len(base)

    def _load_live_delays(self) -> None:
        """Paper with SNIPER_DELAY_SOURCE=live: refuse to start without enough
        live orders rather than fall back to the model quietly."""
        import random
        p = Path(self.s.delay_journal)
        if not p.is_absolute():
            p = Path(__file__).resolve().parent / p
        try:
            delays, one_way = live_delays(p)
        except OSError as exc:
            raise ValueError(f"SNIPER_DELAY_SOURCE=live: cannot read the live journal {p}: {exc}") from exc
        if len(delays) < MIN_LIVE_DELAYS:
            raise ValueError(f"SNIPER_DELAY_SOURCE=live: {len(delays)} live orders with a venue reply in {p}, "
                             f"need >= {MIN_LIVE_DELAYS}")
        self._live_delays = delays
        self._rng = random.Random()
        q = sorted(delays)
        pick = lambda f: q[min(len(q) - 1, int(len(q) * f))]  # noqa: E731
        self.s.delay_note = (f"each order's flight drawn from {len(q)} live orders' order-out -> match times "
                             f"(p50 {pick(0.5):.0f}ms, p90 {pick(0.9):.0f}ms, max {q[-1]:.0f}ms; {p.name})")

    def lines(self) -> list[str]:
        out = self.s.lines(live=self.live is not None)
        if self.live is not None:
            out.append(self.live.limits_line())
        return out

    # ---------------------------------------------------------------- journal
    def _write(self, row: dict) -> None:
        if self.journal is None:
            return
        try:
            line = json.dumps(row, separators=(",", ":"), default=str) + "\n"
            if os.name == "nt":
                # Windows cannot rename or delete a file held open (log tools,
                # test cleanup): open, append, close per row there.
                with open(self.journal, "a", encoding="utf-8") as fh:
                    fh.write(line)
                return
            self._journal_append(line.encode("utf-8"))
        except OSError:
            self._journal_close()
            self.n["journal errors"] += 1

    JOURNAL_RECHECK_S = 5.0

    def _journal_append(self, data: bytes) -> None:
        """POSIX: one write() on a kept-open O_APPEND descriptor - no open and
        close per row on the event loop (audit L9). The descriptor is reopened
        when the path no longer names the file it holds (rotated, moved or
        deleted), checked every JOURNAL_RECHECK_S, so rows never go to an
        unlinked file for longer than that. Not fsynced, as before: the journal
        is diagnostics; orders and fills are made durable elsewhere."""
        fd = getattr(self, "_jfd", None)
        now = time.monotonic()
        if fd is not None and now - getattr(self, "_jfd_checked", 0.0) > self.JOURNAL_RECHECK_S:
            self._jfd_checked = now
            try:
                st, cur = os.stat(self.journal), os.fstat(fd)
                same = (st.st_ino, st.st_dev) == (cur.st_ino, cur.st_dev)
            except OSError:
                same = False
            if not same:
                self._journal_close()
                fd = None
        if fd is None:
            fd = os.open(self.journal, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o666)
            self._jfd, self._jfd_checked = fd, now
        view = memoryview(data)
        while view:
            n = os.write(fd, view)
            view = view[n:]

    def _journal_close(self) -> None:
        fd, self._jfd = getattr(self, "_jfd", None), None
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    def _table_row(self, side: str, amount, result: str, end, *, order_id=None) -> None:
        rows = self.trade_rows
        if rows is None:
            return
        if self.own_rounds and self.rows_round is not None and end is not None and end < self.rows_round:
            return      # a round that already ended; the table has moved on
        if self.own_rounds and end != self.rows_round:
            rows.clear()
        self.rows_round = end
        if order_id:
            for old in rows:
                if old.get("order_id") == order_id:
                    old.update(amount=float(amount or 0), result=result)
                    return
        rows.append({"time_et": now_et().strftime("%b %d %H:%M:%S ET"), "phase": "sniper",
                     "side": side, "amount": float(amount or 0), "price_side": side,
                     "book_side": "", "chainlink_side": "", "result": result})
        if order_id:
            rows[-1]["order_id"] = order_id

    def roll_rounds(self) -> None:
        """Empty the table as soon as the hub is on a new round.

        Clearing only when the new round's first row arrived left the last
        round's trades on screen until the sniper next traded - often a
        minute or more into the new round.
        """
        end = getattr(self.hub, "window_end", None)
        if (self.own_rounds and self.trade_rows is not None and end
                and self.rows_round is not None and end != self.rows_round):
            self.trade_rows.clear()
            self.rows_round = end

    async def _prefetch_rules(self) -> None:
        """Warm the paper broker's market-rules cache as soon as a round starts.

        Otherwise the round's first order fetched them over HTTP inside its own
        modelled flight time; a lean live sender loads them at round start."""
        fetch = getattr(self.broker, "_rules", None)
        if fetch is None:
            return
        done: list = []
        while True:
            # this round's market, and the next one's once it is prepared - so
            # its fee curve is cached before its boundary (audit M5)
            prepared = getattr(self.hub, "prepared_round", None) or {}
            for cond in (getattr(self.hub, "condition_id", None), prepared.get("condition_id")):
                if cond and cond not in done:
                    try:
                        await asyncio.to_thread(fetch, cond)
                        done = (done + [cond])[-4:]
                        self.n["rules prefetched"] += 1
                    except Exception:  # noqa: BLE001 - the order path fetches them itself if this fails
                        self.n["rules prefetch errors"] += 1
            await asyncio.sleep(0.5)

    async def _gc_schedule(self) -> None:
        """Full garbage collections only where they cannot touch an order.

        Automatic gen-2 collections stopped the whole process for up to
        208ms on the VPS (in 124 of 265 event-loop stalls in 22h), at random
        moments - including inside order flights. Here they are made rare
        (threshold 2: 1000 gen-1 collections instead of 10) and one full
        collection runs per round in its last seconds, when the sniper may
        not trade (< min_left_s left). Young garbage is still collected
        automatically. SNIPER_GC_SCHEDULE=0 turns this off."""
        if _env_float("SNIPER_GC_SCHEDULE", 1) <= 0 or self.s.min_left_s < 4.0:
            # off, or no no-trade window to put the collection in: keep
            # Python's automatic collections rather than starve gen 2
            return
        g0, g1, _g2 = gc.get_threshold()
        gc.set_threshold(g0, g1, 1000)
        done = None
        while True:
            end = getattr(self.hub, "window_end", None)
            try:
                left = float(end) - time.time() if end else None
            except (TypeError, ValueError):
                left = None
            if left is not None and 2.0 < left < self.s.min_left_s - 2.0 and end != done:
                done = end
                t = time.perf_counter()
                found = gc.collect()
                self.n["scheduled gc"] += 1
                self._write({"wall": time.time(), "result": "gc", "ms": round((time.perf_counter() - t) * 1000, 1),
                             "collected": found, "left_s": round(left, 2)})
            await asyncio.sleep(0.5)

    def _side_held(self, end, side: str, token) -> float:
        """Shares this side already counts in the round ending at `end` (its round
        entry, else what the ledger held at startup) - never creates the entry."""
        rd = self.rounds.get(end)
        if rd is not None:
            return float(rd[side])
        return float(self.round_baseline.get(str(token), (0.0, 0))[0])

    def _kelly_inventory(self, condition, end, side: str, token, opposite_token) -> tuple[float, float]:
        """Same-side risk and guaranteed opposing shares, with no venue I/O.

        Provisional/pending fills count as same-side risk. Only confirmed ledger
        shares provide an opposing payout: a pending FAK could fill nothing.
        """
        ledger = getattr(self.live, "ledger", None) if self.live is not None else None
        ledger = ledger or getattr(self.broker, "ledger", None)
        read = getattr(ledger, "open_leg_shares", None)
        own = float(read(condition, token)) if callable(read) else 0.0
        opposite = float(read(condition, opposite_token)) if callable(read) else 0.0
        live_risk = getattr(self.live, "own_risk_shares", None)
        if callable(live_risk):
            own = max(own, float(live_risk(token, condition)))
        else:
            own = max(own, self._side_held(end, side, token))
            reserved = self.inflight.get(side)
            if isinstance(reserved, dict) and reserved.get("end") == end:
                own += float(reserved.get("risk_shares") or 0.0)
            for row, plan in self._fak_pending.values():
                if plan.get("cond") == str(condition) and row.get("side") == side:
                    own += float(plan.get("risk_shares") or 0.0)
        if not all(math.isfinite(v) and v >= 0 for v in (own, opposite)):
            raise ValueError("invalid Kelly inventory")
        return own, opposite

    def _fair_fee_parameters(self, condition, up, down) -> tuple[float, int] | None:
        """The execution market's prefetched fee curve, never a fetch or guess."""
        cached = getattr(self.broker, "cached_rules", None)
        if callable(cached):
            rules = cached(str(condition))
        else:
            hit = getattr(self.broker, "_rules_cache", {}).get(str(condition))
            ttl = float(getattr(self.broker, "_rules_cache_ttl", 600.0))
            rules = hit[0] if hit and 0 <= time.time() - float(hit[1]) <= ttl else None
        if rules is None or str(getattr(rules, "condition_id", "")) != str(condition):
            return None
        try:
            rate = float(rules.fee_rate)
            exponent = int(rules.fee_exponent)
            if (isinstance(rules.fee_exponent, bool) or exponent != rules.fee_exponent
                    or not math.isfinite(rate) or not 0 <= rate <= 1 or not 1 <= exponent <= 8):
                return None
            for attr, token in (("up_token_id", up), ("down_token_id", down)):
                mapped = getattr(rules, attr, None)
                if mapped is not None and str(mapped) != str(token):
                    return None
        except (AttributeError, TypeError, ValueError, OverflowError):
            return None
        return rate, exponent

    BANKROLL_EVERY_S = 5.0     # paper: the broker's cash is re-read this often, off the order path
    BANKROLL_STALE_S = 30.0    # older than this (no refresher running): read it on the spot
    BANKROLL_SETTLE_S = 2.0    # paper: an order sent this long before a reading may not be in it yet

    def _read_bankroll(self) -> float | None:
        """The account's cash: live, the canary's collateral reading less what its
        orders in flight reserve (plain attributes - nothing on the wire); paper,
        the broker's cash (which sums every ledger position - never per tick)."""
        try:
            if self.live is not None:
                v = getattr(self.live, "balance", None)
                if v is not None:
                    v = float(v) - float(getattr(self.live, "reserved", 0.0) or 0.0)
            else:
                read = getattr(self.broker, "cash_balance", None)
                v = read() if callable(read) else None
            v = None if v is None else float(v)
        except Exception:  # noqa: BLE001 - no reading: Kelly then skips, it never guesses
            v = None
        return v if v is not None and math.isfinite(v) else None

    def _bankroll_now(self) -> float | None:
        if self.live is not None:
            # The sender debits its collateral itself (in flight: reserved; then
            # spent): read on every decision, so two orders seconds apart never
            # both size from cash only one of them still has.
            return self._read_bankroll()
        if self._bankroll is None or time.monotonic() - self._bankroll_at > self.BANKROLL_STALE_S:
            self._set_bankroll(self._read_bankroll(), time.monotonic())
        return self._bankroll

    def _set_bankroll(self, v, read_at: float) -> None:
        """Paper: a cash reading taken at read_at, less what orders sent from
        BANKROLL_SETTLE_S before it may not have booked yet (no fill, or a fill
        the reading already holds, only makes the next order smaller until the
        reading after)."""
        cut = read_at - self.BANKROLL_SETTLE_S
        self._spends = [s for s in self._spends if s[0] >= cut]
        self._bankroll = None if v is None else v - sum(a for _t, a in self._spends)
        self._bankroll_at = read_at

    def _bankroll_spent(self, amount: float) -> None:
        """Paper: an order went out - its spend leaves the cash Kelly sizes from
        at once, not at the next reading (up to 5s later: the next order on the
        other side, or this side's after its cooldown, sized from the same cash
        and the two together bet more than Kelly)."""
        self._spends.append((time.monotonic(), float(amount)))
        if self._bankroll is not None:
            self._bankroll -= float(amount)

    async def _bankroll_loop(self) -> None:
        while True:
            t0 = time.monotonic()
            v = await asyncio.to_thread(self._read_bankroll)
            self._set_bankroll(v, t0)
            await asyncio.sleep(self.BANKROLL_EVERY_S)

    async def _round_watch(self) -> None:
        while True:
            self.roll_rounds()
            self._journal_round_signals()
            await asyncio.sleep(0.5)

    def _journal_round_signals(self) -> None:
        """One round_summary row per ended round: its signals by side and the
        signals the cooldown / in-flight gates stopped (the journal's order and
        skip rows cover the rest; skips are coalesced there)."""
        now_end = getattr(self.hub, "window_end", None)
        for end in [e for e in self.round_signals if e is not None and now_end is not None and e < now_end]:
            c = self.round_signals.pop(end)
            self._write({"wall": time.time(), "result": "round_summary", "round_end": end,
                         "signals": c["UP"] + c["DOWN"], "signals_up": c["UP"], "signals_down": c["DOWN"],
                         "cooldown_skips": c["cooldown"], "in_flight_skips": c["in_flight"]})
        self.round_signals.pop(None, None)

    def _spawn(self, coro, *, order: bool = False):
        # Keep a reference: the loop holds tasks weakly, so an unreferenced
        # markout or shadow task can be collected before it finishes.
        if self._closing and not order:
            coro.close()
            return None
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        if order:
            self._order_tasks.add(task)
            task.add_done_callback(self._order_tasks.discard)
        return task

    def _task_done(self, task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self.n["task errors"] += 1
        # An order worker that escaped its own error handling has an uncertain
        # outcome. Keep reservations and pause live decisions for five minutes.
        if task in self._order_tasks:
            kill = getattr(self.live, "kill", None)
            if kill is not None:
                # This callback runs once per completed task. Object addresses
                # can be reused by later tasks, so they are not incident IDs.
                kill.trip("order task failed: reconcile before trading")
            else:
                self._closing = True
        detail = f"{task.get_name()}: {type(exc).__name__}: {exc}"[:240]
        self._write({"wall": time.time(), "result": "task_error", "reason": detail})
        try:
            self.on_event("sniper", detail, "bad")
        except Exception:
            self.n["event errors"] += 1

    async def stop(self) -> None:
        """Stop auxiliary feeds first, then finish orders before ledger teardown.

        A worker may still be booking a fill after its asyncio waiter is
        cancelled. Draining those waiters keeps authorization and fills ahead
        of the runner's final snapshot and session persistence.
        """
        self._closing = True
        auxiliary = list(self._tasks - self._order_tasks)
        for task in auxiliary:
            task.cancel()
        await asyncio.gather(*auxiliary, return_exceptions=True)
        await asyncio.gather(*list(self._order_tasks), return_exceptions=True)
        watch = getattr(self, "stallwatch", None)
        if watch is not None:
            watch.stop()
        self._journal_close()

    # ---------------------------------------------------------------- ticks
    def on_tick(self, raw) -> None:
        """Binance spot bookTicker - the trigger when SNIPER_PRIMARY_FEED=binance.
        On the live socket the messages come wrapped by the combined stream
        ({"stream", "data"}), with btcusdt@trade events between them that only
        time the feed. With another primary, a Binance tick (a replay, a bench)
        moves no rule: it is counted and dropped."""
        t = time.monotonic() * 1000.0
        t_pc = time.perf_counter_ns()
        try:
            d = _loads(raw)
            if "data" in d:
                d = d["data"]
                if d.get("e") == "trade":
                    self.fresh.note_trade(d.get("E"), time.time() * 1000.0, t / 1000.0)
                    return
            bid, ask = _dec(d["b"]), _dec(d["a"])
            if bid is None or ask is None or bid > ask:
                raise ValueError("invalid best bid/ask")
            mid = (bid + ask) / 2.0
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError):
            self.n["binance parse errors"] += 1
            return
        if not (math.isfinite(mid) and mid > 0):
            self.n["binance parse errors"] += 1
            return
        if self.s.primary_feed != "binance":
            self.n["binance ticks ignored (not the trigger)"] += 1
            return
        self.vol_model.observe(t / 1000.0, mid)
        self._signal("binance", self.trigger, mid, t, t_pc)

    def on_coinbase(self, raw) -> str:
        """Coinbase BTC-USD ticker - the trigger when SNIPER_PRIMARY_FEED=coinbase
        (the default), or a second trigger (SNIPER_EXTRA_FEEDS). Returns what
        the message was: "tick" (a usable BTC-USD best bid/ask, fed to the
        rule), "heartbeat", "subscribed" (an ack covering ticker + heartbeat for
        BTC-USD - any other ack, or an error message, raises
        CoinbaseSubscribeError), "late" (a ticker older than one already used),
        "other" or "malformed".

        A ticker comes on a match, with the best bid/ask as of that match: the
        book can move between matches without a message. So, unlike Kraken's
        bbo heartbeats, a Coinbase heartbeat does NOT carry the last price
        forward - it only proves the socket alive, and a spell with no match
        longer than the max gap restarts the rule (a quiet feed must not turn
        seconds of drift into a 250ms move).

        The ticker's own match time is noted BEFORE the rule runs, so the
        freshness gate judges the very tick that signals."""
        t = time.monotonic() * 1000.0
        t_pc = time.perf_counter_ns()
        try:
            d = _loads(raw)
        except (ValueError, TypeError):
            d = None
        if not isinstance(d, dict):
            self.n["coinbase malformed"] += 1
            return "malformed"
        kind = d.get("type")
        if kind != "ticker":
            if kind == "heartbeat":
                self.n["coinbase heartbeats"] += 1
                return "heartbeat"
            if kind == "subscriptions":
                coinbase_ack(d)                              # raises unless ticker + heartbeat / BTC-USD
                return "subscribed"
            if kind == "error":
                raise CoinbaseSubscribeError(f"{d.get('message')}: {d.get('reason')}"[:160])
            return "other"
        mid = coinbase_mid(d)
        if mid is None:
            self.n["coinbase malformed"] += 1
            return "malformed"
        seq = d.get("sequence")
        many = self.s.coinbase_conns > 1 and self.s.primary_feed == "coinbase"
        if isinstance(seq, int) and not isinstance(seq, bool):
            if self._coinbase_seq is not None and seq <= self._coinbase_seq:
                # older than a ticker already used - with several sockets, almost
                # always the same ticker arriving second on another one
                self._cb_late_seq = seq
                self.n["coinbase already heard" if many else "coinbase out of order"] += 1
                return "late"
            self._coinbase_seq = seq
        elif many:
            self.n["coinbase malformed"] += 1                # no sequence: its copy on the other socket would count twice
            return "malformed"
        primary = self.s.primary_feed == "coinbase"
        trig = self.trigger if primary else self.triggers.get("coinbase")
        wall = time.time()
        # (no match time -> note_trade counts "trade without E"; the gate then says "age unknown")
        self.cb_fresh.note_trade(_exchange_ms(d.get("time")), wall * 1000.0, t / 1000.0)
        self.n["coinbase ticks"] += 1
        if trig is None:
            return "tick"
        fresh_enough = self.cb_fresh.why_not(t / 1000.0) is None
        if primary:
            self.last_mid = mid
            if fresh_enough:
                self.vol_model.observe(t / 1000.0, mid)
        q = trig.q
        reset = bool(q) and t - q[-1][0] > trig.max_gap_ms      # no match for that long: the rule restarts
        self._signal("coinbase", trig, mid, t, t_pc)
        if not fresh_enough:
            # _signal already journals an unsafe threshold crossing, but an
            # unsafe tick below the threshold also cannot become a future
            # reference or a volatility sample. Clear it on every refusal.
            trig.q.clear()
            trig.ref = None
        # Feed quality, after the decision so it costs the decision nothing.
        if self._coinbase_prev_t is not None:
            self.coinbase_gap_max = max(self.coinbase_gap_max, t - self._coinbase_prev_t)
        if reset:
            self.n["coinbase gaps over max gap"] += 1
        self._coinbase_prev_t = t
        return "tick"

    def on_binance_fut(self, raw, evaluate: bool = True) -> str:
        """Binance USD-M perpetual BTCUSDT book ticker - a SECOND trigger only
        (SNIPER_EXTRA_FEEDS=binance_fut; PAPER: the frozen spec has none and
        LIVE refuses it). Returns "tick" (a usable best bid/ask, fed to its own
        rule), "other" (a subscription reply) or "malformed". Each message's
        event time E is noted before the rule runs, so the freshness gate
        judges the very tick that signals. The futures price sits at its own
        level (basis, USDT): only its MOVE is used, on its own rule."""
        t = time.monotonic() * 1000.0
        t_pc = time.perf_counter_ns()
        try:
            d = _loads(raw)
        except (ValueError, TypeError):
            d = None
        if not isinstance(d, dict):
            self.n["binance_fut malformed"] += 1
            return "malformed"
        if d.get("e") != "bookTicker":
            return "other"
        bid, ask = _dec(d.get("b")), _dec(d.get("a"))
        if d.get("s") != BINANCE_FUT_SYMBOL or bid is None or ask is None or bid > ask:
            self.n["binance_fut malformed"] += 1
            return "malformed"
        self.bf_fresh.note_trade(d.get("E"), time.time() * 1000.0, t / 1000.0)
        self.n["binance_fut ticks"] += 1
        update_id = d.get("u")
        if isinstance(update_id, int) and not isinstance(update_id, bool):
            if self._bf_last_u is not None and update_id < self._bf_last_u:
                self.n["binance_fut out of order"] += 1        # counted only (one socket delivers in order)
            self._bf_last_u = update_id
        trig = self.triggers.get("binance_fut")
        if trig is not None:
            if evaluate:
                self._signal("binance_fut", trig, (bid + ask) / 2.0, t, t_pc)
            else:
                trig.observe(t, (bid + ask) / 2.0)
                self.n["binance_fut coalesced"] += 1
        return "tick"

    def on_kraken(self, raw) -> str:
        """Kraken v2 ticker - the primary trigger when SNIPER_PRIMARY_FEED=kraken.
        Returns what the message was: "tick" (a usable BTC/USD best bid/offer,
        fed to the rule), "heartbeat", "subscribed" (an ack of exactly ticker /
        BTC/USD / bbo - any other ack raises KrakenSubscribeError), "status"
        (the venue's system state, kept in kraken_system), "other" (pong, ...)
        or "malformed".

        Heartbeats prove the socket alive and never move the rule: like
        Binance's book ticker, the last price heard IS the price until the
        next message, and Trigger's max gap decides when a reference is too
        old. A message with several rows feeds only its last good one - two
        rows stamped alike would read as a clock fault and reset the rule."""
        t = time.monotonic() * 1000.0
        t_pc = time.perf_counter_ns()
        try:
            d = _loads(raw)
        except (ValueError, TypeError):
            d = None
        if not isinstance(d, dict):
            self.n["kraken malformed"] += 1
            return "malformed"
        channel = d.get("channel")
        if channel == "heartbeat":
            # Kraken only heartbeats while nothing changed: the price still stands.
            # (Measured live, 10-06: 26 quiet spells over 2s in 5 minutes each reset
            # the rule and threw away the first move out of calm.)
            self.n["kraken heartbeats"] += 1
            self.trigger.hold(t)
            return "heartbeat"
        if channel != "ticker":
            if d.get("method") == "subscribe":
                kraken_ack(d)                                # raises unless ticker / BTC/USD / bbo
                return "subscribed"
            if channel == "status":
                rows = d.get("data")
                first = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
                self.kraken_system = str(first.get("system") or "unknown")
                return "status"
            return "other"
        rows = d.get("data")
        mid = good = None
        for row in rows if isinstance(rows, list) else ():
            m = kraken_mid(row)
            if m is None:
                self.n["kraken malformed"] += 1
            else:
                mid, good = m, row
        if mid is None:
            if not isinstance(rows, list) or not rows:       # bad rows were each counted above
                self.n["kraken malformed"] += 1
            return "malformed"
        self.last_mid = mid
        q = self.trigger.q
        reset = bool(q) and t - q[-1][0] > self.trigger.max_gap_ms     # no tick and no heartbeat for that long
        self.vol_model.observe(t / 1000.0, mid)
        self._signal("kraken", self.trigger, mid, t, t_pc)
        # Feed quality, measured after the decision so it costs the decision nothing:
        # gaps between updates (one longer than the max gap resets the rule) and how
        # old each update is on arrival (receipt minus Kraken's own timestamp - a
        # rough figure: the row's timestamp is not always the moment the bbo changed).
        self.n["kraken ticks"] += 1
        if self._kraken_prev_t is not None:
            self.kraken_gap_max = max(self.kraken_gap_max, t - self._kraken_prev_t)   # longest quiet book
        if reset:
            self.n["kraken gaps over max gap"] += 1                                   # rule restarted
        self._kraken_prev_t = t
        ex = _exchange_ms(good.get("timestamp"))
        if ex is not None:
            self.kraken_lag.append(time.time() * 1000.0 - ex)
        return "tick"

    @property
    def busy(self) -> bool:
        """An order in flight on either side (the dashboard's 'busy')."""
        return any(v is not None for v in self.inflight.values())

    @busy.setter
    def busy(self, value: bool) -> None:
        """False frees both sides; True marks both in flight."""
        self.inflight = {side: ({"end": None} if value else None) for side in ("UP", "DOWN")}

    def _release(self, side: str) -> None:
        """This side's order is resolved (or was never sent): the side is free."""
        self.inflight[side] = None

    @property
    def last_fire(self) -> float:
        """The most recent submission on either side (monotonic ms)."""
        return max(self.last_fire_by_side.values())

    @last_fire.setter
    def last_fire(self, t_ms: float) -> None:
        """Set both sides' cooldown start (e.g. -1e18 clears them)."""
        self.last_fire_by_side = {"UP": t_ms, "DOWN": t_ms}

    def _gate_for(self, feed: str):
        """The freshness gate that judges a signal from this feed (None: ungated)."""
        return {"binance": self.fresh, "coinbase": self.cb_fresh, "binance_fut": self.bf_fresh}.get(feed)

    def _signal(self, feed: str, trigger, mid: float, t: float, t_pc: int) -> None:
        """The rule on one feed's mid, then the gates every feed shares: the
        signal's SIDE cooldown and one order in flight, so a move seen by two
        feeds (the same side) is one order - fired by whichever saw it first.
        Binance's counters keep their old names; a second feed's are suffixed
        with its name."""
        if self._closing:
            return
        direction, r = trigger.update(t, mid)
        if direction is None:
            return
        trig_pc = time.perf_counter_ns()                # T1: the rule fired
        primary = feed == self.s.primary_feed          # its counters keep the names the reports read
        tag = "" if primary else f" ({feed})"
        self.n["signals" + ("" if primary else f" {feed}")] += 1
        gate = self._gate_for(feed)
        if gate is not None:
            why = gate.why_not(t / 1000.0)
            if why is not None:
                if why.endswith("receive backlog"):
                    # H4 evidence: was the refused tick itself fresh (a burst) or old (a stall)?
                    self.n[f"backlog refusals {'with fresh data' if gate.fresh_burst() else 'stale'} {feed}"] += 1
                # The move was measured on ticks that cannot be shown current
                # (a backlog compresses seconds of movement into milliseconds):
                # drop the reference too, so the rule restarts on fresh ticks only.
                window = self._window_fields(feed, t)
                trigger.q.clear()
                side = "UP" if direction > 0 else "DOWN"
                self._skip(why, {"wall": time.time(), "id": f"{time.time():.3f}-{side}", "side": side,
                                 "bp": round(r, 3), "feed": feed, **window, f"{feed}_backlog": gate.backlog,
                                 f"{feed}_excess_ms": (round(gate.last_excess, 1)
                                                       if gate.last_excess is not None else None)})
                return
        side = "UP" if direction > 0 else "DOWN"
        per_round = self.round_signals[getattr(self.hub, "window_end", None)]   # diagnostics: never read here
        per_round[side] += 1
        if t - self.last_fire_by_side[side] < self.s.cooldown_ms:
            self.n["skip: cooldown" + tag] += 1         # this side only: the other side is not blocked
            per_round["cooldown"] += 1
            return
        if self.inflight[side] is not None:
            self.n["skip: previous order in flight" + tag] += 1     # this side's own order only
            per_round["in_flight"] += 1
            return
        self.inflight[side] = {"end": None}
        self._t1 = (trig_pc, mid)                       # read by _fire_now in this same call
        task = self._fire_now(side, r, t, t_pc, feed=feed)
        self._t1 = None
        if task is not None:
            # A closed window, stale book or failed live preflight is not an
            # order. Only a submitted order starts its side's cooldown, from the
            # moment it was submitted (_fire_now has sent it / handed it to the
            # matcher by now), on the same monotonic clock as the ticks.
            self.last_fire_by_side[side] = time.monotonic() * 1000.0
            self.n[f"fired {feed}"] += 1

    # ---------------------------------------------------------------- limits
    def _window(self) -> tuple[float, float]:
        """(most, least) seconds left at which an order can still fill.

        The broker only fills from TRADE_LAST_SECONDS left down to
        MIN_SECONDS_TO_EXPIRY left, and refuses an order whose latency would
        carry it past that cutoff. Signals outside are skipped here, not sent
        to be refused, so REJECT keeps meaning "the price ran past the cap".
        """
        b = self.broker
        first = float(getattr(b, "trade_window_seconds", math.inf) or math.inf)
        last = float(getattr(b, "min_seconds_to_expiry", 0.0) or 0.0) + self.latency_plan()[0] / 1000.0
        return first, max(self.s.min_left_s, last)

    # ---------------------------------------------------------------- live-like execution
    def latency_plan(self) -> tuple[float, dict]:
        """(ms the broker waits before matching, the parts it is made of).

        Live-like: measured one-way to the CLOB (half the median of the last
        round trips, the lab's 14ms until the first probe lands) + the venue's
        taker hold + a server allowance. Signing is not in here: it is real
        work done just before, so its time passes by itself.
        """
        if self.s.fixed_latency:
            return self.s.latency_ms, {"latency_mode": "fixed"}
        # SNIPER_PAPER_EXTRA_MS: paper only - a live order meets the real venue
        extra = float(getattr(self.s, "paper_extra_ms", 0.0) or 0.0) if self.live is None else 0.0
        tag = {"extra_ms": extra} if extra else {}
        if self._live_delays:
            d = self._rng.choice(self._live_delays)
            return d + extra, {"latency_mode": "live-sampled", "delay_ms": round(d, 1),
                               "delay_n": len(self._live_delays), **tag}
        measured = bool(self.net_rtt)
        net = sorted(self.net_rtt)[len(self.net_rtt) // 2] / 2.0 if measured else self.s.net_default_ms
        parts = {"latency_mode": "live-like", "net_ms": round(net, 1), "net_measured": measured,
                 "hold_ms": self.s.hold_ms, "server_ms": self.s.server_ms, **tag}
        return net + self.s.hold_ms + self.s.server_ms + extra, parts

    def _sign_now(self, plan):
        """(sign_ms, error or None, signed order) - signs on the calling thread, as live would."""
        if self.s.fixed_latency or self._builder is None:
            return None, None, None
        t = time.perf_counter()
        err, signed = self._sign(plan["token"], plan["amount"], plan["cap"], plan["tick"])
        return (time.perf_counter() - t) * 1000.0, (f"order signing failed: {err}" if err else None), signed

    def _prepare_request(self, signed):
        """Build the POST /order body and L2 headers the way post_order does
        (throwaway credentials made in memory; nothing is sent). None or error."""
        c = self._clob
        if not c.get("order_to_json"):
            return None
        try:
            payload = c["order_to_json"](signed, c["creds"].api_key, c[self.s.order_type])
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            c["l2_headers"](c["signer"], c["creds"],
                            c["RequestArgs"](method="POST", request_path=c["POST_ORDER"],
                                             body=payload, serialized_body=body))
        except Exception as exc:  # noqa: BLE001 - whatever fails here would fail live too
            return f"{type(exc).__name__}: {exc}"[:160]
        return None

    def _match(self, plan, sign_ms, st):
        """Worker thread: paper-match an order that is already signed and sent.

        The modelled network + hold + server time runs from st["sent_pc"], so
        waking this thread and the paper broker's own checks overlap it, as a
        live order on the wire is not waiting for either."""
        st["thread_pc"] = time.perf_counter_ns()          # worker running (the handoff ends)
        side, amount, up, down, cond, end, cap, _token, _tick, latency_ms = plan["send"]
        kw = {} if self.s.fixed_latency else {"latency_from_pc": st["sent_pc"]}
        if self.s.order_type == "FAK":
            kw["fak"] = True
        if getattr(self.broker, "supports_size_at_limit", False):
            kw["size_at_limit"] = True               # sized as the live sender sizes it
        if kw and self._history_book() is not None and getattr(self.broker, "supports_arrival_book", False):
            arrival_ms = plan["sent_wall"] * 1000.0 + float(latency_ms) + self.s.venue_offset_ms
            arrival_mono = plan["sent_mono"] + float(latency_ms) / 1000.0
            kw["arrival_book"] = lambda token: self._arrival_book(token, arrival_ms, arrival_mono)
        ok = self.broker.place_trade(side, amount, up, down, cond, end, cap, None,
                                     latency_ms=latency_ms, tag="sniper", **kw)
        # This order's own outcome, read on the thread that placed it: with
        # UP and DOWN in flight together the broker's other order may finish
        # in between, so the event loop must not read the broker afterwards.
        plan["result"] = {"fill": getattr(self.broker, "last_fill", None),
                          "error": getattr(self.broker, "last_error", None)}
        st.update(getattr(self.broker, "last_timing", None) or {})
        return ok, sign_ms, None, st

    @staticmethod
    def _stages(tick_pc, start_pc, call_pc, st) -> dict:
        """Where the milliseconds went between the BTC tick and the match,
        all on perf_counter_ns: event loop -> decision -> signing -> in flight
        (modelled network + hold + server) -> met the book. These four add up
        to the total. The thread handoff and the broker's checks both happen
        after the order is sent, inside the in-flight time, and are shown on
        their own (t_inflight_handoff_ms, t_checks_ms)."""
        ms = lambda a, b: round((b - a) / 1e6, 2) if a is not None and b is not None else None
        sign, signed, th, sent, ent, dl, mt = (st.get("sign_pc", call_pc), st.get("signed_pc"),
                                               st.get("thread_pc"), st.get("sent_pc"), st.get("enter_pc"),
                                               st.get("delay_pc"), st.get("matched_pc"))
        out = {"t_loop_ms": ms(tick_pc, start_pc), "t_decide_ms": ms(start_pc, sign),
               "t_sign_ms": ms(sign, signed if signed is not None else sent),
               "t_request_ms": ms(signed, sent), "t_wire_ms": ms(sent, mt),
               "t_inflight_handoff_ms": ms(st.get("handoff_pc", sent), th), "t_checks_ms": ms(ent, dl),
               "t_total_ms": ms(tick_pc, mt)}
        out = {k: v for k, v in out.items() if v is not None}
        # Every raw stamp, in microseconds after the BTC tick, so any interval
        # can be rebuilt later. Labels: tick..sent are MEASURED in this process;
        # sent -> matched_pc is the SIMULATED flight (RTT/2 measured + hold and
        # server ASSUMED); matched_pc is the paper book read; ledger_* and
        # audit_* are the durable writes AFTER pricing.
        out["ts_us"] = {k[:-3]: round((v - tick_pc) / 1000) for k, v in st.items()
                        if k.endswith("_pc") and isinstance(v, int)}
        for k in ("sign_cpu_ms", "sign_gc", "arrival"):
            if k in st:
                out[k] = st[k]
        return out

    # ---------------------------------------------------------------- the book at an instant
    def _history_book(self):
        """The hub's book store if it can answer "the book at time T", else None."""
        if self.s.book_clock == "wake":
            return None
        book = getattr(self.hub, "book", None)
        return book if hasattr(book, "view_at") and hasattr(book, "view_at_mono") else None

    def _arrival_book(self, token, arrival_ms, arrival_mono):
        """Worker thread: (BookSnapshot | None, info) - the book at the modelled
        match instant, by book_clock, plus the other rule's book as an
        alternative for comparison. Exchange clock: waits up to book_wait_ms
        for the feed to carry something stamped after the instant, so a
        stalled loop or a lagging feed cannot pass an older book off."""
        from paper_trade import PaperRejected, snapshot_from_book_view
        book = self._history_book()
        t0 = time.perf_counter()
        deadline = t0 + self.s.book_wait_ms / 1000.0
        while True:
            view, info = book.view_at(token, arrival_ms)
            if (view is not None or not str(info.get("why", "")).startswith("the feed has not yet")
                    or time.perf_counter() >= deadline):
                break
            time.sleep(0.005)
        if (view is None and getattr(self.s, "book_quiet", False) and self.s.book_clock == "exchange"
                and str(info.get("why", "")).startswith("the feed has not yet")):
            view, info = book.view_at(token, arrival_ms, allow_quiet=True)        # P2, opt-in: labelled
        local = book.view_at_mono(token, arrival_mono)
        info = dict(info)
        hwm = info.pop("hwm_ms", None)
        info.update(clock=self.s.book_clock, arrival_ms=round(arrival_ms, 1),
                    arrival_wall=(arrival_ms - self.s.venue_offset_ms) / 1000.0,
                    arrival_mono=arrival_mono, book_wait_ms=round((time.perf_counter() - t0) * 1000.0, 1),
                    hwm_lead_ms=None if hwm is None else round(hwm - arrival_ms, 1))
        if view is not None and info.get("book_ts") is not None:
            info["book_age_ms"] = round(arrival_ms - float(info["book_ts"]), 1)
        official, other_name, other = ((local, "exchange", view) if self.s.book_clock == "local"
                                       else (view, "local", local))
        if official is not None:
            info["best_ask"] = official.best_ask

        def snap(v):
            try:
                return None if v is None else snapshot_from_book_view(v, expected_token=token)
            except PaperRejected:
                return None
        info["alternatives"] = {other_name: snap(other)}
        if official is None:
            info.setdefault("why", "no book in our hands at that instant")
            return None, info
        try:
            return snapshot_from_book_view(official, expected_token=token), info
        except PaperRejected as exc:
            info["why"] = str(exc)[:120]
            return None, info

    async def _book_at(self, token, venue_ms, mono):
        """The book at an instant, for shadows and markouts (None if unknown):
        sleeps until the instant, then (exchange clock) waits for the feed."""
        book = self._history_book()
        await asyncio.sleep(max(0.0, mono - time.monotonic()))
        if book is None:
            return self.hub.book.view(token)
        if self.s.book_clock == "local":
            return book.view_at_mono(token, mono)
        deadline = time.monotonic() + self.s.book_wait_ms / 1000.0
        while True:
            view, info = book.view_at(token, venue_ms)
            if (view is not None or not str(info.get("why", "")).startswith("the feed has not yet")
                    or time.monotonic() >= deadline):
                return view
            await asyncio.sleep(0.01)

    @staticmethod
    def _outcome(filled: bool, reason, row: dict, history: bool = True) -> str:
        """One explicit label per order, instead of a generic miss."""
        if filled:
            return "FILLED"
        r = str(reason or "").lower()
        arrival = row.get("arrival") or {}
        if "unknown book state" in r:
            return "UNKNOWN_BOOK_STATE"
        if "fok no-fill" in r or "fak no-fill" in r:
            ask = arrival.get("best_ask")
            if ask is None:
                # no arrival book: refused by the pre-send check on the
                # decision book - or, on the old rule, by the book at wake-up
                return "MISS_AT_DECISION" if history else "FOK_MISS_AT_WAKE"
            return "PRICE_CAP_MISS" if float(ask) > float(row.get("cap") or 0) + 1e-9 else "INSUFFICIENT_LIQUIDITY"
        if "below venue minimum" in r:
            return "BELOW_VENUE_MINIMUM"
        if "stale" in r or "has not changed for" in r or "future-dated" in r:
            return "STALE_BOOK"
        if "spread" in r:
            return "SPREAD_TOO_WIDE"
        return "TECHNICAL_ERROR"

    def _sign(self, token, amount, cap, tick):
        try:
            # create_market_order's own range check, which the builder skips.
            t = float(tick)
            price = signing_price(cap, tick)
            if not t <= price <= 1 - t + 1e-9:
                return f"invalid price ({cap}), min: {t} - max: {1 - t}", None
            signed = self._builder.build_market_order(
                self._clob["MarketOrderArgsV2"](token_id=str(token), amount=signing_amount(amount),
                                                side=self._clob["BUY"], price=price,
                                                order_type=self._clob[self.s.order_type]),
                self._clob["CreateOrderOptions"](tick_size=tick, neg_risk=False), version=2)
        except Exception as exc:  # noqa: BLE001 - whatever the library refuses, live would too
            return f"{type(exc).__name__}: {exc}"[:160], None
        return None, signed

    @staticmethod
    def _make_builder():
        """The live order builder with a throwaway key made in memory.

        The key is random and never funded, stored or sent: it exists only so
        signing costs what it costs live. No credentials are read.
        """
        try:
            import secrets
            from py_clob_client_v2.clob_types import CreateOrderOptions, MarketOrderArgsV2, OrderType
            from py_clob_client_v2.order_builder.builder import OrderBuilder
            from py_clob_client_v2.order_builder.constants import BUY
            signer = fast_signer("0x" + secrets.token_hex(32), 137)    # as the live sender signs
            builder = FastOrderBuilder(OrderBuilder(signer))
        except Exception:  # noqa: BLE001 - no library: latency stays live-like, signing is skipped
            return None, {}
        clob = {"MarketOrderArgsV2": MarketOrderArgsV2, "CreateOrderOptions": CreateOrderOptions,
                "BUY": BUY, "FOK": OrderType.FOK, "FAK": OrderType.FAK, "signer": signer}
        try:
            # The request half of post_order, with throwaway credentials made
            # here (random, never registered or sent) so its CPU cost is real.
            import base64
            import uuid
            from py_clob_client_v2.clob_types import ApiCreds, RequestArgs
            from py_clob_client_v2.endpoints import POST_ORDER
            from py_clob_client_v2.headers.headers import create_level_2_headers
            from py_clob_client_v2.order_utils.model.order_data_v2 import order_to_json_v2
            clob.update(order_to_json=order_to_json_v2, l2_headers=create_level_2_headers,
                        RequestArgs=RequestArgs, POST_ORDER=POST_ORDER,
                        creds=ApiCreds(api_key=str(uuid.uuid4()),
                                       api_secret=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
                                       api_passphrase=secrets.token_hex(16)))
        except Exception:  # noqa: BLE001 - signing still works; the request step is skipped
            pass
        return builder, clob

    @staticmethod
    def _rtt_once(url: str):
        import http_pool
        t = time.perf_counter()
        r = http_pool.get(url, timeout=3)
        return getattr(r, "status_code", 0), (time.perf_counter() - t) * 1000.0

    async def _net_probe(self) -> None:
        """Round trip to the CLOB every 5s.

        On ONE dedicated thread, timed inside it: http_pool keeps a connection
        per thread, so probing from the shared pool landed on cold or idle
        connections and counted TLS set-up and queueing as network time. A
        live sender keeps one warm connection; so does this. The first sample
        after a (re)connect is dropped.
        """
        import concurrent.futures
        import config
        url = f"{config.CLOB_HOST.rstrip('/')}/time"
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="sniper-rtt")
        loop = asyncio.get_running_loop()
        first = True
        try:
            while True:
                try:
                    status, ms = await loop.run_in_executor(pool, self._rtt_once, url)
                    if status == 200 and not first:
                        self.net_rtt.append(ms)
                    elif status != 200:
                        self.n["net probe errors"] += 1
                    first = False
                except Exception:  # noqa: BLE001 - the latency falls back to the last good samples
                    self.n["net probe errors"] += 1
                    first = True
                await asyncio.sleep(self.NET_PROBE_S)
        finally:
            pool.shutdown(wait=False)

    def plan_gaps(self) -> list[str]:
        """Where the account's paper rules are narrower than the lab plan
        (whole round down to SNIPER_MIN_LEFT_S, asks 0.05-0.95)."""
        first, last = self._window()
        lo = float(getattr(self.broker, "min_buy_price", 0.0) or 0.0)
        hi = float(getattr(self.broker, "max_buy_price", 1.0) or 1.0)
        out = []
        if first < 300 or last > self.s.min_left_s + 1:
            out.append(f"SNIPER NARROWED: trades only from {min(first, 300):.0f}s to {last:.0f}s left "
                       f"(lab plan: 300s to {self.s.min_left_s:.0f}s) - set TRADE_LAST_SECONDS=300 "
                       f"and MIN_SECONDS_TO_EXPIRY={self.s.min_left_s:.0f}")
        if lo > 0.05 + 1e-9 or hi < 0.95 - 1e-9:
            out.append(f"SNIPER NARROWED: asks {lo:.2f}-{hi:.2f} only (lab plan: 0.05-0.95) - "
                       f"set MIN_BUY_PRICE=0.05 and MAX_BUY_PRICE=0.95")
        return out

    # ---------------------------------------------------------------- IOC target and carry
    def _carry_for(self, cond, end) -> dict:
        """The carry of this market. A different market (or round) clears both
        sides first: a remainder never outlives the market it was owed in."""
        key = (str(cond), end)
        if self.carry_market != key:
            self.carry = {"UP": 0.0, "DOWN": 0.0}
            self.carry_market = key
        return self.carry

    def carry_view(self) -> dict:
        """The carry as it applies to the hub's current market: zero on both
        sides once the market changed, even before the next order clears it."""
        key = (str(getattr(self.hub, "condition_id", None)), getattr(self.hub, "window_end", None))
        current = self.carry_market == key
        return {side: (self.carry[side] if current else 0.0) for side in ("UP", "DOWN")}

    def _settle_carry(self, plan, side: str, filled) -> None:
        """An IOC's outcome is known: the shares it did not fill wait for the
        next order on the SAME side in the SAME market (replacing that side's
        carry - the target already included it). A FOK, an order whose market
        has changed since, and an outcome nobody knows (rejected, ambiguous,
        unknown book) leave the carry as it is."""
        key = plan.get("carry_market")
        if key is None or key != self.carry_market:
            return
        left = max(0.0, float(plan["target"]) - float(filled or 0.0))
        self.carry[side] = round(left, 4) if left >= 0.01 else 0.0

    def _visible_levels(self, token: str, view) -> list:
        """Ask levels (price, size), best first, as a live account would see
        them: net of our own recent paper fills, exactly as _visible_ask."""
        net = getattr(self.broker, "_net_of_own_fills", None)
        ttl = getattr(self.broker, "own_fill_ttl_s", 0)
        recent = getattr(self.broker, "has_recent_own_fill", None)
        if net is not None and ttl and (recent is None or recent(token)):
            from paper_trade import PaperRejected, snapshot_from_book_view
            try:
                return [(float(p), float(q)) for p, q in net(snapshot_from_book_view(view, expected_token=token)).asks]
            except PaperRejected:
                pass
        return [(float(p), float(q)) for p, q in (view.asks or ())]

    @staticmethod
    def ioc_spend(levels, target: float, ask: float, cap: float) -> float:
        """Dollars that buy `target` shares on the book the decision saw: the
        levels from the best ask up to the cap, consumed in order. Shares the
        book does not show within the cap are priced at the reference ask - by
        the match more depth may be there; whatever is not bought is carried,
        never chased above the cap. Rounded up to the cent (orders are signed
        in whole cents), so a full book gives the whole target."""
        need, cost = float(target), 0.0
        for price, size in levels:
            if price > cap + 1e-9 or need <= 1e-9:
                break
            take = min(need, float(size))
            cost += take * float(price)
            need -= take
        cost += max(0.0, need) * float(ask)
        return math.ceil(cost * 100.0 - 1e-6) / 100.0

    def _visible_ask(self, token: str, view):
        """Best ask as a live account would see it.

        The broker takes our own recent paper fills off the book (live, those
        shares would be gone). Deciding from the raw book instead set the cap
        off a level live would no longer show, so paper missed more than live.
        """
        net = getattr(self.broker, "_net_of_own_fills", None)
        ttl = getattr(self.broker, "own_fill_ttl_s", 0)
        if net is None or not ttl:
            return float(view.best_ask)
        recent = getattr(self.broker, "has_recent_own_fill", None)
        if recent is not None and not recent(token):
            # None of our fills on this token is inside the window, so the
            # netted book IS the raw book: skip converting every level to
            # Decimal (~0.2ms on the hot path) - the answer is identical.
            return float(view.best_ask)
        from paper_trade import PaperRejected, snapshot_from_book_view
        try:
            book = net(snapshot_from_book_view(view, expected_token=token))
        except PaperRejected:
            return float(view.best_ask)
        return float(book.asks[0][0]) if book.asks else None

    # ---------------------------------------------------------------- one order
    def _skip(self, why, row):
        self.n["skip: " + why] += 1
        row["result"] = "skipped"
        row["reason"] = why
        # Eligibility is checked on every signal now; coalesce repeated disk
        # writes without delaying a newly eligible order or losing counters.
        key = (why, row.get("round_end"), row.get("side"))
        now = time.monotonic()
        previous = self._skip_logged.get(key)
        if previous is not None and now - previous < 1.0:
            return None
        self._skip_logged[key] = now
        while len(self._skip_logged) > 128:
            self._skip_logged.pop(next(iter(self._skip_logged)))
        row["skip_count"] = self.n["skip: " + why]
        self._write(row)

    async def _fire(self, side: str, r_bp: float, t_ms: float, t_pc: int | None = None,
                    feed: str | None = None) -> None:
        """Decide, send and wait for the outcome - for callers that await one
        order. The tick handler uses _fire_now and does not wait. feed: the
        feed that signalled (None: the trigger feed, SNIPER_PRIMARY_FEED)."""
        task = self._fire_now(side, r_bp, t_ms, t_pc, feed=feed)
        if task is not None:
            await task

    def _window_fields(self, feed: str, t_ms: float) -> dict:
        """The interval the move was measured over: from the reference price
        (the one standing SNIPER_WINDOW_MS before the tick, so >= that) to the
        tick that signalled, and that reference mid."""
        trig = self.trigger if feed == self.s.primary_feed else self.triggers.get(feed)
        ref = getattr(trig, "ref", None)
        if not ref or ref[0] > t_ms:
            return {}
        return {"window_ms": round(t_ms - ref[0], 1), "ref_mid": ref[1]}

    def _stamp_fill_confirmed(self, row: dict, plan: dict, seen_mono=None) -> None:
        """Signal -> the order's full quantity known, and send -> it, on the
        ticks' monotonic clock. seen_mono: when the private channel delivered
        the last of its trades (or the venue's final status); None: the reply
        itself told (a no-match). The bot needs the reply's order id as well,
        so a trade seen before the reply counts from the reply.
        t_signal_to_fill_known_ms: when this process actually booked it."""
        t0_ms = float(plan["t0"]) * 1000.0
        response = row.get("t_signal_to_response_ms")
        ms = (float(seen_mono) * 1000.0 - t0_ms) if seen_mono is not None else response
        if ms is None:
            return
        if response is not None:
            ms = max(ms, response)
        row["fill_confirmed_ms"] = round(ms, 2)
        if row.get("t_signal_to_send_ms") is not None:
            row["t_send_to_fill_confirmed_ms"] = round(ms - row["t_signal_to_send_ms"], 2)
        row["t_signal_to_fill_known_ms"] = round(time.monotonic() * 1000.0 - t0_ms, 2)

    def _fire_now(self, side: str, r_bp: float, t_ms: float, t_pc: int | None = None, feed: str | None = None):
        """Decide on the spot and hand the order to the sender at once.

        Runs synchronously inside the BTC tick handler: every check, the size,
        the cap and the latency plan are settled here and the order goes to
        the worker thread (sign -> send) before the event loop runs anything
        else. Spawning a task first cost the loop's scheduling delay (~4ms
        median) before any of this started. Returns the task that records the
        outcome, or None when the signal was skipped.
        """
        if self._closing:
            self._release(side)
            return None
        start_pc = time.perf_counter_ns()
        t_pc = start_pc if t_pc is None else t_pc
        feed = feed or self.s.primary_feed                              # None: the trigger feed
        trig_pc, mid = getattr(self, "_t1", None) or (None, None)      # T1 and the mid, from _signal
        row = {"wall": time.time(), "id": f"{time.time():.3f}-{side}", "side": side, "bp": round(r_bp, 3),
               "feed": feed,
               # BTC tick received -> decision started: our own event loop's delay.
               "loop_lag_ms": round(time.monotonic() * 1000.0 - t_ms, 1)}
        row.update(self._window_fields(feed, t_ms))
        if mid is not None:
            row["signal_mid"] = mid
        if feed == "binance" and self.fresh.last_excess is not None:
            row["binance_excess_ms"] = round(self.fresh.last_excess, 1)    # feed age past its usual (exchange stamps)
        elif feed == "coinbase" and self.cb_fresh.last_excess is not None:
            row["coinbase_excess_ms"] = round(self.cb_fresh.last_excess, 1)
        elif feed == "binance_fut" and self.bf_fresh.last_excess is not None:
            row["binance_fut_excess_ms"] = round(self.bf_fresh.last_excess, 1)
        try:
            plan = self._decide(side, row, t_ms)
            if plan is None:
                self._release(side)
                return None
            plan.update(t_pc=t_pc, trig_pc=trig_pc if trig_pc is not None else start_pc, t_ms=t_ms)
            if self.live is not None:
                return self._fire_live(side, r_bp, row, plan, t_pc, start_pc)
            # Sign here, in the tick handler, and only then hand the order to a
            # worker: waking a thread on the 1-vCPU VPS took ~2-4ms (it waits
            # for the GIL), and that wait now falls inside the in-flight time
            # instead of before it. A live sender signs and sends from the loop
            # the same way.
            st = {"tick_pc": t_pc, "decide_pc": start_pc, "decided_pc": plan["decided_pc"],
                  "built_pc": plan["built_pc"], "sign_pc": time.perf_counter_ns()}
            cpu0, gc0 = time.thread_time(), [g["collections"] for g in gc.get_stats()]
            if self._builder is not None:
                self._builder.last_sign_ns = None
            sign_ms, sign_err, signed = self._sign_now(plan)
            st["signed_pc"] = time.perf_counter_ns()
            if getattr(self._builder, "last_sign_ns", None):
                st["sign_start_pc"], st["sign_end_pc"] = self._builder.last_sign_ns
            # Signing's own CPU time and any garbage collections inside it:
            # wall time far above CPU time means it waited (GIL or OS).
            st["sign_cpu_ms"] = round((time.thread_time() - cpu0) * 1000.0, 3)
            gcd = [b["collections"] - a for a, b in zip(gc0, gc.get_stats())]
            if any(gcd):
                st["sign_gc"] = gcd
            if signed is not None and not sign_err:
                # The request exactly as post_order builds it (order JSON +
                # HMAC L2 headers): "sent" is when the bytes are ready, not
                # merely when the order is signed.
                req_err = self._prepare_request(signed)
                if req_err:
                    sign_err = f"order request build failed: {req_err}"
            st["sent_pc"] = time.perf_counter_ns()      # network-send boundary (simulated in paper)
            plan["sent_wall"], plan["sent_mono"] = time.time(), time.monotonic()
            loop = asyncio.get_running_loop()
            if sign_err:
                fut = loop.create_future()
                fut.set_result((False, sign_ms, sign_err, st))
            else:
                st["handoff_pc"] = time.perf_counter_ns()
                fut = loop.run_in_executor(None, self._match, plan, sign_ms, st)
                self._bankroll_spent(plan["amount"])
            if self.s.shadow_ms:
                # After the send: the shadow sleeps until absolute times anyway.
                self._spawn(self._shadow(row, plan["token"], plan["other"], plan["cond"],
                                         plan["amount"], plan["cap"], plan["t0"]))
            if not sign_err:
                self._start_trace(row, plan)
            self._guard_diag_after_send(side, row, plan)
            return self._spawn(self._finish(side, r_bp, row, plan, fut, t_pc, start_pc, st["sign_pc"]), order=True)
        except Exception as exc:  # noqa: BLE001 - a sniper fault must never stop the bot
            self.n["errors"] += 1
            row.update(result="error", reason=f"{type(exc).__name__}: {exc}")
            self._write(row)
            self._release(side)
            return None

    # ---------------------------------------------------------------- live canary
    def _fire_live(self, side, r_bp, row, plan, t_pc, start_pc):
        """LIVE CANARY: the decision above, then a real order. Sized exactly
        as paper sizes it, signed and written to the warm HTTP/2 connection
        here, in the tick handler - no event-loop turn before the bytes leave.
        The reply is awaited in the task spawned below. A refusal or
        a halted canary is journalled as a skip: nothing was sent."""
        from sniper_live import Halted, Refused
        feed = row.get("feed", self.s.primary_feed)
        gate = self._gate_for(feed)
        excess = gate.last_excess if gate is not None else None
        try:
            p = self.live.prepare(plan, decided_pc=plan["decided_pc"])
        except (Refused, Halted) as exc:
            self.n["orders"] -= 1                       # decided, but not sent
            row["detail"] = str(exc)[:200]
            self._skip("live halted" if isinstance(exc, Halted) else "live refused", row)
            self._release(side)
            return None
        previous_guard = getattr(p, "send_guard", None)
        def send_guard():
            if previous_guard is not None:
                previous_guard()
            self._check_dispatch_age(plan, gate, excess)
        p.send_guard = send_guard
        # The bytes go out here, still inside the BTC tick's own step: no
        # event-loop turn between the decision and the wire (None: no warm
        # HTTP/2 connection - the task below then connects and sends).
        started = self.live.start_send(p)
        plan["order_type"] = p.order_type
        self._start_trace(row, plan)
        task = self._spawn(self._finish_live(side, r_bp, row, plan, p, t_pc, started), order=True)
        if self.s.shadow_ms:
            self._spawn(self._shadow(row, plan["token"], plan["other"], plan["cond"],
                                     plan["amount"], plan["cap"], plan["t0"]))
        self._guard_diag_after_send(side, row, plan)
        return task

    def _check_dispatch_age(self, plan, gate, original_excess) -> None:
        """Recheck the original decision at the transport's write boundary.

        A cold reconnect or durable intent write can take seconds. New book
        updates and BTC ticks must not make that old signed decision fresh.
        Reuse the configured freshness limits; never reprice or resize it.
        """
        from sniper_live import Refused
        if self._closing:
            raise Refused("sniper is shutting down before dispatch")
        now = time.monotonic()
        updated = getattr(plan["view"], "updated_mono", None)
        if updated is not None:
            age = (now - float(updated)) * 1000.0
            if not math.isfinite(age) or age < 0 or age > self.s.stale_ms:
                raise Refused("decision book became stale before dispatch")
        if gate is not None and gate.enabled:
            why = gate.why_not(now)
            if why is not None:
                raise Refused(why + " before dispatch")
            elapsed = now * 1000.0 - float(plan["t_ms"])
            excess = float(original_excess or 0.0)
            if (not math.isfinite(elapsed) or elapsed < 0
                    or elapsed > gate.unknown_ms or excess + elapsed > gate.stale_ms):
                raise Refused("trigger signal became stale before dispatch")

    async def _finish_live(self, side, r_bp, row, plan, p, t_pc, started=None) -> None:
        """Send, then journal the venue's answer. Fills count towards the
        round caps at once (a pending order counts too: it may fill); the
        ledger books them only when the private channel confirms."""
        from sniper_live import MATCHED, NO_MATCH, PENDING
        end, rd, token = plan["end"], plan["rd"], plan["token"]
        try:
            res = await self.live.send(p, started)
            kind = res["kind"]
            if res.get("auto_reconcile"):
                row["live_send_auto_reconcile"] = True
            ts = res.get("ts_us") or {}                 # microseconds after the decision
            est = getattr(p, "estimate", None) or {}
            row.update(live=True, kind=kind, outcome=self.LIVE_OUTCOME.get(kind, kind),
                       order_id=res.get("order_id"), venue_status=res.get("venue_status"),
                       http_status=res.get("http_status"), error=res.get("error"),
                       new_connection=res.get("new_connection"), amount=round(p.amount, 4), cap=p.cap,
                       price_signed=p.price, order_type=p.order_type,
                       client_order_id=p.client_order_id, estimate=est or None,
                       # the market's taker-delay flag (itode) in the rules this order was signed under
                       taker_delay=(getattr(p, "rules", None) or {}).get("taker_delay"),
                       live_ts_us=ts, t_tick_to_decided_ms=round((plan["decided_pc"] - t_pc) / 1e6, 2))
            self._latency_live(row, plan, res)
            reply = res.get("response") if isinstance(res.get("response"), dict) else {}
            tids = reply.get("tradeIDs") or reply.get("trade_ids")
            if isinstance(tids, list) and tids:
                row["reply_trade_ids"] = [str(t) for t in tids]
            self._trace_response(
                plan, mono=self._pc_to_mono(plan["decided_pc"] + int(ts["response"]) * 1000)
                if ts.get("response") is not None else time.monotonic(),
                source="CLOB" if ts.get("response") is not None else "CLOB (reply handled)",
                order_type=p.order_type, order_id=res.get("order_id"), kind=kind,
                venue_status=res.get("venue_status"), http_status=res.get("http_status"), error=res.get("error"),
                reply={k: reply[k] for k in self.TRACE_REPLY_KEYS if k in reply} or None)
            if "net_send_start" in ts:
                row["t_decided_to_wire_ms"] = round(ts["net_send_start"] / 1000.0, 2)
                row["t_signal_to_send_ms"] = round(row["t_tick_to_decided_ms"] + ts["net_send_start"] / 1000.0, 2)
                if "response" in ts:
                    row["t_wire_to_reply_ms"] = round((ts["response"] - ts["net_send_start"]) / 1000.0, 2)
                    row["t_send_to_response_ms"] = row["t_wire_to_reply_ms"]
                    row["t_signal_to_response_ms"] = round(row["t_tick_to_decided_ms"] + ts["response"] / 1000.0, 2)
            if kind == NO_MATCH:
                self._stamp_fill_confirmed(row, plan)            # the reply says nothing filled
                self._latency_fill(row, plan, None, "ACK", filled=False)
            if p.order_type == "FAK" and row.get("live_send_auto_reconcile"):
                # Outcome unknown, being reconciled with the venue by the order's
                # hash (audit M7): held exactly like an accepted FAK whose size is
                # not known yet - its round slot reserved, this side waiting - so
                # recovery can never let the round caps be overrun.
                rd["fills"] += 1
                self.n["pending"] += 1
                row.update(result="pending", shares=None, price=None, fee=None, cost=None,
                           order_id=p.order_hash, reason="outcome unknown; reconciling with the venue")
                self._observe_live(row)
                self._fak_pending[p.order_hash] = (dict(row), plan)
                self._write(row)
                self._table_row(side, p.amount, "pending", end, order_id=p.order_hash)
                return
            if p.order_type == "FAK" and kind in (MATCHED, PENDING):
                rd["fills"] += 1           # reserve one slot, even before the quantity is known
                fill = res.get("fill")
                if fill is not None:
                    self._record_fak_fill(row, plan, fill)
                else:
                    self.n["pending"] += 1
                    row.update(result="pending", shares=None, price=None, fee=None, cost=None,
                               reason="accepted; waiting for complete private trade quantities")
                    self._observe_live(row)
                    self._fak_pending[res["order_id"]] = (dict(row), plan)
                    self._write(row)
                    self._table_row(side, p.amount, "pending", end, order_id=res["order_id"])
                    self.on_event("sniper", "LIVE FAK accepted; fill size pending", "info")
                self._spawn(self._markout_live(row, token, res.get("order_id")))
                return
            if kind in (MATCHED, PENDING):
                fill = res.get("fill") or {}
                shares = float(fill.get("shares") or est.get("shares") or p.amount / p.price)
                rd["fills"] += 1
                rd[side] += shares
            if kind == MATCHED:
                self.n["filled"] += 1
                fill = res.get("fill")
                price = float(fill["price"]) if fill else est.get("price")
                row.update(result="filled", shares=round(shares, 4), price=price, fee=est.get("fee"),
                           cost=round(p.amount, 4),
                           price_source=(fill or {}).get("source") or "decision-book estimate")
                if fill:
                    self._latency_fill(row, plan, fill.get("seen_mono"),
                                       "ACK" if fill.get("source") == "order reply" else "PRIVATE_WS")
                self._observe_live(row)
                self._write(row)
                self._table_row(side, p.amount, "filled", end)
                self.on_event("sniper", f"LIVE FILL {side} {shares:.1f} @ {float(price or 0):.3f} "
                                        f"(BTC {r_bp:+.2f}bp)", "good")
                self._spawn(self._markout_live(row, token, res.get("order_id")))
            elif kind == PENDING:
                self.n["pending"] += 1
                row.update(result="pending", reason=res.get("error"), fee=est.get("fee"),
                           shares=round(shares, 4), price=est.get("price"))
                self._observe_live(row)
                self._write(row)
                self._table_row(side, p.amount, "pending", end)
                self.on_event("sniper", f"LIVE order accepted, outcome pending ({res.get('venue_status')})", "info")
                self._spawn(self._markout_live(row, token, res.get("order_id")))
            else:
                self.n["refused"] += 1
                row.update(result="refused", reason=res.get("error") or kind)
                if kind == NO_MATCH:
                    self._settle_carry(plan, side, 0.0)       # an IOC that matched nothing: all of it carried
                    if plan.get("target") is not None:
                        row["carry_out"] = self.carry[side]
                self._observe_live(row)
                self._write(row)
                self._table_row(side, p.amount, "missed" if kind == NO_MATCH else "rejected", end)
                if kind != NO_MATCH:
                    self.on_event("sniper", f"LIVE order {kind}: {str(res.get('error'))[:120]}", "warn")
        except Exception as exc:  # noqa: BLE001 - a sniper fault must never stop the bot
            self.n["errors"] += 1
            row.update(result="error", reason=f"{type(exc).__name__}: {exc}")
            self._observe_live(row)
            self._write(row)
        finally:
            self._trace_response(plan, mono=None, source="no reply")
            self._release(side)

    def _observe_live(self, row: dict) -> None:
        """A live order's first outcome: its latency and its quality counts."""
        self.quality.observe(row)
        if row.get("lat"):
            self.latency.add(row["lat"])

    def _note_over_target(self, row: dict, plan: dict, shares) -> None:
        """F1 (audit): a dollar-sized IOC whose ask fell before the match buys
        more shares than its target. Journalled and counted - never hidden.
        Diagnostics only, after the fill: no decision reads it."""
        target = plan.get("target")
        try:
            got = float(shares or 0)
        except (TypeError, ValueError):
            return
        if target is not None and got > float(target) + 1e-6:
            row["over_target"] = round(got - float(target), 4)
            self.n["fills above target (ask fell)"] += 1

    def _record_fak_fill(self, row, plan, fill, *, update=False):
        """Report actual executed quantity, never the request or book estimate."""
        side, rd = row["side"], plan["rd"]
        shares = float(fill["shares"])
        row.update(shares=shares, price=fill.get("price"), cost=fill["cost"], fee=fill.get("fee", 0),
                   partial=fill.get("partial", False), price_source=fill["source"],
                   fill_statuses=fill.get("statuses", []), reason=None)
        if fill.get("trade_ids"):
            row["trade_ids"] = list(fill["trade_ids"])
        self._settle_carry(plan, side, shares)
        if plan.get("target") is not None:
            row["carry_out"] = self.carry[side]
        self._stamp_fill_confirmed(row, plan, fill.get("seen_mono"))
        lat = self._latency_fill(row, plan, fill.get("seen_mono"),
                                 "ORDER_STATUS" if fill.get("source") == "terminal order status" else "PRIVATE_WS",
                                 filled=shares > 0)
        if shares > 0:
            rd[side] += shares
            self._note_fill(plan["end"], side, shares, float(fill["cost"]) + float(fill.get("fee", 0) or 0))
            self._note_over_target(row, plan, shares)
            self.n["filled"] += 1
            row.update(result="filled", outcome="FILLED")
            self.on_event("sniper", f"LIVE FAK {'PARTIAL ' if row['partial'] else ''}FILL {side} "
                                   f"{shares:.4f} @ {fill['price']:.3f}", "good")
        else:
            rd["fills"] -= 1               # terminal, proven-zero delayed order
            self.n["refused"] += 1
            row.update(result="refused", outcome="LIVE_NO_MATCH", reason="FAK canceled with zero matched size")
        if update:
            # Keep the initial acknowledgement distinct from the quantity update.
            row.update(result="live_fill_update", wall=time.time(), resolved_result="filled" if shares else "refused")
            self.quality.resolve(row)
            self.latency.add({k: lat[k] for k in ("trigger_to_fill", "ack_to_fill") if k in lat})
        else:
            self._observe_live(row)
        self._write(row)
        self._table_row(side, fill["cost"] + fill.get("fee", 0), "filled" if shares else "missed", plan["end"],
                        order_id=row.get("order_id"))

    def _sync_live_fak(self):
        """Called on the event loop, including after background reconciliation."""
        if self.live is None:
            return
        for oid, (row, plan) in list(self._fak_pending.items()):
            fill = self.live.fill_for(oid)
            if fill is not None:
                self._fak_pending.pop(oid)
                self._record_fak_fill(row, plan, fill, update=True)

    async def _markout_live(self, row: dict, token: str, order_id) -> None:
        """Bought-side mid 1s and 5s after the venue's reply, minus the price
        paid and the fee (c/share). The price is the private channel's own
        report when it has arrived by then, else the decision-book estimate
        (flagged in price_source)."""
        try:
            t0 = time.monotonic()
            mids = {}
            for k, h in zip(("1s", "5s"), self.MARKOUT_S):
                await asyncio.sleep(max(0.0, t0 + h - time.monotonic()))
                mids[k] = getattr(self.hub.book.view(token), "mid", None)
            fill = self.live.fill_for(order_id)
            self._sync_live_fak()
            self._write({"wall": time.time(), "result": "live_fill", "order_id": order_id, "fill": fill})
            if fill is None and row.get("result") != "filled":
                return                                      # pending, and nothing filled (yet)
            if fill is not None and row.get("result") == "pending":
                self.n["pending filled"] += 1
            px = float(fill["price"]) if fill else float(row.get("price") or 0)
            fee_row = fill if row.get("order_type") == "FAK" and fill else row
            fee_ps = float(fee_row.get("fee") or 0) / max(1e-9, float(fee_row.get("shares") or 0) or 1.0)
            mine = {k: (float(m) - px - fee_ps) * 100 for k, m in mids.items() if m is not None and px > 0}
            for k, v in mine.items():
                self.marks[k].append(v)
            self._write({"wall": time.time(), "result": "markout", "order_id": order_id,
                         "markout_1s_c": mine.get("1s"), "markout_5s_c": mine.get("5s"),
                         "price_source": (fill or {}).get("source") or row.get("price_source")})
        except Exception:  # noqa: BLE001 - diagnostics only
            pass

    def _fair_anchor(self, token: str, view, row: dict, t_ms: float):
        """(market mid, which) the fair value is anchored to: the side's book as
        it stood when the move BEGAN (the trigger's reference, window_ms before
        the tick), never higher than its mid now. The book on our side reacts
        to BTC within the same 250ms, and the model adds the whole move: a mid
        read after it had already moved part of the way counted that part
        twice and bid above fair. Missing pre-move history cannot prove an edge."""
        now = getattr(view, "mid", None)
        book = getattr(self.hub, "book", None)
        win = row.get("window_ms")
        if now is None:
            return None, "no market price"
        if win is None or not hasattr(book, "view_at_mono"):
            return None, "history unavailable"
        try:
            then = getattr(book.view_at_mono(token, (float(t_ms) - float(win)) / 1000.0), "mid", None)
        except Exception:  # noqa: BLE001 - absent/unusable history fails closed
            then = None
        if then is None:
            return None, "history unavailable"
        row["market_mid_now"] = round(float(now), 4)
        return (then, "before move") if then <= now else (now, "now")

    def _decide(self, side: str, row: dict, t_ms: float):
        """Every check and every field of the order; None (skip journalled) if it must not trade."""
        self._sync_live_fak()
        pending_guard = getattr(self.live, "blocks_pending_fak", None)
        for oid, (pending_row, _plan) in list(self._fak_pending.items()):
            if pending_row.get("side") != side:
                continue
            if pending_guard is None or pending_guard(oid, client_order_id=pending_row.get("client_order_id")):
                return self._skip("awaiting FAK fill size", row)
        hub = self.hub
        up, down, cond, end = hub.up_token, hub.down_token, hub.condition_id, hub.window_end
        if not (up and down and cond and end):
            return self._skip("no current round", row)
        left = float(end) - time.time()
        row.update(round_end=end, left_s=round(left, 1), condition_id=str(cond))
        first, last = self._window()
        if left > first:
            return self._skip("before trade window", row)
        if left < last:
            return self._skip("too late in round", row)
        token = up if side == "UP" else down
        view = hub.book.view(str(token))
        status = str(getattr(view, "status", ""))
        if status != "LIVE":
            row["book_status"] = status                 # DISCONNECTED / UNSYNCED / STALE: which one
            return self._skip("book not live", row)
        if not view.asks:
            # A live book with nothing offered on this side (a market that is
            # all but decided) - not a feed problem.
            return self._skip("no asks on this side", row)
        upd = getattr(view, "updated_mono", None)
        if upd is not None and (time.monotonic() - float(upd)) * 1000.0 > self.s.stale_ms:
            return self._skip("book stale", row)
        ask = self._visible_ask(str(token), view)
        if ask is None:
            return self._skip("no asks left after our own recent fills", row)
        # The broker refuses asks outside the account band (MIN_BUY_PRICE /
        # MAX_BUY_PRICE). Sending one anyway counted as a refused ORDER, which
        # put REJECT rows in the table and dragged the fill rate down.
        lo = float(getattr(self.broker, "min_buy_price", 0.0) or 0.0)
        hi = float(getattr(self.broker, "max_buy_price", 1.0) or 1.0)
        if not lo <= ask <= hi:
            row.update(ask_seen=ask)
            return self._skip("ask outside price band", row)
        # The price ceiling is fixed here, once, from the ask this decision saw:
        # never recomputed from a later ask, so it cannot chase the book.
        cap = min(0.99, round(ask + self.s.slip, 4))
        fair = None
        fee_rate = fee_exponent = None
        if self.s.fair:
            # Buy only at or under what the side is worth right after this move,
            # anchored to the market's own price for it (anchored_fair), less the fee
            # and the margin (SNIPER_FAIR): first buys and add-ons alike.
            sigma = self.vol_model.vol()
            mkt, anchor = self._fair_anchor(str(token), view, row, t_ms)
            tau = float(end) - time.time()
            move = abs(float(row.get("bp") or 0.0))
            why = ("volatility warming up" if sigma is None else
                   "no pre-move market history" if mkt is None and anchor == "history unavailable" else
                   "no market price" if mkt is None
                   else "no move" if move <= 0 else "round over" if tau <= 0 else None)
            if why is not None:
                return self._skip(f"fair value unknown: {why}", row)
            fees = self._fair_fee_parameters(cond, up, down)
            if fees is None:
                return self._skip("fair value unknown: market fee rules not loaded", row)
            fee_rate, fee_exponent = fees
            fair = anchored_fair(float(mkt), move, sigma, tau)
            tick = getattr(view, "tick_size", None)
            limit = fair_limit(fair, self.s.fair_margin, float(tick) if tick else 0.01, fee_rate, fee_exponent)
            row.update(market_mid=round(float(mkt), 4), fair_anchor=anchor, fair=round(fair, 4), fair_limit=limit,
                       vol_bp=round(sigma * 1e4, 3), tau_s=round(tau, 1),
                       fee_rate=fee_rate, fee_exponent=fee_exponent)
            self.last_fair = {"side": side, "market_mid": round(float(mkt), 4), "anchor": anchor, "fair": round(fair, 4),
                              "limit": limit, "ask": ask, "wall": time.time()}
            if ask > limit + 1e-9:
                row.update(ask_seen=ask)
                return self._skip("ask not below fair value", row)
            cap = min(cap, limit)
        if lo > 0 and cap <= lo + 1e-9:
            # An ask at MIN_BUY_PRICE whose ceiling is that same price: the paper
            # broker refuses a cap at or below the floor ("order price cap ... is at
            # or below the floor") while live would buy there. Neither should send it.
            row.update(ask_seen=ask, cap=cap)
            return self._skip("price cap at the price floor", row)
        target = carry = None
        kelly = False
        if self.s.order_type == "FAK":
            if fair is not None and self.s.kelly:
                # Incremental portfolio Kelly: cash and current outcome payouts
                # determine how much to add. Existing inventory is not a fresh bet.
                # Whole shares from the venue minimum to the available room; no carry.
                bank = self._bankroll_now()
                if bank is None:
                    return self._skip("kelly size unknown: no cash reading", row)
                # No per-order cap (the operator's choice): only what is left of this
                # side's per-market cap (SNIPER_MAX_SIDE_SHARES), and live, the canary's
                # per-order cap - never a bigger order than the account's limits allow.
                room = self.s.max_side_shares - self._side_held(end, side, token)
                live_cap = getattr(getattr(getattr(self.live, "sender", None), "limits", None), "max_order_shares", None)
                if live_cap is not None:
                    room = min(room, float(live_cap))
                held, opposite = self._kelly_inventory(cond, end, side, token, down if side == "UP" else up)
                shares, f = kelly_shares(fair, ask, fee_rate, self.s.kelly_fraction, bank,
                                         max(room, 0.0), held_shares=held, opposite_shares=opposite,
                                         fee_exponent=fee_exponent)
                row.update(kelly_f=round(f, 4), bankroll=round(bank, 2), kelly_shares=shares,
                           kelly_held_shares=round(held, 4), kelly_opposite_shares=round(opposite, 4))
                self.last_fair["shares"] = shares
                if shares < KELLY_MIN_SHARES:
                    row.update(ask_seen=ask)
                    return self._skip(f"kelly size under the {KELLY_MIN_SHARES}-share minimum", row)
                target, carry, kelly = shares, 0.0, True
            else:
                # IOC: a share target - the base clip plus what this side's last
                # order in this market did not fill - sized on the visible levels
                # up to the ceiling (and never past the account's MAX_BUY_PRICE).
                carry = self._carry_for(cond, end)[side]
                target = min(self.s.shares + carry, self.s.max_target_shares)
            amount = max(self.ioc_spend(self._visible_levels(str(token), view), target, ask, min(cap, hi)),
                         MIN_ORDER_USD)
            if kelly:
                # The round gate deliberately estimates shares at the best ask.
                # Paying deeper levels (or rounding the stake up a cent) can
                # exceed that estimate even after Kelly trimmed its target to
                # the available room. Submit a smaller, conservatively bounded
                # stake instead of rejecting the whole otherwise eligible order.
                max_spend = (Decimal(str(max(room, 0.0))) * Decimal(str(ask))).quantize(
                    Decimal("0.01"), rounding=ROUND_FLOOR)
                amount = min(amount, float(max_spend))
                # Both senders size to at least five shares at the signed limit.
                # A stake below that floor would be enlarged after this gate,
                # defeating the room bound. Keep an unfillable remainder closed.
                tick = Decimal(str(getattr(view, "tick_size", None) or 0.01))
                limit = (Decimal(str(min(cap, hi))) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
                minimum = (Decimal(str(KELLY_MIN_SHARES)) * limit).quantize(
                    Decimal("0.01"), rounding="ROUND_CEILING")
                if amount < max(MIN_ORDER_USD, float(minimum)):
                    row.update(ask_seen=ask, amount=round(amount, 4), room_shares=round(room, 4))
                    return self._skip("kelly room under venue minimum at price cap", row)
        else:
            # The venue refuses a marketable BUY under $1, so under 10c the
            # 10-share clip becomes $1 - the smallest order live could place.
            amount = max(self.s.shares * ask, MIN_ORDER_USD)
        # The gates below judge the book cost (`gate_amount`) whatever the sizing:
        # the same signals qualify. Share sizing (the default) then spends the
        # whole shares x the cap's cent limit - lifted to the venue's 5 shares
        # and its $1 at the limit, as the book-cost order is.
        gate_amount, order_shares = amount, None
        if self.s.order_type == "FAK" and self.s.order_sizing == "shares":
            tick = Decimal(str(getattr(view, "tick_size", None) or 0.01))
            limit = (Decimal(str(min(cap, hi))) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
            floor_shares = max(KELLY_MIN_SHARES, int((Decimal(str(MIN_ORDER_USD)) / limit).to_integral_value(
                rounding="ROUND_CEILING")))
            order_shares = max(int(math.floor(target + 1e-9)), floor_shares)
            if kelly and order_shares > room + 1e-9:
                row.update(ask_seen=ask, order_shares=order_shares, room_shares=round(room, 4))
                return self._skip("kelly room under venue minimum at price cap", row)
            amount = max(float((Decimal(order_shares) * limit).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)),
                         gate_amount)
        rd = self.rounds.get(end)
        if rd is None:
            # A new round: rounds that ended over an hour ago go (one entry per
            # market otherwise accumulates for the life of the process). An order
            # still settling holds its own round dict in its plan.
            for old in [e for e in self.rounds if e < float(end) - 3600]:
                del self.rounds[old]
            rd = self.rounds[end]
            for s_, tok in (("UP", up), ("DOWN", down)):
                shares, fills = self.round_baseline.get(str(tok), (0.0, 0))
                rd[s_] += shares
                rd["fills"] += fills
        # The other side may have an order in flight in this round: it holds a
        # fill slot until its outcome is known, so two sides together never
        # pass the round's fill cap.
        other = self.inflight.get("DOWN" if side == "UP" else "UP")
        in_flight = 1 if isinstance(other, dict) and other.get("end") == end else 0
        if order_shares is not None:
            # A share-sized order expects amount / ask shares at the ask: never more
            # than the room - Kelly's (the side's cap and the canary's per-order cap
            # in it), else the side's room this round and the canary's cap. Near a
            # cap it spends less (never under the book cost the gates judged); more
            # shares then come only from a fall below the ask, as with the book cost.
            if kelly:
                bound = room
            else:
                bound = self.s.max_side_shares - rd[side]
                live_cap = getattr(getattr(getattr(self.live, "sender", None), "limits", None),
                                   "max_order_shares", None)
                if live_cap is not None:
                    bound = min(bound, float(live_cap))
            most = float((Decimal(str(max(bound, 0.0))) * Decimal(str(ask))).quantize(
                Decimal("0.01"), rounding=ROUND_FLOOR))
            if amount > most:
                amount = max(gate_amount, most)
        if (rd["fills"] + in_flight >= self.s.max_fills_round
                or rd[side] + gate_amount / ask > self.s.max_side_shares):
            return self._skip("round cap", row)
        decided_pc = time.perf_counter_ns()     # every check passed: this signal trades
        self.inflight[side] = {"end": end, "amount": amount, "risk_shares": amount / ask}
        row.update(ask_seen=ask, cap=cap, shares=round(amount / ask, 4), amount=round(amount, 4))
        if order_shares is not None:
            row.update(order_shares=order_shares, book_cost=round(gate_amount, 4))
        if target is not None:
            row.update(target_shares=round(target, 4), carry_in=round(carry, 4))
        self.n["orders"] += 1
        lat, parts = self.latency_plan()
        row.update(latency_ms=round(lat, 1), **parts)
        tick = getattr(view, "tick_size", None)
        return {"end": end, "token": str(token), "amount": amount, "rd": rd, "cap": cap, "lat": lat, "view": view,
                "risk_shares": amount / ask, "fair_fee_rate": fee_rate, "fair_fee_exponent": fee_exponent,
                # a Kelly-sized order carries nothing to the next: each is sized fresh
                "target": target, "carry_market": self.carry_market if target is not None and not kelly else None,
                "order_type": self.s.order_type, "tick": f"{float(tick):g}" if tick else "0.01",
                "other": str(down if side == "UP" else up), "cond": str(cond), "t0": t_ms / 1000.0,
                "decided_pc": decided_pc, "built_pc": time.perf_counter_ns(),
                "send": (side, amount, up, down, cond, end, cap, str(token),
                         f"{float(tick):g}" if tick else "0.01", lat)}

    async def _finish(self, side, r_bp, row, plan, fut, t_pc, start_pc, call_pc) -> None:
        """The order's outcome: journal, dashboard row, markout. Off the critical path."""
        end, rd, amount, token = plan["end"], plan["rd"], plan["amount"], plan["token"]
        try:
            ok, sign_ms, sign_err, st = await fut
            if sign_ms is not None:
                row["sign_ms"] = round(sign_ms, 2)
            row.update(self._stages(t_pc, start_pc, call_pc, st))
            if st.get("sent_pc") and t_pc is not None:
                row["t_signal_to_send_ms"] = round((st["sent_pc"] - t_pc) / 1e6, 2)
                if st.get("matched_pc"):
                    # paper knows the quantity at its modelled match (no private channel)
                    row["fill_confirmed_ms"] = round((st["matched_pc"] - t_pc) / 1e6, 2)
                    row["t_send_to_fill_confirmed_ms"] = round((st["matched_pc"] - st["sent_pc"]) / 1e6, 2)
            result = plan.get("result") or {"fill": getattr(self.broker, "last_fill", None),
                                            "error": getattr(self.broker, "last_error", None)}
            fill = result["fill"] or {}
            filled = bool(ok and fill.get("tag") == "sniper")
            self._latency_paper(row, plan, st, filled=filled and float(fill.get("shares") or 0) > 0)
            self._trace_response(plan, mono=self._pc_to_mono(st.get("matched_pc")), source="SIMULATED",
                                 order_type=plan.get("order_type"), order_id=fill.get("order_id") if filled else None,
                                 kind="FILLED" if filled else "NO_FILL",
                                 error=None if filled else str(sign_err or result["error"] or "")[:200] or None)
            row["outcome"] = self._outcome(filled, None if filled else (sign_err or result["error"]),
                                           row, history=self._history_book() is not None)
            arrival = row.get("arrival")
            if arrival:
                late = None
                ts = row.get("ts_us") or {}
                if "sent" in ts and "matched" in ts and row.get("latency_ms") is not None:
                    late = (ts["matched"] - ts["sent"]) / 1000.0 - float(row["latency_ms"])
                woke = (arrival.get("wake") or {}).get("fill")
                if late is not None and late >= 10.0:
                    row["late_recovered"] = True       # the timer woke >=10ms late; the book did not move with it
                if arrival.get("evidence") == "quiet":
                    row["book_evidence"] = "quiet"         # P2 opt-in: inferred, not proven
                    self.n["paper orders on an inferred quiet-feed book"] += 1
                if woke is not None and row["outcome"] != "UNKNOWN_BOOK_STATE" and woke != filled:
                    row["old_rule"] = "false_miss" if filled else "false_fill"
                row["arrival_ms"] = arrival.get("arrival_ms")
            if filled:
                self.n["filled"] += 1
                rd["fills"] += 1
                rd[side] += float(fill.get("shares") or 0)
                self._note_over_target(row, plan, fill.get("shares"))
                self._settle_carry(plan, side, fill.get("shares"))
                if plan.get("target") is not None:
                    row["carry_out"] = self.carry[side]
                row.update(result="filled", order_id=fill.get("order_id"), shares=fill.get("shares"),
                           price=fill.get("average_price"), fee=fill.get("fee"), cost=fill.get("total_cost"))
                self._note_fill(end, side, fill.get("shares"), fill.get("total_cost"))
                if fill.get("partial"):
                    row.update(partial=True, stake=fill.get("stake"))
                if fill.get("matched_wall"):
                    # BTC tick -> the order met the book, all of our own delays included.
                    row["match_ms"] = round((float(fill["matched_wall"]) - row["wall"]) * 1000.0
                                            + row["loop_lag_ms"], 1)
                self.quality.observe(row)
                self._write(row)
                self._table_row(side, fill.get("total_cost"), "filled", end)
                self.on_event("sniper", f"FILL {side} {float(fill.get('shares') or 0):.1f} @ "
                                        f"{float(fill.get('average_price') or 0):.3f} (BTC {r_bp:+.2f}bp)", "good")
                self._spawn(self._markout(row, token))
            else:
                self.n["refused"] += 1
                reason = sign_err or result["error"]
                row.update(result="refused", reason=reason)
                if "fak no-fill" in str(reason).lower():
                    self._settle_carry(plan, side, 0.0)
                    if plan.get("target") is not None:
                        row["carry_out"] = self.carry[side]
                self.quality.observe(row)
                self._write(row)
                # "FOK no-fill": the price ran past the cap while the order was
                # in flight - a miss, not a rejection. Never retried: chasing a
                # price that already moved is exactly the fill that loses.
                self._table_row(side, amount, "missed" if ("fok no-fill" in str(reason).lower()
                                                            or "fak no-fill" in str(reason).lower())
                                else "unknown" if row.get("outcome") == "UNKNOWN_BOOK_STATE" else "rejected", end)
        except Exception as exc:  # noqa: BLE001 - a sniper fault must never stop the bot
            self.n["errors"] += 1
            row.update(result="error", reason=f"{type(exc).__name__}: {exc}")
            self.quality.observe(row)
            self._write(row)
        finally:
            self._trace_response(plan, mono=None, source="no reply")
            self._release(side)

    # ---------------------------------------------------------------- T0..T7 (after the send)
    SIMULATED = ("send_to_ack", "trigger_to_ack", "ack_to_fill", "trigger_to_fill")

    def _latency_paper(self, row: dict, plan: dict, st: dict, filled: bool = True) -> None:
        """Paper: T0..T5 are measured here; the venue's ACK and the fill are
        the modelled match (row lat_*_source SIMULATED). A zero-fill order
        has an ACK but no fill time."""
        if st.get("sent_pc") is None:
            return
        lat = exec_latency.stages(t0=plan.get("t_pc"), t1=plan.get("trig_pc"), t2=plan.get("decided_pc"),
                                  t3=st.get("sign_start_pc"), t4=st.get("sign_end_pc") or st.get("signed_pc"),
                                  t5=st.get("sent_pc"), t6=st.get("matched_pc"))
        if st.get("matched_pc") is not None:
            lat = exec_latency.with_fill(lat, t_ms=plan["t_ms"], t0=plan.get("t_pc"), t1=plan.get("trig_pc"),
                                         seen_mono=None, filled=filled)
            if filled:
                row["lat_fill_source"] = "SIMULATED"
        row.update(lat=lat, lat_ack_source="SIMULATED")
        self.latency.add(lat, simulated=self.SIMULATED)

    def _latency_live(self, row: dict, plan: dict, res: dict) -> None:
        """Live: T0..T6 from this process and the transport's own stamps."""
        decided = plan.get("decided_pc")
        if decided is None:
            return
        lat = exec_latency.stages(t0=plan.get("t_pc"), t1=plan.get("trig_pc"), t2=decided,
                                  **exec_latency.live_stamps(decided, res.get("ts_us") or {}))
        row["lat"] = lat
        if lat.get("trigger_to_ack") is not None:
            row["lat_ack_source"] = "CLOB"

    def _latency_fill(self, row: dict, plan: dict, seen_mono, source: str, filled: bool = True) -> dict:
        """T7: the first authoritative observation of the fill quantity (none
        for a zero fill: no shares, no fill time)."""
        lat = exec_latency.with_fill(dict(row.get("lat") or {}), t_ms=plan["t_ms"] if "t_ms" in plan else plan["t0"] * 1000.0,
                                     t0=plan.get("t_pc"), t1=plan.get("trig_pc"), seen_mono=seen_mono, filled=filled)
        row["lat"] = lat
        if lat.get("trigger_to_fill") is not None:
            row["lat_fill_source"] = source
        return lat

    async def _markout(self, row: dict, token: str) -> None:
        """Bought-side mid 1s and 5s after the fill, minus price and fee (c/share)."""
        try:
            px = float(row.get("price") or 0)
            fee_ps = float(row.get("fee") or 0) / max(1e-9, float(row.get("shares") or 0))
            waited, mine = 0.0, {}
            arrival = row.get("arrival") or {}
            for k, h in zip(("1s", "5s"), self.MARKOUT_S):
                if arrival.get("arrival_ms") is not None and arrival.get("arrival_mono") is not None:
                    # h after the modelled match, on the book as it stood then
                    view = await self._book_at(token, arrival["arrival_ms"] + h * 1000.0,
                                               arrival["arrival_mono"] + h)
                else:
                    await asyncio.sleep(h - waited)
                    view = self.hub.book.view(token)
                waited = h
                mid = getattr(view, "mid", None)
                if mid is not None:
                    mine[k] = (float(mid) - px - fee_ps) * 100
                    self.marks[k].append(mine[k])
            # This fill's own numbers. Reading the shared lists' last entry
            # reported another fill's markout whenever two fills overlapped
            # within 5s, or the previous fill's when this mid was missing.
            self._write({"wall": time.time(), "result": "markout", "order_id": row.get("order_id"),
                         "markout_1s_c": mine.get("1s"), "markout_5s_c": mine.get("5s")})
        except Exception:  # noqa: BLE001 - diagnostics only
            pass

    # ---------------------------------------------------------------- shadow fills
    # ------------------------------------------------------------ guard diagnostics
    def _note_fill(self, end, side: str, shares, cost) -> None:
        """A confirmed fill (paper: the broker's; live: the private channel's
        complete quantity) into this round's per-side tally, for guard_diag."""
        try:
            sh, c = float(shares or 0.0), float(cost or 0.0)
        except (TypeError, ValueError):
            return
        if sh <= 0:
            return
        cur = self._round_fills.setdefault((end, side), {"shares": 0.0, "cost": 0.0, "last_wall": None})
        cur["shares"] += sh
        cur["cost"] += c
        cur["last_wall"] = time.time()

    def _guard_diag_after_send(self, side: str, row: dict, plan: dict) -> None:
        """Snapshot what an opposite-side guard would see for this SENT order and
        journal it off the order path (a guard_diag row, joined by id).
        Diagnostics only: nothing here can refuse, delay or change an order."""
        if not self.s.guard_diag:
            return
        try:
            end = plan.get("end")
            opp = "DOWN" if side == "UP" else "UP"
            ask = float(row.get("ask_seen") or 0.0)
            shares = plan.get("shares")
            shares = float(shares) if shares else (float(plan.get("amount") or 0.0) / ask if ask > 0 else 0.0)
            vol = row.get("vol_bp")
            fee_rate = row.get("fee_rate") if row.get("fee_rate") is not None else plan.get("fair_fee_rate")
            fee_exp = row.get("fee_exponent") or plan.get("fair_fee_exponent")
            if fee_rate is None:
                try:                     # the market's cached rules (no I/O); fair value off leaves them unread
                    fees = self._fair_fee_parameters(plan.get("cond"), getattr(self.hub, "up_token", None),
                                                     getattr(self.hub, "down_token", None))
                except Exception:  # noqa: BLE001
                    fees = None
                if fees is not None:
                    fee_rate, fee_exp = fees
            p_sh = p_cost = 0.0
            inf = self.inflight.get(opp)
            if isinstance(inf, dict) and inf.get("end") == end and inf.get("risk_shares"):
                p_sh, p_cost = float(inf["risk_shares"]), float(inf.get("amount") or 0.0)
            f_sh = f_cost = 0.0
            for prow, pplan in list(self._fak_pending.values()):
                if pplan.get("end") == end and prow.get("side") == opp:
                    f_sh += float(pplan.get("risk_shares") or 0.0)
                    f_cost += float(pplan.get("amount") or 0.0)
            if f_sh > p_sh:                      # never both counted: the larger of the two views
                p_sh, p_cost = f_sh, f_cost
            snap = {"side": side, "ask": ask, "shares": shares, "now_wall": float(row.get("wall") or time.time()),
                    "opp_pending": {"shares": p_sh, "cost": p_cost} if p_sh > 0 else None,
                    "fee_rate": float(fee_rate or 0.0), "fee_exponent": int(fee_exp or 1),
                    "mine": dict(self._round_fills[(end, side)]) if (end, side) in self._round_fills else None,
                    "opp": dict(self._round_fills[(end, opp)]) if (end, opp) in self._round_fills else None,
                    "prev_opp": (dict(self._round_decisions[(end, opp)])
                                 if (end, opp) in self._round_decisions else None),
                    "market_mid": row.get("market_mid"), "sigma": float(vol) / 1e4 if vol else None,
                    "tau": row.get("tau_s"), "bp": float(row.get("bp") or 0.0),
                    "tick": float(plan.get("tick") or 0.01), "margin": float(self.s.fair_margin)}
            self._round_decisions[(end, side)] = {"wall": snap["now_wall"], "bp": row.get("bp"),
                                                  "market_mid": row.get("market_mid"), "feed": row.get("feed")}
            if len(self._round_decisions) + len(self._round_fills) > 32 and end is not None:
                for store in (self._round_decisions, self._round_fills):
                    for key in [k for k in store if k[0] is not None and float(k[0]) < float(end) - 900]:
                        store.pop(key, None)
            self._spawn(self._guard_diag(row.get("id"), row.get("condition_id"), end, snap, fee_rate is not None))
        except Exception:  # noqa: BLE001 - diagnostics only, never the order path
            self.n["guard diag errors"] += 1

    async def _guard_diag(self, oid, cond, end, snap: dict, fee_known: bool = True) -> None:
        try:
            out = whipsaw_diag(**snap)
            self._write({"wall": time.time(), "result": "guard_diag", "id": oid, "side": snap["side"],
                         "round_end": end, "condition_id": cond, "ask_seen": snap["ask"],
                         "order_shares": round(snap["shares"], 4), "fee_known": fee_known, **out})
            for name, hit in out["guards"].items():
                if hit:
                    self.n[f"guard diag would reject: {name}"] += 1
        except Exception:  # noqa: BLE001 - diagnostics only
            self.n["guard diag errors"] += 1

    async def _shadow(self, ref: dict, token: str, other: str, cond: str,
                      spend: float, cap: float, t0: float) -> None:
        """Price the same order at other latencies - arithmetic only.

        t0 is the monotonic time the BTC tick arrived (the decision). The book
        views are captured at t0 + L first and priced afterwards, so a slow
        market-rules lookup can never shift when the book was read.
        """
        try:
            views, late, mids = {}, {}, {}
            # The tick on the venue's clock: our wall time at the tick (the
            # decision row's wall, less the loop's delay before on_tick ran).
            tick_ms = (float(ref["wall"]) - float(ref.get("loop_lag_ms") or 0) / 1000.0) * 1000.0 \
                + self.s.venue_offset_ms
            hist = self._history_book() is not None
            for L in self.s.shadow_ms:
                await asyncio.sleep(max(0.0, t0 + L / 1000.0 - time.monotonic()))
                late[L] = round((time.monotonic() - t0) * 1000.0 - L, 1)
                if not hist:
                    views[L] = self.hub.book.view(token)          # the old rule: the book at wake-up
            for L in self.s.shadow_ms if hist else ():
                # every instant has passed: read each from the history (the
                # feed wait is paid once), so a late timer changes nothing
                views[L] = await self._book_at(token, tick_ms + L, t0 + L / 1000.0)
            for L in self.s.shadow_ms:
                at = L / 1000.0 + self.SHADOW_MARK_S
                mids[L] = getattr(await self._book_at(token, tick_ms + at * 1000.0, t0 + at), "mid", None)
            rules = await asyncio.to_thread(self.broker._rules, cond)
            ask = float(ref.get("ask_seen") or 0)
            alt = {f"{x * 100:g}": min(0.99, round(ask + x, 4)) for x in self.s.shadow_slips
                   if ask and ask + x > cap + 1e-9}
            tight = None
            if self.s.shadow_tight and ask and cap > 0:
                tc = round(ask + self.s.shadow_tight, 4)
                if tc < cap - 1e-9:
                    # the same whole shares at the tighter limit (amount = shares x limit)
                    tight = (tc, spend * tc / cap)
            fv = None
            if ref.get("fair_limit") is not None and cap > 0:
                # The fair-value cap alone (policies C/D of the execution comparison):
                # different from the real cap only where ask + slip bound it.
                cap_fv = min(float(ref["fair_limit"]), float(getattr(self.broker, "max_buy_price", 1.0) or 1.0))
                if cap_fv > cap + 1e-9:
                    shares = ref.get("order_shares") or ref.get("target_shares")
                    fv = (round(cap_fv, 4), round(float(shares) * cap_fv, 2) if shares else None)
            out = {str(L): self._shadow_fill(views[L], token, spend, cap, rules, mids[L], late[L], alt, tight, fv)
                   for L in self.s.shadow_ms}
            self._write({"wall": time.time(), "result": "shadow", "id": ref.get("id"),
                         "side": ref.get("side"), "round_end": ref.get("round_end"),
                         "condition_id": cond, "token": token, "other_token": other, "L": out})
        except Exception as exc:  # noqa: BLE001 - diagnostics only, never the order path
            self.n["shadow errors"] += 1
            self._write({"wall": time.time(), "result": "shadow_error", "id": ref.get("id"),
                         "reason": f"{type(exc).__name__}: {exc}"[:200]})

    def _shadow_fill(self, view, token, spend, cap, rules, mid5, late_ms, alt_caps=None, tight=None,
                     fv=None) -> dict:
        """The paper broker's own FOK arithmetic against one captured book:
        the real cap, plus each wider cap in alt_caps ({"3": cap, ...}) -
        would paying up to ask+3c/4c/6c have turned misses into good fills? -
        and, with tight=(limit, spend), a TIGHTER limit under the real cap
        (FOK, else what FAK keeps): is the room above it worth having?"""
        from paper_trade import PaperRejected, snapshot_from_book_view
        row = {"late_ms": late_ms}
        if view is None:
            row["miss"] = "unknown book state"
            return row
        try:
            if str(getattr(view, "status", "")) != "LIVE":
                raise PaperRejected("book not live")
            book = snapshot_from_book_view(view, expected_token=token)
            if not book.asks or book.best_bid is None:
                raise PaperRejected("one-sided book")
            if float(book.asks[0][0] - book.best_bid) > float(getattr(self.broker, "max_spread", 1.0)):
                raise PaperRejected("spread over the paper limit")
        except PaperRejected as exc:
            row["miss"] = str(exc)[:60]
            return row
        row.update(self._shadow_quote(book, spend, cap, rules, mid5))
        if "miss" in row:
            # what fill-and-kill would have kept at the same cap (a full fill is
            # the same either way, so only priced when the FOK misses)
            row["fak"] = self._shadow_quote(book, spend, cap, rules, mid5, fak=True)
        if alt_caps:
            row["alt"] = {k: self._shadow_quote(book, spend, c, rules, mid5) for k, c in alt_caps.items()}
        if tight:
            tc, tspend = tight
            t = self._shadow_quote(book, tspend, tc, rules, mid5)
            if "miss" in t:
                t["fak"] = self._shadow_quote(book, tspend, tc, rules, mid5, fak=True)
            row["tight"] = {"cap": tc, **t}
        if fv:
            # at the fair-value cap: "same" = the order's dollars (sizing held fixed),
            # "own" = whole shares x the fair cap (share sizing at that cap)
            fc, own = fv
            f = {"cap": fc}
            for name, s_ in (("same", spend), ("own", own)):
                if s_:
                    q = self._shadow_quote(book, s_, fc, rules, mid5)
                    if "miss" in q:
                        q["fak"] = self._shadow_quote(book, s_, fc, rules, mid5, fak=True)
                    f[name] = {"spend": s_, **q}
            row["fv"] = f
        return row

    def _shadow_quote(self, book, spend, cap, rules, mid5, fak: bool = False) -> dict:
        from paper_trade import PaperRejected, estimate_fak, estimate_fok
        try:
            limit = min(float(cap), float(getattr(self.broker, "max_buy_price", 1.0) or 1.0))
            q = (estimate_fak if fak else estimate_fok)(book, spend, limit, rules,
                                                        min_price=float(getattr(self.broker, "min_buy_price", 0.0) or 0.0))
        except PaperRejected as exc:
            return {"miss": str(exc)[:60]}
        sh, px, fee = float(q.shares), float(q.average_price), float(q.fee)
        out = {"sh": round(sh, 4), "px": round(px, 4), "fee": round(fee, 5)}
        if mid5 is not None and sh > 0:
            out["mk5"] = round((float(mid5) - px - fee / sh) * 100, 3)
        return out

    # ---------------------------------------------------------------- book trace (diagnostics only)
    TRACE_RESP_WAIT_S = 15.0       # the longest the trace waits for the venue's reply
    USER_TRADES_WAIT_S = 90.0      # an accepted order's trades: until all are terminal, at most this
    NO_MATCH_TRADES_WAIT_S = 5.0   # an order the reply says matched nothing: proof that no trade came
    TRACE_REPLY_KEYS = ("success", "status", "errorMsg", "orderID", "tradeIDs", "makingAmount", "takingAmount",
                        "transactionsHashes")
    TRADE_TERMINAL = ("CONFIRMED", "FAILED", "CONFLICT")

    @staticmethod
    def _pc_to_mono(pc):
        """A perf_counter_ns stamp on the time.monotonic clock (the ticks' clock)."""
        if pc is None:
            return None
        return time.monotonic() - (time.perf_counter_ns() - int(pc)) / 1e9

    def _start_trace(self, row: dict, plan: dict) -> None:
        """After the send: record the book around this order (_book_trace).
        The finish handler resolves plan["trace_resp"] with the reply."""
        if not self.s.book_trace_ms or self._closing:
            return
        fut = asyncio.get_running_loop().create_future()
        plan["trace_resp"] = fut
        ref = {k: row.get(k) for k in ("id", "side", "wall", "loop_lag_ms", "round_end", "ask_seen", "bp", "feed")}
        self._spawn(self._book_trace(ref, plan, fut))

    @staticmethod
    def _trace_response(plan: dict, *, mono, source: str, **ident) -> None:
        fut = plan.get("trace_resp")
        if fut is not None and not fut.done():
            fut.set_result(dict(ident, mono=mono, source=source))

    def _book_point(self, view, cap: float) -> dict:
        """What a BUY could take at one instant: the best levels, the best bid,
        and the shares / dollars offered at or under our cap."""
        if view is None:
            return {"status": "UNKNOWN"}
        n = self.s.book_trace_levels
        asks = [(float(p), float(q)) for p, q in (getattr(view, "asks", ()) or ())]
        bids = [(float(p), float(q)) for p, q in (getattr(view, "bids", ()) or ())]
        under = [(p, q) for p, q in asks if p <= float(cap) + 1e-9]
        out = {"status": str(getattr(view, "status", "")),
               "bid": bids[0][0] if bids else None, "bid_size": bids[0][1] if bids else None,
               "asks": [[p, round(q, 4)] for p, q in asks[:n]],
               "depth_cap_sh": round(sum(q for _p, q in under), 4),
               "depth_cap_usd": round(sum(p * q for p, q in under), 4)}
        ts = getattr(view, "exchange_ts_ms", None)
        if ts is not None:
            out["book_ts"] = ts
        return out

    async def _book_trace(self, ref: dict, plan: dict, resp_fut) -> None:
        """book_trace row: the book at the tick + each SNIPER_BOOK_TRACE_MS and
        at the venue's reply, with the order's identity and the reply's status.
        Then, live, the order's trades as the authenticated user channel saw
        them (user_trades row). Diagnostics only: never the order path."""
        try:
            token, cap = plan["token"], float(plan["cap"])
            t0 = float(plan["t0"])                              # the BTC tick, time.monotonic seconds
            tick_ms = (float(ref["wall"]) - float(ref.get("loop_lag_ms") or 0) / 1000.0) * 1000.0 \
                + self.s.venue_offset_ms
            hist = self._history_book() is not None
            points = {}
            for L in self.s.book_trace_ms:
                view = await self._book_at(token, tick_ms + L, t0 + L / 1000.0)
                points[str(L)] = self._book_point(view, cap)
                if not hist:                                    # the book at wake-up: how late it woke
                    points[str(L)]["late_ms"] = round((time.monotonic() - t0) * 1000.0 - L, 1)
            try:
                resp = await asyncio.wait_for(asyncio.shield(resp_fut), self.TRACE_RESP_WAIT_S)
            except asyncio.TimeoutError:
                resp = {"source": "no reply in time", "mono": None}
            resp = dict(resp or {})
            mono = resp.pop("mono", None)
            if mono is not None:
                at = (float(mono) - t0) * 1000.0
                view = await self._book_at(token, tick_ms + at, float(mono))
                points["response"] = dict(self._book_point(view, cap), at_ms=round(at, 1))
            if resp.get("order_type") is None:
                resp["order_type"] = plan.get("order_type")
            self._write({"wall": time.time(), "result": "book_trace", "id": ref.get("id"), "side": ref.get("side"),
                         "round_end": ref.get("round_end"), "condition_id": plan.get("cond"), "token": token,
                         "live": self.live is not None, "cap": cap, "amount": round(float(plan["amount"]), 4),
                         "ask_seen": ref.get("ask_seen"), "bp": ref.get("bp"), "feed": ref.get("feed"),
                         "points": points, **{k: v for k, v in resp.items() if v is not None}})
            if self.live is not None and resp.get("order_id") and resp.get("kind") is not None:
                await self._user_trades(ref, resp, t0, token=token, tick_ms=tick_ms,
                                        reply_ms=None if mono is None else tick_ms + (float(mono) - t0) * 1000.0)
        except Exception as exc:  # noqa: BLE001 - diagnostics only, never the order path
            self.n["book trace errors"] += 1
            self._write({"wall": time.time(), "result": "book_trace_error", "id": ref.get("id"),
                         "reason": f"{type(exc).__name__}: {exc}"[:200]})

    async def _user_trades(self, ref: dict, resp: dict, t0: float, *, token=None, tick_ms=None,
                           reply_ms=None) -> None:
        """user_trades row: this order's trades from the authenticated user
        channel (feeds.poly_user) - each trade id with its status, price, size,
        maker legs, and when we first / last heard of it (ms after the tick).
        Waits until every trade is terminal and the reply's trade ids are all
        in, at most USER_TRADES_WAIT_S (no match: NO_MATCH_TRADES_WAIT_S)."""
        from sniper_live import fills_of_order
        oid = str(resp["order_id"])
        expected = {str(t) for t in ((resp.get("reply") or {}).get("tradeIDs") or ())}
        accepted = resp.get("kind") in ("MATCHED", "PENDING")
        limit = time.monotonic() + (self.USER_TRADES_WAIT_S if accepted else self.NO_MATCH_TRADES_WAIT_S)
        while True:
            rows = fills_of_order(getattr(self.live, "fills", None), oid)
            ids = {str(f.trade_id) for f in rows}
            complete = (bool(rows) and expected <= ids
                        and all(str(f.status or "").upper() in self.TRADE_TERMINAL for f in rows))
            if complete or self._closing or time.monotonic() >= limit:
                break
            await asyncio.sleep(1.0)

        def rel(m):
            return None if not m else round((float(m) - t0) * 1000.0, 1)
        trades = [{"trade_id": str(f.trade_id), "status": f.status, "side": f.side, "price": f.price,
                   "size": f.size, "paid": getattr(f, "paid", None),
                   "maker_legs": [list(x) for x in (getattr(f, "maker_legs", None) or ())] or None,
                   "sources": sorted(map(str, getattr(f, "sources", ()) or ())),
                   "revisions": getattr(f, "revisions", None),
                   "first_seen_ms": rel(getattr(f, "first_seen", None)),
                   "last_seen_ms": rel(getattr(f, "last_seen", None))} for f in rows]
        got = {t["trade_id"] for t in trades}
        match = self._venue_match(token, trades, tick_ms, reply_ms)
        if match.get("match_after_tick_ms") is not None:
            self.latency.add({"trigger_to_match": match["match_after_tick_ms"]})
        self._write({"wall": time.time(), "result": "user_trades", "id": ref.get("id"), "order_id": oid, **match,
                     "order_type": resp.get("order_type"), "kind": resp.get("kind"),
                     "reply_trade_ids": sorted(expected) or None, "trades": trades, "complete": complete,
                     "missing_reply_ids": sorted(expected - got) or None,
                     "shares": round(sum(float(f.size or 0) for f in rows
                                         if str(f.status or "").upper() not in ("FAILED", "CONFLICT")), 6)})

    MATCH_LOOKBACK_MS = 1000.0     # trade prints this long before the tick still count (clock skew)
    MATCH_LOOKAHEAD_MS = 2000.0    # and this long after the reply

    def _venue_match(self, token, trades: list, tick_ms, reply_ms) -> dict:
        """The venue's own match time for our trades, from the public market
        channel's trade prints (millisecond venue stamps): a BUY print on our
        token at one of our trades' prices whose size is that trade's size or
        one of its maker legs. The earliest such print is the match.
        match_after_tick_ms: tick -> match on the venue's clock (our tick wall
        time shifted by SNIPER_VENUE_CLOCK_OFFSET_MS); ack_after_match_ms:
        match -> the reply. {} when there is nothing to look in."""
        market = getattr(self.hub, "market", None)
        look = getattr(market, "trades_between", None)
        mine = [t for t in trades if str(t.get("status") or "").upper() not in ("FAILED", "CONFLICT")
                and t.get("size") and t.get("price")]
        if look is None or not token or tick_ms is None or not mine:
            return {}
        hi = (reply_ms if reply_ms is not None else tick_ms + 5000.0) + self.MATCH_LOOKAHEAD_MS
        prints = look(token, tick_ms - self.MATCH_LOOKBACK_MS, hi)
        sizes = set()
        prices = set()
        for t in mine:
            sizes.add(round(float(t["size"]), 4))
            prices.add(round(float(t["price"]), 4))
            for q, px in (t.get("maker_legs") or ()):
                sizes.add(round(float(q), 4))
                prices.add(round(float(px), 4))
        hits = [p for p in prints if p.get("side") in (None, "BUY") and p.get("size") is not None
                and round(float(p["size"]), 4) in sizes and round(float(p["price"]), 4) in prices]
        out = {"match_prints_seen": len(prints), "match_prints_hit": len(hits)}
        if hits:
            first = min(float(p["ts"]) for p in hits)
            out.update(venue_match_ms=int(first), match_after_tick_ms=round(first - tick_ms, 1))
            if reply_ms is not None:
                out["ack_after_match_ms"] = round(reply_ms - first, 1)
        return out

    async def _extra_feed(self, name: str) -> None:
        """A second-trigger feed's socket (SNIPER_EXTRA_FEEDS), reconnecting
        like the Binance one. Each (re)connect starts its rule afresh: a price
        from before a gap is no reference for "moved within 250ms"."""
        url, sub = EXTRA_FEEDS[name]
        handler = getattr(self, f"on_{name}")
        coalesce = self.s.feed_coalesce and name == "binance_fut"
        from feeds.supervisor import backoff_delay
        fresh = self._gate_for(name)
        if fresh is not None:
            fresh.enabled = self.s.coinbase_freshness if name == "coinbase" else True
        attempt = 0
        while True:
            opened = None
            try:
                from feeds.tls import ws_ssl
                async with websockets.connect(url, ssl=ws_ssl(url), ping_interval=20, ping_timeout=20,
                                              open_timeout=10, compression=None) as ws:
                    opened = time.monotonic()
                    if sub is not None:                     # binance_fut: the stream is in the URL
                        await ws.send(json.dumps(sub))
                    self.triggers[name] = Trigger(self.s.k_bp, self.s.window_ms)
                    if name == "binance_fut":
                        self._bf_last_u = None
                    if name == "coinbase":
                        self._coinbase_seq = self._coinbase_prev_t = None    # a new connection's own order
                    if fresh is not None:
                        fresh.on_connect()
                    last_tick = time.monotonic()
                    self.on_event("sniper", f"{name} ticker connected (second trigger)", "good")
                    while True:
                        # Binance futures ticks many times a second: this long a
                        # silence is a dead socket, as for Binance spot.
                        timeout = (self.s.coinbase_silent_s if name == "coinbase"
                                   else self.s.binance_silent_s if name == "binance_fut" else 30)
                        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        now = time.monotonic()
                        if fresh is not None:
                            fresh.note_message(now, rx_backlog(ws))
                        if coalesce:
                            # Frames already queued: history only; the newest is evaluated.
                            while (rx_backlog(ws) or 0) > 0:
                                handler(raw.decode() if isinstance(raw, bytes) else raw, evaluate=False)
                                raw = await ws.recv()
                                now = time.monotonic()
                                if fresh is not None:
                                    fresh.note_message(now, rx_backlog(ws))
                        kind = handler(raw.decode() if isinstance(raw, bytes) else raw)
                        if kind in ("tick", "late"):
                            last_tick = now
                        elif name == "coinbase" and now - last_tick >= COINBASE_STARVED_S:
                            raise CoinbaseFeedError("second trigger subscription starved")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect; Binance keeps triggering meanwhile
                self.n[f"{name} reconnects"] += 1
                self.on_event("sniper", f"{name} ticker {type(exc).__name__}; reconnecting", "warn")
            finally:
                if fresh is not None:
                    fresh.on_disconnect()
            if opened is not None and time.monotonic() - opened >= 30:
                attempt = 0
            await asyncio.sleep(backoff_delay(attempt))
            attempt += 1

    # ---------------------------------------------------------------- status
    def summary(self) -> dict:
        """The status summary (dashboard, journal rows, reports). On the event
        loop prefer summary_snapshot() there and summary_from() on a worker:
        the percentiles of the feeds' sample windows (tens of thousands of
        samples) cost ~8ms of sorting a call, and every BTC tick waited for it."""
        return self.summary_from(self.summary_snapshot())

    def summary_snapshot(self) -> dict:
        """Everything summary() reads, copied: cheap, for the event loop."""
        book = getattr(self.hub, "book", None)
        return {"n": dict(self.n), "marks_1s": list(self.marks["1s"]), "marks_5s": list(self.marks["5s"]),
                "kraken_lag": list(self.kraken_lag) if self.s.primary_feed == "kraken" else None,
                "kraken_gap_max": self.kraken_gap_max, "kraken_system": self.kraken_system,
                "coinbase": self.cb_fresh.snapshot() if self.s.primary_feed == "coinbase" else None,
                "coinbase_gap_max": self.coinbase_gap_max, "cb_up": len(self._cb_up),
                "cb_behind": list(self.cb_behind) if self.s.coinbase_conns > 1 else None,
                "binance_fut": self.bf_fresh.snapshot() if "binance_fut" in self.s.extra_feeds else None,
                "binance": self.fresh.snapshot(),
                "carry": self.carry_view() if self.s.order_type == "FAK" else None,
                "fair": ({"vol_bp": (round(v * 1e4, 3) if (v := self.vol_model.vol()) is not None else None),
                          "samples": len(self.vol_model._incs),
                          "bankroll": self._bankroll if self.live is None else self._read_bankroll(),
                          "last": self.last_fair} if self.s.fair else None),
                "latency_ms": round(self.latency_plan()[0], 1),
                "exec_latency": self.latency.snapshot(), "exec_quality": self.quality.summary(),
                "clob_rtt_ms": round(sorted(self.net_rtt)[len(self.net_rtt) // 2], 1) if self.net_rtt else None,
                "loop_lag": list(self.loop_lag),
                "book_feed_age": book.feed_age_samples() if hasattr(book, "feed_age_samples") else (
                    book.feed_age_summary() if hasattr(book, "feed_age_summary") else None),
                "live": self.live.status() if self.live is not None else None}

    def summary_from(self, snap: dict) -> dict:
        """summary() from a summary_snapshot() - any thread; the sorting is here."""
        n = collections.Counter(snap["n"])
        avg = lambda xs: round(sum(xs) / len(xs), 2) if xs else None
        done = n["filled"] + n["refused"]
        book_age = snap["book_feed_age"]
        return {"signals": n["signals"], "orders": n["orders"], "filled": n["filled"],
                "refused": n["refused"], "fill_rate": round(n["filled"] / done, 3) if done else None,
                "markout_1s_c": avg(snap["marks_1s"]), "markout_5s_c": avg(snap["marks_5s"]),
                "skips": {k[6:]: v for k, v in n.items() if k.startswith("skip: ")},
                "fired_by_feed": {k[6:]: v for k, v in n.items() if k.startswith("fired ")},
                **({"signals_by_feed": {self.s.primary_feed: n["signals"],
                                        **{f: n[f"signals {f}"] for f in self.s.extra_feeds}}}
                   if self.s.extra_feeds else {}),
                **({"kraken": {"ticks": n["kraken ticks"], "heartbeats": n["kraken heartbeats"],
                               "malformed": n["kraken malformed"], "reconnects": n["reconnects"],
                               "gaps_over_max_gap": n["kraken gaps over max gap"],
                               "gap_ms_max": round(snap["kraken_gap_max"], 1),
                               "lag_ms": quantiles(snap["kraken_lag"]), "system": snap["kraken_system"]}}
                   if self.s.primary_feed == "kraken" else {}),
                **({"coinbase": {**self.cb_fresh.summary(snap["coinbase"]), "ticks": n["coinbase ticks"],
                                 "heartbeats": n["coinbase heartbeats"],
                                 "malformed": n["coinbase malformed"],
                                 "out_of_order": n["coinbase out of order"], "reconnects": n["reconnects"],
                                 "gaps_over_max_gap": n["coinbase gaps over max gap"],
                                 "gap_ms_max": round(snap["coinbase_gap_max"], 1),
                                 **({"sockets": self.s.coinbase_conns, "sockets_up": snap["cb_up"],
                                     "already_heard": n["coinbase already heard"],
                                     "first_by_socket": [n[f"coinbase first on conn {j + 1}"]
                                                         for j in range(self.s.coinbase_conns)],
                                     "copy_behind_ms": quantiles(snap["cb_behind"])}
                                    if self.s.coinbase_conns > 1 else {})}}
                   if self.s.primary_feed == "coinbase" else {}),
                **({"binance_fut": {**self.bf_fresh.summary(snap["binance_fut"]), "ticks": n["binance_fut ticks"],
                                    "malformed": n["binance_fut malformed"],
                                    "reconnects": n["binance_fut reconnects"]}}
                   if "binance_fut" in self.s.extra_feeds else {}),
                **({"carry": snap["carry"]} if self.s.order_type == "FAK" else {}),
                **({"fair": snap["fair"]} if self.s.fair else {}),
                "errors": n["errors"], "shadow_errors": n["shadow errors"],
                "latency_ms": snap["latency_ms"],       # paper's MODELLED flight, not a measurement
                "exec_latency": self.latency.summary_from(snap["exec_latency"]), "exec_quality": snap["exec_quality"],
                "clob_rtt_ms": snap["clob_rtt_ms"],
                "loop_lag_ms": self._lag_summary(snap["loop_lag"]),
                # (the trigger socket's reconnects: Binance's only when it is the trigger)
                "binance": {**self.fresh.summary(snap["binance"]),
                            "reconnects": n["reconnects"] if self.s.primary_feed == "binance" else 0,
                            "parse_errors": n["binance parse errors"]},
                "book_feed_age_ms": (book_age if not isinstance(book_age, list) else
                                     ({k: v for k, v in quantiles(book_age).items() if k != "p95"}
                                      if book_age else None)),
                **({"pending": n["pending"], "live": snap["live"]} if self.live is not None else {})}

    def _lag_summary(self, xs=None):
        q = quantiles(list(self.loop_lag) if xs is None else xs, digits=2)
        if q is None:
            return None
        return {k: v for k, v in q.items() if k != "p95"}

    async def report(self, describe) -> None:
        """The 5-minute status: event line + journal summary row, with the
        sorting on a worker (it used to stall the tick loop ~8ms each time)."""
        try:
            s = await asyncio.get_running_loop().run_in_executor(None, self.summary_from, self.summary_snapshot())
        except Exception as exc:  # noqa: BLE001 - a status row must never stop the feed
            self.on_event("sniper", f"status summary failed: {type(exc).__name__}: {exc}", "warn")
            return
        self.on_event("sniper", describe(s), "info")
        self._write({"wall": time.time(), "result": "summary", **s})

    async def _loop_lag_probe(self) -> None:
        """How late a 100ms timer wakes, every 100ms, for the last ~5 minutes.

        Every stage the sniper stamps starts when on_tick RUNS; a BTC packet
        that arrived while the event loop was busy (a dashboard frame, a GC
        pause, a GIL-holding write) waited before that, invisibly. This is
        that wait, sampled: it is printed by sniper_latency.py."""
        while True:
            t = time.perf_counter()
            await asyncio.sleep(0.1)
            self.loop_lag.append((time.perf_counter() - t - 0.1) * 1000.0)

    # ---------------------------------------------------------------- run
    async def run(self) -> None:
        import sys
        switch_ms = _env_float("SNIPER_SWITCH_MS", 0)
        if switch_ms > 0:
            # How long another thread may hold the GIL before the event loop
            # gets it back (Python default 5ms). Shorter = the loop waits less
            # behind ledger/dashboard work; those threads switch more often.
            sys.setswitchinterval(switch_ms / 1000.0)
        # Marks a restart in the journal: the checker reports from here by default.
        self._write({"wall": time.time(), "result": "start", "code": self.code, "live": self.live is not None,
                     "settings": self.lines()[0], "switch_ms": round(sys.getswitchinterval() * 1000, 3)})
        self.on_event("sniper", self.lines()[0], "warn" if self.live is not None else "info")
        for gap in self.plan_gaps():
            self.on_event("sniper", gap, "warn")
        if self.own_rounds and self.trade_rows is not None:
            self._spawn(self._round_watch())
        self._spawn(self._prefetch_rules())
        for name in self.s.extra_feeds:
            self._spawn(self._extra_feed(name))
        self._spawn(self._loop_lag_probe())
        self._spawn(self._gc_schedule())
        if self.s.fair and self.s.kelly and self.live is None:
            self._spawn(self._bankroll_loop())          # live reads the sender's own figures each time
        stall_ms = _env_float("SNIPER_STALL_MS", 50)
        if stall_ms > 0 and self.journal is not None:
            self.stallwatch = StallWatch(asyncio.get_running_loop(), self._write, threshold_ms=stall_ms).start()
        if not self.s.fixed_latency:
            self._spawn(self._net_probe())
            if self._builder is None and self.live is None:
                self.on_event("sniper", "py_clob_client_v2 not importable: orders are not signed "
                                        "(latency is still live-like)", "warn")
        if self.s.primary_feed == "kraken":
            await self._kraken_loop()
            return
        if self.s.primary_feed == "coinbase":
            await self._coinbase_loop()
            return
        from feeds.supervisor import backoff_delay
        from feeds.tls import ws_ssl
        fresh = self.fresh
        fresh.enabled = self.s.binance_freshness
        url = binance_stream_url(self.s.binance_url) if fresh.enabled else self.s.binance_url
        if not fresh.enabled:
            self.on_event("sniper", "SNIPER_BINANCE_FRESHNESS=0: Binance ticks are NOT checked for age "
                                    "(bare bookTicker, no exchange timestamps)", "warn")
        report_at = time.monotonic() + 300
        attempt = 0
        while True:
            opened = None
            try:
                async with websockets.connect(url, ssl=ws_ssl(url), ping_interval=20,
                                              ping_timeout=20, open_timeout=10, compression=None) as ws:
                    opened = time.monotonic()
                    self.trigger = Trigger(self.s.k_bp, self.s.window_ms)
                    self.triggers["binance"] = self.trigger
                    fresh.on_connect()
                    self.on_event("sniper", "Binance book ticker connected"
                                  + (" (+ trade timestamps: freshness gate on)" if fresh.enabled else ""), "good")
                    while True:
                        # Binance BTCUSDT ticks many times a second: this long a
                        # silence is a dead or stuck socket, not a quiet market.
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.s.binance_silent_s)
                        fresh.note_message(time.monotonic(), rx_backlog(ws))
                        self.on_tick(raw.decode() if isinstance(raw, bytes) else raw)
                        if time.monotonic() >= report_at:
                            report_at = time.monotonic() + 300

                            def describe(s):
                                b = s.get("binance") or {}
                                ex = b.get("excess_ms") or {}
                                return (f"orders {s['orders']} filled {s['filled']} "
                                        f"refused {s['refused']} markout 5s {s['markout_5s_c']}c; "
                                        f"binance {b.get('status')} excess p50/p99 "
                                        f"{ex.get('p50')}/{ex.get('p99')}ms "
                                        f"backlog max {b.get('backlog_max')}")
                            self._spawn(self.report(describe))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect; the bot keeps running
                self.n["reconnects"] += 1
                self.on_event("sniper", f"book ticker {type(exc).__name__}"
                                        f"{' (silent ' + format(self.s.binance_silent_s, 'g') + 's)' if isinstance(exc, asyncio.TimeoutError) else ''}"
                                        f"; reconnecting", "warn")
            finally:
                fresh.on_disconnect()
            # Fast, jittered retries (0.25s .. 8s); the ladder only resets after
            # a connection has held for 30s, so a connect/drop loop cannot spin.
            if opened is not None and time.monotonic() - opened >= 30:
                attempt = 0
            await asyncio.sleep(backoff_delay(attempt))
            attempt += 1

    async def _coinbase_loop(self) -> None:
        """The trigger's sockets when SNIPER_PRIMARY_FEED=coinbase (the default):
        Coinbase Exchange's public BTC-USD ticker + heartbeat channels, on
        SNIPER_COINBASE_CONNS sockets at once (default 2). Each ticker is used
        from whichever socket delivers it first - the venue's sequence numbers
        it - and its copy on another socket is dropped, so one socket stalling,
        dropping or falling behind costs nothing while another is up.

        Each socket reconnects on its own, fast and jittered (0.25s .. 8s), the
        ladder resetting only after a connection has held 30s, so a
        connect/drop loop cannot spin. When NO socket was up, the first one
        back starts the rule afresh - a price from before a gap is no reference
        for "moved within 250ms". Heartbeats arrive every second whatever the
        market does, so SNIPER_COINBASE_SILENT_S of silence is a dead socket,
        and heartbeats with no BTC-USD ticker (first-heard or a copy) for
        COINBASE_STARVED_S are a lost subscription. The freshness gate
        (cb_fresh) is on for these sockets only. No permessage-deflate:
        inflating every message is CPU the 1-vCPU VPS spends before the tick
        can be read."""
        fresh = self.cb_fresh
        fresh.enabled = self.s.coinbase_freshness
        if not fresh.enabled:
            self.on_event("sniper", "SNIPER_COINBASE_FRESHNESS=0: Coinbase ticks are NOT checked for age", "warn")
        self._cb_report_at = time.monotonic() + 300
        n = self.s.coinbase_conns
        if n == 1:
            await self._coinbase_conn(0)
            return
        await asyncio.gather(*(self._coinbase_conn(i) for i in range(n)))

    def _cb_race(self, i: int, kind: str, now: float) -> None:
        """Several sockets: which one heard each ticker first, and how far behind
        the copy came on the other(s)."""
        if kind == "tick":
            self.n[f"coinbase first on conn {i + 1}"] += 1
            if self._coinbase_seq is not None:
                first = self._cb_first
                first[self._coinbase_seq] = now
                if len(first) > 512:
                    del first[next(iter(first))]
        else:
            t0 = self._cb_first.get(self._cb_late_seq)
            if t0 is not None:
                self.cb_behind.append((now - t0) * 1000.0)

    async def _coinbase_conn(self, i: int) -> None:
        """One of the trigger's Coinbase sockets (see _coinbase_loop)."""
        from feeds.supervisor import backoff_delay
        from feeds.tls import ws_ssl
        fresh, url, n = self.cb_fresh, self.s.coinbase_url, self.s.coinbase_conns
        tag = f" [socket {i + 1}/{n}]" if n > 1 else ""
        attempt = 0
        while True:
            opened = None
            try:
                async with websockets.connect(url, ssl=ws_ssl(url), ping_interval=20, ping_timeout=20,
                                              open_timeout=10, compression=None) as ws:
                    opened = time.monotonic()
                    if not self._cb_up:
                        # no socket was up: nothing heard before vouches for this one
                        self.trigger = Trigger(self.s.k_bp, self.s.window_ms)
                        self.triggers["coinbase"] = self.trigger
                        self.last_mid = None
                        self._coinbase_seq = self._coinbase_prev_t = None
                        self._cb_first.clear()
                        fresh.on_connect()
                    self._cb_up.add(i)
                    await ws.send(json.dumps(COINBASE_SUBSCRIBE))
                    ticked = False
                    last_tick = time.monotonic()
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.s.coinbase_silent_s)
                        now = time.monotonic()
                        fresh.note_message(now, rx_backlog(ws))
                        kind = self.on_coinbase(raw.decode() if isinstance(raw, bytes) else raw)
                        if kind == "tick" or kind == "late":
                            last_tick = now
                            if n > 1:
                                self._cb_race(i, kind, now)
                            if not ticked:
                                ticked = True
                                self.on_event("sniper", f"Coinbase {COINBASE_PRODUCT} ticker connected"
                                              + (" (freshness gate on)" if fresh.enabled else "") + tag, "good")
                        elif now - last_tick >= COINBASE_STARVED_S:
                            raise CoinbaseFeedError(f"no {COINBASE_PRODUCT} ticker for {COINBASE_STARVED_S:.0f}s "
                                                    f"(heartbeats only): subscription lost")
                        if now >= self._cb_report_at:
                            self._cb_report_at = now + 300

                            def describe(s, n=n):
                                c = s.get("coinbase") or {}
                                ex = c.get("excess_ms") or {}
                                return (f"orders {s['orders']} filled {s['filled']} "
                                        f"refused {s['refused']} markout 5s {s['markout_5s_c']}c; "
                                        f"coinbase {c.get('status')} {c.get('ticks')} tickers, excess "
                                        f"p50/p99 {ex.get('p50')}/{ex.get('p99')}ms, longest gap "
                                        f"{c.get('gap_ms_max')}ms"
                                        + (f", sockets up {c.get('sockets_up')}/{n}" if n > 1 else ""))
                            self._spawn(self.report(describe))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect; the bot keeps running
                self.n["reconnects"] += 1
                left = len(self._cb_up - {i})
                self.on_event("sniper", f"Coinbase ticker {type(exc).__name__}"
                                        f"{': ' + str(exc)[:100] if isinstance(exc, CoinbaseFeedError) else ''}"
                                        f"{' (silent ' + format(self.s.coinbase_silent_s, 'g') + 's)' if isinstance(exc, asyncio.TimeoutError) else ''}"
                                        f"; reconnecting{tag}"
                                        + (f" ({left} other socket{'s' if left > 1 else ''} still up)" if left else ""),
                              "info" if left else "warn")
            finally:
                self._cb_up.discard(i)
                if not self._cb_up:
                    fresh.on_disconnect()                    # the last socket down: no signal until one is back
            if opened is not None and time.monotonic() - opened >= 30:
                attempt = 0
            await asyncio.sleep(backoff_delay(attempt))
            attempt += 1

    async def _kraken_loop(self) -> None:
        """The trigger's socket when SNIPER_PRIMARY_FEED=kraken: Kraken's public
        BTC/USD ticker, reconnecting with backoff. Every (re)connect starts the
        rule afresh - a price from before a gap is no reference for "moved
        within 250ms" - and the backoff only resets once a usable update has
        arrived, so a socket that connects but refuses the subscription cannot
        spin. The server's heartbeats keep recv() ticking while the book is
        quiet, so a silence of KRAKEN_RECV_TIMEOUT_S is a dead socket;
        heartbeats with no BTC/USD update for KRAKEN_STARVED_S are a lost
        subscription. Liveness is the heartbeat plus an application-level ping
        every KRAKEN_PING_S. No permessage-deflate: inflating every message is
        CPU the 1-vCPU VPS spends before the tick can be read."""
        from feeds.tls import ws_ssl
        backoff = KRAKEN_BACKOFF_S
        report_at = time.monotonic() + 300
        while True:
            try:
                async with websockets.connect(self.s.kraken_url, ssl=ws_ssl(self.s.kraken_url), ping_interval=None,
                                              open_timeout=10, compression=None) as ws:
                    self.trigger = Trigger(self.s.k_bp, self.s.window_ms)
                    self.triggers["kraken"] = self.trigger
                    self.last_mid = None
                    self._kraken_prev_t = None
                    await ws.send(json.dumps(KRAKEN_SUBSCRIBE))
                    ticked = False
                    last_rx = last_tick = time.monotonic()
                    next_ping = last_rx + KRAKEN_PING_S
                    while True:
                        # Wait for the next message, but no longer than the next ping or
                        # the liveness deadline - whichever comes first.
                        wait = min(next_ping, last_rx + KRAKEN_RECV_TIMEOUT_S) - time.monotonic()
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=max(wait, 0.001))
                        except asyncio.TimeoutError:
                            if time.monotonic() - last_rx >= KRAKEN_RECV_TIMEOUT_S:
                                raise                                  # nothing, not even a heartbeat: dead
                        else:
                            last_rx = time.monotonic()
                            kind = self.on_kraken(raw.decode() if isinstance(raw, bytes) else raw)
                            if kind == "tick":
                                last_tick = last_rx
                                if not ticked:
                                    ticked, backoff = True, KRAKEN_BACKOFF_S
                                    self.on_event("sniper", f"Kraken {KRAKEN_SYMBOL} ticker connected", "good")
                            elif kind == "status" and self.kraken_system != "online":
                                self.on_event("sniper", f"Kraken reports system '{self.kraken_system}'", "warn")
                        if time.monotonic() - last_tick >= KRAKEN_STARVED_S:
                            raise KrakenFeedError(f"no {KRAKEN_SYMBOL} update for {KRAKEN_STARVED_S:.0f}s "
                                                  f"(heartbeats only): subscription lost")
                        if time.monotonic() >= report_at:
                            report_at = time.monotonic() + 300

                            def describe(s):
                                k = s.get("kraken") or {}
                                lag = k.get("lag_ms") or {}
                                return (f"orders {s['orders']} filled {s['filled']} "
                                        f"refused {s['refused']} markout 5s {s['markout_5s_c']}c; "
                                        f"kraken {k.get('ticks')} updates, lag p50/p99 "
                                        f"{lag.get('p50')}/{lag.get('p99')}ms, longest gap "
                                        f"{k.get('gap_ms_max')}ms")
                            self._spawn(self.report(describe))
                        if time.monotonic() >= next_ping:
                            next_ping = time.monotonic() + KRAKEN_PING_S
                            await ws.send('{"method":"ping"}')
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect; the bot keeps running
                self.n["reconnects"] += 1
                self.on_event("sniper", f"Kraken ticker {type(exc).__name__}"
                                        f"{': ' + str(exc)[:100] if isinstance(exc, KrakenFeedError) else ''}; "
                                        f"reconnecting", "warn")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)
