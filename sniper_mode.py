"""BTC-tick sniper inside the bot - paper, or a capped LIVE CANARY.

Paper by default. With --live, SNIPER_ONLY=1 and SNIPER_LIVE_CANARY=1 (all
three, or run_feeds refuses to start) the same decisions go to the venue as
real FOK orders through sniper_live.LiveExecution, under hard canary limits;
nothing below changes what or when it trades, only who executes.

The latency lab's frozen plan, run on the bot's own paper broker, ledger and
settlement so its fills land in the same account:

  * when the Binance SPOT mid moves >= SNIPER_K_BP within SNIPER_WINDOW_MS,
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

It fires from its own Binance book-ticker socket, not from the strategy's
decision loop: the edge is gone within ~300ms of the BTC move and the loop
runs about once a second. Every paper-broker guard still applies (price band,
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
    SNIPER_K_BP=1.0             SNIPER_WINDOW_MS=250
    SNIPER_SHARES=10            SNIPER_SLIP=0.02
    SNIPER_LATENCY_MS=(unset)   SNIPER_COOLDOWN_MS=1000
    SNIPER_MIN_LEFT_S=10        SNIPER_MAX_FILLS_ROUND=20
    SNIPER_MAX_SIDE_SHARES=200  SNIPER_STALE_MS=2000
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
                                SECOND TRIGGER: the same rule on its own mid, and
                                whichever feed signals first fires (one shared
                                cooldown and in-flight flag: one move, one order).
                                Every decision is journalled with the feed that fired.
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
import time
from datetime import datetime
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path

import websockets

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
    msgs = getattr(ws, "messages", None)                         # legacy implementation (12-13)
    return len(msgs) if msgs is not None and hasattr(msgs, "__len__") else None


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
    clock check, never used to gate."""

    def __init__(self, stale_ms: float, unknown_ms: float, backlog_max: int, baseline_s: float = 300.0) -> None:
        self.stale_ms, self.unknown_ms, self.backlog_max = stale_ms, unknown_ms, backlog_max
        self.baseline_s = baseline_s
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
        except (TypeError, ValueError):
            self.n["trade without E"] += 1
            return
        if not math.isfinite(lag):
            self.n["trade without E"] += 1
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
            return "binance disconnected"
        if self.backlog > self.backlog_max:
            return "binance receive backlog"
        if self.last_trade_mono is None or (now_mono - self.last_trade_mono) * 1000.0 > self.unknown_ms:
            return "binance age unknown"
        if self.last_excess is not None and self.last_excess > self.stale_ms:
            return "binance feed behind"
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
        xs = sorted(xs)
        if not xs:
            return None
        q = lambda p: round(xs[min(len(xs) - 1, int(len(xs) * p / 100))], 1)
        return {"p50": q(50), "p90": q(90), "p95": q(95), "p99": q(99), "max": round(xs[-1], 1), "n": len(xs)}

    def summary(self) -> dict:
        base = self._min[0][1] if self._min else None
        return {"status": self.status(), "trades": self.n["trades"], "backlog_max": self.backlog_max_seen,
                "excess_ms": self._q(self.excess), "age_ms": self._q(self.lag),
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
PRIMARY_FEEDS = ("binance", "kraken")
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
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000.0
    except ValueError:
        return None


def _num(v) -> float | None:
    """A finite positive JSON number (never a bool or a string), else None."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v) if math.isfinite(v) and v > 0 else None


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


# Second-trigger feeds (SNIPER_EXTRA_FEEDS): name -> (url, subscribe message).
# Coinbase's engine is ~40ms from Amsterdam against Binance's ~127ms, so a BTC
# move that starts in the US reaches the VPS sooner through it. Its ticker
# carries the best bid/ask after every match; heartbeats keep a quiet
# socket provably alive.
EXTRA_FEEDS = {
    "coinbase": ("wss://ws-feed.exchange.coinbase.com",
                 {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker", "heartbeat"]}),
}
MIN_ORDER_USD = 1.0   # venue floor on a marketable BUY (paper_trade.MIN_MARKETABLE_BUY_USD)


def _env_float(name: str, default: float) -> float:
    try:
        v = float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)
    return v if math.isfinite(v) else float(default)


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
        return bm.SignedOrderV2(**{**dataclasses.asdict(order), "signature": self.sign(eb, order)})

    def sign(self, eb, order) -> str:
        """The order's EIP-712 signature: '0x' + r || s || v (v = 27/28), as
        eth_account's sign_message returns it."""
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
        sig = self._pk().sign_msg_hash(keccak(b"\x19\x01" + eb.app_domain_separator + struct))
        return "0x" + (sig.r.to_bytes(32, "big") + sig.s.to_bytes(32, "big") + bytes([sig.v + 27])).hex()


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
        self.k_bp = _env_float("SNIPER_K_BP", 1.0)
        self.window_ms = _env_float("SNIPER_WINDOW_MS", 250)
        self.shares = _env_float("SNIPER_SHARES", 10)
        self.slip = _env_float("SNIPER_SLIP", 0.02)
        # Set -> the old fixed delay, no signing. Unset -> live-like.
        self.fixed_latency = bool(str(os.environ.get("SNIPER_LATENCY_MS", "")).strip())
        self.latency_ms = _env_float("SNIPER_LATENCY_MS", 200)
        # Polymarket's documented taker delay on up/down markets flagged itode:
        # "The order is held for 250 ms, then validation runs again" (docs,
        # concepts/order-lifecycle). 150 was an older assumption.
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
        self.delay_note = ""                   # set by a paper Sniper once the live delays are loaded
        self.cooldown_ms = _env_float("SNIPER_COOLDOWN_MS", 1000)
        self.min_left_s = _env_float("SNIPER_MIN_LEFT_S", 10)
        self.max_fills_round = int(_env_float("SNIPER_MAX_FILLS_ROUND", 20))
        self.max_side_shares = _env_float("SNIPER_MAX_SIDE_SHARES", 200)
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
        self.venue_offset_ms = _env_float("SNIPER_VENUE_CLOCK_OFFSET_MS", 0)
        raw = os.environ.get("SNIPER_SHADOW_MS", "100,200,300,400,500")
        try:
            self.shadow_ms = tuple(sorted({int(float(x)) for x in raw.split(",") if x.strip()}))
        except ValueError:
            self.shadow_ms = ()
        self.shadow_ms = tuple(x for x in self.shadow_ms if 0 <= x <= 5000)
        raw = os.environ.get("SNIPER_SHADOW_SLIPS", "0.03,0.04,0.06")
        try:
            self.shadow_slips = tuple(sorted({round(float(x), 4) for x in raw.split(",") if x.strip()}))
        except ValueError:
            self.shadow_slips = ()
        self.shadow_slips = tuple(x for x in self.shadow_slips if 0 < x < 0.5)
        # Sniper-only defaults to keeping partial fills inside the same cap.
        # An explicit .env/shell choice still wins; legacy combined mode is FOK.
        default_type = "FAK" if only() else "FOK"
        self.order_type = str(os.environ.get("SNIPER_ORDER_TYPE", default_type)).strip().upper() or default_type
        if self.order_type not in ("FOK", "FAK"):
            raise ValueError(f"invalid SNIPER_ORDER_TYPE {self.order_type!r}: FOK or FAK")
        self.binance_url = binance_url(os.environ.get("SNIPER_BINANCE_URL"))
        # Binance freshness gate (BinanceFreshness): on by default; 0 restores the
        # bare bookTicker socket with no exchange timestamps and no gate.
        self.binance_freshness = str(os.environ.get("SNIPER_BINANCE_FRESHNESS", "1")).strip() not in ("0", "false", "off")
        self.binance_stale_ms = _env_float("SNIPER_BINANCE_STALE_MS", 1000)
        self.binance_unknown_ms = _env_float("SNIPER_BINANCE_UNKNOWN_MS", 5000)
        self.binance_backlog_max = int(_env_float("SNIPER_BINANCE_BACKLOG_MAX", 20))
        self.binance_silent_s = _env_float("SNIPER_BINANCE_SILENT_S", 5)
        # Which feed triggers the sniper: Binance's BTCUSDT book ticker (the
        # default) or Kraken's BTC/USD best bid/offer - paper only (Sniper and
        # run_feeds refuse a live canary on it).
        self.primary_feed = str(os.environ.get("SNIPER_PRIMARY_FEED", "binance")).strip().lower() or "binance"
        if self.primary_feed not in PRIMARY_FEEDS:
            raise ValueError(f"invalid SNIPER_PRIMARY_FEED {self.primary_feed!r}: {' or '.join(PRIMARY_FEEDS)}")
        self.kraken_url = kraken_url(os.environ.get("SNIPER_KRAKEN_URL"))
        raw = os.environ.get("SNIPER_EXTRA_FEEDS", "")
        self.extra_feeds = tuple(dict.fromkeys(x.strip().lower() for x in raw.split(",") if x.strip()))
        unknown = [x for x in self.extra_feeds if x not in EXTRA_FEEDS]
        if unknown:
            raise ValueError(f"invalid SNIPER_EXTRA_FEEDS {unknown}: supported {sorted(EXTRA_FEEDS)}")
        bad = [text for ok, text in (
            (self.k_bp > 0, f"SNIPER_K_BP={self.k_bp:g} (must be > 0)"),
            (self.window_ms > 0, f"SNIPER_WINDOW_MS={self.window_ms:g} (must be > 0)"),
            (self.shares >= 5, f"SNIPER_SHARES={self.shares:g} (venue minimum is 5)"),
            (0 <= self.slip < 0.5, f"SNIPER_SLIP={self.slip:g} (must be 0 to 0.49; the price limit is also "
                                   f"capped at MAX_BUY_PRICE, so 0.45 already means 'up to MAX_BUY_PRICE' for "
                                   f"asks from 0.50)"),
            (0 <= self.latency_ms < 60_000, f"SNIPER_LATENCY_MS={self.latency_ms:g} (0 to 60000)"),
            (self.max_fills_round > 0, f"SNIPER_MAX_FILLS_ROUND={self.max_fills_round} (must be > 0)"),
            (min(self.hold_ms, self.server_ms, self.net_default_ms) >= 0,
             "SNIPER_HOLD_MS / SNIPER_SERVER_MS / SNIPER_NET_MS must be >= 0"),
            (self.binance_stale_ms > 0 and self.binance_unknown_ms > 0 and self.binance_backlog_max >= 0
             and self.binance_silent_s > 0,
             "SNIPER_BINANCE_STALE_MS / _UNKNOWN_MS / _SILENT_S must be > 0, _BACKLOG_MAX >= 0")) if not ok]
        if bad:
            raise ValueError("invalid SNIPER_* settings: " + "; ".join(bad))

    def feeds_text(self) -> str:
        route = "" if self.binance_url == BOOK_TICKER_URL else f"; Binance via {self.binance_url}"
        if self.primary_feed == "kraken" and self.kraken_url != KRAKEN_TICKER_URL:
            route += f"; Kraken via {self.kraken_url}"
        return route + (("; second trigger: " + ",".join(self.extra_feeds)
                         + " (whichever feed signals first, shared cooldown)") if self.extra_feeds else "")

    def lead_text(self) -> str:
        """Names the trigger on every start line when it is not Binance, so a
        journal says which feed decided its orders."""
        return f" [triggered by Kraken {KRAKEN_SYMBOL} ticker (bbo), not Binance]" if self.primary_feed == "kraken" else ""

    def lines(self, live: bool = False) -> list[str]:
        if live:
            return [f"SNIPER LIVE CANARY{self.lead_text()}: spot mid >= {self.k_bp}bp within {self.window_ms:.0f}ms -> real {self.order_type} "
                    f"{self.shares:.0f} sh at ask+{self.slip:.2f}, cooldown {self.cooldown_ms:.0f}ms, "
                    f">= {self.min_left_s:.0f}s left, <= {self.max_fills_round} fills and "
                    f"{self.max_side_shares:.0f} sh/side per round"
                    + (f"; shadow fills at {','.join(map(str, self.shadow_ms))}ms (arithmetic only)"
                       if self.shadow_ms else "") + self.feeds_text()]
        return [f"SNIPER paper-only{self.lead_text()}: spot mid >= {self.k_bp}bp within {self.window_ms:.0f}ms -> {self.order_type} "
                f"{self.shares:.0f} sh at ask+{self.slip:.2f}, "
                + (f"matched {self.latency_ms:.0f}ms after deciding (fixed); " if self.fixed_latency else
                   f"real order signing + {self.delay_note}; " if self.delay_note else
                   f"live-like: real order signing + measured one-way to the CLOB + {self.hold_ms:.0f}ms hold "
                   f"+ {self.server_ms:.0f}ms server allowance; ")
                + f"cooldown {self.cooldown_ms:.0f}ms, >= {self.min_left_s:.0f}s left, "
                f"<= {self.max_fills_round} fills and {self.max_side_shares:.0f} sh/side per round"
                + (f"; shadow fills at {','.join(map(str, self.shadow_ms))}ms (arithmetic only)"
                   if self.shadow_ms else "")
                + ("" if self.fixed_latency else
                   f"; paper fills against the book at the modelled match instant ({self.book_clock} clock)")
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
        r = (mid / q[0][1] - 1.0) * 1e4
        if r >= self.k_bp:
            return 1, r
        if r <= -self.k_bp:
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
        if live is not None and self.s.primary_feed != "binance" and not kraken_live():
            raise ValueError(f"SNIPER_PRIMARY_FEED={self.s.primary_feed} is paper-only: the live canary "
                             f"triggers from Binance unless SNIPER_KRAKEN_LIVE=1 is set")
        self.trigger = Trigger(self.s.k_bp, self.s.window_ms)       # the primary feed's mid
        self.last_mid: float | None = None         # the Kraken mid last heard
        # Kraken feed quality (summary rows): arrival lag, gaps, the venue's own status.
        self.kraken_lag: collections.deque = collections.deque(maxlen=3000)
        self.kraken_gap_max = 0.0
        self.kraken_system: str | None = None
        self._kraken_prev_t: float | None = None   # None after a (re)connect: an outage is not a market gap
        # One trigger per second-trigger feed: each feed's own mid (Coinbase
        # BTC-USD and Binance BTCUSDT sit at different levels).
        self.triggers = {name: Trigger(self.s.k_bp, self.s.window_ms) for name in self.s.extra_feeds}
        self.fresh = BinanceFreshness(self.s.binance_stale_ms, self.s.binance_unknown_ms, self.s.binance_backlog_max)
        self.journal = Path(journal_path) if journal_path else None
        self.last_fire = -1e18
        self.busy = False
        self.rounds = collections.defaultdict(lambda: {"fills": 0, "UP": 0.0, "DOWN": 0.0})
        self._fak_pending: dict = {}        # accepted orders awaiting actual trade quantities
        self.n = collections.Counter()
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
        self.code = code_version()
        self._live_delays: list = []
        if live is None and self.s.delay_source == "live":
            self._load_live_delays()
        if live is not None:
            live.attach(self)

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
            with open(self.journal, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, separators=(",", ":"), default=str) + "\n")
        except OSError:
            self.n["journal errors"] += 1

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
        done = None
        while True:
            cond = getattr(self.hub, "condition_id", None)
            if cond and cond != done:
                try:
                    await asyncio.to_thread(fetch, cond)
                    done = cond
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

    async def _round_watch(self) -> None:
        while True:
            self.roll_rounds()
            await asyncio.sleep(0.5)

    def _spawn(self, coro) -> None:
        # Keep a reference: the loop holds tasks weakly, so an unreferenced
        # markout or shadow task can be collected before it finishes.
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # ---------------------------------------------------------------- ticks
    def on_tick(self, raw) -> None:
        """Binance spot bookTicker - the primary trigger. On the live socket the
        messages come wrapped by the combined stream ({"stream", "data"}), with
        btcusdt@trade events between them that only time the feed."""
        t = time.monotonic() * 1000.0
        t_pc = time.perf_counter_ns()
        try:
            d = _loads(raw)
            if "data" in d:
                d = d["data"]
                if d.get("e") == "trade":
                    self.fresh.note_trade(d.get("E"), time.time() * 1000.0, t / 1000.0)
                    return
            mid = (float(d["b"]) + float(d["a"])) / 2.0
        except (ValueError, KeyError, TypeError, AttributeError):
            self.n["binance parse errors"] += 1
            return
        if not (math.isfinite(mid) and mid > 0):
            self.n["binance parse errors"] += 1
            return
        self._signal("binance", self.trigger, mid, t, t_pc)

    def on_coinbase(self, raw) -> None:
        """Coinbase BTC-USD ticker - a second trigger (SNIPER_EXTRA_FEEDS)."""
        t = time.monotonic() * 1000.0
        t_pc = time.perf_counter_ns()
        try:
            d = _loads(raw)
            if d.get("type") != "ticker":
                return                                  # subscriptions ack, heartbeats
            mid = (float(d["best_bid"]) + float(d["best_ask"])) / 2.0
        except (ValueError, KeyError, TypeError, AttributeError):
            return
        trig = self.triggers.get("coinbase")
        if trig is not None:
            self._signal("coinbase", trig, mid, t, t_pc)

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

    def _signal(self, feed: str, trigger, mid: float, t: float, t_pc: int) -> None:
        """The rule on one feed's mid, then the gates every feed shares: one
        cooldown and one order in flight, so a move seen by two feeds is one
        order - fired by whichever saw it first. Binance's counters keep their
        old names; a second feed's are suffixed with its name."""
        direction, r = trigger.update(t, mid)
        if direction is None:
            return
        primary = feed == self.s.primary_feed          # its counters keep the names the reports read
        tag = "" if primary else f" ({feed})"
        self.n["signals" + ("" if primary else f" {feed}")] += 1
        if feed == "binance":
            why = self.fresh.why_not(t / 1000.0)
            if why is not None:
                # The move was measured on ticks that cannot be shown current
                # (a backlog compresses seconds of movement into milliseconds):
                # drop the reference too, so the rule restarts on fresh ticks only.
                trigger.q.clear()
                side = "UP" if direction > 0 else "DOWN"
                self._skip(why, {"wall": time.time(), "id": f"{time.time():.3f}-{side}", "side": side,
                                 "bp": round(r, 3), "feed": feed, "binance_backlog": self.fresh.backlog,
                                 "binance_excess_ms": (round(self.fresh.last_excess, 1)
                                                       if self.fresh.last_excess is not None else None)})
                return
        if t - self.last_fire < self.s.cooldown_ms:
            self.n["skip: cooldown" + tag] += 1
            return
        if self.busy:
            self.n["skip: previous order in flight" + tag] += 1
            return
        self.busy = True
        task = self._fire_now("UP" if direction > 0 else "DOWN", r, t, t_pc, feed=feed)
        if task is not None:
            # A closed window, stale book or failed live preflight is not an
            # order. Only an eligible attempt consumes the shared cooldown.
            self.last_fire = t
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
        if self._live_delays:
            d = self._rng.choice(self._live_delays)
            return d, {"latency_mode": "live-sampled", "delay_ms": round(d, 1), "delay_n": len(self._live_delays)}
        measured = bool(self.net_rtt)
        net = sorted(self.net_rtt)[len(self.net_rtt) // 2] / 2.0 if measured else self.s.net_default_ms
        parts = {"latency_mode": "live-like", "net_ms": round(net, 1), "net_measured": measured,
                 "hold_ms": self.s.hold_ms, "server_ms": self.s.server_ms}
        return net + self.s.hold_ms + self.s.server_ms, parts

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
                self._clob["MarketOrderArgsV2"](token_id=str(token), amount=float(amount),
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
                    feed: str = "binance") -> None:
        """Decide, send and wait for the outcome - for callers that await one
        order. The tick handler uses _fire_now and does not wait."""
        task = self._fire_now(side, r_bp, t_ms, t_pc, feed=feed)
        if task is not None:
            await task

    def _fire_now(self, side: str, r_bp: float, t_ms: float, t_pc: int | None = None, feed: str = "binance"):
        """Decide on the spot and hand the order to the sender at once.

        Runs synchronously inside the BTC tick handler: every check, the size,
        the cap and the latency plan are settled here and the order goes to
        the worker thread (sign -> send) before the event loop runs anything
        else. Spawning a task first cost the loop's scheduling delay (~4ms
        median) before any of this started. Returns the task that records the
        outcome, or None when the signal was skipped.
        """
        start_pc = time.perf_counter_ns()
        t_pc = start_pc if t_pc is None else t_pc
        row = {"wall": time.time(), "id": f"{time.time():.3f}-{side}", "side": side, "bp": round(r_bp, 3),
               "feed": feed,
               # BTC tick received -> decision started: our own event loop's delay.
               "loop_lag_ms": round(time.monotonic() * 1000.0 - t_ms, 1)}
        try:
            plan = self._decide(side, row, t_ms)
            if plan is None:
                self.busy = False
                return None
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
            sign_ms, sign_err, signed = self._sign_now(plan)
            st["signed_pc"] = time.perf_counter_ns()
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
            if self.s.shadow_ms:
                # After the send: the shadow sleeps until absolute times anyway.
                self._spawn(self._shadow(row, plan["token"], plan["other"], plan["cond"],
                                         plan["amount"], plan["cap"], plan["t0"]))
            return self._spawn(self._finish(side, r_bp, row, plan, fut, t_pc, start_pc, st["sign_pc"]))
        except Exception as exc:  # noqa: BLE001 - a sniper fault must never stop the bot
            self.n["errors"] += 1
            row.update(result="error", reason=f"{type(exc).__name__}: {exc}")
            self._write(row)
            self.busy = False
            return None

    # ---------------------------------------------------------------- live canary
    def _fire_live(self, side, r_bp, row, plan, t_pc, start_pc):
        """LIVE CANARY: the decision above, then a real order. Sized exactly
        as paper sizes it, signed and written to the warm HTTP/2 connection
        here, in the tick handler - no event-loop turn before the bytes leave.
        The reply is awaited in the task spawned below. A refusal or
        a halted canary is journalled as a skip: nothing was sent."""
        from sniper_live import Halted, Refused
        try:
            p = self.live.prepare(plan, decided_pc=plan["decided_pc"])
        except (Refused, Halted) as exc:
            self.n["orders"] -= 1                       # decided, but not sent
            row["detail"] = str(exc)[:200]
            self._skip("live halted" if isinstance(exc, Halted) else "live refused", row)
            self.busy = False
            return None
        # The bytes go out here, still inside the BTC tick's own step: no
        # event-loop turn between the decision and the wire (None: no warm
        # HTTP/2 connection - the task below then connects and sends).
        started = self.live.start_send(p)
        task = self._spawn(self._finish_live(side, r_bp, row, plan, p, t_pc, started))
        if self.s.shadow_ms:
            self._spawn(self._shadow(row, plan["token"], plan["other"], plan["cond"],
                                     plan["amount"], plan["cap"], plan["t0"]))
        return task

    async def _finish_live(self, side, r_bp, row, plan, p, t_pc, started=None) -> None:
        """Send, then journal the venue's answer. Fills count towards the
        round caps at once (a pending order counts too: it may fill); the
        ledger books them only when the private channel confirms."""
        from sniper_live import MATCHED, NO_MATCH, PENDING
        end, rd, token = plan["end"], plan["rd"], plan["token"]
        try:
            res = await self.live.send(p, started)
            kind = res["kind"]
            ts = res.get("ts_us") or {}                 # microseconds after the decision
            est = getattr(p, "estimate", None) or {}
            row.update(live=True, kind=kind, outcome=self.LIVE_OUTCOME.get(kind, kind),
                       order_id=res.get("order_id"), venue_status=res.get("venue_status"),
                       http_status=res.get("http_status"), error=res.get("error"),
                       new_connection=res.get("new_connection"), amount=round(p.amount, 4), cap=p.cap,
                       price_signed=p.price, order_type=p.order_type,
                       client_order_id=p.client_order_id, estimate=est or None,
                       live_ts_us=ts, t_tick_to_decided_ms=round((plan["decided_pc"] - t_pc) / 1e6, 2))
            if "net_send_start" in ts:
                row["t_decided_to_wire_ms"] = round(ts["net_send_start"] / 1000.0, 2)
                if "response" in ts:
                    row["t_wire_to_reply_ms"] = round((ts["response"] - ts["net_send_start"]) / 1000.0, 2)
            if p.order_type == "FAK" and kind in (MATCHED, PENDING):
                rd["fills"] += 1           # reserve one slot, even before the quantity is known
                fill = res.get("fill")
                if fill is not None:
                    self._record_fak_fill(row, plan, fill)
                else:
                    self.n["pending"] += 1
                    row.update(result="pending", shares=None, price=None, fee=None, cost=None,
                               reason="accepted; waiting for complete private trade quantities")
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
                self._write(row)
                self._table_row(side, p.amount, "filled", end)
                self.on_event("sniper", f"LIVE FILL {side} {shares:.1f} @ {float(price or 0):.3f} "
                                        f"(BTC {r_bp:+.2f}bp)", "good")
                self._spawn(self._markout_live(row, token, res.get("order_id")))
            elif kind == PENDING:
                self.n["pending"] += 1
                row.update(result="pending", reason=res.get("error"), fee=est.get("fee"),
                           shares=round(shares, 4), price=est.get("price"))
                self._write(row)
                self._table_row(side, p.amount, "pending", end)
                self.on_event("sniper", f"LIVE order accepted, outcome pending ({res.get('venue_status')})", "info")
                self._spawn(self._markout_live(row, token, res.get("order_id")))
            else:
                self.n["refused"] += 1
                row.update(result="refused", reason=res.get("error") or kind)
                self._write(row)
                self._table_row(side, p.amount, "missed" if kind == NO_MATCH else "rejected", end)
                if kind != NO_MATCH:
                    self.on_event("sniper", f"LIVE order {kind}: {str(res.get('error'))[:120]}", "warn")
        except Exception as exc:  # noqa: BLE001 - a sniper fault must never stop the bot
            self.n["errors"] += 1
            row.update(result="error", reason=f"{type(exc).__name__}: {exc}")
            self._write(row)
        finally:
            self.busy = False

    def _record_fak_fill(self, row, plan, fill, *, update=False):
        """Report actual executed quantity, never the request or book estimate."""
        side, rd = row["side"], plan["rd"]
        shares = float(fill["shares"])
        row.update(shares=shares, price=fill.get("price"), cost=fill["cost"], fee=fill.get("fee", 0),
                   partial=fill.get("partial", False), price_source=fill["source"],
                   fill_statuses=fill.get("statuses", []), reason=None)
        if shares > 0:
            rd[side] += shares
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

    def _decide(self, side: str, row: dict, t_ms: float):
        """Every check and every field of the order; None (skip journalled) if it must not trade."""
        self._sync_live_fak()
        if self._fak_pending:
            return self._skip("awaiting FAK fill size", row)
        hub = self.hub
        up, down, cond, end = hub.up_token, hub.down_token, hub.condition_id, hub.window_end
        if not (up and down and cond and end):
            return self._skip("no current round", row)
        left = float(end) - time.time()
        row.update(round_end=end, left_s=round(left, 1))
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
        # The venue refuses a marketable BUY under $1, so under 10c the
        # 10-share clip becomes $1 - the smallest order live could place.
        amount = max(self.s.shares * ask, MIN_ORDER_USD)
        rd = self.rounds[end]
        if rd["fills"] >= self.s.max_fills_round or rd[side] + amount / ask > self.s.max_side_shares:
            return self._skip("round cap", row)
        decided_pc = time.perf_counter_ns()     # every check passed: this signal trades
        cap = min(0.99, round(ask + self.s.slip, 4))
        row.update(ask_seen=ask, cap=cap, shares=round(amount / ask, 4), amount=round(amount, 4))
        self.n["orders"] += 1
        lat, parts = self.latency_plan()
        row.update(latency_ms=round(lat, 1), **parts)
        tick = getattr(view, "tick_size", None)
        return {"end": end, "token": str(token), "amount": amount, "rd": rd, "cap": cap, "lat": lat, "view": view,
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
            fill = getattr(self.broker, "last_fill", None) or {}
            filled = bool(ok and fill.get("tag") == "sniper")
            row["outcome"] = self._outcome(filled, None if filled else (sign_err or getattr(self.broker, "last_error", None)),
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
                if woke is not None and row["outcome"] != "UNKNOWN_BOOK_STATE" and woke != filled:
                    row["old_rule"] = "false_miss" if filled else "false_fill"
                row["arrival_ms"] = arrival.get("arrival_ms")
            if filled:
                self.n["filled"] += 1
                rd["fills"] += 1
                rd[side] += float(fill.get("shares") or 0)
                row.update(result="filled", order_id=fill.get("order_id"), shares=fill.get("shares"),
                           price=fill.get("average_price"), fee=fill.get("fee"), cost=fill.get("total_cost"))
                if fill.get("partial"):
                    row.update(partial=True, stake=fill.get("stake"))
                if fill.get("matched_wall"):
                    # BTC tick -> the order met the book, all of our own delays included.
                    row["match_ms"] = round((float(fill["matched_wall"]) - row["wall"]) * 1000.0
                                            + row["loop_lag_ms"], 1)
                self._write(row)
                self._table_row(side, fill.get("total_cost"), "filled", end)
                self.on_event("sniper", f"FILL {side} {float(fill.get('shares') or 0):.1f} @ "
                                        f"{float(fill.get('average_price') or 0):.3f} (BTC {r_bp:+.2f}bp)", "good")
                self._spawn(self._markout(row, token))
            else:
                self.n["refused"] += 1
                reason = sign_err or getattr(self.broker, "last_error", None)
                row.update(result="refused", reason=reason)
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
            self._write(row)
        finally:
            self.busy = False

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
            out = {str(L): self._shadow_fill(views[L], token, spend, cap, rules, mids[L], late[L], alt)
                   for L in self.s.shadow_ms}
            self._write({"wall": time.time(), "result": "shadow", "id": ref.get("id"),
                         "side": ref.get("side"), "round_end": ref.get("round_end"),
                         "condition_id": cond, "token": token, "other_token": other, "L": out})
        except Exception as exc:  # noqa: BLE001 - diagnostics only, never the order path
            self.n["shadow errors"] += 1
            self._write({"wall": time.time(), "result": "shadow_error", "id": ref.get("id"),
                         "reason": f"{type(exc).__name__}: {exc}"[:200]})

    def _shadow_fill(self, view, token, spend, cap, rules, mid5, late_ms, alt_caps=None) -> dict:
        """The paper broker's own FOK arithmetic against one captured book:
        the real cap, plus each wider cap in alt_caps ({"3": cap, ...}) -
        would paying up to ask+3c/4c/6c have turned misses into good fills?"""
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

    async def _extra_feed(self, name: str) -> None:
        """A second-trigger feed's socket (SNIPER_EXTRA_FEEDS), reconnecting
        like the Binance one. Each (re)connect starts its rule afresh: a price
        from before a gap is no reference for "moved within 250ms"."""
        url, sub = EXTRA_FEEDS[name]
        handler = getattr(self, f"on_{name}")
        backoff = 1.0
        while True:
            try:
                from feeds.tls import ws_ssl
                async with websockets.connect(url, ssl=ws_ssl(url), ping_interval=20, ping_timeout=20,
                                              open_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    self.triggers[name] = Trigger(self.s.k_bp, self.s.window_ms)
                    backoff = 1.0
                    self.on_event("sniper", f"{name} ticker connected (second trigger)", "good")
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        handler(raw.decode() if isinstance(raw, bytes) else raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect; Binance keeps triggering meanwhile
                self.n[f"{name} reconnects"] += 1
                self.on_event("sniper", f"{name} ticker {type(exc).__name__}; reconnecting", "warn")
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 2)

    # ---------------------------------------------------------------- status
    def summary(self) -> dict:
        avg = lambda xs: round(sum(xs) / len(xs), 2) if xs else None
        done = self.n["filled"] + self.n["refused"]
        return {"signals": self.n["signals"], "orders": self.n["orders"], "filled": self.n["filled"],
                "refused": self.n["refused"], "fill_rate": round(self.n["filled"] / done, 3) if done else None,
                "markout_1s_c": avg(self.marks["1s"]), "markout_5s_c": avg(self.marks["5s"]),
                "skips": {k[6:]: v for k, v in self.n.items() if k.startswith("skip: ")},
                "fired_by_feed": {k[6:]: v for k, v in self.n.items() if k.startswith("fired ")},
                **({"signals_by_feed": {self.s.primary_feed: self.n["signals"],
                                        **{f: self.n[f"signals {f}"] for f in self.s.extra_feeds}}}
                   if self.s.extra_feeds else {}),
                **({"kraken": {"ticks": self.n["kraken ticks"], "heartbeats": self.n["kraken heartbeats"],
                               "malformed": self.n["kraken malformed"], "reconnects": self.n["reconnects"],
                               "gaps_over_max_gap": self.n["kraken gaps over max gap"],
                               "gap_ms_max": round(self.kraken_gap_max, 1),
                               "lag_ms": BinanceFreshness._q(self.kraken_lag), "system": self.kraken_system}}
                   if self.s.primary_feed == "kraken" else {}),
                "errors": self.n["errors"], "shadow_errors": self.n["shadow errors"],
                "latency_ms": round(self.latency_plan()[0], 1),
                "clob_rtt_ms": round(sorted(self.net_rtt)[len(self.net_rtt) // 2], 1) if self.net_rtt else None,
                "loop_lag_ms": self._lag_summary(),
                "binance": {**self.fresh.summary(), "reconnects": self.n["reconnects"],
                            "parse_errors": self.n["binance parse errors"]},
                "book_feed_age_ms": (self.hub.book.feed_age_summary()
                                     if hasattr(getattr(self.hub, "book", None), "feed_age_summary") else None),
                **({"pending": self.n["pending"], "live": self.live.status()} if self.live is not None else {})}

    def _lag_summary(self):
        xs = sorted(self.loop_lag)
        if not xs:
            return None
        q = lambda p: round(xs[min(len(xs) - 1, int(len(xs) * p / 100))], 2)
        return {"p50": q(50), "p90": q(90), "p99": q(99), "max": round(xs[-1], 2), "n": len(xs)}

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
                                              ping_timeout=20, open_timeout=10) as ws:
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
                            s = self.summary()
                            b = s.get("binance") or {}
                            ex = b.get("excess_ms") or {}
                            self.on_event("sniper", f"orders {s['orders']} filled {s['filled']} "
                                                    f"refused {s['refused']} markout 5s {s['markout_5s_c']}c; "
                                                    f"binance {b.get('status')} excess p50/p99 "
                                                    f"{ex.get('p50')}/{ex.get('p99')}ms "
                                                    f"backlog max {b.get('backlog_max')}", "info")
                            self._write({"wall": time.time(), "result": "summary", **s})
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
                            s = self.summary()
                            k = s.get("kraken") or {}
                            lag = k.get("lag_ms") or {}
                            self.on_event("sniper", f"orders {s['orders']} filled {s['filled']} "
                                                    f"refused {s['refused']} markout 5s {s['markout_5s_c']}c; "
                                                    f"kraken {k.get('ticks')} updates, lag p50/p99 "
                                                    f"{lag.get('p50')}/{lag.get('p99')}ms, longest gap "
                                                    f"{k.get('gap_ms_max')}ms", "info")
                            self._write({"wall": time.time(), "result": "summary", **s})
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
