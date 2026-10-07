"""Latency lab recorder - PAPER / OBSERVATION ONLY.

Records every public market-data message that could carry the BTC -> Polymarket
repricing signal, each stamped the moment our process received it, so the
whole information path can be replayed offline at millisecond resolution.

It places no orders, reads no credentials, imports nothing from the trading
path except the read-only round discovery, and shares no state with a running
bot. It can run next to the bot or on its own.

Sources
  bs   Binance spot       btcusdt@trade + btcusdt@bookTicker
  bf   Binance USD-M perp btcusdt@bookTicker + btcusdt@trade
  pm   Polymarket CLOB market channel for the current and next round's
       Up/Down tokens (book, price_change, best_bid_ask, last_trade_price)
  rt   Polymarket RTDS: Chainlink 60s TWAP (the resolution source) and the
       Chainlink spot stream

Every record is a JSON array, one per line, gzip, rotated hourly (UTC):
  [kind, recv_mono_ns, recv_wall_ns, ...payload]
The writer thread compacts messages (the receive path never does):
  bsb/bfb  best bid/ask     [.., u | E,T,u, bid, bid_qty, ask, ask_qty]; size-only
           changes are kept at most every 50ms, price changes always
  bst/bft  trades           [.., T, E, price, qty, buyer_is_maker]
  aid      asset index      [.., asset_id, idx, side]
  pmb      book snapshot    [.., ts, idx, bids[:15], asks[:15]]
  pmc      level changes    [.., ts, [[idx, price, size, 0=bid|1=ask, best_bid, best_ask], ...]]
           (levels more than 5c from that side's best are dropped - no
           5-100 share order reaches them)
  pmt      last trade       [.., ts, idx, price, size, side]
  pmq      best bid/ask     [.., ts, idx, best_bid, best_ask]
recv_mono_ns is time.perf_counter_ns() (monotonic, never steps); recv_wall_ns
is time.time_ns(). Exchange timestamps stay inside the raw payloads - they are
for latency analysis only and never feed a simulated decision.

Also recorded: the event loop's own lag, GC pauses, per-message handling time,
websocket (re)connects, clock offsets to Binance, HTTPS round trips to the
CLOB (the path an order would take), and 'dec' timing probes that time a toy
signal->decision step inside the receive path.

    python -m latency_lab.recorder                 # until Ctrl+C
    python -m latency_lab.recorder --hours 24
    LAB_DATA=/path python -m latency_lab.recorder  # choose the data folder
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import gc
import gzip
import json
import os
import platform
import queue
import signal
import socket
import sys
import threading
import time

import requests
import websockets

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
import market_discovery  # noqa: E402  (read-only Gamma lookup of the round's token ids)
from latency_lab import feeds as XF  # noqa: E402  (other exchanges' top of book, parse only)

DATA_DIR = os.environ.get("LAB_DATA", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))

BS_URLS = ("wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@bookTicker",
           "wss://data-stream.binance.vision/stream?streams=btcusdt@trade/btcusdt@bookTicker")
# aggTrade on the perp delivered nothing in testing; the plain trade stream does.
BF_URLS = ("wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@trade",)
PM_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RT_URL = "wss://ws-live-data.polymarket.com"
CLOB = "https://clob.polymarket.com"

mono = time.perf_counter_ns
wall = time.time_ns

try:                      # 5-10x faster JSON; the recorder must not load the CPU it measures
    import orjson

    _loads = orjson.loads

    def _dumps(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:       # plain json still works, just costs more CPU
    _loads = json.loads

    def _dumps(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()


# ---------------------------------------------------------------- writer
class Writer(threading.Thread):
    """Owns the disk. The receive path only appends to a queue."""

    def __init__(self, folder: str) -> None:
        super().__init__(name="lab-writer", daemon=True)
        os.makedirs(folder, exist_ok=True)
        self.folder = folder
        self.q: queue.SimpleQueue = queue.SimpleQueue()
        self.stop = threading.Event()
        self.written = 0
        self._f = None
        self._hour = None
        # Every run writes its own files. A run that is killed leaves a gzip
        # stream without its end marker; appending a second run's stream to
        # that file would make everything after the break unreadable.
        self._run = time.strftime("%Y%m%dT%H%M%S", time.gmtime())

    def put(self, rec) -> None:
        self.q.put(rec)

    # ---------------------------------------------------------- compaction
    _aid: dict = {}
    _side: dict = {}
    _bt_last: dict = {}

    def _idx(self, asset, t_mono, t_wall, out):
        i = self._aid.get(asset)
        if i is None:
            i = self._aid[asset] = len(self._aid)
            out.append(["aid", t_mono, t_wall, asset, i, self._side.get(asset)])
        return i

    _bt_pending: dict = {}
    _x_last: dict = {}
    _x_bad = collections.Counter()

    def _xfeed(self, rec):
        """Another exchange's message -> ["xb", mono, wall, code, exch_ms, bid, ask],
        written only when its best bid or ask price changes."""
        t_mono, t_wall, name, raw = rec[1], rec[2], rec[3], rec[4]
        if raw in ("pong", "PONG"):       # plain-text keep-alive replies (OKX)
            return []
        try:
            got = XF.parse(name, _loads(raw) if isinstance(raw, (str, bytes)) else raw)
        except (ValueError, KeyError, TypeError, IndexError):
            self._x_bad[name] += 1        # a changed format must not flood the disk
            return [["xx", t_mono, t_wall, name, str(raw)[:300]]] if self._x_bad[name] <= 20 else []
        if got is None:
            return []
        ex, bid, ask = got
        if self._x_last.get(name) == (bid, ask):
            return []
        self._x_last[name] = (bid, ask)
        return [["xb", t_mono, t_wall, XF.CODE[name], ex, bid, ask]]

    def _bt(self, key, t_mono, full, out):
        """Best bid/ask: every price change is written; a size-only change is
        written at most every 50ms. Binance sends these in bursts, so a
        size-only update inside the 50ms window is held as 'pending' and the
        LAST one of the burst is written (with its own receive time) before
        the next row - the burst's final state is never lost."""
        prices = (full[-4], full[-2])
        last = self._bt_last.get(key)
        if last and last[1] == prices and t_mono - last[0] < 50_000_000:
            self._bt_pending[key] = full
            return
        pend = self._bt_pending.pop(key, None)
        if pend is not None:
            out.append(pend)
        out.append(full)
        self._bt_last[key] = (t_mono, prices)

    def compact(self, rec):
        kind, t_mono, t_wall = rec[0], rec[1], rec[2]
        if kind == "xq":
            return self._xfeed(rec)
        out = []
        if kind in ("bs", "bf"):
            d = rec[3]
            if isinstance(d, str):
                try:
                    d = _loads(d)
                except ValueError:
                    return [[kind + "x", t_mono, t_wall, rec[3]]]
            d = d.get("data") or {}
            try:
                if d.get("e") == "trade":
                    out.append([kind + "t", t_mono, t_wall, d.get("T"), d.get("E"),
                                float(d["p"]), float(d["q"]), 1 if d.get("m") else 0])
                elif "b" in d and "a" in d:
                    row = [float(d["b"]), float(d["B"]), float(d["a"]), float(d["A"])]
                    head = [d.get("u")] if kind == "bs" else [d.get("E"), d.get("T"), d.get("u")]
                    self._bt(kind, t_mono, [kind + "b", t_mono, t_wall] + head + row, out)
            except (KeyError, TypeError, ValueError):
                out.append([kind + "x", t_mono, t_wall, d])
            return out
        if kind == "pm":
            msg = rec[3]
            if msg in ("PONG", "pong"):
                return [["pong", t_mono, t_wall, "pm"]]
            if isinstance(msg, str):          # the receive path no longer parses Polymarket
                try:
                    msg = _loads(msg)
                except ValueError:
                    return [["pmx", t_mono, t_wall, rec[3]]]
            for m in (msg if isinstance(msg, list) else [msg]):
                if not isinstance(m, dict):
                    continue
                et, ts = m.get("event_type"), m.get("timestamp")
                ts = int(ts) if ts is not None and str(ts).isdigit() else ts
                try:
                    if et == "book":
                        i = self._idx(str(m["asset_id"]), t_mono, t_wall, out)
                        bids = sorted(((float(x["price"]), float(x["size"])) for x in m.get("bids") or ()),
                                      reverse=True)[:15]
                        asks = sorted((float(x["price"]), float(x["size"])) for x in m.get("asks") or ())[:15]
                        out.append(["pmb", t_mono, t_wall, ts, i, bids, asks])
                    elif et == "price_change":
                        rows = []
                        for c in m.get("price_changes") or ():
                            i = self._idx(str(c["asset_id"]), t_mono, t_wall, out)
                            pr, sd = float(c["price"]), 0 if c.get("side") == "BUY" else 1
                            bb = float(c["best_bid"]) if c.get("best_bid") not in (None, "") else None
                            ba = float(c["best_ask"]) if c.get("best_ask") not in (None, "") else None
                            ref = bb if sd == 0 else ba
                            if ref is not None and abs(pr - ref) > 0.0501:
                                continue
                            rows.append([i, pr, float(c["size"]), sd, bb, ba])
                        if rows:
                            out.append(["pmc", t_mono, t_wall, ts, rows])
                    elif et == "last_trade_price":
                        i = self._idx(str(m["asset_id"]), t_mono, t_wall, out)
                        out.append(["pmt", t_mono, t_wall, ts, i, float(m["price"]), float(m.get("size") or 0),
                                    m.get("side")])
                    elif et == "best_bid_ask":
                        i = self._idx(str(m["asset_id"]), t_mono, t_wall, out)
                        out.append(["pmq", t_mono, t_wall, ts, i, float(m["best_bid"]), float(m["best_ask"])])
                    else:
                        out.append(["pmx", t_mono, t_wall, m])
                except (KeyError, TypeError, ValueError):
                    out.append(["pmx", t_mono, t_wall, m])
            return out
        return [rec]

    def _open(self, wall_ns: int) -> None:
        hour = time.strftime("%Y%m%d_%H", time.gmtime(wall_ns / 1e9))
        if hour == self._hour:
            return
        if self._f:
            self._f.close()
        self._hour = hour
        path = os.path.join(self.folder, f"lab_{hour}_{self._run}.jsonl.gz")
        self._f = gzip.open(path, "wb", compresslevel=1)

    def run(self) -> None:
        last_flush = time.monotonic()
        while not (self.stop.is_set() and self.q.empty()):
            try:
                rec = self.q.get(timeout=0.5)
            except queue.Empty:
                rec = None
            if rec is not None:
                if rec[0] == "round":
                    self._side[rec[3]["up_token_id"]] = "Up"
                    self._side[rec[3]["down_token_id"]] = "Down"
                self._open(rec[2])
                for r in self.compact(rec):
                    self._f.write(_dumps(r) + b"\n")
                    self.written += 1
            if self._f and time.monotonic() - last_flush > 2.0:
                self._f.flush()
                last_flush = time.monotonic()
        if self._f:
            self._f.close()


# ---------------------------------------------------------------- state
class Lab:
    def __init__(self, writer: Writer) -> None:
        self.w = writer
        self.stop = asyncio.Event()
        self.counts = collections.Counter()
        self.handle_ns = collections.defaultdict(list)     # sampled handling cost per source
        # tiny in-process state for the timing probe only
        self.bs_mid = collections.deque()                   # (mono_ns, mid), last ~500ms only
        self.last_pm_mono = None                            # when the book last changed for us
        self.pm_top = {}                                    # asset -> (bid, ask, recv_mono)
        self.side_of = {}                                   # asset -> "Up"/"Down"
        self.last_probe = 0
        # subscriptions
        self.rounds = {}                                    # window_start -> discovery dict
        self.want_assets: set[str] = set()
        self.pm_ws = None
        self.pm_sent: set[str] = set()
        self.xfeeds: tuple = ()                             # other exchanges being recorded

    def rec(self, *fields) -> None:
        self.w.put(list(fields))

    # ------------------------------------------------------ hot paths
    def on_binance_spot(self, raw: str, t_mono: int, t_wall: int) -> None:
        self.counts["bs"] += 1
        # Timing probe: parse, update a mid, compute a 500ms return, decide.
        try:
            full = _loads(raw)
        except ValueError:
            self.rec("bs", t_mono, t_wall, raw)
            return
        self.rec("bs", t_mono, t_wall, full)
        d = full.get("data") or {}
        if d.get("e") == "trade":
            return
        b, a = d.get("b"), d.get("a")
        if b is None or a is None:
            return
        mid = (float(b) + float(a)) / 2.0
        t_sig = mono()
        # Keep exactly one price at or before t - 500ms plus everything newer:
        # the reference is O(1), not a scan of the whole history.
        q = self.bs_mid
        q.append((t_mono, mid))
        cut = t_mono - 500_000_000
        while len(q) >= 2 and q[1][0] <= cut:
            q.popleft()
        if q[0][0] > cut or t_mono - self.last_probe < 1_000_000_000:
            return
        r_bp = (mid / q[0][1] - 1.0) * 1e4
        if abs(r_bp) < 1.0:
            return
        side = "Up" if r_bp > 0 else "Down"
        t_dec = mono()
        self.last_probe = t_mono
        # book age = time since the last Polymarket message reached us
        self.rec("dec", t_mono, t_wall, t_sig, t_dec, side, round(r_bp, 3), None,
                 (t_dec - self.last_pm_mono) if self.last_pm_mono else None)

    def on_pm(self, raw: str, t_mono: int, t_wall: int) -> None:
        """Stamp and queue. Parsing happens in the writer thread."""
        self.counts["pm"] += 1
        self.last_pm_mono = t_mono
        self.rec("pm", t_mono, t_wall, raw)


# ---------------------------------------------------------------- sockets
async def ws_loop(lab: Lab, name: str, urls, on_open, on_raw, ping=None, ping_every=10.0):
    """Connect, pump raw frames to on_raw(raw, t_mono, t_wall), reconnect with backoff."""
    i, backoff = 0, 1.0
    while not lab.stop.is_set():
        url = urls[i % len(urls)]
        host = url.split("/")[2]
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=2,
                                          max_size=8 * 2 ** 20, open_timeout=10) as ws:
                lab.rec("conn", mono(), wall(), name, "open", host)
                backoff = 1.0
                await on_open(ws)
                pinger = asyncio.create_task(_app_ping(lab, name, ws, ping, ping_every)) if ping else None
                try:
                    while not lab.stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            lab.rec("conn", mono(), wall(), name, "silent30s", host)
                            break
                        t_mono, t_wall = mono(), wall()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", "replace")
                        on_raw(raw, t_mono, t_wall)
                        h = mono() - t_mono
                        if lab.counts[name] % 50 == 0:
                            lab.handle_ns[name].append(h)
                finally:
                    if pinger:
                        pinger.cancel()
        except Exception as exc:  # noqa: BLE001 - every failure is recorded and retried
            lab.rec("conn", mono(), wall(), name, "error", f"{type(exc).__name__}: {str(exc)[:160]}")
            i += 1
        if lab.stop.is_set():
            break
        lab.rec("conn", mono(), wall(), name, "closed", host)
        try:
            await asyncio.wait_for(lab.stop.wait(), timeout=backoff)
        except asyncio.TimeoutError:
            pass
        backoff = min(60.0, backoff * 2)


async def _app_ping(lab, name, ws, text, every):
    while True:
        await asyncio.sleep(every)
        lab.rec("ping", mono(), wall(), name)
        await ws.send(text)


async def run_binance_spot(lab: Lab):
    async def opened(ws):
        pass
    await ws_loop(lab, "bs", BS_URLS, opened, lab.on_binance_spot)


async def run_binance_perp(lab: Lab):
    async def opened(ws):
        pass

    def raw(r, tm, tw):
        lab.rec("bf", tm, tw, r)
        lab.counts["bf"] += 1
    await ws_loop(lab, "bf", BF_URLS, opened, raw)


async def run_xfeed(lab: Lab, name: str):
    """Another exchange's BTC top of book: stamped on receipt, parsed by the writer."""
    url, sub, ping, every = XF.SUBS[name]

    async def opened(ws):
        await ws.send(json.dumps(sub))

    def raw(r, tm, tw):
        lab.counts[name] += 1
        lab.rec("xq", tm, tw, name, r)
    await ws_loop(lab, name, (url,), opened, raw, ping=ping, ping_every=every or 10.0)


async def run_rtds(lab: Lab):
    async def opened(ws):
        subs = [{"topic": "crypto_prices_twap_sixty", "type": "update",
                 "filters": json.dumps({"symbol": "btc/usd"}, separators=(",", ":"))},
                {"topic": "crypto_prices_chainlink", "type": "*",
                 "filters": json.dumps({"symbol": "btc/usd"}, separators=(",", ":"))}]
        await ws.send(json.dumps({"action": "subscribe", "subscriptions": subs}))

    def raw(r, tm, tw):
        lab.rec("rt", tm, tw, r)
        lab.counts["rt"] += 1
    await ws_loop(lab, "rt", (RT_URL,), opened, raw, ping="PING", ping_every=5.0)


async def run_pm(lab: Lab):
    async def opened(ws):
        lab.pm_ws = ws
        lab.pm_sent = set()
        await sync_pm(lab, initial=True)

    try:
        await ws_loop(lab, "pm", (PM_URL,), opened, lab.on_pm, ping="PING", ping_every=10.0)
    finally:
        lab.pm_ws = None


async def sync_pm(lab: Lab, initial=False):
    ws = lab.pm_ws
    if ws is None:
        return
    want = set(lab.want_assets)
    if initial:
        if want:
            await ws.send(json.dumps({"assets_ids": sorted(want), "type": "market", "custom_feature_enabled": True}))
            lab.pm_sent = want
            lab.rec("sub", mono(), wall(), "init", sorted(want))
        return
    add, drop = sorted(want - lab.pm_sent), sorted(lab.pm_sent - want)
    if add:
        await ws.send(json.dumps({"assets_ids": add, "operation": "subscribe", "custom_feature_enabled": True}))
        lab.pm_sent |= set(add)
        lab.rec("sub", mono(), wall(), "add", add)
    if drop:
        await ws.send(json.dumps({"assets_ids": drop, "operation": "unsubscribe"}))
        lab.pm_sent -= set(drop)
        lab.rec("sub", mono(), wall(), "drop", drop)


async def rounds(lab: Lab):
    """Keep the current and the next round subscribed; drop a round 20s after it ends."""
    while not lab.stop.is_set():
        now = time.time()
        cur = int(now) - int(now) % 300
        for w in (cur, cur + 300):
            if w not in lab.rounds:
                info = await asyncio.to_thread(market_discovery.get_btc_5m_tokens, w)
                if info:
                    lab.rounds[w] = info
                    lab.side_of[info["up_token_id"]] = "Up"
                    lab.side_of[info["down_token_id"]] = "Down"
                    lab.rec("round", mono(), wall(), {k: info[k] for k in (
                        "window_start", "window_end", "slug", "condition_id", "up_token_id", "down_token_id")})
        want = set()
        for w, info in list(lab.rounds.items()):
            if w > cur and now < cur + 270:
                continue                      # next round: subscribe only in its last 30s
            if now > w + 300 + 20:
                del lab.rounds[w]
                lab.side_of.pop(info["up_token_id"], None)
                lab.side_of.pop(info["down_token_id"], None)
                for k in (info["up_token_id"], info["down_token_id"]):
                    lab.pm_top.pop(k, None)
                continue
            want |= {info["up_token_id"], info["down_token_id"]}
        lab.want_assets = want
        try:
            await sync_pm(lab)
        except Exception as exc:  # noqa: BLE001
            # The socket died under us; ws_loop reconnects and re-subscribes
            # everything on open, so stop poking the dead one.
            lab.pm_ws = None
            lab.rec("conn", mono(), wall(), "pm", "sub-error", f"{type(exc).__name__}")
        try:
            await asyncio.wait_for(lab.stop.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


# ---------------------------------------------------------------- probes
async def loop_lag(lab: Lab, period=0.02):
    """How late the event loop wakes up: any receive-path stall shows here."""
    lags = []
    t_report = mono()
    cpu0 = time.process_time()
    while not lab.stop.is_set():
        t0 = mono()
        await asyncio.sleep(period)
        lag = mono() - t0 - int(period * 1e9)
        lags.append(lag)
        if lag > 20_000_000:
            lab.rec("lag", t0, wall(), lag)
        if mono() - t_report > 10_000_000_000:
            lags.sort()
            n = len(lags)
            lab.rec("lagsum", mono(), wall(), n, lags[n // 2], lags[int(n * 0.9)], lags[int(n * 0.99)], lags[-1])
            hs = {k: sorted(v) for k, v in lab.handle_ns.items() if v}
            lab.rec("handle", mono(), wall(), {k: [len(v), v[len(v) // 2], v[int(len(v) * 0.99)], v[-1]] for k, v in hs.items()})
            lab.handle_ns.clear()
            # Who is using the CPU: this process's share of the window, and the
            # machine's 1-minute load (Linux) - a load above the core count
            # means everything on the box, the bot included, waits to run.
            cpu1 = time.process_time()
            span = (mono() - t_report) / 1e9
            try:
                load1 = os.getloadavg()[0]
            except (AttributeError, OSError):
                load1 = None
            lab.rec("cpu", mono(), wall(), round((cpu1 - cpu0) / span, 3), load1, os.cpu_count())
            cpu0 = cpu1
            lags, t_report = [], mono()


def gc_probe(lab: Lab):
    start = {}

    def cb(phase, info):
        if phase == "start":
            start["t"] = mono()
        elif "t" in start:
            lab.rec("gc", start["t"], wall(), info.get("generation"), mono() - start.pop("t"))
    gc.callbacks.append(cb)


def http_probes(lab: Lab, stop: threading.Event, every=15.0):
    """Clock offset to Binance (RTT/2 method) and HTTPS round trips to the CLOB.

    The CLOB /book call is the REST read the bot's decision path makes in its
    default book mode; its round trip is also our best order-path estimate
    that does not require sending an order."""
    sess = {h: requests.Session() for h in ("bs", "bf", "clob")}
    urls = {"bs": "https://api.binance.com/api/v3/time", "bf": "https://fapi.binance.com/fapi/v1/time"}
    while not stop.wait(every):
        for k, url in urls.items():
            try:
                t0, m0 = wall(), mono()
                r = sess[k].get(url, timeout=5)
                t1, m1 = wall(), mono()
                srv = r.json().get("serverTime")
                lab.rec("clk", m0, t0, k, srv, t1, m1 - m0)
            except Exception as exc:  # noqa: BLE001
                lab.rec("clk", mono(), wall(), k, None, None, f"{type(exc).__name__}")
        tok = next(iter(sorted(lab.want_assets)), None)
        for name, url, params in (("clob_time", f"{CLOB}/time", None),
                                  ("clob_book", f"{CLOB}/book", {"token_id": tok} if tok else None)):
            if name == "clob_book" and not tok:
                continue
            try:
                m0, t0 = mono(), wall()
                r = sess["clob"].get(url, params=params, timeout=5)
                lab.rec("rtt", m0, t0, name, mono() - m0, r.status_code)
            except Exception as exc:  # noqa: BLE001
                lab.rec("rtt", mono(), wall(), name, None, f"{type(exc).__name__}")


async def status(lab: Lab, every=60):
    prev = collections.Counter()
    while not lab.stop.is_set():
        try:
            await asyncio.wait_for(lab.stop.wait(), timeout=every)
        except asyncio.TimeoutError:
            pass
        rate = {k: (lab.counts[k] - prev[k]) / every for k in ("bs", "bf", "pm", "rt") + lab.xfeeds}
        prev = collections.Counter(lab.counts)
        print(time.strftime("%H:%M:%S"), "msgs/s", {k: round(v, 1) for k, v in rate.items()},
              "rounds", sorted(lab.rounds), "written", lab.w.written, flush=True)


# ---------------------------------------------------------------- main
async def main(hours: float | None, xfeeds: tuple = XF.NAMES):
    w = Writer(DATA_DIR)
    w.start()
    lab = Lab(w)
    lab.xfeeds = tuple(xfeeds)
    lab.rec("meta", mono(), wall(), {
        "host": socket.gethostname(), "platform": platform.platform(), "python": sys.version.split()[0],
        "websockets": websockets.__version__, "pid": os.getpid(), "clock": "perf_counter_ns + time_ns",
        "note": "paper/observation only - no orders, no credentials"})
    gc_probe(lab)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lab.stop.set)
        except (NotImplementedError, RuntimeError):
            pass                                  # Windows: Ctrl+C raises KeyboardInterrupt instead
    probe_stop = threading.Event()
    probe = threading.Thread(target=http_probes, args=(lab, probe_stop), daemon=True, name="lab-http")
    probe.start()
    tasks = [asyncio.create_task(c) for c in (rounds(lab), run_binance_spot(lab), run_binance_perp(lab),
                                              run_pm(lab), run_rtds(lab), loop_lag(lab), status(lab))]
    tasks += [asyncio.create_task(run_xfeed(lab, n)) for n in lab.xfeeds]
    try:
        if hours:
            try:
                await asyncio.wait_for(lab.stop.wait(), timeout=hours * 3600)
            except asyncio.TimeoutError:
                pass
        else:
            await lab.stop.wait()
    finally:
        lab.stop.set()
        probe_stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        w.stop.set()
        w.join(timeout=10)
        print("stopped; records written:", w.written, flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--hours", type=float, default=None)
    ap.add_argument("--feeds", default="all",
                    help=f"other exchanges to record: all, none, or a comma list of {','.join(XF.NAMES)}")
    args = ap.parse_args()
    xf = (XF.NAMES if args.feeds == "all" else () if args.feeds == "none"
          else tuple(n for n in args.feeds.split(",") if n in XF.NAMES))
    try:
        asyncio.run(main(args.hours, xf))
    except KeyboardInterrupt:
        pass
