"""Load recorder output into analysis-ready arrays.

Two clocks are kept apart on purpose:
  recv    our own clock at the moment a message was received (monotonic
          perf_counter ns, mapped to wall ns). Simulated DECISIONS may only
          use state as of recv time.
  true    an estimate of real time, used to evaluate what an order arriving
          at a given moment would have met in the matching engine:
            Binance:    recv_wall + offset, offset from REST clock probes
                        (server_time - midpoint of the request)
            Polymarket: its event `timestamp` (ms), corrected by a residual
                        offset so that its fastest messages arrive after half
                        the websocket PING/PONG round trip.

Polymarket books are rebuilt per asset from snapshots (top 15 levels) and
level changes (levels within 5c of the best), then sampled after every event:
best bid/ask (from the server's own best fields when present), and the first
five ask and bid levels for walking an order.
"""
from __future__ import annotations

import collections
import glob
import gzip
import json
import os
import pickle
import zlib

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.environ.get("LAB_DATA", os.path.join(HERE, "data"))
NLEV = 5


def files(folder=DATA):
    """Recording files in time order (name = lab_<UTC hour>_<run start>)."""
    return sorted(glob.glob(os.path.join(folder, "lab_*.jsonl.gz")))


def _lines(path):
    """Lines of a gzip file; the file still being written ends mid-block, so
    stop quietly at its last complete line."""
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        try:
            for line in fh:
                if line.endswith("\n"):
                    yield line
        except (EOFError, OSError, zlib.error):
            return


class Book:
    __slots__ = ("bids", "asks", "bb", "ba")

    def __init__(self):
        self.bids, self.asks, self.bb, self.ba = {}, {}, None, None

    def snapshot(self, bids, asks):
        self.bids = {p: s for p, s in bids if s > 0}
        self.asks = {p: s for p, s in asks if s > 0}
        self.bb = max(self.bids) if self.bids else None
        self.ba = min(self.asks) if self.asks else None

    def change(self, price, size, side, bb, ba):
        d = self.bids if side == 0 else self.asks
        if size > 0:
            d[price] = size
        else:
            d.pop(price, None)
        # the server's own best prices win over our (partial-depth) rebuild
        self.bb = bb if bb is not None else (max(self.bids) if self.bids else None)
        self.ba = ba if ba is not None else (min(self.asks) if self.asks else None)
        if self.bb is not None:
            for p in [p for p in self.bids if p > self.bb + 1e-9]:
                del self.bids[p]
        if self.ba is not None:
            for p in [p for p in self.asks if p < self.ba - 1e-9]:
                del self.asks[p]

    def ladder(self):
        a = sorted((p, s) for p, s in self.asks.items() if self.ba is None or p >= self.ba - 1e-9)[:NLEV]
        b = sorted(((p, s) for p, s in self.bids.items() if self.bb is None or p <= self.bb + 1e-9), reverse=True)[:NLEV]
        return a, b


def load(paths=None, cache=True):
    paths = paths or files()
    key = os.path.join(DATA, "_cache_" + str(abs(hash(tuple((p, os.path.getsize(p)) for p in paths)))) + ".pkl")
    if cache and os.path.exists(key):
        return pickle.load(open(key, "rb"))
    bsb, bst, bfb, bft, rt = [], [], [], [], []
    probes = collections.defaultdict(list)
    rounds, aid_side, aid_asset = {}, {}, {}
    pm_rows = collections.defaultdict(list)       # idx -> [(recv_mono, recv_wall, ts, bb, ba, asks, bids)]
    pm_trades = []                                 # (recv_mono, recv_wall, ts, idx, price, size, side)
    books = collections.defaultdict(Book)
    mono0 = wall0 = None
    for path in paths:
        for line in _lines(path):
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                k, tm, tw = r[0], r[1], r[2]
                if mono0 is None:
                    mono0, wall0 = tm, tw
                if k == "bsb":
                    bsb.append((tm, tw, r[4], r[5], r[6], r[7]))
                elif k == "bfb":
                    bfb.append((tm, tw, r[3] or 0, r[4] or 0, r[6], r[7], r[8], r[9]))
                elif k == "bst":
                    bst.append((tm, tw, r[3] or 0, r[4] or 0, r[5], r[6], r[7]))
                elif k == "bft":
                    bft.append((tm, tw, r[3] or 0, r[4] or 0, r[5], r[6], r[7]))
                elif k == "aid":
                    aid_asset[r[4]] = r[3]
                    if r[5]:
                        aid_side[r[4]] = r[5]
                elif k == "round":
                    rounds[r[3]["window_start"]] = r[3]
                elif k == "pmb":
                    b = books[r[4]]
                    b.snapshot(r[5], r[6])
                    a, bi = b.ladder()
                    pm_rows[r[4]].append((tm, tw, r[3], b.bb, b.ba, a, bi))
                elif k == "pmc":
                    touched = {}
                    for idx, p, s, sd, bb, ba in r[4]:
                        books[idx].change(p, s, sd, bb, ba)
                        touched[idx] = True
                    for idx in touched:
                        b = books[idx]
                        a, bi = b.ladder()
                        pm_rows[idx].append((tm, tw, r[3], b.bb, b.ba, a, bi))
                elif k == "pmt":
                    pm_trades.append((tm, tw, r[3], r[4], r[5], r[6], r[7]))
                elif k == "rt":
                    rt.append((tm, tw, r[3]))
                elif k in ("clk", "rtt", "lag", "lagsum", "gc", "handle", "dec", "conn", "ping", "pong", "sub",
                           "meta", "cpu"):
                    probes[k].append(r)
    # asset -> round / side
    tok_round = {}
    for w, info in rounds.items():
        tok_round[info["up_token_id"]] = (w, "Up")
        tok_round[info["down_token_id"]] = (w, "Down")
    idx_info = {i: tok_round.get(a) for i, a in aid_asset.items()}

    def arr(rows, n):
        a = np.array(rows, dtype=float).reshape(-1, n) if rows else np.zeros((0, n))
        return a[np.argsort(a[:, 0], kind="stable")]     # held burst rows are written late

    D = {"mono0": mono0, "wall0": wall0, "rounds": rounds, "idx_info": idx_info,
         "bsb": arr(bsb, 6), "bst": arr(bst, 7), "bfb": arr(bfb, 8), "bft": arr(bft, 7),
         "probes": dict(probes), "rt_raw": rt, "pm_trades": pm_trades}
    pm = {}
    for idx, rows in pm_rows.items():
        n = len(rows)
        A = np.full((n, NLEV, 2), np.nan)
        B = np.full((n, NLEV, 2), np.nan)
        for j, row in enumerate(rows):
            for q, (p, s) in enumerate(row[5]):
                A[j, q] = (p, s)
            for q, (p, s) in enumerate(row[6]):
                B[j, q] = (p, s)
        head = np.array([(x[0], x[1], x[2] if isinstance(x[2], (int, float)) else np.nan,
                          np.nan if x[3] is None else x[3], np.nan if x[4] is None else x[4]) for x in rows], dtype=float)
        pm[idx] = {"recv_mono": head[:, 0], "recv_wall": head[:, 1], "ts": head[:, 2], "bb": head[:, 3],
                   "ba": head[:, 4], "asks": A, "bids": B}
    D["pm"] = pm
    if cache:
        pickle.dump(D, open(key, "wb"))
    return D


def clock(D):
    """Offsets (ns) that map our wall clock to Binance and Polymarket time."""
    clk = [r for r in D["probes"].get("clk", []) if r[4] is not None and r[5] is not None]
    off_b = []
    for r in clk:
        if r[3] != "bs":
            continue
        t0, srv_ms, t1 = r[2], r[4], r[5]
        off_b.append(srv_ms * 1e6 - (t0 + t1) / 2)
    off_b = float(np.median(off_b)) if off_b else 0.0
    # Polymarket: fastest one-way message ~ half the app-level PING/PONG RTT
    pings = [r[1] for r in D["probes"].get("ping", []) if r[3] == "pm"]
    pongs = [r[1] for r in D["probes"].get("pong", [])]
    rtts = []
    j = 0
    for p in pings:
        while j < len(pongs) and pongs[j] < p:
            j += 1
        if j < len(pongs) and pongs[j] - p < 5e9:
            rtts.append(pongs[j] - p)
    half_rtt = float(np.percentile(rtts, 5)) / 2 if rtts else 0.0
    lat = []
    for v in D["pm"].values():
        ok = ~np.isnan(v["ts"])
        lat.append(v["recv_wall"][ok] + off_b - v["ts"][ok] * 1e6)
    lat = np.concatenate(lat) if lat else np.array([0.0])
    resid = float(np.percentile(lat, 0.5)) - half_rtt if len(lat) else 0.0
    return {"binance_offset_ns": off_b, "pm_residual_ns": resid, "pm_half_rtt_ns": half_rtt,
            "pm_ping_rtt_ns": rtts}


# ---------------------------------------------------------------- streaming
# Multi-day recordings do not fit in memory, so the report reads them twice:
#   scan()           probes, clock probes and the round list only (small)
#   stream_rounds()  everything else, emitting one round at a time once the
#                    stream is AFTER_S past its end, with BEFORE_S of BTC
#                    history before its start; memory stays about one round.
PROBES = frozenset(("clk", "rtt", "lag", "lagsum", "gc", "handle", "dec", "conn", "ping", "pong", "sub", "meta", "cpu"))
BEFORE_S, AFTER_S = 65, 35

try:
    import orjson
    _loads = orjson.loads
except ImportError:
    _loads = json.loads


def _kind(line):
    return line[2:line.find('"', 2)]


def scan(paths=None):
    """Pass 1: probes and rounds, without parsing the market-data lines.
    Each file's result is cached under its size, so only new or still-growing
    files are read again."""
    paths = paths or files()
    probes, rounds = collections.defaultdict(list), {}
    cdir = os.path.join(DATA, "_scan")
    os.makedirs(cdir, exist_ok=True)
    for path in paths:
        key = os.path.join(cdir, f"{os.path.basename(path)}.{os.path.getsize(path)}.pkl")
        if os.path.exists(key):
            fp, fr = pickle.load(open(key, "rb"))
        else:
            fp, fr = collections.defaultdict(list), {}
            for line in _lines(path):
                k = _kind(line)
                if k == "round":
                    r = _loads(line)
                    fr[r[3]["window_start"]] = r[3]
                elif k in PROBES:
                    fp[k].append(_loads(line))
            fp = dict(fp)
            for old in os.listdir(cdir):                  # drop this file's outdated entries
                if old.startswith(os.path.basename(path) + "."):
                    os.remove(os.path.join(cdir, old))
            pickle.dump((fp, fr), open(key, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
        for k, v in fp.items():
            probes[k] += v
        rounds.update(fr)
    return dict(probes), rounds


def clock_from_probes(probes):
    """Binance offset (REST midpoint method) and the Polymarket PING/PONG RTTs."""
    off_b = [r[4] * 1e6 - (r[2] + r[5]) / 2 for r in probes.get("clk", [])
             if r[3] == "bs" and r[4] is not None and r[5] is not None]
    off_b = float(np.median(off_b)) if off_b else 0.0
    pings = [r[1] for r in probes.get("ping", []) if r[3] == "pm"]
    pongs = sorted(r[1] for r in probes.get("pong", []))
    rtts, j = [], 0
    for p in pings:
        while j < len(pongs) and pongs[j] < p:
            j += 1
        if j < len(pongs) and pongs[j] - p < 5e9:
            rtts.append(pongs[j] - p)
    half = float(np.percentile(rtts, 5)) / 2 if rtts else 0.0
    return {"binance_offset_ns": off_b, "pm_half_rtt_ns": half, "pm_ping_rtt_ns": rtts, "pm_residual_ns": 0.0}


def pm_residual(pm, base):
    """Shift Polymarket event time so its fastest messages arrive after half a PING RTT."""
    lat = [v["recv_wall"][~np.isnan(v["ts"])] + base["binance_offset_ns"] - v["ts"][~np.isnan(v["ts"])] * 1e6
           for v in pm.values()]
    lat = np.concatenate(lat) if lat else np.array([])
    return float(np.percentile(lat, 0.5)) - base["pm_half_rtt_ns"] if len(lat) else 0.0


def stream_rounds(paths, rounds, probes):
    """Yield (window_start, D) per round, D shaped like load()'s output but
    holding only that round's two books and the BTC rows around it."""
    tok_round = {}
    for w, info in rounds.items():
        tok_round[info["up_token_id"]] = (w, "Up")
        tok_round[info["down_token_id"]] = (w, "Down")
    idx_round, books, last = {}, {}, {}
    rows = collections.defaultdict(list)          # idx -> (tm, tw, ts, bb, ba, asks_flat)
    trades = collections.defaultdict(list)
    btc = {k: collections.deque() for k in ("bsb", "bst", "bfb", "bft", "xb")}
    rt = collections.deque()
    pending, ip = sorted(rounds), 0

    def push(idx, tm, tw, ts, b, asks_changed):
        st = last.get(idx)
        if asks_changed or st is None:
            lad = sorted((p, s) for p, s in b.asks.items() if b.ba is None or p >= b.ba - 1e-9)[:NLEV]
            asks = tuple(x for ps in lad for x in ps)
        else:
            asks = st[1][2]
        state = (b.bb, b.ba, asks)
        # an unchanged book is not re-stored, except once a second so a quiet
        # feed and a dead one stay distinguishable
        if st is not None and st[1] == state and tw - st[0] < 1e9:
            return
        last[idx] = (tw, state)
        rows[idx].append((tm, tw, ts, b.bb, b.ba, asks))

    def build(w):
        lo, hi = (w - BEFORE_S) * 1e9, (w + 300 + AFTER_S) * 1e9

        def arr(dq, n):
            a = np.array([x for x in dq if lo <= x[1] <= hi], dtype=float).reshape(-1, n)
            return a[np.argsort(a[:, 0], kind="stable")]
        mine = {i: s for i, (ww, s) in idx_round.items() if ww == w}
        pm, tr = {}, []
        for idx in mine:
            rr = rows.get(idx, [])
            n = len(rr)
            A = np.full((n, NLEV, 2), np.nan)
            for j, x in enumerate(rr):
                a = x[5]
                for q in range(len(a) // 2):
                    A[j, q] = (a[2 * q], a[2 * q + 1])
            head = np.array([(x[0], x[1], x[2] if isinstance(x[2], (int, float)) else np.nan,
                              np.nan if x[3] is None else x[3], np.nan if x[4] is None else x[4]) for x in rr],
                            dtype=float).reshape(-1, 5)
            pm[idx] = {"recv_mono": head[:, 0], "recv_wall": head[:, 1], "ts": head[:, 2], "bb": head[:, 3],
                       "ba": head[:, 4], "asks": A}
            tr += [(x[0], x[1], x[2], idx, x[3], x[4], x[5]) for x in trades.get(idx, [])]
        return {"rounds": {w: rounds[w]}, "idx_info": {i: (w, s) for i, s in mine.items()},
                "bsb": arr(btc["bsb"], 6), "bst": arr(btc["bst"], 7), "bfb": arr(btc["bfb"], 8),
                "bft": arr(btc["bft"], 7), "xb": arr(btc["xb"], 6),
                "probes": probes, "pm_trades": tr, "pm": pm,
                "rt_raw": [x for x in rt if lo <= x[1] <= hi]}

    def finish(w):
        for idx in [i for i, (ww, _) in idx_round.items() if ww == w]:
            for d in (idx_round, books, last, rows, trades):
                d.pop(idx, None)
        keep = ((pending[ip] if ip < len(pending) else w + 300) - BEFORE_S) * 1e9
        for dq in list(btc.values()) + [rt]:
            while dq and dq[0][1] < keep:
                dq.popleft()

    for path in paths:
        for line in _lines(path):
            k = _kind(line)
            if k in PROBES or k == "round":
                continue
            try:
                r = _loads(line)
            except ValueError:
                continue
            tm, tw = r[1], r[2]
            if k == "bsb":
                btc["bsb"].append((tm, tw, r[4], r[5], r[6], r[7]))
            elif k == "bfb":
                btc["bfb"].append((tm, tw, r[3] or 0, r[4] or 0, r[6], r[7], r[8], r[9]))
            elif k == "bst":
                btc["bst"].append((tm, tw, r[3] or 0, r[4] or 0, r[5], r[6], r[7]))
            elif k == "bft":
                btc["bft"].append((tm, tw, r[3] or 0, r[4] or 0, r[5], r[6], r[7]))
            elif k == "xb":
                btc["xb"].append((tm, tw, r[3], np.nan if r[4] is None else r[4], r[5], r[6]))
            elif k == "aid":
                if r[3] in tok_round:
                    idx_round[r[4]] = tok_round[r[3]]
            elif k == "pmb":
                if r[4] in idx_round:
                    b = books.setdefault(r[4], Book())
                    b.snapshot(r[5], r[6])
                    push(r[4], tm, tw, r[3], b, True)
            elif k == "pmc":
                touched = {}
                for idx, p, s, sd, bb, ba in r[4]:
                    if idx not in idx_round:
                        continue
                    b = books.setdefault(idx, Book())
                    old_ba = b.ba
                    b.change(p, s, sd, bb, ba)
                    touched[idx] = touched.get(idx, False) or sd == 1 or b.ba != old_ba
                for idx, ch in touched.items():
                    push(idx, tm, tw, r[3], books[idx], ch)
            elif k == "pmt":
                if r[4] in idx_round:
                    trades[r[4]].append((tm, tw, r[3], r[5], r[6], r[7]))
            elif k == "rt":
                rt.append((tm, tw, r[3]))
            while ip < len(pending) and tw > (pending[ip] + 300 + AFTER_S) * 1e9:
                w = pending[ip]
                ip += 1
                yield w, build(w), True
                finish(w)
    while ip < len(pending):                      # recording ended: the gap check drops unfinished rounds
        w = pending[ip]
        ip += 1
        yield w, build(w), False                  # incomplete: never cached
        finish(w)


# ---------------------------------------------------------------- round cache
# Rebuilding a round's books is most of a report's run time, and the report is
# run several times over the same recording (readout, freeze, validation,
# test). A complete round is saved once, rebuilt never again.
CACHE = os.path.join(DATA, "_rounds")


def file_hour(path):
    """UTC hour start (unix s) encoded in a recording file name."""
    import calendar
    import time as _t
    stem = os.path.basename(path).split("_")
    return calendar.timegm(_t.strptime(stem[1] + stem[2], "%Y%m%d%H"))


def cached_rounds(paths, rounds, probes):
    """Yield (w, D, complete) for every round: cached ones from disk, the rest
    streamed from only the files that can hold them, and saved as they finish."""
    os.makedirs(CACHE, exist_ok=True)
    have = {int(f[:-4]) for f in os.listdir(CACHE) if f.endswith(".pkl")}
    for w in sorted(set(rounds) & have):
        try:
            D = pickle.load(open(os.path.join(CACHE, f"{w}.pkl"), "rb"))
        except Exception:  # noqa: BLE001 - a damaged cache entry is simply rebuilt
            have.discard(w)
            continue
        D["probes"] = probes
        yield w, D, True
    todo = {w: rounds[w] for w in rounds if w not in have}
    if not todo:
        return
    lo, hi = min(todo) - 3600, max(todo) + 3600
    need = [p for p in paths if lo <= file_hour(p) <= hi]
    for w, D, complete in stream_rounds(need, todo, probes):
        if complete:
            keep = {k: v for k, v in D.items() if k != "probes"}
            tmp = os.path.join(CACHE, f"{w}.tmp")
            pickle.dump(keep, open(tmp, "wb"), protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(tmp, os.path.join(CACHE, f"{w}.pkl"))
        yield w, D, complete


def to_true(recv_wall, C):
    """Our wall clock -> Binance/real time (ns)."""
    return recv_wall + C["binance_offset_ns"]


def pm_true(ts_ms, C):
    """Polymarket event timestamp (ms) -> real time (ns) on the same scale."""
    return ts_ms * 1e6 + C["pm_residual_ns"]
