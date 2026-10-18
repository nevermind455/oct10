"""Public trade prints and market-feed coverage, persisted - MEASUREMENT ONLY.

The market channel's last_trade_price prints were kept only in memory (a
bounded deque per token, dropped when the round's tokens rotate), so no
diagnostic archive ever held them. This log keeps every print the socket
delivered - accepted or refused, with the reason - and the feed's connect /
disconnect / subscription events, so liquidity attribution can be checked
offline against what was really observed.

The receive path only appends a tuple to a bounded deque (no I/O, no JSON);
a daemon thread writes JSON lines once a second to one file per UTC day,
market_prints-YYYYMMDD.jsonl, and deletes files older than keep_days. A full
queue (20,000 records, ~0.9 KB each) drops the new record and counts it - never
blocks, never raises into the feed. Measured 2026-10-18: +2 us per print on the
receive path; the writer did 20,000 lines in ~0.1 s. Nothing here can change
an order.

Each line: {"v": 1, "seq": n, "t": kind, "wall_ns": ..., "mono_ns": ..., ...}
  kind "trade":      asset_id, market, price, size, side, fee_rate_bps, venue_ms
                     (the raw strings as received), conn, accepted, why
  kind "connect" / "disconnect": conn, tokens
  kind "subscribe":  conn, tokens
seq is this process's ingest order; conn counts the socket's connections.
"""
from __future__ import annotations

import collections
import itertools
import json
import threading
import time
from pathlib import Path

SCHEMA = 1


class PrintLog:
    def __init__(self, folder, *, max_queue: int = 20_000, flush_s: float = 1.0, keep_days: int = 3) -> None:
        self.folder = Path(folder)
        self.max_queue = int(max_queue)
        self.flush_s = float(flush_s)
        self.keep_days = int(keep_days)
        self._q: collections.deque = collections.deque()
        self._seq = itertools.count(1)
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.written = self.dropped = self.errors = 0
        self.max_depth = 0

    # ---------------------------------------------------------------- the receive path
    def record(self, kind: str, **fields) -> None:
        """Queue one record. O(1), no I/O; a full queue drops it (counted)."""
        depth = len(self._q)
        if depth >= self.max_queue:
            self.dropped += 1
            return
        if depth >= self.max_depth:
            self.max_depth = depth + 1
        self._q.append((next(self._seq), time.time_ns(), time.monotonic_ns(), kind, fields))

    # ---------------------------------------------------------------- the writer
    def start(self) -> "PrintLog":
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="print-log", daemon=True)
            self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.wait(self.flush_s):
            self.flush()
        self.flush()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.flush()

    def flush(self) -> int:
        """Write everything queued (any thread); returns the records written."""
        with self._write_lock:
            batch = []
            while self._q:
                try:
                    batch.append(self._q.popleft())
                except IndexError:
                    break
            if not batch:
                return 0
            by_day: dict[str, list[str]] = collections.defaultdict(list)
            for seq, wall_ns, mono_ns, kind, fields in batch:
                day = time.strftime("%Y%m%d", time.gmtime(wall_ns / 1e9))
                by_day[day].append(json.dumps({"v": SCHEMA, "seq": seq, "t": kind, "wall_ns": wall_ns,
                                               "mono_ns": mono_ns, **fields}, separators=(",", ":"), default=str))
            try:
                self.folder.mkdir(parents=True, exist_ok=True)
                for day, lines in by_day.items():
                    with open(self.folder / f"market_prints-{day}.jsonl", "a", encoding="utf-8") as fh:
                        fh.write("\n".join(lines) + "\n")
                self.written += len(batch)
                self._prune()
            except OSError:
                self.errors += 1
            return len(batch)

    def _prune(self) -> None:
        if self.keep_days <= 0:
            return
        cutoff = time.strftime("%Y%m%d", time.gmtime(time.time() - self.keep_days * 86400))
        for p in self.folder.glob("market_prints-*.jsonl"):
            day = p.stem.rsplit("-", 1)[-1]
            if day.isdigit() and day < cutoff:
                try:
                    p.unlink()
                except OSError:
                    self.errors += 1

    def stats(self) -> dict:
        return {"written": self.written, "dropped": self.dropped, "queued": len(self._q),
                "max_depth": self.max_depth, "errors": self.errors}
