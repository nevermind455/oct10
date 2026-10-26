#!/usr/bin/env python3
"""Rebuild the order delays paper draws from, out of the live journal. Read-only on the bot.

    .venv/bin/python refresh_delays.py                    # live_sniper.jsonl, last 7 days
    .venv/bin/python refresh_delays.py --days 3
    .venv/bin/python refresh_delays.py --journal /opt/sniperbtc/live_sniper.jsonl --dry-run

Paper (SNIPER_DELAY_SOURCE=live) draws each order's flight time from real live replies. The
shipped sample, data/live_delays_2026-10-04.jsonl, is the canary of 2026-10-04..06 (p90 480ms,
p99 1,420ms); live replies on 2026-10-22 ran p90 966ms, p99 2,536ms. A faster sample makes paper
meet the book earlier than live does, so paper fills more and earns more than live would.

Writes data/live_delays_<UTC date>.jsonl in exactly the shipped format - each order's venue
answer (MATCHED / NO_MATCH), its wire -> reply ms, and the canary's warm round trips. No order,
trade or token ids, no prices, amounts, times or addresses: it may be committed (.gitignore
admits data/live_delays_*.jsonl only). The order rows are the ones sniper_mode.live_delays
reads (one row per order), the file is checked by reading it back the way paper does, and it
is refused below the MIN_LIVE_DELAYS orders paper needs. Then point paper at it in .env:

    SNIPER_DELAY_JOURNAL=data/live_delays_<UTC date>.jsonl
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MIN_LIVE_DELAYS = 30            # sniper_mode.MIN_LIVE_DELAYS: paper refuses a smaller sample


def is_order(row: dict) -> bool:
    """sniper_mode.live_delays' own test: one row per live order the engine answered."""
    return (row.get("live") is True and row.get("kind") in ("MATCHED", "NO_MATCH")
            and row.get("result") in ("filled", "refused", "pending"))


def _finite(value) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def select(lines, since: float) -> tuple[list[dict], dict]:
    """The sanitized rows of the journal at or after `since` (unix seconds), in journal order."""
    out, n = [], {"lines": 0, "malformed": 0, "orders": 0, "round_trips": 0, "undated": 0}
    for line in lines:
        n["lines"] += 1
        try:
            row = json.loads(line)
        except ValueError:
            n["malformed"] += 1
            continue
        if not isinstance(row, dict):
            n["malformed"] += 1
            continue
        wall = _finite(row.get("wall"))
        if is_order(row):
            ms = _finite(row.get("t_wire_to_reply_ms"))
            if ms is None or not 0 <= ms < 60_000:
                continue
            if wall is None:
                n["undated"] += 1
                continue
            if wall >= since:
                out.append({"live": True, "kind": row["kind"], "result": row["result"],
                            "t_wire_to_reply_ms": round(ms, 2)})
                n["orders"] += 1
        elif row.get("result") == "summary" and isinstance(row.get("live"), dict):
            clock = row["live"].get("clock")
            rtt = _finite(clock.get("rtt_ms_p50")) if isinstance(clock, dict) else None
            if rtt is not None and rtt > 0 and wall is not None and wall >= since:
                out.append({"result": "summary", "live": {"clock": {"rtt_ms_p50": round(rtt, 3)}}})
                n["round_trips"] += 1
    return out, n


def quantiles(xs) -> str:
    q = sorted(xs)
    if not q:
        return "no orders"
    pick = lambda f: q[min(len(q) - 1, int(len(q) * f))]  # noqa: E731 - the same pick paper prints
    return (f"{len(q)} orders: p50 {pick(0.5):.0f}ms, p90 {pick(0.9):.0f}ms, p99 {pick(0.99):.0f}ms, "
            f"max {q[-1]:.0f}ms")


def read_back(path: Path) -> tuple[list[float], float]:
    """The sample exactly as paper reads it (sniper_mode.live_delays)."""
    sys.path.insert(0, str(ROOT))
    from sniper_mode import live_delays
    return live_delays(path)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--journal", default=os.environ.get("LIVE_SNIPER_JOURNAL") or "live_sniper.jsonl",
                    help="the live sniper journal (default: LIVE_SNIPER_JOURNAL or live_sniper.jsonl)")
    ap.add_argument("--days", type=float, default=7.0, help="how many recent days of orders (default 7)")
    ap.add_argument("--out", default=None, help="default: data/live_delays_<UTC date>.jsonl")
    ap.add_argument("--current", default=os.environ.get("SNIPER_DELAY_JOURNAL")
                    or "data/live_delays_2026-10-04.jsonl", help="the sample paper uses now (for comparison)")
    ap.add_argument("--dry-run", action="store_true", help="print the comparison, write nothing")
    args = ap.parse_args(argv)
    if not (math.isfinite(args.days) and args.days > 0):
        print(f"--days must be a positive number of days, not {args.days!r}", file=sys.stderr)
        return 2
    journal = Path(args.journal)
    if not journal.is_absolute():
        journal = ROOT / journal
    try:
        with open(journal, encoding="utf-8") as fh:
            rows, n = select(fh, time.time() - args.days * 86400.0)
    except OSError as exc:
        print(f"cannot read the live journal {journal}: {exc}", file=sys.stderr)
        return 1
    print(f"{journal}: {n['lines']} lines, {n['orders']} orders with a venue reply in the last {args.days:g} days, "
          f"{n['round_trips']} round-trip summaries" + (f", {n['undated']} undated orders skipped" if n["undated"] else "")
          + (f", {n['malformed']} malformed lines skipped" if n["malformed"] else ""))
    if n["orders"] < MIN_LIVE_DELAYS:
        print(f"REFUSED: {n['orders']} orders is below the {MIN_LIVE_DELAYS} paper needs - widen --days",
              file=sys.stderr)
        return 1
    current = Path(args.current)
    if not current.is_absolute():
        current = ROOT / current
    try:
        old, old_one_way = read_back(current)
        print(f"current sample  {current.name}: {quantiles(old)} (one-way {old_one_way:.1f}ms off each reply)")
    except OSError:
        print(f"current sample  {current}: not readable")
    out = Path(args.out) if args.out else ROOT / "data" / f"live_delays_{dt.datetime.now(dt.timezone.utc):%Y-%m-%d}.jsonl"
    if not out.is_absolute():
        out = ROOT / out
    if args.dry_run:
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            probe = Path(tmp) / out.name
            probe.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows), encoding="utf-8")
            new, one_way = read_back(probe)
        print(f"refreshed (dry) {out.name}: {quantiles(new)} (one-way {one_way:.1f}ms) - nothing written")
        return 0
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        for r in rows:
            fh.write(json.dumps(r, separators=(",", ":")) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    new, one_way = read_back(tmp)
    if len(new) < MIN_LIVE_DELAYS:
        tmp.unlink(missing_ok=True)
        print(f"REFUSED: paper would read only {len(new)} delays from it (needs {MIN_LIVE_DELAYS})", file=sys.stderr)
        return 1
    if len(new) != n["orders"]:
        print(f"note: paper reads {len(new)} of the {n['orders']} orders (a reply shorter than the one-way "
              f"{one_way:.1f}ms is dropped)")
    os.replace(tmp, out)
    rel = out.relative_to(ROOT) if out.is_relative_to(ROOT) else out
    print(f"refreshed       {out.name}: {quantiles(new)} (one-way {one_way:.1f}ms off each reply)")
    print(f"\nwrote {out}\nput this in .env (paper reads it at start):\n    SNIPER_DELAY_JOURNAL={rel}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
