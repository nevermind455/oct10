#!/usr/bin/env python3
"""Fold a ledger's journal into its snapshot (rollback helper, no orders).

    python ledger_compact.py live_ledger.json

The repaired ledger journals order authorizations ({"op": "authorize"}),
which code from before the 2026-10-14 repair does not know: it refuses to
start on such a ledger ("exists but is unreadable or invalid"), losing
nothing. A clean stop of the repaired bot already leaves an empty journal;
after a crash or kill, run this with the REPAIRED code before switching back.

It takes the same per-ledger lock the bot takes, so it refuses while a bot
runs on that ledger. Prints counts only.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


def compact(ledger_path: str) -> dict:
    import run
    from accounting.ledger import Ledger
    path = Path(ledger_path).resolve()
    if not path.exists():
        raise SystemExit(f"no ledger at {path}")
    lock = run._ProcessLock(Path(str(path) + ".lock"))
    try:
        lock.acquire()
    except RuntimeError:
        raise SystemExit(f"a bot process holds {path.name}: stop it first") from None
    try:
        led = Ledger(path=str(path), category="crypto")
        if not led.load():
            raise SystemExit(f"ledger did not load: {led.last_persistence_error}")
        pending = led.journal_pending()
        if pending and not led.save():
            raise SystemExit(f"snapshot failed: {led.last_persistence_error}")
        with led._lock:
            out = {"journal_entries_folded": pending, "positions": len(led.positions),
                   "lots": sum(len(p.lots) for p in led.positions.values()),
                   "authorized_orders": len(led.authorized_orders), "journal_left": led.journal_pending()}
        return out
    finally:
        lock.release()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    print(compact(sys.argv[1]))
