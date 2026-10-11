"""Bounded retries for Windows atomic state-file replacement.

Virus scanners and sync clients can briefly hold the destination without
FILE_SHARE_DELETE. Retry only that filesystem operation, off the order path;
never retry a venue POST or weaken durability/authorization checks.
"""
import os
import time


def replace_atomic(source, destination) -> None:
    for attempt in range(4):
        try:
            os.replace(source, destination)
            return
        except OSError as exc:
            if getattr(exc, "winerror", None) not in (5, 32, 33) or attempt == 3:
                raise
            time.sleep(0.005 * (2 ** attempt))
