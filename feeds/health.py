"""Feed status vocabulary and metrics.

Every duration here is measured with time.monotonic(). Wall clock is used
only for display, never for freshness, watchdog or backoff decisions - a
clock step (NTP, VM resume, DST) must not make a stale feed look fresh.
"""
from __future__ import annotations

import threading
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------- statuses --
LIVE = "LIVE"                  # connected, subscribed, receiving fresh data
STALE = "STALE"                # connected but no fresh message inside the window
UNSYNCED = "UNSYNCED"          # connected, but local state cannot be trusted
                               # (post-reconnect gap: CLOB never replays deltas)
DISCONNECTED = "DISCONNECTED"  # socket down / never established

ORDER = {LIVE: 0, STALE: 1, UNSYNCED: 2, DISCONNECTED: 3}


def worst(*statuses: str) -> str:
    return max((s for s in statuses if s), key=lambda s: ORDER.get(s, 3), default=DISCONNECTED)


@dataclass
class FeedHealth:
    """The contract every feed exposes."""
    name: str
    status: str = DISCONNECTED
    connected_at: float | None = None       # monotonic
    last_message_mono: float | None = None
    last_error: str | None = None
    reconnect_count: int = 0
    messages: int = 0
    latency_ms: float | None = None         # ping/pong round trip
    subscribed: tuple[str, ...] = ()
    detail: str = ""

    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    # ------------------------------------------------------------- helpers
    def mark_message(self) -> None:
        with self._lock:
            self.last_message_mono = time.monotonic()
            self.messages += 1

    def mark_connected(self) -> None:
        with self._lock:
            self.connected_at = time.monotonic()
            self.last_error = None

    def mark_error(self, exc: BaseException | str) -> None:
        with self._lock:
            self.last_error = safe_log_text(exc, limit=200)

    def mark_reconnect(self) -> None:
        with self._lock:
            self.reconnect_count += 1

    @property
    def last_message_age_ms(self) -> float | None:
        with self._lock:
            m = self.last_message_mono
            return None if m is None else max(0.0, (time.monotonic() - m) * 1000.0)

    @property
    def uptime_s(self) -> float | None:
        with self._lock:
            c = self.connected_at
            return None if c is None else max(0.0, time.monotonic() - c)

    def as_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "status": self.status,
                "last_message_age_ms": self.last_message_age_ms,
                "latency_ms": self.latency_ms,
                "reconnect_count": self.reconnect_count,
                "last_error": self.last_error,
                "messages": self.messages,
                "subscribed": self.subscribed,
                "detail": self.detail,
                "uptime_s": self.uptime_s,
            }


# ----------------------------------------------------------------- redaction --
_SECRET_KEYS = ("secret", "passphrase", "apikey", "api_key", "private_key",
                "poly_private_key", "signature", "password", "token")


def redact(text: str) -> str:
    """Strip anything that looks like a credential out of a log line.

    Applied to every error string and every outbound log, so a stack trace or
    a server error echoing the subscribe frame cannot leak L2 creds.
    """
    if not text:
        return text
    out = text
    low = out.lower()
    for key in _SECRET_KEYS:
        start = 0
        while True:
            i = low.find(key, start)
            if i < 0:
                break
            j = i + len(key)
            while j < len(out) and out[j] in " \"':=,":
                j += 1
            k = j
            while k < len(out) and out[k] not in " \"',}\n\t":
                k += 1
            if k > j:
                out = out[:j] + "<redacted>" + out[k:]
                low = out.lower()
                start = j + len("<redacted>")
            else:
                start = j
    return out


def redact_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k.lower() in _SECRET_KEYS else redact_obj(v))
                for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(redact_obj(v) for v in obj)
    if isinstance(obj, str):
        return redact(obj)
    return obj


def safe_log_text(value: Any, *, limit: int = 500) -> str:
    """Return bounded, credential-redacted, terminal-safe diagnostic text.

    WebSocket fields are remote input.  Keeping raw control characters (most
    notably ESC) lets a venue error or malformed message rewrite the terminal,
    while keeping an unbounded field in the in-memory event ring is a trivial
    memory denial of service.  Diagnostics retain printable content and make
    truncation explicit.
    """
    text = redact(str(value or ""))
    cleaned = "".join(
        ch if ch.isprintable() and unicodedata.category(ch) != "Cf" else " "
        for ch in text
    )
    cleaned = " ".join(cleaned.split())
    if limit < 1:
        return ""
    if len(cleaned) <= limit:
        return cleaned
    suffix = "...<truncated>"
    return cleaned[:max(0, limit - len(suffix))] + suffix
