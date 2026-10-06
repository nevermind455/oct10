"""One verified TLS client context for every websocket the bot opens.

websockets builds a fresh context for each wss:// connect unless it is given
one, and building one loads the system CA bundle: ~80ms on the VPS, on the
event loop, at every reconnect. The stall watchdog put ssl.load_default_certs
on the loop's stack in 35 stalls in 22 hours. The shared context is exactly
what websockets would build (ssl.create_default_context(): certificate and
hostname verification on), built once.
"""
from __future__ import annotations

import ssl

_CTX: ssl.SSLContext | None = None


def ws_ssl(url: str):
    """The shared context for a wss:// URL; None for ws:// (local test servers)."""
    global _CTX
    if not str(url or "").lower().startswith("wss://"):
        return None
    if _CTX is None:
        _CTX = ssl.create_default_context()
    return _CTX
