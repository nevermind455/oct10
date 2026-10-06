"""Pooled HTTP for venue reads.

Every venue call used to go through ``requests.get``, which builds a fresh
connection and throws it away again. Measured against the live CLOB on this
machine's link that cost 415ms per request versus 95ms once pooled - roughly
320ms of pure TCP+TLS handshake on every single call, and one more independent
chance to time out or be reset. At thirteen calls per six-second trade cycle
that was 5.4s of the cycle spent shaking hands.

Sessions are per-thread. The bot issues these reads from ``asyncio.to_thread``
workers, and ``requests.Session`` is not documented as thread-safe; one session
per worker keeps the connection reuse without sharing mutable state. Sequential
``to_thread`` calls reuse the same worker, so the trade cycle keeps one warm
connection rather than paying a handshake per read.

Callers go through the module-level ``get`` so there is exactly one place to
stub in tests, and one place to change if pooling ever needs tuning.

Two things the stock adapter gets wrong on a long-running bot:

  Reset pooled connections.  urllib3 already re-opens a connection the peer
  closed cleanly, but a reset while a request is in flight surfaces as a read
  error, and ``requests`` mounts ``Retry(total=0)`` by default - so it reaches
  the caller as ``ConnectionError``.  Only ``orderbook`` and
  ``market_discovery`` retry; the clock check, the boundary-print recovery and
  the paper book read do not, and each of those losing a round to a single
  reset is a bug.  Measured against a socket that resets the second request:
  ``ConnectionError`` with the stock adapter, recovered in 15ms with one read
  retry.

  Scalar timeouts.  ``requests`` applies a scalar ``timeout`` to the connect
  phase and the read phase *separately*, so ``timeout=8`` permits 16s.  This
  module splits it, which both bounds the worst case and stops a dead route
  from consuming a whole trading window before it fails.
"""
from __future__ import annotations

import socket
import threading

import requests
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

import config

_local = threading.local()

# Transport failures only.  Status codes are deliberately NOT retried here:
# the callers that care about 429/5xx already implement their own backoff and
# honour Retry-After, and retrying in both layers would multiply attempts
# inside a budget measured in single seconds.  GET/HEAD only - nothing that
# submits an order goes through this module (order entry uses httpx in
# polymarket_trade), so a replayed request cannot duplicate a trade.
_RETRY = Retry(
    total=config.HTTP_TRANSPORT_RETRIES,
    connect=config.HTTP_TRANSPORT_RETRIES,
    read=config.HTTP_TRANSPORT_RETRIES,
    status=0,
    redirect=0,
    other=0,
    allowed_methods=frozenset({"GET", "HEAD"}),
    backoff_factor=config.HTTP_RETRY_BACKOFF_SECONDS,
    raise_on_status=False,
)


def _socket_options():
    """TCP_NODELAY plus keepalive, skipping anything this platform lacks.

    Without keepalive a pooled socket dropped by a NAT or load balancer during
    a quiet stretch still looks healthy, and the next read blocks for the full
    timeout instead of failing immediately.  The names below exist on Linux and
    on current Windows builds but are not guaranteed, so each is probed.
    """
    options = list(getattr(HTTPConnection, "default_socket_options", []))
    options.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
    for name, value in (
            ("TCP_KEEPIDLE", config.HTTP_KEEPALIVE_IDLE_SECONDS),
            ("TCP_KEEPINTVL", config.HTTP_KEEPALIVE_INTERVAL_SECONDS),
            ("TCP_KEEPCNT", 3),
    ):
        option = getattr(socket, name, None)
        if option is not None:
            options.append((socket.IPPROTO_TCP, option, int(value)))
    return options


def _adapter() -> HTTPAdapter:
    adapter = HTTPAdapter(
        max_retries=_RETRY,
        # One pool per venue host (CLOB, Gamma, Binance), a few
        # connections each. Sessions are per-thread, so this is per worker.
        pool_connections=8,
        pool_maxsize=4,
        pool_block=False,
    )
    # urllib3 reads socket options off the connection it builds, not off the
    # adapter, so they are handed to the pool manager rather than set here.
    adapter.poolmanager.connection_pool_kw["socket_options"] = _socket_options()
    return adapter


def _split_timeout(timeout):
    """Normalise a caller's timeout into an explicit ``(connect, read)`` pair.

    A scalar reaching ``requests`` is applied to both phases, so the caller's
    number silently doubles.  Callers passing an explicit pair are left alone.

    A caller's own scalar always wins: when it fits inside the configured
    connect floor both phases just get that number, unchanged from before.
    Above the floor, the budget is split into connect/read halves - never
    giving connect less than the floor, since callers such as orderbook.py
    (timeout=8.0) and market_discovery.py (timeout=10) are deliberately
    asking for more than it to tolerate a slow handshake (this module's own
    `warm()` has measured 2.3s-15.8s against the CLOB). Capping connect at
    the bare floor regardless of what the caller asked for reintroduces
    spurious ConnectTimeouts during exactly the cold-start conditions this
    function exists to tolerate.
    """
    if timeout is None:
        return (config.HTTP_CONNECT_TIMEOUT_SECONDS, None)
    if isinstance(timeout, (tuple, list)):
        return tuple(timeout)
    try:
        total = float(timeout)
    except (TypeError, ValueError):
        return timeout
    floor = config.HTTP_CONNECT_TIMEOUT_SECONDS
    if total <= floor:
        return (total, total)
    connect = max(floor, total / 2.0)
    read = max(total - connect, floor / 2.0)
    return (connect, read)


def session() -> requests.Session:
    """The calling thread's session, created on first use."""
    existing = getattr(_local, "session", None)
    if existing is not None:
        return existing
    created = requests.Session()
    created.mount("https://", _adapter())
    created.mount("http://", _adapter())
    _local.session = created
    return created


def get(url, **kwargs):
    """GET through this thread's pooled connection.

    Still a thin passthrough - same arguments and same exceptions as
    ``requests.get`` - except that a scalar ``timeout`` is split into its
    connect and read halves so it means what the caller wrote.
    """
    kwargs["timeout"] = _split_timeout(kwargs.get("timeout"))
    return session().get(url, **kwargs)


def warm(*urls, timeout=None) -> int:
    """Open this thread's connection to each host before it is needed.

    The first read of a run pays DNS plus TCP plus TLS - measured on this
    machine's link at between 2.3s and 15.8s against the CLOB. Paying it at
    startup keeps it out of the first trade cycle, where the freshness budget
    is three seconds. Best effort by contract: a venue that is down at startup
    is the caller's problem to report, not this function's to raise.
    """
    opened = 0
    for url in urls:
        if not url:
            continue
        try:
            get(url, timeout=timeout or config.HTTP_CONNECT_TIMEOUT_SECONDS)
            opened += 1
        except Exception:
            continue
    return opened


def close() -> None:
    """Drop this thread's session. Only needed by tests and shutdown paths."""
    existing = getattr(_local, "session", None)
    if existing is not None:
        try:
            existing.close()
        finally:
            _local.session = None


# ---------------------------------------------------------------- dns check ---
# Diagnostic only. Nothing here changes how a request is made or resolved: it
# reports what the OS resolver is answering so a redirect is named once at
# startup instead of arriving as a stream of SSLError/ConnectTimeout that
# reads like a venue outage or a slow link.
_DNS_HOSTS = ("clob.polymarket.com", "gamma-api.polymarket.com")


def dns_redirect_report(hosts=_DNS_HOSTS, timeout: float = 4.0):
    """Is the OS resolver answering these hosts with a single decoy address?

    Returns (status, detail) where status is "ok", "redirected" or "unknown".

    Separate services do not share one address, so every host collapsing to a
    single IP is the signal. A DoH lookup then names the real address when it
    is reachable - best effort, because a machine with no route to it is
    exactly the machine that cannot check. Never raises: a diagnostic that
    breaks startup is worse than the ambiguity it reports.
    """
    import json
    import socket
    import urllib.request

    seen = {}
    for host in hosts:
        try:
            seen[host] = socket.gethostbyname(host)
        except Exception as exc:
            return "unknown", f"{host} did not resolve ({type(exc).__name__})"

    addresses = set(seen.values())
    if len(seen) > 1 and len(addresses) > 1:
        return "ok", ", ".join(f"{h}={ip}" for h, ip in seen.items())

    collapsed = next(iter(addresses))
    real = []
    try:
        req = urllib.request.Request(
            f"https://dns.google/resolve?name={hosts[0]}&type=A",
            headers={"accept": "application/dns-json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.load(resp)
        real = [a["data"] for a in payload.get("Answer", ())
                if a.get("type") == 1]
    except Exception:
        real = []

    if real and collapsed not in real:
        return "redirected", (
            f"every Polymarket host resolves to {collapsed}; "
            f"public DNS says {real[0]}. The OS resolver is answering with a "
            f"decoy, so venue calls fail TLS while a browser using "
            f"DNS-over-HTTPS still works")
    if real and collapsed in real:
        return "ok", f"{collapsed} matches public DNS"
    return "unknown", (
        f"every Polymarket host resolves to {collapsed}; could not reach "
        f"public DNS to confirm")
