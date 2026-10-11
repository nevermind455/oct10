"""A minimal HTTP/2 client for ONE origin, built for the live order path.

Why: httpx's stack (httpcore + anyio) hands control back to the event loop
4-5 times between `await client.post(...)` and the request headers reaching
the socket (anyio's lock checkpoints) - measured on a local HTTP/2 venue, and
an eager task saves only one of those turns. On the bot's busy loop every
turn lets queued work (Polymarket book messages, a dashboard frame) run
first. Here `post_now` encodes HEADERS+DATA and writes them to the transport
synchronously, inside the BTC tick handler: zero loop turns between the
decision and the bytes leaving.

Scope, deliberately small: one origin, one connection (HTTP/2 multiplexes),
TLS with certificate verification and ALPN h2 (or cleartext prior-knowledge
h2 for tests), request bodies that fit the initial flow-control window
(orders are ~1KB), no redirects, no cookies, no retries.

Interface used by sniper_live: `await get(url, params=, extensions=)` and
`await post(url, content=, headers=, extensions=, timeout=)` return a Resp
with status_code / text / json() / http_version, and emit httpcore-style
trace names so the existing send journal and outcome classification keep
working; `post_now(url, body, headers)` is the synchronous write and returns
a Stream whose `.response(timeout)` awaits the reply; `connected`,
`aclose()`, `is_closed`.

Outcome semantics the sender relies on:
  * NotConnected from post_now: nothing was written (the caller may send the
    slow way, which connects first and is flagged as a new connection).
  * NotProcessed: the server said it did not process the stream (GOAWAY
    with a lower last-stream-id, or RST_STREAM REFUSED_STREAM) - per RFC 9113
    safe to treat as never sent.
  * Any other failure after the write (reset, connection lost, timeout) means
    the venue may have the order: the caller must treat it as ambiguous.
"""
from __future__ import annotations

import asyncio
import json
import ssl
import time
from urllib.parse import urlencode, urlsplit

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions

_DROP = {"host", "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade", "content-length"}


class NotConnected(ConnectionError):
    """No live connection: nothing was written."""


class NotProcessed(ConnectionError):
    """The server said it did not process this stream (safe: never sent)."""


class Resp:
    http_version = "HTTP/2"

    def __init__(self, status: int, headers: list, body: bytes) -> None:
        self.status_code, self.headers, self.content = status, headers, body

    @property
    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")

    def json(self):
        return json.loads(self.content)


class Stream:
    __slots__ = ("sid", "future", "events", "status", "headers", "body", "abandon")

    def __init__(self, sid: int, future) -> None:
        self.sid, self.future = sid, future
        self.events: list = []                       # (httpcore-style name, perf_counter_ns)
        self.status, self.headers, self.body = None, [], bytearray()
        self.abandon = None

    async def response(self, timeout: float | None) -> Resp:
        try:
            if timeout is None:
                return await asyncio.shield(self.future)
            return await asyncio.wait_for(asyncio.shield(self.future), timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # A timeout says nothing about order acceptance. Only release the
            # transport's resources; the sender still records AMBIGUOUS.
            if self.abandon is not None:
                self.abandon(self)
            raise


class _Proto(asyncio.Protocol):
    def __init__(self, client: "FastH2") -> None:
        self.client = client
        self.conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True, header_encoding="utf-8",
                                             validate_inbound_headers=False))
        self.transport = None
        self.alive = False
        self.streams: dict[int, Stream] = {}

    # ---- connection
    def connection_made(self, transport) -> None:
        self.transport = transport
        self.conn.initiate_connection()
        self.conn.increment_flow_control_window(2 ** 24)       # responses never wait on our window
        transport.write(self.conn.data_to_send())
        self.alive = True

    def connection_lost(self, exc) -> None:
        self.alive = False
        self._fail_all(lambda s: ConnectionError(f"connection lost: {exc or 'closed'}"))
        if self.client._proto is self:
            self.client._proto = None

    def _fail_all(self, make_exc) -> None:
        for s in list(self.streams.values()):
            if not s.future.done():
                s.future.set_exception(make_exc(s))
                s.future.exception()       # observed: a caller that gave up (timeout, shutdown) logs nothing
        self.streams.clear()

    # ---- the synchronous write
    def request(self, method: str, path: str, headers: dict, body: bytes) -> Stream:
        if not self.alive or self.transport is None or self.transport.is_closing():
            raise NotConnected("no live HTTP/2 connection")
        conn = self.conn
        if conn.open_outbound_streams >= conn.remote_settings.max_concurrent_streams:
            raise NotProcessed("peer's concurrent stream limit reached; nothing written")
        sid = conn.get_next_available_stream_id()
        hdrs = [(":method", method), (":authority", self.client.authority), (":scheme", self.client.scheme),
                (":path", path)]
        hdrs += [(str(k).lower(), str(v)) for k, v in (headers or {}).items() if str(k).lower() not in _DROP]
        if body:
            # checked before anything is encoded: the stream does not exist
            # yet, so use the window it will open with
            if len(body) > min(conn.outbound_flow_control_window, conn.remote_settings.initial_window_size,
                               conn.max_outbound_frame_size):
                raise NotProcessed("request body exceeds available flow-control window / frame; nothing written")
            hdrs.append(("content-length", str(len(body))))
        s = Stream(sid, asyncio.get_running_loop().create_future())
        s.abandon = self._abandon
        self.streams[sid] = s
        try:
            conn.send_headers(sid, hdrs, end_stream=not body)
            if body:
                conn.send_data(sid, body, end_stream=True)
            encoded = conn.data_to_send()
        except Exception as exc:
            self.streams.pop(sid, None)
            s.future.cancel()
            # Encoding may have partially advanced h2 state. Never let those
            # buffered bytes escape on a later request.
            self.alive = False
            self._fail_all(lambda old: ConnectionError("HTTP/2 request encoding failed"))
            self.transport.close()
            raise NotProcessed("request encoding failed before write") from exc
        s.events.append(("http2.send_request_headers.started", time.perf_counter_ns()))
        try:
            self.transport.write(encoded)
        except Exception as exc:
            # write() may have handed bytes to the kernel before failing. Return
            # the failed stream so start() cannot turn this into a second POST.
            self.alive = False
            self._fail_all(lambda old: ConnectionError(f"transport write failed: {type(exc).__name__}"))
            self.transport.close()
            return s
        s.events.append(("http2.send_request_body.complete", time.perf_counter_ns()))
        return s

    def _abandon(self, s: Stream) -> None:
        if self.streams.pop(s.sid, None) is None:
            return
        if not s.future.done():
            s.future.cancel()
        s.body.clear()
        if self.alive and not self.transport.is_closing():
            try:
                self.conn.reset_stream(s.sid, error_code=h2.errors.ErrorCodes.CANCEL)
                self.transport.write(self.conn.data_to_send())
            except Exception:
                self.alive = False
                self._fail_all(lambda old: ConnectionError("HTTP/2 stream cleanup failed"))
                self.transport.close()

    # ---- replies
    def data_received(self, data: bytes) -> None:
        try:
            events = self.conn.receive_data(data)
        except h2.exceptions.ProtocolError as exc:
            self.alive = False
            msg = f"HTTP/2 protocol error: {exc}"
            self._fail_all(lambda s: ConnectionError(msg))
            self.transport.close()
            return
        for ev in events:
            s = self.streams.get(getattr(ev, "stream_id", None) or -1)
            if isinstance(ev, h2.events.ResponseReceived) and s is not None:
                s.events.append(("http2.receive_response_headers.complete", time.perf_counter_ns()))
                for k, v in ev.headers:
                    if k == ":status":
                        try:
                            status = int(v)
                            if not 100 <= status <= 599:
                                raise ValueError("invalid HTTP status")
                            s.status = status
                        except (TypeError, ValueError):
                            self.alive = False
                            self._fail_all(lambda old: ConnectionError("malformed HTTP/2 response status"))
                            self.transport.close()
                            return
                    else:
                        s.headers.append((k, v))
            elif isinstance(ev, h2.events.DataReceived):
                self.conn.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
                if s is not None:
                    s.body += ev.data
            elif isinstance(ev, h2.events.StreamEnded) and s is not None:
                self.streams.pop(ev.stream_id, None)
                if not s.future.done():
                    s.future.set_result(Resp(s.status or 0, s.headers, bytes(s.body)))
            elif isinstance(ev, h2.events.StreamReset) and s is not None:
                self.streams.pop(ev.stream_id, None)
                if not s.future.done():
                    refused = ev.error_code == h2.errors.ErrorCodes.REFUSED_STREAM
                    s.future.set_exception(NotProcessed("stream refused") if refused else
                                           ConnectionError(f"stream reset by the server ({ev.error_code!r})"))
                    s.future.exception()
            elif isinstance(ev, h2.events.ConnectionTerminated):
                self.alive = False                     # GOAWAY: no new streams on this connection
                last = ev.last_stream_id if ev.last_stream_id is not None else 2 ** 31
                for sid in [i for i in self.streams if i > last]:
                    st = self.streams.pop(sid)
                    if not st.future.done():
                        st.future.set_exception(NotProcessed(f"GOAWAY (last stream {last})"))
                        st.future.exception()
                # hyper-h2 has entered CLOSED; older streams cannot safely be
                # drained through it. Their outcome remains unknown.
                self._fail_all(lambda old: ConnectionError("GOAWAY after request write"))
                self.transport.close()
        out = self.conn.data_to_send()
        if out and self.transport is not None and not self.transport.is_closing():
            try:
                self.transport.write(out)
            except Exception as exc:
                self.alive = False
                self._fail_all(lambda old: ConnectionError(f"HTTP/2 control write failed: {type(exc).__name__}"))
                self.transport.close()


class FastH2:
    def __init__(self, origin: str, timeout_s: float = 3.0, connect_timeout_s: float = 10.0) -> None:
        u = urlsplit(origin)
        if u.scheme not in ("https", "http") or not u.hostname:
            raise ValueError(f"bad origin {origin!r}")
        self.scheme, self.host = u.scheme, u.hostname
        self.port = u.port or (443 if u.scheme == "https" else 80)
        default = (u.scheme == "https" and self.port == 443) or (u.scheme == "http" and self.port == 80)
        self.authority = self.host if default else f"{self.host}:{self.port}"
        self.timeout_s, self.connect_timeout_s = timeout_s, connect_timeout_s
        self._proto: _Proto | None = None
        self._lock: asyncio.Lock | None = None
        self._ssl_context: ssl.SSLContext | None = None
        self.connects = 0
        self.is_closed = False

    @property
    def connected(self) -> bool:
        return self._proto is not None and self._proto.alive and not self._proto.transport.is_closing()

    def _path(self, url: str, params=None) -> str:
        u = urlsplit(url)
        if u.netloc or u.scheme:
            effective_port = u.port or (443 if u.scheme == "https" else 80)
            if (u.scheme != self.scheme or u.hostname != self.host or effective_port != self.port
                    or u.username is not None or u.password is not None):
                raise ValueError("request URL is outside the configured HTTP/2 origin")
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        if params:
            path += ("&" if "?" in path else "?") + urlencode(params)
        return path

    async def _connect(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self.is_closed:
                raise RuntimeError("client closed")
            if self.connected:
                return
            old, self._proto = self._proto, None
            if old is not None and old.transport is not None:
                old.transport.close()
            loop = asyncio.get_running_loop()
            ctx = None
            if self.scheme == "https":
                if self._ssl_context is None:
                    # Loading Windows/system trust stores can block for tens
                    # of milliseconds. Do it once, away from the feed loop.
                    self._ssl_context = await asyncio.to_thread(ssl.create_default_context)
                    self._ssl_context.set_alpn_protocols(["h2"])
                ctx = self._ssl_context
            transport, proto = await asyncio.wait_for(
                loop.create_connection(lambda: _Proto(self), self.host, self.port, ssl=ctx,
                                       server_hostname=self.host if ctx else None),
                self.connect_timeout_s)
            if self.is_closed:
                transport.close()
                raise RuntimeError("client closed during connect")
            if ctx is not None:
                alpn = transport.get_extra_info("ssl_object").selected_alpn_protocol()
                if alpn != "h2":
                    transport.close()
                    raise ConnectionError(f"server did not agree to HTTP/2 (ALPN {alpn!r})")
            self._proto = proto
            self.connects += 1

    # ---- the order path: synchronous write
    def post_now(self, url: str, body: bytes, headers: dict) -> Stream:
        """Write a POST now, on the live connection; NotConnected (nothing
        written) if there is none."""
        if self.is_closed or not self.connected:
            raise NotConnected("no live HTTP/2 connection")
        return self._proto.request("POST", self._path(url), headers, body)

    # ---- async requests (clock refresh, keep-warm, the slow fallback)
    async def _request(self, method, url, *, params=None, headers=None, body=b"", extensions=None, timeout=None):
        if self.is_closed:
            raise RuntimeError("client closed")
        trace = (extensions or {}).get("trace")
        if not self.connected:
            if trace:
                await trace("connection.connect_tcp.started", {})
            await self._connect()
            if trace:
                await trace("connection.start_tls.complete", {})
        before_write = (extensions or {}).get("before_write")
        if before_write is not None:
            before_write()
        s = self._proto.request(method, self._path(url, params), headers or {}, body)
        sent_events = len(s.events)
        try:
            if trace:
                for name, _t in s.events:
                    await trace(name, {"timestamp_ns": _t})
            r = await s.response(self.timeout_s if timeout is None else timeout)
        except BaseException:
            # A tracing callback can fail after bytes have left the process,
            # before response() gets a chance to clean up a cancelled waiter.
            if s.abandon is not None:
                s.abandon(s)
            raise
        finally:
            if trace:
                for name, _t in s.events[sent_events:]:
                    await trace(name, {"timestamp_ns": _t})
        return r

    async def get(self, url, *, params=None, headers=None, extensions=None, timeout=None) -> Resp:
        return await self._request("GET", url, params=params, headers=headers, extensions=extensions,
                                   timeout=timeout)

    async def post(self, url, *, content=b"", headers=None, extensions=None, timeout=None) -> Resp:
        return await self._request("POST", url, headers=headers, body=content, extensions=extensions,
                                   timeout=timeout)

    async def aclose(self) -> None:
        self.is_closed = True
        p, self._proto = self._proto, None
        if p is not None and p.transport is not None:
            try:
                p.conn.close_connection()
                p.transport.write(p.conn.data_to_send())
            except Exception:  # noqa: BLE001 - closing anyway
                pass
            p.transport.close()
