"""Keep stray output off the dashboard.

While the alt screen is up, anything printed - a "[PAPER] FOK rejected"
line, a library warning, a traceback from a failing task - would land in
the middle of the frame, and the renderer only rewrites rows IT changed, so
the damage stays on screen. ConsoleCapture swaps stdout, stderr and the root
logging handler for one sink that turns every complete line into an event
(source, text, level) instead. Restored on exit, whatever happened.

Port of the stdout part of spe8's dashboard/probe.py; it reads nothing from
the bot and patches nothing but the three standard streams.
"""
from __future__ import annotations

import io
import logging
import re
import sys
import threading

from .safety import terminal_text

_TAG = re.compile(r"\[(?P<tag>[A-Za-z_ ]{1,20})\]\s*(?P<msg>.*)$")
_ET_PREFIX = re.compile(r"^\[[A-Za-z]{3} \d{2} \d{2}:\d{2}:\d{2} ET\]\s*")
_LEVELS = (("error", "bad"), ("fail", "bad"), ("traceback", "bad"), ("warn", "warn"),
           ("could not", "warn"), ("reconnect", "warn"), ("rejected", "warn"),
           ("connected", "good"), ("filled", "good"))


class EventSink(io.TextIOBase):
    """A text stream whose lines become events."""

    MAX_LINE = 8192

    def __init__(self, emit) -> None:
        super().__init__()
        self._emit_event = emit         # emit(source, text, level)
        self._buf = ""
        self._lock = threading.Lock()

    def writable(self) -> bool:
        return True

    def write(self, s: str) -> int:
        if not s:
            return 0
        if not isinstance(s, str):
            raise TypeError(f"write() argument must be str, not {type(s).__name__}")
        with self._lock:
            pieces = s.split("\n")
            for index, piece in enumerate(pieces):
                self._buf = (self._buf + piece)[: self.MAX_LINE]
                if index < len(pieces) - 1:
                    line, self._buf = self._buf, ""
                    self._line(line)
        return len(s)

    def flush(self) -> None:
        return None

    def finish(self) -> None:
        with self._lock:
            line, self._buf = self._buf, ""
        if line:
            self._line(line)

    def _line(self, line: str) -> None:
        raw = terminal_text(line.rstrip(), self.MAX_LINE)
        if not raw:
            return
        body = _ET_PREFIX.sub("", raw)
        m = _TAG.match(body)
        source, text = (m.group("tag").strip().lower(), m.group("msg")) if m else ("console", body)
        low = text.lower()
        level = next((lv for needle, lv in _LEVELS if needle in low), "info")
        try:
            self._emit_event(source, text, level)
        except Exception:
            pass            # an event sink that raised would print - the thing this prevents


class _SinkLogHandler(logging.Handler):
    """Log records into the sink. Never raises: logging would report it on stderr."""

    def __init__(self, sink: EventSink) -> None:
        super().__init__()
        self._sink = sink
        self.setFormatter(logging.Formatter("[log] %(name)s: %(message)s"))

    def emit(self, record) -> None:
        try:
            self._sink.write(self.format(record) + "\n")
        except Exception:
            pass

    def handleError(self, record) -> None:
        return


class ConsoleCapture:
    """with ConsoleCapture(on_event) as real_stdout: ... - real_stdout is the
    terminal the renderer must draw on."""

    def __init__(self, emit) -> None:
        self.sink = EventSink(emit)
        self._saved = None
        self._handler = None

    def __enter__(self):
        self._saved = (sys.stdout, sys.stderr)
        self._handler = _SinkLogHandler(self.sink)
        logging.getLogger().addHandler(self._handler)
        sys.stdout = self.sink
        sys.stderr = self.sink
        return self._saved[0]

    def __exit__(self, *exc):
        try:
            self.sink.finish()
        finally:
            if self._handler is not None:
                logging.getLogger().removeHandler(self._handler)
                self._handler = None
            if self._saved is not None:
                if sys.stdout is self.sink:
                    sys.stdout = self._saved[0]
                if sys.stderr is self.sink:
                    sys.stderr = self._saved[1]
                self._saved = None
        return False
