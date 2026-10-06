"""Safety helpers for text that crosses the terminal/log boundary.

Dashboard text includes remote error messages and market metadata.  Treat it as
untrusted: terminal control sequences can clear the screen, forge status rows,
or place text in the clipboard, and exception strings can echo credentials.
"""
from __future__ import annotations

import re
import unicodedata
from functools import lru_cache
from typing import Any


_NAMED_SECRET = re.compile(
    r"(?i)\b(authorization|bearer|api[_-]?key|secret|passphrase|password|"
    r"private[_-]?key|poly[_-]?private[_-]?key|signature)\b"
    r"(\s*[:=]\s*|\s+)(['\"]?)([^\s,;'\"}\]]+)(\3)"
)
_URL_CREDENTIAL = re.compile(r"(?i)(https?://)([^/@\s:]+):([^/@\s]+)@")
_QUERY_SECRET = re.compile(
    r"(?i)([?&](?:access[_-]?token|api[_-]?key|key|secret|signature)=)[^&#\s]+"
)
# A raw 32-byte hex value is much more likely to be a private key than useful
# dashboard content.  Public condition ids may consequently be hidden in an
# error line; that conservative trade-off is preferable on a live-wallet TTY.
_RAW_32_BYTE_HEX = re.compile(r"(?<![0-9A-Fa-f])(?:0x)?[0-9A-Fa-f]{64}(?![0-9A-Fa-f])")
_JWT = re.compile(r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
                  r"\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])")


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return f"<unprintable {type(value).__name__}>"


def redact(value: Any, max_chars: int = 4096) -> str:
    """Return bounded text with common credential forms removed."""
    text = _as_text(value)
    if max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars] + "...<truncated>"
    text = _URL_CREDENTIAL.sub(r"\1<redacted>:<redacted>@", text)
    text = _QUERY_SECRET.sub(r"\1<redacted>", text)
    text = _NAMED_SECRET.sub(
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}<redacted>{m.group(5)}",
        text,
    )
    text = _JWT.sub("<redacted-jwt>", text)
    return _RAW_32_BYTE_HEX.sub("<redacted-32-byte-value>", text)


def terminal_text(value: Any, max_chars: int = 4096, *, redact_secrets: bool = True) -> str:
    """Make text safe and single-column before it is written to a terminal.

    C0/C1 controls, bidi/format controls, combining marks, and double-width
    glyphs are replaced or removed.  This both blocks escape-sequence injection
    and preserves the renderer's exact cursor geometry.

    It runs on every piece of every dashboard frame (~14k characters at
    240x60), on a thread that shares the GIL with the trading loop, so two
    exact shortcuts: a short string's answer is cached (frames repeat their
    text), and printable ASCII skips the per-character pass, which would
    return it unchanged.
    """
    if isinstance(value, str) and len(value) <= _CACHE_MAX_LEN:
        return _terminal_text_cached(value, max_chars, redact_secrets)
    return _terminal_text(value, max_chars, redact_secrets)


_CACHE_MAX_LEN = 512


@lru_cache(maxsize=8192)
def _terminal_text_cached(value: str, max_chars: int, redact_secrets: bool) -> str:
    return _terminal_text(value, max_chars, redact_secrets)


def _terminal_text(value: Any, max_chars: int, redact_secrets: bool) -> str:
    text = redact(value, max_chars=max_chars) if redact_secrets else _as_text(value)
    if max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars] + "...<truncated>"
    if text.isascii() and text.isprintable():
        # No control, format, combining or wide character can be in it.
        return text
    out: list[str] = []
    for char in text:
        code = ord(char)
        category = unicodedata.category(char)
        if char in "\r\n\t":
            out.append(" ")
        elif code < 32 or 0x7F <= code <= 0x9F:
            out.append("?")
        elif category in ("Mn", "Me", "Cf"):
            # Combining and format controls have zero/ambiguous cursor width;
            # Cf also contains bidi overrides used for log spoofing.
            continue
        elif unicodedata.east_asian_width(char) in ("W", "F"):
            out.append("?")
        else:
            out.append(char)
    return "".join(out)


def exception_summary(exc: BaseException, max_chars: int = 240) -> str:
    """A contextual, redacted exception string suitable for operator logs."""
    detail = terminal_text(exc, max_chars=max_chars)
    return f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
