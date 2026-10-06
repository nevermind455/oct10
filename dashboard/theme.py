"""Palette, SGR generation and glyph sets for the terminal.

Pure presentation. Imports nothing from the bot.
"""
from __future__ import annotations

import os
import sys
from typing import NamedTuple

# ---------------------------------------------------------------- palette ---
# Warm cream terminal: paper background, ink foreground, restrained accents.
RGB = {
    "cream":   (244, 239, 228),
    "paper2":  (236, 230, 216),   # alternating row wash
    "ink":     (26, 26, 28),
    "dim":     (128, 122, 110),
    "faint":   (176, 169, 155),
    "rule":    (74, 70, 62),      # module borders
    "green":   (0, 118, 51),
    "green2":  (0, 156, 68),
    "red":     (186, 28, 42),
    "red2":    (222, 60, 60),
    "amber":   (176, 122, 0),
    "blue":    (28, 78, 168),
    "purple":  (108, 58, 158),
    "cyan":    (0, 116, 128),
    "white":   (255, 253, 248),
}

# 256-colour fallback indices (approximate matches for the above)
XTERM = {
    "cream": 230, "paper2": 224, "ink": 234, "dim": 244, "faint": 250,
    "rule": 238, "green": 28, "green2": 34, "red": 124, "red2": 160,
    "amber": 136, "blue": 25, "purple": 91, "cyan": 30, "white": 231,
}


class Style(NamedTuple):
    fg: str = "ink"
    bg: str = "cream"
    bold: bool = False
    reverse: bool = False


DEFAULT = Style()

RESET = "\x1b[0m"


def _depth() -> str:
    """truecolor | xterm256 | mono"""
    forced = os.environ.get("TERM_COLOR_MODE", "").strip().lower()
    if forced in ("truecolor", "xterm256", "mono"):
        return forced
    if os.environ.get("NO_COLOR"):
        return "mono"
    if os.environ.get("COLORTERM", "").lower() in ("truecolor", "24bit"):
        return "truecolor"
    if sys.platform == "win32":
        # Windows Terminal / conhost with VT enabled handles 24-bit fine.
        return "truecolor" if os.environ.get("WT_SESSION") else "xterm256"
    term = os.environ.get("TERM", "")
    if "256" in term or "truecolor" in term:
        return "truecolor"
    return "xterm256" if term and term != "dumb" else "mono"


COLOR_MODE = _depth()

_sgr_cache: dict[Style, str] = {}


def sgr(style: Style) -> str:
    """Escape sequence for a style. Memoised; called once per run per style."""
    hit = _sgr_cache.get(style)
    if hit is not None:
        return hit
    if COLOR_MODE == "mono":
        parts = ["0"]
        if style.bold:
            parts.append("1")
        if style.reverse:
            parts.append("7")
        out = "\x1b[" + ";".join(parts) + "m"
        _sgr_cache[style] = out
        return out

    fg, bg = style.fg, style.bg
    if style.reverse:
        fg, bg = bg, fg
    parts = ["0"]
    if style.bold:
        parts.append("1")
    if COLOR_MODE == "truecolor":
        r, g, b = RGB.get(fg, RGB["ink"])
        parts += ["38", "2", str(r), str(g), str(b)]
        r, g, b = RGB.get(bg, RGB["cream"])
        parts += ["48", "2", str(r), str(g), str(b)]
    else:
        parts += ["38", "5", str(XTERM.get(fg, 234))]
        parts += ["48", "5", str(XTERM.get(bg, 230))]
    out = "\x1b[" + ";".join(parts) + "m"
    _sgr_cache[style] = out
    return out


# ------------------------------------------------------------------ glyphs ---
class Glyphs(NamedTuple):
    tl: str; tr: str; bl: str; br: str
    h: str; v: str
    tee_d: str; tee_u: str; tee_r: str; tee_l: str; cross: str
    arrow_d: str; arrow_r: str
    block: str; half: str; light: str
    up_tick: str; dn_tick: str
    candle_body: str; candle_wick: str


UNICODE = Glyphs(
    tl="\u250c", tr="\u2510", bl="\u2514", br="\u2518",
    h="\u2500", v="\u2502",
    tee_d="\u252c", tee_u="\u2534", tee_r="\u251c", tee_l="\u2524", cross="\u253c",
    arrow_d="\u25bc", arrow_r="\u25b6",
    block="\u2588", half="\u2584", light="\u2591",
    up_tick="\u25b2", dn_tick="\u25bc",
    candle_body="\u2588", candle_wick="\u2502",
)

# There is deliberately no second, "ASCII-safe" glyph set.  Swapping solid
# blocks for repeated punctuation does not degrade a bar, a candle or a
# numeral - it replaces it with something that only looks like one, and the
# substitution used to be chosen by accident: the dashboard replaces
# sys.stdout with an event sink whose `encoding` is None, so sniffing the
# stream picked the fallback even on a UTF-8 terminal.  The renderer now puts
# the stream into UTF-8 instead, and this is the only glyph set.
GLYPHS = UNICODE


def glyphs() -> Glyphs:
    return GLYPHS


def enable_utf8_output(stream=None) -> bool:
    """Make `stream` able to carry the frame's box and block glyphs.

    Windows consoles still start on a legacy code page, and unencodable
    characters must never take the frame down, so the stream is reconfigured
    with `errors="replace"`: the worst case is a `?`, never a wall of ASCII.
    """
    ok = True
    if sys.platform == "win32":
        try:
            import ctypes
            ok = bool(ctypes.windll.kernel32.SetConsoleOutputCP(65001))
        except Exception:
            ok = False
    target = sys.__stdout__ if stream is None else stream
    reconfigure = getattr(target, "reconfigure", None)
    if reconfigure is None:
        return ok
    try:
        reconfigure(encoding="utf-8", errors="replace")
    except (ValueError, OSError, AttributeError):
        return False
    return ok


# --------------------------------------------------------- semantic styles ---
def state_style(state: str) -> Style:
    """Map a state token to a style. Single source of colour semantics."""
    s = (state or "").upper()
    if s in ("PASS", "OK", "UP", "LIVE", "FILLED", "CONNECTED", "HEALTHY", "ON", "TRUE"):
        return Style("white", "green", bold=True)
    if s in ("BLOCK", "FAIL", "DOWN", "ERROR", "REJECTED", "DISCONNECTED", "LOSS", "STALLED"):
        return Style("white", "red", bold=True)
    if s in ("WAIT", "PENDING", "STALE", "ARMED", "WARN", "IDLE"):
        return Style("ink", "amber", bold=True)
    if s in ("ABSENT", "N/A", "--", ""):
        return Style("faint", "paper2")
    return Style("ink", "paper2")


def state_fg(state: str) -> Style:
    """Same semantics as state_style but as coloured text on paper.

    Filled cells are for the status strip, where a block of colour IS the
    signal. Inside a panel a filled run just reads as a blob, so nodes and
    inline values use this instead.
    """
    s = (state or "").upper()
    if s in ("PASS", "OK", "UP", "LIVE", "FILLED", "CONNECTED", "HEALTHY", "ON", "TRUE"):
        return Style("green", bold=True)
    if s in ("BLOCK", "FAIL", "DOWN", "ERROR", "REJECTED", "DISCONNECTED", "LOSS", "STALLED"):
        return Style("red", bold=True)
    if s in ("WAIT", "PENDING", "STALE", "ARMED", "WARN", "IDLE"):
        return Style("amber", bold=True)
    if s in ("ABSENT", "N/A", "--", ""):
        return Style("faint")
    return Style("ink")


def pnl_style(value, big: bool = False) -> Style:
    if value is None:
        return Style("faint")
    if value > 0:
        return Style("green2" if big else "green", bold=True)
    if value < 0:
        return Style("red2" if big else "red", bold=True)
    return Style("dim", bold=big)


def enable_windows_vt() -> bool:
    """Turn on ENABLE_VIRTUAL_TERMINAL_PROCESSING. No-op off Windows."""
    if sys.platform != "win32":
        return True
    try:
        import ctypes
        from ctypes import wintypes

        k32 = ctypes.windll.kernel32
        handle = k32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = wintypes.DWORD()
        if not k32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        return bool(k32.SetConsoleMode(handle, mode.value | 0x0004))
    except Exception:
        return False
