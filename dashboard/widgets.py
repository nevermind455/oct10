"""Widget primitives.

A Row is a list of (text, Style) runs whose visible widths sum to exactly the
allotted column count. Every builder here is pure: same inputs -> same rows.
That is what makes the whole layout testable without a terminal.
"""
from __future__ import annotations

import math
from typing import Iterable, Sequence

from .safety import terminal_text
from .theme import Glyphs, Style

Run = tuple[str, Style]
Row = list[Run]

PAPER = Style()
DIM = Style("dim")
FAINT = Style("faint")
RULE = Style("rule")


def width(row: Row) -> int:
    return sum(len(terminal_text(t)) for t, _ in row)


def pad(row: Row, cols: int, style: Style = PAPER) -> Row:
    """Force a row to exactly `cols` visible characters."""
    cols = max(0, int(cols))
    # `pad` is the final path for every layout row.  Normalising here ensures
    # remote error strings cannot inject terminal escapes or silently consume
    # two terminal cells with an emoji/wide glyph.
    row = [(terminal_text(text), st) for text, st in row]
    w = width(row)
    if w == cols:
        return row
    if w < cols:
        return row + [(" " * (cols - w), style)]
    out: Row = []
    left = cols
    for text, st in row:
        if left <= 0:
            break
        if len(text) <= left:
            out.append((text, st))
            left -= len(text)
        else:
            out.append((text[:left], st))
            left = 0
    return out


def blank(cols: int, style: Style = PAPER) -> Row:
    return [(" " * cols, style)]


def trunc(text: str, n: int) -> str:
    text = terminal_text(text)
    if n <= 0:
        return ""
    return text if len(text) <= n else (text[: n - 1] + "\u2026" if n > 1 else text[:n])


def fit(text: str, n: int, align: str = "<") -> str:
    t = trunc(text, n)
    if align == ">":
        return t.rjust(n)
    if align == "^":
        return t.center(n)
    return t.ljust(n)


# ------------------------------------------------------------------ panel ---
def panel(title: str, body: Sequence[Row], cols: int, rows: int, g: Glyphs,
          accent: Style = RULE, right_note: str = "",
          title_style: Style | None = None,
          note_style: Style | None = None) -> list[Row]:
    """Thin rectangular module with the title inlaid in the top border."""
    if cols < 4 or rows < 2:
        return [pad([], cols) for _ in range(max(0, rows))]

    inner_w = cols - 2
    cap = f" {title[: max(0, inner_w - 4)]} "
    note = f" {right_note} " if right_note else ""
    # -1 for the rule segment between the corner and the title: without it the
    # top border came out one cell too wide and pad() cut off the right corner.
    fill = inner_w - 1 - len(cap) - len(note)
    cap_style = title_style or Style("ink", "cream", bold=True)
    top: Row = [(g.tl, accent), (g.h, accent), (cap, cap_style)]
    if fill > 0:
        top.append((g.h * fill, accent))
    if note:
        top.append((note, note_style or FAINT))
    top.append((g.tr, accent))
    top = pad(top, cols)

    out = [top]
    body_h = rows - 2
    for i in range(body_h):
        content = body[i] if i < len(body) else []
        line: Row = [(g.v, accent)] + list(pad(content, inner_w)) + [(g.v, accent)]
        out.append(pad(line, cols))
    bottom = pad([(g.bl, accent), (g.h * inner_w, accent), (g.br, accent)], cols)
    out.append(bottom)
    return out[:rows]


def hsplit(total: int, weights: Sequence[float], mins: Sequence[int]) -> list[int]:
    """Distribute `total` columns across panels, honouring minimums exactly."""
    n = len(weights)
    if n == 0:
        return []
    out = list(mins)
    spare = total - sum(mins)
    if spare <= 0:
        # Not enough room: shrink from the right but never below 3.
        i = n - 1
        while spare < 0 and i >= 0:
            take = min(out[i] - 3, -spare)
            if take > 0:
                out[i] -= take
                spare += take
            i -= 1
        return out
    wsum = sum(weights) or 1.0
    for i in range(n):
        out[i] += int(spare * weights[i] / wsum)
    out[-1] += total - sum(out)
    return out


def join(parts: Sequence[list[Row]], widths: Sequence[int], rows: int) -> list[Row]:
    """Place panels side by side into `rows` full-width rows."""
    out: list[Row] = []
    for r in range(rows):
        line: Row = []
        for p, w in zip(parts, widths):
            line += pad(p[r] if r < len(p) else [], w)
        out.append(line)
    return out


# ------------------------------------------------------------------- text ---
def kv(label: str, value: str, cols: int, vstyle: Style = PAPER,
       lstyle: Style = DIM, gap: int = 1) -> Row:
    lw = min(len(label), max(1, cols - gap - 1))
    vw = cols - lw - gap
    return [(label[:lw], lstyle), (" " * gap, PAPER), (fit(value, vw, ">"), vstyle)]


def table(headers: Sequence[str], widths_: Sequence[int], rows_data: Iterable[Sequence[tuple[str, Style]]],
          cols: int, max_rows: int, zebra: bool = True) -> list[Row]:
    """Compact table. rows_data yields per-cell (text, style)."""
    head: Row = []
    for h, w in zip(headers, widths_):
        head.append((fit(h, w, "<"), Style("dim", "cream", bold=True)))
        head.append((" ", PAPER))
    out = [pad(head, cols)]
    for i, rd in enumerate(rows_data):
        if len(out) - 1 >= max_rows:
            break
        bg = "paper2" if (zebra and i % 2 == 1) else "cream"
        line: Row = []
        for (text, st), w in zip(rd, widths_):
            line.append((fit(text, w, "<"), st._replace(bg=bg)))
            line.append((" ", Style("ink", bg)))
        out.append(pad(line, cols, Style("ink", bg)))
    return out


# Eighth-width blocks. A gauge that can only move in whole cells jumps in
# 5-10% steps at these panel widths, which reads as a stuck bar rather than a
# live one.
_EIGHTHS = "▏▎▍▌▋▊▉"


def bar(frac: float, width: int, g: Glyphs, fill: Style,
        track: Style = FAINT) -> Row:
    """Filled bar of exactly `width` cells, resolved to an eighth of a cell."""
    if width <= 0:
        return []
    try:
        frac = float(frac)
    except (TypeError, ValueError, OverflowError):
        frac = 0.0
    if not math.isfinite(frac):
        frac = 0.0
    frac = max(0.0, min(1.0, frac))
    units = int(round(frac * width * 8))
    full, rem = divmod(units, 8)
    full = min(full, width)
    out: Row = []
    if full:
        out.append((g.block * full, fill))
    if rem and full < width:
        out.append((_EIGHTHS[rem - 1], fill))
        full += 1
    if full < width:
        out.append((g.light * (width - full), track))
    return out


# ------------------------------------------------------------- big digits ---
# Three-row box-drawing seven-segment font, the fallback size for the cash
# figure. A terminal cannot change font size for one region, so height is
# bought in rows and weight in glyphs.
_SEG_BOX = {
    "0": ("\u250c\u2500\u2510", "\u2502 \u2502", "\u2514\u2500\u2518"),
    "1": ("  \u2577", "  \u2502", "  \u2575"),
    "2": ("\u2576\u2500\u2510", "\u250c\u2500\u2518", "\u2514\u2500\u2574"),
    "3": ("\u2576\u2500\u2510", " \u2500\u2524", "\u2576\u2500\u2518"),
    "4": ("\u2577 \u2577", "\u2514\u2500\u2524", "  \u2575"),
    "5": ("\u250c\u2500\u2574", "\u2514\u2500\u2510", "\u2576\u2500\u2518"),
    "6": ("\u250c\u2500\u2574", "\u251c\u2500\u2510", "\u2514\u2500\u2518"),
    "7": ("\u2576\u2500\u2510", "  \u2502", "  \u2575"),
    "8": ("\u250c\u2500\u2510", "\u251c\u2500\u2524", "\u2514\u2500\u2518"),
    "9": ("\u250c\u2500\u2510", "\u2514\u2500\u2524", "\u2576\u2500\u2518"),
    ".": (" ", " ", "\u2584"), ",": (" ", " ", ","), " ": (" ", " ", " "),
    "-": ("   ", "\u2500\u2500\u2500", "   "), "+": ("   ", " + ", "   "),
    "$": (" ", "$", " "), "%": ("  ", "%%", "  "),
}


_SEG = _SEG_BOX


def big_digits_width(text: str, table: dict | None = None) -> int:
    table = table or _SEG
    total = 0
    for ch in text:
        seg = table.get(ch)
        total += (len(seg[0]) + 1) if seg else 2
    return max(0, total - 1)


def big_digits(text: str, cols: int, style: Style, align: str = ">",
               g: Glyphs | None = None) -> list[Row]:
    """Three rows of seven-segment glyphs, or a bold fallback if too narrow."""
    table = _SEG
    if not any(c.isdigit() for c in text) or big_digits_width(text, table) > cols:
        return [blank(cols), pad([(fit(text, cols, align), style)], cols), blank(cols)]
    lines = ["", "", ""]
    for ch in text:
        seg = table.get(ch, ("?", "?", "?"))
        for i in range(3):
            lines[i] += seg[i] + " "
    lines = [ln[:-1] for ln in lines]
    return [pad([(fit(ln, cols, align), style)], cols) for ln in lines]


# Five-row solid-block digits for the cash figure: the largest numeral a
# terminal can actually set, drawn in full block glyphs rather than in
# repeated punctuation pretending to be a numeral.
#
# Weight is the whole trick. A terminal cell is about twice as tall as it is
# wide, so a one-column upright next to a one-row bar reads as a hairline
# beside a slab. Uprights are two columns here, and the two glyphs no
# five-row grid can hold - `$` and the decimal point - stop pretending: the
# currency mark is a normal `$` on the centre line and the point is a square
# block on the baseline, the way a large figure is actually typeset.
_BLOCK_WIDE = {
    "0": ("██████", "██  ██", "██  ██", "██  ██", "██████"),
    "1": ("    ██", "    ██", "    ██", "    ██", "    ██"),
    "2": ("██████", "    ██", "██████", "██    ", "██████"),
    "3": ("██████", "    ██", "██████", "    ██", "██████"),
    "4": ("██  ██", "██  ██", "██████", "    ██", "    ██"),
    "5": ("██████", "██    ", "██████", "    ██", "██████"),
    "6": ("██████", "██    ", "██████", "██  ██", "██████"),
    "7": ("██████", "    ██", "    ██", "    ██", "    ██"),
    "8": ("██████", "██  ██", "██████", "██  ██", "██████"),
    "9": ("██████", "██  ██", "██████", "    ██", "██████"),
    "$": (" ", " ", "$", " ", " "),
    ".": ("  ", "  ", "  ", "  ", "██"),
    ",": ("  ", "  ", "  ", "  ", "█ "),
    "-": ("      ", "      ", "██████", "      ", "      "),
    "+": ("      ", "  ██  ", "██████", "  ██  ", "      "),
    " ": ("  ", "  ", "  ", "  ", "  "),
}
_BLOCK_NARROW = {
    "0": ("████", "█  █", "█  █", "█  █", "████"),
    "1": ("   █", "   █", "   █", "   █", "   █"),
    "2": ("████", "   █", "████", "█   ", "████"),
    "3": ("████", "   █", "████", "   █", "████"),
    "4": ("█  █", "█  █", "████", "   █", "   █"),
    "5": ("████", "█   ", "████", "   █", "████"),
    "6": ("████", "█   ", "████", "█  █", "████"),
    "7": ("████", "   █", "   █", "   █", "   █"),
    "8": ("████", "█  █", "████", "█  █", "████"),
    "9": ("████", "█  █", "████", "   █", "████"),
    "$": (" ", " ", "$", " ", " "),
    ".": (" ", " ", " ", " ", "█"),
    ",": (" ", " ", " ", " ", "█"),
    "-": ("    ", "    ", "████", "    ", "    "),
    "+": ("    ", " ██ ", "████", " ██ ", "    "),
    " ": (" ", " ", " ", " ", " "),
}
_BLOCK_ROWS = 5


def block_digits_width(text: str, table: dict) -> int:
    """Columns this block font needs for `text`, one blank column per gap."""
    total = 0
    for ch in text:
        rows = table.get(ch)
        total += (len(rows[0]) + 1) if rows else 3
    return max(0, total - 1)


def _render_blocks(text: str, table: dict, cols: int, style: Style,
                   align: str) -> list[Row]:
    lines = [""] * _BLOCK_ROWS
    for ch in text:
        rows = table.get(ch) or table[" "]
        for i in range(_BLOCK_ROWS):
            lines[i] += rows[i] + " "
    return [pad([(fit(ln[:-1], cols, align), style)], cols) for ln in lines]


def giant_digits(text: str, cols: int, style: Style, align: str = ">",
                 g: Glyphs | None = None) -> list[Row]:
    """The cash figure, five rows tall, thinning only when the panel demands it."""
    if any(c.isdigit() for c in text):
        for table in (_BLOCK_WIDE, _BLOCK_NARROW):
            if block_digits_width(text, table) <= cols:
                return _render_blocks(text, table, cols, style, align)
    # Below that the seven-segment font is the last legible size; centring it
    # keeps the panel's rows from shifting under the figure.
    return [blank(cols)] + big_digits(text, cols, style, align, g) + [blank(cols)]
