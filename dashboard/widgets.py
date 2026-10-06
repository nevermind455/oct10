"""Widget primitives.

A Row is a list of (text, Style) runs whose visible widths sum to exactly the
allotted column count. Every builder here is pure: same inputs -> same rows.
That is what makes the whole layout testable without a terminal.
"""
from __future__ import annotations

import time
import math
from typing import Iterable, Sequence

from .safety import terminal_text
from .theme import Glyphs, Style, state_style

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
    fill = inner_w - len(cap) - len(note)
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


def chip(text: str, st: str, cols: int | None = None) -> Row:
    style = state_style(st)
    body = f" {text} "
    if cols is not None:
        body = fit(body, cols, "^")
    return [(body, style)]


def big_number(value: str, cols: int, style: Style) -> Row:
    return [(fit(value, cols, ">"), style)]


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


# ----------------------------------------------------------------- charts ---
# Half-height blocks. A terminal cell is two sub-rows tall for drawing
# purposes, which is what lets a candle whose whole range is under one row
# still show a body instead of vanishing into a wick.
_HALF_TOP = "▀"
_HALF_BOT = "▄"
_WICK_TOP = "╵"
_WICK_BOT = "╷"
_BODY, _WICK = 2, 1          # paint priority within a sub-cell


def candles(data: Sequence[tuple[float, float, float, float]], cols: int, rows: int,
            g: Glyphs, ref: float | None = None, last: float | None = None,
            times: Sequence[float] | None = None) -> list[Row]:
    """Live OHLC candlesticks from the real trade feed.

    `data` is newest-last (o,h,l,c). Green when the close is at or above the
    open, red below. `ref` is the Polymarket strike and `last` the current
    price; both are drawn as dashed lines, interleaved so they stay readable
    where they cross. `times` are the candle open timestamps for the X axis.
    """
    if rows < 3 or cols < 12:
        return [blank(cols) for _ in range(max(0, rows))]
    label_w = 10
    plot_w = cols - label_w
    axis_h = 1 if rows >= 5 else 0
    plot_h = rows - axis_h
    # One blank column between candles once there is room; a wall of adjacent
    # bodies stops reading as candles.
    step = 2 if plot_w >= 30 else 1
    keep = max(1, plot_w // step)
    series = []
    for candle in list(data)[-keep:]:
        try:
            values = tuple(float(v) for v in candle)
        except (TypeError, ValueError, OverflowError):
            continue
        if len(values) == 4 and all(math.isfinite(v) for v in values):
            series.append(values)
    stamps = list(times or ())[-len(series):]
    if not series:
        return [pad([(fit("  waiting for the Binance trade feed", cols), FAINT)], cols)] + \
               [blank(cols) for _ in range(rows - 1)]

    lo = min(c[2] for c in series)
    hi = max(c[1] for c in series)
    for level in (ref, last):
        if level is not None:
            lo, hi = min(lo, level), max(hi, level)
    if hi - lo < 1e-9:
        hi, lo = hi + 1.0, lo - 1.0
    span = hi - lo
    sub_h = plot_h * 2

    def sub_y(p: float) -> int:
        return max(0, min(sub_h - 1, int(round((hi - p) / span * (sub_h - 1)))))

    # Candles live on a half-cell grid; the two dashed price lines live on a
    # whole-cell layer underneath and only show where no candle covers them.
    marks: list[list[tuple[int, Style] | None]] = [[None] * plot_w for _ in range(sub_h)]
    lines: list[list[tuple[str, Style] | None]] = [[None] * plot_w for _ in range(plot_h)]

    def rule(level: float | None, style: Style, phase: int) -> None:
        if level is None:
            return
        y = sub_y(level) // 2
        for x in range(phase, plot_w, 2):
            lines[y][x] = (g.h, style)

    rule(ref, Style("amber", bold=True), 0)
    rule(last, Style("blue"), 1)

    pad_left = plot_w - len(series) * step
    for i, (o, h, l, c) in enumerate(series):
        x = pad_left + i * step
        if not 0 <= x < plot_w:
            continue
        st = Style("green2" if c >= o else "red2", bold=True)
        y_hi, y_lo = sub_y(h), sub_y(l)
        body_top, body_bot = min(sub_y(o), sub_y(c)), max(sub_y(o), sub_y(c))
        for y in range(y_hi, y_lo + 1):
            kind = _BODY if body_top <= y <= body_bot else _WICK
            cur = marks[y][x]
            if cur is None or kind >= cur[0]:
                marks[y][x] = (kind, st)

    def cell(cy: int, x: int) -> tuple[str, Style]:
        top, bot = marks[cy * 2][x], marks[cy * 2 + 1][x]
        if top is None and bot is None:
            drawn = lines[cy][x]
            return drawn if drawn else (" ", PAPER)
        style = (top or bot)[1]
        kt = top[0] if top else 0
        kb = bot[0] if bot else 0
        if kt == _BODY and kb == _BODY:
            return g.block, style
        if kt == _BODY:
            return _HALF_TOP, style
        if kb == _BODY:
            return _HALF_BOT, style
        if kt and kb:
            return g.candle_wick, style
        return (_WICK_TOP if kt else _WICK_BOT), style

    out: list[Row] = []
    for cy in range(plot_h):
        price = hi - (cy / max(1, plot_h - 1)) * span
        row: Row = [(f"{price:>9,.0f} "[:label_w], FAINT)]
        run_text, run_style = "", None
        for x in range(plot_w):
            ch, st = cell(cy, x)
            if st == run_style:
                run_text += ch
            else:
                if run_text:
                    row.append((run_text, run_style))
                run_text, run_style = ch, st
        if run_text:
            row.append((run_text, run_style))
        out.append(pad(row, cols))

    if axis_h:
        out.append(pad([(" " * label_w, FAINT),
                        (_time_axis(stamps, series, plot_w, pad_left, step), FAINT)], cols))
    return out


def _time_axis(stamps: Sequence[float], series: Sequence, plot_w: int,
               pad_left: int, step: int) -> str:
    """Clock labels under the candles they belong to, newest at the right."""
    axis = [" "] * plot_w
    if len(stamps) != len(series):
        return "".join(axis)
    gap = max(12, step * 6)
    x = plot_w - 1
    while x >= 0:
        i = (x - pad_left) // step
        if 0 <= i < len(stamps):
            try:
                text = time.strftime("%H:%M:%S", time.localtime(float(stamps[i])))
            except (TypeError, ValueError, OverflowError, OSError):
                x -= gap
                continue
            start = x - len(text) + 1
            if start < 0:
                break
            for k, ch in enumerate(text):
                axis[start + k] = ch
        x -= gap
    return "".join(axis)


def sparkline(values: Sequence[float], cols: int, rows: int, g: Glyphs,
              baseline: float | None = None) -> list[Row]:
    """Filled area line. Green above baseline, red below."""
    if rows < 2 or cols < 4:
        return [blank(cols) for _ in range(max(0, rows))]
    vals = []
    for value in list(values)[-cols:]:
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(value):
            vals.append(value)
    if len(vals) < 2:
        return [pad([(fit("  no series yet", cols), FAINT)], cols)] + \
               [blank(cols) for _ in range(rows - 1)]
    lo, hi = min(vals), max(vals)
    if baseline is not None:
        lo, hi = min(lo, baseline), max(hi, baseline)
    if hi - lo < 1e-12:
        hi, lo = hi + 1.0, lo - 1.0
    span = hi - lo
    base_y = int(round((hi - (baseline if baseline is not None else lo)) / span * (rows - 1)))

    grid = [[(" ", PAPER) for _ in range(cols)] for _ in range(rows)]
    off = cols - len(vals)
    for i, v in enumerate(vals):
        x = off + i
        y = int(round((hi - v) / span * (rows - 1)))
        up = baseline is None or v >= baseline
        st = Style("green" if up else "red")
        top, bot = (y, base_y) if y <= base_y else (base_y, y)
        for yy in range(top, bot + 1):
            grid[yy][x] = (g.block if yy == y else g.light, st)
    out: list[Row] = []
    for y in range(rows):
        row: Row = []
        run_text, run_style = "", grid[y][0][1]
        for x in range(cols):
            ch, st = grid[y][x]
            if st == run_style:
                run_text += ch
            else:
                row.append((run_text, run_style))
                run_text, run_style = ch, st
        row.append((run_text, run_style))
        out.append(pad(row, cols))
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


def meter(label: str, frac: float | None, cols: int, g: Glyphs,
          good_high: bool = True) -> Row:
    """Horizontal gauge: filled bar, empty track, percentage. frac None -> `--`."""
    lab = fit(label, min(len(label), max(4, cols // 3)), "<")
    bar_w = max(3, cols - len(lab) - 7)
    if frac is None:
        return pad([(lab, DIM), (" ", PAPER), (g.light * bar_w, FAINT), ("   --", FAINT)], cols)
    try:
        frac = float(frac)
    except (TypeError, ValueError, OverflowError):
        frac = 0.0
    if not math.isfinite(frac):
        frac = 0.0
    frac = max(0.0, min(1.0, frac))
    good = frac >= 0.5 if good_high else frac <= 0.5
    st = Style("green" if good else ("amber" if 0.25 <= frac <= 0.75 else "red"))
    pct = f"{frac * 100:>4.0f}%"
    return pad([(lab, DIM), (" ", PAPER)] + bar(frac, bar_w, g, st) +
               [(" ", PAPER), (pct, st)], cols)


def histogram(buckets: Sequence[tuple[str, int]], cols: int, rows: int, g: Glyphs,
              totals: Sequence[int] | None = None) -> list[Row]:
    """Labelled horizontal bars.

    With `totals` each bar is that bucket's share of its own denominator and
    the share is printed, so `UP` and `DOWN` read as one split rather than as
    two unrelated counts.  Without it, bars are scaled to the largest bucket.
    """
    out: list[Row] = []
    if not buckets:
        return [pad([(fit("  no samples", cols), FAINT)], cols)] + \
               [blank(cols) for _ in range(max(0, rows - 1))]
    top = max((n for _, n in buckets), default=0) or 1
    lab_w = max(len(b) for b, _ in buckets)
    lab_w = min(lab_w, max(4, cols // 3))
    tail_w = 10 if totals is not None else 5
    bar_w = max(1, cols - lab_w - tail_w - 1)
    for i, (name, n) in enumerate(buckets[:rows]):
        if totals is not None:
            denom = totals[i] if i < len(totals) else 0
            frac = (n / denom) if denom else 0.0
            tail = f"{n:>4d} {frac * 100:>3.0f}%" if denom else f"{n:>4d}   --"
            tail_style = Style("ink") if denom else FAINT
        else:
            frac, tail, tail_style = n / top, f"{n:>4d}", Style("ink")
        out.append(pad(
            [(fit(name, lab_w, "<"), DIM), (" ", PAPER)] +
            bar(frac, bar_w, g, Style("blue"), FAINT) +
            [(" ", PAPER), (tail, tail_style)], cols))
    while len(out) < rows:
        out.append(blank(cols))
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
