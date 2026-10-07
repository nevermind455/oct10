"""The sniper dashboard: one pure function from a snapshot dict to a frame.

    build(snap, cols, rows, g) -> list[Row]     exactly `rows` rows, each
                                                exactly `cols` cells wide

run.py builds `snap` from the hub, the Sniper, the broker/ledger and the
live canary each frame - plain values only, nothing live-referenced - so this
module never touches the bot and can be tested without a terminal. Every key
is optional: a missing value is drawn as "--", never invented.

    mode        "PAPER" | "LIVE"
    clock       "HH:MM:SS ET"
    trigger     feed, status, mid, move_bp, k_bp, window_ms, last_signal_bp,
                last_signal_age_s, signals, signals_per_min, orders, filled,
                refused, fill_rate, markout_1s_c, markout_5s_c, order_type,
                slip, latency_ms, clob_rtt_ms, errors, in_flight
    market      slug, seconds_left, trade_until_s, book_status, up / down
                ({bid, ask, status, age_ms}), round_fills, max_fills,
                round_up_sh, round_down_sh, max_side_shares
    orders      [{time, side, bp, ask, cap, result, shares, price, detail}],
                newest last
    account     kind, cash, equity, pnl, realized, open, pending_cost, fees,
                wins, losses, start, balance, balance_status
    canary      None in paper; halted [reasons], orders/max_orders,
                fills/max_fills, spend/max_spend, rounds/max_rounds,
                loss/max_loss, guard, guard_reason, balance, awaiting,
                clock_ok, rules_ready
    health      [(name, state, detail)]
    skips       [(reason, count)], most frequent first
    events      [(wall, source, text, level)], newest last
"""
from __future__ import annotations

import math
import time

from .safety import terminal_text
from .theme import Glyphs, Style, pnl_style, state_fg, state_style
from .widgets import DIM, FAINT, PAPER, Row, bar, blank, fit, hsplit, join, pad, panel

MISSING = "--"
LEVEL_STYLE = {"good": Style("green"), "warn": Style("amber"), "bad": Style("red", bold=True),
               "info": Style("ink")}
RESULT_STYLE = {"filled": Style("green", bold=True), "missed": Style("amber"), "refused": Style("amber"),
                "pending": Style("blue", bold=True), "error": Style("red", bold=True),
                "rejected": Style("red", bold=True), "unknown": Style("purple")}


# ---------------------------------------------------------------- formatting
def _num(v):
    """A finite float, or None."""
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def money(v, sign: bool = False) -> str:
    f = _num(v)
    if f is None:
        return MISSING
    return f"{'+' if sign and f > 0 else '-' if f < 0 else ''}${abs(f):,.2f}"


def price(v) -> str:
    f = _num(v)
    return MISSING if f is None else f"{f:.3f}"


def bp(v) -> str:
    f = _num(v)
    return MISSING if f is None else f"{f:+.2f}bp"


def cents(v) -> str:
    f = _num(v)
    return MISSING if f is None else f"{f:+.1f}c"


def count(v) -> str:
    f = _num(v)
    return MISSING if f is None else f"{int(f)}"


def ratio(a, b) -> str:
    return f"{count(a)}/{count(b)}"


def _text(v, default: str = MISSING) -> str:
    return default if v is None or v == "" else terminal_text(v, 400)


# ------------------------------------------------------------------ panels
def _line(label: str, value: str, cols: int, vstyle: Style = PAPER, lw: int = 9) -> Row:
    lw = max(1, min(lw, cols // 2))
    return pad([(fit(label, lw), DIM), (" ", PAPER), (fit(value, max(0, cols - lw - 1)), vstyle)], cols)


def _runs(parts, cols: int) -> Row:
    return pad([(terminal_text(t, 400), s) for t, s in parts if t], cols)


def _header(snap: dict, cols: int) -> Row:
    mode = str(snap.get("mode") or MISSING).upper()
    canary = snap.get("canary") or {}
    market = snap.get("market") or {}
    left = _num(market.get("seconds_left"))
    parts = [(" BTC SNIPER ", Style("white", "rule", bold=True)), (" ", PAPER),
             (f" {mode} ", Style("white", "red", bold=True) if mode == "LIVE" else Style("white", "green", bold=True))]
    if canary:
        halted = canary.get("halted") or []
        parts += [(" ", PAPER), (" HALTED " if halted else " CANARY ARMED ",
                                 state_style("FAIL" if halted else "WARN"))]
    parts += [("  ", PAPER), (_text(market.get("slug"), "no round yet"), Style("ink", bold=True)),
              ("  ", PAPER), (f"T-{left:.0f}s" if left is not None else "T-?", Style("blue", bold=True))]
    clock = _text(snap.get("clock"), "")
    used = sum(len(t) for t, _ in parts)
    gap = cols - used - len(clock) - 1
    if gap > 0:
        parts += [(" " * gap, PAPER), (clock, DIM), (" ", PAPER)]
    return pad(parts, cols)


def _trigger_body(t: dict, w: int) -> list[Row]:
    feed = _text(t.get("feed"))
    status = _text(t.get("status"))
    move = _num(t.get("move_bp"))
    k = _num(t.get("k_bp"))
    hot = move is not None and k is not None and abs(move) >= k
    age = _num(t.get("last_signal_age_s"))
    rate = _num(t.get("signals_per_min"))
    fr = _num(t.get("fill_rate"))
    window = _num(t.get("window_ms"))
    rows = [
        _runs([(fit("feed", 9), DIM), (" ", PAPER), (f"{feed} ", Style("ink", bold=True)),
               (status, state_fg(status)), ("  busy" if t.get("in_flight") else "", Style("blue"))], w),
        _line("BTC mid", f"{_num(t['mid']):,.2f}" if _num(t.get("mid")) is not None else MISSING, w,
              Style("ink", bold=True)),
        _runs([(fit("move", 9), DIM), (" ", PAPER), (bp(move), Style("red", bold=True) if hot else Style("ink")),
               (f" /{window:.0f}ms" if window is not None else "", DIM),
               (f" k{k:g}" if k is not None else "", FAINT)], w),
        _runs([(fit("last sig", 9), DIM), (" ", PAPER), (bp(t.get("last_signal_bp")), Style("ink")),
               (f"  {age:.0f}s ago" if age is not None else "", DIM)], w),
        _runs([(fit("signals", 9), DIM), (" ", PAPER), (count(t.get("signals")), Style("ink", bold=True)),
               (f"  {rate:.1f}/min" if rate is not None else "", DIM)], w),
        _runs([(fit("orders", 9), DIM), (" ", PAPER), (count(t.get("orders")), Style("ink", bold=True)),
               ("  fill ", DIM), (count(t.get("filled")), Style("green", bold=True)),
               ("  miss ", DIM), (count(t.get("refused")), Style("amber")),
               (f"  {fr * 100:.0f}%" if fr is not None else "", DIM)], w),
        _runs([(fit("markout", 9), DIM), (" ", PAPER), ("1s ", DIM),
               (cents(t.get("markout_1s_c")), pnl_style(_num(t.get("markout_1s_c")))), ("  5s ", DIM),
               (cents(t.get("markout_5s_c")), pnl_style(_num(t.get("markout_5s_c"))))], w),
        _runs([(fit("order", 9), DIM), (" ", PAPER), (_text(t.get("order_type")), Style("ink")),
               (f" ask+{_num(t['slip']):.2f}" if _num(t.get("slip")) is not None else "", DIM),
               (f"  lat {_num(t['latency_ms']):.0f}ms" if _num(t.get("latency_ms")) is not None else "", DIM),
               (f"  err {count(t.get('errors'))}" if _num(t.get("errors")) else "", Style("red", bold=True))], w),
    ]
    return rows


def _book_side(side: dict | None) -> tuple[str, str, str, str]:
    side = side or {}
    age = _num(side.get("age_ms"))
    return (price(side.get("bid")), price(side.get("ask")), _text(side.get("status")),
            f"{age:.0f}ms" if age is not None else MISSING)


def _market_body(m: dict, w: int, g: Glyphs) -> list[Row]:
    left = _num(m.get("seconds_left"))
    until = _num(m.get("trade_until_s"))
    bar_w = max(3, w - 18)
    bar_style = Style("amber") if (left is not None and until is not None and left < until) else Style("blue")
    left_row = _runs([(fit("left", 9), DIM), (" ", PAPER)]
                     + bar((left or 0.0) / 300.0, bar_w, g, bar_style)
                     + [(f" {left:4.0f}s" if left is not None else "    --", Style("ink", bold=True))], w)
    ub, ua, us, uage = _book_side(m.get("up"))
    db, da, ds, dage = _book_side(m.get("down"))
    cw = max(4, (w - 10) // 2)
    head = _runs([(" " * 10, PAPER), (fit("UP", cw), Style("green", bold=True)),
                  (fit("DOWN", cw), Style("red", bold=True))], w)

    def pair(label, a, b, sa=Style("ink"), sb=Style("ink")):
        return _runs([(fit(label, 9), DIM), (" ", PAPER), (fit(a, cw), sa), (fit(b, cw), sb)], w)

    up_sh, dn_sh = _num(m.get("round_up_sh")), _num(m.get("round_down_sh"))
    return [
        _line("round", _text(m.get("slug"), "waiting for discovery"), w, Style("ink", bold=True)),
        left_row,
        head,
        pair("best bid", ub, db),
        pair("best ask", ua, da, Style("ink", bold=True), Style("ink", bold=True)),
        pair("book", f"{us} {uage}", f"{ds} {dage}", state_fg(us), state_fg(ds)),
        _runs([(fit("this rnd", 9), DIM), (" ", PAPER), ("fills ", DIM),
               (ratio(m.get("round_fills"), m.get("max_fills")), Style("ink")),
               ("  UP ", DIM), (f"{up_sh:.0f}" if up_sh is not None else MISSING, Style("ink")),
               ("  DN ", DIM), (f"{dn_sh:.0f}" if dn_sh is not None else MISSING, Style("ink")),
               (f" /{_num(m['max_side_shares']):.0f}sh" if _num(m.get("max_side_shares")) is not None else "", FAINT)],
              w),
    ]


def _account_body(a: dict, w: int) -> list[Row]:
    kind = str(a.get("kind") or "").upper()
    pnl = _num(a.get("pnl"))
    realized = _num(a.get("realized"))
    rows = []
    if kind == "LIVE":
        rows.append(_line("collatrl", money(a.get("balance")), w, Style("ink", bold=True)))
        rows.append(_line("bal check", _text(a.get("balance_status")), w, state_fg(str(a.get("balance_status") or ""))))
    else:
        rows.append(_line("cash", money(a.get("cash")), w, Style("ink", bold=True)))
        rows.append(_line("equity", money(a.get("equity")), w, Style("ink")))
    rows += [
        _line("P&L" if kind != "LIVE" else "realized", money(pnl if kind != "LIVE" else realized, sign=True), w,
              pnl_style(pnl if kind != "LIVE" else realized, big=True)),
        _line("realized" if kind != "LIVE" else "start", money(realized, sign=True) if kind != "LIVE"
              else _text(a.get("start")), w, pnl_style(realized) if kind != "LIVE" else DIM),
        _line("open", f"{count(a.get('open'))}  ({money(a.get('pending_cost'))})", w, Style("ink")),
        _line("fees", money(a.get("fees")), w, Style("ink")),
        _line("won/lost", f"{count(a.get('wins'))}/{count(a.get('losses'))}", w, Style("ink")),
    ]
    if kind != "LIVE":
        rows.append(_line("start", money(a.get("start")), w, DIM))
    return rows


def _orders_body(orders: list, w: int, h: int) -> list[Row]:
    widths = [8, 4, 7, 5, 5, 7, 13]
    head_txt = ["TIME", "SIDE", "BTC", "ASK", "LIMIT", "RESULT", "FILLED"]
    used = sum(widths) + len(widths)
    detail_w = max(0, w - used)
    out = [_runs([(fit(t, n) + " ", Style("dim", bold=True)) for t, n in zip(head_txt, widths)]
                 + ([(fit("DETAIL", detail_w), Style("dim", bold=True))] if detail_w else []), w)]
    if not orders:
        out.append(_runs([("  no orders yet this session", FAINT)], w))
    for o in list(orders)[-max(0, h - 1):][::-1]:          # newest first
        side = _text(o.get("side"))
        result = str(o.get("result") or MISSING).lower()
        sh, px = _num(o.get("shares")), _num(o.get("price"))
        filled = f"{sh:.1f}@{px:.3f}" if (result == "filled" and sh is not None and px is not None) else ""
        cells = [(_text(o.get("time")), DIM), (side, Style("green" if side == "UP" else "red", bold=True)),
                 (bp(o.get("bp")).replace("bp", ""), Style("ink")), (price(o.get("ask")), Style("ink")),
                 (price(o.get("cap")), DIM), (result.upper(), RESULT_STYLE.get(result, Style("ink"))),
                 (filled, Style("ink"))]
        row = [(fit(t, n) + " ", s) for (t, s), n in zip(cells, widths)]
        if detail_w:
            row.append((fit(_text(o.get("detail"), ""), detail_w), FAINT))
        out.append(_runs(row, w))
    return out


def _side_body(snap: dict, w: int, h: int) -> list[Row]:
    out: list[Row] = []
    c = snap.get("canary")
    if c:
        halted = c.get("halted") or []
        out.append(_runs([(fit("canary", 9), DIM), (" ", PAPER),
                          ("HALTED" if halted else "ARMED", state_fg("FAIL" if halted else "WARN"))], w))
        out.append(_line("orders", ratio(c.get("orders"), c.get("max_orders"))
                         + "  fills " + ratio(c.get("fills"), c.get("max_fills")), w))
        out.append(_line("spend", f"{money(c.get('spend'))}/{money(c.get('max_spend'))}"
                         f"  rounds {ratio(c.get('rounds'), c.get('max_rounds'))}", w))
        out.append(_line("loss", f"{money(c.get('loss'))}/{money(c.get('max_loss'))}"
                         f"  awaiting {count(c.get('awaiting'))}", w))
        guard = _text(c.get("guard"))
        out.append(_runs([(fit("latency", 9), DIM), (" ", PAPER), (guard, state_fg("OK" if guard == "OK" else "WARN")),
                          (f" {_text(c.get('guard_reason'), '')}", FAINT)], w))
        for reason in halted[:3]:
            # Wrapped, never cut: the reason says how to clear the halt (e.g. the stop file's path).
            text, first = _text(reason), True
            cw = max(1, w - 10)
            for _ in range(3):
                if not text:
                    break
                out.append(_runs([(fit("HALT" if first else "", 9), Style("red", bold=True)), (" ", PAPER),
                                  (text[:cw], Style("red"))], w))
                text, first = text[cw:], False
    for name, state, detail in (snap.get("health") or []):
        st = _text(state)
        out.append(_runs([(fit(_text(name), 9), DIM), (" ", PAPER), (fit(st, 12), state_fg(st)), (" ", PAPER),
                          (_text(detail, ""), FAINT)], w))
    skips = snap.get("skips") or []
    if skips and len(out) < h:
        out.append(_runs([("skips    ", DIM)] + [(f"{_text(r)} {count(n)}  ", FAINT) for r, n in skips[:4]], w))
    return out


def _events_body(events: list, w: int, h: int) -> list[Row]:
    out = []
    for wall, source, text, level in list(events)[-h:][::-1]:      # newest first
        try:
            stamp = time.strftime("%H:%M:%S", time.localtime(float(wall)))
        except (TypeError, ValueError, OverflowError, OSError):
            stamp = MISSING
        out.append(_runs([(stamp + " ", DIM), (fit(_text(source), 9) + " ", Style("dim", bold=True)),
                          (_text(text, ""), LEVEL_STYLE.get(level, Style("ink")))], w))
    if not out:
        out.append(_runs([("  no events", FAINT)], w))
    return out


# ------------------------------------------------------------------- frame
def build(snap: dict, cols: int, rows: int, g: Glyphs) -> list[Row]:
    """The whole frame. Never raises: a layout fault is drawn, not thrown."""
    cols, rows = max(1, int(cols)), max(1, int(rows))
    try:
        frame = _build(snap or {}, cols, rows, g)
    except Exception as exc:  # noqa: BLE001 - the dashboard must never take the bot down
        msg = f" dashboard layout error: {type(exc).__name__}: {exc}"
        frame = [pad([(fit(msg, cols), Style("red", bold=True))], cols)]
    frame = [pad(r, cols) for r in frame[:rows]]
    while len(frame) < rows:
        frame.append(blank(cols))
    return frame


def _build(snap: dict, cols: int, rows: int, g: Glyphs) -> list[Row]:
    out = [_header(snap, cols)]
    remaining = rows - 1
    if remaining <= 0:
        return out
    # top band: TRIGGER | MARKET | ACCOUNT, 8 body rows when there is room
    top_h = min(10, remaining)
    w = hsplit(cols, (1.0, 1.1, 0.9), (26, 30, 24))
    body_h = max(0, top_h - 2)
    trig = panel("TRIGGER", _trigger_body(snap.get("trigger") or {}, w[0] - 2)[:body_h], w[0], top_h, g)
    mkt = panel("MARKET", _market_body(snap.get("market") or {}, w[1] - 2, g)[:body_h], w[1], top_h, g)
    acct = (snap.get("account") or {})
    acc = panel("ACCOUNT " + str(acct.get("kind") or snap.get("mode") or "").upper(),
                _account_body(acct, w[2] - 2)[:body_h], w[2], top_h, g)
    out += join([trig, mkt, acc], w, top_h)
    remaining -= top_h
    if remaining <= 2:
        return out
    # middle band: ORDERS | CANARY + HEALTH
    side_rows = _side_body(snap, 1, 99)          # how many rows the side panel wants
    mid_h = min(remaining, max(8, len(side_rows) + 2))
    if remaining - mid_h < 5:                     # not enough for events below: take it all
        mid_h = remaining
    w2 = hsplit(cols, (1.6, 1.0), (40, 30))
    mbody = max(0, mid_h - 2)
    orders = panel("ORDERS", _orders_body(snap.get("orders") or [], w2[0] - 2, mbody), w2[0], mid_h, g)
    title = "CANARY + HEALTH" if snap.get("canary") else "HEALTH"
    side = panel(title, _side_body(snap, w2[1] - 2, mbody)[:mbody], w2[1], mid_h, g)
    out += join([orders, side], w2, mid_h)
    remaining -= mid_h
    if remaining >= 3:
        out += panel("EVENTS", _events_body(snap.get("events") or [], cols - 2, remaining - 2), cols, remaining, g)
    return out


def plain_line(snap: dict) -> str:
    """One status line for a non-terminal --dash (a log file, systemd)."""
    t = snap.get("trigger") or {}
    m = snap.get("market") or {}
    a = snap.get("account") or {}
    left = _num(m.get("seconds_left"))
    return terminal_text(
        f"[DASH] {snap.get('mode')} {_text(m.get('slug'), 'no-round')} T-{left:.0f}s "
        if left is not None else f"[DASH] {snap.get('mode')} {_text(m.get('slug'), 'no-round')} T-? ", 2000) + \
        terminal_text(f"btc={_text(t.get('status'))} mid={t.get('mid') or MISSING} signals={count(t.get('signals'))} "
                      f"orders={count(t.get('orders'))} filled={count(t.get('filled'))} "
                      f"pnl={money(a.get('pnl') if a.get('kind') != 'LIVE' else a.get('realized'), sign=True)}", 2000)
