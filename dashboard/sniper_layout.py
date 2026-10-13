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
                slip, send_p50_ms, ack_p50_ms, ack_simulated, errors, in_flight
    pm          position_manager.PositionManager.snapshot(): engine, live_orders,
                shadow, orders, spend, sides {UP/DOWN: shares, avg, cost, best_bid,
                best_ask, pending, new_avg_if_buy, state WAIT/IMPROVE/BLOCKED,
                reason}, portfolio {combined_avg, paired, unpaired_up/down,
                total_cost, pending, fees, exposure}
    exec        latency {metric: {n, last, avg, p50, p95, p99, max, simulated}}
                (exec_latency.STAGES + TOTALS), quality (exec_latency.ExecQuality
                summary), paper
    budget      exec_latency.latency_budget(): primary, feeds {feed: ms}, bot_ms,
                one_way_ms, hold_ms, venue_ms, venue_simulated, totals {feed: ms}
    market      slug, seconds_left, trade_until_s, book_status, up / down
                ({bid, ask, status, age_ms}), round_fills, max_fills,
                round_up_sh, round_down_sh, max_side_shares
    orders      [{time, side, bp, ask, cap, result, shares, price, detail}],
                newest last
    account     kind, cash, equity, pnl, realized, open, pending_cost, fees,
                wins, losses, start, balance, balance_status, position_value,
                unrealized, total, awaiting_cost, redeemable (LIVE)
    positions   rows [{side, shares, avg, cost, bid, value, pnl}] (the market
                trading now), pending {markets, shares, cost} (ended markets
                awaiting their result, carried at cost)
    settlement  SettlementWorker.panel(): active, subject, pending [...],
                previous [...], last_settled, pending_count, redeem_pending;
                each market {label, state, age_s, winner, settlement_price,
                realized, settlement_s, redeem, redeem_amount, detail, ...}
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
PENDING_S = "PENDING SETTLEMENT"
# A market's lifecycle state, as text on paper inside panels and as a badge in the header.
STATE_FG = {"ACTIVE": Style("green", bold=True), "EXPIRED": Style("amber", bold=True),
            PENDING_S: Style("amber", bold=True), "SETTLING": Style("blue", bold=True),
            "SETTLED": Style("ink", bold=True), "DISCOVERING": Style("faint")}
STATE_BADGE = {"ACTIVE": Style("white", "green", bold=True), "EXPIRED": Style("ink", "amber", bold=True),
               PENDING_S: Style("ink", "amber", bold=True), "SETTLING": Style("white", "blue", bold=True),
               "SETTLED": Style("white", "rule", bold=True)}
REDEEM_FG = {"REDEEM PENDING": Style("amber", bold=True), "REDEEMED": Style("green", bold=True),
             "REDEMPTION UNVERIFIED": Style("amber", bold=True)}
LEVEL_STYLE = {"good": Style("green"), "warn": Style("amber"), "bad": Style("red", bold=True),
               "info": Style("ink")}
# The NET line: (green up to, amber up to) milliseconds per path, red above.
# Coinbase is the trigger feed's delay (receipt - the match's own time); the
# CLOB and Binance are round trips. Binance sits in Tokyo: from Europe a
# round trip near 200ms is normal, so its limits are its own.
NET_LIMITS = {"coinbase": (80.0, 150.0), "clob": (40.0, 80.0), "binance": (250.0, 400.0)}
NET_ORDER = ("coinbase", "clob", "binance")
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


def net_style(key: str, ms) -> Style:
    """Green / amber / red for one NET path by its own limits; faint when unmeasured."""
    v = _num(ms)
    if v is None or key not in NET_LIMITS:
        return FAINT
    green, amber = NET_LIMITS[key]
    return Style("green" if v <= green else "amber" if v <= amber else "red", bold=True)


NET_SHORT = {"coinbase": "cb", "clob": "clob", "binance": "bn"}


def _net_row(net: dict, cols: int) -> Row:
    """NET ms  coinbase 48  clob 24  binance 212 - each value coloured by its
    own limits; the names shorten (cb / clob / bn) when the panel is narrow."""
    def runs(names):
        out = [(fit("NET ms", 9), DIM), (" ", PAPER)]
        for key in NET_ORDER:
            v = _num(net.get(key))
            out += [(f"{names[key]} ", FAINT), (MISSING if v is None else f"{v:.0f}", net_style(key, v)),
                    ("  ", PAPER)]
        return out
    full = runs({k: k for k in NET_ORDER})
    if sum(len(t) for t, _ in full) - 2 > cols:
        full = runs(NET_SHORT)
    return _runs(full, cols)


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
    elif mode == "PAPER" and cols >= 140:
        parts += [(" ", PAPER), (" NO REAL ORDERS · $0 AT RISK ", Style("ink", "paper2", bold=True))]
    parts += [("  ", PAPER), (_text(market.get("slug"), "no round yet")[:40], Style("ink", bold=True)),
              ("  ", PAPER), (f"T-{left:.0f}s" if left is not None else "T-?", Style("blue", bold=True))]
    st = snap.get("settlement") if isinstance(snap.get("settlement"), dict) else {}
    active = st.get("active") if isinstance(st.get("active"), dict) else {}
    state = _text(active.get("state"), "DISCOVERING")
    parts += [("  ", PAPER), (f" {state} ", STATE_BADGE.get(state, Style("ink", "paper2", bold=True)))]
    waiting = _num(st.get("pending_count"))
    if waiting:
        parts += [(" ", PAPER), (f" {int(waiting)} PENDING SETTLEMENT ", STATE_BADGE[PENDING_S])]
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
    fair = t.get("fair") if isinstance(t.get("fair"), dict) else None
    rows = [
        _runs([(fit("feed", 9), DIM), (" ", PAPER), (f"{feed} ", Style("ink", bold=True)),
               (status, state_fg(status)),
               (f"  busy {t['in_flight']}" if isinstance(t.get("in_flight"), str) else "  busy" if t.get("in_flight")
                else "", Style("blue"))], w),
        _runs([(fit("BTC mid", 9), DIM), (" ", PAPER),
               (f"{_num(t['mid']):,.2f}" if _num(t.get("mid")) is not None else MISSING, Style("ink", bold=True)),
               # the trigger feed at the round's start, when this process saw it begin (approximate:
               # the market's strike is Chainlink's price); N/A after joining mid-round
               ((f"  start ~{_num(t['btc_start']):,.0f}" if _num(t.get("btc_start")) is not None
                 else "  start N/A") if "btc_start" in t else "", FAINT)], w),
    ]
    if fair is not None:
        # the fair-value gate (SNIPER_FAIR): its volatility and the last decision -
        # the market's mid -> the anchored fair value, the limit, the Kelly size
        vol = _num(fair.get("vol_bp"))
        last = fair.get("last") if isinstance(fair.get("last"), dict) else None
        c = lambda x: f"{_num(x):.2f}".lstrip("0") if _num(x) is not None else MISSING  # noqa: E731
        if vol is None:
            rows.append(_runs([(fit("fair", 9), DIM), (" ", PAPER),
                               (f"unknown: {_text(fair.get('why'))} - no buys", Style("amber"))], w))
        else:
            parts = [(fit("fair", 9), DIM), (" ", PAPER), (f"vol {vol:.2f}bp", FAINT)]
            if last:
                sh = _num(last.get("shares"))
                parts += [("  ", PAPER), (_text(last.get("side")), Style("green" if last.get("side") == "UP" else "red", bold=True)),
                          (f" {c(last.get('market_mid'))}->{c(last.get('fair'))}", Style("ink", bold=True)),
                          (f" <={c(last.get('limit'))}", DIM), (f" {sh:g}sh" if sh else "", Style("ink"))]
            rows.append(_runs(parts, w))
    rows += [
        _runs([(fit("move", 9), DIM), (" ", PAPER), (bp(move), Style("red", bold=True) if hot else Style("ink")),
               (f" /{window:.0f}ms" if window is not None else "", DIM),
               (f" k{k:g}" if k is not None else "", FAINT)], w),
        # signals, their rate and the last one on one line (frees a line for the fair value)
        _runs([(fit("signals", 9), DIM), (" ", PAPER), (count(t.get("signals")), Style("ink", bold=True)),
               (f"  {rate:.1f}/min" if rate is not None else "", DIM),
               ("  last ", DIM) if t.get("last_signal_bp") is not None else ("", DIM),
               (bp(t.get("last_signal_bp")) if t.get("last_signal_bp") is not None else "", Style("ink")),
               (f" {age:.0f}s ago" if age is not None and t.get("last_signal_bp") is not None else "", DIM)], w),
        _runs([(fit("orders", 9), DIM), (" ", PAPER), (count(t.get("orders")), Style("ink", bold=True)),
               ("  fill ", DIM), (count(t.get("filled")), Style("green", bold=True)),
               ("  miss ", DIM), (count(t.get("refused")), Style("amber")),
               (f"  {fr * 100:.0f}%" if fr is not None else "", DIM)], w),
        _runs([(fit("markout", 9), DIM), (" ", PAPER), ("1s ", DIM),
               (cents(t.get("markout_1s_c")), pnl_style(_num(t.get("markout_1s_c")))), ("  5s ", DIM),
               (cents(t.get("markout_5s_c")), pnl_style(_num(t.get("markout_5s_c"))))], w),
        _runs([(fit("order", 9), DIM), (" ", PAPER), (_text(t.get("order_type")), Style("ink")),
               (f" +{_num(t['slip']):.2f}" if _num(t.get("slip")) is not None else "", DIM),
               # measured trigger -> send and -> ACK (p50); paper's ACK is the model's
               (f" snd {_lat(t.get('send_p50_ms'))}", Style("ink")),
               (f" ack {_lat(t.get('ack_p50_ms'))}{' sim' if t.get('ack_simulated') else ''}", DIM),
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
    carry = m.get("carry") if isinstance(m.get("carry"), dict) else {}

    def carried(side):
        c = carry.get(side) if isinstance(carry.get(side), dict) else {}
        n, nxt = _num(c.get("carry")), _num(c.get("next"))
        return MISSING if n is None else f"{n:g} -> {nxt:g}" if nxt is not None else f"{n:g}"

    rows = [
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
    if carry:      # IOC: unfilled shares waiting for the next same-side signal -> that order's target
        rows.append(pair("carry", carried("UP"), carried("DOWN"),
                         Style("amber", bold=True) if (_num((carry.get("UP") or {}).get("carry")) or 0) else Style("ink"),
                         Style("amber", bold=True) if (_num((carry.get("DOWN") or {}).get("carry")) or 0) else Style("ink")))
    return rows


def _account_body(a: dict, w: int) -> list[Row]:
    """CASH, POSITION VALUE, EQUITY, REALIZED, UNREALIZED and TOTAL P/L.
    Positions awaiting their result are in POSITION VALUE at cost; LIVE also
    shows REDEEMABLE - settled winnings that are not collateral until redeemed."""
    kind = str(a.get("kind") or "").upper()
    full = w >= 30
    lw = 14 if full else 9
    realized, unreal = _num(a.get("realized")), _num(a.get("unrealized"))
    total = _num(a.get("total")) if a.get("total") is not None else _num(a.get("pnl") if kind != "LIVE" else None)

    def row(full_label, short, value, style=Style("ink")):
        return _line(full_label if full else short, value, w, style, lw=lw)

    rows = []
    if kind == "LIVE":
        rows.append(row("COLLATERAL", "collatrl", money(a.get("balance")), Style("ink", bold=True)))
        redeem = _num(a.get("redeemable"))
        rows.append(row("REDEEMABLE", "redeemabl", money(redeem),
                        Style("amber", bold=True) if redeem else Style("ink")))
    else:
        rows.append(row("CASH", "cash", money(a.get("cash")), Style("ink", bold=True)))
    rows += [
        row("POSITION VALUE", "pos value", money(a.get("position_value"))),
        row("EQUITY", "equity", money(a.get("equity")), Style("ink", bold=True)),
        row("REALIZED P/L", "realized", money(realized, sign=True), pnl_style(realized)),
        row("UNREALIZED P/L", "unrealzd", money(unreal, sign=True), pnl_style(unreal)),
        row("TOTAL P/L", "total P/L", money(total, sign=True), pnl_style(total, big=True)),
    ]
    if kind == "LIVE":
        rows.append(row("BALANCE CHECK", "bal check", _text(a.get("balance_status")),
                        state_fg(str(a.get("balance_status") or ""))))
    else:
        rows.append(row("FEES  WON/LOST", "fees", f"{money(a.get('fees'))}  {count(a.get('wins'))}/"
                        f"{count(a.get('losses'))}", Style("ink")))
    return rows


def _orders_body(orders: list, w: int, h: int) -> list[Row]:
    widths = [8, 4, 7, 5, 5, 7, 13]
    head_txt = ["TIME", "SIDE", "BTC", "ASK", "LIMIT", "RESULT", "FILLED"]
    used = sum(widths) + len(widths)
    detail_w = max(0, w - used)
    out = [_runs([(fit(t, n) + " ", Style("dim", bold=True)) for t, n in zip(head_txt, widths)]
                 + ([(fit("DETAIL", detail_w), Style("dim", bold=True))] if detail_w else []), w)]
    if not orders:
        out.append(_runs([("  no orders yet in this market", FAINT)], w))
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
        out.append(_line("risk", f"{money(c.get('loss'))}/{money(c.get('max_loss'))}"
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
    net = snap.get("net")
    if isinstance(net, dict):
        out.append(_net_row(net, w))
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


def _px(v) -> str:
    """A share price as the position table shows it: .460, 1.000, --."""
    f = _num(v)
    if f is None:
        return MISSING
    return f"{f:.3f}"[1:] if 0 <= f < 1 else f"{f:.3f}"


def _lat(v) -> str:
    """A latency in ms: 0.41ms, 12.3ms, 190ms; N/A without a sample."""
    f = _num(v)
    if f is None:
        return "N/A"
    return f"{f:.2f}ms" if f < 10 else f"{f:.1f}ms" if f < 100 else f"{f:.0f}ms"


LAT_STAGES = (("feed_to_trigger", "feed>trig"), ("trigger_to_decision", "decide"), ("build", "build"),
              ("sign", "sign"), ("dispatch", "dispatch"), ("send_to_ack", "net>ACK"), ("ack_to_fill", "ACK>fill"))
LAT_TOTALS = (("trigger_to_send", "TRIG>SEND"), ("trigger_to_ack", "TRIG>ACK"), ("trigger_to_fill", "TRIG>FILL"))


def _latency_body(lat: dict, w: int) -> list[Row]:
    """SNIPER EXECUTION LATENCY: every stage (p50), then LAST / AVG / P50 /
    P95 / P99 / MAX for trigger -> send, -> ACK, -> fill known. 'sim': the
    paper model's venue answer, not a measurement."""
    lat = lat if isinstance(lat, dict) else {}

    def p50(key):
        d = lat.get(key) if isinstance(lat.get(key), dict) else {}
        return _lat(d.get("p50")) + (" sim" if d.get("simulated") else "")
    parts = ["p50"] + [f"{label} {p50(key)}" for key, label in LAT_STAGES]
    rows, line = [], ""
    for part in parts:                                  # wrap the stage list to the panel
        if line and len(line) + 2 + len(part) > w:
            rows.append(_runs([(line, Style("ink"))], w))
            line = part
        else:
            line = f"{line}  {part}" if line else part
    if line:
        rows.append(_runs([(line, Style("ink"))], w))
    rows = rows[:2]
    cw = max(6, min(9, (w - 11) // 7))
    rows.append(_runs([(fit("ms", 10), FAINT)] + [(fit(h, cw, ">"), Style("dim", bold=True))
                                                        for h in ("LAST", "AVG", "P50", "P95", "P99", "MAX", "n")], w))
    for key, label in LAT_TOTALS:
        d = lat.get(key) if isinstance(lat.get(key), dict) else {}
        vals = [_lat(d.get(k)).replace("ms", "") for k in ("last", "avg", "p50", "p95", "p99", "max")]
        vals.append(count(d.get("n")) if d else "N/A")
        rows.append(_runs([(fit(label + (" sim" if d.get("simulated") else ""), 10), Style("ink", bold=True))]
                          + [(fit(v, cw, ">"), Style("ink")) for v in vals], w))
    return rows


def _quality_body(q: dict, w: int) -> list[Row]:
    q = q if isinstance(q, dict) else {}
    last = q.get("last") if isinstance(q.get("last"), dict) else {}
    pct = lambda v: "N/A" if _num(v) is None else f"{_num(v) * 100:.0f}%"  # noqa: E731
    mid = _num(last.get("signal_mid"))
    slip = _num(last.get("slip_c"))
    avg = _num(q.get("avg_slip_c"))
    return [
        _runs([(f"attempted {count(q.get('attempted'))}  sent {count(q.get('sent'))}  "
                f"acked {count(q.get('acked'))}  pending {count(q.get('pending'))}", Style("ink"))], w),
        _runs([("filled ", DIM), (count(q.get("filled")), Style("green", bold=True)),
               (" partial ", DIM), (count(q.get("partial")), Style("green")),
               (" zero ", DIM), (count(q.get("zero_fill")), Style("amber")),
               (" rej ", DIM), (count(q.get("rejected")), Style("red")),
               (" fail ", DIM), (count(q.get("failed")), Style("red", bold=True))], w),
        _runs([("fill rate ", DIM), (pct(q.get("fill_rate")), Style("ink", bold=True)),
               ("  reject rate ", DIM), (pct(q.get("reject_rate")), Style("ink"))], w),
        _runs([("last  signal ", DIM), (f"{mid:,.2f}" if mid is not None else "N/A", Style("ink")),
               ("  ref ask ", DIM), (_px(last.get("ask_seen")), Style("ink")),
               ("  limit ", DIM), (_px(last.get("cap")), Style("ink"))], w),
        _runs([("      avg fill ", DIM), (_px(last.get("price")), Style("ink")),
               ("  slip ", DIM), ("N/A" if slip is None else f"{slip:+.1f}c", pnl_style(None if slip is None else -slip))], w),
        _runs([("avg slippage ", DIM), ("N/A" if avg is None else f"{avg:+.2f}c", Style("ink")),
               (f"  over {count(q.get('slip_n'))} fills" if q.get("slip_n") else "", FAINT)], w),
    ]


STATE_PM = {"IMPROVE": Style("green", bold=True), "BLOCKED": Style("amber", bold=True), "WAIT": Style("faint")}


def _pm_side_body(sd: dict, w: int) -> list[Row]:
    sd = sd if isinstance(sd, dict) else {}
    sh = _num(sd.get("shares"))
    avg = _num(sd.get("avg"))
    # a missing value says why: STALE (its book), WAITING (no market / state busy),
    # ERROR (the last evaluation failed), else N/A - never a bare "--"
    blank = _text(sd.get("blank"), "N/A") if sd.get("blank") in ("N/A", "WAITING", "STALE", "ERROR") else "N/A"

    def money4(v):
        f = _num(v)
        return blank if f is None else f"${f:.4f}"
    # The manager's averages include fees ((notional + fees) / shares); the
    # positions table's AVG is the price paid without fees (the ledger's avg_price).
    rows = [
        _line("Shares", "0.00" if sh is None else f"{sh:.2f}", w, Style("ink", bold=True), lw=12),
        _line("Avg+fees", money4(avg) if sh else "N/A", w, Style("ink"), lw=12),
        _line("Cost", money(sd.get("cost")) if sh else "$0.00", w, Style("ink"), lw=12),
        _line("Best Bid", money4(sd.get("best_bid")), w, Style("ink"), lw=12),
        _line("Best Ask", money4(sd.get("best_ask")), w, Style("ink"), lw=12),
        _line("Pending", f"{_num(sd.get('pending')) or 0:.2f}", w, Style("ink"), lw=12),
        _line("New Avg+fees", money4(sd.get("new_avg_if_buy")) if sd.get("new_avg_if_buy") is not None else "N/A",
              w, Style("ink"), lw=12),
    ]
    state = _text(sd.get("state"), "WAIT")
    reason = _text(sd.get("reason"), "")
    rows.append(_runs([(fit("Opportunity", 12), DIM), (" ", PAPER), (state, STATE_PM.get(state, Style("ink")))], w))
    rows.append(_runs([(" " * 13, PAPER), (reason if reason and state != "WAIT" else "", FAINT)], w))
    return rows


def _pm_portfolio_body(pm: dict, w: int) -> list[Row]:
    pf = pm.get("portfolio") if isinstance(pm.get("portfolio"), dict) else {}

    def n2(v):
        f = _num(v)
        return "0.00" if f is None else f"{f:.2f}"
    comb = _num(pf.get("combined_avg"))
    mode = ("SHADOW (no orders)" if pm.get("shadow") else
            "LIVE ORDERS" if pm.get("live_orders") else "REAL-TIME PAPER")
    unknown = int(_num(pm.get("unknown_orders")) or 0)
    trade, block = pm.get("last_trade"), pm.get("last_block")
    activity = [
        _line("Activity", f"eval {count(pm.get('evaluations'))}  opp {count(pm.get('opportunities'))}  "
              f"ord {count(pm.get('orders_total'))}  fill {count(pm.get('fills_total'))}", w, Style("ink"), lw=13),
        _line("Last trade", f"{trade[1]} {trade[2]}" if trade else "N/A", w, Style("green"), lw=13),
        _line("Last block", f"{block[1]} {block[2]}" if block else "N/A", w, FAINT, lw=13)]
    return [
        _line("Comb Avg+fees", ("WAITING" if not pf else "N/A") if comb is None else f"${comb:.4f}", w,
              (Style("green", bold=True) if comb is not None and comb < 1 else Style("red", bold=True))
              if comb is not None else FAINT, lw=13),
        _line("Paired", n2(pf.get("paired")), w, Style("ink"), lw=13),
        _line("Unpaired UP", n2(pf.get("unpaired_up")), w, Style("ink"), lw=13),
        _line("Unpaired DOWN", n2(pf.get("unpaired_down")), w, Style("ink"), lw=13),
        _line("Total Cost", money(pf.get("total_cost") or 0.0), w, Style("ink"), lw=13),
        _line("Pending Exp", money(pf.get("pending") or 0.0), w, Style("ink"), lw=13),
        _line("Fees", money(pf.get("fees") or 0.0), w, Style("ink"), lw=13),
        _line("Orders", f"{count(pm.get('orders'))}/{count(pm.get('max_orders'))}  spend "
              f"{money(pm.get('spend'))}/{money(pm.get('max_spend'))}", w, Style("ink"), lw=13),
        _runs([(mode, Style("amber") if pm.get("shadow") else DIM),
               (f"  {unknown} UNKNOWN (reconciliation required)" if unknown else "", Style("red", bold=True))], w),
    ] + activity


def _manager_band(snap: dict, cols: int, h: int, g: Glyphs) -> list[Row]:
    pm = snap.get("pm") if isinstance(snap.get("pm"), dict) else {}
    sides = pm.get("sides") if isinstance(pm.get("sides"), dict) else {}
    body_h = max(0, h - 2)
    if cols < 90:
        up = (sides.get("UP") or {}) if isinstance(sides.get("UP"), dict) else {}
        dn = (sides.get("DOWN") or {}) if isinstance(sides.get("DOWN"), dict) else {}
        rows = [_runs([("UP   ", Style("green", bold=True)), (_text(up.get("state"), "WAIT"),
                                                              STATE_PM.get(_text(up.get("state"), "WAIT"), Style("ink"))),
                       (f" {_text(up.get('reason'), '')}", FAINT)], cols - 2),
                _runs([("DOWN ", Style("red", bold=True)), (_text(dn.get("state"), "WAIT"),
                                                            STATE_PM.get(_text(dn.get("state"), "WAIT"), Style("ink"))),
                       (f" {_text(dn.get('reason'), '')}", FAINT)], cols - 2)]
        return panel("POSITION MANAGER", (rows + _pm_portfolio_body(pm, cols - 2))[:body_h], cols, h, g)
    w = hsplit(cols, (1.0, 1.0, 1.0), (30, 30, 30))
    return join([panel("POSITION MANAGER  UP", _pm_side_body(sides.get("UP"), w[0] - 2)[:body_h], w[0], h, g),
                 panel("DOWN", _pm_side_body(sides.get("DOWN"), w[1] - 2)[:body_h], w[1], h, g),
                 panel("PORTFOLIO", _pm_portfolio_body(pm, w[2] - 2)[:body_h], w[2], h, g)], w, h)


def _pnl_body(acct: dict, w: int) -> list[Row]:
    """PAPER P/L by source: every figure from simulated fills on the real books."""
    attr = acct.get("attribution") if isinstance(acct.get("attribution"), dict) else {}
    cols = (("SOURCE", 9), ("FILLS", 6), ("SPEND", 10), ("FEES", 8), ("REALIZED", 10), ("UNREAL", 9),
            ("TOTAL", 10), ("W/L", 7), ("WIN%", 5))
    head = _runs([(fit(n, k), DIM) for n, k in cols], w)
    rows = [head]
    tot = {"fills": 0, "spend": 0.0, "fees": 0.0, "realized": 0.0, "unrealized": 0.0, "total": 0.0, "wins": 0,
           "losses": 0}
    names = [n for n in ("sniper", "manager") if n in attr] + [n for n in attr if n not in ("sniper", "manager")]
    for name in names or ["sniper"]:
        a = attr.get(name) or {}
        for k in tot:
            tot[k] += a.get(k) or 0
        rows.append(_pnl_row(name.upper(), a, cols, w))
    rows.append(_pnl_row("COMBINED", tot, cols, w, bold=True))
    rows.append(_runs([("capital ", DIM), (money(tot["spend"]), Style("ink")),
                       ("  reserved ", DIM), (money(acct.get("reserved") or 0.0), Style("ink")),
                       ("  max exposure ", DIM), (money(acct.get("max_exposure")), Style("ink")),
                       ("  max drawdown ", DIM), (money(acct.get("max_drawdown")), Style("ink")),
                       ("  (this run)", FAINT)], w))
    return rows


def _pnl_row(label, a, cols, w, bold=False):
    total = _num(a.get("total"))
    won, lost = int(a.get("wins") or 0), int(a.get("losses") or 0)
    vals = [label, f"{int(a.get('fills') or 0)}", money(a.get("spend") or 0.0), money(a.get("fees") or 0.0),
            money(a.get("realized") or 0.0, sign=True), money(a.get("unrealized") or 0.0, sign=True),
            money(total or 0.0, sign=True), f"{won}/{lost}",
            f"{100.0 * won / (won + lost):.0f}%" if won + lost else "N/A"]
    st = Style("green", bold=bold) if (total or 0) > 0 else Style("red", bold=bold) if (total or 0) < 0 else Style("ink", bold=bold)
    return _runs([(fit(v, k), st if i in (4, 6) else Style("ink", bold=bold)) for i, (v, (_n, k)) in
                  enumerate(zip(vals, cols))], w)


FEED_NAME = {"coinbase": "Coinbase feed", "binance": "Binance feed", "kraken": "Kraken feed",
             "binance_fut": "Bin fut feed"}
FEED_SHORT = {"coinbase": "cb", "binance": "bn", "kraken": "kr", "binance_fut": "bin fut"}


def _ms_short(v) -> str:
    f = _num(v)
    return "N/A" if f is None else f"{f:.1f}ms" if f < 10 else f"{f:.0f}ms"


def _budget_body(b: dict, w: int, g: Glyphs) -> list[Row]:
    """LATENCY BUDGET: BTC trade -> our order matched, p50 parts with their
    share of the trigger feed's total. N/A where not measured yet; 'sim' marks
    paper's modelled venue time."""
    b = b if isinstance(b, dict) else {}
    feeds = b.get("feeds") if isinstance(b.get("feeds"), dict) else {}
    totals = b.get("totals") if isinstance(b.get("totals"), dict) else {}
    primary = _text(b.get("primary"), "")
    parts = [(FEED_NAME.get(primary, f"{primary} feed"), feeds.get(primary), Style("blue")),
             ("Bot in-process", b.get("bot_ms"), Style("green")),
             ("Network > CLOB", b.get("one_way_ms"), Style("amber")),
             ("Polymarket hold", b.get("hold_ms"), Style("dim")),
             ("Venue match" + (" sim" if b.get("venue_simulated") else ""), b.get("venue_ms"), Style("purple"))]
    total = _num(totals.get(primary))
    known = sum(_num(v) or 0.0 for _, v, _ in parts)
    base = total if total else known
    lw, vw, pw = 16, 6, 4
    bw = max(4, w - lw - vw - pw - 3)
    rows = []
    for label, v, style in parts:
        f = _num(v)
        frac = max(0.0, f / base) if f is not None and base else 0.0
        shown = max(frac, 1.0 / (bw * 8)) if frac > 0 else 0.0      # a part > 0 always shows a sliver
        pct = "" if f is None or not base else "<1%" if 0 < frac < 0.005 else f"{frac * 100:.0f}%"
        rows.append(_runs([(fit(label, lw), DIM), (" ", PAPER)] + bar(shown, bw, g, style)
                          + [(" ", PAPER), (fit(_ms_short(f), vw, ">"), Style("ink", bold=True)),
                             (" ", PAPER), (fit(pct, pw, ">"), FAINT)], w))
    head = (f"{total:.0f}ms" if total is not None else f">={known:.0f}ms" if known else "N/A")
    tail = []
    for k in feeds:
        if k != primary:
            tail += [(f"  via {FEED_SHORT.get(k, k)} ", FAINT), (_ms_short(totals.get(k)), Style("ink"))]
    rows.append(_runs([(fit("BTC > MATCHED", lw), Style("ink", bold=True)), (" ", PAPER),
                       (head, Style("ink", bold=True))] + tail, w))
    return rows


def _latency_band(snap: dict, cols: int, h: int, g: Glyphs) -> list[Row]:
    ex = snap.get("exec") if isinstance(snap.get("exec"), dict) else {}
    title = "SNIPER EXECUTION LATENCY" + (" (paper: ACK/fill simulated)" if ex.get("paper") else "")
    body_h = max(0, h - 2)
    if cols < 100:
        return panel(title, _latency_body(ex.get("latency"), cols - 2)[:body_h], cols, h, g)
    budget = snap.get("budget") if isinstance(snap.get("budget"), dict) else None
    if budget is not None and cols >= 150:
        w = hsplit(cols, (1.4, 1.15, 0.85), (66, 46, 34))
        return join([panel(title, _latency_body(ex.get("latency"), w[0] - 2)[:body_h], w[0], h, g),
                     panel("LATENCY BUDGET  BTC trade > matched (p50)", _budget_body(budget, w[1] - 2, g)[:body_h],
                           w[1], h, g),
                     panel("EXECUTION QUALITY", _quality_body(ex.get("quality"), w[2] - 2)[:body_h], w[2], h, g)],
                    w, h)
    w = hsplit(cols, (1.6, 1.0), (66, 34))
    return join([panel(title, _latency_body(ex.get("latency"), w[0] - 2)[:body_h], w[0], h, g),
                 panel("EXECUTION QUALITY", _quality_body(ex.get("quality"), w[1] - 2)[:body_h], w[1], h, g)], w, h)


def _secs(v) -> str:
    f = _num(v)
    if f is None:
        return MISSING
    return f"{f:.1f}s" if f < 100 else f"{f:.0f}s"


def _position_body(p: dict, w: int) -> list[Row]:
    """SIDE SHARES AVG COST BID VALUE P/L for the market trading now; one
    line for ended markets still awaiting their result."""
    full = [("SIDE", 5, "<"), ("SHARES", 8, ">"), ("AVG", 6, ">"), ("COST", 7, ">"), ("BID", 6, ">"),
            ("VALUE", 8, ">"), ("P/L", 8, ">")]
    cols = full if w >= sum(n for _, n, _ in full) else [c for c in full if c[0] in ("SIDE", "SHARES", "COST", "VALUE", "P/L")]
    if w < sum(n for _, n, _ in cols):
        cols = [c for c in cols if c[0] in ("SIDE", "SHARES", "P/L")]
    def cells(values, styles):      # each column after SIDE keeps one space before its value
        return _runs([((" " if i else "") + fit(values[t], n - (1 if i else 0), a), styles.get(t, Style("ink")))
                      for i, (t, n, a) in enumerate(cols)], w)

    out = [cells({t: t for t, _, _ in cols}, {t: Style("dim", bold=True) for t, _, _ in cols})]
    rows = p.get("rows") if isinstance(p.get("rows"), list) else []
    if not rows:
        rows = [{"side": "UP"}, {"side": "DOWN"}]
    for r in rows[:2]:
        r = r if isinstance(r, dict) else {}
        side = _text(r.get("side"))
        shares = _num(r.get("shares"))
        value = {"SIDE": side, "SHARES": MISSING if shares is None else f"{shares:.3f}", "AVG": _px(r.get("avg")),
                 "COST": money(r.get("cost")), "BID": _px(r.get("bid")), "VALUE": money(r.get("value")),
                 "P/L": money(r.get("pnl"), sign=True)}
        style = {"SIDE": Style("green" if side == "UP" else "red", bold=True),
                 "P/L": pnl_style(_num(r.get("pnl"))), "SHARES": Style("ink", bold=bool(shares))}
        out.append(cells(value, style))
    pend = p.get("pending") if isinstance(p.get("pending"), dict) else {}
    n = _num(pend.get("markets"))
    if n:
        out.append(_runs([("awaiting ", DIM), (f"{int(n)} mkt ", Style("amber", bold=True)),
                          (f"{_num(pend.get('shares')) or 0:.3f}sh ", Style("ink")),
                          (f"cost {money(pend.get('cost'))} (held at cost until the result)", DIM)], w))
    return out


def _settlement_body(st: dict, w: int) -> tuple[list[Row], str]:
    """The SETTLEMENT block about one market (the panel title names it):
    the oldest still waiting for its result, else one just settled, else the
    one trading now. Returns (rows, market label)."""
    subject = st.get("subject") if isinstance(st.get("subject"), dict) else {}
    last = st.get("last_settled") if isinstance(st.get("last_settled"), dict) else {}
    state = _text(subject.get("state"), "DISCOVERING")
    lw = 13

    def row(label, value, style=Style("ink")):
        return _line(label, value, w, style, lw=lw)

    rows = [row("MARKET STATUS", state, STATE_FG.get(state, Style("ink", bold=True)))]
    if state in (PENDING_S, "SETTLING", "EXPIRED") and not subject.get("winner"):
        rows.append(row("RESULT", "WAITING" if state != "EXPIRED" or not subject.get("detail") else "NONE",
                        Style("amber", bold=True)))
        rows.append(row("PENDING AGE", _secs(subject.get("age_s")), Style("ink", bold=True)))
        why = subject.get("error") or subject.get("detail")
        if why:
            rows.append(row("WAITING ON", _text(why), Style("red") if subject.get("error") else FAINT))
        more = (_num(st.get("pending_count")) or 0) - 1
        if more > 0:
            rows.append(row("ALSO PENDING", f"{int(more)} more market(s)", Style("amber")))
    elif state in ("SETTLING", "SETTLED") or subject.get("winner"):
        winner = _text(subject.get("winner"))
        rows.append(row("WINNER", winner, state_fg(winner) if winner in ("UP", "DOWN") else Style("ink", bold=True)))
        if state == "SETTLED":
            realized = _num(subject.get("realized"))
            rows.append(row("SETTLE PRICE", money(subject.get("settlement_price"))))
            rows.append(row("REALIZED P/L", money(realized, sign=True) if subject.get("has_position") is not False
                            else "$0.00 (no position)", pnl_style(realized)))
            rows.append(row("SETTLE TIME", _secs(subject.get("settlement_s")), Style("ink", bold=True)))
            redeem = _text(subject.get("redeem"))
            amount = _num(subject.get("redeem_remaining_amount", subject.get("redeem_amount")))
            shown = ("PENDING" if redeem == "REDEEM PENDING" else
                     "UNVERIFIED" if redeem == "REDEMPTION UNVERIFIED" else redeem)
            rows.append(row("REDEEM", shown + (f" {money(amount)}" if amount and redeem != "N/A (PAPER)" else ""),
                            REDEEM_FG.get(redeem, FAINT)))
            unverified = _num(subject.get("redeem_unverified_amount"))
            if unverified:
                rows.append(row("UNVERIFIED", money(unverified) + " collateral not verified", Style("amber")))
        else:
            rows.append(row("PENDING AGE", _secs(subject.get("age_s"))))
    else:
        rows.append(row("RESULT", MISSING, FAINT))
        rows.append(row("PENDING AGE", MISSING, FAINT))
    if not last or last is subject or last.get("condition_id") == subject.get("condition_id"):
        rows.append(row("LAST SETTLED", MISSING if not last or last is not subject else "this market", FAINT))
    else:
        tail = f" {_text(last.get('winner'))} {money(last.get('realized'), sign=True)}"
        name = _text(last.get("label"))
        if len(name) + len(tail) > w - lw - 1:
            name = name.split("-")[0]                     # "14:00" when "14:00-14:05" does not fit
        rows.append(row("LAST SETTLED", name + tail, pnl_style(_num(last.get("realized")))))
    return rows, _text(subject.get("label"), "")


def _market_line(v: dict, w: int, now_left=None) -> Row:
    state = _text(v.get("state"))
    text = _text(v.get("label"))
    wide = w >= 33
    label = fit(text, 11) if wide else fit(text.split("-")[0], 5)      # "14:00-14:05", or "14:00" when narrow
    if state == "ACTIVE":
        tail = [("  ", PAPER), (f"T-{now_left:.0f}s" if now_left is not None else "", Style("blue"))]
        shown = "ACTIVE"
    elif state in (PENDING_S, "SETTLING") or (state == "EXPIRED" and not v.get("detail")):
        shown = ("WAITING RESULT" if wide else "WAITING") if state != "SETTLING" else "SETTLING"
        tail = [(" ", PAPER), (_secs(v.get("age_s")), Style("ink", bold=True))]
    elif state == "SETTLED":
        shown = f"SETTLED {_text(v.get('winner'))[:4]}"
        pnl = _num(v.get("realized"))
        redeem = v.get("redeem")
        tail = [(" ", PAPER), (money(pnl, sign=True) if v.get("has_position") else "no pos",
                               pnl_style(pnl) if v.get("has_position") else FAINT)]
        if redeem in REDEEM_FG:
            tail += [(" ", PAPER), ("REDEEM" if redeem == "REDEEM PENDING" else "redeemed", REDEEM_FG[redeem])]
    else:
        shown, tail = f"{state} no result", []
    return _runs([(label + " ", Style("ink")), (fit(shown, 14 if wide else 11), STATE_FG.get(state, Style("ink")))]
                 + tail, w)


def _markets_body(st: dict, m: dict, w: int) -> list[Row]:
    """ACTIVE MARKET, then every market still waiting for its result (each
    keyed by its own market id), then the newest settled ones."""
    out: list[Row] = []
    active = st.get("active") if isinstance(st.get("active"), dict) else None
    out.append(_runs([("ACTIVE MARKET", Style("dim", bold=True))], w))
    out.append(_market_line(active, w, _num(m.get("seconds_left"))) if active
               else _runs([("  waiting for discovery", FAINT)], w))
    pending = [v for v in (st.get("pending") or []) if isinstance(v, dict)]
    previous = [v for v in (st.get("previous") or []) if isinstance(v, dict) and not any(v is x for x in pending)]
    if pending:
        out.append(_runs([("PENDING SETTLEMENT", Style("amber", bold=True))], w))
        out += [_market_line(v, w) for v in sorted(pending, key=lambda v: _num(v.get("window_end")) or 0)]
    out.append(_runs([("PREVIOUS MARKETS", Style("dim", bold=True))], w))
    if previous:
        out += [_market_line(v, w) for v in previous]
    else:
        out.append(_runs([("  none yet this session", FAINT)], w))
    return out


def _settlement_band(snap: dict, cols: int, h: int, g: Glyphs) -> list[Row]:
    """POSITION | SETTLEMENT | MARKETS, under ORDERS + HEALTH. Narrower
    terminals drop MARKETS, then POSITION; SETTLEMENT always stays."""
    st = snap.get("settlement") if isinstance(snap.get("settlement"), dict) else {}
    body_h = max(0, h - 2)
    if cols >= 120:
        w = hsplit(cols, (1.4, 1.0, 1.0), (50, 34, 36))      # the full POSITION table fits
    elif cols >= 100:
        w = hsplit(cols, (1.2, 1.0, 1.0), (38, 34, 28))
    elif cols >= 70:
        w = hsplit(cols, (1.0, 1.0), (36, 34))
    else:
        w = [cols]
    s_rows, label = _settlement_body(st, (w[1] if len(w) > 1 else w[0]) - 2)
    settle = panel("SETTLEMENT", s_rows[:body_h], w[1] if len(w) > 1 else w[0], h, g, right_note=label)
    if len(w) == 1:
        return settle
    pos = panel("POSITION", _position_body(snap.get("positions") if isinstance(snap.get("positions"), dict) else {},
                                           w[0] - 2)[:body_h], w[0], h, g)
    parts = [pos, settle]
    if len(w) == 3:
        parts.append(panel("MARKETS", _markets_body(st, snap.get("market") or {}, w[2] - 2)[:body_h], w[2], h, g))
    return join(parts, w, h)


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
    # middle band: ORDERS | CANARY + HEALTH, then POSITION | SETTLEMENT | MARKETS
    # (full when there is room, three rows when tight - before EVENTS either way)
    settle_h = 9 if remaining >= 15 else 5 if remaining >= 11 else 0
    side_rows = _side_body(snap, 1, 99)          # how many rows the side panel wants
    room = remaining - settle_h
    # SNIPER EXECUTION LATENCY | EXECUTION QUALITY when it still leaves the
    # middle band its 8 rows and EVENTS 5 (the trigger row shows send/ACK p50 always)
    # POSITION MANAGER (only when its engine is on), then the latency band
    pm_on = isinstance(snap.get("pm"), dict) and snap["pm"].get("engine")
    pm_h = (14 if pm_on and room >= 14 + 8 + 8 + 3 else 11 if pm_on and room >= 11 + 8 + 3 else 0)
    room -= pm_h
    acct_now = snap.get("account") if isinstance(snap.get("account"), dict) else {}
    pnl_h = (8 if str(acct_now.get("kind") or "").upper() == "PAPER" and isinstance(acct_now.get("attribution"), dict)
             and room >= 8 + 8 + 3 else 0)
    room -= pnl_h
    lat_h = 8 if room >= 8 + 8 + 5 else 0
    room -= lat_h
    mid_h = min(room, max(8, len(side_rows) + 2))
    if room - mid_h < 5:                          # not enough for events below: take it all
        mid_h = room
    w2 = hsplit(cols, (1.6, 1.0), (40, 30))
    mbody = max(0, mid_h - 2)
    orders = panel("ORDERS", _orders_body(snap.get("orders") or [], w2[0] - 2, mbody), w2[0], mid_h, g)
    title = "CANARY + HEALTH" if snap.get("canary") else "HEALTH"
    side = panel(title, _side_body(snap, w2[1] - 2, mbody)[:mbody], w2[1], mid_h, g)
    out += join([orders, side], w2, mid_h)
    remaining -= mid_h
    if settle_h:
        out += _settlement_band(snap, cols, settle_h, g)
        remaining -= settle_h
    if pm_h:
        out += _manager_band(snap, cols, pm_h, g)
        remaining -= pm_h
    if pnl_h:
        out += panel("PAPER P/L BY SOURCE (simulated fills on real books)",
                     _pnl_body(acct_now, cols - 2)[:pnl_h - 2], cols, pnl_h, g)
        remaining -= pnl_h
    if lat_h:
        out += _latency_band(snap, cols, lat_h, g)
        remaining -= lat_h
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
                      f"pnl={money(a.get('pnl') if a.get('kind') != 'LIVE' else a.get('realized'), sign=True)}"
                      + _plain_settlement(snap), 2000)


def _plain_settlement(snap: dict) -> str:
    st = snap.get("settlement") if isinstance(snap.get("settlement"), dict) else {}
    subject = st.get("subject") if isinstance(st.get("subject"), dict) else {}
    if not subject:
        return ""
    state = _text(subject.get("state"))
    tail = (f" {_text(subject.get('winner'))} {money(subject.get('realized'), sign=True)} in "
            f"{_secs(subject.get('settlement_s'))}" if state == "SETTLED" else f" {_secs(subject.get('age_s'))}"
            if state != "ACTIVE" else "")
    return f" settle={_text(subject.get('label'))}:{state.replace(' ', '_')}{tail}"
