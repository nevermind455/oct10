#!/usr/bin/env python3
"""Headless tests for the terminal layer.

Two jobs:
  1. Prove the dashboard renders correctly at any terminal size.
  2. Prove the dashboard did not change the bot.

Run:  python tests_dashboard.py
Needs no TTY, no network, no venue credentials.
"""
from __future__ import annotations

import hashlib
import io
import itertools
import os
import pathlib
import subprocess
import sys
import types
import unicodedata

ROOT = pathlib.Path(__file__).parent
sys.path.insert(0, str(ROOT))

PASS, FAIL = 0, 0
FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL {name} {detail}")


# --------------------------------------------------------------- stubbing ---
def stub_missing() -> None:
    """Stub venue SDKs so the suite runs on any machine. Never used to fake
    a value that reaches the screen — only to make imports resolve."""
    if "websockets" not in sys.modules:
        m = types.ModuleType("websockets")
        m.connect = lambda *a, **k: None
        sys.modules["websockets"] = m
    if "web3" not in sys.modules:
        m = types.ModuleType("web3")

        class _W3:
            def __init__(self, *a, **k): pass
            def is_connected(self): return False
            @staticmethod
            def HTTPProvider(*a, **k): return None
            @staticmethod
            def to_checksum_address(a): return a
        m.Web3 = _W3
        sys.modules["web3"] = m
    if "py_clob_client_v2" not in sys.modules:
        m = types.ModuleType("py_clob_client_v2")
        for n in ("AssetType", "BalanceAllowanceParams", "ClobClient",
                  "MarketOrderArgs", "OrderType", "PartialCreateOrderOptions", "Side"):
            setattr(m, n, type(n, (), {"__init__": lambda self, *a, **k: None}))
        sys.modules["py_clob_client_v2"] = m


stub_missing()

from dashboard import TerminalState, build, snapshot  # noqa: E402
from dashboard.renderer import (ALT_OFF, ALT_ON, CLEAR, CURSOR_ON,  # noqa: E402
                                PlainRenderer, Renderer, render_row)
from dashboard.theme import UNICODE, Style  # noqa: E402
from dashboard.widgets import big_digits, blank, hsplit, join, pad, table  # noqa: E402

SIZES = [(c, r) for c in (40, 56, 64, 72, 80, 84, 90, 100, 110, 120, 140, 160, 200, 240)
         for r in (10, 12, 14, 16, 18, 20, 24, 28, 30, 34, 40, 48, 60)]


# ------------------------------------------------------------ 1. geometry ---
def populated() -> TerminalState:
    """A state filled from values a real run would produce.

    These are inputs to the RENDERER under test, never displayed to a user
    as if they came from the venue.
    """
    st = TerminalState()
    st.set_round_context(1_754_780_700, "9:05PM-9:10PM ET", 47)
    st.bet_size, st.trade_window, st.max_buy_price = 2.0, 60, 0.99
    st.min_buy_price = 0.20
    base = 64_890.0
    for i in range(400):
        st.push_spot(base + (i % 37) - 18 + (i * 0.11))
    st.push_chainlink(64_894.0, 812.0)
    st.push_chainlink(64_894.0, 790.0)
    st.push_book("7213...UP",
                 [{"price": "0.47", "size": "310"}, {"price": "0.46", "size": "900"}],
                 [{"price": "0.52", "size": "180"}, {"price": "0.53", "size": "640"}], 121.0)
    st.start_price.set(64_894.0, source="ROUND log line")
    st.push_price_to_beat(64_894.0)
    st.sig_price.set("UP"); st.sig_book.set("DOWN"); st.sig_chainlink.set("UP")
    st.decision.set("UP")
    st.balance.set({"balance": 41.37, "allowance": 1e6})
    st.tokens.set({"slug": "btc-updown-5m-1754780700", "up_token_id": "72131"})
    st.cancel.set(True)
    st.loop_beat.set(47)
    st.record_order("UP", 2.0, True, None, 640.0)
    st.record_order("DOWN", 2.0, False, "not enough balance", 410.0)
    for i in range(30):
        st.event("BOT", f"line {i}", "info")
    st.trades = [{"time_et": "Aug 09 21:04:58 ET", "side": "UP", "amount": 2.0,
                  "price_side": "UP", "book_side": "DOWN", "chainlink_side": "UP",
                  "result": "ok"}] * 12
    return st


def test_geometry() -> None:
    for st, tag in ((TerminalState(), "empty"), (populated(), "full")):
        snap = snapshot(st, session_trades=st.trades)
        for cols, rows in SIZES:
            frame = build(snap, cols, rows, UNICODE)
            check(f"rowcount {tag} {cols}x{rows}",
                  len(frame) == rows, f"got {len(frame)}")
            bad = [(i, sum(len(t) for t, _ in row)) for i, row in enumerate(frame)
                   if sum(len(t) for t, _ in row) != cols]
            check(f"width {tag} {cols}x{rows}", not bad, f"rows {bad[:3]}")


def _frame_text(snap, cols: int, rows: int) -> str:
    return "\n".join("".join(t for t, _ in row) for row in build(snap, cols, rows, UNICODE))


def test_stop_panel_appears_when_stop_loss_is_enabled() -> None:
    """state.exits/state.stop_status must actually reach a rendered panel.

    They used to be copied into every snapshot and then read by nothing: an
    operator running with STOP_LOSS_ENABLED=1 had zero dashboard visibility
    into what the stop was watching or had sold. This checks the STOP LOSS
    panel actually appears (wide and narrow layouts) once stop_status says
    the feature is enabled, and that the geometry contract still holds; and
    that it stays absent when the feature is off, matching this repo's
    default (STOP_LOSS_ENABLED=0) so existing behavior is unchanged then.
    """
    st = populated()
    st.exits = [{"time": "21:07:03", "side": "UP", "shares": 4.6,
                 "price": 0.24, "proceeds": 1.08}]
    st.stop_status = {
        "enabled": True, "armed": True, "trigger": 0.25, "floor": 0.05,
        "arm": 120.0, "cutoff": 20.0,
        "held": [{"side": "UP", "shares": 4.6, "bid": 0.23}],
    }
    snap = snapshot(st, session_trades=st.trades)
    for cols, rows in ((160, 45), (118, 40)):
        frame = build(snap, cols, rows, UNICODE)
        check(f"rowcount with stop panel {cols}x{rows}",
              len(frame) == rows, f"got {len(frame)}")
        bad = [(i, sum(len(t) for t, _ in row)) for i, row in enumerate(frame)
               if sum(len(t) for t, _ in row) != cols]
        check(f"width with stop panel {cols}x{rows}", not bad, f"rows {bad[:3]}")
        text = _frame_text(snap, cols, rows)
        check(f"STOP LOSS panel renders at {cols}x{rows}", "STOP LOSS" in text)

    # Off by default: nothing about the layout changes when the feature is
    # disabled, which is this repo's out-of-the-box configuration.
    off = populated()
    off_snap = snapshot(off, session_trades=off.trades)
    for cols, rows in ((160, 45), (118, 40)):
        text = _frame_text(off_snap, cols, rows)
        check(f"STOP LOSS panel absent when disabled {cols}x{rows}",
              "STOP LOSS" not in text)


def test_one_sided_books_render_at_every_size() -> None:
    """A temporarily empty side of the live book must not break the frame."""
    fixtures = (
        ("ask-only", [], [{"price": "0.52", "size": "180"}]),
        ("bid-only", [{"price": "0.47", "size": "310"}], []),
    )
    for tag, bids, asks in fixtures:
        st = populated()
        st.push_book("7213...UP", bids, asks, 121.0)
        snap = snapshot(st, session_trades=st.trades)
        for cols, rows in SIZES:
            try:
                frame = build(snap, cols, rows, UNICODE)
            except Exception as exc:
                check(f"one-sided render {tag} {cols}x{rows}", False,
                      f"{type(exc).__name__}: {exc}")
                continue
            bad = [(i, sum(len(t) for t, _ in row))
                   for i, row in enumerate(frame)
                   if sum(len(t) for t, _ in row) != cols]
            check(f"one-sided render {tag} {cols}x{rows}",
                  len(frame) == rows and not bad,
                  f"rows={len(frame)}, bad_widths={bad[:3]}")


def test_no_wide_or_control_chars() -> None:
    """A double-width or control character silently shifts every later column."""
    snap = snapshot(populated(), session_trades=[])
    for cols, rows in ((120, 40), (84, 24), (200, 60)):
        for row in build(snap, cols, rows, UNICODE):
            for text, _ in row:
                for ch in text:
                    check("no control char", ch == "\n" or ord(ch) >= 32 or ch == " ",
                          repr(ch))
                    check("single width",
                          unicodedata.east_asian_width(ch) not in ("W", "F"), repr(ch))


# ----------------------------------------------------- 1b. zero `#` in UI ---
HASH_SIZES = [(40, 12), (56, 10), (64, 20), (72, 16), (84, 24), (100, 30),
              (120, 40), (160, 48), (200, 34), (240, 60)]


def ui_states() -> list[tuple[str, TerminalState]]:
    """One fixture per state the dashboard has to survive."""
    import time as _time

    out: list[tuple[str, TerminalState]] = [("startup", TerminalState())]

    waiting = TerminalState()
    waiting.round_label = "9:05PM-9:10PM ET"
    waiting.seconds_left = 240
    out.append(("waiting for market", waiting))

    out.append(("active market", populated()))

    for side in ("UP", "DOWN"):
        st = populated()
        st.sig_price.set(side); st.sig_book.set(side); st.sig_chainlink.set(side)
        st.decision.set(side)
        out.append((f"{side.lower()} signal", st))

    filled = populated()
    filled.record_order("UP", 5.0, True, None, 210.0)
    filled.flash("FILLED", "UP $5.00 @ 0.52", "good")
    out.append(("order filled", filled))

    rejected = populated()
    rejected.record_order("UP", 5.0, False,
                          "cannot FOK buy UP: no asks on the live book", 180.0)
    rejected.flash("REJECTED", "no asks on the live book", "bad")
    out.append(("order rejected", rejected))

    base_acct = {"realized_pnl": 0.0, "unrealized_mark_to_bid": 0.0,
                 "pending_cost": 5.0, "win_rate": 0.5, "wins": 5, "losses": 5,
                 "open_positions": 1, "cash": 1000.0}
    for name, pnl, wins, losses in (("settlement", 4.9, 8, 5),
                                    ("positive pnl", 128.5, 10, 4),
                                    ("negative pnl", -18.75, 2, 9)):
        st = populated()
        st.accounting = dict(base_acct, total_pnl=pnl, realized_pnl=pnl,
                             wins=wins, losses=losses)
        st.balance.set({"balance": 1000.0 + pnl, "allowance": 1e6, "paper": True})
        out.append((name, st))

    # The endgame book, which the bot meets in the last minute of every round:
    # the winning token keeps only bids, the losing token only asks.
    bids = [{"price": "0.99", "size": "12783"}]
    asks = [{"price": "0.01", "size": "12796"}]
    for name, b, a in (("one-sided book: bids only", bids, []),
                       ("one-sided book: asks only", [], asks),
                       ("empty book", [], [])):
        st = populated()
        st.push_book("7213...UP", b, a, 118.0)
        st.push_down_book("4491...DOWN", a, b, 118.0)
        out.append((name, st))

    no_balance = populated()
    no_balance.balance.set({"balance": None, "allowance": None, "paper": True})
    out.append(("balance not yet read", no_balance))

    dead = populated()
    stale = _time.monotonic() - 900.0
    dead.spot_changed.at = dead.book.at = dead.chainlink.at = stale
    dead.loop_beat.at = stale
    dead.event("WS", "reconnecting after socket close", "warn")
    out.append(("reconnecting feeds", dead))

    return out


def test_no_hash_glyph_in_any_state() -> None:
    """`#` must never reach the screen: not as a bar, a candle or a numeral."""
    for name, st in ui_states():
        snap = snapshot(st, session_trades=st.trades)
        for cols, rows in HASH_SIZES:
            for i, row in enumerate(build(snap, cols, rows, UNICODE)):
                line = "".join(t for t, _ in row)
                check(f"no hash: {name} {cols}x{rows}", "#" not in line,
                      f"row {i}: {line.strip()[:70]!r}")


def test_build_never_raises() -> None:
    """A render exception kills the panel silently: the dashboard task is
    never awaited, so the screen just stops and the bot trades on unseen.
    The one-sided book that does it appears in the last minute of every
    round, which is why this has to hold for every state at every size."""
    for name, st in ui_states():
        snap = snapshot(st, session_trades=st.trades)
        for cols, rows in HASH_SIZES:
            try:
                build(snap, cols, rows, UNICODE)
                check(f"build survives {name} {cols}x{rows}", True)
            except Exception as exc:
                check(f"build survives {name} {cols}x{rows}", False,
                      f"{type(exc).__name__}: {exc}")


def test_no_hash_across_every_size() -> None:
    """The resize sweep: no width may bring a fallback renderer back."""
    snap = snapshot(populated(), session_trades=[])
    for cols, rows in SIZES:
        for row in build(snap, cols, rows, UNICODE):
            check(f"no hash on resize {cols}x{rows}",
                  "#" not in "".join(t for t, _ in row))


def test_no_hash_in_rendered_bytes() -> None:
    """Through the real renderer, escape codes and all."""
    class Tty(io.StringIO):
        def isatty(self) -> bool:
            return True

    snap = snapshot(populated(), session_trades=[])
    for cols, rows in ((120, 40), (84, 24)):
        tty = Tty()
        r = Renderer(tty)
        r.cols, r.rows = cols, rows
        r.start()
        r.draw(build(snap, cols, rows, r.g))
        r.stop()
        out = tty.getvalue()
        check(f"renderer emits no hash {cols}x{rows}", "#" not in out,
              repr(out[:80]))
        check(f"renderer emits block glyphs {cols}x{rows}", "█" in out)


def test_hsplit_exact() -> None:
    for total in range(30, 260):
        parts = hsplit(total, [0.24, 0.30, 0.46], [28, 32, 30])
        check("hsplit sums", sum(parts) == total, f"{total} -> {parts}")
        check("hsplit positive", all(p >= 3 for p in parts), str(parts))


def test_table_and_pad() -> None:
    rows = table(["A", "B"], [4, 5], [[("x", Style()), ("y", Style())]], 30, 4)
    for r in rows:
        check("table width", sum(len(t) for t, _ in r) == 30)
    check("pad truncates", sum(len(t) for t, _ in pad([("abcdef", Style())], 3)) == 3)
    check("big digits fit", all(sum(len(t) for t, _ in r) == 20
                                for r in big_digits("41.37", 20, Style())))
    check("big digits fallback on non-numeric",
          "".join(t for t, _ in big_digits("--", 20, Style())[1]).strip() == "--")
    from dashboard.widgets import giant_digits
    giant = giant_digits("$41.37", 48, Style("green", bold=True), g=UNICODE)
    check("giant cash is five rows", len(giant) == 5, str(len(giant)))
    check("giant cash uses block glyphs",
          any("\u2588" in t for row in giant for t, _ in row))
    check("giant cash stays in width",
          all(sum(len(t) for t, _ in r) == 48 for r in giant))
    lines = ["".join(t for t, _ in row) for row in giant]
    # Weight: a terminal cell is twice as tall as it is wide, so a
    # single-column upright reads as a hairline against a full-width bar.
    check("giant cash uprights are two columns wide",
          all("██" in ln for ln in lines), str(lines))
    # The pattern tables spell "off" with spaces; a stray dot or hash in one
    # of them would print as itself.
    check("giant cash draws nothing but blocks and the currency mark",
          all(set(ln) <= {" ", "█", "$"} for ln in lines), str(lines))
    check("the currency mark sits on the centre line alone",
          "$" in lines[2] and not any("$" in ln for i, ln in enumerate(lines) if i != 2),
          str(lines))
    check("the decimal point sits on the baseline",
          lines[4].count("█") > lines[3].count("█"), str(lines[3:]))
    wide = giant_digits("$100000.00", 30, Style("green", bold=True), g=UNICODE)
    check("a figure too wide for the block font keeps the panel's height",
          len(wide) == 5 and all(sum(len(t) for t, _ in r) == 30 for r in wide),
          str(len(wide)))
    narrow = giant_digits("$41.37", 26, Style("green", bold=True), g=UNICODE)
    check("the narrow block font is still blocks only",
          all(set("".join(t for t, _ in row)) <= {" ", "█", "$"} for row in narrow),
          str(["".join(t for t, _ in row) for row in narrow]))


# ------------------------------------------------------------ 2. renderer ---
class FakeTTY(io.StringIO):
    def isatty(self) -> bool:
        return True


def test_frame_diff() -> None:
    st = populated()
    snap = snapshot(st, session_trades=st.trades)
    out = FakeTTY()
    r = Renderer(out)
    r.cols, r.rows = 120, 40
    r.size = lambda: (120, 40)
    r.start()
    check("start clears the alt screen once", out.getvalue().count(CLEAR) == 1,
          str(out.getvalue().count(CLEAR)))
    out.truncate(0); out.seek(0)

    frame = build(snap, 120, 40, r.g)
    r.draw(frame)
    first = out.getvalue()
    check("first paint does not flash-clear", CLEAR not in first)
    check("first paint writes the home row", "\x1b[1;1H" in first)

    out.truncate(0); out.seek(0)
    r.draw(frame)
    check("identical frame writes nothing", out.getvalue() == "", repr(out.getvalue()[:80]))

    out.truncate(0); out.seek(0)
    changed = [list(x) for x in frame]
    changed[7] = pad([("ZZZ", Style())], 120)
    r.draw(changed)
    body = out.getvalue()
    check("changed frame writes something", body != "")
    check("no full clear on update", CLEAR not in body)
    check("only the changed row is addressed", body.count("\x1b[8;1H") == 1, body[:60])
    check("one row rewritten", len([1 for i in range(1, 41)
                                    if f"\x1b[{i};1H" in body]) <= 2)

    out.truncate(0); out.seek(0)
    r._resized = True
    r.draw(frame)
    resized = out.getvalue()
    check("resize does not flash-clear", CLEAR not in resized)
    check("resize rewrites in place", "\x1b[1;1H" in resized)

    out.truncate(0); out.seek(0)
    r.stop()
    tail = out.getvalue()
    check("cursor restored", CURSOR_ON in tail)
    check("alt screen exited", ALT_OFF in tail)


def test_join_keeps_requested_height() -> None:
    short = [blank(10) for _ in range(3)]
    tall = [blank(10) for _ in range(8)]
    out = join([tall, short], [10, 10], 8)
    check("join keeps requested rows when a column is short",
          len(out) == 8, str(len(out)))


def test_size_hysteresis_ignores_one_frame_jitter() -> None:
    import dashboard.renderer as rend
    r = Renderer(FakeTTY())
    r.cols, r.rows = 120, 40
    r._size_pending = None
    orig = rend.shutil.get_terminal_size
    rend.shutil.get_terminal_size = lambda fallback=(120, 40): os.terminal_size((121, 41))
    try:
        first = r.size()
        check("one-frame size jitter is ignored", first == (120, 40), str(first))
        second = r.size()
        check("a size that holds for two frames is adopted",
              second == (121, 41), str(second))
    finally:
        rend.shutil.get_terminal_size = orig


def test_context_manager_restores_on_exception() -> None:
    out = FakeTTY()
    r = Renderer(out)
    caught = False
    try:
        with r:
            raise RuntimeError("boom")
    except RuntimeError:
        caught = True
    check("test exception was observed", caught)
    check("restore after exception", CURSOR_ON in out.getvalue() and ALT_OFF in out.getvalue())


def test_non_tty_emits_no_escapes() -> None:
    out = io.StringIO()          # isatty() False
    r = PlainRenderer(out, every=0.0)
    st = populated()
    r.status(snapshot(st, session_trades=st.trades))
    body = out.getvalue()
    check("plain renderer emits no escapes", "\x1b" not in body, repr(body[:60]))
    check("plain renderer emits a status line", "[DASH]" in body)
    check("plain renderer names official round prices",
          "ptb=$64,894.00" in body and "running=$64,894.00" in body, body)
    check("make_renderer picks plain for a pipe",
          type(__import__("dashboard.renderer", fromlist=["make_renderer"])
               .make_renderer(io.StringIO())).__name__ == "PlainRenderer")


def test_selftest_survives_ascii_strict_stdout() -> None:
    """The direct preview write must initialize redirected legacy stdout."""
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "ascii:strict"
    env["TERM_PREVIEW"] = "40x12"
    proc = subprocess.run(
        [sys.executable, str(ROOT / "run_terminal.py"), "--selftest"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )
    detail = (proc.stderr or proc.stdout[-1000:]).decode("utf-8", "replace")
    check("selftest supports ascii-strict redirected stdout",
          proc.returncode == 0, detail)
    check("selftest completes its Unicode preview",
          b"selftest: PASS" in proc.stdout, detail)


def test_quit_key_stops_the_render_loop() -> None:
    """'q' must stop the terminal exactly like Ctrl+C, independent of SIGINT.

    render_loop's software-level quit key was dropped at some point, leaving
    an OS SIGINT (via Ctrl+C) as the only way to stop the dashboard - which
    does not always reach the process in every SSH/tmux/supervised setup.
    This drives render_loop directly with a fake key reader and checks that
    'q' (and Ctrl+C's raw \\x03 byte) sets both the loop's own stop event and
    main_bot.stop_event, and that an unrelated key does neither.
    """
    import asyncio
    import threading

    import main_bot
    import run_terminal
    from dashboard.renderer import PlainRenderer

    class FakeKeys:
        def __init__(self, chars):
            self._chars = list(chars)

        def pop(self):
            out, self._chars = self._chars, []
            return out

    async def drive(chars, *, expect_stop):
        st = TerminalState()
        stop = threading.Event()
        renderer = PlainRenderer(stream=io.StringIO(), every=1000.0)
        keys = FakeKeys(chars)
        main_bot.stop_event.clear()
        task = asyncio.create_task(
            run_terminal.render_loop(st, stop, keys, renderer))
        try:
            if expect_stop:
                await asyncio.wait_for(task, timeout=2.0)
                return stop.is_set(), main_bot.stop_event.is_set()
            await asyncio.sleep(0.3)
            return stop.is_set(), main_bot.stop_event.is_set()
        finally:
            stop.set()
            if not task.done():
                await asyncio.wait_for(task, timeout=2.0)

    stopped, bot_stopped = asyncio.run(drive(["q"], expect_stop=True))
    check("'q' stops the render loop", stopped)
    check("'q' also sets main_bot.stop_event", bot_stopped)

    stopped, bot_stopped = asyncio.run(drive(["\x03"], expect_stop=True))
    check("Ctrl+C's raw byte stops the render loop too", stopped)
    check("Ctrl+C's raw byte also sets main_bot.stop_event", bot_stopped)

    stopped, bot_stopped = asyncio.run(drive(["x"], expect_stop=False))
    check("an unrelated key does not stop the loop", not stopped)
    check("an unrelated key does not touch main_bot.stop_event", not bot_stopped)


def test_render_row_resets() -> None:
    line = render_row(pad([("hi", Style("green"))], 10))
    check("row ends reset", line.endswith("\x1b[0m"))
    check("alt on constant", ALT_ON.startswith("\x1b"))


# ------------------------------------------------- 3. data integrity ---
NUMERIC_BAN = ("0.00%", "$1,", "$2,", "12.3", "45.6")


def test_empty_state_invents_nothing() -> None:
    snap = snapshot(TerminalState(), session_trades=[])
    text = "\n".join("".join(t for t, _ in row) for row in build(snap, 160, 50, UNICODE))
    for key in ("PNL", "REALIZED", "EXPOSURE", "WINS", "SPOT",
                "PRICE TO BEAT", "RUNNING PRICE"):
        check(f"{key} present", key in text)
    check("shows the missing marker", "--" in text)
    check("no fabricated dollar pnl", "$-" not in text and "+$" not in text, "")
    for pat in NUMERIC_BAN:
        check(f"no demo number {pat}", pat not in text)
    check("absent modules labelled", "ABSENT" in text)


def test_round_panel_uses_official_chainlink_pair() -> None:
    """Binance is auxiliary; it must never be labelled as market strike."""
    st = populated()
    st.start_price.set(100.0, source="Binance boundary print")
    st.push_price_to_beat(200.0)
    st.push_chainlink(210.0, 25.0, observation_id=1_754_780_747_000)
    snap = snapshot(st, session_trades=[])
    lines = ["".join(text for text, _ in row)
             for row in build(snap, 160, 50, UNICODE)]
    ptb = next((line for line in lines if "PRICE TO BEAT" in line), "")
    running = next((line for line in lines if "RUNNING PRICE" in line), "")
    distance = next((line for line in lines if "DIST TO BEAT" in line), "")
    full = "\n".join(lines)
    check("Price To Beat renders Chainlink opening TWAP", "$200.00" in ptb, ptb)
    check("Running Price renders current Chainlink TWAP", "$210.00" in running,
          running)
    check("distance compares the two official TWAP values", "+10.00" in distance,
          distance)
    check("Binance opening print is never called strike",
          "STRIKE (start px)" not in full, full)


def test_momentum_uses_sig_price_not_a_placeholder() -> None:
    """MOMENTUM is SIG PRICE (Binance now vs open); DIST is that split this round."""
    st = populated()
    st.trades = [
        {"time_et": "Aug 26 12:00:00 ET", "side": "UP", "amount": 2.5,
         "price_side": "UP", "book_side": "DOWN", "chainlink_side": "UP",
         "result": "ok"},
        {"time_et": "Aug 26 12:00:12 ET", "side": "DOWN", "amount": 2.5,
         "price_side": "DOWN", "book_side": "DOWN", "chainlink_side": "UP",
         "result": "ok"},
        {"time_et": "Aug 26 12:00:24 ET", "side": "UP", "amount": 2.5,
         "price_side": "UP", "book_side": "UP", "chainlink_side": "DOWN",
         "result": "ok"},
    ]
    st.start_price.set(100.0)
    st.push_spot(112.5)
    st.sig_price.set("UP")
    snap = snapshot(st, session_trades=st.trades)
    lines = ["".join(text for text, _ in row)
             for row in build(snap, 160, 50, UNICODE)]
    dist = next((line for line in lines if "MOMENTUM DIST" in line), "")
    mom_row = next((line for line in lines
                    if "MOMENTUM" in line and "MOMENTUM DIST" not in line
                    and "+12.50" in line), "")
    check("momentum dist counts SIG PRICE sides this round",
          "UP 2  DOWN 1" in dist, dist)
    check("live momentum is SIG PRICE plus Binance move from open",
          "UP" in mom_row and "+12.50" in mom_row, mom_row)
    empty = snapshot(TerminalState(), session_trades=[])
    empty_lines = ["".join(text for text, _ in row)
                   for row in build(empty, 160, 50, UNICODE)]
    empty_dist = next((line for line in empty_lines if "MOMENTUM DIST" in line), "")
    check("no trades still shows the missing marker, not a fake 0/0",
          "--" in empty_dist, empty_dist)


def test_round_rollover_clears_old_price_and_signal_state() -> None:
    st = TerminalState()
    check("first round context is a transition",
          st.set_round_context(300, "ROUND A", 200))
    st.start_price.set(100.0)
    st.push_price_to_beat(200.0)
    st.sig_price.set("UP")
    st.sig_book.set("DOWN")
    st.sig_chainlink.set("UP")
    st.decision.set("UP")
    st.decision_forced = True

    check("same round does not erase observations",
          not st.set_round_context(300, "ROUND A", 199)
          and st.start_chainlink.value == 200.0
          and st.decision.value == "UP")
    check("new round is detected",
          st.set_round_context(600, "ROUND B", 300))
    snap = snapshot(st, session_trades=[])
    check("new label and key commit with reset",
          snap["round_key"] == 600 and snap["round_label"] == "ROUND B"
          and snap["seconds_left"] == 300, repr(snap["round_key"]))
    check("old opening values cannot cross a boundary",
          snap["start_price"] is None and snap["start_chainlink"] is None)
    check("old signals and decision cannot cross a boundary",
          all(snap[key] is None for key in
              ("sig_price", "sig_book", "sig_chainlink", "decision"))
          and not snap["decision_forced"])


def test_running_price_clears_when_chainlink_is_unavailable() -> None:
    st = TerminalState()
    st.push_chainlink(210.0, 20.0, observation_id=1_000)
    calls = st.chainlink.count
    st.push_chainlink(210.0, 30.0, observation_id=1_000)
    check("dashboard polling does not duplicate an RTDS observation",
          st.chainlink.count == calls and st.chainlink_repeat == 0)
    st.push_chainlink(210.0, 5.0, observation_id=2_000)
    check("equal values from distinct RTDS observations count once each",
          st.chainlink.count == calls + 1 and st.chainlink_repeat == 1)
    st.push_chainlink(211.0, 4.0, observation_id=2_000)
    check("a corrected value at the same timestamp is not deduplicated",
          st.chainlink.value == 211.0 and st.chainlink.count == calls + 2
          and st.chainlink_repeat == 0)
    st.push_chainlink(None, 500.0, observation_id=2_000)
    snap = snapshot(st, session_trades=[])
    check("missing or stale Chainlink hides the old numeric value",
          snap["chainlink"] is None and snap["chainlink_age"] is None
          and snap["chainlink_repeat"] == 0)


def test_late_old_round_strategy_probe_cannot_repopulate_reset_state() -> None:
    """An awaited old validation may finish after the wall-clock boundary."""
    os.environ.setdefault("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    import main_bot
    import orderbook
    import strategy
    from dashboard import probe

    st = TerminalState()
    st.set_round_context(300, "ROUND A", 1)
    st.mark_strategy_round(300)
    st.push_price_to_beat(200.0, round_key=300)
    real_stdout = probe.install(st)
    try:
        st.set_round_context(600, "ROUND B", 300)
        # These are the old coroutine's late final-validation callbacks.
        main_bot.price_signal(300, 100.0, 101.0)
        main_bot.chainlink_signal(300, 200.0, 201.0)
        orderbook.liquidity_signal(
            [{"price": 0.4, "size": 10.0}],
            [{"price": 0.6, "size": 20.0}],
        )
        strategy.final_decision("UP", "DOWN", "UP")
        snap = snapshot(st, session_trades=[])
        check("late old Price To Beat stays rejected",
              snap["start_chainlink"] is None)
        check("late old signals and decision stay rejected",
              all(snap[key] is None for key in
                  ("sig_price", "sig_book", "sig_chainlink", "decision")))
    finally:
        probe.uninstall()
        sys.stdout = real_stdout


def test_explicit_round_keyed_signal_probes_never_swap_sources() -> None:
    os.environ.setdefault("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    import main_bot
    from dashboard import probe

    st = TerminalState()
    st.set_round_context(300, "ROUND A", 200)
    st.mark_strategy_round(300)
    real_stdout = probe.install(st)
    try:
        main_bot.price_signal(300, 100.0, 101.0)
        snap = snapshot(st, session_trades=[])
        check("a solitary phase-1 call updates only SIG PRICE",
              snap["sig_price"] == "UP" and snap["sig_chainlink"] is None,
              str((snap["sig_price"], snap["sig_chainlink"])))

        main_bot.chainlink_signal(300, 200.0, 199.0)
        snap = snapshot(st, session_trades=[])
        check("the next explicit Chainlink call cannot steal the price slot",
              snap["sig_price"] == "UP" and snap["sig_chainlink"] == "DOWN",
              str((snap["sig_price"], snap["sig_chainlink"])))

        main_bot.price_signal(300, 100.0, 100.0)
        snap = snapshot(st, session_trades=[])
        check("a neutral current price clears SIG PRICE visibly",
              snap["sig_price"] is None and snap["sig_chainlink"] == "DOWN",
              str((snap["sig_price"], snap["sig_chainlink"])))

        main_bot.price_signal(0, 100.0, 101.0)
        check("an old round cannot overwrite the neutral current-round signal",
              snapshot(st, session_trades=[])["sig_price"] is None)
    finally:
        probe.uninstall()
        sys.stdout = real_stdout


def test_absent_are_absent_not_disconnected() -> None:
    h = TerminalState().feed_health()
    for k in ("POLY WS", "USER WS", "DATABASE", "RECONCILE", "SETTLEMENT"):
        check(f"{k} is ABSENT", h[k] == "ABSENT", h[k])
    check("binance starts WAIT not OK", h["BINANCE WS"] == "WAIT", h["BINANCE WS"])


def test_staleness_marks_rather_than_hides() -> None:
    st = populated()
    st.spot_changed.at -= 30.0          # simulate a silent feed
    snap = snapshot(st, session_trades=[])
    check("stale feed is marked", snap["spot_status"] in ("STALE", "DISCONNECTED"),
          snap["spot_status"])
    check("last value retained", snap["spot"] is not None)


def test_overlay_preserves_geometry() -> None:
    st = populated()
    st.flash("+$4.52", "ORDER FILLED", "good")
    snap = snapshot(st, session_trades=st.trades)
    for cols, rows in ((120, 40), (84, 24), (200, 60)):
        frame = build(snap, cols, rows, UNICODE)
        check("overlay keeps rowcount", len(frame) == rows)
        for row in frame:
            check("overlay keeps width", sum(len(t) for t, _ in row) == cols)
        check("overlay text present",
              "+$4.52" in "".join("".join(t for t, _ in r) for r in frame))


# ----------------------------------------- 4. REGRESSION: bot unchanged ---
TRADING_FILES = ["main_bot.py", "strategy.py", "polymarket_trade.py", "orderbook.py",
                 "market_discovery.py", "price_ws.py", "timer.py",
                 "config.py"]

BASELINE_SHA = {  # approved trading-file baseline; intentional changes require review
    # Re-approved 2026-09-07 after normalising line endings to LF.
    # These hashes are over raw BYTES, so a CRLF working copy on Windows and
    # an LF one on Linux produce different digests for identical source. The
    # previous set was recorded on Windows where three files still carried
    # CRLF, and they failed on a Linux checkout - the guard firing on a
    # platform difference rather than on an edit, which is exactly the noise
    # that made it useless before. .gitattributes now pins text files to LF
    # so the digest means the same thing on both.
    # main_bot.py, polymarket_trade.py, orderbook.py re-approved 2026-09-09:
    # SIGNAL_MINORITY_RULE guard fix, venue-minimum sizing dedup (see
    # orderbook.venue_minimum_stake). main_bot.py, config.py re-approved again
    # same day: TAPER_HEDGE_ENABLED (PAPER-only tapering entry + growing
    # hedge, backtested against 102 real settled rounds - see the entry-cap
    # analysis).
    # main_bot.py re-approved 2026-09-09: the taper cycle now follows the
    # signal - a flip away from the side a cycle was built on retires that
    # cycle and restarts from entry 1 on the new side, instead of holding
    # the original anchor for the rest of the round.
    # main_bot.py, config.py re-approved 2026-09-09: PRIMARY_ENTRY_MIN/MAX_PRICE
    # - a tighter price band for primary phase-2 entries, with taper hedge legs
    # left on the account bounds. Measured over 620 settled fills: primary paid
    # 0.577 for a 52% hit rate (edge -0.058), hedge paid 0.418 for 54% (+0.118).
    # main_bot.py, config.py re-approved 2026-09-09: SIGNAL_DECISION_RULE
    # (price | minority | final) chooses the order side, read in exactly one
    # place - _authority_side - which the phase-2 chooser and all three
    # re-validation gates now share, so a gate can no longer evaluate a
    # different rule from the chooser and reject its orders.
    # main_bot.py re-approved 2026-09-09: a signal flip re-anchors the taper
    # cycle to the new side but keeps its position. Resetting the count
    # starved the opposite-side slot to 1-in-5.7 fills instead of 1-in-3.
    # main_bot.py, config.py re-approved 2026-09-09: MAX_UNSETTLED_EXPOSURE,
    # a cap on cash committed to rounds the venue has not resolved. Default 0
    # (off). Per-round exposure could not see a settlement stall freezing 103%
    # of a wallet across 20 open rounds.
    # main_bot.py, config.py re-approved 2026-09-10: REQUIRE_SIGNAL_UNANIMITY,
    # a phase-2 filter refusing contested reads. Default off. Measured over 894
    # settled fills: contested 46% of positions won vs 60% unanimous, but both
    # groups lost - it is a turnover brake, not an edge.
    # main_bot.py, config.py re-approved 2026-09-10: TAPER_ADVANCE_AFTER_SKIPS,
    # stepping the taper cycle past a slot whose side has priced out of the
    # band. Default 0 (wait, as before).
    # main_bot.py, config.py re-approved 2026-09-10: TAPER_PRIMARY_SLOTS makes
    # the signal:opposite ratio configurable (default 2, the original shape).
    # main_bot.py, config.py re-approved 2026-09-10: TAPER_PRIMARY_LADDER /
    # TAPER_HEDGE_LADDER give each cycle slot its own size, with the list
    # lengths setting the slot counts. Unset keeps the old scheme.
    # main_bot.py, config.py re-approved 2026-09-10: the opening-print REST
    # recovery now retries (BOUNDARY_BACKFILL_RETRIES) instead of running once.
    # A single timeout used to leave a round with no strike, and 10% of rounds
    # the bot was up for produced no trade at all.
    # main_bot.py, config.py re-approved 2026-09-12: entry_cost_ceiling now
    # charges the slot's real stake. It always charged BET_SIZE, which merely
    # over-charged until a taper ladder staked $5 per slot - then the round cap
    # under-counted by 19%.
    # main_bot.py, config.py re-approved 2026-09-22: every complement
    # purchase now prices the finished pair at the submit choke point
    # (COMPLEMENT_REQUIRES_PROFIT). A matched pair pays exactly $1.00, and
    # 72 of 89 completed pairs had cost more than that.
    # main_bot.py, config.py re-approved 2026-09-22: execution-readiness
    # telemetry (Finding 5) and the PAPER/LIVE strategy parity report.
    # The LIVE readiness gate is unchanged - it is only described now.
    "main_bot.py": "132824966b9a0fa61377b7a485c9165cea6179c1f89cdbd490b730cd9258a506",
    "strategy.py": "069e61b18709a6f56de1b54582ffd803fb695590341fd53e1c3dd670a2df1878",
    # config.py, polymarket_trade.py re-approved 2026-09-16: paper fill-delay
    # probes (PAPER_FILL_DELAY_PROBES) and live POST timing. Both diagnostic:
    # nothing reads them for a decision. Paper had assumed instant matching
    # and live won 10.5 points less often at the same prices.
    # polymarket_trade.py re-approved 2026-09-22: live attempt telemetry.
    # Every intended order is journalled BEFORE submission, so orders that
    # never fill finally exist in the record - the live fill rate was
    # previously unknowable because only matched orders were kept.
    # polymarket_trade.py re-approved 2026-09-22: the book's own timestamps
    # now reach the order path (P3), so an attempt records a MEASURED age
    # from monotonic clocks only. book_age_bound_s keeps its old meaning.
    # polymarket_trade.py re-approved 2026-10-05: _is_no_match also knows the
    # venue's documented FOK 500 wording and the FAK zero-fill reply ("no
    # orders found to match with FAK order"). Classification only.
    "polymarket_trade.py": "71b5da67c456c2fb30f3578b84019b0152324bdf338c20c53941eba6b72adc94",
    "orderbook.py": "fe3decc44a8f1061c2788c1f2ae85739ade137910033031eac3138f11b22f090",
    "market_discovery.py": "23c605f678eaf1c6caf60259293b9bccf73413e7f632c0a6749c55acc571aa11",
    "price_ws.py": "0dc5e08fede52b8ec20d60cca83c6811baa811832d711f4c8236cf6128b628c7",
    "timer.py": "3ca35cc64539d45f7e4b982cbe9b6153138f87ee1be79adfae0c8eaccc875d50",
    # config.py re-approved 2026-09-22: POSITION_SIZING/kelly_stake added.
    # Sizing only; default stays 'fixed' and kelly refuses any band without
    # a measured positive edge, so no path changes unless asked.
    # config.py re-approved 2026-09-22: PAPER_ADVERSE_FILL_TICKS models the
    # paper-to-live leak - paper filled whenever the book allowed, live only
    # when the offer survived the round trip. PAPER-side only.
    # config.py re-approved 2026-09-23: PAPER_ADVERSE_FILL_TICKS removed. It
    # refused a fill when the ask ticked up, which is not what a FOK does -
    # a signed order fills at any price up to its limit. Paper now aligns on
    # the price limit and a stake fixed before the delay, as live does.
    "config.py": "4a1584c419e24d805c691d3bf8d7885f998c00aba2e2195f647e963154fda45d",
}


def test_trading_file_baselines() -> None:
    for name in TRADING_FILES:
        # Digest the file as GIT STORES it, not as the working copy
        # happens to sit on disk. .gitattributes pins text to LF, but a
        # Windows checkout can still hold CRLF, and hashing raw bytes made
        # this guard fire on a fresh clone (every LF file differing from a
        # CRLF-recorded baseline) rather than on an actual edit - the exact
        # platform-difference noise it was rewritten to stop.
        raw = (ROOT / name).read_bytes().replace(b"\r\n", b"\n")
        digest = hashlib.sha256(raw).hexdigest()
        check(f"approved baseline {name}", digest == BASELINE_SHA[name],
              f"{digest[:12]} != {BASELINE_SHA[name][:12]}")


SIDES = (None, "UP", "DOWN")
PRICES = (None, 0.0, 64_000.0, 64_894.0, 64_894.01, 1e9, -5.0)


def _truth_tables(strategy) -> tuple[dict, dict]:
    a = {(s, c): strategy.decide(s, c) for s in PRICES for c in PRICES}
    b = {t: strategy.final_decision(*t) for t in itertools.product(SIDES, SIDES, SIDES)}
    return a, b


def test_decisions_identical_after_probe() -> None:
    os.environ.setdefault("POLY_PRIVATE_KEY", "0x" + "1" * 64)
    import main_bot
    import strategy
    from dashboard import probe

    before = _truth_tables(strategy)

    # spy on the real order path BEFORE probing, so we can prove the wrapper
    # passes arguments through untouched and returns the callee's own value
    seen: list[tuple] = []
    sentinel = object()
    main_bot.place_trade = lambda *a, **k: (seen.append((a, k)), sentinel)[1]
    main_bot.cancel_all_open_orders = lambda *a, **k: "CANCEL-RET"
    main_bot.get_balance_allowance = lambda *a, **k: {"balance": 1.0, "allowance": 2.0}

    st = TerminalState()
    real_stdout = probe.install(st)
    try:
        after = _truth_tables(strategy)
        check("decide() truth table unchanged", before[0] == after[0])
        check("final_decision() truth table unchanged", before[1] == after[1])

        ret = main_bot.place_trade("UP", 2.0, "UPID", "DOWNID")
        check("place_trade return passed through", ret is sentinel)
        check("place_trade args passed through",
              seen == [(("UP", 2.0, "UPID", "DOWNID"), {})], str(seen))
        check("cancel return passed through",
              main_bot.cancel_all_open_orders() == "CANCEL-RET")

        import orderbook
        bids = [{"price": "0.4", "size": "10"}]
        asks = [{"price": "0.6", "size": "20"}]
        check("liquidity_signal unchanged", orderbook.liquidity_signal(bids, asks) == "DOWN")

        # the sink must not print to the terminal
        print("[BOT] a captured line")
        check("stdout captured, not printed", isinstance(sys.stdout, probe.EventSink))
        check("captured line reached the feed",
              any("a captured line" in e.text for e in st.events))
    finally:
        probe.uninstall()
        sys.stdout = real_stdout

    check("uninstall restores strategy", _truth_tables(strategy) == before)


def test_probe_survives_telemetry_failure() -> None:
    """A bug in the dashboard must never reach a trading call."""
    import main_bot
    from dashboard import probe

    class Exploding(TerminalState):
        def record_order(self, *a, **k):
            raise RuntimeError("dashboard bug")

    calls: list = []
    main_bot.place_trade = lambda *a, **k: (calls.append(a), True)[1]
    st = Exploding()
    real = probe.install(st)
    try:
        check("order still succeeds when telemetry raises",
              main_bot.place_trade("UP", 2.0, "A", "B") is True)
        check("order still reached the venue path", len(calls) == 1)
    finally:
        probe.uninstall()
        sys.stdout = real


def test_order_response_requires_acceptance_evidence() -> None:
    import polymarket_trade as trade

    originals = {
        "client": trade._client,
        "Side": trade.Side,
        "OrderType": trade.OrderType,
        "MarketOrderArgs": trade.MarketOrderArgs,
        "PartialCreateOrderOptions": trade.PartialCreateOrderOptions,
        "AssetType": trade.AssetType,
        "BalanceAllowanceParams": trade.BalanceAllowanceParams,
        "sleep": trade.time.sleep,
        "book": trade.orderbook.validate_buy_liquidity,
        "observer": trade._order_observer,
        "journal_fault": trade._journal_fault,
        "trade_window": trade.config.TRADE_LAST_SECONDS,
        "min_expiry": trade.config.MIN_SECONDS_TO_EXPIRY,
        "ambiguous_condition": trade._ambiguous_condition,
        "ambiguous_until": trade._ambiguous_until,
        "ambiguous_tokens": set(trade._ambiguous_tokens),
        "ambiguous_all_tokens": trade._ambiguous_all_tokens,
        "assumed_delay": trade.config.ASSUMED_MATCH_DELAY_SECONDS,
    }
    up, down = "101", "202"
    condition = "0x" + "a" * 64
    window_end = (int(trade.time.time()) // 300 + 1) * 300

    class FakeClient:
        def __init__(self, response):
            self.response = response
        def get_clob_market_info(self, _condition):
            return {
                "t": [{"o": "Up", "t": up}, {"o": "Down", "t": down}],
                "mos": "1", "mts": "0.01", "nr": False,
                # `to` is the venue's own flag for "this curve is charged to
                # the taker"; without it the live path will not price a fill.
                # `itode` false because this endpoint never discloses the
                # delay's duration - a market that declares one is refused
                # outright, which the check below pins down separately.
                "fd": {"r": "0.07", "e": 1, "to": True}, "itode": False,
            }
        def get_balance_allowance(self, *_a, **_kw):
            return {"balance": "100000000",
                    "allowances": {"exchange": "100000000",
                                   "neg_risk": "100000000"}}
        def create_market_order(self, *_a, **_kw):
            return "signed"
        def post_order(self, *_a, **_kw):
            return self.response

    trade.Side = types.SimpleNamespace(BUY="BUY")
    trade.OrderType = types.SimpleNamespace(FOK="FOK")
    trade.MarketOrderArgs = lambda **kw: kw
    trade.PartialCreateOrderOptions = lambda **kw: kw
    trade.AssetType = types.SimpleNamespace(COLLATERAL="COLLATERAL")
    trade.BalanceAllowanceParams = lambda **kw: kw
    trade.orderbook.validate_buy_liquidity = lambda *_a, **_kw: (
        [{"price": "0.49", "size": "100"}],
        [{"price": "0.50", "size": "100"}])
    trade.config.TRADE_LAST_SECONDS = 300
    trade.config.MIN_SECONDS_TO_EXPIRY = 0
    trade.set_order_observer(lambda _receipt: True)
    try:
        # A market that declares a matching delay without its duration cannot
        # be timed against the expiry cutoff, so it never reaches signing.
        class DelayedClient(FakeClient):
            def get_clob_market_info(self, condition):
                info = FakeClient.get_clob_market_info(self, condition)
                return {**info, "itode": True}

        trade._client = DelayedClient(
            {"success": True, "orderID": "accepted", "status": "matched",
             "tradeIDs": ["trade-0"]})
        trade._ambiguous_condition = None
        trade.config.ASSUMED_MATCH_DELAY_SECONDS = 0
        check("an undisclosed matching delay is refused before signing",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is False
              and "matching delay" in (trade.last_order_error or ""),
              str(trade.last_order_error))
        trade.config.ASSUMED_MATCH_DELAY_SECONDS = originals["assumed_delay"]

        for response in (
            None,
            {},
            "venue rejected",
            {"errorMsg": "venue rejected"},
            {"success": False, "orderID": "ghost", "errorMsg": "venue rejected"},
            {"ok": False, "order_id": "ghost", "message": "venue rejected"},
            {"success": True},
            {"orderID": "ghost", "status": "failed"},
        ):
            trade._ambiguous_condition = None
            trade._ambiguous_until = 0
            trade._ambiguous_tokens.clear()
            trade._ambiguous_all_tokens = False
            trade._client = FakeClient(response)
            check(f"malformed/rejected response {response!r} is not success",
                  trade.place_trade("UP", 2.0, up, down, condition, window_end) is False,
                  str(trade.last_order_error))

        # When the venue reports execution amounts they are stored as-is.
        # When it omits them, the receipt records None rather than a guess;
        # fill size still comes from a later CONFIRMED user-channel trade.
        amounts = {"makingAmount": "2000000", "takingAmount": "4000000"}
        trade._client = FakeClient(
            {"success": True, "orderID": "accepted", "status": "matched",
             "tradeIDs": ["trade-1"], **amounts})
        trade._ambiguous_condition = None
        check("explicit venue acceptance succeeds",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is True)

        trade._client = FakeClient({"ok": True, "order_id": "accepted-v2",
                                    "status": "matched", "trade_ids": ["trade-2"],
                                    **amounts})
        check("current ok/order_id response succeeds",
              trade.place_trade("DOWN", 2.0, up, down, condition, window_end) is True)

        trade._client = FakeClient(
            {"success": True, "orderID": "no-amounts", "status": "matched",
             "tradeIDs": ["trade-x"]})
        trade._ambiguous_condition = None
        trade._ambiguous_until = 0
        check("a matched FOK without execution amounts is still placed",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is True,
              str(trade.last_order_error))
        check("omitted execution amounts are not invented on the receipt",
              trade.last_order_receipt["making_amount_base_units"] is None
              and trade.last_order_receipt["taking_amount_base_units"] is None
              and trade.last_order_receipt["order_id"] == "no-amounts"
              and trade.last_order_receipt["trade_ids"] == ["trade-x"],
              str(trade.last_order_receipt))
        check("a known matched FOK does not block the rest of the round",
              trade._ambiguous_condition is None,
              str(trade._ambiguous_condition))

        class RetryClient:
            def __init__(self):
                self.created = []
                self.posted = []
                self.responses = [
                    {"success": False, "errorMsg": "no match"},
                    {"success": False,
                     "errorMsg": "order couldn't be fully filled"},
                    {"success": True, "orderID": "fresh-third",
                     "status": "matched", "tradeIDs": ["trade-3"],
                     "makingAmount": "2000000", "takingAmount": "4000000"},
                ]
            def get_clob_market_info(self, _condition):
                return FakeClient(None).get_clob_market_info(_condition)
            def get_balance_allowance(self, *_a, **_kw):
                return FakeClient(None).get_balance_allowance()
            def create_market_order(self, *_a, **_kw):
                signed = f"signed-{len(self.created) + 1}"
                self.created.append(signed)
                return signed
            def post_order(self, signed, *_a, **_kw):
                self.posted.append(signed)
                return self.responses.pop(0)

        retry = RetryClient()
        trade.time.sleep = lambda *_a, **_kw: None
        trade._client = retry
        trade._ambiguous_condition = None
        check("explicit no-fill retries eventually succeed",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is True)
        check("every FOK retry is rebuilt and re-signed",
              retry.created == ["signed-1", "signed-2", "signed-3"] and
              retry.posted == retry.created, repr((retry.created, retry.posted)))

        class TimeoutClient:
            def __init__(self):
                self.created = 0
                self.posted = 0
            def get_clob_market_info(self, _condition):
                return FakeClient(None).get_clob_market_info(_condition)
            def get_balance_allowance(self, *_a, **_kw):
                return FakeClient(None).get_balance_allowance()
            def create_market_order(self, *_a, **_kw):
                self.created += 1
                return f"ambiguous-{self.created}"
            def post_order(self, *_a, **_kw):
                self.posted += 1
                raise TimeoutError("transport timeout")

        timeout = TimeoutClient()
        trade._client = timeout
        trade._ambiguous_condition = None
        trade._ambiguous_tokens.clear()
        trade._ambiguous_all_tokens = False
        check("ambiguous transport failure is not blindly retried",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is False and
              timeout.created == timeout.posted == 1,
              repr((timeout.created, timeout.posted)))

        trade._client = FakeClient(
            {"success": True, "orderID": "other-side", "status": "matched",
             "tradeIDs": ["trade-other"], **amounts})
        check("the complementary outcome is still placeable after an ambiguous first leg",
              trade.place_trade("DOWN", 2.0, up, down, condition, window_end) is True,
              str(trade.last_order_error))
        trade._client = FakeClient(
            {"success": True, "orderID": "same-side", "status": "matched",
             "tradeIDs": ["trade-same"], **amounts})
        check("the ambiguous outcome stays blocked to prevent a duplicate",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is False
              and "ambiguous" in (trade.last_order_error or ""),
              str(trade.last_order_error))

        import threading
        import time as wall_time
        held = threading.Event()
        release = threading.Event()

        def hold_lock():
            trade._execution_lock.acquire()
            held.set()
            release.wait(5)
            trade._execution_lock.release()

        locker = threading.Thread(target=hold_lock)
        waiter = None
        results = []
        locker.start()
        try:
            check("lock holder started", held.wait(2))
            trade.last_order_error = "keep-me"
            started = wall_time.time()
            skipped = trade.get_balance_allowance()
            elapsed = wall_time.time() - started
            check("a balance poll does not queue behind an in-flight order",
                  skipped is None and elapsed < 1.0
                  and trade.last_order_error == "keep-me",
                  f"elapsed={elapsed:.3f} result={skipped} err={trade.last_order_error}")

            def submit_other_side():
                results.append(trade.place_trade(
                    "DOWN", 2.0, up, down, condition, window_end))

            trade._client = FakeClient(
                {"success": True, "orderID": "after-wait", "status": "matched",
                 "tradeIDs": ["trade-wait"], **amounts})
            trade._ambiguous_condition = None
            trade._ambiguous_tokens.clear()
            trade._ambiguous_all_tokens = False
            waiter = threading.Thread(target=submit_other_side)
            waiter.start()
            threading.Event().wait(0.2)
        finally:
            release.set()
            if waiter is not None:
                waiter.join(5)
            locker.join(2)
        check("the complementary FOK waits out a non-order API holder",
              results == [True], str(results))

        trade._journal_fault = None
        trade._ambiguous_condition = None
        trade._ambiguous_tokens.clear()
        trade._ambiguous_all_tokens = False
        trade.set_order_observer(lambda _receipt: False)
        trade._client = FakeClient(
            {"success": True, "orderID": "unjournaled", "status": "matched",
             "tradeIDs": ["trade-unjournaled"],
             "makingAmount": "2000000", "takingAmount": "4000000"})
        check("an accepted order remains submitted when its journal fails",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is True)
        check("journal failure is explicit on the receipt and process state",
              trade.last_order_receipt["accounting_journaled"] is False and
              "CRITICAL" in (trade.last_order_error or "") and trade._journal_fault)
        check("journal failure disables every later live submission",
              trade.place_trade("UP", 2.0, up, down, condition, window_end) is False)
    finally:
        trade._client = originals["client"]
        trade.Side = originals["Side"]
        trade.OrderType = originals["OrderType"]
        trade.MarketOrderArgs = originals["MarketOrderArgs"]
        trade.PartialCreateOrderOptions = originals["PartialCreateOrderOptions"]
        trade.AssetType = originals["AssetType"]
        trade.BalanceAllowanceParams = originals["BalanceAllowanceParams"]
        trade.time.sleep = originals["sleep"]
        trade.orderbook.validate_buy_liquidity = originals["book"]
        trade.set_order_observer(originals["observer"])
        trade._journal_fault = originals["journal_fault"]
        trade.config.TRADE_LAST_SECONDS = originals["trade_window"]
        trade.config.MIN_SECONDS_TO_EXPIRY = originals["min_expiry"]
        trade._ambiguous_condition = originals["ambiguous_condition"]
        trade._ambiguous_until = originals["ambiguous_until"]
        trade._ambiguous_tokens.clear()
        trade._ambiguous_tokens.update(originals["ambiguous_tokens"])
        trade._ambiguous_all_tokens = originals["ambiguous_all_tokens"]
        trade.config.ASSUMED_MATCH_DELAY_SECONDS = originals["assumed_delay"]


def test_collateral_balance_uses_pusd_units() -> None:
    import polymarket_trade as trade

    original = trade._client
    original_asset_type = trade.AssetType
    original_params = trade.BalanceAllowanceParams

    class BalanceClient:
        def get_balance_allowance(self, *_a, **_kw):
            return {
                "balance": "123450000",
                "allowances": {"exchange": "9000000", "neg_risk": "7500000"},
            }

    trade._client = BalanceClient()
    trade.AssetType = types.SimpleNamespace(COLLATERAL="COLLATERAL")
    trade.BalanceAllowanceParams = lambda **kw: kw
    try:
        result = trade.get_balance_allowance()
        check("pUSD balance converts six-decimal base units",
              result["balance"] == 123.45, str(result))
        check("pUSD allowance converts six-decimal base units",
              result["allowance"] == 7.5, str(result))
    finally:
        trade._client = original
        trade.AssetType = original_asset_type
        trade.BalanceAllowanceParams = original_params


def test_unsent_attempts_are_not_counted_as_failed_sends() -> None:
    """An attempt the bot declined to submit is not a rejected order.

    Regression: `_dist` derived failures as "every row that is not a
    success" (`len(trades) - oks`), so a phase-2 attempt the bot deliberately
    did NOT submit - skipped_unfillable, because the leg priced through
    MAX_BUY_PRICE - was booked as a failed SEND. On a live paper run that
    rendered as `SENT FAIL 1 50%` in SIGNAL DISTRIBUTION beside
    `ORDERS OK/FAIL 1 / 0` and `SEND RATE 100%` in the cash panel: one
    dashboard giving two different answers for the same round. The trade log
    stamped the same row a red REJECT, indistinguishable from a venue
    refusal.
    """
    from dashboard.layout import _trade_sent, _trade_success

    for skipped in ("skipped_unfillable", "skipped_signal_moved",
                    "skipped_pair_would_lose"):
        check(f"{skipped} never reached submission", not _trade_sent(skipped))
        check(f"{skipped} is not a success", not _trade_success(skipped))
    # rejected_or_unsubmitted keeps the bucket it has always had: the row
    # alone cannot say which half of that name applied.
    for submitted in ("paper_filled", "matched", "accepted_pending_confirmation",
                      "rejected_or_unsubmitted"):
        check(f"{submitted} counts as a send", _trade_sent(submitted))

    st = populated()
    st.trades = [
        {"time_et": "Aug 09 21:04:58 ET", "phase": "phase2", "side": "DOWN",
         "amount": 3.0, "price_side": "DOWN", "book_side": "DOWN",
         "chainlink_side": "", "result": "paper_filled"},
        {"time_et": "Aug 09 21:05:11 ET", "phase": "phase2", "side": "DOWN",
         "amount": 2.0, "price_side": "DOWN", "book_side": "DOWN",
         "chainlink_side": "", "result": "skipped_unfillable"},
    ]
    snap = snapshot(st, session_trades=st.trades)
    frame = build(snap, 160, 50, UNICODE)
    lines = ["".join(text for text, _ in row) for row in frame]
    full = "\n".join(lines)

    ok_line = next((ln for ln in lines if "SENT OK" in ln), "")
    fail_line = next((ln for ln in lines if "SENT FAIL" in ln), "")
    decide_line = next((ln for ln in lines if "DECIDE DOWN" in ln), "")
    check("the single submitted order is the whole SENT denominator",
          "1 100%" in ok_line, ok_line)
    check("the unsent attempt is not a failed send",
          "0   0%" in fail_line, fail_line)
    check("both attempts still count as decisions",
          "2 100%" in decide_line, decide_line)
    # The count must still be visible somewhere: silently dropping the row
    # from every bar would be a quieter version of the same bug.
    not_sent_line = next((ln for ln in lines if "NOT SENT" in ln), "")
    check("the skipped attempt is still reported, not dropped",
          "1  50%" in not_sent_line, ascii(not_sent_line))

    # Scoped to the trade log's own rows. A frame-wide search would also hit
    # the PLACE FOK pipeline row, which says REJECTED for populated()'s
    # seeded order-counter sample and has nothing to do with these rows.
    skip_row = next((ln for ln in lines if "21:05:11" in ln), "")
    fill_row = next((ln for ln in lines if "21:04:58" in ln), "")
    check("a never-submitted row reads SKIP, not REJECT",
          "SKIP" in skip_row and "REJECT" not in skip_row, ascii(skip_row[:140]))
    check("the accepted entry is untouched by the new branch",
          "SKIP" not in fill_row and "REJECT" not in fill_row,
          ascii(fill_row[:140]))

    # Geometry is a hard contract for every frame this file builds.
    check("rowcount survives the skip note", len(frame) == 50, str(len(frame)))
    bad = [(i, sum(len(t) for t, _ in row)) for i, row in enumerate(frame)
           if sum(len(t) for t, _ in row) != 160]
    check("width survives the skip note", not bad, f"rows {bad[:3]}")


def test_rejected_send_still_reads_as_a_failed_send() -> None:
    """The fix must not swing the other way and hide real refusals."""
    st = populated()
    st.trades = [
        {"time_et": "Aug 09 21:04:58 ET", "phase": "phase2", "side": "UP",
         "amount": 2.0, "price_side": "UP", "book_side": "UP",
         "chainlink_side": "", "result": "rejected_or_unsubmitted"},
    ]
    snap = snapshot(st, session_trades=st.trades)
    lines = ["".join(text for text, _ in row)
             for row in build(snap, 160, 50, UNICODE)]
    full = "\n".join(lines)
    fail_line = next((ln for ln in lines if "SENT FAIL" in ln), "")
    ok_line = next((ln for ln in lines if "SENT OK" in ln), "")
    check("a refused submission is still a failed send",
          "1 100%" in fail_line, fail_line)
    check("and is not counted as a successful one", "0   0%" in ok_line, ok_line)
    reject_row = next((ln for ln in lines if "21:04:58" in ln), "")
    check("it still reads REJECT in the trade log",
          "REJECT" in reject_row and "SKIP" not in reject_row,
          ascii(reject_row[:140]))
    check("no NOT SENT bar when nothing was skipped",
          not any("NOT SENT" in ln for ln in lines), ascii(full[:200]))


def _with_positions(**over):
    """populated() plus a two-leg round and a third position still settling."""
    st = populated()
    st.tokens.set({"slug": "btc-updown-5m-1754780700",
                   "up_token_id": "72131", "down_token_id": "88214"})
    st.accounting = {
        "realized_pnl": -27.1988,
        "unrealized_mark_to_bid": -0.25,
        "total_pnl": -27.4488,
        "open_position_details": [
            {"token_id": "72131", "shares": 12.5, "average_entry_price": 0.412,
             "cost": 5.15, "mark_bid": 0.38, "unrealized_to_bid": -0.40},
            {"token_id": "88214", "shares": 5.0, "average_entry_price": 0.88,
             "cost": 4.40, "mark_bid": 0.91, "unrealized_to_bid": 0.15},
            {"token_id": "prev-round", "shares": 5.0,
             "average_entry_price": 0.5, "cost": 2.5, "mark_bid": 0.5,
             "unrealized_to_bid": 0.0},
        ],
    }
    for k, v in over.items():
        setattr(st, k, v)
    return st


def _pos_rows(snap, cols: int = 160, rows: int = 50):
    """(all lines, panel block, UP leg row, DOWN leg row) for POSITIONS.

    Legs are located by their share count, not by column spacing: the first
    version of these assertions matched an exact run of spaces and broke the
    moment the SIDE column was retrimmed. The combined "UP / DOWN SHARES"
    footer row repeats the same numbers, so rows holding "/" are excluded.
    """
    lines = ["".join(text for text, _ in row)
             for row in build(snap, cols, rows, UNICODE)]
    start = next(i for i, ln in enumerate(lines) if "POSITIONS" in ln)
    block = lines[start:start + 12]
    def leg(marker):
        return next((ln for ln in block if marker in ln and "/" not in ln), "")
    return lines, block, leg("12.500"), leg("5.000")


def test_positions_panel_replaces_the_band_log_by_default() -> None:
    """The band slot shows the live position while PHASE1 is off.

    PHASE1_ENABLED=0 is this repo's default, so that slot rendered "bands are
    OFF" on every frame while what the position actually was had to be read
    off three other panels. This checks the panel reports each side's held
    shares, the share-weighted average it was built at, the live bid and the
    mark-to-bid P&L - all values the ledger already computes, so the panel
    must not disagree with STAKE / PNL.
    """
    snap = snapshot(_with_positions(), session_trades=[])
    lines, _block, up, down = _pos_rows(snap)
    full = "\n".join(lines)

    check("the positions panel takes the slot", "POSITIONS" in full,
          ascii(full[:200]))
    check("the band log is not also drawn", "BAND TRADES" not in full,
          ascii(full[:200]))

    check("UP row carries shares, average and running cost",
          "0.412" in up and "$5.15" in up, ascii(up[:160]))
    check("UP row carries its mark-to-bid loss", "$-0.40" in up, ascii(up[:160]))
    check("DOWN row carries shares, average and running cost",
          "0.880" in down and "$4.40" in down, ascii(down[:160]))
    check("DOWN row carries its mark-to-bid gain", "$+0.15" in down,
          ascii(down[:160]))

    # BID is the first column shed when the panel is narrow - PNL already
    # carries the mark and MARKET MATRIX shows both live bids - but it must
    # come back once there is room, rather than being dropped for good.
    _wl, _wb, wide_up, _wd = _pos_rows(snap, cols=200)
    check("a wider terminal restores the live bid alongside cost",
          "0.380" in wide_up and "$5.15" in wide_up, ascii(wide_up[:200]))

    # Shares appear nowhere else on the dashboard, so no width may shed them.
    # An earlier version of the COST column dropped SHARES at 120 and 140 to
    # make room, which is the wrong trade.
    for cols in (120, 140, 160, 200, 240):
        narrow, _nb, row_up, row_dn = _pos_rows(snap, cols=cols)
        check(f"UP shares survive at {cols} cols", bool(row_up), str(cols))
        check(f"DOWN shares survive at {cols} cols", bool(row_dn), str(cols))
        # AVG is the price every other figure is judged against; it must not
        # be traded away for COST at any width (it briefly was).
        check(f"UP average price survives at {cols} cols",
              "0.412" in row_up, ascii(row_up[:200]))
        check(f"DOWN average price survives at {cols} cols",
              "0.880" in row_dn, ascii(row_dn[:200]))
        # The combined figure is a full-width row, so it cannot be trimmed
        # out the way a column can.
        # The label abbreviates on a narrow panel; the numbers must not.
        combined = next((ln for ln in narrow
                         if "UP / DOWN SHARES" in ln or "UP / DN SH" in ln), "")
        check(f"combined shares row present at {cols} cols",
              "12.500 / 5.000" in combined, ascii(combined[:200]))

    # Scoped to the panel's own block: TOTAL PNL also appears in STAKE / PNL
    # higher up the frame, at a different precision.
    start = next(i for i, ln in enumerate(lines) if "POSITIONS" in ln)
    block = lines[start:start + 12]
    cost = next((ln for ln in block if "ROUND COST" in ln), "")
    pnl = next((ln for ln in block if "ROUND PNL" in ln), "")
    total = next((ln for ln in block if "TOTAL PNL" in ln), "")
    check("round cost sums both legs (5.15 + 4.40)", "$9.55" in cost,
          ascii(cost[:160]))
    check("round P&L sums both legs (-0.40 + 0.15)", "$-0.25" in pnl,
          ascii(pnl[:160]))
    check("account total P&L is the ledger's, not a re-derivation",
          "$-27.45" in total, ascii(total[:160]))

    # A position on another market is still real money; it must be declared
    # rather than silently excluded from a panel that reads as the book.
    check("positions outside this round are declared", "+1 settling" in full,
          ascii(full[:200]))


def test_positions_panel_invents_nothing_when_flat() -> None:
    """A fresh wallet must render dashes, not zeros."""
    st = populated()
    st.tokens.set({"slug": "s", "up_token_id": "72131",
                   "down_token_id": "88214"})
    st.accounting = {"realized_pnl": 0.0, "open_position_details": []}
    snap = snapshot(st, session_trades=[])
    lines = ["".join(text for text, _ in row)
             for row in build(snap, 160, 50, UNICODE)]
    start = next(i for i, ln in enumerate(lines) if "POSITIONS" in ln)
    block = lines[start:start + 11]
    up = next((ln for ln in block if ln.count("UP") and "SIDE" not in ln), "")
    check("a flat UP leg shows the missing marker, not 0.000",
          "--" in up and "0.000" not in up, ascii(up[:160]))
    check("no fabricated cost",
          not any("$0.00" in ln for ln in block),
          ascii("|".join(block)[:300]))


def test_band_log_reclaims_the_slot_when_bands_are_on() -> None:
    """Turning PHASE1 on must not cost the operator the band record."""
    st = _with_positions()
    st.bands_enabled = True
    snap = snapshot(st, session_trades=st.trades)
    full = "\n".join("".join(text for text, _ in row)
                       for row in build(snap, 160, 50, UNICODE))
    check("bands on -> band log is drawn", "BAND TRADES" in full,
          ascii(full[:200]))
    check("bands on -> positions panel yields the slot",
          "POSITIONS" not in full, ascii(full[:200]))


def test_positions_panel_holds_geometry_at_every_size() -> None:
    """The panel is adaptive; the frame contract is not negotiable."""
    snap = snapshot(_with_positions(), session_trades=[])
    for cols, rows in SIZES:
        frame = build(snap, cols, rows, UNICODE)
        check(f"rowcount positions {cols}x{rows}", len(frame) == rows,
              f"got {len(frame)}")
        bad = [(i, sum(len(t) for t, _ in row))
               for i, row in enumerate(frame)
               if sum(len(t) for t, _ in row) != cols]
        check(f"width positions {cols}x{rows}", not bad, f"rows {bad[:3]}")


# -------------------------------------------------------------------- main ---
def _telemetry_client(trade, response, raises=None):
    """A venue stub whose post_order behaviour the caller chooses."""
    up, down = "101", "202"

    class FakeClient:
        def get_clob_market_info(self, _condition):
            return {"t": [{"o": "Up", "t": up}, {"o": "Down", "t": down}],
                    "mos": "1", "mts": "0.01", "nr": False,
                    "fd": {"r": "0.07", "e": 1, "to": True}, "itode": False}

        def get_balance_allowance(self, *_a, **_kw):
            return {"balance": "100000000",
                    "allowances": {"exchange": "100000000",
                                   "neg_risk": "100000000"}}

        def create_market_order(self, *_a, **_kw):
            return "signed"

        def post_order(self, *_a, **_kw):
            if raises is not None:
                raise raises
            return response

    return FakeClient()


def t_live_attempt_telemetry_records_every_intended_order():
    """An order that never fills must still exist in the telemetry.

    The ledger journals only orders that MATCHED - 221 of 221 entries in the
    real live ledger carried trade evidence - so orders that were rejected or
    simply never filled left no trace at all. Without them there is no live
    fill rate, and with no live fill rate PAPER cannot be calibrated against
    LIVE. That is why the same strategy showed a profit in paper and lost
    money live with nothing in the record to explain it.
    """
    import json
    import os
    import tempfile
    import types
    import polymarket_trade as trade

    up, down = "101", "202"
    condition = "0x" + "a" * 64
    window_end = (int(trade.time.time()) // 300 + 1) * 300
    saved = {
        "client": trade._client,
        "side": getattr(trade, "Side", None),
        "otype": getattr(trade, "OrderType", None),
        "moa": getattr(trade, "MarketOrderArgs", None),
        "pcoo": getattr(trade, "PartialCreateOrderOptions", None),
        "vbl": trade.orderbook.validate_buy_liquidity,
        "asset": getattr(trade, "AssetType", None),
        "bap": getattr(trade, "BalanceAllowanceParams", None),
        "last_secs": trade.config.TRADE_LAST_SECONDS,
        "min_expiry": trade.config.MIN_SECONDS_TO_EXPIRY,
        "observer": None,
        "env": os.environ.get("LIVE_ATTEMPTS_PATH"),
        "ambig_cond": trade._ambiguous_condition,
        "ambig_tokens": set(trade._ambiguous_tokens),
    }
    tmp = tempfile.mkdtemp()
    journal = pathlib.Path(tmp) / "live_attempts.jsonl"
    try:
        os.environ["LIVE_ATTEMPTS_PATH"] = str(journal)
        trade.Side = types.SimpleNamespace(BUY="BUY")
        trade.OrderType = types.SimpleNamespace(FOK="FOK")
        trade.MarketOrderArgs = lambda **kw: kw
        trade.PartialCreateOrderOptions = lambda **kw: kw
        trade.AssetType = types.SimpleNamespace(COLLATERAL="COLLATERAL")
        trade.BalanceAllowanceParams = lambda **kw: kw
        trade.orderbook.validate_buy_liquidity = lambda *_a, **_kw: (
            [{"price": "0.49", "size": "100"}],
            [{"price": "0.50", "size": "100"}])
        trade.config.TRADE_LAST_SECONDS = 300
        trade.config.MIN_SECONDS_TO_EXPIRY = 0
        trade.set_order_observer(lambda _receipt: True)
        trade._ambiguous_condition = None
        trade._ambiguous_tokens = set()

        # 1. an order the venue never fills
        trade._client = _telemetry_client(
            trade, {"success": True, "orderID": "oid-nofill",
                    "status": "unmatched"})
        trade.place_trade("UP", 2.0, up, down, condition, window_end)

        # 2. an order whose submission raises
        trade._client = _telemetry_client(
            trade, None, raises=RuntimeError("transport timeout"))
        trade.place_trade("UP", 2.0, up, down, condition, window_end)

        # 3. an order blocked in preflight, before any network call
        trade.orderbook.validate_buy_liquidity = (
            lambda *_a, **_kw: (_ for _ in ()).throw(
                ValueError("selected token has no asks")))
        trade._client = _telemetry_client(
            trade, {"success": True, "orderID": "oid-x", "status": "matched",
                    "tradeIDs": ["t0"]})
        trade.place_trade("UP", 2.0, up, down, condition, window_end)

        rows = ([json.loads(line) for line in
                 journal.read_text(encoding="utf-8").splitlines() if line.strip()]
                if journal.exists() else [])
    finally:
        trade._client = saved["client"]
        if saved["side"] is not None:
            trade.Side = saved["side"]
            trade.OrderType = saved["otype"]
            trade.MarketOrderArgs = saved["moa"]
            trade.PartialCreateOrderOptions = saved["pcoo"]
        trade.orderbook.validate_buy_liquidity = saved["vbl"]
        if saved["asset"] is not None:
            trade.AssetType = saved["asset"]
            trade.BalanceAllowanceParams = saved["bap"]
        trade.config.TRADE_LAST_SECONDS = saved["last_secs"]
        trade.config.MIN_SECONDS_TO_EXPIRY = saved["min_expiry"]
        trade.set_order_observer(None)
        trade._ambiguous_condition = saved["ambig_cond"]
        trade._ambiguous_tokens = saved["ambig_tokens"]
        if saved["env"] is None:
            os.environ.pop("LIVE_ATTEMPTS_PATH", None)
        else:
            os.environ["LIVE_ATTEMPTS_PATH"] = saved["env"]

    by_event = {}
    for r in rows:
        by_event.setdefault(r.get("event"), []).append(r)
    check("a zero-fill order is recorded as an attempt",
          len(by_event.get("attempt", [])) >= 1, str(sorted(by_event)))
    check("its submissions are recorded even though nothing filled",
          any(r.get("final_status") == "REJECTED"
              for r in by_event.get("post", [])), str(by_event.get("post")))
    check("a submission that raised is recorded, not lost",
          any(r.get("final_status") == "SUBMIT_FAILED"
              for r in by_event.get("post", [])), str(by_event.get("post")))
    check("an order blocked in preflight still appears",
          len(by_event.get("blocked", [])) == 1, str(by_event.get("blocked")))
    attempt = (by_event.get("attempt") or [{}])[0]
    for field in ("attempt_id", "best_bid", "best_ask", "spread",
                  "requested_notional", "limit_price", "expected_shares",
                  "expected_vwap", "expected_fee", "order_type"):
        check(f"the attempt row carries {field}",
              attempt.get(field) is not None, str(attempt))
    ids = {r.get("attempt_id") for r in rows}
    check("each intended order gets its own attempt id",
          len(ids) == 3 and None not in ids, str(ids))
    posts = by_event.get("post", [])
    first_attempt_id = attempt.get("attempt_id")
    check("post events join back to their attempt by attempt_id",
          any(r.get("attempt_id") == first_attempt_id for r in posts),
          str(posts[:2]))


def t_paper_and_live_price_a_complement_from_the_same_interface():
    """Same confirmed fills + same book + same config => same decision.

    LIVE used to have no per-leg cost basis at all, so it refused every
    complement while PAPER priced them - the two modes made different
    decisions from the same information. Both now read open_leg_basis, which
    is derived only from CONFIRMED lots, so only the data source differs.
    """
    import tempfile
    import main_bot
    from accounting.ledger import Ledger

    condition, token = "0x" + "c" * 64, "777"
    saved = main_bot._round_leg_basis_provider
    decisions = {}
    try:
        for label, source in (("paper", "paper_live_book"), ("live", "user_ws")):
            tmp = tempfile.mkdtemp()
            ledger = Ledger(path=str(pathlib.Path(tmp) / "ledger.json"))
            # Identical authoritative fills, recorded the way each mode
            # records them.
            ledger.record_fill_durable(
                f"{label}-1", token, shares=10.0, price=0.40, side="BUY",
                condition_id=condition, status="CONFIRMED", source=source,
                fee=0.168)
            main_bot._round_leg_basis_provider = (
                lambda c, t, _l=ledger: _l.open_leg_basis(c, t))
            decisions[label] = [
                main_bot._pair_profit_guard(condition, token, ask)[0]
                for ask in (0.30, 0.45, 0.58, 0.70)]
    finally:
        main_bot._round_leg_basis_provider = saved

    check("PAPER and LIVE reach the same complement decisions",
          decisions.get("paper") == decisions.get("live"), str(decisions))
    check("and those decisions are not vacuous - cheap allowed, dear refused",
          decisions.get("paper") == [True, True, False, False],
          str(decisions.get("paper")))


def test_terminal_text_fast_paths_are_exact():
    """The cache and the printable-ASCII shortcut must give exactly what the
    per-character pass gives, for every kind of character it handles."""
    import random
    import unicodedata
    from dashboard import safety

    def reference(value, max_chars=4096, redact_secrets=True):
        text = safety.redact(value, max_chars=max_chars) if redact_secrets else safety._as_text(value)
        if max_chars > 0 and len(text) > max_chars:
            text = text[:max_chars] + "...<truncated>"
        out = []
        for char in text:
            code, cat = ord(char), unicodedata.category(char)
            if char in "\r\n\t":
                out.append(" ")
            elif code < 32 or 0x7F <= code <= 0x9F:
                out.append("?")
            elif cat in ("Mn", "Me", "Cf"):
                continue
            elif unicodedata.east_asian_width(char) in ("W", "F"):
                out.append("?")
            else:
                out.append(char)
        return "".join(out)

    rng = random.Random(7)
    alphabet = ([chr(c) for c in range(32, 127)] + list("\r\n\t\x00\x1b\x7f\x85\x9b")
                + ["\u202e", "\u200b", "\u0301", "\u20dd", "\u4e2d", "\uff21", "\u00e9", "\u2014", "\U0001f600"])
    cases = ["", "plain text", "api_key=abc123 Bearer xyz", "0x" + "ab" * 32, "https://u:p@host/x?key=s3cret",
             "x" * 600, "\x1b[2J" + "y" * 30]
    cases += ["".join(rng.choice(alphabet) for _ in range(rng.randint(0, 80))) for _ in range(20000)]
    bad = []
    for c in cases:
        for mc in (0, 16, 4096):
            for rs in (True, False):
                want = reference(c, mc, rs)
                for _ in range(2):                    # second call comes from the cache
                    got = safety.terminal_text(c, mc, redact_secrets=rs)
                    if got != want:
                        bad.append((c[:40], mc, rs, got[:40], want[:40]))
                        break
    for v in (None, 12.5, ValueError("secret=hunter2")):
        if safety.terminal_text(v) != reference(v):
            bad.append((repr(v), "non-str"))
    check("terminal_text: cached and ASCII fast paths equal the per-character pass on 20k strings",
          not bad, str(bad[:3]))


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        try:
            t()
        except Exception as exc:  # a crashing test is a failing test
            global FAIL
            FAIL += 1
            FAILURES.append(f"{t.__name__} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {t.__name__}: {type(exc).__name__}: {exc}")
    print(f"\n{PASS} passed, {FAIL} failed")
    if FAILURES:
        print("\nFailures:")
        for f in FAILURES[:25]:
            print("  -", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
