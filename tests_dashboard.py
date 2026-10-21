#!/usr/bin/env python3
"""Headless tests for the terminal layer.

Two jobs:
  1. Prove the sniper dashboard renders correctly at any terminal size, from
     any snapshot - empty, full, hostile - and never raises.
  2. Prove the dashboard cannot change the bot: it imports nothing from it,
     and the trading files still match their approved digests.

Run:  python tests_dashboard.py
Needs no TTY, no network, no venue credentials.
"""
from __future__ import annotations

import hashlib
import io
import math
import os
import pathlib
import sys
import time
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
    if "clob_sdk" not in sys.modules:
        m = types.ModuleType("clob_sdk")
        for n in ("AssetType", "BalanceAllowanceParams", "ClobClient",
                  "MarketOrderArgs", "OrderType", "PartialCreateOrderOptions", "Side"):
            setattr(m, n, type(n, (), {"__init__": lambda self, *a, **k: None}))
        sys.modules["clob_sdk"] = m


stub_missing()


import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
from dashboard import ConsoleCapture, build, plain_line  # noqa: E402
from dashboard.renderer import (ALT_OFF, ALT_ON, CLEAR, CURSOR_ON,  # noqa: E402
                                PlainRenderer, Renderer, render_row)
from dashboard.theme import UNICODE, Style  # noqa: E402
from dashboard.widgets import big_digits, blank, hsplit, join, pad, panel, table  # noqa: E402

SIZES = [(c, r) for c in (40, 56, 64, 72, 80, 84, 90, 100, 110, 120, 140, 160, 200, 240)
         for r in (10, 12, 14, 16, 18, 20, 24, 28, 30, 34, 40, 48, 60)]


# --------------------------------------------------------------- fixtures ---
def populated(mode: str = "PAPER") -> dict:
    """A snapshot shaped like the one run.DashView builds on a busy round.
    These are inputs to the RENDERER under test, never shown as venue data."""
    now = time.time()
    snap = {
        "mode": mode, "clock": "21:04:58 ET",
        "trigger": {"feed": "binance", "status": "LIVE", "mid": 64_894.12, "move_bp": 0.91, "k_bp": 0.8,
                    "window_ms": 250, "last_signal_bp": -1.2, "last_signal_age_s": 12.0, "signals": 417,
                    "signals_per_min": 5.6, "orders": 12, "filled": 7, "refused": 5, "fill_rate": 0.583,
                    "markout_1s_c": 1.2, "markout_5s_c": -0.4, "order_type": "FAK", "slip": 0.45,
                    "latency_ms": 44.0, "errors": 0, "in_flight": True},
        "market": {"slug": "btc-updown-5m-1754780700", "seconds_left": 123.4, "trade_until_s": 10.4,
                   "book_status": "LIVE",
                   "up": {"bid": 0.45, "ask": 0.47, "status": "LIVE", "age_ms": 120.0},
                   "down": {"bid": 0.52, "ask": 0.55, "status": "LIVE", "age_ms": 80.0},
                   "round_fills": 2, "max_fills": 20, "round_up_sh": 20.0, "round_down_sh": 0.0,
                   "max_side_shares": 200.0},
        "orders": [{"time": f"21:0{i % 10}:0{i % 6}", "side": "UP" if i % 2 else "DOWN", "bp": 0.9 if i % 2 else -1.1,
                    "ask": 0.47, "cap": 0.92, "result": ("filled", "missed", "pending", "error")[i % 4],
                    "shares": 10.0, "price": 0.48, "detail": "FAK no-fill: price ran past the cap"}
                   for i in range(30)],
        "account": {"kind": "PAPER", "cash": 997.08, "equity": 1001.2, "pnl": 1.2, "realized": 3.1, "open": 2,
                    "pending_cost": 9.4, "fees": 0.16, "wins": 5, "losses": 3, "start": 1000.0},
        "canary": None,
        "health": [("BINANCE", "LIVE", "behind p50 3ms backlog max 0 rc 0"),
                   ("POLY BOOK", "LIVE", "rc 0 resync 1"), ("USER WS", "ABSENT", "paper"),
                   ("SETTLE", "OK", ""), ("LEDGER", "OK", ""), ("CLOCK", "OK", "clock synchronized"),
                   ("LOOP LAG", "OK", "p50 8ms p99 15ms")],
        "skips": [("cooldown", 309), ("ask outside price band", 106), ("binance feed behind", 1)],
        "positions": {"rows": [{"side": "UP", "shares": 5.435, "avg": 0.46, "cost": 2.50, "bid": 0.47,
                                "value": 2.55, "pnl": 0.05},
                               {"side": "DOWN", "shares": 0.0, "avg": None, "cost": 0.0, "bid": 0.52,
                                "value": 0.0, "pnl": 0.0}],
                      "pending": {"markets": 1, "shares": 10.0, "cost": 4.6}},
        "settlement": settlement_panel(),
        "pm": pm_fixture(shadow=mode == "LIVE"),
        "exec": {"paper": mode != "LIVE", "latency": exec_latency_fixture(), "quality": {
            "attempted": 120, "sent": 118, "acked": 118, "filled": 70, "partial": 12, "zero_fill": 30,
            "rejected": 2, "failed": 0, "pending": 1, "fill_rate": 0.695, "reject_rate": 0.017,
            "last": {"signal_mid": 64012.5, "ask_seen": 0.55, "cap": 0.6, "price": 0.556, "slip_c": 0.6},
            "avg_slip_c": 0.41, "slip_n": 82}},
        "events": [(now - i, "sniper", f"line {i}", ("info", "good", "warn", "bad")[i % 4])
                   for i in reversed(range(40))],                        # newest last, as run.py keeps them
    }
    if mode == "LIVE":
        snap["account"] = {"kind": "LIVE", "balance": 123.45, "balance_status": "OK", "realized": -2.3, "open": 1,
                           "pending_cost": 4.8, "fees": 0.12, "wins": 3, "losses": 4, "start": "10-07 01:00"}
        snap["canary"] = {"halted": [], "orders": 12, "max_orders": 40, "fills": 7, "max_fills": 20,
                          "spend": 45.2, "max_spend": 60, "rounds": 3, "max_rounds": 3, "loss": 4.2,
                          "max_loss": 10, "guard": "OK", "guard_reason": None, "balance": 123.45, "awaiting": 0,
                          "rules_ready": True}
        snap["health"][2] = ("USER WS", "LIVE", "fills 7")
    return snap


def pm_fixture(shadow=False) -> dict:
    return {"engine": True, "live_orders": False, "shadow": shadow, "orders": 2, "spend": 7.09, "max_orders": 6,
            "max_spend": 25.0,
            "sides": {"UP": {"shares": 20.0, "avg": 0.4512, "cost": 9.02, "best_bid": 0.44, "best_ask": 0.46,
                             "pending": 0.0, "new_avg_if_buy": 0.4581, "state": "BLOCKED",
                             "reason": "IMPROVEMENT_TOO_SMALL"},
                      "DOWN": {"shares": 9.68, "avg": 0.4891, "cost": 4.73, "best_bid": 0.47, "best_ask": 0.49,
                               "pending": 5.0, "new_avg_if_buy": 0.4921, "state": "IMPROVE",
                               "reason": "PAIR_COST_IMPROVEMENT"}},
            "portfolio": {"combined_avg": 0.9403, "paired": 9.68, "unpaired_up": 10.32, "unpaired_down": 0.0,
                          "total_cost": 13.75, "pending": 2.6, "fees": 0.21, "exposure": 4.07}}


def exec_latency_fixture() -> dict:
    st = lambda **kw: dict(dict(n=841, last=0.4, avg=0.42, p50=0.40, p95=0.56, p99=0.67, max=0.82,  # noqa: E731
                                simulated=False), **kw)
    return {"feed_to_trigger": st(p50=0.01), "trigger_to_decision": st(p50=0.04), "build": st(p50=0.12),
            "sign": st(p50=0.11), "dispatch": st(p50=0.19), "send_to_ack": st(p50=187.3), "ack_to_fill": st(p50=41.2),
            "trigger_to_send": st(), "trigger_to_ack": st(last=190.1, avg=192.4, p50=188.0, p95=231.0, p99=402.0,
                                                          max=611.0),
            "trigger_to_fill": st(last=230.0, avg=236.1, p50=229.5, p95=300.2, p99=510.0, max=1210.0, n=168)}


def market_view(label, state, end, **kw) -> dict:
    v = {"condition_id": "0x" + label.replace(":", "").replace("-", "")[:8].ljust(64, "0"), "label": label,
         "state": state, "window_start": end - 300, "window_end": end, "age_s": None, "winner": None,
         "settlement_price": None, "realized": None, "settlement_s": None, "redeem": None, "redeem_amount": None,
         "detail": "", "error": None, "has_position": True, "source": "rotation"}
    v.update(kw)
    return v


def settlement_panel(subject: str = "pending") -> dict:
    """SettlementWorker.panel() as run.DashView labels it: one market trading,
    two waiting for their result, one settled."""
    active = market_view("14:05-14:10", "ACTIVE", 1754781000)
    p1 = market_view("14:00-14:05", "PENDING SETTLEMENT", 1754780700, age_s=6.2, detail="not resolved on chain yet")
    p2 = market_view("13:55-14:00", "PENDING SETTLEMENT", 1754780400, age_s=306.2, error="polygon rpc unavailable")
    done = market_view("13:50-13:55", "SETTLED", 1754780100, winner="UP", settlement_price=1.0, realized=2.31,
                       settlement_s=58.3, redeem="N/A (PAPER)")
    subj = {"pending": p2, "settled": done, "active": active}[subject]
    return {"active": active, "subject": subj, "pending": [p1, p2], "previous": [p1, p2, done],
            "last_settled": done, "pending_count": 2, "redeem_pending": 0.0}


def ui_states() -> list[tuple[str, dict]]:
    halted = populated("LIVE")
    halted["canary"]["halted"] = ["canary daily loss $10.20 >= $10.00", "stop file present: /opt/sniperbtc/STOP_LIVE"]
    kraken = populated()
    kraken["trigger"].update(feed="kraken", status="STALE")
    no_round = populated()
    no_round["market"] = {"slug": None, "seconds_left": None}
    hostile = populated()
    hostile["trigger"].update(mid=float("nan"), move_bp=float("inf"), signals=10**15, fill_rate=-3)
    hostile["market"]["slug"] = "\x1b[2Jbtc\u4e2d\u200b\u202e" + "x" * 500
    hostile["orders"][0]["detail"] = "\x1b]0;pwned\x07\r\nreason\U0001f600"
    hostile["events"].append((time.time(), "\x1b[31m", "\x9b2J" + "y" * 3000, "nonsense-level"))
    hostile["health"].append(("\x1b", None, None))
    hostile["account"]["cash"] = 1e18
    hostile["settlement"]["subject"].update(label="\x1b[2J" + "z" * 300, age_s=float("nan"), detail="\x07" * 50)
    hostile["settlement"]["pending"].append({"state": None, "label": None, "age_s": "x"})
    hostile["positions"]["rows"].append({"side": "\x1b", "shares": float("inf"), "pnl": "nope"})
    hostile["exec"]["latency"]["trigger_to_send"] = {"p50": float("nan"), "last": "x", "max": float("inf")}
    hostile["pm"]["sides"]["UP"].update(reason="\x1b[31m" + "r" * 300, shares=float("nan"), state=None)
    hostile["pm"]["portfolio"] = {"combined_avg": "x", "paired": float("inf")}
    hostile["exec"]["quality"]["last"] = {"signal_mid": "\x1b[2J", "price": None}
    settled = populated()
    settled["settlement"] = settlement_panel("settled")
    idle = populated()
    idle["settlement"] = settlement_panel("active")
    wrong_types = {"mode": 7, "trigger": {"mid": "abc", "signals": "x", "markout_1s_c": "1"},
                   "market": {"up": {"bid": "0.4", "ask": None}, "seconds_left": "soon"},
                   "orders": [{"result": None, "shares": "ten"}], "events": [(None, None, None, None)],
                   "health": [("A", "B", "C")], "skips": [("cooldown", None)]}
    wrong_types["settlement"] = {"subject": "x", "pending": [None, 3], "active": 7}
    wrong_types["positions"] = {"rows": "x", "pending": [1]}
    wrong_types["exec"] = {"latency": [1, 2], "quality": "x"}
    wrong_types["pm"] = {"engine": True, "sides": [1], "portfolio": "x"}
    return [("empty", {}), ("paper", populated()), ("live", populated("LIVE")), ("halted", halted),
            ("kraken", kraken), ("no round", no_round), ("hostile", hostile), ("wrong types", wrong_types),
            ("settled", settled), ("idle", idle)]


def _text(frame) -> str:
    return "\n".join("".join(t for t, _ in row) for row in frame)


def _geometry_ok(frame, cols: int, rows: int) -> tuple[bool, str]:
    bad = [(i, sum(len(t) for t, _ in row)) for i, row in enumerate(frame) if sum(len(t) for t, _ in row) != cols]
    return len(frame) == rows and not bad, f"rows={len(frame)} bad_widths={bad[:3]}"


# ------------------------------------------------------ 1. the sniper frame ---
def test_geometry() -> None:
    for tag, snap in ui_states():
        for cols, rows in SIZES:
            ok, detail = _geometry_ok(build(snap, cols, rows, UNICODE), cols, rows)
            check(f"geometry {tag} {cols}x{rows}", ok, detail)


def test_build_never_raises() -> None:
    """A render exception would stop the screen while the bot trades on unseen."""
    for tag, snap in ui_states():
        for cols, rows in ((1, 1), (3, 2), (40, 10), (120, 40), (240, 60)):
            try:
                frame = build(snap, cols, rows, UNICODE)
                ok, detail = _geometry_ok(frame, cols, rows)
                check(f"build survives {tag} {cols}x{rows}", ok, detail)
            except Exception as exc:
                check(f"build survives {tag} {cols}x{rows}", False, f"{type(exc).__name__}: {exc}")
    frame = build({"trigger": object()}, 80, 24, UNICODE)
    check("a snapshot the layout cannot read is drawn as an error, not raised",
          "dashboard layout error" in _text(frame) and len(frame) == 24, _text(frame)[:120])


def test_one_sided_books_render_at_every_size() -> None:
    """An empty side of the live book (late in a decided round) must not break the frame."""
    for tag, up in (("ask-only", {"bid": None, "ask": 0.99, "status": "LIVE", "age_ms": 5.0}),
                    ("bid-only", {"bid": 0.01, "ask": None, "status": "LIVE", "age_ms": 5.0}),
                    ("missing", None)):
        snap = populated()
        snap["market"]["up"] = up
        for cols, rows in SIZES:
            ok, detail = _geometry_ok(build(snap, cols, rows, UNICODE), cols, rows)
            check(f"one-sided render {tag} {cols}x{rows}", ok, detail)


def test_no_wide_or_control_chars() -> None:
    """A double-width or control character silently shifts every later column."""
    for tag, snap in ui_states():
        for cols, rows in ((120, 40), (84, 24), (200, 60)):
            for row in build(snap, cols, rows, UNICODE):
                for text, _ in row:
                    for ch in text:
                        if not (ord(ch) >= 32 and ch != "\x7f" and not 0x80 <= ord(ch) <= 0x9f):
                            check(f"no control char ({tag})", False, repr(ch))
                        if unicodedata.east_asian_width(ch) in ("W", "F"):
                            check(f"single width ({tag})", False, repr(ch))
    check("control and wide characters never reach a frame", True)


def test_no_hash_glyph_in_any_state() -> None:
    """`#` must never reach the screen: not as a bar or a numeral."""
    for tag, snap in ui_states():
        for cols, rows in ((40, 12), (64, 20), (84, 24), (120, 40), (240, 60)):
            check(f"no hash: {tag} {cols}x{rows}", "#" not in _text(build(snap, cols, rows, UNICODE)))


def test_empty_snapshot_invents_nothing() -> None:
    text = _text(build({}, 120, 40, UNICODE))
    values = text.replace("T-?", "").replace("1s ", "").replace("5s ", "")       # labels, not values
    check("no price is invented", not any(c.isdigit() for c in values), text[:400])
    check("the round says it is waiting", "no round yet" in text, text[:200])
    check("no orders yet is said, not drawn as rows", "no orders yet" in text)


def test_panel_corners_are_drawn() -> None:
    """The top border used to come out one cell too wide, so pad() cut the corner."""
    for cols in (6, 10, 30, 80):
        top = "".join(t for t, _ in panel("TITLE", [], cols, 4, UNICODE)[0])
        check(f"top-right corner present at {cols}", top.endswith(UNICODE.tr) and len(top) == cols, repr(top))
    top = "".join(t for t, _ in panel("T", [], 30, 4, UNICODE, right_note="note")[0])
    check("corner present with a right note", top.endswith(UNICODE.tr) and "note" in top, repr(top))


def test_mode_market_and_account_are_visible() -> None:
    paper = _text(build(populated(), 120, 40, UNICODE))
    check("PAPER is named", "PAPER" in paper)
    check("the market slug and seconds left are shown", "btc-updown-5m-1754780700" in paper and "T-123s" in paper)
    check("UP / DOWN best bid and ask are shown", "0.450" in paper and "0.470" in paper and "0.550" in paper)
    check("BTC mid and the live move are shown", "64,894.12" in paper and "+0.91bp" in paper, paper[:600])
    check("signals per minute are shown", "5.6/min" in paper)
    check("cash and P&L are shown", "$997.08" in paper and "+$1.20" in paper)
    live = _text(build(populated("LIVE"), 120, 40, UNICODE))
    check("LIVE is named and the canary is armed", "LIVE" in live and "CANARY ARMED" in live)
    check("canary limits used are shown", "12/40" in live and "3/3" in live and "$4.20/$10.00" in live, live)
    check("live collateral is shown", "$123.45" in live)


def test_ioc_carry_is_visible() -> None:
    snap = populated()
    snap["market"]["carry"] = {"UP": {"carry": 4.0, "next": 14.0}, "DOWN": {"carry": 0.0, "next": 10.0}}
    text = _text(build(snap, 120, 40, UNICODE))
    row = next((ln for ln in text.splitlines() if "carry" in ln and "->" in ln), "")
    check("the carry row shows each side's carried shares and its next target",
          "4 -> 14" in row and "0 -> 10" in row, row)
    no_carry = _text(build(populated(), 120, 40, UNICODE))
    check("no carry row without IOC carry data (FOK)", not any("carry" in ln and "->" in ln
                                                               for ln in no_carry.splitlines()))
    for cols, rows in SIZES:
        ok, detail = _geometry_ok(build(snap, cols, rows, UNICODE), cols, rows)
        check(f"geometry with carry {cols}x{rows}", ok, detail)


def test_settlement_status_is_always_visible() -> None:
    """MARKET STATUS is never blank: in the header at every size, and in the
    SETTLEMENT block whenever the band fits."""
    for tag, snap in ui_states():
        for cols, rows in ((200, 60), (120, 40), (100, 30), (80, 24), (64, 20)):
            text = _text(build(snap, cols, rows, UNICODE))
            head = text.splitlines()[0] if text else ""
            if cols >= 100:
                check(f"header carries the market status ({tag} {cols}x{rows})",
                      any(s in head for s in ("ACTIVE", "DISCOVERING")), head)
            status = [ln for ln in text.splitlines() if "MARKET STATUS" in ln]
            if rows >= 24 and cols >= 64 and tag not in ("empty", "wrong types"):
                check(f"SETTLEMENT block shown ({tag} {cols}x{rows})", bool(status), text[:300])
            for ln in status:
                value = ln.split("MARKET STATUS", 1)[1].strip(" \u2502")
                check(f"MARKET STATUS has a value ({tag} {cols}x{rows})", bool(value.strip()), ln)


def test_settlement_states_are_spelled_out() -> None:
    pending = _text(build(populated(), 120, 40, UNICODE))
    check("the oldest pending market is the SETTLEMENT subject", "13:55-14:00" in pending, pending)
    check("PENDING SETTLEMENT and WAITING are shown",
          "PENDING SETTLEMENT" in pending and "WAITING" in pending, pending)
    check("its age is shown", "306s" in pending, pending)
    check("the failure it waits on is named", "polygon rpc" in pending, pending)
    check("the header counts pending markets", "2 PENDING SETTLEMENT" in pending.splitlines()[0],
          pending.splitlines()[0])
    check("MARKETS lists the active market and each pending one",
          "ACTIVE MARKET" in pending and "14:05-14:10" in pending and "14:00-14:05 WAITING RESULT" in pending,
          pending)
    check("the position table has its columns",
          all(h in pending for h in ("SIDE", "SHARES", "AVG", "COST", "BID", "VALUE", "P/L")), pending)
    check("a position row reads shares, avg, cost, bid, value, P/L",
          any(all(x in ln for x in ("UP", "5.435", ".460", "$2.50", ".470", "$2.55", "+$0.05"))
              for ln in pending.splitlines()), pending)
    check("ended markets awaiting a result are carried at cost", "cost $4.60" in pending, pending)
    snap = populated()
    snap["settlement"] = settlement_panel("settled")
    settled = _text(build(snap, 120, 40, UNICODE))
    for label in ("WINNER", "SETTLE PRICE", "REALIZED P/L", "SETTLE TIME", "REDEEM"):
        check(f"settled block shows {label}", label in settled, settled)
    check("settled values", "$1.00" in settled and "+$2.31" in settled and "58.3s" in settled
          and "N/A (PAPER)" in settled, settled)
    snap["settlement"] = settlement_panel("active")
    idle = _text(build(snap, 120, 40, UNICODE))
    row = next(ln for ln in idle.splitlines() if "MARKET STATUS" in ln)
    check("an idle block says ACTIVE", "ACTIVE" in row, row)
    check("and names the last settled market", "13:50 UP +$2.31" in idle or "13:50-13:55 UP +$2.31" in idle, idle)
    live = populated("LIVE")
    live["account"].update(redeemable=4.37, position_value=2.55, equity=130.37, unrealized=0.05, total=-2.25)
    live["settlement"] = settlement_panel("settled")
    live["settlement"]["subject"].update(redeem="REDEEM PENDING", redeem_amount=4.37)
    text = _text(build(live, 120, 40, UNICODE))
    check("LIVE shows REDEEMABLE apart from collateral", "REDEEMABLE" in text and "$4.37" in text, text)
    check("LIVE shows REDEEM PENDING with its amount",
          any("REDEEM" in ln and "PENDING $4.37" in ln for ln in text.splitlines()), text)
    live["settlement"]["subject"].update(redeem_remaining_amount=3.37,
                                        redeem_unverified_amount=1.0)
    partial = _text(build(live, 120, 40, UNICODE))
    check("partial redemption shows only the remaining wallet-held payout",
          any("REDEEM" in ln and "PENDING $3.37" in ln for ln in partial.splitlines()), partial)
    live["settlement"]["subject"].update(redeem="REDEMPTION UNVERIFIED",
                                        redeem_remaining_amount=0.0)
    unverified = _text(build(live, 120, 40, UNICODE))
    check("token disappearance is shown as unverified collateral",
          any("UNVERIFIED" in ln and "$1.00" in ln and "collateral" in ln
              for ln in unverified.splitlines()), unverified)


def test_execution_latency_and_quality_are_visible() -> None:
    text = _text(build(populated("LIVE"), 140, 52, UNICODE))
    check("the latency band is drawn when there is room", "SNIPER EXECUTION LATENCY" in text, text)
    for label in ("feed>trig", "decide", "build", "sign", "dispatch", "net>ACK", "ACK>fill", "TRIG>SEND",
                  "TRIG>ACK", "TRIG>FILL", "LAST", "AVG", "P50", "P95", "P99", "MAX"):
        check(f"latency shows {label}", label in text, text)
    row = next((ln for ln in text.splitlines() if "TRIG>ACK" in ln), "")
    check("trigger -> ACK row has its numbers", all(v in row for v in ("190", "192", "188", "231", "402", "611")), row)
    for label in ("EXECUTION QUALITY", "attempted 120", "fill rate 70%", "ref ask .550", "limit .600", "avg fill .556"):
        check(f"quality shows {label}", label in text, text)
    paper = _text(build(populated(), 140, 52, UNICODE))
    check("paper says its ACK and fill are simulated", "simulated" in paper, paper)
    empty = populated("LIVE")
    empty["exec"] = {"latency": {}, "quality": {}, "paper": False}
    text = _text(build(empty, 140, 52, UNICODE))
    check("no samples reads N/A, never a number", "N/A" in next((ln for ln in text.splitlines() if "TRIG>SEND" in ln), ""))


def test_latency_budget_panel() -> None:
    import exec_latency
    snap = populated("LIVE")
    snap["budget"] = exec_latency.latency_budget({"coinbase": 46.3, "binance_fut": 109.4}, "coinbase",
                                                 snap["exec"]["latency"], 34.2)
    text = _text(build(snap, 200, 52, UNICODE))
    check("the budget panel is drawn on a wide terminal", "LATENCY BUDGET" in text, text)
    for label in ("Coinbase feed", "Bot in-process", "Network > CLOB", "Network < CLOB", "Polymarket hold", "Unattributed response delay",
                  "BTC > ACK", "via bin fut"):
        check(f"budget shows {label}", label in text, text)
    row = next((ln for ln in text.splitlines() if "Polymarket hold" in ln), "")
    check("the hold row has its ms and share", "150ms" in row and "%" in row, row)
    check("live venue time is not marked sim", "Unattributed response delay sim" not in text, text)
    check("EXECUTION QUALITY stays beside it", "EXECUTION QUALITY" in text and "SNIPER EXECUTION LATENCY" in text, text)
    paper = populated()
    paper["exec"]["latency"]["send_to_ack"] = dict(paper["exec"]["latency"]["send_to_ack"], simulated=True)
    paper["budget"] = exec_latency.latency_budget({"coinbase": 46.3}, "coinbase", paper["exec"]["latency"], 34.2)
    ptext = _text(build(paper, 200, 52, UNICODE))
    check("paper marks its venue time sim", "Unattributed response delay sim" in ptext, ptext)
    check("no second-trigger total without a second trigger", "via " not in next(
        (ln for ln in ptext.splitlines() if "BTC > ACK" in ln), "via"), ptext)
    narrow = _text(build(snap, 120, 52, UNICODE))
    check("a narrower terminal keeps the old two panels", "LATENCY BUDGET" not in narrow
          and "EXECUTION QUALITY" in narrow, narrow)
    unknown = populated("LIVE")
    unknown["budget"] = exec_latency.latency_budget({"coinbase": 46.3}, "coinbase", {}, None)
    utext = _text(build(unknown, 200, 52, UNICODE))
    row = next((ln for ln in utext.splitlines() if "BTC > ACK" in ln), "")
    check("unmeasured parts read N/A and the total is a lower bound", ">=196ms" in row
          and "N/A" in next((ln for ln in utext.splitlines() if "Unattributed response delay" in ln), ""), utext)
    for bad in ({"feeds": "x", "totals": None}, {"primary": None, "feeds": {"coinbase": float("nan")}},
                {"bot_ms": "fast", "hold_ms": -5, "totals": {"coinbase": float("inf")}}):
        hostile = populated("LIVE")
        hostile["budget"] = bad
        frame = build(hostile, 200, 52, UNICODE)
        check(f"a malformed budget is drawn, never raised ({bad})",
              "layout error" not in _text(frame) and all(sum(len(t) for t, _ in r) == 200 for r in frame))


def test_net_line_colours_each_path_by_its_own_limits() -> None:
    from dashboard.sniper_layout import net_style
    snap = populated()
    snap["net"] = {"coinbase": 48.0, "clob": 95.0, "binance": None}
    frame = build(snap, 120, 40, UNICODE)
    row = next((r for r in frame if "NET ms" in "".join(t for t, _ in r)), None)
    check("the NET line is drawn", row is not None, _text(frame))
    text = "".join(t for t, _ in row or [])
    check("each path with its ms (unmeasured reads --)",
          all(p in text for p in ("coinbase 48", "clob 95", "binance --")), text)
    style = {t: s for t, s in row or []}
    check("a fast feed is green, a slow CLOB red", style.get("48", Style()).fg == "green"
          and style.get("95", Style()).fg == "red", str(row))
    snap["net"] = {"coinbase": 48.0, "clob": 24.0, "binance": 212.0}
    for cols in (84, 120, 160):
        row = next((r for r in build(snap, cols, 40, UNICODE) if "NET ms" in "".join(t for t, _ in r)), [])
        text = "".join(t for t, _ in row)
        check(f"all three values fit at {cols} columns (names shorten when narrow)",
              all(v in text for v in (" 48", " 24", " 212")), text)
    check("limits are per path: 200ms is fine for Binance, not for the CLOB",
          net_style("binance", 200.0).fg == "green" and net_style("clob", 200.0).fg == "red"
          and net_style("coinbase", 120.0).fg == "amber" and net_style("clob", None).fg == "faint")
    check("no NET line when the snapshot has none", "NET " not in _text(build(populated(), 120, 40, UNICODE)))


def test_position_manager_panel() -> None:
    text = _text(build(populated(), 120, 60, UNICODE))
    check("the manager panel is drawn when its engine is on and there is room", "POSITION MANAGER" in text, text)
    for label in ("Shares", "Avg+fees", "Cost", "Best Bid", "Best Ask", "Pending", "New Avg+fees", "Opportunity",
                  "Comb Avg+fees", "Paired", "Unpaired UP", "Unpaired DOWN", "Total Cost", "Pending Exp", "Fees"):
        check(f"manager shows {label}", label in text, label)
    check("each side's state and full reason are readable",
          "IMPROVEMENT_TOO_SMALL" in text and "PAIR_COST_IMPROVEMENT" in text and "BLOCKED" in text
          and "IMPROVE" in text, text)
    check("values are shown", "$0.4512" in text and "$0.9403" in text and "10.32" in text, text)
    # a missing value says why, never a bare "--"
    blanks = populated()
    blanks["pm"]["sides"]["UP"].update(best_bid=None, best_ask=None, blank="STALE", state="BLOCKED", reason="STALE_BOOK")
    blanks["pm"]["sides"]["DOWN"].update(best_bid=None, best_ask=None, blank="WAITING")
    blanks["pm"]["unknown_orders"] = 1
    bt = _text(build(blanks, 160, 60, UNICODE))
    seg = bt[bt.find("POSITION MANAGER"):]
    check("missing manager values say STALE / WAITING, not '--'", "STALE" in seg and "WAITING" in seg, seg[:400])
    check("an UNKNOWN manager order is shown", "1 UNKNOWN" in bt, bt)
    live = _text(build(populated("LIVE"), 120, 60, UNICODE))
    check("live says the manager is shadow-only", "SHADOW (no orders)" in live, live)
    off = populated()
    off["pm"] = {"engine": False}
    check("no panel while the engine is off", "POSITION MANAGER" not in _text(build(off, 120, 60, UNICODE)))
    for cols, rows in ((80, 60), (120, 60), (200, 60)):
        ok, detail = _geometry_ok(build(populated(), cols, rows, UNICODE), cols, rows)
        check(f"geometry with the manager {cols}x{rows}", ok, detail)


def test_live_canary_halt_is_visible() -> None:
    snap = dict(ui_states())["halted"]
    text = _text(build(snap, 120, 40, UNICODE))
    check("a halted canary says HALTED in the header", "HALTED" in text.splitlines()[0], text.splitlines()[0])
    check("every halt reason is printed", "canary daily loss $10.20 >= $10.00" in text, text)
    joined = "".join(ln.split("│")[-2][10:].rstrip() for ln in text.splitlines()
                     if "│" in ln and ("HALT" in ln or ln.split("│")[-2].startswith(" " * 10)))
    check("a long halt reason wraps instead of being cut (it names the stop file to remove)",
          "/opt/sniperbtc/STOP_LIVE" in joined, joined)
    narrow = _text(build(snap, 80, 40, UNICODE))
    check("halted is still in the header at 80 columns", "HALTED" in narrow.splitlines()[0])
    newest = _text(build(populated(), 120, 40, UNICODE)).split("EVENTS")[1].splitlines()[1]
    check("events are newest first", "line 0" in newest, newest)


def test_orders_newest_first_and_labelled() -> None:
    snap = populated()
    snap["orders"] = [
        {"time": "21:00:01", "side": "UP", "bp": 0.9, "ask": 0.47, "cap": 0.92, "result": "filled",
         "shares": 10.0, "price": 0.48, "detail": ""},
        {"time": "21:00:02", "side": "DOWN", "bp": -1.1, "ask": 0.51, "cap": 0.95, "result": "missed",
         "shares": None, "price": None, "detail": "FAK no-fill"},
    ]
    lines = _text(build(snap, 120, 40, UNICODE)).splitlines()
    first = next(i for i, ln in enumerate(lines) if "21:00:02" in ln)
    second = next(i for i, ln in enumerate(lines) if "21:00:01" in ln)
    check("the newest order is on top", first < second, f"{first} {second}")
    check("a fill shows side, ask, limit and shares@price",
          all(s in lines[second] for s in ("UP", "0.470", "0.920", "FILLED", "10.0@0.480")), lines[second])
    check("a miss is labelled MISSED with no fill", "MISSED" in lines[first] and "@" not in lines[first],
          lines[first])


def test_plain_line_and_non_tty_emit_no_escapes() -> None:
    out = io.StringIO()          # isatty() False
    r = PlainRenderer(out, every=0.0)
    r.status(plain_line(populated()))
    body = out.getvalue()
    check("plain renderer emits no escapes", "\x1b" not in body, repr(body[:60]))
    check("plain renderer emits a status line", body.startswith("[DASH] PAPER btc-updown-5m-1754780700 T-123s"),
          body)
    check("the line names signals, orders and P&L", "signals=417" in body and "orders=12" in body
          and "pnl=+$1.20" in body, body)
    check("plain line survives an empty snapshot", plain_line({}).startswith("[DASH]"))
    check("make_renderer picks plain for a pipe",
          type(__import__("dashboard.renderer", fromlist=["make_renderer"])
               .make_renderer(io.StringIO())).__name__ == "PlainRenderer")


def test_console_capture_turns_prints_into_events_and_restores() -> None:
    import logging
    events = []
    real_out, real_err = sys.stdout, sys.stderr
    with ConsoleCapture(lambda *e: events.append(e)) as terminal:
        check("the renderer gets the real terminal", terminal is real_out)
        print("[PAPER] FOK rejected: price ran past the cap")
        print("partial ", end="")
        print("line\x1b[2J")
        sys.stderr.write("Traceback (most recent call last):\n")
        logging.getLogger("accounting.settlement").error("settle failed")
    check("stdout and stderr are restored", sys.stdout is real_out and sys.stderr is real_err)
    check("a tagged line becomes a tagged event",
          ("paper", "FOK rejected: price ran past the cap", "warn") in events, str(events))
    check("partial writes join into one line, escapes neutralised",
          any(e[1].startswith("partial line") and "\x1b" not in e[1] for e in events), str(events))
    check("stderr becomes a bad event", any(e[2] == "bad" and "Traceback" in e[1] for e in events), str(events))
    check("logging is routed too", any("settle failed" in e[1] for e in events), str(events))
    try:
        with ConsoleCapture(lambda *e: None):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    check("streams are restored after an exception", sys.stdout is real_out and sys.stderr is real_err)


def test_dashboard_imports_nothing_from_the_bot() -> None:
    """The dashboard draws a snapshot; it must not be able to reach trading code."""
    import ast as _ast
    bot = {"run", "sniper_mode", "sniper_live", "paper_trade", "polymarket_trade", "orderbook", "config",
           "feeds", "accounting", "market_discovery", "timer", "http_pool", "fast_h2", "main_bot", "strategy"}
    for path in sorted((ROOT / "dashboard").glob("*.py")):
        tree = _ast.parse(path.read_text(encoding="utf-8"))
        names = set()
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Import):
                names |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, _ast.ImportFrom) and node.level == 0 and node.module:
                names.add(node.module.split(".")[0])
        check(f"dashboard/{path.name} imports nothing from the bot", not (names & bot), str(names & bot))


# ------------------------------------------------------------ 2. renderer ---
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


class FakeTTY(io.StringIO):
    def isatty(self) -> bool:
        return True


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


def test_render_row_resets() -> None:
    line = render_row(pad([("hi", Style("green"))], 10))
    check("row ends reset", line.endswith("\x1b[0m"))
    check("alt on constant", ALT_ON.startswith("\x1b"))


def test_frame_diff() -> None:
    snap = populated()
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


def test_a_console_that_takes_part_of_a_write_still_gets_the_whole_frame() -> None:
    """A Windows console takes ~16k characters per write and says how many it
    took; the rest was lost, and the renderer never redrew those rows."""
    class Chunky(io.RawIOBase):
        def __init__(self, limit):
            self.limit, self.data, self.calls = limit, bytearray(), []

        def writable(self):
            return True

        def write(self, b):
            n = min(len(b), self.limit)
            self.data += bytes(b[:n])
            self.calls.append(n)
            return n

    class Console(io.TextIOWrapper):
        def isatty(self):
            return True

    raw = Chunky(1000)
    tty = Console(raw, encoding="utf-8", write_through=True)
    r = Renderer(tty)
    r.cols, r.rows = 160, 48
    r.size = lambda: (160, 48)
    r.start()
    frame = build(populated("LIVE"), 160, 48, r.g)
    r.draw(frame)
    r.stop()
    out = raw.data.decode("utf-8")
    check("a frame bigger than one console write was split", len(raw.calls) > 3, str(len(raw.calls)))
    check("no part write took more than the console allows", max(raw.calls) <= 1000, str(max(raw.calls)))
    check("every row of the frame reached the console",
          all(f"\x1b[{i};1H" in out for i in range(1, 49)), out[-200:])
    check("the frame is wrapped in synchronized output", "\x1b[?2026h" in out and "\x1b[?2026l" in out)
    last_row = render_row(frame[-1])
    check("the last row arrived intact", last_row in out, last_row[:60])


def test_write_all_refuses_a_writer_that_takes_nothing() -> None:
    from dashboard.renderer import write_all

    class Dead:
        def write(self, b):
            return 0
    try:
        write_all(Dead(), b"frame")
        check("a dead writer raises instead of spinning", False, "returned")
    except OSError:
        check("a dead writer raises instead of spinning", True)


def test_no_hash_in_rendered_bytes() -> None:
    """Through the real renderer, escape codes and all."""
    for cols, rows in ((120, 40), (84, 24)):
        tty = FakeTTY()
        r = Renderer(tty)
        r.cols, r.rows = cols, rows
        r.start()
        r.draw(build(populated(), cols, rows, r.g))
        r.stop()
        out = tty.getvalue()
        check(f"renderer emits no hash {cols}x{rows}", "#" not in out, repr(out[:80]))
        check(f"renderer emits box glyphs {cols}x{rows}", UNICODE.tl in out and UNICODE.tr in out)


def test_frame_cost_is_small() -> None:
    """A frame competes with the sniper for the VPS's one CPU at DASH_FPS=2."""
    snap = populated("LIVE")
    t = time.perf_counter()
    for _ in range(20):
        build(snap, 160, 48, UNICODE)
    ms = (time.perf_counter() - t) * 1000.0 / 20
    check("a 160x48 frame builds in well under 50ms", ms < 50.0, f"{ms:.1f}ms")


# --------------------------------------------- 3. the bot is unchanged ---
def test_trading_file_baselines() -> None:
    """The same approved digests tests_feeds holds: the dashboard work changed no trading file."""
    import tests_feeds
    for name in tests_feeds.TRADING_FILES:
        raw = (ROOT / name).read_bytes().replace(b"\r\n", b"\n")
        digest = hashlib.sha256(raw).hexdigest()
        check(f"approved baseline {name}", digest == tests_feeds.BASELINE_SHA[name],
              f"{digest[:12]} != {tests_feeds.BASELINE_SHA[name][:12]}")


# ------------------------------------------ 4. the venue client it shows ---
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
        "exec_window": trade.config.EXECUTION_WINDOW_SECONDS,
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
    trade.config.EXECUTION_WINDOW_SECONDS = 300     # what _validate_round_end reads
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
        trade.config.EXECUTION_WINDOW_SECONDS = originals["exec_window"]
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
