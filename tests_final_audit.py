#!/usr/bin/env python3
"""Final end-to-end audit (2026-10-07): independent-oracle tests of the whole bot.

    python tests_final_audit.py

Every expected value here is computed by the test itself - Decimal/Fraction
re-implementations of the rule written from the specification - never by the
production function under test. Sections follow the audit request:

  CONFIG      frozen spec, old values, malformed/NaN/inf/negative input (D1, D2)
  TRIGGER     0.49/0.50/0.51bp, 50,000 random boundary cases, the 250ms window
              against a brute-force oracle, gaps, duplicates, clock reversal
  BOOK        empty/one-sided/crossed/duplicate/negative/zero/invalid/out-of-order
              /stale/rapid replacement/huge depth, a refused update desyncs (D5)
  EXECUTION   reference ask anchoring, +$0.05 cap, tick rounding, FAK on the
              sniper path, 20,000 signed-amount combinations, carry, cooldown,
              per-side concurrency, empty book at the match (D6)
  ACCOUNTING  ledger/cash/realized/unrealized against a Fraction oracle,
              fractional shares, duplicate and out-of-order fills, settlement
              exactly once
  MANAGER     pair economics (fractional new pairs, threshold +-eps), the
              original bad pair, VWAP, lower average with more risk,
              anti-martingale ladder, the sniper's 20-share reserve, priority,
              UNKNOWN timeout, duplicate outcomes, stale snapshots, shadow
              isolation, the live hard-disable, double use of depth (D4)
  SETTLEMENT  lifecycle, idempotency, A/C pending while B trades, never blocks
  RESTART     what is persisted; round caps after a restart (R1); process lock
  SECURITY    no secrets in tracked files, creds never in reprs/config lines
  ERRORS      no bare except in production code

No network, no wallet, no .env (testing_env).
"""
from __future__ import annotations

import ast
import asyncio
import collections
import json
import math
import os
import pathlib
import random
import re
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from dataclasses import replace
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal as D
from fractions import Fraction as F
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency  # noqa: E402
import position_manager as PM  # noqa: E402
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402  (paper harness: Market, PaperIOCBase)
from accounting.ledger import Ledger  # noqa: E402
from accounting import settlement as ST  # noqa: E402
from accounting.resolution import RESOLVED, PENDING, Resolution  # noqa: E402
from feeds.book import BookState  # noqa: E402
from paper_trade import MarketRules, PaperBroker  # noqa: E402

PRODUCTION = ["run.py", "sniper_mode.py", "sniper_live.py", "paper_trade.py", "polymarket_trade.py",
              "position_manager.py", "exec_latency.py", "market_discovery.py", "timer.py", "config.py",
              "fast_h2.py", "http_pool.py", "onchain_resolution.py", "orderbook.py",
              "accounting/ledger.py", "accounting/settlement.py", "accounting/resolution.py", "accounting/fees.py",
              "feeds/book.py", "feeds/poly_user.py", "feeds/poly_market.py", "feeds/hub.py", "feeds/reconcile.py",
              "dashboard/sniper_layout.py", "dashboard/renderer.py", "dashboard/widgets.py", "dashboard/console.py"]


def env(*maps, **kw):
    """A context manager: these environment values (later ones win), the rest
    restored after."""
    over = {}
    for m in maps + (kw,):
        over.update(m)
    class _Env:
        def __enter__(self):
            self.saved = {k: os.environ.get(k) for k in over}
            for k, v in over.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = str(v)
            return self

        def __exit__(self, *exc):
            for k, v in self.saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
    return _Env()


SPEC = {"SNIPER_ONLY": "1"}
for _attr, _var, _val, _label, _fmt in exec_latency.FROZEN_SPEC:
    SPEC[_var] = None                 # unset: the code defaults must BE the spec


# =========================================================================== CONFIG
class Config(unittest.TestCase):
    def test_effective_sniper_configuration_is_the_spec_from_code_defaults(self):
        with env(SPEC):
            s = S.Settings()
            lim = L.limits_from_env({})
        got = (s.k_bp, s.window_ms, s.order_type, s.slip, s.cooldown_ms, s.shares, s.max_target_shares,
               lim.max_order_shares)
        self.assertEqual(got[:7], (0.5, 250.0, "FAK", 0.05, 300.0, 10.0, 20.0))
        self.assertGreaterEqual(lim.max_order_shares, 20.0)                    # canary cap >= max target
        self.assertEqual(exec_latency.spec_violations(s, lim.max_order_shares), [])

    def test_old_values_cannot_silently_restore_the_old_strategy(self):
        olds = {"SNIPER_K_BP": "1.0", "SNIPER_ORDER_TYPE": "FOK", "SNIPER_SLIP": "0.45",
                "SNIPER_COOLDOWN_MS": "1000", "SNIPER_WINDOW_MS": "500"}
        for var, raw in olds.items():
            with env(SPEC, {var: raw}):
                bad = exec_latency.spec_violations(S.Settings(), 25)
            self.assertTrue(any(var in b for b in bad), (var, bad))
        with env(SPEC):
            bad = exec_latency.spec_violations(S.Settings(), 10)       # the old canary cap of 10 shares
        self.assertTrue(any("canary_cap" in b for b in bad), bad)

    def test_live_gate_refuses_an_old_value_and_the_manager_live_flag(self):
        import run
        for extra in ({"SNIPER_K_BP": "1.0"}, {"SNIPER_ORDER_TYPE": "FOK"}, {"SNIPER_CANARY_MAX_SHARES": "10"},
                      {"POSITION_MANAGER_LIVE_ENABLED": "1"}, {"SNIPER_CANARY_MAX_SPEND_USD": "nan"}):
            with env(SPEC, {"SNIPER_LIVE_CANARY": "1"}, extra):
                with self.assertRaises((RuntimeError, ValueError), msg=extra):
                    run._live_sniper_gate(paper=False)
        with env(SPEC, {"SNIPER_LIVE_CANARY": "1"}):
            self.assertTrue(run._live_sniper_gate(paper=False))             # the spec itself starts

    def test_D2_malformed_nonfinite_negative_sniper_values_fail_closed(self):
        cases = [("SNIPER_K_BP", "1.0bp"), ("SNIPER_K_BP", "nan"), ("SNIPER_K_BP", "inf"), ("SNIPER_K_BP", "0"),
                 ("SNIPER_K_BP", "-0.5"), ("SNIPER_WINDOW_MS", "0"), ("SNIPER_WINDOW_MS", "abc"),
                 ("SNIPER_SLIP", "-0.01"), ("SNIPER_SLIP", "0.5"), ("SNIPER_SLIP", "NaN"),
                 ("SNIPER_COOLDOWN_MS", "-300"), ("SNIPER_COOLDOWN_MS", "nan"), ("SNIPER_SHARES", "4"),
                 ("SNIPER_MAX_TARGET_SHARES", "9"), ("SNIPER_MAX_SIDE_SHARES", "5O"),
                 ("SNIPER_MAX_SIDE_SHARES", "0"), ("SNIPER_MIN_LEFT_S", "-1"), ("SNIPER_STALE_MS", "0"),
                 ("SNIPER_MAX_FILLS_ROUND", "abc"), ("SNIPER_MAX_FILLS_ROUND", "0"), ("SNIPER_ORDER_TYPE", "IOC2"),
                 ("SNIPER_BOOK_WAIT_MS", "-1"), ("SNIPER_LATENCY_MS", "inf")]
        for var, raw in cases:
            with env(SPEC, {var: raw}):
                with self.assertRaises(ValueError, msg=f"{var}={raw!r}"):
                    S.Settings()

    def test_D1_canary_caps_fail_closed(self):
        # before the fix: "nan" disabled the cap (any order passed), "3O" became 25
        for var in ("SNIPER_CANARY_MAX_SHARES", "SNIPER_CANARY_MAX_SPEND_USD", "SNIPER_CANARY_MAX_LOSS_USD",
                    "SNIPER_CANARY_MAX_ORDERS", "SNIPER_CANARY_MAX_FILLS", "SNIPER_CANARY_MAX_ROUNDS"):
            for raw in ("nan", "NaN", "inf", "-inf", "-1", "3O", "abc", "1e400"):
                with self.assertRaises(ValueError, msg=f"{var}={raw!r}"):
                    L.limits_from_env({var: raw})
        for var in ("SNIPER_CANARY_MAX_ORDERS", "SNIPER_CANARY_MAX_FILLS", "SNIPER_CANARY_MAX_ROUNDS"):
            with self.assertRaises(ValueError):
                L.limits_from_env({var: "2.5"})
        lim = L.limits_from_env({"SNIPER_CANARY_MAX_SHARES": "0"})           # zero: nothing may be sent
        with self.assertRaises(L.Refused):
            lim.check(1.0, 5.0, "c", L.KillSwitch(None))
        self.assertTrue(exec_latency.spec_violations(S.Settings(), float("nan")))   # NaN cap is named too

    def test_every_canary_cap_holds_at_its_boundary(self):
        lim = L.limits_from_env({})
        kill = L.KillSwitch(None)
        lim.check(1.0, lim.max_order_shares, "c", kill)                              # exactly the cap: ok
        with self.assertRaises(L.Refused):
            lim.check(1.0, lim.max_order_shares + 1e-6, "c", kill)
        lim.exposure_usd = lim.max_exposure_usd - 1.0
        lim.check(1.0, 5.0, "c", kill)                                               # exactly the spend cap
        with self.assertRaises(L.Halted):
            lim.check(1.0 + 1e-6, 5.0, "c", L.KillSwitch(None))
        with self.assertRaises(L.Halted):
            lim.check(1.0, 5.0, "c", L.KillSwitch(None), realized_loss_usd=lim.max_daily_loss_usd)

    def test_manager_configuration_fails_closed_and_never_exceeds_global_limits(self):
        for raw in ("nan", "inf", "-1", "abc"):
            with self.assertRaises(ValueError):
                PM.PMConfig.from_env({"POSITION_MANAGER_MAX_ORDER_USD": raw})
        with self.assertRaises(ValueError):
            PM.PMConfig.from_env({"POSITION_MANAGER_EXPIRY_TIERS": "60:0.5"})
        cfg = PM.PMConfig.from_env({"POSITION_MANAGER_ENABLED": "1", "POSITION_MANAGER_MAX_UP_SHARES": "999",
                                    "POSITION_MANAGER_MAX_TOTAL_EXPOSURE": "999"},
                                   global_max_buy_price=0.80, global_max_side_shares=50, global_max_unsettled_usd=30)
        self.assertEqual((cfg.max_buy_price, cfg.max_up_shares, cfg.max_total_exposure_usd), (0.80, 50.0, 30.0))
        self.assertFalse(PM.PMConfig.from_env({}).enabled)                  # off by default
        self.assertFalse(PM.PMConfig.from_env({}).live_enabled)

    def test_shell_environment_wins_over_dotenv(self):
        import config
        src = (ROOT / "config.py").read_text(encoding="utf-8")
        self.assertIn("load_dotenv(_ENV_PATH, override=False", src)           # .env never overrides the shell
        self.assertTrue(config.MIN_BUY_PRICE < config.MAX_BUY_PRICE)


# =========================================================================== TRIGGER
def oracle_signals(ticks, k=D("0.5"), window=250, max_gap=2000):
    """Brute force, from the rule as specified: for each tick, the reference is
    the latest earlier tick at least `window` ms older in the same unbroken run
    (a run breaks on time going backwards or standing still, a gap over
    max_gap, or an invalid price), no more than max_gap ms older. Signal when
    |mid/ref - 1| * 1e4 >= k (exact decimal)."""
    out, run = [], []
    for t, mid in ticks:
        if not (math.isfinite(t) and math.isfinite(mid) and mid > 0):
            run = []
            out.append(None)
            continue
        if run and (t <= run[-1][0] or t - run[-1][0] > max_gap):
            run = []
        run.append((t, mid))
        refs = [x for x in run[:-1] if x[0] <= t - window]
        if not refs or t - refs[-1][0] > max_gap:
            out.append(None)
            continue
        ref = refs[-1]
        move = (D(repr(mid)) / D(repr(ref[1])) - 1) * 10000
        out.append(1 if move >= k else -1 if move <= -k else None)
    return out


class Trigger(unittest.TestCase):
    def one(self, ref, mid, dt=260.0):
        t = S.Trigger(0.5, 250)
        t.update(0.0, ref)
        return t.update(dt, mid)[0]

    def test_049_050_051(self):
        for ref in (60_000.0, 100_000.0, 123_456.78):
            for bp, want_up in ((D("0.49"), None), (D("0.50"), 1), (D("0.51"), 1)):
                mid = float(D(repr(ref)) * (1 + bp / 10000))
                exact = (D(repr(mid)) / D(repr(ref)) - 1) * 10000
                want = 1 if exact >= D("0.5") else None
                self.assertEqual(self.one(ref, mid), want, (ref, bp, exact))
                mid = float(D(repr(ref)) * (1 - bp / 10000))
                exact = (D(repr(mid)) / D(repr(ref)) - 1) * 10000
                want = -1 if exact <= D("-0.5") else None
                self.assertEqual(self.one(ref, mid), want, (ref, -bp, exact))

    def test_50000_random_boundary_cases_on_real_price_grids(self):
        rng, wrong = random.Random(20261007), []
        for _ in range(50_000):
            cents = rng.randrange(3_000_000, 15_000_000)                # $30,000.00 .. $150,000.00
            ref = D(cents) / 100
            # a mid on the 0.01 grid right around +-0.5bp (exact hits, one cent under, one over)
            step = ref * D("0.00005")
            sign = rng.choice((1, -1))
            mid = (ref + sign * step).quantize(D("0.01"), rounding=ROUND_FLOOR) + D("0.01") * rng.choice((-1, 0, 1))
            exact = (mid / ref - 1) * 10000
            want = 1 if exact >= D("0.5") else -1 if exact <= D("-0.5") else None
            got = self.one(float(ref), float(mid))
            if got != want:
                wrong.append((str(ref), str(mid), str(exact), got))
        self.assertEqual(wrong[:5], [])

    def test_exact_half_bp_on_the_binance_half_cent_grid(self):
        rng, missed = random.Random(5), 0
        for _ in range(20_000):
            ref = D(rng.randrange(6_000_000, 30_000_000)) / D(200)        # 0.005 grid
            for sign in (1, -1):
                mid = ref + sign * ref / 20000
                if mid != mid.quantize(D("0.005")):
                    continue
                if self.one(float(ref), float(mid)) != sign:
                    missed += 1
        self.assertEqual(missed, 0)

    def test_window_boundaries_249_250_251(self):
        ref, mid = 60_000.0, 60_006.0                                       # 1bp
        self.assertIsNone(self.one(ref, mid, dt=249.0))                     # no reference 250ms old yet
        self.assertEqual(self.one(ref, mid, dt=250.0), 1)                   # exactly 250ms: the reference
        self.assertEqual(self.one(ref, mid, dt=251.0), 1)

    def test_random_tick_streams_against_the_brute_force_oracle(self):
        rng, bad = random.Random(11), []
        for case in range(3_000):
            t, mid, ticks = 0.0, 60_000.0 + rng.random() * 1000, []
            for _ in range(rng.randrange(2, 40)):
                r = rng.random()
                if r < 0.04:
                    t += rng.choice((0.0, -5.0))                              # duplicate / reversed timestamp
                elif r < 0.07:
                    t += rng.uniform(2001, 5000)                              # a stall (gap)
                elif r < 0.5:
                    t += rng.uniform(0.1, 30)                                 # a burst
                else:
                    t += rng.uniform(30, 400)                                 # sparse
                if rng.random() < 0.02:
                    m = rng.choice((float("nan"), float("inf"), -1.0, 0.0))
                else:
                    mid = round(mid * (1 + rng.uniform(-1.2e-4, 1.2e-4)), 2)
                    m = mid
                ticks.append((t, m))
            trig = S.Trigger(0.5, 250)
            got = [trig.update(tt, mm)[0] for tt, mm in ticks]
            want = oracle_signals(ticks)
            if got != want:
                bad.append((case, ticks, got, want))
        self.assertEqual(len(bad), 0, bad[:1])

    def test_direction_up_is_up_down_is_down(self):
        self.assertEqual(self.one(60_000.0, 60_100.0), 1)
        self.assertEqual(self.one(60_000.0, 59_900.0), -1)
        self.assertIsNone(self.one(60_000.0, 60_000.0))

    def test_stale_reference_never_turns_drift_into_a_signal(self):
        t = S.Trigger(0.5, 250)
        t.update(0.0, 60_000.0)
        self.assertIsNone(t.update(2500.0, 60_100.0)[0])                    # 2.5s gap: rule restarts
        self.assertIsNone(t.update(2600.0, 60_100.0)[0])

    def test_sniper_sides_and_the_frozen_feed(self):
        with env(SPEC):
            s = S.Settings()
        self.assertEqual((s.primary_feed, s.k_bp, s.window_ms), ("binance", 0.5, 250.0))


# =========================================================================== BOOK
class Book(unittest.TestCase):
    def setUp(self):
        self.b = BookState(stale_after=8.0)
        self.b.connected = True
        self.b.set_active(["A", "B"])
        self.now = int(time.time() * 1000)

    def snap(self, bids, asks, tok="A", ts=None):
        return self.b.apply_snapshot(tok, bids, asks, ts_ms=self.now if ts is None else ts, require_exchange_ts=True)

    def delta(self, *changes, ts=None):
        return self.b.apply_price_changes([{"asset_id": a, "price": p, "size": s, "side": sd}
                                           for a, p, s, sd in changes], ts_ms=ts or self.now + 1)

    def test_empty_one_sided_huge(self):
        self.assertTrue(self.snap([], []))
        v = self.b.view("A")
        self.assertEqual((v.status, v.best_ask, v.best_bid), ("LIVE", None, None))
        self.assertTrue(self.snap([("0.40", "5")], [], tok="B"))
        self.assertIsNone(self.b.view("B").best_ask)
        levels = [(f"{p / 1000:.3f}", "1") for p in range(501, 999)]
        self.assertTrue(self.snap([("0.50", "1")], levels))
        self.assertEqual(len(self.b.view("A").asks), len(levels))
        self.assertEqual(self.b.view("A").best_ask, 0.501)

    def test_invalid_snapshots_are_refused(self):
        for bids, asks in (([("0.55", "1")], [("0.50", "1")]),               # crossed
                           ([("0.50", "1")], [("0.50", "1")]),               # locked
                           ([], [("0.50", "-1")]), ([], [("0.50", "0")]),    # negative / zero size
                           ([], [("1.5", "1")]), ([], [("0", "1")]), ([], [("nan", "1")]),
                           ([], [("x", "1")])):
            b = BookState(); b.connected = True; b.set_active(["A"])
            self.assertFalse(b.apply_snapshot("A", bids, asks, ts_ms=self.now, require_exchange_ts=True),
                             (bids, asks))
            self.assertNotEqual(b.view("A").status, "LIVE")

    def test_duplicate_levels_aggregate(self):
        self.assertTrue(self.snap([], [("0.50", "3"), ("0.50", "4")]))
        self.assertEqual(self.b.view("A").asks, ((0.5, 7.0),))

    def test_old_snapshot_after_a_newer_delta_is_ignored(self):
        self.assertTrue(self.snap([("0.40", "5")], [("0.50", "5")]))
        self.assertTrue(self.delta(("A", "0.49", "2", "SELL"), ts=self.now + 50))
        self.assertFalse(self.snap([("0.40", "5")], [("0.55", "5")], ts=self.now + 10))
        v = self.b.view("A")
        self.assertEqual((v.status, v.best_ask), ("LIVE", 0.49))

    def test_D5_a_refused_delta_desyncs_instead_of_leaving_a_wrong_live_book(self):
        self.assertTrue(self.snap([("0.50", "100")], [("0.52", "50"), ("0.53", "50")]))
        self.assertFalse(self.delta(("A", "0.52", "30", "BUY")))           # crosses OUR copy: we missed a change
        self.assertEqual(self.b.view("A").status, "UNSYNCED")
        self.assertIn("A", self.b.needs_resync())
        self.assertTrue(self.snap([("0.52", "30")], [("0.53", "50")], ts=self.now + 5))   # the resync snapshot
        self.assertEqual((self.b.view("A").status, self.b.view("A").best_ask), ("LIVE", 0.53))
        for bad in (("A", "1.5", "1", "SELL"), ("A", "0.5", "-1", "SELL"), ("A", "nan", "1", "BUY"),
                    ("A", "0.5", "1", "SIDEWAYS")):
            b = BookState(); b.connected = True; b.set_active(["A"])
            self.assertTrue(b.apply_snapshot("A", [("0.4", "1")], [("0.6", "1")], ts_ms=self.now,
                                             require_exchange_ts=True))
            self.assertFalse(b.apply_price_changes([{"asset_id": bad[0], "price": bad[1], "size": bad[2],
                                                     "side": bad[3]}], ts_ms=self.now + 1))
            self.assertEqual(b.view("A").status, "UNSYNCED", bad)
        b = BookState(); b.connected = True; b.set_active(["A"])
        self.assertTrue(b.apply_snapshot("A", [("0.4", "1")], [("0.6", "1")], ts_ms=self.now, require_exchange_ts=True))
        self.assertFalse(b.apply_price_changes([{"asset_id": "A", "price": "0.5", "size": "1", "side": "BUY"}],
                                               ts_ms=self.now + 3_600_000))            # dated an hour ahead
        self.assertEqual(b.view("A").status, "UNSYNCED")

    def test_disconnect_rotation_and_stale(self):
        self.assertTrue(self.snap([("0.40", "5")], [("0.50", "5")]))
        self.b.desync_all("disconnect")
        self.assertEqual(self.b.view("A").status, "UNSYNCED")
        self.assertFalse(self.delta(("A", "0.49", "2", "SELL")) and self.b.view("A").status == "LIVE")
        self.b.set_active(["C"])
        self.assertEqual(self.b.view("A").status, "DISCONNECTED")         # old round's token is gone
        self.assertFalse(self.b.apply_snapshot("A", [], [("0.5", "1")], ts_ms=self.now))
        b = BookState(stale_after=0.05); b.connected = True; b.set_active(["A"])
        b.apply_snapshot("A", [], [("0.5", "1")], ts_ms=self.now, require_exchange_ts=True)
        time.sleep(0.08)
        self.assertEqual(b.view("A").status, "STALE")

    def test_rapid_replacement_and_random_streams_match_a_reference_model(self):
        rng = random.Random(3)
        for _case in range(400):
            b = BookState(); b.connected = True; b.set_active(["A"])
            model, synced, ts = {"BUY": {}, "SELL": {}}, False, self.now
            for _ in range(60):
                ts += rng.randrange(0, 3)
                r = rng.random()
                if r < 0.15:
                    bids = {round(rng.uniform(0.01, 0.48), 2): rng.randrange(1, 50) for _ in range(rng.randrange(0, 4))}
                    asks = {round(rng.uniform(0.52, 0.99), 2): rng.randrange(1, 50) for _ in range(rng.randrange(0, 4))}
                    ok = b.apply_snapshot("A", list(bids.items()), list(asks.items()), ts_ms=ts,
                                          require_exchange_ts=True)
                    if ok:
                        model, synced = {"BUY": dict(bids), "SELL": dict(asks)}, True
                    continue
                sd = rng.choice(("BUY", "SELL"))
                p = round(rng.uniform(0.01, 0.99), 2)
                sz = rng.choice((0, rng.randrange(1, 50)))
                ok = b.apply_price_changes([{"asset_id": "A", "price": str(p), "size": str(sz), "side": sd}],
                                           ts_ms=ts)
                if not synced:
                    continue
                trial = {k: dict(v) for k, v in model.items()}
                if sz == 0:
                    trial[sd].pop(p, None)
                else:
                    trial[sd][p] = sz
                crossed = trial["BUY"] and trial["SELL"] and max(trial["BUY"]) >= min(trial["SELL"])
                if crossed:
                    self.assertFalse(ok)
                    synced = False                                          # D5: fail closed
                else:
                    model = trial
                v = b.view("A")
                if synced:
                    self.assertEqual(v.status, "LIVE")
                    self.assertEqual(dict(v.asks), {k: float(x) for k, x in model["SELL"].items()})
                    self.assertEqual(dict(v.bids), {k: float(x) for k, x in model["BUY"].items()})
                else:
                    self.assertNotEqual(v.status, "LIVE")
                if v.status == "LIVE" and v.asks and v.bids:
                    self.assertLess(v.best_bid, v.best_ask)                  # a LIVE book is never crossed


# =========================================================================== EXECUTION (paper, real Sniper + PaperBroker)
class Execution(I.PaperIOCBase):
    def test_reference_ask_anchors_the_cap_and_never_chases(self):
        r = self.fire("UP", [(0.47, 4), (0.49, 4)], [(0.50, 30), (0.52, 30), (0.53, 30)])
        self.assertEqual((r["ask_seen"], r["cap"]), (0.47, 0.52))           # ref ask + $0.05, from the decision
        lots = self.broker.ledger.positions[I.UP].lots
        self.assertTrue(lots and all(l.price <= 0.52 + 1e-9 for l in lots))
        # what the IOC bought: the levels at or under the anchored cap, never 0.53
        audit = [json.loads(x) for x in pathlib.Path(self.broker.audit_path).read_text().splitlines() if x.strip()]
        levels = [lv for row in audit if row.get("levels") for lv in row["levels"]]
        self.assertTrue(levels and max(lv[0] for lv in levels) <= 0.52 + 1e-9, levels)

    def test_cap_never_exceeds_0_99_or_the_account_ceiling(self):
        r = self.fire("UP", [(0.93, 100)], [(0.93, 100)])
        self.assertLessEqual(r["cap"], 0.99)
        self.assertEqual(r["result"], "filled")
        self.assertLessEqual(r["price"], 0.95 + 1e-9)                      # max_buy_price of this broker
        r = self.fire("DOWN", [(0.97, 100)], [(0.97, 100)])
        self.assertEqual(r["result"], "skipped")                            # ask outside the account band

    def test_fak_is_the_order_type_on_the_sniper_path(self):
        seen = []
        orig = self.broker.place_trade

        def spy(*a, **kw):
            seen.append(kw.get("fak"))
            return orig(*a, **kw)
        self.broker.place_trade = spy
        self.fire("UP", [(0.47, 100)])
        self.assertEqual(seen, [True])
        self.assertEqual(self.sn.s.order_type, "FAK")

    def test_full_partial_zero_and_carry(self):
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r["shares"], self.sn.carry["UP"]), (10.0, 0.0))
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.47, 100)], [(0.47, 6)])                     # 6 of 10
        self.assertAlmostEqual(r["shares"], 6.0, places=6)
        self.assertAlmostEqual(self.sn.carry["UP"], 4.0, places=6)
        self.assertEqual(self.sn.carry["DOWN"], 0.0)                        # never crosses sides
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.47, 100)], [(0.60, 100)])                   # nothing under the cap
        self.assertEqual(r["result"], "refused")
        self.assertEqual(r["target_shares"], 14.0)                          # min(10 + 4, 20)
        self.assertAlmostEqual(self.sn.carry["UP"], 14.0, places=6)
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.47, 100)], [(0.60, 100)])
        self.assertEqual(r["target_shares"], 20.0)                          # capped at 20
        self.assertEqual(self.sn.carry["DOWN"], 0.0)

    def test_FINDING_a_dollar_ioc_buys_more_than_its_target_when_the_ask_falls(self):
        """Documented, NOT changed (it is the execution model): the IOC is signed as a
        dollar amount (makerAmount in USDC, as live), sized for the share target at the
        decision's levels. If the ask falls before the match, the stake buys MORE shares
        than the target - and share limits (round side cap, canary shares/order) are
        checked on the decision's estimate. The next order sees the real count."""
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_MAX_SIDE_SHARES="20"))
        self.sn._write = self.rows.append
        r = self.fire("UP", [(0.50, 100)], [(0.30, 100)])
        self.assertEqual((r["target_shares"], r["amount"]), (10.0, 5.0))
        self.assertAlmostEqual(r["shares"], 5.0 / 0.30, places=4)                     # 16.67 > target 10
        self.assertEqual(self.sn.carry["UP"], 0.0)                                     # never a negative carry
        self.sn.last_fire_by_side["UP"] = -1e18
        r = self.fire("UP", [(0.50, 100)], [(0.30, 100)])
        self.assertEqual((r["result"], r["reason"]), ("skipped", "round cap"))        # 16.67 + 10 > 20

    def test_D6_an_ioc_meeting_an_empty_book_is_a_zero_fill(self):
        r = self.fire("UP", [(0.47, 100)], [])
        self.assertEqual(r["result"], "refused")
        self.assertIn("FAK no-fill", r["reason"])
        self.assertEqual(self.sn.carry["UP"], 10.0)                         # request 10 / fill 0 -> carry 10

    def test_carry_resets_on_market_change(self):
        self.fire("UP", [(0.47, 100)], [(0.47, 4)])
        self.assertAlmostEqual(self.sn.carry["UP"], 6.0, places=6)
        self.m.condition_id, self.m.window_end = "cond-2", self.m.window_end + 300
        self.m.up_token, self.m.down_token = "up2", "down2"
        self.assertEqual(self.sn._carry_for("cond-2", self.m.window_end), {"UP": 0.0, "DOWN": 0.0})

    def test_accounting_counts_actual_fills_never_the_request(self):
        self.fire("UP", [(0.47, 100)], [(0.47, 3.5)])
        pos = self.broker.ledger.positions[I.UP]
        self.assertAlmostEqual(pos.shares, 3.5, places=6)
        self.assertAlmostEqual(pos.cost, 3.5 * 0.47, places=6)              # fee rate 0 in this harness
        self.assertAlmostEqual(self.broker.cash_balance(), 1000 - 3.5 * 0.47, places=6)


class Precision(unittest.TestCase):
    def amounts(self, stake, cap, tick):
        from py_clob_client_v2.order_builder.builder import ROUNDING_CONFIG, OrderBuilder
        from py_clob_client_v2.order_builder.constants import BUY
        _side, maker, taker = OrderBuilder.get_market_order_amounts(
            None, BUY, S.signing_amount(stake), S.signing_price(cap, tick), ROUNDING_CONFIG[tick])
        return D(maker) / 1_000_000, D(taker) / 1_000_000

    def test_20000_signed_orders_never_exceed_cap_stake_or_cash(self):
        rng, bad, excess = random.Random(17), [], []
        for _ in range(20_000):
            tick = rng.choice(("0.01", "0.001"))
            ask = D(rng.randrange(1, 95)) / 100 if tick == "0.01" else D(rng.randrange(10, 950)) / 1000
            cap = min(D("0.99"), ask + D("0.05"))
            carry = D(rng.randrange(0, 1001)) / 100                          # fractional carry 0..10
            target = min(D(10) + carry, D(20))
            levels = [(float(ask), float(target))]                           # the whole target at the ask
            stake = S.Sniper.ioc_spend(levels, float(target), float(ask), float(cap))
            maker, taker = self.amounts(stake, float(cap), tick)
            exact_stake = (D(str(stake))).quantize(D("0.01"), rounding=ROUND_HALF_UP)
            signed_price = maker / taker
            tick_d = D(tick)
            cap_on_tick = (cap / tick_d).to_integral_value(rounding=ROUND_FLOOR) * tick_d
            if maker != exact_stake:
                bad.append(("stake", tick, ask, target, stake, maker))
            # The SDK rounds the share amount DOWN, so maker/taker sits a hair above the
            # limit (measured < 1e-5). Fills happen on the tick grid: what matters is
            # that no tick above the cap is reachable.
            excess.append(signed_price - cap_on_tick)
            if signed_price >= cap_on_tick + tick_d:
                bad.append(("reaches the tick above the cap", tick, ask, cap, signed_price))
            if maker / ask < target - D("1e-9"):
                bad.append(("short of target at the ask", tick, ask, target, maker))
            if maker > exact_stake + D("0.005"):
                bad.append(("over the stake", tick, ask, maker))
        self.assertEqual(bad[:5], [])
        self.assertLess(max(excess), D("0.00001"))                           # far below a 0.001 tick
        print(f"  [signed price above the tick-floored cap: max {max(excess):.2e}; one tick >= 1e-3]")

    def test_signing_price_never_rounds_above_the_cap(self):
        for cents in range(1, 100):
            for tick in ("0.01", "0.001"):
                cap = cents / 100 + 0.0049
                p = D(repr(S.signing_price(cap, tick)))
                self.assertLessEqual(p, D(repr(cap)) + D("1e-9"))
                self.assertLess(p - D("1e-9"), D(repr(cap)) + D("1e-12"))


class CooldownAndConcurrency(unittest.TestCase):
    """The real _signal gates with the order path stubbed (it records)."""

    def setUp(self):
        with env(SPEC):
            self.sn = S.Sniper(types.SimpleNamespace(), types.SimpleNamespace(), settings=S.Settings())
        self.sent = []
        self.now = [1_000_000.0]                                            # monotonic ms
        self.sn.fresh.why_not = lambda t: None

        def fire(side, r, t, t_pc=None, feed="binance"):
            self.sent.append((side, t))
            if not self.hold:
                self.sn._release(side)                                       # answered at once
            return object()                                                  # "submitted"
        self.hold = False
        self.sn._fire_now = fire
        self.trig = {1: types.SimpleNamespace(update=lambda t, m: (1, 0.6), q=collections.deque(), ref=None),
                     -1: types.SimpleNamespace(update=lambda t, m: (-1, -0.6), q=collections.deque(), ref=None)}

    def signal(self, side, at_ms):
        with patch.object(S.time, "monotonic", lambda: at_ms / 1000.0):
            self.sn._signal("binance", self.trig[1 if side == "UP" else -1], 60_000.0, at_ms, 0)

    def test_300ms_per_side_boundary(self):
        self.signal("UP", 0.0)
        self.signal("UP", 299.0)                                             # blocked
        self.signal("UP", 299.999)                                           # blocked
        self.signal("UP", 300.0)                                             # allowed (>= 300ms since submit)
        self.assertEqual([s for s, _ in self.sent], ["UP", "UP"])
        self.assertEqual(self.sn.n["skip: cooldown"], 2)

    def test_down_at_150ms_is_not_blocked_by_up(self):
        self.hold = True                                                     # orders stay outstanding
        self.signal("UP", 0.0)
        self.assertIsNotNone(self.sn.inflight["UP"])                         # UP still outstanding
        self.signal("DOWN", 150.0)
        self.assertEqual([s for s, _ in self.sent], ["UP", "DOWN"])
        self.signal("UP", 400.0)                                             # UP's own order in flight
        self.assertEqual(self.sn.n["skip: previous order in flight"], 1)

    def test_cooldown_and_trigger_ignore_wall_clock_jumps(self):
        self.signal("UP", 0.0)
        with patch.object(S.time, "time", lambda: 10.0 ** 10):               # wall clock jumps far ahead
            self.signal("UP", 100.0)
        with patch.object(S.time, "time", lambda: 1.0):                      # ... and far back
            self.signal("UP", 200.0)
        self.assertEqual(len(self.sent), 1)                                  # cooldown on the monotonic clock


# =========================================================================== ACCOUNTING (Fraction oracle)
class Accounting(unittest.TestCase):
    def test_random_fills_settlements_and_duplicates_match_the_oracle(self):
        rng = random.Random(23)
        for case in range(300):
            with tempfile.TemporaryDirectory() as tmp:
                led = Ledger(path=os.path.join(tmp, "l.json"))
                o_pos = collections.defaultdict(lambda: {"sh": F(0), "cost": F(0), "fees": F(0)})
                o_seen, o_realized, o_payout = set(), F(0), F(0)
                markets = {f"0x{m:064x}": (f"{m}1", f"{m}2") for m in range(1, 4)}
                result, o_settled = {}, {}                       # market -> winner; token -> payout
                tids = []
                for step in range(rng.randrange(5, 40)):
                    cid = rng.choice(list(markets))
                    if cid in result and rng.random() < 0.7:
                        continue
                    tok = rng.choice(markets[cid])
                    if tids and rng.random() < 0.15:
                        tid = rng.choice(tids)                               # a duplicate message
                    else:
                        tid = f"t{case}-{step}"
                        tids.append(tid)
                    sh = rng.choice((D("0.01"), D("0.10"), D("0.99"), D("1.01"), D("4.25"), D("10.37"),
                                     D(rng.randrange(1, 2000)) / 100))
                    px = D(rng.randrange(1, 99)) / 100
                    fee = (sh * D("0.07") * px * (1 - px)).quantize(D("0.00001"), rounding=ROUND_HALF_UP)
                    ok = led.record_fill(tid, tok, shares=float(sh), price=float(px), condition_id=cid,
                                         fee=float(fee))
                    want = tid not in o_seen
                    self.assertEqual(ok, want)
                    if want:
                        o_seen.add(tid)
                        p = o_pos[tok]
                        p["sh"] += F(str(sh)); p["fees"] += F(str(fee))
                        p["cost"] += F(str(sh)) * F(str(px)) + F(str(fee))
                        o_settled.pop(tok, None)                             # a late fill reopens ITS position
                    if rng.random() < 0.25:
                        win = result.setdefault(cid, rng.choice((0, 1)))     # a market's result never changes
                        payouts = {markets[cid][0]: 1.0 - win, markets[cid][1]: float(win)}
                        led.settle(Resolution(cid, RESOLVED, payouts, "Up" if win == 0 else "Down"))
                        led.settle(Resolution(cid, RESOLVED, payouts, "Up" if win == 0 else "Down"))  # twice
                        for t in markets[cid]:
                            if t in o_pos and o_pos[t]["sh"] > 0:
                                o_settled[t] = F(str(payouts[t]))
                # the oracle's realized: every settled position, recomputed from scratch
                for tok, pay in o_settled.items():
                    p = o_pos[tok]
                    o_realized += p["sh"] * pay - p["cost"]
                    o_payout += p["sh"] * pay
                s = led.summary()
                self.assertAlmostEqual(s["realized_pnl"], float(o_realized), places=9, msg=case)
                for tok, p in o_pos.items():
                    pos = led.positions[tok]
                    self.assertAlmostEqual(pos.shares, float(p["sh"]), places=9)
                    self.assertAlmostEqual(pos.cost, float(p["cost"]), places=9)
                    self.assertAlmostEqual(pos.fees, float(p["fees"]), places=9)
                    self.assertGreaterEqual(pos.shares, 0)
                    self.assertGreaterEqual(pos.cost, 0)
                self.assertEqual(len(led.seen), len(o_seen))

    def test_paper_cash_equity_and_pnl_identities(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            m = I.Market(I.current_round_end())
            br = PaperBroker(Ledger(path=str(tmp / "l.json")),
                             market_context=lambda: {"condition_id": m.condition_id, "up_token_id": m.up_token,
                                                     "down_token_id": m.down_token},
                             host="https://public.invalid", max_buy_price=0.95, min_buy_price=0.05,
                             start_balance=100, account_path=tmp / "a.json", audit_path=tmp / "o.jsonl",
                             book_fetch=m.fetch, rules_fetch=lambda cid: MarketRules(
                                 cid, D("0.07"), 1, min_order_size=D("5"), tick_size=D("0.01"),
                                 up_token_id=m.up_token, down_token_id=m.down_token),
                             min_seconds_to_expiry=0, trade_window_seconds=300, own_fill_ttl_s=0)
            m.set(m.up_token, [(0.40, 100)]); m.set(m.down_token, [(0.55, 100)])
            self.assertTrue(br.place_trade("UP", 4.0, m.up_token, m.down_token, m.condition_id, m.window_end,
                                           0.45, None, latency_ms=0, tag="sniper", fak=True))
            self.assertTrue(br.place_trade("DOWN", 5.5, m.up_token, m.down_token, m.condition_id, m.window_end,
                                           0.60, None, latency_ms=0, tag="sniper", fak=True))
            up, dn = br.ledger.positions[m.up_token], br.ledger.positions[m.down_token]
            fee = lambda sh, p: F(str(sh)) * F("0.07") * F(str(p)) * (1 - F(str(p)))
            o_up_cost = F(10) * F("0.40") + fee(10, 0.40)
            o_dn_cost = F(10) * F("0.55") + fee(10, 0.55)
            self.assertAlmostEqual(up.cost, float(o_up_cost), places=4)
            self.assertAlmostEqual(dn.cost, float(o_dn_cost), places=4)
            cash = F(100) - o_up_cost - o_dn_cost
            self.assertAlmostEqual(br.cash_balance(), float(cash), places=4)
            # mark: bids 0.39 / 0.54 -> unrealized = value - cost; equity = cash + cost + unrealized = cash + value
            marks = {m.up_token: 0.39, m.down_token: 0.54}
            s = br.summary(mark=lambda t: marks.get(t))
            value = F(10) * F("0.39") + F(10) * F("0.54")
            self.assertAlmostEqual(s["unrealized_mark_to_bid"], float(value - o_up_cost - o_dn_cost), places=4)
            self.assertAlmostEqual(s["equity"], float(cash + value), places=4)
            self.assertAlmostEqual(s["total_pnl"], float(cash + value - 100), places=4)
            # UP wins: realized once, cash credited once, unrealized gone
            res = Resolution(m.condition_id, RESOLVED, {m.up_token: 1.0, m.down_token: 0.0}, "Up")
            br.ledger.settle(res); br.ledger.settle(res)
            s = br.summary(mark=lambda t: marks.get(t))
            realized = F(10) - o_up_cost - o_dn_cost
            self.assertAlmostEqual(s["realized_pnl"], float(realized), places=4)
            self.assertAlmostEqual(br.cash_balance(), float(F(100) + realized), places=4)
            self.assertAlmostEqual(s["equity"], float(F(100) + realized), places=4)
            self.assertEqual(s["open_positions"], 0)

    def test_fill_feed_integrity_duplicates_out_of_order_conflicts(self):
        from feeds.poly_user import FillStore
        fs = FillStore()
        self.assertTrue(fs.record_trade("T1", order_id="O1", asset_id="11", market="0x" + "1" * 64, side="BUY",
                                        price="0.5", size="10", status="MATCHED"))
        self.assertFalse(fs.record_trade("T1", status="CONFIRMED", source="rest"))   # forward: confirmed
        self.assertEqual(fs.fills["T1"].status, "CONFIRMED")
        self.assertFalse(fs.record_trade("T1", status="MATCHED"))                    # a replay never undoes it
        self.assertEqual(fs.fills["T1"].status, "CONFIRMED")
        self.assertFalse(fs.record_trade("T1", status="FAILED"))                     # contradictory terminal
        self.assertEqual(fs.fills["T1"].status, "CONFLICT")
        self.assertFalse(fs.fills["T1"].counts)
        fs.record_trade("T2", order_id="O1", asset_id="11", market="0x" + "1" * 64, side="BUY", price="0.5",
                        size="10", status="CONFIRMED")
        self.assertFalse(fs.record_trade("T2", size="11", status="CONFIRMED"))       # immutable field changed
        self.assertEqual(fs.fills["T2"].status, "CONFLICT")


# =========================================================================== MANAGER
FEE = PM.fee_curve(0.07, 1)
CFG = PM.PMConfig(enabled=True)


def pside(shares=0.0, avg=None, asks=(), pending=0.0, pending_usd=0.0):
    cost = shares * avg if avg is not None else 0.0
    best = asks[0][0] if asks else None
    return PM.SideState(shares=shares, cost=cost, fees=0.0, pending_shares=pending, pending_usd=pending_usd,
                        best_bid=round(best - 0.01, 2) if best else None, best_ask=best, asks=tuple(asks))


def pm_eval(side, port, cfg=CFG, left=200.0, hist=None, busy=False, cash=1000.0, reserve=None):
    return PM.Engine(cfg).evaluate(side, port, fee=FEE, left_s=left, book_ok=True, active=left > 0,
                                   hist=hist or PM.MarketHistory(), now_mono=1e6, sniper_busy=busy,
                                   cash_free=cash, sniper_reserve=reserve)


def oracle_walk(asks, want, boundary, rate=F("0.07")):
    qty, notional, fee = F(0), F(0), F(0)
    need = F(str(want))
    for p, q in asks:
        p, q = F(str(p)), F(str(q))
        if p > F(str(boundary)) or need <= 0:
            break
        take = min(need, q)
        qty += take; notional += take * p; fee += take * rate * p * (1 - p)
        need -= take
    return qty, notional, fee


LOOSE = replace(CFG, side_avg_max_added_loss_usd=1000.0, side_avg_min_gain_per_usd=0.0, max_order_usd=1000.0,
                max_market_usd=1000.0, max_total_exposure_usd=1000.0, max_up_shares=1000.0, max_down_shares=1000.0,
                pair_max_added_loss_usd=1000.0, rearm_price_step=0.0)


class Manager(unittest.TestCase):
    def test_original_bad_pair_regression(self):
        p = PM.Portfolio(pside(20, 0.59, asks=((0.60, 100),)), pside(10, 0.85, asks=((0.73, 100),)))
        d = pm_eval("DOWN", p, LOOSE)
        self.assertNotEqual(d.action, "BUY")
        self.assertEqual(d.reason, "PAIR_ECONOMICS_BAD")

    def test_vwap_walks_the_book_never_the_best_price_for_all(self):
        asks = ((0.40, 2), (0.42, 3), (0.45, 10))
        f = PM.expected_fill(asks, 10, 0.50, FEE)
        qty, notional, fee = oracle_walk(asks, 10, 0.50)
        self.assertAlmostEqual(f.qty, float(qty))
        self.assertAlmostEqual(f.vwap, float(notional / qty))                # (0.8 + 1.26 + 2.25) / 10 = 0.431
        self.assertAlmostEqual(f.vwap, 0.431)
        self.assertAlmostEqual(f.fees, float(fee))
        f = PM.expected_fill(asks, 10, 0.42, FEE)                            # boundary below the 0.45 level
        self.assertAlmostEqual(f.qty, 5.0)                                   # insufficient depth inside it
        f = PM.expected_fill(((0.40, 0.25), (0.41, 0.5)), 10, 0.50, FEE)     # fractional levels
        self.assertAlmostEqual(f.qty, 0.75)
        self.assertEqual(PM.expected_fill((), 10, 0.5, FEE).qty, 0.0)         # disappearing liquidity

    def test_lower_average_with_more_risk_is_rejected(self):
        p = PM.Portfolio(pside(0, None, asks=((0.79, 100),)), pside(20, 0.60, asks=((0.20, 100),)))
        d = pm_eval("DOWN", p, replace(LOOSE, side_avg_max_added_loss_usd=1.0))
        self.assertNotEqual(d.action, "BUY")
        self.assertEqual(d.reason, "AVG_IMPROVES_BUT_RISK_REJECTED")
        self.assertEqual(d.classification, "AVG_IMPROVES_BUT_RISK_REJECTED")

    def test_pair_economics_on_every_buy_that_adds_pairs_fuzz_oracle(self):
        """10,000 random portfolios and books: every BUY whose fill adds pairs
        (any amount, even 0.0001) has an incremental pair cost - recomputed
        here from the book walk - at or under the configured threshold, and no
        BUY breaks a configured limit."""
        rng, bad, buys, pair_buys = random.Random(31), [], 0, 0
        for i in range(10_000):
            cfg = replace(LOOSE, min_pair_improvement=rng.choice((0.02, 0.05)),
                          max_order_usd=rng.choice((10.0, 1000.0)), max_up_shares=rng.choice((60.0, 1000.0)),
                          max_down_shares=rng.choice((60.0, 1000.0)), min_new_pairs=rng.choice((1.0, 0.0001)))
            up_sh = rng.choice((0, rng.uniform(0, 40)))
            dn_sh = rng.choice((0, rng.uniform(0, 40)))
            mk = lambda: tuple(sorted({round(rng.uniform(0.02, 0.98), 2): round(rng.uniform(0.0001, 30), 4)
                                       for _ in range(rng.randrange(0, 5))}.items()))
            p = PM.Portfolio(pside(up_sh, rng.uniform(0.05, 0.95) if up_sh else None, asks=mk(),
                                   pending=rng.choice((0, 0, rng.uniform(0, 5)))),
                             pside(dn_sh, rng.uniform(0.05, 0.95) if dn_sh else None, asks=mk()))
            p.up.pending_usd = p.up.pending_shares * 0.5
            side = rng.choice(("UP", "DOWN"))
            left = rng.choice((200.0, 90.0, 45.0))
            d = pm_eval(side, p, cfg, left=left)
            if d.action != "BUY":
                continue
            buys += 1
            s, o = p.side(side), p.side("DOWN" if side == "UP" else "UP")
            qty, notional, fee = oracle_walk(s.asks, d.size, d.limit)
            other_avg = (F(o.cost) + F(o.pending_usd)) / F(o.total_shares) if o.total_shares > 0 else None
            new_pairs = min(F(p.up.total_shares) + (qty if side == "UP" else 0),
                            F(p.down.total_shares) + (qty if side == "DOWN" else 0)) - F(p.paired)
            mult = PM.Engine(cfg).multiplier(left)
            if new_pairs > 0 and other_avg is not None:
                pair_buys += 1
                inc = (notional + fee) / qty + other_avg
                if inc > F(1) - F(str(cfg.min_pair_improvement)) * F(str(mult)) + F("1e-9"):
                    bad.append(("pair threshold", i, float(inc), float(new_pairs)))
            if s.total_shares + float(qty) > (cfg.max_up_shares if side == "UP" else cfg.max_down_shares) + 1e-6:
                bad.append(("side cap", i))
            if d.metrics["capital_max"] > cfg.max_order_usd + 1e-9:
                bad.append(("order cap", i))
            if float(qty) + 1e-6 < d.size:
                bad.append(("FOK size not fillable", i))
        self.assertEqual(bad[:5], [])
        self.assertGreater(buys, 50)
        self.assertGreater(pair_buys, 10)

    def test_fractional_new_pairs_are_always_validated(self):
        for extra in (0.0, 0.0001, 0.5, 1.0, 10.0):
            # DOWN short of UP by `extra`: buying DOWN adds exactly min(extra, qty) pairs
            p = PM.Portfolio(pside(20 + extra, 0.70, asks=((0.71, 100),)), pside(20, 0.60, asks=((0.50, 100),)))
            d = pm_eval("DOWN", p, replace(LOOSE, min_new_pairs=1.0))
            if extra > 0:
                self.assertTrue(d.metrics.get("new_pairs", 0) > 0, extra)
                # 0.50 + fees + 0.70 > 0.98: pair economics must refuse, whatever the classification
                self.assertEqual((d.action, d.reason), ("BLOCKED", "PAIR_ECONOMICS_BAD"), extra)

    def test_pair_threshold_minus_exact_plus_epsilon(self):
        # zero fees: inc = ask + other avg; threshold 0.98
        zero = PM.fee_curve(0.0, 1)
        for ask, want in ((0.4799, "BUY"), (0.48, "BUY"), (0.4801, "BLOCKED")):
            p = PM.Portfolio(pside(20, 0.50, asks=((0.60, 100),)), pside(0, None, asks=((ask, 100),)))
            d = PM.Engine(replace(LOOSE, min_pair_improvement=0.02)).evaluate(
                "DOWN", p, fee=zero, left_s=200, book_ok=True, active=True, hist=PM.MarketHistory(),
                now_mono=1e6, sniper_busy=False, cash_free=1000)
            self.assertEqual(d.action, want, (ask, d.reason, d.metrics.get("incremental_pair_cost")))

    def test_anti_martingale_ladder(self):
        led_hist, orders = PM.MarketHistory(), []
        cfg = replace(CFG, cooldown_ms=0.0)
        shares, cost = 20.0, 12.0                                            # 20 DOWN @ 0.60
        for i, ask in enumerate((.60, .55, .50, .45, .40, .35, .30, .25, .20, .15, .10, .05)):
            p = PM.Portfolio(pside(0, None, asks=((0.95, 100),)), pside(shares, cost / shares, asks=((ask, 100),)))
            d = pm_eval("DOWN", p, cfg, hist=led_hist)
            if d.action == "BUY":
                orders.append((ask, d.size))
                h = led_hist.sides["DOWN"]
                led_hist.orders += 1; h.orders += 1; led_hist.spend += d.metrics["capital_max"]
                h.last_buy_price = ask
                shares += d.fill.qty; cost += d.fill.cost
        self.assertLessEqual(len(orders), cfg.max_orders_per_side)
        self.assertLessEqual(led_hist.spend, cfg.max_market_usd + 1e-9)
        prices = [a for a, _ in orders]
        self.assertTrue(all(b <= a - cfg.rearm_price_step + 1e-9 for a, b in zip(prices, prices[1:])), prices)
        self.assertLess(len(orders), 12)                                     # not a buy at every decline

    def test_sniper_liquidity_reserve_levels(self):
        # UP held, buying DOWN pairs it; the book's depth inside the sniper's cap decides
        for depth, want in ((0, False), (5, False), (10, False), (19.99, False), (20, False), (20.01, False),
                            (25, True), (30, True), (40, True), (100, True)):
            asks = ((0.40, depth),) if depth else ()
            p = PM.Portfolio(pside(5, 0.50, asks=((0.60, 100),)), pside(0, None, asks=asks + ((0.99, 1000),)))
            d = pm_eval("DOWN", p, replace(LOOSE, max_price_over_best=0.0), reserve=(20.0, 0.05))
            if want:
                self.assertEqual(d.action, "BUY", (depth, d.reason))
                self.assertGreaterEqual(depth - d.fill.qty, 20.0 - 1e-9)
            else:
                self.assertNotEqual(d.action, "BUY", depth)
                if depth >= 5:
                    self.assertEqual(d.reason, "SNIPER_LIQUIDITY_RESERVE", depth)

    def test_sniper_priority_blocks_the_manager(self):
        p = PM.Portfolio(pside(10, 0.40, asks=((0.45, 100),)), pside(0, None, asks=((0.50, 100),)))
        free = pm_eval("DOWN", p, LOOSE)
        self.assertEqual(free.action, "BUY")
        busy = pm_eval("DOWN", p, LOOSE, busy=True)
        self.assertEqual((busy.action, busy.reason), ("BLOCKED", "SNIPER_PRIORITY"))


class ManagerRuntime(unittest.TestCase):
    """The PositionManager object: reservations, timeouts, outcomes, isolation."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.led = Ledger(path=os.path.join(self.tmp.name, "l.json"))
        from tests_sniper_live import live_book
        self.hub = types.SimpleNamespace(up_token="111", down_token="222", condition_id="0x" + "ab" * 32,
                                         window_end=time.time() + 200, book=live_book())
        rules = MarketRules(self.hub.condition_id, D("0.07"), 1, D("5"), D("0.01"))
        self.calls = []
        self.broker = types.SimpleNamespace(_rules_cache={self.hub.condition_id: (rules, time.time())},
                                            place_trade=lambda *a, **k: self.calls.append((a, k)) or False)

    def tearDown(self):
        self.tmp.cleanup()

    def mk(self, cfg=CFG, live=False):
        pm = PM.PositionManager(cfg, hub=self.hub, ledger=self.led, broker=self.broker, live_mode=live)
        self.addCleanup(pm.journal.close)
        return pm

    def order(self, pm, side="UP", qty=10.0, usd=4.0):
        d = PM.Decision(side, action="BUY", classification="SIDE_AVG_IMPROVEMENT", size=qty, limit=0.45,
                        stake=usd, fill=PM.Fill(qty=qty, notional=usd, vwap=usd / qty))
        d.metrics["capital_max"] = usd
        pm.hist = pm._hist_for(self.hub.condition_id)
        return pm.reserve(d)

    def test_duplicate_outcome_applies_once(self):
        pm = self.mk()
        o = self.order(pm)
        fill = {"order_id": "m1", "shares": 10.0, "average_price": 0.40, "fee": 0.1, "total_cost": 4.1}
        for _ in range(3):
            pm.settle_order(o, ok=True, fill=fill, error=None)
        h = pm.hist
        self.assertEqual((o.status, o.filled_qty, h.orders), ("FILLED", 10.0, 1))
        self.assertAlmostEqual(h.spend, 4.1)                                 # actual cost, once
        self.assertFalse(h.sides["UP"].in_flight)
        self.assertEqual(pm._open_orders(), [])                              # reservation released once

    def test_timeout_unknown_keeps_the_reservation_until_the_late_outcome(self):
        for late in ({"ok": True, "fill": {"order_id": "m", "shares": 10.0, "average_price": 0.4, "fee": 0,
                                            "total_cost": 4.0}},
                     {"ok": True, "fill": {"order_id": "m", "shares": 6.0, "average_price": 0.4, "fee": 0,
                                            "total_cost": 2.4, "partial": True}},
                     {"ok": False, "fill": None, "error": "FOK no-fill"},
                     {"ok": False, "fill": None, "error": "rejected"},
                     {"ok": False, "fill": None, "error": "cancelled before send: SNIPER_PRIORITY"}):
            pm = self.mk()
            o = self.order(pm)
            pm._timed_out(o)
            self.assertEqual(o.status, "UNKNOWN")
            self.assertEqual(len(pm._open_orders()), 1)                      # still counted: no blind retry
            self.assertTrue(pm.hist.sides["UP"].in_flight)
            pm._timed_out(o)                                                 # a second timer: no change
            pm.settle_order(o, ok=late["ok"], fill=late.get("fill"), error=late.get("error"))
            self.assertNotEqual(o.status, "UNKNOWN")
            self.assertEqual(pm._open_orders(), [])
            self.assertFalse(pm.hist.sides["UP"].in_flight)
            self.assertGreaterEqual(pm.hist.spend, -1e-9)                    # never negative

    def test_outcome_lands_in_its_own_market_after_a_switch(self):
        pm = self.mk()
        a = self.order(pm)
        hist_a = pm.hist
        self.hub.condition_id, self.hub.up_token, self.hub.down_token = "0x" + "cd" * 32, "333", "444"
        pm.hist = pm._hist_for(self.hub.condition_id)                        # market B is active now
        pm.settle_order(a, ok=True, fill={"order_id": "x", "shares": 10.0, "average_price": 0.4, "fee": 0,
                                          "total_cost": 4.0}, error=None)
        self.assertEqual(pm.hist.orders, 0)                                  # B untouched
        self.assertEqual(pm.hist.spend, 0.0)
        self.assertAlmostEqual(hist_a.spend, 4.0)
        self.assertEqual(a.market_id.split("@")[0], "0x" + "ab" * 32)

    def test_stale_snapshot_is_never_sent(self):
        pm = self.mk()
        pm._snap_version = ("old",)
        d = PM.Decision("UP", action="BUY", classification="SIDE_AVG_IMPROVEMENT", size=10, limit=0.45, stake=4.0,
                        fill=PM.Fill(qty=10, notional=4.0, vwap=0.4))

        async def go():
            return pm._dispatch(d, asyncio.get_running_loop())
        self.assertIsNone(asyncio.run(go()))
        self.assertEqual(d.reason, "STATE_CHANGED")
        self.assertEqual(self.calls, [])

    def test_live_mode_is_shadow_only_and_sends_nothing(self):
        pm = self.mk(replace(CFG, live_enabled=True), live=True)
        self.assertTrue(pm.shadow)
        d = PM.Decision("UP", action="BUY", classification="SIDE_AVG_IMPROVEMENT", size=10, limit=0.45, stake=4.0,
                        fill=PM.Fill(qty=10, notional=4.0, vwap=0.4))
        with self.assertRaises(RuntimeError):
            pm._execute_paper(d, self.hub.condition_id, "111", "222", self.hub.window_end)
        self.assertEqual(self.calls, [])                                     # REAL MANAGER ORDERS SENT = 0
        src = (ROOT / "position_manager.py").read_text(encoding="utf-8")
        self.assertEqual(src.count("place_trade("), 1)                       # the one paper call, guarded above
        for name in ("post_order", "create_order", "LeanSender", "sniper_live", "polymarket_trade"):
            self.assertNotIn(name, src)

    def test_shadow_state_never_touches_the_ledger(self):
        pm = self.mk(replace(CFG, live_enabled=True), live=True)
        before = json.dumps({k: (v.shares, v.cost) for k, v in self.led.positions.items()})
        pm.n["shadow buys"] += 1
        self.assertEqual(json.dumps({k: (v.shares, v.cost) for k, v in self.led.positions.items()}), before)
        self.assertEqual(len(self.led.seen), 0)


class ManagerPaperDepth(unittest.TestCase):
    def test_D4_sniper_and_manager_never_buy_the_same_depth_twice(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            m = I.Market(I.current_round_end())
            m.set(m.up_token, [(0.47, 10)], [(0.47, 10)])
            fetch = m.fetch
            br = PaperBroker(Ledger(path=str(tmp / "l.json")),
                             market_context=lambda: {"condition_id": m.condition_id, "up_token_id": m.up_token,
                                                     "down_token_id": m.down_token},
                             host="https://public.invalid", max_buy_price=0.95, min_buy_price=0.05,
                             start_balance=1000, account_path=tmp / "a.json", audit_path=tmp / "o.jsonl",
                             book_fetch=lambda t: fetch(t) if m.reads[t] == 0 else m.fetch(t),
                             rules_fetch=lambda cid: MarketRules(cid, D("0"), 1, min_order_size=D("5"),
                                                                 tick_size=D("0.01"), up_token_id=m.up_token,
                                                                 down_token_id=m.down_token),
                             min_seconds_to_expiry=0, trade_window_seconds=300, own_fill_ttl_s=2.0)
            m.match[m.up_token] = [(0.47, 10)]
            m.reads = collections.Counter()
            res = {}

            def slow_guard():
                time.sleep(0.4)
                return True

            def manager():
                res["m"] = br.place_trade("UP", 4.70, m.up_token, m.down_token, m.condition_id, m.window_end,
                                          0.49, None, latency_ms=0, tag="manager", fak=False,
                                          pre_submit_guard=slow_guard)

            def sniper():
                res["s"] = br.place_trade("UP", 4.70, m.up_token, m.down_token, m.condition_id, m.window_end,
                                          0.52, None, latency_ms=200, tag="sniper", fak=True)
            ts = threading.Thread(target=sniper); ts.start(); time.sleep(0.05)
            tm = threading.Thread(target=manager); tm.start()
            tm.join(10); ts.join(10)
            held = br.ledger.positions[m.up_token].shares
            self.assertLessEqual(held, 10.0 + 1e-9, res)                     # was 20 before the fix
            self.assertEqual(sorted(res.values()), [False, True])


# =========================================================================== SETTLEMENT
class FakeResolver:
    def __init__(self):
        self.answers, self.calls, self.block = {}, collections.Counter(), {}

    def __call__(self, cid):
        self.calls[cid] += 1
        ev = self.block.get(cid)
        if ev is not None:
            ev.wait(5)
        r = self.answers.get(cid)
        if isinstance(r, Exception):
            raise r
        return r or Resolution(cid, PENDING, {}, detail="not resolved on chain yet")


class Settlement(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.led = Ledger(path=os.path.join(self.tmp.name, "l.json"))
        self.clock = [1_000_000.0]
        self.fetch = FakeResolver()
        self.events = []
        self.w = ST.SettlementWorker(self.led, interval=0.05, fetch=self.fetch, clock=lambda: self.clock[0],
                                     poll_active=False, tick_s=0.01, paper=True,
                                     on_event=lambda t, m, lv: self.events.append(m))

    def tearDown(self):
        self.tmp.cleanup()

    def market(self, n, end):
        cid = "0x" + f"{n:064x}"
        self.w.track(cid, end - 300, end, up_token=f"{n}1", down_token=f"{n}2")
        return cid, f"{n}1", f"{n}2"

    async def ticks(self, n=40):
        for _ in range(n):
            await self.w.tick()
            await asyncio.sleep(0.01)

    def test_lifecycle_both_sides_idempotent_and_multiple_pending(self):
        async def go():
            end = 1_000_000
            a = self.market(1, end - 300)                                    # A: ended earlier
            b = self.market(2, end + 300)                                    # B: trading now
            c = self.market(3, end)                                          # C: just ended
            for cid, up, dn in (a, c, b):
                self.led.record_fill(f"{cid}u", up, shares=10, price=0.40, condition_id=cid, fee=0.0)
                self.led.record_fill(f"{cid}d", dn, shares=4, price=0.55, condition_id=cid, fee=0.0)
            self.clock[0] = end + 1
            await self.ticks()
            states = {r.condition_id: r.state for r in self.w.records.values()}
            self.assertEqual(states[a[0]], ST.PENDING)
            self.assertEqual(states[c[0]], ST.PENDING)
            self.assertEqual(states[b[0]], ST.ACTIVE)                        # B unaffected by A and C
            self.assertEqual(self.fetch.calls[b[0]], 0)                      # never asked before it ends
            self.fetch.answers[c[0]] = Resolution(c[0], RESOLVED, {c[1]: 0.0, c[2]: 1.0}, "Down")
            await self.ticks()
            rc = self.w.records[c[0]]
            self.assertEqual(rc.state, ST.SETTLED)
            self.assertAlmostEqual(rc.realized, 4 * 1.0 - (10 * 0.40 + 4 * 0.55))
            self.assertEqual(self.w.records[a[0]].state, ST.PENDING)        # A still waits, independently
            realized_once = self.led.summary()["realized_pnl"]
            for _ in range(3):                                               # duplicate notifications
                await self.w._apply_result(rc, self.fetch.answers[c[0]])
                self.led.settle(self.fetch.answers[c[0]])
            self.assertAlmostEqual(self.led.summary()["realized_pnl"], realized_once)
            self.assertGreaterEqual(self.w.duplicate_results, 3)
            self.assertEqual(rc.redeem, ST.REDEEM_PAPER)                     # paper: never "REDEEMED"
            names = " ".join(self.events)
            for ev in ("MARKET_EXPIRED", "SETTLEMENT_PENDING", "RESULT_RECEIVED", "ACCOUNTING_SETTLED"):
                self.assertIn(ev, names)
        asyncio.run(go())

    def test_api_failure_and_not_final_never_settle(self):
        async def go():
            cid, up, dn = self.market(5, 1_000_000)
            self.led.record_fill("x", up, shares=10, price=0.4, condition_id=cid, fee=0.0)
            self.clock[0] = 1_000_001
            self.fetch.answers[cid] = RuntimeError("rpc down")
            await self.ticks(15)
            self.assertEqual(self.w.records[cid].state, ST.PENDING)
            self.assertIn("RESULT_API_ERROR", " ".join(self.events))
            self.fetch.answers[cid] = Resolution(cid, "UNKNOWN", {}, detail="CLOB and Gamma final payouts disagree")
            await self.ticks(15)
            self.assertEqual(self.w.records[cid].state, ST.PENDING)
            self.assertIn("RESULT_NOT_FINAL", " ".join(self.events))
            self.assertFalse(self.led.positions[up].settled)
        asyncio.run(go())

    def test_a_slow_result_read_never_blocks_the_event_loop(self):
        async def go():
            cid, up, dn = self.market(6, 1_000_000)
            self.led.record_fill("x", up, shares=10, price=0.4, condition_id=cid, fee=0.0)
            self.clock[0] = 1_000_001
            gate = threading.Event()
            self.fetch.block[cid] = gate                                     # the RPC hangs
            worst = 0.0
            for _ in range(30):
                t0 = time.perf_counter()
                await self.w.tick()
                await asyncio.sleep(0)
                worst = max(worst, time.perf_counter() - t0)
                await asyncio.sleep(0.01)
            gate.set()
            await self.ticks(5)
            await self.w.stop()
            self.assertLess(worst, 0.05)                                     # the loop kept running
        asyncio.run(go())

    def test_live_redemption_is_proven_from_the_wallet_never_assumed(self):
        async def go():
            balances = {}
            w = ST.SettlementWorker(self.led, interval=0.05, fetch=self.fetch, clock=lambda: self.clock[0],
                                    poll_active=False, tick_s=0.01, paper=False, redeem_every_s=0.0,
                                    redeem_check=lambda tok: (balances.get(tok, 10.0), "test"))
            self.w = w
            cid, up, dn = self.market(7, 1_000_000)
            self.led.record_fill("x", up, shares=10, price=0.4, condition_id=cid, fee=0.0)
            self.clock[0] = 1_000_001
            self.fetch.answers[cid] = Resolution(cid, RESOLVED, {up: 1.0, dn: 0.0}, "Up")
            await self.ticks(20)
            rec = w.records[cid]
            self.assertEqual((rec.state, rec.redeem), (ST.SETTLED, ST.REDEEM_PENDING))   # winner known != redeemed
            balances[up] = 0.0                                               # the shares left the wallet
            await self.ticks(20)
            self.assertEqual(rec.redeem, ST.REDEEMED)
            await w.stop()
        asyncio.run(go())


# =========================================================================== RESTART / PROCESS
class Restart(I.PaperIOCBase):
    def test_R1_round_caps_continue_from_the_ledger_after_a_restart(self):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_MAX_SIDE_SHARES="20"))
        self.sn._write = self.rows.append
        for _ in range(2):
            self.fire("UP", [(0.47, 100)])
            self.sn.last_fire_by_side["UP"] = -1e18
        self.assertAlmostEqual(self.held(I.UP), 20.0, places=6)
        # a new process on the persisted ledger
        led2 = Ledger(path=self.broker.ledger.path)
        self.assertTrue(led2.load())
        self.assertAlmostEqual(led2.positions[I.UP].shares, 20.0, places=6)         # inventory persisted
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_MAX_SIDE_SHARES="20"))
        self.sn._write = self.rows.append
        self.assertEqual(self.sn.seed_rounds(led2), 1)
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r["result"], r["reason"]), ("skipped", "round cap"))       # was "filled" (30 held)
        self.assertAlmostEqual(self.held(I.UP), 20.0, places=6)
        r = self.fire("DOWN", [(0.47, 100)])                                          # the other side is free
        self.assertEqual(r["result"], "filled")

    def test_run_seeds_the_round_caps_before_the_sniper_starts(self):
        src = (ROOT / "run.py").read_text(encoding="utf-8")
        seed, start = src.index("rt.sniper.seed_rounds"), src.index('rt.sniper_task = asyncio.create_task')
        self.assertLess(seed, start)


class ProcessLock(unittest.TestCase):
    def test_a_second_instance_on_the_same_state_is_refused(self):
        import run
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "bot.lock"
            a = run._ProcessLock(path)
            a.acquire()
            try:
                code = ("import sys, pathlib; sys.path.insert(0, %r); import testing_env, run;"
                        "lk = run._ProcessLock(pathlib.Path(%r));\n"
                        "try:\n    lk.acquire(); print('ACQUIRED')\nexcept Exception as e:\n    print('REFUSED')")
                out = subprocess.run([sys.executable, "-c", code % (str(ROOT), str(path))], capture_output=True,
                                     text=True, timeout=60).stdout
                self.assertIn("REFUSED", out)
            finally:
                a.release()


# =========================================================================== EVENT LOOP / RESOURCES
class EventLoopWork(unittest.TestCase):
    def test_dashboard_accounting_runs_off_the_event_loop(self):
        import run
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            m = I.Market(I.current_round_end())
            br = PaperBroker(Ledger(path=str(tmp / "l.json")),
                             market_context=lambda: {"condition_id": m.condition_id, "up_token_id": m.up_token,
                                                     "down_token_id": m.down_token},
                             host="https://public.invalid", max_buy_price=0.95, min_buy_price=0.05,
                             start_balance=100, account_path=tmp / "a.json", audit_path=tmp / "o.jsonl",
                             book_fetch=m.fetch, rules_fetch=lambda cid: MarketRules(
                                 cid, D("0"), 1, min_order_size=D("5"), tick_size=D("0.01"),
                                 up_token_id=m.up_token, down_token_id=m.down_token),
                             min_seconds_to_expiry=0, trade_window_seconds=300, own_fill_ttl_s=0)
            hub = types.SimpleNamespace(up_token=m.up_token, down_token=m.down_token, condition_id=m.condition_id,
                                        window_end=m.window_end, window_start=m.window_end - 300,
                                        book=types.SimpleNamespace(view=lambda t: types.SimpleNamespace(
                                            best_bid=0.46, best_ask=0.47, status="LIVE")),
                                        health=lambda: {})
            rt = types.SimpleNamespace(ledger=br.ledger, broker=br, hub=hub, live_exec=None, started=time.time(),
                                       sniper=None, journal_path=None, settler=None)
            view = run.DashView(rt, accounting_off_loop=True)
            threads = []
            real = view._accounting_nowait

            def spy():
                threads.append(threading.get_ident())
                return real()
            view._accounting_nowait = spy

            async def go():
                view._refresh_slow()
                for _ in range(200):
                    await asyncio.sleep(0.005)
                    view._collect_accounting()
                    if view._acct:
                        return
            asyncio.run(go())
            self.assertTrue(threads and all(t != threading.get_ident() for t in threads))   # never on the loop
            self.assertEqual(view._acct.get("kind"), "PAPER")
            self.assertAlmostEqual(view._acct["cash"], 100.0)

    def test_manager_cash_is_recomputed_after_every_fill(self):
        with tempfile.TemporaryDirectory() as tmp:
            led = Ledger(path=os.path.join(tmp, "l.json"))
            cash = [500.0]
            calls = []
            broker = types.SimpleNamespace(cash_balance=lambda: calls.append(1) or cash[0], _rules_cache={})
            clock = [100.0]
            hub = types.SimpleNamespace(up_token="1", down_token="2", condition_id="0x" + "1" * 64,
                                        window_end=time.time() + 200, book=None)
            pm = PM.PositionManager(CFG, hub=hub, ledger=led, broker=broker, mono=lambda: clock[0])
            self.addCleanup(pm.journal.close)
            self.assertEqual(pm._cash_free(), 500.0)
            cash[0] = 400.0                                                   # a fill lowered cash ...
            led.record_fill("f1", "1", shares=1, price=0.5, condition_id=hub.condition_id, fee=0.0)
            self.assertEqual(pm._cash_free(), 400.0)                          # ... seen at once
            cash[0] = 450.0                                                   # a payout raised it
            self.assertEqual(pm._cash_free(), 400.0)                          # understated, for under 1s
            clock[0] += 1.0
            self.assertEqual(pm._cash_free(), 450.0)
            clock[0] -= 5.0                                                   # a clock going back recomputes
            self.assertEqual(pm._cash_free(), 450.0)
            self.assertEqual(len(calls), 4)

    def test_old_rounds_and_finished_manager_orders_are_dropped(self):
        with env(SPEC):
            sn = S.Sniper(types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_end=None),
                          types.SimpleNamespace(), settings=S.Settings())
        for end in (1_000, 2_000, 5_000, 8_000):                             # earlier rounds' entries
            sn.rounds[end]["fills"] = 1
        view = types.SimpleNamespace(status="LIVE", asks=((0.47, 100.0),), best_ask=0.47,
                                     updated_mono=time.monotonic(), tick_size=0.01)
        hub = types.SimpleNamespace(up_token="u", down_token="d", condition_id="c", window_end=9_000,
                                    book=types.SimpleNamespace(view=lambda t: view))
        sn.hub = hub
        sn.broker = types.SimpleNamespace(min_buy_price=0.05, max_buy_price=0.95, trade_window_seconds=300,
                                          min_seconds_to_expiry=0)
        with patch.object(S.time, "time", lambda: 8_950.0):
            self.assertIsNotNone(sn._decide("UP", {}, 0.0))                  # round 9,000's first decision
        self.assertEqual(sorted(sn.rounds), [8_000, 9_000])                # ended > 1h before 9,000: gone
        pm = PM.PositionManager(CFG, hub=hub, ledger=Ledger(path=os.path.join(tempfile.mkdtemp(), "l.json")),
                                broker=types.SimpleNamespace())
        self.addCleanup(pm.journal.close)
        for i in range(1, 260):
            o = PM.ManagerOrder(None, "POSITION_MANAGER", "UP", "x", 1.0, 1.0, 0.5, 0.5, condition="c",
                                reservation_id=i)
            o.finished = i < 250                                               # 9 still owed an outcome
            pm.orders[i] = o
        pm._prune_orders()
        self.assertEqual(sum(1 for o in pm.orders.values() if o.finished), PM.PositionManager.KEEP_FINISHED)
        self.assertEqual(sum(1 for o in pm.orders.values() if not o.finished), 10)
        self.assertEqual(max(pm.orders), 259)                                  # the newest are the ones kept


# =========================================================================== SECURITY / ERRORS
class Security(unittest.TestCase):
    def tracked(self):
        out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True).stdout.split()
        return [f for f in out if not f.endswith((".gz", ".png"))]

    def test_no_env_file_or_private_key_is_tracked(self):
        files = self.tracked()
        self.assertNotIn(".env", files)
        secret_assign = re.compile(r"^(POLY_PRIVATE_KEY|PRIVATE_KEY|POLY_API_SECRET|POLY_PASSPHRASE|POLY_API_KEY|"
                                   r"CLOB_SECRET|CLOB_PASS_PHRASE|CLOB_API_KEY)[ \t]*=[ \t]*\S+", re.M)
        hits = []
        for f in files:
            text = (ROOT / f).read_text(encoding="utf-8", errors="replace")
            for m in secret_assign.finditer(text):
                value = m.group(0).split("=", 1)[1].strip()
                if value not in ("", '""', "''") and not value.startswith(("<", "your", "0x...", "#")):
                    hits.append((f, m.group(1)))                                     # names only, never values
        self.assertEqual(hits, [])

    def test_credentials_never_reach_reprs_or_config_lines(self):
        from feeds.poly_user import PolyUserFeed
        feed = PolyUserFeed({"apiKey": "KEY-SHOULD-NOT-PRINT", "secret": "SECRET-SHOULD-NOT-PRINT",
                             "passphrase": "PASS-SHOULD-NOT-PRINT"})
        text = repr(feed) + str(feed)
        for leak in ("KEY-SHOULD-NOT-PRINT", "SECRET-SHOULD-NOT-PRINT", "PASS-SHOULD-NOT-PRINT"):
            self.assertNotIn(leak, text)
        with env(SPEC, {"POLY_PRIVATE_KEY": "0x" + "ab" * 32}):
            lines = "\n".join(exec_latency.effective_config_lines(S.Settings(), live=True, canary_max_shares=25))
        self.assertNotIn("ab" * 32, lines)


class ErrorHandling(unittest.TestCase):
    def test_no_bare_except_in_production_code(self):
        bare = []
        for name in PRODUCTION:
            tree = ast.parse((ROOT / name).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.ExceptHandler) and node.type is None:
                    bare.append(f"{name}:{node.lineno}")
        self.assertEqual(bare, [])


if __name__ == "__main__":
    unittest.main(verbosity=1)
