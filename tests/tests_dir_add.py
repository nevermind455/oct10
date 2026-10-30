#!/usr/bin/env python3
"""SNIPER_DIR_ADD - the direction add-on (2026-10-30, the operator's request).

    python tests/tests_dir_add.py

"Keep the sniper strategy the same, add a little direction for profit." The
sniper decides and sizes every order as before; when the side it buys is already
beating the round's price to beat (the trigger feed's mid at the round's start),
the same order buys SNIPER_DIR_ADD of its Kelly target more:
  1  the price to beat is the first trusted trigger mid of each 5-minute round,
     unknown if it came more than SNIPER_PTB_MAX_GAP_S after the start
  2  a side beating it gets the add-on on top of the sniper's own shares
  3  a side not beating it, an unknown price to beat or a stale mid: the
     sniper's order exactly as without the add-on
  4  the add-on only uses room the sniper's own limits leave, and in LIVE only
     where the send checks the sniper's order passes still pass with it
  5  Kelly sizes the sniper's later orders without the add-on's filled shares
  6  off by default (the price to beat is journalled anyway); LIVE refuses it
     without SNIPER_DIR_ADD_LIVE=1 and above 0.5; bad values stop the start
The real Sniper and paper broker (tests_sniper_fair.Gate). No network.
"""
from __future__ import annotations

import math
import pathlib
import sys
import time
import types
import unittest
from decimal import Decimal

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import exec_latency as X  # noqa: E402
import paper_parity  # noqa: E402
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from dashboard.sniper_layout import _beat_text  # noqa: E402
from hybrid_sizing import cent_compatible_shares  # noqa: E402

PTB = 60_000.0
T0 = 1_800_000_000.0           # a round start: a multiple of 300s


class PriceToBeat(unittest.TestCase):
    def sniper(self, **env):
        return S.Sniper(I.Market(T0 + 300), None, settings=I.settings(**env))

    def test_the_first_trusted_mid_of_each_round_and_never_a_late_one(self):
        sn = self.sniper()
        sn._note_primary_mid(60_000.0, T0 + 0.4, 1.0)
        sn._note_primary_mid(60_100.0, T0 + 10.0, 2.0)            # later in the same round: not the price to beat
        self.assertEqual(sn.price_to_beat(T0 + 300), 60_000.0)
        self.assertEqual(sn.primary_mid, (60_100.0, 2.0))
        sn._note_primary_mid(60_200.0, T0 + 300 + 7.0, 3.0)      # first heard 7s in (a restart): unknown
        self.assertIsNone(sn.price_to_beat(T0 + 600))
        self.assertEqual(sn.ptb_rounds[T0 + 300], (None, 7.0))
        sn._note_primary_mid(60_300.0, T0 + 600 + 0.1, 4.0)
        self.assertEqual(sn.price_to_beat(T0 + 900), 60_300.0)
        self.assertIsNone(sn.price_to_beat(None))
        self.assertIsNone(sn.price_to_beat(T0 + 3000))            # a round never seen

    def test_the_allowed_gap_is_a_setting_and_old_rounds_go(self):
        sn = self.sniper(SNIPER_PTB_MAX_GAP_S="10")
        sn._note_primary_mid(60_200.0, T0 + 7.0, 1.0)
        self.assertEqual(sn.price_to_beat(T0 + 300), 60_200.0)
        for i in range(1, 7):
            sn._note_primary_mid(60_000.0 + i, T0 + 300 * i + 0.2, 1.0 + i)
        self.assertLessEqual(len(sn.ptb_rounds), 4)
        self.assertEqual(sn.price_to_beat(T0 + 300 * 7), 60_006.0)

    def test_a_coinbase_ticker_sets_it(self):
        import tests_sniper_coinbase as C
        sn, _fired = C.make()
        sn.on_coinbase(C.ticker(60_000.0))
        self.assertAlmostEqual(sn.primary_mid[0], 60_000.0, places=6)
        now = time.time()
        self.assertIn(now - now % S.ROUND_S, sn.ptb_rounds)

    def test_beating_is_measured_in_the_signals_direction(self):
        sn = self.sniper()
        sn.ptb_rounds[T0] = (PTB, 0.2)
        sn.primary_mid = (PTB * (1 + 3e-4), time.monotonic())    # 3bp above
        beating, f = sn._ptb_view("UP", T0 + 300)
        self.assertTrue(beating)
        self.assertEqual(f, {"ptb": PTB, "ptb_dist_bp": 3.0})
        beating, f = sn._ptb_view("DOWN", T0 + 300)
        self.assertFalse(beating)
        self.assertEqual(f["ptb_dist_bp"], -3.0)
        sn.primary_mid = (PTB, time.monotonic())                  # exactly at it: not beating
        self.assertFalse(sn._ptb_view("UP", T0 + 300)[0])
        sn.primary_mid = (PTB * 1.001, time.monotonic() - S.PTB_MID_MAX_AGE_S - 1)
        self.assertEqual(sn._ptb_view("UP", T0 + 300), (None, {"ptb": PTB, "ptb_dist_bp": None}))
        self.assertEqual(sn._ptb_view("UP", T0 + 600), (None, {"ptb": None}))


class Decision(F.Gate):
    """Share sizing, as live runs it (the frozen spec), through the real decision and paper fill."""

    def make(self, **env):
        super().make(**{"SNIPER_ORDER_SIZING": "shares", **env})
        # $100, not the harness's $1000: Kelly then wants tens of shares, not the side's whole
        # 200-share cap, so there is room left for an add-on
        self.sn._read_bankroll = lambda: 100.0

    def beat(self, bp: float, *, age_s: float = 0.0) -> None:
        """The current round's price to beat, and the trigger mid now `bp` past it (UP's way)."""
        self.sn.ptb_rounds[float(self.m.window_end) - S.ROUND_S] = (PTB, 0.2)
        self.sn.primary_mid = (PTB * (1 + bp / 1e4), time.monotonic() - age_s)

    def base_shares(self, r) -> int:
        """What the sniper alone would sign: its Kelly target in whole-cent shares at the limit."""
        return cent_compatible_shares(r["kelly_shares"], Decimal(str(r["cap"])))

    def test_a_side_beating_the_price_to_beat_buys_the_add_on_on_top(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(+5.0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        base = self.base_shares(r)
        want = cent_compatible_shares(r["kelly_shares"] + math.floor(r["kelly_shares"] * 0.5), Decimal(str(r["cap"])))
        self.assertEqual(r["dir_base_shares"], base)
        self.assertEqual(r["order_shares"], want)
        self.assertEqual(r["dir_add_shares"], want - base)
        self.assertGreater(r["dir_add_shares"], 0)
        self.assertNotIn("dir_add_why", r)
        self.assertEqual((r["ptb"], r["ptb_dist_bp"]), (PTB, 5.0))
        self.assertAlmostEqual(r["target_shares"], r["kelly_shares"] + r["dir_add_shares"])

    def test_a_down_side_beats_it_when_btc_is_below(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(-4.0)
        r = self.order("DOWN", [(0.73, 1000)], mid=0.72, move_bp=-6.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual(r["ptb_dist_bp"], 4.0)
        self.assertGreater(r["dir_add_shares"], 0)

    def test_a_side_not_beating_it_is_the_snipers_order_unchanged(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(-5.0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual(r["order_shares"], self.base_shares(r))
        self.assertEqual((r["dir_add_shares"], r["dir_add_why"]), (0, "side not beating the price to beat"))
        self.assertEqual(r["target_shares"], r["kelly_shares"])

    def test_an_unknown_price_to_beat_adds_nothing(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)       # never heard this round's start
        self.assertEqual(r["result"], "filled", r)
        self.assertIsNone(r["ptb"])
        self.assertEqual((r["dir_add_shares"], r["dir_add_why"]), (0, "price to beat unknown"))
        self.assertEqual(r["order_shares"], self.base_shares(r))

    def test_a_stale_trigger_mid_adds_nothing(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(+5.0, age_s=S.PTB_MID_MAX_AGE_S + 1)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual((r["ptb_dist_bp"], r["dir_add_why"]), (None, "price to beat unknown"))
        self.assertEqual(r["order_shares"], self.base_shares(r))

    def test_the_add_on_never_takes_room_the_sniper_does_not_leave(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.sn._read_bankroll = lambda: 1000.0                     # Kelly wants more than the room
        self.pin_time_left(120.0)
        self.kelly_wants_the_room(20.0)
        self.sn.rounds[self.m.window_end]["UP"] = 180.0           # 20 left: Kelly takes them all
        self.beat(+5.0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertLessEqual(r["order_shares"], 20)
        self.assertEqual((r["dir_add_shares"], r["dir_add_why"]), (0, "no room left by the sniper's limits"))

    def test_off_by_default_the_price_to_beat_is_only_journalled(self):
        self.pin_time_left()
        self.beat(+5.0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual((r["ptb"], r["ptb_dist_bp"]), (PTB, 5.0))
        self.assertEqual(r["order_shares"], self.base_shares(r))
        for key in ("dir_add_shares", "dir_base_shares", "dir_add_why"):
            self.assertNotIn(key, r)

    def test_kelly_sizes_the_snipers_next_order_without_the_add_on(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(+5.0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertGreater(r["dir_add_shares"], 0)
        cond, up, down, end = self.m.condition_id, self.m.up_token, self.m.down_token, self.m.window_end
        added = self.sn._dir_held(cond, up)
        self.assertAlmostEqual(added, r["shares"] * r["dir_add_shares"] / r["order_shares"], places=6)
        own, _opp = self.sn._kelly_inventory(cond, end, "UP", up, down)
        self.sn._dir_filled.clear()
        own_all, _opp = self.sn._kelly_inventory(cond, end, "UP", up, down)
        self.assertAlmostEqual(own, own_all - added, places=6)
        self.assertAlmostEqual(own_all, r["shares"], places=4)


# run only this file's tests, not every inherited tests_sniper_fair.Gate test again
for _name in [n for n in dir(F.Gate) if n.startswith("test") and n not in Decision.__dict__]:
    setattr(Decision, _name, None)


class AddOnRules(unittest.TestCase):
    """Sniper._dir_add on its own: the sniper's order in, the order with or without the add-on out."""

    def setUp(self):
        self.sn = S.Sniper(I.Market(T0 + 300), None, settings=I.settings(
            SNIPER_DIR_ADD="0.5", SNIPER_FAIR="1", SNIPER_KELLY="1", SNIPER_ORDER_SIZING="shares"))

    def add(self, *, beating=True, target=10.0, base=10, floor=5, room=100.0, limit="0.50", fixed=False):
        row = {}
        shares, new_target = self.sn._dir_add(row, beating, target, base, floor, room, Decimal(limit), 0.07, fixed)
        return shares, new_target, row

    def test_half_the_kelly_target_more(self):
        self.assertEqual(self.add()[:2], (15, 15.0))
        self.assertEqual(self.add(target=11.0, base=11)[:2], (16, 16.0))          # floor(5.5) = 5

    def test_bounded_by_the_room_and_whole_cent_stakes(self):
        shares, target, row = self.add(room=12.0)
        self.assertEqual((shares, target, row["dir_add_shares"]), (12, 12.0, 2))
        shares, _t, row = self.add(target=10.0, base=10, room=13.0, limit="0.505")  # 0.505: even shares only
        self.assertEqual((shares, row["dir_add_shares"]), (12, 2))
        shares, _t, row = self.add(room=10.0)
        self.assertEqual((shares, row["dir_add_why"]), (10, "no room left by the sniper's limits"))

    def test_never_changes_whether_or_how_the_sniper_trades(self):
        for kw, why in (({"beating": None}, "price to beat unknown"),
                        ({"beating": False}, "side not beating the price to beat"),
                        ({"fixed": True}, "fixed-size order"),
                        ({"base": 4}, "sniper order outside its own room"),           # the sniper skips: so does the add-on
                        ({"base": 10, "room": 9.0}, "sniper order outside its own room")):
            shares, target, row = self.add(**kw)
            self.assertEqual((shares, target, row["dir_add_shares"], row["dir_add_why"]),
                             (kw.get("base", 10), 10.0, 0, why), kw)
            self.assertEqual(row["dir_base_shares"], kw.get("base", 10))

    def test_live_drops_the_add_on_where_a_send_check_would_refuse_it(self):
        asked = []

        def headroom(spend, fee_rate):
            asked.append((spend, fee_rate))
            return "canary exposure cap $30.00"
        self.sn.live = types.SimpleNamespace(addon_headroom=headroom)
        shares, target, row = self.add()
        self.assertEqual((shares, target), (10, 10.0))
        self.assertEqual(row["dir_add_why"], "live limits: canary exposure cap $30.00")
        self.assertEqual(asked, [(7.5, 0.07)])                    # 15 shares at the 0.50 limit
        self.sn.live = types.SimpleNamespace(addon_headroom=lambda spend, fee_rate: None)
        self.assertEqual(self.add()[:2], (15, 15.0))

    def test_fills_are_booked_pro_rata_and_retracted(self):
        plan = {"dir_add_shares": 5, "shares": 15, "cond": "c", "token": "t", "end": time.time() + 100}
        self.sn._dir_book(plan, 12.0)
        self.assertAlmostEqual(self.sn._dir_held("c", "t"), 4.0)
        self.sn._dir_book(plan, 12.0, sign=-1.0)
        self.assertEqual(self.sn._dir_held("c", "t"), 0.0)
        self.sn._dir_book(dict(plan, dir_add_shares=0), 12.0)
        self.sn._dir_book(plan, 0.0)
        self.assertEqual(self.sn._dir_held("c", "t"), 0.0)


class LiveHeadroom(unittest.TestCase):
    """LiveExecution.addon_headroom: the send checks prepare would run, read only."""

    def live(self, *, balance=100.0, reserved=10.0, exposure=5.0, used=2.0):
        lx = object.__new__(L.LiveExecution)
        lx.balance, lx.reserved = balance, reserved
        lx.sender = types.SimpleNamespace(limits=types.SimpleNamespace(
            exposure_usd=exposure, max_exposure_usd=30.0, max_daily_loss_usd=15.0))
        lx._risk_budget_used = used if callable(used) else (lambda: used)
        return lx

    def test_fits_or_names_the_check_that_would_refuse_it(self):
        self.assertIsNone(self.live().addon_headroom(5.0, 0.07))
        self.assertEqual(self.live(balance=12.0).addon_headroom(5.0, 0.07), "collateral $2.00 below $5.35")
        self.assertEqual(self.live().addon_headroom(26.0, 0.0), "canary exposure cap $30.00")
        self.assertEqual(self.live(used=12.0).addon_headroom(5.0, 0.07), "canary loss budget $15.00")

        def broken():
            raise RuntimeError("ledger")
        self.assertEqual(self.live(used=broken).addon_headroom(5.0, 0.07), "loss budget unknown (RuntimeError)")


class SettingsAndLive(unittest.TestCase):
    ON = {"SNIPER_DIR_ADD": "0.5", "SNIPER_FAIR": "1", "SNIPER_KELLY": "1", "SNIPER_ORDER_SIZING": "shares"}

    def test_live_refuses_it_without_the_opt_in_and_above_half(self):
        dir_rules = lambda s: [v for v in X.spec_violations(s) if "direction add-on" in v]  # noqa: E731
        self.assertTrue(dir_rules(I.settings(**self.ON)))
        self.assertEqual(dir_rules(I.settings(**self.ON, SNIPER_DIR_ADD_LIVE="1")), [])
        self.assertEqual(dir_rules(I.settings(**{**self.ON, "SNIPER_DIR_ADD_LIVE": "1", "SNIPER_DIR_ADD": "0.25"})), [])
        bad = dir_rules(I.settings(**{**self.ON, "SNIPER_DIR_ADD_LIVE": "1", "SNIPER_DIR_ADD": "0.75"}))
        self.assertTrue(bad and "at most 0.5" in bad[0], bad)
        self.assertEqual(dir_rules(I.settings(SNIPER_FAIR="1")), [])

    def test_bad_values_stop_the_start(self):
        for env in ({"SNIPER_DIR_ADD": "-0.1"}, {"SNIPER_DIR_ADD": "1.5"}, {"SNIPER_DIR_ADD": "abc"},
                    {"SNIPER_ORDER_SIZING": "dollars"}, {"SNIPER_KELLY": "0"},
                    {"SNIPER_PTB_MAX_GAP_S": "0"}, {"SNIPER_PTB_MAX_GAP_S": "61"}):
            with self.assertRaises(ValueError, msg=env):
                I.settings(**{**self.ON, **env})

    def test_the_start_line_says_so_only_when_on(self):
        on, off = I.settings(**self.ON), I.settings(SNIPER_FAIR="1")
        self.assertIn("DIRECTION ADD-ON: +50% of the Kelly target", on.lines()[0])
        self.assertIn("DIRECTION ADD-ON (LIVE TEST, SNIPER_DIR_ADD_LIVE)", on.lines(live=True)[0])
        self.assertNotIn("DIRECTION", off.lines()[0] + off.lines(live=True)[0])

    def test_strict_paper_comparison_matches_it_with_live(self):
        for key in ("dir_add", "dir_add_live", "ptb_max_gap_s"):
            self.assertIn(key, paper_parity.CONFIG_FIELDS)

    def test_the_pnl_report_splits_the_add_on_from_the_sniper(self):
        import live_pnl
        self.assertEqual(live_pnl.direction_summary([{"ptb_dist": None, "won": True, "pnl": 1.0}]), [])
        done = [
            # 15 shares, 5 of them the add-on's: a third of the order's PnL
            {"ptb_dist": 3.0, "won": True, "pnl": 6.0, "dir_add": 5, "order_shares": 15},
            {"ptb_dist": 2.0, "won": False, "pnl": -2.0, "dir_add": 0, "order_shares": 10},
            {"ptb_dist": -4.0, "won": False, "pnl": -3.0, "dir_add": 0, "order_shares": 10},
            {"ptb_dist": None, "won": True, "pnl": 1.0, "dir_add": 0, "order_shares": 10},
        ]
        text = "\n".join(live_pnl.direction_summary(done))
        self.assertIn("price to beat known on 3 of 4 resolved trades", text)
        self.assertIn("side beating it :    2 trades, won 1 (50%), PnL $+4.00", text)
        self.assertIn("side not beating:    1 trades, won 0 (0%), PnL $-3.00", text)
        self.assertIn("add-on bought on 1 orders: its own PnL $+2.00, the sniper's part of those orders $+4.00", text)
        self.assertIn("+25% $+0.50, +50% $+1.00", text)                 # on the sniper's own $+2.00 there

    def test_the_dashboard_shows_the_price_to_beat(self):
        self.assertEqual(_beat_text({"btc_start": 60_000.0, "mid": 60_030.0}), "  to beat ~60,000 (+5.0bp)")
        self.assertEqual(_beat_text({"btc_start": 60_000.0}), "  to beat ~60,000")
        self.assertEqual(_beat_text({"btc_start": None, "mid": 60_030.0}), "  to beat N/A")


if __name__ == "__main__":
    unittest.main(verbosity=1)
