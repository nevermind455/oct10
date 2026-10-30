#!/usr/bin/env python3
"""SNIPER_DIR_ADD - the direction add-on (2026-10-30, the operator's request).

    python tests/tests_dir_add.py

"Keep the sniper strategy the same, add a little direction for profit." The
sniper decides and sizes every order as before; when the side it buys is past the
round's price to beat by more than its estimate can be wrong, the same order buys
SNIPER_DIR_ADD of its Kelly target more:
  1  the price to beat is Chainlink's 60s TWAP at the round's start ("make sure it
     is exact"): estimated live as the trigger feed's own 60s TWAP, corrected by and
     checked against every value Polymarket publishes after the close; unknown when
     the trigger prices do not cover the window
  2  a side past it by more than the estimate's largest recent error (and the
     SNIPER_PTB_MARGIN_BP floor), after SNIPER_PTB_MIN_CHECKS published rounds,
     gets the add-on on top of the sniper's own shares
  3  a side not past it, too close to call, not yet checked, an unknown price to
     beat or a stale mid: the sniper's order exactly as without the add-on
  4  the add-on only uses room the sniper's own limits leave, and in LIVE only
     where the send checks the sniper's order passes still pass with it
  5  Kelly sizes the sniper's later orders without the add-on's filled shares
  6  off by default (the price to beat is journalled anyway); LIVE refuses it
     without SNIPER_DIR_ADD_LIVE=1 and above 0.5; bad values stop the start
The real Sniper and paper broker (tests_sniper_fair.Gate). No network.
"""
from __future__ import annotations

import asyncio
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
import price_to_beat as P  # noqa: E402
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from dashboard.sniper_layout import _beat_text  # noqa: E402
from hybrid_sizing import cent_compatible_shares  # noqa: E402

PTB = 60_000.0
T0 = 1_800_000_000.0           # a round start: a multiple of 300s


class Twap(unittest.TestCase):
    """price_to_beat.twap: Chainlink's 60s TWAP at the round's start, computed the same way on the trigger feed."""

    def test_each_price_stands_until_the_next(self):
        samples = [(T0 - 61, 100.0), (T0 - 30, 200.0), (T0 + 1, 999.0)]   # after the start: not used
        self.assertEqual(P.twap(samples, T0, max_gap=60), (150.0, None))
        flat = [(T0 - 60 + i, 80_000.0) for i in range(60)]
        self.assertAlmostEqual(P.twap(flat, T0, max_gap=5)[0], 80_000.0)

    def test_a_window_the_prices_do_not_cover_is_unknown(self):
        late = [(T0 - 30 + i, 80_000.0) for i in range(30)]               # started 30s before the round
        price, why = P.twap(late, T0, max_gap=5)
        self.assertIsNone(price)
        self.assertIn("60s before the round's start", why)
        gap = [(T0 - 61, 1.0), (T0 - 40, 1.0), (T0 - 1, 1.0)]             # 21s and 39s without a price
        price, why = P.twap(gap, T0, max_gap=5)
        self.assertIsNone(price)
        self.assertIn("39.0s gap", why)
        stale_end = [(T0 - 60 + i, 1.0) for i in range(50)]               # nothing in the last 10s
        self.assertIsNone(P.twap(stale_end, T0, max_gap=5)[0])

    def test_it_is_the_markets_own_lookback(self):
        import market_discovery
        self.assertEqual(P.TWAP_S, market_discovery.EXPECTED_TWAP_LOOKBACK_SECONDS)


class Published(unittest.TestCase):
    """What Polymarket publishes after the close (Gamma eventMetadata), and the checks against it."""

    def test_the_value_from_a_gamma_event(self):
        self.assertEqual(P.official_from_event({"eventMetadata": {"priceToBeat": 80953.12, "finalPrice": "80961.5"}}),
                         (80953.12, 80961.5))
        self.assertEqual(P.official_from_event({"eventMetadata": '{"priceToBeat": "80953.12"}'}), (80953.12, None))
        for event in (None, {}, {"eventMetadata": None}, {"eventMetadata": "{bad"},
                      {"eventMetadata": {"priceToBeat": "n/a"}}, {"eventMetadata": {"priceToBeat": -1}},
                      {"eventMetadata": {"priceToBeat": float("nan")}}):
            self.assertIsNone(P.official_from_event(event)[0], event)
        self.assertEqual(P.metadata_keys({"eventMetadata": {"b": 1, "a": 2}}), ["a", "b"])
        self.assertEqual(P.metadata_keys({}), [])

    def test_the_basis_corrects_and_the_error_is_measured_once_it_has_rounds(self):
        c = P.Calibration()
        self.assertEqual(c.estimate(80_000.0), (80_000.0, 0))
        for i in range(3):                                       # Chainlink 8 dollars over our TWAP
            est, n = c.estimate(80_000.0)
            row = c.add(T0 + 300 * i, 80_000.0, est, n, 80_008.0)
            self.assertIsNone(row["err_bp"])                     # no basis yet: not an error of the method
        self.assertEqual(c.basis, 8.0)
        self.assertEqual(c.checks, 0)
        est, n = c.estimate(81_000.0)
        self.assertEqual((est, n), (81_008.0, 3))
        row = c.add(T0 + 900, 81_000.0, est, n, 81_016.1)        # 8.1 dollars off the estimate
        self.assertAlmostEqual(row["err_bp"], 8.1 / 81_016.1 * 1e4, places=3)
        self.assertEqual(c.checks, 1)
        self.assertAlmostEqual(c.margin_bp(0.5), 8.1 / 81_016.1 * 1e4)
        self.assertEqual(c.margin_bp(5.0), 5.0)                  # never under the operator's floor
        self.assertEqual(c.add(T0 + 900, 81_000.0, est, n, 81_016.1), {})   # a round counts once
        self.assertEqual(c.checks, 1)

    def test_a_round_without_our_own_twap_still_counts_nothing_wrong(self):
        c = P.Calibration()
        row = c.add(T0, None, None, 0, 80_000.0)
        self.assertEqual((row["raw"], row["err_bp"], c.basis, c.checks), (None, None, 0.0, 0))


class PriceToBeat(unittest.TestCase):
    def sniper(self, **env):
        sn = S.Sniper(I.Market(T0 + 600), None, settings=I.settings(**env))
        sn.rows = []
        sn._write = sn.rows.append
        return sn

    def feed(self, sn, first, last, price=lambda t: 80_000.0, step=1.0):
        t = first
        while t <= last + 1e-9:
            sn._note_primary_mid(price(t), t, t - T0)
            t += step

    def test_each_rounds_estimate_is_the_trigger_feeds_60s_twap_at_its_start(self):
        sn = self.sniper()
        self.feed(sn, T0 + 200, T0 + 300.5, price=lambda t: 80_000.0 if t < T0 + 270 else 80_090.0)
        rec = sn.ptb_rounds[T0 + 300]
        self.assertAlmostEqual(rec["raw"], (80_000.0 * 30 + 80_090.0 * 30) / 60)   # 30s at each price
        self.assertEqual((rec["est"], rec["basis_rounds"], rec["why"]), (rec["raw"], 0, None))
        self.assertEqual(sn.price_to_beat(T0 + 600), rec["est"])
        self.assertLessEqual(len(sn._ptb_hist), 70)                # only the last ~65s are kept
        self.assertIn(T0 + 300, sn._ptb_pending)                    # waits for Polymarket's value

    def test_unknown_after_a_start_too_close_to_the_round(self):
        sn = self.sniper()
        self.feed(sn, T0 + 280, T0 + 300.5)                         # 20s of prices before the start
        rec = sn.ptb_rounds[T0 + 300]
        self.assertIsNone(rec["est"])
        beating, f = sn._ptb_view("UP", T0 + 600)
        self.assertIsNone(beating)
        self.assertIn("60s before the round's start", f["ptb_why"])
        self.assertIsNone(sn.price_to_beat(T0 + 900))              # a round never seen

    def test_a_coinbase_ticker_feeds_it(self):
        import tests_sniper_coinbase as C
        sn, _fired = C.make()
        sn.on_coinbase(C.ticker(60_000.0))
        self.assertAlmostEqual(sn.primary_mid[0], 60_000.0, places=6)
        self.assertEqual(len(sn._ptb_hist), 1)

    def calibrated(self, sn, errors=(0.2,) * 6):
        sn.ptb_calib.errors.extend(errors)
        sn.ptb_rounds[T0 + 300] = {"est": PTB, "raw": PTB, "basis_rounds": 12, "why": None}

    def test_only_past_the_estimates_largest_error_counts_as_beating(self):
        sn = self.sniper()
        self.calibrated(sn, errors=(0.2, 1.5, 0.3, 0.1, 0.4, 0.2))  # margin: max(1.0 floor, 1.5)
        sn.primary_mid = (PTB * (1 + 3e-4), time.monotonic())       # 3bp above
        beating, f = sn._ptb_view("UP", T0 + 600)
        self.assertTrue(beating)
        self.assertEqual(f, {"ptb": PTB, "ptb_dist_bp": 3.0, "ptb_margin_bp": 1.5, "ptb_checks": 6})
        beating, f = sn._ptb_view("DOWN", T0 + 600)
        self.assertFalse(beating)
        self.assertEqual(f["ptb_dist_bp"], -3.0)
        sn.primary_mid = (PTB * (1 + 1e-4), time.monotonic())       # 1bp above: inside the 1.5bp error
        beating, f = sn._ptb_view("UP", T0 + 600)
        self.assertIsNone(beating)
        self.assertEqual(f["ptb_why"], "within the estimate's error (+-1.50bp)")
        sn.primary_mid = (PTB, time.monotonic())                    # exactly at it: not beating
        self.assertFalse(sn._ptb_view("UP", T0 + 600)[0])
        sn.primary_mid = (PTB * 1.001, time.monotonic() - S.PTB_MID_MAX_AGE_S - 1)
        self.assertEqual(sn._ptb_view("UP", T0 + 600),
                         (None, {"ptb": PTB, "ptb_dist_bp": None, "ptb_why": "no fresh trigger price"}))

    def test_not_before_enough_published_rounds_checked_it(self):
        sn = self.sniper()
        self.calibrated(sn, errors=(0.2,) * 5)
        sn.primary_mid = (PTB * 1.001, time.monotonic())
        beating, f = sn._ptb_view("UP", T0 + 600)
        self.assertIsNone(beating)
        self.assertEqual(f["ptb_why"], "estimate checked against Polymarket's published price to beat on 5 of 6 rounds")
        sn = self.sniper(SNIPER_PTB_MIN_CHECKS="5")
        self.calibrated(sn, errors=(0.2,) * 5)
        sn.primary_mid = (PTB * 1.001, time.monotonic())
        self.assertTrue(sn._ptb_view("UP", T0 + 600)[0])

    def test_polymarkets_published_value_checks_and_corrects_each_round(self):
        sn = self.sniper()
        asked = []
        events = {}

        def fetch(slug):
            asked.append(slug)
            return events.get(slug)
        sn._ptb_pending = {T0: {"est": 80_000.0, "raw": 80_000.0, "basis_rounds": 0},
                           T0 + 300: {"est": 80_100.0, "raw": 80_100.0, "basis_rounds": 0}}
        clock = [T0 + 300 + 10.0]                                   # 10s after the first one closed: too soon
        real_time = S.time.time
        S.time.time = lambda: clock[0]
        try:
            asyncio.run(sn._ptb_official_once(fetch))
            self.assertEqual(asked, [])
            clock[0] = T0 + 300 + 30.0
            events["btc-updown-5m-1800000000"] = {"eventMetadata": {"other": 1}}   # not published yet
            asyncio.run(sn._ptb_official_once(fetch))
            self.assertEqual(asked, ["btc-updown-5m-1800000000"])
            self.assertEqual(sn._ptb_pending[T0]["keys"], ["other"])
            events["btc-updown-5m-1800000000"] = {"eventMetadata": {"priceToBeat": 80_008.0, "finalPrice": 80_100.0}}
            asyncio.run(sn._ptb_official_once(fetch))
            self.assertNotIn(T0, sn._ptb_pending)
            row = [r for r in sn.rows if r.get("result") == "ptb_official"][-1]
            self.assertEqual((row["round_start"], row["official"], row["raw"], row["final"]),
                             (T0, 80_008.0, 80_000.0, 80_100.0))
            self.assertEqual(sn.ptb_calib.basis, 8.0)
            clock[0] = T0 + 600 + S.PTB_GIVE_UP_S + 1                # never published: given up, journalled
            asyncio.run(sn._ptb_official_once(fetch))
            self.assertEqual(sn._ptb_pending, {})
            self.assertEqual([r["result"] for r in sn.rows][-1], "ptb_official_missing")
        finally:
            S.time.time = real_time

    def test_a_published_value_far_from_our_twap_is_journalled_not_used(self):
        sn = self.sniper()
        sn._ptb_pending = {T0: {"est": 80_000.0, "raw": 80_000.0, "basis_rounds": 0}}
        real_time = S.time.time
        S.time.time = lambda: T0 + 400.0
        try:
            asyncio.run(sn._ptb_official_once(lambda slug: {"eventMetadata": {"priceToBeat": 8_000.0}}))
        finally:
            S.time.time = real_time
        self.assertEqual(sn.rows[-1]["result"], "ptb_official_suspect")
        self.assertEqual((sn.ptb_calib.basis, len(sn.ptb_calib.seen)), (0.0, 0))

    def test_a_restart_restores_the_checks_from_the_journal(self):
        import json
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = pathlib.Path(d) / "live_sniper.jsonl"
            with open(path, "w") as fh:
                for i in range(10):
                    fh.write(json.dumps({"wall": T0 + 300 * i, "result": "skipped", "reason": "x"}) + "\n")
                    fh.write(json.dumps({"wall": T0 + 300 * i + 60, "result": "ptb_official", "round_start": T0 + 300 * i,
                                         "official": 80_008.0, "raw": 80_000.0, "est": 80_008.5,
                                         "basis_rounds": i}) + "\n")
            sn = S.Sniper(I.Market(T0 + 600), None, settings=I.settings(), journal_path=path)
            self.assertEqual(sn._restore_ptb_checks(), 10)
            newest = S.journal_tail_rows(path, b'"ptb_official"', 4, chunk=64)   # tiny chunks: lines split across reads
            self.assertEqual([r["round_start"] for r in newest], [T0 + 300 * i for i in range(6, 10)])
        self.assertEqual(sn.ptb_calib.basis, 8.0)
        self.assertEqual(sn.ptb_calib.checks, 7)                   # rounds whose basis had 3+ rounds
        self.assertAlmostEqual(sn.ptb_calib.margin_bp(0.0), 0.5 / 80_008.0 * 1e4)

class Decision(F.Gate):
    """Share sizing, as live runs it (the frozen spec), through the real decision and paper fill."""

    def make(self, **env):
        super().make(**{"SNIPER_ORDER_SIZING": "shares", **env})
        # $100, not the harness's $1000: Kelly then wants tens of shares, not the side's whole
        # 200-share cap, so there is room left for an add-on
        self.sn._read_bankroll = lambda: 100.0

    def beat(self, bp: float, *, age_s: float = 0.0, checks: int = 6) -> None:
        """The current round's price to beat (checked on `checks` published rounds, each 0.2bp
        off), and the trigger mid now `bp` past it (UP's way)."""
        self.sn.ptb_rounds[float(self.m.window_end) - S.ROUND_S] = {"est": PTB, "raw": PTB, "basis_rounds": 12,
                                                                      "why": None}
        self.sn.ptb_calib.errors.extend([0.2] * checks)
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
        self.assertEqual((r["dir_add_shares"], r["dir_add_why"]), (0, "this process did not see the round start"))
        self.assertEqual(r["order_shares"], self.base_shares(r))

    def test_a_stale_trigger_mid_adds_nothing(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(+5.0, age_s=S.PTB_MID_MAX_AGE_S + 1)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual((r["ptb_dist_bp"], r["dir_add_why"]), (None, "no fresh trigger price"))
        self.assertEqual(r["order_shares"], self.base_shares(r))

    def test_too_close_to_call_or_not_yet_checked_adds_nothing(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(+0.5)                                            # past it, but inside the 1bp floor
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual((r["dir_add_shares"], r["dir_add_why"]), (0, "within the estimate's error (+-1.00bp)"))
        self.assertEqual(r["order_shares"], self.base_shares(r))

    def test_not_yet_checked_against_polymarket_adds_nothing(self):
        self.make(SNIPER_DIR_ADD="0.5")
        self.pin_time_left()
        self.beat(+5.0, checks=0)
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertEqual(r["result"], "filled", r)
        self.assertEqual(r["dir_add_why"],
                         "estimate checked against Polymarket's published price to beat on 0 of 6 rounds")
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
                    {"SNIPER_PTB_MAX_GAP_S": "0"}, {"SNIPER_PTB_MAX_GAP_S": "61"},
                    {"SNIPER_PTB_MARGIN_BP": "-1"}, {"SNIPER_PTB_MARGIN_BP": "51"},
                    {"SNIPER_PTB_MIN_CHECKS": "0"}, {"SNIPER_PTB_MIN_CHECKS": "25"}, {"SNIPER_PTB_MIN_CHECKS": "2.5"}):
            with self.assertRaises(ValueError, msg=env):
                I.settings(**{**self.ON, **env})

    def test_the_start_line_says_so_only_when_on(self):
        on, off = I.settings(**self.ON), I.settings(SNIPER_FAIR="1")
        self.assertIn("DIRECTION ADD-ON: +50% of the Kelly target", on.lines()[0])
        self.assertIn("DIRECTION ADD-ON (LIVE TEST, SNIPER_DIR_ADD_LIVE)", on.lines(live=True)[0])
        self.assertNotIn("DIRECTION", off.lines()[0] + off.lines(live=True)[0])

    def test_strict_paper_comparison_matches_it_with_live(self):
        for key in ("dir_add", "dir_add_live", "ptb_max_gap_s", "ptb_margin_bp", "ptb_min_checks"):
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

    def test_the_pnl_report_shows_how_exact_the_price_to_beat_was(self):
        import live_pnl
        self.assertEqual(live_pnl.ptb_check_summary([{"result": "skipped"}]), [])
        rows = [{"result": "ptb_official", "err_bp": None, "basis_now": 8.0},
                {"result": "ptb_official", "err_bp": 0.4, "basis_now": 8.1},
                {"result": "ptb_official", "err_bp": 1.2, "basis_now": 8.2},
                {"result": "ptb_official_missing", "metadata_keys": ["finalPrice"]},
                {"result": "ptb_official_suspect"}]
        text = "\n".join(live_pnl.ptb_check_summary(rows))
        self.assertIn("3 published, 1 never published, 1 set aside as implausible", text)
        self.assertIn("estimate error, all 2: avg 0.80bp, largest 1.20bp", text)
        self.assertIn("correction now: Polymarket's value - the trigger feed's TWAP = $+8.20", text)
        self.assertIn("eventMetadata held ['finalPrice']", text)

    def test_the_dashboard_shows_the_price_to_beat(self):
        self.assertEqual(_beat_text({"btc_start": 60_000.0, "mid": 60_030.0}), "  to beat ~60,000 (+5.0bp)")
        self.assertEqual(_beat_text({"btc_start": 60_000.0}), "  to beat ~60,000")
        self.assertEqual(_beat_text({"btc_start": None, "mid": 60_030.0}), "  to beat N/A")


if __name__ == "__main__":
    unittest.main(verbosity=1)
