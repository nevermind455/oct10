#!/usr/bin/env python3
"""Honest paper fills: live's odds of a book fill by its 5s markout (2026-10-29,
the operator's request "make paper honest").

    python tests/tests_fill_odds.py

Live misses the orders whose price runs our way: makers pull their quotes
faster than our copy of the book shows it. Paper filled every order its book
copy filled, so it beat live. Now:
  1  live_fill_odds learns, from the live journal's own orders and shadow
     rows, how often live filled an order the book copy filled, per 5s markout
     bucket - drawing arrivals as paper does (the live delays + extra), never
     from an order's own reply time (misses reply later: it would leak)
  2  the paper broker's fill_gate runs after pricing, before booking: a reason
     refuses the order, the token's slot is free meanwhile and its cost held
  3  the sniper keeps each book fill at those odds after waiting out the 5s
     markout; the side is free for the next order meanwhile, as a live reply
     frees it, and the waiting shares count as Kelly's same-side risk
  4  auto runs without odds when the journal has too few orders (and says so),
     live refuses to start, a wider paper limit than live's is flagged
Temp files and the local paper broker only. No network.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import paper_trade  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_fair as F  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402

FILL = {"sh": 10.0, "px": 0.5, "fee": 0.1}
MISS = {"miss": "FOK no-fill: only $0 available at or below 0.55", "fak": {"miss": "FAK no-fill: nothing"}}


def point(mk5=None, fill=True):
    return {**FILL, "mk5": mk5} if fill else dict(MISS)


def write(path, orders):
    """orders: (wall, id, matched, room, {"L": entry}, reply_ms) -> the live journal."""
    with open(path, "w", encoding="utf-8") as fh:
        for wall, oid, matched, room, grid, reply in orders:
            fh.write(json.dumps({"wall": wall, "id": oid, "live": True, "side": "UP",
                                 "kind": "MATCHED" if matched else "NO_MATCH",
                                 "result": "filled" if matched else "refused", "ask_seen": 0.50,
                                 "cap": round(0.50 + room, 4), "t_wire_to_reply_ms": reply}) + "\n")
            fh.write(json.dumps({"wall": wall + 5.5, "result": "shadow", "id": oid, "L": grid}) + "\n")
    return path


class Learner(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name) / "live_sniper.jsonl"

    def odds(self, rows, **kw):
        write(self.path, rows)
        args = {"delays": [50.0], "extra_ms": 0.0, "max_room": 0.05, **kw}
        return S.live_fill_odds(self.path, **args)

    def test_buckets_are_markout_cents_with_an_unknown_bucket(self):
        got = [S.fill_odds_bucket(x) for x in (None, -5, -3, -0.1, 0, 2.9, 3, 5.9, 6, 9.9, 10, 40)]
        self.assertEqual(got, ["none", "<-3", "<0", "<0", "<3", "<3", "<6", "<6", "<10", "<10", ">=10", ">=10"])

    def test_odds_per_bucket_are_live_fills_of_the_book_fills(self):
        orders = [(1000 + i, f"a{i}", i < 3, 0.05, {"100": point(7.0)}, 250) for i in range(10)]
        orders += [(2000 + i, f"b{i}", True, 0.05, {"100": point(-5.0)}, 250) for i in range(4)]
        o = self.odds(orders)
        self.assertAlmostEqual(o["odds"]["<10"], (3 + 1) / (10 + 2))
        self.assertAlmostEqual(o["odds"]["<-3"], (4 + 1) / (4 + 2))
        self.assertAlmostEqual(o["odds"]["all"], (7 + 1) / (14 + 2))
        self.assertEqual((o["orders"], o["filled"]), (14, 7))
        self.assertEqual((o["first_wall"], o["last_wall"]), (1000, 2003))

    def test_arrivals_are_drawn_from_the_live_delays_not_the_orders_own_reply(self):
        grid = {"100": point(7.0), "300": point(fill=False)}
        quick = [(1000 + i, f"o{i}", True, 0.05, grid, 120) for i in range(4)]
        slow = [(1000 + i, f"o{i}", True, 0.05, grid, 900) for i in range(4)]
        self.assertEqual(self.odds(quick), self.odds(slow))           # the reply time is not read
        self.assertEqual(self.odds(quick)["weight"]["all"], 4.0)        # 50ms -> the 100 point fills
        self.assertNotIn("all", self.odds(quick, delays=[250.0])["weight"])   # 250ms -> the 300 point misses
        half = self.odds(quick, delays=[50.0] * 10 + [250.0] * 10)
        self.assertAlmostEqual(half["weight"]["all"], 2.0)              # half of each order's draws
        self.assertAlmostEqual(self.odds(quick, delays=[50.0], extra_ms=200.0)["weight"].get("all", 0), 0.0)

    def test_an_arrival_past_the_last_point_uses_it_a_gap_is_skipped(self):
        far = [(1000, "o1", True, 0.05, {"100": point(fill=False), "500": point(2.0)}, 250)]
        self.assertEqual(self.odds(far, delays=[1500.0])["weight"]["<3"], 1.0)
        self.assertEqual(self.odds(far, delays=[300.0])["weight"], {})                # 200ms from either point

    def test_only_orders_within_the_paper_limit_and_the_newest(self):
        orders = [(1000 + i, f"o{i}", True, 0.03, {"100": point(1.0)}, 250) for i in range(5)]
        orders.append((900, "wide", False, 0.15, {"100": point(1.0)}, 250))
        o = self.odds(orders)
        self.assertEqual((o["orders"], o["too_wide"], o["widest_room"]), (5, 1, 0.03))
        o = self.odds(orders, max_room=0.15)
        self.assertEqual((o["orders"], o["widest_room"]), (6, 0.15))
        o = self.odds(orders, orders=2)
        self.assertEqual((o["orders"], o["first_wall"]), (2, 1003))

    def test_rows_that_are_not_a_live_venue_answer_are_ignored(self):
        write(self.path, [(1000, "o1", True, 0.05, {"100": point(1.0), "200": {"miss": "unknown book state"}}, 250)])
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({"wall": 1001, "id": "o1", "live": True, "kind": "NO_MATCH", "result": "refused",
                                 "cap": 0.55, "ask_seen": 0.5, "t_wire_to_reply_ms": 1}) + "\n")   # not its first row
            fh.write(json.dumps({"wall": 1002, "id": "p1", "live": False, "kind": "MATCHED", "result": "filled",
                                 "cap": 0.55, "ask_seen": 0.5, "t_wire_to_reply_ms": 1}) + "\n")   # paper
            fh.write(json.dumps({"wall": 1003, "result": "shadow", "id": "p1", "L": {"100": point(1.0)}}) + "\n")
            fh.write('{"result": "shadow", "id": "broken"\n')
        o = S.live_fill_odds(self.path, delays=[150.0], extra_ms=0.0, max_room=0.05)
        self.assertEqual((o["orders"], o["filled"], o["weight"]), (1, 1, {"<3": 1.0, "all": 1.0}))  # 200: unknown

    def test_no_delays_is_an_error(self):
        write(self.path, [])
        with self.assertRaises(ValueError):
            S.live_fill_odds(self.path, delays=[], extra_ms=0.0, max_room=0.05)


class BrokerGate(I.PaperIOCBase):
    """place_trade(fill_gate=...) on the real paper broker."""

    def place(self, gate, amount=4.7, cap=0.52):
        return self.broker.place_trade("UP", amount, I.UP, I.DOWN, self.m.condition_id, self.m.window_end, cap,
                                       None, latency_ms=20, tag="sniper", fak=True, fill_gate=gate)

    def test_a_reason_refuses_after_pricing_and_books_nothing(self):
        seen = []
        cash = self.broker.cash_balance()
        ok = self.place(lambda fill: seen.append(fill) or "FAK no-fill: makers pulled (live fill odds 20%)")
        self.assertFalse(ok)
        self.assertIn("makers pulled", self.broker.last_error)
        self.assertAlmostEqual(seen[0]["shares"], 10.0, places=6)
        self.assertAlmostEqual(seen[0]["average_price"], 0.47, places=6)
        self.assertEqual((self.held(I.UP), self.broker.cash_balance()), (0.0, cash))
        self.assertEqual(self.broker._gate_reserved, 0.0)

    def test_none_books_the_priced_fill(self):
        self.assertTrue(self.place(lambda fill: None))
        self.assertAlmostEqual(self.held(I.UP), 10.0, places=6)

    def test_while_the_gate_runs_the_slot_is_free_and_its_cost_is_held(self):
        out = {}

        def gate(fill):
            out["slot_locked"] = self.broker._execution_locks[(I.UP, "sniper")].locked()
            out["held"] = self.broker._gate_reserved
            self.m.set(I.UP, [(0.47, 100)])
            self.broker.start_balance = 4.7 + fill["total_cost"] - 0.5  # the next order fits only with this one's cost free
            out["second"] = self.place(lambda f: None)
            out["second_error"] = self.broker.last_error
            return None
        self.assertTrue(self.place(gate))
        self.assertFalse(out["slot_locked"])                                # another order on the token may go
        self.assertAlmostEqual(out["held"], 4.7, places=6)
        self.assertFalse(out["second"])                                     # its booking saw this one's cost held
        self.assertIn("paper cash", out["second_error"])

    def test_the_round_cutoff_is_not_checked_again_after_the_gate(self):
        self.broker.min_seconds_to_expiry = 5.0
        late = self.m.window_end - 1.0                                     # past the cutoff when the gate ends
        patcher = mock.patch.object(paper_trade.timer, "unix", lambda: late)

        def gate(fill):
            patcher.start()
            return None
        try:
            self.assertTrue(self.place(gate), self.broker.last_error)
        finally:
            patcher.stop()


class HView:
    """A history-book view (what snapshot_from_book_view and the decision read)."""

    def __init__(self, token, levels):
        self.token = token
        self.asks = tuple((float(p), float(q)) for p, q in levels)
        self.best_ask = self.asks[0][0] if self.asks else None
        self.best_bid = round((self.best_ask or 0.5) - 0.01, 2)
        self.bids = ((self.best_bid, 100.0),)
        self.mid = self.best_ask
        self.status, self.tick_size = "LIVE", 0.01
        self.updated_mono, self.exchange_ts_ms = time.monotonic(), int(time.time() * 1000)


class SniperGate(I.PaperIOCBase):
    """The real Sniper and paper broker, with a book history so orders meet the
    book at their arrival, and live's odds set directly."""

    def setUp(self):
        super().setUp()
        m = self.m
        m.book = type("History", (), {
            "view": staticmethod(lambda token: HView(str(token), m.decision[str(token)])),
            "view_at_mono": staticmethod(lambda token, mono: HView(str(token), m.match[str(token)])),
            "view_at": staticmethod(lambda token, ms: (HView(str(token), m.match[str(token)]), {})),
        })()
        self.sn.SHADOW_MARK_S = 0.05
        self.during = {}
        test = self

        def mid_at(token, venue_ms, mono):
            # the worker, mid-gate: once the loop has run _paper_gate_open, the side is free
            deadline = time.monotonic() + 2.0
            while test.sn.inflight.get("UP") is not None and time.monotonic() < deadline:
                time.sleep(0.005)
            test.during.update(side_free=test.sn.inflight.get("UP") is None,
                               gated=[dict(g) for g in test.sn._paper_gated.values()],
                               slot_locked=test.broker._execution_locks[(I.UP, "sniper")].locked(),
                               thread=threading.current_thread() is not threading.main_thread())
            return test.mid
        self.sn._mid_at = mid_at
        self.mid = 0.60

    def odds(self, p):
        self.sn._fill_odds = {"odds": {b: p for b in ("<-3", "<0", "<3", "<6", "<10", ">=10", "none", "all")}}

    def test_without_odds_orders_fill_on_the_book_as_before(self):
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual(r["result"], "filled")
        self.assertNotIn("fill_odds", r)
        self.assertEqual(self.during, {})

    def test_a_miss_at_live_odds_is_its_own_outcome_and_books_nothing(self):
        self.odds(0.0)
        cash = self.broker.cash_balance()
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r["result"], r["outcome"]), ("refused", "LIVE_ODDS_MISS"))
        self.assertIn("makers pulled (live fill odds 0% at a +13.0c 5s markout)", r["reason"])
        self.assertEqual((r["fill_odds"]["bucket"], r["fill_odds"]["kept"]), (">=10", False))
        self.assertAlmostEqual(r["fill_odds"]["markout_5s_c"], (0.60 - 0.47) * 100, places=3)   # no fee here
        self.assertEqual((self.held(I.UP), self.broker.cash_balance()), (0.0, cash))
        self.assertEqual(self.sn.n["paper book fills missed at live odds"], 1)

    def test_a_kept_fill_books_and_the_side_was_free_while_it_waited(self):
        self.odds(1.0)
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual(r["result"], "filled")
        self.assertTrue(r["fill_odds"]["kept"])
        self.assertAlmostEqual(self.held(I.UP), 10.0, places=6)
        self.assertTrue(self.during["thread"])
        self.assertTrue(self.during["side_free"])                          # the next UP signal could go
        self.assertFalse(self.during["slot_locked"])
        self.assertEqual([g["side"] for g in self.during["gated"]], ["UP"])
        self.assertAlmostEqual(self.during["gated"][0]["risk_shares"], 10.0, places=6)
        self.assertEqual((self.sn.inflight["UP"], self.sn._paper_gated), (None, {}))   # resolved: nothing left
        self.assertEqual(self.sn.n["paper book fills kept at live odds"], 1)

    def test_no_mid_five_seconds_later_uses_the_unknown_odds(self):
        self.mid = None
        self.sn._fill_odds = {"odds": {"none": 0.0, "all": 1.0}}
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r["result"], r["fill_odds"]["bucket"], r["fill_odds"]["markout_5s_c"]),
                         ("refused", "none", None))
        self.assertIn("at a unknown 5s markout", r["reason"])


class SideAndKelly(I.PaperIOCBase):
    def plan(self):
        return {"end": self.m.window_end, "cond": self.m.condition_id, "amount": 4.7, "risk_shares": 10.0}

    def test_gate_open_frees_only_its_own_reservation(self):
        mine = {"end": self.m.window_end, "amount": 4.7, "risk_shares": 10.0}
        plan = {**self.plan(), "inflight": mine}
        self.sn.inflight["UP"] = mine
        self.sn._paper_gate_open("UP", plan, 12.0)
        self.assertIsNone(self.sn.inflight["UP"])
        self.assertEqual(self.sn._paper_gated[id(plan)]["risk_shares"], 12.0)    # the priced shares, if more
        newer = {"end": self.m.window_end, "amount": 1.0, "risk_shares": 2.0}
        other = {**self.plan(), "inflight": {"end": None}}
        self.sn.inflight["UP"] = newer
        self.sn._paper_gate_open("UP", other, 5.0)
        self.assertIs(self.sn.inflight["UP"], newer)                      # a newer order's side stays held
        self.sn._paper_gate_open("UP", other, 5.0)                        # once per order
        self.assertEqual(len(self.sn._paper_gated), 2)

    def test_waiting_shares_are_kellys_same_side_risk(self):
        plan = {**self.plan(), "inflight": None}
        self.sn._paper_gate_open("UP", plan, 10.0)
        own, opposite = self.sn._kelly_inventory(self.m.condition_id, self.m.window_end, "UP", I.UP, I.DOWN)
        self.assertEqual((own, opposite), (10.0, 0.0))
        own, _ = self.sn._kelly_inventory(self.m.condition_id, self.m.window_end, "DOWN", I.DOWN, I.UP)
        self.assertEqual(own, 0.0)

    def test_the_dashboard_counts_waiting_orders_as_reserved_cash(self):
        import run
        plan = {**self.plan(), "inflight": None}
        self.sn._paper_gate_open("UP", plan, 10.0)
        self.sn.inflight["DOWN"] = {"end": self.m.window_end, "amount": 2.0, "risk_shares": 4.0}
        self.assertAlmostEqual(run._reserved_cash(types.SimpleNamespace(sniper=self.sn, pm=None)), 6.7)


class Loading(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name) / "live_sniper.jsonl"
        self.base = I.PaperIOCBase(methodName="setUp")
        self.base.setUp()
        self.addCleanup(self.base.tearDown)

    def sniper(self, **env):
        return S.Sniper(self.base.m, self.base.broker,
                        settings=I.settings(**{"LIVE_SNIPER_JOURNAL": str(self.path), "SNIPER_FILL_ODDS": "auto", **env}))

    def journal(self, n, room=0.05):
        write(self.path, [(1000 + i, f"o{i}", i % 3 == 0, room, {"100": point(4.0)}, 250) for i in range(n)])

    def test_auto_without_a_journal_runs_and_says_paper_is_optimistic(self):
        sn = self.sniper()
        self.assertIsNone(sn._fill_odds)
        self.assertIn("PAPER FILL ODDS OFF", sn.s.fill_odds_note)
        self.assertIn("no live journal", sn.s.fill_odds_note)
        self.assertIn("PAPER FILL ODDS OFF", "\n".join(sn.lines()))

    def test_auto_with_too_few_orders_is_off_live_refuses(self):
        self.journal(S.MIN_FILL_ODDS_ORDERS - 1)
        self.assertIn(f"only {S.MIN_FILL_ODDS_ORDERS - 1} live orders", self.sniper().s.fill_odds_note)
        with self.assertRaises(ValueError):
            self.sniper(SNIPER_FILL_ODDS="live")

    def test_enough_orders_load_the_odds_and_the_settings_line_shows_them(self):
        self.journal(S.MIN_FILL_ODDS_ORDERS)
        sn = self.sniper(SNIPER_FILL_ODDS="live")
        self.assertAlmostEqual(sn._fill_odds["odds"]["<6"], (67 + 1) / (200 + 2))
        line = sn.s.fill_odds_note
        self.assertIn("PAPER FILL ODDS: a book fill is kept at live's odds for its 5s markout (<6 34%", line)
        self.assertIn(f"{S.MIN_FILL_ODDS_ORDERS} live orders", line)
        self.assertNotIn("EXTRAPOLATED", line)
        self.assertIn(line, sn.lines())
        self.assertEqual(sn._match_pool._max_workers, 32)      # a 5s wait never starves the next order

    def test_a_wider_paper_limit_than_lives_is_flagged(self):
        self.journal(S.MIN_FILL_ODDS_ORDERS, room=0.05)
        line = self.sniper(SNIPER_SLIP="0.08").s.fill_odds_note
        self.assertIn("EXTRAPOLATED: live's widest limit there ask+0.05, this paper's ask+0.08", line)

    def test_off_never_reads_the_journal(self):
        self.journal(S.MIN_FILL_ODDS_ORDERS)
        with mock.patch.object(S, "live_fill_odds", side_effect=AssertionError("read")):
            sn = self.sniper(SNIPER_FILL_ODDS="off")
        self.assertEqual((sn._fill_odds, sn.s.fill_odds_note, sn._match_pool), (None, "", None))

    def test_settings_are_checked(self):
        for env in ({"SNIPER_FILL_ODDS": "maybe"}, {"SNIPER_FILL_ODDS_ORDERS": "50"},
                    {"SNIPER_FILL_ODDS_ORDERS": "250.5"}):
            with self.subTest(env=env), self.assertRaises(ValueError):
                I.settings(**env)

    def test_a_failed_refresh_under_live_keeps_the_odds(self):
        self.journal(S.MIN_FILL_ODDS_ORDERS)
        sn = self.sniper(SNIPER_FILL_ODDS="live")
        odds = sn._fill_odds
        os.remove(self.path)
        sn._refresh_fill_odds()
        self.assertIs(sn._fill_odds, odds)
        self.assertIn("[refresh failed, odds kept", sn.s.fill_odds_note)
        self.assertEqual(sn.n["fill odds refresh failed"], 1)


class WithDirectionAddOn(F.Gate):
    """The two paper strategies together (2026-10-30): the direction add-on (SNIPER_DIR_ADD)
    enlarges an order, then live's fill odds decide whether it filled at all. A miss must
    leave no add-on shares behind; a kept fill books them; while it waits, Kelly counts the
    whole order (add-on included) as the side's risk."""

    PTB = 60_000.0

    def setUp(self):
        super().setUp()
        m, test = self.m, self

        def view(token, levels):
            v = HView(str(token), levels)
            v.mid = test.mids.get(str(token), v.mid)
            return v
        m.book = type("History", (), {
            "view": staticmethod(lambda token: view(token, m.decision[str(token)])),
            "view_at_mono": staticmethod(lambda token, mono: view(token, m.decision[str(token)])),
            "view_at": staticmethod(lambda token, ms: (HView(str(token), m.match[str(token)]), {})),
        })()
        self.during = {}

    def make(self, **env):
        super().make(**{"SNIPER_ORDER_SIZING": "shares", "SNIPER_DIR_ADD": "0.5", **env})
        self.sn._read_bankroll = lambda: 100.0          # Kelly wants tens of shares: room for an add-on
        self.sn.SHADOW_MARK_S = 0.05
        test = self

        def mid_at(token, venue_ms, mono):
            deadline = time.monotonic() + 2.0
            while not test.sn._paper_gated and time.monotonic() < deadline:
                time.sleep(0.005)
            cond, end = test.m.condition_id, test.m.window_end
            test.during["own"] = test.sn._kelly_inventory(cond, end, "UP", I.UP, I.DOWN)[0]
            return 0.60
        self.sn._mid_at = mid_at
        self.sn.ptb_calib.errors.extend([0.2] * 6)       # the estimate checked on 6 published rounds

    def odds(self, p):
        self.sn._fill_odds = {"odds": {"all": p}}

    def fire_up(self):
        self.pin_time_left()
        # this round's price to beat, and BTC 5bp past it (UP's way), fresh at the decision
        self.sn.ptb_rounds[float(self.m.window_end) - S.ROUND_S] = {"est": self.PTB, "raw": self.PTB,
                                                                      "basis_rounds": 12, "why": None}
        self.sn.primary_mid = (self.PTB * (1 + 5e-4), time.monotonic())
        r = self.order("UP", [(0.41, 1000)], mid=0.40, move_bp=5.0)
        self.assertGreater(r["dir_add_shares"], 0, r.get("dir_add_why"))
        return r

    def test_a_kept_fill_books_the_add_on_and_kelly_saw_the_whole_order_while_it_waited(self):
        self.make()
        self.odds(1.0)
        r = self.fire_up()
        self.assertEqual(r["result"], "filled", r)
        self.assertGreater(r["dir_add_shares"], 0)
        self.assertTrue(r["fill_odds"]["kept"])
        # the priced shares (a FAK spends shares x cap, so a cheaper fill buys more), add-on included, once
        self.assertAlmostEqual(self.during["own"], r["shares"], places=6)
        self.assertGreater(r["shares"], r["order_shares"])
        held = self.sn._dir_held(self.m.condition_id, I.UP)
        self.assertAlmostEqual(held, r["shares"] * r["dir_add_shares"] / r["order_shares"], places=6)
        self.assertEqual((self.sn.inflight["UP"], self.sn._paper_gated), (None, {}))

    def test_a_miss_at_live_odds_leaves_no_add_on_shares(self):
        self.make()
        self.odds(0.0)
        r = self.fire_up()
        self.assertEqual((r["result"], r["outcome"]), ("refused", "LIVE_ODDS_MISS"))
        self.assertGreater(r["dir_add_shares"], 0)                                  # it was asked for
        self.assertEqual(self.sn._dir_filled, {})                                   # nothing booked
        self.assertEqual(self.held(I.UP), 0.0)
        own, _ = self.sn._kelly_inventory(self.m.condition_id, self.m.window_end, "UP", I.UP, I.DOWN)
        self.assertEqual(own, 0.0)


for _name in [n for n in dir(F.Gate) if n.startswith("test")]:
    setattr(WithDirectionAddOn, _name, None)     # the harness only: its own tests run in tests_sniper_fair.py


if __name__ == "__main__":
    unittest.main()
