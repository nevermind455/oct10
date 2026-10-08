#!/usr/bin/env python3
"""Position Manager: deterministic tests of its accounting, economics, risk,
anti-martingale, expiry and concurrency rules.

    python tests_position_manager.py

Engine cases build portfolios and books by hand; runtime cases use a real
Ledger and, for the concurrency cases, the real PaperBroker (its manager slot
beside the sniper's). No network.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import tempfile
import threading
import time
import types
import unittest
from dataclasses import replace
from decimal import Decimal as D

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import position_manager as PM  # noqa: E402
import tests_paper as TP  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from paper_trade import BookSnapshot, MarketRules  # noqa: E402

FEE = PM.fee_curve(0.07, 1)                      # shares * 0.07 * p(1-p)
CFG = PM.PMConfig(enabled=True)
UP_T, DN_T, COND = "111", "222", "0x" + "ab" * 32


def side(shares=0.0, avg=None, asks=(), bid=None, pending=0.0, pending_usd=0.0):
    cost = shares * avg if avg is not None else 0.0
    best = asks[0][0] if asks else None
    return PM.SideState(shares=shares, cost=cost, fees=0.0, pending_shares=pending, pending_usd=pending_usd,
                        best_bid=bid if bid is not None else (round(best - 0.01, 2) if best else None),
                        best_ask=best, asks=tuple(asks))


def port(up=None, down=None):
    return PM.Portfolio(up or side(), down or side())


def evaluate(s, p, cfg=CFG, left=200.0, hist=None, busy=False, cash=1000.0, now=1000.0, book_ok=True):
    eng = PM.Engine(cfg)
    return eng.evaluate(s, p, fee=FEE, left_s=left, book_ok=book_ok, active=left > 0, hist=hist or PM.MarketHistory(),
                        now_mono=now, sniper_busy=busy, cash_free=cash)


PERMISSIVE = replace(CFG, side_avg_max_added_loss_usd=1000.0, side_avg_min_gain_per_usd=0.0, max_order_usd=1000.0,
                     max_market_usd=1000.0, max_total_exposure_usd=1000.0, max_up_shares=1000.0,
                     max_down_shares=1000.0)


# =========================================================================== position accounting
class Accounting(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.led = Ledger(path=os.path.join(self.tmp.name, "l.json"))
        from tests_sniper_live import live_book
        self.book = live_book()
        self.hub = types.SimpleNamespace(up_token=UP_T, down_token=DN_T, condition_id=COND, window_end=time.time() + 200,
                                         book=self.book)
        rules = MarketRules(COND, D("0.07"), 1, D("5"), D("0.01"))
        self.broker = types.SimpleNamespace(_rules_cache={COND: (rules, time.time())})
        self.pm = PM.PositionManager(CFG, hub=self.hub, ledger=self.led, broker=self.broker)

    def tearDown(self):
        self.pm.journal.close()
        self.tmp.cleanup()

    def fill(self, tid, token, shares, price, fee=0.0):
        self.assertTrue(self.led.record_fill(tid, token, shares=shares, price=price, condition_id=COND, fee=fee))

    def test_up_only_down_only_and_both(self):
        self.fill("a", UP_T, 10, 0.40, fee=0.1)
        p = self.pm.portfolio(COND, UP_T, DN_T)
        self.assertAlmostEqual(p.up.avg, (4.0 + 0.1) / 10)               # fees in the average
        self.assertEqual((p.down.shares, p.paired, p.unpaired_up, p.unpaired_down), (0.0, 0.0, 10.0, 0.0))
        self.fill("b", DN_T, 6, 0.55)
        p = self.pm.portfolio(COND, UP_T, DN_T)
        self.assertEqual((p.paired, p.unpaired_up, p.unpaired_down), (6.0, 4.0, 0.0))
        self.assertAlmostEqual(p.combined_avg, 0.41 + 0.55)
        self.assertAlmostEqual(p.fees, 0.1)

    def test_multiple_prices_partial_fills_and_fees(self):
        self.fill("a", UP_T, 5.435, 0.46, fee=0.0175)          # a partial IOC fill
        self.fill("b", UP_T, 3.2, 0.50, fee=0.0112)
        p = self.pm.portfolio(COND, UP_T, DN_T)
        self.assertAlmostEqual(p.up.shares, 8.635)
        self.assertAlmostEqual(p.up.cost, 5.435 * 0.46 + 3.2 * 0.50 + 0.0175 + 0.0112)
        self.assertAlmostEqual(p.up.avg, p.up.cost / 8.635)

    def test_a_duplicate_fill_message_changes_nothing(self):
        self.fill("a", UP_T, 10, 0.40)
        self.assertFalse(self.led.record_fill("a", UP_T, shares=10, price=0.40, condition_id=COND))
        self.assertEqual(self.pm.portfolio(COND, UP_T, DN_T).up.shares, 10.0)

    def test_requested_quantity_never_enters_the_average(self):
        self.fill("a", UP_T, 10, 0.40)
        o = PM.ManagerOrder(None, "POSITION_MANAGER", "UP", "SIDE_AVG_IMPROVEMENT", 20.0, 6.0, 0.30, 0.32,
                            condition=COND)
        self.pm.orders[1] = o                                          # pending: reserved, not filled
        p = self.pm.portfolio(COND, UP_T, DN_T)
        self.assertEqual((p.up.shares, p.up.pending_shares), (10.0, 20.0))
        self.assertAlmostEqual(p.up.avg, 0.40)                         # filled only
        self.assertAlmostEqual(p.total_cost, 4.0 + 6.0)                 # risk sees filled + pending


# =========================================================================== average improvement
class AverageImprovement(unittest.TestCase):
    def test_clear_improvement(self):
        d = evaluate("DOWN", port(down=side(20, 0.60, asks=((0.40, 100),))), PERMISSIVE)
        self.assertEqual((d.action, d.classification), ("BUY", "SIDE_AVG_IMPROVEMENT"))
        self.assertGreater(d.metrics["avg_improvement"], CFG.min_avg_improvement)
        self.assertLess(d.metrics["projected_avg"], 0.60)

    def test_tiny_improvement(self):
        d = evaluate("UP", port(up=side(30, 0.56, asks=((0.55, 100),))), PERMISSIVE)
        self.assertEqual((d.action, d.reason), ("BLOCKED", "IMPROVEMENT_TOO_SMALL"))

    def test_no_improvement(self):
        d = evaluate("UP", port(up=side(30, 0.50, asks=((0.55, 100),))), PERMISSIVE)
        self.assertEqual((d.action, d.reason), ("BLOCKED", "PRICE_NOT_BETTER"))

    def test_average_improves_but_risk_is_rejected(self):
        # 20 DOWN @ .60 + 20 DOWN @ .20 -> ~.40 average, but a bigger loss if UP wins
        d = evaluate("DOWN", port(down=side(20, 0.60, asks=((0.20, 100),))))
        self.assertEqual((d.action, d.reason, d.classification),
                         ("BLOCKED", "AVG_IMPROVES_BUT_RISK_REJECTED", "AVG_IMPROVES_BUT_RISK_REJECTED"))
        self.assertGreater(d.metrics["avg_improvement"], 0.03)
        self.assertGreater(d.metrics["added_loss"], 0)                 # the risk that improvement costs


# =========================================================================== pair economics
class PairEconomics(unittest.TestCase):
    def test_equal_quantities_create_no_new_pairs(self):
        p = port(up=side(10, 0.45, asks=((0.40, 100),)), down=side(10, 0.45, asks=((0.50, 100),)))
        d = evaluate("UP", p, PERMISSIVE)
        self.assertEqual(d.metrics["new_pairs"], 0.0)
        self.assertNotEqual(d.classification, "PAIR_COST_IMPROVEMENT")

    def test_unequal_quantities_favourable_incremental_pair(self):
        p = port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 100),)))
        d = evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40))
        self.assertEqual((d.action, d.classification), ("BUY", "PAIR_COST_IMPROVEMENT"))
        self.assertAlmostEqual(d.metrics["new_pairs"], 20.0)
        self.assertLess(d.metrics["incremental_pair_cost"], 1.0 - CFG.min_pair_improvement)
        self.assertLessEqual(d.metrics["exposure_after"], d.metrics["exposure_before"])
        self.assertGreater(d.score_parts["locked_pair_profit"], 0)

    def test_new_pairs_only_up_to_the_unpaired_quantity(self):
        p = port(up=side(20, 0.45), down=side(8, 0.40, asks=((0.50, 100),)))
        d = evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40))
        self.assertEqual(d.classification, "PAIR_COST_IMPROVEMENT")
        self.assertLessEqual(d.metrics["new_pairs"], 12.0 + 1e-9)
        self.assertLessEqual(d.size, 12.0 + 1e-9)                      # not past what pairs

    def test_unfavourable_incremental_pair_is_refused(self):
        # UP @ .59 and DOWN asks .73: 1.32 a pair
        d = evaluate("DOWN", port(up=side(20, 0.59), down=side(0, None, asks=((0.73, 100),))))
        self.assertEqual((d.action, d.reason), ("BLOCKED", "PAIR_ECONOMICS_BAD"))
        self.assertGreater(d.metrics["incremental_pair_cost"], 1.0)

    def test_one_side_average_going_down_is_not_pair_improvement(self):
        p = port(up=side(10, 0.70, asks=((0.60, 100),)), down=side(10, 0.45, asks=((0.55, 100),)))
        d = evaluate("UP", p, PERMISSIVE)
        self.assertNotEqual(d.classification, "PAIR_COST_IMPROVEMENT")

    def test_exposure_improvement_needs_its_own_permission(self):
        p = port(up=side(20, 0.50), down=side(0, None, asks=((0.50, 100),)))   # pair cost ~1.0175 after fees
        self.assertEqual(evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40)).reason,
                         "PAIR_ECONOMICS_BAD")
        d = evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40, exposure_max_pair_cost=1.03))
        self.assertEqual((d.action, d.classification), ("BUY", "EXPOSURE_IMPROVEMENT"))


# =========================================================================== liquidity
class Liquidity(unittest.TestCase):
    def test_vwap_over_several_levels(self):
        f = PM.expected_fill(((0.40, 3), (0.41, 3), (0.42, 10)), 10, 0.42, FEE)
        self.assertAlmostEqual(f.qty, 10)
        self.assertAlmostEqual(f.vwap, (0.40 * 3 + 0.41 * 3 + 0.42 * 4) / 10)
        self.assertEqual(f.worst, 0.42)
        self.assertAlmostEqual(f.slippage, f.vwap - 0.40)
        self.assertAlmostEqual(f.fees, sum(FEE(q, p) for p, q in ((0.40, 3), (0.41, 3), (0.42, 4))))

    def test_enough_and_insufficient_depth(self):
        p = port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 100),)))
        self.assertEqual(evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40)).action, "BUY")
        thin = port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 3), (0.60, 100))))
        self.assertEqual(evaluate("DOWN", thin).reason, "INSUFFICIENT_LIQUIDITY")   # 3 within the boundary

    def test_fok_only_sizes_the_book_fills_completely(self):
        # 20 unpaired UP, but only 7 DOWN within the limit: a 20-share FOK would be killed,
        # so the manager asks for a size the book holds in full (5), never more
        p = port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 7), (0.60, 100))))
        d = evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40))
        self.assertEqual((d.action, d.size, d.metrics["order_type"]), ("BUY", 5.0, "FOK"))
        self.assertAlmostEqual(d.fill.qty, 5.0)                          # every share of the order
        self.assertLessEqual(d.stake, 0.50 * 7 + 1e-9)                  # the stake fits the depth in full
        self.assertEqual(d.stake, round(d.stake, 2))                    # whole cents

    def test_excessive_slippage(self):
        p = port(up=side(20, 0.40), down=side(0, None, asks=((0.40, 1), (0.42, 30))))
        d = evaluate("DOWN", p, replace(CFG, max_slippage=0.005, max_order_usd=20, max_market_usd=40))
        self.assertEqual(d.reason, "SLIPPAGE_TOO_HIGH")


# =========================================================================== risk
class Risk(unittest.TestCase):
    P = staticmethod(lambda: port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 100),))))

    def test_max_manager_spend(self):
        hist = PM.MarketHistory(spend=24.0)
        self.assertEqual(evaluate("DOWN", self.P(), hist=hist).reason, "MAX_MANAGER_SPEND")

    def test_max_side_shares(self):
        p = port(up=side(20, 0.45), down=side(58, 0.40, asks=((0.50, 100),)))
        p.up.shares = 70.0
        p.up.cost = 70 * 0.45
        self.assertEqual(evaluate("DOWN", p).reason, "MAX_SIDE_INVENTORY")

    def test_max_total_exposure(self):
        d = evaluate("DOWN", self.P(), replace(CFG, max_total_exposure_usd=10.0))    # 9 held + >= 2.6
        self.assertEqual(d.reason, "MAX_EXPOSURE")

    def test_pending_exposure_counts(self):
        p = self.P()
        p.down.pending_shares, p.down.pending_usd = 0.0, 40.0          # an order in flight elsewhere
        self.assertEqual(evaluate("DOWN", p, replace(CFG, max_total_exposure_usd=50.0)).reason, "MAX_EXPOSURE")

    def test_order_limits(self):
        hist = PM.MarketHistory(orders=6)
        self.assertEqual(evaluate("DOWN", self.P(), hist=hist).reason, "MAX_ORDERS")
        hist = PM.MarketHistory()
        hist.sides["DOWN"].orders = 3
        self.assertEqual(evaluate("DOWN", self.P(), hist=hist).reason, "MAX_ORDERS")

    def test_cash_reserve_for_the_sniper(self):
        self.assertEqual(evaluate("DOWN", self.P(), cash=27.0).reason, "INSUFFICIENT_BALANCE")

    def test_limits_never_enlarge_a_global_maximum(self):
        env = {"POSITION_MANAGER_ENABLED": "1", "POSITION_MANAGER_MAX_UP_SHARES": "999",
               "POSITION_MANAGER_MAX_TOTAL_EXPOSURE": "999"}
        cfg = PM.PMConfig.from_env(env, global_max_buy_price=0.90, global_max_side_shares=200,
                                   global_max_unsettled_usd=50)
        self.assertEqual((cfg.max_up_shares, cfg.max_total_exposure_usd, cfg.max_buy_price), (200.0, 50.0, 0.90))


# =========================================================================== anti-martingale
class AntiMartingale(unittest.TestCase):
    LADDER = (0.50, 0.40, 0.30, 0.20, 0.10)

    def walk(self, cfg):
        """Hold 20 DOWN @ .60; the ask falls .50 .. .10, one step every 3s.
        Buys (filled at the ask) update the position; returns the buy prices."""
        st = side(20, 0.60)
        hist = PM.MarketHistory()
        bought, now = [], 1000.0
        for ask in self.LADDER:
            now += 3.0
            st.asks, st.best_ask, st.best_bid = ((ask, 100),), ask, ask - 0.01
            d = evaluate("DOWN", port(down=st), cfg, hist=hist, now=now)
            if d.action != "BUY":
                continue
            bought.append(ask)
            st.shares += d.fill.qty
            st.cost += d.fill.cost
            hist.orders += 1
            hist.spend += d.fill.cost
            h = hist.sides["DOWN"]
            h.orders, h.last_buy_price, h.last_done_mono = h.orders + 1, d.fill.vwap, now
        return bought

    def test_default_settings_never_average_down_a_falling_book(self):
        self.assertEqual(self.walk(CFG), [])                            # every step: risk rejected

    def test_even_with_risk_allowed_it_never_buys_every_decline(self):
        bought = self.walk(PERMISSIVE)
        self.assertLess(len(bought), len(self.LADDER))
        self.assertLessEqual(len(bought), PERMISSIVE.max_orders_per_side)

    def test_one_book_update_cannot_make_two_orders(self):
        p = port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 100),)))
        hist = PM.MarketHistory()
        d = evaluate("DOWN", p, replace(CFG, max_order_usd=20, max_market_usd=40), hist=hist)
        self.assertEqual(d.action, "BUY")
        hist.sides["DOWN"].in_flight = True                             # what reserve() sets
        self.assertEqual(evaluate("DOWN", p, hist=hist).reason, "ORDER_PENDING")
        hist.sides["DOWN"].in_flight, hist.sides["DOWN"].last_done_mono = False, 999.5
        self.assertEqual(evaluate("DOWN", p, hist=hist, now=1000.0).reason, "COOLDOWN")

    def test_rearm_needs_a_lower_price(self):
        hist = PM.MarketHistory()
        hist.sides["DOWN"].last_buy_price = 0.41
        d = evaluate("DOWN", port(down=side(20, 0.60, asks=((0.40, 100),))), PERMISSIVE, hist=hist)
        self.assertEqual(d.reason, "REARM_REQUIRED")
        d = evaluate("DOWN", port(down=side(20, 0.60, asks=((0.37, 100),))), PERMISSIVE, hist=hist)
        self.assertEqual(d.action, "BUY")


# =========================================================================== expiry
class Expiry(unittest.TestCase):
    P = staticmethod(lambda: port(up=side(20, 0.45), down=side(0, None, asks=((0.505, 100),))))
    C = replace(CFG, max_order_usd=20, max_market_usd=40)

    def test_plenty_approaching_and_too_close(self):
        self.assertEqual(evaluate("DOWN", self.P(), self.C, left=200.0).action, "BUY")    # needs 2c: has ~2.8c
        near = evaluate("DOWN", self.P(), self.C, left=90.0)                             # needs 3c (x1.5)
        self.assertEqual((near.action, near.reason), ("BLOCKED", "PAIR_ECONOMICS_BAD"))
        self.assertEqual(near.metrics["threshold_multiplier"], 1.5)
        self.assertEqual(evaluate("DOWN", self.P(), self.C, left=20.0).reason, "TOO_CLOSE_TO_EXPIRY")


# =========================================================================== gates and priority
class Gates(unittest.TestCase):
    P = staticmethod(lambda: port(up=side(20, 0.45), down=side(0, None, asks=((0.50, 100),))))
    C = replace(CFG, max_order_usd=20, max_market_usd=40)

    def test_stale_book_inactive_market_and_nothing_to_manage(self):
        self.assertEqual(evaluate("DOWN", self.P(), self.C, book_ok=False).reason, "STALE_BOOK")
        self.assertEqual(evaluate("DOWN", self.P(), self.C, left=-1).reason, "MARKET_INACTIVE")
        self.assertEqual(evaluate("UP", port(), self.C).reason, "NOTHING_TO_MANAGE")

    def test_the_sniper_wins_a_simultaneous_candidate(self):
        d = evaluate("DOWN", self.P(), self.C, busy=True)
        self.assertEqual((d.action, d.reason), ("BLOCKED", "SNIPER_PRIORITY"))
        self.assertEqual(d.classification, "PAIR_COST_IMPROVEMENT")    # it was a real opportunity

    def test_every_decision_is_explained(self):
        d = evaluate("DOWN", self.P(), self.C)
        row = d.row()
        for k in ("current_shares", "best_ask", "expected_qty", "expected_vwap", "expected_fees", "projected_avg",
                  "paired_before", "paired_after", "new_pairs", "incremental_pair_cost", "exposure_before",
                  "exposure_after", "classification", "score", "score_parts"):
            self.assertIn(k, row, k)
        self.assertAlmostEqual(sum(row["score_parts"].values()), row["score"], places=4)


# =========================================================================== concurrency on the real paper broker
class Concurrency(unittest.TestCase):
    def broker(self, tmp, book_fetch=None):
        b = TP._broker(tmp, balance=500, context={"condition_id": "cond", "up_token_id": "up", "down_token_id": "down"})
        if book_fetch is not None:
            b._book_fetch = book_fetch
        return b

    def test_a_pending_manager_order_never_blocks_a_sniper_order_on_the_same_token(self):
        with tempfile.TemporaryDirectory() as tmp:
            release, entered = threading.Event(), threading.Event()

            def slow_book(token):
                if threading.current_thread().name == "manager":
                    entered.set()
                    release.wait(5)
                return TP.book(token)
            b = self.broker(tmp, slow_book)
            out = {}
            t = threading.Thread(target=lambda: out.setdefault("pm", b.place_trade(
                "UP", 2, "up", "down", "cond", TP.round_end(), 0.99, None, tag="manager", fak=False)), name="manager")
            t.start()
            self.assertTrue(entered.wait(2))
            ok = b.place_trade("UP", 2, "up", "down", "cond", TP.round_end(), 0.99, None, tag="sniper", fak=True)
            self.assertTrue(ok, b.last_error)                           # the sniper is not refused or held
            release.set()
            t.join(5)
            self.assertTrue(out.get("pm"))
            self.assertEqual(len(b.ledger.positions["up"].lots), 2)     # two orders, two lots

    def test_the_same_source_still_cannot_double_submit(self):
        with tempfile.TemporaryDirectory() as tmp:
            release, entered = threading.Event(), threading.Event()

            def slow_book(token):
                entered.set()
                release.wait(5)
                return TP.book(token)
            b = self.broker(tmp, slow_book)
            t = threading.Thread(target=lambda: b.place_trade("UP", 2, "up", "down", "cond", TP.round_end(), 0.99,
                                                              None, tag="manager", fak=True))
            t.start()
            self.assertTrue(entered.wait(2))
            self.assertFalse(b.place_trade("UP", 2, "up", "down", "cond", TP.round_end(), 0.99, None, tag="manager",
                                           fak=True))
            self.assertIn("already in flight", b.last_error)
            release.set()
            t.join(5)

    def test_reservation_released_on_full_partial_zero_and_reject(self):
        pm = PM.PositionManager(CFG, hub=types.SimpleNamespace(), ledger=Ledger(), broker=types.SimpleNamespace())
        try:
            d = PM.Decision("DOWN", action="BUY", classification="PAIR_COST_IMPROVEMENT", size=10, limit=0.52,
                            fill=PM.Fill(qty=10, notional=5.0, fees=0.09, vwap=0.50, best=0.50),
                            metrics={"capital_max": 5.3})
            for fill, ok, err, status, spent in (
                    ({"order_id": "p1", "shares": 10, "average_price": 0.5, "fee": 0.09, "total_cost": 5.09}, True,
                     None, "FILLED", 5.09),
                    (None, False, "FOK no-fill: only $3.10 available at or below 0.52", "ZERO_FILL", 0.0),
                    (None, False, "spread exceeds paper limit", "REJECTED", 0.0)):
                pm.hist = PM.MarketHistory(condition="c")
                o = pm.reserve(d)
                self.assertTrue(pm.hist.sides["DOWN"].in_flight)
                self.assertAlmostEqual(pm.hist.spend, 5.3)                  # reserved while pending
                pm.settle_order(o, ok=ok, fill=fill, error=err)
                self.assertEqual(o.status, status)
                self.assertFalse(pm.hist.sides["DOWN"].in_flight)
                self.assertAlmostEqual(pm.hist.spend, spent)                # only what was really spent
                self.assertEqual(o.filled_qty, (fill or {}).get("shares", 0))
            self.assertEqual(pm.manager_order_ids, {"p1"})                  # attributable by order id
        finally:
            pm.journal.close()

    def test_manager_partial_fill_while_the_sniper_executes_books_both_by_order_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            b = self.broker(tmp)
            res = {}

            def run(tag, amount):
                ok = b.place_trade("DOWN", amount, "up", "down", "cond", TP.round_end(), 0.99, None, tag=tag, fak=True)
                res[tag] = (ok, b.last_fill)
            threads = [threading.Thread(target=run, args=("sniper", 2)), threading.Thread(target=run, args=("manager", 3))]
            # (run() sends both as FAK; the manager's real executor uses FOK - see test_the_manager_executes_fok)
            for t in threads:
                t.start()
            for t in threads:
                t.join(5)
            ids = {res[k][1]["order_id"] for k in res}
            # a paper fill's ledger trade id IS its order id (live lots carry order_id too)
            lots = {lot.trade_id for lot in b.ledger.positions["down"].lots}
            self.assertEqual(len(ids), 2)
            self.assertEqual(lots, ids)                                 # each fill under its own order id


class FokExecution(unittest.TestCase):
    def test_the_manager_executes_fok(self):
        calls = []

        def place_trade(*a, **k):
            calls.append(k)
            return False
        broker = types.SimpleNamespace(place_trade=place_trade, last_fill=None, last_error="FOK no-fill: thin")
        hub = types.SimpleNamespace(condition_id=COND)
        pm = PM.PositionManager(CFG, hub=hub, ledger=Ledger(), broker=broker)
        try:
            d = PM.Decision("UP", action="BUY", fill=PM.Fill(qty=5, notional=2.0, vwap=0.4), stake=2.0, limit=0.42)
            ok, fill, err = pm._execute_paper(d, COND, UP_T, DN_T, time.time() + 200)
            self.assertEqual(calls[0]["fak"], False)                     # fill-or-kill
            self.assertEqual(calls[0]["tag"], "manager")
            o = pm.reserve(replace(d, classification="PAIR_COST_IMPROVEMENT",
                                   fill=PM.Fill(qty=5, notional=2.0, fees=0.03, vwap=0.4)))
            pm.settle_order(o, ok=ok, fill=fill, error=err)
            self.assertEqual((o.status, o.filled_qty), ("ZERO_FILL", 0.0))  # killed: nothing bought
            self.assertIn("Execution:    FOK", "\n".join(CFG.lines(live_mode=False)))
        finally:
            pm.journal.close()

    def test_a_fok_that_filled_at_a_worse_price_is_filled_not_partial(self):
        broker = types.SimpleNamespace(place_trade=lambda *a, **k: True, last_error=None,
                                       last_fill=None)
        hub = types.SimpleNamespace(condition_id=COND)
        pm = PM.PositionManager(CFG, hub=hub, ledger=Ledger(), broker=broker)
        try:
            d = PM.Decision("UP", action="BUY", classification="PAIR_COST_IMPROVEMENT",
                            fill=PM.Fill(qty=5, notional=2.0, fees=0.03, vwap=0.4), stake=2.0, limit=0.42)
            o = pm.reserve(d)
            # the whole $2.00 executed, but the book had moved to 0.41: 4.88 shares
            pm.settle_order(o, ok=True, fill={"order_id": "f1", "shares": 4.878, "average_price": 0.41,
                                              "fee": 0.03, "total_cost": 2.03}, error=None)
            self.assertEqual(o.status, "FILLED")
            self.assertAlmostEqual(o.filled_qty, 4.878)
        finally:
            pm.journal.close()

    def test_the_real_paper_broker_kills_a_manager_fok_it_cannot_fill(self):
        with tempfile.TemporaryDirectory() as tmp:
            b = TP._broker(tmp, balance=500,
                           context={"condition_id": "cond", "up_token_id": "up", "down_token_id": "down"})
            # the book holds $1.20 at 0.40 and $5.00 at 0.50: $3.00 capped at 0.45 cannot fill in full
            ok = b.place_trade("UP", 3, "up", "down", "cond", TP.round_end(), 0.45, None, tag="manager", fak=False)
            self.assertFalse(ok)
            self.assertIn("FOK no-fill", b.last_error)
            self.assertNotIn("up", b.ledger.positions)                    # nothing bought


class LiveGuard(unittest.TestCase):
    def test_live_mode_never_executes_an_order(self):
        pm = PM.PositionManager(CFG, hub=types.SimpleNamespace(), ledger=Ledger(), broker=types.SimpleNamespace(
            place_trade=lambda *a, **k: self.fail("a live-mode manager reached the broker")), live_mode=True)
        try:
            self.assertTrue(pm.shadow)
            d = PM.Decision("UP", action="BUY", fill=PM.Fill(qty=5, notional=2.0, vwap=0.4), stake=2.0, limit=0.42)
            with self.assertRaises(RuntimeError):
                pm._execute_paper(d, COND, UP_T, DN_T, time.time() + 200)
        finally:
            pm.journal.close()

    def test_live_with_manager_orders_refuses_to_start(self):
        import run
        env = {"SNIPER_ONLY": "1", "SNIPER_LIVE_CANARY": "1", "POSITION_MANAGER_LIVE_ENABLED": "1"}
        from unittest.mock import patch as _patch
        with _patch.dict(os.environ, env, clear=True), self.assertRaises(RuntimeError) as ctx:
            run._live_sniper_gate(paper=False)
        self.assertIn("POSITION_MANAGER_LIVE_ENABLED", str(ctx.exception))


class Startup(unittest.TestCase):
    def test_off_by_default_and_live_needs_its_own_switch(self):
        cfg = PM.PMConfig.from_env({})
        self.assertEqual((cfg.enabled, cfg.live_enabled), (False, False))
        lines = "\n".join(PM.PMConfig.from_env({"POSITION_MANAGER_ENABLED": "1"}).lines(live_mode=True))
        self.assertIn("Engine:       ENABLED", lines)
        self.assertIn("Live Orders:  DISABLED", lines)                 # never inferred from a live sniper

    def test_bad_settings_are_named(self):
        with self.assertRaises(ValueError) as ctx:
            PM.PMConfig.from_env({"POSITION_MANAGER_MAX_ORDER_USD": "x", "POSITION_MANAGER_EXPIRY_TIERS": "a:b"})
        self.assertIn("POSITION_MANAGER_MAX_ORDER_USD", str(ctx.exception))
        self.assertIn("POSITION_MANAGER_EXPIRY_TIERS", str(ctx.exception))


# =========================================================================== audit fixes
class FakeBook:
    """hub.book for runtime cases: views built from levels set per token."""

    def __init__(self, mono=time.monotonic):
        self.v, self.mono, self.ver = {}, mono, 0

    def set(self, token, asks, bid=None, status="LIVE"):
        self.ver += 1
        self.v[str(token)] = (tuple(asks), bid, status, self.mono() + self.ver * 1e-9)   # when it was updated

    def view(self, token):
        asks, bid, status, upd = self.v.get(str(token), ((), None, "UNSYNCED", None))
        return types.SimpleNamespace(asks=asks, best_ask=asks[0][0] if asks else None,
                                     best_bid=bid if bid is not None else (round(asks[0][0] - 0.01, 2) if asks else None),
                                     status=status, updated_mono=upd)


class World:
    """A PositionManager over a real Ledger, fake books and any broker."""
    RULES = MarketRules(COND, D("0.07"), 1, D("5"), D("0.01"))

    def __init__(self, cfg=CFG, broker=None, sniper=None, left=250.0, live_mode=False):
        self.tmp = tempfile.TemporaryDirectory()
        self.led = Ledger(path=os.path.join(self.tmp.name, "l.json"))
        self.book = FakeBook()
        self.hub = types.SimpleNamespace(up_token=UP_T, down_token=DN_T, condition_id=COND,
                                         window_end=time.time() + left, book=self.book)
        rules_b = MarketRules(COND_B, D("0.07"), 1, D("5"), D("0.01"))
        self.broker = broker or types.SimpleNamespace()
        self.broker._rules_cache = {COND: (self.RULES, time.time()), COND_B: (rules_b, time.time())}
        if not hasattr(self.broker, "cash_balance"):
            self.broker.cash_balance = lambda: 1000.0
        self.pm = PM.PositionManager(cfg, hub=self.hub, ledger=self.led, broker=self.broker, sniper=sniper,
                                     live_mode=live_mode)

    def fill(self, tid, token, shares, price, cond=COND, fee=0.0):
        assert self.led.record_fill(tid, token, shares=shares, price=price, condition_id=cond, fee=fee)

    def close(self):
        self.pm.journal.close()
        self.tmp.cleanup()


COND_B, UP_B, DN_B = "0x" + "cd" * 32, "333", "444"


def fake_sniper(target=20.0, slip=0.05):
    """The sniper's state as the manager reads it (settings + in-flight + last send)."""
    return types.SimpleNamespace(s=types.SimpleNamespace(max_target_shares=target, shares=10.0, slip=slip,
                                                         order_type="FAK", stale_ms=2000.0),
                                 inflight={"UP": None, "DOWN": None},
                                 last_fire_by_side={"UP": -1e18, "DOWN": -1e18})


class BadPairsThroughSideAvg(unittest.TestCase):
    """FIX 1: no classification bypasses the economics of the pairs a buy creates."""

    def test_the_audit_reproduction_is_rejected(self):
        # UP 20 @0.59, DOWN 10 @0.85, DOWN ask 0.73: a lower DOWN average, new pairs at ~1.33
        p = port(up=side(20, 0.59), down=side(10, 0.85, asks=((0.73, 100),)))
        d = evaluate("DOWN", p)
        self.assertNotEqual(d.action, "BUY")
        self.assertEqual(d.reason, "PAIR_ECONOMICS_BAD")
        for q in (5.0, 10.0):
            c = PM.Engine(CFG)._candidate("DOWN", p, p.down, p.up, q, 0.75, FEE, 0.03, 0.02, PM.MarketHistory(), 1e3)
            self.assertGreater(c.metrics["avg_improvement"], 0.03)          # the average does improve ...
            self.assertGreater(c.metrics["incremental_pair_cost"], 1.3)       # ... at ~1.33 a pair
            self.assertEqual((c.action, c.reason), ("BLOCKED", "PAIR_ECONOMICS_BAD"))

    def test_fractional_new_pairs_are_checked_too(self):
        # paired 20 -> 20.5: half a new pair, still at ~1.33 - previously below min_new_pairs, unchecked
        p = port(up=side(20.5, 0.59), down=side(20, 0.85, asks=((0.73, 100),)))
        c = PM.Engine(PERMISSIVE)._candidate("DOWN", p, p.down, p.up, 5.0, 0.75, FEE, 0.03, 0.02, PM.MarketHistory(), 1e3)
        self.assertAlmostEqual(c.metrics["new_pairs"], 0.5)
        self.assertEqual(c.reason, "PAIR_ECONOMICS_BAD")

    def test_the_threshold_is_the_configured_one_with_vwap_and_fees(self):
        # new pairs at 0.40 VWAP + fees (0.0168) + UP 0.57 = 0.9868: over 1 - 0.02, under $1
        p = port(up=side(20, 0.57), down=side(10, 0.50, asks=((0.40, 100),)))
        c = PM.Engine(CFG)._candidate("DOWN", p, p.down, p.up, 10.0, 0.42, FEE, 0.03, 0.02, PM.MarketHistory(), 1e3)
        self.assertGreater(c.metrics["incremental_pair_cost"], 0.98)
        self.assertLess(c.metrics["incremental_pair_cost"], 1.0)
        self.assertEqual(c.reason, "PAIR_ECONOMICS_BAD")                    # not the hard-coded $1
        loose = PM.Engine(replace(CFG, min_pair_improvement=0.0))._candidate(
            "DOWN", p, p.down, p.up, 10.0, 0.42, FEE, 0.03, 0.0, PM.MarketHistory(), 1e3)
        self.assertNotEqual(loose.reason, "PAIR_ECONOMICS_BAD")             # a looser configured threshold admits it

    def test_good_pairs_and_the_opt_in_hedge_still_pass(self):
        good = evaluate("DOWN", port(up=side(20, 0.45), down=side(0, None, asks=((0.49, 100),))))
        self.assertEqual((good.action, good.classification), ("BUY", "PAIR_COST_IMPROVEMENT"))
        hedge_cfg = replace(PERMISSIVE, exposure_max_pair_cost=1.40, exposure_min_loss_cut_usd=0.5)
        p = port(up=side(20, 0.59), down=side(10, 0.85, asks=((0.73, 100),)))
        d = evaluate("DOWN", p, hedge_cfg)
        self.assertEqual(d.action, "BUY")                                   # the operator allowed that hedge cost

    def test_a_buy_that_creates_no_pairs_is_unaffected(self):
        p = port(down=side(20, 0.60, asks=((0.20, 100),)))                  # audit #7: only DOWN held
        self.assertEqual(evaluate("DOWN", p).reason, "AVG_IMPROVES_BUT_RISK_REJECTED")


class SniperLiquidity(unittest.TestCase):
    """FIX 2: the manager yields the sniper's liquidity and its instant."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def paper(self, levels):
        """The real PaperBroker over a fixed UP book (production own-fill memory: 2s)."""
        snap = BookSnapshot("up", tuple((D(str(p)), D(str(q))) for p, q in levels), tick_size=D("0.01"),
                            timestamp=str(int(time.time() * 1000)), book_hash="h", received_wall=time.time(),
                            best_bid=D("0.49"))
        b = TP._broker(self.tmp.name, balance=1000, selected_book=snap,
                       context={"condition_id": "cond", "up_token_id": "up", "down_token_id": "down"})
        b.own_fill_ttl_s = 2.0
        return b

    def manager_decides(self, levels, sniper):
        """The manager's decision for UP on that book: 10 DOWN @0.40 to pair."""
        w = World(cfg=CFG, sniper=sniper)
        w.fill("d", DN_T, 10, 0.40)
        w.book.set(UP_T, levels)
        w.book.set(DN_T, [(0.61, 100)])
        dec = w.pm.evaluate()
        return w, dec["UP"]

    def sniper_order(self, b, target=10.0):
        ask = float(b._net_of_own_fills(b._book_fetch("up")).asks[0][0])
        import sniper_mode
        cap = round(ask + 0.05, 4)
        levels = [(float(p), float(q)) for p, q in b._net_of_own_fills(b._book_fetch("up")).asks]
        amount = max(sniper_mode.Sniper.ioc_spend(levels, target, ask, cap), 1.0)
        ok = b.place_trade("UP", amount, "up", "down", "cond", TP.round_end(), cap, None, tag="sniper", fak=True)
        return ok, (b.last_fill or {}).get("shares", 0.0)

    def test_reserve_rule(self):
        sn = fake_sniper()
        thin = evaluate("UP", port(down=side(10, 0.40), up=side(0, None, asks=((0.50, 10), (0.60, 100)))), CFG)
        self.assertEqual(thin.action, "BUY")                                # without the sniper attached ...
        w, d = self.manager_decides([(0.50, 10), (0.60, 100)], sn)
        self.assertEqual(d.reason, "SNIPER_LIQUIDITY_RESERVE")              # ... with it: yields the 10 shares
        w.close()
        w, d = self.manager_decides([(0.50, 40), (0.60, 100)], sn)
        self.assertEqual(d.action, "BUY")                                   # 40 - 10 leaves the sniper's 20
        self.assertGreaterEqual(d.metrics["sniper_depth"] - d.fill.qty, 20.0)
        w.close()

    def test_manager_qualifies_first_then_the_sniper_fires(self):
        # thin book: the manager no longer takes the 10 shares; the sniper gets all of them
        w, d = self.manager_decides([(0.50, 10), (0.60, 100)], fake_sniper())
        self.assertNotEqual(d.action, "BUY")
        b = self.paper([(0.50, 10), (0.60, 100)])
        ok, got = self.sniper_order(b)
        self.assertTrue(ok)
        self.assertAlmostEqual(got, 10.0)
        w.close()
        # deep book: the manager buys first, the sniper still fills its whole target
        w, d = self.manager_decides([(0.50, 40), (0.60, 100)], fake_sniper())
        self.assertEqual(d.action, "BUY")
        b = self.paper([(0.50, 40), (0.60, 100)])
        self.assertTrue(b.place_trade("UP", d.stake, "up", "down", "cond", TP.round_end(), d.limit, None,
                                      tag="manager", fak=False))
        ok, got = self.sniper_order(b, target=20.0)
        self.assertTrue(ok)
        self.assertAlmostEqual(got, 20.0)
        w.close()

    def test_sniper_qualifies_first_then_the_manager(self):
        sn = fake_sniper()
        sn.inflight["UP"] = {"end": 1}
        w, d = self.manager_decides([(0.50, 40), (0.60, 100)], sn)
        self.assertEqual(d.reason, "SNIPER_PRIORITY")
        w.close()

    def test_simultaneous_eligibility_the_send_rechecks(self):
        sn = fake_sniper()
        w, d = self.manager_decides([(0.50, 40), (0.60, 100)], sn)
        self.assertEqual(d.action, "BUY")                                   # decided ...
        sn.last_fire_by_side["UP"] = time.monotonic() * 1000.0              # ... the sniper fires before the send

        async def go():
            return w.pm._dispatch(d, asyncio.get_running_loop())
        self.assertIsNone(asyncio.run(go()))
        self.assertEqual(d.reason, "SNIPER_PRIORITY")
        self.assertEqual(w.pm.orders, {})                                   # nothing reserved, nothing leaked
        self.assertFalse(w.pm.hist.sides["UP"].in_flight)
        w.close()

    def test_manager_order_preparing_then_the_sniper_fires(self):
        sn = fake_sniper()
        calls = []
        broker = types.SimpleNamespace(place_trade=lambda *a, **k: calls.append(k) or True, last_fill=None,
                                       last_error=None)
        w = World(cfg=CFG, sniper=sn, broker=broker)
        w.fill("d", DN_T, 10, 0.40)
        w.book.set(UP_T, [(0.50, 40), (0.60, 100)])
        w.book.set(DN_T, [(0.61, 100)])
        d = w.pm.choose(w.pm.evaluate())
        o = w.pm.reserve(d)                                                 # reserved, handed to the worker ...
        sn.inflight["UP"] = {"end": 1}                                      # ... the sniper fires
        ok, fill, err = w.pm._execute_paper(d, COND, UP_T, DN_T, w.hub.window_end, o=o)
        w.pm._finish(o)
        self.assertEqual(calls, [])                                         # nothing was sent
        self.assertEqual((o.status, o.filled_qty), ("CANCELLED", 0.0))
        self.assertFalse(w.pm.hist.sides["UP"].in_flight)
        self.assertAlmostEqual(w.pm.hist.spend, 0.0)
        sn.inflight["UP"] = None
        sn.last_fire_by_side["UP"] = -1e18
        self.assertEqual(w.pm.portfolio(COND, UP_T, DN_T).up.pending_shares, 0.0)
        w.close()

    def test_manager_order_already_dispatched_then_the_sniper_fires(self):
        # Once sent it cannot be recalled (a FOK executes or dies on arrival); the
        # reserve is what keeps the sniper whole in a book that holds still.
        w, d = self.manager_decides([(0.50, 30), (0.60, 100)], fake_sniper())
        self.assertEqual(d.action, "BUY")
        b = self.paper([(0.50, 30), (0.60, 100)])
        self.assertTrue(b.place_trade("UP", d.stake, "up", "down", "cond", TP.round_end(), d.limit, None,
                                      tag="manager", fak=False))            # lands first
        ok, got = self.sniper_order(b, target=20.0)
        self.assertEqual((ok, got), (True, 20.0))
        w.close()
        # NOT preventable: another trader takes the reserve after the decision - the
        # venue reserves nothing. The sniper then gets what is left, no more.
        b = self.paper([(0.50, 15), (0.60, 100)])
        self.assertTrue(b.place_trade("UP", d.stake, "up", "down", "cond", TP.round_end(), d.limit, None,
                                      tag="manager", fak=False))
        ok, got = self.sniper_order(b, target=20.0)
        self.assertTrue(ok)
        self.assertLess(got, 20.0)


class MarketTransition(unittest.TestCase):
    """FIX 3: an order settles into ITS market's history."""

    def test_a_late_market_a_result_leaves_market_b_alone(self):
        w = World()
        w.fill("a1", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        dA = w.pm.choose(w.pm.evaluate())
        oA = w.pm.reserve(dA)
        self.assertEqual((oA.condition_id, oA.token_id, oA.side, oA.reservation_id, oA.source),
                         (COND, DN_T, "DOWN", 1, "POSITION_MANAGER"))
        self.assertTrue(oA.market_id.startswith(COND + "@"))
        # A expires, B begins, B creates a DOWN pending order
        w.hub.up_token, w.hub.down_token, w.hub.condition_id = UP_B, DN_B, COND_B
        w.hub.window_end = time.time() + 280
        w.book.set(UP_B, [(0.45, 100)])
        w.book.set(DN_B, [(0.50, 100)])
        w.fill("b1", UP_B, 40, 0.40, cond=COND_B)
        dB = w.pm.choose(w.pm.evaluate())
        oB = w.pm.reserve(dB)
        hB = w.pm.hist
        before = (hB.spend, hB.orders, hB.sides["DOWN"].in_flight, hB.sides["DOWN"].orders,
                  hB.sides["DOWN"].last_buy_price, w.pm.portfolio(COND_B, UP_B, DN_B).down.pending_shares,
                  w.pm.portfolio(COND_B, UP_B, DN_B).down.shares)
        # the old A response arrives (it filled)
        w.fill("manager-a", DN_T, dA.fill.qty, dA.fill.vwap, fee=dA.fill.fees)
        w.pm.settle_order(oA, ok=True, fill={"order_id": "manager-a", "shares": dA.fill.qty,
                                             "average_price": dA.fill.vwap, "fee": dA.fill.fees,
                                             "total_cost": dA.fill.cost}, error=None)
        after = (hB.spend, hB.orders, hB.sides["DOWN"].in_flight, hB.sides["DOWN"].orders,
                 hB.sides["DOWN"].last_buy_price, w.pm.portfolio(COND_B, UP_B, DN_B).down.pending_shares,
                 w.pm.portfolio(COND_B, UP_B, DN_B).down.shares)
        self.assertEqual(before, after)                                     # B's spend, pending, inventory, counts
        hA = w.pm.histories[COND]
        self.assertFalse(hA.sides["DOWN"].in_flight)                        # A's own history took it
        self.assertAlmostEqual(hA.spend, dA.fill.cost)
        self.assertEqual(w.pm.evaluate()["DOWN"].reason, "ORDER_PENDING")   # no duplicate B order
        self.assertEqual(oB.status, "PENDING")
        w.close()


class SlowBroker:
    """A paper-like broker whose call returns when the test says: books the
    fill into the ledger right after the pre-submit guard, as PaperBroker does."""

    def __init__(self, ledger, *, shares, price, partial=False, delay=None, guard=True):
        self.ledger, self.shares, self.price, self.partial = ledger, shares, price, partial
        self.release, self.delay, self.use_guard = threading.Event(), delay, guard
        self.calls, self._t = 0, threading.local()

    last_fill = property(lambda self: getattr(self._t, "fill", None))
    last_error = property(lambda self: getattr(self._t, "err", None))

    def cash_balance(self):
        return 1000.0

    def place_trade(self, side, amount, up, down, cond, end, limit, _min, *, latency_ms=None, tag=None, fak=False,
                    pre_submit_guard=None):
        self.calls += 1
        if self.delay is not None:
            time.sleep(self.delay)
        else:
            self.release.wait(10)
        self._t.fill, self._t.err = None, None
        if self.use_guard and pre_submit_guard is not None and not pre_submit_guard():
            self._t.err = "pre-submit guard rejected order"
            return False
        if not self.shares:
            self._t.err = "FOK no-fill: nothing"
            return False
        oid = f"slow-{self.calls}"
        token = up if side == "UP" else down
        self.ledger.record_fill(oid, token, shares=self.shares, price=self.price, condition_id=cond, fee=0.0)
        self._t.fill = {"order_id": oid, "shares": self.shares, "average_price": self.price, "fee": 0.0,
                        "total_cost": self.shares * self.price, "partial": self.partial, "tag": tag}
        return True


class Timeouts(unittest.TestCase):
    """FIX 4: no outcome by order_timeout_s -> UNKNOWN, reservation kept, no retry."""
    # a long cooldown: after the outcome lands, no follow-up order inside the test's window
    CFG_T = replace(CFG, poll_ms=5.0, order_timeout_s=0.15, cooldown_ms=60_000.0)

    def run_case(self, broker_kw, *, wait_s, release_at=None, switch_market_at=None):
        w = World(cfg=self.CFG_T)
        broker = SlowBroker(w.led, **broker_kw)
        broker._rules_cache = w.broker._rules_cache
        w.pm.broker = broker
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        seen = {}

        async def go():
            w.pm.start()
            t0 = time.monotonic()
            while time.monotonic() - t0 < wait_s:
                await asyncio.sleep(0.01)
                el = time.monotonic() - t0
                o = next(iter(w.pm.orders.values()), None)
                if o is not None and o.status == "UNKNOWN" and "unknown" not in seen:
                    p = w.pm.portfolio(COND, UP_T, DN_T)
                    seen["unknown"] = (p.down.pending_shares if p else None, w.pm.hist.sides["DOWN"].in_flight)
                if switch_market_at is not None and el >= switch_market_at and "switched" not in seen:
                    seen["switched"] = True
                    w.hub.up_token, w.hub.down_token, w.hub.condition_id = UP_B, DN_B, COND_B
                    w.book.set(UP_B, [(0.45, 100)])
                    w.book.set(DN_B, [(0.50, 100)])
                if release_at is not None and el >= release_at:
                    broker.release.set()
            await asyncio.sleep(0.05)
            await w.pm.stop()
            broker.release.set()                                            # never-returning calls end here
        asyncio.run(go())
        return w, broker, seen

    def test_a_request_that_never_returns_is_unknown_and_keeps_its_reservation(self):
        w, broker, seen = self.run_case(dict(shares=10, price=0.50), wait_s=0.6)
        o = w.pm.orders[1]
        self.assertIn("unknown", seen)
        self.assertEqual(seen["unknown"], (o.requested_qty, True))           # still counted, side still in flight
        self.assertEqual(broker.calls, 1)                                   # never retried
        self.assertEqual(len(w.pm.orders), 1)
        w.close()

    def test_an_ack_before_the_timeout_is_ordinary(self):
        w, broker, seen = self.run_case(dict(shares=10, price=0.50, delay=0.02), wait_s=0.4)
        o = w.pm.orders[1]
        self.assertNotIn("unknown", seen)
        self.assertEqual(o.status, "FILLED")
        w.close()

    def test_an_ack_after_the_timeout_settles_the_unknown_order_once(self):
        w, broker, seen = self.run_case(dict(shares=10, price=0.50), wait_s=0.6, release_at=0.35)
        o = w.pm.orders[1]
        self.assertIn("unknown", seen)
        self.assertEqual(o.status, "FILLED")
        p = w.pm.portfolio(COND, UP_T, DN_T)
        self.assertEqual((p.down.shares, p.down.pending_shares), (10.0, 0.0))   # once: filled, nothing pending
        self.assertAlmostEqual(w.pm.histories[COND].spend, 5.0)
        w.close()

    def test_partial_fill_before_and_after_the_timeout(self):
        for kw in (dict(delay=0.02), dict(release_at=0.35)):
            release_at = kw.pop("release_at", None)
            w, broker, seen = self.run_case(dict(shares=4.25, price=0.50, partial=True, **kw), wait_s=0.6,
                                            release_at=release_at)
            o = w.pm.orders[1]
            self.assertEqual((o.status, o.filled_qty), ("PARTIAL", 4.25))
            p = w.pm.portfolio(COND, UP_T, DN_T)
            self.assertEqual((p.down.shares, p.down.pending_shares), (4.25, 0.0))
            self.assertEqual("unknown" in seen, release_at is not None)
            w.close()

    def test_the_market_changes_while_an_order_is_unknown(self):
        # the order already reached the venue (no broker-side guard): it fills late, in A
        w, broker, seen = self.run_case(dict(shares=10, price=0.50, guard=False), wait_s=0.7,
                                        switch_market_at=0.25, release_at=0.45)
        o = w.pm.orders[1]
        self.assertIn("unknown", seen)
        self.assertEqual(o.status, "FILLED")
        self.assertEqual(o.condition_id, COND)
        self.assertFalse(w.pm.histories[COND].sides["DOWN"].in_flight)       # A's history settled
        self.assertAlmostEqual(w.pm.histories[COND_B].spend, 0.0)             # B untouched
        pB = w.pm.portfolio(COND_B, UP_B, DN_B)
        self.assertEqual((pB.down.shares, pB.down.pending_shares), (0.0, 0.0))
        w.close()


class BookingWindow(unittest.TestCase):
    """FIX 5: filled + still-pending is never counted twice."""

    def run_booking(self, shares, partial):
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        d = w.pm.choose(w.pm.evaluate())
        o = w.pm.reserve(d)
        seen = {}

        def place_trade(side, amount, up, down, cond, end, limit, _m, *, pre_submit_guard=None, **_k):
            self.assertTrue(pre_submit_guard())
            seen["guard"] = w.pm.portfolio(COND, UP_T, DN_T)             # booking under way: not readable
            w.led.record_fill("p1", DN_T, shares=shares, price=0.50, condition_id=COND, fee=0.0)
            seen["booked"] = w.pm.portfolio(COND, UP_T, DN_T)            # in the ledger, outcome not on the order yet
            broker.last_fill = {"order_id": "p1", "shares": shares, "average_price": 0.5, "fee": 0.0,
                                "total_cost": shares * 0.5, "partial": partial}
            return True
        broker = w.pm.broker
        broker.place_trade, broker.last_error = place_trade, None
        w.pm._execute_paper(d, COND, UP_T, DN_T, w.hub.window_end, o=o)
        after = w.pm.portfolio(COND, UP_T, DN_T)                          # the loop's _finish has NOT run yet
        w.close()
        return d, seen, after

    def test_full_and_partial_fills_under_a_delayed_callback(self):
        for shares, partial in ((None, False), (4.25, True)):
            with self.subTest(partial=partial):
                d, seen, after = self.run_booking(shares or 10.0, partial)
                got = shares or 10.0
                self.assertIsNone(seen["guard"])
                self.assertIsNone(seen["booked"])
                self.assertEqual((after.down.shares, after.down.pending_shares), (got, 0.0))

    def test_a_booking_that_starts_while_the_ledger_is_read_voids_the_read(self):
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        o = w.pm.reserve(w.pm.choose(w.pm.evaluate()))

        class Trap(dict):
            # mid-read, whichever way portfolio() reads the positions (values() or get())
            def _hit(inner):
                with w.pm._olock:                       # what the pre-submit guard does, mid-read
                    o.booking = True
                    w.pm._epoch += 1

            def values(inner):
                inner._hit()
                return dict.values(inner)

            def get(inner, *a):
                inner._hit()
                return dict.get(inner, *a)
        w.led.positions = Trap(w.led.positions)
        self.assertIsNone(w.pm.portfolio(COND, UP_T, DN_T))
        self.assertEqual(w.pm.busy_reason, "BOOKING_IN_PROGRESS")
        w.close()


class EventLoopSlices(unittest.TestCase):
    """FIX 6: an evaluation holds the loop for one candidate at a time, never acts on stale state."""

    def test_steps_yield_between_candidates_and_match_the_whole_evaluation(self):
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        steps, n = w.pm.evaluate_steps(), 0
        while True:
            try:
                next(steps)
                n += 1
            except StopIteration as stop:
                sliced = stop.value
                break
        whole = w.pm.evaluate()
        self.assertGreater(n, 4)                                            # the loop gets control back n times
        self.assertEqual({s: (d.action, d.reason, d.size) for s, d in sliced.items()},
                         {s: (d.action, d.reason, d.size) for s, d in whole.items()})
        w.close()

    def test_a_state_change_between_snapshot_and_send_blocks_the_send(self):
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        d = w.pm.choose(w.pm.evaluate())
        w.book.set(DN_T, [(0.50, 3), (0.70, 100)])                          # the book moved after the snapshot

        async def go():
            return w.pm._dispatch(d, asyncio.get_running_loop())
        self.assertIsNone(asyncio.run(go()))
        self.assertEqual(d.reason, "STATE_CHANGED")
        self.assertEqual(w.pm.orders, {})
        w.close()


    def test_a_fill_booked_while_the_state_is_read_blocks_the_send(self):
        # the sniper's worker books a fill DURING the manager's ledger read: the version
        # was taken before the read, so the send sees the change and drops the decision
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        booked = []

        class Trap(dict):
            # mid-read, whichever way portfolio() reads the positions (values() or get())
            def _hit(inner):
                if not booked:
                    booked.append(1)
                    w.led.record_fill("sniper-x", UP_T, shares=5, price=0.45, condition_id=COND, fee=0.0)

            def values(inner):
                inner._hit()
                return dict.values(inner)

            def get(inner, *a):
                inner._hit()
                return dict.get(inner, *a)
        w.led.positions = Trap(w.led.positions)
        d = w.pm.choose(w.pm.evaluate())
        self.assertIsNotNone(d)

        async def go():
            return w.pm._dispatch(d, asyncio.get_running_loop())
        self.assertIsNone(asyncio.run(go()))
        self.assertEqual(d.reason, "STATE_CHANGED")
        self.assertEqual(w.pm.orders, {})
        w.close()


class Hardening(unittest.TestCase):
    def test_settlement_is_idempotent(self):
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        o = w.pm.reserve(w.pm.choose(w.pm.evaluate()))
        f = {"order_id": "m1", "shares": o.requested_qty, "average_price": 0.5, "fee": 0.1,
             "total_cost": o.requested_qty * 0.5 + 0.1}
        w.pm.settle_order(o, ok=True, fill=f, error=None)
        state = (w.pm.hist.spend, dict(w.pm.n), w.pm.hist.sides["DOWN"].last_done_mono, o.status, o.cost)
        w.pm.settle_order(o, ok=True, fill=f, error=None)
        w.pm.settle_order(o, ok=False, fill=None, error="late duplicate")
        self.assertEqual((w.pm.hist.spend, dict(w.pm.n), w.pm.hist.sides["DOWN"].last_done_mono, o.status, o.cost),
                         state)
        w.close()

    def test_a_busy_ledger_is_its_own_reason_not_insufficient_balance(self):
        w = World()
        w.fill("s", UP_T, 20, 0.40)
        w.book.set(UP_T, [(0.45, 100)])
        w.book.set(DN_T, [(0.50, 100)])
        held, go = threading.Event(), threading.Event()

        def hold():
            with w.led._lock:
                held.set()
                go.wait(5)
        t = threading.Thread(target=hold)
        t.start()
        held.wait(5)
        try:
            dec = w.pm.evaluate()
        finally:
            go.set()
            t.join()
        self.assertEqual({d.reason for d in dec.values()}, {"LEDGER_BUSY"})
        self.assertIs(w.pm._cash_free() is PM._BUSY, False)                # free again now
        w.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
