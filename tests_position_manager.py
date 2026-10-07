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
from paper_trade import MarketRules  # noqa: E402

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
