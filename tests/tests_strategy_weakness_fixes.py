"""Offline regressions for portfolio Kelly, historical anchors and venue fees.

Load the actual pure functions and decision methods with the standard library
so these checks also run on a checkout without the trading SDK. Fixtures model
cash and terminal outcome inventory; no venue, wallet or external I/O is used.
"""
from __future__ import annotations

import ast
import collections
import math
import pathlib
import statistics
import types
import unittest
from decimal import Decimal, ROUND_FLOOR


def load_strategy():
    tree = ast.parse(pathlib.Path(__file__).resolve().parent.parent.joinpath("sniper_mode.py").read_text())
    helpers = {"taker_fee", "anchored_fair", "fair_limit", "kelly_shares"}
    methods = {"_side_held", "_kelly_inventory", "_fair_fee_parameters", "_fair_anchor", "_decide", "ioc_spend",
               "_mirror_on", "_wide_cap_now", "_slip_now"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in helpers]
    sniper = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Sniper")
    cls = ast.ClassDef(name="Decision", bases=[], keywords=[], decorator_list=[],
                       body=[n for n in sniper.body if isinstance(n, ast.FunctionDef) and n.name in methods])
    nodes.append(cls)
    clock = types.SimpleNamespace(time=lambda: 1000.0, monotonic=lambda: 10.0, perf_counter_ns=lambda: 1)
    ns = dict(math=math, Decimal=Decimal, ROUND_FLOOR=ROUND_FLOOR, time=clock,
              _NORMAL=statistics.NormalDist(), FAIR_MID_CLAMP=0.005, KELLY_MIN_SHARES=5, MIN_ORDER_USD=1.0)
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
                 "<production_strategy>", "exec"), ns)
    return types.SimpleNamespace(**{name: ns[name] for name in helpers | {"Decision"}})


S = load_strategy()


class PortfolioKelly(unittest.TestCase):
    def test_same_outcome_repeated_signal_does_not_repeat_the_bet(self):
        cash, held, p = 100.0, 0.0, 0.5175
        first, _ = S.kelly_shares(0.60, 0.50, 0.07, 1, cash, 200)
        self.assertEqual(first, 33)
        cash -= first * p
        held += first
        second, _ = S.kelly_shares(0.60, 0.50, 0.07, 1, cash, 200, held_shares=held)
        self.assertEqual(second, 0)

    def test_full_kelly_maximizes_terminal_wealth_with_both_legs(self):
        q, ask, rate, cash, own, opposite = 0.62, 0.50, 0.07, 100, 18, 8
        p = ask + rate * ask * (1 - ask)
        got, _ = S.kelly_shares(q, ask, rate, 1, cash, 200,
                                held_shares=own, opposite_shares=opposite)
        # Independent numerical expected-log search with sub-share precision.
        def utility(x):
            return q * math.log(cash + own + (1-p)*x) + (1-q)*math.log(cash + opposite - p*x)
        best = max((i / 100 for i in range(int(cash / p * 100))), key=utility)
        self.assertEqual(got, math.floor(best))
        self.assertLess(utility(got - 1), utility(got))

    def test_fractional_kelly_targets_total_exposure(self):
        cash, held = 100.0, 0.0
        first, _ = S.kelly_shares(0.60, 0.50, 0.07, 0.25, cash, 200)
        self.assertEqual(first, 8)
        cash -= first * 0.5175
        held += first
        again, _ = S.kelly_shares(0.60, 0.50, 0.07, 0.25, cash, 200, held_shares=held)
        self.assertEqual(again, 0)

    def test_opposing_inventory_can_be_hedged_but_never_spends_more_than_cash(self):
        unhedged, _ = S.kelly_shares(0.60, 0.50, 0.07, 1, 10, 200)
        hedge, _ = S.kelly_shares(0.60, 0.50, 0.07, 1, 10, 200, opposite_shares=100)
        self.assertGreater(hedge, unhedged)
        self.assertLessEqual(hedge * 0.5175, 10)

    def test_fee_exponent_changes_the_cash_cost_used_for_kelly(self):
        got, _ = S.kelly_shares(0.60, 0.50, 0.14, 1, 100, 200, fee_exponent=2)
        p = 0.50 + 0.14 * (0.50 * 0.50) ** 2
        self.assertEqual(got, math.floor((0.60-p)*100/(p*(1-p))))


class DecisionPath(unittest.TestCase):
    def setUp(self):
        self.sn = S.Decision()
        self.inventory = {"up": 0.0, "down": 0.0}
        self.rules = types.SimpleNamespace(condition_id="market", fee_rate=Decimal(".07"), fee_exponent=1,
                                          up_token_id="up", down_token_id="down")
        self.view = types.SimpleNamespace(status="LIVE", asks=[(.50, 1000)], mid=.49,
                                          updated_mono=10.0, tick_size=.01)
        self.book = types.SimpleNamespace(view=lambda token: self.view,
                                          view_at_mono=lambda token, mono: self.view)
        self.sn.hub = types.SimpleNamespace(up_token="up", down_token="down", condition_id="market",
                                           window_end=1100.0, book=self.book)
        ledger = types.SimpleNamespace(open_leg_shares=lambda condition, token: self.inventory.get(token, 0.0))
        self.sn.broker = types.SimpleNamespace(ledger=ledger, min_buy_price=.05, max_buy_price=.95,
                                              _rules_cache={"market": (self.rules, 1000.0)}, _rules_cache_ttl=600)
        self.sn.s = types.SimpleNamespace(fair=True, kelly=True, fair_margin=.01, fair_fee_rate=0,
                                         stale_ms=2000, slip=.05, order_type="FAK", max_side_shares=200,
                                         kelly_fraction=1, max_fills_round=20, order_sizing="dollars")
        self.sn.live = None
        self.sn._fak_pending = {}
        self.sn.inflight = {"UP": None, "DOWN": None}
        self.sn.rounds = collections.defaultdict(lambda: {"fills": 0, "UP": 0.0, "DOWN": 0.0})
        self.sn.round_baseline = {}
        self.sn.n = collections.Counter()
        self.sn.vol_model = types.SimpleNamespace(vol=lambda: .0001)
        self.sn._sync_live_fak = lambda: None
        self.sn._window = lambda: (300, 10)
        self.sn._visible_ask = lambda token, view: view.asks[0][0]
        self.sn._visible_levels = lambda token, view: view.asks
        self.sn.cash = 100.0
        self.sn._bankroll_now = lambda: self.sn.cash
        self.sn.latency_plan = lambda: (170, {})
        self.skips = []
        self.sn._skip = lambda reason, row: self.skips.append(reason)

    def decide(self):
        return self.sn._decide("UP", {"bp": 3.0, "window_ms": 250.0}, 10000.0)

    def test_repeated_decision_accounts_for_a_fill_already_owned(self):
        first = self.decide()
        self.assertIsNotNone(first)
        held = first["target"]
        self.inventory["up"] = held
        self.sn.rounds[1100.0]["UP"] = held
        self.sn.cash -= held * .5175
        self.sn.inflight["UP"] = None
        self.assertIsNone(self.decide())
        self.assertEqual(self.skips[-1], "kelly size under the 5-share minimum")

    def test_missing_history_fails_closed_after_rotation_or_reconnect(self):
        self.book.view_at_mono = lambda token, mono: None
        self.assertIsNone(self.decide())
        self.assertEqual(self.skips[-1], "fair value unknown: no pre-move market history")
        self.assertEqual(self.sn.n["orders"], 0)

    def test_missing_reference_fails_closed(self):
        self.assertIsNone(self.sn._decide("UP", {"bp": 3.0}, 10000.0))
        self.assertEqual(self.skips[-1], "fair value unknown: no pre-move market history")

    def test_missing_fee_rules_does_not_fetch_or_guess(self):
        self.sn.broker._rules_cache.clear()
        self.sn.broker._rules = lambda condition: self.fail("decision fetched venue rules")
        self.assertIsNone(self.decide())
        self.assertEqual(self.skips[-1], "fair value unknown: market fee rules not loaded")

    def test_actual_fee_rate_overrides_the_old_environment_assumption(self):
        self.view.asks = [(.58, 1000)]
        # Environment assumption zero would accept, actual .14 fees reject.
        self.rules.fee_rate = Decimal(".14")
        self.assertIsNone(self.decide())
        self.assertEqual(self.skips[-1], "ask not below fair value")

    def test_fee_exponent_and_rate_match_execution_rules(self):
        self.rules.fee_rate, self.rules.fee_exponent = Decimal(".14"), 2
        row = {"bp": 3.0, "window_ms": 250.0}
        plan = self.sn._decide("UP", row, 10000.0)
        self.assertIsNotNone(plan)
        exact_fair = S.anchored_fair(.49, 3, .0001, 100)
        candidates = [c / 100 for c in range(1, 100)
                      if c/100 + .14 * ((c/100)*(1-c/100))**2 + .01 <= exact_fair]
        self.assertEqual(row["fair_limit"], max(candidates))
        self.assertEqual((plan["fair_fee_rate"], plan["fair_fee_exponent"]), (.14, 2))

    def test_uncertain_opposite_order_is_not_a_guaranteed_hedge(self):
        self.inventory["up"], self.inventory["down"] = 10, 3
        self.sn.inflight["UP"] = {"end": 1100, "risk_shares": 4}
        self.sn.inflight["DOWN"] = {"end": 1100, "risk_shares": 100}
        self.sn._fak_pending["pending-up"] = ({"side": "UP"}, {"cond": "market", "risk_shares": 7})
        own, hedge = self.sn._kelly_inventory("market", 1100, "UP", "up", "down")
        self.assertEqual((own, hedge), (21, 3))

    def test_live_reservation_hook_is_used_without_double_counting(self):
        self.inventory["up"], self.inventory["down"] = 10, 3
        self.sn.live = types.SimpleNamespace(ledger=self.sn.broker.ledger,
                                            own_risk_shares=lambda token, condition: 24)
        self.sn.inflight["UP"] = {"end": 1100, "risk_shares": 14}
        self.assertEqual(self.sn._kelly_inventory("market", 1100, "UP", "up", "down"), (24, 3))


if __name__ == "__main__":
    unittest.main()
