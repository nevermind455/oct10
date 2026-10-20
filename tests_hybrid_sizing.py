"""Offline risk invariants for hybrid Kelly allocation and optional caps."""
from dataclasses import FrozenInstanceError
from decimal import Decimal
import unittest

from hybrid_sizing import (HybridPolicy, cent_compatible_shares, enforce_order_budget, minimum_entry_shares, risk_total,
                           shares_within_budget, sizing_factors)


class HybridFactors(unittest.TestCase):
    def factors(self, policy=None, **kwargs):
        args = {"sigma": 0.0001, "equity": 100.0, "high_water": 100.0}
        args.update(kwargs)
        return sizing_factors(policy or HybridPolicy(), **args)

    def test_default_is_quarter_kelly_with_no_unvalidated_confidence_adjustment(self):
        result = self.factors()
        self.assertEqual(result["effective_fraction"], .25)
        self.assertEqual(result["confidence_scale"], 1)
        self.assertEqual(result["confidence_status"], "unvalidated_neutral")
        self.assertIsNone(result["risk_budget_usd"])
        self.assertFalse(result["hard_cap_enabled"])

    def test_low_volatility_never_increases_quarter_kelly(self):
        self.assertEqual(self.factors(sigma=.000025)["effective_fraction"], .25)

    def test_above_baseline_volatility_reduces_total_target_fraction(self):
        result = self.factors(sigma=.0002)
        self.assertEqual(result["volatility_scale"], .5)
        self.assertEqual(result["effective_fraction"], .125)

    def test_progressive_drawdown_reaches_zero_at_stop(self):
        fractions = [self.factors(equity=equity)["effective_fraction"]
                     for equity in (100, 95, 90, 85, 80, 75, 0)]
        self.assertEqual(fractions, sorted(fractions, reverse=True))
        self.assertAlmostEqual(fractions[2], .125)
        self.assertEqual(fractions[4], 0)
        self.assertEqual(fractions[-2:], [0, 0])

    def test_drawdown_and_volatility_compound_without_confidence_guess(self):
        result = self.factors(sigma=.0002, equity=90)
        self.assertAlmostEqual(result["effective_fraction"], .0625)

    def test_decimal_equity_stops_exactly_at_drawdown_boundary(self):
        for high_water, equity in ((123.45, 98.76), (50.05, 40.04),
                                   (50.01, 40.008), (1000.01, 800.008)):
            with self.subTest(high_water=high_water, equity=equity):
                result = self.factors(equity=equity, high_water=high_water)
                self.assertEqual(result["drawdown_scale"], 0)
                self.assertEqual(result["effective_fraction"], 0)

    def test_decimal_drawdown_comparison_preserves_positive_allocation_before_stop(self):
        for high_water, boundary in ((123.45, Decimal("98.76")),
                                      (50.05, Decimal("40.04"))):
            with self.subTest(high_water=high_water):
                above = self.factors(equity=boundary + Decimal("0.0000000001"), high_water=high_water)
                below = self.factors(equity=boundary - Decimal("0.0000000001"), high_water=high_water)
                self.assertGreater(above["effective_fraction"], 0)
                self.assertEqual(below["effective_fraction"], 0)

    def test_fractional_and_absolute_caps_can_be_enabled_independently(self):
        for policy, expected in ((HybridPolicy(risk_fraction=.01), 1),
                                 (HybridPolicy(max_order_usd=5), 5),
                                 (HybridPolicy(risk_fraction=.01, max_order_usd=5), 1),
                                 (HybridPolicy(risk_fraction=.1, max_order_usd=5), 5)):
            with self.subTest(policy=policy):
                result = self.factors(policy)
                self.assertTrue(result["hard_cap_enabled"])
                self.assertEqual(result["risk_budget_usd"], expected)

    def test_zero_equity_halts_allocation_and_enabled_equity_cap(self):
        result = self.factors(HybridPolicy(risk_fraction=.01), equity=0)
        self.assertEqual(result["effective_fraction"], 0)
        self.assertEqual(result["risk_budget_usd"], 0)
        self.assertTrue(result["hard_cap_enabled"])

    def test_policy_is_frozen_and_normalizes_numeric_fields(self):
        policy = HybridPolicy(vol_baseline_bp=Decimal("2"))
        self.assertIsInstance(policy.vol_baseline_bp, float)
        with self.assertRaises(FrozenInstanceError):
            policy.vol_baseline_bp = 3

    def test_invalid_policies_fail_closed(self):
        invalid = (True, None, "1", -1, float("nan"), float("inf"))
        for name in ("vol_baseline_bp", "drawdown_stop", "risk_fraction", "max_order_usd"):
            for value in invalid:
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    HybridPolicy(**{name: value})
        for name in ("vol_baseline_bp", "drawdown_stop"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                HybridPolicy(**{name: 0})
        for name in ("drawdown_stop", "risk_fraction"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                HybridPolicy(**{name: 1.01})
        with self.assertRaises(ValueError):
            HybridPolicy(risk_fraction=Decimal("1e-1000"))

    def test_invalid_inputs_never_produce_exposure(self):
        for name in ("sigma", "equity", "high_water", "base_fraction"):
            for value in (True, None, "1", -1, float("nan"), float("inf")):
                with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                    self.factors(**{name: value})
        for kwargs in ({"sigma": 0}, {"high_water": 0}, {"high_water": 99},
                       {"base_fraction": 1.01}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.factors(**kwargs)


class HardRiskLimits(unittest.TestCase):
    def test_all_in_bound_includes_larger_quoted_or_rate_fee(self):
        self.assertEqual(risk_total(5, .07), Decimal("5.35"))
        self.assertEqual(risk_total(5, .07, .5), Decimal("5.5"))
        self.assertEqual(risk_total(5, .07, .1), Decimal("5.35"))

    def test_fee_budget_boundary_uses_decimal_not_float_rounding(self):
        self.assertEqual(shares_within_budget(1.07, .1, .07), 10)
        self.assertEqual(shares_within_budget(Decimal("1.069999999999999999"), .1, .07), 9)
        self.assertEqual(enforce_order_budget(1, .07, 1.07), Decimal("1.07"))
        with self.assertRaises(ValueError):
            enforce_order_budget(1, .07, Decimal("1.069999999999999999"))

    def test_venue_minimum_cannot_be_rounded_past_budget(self):
        budget, cap, fee_rate = 1.0, .25, .07
        self.assertEqual(shares_within_budget(budget, cap, fee_rate), 3)
        with self.assertRaisesRegex(ValueError, "exceeds hybrid budget"):
            enforce_order_budget(5 * cap, fee_rate, budget)

    def test_integer_bound_is_safe_across_prices_and_budgets(self):
        for price in (.01, .13, .5, .97):
            for budget in (0, .1, 1, 5, 13.37):
                for fee in (0, .07, .14):
                    with self.subTest(price=price, budget=budget, fee=fee):
                        shares = shares_within_budget(budget, price, fee)
                        spend = Decimal(shares) * Decimal(str(price))
                        self.assertLessEqual(risk_total(spend, fee), Decimal(str(budget)))
                        self.assertGreater(risk_total(spend + Decimal(str(price)), fee),
                                           Decimal(str(budget)))

    def test_quoted_fee_is_checked_on_final_order(self):
        with self.assertRaises(ValueError):
            enforce_order_budget(1, .01, 1.1, quoted_fee=.2)

    def test_invalid_money_inputs_fail_closed(self):
        for value in (True, None, "1", -1, float("nan"), float("inf")):
            for function, args in ((risk_total, (value, .07)),
                                   (risk_total, (1, value)),
                                   (risk_total, (1, .07, value)),
                                   (shares_within_budget, (value, .5, .07)),
                                   (shares_within_budget, (1, value, .07)),
                                   (shares_within_budget, (1, .5, value)),
                                   (enforce_order_budget, (1, .07, value))):
                with self.subTest(function=function.__name__, args=args), self.assertRaises(ValueError):
                    function(*args)
        with self.assertRaises(ValueError):
            shares_within_budget(1, 0, .07)


class MinimumEntryQuantity(unittest.TestCase):
    def test_fine_tick_targets_are_reduced_to_compatible_quantity(self):
        for target, price, expected in ((11, Decimal('.501'), 10), (9, Decimal('.501'), 0),
                                        (5.9, Decimal('.46'), 5), (25, Decimal('.333'), 20)):
            with self.subTest(target=target, price=price):
                quantity = cent_compatible_shares(target, price)
                self.assertEqual(quantity, expected)
                self.assertLessEqual(quantity, target)
                stake = price * quantity
                self.assertEqual(stake, stake.quantize(Decimal('.01')))

    def test_smallest_quantity_satisfies_venue_dollar_and_whole_cent_minimums(self):
        for cap, venue, expected in ((Decimal('.46'), 5, 5), (Decimal('.11'), 5, 10),
                                     (Decimal('.501'), 5, 10), (Decimal('.46'), 12, 12),
                                     (Decimal('.333'), 12, 20)):
            with self.subTest(cap=cap, venue=venue):
                shares = minimum_entry_shares(cap, venue)
                self.assertEqual(shares, expected)
                spend = cap * shares
                self.assertGreaterEqual(spend, 1)
                self.assertGreaterEqual(shares, venue)
                self.assertEqual(spend, spend.quantize(Decimal('.01')))

    def test_invalid_minimum_order_inputs_are_refused(self):
        for cap, venue in ((0, 5), (1, 5), (float('nan'), 5), (True, 5), (.5, 0), (.5, True)):
            with self.subTest(cap=cap, venue=venue), self.assertRaises(ValueError):
                minimum_entry_shares(cap, venue)


if __name__ == "__main__":
    unittest.main()
