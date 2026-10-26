"""Offline integration of hybrid sizing with real account and portfolio gates.

These tests hold the book and modeled fair value steady to distinguish changes
to the allocation fraction from changes to price prediction or execution.
"""
from __future__ import annotations

from decimal import Decimal
import time
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import testing_env  # noqa: F401 - never load a deployment's .env
import sniper_mode as S
import tests_sniper_ioc as I
from accounting.ledger import Ledger
from paper_trade import MarketRules


class HybridStrategy(I.PaperIOCBase):
    def setUp(self):
        super().setUp()
        self.m.window_end = time.time() + 120
        self.sigma = .0001
        self.m.set(I.UP, [(.41, 1000)])
        self.m.set(I.DOWN, [(.61, 1000)])

        def view(token):
            result = I.View(self.m.decision[str(token)])
            result.mid = .40 if str(token) == I.UP else .60
            return result

        self.m.book = types.SimpleNamespace(
            view=view, view_at_mono=lambda token, _mono: view(token))
        self.broker._rules_fetch = lambda cid: MarketRules(
            cid, Decimal('.07'), 1, min_order_size=Decimal('5'),
            tick_size=Decimal('.01'), up_token_id=I.UP, down_token_id=I.DOWN)
        self.broker._rules_cache.clear()
        self.broker._rules(self.m.condition_id)
        self.make()

    def make(self, **overrides):
        env = {
            'SNIPER_FAIR': '1', 'SNIPER_FAIR_MARGIN': '.01', 'SNIPER_KELLY': '1',
            'SNIPER_KELLY_FRACTION': '.25', 'SNIPER_HYBRID_SIZING': '1',
            'SNIPER_HYBRID_VOL_BASELINE_BP': '1', 'SNIPER_HYBRID_DRAWDOWN_STOP': '.20',
            'SNIPER_HYBRID_RISK_FRACTION': '0', 'SNIPER_HYBRID_MAX_ORDER_USD': '0',
            'SNIPER_ORDER_SIZING': 'shares', **overrides,
        }
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(**env))
        self.sn.vol_model.vol = lambda: self.sigma
        self.sn._write = self.rows.append
        self.sn._set_bankroll(self.broker.cash_balance(), time.monotonic())

    def refresh(self):
        result = self.sn._refresh_hybrid_risk()
        self.assertIsNotNone(result, self.sn._hybrid_risk_error)
        return result

    def plan(self, *, move=5):
        self.sn.inflight = {'UP': None, 'DOWN': None}
        now_ms = time.monotonic() * 1000
        self.sn.trigger.ref = (now_ms - 250, 60_000)
        row = {'bp': move, 'window_ms': 250}
        result = self.sn._decide('UP', row, now_ms)
        return result, row

    def realize(self, name, *, loss=100):
        token, condition = 'historical-' + name, 'condition-' + name
        self.assertTrue(self.broker.ledger.record_fill_durable(
            'fill-' + name, token, condition_id=condition,
            shares=2 * loss, price=.5, fee=0, status='CONFIRMED'))
        self.broker.ledger.settle(types.SimpleNamespace(
            resolved=True, condition_id=condition,
            payouts={token: 0, 'opposite-' + name: 1}))
        self.sn._set_bankroll(self.broker.cash_balance(), time.monotonic())

    def test_real_cash_initializes_durable_equity_and_neutral_confidence(self):
        snapshot = self.refresh()
        self.assertEqual((snapshot['equity'], snapshot['high_water']), (1000, 1000))
        reloaded = Ledger(self.broker.ledger.path)
        self.assertEqual(reloaded.hybrid_risk_view()['high_water'], 1000)
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        for factors in (row['hybrid'], plan['hybrid']):
            self.assertEqual(factors['effective_fraction'], .25)
            self.assertEqual(factors['confidence_scale'], 1)
            self.assertEqual(factors['confidence_status'], 'unvalidated_neutral')
            self.assertFalse(factors['hard_cap_enabled'])
            self.assertIsNone(factors['risk_budget_usd'])
        self.assertIsNone(plan.get('risk_budget_usd'))
        self.assertGreater(plan['target'], 25)  # no new default small-order cap

    def test_inventory_purchase_does_not_look_like_realized_drawdown(self):
        self.refresh()
        self.assertTrue(self.broker.ledger.record_fill_durable(
            'inventory', I.UP, condition_id=self.m.condition_id,
            shares=100, price=.41, fee=1, status='CONFIRMED'))
        self.assertEqual(self.broker.cash_balance(), 958)
        snapshot = self.refresh()
        self.assertEqual(snapshot['equity'], 1000)
        factors = self.sn._hybrid_factors()
        self.assertEqual(factors['drawdown_scale'], 1)

    def test_first_cash_reference_rejects_a_concurrent_confirmed_purchase(self):
        read_cash = self.sn._read_bankroll

        def cash_then_fill():
            old_cash = read_cash()
            self.assertTrue(self.broker.ledger.record_fill_durable(
                'initialization-race', I.UP, condition_id=self.m.condition_id,
                shares=200, price=.5, fee=0, status='CONFIRMED'))
            return old_cash

        with patch.object(self.sn, '_read_bankroll', side_effect=cash_then_fill):
            self.assertIsNone(self.sn._refresh_hybrid_risk())
        self.assertIsNone(self.broker.ledger.hybrid_risk)
        snapshot = self.refresh()
        self.assertEqual(snapshot['reference_equity'], 1000)
        self.assertEqual(snapshot['equity'], 1000)

    def test_high_volatility_reduces_allocation_with_same_fair_value(self):
        self.refresh()
        ordinary, row = self.plan(move=5)
        self.assertIsNotNone(ordinary, row)
        self.sigma = .0002
        volatile, high_row = self.plan(move=10)
        self.assertIsNotNone(volatile, high_row)
        self.assertAlmostEqual(row['fair'], high_row['fair'], places=3)
        self.assertEqual(volatile['hybrid']['volatility_scale'], .5)
        self.assertEqual(volatile['hybrid']['effective_fraction'], .125)
        self.assertLess(volatile['target'], ordinary['target'])
        self.assertAlmostEqual(volatile['target'], ordinary['target'] / 2, delta=1)

    def test_realized_drawdown_reduces_target_without_guessing_confidence(self):
        self.refresh()
        ordinary, row = self.plan()
        self.assertIsNotNone(ordinary, row)
        self.realize('loss', loss=100)
        self.refresh()
        reduced, loss_row = self.plan()
        self.assertIsNotNone(reduced, loss_row)
        factors = reduced['hybrid']
        self.assertEqual(factors['equity'], 900)
        self.assertEqual(factors['high_water'], 1000)
        self.assertAlmostEqual(factors['drawdown_scale'], .5)
        self.assertAlmostEqual(factors['effective_fraction'], .125)
        self.assertEqual(factors['confidence_scale'], 1)
        self.assertLess(reduced['target'], ordinary['target'])

    def test_drawdown_stop_produces_no_order_even_without_a_hard_dollar_cap(self):
        self.refresh()
        self.realize('stop', loss=200)
        self.refresh()
        plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertEqual(self.sn.n['orders'], 0)
        self.assertEqual(self.sn._hybrid_factors()['effective_fraction'], 0)

    def test_missing_or_stale_risk_state_never_creates_an_order(self):
        for snapshot in (None, {'observed_mono': time.monotonic() - 31,
                                'equity': 1000, 'high_water': 1000}):
            with self.subTest(snapshot=snapshot):
                self.sn._hybrid_risk = snapshot
                plan, row = self.plan()
                self.assertIsNone(plan, row)
        self.assertEqual(self.sn.n['orders'], 0)

    def test_hot_decision_uses_cached_risk_and_does_not_read_account_or_persist(self):
        self.refresh()
        ledger = self.broker.ledger
        with patch.object(ledger, 'hybrid_risk_view', side_effect=AssertionError('risk I/O on hot path')), \
                patch.object(ledger, '_write_payload', side_effect=AssertionError('disk I/O on hot path')), \
                patch.object(self.broker, 'cash_balance', side_effect=AssertionError('cash scan on hot path')):
            plan, row = self.plan()
        self.assertIsNotNone(plan, row)

    def test_scaled_total_target_cannot_creep_up_through_repeated_signals(self):
        self.sigma = .0002
        self.refresh()
        first, row = self.plan(move=10)
        self.assertIsNotNone(first, row)
        quantity = first['target']
        fee = quantity * S.taker_fee(.41, .07)
        self.assertTrue(self.broker.ledger.record_fill_durable(
            'first-scaled-fill', I.UP, condition_id=self.m.condition_id,
            shares=quantity, price=.41, fee=fee, status='CONFIRMED'))
        self.sn.rounds[self.m.window_end]['UP'] = quantity
        self.sn._set_bankroll(self.broker.cash_balance(), time.monotonic())
        for _ in range(3):
            again, next_row = self.plan(move=10)
            self.assertIsNone(again, next_row)
            self.assertEqual(next_row['kelly_shares'], 0)
        self.assertEqual(self.broker.ledger.open_leg_shares(self.m.condition_id, I.UP), quantity)

    def test_optional_absolute_cap_bounds_whole_shares_and_fee(self):
        self.make(SNIPER_HYBRID_MAX_ORDER_USD='5')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['risk_budget_usd'], 5)
        self.assertTrue(row['hybrid']['hard_cap_enabled'])
        self.assertLessEqual(Decimal(str(plan['amount'])) * Decimal('1.07'), Decimal('5'))
        self.assertGreaterEqual(row['order_shares'], 5)

    def test_optional_equity_cap_is_calculated_from_risk_equity(self):
        self.make(SNIPER_HYBRID_RISK_FRACTION='.01')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['risk_budget_usd'], 10)
        self.assertLessEqual(Decimal(str(plan['amount'])) * Decimal('1.07'), Decimal('10'))

    def test_optional_cap_below_venue_minimum_skips_instead_of_rounding_up(self):
        self.make(SNIPER_HYBRID_MAX_ORDER_USD='1')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertEqual(self.sn.n['orders'], 0)
        self.assertFalse(any(self.sn.inflight.values()))

    def test_match_forwards_decided_share_size_and_keeps_actual_cost_within_cap(self):
        self.make(SNIPER_HYBRID_MAX_ORDER_USD='5')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['shares'], row['order_shares'])
        with patch.object(self.broker, 'place_trade', wraps=self.broker.place_trade) as submit:
            result = self.sn._match(plan, 0, {'sent_pc': time.perf_counter_ns()})
        self.assertTrue(result[0], self.broker.last_error)
        self.assertEqual(submit.call_args.kwargs['shares'], plan['shares'])
        position = self.broker.ledger.positions[I.UP]
        self.assertGreater(position.shares, 0)
        self.assertLessEqual(position.cost, 5)
        self.assertTrue(submit.call_args.kwargs['hybrid_risk_guard'])

    def test_paper_minimum_rounding_is_rechecked_before_booking_any_fill(self):
        self.make(SNIPER_HYBRID_MAX_ORDER_USD='5')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        # Exercise the broker's final guard independently of the strategy's
        # earlier gate: a dollar-sized stake below the minimum is enlarged
        # by the broker, and that enlarged spend must still fit the budget.
        plan['shares'] = None
        plan['risk_budget_usd'] = 1
        args = list(plan['send'])
        args[1] = .1
        plan['send'] = tuple(args)
        result = self.sn._match(plan, 0, {'sent_pc': time.perf_counter_ns()})
        self.assertFalse(result[0])
        self.assertIn('hybrid', self.broker.last_error.lower())
        self.assertNotIn(I.UP, self.broker.ledger.positions)

    def test_dispatch_refuses_when_volatility_reduces_decided_allocation(self):
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.sigma = .0002
        self.assertGreater(self.sn._hybrid_factors()['effective_fraction'], 0)
        with self.assertRaisesRegex(ValueError, 'hybrid'):
            self.sn._check_hybrid_order(plan, plan['amount'], .07)

    def test_dispatch_refuses_partial_drawdown_reduction_before_the_stop(self):
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.realize('before-dispatch', loss=100)
        self.refresh()
        self.assertGreater(self.sn._hybrid_factors()['effective_fraction'], 0)
        with self.assertRaisesRegex(ValueError, 'hybrid'):
            self.sn._check_hybrid_order(plan, plan['amount'], .07)

    def test_paper_arrival_does_not_cancel_fill_for_risk_worsening_after_send(self):
        self.make(SNIPER_HYBRID_MAX_ORDER_USD='5')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.sn._check_hybrid_order(plan, plan['amount'], .07)
        original_fetch = self.broker._book_fetch
        reads = []

        def arrival_fetch(token):
            reads.append(token)
            if len(reads) == 2:
                # The quote remains available, but volatility and the account
                # deteriorate while the already-submitted order is in flight.
                # A real FAK cannot retroactively cancel on that information.
                self.sigma = .0002
                self.realize('during-flight', loss=200)
                self.refresh()
            return original_fetch(token)

        with patch.object(self.broker, '_book_fetch', side_effect=arrival_fetch):
            result = self.sn._match(plan, 0, {'sent_pc': time.perf_counter_ns()})
        self.assertGreaterEqual(len(reads), 2)
        self.assertTrue(result[0], self.broker.last_error)
        self.assertEqual(self.sn._hybrid_factors()['effective_fraction'], 0)
        self.assertGreater(self.broker.ledger.positions[I.UP].shares, 0)
        self.assertLessEqual(self.broker.ledger.positions[I.UP].cost, 5)

    def test_valid_five_share_kelly_order_executes_at_the_capped_stake(self):
        self.broker.start_balance = 32
        self.sn._set_bankroll(self.broker.cash_balance(), time.monotonic())
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual((plan['target'], plan['shares']), (5, 5))
        self.assertEqual((row['book_cost'], plan['amount']), (2.05, 2.30))
        self.assertEqual(plan['risk_shares'], 5)
        result = self.sn._match(plan, 0, {'sent_pc': time.perf_counter_ns()})
        self.assertTrue(result[0], self.broker.last_error)
        # The five-share sizing target signs $2.30 at the cap. A FAK BUY
        # spends those maker dollars, buying extra shares at the .41 ask.
        self.assertAlmostEqual(self.broker.ledger.positions[I.UP].shares, 2.30 / .41)
        self.assertAlmostEqual(self.broker.ledger.positions[I.UP].cost,
                               2.30 + self.broker.last_fill['fee'])
        self.assertGreaterEqual(self.broker.cash_balance(), 0)

    def test_room_limited_share_order_keeps_exact_signed_stake_and_quantity(self):
        self.sn.s.max_side_shares = 20
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['shares'], 20)
        self.assertEqual(plan['amount'], 9.20)
        self.assertEqual(row['book_cost'], 8.20)
        self.assertEqual(plan['risk_shares'], 20)
        self.assertEqual(self.sn.inflight['UP']['risk_shares'], 20)
        self.assertEqual(Decimal(str(plan['amount'])),
                         Decimal(plan['shares']) * Decimal(str(plan['cap'])))

    def test_deeper_book_cost_does_not_falsely_exceed_exact_share_room(self):
        self.sn.s.max_side_shares = 20
        self.m.set(I.UP, [(.41, 5), (.45, 1000)])
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['shares'], 20)
        self.assertGreater(row['book_cost'] / row['ask_seen'], 20)
        self.assertEqual(plan['amount'], 9.20)

    def test_kelly_target_is_not_raised_to_satisfy_one_dollar_minimum(self):
        self.m.set(I.UP, [(.06, 1000)])
        self.refresh()
        with patch.object(S, 'kelly_shares', return_value=(5, .1)):
            plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertIn('minimum', row['reason'])
        self.assertEqual(row['kelly_shares'], 5)
        self.assertEqual(self.sn.n['orders'], 0)

    def test_signed_stake_and_fee_must_fit_cash_even_if_visible_fill_cost_fits(self):
        self.broker.start_balance = 2.40
        self.sn._set_bankroll(self.broker.cash_balance(), time.monotonic())
        self.refresh()
        with patch.object(S, 'kelly_shares', return_value=(5, .1)), \
                patch.object(S, 'shares_within_budget', return_value=5):
            plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertEqual(row['reason'], 'kelly cash below signed order reservation')
        self.assertEqual(self.sn.n['orders'], 0)

    def test_paper_signs_exact_share_quantity_using_limit_order_builder(self):
        from clob_sdk import CreateOrderOptions, MarketOrderArgsV2, OrderArgsV2

        self.sn.s.max_side_shares = 20
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        signed = object()
        builder = types.SimpleNamespace(
            build_order=Mock(return_value=signed),
            build_market_order=Mock(side_effect=AssertionError('share order used dollar builder')))
        self.sn._builder = builder
        self.sn.s.fixed_latency = False
        self.sn._clob = {'OrderArgsV2': OrderArgsV2, 'MarketOrderArgsV2': MarketOrderArgsV2,
                         'CreateOrderOptions': CreateOrderOptions, 'BUY': 'BUY', 'FAK': 'FAK'}
        _elapsed, error, actual = self.sn._sign_now(plan)
        self.assertIsNone(error)
        self.assertIs(actual, signed)
        args, options = builder.build_order.call_args.args
        self.assertEqual((args.token_id, args.size, args.side), (I.UP, 20, 'BUY'))
        self.assertEqual(args.price, S.signing_price(plan['cap'], plan['tick']))
        self.assertEqual(options.tick_size, plan['tick'])
        self.assertEqual(builder.build_order.call_args.kwargs['version'], 2)
        builder.build_market_order.assert_not_called()

    def test_dollar_signing_retains_market_order_builder(self):
        from clob_sdk import CreateOrderOptions, MarketOrderArgsV2, OrderArgsV2

        self.make(SNIPER_ORDER_SIZING='dollars')
        self.refresh()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        signed = object()
        builder = types.SimpleNamespace(
            build_order=Mock(side_effect=AssertionError('dollar order used share builder')),
            build_market_order=Mock(return_value=signed))
        self.sn._builder = builder
        self.sn.s.fixed_latency = False
        self.sn._clob = {'OrderArgsV2': OrderArgsV2, 'MarketOrderArgsV2': MarketOrderArgsV2,
                         'CreateOrderOptions': CreateOrderOptions, 'BUY': 'BUY', 'FAK': 'FAK'}
        _elapsed, error, actual = self.sn._sign_now(plan)
        self.assertIsNone(error)
        self.assertIs(actual, signed)
        args, _options = builder.build_market_order.call_args.args
        self.assertEqual(args.amount, S.signing_amount(plan['amount']))
        self.assertEqual(args.order_type, 'FAK')
        builder.build_order.assert_not_called()


    def enable_small_entry(self, cash=20, **settings):
        self.broker.start_balance = cash
        self.make(SNIPER_KELLY_MIN_ENTRY='1', **settings)
        self.refresh()

    def test_positive_small_target_can_submit_and_fill_one_minimum_entry(self):
        self.broker.start_balance = 20
        self.make(SNIPER_KELLY_MIN_ENTRY='0')
        self.refresh()
        skipped, strict_row = self.plan()
        self.assertIsNone(skipped, strict_row)
        self.assertEqual(self.sn.n['orders'], 0)
        self.enable_small_entry()
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertGreater(row['kelly_shares'], 0)
        self.assertLess(row['kelly_shares'], 5)
        self.assertEqual(plan['shares'], 5)
        self.assertTrue(plan['minimum_entry'])
        self.assertEqual(plan['amount'], 2.30)
        plan['sent_wall'], plan['sent_mono'] = time.time(), time.monotonic()
        ok, *_rest = self.sn._match(plan, None, {'sent_pc': time.perf_counter_ns()})
        self.assertTrue(ok, plan.get('result'))
        self.assertAlmostEqual(self.broker.last_fill['shares'], 2.30 / .41)
        self.assertEqual(self.broker.last_fill['requested_amount'], 2.30)

    def test_minimum_entry_is_not_repeated_by_another_signal(self):
        self.enable_small_entry()
        first, row = self.plan()
        self.assertIsNotNone(first, row)
        again, row = self.plan()
        self.assertIsNone(again)
        self.assertIn('already attempted', row['reason'])
        self.assertEqual(self.sn.n['orders'], 1)

    def test_existing_partial_or_pending_inventory_cannot_be_topped_up_by_floor(self):
        self.enable_small_entry()
        with patch.object(self.sn, '_kelly_inventory', return_value=(1, 0)):
            plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertNotIn('minimum_entry', row)

    def test_minimum_entry_respects_one_dollar_floor(self):
        self.m.set(I.UP, [(.06, 1000)])
        self.enable_small_entry(cash=2)
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertTrue(plan['minimum_entry'])
        self.assertEqual(plan['shares'], 10)
        self.assertEqual(plan['amount'], 1.10)

    def test_insufficient_cash_cannot_be_overridden_by_minimum_entry(self):
        self.enable_small_entry(cash=2.20)
        plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertFalse(row.get('minimum_entry', False))
        self.assertEqual(self.sn.n['orders'], 0)

    def test_minimum_entry_cannot_override_drawdown_stop(self):
        self.enable_small_entry()
        self.realize('minimum-stop', loss=4)
        self.refresh()
        plan, row = self.plan()
        self.assertIsNone(plan)
        self.assertEqual(row['reason'], 'hybrid drawdown stop')

    def test_minimum_entry_cannot_override_decimal_drawdown_stop(self):
        self.enable_small_entry(cash=123.45)
        self.realize('decimal-minimum-stop', loss=24.69)
        snapshot = self.refresh()
        self.assertEqual(snapshot['high_water'], 123.45)
        self.assertAlmostEqual(snapshot['equity'], 98.76)
        plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertEqual(row['reason'], 'hybrid drawdown stop')
        self.assertEqual(self.sn.n['orders'], 0)

    def test_minimum_entry_is_never_taken_in_a_realized_drawdown(self):
        # AUDIT_20261023.md M3 (fixed 2026-10-25): an 18.7% drawdown (scale 0.064) still bought a
        # 5-share minimum where Kelly said 0. Below the high-water mark the hybrid scale decides alone.
        self.enable_small_entry()
        self.realize('minimum-drawdown', loss=1)                 # 5% below the peak: scale 0.75, no stop
        snapshot = self.refresh()
        self.assertLess(snapshot['equity'], snapshot['high_water'])
        plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertTrue(0 < row['kelly_shares'] < 5 and row['kelly_f'] > 0, row)   # a minimum entry used to fire
        self.assertEqual(row.get('minimum_entry_blocked'), 'realized drawdown')
        self.assertFalse(row.get('minimum_entry', False))
        self.assertIn('kelly size under', row['reason'])
        self.assertEqual(self.sn.n['orders'], 0)

    def test_minimum_entry_cannot_override_an_optional_risk_cap(self):
        self.enable_small_entry(SNIPER_HYBRID_MAX_ORDER_USD='2')
        plan, row = self.plan()
        self.assertIsNone(plan, row)
        self.assertFalse(row.get('minimum_entry', False))

    def test_fallback_honors_prefetched_venue_share_minimum(self):
        self.enable_small_entry()
        self.broker._rules_cache.clear()
        self.broker._rules_fetch = lambda cid: MarketRules(
            cid, Decimal('.07'), 1, min_order_size=Decimal('10'), tick_size=Decimal('.01'),
            up_token_id=I.UP, down_token_id=I.DOWN)
        self.broker._rules(self.m.condition_id)
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['shares'], 10)
        self.assertEqual(plan['amount'], 4.60)

    def test_cash_limited_hedge_is_reduced_instead_of_falsely_skipped(self):
        self.broker.start_balance = 620
        self.assertTrue(self.broker.ledger.record_fill_durable(
            'opposite-holding', I.DOWN, condition_id=self.m.condition_id,
            shares=1000, price=.61, fee=0, status='CONFIRMED'))
        self.make(SNIPER_KELLY_MIN_ENTRY='1')
        self.refresh()
        self.assertEqual(self.broker.cash_balance(), 10)
        plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['shares'], 20)
        self.assertFalse(plan['minimum_entry'])
        self.assertLessEqual(Decimal(str(plan['amount'])) * Decimal('1.07'), Decimal(10))

    def test_completed_paper_attempts_restore_claims_without_claiming_unsent_orders(self):
        self.enable_small_entry()
        key = [str(self.m.condition_id), I.UP, float(self.m.window_end)]
        with tempfile.TemporaryDirectory() as directory:
            self.sn.journal = Path(directory) / 'orders.jsonl'
            row = {'minimum_entry': True, 'minimum_entry_key': key}
            self.sn.journal.write_text(json.dumps(row) + '\n')
            self.sn._seed_minimum_entries()
            self.assertNotIn(tuple(key), self.sn._minimum_entry_keys)
            row['minimum_entry_attempted'] = True
            self.sn.journal.write_text(json.dumps(row) + '\n')
            self.sn._seed_minimum_entries()
            self.assertIsNone(self.sn._minimum_entry_seed_error)
            plan, row = self.plan()
            self.assertIsNone(plan)
            self.assertIn('already attempted', row['reason'])

    def test_proven_unsent_minimum_entry_can_be_released(self):
        self.enable_small_entry()
        first, row = self.plan()
        self.assertIsNotNone(first, row)
        self.sn._release_minimum_entry(first)
        next_plan, row = self.plan()
        self.assertIsNotNone(next_plan, row)
        self.assertTrue(next_plan['minimum_entry'])

    def test_malformed_completed_attempt_blocks_minimum_entry_recovery(self):
        self.enable_small_entry()
        with tempfile.TemporaryDirectory() as directory:
            self.sn.journal = Path(directory) / 'orders.jsonl'
            self.sn.journal.write_text(json.dumps({
                'minimum_entry': True, 'minimum_entry_attempted': True,
                'minimum_entry_key': ['wrong-shape']}) + '\n')
            self.sn._seed_minimum_entries()
            self.assertIsNotNone(self.sn._minimum_entry_seed_error)
            plan, row = self.plan()
            self.assertIsNone(plan)
            self.assertEqual(row['reason'], 'minimum entry state unavailable')

    def test_fine_tick_normal_target_is_reduced_instead_of_skipped(self):
        original_view = self.m.book.view
        def fine_view(token):
            view = original_view(token)
            view.tick_size = .001
            return view
        self.m.book.view = fine_view
        self.m.book.view_at_mono = lambda token, _mono: fine_view(token)
        self.broker._rules_cache.clear()
        self.broker._rules_fetch = lambda cid: MarketRules(
            cid, Decimal('.07'), 1, min_order_size=Decimal('5'), tick_size=Decimal('.001'),
            up_token_id=I.UP, down_token_id=I.DOWN)
        self.broker._rules(self.m.condition_id)
        self.m.set(I.UP, [(.491, 1000)])
        self.refresh()
        with patch.object(S, 'fair_limit', return_value=.501), \
                patch.object(S, 'kelly_shares', return_value=(11, .1)):
            plan, row = self.plan()
        self.assertIsNotNone(plan, row)
        self.assertEqual(plan['shares'], 10)
        self.assertEqual(plan['amount'], 5.01)
        self.assertFalse(plan['minimum_entry'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
