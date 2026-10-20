"""Offline regression for the real FAK maker-dollar execution contract.

The recorded Oct 14 price improvement bought more shares than the sizing
target. Paper and live accounting must agree without requiring SDK imports,
signing keys, HTTP sockets or a simulated exchange acknowledgment.
"""
from __future__ import annotations

import testing_env  # noqa: F401 - isolate configuration before bot imports

from decimal import Decimal as D
import types
import unittest

import sniper_live as L
from paper_trade import (BookSnapshot, MarketRules, PaperBroker, PaperRejected,
                         estimate_fak_shares)
import tests_sniper_ioc as I


class FakBudgetMath(unittest.TestCase):
    RULES = MarketRules('condition', D('.07'), 1, min_order_size=D(5),
                        tick_size=D('.01'), up_token_id='up', down_token_id='down')

    @staticmethod
    def book(price='.29', depth=100, tick='.01'):
        return BookSnapshot(token_id='up', asks=((D(price), D(depth)),),
                            bids=((D(price) - D(tick), D(100)),),
                            tick_size=D(tick), min_order_size=D(5))

    def test_recorded_incident_spends_signed_stake_and_can_exceed_target(self):
        quote = estimate_fak_shares(self.book(), 11, D('.34'), self.RULES)
        self.assertEqual(quote.notional, D('3.74'))
        self.assertAlmostEqual(float(quote.shares), 12.89655172413793)
        self.assertGreater(quote.shares, 11)
        self.assertEqual(quote.average_price, D('.29'))

    def test_partial_depth_can_exceed_target_without_spending_whole_budget(self):
        quote = estimate_fak_shares(self.book('.40', 12), 10, D('.55'), self.RULES)
        self.assertEqual((quote.shares, quote.notional), (D(12), D('4.80')))
        self.assertLess(quote.notional, D('5.50'))

    def test_arrival_tick_refresh_cannot_rebuild_the_signed_dollars(self):
        # The order signed 10*.55 on a .01 grid. Arrival metadata has become
        # coarser: a .1 grid caps execution at .5, but its stake remains5.50.
        rules = MarketRules('condition', D('.07'), 1, min_order_size=D(5),
                            tick_size=D('.1'))
        quote = PaperBroker._quote(self.book('.5', tick='.1'), D('5.50'),
                                   D('.55'), rules, 0, True, D(10))
        self.assertEqual((quote.shares, quote.notional), (D(11), D('5.50')))

    def test_subcent_signed_stake_cannot_be_rounded_up(self):
        rules = MarketRules('condition', D('.07'), 1, min_order_size=D(5),
                            tick_size=D('.001'))
        with self.assertRaisesRegex(PaperRejected, 'whole-cent'):
            estimate_fak_shares(self.book('.50', tick='.001'), 5, D('.501'), rules)


class LiveBudgetAccounting(unittest.TestCase):
    def setUp(self):
        self.trips = []
        self.ex = object.__new__(L.LiveExecution)
        self.ex._trip_fak = lambda a, reason: self.trips.append(reason)
        self.order = dict(amount=3.74, cap=.34, token='up', condition='condition',
                          size=11, kind=L.MATCHED, trade_ids={'t1'},
                          fee_rate=.07, fee_exponent=1)

    def fill(self, **changes):
        fields = dict(size=12.896552, price=.29, asset_id='up', market='condition',
                      side='BUY', status='MATCHED', trade_id='t1', paid=None,
                      maker_legs=None, first_seen=1)
        fields.update(changes)
        return L.LiveExecution._fak_fill(self.ex, 'order', self.order,
                                        [types.SimpleNamespace(**fields)])

    def test_recorded_price_improvement_is_booked_at_actual_quantity(self):
        result = self.fill()
        self.assertIsNotNone(result)
        self.assertEqual(result['shares'], 12.896552)
        self.assertAlmostEqual(result['cost'], 3.74, places=6)
        self.assertFalse(self.trips)
        self.assertTrue(self.order['quantity_resolved'])

    def test_partial_fill_above_target_is_still_partial_by_spend(self):
        self.order.update(amount=5.50, cap=.55, size=10)
        result = self.fill(size=12, price=.40)
        self.assertEqual((result['shares'], result['cost']), (12, 4.8))
        self.assertTrue(result['partial'])
        self.assertFalse(self.trips)

    def test_overspending_signed_dollars_still_halts(self):
        self.assertIsNone(self.fill(size=13))
        self.assertIn('above its budget', self.trips[-1])

    def test_price_or_identity_conflicts_still_halt(self):
        for fields in ({'price': .35}, {'asset_id': 'other'},
                       {'market': 'other'}, {'side': 'SELL'}):
            with self.subTest(fields=fields):
                self.assertIsNone(self.fill(**fields))
                self.assertIn('invalid fill', self.trips[-1])


class PaperBudgetBooking(I.PaperIOCBase):
    def test_broker_books_improved_price_for_full_signed_stake(self):
        self.m.set(I.UP, [(.29, 100)])
        before = self.broker.cash_balance()
        self.assertTrue(self.broker.place_trade(
            'UP', 3.74, I.UP, I.DOWN, self.m.condition_id, self.m.window_end,
            .34, fak=True, shares=11, latency_ms=0), self.broker.last_error)
        fill = self.broker.last_fill
        self.assertAlmostEqual(fill['shares'], 3.74 / .29)
        self.assertAlmostEqual(fill['average_price'], .29)
        self.assertAlmostEqual(before - self.broker.cash_balance(),
                               3.74 + fill['fee'])

    def test_partial_budget_is_reported_even_if_quantity_exceeds_target(self):
        self.m.set(I.UP, [(.40, 12)])
        self.assertTrue(self.broker.place_trade(
            'UP', 5.50, I.UP, I.DOWN, self.m.condition_id, self.m.window_end,
            .55, fak=True, shares=10, latency_ms=0), self.broker.last_error)
        fill = self.broker.last_fill
        self.assertEqual(fill['shares'], 12)
        self.assertTrue(fill['partial'])
        self.assertAlmostEqual(fill['stake'], 5.50)


if __name__ == '__main__':
    unittest.main()
