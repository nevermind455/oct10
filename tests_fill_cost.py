"""What a fill really cost: the maker legs, not the venue's headline price.

    python -m unittest -v tests_fill_cost

The live canary halted on 10-04 on its own fill: a $1.00 buy of 10.322 shares,
matched against three makers on the complementary token at 0.91 / 0.90 / 0.90
(i.e. ours at 0.09 / 0.10 / 0.10 - exactly $1.00), which the venue reported as
size 10.322 at price 0.10. The ledger compared price x size ($1.0322) with the
order's $1.00 (+ $0.02) and refused it as a mismatch.
"""
import tempfile
import types
import unittest

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L
from accounting import Ledger
from feeds.poly_user import Fill, FillStore, taker_paid

# The venue's own record of that trade (trade ...d649d7bf69), legs as the REST API returned them.
REAL_LEGS = [{"asset_id": "DOWN", "matched_amount": "3.22", "price": "0.91"},
             {"asset_id": "DOWN", "matched_amount": "7", "price": "0.9"},
             {"asset_id": "DOWN", "matched_amount": "0.102", "price": "0.9"}]


class TakerPaidTests(unittest.TestCase):
    def test_the_real_trade_cost_exactly_its_dollar(self):
        self.assertEqual(taker_paid("UP", REAL_LEGS, 10.322), 1.0)

    def test_makers_on_our_own_token_are_priced_as_is(self):
        legs = [{"asset_id": "UP", "matched_amount": "4", "price": "0.50"},
                {"asset_id": "UP", "matched_amount": "6", "price": "0.51"}]
        self.assertAlmostEqual(taker_paid("UP", legs, 10.0), 4 * 0.50 + 6 * 0.51)

    def test_anything_missing_or_inconsistent_is_unknown(self):
        for name, asset, legs, size in (
                ("no legs", "UP", None, 10.322), ("empty", "UP", [], 10.322), ("no asset", None, REAL_LEGS, 10.322),
                ("legs do not add up to the size", "UP", REAL_LEGS, 11.0),
                ("a leg without its token", "UP", [{"matched_amount": "10.322", "price": "0.9"}], 10.322),
                ("a bad price", "UP", [{"asset_id": "DOWN", "matched_amount": "10.322", "price": "1.5"}], 10.322),
                ("a bad size", "UP", [{"asset_id": "DOWN", "matched_amount": "-1", "price": "0.9"}], -1.0)):
            with self.subTest(name):
                self.assertIsNone(taker_paid(asset, legs, size))


class FillStoreTests(unittest.TestCase):
    def test_the_store_keeps_the_legs_cost_beside_the_headline_price(self):
        st = FillStore()
        st.record_trade("t1", order_id="o1", asset_id="UP", market="C", side="BUY", price="0.1", size="10.322",
                        status="MATCHED", maker_orders=REAL_LEGS)
        f = st.fills["t1"]
        self.assertEqual((f.price, f.paid), (0.1, 1.0))
        self.assertAlmostEqual(f.notional, 1.0322)
        self.assertAlmostEqual(f.paid_notional, 1.0)

    def test_a_replay_without_legs_neither_loses_the_cost_nor_conflicts(self):
        st = FillStore()
        st.record_trade("t1", order_id="o1", asset_id="UP", market="C", side="BUY", price="0.1", size="10.322",
                        status="MATCHED", maker_orders=REAL_LEGS)
        st.record_trade("t1", order_id="o1", asset_id="UP", market="C", side="BUY", price="0.1", size="10.322",
                        status="CONFIRMED", source="reconcile")
        f = st.fills["t1"]
        self.assertEqual((f.status, f.paid, f.conflict_reason), ("CONFIRMED", 1.0, None))

    def test_the_legs_arriving_later_fill_the_cost_in(self):
        st = FillStore()
        st.record_trade("t1", order_id="o1", asset_id="UP", market="C", side="BUY", price="0.1", size="10.322",
                        status="MATCHED")
        self.assertIsNone(st.fills["t1"].paid)
        self.assertAlmostEqual(st.fills["t1"].paid_notional, 1.0322)
        st.record_trade("t1", order_id="o1", asset_id="UP", market="C", side="BUY", price="0.1", size="10.322",
                        status="CONFIRMED", maker_orders=REAL_LEGS)
        self.assertEqual(st.fills["t1"].paid, 1.0)


class LedgerAuthorizationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.led = Ledger(path=f"{self.tmp.name}/ledger.json")
        self.auth = {"token_id": "UP", "condition_id": "C", "requested_notional": 1.0}

    def tearDown(self):
        self.tmp.cleanup()

    def fill(self, paid):
        return Fill(trade_id="t1", asset_id="UP", market="C", side="BUY", price=0.1, size=10.322,
                    status="CONFIRMED", order_id="o1", paid=paid)

    def test_the_halting_fill_now_matches_its_order(self):
        self.assertTrue(self.led._fill_matches_authorization(self.fill(1.0), "o1", self.auth))

    def test_without_its_legs_it_is_still_judged_on_price_x_size(self):
        self.assertFalse(self.led._fill_matches_authorization(self.fill(None), "o1", self.auth))

    def test_a_real_overfill_is_still_refused(self):
        self.assertFalse(self.led._fill_matches_authorization(self.fill(1.5), "o1", self.auth))
        self.assertFalse(self.led._fill_matches_authorization(self.fill(1.0), "o1",
                                                               {**self.auth, "token_id": "DOWN"}))


class LiveFillPriceTests(unittest.TestCase):
    def test_the_live_journal_price_is_the_legs_average(self):
        st = FillStore()
        st.record_trade("t1", order_id="o1", asset_id="UP", market="C", side="BUY", price="0.1", size="10.322",
                        status="MATCHED", maker_orders=REAL_LEGS)
        got = L.LiveExecution.fill_for(types.SimpleNamespace(fills=st, fak_orders={}), "o1")
        self.assertAlmostEqual(got["cost"], 1.0)
        self.assertAlmostEqual(got["price"], 1.0 / 10.322, places=6)


if __name__ == "__main__":
    unittest.main()
