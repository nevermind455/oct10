"""Terminal FAILED FAKs refund their own provisional strategy state, offline.

Actual reconciliation, strategy and quality methods run against in-memory
orders/fills. No signing SDK, credentials, venue calls or ledger writes.
"""
import testing_env  # noqa: F401 - before config or any bot imports

import collections
import os
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

import exec_latency
import sniper_live as L
from feeds.book import BookView
from sniper_mode import Settings, Sniper


class PendingFakMarketScope(unittest.TestCase):
    """A pending quantity belongs to its original market and outcome token."""

    def setUp(self):
        self.end = time.time() + 120
        book = types.SimpleNamespace(view=lambda token: BookView(
            token=str(token), generation=1, status="LIVE", asks=((.50, 100),),
            updated_mono=time.monotonic(), tick_size=.01))
        self.hub = types.SimpleNamespace(up_token="new-up", down_token="new-down",
                                         condition_id="new-condition", window_end=self.end, book=book)
        broker = types.SimpleNamespace(min_buy_price=.05, max_buy_price=.95)
        with patch.dict(os.environ, {"SNIPER_LATENCY_MS": "20", "SNIPER_DELAY_SOURCE": "model",
                                     "SNIPER_FAIR": "0", "SNIPER_ORDER_TYPE": "FAK",
                                     "SNIPER_ORDER_SIZING": "shares"}):
            self.sn = Sniper(self.hub, broker, settings=Settings())
        self.rows = []
        self.sn._write = self.rows.append
        # Exercise the real live pending guard and five-minute pause, with no
        # signing, account, network or persistence dependencies.
        self.ex = object.__new__(L.LiveExecution)
        self.ex.sender = types.SimpleNamespace(kill=L.KillSwitch())
        self.ex.fill_for = Mock(return_value=None)
        self.ex.fak_orders = {}
        self.ex._tripped_once, self.ex._announced = set(), set()
        self.ex.on_event = Mock()
        self.sn.live = self.ex

    def pending(self, *, side="UP", condition="old-condition", token="old-up", end=None):
        row = {"side": side, "client_order_id": "pending-client"}
        plan = {"cond": condition, "token": token, "end": self.end - 300 if end is None else end}
        self.sn._fak_pending["pending-order"] = (row, plan)
        return row, plan

    def decide(self, side="UP"):
        return self.sn._decide(side, {"side": side, "bp": 1}, time.monotonic() * 1000)

    def test_old_market_pending_does_not_pause_new_market_on_either_side(self):
        for side, token in (("UP", "old-up"), ("DOWN", "old-down")):
            with self.subTest(side=side):
                pending = self.pending(side=side, token=token)
                plan = self.decide(side)
                self.assertIsNotNone(plan, self.rows)
                self.assertEqual(plan["token"], "new-up" if side == "UP" else "new-down")
                self.assertFalse(self.ex.kill.tripped)
                self.assertEqual(self.sn._fak_pending["pending-order"], pending)
                self.assertFalse(self.ex._tripped_once)

    def test_current_market_pending_still_blocks_and_pauses_its_side(self):
        self.pending(condition=self.hub.condition_id, token=self.hub.up_token, end=self.end)
        self.assertIsNone(self.decide())
        self.assertEqual(self.rows[-1]["reason"], "awaiting FAK fill size")
        self.assertTrue(self.ex.kill.tripped)
        self.assertIn("pending-order", self.sn._fak_pending)

    def test_current_market_opposite_pending_does_not_pause_this_side(self):
        self.pending(side="DOWN", condition=self.hub.condition_id, token=self.hub.down_token, end=self.end)
        self.assertIsNotNone(self.decide(), self.rows)
        self.assertFalse(self.ex.kill.tripped)

    def test_rotation_does_not_clear_an_existing_pause(self):
        self.pending()
        self.ex.kill.trip("existing incident")
        self.decide()
        self.assertEqual(self.ex.kill.reasons, ["existing incident"])

    def test_missing_current_round_does_not_pause_for_an_old_order(self):
        self.pending()
        self.hub.condition_id = None
        self.assertIsNone(self.decide())
        self.assertEqual(self.rows[-1]["reason"], "no current round")
        self.assertFalse(self.ex.kill.tripped)


class FailedFakStrategy(unittest.TestCase):
    def setUp(self):
        self.clock = [5.0]
        self.limits = L.CanaryLimits()
        self.limits.record_send(5.0, "other-order")
        self.limits.record_outcome(L.MATCHED, 5.0)
        self.limits.record_send(9.0, "condition")
        self.limits.record_outcome(L.MATCHED, 9.0)
        self.kill = L.KillSwitch(mono=lambda: self.clock[0])
        self.ex = object.__new__(L.LiveExecution)
        self.ex.sender = types.SimpleNamespace(limits=self.limits, kill=self.kill)
        self.ex._risk_lock = threading.RLock()
        self.ex._risk_orders = {"client-1": {"reserve": 9.63}}
        self.ex.session = None
        self.ex.n = collections.Counter()
        self.events = []
        self.ex.on_event = lambda *args: self.events.append(args)
        self.ex._tripped_once, self.ex._announced = set(), set()
        self.ex.ledger = types.SimpleNamespace(_lock=threading.RLock(), positions={})
        self.ex.confirm_within_s = 180.0
        self.ex.reconcile_every_s = 5.0
        self.ex.order_status = None
        self.trade = types.SimpleNamespace(
            trade_id="trade-1", order_id="order-1", status="FAILED", first_seen=1.0,
            asset_id="token", market="condition", side="BUY", size=10.0, price=0.90,
        )
        self.ex.fills = types.SimpleNamespace(fills={"trade-1": self.trade})
        self.accepted = {
            "wall": 0.0, "condition": "condition", "token": "token", "amount": 9.0,
            "kind": L.MATCHED, "client_order_id": "client-1", "authorized": True,
            "order_type": "FAK", "cap": 0.93, "fee_rate": 0.07, "fee_exponent": 1,
            "trade_ids": {"trade-1"}, "final_size": 10.0,
        }
        self.ex.awaiting = {"order-1": self.accepted}
        self.ex.fak_orders = {"order-1": self.accepted}

        self.sn = object.__new__(Sniper)
        self.sn.live = self.ex
        self.rd = {"fills": 3, "UP": 4.0, "DOWN": 7.0}  # two other fills plus this reserved slot
        self.market = ("condition", 300.0)
        self.plan = {"rd": self.rd, "cond": "condition", "end": 300.0, "t0": time.monotonic(),
                     "carry_market": self.market, "target": 10.0}
        self.row = {"id": "decision-1", "side": "UP", "order_id": "order-1", "client_order_id": "client-1",
                    "ask_seen": 0.90, "kind": L.MATCHED, "result": "pending", "live": True}
        self.sn._fak_pending = {"order-1": (dict(self.row), self.plan)}
        self.sn._fak_reported = {}
        self.sn.n = collections.Counter()
        self.sn.carry_market = self.market
        self.sn.carry = {"UP": 0.0, "DOWN": 3.0}
        self.sn._round_fills = {(300.0, "UP"): {"shares": 4.0, "cost": 2.0, "last_wall": 1.0}}
        self.sn.on_event = lambda *args: self.events.append(args)
        self.rows = []
        self.sn._write = lambda row: self.rows.append(dict(row))
        self.table = []
        self.sn._table_row = lambda *args, **kwargs: self.table.append((args, kwargs))
        self.sn.quality = exec_latency.ExecQuality()
        self.sn.latency = exec_latency.LatencyStats()
        self.sn._observe_live(self.row)

    def reconcile(self):
        self.ex._reconcile_fak("order-1", self.accepted, self.clock[0])

    def prove_failure(self):
        self.trade.status = "FAILED"
        self.reconcile()
        self.clock[0] += self.ex.FAILED_RELEASE_S + 1.0
        self.reconcile()

    def report_match(self):
        self.trade.status = "MATCHED"
        self.sn._sync_live_fak()
        self.assertNotIn("order-1", self.sn._fak_pending)
        self.assertIn("order-1", self.sn._fak_reported)

    def assert_accounting_released_once(self):
        self.assertNotIn("order-1", self.ex.awaiting)
        self.assertNotIn("client-1", self.ex._risk_orders)
        self.assertEqual(self.limits.exposure_usd, 5.0)  # other order preserved
        self.assertEqual(self.limits.fills, 1)
        self.assertEqual(self.ex.n["failed trades released"], 1)

    def test_pending_failure_retires_slot_and_restores_full_target_on_event_loop(self):
        self.prove_failure()
        self.assert_accounting_released_once()
        # The worker published evidence; it has not touched strategy state.
        self.assertIn("order-1", self.sn._fak_pending)
        self.assertEqual(self.rd["fills"], 3)
        failure = self.ex.fill_for("order-1")
        self.assertEqual((failure["shares"], failure["cost"], failure["source"]),
                         (0.0, 0.0, "failed trade settlement"))
        self.assertTrue(failure["terminal_failure"])
        reasons = list(self.kill.reasons)
        self.sn._sync_live_fak()
        self.assertEqual(self.rd, {"fills": 2, "UP": 4.0, "DOWN": 7.0})
        self.assertEqual(self.sn.carry, {"UP": 10.0, "DOWN": 3.0})
        self.assertFalse(self.sn._fak_pending)
        self.assertFalse(self.sn._fak_reported)
        self.assertIn("FAILED", self.rows[-1]["reason"])
        self.assertNotIn("canceled", self.rows[-1]["reason"])
        self.assertEqual(self.rows[-1]["fill_statuses"], ["FAILED"])
        self.assertEqual(self.sn.quality.n["pending"], 0)
        self.assertEqual(self.sn.quality.n["zero_fill"], 1)
        self.assertEqual(self.kill.reasons, reasons)  # unrelated/existing pauses are never cleared
        for _ in range(3):
            self.reconcile()
            self.sn._sync_live_fak()
        self.assert_accounting_released_once()
        self.assertEqual((self.rd["fills"], self.sn.carry["UP"]), (2, 10.0))

    def test_matched_then_failed_retracts_only_its_inventory_cost_and_quality(self):
        self.report_match()
        self.assertEqual(self.rd["UP"], 14.0)
        self.assertEqual(self.sn.quality.n["filled"], 1)
        self.assertEqual(len(self.sn.quality.slips), 1)
        # A later order in the same market changed carry and added inventory.
        self.rd["fills"] += 1
        self.rd["UP"] += 6.0
        self.sn._round_fills[(300.0, "UP")]["shares"] += 6.0
        self.sn._round_fills[(300.0, "UP")]["cost"] += 3.5
        later_plan = dict(self.plan, target=8.0)
        self.sn._settle_carry(later_plan, "UP", 6.0)
        self.prove_failure()
        self.assertEqual(self.rd["UP"], 20.0)  # still untouched until loop synchronization
        self.sn._sync_live_fak()
        self.assert_accounting_released_once()
        self.assertEqual(self.rd, {"fills": 3, "UP": 10.0, "DOWN": 7.0})
        self.assertEqual(self.sn.carry, {"UP": 2.0, "DOWN": 3.0})
        self.assertEqual(self.sn._round_fills[(300.0, "UP")]["shares"], 10.0)
        self.assertAlmostEqual(self.sn._round_fills[(300.0, "UP")]["cost"], 5.5)
        self.assertEqual(self.sn.n["filled"], 0)
        self.assertEqual(self.sn.quality.n["filled"], 0)
        self.assertEqual(self.sn.quality.n["zero_fill"], 1)
        self.assertEqual(len(self.sn.quality.slips), 0)
        self.assertEqual(self.rows[-1]["retracted_shares"], 10.0)
        before = (dict(self.rd), dict(self.sn.carry), dict(self.sn.quality.n), len(self.rows))
        for _ in range(3):
            self.reconcile()
            self.sn._sync_live_fak()
        self.assertEqual(before, (self.rd, self.sn.carry, self.sn.quality.n, len(self.rows)))

    def test_late_failure_corrects_original_round_without_changing_new_round_carry(self):
        self.report_match()
        new_round = {"fills": 2, "UP": 20.0, "DOWN": 2.0}
        self.sn.rounds = {600.0: new_round}
        self.sn.carry_market = ("next-condition", 600.0)
        self.sn.carry = {"UP": 8.0, "DOWN": 9.0}
        self.sn._round_fills[(600.0, "UP")] = {"shares": 20.0, "cost": 12.0, "last_wall": 4.0}
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assertEqual(self.rd, {"fills": 2, "UP": 4.0, "DOWN": 7.0})
        self.assertEqual(new_round, {"fills": 2, "UP": 20.0, "DOWN": 2.0})
        self.assertEqual(self.sn.carry, {"UP": 8.0, "DOWN": 9.0})
        self.assertEqual(self.sn._round_fills[(600.0, "UP")]["cost"], 12.0)

    def test_partial_failure_preserves_other_pending_and_opposite_side_orders(self):
        self.trade.size = self.accepted["final_size"] = 6.0
        self.report_match()
        self.assertEqual(self.sn.carry["UP"], 4.0)
        self.assertEqual(self.sn.quality.n["partial"], 1)
        other_row = {"side": "DOWN", "order_id": "other-pending", "result": "pending", "live": True,
                     "kind": L.PENDING}
        self.sn._fak_pending["other-pending"] = (other_row, self.plan)
        self.sn.quality.observe(other_row)
        self.rd["fills"] += 2                      # a later UP fill and the opposite pending order
        self.rd["UP"] += 2.0
        self.rd["DOWN"] += 3.0
        self.sn._round_fills[(300.0, "UP")]["shares"] += 2.0
        self.sn._round_fills[(300.0, "UP")]["cost"] += 1.0
        self.sn._round_fills[(300.0, "DOWN")] = {"shares": 3.0, "cost": 1.5, "last_wall": 3.0}
        self.sn._settle_carry(dict(self.plan, target=3.0), "UP", 2.0)
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assertEqual(self.rd, {"fills": 4, "UP": 6.0, "DOWN": 10.0})
        self.assertEqual(self.sn.carry, {"UP": 1.0, "DOWN": 3.0})
        self.assertEqual(self.sn.quality.n["partial"], 0)
        self.assertEqual(self.sn.quality.n["pending"], 1)
        self.assertEqual(self.sn.quality.n["zero_fill"], 1)
        self.assertIn("other-pending", self.sn._fak_pending)
        self.assertAlmostEqual(self.sn._round_fills[(300.0, "UP")]["cost"], 3.0)
        self.assertEqual(self.sn._round_fills[(300.0, "DOWN")]["cost"], 1.5)

    def test_terminal_failure_as_first_quantity_is_classified_as_zero_fill(self):
        self.prove_failure()
        self.sn._fak_pending.clear()
        self.sn.quality = exec_latency.ExecQuality()
        self.sn._record_fak_fill(dict(self.row), self.plan, self.ex.fill_for("order-1"))
        self.assertEqual(self.sn.quality.n["zero_fill"], 1)
        self.assertEqual(self.sn.quality.n["rejected"], 0)
        self.assertEqual(self.sn.quality.n["filled"], 0)

    def test_failed_over_target_order_restores_only_the_target_it_consumed(self):
        self.plan["target"] = 7.0
        self.report_match()
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assertEqual(self.sn.carry["UP"], 7.0)

    def test_latest_partial_order_failure_restores_its_full_original_target(self):
        self.plan["target"] = 5.0
        self.trade.size = self.accepted["final_size"] = 3.0
        self.report_match()
        self.assertEqual(self.sn.carry["UP"], 2.0)
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assertEqual(self.sn.carry["UP"], 5.0)

    def test_later_over_target_fill_keeps_its_own_zero_carry_after_earlier_failure(self):
        self.plan["target"] = 5.0
        self.trade.size = self.accepted["final_size"] = 3.0
        self.report_match()
        self.assertEqual(self.sn.carry["UP"], 2.0)
        later_plan = dict(self.plan, target=7.0)
        self.sn._settle_carry(later_plan, "UP", 10.0)
        self.assertEqual(self.sn.carry["UP"], 0.0)
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assertEqual(self.sn.carry["UP"], 0.0)

    def test_kelly_order_failure_does_not_create_ioc_carry(self):
        self.plan["carry_market"] = None
        self.sn.carry["UP"] = 2.0
        self.report_match()
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assertEqual(self.sn.carry, {"UP": 2.0, "DOWN": 3.0})

    def test_missing_mixed_mismatched_or_booked_evidence_never_releases(self):
        scenarios = ("missing", "mixed", "identity", "booked")
        for scenario in scenarios:
            with self.subTest(scenario=scenario):
                self.setUp()
                if scenario == "missing":
                    self.accepted["trade_ids"].add("absent-trade")
                elif scenario == "mixed":
                    other = types.SimpleNamespace(**vars(self.trade))
                    other.trade_id, other.status = "confirmed-trade", "CONFIRMED"
                    self.ex.fills.fills[other.trade_id] = other
                    self.accepted["trade_ids"].add(other.trade_id)
                elif scenario == "identity":
                    self.trade.asset_id = "another-token"
                else:
                    self.ex._order_booked = lambda *args: True
                self.prove_failure()
                self.sn._sync_live_fak()
                self.assertIn("order-1", self.ex.awaiting)
                self.assertIn("client-1", self.ex._risk_orders)
                self.assertNotIn("failed_fill", self.accepted)
                self.assertIn("order-1", self.sn._fak_pending)
                self.assertEqual((self.rd["fills"], self.sn.carry["UP"]), (3, 0.0))
                self.assertEqual(self.limits.exposure_usd, 14.0)

    def test_size_evidence_without_reply_trade_ids_still_requires_complete_failure(self):
        self.accepted["trade_ids"] = set()
        self.trade.size = 6.0
        self.prove_failure()
        self.assertNotIn("failed_fill", self.accepted)
        self.trade.size = 10.0
        self.prove_failure()
        self.sn._sync_live_fak()
        self.assert_accounting_released_once()
        self.assertFalse(self.sn._fak_pending)

    def test_confirmed_quantity_retires_provisional_tracking_without_a_refund(self):
        self.report_match()
        self.trade.status = "CONFIRMED"
        self.sn._sync_live_fak()
        self.assertFalse(self.sn._fak_reported)
        self.assertEqual(self.rd, {"fills": 3, "UP": 14.0, "DOWN": 7.0})
        self.assertEqual(self.sn.quality.n["filled"], 1)
        self.assertEqual(self.limits.exposure_usd, 14.0)

    def test_worker_reconciliation_does_not_mutate_strategy(self):
        self.report_match()
        self.trade.status = "FAILED"
        errors = []
        def reconcile_in_worker():
            try:
                self.reconcile()
                self.clock[0] += self.ex.FAILED_RELEASE_S + 1.0
                self.reconcile()
            except BaseException as exc:
                errors.append(exc)
        worker = threading.Thread(target=reconcile_in_worker, daemon=True)
        worker.start()
        worker.join(timeout=5.0)
        self.assertFalse(worker.is_alive(), "worker reconciliation did not finish")
        self.assertFalse(errors, errors)
        self.assertEqual(self.rd["UP"], 14.0)
        self.assertEqual(self.rd["fills"], 3)
        self.sn._sync_live_fak()
        self.assertEqual((self.rd["UP"], self.rd["fills"]), (4.0, 2))


class FailedFillQuality(unittest.TestCase):
    def test_retraction_preserves_other_orders_pending_counts_and_slippage(self):
        quality = exec_latency.ExecQuality()
        original = {"order_id": "failed", "result": "filled", "partial": True,
                    "price": 0.51, "ask_seen": 0.50, "shares": 5.0}
        quality.observe(original)
        quality.observe({"order_id": "pending", "result": "pending"})
        quality.observe({"order_id": "other", "result": "filled", "price": 0.52, "ask_seen": 0.50})
        quality.retract_fill(original, {"shares": 0.0, "result": "live_fill_update"})
        self.assertEqual((quality.n["pending"], quality.n["partial"], quality.n["filled"], quality.n["zero_fill"]),
                         (1, 0, 1, 1))
        self.assertEqual(len(quality.slips), 1)
        self.assertAlmostEqual(quality.slips[0], 2.0)

    def test_retracting_an_aged_out_slip_does_not_remove_a_newer_equal_value(self):
        quality = exec_latency.ExecQuality()
        original = {"order_id": "old", "result": "filled", "price": 0.51, "ask_seen": 0.50}
        quality.observe(original)
        for i in range(1000):
            quality.observe(dict(original, order_id=f"later-{i}"))
        quality.retract_fill(original, {"shares": 0.0, "result": "live_fill_update"})
        self.assertEqual(len(quality.slips), 1000)
        self.assertEqual(len(quality._slip_ids), 1000)


if __name__ == "__main__":
    unittest.main()
