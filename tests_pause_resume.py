"""Five-minute live pauses and stepped budgets, using fake clocks and local files."""
from __future__ import annotations

import asyncio
import collections
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import testing_env  # noqa: F401 - never load the bot's credentials
import sniper_live as L
from accounting import Ledger
from tests_execution_weakness_fixes import prepared


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class PauseTiming(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.kill = L.KillSwitch(mono=self.clock)

    def test_each_live_incident_expires_at_exactly_five_minutes(self):
        for reason in ("Polymarket trouble", "order outcome unknown", "disk full", "unexplained fill",
                       "books disagree", "canary daily loss $50.00 >= $50.00"):
            with self.subTest(reason=reason):
                self.kill.trip(reason)
                self.clock.advance(299.999)
                with self.assertRaisesRegex(L.Halted, reason.replace("$", r"\$")):
                    self.kill.check()
                self.clock.advance(.001)
                self.kill.check()
                self.assertFalse(self.kill.tripped)

    def test_repeated_reports_do_not_extend_an_active_pause(self):
        self.kill.trip("venue down")
        self.clock.advance(299)
        self.kill.trip("venue down")
        self.clock.advance(1)
        self.kill.check()
        self.assertEqual(self.kill.resumed, [("venue down", 300)])

    def test_persistent_condition_pauses_once_but_a_distinct_order_can_pause(self):
        self.kill.trip_once(("unconfirmed", "order-1"), "unconfirmed order-1")
        self.clock.advance(300)
        for _ in range(20):
            self.kill.trip_once(("unconfirmed", "order-1"), "unconfirmed order-1")
            self.kill.check()
        self.kill.trip_once(("unconfirmed", "order-2"), "unconfirmed order-2")
        with self.assertRaisesRegex(L.Halted, "order-2"):
            self.kill.check()

    def test_overlapping_incidents_keep_their_own_deadlines(self):
        self.kill.trip("disk")
        self.clock.advance(60)
        self.kill.trip("venue")
        self.clock.advance(240)
        self.assertEqual(self.kill.active(), ["venue"])
        self.clock.advance(60)
        self.kill.check()

    def test_stop_live_holds_while_present_and_releases_when_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            stop = Path(tmp) / "STOP_LIVE"
            kill = L.KillSwitch(str(stop), mono=self.clock)
            stop.touch()
            self.assertTrue(kill.tripped)                  # status and dispatch agree
            for _ in range(3):
                self.clock.advance(1000)
                with self.assertRaisesRegex(L.Halted, "stop file present"):
                    kill.check()
            kill.trip("another incident")
            stop.unlink()
            self.assertEqual(kill.active(), ["another incident"])
            self.clock.advance(300)
            kill.check()

    def test_resume_callback_failure_does_not_hold_trading(self):
        self.kill.on_resume = lambda *args: (_ for _ in ()).throw(OSError("journal unavailable"))
        self.kill.trip("disk")
        self.clock.advance(300)
        self.kill.check()


class SteppedBudgets(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.kill = L.KillSwitch(mono=self.clock)

    def test_cumulative_caps_unlock_one_original_increment_after_each_wait(self):
        cases = (
            ("max_orders", {"max_orders": 2, "orders": 2}, dict(amount=1, condition="c"), 2),
            ("max_fills", {"max_fills": 2, "fills": 1, "pending_fills": 1}, dict(amount=1, condition="c"), 2),
            ("max_rounds", {"max_rounds": 2, "rounds": {"a", "b"}}, dict(amount=1, condition="c"), 2),
            ("max_exposure_usd", {"max_exposure_usd": 50, "exposure_usd": 49}, dict(amount=2, condition="c"), 50),
            ("max_daily_loss_usd", {"max_daily_loss_usd": 50},
             dict(amount=1, condition="c", realized_loss_usd=50), 50),
        )
        for field, kwargs, check_args, step in cases:
            with self.subTest(cap=field):
                clock = Clock()
                kill = L.KillSwitch(mono=clock)
                limits = L.CanaryLimits(**kwargs)
                before = getattr(limits, field)
                with self.assertRaises(L.Halted):
                    limits.check(shares=1, kill=kill, **check_args)
                self.assertEqual(getattr(limits, field), before)
                for _ in range(10):
                    with self.assertRaises(L.Halted):
                        limits.check(shares=1, kill=kill, **check_args)
                self.assertEqual(getattr(limits, field), before, "repeated checks must not spend future increments")
                clock.advance(299.999)
                with self.assertRaises(L.Halted):
                    limits.check(shares=1, kill=kill, **check_args)
                clock.advance(.001)
                limits.check(shares=1, kill=kill, **check_args)
                self.assertEqual(getattr(limits, field), before + step)
                limits.check(shares=1, kill=kill, **check_args)
                self.assertEqual(getattr(limits, field), before + step)

    def test_loss_budget_far_over_cap_requires_a_separate_wait_for_each_step(self):
        limits = L.CanaryLimits(max_daily_loss_usd=50)
        for allowed in (50, 100, 150):
            with self.assertRaises(L.Halted):
                limits.check_loss_budget(155, self.kill)
            self.assertEqual(limits.max_daily_loss_usd, allowed)
            self.clock.advance(300)
        limits.check_loss_budget(155, self.kill)
        self.assertEqual(limits.max_daily_loss_usd, 200)

    def test_twice_reached_loss_limit_repeats_the_original_fifty_dollar_step(self):
        limits = L.CanaryLimits(max_daily_loss_usd=50)
        with self.assertRaises(L.Halted):
            limits.check(1, 1, "c", self.kill, realized_loss_usd=50)
        self.clock.advance(300)
        limits.check(1, 1, "c", self.kill, realized_loss_usd=50)
        self.assertEqual(limits.max_daily_loss_usd, 100)
        with self.assertRaises(L.Halted):
            limits.check(1, 1, "c", self.kill, realized_loss_usd=100)
        self.clock.advance(300)
        limits.check(1, 1, "c", self.kill, realized_loss_usd=100)
        self.assertEqual(limits.max_daily_loss_usd, 150)

    def test_order_size_policy_still_refuses_without_changing_size(self):
        limits = L.CanaryLimits(max_order_shares=10)
        with self.assertRaises(L.Refused):
            limits.check(5, 11, "c", self.kill)
        self.assertEqual(limits.max_order_shares, 10)
        self.assertFalse(self.kill.tripped)


class PersistentAccounting(unittest.TestCase):
    def test_persistent_disk_or_integrity_error_pauses_once(self):
        for attr in ("integrity_error", "persistence_halt"):
            with self.subTest(error=attr):
                clock = Clock()
                ex = L.LiveExecution.__new__(L.LiveExecution)
                ex.sender = types.SimpleNamespace(kill=L.KillSwitch(mono=clock))
                ex.ledger = types.SimpleNamespace(**{attr: "same disk or book conflict"})
                ex._tripped_once, ex._announced = set(), set()
                ex.n = collections.Counter()
                ex.on_event = lambda *args: None
                with self.assertRaises(L.Halted):
                    ex._check_accounting_integrity()
                clock.advance(300)
                for _ in range(20):
                    ex._check_accounting_integrity()
                    ex.kill.check()

    def test_unconfirmed_order_stays_recorded_but_never_pauses_twice(self):
        clock = Clock()
        ex = L.LiveExecution.__new__(L.LiveExecution)
        ex.sender = types.SimpleNamespace(kill=L.KillSwitch(mono=clock))
        ex._tripped_once, ex._announced = set(), set()
        ex.on_event = lambda *args: None
        first = {"client_order_id": "client-1", "reserve": 5.35, "quantity_resolved": False}
        ex.fak_orders = {"order-1": first}
        ex.fill_for = lambda oid: None
        self.assertTrue(ex.blocks_pending_fak("order-1"))
        clock.advance(300)
        for _ in range(20):
            self.assertFalse(ex.blocks_pending_fak("order-1"))
            ex._trip_fak(first, "same order now has contradictory fill evidence")
            ex.kill.check()
        self.assertIs(ex.fak_orders["order-1"], first)
        self.assertEqual(first["reserve"], 5.35)
        ex.fak_orders["order-2"] = {"client_order_id": "client-2", "quantity_resolved": False}
        self.assertTrue(ex.blocks_pending_fak("order-2"))


class DurablePauseState(unittest.TestCase):
    def test_restart_keeps_remaining_wait_and_does_not_rearm_a_persistent_cause(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(path=str(Path(tmp) / "ledger.json"))
            path = Path(tmp) / "session.json"
            wall, mono = Clock(2_000_000_000.0), Clock()
            sessions = []
            try:
                with patch.object(L.time, "time", side_effect=wall):
                    kill, limits = L.KillSwitch(mono=mono), L.CanaryLimits()
                    first = L.CanarySession(path, "offline-wallet")
                    sessions.append(first)
                    first.open(limits, kill, ledger)
                    kill.trip_once(("disk", "same failure"), "disk unavailable")
                    wall.advance(60)
                    mono.advance(60)
                    first.persist(limits, kill)
                    first.flush()
                    first.close()
                    wall.advance(60)                       # process was down for one minute
                    mono2 = Clock(10)
                    kill2, limits2 = L.KillSwitch(mono=mono2), L.CanaryLimits()
                    second = L.CanarySession(path, "offline-wallet")
                    sessions.append(second)
                    second.open(limits2, kill2, ledger)
                    mono2.advance(179.999)
                    with self.assertRaises(L.Halted):
                        kill2.check()
                    mono2.advance(.001)
                    kill2.check()                         # 300 elapsed in total, including downtime
                    kill2.trip_once(("disk", "same failure"), "disk unavailable")
                    kill2.check()
            finally:
                for session in sessions:
                    session.close()

    def test_restart_preserves_the_original_cap_step_and_pending_wait(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(path=str(Path(tmp) / "ledger.json"))
            path = Path(tmp) / "session.json"
            wall, mono = Clock(2_000_000_000.0), Clock()
            sessions = []
            try:
                with patch.object(L.time, "time", side_effect=wall):
                    kill, limits = L.KillSwitch(mono=mono), L.CanaryLimits(max_daily_loss_usd=50)
                    first = L.CanarySession(path, "offline-wallet")
                    sessions.append(first)
                    first.open(limits, kill, ledger)
                    with self.assertRaises(L.Halted):
                        limits.check_loss_budget(51, kill)
                    first.persist(limits, kill)
                    first.flush()
                    first.close()
                    wall.advance(300)
                    mono2 = Clock(10)
                    kill2, limits2 = L.KillSwitch(mono=mono2), L.CanaryLimits(max_daily_loss_usd=50)
                    second = L.CanarySession(path, "offline-wallet")
                    sessions.append(second)
                    second.open(limits2, kill2, ledger)
                    limits2.check_loss_budget(51, kill2)
                    self.assertEqual(limits2.max_daily_loss_usd, 100)
                    with self.assertRaises(L.Halted):
                        limits2.check_loss_budget(101, kill2)
                    mono2.advance(300)
                    limits2.check_loss_budget(101, kill2)
                    self.assertEqual(limits2.max_daily_loss_usd, 150)
            finally:
                for session in sessions:
                    session.close()


class RestartReservations(unittest.IsolatedAsyncioTestCase):
    async def test_unresolved_previous_run_stays_in_loss_budget_after_pause_expires(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(path=str(Path(tmp) / "ledger.json"))
            path = Path(tmp) / "session.json"
            old = L.CanarySession(path, "offline-wallet")
            old_limits = L.CanaryLimits(max_daily_loss_usd=50)
            old_kill = L.KillSwitch()
            old.open(old_limits, old_kill, ledger)
            p = prepared()
            p.order_hash = "0x" + "ab" * 32
            old.before_send(p, old_limits, old_kill)
            old.close()

            clock = Clock()
            kill, limits = L.KillSwitch(mono=clock), L.CanaryLimits(max_daily_loss_usd=50)
            session = L.CanarySession(path, "offline-wallet")
            session.open(limits, kill, ledger)
            sender = types.SimpleNamespace(kill=kill, limits=limits, rules=types.SimpleNamespace(),
                                           guard=L.VenueGuard(kill, mono=clock), http=types.SimpleNamespace(),
                                           clock=types.SimpleNamespace(refresh=AsyncMock(return_value=True)))
            ex = L.LiveExecution(sender=sender, shim=types.SimpleNamespace(invalidate=lambda *args: None),
                                 ledger=ledger, receipt_sink=lambda *args: None, balance_fetch=lambda: 100,
                                 clock_url="http://memory.invalid/time")
            ex.session = session
            ex.balance = 100
            ex._read_balance = AsyncMock()
            try:
                await ex.start(types.SimpleNamespace())
                for task in ex._tasks:
                    task.cancel()
                await asyncio.gather(*ex._tasks, return_exceptions=True)
                self.assertAlmostEqual(ex._risk_budget_used(), p.reserve)
                clock.advance(300)
                kill.check()
                self.assertFalse(kill.tripped)
                self.assertEqual(len(session.unresolved()), 1)
                self.assertAlmostEqual(ex._risk_budget_used(), p.reserve)
                with self.assertRaises(L.Halted):
                    ex._check_loss_budget(50 - p.reserve + .01)
                self.assertEqual(limits.max_daily_loss_usd, 50)
            finally:
                for task in ex._tasks:
                    task.cancel()
                await asyncio.gather(*ex._tasks, return_exceptions=True)
                session.close()


if __name__ == "__main__":
    unittest.main(verbosity=1)
