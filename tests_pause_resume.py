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

    def test_fresh_incident_reusing_a_reason_after_expiry_is_not_dropped(self):
        self.kill.trip("consecutive order errors")
        self.clock.advance(300)                         # no status/check observed expiry
        self.kill.trip("consecutive order errors")
        with self.assertRaises(L.Halted):
            self.kill.check()
        self.assertEqual(self.kill.resumed, [("consecutive order errors", 300)])
        self.clock.advance(300)
        self.kill.check()

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


class VenuePauseRegressions(unittest.TestCase):
    def test_new_venue_incident_after_expiry_starts_a_fresh_pause(self):
        clock = Clock()
        guard = L.VenueGuard(L.KillSwitch(mono=clock), mono=clock)
        guard.pause_for(300, "first incident")
        clock.advance(300)                         # no check/probe observed expiry yet
        guard.pause_for(300, "new incident")
        with self.assertRaises(L.Refused):
            guard.check()
        clock.advance(299)
        self.assertEqual(guard.on_probe(False), guard.PAUSED)
        clock.advance(1)
        guard.check()

    def test_pause_at_monotonic_zero_reports_its_actual_age(self):
        clock = Clock(0)
        guard = L.VenueGuard(L.KillSwitch(mono=clock), mono=clock)
        guard.pause_for(300, "incident")
        clock.advance(20)
        with self.assertRaisesRegex(L.Refused, "PAUSED for 20s"):
            guard.check()

    def test_announcement_failure_cannot_interrupt_pause_or_recovery(self):
        clock = Clock()
        def broken_event(*args):
            raise OSError("event log unavailable")
        guard = L.VenueGuard(L.KillSwitch(mono=clock), mono=clock, on_event=broken_event)
        self.assertEqual(guard.on_reply(6000), guard.PAUSED)
        with self.assertRaises(L.Refused):
            guard.check()
        clock.advance(300)
        guard.check()
        self.assertEqual(guard.state, guard.HEALTHY)


class OrderTaskPauseRegressions(unittest.TestCase):
    def test_distinct_failed_tasks_pause_even_when_their_object_ids_are_reused(self):
        import sniper_mode as S
        class FailedTask:
            def cancelled(self):
                return False
            def exception(self):
                return RuntimeError("order worker failed")
            def get_name(self):
                return "order-worker"
        clock = Clock()
        kill = L.KillSwitch(mono=clock)
        sniper = S.Sniper.__new__(S.Sniper)
        first, second = FailedTask(), FailedTask()
        sniper._tasks = {first, second}
        sniper._order_tasks = {first, second}
        sniper._closing = False
        sniper.n = collections.Counter()
        sniper.live = types.SimpleNamespace(kill=kill)
        sniper._write = sniper.on_event = lambda *args: None
        with patch.object(S, "id", return_value=123, create=True):
            sniper._task_done(first)
            self.assertTrue(kill.tripped)
            clock.advance(300)
            kill.check()
            sniper._task_done(second)
            self.assertTrue(kill.tripped)
        self.assertFalse(sniper._closing)


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
            # 2026-10-23 (H2): its token keeps waiting while the size is unknown
            # (it used to be released here) - but it never pauses everything twice
            self.assertTrue(ex.blocks_pending_fak("order-1"))
            ex._trip_fak(first, "same order now has contradictory fill evidence")
            ex.kill.check()
        self.assertIs(ex.fak_orders["order-1"], first)
        self.assertEqual(first["reserve"], 5.35)
        ex.fak_orders["order-2"] = {"client_order_id": "client-2", "quantity_resolved": False}
        self.assertTrue(ex.blocks_pending_fak("order-2"))


class DurablePauseState(unittest.TestCase):
    def test_restart_does_not_start_a_second_pause_for_the_same_unknown_order(self):
        for elapsed, legacy in ((60, False), (300, False), (60, True), (300, True)):
            with self.subTest(elapsed=elapsed, legacy=legacy), tempfile.TemporaryDirectory() as tmp:
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
                        p = prepared()
                        first.before_send(p, limits, kill)
                        key = ((first.PREVIOUS_RUN, p.client_order_id) if legacy else
                               ("order-uncertain", str(p.client_order_id)))
                        kill.trip_once(key, "outcome unknown")
                        first.persist(limits, kill)
                        first.flush()
                        first.close()
                        wall.advance(elapsed)
                        mono2 = Clock(10)
                        kill2, limits2 = L.KillSwitch(mono=mono2), L.CanaryLimits()
                        second = L.CanarySession(path, "offline-wallet")
                        sessions.append(second)
                        second.open(limits2, kill2, ledger)
                        if elapsed < 300:
                            with self.assertRaises(L.Halted):
                                kill2.check()
                        mono2.advance(300 - elapsed)
                        kill2.check()
                        self.assertEqual(len(second.unresolved()), 1)
                        self.assertEqual(second.unresolved()[0][1]["reserve"], p.reserve)
                        kill2.trip_once(("order-uncertain", str(p.client_order_id)), "same order still unknown")
                        kill2.check()
                        kill2.trip_once(("order-uncertain", "different-order"), "new order unknown")
                        with self.assertRaisesRegex(L.Halted, "new order unknown"):
                            kill2.check()
                finally:
                    for session in sessions:
                        session.close()

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
