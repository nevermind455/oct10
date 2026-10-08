#!/usr/bin/env python3
"""The audit's open findings F1-F5, fixed or bounded, and real-time paper mode.

    python tests_f_fixes.py

F1  a dollar-sized IOC can buy more shares than its target: documented,
    measured, flagged (over_target); every DOLLAR limit is proven hard
F2  the ledger journal: O(1) fill booking, exact recovery after a crash,
    torn/corrupt/gapped journals, snapshot + journal never double-applied
F3  the live canary's session budget survives restarts (and crash loops),
    reconciled with the ledger's durable authorizations; fails closed
F4  one process per ledger, one live process per wallet (real subprocesses)
F5  settlement: the payout slot of each token is proven on chain; without
    the proof a result settles only when the mirrors agree; RESULT_MISMATCH
PAPER  the firewall (every live order path refused and counted), source-
    tagged fills and per-source P/L, the manager's restart restore

No network, no wallet, no .env. Expected values are computed here.
"""
from __future__ import annotations

import asyncio
import collections
import json
import os
import pathlib
import random
import subprocess
import sys
import tempfile
import time
import types
import unittest
from decimal import Decimal as D
from fractions import Fraction as F
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import position_manager as PM  # noqa: E402
import sniper_live as L  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from accounting import ledger as LG  # noqa: E402
from accounting import settlement as ST  # noqa: E402
from accounting.ledger import Ledger, current_payload  # noqa: E402
from accounting.resolution import RESOLVED, UNKNOWN, Resolution  # noqa: E402
from paper_trade import MarketRules, PaperBroker  # noqa: E402


# =========================================================================== F1
class F1DollarIOC(I.PaperIOCBase):
    """Target 10 shares at a $0.50 ask: the stake is $5.00 whatever happens next."""

    def test_the_ask_falls_to_049_040_030(self):
        for ask in (0.49, 0.40, 0.30):
            self.sn.last_fire_by_side["UP"] = -1e18
            before = self.broker.cash_balance()
            r = self.fire("UP", [(0.50, 100)], [(ask, 100)])
            self.assertEqual((r["target_shares"], r["amount"]), (10.0, 5.0))
            want = F(5) / F(str(ask))                                         # the oracle: the whole stake at the ask
            self.assertAlmostEqual(r["shares"], float(want), places=4)
            self.assertAlmostEqual(r["over_target"], float(want - 10), places=3)
            self.assertAlmostEqual(before - self.broker.cash_balance(), 5.0, places=6)   # dollars: exactly the stake
        self.assertEqual(self.sn.n["fills above target (ask fell)"], 3)

    def test_partial_depth_tick_and_minimum(self):
        r = self.fire("UP", [(0.50, 100)], [(0.30, 6), (0.60, 100)])        # 6 shares under the cap, then above it
        self.assertAlmostEqual(r["shares"], 6.0, places=6)
        self.assertAlmostEqual(r["cost"], 1.80, places=6)
        self.assertNotIn("over_target", r)
        self.assertAlmostEqual(self.sn.carry["UP"], 4.0, places=6)          # never above target: 10 - 6
        # minimum order size: the stake buys >= 5 shares at the LIMIT (the order as signed)
        self.sn.last_fire_by_side["DOWN"] = -1e18
        r = self.fire("DOWN", [(0.90, 100)], [(0.90, 100)])
        self.assertGreaterEqual(r["amount"] / r["cap"], 5.0 - 1e-9)

    def test_the_signed_dollars_are_the_stake_exactly(self):
        """What makes the dollar limits hard: makerAmount (USDC) IS the stake."""
        from py_clob_client_v2.order_builder.builder import ROUNDING_CONFIG, OrderBuilder
        from py_clob_client_v2.order_builder.constants import BUY
        import sniper_mode as S
        for stake, cap, tick in ((5.00, 0.55, "0.01"), (9.99, 0.355, "0.001"), (1.00, 0.99, "0.01")):
            _s, maker, taker = OrderBuilder.get_market_order_amounts(
                None, BUY, S.signing_amount(stake), S.signing_price(cap, tick), ROUNDING_CONFIG[tick])
            self.assertEqual(D(maker), D(str(stake)) * 1_000_000)               # never a cent more
            self.assertGreaterEqual(D(taker) / 1_000_000, D(str(stake)) / D(str(cap)) - D("0.0001"))   # a FLOOR
        lim = L.limits_from_env({"SNIPER_CANARY_MAX_SPEND_USD": "10"})
        lim.exposure_usd = 5.0
        lim.check(5.0, 10.0, "c", L.KillSwitch(None))                         # exactly the dollar cap: allowed
        with self.assertRaises(L.Halted):
            lim.check(5.01, 10.0, "c", L.KillSwitch(None))                    # a cent over: halted

    def test_the_effective_config_says_share_caps_are_estimates(self):
        import exec_latency
        lines = "\n".join(exec_latency.effective_config_lines(self.sn.s, live=True, canary_max_shares=25))
        self.assertIn("ESTIMATES", lines)
        self.assertIn("hard caps", lines)


class F1Fees(I.PaperIOCBase):
    def setUp(self):
        super().setUp()
        m = self.m
        self.broker._rules_fetch = lambda cid: MarketRules(cid, D("0.07"), 1, min_order_size=D("5"),
                                                          tick_size=D("0.01"), up_token_id=m.up_token,
                                                          down_token_id=m.down_token)

    def test_fees_follow_the_shares_actually_bought(self):
        r = self.fire("UP", [(0.50, 100)], [(0.40, 100)])
        shares = F(5) / F("0.40")
        fee = shares * F("0.07") * F("0.40") * F("0.60")
        self.assertAlmostEqual(r["fee"], float(fee), places=4)
        self.assertAlmostEqual(r["cost"], 5.0 + float(fee), places=4)


# =========================================================================== F2
def lots(led):
    return {k: [(l.trade_id, l.shares, l.price, l.fee, l.wall, l.source, l.order_id) for l in p.lots]
            for k, p in led.positions.items()}


def state(led):
    return (lots(led), {k: (p.settled, p.payout_per_share, p.realized, p.settled_wall)
                        for k, p in led.positions.items()}, sorted(led.seen))


class F2Journal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "l.json")

    def tearDown(self):
        self.tmp.cleanup()

    def new(self):
        led = Ledger(path=self.path)
        if not os.path.exists(self.path):
            led.save()
        return led

    def test_random_operations_survive_a_crash_at_any_point(self):
        rng = random.Random(41)
        for case in range(120):
            for f in (self.path, self.path + ".journal"):
                if os.path.exists(f):
                    os.unlink(f)
            led = self.new()
            led.JOURNAL_COMPACT_EVERY = rng.choice((3, 7, 500))
            markets = {f"0x{m:064x}": (f"{m}1", f"{m}2") for m in range(1, 4)}
            result, tids = {}, []
            for step in range(rng.randrange(3, 30)):
                cid = rng.choice(list(markets))
                tok = rng.choice(markets[cid])
                r = rng.random()
                if r < 0.65:
                    tid = rng.choice(tids) if tids and rng.random() < 0.1 else f"t{case}-{step}"
                    tids.append(tid)
                    led.record_fill_durable(tid, tok, shares=rng.choice((0.01, 1.01, 4.25, 10.37)),
                                            price=rng.randrange(1, 99) / 100, condition_id=cid,
                                            fee=rng.randrange(0, 50) / 1000, source=f"paper_live_book/{rng.choice(('sniper', 'manager'))}")
                elif r < 0.85:
                    win = result.setdefault(cid, rng.choice((0, 1)))
                    led.settle(Resolution(cid, RESOLVED, {markets[cid][0]: float(win == 0),
                                                          markets[cid][1]: float(win == 1)}, "x"))
                elif r < 0.92:
                    led.save()
                # the crash: a fresh process reads the files as they are now
                again = Ledger(path=self.path)
                self.assertEqual(state(again), state(led), (case, step))
                self.assertAlmostEqual(again.summary()["realized_pnl"], led.summary()["realized_pnl"], places=9)

    def test_torn_corrupt_gapped_and_orphaned_journals(self):
        led = self.new()
        led.record_fill_durable("a", "11", shares=1.0, price=0.5, condition_id="0x" + "1" * 64, fee=0.0)
        led.record_fill_durable("b", "11", shares=2.0, price=0.5, condition_id="0x" + "1" * 64, fee=0.0)
        good = open(self.path + ".journal", "rb").read()
        with open(self.path + ".journal", "ab") as fh:                     # a torn final append: never acknowledged
            fh.write(b'{"seq": 3, "op": "fill", "trade_id": "c"')
        self.assertEqual(sorted(Ledger(path=self.path).seen), ["a", "b"])
        lines = good.split(b"\n")
        open(self.path + ".journal", "wb").write(b"garbage\n" + lines[1] + b"\n")   # damaged in the middle
        with self.assertRaises(RuntimeError):
            Ledger(path=self.path)
        open(self.path + ".journal", "wb").write(lines[1] + b"\n")               # entry 1 lost: a gap
        with self.assertRaises(RuntimeError):
            Ledger(path=self.path)
        open(self.path + ".journal", "wb").write(good)
        os.unlink(self.path)                                                       # the snapshot gone, the journal left
        with self.assertRaises(RuntimeError) as ctx:
            Ledger(path=self.path)
        self.assertIn("ledger file is missing", str(ctx.exception))

    def test_a_crash_between_snapshot_and_truncation_never_double_applies(self):
        led = self.new()
        led.record_fill_durable("a", "11", shares=1.0, price=0.5, condition_id="0x" + "1" * 64, fee=0.0)
        journal = open(self.path + ".journal", "rb").read()
        led.save()                                                  # snapshot (journal_seq 1) + truncate
        open(self.path + ".journal", "wb").write(journal)          # ... as if the truncate never happened
        again = Ledger(path=self.path)
        self.assertEqual(again.positions["11"].shares, 1.0)        # once, not twice
        self.assertEqual(again.journal_replayed, 0)

    def test_compaction_and_replayed_times(self):
        led = self.new()
        led.JOURNAL_COMPACT_EVERY = 5
        walls = []
        for i in range(12):
            led.record_fill_durable(f"t{i}", "11", shares=1.0, price=0.5, condition_id="0x" + "1" * 64, fee=0.0)
            walls.append(led.positions["11"].lots[-1].wall)
        self.assertEqual(led.journal_pending(), 2)                 # compacted at 5 and 10
        again = Ledger(path=self.path)
        self.assertEqual([l.wall for l in again.positions["11"].lots], walls)   # the original lot times
        self.assertEqual(current_payload(self.path)["seen"], sorted(f"t{i}" for i in range(12)))

    def test_booking_does_not_grow_with_the_ledger(self):
        led = self.new()
        for i in range(3000):                                          # a big history, saved once
            led.record_fill(f"h{i}", f"{i}", shares=1.0, price=0.5, condition_id="0x" + f"{i + 1:064x}", fee=0.0)
        led.save()

        def book(n0):
            xs = []
            for k in range(30):
                t = time.perf_counter()
                led.record_fill_durable(f"x{n0}-{k}", "big", shares=1.0, price=0.5, condition_id="0x" + "f" * 64,
                                        fee=0.0)
                xs.append(time.perf_counter() - t)
            return sorted(xs)[15]
        big = book(1)
        small_dir = tempfile.mkdtemp()
        small = Ledger(path=os.path.join(small_dir, "s.json"))
        small.save()
        xs = []
        for k in range(30):
            t = time.perf_counter()
            small.record_fill_durable(f"y{k}", "s", shares=1.0, price=0.5, condition_id="0x" + "e" * 64, fee=0.0)
            xs.append(time.perf_counter() - t)
        self.assertLess(big, max(sorted(xs)[15] * 4, 0.01))          # one fsync either way, not a rewrite


# =========================================================================== F3
COND = "0x" + "c" * 64


def authorize(led, oid, end, notional=5.0, cond=COND):
    led.authorize_order(oid, {"condition_id": cond, "token_id": "11", "window_end": end,
                              "requested_notional": notional})
    led.save()


class F3CanarySession(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        self.led = Ledger(path=str(self.dir / "live_ledger.json"))
        self.led.save()
        self.state = self.dir / "live_canary_state.json"
        self.now = (int(time.time()) // 300) * 300 + 60

    def tearDown(self):
        self.tmp.cleanup()

    def boot(self, *, new=False, wallet="0xabc", now=None):
        lim, kill = L.limits_from_env({}), L.KillSwitch(None)
        s = L.CanarySession(self.state, L.CanarySession.wallet_key_for(wallet))
        line = s.open(lim, kill, self.led, new_session=new, now=self.now if now is None else now)
        return s, lim, kill, line

    def test_normal_restart_restores_every_counter(self):
        s, lim, kill, line = self.boot()
        self.assertIn("new canary session", line)
        lim.record_send(5.0, COND)
        lim.record_outcome(L.MATCHED, 5.0)
        s.save(lim, kill)
        s2, lim2, _k, line2 = self.boot()
        self.assertIn("restored", line2)
        self.assertEqual((s2.session_id, lim2.orders, lim2.fills, lim2.exposure_usd, lim2.rounds),
                         (s.session_id, 1, 1, 5.0, {COND}))
        self.assertEqual(s2.started_wall, s.started_wall)

    def test_a_crash_before_the_state_was_written_is_rebuilt_from_the_ledger(self):
        s, lim, kill, _ = self.boot()
        end = (int(self.now) // 300 + 1) * 300
        for i in range(3):                                   # three orders the ledger proves; the file never saw them
            authorize(self.led, f"o{i}", end, 7.5)
        self.led.record_fill_durable("f0", "11", shares=5.0, price=0.5, condition_id=COND, fee=0.0, order_id="o0")
        _s2, lim2, _k, _ = self.boot()
        self.assertEqual((lim2.orders, lim2.exposure_usd, lim2.fills), (3, 22.5, 1))   # restart during pending orders

    def test_halts_and_the_loss_window_survive_and_crash_loops_never_reset(self):
        s, lim, kill, _ = self.boot()
        kill.trip("canary daily loss $25.00 >= $20.00")
        kill.trip("shutting down")                           # transient: not carried over
        s.save(lim, kill)
        for _ in range(5):                                   # repeated automatic restarts
            s, lim, kill, _ = self.boot()
            self.assertEqual(kill.reasons, ["canary daily loss $25.00 >= $20.00"])
            with self.assertRaises(L.Halted):
                lim.check(1.0, 5.0, COND, kill)
            lim.record_send(1.0, COND)
            s.save(lim, kill)
        self.assertEqual(lim.orders, 5)

    def test_fails_closed(self):
        self.state.write_text("{not json", encoding="utf-8")
        with self.assertRaises(RuntimeError):                # corrupted
            self.boot()
        self.state.write_text(json.dumps({"version": 1, "session_id": "x", "wallet": "w", "started_wall": -1,
                                          "orders": 0, "fills": 0, "exposure_usd": 0, "rounds": [],
                                          "kill_reasons": []}), encoding="utf-8")
        with self.assertRaises(RuntimeError):                # malformed values
            self.boot()
        self.state.unlink()
        authorize(self.led, "recent", (int(self.now) // 300 + 1) * 300)
        with self.assertRaises(RuntimeError) as ctx:         # deleted while live orders are recent
            self.boot()
        self.assertIn("--new-canary-session", str(ctx.exception))
        s, lim, _k, line = self.boot(new=True)              # deliberate: a new budget, counted from now
        self.assertIn("new canary session", line)
        _s, _l, _k, _ = self.boot()
        with self.assertRaises(RuntimeError):                # another wallet's state
            self.boot(wallet="0xdef")
        self.state.write_text(json.dumps({**json.loads(self.state.read_text()), "wallet": "someone-else"}))
        with self.assertRaises(RuntimeError):
            self.boot()

    def test_a_new_session_keeps_the_old_file(self):
        self.boot()
        self.boot(new=True)
        self.assertTrue(list(self.dir.glob("live_canary_state.json.*.bak")))


# =========================================================================== F4
class F4OneProcess(unittest.TestCase):
    # testing_env strips PAPER_* / POLY_* from the inherited environment: the
    # child sets its own settings after it, before anything reads them
    CODE = ("import json, os, sys, time; sys.path.insert(0, %r); import testing_env\n"
            "os.environ.update(json.loads(sys.argv[1])); import run\n"
            "locks = run.ownership_locks(paper=%r)\nprint('ACQUIRED', flush=True)\ntime.sleep(6)\n")

    def spawn(self, env, paper):
        return subprocess.Popen([sys.executable, "-c", self.CODE % (str(ROOT), paper), json.dumps(env)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_two_paper_bots_on_one_ledger_and_two_live_bots_on_one_wallet(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = {}
            cases = (({"PAPER_LEDGER_PATH": os.path.join(tmp, "p.json")}, True, "the ledger"),
                     ({"LEDGER_PATH": os.path.join(tmp, "a.json"), "POLY_FUNDER": "0x" + "1" * 40,
                       "POLY_SIGNATURE_TYPE": "1", "SNIPER_WALLET_LOCK_DIR": os.path.join(tmp, "locks")},
                      False, "this wallet"))
            for extra, paper, what in cases:
                env = dict(base, **extra)
                first = self.spawn(env, paper)
                self.assertEqual(first.stdout.readline().strip(), "ACQUIRED")
                second_env = dict(env)
                if not paper:                                      # another folder, another ledger: same wallet
                    second_env["LEDGER_PATH"] = os.path.join(tmp, "b.json")
                second = subprocess.run([sys.executable, "-c", self.CODE % (str(ROOT), paper),
                                         json.dumps(second_env)], capture_output=True, text=True, timeout=60)
                first.kill()
                first.wait()
                self.assertNotIn("ACQUIRED", second.stdout)
                self.assertIn(f"another bot process is already trading {what}", second.stderr)


# =========================================================================== F5
class F5Settlement(unittest.TestCase):
    def test_result_mismatch_is_raised_and_never_settled(self):
        async def go():
            with tempfile.TemporaryDirectory() as tmp:
                led = Ledger(path=os.path.join(tmp, "l.json"))
                cid, up, dn = "0x" + "9" * 64, "91", "92"
                led.record_fill("t", up, shares=10, price=0.4, condition_id=cid, fee=0.0)
                events = []
                fetch = lambda c: Resolution(c, UNKNOWN, {}, detail="RESULT_MISMATCH: on chain {...} but mirrors {...}")
                w = ST.SettlementWorker(led, interval=0.02, fetch=fetch, clock=lambda: 2_000_000.0,
                                        poll_active=False, tick_s=0.01, on_event=lambda t, m, lv: events.append(m))
                w.track(cid, 1_999_700, 1_999_800, up_token=up, down_token=dn)
                for _ in range(20):
                    await w.tick()
                    await asyncio.sleep(0.01)
                await w.stop()
                self.assertTrue(any(e.startswith("RESULT_MISMATCH") for e in events), events)
                self.assertFalse(led.positions[up].settled)
                self.assertEqual(w.records[cid].state, ST.PENDING)
                self.assertEqual(w.health_status(), "ERROR")        # an alarm, not a quiet wait
        asyncio.run(go())

    def test_a_restart_during_settlement_settles_exactly_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "l.json")
            led = Ledger(path=path)
            led.save()
            cid = "0x" + "8" * 64
            led.record_fill_durable("t", "81", shares=10, price=0.4, condition_id=cid, fee=0.1)
            res = Resolution(cid, RESOLVED, {"81": 1.0, "82": 0.0}, "Up")
            led.settle(res)                                      # durable in the journal...
            again = Ledger(path=path)                            # ...the process dies before any snapshot
            self.assertTrue(again.positions["81"].settled)
            self.assertAlmostEqual(again.positions["81"].realized, 10 - 4.1)
            self.assertEqual(again.settle(res), [])              # a second notification after restart: nothing
            self.assertAlmostEqual(again.summary()["realized_pnl"], 10 - 4.1)


# =========================================================================== PAPER
class PaperFirewall(unittest.TestCase):
    def test_every_live_order_path_refuses_and_is_counted(self):
        code = r'''
import sys, asyncio; sys.path.insert(0, %r)
import testing_env, run, polymarket_trade, sniper_live, fast_h2
from py_clob_client_v2.client import ClobClient
paths = run.install_paper_firewall()
tries = [lambda: ClobClient.post_order(None, None), lambda: ClobClient.create_market_order(None, None),
         lambda: ClobClient.create_and_post_order(None, None), lambda: polymarket_trade.place_trade("UP", 5),
         lambda: polymarket_trade.sell_shares("1", 1), lambda: sniper_live.LeanSender.start(None, None),
         lambda: sniper_live.LeanSender.send(None, None), lambda: fast_h2.FastH2.post_now(None, "u", b"", {}),
         lambda: polymarket_trade._get_client()]
refused = 0
for t in tries:
    try:
        t()
    except RuntimeError:
        refused += 1
print("PATHS", len(paths), "REFUSED", refused, "COUNTED", sum(run.LIVE_ORDER_CALLS.values()))
'''
        out = subprocess.run([sys.executable, "-c", code % str(ROOT)], capture_output=True, text=True,
                             timeout=120).stdout
        self.assertIn("REFUSED 9", out, out)
        self.assertIn("COUNTED 8", out, out)          # _get_client refuses by the paper switch itself
        self.assertIn("PATHS 18", out, out)


class PaperAttribution(I.PaperIOCBase):
    def test_fills_carry_their_source_and_pnl_splits_by_source(self):
        import run
        self.fire("UP", [(0.47, 100)])                                        # the sniper: 10 @ 0.47
        m = self.m
        ok = self.broker.place_trade("DOWN", 2.5, m.up_token, m.down_token, m.condition_id, m.window_end, 0.52,
                                     None, latency_ms=0, tag="manager", fak=False)
        self.assertTrue(ok)
        led = self.broker.ledger
        sources = {l.source for p in led.positions.values() for l in p.lots}
        self.assertEqual(sources, {"paper_live_book/sniper", "paper_live_book/manager"})
        led.settle(Resolution(m.condition_id, RESOLVED, {m.up_token: 1.0, m.down_token: 0.0}, "Up"))
        with led._lock:
            a = run._attribution(led, "", {})
        self.assertAlmostEqual(a["sniper"]["realized"], 10 - 4.70, places=6)
        down = led.positions[m.down_token]
        self.assertAlmostEqual(a["manager"]["realized"], -down.cost, places=6)
        self.assertEqual((a["sniper"]["wins"], a["manager"]["losses"]), (1, 1))
        self.assertAlmostEqual(a["sniper"]["realized"] + a["manager"]["realized"], led.summary()["realized_pnl"],
                               places=9)


class StopRequest(unittest.TestCase):
    def test_a_stop_file_asks_for_a_clean_shutdown_and_is_removed(self):
        import run
        with tempfile.TemporaryDirectory() as tmp:
            req = pathlib.Path(tmp) / "bot.lock.stop"
            rt = types.SimpleNamespace(stop=types.SimpleNamespace(is_set=lambda: False), sniper_task=None,
                                       stop_request=req)

            async def go():
                waiter = asyncio.create_task(run._wait_until_stopped(rt))
                await asyncio.sleep(0.6)
                self.assertFalse(waiter.done())                         # no file: keeps running
                req.write_text("stop", encoding="utf-8")
                return await asyncio.wait_for(waiter, 3)
            self.assertIsNone(asyncio.run(go()))                        # a clean stop, not a failure
            self.assertFalse(req.exists())


class ManagerRestore(unittest.TestCase):
    def test_per_market_limits_come_back_after_a_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            j = pathlib.Path(tmp) / "pm.jsonl"
            now = time.time()
            rows = [{"wall": now - 50, "event": "POSITION_MANAGER BUY UP", "condition": "c1", "run_id": "r1",
                     "reservation_id": 1, "side": "UP", "capital_max": 4.0},
                    {"wall": now - 49, "event": "POSITION_MANAGER RESULT UP", "condition": "c1", "run_id": "r1",
                     "reservation_id": 1, "status": "FILLED", "cost": 3.9, "actual_price": 0.39},
                    {"wall": now - 40, "event": "POSITION_MANAGER BUY DOWN", "condition": "c1", "run_id": "r2",
                     "reservation_id": 1, "side": "DOWN", "capital_max": 5.0},          # no result: the crash
                    {"wall": now - 9000, "event": "POSITION_MANAGER BUY UP", "condition": "old", "run_id": "r0",
                     "reservation_id": 1, "side": "UP", "capital_max": 9.0}]
            j.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
            hub = types.SimpleNamespace(up_token=None, down_token=None, condition_id=None, window_end=None, book=None)
            pm = PM.PositionManager(PM.PMConfig(enabled=True), hub=hub, ledger=Ledger(path=os.path.join(tmp, "l.json")),
                                    broker=types.SimpleNamespace())
            self.addCleanup(pm.journal.close)
            out = pm.restore_from_journal(j)
            h = pm._hist_for("c1")
            self.assertEqual((h.orders, h.sides["UP"].orders, h.sides["DOWN"].orders), (2, 1, 1))
            self.assertAlmostEqual(h.spend, 3.9 + 5.0)                       # the unknown one at its reservation
            self.assertEqual(h.sides["UP"].last_buy_price, 0.39)
            self.assertNotIn("old", out)


if __name__ == "__main__":
    unittest.main(verbosity=1)
