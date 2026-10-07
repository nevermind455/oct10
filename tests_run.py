#!/usr/bin/env python3
"""Tests for run.py, the sniper-only runner, and the regressions that ship
without a test of their own in spe8 (aa3adc7, 7eedf06).

    python tests_run.py

No network: the hub's sockets, discovery, the CLOB clock and the sniper's
trigger socket are replaced by fakes; the paper broker, ledger and Sniper
are the real ones, in a temporary directory.
"""
from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import run  # noqa: E402
import sniper_mode  # noqa: E402


@contextlib.contextmanager
def env(**values):
    """Set (str) or unset (None) environment variables for the block."""
    saved = {k: os.environ.get(k) for k in values}
    try:
        for k, v in values.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextlib.contextmanager
def patched(obj, **attrs):
    saved = {k: getattr(obj, k) for k in attrs}
    try:
        for k, v in attrs.items():
            setattr(obj, k, v)
        yield
    finally:
        for k, v in saved.items():
            setattr(obj, k, v)


SNIPER_ENV = {"SNIPER_ONLY": "1", "SNIPER_LIVE_CANARY": None, "SNIPER_PRIMARY_FEED": None,
              "SNIPER_KRAKEN_LIVE": None}


# ---------------------------------------------------------------- live gate
class LiveGateTests(unittest.TestCase):
    def test_sniper_only_is_required_in_both_modes(self):
        with env(**{**SNIPER_ENV, "SNIPER_ONLY": None}):
            for paper in (True, False):
                with self.assertRaisesRegex(RuntimeError, "SNIPER_ONLY=1"):
                    run._live_sniper_gate(paper)

    def test_paper_passes_and_ignores_the_canary_flag(self):
        with env(**{**SNIPER_ENV, "SNIPER_LIVE_CANARY": "1"}):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertFalse(run._live_sniper_gate(True))
            self.assertIn("ignored in paper", out.getvalue())

    def test_live_needs_the_canary_flag(self):
        with env(**SNIPER_ENV):
            with self.assertRaisesRegex(RuntimeError, "refusing to start LIVE.*SNIPER_LIVE_CANARY=1"):
                run._live_sniper_gate(False)
        with env(**{**SNIPER_ENV, "SNIPER_LIVE_CANARY": "1"}):
            self.assertTrue(run._live_sniper_gate(False))

    def test_kraken_trigger_trades_live_only_with_the_explicit_opt_in(self):
        with env(**{**SNIPER_ENV, "SNIPER_LIVE_CANARY": "1", "SNIPER_PRIMARY_FEED": "kraken"}):
            with self.assertRaisesRegex(RuntimeError, "PAPER only.*SNIPER_KRAKEN_LIVE=1"):
                run._live_sniper_gate(False)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(run._live_sniper_gate(True))       # paper on Kraken is fine
        with env(**{**SNIPER_ENV, "SNIPER_LIVE_CANARY": "1", "SNIPER_PRIMARY_FEED": "kraken",
                    "SNIPER_KRAKEN_LIVE": "1"}):
            self.assertTrue(run._live_sniper_gate(False))
        with env(**{**SNIPER_ENV, "SNIPER_PRIMARY_FEED": "kraken", "SNIPER_KRAKEN_LIVE": "1"}):
            with self.assertRaisesRegex(RuntimeError, "SNIPER_LIVE_CANARY"):
                run._live_sniper_gate(False)                      # the opt-in is not the canary flag

    def test_a_refused_live_start_touches_nothing(self):
        with tempfile.TemporaryDirectory() as tmp, env(**SNIPER_ENV, BOT_LOCK_PATH=f"{tmp}/live.lock"):
            with self.assertRaisesRegex(RuntimeError, "SNIPER_LIVE_CANARY"):
                asyncio.run(run.run(paper=False))
            self.assertFalse(pathlib.Path(tmp, "live.lock").exists(), "the lock was taken before the gate")

    def test_old_strategy_settings_are_named_as_ignored(self):
        got = run.ignored_settings({"STOP_LOSS_ENABLED": "1", "BOOK_SOURCE": "ws", "BTC_FEED": "",
                                    "FLIP_EXIT_ENABLED": "0"})
        self.assertEqual(len(got), 2, got)
        self.assertTrue(any(line.startswith("STOP_LOSS_ENABLED='1' ignored") for line in got), got)


# ---------------------------------------------------------------- locks
HOLDER = """
import sys, time
sys.path.insert(0, sys.argv[1])
import testing_env
import run
lock = run._ProcessLock(__import__("pathlib").Path(sys.argv[2]))
lock.acquire()
print("held", flush=True)
time.sleep(30)
"""


class LockTests(unittest.TestCase):
    def test_lock_contention_across_processes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / ".btc_bot_live.lock"
            holder = subprocess.Popen([sys.executable, "-c", HOLDER, str(ROOT), str(path)],
                                      stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(holder.stdout.readline().strip(), "held")
                with self.assertRaisesRegex(RuntimeError, "another bot may be running"):
                    run._ProcessLock(path).acquire()
                self.assertTrue(run.lock_held_elsewhere(path))
                with env(OTHER_BOT_LOCKS=str(path), ALLOW_OTHER_BOT=None):
                    own = pathlib.Path(tmp) / "own.lock"
                    with self.assertRaisesRegex(RuntimeError, "stop it before starting LIVE"):
                        run.check_other_bots(paper=False, own_lock=own)
                    with self.assertRaisesRegex(RuntimeError, "ALLOW_OTHER_BOT=1"):
                        run.check_other_bots(paper=True, own_lock=own)
                    with env(ALLOW_OTHER_BOT="1"), contextlib.redirect_stdout(io.StringIO()):
                        run.check_other_bots(paper=True, own_lock=own)       # paper may, when told
                    with self.assertRaisesRegex(RuntimeError, "LIVE"):
                        with env(ALLOW_OTHER_BOT="1"):
                            run.check_other_bots(paper=False, own_lock=own)  # live never may
                    run.check_other_bots(paper=False, own_lock=path)          # our own lock is not "another"
            finally:
                holder.kill()
                holder.wait()
                holder.stdout.close()
            self.assertFalse(run.lock_held_elsewhere(path), "a dead holder still reads as running")
            lock = run._ProcessLock(path)
            lock.acquire()
            lock.release()

    def test_probe_never_creates_a_lock_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = pathlib.Path(tmp) / "nothing.lock"
            self.assertFalse(run.lock_held_elsewhere(missing))
            self.assertFalse(missing.exists())
            with env(OTHER_BOT_LOCKS=str(missing)):
                run.check_other_bots(paper=False, own_lock=pathlib.Path(tmp) / "own.lock")

    def test_a_lock_this_user_cannot_open_is_unknown_and_blocks_live(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "other.lock"
            path.write_text("", encoding="utf-8")

            def denied(*_a, **_k):
                raise PermissionError(13, "Permission denied")

            with patched(os, open=denied):
                self.assertIsNone(run.lock_held_elsewhere(path))
                with env(OTHER_BOT_LOCKS=str(path)):
                    with self.assertRaisesRegex(RuntimeError, "cannot tell"):
                        run.check_other_bots(paper=False, own_lock=pathlib.Path(tmp) / "own.lock")
                    run.check_other_bots(paper=True, own_lock=pathlib.Path(tmp) / "own.lock")

    def test_the_default_other_install_is_the_old_bot(self):
        with env(OTHER_BOT_LOCKS=None):
            locks = run._other_bot_locks()
        if os.name == "posix":
            self.assertIn(pathlib.Path("/opt/btcbot/.btc_bot_live.lock"), locks)
        else:
            self.assertEqual(locks, [])


# ---------------------------------------------------------------- paper wiring
class FakeSocketsHub(run.FeedHub):
    """The real FeedHub with its sockets left closed: start() records what
    the runner asked for."""

    def start(self, *, user=True, binance=True):
        self.started_with = {"user": user, "binance": binance}
        return []


def _tokens(window):
    return {"window_start": window, "window_end": window + 300, "up_token_id": "111",
            "down_token_id": "222", "condition_id": "0x" + "c" * 64}


class PaperWiringTests(unittest.TestCase):
    def test_paper_wiring_with_a_fake_hub(self):
        """The real startup and shutdown, on a hub whose sockets never open."""
        import http_pool
        import market_discovery
        import polymarket_trade
        import timer

        async def idle_sniper(_self):
            await asyncio.Event().wait()

        with tempfile.TemporaryDirectory() as tmp, \
                env(**SNIPER_ENV, SNIPER_DELAY_SOURCE="model", SNIPER_STALL_MS="0",
                    PAPER_LEDGER_PATH=f"{tmp}/pl.json", PAPER_ACCOUNT_PATH=f"{tmp}/pa.json",
                    PAPER_AUDIT_PATH=f"{tmp}/po.jsonl"), \
                patched(sniper_mode.Sniper, run=idle_sniper), \
                patched(timer, check_clock=lambda *a, **k: (True, "clock synchronized", 0.01)), \
                patched(http_pool, warm=lambda *hosts, **k: len(hosts),
                        dns_redirect_report=lambda *a, **k: ("ok", "")), \
                patched(market_discovery, get_tokens_for_current_round=lambda w=None: _tokens(
                    timer.window_start() if w is None else w)):
            hub, cfg = run.build_hub(paper=True)
            hub.__class__ = FakeSocketsHub
            rt = run.Runtime(paper=True, hub=hub, cfg=cfg)
            out = io.StringIO()

            async def go():
                task = asyncio.create_task(run.run_runtime(rt, paper_balance=250.0))
                for _ in range(200):
                    if rt.sniper_task is not None and hub.up_token:
                        break
                    await asyncio.sleep(0.02)
                state = {"started_with": getattr(hub, "started_with", None), "up": hub.up_token,
                         "tasks": sorted(t.get_name() for t in rt.tasks)}
                task.cancel()
                await task
                return state

            with contextlib.redirect_stdout(out):
                state = asyncio.run(go())
            printed = out.getvalue()

            self.assertEqual(state["started_with"], {"user": False, "binance": False},
                             "paper must open no private channel and no second BTC socket")
            self.assertEqual(state["up"], "111", "the rotation loop did not promote this round")
            self.assertIn("sniper", state["tasks"])
            self.assertIn("rotation", state["tasks"])
            self.assertIn("status", state["tasks"])               # no --dash: the 60s status line
            self.assertTrue(polymarket_trade.live_execution_disabled(), "paper left the live client usable")
            self.assertIsNone(rt.reconciler, "paper must not reconcile the wallet")
            self.assertIsNone(rt.live_exec)
            self.assertEqual(type(rt.broker).__name__, "PaperBroker")
            self.assertEqual(rt.broker.start_balance, 250.0)
            self.assertEqual(rt.journal_path, pathlib.Path(tmp) / "po_sniper.jsonl")
            self.assertTrue(rt.sniper.own_rounds)
            self.assertIs(rt.sniper.trade_rows, rt.trade_rows)
            self.assertTrue(pathlib.Path(tmp, "pl.json").exists(), "the ledger was not proven writable")
            self.assertEqual(rt.shutdown_log, ["sniper", "settler", "hub", "tasks", "drain", "report"])
            self.assertIn("MODE=PAPER", printed)
            self.assertIn("SNIPER paper-only", printed)
            report = json.loads(printed[printed.index("{", printed.index("final health")):])
            self.assertEqual(report["mode"], "PAPER")
            self.assertIn("sniper", report)
            self.assertIn("paper_account", report)


# ---------------------------------------------------------------- shutdown order
class Recorder:
    def __init__(self):
        self.calls = []

    def note(self, name):
        self.calls.append(name)


def _live_runtime(rec: Recorder):
    kill = types.SimpleNamespace(trip=lambda reason: rec.note(f"kill:{reason}"))

    async def stopper(name):
        rec.note(name)

    live_exec = types.SimpleNamespace(kill=kill, stop=lambda: stopper("live_exec.stop"),
                                      status=lambda: {"halted": ["shutting down"]})
    reconciler = types.SimpleNamespace(stop=lambda: stopper("reconciler.stop"), summary=lambda: {})
    settler = types.SimpleNamespace(stop=lambda: stopper("settler.stop"), summary=lambda: {})
    store = object()
    hub = types.SimpleNamespace(stop=lambda: stopper("hub.stop"), fill_store=store,
                                health=lambda: {}, book=types.SimpleNamespace(view=lambda t: None))
    hub.fill_store = types.SimpleNamespace(summary=lambda: {"fills": 0})

    class Ledger:
        def ingest_fill_store(self, s):
            rec.note("ledger.drain")
            return 0

        def save(self):
            return True

        def summary(self, mark=None):
            return {}

        def reconcile_balance(self):
            return {"status": "OK"}

    sniper = types.SimpleNamespace(summary=lambda: {"signals": 3})
    rt = run.Runtime(paper=False, hub=hub, cfg=run.FeedConfig(private=True))
    rt.live_exec, rt.reconciler, rt.settler, rt.ledger, rt.sniper = live_exec, reconciler, settler, Ledger(), sniper
    return rt


class ShutdownTests(unittest.TestCase):
    def test_live_shutdown_order(self):
        import polymarket_trade
        rec = Recorder()

        async def go():
            rt = _live_runtime(rec)

            async def sniper_forever():
                try:
                    await asyncio.Event().wait()
                finally:
                    rec.note("sniper.cancelled")
            rt.sniper_task = asyncio.create_task(sniper_forever(), name="sniper")
            rt.tasks.append(rt.sniper_task)
            other = asyncio.create_task(asyncio.Event().wait(), name="rotation")
            rt.tasks.append(other)
            polymarket_trade.set_order_observer(lambda receipt: True)
            polymarket_trade._telemetry_dir = ROOT
            await asyncio.sleep(0)
            with contextlib.redirect_stdout(io.StringIO()) as out:
                await run.shutdown(rt)
            return rt, out.getvalue(), other

        rt, printed, other = asyncio.run(go())
        self.assertEqual(rec.calls[0], "kill:shutting down", "no new orders must be the first thing")
        self.assertEqual(rec.calls[1:], ["sniper.cancelled", "live_exec.stop", "reconciler.stop", "settler.stop",
                                         "hub.stop", "ledger.drain"])
        self.assertEqual(rt.shutdown_log, ["kill", "sniper", "live_exec", "reconciler", "settler", "hub",
                                           "observer", "tasks", "drain", "report"])
        self.assertIsNone(polymarket_trade._order_observer, "the order observer outlived the run")
        self.assertIsNone(polymarket_trade._telemetry_dir)
        self.assertTrue(other.cancelled())
        report = json.loads(printed[printed.index("{"):])
        self.assertEqual(report["sniper"], {"signals": 3})
        self.assertEqual(report["mode"], "LIVE")
        self.assertIn("canary", report)

    def test_a_failed_live_start_undoes_the_observer_and_prints_no_report(self):
        import http_pool
        import polymarket_trade
        import sniper_live

        def cannot_build(**_kw):
            raise RuntimeError("no CLOB client")

        def no_client():
            raise RuntimeError("no client in tests")

        with tempfile.TemporaryDirectory() as tmp, env(**SNIPER_ENV, LEDGER_PATH=f"{tmp}/ledger.json"), \
                patched(sniper_live, build_live_execution=cannot_build), \
                patched(polymarket_trade, _get_client=no_client), \
                patched(http_pool, dns_redirect_report=lambda *a, **k: ("ok", "")):
            hub = run.FeedHub(creds={}, on_event=run.on_event)
            rt = run.Runtime(paper=False, hub=hub, cfg=run.FeedConfig(private=True))
            out = io.StringIO()
            with contextlib.redirect_stdout(out), self.assertRaisesRegex(RuntimeError, "no CLOB client"):
                asyncio.run(run.run_runtime(rt))
        self.assertIsNone(polymarket_trade._order_observer)
        self.assertIsNone(polymarket_trade._telemetry_dir)
        self.assertNotIn("final health", out.getvalue())

    def test_a_dead_sniper_task_ends_the_run(self):
        async def go():
            rt = types.SimpleNamespace(stop=threading.Event(), sniper_task=None)

            async def dies():
                raise ValueError("socket handler bug")
            rt.sniper_task = asyncio.create_task(dies())
            return await asyncio.wait_for(run._wait_until_stopped(rt), 3)

        with contextlib.redirect_stdout(io.StringIO()):
            exc = asyncio.run(go())
        self.assertIsInstance(exc, ValueError)


# ---------------------------------------------------------------- signals / exit
class ProcessExitTests(unittest.TestCase):
    def test_sigint_is_a_clean_exit_130(self):
        import signal as _signal
        run._STOP_SIGNAL.clear()

        async def main():
            loop = asyncio.get_running_loop()
            loop.call_later(0.05, lambda: _signal.raise_signal(_signal.SIGINT))
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                await asyncio.sleep(0.01)          # shutdown still gets to await
                raise

        self.assertEqual(run.run_quietly(main()), 130)
        self.assertEqual(run._STOP_SIGNAL, ["SIGINT"])

    @unittest.skipUnless(os.name == "posix", "SIGTERM delivery to self is POSIX only")
    def test_sigterm_is_a_clean_exit_143(self):
        import signal as _signal
        run._STOP_SIGNAL.clear()

        async def main():
            asyncio.get_running_loop().call_later(0.05, lambda: os.kill(os.getpid(), _signal.SIGTERM))
            await asyncio.sleep(5)

        self.assertEqual(run.run_quietly(main()), 143)

    def test_bad_settings_exit_1_and_are_recorded(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = pathlib.Path(tmp) / "bot_exit.log"
            with env(**{**SNIPER_ENV, "SNIPER_ONLY": None}), patched(run, ROOT=pathlib.Path(tmp)), \
                    contextlib.redirect_stderr(io.StringIO()) as err:
                code = run.main(["--paper"])
            self.assertEqual(code, 1)
            self.assertIn("SNIPER_ONLY=1", err.getvalue())
            self.assertIn("SNIPER_ONLY=1", log.read_text(encoding="utf-8"))


# ---------------------------------------------------------------- dashboard feed
class JournalTailTests(unittest.TestCase):
    def test_incremental_read_partial_lines_and_labels(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "j.jsonl"
            tail = run.JournalTail(path, since=0.9)
            self.assertEqual(tail.read(), [])                      # no file yet
            rows = [{"wall": 0.5, "side": "UP", "bp": 3.0, "result": "filled"},      # a previous run's row
                    {"wall": 1.0, "result": "start"},
                    {"wall": 2.0, "side": "UP", "bp": 0.9, "result": "skipped", "reason": "cooldown"},
                    {"wall": 3.0, "side": "UP", "bp": 1.1, "ask_seen": 0.47, "cap": 0.92, "result": "filled",
                     "shares": 10.0, "price": 0.48},
                    {"wall": 4.0, "side": "DOWN", "bp": -1.2, "ask_seen": 0.51, "cap": 0.95, "result": "refused",
                     "reason": "FAK no-fill: only $0.00 at or below 0.95"},
                    {"wall": 5.0, "side": "UP", "bp": 0.8, "result": "pending", "order_id": "0xabc"}]
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("".join(json.dumps(r) + "\n" for r in rows))
                fh.write('{"wall": 6.0, "result": "live_fill_update", "order_id": "0xabc", "resolved_result": '
                         '"filled", "shares": 4.2')                # a line still being written
            got = tail.read()
            self.assertEqual([r["result"] for r in got], ["filled", "missed", "pending"])
            self.assertEqual(got[0]["ask"], 0.47)
            self.assertEqual(got[0]["cap"], 0.92)
            self.assertEqual(got[0]["detail"], "", "a fill needs no detail")
            self.assertIn("no-fill", got[1]["detail"])
            self.assertEqual(tail.last_signal, (5.0, 0.8))
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(', "price": 0.5}\n')
            got = tail.read()
            self.assertEqual(len(got), 3, "a fill update must merge into its order, not add a row")
            self.assertEqual((got[2]["result"], got[2]["shares"], got[2]["price"]), ("filled", 4.2, 0.5))

    def test_a_big_journal_is_read_from_its_tail_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "j.jsonl"
            pad = {"wall": 1.0, "result": "summary", "blob": "x" * 900}
            with open(path, "w", encoding="utf-8") as fh:
                for _ in range(1000):                              # ~1MB of rows the table never shows
                    fh.write(json.dumps(pad) + "\n")
                fh.write(json.dumps({"wall": 2.0, "side": "UP", "bp": 1.0, "result": "filled"}) + "\n")
            t0 = time.perf_counter()
            got = run.JournalTail(path).read()
            self.assertLess(time.perf_counter() - t0, 0.5)
            self.assertEqual([r["result"] for r in got], ["filled"])


class DashViewTests(unittest.TestCase):
    def test_snapshot_from_a_real_sniper_and_fake_hub(self):
        from dashboard import build, glyphs
        now = time.time()

        class View:
            status, best_bid, best_ask, mid = "LIVE", 0.45, 0.47, 0.46
            asks = ((0.47, 100.0),)

            def __init__(self):
                self.updated_mono = time.monotonic() - 0.2

        hub = types.SimpleNamespace(
            up_token="111", down_token="222", condition_id="0xc", window_start=int(now // 300 * 300),
            window_end=int(now // 300 * 300) + 300, book=types.SimpleNamespace(view=lambda t: View()),
            health=lambda: {"book": {"status": "LIVE", "rest_resyncs": 1},
                            "poly_market": {"reconnect_count": 0}},
            fill_store=types.SimpleNamespace(summary=lambda: {"fills": 0}))
        broker = types.SimpleNamespace(min_buy_price=0.05, max_buy_price=0.95, trade_window_seconds=300.0,
                                       min_seconds_to_expiry=10.0)
        with env(**SNIPER_ENV, SNIPER_LATENCY_MS="50", SNIPER_DELAY_SOURCE="model"):
            sn = sniper_mode.Sniper(hub, broker)
        t = time.monotonic() * 1000.0
        sn.trigger.q.extend([(t - 300.0, 64000.0), (t, 64006.4)])   # +1bp over the window
        sn.n.update({"signals": 7, "orders": 2, "filled": 1, "refused": 1, "skip: cooldown": 5})
        rt = types.SimpleNamespace(mode="PAPER", paper=True, hub=hub, sniper=sn, ledger=None, broker=None,
                                   live_exec=None, reconciler=None, settler=None, journal_path=None,
                                   clock_ok=True, clock_detail="clock synchronized", started=now)
        snap = run.DashView(rt).snapshot()
        trig, market = snap["trigger"], snap["market"]
        self.assertAlmostEqual(trig["mid"], 64006.4)
        self.assertAlmostEqual(trig["move_bp"], 1.0, places=6)
        self.assertEqual((trig["signals"], trig["orders"], trig["filled"]), (7, 2, 1))
        self.assertEqual(market["slug"], f"btc-updown-5m-{hub.window_start}")
        self.assertEqual(market["up"]["ask"], 0.47)
        self.assertTrue(0 < market["seconds_left"] <= 300)
        self.assertEqual(snap["skips"], [("cooldown", 5)])
        self.assertIsNone(snap["canary"])
        self.assertEqual(dict((n, s) for n, s, _ in snap["health"])["USER WS"], "ABSENT")
        self.assertEqual(len(sn.rounds), 0, "the dashboard created a round entry by reading it")
        self.assertEqual(market["carry"], {"UP": {"carry": 0.0, "next": 10.0}, "DOWN": {"carry": 0.0, "next": 10.0}})
        sn.carry_market, sn.carry["UP"] = (str(hub.condition_id), hub.window_end), 4.0
        self.assertEqual(run.DashView(rt).snapshot()["market"]["carry"]["UP"], {"carry": 4.0, "next": 14.0})
        self.assertIn("carry UP 4 DN 0", run.status_text(run.DashView(rt).snapshot()))
        line = run.status_text(snap)
        for part in ("PAPER", "binance=", "signals=7", "orders=2", "filled=1", "skips: cooldown 5"):
            self.assertIn(part, line)
        frame = build(snap, 120, 40, glyphs())
        self.assertEqual(len(frame), 40)


# ---------------------------------------------------------------- regressions without a spe8 test
class PreflightStopFileTests(unittest.TestCase):
    """aa3adc7: a present STOP_LIVE is a FAIL that names it and how to remove it
    (it printed 'WARN no stop file', which read as the opposite)."""

    def _check(self, stop_file: pathlib.Path):
        import config
        import live_preflight
        live_preflight.RESULTS.clear()
        with env(**{**SNIPER_ENV, "SNIPER_LIVE_CANARY": "1"}, SNIPER_CANARY_STOP_FILE=str(stop_file),
                 USER_WS="on", POLY_PRIVATE_KEY=None, PRIVATE_KEY=None), \
                patched(config, ASSUMED_MATCH_DELAY_SECONDS=5.0), contextlib.redirect_stdout(io.StringIO()):
            live_preflight.check_settings(config)
        return [r for r in live_preflight.RESULTS if "stop file" in r[1]]

    def test_present_stop_file_fails_and_says_how_to_clear_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            stop = pathlib.Path(tmp) / "STOP_LIVE"
            stop.write_text("", encoding="utf-8")
            rows = self._check(stop)
        self.assertEqual(len(rows), 1, rows)
        level, name, detail = rows[0]
        self.assertEqual(level, "FAIL")
        self.assertIn("every live order will be refused", name)
        self.assertIn(f"rm {stop}", detail)

    def test_absent_stop_file_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = self._check(pathlib.Path(tmp) / "STOP_LIVE")
        self.assertEqual([r[0] for r in rows], ["PASS"])


class SniperCheckToolTests(unittest.TestCase):
    """sniper_check.py reads FAK (IOC) outcomes and the real price limit."""

    def test_fak_no_fill_is_the_price_moving_and_unknown_book_is_a_gap(self):
        import sniper_check as sc
        self.assertEqual(sc.refusal_kind("FAK no-fill: nothing at or below 0.64"), "price ran past the cap (normal)")
        self.assertEqual(sc.refusal_kind("FOK no-fill: only $0.00 available"), "price ran past the cap (normal)")
        self.assertIn("measurement gap",
                      sc.refusal_kind("unknown book state at the modelled arrival: the feed has not yet passed"))

    def test_the_cap_is_read_from_the_orders(self):
        import sniper_check as sc
        rows = [{"cap": 0.52, "ask_seen": 0.47}, {"cap": 0.60, "ask_seen": 0.55}, {"cap": 0.95, "ask_seen": 0.93}]
        self.assertEqual(sc.order_slip_c(rows), 5.0)                  # the clipped 0.93 -> 0.95 is the outlier
        self.assertEqual(sc.order_slip_c([]), 2.0)


class SettingsErrorTests(unittest.TestCase):
    """7eedf06: a bad SNIPER_* value is named with its allowed range, not
    reported as 'invalid SNIPER_* settings'."""

    def test_each_bad_value_is_named_with_its_range(self):
        cases = {"SNIPER_SLIP": ("0.85", "SNIPER_SLIP=0.85 (must be 0 to 0.49"),
                 "SNIPER_K_BP": ("0", "SNIPER_K_BP=0 (must be > 0)"),
                 "SNIPER_WINDOW_MS": ("-5", "SNIPER_WINDOW_MS=-5 (must be > 0)"),
                 "SNIPER_SHARES": ("3", "SNIPER_SHARES=3 (venue minimum is 5)"),
                 "SNIPER_MAX_FILLS_ROUND": ("0", "SNIPER_MAX_FILLS_ROUND=0 (must be > 0)")}
        for key, (value, expected) in cases.items():
            with env(**{**SNIPER_ENV, key: value}), self.assertRaises(ValueError) as caught:
                sniper_mode.Settings()
            self.assertIn(expected, str(caught.exception), key)

    def test_several_bad_values_are_all_named(self):
        with env(**SNIPER_ENV, SNIPER_SLIP="0.9", SNIPER_K_BP="-1"), self.assertRaises(ValueError) as caught:
            sniper_mode.Settings()
        self.assertIn("SNIPER_SLIP=0.9", str(caught.exception))
        self.assertIn("SNIPER_K_BP=-1", str(caught.exception))

    def test_enum_settings_name_the_value(self):
        for key, value in (("SNIPER_ORDER_TYPE", "GTC"), ("SNIPER_PRIMARY_FEED", "coinbase"),
                           ("SNIPER_DELAY_SOURCE", "guess")):
            with env(**{**SNIPER_ENV, key: value}), self.assertRaises(ValueError) as caught:
                sniper_mode.Settings()
            self.assertIn(value, str(caught.exception), key)

    def test_the_shipped_example_settings_are_valid(self):
        """.env.example is what the VPS runs: every value in it must load."""
        values = {}
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                values[k.strip()] = v.strip() or None
        self.assertFalse(any(values.get(k) for k in ("POLY_PRIVATE_KEY", "POLY_FUNDER", "PRIVATE_KEY")),
                         ".env.example must never carry a wallet value")
        values["SNIPER_DELAY_SOURCE"] = "model"      # "live" needs the live journal, checked at start
        with env(**values):
            s = sniper_mode.Settings()
        self.assertEqual((s.k_bp, s.window_ms, s.order_type, s.slip, s.cooldown_ms, s.min_left_s,
                          s.max_fills_round, s.max_side_shares, s.primary_feed),
                         (0.5, 250.0, "FAK", 0.05, 300.0, 10.0, 20, 200.0, "binance"))
        self.assertEqual(s.shares, 10.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
