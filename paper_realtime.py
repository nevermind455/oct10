#!/usr/bin/env python3
"""Real-time PAPER run: the sniper AND the Position Manager trading the paper
account on live Polymarket BTC 5-minute markets, supervised and measured.

    python paper_realtime.py run --markets 25          # foreground supervisor
    python paper_realtime.py ctl restart               # graceful restart (CTRL_BREAK / SIGTERM)
    python paper_realtime.py ctl hard-restart          # crash test: kill -9, then restart
    python paper_realtime.py ctl stop
    python paper_realtime.py report                    # = paper_report.py --dir <dir>

What it starts, in its own state folder (default ~/sniperbtc-realtime; never
the bot's usual paper state):
  - run.py --paper with POSITION_MANAGER_ENABLED=1 (paper orders), its paper
    ledger / account / audit / lock files in that folder, output to bot.log;
  - the latency_lab recorder (LAB_DATA=<dir>/lab), so the very same live
    stream can be replayed later as A = sniper only vs B = sniper + manager.

Every 30s it samples the bot's CPU / RAM / threads / handles and re-checks the
paper invariants (paper_report.check) into monitor.jsonl; anything wrong is an
"alert" row. It stops (gracefully) after --markets complete markets (settled,
or given up as position-less) counted from the settlement journal.

Real market data, simulated execution: the bot's paper firewall refuses and
counts every live order path; REAL MONEY RISK is $0.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
DEFAULT_DIR = os.path.expanduser("~/sniperbtc-realtime")


def bot_env(d: pathlib.Path, balance: float) -> dict:
    env = dict(os.environ)
    env.update({
        "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1",
        "SNIPER_ONLY": "1",
        "POSITION_MANAGER_ENABLED": "1", "POSITION_MANAGER_LIVE_ENABLED": "0",
        "PAPER_LEDGER_PATH": str(d / "paper_ledger.json"), "PAPER_ACCOUNT_PATH": str(d / "paper_account.json"),
        "PAPER_AUDIT_PATH": str(d / "paper_orders.jsonl"), "BOT_LOCK_PATH": str(d / "bot.lock"),
        "PAPER_START_BALANCE": str(balance),
    })
    return env


def popen(cmd, env, log):
    flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    return subprocess.Popen(cmd, cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
                            creationflags=flags, start_new_session=os.name != "nt")


def graceful(proc, timeout=90, stop_file: pathlib.Path | None = None):
    """A clean shutdown: the bot's stop-request file when given (works without a
    console), else SIGTERM / CTRL_BREAK; killed only if it does not stop."""
    if proc.poll() is not None:
        return proc.returncode
    try:
        if stop_file is not None:
            stop_file.write_text("stop", encoding="utf-8")
        elif os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)      # SIGBREAK in run.py: a clean shutdown
        else:
            proc.send_signal(signal.SIGTERM)
        return proc.wait(timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.wait(30)
    except OSError:                                        # no console to deliver CTRL_BREAK on
        proc.terminate()
        return proc.wait(30)


def completed_markets(d: pathlib.Path) -> set:
    """Markets whose venue result has been booked (every tracked market gets an
    ACCOUNTING_SETTLED row when its result arrives, with or without positions)."""
    path = d / "paper_ledger_settlements.jsonl"
    done = set()
    if path.exists():
        for line in path.open(encoding="utf-8", errors="replace"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("event") == "ACCOUNTING_SETTLED":
                done.add(r.get("condition_id"))
    return done


def expired_markets(d: pathlib.Path) -> set:
    path = d / "paper_ledger_settlements.jsonl"
    seen = set()
    if path.exists():
        for line in path.open(encoding="utf-8", errors="replace"):
            try:
                r = json.loads(line)
            except ValueError:
                continue
            if r.get("event") == "MARKET_EXPIRED":
                seen.add(r.get("condition_id"))
    return seen


def run(args) -> int:
    import psutil
    sys.path.insert(0, str(ROOT))
    import paper_report
    d = pathlib.Path(args.dir)
    d.mkdir(parents=True, exist_ok=True)
    (d / "lab").mkdir(exist_ok=True)
    mon = (d / "monitor.jsonl").open("a", encoding="utf-8")

    def note(kind, **kw):
        mon.write(json.dumps({"wall": time.time(), "kind": kind, **kw}, default=str) + "\n")
        mon.flush()
        if kind != "resources":
            print(f"[supervisor] {kind} {json.dumps(kw, default=str)[:300]}", flush=True)

    env = bot_env(d, args.balance)
    bot_log = (d / "bot.log").open("a", encoding="utf-8")
    rec_log = (d / "recorder.log").open("a", encoding="utf-8")
    bot = popen([sys.executable, "-u", "run.py", "--paper"], env, bot_log)
    rec = None
    if not args.no_recorder:
        rec = popen([sys.executable, "-u", "-m", "latency_lab.recorder"], dict(env, LAB_DATA=str(d / "lab")), rec_log)
    note("start", bot_pid=bot.pid, recorder_pid=rec.pid if rec else None, target_markets=args.markets)
    ctl = d / "control"
    stop = d / "bot.lock.stop"                              # run.py's stop-request file (BOT_LOCK_PATH + .stop)
    started = time.time()
    last_check = 0.0
    restarts = 0
    sampled: dict = {}                                      # pid -> its psutil.Process (cpu_percent needs one)
    try:
        while True:
            time.sleep(2.0)
            if bot.poll() is not None:
                note("alert", what="bot exited", code=bot.returncode)
                if restarts >= 20:
                    note("alert", what="too many restarts; stopping")
                    break
                restarts += 1
                time.sleep(5)
                bot = popen([sys.executable, "-u", "run.py", "--paper"], env, bot_log)
                note("restart", why="bot exited", bot_pid=bot.pid)
            if ctl.exists():
                cmd = ctl.read_text(encoding="utf-8").strip()
                ctl.unlink()
                if cmd == "stop":
                    note("control", cmd=cmd)
                    break
                if cmd in ("restart", "hard-restart"):
                    t0 = time.time()
                    code = bot.kill() or bot.wait(30) if cmd == "hard-restart" else graceful(bot, stop_file=stop)
                    note("restart", why=cmd, exit_code=code, stopped_in_s=round(time.time() - t0, 2))
                    bot = popen([sys.executable, "-u", "run.py", "--paper"], env, bot_log)
                    note("restart", why=f"{cmd} done", bot_pid=bot.pid)
            if time.time() - last_check >= args.every:
                last_check = time.time()
                try:
                    # one Process object per bot pid: cpu_percent() measures since its
                    # previous call on the SAME object (a fresh one always says 0.0)
                    if sampled.get("pid") != bot.pid:
                        sampled.update(pid=bot.pid, proc=psutil.Process(bot.pid))
                        sampled["proc"].cpu_percent(interval=None)
                    p = sampled["proc"]
                    with p.oneshot():
                        note("resources", cpu_pct=p.cpu_percent(interval=None), rss_mb=round(p.memory_info().rss / 2**20, 1),
                             threads=p.num_threads(),
                             handles=p.num_handles() if hasattr(p, "num_handles") else p.num_fds())
                except Exception as exc:  # noqa: BLE001
                    note("alert", what=f"resource sample failed: {type(exc).__name__}")
                try:
                    rep = paper_report.build(d)
                    bad = {k: v for k, v in rep["checks"].items() if v and not k.startswith("measured_")}
                    if bad:
                        note("alert", what="invariant", checks=bad)
                    done = completed_markets(d)
                    note("progress", completed=len(done), expired=len(expired_markets(d)),
                         elapsed_min=round((time.time() - started) / 60, 1),
                         pnl=rep["totals"].get("combined_pnl"), sniper_orders=rep["totals"].get("sniper_orders"),
                         manager_orders=rep["totals"].get("manager_orders"))
                    if args.markets and len(done) >= args.markets:
                        note("target reached", completed=len(done))
                        break
                except Exception as exc:  # noqa: BLE001
                    note("alert", what=f"check failed: {type(exc).__name__}: {exc}")
    finally:
        code = graceful(bot, stop_file=stop)
        note("stopped", bot_exit=code)
        if rec is not None:
            graceful(rec, timeout=30)
        mon.close()
    return 0


def ctl(args) -> int:
    d = pathlib.Path(args.dir)
    (d / "control").write_text(args.cmd, encoding="utf-8")
    print(f"sent {args.cmd} to the supervisor of {d}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="what", required=True)
    r = sub.add_parser("run")
    r.add_argument("--dir", default=DEFAULT_DIR)
    r.add_argument("--markets", type=int, default=25, help="stop after this many settled markets (0 = never)")
    r.add_argument("--balance", type=float, default=1000.0)
    r.add_argument("--every", type=float, default=30.0, help="seconds between resource/invariant samples")
    r.add_argument("--no-recorder", action="store_true")
    c = sub.add_parser("ctl")
    c.add_argument("cmd", choices=("stop", "restart", "hard-restart"))
    c.add_argument("--dir", default=DEFAULT_DIR)
    rp = sub.add_parser("report")
    rp.add_argument("--dir", default=DEFAULT_DIR)
    rp.add_argument("--json", default=None)
    args = ap.parse_args(argv)
    if args.what == "run":
        return run(args)
    if args.what == "ctl":
        return ctl(args)
    sys.path.insert(0, str(ROOT))
    import paper_report
    return paper_report.main(["--dir", args.dir] + (["--json", args.json] if args.json else []))


if __name__ == "__main__":
    raise SystemExit(main())
