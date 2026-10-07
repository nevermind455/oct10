#!/usr/bin/env python3
"""Run every test suite, each in its own process, and print one line per suite.

    python run_tests.py            # all suites
    python run_tests.py feeds run  # only tests_feeds.py and tests_run.py

No network, no wallet: the suites never read .env (see testing_env.py).
Exit code 0 only when every suite passed.
"""
import os
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent


def main(argv) -> int:
    suites = sorted(ROOT.glob("tests_*.py"))
    if argv:
        suites = [s for s in suites if any(s.stem == f"tests_{a}" or s.stem == a for a in argv)]
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    failed = []
    for suite in suites:
        t0 = time.monotonic()
        proc = subprocess.run([sys.executable, str(suite)], cwd=ROOT, env=env, capture_output=True,
                              text=True, encoding="utf-8", errors="replace", timeout=1200)
        out = proc.stdout + proc.stderr
        summary = re.findall(r"^(?:\d+ passed, \d+ failed|Ran \d+ tests? in [\d.]+s|OK.*|FAILED.*)$", out, re.M)
        ok = proc.returncode == 0
        print(f"{'PASS' if ok else 'FAIL'}  {suite.name:30s} {time.monotonic() - t0:6.1f}s  "
              f"{' | '.join(summary[-2:])}", flush=True)
        if not ok:
            failed.append(suite.name)
            print("\n".join(out.splitlines()[-40:]), flush=True)
    print(f"\n{len(suites) - len(failed)}/{len(suites)} suites passed" + (f"; FAILED: {', '.join(failed)}" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
