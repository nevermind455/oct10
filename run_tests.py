#!/usr/bin/env python3
"""Run every test suite, each in its own process, and print one line per suite.

    python run_tests.py            # all suites
    python run_tests.py feeds run  # only tests/tests_feeds.py and tests/tests_run.py
    python run_tests.py --require-native-sdk unified_sdk  # release check: no SDK skips

No network, no wallet: the suites never read .env (see testing_env.py).
Exit code 0 only when every suite passed.
"""
import importlib
import importlib.metadata
import os
import pathlib
import re
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent
TESTS = ROOT / "tests"


def native_sdk_problem() -> str | None:
    """Check the real installed distribution and imports, without network or keys."""
    try:
        version = importlib.metadata.version("polymarket-client")
    except importlib.metadata.PackageNotFoundError:
        return "polymarket-client==0.13.0 is not installed"
    if version != "0.13.0":
        return f"polymarket-client==0.13.0 is required; installed version is {version}"
    for name in ("polymarket", "eth_account.messages", "eth_utils.crypto"):
        try:
            importlib.import_module(name)
        except Exception as exc:
            return f"native dependency import {name} failed: {type(exc).__name__}: {exc}"
    return None


def main(argv) -> int:
    require_native = "--require-native-sdk" in argv
    argv = [a for a in argv if a != "--require-native-sdk"]
    if require_native:
        problem = native_sdk_problem()
        if problem:
            print(f"FAIL  native SDK validation required: {problem}. Native checks did not run.", file=sys.stderr)
            return 1
    suites = sorted(TESTS.glob("tests_*.py"))
    if argv:
        unknown = [a for a in argv if not any(s.stem in (a, f"tests_{a}") for s in suites)]
        if unknown:
            print(f"Unknown test suites: {', '.join(unknown)}", file=sys.stderr)
            return 2
        suites = [s for s in suites if any(s.stem == f"tests_{a}" or s.stem == a for a in argv)]
        if require_native and TESTS / "tests_unified_sdk.py" not in suites:
            suites.append(TESTS / "tests_unified_sdk.py")
    if not suites:
        print("No test suites found", file=sys.stderr)
        return 2
    # the suites in tests/ import the bot's modules from the repository root
    env = dict(os.environ, PYTHONIOENCODING="utf-8",
               PYTHONPATH=os.pathsep.join(p for p in (str(ROOT), os.environ.get("PYTHONPATH")) if p))
    return _run_suites(suites, env, require_native_sdk=require_native)


def _run_suites(suites, env, *, require_native_sdk=False) -> int:
    failed = []
    for suite in suites:
        t0 = time.monotonic()
        try:
            proc = subprocess.run([sys.executable, str(suite)], cwd=ROOT, env=env, capture_output=True,
                                  text=True, encoding="utf-8", errors="replace", timeout=1200)
        except subprocess.TimeoutExpired:
            failed.append(suite.name)
            print(f"FAIL  {suite.name:30s} {time.monotonic() - t0:6.1f}s  suite timed out", flush=True)
            continue
        out = proc.stdout + proc.stderr
        summary = re.findall(r"^(?:\d+ passed, \d+ failed|Ran \d+ tests? in [\d.]+s|OK.*|FAILED.*)$", out, re.M)
        ok = proc.returncode == 0
        if (require_native_sdk and suite.stem == "tests_unified_sdk"
                and re.search(r"\bskipped=[1-9]\d*\b", out)):
            ok = False
            out += "\nNative SDK checks were skipped; strict validation cannot pass.\n"
        print(f"{'PASS' if ok else 'FAIL'}  {suite.name:30s} {time.monotonic() - t0:6.1f}s  "
              f"{' | '.join(summary[-2:])}", flush=True)
        if not ok:
            failed.append(suite.name)
            print("\n".join(out.splitlines()[-40:]), flush=True)
    print(f"\n{len(suites) - len(failed)}/{len(suites)} suites passed" + (f"; FAILED: {', '.join(failed)}" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
