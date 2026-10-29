"""Test isolation. Every test suite imports this FIRST, before anything that
imports config.py.

config.py loads the bot's .env into os.environ when it is imported. A test
process must not see it: the suites assume the shipped defaults (a real .env
sets SNIPER_DELAY_SOURCE=live, TRADE_LAST_SECONDS=300, ... and 15 paper tests
then fail on the VPS), and a test has no business holding the wallet key.

So, for the test process only: the .env load becomes a no-op and the bot's
own settings are removed from the inherited environment. The bot and its
tools are untouched - they still read .env exactly as before.
"""
import os
import sys
import ipaddress

# The suites live in tests/; the bot's modules in the repository root above it.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(1, _ROOT)

import dotenv  # noqa: E402


def _loopback_host(host) -> bool:
    if isinstance(host, bytes):
        host = host.decode("ascii", errors="replace")
    if isinstance(host, str) and host.rstrip(".").lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except (TypeError, ValueError):
        return False


def _offline_network(event, args) -> None:
    """Allow fake CLOBs on loopback; reject venue/DNS traffic before I/O.

    An audit hook also covers sockets opened by HTTP clients and native
    asyncio transports. Removing .env alone cannot guarantee offline tests.
    """
    if event == "socket.getaddrinfo" and not _loopback_host(args[0]):
        raise PermissionError("offline tests forbid non-loopback DNS")
    if event == "socket.connect":
        address = args[1]
        if not isinstance(address, tuple) or not _loopback_host(address[0]):
            raise PermissionError("offline tests forbid non-loopback connections")


sys.addaudithook(_offline_network)

if "config" in sys.modules:
    raise RuntimeError("import testing_env before anything that imports config (it has already read .env)")


def _no_dotenv(*_args, **_kwargs) -> bool:
    return False


dotenv.load_dotenv = _no_dotenv
dotenv.dotenv_values = lambda *_args, **_kwargs: {}       # run.shadowed_settings: never the bot's .env

_PREFIXES = ("SNIPER_", "PAPER_", "POLY_", "LIVE_", "DASH_", "STOP_LOSS_", "FLIP_", "PHASE1_", "PHASE2_",
             "SETTLE_")
_NAMES = {
    "PRIVATE_KEY", "POLYGON_RPC", "LEDGER_PATH", "USER_WS", "RECONCILE", "BOOK_SOURCE", "BTC_FEED",
    "BTC_STALE_AFTER", "BOOK_STALE_AFTER", "PRICE_STALE_POLICY", "BOOK_AUDIT", "TRADE_LAST_SECONDS",
    "TRADE_INTERVAL_SECONDS", "MIN_SECONDS_TO_EXPIRY", "MIN_BUY_PRICE", "MAX_BUY_PRICE", "BET_SIZE",
    "ASSUMED_MATCH_DELAY_SECONDS", "MARKET_CATEGORY", "CLOB_HOST", "BOT_LOCK_PATH", "ALLOW_OTHER_BOT",
    "MAX_ALLOWED_SPREAD", "MAX_ROUND_EXPOSURE", "MAX_UNSETTLED_EXPOSURE", "SETTLE_ONCHAIN",
    "GAMMA_HOST", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
}
for _key in list(os.environ):
    if _key.startswith(_PREFIXES) or _key in _NAMES:
        os.environ.pop(_key, None)
# The other-install lock probe defaults to /opt/btcbot on Linux: tests name their own.
os.environ["OTHER_BOT_LOCKS"] = ""
# Settlement reads the chain by default; no test may reach Polygon or the CLOB
# through it. The tests of that path turn it back on around fakes.
os.environ["SETTLE_ONCHAIN"] = "0"
# No test writes the persisted public-print log (run._print_log); its own tests build one.
os.environ["SNIPER_PRINTS_LOG"] = "0"
# The fair-value gate (SNIPER_FAIR, on in the bot) needs a round-start price and a
# minute of volatility samples that order-path tests do not build: off here, on in
# tests_sniper_fair.py and wherever a test resolves the frozen spec (clear env / SPEC).
os.environ["SNIPER_FAIR"] = "0"
# Paper reads the LIVE journal in its folder by default (2026-10-29: SNIPER_DELAY_SOURCE=auto,
# SNIPER_PAPER_CASH_FROM unset = auto). No suite may depend on whether a live_sniper.jsonl sits
# next to the code (it does on the VPS): the fixed model and paper's own cash here, as before;
# tests_paper_auto.py turns auto on around journals of its own.
os.environ["SNIPER_DELAY_SOURCE"] = "model"
os.environ["SNIPER_PAPER_CASH_FROM"] = "off"
# Historical fixtures intentionally exercise the independent model. Strict
# comparison tests opt in around synthetic live references of their own.
os.environ["SNIPER_PAPER_LIVE_PARITY"] = "0"
