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

import dotenv

if "config" in sys.modules:
    raise RuntimeError("import testing_env before anything that imports config (it has already read .env)")


def _no_dotenv(*_args, **_kwargs) -> bool:
    return False


dotenv.load_dotenv = _no_dotenv

_PREFIXES = ("SNIPER_", "PAPER_", "POLY_", "LIVE_", "DASH_", "STOP_LOSS_", "FLIP_", "PHASE1_", "PHASE2_",
             "SETTLE_")
_NAMES = {
    "PRIVATE_KEY", "POLYGON_RPC", "LEDGER_PATH", "USER_WS", "RECONCILE", "BOOK_SOURCE", "BTC_FEED",
    "BTC_STALE_AFTER", "BOOK_STALE_AFTER", "PRICE_STALE_POLICY", "BOOK_AUDIT", "TRADE_LAST_SECONDS",
    "TRADE_INTERVAL_SECONDS", "MIN_SECONDS_TO_EXPIRY", "MIN_BUY_PRICE", "MAX_BUY_PRICE", "BET_SIZE",
    "ASSUMED_MATCH_DELAY_SECONDS", "MARKET_CATEGORY", "CLOB_HOST", "BOT_LOCK_PATH", "ALLOW_OTHER_BOT",
    "MAX_ALLOWED_SPREAD", "MAX_ROUND_EXPOSURE", "MAX_UNSETTLED_EXPOSURE", "SETTLE_ONCHAIN",
}
for _key in list(os.environ):
    if _key.startswith(_PREFIXES) or _key in _NAMES:
        os.environ.pop(_key, None)
# The other-install lock probe defaults to /opt/btcbot on Linux: tests name their own.
os.environ["OTHER_BOT_LOCKS"] = ""
# Settlement reads the chain by default; no test may reach Polygon or the CLOB
# through it. The tests of that path turn it back on around fakes.
os.environ["SETTLE_ONCHAIN"] = "0"
