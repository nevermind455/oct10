#!/usr/bin/env python3
"""BTC sniper - the bot's only entry point.

    python run.py                       # PAPER (default): live public data, simulated fills
    python run.py --paper --dash        # PAPER + terminal dashboard
    python run.py --live --dash         # the capped LIVE CANARY (SNIPER_ONLY=1 + SNIPER_LIVE_CANARY=1)
    python run.py --health              # public feeds only, health as JSON for 30s; no wallet, no trading
    python run.py --paper-balance 500   # starting paper cash (first launch of a paper ledger only)

The sniper (sniper_mode.Sniper) is the only strategy: when the BTC mid
(Binance BTCUSDT bookTicker, or Kraken BTC/USD) moves >= SNIPER_K_BP within
SNIPER_WINDOW_MS it buys the side BTC moved toward in the current 5-minute
btc-updown market, and holds to settlement. Paper fills against Polymarket's
book; live sends real orders under the canary's hard limits (sniper_live).

Feeds (read from the environment, same contract as spe8's run_feeds):
    USER_WS=on|off          default on    private fill channel (LIVE requires on)
    RECONCILE=auto|off      default auto  REST backup for fills (LIVE only)
    BOOK_STALE_AFTER=8.0    seconds before a quiet Polymarket book is STALE
    BTC_STALE_AFTER=3.0     the hub's Binance trade feed (--health only)
PAPER forces USER_WS and RECONCILE off: it never touches the wallet.

Stop with Ctrl+C or SIGTERM: no new orders from that moment, then the live
sender, reconciler, settlement and feeds stop, late fills are booked, and a
final JSON report is printed. Every exit appends a line to bot_exit.log.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import dataclasses
import functools
import json
import math
import os
import signal
import sys
import threading
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from accounting import Ledger  # noqa: E402 - also loads config (.env) first
from accounting.settlement import SettlementWorker  # noqa: E402
from feeds import FeedHub, RestReconciler  # noqa: E402
from feeds.health import safe_log_text  # noqa: E402

TAG = "[RUN]"
EVENTS: list[tuple[float, str, str, str]] = []
_EV_LOCK = threading.Lock()
# Set when a connection changes state; the dashboard repaints every row then
# (anything that ran while a link was down may have disturbed the terminal).
_REPAINT = threading.Event()
_REPAINT_MARKS = ("reconnect", "connected", "subscribed", "resubscrib", "stream open", "session rotated")
# Which signal stopped the process, for bot_exit.log and the exit code.
_STOP_SIGNAL: list[str] = []

# Settings an old main-strategy .env may still carry. This build has no code
# that reads them; naming them at start beats letting them look active.
IGNORED_SETTINGS = {
    "BOOK_SOURCE": "the sniper reads the socket book directly",
    "BTC_FEED": "the sniper runs its own Binance socket",
    "PRICE_STALE_POLICY": "main-strategy display setting",
    "BOOK_AUDIT": "main-strategy REST/WS book audit",
    "SNIPER_ENABLED": "the sniper always runs (SNIPER_ONLY=1)",
    "STOP_LOSS_ENABLED": "the sniper holds to settlement; no stop-loss loop in this build",
    "FLIP_EXIT_ENABLED": "the sniper holds to settlement; no flip exits in this build",
}


def on_event(source: str, text: str, level: str = "info") -> None:
    """Cheap by contract - feeds call this from receive paths."""
    safe_source = safe_log_text(source, limit=40) or "unknown"
    safe_text = safe_log_text(text, limit=1000)
    safe_level = level if level in ("info", "good", "warn", "bad") else "warn"
    with _EV_LOCK:
        EVENTS.append((time.time(), safe_source, safe_text, safe_level))
        if len(EVENTS) > 2000:
            del EVENTS[:1000]
    if any(mark in safe_text.lower() for mark in _REPAINT_MARKS):
        _REPAINT.set()


def recent_events(n: int = 50) -> list[tuple[float, str, str, str]]:
    with _EV_LOCK:
        return list(EVENTS[-n:])


# ---------------------------------------------------------------- process
def _drain_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Finish or cancel leftover tasks so Ctrl+C never leaves pending-task noise."""
    pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        with contextlib.suppress(KeyboardInterrupt):
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    with contextlib.suppress(KeyboardInterrupt, Exception):
        loop.run_until_complete(loop.shutdown_asyncgens())
    shutdown_executor = getattr(loop, "shutdown_default_executor", None)
    if shutdown_executor is not None:
        with contextlib.suppress(KeyboardInterrupt, Exception):
            loop.run_until_complete(shutdown_executor())


def run_quietly(coro) -> int:
    """Run *coro*. Ctrl+C or SIGTERM is a clean shutdown, never a traceback:
    the first one cancels the run (which then shuts down in order), a second
    Ctrl+C forces the exit. Returns 0, 130 (interrupt) or 143 (SIGTERM)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(coro)
    hits = 0

    def _on_signal(signum, _frame) -> None:
        nonlocal hits
        hits += 1
        name = "SIGTERM" if signum == getattr(signal, "SIGTERM", None) else "SIGINT"
        if name not in _STOP_SIGNAL:
            _STOP_SIGNAL.append(name)
        if hits == 1 and not task.done():
            loop.call_soon_threadsafe(task.cancel)
            return
        if name == "SIGINT":
            raise KeyboardInterrupt()

    previous = {signal.SIGINT: signal.getsignal(signal.SIGINT)}
    for extra in ("SIGTERM", "SIGBREAK"):
        if hasattr(signal, extra):
            previous[getattr(signal, extra)] = signal.getsignal(getattr(signal, extra))
    for signum in previous:
        with contextlib.suppress(ValueError, OSError):
            signal.signal(signum, _on_signal)
    try:
        try:
            loop.run_until_complete(task)
        except (KeyboardInterrupt, asyncio.CancelledError):
            if "SIGINT" not in _STOP_SIGNAL and "SIGTERM" not in _STOP_SIGNAL:
                _STOP_SIGNAL.append("SIGINT")
            if not task.done():
                task.cancel()
                with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
                    loop.run_until_complete(task)
        if "SIGTERM" in _STOP_SIGNAL:
            return 143
        return 130 if _STOP_SIGNAL else 0
    finally:
        for signum, handler in previous.items():
            with contextlib.suppress(Exception):
                signal.signal(signum, handler)
        with contextlib.suppress(KeyboardInterrupt):
            _drain_loop(loop)
        loop.close()
        asyncio.set_event_loop(None)


def _state_path(env_name: str, default_name: str) -> Path:
    """One runtime-state path: the environment's, else the default, relative to this folder."""
    raw = os.environ.get(env_name) or default_name
    path = Path(raw)
    return path if path.is_absolute() else ROOT / path


def _paper_state_paths() -> dict[str, Path]:
    """Every mutable PAPER file, each overridable on its own."""
    return {
        "ledger": _state_path("PAPER_LEDGER_PATH", "paper_ledger.json"),
        "account": _state_path("PAPER_ACCOUNT_PATH", "paper_account.json"),
        "audit": _state_path("PAPER_AUDIT_PATH", "paper_orders.jsonl"),
    }


class _ProcessLock:
    """Prevent two bot processes from sharing one wallet/paper ledger."""

    # Windows byte-range locks are mandatory, not advisory: a lock over a byte
    # that carries content makes the file unreadable for every other handle.
    # Lock one sentinel byte far past end-of-file instead.
    _NT_LOCK_OFFSET = 1 << 30

    def __init__(self, path: Path) -> None:
        self.path = path
        self._file = None
        # Imported here, not in release(): release() runs on the shutdown path,
        # where a fresh import can fail while the interpreter is exiting.
        self._locker = None
        if os.name == "posix":
            import fcntl
            self._locker = fcntl
        elif os.name == "nt":
            import msvcrt
            self._locker = msvcrt

    def acquire(self) -> None:
        if self._file is not None:
            raise RuntimeError("process lock is already held by this instance")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | os.O_APPEND
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        handle = os.fdopen(fd, "a+", encoding="utf-8")
        try:
            if os.name == "posix":
                fcntl = self._locker
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif os.name == "nt":
                msvcrt = self._locker
                # msvcrt locks bytes from the current position; append mode
                # ignores this position for writes, so nothing is truncated.
                handle.seek(self._NT_LOCK_OFFSET)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                raise RuntimeError(f"single-process locking is unsupported on {os.name!r}")
        except (BlockingIOError, OSError) as exc:
            handle.close()
            raise RuntimeError(f"cannot acquire {self.path.name}; another bot may be running "
                               f"({type(exc).__name__})") from None
        except Exception:
            handle.close()
            raise
        try:
            handle.seek(0, os.SEEK_END)
            handle.write(f"pid={os.getpid()} started={time.time():.3f}\n")
            handle.flush()
            os.fsync(handle.fileno())
        except Exception:
            handle.close()             # closing the descriptor releases the lock as well
            raise
        self._file = handle

    def release(self) -> None:
        if self._file is None:
            return
        try:
            if os.name == "posix" and self._locker is not None:
                self._locker.flock(self._file.fileno(), self._locker.LOCK_UN)
            elif os.name == "nt" and self._locker is not None:
                self._file.seek(self._NT_LOCK_OFFSET)
                self._locker.locking(self._file.fileno(), self._locker.LK_UNLCK, 1)
        except Exception:
            pass                       # closing the handle below releases it anyway
        finally:
            self._file.close()
            self._file = None


def lock_held_elsewhere(path: Path) -> bool | None:
    """True when another process holds the bot lock at *path*, False when
    nobody does, None when this user cannot tell (the file is another user's).
    Never creates the file, and holds the lock only for the instant of the probe."""
    fd = None
    for flags in (os.O_RDWR, os.O_RDONLY):          # flock works on a read-only descriptor too
        try:
            fd = os.open(path, flags)
            break
        except FileNotFoundError:
            return False               # no such lock file: nothing ever ran there
        except OSError:
            continue
    if fd is None:
        return None
    try:
        if os.name == "posix":
            import fcntl
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, _ProcessLock._NT_LOCK_OFFSET, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                return True
            os.lseek(fd, _ProcessLock._NT_LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        return False
    finally:
        os.close(fd)


def _other_bot_locks() -> list[Path]:
    """Lock files of other installs on this machine (OTHER_BOT_LOCKS, comma
    separated). Default: the old bot in /opt/btcbot, on Linux."""
    raw = os.environ.get("OTHER_BOT_LOCKS")
    if raw is None:
        raw = ("/opt/btcbot/.btc_bot_live.lock,/opt/btcbot/.btc_bot_paper.lock"
               if os.name == "posix" else "")
    return [Path(p.strip()) for p in raw.split(",") if p.strip()]


def check_other_bots(*, paper: bool, own_lock: Path) -> None:
    """One bot per VPS: refuse to start beside another install's running bot.
    LIVE always refuses (one wallet, two bots); PAPER refuses unless
    ALLOW_OTHER_BOT=1 (two bots on one vCPU skew each other's latency)."""
    probes = {p: lock_held_elsewhere(p) for p in _other_bot_locks() if p.resolve() != own_lock.resolve()}
    unknown = [str(p) for p, state in probes.items() if state is None]
    if unknown and not paper:
        raise RuntimeError(f"cannot tell whether another bot holds {', '.join(unknown)} (no permission to open it): "
                           f"run as a user that can, or set OTHER_BOT_LOCKS= if that install is gone")
    held = [p for p, state in probes.items() if state]
    if not held:
        return
    names = ", ".join(str(p) for p in held)
    if not paper:
        raise RuntimeError(f"another bot is running ({names} is locked): stop it before starting LIVE - "
                           f"two bots on one wallet double the exposure and fight over the ledger")
    if str(os.environ.get("ALLOW_OTHER_BOT", "")).strip() != "1":
        raise RuntimeError(f"another bot is running ({names} is locked): stop it first - one bot per vCPU "
                           f"(ALLOW_OTHER_BOT=1 runs paper beside it anyway)")
    print(f"{TAG} WARNING: another bot is running ({names}); ALLOW_OTHER_BOT=1 - paper latency is shared")


# ---------------------------------------------------------------- feeds
def _choice(name: str, default: str, allowed: tuple[str, ...]) -> str:
    value = (os.environ.get(name) or default).strip().lower()
    if value not in allowed:
        raise ValueError(f"{name}={value!r} is not valid: use one of {', '.join(allowed)}")
    return value


def _positive(name: str, default: str) -> float:
    raw = os.environ.get(name, default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = float("nan")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name}={raw!r} is not valid: a positive number of seconds")
    return value


class FeedConfig:
    """The feed settings spe8's adapters.AdapterConfig read, minus the ones
    that only steered the old strategy's read points."""

    def __init__(self, *, private: bool) -> None:
        self.user_ws = _choice("USER_WS", "on", ("on", "off"))
        self.reconcile = _choice("RECONCILE", "auto", ("auto", "off"))
        self.btc_stale_after = _positive("BTC_STALE_AFTER", "3.0")
        self.book_stale_after = _positive("BOOK_STALE_AFTER", "8.0")
        if not private:
            # Paper and health never touch the wallet's private surfaces.
            self.user_ws = "off"
            self.reconcile = "off"

    def describe(self) -> str:
        return (f"USER_WS={self.user_ws} RECONCILE={self.reconcile} "
                f"BOOK_STALE_AFTER={self.book_stale_after:g}s")


def creds_from(obj) -> dict:
    """Normalise whatever the SDK hands back into the WS auth shape."""
    if obj is None:
        return {}
    get = obj.get if isinstance(obj, dict) else (lambda k: getattr(obj, k, None))
    out = {
        "apiKey": get("api_key") or get("apiKey") or get("key"),
        "secret": get("api_secret") or get("secret"),
        "passphrase": get("api_passphrase") or get("passphrase"),
    }
    return out if all(out.values()) else {}


def derive_creds() -> dict:
    """L2 API credentials for the user channel. Read off the already
    authenticated client first; never logged, never written, never in an
    exception message."""
    if (os.environ.get("USER_WS") or "on").lower() == "off":
        return {}
    try:
        import polymarket_trade
        client = polymarket_trade._get_client()
    except Exception as exc:
        on_event("user_ws", f"no CLOB client: {type(exc).__name__}", "warn")
        return {}
    for attr in ("creds", "api_creds", "_api_creds"):
        out = creds_from(getattr(client, attr, None))
        if out:
            return out
    for attempt in ("create_or_derive_api_key", "create_or_derive_api_creds", "derive_api_key"):
        fn = getattr(client, attempt, None)
        if not callable(fn):
            continue
        try:
            out = creds_from(fn())
            if out:
                return out
        except Exception as exc:
            on_event("user_ws", f"{attempt} failed: {type(exc).__name__}", "warn")
    for attr in ("creds", "api_creds", "_api_creds"):
        out = creds_from(getattr(client, attr, None))
        if out:
            return out
    on_event("user_ws", "could not derive L2 credentials; user channel will stay idle. "
                        "Verify the wallet/signature configuration without printing credentials.", "warn")
    return {}


def build_hub(*, paper: bool = False, read_only: bool = False):
    """The feed hub. Paper and health launches never derive wallet credentials."""
    import orderbook
    private = not (paper or read_only)
    cfg = FeedConfig(private=private)
    if private and cfg.user_ws == "off" and cfg.reconcile == "off":
        raise RuntimeError("live mode requires USER_WS=on or RECONCILE=auto for fill accounting")
    credentials = derive_creds() if private else {}
    if private and cfg.user_ws == "on" and not credentials:
        raise RuntimeError("live USER_WS is enabled but L2 credentials could not be derived "
                           "(CLOB create/derive timed out or failed). Check connectivity to CLOB_HOST and retry.")
    hub = FeedHub(creds=credentials, on_event=on_event,
                  btc_stale_after=cfg.btc_stale_after, book_stale_after=cfg.book_stale_after,
                  rest_book_fetch=orderbook.get_orderbook)
    return hub, cfg


# ---------------------------------------------------------------- live fill accounting
def fetch_trades(markets=()):
    """REST backup source for fills. Every request carries an explicit recent
    bot condition ID: an unfiltered account query would mix manual trades
    into the sniper's P&L."""
    selected = tuple(dict.fromkeys(str(m) for m in markets if m))
    if not selected:
        on_event("reconcile", "no explicit market filters; REST reconcile skipped", "warn")
        return []
    try:
        import polymarket_trade
        from py_clob_client_v2 import TradeParams
        client = polymarket_trade._get_client()
    except Exception as exc:
        detail = f"cannot open CLOB client: {type(exc).__name__}"
        on_event("reconcile", detail, "warn")
        # Not an authoritative empty result: raising keeps the generation due.
        raise RuntimeError(detail) from exc
    fn = getattr(client, "get_trades", None)
    if callable(fn):
        merged = {}
        try:
            after = int(time.time()) - int(Ledger.RECOVERY_LOOKBACK_SECONDS)
            rejected = 0
            for condition in selected:
                rows = fn(TradeParams(market=condition, after=after))
                if rows is not None and isinstance(rows, (str, bytes, dict)):
                    raise TypeError("get_trades() returned a non-list payload")
                for row in rows or ():
                    if isinstance(row, dict):
                        # Never trust a server/client filter as an accounting boundary.
                        if str(row.get("market") or "") != condition:
                            rejected += 1
                            continue
                        key = str(row.get("id") or row.get("trade_id") or "")
                        if key:
                            merged[key] = row
                    else:
                        rejected += 1
            if rejected:
                on_event("reconcile", f"rejected {rejected} malformed/cross-market row(s)", "warn")
        except Exception as exc:
            detail = f"get_trades() failed: {type(exc).__name__}: {exc}"
            on_event("reconcile", detail, "warn")
            raise RuntimeError(detail) from exc
        return list(merged.values())
    detail = "no trades method on the CLOB client - REST backup is INERT, fills rely on the user socket alone"
    on_event("reconcile", detail, "bad")
    raise RuntimeError(detail)


def _reconcile_markets(ledger, hub) -> tuple[str, ...]:
    """Stable union of durable authorizations and in-process discovery."""
    return tuple(dict.fromkeys((
        *ledger.recovery_conditions(lookback_s=Ledger.RECOVERY_LOOKBACK_SECONDS),
        *hub.recent_markets(),
    )))


async def _recover_startup_fills(reconciler, ledger, store) -> tuple[int, int]:
    """Finish one REST -> FillStore -> Ledger transaction before trading starts."""
    if reconciler is None:
        return 0, 0
    recovered = await reconciler.run_once()
    ingested = _persist_fill_drain(ledger, store)
    if reconciler.last_error:
        on_event("reconcile", "startup audit incomplete; background retries remain "
                 f"armed: {reconciler.last_error}", "warn")
    elif recovered or ingested:
        on_event("reconcile", f"startup audit recovered {recovered} trade record(s), "
                 f"booked {ingested} authorized fill(s)", "warn")
    return recovered, ingested


def _persist_fill_drain(ledger, store) -> int:
    """Synchronously ingest a feed snapshot and durably save all counters."""
    ingested = ledger.ingest_fill_store(store)
    if not ledger.save():
        raise RuntimeError("ledger save failed after fill-store drain")
    return ingested


def _require_live_fill_coverage(*, paper: bool, user_ws: str) -> None:
    """The private stream is LIVE's primary fill source; REST is backup only."""
    if paper or user_ws == "on":
        return
    raise RuntimeError("live trading requires USER_WS=on; REST reconcile is backup-only")


def _install_io_executor():
    """Size the thread pool for blocking network reads, not for CPU count:
    on a 1-vCPU VPS the default is five workers, and the settlement poll alone
    can hold one for seconds - further to_thread calls then queue and the bot
    looks frozen. Returns the executor so the caller can shut it down."""
    try:
        from concurrent.futures import ThreadPoolExecutor
        loop = asyncio.get_running_loop()
        workers = max(32, (os.cpu_count() or 1) + 4)
        executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sniper-io")
        loop.set_default_executor(executor)
        on_event("startup", f"io thread pool sized to {workers} workers ({os.cpu_count() or 1} cpu)", "info")
        return executor
    except Exception as exc:
        on_event("startup", f"could not resize io pool: {type(exc).__name__}", "warn")
        return None


def _env_seconds(name: str, default: float) -> float:
    """A non-negative number of seconds; a typo falls back with a warning."""
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        value = float("nan")
    if not math.isfinite(value) or value < 0:
        print(f"{TAG} {name}={raw!r} is not a number of seconds; using {default}")
        return default
    return value


# ---------------------------------------------------------------- gates
def _live_sniper_gate(paper: bool) -> bool:
    """True when this run is the LIVE CANARY. Checked before anything starts.

    This build is the sniper and nothing else, so SNIPER_ONLY=1 is required in
    both modes (it also picks the sniper's sniper-only defaults, e.g. FAK).
    LIVE trades real money, so it needs SNIPER_LIVE_CANARY=1 as well; a Kraken
    trigger trades live only with the explicit SNIPER_KRAKEN_LIVE=1."""
    import sniper_mode
    if not sniper_mode.only():
        raise RuntimeError("this bot is the BTC sniper only: set SNIPER_ONLY=1 in .env (see .env.example)")
    if paper:
        if sniper_mode.live_canary():
            print(f"{TAG} SNIPER_LIVE_CANARY is ignored in paper")
        return False
    if (str(os.environ.get("SNIPER_PRIMARY_FEED", "")).strip().lower() == "kraken"
            and not sniper_mode.kraken_live()):
        raise RuntimeError("SNIPER_PRIMARY_FEED=kraken is PAPER only: refusing --live. To trade live on "
                           "Kraken's trigger, set SNIPER_KRAKEN_LIVE=1 as well (an explicit choice)")
    if not sniper_mode.live_canary():
        raise RuntimeError("SNIPER_ONLY with --live trades real money with the sniper: refusing to start LIVE "
                           "unless SNIPER_LIVE_CANARY=1 is set too")
    if str(os.environ.get("POSITION_MANAGER_LIVE_ENABLED", "")).strip().lower() in ("1", "true", "yes", "on"):
        raise RuntimeError("POSITION_MANAGER_LIVE_ENABLED=1: real Position Manager orders are not implemented "
                           "until its paper/replay results are reviewed (replay_ab.py). Unset it; with "
                           "POSITION_MANAGER_ENABLED=1 the manager runs shadow-only in LIVE (decisions, no orders).")
    # The resolved settings must be the frozen spec: an old .env value (or a
    # missing one, falling back to a code default that is not the spec)
    # used to change the strategy silently.
    import exec_latency
    import sniper_live
    bad = exec_latency.spec_violations(sniper_mode.Settings(), sniper_live.limits_from_env().max_order_shares)
    if bad:
        raise RuntimeError("refusing LIVE: the resolved sniper configuration is not the frozen spec\n  - "
                           + "\n  - ".join(bad) + "\nFix .env (see .env.example). A deliberate strategy change "
                           "goes in exec_latency.FROZEN_SPEC, reviewed - not in an environment variable.")
    return True


def ignored_settings(env=None) -> list[str]:
    """Old main-strategy settings present in the environment, with why they do nothing here."""
    env = os.environ if env is None else env
    return [f"{k}={env.get(k)!r} ignored: {why}" for k, why in IGNORED_SETTINGS.items()
            if str(env.get(k, "")).strip() not in ("", "0", "false", "off", "no")]


# ---------------------------------------------------------------- the run
@dataclasses.dataclass
class Runtime:
    """Everything one run owns, so shutdown and the dashboard read one object."""
    paper: bool
    hub: object
    cfg: FeedConfig
    stop: threading.Event = dataclasses.field(default_factory=threading.Event)
    ledger: object = None
    broker: object = None
    sniper: object = None
    live_exec: object = None
    reconciler: object = None
    settler: object = None
    pm: object = None                     # position_manager.PositionManager (off unless enabled)
    journal_path: Path | None = None
    trade_rows: list = dataclasses.field(default_factory=list)
    tasks: list = dataclasses.field(default_factory=list)
    sniper_task: asyncio.Task | None = None
    clock_detail: str = "not checked"
    clock_ok: bool | None = None
    started: float = dataclasses.field(default_factory=time.time)
    shutdown_log: list = dataclasses.field(default_factory=list)   # the order shutdown ran in (tests)

    @property
    def mode(self) -> str:
        return "PAPER" if self.paper else "LIVE"


def build_paper(rt: Runtime, *, paper_balance: float | None = None) -> None:
    """PAPER: a paper ledger and broker on Polymarket's real book, the Sniper on top."""
    import config
    import polymarket_trade
    import sniper_mode
    from paper_trade import PaperBroker, fetch_executable_book
    # Paper never obtains a live client, signs with the wallet or posts an order.
    polymarket_trade.disable_live_execution()
    hub = rt.hub
    paths = _paper_state_paths()
    rt.ledger = Ledger(path=str(paths["ledger"]), category=os.environ.get("MARKET_CATEGORY", "crypto"),
                       allow_sells=config.STOP_LOSS_ENABLED or config.FLIP_EXIT_ENABLED)
    starting = (paper_balance if paper_balance is not None else
                float(os.environ.get("PAPER_START_BALANCE", "1000")))
    rt.broker = PaperBroker(
        rt.ledger,
        market_context=lambda: {"condition_id": hub.condition_id, "up_token_id": hub.up_token,
                                "down_token_id": hub.down_token},
        host=config.CLOB_HOST,
        max_buy_price=config.MAX_BUY_PRICE,
        min_buy_price=config.MIN_BUY_PRICE,
        start_balance=starting,
        account_path=paths["account"],
        audit_path=paths["audit"],
        category=os.environ.get("MARKET_CATEGORY", "crypto"),
        latency_ms=config.PAPER_LATENCY_MS,
        max_book_age_s=config.ORDERBOOK_MAX_AGE_SECONDS,
        max_quiet_s=config.ORDERBOOK_MAX_QUIET_SECONDS,
        future_tol_s=config.ORDERBOOK_FUTURE_TOLERANCE_SECONDS,
        max_spread=config.MAX_ALLOWED_SPREAD,
        min_seconds_to_expiry=config.MIN_SECONDS_TO_EXPIRY,
        trade_window_seconds=config.EXECUTION_WINDOW_SECONDS,
        fill_delay_probes=config.PAPER_FILL_DELAY_PROBES,
        flagged_delay_ms=config.PAPER_FLAGGED_DELAY_SECONDS * 1000.0,
        # Our own paper buys leave the public book untouched; live they would not.
        own_fill_ttl_s=_env_seconds("PAPER_OWN_FILL_TTL_S", 2.0),
        book_fetch=lambda token: fetch_executable_book(token, host=config.CLOB_HOST,
                                                       ws_view=hub.book.view(str(token))),
        on_event=on_event,
    )
    audit = Path(paths["audit"])
    rt.journal_path = audit.with_name(f"{audit.stem}_sniper.jsonl")
    rt.sniper = sniper_mode.Sniper(hub, rt.broker, on_event=on_event, journal_path=rt.journal_path,
                                   trade_rows=rt.trade_rows, own_rounds=True)
    rt.pm = build_manager(rt, broker=rt.broker)


async def build_live(rt: Runtime) -> None:
    """LIVE CANARY: the live ledger authorizes every accepted order before its
    fills can count; the lean sender executes the Sniper's decisions."""
    import config
    import polymarket_trade
    import sniper_live
    import sniper_mode
    hub = rt.hub
    ledger = Ledger(path=str(_state_path("LEDGER_PATH", "ledger.json")),
                    category=os.environ.get("MARKET_CATEGORY", "crypto"),
                    fee_resolver=polymarket_trade.market_fee_parameters,
                    allow_sells=config.STOP_LOSS_ENABLED or config.FLIP_EXIT_ENABLED)
    rt.ledger = ledger

    def journal_live_order(receipt: dict) -> bool:
        order_id = receipt.get("order_id")
        ledger.authorize_order(order_id, receipt, venue_min_shares=config.VENUE_MIN_SHARES,
                               price_cap=config.MAX_BUY_PRICE)
        if not ledger.save():
            raise RuntimeError("authorized-order ledger save failed")
        return True

    polymarket_trade.set_order_observer(journal_live_order)
    # Live diagnostics (live_attempts.jsonl, live_fill_timing.jsonl) go beside
    # the bot - only ever switched on here.
    polymarket_trade._telemetry_dir = ROOT
    rt.live_exec = await asyncio.to_thread(
        sniper_live.build_live_execution, ledger=ledger, on_event=on_event,
        user_ready=hub.user.ready_for_market, fills=hub.fill_store)
    rt.journal_path = _state_path("LIVE_SNIPER_JOURNAL", "live_sniper.jsonl")
    rt.sniper = sniper_mode.Sniper(hub, rt.live_exec.shim, on_event=on_event, journal_path=rt.journal_path,
                                   trade_rows=rt.trade_rows, own_rounds=True, live=rt.live_exec)
    rt.pm = build_manager(rt, broker=rt.live_exec.shim)


def start_lines(rt: Runtime) -> list[str]:
    """What this run is, printed at every start (and kept in the event ring)."""
    import config
    mode = ("PAPER (NO WALLET / NO SIGNATURE / NO LIVE ORDERS)" if rt.paper
            else "LIVE CANARY (REAL ORDERS UNDER THE CANARY LIMITS)")
    lines = [f"MODE={mode} | {rt.cfg.describe()}"]
    if rt.sniper is not None:
        lines += list(rt.sniper.lines()) + list(rt.sniper.plan_gaps())
    lines.append(f"account band {config.MIN_BUY_PRICE:g}-{config.MAX_BUY_PRICE:g}, trades from "
                 f"{config.TRADE_LAST_SECONDS}s to {config.MIN_SECONDS_TO_EXPIRY:g}s left")
    lines.append(f"state: ledger {getattr(rt.ledger, 'path', '--')}; journal {rt.journal_path}")
    if rt.sniper is not None:
        import exec_latency
        limits = getattr(getattr(rt.live_exec, "sender", None), "limits", None)
        cap = getattr(limits, "max_order_shares", None)
        lines += exec_latency.effective_config_lines(rt.sniper.s, live=not rt.paper, canary_max_shares=cap)
        if rt.paper:
            lines += [f"WARNING paper differs from the frozen spec (LIVE would refuse): {v}"
                      for v in exec_latency.spec_violations(rt.sniper.s, cap)]
        if rt.pm is not None:
            lines += rt.pm.cfg.lines(live_mode=not rt.paper)
        if "pure-Python" in exec_latency.signing_backend():
            lines.append("WARNING orders are signed with the pure-Python ECDSA (~1.4ms vs ~0.1ms): pip install coincurve")
    lines += ignored_settings()
    return lines


def build_manager(rt: Runtime, *, broker):
    """The Position Manager: a separate consumer of the books, the ledger and
    the sniper's state. Off unless POSITION_MANAGER_ENABLED=1; paper orders in
    paper, shadow-only (no orders) in LIVE. Its limits never exceed the global ones."""
    import config
    import position_manager
    cfg = position_manager.PMConfig.from_env(global_max_buy_price=config.MAX_BUY_PRICE,
                                             global_max_side_shares=rt.sniper.s.max_side_shares,
                                             global_max_unsettled_usd=config.MAX_UNSETTLED_EXPOSURE)
    journal = rt.journal_path.with_name(f"{rt.journal_path.stem}_manager.jsonl") if rt.journal_path else None
    return position_manager.PositionManager(cfg, hub=rt.hub, ledger=rt.ledger, broker=broker, sniper=rt.sniper,
                                            on_event=on_event, journal_path=journal if cfg.enabled else None,
                                            live_mode=not rt.paper)


def build_settler(rt: Runtime) -> SettlementWorker:
    """The settlement worker: one record per market (condition id), checked
    from the moment it ends - the chain first (~1 minute after a round), the
    CLOB + Gamma mirrors as fallback - on its own thread pool, never the
    order path's. Its lifecycle events also go to <ledger>_settlements.jsonl."""
    import timer
    from accounting import resolution
    ledger_path = Path(rt.ledger.path)
    redeem_check = None
    if not rt.paper:
        holder = _wallet_holder()
        if holder is None:
            on_event("settle", "no wallet address to read: REDEEM PENDING is shown but not tracked", "warn")
        else:
            import onchain_resolution
            redeem_check = functools.partial(onchain_resolution.balance_of, holder)
    return SettlementWorker(
        rt.ledger, interval=1.0,
        fetch=resolution.Resolver(api_every_s=_env_seconds("SETTLE_API_EVERY_S", 5.0) or 5.0),
        on_event=on_event, journal_path=ledger_path.with_name(f"{ledger_path.stem}_settlements.jsonl"),
        paper=rt.paper, redeem_check=redeem_check, clock=timer.unix, poll_active=False,
        timeout_s=_env_seconds("SETTLE_TIMEOUT_S", 900.0) or 900.0)


def _wallet_holder() -> str | None:
    """The address that holds the outcome tokens: POLY_FUNDER (proxy wallets),
    else the signer's own address. Derived in memory; never logged."""
    import config
    if config.POLY_FUNDER:
        return str(config.POLY_FUNDER)
    try:
        import polymarket_trade
        from eth_account import Account
        key = polymarket_trade._normalize_private_key(os.environ.get("POLY_PRIVATE_KEY")
                                                      or os.environ.get("PRIVATE_KEY"))
        return Account.from_key(key).address if key else None
    except Exception:
        return None


def _tracker(settler):
    """rotation -> settlement: every confirmed round is tracked by its condition id."""
    def on_round(tokens: dict) -> None:
        if settler is None or not tokens:
            return
        try:
            settler.track(tokens.get("condition_id"), tokens.get("window_start"), tokens.get("window_end"),
                          up_token=tokens.get("up_token_id"), down_token=tokens.get("down_token_id"),
                          slug=tokens.get("slug"))
        except Exception as exc:
            on_event("settle", f"SETTLEMENT_ERROR | could not track the round: {type(exc).__name__}: {exc}", "bad")
    return on_round


async def run(dash: bool = False, *, paper: bool = True, paper_balance: float | None = None) -> None:
    configured = os.environ.get("BOT_LOCK_PATH")
    lock_path = Path(configured) if configured else ROOT / (".btc_bot_paper.lock" if paper else ".btc_bot_live.lock")
    if not lock_path.is_absolute():
        lock_path = ROOT / lock_path
    # Gates first: they read only the environment, so a refused start costs nothing.
    _live_sniper_gate(paper)
    check_other_bots(paper=paper, own_lock=lock_path)
    process_lock = _ProcessLock(lock_path)
    process_lock.acquire()
    executor = _install_io_executor()
    try:
        hub, cfg = build_hub(paper=paper)
        rt = Runtime(paper=paper, hub=hub, cfg=cfg)
        await run_runtime(rt, dash=dash, paper_balance=paper_balance)
    finally:
        process_lock.release()
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)


async def _dns_check() -> None:
    """Name a hijacked resolver once, rather than leaving it to be inferred
    from a stream of TLS errors that reads like a venue outage. Diagnostic only."""
    try:
        import http_pool
        status, detail = await asyncio.to_thread(http_pool.dns_redirect_report)
        if status == "redirected":
            on_event("dns", f"Polymarket DNS is redirected: {detail}", "bad")
            print("[DNS] Polymarket name resolution is redirected.", file=sys.stderr)
            print(f"      {detail}", file=sys.stderr)
            print("      Venue calls will fail TLS: market discovery and every order fail here.", file=sys.stderr)
        elif status == "unknown":
            on_event("dns", f"DNS check inconclusive: {detail}", "warn")
    except Exception as exc:
        on_event("dns", f"DNS check failed: {type(exc).__name__}", "warn")


async def _clob_warm() -> None:
    """LIVE: derive the CLOB client's credentials before trading, not inside
    the first order (up to ~63s of retried HTTP)."""
    try:
        import polymarket_trade
        t0 = time.monotonic()
        await asyncio.to_thread(polymarket_trade._get_client)
        on_event("live", f"CLOB client ready in {(time.monotonic() - t0) * 1000.0:.0f}ms", "good")
    except Exception as exc:
        on_event("live", f"CLOB client not pre-warmed ({type(exc).__name__}); "
                         f"the first order will derive credentials itself", "warn")


async def run_runtime(rt: Runtime, *, dash: bool = False, paper_balance: float | None = None) -> None:
    """Build, start, wait for a stop, shut down in order. Everything that sets
    process-global state is inside the try, so a failed start is undone too."""
    import config
    dns_task = asyncio.create_task(_dns_check(), name="dns-check")
    warm_task = None if rt.paper else asyncio.create_task(_clob_warm(), name="clob-warm")
    dash_task = None
    failure: BaseException | None = None
    try:
        if rt.paper:
            build_paper(rt, paper_balance=paper_balance)
        else:
            await build_live(rt)
        # Prove the accounting directory is writable before any order can happen.
        if not rt.ledger.save():
            raise RuntimeError("accounting ledger is not writable; refusing to start")
        rt.settler = build_settler(rt)
        if rt.cfg.reconcile == "auto":
            rt.reconciler = RestReconciler(rt.hub.fill_store,
                                           lambda: fetch_trades(_reconcile_markets(rt.ledger, rt.hub)),
                                           user_feed=rt.hub.user, known_trade=rt.ledger.rest_trade_is_booked,
                                           on_event=on_event)
        for line in start_lines(rt):
            print(f"{TAG} {line}", flush=True)
            on_event("config", line, "info")

        if dash:
            dash_task = asyncio.create_task(_dashboard(rt), name="dashboard")
            dash_task.add_done_callback(lambda task: _task_failure_event(task, "dashboard"))

        # Measure CLOB time before any feed timestamp is judged.
        import timer
        rt.clock_ok, rt.clock_detail, drift = await asyncio.to_thread(
            timer.check_clock, config.CLOB_HOST, config.CLOCK_MAX_DRIFT_SECONDS)
        if drift is not None:
            on_event("clock", f"{rt.clock_detail}; offset {timer.clock_offset():+.3f}s",
                     "info" if rt.clock_ok else "warn")
        else:
            on_event("clock", rt.clock_detail, "warn")

        # A restart can follow an authorized order whose fill never reached the
        # ledger: book it before the sniper can place another.
        await _recover_startup_fills(rt.reconciler, rt.ledger, rt.hub.fill_store)
        # ... and count what the ledger already holds into the per-round caps:
        # a restart mid-round must not start that round's caps from zero.
        seeded = await asyncio.to_thread(rt.sniper.seed_rounds, rt.ledger)
        if seeded:
            on_event("startup", f"round caps start from the ledger: {seeded} open position(s) counted", "info")
        _require_live_fill_coverage(paper=rt.paper, user_ws=rt.cfg.user_ws)
        if rt.live_exec is not None:
            try:
                await rt.live_exec.start(rt.hub)
            except Exception as exc:
                raise RuntimeError(f"live canary could not arm: {type(exc).__name__}: {exc}") from None

        # The sniper runs its own trigger socket, so the hub's Binance trade
        # feed would be a second, unused BTC socket on a 1-vCPU box.
        rt.tasks += rt.hub.start(user=rt.cfg.user_ws == "on", binance=False)
        rt.sniper_task = asyncio.create_task(rt.sniper.run(), name="sniper")
        rt.tasks.append(rt.sniper_task)
        if rt.reconciler is not None:
            rt.tasks.append(rt.reconciler.start())
        rt.tasks.append(asyncio.create_task(_rotation_loop(rt.hub, rt.stop, on_round=_tracker(rt.settler)),
                                            name="rotation"))
        rt.tasks.append(rt.settler.start())
        if rt.pm is not None and rt.pm.cfg.enabled:
            rt.tasks.append(rt.pm.start())
        if not rt.paper:
            rt.tasks.append(asyncio.create_task(_ledger_loop(rt.hub, rt.ledger, rt.stop), name="ledger"))
        if dash_task is not None:
            rt.tasks.append(dash_task)
        else:
            rt.tasks.append(asyncio.create_task(_status_loop(rt), name="status"))

        # Pre-open the venue connections (DNS + TCP + TLS can take seconds).
        import http_pool
        import market_discovery
        hosts = (f"{config.CLOB_HOST.rstrip('/')}/time", market_discovery.GAMMA)
        warmed, *_ = await asyncio.gather(
            asyncio.gather(*(asyncio.to_thread(http_pool.warm, *hosts) for _ in range(2))),
            dns_task, *( [warm_task] if warm_task else []))
        on_event("bot", f"venue connections pre-warmed ({min(warmed)}/{len(hosts)} hosts on "
                        f"{len(warmed)} workers)", "good" if min(warmed) == len(hosts) else "warn")
        print(f"{TAG} running - Ctrl+C or SIGTERM stops it" + ("" if dash else
              "; a status line every 60s"), flush=True)
        failure = await _wait_until_stopped(rt)
    except asyncio.CancelledError:
        pass                                    # Ctrl+C / SIGTERM: shut down in order below
    finally:
        for t in (dns_task, warm_task):
            if t is not None and not t.done():
                t.cancel()
        if dash_task is not None and dash_task not in rt.tasks:
            rt.tasks.append(dash_task)
        await shutdown(rt)
    if failure is not None:
        raise RuntimeError(f"sniper task stopped: {type(failure).__name__}: {failure}") from failure


async def _wait_until_stopped(rt: Runtime) -> BaseException | None:
    """Until stop is set or the sniper task ends. A sniper that died leaves a
    bot that only looks alive, so that is a failed run, not a quiet idle."""
    while not rt.stop.is_set():
        task = rt.sniper_task
        if task is not None and task.done():
            exc = None if task.cancelled() else task.exception()
            on_event("sniper", f"sniper task ended: {exc!r}", "bad")
            return exc or RuntimeError("the sniper task returned")
        await asyncio.sleep(0.5)
    return None


async def shutdown(rt: Runtime) -> None:
    """Stop in an order that never loses a fill: no new orders, then the live
    sender, then the background services and feeds, then the observer; late
    fills are drained into the ledger last and the report is printed."""
    log = rt.shutdown_log
    rt.stop.set()
    if rt.live_exec is not None:
        rt.live_exec.kill.trip("shutting down")                 # no new orders from here on
        log.append("kill")
    if rt.sniper_task is not None and not rt.sniper_task.done():
        rt.sniper_task.cancel()                                 # no new decisions either
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await rt.sniper_task
        log.append("sniper")
    if rt.live_exec is not None:
        with contextlib.suppress(Exception):
            await rt.live_exec.stop()
        log.append("live_exec")
    pm = rt.pm if rt.pm is not None and rt.pm.cfg.enabled else None     # stopped (journal flushed) when it ran
    for name, service in (("reconciler", rt.reconciler), ("settler", rt.settler), ("manager", pm),
                          ("hub", rt.hub)):
        if service is None:
            continue
        try:
            await service.stop()
        except Exception as exc:
            on_event("shutdown", f"{name} stop failed: {type(exc).__name__}: {exc}", "bad")
        log.append(name)
    if not rt.paper:
        try:
            import polymarket_trade
            polymarket_trade.set_order_observer(None)
            polymarket_trade._telemetry_dir = None
        except Exception:
            pass
        log.append("observer")
    for t in rt.tasks:
        t.cancel()
    results = await asyncio.gather(*rt.tasks, return_exceptions=True)
    for task, result in zip(rt.tasks, results):
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            on_event("shutdown", f"task {task.get_name()} failed: {type(result).__name__}: {result}", "bad")
    log.append("tasks")
    traded = rt.sniper_task is not None
    if traded:
        print(f"\n{TAG} final health:")
    if rt.ledger is not None:
        try:
            drained = _persist_fill_drain(rt.ledger, rt.hub.fill_store)
            if drained:
                on_event("shutdown", f"durably booked {drained} late fill(s)", "warn")
        except Exception as exc:
            detail = f"final fill drain failed: {type(exc).__name__}: {exc}"
            on_event("shutdown", detail, "bad")
            print(f"{TAG} CRITICAL: {detail}")
        log.append("drain")
    if not traded:
        return                     # refused before trading: the reason is the message, not a report
    try:
        final = report(rt)
    except Exception as exc:
        on_event("shutdown", f"final report failed: {type(exc).__name__}: {exc}", "bad")
        final = {"error": safe_log_text(f"final report failed: {type(exc).__name__}: {exc}")}
    if rt.sniper is not None:
        try:
            final["sniper"] = rt.sniper.summary()
        except Exception as exc:
            final["sniper"] = {"error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(final, indent=2, default=str))
    log.append("report")


def _mark(hub):
    def mark(token):
        view = hub.book.view(token)
        return view.best_bid if view.status == "LIVE" else None
    return mark


def report(rt: Runtime) -> dict:
    out = {"mode": rt.mode, "feeds": rt.hub.health(), "fills": rt.hub.fill_store.summary(),
           "recent_events": [{"wall": w, "source": s, "text": t, "level": lv} for w, s, t, lv in recent_events(50)]}
    if rt.reconciler is not None:
        out["reconcile"] = rt.reconciler.summary()
    if rt.ledger is not None:
        mark = _mark(rt.hub)
        out["pnl"] = rt.ledger.summary(mark=mark)
        if rt.broker is not None:
            out["paper_account"] = rt.broker.summary(mark=mark)
            out["balance_check"] = {"status": "PAPER",
                                    "detail": "cash is derived from simulated fills and resolved payouts"}
        else:
            out["balance_check"] = rt.ledger.reconcile_balance()
    if rt.settler is not None:
        out["settlement"] = rt.settler.summary()
    if rt.pm is not None and rt.pm.cfg.enabled:
        out["position_manager"] = rt.pm.summary()
    if rt.live_exec is not None:
        out["canary"] = rt.live_exec.status()
    return out


def _task_failure_event(task: asyncio.Task, name: str) -> None:
    if task.cancelled():
        return
    try:
        exc = task.exception()
    except (asyncio.CancelledError, Exception) as inspect_exc:
        on_event(name, f"cannot inspect task result: {type(inspect_exc).__name__}: {inspect_exc}", "bad")
        return
    if exc is not None:
        on_event(name, f"task stopped unexpectedly: {type(exc).__name__}: {exc}", "bad")


# ---------------------------------------------------------------- background loops
async def _rotation_loop(hub, stop, on_round=None) -> None:
    """Discover this round's tokens EARLY, so the book socket is warm long
    before the sniper reads it, and pre-subscribe the next round's books.
    `on_round(tokens)` hears every confirmed round (the settlement tracker)."""
    import config
    import market_discovery
    import timer
    # A discovery call can block ~21s; never start a prewarm that close to a boundary.
    PREPARE_FLOOR_SECONDS = 25.0
    last_key = None
    cleared_key = None
    prepared_key = None
    while not stop.is_set():
        try:
            sampled = timer.unix()
            window = timer.window_start(sampled)
            key = window
            if key != cleared_key:
                # Remove the previous round from every read surface at once. A
                # prewarmed next book stays subscribed but is not promoted until
                # fresh discovery agrees.
                hub.set_round(None, None, None, window, window + 300)
                cleared_key = key
                last_key = None
                prepared_key = None
            if key != last_key or not hub.up_token or not hub.down_token:
                tokens = await asyncio.to_thread(market_discovery.get_tokens_for_current_round, window)
                now_window = timer.window_start()
                if (tokens and int(tokens.get("window_start") or 0) == window and now_window == window):
                    changed = hub.set_round(tokens.get("up_token_id"), tokens.get("down_token_id"),
                                            tokens.get("condition_id"), tokens.get("window_start"),
                                            tokens.get("window_end"))
                    if changed:
                        on_event("rotation", f"round tokens subscribed with {timer.seconds_left()}s left", "info")
                    if on_round is not None:
                        on_round(tokens)
                    last_key = key
            # Re-read the clock: discovery above may have blocked for seconds.
            remain_now = timer.seconds_left()
            if (PREPARE_FLOOR_SECONDS <= remain_now <= config.ROUND_PREPARE_LEAD_SECONDS
                    and prepared_key != window + 300):
                next_window = window + 300
                prepared = await asyncio.to_thread(market_discovery.get_tokens_for_current_round, next_window)
                if prepared and int(prepared.get("window_start") or 0) == next_window:
                    if hub.prepare_round(prepared):
                        on_event("rotation", f"next round books pre-subscribed with "
                                             f"{timer.seconds_left()}s left", "good")
                    prepared_key = next_window
        except Exception as exc:
            on_event("rotation", f"{type(exc).__name__}: {exc}", "warn")
        # Poll every second across the boundary, back off mid-round (gamma
        # rate-limits), and never sleep past a boundary however long discovery took.
        sampled_now = timer.unix()
        to_boundary = (timer.window_start(sampled_now) + 300) - sampled_now
        near_boundary = to_boundary <= 10.0 or to_boundary >= 290.0
        delay = 1.0 if near_boundary else config.ROUND_POLL_SECONDS
        await asyncio.sleep(max(0.1, min(delay, to_boundary + 0.05)))


async def _ledger_loop(hub, ledger, stop) -> None:
    """LIVE: pull venue-reported fills into the ledger and mark the wallet,
    on a worker thread (a save rewrites and fsyncs the whole file)."""
    last_save = 0.0
    last_balance = 0.0
    while not stop.is_set():
        await asyncio.sleep(2.0)
        try:
            if await asyncio.to_thread(ledger.ingest_fill_store, hub.fill_store):
                if not await asyncio.to_thread(ledger.save):
                    raise RuntimeError("ledger save failed after confirmed fill")
                last_save = time.monotonic()
            if time.monotonic() - last_balance > 120:
                last_balance = time.monotonic()
                try:
                    import polymarket_trade
                    bal = await asyncio.to_thread(polymarket_trade.get_balance_allowance)
                    if bal and bal.get("balance") is not None:
                        ledger.mark_balance(bal["balance"])
                except Exception as exc:
                    on_event("ledger", f"balance read failed: {type(exc).__name__}", "warn")
            if time.monotonic() - last_save > 60:
                if not await asyncio.to_thread(ledger.save):
                    raise RuntimeError("periodic ledger save failed")
                last_save = time.monotonic()
        except Exception as exc:
            on_event("ledger", f"{type(exc).__name__}: {exc}", "warn")


async def _status_loop(rt: Runtime) -> None:
    """Without --dash: one compact line a minute, safe for a log file."""
    view = DashView(rt, accounting_off_loop=True)
    while not rt.stop.is_set():
        await asyncio.sleep(60)
        try:
            print(status_text(view.snapshot()), flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            on_event("status", f"status line failed: {type(exc).__name__}: {exc}", "warn")


# ---------------------------------------------------------------- dashboard
class JournalTail:
    """The sniper journal's order rows, read incrementally (new bytes only)
    from the file the Sniper writes - no hook into the Sniper itself."""

    KINDS = ("filled", "refused", "pending", "error", "live_fill_update")
    START_BYTES = 256 * 1024

    def __init__(self, path, keep: int = 40, since: float = 0.0) -> None:
        self.path = Path(path) if path else None
        self.since = since                                       # rows older than this run are not shown
        self.rows: collections.deque = collections.deque(maxlen=keep)
        self.last_signal: tuple[float, float] | None = None     # (wall, bp) of the newest decision row
        self._pos: int | None = None
        self._rest = b""

    def read(self) -> list[dict]:
        if self.path is None:
            return list(self.rows)
        try:
            size = self.path.stat().st_size
        except OSError:
            return list(self.rows)
        if self._pos is None or size < self._pos:               # first read, or the file was replaced
            self._pos, self._rest = max(0, size - self.START_BYTES), b""
            skip_partial = self._pos > 0
        else:
            skip_partial = False
        if size == self._pos:
            return list(self.rows)
        try:
            with open(self.path, "rb") as fh:
                fh.seek(self._pos)
                data = fh.read(size - self._pos)
        except OSError:
            return list(self.rows)
        self._pos += len(data)
        lines = (self._rest + data).split(b"\n")
        self._rest = lines.pop()                                 # a partial last line waits for its end
        if skip_partial and lines:
            lines = lines[1:]
        for raw in lines:
            try:
                row = json.loads(raw)
            except ValueError:
                continue
            if isinstance(row, dict):
                self._take(row)
        return list(self.rows)

    def _take(self, row: dict) -> None:
        try:
            if float(row.get("wall") or 0.0) < self.since:
                return
        except (TypeError, ValueError):
            return
        if row.get("bp") is not None and row.get("side"):
            try:
                self.last_signal = (float(row.get("wall") or 0.0), float(row["bp"]))
            except (TypeError, ValueError):
                pass
        result = row.get("result")
        if result not in self.KINDS:
            return
        if result == "live_fill_update":
            for old in self.rows:
                if row.get("order_id") and old.get("order_id") == row.get("order_id"):
                    old.update(self._shape(row, row.get("resolved_result") or "filled"))
                    return
            result = row.get("resolved_result") or "filled"
        self.rows.append(self._shape(row, result))

    @staticmethod
    def _shape(row: dict, result: str) -> dict:
        reason = str(row.get("reason") or row.get("outcome") or "")
        if result == "refused" and ("no-fill" in reason.lower() or row.get("kind") == "NO_MATCH"):
            result = "missed"
        try:
            stamp = time.strftime("%H:%M:%S", time.localtime(float(row.get("wall") or 0.0)))
        except (TypeError, ValueError, OverflowError, OSError):
            stamp = "--"
        confirmed = row.get("fill_confirmed_ms")
        detail = ("partial" if row.get("partial") else "" if result == "filled" else reason[:120])
        if confirmed is not None and result == "filled":
            detail = (detail + " " if detail else "") + f"confirmed {float(confirmed):.0f}ms"
        return {"time": stamp, "side": row.get("side"), "bp": row.get("bp"), "ask": row.get("ask_seen"),
                "cap": row.get("cap"), "result": result, "shares": row.get("shares"), "price": row.get("price"),
                "order_id": row.get("order_id"), "detail": detail,
                "window_ms": row.get("window_ms"), "fill_confirmed_ms": confirmed}


class DashView:
    """Builds the dashboard snapshot: plain values copied out of the hub, the
    Sniper, the broker/ledger and the canary. Call snapshot() on the event
    loop (it reads state the loop owns); orders() may run on a worker."""

    def __init__(self, rt: Runtime, *, accounting_every_s: float = 1.0, accounting_off_loop: bool = False) -> None:
        self.rt = rt
        # The bot passes True: the account figures walk every position the ledger
        # ever held - measured 2.4ms at 5,000 positions and 10ms at 20,000
        # (bench_loop_costs.py) - and on the event loop every BTC tick arriving
        # meanwhile waited for them. Off the loop, the newest finished result is
        # shown (one computation at a time, started at most once per refresh).
        self.off_loop = accounting_off_loop
        self._acct_future = None
        self.tail = JournalTail(rt.journal_path, since=getattr(rt, "started", 0.0) - 1.0)
        self._signals: collections.deque = collections.deque()
        self._acct: dict = {}
        self._acct_at = -1e9
        self._slow: dict = {}
        self._slow_at = -1e9
        self._positions: dict = {}
        self.every = accounting_every_s

    # ---- slow parts, refreshed once a second
    def _refresh_slow(self) -> None:
        now = time.monotonic()
        if now - self._slow_at < self.every:
            return
        self._slow_at = now
        try:
            self._slow["summary"] = self.rt.sniper.summary() if self.rt.sniper is not None else {}
        except Exception:
            self._slow["summary"] = {}
        try:
            self._slow["health"] = self.rt.hub.health()
        except Exception:
            self._slow["health"] = {}
        if self.off_loop:
            self._collect_accounting()
            if self._acct_future is not None:
                return                          # the previous one is still running
            try:
                self._acct_future = asyncio.get_running_loop().run_in_executor(None, self._accounting_nowait)
                return
            except RuntimeError:                # no running loop (a final report): compute here
                pass
        acct = self._accounting_nowait()
        if acct is not None:
            self._acct = acct

    def _collect_accounting(self) -> None:
        """Take a finished off-loop accounting result, if there is one."""
        fut = self._acct_future
        if fut is None or not fut.done():
            return
        self._acct_future = None
        try:
            acct = fut.result()
        except Exception as exc:                # the figures stay as they were; say why
            on_event("dashboard", f"accounting refresh failed: {type(exc).__name__}: {exc}", "warn")
            return
        if acct is not None:
            self._acct = acct

    def _accounting_nowait(self) -> dict | None:
        """Account figures and the POSITION table, or None while a fill or
        settlement holds the ledger lock (waiting for it froze every feed and
        the sniper for the write).

        Positions in the market trading now are marked to its live bid.
        Positions in markets that have ended and await their result have no
        book left to mark (their tokens are unsubscribed at rotation): they are
        carried at cost - neither a gain nor a loss until the venue's result
        settles them - so equity stays defined through settlement and
        cash + position value = equity, realized + unrealized = total P/L."""
        rt = self.rt
        ledger = rt.ledger
        if ledger is None:
            return {}
        lock = getattr(ledger, "_lock", None)
        if lock is not None and not lock.acquire(blocking=False):
            return None
        try:
            hub = rt.hub
            mark = _mark(hub)
            s = ledger.summary()
            cash = rt.broker.cash_balance() if rt.broker is not None else None
            active = str(hub.condition_id or "")
            sides = {str(t): side for side, t in (("UP", hub.up_token), ("DOWN", hub.down_token)) if t}
            table = {side: {"side": side, "shares": 0.0, "avg": None, "cost": 0.0} for side in ("UP", "DOWN")}
            pending = {"markets": set(), "shares": 0.0, "cost": 0.0}
            active_value, unrealized, unmarkable = 0.0, 0.0, 0
            for pos in ledger.positions.values():
                if pos.settled or pos.shares <= 0:
                    continue
                if active and pos.condition_id == active and str(pos.token_id) in sides:
                    row = table[sides[str(pos.token_id)]]
                    row.update(shares=float(pos.shares), avg=pos.avg_price, cost=float(pos.cost))
                    continue
                pending["markets"].add(pos.condition_id)
                pending["shares"] += float(pos.shares)
                pending["cost"] += float(pos.cost)
            for side, row in table.items():
                token = next((t for t, sd in sides.items() if sd == side), None)
                bid = None
                if token is not None:
                    try:
                        bid = mark(token)
                    except Exception:
                        bid = None
                row["bid"] = bid
                if row["shares"] <= 0:
                    row["value"], row["pnl"] = 0.0, 0.0
                elif bid is None:
                    row["value"] = row["pnl"] = None
                    unmarkable += 1
                else:
                    row["value"] = row["shares"] * float(bid)
                    row["pnl"] = row["value"] - row["cost"]
                    active_value += row["value"]
                    unrealized += row["pnl"]
            position_value = None if unmarkable else active_value + pending["cost"]
            realized = s.get("realized_pnl")
            out = {"realized": realized, "unrealized": None if unmarkable else unrealized,
                   "position_value": position_value, "awaiting_cost": pending["cost"],
                   "open": s.get("open_positions"), "pending_cost": s.get("pending_cost"),
                   "fees": s.get("fees_paid"), "wins": s.get("wins"), "losses": s.get("losses")}
            self._positions = {"rows": [table["UP"], table["DOWN"]],
                               "pending": {"markets": len(pending["markets"]), "shares": pending["shares"],
                                           "cost": pending["cost"]}}
            if rt.broker is not None:
                start = rt.broker.start_balance
                equity = None if position_value is None else cash + position_value
                out.update(kind="PAPER", cash=cash, equity=equity, start=start,
                           pnl=None if equity is None else equity - start,
                           total=None if equity is None else equity - start)
                return out
            bal = getattr(rt.live_exec, "balance", None)
            if bal is None and getattr(ledger, "balance_marks", None):
                bal = ledger.balance_marks[-1][1]
            try:
                check = ledger.reconcile_balance().get("status")
            except Exception:
                check = None
            total = None if out["unrealized"] is None or realized is None else realized + out["unrealized"]
            out.update(kind="LIVE", balance=bal, balance_status=check, total=total,
                       start=time.strftime("%m-%d %H:%M", time.localtime(rt.started)))
            return out
        except Exception:
            return None
        finally:
            if lock is not None:
                lock.release()

    def _settlement(self) -> dict | None:
        """The settlement worker's panel, with each market labelled by its
        window in ET (the dashboard clock's zone)."""
        settler = self.rt.settler
        if settler is None or not hasattr(settler, "panel"):
            return None
        import timer
        try:
            p = settler.panel(timer.unix(), active_condition=self.rt.hub.condition_id)
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        seen = []
        for v in [p.get("active"), p.get("subject"), p.get("last_settled")] + p["pending"] + p["previous"]:
            if v is None or any(v is x for x in seen):
                continue
            seen.append(v)
            v.pop("positions", None)
            start, end = v.get("window_start"), v.get("window_end")
            if start and end:
                v["label"] = (f"{timer.now_et(start).strftime('%H:%M')}-"
                              f"{timer.now_et(end).strftime('%H:%M')}")
            else:
                v["label"] = (v.get("slug") or str(v.get("condition_id") or "")[-8:]) or "--"
        return p

    # ---- the snapshot
    def snapshot(self) -> dict:
        rt, hub, sn = self.rt, self.rt.hub, self.rt.sniper
        self._refresh_slow()
        if self.off_loop:
            self._collect_accounting()
        summary = self._slow.get("summary") or {}
        health = self._slow.get("health") or {}
        now_wall, now_mono = time.time(), time.monotonic()
        import timer
        snap = {"mode": rt.mode, "clock": timer.now_et(now_wall).strftime("%H:%M:%S ET"),
                "account": dict(self._acct), "events": recent_events(40)}
        trig: dict = {}
        if sn is not None:
            s = sn.s
            q = sn.trigger.q
            mid = q[-1][1] if q else getattr(sn, "last_mid", None)
            move = (q[-1][1] / q[0][1] - 1.0) * 1e4 if len(q) >= 2 and q[0][1] else None
            n_sig = sn.n["signals"]
            self._signals.append((now_mono, n_sig))
            while len(self._signals) > 2 and now_mono - self._signals[0][0] > 300:
                self._signals.popleft()
            t0, n0 = self._signals[0]
            rate = (n_sig - n0) / (now_mono - t0) * 60.0 if now_mono - t0 >= 10 else None
            if s.primary_feed == "kraken":
                quiet = (now_mono * 1000.0 - q[-1][0]) / 1000.0 if q else None
                status = "LIVE" if quiet is not None and quiet < 10 else "STALE"
            else:
                status = sn.fresh.status(now_mono)
            last = self.tail.last_signal
            trig = {"feed": s.primary_feed, "status": status, "mid": mid, "move_bp": move, "k_bp": s.k_bp,
                    "window_ms": s.window_ms, "signals": n_sig, "signals_per_min": rate,
                    "orders": sn.n["orders"], "filled": sn.n["filled"], "refused": sn.n["refused"],
                    "fill_rate": summary.get("fill_rate"), "markout_1s_c": summary.get("markout_1s_c"),
                    "markout_5s_c": summary.get("markout_5s_c"), "order_type": s.order_type, "slip": s.slip,
                    "model_latency_ms": summary.get("latency_ms") if rt.paper else None,
                    "send_p50_ms": ((summary.get("exec_latency") or {}).get("trigger_to_send") or {}).get("p50"),
                    "ack_p50_ms": ((summary.get("exec_latency") or {}).get("trigger_to_ack") or {}).get("p50"),
                    "ack_simulated": ((summary.get("exec_latency") or {}).get("trigger_to_ack") or {}).get("simulated"),
                    "errors": sn.n["errors"],
                    "in_flight": "+".join(s for s, v in getattr(sn, "inflight", {}).items() if v) or None,
                    "last_signal_bp": last[1] if last else None,
                    "last_signal_age_s": now_wall - last[0] if last else None}
            skips = sorted(((k[6:], v) for k, v in sn.n.items() if k.startswith("skip: ") and v),
                           key=lambda kv: -kv[1])
            snap["skips"] = skips[:6]
        snap["trigger"] = trig
        snap["exec"] = {"latency": summary.get("exec_latency") or {}, "quality": summary.get("exec_quality") or {},
                        "paper": rt.paper}
        end = hub.window_end
        market = {"slug": f"btc-updown-5m-{hub.window_start}" if hub.window_start and hub.up_token else None,
                  "seconds_left": (float(end) - now_wall) if end else None,
                  "book_status": (health.get("book") or {}).get("status")}
        for side, token in (("up", hub.up_token), ("down", hub.down_token)):
            if not token:
                continue
            v = hub.book.view(str(token))
            age = None if v.updated_mono is None else (now_mono - v.updated_mono) * 1000.0
            market[side] = {"bid": v.best_bid, "ask": v.best_ask, "status": v.status, "age_ms": age}
        if sn is not None:
            with contextlib.suppress(Exception):
                market["trade_until_s"] = sn._window()[1]
            rd = sn.rounds.get(end) if end else None             # .get: never create a round entry
            market.update(round_fills=(rd or {}).get("fills", 0), round_up_sh=(rd or {}).get("UP", 0.0),
                          round_down_sh=(rd or {}).get("DOWN", 0.0), max_fills=sn.s.max_fills_round,
                          max_side_shares=sn.s.max_side_shares)
            if sn.s.order_type == "FAK" and hasattr(sn, "carry_view"):
                carry = sn.carry_view()
                market["carry"] = {side: {"carry": carry[side],
                                          "next": min(sn.s.shares + carry[side], sn.s.max_target_shares)}
                                   for side in ("UP", "DOWN")}
        snap["market"] = market
        settlement = self._settlement()
        snap["settlement"] = settlement
        snap["positions"] = dict(self._positions)
        pm = getattr(rt, "pm", None)
        if pm is not None and pm.cfg.enabled:
            snap["pm"] = pm.snapshot()
        if settlement and not rt.paper and snap["account"]:
            # LIVE: winning shares not yet redeemed are worth their payout but are
            # not in the collateral balance - shown, and counted in equity.
            acct = snap["account"]
            acct["redeemable"] = settlement.get("redeem_pending") or 0.0
            if acct.get("balance") is not None and acct.get("position_value") is not None:
                acct["equity"] = float(acct["balance"]) + acct["position_value"] + acct["redeemable"]
        snap["canary"] = self._canary()
        snap["health"] = self._health_rows(summary, health)
        return snap

    def _canary(self) -> dict | None:
        live = self.rt.live_exec
        if live is None:
            return None
        try:
            st = live.status()
            lim = live.sender.limits
            guard = st.get("guard") or {}
            return {"halted": st.get("halted") or [], "orders": st.get("orders"), "max_orders": lim.max_orders,
                    "fills": st.get("fills"), "max_fills": lim.max_fills, "spend": st.get("spend_usd"),
                    "max_spend": lim.max_exposure_usd, "rounds": st.get("rounds"), "max_rounds": lim.max_rounds,
                    "loss": st.get("session_loss_usd"), "max_loss": lim.max_daily_loss_usd,
                    "guard": guard.get("state"), "guard_reason": guard.get("reason"),
                    "balance": st.get("balance"), "awaiting": st.get("awaiting_confirmation"),
                    "rules_ready": st.get("rules_ready")}
        except Exception as exc:
            return {"halted": [f"canary status unavailable: {type(exc).__name__}"]}

    def _health_rows(self, summary: dict, health: dict) -> list[tuple[str, str, str]]:
        rt = self.rt
        rows = []
        b = summary.get("binance") or {}
        if (rt.sniper is not None and rt.sniper.s.primary_feed == "kraken"):
            k = summary.get("kraken") or {}
            lag = k.get("lag_ms") or {}
            p50 = "--" if lag.get("p50") is None else f"{lag['p50']}ms"
            rows.append(("KRAKEN", str(self._trig_status() or "--"),
                         f"lag p50 {p50} gap max {k.get('gap_ms_max')}ms rc {k.get('reconnects')} "
                         f"sys {k.get('system') or '--'}"))
        else:
            ex = b.get("excess_ms") or {}
            behind = "--" if ex.get("p50") is None else f"{ex['p50']}ms"
            rows.append(("BINANCE", str(b.get("status") or "--"),
                         f"behind p50 {behind} backlog max {b.get('backlog_max')} rc {b.get('reconnects')}"))
        book = health.get("book") or {}
        pm = health.get("poly_market") or {}
        rows.append(("POLY BOOK", str(book.get("status") or "--"),
                     f"rc {pm.get('reconnect_count')} resync {book.get('rest_resyncs')}"))
        if rt.paper:
            rows.append(("USER WS", "ABSENT", "paper"))
        else:
            pu = health.get("poly_user") or {}
            rows.append(("USER WS", str(pu.get("status") or "--"),
                         f"fills {rt.hub.fill_store.summary().get('fills')}"))
            rows.append(("RECONCILE", "ABSENT" if rt.reconciler is None else
                         ("OK" if not rt.reconciler.armed else "WAIT"), ""))
        rows.append(("SETTLE", rt.settler.health_status() if rt.settler is not None else "ABSENT",
                     self._settle_detail()))
        rows.append(("LEDGER", "ABSENT" if rt.ledger is None else
                     ("ERROR" if rt.ledger.last_persistence_error else "OK"), ""))
        rows.append(("CLOCK", "OK" if rt.clock_ok else ("WAIT" if rt.clock_ok is None else "WARN"),
                     safe_log_text(rt.clock_detail, limit=60)))
        lag = summary.get("loop_lag_ms") or {}
        if lag:
            rows.append(("LOOP LAG", "OK" if (lag.get("p99") or 0) < 50 else "WARN",
                         f"p50 {lag.get('p50')}ms p99 {lag.get('p99')}ms"))
        return rows

    def _settle_detail(self) -> str:
        settler = self.rt.settler
        try:
            lat = settler.latency()
            pending = sum(1 for r in settler.records.values() if r.state in ("PENDING SETTLEMENT", "SETTLING"))
        except Exception:
            return ""
        last = lat.get("settled_after_expiry_s") or {}
        return (f"pending {pending}" + (f"  settled p50 {last['p50']:.1f}s n{last['n']}" if last else "")
                + (f"  err {settler.last_error[:40]}" if settler.last_error else ""))

    def _trig_status(self) -> str | None:
        sn = self.rt.sniper
        q = sn.trigger.q if sn is not None else None
        if not q:
            return "STALE"
        return "LIVE" if time.monotonic() * 1000.0 - q[-1][0] < 10_000 else "STALE"

    def orders(self) -> list[dict]:
        return self.tail.read()


def status_text(snap: dict) -> str:
    """The 60-second status line (no dashboard)."""
    t, m, a = snap.get("trigger") or {}, snap.get("market") or {}, snap.get("account") or {}
    health = {name: (state, detail) for name, state, detail in snap.get("health") or []}
    left = m.get("seconds_left")
    pnl = a.get("pnl") if a.get("kind") == "PAPER" else a.get("realized")
    skips = ", ".join(f"{k} {v}" for k, v in (snap.get("skips") or [])[:3])
    canary = snap.get("canary")
    st = (snap.get("settlement") or {}).get("subject") or {}
    settle = ""
    if st:
        settle = f"settle {st.get('label') or '--'} {st.get('state')}"
        if st.get("state") == "SETTLED":
            settle += (f" {st.get('winner') or '?'} {float(st.get('realized') or 0):+.2f}"
                       + (f" in {st['settlement_s']:.1f}s" if st.get("settlement_s") is not None else ""))
        elif st.get("age_s") is not None:
            settle += f" {st['age_s']:.0f}s"
        settle += " | "
    return (f"{TAG} {snap.get('clock')} {snap.get('mode')} | {t.get('feed')}={t.get('status')} "
            f"mid={t.get('mid') if t.get('mid') is None else round(t['mid'], 2)} | "
            f"round={m.get('slug') or '--'} T-{'?' if left is None else round(left)}s "
            f"book={(health.get('POLY BOOK') or ('--',))[0]} user={(health.get('USER WS') or ('--',))[0]} | "
            f"signals={t.get('signals')} orders={t.get('orders')} filled={t.get('filled')} "
            f"missed={t.get('refused')} errors={t.get('errors')} | "
            + (f"carry UP {m['carry']['UP']['carry']:g} DN {m['carry']['DOWN']['carry']:g} | " if m.get("carry") else "")
            + settle
            + f"pnl={'--' if pnl is None else f'{pnl:+.2f}'} open={a.get('open')} fees={a.get('fees')}"
            + (f" | canary {'HALTED: ' + '; '.join(canary['halted']) if canary.get('halted') else 'armed'}"
               f" orders {canary.get('orders')}/{canary.get('max_orders')}" if canary else "")
            + (f" | skips: {skips}" if skips else ""))


async def _dashboard(rt: Runtime) -> None:
    """The terminal dashboard. Snapshot on the loop (cheap reads), frame build
    and draw on a worker thread (pure Python, would hold BTC ticks unread).
    DASH_FPS (default 2) sets the frame rate: every frame is CPU the sniper
    competes for on a 1-vCPU VPS."""
    from dashboard import ConsoleCapture, build, glyphs, make_renderer, plain_line
    view = DashView(rt, accounting_every_s=_env_seconds("DASH_ACCOUNTING_S", 1.0), accounting_off_loop=True)
    frame_s = 1.0 / max(0.5, min(10.0, _env_seconds("DASH_FPS", 2.0) or 2.0))
    renderer = make_renderer(sys.stdout)
    if not getattr(renderer, "interactive", False):
        # Not a terminal (a log file, systemd): one plain line every 10s.
        while not rt.stop.is_set():
            snap = view.snapshot()
            renderer.status(plain_line(snap))
            await asyncio.sleep(1.0)
        return
    g = glyphs()
    failed = False
    with ConsoleCapture(on_event):
        with renderer:
            while not rt.stop.is_set():
                snap = view.snapshot()
                if _REPAINT.is_set():
                    _REPAINT.clear()
                    renderer.repaint()
                try:
                    renderer.cols, renderer.rows = renderer.size()

                    def frame(snap=snap):
                        snap["orders"] = view.orders()
                        return renderer.draw(build(snap, renderer.cols, renderer.rows, g))

                    await asyncio.to_thread(frame)
                except Exception as exc:
                    # A layout bug must not take the trading loop with it.
                    on_event("dashboard", f"render failed: {type(exc).__name__}: {exc}", "bad")
                    if not failed:
                        failed = True
                        _record_exit(f"dashboard render failed (bot continues): {type(exc).__name__}: {exc}",
                                     traceback.format_exc(), echo=False)
                await asyncio.sleep(frame_s)


# ---------------------------------------------------------------- health only
async def health_only() -> None:
    """Public feeds only: the round's book and Binance, health as JSON for 30s."""
    hub, cfg = build_hub(read_only=True)
    print(f"{TAG} MODE=HEALTH-ONLY (NO WALLET AUTH) | {cfg.describe()}")
    stop = threading.Event()
    tasks = hub.start(user=False, binance=True)
    tasks.append(asyncio.create_task(_rotation_loop(hub, stop), name="rotation"))
    try:
        for _ in range(6):
            await asyncio.sleep(5)
            print(json.dumps(hub.health(), indent=2, default=str))
    finally:
        stop.set()
        await hub.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------- entry
def _record_exit(reason: str, detail: str = "", *, echo: bool = True) -> None:
    """Append why this process stopped, and never fail while doing it."""
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{stamp}] pid={os.getpid()} {reason}\n"
    try:
        with (ROOT / "bot_exit.log").open("a", encoding="utf-8") as fh:
            fh.write(line + (detail.rstrip() + "\n" if detail else ""))
    except OSError as exc:
        print(f"{TAG} could not append bot_exit.log: {type(exc).__name__}: {exc}", file=sys.stderr)
    if echo:
        print(f"{TAG} stopped - {reason}", file=sys.stderr)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="BTC sniper: paper by default, a capped live canary with --live.")
    ap.add_argument("--dash", action="store_true", help="attach the terminal dashboard")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--health", action="store_true", help="public feeds only; no wallet auth and no trading")
    mode.add_argument("--paper", action="store_true", help="paper mode (default): live public data, simulated fills")
    mode.add_argument("--live", action="store_true", help="the sniper's LIVE CANARY: real orders under hard limits")
    ap.add_argument("--paper-balance", type=float,
                    help="starting paper cash (first launch only; default PAPER_START_BALANCE or 1000)")
    args = ap.parse_args(argv)
    if args.paper_balance is not None and args.live:
        ap.error("--paper-balance cannot be combined with --live")
    if args.health and args.dash:
        ap.error("--dash cannot be combined with --health")
    reason = "clean shutdown: run() returned"
    try:
        code = run_quietly(health_only() if args.health else
                           run(dash=args.dash, paper=not args.live, paper_balance=args.paper_balance))
        if code == 143:
            reason = "SIGTERM: stopped by the system or `kill`"
        elif code == 130:
            reason = "KeyboardInterrupt: Ctrl+C, or the console sent an interrupt"
    except KeyboardInterrupt:
        code, reason = 130, "KeyboardInterrupt: forced (second Ctrl+C)"
    except BaseException as exc:
        _record_exit(f"{type(exc).__name__}: {exc}", traceback.format_exc())
        if isinstance(exc, (RuntimeError, ValueError)):
            return 1                       # the reason is already on screen and in bot_exit.log
        raise
    _record_exit(reason)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
