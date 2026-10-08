#!/usr/bin/env python3
"""Feed tests.

These run REAL WebSocket servers on localhost and drive the real client code.
No mocked sockets: a disconnect test that never closes a socket proves
nothing about reconnect behaviour.

    python tests_feeds.py
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import itertools
import json
import pathlib
import random
import sys
import tempfile
import threading
import time
import types

ROOT = pathlib.Path(__file__).parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import websockets  # noqa: E402

from feeds import (BookState, FillStore, PolyMarketFeed, PolyUserFeed,  # noqa: E402
                   RestReconciler, backoff_delay)
from feeds.binance import BinanceTradeFeed  # noqa: E402
from feeds.health import DISCONNECTED, LIVE, STALE, UNSYNCED, redact  # noqa: E402
from feeds.hub import FeedHub  # noqa: E402
from feeds.supervisor import BACKOFF_LADDER, JITTER, SupervisedFeed  # noqa: E402

PASS = FAIL = 0
FAILURES: list[str] = []
UP = "1111111111111111111"
DOWN = "2222222222222222222"


def check(name, cond, detail="") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL {name} {detail}")


async def until(pred, timeout=5.0, tick=0.02):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        await asyncio.sleep(tick)
    return False


# ------------------------------------------------------------ mock servers --
class MockServer:
    """Base: an ephemeral-port ws server that records what the client sent."""

    def __init__(self):
        self.received: list[str] = []
        self.conns = 0
        self.server = None
        self.port = 0
        self.live: set = set()
        self.silent = False          # accept but never send
        self.drop_next = False       # close as soon as connected
        self.last_handler_error: str | None = None

    async def start(self):
        self.server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        if self.server:
            await self.kick_all()
            self.server.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.server.wait_closed(), timeout=3)

    async def kick_all(self):
        for ws in list(self.live):
            with contextlib.suppress(Exception):
                await ws.close(code=1001, reason="test kick")
        self.live.clear()

    async def broadcast(self, payload):
        text = payload if isinstance(payload, str) else json.dumps(payload)
        for ws in list(self.live):
            with contextlib.suppress(Exception):
                await ws.send(text)

    async def _handler(self, ws):
        raise NotImplementedError


class BinanceServer(MockServer):
    def __init__(self, price=64_900.0, interval=0.02):
        super().__init__()
        self.price, self.interval = price, interval

    async def _handler(self, ws):
        self.conns += 1
        self.live.add(ws)
        if self.drop_next:
            self.drop_next = False
            await ws.close(code=1006)
            return
        try:
            while True:
                if getattr(ws, "close_code", None) is not None:
                    break
                if not self.silent:
                    self.price += 0.5
                    await ws.send(json.dumps({"e": "trade", "s": "BTCUSDT",
                                              "p": f"{self.price:.2f}",
                                              "T": int(time.time() * 1000)}))
                await asyncio.sleep(self.interval)
        except Exception as exc:
            self.last_handler_error = type(exc).__name__
        finally:
            self.live.discard(ws)


class PolyServer(MockServer):
    """Serves both /market and /user shapes; replies PONG to PING."""

    def __init__(self):
        super().__init__()
        self.subs: list[dict] = []
        self.auths: list[dict] = []
        self.pings = 0

    async def _handler(self, ws):
        self.conns += 1
        self.live.add(ws)
        try:
            async for raw in ws:
                self.received.append(raw)
                if str(raw).strip().upper() == "PING":
                    self.pings += 1
                    await ws.send("PONG")
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                self.subs.append(msg)
                if "auth" in msg:
                    self.auths.append(msg["auth"])
        except Exception as exc:
            self.last_handler_error = type(exc).__name__
        finally:
            self.live.discard(ws)


# ================================================================ BINANCE ===
async def t_binance_connects_and_prices():
    srv = await BinanceServer().start()
    feed = BinanceTradeFeed(f"ws://127.0.0.1:{srv.port}", urls=(f"ws://127.0.0.1:{srv.port}",),
                            stale_after=0.5, recv_timeout=1.0, ping_interval=0.2,
                            ping_timeout=0.5)
    feed.start()
    try:
        ok = await until(lambda: feed.price is not None, 5)
        check("binance receives price", ok, str(feed.health.as_dict()))
        check("binance status LIVE", feed.refresh_status() == LIVE, feed.health.status)
        check("binance fresh_price available", feed.fresh_price() is not None)
        check("binance exchange skew measured", feed.skew_ms is not None)
        await until(lambda: feed.health.latency_ms is not None, 3)
        check("binance latency measured", feed.health.latency_ms is not None,
              str(feed.health.latency_ms))
    finally:
        await feed.stop(); await srv.stop()


async def t_binance_stale_is_marked_and_price_withheld():
    srv = await BinanceServer().start()
    feed = BinanceTradeFeed(f"ws://127.0.0.1:{srv.port}", urls=(f"ws://127.0.0.1:{srv.port}",),
                            stale_after=0.4, recv_timeout=30.0, ping_interval=5.0)
    feed.start()
    try:
        await until(lambda: feed.price is not None, 5)
        last = feed.price
        srv.silent = True                      # connected, but no more data
        ok = await until(lambda: feed.refresh_status() == STALE, 4)
        check("stale feed marked STALE", ok, feed.health.status)
        check("stale feed withholds fresh_price", feed.fresh_price() is None)
        check("stale feed still exposes last value", feed.price == last)
        check("stale feed reports age", (feed.price_age_ms or 0) > 400,
              str(feed.price_age_ms))
    finally:
        await feed.stop(); await srv.stop()


async def t_binance_reconnects_after_disconnect():
    srv = await BinanceServer().start()
    url = f"ws://127.0.0.1:{srv.port}"
    feed = BinanceTradeFeed(url, urls=(url,), stale_after=0.5, recv_timeout=5.0,
                            ping_interval=5.0)
    feed.start()
    try:
        await until(lambda: feed.price is not None, 5)
        first_conns = srv.conns
        t0 = time.monotonic()
        await srv.kick_all()
        ok = await until(lambda: srv.conns > first_conns and feed.price is not None, 6)
        elapsed = time.monotonic() - t0
        check("reconnects after server drop", ok, f"conns={srv.conns}")
        check("reconnect counted", feed.health.reconnect_count >= 1,
              str(feed.health.reconnect_count))
        # first rung is 0.25s +/- 25%
        check("first reconnect is fast", elapsed < 2.0, f"{elapsed:.2f}s")
    finally:
        await feed.stop(); await srv.stop()


async def t_binance_recv_timeout_forces_reconnect():
    srv = await BinanceServer().start()
    url = f"ws://127.0.0.1:{srv.port}"
    feed = BinanceTradeFeed(url, urls=(url,), stale_after=0.3, recv_timeout=0.6,
                            ping_interval=10.0)
    feed.start()
    try:
        await until(lambda: feed.price is not None, 5)
        conns = srv.conns
        srv.silent = True                      # answers pings, sends nothing
        ok = await until(lambda: srv.conns > conns, 6)
        check("silent socket triggers reconnect", ok,
              f"conns={srv.conns} status={feed.health.status}")
    finally:
        await feed.stop(); await srv.stop()


async def t_binance_falls_over_to_the_mirror_endpoint():
    """A primary that works once and then goes dry must still fail over.

    The first version keyed this off `health.messages`, which is cumulative
    across sessions - so one good session made the mirror permanently
    unreachable.
    """
    dead = await BinanceServer().start()
    alive = await BinanceServer(price=70_000.0).start()
    primary, mirror = f"ws://127.0.0.1:{dead.port}", f"ws://127.0.0.1:{alive.port}"
    feed = BinanceTradeFeed(primary, urls=(primary, mirror), stale_after=0.5,
                            recv_timeout=0.5, ping_interval=10.0)
    feed.start()
    try:
        ok = await until(lambda: feed.price is not None, 5)
        check("primary works initially", ok and feed.price < 70_000, str(feed.price))
        dead.silent = True                      # primary goes dry, stays up
        ok = await until(lambda: (feed.price or 0) >= 70_000, 12)
        check("fails over to the mirror after dry sessions", ok,
              f"price={feed.price} url_i={feed._url_i}")
        check("mirror actually served us", alive.conns >= 1, str(alive.conns))
    finally:
        await feed.stop(); await dead.stop(); await alive.stop()


def t_backoff_ladder():
    rng = random.Random(0)
    for attempt, base in enumerate(BACKOFF_LADDER):
        vals = [backoff_delay(attempt, rng=rng) for _ in range(400)]
        check(f"backoff rung {attempt} centred on {base}",
              base * (1 - JITTER) - 1e-9 <= min(vals) and max(vals) <= base * (1 + JITTER) + 1e-9,
              f"{min(vals):.3f}..{max(vals):.3f}")
        check(f"backoff rung {attempt} has jitter", len(set(round(v, 4) for v in vals)) > 50)
    check("backoff caps at 8s", all(backoff_delay(a) <= 8 * (1 + JITTER) for a in range(3, 40)))


def t_process_lock_rejects_duplicates_without_truncating():
    from run import _ProcessLock
    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "bot.lock"
        path.write_text("sentinel\n", encoding="utf-8")
        first, second = _ProcessLock(path), _ProcessLock(path)
        first.acquire()
        try:
            check("lock metadata appends without truncating an existing file",
                  path.read_text(encoding="utf-8").startswith("sentinel\n"))
            try:
                second.acquire()
            except RuntimeError:
                check("second process lock is rejected", True)
            else:
                check("second process lock is rejected", False)
                second.release()
        finally:
            first.release()
    check("ladder is the requested one", BACKOFF_LADDER == (0.25, 0.5, 1.0, 2.0, 4.0, 8.0),
          str(BACKOFF_LADDER))


def _transition_test_hub(events=None):
    return FeedHub(
        urls={"binance": "ws://127.0.0.1:1",
              "poly_market": "ws://127.0.0.1:1",
              "poly_user": "ws://127.0.0.1:1"},
        on_event=(None if events is None else
                  lambda source, text, level: events.append((source, text, level))))


def t_round_transition_serializes_and_same_state_repairs():
    """An older boundary clear must never overwrite a newer promotion."""
    events = []
    hub = _transition_test_hub(events)
    hub.set_round("old-up", "old-down", "old-cond", 1000, 1300)

    original_set_tokens = hub.market.set_tokens
    clear_entered = threading.Event()
    release_clear = threading.Event()
    promote_started = threading.Event()
    promote_done = threading.Event()
    errors = []

    def delayed_set_tokens(tokens):
        if threading.current_thread().name == "boundary-clear":
            clear_entered.set()
            if not release_clear.wait(2):
                raise TimeoutError("test did not release boundary transition")
        return original_set_tokens(tokens)

    def clear_round():
        try:
            hub.set_round(None, None, None, 1300, 1600)
        except BaseException as exc:
            errors.append(exc)

    def promote_round():
        promote_started.set()
        try:
            hub.set_round("new-up", "new-down", "new-cond", 1300, 1600)
        except BaseException as exc:
            errors.append(exc)
        finally:
            promote_done.set()

    hub.market.set_tokens = delayed_set_tokens
    clear_thread = threading.Thread(target=clear_round, name="boundary-clear")
    promote_thread = threading.Thread(target=promote_round, name="round-promotion")
    try:
        clear_thread.start()
        entered = clear_entered.wait(2)
        check("boundary transition reached downstream apply", entered)
        promote_thread.start()
        check("promotion thread started", promote_started.wait(2))
        check("new promotion waits for the older transition to commit",
              not promote_done.wait(0.2))
    finally:
        release_clear.set()
        clear_thread.join(2)
        promote_thread.join(2)
        hub.market.set_tokens = original_set_tokens

    check("round transition threads finished",
          not clear_thread.is_alive() and not promote_thread.is_alive())
    check("round transition threads did not raise", not errors,
          ", ".join(type(exc).__name__ for exc in errors))
    check("hub retained the promoted round",
          (hub.up_token, hub.down_token, hub.condition_id)
          == ("new-up", "new-down", "new-cond"))
    check("book subscriptions match the promoted round",
          set(hub.book.active) == {"new-up", "new-down"}, str(hub.book.active))
    check("user subscription includes the promoted condition",
          tuple(hub.user._want) == ("old-cond", "new-cond"), str(hub.user._want))

    # Recreate the persisted symptom directly: hub state is correct while both
    # downstream intents have been overwritten. An equal call must heal it
    # without claiming a new generation.
    # Corrupt BookState only. Market subscription intent deliberately remains
    # correct, which used to trigger PolyMarketFeed.set_tokens' early return
    # and make the hub's same-state repair ineffective.
    hub.book.set_active([])
    hub.user.set_markets(["old-cond"])
    check("public subscription intent stayed correct during BookState drift",
          set(hub.market._want) == {"new-up", "new-down"}, str(hub.market._want))
    generation = hub.generation
    repaired = hub.set_round("new-up", "new-down", "new-cond", 1300, 1600)
    check("same-state set_round reports no logical transition", repaired is False)
    check("same-state set_round preserves the generation", hub.generation == generation)
    check("same-state set_round repairs public subscriptions",
          set(hub.book.active) == {"new-up", "new-down"}, str(hub.book.active))
    check("same-state set_round repairs private subscriptions",
          tuple(hub.user._want) == ("old-cond", "new-cond"), str(hub.user._want))

    stable_events = len(events)
    stable_book_generations = {
        token: hub.book.view(token).generation for token in ("new-up", "new-down")}
    repeated = hub.set_round("new-up", "new-down", "new-cond", 1300, 1600)
    check("healthy same-state set_round remains false", repeated is False)
    check("healthy same-state set_round emits no subscription churn",
          len(events) == stable_events, str(events[stable_events:]))
    check("healthy same-state set_round does not reset books",
          stable_book_generations == {
              token: hub.book.view(token).generation
              for token in ("new-up", "new-down")})


def t_prepare_transition_serializes_and_same_state_repairs():
    """Two racing prewarms must leave consumers on the newest intent."""
    events = []
    hub = _transition_test_hub(events)
    hub.set_round("cur-up", "cur-down", "cur-cond", 1000, 1300)
    prepared_a = {
        "up_token_id": "a-up", "down_token_id": "a-down",
        "condition_id": "a-cond", "window_start": 1300, "window_end": 1600,
    }
    prepared_b = {
        "up_token_id": "b-up", "down_token_id": "b-down",
        "condition_id": "b-cond", "window_start": 1300, "window_end": 1600,
    }

    original_set_tokens = hub.market.set_tokens
    first_entered = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    second_done = threading.Event()
    errors = []

    def delayed_set_tokens(tokens):
        if threading.current_thread().name == "prepare-a":
            first_entered.set()
            if not release_first.wait(2):
                raise TimeoutError("test did not release first prewarm")
        return original_set_tokens(tokens)

    def prepare_a():
        try:
            hub.prepare_round(prepared_a)
        except BaseException as exc:
            errors.append(exc)

    def prepare_b():
        second_started.set()
        try:
            hub.prepare_round(prepared_b)
        except BaseException as exc:
            errors.append(exc)
        finally:
            second_done.set()

    hub.market.set_tokens = delayed_set_tokens
    first_thread = threading.Thread(target=prepare_a, name="prepare-a")
    second_thread = threading.Thread(target=prepare_b, name="prepare-b")
    try:
        first_thread.start()
        check("first prewarm reached downstream apply", first_entered.wait(2))
        second_thread.start()
        check("second prewarm thread started", second_started.wait(2))
        check("newer prewarm waits for the older transition to commit",
              not second_done.wait(0.2))
    finally:
        release_first.set()
        first_thread.join(2)
        second_thread.join(2)
        hub.market.set_tokens = original_set_tokens

    check("prewarm transition threads finished",
          not first_thread.is_alive() and not second_thread.is_alive())
    check("prewarm transition threads did not raise", not errors,
          ", ".join(type(exc).__name__ for exc in errors))
    check("newest prepared state wins", hub.prepared_round == prepared_b,
          str(hub.prepared_round))
    expected = {"cur-up", "cur-down", "b-up", "b-down"}
    check("public subscriptions match newest prepared state",
          set(hub.book.active) == expected, str(hub.book.active))

    # As above, leave `_want` correct and corrupt only the active BookState.
    hub.book.set_active(["cur-up", "cur-down"])
    check("prewarm intent stayed correct during BookState drift",
          set(hub.market._want) == expected, str(hub.market._want))
    repaired = hub.prepare_round(prepared_b)
    check("same-state prepare_round reports no logical transition", repaired is False)
    check("same-state prepare_round restores prewarmed tokens",
          set(hub.book.active) == expected, str(hub.book.active))

    stable_events = len(events)
    stable_book_generations = {
        token: hub.book.view(token).generation for token in expected}
    repeated = hub.prepare_round(prepared_b)
    check("healthy same-state prepare_round remains false", repeated is False)
    check("healthy same-state prepare_round emits no subscription churn",
          len(events) == stable_events, str(events[stable_events:]))
    check("healthy same-state prepare_round does not reset books",
          stable_book_generations == {
              token: hub.book.view(token).generation for token in expected})


async def t_supervisor_isolates_failures():
    class Boom(SupervisedFeed):
        name = "boom"
        def __init__(self):
            super().__init__()
            self.tries = 0
        async def _session(self):
            self.tries += 1
            raise RuntimeError("venue on fire")

    boom = Boom()
    healthy = {"n": 0}

    async def other():
        while True:
            healthy["n"] += 1
            await asyncio.sleep(0.02)

    o = asyncio.create_task(other())
    boom.start()
    await asyncio.sleep(1.2)
    check("failing feed keeps retrying", boom.tries >= 2, str(boom.tries))
    check("failing feed records the error", "venue on fire" in (boom.health.last_error or ""))
    check("failing feed does not kill other tasks", healthy["n"] > 10 and not o.done())
    await boom.stop()
    o.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await o


# ============================================================ POLY MARKET ===
def _book_msg(token, bids, asks, ts=None):
    return {"event_type": "book", "asset_id": token, "market": "0xcond",
            "bids": [{"price": str(p), "size": str(s)} for p, s in bids],
            "asks": [{"price": str(p), "size": str(s)} for p, s in asks],
            "timestamp": str(ts or int(time.time() * 1000)), "hash": "0xabc"}


def _delta(token, price, size, side):
    return {"event_type": "price_change", "market": "0xcond",
            "price_changes": [{"asset_id": token, "price": str(price),
                               "size": str(size), "side": side,
                               "best_bid": "0.47", "best_ask": "0.52"}],
            "timestamp": str(int(time.time() * 1000))}


async def _market_feed(srv, tokens=(UP, DOWN), **kw):
    feed = PolyMarketFeed(f"ws://127.0.0.1:{srv.port}", stale_after=kw.pop("stale_after", 5.0),
                          recv_timeout=kw.pop("recv_timeout", 30.0),
                          ping_every=kw.pop("ping_every", 0.15), **kw)
    feed.set_tokens(list(tokens))
    feed.start()
    await until(lambda: srv.subs, 5)
    return feed


async def t_market_subscribe_shape_and_heartbeat():
    srv = await PolyServer().start()
    feed = await _market_feed(srv)
    try:
        sub = srv.subs[0]
        check("subscribes with assets_ids", sorted(sub.get("assets_ids", [])) == sorted([UP, DOWN]),
              str(sub))
        check("subscribe type is market", sub.get("type") == "market", str(sub))
        check("custom features requested", sub.get("custom_feature_enabled") is True)
        ok = await until(lambda: srv.pings >= 2, 4)
        check("sends app-level PING", ok, f"pings={srv.pings}")
        ok = await until(lambda: feed.health.latency_ms is not None, 3)
        check("PONG measures latency", ok, str(feed.health.latency_ms))
    finally:
        await feed.stop(); await srv.stop()


async def t_book_snapshot_delta_and_removal():
    srv = await PolyServer().start()
    feed = await _market_feed(srv)
    try:
        await srv.broadcast(_book_msg(UP, [(0.46, 900), (0.47, 310)], [(0.52, 180), (0.53, 640)]))
        ok = await until(lambda: feed.book.view(UP).best_bid == 0.47, 4)
        v = feed.book.view(UP)
        check("snapshot applied", ok, str(v.bids))
        check("best bid is highest", v.best_bid == 0.47, str(v.best_bid))
        check("best ask is lowest", v.best_ask == 0.52, str(v.best_ask))
        check("spread computed", abs((v.spread or 0) - 0.05) < 1e-9, str(v.spread))
        check("depth summed", v.depth_bid == 1210 and v.depth_ask == 820,
              f"{v.depth_bid}/{v.depth_ask}")
        check("book status LIVE", v.status == LIVE, v.status)
        check("book_age_ms present", (v.book_age_ms() or -1) >= 0)

        await srv.broadcast(_delta(UP, 0.48, 55, "BUY"))
        await until(lambda: feed.book.view(UP).best_bid == 0.48, 3)
        check("delta improves bid", feed.book.view(UP).best_bid == 0.48)

        await srv.broadcast(_delta(UP, 0.48, 0, "BUY"))       # size 0 removes
        await until(lambda: feed.book.view(UP).best_bid == 0.47, 3)
        check("size 0 removes the level", feed.book.view(UP).best_bid == 0.47,
              str(feed.book.view(UP).bids))

        await srv.broadcast({"event_type": "tick_size_change", "asset_id": UP,
                             "old_tick_size": "0.01", "new_tick_size": "0.001"})
        await until(lambda: feed.book.view(UP).tick_size == 0.001, 3)
        check("tick size tracked", feed.book.view(UP).tick_size == 0.001,
              str(feed.book.view(UP).tick_size))
    finally:
        await feed.stop(); await srv.stop()


async def t_delta_before_snapshot_is_ignored():
    srv = await PolyServer().start()
    feed = await _market_feed(srv)
    try:
        await srv.broadcast(_delta(UP, 0.99, 100, "BUY"))
        await asyncio.sleep(0.3)
        v = feed.book.view(UP)
        check("unsynced book rejects deltas", v.bids == (), str(v.bids))
        check("unsynced book says UNSYNCED", v.status == UNSYNCED, v.status)
    finally:
        await feed.stop(); await srv.stop()


async def t_market_rotation_does_not_mix_rounds():
    srv = await PolyServer().start()
    feed = await _market_feed(srv)
    NEW_UP, NEW_DOWN = "3333333333", "4444444444"
    try:
        await srv.broadcast(_book_msg(UP, [(0.46, 900)], [(0.52, 180)]))
        await until(lambda: feed.book.view(UP).best_bid == 0.46, 4)

        feed.set_tokens([NEW_UP, NEW_DOWN])                 # round rotates
        ok = await until(lambda: any(s.get("operation") == "unsubscribe" for s in srv.subs), 4)
        check("unsubscribes old tokens", ok, str(srv.subs[-3:]))
        ok = await until(lambda: any(s.get("operation") == "subscribe" for s in srv.subs), 4)
        check("subscribes new tokens", ok, str(srv.subs[-3:]))
        unsub = [s for s in srv.subs if s.get("operation") == "unsubscribe"][0]
        check("unsubscribe names the old tokens",
              sorted(unsub["assets_ids"]) == sorted([UP, DOWN]), str(unsub))

        check("old round book erased", feed.book.view(UP).bids == (),
              str(feed.book.view(UP).bids))
        check("new token starts UNSYNCED", feed.book.view(NEW_UP).status == UNSYNCED,
              feed.book.view(NEW_UP).status)

        before = feed.book.dropped_inactive
        await srv.broadcast(_book_msg(UP, [(0.99, 1)], [(0.999, 1)]))   # late old-round msg
        await until(lambda: feed.book.dropped_inactive > before, 3)
        check("late old-round message dropped", feed.book.dropped_inactive > before,
              str(feed.book.dropped_inactive))
        check("old data never reaches the new book", feed.book.view(NEW_UP).bids == ())

        await srv.broadcast(_book_msg(NEW_UP, [(0.30, 10)], [(0.70, 10)]))
        await until(lambda: feed.book.view(NEW_UP).best_bid == 0.30, 3)
        check("new round book fills", feed.book.view(NEW_UP).best_bid == 0.30)
        check("generation advanced", feed.book.view(NEW_UP).generation >= 1)
    finally:
        await feed.stop(); await srv.stop()


async def t_rotation_actually_frees_and_cannot_resurrect():
    """view() returns empty for ANY inactive token, so asserting on view()
    does not prove the old round was erased. Check the maps themselves."""
    book = BookState()
    book.connected = True
    book.set_active([UP, DOWN])
    book.apply_snapshot(UP, [{"price": "0.46", "size": "900"}],
                        [{"price": "0.52", "size": "180"}])
    check("book populated before rotation", book.view(UP).best_bid == 0.46)

    book.set_active(["A1", "A2"])
    check("old token dropped from the bid map", UP not in book._bids, str(list(book._bids)))
    check("old token dropped from the ask map", UP not in book._asks)
    check("old token dropped from meta", UP not in book._meta)
    check("old token dropped from the view cache", UP not in book._cache)

    for i in range(50):                       # a few hours of 5-minute rounds
        book.set_active([f"t{i}a", f"t{i}b"])
        book.apply_snapshot(f"t{i}a", [{"price": "0.5", "size": "1"}],
                            [{"price": "0.6", "size": "1"}])
    check("state does not grow with rounds", len(book._bids) == 2, str(len(book._bids)))
    check("meta does not grow with rounds", len(book._meta) == 2, str(len(book._meta)))

    # a token that comes back must not resurrect its old book
    book.set_active([UP, DOWN])
    check("returning token has no old levels", book.view(UP).bids == (), str(book.view(UP).bids))
    check("returning token is UNSYNCED", book.view(UP).status == UNSYNCED,
          book.view(UP).status)
    check("returning token gets a new generation", book.view(UP).generation >= 2,
          str(book.view(UP).generation))


async def t_reconnect_marks_unsynced_then_recovers():
    srv = await PolyServer().start()
    feed = await _market_feed(srv, recv_timeout=30.0)
    try:
        await srv.broadcast(_book_msg(UP, [(0.46, 900)], [(0.52, 180)]))
        await until(lambda: feed.book.view(UP).status == LIVE, 4)

        conns = srv.conns
        await srv.kick_all()
        ok = await until(lambda: feed.book.view(UP).status in (UNSYNCED, DISCONNECTED), 4)
        check("disconnect desyncs the book", ok, feed.book.view(UP).status)
        check("stale levels are not served as LIVE",
              feed.book.view(UP).status != LIVE, feed.book.view(UP).status)

        ok = await until(lambda: srv.conns > conns, 6)
        check("market feed reconnects", ok, f"conns={srv.conns}")
        await asyncio.sleep(0.3)
        check("still UNSYNCED until a fresh snapshot",
              feed.book.view(UP).status == UNSYNCED, feed.book.view(UP).status)
        check("resync is requested", UP in feed.book.needs_resync())

        await srv.broadcast(_book_msg(UP, [(0.44, 5)], [(0.55, 5)]))
        ok = await until(lambda: feed.book.view(UP).status == LIVE, 4)
        check("fresh snapshot restores LIVE", ok, feed.book.view(UP).status)
        check("recovered book is the new one", feed.book.view(UP).best_bid == 0.44)
    finally:
        await feed.stop(); await srv.stop()


async def t_rest_resync_recovers_book():
    srv = await PolyServer().start()
    calls = {"n": 0}

    def rest_fetch(token):
        calls["n"] += 1
        return ([{"price": "0.41", "size": "7"}], [{"price": "0.59", "size": "9"}])

    hub = FeedHub(rest_book_fetch=rest_fetch,
                  urls={"binance": "ws://127.0.0.1:1", "poly_market": f"ws://127.0.0.1:{srv.port}",
                        "poly_user": "ws://127.0.0.1:1"})
    hub.market.ping_every = 0.2
    hub.set_round(UP, DOWN, "0xcond")
    hub.start()
    try:
        ok = await until(lambda: hub.book.view(UP).status == LIVE, 8)
        check("REST resync repairs an unsynced book", ok, hub.book.view(UP).status)
        check("REST resync used the REST snapshot", hub.book.view(UP).best_bid == 0.41,
              str(hub.book.view(UP).bids))
        check("REST resync counted", hub.rest_resyncs >= 1, str(hub.rest_resyncs))
        snap = hub.snapshot()
        check("snapshot carries the book", snap.up is not None and snap.up.best_bid == 0.41)
        check("snapshot has both tokens", snap.up_token == UP and snap.down_token == DOWN)
    finally:
        await hub.stop(); await srv.stop()


async def t_no_rest_or_disk_inside_receive_callback():
    """A REST call in a message handler stalls every later message."""
    srv = await PolyServer().start()
    feed = await _market_feed(srv)
    banned = {"hits": 0}
    import builtins
    real_open = builtins.open

    def spy_open(*a, **k):
        banned["hits"] += 1
        return real_open(*a, **k)
    try:
        import requests
        real_get = requests.get
        requests.get = lambda *a, **k: banned.__setitem__("hits", banned["hits"] + 1)
        builtins.open = spy_open
        await srv.broadcast(_book_msg(UP, [(0.46, 900)], [(0.52, 180)]))
        for i in range(300):
            feed._handle(json.dumps(_delta(UP, 0.40 + (i % 9) / 100, 10 + i, "BUY")))
        check("no REST or disk io inside the receive path", banned["hits"] == 0,
              str(banned["hits"]))
        t0 = time.perf_counter()
        for i in range(2000):
            feed._handle(json.dumps(_delta(UP, 0.40 + (i % 9) / 100, 10 + i, "BUY")))
        per = (time.perf_counter() - t0) / 2000 * 1e6
        check("receive path stays cheap", per < 200, f"{per:.0f}us/msg")
    finally:
        builtins.open = real_open
        requests.get = real_get
        await feed.stop(); await srv.stop()


async def t_snapshot_is_atomic_under_concurrent_writes():
    """A reader must never see a half-applied book."""
    book = BookState()
    book.connected = True
    book.set_active([UP])
    book.apply_snapshot(UP, [{"price": "0.40", "size": "10"}],
                        [{"price": "0.60", "size": "10"}])
    stop = threading.Event()
    bad = {"n": 0, "checked": 0}

    def writer():
        i = 0
        while not stop.is_set():
            i += 1
            book.apply_snapshot(
                UP,
                [{"price": f"{0.30 + (i % 5) / 100:.2f}", "size": str(i % 50 + 1)},
                 {"price": "0.29", "size": "5"}],
                [{"price": f"{0.70 - (i % 5) / 100:.2f}", "size": str(i % 40 + 1)},
                 {"price": "0.71", "size": "5"}])

    ts = [threading.Thread(target=writer, daemon=True) for _ in range(3)]
    for t in ts:
        t.start()
    for _ in range(4000):
        v = book.view(UP)
        bad["checked"] += 1
        bids, asks = v.bids, v.asks
        if list(bids) != sorted(bids, key=lambda x: x[0], reverse=True):
            bad["n"] += 1
        if list(asks) != sorted(asks, key=lambda x: x[0]):
            bad["n"] += 1
        if bids and asks and bids[0][0] >= asks[0][0]:
            bad["n"] += 1
        if len(bids) != 2 or len(asks) != 2:      # a half-applied replace
            bad["n"] += 1
    stop.set()
    for t in ts:
        t.join(timeout=2)
    check("snapshot never observed torn", bad["n"] == 0,
          f"{bad['n']} bad of {bad['checked']}")
    check("torture actually ran", bad["checked"] >= 4000)


# ============================================================== POLY USER ===
SECRET = "sUp3r-s3cr3t-value-do-not-log"


async def t_user_auth_and_no_secret_leak():
    srv = await PolyServer().start()
    lines: list[str] = []
    feed = PolyUserFeed({"apiKey": "ak-123", "secret": SECRET, "passphrase": "pp-xyz"},
                        f"ws://127.0.0.1:{srv.port}", markets=["0xcond"],
                        ping_every=0.15, recv_timeout=30.0,
                        on_event=lambda n, t, l: lines.append(f"{n} {t}"))
    feed.start()
    try:
        ok = await until(lambda: srv.auths, 5)
        check("user channel authenticates", ok, str(srv.subs[:1]))
        auth = srv.auths[0]
        check("auth carries apiKey/secret/passphrase",
              set(auth) == {"apiKey", "secret", "passphrase"}, str(sorted(auth)))
        check("subscribes by condition id", srv.subs[0].get("markets") == ["0xcond"],
              str(srv.subs[0]))
        check("subscribe type is user", srv.subs[0].get("type") == "user")

        feed.health.mark_error(f'{{"secret": "{SECRET}", "passphrase": "pp-xyz"}}')
        blob = " ".join(lines) + repr(feed) + str(feed.health.as_dict())
        check("secret never appears in events/health/repr", SECRET not in blob,
              blob[:120])
        check("redaction leaves a marker", "<redacted>" in (feed.health.last_error or ""),
              str(feed.health.last_error))
        check("redact() scrubs private keys",
              "0xdeadbeef" not in redact('private_key=0xdeadbeef more'),
              redact('private_key=0xdeadbeef more'))
        ok = await until(lambda: srv.pings >= 2, 4)
        check("user channel heartbeats", ok, f"pings={srv.pings}")
    finally:
        await feed.stop(); await srv.stop()


async def t_user_execution_readiness_requires_sent_filter_and_real_pong():
    srv = await PolyServer().start()
    feed = PolyUserFeed({"apiKey": "a", "secret": "b", "passphrase": "c"},
                        f"ws://127.0.0.1:{srv.port}", markets=["0xold"],
                        ping_every=1.0, recv_timeout=30.0)
    feed.start()
    try:
        ok = await until(lambda: bool(srv.auths), 5)
        check("private subscription frame is sent", ok, str(srv.subs))
        check("sent subscription alone is not execution-ready before PONG",
              not feed.ready_for_market("0xold"), str(feed.health.as_dict()))
        ok = await until(lambda: feed.ready_for_market("0xold"), 3)
        check("matching sent filter plus application PONG becomes ready",
              ok, str(feed.health.as_dict()))
        check("heartbeat cannot authorize a market that was not sent",
              not feed.ready_for_market("0xother"), str(feed._sent))

        feed.set_markets(["0xnew"])
        ok = await until(lambda: feed.ready_for_market("0xnew"), 3)
        check("successful subscription update makes the new condition ready",
              ok, f"sent={feed._sent} generation={feed.subscription_generation}")
        old_gone = await until(lambda: not feed.ready_for_market("0xold"), 2)
        check("unsubscribed old condition is no longer execution-ready",
              old_gone, str(feed._sent))

        await srv.kick_all()
        ok = await until(lambda: not feed.ready_for_market("0xnew"), 2)
        check("disconnected private session immediately fails readiness", ok)
    finally:
        await feed.stop(); await srv.stop()


async def t_duplicate_fills_are_counted_once():
    srv = await PolyServer().start()
    feed = PolyUserFeed({"apiKey": "a", "secret": "b", "passphrase": "c"},
                        f"ws://127.0.0.1:{srv.port}", markets=["0xcond"],
                        ping_every=0.15, recv_timeout=30.0)
    feed.start()
    try:
        await until(lambda: srv.subs, 5)
        base = {"event_type": "trade", "id": "trade-uuid-1", "market": "0xcond",
                "asset_id": UP, "side": "BUY", "size": "10", "price": "0.57",
                "fee_rate_bps": "700", "type": "TRADE"}
        for status in ("MATCHED", "MINED", "CONFIRMED"):
            await srv.broadcast({**base, "status": status})
        await until(lambda: feed.store.summary()["fills"] == 1, 4)
        s = feed.store.summary()
        check("lifecycle counted as one fill", s["fills"] == 1, str(s))
        check("duplicates suppressed", s["duplicates_suppressed"] == 2, str(s))
        check("size not accumulated", s["shares"] == 10, str(s))
        check("notional correct", abs(s["notional"] - 5.7) < 1e-9, str(s))

        # reconnect replay of an earlier status must not regress or re-count
        await srv.broadcast({**base, "status": "MATCHED"})
        await asyncio.sleep(0.3)
        s2 = feed.store.summary()
        check("replay does not create a fill", s2["fills"] == 1, str(s2))
        rec = feed.store.recent()[0]
        check("replay does not regress status", rec.status == "CONFIRMED", rec.status)
        check("live fee metadata retained", rec.fee_rate_bps == 700.0,
              str(rec.fee_rate_bps))

        # A genuinely different trade is visible but remains pending until
        # the venue reaches its terminal successful state.
        await srv.broadcast({**base, "id": "trade-uuid-2", "status": "MATCHED",
                             "size": "4"})
        await until(lambda: feed.store.summary()["pending"] == 1, 4)
        check("MATCHED trade is pending, not a fill",
              feed.store.summary()["fills"] == 1, str(feed.store.summary()))
        await srv.broadcast({**base, "id": "trade-uuid-2", "status": "CONFIRMED",
                             "size": "4"})
        await until(lambda: feed.store.summary()["fills"] == 2, 4)
        check("distinct trade ids both count", feed.store.summary()["fills"] == 2)
        check("shares summed across distinct trades",
              feed.store.summary()["shares"] == 14, str(feed.store.summary()))

        # FAILED is not a fill
        await srv.broadcast({**base, "id": "trade-uuid-3", "status": "FAILED", "size": "99"})
        await asyncio.sleep(0.3)
        check("FAILED trade is not counted as a fill",
              feed.store.summary()["fills"] == 2, str(feed.store.summary()))

        # Contradictory terminal venue copies must quarantine the trade. The
        # safe answer is unknown inventory, never a confident phantom fill.
        await srv.broadcast({**base, "id": "trade-uuid-4", "status": "CONFIRMED",
                             "size": "3"})
        await until(lambda: feed.store.summary()["fills"] == 3, 4)
        await srv.broadcast({**base, "id": "trade-uuid-4", "status": "FAILED",
                             "size": "3"})
        await until(lambda: feed.store.fills["trade-uuid-4"].status == "CONFLICT", 4)
        check("conflicting terminal status quarantines the fill",
              feed.store.summary()["fills"] == 2,
              str(feed.store.summary()))
    finally:
        await feed.stop(); await srv.stop()


async def t_fill_replay_enriches_missing_identity_fields():
    store = FillStore()
    store.record_trade("enrich", price="0.5", size="2", status="MATCHED")
    store.record_trade("enrich", asset_id=UP, market="0xcond", side="BUY",
                       price="0.5", size="2", status="CONFIRMED",
                       fee_rate_bps="700", source="reconcile")
    rec = store.fills["enrich"]
    check("duplicate lifecycle adds missing asset", rec.asset_id == UP, str(rec))
    check("duplicate lifecycle adds missing market", rec.market == "0xcond", str(rec))
    check("duplicate lifecycle adds missing side", rec.side == "BUY", str(rec))
    store.record_trade("prefix", status="TRADE_STATUS_CONFIRMED")
    check("prefixed status normalises",
          store.fills["prefix"].status == "CONFIRMED",
          store.fills["prefix"].status)
    check("fee metadata enriched", rec.fee_rate_bps == 700.0, str(rec.fee_rate_bps))


async def t_user_market_filters_are_removed_between_rounds():
    srv = await PolyServer().start()
    feed = PolyUserFeed({"apiKey": "a", "secret": "b", "passphrase": "c"},
                        f"ws://127.0.0.1:{srv.port}", markets=["old"],
                        ping_every=0.15, recv_timeout=30.0)
    feed.start()
    try:
        await until(lambda: srv.subs, 5)
        initial_generation = feed.subscription_generation
        check("initial user subscription advances wire generation",
              initial_generation >= 1, str(initial_generation))
        feed.set_markets(["new"])
        ok = await until(lambda: any(
            m.get("operation") == "unsubscribe" and m.get("markets") == ["old"]
            for m in srv.subs), 5)
        check("old round is unsubscribed", ok, str(srv.subs))
        ok = await until(lambda: any(
            m.get("operation") == "subscribe" and m.get("markets") == ["new"]
            for m in srv.subs), 5)
        check("new round is subscribed", ok, str(srv.subs))
        check("subscription state contains only current round", feed._sent == {"new"},
              str(feed._sent))
        check("successful subscription add advances wire generation",
              feed.subscription_generation > initial_generation,
              str(feed.subscription_generation))
    finally:
        await feed.stop(); await srv.stop()


async def t_user_subscription_generation_advances_on_reconnect():
    srv = await PolyServer().start()
    feed = PolyUserFeed({"apiKey": "a", "secret": "b", "passphrase": "c"},
                        f"ws://127.0.0.1:{srv.port}", markets=["0xcond"],
                        ping_every=0.15, recv_timeout=30.0)
    feed.start()
    try:
        ok = await until(lambda: feed.subscription_generation >= 1, 5)
        first = feed.subscription_generation
        check("initial session exposes a completed subscription", ok, str(first))
        first_connections = srv.conns
        await srv.kick_all()
        ok = await until(
            lambda: (srv.conns > first_connections
                     and feed.subscription_generation > first), 6)
        check("reconnect advances subscription generation with unchanged markets",
              ok, f"connections={srv.conns} generation={feed.subscription_generation}")
    finally:
        await feed.stop(); await srv.stop()


async def t_order_updates_are_cumulative_not_additive():
    store = FillStore()
    for matched in ("0", "3", "7", "10"):
        store.record_order("0xorder1", asset_id=UP, side="BUY", price="0.57",
                           original_size="10", size_matched=matched, state="UPDATE")
    rec = store.orders["0xorder1"]
    check("size_matched takes the cumulative value", rec.size_matched == 10.0,
          str(rec.size_matched))
    store.record_order("0xorder1", size_matched="7", state="UPDATE")   # out-of-order replay
    check("out-of-order update cannot reduce matched", store.orders["0xorder1"].size_matched == 10.0)
    store.record_order("0xorder1", state="CANCELLATION")
    check("cancellation tracked", store.summary()["cancelled"] == 1, str(store.summary()))


async def t_reconcile_backup_never_double_counts():
    store = FillStore()
    # A fill only counts when it can be attributed: token and market included.
    store.record_trade("t-1", asset_id=UP, market="0xmarket", side="BUY",
                       price="0.5", size="10", status="CONFIRMED", source="user_ws")

    rest_rows = [
        {"id": "t-1", "asset_id": UP, "market": "0xmarket", "side": "BUY",
         "price": "0.5", "size": "10", "status": "CONFIRMED"},
        {"id": "t-2", "asset_id": UP, "market": "0xmarket", "side": "BUY",
         "price": "0.6", "size": "5", "status": "CONFIRMED"},
    ]

    class FakeUser:
        def __init__(self):
            self.health = types.SimpleNamespace(status=DISCONNECTED)
        def pong_age(self):
            return 999.0

    rec = RestReconciler(store, lambda: rest_rows, user_feed=FakeUser(), interval=0.05)
    check("reconciler arms when the socket is down", rec.armed)
    new = await rec.run_once()
    s = store.summary()
    check("reconcile recovers only the missed fill", new == 1, str(new))
    check("reconcile suppresses the known fill", rec.suppressed == 1, str(rec.suppressed))
    check("total fills correct after merge", s["fills"] == 2, str(s))
    check("shares not double counted", s["shares"] == 15, str(s))

    await rec.run_once()                       # run again: nothing new
    check("repeat reconcile adds nothing", store.summary()["fills"] == 2,
          str(store.summary()))
    check("repeat reconcile suppresses again", rec.suppressed == 3, str(rec.suppressed))

    class LiveUser(FakeUser):
        def __init__(self):
            super().__init__()
            self.health = types.SimpleNamespace(status=LIVE)
        def pong_age(self):
            return 1.0
    rec2 = RestReconciler(store, lambda: rest_rows, user_feed=LiveUser())
    check("reconciler stays idle while the socket is healthy", not rec2.armed)
    check("healthy audit defaults to two minutes",
          rec2.healthy_audit_interval == 120.0,
          str(rec2.healthy_audit_interval))


async def t_reconcile_generation_and_healthy_audit_close_short_gaps():
    store = FillStore()
    available = {"rows": []}

    class HealthyUser:
        def __init__(self):
            self.health = types.SimpleNamespace(status=LIVE)
            self.subscription_generation = 0

        def pong_age(self):
            return 0.01

    user = HealthyUser()
    calls = []

    def fetch():
        calls.append(time.monotonic())
        return list(available["rows"])

    rec = RestReconciler(
        store, fetch, user_feed=user, interval=0.01,
        healthy_audit_interval=0.08, fetch_timeout=1.0)
    # Model the forced startup pass. It succeeds empty while the socket is
    # healthy, so ordinary outage arming alone would have no reason to poll.
    await rec.run_once()
    rec.start()
    try:
        ok = await until(
            lambda: rec._audited_subscription_generation == 0, 1.0, tick=0.005)
        check("initial subscription generation receives a post-wire audit",
              ok, str(rec.summary()))

        before = rec.polls
        # The outage and recovery both occur between reconciler ticks. Health
        # is LIVE again before it can observe DISCONNECTED; only the completed
        # subscription generation proves that a replay gap may exist.
        user.health.status = DISCONNECTED
        user.health.status = LIVE
        user.subscription_generation += 1
        ok = await until(
            lambda: (rec.polls > before
                     and rec._audited_subscription_generation == 1),
            0.07, tick=0.005)
        check("short reconnect forces REST despite already-healthy status",
              ok, str(rec.summary()))

        # The first post-reconnect response can race venue indexing. Make the
        # trade appear only afterward; the healthy audit must still find it.
        available["rows"] = [{
            "id": "eventually-visible", "asset_id": UP,
            "market": "0xmarket", "side": "BUY", "price": "0.5",
            "size": "4", "status": "CONFIRMED",
            "taker_order_id": "bot-order",
        }]
        before = rec.polls
        ok = await until(
            lambda: "eventually-visible" in store.fills, 0.5, tick=0.005)
        check("healthy periodic audit recovers eventually visible fill",
              ok and rec.polls > before, str(rec.summary()))
    finally:
        await rec.stop()


async def t_timed_out_old_fetch_cannot_acknowledge_new_subscription():
    class HealthyUser:
        def __init__(self):
            self.health = types.SimpleNamespace(status=LIVE)
            self.subscription_generation = 0

        def pong_age(self):
            return 0.01

    user = HealthyUser()
    releases = [threading.Event(), threading.Event()]
    started = []

    def fetch():
        index = len(started)
        started.append(index)
        releases[index].wait(timeout=2.0)
        return []

    rec = RestReconciler(
        FillStore(), fetch, user_feed=user, interval=0.01,
        fetch_timeout=0.03, healthy_audit_interval=10.0)
    # Startup request was created for generation zero and remains alive after
    # its timeout. The socket then completes a new subscription.
    await rec.run_once()
    check("startup fetch timeout is retained for later consumption",
          len(started) == 1 and rec._fetch_task is not None,
          f"started={started} error={rec.last_error}")
    user.subscription_generation = 1
    releases[0].set()
    rec.start()
    try:
        ok = await until(lambda: len(started) >= 2, 1.0, tick=0.005)
        check("new generation starts a fresh request after old result",
              ok, f"started={started} {rec.summary()}")
        check("old request cannot acknowledge the newer subscription",
              rec._audited_subscription_generation != 1,
              str(rec.summary()))
        releases[1].set()
        ok = await until(
            lambda: rec._audited_subscription_generation == 1,
            1.0, tick=0.005)
        check("request created for new scope acknowledges generation",
              ok, str(rec.summary()))
    finally:
        for release in releases:
            release.set()
        await rec.stop()


async def t_rest_reconcile_keeps_explicit_two_hour_market_boundary():
    import run

    old_poly = sys.modules.get("polymarket_trade")
    old_sdk = sys.modules.get("py_clob_client_v2")
    old_time = run.time.time
    requests = []

    class TradeParams:
        def __init__(self, *, market, after):
            self.market = market
            self.after = after

    class Client:
        def get_trades(self, params):
            requests.append((params.market, params.after))
            return [
                {"id": f"ok-{params.market}", "market": params.market},
                {"id": f"cross-{params.market}", "market": "0xmanual"},
            ]

    sdk = types.ModuleType("py_clob_client_v2")
    sdk.TradeParams = TradeParams
    poly = types.ModuleType("polymarket_trade")
    poly._get_client = lambda: Client()
    sys.modules["py_clob_client_v2"] = sdk
    sys.modules["polymarket_trade"] = poly
    run.time.time = lambda: 900_000.0
    try:
        rows = run.fetch_trades(("0xold", "0xcurrent", "0xold"))
        expected_after = 900_000 - 2 * 60 * 60
        check("REST audit sends one stable explicit request per condition",
              requests == [("0xold", expected_after),
                           ("0xcurrent", expected_after)], str(requests))
        check("REST audit rejects cross-market rows even if client returns them",
              [row["id"] for row in rows] == ["ok-0xold", "ok-0xcurrent"],
              str(rows))

        class BrokenClient:
            def get_trades(self, _params):
                raise OSError("temporary venue outage")

        poly._get_client = lambda: BrokenClient()
        failed = False
        try:
            run.fetch_trades(("0xold",))
        except RuntimeError:
            failed = True
        check("REST transport failure is retryable, not an empty success", failed)
    finally:
        run.time.time = old_time
        if old_poly is None:
            sys.modules.pop("polymarket_trade", None)
        else:
            sys.modules["polymarket_trade"] = old_poly
        if old_sdk is None:
            sys.modules.pop("py_clob_client_v2", None)
        else:
            sys.modules["py_clob_client_v2"] = old_sdk


async def t_startup_reconcile_uses_durable_and_current_filters_safely():
    from accounting.ledger import Ledger
    import run

    refused = False
    try:
        run._require_live_fill_coverage(
            paper=False, user_ws="off")
    except RuntimeError:
        refused = True
    check("REST-only live mode is fail-closed because startup has no filter",
          refused)
    # PAPER records its own simulated fills synchronously, and a healthy user
    # socket remains an independent live fill source.
    run._require_live_fill_coverage(
        paper=True, user_ws="off")
    run._require_live_fill_coverage(
        paper=False, user_ws="on")

    with tempfile.TemporaryDirectory() as tmp:
        path = str(pathlib.Path(tmp) / "ledger.json")
        ledger = Ledger(path=path)
        now = time.time()
        window_end = int(now // 300 + 1) * 300
        old_condition = "0xdurable-old"
        current_condition = "0xcurrent"
        token = "durable-token"

        def auth(order_id):
            ledger.authorize_order(order_id, {
                "condition_id": old_condition,
                "token_id": token,
                "requested_notional": 2.0,
                "window_end": window_end,
                "fee_rate": 0.07,
                "fee_exponent": 1,
                "validation": "public-market-preflight",
            })

        auth("seen-order")
        auth("missed-order")
        ledger.authorize_order("legacy-order")
        check("preexisting fill is durably seeded",
              ledger.record_fill_durable(
                  "already-booked", token, shares=4, price=0.5,
                  condition_id=old_condition, order_id="seen-order", fee=0.0))

        hub = types.SimpleNamespace(
            fill_store=FillStore(),
            recent_markets=lambda: (old_condition, current_condition,
                                    current_condition),
        )
        selected = run._reconcile_markets(ledger, hub)
        check("startup filter stably unions durable old and current markets",
              selected == (old_condition, current_condition), str(selected))

        rows = [
            {"id": "already-booked", "asset_id": token,
             "market": old_condition, "side": "BUY", "price": "0.5",
             "size": "4", "status": "CONFIRMED",
             "taker_order_id": "seen-order"},
            {"id": "missed-fill", "asset_id": token,
             "market": old_condition, "side": "BUY", "price": "0.5",
             "size": "4", "status": "CONFIRMED",
             "taker_order_id": "missed-order"},
            # REST/lifecycle duplicate of the genuinely missing fill.
            {"id": "missed-fill", "asset_id": token,
             "market": old_condition, "side": "BUY", "price": "0.5",
             "size": "4", "status": "CONFIRMED",
             "taker_order_id": "missed-order"},
            # Same current market, but not a bot-authorized order. It may be
            # visible in the feed store and must never contaminate accounting.
            {"id": "manual-trade", "asset_id": token,
             "market": current_condition, "side": "BUY", "price": "0.5",
             "size": "8", "status": "CONFIRMED",
             "taker_order_id": "manual-order"},
        ]
        calls = []

        def fetch():
            calls.append(run._reconcile_markets(ledger, hub))
            return rows

        user = types.SimpleNamespace(
            health=types.SimpleNamespace(status=LIVE),
            subscription_generation=0,
            pong_age=lambda: 0.01,
        )
        rec = RestReconciler(
            hub.fill_store, fetch, user_feed=user,
            known_trade=ledger.rest_trade_is_booked)
        recovered, ingested = await run._recover_startup_fills(
            rec, ledger, hub.fill_store)
        check("startup audit executes the stable old+current filter union",
              calls == [selected], str(calls))
        check("durably seen and in-response duplicate rows are suppressed",
              rec.suppressed == 2, str(rec.summary()))
        check("startup synchronously books only the missing authorized fill",
              ingested == 1 and recovered == 2,
              f"recovered={recovered} ingested={ingested}")
        check("manual same-market activity cannot contaminate ledger",
              ledger.skipped_unauthorized == 1
              and ledger.positions[token].shares == 8.0,
              str(ledger.summary()))

        reborn = Ledger(path=path)
        check("startup recovery is persisted before bot launch",
              reborn.has_seen_trade("missed-fill")
              and reborn.positions[token].shares == 8.0,
              str(reborn.summary()))

        # A repeated REST audit cannot add either already-durable bot fill.
        new = await rec.run_once()
        ledger.ingest_fill_store(hub.fill_store)
        check("repeated startup data remains idempotent",
              new == 0 and ledger.positions[token].shares == 8.0,
              f"new={new} {ledger.summary()}")

        ordinary_rows = list(rows)
        auth("post-conflict-order")
        rows[:] = [
            {**rows[0], "status": "FAILED"},
            {"id": "after-conflict", "asset_id": token,
             "market": old_condition, "side": "BUY", "price": "0.5",
             "size": "4", "status": "CONFIRMED",
             "taker_order_id": "post-conflict-order"},
        ]
        new = await rec.run_once()
        after_conflict_ingested = ledger.ingest_fill_store(hub.fill_store)
        check("contradictory durable replay alarms without starving later rows",
              new == 1 and after_conflict_ingested == 1
              and "conflicts with durable fill" in (rec.last_error or ""),
              str(rec.summary()))
        rows[:] = ordinary_rows

        # Clean shutdown must drain fills that arrived after the ledger loop's
        # last periodic pass, once feed producers have stopped.
        auth("late-order")
        late_store = FillStore()
        late_store.record_trade(
            "late-confirmed", order_id="late-order", asset_id=token,
            market=old_condition, side="BUY", price="0.5", size="4",
            status="CONFIRMED", source="user_ws")
        drained = run._persist_fill_drain(ledger, late_store)
        after_shutdown = Ledger(path=path)
        check("final fill-store drain survives clean shutdown",
              drained == 1 and after_shutdown.has_seen_trade("late-confirmed")
              and after_shutdown.positions[token].shares == 16.0,
              str(after_shutdown.summary()))


# ============================================================== REGRESSION ==
def t_paper_state_paths_are_isolated_and_configurable():
    import os

    _stub_sdks()
    import run

    names = {
        "PAPER_LEDGER_PATH": "ledger-v1.json",
        "PAPER_ACCOUNT_PATH": "account-v1.json",
        "PAPER_AUDIT_PATH": "orders-v1.jsonl",
    }
    before = {name: os.environ.get(name) for name in names}
    try:
        with tempfile.TemporaryDirectory() as td:
            base = pathlib.Path(td).resolve()
            expected = {}
            for env_name, filename in names.items():
                target = base / filename
                os.environ[env_name] = str(target)
                expected[env_name] = target
            paths = run._paper_state_paths()
            check("paper profile resolves every mutable artifact",
                  paths == {
                      "ledger": expected["PAPER_LEDGER_PATH"],
                      "account": expected["PAPER_ACCOUNT_PATH"],
                      "audit": expected["PAPER_AUDIT_PATH"],
                  }, str(paths))
        for name in names:
            os.environ.pop(name, None)
        paths = run._paper_state_paths()
        check("defaults live beside the bot, not in another install",
              all(p.parent == run.ROOT for p in paths.values()), str(paths))
    finally:
        for name, value in before.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


# The files that decide, size, price, send and book an order. Carved out of
# spe24 byte-for-byte (b314d70; sniper_mode.py at aaff811), so these are
# spe24's own digests: the guard also proves the carve-out changed nothing. polymarket_trade.py, orderbook.py,
# market_discovery.py, timer.py and config.py carry the same digests spe8's own
# baseline approved. An intentional change to any of them needs review and a
# new digest here (and in tests_dashboard, which reads this table).
TRADING_FILES = ["sniper_mode.py", "sniper_live.py", "paper_trade.py", "polymarket_trade.py",
                 "orderbook.py", "market_discovery.py", "timer.py", "config.py", "fast_h2.py",
                 "accounting/ledger.py", "accounting/fees.py", "accounting/settlement.py",
                 "accounting/resolution.py", "feeds/book.py", "feeds/poly_user.py", "feeds/reconcile.py"]
BASELINE_SHA = {  # approved trading-file baseline; intentional changes require review
    # sniper_mode.py re-approved at spe24 ace4d41: the paper model's taker hold
    # is Polymarket's documented 250ms (was 150). SNIPER_DELAY_SOURCE=live is unchanged.
    # Re-approved at spe24 aaff811: order rows also journal condition_id and the
    # market's taker_delay flag (diagnostics; no decision reads them).
    # Re-approved here (sniperBTC, not spe24): IOC (FAK) orders size a SHARE target
    # (SNIPER_SHARES + this side's unfilled carry, <= SNIPER_MAX_TARGET_SHARES) on
    # the visible levels up to ask + SNIPER_SLIP; actual fills settle the carry,
    # which never crosses sides and dies with its market. FOK is unchanged.
    # Re-approved here: the cooldown is per side (an UP order never blocks DOWN, nor
    # back), 300ms by default, and starts when the order is submitted.
    # Re-approved: exact-threshold moves trigger (K_BP_TOLERANCE); the stake is signed in whole
    # cents (signing_amount); one order in flight PER SIDE (inflight), per-order paper outcomes;
    # rows carry window_ms and fill_confirmed_ms / t_signal_to_send_ms / t_send_to_response_ms.
    # Re-approved (latency audit, decisions unchanged - tests_latency replays old vs new): T1 stamped
    # when the rule fires; FastOrderBuilder stamps T3/T4 around the ECDSA step; order rows carry
    # lat (T0..T7, exec_latency), signal_mid and the outcome counts, all after the send.
    # Re-approved: the SNIPER_SLIP code default is the frozen spec's $0.04 (was $0.02), so a
    # missing .env line no longer runs paper at ask + 2c; a set value still wins.
    # Re-approved: the SNIPER_K_BP code default is the frozen spec's 0.5bp (was 1.0bp).
    # Re-approved: the SNIPER_SLIP code default is the frozen spec's $0.05 (was $0.04).
    # Re-approved (final audit, D2/R1 - no decision rule changed): a malformed or non-finite
    # SNIPER_* value refuses the start instead of becoming the default, and negative cooldown /
    # min-left / non-positive side cap / stale limit are refused; seed_rounds() counts the
    # ledger's open positions into a round's caps after a restart. Rounds that ended over an
    # hour ago are dropped when a new round starts (one entry per market accumulated forever).
    # Re-approved (F1, decisions unchanged): a fill above its share target is journalled
    # (over_target) and counted; the in-flight record carries the stake (reserved cash);
    # round_signals counts each round's signals and gate skips (a round_summary journal row).
    "sniper_mode.py": "d64df8c7d55f5dd72c6a0d4bf3b9e3500158beb536afc413de8d965deb4568b1",
    # Re-approved: stakes signed via signing_amount; an unresolved FAK holds only its own
    # token; fills carry seen_mono (when the quantity became known).
    # Re-approved (latency audit): LeanSender.prepare records the builder's T3/T4 stamps.
    # Re-approved (final audit, D1): the canary caps must parse as finite numbers >= 0 (whole
    # numbers for counts) or the start is refused - "nan" used to switch a cap off.
    # Re-approved (F3): CanarySession - the canary's counters, halts and loss window survive
    # restarts, reconciled with the ledger's authorizations; unreadable state fails closed.
    "sniper_live.py": "ef7e061dc3aa935222547678924d67576df3b25fa4a52789a86ca6adf697b845",
    # Re-approved: one entry in flight per TOKEN (UP and DOWN may overlap; booking stays
    # atomic under _lock); last_fill / last_error / last_timing also per thread.
    # Re-approved (settlement audit): _net_of_own_fills, called on the event loop, reads
    # our recent fills without the broker lock (a booking holds it through the ledger's
    # fsync - and behind a settlement's) and prunes only when the lock is free. Same answer.
    # Re-approved (Position Manager): one order in flight per token PER SOURCE (tag), so another
    # strategy's order can neither refuse nor delay the sniper's on the same token.
    # Re-approved (final audit, D4/D6): the book is netted against our own fills again under
    # the booking lock and the order re-quoted if another source booked on that token since
    # (no displayed share bought twice); an IOC meeting an empty book is a FAK no-fill.
    # Idle execution locks of past rounds' tokens and expired own-fill lists are dropped
    # (both grew by a few entries per market for the life of the process).
    # Re-approved (paper attribution): a paper fill's ledger source carries its tag
    # (paper_live_book/sniper, /manager) - per-source P/L from actual fills.
    "paper_trade.py": "c9a160242ada295db79b24f58c1ad2e13f585b43d6698dee3298a22963821032",
    "polymarket_trade.py": "71b5da67c456c2fb30f3578b84019b0152324bdf338c20c53941eba6b72adc94",
    "orderbook.py": "fe3decc44a8f1061c2788c1f2ae85739ade137910033031eac3138f11b22f090",
    "market_discovery.py": "23c605f678eaf1c6caf60259293b9bccf73413e7f632c0a6749c55acc571aa11",
    "timer.py": "3ca35cc64539d45f7e4b982cbe9b6153138f87ee1be79adfae0c8eaccc875d50",
    "config.py": "4a1584c419e24d805c691d3bf8d7885f998c00aba2e2195f647e963154fda45d",
    "fast_h2.py": "768b40391d3c526431997872192ceb62fe1c4371719d5753b2a588886b7a81fb",
    # Re-approved (F2): fills and settlements append one fsynced journal line (the snapshot
    # records the entry it includes; load replays newer ones, validated, torn tail dropped,
    # gaps/corruption refused); snapshots every 500 entries and on save(). current_payload().
    "accounting/ledger.py": "bf59591bc8dad0fef99b7599c6e68022eb60b38ed839e9fd2cd007c7a680381c",
    "accounting/fees.py": "44e45be00c7c3b330ef0ff11950fa42944029ba83a61ec8066af3494c0956dc7",
    # Re-approved (settlement audit): one record per market (condition id), checked from
    # expiry, concurrently, on the worker's own pool; explicit lifecycle events and a journal;
    # REDEEM PENDING / REDEEMED tracked apart from SETTLED. Ledger.settle is unchanged.
    # Re-approved (F5): a RESULT_MISMATCH detail raises the RESULT_MISMATCH event (never settled).
    "accounting/settlement.py": "57965778646c7c3c316b4c0d11d3f64528a4147cf6e6bf65114d4ef5fdcc8c66",
    # Re-approved: Resolver - the chain's payout vector first, the market's outcome order read
    # once per market, the CLOB + Gamma mirrors only when the chain cannot answer. fetch() is
    # unchanged but for its chain=False switch.
    # Re-approved (F5): which token each payout slot pays is proven on chain (CTF position
    # ids); unproven, the chain result settles only when the CLOB + Gamma mirrors agree,
    # and a disagreement is RESULT_MISMATCH (UNKNOWN, never settled).
    "accounting/resolution.py": "17f0229744797a8df1baaf4c17474731a2d2bcc5a641c741bd2346ab6fdfa7ca",
    # Re-approved (final audit, D5): a refused delta/snapshot (crossed result, malformed values,
    # refused timestamp) on a synced book desyncs its tokens (UNSYNCED until a fresh snapshot)
    # instead of leaving a LIVE book that silently missed a change.
    "feeds/book.py": "07891ccc2e82c37111e934215ca82a11462ee1d58cac34b569ab0c6c4256c432",
    "feeds/poly_user.py": "f0dbd62bd0ef708dc15391915aab39e83802106c26af7780c81712ca42ae6b55",
    "feeds/reconcile.py": "71dea5e832fb970e5cfac1880686cf5eff477a34c48c9b91ba961f77b3e9cb6f",
}
SIDES = (None, "UP", "DOWN")
PRICES = (None, 0.0, 64_000.0, 64_894.0, 64_894.01, 1e9, -5.0)


def _stub_sdks():
    if "web3" in sys.modules:
        return
    m = types.ModuleType("web3")

    class _W3:
        def __init__(self, *a, **k): pass
        def is_connected(self): return False
        @staticmethod
        def HTTPProvider(*a, **k): return None
        @staticmethod
        def to_checksum_address(a): return a
    m.Web3 = _W3
    sys.modules["web3"] = m
    p = types.ModuleType("py_clob_client_v2")
    for n in ("AssetType", "BalanceAllowanceParams", "ClobClient", "MarketOrderArgs",
              "OrderType", "PartialCreateOrderOptions", "Side"):
        setattr(p, n, type(n, (), {"__init__": lambda self, *a, **k: None}))
    sys.modules["py_clob_client_v2"] = p


def t_trading_file_baselines():
    for name in TRADING_FILES:
        # Digest the file as GIT STORES it, not as the working copy
        # happens to sit on disk. .gitattributes pins text to LF, but a
        # Windows checkout can still hold CRLF, and hashing raw bytes made
        # this guard fire on a fresh clone (every LF file differing from a
        # CRLF-recorded baseline) rather than on an actual edit - the exact
        # platform-difference noise it was rewritten to stop.
        raw = (ROOT / name).read_bytes().replace(b"\r\n", b"\n")
        digest = hashlib.sha256(raw).hexdigest()
        check(f"approved baseline {name}", digest == BASELINE_SHA[name],
              f"{digest[:12]} != {BASELINE_SHA[name][:12]}")


async def t_runner_honours_feed_thresholds():
    import os
    _stub_sdks()
    import run

    names = ("BTC_STALE_AFTER", "BOOK_STALE_AFTER", "USER_WS", "RECONCILE")
    old_env = {name: os.environ.get(name) for name in names}
    old_derive = run.derive_creds
    run.derive_creds = lambda: {
        "apiKey": "test-key", "secret": "test-secret",
        "passphrase": "test-passphrase",
    }
    os.environ["BTC_STALE_AFTER"] = "1.25"
    os.environ["BOOK_STALE_AFTER"] = "4.5"
    try:
        hub, cfg = run.build_hub()
        check("BTC_STALE_AFTER reaches the Binance feed",
              hub.binance.stale_after == 1.25, str(hub.binance.stale_after))
        check("BOOK_STALE_AFTER reaches the book state",
              hub.book.stale_after == 4.5, str(hub.book.stale_after))
        check("live keeps the private channel and REST backup on by default",
              cfg.user_ws == "on" and cfg.reconcile == "auto", cfg.describe())
        os.environ["BOOK_STALE_AFTER"] = "soon"
        try:
            run.build_hub(paper=True)
            check("a bad BOOK_STALE_AFTER refuses to start", False, "accepted")
        except ValueError as exc:
            check("a bad BOOK_STALE_AFTER refuses to start and names the value",
                  "BOOK_STALE_AFTER='soon'" in str(exc), str(exc))
        os.environ["BOOK_STALE_AFTER"] = "4.5"
        os.environ["USER_WS"] = "maybe"
        try:
            run.build_hub(paper=True)
            check("a bad USER_WS refuses to start", False, "accepted")
        except ValueError as exc:
            check("a bad USER_WS refuses to start and names the value",
                  "USER_WS='maybe'" in str(exc), str(exc))
        os.environ["USER_WS"] = "off"
        os.environ["RECONCILE"] = "off"
        try:
            run.build_hub()
            check("live with no fill source refuses", False, "accepted")
        except RuntimeError as exc:
            check("live with neither USER_WS nor RECONCILE refuses", "USER_WS" in str(exc), str(exc))
    finally:
        run.derive_creds = old_derive
        for name, value in old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def t_price_change_skips_unsynced_sibling():
    book = BookState(stale_after=8.0)
    book.connected = True
    book.set_active([UP, DOWN])
    now_ms = int(time.time() * 1000)
    check("snapshot syncs UP",
          book.apply_snapshot(UP, [{"price": "0.40", "size": "10"}],
                              [{"price": "0.50", "size": "10"}], ts_ms=now_ms))
    applied = book.apply_price_changes(
        [{"asset_id": UP, "price": "0.41", "size": "12", "side": "BUY"},
         {"asset_id": DOWN, "price": "0.59", "size": "8", "side": "SELL"}],
        ts_ms=now_ms + 1, require_exchange_ts=False)
    check("mixed event still applies the synced token", applied is True)
    check("synced UP bid moved", book.view(UP).best_bid == 0.41, str(book.view(UP).bids))
    check("unsynced DOWN stayed empty", book.view(DOWN).asks == (), str(book.view(DOWN).asks))
    check("unsynced DOWN still needs a snapshot", DOWN in book.needs_resync())
    check("snapshot syncs DOWN",
          book.apply_snapshot(DOWN, [{"price": "0.58", "size": "10"}],
                              [{"price": "0.60", "size": "10"}], ts_ms=now_ms + 50))
    later = book.apply_price_changes(
        [{"asset_id": UP, "price": "0.42", "size": "9", "side": "BUY"},
         {"asset_id": DOWN, "price": "0.61", "size": "9", "side": "SELL"}],
        ts_ms=now_ms + 10, require_exchange_ts=False)
    check("older event still updates the token that can take it", later is True)
    check("UP bid moved on the older mixed event",
          book.view(UP).best_bid == 0.42, str(book.view(UP).bids))
    check("DOWN kept the newer snapshot",
          book.view(DOWN).best_ask == 0.60, str(book.view(DOWN).asks))


# ------------------------------------------------------------------- main --
async def main() -> int:
    sync_tests = [t_backoff_ladder,
                  t_process_lock_rejects_duplicates_without_truncating,
                  t_round_transition_serializes_and_same_state_repairs,
                  t_prepare_transition_serializes_and_same_state_repairs,
                  t_paper_state_paths_are_isolated_and_configurable,
                  t_trading_file_baselines,
                  t_price_change_skips_unsynced_sibling]
    async_tests = [v for k, v in sorted(globals().items())
                   if k.startswith("t_") and asyncio.iscoroutinefunction(v)]
    for t in sync_tests:
        try:
            t()
        except Exception as exc:
            globals()["FAIL"] += 1
            FAILURES.append(f"{t.__name__} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {t.__name__}: {exc}")
    for t in async_tests:
        try:
            await asyncio.wait_for(t(), timeout=45)
        except Exception as exc:
            globals()["FAIL"] += 1
            FAILURES.append(f"{t.__name__} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {t.__name__}: {type(exc).__name__}: {exc}")
    print(f"\n{PASS} passed, {FAIL} failed")
    if FAILURES:
        print("\nFailures:")
        for f in FAILURES[:30]:
            print("  -", f)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
