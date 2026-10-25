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
        self.respond_pongs = True

    async def _handler(self, ws):
        self.conns += 1
        self.live.add(ws)
        try:
            async for raw in ws:
                self.received.append(raw)
                if str(raw).strip().upper() == "PING":
                    self.pings += 1
                    if self.respond_pongs:
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
    srv.respond_pongs = False
    feed = PolyUserFeed({"apiKey": "a", "secret": "b", "passphrase": "c"},
                        f"ws://127.0.0.1:{srv.port}", markets=["0xold"],
                        ping_every=1.0, recv_timeout=30.0)
    feed.start()
    try:
        ok = await until(lambda: bool(srv.auths), 5)
        check("private subscription frame is sent", ok, str(srv.subs))
        check("private heartbeat request is sent before readiness", await until(lambda: srv.pings > 0, 3))
        check("sent subscription alone is not execution-ready before PONG",
              not feed.ready_for_market("0xold"), str(feed.health.as_dict()))
        srv.respond_pongs = True
        await srv.broadcast("PONG")
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
    old_sdk = sys.modules.get("clob_sdk")
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

    sdk = types.ModuleType("clob_sdk")
    sdk.TradeParams = TradeParams
    poly = types.ModuleType("polymarket_trade")
    poly._get_client = lambda: Client()
    sys.modules["clob_sdk"] = sdk
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
            sys.modules.pop("clob_sdk", None)
        else:
            sys.modules["clob_sdk"] = old_sdk


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
                 "accounting/resolution.py", "feeds/book.py", "feeds/poly_user.py", "feeds/reconcile.py",
                 # Added 2026-10-20: since the unified-SDK migration these sign, build and post
                 # every order (they replaced the external py-clob-client), and hybrid_sizing.py
                 # sizes every Kelly order. They belong under the same review lock.
                 "clob_client.py", "clob_signing.py", "clob_sdk.py", "hybrid_sizing.py"]
BASELINE_SHA = {  # approved trading-file baseline; intentional changes require review
    # Reviewed 2026-10-11 deep audit: transport/dispatch ambiguity, freshness,
    # subscription and REST recovery races, paper slot publication, accounting
    # validation, proven resolution mapping and durable replacement. Trading
    # formulas, strategy settings, caps and cooldowns are unchanged; regression
    # and strategy/oracle suites validate the reviewed behavior.
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
    # Re-approved 2026-10-10 (the operator's change: Coinbase is the primary trigger): the
    # default SNIPER_PRIMARY_FEED is coinbase - _coinbase_loop (ticker + heartbeat, BTC-USD;
    # ack checked, out-of-order tickers dropped, starvation/silence reconnect) and a
    # freshness gate on each ticker's match time (cb_fresh). Same rule, gates and sizing;
    # Binance/Kraken still selectable. A Binance tick moves no rule unless Binance is the trigger.
    # Re-approved 2026-10-11 (the operator's change): the SNIPER_K_BP default is the frozen
    # spec's 0.3bp (was 0.5bp).
    # Re-approved 2026-10-14 (repair branch repair/2026-10-14, the operator's master prompt):
    # C1 canary intents are one fsynced journal line per send (v3 state, compaction, archive);
    # C2/H3/M5/M6/M7/L9 ledger indexes, off-lock snapshot payloads, off-loop summaries, next-round
    # rules prefetch, fast FAK resolution, order-hash reconciliation of unknown outcomes; H4
    # backlog-gate/coalescing options (paper only, off); F1 share-sized FAK limit orders (the
    # gates judge the dollar order exactly as before - same signals qualify; the order is whole
    # shares at the cap; SNIPER_ORDER_SIZING=dollars for paper A/B, live refuses it); P2 opt-in
    # SNIPER_BOOK_QUIET (off). feeds/poly_market.py and feeds/hub.py already differed from this
    # table at fd916fb (commit fc52ab0, 2026-10-12, recorded no re-approval); hub.py also adds
    # the prepared round to the user channel (M5). Re-recorded here; see the repair report.
    # Re-approved 2026-10-11 (the operator's rule, SNIPER_FAIR): the below-average rule is
    # removed; every buy only at or under FairValue's fair price less the taker fee and
    # SNIPER_FAIR_MARGIN, the limit capped there - fair UP = Phi(ln(S/S0) / (sigma sqrt tau)),
    # S0 the trigger feed's price at the round's first instant, sigma its 5-minute 1s
    # volatility; unknown inputs skip ("fair value unknown"). The feeds feed the model.
    # Re-approved 2026-10-11 (later, the operator's change): the SNIPER_K_BP default is the
    # frozen spec's 0.5bp again (0.3bp for part of the day); nothing else changed.
    # Re-approved 2026-10-11 (later, the operator's changes): the fair value is MARKET-
    # ANCHORED (anchored_fair: the market's mid for the side as a z-score, moved by
    # |move| / (sigma sqrt tau)) - the start-price model is removed (FairValue -> Volatility,
    # one sample a second); FAK orders are sized at SNIPER_KELLY_FRACTION Kelly on it
    # (kelly_shares, 5..SNIPER_MAX_TARGET_SHARES, no carry) from a cash reading cached off
    # the order path (_bankroll_now / _bankroll_loop); unknown inputs skip.
    # Re-approved 2026-10-11 (later, the operator's changes): SNIPER_KELLY_FRACTION defaults
    # to the frozen spec's 1.0 (full Kelly; was 0.25), and a Kelly order has no per-order cap -
    # only what is left of SNIPER_MAX_SIDE_SHARES for the side in the round (_side_held) and,
    # live, the canary's max_order_shares.
    # Re-approved 2026-10-11 (review): the fair value anchors on the side's book as it stood
    # when the move began (_fair_anchor; never above the mid now) - a mid read after the
    # book had partly reacted counted that part of the move twice. Kelly's cash: live reads
    # the sender's balance less its in-flight reserve on every decision; paper takes each
    # sent order's spend off at once (_bankroll_spent / _set_bankroll). The Coinbase trigger
    # runs on SNIPER_COINBASE_CONNS sockets (default 2), each ticker used once by sequence
    # from the first socket to deliver it (_coinbase_conn / _cb_race).
    # Re-approved 2026-10-11 (operator's audit): hardened parsing of feed prices and
    # timestamps, order tasks drained on stop(), Kelly stakes trimmed to the side's round
    # room at the cap (skip "kelly room under venue minimum at price cap").
    # Re-approved 2026-10-12: a price cap at or below MIN_BUY_PRICE is skipped at the
    # decision ("price cap at the price floor") - the paper broker refused it, live would buy.
    # Reviewed 2026-10-12: user-requested recovery, accounting and portfolio safety fixes.
    # Re-approved 2026-10-12 (audit 20261012): order-task failures stop new decisions; a last
    # freshness check of book and signal at the wire
    # Re-approved 2026-10-13: SNIPER_EXTRA_FEEDS=binance_fut - Binance's USD-M perpetual
    # BTCUSDT book ticker as a PAPER second trigger (on_binance_fut, its own freshness gate
    # bf_fresh on each tick's event time; _gate_for picks a feed's gate). LIVE refuses any
    # second trigger (exec_latency.spec_violations).
    # Re-approved 2026-10-13 (the operator's choice): SNIPER_KELLY_FRACTION defaults to the
    # frozen spec's 0.5 (half Kelly; was full).
    # Re-approved 2026-10-15 (the operator asked for it): DIAGNOSTICS ONLY - a guard_diag row per
    # sent order (whipsaw_diag: paired cost vs this round's confirmed fills, worst-case round loss,
    # 500/1000ms opposite-fill and opposite-order cooldowns, opposite orders still in flight, the
    # netted fair when the market had not repriced) and the
    # shadow's tighter limit (ask + 3c under the cap, "tight"). Spawned after the send; no order,
    # size, cap or gate changes (tests_guard_diag). SNIPER_GUARD_DIAG=0 / SNIPER_SHADOW_TIGHT=0 off.
    # 2026-10-15, the operator's choice: unresolved live fill quantities pause once for five
    # minutes; a lasting pending order no longer blocks later signals forever.
    # 2026-10-15 bug fix: separate failed order tasks pause independently;
    # reused Python object addresses must not suppress a later incident.
    # 2026-10-16, the operator's request (measurement only; no decision, size, cap or gate
    # changes - tests_book_trace): a zero-fill order records no trigger->fill time (the
    # dashboard's TRIG>FILL reads N/A instead of the ACK); every order sent gets a book_trace
    # row (asks and depth at or under its cap at tick +0/50/100/150/250ms and at the reply,
    # order type, reply status, order id) and, live, a user_trades row (its trades from the
    # authenticated user channel). Live order/fill rows carry the reply's / fill's trade ids.
    # SNIPER_BOOK_TRACE_MS="" turns the trace off.
    # 2026-10-16 (item 4, measurement only): user_trades rows carry the venue's own match time,
    # found among the public market channel's trade prints (our token, BUY, our size or a maker
    # leg's): match_after_tick_ms / ack_after_match_ms, and a trigger_to_match latency sample.
    # Reviewed 2026-10-17 execution audit: Public price/size coincidences are unverified candidates, not measured matches; require paired legs and BUY side.
    # 2026-10-17, the operator's change: the trigger default follows the frozen spec,
    # SNIPER_K_BP 0.5 -> 1.0 (exec_latency.FROZEN_SPEC); the fair margin stays $0.01.
    # 2026-10-17 (measurement only; no decision reads them - tests_book_trace DecisionBook): order
    # rows carry dec_bid/ask/spread/cap/levels_under_cap/depth_sh/depth_usd/quote_age_ms/venue_age_ms
    # from the decision's own book, computed after the hand-off; book_trace rows carry the
    # client order id for exact joins.
    # 2026-10-17 (measurement only - tests_book_trace TraceFlow): book_trace rows carry a flow - per
    # step between snapshots, the depth under the cap that disappeared, the part public BUY prints
    # account for, and at least how much was cancelled (flow_report.py summarises them).
    # 2026-10-18 (measurement only - tests_liquidity_attribution): book_trace rows carry an
    # attribution - per step and span, gross ask reductions/additions under the original cap from
    # the book's own states, public BUY prints matched against them, feed coverage, a diagnostic
    # label, a print side check and bounded raw events; points list every ask level under the cap.
    # Re-recorded 2026-10-20 (completion pass over the 2026-10-19/20 work, which changed this file
    # without a new digest; review the diff before live): the unified-SDK order builder (clob_sdk);
    # a FAK whose every trade FAILED retracts its provisional slot, shares, cost and fill count
    # (BUGFIXES_2026-10-19.md); hybrid sizing - the quarter-Kelly input reduced by trigger-feed
    # volatility and realized drawdown from a ledger snapshot refreshed off the order path, the
    # optional order-budget caps (off), the bounded minimum-entry exception, and share-sized FAKs
    # signed as whole shares x the cap with the side's room bounding the SIGNED shares (share
    # limits are pre-trade estimates; HYBRID_SIZING.md); paper reserves the all-in stake plus fees
    # until its worker returns, and worker/executor failures stop new decisions
    # (AUDIT_FIXES_20261020.md). The whole offline suite and the strict native-SDK check pass.
    # 2026-10-22: scope the decision-layer pending-FAK gate to the current
    # condition and token; old-market orders retain reconciliation without
    # spuriously starting a new five-minute pause after round rotation.
    # Also freeze the original dispatch-window deadline in each decision and
    # recheck it at the transport boundary after persistence/connection waits.
    # 2026-10-22, the operator's changes: the trigger default back to 0.5bp (the frozen spec;
    # 1.0bp from 10-17); SNIPER_HYBRID_DRAWDOWN_PAUSE_S (default 600): at the drawdown stop
    # no orders that long, then a new hybrid epoch from the current equity (peak = equity).
    # 2026-10-23 (AUDIT_20261023.md M2): an unexpected order-path failure is a visible 5-minute
    # pause (journalled execution_fault, announced, in the summary), never a silent stop of all
    # decisions; a failed dispatch is held as an unknown outcome first; a stop without a kill
    # switch is visible (Sniper.stopped). Trading formulas, sizing and settings are unchanged.
    # 2026-10-25 (the operator's "fix everything"): SNIPER_PAPER_EXTRA_MS defaults to 200 (was 0 -
    # on live's own 1,154 orders 0 made paper +$210.68 where live made +$1.42; 200 matched live's
    # fill rate); paper only, never live. SNIPER_KELLY_MIN_ENTRY defaults to 0 (AUDIT_20261023.md
    # M3), and when on it is never taken below the equity high-water mark (minimum_entry_blocked
    # "realized drawdown"). Trigger, fair value, Kelly formula, caps and live dispatch are unchanged.
    "sniper_mode.py": "2313ab258c8495d18093804cb1cd93845719db39780b55e064038cbc3a72d37b",
    # Re-approved: stakes signed via signing_amount; an unresolved FAK holds only its own
    # token; fills carry seen_mono (when the quantity became known).
    # Re-approved (latency audit): LeanSender.prepare records the builder's T3/T4 stamps.
    # Re-approved (final audit, D1): the canary caps must parse as finite numbers >= 0 (whole
    # numbers for counts) or the start is refused - "nan" used to switch a cap off.
    # Re-approved (F3): CanarySession - the canary's counters, halts and loss window survive
    # restarts, reconciled with the ledger's authorizations; unreadable state fails closed.
    # Reviewed 2026-10-12: user-requested recovery, accounting and portfolio safety fixes.
    # Re-approved 2026-10-12 (audit 20261012): one outcome per prepared order; monotonic server
    # clock; conflicting venue evidence is AMBIGUOUS
    # Re-approved 2026-10-14 evening (live evidence, the operator's choice "Kelly shares at book
    # cost"): Polymarket filled an 11-share FAK ($3.74 at the 0.34 cap) with 12.896552 shares,
    # so share sizing (F1) is withdrawn - SNIPER_ORDER_SIZING=shares is refused and _decide is
    # the pre-repair one again (syntax-tree identical to fd916fb); the plumbing stays tested.
    # sniper_live.py: a FAK whose own evidence halted trading is not timed out a second time.
    # 2026-10-15: the canary intent keeps the reply's trade ids; a restart settles an accepted order
    # the CLOB has no record of from exactly those trades booked under its authorization.
    # It also keeps the reply's totals (makingAmount, takingAmount): with no trade ids, the order's
    # booked lots must add up to them exactly.
    # Halt reasons show an order id short (short_id): the dashboard hides raw 32-byte hex.
    # 2026-10-15, the operator's choice: SNIPER_ORDER_SIZING=shares again - a FAK spends its whole
    # shares x the cap as a dollar order (cheaper fills buy more, never a halt); the gates judge the
    # book cost (same signals); near a cap it spends at most the room at the ask.
    # 2026-10-15, the operator's choice: 3 slow venue replies within 30 min pause new orders 5 min
    # (then healthy checks) instead of hard-halting; >5s replies and unrecovered pauses still halt.
    # ...and the strike window is 5 minutes (was 30), the operator's choice.
    # 2026-10-16: an accepted FAK's final size comes from the reply's own totals (makingAmount /
    # takingAmount) when it names no trade ids - no order-status wait, no 5-minute pause meanwhile.
    # 2026-10-16: the shadow also prices an order at the fair-value cap (fv: FOK + FAK, same dollars and
    # whole shares x that cap) where ask + slip bound the real cap - diagnostics for exec_eval.py only.
    # 2026-10-16: the FOK paper bot's switches (SNIPER_FOK_KELLY, SNIPER_KELLY_BUY_MIN, SNIPER_FAIR_CAP,
    # 7e2b214/33fb5b7/eade7a2) removed at the operator's request - the code as before them.
    # 2026-10-16: SNIPER_PAPER_EXTRA_MS (paper only, default 0): paper meets the book that much later
    # than each order's delay - the book's lag behind the venue; +200ms matched 1,154 live orders.
    # Re-approved 2026-10-14 (restart halt): a previous run's unresolved send intents are looked
    # up on the venue by their own order hash at start (_reconcile_previous_run, M7's evidence
    # rules); proven zero or fully booked resolves them and lifts the derived start-up halt
    # (CanarySession.PREVIOUS_RUN, never persisted). Nothing is ever resent.
    # 2026-10-15, the operator's choice: every live halt pauses five minutes, persistent
    # causes pause once, caps gain one original increment after each completed wait, the
    # slow-reply guard never hard-stops, and prior-run reservations remain in the budget.
    # Pauses and cap steps survive restart; STOP_LIVE holds while its file exists.
    # 2026-10-15 bug fixes: one order identity across restarts (including legacy
    # pause keys); fresh venue incidents cannot be lost at expiry; reporting
    # failures cannot interrupt the guard; monotonic zero is a valid start time.
    # Fresh kill-switch incidents also expire old reasons before deduplication.
    # Re-approved 2026-10-16 (live cash freeze, order 0x0d0ce8b4 - its only trade FAILED and it
    # stayed awaiting; the cash read could only fall for an hour): with orders unconfirmed the
    # cash is the reading less what they could still take (reserve - booked cost); a FAK whose
    # every trade FAILED, covering its whole reply, nothing booked, for 120s, is released as a
    # zero fill; an order past its deadline is warned about every 5 minutes. A failed hold
    # lookup keeps the strict rule. A previous run's FAK whose every trade FAILED (start-up
    # REST audit) and nothing booked resolves NO_MATCH at restart. An unconfirmed order with no
    # known reservation still blocks any refund (CollateralAudit).
    # 2026-10-17 (the operator's request; no order, size or cash-rule change - tests_balance_reads):
    # a balance read 90s and 180s after every round ends (balance_read rows: wallet, applied,
    # rule, hold, Kelly cash) and a cash_stuck alarm when the cash stays > $1 under the wallet
    # for 10 minutes (repeated every 10 minutes until it clears).
    # 2026-10-18 (safety - tests_canary_limits): a restored canary budget may only tighten this
    # start's configured limits - each ceiling and step is min(saved, configured); a lowered .env
    # loss cap was kept at the saved $50.
    # Re-recorded 2026-10-20 (completion pass; review the diff before live): unified-SDK signing
    # (clob_sdk); FAK fills are accepted by their signed maker dollars - improved-price quantity
    # above the sizing target is booked, a spend above the signed budget still halts; the hybrid
    # risk recheck runs at prepare and in the send guard; minimum-entry claims live in the durable
    # send intent; archive compaction drops only an incomplete tail; optional warm-only dispatch
    # (SNIPER_WARM_ONLY) and live_send transport/intent telemetry; the clock refresh requires HTTP
    # 200 and a finite, positive timestamp (BUGFIXES_2026-10-19.md, LATENCY_TARGET_200MS.md,
    # AUDIT_FIXES_20261020.md).
    # 2026-10-22: retain uncertain cash reservations during balance refresh;
    # serialize counter observations with their journal sequence; checkpoint
    # markers reflect persisted state; use execution-observation timestamps and
    # recheck the original decision cutoff at dispatch.
    # 2026-10-22 (live: wallet $40.04, the bot used $12.95): a previous run's unresolved
    # orders no longer hold cash - the wallet already shows whatever they took; they still
    # count in the loss budget. This run's uncertain orders keep holding it.
    # 2026-10-23, the operator's choice "read + orders wait": the cash is read in the 5s before
    # every round starts (retried each second until a read succeeds) and no order in a round
    # until there is a good reading from 10s before its start or later (_pre_start_read_loop).
    # 2026-10-23: `reserved` goes back to exactly 0 when no order is on the wire - float residue
    # from overlapping orders (4.4e-16) kept every read on the in_flight rule and Kelly sized from
    # $0.17 of a $40.60 wallet for about 21 hours on 10-22.
    # 2026-10-23 (AUDIT_20261023.md H1 H2 H3 M1 M2): an unknown-outcome FAK is settled from the
    # account's trade record of its market once that market closed (no trade, or only FAILED: zero;
    # its trades: authorized); a previous run's the same way. A pending fill size holds only its token
    # (all orders pause only after PENDING_FAK_HALT_S). Slow replies pause after 3 in 30 min (one
    # over 5s at once). SNIPER_SEND_TIMEOUT_S (10s) / SNIPER_GUARD_*. A failed dispatch is held as
    # an unknown outcome (dispatch_unknown). The final reconcile-by-hand pause has its own key.
    # 2026-10-25 (H1's window): LeanSender._after stamps the row's wall before the unknown-outcome
    # resolver copies it (it got none and dated the send from the clock after ~120s of get_order
    # lookups, so the trade record was read from ~0.9s AFTER the send and an executed FAK could
    # settle NO_MATCH); the record is read for the whole round (HISTORY_BEFORE_END_S = 600s
    # before its end, or 120s before the send if earlier) - a late or missing send time cannot
    # narrow it. No order, size, cash or pause rule changed.
    "sniper_live.py": "2057cd9fc506d7af485be4e9158106972d50cdfefad732f70d1be2f545cb968c",
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
    # Re-approved 2026-10-12 (audit 20261012): a failing event observer cannot change a committed
    # paper fill or rejection
    # Reviewed 2026-10-17 execution audit: Arrival comparison quotes use the submitted FAK/FOK semantics, including FAK partial fills.
    # Re-recorded 2026-10-20 (completion pass): a share-sized FAK walks its fixed signed dollar
    # budget (shares x the limit, estimate_fak_shares) as the venue does, so a cheaper level buys
    # more shares; partial fills are classified by unspent dollars; the sniper's hybrid risk guard
    # is checked at the decision and again at booking (AUDIT_FIXES_20261020.md).
    "paper_trade.py": "ddfb4b367409a25a5072bbb5c94178b0641021e27e9881973aa11eee4d59e6d8",
    # Re-approved 2026-10-12 (audit 20261012): a rejection that also carries acceptance evidence
    # is not definitive
    # Re-recorded 2026-10-20 (completion pass): the venue client is the unified-SDK adapter
    # (clob_client via clob_sdk), which configures its own transports; py-clob-client is retired
    # (SDK_UPGRADE_2026-10-19.md).
    "polymarket_trade.py": "246f63a7c7a8de604549dceacc7af95babc9b2ad270f0fbba5ddc3a84d8a3125",
    # Re-approved 2026-10-12 (audit 20261012): supplied/cached books pass the same level
    # validation; crossed books refused
    # Re-recorded 2026-10-20 (completion pass): get_orderbook_snapshot keeps a public REST book's
    # venue timestamp, receipt time, tick and hash (RestBookSnapshot) for executable recovery
    # (BUGFIXES_2026-10-19.md).
    "orderbook.py": "650e630fb13b9ae9a552a6e55a08cf4aa9c64723fbc163a178ea1f9c8f22506a",
    # 2026-10-20: market_discovery.py, timer.py, fast_h2.py, accounting/fees.py, settlement.py,
    # resolution.py, feeds/poly_user.py and feeds/reconcile.py were unchanged; the uploaded copy
    # had gained one extra trailing blank line in 113 files, removed again so these match.
    "market_discovery.py": "ca15ccc4c1b0da18248abc91143604d15fe00b1638d727bf5209d6f9ff059dc1",
    "timer.py": "3ca35cc64539d45f7e4b982cbe9b6153138f87ee1be79adfae0c8eaccc875d50",
    # Re-recorded 2026-10-20 (completion pass): unified-SDK migration settings; signature type 3
    # (deposit wallet) stays refused pending its validation with the unified SDK
    # (SDK_UPGRADE_2026-10-19.md).
    "config.py": "27c1aec5a5b914fe5a3c01123a569041825c13faa2e1ba119f99101a49497aee",
    "fast_h2.py": "5b9f222bbc3e3ae134d28c13e28db0c8063a35e19ff98b0548d405f0b87cca71",
    # Re-approved (F2): fills and settlements append one fsynced journal line (the snapshot
    # records the entry it includes; load replays newer ones, validated, torn tail dropped,
    # gaps/corruption refused); snapshots every 500 entries and on save(). current_payload().
    # Reviewed 2026-10-12: user-requested recovery, accounting and portfolio safety fixes.
    # Re-approved 2026-10-12 (audit 20261012): durable-fill rollback keeps an undo record instead
    # of deep-copying the position history
    # Re-approved 2026-10-16: reconcile_balance checks the last 15 minutes of reads, and the wallet
    # may rise by the payouts of positions settled from 10 minutes before them (this account's
    # winnings come back as cash automatically); every payout and any deposit were a MISMATCH for
    # the rest of the run. Missing fills (it fell) and deposits (it rose more) still are.
    # Re-recorded 2026-10-20 (completion pass): the hybrid realized-equity risk epoch (reference
    # capital, realized-P&L baseline, high-water mark) is persisted in the snapshot and its peaks
    # kept through durable transactions and journal replay; hybrid_risk_view refuses a reference
    # reading that raced a fill or settlement (HYBRID_SIZING.md, SDK_UPGRADE_2026-10-19.md).
    # 2026-10-22: durable authorization keeps the writer lock while releasing
    # the inventory state lock before journal/snapshot disk I/O.
    # 2026-10-22: hybrid_risk_view(rebase=True) starts a new epoch from the current realized
    # equity (capital = equity, P&L from now, peak = equity), durable - the drawdown pause's reset.
    "accounting/ledger.py": "32e08e978e02f75693a6ff0b847c022b91fe3011d92cef3230ddad54bd32e62c",
    "accounting/fees.py": "44e45be00c7c3b330ef0ff11950fa42944029ba83a61ec8066af3494c0956dc7",
    # Re-approved (settlement audit): one record per market (condition id), checked from
    # expiry, concurrently, on the worker's own pool; explicit lifecycle events and a journal;
    # REDEEM PENDING / REDEEMED tracked apart from SETTLED. Ledger.settle is unchanged.
    # Re-approved (F5): a RESULT_MISMATCH detail raises the RESULT_MISMATCH event (never settled).
    # Reviewed 2026-10-12: user-requested recovery, accounting and portfolio safety fixes.
    "accounting/settlement.py": "2eadbbd30ebaa53fc88846d0cba148dbaf8f9ebfa83573229c5b3e6492499080",
    # Re-approved: Resolver - the chain's payout vector first, the market's outcome order read
    # once per market, the CLOB + Gamma mirrors only when the chain cannot answer. fetch() is
    # unchanged but for its chain=False switch.
    # Re-approved (F5): which token each payout slot pays is proven on chain (CTF position
    # ids); unproven, the chain result settles only when the CLOB + Gamma mirrors agree,
    # and a disagreement is RESULT_MISMATCH (UNKNOWN, never settled).
    "accounting/resolution.py": "131566abda8c01fc8c1688e24a4348974ba8e63e0b90ad0cf6941158218fb45c",
    # Re-approved (final audit, D5): a refused delta/snapshot (crossed result, malformed values,
    # refused timestamp) on a synced book desyncs its tokens (UNSYNCED until a fresh snapshot)
    # instead of leaving a LIVE book that silently missed a change.
    # Re-approved 2026-10-11 (operator's audit): malformed updates and invalid
    # exchange timestamps fail closed; REST snapshots cannot overwrite newer socket data.
    # Reviewed 2026-10-12: user-requested recovery, accounting and portfolio safety fixes.
    # Re-approved 2026-10-12 (audit 20261012): deltas fenced by the newest observed exchange time,
    # so a REST snapshot cannot rewind them; timestamped tick changes; REST tuples cannot certify
    # a past venue instant
    # Re-recorded 2026-10-20 (completion pass): a REST observation carries its provenance and
    # receipt time, cannot advance the WebSocket watermark or certify an earlier venue-time book;
    # history uses installation time while freshness keeps the receipt (BUGFIXES_2026-10-19.md).
    "feeds/book.py": "8d99e3144f4482270445880cb75099252a273b1e78af7ff4e1a16d65fb1aba50",
    # Reviewed 2026-10-12: user-requested recovery, accounting and portfolio safety fixes.
    # Re-approved 2026-10-12 (audit 20261012): FillStore summary cached until the next mutation
    # 2026-10-22: separately timestamp when a complete trade becomes execution
    # evidence, so RETRYING and incomplete rows cannot backdate fill latency.
    "feeds/poly_user.py": "b1a92f77feb3658f738bf32034c39595d4f7c61ccb5db40973230fdc9f45a1f6",
    # Reviewed 2026-10-12: receive fencing and ordered public-token snapshot recovery.
    # hub.py and poly_market.py are listed, not locked (not in TRADING_FILES). Re-recorded
    # 2026-10-20: proactive REST recovery uses the metadata-preserving book read
    # (BUGFIXES_2026-10-19.md).
    "feeds/hub.py": "78a4290e17723f430b869853559c5af5c391e332a13532209a1bcb10912d46a3",
    "feeds/poly_market.py": "82b4a0bd2a8cece33e6f5190acb8502cced9775b558d8e20aa2b30907d7392e0",
    "feeds/reconcile.py": "71dea5e832fb970e5cfac1880686cf5eff477a34c48c9b91ba961f77b3e9cb6f",
    # Added 2026-10-20 (the unified-SDK migration, SDK_UPGRADE_2026-10-19.md): the adapter that
    # preserves the bot's client API over polymarket-client 0.13.0 and posts each order once; the
    # SDK-backed signing, hashing and HMAC; their import surface; and the pure hybrid sizing
    # functions (HYBRID_SIZING.md). tests_unified_sdk checks the native wire, hashes and HMAC.
    "clob_client.py": "cf39e32e4e78500068de88ef8538073319411fe1710473790cd074cde329f216",
    "clob_signing.py": "bfe704a25e5cc52ea4bdb4f085eac13d2710a83f64d7beee56d2d287707f305a",
    "clob_sdk.py": "f03d066498ecde9e0afcb85ae3e4dcbfc0e18d5ffc1f483d3d0cdcf106382de5",
    # 2026-10-22: at_drawdown_stop - sizing_factors' exact drawdown-stop test alone (no volatility),
    # for the drawdown pause.
    "hybrid_sizing.py": "939ccf5b1ecabdf8b0e3e46f8b012d9324997e8c50dcc927405a371b40eb3cd0",
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
    p = types.ModuleType("clob_sdk")
    for n in ("AssetType", "BalanceAllowanceParams", "ClobClient", "MarketOrderArgs",
              "OrderType", "PartialCreateOrderOptions", "Side"):
        setattr(p, n, type(n, (), {"__init__": lambda self, *a, **k: None}))
    sys.modules["clob_sdk"] = p


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
