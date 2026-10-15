"""Lean live sender + the live canary against a fake CLOB on 127.0.0.1 - no real venue, no
real keys: every key and API credential here is random, made per run.

    python tests_sniper_live.py
"""
from __future__ import annotations

import asyncio
import base64
import collections
import json
import os
import secrets
import sys
import tempfile
import time
import types
import uuid
from decimal import Decimal as D

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import httpx  # noqa: E402,F401 - the HTTP client the lean sender is tested with

import polymarket_trade as pt  # noqa: E402
import sniper_live as L  # noqa: E402
from py_clob_client_v2.clob_types import ApiCreds  # noqa: E402
from py_clob_client_v2.order_builder.builder import OrderBuilder  # noqa: E402
from py_clob_client_v2.signer import Signer  # noqa: E402
from py_clob_client_v2.signing.hmac import build_hmac_signature  # noqa: E402

PASS = FAIL = 0
FAILURES = []
UP = str(10 ** 70 + 2)
DN = str(10 ** 70 + 3)
COND = "0x" + "ab" * 32


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL {name}: {detail}")


MATCHED_REPLY = {"success": True, "status": "matched", "orderID": "0xorder1",
                 "tradeIDs": ["trade-1"], "transactionsHashes": [], "errorMsg": ""}


class FakeClob:
    """HTTP/1.1 keep-alive server: /time, /order with a scripted reply queue."""

    def __init__(self, skew_s=0.0):
        self.skew, self.script = skew_s, collections.deque()
        self.hits = collections.Counter()
        self.orders = []

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.url = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    async def stop(self):
        self.server.close()

    async def _reply(self, writer, status, text):
        body = text.encode()
        writer.write(f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n"
                     .encode() + body)
        await writer.drain()

    async def _handle(self, reader, writer):
        try:
            while True:
                head = (await reader.readuntil(b"\r\n\r\n")).decode()
                lines = head.split("\r\n")
                method, path, _ = lines[0].split(" ", 2)
                hdrs = {k.strip().lower(): v.strip() for k, v in (x.split(":", 1) for x in lines[1:] if ":" in x)}
                n = int(hdrs.get("content-length", 0))
                body = await reader.readexactly(n) if n else b""
                self.hits[(method, path)] += 1
                if path == "/time":
                    await self._reply(writer, 200, str(int(time.time() + self.skew)))
                    continue
                self.orders.append((json.loads(body), hdrs, body.decode(), time.time() + self.skew))
                action = self.script.popleft() if self.script else ("json", 200, MATCHED_REPLY)
                if action[0] == "sleep":
                    await asyncio.sleep(action[1])
                    action = action[2]
                if action[0] == "hang":
                    await asyncio.sleep(action[1])
                    writer.close()
                    return
                if action[0] == "reset":
                    writer.close()
                    return
                await self._reply(writer, action[1], json.dumps(action[2]) if not isinstance(action[2], str)
                                  else action[2])
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass


def rules_fetch(cond, up, down, tick="0.01"):
    return {"tick": D(tick), "neg_risk": False, "minimum": D("5"), "fee_rate": D("0.07"),
            "fee_exponent": 1, "taker_delay": True}


def reset_ambiguity():
    pt._ambiguous_condition = None
    pt._ambiguous_until = 0.0
    pt._ambiguous_tokens.clear()
    pt._ambiguous_all_tokens = False


async def make(fake, *, limits=None, stop_file=None, tick="0.01", latency_halt_ms=1500.0, timeout_s=3.0):
    reset_ambiguity()
    signer = L.fast_signer("0x" + secrets.token_hex(32), 137)
    creds = ApiCreds(api_key=str(uuid.uuid4()), api_secret=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
                     api_passphrase=secrets.token_hex(16))
    http = L.make_http()
    clock = L.ServerClock()
    rules = L.RoundRules(lambda c, u, d: rules_fetch(c, u, d, tick))
    rows = []
    sender = L.LeanSender(builder=OrderBuilder(signer), signer=signer, creds=creds, host=fake.url, http=http,
                          clock=clock, rules=rules, limits=limits or L.CanaryLimits(),
                          kill=L.KillSwitch(stop_file), journal=rows.append, latency_halt_ms=latency_halt_ms,
                          timeout_s=timeout_s)
    await clock.refresh(http, fake.url + "/time")              # off the hot path; also opens the connection
    await rules.prefetch(COND, UP, DN)
    return sender, rows


def order(sender, cap=0.54, amount=5.2, token=UP, side="UP"):
    return sender.prepare(side=side, token=token, up=UP, down=DN, condition=COND, end=time.time() + 120,
                          amount=amount, cap=cap)


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------- clock
def t_server_clock_offset_staleness_and_never_synced():
    async def go():
        fake = await FakeClob(skew_s=7.3).start()
        sender, _ = await make(fake)
        off = sender.clock.offset
        ts = sender.clock.now_s()
        sender.clock.at -= sender.clock.max_age_s + 1
        try:
            sender.clock.now_s()
            stale = False
        except L.Halted:
            stale = True
        fresh = L.ServerClock()
        try:
            fresh.now_s()
            never = False
        except L.Halted:
            never = True
        await fake.stop()
        return off, ts, stale, never
    off, ts, stale, never = run(go())
    check("the measured offset matches a server 7.3s ahead (whole-second /time, centred: within 0.55s)",
          abs(off - 7.3) <= 0.55, str(off))
    check("the signing timestamp is local time + that offset", abs(ts - (time.time() + off)) <= 1.5, str(ts))
    check("a stale offset refuses to timestamp an order (Halted, nothing sent)", stale)
    check("a clock that never synced refuses too", never)


# ---------------------------------------------------------------- the hot path does one request
def t_hot_path_makes_exactly_one_request_and_reuses_the_connection():
    async def go():
        fake = await FakeClob().start()
        sender, rows = await make(fake)
        fake.hits.clear()
        res = [await sender.send(order(sender)) for _ in range(3)]
        hits = dict(fake.hits)
        await sender.http.aclose()
        await fake.stop()
        return res, hits
    res, hits = run(go())
    check("three orders = three POST /order and nothing else (no GET /time, no market info, no balance, no book)",
          hits == {("POST", "/order"): 3}, str(hits))
    check("every order rides the warm connection (no TCP/TLS connect on the hot path)",
          all(r["new_connection"] is False for r in res), str([r["new_connection"] for r in res]))
    check("matched replies classify as MATCHED", all(r["kind"] == L.MATCHED for r in res), str([r["kind"] for r in res]))


def t_an_order_after_a_quiet_spell_still_uses_the_warm_connection():
    async def go():
        fake = await FakeClob().start()
        sender, rows = await make(fake)
        await sender.send(order(sender))
        await asyncio.sleep(6.5)                       # past httpx's default 5s idle expiry
        r = await sender.send(order(sender))
        await sender.http.aclose()
        await fake.stop()
        return r
    r = run(go())
    check("6.5s after the last request, the order still goes out on the open connection (no new TCP)",
          r["new_connection"] is False, str(r["new_connection"]))


def t_keep_warm_refreshes_the_clock_on_the_order_connection():
    async def go():
        fake = await FakeClob(skew_s=3.0).start()
        sender, rows = await make(fake)
        events = []
        task = asyncio.ensure_future(L.keep_warm(sender.http, sender.clock, fake.url + "/time", every_s=0.2,
                                                 on_event=events.append))
        await asyncio.sleep(1.1)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await sender.http.aclose()
        await fake.stop()
        return events, sender.clock
    events, clock = run(go())
    check("keep_warm refreshes repeatedly on the same connection and keeps the clock fresh",
          len(events) >= 4 and all(e.get("ok") for e in events) and not any(e["new_connection"] for e in events)
          and clock.status()["age_s"] < 0.5 and abs(clock.offset - 3.0) <= 0.55, f"{events[:3]} {clock.status()}")


def t_send_timestamps_come_from_the_transport_in_order():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("sleep", 0.05, ("json", 200, MATCHED_REPLY)))
        sender, rows = await make(fake)
        r = await sender.send(order(sender))
        await fake.stop()
        return r
    r = run(go())
    ts = r["ts_us"]
    seq = ["prepare", "build", "signed", "ready", "post_call", "net_send_start", "net_send_done", "response",
           "response_body"]
    check("every stamp present: request ready, network send start/done, response",
          all(k in ts for k in seq), str(ts))
    check("...in path order (ready < send start < send done < response)",
          [ts[k] for k in seq] == sorted(ts[k] for k in seq), str(ts))
    check("network send start is the socket write, not 'signed': the response came >=50ms after it",
          ts["response"] - ts["net_send_start"] >= 49_000, str(ts))


def t_the_venue_could_authenticate_what_was_sent():
    async def go():
        fake = await FakeClob(skew_s=4.0).start()
        sender, rows = await make(fake)
        p = order(sender, cap=0.57, amount=5.7)
        await sender.send(p)
        await fake.stop()
        return sender, p, fake.orders[-1]
    sender, p, (body, hdrs, raw, server_now) = run(go())
    want = build_hmac_signature(sender.creds.api_secret, hdrs["poly_timestamp"], "POST", "/order", raw)
    check("the L2 HMAC verifies over the exact bytes received (POLY_SIGNATURE)", hdrs.get("poly_signature") == want,
          f"{hdrs.get('poly_signature')} vs {want}")
    check("POLY_TIMESTAMP is the venue's clock (server 4s ahead), from the cached offset - no GET /time",
          abs(int(hdrs["poly_timestamp"]) - server_now) <= 1, f"{hdrs['poly_timestamp']} vs {server_now:.1f}")
    check("api key, owner, FOK, signer address all as the library would send them",
          hdrs.get("poly_api_key") == sender.creds.api_key and body["owner"] == sender.creds.api_key
          and body["orderType"] == "FOK" and body["order"]["signer"].lower() == sender.signer.address().lower()
          and hdrs.get("poly_address", "").lower() == sender.signer.address().lower(), str(body)[:300])
    check("0.57 is signed as 0.57 (the library's float floor would make it 0.56)", p.price == 0.57
          and float(D(body["order"]["makerAmount"]) / D(body["order"]["takerAmount"])) > 0.565, str(body["order"]))


def t_fast_signer_signs_byte_identical_orders():
    import dataclasses
    import random
    import py_clob_client_v2.order_builder.builder as B
    from py_clob_client_v2.clob_types import CreateOrderOptions, MarketOrderArgsV2, OrderType
    from py_clob_client_v2.order_builder.constants import BUY
    key = "0x" + secrets.token_hex(32)
    lib, fast = OrderBuilder(Signer(key, 137)), OrderBuilder(L.fast_signer(key, 137))
    rng, same, n, real_ns = random.Random(1), 0, 40, B.time.time_ns
    import py_clob_client_v2.order_utils.exchange_order_builder_v2 as X
    real_init = X.ExchangeOrderBuilderV2.__init__
    salt = [0]

    def init(self, *a, **k):                          # pin the random salt so the two can be compared
        real_init(self, *a, **k)
        self.generate_salt = lambda: salt[0]
    X.ExchangeOrderBuilderV2.__init__ = init
    try:
        for i in range(n):
            salt[0] = rng.getrandbits(60)
            tick = rng.choice(["0.01", "0.001"])
            args = dict(token_id=str(10 ** 70 + rng.randint(0, 9)), amount=round(rng.uniform(1, 9), 2), side=BUY,
                        price=round(rng.uniform(0.05, 0.95), len(tick) - 2), order_type=OrderType.FOK)
            opts = CreateOrderOptions(tick_size=tick, neg_risk=rng.random() < 0.3)
            B.time.time_ns = lambda i=i: (1_790_000_000_000 + i) * 1_000_000
            a = lib.build_market_order(MarketOrderArgsV2(**args), opts, version=2)
            b = fast.build_market_order(MarketOrderArgsV2(**args), opts, version=2)
            same += dataclasses.asdict(a) == dataclasses.asdict(b)
    finally:
        B.time.time_ns, X.ExchangeOrderBuilderV2.__init__ = real_ns, real_init
    check("the parsed-once key signs byte-identical orders to the library's Signer", same == n, f"{same}/{n}")
    check("...and has the same address", L.fast_signer(key, 137).address() == Signer(key, 137).address())


# ---------------------------------------------------------------- the exact price
def t_signed_price_is_the_cap_floored_to_the_tick_never_above():
    async def go(tick, caps):
        fake = await FakeClob().start()
        # canary share cap lifted here: at a 0.04 cap the $1 minimum is 25 shares
        sender, _ = await make(fake, tick=tick, limits=L.CanaryLimits(max_order_shares=1000))
        out = []
        for cap in caps:
            try:
                p = order(sender, cap=cap, amount=max(round(5 * cap + 0.05, 2), 1.0))
                body = json.loads(p.body)["order"]
                maker, taker = D(body["makerAmount"]), D(body["takerAmount"])
                out.append((cap, p.price, float(maker / taker)))
            except L.Refused as exc:
                out.append((cap, "refused", str(exc)[:40]))
        await fake.stop()
        return out
    caps = [0.04, 0.05, 0.57, 0.95, 0.96, 0.0449, 0.0451, 0.5699, 0.5701, 0.995, 0.005]
    bad = []
    for tick in ("0.01", "0.001", "0.0001"):
        for cap, price, implied in run(go(tick, caps)):
            if price == "refused":
                floor = (D(str(cap)) / D(tick)).to_integral_value(rounding="ROUND_FLOOR") * D(tick)
                if floor >= D(tick):
                    bad.append((tick, cap, "refused", implied))
                continue
            want = float((D(str(cap)) / D(tick)).to_integral_value(rounding="ROUND_FLOOR") * D(tick))
            if (price != want or price > cap or (D(str(price)) / D(tick)) % 1 != 0 or implied > price + 1e-4
                    or abs(implied - price) >= float(tick) / 2):      # the signed order's own price
                bad.append((tick, cap, price, implied))
    check("for 0.04, 0.05, 0.57, 0.95, 0.96 and +-half-ticks on 0.01, 0.001 and 0.0001 ticks: signed price = cap "
          "floored to the tick, on the tick, never above the cap (a sub-tick cap is refused)", not bad, str(bad[:6]))


# ---------------------------------------------------------------- outcomes
def t_no_match_is_never_retried_and_releases_exposure():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("json", 200, {"success": False, "errorMsg": "order couldn't be fully filled. FOK orders "
                                                                        "are fully filled or killed."}))
        sender, rows = await make(fake)
        fake.hits.clear()
        r = await sender.send(order(sender))
        await fake.stop()
        return r, dict(fake.hits), sender
    r, hits, sender = run(go())
    check("a FOK no-fill is NO_MATCH after exactly one POST - never chased", r["kind"] == L.NO_MATCH and
          hits == {("POST", "/order"): 1}, f"{r['kind']} {hits}")
    check("...the kill switch stays off and its exposure is released",
          not sender.kill.tripped and sender.limits.exposure_usd == 0, str(sender.kill.reasons))


def t_delayed_is_pending_and_journaled_with_its_order_id():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("json", 200, {"success": True, "status": "delayed", "orderID": "0xdelayed", "errorMsg": ""}))
        sender, rows = await make(fake)
        r = await sender.send(order(sender))
        await fake.stop()
        return r, rows
    r, rows = run(go())
    check("a 'delayed' acknowledgement is PENDING (taken, fill not yet known), journaled with its order id",
          r["kind"] == L.PENDING and rows and rows[-1]["order_id"] == "0xdelayed", str(r))


def t_ambiguous_states_halt_and_block_duplicates():
    async def go(action):
        fake = await FakeClob().start()
        fake.script.append(action)
        sender, rows = await make(fake, timeout_s=0.3)
        r = await sender.send(order(sender))
        after = []
        for attempt in ("kill on", "kill cleared"):
            try:
                order(sender)
                after.append("prepared")
            except (L.Halted, L.Refused) as exc:
                after.append(type(exc).__name__)
            sender.kill.reasons.clear()
        await fake.stop()
        return r, after
    for name, action in (("timeout after the request was sent", ("hang", 2.0)),
                         ("connection reset after the request was sent", ("reset",)),
                         ("HTTP 500", ("json", 500, {"error": "internal"}))):
        r, after = run(go(action))
        check(f"{name}: AMBIGUOUS, new orders halted, and the same token stays blocked even after the "
              f"kill switch is cleared", r["kind"] == L.AMBIGUOUS and after == ["Halted", "Refused"],
              f"{r['kind']} {after} {r['error']}")


def t_not_sent_is_not_ambiguous():
    async def go():
        fake = await FakeClob().start()
        sender, rows = await make(fake)
        await sender.http.aclose()
        sender.http = L.make_http()
        sender.url = "http://127.0.0.1:9/order"              # nothing listens: connect fails before any byte
        r = await sender.send(order(sender))
        await fake.stop()
        return r, sender
    r, sender = run(go())
    check("a connect failure before any byte left is NOT_SENT (known not placed), not AMBIGUOUS",
          r["kind"] == L.NOT_SENT and not pt._ambiguous_blocks(COND, UP), f"{r['kind']} {r['error']}")


def t_rejections_rate_limits_time_errors_and_error_streaks():
    async def go():
        fake = await FakeClob().start()
        fake.script.extend([("json", 429, {"error": "too many requests"}),
                            ("json", 400, {"error": "invalid timestamp"})])
        sender, rows = await make(fake)
        a = await sender.send(order(sender))
        time_kill_before = sender.kill.tripped
        b = await sender.send(order(sender))
        reasons = list(sender.kill.reasons)
        sender.kill.reasons.clear()
        sender.consecutive_errors = 0
        fake.script.extend([("json", 400, {"error": "invalid order payload"})] * 3)
        for _ in range(3):
            await sender.send(order(sender))
        await fake.stop()
        return a, b, time_kill_before, reasons, list(sender.kill.reasons)
    a, b, before, reasons, streak = run(go())
    check("HTTP 429 is REJECTED (not placed) without halting on its own", a["kind"] == L.REJECTED and not before,
          f"{a['kind']} {before}")
    check("a venue time/nonce rejection trips the kill switch", b["kind"] == L.REJECTED and
          any("time" in x for x in reasons), str(reasons))
    check("three consecutive errors trip the kill switch", any("consecutive" in x for x in streak), str(streak))


class FakeMono:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def t_venue_guard_state_machine():
    rows = []
    mono = FakeMono()
    kill = L.KillSwitch(None)
    g = L.VenueGuard(kill, slow_ms=1500, extreme_ms=5000, strikes_to_long_pause=3, strike_window_s=1800,
                     healthy_checks=3, max_pause_s=300, long_pause_s=300, journal=rows.append, mono=mono)
    check("normal replies leave it HEALTHY (no transition)", g.on_reply(265) == "HEALTHY" and not rows, str(rows))
    g.on_reply(1650)
    refused = None
    try:
        g.check()
    except L.Refused as exc:
        refused = str(exc)
    check("one 1650ms reply: HEALTHY -> DEGRADED -> PAUSED, new orders refused with the reason, NOT the kill switch",
          g.state == "PAUSED" and refused and "latency guard PAUSED" in refused and not kill.tripped
          and [(r["from"], r["to"]) for r in rows] == [("HEALTHY", "DEGRADED"), ("DEGRADED", "PAUSED")], str(rows))
    g.on_probe(True, "ok")
    g.on_probe(False, "channel not ready")
    g.on_probe(True, "ok")
    g.on_probe(True, "ok")
    check("a failed check resets the healthy streak (ok, fail, ok, ok -> still PAUSED)", g.state == "PAUSED")
    g.on_probe(True, "ok")
    ok = True
    try:
        g.check()
    except L.Refused:
        ok = False
    check("3 healthy checks in a row: PAUSED -> RECOVERED -> HEALTHY, orders allowed again",
          g.state == "HEALTHY" and ok and [(r["from"], r["to"]) for r in rows][-2:]
          == [("PAUSED", "RECOVERED"), ("RECOVERED", "HEALTHY")] and not kill.tripped, str(rows[-2:]))
    mono.t += 1900                                  # the first strike leaves the 30-minute window
    g.on_reply(1600)
    for _ in range(3):
        g.on_probe(True, "ok")
    g.on_reply(1600)
    check("strikes outside the window do not add up (2 in 30 min -> paused again, not halted)",
          g.state == "PAUSED" and not kill.tripped, g.status())
    for _ in range(3):
        g.on_probe(True, "ok")
    g.on_reply(1700)
    check("3 slow replies within 30 min -> a 5-minute PAUSE, NOT the kill switch (2026-10-15)",
          g.state == "PAUSED" and not kill.tripped and "paused 5 min" in rows[-1]["detail"], f"{g.status()} {rows[-1]}")
    for _ in range(5):
        g.on_probe(True, "ok")
    check("...healthy checks during the 5 minutes do not resume orders", g.state == "PAUSED", g.status())
    mono.t += 301
    for _ in range(3):
        g.on_probe(True, "ok")
    check("...after 5 minutes, 3 healthy checks resume orders", g.state == "HEALTHY" and not kill.tripped, g.status())
    g.on_reply(1650)
    check("...a 4th slow reply while 3 are in the window: another 5-minute pause", g.state == "PAUSED"
          and "paused 5 min" in g.reason and not kill.tripped, g.status())
    mono.t += 301 + 300
    g.on_probe(False, "order connection: ConnectError")
    check("...a long pause still not healthy 5 min after its wait -> HARD_HALTED",
          g.state == "HARD_HALTED" and kill.tripped and any("latency guard" in r for r in kill.reasons), g.status())
    k2 = L.KillSwitch(None)
    g2 = L.VenueGuard(k2, mono=FakeMono())
    g2.on_reply(5200)
    check("one reply over 5s -> HARD_HALTED at once", g2.state == "HARD_HALTED" and k2.tripped, g2.status())
    m3, k3 = FakeMono(), L.KillSwitch(None)
    g3 = L.VenueGuard(k3, max_pause_s=300, mono=m3)
    g3.on_reply(1600)
    m3.t += 301
    g3.on_probe(False, "order connection: ConnectError")
    check("a pause that never checks healthy within 5 min -> HARD_HALTED", g3.state == "HARD_HALTED" and k3.tripped,
          g3.status())


def t_one_slow_reply_then_healthy_venue_recovers_and_orders_continue():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("sleep", 0.15, ("json", 200, MATCHED_REPLY)))     # the slow one (scaled: limit 100ms)
        with tempfile.TemporaryDirectory() as tmp:
            ex, ledger, store, receipts, rows = await make_canary(fake, tmp, limits=L.CanaryLimits(
                max_orders=100, max_fills=100, max_exposure_usd=1e6))
            ex.sender.guard.slow_ms = 100.0
            ex.hub = types.SimpleNamespace(condition_id=COND, up_token=UP, down_token=DN)
            journal = []
            ex.sender.guard.journal = journal.append
            b = live_book()
            ex.hub.book = b
            r1 = await ex.send(ex.prepare(live_plan(b.view(UP))))
            refused = None
            try:
                ex.prepare(live_plan(b.view(UP)))
            except L.Refused as exc:
                refused = str(exc)
            probes = [await ex.health_probe() for _ in range(3)]
            for okp, det in probes:
                ex.sender.guard.on_probe(okp, det)
            reset_ambiguity()
            r2 = await ex.send(ex.prepare(live_plan(b.view(UP))))
            out = (r1.get("slow_reply"), r1.get("guard"), refused, [p[0] for p in probes], ex.sender.guard.state,
                   r2["kind"], ex.kill.tripped, [(j["from"], j["to"]) for j in journal], fake.hits[("POST", "/order")])
            await ex.sender.http.aclose()
        await fake.stop()
        return out
    slow, state1, refused, probes, state, kind2, tripped, path, posts = run(go())
    check("slow reply flagged and PAUSED (not halted); the next order refused with the guard's reason",
          slow is True and state1 == "PAUSED" and refused and "latency guard PAUSED" in refused, f"{state1} {refused}")
    check("recovery checks (order connection, clock, rules, collateral) pass 3x -> HEALTHY; the next order is SENT",
          probes == [True, True, True] and state == "HEALTHY" and kind2 == "MATCHED" and posts == 2 and not tripped,
          f"{probes} {state} {kind2} posts={posts}")
    check("journalled HEALTHY -> DEGRADED -> PAUSED -> RECOVERED -> HEALTHY",
          path == [("HEALTHY", "DEGRADED"), ("DEGRADED", "PAUSED"), ("PAUSED", "RECOVERED"), ("RECOVERED", "HEALTHY")],
          str(path))


def t_repeated_slow_replies_pause_five_minutes_never_halt():
    async def go():
        fake = await FakeClob().start()
        with tempfile.TemporaryDirectory() as tmp:
            ex, *_ = await make_canary(fake, tmp, limits=L.CanaryLimits(max_orders=100, max_fills=100,
                                                                        max_exposure_usd=1e6,
                                                                        max_daily_loss_usd=1e6))
            ex.sender.guard.slow_ms = 100.0
            ex.hub = types.SimpleNamespace(condition_id=COND, up_token=UP, down_token=DN)
            b = live_book()
            ex.hub.book = b
            states = []
            for _ in range(3):
                fake.script.append(("sleep", 0.15, ("json", 200, MATCHED_REPLY)))
                reset_ambiguity()
                r = await ex.send(ex.prepare(live_plan(b.view(UP))))
                states.append(r.get("guard"))
                if ex.sender.guard.state == "PAUSED" and len(states) < 3:   # recover between the strikes
                    for _ in range(3):
                        ex.sender.guard.on_probe(*(await ex.health_probe()))
            refused = None
            try:
                ex.prepare(live_plan(b.view(UP)))
            except (L.Refused, L.Halted) as exc:
                refused = (type(exc).__name__, str(exc))
            tripped = ex.kill.tripped
            await ex.sender.http.aclose()
        await fake.stop()
        return states, refused, tripped
    states, refused, tripped = run(go())
    check("slow, recover, slow, recover, slow (3 strikes in 30 min) -> a 5-minute PAUSE: orders refused, never Halted",
          states == ["PAUSED", "PAUSED", "PAUSED"] and refused and refused[0] == "Refused"
          and "latency guard PAUSED" in refused[1] and not tripped, f"{states} {refused}")


def t_disconnected_venue_never_resumes_unsafely():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("sleep", 0.15, ("json", 200, MATCHED_REPLY)))
        with tempfile.TemporaryDirectory() as tmp:
            ex, *_ = await make_canary(fake, tmp, limits=L.CanaryLimits(max_orders=100, max_fills=100,
                                                                        max_exposure_usd=1e6))
            ex.sender.guard.slow_ms = 100.0
            mono = FakeMono()                                    # the pause clock, independent of slow connects
            ex.sender.guard.mono = mono
            ex.hub = types.SimpleNamespace(condition_id=COND, up_token=UP, down_token=DN)
            b = live_book()
            ex.hub.book = b
            await ex.send(ex.prepare(live_plan(b.view(UP))))
            await fake.stop()                                    # the venue goes away
            await ex.sender.http.aclose()
            ex.sender.http = L.make_http(timeout_s=0.3)          # nothing listens: connects fail
            seen = []
            for _ in range(3):
                okp, det = await ex.health_probe()
                seen.append((okp, ex.sender.guard.on_probe(okp, det)))
            mono.t += ex.sender.guard.max_pause_s + 1            # the pause outlives max_pause_s
            okp, det = await ex.health_probe()
            final = ex.sender.guard.on_probe(okp, det)
            posts = fake.hits[("POST", "/order")]
            refused = None
            try:
                ex.prepare(live_plan(b.view(UP)))
            except (L.Refused, L.Halted) as exc:
                refused = type(exc).__name__
            await ex.sender.http.aclose()
        return seen, final, det, posts, refused
    seen, final, det, posts, refused = run(go())
    check("venue down: every recovery check fails, it stays PAUSED (no order sent after the pause)",
          all(not okp and st == "PAUSED" for okp, st in seen) and posts == 1, f"{seen} posts={posts}")
    check("...and HARD_HALTS once the pause outlives max_pause_s; orders refused (Halted)",
          final == "HARD_HALTED" and refused == "Halted" and "order connection" in det, f"{final} {refused} {det}")


def t_normal_venue_orders_flow_with_no_guard_transition():
    async def go():
        fake = await FakeClob().start()
        with tempfile.TemporaryDirectory() as tmp:
            ex, *_ = await make_canary(fake, tmp, limits=L.CanaryLimits(max_orders=100, max_fills=100,
                                                                        max_exposure_usd=1e6,
                                                                        max_daily_loss_usd=1e6))
            journal = []
            ex.sender.guard.journal = journal.append
            b = live_book()
            kinds = []
            for _ in range(6):
                reset_ambiguity()
                kinds.append((await ex.send(ex.prepare(live_plan(b.view(UP)))))["kind"])
            out = (kinds, ex.sender.guard.state, journal, ex.kill.tripped, fake.hits[("POST", "/order")])
            await ex.sender.http.aclose()
        await fake.stop()
        return out
    kinds, state, journal, tripped, posts = run(go())
    check("normal venue: 6 orders sent and answered, guard stays HEALTHY with no transition",
          kinds == ["MATCHED"] * 6 and state == "HEALTHY" and not journal and not tripped and posts == 6,
          f"{kinds} {state} {journal}")


def t_paused_guard_is_journalled_as_a_live_refusal_never_silent():
    import sniper_mode

    async def go():
        fake = await FakeClob().start()
        with tempfile.TemporaryDirectory() as tmp:
            ex, *_ = await make_canary(fake, tmp)
            b = live_book()
            hub = types.SimpleNamespace(up_token=UP, down_token=DN, condition_id=COND, window_end=round_end(), book=b)
            path = os.path.join(tmp, "j.jsonl")
            sn = sniper_mode.Sniper(hub, ex.shim, journal_path=path, settings=sniper_mode.Settings(), trade_rows=[],
                                    own_rounds=True, live=ex)
            sn.s.shadow_ms = ()
            ex.shim.trade_window_seconds = 600.0
            ex.sender.guard.on_reply(ex.sender.guard.slow_ms + 100)      # paused
            await sn._fire("UP", 1.5, time.monotonic() * 1000.0)
            rows = [json.loads(x) for x in open(path, encoding="utf-8")]
            await ex.sender.http.aclose()
        await fake.stop()
        return rows, fake.hits[("POST", "/order")]
    rows, posts = run(go())
    skip = [r for r in rows if r.get("result") == "skipped"]
    guard = [r for r in rows if r.get("result") == "guard"]
    check("while PAUSED a signal is journalled as 'live refused' with the guard's reason; nothing POSTed",
          skip and skip[0]["reason"] == "live refused" and "latency guard PAUSED" in skip[0].get("detail", "")
          and posts == 0, str(skip))
    check("the transitions reach the same journal", [(g["from"], g["to"]) for g in guard]
          == [("HEALTHY", "DEGRADED"), ("DEGRADED", "PAUSED")], str(guard))


def t_live_check_sorts_refusals_and_halts_by_cause():
    import live_check as LC
    cases = {"latency guard PAUSED for 3s (reply 1650ms > 1500ms (strike 1/3)); 0/3 healthy checks": "latency pause",
             "collateral $1.65 is below the order $5.07 incl. fee": "insufficient collateral",
             "collateral balance is stale": "stale balance",
             "the private fill channel is not subscribed to this market yet": "channel/rules not ready",
             "market rules not loaded for this round yet": "channel/rules not ready",
             "pre-send check: FOK no-fill: only $3.10 available at or below 0.52": "other",
             "latency guard: 3 replies slower than 1500ms within 30 min (last 1700ms)": "latency hard halt",
             "send -> response 1670ms exceeded 1500ms": "latency hard halt",
             "order 0xabc outcome unknown (ReadTimeout)": "unknown order outcome"}
    got = {k: LC.cause(k) for k in cases}
    check("live_check sorts every refusal / halt detail into its cause", got == cases,
          str({k: v for k, v in got.items() if cases[k] != v}))


def t_rejections_that_mean_the_bot_is_wrong_stop_new_orders():
    async def go(status, err):
        fake = await FakeClob().start()
        fake.script.append(("json", status, {"error": err}))
        sender, rows = await make(fake)
        r = await sender.send(order(sender))
        await fake.stop()
        return r["kind"], sender.kill.tripped, COND in sender.rules._rules if hasattr(sender.rules, "_rules") else None
    out = {e: run(go(s, e)) for s, e in ((400, "not enough balance / allowance"), (400, "order_version_mismatch"),
                                          (400, "duplicated order"), (401, "Unauthorized/Invalid api key"),
                                          (400, "invalid signature"), (400, "market is closed"))}
    bad = {e: v for e, v in out.items() if v[0] != L.REJECTED or v[1] != (e != "market is closed")}
    check("balance, version mismatch, duplicate, credentials and signature rejections halt; a closed market "
          "is only that order's rejection", not bad, str(bad))


def t_a_tick_or_minimum_rejection_drops_the_rounds_rules():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("json", 400, {"error": "invalid price, breaks minimum tick size rule: 0.001"}))
        sender, rows = await make(fake)
        r = await sender.send(order(sender))
        try:
            order(sender)
            nxt = "prepared"
        except L.Refused:
            nxt = "Refused"
        await fake.stop()
        return r, nxt
    r, nxt = run(go())
    check("a tick-size rejection invalidates the prefetched rules: the next order waits for a refetch",
          r.get("rules_invalidated") and nxt == "Refused", f"{r} {nxt}")


def t_a_send_cancelled_mid_flight_is_journaled_ambiguous():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("hang", 2.0))
        sender, rows = await make(fake)
        task = asyncio.ensure_future(sender.send(order(sender)))
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
            cancelled = False
        except asyncio.CancelledError:
            cancelled = True
        await fake.stop()
        return cancelled, rows, sender
    cancelled, rows, sender = run(go())
    check("cancelling a send after its bytes left still journals it AMBIGUOUS, halts and blocks the token",
          cancelled and rows and rows[-1]["kind"] == L.AMBIGUOUS and sender.kill.tripped
          and pt._ambiguous_blocks(COND, UP), f"{cancelled} {rows[-1:]}")


# ---------------------------------------------------------------- canary limits, kill switch, rules
def t_canary_limits_and_kill_switch_file():
    async def go():
        fake = await FakeClob().start()
        with tempfile.TemporaryDirectory() as d:
            stop = os.path.join(d, "STOP")
            sender, rows = await make(fake, limits=L.CanaryLimits(max_orders=2, max_order_shares=10,
                                                                   max_exposure_usd=100), stop_file=stop)
            out = []
            try:
                order(sender, amount=10.0, cap=0.5)              # 20 shares
            except L.Refused as exc:
                out.append(("shares", "Refused", str(exc)[:30]))
            for _ in range(2):
                await sender.send(order(sender))
            try:
                order(sender)
                out.append(("cap", "prepared"))
            except L.Halted:
                out.append(("cap", "Halted"))
            sender.kill.reasons.clear()
            sender.limits.orders = 0
            open(stop, "w").close()
            try:
                order(sender)
                out.append(("file", "prepared"))
            except L.Halted:
                out.append(("file", "Halted"))
        await fake.stop()
        return out
    out = run(go())
    check("an order above the canary share cap is refused (never resized)", out[0][:2] == ("shares", "Refused"),
          str(out))
    check("the canary order cap stops new orders", ("cap", "Halted") in out, str(out))
    check("a STOP file stops new orders", ("file", "Halted") in out, str(out))


def t_rules_must_be_prefetched_and_mapping_is_checked():
    async def go():
        fake = await FakeClob().start()
        sender, rows = await make(fake)
        out = []
        sender.rules.invalidate(COND)
        try:
            order(sender)
        except L.Refused as exc:
            out.append("Refused" if "prefetched" in str(exc) else str(exc))
        await sender.rules.prefetch(COND, UP, DN)
        try:
            sender.prepare(side="UP", token=UP, up=DN, down=UP, condition=COND, end=time.time() + 60,
                           amount=5.2, cap=0.54)
        except L.Halted:
            out.append("Halted")
        await fake.stop()
        return out
    out = run(go())
    check("no prefetched rules -> refused (never fetched on the hot path); a token mapping that changed -> halted",
          out == ["Refused", "Halted"], str(out))


# ---------------------------------------------------------------- the live canary (LiveExecution + Sniper)
def paper_rules():
    from paper_trade import MarketRules
    return MarketRules(condition_id=COND, fee_rate=D("0.07"), fee_exponent=1, min_order_size=D("5"),
                       tick_size=D("0.01"), up_token_id=UP, down_token_id=DN, taker_delay_ms=150.0,
                       taker_delay_flagged=True)


def live_book(asks=((0.50, 30),), bids=((0.49, 50),)):
    from feeds.book import BookState
    b = BookState(history_s=30)
    b.connected = True
    b.set_active([UP, DN])
    now_ms = int(time.time() * 1000)
    b.apply_snapshot(UP, list(bids), list(asks), ts_ms=now_ms, require_exchange_ts=False)
    b.apply_snapshot(DN, [(0.49, 50)], [(0.51, 30)], ts_ms=now_ms, require_exchange_ts=False)
    return b


def round_end(min_left=20.0):
    """A real round boundary (multiple of 300s) with at least min_left seconds to go."""
    end = (int(time.time()) // 300 + 1) * 300
    return float(end + 300 if end - time.time() < min_left else end)


async def make_canary(fake, tmp, *, limits=None, balance=100.0, user_ready=None, stop_file=None,
                      load_rules=True):
    from accounting import Ledger
    from feeds.poly_user import FillStore
    sender, rows = await make(fake, limits=limits, stop_file=stop_file)
    ledger = Ledger(path=os.path.join(tmp, "ledger.json"))
    receipts = []

    def sink(receipt):                       # what run.py's journal_live_order does
        receipts.append(receipt)
        ledger.authorize_order(receipt["order_id"], receipt, venue_min_shares=5.0, price_cap=0.95)
        return ledger.save()
    shim = L.LiveBrokerShim(lambda cid: paper_rules(), min_buy_price=0.05, max_buy_price=0.95, max_spread=0.25,
                            trade_window_seconds=300.0, min_seconds_to_expiry=10.0)
    if load_rules:
        shim._rules(COND)
    store = FillStore()
    ex = L.LiveExecution(sender=sender, shim=shim, ledger=ledger, receipt_sink=sink,
                         balance_fetch=lambda: {"balance": balance, "allowance": balance},
                         clock_url=fake.url + "/time", user_ready=user_ready, fills=store, assumed_delay_s=1.0)
    await ex._read_balance()
    return ex, ledger, store, receipts, rows


def live_plan(view, *, cap=0.52, amount=5.0, end=None, side="UP"):
    token = UP if side == "UP" else DN
    end = round_end() if end is None else end
    return {"token": token, "cond": COND, "end": end, "cap": cap, "view": view,
            "send": (side, amount, UP, DN, COND, end, cap, token, "0.01", 191.8)}


AMOUNTS_REPLY = dict(MATCHED_REPLY, makingAmount="5000000", takingAmount="10000000")


def t_canary_sizes_as_paper_sends_one_order_and_authorizes_it():
    async def go():
        fake = await FakeClob().start()
        fake.script.append(("json", 200, AMOUNTS_REPLY))
        with tempfile.TemporaryDirectory() as tmp:
            ex, ledger, store, receipts, rows = await make_canary(fake, tmp)
            b = live_book()
            p = ex.prepare(live_plan(b.view(UP)))
            reserved = ex.reserved
            res = await ex.send(p)
            out = {"amount": p.amount, "price": p.price, "reserved_during": round(reserved, 4),
                   "kind": res["kind"], "fill": res.get("fill"), "posts": fake.hits[("POST", "/order")],
                   "receipt": receipts and {k: receipts[0][k] for k in ("order_id", "requested_notional",
                                                                         "token_id", "window_end", "validation")},
                   "authorized": "0xorder1" in ledger.authorized_orders, "awaiting": list(ex.awaiting),
                   "reserved_after": ex.reserved, "balance": round(ex.balance, 4),
                   "no_connection_header": "connection" not in {k.lower() for k in p.headers},
                   "est": p.estimate}
        await ex.sender.http.aclose()
        await fake.stop()
        return out
    o = run(go())
    check("canary: $5 at cap 0.52 signed as sized (paper's size_to_venue_minimum / estimate_fok)",
          o["amount"] == 5.0 and abs(o["price"] - 0.52) < 1e-12, str(o))
    check("canary: the order's cost + fee is reserved while it is in flight, released after",
          o["reserved_during"] > 5.0 and o["reserved_after"] == 0.0, str(o))
    check("canary: exactly one POST, MATCHED, fill read from the reply's base-unit amounts",
          o["posts"] == 1 and o["kind"] == "MATCHED" and o["fill"]
          and abs(o["fill"]["shares"] - 10) < 1e-9 and abs(o["fill"]["price"] - 0.5) < 1e-9, str(o))
    check("canary: the accepted order is authorized in the live ledger with its risk metadata, then awaited",
          o["authorized"] and o["awaiting"] == ["0xorder1"] and o["receipt"]["requested_notional"] == 5.0
          and o["receipt"]["token_id"] == UP and o["receipt"]["window_end"] % 300 == 0
          and o["receipt"]["validation"] == "MATCHED_WITH_TRADE_EVIDENCE", str(o))
    check("canary: spend comes off the local balance until the next read", o["balance"] < 95.0, str(o))
    check("canary: no Connection header (forbidden on HTTP/2)", o["no_connection_header"], str(o))
    check("canary: the decision-book estimate rides along for the journal",
          abs(o["est"]["shares"] - 10) < 1e-9 and abs(o["est"]["price"] - 0.5) < 1e-9 and o["est"]["fee"] > 0,
          str(o))


def t_canary_refuses_before_any_send():
    async def go():
        fake = await FakeClob().start()
        out = {}
        with tempfile.TemporaryDirectory() as tmp:
            b = live_book()

            async def why(**kw):
                plan_kw = kw.pop("plan", {})
                ex, *_ = await make_canary(fake, tmp, **kw)
                try:
                    ex.prepare(live_plan(b.view(UP), **plan_kw))
                    return "SENT-READY"
                except (L.Refused, L.Halted) as exc:
                    return f"{type(exc).__name__}: {exc}"
                finally:
                    await ex.sender.http.aclose()
            out["no rules"] = await why(load_rules=False)
            out["user channel"] = await why(user_ready=lambda cond: False)
            out["balance"] = await why(balance=3.0)
            out["delay window"] = await why(plan={"end": time.time() + 0.5})
            ex, *_ = await make_canary(fake, tmp)
            ex.balance_at -= 1000
            try:
                ex.prepare(live_plan(b.view(UP)))
                out["stale balance"] = "SENT-READY"
            except L.Refused as exc:
                out["stale balance"] = str(exc)
            await ex.sender.http.aclose()
            thin = live_book(asks=((0.50, 3), (0.60, 50)))
            ex, *_ = await make_canary(fake, tmp)
            try:
                ex.prepare(live_plan(thin.view(UP)))
                out["thin book"] = "SENT-READY"
            except L.Refused as exc:
                out["thin book"] = str(exc)
            await ex.sender.http.aclose()
        out["posts"] = fake.hits[("POST", "/order")]
        await fake.stop()
        return out
    o = run(go())
    check("canary refuses: rules not loaded for the round (never fetched on the order path)",
          "market rules not loaded" in o["no rules"], o["no rules"])
    check("canary refuses: the private fill channel does not cover the market",
          "fill channel" in o["user channel"], o["user channel"])
    check("canary refuses: collateral below cost + fee", "collateral" in o["balance"], o["balance"])
    check("canary refuses: inside the venue's assumed matching-delay window", "delay" in o["delay window"],
          o["delay window"])
    check("canary refuses: collateral reading too old", "stale" in o["stale balance"], o["stale balance"])
    check("canary refuses: the decision book cannot fill the FOK within the cap (as paper)",
          "pre-send check" in o["thin book"], o["thin book"])
    check("canary: nothing was POSTed by any refusal", o["posts"] == 0, str(o["posts"]))


def t_canary_ambiguous_halts_and_delayed_is_followed():
    async def go():
        fake = await FakeClob().start()
        out = {}
        with tempfile.TemporaryDirectory() as tmp:
            b = live_book()
            fake.script.append(("json", 200, {"success": True, "status": "delayed", "orderID": "0xdelayed",
                                              "errorMsg": ""}))
            ex, ledger, *_ = await make_canary(fake, tmp)
            res = await ex.send(ex.prepare(live_plan(b.view(UP))))
            out["delayed"] = (res["kind"], "0xdelayed" in ledger.authorized_orders, ex.kill.tripped,
                              pt._ambiguous_blocks(COND, UP))
            fake.script.append(("reset",))
            res = await ex.send(ex.prepare(live_plan(b.view(UP))))
            out["reset"] = (res["kind"], ex.kill.tripped)
            try:
                ex.prepare(live_plan(b.view(UP)))
                out["after"] = "SENT-READY"
            except L.Halted:
                out["after"] = "Halted"
            await ex.sender.http.aclose()
        await fake.stop()
        return out
    o = run(go())
    check("canary: a delayed placement is authorized and followed, not a halt, token not blocked",
          o["delayed"] == ("PENDING", True, False, False), str(o))
    check("canary: a connection reset after the send is AMBIGUOUS and halts new orders",
          o["reset"] == ("AMBIGUOUS", True) and o["after"] == "Halted", str(o))


def t_canary_reconciler_grace_baseline_and_confirmation():
    async def go():
        fake = await FakeClob().start()
        out = {}
        with tempfile.TemporaryDirectory() as tmp:
            b = live_book()
            ex, ledger, store, receipts, rows = await make_canary(fake, tmp)
            # a fill the wallet already had at start: the operator's, not a halt
            store.record_trade("old-trade", order_id="0xsomeone", asset_id=UP, market=COND, side="BUY",
                               price=0.5, size=4, status="CONFIRMED")
            ledger.ingest_fill_store(store)
            ex.baseline_unexplained = ex._unexplained_now()
            # our fill arrives a moment before its order is authorized (the race)
            store.record_trade("trade-1", order_id="0xorder1", asset_id=UP, market=COND, side="BUY",
                               price=0.5, size=10, status="CONFIRMED")
            ledger.ingest_fill_store(store)
            ex.reconcile_once()
            out["race_before"] = ex.kill.tripped
            res = await ex.send(ex.prepare(live_plan(b.view(UP))))    # authorizes 0xorder1
            ledger.ingest_fill_store(store)                           # now booked
            for tid in list(ex._unexplained_since):
                ex._unexplained_since[tid] -= 1000                    # well past the grace
            ex.reconcile_once()
            out["race_after"] = (res["kind"], ex.kill.tripped, list(ex.awaiting), ex.n["confirmed in ledger"])
            # a fill nobody explains, past the grace
            store.record_trade("t-x", order_id="0xnotours", asset_id=UP, market=COND, side="BUY",
                               price=0.5, size=3, status="CONFIRMED")
            ledger.ingest_fill_store(store)
            ex.reconcile_once()
            out["unexplained_in_grace"] = ex.kill.tripped
            ex._unexplained_since["t-x"] -= 1000
            ex.reconcile_once()
            out["unexplained_after_grace"] = ex.kill.tripped
            ex.kill.reasons.clear()
            # an accepted order that never shows up confirmed
            ex.awaiting["0xlost"] = {"wall": time.time() - 1000, "condition": COND, "token": UP,
                                     "amount": 5.0, "kind": "MATCHED", "venue_matched": True}
            ex.reconcile_once()
            out["lost"] = any("0xlost" in r for r in ex.kill.reasons)
            await ex.sender.http.aclose()
        await fake.stop()
        return out
    o = run(go())
    check("reconciler: a fill booked once its order is authorized is not unexplained (no false halt)",
          o["race_before"] is False and o["race_after"] == ("MATCHED", False, [], 1), str(o))
    check("reconciler: an unexplained fill halts only after the grace; a pre-existing one never",
          o["unexplained_in_grace"] is False and o["unexplained_after_grace"] is True, str(o))
    check("reconciler: an accepted order not confirmed in the ledger by the deadline halts", o["lost"], str(o))


def t_canary_start_loads_rules_and_stop_closes():
    async def go():
        fake = await FakeClob().start()
        events = []
        with tempfile.TemporaryDirectory() as tmp:
            ex, ledger, *_ = await make_canary(fake, tmp, load_rules=False)
            ex.sender.rules.invalidate(COND)
            ex.on_event = lambda *a, **k: events.append(a)
            hub = types.SimpleNamespace(condition_id=COND, up_token=UP, down_token=DN)
            await ex.start(hub)
            for _ in range(40):
                if ex.rules_ready:
                    break
                await asyncio.sleep(0.05)
            ready = (bool(ex.rules_ready), ex.shim.cached_rules(COND) is not None)
            await ex.stop()
            closed = ex.sender.http.is_closed
        await fake.stop()
        return ready, closed, events
    ready, closed, events = run(go())
    check("canary start: arms, then loads the round's order + sizing rules off the order path",
          ready == (True, True) and any("armed" in str(e) for e in events), f"{ready} {events}")
    check("canary stop: background tasks cancelled and the order connection closed", closed, str(closed))


def t_sniper_live_end_to_end_and_halted_skips():
    import sniper_mode

    async def go():
        fake = await FakeClob().start()
        fake.script.append(("json", 200, AMOUNTS_REPLY))
        out = {}
        with tempfile.TemporaryDirectory() as tmp:
            ex, ledger, *_ = await make_canary(fake, tmp)
            b = live_book()
            hub = types.SimpleNamespace(up_token=UP, down_token=DN, condition_id=COND, window_end=round_end(),
                                        book=b)
            journal = os.path.join(tmp, "live_sniper.jsonl")
            sn = sniper_mode.Sniper(hub, ex.shim, journal_path=journal, settings=sniper_mode.Settings(),
                                    trade_rows=[], own_rounds=True, live=ex)
            sn.s.shadow_ms = ()
            ex.shim.trade_window_seconds = 600.0     # round_end() may pick the next round near a boundary
            await sn._fire("UP", 1.5, time.monotonic() * 1000.0)
            rows = [json.loads(x) for x in open(journal, encoding="utf-8")]
            filled = [r for r in rows if r.get("result") == "filled"]
            out["filled"] = filled and {k: filled[0].get(k) for k in ("live", "kind", "outcome", "order_id",
                                                                        "price", "shares", "price_source",
                                                                        "condition_id", "taker_delay")}
            out["send_rows"] = sum(1 for r in rows if r.get("result") == "live_send")
            out["posts"] = fake.hits[("POST", "/order")]
            out["round"] = dict(sn.rounds[hub.window_end])
            out["busy"] = sn.busy
            out["table"] = [r["result"] for r in sn.trade_rows]
            out["no_throwaway_builder"] = sn._builder is None
            out["lines"] = sn.lines()
            ex.kill.trip("test halt")
            sn.last_fire = -1e18
            await sn._fire("UP", 1.5, time.monotonic() * 1000.0)
            rows = [json.loads(x) for x in open(journal, encoding="utf-8")]
            out["halted_skip"] = [r.get("reason") for r in rows if r.get("result") == "skipped"]
            out["posts_after_halt"] = fake.hits[("POST", "/order")]
            out["orders"] = sn.n["orders"]
            out["summary_live"] = "live" in sn.summary()
            await ex.sender.http.aclose()
        await fake.stop()
        return out
    o = run(go())
    check("sniper live: one signal -> one real POST -> journal 'filled' (live, MATCHED, reply price)",
          o["posts"] == 1 and o["filled"] and o["filled"]["live"] is True and o["filled"]["kind"] == "MATCHED"
          and o["filled"]["outcome"] == "FILLED" and o["filled"]["order_id"] == "0xorder1"
          and abs(o["filled"]["price"] - 0.5) < 1e-9 and o["filled"]["price_source"] == "order reply", str(o))
    check("sniper live: the order row names its market and the taker-delay flag it was signed under",
          o["filled"] and o["filled"]["condition_id"] == COND and o["filled"]["taker_delay"] is True, str(o))
    check("sniper live: the send row is journalled, the round cap counted, busy cleared, table row added",
          o["send_rows"] == 1 and o["round"]["fills"] == 1 and abs(o["round"]["UP"] - 10) < 1e-9
          and o["busy"] is False and o["table"] == ["filled"], str(o))
    check("sniper live: no throwaway paper signer; the settings line says LIVE CANARY with its limits",
          o["no_throwaway_builder"] and "LIVE CANARY" in o["lines"][0] and "limits" in o["lines"][1], str(o))
    check("sniper live: a halted canary skips (journalled, not counted as an order) and sends nothing",
          o["halted_skip"] == ["live halted"] and o["posts_after_halt"] == 1 and o["orders"] == 1
          and o["summary_live"], str(o))


def t_build_live_execution_refuses_without_assumed_delay():
    import config
    saved, saved_client = config.ASSUMED_MATCH_DELAY_SECONDS, pt._get_client
    touched = []
    try:
        config.ASSUMED_MATCH_DELAY_SECONDS = 0.0
        pt._get_client = lambda: touched.append(1)
        try:
            L.build_live_execution(ledger=None, env={})
            msg = "built"
        except RuntimeError as exc:
            msg = str(exc)
    finally:
        config.ASSUMED_MATCH_DELAY_SECONDS, pt._get_client = saved, saved_client
    check("build: refuses without ASSUMED_MATCH_DELAY_SECONDS, before touching the client or the key",
          "ASSUMED_MATCH_DELAY_SECONDS" in msg and not touched, msg)


def t_canary_limits_from_env():
    lim = L.limits_from_env({"SNIPER_CANARY_MAX_SPEND_USD": "25", "SNIPER_CANARY_MAX_ORDERS": ""})
    check("limits: env overrides, unset/blank values take the small defaults",
          lim.max_exposure_usd == 25.0 and lim.max_orders == 40 and lim.max_daily_loss_usd == 20.0, str(lim))
    # These are the live safety caps: a bad value refuses the start. It used to fall
    # back to the default ("x" -> 40) - and "nan" switched the cap off entirely.
    for name, raw in (("SNIPER_CANARY_MAX_ORDERS", "x"), ("SNIPER_CANARY_MAX_SPEND_USD", "nan"),
                      ("SNIPER_CANARY_MAX_SHARES", "inf"), ("SNIPER_CANARY_MAX_LOSS_USD", "-1"),
                      ("SNIPER_CANARY_MAX_FILLS", "2.5")):
        try:
            L.limits_from_env({name: raw})
            refused = False
        except ValueError:
            refused = True
        check(f"limits: {name}={raw!r} refuses the start", refused)


# ---------------------------------------------------------------- fast_h2: the synchronous HTTP/2 write
class FakeH2Clob:
    """HTTP/2 (cleartext, prior knowledge) fake venue: /time and /order with a scripted reply queue."""

    def __init__(self, skew_s=0.0):
        self.skew, self.script = skew_s, collections.deque()
        self.hits = collections.Counter()
        self.orders = []
        self.conns = 0

    async def start(self):
        loop = asyncio.get_running_loop()
        self.server = await loop.create_server(lambda: _H2Venue(self), "127.0.0.1", 0)
        self.url = f"http://127.0.0.1:{self.server.sockets[0].getsockname()[1]}"
        return self

    async def stop(self):
        self.server.close()


class _H2Venue(asyncio.Protocol):
    def __init__(self, fake):
        import h2.config
        import h2.connection
        self.fake = fake
        self.c = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False,
                                                                             header_encoding="utf-8"))
        self.req = {}

    def connection_made(self, t):
        self.fake.conns += 1
        self.t = t
        self.c.initiate_connection()
        t.write(self.c.data_to_send())

    def data_received(self, data):
        import h2.events
        for ev in self.c.receive_data(data):
            if isinstance(ev, h2.events.RequestReceived):
                self.req[ev.stream_id] = {"h": dict(ev.headers), "names": [k for k, _ in ev.headers], "b": b""}
            elif isinstance(ev, h2.events.DataReceived):
                self.req[ev.stream_id]["b"] += ev.data
                self.c.acknowledge_received_data(ev.flow_controlled_length, ev.stream_id)
            elif isinstance(ev, h2.events.StreamEnded):
                self.handle(ev.stream_id)
        if not self.t.is_closing():
            self.t.write(self.c.data_to_send())

    def reply(self, sid, status, text):
        body = text.encode()
        self.c.send_headers(sid, [(":status", str(status)), ("content-type", "application/json"),
                                  ("content-length", str(len(body)))])
        self.c.send_data(sid, body, end_stream=True)
        if not self.t.is_closing():
            self.t.write(self.c.data_to_send())

    def handle(self, sid):
        import h2.errors
        r = self.req.pop(sid)
        h = r["h"]
        self.fake.hits[(h[":method"], h[":path"].split("?")[0])] += 1
        if h[":path"] == "/time":
            return self.reply(sid, 200, str(int(time.time() + self.fake.skew)))
        self.fake.orders.append((json.loads(r["b"]), h, r["b"].decode(), time.time() + self.fake.skew, r["names"]))
        action = self.fake.script.popleft() if self.fake.script else ("json", 200, MATCHED_REPLY)

        def act(a):
            if a[0] == "json":
                self.reply(sid, a[1], json.dumps(a[2]))
            elif a[0] == "refuse":
                self.c.reset_stream(sid, h2.errors.ErrorCodes.REFUSED_STREAM)
                self.t.write(self.c.data_to_send())
            elif a[0] == "reset":
                self.c.reset_stream(sid, h2.errors.ErrorCodes.INTERNAL_ERROR)
                self.t.write(self.c.data_to_send())
            elif a[0] == "goaway":                       # this stream was NOT processed
                self.c.close_connection(last_stream_id=max(0, sid - 2))
                self.t.write(self.c.data_to_send())
                self.t.close()
            elif a[0] == "drop":                         # processed or not: nobody can tell
                self.t.close()
            elif a[0] == "sleep":
                asyncio.get_running_loop().call_later(a[1], act, a[2])
        act(action)


async def make_fast(fake, *, limits=None, timeout_s=3.0):
    from fast_h2 import FastH2
    reset_ambiguity()
    signer = L.fast_signer("0x" + secrets.token_hex(32), 137)
    creds = ApiCreds(api_key=str(uuid.uuid4()), api_secret=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
                     api_passphrase=secrets.token_hex(16))
    http = FastH2(fake.url, timeout_s=timeout_s)
    clock = L.ServerClock()
    rules = L.RoundRules(lambda c, u, d: rules_fetch(c, u, d))
    rows = []
    sender = L.LeanSender(builder=OrderBuilder(signer), signer=signer, creds=creds, host=fake.url, http=http,
                          clock=clock, rules=rules, limits=limits or L.CanaryLimits(),
                          kill=L.KillSwitch(None), journal=rows.append, timeout_s=timeout_s)
    await clock.refresh(http, fake.url + "/time")              # opens the connection, off the hot path
    await rules.prefetch(COND, UP, DN)
    return sender, rows


def t_fast_h2_writes_in_the_callers_step_on_the_warm_connection():
    async def go():
        fake = await FakeH2Clob(skew_s=3.0).start()
        sender, rows = await make_fast(fake)
        p = order(sender, cap=0.57, amount=5.7)
        stream = sender.start(p)
        sync = {"stream": stream is not None, "orders_counted": sender.limits.orders,
                "stamped": [n for n, _t in (stream.events if stream else [])]}
        res = await sender.send(p, stream)
        await sender.http.aclose()
        await fake.stop()
        return sync, res, fake, sender, p
    sync, res, fake, sender, p = run(go())
    body, hdrs, raw, server_now, names = fake.orders[-1]
    check("fast path: start() writes the order inside the caller's step (stream returned, counted, stamped)",
          sync["stream"] and sync["orders_counted"] == 1
          and sync["stamped"] == ["http2.send_request_headers.started", "http2.send_request_body.complete"], str(sync))
    check("fast path: MATCHED over the SAME connection the clock refresh opened (no new connection)",
          res["kind"] == L.MATCHED and fake.conns == 1 and res["new_connection"] is False
          and fake.hits[("GET", "/time")] == 1 and fake.hits[("POST", "/order")] == 1, f"{res} conns={fake.conns}")
    want = build_hmac_signature(sender.creds.api_secret, hdrs["poly_timestamp"], "POST", "/order", raw)
    check("fast path: the L2 HMAC verifies over the exact bytes the venue received", hdrs.get("poly_signature") == want,
          f"{hdrs.get('poly_signature')} vs {want}")
    check("fast path: lowercase header names, no connection-specific headers, :authority and content-length right",
          all(n == n.lower() for n in names) and "connection" not in hdrs and "host" not in hdrs
          and hdrs[":authority"] == fake.url.split("//")[1] and int(hdrs["content-length"]) == len(raw.encode()),
          str(names))
    check("fast path: transport stamps reach the journal (wire start, body sent, reply headers)",
          all(k in res["ts_us"] for k in ("net_send_start", "net_send_done", "response")), str(res["ts_us"]))


def t_fast_h2_outcomes_refused_goaway_reset_drop_timeout():
    async def one(action, timeout_s=3.0):
        fake = await FakeH2Clob().start()
        sender, rows = await make_fast(fake, timeout_s=timeout_s)
        fake.script.append(action)
        p = order(sender)
        res = await sender.send(p, sender.start(p))
        out = (res["kind"], sender.kill.tripped, round(sender.limits.exposure_usd, 2))
        await sender.http.aclose()
        await fake.stop()
        return out
    got = {name: run(one(a, **kw)) for name, a, kw in (
        ("refused", ("refuse",), {}), ("goaway", ("goaway",), {}), ("reset", ("reset",), {}),
        ("drop", ("drop",), {}), ("timeout", ("sleep", 1.0, ("json", 200, MATCHED_REPLY)), {"timeout_s": 0.2}))}
    check("refused stream / GOAWAY below our stream: never processed -> NOT_SENT, exposure released, no halt",
          got["refused"] == ("NOT_SENT", False, 0.0) and got["goaway"] == ("NOT_SENT", False, 0.0), str(got))
    check("reset / dropped connection / timeout after the write: AMBIGUOUS, halts, exposure kept",
          all(got[k][0] == "AMBIGUOUS" and got[k][1] and got[k][2] > 0 for k in ("reset", "drop", "timeout")),
          str(got))


def t_fast_h2_without_a_live_connection_falls_back_and_flags_it():
    async def go():
        fake = await FakeH2Clob().start()
        sender, rows = await make_fast(fake)
        fake.script.append(("drop",))
        p = order(sender)
        await sender.send(p, sender.start(p))                 # the venue drops the connection
        await asyncio.sleep(0.05)
        reset_ambiguity()
        sender.kill.reasons.clear()
        p2 = order(sender)
        none = sender.start(p2) is None
        res = await sender.send(p2)                           # the async way: reconnects first
        out = (none, res["kind"], res["new_connection"], fake.conns, sender.http.connected)
        await sender.http.aclose()
        await fake.stop()
        return out
    o = run(go())
    check("no live connection: start() writes nothing (None); send() reconnects, flags a NEW connection, matches",
          o == (True, "MATCHED", True, 2, True), str(o))


def t_fast_h2_keep_warm_and_clock_ride_the_order_connection():
    async def go():
        fake = await FakeH2Clob(skew_s=2.0).start()
        sender, rows = await make_fast(fake)
        seen = []
        task = asyncio.get_running_loop().create_task(
            L.keep_warm(sender.http, sender.clock, fake.url + "/time", every_s=0.05, on_event=seen.append))
        await asyncio.sleep(0.3)
        task.cancel()
        out = (len(seen) >= 3, all(r.get("ok") for r in seen), any(r.get("new_connection") for r in seen),
               fake.conns, round(sender.clock.offset or 0))
        await sender.http.aclose()
        await fake.stop()
        return out
    o = run(go())
    check("keep_warm refreshes the clock on the order connection: samples ok, no reconnect, offset ~ +2s",
          o[0] and o[1] and not o[2] and o[3] == 1 and o[4] in (2, 3), str(o))


def t_fast_order_builder_signs_byte_identically_to_the_library():
    import dataclasses
    import random
    from eth_account import Account
    from py_clob_client_v2.clob_types import CreateOrderOptions, MarketOrderArgsV2, OrderType
    from py_clob_client_v2.config import get_contract_config
    from py_clob_client_v2.order_builder.constants import BUY, SELL
    from py_clob_client_v2.order_utils import ExchangeOrderBuilderV2
    from py_clob_client_v2.order_utils.model.order_data_v2 import OrderV2
    import sniper_mode
    rnd = random.Random(20261004)
    mismatches, recovered_bad, fields_bad, n = [], 0, [], 0
    for i in range(120):
        key = "0x" + secrets.token_hex(32)
        signer = L.fast_signer(key, 137) if i % 2 == 0 else Signer(key, 137)     # both signer kinds
        sig_type = rnd.choice([0, 1, 2])
        funder = None if sig_type == 0 else "0x" + secrets.token_hex(20)
        lib = OrderBuilder(signer, sig_type, funder)
        fast = sniper_mode.FastOrderBuilder(OrderBuilder(signer, sig_type, funder))
        tick = rnd.choice(["0.01", "0.001", "0.0001"])
        price = round(rnd.uniform(0.05, 0.95), len(tick) - 2)
        args = MarketOrderArgsV2(token_id=str(rnd.randrange(10 ** 70, 10 ** 77)), amount=round(rnd.uniform(1, 50), 2),
                                 side=rnd.choice([BUY, SELL]), price=price, order_type=OrderType.FOK)
        opts = CreateOrderOptions(tick_size=tick, neg_risk=rnd.random() < 0.3)
        got = fast.build_market_order(args, opts, version=2)
        ref = lib.build_market_order(args, opts, version=2)
        # the library signing exactly the order the fast path built (same salt + timestamp)
        cc = get_contract_config(137)
        eb = ExchangeOrderBuilderV2(cc.neg_risk_exchange_v2 if opts.neg_risk else cc.exchange_v2, 137, signer)
        order = OrderV2(**{k: v for k, v in dataclasses.asdict(got).items() if k != "signature"})
        typed = eb.build_order_typed_data(order)
        want = eb.build_order_signature(typed)
        n += 1
        if got.signature != want:
            mismatches.append((i, got.signature[:20], want[:20]))
        digest = bytes.fromhex(eb.build_order_hash(typed)[2:])
        if Account._recover_hash(digest, signature=bytes.fromhex(got.signature[2:])).lower() != signer.address().lower():
            recovered_bad += 1
        for f in ("maker", "signer", "tokenId", "makerAmount", "takerAmount", "side", "signatureType", "metadata",
                  "builder", "expiration"):
            if getattr(got, f) != getattr(ref, f):
                fields_bad.append((i, f, getattr(got, f), getattr(ref, f)))
    check(f"FastOrderBuilder: {n} random orders (sides, ticks, neg-risk, signature types 0/1/2, both signer kinds) "
          "sign byte-identically to the library signing the same order", not mismatches, str(mismatches[:3]))
    check("FastOrderBuilder: every signature recovers to the signer over the library's own order hash",
          recovered_bad == 0, str(recovered_bad))
    check("FastOrderBuilder: every order field equals the library's build_market_order (bar salt/timestamp)",
          not fields_bad, str(fields_bad[:3]))


def t_fast_order_builder_hands_poly_1271_and_v1_to_the_library():
    from py_clob_client_v2.clob_types import CreateOrderOptions, MarketOrderArgsV2, OrderType
    from py_clob_client_v2.order_builder.constants import BUY
    import sniper_mode
    signer = L.fast_signer("0x" + secrets.token_hex(32), 137)
    calls = []

    class Spy(OrderBuilder):
        def build_market_order(self, *a, **k):
            calls.append(k.get("version"))
            return super().build_market_order(*a, **k)
    args = MarketOrderArgsV2(token_id=UP, amount=5.0, side=BUY, price=0.5, order_type=OrderType.FOK)
    opts = CreateOrderOptions(tick_size="0.01", neg_risk=False)
    sniper_mode.FastOrderBuilder(Spy(signer)).build_market_order(args, opts, version=2)
    try:
        sniper_mode.FastOrderBuilder(Spy(signer, 3, "0x" + secrets.token_hex(20))).build_market_order(args, opts,
                                                                                                    version=2)
    except Exception:  # noqa: BLE001 - whatever the library does with it, it was the library
        pass
    check("FastOrderBuilder: plain v2 EOA signs itself; POLY_1271 goes to the library unchanged",
          calls == [2], str(calls))


def t_lean_pre_send_view_gives_identical_checks():
    import random
    from feeds.book import BookView
    from paper_trade import PaperRejected, estimate_fok, size_to_venue_minimum, snapshot_from_book_view
    rules = paper_rules()
    rnd = random.Random(20261004)

    def run(view, amount, cap):
        try:
            b = snapshot_from_book_view(view, expected_token=UP)
            if not b.asks or b.best_bid is None:
                return ("refused", "one-sided")
            if b.asks[0][0] - b.best_bid > D("0.25"):
                return ("refused", "spread")
            sp = size_to_venue_minimum(amount, b, rules, cap)
            return ("ok", sp, estimate_fok(b, sp, cap, rules, min_price=0.05))
        except PaperRejected as exc:
            return ("refused", str(exc))
    diffs, n, trimmed = [], 0, 0
    for _ in range(1500):
        bid = rnd.randint(5, 90) / 100
        asks = sorted({round(bid + rnd.randint(1, 40) / 100, 2) for _ in range(rnd.randint(1, 60))})
        asks = tuple((p, float(rnd.choice([1, 3, 5, 10, 50, 200]))) for p in asks if p < 1)
        bids = tuple(sorted(((round(bid - i / 100, 2), 20.0) for i in range(rnd.randint(1, 60)) if bid - i / 100 > 0),
                            reverse=True))
        if not asks or not bids:
            continue
        v = BookView(UP, 1, "LIVE", bids=bids, asks=asks, updated_mono=time.monotonic(),
                     exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)
        cap = min(round(asks[0][0] + rnd.choice([0.0, 0.01, 0.02, 0.03]), 2), 0.95)
        lean = L.lean_view(v, cap)
        trimmed += len(lean.asks) < len(v.asks) or len(lean.bids) < len(v.bids)
        n += 1
        a, b = run(v, max(10 * asks[0][0], 1.0), cap), run(lean, max(10 * asks[0][0], 1.0), cap)
        if a != b:
            diffs.append((a, b))
    check(f"lean pre-send view: {n} random books ({trimmed} actually trimmed), sizing / FOK / refusals identical",
          not diffs and trimmed > n // 2, str(diffs[:2]))


def t_order_type_fak_is_posted_as_fak_default_stays_fok():
    async def go():
        fake = await FakeClob().start()
        sender, rows = await make(fake)
        p1 = order(sender)
        await sender.send(p1)
        reset_ambiguity()
        p2 = sender.prepare(side="UP", token=UP, up=UP, down=DN, condition=COND, end=time.time() + 120,
                            amount=5.2, cap=0.54, order_type="FAK")
        await sender.send(p2)
        bad = None
        try:
            sender.prepare(side="UP", token=UP, up=UP, down=DN, condition=COND, end=time.time() + 120,
                           amount=5.2, cap=0.54, order_type="GTC")
        except L.Refused as exc:
            bad = str(exc)
        await fake.stop()
        return [o[0]["orderType"] for o in fake.orders], [r.get("order_type") for r in rows], bad
    types_, journal, bad = run(go())
    check("default order posted as FOK, order_type='FAK' posted as FAK (same signed market order)",
          types_ == ["FOK", "FAK"] and journal == ["FOK", "FAK"], f"{types_} {journal}")
    check("an unknown order type is refused before signing", bad and "unknown order type" in bad, str(bad))


def main() -> int:
    for name, test in sorted(globals().items()):
        if not name.startswith("t_") or not callable(test):
            continue
        try:
            test()
        except Exception as exc:
            global FAIL
            FAIL += 1
            FAILURES.append(f"{name} raised {type(exc).__name__}: {exc}")
            print(f"  ERROR {name}: {type(exc).__name__}: {exc}")
    print(f"\n{PASS} passed, {FAIL} failed")
    for failure in FAILURES[:30]:
        print("  -", failure)
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
