#!/usr/bin/env python3
"""Deep audit of the NEW execution system on the LIVE code path.

    python tests_audit_live.py          (prints an AUDIT trace of every check)

Inspects and tests only - no strategy change. The live tests run the real
chain end to end:

    trigger-feed message (Coinbase BTC-USD ticker, the frozen spec's feed) ->
    Sniper.on_coinbase -> Trigger (250ms, 0.5bp)
    -> side -> per-side cooldown -> Polymarket book (BookState) -> reference
    ask -> IOC sizing -> LiveExecution.prepare -> LeanSender (real signing,
    real POST body) -> a fake Polymarket CLOB that records the exact bytes ->
    response classification -> private trades (FillStore) -> fill parsing
    -> carry -> live ledger position.

The six findings of the first audit (exact 0.50bp, the stake signed a cent
short, no fill-confirmed latency, no trigger window, and the two global gates
that held up the opposite side) are fixed; their tests are named FIXED_*.
"""
from __future__ import annotations

import ast
import asyncio
import json
import os
import pathlib
import sys
import tempfile
import time
import types
import unittest
from decimal import Decimal as D
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import bench_execution as B  # noqa: E402  (tick_raw / feed_tick: the trigger feed's own message)
import sniper_live as L  # noqa: E402
import sniper_mode as S  # noqa: E402
import tests_sniper_live as T  # noqa: E402  (FakeClob, make_canary, live_book, round_end)

AUDIT_ENV = {"SNIPER_ONLY": "1", "SNIPER_K_BP": "0.5", "SNIPER_WINDOW_MS": "250", "SNIPER_ORDER_TYPE": "FAK",
             "SNIPER_SLIP": "0.05", "SNIPER_SHARES": "10", "SNIPER_MAX_TARGET_SHARES": "20",
             "SNIPER_COOLDOWN_MS": "300", "SNIPER_SHADOW_MS": "", "SNIPER_STALL_MS": "0",
             # the live-path checks rebuy a side at the same or a higher ask; the
             # below-average rule (SNIPER_AVG_DOWN) has its own tests with it on
             "SNIPER_AVG_DOWN": "0"}


def say(msg: str) -> None:
    print(f"  AUDIT | {msg}", flush=True)


def settings(**over):
    with patch.dict(os.environ, {**AUDIT_ENV, **over}):
        return S.Settings()


def log_line(r: dict) -> str:
    """The journal row as the one-line execution log the audit asks for."""
    sig2send = None
    if r.get("t_tick_to_decided_ms") is not None and r.get("t_decided_to_wire_ms") is not None:
        sig2send = r["t_tick_to_decided_ms"] + r["t_decided_to_wire_ms"]
    f = lambda v, fmt: "-" if v is None else format(v, fmt)  # noqa: E731
    return (f"SNIPER | side={r.get('side')} | move={f(r.get('bp'), '+.2f')}bp | refAsk={f(r.get('ask_seen'), '.2f')} "
            f"| cap={f(r.get('cap'), '.2f')} | type={r.get('order_type')} | carryIn={r.get('carry_in')} "
            f"| requested={r.get('target_shares')} | filled={r.get('shares')} | avgFill={f(r.get('price'), '.3f')} "
            f"| carryOut={r.get('carry_out')} | result={r.get('result')}/{r.get('kind')} "
            f"| signalToSend={f(sig2send, '.1f')}ms | response={f(r.get('t_wire_to_reply_ms'), '.1f')}ms")


# =========================================================================== 1. the trigger
class TriggerAudit(unittest.TestCase):
    """Trigger.update is the rule Sniper._signal applies to every Binance mid."""

    def fire(self, ref, mid, age_ms=260.0):
        s = settings()
        trig = S.Trigger(s.k_bp, s.window_ms)
        trig.update(0.0, ref)
        direction, r = trig.update(age_ms, mid)
        ref_t = trig.q[0][0]
        return direction, r, age_ms - ref_t

    def test_thresholds_both_directions(self):
        ref = 60_000.0
        for bp, want in ((0.49, None), (0.50, 1), (0.51, 1), (0.70, 1),
                         (-0.49, None), (-0.50, -1), (-0.51, -1), (-0.70, -1)):
            mid = round(ref * (1 + bp / 1e4), 3)
            direction, r, span = self.fire(ref, mid)
            got = 1 if direction == 1 else -1 if direction == -1 else None
            say(f"trigger {bp:+.2f}bp: ref {ref:.3f} -> mid {mid:.3f} measured {r:+.13f}bp over {span:.0f}ms "
                f"-> {'UP' if got == 1 else 'DOWN' if got == -1 else 'no order'}"
                + ("   <- exact 0.50bp boundary: see FINDING" if abs(bp) == 0.5 and got != want else ""))
            if abs(bp) != 0.5:                       # the exact boundary is asserted in the FINDING test
                self.assertEqual(got, want, bp)

    def test_the_move_is_measured_against_the_price_250ms_ago(self):
        s = settings()
        # a: reference tick 1200ms old but still the standing price 250ms ago
        t = S.Trigger(s.k_bp, s.window_ms)
        t.update(-1000.0, 60_000.0)
        t.update(0.0, 60_000.0)
        d, r = t.update(200.0, 60_003.6)
        say(f"window a: ticks at -1000/0/200ms; at 200ms the reference is the price standing at -50ms "
            f"(tick at {t.q[0][0]:.0f}ms) -> {r:+.2f}bp -> {'UP' if d == 1 else d}")
        self.assertEqual(d, 1)
        self.assertEqual(t.q[0][0], -1000.0)          # the last tick at or before t-250ms
        # b: a move that happened more than 250ms ago and has held flat since is no signal
        t = S.Trigger(s.k_bp, s.window_ms)
        t.update(0.0, 60_000.0)
        t.update(100.0, 60_003.6)
        d, r = t.update(400.0, 60_003.6)
        say(f"window b: +0.6bp at 100ms, flat until 400ms: measured {r:+.2f}bp against the 100ms price -> no order")
        self.assertIsNone(d)
        # c: a reference older than 2s (stalled feed) is not used
        t = S.Trigger(s.k_bp, s.window_ms)
        t.update(0.0, 60_000.0)
        d, r = t.update(2500.0, 60_010.0)
        say(f"window c: only reference 2500ms old -> {d} (stale feed: no 250ms move can be measured)")
        self.assertIsNone(d)
        # d: no tick old enough yet -> no reference, no signal
        t = S.Trigger(s.k_bp, s.window_ms)
        t.update(0.0, 60_000.0)
        d, _ = t.update(200.0, 60_010.0)
        self.assertIsNone(d)

    def test_FIXED_an_exact_half_bp_move_always_triggers(self):
        """FIXED (was a finding, bug, minor): Trigger.update compared the float
        (mid / ref - 1) * 1e4 with k_bp using >=. For mids where the move is
        exactly 0.50bp the float lands at 0.4999999999988 in ~43% of such cases,
        so the order is not sent. sniper_mode.py Trigger.update, line
        `if r >= self.k_bp`."""
        import random
        rng, missed, n = random.Random(1), [], 0
        for _ in range(400_000):
            ref = D(rng.randrange(3_000_000, 15_000_000)) / D(100)
            mid = ref + ref * D("0.00005")
            if mid != mid.quantize(D("0.005")):
                continue
            n += 1
            t = S.Trigger(0.5, 250)
            t.update(0.0, float(ref))
            d, r = t.update(260.0, float(mid))
            if d != 1:
                missed.append((str(ref), str(mid), r))
        for ref, mid in ((60_000.0, 59_997.0), (60_000.0, 60_003.0)):           # the audit's own -0.50 / +0.50
            t = S.Trigger(0.5, 250)
            t.update(0.0, ref)
            d, r = t.update(260.0, mid)
            if d is None:
                missed.append((str(ref), str(mid), r))
        say(f"exact 0.50bp moves: {n} tested, {len(missed)} computed below 0.5 and NOT sent, e.g. {missed[:2]}; "
            f"incl. 60000 -> 59997 (-0.50bp DOWN) not sent: {any(m[1] == '59997.0' for m in missed)}")
        self.assertEqual(missed, [])


# =========================================================================== 2. the live path, end to end
class LivePathBase(unittest.TestCase):
    ASK = 0.55

    def run_async(self, coro):
        return asyncio.run(coro)

    async def setup_live(self, asks=None, end=None):
        self.fake = await T.FakeClob().start()
        self.tmp = tempfile.TemporaryDirectory()
        self.ex, self.ledger, self.store, self.receipts, self.send_rows = await T.make_canary(
            self.fake, self.tmp.name, limits=L.limits_from_env({}))
        self.book = T.live_book(asks=asks or ((self.ASK, 30),), bids=((self.ASK - 0.01, 50),))
        self.hub = types.SimpleNamespace(up_token=T.UP, down_token=T.DN, condition_id=T.COND,
                                         window_end=end or T.round_end(), book=self.book)
        self.journal = os.path.join(self.tmp.name, "live_sniper.jsonl")
        self.sn = S.Sniper(self.hub, self.ex.shim, journal_path=self.journal, settings=settings(),
                           trade_rows=[], own_rounds=True, live=self.ex)
        self.ex.shim.trade_window_seconds = 600.0   # round_end() may pick the next round near a boundary
        self.order_n = 0

    async def teardown_live(self):
        for task in list(self.sn._tasks):
            task.cancel()
        await asyncio.gather(*self.sn._tasks, return_exceptions=True)
        await self.ex.sender.http.aclose()
        await self.fake.stop()
        self.tmp.cleanup()
        T.reset_ambiguity()

    def reply(self, kind="MATCHED", status=200, body=None):
        """Script the venue's answer to the next POST /order."""
        self.order_n += 1
        oid, tid = f"0xorder{self.order_n}", f"trade-{self.order_n}"
        if body is None:
            body = {"success": True, "status": "matched", "orderID": oid, "tradeIDs": [tid],
                    "transactionsHashes": [], "errorMsg": ""}
        self.fake.script.append(("json", status, body))
        return oid, tid

    async def tick(self, ref, mid, side_token=None, clear_cooldown=True):
        """A real trigger-feed message (a Coinbase ticker) moving the mid from ref (300ms ago) to mid."""
        if clear_cooldown:
            self.sn.last_fire = -1e18
        now = time.monotonic() * 1000.0
        self.sn.trigger.q.clear()
        self.sn.trigger.q.append((now - 300.0, ref))
        B.feed_tick(self.sn, B.tick_raw(self.sn.s.primary_feed, mid))
        for _ in range(500):
            if not self.sn.busy:
                break
            await asyncio.sleep(0.01)

    def trade(self, tid, oid, shares, price, status="MATCHED", token=None):
        """A private-channel trade for our order, as poly_user records it."""
        self.store.record_trade(tid, order_id=oid, asset_id=token or T.UP, market=T.COND, side="BUY",
                                price=price, size=shares, status=status)

    def rows(self, result=None):
        out = [json.loads(x) for x in open(self.journal, encoding="utf-8")]
        return out if result is None else [r for r in out if r.get("result") == result]

    def last_post(self):
        body, _hdrs, raw, _t = self.fake.orders[-1]
        return body, raw


class LivePathAudit(LivePathBase):
    def test_live_path_ioc_payload_partial_carry_and_ledger(self):
        self.run_async(self._live_path())

    async def _live_path(self):
        await self.setup_live()
        try:
            # --- signal 1: +0.6bp UP; reference ask 0.55 -> ceiling 0.60
            oid, tid = self.reply()
            await self.tick(60_000.0, 60_003.6)
            self.assertEqual(self.fake.hits[("POST", "/order")], 1)
            body, raw = self.last_post()
            order = body["order"]
            making, taking = D(str(order["makerAmount"])), D(str(order["takerAmount"]))
            limit = making / taking
            say(f"payload orderType={body.get('orderType')!r} owner={'set' if body.get('owner') else 'missing'} "
                f"side={order.get('side')!r} tokenId=...{str(order.get('tokenId'))[-6:]}")
            say(f"payload makerAmount={order['makerAmount']} (USDC x1e6) takerAmount={order['takerAmount']} "
                f"(shares x1e6) -> limit price {limit:.6f}; expiration={order.get('expiration')!r} "
                f"feeRateBps={order.get('feeRateBps')!r} signatureType={order.get('signatureType')!r}")
            say(f"payload keys: {sorted(body)} / order keys: {sorted(order)}")
            self.assertEqual(body["orderType"], "FAK")                 # Polymarket's IOC: fill what can, kill the rest
            self.assertNotIn(body["orderType"], ("FOK", "GTC", "GTD"))
            self.assertIn(str(order["side"]).upper(), ("BUY", "0"))
            self.assertEqual(making, D("5500000"))                      # 10 shares x the 0.55 reference ask
            self.assertLessEqual(abs(limit - D("0.60")), D("0.001"))    # the ceiling: reference ask + 0.05
            pend = self.rows("pending")[-1]
            self.assertEqual((pend["order_type"], pend["target_shares"], pend["carry_in"], pend["ask_seen"],
                              pend["cap"]), ("FAK", 10.0, 0.0, 0.55, 0.6))
            self.assertIn(oid, self.sn._fak_pending)                  # quantity comes from the private channel
            # --- the venue filled 6 of 10 (private trade); 4 were cancelled
            self.trade(tid, oid, 6, 0.55)
            self.sn._sync_live_fak()
            upd = self.rows("live_fill_update")[-1]
            say(log_line({**pend, **upd}))
            self.assertEqual((upd["shares"], upd["carry_out"]), (6.0, 4.0))
            self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 0.0})
            self.assertEqual(self.sn.rounds[self.hub.window_end]["UP"], 6.0)
            # --- the ledger books the CONFIRMED trade: 6 shares owned, nothing for the cancelled 4
            self.trade(tid, oid, 6, 0.55, status="CONFIRMED")
            self.ledger.ingest_fill_store(self.store)
            pos = self.ledger.positions[T.UP]
            say(f"ledger position: {pos.shares:g} shares, avg entry {pos.avg_price:.4f}, cost ${pos.cost:.4f} "
                f"(fees ${pos.fees:.4f})")
            self.assertAlmostEqual(pos.shares, 6.0)
            self.assertAlmostEqual(pos.avg_price, 0.55, places=6)
            # --- canary exposure: requested $5.50 at send, actual cost once confirmed
            before = self.ex.sender.limits.exposure_usd
            await asyncio.to_thread(self.ex.reconcile_once)
            after = self.ex.sender.limits.exposure_usd
            say(f"canary spend: ${before:.2f} counted at send (requested) -> ${after:.2f} after confirmation (actual)")
            self.assertAlmostEqual(before, 5.50, places=6)
            self.assertAlmostEqual(after, 6 * 0.55, places=2)
            # --- signal 2 (same side): 10 base + 4 carry = 14
            oid2, tid2 = self.reply()
            await self.tick(60_000.0, 60_003.6)
            body, _ = self.last_post()
            self.assertEqual(D(str(body["order"]["makerAmount"])), D("7700000"))   # 14 x 0.55
            self.trade(tid2, oid2, 11, 0.55)
            self.sn._sync_live_fak()
            say(log_line({**self.rows("pending")[-1], **self.rows("live_fill_update")[-1]}))
            self.assertEqual(self.sn.carry["UP"], 3.0)
            # --- signal 3: 10 + 3 = 13
            oid3, tid3 = self.reply()
            await self.tick(60_000.0, 60_003.6)
            body, _ = self.last_post()
            self.assertEqual(D(str(body["order"]["makerAmount"])), D("7150000"))   # 13 x 0.55
            self.assertEqual(self.rows("pending")[-1]["target_shares"], 13.0)
            self.assertEqual(self.fake.hits[("POST", "/order")], 3)               # one POST per signal, no retry
        finally:
            await self.teardown_live()

    def test_live_full_fill(self):
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)
                self.trade(tid, oid, 10, 0.55)
                self.sn._sync_live_fak()
                upd = self.rows("live_fill_update")[-1]
                say(log_line({**self.rows("pending")[-1], **upd}))
                self.assertEqual((upd["shares"], upd["carry_out"], upd["resolved_result"]), (10.0, 0.0, "filled"))
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_zero_fill_is_no_fill_and_carries_the_target(self):
        async def go():
            await self.setup_live()
            try:
                self.reply(status=400, body={"success": False, "errorMsg": "no orders found to match with FAK order. "
                                             "FAK orders are partially filled or killed if no match is found."})
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("refused")[-1]
                say(log_line(r) + f" | reason={r.get('reason')!r}")
                self.assertEqual((r["kind"], r["carry_out"]), ("NO_MATCH", 10.0))
                self.assertEqual(self.sn.n["filled"], 0)
                self.assertEqual(self.sn.rounds[self.hub.window_end]["fills"], 0)
                self.assertEqual(self.ex.sender.limits.exposure_usd, 0.0)          # released at once
                self.assertEqual(self.ex.sender.limits.fills, 0)
                self.assertNotIn(T.UP, self.ledger.positions)
                self.assertFalse(self.ex.kill.tripped)
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_rejection_leaves_carry_and_counts_nothing(self):
        async def go():
            await self.setup_live()
            try:
                self.sn.carry_market, self.sn.carry["UP"] = (T.COND, self.hub.window_end), 4.0
                self.reply(status=400, body={"success": False, "errorMsg": "not enough balance / allowance"})
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("refused")[-1]
                say(log_line(r) + f" | reason={r.get('reason')!r} | kill switch: {self.ex.kill.reasons}")
                self.assertEqual(r["kind"], "REJECTED")
                self.assertEqual(self.sn.carry["UP"], 4.0)                       # an unknown-quantity outcome
                self.assertEqual(self.sn.n["filled"], 0)
                self.assertEqual(self.ex.sender.limits.exposure_usd, 0.0)
                self.assertTrue(self.ex.kill.tripped)                            # collateral rejection halts
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_stale_or_not_live_book_sends_nothing(self):
        async def go():
            await self.setup_live()
            try:
                self.sn.s.stale_ms = 0.0001                                      # any book counts as stale
                await self.tick(60_000.0, 60_003.6)
                self.assertEqual(self.fake.hits[("POST", "/order")], 0)
                skip = self.rows("skipped")[-1]
                say(f"stale book: skipped '{skip['reason']}', no POST")
                self.assertEqual(skip["reason"], "book stale")
                self.sn.s.stale_ms = 2000
                self.book.connected = False
                self.book.mark_all_unsynced() if hasattr(self.book, "mark_all_unsynced") else None
                await self.tick(60_000.0, 60_003.6)
                say(f"book not live: last skip '{self.rows('skipped')[-1]['reason']}', POSTs {self.fake.hits[('POST', '/order')]}")
                self.assertEqual(self.fake.hits[("POST", "/order")], 0)
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_20_share_cap(self):
        async def go():
            await self.setup_live()
            try:
                amounts = []
                for _ in range(3):
                    self.reply(status=400, body={"success": False, "errorMsg": "no orders found to match with FAK order"})
                    await self.tick(60_000.0, 60_003.6)
                    amounts.append(D(str(self.last_post()[0]["order"]["makerAmount"])) / D(1_000_000))
                targets = [r["target_shares"] for r in self.rows("refused")]
                say(f"three zero fills: targets {targets}, posted ${', $'.join(str(a) for a in amounts)}")
                self.assertEqual(targets, [10.0, 20.0, 20.0])
                self.assertEqual(amounts, [D("5.5"), D("11"), D("11")])
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_up_and_down_carry_isolation(self):
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)
                self.trade(tid, oid, 6, 0.55)
                self.sn._sync_live_fak()
                self.reply()
                await self.tick(60_000.0, 59_996.4)                              # DOWN; DOWN book ask 0.51
                down = self.rows("pending")[-1]
                body, _ = self.last_post()
                say(f"DOWN after an UP carry of 4: target {down['target_shares']}, carry_in {down['carry_in']}, "
                    f"posted ${D(str(body['order']['makerAmount'])) / D(1_000_000)} (10 x 0.51)")
                self.assertEqual((down["side"], down["target_shares"], down["carry_in"]), ("DOWN", 10.0, 0.0))
                self.assertAlmostEqual(down["amount"], 5.10, places=6)          # 10 x 0.51, no UP carry in it
                self.assertEqual(D(str(body["order"]["makerAmount"])), D("5100000"))    # the whole $5.10 is signed
                self.assertEqual(self.sn.carry["UP"], 4.0)
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_carry_resets_on_the_next_round(self):
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)
                self.trade(tid, oid, 6, 0.55)
                self.sn._sync_live_fak()
                self.assertEqual(self.sn.carry["UP"], 4.0)
                self.hub.window_end += 300                                       # the next market
                self.assertEqual(self.sn.carry_view(), {"UP": 0.0, "DOWN": 0.0})
                self.reply()
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("pending")[-1]
                say(f"next round: target {r['target_shares']}, carry_in {r['carry_in']}")
                self.assertEqual((r["target_shares"], r["carry_in"]), (10.0, 0.0))
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_ceiling_is_anchored_to_the_reference_ask(self):
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)
                body, _ = self.last_post()
                lim1 = D(str(body["order"]["makerAmount"])) / D(str(body["order"]["takerAmount"]))
                self.trade(tid, oid, 3, 0.58)                                    # 3 matched inside the ceiling
                self.sn._sync_live_fak()
                await asyncio.sleep(0.4)
                say(f"order 1: ref 0.55, limit {lim1:.4f}; filled 3; POSTs after 0.4s with no new signal: "
                    f"{self.fake.hits[('POST', '/order')]}")
                self.assertEqual(self.fake.hits[("POST", "/order")], 1)          # no chase without a signal
                # the book moves to 0.61; a NEW signal is a new execution with its own reference
                self.book.apply_snapshot(T.UP, [(0.60, 50)], [(0.61, 30)], ts_ms=int(time.time() * 1000),
                                         require_exchange_ts=False)
                self.reply()
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("pending")[-1]
                say(f"order 2 (new signal): ref {r['ask_seen']}, cap {r['cap']}, target {r['target_shares']} "
                    f"(the 7 carried shares are bought under THIS execution's ceiling)")
                self.assertEqual((r["ask_seen"], r["cap"]), (0.61, 0.66))
                self.assertLessEqual(lim1, D("0.6005"))
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_live_rapid_signals_one_order(self):
        async def go():
            await self.setup_live()
            try:
                self.reply()
                now = time.monotonic() * 1000.0
                self.sn.trigger.q.append((now - 300.0, 60_000.0))
                for i in range(40):                                              # 40 qualifying ticks, ~instant
                    B.feed_tick(self.sn, B.tick_raw(self.sn.s.primary_feed, 60_003.61 + i * 0.01))
                for _ in range(300):
                    if not self.sn.busy:
                        break
                    await asyncio.sleep(0.01)
                say(f"40 qualifying ticks in a burst: POSTs {self.fake.hits[('POST', '/order')]}, "
                    f"skips: cooldown {self.sn.n['skip: cooldown']}, in flight "
                    f"{self.sn.n['skip: previous order in flight']}")
                self.assertEqual(self.fake.hits[("POST", "/order")], 1)
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_latency_instrumentation(self):
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("pending")[-1]
                ts = r.get("live_ts_us") or {}
                say(f"live stamps (us after the decision, perf_counter_ns): {ts}")
                say(f"signal->decided {r.get('t_tick_to_decided_ms')}ms, decided->wire {r.get('t_decided_to_wire_ms')}ms, "
                    f"wire->reply {r.get('t_wire_to_reply_ms')}ms")
                for k in ("decided", "prepare", "build", "signed", "ready", "net_send_start", "response"):
                    self.assertIn(k, ts)
                for k in ("t_tick_to_decided_ms", "t_decided_to_wire_ms", "t_wire_to_reply_ms"):
                    self.assertIsInstance(r.get(k), (int, float))
                self.trade(tid, oid, 10, 0.55)
                fill = self.store.fills[tid]
                say(f"private fill first_seen={fill.first_seen:.3f} (time.monotonic) - recorded on the Fill, "
                    f"not turned into a latency in the journal")
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_FIXED_journal_records_signal_to_fill_confirmed(self):
        """FIXED (was an instrumentation gap): the private fill's arrival is stamped
        on the Fill (feeds/poly_user.py FillStore.record_trade, time.monotonic)
        but sniper_mode.Sniper._record_fak_fill never journals it, so
        signal -> fill_confirmed cannot be computed from the log."""
        async def go():
            await self.setup_live()
            try:
                oid, tid = self.reply()
                await self.tick(60_000.0, 60_003.6)
                self.trade(tid, oid, 10, 0.55)
                self.sn._sync_live_fak()
                upd = self.rows("live_fill_update")[-1]
                self.assertTrue(any("fill" in k and ("confirm" in k or "_ms" in k) for k in upd), sorted(upd))
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_FIXED_journal_records_the_trigger_window(self):
        """FIXED (was a log gap): the order row has the move (bp) but not the
        interval it was measured over (the reference tick's age); Sniper._signal
        does not pass the Trigger's reference time to the row."""
        async def go():
            await self.setup_live()
            try:
                self.reply()
                await self.tick(60_000.0, 60_003.6)
                r = self.rows("pending")[-1]
                self.assertTrue(any(k in r for k in ("window_ms", "ref_age_ms", "move_window_ms")), sorted(r))
            finally:
                await self.teardown_live()
        self.run_async(go())

    def test_FIXED_down_signal_not_blocked_while_up_fak_quantity_pending(self):
        """FIXED (was a requirement conflict): live, after an UP FAK is accepted
        and before its traded quantity arrives on the private channel, every
        signal on BOTH sides is skipped ('awaiting FAK fill size':
        sniper_mode.Sniper._decide, and LiveExecution.prepare refuses too).
        The 300ms cooldown is per side, but this gate is global."""
        async def go():
            await self.setup_live()
            try:
                self.reply()
                await self.tick(60_000.0, 60_003.6)                              # UP accepted, quantity pending
                self.reply()
                await self.tick(60_000.0, 59_996.4)                              # DOWN 0.3s later
                self.assertEqual(self.fake.hits[("POST", "/order")], 2, self.rows("skipped")[-1:])
            finally:
                await self.teardown_live()
        self.run_async(go())


class SizingAudit(unittest.TestCase):
    def test_FIXED_the_signed_stake_is_never_a_cent_below_the_computed_one(self):
        """FIXED (was a finding, bug, minor, pre-existing): py_clob_client_v2 signs a BUY's
        dollar amount with round_down(x, 2) = floor(x * 100) / 100 on a float.
        Amounts whose float sits just below the cent (5.10 -> 5.0999999...)
        are signed a cent lower: the live order buys up to ~1% fewer shares
        than its target (the carry picks the shortfall up next signal) and a
        cent less than paper, which rounds in Decimal. Live: the amount from
        sniper_mode.Sniper.ioc_spend (and before it shares x ask) reaches the
        SDK via sniper_live.LeanSender.prepare unchanged."""
        from py_clob_client_v2.order_builder.helpers import round_down
        short = []
        for cents in range(5, 96):
            ask = cents / 100
            for target in range(10, 21):
                amount = S.Sniper.ioc_spend([(ask, 1000)], target, ask, ask + 0.05)
                signed = round_down(S.signing_amount(amount), 2)            # what the SDK signs now
                if abs(signed - round(amount, 2)) > 1e-9:
                    short.append((target, ask, amount, signed))
        say(f"signed a cent short: {len(short)} of 1001 (ask, target) cases, e.g. {short[:3]}")
        self.assertEqual(short, [])

    def test_canary_share_cap_must_admit_carried_targets(self):
        """CONFIG RISK: the live canary refuses an order whose size at the limit
        exceeds SNIPER_CANARY_MAX_SHARES. With 10 (an old canary setting) every
        carried target above ~11 shares is refused live and the carry never
        executes; .env.example sets 25."""
        lim = L.limits_from_env({"SNIPER_CANARY_MAX_SHARES": "10"})
        kill = L.KillSwitch()
        amount, cap = 14 * 0.55, 0.60
        with self.assertRaises(L.Refused) as caught:
            lim.check(amount, amount / cap, "cond", kill)
        say(f"SNIPER_CANARY_MAX_SHARES=10, target 14 @ 0.55 (limit 0.60): {caught.exception}")
        L.limits_from_env({}).check(20 * 0.55, 20 * 0.55 / 0.60, "cond", kill)   # the default 25 admits 20


# =========================================================================== 3. per-side cooldown
class CooldownAudit(unittest.TestCase):
    def make(self):
        with patch.dict(os.environ, AUDIT_ENV):
            sn = S.Sniper(None, None, settings=S.Settings())
        clock = types.SimpleNamespace(ms=50_000.0)
        sent = []

        def fire(side, *a, **k):
            sent.append((side, clock.ms - 50_000.0))
            sn.busy = False
            return object()
        sn._fire_now = fire
        return sn, clock, sent

    def test_the_audit_timeline(self):
        sn, clock, sent = self.make()
        up = types.SimpleNamespace(update=lambda t, m: (1, 0.6))
        down = types.SimpleNamespace(update=lambda t, m: (-1, -0.6))
        expect = []
        with patch.object(S.time, "monotonic", lambda: clock.ms / 1000.0):
            for at, trig, label, allowed in ((0, up, "UP", True), (200, up, "UP", False), (200, down, "DOWN", True),
                                             (299, up, "UP", False), (300, up, "UP", True)):
                clock.ms = 50_000.0 + at
                before = len(sent)
                sn._signal("binance", trig, 60_000.0, clock.ms, 0)
                got = len(sent) > before
                say(f"cooldown {at:>3}ms {label:<4} -> {'ALLOW' if got else 'BLOCK'}")
                expect.append((at, label, allowed, got))
        self.assertEqual([(a, l, ok) for a, l, ok, _ in expect], [(a, l, g) for a, l, _, g in expect])
        self.assertEqual(sn.s.cooldown_ms, 300)

    def test_FIXED_down_signal_not_blocked_while_an_up_order_is_in_flight(self):
        """FIXED (was a requirement conflict): the one-order-in-flight guard
        (sniper_mode.Sniper._signal, `if self.busy`) is global: while an UP
        order awaits its venue reply (paper: its modelled match), a valid DOWN
        signal is skipped as 'previous order in flight'. Kept as a safety
        guard; it contradicts 'an UP execution must not block DOWN'."""
        sn, clock, sent = self.make()
        sn._fire_now = lambda side, *a, **k: sent.append(side) or object()   # UP stays in flight
        with patch.object(S.time, "monotonic", lambda: clock.ms / 1000.0):
            sn._signal("binance", types.SimpleNamespace(update=lambda t, m: (1, 0.6)), 60_000.0, clock.ms, 0)
            clock.ms += 50
            sn._signal("binance", types.SimpleNamespace(update=lambda t, m: (-1, -0.6)), 60_000.0, clock.ms, 0)
        self.assertEqual(sent, ["UP", "DOWN"])


# =========================================================================== 4. old paths and settings
class OldPathAudit(unittest.TestCase):
    def calls(self, names):
        """(file, line, call) for every call of an attribute/function in `names`
        in the non-test code."""
        out = []
        for path in sorted(ROOT.glob("**/*.py")):
            rel = path.relative_to(ROOT).as_posix()
            if rel.startswith(("tests_", ".venv")) or "/tests_" in rel or rel == "testing_env.py":
                continue
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Call):
                    f = node.func
                    name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
                    if name in names:
                        out.append((rel, node.lineno, ast.unparse(f)))
        return out

    def test_no_live_order_path_other_than_the_lean_sender(self):
        place = self.calls({"place_trade"})
        say("place_trade call sites: " + "; ".join(f"{f}:{n} {c}" for f, n, c in place))
        # sniper_mode calls its broker's place_trade: the PaperBroker (paper) - live uses LiveExecution.
        # position_manager.py: the PAPER broker only (raises in live/shadow mode; --live refuses manager
        # orders); replay_ab.py: its own replay broker, no venue at all.
        self.assertTrue(all(f in ("sniper_mode.py", "paper_trade.py", "polymarket_trade.py", "position_manager.py",
                                  "replay_ab.py") for f, _, _ in place), place)
        self.assertFalse(any(c.startswith(("polymarket_trade.", "pt.")) for _, _, c in place),
                         "something calls the old polymarket_trade.place_trade (FOK path)")
        builds = self.calls({"build_market_order", "create_market_order", "create_order", "post_order"})
        say("order-building / posting call sites: " + "; ".join(f"{f}:{n} {c}" for f, n, c in builds))
        live_files = {f for f, _, _ in builds}
        # bench_execution.py builds orders with a throwaway key made in memory and sends
        # them only to a fake CLOB on 127.0.0.1 (a latency benchmark, not an order path)
        self.assertTrue(live_files <= {"sniper_live.py", "sniper_mode.py", "polymarket_trade.py", "live_preflight.py",
                                       "bench_execution.py"}, live_files)

    def test_every_lean_sender_prepare_passes_the_order_type(self):
        src = (ROOT / "sniper_live.py").read_text(encoding="utf-8")
        hits = [n for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr == "prepare" and "sender" in ast.unparse(n.func.value)]
        say(f"LeanSender.prepare(order_type='FOK' by default) is called {len(hits)}x, each with order_type: "
            f"{[any(k.arg == 'order_type' for k in h.keywords) for h in hits]}")
        self.assertTrue(hits and all(any(k.arg == "order_type" for k in h.keywords) for h in hits))

    def test_effective_settings_and_code_defaults(self):
        with patch.dict(os.environ, {"SNIPER_ONLY": "1"}, clear=True):
            d = S.Settings()
        say(f"code defaults (used when .env omits a key): k_bp={d.k_bp} window={d.window_ms} type={d.order_type} "
            f"slip={d.slip} cooldown={d.cooldown_ms} shares={d.shares} max_target={d.max_target_shares}")
        ex = {}
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if v.strip():
                    ex[k.strip()] = v.strip()
        ex["SNIPER_DELAY_SOURCE"] = "model"
        with patch.dict(os.environ, ex, clear=True):
            e = S.Settings()
        say(f".env.example values:        k_bp={e.k_bp} window={e.window_ms} type={e.order_type} slip={e.slip} "
            f"cooldown={e.cooldown_ms} shares={e.shares} max_target={e.max_target_shares}")
        import exec_latency
        frozen = {attr: value for attr, _var, value, _label, _fmt in exec_latency.FROZEN_SPEC}
        self.assertEqual((e.k_bp, e.window_ms, e.order_type, e.slip, e.cooldown_ms, e.shares, e.max_target_shares),
                         tuple(frozen[k] for k in ("k_bp", "window_ms", "order_type", "slip", "cooldown_ms", "shares",
                                                   "max_target_shares")))      # .env.example is the frozen spec
        with patch.dict(os.environ, {"SNIPER_ONLY": "1", "SNIPER_K_BP": "0.8", "SNIPER_SLIP": "0.45",
                                     "SNIPER_COOLDOWN_MS": "1000", "SNIPER_ORDER_TYPE": "FOK"}, clear=True):
            old = S.Settings()
        say(f"an old .env still wins:     k_bp={old.k_bp} type={old.order_type} slip={old.slip} "
            f"cooldown={old.cooldown_ms} -> start line: {old.lines()[0][:96]}...")
        self.assertEqual((old.k_bp, old.order_type), (0.8, "FOK"))


# =========================================================================== 5. paper accounting (real broker)
class PaperAccountingAudit(unittest.TestCase):
    def test_actual_fill_accounting(self):
        import tests_sniper_ioc as I
        case = I.PaperIOCTests("test_full_fill")
        case.setUp()
        try:
            r = case.fire("UP", [(0.55, 6), (0.70, 100)])                       # 6 within the 0.60 ceiling
            ledger = case.broker.ledger
            pos = ledger.positions["up"]
            s = case.broker.summary(mark=lambda token: 0.60)
            say(f"paper partial: requested {r['target_shares']}, filled {r['shares']} @ {r['price']}, position "
                f"{pos.shares:g} sh avg {pos.avg_price:.3f} cost ${pos.cost:.2f}; cash ${s['cash']:.2f} "
                f"(1000 - {pos.cost:.2f}); unrealized at 0.60 {s.get('unrealized_mark_to_bid')}; "
                f"round UP {case.sn.rounds[case.m.window_end]['UP']}; fills counted {case.sn.n['filled']}")
            self.assertAlmostEqual(pos.shares, 6.0)
            self.assertAlmostEqual(pos.avg_price, 0.55)
            self.assertAlmostEqual(s["cash"], 1000 - 6 * 0.55, places=6)
            self.assertAlmostEqual(s["unrealized_mark_to_bid"], 6 * (0.60 - 0.55), places=6)
            self.assertEqual(case.sn.rounds[case.m.window_end]["UP"], 6.0)
            z = case.fire("UP", [(0.55, 100)], match=[(0.70, 100)])              # zero fill
            self.assertEqual(z["result"], "refused")
            self.assertEqual(case.sn.n["filled"], 1)
            self.assertAlmostEqual(ledger.positions["up"].shares, 6.0)
        finally:
            case.tearDown()


if __name__ == "__main__":
    unittest.main(verbosity=2)
