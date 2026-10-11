#!/usr/bin/env python3
"""IOC (FAK) execution with a share target and same-side unfilled carry.

    python tests_sniper_ioc.py

Paper cases run the real Sniper through the real PaperBroker and ledger, on
books this file controls: the book the decision sees, then the book the
order meets at the match. Live cases drive Sniper._finish_live with a fake
sender that answers as the venue would. No network.
"""
from __future__ import annotations

import asyncio
import collections
import os
import pathlib
import sys
import tempfile
import time
import types
import unittest
from decimal import Decimal as D

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode  # noqa: E402
from accounting.ledger import Ledger  # noqa: E402
from paper_trade import BookSnapshot, MarketRules, PaperBroker  # noqa: E402

UP, DOWN = "up", "down"
ENV = {"SNIPER_ONLY": "1", "SNIPER_ORDER_TYPE": "FAK", "SNIPER_SLIP": "0.05", "SNIPER_SHARES": "10",
       "SNIPER_MAX_TARGET_SHARES": "20", "SNIPER_LATENCY_MS": "20", "SNIPER_SHADOW_MS": "",
       "SNIPER_MIN_LEFT_S": "10", "SNIPER_MAX_FILLS_ROUND": "20", "SNIPER_MAX_SIDE_SHARES": "200",
       # these tests rebuy a side at the same price to test carry, caps and sizing;
       # the below-average rule (SNIPER_AVG_DOWN) has its own tests with it on
       "SNIPER_AVG_DOWN": "0"}


def settings(**over):
    saved = {k: os.environ.get(k) for k in {**ENV, **over}}
    os.environ.update({**ENV, **over})
    try:
        return sniper_mode.Settings()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def current_round_end() -> int:
    """This round's end, waiting out a round with under 20s left (orders
    need >= 10s plus the modelled flight)."""
    while True:
        now = time.time()
        end = (int(now) // 300 + 1) * 300
        if end - now > 20:
            return end
        time.sleep(end - now + 0.5)


class View:
    """The sniper's socket view of one token's book."""

    def __init__(self, levels):
        self.asks = tuple((float(p), float(q)) for p, q in levels)
        self.status = "LIVE"
        self.best_ask = self.asks[0][0] if self.asks else None
        self.mid = self.best_ask
        self.tick_size = 0.01
        self.updated_mono = time.monotonic()


class Market:
    """One market that both the Sniper (its socket view) and the PaperBroker
    (its book fetch) read. Per order: the first broker read is the decision
    book, every later one the book the order meets at the match."""

    def __init__(self, end):
        self.condition_id, self.window_end = "cond-1", end
        self.up_token, self.down_token = UP, DOWN
        self.decision = collections.defaultdict(lambda: [(0.47, 100)])
        self.match = collections.defaultdict(lambda: [(0.47, 100)])
        self.reads = collections.Counter()
        self.book = types.SimpleNamespace(view=lambda token: View(self.decision[str(token)]))

    def set(self, token, decision, match=None):
        self.decision[token] = decision
        self.match[token] = decision if match is None else match
        self.reads[token] = 0

    def fetch(self, token):
        token = str(token)
        levels = self.decision[token] if self.reads[token] == 0 else self.match[token]
        self.reads[token] += 1
        ask = levels[0][0] if levels else 0.5
        return BookSnapshot(token, tuple((D(str(p)), D(str(q))) for p, q in levels),
                            min_order_size=D("5"), tick_size=D("0.01"), timestamp=str(int(time.time() * 1000)),
                            book_hash="h", received_wall=time.time(), best_bid=D(str(round(ask - 0.01, 2))))


class PaperIOCBase(unittest.TestCase):
    order_type = "FAK"

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        tmp = pathlib.Path(self.tmp.name)
        self.m = Market(current_round_end())
        m = self.m
        self.broker = PaperBroker(
            Ledger(path=str(tmp / "ledger.json")),
            market_context=lambda: {"condition_id": m.condition_id, "up_token_id": m.up_token,
                                    "down_token_id": m.down_token},
            host="https://public.invalid", max_buy_price=0.95, min_buy_price=0.05, start_balance=1000,
            account_path=tmp / "account.json", audit_path=tmp / "orders.jsonl", book_fetch=m.fetch,
            rules_fetch=lambda cid: MarketRules(cid, D("0"), 1, min_order_size=D("5"), tick_size=D("0.01"),
                                                up_token_id=m.up_token, down_token_id=m.down_token),
            min_seconds_to_expiry=0, trade_window_seconds=300, own_fill_ttl_s=0)
        self.sn = sniper_mode.Sniper(m, self.broker, settings=settings(SNIPER_ORDER_TYPE=self.order_type))
        self.rows = []
        self.sn._write = self.rows.append

    def tearDown(self):
        self.tmp.cleanup()

    def fire(self, side, decision, match=None):
        """One signal on `side`: the decision sees `decision`, the order meets `match`."""
        self.m.set(self.m.up_token if side == "UP" else self.m.down_token, decision, match)
        before = len(self.rows)
        asyncio.run(self.sn._fire(side, 1.0 if side == "UP" else -1.0, time.monotonic() * 1000.0))
        orders = [r for r in self.rows[before:] if r.get("result") in ("filled", "refused", "skipped", "error")]
        self.assertEqual(len(orders), 1, self.rows[before:])
        return orders[0]

    def held(self, token):
        pos = self.broker.ledger.positions.get(token)
        return 0.0 if pos is None else float(pos.shares)


class PaperIOCTests(PaperIOCBase):
    def test_full_fill(self):
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r["result"], r["target_shares"], r["carry_in"]), ("filled", 10, 0))
        self.assertAlmostEqual(r["shares"], 10.0, places=6)
        self.assertAlmostEqual(r["price"], 0.47, places=6)
        self.assertEqual(self.sn.carry, {"UP": 0.0, "DOWN": 0.0})
        self.assertAlmostEqual(self.held(UP), 10.0, places=6)
        self.assertAlmostEqual(r["amount"], 4.70, places=6)

    def test_full_fill_consumes_levels_from_the_ask_up_to_ask_plus_5c(self):
        r = self.fire("UP", [(0.47, 5), (0.50, 100)])
        self.assertAlmostEqual(r["amount"], 4.85, places=6)                 # 5 x 0.47 + 5 x 0.50
        self.assertAlmostEqual(r["shares"], 10.0, places=6)
        self.assertAlmostEqual(r["price"], 0.485, places=6)
        self.assertEqual(self.sn.carry["UP"], 0.0)

    def test_partial_fill_is_kept_and_its_remainder_carried(self):
        r = self.fire("UP", [(0.47, 6), (0.60, 100)])
        self.assertEqual(r["result"], "filled")
        self.assertTrue(r.get("partial"))
        self.assertAlmostEqual(r["shares"], 6.0, places=6)
        self.assertAlmostEqual(self.held(UP), 6.0, places=6)               # actual shares, not the 10 asked for
        self.assertAlmostEqual(self.sn.rounds[self.m.window_end]["UP"], 6.0, places=6)
        self.assertEqual(r["carry_out"], 4.0)
        self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 0.0})

    def test_zero_fill_is_not_a_fill_and_carries_the_whole_target(self):
        r = self.fire("UP", [(0.47, 100)], match=[(0.60, 100)])             # ran past the ceiling in flight
        self.assertEqual(r["result"], "refused")
        self.assertIn("fak no-fill", str(r["reason"]).lower())
        self.assertEqual(self.sn.n["filled"], 0)
        self.assertEqual(self.sn.rounds[self.m.window_end]["fills"], 0)
        self.assertEqual(self.held(UP), 0.0)
        self.assertEqual(self.sn.carry["UP"], 10.0)

    def test_never_above_the_reference_ask_plus_5c(self):
        # Decided on 0.47: ceiling 0.52. By the match the book moved: 3 at
        # 0.50 inside the ceiling, the rest at 0.53 - which is never touched,
        # and the ceiling is not recomputed from the new ask.
        r = self.fire("UP", [(0.47, 100)], match=[(0.50, 3), (0.53, 100)])
        self.assertAlmostEqual(r["cap"], 0.52, places=6)
        self.assertEqual(r["result"], "filled")
        self.assertAlmostEqual(r["shares"], 3.0, places=6)
        self.assertLessEqual(r["price"], 0.52)
        self.assertAlmostEqual(r["price"], 0.50, places=6)
        self.assertEqual(self.sn.carry["UP"], 7.0)
        # no retry chased the book: one order, one broker fill
        self.assertEqual(self.sn.n["orders"], 1)
        self.assertEqual(len([x for x in self.rows if x.get("result") in ("filled", "refused")]), 1)

    def test_carry_goes_to_the_next_same_side_signal(self):
        # the example: 10 -> 6 filled -> carry 4 -> next target 14 -> 11 fill -> carry 3 -> next target 13
        r1 = self.fire("UP", [(0.47, 6), (0.60, 100)])
        r2 = self.fire("UP", [(0.47, 11), (0.60, 100)])
        self.assertEqual((r2["carry_in"], r2["target_shares"]), (4.0, 14.0))
        self.assertAlmostEqual(r2["shares"], 11.0, places=6)
        self.assertEqual(self.sn.carry["UP"], 3.0)
        r3 = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r3["carry_in"], r3["target_shares"]), (3.0, 13.0))
        self.assertAlmostEqual(r3["shares"], 13.0, places=6)
        self.assertEqual(self.sn.carry["UP"], 0.0)
        self.assertAlmostEqual(self.held(UP), r1["shares"] + r2["shares"] + r3["shares"], places=6)

    def test_up_and_down_carries_never_mix(self):
        self.fire("UP", [(0.47, 6), (0.60, 100)])                           # UP carry 4
        d1 = self.fire("DOWN", [(0.47, 7), (0.60, 100)])
        self.assertEqual((d1["carry_in"], d1["target_shares"]), (0.0, 10.0))  # nothing from UP
        self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 3.0})
        u2 = self.fire("UP", [(0.47, 100)])
        self.assertEqual(u2["target_shares"], 14.0)
        self.assertEqual(self.sn.carry, {"UP": 0.0, "DOWN": 3.0})          # DOWN's untouched by the UP fill
        d2 = self.fire("DOWN", [(0.47, 100)])
        self.assertEqual(d2["target_shares"], 13.0)
        self.assertAlmostEqual(self.held(UP), 6 + 14, places=6)
        self.assertAlmostEqual(self.held(DOWN), 7 + 13, places=6)

    def test_target_is_capped_at_20_shares(self):
        r1 = self.fire("UP", [(0.47, 100)], match=[(0.60, 100)])             # zero fill: carry 10
        r2 = self.fire("UP", [(0.47, 100)], match=[(0.60, 100)])             # target 20, zero again: carry 20
        r3 = self.fire("UP", [(0.47, 100)])
        self.assertEqual([r1["target_shares"], r2["target_shares"], r3["target_shares"]], [10.0, 20.0, 20.0])
        self.assertEqual(r2["carry_out"], 20.0)
        self.assertAlmostEqual(r3["shares"], 20.0, places=6)               # 10 + 20 carried, capped at 20
        self.assertEqual(self.sn.carry["UP"], 0.0)

    def test_carry_is_cleared_when_the_market_changes(self):
        self.fire("UP", [(0.47, 6), (0.60, 100)])
        self.fire("DOWN", [(0.47, 7), (0.60, 100)])
        self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 3.0})
        # the next market: a new condition and new outcome tokens
        self.m.condition_id, self.m.up_token, self.m.down_token = "cond-2", "up-2", "down-2"
        self.assertEqual(self.sn.carry_view(), {"UP": 0.0, "DOWN": 0.0})  # already void for it
        r = self.fire("UP", [(0.47, 100)])
        self.assertEqual((r["result"], r["carry_in"], r["target_shares"]), ("filled", 0.0, 10.0))
        self.assertAlmostEqual(r["shares"], 10.0, places=6)
        self.assertAlmostEqual(self.held("up-2"), 10.0, places=6)
        self.assertEqual(self.sn.carry, {"UP": 0.0, "DOWN": 0.0})
        # a different round with the same condition id is a different market too
        self.sn.carry["UP"] = 4.0
        self.assertEqual(self.sn._carry_for("cond-2", self.m.window_end + 300), {"UP": 0.0, "DOWN": 0.0})

    def test_a_late_outcome_from_an_old_market_cannot_set_the_new_one(self):
        plan = {"target": 10.0, "carry_market": ("cond-1", self.m.window_end)}
        self.sn._carry_for("cond-2", self.m.window_end)
        self.sn._settle_carry(plan, "UP", 2.0)
        self.assertEqual(self.sn.carry, {"UP": 0.0, "DOWN": 0.0})

    def test_carry_is_in_the_summary_and_the_start_line(self):
        self.fire("UP", [(0.47, 6), (0.60, 100)])
        self.assertEqual(self.sn.summary()["carry"], {"UP": 4.0, "DOWN": 0.0})
        self.assertIn("FAK 10 sh (+ same-side unfilled carry, <= 20 sh) at ask+0.05", self.sn.lines()[0])


class PaperFOKUnchangedTests(PaperIOCBase):
    order_type = "FOK"

    def test_fok_keeps_its_old_sizing_and_never_carries(self):
        r = self.fire("UP", [(0.47, 6), (0.60, 100)])
        self.assertEqual(r["result"], "refused")                           # all or nothing
        self.assertNotIn("target_shares", r)
        self.assertAlmostEqual(r["amount"], 4.70, places=6)               # 10 x the ask, as before
        self.assertEqual(self.sn.carry, {"UP": 0.0, "DOWN": 0.0})
        self.assertNotIn("carry", self.sn.summary())
        self.assertNotIn("carry", self.sn.lines()[0])


class SpendTests(unittest.TestCase):
    def test_spend_walks_the_visible_levels_up_to_the_cap(self):
        spend = sniper_mode.Sniper.ioc_spend
        self.assertEqual(spend([(0.47, 100)], 10, 0.47, 0.52), 4.70)
        self.assertEqual(spend([(0.47, 5), (0.50, 100)], 10, 0.47, 0.52), 4.85)
        self.assertEqual(spend([(0.47, 6), (0.60, 100)], 10, 0.47, 0.52), 4.70)   # past the cap: priced at the ask
        self.assertEqual(spend([(0.47, 3.3), (0.48, 100)], 10, 0.47, 0.52), 4.77)  # 4.767, up to the cent
        self.assertEqual(spend([(0.47, 100)], 14, 0.47, 0.52), 6.58)

    def test_invalid_target_cap_is_named(self):
        with self.assertRaisesRegex(ValueError, r"SNIPER_MAX_TARGET_SHARES=8 \(must be >= SNIPER_SHARES=10\)"):
            settings(SNIPER_MAX_TARGET_SHARES="8")


# ---------------------------------------------------------------- live
class FakeLive:
    """The surface Sniper uses of sniper_live.LiveExecution, answering as the venue would."""

    def __init__(self):
        self.reply = {"kind": "NO_MATCH", "order_id": "o1"}
        self.fills = {}
        self.sent = []

    def attach(self, sniper):
        pass

    def prepare(self, plan, decided_pc=None):
        self.sent.append(plan)
        return types.SimpleNamespace(amount=plan["amount"], cap=plan["cap"], price=plan["cap"], order_type="FAK",
                                     client_order_id="c", estimate={"shares": plan["target"], "price": 0.47,
                                                                    "fee": 0.0})

    def start_send(self, p):
        return None

    async def send(self, p, started=None):
        return dict(self.reply)

    def fill_for(self, order_id):
        return self.fills.get(order_id)

    def status(self):
        return {}

    def limits_line(self):
        return ""


def live_fill(shares, price=0.47):
    return {"shares": shares, "price": price, "cost": round(shares * price, 6), "fee": 0.0,
            "partial": shares < 10, "source": "private channel", "statuses": ["MATCHED"]}


class LiveIOCTests(unittest.TestCase):
    def setUp(self):
        self.m = Market(time.time() + 120)
        shim = types.SimpleNamespace(min_buy_price=0.05, max_buy_price=0.95, trade_window_seconds=300.0,
                                     min_seconds_to_expiry=10.0, own_fill_ttl_s=0,
                                     _net_of_own_fills=lambda b: b, has_recent_own_fill=lambda t: False)
        self.live = FakeLive()
        self.sn = sniper_mode.Sniper(self.m, shim, settings=settings(), live=self.live)
        self.rows = []
        self.sn._write = self.rows.append

    def fire(self, side="UP"):
        asyncio.run(self.sn._fire(side, 1.0, time.monotonic() * 1000.0))

    def test_live_partial_fill_carries_the_rest(self):
        self.live.reply = {"kind": "MATCHED", "order_id": "o1", "fill": live_fill(6.0)}
        self.fire()
        self.assertEqual(self.sn.carry["UP"], 4.0)
        self.assertEqual(self.sn.rounds[self.m.window_end]["UP"], 6.0)    # actual shares count toward the caps
        self.live.reply = {"kind": "MATCHED", "order_id": "o2", "fill": live_fill(14.0)}
        self.fire()
        self.assertEqual(self.live.sent[-1]["target"], 14.0)
        self.assertEqual(self.sn.carry["UP"], 0.0)

    def test_live_no_match_is_a_zero_fill_carrying_the_target(self):
        self.live.reply = {"kind": "NO_MATCH", "order_id": "o1"}
        self.fire()
        self.assertEqual(self.sn.carry["UP"], 10.0)
        self.assertEqual(self.sn.n["filled"], 0)
        self.assertEqual(self.sn.rounds[self.m.window_end]["fills"], 0)

    def test_live_zero_quantity_fak_update_is_not_a_fill(self):
        self.live.reply = {"kind": "MATCHED", "order_id": "o1", "fill": live_fill(0.0)}
        self.fire()
        self.assertEqual(self.sn.n["filled"], 0)
        self.assertEqual(self.sn.rounds[self.m.window_end]["fills"], 0)
        self.assertEqual(self.sn.carry["UP"], 10.0)

    def test_live_unknown_outcomes_leave_the_carry_alone(self):
        self.live.reply = {"kind": "MATCHED", "order_id": "o1", "fill": live_fill(6.0)}
        self.fire()
        for kind in ("REJECTED", "AMBIGUOUS", "NOT_SENT"):
            self.live.reply = {"kind": kind, "order_id": None, "error": "test"}
            self.fire()
            self.assertEqual(self.sn.carry["UP"], 4.0, kind)

    def test_live_pending_quantity_settles_the_carry_when_it_arrives(self):
        self.live.reply = {"kind": "PENDING", "order_id": "o9"}
        self.fire()
        self.assertIn("o9", self.sn._fak_pending)
        self.assertEqual(self.sn.carry["UP"], 0.0)                         # not known yet
        self.live.fills["o9"] = live_fill(7.0)
        self.sn._sync_live_fak()
        self.assertEqual(self.sn.carry["UP"], 3.0)
        self.assertAlmostEqual(self.sn.rounds[self.m.window_end]["UP"], 7.0)

    def test_live_carry_stays_on_its_side(self):
        self.live.reply = {"kind": "MATCHED", "order_id": "o1", "fill": live_fill(6.0)}
        self.fire("UP")
        self.live.reply = {"kind": "NO_MATCH", "order_id": "o2"}
        self.fire("DOWN")
        self.assertEqual(self.live.sent[-1]["target"], 10.0)
        self.assertEqual(self.sn.carry, {"UP": 4.0, "DOWN": 10.0})


if __name__ == "__main__":
    unittest.main(verbosity=2)
