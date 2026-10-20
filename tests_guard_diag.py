#!/usr/bin/env python3
"""Guard diagnostics (2026-10-15): what an opposite-side / whipsaw guard would
have done with each sent order, and the tighter-limit shadow. Diagnostics only:
no order may change. No network, no wallet.

    python tests_guard_diag.py

- whipsaw_diag: the paired cost C, the sure loss on paired shares, the
  worst-case round loss before / after, the 500 / 1000 ms cooldowns since the
  opposite fill, and the netted fair when the market had not repriced;
- the paper sniper journals one guard_diag row per sent order, joined by id,
  from its own confirmed fills - and orders and fills are unchanged by it
  (SNIPER_GUARD_DIAG=0 writes none);
- the shadow prices a tighter limit (ask + 3c under the real cap);
- guard_eval.py: the net incremental P&L per eligible signal, per period.
"""
from __future__ import annotations

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
import guard_eval as G  # noqa: E402
import sniper_mode as S  # noqa: E402
from paper_trade import MarketRules  # noqa: E402

NOW = 1_800_000_000.0
DIAG = dict(fee_rate=0.07, fee_exponent=1, now_wall=NOW, market_mid=None, sigma=None, tau=None, bp=1.0,
            tick=0.01, margin=0.01, mine=None, opp=None, prev_opp=None)


class WhipsawDiag(unittest.TestCase):
    def test_signed_fair_matches_the_unsigned_one_and_falls_against_the_side(self):
        for mid, bp in ((0.3, 1.2), (0.55, 0.6), (0.9, 2.5)):
            self.assertAlmostEqual(S.signed_anchored_fair(mid, bp, 4e-5, 120), S.anchored_fair(mid, bp, 4e-5, 120), 12)
            self.assertLess(S.signed_anchored_fair(mid, -bp, 4e-5, 120), mid)

    def test_no_opposite_fill_pairs_nothing(self):
        out = S.whipsaw_diag(side="UP", ask=0.40, shares=10, **DIAG)
        self.assertEqual(out["paired_shares"], 0.0)
        self.assertNotIn("pair_cost", out)
        self.assertFalse(any(out["guards"].values()))
        self.assertEqual(set(out["guards"]), {"c1", "c1_pending", "cd500", "cd1000", "cdo500", "cdo1000", "net"})
        fee = 0.07 * 0.4 * 0.6
        self.assertAlmostEqual(out["wc_after"], 10 * (0.40 + fee), places=4)     # all of it can be lost

    def test_paired_cost_over_a_dollar_is_a_sure_loss_on_those_shares(self):
        opp = {"shares": 10.0, "cost": 8.10, "last_wall": NOW - 5}                # 10 DOWN at 0.81 incl. fees
        out = S.whipsaw_diag(side="UP", ask=0.20, shares=10, **{**DIAG, "opp": opp})
        fee = 0.07 * 0.2 * 0.8
        c = 0.81 + 0.20 + fee
        self.assertAlmostEqual(out["pair_cost"], c, places=4)
        self.assertTrue(out["guards"]["c1"])
        self.assertAlmostEqual(out["locked_loss"], (c - 1) * 10, places=4)
        self.assertAlmostEqual(out["wc_before"], 8.10, places=4)                    # DOWN loses: all its cost
        self.assertAlmostEqual(out["wc_after"], out["locked_loss"], places=4)       # fully paired: the sure loss
        cheap = S.whipsaw_diag(side="UP", ask=0.10, shares=10, **{**DIAG, "opp": opp})
        self.assertFalse(cheap["guards"]["c1"])                                     # C < 1: a locked profit

    def test_only_unpaired_opposite_shares_pair(self):
        out = S.whipsaw_diag(side="UP", ask=0.2, shares=10, **{**DIAG, "opp": {"shares": 8.0, "cost": 6.0, "last_wall": NOW},
                                                               "mine": {"shares": 5.0, "cost": 2.0, "last_wall": NOW}})
        self.assertEqual(out["paired_shares"], 3.0)                                 # 8 DOWN - 5 UP already paired

    def test_an_opposite_order_still_in_flight_counts_as_pending(self):
        """The live whipsaws: the second order was decided 0.3s after the first,
        before the first had even replied - no confirmed fill to pair with."""
        pend = {"shares": 8.09, "cost": 8.09 * 0.93}                                # UP in flight at 0.93
        out = S.whipsaw_diag(side="DOWN", ask=0.09, shares=11.11, **{**DIAG, "opp_pending": pend})
        self.assertEqual(out["paired_shares"], 0.0)                                 # nothing confirmed yet
        self.assertFalse(out["guards"]["c1"])
        self.assertAlmostEqual(out["paired_incl_pending"], 8.09, places=4)
        fee_up, fee_dn = 0.07 * 0.93 * 0.07, 0.07 * 0.09 * 0.91
        self.assertAlmostEqual(out["pair_cost_incl_pending"], 0.93 + fee_up + 0.09 + fee_dn, places=4)
        self.assertTrue(out["guards"]["c1_pending"])                                 # >= $1: a sure loss

    def test_order_cooldowns_count_from_the_opposite_order(self):
        for ago, a, b in ((0.3, True, True), (0.8, False, True), (1.5, False, False)):
            prev = {"wall": NOW - ago, "bp": 1.0, "market_mid": None, "feed": "coinbase"}
            out = S.whipsaw_diag(side="DOWN", ask=0.5, shares=5, **{**DIAG, "prev_opp": prev})
            self.assertEqual((out["guards"]["cdo500"], out["guards"]["cdo1000"]), (a, b), ago)

    def test_cooldowns_count_from_the_opposite_fill(self):
        for ago, cd500, cd1000 in ((0.4, True, True), (0.7, False, True), (1.2, False, False)):
            out = S.whipsaw_diag(side="DOWN", ask=0.5, shares=5,
                                 **{**DIAG, "opp": {"shares": 5.0, "cost": 2.6, "last_wall": NOW - ago}})
            self.assertEqual((out["guards"]["cd500"], out["guards"]["cd1000"]), (cd500, cd1000), ago)
            self.assertAlmostEqual(out["dt_opp_fill_ms"], ago * 1000, places=3)

    def test_an_unrepriced_market_nets_the_two_moves(self):
        sigma, tau = 0.391 / 1e4, 150.0
        prev = {"wall": NOW - 0.5, "bp": +0.57, "market_mid": 0.745, "feed": "coinbase"}
        args = {**DIAG, "prev_opp": prev, "market_mid": 0.255, "sigma": sigma, "tau": tau, "bp": -1.45}
        bot_fair = S.anchored_fair(0.255, 1.45, sigma, tau)
        ask = 0.31                       # under the bot's own limit (0.33), over the netted one (0.29)
        out = S.whipsaw_diag(side="DOWN", ask=ask, shares=10, **args)
        self.assertTrue(out["unrepriced"])
        netted = S.signed_anchored_fair(0.255, 1.45 - 0.57, sigma, tau)
        self.assertAlmostEqual(out["netted_fair"], netted, places=4)
        self.assertLess(out["netted_fair"], bot_fair)
        self.assertEqual(out["netted_limit"], S.fair_limit(netted, 0.01, 0.01, 0.07, 1))
        self.assertEqual(out["guards"]["net"], ask > out["netted_limit"])
        self.assertLessEqual(ask, S.fair_limit(bot_fair, 0.01, 0.01, 0.07, 1))     # the bot's own limit allowed it
        self.assertTrue(out["guards"]["net"])
        self.assertAlmostEqual(out["dt_opp_decision_ms"], 500.0, places=3)

    def test_a_repriced_market_is_not_netted(self):
        prev = {"wall": NOW - 0.5, "bp": +0.57, "market_mid": 0.745, "feed": "coinbase"}
        out = S.whipsaw_diag(side="DOWN", ask=0.2, shares=10,
                             **{**DIAG, "prev_opp": prev, "market_mid": 0.20, "sigma": 4e-5, "tau": 150.0, "bp": -1.45})
        self.assertFalse(out["unrepriced"])
        self.assertNotIn("netted_fair", out)
        self.assertFalse(out["guards"]["net"])


def _settings(**env):
    base = {"SNIPER_ONLY": "1", "SNIPER_LATENCY_MS": "200", "SNIPER_SLIP": "0.02", "SNIPER_FAIR": "0",
            "SNIPER_KELLY": "0", "SNIPER_SHADOW_MS": ""}
    with patch.dict(os.environ, {**base, **env}):
        return S.Settings()


class View:
    status, asks, bids, best_ask, mid, tick_size = "LIVE", ((0.52, 100.0),), ((0.50, 100.0),), 0.52, 0.51, 0.01

    def __init__(self):
        self.updated_mono = time.monotonic()


class Hub:
    up_token, down_token, condition_id = "up", "down", "cond"

    def __init__(self):
        self.window_end = time.time() + 120
        self.book = types.SimpleNamespace(view=lambda token: View())


class Broker:
    # The default FAK sizing (SNIPER_ORDER_SIZING=shares) hands the worker a
    # share-sized order; Sniper._match refuses a broker that cannot take one.
    supports_share_sized = True

    def __init__(self):
        self.last_fill = self.last_error = None

    def place_trade(self, side, amount, up, down, cond, end, max_price, min_price, *, latency_ms=None, tag=None, **kw):
        self.last_fill = {"tag": tag, "order_id": f"o-{side}", "shares": 10.0, "average_price": 0.52,
                          "fee": 0.17, "total_cost": 5.37}
        return True


class PaperWiring(unittest.TestCase):
    def run_two(self, **env):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        journal = pathlib.Path(tmp.name) / "paper_orders_sniper.jsonl"
        sn = S.Sniper(Hub(), Broker(), settings=_settings(**env), journal_path=str(journal))

        async def go():
            await sn._fire("UP", 1.2, time.monotonic() * 1000.0)
            await sn._fire("DOWN", -1.3, time.monotonic() * 1000.0)
            for _ in range(5):
                await asyncio.sleep(0)                 # the diagnostics run off the order path
        asyncio.run(go())
        return [json.loads(x) for x in journal.read_text(encoding="utf-8").splitlines()]

    def test_each_sent_order_gets_one_guard_diag_row_from_confirmed_fills(self):
        rows = self.run_two()
        orders = [r for r in rows if r.get("result") == "filled"]
        diags = {r["id"]: r for r in rows if r.get("result") == "guard_diag"}
        self.assertEqual([o["side"] for o in orders], ["UP", "DOWN"])
        self.assertEqual(set(diags), {o["id"] for o in orders})
        up, down = diags[orders[0]["id"]], diags[orders[1]["id"]]
        self.assertEqual(up["paired_shares"], 0.0)
        self.assertFalse(any(up["guards"].values()))
        self.assertEqual(down["held_opp"], 10.0)                 # the UP fill, confirmed, before DOWN was sent
        self.assertGreater(down["paired_shares"], 0)
        # fair value is off in this harness, so no fee rate was read: flagged, not guessed
        self.assertFalse(down["fee_known"])
        self.assertAlmostEqual(down["pair_cost"], 5.37 / 10 + 0.52, places=4)
        self.assertTrue(down["guards"]["c1"])                    # 0.537 + 0.52 + fee >= 1
        self.assertTrue(down["guards"]["cd500"])                 # the UP fill was moments before
        self.assertTrue(down["guards"]["cdo500"])                # and so was the UP order
        for o in orders:                                         # the orders themselves carry nothing new
            self.assertFalse({"guards", "pair_cost", "paired_shares"} & set(o))

    def test_off_switch_writes_no_diagnostics_and_the_same_orders(self):
        on = [r for r in self.run_two() if r.get("result") == "filled"]
        rows = self.run_two(SNIPER_GUARD_DIAG="0")
        self.assertFalse([r for r in rows if r.get("result") == "guard_diag"])
        off = [r for r in rows if r.get("result") == "filled"]
        keys = ("side", "shares", "price", "fee", "cost", "cap", "amount")
        self.assertEqual([{k: r.get(k) for k in keys} for r in on], [{k: r.get(k) for k in keys} for r in off])


class TightShadow(unittest.TestCase):
    def test_the_tighter_limit_is_priced_fok_then_fak(self):
        from feeds.book import BookView
        sn = S.Sniper(None, None, settings=_settings())
        sn.broker = types.SimpleNamespace(max_spread=0.25, max_buy_price=0.95, min_buy_price=0.05)
        rules = MarketRules("cond", D("0.07"), 1, min_order_size=None, tick_size=D("0.01"), source="venue",
                            up_token_id="up", down_token_id="down")
        book = BookView("up", 1, "LIVE", bids=((0.49, 50.0),), asks=((0.50, 4.0), (0.53, 40.0), (0.55, 50.0)),
                        updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)
        cap, ask = 0.55, 0.50
        tc = round(ask + 0.03, 4)
        r = sn._shadow_fill(book, "up", 5.5, cap, rules, 0.6, 0.0, None, tight=(tc, 5.5 * tc / cap))
        self.assertIn("sh", r)                                   # the real cap fills
        self.assertEqual(r["tight"]["cap"], 0.53)
        self.assertIn("sh", r["tight"])                          # 0.50 x4 + 0.53: still fills under 0.53
        thin = BookView("up", 1, "LIVE", bids=((0.49, 50.0),), asks=((0.50, 4.0), (0.55, 50.0)),
                        updated_mono=time.monotonic(), exchange_ts_ms=int(time.time() * 1000), tick_size=0.01)
        r2 = sn._shadow_fill(thin, "up", 5.5, cap, rules, 0.6, 0.0, None, tight=(tc, 5.5 * tc / cap))
        self.assertIn("miss", r2["tight"])                       # FOK misses at 0.53 ...
        self.assertAlmostEqual(r2["tight"]["fak"]["sh"], 4.0, places=6)   # ... FAK keeps the 4 at 0.50

    def test_settings(self):
        self.assertEqual(_settings().shadow_tight, 0.03)
        self.assertEqual(_settings(SNIPER_SHADOW_TIGHT="0").shadow_tight, 0.0)
        self.assertTrue(_settings().guard_diag)
        self.assertFalse(_settings(SNIPER_GUARD_DIAG="off").guard_diag)


class Evaluator(unittest.TestCase):
    def write(self, rows, settlements):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        d = pathlib.Path(tmp.name)
        (d / "paper_orders_sniper.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        (d / "paper_ledger_settlements.jsonl").write_text(
            "".join(json.dumps({"event": "RESULT_RECEIVED", "condition_id": c, "winner": w}) + "\n"
                    for c, w in settlements.items()), encoding="utf-8")
        return str(d)

    def test_net_incremental_pnl_per_signal(self):
        t0 = NOW
        rows = []
        for i, (side, cond, price, guard) in enumerate((("UP", "c1", 0.50, False), ("DOWN", "c1", 0.60, True),
                                                        ("UP", "c2", 0.40, False))):
            rid = f"id{i}"
            rows += [{"wall": t0 + i, "id": rid, "side": side, "bp": 1.0, "condition_id": cond, "result": "filled",
                      "shares": 10, "price": price, "fee": 0.1, "latency_ms": 200},
                     {"wall": t0 + i, "result": "guard_diag", "id": rid, "guards": {"c1": guard, "cd500": False,
                                                                                    "cd1000": False, "net": False}}]
        d = self.write(rows, {"c1": "UP", "c2": "DOWN"})
        orders = G.load(d, os.path.join(d, "paper_orders_sniper.jsonl"), None, None)
        items = G.compare(orders, "c1")
        base = sum(i["base"] for i in items)
        alt = sum(i["alt"] for i in items)
        # UP c1 wins: 10*(1-0.5)-0.1 = 4.9; DOWN c1 loses: -6.1 (rejected by c1); UP c2 loses: -4.1
        self.assertAlmostEqual(base, 4.9 - 6.1 - 4.1, places=9)
        self.assertAlmostEqual(alt, 4.9 - 4.1, places=9)
        self.assertAlmostEqual(100 * (alt - base) / len(items), 610 / 3, places=6)
        self.assertEqual(G.compare(orders, "tight"), [])     # no shadow rows: no limit comparison

    def test_tight_and_wide_limits_use_the_shadow_at_the_orders_own_delay(self):
        rid = "x"
        rows = [{"wall": NOW, "id": rid, "side": "UP", "bp": 1.0, "condition_id": "c", "result": "refused",
                 "latency_ms": 260},
                {"wall": NOW, "result": "shadow", "id": rid, "side": "UP", "token": "tu", "other_token": "td",
                 "L": {"200": {"sh": 10, "px": 0.5, "fee": 0.1,
                               "tight": {"cap": 0.53, "miss": "FOK no-fill", "fak": {"miss": "FAK no-fill"}}},
                       "300": {"miss": "FOK no-fill", "fak": {"miss": "FAK no-fill"},
                               "alt": {"3": {"sh": 9, "px": 0.6, "fee": 0.1}},
                               "tight": {"cap": 0.53, "miss": "FOK no-fill", "fak": {"sh": 4, "px": 0.52, "fee": 0.05}}}}}]
        d = self.write(rows, {"c": "UP"})
        orders = G.load(d, os.path.join(d, "paper_orders_sniper.jsonl"), None, None)
        self.assertEqual(G.compare(orders, "tight"), [])         # 260ms is not recorded
        self.assertEqual(G.compare(orders, "wide3"), [])
        orders[0]["d"]["latency_ms"] = 200
        tight = G.compare(orders, "tight")[0]
        self.assertAlmostEqual(tight["base"], 10 * 0.5 - 0.1)
        self.assertEqual(tight["alt"], 0.0)                      # the tighter limit misses there
        wide = G.compare(orders, "wide3")[0]
        self.assertAlmostEqual(wide["alt"], wide["base"])        # the real limit filled: nothing to add
        orders[0]["d"]["latency_ms"] = 300                       # exact recorded instant; the real limit missed
        wide = G.compare(orders, "wide3")[0]
        self.assertEqual(wide["base"], 0.0)
        self.assertAlmostEqual(wide["alt"], 9 * (1 - 0.6) - 0.1)
        tight = G.compare(orders, "tight")[0]
        self.assertAlmostEqual(tight["alt"], 4 * (1 - 0.52) - 0.05)   # what FAK keeps at the tighter limit


if __name__ == "__main__":
    unittest.main()
