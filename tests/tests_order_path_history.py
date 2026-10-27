#!/usr/bin/env python3
"""The order path no longer slows down as the session's history grows (2026-10-25).

The live loss budget reads the realized P/L since the canary session began - three
times per order and on every status read. The VPS session had been open 10 days, so
each read walked every position settled since, and decided -> wire went from 3.9ms
(10-15) to 8.3ms (10-25). The ledger now keeps realized_since() per session start
until a settlement changes what the walk would see; the live executor indexes its
FAK orders by token instead of walking every one it ever sent on each order.

These checks hold the kept value exact against a brute-force sum through
settlements, late fills, exits, a failed persist, a reload and an in-place edit;
show the walk runs once per change instead of once per read; and hold the FAK
index to the dict it indexes, in prepare() too. Temp files only, no network.

    python tests_order_path_history.py
"""
from __future__ import annotations

import pathlib
import random
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_live as L  # noqa: E402
import tests_live_20261014_incident as Inc  # noqa: E402
import tests_live_20261016_cash_freeze as CF  # noqa: E402
from accounting import Ledger  # noqa: E402
from accounting.resolution import RESOLVED, Resolution  # noqa: E402


def brute_realized(led: Ledger, since: float) -> float:
    """The definition: every position settled at or after `since`, summed from scratch."""
    with led._lock:
        return sum(float(p.realized or 0) for p in led.positions.values()
                   if p.settled and p.settled_wall is not None and float(p.settled_wall) >= since)


def cond(i: int) -> str:
    return "0x" + f"{i + 1:064x}"


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(pathlib.Path(self.tmp.name) / "ledger.json")
        self.led = Ledger(path=self.path, allow_sells=True)
        self.start = time.time() - 1.0
        self.n = 0

    def buy(self, token, condition, shares=10.0, price=0.47, order_id=None):
        self.n += 1
        self.assertTrue(self.led.record_fill(f"t{self.n}", token, shares=shares, price=price,
                                             condition_id=condition, fee=0.01,
                                             order_id=order_id or f"o{self.n}"))

    def sell(self, token, condition, shares, price=0.6):
        self.n += 1
        return self.led.record_fill(f"t{self.n}", token, shares=shares, price=price, side="SELL",
                                    condition_id=condition, fee=0.01, order_id=f"o{self.n}")

    def settle(self, i, up_wins=True, half=False):
        payouts = ({f"{i}u": 0.5, f"{i}d": 0.5} if half else
                   {f"{i}u": 1.0 if up_wins else 0.0, f"{i}d": 0.0 if up_wins else 1.0})
        return self.led.settle(Resolution(cond(i), RESOLVED, payouts))

    def assert_exact(self, *sinces):
        for since in sinces or (self.start, 0.0):
            want = brute_realized(self.led, since)
            self.assertAlmostEqual(self.led.realized_since(since), want, places=9, msg=f"since {since}")
            self.assertAlmostEqual(self.led.risk_view(since, [])[0], want, places=9, msg=f"since {since}")


# ============================================================ exact through every change
class RealizedSinceStaysExact(LedgerCase):
    def test_random_history_of_fills_exits_settlements_and_late_fills(self):
        rnd = random.Random(20261025)
        open_markets: list[int] = []
        settled_markets: list[int] = []
        nxt = 0
        mid = None
        for step in range(400):
            op = rnd.random()
            if op < 0.35 or not open_markets:
                i = rnd.choice(open_markets) if open_markets and rnd.random() < 0.4 else nxt
                if i == nxt:
                    nxt += 1
                    open_markets.append(i)
                self.buy(f"{i}{rnd.choice('ud')}", cond(i), shares=round(rnd.uniform(1, 20), 2),
                         price=round(rnd.uniform(0.05, 0.95), 2))
            elif op < 0.55:
                i = rnd.choice(open_markets)
                pos = self.led.positions.get(f"{i}u") or self.led.positions.get(f"{i}d")
                if pos is not None and not pos.settled and pos.shares > 0:
                    self.sell(pos.token_id, cond(i), rnd.choice([pos.shares, round(pos.shares / 2, 2)]))
            elif op < 0.85:
                i = open_markets.pop(rnd.randrange(len(open_markets)))
                for token in (f"{i}u", f"{i}d"):
                    if token not in self.led.positions:
                        self.buy(token, cond(i), shares=1.0, price=0.5)
                self.settle(i, up_wins=rnd.random() < 0.5, half=rnd.random() < 0.05)
                settled_markets.append(i)
            elif settled_markets:
                i = rnd.choice(settled_markets)                               # a fill after settlement
                self.buy(f"{i}{rnd.choice('ud')}", cond(i), shares=2.0, price=0.4)
                if i not in open_markets:
                    open_markets.append(i)
                settled_markets.remove(i)
            if step == 200:
                mid = time.time()
            self.assert_exact(self.start, 0.0, *(() if mid is None else (mid,)))
        self.assertGreater(len(self.led._settled), 50)

    def test_a_settlement_that_cannot_be_persisted_rolls_back_in_the_kept_value_too(self):
        self.buy("0u", cond(0))
        self.buy("0d", cond(0))
        self.buy("1u", cond(1))
        self.buy("1d", cond(1))
        self.settle(0)
        self.assert_exact()                                                   # kept, with market 0 in it
        with patch.object(self.led, "_persist_locked", return_value=False):
            with self.assertRaisesRegex(RuntimeError, "could not be persisted"):
                self.settle(1, up_wins=False)
        self.assert_exact()
        self.assertFalse(self.led.positions["1u"].settled)
        self.settle(1, up_wins=False)
        self.assert_exact()

    def test_a_reload_and_a_reopened_ledger_read_the_same(self):
        for i in range(6):
            self.buy(f"{i}u", cond(i))
            self.buy(f"{i}d", cond(i), price=0.5)
            if i < 5:
                self.settle(i, up_wins=i % 2 == 0)
        self.assertTrue(self.led.save())
        want = brute_realized(self.led, self.start)
        self.assert_exact()
        self.assertTrue(self.led.load())
        self.assert_exact()
        self.assertAlmostEqual(Ledger(path=self.path, allow_sells=True).realized_since(self.start), want, places=9)

    def test_an_in_place_edit_is_seen_after_invalidate_caches(self):
        self.buy("0u", cond(0))
        self.buy("0d", cond(0))
        self.settle(0)
        before = self.led.realized_since(self.start)
        self.led.positions["0u"].realized += 42.0                             # behind the ledger's back
        self.assertAlmostEqual(self.led.realized_since(self.start), before, places=9)
        self.led.invalidate_caches()
        self.assert_exact()
        self.assertAlmostEqual(self.led.realized_since(self.start), before + 42.0, places=9)

    def test_positions_settled_behind_the_ledger_are_indexed_and_counted_at_the_next_read(self):
        self.buy("0u", cond(0))
        self.assert_exact()
        pos = self.led.positions["0u"]
        pos.settled, pos.settled_wall, pos.payout_per_share = True, time.time(), 1.0
        pos.realized = pos.shares - pos.cost                                  # as the benches seed them
        self.assert_exact()


# ============================================================ the walk: once per change
class TheWalkRunsOncePerChangeNotPerRead(LedgerCase):
    def setUp(self):
        super().setUp()
        for i in range(300):
            self.buy(f"{i}u", cond(i))
            self.buy(f"{i}d", cond(i), price=0.5)
            self.settle(i, up_wins=i % 3 == 0)
        self.walks = 0
        real = Ledger._realized_walk_locked

        def counting(led, since):
            self.walks += 1
            return real(led, since)
        patcher = patch.object(Ledger, "_realized_walk_locked", counting)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_reads_between_settlements_reuse_one_walk(self):
        want = brute_realized(self.led, self.start)
        for _ in range(3 * 200):                                              # 3 reads per order, 200 orders
            self.assertAlmostEqual(self.led.risk_view(self.start, [("o1", "0u")])[0], want, places=9)
        self.assertEqual(self.walks, 1)

    def test_an_open_fill_does_not_redo_it_and_a_settlement_does_once(self):
        self.led.realized_since(self.start)
        self.buy("900u", cond(900), shares=5.0, price=0.4)                    # a new open position
        realized, open_basis, _ = self.led.risk_view(self.start, [])
        self.assertEqual(self.walks, 1)
        self.assertAlmostEqual(open_basis, 5.0 * 0.4 + 0.01, places=9)       # the open side is always fresh
        self.buy("900d", cond(900), shares=5.0, price=0.6)
        self.settle(900, up_wins=True)
        for _ in range(50):
            self.assertAlmostEqual(self.led.realized_since(self.start), brute_realized(self.led, self.start), places=9)
        self.assertEqual(self.walks, 2)

    def test_two_session_starts_are_kept_apart(self):
        later = self.led.positions["150u"].settled_wall
        a, b = self.led.realized_since(self.start), self.led.realized_since(later)
        for _ in range(20):
            self.assertEqual((self.led.realized_since(self.start), self.led.realized_since(later)), (a, b))
        self.assertEqual(self.walks, 2)
        self.assertAlmostEqual(b, brute_realized(self.led, later), places=9)

    def test_a_kept_read_is_much_cheaper_than_the_walk(self):
        self.led.realized_since(self.start)
        n = 300
        t = time.perf_counter_ns()
        for _ in range(n):
            self.led.risk_view(self.start, [])
        kept = (time.perf_counter_ns() - t) / n
        t = time.perf_counter_ns()
        for _ in range(n):
            self.led.invalidate_caches()
            self.led.risk_view(self.start, [])
        walked = (time.perf_counter_ns() - t) / n
        print(f"risk_view over 600 settled positions: kept {kept / 1000:.1f}us, walked {walked / 1000:.1f}us",
              file=sys.stderr)
        self.assertLess(kept, walked / 2)


# ============================================================ the FAK index
def bare_execution():
    ex = L.LiveExecution.__new__(L.LiveExecution)
    ex.fak_orders = {}
    ex._fak_lock = threading.Lock()
    ex._fak_index = None
    ex._fak_indexed = None
    return ex


class FakIndex(unittest.TestCase):
    def test_one_tokens_orders_oldest_first_and_nothing_else(self):
        ex = bare_execution()
        for oid, token in (("a1", "A"), ("b1", "B"), ("a2", "A"), ("c1", 123)):
            ex._put_fak(oid, {"token": token})
        self.assertEqual(ex._faks_on_token("A"), ["a1", "a2"])
        self.assertEqual(ex._faks_on_token("B"), ["b1"])
        self.assertEqual(ex._faks_on_token("123"), ["c1"])
        self.assertEqual(ex._faks_on_token(123), ["c1"])
        self.assertEqual(ex._faks_on_token("Z"), [])
        ex._put_fak("a1", {"token": "A", "final_size": 3.0})                 # an update is not a second entry
        self.assertEqual(ex._faks_on_token("A"), ["a1", "a2"])
        ex._faks_on_token("A").append("junk")                                 # callers get a copy
        self.assertEqual(ex._faks_on_token("A"), ["a1", "a2"])

    def test_direct_inserts_and_a_replaced_dict_are_picked_up(self):
        ex = bare_execution()
        ex._put_fak("a1", {"token": "A"})
        self.assertEqual(ex._faks_on_token("A"), ["a1"])
        ex.fak_orders["a2"] = {"token": "A"}                                  # behind _put_fak, as tests do
        self.assertEqual(ex._faks_on_token("A"), ["a1", "a2"])
        ex._put_fak("a3", {"token": "A"})
        self.assertEqual(ex._faks_on_token("A"), ["a1", "a2", "a3"])
        ex.fak_orders = {"x1": {"token": "X"}, "x2": {"token": "X"}, "a9": {"token": "A"}}  # same size, new dict
        self.assertEqual(ex._faks_on_token("A"), ["a9"])
        self.assertEqual(ex._faks_on_token("X"), ["x1", "x2"])

    def test_concurrent_puts_from_workers_lose_nothing(self):
        ex = bare_execution()
        ex._faks_on_token("warm")

        def worker(w):
            for i in range(400):
                ex._put_fak(f"{w}-{i}", {"token": f"T{i % 7}"})
                if i % 50 == 0:
                    ex._faks_on_token(f"T{i % 7}")
        threads = [threading.Thread(target=worker, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for k in range(7):
            want = sorted(oid for oid, a in ex.fak_orders.items() if a["token"] == f"T{k}")
            self.assertEqual(sorted(ex._faks_on_token(f"T{k}")), want)
        self.assertEqual(len(ex.fak_orders), 1600)


class CountingDict(dict):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.walks = 0

    def items(self):
        self.walks += 1
        return super().items()


class PrepareReadsOnlyItsTokensFaks(unittest.IsolatedAsyncioTestCase):
    asyncSetUp, asyncTearDown, plan = Inc.Live.asyncSetUp, Inc.Live.asyncTearDown, Inc.Live.plan

    def pending(self, oid, token):
        return {"wall": time.time(), "token": token, "condition": "market-x", "client_order_id": f"k-{oid}",
                "kind": L.MATCHED, "amount": 3.0}

    async def test_a_session_of_faks_on_other_tokens_is_not_walked_per_order(self):
        T = self.T
        self.ex.fak_orders = CountingDict()
        for i in range(2000):                                                 # a long session's FAKs, all settled
            self.ex._put_fak(f"0xold{i}", {**self.pending(f"0xold{i}", f"token-{i % 40}"), "final_size": 0.0,
                                           "final_mono": time.monotonic()})
        walks = self.ex.fak_orders.walks
        self.ex.prepare(self.plan(3.0, side="UP", cap=CF.CAP, ask=CF.PRICE))
        for _ in range(200):                                                  # what each later order asks
            self.assertEqual(self.ex._faks_on_token(T.UP), [])
        self.assertLessEqual(self.ex.fak_orders.walks - walks, 1)            # at most the one index build
        self.ex._put_fak("0xup", self.pending("0xup", T.UP))                  # now one pending on UP itself
        with self.assertRaisesRegex(L.Refused, "awaiting its fill size"):
            self.ex.prepare(self.plan(3.0, side="UP", cap=CF.CAP, ask=CF.PRICE))
        self.ex.prepare(self.plan(3.0, side="DOWN", cap=CF.CAP, ask=CF.PRICE))
        self.assertLessEqual(self.ex.fak_orders.walks - walks, 1)

    async def test_a_pending_fak_inserted_after_the_index_was_built_still_blocks_its_token(self):
        T = self.T
        self.ex.prepare(self.plan(3.0, side="UP", cap=CF.CAP, ask=CF.PRICE))   # builds the (empty) index
        self.ex.fak_orders["0xlate"] = self.pending("0xlate", T.UP)            # directly, behind _put_fak
        with self.assertRaisesRegex(L.Refused, "awaiting its fill size"):
            self.ex.prepare(self.plan(3.0, side="UP", cap=CF.CAP, ask=CF.PRICE))
        self.assertFalse(self.ex.kill.tripped, self.ex.kill.reasons)


if __name__ == "__main__":
    unittest.main(verbosity=1)
