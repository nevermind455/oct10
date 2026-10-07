"""Offline tests for sniper_dynexec: the model's arithmetic and the action
outcomes on a hand-made book.  python tests_dynexec.py"""
from __future__ import annotations

import math
import sys
import traceback

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import numpy as np

import sniper_dynexec as X

NAN = float("nan")


def a_of(kind, n, c):
    return X.ACTIONS.index((kind, float(n), c))


def fake_ctx(rows, side="UP", t0=1_000e9):
    """rows: [(t_offset_s, bb, ba, [(p, s), ...])], receipt time = venue true time."""
    n = len(rows)
    A = np.full((n, 5, 2), NAN)
    for j, (_dt, _bb, _ba, lv) in enumerate(rows):
        for q, (p, s) in enumerate(lv[:5]):
            A[j, q] = (p, s)
    t = np.array([t0 + dt * 1e9 for dt, *_ in rows])
    book = {"recv_wall": t, "ts": t / 1e6, "bb": np.array([r[1] for r in rows], float),
            "ba": np.array([r[2] for r in rows], float), "asks": A}
    return {"books": {side: book}, "vt": {side: t}, "heard": np.sort(t), "trades": {side: np.array([t0 - 0.5e9])}}


def test_choose_picks_best_sendable_and_skips():
    E = np.array([[0.10, 0.30, -0.1], [-0.2, -0.1, -0.3], [0.5, 0.2, 0.1]])
    S = np.array([[True, True, True], [True, True, True], [False, True, False]])
    dyn, never = X.choose(E, S)
    assert list(dyn) == [1, -1, 1], dyn
    assert list(never) == [1, 1, 1], never
    dyn, never = X.choose(E, np.zeros_like(S))
    assert list(dyn) == [-1, -1, -1] and list(never) == [-1, -1, -1]


def test_choose_ignores_nan():
    E = np.array([[NAN, 0.2]])
    dyn, _ = X.choose(E, np.array([[True, True]]))
    assert list(dyn) == [1]


def test_knn_never_uses_own_group():
    train = np.array([[0.0], [0.1], [5.0], [5.1]])
    groups = np.array([1, 1, 2, 2])
    nb = X.knn(train, train, 2, groups, groups)
    assert set(nb[0]) == {2, 3} and set(nb[2]) == {0, 1}, nb
    nb = X.knn(train, np.array([[0.05]]), 2)
    assert set(nb[0]) == {0, 1}


def test_expected_averages_neighbours_at_predicted_delays():
    T = np.full((3, len(X.GRID_MS), 2), NAN)
    T[0, 0], T[0, 4], T[1, 0], T[1, 4] = (1, 0), (3, 0), (5, 0), (7, 0)
    T[2] = 100                                            # not a neighbour
    E = X.expected(T, np.array([[0, 1]]), np.array([[0, 4]]))
    assert E.shape == (1, 2) and abs(E[0, 0] - 4.0) < 1e-9 and abs(E[0, 1]) < 1e-9, E
    T[1, 4, 0] = NAN                                      # unknown cells are left out, not zeros
    E = X.expected(T, np.array([[0, 1]]), np.array([[0, 4]]))
    assert abs(E[0, 0] - 3.0) < 1e-9, E


def test_delay_deciles_and_grid():
    lat = np.arange(100, dtype=float) * 10
    d = X.delay_deciles(lat, np.array([np.arange(100)]))
    assert d.shape == (1, 10) and 40 < d[0, 0] < 60 and 940 < d[0, -1] < 960, d
    assert list(X.grid_index([90, 160, 2000])) == [0, 1, len(X.GRID_MS) - 1]


def test_outcome_at_walks_every_action():
    # the order meets: 3 shares at 0.52, 20 at 0.53; 5s later the mid is 0.60
    ctx = fake_ctx([(0.0, 0.49, 0.52, [(0.52, 3.0), (0.53, 20.0)]),
                    (5.0, 0.59, 0.61, [(0.61, 50.0)])])
    o5, oS, f, sh, px, drift = X.outcome_at(ctx, "UP", 1_000e9 + 0.1e9, 0.50, 1.0)
    now, w3, fak = a_of("FOK", 10, 0.02), a_of("FOK", 10, 0.03), a_of("FAK", 10, 0.02)
    assert f[now] == 0 and o5[now] == 0 and oS[now] == 0          # 3 shares under 0.52: FOK 10 misses
    # $5.00 (10 shares at the ask seen) buys 3 at 0.52 and the rest at 0.53
    n53 = (5.0 - 3 * 0.52) / 0.53
    shares, vwap = 3 + n53, 5.0 / (3 + n53)
    assert f[w3] == 1 and abs(sh[w3] - shares) < 1e-4, sh[w3]
    assert abs(px[w3] - vwap) < 1e-6
    fee = sum(0.07 * p * (1 - p) * s for p, s in ((0.52, 3), (0.53, n53)))
    assert abs(o5[w3] - (shares * (0.60 - vwap) - fee)) < 1e-3, (o5[w3], shares * (0.60 - vwap) - fee)
    assert abs(oS[w3] - (shares * (1.0 - vwap) - fee)) < 1e-3
    assert f[fak] == 1 and abs(sh[fak] - 3.0) < 1e-6             # FAK takes the 3 shares there
    assert abs(drift - 2.0) < 1e-9


def test_outcome_at_feed_silent_is_unknown():
    ctx = fake_ctx([(0.0, 0.49, 0.52, [(0.52, 30.0)])])
    assert X.outcome_at(ctx, "UP", 1_000e9 + 3e9, 0.50, 1.0) is None


def test_outcome_without_settlement_or_markout():
    ctx = fake_ctx([(0.0, 0.49, 0.51, [(0.51, 30.0)])])
    o5, oS, f, *_ = X.outcome_at(ctx, "UP", 1_000e9 + 0.1e9, 0.50, None)
    now = a_of("FOK", 10, 0.02)
    assert f[now] == 1 and math.isnan(oS[now])                   # unsettled: unknown, never zero
    assert not math.isnan(o5[now])                               # 5s mid = the same book (no change)


def test_features_and_sendable():
    ctx = fake_ctx([(-1.5, 0.48, 0.50, [(0.50, 30.0)]), (-0.4, 0.49, 0.51, [(0.51, 4.0), (0.53, 30.0)]),
                    (-0.1, 0.49, 0.51, [(0.51, 4.0), (0.53, 30.0)])])
    x, S = X.features(ctx, "UP", 1_000e9, 0.51, 1.3, 120.0)
    assert x["log_act1"] == math.log1p(2) and abs(x["dask1"] - 1.0) < 1e-9 and abs(x["spread"] - 2.0) < 1e-9
    assert x["log_trades2"] == math.log1p(1)
    assert abs(x["depth2"] - 34.0 / 10.0) < 1e-9                 # 4 + 30 shares at or under ask +2c
    assert not S[a_of("FOK", 10, 0.01)] and S[a_of("FOK", 10, 0.02)] and S[a_of("FAK", 10, 0.01)]


def test_realised_pairs_choices():
    o5 = [np.array([0.0, 1.0]), np.array([0.5, -1.0])]
    oS = [np.array([0.0, 2.0]), np.array([NAN, NAN])]
    sh = [np.array([0.0, 10.0]), np.array([10.0, 10.0])]
    px = [np.array([NAN, 0.52]), np.array([0.51, 0.53])]
    rows = X.realised(np.array([1, -1]), o5, oS, sh, px, [0.50, 0.50], [1, 2])
    assert rows[0]["fill"] and rows[0]["o5"] == 1.0 and abs(rows[0]["paid"] - 2.0) < 1e-6
    assert not rows[1]["sent"] and rows[1]["o5"] == 0.0 and math.isnan(rows[1]["oS"])


def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    bad = 0
    for name, fn in tests:
        try:
            fn()
        except Exception:  # noqa: BLE001
            bad += 1
            print(f"FAIL {name}")
            traceback.print_exc()
    print(f"{len(tests) - bad} passed, {bad} failed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
