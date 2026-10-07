"""Dynamic execution: choose the price limit and the size from the book an
order is predicted to meet when Polymarket processes it.

    LAB_DATA=/opt/lab_data nice -n 19 .venv/bin/python sniper_dynexec.py

What the bot knows at a decision - its WebSocket book (ask, spread, depth
under the cap, how busy the book has been, whether the ask already moved,
how stale it is), Polymarket's recent trades and the BTC move - predicts two
things, each learned from different data:

  delay   Polymarket's order-out -> match time, from the live canary's own
          orders: the K_DELAY live orders whose state was most like this one
          (a conditional delay, not one distribution for every order)
  drift   how the venue's book moves over that delay, from the recorded venue
          book: the K_BOOK simulated decisions most like this one, each looked
          up at THIS order's predicted delays

Together they are a predicted arrival-time book: the spread of books the
order may meet. Every action - FOK 5/10/20 shares or FAK 10/20 shares, at ask
+1..+15c - is scored on it (expected $ at the 5s markout, fees included) and
the best one is sent, or nothing when no action expects a profit.

Scored against FOK 10 @ +2c (what live sends), paired on the same decisions,
standard errors clustered by round. K_DELAY, K_BOOK, the action grid and the
skip threshold (0) were fixed before any result was seen; nothing is tuned on
the data it is scored on.

  1. simulated decisions over the lab recordings, each half scored by a model
     built on the other half, the delay drawn from the conditional model. This
     still assumes the delay is independent of the outcome WITHIN similar
     states - optimistic if it is not.
  2. the live canary's real orders at the instant Polymarket really processed
     them: no delay model in the scoring at all. The book model is built only
     on rounds without live orders; the delay for each order only from live
     orders in OTHER rounds. This is the test that counts.

Read-only. Live action sizes are max(shares x ask, $1); the venue's 5-share
minimum is enforced. The recorder keeps five ask levels, so a FOK that would
need a sixth counts as no fill (slightly against big, wide actions).
"""
from __future__ import annotations

import argparse
import collections
import math
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import sniper_rescore as R  # noqa: E402

GRID_MS = (100.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0, 650.0, 800.0, 1000.0, 1300.0, 1700.0)
OFFSETS = (0.01, 0.02, 0.03, 0.04, 0.06, 0.08, 0.10, 0.15)
ACTIONS = tuple([("FOK", n, c) for n in (5.0, 10.0, 20.0) for c in OFFSETS]
                + [("FAK", n, c) for n in (10.0, 20.0) for c in OFFSETS])
NOW = ACTIONS.index(("FOK", 10.0, 0.02))
K_DELAY = 40
K_BOOK = 60
DELAY_Q = tuple((i + 0.5) / 10 for i in range(10))     # the predicted delay as ten deciles
HI = 0.95
SLOW_MS = 500.0
FEATURES = ("log_bp", "left", "ask", "spread", "depth2", "log_act1", "dask1", "log_trades2", "log_age")
DELAY_FEATURES = ("log_act1", "log_trades2", "abs_dask1", "log_bp", "left")


def action_name(a) -> str:
    kind, n, c = ACTIONS[a]
    return f"{kind} {n:g} @ +{c * 100:g}c"


# ---------------------------------------------------------------- the model (pure: arrays in, arrays out)
def standardise(train, *others):
    import numpy as np
    mu, sd = train.mean(0), train.std(0)
    sd = np.where(sd > 0, sd, 1.0)
    return [(x - mu) / sd for x in (train,) + others]


def knn(train, query, k, train_groups=None, query_groups=None, chunk=256):
    """Indices of the k nearest training rows (standardised on the training
    rows) for each query row; rows in the query's own group are never used."""
    import numpy as np
    A, B = standardise(train, query)
    k = min(k, len(A))
    out = np.empty((len(B), k), dtype=int)
    for s in range(0, len(B), chunk):
        d = ((B[s:s + chunk, None, :] - A[None, :, :]) ** 2).sum(-1)
        if train_groups is not None:
            d[query_groups[s:s + chunk, None] == train_groups[None, :]] = np.inf
        out[s:s + chunk] = np.argsort(d, axis=1)[:, :k]
    return out


def grid_index(ms):
    import numpy as np
    g = np.asarray(GRID_MS)
    return np.abs(g[None, :] - np.atleast_1d(np.asarray(ms, dtype=float))[:, None]).argmin(1)


def delay_deciles(lat, nbrs):
    """Each query's predicted delay: deciles of its neighbours' real delays."""
    import numpy as np
    return np.quantile(lat[nbrs], DELAY_Q, axis=1).T          # (n_query, 10) ms


def expected(table, nbrs, gidx):
    """Mean over neighbours x predicted delays of table[neighbour, delay, action]
    -> (n_query, n_actions); NaN where nothing is known."""
    import numpy as np
    import warnings
    out = np.empty((len(nbrs), table.shape[2]))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for i in range(len(nbrs)):
            out[i] = np.nanmean(table[nbrs[i]][:, gidx[i], :].reshape(-1, table.shape[2]), axis=0)
    return out


def choose(E, sendable, skip_at=0.0):
    """(dynamic, never-skip) action per decision; -1 = send nothing."""
    import numpy as np
    e = np.where(sendable & ~np.isnan(E), E, -np.inf)
    best = e.argmax(1)
    top = e[np.arange(len(e)), best]
    never_skip = np.where(np.isfinite(top), best, -1)
    dynamic = np.where(top > skip_at, best, -1)
    return dynamic, never_skip


# ---------------------------------------------------------------- the data
def outcome_at(ctx, side, t, ask, pay):
    """Every action on the venue book at true time t: (o5, oS, f, sh, px,
    drift_c) or None when the recorder was not hearing the feed then."""
    import numpy as np
    heard = ctx["heard"]
    g = int(np.searchsorted(heard, t, side="right")) - 1
    if g < 0 or (t - heard[g]) / 1e9 > R.GAP_S:
        return None
    v, vt = ctx["books"][side], ctx["vt"][side]
    j = R.last_at_or_before(vt, t)
    if j is None:
        return None
    m = R.last_at_or_before(vt, t + 5e9)
    mid5 = None if m is None or math.isnan(v["bb"][m]) or math.isnan(v["ba"][m]) else \
        (float(v["bb"][m]) + float(v["ba"][m])) / 2
    n = len(ACTIONS)
    o5, oS = np.full(n, np.nan), np.full(n, np.nan)
    f, sh, px = np.zeros(n), np.zeros(n), np.full(n, np.nan)
    for a, (kind, shares, off) in enumerate(ACTIONS):
        spend = max(shares * ask, 1.0)
        cap = min(HI, round(ask + off, 4))
        q = R.walk(v, j, spend, cap, fak=kind == "FAK")
        if q[0] == "fill" and q[1] > 0:
            f[a], sh[a], px[a] = 1.0, q[1], q[2]
            if mid5 is not None:
                o5[a] = q[1] * (mid5 - q[2]) - q[3]
            if pay is not None:
                oS[a] = q[1] * (pay - q[2]) - q[3]
        else:
            if mid5 is not None:
                o5[a] = 0.0
            if pay is not None:
                oS[a] = 0.0
    drift = None if math.isnan(v["ba"][j]) else (float(v["ba"][j]) - ask) * 100
    return o5, oS, f, sh, px, drift


def features(ctx, side, tw, ask, bp, left):
    """What the bot's own WebSocket copy of the market showed at the decision
    (receipt time <= tw): (feature dict, sendable mask) or None."""
    import numpy as np
    v = ctx["books"][side]
    rw = v["recv_wall"]
    k = R.last_at_or_before(rw, tw)
    if k is None or (tw - rw[k]) / 1e9 > R.GAP_S + 2.0:
        return None
    lv = [(float(p), float(s)) for p, s in v["asks"][k] if not math.isnan(p)]
    want = max(10 * ask, 1.0) / ask
    depth2 = sum(s for p, s in lv if p <= ask + 0.02 + 1e-9) / want
    act1 = int(np.searchsorted(rw, tw, "right") - np.searchsorted(rw, tw - 1e9, "right"))
    k1 = R.last_at_or_before(rw, tw - 1e9)
    dask1 = 0.0 if k1 is None or math.isnan(v["ba"][k1]) or math.isnan(v["ba"][k]) else \
        (float(v["ba"][k]) - float(v["ba"][k1])) * 100
    tt = ctx["trades"][side]
    trades2 = int(np.searchsorted(tt, tw, "right") - np.searchsorted(tt, tw - 2e9, "right"))
    bb = v["bb"][k]
    x = {"log_bp": math.log(max(float(bp), 0.1)), "left": float(left), "ask": ask,
         "spread": 5.0 if math.isnan(bb) else (ask - float(bb)) * 100, "depth2": min(depth2, 5.0),
         "log_act1": math.log1p(act1), "dask1": dask1, "abs_dask1": abs(dask1),
         "log_trades2": math.log1p(trades2), "log_age": math.log1p(max(0.0, (tw - rw[k]) / 1e6))}
    sendable = np.zeros(len(ACTIONS), dtype=bool)
    for a, (kind, shares, off) in enumerate(ACTIONS):
        cap = min(HI, round(ask + off, 4))
        sendable[a] = R.walk(v, k, max(shares * ask, 1.0), cap, fak=kind == "FAK")[0] == "fill"
    return x, sendable


def round_ctx(D, C):
    import numpy as np
    from latency_lab import load as L
    idx_of = {s: i for i, (_w, s) in D["idx_info"].items()}
    if "Up" not in idx_of or "Down" not in idx_of:
        return None
    books = {"UP": D["pm"].get(idx_of["Up"]), "DOWN": D["pm"].get(idx_of["Down"])}
    if any(b is None or not len(b["ts"]) for b in books.values()):
        return None
    vt = {k: np.where(np.isnan(L.pm_true(b["ts"], C)), np.inf, L.pm_true(b["ts"], C)) for k, b in books.items()}
    side_of = {idx_of["Up"]: "UP", idx_of["Down"]: "DOWN"}
    trades = {"UP": [], "DOWN": []}
    for tr in D.get("pm_trades") or []:
        if tr[3] in side_of:
            trades[side_of[tr[3]]].append(float(tr[1]))
    heard = L.to_true(np.sort(np.concatenate([b["recv_wall"] for b in books.values()])), C)
    return {"books": books, "vt": vt, "heard": heard,
            "trades": {k: np.array(sorted(x)) for k, x in trades.items()}}


def payout(ctx, C, side, round_end):
    from latency_lab import load as L
    v, vt = ctx["books"][side], ctx["vt"][side]
    m = R.last_at_or_before(vt, L.to_true(float(round_end) * 1e9, C))
    if m is None or math.isnan(v["bb"][m]) or math.isnan(v["ba"][m]):
        return None
    mid = (float(v["bb"][m]) + float(v["ba"][m])) / 2
    return 1.0 if mid >= 0.9 else 0.0 if mid <= 0.1 else None


def record(ctx, C, side, tw, ask, bp, left, pay, w, real_t=None):
    """One decision: its features, what it could send, and every action's
    outcome at every delay on the grid (and at real_t, a live order's real
    processing instant)."""
    import numpy as np
    from latency_lab import load as L
    got = features(ctx, side, tw, ask, bp, left)
    if got is None:
        return None
    x, sendable = got
    t_out = L.to_true(tw + R.LIVE_OWN_MS * 1e6, C)
    G, A = len(GRID_MS), len(ACTIONS)
    rec = {"w": w, "side": side, "ask": ask, "x": x, "sendable": sendable, "pay": pay,
           "o5": np.full((G, A), np.nan, dtype=np.float32), "oS": np.full((G, A), np.nan, dtype=np.float32),
           "f": np.full((G, A), np.nan, dtype=np.float32),
           "sh": np.zeros((G, A), dtype=np.float32), "px": np.full((G, A), np.nan, dtype=np.float32),
           "drift": np.full(G, np.nan)}
    for g, ms in enumerate(GRID_MS):
        out = outcome_at(ctx, side, t_out + ms * 1e6, ask, pay)
        if out is None:
            continue
        rec["o5"][g], rec["oS"][g], rec["f"][g], rec["sh"][g], rec["px"][g] = out[:5]
        rec["drift"][g] = np.nan if out[5] is None else out[5]
    if real_t is not None:
        out = outcome_at(ctx, side, real_t, ask, pay)
        rec["real"] = out
    return rec


def collect(env_path):
    """Simulated decisions on every recorded round, and the live orders."""
    from latency_lab import load as L
    import sniper_mode
    live, one_way = R.load_live_orders(env_path)
    live_by_round = collections.defaultdict(list)
    for o in live:
        live_by_round[o["w"]].append(o)
    st = R.sim_settings([150.0])
    paths = L.files()
    probes, rounds = L.scan(paths)
    base = L.clock_from_probes(probes)
    print(f"lab data: {L.DATA} ({len(paths)} files, {len(rounds)} rounds) | live orders {len(live)} in "
          f"{len(live_by_round)} rounds", file=sys.stderr)
    sim, lv, t0 = [], [], time.time()
    for i, (w, D, complete) in enumerate(L.cached_rounds(paths, rounds, probes)):
        if not complete:
            continue
        C = dict(base, pm_residual_ns=L.pm_residual(D["pm"], base))
        ctx = round_ctx(D, C)
        if ctx is None:
            continue
        for res in R.simulate_round(D, C, w, st, sniper_mode.Trigger, 0.05, HI):
            rec = record(ctx, C, res["side"], res["tw"], res["ask"], res["bp"], res["left"], res["pay"], w)
            if rec is not None:
                sim.append(rec)
        for o in live_by_round.get(w, []):
            r = o["r"]
            ts = r.get("live_ts_us") or {}
            ask = float(r.get("ask_seen") or 0)
            if not ask:
                continue
            tw = float(r["wall"]) * 1e9
            rec = record(ctx, C, r["side"], tw, ask, float(r.get("bp") or 1.0), float(r["round_end"]) - float(r["wall"]),
                         payout(ctx, C, r["side"], r["round_end"]), w, real_t=L.to_true(o["match_wall"] * 1e9, C))
            if rec is None:
                continue
            rec["group"] = o["group"]
            rec["lat"] = (o["match_wall"] - float(r["wall"]) - ts["net_send_start"] / 1e6) * 1000.0
            lv.append(rec)
        if i % 50 == 0:
            print(f"  ... {i + 1}/{len(rounds)} rounds, {len(sim)} decisions, {len(lv)} live, "
                  f"{time.time() - t0:.0f}s", file=sys.stderr)
    return sim, lv, one_way, len(live)


# ---------------------------------------------------------------- scoring
def matrix(recs, names):
    import numpy as np
    return np.array([[r["x"][n] for n in names] for r in recs], dtype=float)


def spearman(a, b):
    import numpy as np
    a, b = np.asarray(a, float), np.asarray(b, float)
    if len(a) < 3:
        return float("nan")
    ra, rb = a.argsort().argsort(), b.argsort().argsort()
    return float(np.corrcoef(ra, rb)[0, 1])


def auc(score, label):
    """P(a random positive scores above a random negative); 0.5 = no skill."""
    import numpy as np
    score, label = np.asarray(score, float), np.asarray(label, bool)
    pos, neg = score[label], score[~label]
    if not len(pos) or not len(neg):
        return float("nan")
    gt = (pos[:, None] > neg[None, :]).sum() + 0.5 * (pos[:, None] == neg[None, :]).sum()
    return float(gt / (len(pos) * len(neg)))


def realised(choice, o5, oS, sh, px, ask, w):
    """Per decision $ at 5s and settled for the chosen actions (-1 = nothing)."""
    out = []
    for i, a in enumerate(choice):
        if a < 0:
            out.append({"sent": False, "fill": False, "o5": 0.0, "oS": 0.0 if not math.isnan(oS[i][0]) else math.nan,
                        "w": w[i]})
            continue
        fill = sh[i][a] > 0
        out.append({"sent": True, "fill": bool(fill), "o5": float(o5[i][a]), "oS": float(oS[i][a]),
                    "sh": float(sh[i][a]), "paid": (float(px[i][a]) - ask[i]) * 100 if fill else None, "w": w[i],
                    "a": int(a)})
    return out


def policy_table(title, rows_by_policy, n_dec) -> None:
    """One block: every policy on the same decisions, then each against now."""
    print(f"\n  {title}")
    print(f"    {'policy':22s} {'sent':>5s} {'fills':>5s} {'sh/fill':>7s} {'paid-ask c':>10s} {'5s c/sh':>16s} "
          f"{'settled c/sh':>16s} {'5s $/decision':>14s} {'settled $/dec':>14s}")
    for name, rows in rows_by_policy.items():
        fills = [x for x in rows if x["fill"]]
        c5 = R.clustered([x["o5"] / x["sh"] * 100 for x in fills if not math.isnan(x["o5"])],
                         [x["w"] for x in fills if not math.isnan(x["o5"])])
        cS = R.clustered([x["oS"] / x["sh"] * 100 for x in fills if not math.isnan(x["oS"])],
                         [x["w"] for x in fills if not math.isnan(x["oS"])])
        sh = sum(x["sh"] for x in fills) / len(fills) if fills else 0.0
        paid = sum(x["paid"] for x in fills) / len(fills) if fills else float("nan")
        d5 = sum(x["o5"] for x in rows) / max(1, n_dec)
        dS_rows = [x["oS"] for x in rows if not math.isnan(x["oS"])]
        dS = sum(dS_rows) / max(1, len(dS_rows))

        def c(m):
            return "-" if m[0] is None else (f"{m[0]:+6.2f} se {m[1]:4.2f}" if m[1] else f"{m[0]:+6.2f}")
        print(f"    {name:22s} {sum(x['sent'] for x in rows):>5d} {len(fills):>5d} {sh:7.1f} {paid:+10.2f} "
              f"{c(c5):>16s} {c(cS):>16s} {d5:+14.4f} {dS:+14.4f}")
    base = rows_by_policy["FOK 10 @ +2c (now)"]
    for name, rows in rows_by_policy.items():
        if rows is base:
            continue
        cells = []
        for key in ("o5", "oS"):
            d = [(x[key] - b[key], x["w"]) for x, b in zip(rows, base) if not (math.isnan(x[key]) or math.isnan(b[key]))]
            m, se, n = R.clustered([a for a, _ in d], [b for _, b in d]) if d else (None, None, 0)
            cells.append("-" if m is None else f"{m:+.4f} se {se or 0:.4f} t={m / se:+.1f}" if se else f"{m:+.4f}")
        print(f"    {name:22s} vs now, $ per decision: 5s {cells[0]:28s} settled {cells[1]}")


def run_policies(E, sendable):
    import numpy as np
    dyn, never = choose(E, sendable)
    now = np.where(sendable[:, NOW], NOW, -1)
    return {"FOK 10 @ +2c (now)": now, "dynamic (skip if E<=0)": dyn, "dynamic, never skip": never}


def chosen_mix(choice) -> str:
    c = collections.Counter(int(a) for a in choice)
    total = max(1, len(choice))
    parts = [f"{'skip' if a < 0 else action_name(a)} {n / total:.0%}" for a, n in c.most_common(6)]
    return ", ".join(parts)


def report(sim, lv, one_way, n_live) -> None:
    import numpy as np
    print("\nDYNAMIC EXECUTION - predicted arrival-time book -> price limit and size by expected edge")
    print(f"  {len(sim)} simulated decisions, {len(lv)} of {n_live} live orders with their decision state on record; "
          f"{len(ACTIONS)} actions, K_DELAY {K_DELAY}, K_BOOK {K_BOOK}")
    if len(lv) < 20 or len(sim) < 200:
        print("  not enough data")
        return
    lat = np.array([r["lat"] for r in lv])
    lgroups = np.array([r["w"] for r in lv])
    Xl_delay = matrix(lv, DELAY_FEATURES)

    # 1. the conditional delay
    print(f"\n  1. POLYMARKET'S DELAY GIVEN THE STATE - {len(lv)} live orders, order-out -> match "
          f"(reply - {one_way:.1f}ms way back): p50 {np.median(lat):.0f}ms, p90 {np.quantile(lat, 0.9):.0f}ms, "
          f"> {SLOW_MS:g}ms {np.mean(lat > SLOW_MS):.0%}")
    print(f"    {'feature':14s} {'spearman':>9s}   delay p50 / p90 / >{SLOW_MS:g}ms by feature tercile (low | mid | high)")
    for j, name in enumerate(DELAY_FEATURES):
        col = Xl_delay[:, j]
        qs = np.quantile(col, [1 / 3, 2 / 3])
        cells = []
        for sel in (col <= qs[0], (col > qs[0]) & (col <= qs[1]), col > qs[1]):
            if sel.sum():
                cells.append(f"{np.median(lat[sel]):4.0f}/{np.quantile(lat[sel], 0.9):5.0f}/{np.mean(lat[sel] > SLOW_MS):4.0%}")
            else:
                cells.append("-")
        print(f"    {name:14s} {spearman(col, lat):+9.2f}   " + " | ".join(cells))
    nb_l = knn(Xl_delay, Xl_delay, K_DELAY, lgroups, lgroups)          # each live order: other rounds only
    dq_live = delay_deciles(lat, nb_l)
    p_slow = (lat[nb_l] > SLOW_MS).mean(1)
    print(f"    out of sample (each order predicted from live orders in other rounds): predicted vs real delay "
          f"spearman {spearman(np.median(dq_live, 1), lat):+.2f}, AUC for >{SLOW_MS:g}ms {auc(p_slow, lat > SLOW_MS):.2f} "
          f"(0.50 = no skill)")

    # 2. live orders at their real instant: the test that counts
    live_rounds = set(lgroups.tolist())
    train = [r for r in sim if r["w"] not in live_rounds]
    ok = [i for i, r in enumerate(lv) if r.get("real") is not None and not math.isnan(r["real"][0][NOW])]
    if len(train) >= 200 and ok:
        L_ = [lv[i] for i in ok]
        Xtr, Xq = matrix(train, FEATURES), matrix(L_, FEATURES)
        nb = knn(Xtr, Xq, K_BOOK)
        gidx = np.stack([grid_index(d) for d in dq_live[ok]])
        T5 = np.stack([r["o5"] for r in train])
        TF = np.stack([r["f"] for r in train])
        TD = np.stack([r["drift"][:, None] for r in train])
        E = expected(T5, nb, gidx)
        pf = expected(TF, nb, gidx)[:, NOW]
        pdrift = expected(TD, nb, gidx)[:, 0]
        real_f = np.array([r["real"][2][NOW] for r in L_])
        real_drift = np.array([np.nan if r["real"][5] is None else r["real"][5] for r in L_])
        okd = ~np.isnan(real_drift) & ~np.isnan(pdrift)
        print(f"\n  2. LIVE ORDERS AT THE INSTANT POLYMARKET REALLY PROCESSED THEM - {len(L_)} orders; book model from "
              f"{len(train)} simulated decisions in rounds without live orders")
        print(f"    predicted arrival book vs the real one: ask drift spearman {spearman(pdrift[okd], real_drift[okd]):+.2f} "
              f"(predicted mean {np.nanmean(pdrift):+.1f}c, real {np.nanmean(real_drift):+.1f}c); "
              f"FOK 10 @ +2c fill AUC {auc(pf, real_f > 0):.2f}")
        sendable = np.stack([r["sendable"] for r in L_])
        # live DID send FOK 10 @ +2c on every one of these, so it and every action
        # no harder to send (FAK, or <= 10 shares at a cap >= +2c) were sendable
        for a, (kind, n, c) in enumerate(ACTIONS):
            if (kind == "FAK" or n <= 10) and c >= 0.02 - 1e-9:
                sendable[:, a] = True
        pols = run_policies(E, sendable)
        o5 = [r["real"][0] for r in L_]
        oS = [r["real"][1] for r in L_]
        sh = [r["real"][3] for r in L_]
        px = [r["real"][4] for r in L_]
        ask = [r["ask"] for r in L_]
        w = [r["w"] for r in L_]
        rows = {k: realised(v, o5, oS, sh, px, ask, w) for k, v in pols.items()}
        policy_table("each policy at the real instant (5s $ and settled $ per decision, paired with now):", rows, len(L_))
        print(f"    dynamic chose: {chosen_mix(pols['dynamic (skip if E<=0)'])}")
        groups = ("live filled", "missed, shadow@200 filled", "missed, shadow missed too")
        print(f"\n    by what live did:   {'orders':>6s} | now fills  5s $ | dynamic sent fills  5s $ | never-skip fills  5s $")
        for g in groups:
            idx = [i for i, r in enumerate(L_) if r.get("group") == g]
            if not idx:
                continue
            cells = []
            for k in ("FOK 10 @ +2c (now)", "dynamic (skip if E<=0)", "dynamic, never skip"):
                rr = [rows[k][i] for i in idx]
                cells.append((sum(x["sent"] for x in rr), sum(x["fill"] for x in rr),
                              sum(x["o5"] for x in rr if not math.isnan(x["o5"]))))
            print(f"    {g:26s} {len(idx):>6d} | {cells[0][1]:>9d} {cells[0][2]:+6.2f} | {cells[1][0]:>12d} "
                  f"{cells[1][1]:>5d} {cells[1][2]:+6.2f} | {cells[2][1]:>16d} {cells[2][2]:+6.2f}")
    else:
        print("\n  2. live orders: not enough recorded rounds without live orders to build the book model")

    # 3. simulated decisions, halves scored by each other
    order = sorted({r["w"] for r in sim})
    mid = order[len(order) // 2]
    Xs_delay = matrix(sim, DELAY_FEATURES)
    nb_sd = knn(Xl_delay, Xs_delay, K_DELAY)
    dq_sim = delay_deciles(lat, nb_sd)
    rng = random.Random(7)
    draw = np.array([lat[nb_sd[i][rng.randrange(nb_sd.shape[1])]] for i in range(len(sim))])
    g_draw = grid_index(draw)
    print(f"\n  3. SIMULATED DECISIONS - each half scored by a model built on the other; one delay per decision drawn "
          f"from its conditional delay (p50 {np.median(draw):.0f}ms, p90 {np.quantile(draw, 0.9):.0f}ms)")
    for hname, test_sel in (("first half", lambda r: r["w"] < mid), ("second half", lambda r: r["w"] >= mid)):
        te = [i for i, r in enumerate(sim) if test_sel(r) and not math.isnan(r["o5"][g_draw[i], NOW])]
        tr = [i for i, r in enumerate(sim) if not test_sel(r)]
        if len(te) < 50 or len(tr) < 200:
            print(f"    {hname}: too few decisions")
            continue
        Xtr, Xq = matrix([sim[i] for i in tr], FEATURES), matrix([sim[i] for i in te], FEATURES)
        nb = knn(Xtr, Xq, K_BOOK)
        gidx = np.stack([grid_index(dq_sim[i]) for i in te])
        E = expected(np.stack([sim[i]["o5"] for i in tr]), nb, gidx)
        sendable = np.stack([sim[i]["sendable"] for i in te])
        pols = run_policies(E, sendable)
        o5 = [sim[i]["o5"][g_draw[i]] for i in te]
        oS = [sim[i]["oS"][g_draw[i]] for i in te]
        sh = [sim[i]["sh"][g_draw[i]] for i in te]
        px = [sim[i]["px"][g_draw[i]] for i in te]
        ask = [sim[i]["ask"] for i in te]
        w = [sim[i]["w"] for i in te]
        rows = {k: realised(v, o5, oS, sh, px, ask, w) for k, v in pols.items()}
        policy_table(f"{hname} - {len(te)} decisions scored, model from {len(tr)}:", rows, len(te))
        print(f"    dynamic chose: {chosen_mix(pols['dynamic (skip if E<=0)'])}")
    print("\n  read it: block 2 is the verdict - a policy is worth a live test only if it beats now there (and in both")
    print("  halves of block 3) with a positive 5s $ per decision. Block 1 says whether the state predicts")
    print("  Polymarket's delay at all; block 2's first line says whether the predicted arrival book tracks the real one.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", default=str(ROOT / ".env"))
    args = ap.parse_args()
    try:
        import numpy  # noqa: F401
        from latency_lab import load as _L  # noqa: F401
    except ImportError as exc:
        print(f"cannot load the lab tools ({exc}); on the VPS: .venv/bin/pip install numpy")
        return 1
    sim, lv, one_way, n_live = collect(args.env)
    report(sim, lv, one_way, n_live)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
