"""Re-score past paper sniper orders on the VENUE's book, from the latency
lab's recordings - which past paper fills could really have happened?

    LAB_DATA=/opt/lab_data nice -n 19 .venv/bin/python sniper_rescore.py --all
    ... sniper_rescore.py --hours 48        # only orders from the last 48 hours
    ... sniper_rescore.py --simulate        # the sniper's rule replayed on ALL lab recordings

Until 7dc208b the paper sniper priced each order on the book in OUR hands
when its timer woke. That book lags the venue by the market feed's delay,
which is longest exactly after a BTC move - so paper could fill at asks the
makers had already pulled. This replays every journalled order against the
book as it stood ON THE VENUE at the modelled match instant:

    T = our send time (on the true clock) + one-way + hold + server time

using the lab recorder's Polymarket feed: book snapshots and level changes,
each with the venue's own timestamp, mapped to true time by the lab's clock
calibration (latency_lab.load: Binance REST offset; Polymarket residual so
the fastest messages take half a PING round trip). The order is a FOK for
the journalled stake at the journalled cap, walked over the recorded ask
levels; fees 0.07*p*(1-p) per share. It also replays the order on the
recorder's RECEIPT-time book (what a paper bot reading its own feed would
see) as a check that the recordings line up with what paper reported.

--simulate needs no journal: it replays the sniper's exact decision rule
(sniper_mode.Trigger on the recorded Binance spot mid, cooldown, round window,
price band, 10 shares / $1 minimum, ask+2c, 20 fills a round, own fills netted
for 2s) over every recorded round, and prices each order both ways - the old
rule (the book in our hands at send + flight) and the venue-time book - so the
size of the old paper's optimism is measured on the recordings themselves.
Settlement there uses the round's last recorded mid (>=0.90 won, <=0.10 lost).

Read-only: the journal, the paper ledger (for each token's payout) and the
lab files. Round books it rebuilds are cached in LAB_DATA/_rounds (shared
with the lab report). It needs the recorder to have run during the orders;
orders without lab coverage are counted, never guessed.
"""
from __future__ import annotations


import argparse
import collections
import json
import math
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from accounting.ledger import current_payload  # noqa: E402

FEE_RATE = 0.07
MIN_SHARES = 5.0
GAP_S = 1.5            # the recorder re-stores an unchanged book every second: older means the feed was down


# ---------------------------------------------------------------- the arithmetic (no data needed)
def fok(asks, spend: float, cap: float, fee_rate: float = FEE_RATE, fak: bool = False):
    """Walk ask levels [(price, size), ...] (NaN = no level) for a FOK BUY of
    `spend` dollars up to `cap`. Returns ("fill", shares, avg_price, fee),
    ("miss", why) or ("unknown", why) when the recorded depth ends before the
    order is filled while every recorded level was still under the cap.
    fak=True: fill-and-kill - whatever the levels at or under the cap hold is
    taken and the rest cancelled; a partial fill returns a 6th element True.
    (Assumes the venue's 5-share / $1 minimums apply to the order as placed,
    not to each partial match.)"""
    remaining, shares, cost, fee, levels, used = float(spend), 0.0, 0.0, 0.0, 0, []
    for p, s in asks:
        if p is None or s is None or math.isnan(p) or math.isnan(s):
            break
        levels += 1
        if p > cap + 1e-9:
            if fak and shares > 0:
                return ("fill", shares, cost / shares, fee, used, True)
            return ("miss", "cap") if shares == 0 and levels == 1 else ("miss", "depth under cap")
        take = min(remaining, p * s)
        sh = take / p
        if sh <= 0:
            continue
        used.append((float(p), sh))
        shares, cost, fee, remaining = shares + sh, cost + take, fee + fee_rate * sh * p * (1 - p), remaining - take
        if remaining <= 0.005:
            remaining = 0.0
            break
    if remaining > 0:
        if levels == 0:
            return ("miss", "no asks")
        if fak and shares > 0:                          # what the recorded levels hold, at least
            return ("fill", shares, cost / shares, fee, used, True)
        return ("unknown", "recorded depth ends under the cap")
    if shares < MIN_SHARES - 1e-9:
        return ("miss", "below venue minimum")
    return ("fill", shares, cost / shares, fee, used)


def clustered(values, clusters):
    """(mean, standard error clustered by round, n). Fills in one round share
    one settlement, so they are not independent observations."""
    n = len(values)
    if n < 2:
        return (values[0] if values else None), None, n
    mean = sum(values) / n
    by = collections.defaultdict(float)
    for v, c in zip(values, clusters):
        by[c] += v - mean
    g = len(by)
    if g < 2:
        return mean, None, n
    var = sum(x * x for x in by.values()) / (n * n) * g / (g - 1)
    return mean, math.sqrt(var), n


# ---------------------------------------------------------------- inputs
def load_orders(args):
    import sniper_check as sc
    env = sc.read_env(Path(args.env))
    audit = sc.state_path(env, "PAPER_AUDIT_PATH", "paper_orders.jsonl")
    journal = sc.jsonl(audit.with_name(f"{audit.stem}_sniper.jsonl"))
    since = time.time() - args.hours * 3600 if args.hours else 0.0
    orders = [r for r in journal if r.get("result") in ("filled", "refused") and not r.get("live")
              and float(r.get("wall") or 0) >= since and r.get("round_end") and r.get("cap")
              and r.get("amount") and r.get("side") in ("UP", "DOWN")]
    marks = {r.get("order_id"): r for r in journal if r.get("result") == "markout"}
    ledger_path = sc.state_path(env, "PAPER_LEDGER_PATH", "paper_ledger.json")
    try:
        ledger = current_payload(str(ledger_path))     # the snapshot plus its journal
    except (OSError, ValueError, RuntimeError) as exc:
        # no payouts: everything but the settled column still works
        print(f"paper ledger unreadable ({type(exc).__name__}: {ledger_path}); settled P&L will be blank",
              file=sys.stderr)
        ledger = {}
    payout = {}
    for tok, p in (ledger.get("positions") or {}).items():
        if isinstance(p, dict) and p.get("settled") and p.get("payout_per_share") is not None:
            payout[str(p.get("token_id") or tok)] = float(p["payout_per_share"])
    return orders, marks, payout


def sent_wall(r) -> float:
    """Our wall clock when the order left (decision wall + decide->sent)."""
    ts = r.get("ts_us") or {}
    if "sent" in ts and "decide" in ts:
        return float(r["wall"]) + (ts["sent"] - ts["decide"]) / 1e6
    return float(r["wall"]) + 0.005


def flight_ms(r, hold_ms=None) -> float:
    if hold_ms is None:
        return float(r.get("latency_ms") or 191.8)
    return float(r.get("net_ms") or 16.8) + hold_ms + float(r.get("server_ms") or 25.0)


# ---------------------------------------------------------------- one round
def last_at_or_before(times, t):
    """Index of the last recorded row whose time <= t (rows in receipt order), or None."""
    import numpy as np
    ok = np.nonzero(times <= t)[0]
    return int(ok[-1]) if len(ok) else None


def walk(v, j, amount: float, cap: float, own=None, fak: bool = False):
    """fok() on recorded row j - but no ask levels on record while the venue
    reported a best ask under $1 is missing data, not an empty book. `own`:
    [(price, shares)] of our own recent paper fills, taken off the levels
    (paper buys never reach the venue; live, those shares would be gone)."""
    a = v["asks"][j]
    if math.isnan(a[0][0]) and not math.isnan(v["ba"][j]) and v["ba"][j] < 0.999:
        return ("unknown", "ask levels not recorded")
    if own:
        gone = collections.Counter()
        for px, sh in own:
            gone[round(px, 4)] += sh
        a = [(p, s - gone.get(round(float(p), 4), 0.0)) for p, s in a if math.isnan(p)
             or s - gone.get(round(float(p), 4), 0.0) > 1e-9]
    return fok(a, amount, cap, fak=fak)


# --exec-study: the execution variants under test, each sized and pre-send
# checked exactly as live does it: name -> (shares, headroom over the ask seen, FAK)
EXEC_VARIANTS = {
    "FOK 10 @ +2c (now)": (10.0, 0.02, False),
    "FOK 5 @ +2c": (5.0, 0.02, False),
    "FOK 10 @ +3c": (10.0, 0.03, False),
    "FOK 10 @ +4c": (10.0, 0.04, False),
    "FAK 10 @ +2c": (10.0, 0.02, True),
}
LIVE_OWN_MS = 7.0          # BTC tick -> order out, measured live (median 6.9ms)


def live_flights(env_path) -> tuple:
    """The live canary's own order-out -> match times (reply minus the way
    back), or ([], one_way) when there is no live journal here."""
    import statistics
    import sniper_check as sc
    env = sc.read_env(Path(env_path))
    rows = sc.jsonl(sc.state_path(env, "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl"))
    rtts = [((r.get("live") or {}).get("clock") or {}).get("rtt_ms_p50") for r in rows if r.get("result") == "summary"]
    rtts = [x for x in rtts if x]
    one_way = statistics.median(rtts) / 2.0 if rtts else 13.3
    out = sorted(float(r["t_wire_to_reply_ms"]) - one_way for r in rows
                 if r.get("live") and r.get("t_wire_to_reply_ms") is not None and r.get("kind") != "AMBIGUOUS")
    return out, one_way


def live_stake(shares, ask, cap, asks_dec, minimum=MIN_SHARES):
    """The stake live would send: max(shares x ask, $1), raised by the shared
    venue-minimum walk (with its price headroom) on the decision book."""
    from decimal import Decimal
    import orderbook
    wanted = Decimal(str(max(shares * ask, 1.0))).quantize(Decimal("0.01"))
    try:
        return float(orderbook.venue_minimum_stake(wanted, asks_dec, Decimal(str(minimum)), Decimal(str(cap))))
    except Exception:  # noqa: BLE001 - unreadable: the stake stays as asked, as live
        return float(wanted)


# --study: the same decisions priced at four price headrooms, FOK and FAK
STUDY_VARIANTS = {
    "FOK +2c (now)": (10.0, 0.02, False),
    "FOK +3c": (10.0, 0.03, False),
    "FOK +4c": (10.0, 0.04, False),
    "FOK +6c": (10.0, 0.06, False),
    "FAK +2c": (10.0, 0.02, True),
    "FAK +3c": (10.0, 0.03, True),
    "FAK +4c": (10.0, 0.04, True),
}

# Execution variants priced on the SAME decisions at the headline hold
# (--variants): name -> (shares, price headroom over the ask seen, fill-and-kill)
VARIANTS = {
    "FOK 10sh ask+2c (now)": (10.0, 0.02, False),
    "FAK 10sh ask+2c": (10.0, 0.02, True),
    "FOK 5sh ask+2c": (5.0, 0.02, False),
    "FOK 10sh ask+3c": (10.0, 0.03, False),
    "FOK 10sh ask+4c": (10.0, 0.04, False),
    "FAK 10sh ask+4c": (10.0, 0.04, True),
}


def score_round(D, C, orders, holds):
    """Each order of one round, on the venue-time book (per hold) and on the
    recorder's receipt-time book."""
    import numpy as np
    from latency_lab import load as L
    info = next(iter(D["rounds"].values()))
    idx_of = {s: i for i, (_w, s) in D["idx_info"].items()}
    tok_of = {"UP": str(info.get("up_token_id")), "DOWN": str(info.get("down_token_id"))}
    # the recorder heard the round's feed at these (true) times, either token:
    # silence longer than GAP_S before an instant means its feed was down
    heard_wall = np.sort(np.concatenate([v["recv_wall"] for v in D["pm"].values()] or [np.array([])]))
    heard = L.to_true(heard_wall, C)
    out = []
    for r in orders:
        res = {"order": r, "token": tok_of[r["side"]], "venue": {}, "receipt": None}
        idx = idx_of.get("Up" if r["side"] == "UP" else "Down")
        v = D["pm"].get(idx) if idx is not None else None
        if v is None or not len(v["ts"]):
            res["skip"] = "no recorded book for this token"
            out.append(res)
            continue
        venue_t = L.pm_true(v["ts"], C)
        venue_t = np.where(np.isnan(venue_t), np.inf, venue_t)
        sent = sent_wall(r)
        amount, cap = float(r["amount"]), float(r["cap"])
        for h in holds:
            t = L.to_true(sent * 1e9, C) + flight_ms(r, h) * 1e6
            k = int(np.searchsorted(heard, t, side="right")) - 1
            if k < 0 or (t - heard[k]) / 1e9 > GAP_S:
                res["venue"][h] = ("unknown", "the recorder heard nothing from the feed then")
                continue
            j = last_at_or_before(venue_t, t)
            if j is None:
                res["venue"][h] = ("unknown", "no book before the instant")
                continue
            res["venue"][h] = walk(v, j, amount, cap)
            if h == holds[0]:
                m = last_at_or_before(venue_t, t + 5e9)
                if m is not None and not (math.isnan(v["bb"][m]) or math.isnan(v["ba"][m])):
                    res["mid5"] = (float(v["bb"][m]) + float(v["ba"][m])) / 2
        # what a paper bot reading its own copy of the feed saw at the same moment
        t_r = (sent + flight_ms(r) / 1000.0) * 1e9
        g = int(np.searchsorted(heard_wall, t_r, side="right")) - 1
        k = last_at_or_before(v["recv_wall"], t_r)
        if g < 0 or (t_r - heard_wall[g]) / 1e9 > GAP_S or k is None:
            res["receipt"] = ("unknown", "the recorder heard nothing from the feed then")
        else:
            res["receipt"] = walk(v, k, amount, cap)
        out.append(res)
    return out


# ---------------------------------------------------------------- the sniper replayed on the recordings
def simulate_round(D, C, w, st, Trigger, lo=0.05, hi=0.95, own_ttl_s=2.0, own_ms=3.0, lag_ms=0.0):
    """The sniper's decisions over one recorded round, each order priced on
    the old rule (our receipt-time book) and on the venue-time book. lag_ms:
    the bot reads its own copy of the feed later than the lean recorder did -
    decide (and, for the old rule, fill) on the receipt-time book that many
    ms older. The venue-time book does not move."""
    lag = lag_ms * 1e6
    import numpy as np
    from latency_lab import load as L
    b = D["bsb"]
    idx_of = {side: i for i, (_w, side) in D["idx_info"].items()}
    if not len(b) or "Up" not in idx_of or "Down" not in idx_of:
        return []
    books = {"UP": D["pm"][idx_of["Up"]], "DOWN": D["pm"][idx_of["Down"]]}
    if any(not len(v["ts"]) for v in books.values()):
        return []
    heard_wall = np.sort(np.concatenate([v["recv_wall"] for v in books.values()]))
    vt = {k: np.where(np.isnan(L.pm_true(v["ts"], C)), np.inf, L.pm_true(v["ts"], C)) for k, v in books.items()}
    end_ns = (w + 300) * 1e9

    def payout(side):
        j = last_at_or_before(vt[side], L.to_true(end_ns, C))
        v = books[side]
        if j is None or math.isnan(v["bb"][j]) or math.isnan(v["ba"][j]):
            return None
        m = (v["bb"][j] + v["ba"][j]) / 2
        return 1.0 if m >= 0.9 else 0.0 if m <= 0.1 else None

    pay = {"UP": payout("UP"), "DOWN": payout("DOWN")}
    # Polymarket's own trade tape (last_trade_price): independent of the book
    # feed, so it can contradict the book reconstruction. (true time, price, taker side, size)
    tape = {"UP": [], "DOWN": []}
    side_of = {idx_of["Up"]: "UP", idx_of["Down"]: "DOWN"}
    for tr in D.get("pm_trades") or []:
        sd = side_of.get(tr[3])
        if sd and tr[2] is not None and not (isinstance(tr[2], float) and math.isnan(tr[2])):
            tape[sd].append((float(L.pm_true(float(tr[2]), C)), float(tr[4]), str(tr[6] or "").upper(),
                             float(tr[5] or 0)))
    for sd in tape:
        tape[sd].sort()
    tape_t = {sd: np.array([x[0] for x in tape[sd]]) for sd in tape}
    trig = Trigger(st.k_bp, st.window_ms)
    last_fire, out = -1e18, []
    fills_round, side_sh = 0, collections.Counter()
    own = {rule: {"UP": [], "DOWN": []} for rule in ("old", "venue")}
    holds = st.holds

    def silent(t_wall):
        g = int(np.searchsorted(heard_wall, t_wall, side="right")) - 1
        return g < 0 or (t_wall - heard_wall[g]) / 1e9 > GAP_S

    def recent(rule, side, t_wall):
        return [(p, sh) for t0, p, sh in own[rule][side] if 0 <= t_wall - t0 < own_ttl_s * 1e9]

    for row in b:
        tw = float(row[1])
        mid = (float(row[2]) + float(row[4])) / 2
        d, _r = trig.update(tw / 1e6, mid)
        if d is None:
            continue
        if tw / 1e6 - last_fire < st.cooldown_ms:
            continue
        if not getattr(st, "cooldown_eligible_only", False):
            last_fire = tw / 1e6                 # the old on_tick: any signal past the cooldown restarts it
        left = (end_ns - tw) / 1e9
        if left > 300 or left < st.min_left_s:
            continue
        side = "UP" if d > 0 else "DOWN"
        v = books[side]
        k = last_at_or_before(v["recv_wall"], tw - lag)
        # the sniper skips a book not updated for stale_ms; the recorder re-stores
        # an unchanged book at most once a second, so allow that much more
        if k is None or silent(tw - lag) or (tw - lag - v["recv_wall"][k]) / 1e6 > st.stale_ms + 1000.0:
            continue
        asks = [(p, s) for p, s in v["asks"][k] if not math.isnan(p)]
        gone = collections.Counter()
        for px, sh in recent("old", side, tw):
            gone[round(px, 4)] += sh
        asks = [p for p, s in asks if s - gone.get(round(float(p), 4), 0.0) > 1e-9]
        if not asks:
            continue
        ask = float(asks[0])
        if not lo <= ask <= hi:
            continue
        amount = max(st.shares * ask, 1.0)
        if fills_round >= st.max_fills_round or side_sh[side] + amount / ask > st.max_side_shares:
            continue
        cap = min(0.99, round(ask + st.slip, 4))
        sent = tw + own_ms * 1e6
        res = {"round": w, "side": side, "ask": ask, "cap": cap, "amount": amount, "pay": pay[side], "venue": {},
               "tw": tw, "bp": abs(float(_r)), "left": left}
        if getattr(st, "exec_flights", None):
            import random
            from decimal import Decimal as _D
            rng = random.Random(int(w) * 1000 + len(out))
            flight = rng.choice(st.exec_flights)            # one real live delay for this order, all variants
            t_x = L.to_true(tw + LIVE_OWN_MS * 1e6, C) + flight * 1e6
            own_dec = recent("old", side, tw)
            gone_x = collections.Counter()
            for px, sh in own_dec:
                gone_x[round(px, 4)] += sh
            asks_dec = [(_D(str(p_)), _D(str(s_ - gone_x.get(round(float(p_), 4), 0.0)))) for p_, s_ in v["asks"][k]
                        if not math.isnan(p_) and s_ - gone_x.get(round(float(p_), 4), 0.0) > 1e-9]
            j_x = None if silent(t_x - L.to_true(0.0, C)) else last_at_or_before(vt[side], t_x)
            mids_x = []
            for dt in (1e9, 5e9):
                m = last_at_or_before(vt[side], t_x + dt)
                mids_x.append(None if m is None or math.isnan(v["bb"][m]) or math.isnan(v["ba"][m])
                              else (float(v["bb"][m]) + float(v["ba"][m])) / 2)
            ex = {}
            for name, (sh_v, slip_v, fak_v) in (getattr(st, "exec_variants", None) or EXEC_VARIANTS).items():
                cap_v = min(hi, round(ask + slip_v, 4))
                stake = live_stake(sh_v, ask, cap_v, asks_dec)
                pre = walk(v, k, stake, cap_v, own_dec, fak_v)       # live's pre-send check (FAK: any depth)
                if pre[0] != "fill":
                    ex[name] = ("not sent", pre[0], stake)
                elif j_x is None:
                    ex[name] = ("unknown", "no venue book at the match instant", stake)
                else:
                    ex[name] = walk(v, j_x, stake, cap_v, recent("venue", side, tw + (LIVE_OWN_MS + flight) * 1e6), fak_v) + (stake,)
            res["exec"] = {"flight": flight, "mids": tuple(mids_x), "v": ex}
        if getattr(st, "study", False):
            # what the bot knew at the decision: the move, the book it priced on
            lv = [(float(p_), float(s_) - gone.get(round(float(p_), 4), 0.0)) for p_, s_ in v["asks"][k]
                  if not math.isnan(p_)]
            bbk = v["bb"][k]
            res["feat"] = {"bp": abs(float(_r)), "left": left,
                           "depth": sum(s_ for p_, s_ in lv if p_ <= cap + 1e-9 and s_ > 0) / (amount / ask),
                           "age_ms": (tw - lag - v["recv_wall"][k]) / 1e6,
                           "spread": None if math.isnan(bbk) else ask - float(bbk),
                           # live's pre-send check: the decision book must fill the FOK at the cap
                           "presend": walk(v, k, amount, cap, recent("old", side, tw))[0]}
        t_rec = sent + (16.8 + 150.0 + 25.0) * 1e6
        kr = last_at_or_before(v["recv_wall"], t_rec - lag)
        res["old"] = (("unknown", "the recorder heard nothing from the feed then") if kr is None or silent(t_rec - lag)
                      else walk(v, kr, amount, cap, recent("old", side, t_rec)))
        for h in holds:
            t = L.to_true(sent, C) + (16.8 + h + 25.0) * 1e6
            if silent(t - L.to_true(0.0, C)):
                res["venue"][h] = ("unknown", "the recorder heard nothing from the feed then")
                continue
            j = last_at_or_before(vt[side], t)
            res["venue"][h] = (("unknown", "no book before the instant") if j is None
                               else walk(v, j, amount, cap, recent("venue", side, t_rec)))
            # this hold's own markout instants: 1s and 5s after ITS match
            mids = []
            for dt in (1e9, 5e9):
                m = last_at_or_before(vt[side], t + dt)
                mids.append(None if m is None or math.isnan(v["bb"][m]) or math.isnan(v["ba"][m])
                            else (float(v["bb"][m]) + float(v["ba"][m])) / 2)
            res.setdefault("mids", {})[h] = tuple(mids)
            if h == holds[0] and j is not None and getattr(st, "variants", None):
                own_now = recent("venue", side, t_rec)
                res["var"] = {name: walk(v, j, max(sh_v * ask, 1.0), min(0.99, round(ask + slip_v, 4)), own_now, fak_v)
                              for name, (sh_v, slip_v, fak_v) in st.variants.items()}
            if h == holds[0]:
                m = last_at_or_before(vt[side], t + 5e9)
                if m is not None and not (math.isnan(v["bb"][m]) or math.isnan(v["ba"][m])):
                    res["mid5"] = (float(v["bb"][m]) + float(v["ba"][m])) / 2
                # trades on this token just before and just after the match instant
                a = int(np.searchsorted(tape_t[side], t - 100e6))
                b = int(np.searchsorted(tape_t[side], t + 300e6))
                c0 = int(np.searchsorted(tape_t[side], t))
                after = tape[side][c0:b]
                before = tape[side][a:c0]
                res["tape"] = {
                    "after_any": bool(after),
                    "after_buy_le_cap": any(sd == "BUY" and px <= cap + 1e-9 for _t, px, sd, _q in after),
                    "after_buy_gt_cap": any(sd == "BUY" and px > cap + 1e-9 for _t, px, sd, _q in after),
                    "after_buy_le_cap_enough": sum(q for _t, px, sd, q in after
                                                   if sd == "BUY" and px <= cap + 1e-9) >= amount / ask - 1e-9,
                    "before_buy_le_cap": any(sd == "BUY" and px <= cap + 1e-9 for _t, px, sd, _q in before)}
        for rule, x in (("old", res["old"]), ("venue", res["venue"].get(holds[0]))):
            if x and x[0] == "fill":
                own[rule][side] += [(t_rec, p, sh) for p, sh in x[4]]
        if res["old"][0] == "fill":                 # caps counted as the old paper counted them
            fills_round += 1
            side_sh[side] += res["old"][1]
        if getattr(st, "cooldown_eligible_only", False):
            last_fire = tw / 1e6                 # the current bot: only an order starts the cooldown
        out.append(res)
    return out


def tape_check(dec, of, vf) -> None:
    """Polymarket's own trades around each order's match instant, by what the
    two rules said. If the venue-time rule called misses that were really
    fillable, its 'false fills' would show taker BUYs at or under our cap
    right after the instant about as often as its real fills do."""
    groups = (("venue-time FILLED", [r for r, a, b in zip(dec, of, vf) if b]),
              ("old FILL, venue MISS ('false fills')", [r for r, a, b in zip(dec, of, vf) if a and not b]),
              ("both rules MISS", [r for r, a, b in zip(dec, of, vf) if not a and not b]))
    print("\n  TAPE CHECK - Polymarket's own trades on the token (independent of the book feed):")
    print(f"  {'':38s} {'orders':>6s} {'any trade':>9s} {'BUY<=cap':>8s} {'>=our sh':>8s} {'BUY>cap':>7s} "
          f"{'BUY<=cap':>13s}")
    print(f"  {'':38s} {'':>6s} {'---- 0-300ms after the match instant ----':>44s} {'100ms before':>13s}")
    for name, sel in groups:
        tp = [r["tape"] for r in sel if r.get("tape")]
        n = max(1, len(tp))
        pc = lambda k: f"{100 * sum(1 for t in tp if t[k]) / n:5.0f}%"
        print(f"  {name:38s} {len(tp):>6} {pc('after_any'):>9s} {pc('after_buy_le_cap'):>8s} "
              f"{pc('after_buy_le_cap_enough'):>8s} {pc('after_buy_gt_cap'):>7s} {pc('before_buy_le_cap'):>13s}")
    print("  (taker side as Polymarket prints it; '>=our sh': taker BUYs at or under our cap added up to at least")
    print("   our order's shares. If the 'false fills' row looks like the FILLED row, the venue-time book is")
    print("   wrong about them; if it looks like the both-MISS row, the venue really could not fill them.)")


def simulate_report(results, holds, rounds_ok, hours, lag_ms=0.0):
    h0 = holds[0]
    print(f"\nSNIPER REPLAYED ON THE LAB RECORDINGS: {rounds_ok} complete rounds (~{hours:.0f}h), "
          f"{len(results)} orders" + (f" | decisions on a book {lag_ms:g}ms older than the recorder's" if lag_ms else ""))
    if not results:
        return
    kind = lambda x: x[0] if x else "none"
    dec = [r for r in results if kind(r["old"]) in ("fill", "miss") and kind(r["venue"].get(h0)) in ("fill", "miss")]
    of = [kind(r["old"]) == "fill" for r in dec]
    vf = [kind(r["venue"][h0]) == "fill" for r in dec]
    print(f"  decidable by both rules: {len(dec)} (the rest: feed gaps or unrecorded depth)")
    print(f"  fill rate: old paper rule {sum(of) / max(1, len(dec)):.0%} -> venue-time {sum(vf) / max(1, len(dec)):.0%}")
    print(f"  filled in both {sum(a and b for a, b in zip(of, vf))} | FALSE FILLS (old filled, the venue could not) "
          f"{sum(a and not b for a, b in zip(of, vf))} | venue-only fills {sum(b and not a for a, b in zip(of, vf))}")

    def stats(sel, rule):
        mk, mc, stl, sc_ = [], [], [], []
        for r in sel:
            x = r["old"] if rule == "old" else r["venue"].get(h0)
            if kind(x) != "fill":
                continue
            shares, px, fee = x[1], x[2], x[3]
            if r.get("mid5") is not None:
                mk.append((r["mid5"] - px - fee / shares) * 100)
                mc.append(r["round"])
            if r["pay"] is not None:
                stl.append((r["pay"] - px - fee / shares) * 100)
                sc_.append(r["round"])
        return clustered(mk, mc), clustered(stl, sc_)
    fmt = lambda m, se, n: (f"{m:+6.2f} +-{se:4.2f} (t={m / se:+.1f}, n={n})" if se else
                            (f"{m:+6.2f} (n={n})" if m is not None else "-"))
    print("\n  c/share (clustered by round)          5s markout                    settled (round-end mid)")
    for name, sel, rule in (("old paper rule", dec, "old"), ("venue-time", dec, "venue"),
                            ("  the old rule's FALSE fills", [r for r, a, b in zip(dec, of, vf) if a and not b], "old")):
        mk, stl = stats(sel, rule)
        print(f"  {name:36s} {fmt(*mk):30s} {fmt(*stl)}")
    tape_check(dec, of, vf)
    latency_table(results, holds, kind, fmt)
    if any(r.get("var") for r in results):
        variants_table(results, holds, kind, fmt)


def variants_table(results, holds, kind, fmt) -> None:
    """Execution variants on the SAME decisions at the headline hold: does
    fill-or-kill, the 10-share clip or the 2c headroom throw away fills that
    would have paid? Paired: every variant is priced on exactly the orders
    the others are, so the $/order differences are not sampling noise
    between different order sets."""
    h0 = holds[0]
    names = list(next(r["var"] for r in results if r.get("var")))
    both = [r for r in results if r.get("var") and all(kind(r["var"].get(n)) in ("fill", "miss") for n in names)]
    print(f"\n  EXECUTION VARIANTS at a {h0:g}ms hold - the same {len(both)} decisions priced each way "
          f"(markouts 1s / 5s after the match, settled at round end):")
    print(f"  {'variant':22s} {'fill':>5s} {'partial':>7s} {'sh/fill':>7s}  {'1s c/share':22s} {'5s c/share':22s} "
          f"{'settled c/share':22s} {'$/order':>8s} {'vs now':>8s}")
    base = None
    for n in names:
        f = [r for r in both if kind(r["var"][n]) == "fill"]

        def per_share(r, value):
            x = r["var"][n]
            return (value - x[2] - x[3] / x[1]) * 100
        cols = []
        for i in (0, 1):
            sel = [r for r in f if (r.get("mids") or {}).get(h0, (None, None))[i] is not None]
            cols.append(clustered([per_share(r, r["mids"][h0][i]) for r in sel], [r["round"] for r in sel]))
        sel = [r for r in f if r["pay"] is not None]
        cols.append(clustered([per_share(r, r["pay"]) for r in sel], [r["round"] for r in sel]))
        usd = sum((r["pay"] - r["var"][n][2]) * r["var"][n][1] - r["var"][n][3] for r in sel)
        per_order = usd / max(1, len(both))
        base = per_order if base is None else base
        partial = sum(1 for r in f if len(r["var"][n]) > 5 and r["var"][n][5])
        shares = sum(r["var"][n][1] for r in f) / max(1, len(f))
        short = lambda m, se, k: (f"{m:+5.2f} +-{se:4.2f} n={k}" if se else (f"{m:+5.2f} n={k}" if m is not None else "-"))  # noqa: E731
        print(f"  {n:22s} {len(f) / max(1, len(both)):5.0%} {partial:7d} {shares:7.1f}  {short(*cols[0]):22s} "
              f"{short(*cols[1]):22s} {short(*cols[2]):22s} {per_order:+8.4f} {per_order - base:+8.4f}")
    print("  read it as: a variant earns a live test only if its $/order beats 'now' AND its 5s markout stays"
          " positive - more fills at a worse price is not an improvement. FAK partials assume the venue's"
          " minimums apply to the order, not to each match.")


# --k-study: the trigger threshold against "an order in every market", each
# order executed as the paper/live bot is set up now (FAK at the ask + 10c),
# plus two narrower caps for comparison; one live delay per order
K_VARIANTS = {
    "FAK 10 @ +10c": (10.0, 0.10, True),
    "FAK 10 @ +4c": (10.0, 0.04, True),
    "FOK 10 @ +2c": (10.0, 0.02, False),
}


def k_study_report(by_k: dict, rounds_ok: int, n_flights: int, band, cooldown_ms: float = 1000.0) -> None:
    """Per threshold: share of markets with an order and with a fill, orders
    per market, and what the fills earned - same rounds, same live delays."""
    names = list(K_VARIANTS)
    print(f"\n  ORDER IN EVERY MARKET? - {rounds_ok} recorded markets, the sniper's rule at each threshold (move within "
          f"250ms), only an order starts the {cooldown_ms:g}ms cooldown (as the bot now does); ask band {band[0]:g}-{band[1]:g}; each "
          f"order at one delay drawn from {n_flights or 'a fixed'} live order-out -> match times")

    def summarise(rs, name):
        sent = [r for r in rs if r["exec"]["v"][name][0] not in ("not sent", "unknown")]
        fills = [r for r in sent if r["exec"]["v"][name][0] == "fill"]
        x = lambda r: r["exec"]["v"][name]  # noqa: E731
        c5 = clustered([(r["exec"]["mids"][1] - x(r)[2] - x(r)[3] / x(r)[1]) * 100 for r in fills
                        if r["exec"]["mids"][1] is not None],
                       [r["round"] for r in fills if r["exec"]["mids"][1] is not None])
        cS = clustered([(r["pay"] - x(r)[2] - x(r)[3] / x(r)[1]) * 100 for r in fills if r["pay"] is not None],
                       [r["round"] for r in fills if r["pay"] is not None])
        usd5 = sum(x(r)[1] * (r["exec"]["mids"][1] - x(r)[2]) - x(r)[3] for r in fills
                   if r["exec"]["mids"][1] is not None)
        usdS = sum(x(r)[1] * (r["pay"] - x(r)[2]) - x(r)[3] for r in fills if r["pay"] is not None)
        return {"sent": sent, "fills": fills, "c5": c5, "cS": cS, "usd5": usd5, "usdS": usdS,
                "markets_fill": len({r["round"] for r in fills}),
                "sh": (sum(x(r)[1] for r in fills) / len(fills)) if fills else 0.0}

    def c(m):
        return "-" if m[0] is None else (f"{m[0]:+5.2f}+-{m[1]:4.2f}" if m[1] else f"{m[0]:+5.2f}")
    main = names[0]
    print(f"\n  {'move':>5s} {'markets with':>13s} {'orders':>7s} | {main}: {'fill%':>5s} {'markets w/':>10s} "
          f"{'sh/fill':>7s} {'5s c/sh':>11s} {'settled c/sh':>12s} {'$/order 5s':>10s} {'$/market':>8s}")
    print(f"  {'bp':>5s} {'an order':>13s} {'/market':>7s} | {'':{len(main)}s}  {'':>5s} {'a fill':>10s}")
    for k in sorted(by_k, reverse=True):
        rs = by_k[k]
        markets = len({r["round"] for r in rs})
        m = summarise(rs, main)
        n_sent = len(m["sent"])
        print(f"  {k:5.2f} {markets / max(1, rounds_ok):13.0%} {len(rs) / max(1, rounds_ok):7.2f} | {'':{len(main)}s}"
              f"  {len(m['fills']) / max(1, n_sent):5.0%} {m['markets_fill'] / max(1, rounds_ok):10.0%} "
              f"{m['sh']:7.1f} {c(m['c5']):>11s} {c(m['cS']):>12s} {m['usd5'] / max(1, n_sent):+10.4f} "
              f"{m['usdS'] / max(1, rounds_ok):+8.4f}")
    print("\n  the same thresholds with narrower caps (fill% of orders sent | $/order at 5s | $/market settled):")
    for k in sorted(by_k, reverse=True):
        cells = []
        for name in names[1:]:
            m = summarise(by_k[k], name)
            n_sent = len(m["sent"])
            cells.append(f"{name} {len(m['fills']) / max(1, n_sent):4.0%} {m['usd5'] / max(1, n_sent):+.4f} "
                         f"{m['usdS'] / max(1, rounds_ok):+.4f}")
        print(f"    {k:5.2f}bp  " + "   ".join(cells))
    print("  'markets with an order' is what the threshold controls; the rest says what those orders earn. Settled is")
    print("  one binary outcome per market - noisy; 5s markout is the steadier read. Replay fills run optimistic vs live")
    print("  (live selection: the venue is slowest on the best moves).")


def exec_study_report(results, n_flights, one_way) -> None:
    """Execution variants on the same decisions, each order at one delay drawn
    from the live canary's own order-out -> match distribution."""
    import statistics
    names = list(EXEC_VARIANTS)
    rows = [r for r in results if r.get("exec")
            and all(r["exec"]["v"][n][0] in ("fill", "miss", "not sent") for n in names)]
    fl = sorted(r["exec"]["flight"] for r in rows)
    q = lambda p: fl[min(len(fl) - 1, int(len(fl) * p / 100))] if fl else float("nan")  # noqa: E731
    print(f"\n  EXECUTION STUDY - {len(rows)} decisions, each at a delay drawn from "
          + (f"{n_flights} live orders' order-out -> match times (one-way {one_way:.1f}ms taken off the reply)"
             if n_flights else "a FIXED delay (no live journal here)")
          + f": drawn p50 {q(50):.0f}ms, p90 {q(90):.0f}ms; tick -> order out {LIVE_OWN_MS:g}ms")
    print("  every variant sized and pre-send checked as live: max(shares x ask, $1) raised to the venue's 5-share")
    print("  minimum (shared walk, +4c headroom), sent only if the decision book fills it - same delay per order")

    def per_share(x, value):
        return (value - x[2] - x[3] / x[1]) * 100

    def stats(sel, n):
        sent = [r for r in sel if r["exec"]["v"][n][0] != "not sent"]
        fills = [r for r in sent if r["exec"]["v"][n][0] == "fill"]
        pnl = [((r["pay"] - r["exec"]["v"][n][2]) * r["exec"]["v"][n][1] - r["exec"]["v"][n][3], r["round"], r)
               for r in fills if r["pay"] is not None]
        mk = {}
        for i in (0, 1):
            pts = [(per_share(r["exec"]["v"][n], r["exec"]["mids"][i]), r["round"]) for r in fills
                   if r["exec"]["mids"][i] is not None]
            mk[i] = clustered(*zip(*pts)) if pts else (None, None, 0)
        st_ = [(per_share(r["exec"]["v"][n], r["pay"]), r["round"]) for r in fills if r["pay"] is not None]
        usd = sum(x[0] for x in pnl)
        path, peak, dd = 0.0, 0.0, 0.0
        for x in sorted(pnl, key=lambda x: x[1]):
            path += x[0]
            peak, dd = max(peak, path), max(dd, peak - path)
        return {"sent": len(sent), "fills": len(fills),
                "partial": sum(1 for r in fills if len(r["exec"]["v"][n]) > 6 and r["exec"]["v"][n][5] is True),
                "shares": statistics.fmean([r["exec"]["v"][n][1] for r in fills]) if fills else 0.0,
                "px": statistics.fmean([r["exec"]["v"][n][2] for r in fills]) if fills else None,
                "slip": statistics.fmean([(r["exec"]["v"][n][2] - r["ask"]) * 100 for r in fills]) if fills else None,
                "cost": statistics.fmean([r["exec"]["v"][n][1] * r["exec"]["v"][n][2] for r in fills]) if fills else 0.0,
                "mk1": mk[0], "mk5": mk[1], "settled": clustered(*zip(*st_)) if st_ else (None, None, 0),
                "usd": usd, "per_sent": usd / max(1, len(sent)), "per_dec": usd / max(1, len(sel)),
                "sd": statistics.pstdev([x[0] for x in pnl]) if len(pnl) > 1 else 0.0,
                "win": sum(1 for x in pnl if x[0] > 0) / max(1, len(pnl)), "worst": min([x[0] for x in pnl] or [0.0]),
                "dd": dd}

    def c(m):
        return "-" if m[0] is None else (f"{m[0]:+5.2f}+-{m[1]:4.2f}" if m[1] else f"{m[0]:+5.2f}")
    all_ = {n: stats(rows, n) for n in names}
    print(f"\n  {'variant':20s} {'sent':>5s} {'fills':>5s} {'fill%':>5s} {'partial':>7s} {'sh/fill':>7s} "
          f"{'avg px':>7s} {'paid-ask c':>10s} {'1s c/sh':>11s} {'5s c/sh':>11s} {'settled':>11s}")
    for n in names:
        x = all_[n]
        print(f"  {n:20s} {x['sent']:>5d} {x['fills']:>5d} {x['fills'] / max(1, x['sent']):5.0%} {x['partial']:>7d} "
              f"{x['shares']:7.1f} {x['px'] if x['px'] is not None else float('nan'):7.3f} "
              f"{x['slip'] if x['slip'] is not None else float('nan'):+10.2f} {c(x['mk1']):>11s} {c(x['mk5']):>11s} "
              f"{c(x['settled']):>11s}")
    base = all_[names[0]]
    print(f"\n  {'variant':20s} {'$ total':>8s} {'$/order':>8s} {'$/decision':>10s} {'vs now':>8s} | RISK: "
          f"{'$/fill':>6s} {'sd $/fill':>9s} {'win%':>5s} {'worst $':>8s} {'max DD $':>8s}")
    for n in names:
        x = all_[n]
        print(f"  {n:20s} {x['usd']:+8.2f} {x['per_sent']:+8.4f} {x['per_dec']:+10.4f} {x['per_dec'] - base['per_dec']:+8.4f}"
              f" |       {x['cost']:6.2f} {x['sd']:9.2f} {x['win']:5.0%} {x['worst']:+8.2f} {x['dd']:8.2f}")
    order = sorted({r["round"] for r in rows})
    mid = order[len(order) // 2] if order else 0
    halves = (("first half", [r for r in rows if r["round"] < mid]), ("second half", [r for r in rows if r["round"] >= mid]))
    print("\n  stability - $/decision vs now in each half (a change must beat now in BOTH):")
    for n in names[1:]:
        cells = []
        for hname, sel in halves:
            a, b = stats(sel, n), stats(sel, names[0])
            cells.append(f"{hname} {a['per_dec'] - b['per_dec']:+.4f}")
        print(f"    {n:20s} " + "   ".join(cells))
    print("  $/decision compares variants that send different numbers of orders (the pre-send check refuses more"
          " when the stake is larger); FAK partials assume the venue's minimums apply to the order as placed.")


def study_report(results, holds) -> None:
    """Which orders survive Polymarket's taker delay, and would a rule fixed
    in advance help? Only orders live would really send (its pre-send FOK
    check passes on the decision book). Rules (fixed before this was run):
      R1 cap by move   +2c under 1.5bp, +3c from 1.5bp, +4c from 2.5bp
      R2 depth >= 2x   only when the decision book holds 2x our shares at the cap
      R3 fresh book    only when our copy of the book is <= 500ms old
      R4 FAK +2c       fill-and-kill at today's cap
      R5 R1 + FAK      cap by move, fill-and-kill
    Scored on the first and second half of the rounds separately: a rule is
    worth a live test only if it beats today's on BOTH halves."""
    h0 = holds[0]

    def kind(x):
        return x[0] if x else "none"
    rows = [r for r in results if r.get("var") and r.get("feat")]
    valid = [r for r in rows if r["feat"]["presend"] == "fill"
             and all(kind(r["var"].get(n)) in ("fill", "miss") for n in STUDY_VARIANTS)]
    refused = sum(1 for r in rows if r["feat"]["presend"] != "fill")
    print(f"\n  STUDY at a {h0:g}ms hold: {len(valid)} orders live would send ({refused} more refused by the "
          f"pre-send check on the decision book: thin or moved before sending)")

    def per_share(r, name, value):
        x = r["var"][name]
        return (value - x[2] - x[3] / x[1]) * 100

    def stats(sel, pick):
        """pick(r) -> variant name for that order (None = not sent)."""
        sent = [(r, pick(r)) for r in sel if pick(r)]
        f = [(r, n) for r, n in sent if kind(r["var"][n]) == "fill"]
        mk = [(per_share(r, n, r["mids"][h0][1]), r["round"]) for r, n in f
              if (r.get("mids") or {}).get(h0, (None, None))[1] is not None]
        st_ = [(per_share(r, n, r["pay"]), r["round"]) for r, n in f if r["pay"] is not None]
        usd = sum((r["pay"] - r["var"][n][2]) * r["var"][n][1] - r["var"][n][3] for r, n in f if r["pay"] is not None)
        return {"n": len(sent), "fill": len(f) / max(1, len(sent)), "mk": clustered(*zip(*mk)) if mk else (None, None, 0),
                "settled": clustered(*zip(*st_)) if st_ else (None, None, 0), "usd": usd,
                "per": usd / max(1, len(sent))}

    def mk_txt(c):
        m, se, k = c
        return "-" if m is None else (f"{m:+5.2f}+-{se:4.2f}" if se else f"{m:+5.2f}")

    buckets = (("BTC move at the signal (bp)", "bp", ((1.0, 1.5), (1.5, 2.0), (2.0, 3.0), (3.0, 1e9))),
               ("depth at the cap / our shares", "depth", ((1.0, 2.0), (2.0, 5.0), (5.0, 1e9))),
               ("age of our book copy (ms)", "age_ms", ((0, 100), (100, 500), (500, 1e9))),
               ("spread at the decision", "spread", ((0, 0.015), (0.015, 0.035), (0.035, 1.0))),
               ("seconds left in the round", "left", ((10, 60), (60, 180), (180, 301))))
    caps = ("FOK +2c (now)", "FOK +3c", "FOK +4c", "FOK +6c")
    for title, key, edges in buckets:
        print(f"\n  by {title}: fill | 5s c/share | $/order, per cap")
        print(f"    {'bucket':>12s} {'orders':>6s}   " + "   ".join(f"{c:>23s}" for c in caps))
        for a, b in edges:
            sel = [r for r in valid if r["feat"].get(key) is not None and a <= r["feat"][key] < b]
            if not sel:
                continue
            cells = []
            for c in caps:
                x = stats(sel, lambda r, c=c: c)
                cells.append(f"{x['fill']:4.0%} {mk_txt(x['mk']):>11s} {x['per']:+6.3f}")
            hi = "+" if b >= 1e9 else f"{b:g}"
            print(f"    {f'{a:g}-{hi}':>12s} {len(sel):>6d}   " + "   ".join(f"{c:>23s}" for c in cells))

    def by_move(r, fak=False):
        bp = r["feat"]["bp"]
        c = "+2c" if bp < 1.5 else "+3c" if bp < 2.5 else "+4c"
        return f"FAK {c}" if fak else ("FOK +2c (now)" if c == "+2c" else f"FOK {c}")
    rules = (("today: FOK +2c", lambda r: "FOK +2c (now)"),
             ("R1 cap by move", lambda r: by_move(r)),
             ("R2 depth >= 2x", lambda r: "FOK +2c (now)" if r["feat"]["depth"] >= 2 else None),
             ("R3 fresh book <=500ms", lambda r: "FOK +2c (now)" if r["feat"]["age_ms"] <= 500 else None),
             ("R4 FAK +2c", lambda r: "FAK +2c"),
             ("R5 R1 + FAK", lambda r: by_move(r, fak=True)))
    order = sorted({r["round"] for r in valid})
    mid = order[len(order) // 2] if order else 0
    halves = (("first half", [r for r in valid if r["round"] < mid]), ("second half", [r for r in valid if r["round"] >= mid]))
    print("\n  RULES fixed in advance, each half of the rounds on its own (a rule must beat today on BOTH):")
    print(f"    {'rule':24s} {'half':12s} {'orders':>6s} {'fill':>5s} {'5s c/share':>13s} {'settled':>13s} "
          f"{'$ total':>8s} {'$/order':>8s} {'vs today $':>10s}")
    for name, pick in rules:
        for hname, sel in halves:
            x, base = stats(sel, pick), stats(sel, rules[0][1])
            print(f"    {name:24s} {hname:12s} {x['n']:>6d} {x['fill']:5.0%} {mk_txt(x['mk']):>13s} "
                  f"{mk_txt(x['settled']):>13s} {x['usd']:+8.2f} {x['per']:+8.4f} {x['usd'] - base['usd']:+10.2f}")
    print("    filters (R2, R3) send fewer orders: compare $ total (what the period earned), not only $/order.")


def latency_table(results, holds, kind, fmt) -> None:
    """The same decisions reaching the venue at each hold: a millisecond saved
    anywhere on the path (feed, signing, send, network) moves the match
    instant exactly like a shorter hold, so `--holds 250,248,245,240` reads as
    before / after a 2, 5 or 10ms improvement. Markouts are taken 1s and 5s
    after each hold's own match; settled = round-end payout; $/order = the
    settled dollars over every decidable order (misses earn 0)."""
    h0 = holds[0]
    print("\n  LATENCY SENSITIVITY - same decisions, the venue reached at each hold (a ms saved anywhere = a ms off "
          "the hold):")
    print(f"  {'hold':>7s} {'fill':>5s}  {'1s c/share':24s} {'5s c/share':24s} {'settled c/share':24s} "
          f"{'$/order':>8s} {'$ total':>9s}")
    base = None
    for h in holds:
        d = [r for r in results if kind(r["venue"].get(h)) in ("fill", "miss")]
        f = [r for r in d if kind(r["venue"][h]) == "fill"]

        def per_share(r, value):
            x = r["venue"][h]
            return (value - x[2] - x[3] / x[1]) * 100

        cols = []
        for i in (0, 1):
            sel = [r for r in f if (r.get("mids") or {}).get(h, (None, None))[i] is not None]
            cols.append(clustered([per_share(r, r["mids"][h][i]) for r in sel], [r["round"] for r in sel]))
        sel = [r for r in f if r["pay"] is not None]
        cols.append(clustered([per_share(r, r["pay"]) for r in sel], [r["round"] for r in sel]))
        usd = sum((r["pay"] - r["venue"][h][2]) * r["venue"][h][1] - r["venue"][h][3] for r in sel)
        per_order = usd / max(1, len(d))
        fill = len(f) / max(1, len(d))
        if base is None:
            base = (fill, per_order)
        delta = "" if h == h0 else f"   vs {h0:g}ms: fill {100 * (fill - base[0]):+.1f}pt, $/order {per_order - base[1]:+.4f}"
        print(f"  {h:5g}ms {fill:5.0%}  {fmt(*cols[0]):24s} {fmt(*cols[1]):24s} {fmt(*cols[2]):24s} "
              f"{per_order:+8.4f} {usd:+9.2f}{delta}")


# ---------------------------------------------------------------- live orders at their real match instant
LIVE_CAPS = (("+2c (sent)", 0.02, False), ("FAK +2c", 0.02, True), ("+3c", 0.03, False), ("+4c", 0.04, False),
             ("+6c", 0.06, False), ("+10c", 0.10, False), ("any to 0.95", 1.0, False))


def load_live_orders(env_path):
    """The live canary's orders, each with the instant the venue processed it:
    sent (decision wall + decision -> first byte) + reply - the way back."""
    import statistics
    import sniper_check as sc
    env = sc.read_env(Path(env_path))
    rows = sc.jsonl(sc.state_path(env, "LIVE_SNIPER_JOURNAL", "live_sniper.jsonl"))
    rtts = [((r.get("live") or {}).get("clock") or {}).get("rtt_ms_p50") for r in rows if r.get("result") == "summary"]
    rtts = [x for x in rtts if x]
    one_way = statistics.median(rtts) / 2.0 if rtts else 13.3
    shadows = {r.get("id"): r.get("L") or {} for r in rows if r.get("result") == "shadow"}
    fills = {r.get("order_id"): r.get("fill") for r in rows if r.get("result") == "live_fill"}
    out = []
    for r in rows:
        ts = r.get("live_ts_us") or {}
        if not (r.get("live") and r.get("result") in ("filled", "refused", "pending")
                and r.get("t_wire_to_reply_ms") is not None and "net_send_start" in ts and r.get("round_end")):
            continue
        send = float(r["wall"]) + ts["net_send_start"] / 1e6
        s200 = shadows.get(r.get("id"), {}).get("200") or {}
        live_filled = r.get("kind") == "MATCHED" or bool(fills.get(r.get("order_id")))
        group = ("live filled" if live_filled else
                 "missed, shadow@200 filled" if "sh" in s200 else "missed, shadow missed too")
        out.append({"r": r, "match_wall": send + (float(r["t_wire_to_reply_ms"]) - one_way) / 1000.0,
                    "w": int(float(r["round_end"])) - 300, "group": group,
                    "shadow_mk5": s200.get("mk5") if "sh" in s200 else None})
    return out, one_way


def score_live_round(D, C, orders):
    """Each live order on the recorded venue book at the instant the venue
    processed it: what each price limit would have filled, and its markouts."""
    import numpy as np
    from latency_lab import load as L
    idx_of = {s_: i for i, (_w, s_) in D["idx_info"].items()}
    heard_wall = np.sort(np.concatenate([v["recv_wall"] for v in D["pm"].values()] or [np.array([])]))
    heard = L.to_true(heard_wall, C)
    out = []
    for o in orders:
        r = o["r"]
        idx = idx_of.get("Up" if r["side"] == "UP" else "Down")
        v = D["pm"].get(idx) if idx is not None else None
        if v is None or not len(v["ts"]):
            continue
        vt = L.pm_true(v["ts"], C)
        vt = np.where(np.isnan(vt), np.inf, vt)
        t = L.to_true(o["match_wall"] * 1e9, C)
        k = int(np.searchsorted(heard, t, side="right")) - 1
        if k < 0 or (t - heard[k]) / 1e9 > GAP_S:
            continue                                     # the recorder was not hearing the feed then
        j = last_at_or_before(vt, t)
        if j is None:
            continue

        def mid_at(tt):
            m = last_at_or_before(vt, tt)
            return None if m is None or math.isnan(v["bb"][m]) or math.isnan(v["ba"][m]) else \
                (float(v["bb"][m]) + float(v["ba"][m])) / 2
        end = mid_at(L.to_true(float(r["round_end"]) * 1e9, C))
        pay = None if end is None else 1.0 if end >= 0.9 else 0.0 if end <= 0.1 else None
        ask = float(r.get("ask_seen") or 0)
        amount = float(r.get("amount") or max(10 * ask, 1.0))
        o2 = dict(o, pay=pay, mid1=mid_at(t + 1e9), mid5=mid_at(t + 5e9),
                  ask_now=None if math.isnan(v["ba"][j]) else float(v["ba"][j]), ask_seen=ask, caps={})
        for name, x, fak in LIVE_CAPS:
            cap = 0.95 if x >= 1 else min(0.95, round(ask + x, 4))
            o2["caps"][name] = walk(v, j, amount, cap, fak=fak)
        out.append(o2)
    return out


def live_orders_report(scored, one_way, n_orders) -> None:
    print(f"\nLIVE ORDERS AT THE INSTANT POLYMARKET PROCESSED THEM - {len(scored)} of {n_orders} live orders on the "
          f"recorded venue book (match = first byte + reply - {one_way:.1f}ms way back)")
    groups = ("live filled", "missed, shadow@200 filled", "missed, shadow missed too")
    for g in groups:
        sel = [o for o in scored if o["group"] == g]
        if not sel:
            continue
        drift = [(o["ask_now"] - o["ask_seen"]) * 100 for o in sel if o["ask_now"] is not None]
        sh = [o["shadow_mk5"] for o in sel if o["shadow_mk5"] is not None]
        print(f"\n  {g}: {len(sel)} orders | ask at the venue's instant minus ask seen: "
              f"{(sum(drift) / len(drift)) if drift else float('nan'):+.1f}c"
              + (f" | their 200ms shadow had marked out {sum(sh) / len(sh):+.1f}c" if sh else ""))
        print(f"    {'limit':14s} {'fills':>6s} {'avg paid - ask seen':>20s} {'1s c/sh':>14s} {'5s c/sh':>14s} "
              f"{'settled c/sh':>14s}")
        for name, _x, _fak in LIVE_CAPS:
            f = [o for o in sel if o["caps"][name][0] == "fill"]

            def ps(o, value):
                q_ = o["caps"][name]
                return (value - q_[2] - q_[3] / q_[1]) * 100
            m1 = [ps(o, o["mid1"]) for o in f if o["mid1"] is not None]
            m5 = [ps(o, o["mid5"]) for o in f if o["mid5"] is not None]
            st_ = [ps(o, o["pay"]) for o in f if o["pay"] is not None]
            paid = [(o["caps"][name][2] - o["ask_seen"]) * 100 for o in f]
            avg = lambda xs: f"{sum(xs) / len(xs):+6.2f} n={len(xs):<3d}" if xs else "-"  # noqa: E731
            print(f"    {name:14s} {len(f):>3d}/{len(sel):<3d} {avg(paid):>20s} {avg(m1):>14s} {avg(m5):>14s} "
                  f"{avg(st_):>14s}")
    print("\n  read it: in the 'missed, shadow@200 filled' block, if a wider limit fills at the venue's real instant")
    print("  WITH a positive 5s markout, the +2c limit is too tight; if the fills it adds mark out <= 0 (or nothing")
    print("  fills), there was no profitable price left by the time Polymarket processed the order.")
    limits_vs_sent(scored)


def _order_pnl(o, name, value):
    """$ the fill at limit `name` makes at `value` a share: 0.0 without a fill,
    None when the fill's value is not on record."""
    q_ = o["caps"][name]
    if q_[0] != "fill":
        return 0.0
    return None if value is None else q_[1] * (value - q_[2]) - q_[3]


def limits_vs_sent(scored) -> None:
    """Each limit against the one live sent, order by order on every order: the
    bot cannot tell a winner from a loser when it sends, so a limit applies to
    all of them. Standard errors clustered by round."""
    base = LIVE_CAPS[0][0]
    print(f"\n  ALL {len(scored)} ORDERS - what each limit adds over {base}, paired order by order ($ per order sent,")
    print("  standard error clustered by round; 'orders for t=2' = how many orders a test needs to settle it if the")
    print("  effect stays what it is now)")
    print(f"    {'limit':14s} {'extra fills':>11s} {'their 5s c/sh':>24s} {'5s $/order vs sent':>28s} "
          f"{'settled $/order vs sent':>28s} {'orders for t=2':>15s}")

    def cell(m, se, digits):
        if m is None:
            return "-"
        if se is None or se == 0:
            return f"{m:+.{digits}f}"
        return f"{m:+.{digits}f} se {se:.{digits}f} t={m / se:+.1f}"
    for name, _x, _fak in LIVE_CAPS[1:]:
        extra = [o for o in scored if o["caps"][name][0] == "fill" and o["caps"][base][0] != "fill"]
        their = [((o["mid5"] - o["caps"][name][2]) - o["caps"][name][3] / o["caps"][name][1]) * 100
                 for o in extra if o["mid5"] is not None]
        m_e, se_e, _n = clustered(their, [o["w"] for o in extra if o["mid5"] is not None])
        out = {}
        for key in ("mid5", "pay"):
            d, w = [], []
            for o in scored:
                a, b = _order_pnl(o, name, o[key]), _order_pnl(o, base, o[key])
                if a is not None and b is not None:
                    d.append(a - b)
                    w.append(o["w"])
            out[key] = clustered(d, w)
        m5, se5, n5 = out["mid5"]
        need = "-"
        if m5 and se5:
            t = abs(m5 / se5)
            need = "now" if t >= 2 else f"~{int(round(n5 * (2 / t) ** 2))}"
        print(f"    {name:14s} {len(extra):>11d} {cell(m_e, se_e, 2):>24s} {cell(m5, se5, 4):>28s} "
              f"{cell(*out['pay'][:2], 4):>28s} {need:>15s}")
    print("  a limit is worth a live test only where its 5s $/order vs sent is positive; t below 2 = not settled yet")


def live_orders_main(args) -> int:
    try:
        from latency_lab import load as L
    except ImportError as exc:
        print(f"cannot load the lab tools ({exc}); on the VPS: .venv/bin/pip install numpy")
        return 1
    orders, one_way = load_live_orders(args.env)
    if not orders:
        print("no live orders with reply times in the live journal")
        return 1
    by_round = collections.defaultdict(list)
    for o in orders:
        by_round[o["w"]].append(o)
    paths = L.files()
    probes, rounds = L.scan(paths)
    base = L.clock_from_probes(probes)
    want = {w: rounds[w] for w in by_round if w in rounds}
    print(f"lab data: {L.DATA} ({len(paths)} files) | live orders: {len(orders)} in {len(by_round)} rounds, "
          f"{len(want)} of them recorded", file=sys.stderr)
    scored = []
    for w, D, complete in L.cached_rounds(paths, want, probes):
        if not complete:
            continue
        C = dict(base, pm_residual_ns=L.pm_residual(D["pm"], base))
        scored += score_live_round(D, C, by_round[w])
    live_orders_report(scored, one_way, len(orders))
    return 0


# ---------------------------------------------------------------- report
def report(results, payout, marks, holds, coverage):
    print(f"orders in the journal scope: {coverage['orders']} | rounds the lab recorded completely: "
          f"{coverage['rounds_ok']} of {coverage['rounds']} | orders re-scored: {len(results)}")
    if not results:
        print("nothing to re-score: " + ("no paper orders in the journal scope (is PAPER_AUDIT_PATH the bot's?)"
                                          if not coverage["orders"] else
                                          "the lab recorder was not running for these orders "
                                          "(set LAB_DATA to the recorder's folder)"))
        return
    skipped = collections.Counter(r["skip"] for r in results if r.get("skip"))
    for why, n in skipped.items():
        print(f"  skipped {n}: {why}")
    rs = [r for r in results if not r.get("skip")]
    h0 = holds[0]

    def kind(x):
        return x[0] if x else "none"
    orig_fill = [r["order"]["result"] == "filled" for r in rs]

    # 1. the recordings line up with what paper saw
    rc = [(of, kind(r["receipt"])) for of, r in zip(orig_fill, rs) if r["receipt"] and kind(r["receipt"]) != "unknown"]
    agree = sum(1 for of, k in rc if of == (k == "fill"))
    print(f"\nCHECK - the recorder's RECEIPT-time book vs what paper reported: same fill/miss on {agree} of "
          f"{len(rc)} orders ({agree / max(1, len(rc)):.0%}). High agreement = the data lines up; the "
          f"difference below is then the venue clock, not the data.")

    # 2. the venue-time result - every comparison on the SAME orders: those
    # the venue-time book can decide at the headline hold
    vk = [kind(r["venue"].get(h0)) for r in rs]
    known = [(of, k) for of, k in zip(orig_fill, vk) if k in ("fill", "miss")]
    unknown = sum(1 for k in vk if k == "unknown")
    dec = [r for r, k in zip(rs, vk) if k in ("fill", "miss")]
    ff = sum(1 for of, k in known if of and k == "miss")
    fm = sum(1 for of, k in known if not of and k == "fill")
    both = sum(1 for of, k in known if of and k == "fill")
    print(f"\nVENUE-TIME RE-SCORE (hold {h0:g}ms, the paper model's flight):")
    print(f"  on the {len(known)} orders the venue book decides: paper filled "
          f"{sum(1 for of, _k in known if of) / max(1, len(known)):.0%} -> venue-time "
          f"{sum(1 for _o, k in known if k == 'fill') / max(1, len(known)):.0%}   ({unknown} more unknown; "
          f"paper filled {sum(orig_fill) / len(rs):.0%} of all {len(rs)})")
    print(f"  filled in both {both} | FALSE FILLS (paper filled, the venue book could not) {ff} | "
          f"fills paper missed {fm}")
    unk_why = collections.Counter(r["venue"][h0][1] for r, k in zip(rs, vk) if k == "unknown")
    if unk_why:
        print("  unknown: " + ", ".join(f"{w} {n}" for w, n in unk_why.most_common()))
    miss_why = collections.Counter(r["venue"][h0][1] for r, k in zip(rs, vk) if k == "miss")
    if miss_why:
        print("  venue-time misses: " + ", ".join(f"{w} {n}" for w, n in miss_why.most_common()))

    # 3. what each set of fills earned
    def money(rows, which):
        mk, st, sh, cl = [], [], 0.0, []
        for r in rows:
            o = r["order"]
            if which == "paper":
                if o["result"] != "filled" or not o.get("shares"):
                    continue
                shares, px, fee = float(o["shares"]), float(o["price"]), float(o.get("fee") or 0)
                m5 = (marks.get(o.get("order_id")) or {}).get("markout_5s_c")
            else:
                x = r["venue"].get(h0)
                if kind(x) != "fill":
                    continue
                shares, px, fee = x[1], x[2], x[3]
                m5 = None if r.get("mid5") is None else (r["mid5"] - px - fee / shares) * 100
            pay = payout.get(r["token"])
            if m5 is not None:
                mk.append(m5)
            if pay is not None:
                st.append((pay - px - fee / shares) * 100)
                cl.append(o["round_end"])
                sh += shares
        return mk, st, cl, sh
    print("\n  c/share (clustered by round), decidable orders   5s markout                settled")
    for name, which, sel in (("paper as reported", "paper", dec), ("venue-time re-score", "venue", dec),
                             ("  paper's FALSE fills only", "paper",
                              [r for r, of, k in zip(rs, orig_fill, vk) if of and k == "miss"]),
                             ("  paper fills the venue can't judge", "paper",
                              [r for r, of, k in zip(rs, orig_fill, vk) if of and k == "unknown"])):
        mk, st, cl, _sh = money(sel, which)
        m, ms, mn = clustered(mk, [0] * len(mk)) if mk else (None, None, 0)
        s, ss, sn = clustered(st, cl)
        fmt = lambda a, b, n: (f"{a:+6.2f} +-{b:4.2f} (t={a / b:+.1f}, n={n})" if b else
                               (f"{a:+6.2f} (n={n})" if a is not None else "-"))
        print(f"  {name:46s} {fmt(m, ms, mn):26s} {fmt(s, ss, sn)}")
    nop = sum(1 for r in rs if r["token"] not in payout)
    if nop:
        print(f"  ({nop} orders on tokens with no settled payout in the paper ledger: not in 'settled')")

    # 4. how much the answer leans on the assumed hold
    print("\n  sensitivity to the venue's taker hold (fill rate | settled c/share):")
    for h in holds:
        ks = [kind(r["venue"].get(h)) for r in rs]
        dec = [k for k in ks if k in ("fill", "miss")]
        st = [(payout[r["token"]] - x[2] - x[3] / x[1]) * 100 for r in rs
              if kind(x := r["venue"].get(h)) == "fill" and r["token"] in payout]
        cl = [r["order"]["round_end"] for r in rs if kind(r["venue"].get(h)) == "fill" and r["token"] in payout]
        s, ss, n = clustered(st, cl)
        print(f"    hold {h:5g}ms: fill {sum(1 for k in dec if k == 'fill') / max(1, len(dec)):5.0%} | settled "
              + (f"{s:+.2f} +-{ss:.2f} c/share (n={n})" if ss else (f"{s:+.2f} (n={n})" if s is not None else "-")))


def sim_settings(holds):
    """The sniper's settings (from the environment, as the bot reads them) for
    a replay at these holds."""
    import sniper_mode
    st = sniper_mode.Settings()
    for k, v in (("k_bp", 1.0), ("window_ms", 250.0), ("cooldown_ms", 1000.0), ("min_left_s", 10.0),
                 ("shares", 10.0), ("slip", 0.02), ("max_fills_round", 20), ("max_side_shares", 200.0),
                 ("stale_ms", 2000.0)):
        if not hasattr(st, k):
            setattr(st, k, v)
    st.holds = holds
    st.study, st.exec_flights, st.variants = False, None, None
    return st


def simulate_main(args, holds) -> int:
    try:
        from latency_lab import load as L
    except ImportError as exc:
        print(f"cannot load the lab tools ({exc}); on the VPS: .venv/bin/pip install numpy")
        return 1
    import sniper_mode
    st = sim_settings(holds)
    st.study = bool(getattr(args, "study", False))
    st.exec_flights, n_flights, one_way = None, 0, 13.3
    if getattr(args, "exec_study", False):
        flights, one_way = live_flights(args.env)
        n_flights = len(flights)
        st.exec_flights = flights or [16.8 + holds[0] + 25.0 - LIVE_OWN_MS]
    st.variants = STUDY_VARIANTS if st.study else (VARIANTS if getattr(args, "variants", False) else None)
    if getattr(args, "k_study", None):
        return k_study_main(args, st, holds)
    lo = float(os.environ.get("MIN_BUY_PRICE", 0.05))
    hi = float(os.environ.get("MAX_BUY_PRICE", 0.95))
    paths = L.files()
    probes, rounds = L.scan(paths)
    base = L.clock_from_probes(probes)
    print(f"lab data: {L.DATA} ({len(paths)} files, {len(rounds)} rounds) | rule: {st.k_bp:g}bp in "
          f"{st.window_ms:g}ms, cooldown {st.cooldown_ms:g}ms, ask {lo:g}-{hi:g}, {st.shares:g} sh at ask+"
          f"{st.slip:g}, <= {st.max_fills_round} fills/round", file=sys.stderr)
    results, ok, t0 = [], 0, time.time()
    for i, (w, D, complete) in enumerate(L.cached_rounds(paths, rounds, probes)):
        if not complete:
            continue
        ok += 1
        C = dict(base, pm_residual_ns=L.pm_residual(D["pm"], base))
        results += simulate_round(D, C, w, st, sniper_mode.Trigger, lo, hi, lag_ms=args.decision_lag_ms)
        if i % 25 == 0:
            print(f"  ... {i + 1}/{len(rounds)} rounds, {len(results)} orders, {time.time() - t0:.0f}s", file=sys.stderr)
    simulate_report(results, holds, ok, ok * 300 / 3600, args.decision_lag_ms)
    if st.study:
        study_report(results, holds)
    if st.exec_flights:
        exec_study_report(results, n_flights, one_way)
    return 0


def env_band(env_path):
    """MIN_BUY_PRICE / MAX_BUY_PRICE as the bot reads them (shell, else .env)."""
    vals = {}
    try:
        from dotenv import dotenv_values
        vals = dotenv_values(env_path) or {}
    except Exception:  # noqa: BLE001 - no python-dotenv / no file: the defaults
        pass
    get = lambda k, d: float(os.environ.get(k) or vals.get(k) or d)  # noqa: E731
    return get("MIN_BUY_PRICE", 0.05), get("MAX_BUY_PRICE", 0.95)


def k_study_main(args, st, holds) -> int:
    from latency_lab import load as L
    import sniper_mode
    ks = sorted({float(x) for x in str(args.k_study).split(",") if x.strip()}, reverse=True)
    flights, _one_way = live_flights(args.env)
    st.exec_flights = flights or [16.8 + holds[0] + 25.0 - LIVE_OWN_MS]
    st.exec_variants = K_VARIANTS
    st.cooldown_eligible_only = True
    lo, hi = env_band(args.env)
    paths = L.files()
    probes, rounds = L.scan(paths)
    base = L.clock_from_probes(probes)
    print(f"lab data: {L.DATA} ({len(paths)} files, {len(rounds)} rounds) | thresholds {ks} | live delays "
          f"{len(flights)}", file=sys.stderr)
    by_k, ok, t0 = {k: [] for k in ks}, 0, time.time()
    for i, (w, D, complete) in enumerate(L.cached_rounds(paths, rounds, probes)):
        if not complete:
            continue
        ok += 1
        C = dict(base, pm_residual_ns=L.pm_residual(D["pm"], base))
        for k in ks:
            st.k_bp = k
            by_k[k] += [r for r in simulate_round(D, C, w, st, sniper_mode.Trigger, lo, hi) if r.get("exec")]
        if i % 50 == 0:
            print(f"  ... {i + 1}/{len(rounds)} rounds, {time.time() - t0:.0f}s", file=sys.stderr)
    k_study_report(by_k, ok, len(flights), (lo, hi), st.cooldown_ms)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--hours", type=float, default=None, help="only orders from the last N hours (default: all)")
    ap.add_argument("--all", action="store_true", help="every journalled order (the default)")
    ap.add_argument("--env", default=str(ROOT / ".env"))
    ap.add_argument("--holds", default="250,200,300",
                    help="taker holds to try, ms; the first is the headline (250 = Polymarket's documented hold)")
    ap.add_argument("--simulate", action="store_true",
                    help="replay the sniper's rule on every lab round (no journal needed)")
    ap.add_argument("--decision-lag-ms", type=float, default=0.0,
                    help="--simulate: decide on a book this much older than the recorder's (the bot's own lag)")
    ap.add_argument("--live-orders", action="store_true",
                    help="every live canary order on the recorded venue book at the instant Polymarket processed "
                         "it: would a wider price limit have filled it, and would that fill have paid?")
    ap.add_argument("--k-study", default=None, metavar="BP,BP,...",
                    help="--simulate: per BTC-move threshold (e.g. 1,0.7,0.5,0.3): share of markets with an order and "
                         "with a fill, orders per market, and what FAK at +10c (and +4c, FOK +2c) earned at live delays")
    ap.add_argument("--exec-study", action="store_true",
                    help="--simulate: FOK 10/5 @ +2c, FOK 10 @ +3c/+4c, FAK 10 @ +2c on the same decisions, each "
                         "order at a delay drawn from the live canary's own order-out -> match times")
    ap.add_argument("--study", action="store_true",
                    help="--simulate: which orders survive the taker delay (by move, depth, book age, spread, "
                         "time left) and rules fixed in advance, scored on each half of the rounds")
    ap.add_argument("--variants", action="store_true",
                    help="--simulate: also price fill-and-kill / 5 shares / wider caps on the same decisions "
                         "at the first --holds value")
    args = ap.parse_args()
    holds = [float(x) for x in args.holds.split(",")]
    if args.live_orders:
        return live_orders_main(args)
    if args.simulate:
        return simulate_main(args, holds)
    try:
        from latency_lab import load as L
    except ImportError as exc:
        print(f"cannot load the lab tools ({exc}); on the VPS: .venv/bin/pip install numpy")
        return 1
    orders, marks, payout = load_orders(args)
    by_round = collections.defaultdict(list)
    for r in orders:
        by_round[int(float(r["round_end"])) - 300].append(r)
    paths = L.files()
    print(f"lab data: {L.DATA} ({len(paths)} files) | journal orders: {len(orders)} in {len(by_round)} rounds",
          file=sys.stderr)
    probes, rounds = L.scan(paths)
    base = L.clock_from_probes(probes)
    want = {w: rounds[w] for w in by_round if w in rounds}
    span = lambda a, b: f"{time.strftime('%m-%d %H:%M', time.gmtime(a))} -> {time.strftime('%m-%d %H:%M', time.gmtime(b))} UTC"
    if rounds:
        print(f"lab recorded rounds {span(min(rounds), max(rounds) + 300)} | journal orders "
              f"{span(min(by_round), max(by_round) + 300) if by_round else '-'}")
    results, ok, t0 = [], 0, time.time()
    for i, (w, D, complete) in enumerate(L.cached_rounds(paths, want, probes)):
        if not complete:
            continue
        ok += 1
        C = dict(base, pm_residual_ns=L.pm_residual(D["pm"], base))
        results += score_round(D, C, by_round[w], holds)
        if i % 25 == 0:
            print(f"  ... {i + 1}/{len(want)} rounds, {time.time() - t0:.0f}s", file=sys.stderr)
    report(results, payout, marks, holds,
           {"orders": len(orders), "rounds": len(by_round), "rounds_ok": ok})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
