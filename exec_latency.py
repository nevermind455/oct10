"""Execution latency, execution quality and the frozen sniper spec.

Pure bookkeeping on stamps the order path already takes - nothing here runs
between the trigger and the send. The Sniper calls observe_*() when an
order's outcome is journalled (after its bytes left), and the dashboard reads
the summaries.

Stamps, one clock (time.perf_counter_ns) unless noted:

    T0  feed event received by the application   Sniper.on_tick, first line
    T1  sniper trigger fired                      Sniper._signal, the rule said yes
    T2  strategy decision completed               Sniper._decide, every gate passed
    T3  order construction completed              FastOrderBuilder, just before the ECDSA step
    T4  signing completed                         FastOrderBuilder, right after it
    T5  request dispatched                        FastH2: HEADERS+DATA handed to the kernel
    T6  Polymarket ACK received                   FastH2: the reply's HEADERS frame
    T7  first authoritative fill observation      the reply (no match), the private
                                                  channel's last trade, or the order status
                                                  (time.monotonic - same clock as T0's tick time)

Paper has no venue: its T5 is the request ready to send, and its T6/T7 are
the modelled match (measured one-way + the venue hold + an allowance) -
labelled SIMULATED, never mixed with live numbers.
"""
from __future__ import annotations

import collections
import math
import os

# ---------------------------------------------------------------- the frozen spec
# The sniper as specified. LIVE refuses to start when a resolved setting
# differs: an old .env value used to win silently. Changing the strategy means
# changing this table (reviewed), not an environment variable.
# 2026-10-07: 0.50bp / ask+$0.05 -> 1.00bp / ask+$0.04 (the operator's change):
# one IOC that buys what is offered up to the reference ask + 4c, never more.
# 2026-10-07 (later): 1.00bp -> 0.50bp (the operator's change); still one FAK
# limit order up to the reference ask + $0.04.
# 2026-10-07 (later): ask + $0.04 -> ask + $0.05 (the operator's spec): one IOC per
# signal up to the reference ask + 5c, partial fills kept, the unfilled rest carried
# to the next same-side signal (cap 20). No retry, no chasing.
# 2026-10-10: trigger feed binance -> coinbase (the operator's change): the same
# 0.5bp / 250ms rule on Coinbase Exchange's BTC-USD ticker (best bid/ask as of
# each match; heartbeats for liveness; its own freshness gate). Binance and
# Kraken stay selectable in paper.
# 2026-10-11: trigger 0.50bp -> 0.30bp (the operator's change): the same 250ms
# window on the Coinbase mid, a smaller move.
# 2026-10-11 (later): 0.30bp -> 0.50bp again (the operator's change).
# 2026-10-11: every buy only at or under the share's fair value less the taker
# fee and a $0.01 margin (the operator's rule); the limit is capped there too.
# 2026-10-11 (later): the fair value is MARKET-ANCHORED (sniper_mode.anchored_fair:
# the market's mid for the side, moved by the BTC move over the volatility and time
# left) - the start-price model disagreed with the market by 10-30c and bought the
# losing side; and FAK orders are sized at 1/4 Kelly on it, 5 to 20 shares, from the
# account's cash (the operator's choice), replacing the fixed 10 shares + carry.
# 2026-10-11 (later): 1/4 Kelly -> full Kelly, and no per-order cap (the operator's
# changes): an order is bounded only by what is left of the side's 200-share cap in
# the round, the cash, and live, the canary's per-order cap.
FROZEN_SPEC = (
    # (Settings attribute, environment variable, frozen value, label, format)
    ("primary_feed", "SNIPER_PRIMARY_FEED", "coinbase", "trigger feed", "{}"),
    ("k_bp", "SNIPER_K_BP", 0.5, "trigger", "{:.2f} bp"),
    ("window_ms", "SNIPER_WINDOW_MS", 250.0, "window", "{:.0f} ms"),
    ("order_type", "SNIPER_ORDER_TYPE", "FAK", "execution", "{}"),
    ("slip", "SNIPER_SLIP", 0.05, "slip", "${:.2f}"),
    ("cooldown_ms", "SNIPER_COOLDOWN_MS", 300.0, "cooldown", "{:.0f} ms / side"),
    ("shares", "SNIPER_SHARES", 10.0, "base_target", "{:g}"),
    ("max_target_shares", "SNIPER_MAX_TARGET_SHARES", 20.0, "max_target", "{:g}"),
    ("fair", "SNIPER_FAIR", True, "fair value", "{}"),
    ("fair_margin", "SNIPER_FAIR_MARGIN", 0.01, "fair margin", "${:.2f}"),
    ("kelly", "SNIPER_KELLY", True, "kelly size", "{}"),
    ("kelly_fraction", "SNIPER_KELLY_FRACTION", 1.0, "kelly fraction", "{:g}"),
)


def _same(a, b) -> bool:
    if isinstance(b, float):
        try:
            return math.isclose(float(a), b, rel_tol=0.0, abs_tol=1e-9)
        except (TypeError, ValueError):
            return False
    return str(a).strip().lower() == str(b).strip().lower()


def spec_violations(settings, canary_max_shares=None) -> list[str]:
    """Every way the resolved settings differ from FROZEN_SPEC, named with the
    variable that set it. Empty when they match."""
    out = []
    for attr, var, want, label, fmt in FROZEN_SPEC:
        got = getattr(settings, attr, None)
        if not _same(got, want):
            src = f"{var}={os.environ.get(var)!r}" if os.environ.get(var) not in (None, "") else f"{var} unset (code default)"
            out.append(f"{label}: resolved {fmt.format(got) if got is not None else got} but the frozen spec is "
                       f"{fmt.format(want)} ({src})")
    extra = tuple(getattr(settings, "extra_feeds", ()) or ())
    if extra:
        # A second trigger (e.g. binance_fut since 2026-10-13) is a paper A/B
        # test: it changes which moves trade, so it is not the frozen spec.
        out.append(f"second trigger: SNIPER_EXTRA_FEEDS={','.join(extra)} - the frozen spec trades on the "
                   f"primary feed alone (a second trigger is paper-only until its A/B result is reviewed)")
    target = float(getattr(settings, "max_target_shares", 0) or 0)
    # `not >=` rather than `<`: a NaN cap compares false both ways and must fail here too.
    if canary_max_shares is not None and not float(canary_max_shares) + 1e-9 >= target:
        out.append(f"canary_cap: SNIPER_CANARY_MAX_SHARES={float(canary_max_shares):g} is below the maximum carried "
                   f"target {target:g} - a carried order would be refused")
    return out


def effective_config_lines(settings, *, live: bool, canary_max_shares=None) -> list[str]:
    """The resolved configuration, one line per setting, with where it came
    from. No credentials: only SNIPER_* strategy values."""
    lines = [f"SNIPER {'LIVE' if live else 'PAPER'} CONFIG (resolved: environment / .env, else code default)"]
    for attr, var, want, label, fmt in FROZEN_SPEC:
        got = getattr(settings, attr, None)
        src = "env" if os.environ.get(var) not in (None, "") else "default"
        mark = "" if _same(got, want) else f"   <- frozen spec {fmt.format(want)}"
        lines.append(f"  {label:13s} {fmt.format(got) if got is not None else '--':16s} ({var}, {src}){mark}")
    if canary_max_shares is not None:
        target = float(getattr(settings, "max_target_shares", 0) or 0)
        ok = float(canary_max_shares) + 1e-9 >= target
        lines.append(f"  {'canary_cap':13s} {float(canary_max_shares):g} shares{'':9s} (SNIPER_CANARY_MAX_SHARES)"
                     + ("" if ok else f"   <- must be >= {target:g}"))
    lines.append(f"  {'signing':13s} {signing_backend()}")
    # F1 (audit 2026-10-07): say what is a hard limit and what is an estimate.
    lines.append(f"  {'hard caps':13s} dollars: each IOC signs its stake (makerAmount, USDC) - the venue cannot "
                 f"take more; the canary spend cap and cash are enforced on it")
    lines.append(f"  {'share caps':13s} ESTIMATES: target / max target / side cap / canary shares-per-order are "
                 f"checked at the decision's prices; a dollar-sized IOC buys MORE shares if the ask falls before "
                 f"the match (journal: over_target)")
    return lines


def signing_backend() -> str:
    """Which ECDSA implementation eth_keys signs with here. Measured: about
    0.1ms per order with coincurve, about 1.4ms with the pure-Python fallback."""
    try:
        from eth_keys.backends import get_backend
        name = type(get_backend()).__name__
    except Exception as exc:  # noqa: BLE001 - reported, not fatal
        return f"unknown ({type(exc).__name__})"
    return {"CoinCurveECCBackend": "coincurve (C)",
            "NativeECCBackend": "pure-Python eth_keys (slow: pip install coincurve)"}.get(name, name)


# ---------------------------------------------------------------- stage math
STAGES = ("feed_to_trigger", "trigger_to_decision", "build", "sign", "dispatch", "send_to_ack", "ack_to_fill")
TOTALS = ("trigger_to_send", "trigger_to_ack", "trigger_to_fill")


def _ms(a, b):
    if a is None or b is None:
        return None
    return round((b - a) / 1e6, 4)


def stages(*, t0, t1, t2, t3, t4, t5, t6) -> dict:
    """The T0..T6 intervals in ms (None where a stamp is missing)."""
    t3 = t3 if t3 is not None else t4          # no split stamp: build and sign show as one
    out = {"feed_to_trigger": _ms(t0, t1), "trigger_to_decision": _ms(t1, t2), "build": _ms(t2, t3),
           "sign": _ms(t3, t4), "dispatch": _ms(t4, t5), "send_to_ack": _ms(t5, t6),
           "trigger_to_send": _ms(t1, t5), "trigger_to_ack": _ms(t1, t6)}
    return {k: v for k, v in out.items() if v is not None}


def live_stamps(decided_pc: int, ts_us: dict) -> dict:
    """LeanSender's stamps (us after the decision) back to perf_counter_ns."""
    def at(key):
        v = ts_us.get(key)
        return None if v is None else decided_pc + int(v) * 1000
    return {"t3": at("sign_start"), "t4": at("sign_end") or at("signed"), "t5": at("net_send_done") or at("net_send_start"),
            "t6": at("response")}


def with_fill(lat: dict, *, t_ms: float, t0: int | None, t1: int | None, seen_mono: float | None) -> dict:
    """Add T7: trigger -> fill known and ACK -> fill known. seen_mono is
    time.monotonic seconds (the tick's clock); None means the reply itself
    settled the quantity (T7 = T6). Never earlier than the ACK: the order id
    the fill is matched by comes in the reply."""
    ack = lat.get("trigger_to_ack")
    if seen_mono is None:
        fill = ack
    else:
        lead = (t1 - t0) / 1e6 if t0 is not None and t1 is not None else 0.0
        fill = float(seen_mono) * 1000.0 - float(t_ms) - lead
        if ack is not None:
            fill = max(fill, ack)
    if fill is None:
        return lat
    lat = dict(lat, trigger_to_fill=round(fill, 4))
    if ack is not None:
        lat["ack_to_fill"] = round(fill - ack, 4)
    return lat


# ---------------------------------------------------------------- rolling statistics
class LatencyStats:
    """LAST / AVG / P50 / P95 / P99 / MAX per metric over the newest `keep`
    orders, live and simulated kept apart. Computed on read, cached until the
    next sample - the dashboard reads it once a second."""

    def __init__(self, keep: int = 1000) -> None:
        self.keep = keep
        self.samples: dict = collections.defaultdict(lambda: collections.deque(maxlen=keep))
        self._version = 0
        self._cache = (None, None)

    def add(self, lat: dict, *, simulated: tuple = ()) -> None:
        for k, v in lat.items():
            if v is None or not isinstance(v, (int, float)) or not math.isfinite(v):
                continue
            self.samples[(k, k in simulated)].append(float(v))
        self._version += 1

    @staticmethod
    def describe(xs) -> dict | None:
        if not xs:
            return None
        s = sorted(xs)
        q = lambda p: s[min(len(s) - 1, int(len(s) * p / 100))]  # noqa: E731
        return {"n": len(s), "last": xs[-1], "avg": sum(s) / len(s), "p50": q(50), "p95": q(95), "p99": q(99),
                "max": s[-1]}

    def summary(self) -> dict:
        if self._cache[0] == self._version:
            return self._cache[1]
        out = {}
        for (k, sim), xs in self.samples.items():
            d = self.describe(list(xs))
            if d is None:
                continue
            d["simulated"] = sim
            if k not in out or (out[k]["simulated"] and not sim):     # a measured figure wins over a modelled one
                out[k] = d
        self._cache = (self._version, out)
        return out


# ---------------------------------------------------------------- execution quality
class ExecQuality:
    """Order outcomes and prices. One observe() per order when its first
    outcome is journalled, one resolve() when a pending fill's quantity
    becomes known."""

    def __init__(self) -> None:
        self.n = collections.Counter()
        self.slips: collections.deque = collections.deque(maxlen=1000)
        self.last: dict = {}

    def observe(self, row: dict) -> None:
        n = self.n
        n["attempted"] += 1
        kind = row.get("kind")
        result = row.get("result")
        live = bool(row.get("live"))
        reason = str(row.get("reason") or "").lower()
        if live:
            if kind != "NOT_SENT" and result != "error":
                n["sent"] += 1
            if kind in ("MATCHED", "PENDING", "NO_MATCH", "REJECTED"):
                n["acked"] += 1
        else:
            if "signing failed" not in reason and "request build failed" not in reason and result != "error":
                n["sent"] += 1
                n["acked"] += 1                          # paper: the simulated venue always answers
        if result == "pending":
            n["pending"] += 1
        elif result == "filled":
            self._filled(row)
        elif result == "error" or kind in ("AMBIGUOUS", "NOT_SENT"):
            n["failed"] += 1
        elif kind == "NO_MATCH" or "no-fill" in reason or "zero matched" in reason:
            n["zero_fill"] += 1
        else:
            n["rejected"] += 1
        self.last = {k: row.get(k) for k in ("side", "signal_mid", "ask_seen", "cap", "price", "shares", "result")}

    def resolve(self, row: dict) -> None:
        """A pending order's quantity is known (row is its updated row)."""
        self.n["pending"] = max(0, self.n["pending"] - 1)
        if float(row.get("shares") or 0) > 0:
            self._filled(row)
        else:
            self.n["zero_fill"] += 1
        self.last = {k: row.get(k) for k in ("side", "signal_mid", "ask_seen", "cap", "price", "shares", "result")}

    def _filled(self, row: dict) -> None:
        self.n["partial" if row.get("partial") else "filled"] += 1
        try:
            slip = (float(row["price"]) - float(row["ask_seen"])) * 100.0
        except (KeyError, TypeError, ValueError):
            return
        if math.isfinite(slip):
            self.slips.append(slip)

    def summary(self) -> dict:
        n = self.n
        acked = n["acked"]
        filled_any = n["filled"] + n["partial"]
        slip = None
        if self.last.get("price") is not None and self.last.get("ask_seen") is not None:
            try:
                slip = round((float(self.last["price"]) - float(self.last["ask_seen"])) * 100.0, 2)
            except (TypeError, ValueError):
                slip = None
        return {"attempted": n["attempted"], "sent": n["sent"], "acked": acked, "filled": n["filled"],
                "partial": n["partial"], "zero_fill": n["zero_fill"], "rejected": n["rejected"],
                "failed": n["failed"], "pending": n["pending"],
                "fill_rate": round(filled_any / acked, 4) if acked else None,
                "reject_rate": round(n["rejected"] / n["sent"], 4) if n["sent"] else None,
                "last": dict(self.last, slip_c=slip),
                "avg_slip_c": round(sum(self.slips) / len(self.slips), 3) if self.slips else None,
                "slip_n": len(self.slips)}
