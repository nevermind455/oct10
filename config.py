"""Runtime configuration with fail-fast validation."""
import math
import os
import re
import stat
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from dotenv import load_dotenv


_ENV_PATH = Path(__file__).resolve().parent / ".env"
if _ENV_PATH.exists():
    if _ENV_PATH.is_symlink() or not _ENV_PATH.is_file():
        raise PermissionError(".env must be a regular, non-symlink file")
    # POSIX makes private-file permissions directly inspectable.  Windows
    # operators must apply the ACL documented in SECURITY.md.
    if os.name != "nt" and stat.S_IMODE(_ENV_PATH.stat().st_mode) & 0o077:
        raise PermissionError(".env contains live credentials and must have mode 0600")
load_dotenv(_ENV_PATH, override=False, encoding="utf-8")


def _env_text(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip()


def _env_float(name: str, default: str) -> float:
    raw = _env_text(name, default)
    try:
        return float(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be a number") from exc


def _env_int(name: str, default: str | None = None) -> int | None:
    raw = _env_text(name, default)
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _env_bool(name: str, default: bool | None = False) -> bool | None:
    raw = _env_text(name)
    if raw in (None, ""):
        return default
    value = raw.lower()
    if value in ("1", "true", "yes", "on"):
        return True
    if value in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"{name} must be one of 1/0, true/false, yes/no, on/off")

SYMBOL = "BTCUSDT"
# $2.50 is not a round number by accident: the venue minimum is 5 shares, and
# at the top of the phase-1 band (0.50) that is exactly $2.50. Anything less
# and the engine sizes the order up at the most expensive price in the band.
# It is also the largest size whose bad-case drawdown fits the paper balance.
BET_SIZE = _env_float("BET_SIZE", "2.50")
# Phase 2 owns the final stretch only; phase 1 owns everything before it.
TRADE_LAST_SECONDS = _env_int("TRADE_LAST_SECONDS", "120")
if TRADE_LAST_SECONDS is None:
    raise ValueError("TRADE_LAST_SECONDS cannot be empty")
TRADE_INTERVAL_SECONDS = _env_float("TRADE_INTERVAL_SECONDS", "6")
MAX_BUY_PRICE = _env_float("MAX_BUY_PRICE", "0.90")
# Momentum gate on SIG PRICE, in basis points of the opening print. The
# Binance move from the open must be at least this large before it votes at
# all. strategy.decide has always returned a full-confidence UP/DOWN for any
# non-zero move, so a $0.01 drift on $79,000 (0.0013bps) selected a side as
# firmly as a $200 run - and paid the same ask for it. MOMENTUM on the
# dashboard has displayed that magnitude all along without anything acting
# on it; this is the knob that acts on it.
#
# 0 = off, exactly the historical behaviour. Do NOT guess a value: the
# journal (signal_journal.py analyze) measures accuracy by move size, and at
# the time of writing it held 7 rounds against the ~1,111 it says are needed
# to read a 3-point edge.
SIG_PRICE_MIN_MOVE_BPS = _env_float("SIG_PRICE_MIN_MOVE_BPS", "0")
MIN_BUY_PRICE = _env_float("MIN_BUY_PRICE", "0.20")
# ---- primary-entry price band --------------------------------------------
# MIN/MAX_BUY_PRICE are the ACCOUNT-wide bounds: the widest any order may go.
# These two narrow that band for a PRIMARY phase-2 entry only - the leg that
# follows the signal - and leave a taper hedge leg on the account bounds.
#
# Why the split. Measured over 620 settled fills (Aug 26 - Sep 09), the two
# leg types are priced completely differently for almost the same hit rate:
#
#     PRIMARY  521 fills  avg price 0.577  hit 52%  edge -0.058  -12.3%
#     HEDGE     99 fills  avg price 0.418  hit 54%  edge +0.118  +24.4%
#
# Bucketed by price paid, the hit rate tracked the price to within a couple
# of points in every bucket - the book is efficiently priced and there is no
# selection edge to harvest - so what decides the result is what gets paid.
# 0.60-0.80 alone held 52% of turnover at -0.047/-0.060 edge. A ceiling on
# the primary leg attacks that directly; a floor keeps it out of the deep
# longshots, where 4 primary fills under 0.15 returned -100%.
#
# The hedge is deliberately NOT capped or floored here: its edge is largest
# in exactly the cheap buckets a shared floor would forbid (+74.5% under
# 0.15 across 6 fills). Note this means the ACCOUNT floor must stay low -
# both brokers may only TIGHTEN these per-order bounds, never loosen them,
# so a high MIN_BUY_PRICE would silently re-impose itself on the hedge.
#
# Both default to the account bound, so this is inert until set.
PRIMARY_ENTRY_MIN_PRICE = _env_float(
    "PRIMARY_ENTRY_MIN_PRICE", str(MIN_BUY_PRICE))
PRIMARY_ENTRY_MAX_PRICE = _env_float(
    "PRIMARY_ENTRY_MAX_PRICE", str(MAX_BUY_PRICE))
BTC_STALE_AFTER = _env_float("BTC_STALE_AFTER", "3.0")
# How long the book we hold may have been in our hands. For a REST read
# this is the request round trip, so it stays near zero.
ORDERBOOK_MAX_AGE_SECONDS = _env_float("ORDERBOOK_MAX_AGE_SECONDS", "8.0")
# How long the venue may have left the book UNCHANGED before we stop
# believing it. This is not freshness: a quiet market legitimately goes
# minutes without a single change, and measuring staleness from the last
# change refused perfectly current books. Measured on btc-updown-5m, gaps
# of 33s between changes are ordinary. The bound exists only to catch a
# venue serving a frozen or cached book.
ORDERBOOK_MAX_QUIET_SECONDS = _env_float("ORDERBOOK_MAX_QUIET_SECONDS", "900.0")
# A book timestamp ahead of our clock by more than this means the clock
# or the timestamp unit is wrong, never a real book.
ORDERBOOK_FUTURE_TOLERANCE_SECONDS = _env_float(
    "ORDERBOOK_FUTURE_TOLERANCE_SECONDS", "5.0")
MAX_ALLOWED_SPREAD = _env_float("MAX_ALLOWED_SPREAD", "0.25")

# ---- HTTP transport: how venue reads survive a bad link --------------------
# Every unauthenticated venue read goes through http_pool on a kept-alive
# connection. Two failure modes cost real rounds:
#
#   1. The peer resets a pooled connection mid-request. urllib3 already
#      re-opens a connection the peer closed CLEANLY, but a reset while the
#      request is in flight surfaces as a read error and, with the stock
#      adapter's Retry(total=0), propagates straight to the caller. Measured
#      locally against a socket that RSTs the second request: ConnectionError
#      with the stock adapter, recovered in 15ms with one read retry.
#   2. The peer accepts and then goes silent. Only a timeout ends that, and a
#      scalar requests timeout is applied to the connect AND the read phase
#      separately, so `timeout=8` means up to 16s, not 8s.
#
# Retries here are transport-only. Callers that care about 429/5xx already
# implement their own backoff with Retry-After, and retrying in both places
# would multiply attempts inside a budget measured in single seconds.
HTTP_TRANSPORT_RETRIES = _env_int("HTTP_TRANSPORT_RETRIES", "1")
if HTTP_TRANSPORT_RETRIES is None or not 0 <= HTTP_TRANSPORT_RETRIES <= 3:
    raise ValueError("HTTP_TRANSPORT_RETRIES must be between 0 and 3")
HTTP_RETRY_BACKOFF_SECONDS = _env_float("HTTP_RETRY_BACKOFF_SECONDS", "0.1")
# Establishing a connection is either fast or not happening. Kept separate
# from the read budget so a caller asking for an 8s read cannot silently wait
# 16s, and so a dead route fails while the round is still tradeable.
HTTP_CONNECT_TIMEOUT_SECONDS = _env_float("HTTP_CONNECT_TIMEOUT_SECONDS", "4.0")
# Keepalive probes on idle pooled sockets. Between trade cycles a connection
# can be dropped by a NAT or load balancer without a FIN ever arriving; the
# next read then blocks for the full timeout instead of failing at once.
HTTP_KEEPALIVE_IDLE_SECONDS = _env_int("HTTP_KEEPALIVE_IDLE_SECONDS", "30")
HTTP_KEEPALIVE_INTERVAL_SECONDS = _env_int("HTTP_KEEPALIVE_INTERVAL_SECONDS", "10")
CLOCK_MAX_DRIFT_SECONDS = _env_float("CLOCK_MAX_DRIFT_SECONDS", "2.0")
PAPER_LATENCY_MS = _env_float("PAPER_LATENCY_MS", "150")

# Taker matching delay paper assumes when the venue flags one (itode=true) but
# publishes seconds_delay=0 - which it does on these markets. Paper used to
# trust the 0 and match 0.15s after deciding, while a live order sat through
# the real delay; that gap is what let paper look profitable where live lost.
#
# The true delay is still UNMEASURED - no live submit-to-match timing exists.
# 1.0s is the shortest probed delay at which the failure reproduced (3 of 5
# paper fills re-quoted 1s later would not have filled), so it is a floor on
# realism, not an estimate of the venue. Raise it once live_attempts.jsonl
# holds real timings. ASSUMED_MATCH_DELAY_SECONDS is a separate, deliberately
# generous bound used only to refuse orders near expiry.
PAPER_FLAGGED_DELAY_SECONDS = _env_float("PAPER_FLAGGED_DELAY_SECONDS", "1.0")
if (not math.isfinite(PAPER_FLAGGED_DELAY_SECONDS)
        or not 0.0 <= PAPER_FLAGGED_DELAY_SECONDS <= 30.0):
    raise ValueError("PAPER_FLAGGED_DELAY_SECONDS must be in [0, 30]")


def _delay_probes(name: str) -> tuple[float, ...]:
    """Seconds-after-decision offsets for PAPER fill-delay probes, e.g. "1,3,5"."""
    raw = (_env_text(name, "") or "").strip()
    if not raw:
        return ()
    out = []
    for piece in raw.replace(";", ",").split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            value = float(piece)
        except ValueError:
            raise ValueError(
                f"{name} entries must be numbers of seconds, got {piece!r}") from None
        if not math.isfinite(value) or not 0 < value <= 60:
            raise ValueError(f"{name} entries must be between 0 and 60 seconds")
        out.append(value)
    if len(out) > 6:
        raise ValueError(f"{name} allows at most 6 probes")
    return tuple(sorted(set(out)))


# Fill-delay probes: after each PAPER fill, re-quote the same order against
# the book this many seconds after the decision, and record what it would
# have cost - or that it would not have filled. Empty disables it.
#
# Why. Every archived paper fill matched after exactly PAPER_LATENCY_MS: the
# venue publishes seconds_delay=0 while flagging itode=true (a taker matching
# delay exists but is never stated), and paper trusts the 0. Nothing has ever
# measured the real submit-to-match time, so no latency setting can be chosen
# from evidence - which is the whole problem, because gate2.py shows paper's
# edge is worth roughly a tick: hold the band choice out of sample and the
# record is -0.017 edge, -4.7% return over 521 orders, and assuming the
# orders that miss are the winners takes it to -17%. A model that only works
# when it fills instantly is not a model of this venue.
#
# These rows measure the decay directly, so paper can be calibrated instead
# of trusted.
#
# Diagnostic only: a probe places nothing, never touches cash or the ledger,
# and any failure is swallowed. The journal sits beside the paper audit file
# as <audit stem>_fill_delay.jsonl.
PAPER_FILL_DELAY_PROBES = _delay_probes("PAPER_FILL_DELAY_PROBES")

TWAP_STALE_AFTER = _env_float("TWAP_STALE_AFTER", "10.0")
# No order inside the final minute. Measured over 16 fills: 31.2% won against
# a 69.6% break-even, z = -3.29 - and it has a mechanism, not just a p-value.
# In the last minute the book goes one-sided (the winning leg keeps only bids,
# the losing leg only asks), so the fills still available are the ones the
# market is content to sell. That is adverse selection, and no signal fixes it.
# Trading T-120..T-60 was +1.4 per $100 over the same period; the damage was
# entirely in the tail.
MIN_SECONDS_TO_EXPIRY = _env_float("MIN_SECONDS_TO_EXPIRY", "60.0")

# Phase 2 keeps book and Chainlink votes as diagnostics, while fresh Binance
# SIG PRICE is the sole order-side authority.  It remains off by default;
# enable it explicitly to run the experimental path.
PHASE2_ENABLED = bool(_env_bool("PHASE2_ENABLED", False))

# Give SIG BOOK and SIG CHAINLINK their own orders instead of leaving them as
# diagnostics. Each non-neutral signal trades its own side, so a round where
# they disagree buys BOTH legs on purpose. Measured on 1,957 logged decisions
# that is 26.1% of them, and a simultaneous pair costs the overround: at the
# 1.0100 sum observed live, ~-$0.22 per $5 pair whichever way BTC settles.
# The complement guard exists to prevent exactly that, so this switch stands
# it down for signal-driven legs and cannot be combined with a lock that would
# refuse them. Off by default; PAPER only.
# How early the NEXT round's books are discovered and pre-subscribed. The
# websocket needs time to subscribe and receive a first snapshot before the
# boundary, or the opening seconds of the new round trade against an empty
# book. Raising this costs one extra gamma-api call per round, no more.
ROUND_PREPARE_LEAD_SECONDS = _env_float("ROUND_PREPARE_LEAD_SECONDS", "30")
if not 5.0 <= ROUND_PREPARE_LEAD_SECONDS <= 280.0:
    raise ValueError("ROUND_PREPARE_LEAD_SECONDS must be between 5 and 280")
# Rotation poll interval away from a boundary. Near one the loop polls every
# second regardless: the opening print may only be latched in the first 5s of
# a round, so a rotation that lands 6s late costs the entire round. Mid-round
# there is nothing to gain and gamma-api rate-limits, hence the slower default.
ROUND_POLL_SECONDS = _env_float("ROUND_POLL_SECONDS", "5")
if not 0.5 <= ROUND_POLL_SECONDS <= 30.0:
    raise ValueError("ROUND_POLL_SECONDS must be between 0.5 and 30")

# Phase 2 normally refuses to trade a round unless all four boundary inputs are
# present: both Binance values and both Chainlink values. But SIG PRICE alone
# owns the order side, and SIG CHAINLINK is either a diagnostic or - under
# PHASE2_MULTI_SIGNAL - a leg of its own that can simply abstain. Requiring its
# inputs to trade cancels rounds that SIG PRICE could have handled: one missed
# one-second TWAP observation kills five minutes of trading. With this on, only
# missing BINANCE inputs cancel the round; Chainlink abstains like SIG BOOK
# already does on a one-sided book. Off by default so the stricter original
# contract is what you get unless the looser one is chosen deliberately.
# Polymarket enables a taker matching delay on these markets (`itode: true` on
# /clob-markets) but the endpoint states only THAT a delay exists, never how
# long. An order submitted without knowing it can match after the round has
# already resolved, so the live path refuses outright by default.
#
# Set this to the delay you are willing to assume, in seconds, and live orders
# are permitted outside that many seconds from the round end - refused inside
# it, where an unknown delay could span resolution. 0 keeps the hard refusal.
#
# Choose generously: if the real delay exceeds this, orders sent just outside
# the window can still match after expiry, which is exactly the failure the
# refusal exists to prevent. It costs only the tail of a 300s round.
ASSUMED_MATCH_DELAY_SECONDS = _env_float("ASSUMED_MATCH_DELAY_SECONDS", "0")
if (not math.isfinite(ASSUMED_MATCH_DELAY_SECONDS)
        or not 0 <= ASSUMED_MATCH_DELAY_SECONDS <= 120):
    raise ValueError("ASSUMED_MATCH_DELAY_SECONDS must be between 0 and 120")

# The opening print is latched from a websocket trade stamped in the first 5
# seconds of the round. A socket mid-reconnect across the boundary never
# receives it and the whole round is lost - measured at about one round in
# five. After this many seconds the bot asks Binance REST for a trade from the
# SAME 5-second interval, which recovers the identical value rather than
# substituting a later price. Give the socket a real chance first; retrying too
# early just spends an API call on a print that was about to arrive.
BOUNDARY_BACKFILL_AFTER = _env_float("BOUNDARY_BACKFILL_AFTER", "15")
# How many times the REST recovery may be tried, and how long to wait
# between attempts. It used to run exactly ONCE per round: the "already
# tried" flag was set BEFORE the call, so a single timeout or 429 cost the
# whole round - no opening print means price_signal returns None, every
# attempt is refused, and the round produces nothing at all. Measured over 89
# rounds the bot was actually up for, 10% produced no trade whatsoever.
#
# Retrying is safe because _recover_boundary_print queries the SAME
# [window, window+5) interval every time and refuses a response stamped
# outside it. A later attempt therefore returns the same opening print the
# socket would have latched, never a mid-round price standing in for it -
# which is the substitution the strike logic exists to prevent.
BOUNDARY_BACKFILL_RETRIES = int(_env_float("BOUNDARY_BACKFILL_RETRIES", "3"))
if not 1 <= BOUNDARY_BACKFILL_RETRIES <= 20:
    raise ValueError("BOUNDARY_BACKFILL_RETRIES must be between 1 and 20")
BOUNDARY_BACKFILL_RETRY_GAP = _env_float("BOUNDARY_BACKFILL_RETRY_GAP", "10")
if (not math.isfinite(BOUNDARY_BACKFILL_RETRY_GAP)
        or not 1 <= BOUNDARY_BACKFILL_RETRY_GAP <= 120):
    raise ValueError("BOUNDARY_BACKFILL_RETRY_GAP must be between 1 and 120 seconds")
if not math.isfinite(BOUNDARY_BACKFILL_AFTER) or not 5 <= BOUNDARY_BACKFILL_AFTER <= 120:
    raise ValueError("BOUNDARY_BACKFILL_AFTER must be between 5 and 120 seconds")

# A round already underway when the bot starts is a degraded round: its
# opening print may only be recoverable from REST, the book has moved, and
# part of its trading window is already gone. With this on the bot observes
# that round without trading it and begins at the next clean boundary.
SKIP_JOINED_ROUND = bool(_env_bool("SKIP_JOINED_ROUND", False))

PHASE2_PARTIAL_SIGNALS = bool(_env_bool("PHASE2_PARTIAL_SIGNALS", False))

# Order side follows the DISSENTING signal when the three disagree, instead of
# SIG PRICE unconditionally. Measured over 275 archived rounds this scored -5.83
# per $100 against -1.90 for SIG PRICE alone, so it is off by default and is
# selected deliberately. Requires PHASE2_MULTI_SIGNAL off: the minority rule
# picks ONE side, and multi-signal exists to buy several.
SIGNAL_MINORITY_RULE = bool(_env_bool("SIGNAL_MINORITY_RULE", False))

# WHICH signal actually picks the order side.
#
#   "price"    - SIG PRICE alone. BOOK and CHAINLINK stay diagnostics.
#   "minority" - follow the dissenting signal (strategy.minority_decision).
#   "final"    - strategy.final_decision, the line the dashboard has always
#                shown as "diagnostic": PRICE and BOOK agreeing wins; failing
#                that CHAINLINK breaks the tie by siding with one of them;
#                failing that, whichever signal is present at all. This is a
#                CONFIRMATION rule - it trades the side two feeds agree on,
#                where "price" trades one feed's opinion unconfirmed.
#
# main_bot._authority_side is the single place this is read, and every
# re-validation gate asks it the same question. That matters more than the
# rule itself: when the chooser and the gates disagree, the gates reject
# nearly every order the chooser makes.
#
# Defaults to the SIGNAL_MINORITY_RULE setting, so an existing .env keeps the
# behaviour it already had.
SIGNAL_DECISION_RULE = (
    _env_text("SIGNAL_DECISION_RULE",
              "minority" if SIGNAL_MINORITY_RULE else "price") or "").strip().lower()
if SIGNAL_DECISION_RULE not in {"price", "minority", "final"}:
    raise ValueError(
        "SIGNAL_DECISION_RULE must be one of: price, minority, final")

# Refuse a phase-2 entry unless every signal that voted agrees. A signal that
# abstained does not count against unanimity - silence is not dissent.
#
# Measured over 894 settled fills, split by whether any signal dissented:
#
#     UNANIMOUS  648 fills  $2104.89  -5.7%  60% of positions won
#     CONTESTED  246 fills  $ 692.92  -6.7%  46% of positions won
#
# Filtering to unanimous only would have moved total P&L -165.66 -> -119.01
# (+46.65) and cut capital at risk by 25%. Read that honestly: BOTH groups
# lose, and the return improves only 1.0 point. Most of the gain is from
# trading less, not from finding a winning subset - so this is a turnover
# brake, not an edge. It is off by default for that reason.
#
# Phase 1 is unaffected: the bands are price-only by design and never read
# BOOK or CHAINLINK, so there is no unanimity to test there.
REQUIRE_SIGNAL_UNANIMITY = bool(_env_bool("REQUIRE_SIGNAL_UNANIMITY", False))


# The order path refuses any submission outside the round's execution
# interval. That interval has to reach back to the earliest second ANY enabled
# phase can trade: sizing it from TRADE_LAST_SECONDS alone silently refuses
# every phase-1 order as "outside the current round execution interval",
# because phase 1 runs entirely before phase 2's window opens.
EXECUTION_WINDOW_SECONDS = TRADE_LAST_SECONDS


# Polymarket takes shares * theta * p * (1-p) from a taker. For a fixed
# notional that is at most notional * theta, which is all a cap needs.
TAKER_FEE_RATE = 0.07
VENUE_MIN_SHARES = 5.0

# Price headroom when sizing a stake up to the venue's share minimum.
#
# Orders are dollar-denominated in both modes (live signs MarketOrderArgs with
# amount=<USDC>), and the stake is fixed at the decision book. Sized to buy
# EXACTLY the 5-share minimum, a single tick of rise before the order matches
# leaves the same dollars buying 4.93 shares - below the minimum - and the
# venue rejects it. The ask rises when the market moves toward the side being
# bought, so this rejects the orders that would have won and lets the ones
# that would have lost fill: systematic adverse selection, not random misses.
#
# Paper did not see it because it matched 0.15s after deciding. Measured by
# the fill-delay probes (pc_orders_fill_delay.jsonl, Sep 20): re-quoted 1s
# later, 3 of 5 paper fills would not have filled, and EVERY failure was this
# one - "order size 4.93 shares is below venue minimum 5". The rises that
# killed them: median +0.01, worst +0.04. This covers the worst of those 7
# observations; it is not a measured optimum. Cost: about $0.20 more stake
# per minimum-sized order (5 shares x 0.04). 0 restores exact sizing.
VENUE_MIN_PRICE_HEADROOM = _env_float("VENUE_MIN_PRICE_HEADROOM", "0.04")
if not math.isfinite(VENUE_MIN_PRICE_HEADROOM) or not 0.0 <= VENUE_MIN_PRICE_HEADROOM < 1.0:
    raise ValueError("VENUE_MIN_PRICE_HEADROOM must be in [0, 1)")

# ---- scale in only at a better price ---------------------------------------
# The directional signal says WHICH side; this says whether a round may add to
# a side it already holds. On when the new ask is below what the position has
# averaged so far, so every add lowers the average and the break-even with it.
# Off, a round buys once and holds.
#
# In this market price IS probability, so an ask below the average is better
# odds on the same call - which is the case for adding. The risk is the
# mirror of that: the price falls when the market thinks the position is
# wrong, so the rounds that end up largest are the ones that moved against it
# most. That concentration is the thing to watch in the ledger, not the
# average price, which this rule improves by construction.
ADD_BELOW_AVERAGE_ONLY = bool(_env_bool("ADD_BELOW_AVERAGE_ONLY", True))

# How far below the average an ask has to be before it counts as an
# improvement. Without it a tick of 0.001 qualifies and the round grinds in a
# fee at a time: the taker pays theta*(1-price) per add, so an improvement
# smaller than the fee it costs makes the position worse, not better.
ADD_MIN_IMPROVEMENT = _env_float("ADD_MIN_IMPROVEMENT", "0.02")
if not math.isfinite(ADD_MIN_IMPROVEMENT) or not 0.0 <= ADD_MIN_IMPROVEMENT < 1.0:
    raise ValueError("ADD_MIN_IMPROVEMENT must be in [0, 1)")

# How many times one round may add to a side it already holds. The opening
# buy and a flip onto the other outcome are not adds; only a repeat purchase
# of a held side counts, and only once it fills. Applies whether or not
# ADD_BELOW_AVERAGE_ONLY is on, so switching that off cannot lift the cap.
#
# Why it exists: adds were 4x the volume of entries and decided the result in
# both directions. The same below-average adds won 67% (+26.3%) in one window
# on Sep 24 and 45% (-15.2%) in the next, while entries won 62-63% in both.
# Adding on a dip bets the dip recovers, so it multiplies whatever the market
# does in that window rather than adding an edge of its own.
#
# -1 = unlimited, which is what those runs did. 0 = entries only. Counted in
# memory per round; a restart mid-round starts the count again, and
# MAX_ROUND_EXPOSURE - which is restored from the ledger - still bounds it.
MAX_ADDS_PER_ROUND = _env_int("MAX_ADDS_PER_ROUND", "-1")
if MAX_ADDS_PER_ROUND is None or MAX_ADDS_PER_ROUND < -1:
    raise ValueError("MAX_ADDS_PER_ROUND must be -1 (unlimited) or 0 and up")


# ---- follow the signal when it turns ---------------------------------------
# The directional rule says hold one side. When the signal changes sides there
# are two ways to act on it, and only one of them is still directional:
#
#   buy the other leg  - the round then holds both. A matched pair redeems
#                        exactly $1.00, so above that the result is fixed
#                        whatever BTC does. Measured here: 41 of 44 both-leg
#                        rounds cost over $1.00, -$87.85 locked in. There is
#                        no directional position left to win.
#   SELL, then buy     - exit the held side and enter the new one. Costs the
#                        spread plus a fee each way, and the round stays a
#                        bet that can still be right.
#
# This is the second. It never holds both legs. Off by default: it turns the
# bot into something that sells, and a signal that whipsaws pays the spread
# every time it changes its mind.
FLIP_EXIT_ENABLED = bool(_env_bool("FLIP_EXIT_ENABLED", False))

# With selling ruled out, a round commits to the side it bought first. The
# signal still runs every pass, but once a leg is held a turn in the signal
# cannot move the round onto the other outcome - there is no way to act on it
# that does not either sell (disallowed) or complete a pair (a fixed result).
# So the round keeps building the side it opened with, or stands still.
#
# Read the interaction with ADD_BELOW_AVERAGE_ONLY before raising the stake:
# an add needs the ask to fall, and the ask falls precisely when the market
# turns against the held side. Together they mean the rounds that grow
# largest are the ones the signal has already abandoned. That is averaging
# down by construction - bounded here only by MAX_ROUND_EXPOSURE, which is
# what actually caps the damage on a round that never comes back.
ANCHOR_TO_FIRST_SIDE = bool(_env_bool("ANCHOR_TO_FIRST_SIDE", False))

# Follow a turned signal by BUYING the newly favoured outcome, which leaves
# the round holding both legs. A matched pair redeems exactly $1.00 whatever
# BTC does, so the round's result stops being a prediction and becomes the
# sum of what the two legs cost.
ALLOW_COMPLEMENT_ON_FLIP = bool(_env_bool("ALLOW_COMPLEMENT_ON_FLIP", False))

# How much locked loss per pair is acceptable to follow the signal. 0.0 admits
# only pairs that finish under $1.00 - free money, and rare. Measured on this
# repo's ledger, 44 rounds that held both legs: 41 cost over $1.00, avg second
# leg 0.555, worst pair $1.5042. Admitted at each budget, with what it costs:
#     0.00   3/45 rounds    $0.00
#     0.10  22/45 rounds   -$0.98
#     0.15  32/45 rounds   -$2.23
#     0.50  44/45 rounds   -$4.91
COMPLEMENT_MAX_LOCKED_LOSS = _env_float("COMPLEMENT_MAX_LOCKED_LOSS", "0.15")
if (not math.isfinite(COMPLEMENT_MAX_LOCKED_LOSS)
        or not 0.0 <= COMPLEMENT_MAX_LOCKED_LOSS <= 1.0):
    raise ValueError("COMPLEMENT_MAX_LOCKED_LOSS must be in [0, 1]")

# Do not exit into a bid this far below what the position averaged. A signal
# that turns after the price has already collapsed is reporting a loss that
# has happened, not predicting one; paying the spread to realise it and then
# buying the expensive other side is the worst of both. 0 disables the brake.
FLIP_MAX_GIVEUP = _env_float("FLIP_MAX_GIVEUP", "0.15")
if not math.isfinite(FLIP_MAX_GIVEUP) or not 0.0 <= FLIP_MAX_GIVEUP < 1.0:
    raise ValueError("FLIP_MAX_GIVEUP must be in [0, 1)")

# Seconds before the round ends after which a flip is pointless: the exit has
# to fill, the entry has to fill, and near expiry the book goes one-sided.
# Headroom under the quoted bid when placing the exit. A bid is not a fill:
# the book can move a tick between the read and the sell, and an exit that
# refuses to cross leaves the round holding the side the signal abandoned -
# the worst of both. This is the most the exit may slip below the bid it saw.
FLIP_SLIPPAGE = _env_float("FLIP_SLIPPAGE", "0.03")
if not math.isfinite(FLIP_SLIPPAGE) or not 0.0 <= FLIP_SLIPPAGE < 1.0:
    raise ValueError("FLIP_SLIPPAGE must be in [0, 1)")

FLIP_CUTOFF_SECONDS = _env_float("FLIP_CUTOFF_SECONDS", "90")
if not math.isfinite(FLIP_CUTOFF_SECONDS) or not 0.0 <= FLIP_CUTOFF_SECONDS <= 300.0:
    raise ValueError("FLIP_CUTOFF_SECONDS must be in [0, 300]")




def entry_cost_ceiling(cap_price: float, amount: float | None = None) -> float:
    """The most one entry can take out of the account at this price cap.

    BUGFIX: main_bot used to charge MAX_ROUND_EXPOSURE exactly BET_SIZE per
    entry. The broker sizes UP to the venue's 5-share minimum, so any fill
    above BET_SIZE/5 costs more than BET_SIZE and the fee is on top. Measured
    on a real paper run the tracker was 22% low overall and 76% low on one
    round, which made the cap nominal rather than real. A limit has to use an
    upper bound, so this returns one.

    BUGFIX 2: it also ignored what the order actually stakes, charging
    BET_SIZE however large the entry was. That was harmless while every
    entry was BET_SIZE or smaller - it merely over-charged - but a taper
    ladder stakes per slot, and a $5 slot really costs up to $5.28 against
    the $4.28 charged. The cap silently under-counts by 19% and stops
    meaning what it says. `amount` defaults to BET_SIZE so every existing
    caller (phase-1 bands, the multi-signal legs, the round budget) is
    unchanged.
    """
    stake = float(BET_SIZE if amount is None else amount)
    notional = max(stake, VENUE_MIN_SHARES * float(cap_price))
    return notional * (1.0 + TAKER_FEE_RATE)



def _round_entry_budget() -> float:
    """Worst-case CASH for one round, counting only the phases switched on.

    Deriving it from the phases themselves means parking phase 2 lowers the
    cap automatically, instead of leaving a ceiling sized for a path that no
    longer runs. Each band is budgeted at its own ceiling price, so a band
    priced above BET_SIZE/5 gets the room it will actually need.
    """
    budget = 0.0
    if PHASE2_ENABLED:
        # Phase 2 stops at MIN_SECONDS_TO_EXPIRY, so its budget is the window
        # it can actually reach, not the whole tail of the round.
        entries = math.ceil(
            max(0.0, TRADE_LAST_SECONDS - MIN_SECONDS_TO_EXPIRY)
            / max(TRADE_INTERVAL_SECONDS, 1))
        budget += entries * entry_cost_ceiling(MAX_BUY_PRICE)
    return budget if budget > 0 else entry_cost_ceiling(MAX_BUY_PRICE)


MAX_ROUND_EXPOSURE = _env_float("MAX_ROUND_EXPOSURE", str(_round_entry_budget()))

# Ceiling on cash committed to rounds the venue has not resolved yet, across
# ALL open rounds. MAX_ROUND_EXPOSURE caps one round in isolation, which is
# blind to the case that actually empties an account: settlement stalling
# upstream while every new round passes its own budget check. Observed doing
# exactly that - 20 open rounds holding 103% of a $300 wallet, leaving $3
# tradeable, purely because the venue had not written resolutions on chain.
#
# The bot cannot settle those itself without inventing an outcome, so the only
# safe lever is to stop opening new ones. Entries are refused while unsettled
# cost is at or above this, and resume by themselves as positions settle.
#
# 0 disables it, which is the default: this changes nothing until set.
MAX_UNSETTLED_EXPOSURE = _env_float("MAX_UNSETTLED_EXPOSURE", "0")
if not math.isfinite(MAX_UNSETTLED_EXPOSURE) or MAX_UNSETTLED_EXPOSURE < 0:
    raise ValueError("MAX_UNSETTLED_EXPOSURE must be finite and non-negative")

# ---- stop loss --------------------------------------------------------------
# Sell a held leg once its BID reaches STOP_LOSS_PRICE. This is a client-side
# trigger, not an order type: the CLOB has only FAK/FOK/GTC/GTD, so nothing
# resting on the book can act as a stop. A resting sell fills when price RISES,
# which is a take-profit; a stop has to be watched and then crossed.
#
# Measured over 275 archived rounds: 14.1% of legs that eventually WON traded
# at or below 0.25 first, dipping as low as 0.04 with 143s still to run. A stop
# therefore cuts roughly one winner in seven. On both-leg rounds the exit was
# worth +$392 (t=+3.44) because it was selling a structurally dead second leg;
# on single-leg rounds the same test came out at -0.58. Whether it pays depends
# entirely on the haircut actually paid on the way out, which no archived run
# recorded, so this ships OFF and instrumented.
STOP_LOSS_ENABLED = bool(_env_bool("STOP_LOSS_ENABLED", False))
STOP_LOSS_PRICE = _env_float("STOP_LOSS_PRICE", "0.25")
# The absolute worst price the exit may accept while walking the book down.
# Setting this to the trigger price makes the stop a pure limit that simply
# does not fill in a thin book; lowering it buys certainty of exit with price.
# At a haircut beyond 0.19 the measured benefit inverts, so a floor far below
# the trigger is choosing execution over expectancy - deliberately.
STOP_LOSS_FLOOR_PRICE = _env_float("STOP_LOSS_FLOOR_PRICE", "0.05")
# Do not arm before this many seconds remain. The winners that recovered from
# under 0.25 did so at 93-209s left; a stop armed round-wide cut 13 of them
# against 5 when it only armed inside 120s.
STOP_LOSS_ARM_SECONDS = _env_float("STOP_LOSS_ARM_SECONDS", "120")
# Stop placing exits this close to expiry. The trader studied here stopped
# trading entirely at T-30 and placed nothing in the final 30 seconds.
STOP_LOSS_EXIT_CUTOFF_SECONDS = _env_float("STOP_LOSS_EXIT_CUTOFF_SECONDS", "20")
STOP_LOSS_POLL_SECONDS = _env_float("STOP_LOSS_POLL_SECONDS", "1.0")
if not 0.0 < STOP_LOSS_PRICE < 1.0:
    raise ValueError("STOP_LOSS_PRICE must be strictly between 0 and 1")
if not 0.0 < STOP_LOSS_FLOOR_PRICE <= STOP_LOSS_PRICE:
    raise ValueError(
        "STOP_LOSS_FLOOR_PRICE must be in (0, STOP_LOSS_PRICE]: a floor above "
        "the trigger could never fill")
if not 0.0 <= STOP_LOSS_EXIT_CUTOFF_SECONDS < STOP_LOSS_ARM_SECONDS <= 300.0:
    raise ValueError(
        "need 0 <= STOP_LOSS_EXIT_CUTOFF_SECONDS < STOP_LOSS_ARM_SECONDS <= 300")
if not 0.2 <= STOP_LOSS_POLL_SECONDS <= 30.0:
    raise ValueError("STOP_LOSS_POLL_SECONDS must be between 0.2 and 30")

CANCEL_OPEN_BEFORE_TRADE = bool(_env_bool("CANCEL_OPEN_BEFORE_TRADE", False))
ALLOW_GLOBAL_CANCEL_ALL = bool(_env_bool("ALLOW_GLOBAL_CANCEL_ALL", False))
ALLOW_CUSTOM_CLOB_HOST = bool(_env_bool("ALLOW_CUSTOM_CLOB_HOST", False))
CLOB_HOST = _env_text("CLOB_HOST", "https://clob.polymarket.com")
CHAIN_ID = 137
_VALID_TICKS = ("0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001")
TICK_SIZE = _env_text("TICK_SIZE") or None
if TICK_SIZE is not None and TICK_SIZE not in _VALID_TICKS:
    raise ValueError(f"TICK_SIZE must be one of {_VALID_TICKS}, got {TICK_SIZE!r}")
NEG_RISK = _env_bool("NEG_RISK", None)
UP_TOKEN_ID = _env_text("UP_TOKEN_ID") or None
DOWN_TOKEN_ID = _env_text("DOWN_TOKEN_ID") or None
ORDERBOOK_TOKEN_ID = _env_text("ORDERBOOK_TOKEN_ID") or None
POLY_FUNDER = _env_text("POLY_FUNDER") or None
POLY_SIGNATURE_TYPE = _env_int("POLY_SIGNATURE_TYPE")


def _finite_positive(name, value):
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite positive number, got {value!r}")


_finite_positive("BET_SIZE", BET_SIZE)
if Decimal(str(BET_SIZE)).quantize(Decimal("0.01")) != Decimal(str(BET_SIZE)):
    raise ValueError("BET_SIZE must be expressed in whole pUSD cents")
if not 1 <= TRADE_LAST_SECONDS <= 300:
    raise ValueError("TRADE_LAST_SECONDS must be between 1 and 300")
if (not math.isfinite(TRADE_INTERVAL_SECONDS)
        or not 1 <= TRADE_INTERVAL_SECONDS <= TRADE_LAST_SECONDS):
    raise ValueError("TRADE_INTERVAL_SECONDS must be in [1, TRADE_LAST_SECONDS]")
if not math.isfinite(MAX_BUY_PRICE) or not 0 < MAX_BUY_PRICE < 1:
    raise ValueError("MAX_BUY_PRICE must be strictly between 0 and 1")
if not math.isfinite(MIN_BUY_PRICE) or not 0 < MIN_BUY_PRICE < 1:
    raise ValueError("MIN_BUY_PRICE must be strictly between 0 and 1")
if not MIN_BUY_PRICE < MAX_BUY_PRICE:
    raise ValueError("MIN_BUY_PRICE must be below MAX_BUY_PRICE")
# The per-order bounds may only tighten the account band - both brokers
# enforce that at execution, so a value outside it would be silently ignored
# rather than applied. Refuse it here instead, where it is visible.
if (not math.isfinite(PRIMARY_ENTRY_MIN_PRICE)
        or not MIN_BUY_PRICE <= PRIMARY_ENTRY_MIN_PRICE < 1):
    raise ValueError(
        "PRIMARY_ENTRY_MIN_PRICE must be at or above MIN_BUY_PRICE and below 1")
if (not math.isfinite(PRIMARY_ENTRY_MAX_PRICE)
        or not 0 < PRIMARY_ENTRY_MAX_PRICE <= MAX_BUY_PRICE):
    raise ValueError(
        "PRIMARY_ENTRY_MAX_PRICE must be positive and at or below MAX_BUY_PRICE")
if not PRIMARY_ENTRY_MIN_PRICE < PRIMARY_ENTRY_MAX_PRICE:
    raise ValueError(
        "PRIMARY_ENTRY_MIN_PRICE must be below PRIMARY_ENTRY_MAX_PRICE")
_finite_positive("BTC_STALE_AFTER", BTC_STALE_AFTER)
_finite_positive("ORDERBOOK_MAX_AGE_SECONDS", ORDERBOOK_MAX_AGE_SECONDS)
_finite_positive("ORDERBOOK_MAX_QUIET_SECONDS", ORDERBOOK_MAX_QUIET_SECONDS)
_finite_positive("ORDERBOOK_FUTURE_TOLERANCE_SECONDS",
                 ORDERBOOK_FUTURE_TOLERANCE_SECONDS)
if ORDERBOOK_FUTURE_TOLERANCE_SECONDS < CLOCK_MAX_DRIFT_SECONDS:
    raise ValueError(
        "ORDERBOOK_FUTURE_TOLERANCE_SECONDS must be >= "
        "CLOCK_MAX_DRIFT_SECONDS, or a clock inside its allowed drift "
        "would still refuse every book as future-dated")
if not math.isfinite(MAX_ALLOWED_SPREAD) or not 0 < MAX_ALLOWED_SPREAD <= 1:
    raise ValueError("MAX_ALLOWED_SPREAD must be in (0, 1]")
_finite_positive("CLOCK_MAX_DRIFT_SECONDS", CLOCK_MAX_DRIFT_SECONDS)
if not math.isfinite(PAPER_LATENCY_MS) or PAPER_LATENCY_MS < 0:
    raise ValueError("PAPER_LATENCY_MS must be finite and non-negative")
_finite_positive("TWAP_STALE_AFTER", TWAP_STALE_AFTER)
_finite_positive("HTTP_CONNECT_TIMEOUT_SECONDS", HTTP_CONNECT_TIMEOUT_SECONDS)
if not math.isfinite(HTTP_RETRY_BACKOFF_SECONDS) or not 0 <= HTTP_RETRY_BACKOFF_SECONDS <= 2:
    raise ValueError("HTTP_RETRY_BACKOFF_SECONDS must be between 0 and 2")
if HTTP_KEEPALIVE_IDLE_SECONDS is None or not 1 <= HTTP_KEEPALIVE_IDLE_SECONDS <= 3600:
    raise ValueError("HTTP_KEEPALIVE_IDLE_SECONDS must be between 1 and 3600")
if (HTTP_KEEPALIVE_INTERVAL_SECONDS is None
        or not 1 <= HTTP_KEEPALIVE_INTERVAL_SECONDS <= 3600):
    raise ValueError("HTTP_KEEPALIVE_INTERVAL_SECONDS must be between 1 and 3600")
if (not math.isfinite(MIN_SECONDS_TO_EXPIRY)
        or not 0 <= MIN_SECONDS_TO_EXPIRY < TRADE_LAST_SECONDS):
    raise ValueError("MIN_SECONDS_TO_EXPIRY must be non-negative and below TRADE_LAST_SECONDS")
if not math.isfinite(MAX_ROUND_EXPOSURE) or MAX_ROUND_EXPOSURE < BET_SIZE:
    raise ValueError("MAX_ROUND_EXPOSURE must be finite and at least BET_SIZE")
if POLY_SIGNATURE_TYPE not in (None, 0, 1, 2, 3):
    raise ValueError("POLY_SIGNATURE_TYPE must be 0, 1, 2, or 3")
# Keep the bot's existing type-3 restriction until its configured deposit
# wallet has been validated end to end with the unified SDK. SDK support
# alone does not establish that the deployed bot's credentials are correct.
if POLY_SIGNATURE_TYPE == 3:
    raise ValueError(
        "POLY_SIGNATURE_TYPE=3 remains disabled pending unified-SDK "
        "deposit-wallet integration validation"
    )
if POLY_SIGNATURE_TYPE in (1, 2, 3) and not POLY_FUNDER:
    raise ValueError("proxy signature types 1/2/3 require POLY_FUNDER")
if POLY_FUNDER and POLY_SIGNATURE_TYPE is None:
    raise ValueError("POLY_FUNDER requires an explicit POLY_SIGNATURE_TYPE")
if POLY_FUNDER and not re.fullmatch(r"0x[0-9A-Fa-f]{40}", POLY_FUNDER):
    raise ValueError("POLY_FUNDER must be a 20-byte 0x-prefixed Ethereum address")
for _name, _token in (
        ("UP_TOKEN_ID", UP_TOKEN_ID),
        ("DOWN_TOKEN_ID", DOWN_TOKEN_ID),
        ("ORDERBOOK_TOKEN_ID", ORDERBOOK_TOKEN_ID)):
    if _token is not None and (not re.fullmatch(r"[0-9]{1,78}", _token)
                               or int(_token) <= 0):
        raise ValueError(f"{_name} must be a positive decimal uint256 token id")
if CANCEL_OPEN_BEFORE_TRADE and not ALLOW_GLOBAL_CANCEL_ALL:
    raise ValueError(
        "CANCEL_OPEN_BEFORE_TRADE uses the wallet-wide cancel-all endpoint; "
        "set ALLOW_GLOBAL_CANCEL_ALL=1 only for a dedicated bot wallet"
    )
try:
    _clob_url = urlsplit(CLOB_HOST)
    _clob_port = _clob_url.port
except (TypeError, ValueError) as exc:
    raise ValueError("CLOB_HOST must be a valid HTTPS origin") from exc
if (_clob_url.scheme.lower() != "https" or not _clob_url.hostname
        or _clob_url.username is not None or _clob_url.password is not None
        or _clob_url.query or _clob_url.fragment
        or _clob_url.path not in ("", "/")):
    raise ValueError(
        "CLOB_HOST must be an HTTPS origin without credentials, path, query, or fragment"
    )
_official_clob = (
    _clob_url.hostname.lower() == "clob.polymarket.com"
    and _clob_port in (None, 443)
)
if not _official_clob and not ALLOW_CUSTOM_CLOB_HOST:
    raise ValueError(
        "custom CLOB_HOST is blocked because it receives authenticated requests; "
        "set ALLOW_CUSTOM_CLOB_HOST=1 only for an endpoint you control"
    )
# Store an origin without a trailing slash so every SDK endpoint is joined in a
# single, predictable way.
CLOB_HOST = f"https://{_clob_url.hostname}"
if _clob_port not in (None, 443):
    CLOB_HOST += f":{_clob_port}"
