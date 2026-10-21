# Hybrid position sizing

The fair-value margin stays at **$0.01 per share**. Position sizing starts at
**quarter Kelly**, then reduces allocation for high volatility and realized
drawdown. Confidence remains neutral until predictive accuracy has been validated
historically. The optional new per-order hard caps are **disabled**, as requested;
cash, venue minimums, per-round exposure limits and live canary limits still apply.

## Allocation rule

The existing portfolio-aware Kelly calculation uses the estimated fair probability,
the effective buy price including fees, available wealth and existing UP/DOWN
inventory. Hybrid sizing replaces its Kelly fraction with:

```text
effective_fraction = 0.25 × volatility_scale × drawdown_scale × confidence_scale
volatility_scale   = min(1, volatility_baseline / observed_volatility)
drawdown           = max(0, 1 - realized_equity / realized_equity_high_water)
drawdown_scale     = max(0, 1 - drawdown / drawdown_stop)
confidence_scale   = 1
```

Volatility is the trigger feed's existing realized-volatility estimate in basis
points per square root of a second. The baseline defaults to **1.0 bp/√second**.
It is a configurable operating assumption, not an empirically optimized threshold.
Volatility below the baseline does not increase exposure above quarter Kelly.
For example, twice the baseline volatility halves the allocation fraction.

**Drawdown pause (2026-10-22, the operator's choice).** At the stop, no new orders for
`SNIPER_HYBRID_DRAWDOWN_PAUSE_S` (default **600 s**). Then the ledger starts a new hybrid
epoch from the current realized equity (capital = equity, P&L counted from then, peak =
equity), so sizing returns to the full fraction. It repeats on the next 20% fall: the stop
then limits each run of losses, not their total; the canary loss budget still applies.
The reset is durable, journalled as `hybrid_drawdown_pause` / `hybrid_drawdown_reset`.
`SNIPER_HYBRID_DRAWDOWN_PAUSE_S=0` keeps the old rule: the stop holds until equity recovers.

The drawdown stop defaults to **20%**. At 5%, 10% and 20% realized drawdown, its
multiplier is 0.75, 0.5 and 0 respectively. At twice baseline volatility and 10%
drawdown, the effective fraction is `0.25 × 0.5 × 0.5 = 0.0625` of full Kelly.
This is an allocation fraction, not a promise to spend that fraction of cash on
each signal. Existing inventory is subtracted after these modifiers. Already
holding the reduced target results in a skip; reducing the target does not submit
an automatic sell.

The resulting new-order size is rounded down and checked against the venue's
minimum shares and minimum notional. With strict target sizing, a size below
those minimums is skipped. The enabled minimum-entry exception below allows
a bounded first attempt above a small positive target.

## Minimum-entry exception

`SNIPER_KELLY_MIN_ENTRY=1` reduces minimum-size skips by permitting one
venue-minimum order attempt per side and round when the estimated edge and
hybrid allocation are positive, but the target is below the order minimum.
It applies only with no same-side inventory or outstanding same-side risk.
It does not top up existing small positions, and a new signal cannot repeatedly
use the exception. Ordinary additions can still proceed when the hybrid target
itself supports them. Set the option to `0` for strict target sizing.

This is an explicit exception to quarter-Kelly sizing: a target of three shares
can become a stake sized from five shares at the cap. The minimum also respects prefetched venue rules,
the $1 signed-stake minimum and whole-cent sizing; it can exceed five shares.
All-in signed stake plus conservative fees must fit cash, available share room
and any configured optional cap. The fair-value margin remains $0.01. Missing
or stale risk state, the drawdown stop, and freshness checks still prevent orders.

Live uses its existing durable send intent to retain a used attempt through
normal restarts and compaction, including NO_MATCH and a proven NOT_SENT after
the intent was recorded. A refusal before recording the intent leaves it unused.
An explicit new canary-session reset can clear recent terminal attempt history.
Paper records simulated attempts in its order journal. Recovery runs before feed
connections and never scans the archive on the order path.

Cash-limited share orders are also reduced to an affordable signed quantity
instead of rejecting a larger target when a valid smaller order fits. These
changes allow additional order attempts; they do not establish a higher live
fill rate or profitability.

FAK BUY fills consume the signed maker-dollar stake. A five-share sizing target
at a $0.46 cap signs $2.30; if the venue fills at $0.41, those dollars can buy
about 5.61 shares. The target and pre-trade share checks are estimates, not a
guaranteed maximum filled quantity. Paper uses the same budget-based matching
rule, and actual fills feed subsequent inventory checks. Price improvement alone
does not trigger a halt; identity, price-cap and signed-spend checks still apply.
Kelly calculates the nominal share target using the decision ask, while the
existing share-at-cap setting converts that target to dollars at the higher
signed cap. That conversion can increase stake above the ask-valued target even
without the minimum-entry exception; quarter Kelly describes the sizing input,
not an additional hard dollar cap. This existing setting remains unchanged.
Paper reserves the signed stake plus conservative fees when an order is handed
to its worker. Pending reservations remain held even when execution takes longer
than two seconds. A fatal worker/accounting failure stops further decisions.

## Realized drawdown state

Realized equity is an accounting proxy: persisted reference capital plus the
change in confirmed cumulative realized P&L since initialization. Reference capital
is initialized once from available cash plus the ledger's confirmed open-position
cost basis. Live available cash excludes order reservations; unconfirmed positions
receive no credit. An unavailable reference leaves hybrid sizing unready and
prevents new orders. Confirmed partial sales contribute realized P&L as well as
settlements. Its high-water mark is persisted in the ledger so restarting the bot
does not erase drawdown. Paper and live use the same accounting rule.

This measure excludes unrealized gains and losses on open positions. It is not
mark-to-market account equity and is not a complete account cash-flow statement.
Deposits, withdrawals and external activity do not automatically change the
reference capital. Check that reference before interpreting the percentage as
account-level drawdown.

Risk state is refreshed off the order path every 5 seconds. A new order uses the
cached state and is skipped if that state is unavailable or older than 30 seconds.
The order path does not perform a ledger scan or network request to calculate
drawdown. The ledger records high-water changes with each durable realized-P&L
transition and reconstructs them during journal recovery, so peaks between refreshes
are retained. Initialization rejects a reference reading if a fill or settlement
changes the accounting sequence while cash and open basis are being read.
A stricter allocation discovered
before dispatch refuses the old plan and requires a fresh decision. Once an order
is sent, subsequent risk changes cannot cancel its fill, including in paper mode.

## Confidence

The confidence multiplier is fixed at **1.0**. The bot does not treat its fair-value
estimate, a recent win streak, or an arbitrary operator multiplier as validated
predictive accuracy. There is no confidence tuning setting in this version.

Any future confidence adjustment needs a historical estimate tested on outcomes
outside the sample used to fit it, with fees, actual fill selection and execution
prices included. Until such evidence is supplied and evaluated, confidence makes
no adjustment.

## Settings

```dotenv
SNIPER_FAIR=1
SNIPER_FAIR_MARGIN=0.01
SNIPER_KELLY=1
SNIPER_KELLY_FRACTION=0.25
SNIPER_HYBRID_SIZING=1
SNIPER_KELLY_MIN_ENTRY=1
SNIPER_HYBRID_VOL_BASELINE_BP=1.0
SNIPER_HYBRID_DRAWDOWN_STOP=0.20
SNIPER_HYBRID_RISK_FRACTION=0
SNIPER_HYBRID_MAX_ORDER_USD=0
```

Live mode requires fair margin 0.01, Kelly enabled, Kelly fraction 0.25 and hybrid
sizing enabled. The volatility baseline, drawdown stop and optional cap values are
configurable risk-policy settings. Changing them does not alter the fair-value
model or bypass the execution gates.

Both optional new cap settings are zero by default. No new per-trade cap applies
until at least one is positive:

- `SNIPER_HYBRID_RISK_FRACTION`: maximum new-order all-in spend as a fraction of
  realized equity.
- `SNIPER_HYBRID_MAX_ORDER_USD`: maximum new-order all-in spend in dollars.

When enabled, the tighter cap limits the signed order's maximum spend plus a
conservative fee allowance. A buy's full spend can be lost if its outcome loses.
If a cap cannot admit a venue-valid minimum order, the order is skipped. These
settings do not remove existing live canary share, spend or loss limits.

## Verification and rollout

The focused regression set passed 387 tests, with three native SDK checks skipped
because the SDK dependencies are unavailable in the review environment. Another
188 existing accounting checks passed. Native SDK, exchange connectivity and live execution
remain unverified here; no live orders were placed for this change.

A later pass in the pinned environment (`polymarket-client==0.13.0`, Python 3.12)
ran the three native SDK checks without skips and the whole offline suite; see
the completion check in `AUDIT_FIXES_20261020.md`. The A/B replay now runs this
sizing too. Exchange connectivity and live execution remain unverified.

An existing deployment must update `SNIPER_KELLY_FRACTION` from `0.5` to `0.25`
and use `SNIPER_HYBRID_SIZING=1` before startup. Keep its existing account files;
the ledger initializes and persists the hybrid risk epoch without resetting P&L.
These source changes have not been deployed to the trading server.

Strict hybrid sizing can increase minimum-size skips; the bounded first-entry
exception reduces that category by allowing extra exposure at the venue minimum.
Neither setting makes order submission faster, establishes a profitable edge, or guarantees a live
fill within 200 ms. Compare paper and live results on matched signals and equal
stakes before attributing performance changes to these modifiers.
