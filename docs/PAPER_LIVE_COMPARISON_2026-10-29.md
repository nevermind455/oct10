# Paper profit versus live results: investigation, 2026-10-29

The local evidence supports optimistic paper fills and an unequal comparison of
trading periods and sizes. It does not establish the cause or amount of the latest
live loss: the current VPS journals are not present locally.

## Recomputed historical results

Source: `data/execution_audit_20261021/orders.csv` and `metrics.json`.
The archived source is `all-records-20261021T2132Z.tgz` in Downloads; its SHA-256
matches the audit manifest. Figures below were recomputed from the CSV, using
confirmed fills and excluding unknown payouts from settlement P&L.

| Metric | Live | Paper50 |
|---|---:|---:|
| Submitted orders | 2,734 | 655 |
| Confirmed/modelled filled orders | 573 | 243 |
| Fill rate | 20.96% | 37.10% |
| Fills with known settlement P&L | 573 | 240 |
| Net settlement P&L | +$1.71 | +$307.83 |
| Median filled shares per order | 7.43 | 33.68 |

These are different trading periods, not matched signals. Paper50 submitted orders
from **2026-10-17 16:43:16 UTC to 2026-10-18 06:36:08 UTC**. There are **zero live
submissions during that interval** in this export. Three paper fills have unknown
payouts and are excluded from its P&L. Five unresolved live submissions are not
assumed to be confirmed fills. The full archived live period is slightly profitable,
so it cannot verify a later period's reported live loss.

## Why the gap can arise

1. **Paper grants fills from a modelled book, rather than an actual venue match.**
   `paper_trade.py` quotes the available ask ladder at the selected arrival instant.
   It subtracts its own recent simulated fills, but has no per-order observation of
   which competing orders or cancellations reached the matcher first. A public book
   snapshot does not prove those shares were executable for this order.

2. **The sampled delay is not the actual delay of the paired live order.**
   `Sniper.latency_plan()` in `sniper_mode.py` uses
   `self._rng.choice(self._live_delays)`. Samples are loaded at startup, with a fixed
   paper-only calibration added. This reproduces a historical marginal distribution,
   not the relationship between each signal, venue response, liquidity and fill.
   Matching the aggregate fill rate alone does not validate P&L or fill selection.
   ACK latency is also not an exact matching timestamp.

3. **Recorded simulations have credited fills that live did not get.**
   In the newer audit's earlier-grid sensitivity, 1,122 supported live orders predict
   345 fills and +$218.58, versus 238 confirmed fills and +$18.55 on those same orders.
   There are 123 false fill predictions and 16 missed actual fills. This is a
   historical diagnostic discrepancy, not proof that the current broker still uses
   that earlier-grid evaluator. The current code includes timing fixes and a 200 ms
   calibration, whose fidelity still needs paired live validation.

4. **Execution cost consumes a small gross edge.**
   The archived live period earned $58.42 before its recorded $56.71 of fees,
   leaving $1.71. Mean execution slippage was 1.33 cents per share versus the decision
   ask. This supports fees and execution prices as material costs, without proving
   that faster execution or a wider cap would improve future results.

5. **Sizing and strategy settings can differ.**
   The archived median paper fill was about 4.53 times the live median; these are
   unpaired samples. Current auto cash mirroring can fall back to paper's own cash
   when live data is stale or absent. Larger proportional sizing alone would not
   reverse the sign of P&L on identical fills; different trade eligibility and fill
   selection matter. The newer wide-cap live experiment can also switch itself off
   after its live loss stop, while paper retains its wide cap. Whether any of these
   settings apply on the VPS is unverified.

   There is an additional distinction in the current configuration: a positive
   `SNIPER_WIDE_CAP_C` bypasses the fair-price ceiling and permits a fixed minimum
   order even when the ask is above that ceiling. Increasing `SNIPER_SLIP` alone
   preserves the fair-price ceiling. Those are different strategies; the wide-cap
   path can deliberately pay more than the model's estimated value after fees.

## Reporting issue found during this investigation

The initial investigation found these problems in `handoff_scripts/pvl.py`:

- Paper sniper rows set `cost = fill['total_cost']`, already including fees, but
  `pvl.py` subtracts both `cost` and `fee` again. This understates paper P&L; it does
  not explain paper appearing more profitable.
- Its loader ignores `live_fill_update` rows, so delayed confirmed quantities or
  failed provisional fills can be missing from its comparison.

The figures above use the previously reconciled CSV instead of that helper.
The follow-up implementation corrects fee normalization, deduplicates deferred fill
updates, preserves their original signal times and labels unresolved outcomes.
The helper still reports journal observations, not confirmed-ledger reconciliation.

## What is needed for the current loss period

Read the current live and paper journals plus confirmed ledgers and settlement
records for the same hours. Join orders by market, side and signal identity/time;
deduplicate deferred updates and exclude unresolved outcomes from settled P&L.
Compare net P&L per share, prices, fees, order sizes, paper-only fills, live-only fills,
and skips. Inspect each run's logged configuration, sampled-delay age and any
`wide_cap_off` / `venue_min_off` events. Account for fractional settlement payouts.

The initial investigation changed no execution code or account state. The follow-up
adds strict paper comparison with cached live controls; see README.md. Simulated
fills remain unvalidated and the change does not guarantee live profitability.
