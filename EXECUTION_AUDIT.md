# Execution audit and offline replay

`execution_audit.py` reads the downloaded VPS archive without extracting it,
opening credentials, importing bot configuration, or making network requests:

```powershell
python execution_audit.py --archive C:\Users\aakas\Downloads\sniper-records.tgz --out data\execution_audit --holdout-utc 2026-10-16T00:00:00+00:00
```

Private results stay under the ignored `data/execution_audit/` directory.
`metrics.json` contains input hashes, coverage/exclusions, run versions,
actual results, latency distributions, explicitly labelled snapshot
sensitivities and a retrospective chronological validation. `orders.csv`
joins each submission to confirmed ledger lots. Do not publish these files
without checking their trading data.

The archive layout expected is `sniperbtc/` (live and old paper) and
`sniperbtc-paper50/`. The tool rejects duplicate identities, malformed JSON,
unapplied ledger journals and sale histories it cannot account for. It does
not silently replace unknown order outcomes with zero fills. Real P&L uses
confirmed principal plus fees and actual settlement payouts. Paper journal
`cost` includes fees; legacy live `cost` is principal. They must not be
compared or have fees deducted identically.

Terminal canary NO_MATCH records with explicit spent=0 are a separate
RECONCILED_ZERO_CANARY evidence tier. Signed order bodies in that state file
are never included in outputs. Confirmed ledger lots take precedence. The
persisted original reservation amount may remain positive after release;
it is not evidence that the terminal order is still live. This is the bot's
recorded reconciliation, not a fresh independent venue query.

`guard_eval.py` and `exec_eval.py` now require an exact recorded shadow grid
instant for their policy comparisons. An instant between recorded snapshots
is unknown. ACK timing is an observable proxy, not a measured matching-engine
timestamp. The audit separately reproduces the old floor selection and the
next snapshot as sensitivities. Neither is an exact counterfactual execution
or a bound: books can change and replenish between observations. Unknown
books and out-of-grid observations are excluded with explicit counts.

The validation selects an extra-delay scenario using fill-classification
error on training conditions only, with a five-minute embargo before the
cutoff. Candidates share the same evidence coverage. Later conditions are
reported separately. Confidence intervals resample paired 30-minute time
blocks, not individual orders. This archive was previously inspected and
used for an older extra-delay calibration, so this is retrospective evidence,
not a pristine prospective trial. Do not choose trading thresholds or report
new live profitability from these outputs. Aggregate fill-rate agreement
alone does not validate a simulator.

## Event replay

`execution_replay.py` is an independent, deterministic BUY simulator. It is
not connected to the live sender or substituted into the running paper bot.
Use its module docstring as the event/order schema:

```powershell
python execution_replay.py recorded_events_and_orders.json replay_result.json
```

It supports delayed FAK/FOK and POST_ONLY arrivals, fixed original limits,
depth walking, partial fills, fee curves, recorded tick/minimum rules, own
liquidity consumption, cancellation effective times and conservative maker
volume ahead. It never credits a maker fill just because the market touched
the order's price. Only unique aggressive prints at that price can reduce
the model queue. Cancellations of other orders do not improve queue priority.
Own makers share print volume and their known cancellations remove their
remaining size from later own orders' queues.

Repeated snapshots do not restore consumed taker depth. A level must
disappear before that level's consumption resets. This can underfill when
real liquidity is replaced at the same price: it is a documented conservative
scenario rather than an invented replenishment clock. The production
PaperBroker still uses its configured own-fill TTL approximation; this audit
does not certify that heuristic as observed venue replenishment.

Recording gaps invalidate the book and any existing maker queue identity.
A full snapshot recovers the public book, not the old order's queue rank.
Actions after the final recorded coverage return UNKNOWN_COVERAGE. All
market events run before our action at the same timestamp: that ordering is
an explicit scenario convention, not a claim about unobserved engine order.

The current VPS export has sparse shadow quotes, not a complete market event
recording. Do not fabricate the missing sequence, print IDs, queue ranks,
clock offsets or matcher arrival times to run it. A future recording needs
full initial ladders and updates, reconnect/gap markers, unique prints with
aggressor direction, public and local monotonic receipt timestamps, clock
uncertainty, private confirmed trade identities, and contemporaneous market
rules. Public L2 alone cannot establish exact queue position or how many
shares any particular competitor took.

## Corrections

- Evaluation joins live executions to CONFIRMED ledger lots rather than
  provisional matched receipts. Unknown orders remain unknown.
- Policy replay no longer rounds latency down or invents missing latency.
- Split/fractional payouts are valued as payouts instead of binary wins.
- Public prints matched on price and size are UNVERIFIED candidates, require
  paired size/price legs and an explicit BUY side, and cannot populate the
  measured trigger-to-match latency series.
- Arrival comparison diagnostics use FAK partial-fill semantics when the
  submitted order is FAK, instead of falsely reporting an FOK rejection.

No price caps, sizing formulas, retry policy, risk pauses or STOP_LIVE
behavior are modified. No network-speed or live-profit improvement is
claimed by these changes. Public print schema and fees are described in
[Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data)
and [Polymarket fees](https://docs.polymarket.com/trading/fees).

Run `python run_tests.py` before deployment. No bot is started by either CLI.

The shared checkout also contains strategy changes from another session.
Audit validation uses an ignored snapshot of the original HEAD strategy
plus this audit's changes, with a separate manifest under
`data/execution_audit/validation_manifest.json`. It does not approve or
overwrite that other session's strategy changes.
