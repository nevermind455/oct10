# Sizing and execution audit, October 20, 2026

The audit reproduced and repaired defects in the hybrid sizing, FAK execution,
cash reservation, dispatch and crash recovery paths. Fair margin remains $0.01,
the Kelly input fraction remains 0.25, the minimum-entry exception remains
enabled, and the two additional per-trade caps remain disabled by default.

## Confirmed defects and resulting behavior

- **Drawdown boundary:** a high-water mark of $123.45 and equity of $98.76 is
  exactly 20% drawdown. Float division previously left a tiny positive fraction,
  which the minimum-entry exception could turn into a full order. Decimal
  monetary comparisons now stop allocation exactly at or beyond the boundary.
- **FAK maker-dollar execution:** the recorded October 14 incident spent $3.74
  at $0.29 and bought 12.896552 shares, although the order was sized as eleven
  shares at a $0.34 cap. The old share-ceiling assumption could falsely halt
  live trading and understate paper quantity. Paper now walks the fixed signed
  dollar budget. Live accepts improved-price quantity within that budget while
  retaining identity, price-cap, trade-set and signed-spend checks. Partial fills
  are classified by unspent dollars; arrival tick refreshes cannot resize stake.
- **Paper fees:** only principal was removed from the cached bankroll. With
  $4.77 cash, a $2.30 fill plus fees could leave a second $2.30 order appearing
  affordable when it was not. Dispatch now reserves the conservative all-in
  amount before the worker starts.
- **Slow paper orders:** cash reservations used to expire after two seconds
  even if the worker had not finished. Pending reservations now remain held;
  completion retains a short tail to cover cash reads begun before booking.
- **Failure handling:** an escaped paper persistence error was logged but
  trading continued. Worker failures, interrupted waits and executor failures
  now stop new decisions and retain uncertain reservations. Unexpected live
  preparation errors release unused minimum-entry claims and stop decisions;
  uncertain dispatch errors retain claims and cash for reconciliation.
- **Archive crash recovery:** compaction could append a complete record onto
  an unfinished archive tail, permanently corrupting recovery. Append preparation
  now removes only the incomplete tail and preserves complete evidence, including
  valid final JSON missing its newline, before the existing archive fsync.

## Sizing semantics

The existing whole-share target times limit-price staking policy is preserved.
Kelly computes its nominal target at the decision ask; converting it to dollars
at the cap can increase stake above its ask-valued target. The minimum-entry
exception can also exceed that target. Improved-price execution can buy additional
shares. Share limits therefore remain pre-trade estimates, while signed principal,
cash, conservative fee reserves and enabled dollar limits remain binding. Actual
fills feed subsequent inventory checks. See HYBRID_SIZING.md for the full policy.

## Validation and limits

Twenty-eight new regression tests cover the reproduced failures. The combined
focused run exercised 390 unittest cases in isolated processes: **387 passed,
three native SDK checks skipped**, with no failures. This includes 3,000 random
book comparisons in the existing quote tests and concurrent intent-compaction
coverage. Another **188 accounting checks passed**. Compile and diff checks pass.
The review sandbox needs periodic asyncio selector wakeups for executor tests;
the test helper changes neither assertions nor production execution.

`python run_tests.py --require-native-sdk unified_sdk` still fails because
`polymarket-client==0.13.0` is not installed in this review environment. Native
signatures, authenticated exchange integration and live latency remain unverified.
No live orders were submitted and no trading-server deployment was performed.
The updated paper model requires a new replay; historical P&L from earlier logs
does not validate these fixes or the revised paper fill accounting.

## Completion check in the pinned environment

The review above could not install the SDK. A follow-up pass installed
`requirements.lock.txt` (with `polymarket-client==0.13.0`) on Linux with
Python 3.12.3, as CI does, and `pip check` found no conflicts.

- `python run_tests.py --require-native-sdk unified_sdk` passes. The three
  native checks (order wire payload, V2/V3 typed-data hashes, HMAC) ran, with
  no skips. The CI workflow now runs this strict check before the full suite,
  so a skipped SDK check fails the build.
- `python run_tests.py` passes all 69 suites. Before this pass, 10 of them
  failed in this environment.

What failed, and what changed:

- **Trading-file hash lock.** Eight trading files had changed on October
  19-20 without new digests. Eight others differed only by an extra trailing
  blank line that the uploaded copy had gained in 113 files. Removing that
  line restores their approved bytes exactly. The eight changed files are
  re-recorded in `tests_feeds.BASELINE_SHA`, each with what changed. The SDK
  adapter, the signing module and `hybrid_sizing.py` are now locked too.
  Review the diff of those eight against the last approved commit before
  trading live.
- **The A/B replay could not run the current strategy.** `replay_ab.py` never
  refreshed the hybrid risk snapshot, so every signal was skipped as "hybrid
  risk unavailable". Its broker also lacked the hybrid guard and still capped
  share-sized fills at the share count. It now refreshes the snapshot every
  5 seconds of replay time and fills share-sized FAKs by their signed dollars,
  as paper does. Replay results from before this change are not comparable.
- **Paper firewall.** With the SDK installed, paper blocks 58 entry points,
  not 18. The SDK clients' `cancel_market_orders` was missing; their
  transfer, approval, split/merge/redeem, perps deposit/withdraw and
  builder-key calls are now blocked in paper as well. The test checks every
  method of the installed SDK in those families.
- **Stale tests.** Fake brokers now declare share-sized support, which is
  the default sizing. A reservation test used the UP book's ask for DOWN. A
  check described the retired library's float rounding. An audit allowlist
  predated `clob_client.py`.

**Side cap: behavior unchanged, flagged for the operator.** The October 15
rule "near a cap it spends at most the room at the ask" no longer holds. The
side's room now bounds the signed shares, so a fill at the decision ask can buy
up to cap/ask times that. With 10 shares of room, a 0.47 ask and a 0.52 cap,
the order signs $5.20 and buys 11.06 shares. This follows this audit's "share
limits are pre-trade estimates" and its own room tests, so the October 15 test
was updated to match. The dollars at risk are the signed stake either way.
Restoring the old rule means signing fewer shares near a cap.

Still unverified: authenticated exchange integration, live latency and live
fills. No orders were sent and nothing was deployed.
