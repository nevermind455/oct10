# Live analysis 2026-10-25

Source: the VPS's `sniperbtc/` records of 10-25 10:14 UTC (`live_sniper.jsonl` 148 MB, `live_ledger.json` +
journal, settlements, canary state). Every filled live order since 10-04 (1,178), and in detail the `0533fa9`
run: 10-22 23:11 -> 10-25 ~10:10 UTC, 59 h, 2,001 orders (517 matched, 1,483 no match, 1 unknown).
Ledger, journal and settlements reconcile; no redemption or cash problem (wallet - applied p99 $1.1).

## Results

| | filled orders | stake | P&L | per stake | t |
|---|---|---|---|---|---|
| all live (10-04 ->) | 1,178 | $4,554.77 | +$42.34 | +0.9% | 0.39 |
| `0533fa9` run | 513 | $1,621.69 | +$40.07 | +2.5% | 0.66 |
| ... minimum entries | 305 | $790.18 | +$5.01 | +0.6% | 0.12 |
| ... Kelly-sized | 208 | $831.51 | +$35.06 | +4.2% | 0.80 |

No significant edge yet (t < 1). Minimum entries were 59% of the run's fills at breakeven; off by default
since `9695c07`.

**Fair value is too confident.** All 1,178: 670 wins; fair predicted 701.0 (z = -2.12), the market mid 621.1,
the prices paid 646.7. The move added 49 wins over the mid; the model expected 80 - about 60% of its move
term is real. Not changed (frozen spec). Suggestion: scale the move term by ~0.6 and prove it on paper first.

## What decides a fill (`0533fa9` run)

| Coinbase tick lateness (past its usual) | < 2ms | 2-5 | 5-10 | 10-20 | 20-50 | > 50 |
|---|---|---|---|---|---|---|
| fill rate | 40.6% | 28.8% | 16.7% | 15.9% | 17.5% | 10.5% |

The race is lost within a few milliseconds of the print. In-process trigger -> send varied 5-12ms with no visible
gradient (24.8-29.7%) at that spread, so the feed is the larger lever: `SNIPER_EXTRA_FEEDS=binance_fut` (the
repo measured it leading Coinbase on ~75% of moves) is worth a paper run. Venue replies <= 250ms filled 57-61%,
slower ones ~16% (partly an outcome effect). The slow-reply guard paused orders 27 times (5 min each).

## Bug: the order path slowed down as the session aged

decided -> wire, p50 by day: 10-15 3.86ms, 10-19 5.56, 10-23 6.34, 10-24 7.60, 10-25 8.27.

Per order (p50, ms after the decision, last 2,001 orders): prepare 1.49 | sign 1.87 -> 2.94 | request ready 3.35 |
durable intent 4.58 -> 5.80 | on the wire 7.26. Three gaps grew with history: before `prepare`, before the
intent and after it. Each is a loss-budget read (`prepare`, then `send_guard` in both `_check_send`s) that summed
the realized P/L since the canary session start - 10-15 16:42 UTC, kept across restarts - by walking every
position settled since. Fixed (O1), with the FAK walk in `prepare` (O2): `FIXES_2026-10-25.md`.

## Execution changes

| | before | after |
|---|---|---|
| loss-budget read, 600 settled positions | ~376us, 3x per order | ~2us (kept until a settlement) |
| build + sign | ~0.6ms (`sign_typed_data`) | ~0.1ms, same bytes (O3, O4) |
| packet -> event loop behind a dashboard frame | 10.4ms p50 (5ms GIL switch) | 2.4ms (1ms, O5) |
| bench, VPS ledger copy, fsynced intents, same host back to back: trigger -> send p50 / p95 | 7.82 / 13.2ms | 4.16 / 6.34ms |
| ... decided -> prepare / build+sign / ready -> write | 1.34 / 1.56 / 3.36ms | 0.36 / 0.59 / 1.48ms |

## Not changed

- Fair value calibration (above) - a strategy change.
- The slow-reply guard (27 pauses): a policy choice; fills during slow replies were not worse.
- `SNIPER_EXTRA_FEEDS=binance_fut` - recommended for a paper run.
