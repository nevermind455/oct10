# Liquidity disappearance attribution (measurement only, 2026-10-18)

Question: when the bot sees executable asks under its cap after a BTC signal,
are they consumed by public trades, withdrawn without trades, or is the
evidence insufficient?

Nothing here changes a trading decision, an order, a cap, a size or a safety
control. All of it runs after the order is handed off (or off the event loop).

## What is collected

| Data | Where | Notes |
|---|---|---|
| Public trade prints (`last_trade_price`), accepted or refused with the reason | `market_prints/market_prints-YYYYMMDD.jsonl` (feeds/print_log.py) | raw price/size/side strings, venue ms, local wall + monotonic receive time, connection id, ingest sequence, schema version; 3 days kept; `SNIPER_PRINTS_LOG=0` off |
| Market-feed connect / disconnect / subscribe events | same file | the coverage record for the prints |
| Book states | the bot's own book history (30 s, every installed state, gap markers on disconnect/resync/reset) | read after the reply, never persisted in full |
| Per-order attribution | `book_trace` rows, field `attribution` | steps between trace instants, spans 0-50 / 0-150 / 0-250 ms / signal-reply, side check, raw level changes and prints (bounded) |

Prints before 2026-10-18 were never written to disk (memory only), so no
older archive can be attributed: those orders report `NO_TELEMETRY`, never
"zero trades".

## How a window is attributed

1. Gross ask reductions and additions under the ORIGINAL cap, summed per price
   level between consecutive installed book states (never across a gap).
2. Public prints RECEIVED in the window (local monotonic time, the same clock
   as the book states) for the exact outcome token, priced at or under the cap.
3. `matched_trade_depletion_estimate = min(BUY print volume, gross reductions)`;
   the rest of the reductions is `unexplained_reduction_estimate`.
4. Labels (thresholds `SNIPER_ATTRIB_HI`=0.7 / `SNIPER_ATTRIB_LO`=0.3):
   TRADE_DOMINATED, NON_TRADE_DOMINATED, MIXED, NO_DEPLETION (< 1 share),
   INCONCLUSIVE (coverage not COMPLETE_OBSERVED, or prints without a side
   above 20% of the reductions).

Coverage: COMPLETE_OBSERVED (history covers the window start, no gap inside),
GAPPED, PARTIAL (history starts inside), UNKNOWN (no history). COMPLETE_OBSERVED
means continuous local observation - not proof the venue sent every event.

## Limitations - what this cannot prove

- **Print side semantics are assumed** to be the taker's side. The `side_check`
  (does the ask or the bid at the print's price shrink in the next book state?)
  tests that empirically; read every label as uncertain until it agrees.
- **Prints carry no trade id** in the market channel: identical prints are all
  kept (a replay and two real trades look the same); matching is capped at the
  observed reductions so duplicates cannot inflate it.
- **Unexplained is not a proven cancel.** It is consistent with cancels or
  repricing; it can also hide aggregation (several updates in one state) or
  events the venue did not send. Additions are counted separately, so new asks
  arriving do not mask reductions, but a cancel and a re-post at the same level
  inside one state are invisible.
- **Alignment is by local receive time.** A print can arrive after the book
  update it caused; `liquidity_report.py` shows the label sensitivity to a
  0-250 ms tolerance. Venue and local clocks are never subtracted from each
  other.
- **No queue position or competitor identity** is observable on public L2.
- **Small samples prove nothing.** Labels are diagnostics, never gates.

## Reading it

    python liquidity_report.py live_sniper.jsonl --hours 24 --csv out.csv --json out.json
    bash export_records.sh          # archive incl. the persisted prints (VPS)
