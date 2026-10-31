# Direction add-on: buy more on the side beating the price to beat, 2026-10-30

The operator asked to keep the sniper strategy the same, but add a little
direction for profit. This note gives the evidence and describes the change
(`SNIPER_DIR_ADD`, tests in `tests/tests_dir_add.py`).

## The evidence

Source: the 10-15..25 live archive (`live_sniper.jsonl`, `live_ledger.json`,
`live_ledger_settlements.jsonl`). The ledger holds 1,178 filled live orders. Each
was joined to its signal row and its market's on-chain winner. The settled result
for the period is +$38.84, which matches the settlement journal.

A fill counts only once. Orders whose fill was confirmed in the order reply are
journalled `filled` (586). Orders confirmed later are journalled `pending`, then
`live_fill_update` (592). An analysis of `filled` rows alone sees half the trades:
that half lost -$6.54, while the other made +$48.88.

**Price to beat.** The journal had no price to beat. It was rebuilt as Coinbase's
mid at the round's start, from signal rows within 10s of the start. That covers
901 of the 1,178 fills.

| fills, by the side bought vs the price to beat | fills | won | PnL |
|---|---:|---:|---:|
| beating it (BTC already past it, the signal's way) | 538 | 70% | +$93.11 |
| not beating it | 363 | 34% | -$64.48 |

**By distance from the price to beat.** BTC past it in the signal's direction:

| bp | fills | PnL |
|---|---:|---:|
| below -10 | 17 | -$13.43 |
| -10 to -3 | 115 | -$71.04 |
| -3 to 0 | 231 | +$19.98 |
| 0 to 3 | 332 | +$50.26 |
| 3 to 10 | 159 | +$9.01 |
| above 10 | 47 | +$33.85 |

The same split holds in the last four days, 10-22..25 (0.5bp trigger, 0.15 price
floor, the current settings): +$46.02 beating vs -$13.43 not. The operator stated
the rule; it was not fitted to these data.

**The market's own price is not a substitute.** Using "the side's Polymarket mid is
0.5 or more" in place of Coinbase's price gives a different answer on 25% of fills,
and does not hold on 10-22..25. The same is true of the other directional filters
tried: no chop, Polymarket already moving our way, or 30s BTC momentum. All looked
good on 10-15..21 and did worse than the bot without them on 10-22..25.

**What the add-on would have added**, by multiplying each beating fill's PnL (so the
extra shares are assumed to fill at the same price, which is optimistic):

| add-on | 10-15..25 | 10-22..25 | chance the add-on is really <= $0 |
|---|---:|---:|---:|
| +25% | +$23.28 | +$11.51 | 6-9% |
| +50% | +$46.56 | +$23.01 | 6-9% |
| +100% | +$93.11 | +$46.02 | 7-9% |

The add-on's daily PnL at +50% on 10-22..25 was +$7.6, +$14.5, -$5.4 and +$6.4. The
probability range is a bootstrap over markets. Four test days are not enough to
call the edge proven.

## The price to beat: what it is, and how the bot makes sure of it

**What it is.** A btc-updown-5m market resolves Up when Chainlink's BTC/USD
60-second TWAP at the round's end is at or above the same TWAP at its start. That
start value is the price to beat. Gamma declares this on every market
(`cryptoMarketConfig`: `twapEnabled`, `twapLookbackSeconds` 60), and
`market_discovery.py` already refuses any market that says otherwise. It is not a
single price at the start, which is what the first version of this change used.

**When Polymarket says it.** Polymarket publishes the exact value in the Gamma
event's `eventMetadata.priceToBeat` only after the round closes. A third-party
bot measured that at 30-300s after the close (alessiocriscio/PolyHarvest PR #8).
The same source finds a round's price to beat equal to the previous round's
`finalPrice`. So no exact value exists while a round can still be traded.

Polymarket's own documentation could not be read from this environment (its hosts
are blocked here), so the field name comes from that bot's code. It reports
having verified it on a live response. If the field is elsewhere, nothing is ever
calibrated and no add-on is bought: the journal's `ptb_official_missing` rows list
the `eventMetadata` keys Gamma did return.

**What the bot does** (`price_to_beat.py`, `Sniper._note_primary_mid`,
`_ptb_view`, `_ptb_official_loop`):

1. **Estimate.** At each round's first trusted trigger price, the bot computes
   the trigger feed's own 60s TWAP ending at the start. That is the same
   calculation on a different venue's prices: each price stands until the next.
   It needs a price at or before the window's start and no gap over
   `SNIPER_PTB_MAX_GAP_S` (5s); otherwise the round's price to beat is unknown.
2. **Correction.** The estimate is that TWAP plus the basis: the median of
   (published value - our TWAP) over the last 12 published rounds.
3. **Check.** From 20s after each close, every 10s for up to 30 minutes, the bot
   reads the round's published value from Gamma and journals a `ptb_official`
   row: the published value, our TWAP, the estimate used live, its error in bp,
   and the basis now. A published value over 100bp from our TWAP is journalled
   (`ptb_official_suspect`) but never used. After a restart, these rows are read
   back from the end of the journal, so the checks continue at once.
4. **Decision.** The add-on needs the side past the estimate by more than both
   `SNIPER_PTB_MARGIN_BP` (1bp) and the largest error over the last 24 checks,
   after at least `SNIPER_PTB_MIN_CHECKS` (6) checks. An error counts once the
   basis behind it had 3 published rounds. So the estimate would have to be
   wrong by more than it has recently been wrong to turn "beating" into "not
   beating". Too close, not yet checked, unknown or a stale price: no add-on,
   and `dir_add_why` says which.

`live_pnl.py` prints PRICE TO BEAT CHECKS: published, never published, set aside,
the average and largest error, and the basis now. That is the evidence of how
exact the estimate is on the VPS.

## The change

1. **Every decided signal** journals `ptb` (the estimate), `ptb_dist_bp` (how far
   the trigger mid is past it, in the signal's direction), `ptb_margin_bp`,
   `ptb_checks` and, when it cannot say, `ptb_why`. This happens whether or not
   the add-on is on. A mid older than 2s counts as unknown.
2. **The add-on** (`Sniper._dir_add`) runs after Kelly has sized the sniper's order
   and the order has been rounded to whole-cent shares. If the side is beating the
   price to beat, the order gets `floor(SNIPER_DIR_ADD x Kelly target)` more
   shares. The extra is limited to the room the sniper's own limits leave: the
   side cap, cash, the hybrid risk budget and the canary per-order cap. In LIVE,
   `LiveExecution.addon_headroom` first checks that the bigger order still passes
   three send checks: free collateral, the canary exposure cap and the loss
   budget. If it would not, the add-on is dropped and `dir_add_why` says why. An
   order the sniper would skip stays skipped. A fixed-size order (a minimum entry,
   or the wide-cap experiment) gets nothing.
3. **Kelly** sizes the sniper's later orders without the add-on's filled shares
   (`_dir_book`, pro rata by shares, and undone on a retracted fill). So the
   sniper's own sizing stays as it was. The per-side cap is still shared. After a
   restart the add-on's earlier shares count as the sniper's for that round, which
   is the conservative direction.
4. **Live** refuses `SNIPER_DIR_ADD` without `SNIPER_DIR_ADD_LIVE=1`, and above
   0.5. Strict paper comparison includes the five settings, so paper with the
   add-on beside a live bot without it needs `SNIPER_PAPER_LIVE_PARITY=0`.
5. **Reporting.** `live_pnl.py` (and `--paper`) prints a DIRECTION ADD-ON section:
   - fills beating vs not beating the price to beat;
   - the add-on's own PnL apart from the sniper's part of the same orders;
   - what +25% / +50% would have made at the same prices.

   The dashboard shows the price to beat and the distance.

## The direction-gated wide limit (2026-10-31)

The operator asked for a wide limit with direction, tested directly in live. A replay
of 3,858 live orders (10-15..25) estimated it. Each order was priced against the book
the bot had when the venue replied, and scored with the real winners. At the actual
limits, the replay agreed with the venue's fill or miss on 93.8% of orders. It came
to +$63.60 where live made +$54.71, so it reads somewhat high.

| Same orders, replayed | Fills | PnL | First half | Second half |
|---|---:|---:|---:|---:|
| the actual limit | 1,029 | +$65.01 | +$45.63 | +$19.38 |
| ask + 0.10 for every order | 3,007 | -$47.89 | -$0.05 | -$47.85 |
| ask + 0.10 only when the side is past the price to beat by over 1bp | 1,690 | +$197.58 | +$150.96 | +$46.62 |
| that, plus the add-on at +50% | 1,690 | +$272.69 | +$211.57 | +$61.12 |

Halves split at 10-23 00:53 UTC. The price to beat in this replay is Coinbase's price
at the round's start (within 10s); live uses the checked 60s TWAP estimate. The wide
limit's extra fills by side vs the price to beat:

| Side vs the price to beat | Fills | Won | PnL |
|---|---:|---:|---:|
| past it by over 5bp | 192 | 86.5% | +$37.55 |
| 1-5bp | 469 | 72.7% | +$92.14 |
| 0-1bp | 228 | 53.5% | -$48.32 |
| not past it | 621 | 41.2% | -$115.06 |
| unknown | 468 | 62.6% | -$68.22 |

`SNIPER_WIDE_CAP_DIR=1` applies the wide limit only when `_ptb_view` says the side is
past the estimate by more than its margin. Orders whose ask is above the fair limit
are still skipped; the replay could not price them. Run under `SNIPER_WIDE_CAP_LIVE=1`,
its loss stop (`SNIPER_WIDE_CAP_LIVE_STOP_USD`, state file `wide_cap_live_test.json`)
also ends the direction add-on.

## Rollout

1. **One week:** live with `SNIPER_DIR_ADD=0`, which only journals the price to
   beat and its checks. Alongside it, paper with `SNIPER_DIR_ADD=0.5` and
   `SNIPER_PAPER_LIVE_PARITY=0`. Check each with `live_pnl.py`. Its PRICE TO BEAT
   CHECKS section must show published values arriving, and an error well under
   the distances the add-on acts on.
2. **If the beating side keeps its edge in live's journal:** live
   `SNIPER_DIR_ADD=0.25`, `SNIPER_DIR_ADD_LIVE=1`.
3. **Later:** 0.5, if the add-on's own PnL stays positive.

## Limits

- Live, the price to beat is an estimate from a different venue's prices. The
  margin guards against its measured errors, but not against one larger than any
  of the last 24. A sharper estimate would use Chainlink's own prices, for example
  Polymarket's real-time data socket. That source could not be reached or checked
  from here.
- The evidence above used Coinbase's single price at the start, not the 60s TWAP
  (the journal has no continuous price history to rebuild it). The beating /
  not-beating split may shift a little with the exact definition.
- The evidence covers filled orders only. A bigger order goes deeper into the
  book, so it pays a higher average price than the estimates assume.
- There is no automatic stop for the add-on alone. The canary loss budget, the
  hybrid drawdown stop and the wallet still apply to everything.
