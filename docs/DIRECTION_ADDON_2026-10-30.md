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

## The change

1. **Price to beat.** Each trusted trigger-feed mid (those the volatility model
   takes) is offered to `Sniper._note_primary_mid`. The first one at or after a
   round's start becomes its price to beat, if it came within
   `SNIPER_PTB_MAX_GAP_S` (5s). Otherwise the price to beat is unknown for that
   round: after a restart mid-round, or a feed outage at the start.
2. **Every decided signal** journals `ptb` and `ptb_dist_bp`: how far the trigger
   mid is past it, in the signal's direction. This happens whether or not the
   add-on is on. A mid older than 2s counts as unknown.
3. **The add-on** (`Sniper._dir_add`) runs after Kelly has sized the sniper's order
   and the order has been rounded to whole-cent shares. If the side is beating the
   price to beat, the order gets `floor(SNIPER_DIR_ADD x Kelly target)` more
   shares. The extra is limited to the room the sniper's own limits leave: the
   side cap, cash, the hybrid risk budget and the canary per-order cap. In LIVE,
   `LiveExecution.addon_headroom` first checks that the bigger order still passes
   three send checks: free collateral, the canary exposure cap and the loss
   budget. If it would not, the add-on is dropped and `dir_add_why` says why. An
   order the sniper would skip stays skipped. A fixed-size order (a minimum entry,
   or the wide-cap experiment) gets nothing.
4. **Kelly** sizes the sniper's later orders without the add-on's filled shares
   (`_dir_book`, pro rata by shares, and undone on a retracted fill). So the
   sniper's own sizing stays as it was. The per-side cap is still shared. After a
   restart the add-on's earlier shares count as the sniper's for that round, which
   is the conservative direction.
5. **Live** refuses `SNIPER_DIR_ADD` without `SNIPER_DIR_ADD_LIVE=1`, and above
   0.5. Strict paper comparison includes the three settings, so paper with the
   add-on beside a live bot without it needs `SNIPER_PAPER_LIVE_PARITY=0`.
6. **Reporting.** `live_pnl.py` (and `--paper`) prints a DIRECTION ADD-ON section:
   - fills beating vs not beating the price to beat;
   - the add-on's own PnL apart from the sniper's part of the same orders;
   - what +25% / +50% would have made at the same prices.

   The dashboard shows the price to beat and the distance.

## Rollout

1. **One week:** live with `SNIPER_DIR_ADD=0`, which only journals the price to
   beat. Alongside it, paper with `SNIPER_DIR_ADD=0.5` and
   `SNIPER_PAPER_LIVE_PARITY=0`. Check each with `live_pnl.py`.
2. **If the beating side keeps its edge in live's journal:** live
   `SNIPER_DIR_ADD=0.25`, `SNIPER_DIR_ADD_LIVE=1`.
3. **Later:** 0.5, if the add-on's own PnL stays positive.

## Limits

- The price to beat is the trigger feed's mid at the round's start, not the
  Chainlink price the market settles on. The two can differ (not measured here),
  so a fill very close to the price to beat can be on the wrong side of the real
  one.
- The evidence covers filled orders only. A bigger order goes deeper into the
  book, so it pays a higher average price than the estimates assume.
- There is no automatic stop for the add-on alone. The canary loss budget, the
  hybrid drawdown stop and the wallet still apply to everything.
