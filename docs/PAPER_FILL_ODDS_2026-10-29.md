# Honest paper fills: live's fill odds by 5s markout, 2026-10-29

The operator asked to "make paper honest": paper kept winning while live kept
losing. This note gives the evidence and describes the change (`SNIPER_FILL_ODDS`,
tests in `tests/tests_fill_odds.py`).

## What live misses

Source: the 10-15..21 live archive (`live_sniper.jsonl`, `live_ledger.json`).
It holds 1,157 live orders that also have a shadow row: our book copy at
100..500ms after the tick at the order's real cap, with `mk5` (the mid 5s later,
less price and fee per share, in cents). Live filled 246 of them (21%). The
journal's `kind` (MATCHED / NO_MATCH) agrees with the ledger on 1,155 of the
1,157.

Our book copy at 400-500ms fills 29-33% of these orders. That is about where paper
meets it: a live delay plus `SNIPER_PAPER_EXTRA_MS` 200. A longer delay does not
fix the gap. At 500ms the book copy still fills 144 orders that live missed, worth
+$131.

What decides it is where the price goes next. Of the orders the book copy fills at
300ms:

| 5s markout | live filled |
|---|---:|
| below -1c (price ran against us) | 71-76% |
| -1c to +3c | 49-62% |
| +3c to +6c | 40% |
| +6c or more (price ran our way) | 20% |

Makers pull their quotes when the move is coming, and our book copy shows it too
late. Within one markout bucket, live fills and live misses have about the same
P&L per order, so keeping book fills at those odds removes the bias. The limit room
(cap - fill price) did not change the odds within 0-5c.

## The model

1. Paper prices each order from the book at its arrival, as before.
2. It waits 5s, measures the order's markout (the shadow's `mk5` arithmetic), and
   keeps the fill with probability equal to live's odds for that bucket
   (`FILL_ODDS_EDGES_C`: -3, 0, 3, 6, 10c, plus "none" when there is no mid).
   Otherwise the order is a miss: "makers pulled (live fill odds ...)", outcome
   `LIVE_ODDS_MISS`.
3. The odds are learned from the newest `SNIPER_FILL_ODDS_ORDERS` (1000) live
   orders in `LIVE_SNIPER_JOURNAL`, at startup and every 30 minutes. Only orders
   whose limit room is within paper's are used. A paper limit wider than any
   live order's is flagged EXTRAPOLATED on the settings line.
4. The arrivals are drawn as paper draws its own: quantiles of the loaded live
   delays plus the extra. An order's own reply time is never used, because
   misses reply later (median 299ms against 259ms), so it would leak the outcome.
   Fitting on each order's own time gave odds of 60-96% and no correction.
5. While an order waits, its side is free for the next order, as a live reply
   frees it. Live sent another order on the same side within 5s on 11% of
   orders. The order's cash stays reserved, its shares count as same-side Kelly
   risk, and paper matches run on their own 32 threads.

## Check on days the model had not seen

The odds were fit on the first half of the archive (578 orders) and tested on the
second half (579). Paper's arrival was drawn from the first half's live delays plus
200ms:

| | fills | P&L |
|---|---:|---:|
| live (actual) | 114 | -$24.40 |
| paper as it was (book only) | 158 expected | +$61.84 (one run, 10-90%: +$36 .. +$86) |
| paper with live odds | 94 expected | -$4.18 (one run, 10-90%: -$36 .. +$31) |

## Limits

- Fills are random per order: judge a paper run over two or more days.
- Paper fills about 18% fewer orders than live, because live fills some orders
  that the book copy shows as unfillable. P&L is close; the fill count runs low.
- The odds come from live's own limits. A wider paper limit is an extrapolation
  until live has run near it.
- Fills appear 5s after the match.
