# BTC sniper (sniperBTC)

A Polymarket bot that trades one thing: Polymarket's 5-minute **btc-updown-5m**
markets, the instant BTC moves. It runs in **paper** (simulated fills on the real book, the default) or as a capped **live canary** (real orders under hard limits).

This repo is carved out of `spe24` at **aaff811**: b314d70's engine, plus
ace4d41 (the paper model's documented 250ms taker hold) and aaff811 (order rows
journal their market and taker-delay flag). The trading engine was copied byte for
byte, with no rewrite. What is new: `run.py` (the runner, which
replaces `run_feeds.py`'s `SNIPER_ONLY` path), the sniper dashboard, `setup.sh`
and the tests for those. The old main strategy (`main_bot.py`, `strategy.py`,
`feeds/adapters.py`, Chainlink, research tools) is not here.

## The strategy

- **Trigger.** The BTC mid moves at least `SNIPER_K_BP` (0.5bp; 0.3bp briefly on 2026-10-11) within `SNIPER_WINDOW_MS` (250ms).
  The mid comes from **Coinbase Exchange's BTC-USD ticker** by default (the frozen
  spec's feed since 2026-10-10), or Binance BTCUSDT bookTicker, or Kraken BTC/USD.
  The move is measured against the last price at or before the lookback, and
  that reference must be no more than 2s old. Kraken heartbeats carry the price
  forward; Coinbase heartbeats do not (its ticker speaks on matches, so the book
  can move between messages unseen). A move up buys **UP**; a move down buys
  **DOWN**, in the current round.
  - **Coinbase.** One socket to `wss://ws-feed.exchange.coinbase.com`, channels
    `ticker` + `heartbeat` for BTC-USD. The ack must cover both; an `error` or a
    wrong ack reconnects. A ticker older (by sequence) than one already used is
    dropped. No message for `SNIPER_COINBASE_SILENT_S` (5s; heartbeats come every
    second), or heartbeats with no ticker for 60s, reconnect (0.25s..8s,
    jittered); every reconnect starts the rule afresh.
  - Measured from the PC on 2026-10-10 (45s, a quiet Saturday): 103 tickers,
    match -> receipt 64ms min / 104ms median, but gaps between tickers up to
    10.7s, 4 of them over 2s. In quiet spells the rule restarts more often than on
    Binance's book ticker. Measure it on the VPS before trading live.
- **Gates.** An order only goes out if all of these hold:
  - Cooldown: 300ms per side, started when an order is submitted. An UP order never blocks a DOWN signal, and vice versa.
  - At most one order in flight.
  - The round window allows it, with at least `SNIPER_MIN_LEFT_S` seconds left.
  - The book is LIVE and fresh, and has asks on the chosen side.
  - The ask is between `MIN_BUY_PRICE` and `MAX_BUY_PRICE`.
  - The per-round caps (`SNIPER_MAX_FILLS_ROUND`, `SNIPER_MAX_SIDE_SHARES`) are not reached.
  - The trigger feed's freshness gate passes: the feed is not behind (each
    Coinbase ticker's own match time; Binance's trade events), has no backlog,
    and its age is known.
- **Order.** An IOC limit order (Polymarket's FAK) with a share target:
  - **Target.** 10 shares, plus whatever this side's last order in this market
    did not fill, capped at 20 shares.
  - **Limit.** The freshest best ask + `SNIPER_SLIP` ($0.05). One order buys what is
    offered from the ask up to +5c, and nothing above. It is fixed at the decision,
    never recomputed from a later ask, and clipped at `MAX_BUY_PRICE`.
  - **Sizing.** The stake buys the target on the book the decision saw, walking
    the levels from the ask up to the limit. It is at least $1, and at least the
    venue minimum shares x the limit.
  - **Fills.** A partial fill is kept, and its remainder waits for the next
    same-side signal. It is never retried. A zero fill is not a fill.
  - **Carry.** UP and DOWN carries are kept apart, and both are cleared when the
    market changes.
  - **Fair value (2026-10-11).** Every buy, first or add-on, must be at or under what
    the share is worth now, less the taker fee and `SNIPER_FAIR_MARGIN` ($0.01):
    fair UP = Φ(ln(BTC now / BTC at the round's start) / (σ·√seconds left)), DOWN =
    1 − UP, with σ the trigger feed's own volatility over the last 5 minutes. The
    limit is capped at that price too. The start price is the trigger feed's price at
    the round's first instant (Polymarket settles on Chainlink's, usually a few dollars
    off). A bot that joined mid-round, or has under a minute of history, does not buy
    ("fair value unknown"). The dashboard's trigger panel shows fair UP / DOWN.
  - **FOK.** `SNIPER_ORDER_TYPE=FOK` keeps the old all-or-nothing order with no carry.
- **Paper fills.** Paper fills against Polymarket's book at the modelled match
  instant. With `SNIPER_DELAY_SOURCE=live`, each order's delay is drawn from real
  live orders.
- **Exit.** Every position is held to settlement.

## Execution latency

Every order records T0..T7 (`exec_latency.py`): feed event received, trigger fired,
decision done, order built, signed, request dispatched (bytes to the kernel),
Polymarket's ACK, and the first authoritative fill observation with its source
(`ACK`, `PRIVATE_WS`, `ORDER_STATUS`). The dashboard's SNIPER EXECUTION LATENCY
panel shows every stage and LAST/AVG/P50/P95/P99/MAX for trigger -> send, -> ACK
and -> fill known. Paper has no venue, so its ACK and fill are the model's,
labelled `sim`.

Measured on the live code path (`python bench_execution.py`: real signing, a warm
HTTP/2 connection, a fake CLOB on 127.0.0.1), trigger -> send is about 0.4ms. What
is left of trigger -> ACK is outside the process:

- **The network.** Run `python bench_network.py` on the VPS: warm round trip to the
  CLOB, TCP/TLS setup, and the Binance and Polymarket sockets.
- **Polymarket's taker delay.** Crypto markets hold every taker order: 150ms since
  2026-09-04 14:00 UTC (docs changelog; 250ms until 2026-08-17). The reply waits for
  it, so trigger -> ACK cannot go below about 150ms plus one round trip.
- **The paper model.** It still assumes the old 250ms hold (`SNIPER_HOLD_MS`).
  `SNIPER_HOLD_MS=150` models the venue as documented now. It also moves the
  end-of-round cutoff 0.1s later, which is why it is your choice, not a default.

**The frozen spec.** `--live` refuses to start unless the resolved settings are
exactly the frozen spec (`exec_latency.FROZEN_SPEC`: Coinbase, 0.50bp in 250ms, FAK,
ask+$0.05, 300ms per side, 10 shares, carry to 20, every buy at or under fair value
− fee − $0.01) and `SNIPER_CANARY_MAX_SHARES` admits the 20-share carried target. A missing
`.env` value falls back to a code default; for the trigger and the limit those are the
spec (0.5bp, ask+$0.05). Every
start prints the resolved configuration and which ECDSA backend signs (install
`coincurve`: ~0.1ms per order against ~1.4ms in pure Python).

## Position Manager

An optional layer beside the sniper (`position_manager.py`), **off by default**
(`POSITION_MANAGER_ENABLED=0`). It never changes what the sniper does. It watches
both sides of the current market: the ledger's confirmed fills, both books, and its
own pending orders. It buys only when a purchase measurably improves the position:

- **PAIR_COST_IMPROVEMENT.** New matched UP+DOWN shares cost less than $1 after
  fees and expected slippage, by `POSITION_MANAGER_MIN_PAIR_IMPROVEMENT`. One UP
  plus one DOWN pays $1 whichever side wins (0.50 + 0.50 on a 50-50 result), and
  fees are charged at the purchase. It buys only as much as pairs the other side's
  unpaired shares.
- **SIDE_AVG_IMPROVEMENT.** A lower average on a side already held, by
  `POSITION_MANAGER_MIN_AVG_IMPROVEMENT`. A lower average is not lower risk: every
  extra share of one side adds its whole cost to the loss if the other side wins.
  So by default only a buy adding at most $1 of worst-case loss passes. Larger ones
  are `AVG_IMPROVES_BUT_RISK_REJECTED`.
- **EXPOSURE_IMPROVEMENT.** Cutting the worst-case loss at a pair cost the operator
  allows. Off by default (`POSITION_MANAGER_EXPOSURE_MAX_PAIR_COST=0`): hedging is
  not free.

Whatever a buy is for, the UP+DOWN pairs it creates must pay: if it adds pairs
(paired after > paired before, even a fraction of one), their cost at the expected
VWAP + fees + the other side's average must be within the pair threshold (or the
operator's hedge allowance), else `PAIR_ECONOMICS_BAD`. A lower average never
excuses pairs above it.

Manager orders are **FOK** (fill-or-kill): the whole order at or under its limit,
or nothing. The manager only proposes a size the book can fill in full within its
price boundary.

Every buy passes two checks: the benefit, then the added capital and risk.
Its limits are spend, orders, side inventory and total exposure (filled + pending),
plus a cash reserve kept for the sniper. Prices come from the book walked to the
order's limit (expected VWAP, never the displayed best). A same-side
average-down needs the ask a further `POSITION_MANAGER_MIN_PRICE_IMPROVEMENT_BETWEEN_ENTRIES`
lower and waits out a cooldown, so a falling book is never laddered. Its thresholds
tighten as the round ends, and it stops `POSITION_MANAGER_MIN_LEFT_S` before.
Every non-trade has a reason (`IMPROVEMENT_TOO_SMALL`, `PAIR_ECONOMICS_BAD`,
`SNIPER_PRIORITY`, ...), shown on the dashboard and in
`<sniper journal>_manager.jsonl`.

**The sniper comes first.** Every rule is on the manager's side; the sniper's own
path is unchanged.
- **Its liquidity.** A manager buy must leave, within the sniper's own cap (best ask
  + `SNIPER_SLIP`), at least the sniper's largest target (`SNIPER_MAX_TARGET_SHARES`),
  else `SNIPER_LIQUIDITY_RESERVE`. So a manager fill that lands first cannot leave
  the sniper's next order short - in a book that holds still. Not preventable: an
  order already at the venue cannot be recalled, and the venue reserves no
  liquidity (other traders can take what was left).
- **Its instant.** Nothing is sent while a sniper order is in flight or within
  300ms of one. The send is one synchronous step that re-checks this and that the
  state the decision used has not changed (`SNIPER_PRIORITY`, `STATE_CHANGED`), and
  the worker checks once more right before the order leaves (`CANCELLED`, nothing sent).
- **Its event loop.** An evaluation captures its inputs in one short step, then
  computes one candidate at a time and lets the sniper's ticks run in between.
- In paper it has its own execution slot in the broker, so it can neither refuse
  nor delay a sniper order.

**Orders.** Each order carries its market, token, side, reservation id and order id
for good, and settles into its own market's history (a late result from an old
market never touches the new one). Settlement is idempotent. An order without an
outcome after `POSITION_MANAGER_ORDER_TIMEOUT_S` (5) is `UNKNOWN`: it may have
reached the venue, so its reservation stays counted and its side stays blocked -
no retry - until the outcome arrives. A fill is never counted as filled and
pending at once. The dashboard's manager averages include fees (`Avg+fees`); the
positions table's `AVG` is the price paid without fees.

**Live.** `POSITION_MANAGER_LIVE_ENABLED=1` refuses to start: real manager orders
wait for the replay results to be reviewed. With `--live` and the engine on, it is
shadow-only (decisions logged, no orders).

**A/B replay.** `python replay_ab.py --data <recordings>` replays every recorded
round twice: the sniper alone, then the sniper + manager. Both get identical events,
each round settles on its venue result, and the output includes the manager's
capital, its attributable P/L, the P/L per extra dollar, the extra drawdown and
exposure, and any sniper order that differs. Record with
`python -m latency_lab.recorder` (public data only). The replay triggers from the
frozen spec's feed (Coinbase, the recorder's `xb` rows); `--feed binance` replays
the Binance trigger instead. The recorder keeps a Coinbase ticker only when its
best bid or ask changed, so the replay's rule restarts after quiet spells a
little more often than live.

## Settlement

Every market the bot sees is tracked by its condition id, from discovery to its result:

```
ACTIVE -> EXPIRED -> PENDING SETTLEMENT -> SETTLING -> SETTLED
                         (LIVE, winning shares held) -> REDEEM PENDING -> REDEEMED
```

- **Only the venue's result settles.** A timer reaching zero moves a market to
  PENDING SETTLEMENT, nothing more; a BTC price never picks a winner. The result is
  the payout vector of the Conditional Tokens contract on Polygon (what a
  redemption actually pays), read about a minute after a round ends. The CLOB and
  Gamma APIs mirror it 5 to 13 minutes later; they are the fallback when the chain
  cannot be read (`SETTLE_ONCHAIN=0`, or the RPC is unreachable).
- **The next market is never held up.** Each market settles on its own; any number
  can be pending at once. Checks run concurrently on the settlement worker's own
  threads, never on the event loop or the order path's thread pool.
- **P/L moves once.** Settlement books the payout in the ledger exactly once (a
  repeated result, a second check, a restart: all no-ops). Until then, a position in
  an ended market is carried at cost, so equity stays defined and
  `cash + position value = equity`, `realized + unrealized = total P/L`.
- **Settled is not redeemed.** Paper credits the payout to cash at settlement. Live,
  winning shares are still outcome tokens in the wallet until they are redeemed for
  collateral (claim on polymarket.com); the dashboard shows REDEEM PENDING and
  REDEEMABLE until the wallet's on-chain balance proves REDEEMED.
- **Events.** `MARKET_EXPIRED`, `SETTLEMENT_PENDING`, `RESULT_RECEIVED`,
  `ACCOUNTING_SETTLED`, `REDEEM_PENDING`, `REDEEMED`, and on failure
  `SETTLEMENT_TIMEOUT`, `RESULT_API_ERROR`, `RESULT_NOT_FINAL`, `ACCOUNTING_ERROR`,
  `REDEEM_FAILED`. They show in the dashboard's events and are appended, with their
  timestamps, to `<ledger>_settlements.jsonl` beside the ledger.

## Run

```bash
.venv/bin/python run.py                     # paper (same as --paper)
.venv/bin/python run.py --paper --dash      # paper + terminal dashboard
.venv/bin/python run.py --live --dash       # live canary (needs SNIPER_ONLY=1 + SNIPER_LIVE_CANARY=1)
.venv/bin/python run.py --health            # public feeds for 30s as JSON; no wallet, no trading
.venv/bin/python run.py --paper-balance 500 # starting paper cash (first launch of a paper ledger only)
```

- **Stopping.** **Ctrl+C** or **SIGTERM** (`kill <pid>`) stops the bot. A second Ctrl+C forces it.
- **Shutdown order.** These steps happen in sequence:
  1. Live: the kill switch trips, so no new orders go out.
  2. The sniper stops.
  3. The live sender stops.
  4. The reconciler, settlement and feeds stop.
  5. The order observer is removed.
  6. Late fills are booked into the ledger.
  7. A final JSON report is printed, including the sniper's summary.
- **Exit log.** Every exit appends its reason to `bot_exit.log`. Ctrl+C exits with
  code 130, SIGTERM with 143, and a refused start (bad settings, another bot
  running) with 1.
- **Without `--dash`.** A one-line status is printed every 60s, safe to redirect to a log.

**Refusals.** The bot will not start when:

- `SNIPER_ONLY=1` is missing.
- `--live` is given without `SNIPER_LIVE_CANARY=1`.
- The trigger is Kraken under `--live` without `SNIPER_KRAKEN_LIVE=1`.
- `--live` is given and any setting differs from the frozen spec (`exec_latency.FROZEN_SPEC`), including a trigger feed other than `coinbase`.
- `--live` is given and the old bot in `/opt/btcbot` holds its lock file.
  - Paper refuses in this case too, unless `ALLOW_OTHER_BOT=1`.
  - The lock files checked are set by `OTHER_BOT_LOCKS`.
- Another instance of this bot already holds the same lock: `.btc_bot_paper.lock` or `.btc_bot_live.lock`.

The dashboard shows:

- **Mode.** PAPER or LIVE.
- **Trigger.** Feed and status, BTC mid, the move over the window, the last signal, signals per minute, orders, fills, misses and markouts.
- **Market.** The current market, seconds left, and UP/DOWN best bid and ask with book status.
- **This round.** Fills and shares per side, against the caps.
- **Orders.** The latest orders: side, BTC move, ask seen, limit, result, and shares@price.
- **Account.** Cash (live: collateral and REDEEMABLE), position value, equity, realized, unrealized and total P/L.
- **Market status.** In the header: ACTIVE, and how many markets are PENDING SETTLEMENT.
- **Position.** SIDE, SHARES, AVG, COST, BID, VALUE and P/L in the market trading now, and what ended markets still hold at cost.
- **Settlement.** The oldest market still waiting (status, PENDING AGE, what it waits on), or one just settled (WINNER, SETTLE PRICE, REALIZED P/L, SETTLE TIME, REDEEM), and the last settled market.
- **Markets.** The active market, every market pending settlement with its age, and the latest settled ones.
- **Live canary.** Limits used, the latency guard, and every HALT reason.
- **Feed health.** The trigger feed (Coinbase or Binance freshness, or Kraken lag), Polymarket book, user channel, settlement, ledger, clock and loop lag.
- **Skips and events.** The skip reasons, and recent events.

On the 1-vCPU VPS the dashboard is cheap: 2 frames/s (`DASH_FPS`), and frames are built off the event loop.

## Settings

All settings live in `.env`; `.env.example` lists them with the values in use. Wallet keys are listed by name only.

| setting | value | meaning |
| --- | --- | --- |
| `SNIPER_ONLY` | 1 | required: this build is the sniper only |
| `SNIPER_PRIMARY_FEED` | coinbase | trigger feed: `coinbase` (the frozen spec; LIVE refuses any other), `binance` or `kraken` |
| `SNIPER_COINBASE_FRESHNESS` | 1 (unset) | skip a Coinbase signal unless the feed is shown current: `_STALE_MS` 1000 (match age above its 5-min best), `_BACKLOG_MAX` 20, `_UNKNOWN_MS` 5000 |
| `SNIPER_COINBASE_SILENT_S` | 5 (unset) | no Coinbase message (heartbeats included) this long: reconnect |
| `SNIPER_K_BP` / `SNIPER_WINDOW_MS` | 0.5 / 250 | trigger: a move of >= 0.5bp within 250ms |
| `SNIPER_SHARES` | 10 | base target shares per order (at least $1) |
| `SNIPER_MAX_TARGET_SHARES` | 20 | cap on base + same-side unfilled carry |
| `SNIPER_ORDER_TYPE` | FAK | IOC limit: partial fills kept, the rest cancelled; FOK is all-or-nothing |
| `SNIPER_SLIP` | 0.05 | limit = the ask seen + 0.05, clipped at `MAX_BUY_PRICE`: never pays more |
| `SNIPER_FAIR` / `SNIPER_FAIR_MARGIN` | 1 / 0.01 | every buy at or under fair value − fee − margin (the limit capped there) |
| `SNIPER_FAIR_VOL_S` / `SNIPER_FAIR_FEE_RATE` | 300 / 0.07 | the volatility window (s) and the taker fee rate the fair limit uses |
| `SNIPER_COOLDOWN_MS` | 300 | per side: an order starts a pause on ITS side only, from its submission |
| `SNIPER_MIN_LEFT_S` | 10 | no order with fewer seconds left in the round |
| `SNIPER_MAX_FILLS_ROUND` / `SNIPER_MAX_SIDE_SHARES` | 20 / 200 | per-round caps |
| `MIN_BUY_PRICE` / `MAX_BUY_PRICE` | 0.05 / 0.95 | the asks the bot may buy |
| `TRADE_LAST_SECONDS` / `MIN_SECONDS_TO_EXPIRY` | 300 / 10 | the whole round, down to 10s left |
| `SNIPER_DELAY_SOURCE` / `SNIPER_DELAY_JOURNAL` | live / `data/live_delays_2026-10-04.jsonl` | paper delays drawn from >= 30 real live orders: the shipped copy of 238 canary orders (reply times only, no ids, prices or addresses) |
| `SNIPER_LIVE_CANARY` | 1 | `--live` sends real orders (with `SNIPER_ONLY=1`) |
| `ASSUMED_MATCH_DELAY_SECONDS` | 5 | Polymarket's taker delay; live refuses to start without it |
| `USER_WS` / `RECONCILE` | on / auto | live fills come from the private channel, with REST as backup |
| `SNIPER_CANARY_MAX_*` | 25 sh, 40 orders, 20 fills, 3 rounds, $60 spend, $10 loss | hard canary caps; any breach halts new orders |
| `SNIPER_KRAKEN_LIVE` | 0 | 1 = explicit opt-in to trade live on the Kraken trigger |
| `LEDGER_PATH` / `LIVE_SNIPER_JOURNAL` | live_ledger.json / live_sniper.jsonl | live state files |
| `PAPER_LEDGER_PATH` / `PAPER_ACCOUNT_PATH` / `PAPER_AUDIT_PATH` | paper_*.json(l) | paper state files; the sniper journal is `<audit>_sniper.jsonl` |
| `POSITION_MANAGER_ENABLED` / `_LIVE_ENABLED` | 0 / 0 | the Position Manager (paper orders; LIVE shadow-only) / real manager orders (refused for now) |
| `SNIPER_HOLD_MS` | 250 | paper's modelled taker hold; the venue's crypto delay is 150ms since 2026-09-04 |
| `SNIPER_SWITCH_MS` | unset (5ms) | Python's GIL switch interval: how long another thread (a dashboard frame) can hold up a tick |
| `SETTLE_ONCHAIN` | on (unset) | read results from the chain first; `0` = the CLOB + Gamma mirrors only (5-13 min later) |
| `POLYGON_RPC` | publicnode | the Polygon RPC for those reads (may embed an API key) |
| `SETTLE_RPC_TIMEOUT` / `SETTLE_API_EVERY_S` / `SETTLE_TIMEOUT_S` | 8 / 5 / 900 | one RPC read's limit; how often a market asks the mirrors when the chain cannot answer; when a pending market raises SETTLEMENT_TIMEOUT (it keeps being checked) |

`run.py` names, at start, any old main-strategy setting it ignores (`STOP_LOSS_ENABLED`, `BOOK_SOURCE`, ...).

A set `SNIPER_*` number that does not parse, or is NaN or infinite, stops the start and names the variable; so does a negative cooldown, minimum time left or book-wait, or a side cap or stale limit of 0. The `SNIPER_CANARY_MAX_*` caps must be finite numbers >= 0 (whole numbers for the counts). A bad value used to quietly become the default, and `nan` switched a canary cap off.

## Real-time paper: the sniper and the Position Manager on live markets

```bash
python paper_realtime.py run --markets 25      # supervisor; stops after 25 settled markets
python paper_realtime.py ctl restart           # clean restart (state restored from disk)
python paper_realtime.py ctl hard-restart      # crash test: killed, then restarted
python paper_realtime.py ctl stop
python paper_realtime.py report                # per-market table, P/L by source, invariant checks
```

The supervisor starts two things in their own folder (`~/sniperbtc-realtime`; never your usual paper state):

- `run.py --paper` with `POSITION_MANAGER_ENABLED=1`. Both the sniper and the manager trade the same paper account against the real Binance feed and real Polymarket books, rounds and results. Every fill is a **simulated paper fill**.
- The market recorder, so the same stream can be replayed later as sniper-only vs sniper + manager (`replay_ab.py --data <dir>/lab`).

Every 30 seconds it samples the bot's CPU, RAM, threads and handles, re-checks the paper invariants, and writes both to `monitor.jsonl`.

At start, paper prints `TRADING MODE: REAL-TIME PAPER ... LIVE ORDER EXECUTION: DISABLED ... REAL MONEY RISK: $0`. Every order-capable live entry point (18 of them) is replaced by a refusal that is counted; the final report shows the count.

Each paper fill's ledger source carries its strategy (`paper_live_book/sniper` or `/manager`). That gives exact per-source spend, fees and P/L, which the dashboard shows in `PAPER P/L BY SOURCE`.

To stop any bot cleanly without a console (on Windows a background process cannot receive CTRL_BREAK), create the file `<BOT_LOCK_PATH>.stop`. The bot deletes it and shuts down as on SIGTERM.

## Restart, ownership and durability

- **Ledger.** A fill or settlement appends one fsynced line to `<ledger>.journal`. A full snapshot is written every 500 entries, every 2 minutes in paper (every 60 seconds live), and at shutdown. A restart replays the journal, and a damaged one refuses to start. Booking a fill costs the same at any ledger size (0.7 ms; it was 188 ms at 50,000 positions).
- **One process per ledger and per wallet.** Two bots on one ledger file, or two live bots on one wallet on this machine, are refused. The wallet lock lives in `~/.sniperbtc/locks`. Locks cannot see another machine; there, live reconciliation is the safety net.
- **The live canary's budget survives restarts.** Orders, fills, spend, rounds, the loss window and halts are kept in `live_canary_state.json`. At start they are reconciled with the ledger's order authorizations, and an unreadable or missing file refuses the live start. `--new-canary-session` starts a new budget deliberately and keeps the old file as `.bak`.
- **Paper.** Round caps are rebuilt from the ledger. The manager's per-market limits are rebuilt from its journal. The carry restarts at 0, which is smaller, never larger.

**Share caps are estimates.** The sniper's IOC is signed in dollars (makerAmount, USDC). If the ask falls before the match, the same stake buys more shares than the target, so the share target, the round side cap and the canary's shares-per-order cap are checked at the decision's prices only. Dollar limits (stake, spend cap, cash) are hard. A fill above target is journalled as `over_target`.

## Daily checks (read-only; safe beside a running bot)

```bash
.venv/bin/python sniper_why.py           # is it trading, and if not, which check stops it (--live: the canary)
.venv/bin/python sniper_check.py         # paper plumbing + numbers vs the lab (fill rate, markouts)
.venv/bin/python sniper_deep.py          # where the edge comes from (--daily, --hourly, --gates, --live)
.venv/bin/python live_check.py           # the live canary: fills, prices paid, latency, halts vs paper
.venv/bin/python live_reconcile.py       # why the live ledger refused a fill (venue trades vs authorizations)
.venv/bin/python live_preflight.py       # before --live: settings, keys, clock, collateral; must say READY
.venv/bin/python kraken_race.py          # Kraken vs Binance: which tells this machine first
```

Research (read-only, heavy; run it beside the bot at low priority). It scores dynamic price limits and sizes against what the bot sends now, on the latency lab's recordings and the live canary's real orders:

```bash
LAB_DATA=/opt/lab_data nice -n 19 .venv/bin/python sniper_dynexec.py
```

## Run on a Windows PC (paper)

Paper runs fine on a PC; live belongs on the VPS (a home link is slow to the CLOB, and some ISPs hijack polymarket.com's DNS).

1. Make `.env` from `.env.example`, with three changes:
   - Nothing for the delays: `SNIPER_DELAY_SOURCE=live` reads the shipped `data/live_delays_2026-10-04.jsonl`.
   - `SNIPER_LIVE_CANARY=0`.
   - `PAPER_LEDGER_PATH`, `PAPER_ACCOUNT_PATH`, `PAPER_AUDIT_PATH` and `BOT_LOCK_PATH` pointing to a folder outside OneDrive. Its sync can lock a file at the moment the ledger replaces it.
2. Double-click `run_paper.cmd`, or run `python run.py --paper --dash` in Windows Terminal. Ctrl+C stops it.
3. Check it with the same daily tools: `python sniper_why.py`, `python sniper_check.py`.

If the start prints `[DNS] Polymarket name resolution is redirected`, set Windows' DNS to 1.1.1.1 or 8.8.8.8, or enable DNS-over-HTTPS. Otherwise discovery and every venue call fail.

## Install on the VPS (beside the old bot)

Run these as the user that `git pull`s `/opt/btcbot`; the deploy key belongs to that user.

```bash
git clone --branch sniperbtc --single-branch "$(git -C /opt/btcbot remote get-url origin)" /opt/sniperbtc
cd /opt/sniperbtc && ./setup.sh
```

If `/opt/btcbot`'s remote is not called `origin`, use `git -C /opt/btcbot remote -v` to find the URL.

`setup.sh` does the following:

- Updates the checkout.
- Makes `.venv` and installs `requirements.txt`.
- Copies `/opt/btcbot/.env` to `.env` and sets it to mode 600. It never prints a value.
- Re-points any setting that names `/opt/btcbot` to `/opt/sniperbtc`.
- Lists missing settings, by name only.
- Copies `/opt/btcbot/live_sniper.jsonl`, because paper delays need at least 30 live orders, and counts them.
- Reports whether the old bot is running.
- Leaves paper state behind: paper starts fresh here.

Re-running it is safe: it keeps `.env`, journals and ledgers.

**One bot per VPS.** Stop the old bot first:

- systemd: `sudo systemctl stop btcbot`, and `sudo systemctl disable btcbot` so it does not come back on boot.
- tmux: Ctrl+C in its window.

`run.py` refuses `--live` while the old bot's lock file is held.

```bash
cd /opt/sniperbtc
.venv/bin/python run_tests.py                                    # every suite passes, no network
.venv/bin/python run.py --health                                 # Binance + the round's book connect
tmux new -s sniper '.venv/bin/python -u run.py --paper --dash'   # let it trade an hour; detach: Ctrl+B D
.venv/bin/python sniper_why.py                                   # signals, orders, fills, why skipped
.venv/bin/python sniper_check.py --hours 1                       # 0 FAIL
```

**Live canary (3 markets, $10 loss cap).** Stop the paper bot first.

```bash
./setup.sh --with-live-ledger            # copies the old bot's live ledger; refuses while the old bot runs
.venv/bin/python live_preflight.py       # must end with VERDICT: READY
tmux new -s live '.venv/bin/python -u run.py --live --dash'
.venv/bin/python live_check.py           # afterwards: what the canary measured
```

The canary stops sending orders by itself when any cap is reached: `SNIPER_CANARY_MAX_ROUNDS=3` or `SNIPER_CANARY_MAX_LOSS_USD=10`. `touch STOP_LIVE` stops new orders at once. Remove the file and restart to trade again. From the first live run on, run LIVE only from `/opt/sniperbtc`: the old bot's ledger copy is frozen.

To run under systemd instead of tmux, copy `/etc/systemd/system/btcbot.service` to `sniperbtc.service`. In the copy, set `WorkingDirectory=/opt/sniperbtc`, set `ExecStart=/opt/sniperbtc/.venv/bin/python -u run.py --paper` (without `--dash`), and give it its own log file. `systemctl stop` sends SIGTERM, which is a clean shutdown.

## Tests

```bash
python run_tests.py              # every tests_*.py in its own process; exit 0 = all passed
python run_tests.py run feeds    # just tests_run.py and tests_feeds.py
```

- **No network, no wallet.** The suites never read `.env`: `testing_env.py` turns the load off for the test process and removes the bot's settings from the inherited environment.
- **Hash-locked trading files.** `tests_feeds.py` locks the trading files to their approved SHA-256. Any change to a trading file needs review, a new digest, and a comment beside it saying what changed.
- **The final audit (2026-10-07).** `tests_final_audit.py` checks every rule against expected values the test computes itself (Decimal/Fraction), never with the production function. `tests_stress.py` runs 10,000 concurrent order races and 10,000 full-system scenarios, then 600 market rotations for resource growth. That suite takes about 5 minutes; `STRESS_SCALE=0.1` runs a tenth. `bench_loop_costs.py` measures what the event loop pays as the ledger grows. `bench_execution.py --pm off|on|stress` compares the order path with the Position Manager off and on.
- **Regression tests carried over.**
  - b50cb59: a 400 FOK miss is NO_MATCH (`tests_sniper_recovery`).
  - 8abe70f: fill cost comes from the maker legs (`tests_fill_cost`).
  - d028e27: a Kraken heartbeat holds the price (`tests_sniper_kraken`).
  - 0eca592: paper sizing at the limit, and FAK partial depth (`tests_paper`).
  - 1036dae: the trigger reference is at most 2s old (`tests_sniper_recovery`).
  - 428d904: paper delays are drawn from live orders (`tests_paper_delay`).
  - 8e4468d: Binance freshness (`tests_binance_freshness`).
- **Regression tests written here.** spe24 had none for these two; they are in `tests_run.py`:
  - aa3adc7: a present STOP_LIVE makes preflight FAIL.
  - 7eedf06: a settings error names the bad value.
- **The Coinbase trigger (2026-10-10).** `tests_sniper_coinbase.py`: the venue's message shapes, the ack, out-of-order and malformed tickers, heartbeats that do not carry the price, the freshness gate, the socket loop against a local server (silence, starvation, errors, reconnects), and the replay. `tests_latency.py` checks the Coinbase-triggered sniper decides exactly as the old Binance one on the same mids.

## Keeping up with spe24

spe24 keeps moving. To take a later spe24 commit into this tree, run this on the dev PC from `sniperBTC`, with `spe8` beside it:

```bash
git -C ../spe8 diff --stat aaff811 HEAD -- $(git ls-files)         # what changed in the files carried here
git -C ../spe8 diff aaff811 HEAD -- $(git ls-files) | git apply -3  # take it; adapted tests may need a hand-merge
```

Then update the changed digests in `BASELINE_SHA` (`tests_feeds.py`), run `python run_tests.py`, commit with the spe24 hash in the message, and push.

## Layout

| path | role |
| --- | --- |
| `run.py` | the runner: gates, locks, feeds, paper broker or live canary, rotation, settlement, dashboard snapshot, shutdown |
| `sniper_mode.py` | the sniper: trigger, gates, sizing, paper execution, journal |
| `sniper_live.py`, `fast_h2.py` | live canary: lean signed sender on HTTP/2, limits, kill switch, reconciliation |
| `exec_latency.py` | T0..T7 latency, execution quality, the frozen spec and the effective-config check |
| `position_manager.py`, `replay_ab.py` | the optional Position Manager, and its A/B replay on recorded markets |
| `bench_execution.py`, `bench_network.py` | latency benchmarks: the live code path in-process, and this box's network to the venue |
| `paper_trade.py` | paper broker: fills against Polymarket's book at the modelled match instant |
| `polymarket_trade.py`, `orderbook.py`, `market_discovery.py` | venue client, book parsing, round discovery |
| `feeds/` | Polymarket market and user channels, book state, REST reconcile, supervision |
| `accounting/` | ledger, fees, resolution, settlement |
| `config.py`, `timer.py`, `http_pool.py`, `onchain_resolution.py` | settings, clock, HTTP, on-chain settlement |
| `dashboard/` | renderer, widgets and theme (from spe24), plus `sniper_layout.py` and `console.py` |
| `sniper_why.py` ... `kraken_race.py` | the daily read-only tools |
| `sniper_dynexec.py`, `sniper_rescore.py`, `latency_lab/load.py` | research: dynamic execution scored on the lab recordings (`LAB_DATA`) and live orders; needs numpy |
| `setup.sh`, `.env.example`, `requirements.txt` | VPS install |
