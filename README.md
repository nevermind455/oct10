# BTC sniper (sniperBTC)

A Polymarket bot that trades one thing: Polymarket's 5-minute **btc-updown-5m**
markets, the instant BTC moves. It runs in **paper** (simulated fills on the real book, the default) or as a capped **live canary** (real orders under hard limits).

This repo is carved out of `spe24` at **b314d70**. The trading engine was copied
byte for byte, with no rewrite. What is new: `run.py` (the runner, which
replaces `run_feeds.py`'s `SNIPER_ONLY` path), the sniper dashboard, `setup.sh`
and the tests for those. The old main strategy (`main_bot.py`, `strategy.py`,
`feeds/adapters.py`, Chainlink, research tools) is not here.

## The strategy

- **Trigger.** The BTC mid moves at least `SNIPER_K_BP` within `SNIPER_WINDOW_MS`.
  The mid comes from Binance BTCUSDT bookTicker by default, or Kraken BTC/USD.
  The move is measured against the last price at or before the lookback, and
  that reference must be no more than 2s old. Kraken heartbeats carry the price
  forward. A move up buys **UP**; a move down buys **DOWN**, in the current round.
- **Gates.** An order only goes out if all of these hold:
  - Cooldown: only a sent order starts it.
  - At most one order in flight.
  - The round window allows it, with at least `SNIPER_MIN_LEFT_S` seconds left.
  - The book is LIVE and fresh, and has asks on the chosen side.
  - The ask is between `MIN_BUY_PRICE` and `MAX_BUY_PRICE`.
  - The per-round caps (`SNIPER_MAX_FILLS_ROUND`, `SNIPER_MAX_SIDE_SHARES`) are not reached.
  - The Binance freshness gate passes: the feed is not behind, has no backlog,
    and its age is known.
- **Order.** FOK or FAK for 10 shares at the ask. The stake is at least $1, and
  at least the venue minimum shares x the limit price. The limit is ask +
  `SNIPER_SLIP`, clipped at `MAX_BUY_PRICE`.
- **Paper fills.** Paper fills against Polymarket's book at the modelled match
  instant. With `SNIPER_DELAY_SOURCE=live`, each order's delay is drawn from real
  live orders.
- **Exit.** Every position is held to settlement.

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
- **Account.** Cash and P&L (the paper account), or collateral and realized P&L (the live ledger).
- **Live canary.** Limits used, the latency guard, and every HALT reason.
- **Feed health.** Binance freshness or Kraken lag, Polymarket book, user channel, settlement, ledger, clock and loop lag.
- **Skips and events.** The skip reasons, and recent events.

On the 1-vCPU VPS the dashboard is cheap: 2 frames/s (`DASH_FPS`), and frames are built off the event loop.

## Settings

All settings live in `.env`; `.env.example` lists them with the values in use. Wallet keys are listed by name only.

| setting | value | meaning |
| --- | --- | --- |
| `SNIPER_ONLY` | 1 | required: this build is the sniper only |
| `SNIPER_PRIMARY_FEED` | binance | trigger feed: `binance` or `kraken` |
| `SNIPER_K_BP` / `SNIPER_WINDOW_MS` | 0.8 / 250 | trigger: a move of >= 0.8bp within 250ms |
| `SNIPER_ORDER_TYPE` | FAK | FAK keeps partial fills; FOK is all-or-nothing |
| `SNIPER_SLIP` | 0.45 | limit = ask + 0.45, clipped at `MAX_BUY_PRICE` |
| `SNIPER_COOLDOWN_MS` | 1000 | the pause after each sent order |
| `SNIPER_MIN_LEFT_S` | 10 | no order with fewer seconds left in the round |
| `SNIPER_MAX_FILLS_ROUND` / `SNIPER_MAX_SIDE_SHARES` | 20 / 200 | per-round caps |
| `MIN_BUY_PRICE` / `MAX_BUY_PRICE` | 0.05 / 0.95 | the asks the bot may buy |
| `TRADE_LAST_SECONDS` / `MIN_SECONDS_TO_EXPIRY` | 300 / 10 | the whole round, down to 10s left |
| `SNIPER_DELAY_SOURCE` | live | paper delays drawn from >= 30 real live orders in `live_sniper.jsonl` |
| `SNIPER_LIVE_CANARY` | 1 | `--live` sends real orders (with `SNIPER_ONLY=1`) |
| `ASSUMED_MATCH_DELAY_SECONDS` | 5 | Polymarket's taker delay; live refuses to start without it |
| `USER_WS` / `RECONCILE` | on / auto | live fills come from the private channel, with REST as backup |
| `SNIPER_CANARY_MAX_*` | 25 sh, 40 orders, 20 fills, 3 rounds, $60 spend, $10 loss | hard canary caps; any breach halts new orders |
| `SNIPER_KRAKEN_LIVE` | 0 | 1 = explicit opt-in to trade live on the Kraken trigger |
| `LEDGER_PATH` / `LIVE_SNIPER_JOURNAL` | live_ledger.json / live_sniper.jsonl | live state files |
| `PAPER_LEDGER_PATH` / `PAPER_ACCOUNT_PATH` / `PAPER_AUDIT_PATH` | paper_*.json(l) | paper state files; the sniper journal is `<audit>_sniper.jsonl` |

`run.py` names, at start, any old main-strategy setting it ignores (`STOP_LOSS_ENABLED`, `BOOK_SOURCE`, ...).

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
- **Hash-locked trading files.** `tests_feeds.py` locks the trading files to their approved SHA-256. The engine is byte-identical to spe24 b314d70. Any change to a trading file needs review and a new digest.
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

## Layout

| path | role |
| --- | --- |
| `run.py` | the runner: gates, locks, feeds, paper broker or live canary, rotation, settlement, dashboard snapshot, shutdown |
| `sniper_mode.py` | the sniper: trigger, gates, sizing, paper execution, journal |
| `sniper_live.py`, `fast_h2.py` | live canary: lean signed sender on HTTP/2, limits, kill switch, reconciliation |
| `paper_trade.py` | paper broker: fills against Polymarket's book at the modelled match instant |
| `polymarket_trade.py`, `orderbook.py`, `market_discovery.py` | venue client, book parsing, round discovery |
| `feeds/` | Polymarket market and user channels, book state, REST reconcile, supervision |
| `accounting/` | ledger, fees, resolution, settlement |
| `config.py`, `timer.py`, `http_pool.py`, `onchain_resolution.py` | settings, clock, HTTP, on-chain settlement |
| `dashboard/` | renderer, widgets and theme (from spe24), plus `sniper_layout.py` and `console.py` |
| `sniper_why.py` ... `kraken_race.py` | the daily read-only tools |
| `setup.sh`, `.env.example`, `requirements.txt` | VPS install |
