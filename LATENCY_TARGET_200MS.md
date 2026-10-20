# Measuring the 200 ms acknowledgement target

This is a latency target, not a guarantee for every order. A submitted order
can receive no match, and network or exchange delays can exceed the target.
Changing the request timeout to 200 ms does not speed up a fill: an order can
still execute after the caller times out, leaving its outcome unknown.

The target is trigger -> venue response headers, strictly below 200 ms.
Order submission, a no-match response, a private provisional fill report and
confirmed settlement are separate measurements. A fast rejection is not a
successful fast fill.

## Read-only probe on the trading VPS

Use the same machine and network route that runs the bot:

```bash
python bench_network.py --clob-only --n 200 --target-ack-ms 200 \
    --in-process-ms 5 --server-ms 25 --json data/ack-probe.json
```

This sends public HTTP/2 GET /time requests only. It does not derive wallet
credentials or submit orders. The result is a planning scenario:

```
local trigger-to-dispatch allowance
+ full warm network RTT
+ venue taker-hold assumption
+ server-processing allowance
```

The default assumptions are 5 ms local, 150 ms hold (the documented crypto
market hold used by this code), and 25 ms server allowance. They leave a warm
RTT budget below 20 ms for this scenario. The probe uses the full return path
for an ACK. A one-way match estimate cannot substitute for it.

GET /time does not measure the matching engine's queue or per-order processing.
Passing this scenario does not establish a live 200 ms result; actual order
acknowledgements must establish that. Missing or failed samples remain unknown.

## Confirm the actual execution path

For optional warm-only dispatch, set:

```dotenv
SNIPER_HTTP=fast
SNIPER_WARM_ONLY=1
```

This requires the synchronous, already connected HTTP/2 path. Cold signals
are skipped while the existing background public probe reconnects. A skipped
signed order is never retried when the connection recovers. If the connection
disappears after its durable intent, a proven unsent order releases its pending
reservation; possible writes retain the existing unknown-outcome handling.
This avoids cold setup during dispatch at the cost of missed opportunities.
It does not accelerate already-warm orders or exchange matching. The option
defaults to off; `SNIPER_WARM_ONLY` accepts only `0` or `1`.

Exchange authentication timestamps are still required. They use the cached
server clock and do not cause a per-order GET /time request. Order-book
timestamps still establish freshness; missing metadata is repaired at its
source rather than replaced with a made-up exchange timestamp.

The live sender already selects FastH2 by default. `SNIPER_HTTP=httpx` selects
the fallback. The dependencies in requirements.lock.txt include the HTTP/2
client and fast signing backend. Inspect the existing backend report with:

```bash
python -c 'import exec_latency; print(exec_latency.signing_backend())'
```

The unified SDK migration replaces the old custom signing implementation.
Benchmark its local timing again in the installed dependency environment;
earlier signing measurements do not establish the updated path's performance.
See `SDK_UPGRADE_2026-10-19.md` for installation and validation limits.

New `live_send` rows record:

- `http_transport`: actual client class, without credentials or headers.
- `dispatch_mode`: synchronous warmed write or asynchronous path.
- `http_version`: protocol reported by the response, when available.
- `ts_us.intent_start` and `intent_end`: durable-intent elapsed time, on the
  same clock as the existing send stamps.

Durable intent remains before the possible network write. This telemetry
does not remove persistence, alter retry policy, change order limits, or turn
the 200 ms goal into an order timeout.

Clock refresh and connection warming validate the HTTP status and timestamp
before trusting a public time response. An error response cannot refresh the
signing clock. This prevents invalid authentication timestamps; it does not
reduce the exchange's processing time.

Evaluate both the fraction of acknowledgements below 200 ms and the fraction
of eventually confirmed filled orders meeting that target. Keep no-match,
unknown and simulated paper outcomes separate. Report percentile latency and
sample counts; public price/size coincidences do not identify our match time.
