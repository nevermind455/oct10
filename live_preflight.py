"""Checks before the sniper's LIVE CANARY - run on the VPS with the canary
wallet's keys in .env, before the first `run.py --live`:

    .venv/bin/python live_preflight.py
        places NO order: settings, key/funder/credentials, the order
        connection (HTTP/2, clock), collateral, this round's rules, and an
        unauthenticated POST /order (throwaway key + credentials) that the
        venue must refuse with 401 - proof the request shape gets through.

    .venv/bin/python live_preflight.py --dry-fire --confirm-real-order
    .venv/bin/python live_preflight.py --dry-fire --fak --confirm-real-order
        the same one real $1 order at the minimum tick, sent as FILL-AND-KILL:
        shows Polymarket's exact reply to an unfilled FAK (raw), which the
        canary must recognise as a miss before FAK can trade live.
        also ONE real order that should not fill: FOK BUY $1 at the minimum
        tick, on a side whose best ask is >= 0.10, with more than 60s left.
        Proves the whole chain (key, funder, L2 credentials, order version)
        end to end. It is a real order on the real wallet: if the book moved
        to the minimum tick in that instant it buys $1 of that side.

Prints PASS / WARN / FAIL per check and a verdict. Never prints a key or a
credential; the balance is printed (it is yours, on your terminal).
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import secrets
import sys
import tempfile
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RESULTS: list[tuple[str, str, str]] = []


def mark(level: str, name: str, detail: str = "") -> None:
    RESULTS.append((level, name, detail))
    print(f"  {level:4s} {name}" + (f" - {detail}" if detail else ""), flush=True)


def check_settings(config) -> None:
    import sniper_live
    import sniper_mode
    print("\n1. SETTINGS (.env)")
    mark("PASS" if sniper_mode.only() else "FAIL", "SNIPER_ONLY=1", "" if sniper_mode.only() else "not set")
    mark("PASS" if sniper_mode.live_canary() else "FAIL", "SNIPER_LIVE_CANARY=1",
         "" if sniper_mode.live_canary() else "not set: run.py --live refuses SNIPER_ONLY without it")
    user_ws = (os.environ.get("USER_WS") or "on").lower()
    mark("PASS" if user_ws == "on" else "FAIL", "USER_WS=on (fills arrive on the private channel)", user_ws)
    delay = float(config.ASSUMED_MATCH_DELAY_SECONDS)
    st = sniper_mode.Settings()
    mark("PASS" if 0 < delay < st.min_left_s else "FAIL",
         f"ASSUMED_MATCH_DELAY_SECONDS={delay:g} (> 0, < SNIPER_MIN_LEFT_S={st.min_left_s:g})")
    key = bool(os.environ.get("POLY_PRIVATE_KEY") or os.environ.get("PRIVATE_KEY"))
    mark("PASS" if key else "FAIL", "POLY_PRIVATE_KEY present (value not shown)")
    mark("PASS" if float(config.MIN_SECONDS_TO_EXPIRY) <= st.min_left_s else "WARN",
         f"MIN_SECONDS_TO_EXPIRY={float(config.MIN_SECONDS_TO_EXPIRY):g} (paper ran with {st.min_left_s:g})")
    mark("PASS" if int(config.TRADE_LAST_SECONDS) >= 300 else "WARN",
         f"TRADE_LAST_SECONDS={config.TRADE_LAST_SECONDS} (the whole round is 300)")
    band = (float(config.MIN_BUY_PRICE), float(config.MAX_BUY_PRICE))
    mark("PASS" if band == (0.05, 0.95) else "WARN", f"price band {band[0]:g}-{band[1]:g} (paper: 0.05-0.95)")
    lim = sniper_live.limits_from_env()
    mark("INFO", "canary limits", f"{lim.max_order_shares:g} sh/order, {lim.max_orders} orders, {lim.max_fills} fills, "
                                  f"{lim.max_rounds} rounds, ${lim.max_exposure_usd:g} total spend, "
                                  f"${lim.max_daily_loss_usd:g} realized loss")
    stop = os.environ.get("SNIPER_CANARY_STOP_FILE") or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                       "STOP_LIVE")
    if os.path.exists(stop):
        # The emergency brake is ON: the canary refuses every order while it exists (it once read as a mere
        # warning - "WARN no stop file" - and a live start then halted on it within seconds).
        mark("FAIL", "stop file present - every live order will be refused", f"remove it to trade: rm {stop}")
    else:
        mark("PASS", "no stop file (touch STOP_LIVE stops new orders at once)")


async def run(args) -> int:
    import config
    import market_discovery
    import sniper_live as L
    from accounting import Ledger
    check_settings(config)
    if any(level == "FAIL" for level, _n, _d in RESULTS) and not args.force:
        print("\nsettings fail: fix .env first (or --force to run the venue checks anyway)")
        return 1

    print("\n2. WALLET + CLIENT (derives the L2 credentials; nothing is printed)")
    tmp = tempfile.TemporaryDirectory()
    ledger = Ledger(path=os.path.join(tmp.name, "preflight_ledger.json"))   # never the bot's ledger
    try:
        ex = await asyncio.to_thread(L.build_live_execution, ledger=ledger)
    except Exception as exc:  # noqa: BLE001 - reported, nothing else can run
        mark("FAIL", "build the canary on the audited live client", f"{type(exc).__name__}: {exc}"[:200])
        return 1
    mark("PASS", "client ready; the canary's signer matches the live client's signer",
         f"signature type {ex.sender.builder.signature_type}, funder {'set' if config.POLY_FUNDER else 'signer'}")
    http, clock = ex.sender.http, ex.sender.clock
    try:
        print("\n3. ORDER CONNECTION")
        r = await http.get(ex.clock_url)
        mark("PASS" if r.http_version == "HTTP/2" else "WARN", f"protocol {r.http_version}")
        ok = await clock.refresh(http, ex.clock_url)
        cs = clock.status()
        mark("PASS" if ok else "FAIL", "server clock synced on the order connection",
             f"offset {cs['offset_s']:+.2f}s, rtt {cs['rtt_ms_p50']:.0f}ms" if ok else "")

        print("\n4. COLLATERAL")
        await ex._read_balance()
        if ex.balance is None:
            mark("FAIL", "balance/allowance read")
        else:
            lim = ex.sender.limits
            mark("PASS" if ex.balance >= 5 else "FAIL", f"spendable ${ex.balance:.2f} (min of balance and allowance)")
            mark("PASS" if ex.balance >= lim.max_exposure_usd else "WARN",
                 f"covers the ${lim.max_exposure_usd:g} spend cap" if ex.balance >= lim.max_exposure_usd
                 else f"below the ${lim.max_exposure_usd:g} spend cap: orders are refused once it runs out")

        print("\n5. THIS ROUND")
        rnd = await asyncio.to_thread(market_discovery.get_tokens_for_current_round)
        if not rnd:
            mark("FAIL", "current BTC 5m round", "discovery returned nothing (between rounds? rerun)")
            return 1
        cond, up, down, end = rnd["condition_id"], rnd["up_token_id"], rnd["down_token_id"], rnd["window_end"]
        try:
            rules = await ex.sender.rules.prefetch(cond, up, down)
            mark("PASS", "order rules through the audited validator",
                 f"tick {rules['tick']}, min {rules['minimum']} sh, fee {rules['fee_rate']}^{rules['fee_exponent']}, "
                 f"taker delay {rules['taker_delay']}, neg_risk {rules['neg_risk']}")
            paper = await asyncio.to_thread(ex.shim._rules, cond)
            same = str(paper.tick_size) == str(rules["tick"]) and float(paper.min_order_size) == float(rules["minimum"])
            mark("PASS" if same else "FAIL", "sizing rules agree with the order rules (tick, minimum)")
        except Exception as exc:  # noqa: BLE001
            mark("FAIL", "order rules", f"{type(exc).__name__}: {exc}"[:200])
            return 1

        print("\n6. POST /order SHAPE (throwaway key + credentials: the venue must refuse it)")
        from py_clob_client_v2.clob_types import ApiCreds
        from py_clob_client_v2.order_builder.builder import OrderBuilder
        ghost_signer = L.fast_signer("0x" + secrets.token_hex(32), config.CHAIN_ID)
        ghost = L.LeanSender(builder=OrderBuilder(ghost_signer), signer=ghost_signer,
                             creds=ApiCreds(api_key=str(uuid.uuid4()),
                                            api_secret=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
                                            api_passphrase=secrets.token_hex(16)),
                             host=config.CLOB_HOST, http=http, clock=clock, rules=ex.sender.rules,
                             limits=L.CanaryLimits(max_order_shares=1e9, max_rounds=10 ** 6),
                             kill=L.KillSwitch(None))
        tick = float(rules["tick"])
        try:
            gp = ghost.prepare(side="UP", token=up, up=up, down=down, condition=cond, end=end, amount=1.0, cap=tick)
            row = await ghost.send(gp, ghost.start(gp))      # the synchronous write the canary uses
        except (L.Refused, L.Halted) as exc:
            mark("FAIL", "could not build the probe order", str(exc)[:200])
            return 1
        reached = row["kind"] == L.REJECTED and row["http_status"] in (401, 403)
        mark("PASS" if reached else "FAIL", "unauthenticated order reached the venue intact and was refused",
             f"HTTP {row['http_status']} {row['kind']}: {str(row['error'])[:100]}; written "
             + ("in the caller's step (fast path)" if row.get("ts_us", {}).get("net_send_start") is not None
                and type(http).__name__ == "FastH2" else "the async way")
             + ("; NEW connection on the send" if row.get("new_connection") else ""))

        if args.dry_fire:
            otype = "FAK" if args.fak else "FOK"
            print(f"\n7. DRY FIRE - one real {otype} BUY that should not fill")
            if not args.confirm_real_order:
                mark("FAIL", "--dry-fire needs --confirm-real-order as well")
                return 1
            if end - time.time() < 60:
                mark("FAIL", "dry fire needs > 60s left in the round", f"{end - time.time():.0f}s left; rerun")
                return 1
            side = None
            for name, token in (("UP", up), ("DOWN", down)):
                b = (await http.get(f"{config.CLOB_HOST.rstrip('/')}/book", params={"token_id": token})).json()
                asks = [float(a["price"]) for a in b.get("asks") or []]
                if asks and min(asks) >= 0.10:
                    side = (name, token, min(asks))
                    break
            if side is None:
                mark("FAIL", "no side with a best ask >= 0.10 right now; rerun early in a round")
                return 1
            # Its own limits: exactly one $1 order ($1 at the minimum tick is
            # 100 shares, above the canary's per-order cap). The stop file
            # still applies.
            ex.sender.limits = L.CanaryLimits(max_order_shares=1e9, max_orders=1, max_fills=1, max_rounds=1,
                                              max_exposure_usd=1.0, max_daily_loss_usd=1.0)
            try:
                p = ex.sender.prepare(side=side[0], token=side[1], up=up, down=down, condition=cond, end=end,
                                      amount=1.0, cap=tick, order_type=otype)
            except (L.Refused, L.Halted) as exc:
                mark("FAIL", "dry-fire order not sent", str(exc)[:200])
                return 1
            row = await ex.sender.send(p, ex.sender.start(p))
            note = f"{side[0]} best ask {side[2]:.2f}, limit {p.price:g}: HTTP {row['http_status']} {row['kind']}" \
                   f" ({str(row['error'] or row.get('venue_status'))[:100]})"
            print(f"  raw venue reply ({otype}): {json.dumps(row.get('response'), default=str)[:400]}")
            if row["kind"] == L.NO_MATCH:
                mark("PASS", f"real order accepted by the venue's checks and killed unfilled ({otype}); "
                             f"the canary reads this reply as a miss", note)
            elif row["kind"] == L.PENDING:
                status = None
                for _ in range(6):
                    await asyncio.sleep(2)
                    try:
                        status = await asyncio.to_thread(lambda: ex.order_status(row["order_id"]))
                    except Exception as exc:  # noqa: BLE001
                        status = {"error": type(exc).__name__}
                mark("PASS" if str((status or {}).get("status", "")).lower() not in ("matched",) else "WARN",
                     "real order accepted (delayed) - final venue status", f"{note}; {(status or {}).get('status')}")
            elif row["kind"] == L.MATCHED:
                mark("WARN", "the dry-fire order FILLED ($1 spent; the bot's ledger does not know this order)", note)
            else:
                mark("FAIL", "real order refused - the canary would be refused the same way", note)
    finally:
        await http.aclose()
        tmp.cleanup()

    fails = [n for level, n, _d in RESULTS if level == "FAIL"]
    warns = [n for level, n, _d in RESULTS if level == "WARN"]
    print("\nVERDICT:", "NOT READY - " + "; ".join(fails) if fails else
          ("READY" + (f" (warnings: {'; '.join(warns)})" if warns else "")))
    if not fails:
        print("start the canary (stop the paper bot first - one bot per vCPU):")
        print("    tmux new -s live '.venv/bin/python -u run.py --live --dash'")
        print("stop new orders at once:  touch STOP_LIVE   (restart to trade again after removing it)")
    return 1 if fails else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-fire", action="store_true", help="also send ONE real FOK order that should not fill")
    ap.add_argument("--confirm-real-order", action="store_true", help="required with --dry-fire")
    ap.add_argument("--fak", action="store_true", help="with --dry-fire: send it as fill-and-kill")
    ap.add_argument("--force", action="store_true", help="run the venue checks even if a setting fails")
    return asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
