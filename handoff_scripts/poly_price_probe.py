"""Read-only public-price diagnostic; never loads .env or wallet credentials.

Run on the bot host: .venv/bin/python -I handoff_scripts/poly_price_probe.py
Prints a short report from Gamma and the public TWAP websocket, then exits.
Uses verified TLS and the bot's existing websockets dependency. No orders.
"""
import asyncio
import contextlib
import hashlib
import json
from pathlib import Path
import ssl
import time
from urllib.request import urlopen

import websockets


def gamma():
    start = int(time.time()) // 300 * 300
    for boundary in (start, start - 300):
        slug = f"btc-updown-5m-{boundary}"
        try:
            with urlopen(f"https://gamma-api.polymarket.com/events/slug/{slug}", timeout=8) as response:
                event = json.load(response)
            meta = event.get("eventMetadata")
            if isinstance(meta, str):
                meta = json.loads(meta)
            meta = meta if isinstance(meta, dict) else {}
            markets = event.get("markets") or []
            if isinstance(markets, str):
                markets = json.loads(markets)
            configs = [m.get("cryptoMarketConfig") for m in markets if isinstance(m, dict)]
            print("GAMMA", json.dumps({"slug": event.get("slug"), "expected_slug": slug,
                  "priceToBeat": meta.get("priceToBeat"), "finalPrice": meta.get("finalPrice"),
                  "metadata_keys": sorted(meta), "configs": configs})[:1600], flush=True)
        except Exception as exc:
            print("GAMMA", slug, "read failed:", type(exc).__name__, flush=True)


async def public_stream():
    heartbeat = None
    frames = btc = 0
    try:
        async with websockets.connect("wss://ws-live-data.polymarket.com", ssl=ssl.create_default_context(),
                                      open_timeout=8, close_timeout=2, max_size=262144) as ws:
            sub = {"action": "subscribe", "subscriptions": [
                {"topic": "crypto_prices_twap_sixty", "type": "update"}]}
            await ws.send(json.dumps(sub))
            print("RTDS connected; SDK update subscription sent", flush=True)

            async def ping():
                while True:
                    await asyncio.sleep(5)
                    await ws.send("PING")

            heartbeat = asyncio.create_task(ping())
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline and btc < 3:
                try:
                    raw = await asyncio.wait_for(ws.recv(), min(5, max(0.01, deadline - time.monotonic())))
                except asyncio.TimeoutError:
                    continue
                if raw in ("PING", "PONG", "ping", "pong", ""):
                    if raw in ("PING", "ping"):
                        await ws.send("PONG" if raw == "PING" else "pong")
                    continue
                frames += 1
                message = json.loads(raw)
                if not isinstance(message, dict):
                    continue
                payload = message.get("payload")
                payload = payload if isinstance(payload, dict) else {}
                if message.get("type") == "error" or message.get("op") == "error":
                    print("RTDS rejected:", json.dumps({"code": message.get("code") or payload.get("code")}), flush=True)
                    break
                if payload.get("symbol") == "btc/usd":
                    btc += 1
                    fields = {k: payload.get(k) for k in ("symbol", "timestamp", "window_s", "value", "full_accuracy_value")}
                    print("RTDS BTC", json.dumps(fields), flush=True)
    except Exception as exc:
        print("RTDS connection/read failed:", type(exc).__name__, flush=True)
    finally:
        if heartbeat:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await heartbeat
    print(f"RTDS totals: {frames} frames, {btc} BTC reports", flush=True)


async def main():
    reader = Path(__file__).resolve().parents[1] / "polymarket_prices.py"
    if reader.exists():
        digest = hashlib.sha256(reader.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        print("READER", digest[:12], flush=True)
    await asyncio.gather(public_stream(), asyncio.to_thread(gamma))


if __name__ == "__main__":
    asyncio.run(main())
