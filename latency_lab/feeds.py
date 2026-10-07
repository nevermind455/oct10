"""Other exchanges' BTC top of book, for the feed race: does any price feed tell
the VPS about a BTC move before Binance does - or closer to when Polymarket
reprices?

Binance's engine is in Tokyo, ~120ms from Amsterdam. Deribit (London) and
Bitstamp (Luxembourg) are a few ms away, Coinbase and Kraken ~40ms. A move that
starts in Tokyo cannot reach Amsterdam faster than physics allows from ANY of
them, but a move that starts in the US or Europe may show up on a nearby venue
first. The race measures which, how often and whether it pays.

Pure parsing and the subscription table only - no sockets here, so the loader
can import it without websockets.
"""
from __future__ import annotations

from datetime import datetime

# Fixed order: the recorder writes the index, the loader maps it back.
NAMES = ("coinbase", "kraken", "bitstamp", "deribit", "okx", "bybit")
CODE = {n: i for i, n in enumerate(NAMES)}

# name -> (url, subscribe message, application ping text or None, ping every s)
SUBS = {
    "coinbase": ("wss://ws-feed.exchange.coinbase.com",
                 {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}, None, 0),
    "kraken": ("wss://ws.kraken.com/v2",
               {"method": "subscribe",
                "params": {"channel": "ticker", "symbol": ["BTC/USD"], "event_trigger": "bbo"}}, None, 0),
    "bitstamp": ("wss://ws.bitstamp.net",
                 {"event": "bts:subscribe", "data": {"channel": "order_book_btcusd"}}, None, 0),
    "deribit": ("wss://www.deribit.com/ws/api/v2",
                {"jsonrpc": "2.0", "id": 1, "method": "public/subscribe",
                 "params": {"channels": ["quote.BTC-PERPETUAL"]}}, None, 0),
    "okx": ("wss://ws.okx.com:8443/ws/v5/public",
            {"op": "subscribe", "args": [{"channel": "bbo-tbt", "instId": "BTC-USDT"}]}, "ping", 20.0),
    "bybit": ("wss://stream.bybit.com/v5/public/spot",
              {"op": "subscribe", "args": ["orderbook.1.BTCUSDT"]}, '{"op":"ping"}', 20.0),
}


def _iso_ms(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp() * 1000.0
    except ValueError:
        return None


def parse(name: str, d):
    """One decoded message -> (exchange_ms or None, bid, ask), or None when the
    message carries no top of book (acks, heartbeats, pongs)."""
    if not isinstance(d, dict):
        return None
    if name == "coinbase":
        if d.get("type") != "ticker":
            return None
        return _iso_ms(d.get("time")), float(d["best_bid"]), float(d["best_ask"])
    if name == "kraken":
        if d.get("channel") != "ticker" or d.get("type") not in ("snapshot", "update"):
            return None
        x = (d.get("data") or [None])[0]
        return (_iso_ms(x.get("timestamp")), float(x["bid"]), float(x["ask"])) if x else None
    if name == "bitstamp":
        if d.get("event") != "data":
            return None
        x = d.get("data") or {}
        if not x.get("bids") or not x.get("asks"):
            return None
        ts = x.get("microtimestamp")
        return (int(ts) / 1000.0 if ts else None), float(x["bids"][0][0]), float(x["asks"][0][0])
    if name == "deribit":
        if d.get("method") != "subscription":
            return None
        x = (d.get("params") or {}).get("data") or {}
        if x.get("best_bid_price") is None or x.get("best_ask_price") is None:
            return None
        return x.get("timestamp"), float(x["best_bid_price"]), float(x["best_ask_price"])
    if name == "okx":
        if (d.get("arg") or {}).get("channel") != "bbo-tbt" or not d.get("data"):
            return None
        x = d["data"][0]
        if not x.get("bids") or not x.get("asks"):
            return None
        return float(x["ts"]), float(x["bids"][0][0]), float(x["asks"][0][0])
    if name == "bybit":
        if not str(d.get("topic") or "").startswith("orderbook.1."):
            return None
        x = d.get("data") or {}
        if not x.get("b") or not x.get("a"):
            return None
        return d.get("ts"), float(x["b"][0][0]), float(x["a"][0][0])
    return None
