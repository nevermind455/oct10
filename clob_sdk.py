"""Bot integration surface for the pinned official unified Polymarket SDK.

The small compatibility objects keep strategy and durable accounting separate
from changes in the upstream client API. There is no legacy SDK fallback.
"""
from clob_client import AssetType, BalanceAllowanceParams, ClobClient, OpenOrderParams, TradeParams
from clob_signing import (
    ApiCreds, BUY, SELL, CreateOrderOptions, MarketOrderArgs, MarketOrderArgsV2,
    OrderArgsV2, OrderBuilder, OrderType, PartialCreateOrderOptions, RequestArgs,
    Side, Signer,
)
