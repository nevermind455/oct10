"""Resolution read from the Conditional Tokens contract on Polygon.

Measured on this machine, the ``ConditionResolution`` event fires **85 seconds**
after a five-minute round ends (four of five sampled rounds were exactly 85s,
so it is a scheduled job rather than a queue). Over the same window Gamma's
``umaResolutionStatus`` still read ``None`` at 8.6 minutes and the CLOB
``closed`` flag had not flipped at all; both only caught up around 13 minutes.
Those two APIs are mirrors that lag the chain by roughly ten minutes. This
reads the source they mirror. Measured again for sniperBTC (round ending
2026-10-07 11:55 UTC): on chain at 58s, the market socket's market_resolved
at 123s, Gamma resolved at 290s, the CLOB still unresolved at 328s.

On by default (SETTLE_ONCHAIN=0 turns it off): the contract's payout vector is
what a redemption actually pays, so it is the authoritative result, not a
prediction. The three reads go out as ONE JSON-RPC batch (one round trip,
measured 1-3s against 7-12s sequential from a slow link), falling back to
sequential calls on an RPC that refuses batches.

Index mapping, which is the dangerous part: ``payoutNumerators[i]`` belongs to
outcome slot ``i``. Verified empirically against 12 resolved rounds that slot
order matches Gamma's ``outcomes``/``clobTokenIds`` order, and against 9 rounds
that the CLOB ``tokens`` array carries the same order (both directions
represented, so it is not an artefact of one-sided data). Callers still pass
the token ids explicitly rather than letting this module guess them - getting
this backwards would silently invert every settlement.
"""
from __future__ import annotations

import os
import threading

CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
DEFAULT_RPC = "https://polygon-bor-rpc.publicnode.com"

# payoutDenominator(bytes32), payoutNumerators(bytes32, uint256), and the
# ERC-1155 balanceOf(address, uint256) the redemption check reads.
_ABI = [
    {"inputs": [{"name": "", "type": "bytes32"}],
     "name": "payoutDenominator",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "", "type": "bytes32"}, {"name": "", "type": "uint256"}],
     "name": "payoutNumerators",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
     "name": "balanceOf",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "parentCollectionId", "type": "bytes32"}, {"name": "conditionId", "type": "bytes32"},
                {"name": "indexSet", "type": "uint256"}],
     "name": "getCollectionId",
     "outputs": [{"name": "", "type": "bytes32"}],
     "stateMutability": "view", "type": "function"},
]
# The collateral a market's outcome tokens were minted against. Proved on chain
# 2026-10-08 for Polymarket's BTC 5-minute markets: USDC.e. py_clob_client_v2's
# Polygon config names pUSD for V2. Both are tried; only an exact match of BOTH
# tokens counts (SETTLE_COLLATERALS, comma-separated, replaces the list).
COLLATERALS = ("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",     # USDC.e
               "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")     # pUSD (SDK V2 config)
# Outcome tokens use the collateral's 6 decimals: 1 share = 1_000_000 units.
SHARE_UNITS = 1_000_000

_lock = threading.Lock()
_contract = None
_last_error: str | None = None


def enabled() -> bool:
    """On unless SETTLE_ONCHAIN is 0/false/no/off. Read per call, never
    cached: config.py loads .env after import."""
    return (os.environ.get("SETTLE_ONCHAIN", "") or "").strip().lower() not in (
        "0", "false", "no", "off")


def rpc_timeout() -> float:
    """Seconds one RPC request may take (SETTLE_RPC_TIMEOUT, default 8)."""
    try:
        value = float(os.environ.get("SETTLE_RPC_TIMEOUT", "") or 8.0)
    except ValueError:
        return 8.0
    return value if 0.5 <= value <= 60 else 8.0


def rpc_url() -> str:
    configured = (os.environ.get("POLYGON_RPC", "") or "").strip()
    return configured or DEFAULT_RPC


def last_error() -> str | None:
    return _last_error


def _client():
    """Build the contract handle once, or report why it could not be built.

    Polygon needs the PoA middleware: without it every ``get_block`` raises
    ExtraDataLengthError because Bor's extraData is 105 bytes. Note that some
    public RPCs are unusable here - polygon-rpc.com and rpc.ankr.com both
    resolve to the same hijacked address on this network and answer 401.
    """
    global _contract, _last_error
    with _lock:
        if _contract is not None:
            return _contract
        try:
            from web3 import Web3
            try:
                from web3.middleware import ExtraDataToPOAMiddleware as poa
            except ImportError:                      # web3 < 7
                from web3.middleware import geth_poa_middleware as poa
            w3 = Web3(Web3.HTTPProvider(rpc_url(), request_kwargs={"timeout": rpc_timeout()}))
            w3.middleware_onion.inject(poa, layer=0)
            _contract = w3.eth.contract(
                address=Web3.to_checksum_address(CTF_ADDRESS), abi=_ABI)
            _last_error = None
        except Exception as exc:
            _contract = None
            _last_error = f"{type(exc).__name__}: {exc}"[:160]
        return _contract


def reset() -> None:
    """Drop the cached client so the next call rebuilds it."""
    global _contract
    with _lock:
        _contract = None


def payouts_for(condition_id: str, token_ids) -> tuple[dict | None, str]:
    """Final payout per token id, or ``(None, reason)`` when not resolved.

    ``token_ids`` must be in outcome-slot order - index 0 is slot 0. Returns
    ``{token_id: 1.0 or 0.0}``. Anything unverifiable returns None: an
    unreachable RPC, a denominator of zero (not resolved yet), a shape that is
    not binary, or numerators that do not form a clean single-winner split.
    A 50/50 UMA resolution is reported as 0.5/0.5, matching the API parsers.
    """
    cid = str(condition_id or "")
    if not cid.startswith("0x") or len(cid) != 66:
        return None, "invalid condition id"
    tokens = [str(t or "") for t in (token_ids or [])]
    if len(tokens) != 2 or not all(tokens) or tokens[0] == tokens[1]:
        return None, "token ids are not a distinct binary pair"
    contract = _client()
    if contract is None:
        return None, f"polygon rpc unavailable ({_last_error})"
    try:
        key = bytes.fromhex(cid[2:])
        denominator, numerators = _read_payouts(contract, key)
        if denominator <= 0:
            return None, "not resolved on chain yet"
    except Exception as exc:
        return None, f"chain read failed: {type(exc).__name__}"
    if sum(numerators) != denominator:
        return None, (f"numerators {numerators} do not sum to denominator "
                      f"{denominator}")
    shares = [n / denominator for n in numerators]
    if all(abs(v - 0.5) <= 1e-9 for v in shares):
        return {tokens[0]: 0.5, tokens[1]: 0.5}, "on-chain 50/50 resolution"
    if sorted(shares) != [0.0, 1.0]:
        return None, f"payout split {shares} is neither 1/0 nor 50/50"
    return ({tokens[0]: shares[0], tokens[1]: shares[1]},
            "on-chain ConditionResolution")


def _read_payouts(contract, key: bytes) -> tuple[int, list[int]]:
    """(payoutDenominator, [payoutNumerators[0], [1]]) in one batch request,
    or three sequential calls when the RPC (or web3) cannot batch."""
    w3 = getattr(contract, "w3", None)
    batch_requests = getattr(w3, "batch_requests", None)
    if batch_requests is not None:
        try:
            with batch_requests() as batch:
                batch.add(contract.functions.payoutDenominator(key))
                batch.add(contract.functions.payoutNumerators(key, 0))
                batch.add(contract.functions.payoutNumerators(key, 1))
                values = [int(v) for v in batch.execute()]
            if len(values) == 3:
                return values[0], values[1:]
        except Exception:
            pass                               # a batch-less RPC: the plain calls below
    denominator = int(contract.functions.payoutDenominator(key).call())
    if denominator <= 0:
        return denominator, [0, 0]
    return denominator, [int(contract.functions.payoutNumerators(key, i).call()) for i in range(2)]


def collaterals() -> tuple[str, ...]:
    raw = (os.environ.get("SETTLE_COLLATERALS", "") or "").strip()
    if not raw:
        return COLLATERALS
    out = tuple(x.strip() for x in raw.split(",") if x.strip())
    return tuple(a for a in out if a.startswith("0x") and len(a) == 42) or COLLATERALS


def position_id(collateral: str, collection_id: bytes) -> str:
    """The Conditional Tokens position id (= the ERC-1155 token id) of one
    outcome: uint(keccak256(abi.encodePacked(collateral, collectionId))) -
    the contract's own getPositionId, computed here (checked equal on chain)."""
    from web3 import Web3
    digest = Web3.solidity_keccak(["address", "bytes32"], [Web3.to_checksum_address(collateral), collection_id])
    return str(int.from_bytes(digest, "big"))


def slot_positions(condition_id: str, token_ids) -> tuple[dict | None, str]:
    """{token_id: outcome slot} PROVEN from the Conditional Tokens contract, or
    (None, reason). For outcome slot i (index set 1 << i) the contract's
    collection id, combined with the market's collateral, IS the token id - so
    this proves which token each payoutNumerators[i] pays, instead of
    assuming the CLOB lists the tokens in slot order (finding F5)."""
    cid = str(condition_id or "")
    if not cid.startswith("0x") or len(cid) != 66:
        return None, "invalid condition id"
    tokens = [str(t or "") for t in (token_ids or [])]
    if len(tokens) != 2 or not all(tokens) or tokens[0] == tokens[1]:
        return None, "token ids are not a distinct binary pair"
    contract = _client()
    if contract is None:
        return None, f"polygon rpc unavailable ({_last_error})"
    try:
        key = bytes.fromhex(cid[2:])
        collections = [bytes(contract.functions.getCollectionId(b"\x00" * 32, key, 1 << slot).call())
                       for slot in (0, 1)]
    except Exception as exc:
        return None, f"chain read failed: {type(exc).__name__}"
    for collateral in collaterals():
        try:
            ids = [position_id(collateral, c) for c in collections]
        except Exception as exc:                 # a malformed configured address
            return None, f"position id failed: {type(exc).__name__}"
        if sorted(ids) == sorted(tokens):
            return ({ids[0]: 0, ids[1]: 1},
                    f"outcome slots proven on chain (collateral {collateral[:6]}..{collateral[-4:]})")
    return None, "no known collateral maps these tokens to this condition's outcome slots"


def balance_of(holder: str, token_id) -> tuple[float | None, str]:
    """Outcome shares `holder` still owns of `token_id` (ERC-1155 on the CTF),
    or ``(None, reason)``. A winning share leaves this balance when it is
    redeemed for collateral, so this is how REDEEMED is proven - never assumed
    from the winner being known."""
    address = str(holder or "")
    if not (address.startswith("0x") and len(address) == 42):
        return None, "invalid holder address"
    token = str(token_id or "")
    if not token.isdigit():
        return None, "invalid token id"
    contract = _client()
    if contract is None:
        return None, f"polygon rpc unavailable ({_last_error})"
    try:
        from web3 import Web3
        units = int(contract.functions.balanceOf(Web3.to_checksum_address(address), int(token)).call())
    except Exception as exc:
        return None, f"balance read failed: {type(exc).__name__}"
    return units / SHARE_UNITS, "on-chain balanceOf"
