"""Resolution read from the Conditional Tokens contract on Polygon.

Measured on this machine, the ``ConditionResolution`` event fires **85 seconds**
after a five-minute round ends (four of five sampled rounds were exactly 85s,
so it is a scheduled job rather than a queue). Over the same window Gamma's
``umaResolutionStatus`` still read ``None`` at 8.6 minutes and the CLOB
``closed`` flag had not flipped at all; both only caught up around 13 minutes.
Those two APIs are mirrors that lag the chain by roughly ten minutes. This
reads the source they mirror.

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

# payoutDenominator(bytes32) and payoutNumerators(bytes32, uint256).
_ABI = [
    {"inputs": [{"name": "", "type": "bytes32"}],
     "name": "payoutDenominator",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "", "type": "bytes32"}, {"name": "", "type": "uint256"}],
     "name": "payoutNumerators",
     "outputs": [{"name": "", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]

_lock = threading.Lock()
_contract = None
_last_error: str | None = None


def enabled() -> bool:
    """Read per call, never cached: config.py loads .env after import."""
    return (os.environ.get("SETTLE_ONCHAIN", "") or "").strip().lower() in (
        "1", "true", "yes", "on")


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
            w3 = Web3(Web3.HTTPProvider(rpc_url(), request_kwargs={"timeout": 12}))
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
        denominator = int(contract.functions.payoutDenominator(key).call())
        if denominator <= 0:
            return None, "not resolved on chain yet"
        numerators = [int(contract.functions.payoutNumerators(key, i).call())
                      for i in range(2)]
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
