"""Settlement — resolved from Polymarket's own outcome data.

Two rules:

1. **Never infer a winner from price or ``closed``.** A BTC close above the
   strike is a good guess at the outcome, not the outcome. Likewise, a closed
   book whose outcome prices happen to be 1/0 is not explicit resolution
   evidence. The market resolves through Polymarket's oracle flow and can
   disagree with Binance (different snapshot instant, different source). A
   ledger that settles on a guess produces PnL that looks precise and is
   wrong, which is worse than PnL that says PENDING.

2. **Map by the `outcomes` field, never by index.** `clobTokenIds[0]` is not
   guaranteed to be Up. Assuming it silently inverts every trade
   (finding H5). If the labels are unrecognised this returns UNKNOWN rather
   than picking one.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

import requests

import http_pool
import onchain_resolution

GAMMA = os.environ.get("GAMMA_HOST", "https://gamma-api.polymarket.com")
CLOB = "https://clob.polymarket.com"


def _env_flag(name: str) -> bool:
    return (os.environ.get(name, "") or "").strip().lower() in (
        "1", "true", "yes", "on")


def trust_gamma_alone() -> bool:
    """Whether Gamma may settle a round the CLOB has not marked closed.

    The CLOB `closed` flag is a market-status field, not the oracle's verdict,
    and it was measured lagging Gamma's umaResolutionStatus by minutes on
    rounds that had already resolved - while /markets was also the slowest
    surface polled (7.8s, and timing out outright). Gamma exposes the UMA
    result that actually determines the on-chain payout, so trusting it alone
    settles sooner. It drops the independent second opinion, so it is opt-in
    and meant for PAPER, where a mis-settled row costs statistics not money.

    Read per call, never cached at import: this module is imported through the
    accounting package, which can load before config.py runs load_dotenv(). A
    module-level read silently saw an unset variable and left the flag off.
    """
    return _env_flag("SETTLE_TRUST_GAMMA")


CONDITION_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
SLUG_RE = re.compile(r"^btc-updown-5m-[0-9]+$")
TOKEN_RE = re.compile(r"^[0-9]+$")

PENDING = "PENDING"
RESOLVED = "RESOLVED"
UNKNOWN = "UNKNOWN"

UP_LABELS = {"up", "yes", "higher", "above"}
DOWN_LABELS = {"down", "no", "lower", "below"}


@dataclass(frozen=True)
class Resolution:
    condition_id: str
    status: str                       # PENDING | RESOLVED | UNKNOWN
    payouts: dict                     # token_id -> 1.0 / 0.0
    winning_label: str | None = None
    detail: str = ""

    @property
    def resolved(self) -> bool:
        return self.status == RESOLVED

    def payout(self, token_id) -> float | None:
        return self.payouts.get(str(token_id))


def _loads(v):
    if isinstance(v, (list, dict)):
        return v
    if isinstance(v, str):
        try:
            return json.loads(v)
        except (json.JSONDecodeError, TypeError, ValueError):
            return None
    return None


def _true(v) -> bool:
    return v is True or str(v).strip().lower() in {"1", "true", "yes"}


def parse_market(m: dict) -> Resolution:
    """Turn one Gamma market object into a Resolution. Pure - unit testable."""
    if not isinstance(m, dict):
        return Resolution("", UNKNOWN, {}, detail="Gamma market is not an object")
    cid = str(m.get("conditionId") or m.get("condition_id") or "")
    tokens = _loads(m.get("clobTokenIds")) or []
    outcomes = _loads(m.get("outcomes")) or []
    prices = _loads(m.get("outcomePrices")) or []
    if any(not isinstance(values, list) for values in (tokens, outcomes, prices)):
        return Resolution(cid, UNKNOWN, {}, detail="Gamma outcome fields are not arrays")

    closed = _true(m.get("closed"))
    if not closed:
        return Resolution(cid, PENDING, {}, detail="market still open")

    # Gamma exposes the oracle state separately from whether trading is
    # closed.  Only an explicit resolved state authorizes a payout; 1/0
    # outcome prices alone can also be ordinary market prices near expiry.
    oracle_status = m.get("umaResolutionStatus")
    if oracle_status is None:
        oracle_status = m.get("uma_resolution_status")
    if oracle_status is None:
        oracle_status = m.get("resolutionStatus")
    resolved_flag = m.get("resolved") if "resolved" in m else None
    explicitly_resolved = (
        str(oracle_status or "").strip().lower() == "resolved"
        or _true(resolved_flag)
    )
    if not explicitly_resolved:
        state = str(oracle_status or "missing").strip().lower()
        return Resolution(cid, PENDING, {},
                          detail=f"oracle resolution status is {state}")

    if not (len(tokens) == len(outcomes) == len(prices) == 2):
        return Resolution(cid, UNKNOWN, {},
                          detail=f"shape mismatch tokens={len(tokens)} "
                                 f"outcomes={len(outcomes)} prices={len(prices)}")

    if (any(token is None or isinstance(token, bool) or not str(token).strip()
            or not str(token).isprintable() or any(c.isspace() for c in str(token))
            for token in tokens) or len({str(token) for token in tokens}) != 2):
        return Resolution(cid, UNKNOWN, {}, detail="duplicate outcome token id")

    labels = {str(o).strip().lower() for o in outcomes}
    if labels != {"up", "down"}:
        return Resolution(cid, UNKNOWN, {},
                          detail=f"unrecognised outcome labels {sorted(labels)}")

    payouts, winner = {}, None
    total = 0.0
    for tok, label, px in zip(tokens, outcomes, prices):
        try:
            if isinstance(px, bool):
                raise ValueError("boolean payout")
            v = float(px)
        except (TypeError, ValueError, OverflowError):
            return Resolution(cid, UNKNOWN, {}, detail=f"unparsable price {px!r}")
        if not 0.0 <= v <= 1.0:
            return Resolution(cid, UNKNOWN, {}, detail=f"invalid payout {v}")
        payouts[str(tok)] = v
        total += v
        if abs(v - 1.0) <= 1e-9:
            winner = str(label)

    # A resolved binary market pays exactly 1.0 across its outcomes. Anything
    # else means it is mid-resolution or disputed - do not settle on it.
    if abs(total - 1.0) > 1e-6:
        return Resolution(cid, PENDING, {}, detail=f"outcomePrices sum to {total}")
    values = sorted(payouts.values())
    if all(abs(v - 0.5) <= 1e-9 for v in values):
        # UMA's explicit Unknown/50-50 resolution pays both outcomes $0.50.
        return Resolution(cid, RESOLVED, payouts, winning_label="50/50")
    if winner is None or any(min(abs(v), abs(v - 1.0)) > 1e-9
                             for v in payouts.values()):
        return Resolution(cid, PENDING, {},
                          detail="payouts are neither final 1/0 nor official 50/50")
    return Resolution(cid, RESOLVED, payouts, winning_label=winner)


def parse_clob_market(m: dict, condition_id: str) -> Resolution:
    """Parse the independent public CLOB winner surface, fail closed."""
    cid = str(condition_id)
    if not isinstance(m, dict):
        return Resolution(cid, UNKNOWN, {}, detail="CLOB market is not an object")
    returned = str(m.get("condition_id") or "")
    if returned != cid or not CONDITION_RE.fullmatch(returned):
        return Resolution(cid, UNKNOWN, {}, detail="CLOB condition id mismatch")
    if not _true(m.get("closed")):
        return Resolution(cid, PENDING, {}, detail="CLOB market still open")
    rows = m.get("tokens")
    if not isinstance(rows, list) or len(rows) != 2:
        return Resolution(cid, UNKNOWN, {}, detail="CLOB market is not binary")

    payouts: dict[str, float] = {}
    winners: list[str] = []
    labels: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            return Resolution(cid, UNKNOWN, {}, detail="malformed CLOB token row")
        token = str(row.get("token_id") or "")
        label = str(row.get("outcome") or "").strip()
        label_l = label.lower()
        if (not TOKEN_RE.fullmatch(token) or int(token) <= 0
                or label_l not in {"up", "down"} or token in payouts):
            return Resolution(cid, UNKNOWN, {}, detail="invalid CLOB token mapping")
        try:
            if isinstance(row.get("price"), bool):
                raise ValueError("boolean payout")
            payout = float(row.get("price"))
        except (TypeError, ValueError, OverflowError):
            return Resolution(cid, UNKNOWN, {}, detail="invalid CLOB payout price")
        if not 0.0 <= payout <= 1.0:
            return Resolution(cid, UNKNOWN, {}, detail="out-of-range CLOB payout")
        labels.add(label_l)
        payouts[token] = payout
        if _true(row.get("winner")):
            winners.append(token)
    if labels != {"up", "down"}:
        return Resolution(cid, UNKNOWN, {}, detail="CLOB outcomes are not UP/DOWN")

    values = sorted(payouts.values())
    if all(abs(v - 0.5) <= 1e-9 for v in values) and not winners:
        if _true(m.get("is_50_50_outcome")):
            return Resolution(cid, RESOLVED, payouts, winning_label="50/50")
        return Resolution(cid, PENDING, {},
                          detail="0.5/0.5 prices lack an explicit 50/50 outcome flag")
    if values != [0.0, 1.0] or len(winners) != 1 or payouts[winners[0]] != 1.0:
        return Resolution(cid, PENDING, {},
                          detail="CLOB winner flags/payouts are not final")
    winner_label = next(
        str(row.get("outcome")) for row in rows
        if str(row.get("token_id")) == winners[0])
    return Resolution(cid, RESOLVED, payouts, winning_label=winner_label)


def fetch(condition_id: str, timeout: float = 10.0, *, chain: bool = True) -> Resolution:
    """Require independent CLOB and Gamma resolution surfaces to agree.

    With ``chain`` (and SETTLE_ONCHAIN not off) the on-chain payout vector is
    asked first. Resolver passes chain=False: it has just read the chain."""
    cid = str(condition_id)
    if not CONDITION_RE.fullmatch(cid):
        return Resolution(cid, UNKNOWN, {}, detail="invalid condition id")
    try:
        timeout = float(timeout)
    except (TypeError, ValueError):
        return Resolution(cid, UNKNOWN, {}, detail="invalid resolution timeout")
    if not 0 < timeout <= 60:
        return Resolution(cid, UNKNOWN, {}, detail="invalid resolution timeout")
    gamma = GAMMA.rstrip("/")
    parsed_gamma = urlsplit(gamma)
    if (parsed_gamma.scheme != "https" or not parsed_gamma.hostname
            or parsed_gamma.username or parsed_gamma.password
            or parsed_gamma.query or parsed_gamma.fragment):
        return Resolution(cid, UNKNOWN, {}, detail="GAMMA_HOST must be a clean HTTPS origin")
    try:
        clob_response = http_pool.get(
            f"{CLOB}/markets/{cid}", timeout=timeout, allow_redirects=False)
        clob_response.raise_for_status()
        if clob_response.status_code != 200:
            raise RuntimeError(f"unexpected HTTP {clob_response.status_code}")
        clob_data = clob_response.json()
    except Exception as exc:
        return Resolution(cid, PENDING, {},
                          detail=(f"CLOB resolution fetch failed: {type(exc).__name__}: "
                                  f"{exc}")[:240])
    if not isinstance(clob_data, dict) or str(clob_data.get("condition_id") or "") != cid:
        return Resolution(cid, UNKNOWN, {}, detail="CLOB condition id/object mismatch")
    # CLOB row order is not proof of an on-chain outcome slot. Use exactly
    # the same contract-derived mapping as Resolver before accepting payouts.
    if chain and onchain_resolution.enabled():
        rows = slot_rows(clob_data, cid)
        if rows is None:
            return Resolution(cid, UNKNOWN, {}, detail="invalid CLOB token mapping")
        proof, _ = onchain_resolution.slot_positions(cid, [token for token, _ in rows])
        valid_proof = _valid_slot_proof(proof, rows)
        chain_payouts, chain_detail = (onchain_resolution.payouts_for(
            cid, sorted(proof, key=proof.get)) if valid_proof else (None, "outcome slots unproven"))
        if chain_payouts:
            winner = None
            for token, label in rows:
                if abs(chain_payouts.get(token, 0.0) - 1.0) <= 1e-9:
                    winner = label or None
            if winner is None and all(
                    abs(v - 0.5) <= 1e-9 for v in chain_payouts.values()):
                winner = "50/50"
            return Resolution(cid, RESOLVED, dict(chain_payouts),
                              winning_label=winner, detail=chain_detail)

    clob_resolution = parse_clob_market(clob_data, cid)
    # Fall through to Gamma only when CLOB is merely BEHIND. PENDING means
    # "not closed yet", which is exactly the lag this flag exists for. UNKNOWN
    # means the response was malformed, mismatched or non-binary - a signal
    # that something is wrong, and settling on the other surface there would
    # hide it. Fail closed on UNKNOWN regardless of the flag.
    trust_gamma = trust_gamma_alone()
    clob_lagging = (not clob_resolution.resolved
                    and clob_resolution.status == PENDING)
    if not clob_resolution.resolved and not (trust_gamma and clob_lagging):
        return clob_resolution

    slug = str(clob_data.get("market_slug") or "")
    if not SLUG_RE.fullmatch(slug):
        return Resolution(cid, UNKNOWN, {}, detail="CLOB market slug is invalid")
    try:
        gamma_response = http_pool.get(
            f"{gamma}/events/slug/{quote(slug, safe='-')}",
            timeout=timeout, allow_redirects=False)
        gamma_response.raise_for_status()
        if gamma_response.status_code != 200:
            raise RuntimeError(f"unexpected HTTP {gamma_response.status_code}")
        event = gamma_response.json()
    except Exception as exc:
        return Resolution(cid, PENDING, {},
                          detail=(f"Gamma resolution fetch failed: {type(exc).__name__}: "
                                  f"{exc}")[:240])
    if not isinstance(event, dict) or event.get("slug") != slug:
        return Resolution(cid, UNKNOWN, {}, detail="Gamma event/slug mismatch")
    matches = [m for m in (_loads(event.get("markets")) or [])
               if isinstance(m, dict)
               and str(m.get("conditionId") or m.get("condition_id") or "") == cid]
    if len(matches) != 1:
        return Resolution(cid, UNKNOWN, {}, detail="Gamma condition is missing or ambiguous")
    gamma_resolution = parse_market(matches[0])
    if not gamma_resolution.resolved:
        # Neither surface has a verdict. Report the CLOB reason when it had
        # one, so "still open" does not get masked by a Gamma-shaped message.
        return gamma_resolution if clob_resolution.resolved else clob_resolution
    if not clob_resolution.resolved:
        # Gamma alone, by explicit configuration. Reached only when CLOB
        # answered and reported the market still open; a CLOB fetch that
        # failed outright returned PENDING above, because the slug this
        # lookup needs comes from that same response.
        return Resolution(cid, RESOLVED, dict(gamma_resolution.payouts),
                          winning_label=gamma_resolution.winning_label,
                          detail="Gamma resolved; CLOB still open "
                                 "(SETTLE_TRUST_GAMMA)")
    if gamma_resolution.payouts != clob_resolution.payouts:
        return Resolution(cid, UNKNOWN, {},
                          detail="CLOB and Gamma final payouts disagree")
    return Resolution(cid, RESOLVED, dict(clob_resolution.payouts),
                      winning_label=clob_resolution.winning_label,
                      detail="CLOB and Gamma final payouts agree")


def slot_rows(m: dict, condition_id: str) -> list[tuple[str, str]] | None:
    """The CLOB market's ``tokens`` as [(token_id, outcome label)] in
    outcome-slot order, or None unless it is a clean UP/DOWN binary for this
    condition. Fixed for a market's life, so one read serves every poll."""
    cid = str(condition_id)
    if not isinstance(m, dict) or str(m.get("condition_id") or "") != cid:
        return None
    rows = m.get("tokens")
    if not isinstance(rows, list) or len(rows) != 2:
        return None
    out: list[tuple[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            return None
        token = str(row.get("token_id") or "")
        label = str(row.get("outcome") or "").strip()
        if not TOKEN_RE.fullmatch(token) or int(token) <= 0:
            return None
        out.append((token, label))
    if (out[0][0] == out[1][0]
            or {label.lower() for _, label in out} != {"up", "down"}):
        return None
    return out


def _valid_slot_proof(proof, rows) -> bool:
    return (isinstance(proof, dict) and set(proof) == {token for token, _ in rows}
            and all(type(slot) is int for slot in proof.values())
            and sorted(proof.values()) == [0, 1])


class Resolver:
    """resolver(condition_id) -> Resolution: the chain first, the API mirrors
    as a slower fallback. Thread-safe; one instance per settlement worker.

    The old path GET the CLOB market on every poll before it could even ask
    the chain (the CLOB response carries the outcome-slot order), and without
    SETTLE_ONCHAIN it waited for the CLOB and Gamma mirrors to agree - 5 to
    13 minutes after a round. Here the slot order is read once per market
    (``prepare`` does it while the market is still trading), so a poll after
    expiry is one batched chain read. The mirrors are asked at most every
    ``api_every_s`` per market, and only when the chain cannot answer (RPC
    unreachable, SETTLE_ONCHAIN=0): a chain that answers "not resolved yet"
    is ahead of both mirrors, so asking them then only costs requests.

    Which token each payout slot pays is PROVEN, not assumed (audit F5): the
    Conditional Tokens contract's position id of slot i must equal the token
    (onchain_resolution.slot_positions). With the proof, the chain's payout
    vector settles. Without it (RPC trouble, an unknown collateral) the chain
    result settles only once the CLOB + Gamma mirrors - which name each token
    explicitly - agree with it; if they disagree it is RESULT_MISMATCH (status
    UNKNOWN: never settled, reported as an error) until a human looks.
    """

    def __init__(self, *, timeout: float = 8.0, api_every_s: float = 5.0,
                 clock=time.monotonic) -> None:
        self.timeout = float(timeout)
        self.api_every_s = float(api_every_s)
        self._clock = clock
        self._lock = threading.Lock()
        self._slots: dict[str, list[tuple[str, str]]] = {}
        self._proof: dict[str, dict[str, int]] = {}
        self._api_at: dict[str, float] = {}

    def slots(self, condition_id: str) -> list[tuple[str, str]] | None:
        with self._lock:
            rows = self._slots.get(str(condition_id))
            return None if rows is None else list(rows)

    def proof(self, condition_id: str) -> dict[str, int] | None:
        """{token: outcome slot} as proven on chain for this market, or None."""
        with self._lock:
            proof = self._proof.get(str(condition_id))
            return None if proof is None else dict(proof)

    def prove(self, condition_id: str) -> tuple[dict[str, int] | None, str]:
        """Prove (once per market, cached) which outcome slot each token is."""
        cid = str(condition_id)
        hit = self.proof(cid)
        if hit:
            return hit, "outcome slots proven on chain"
        rows = self.slots(cid)
        if not rows:
            return None, "market outcome order not read yet"
        proof, detail = onchain_resolution.slot_positions(cid, [t for t, _ in rows])
        if _valid_slot_proof(proof, rows):
            with self._lock:
                self._proof[cid] = dict(proof)
                if len(self._proof) > 256:
                    for old in list(self._proof)[:128]:
                        self._proof.pop(old, None)
            return dict(proof), detail
        return None, detail

    def prepare(self, condition_id: str) -> bool:
        """Read and cache the market's outcome-slot order (one CLOB GET).
        Only the chain read needs it: with SETTLE_ONCHAIN off it is a no-op."""
        cid = str(condition_id)
        if not CONDITION_RE.fullmatch(cid) or not onchain_resolution.enabled():
            return False
        if self.slots(cid):
            return True
        try:
            response = http_pool.get(f"{CLOB}/markets/{cid}", timeout=self.timeout,
                                     allow_redirects=False)
            response.raise_for_status()
            if response.status_code != 200:
                return False
            rows = slot_rows(response.json(), cid)
        except Exception:
            return False
        if not rows:
            return False
        with self._lock:
            self._slots[cid] = rows
            if len(self._slots) > 256:                     # one per 5-minute round: ~21 hours
                for old in list(self._slots)[:128]:
                    self._slots.pop(old, None)
        self.prove(cid)                                    # best effort here; retried at each check
        return True

    def forget(self, condition_id: str) -> None:
        with self._lock:
            self._slots.pop(str(condition_id), None)
            self._proof.pop(str(condition_id), None)
            self._api_at.pop(str(condition_id), None)

    def __call__(self, condition_id: str) -> Resolution:
        cid = str(condition_id)
        if not CONDITION_RE.fullmatch(cid):
            return Resolution(cid, UNKNOWN, {}, detail="invalid condition id")
        chain_detail = None
        unproven = None                                    # (payouts, winner) awaiting the mirrors' word
        if onchain_resolution.enabled():
            if not self.prepare(cid):
                chain_detail = "market outcome order not read yet (CLOB unreachable)"
            else:
                slots = self.slots(cid) or []
                proof, proof_detail = self.prove(cid)
                order = ([t for t, _ in sorted(proof.items(), key=lambda kv: kv[1])] if proof
                         else [t for t, _ in slots])
                payouts, chain_detail = onchain_resolution.payouts_for(cid, order)
                if payouts:
                    winner = next((label for token, label in slots
                                   if abs(payouts.get(token, 0.0) - 1.0) <= 1e-9), None)
                    if winner is None and all(abs(v - 0.5) <= 1e-9 for v in payouts.values()):
                        winner = "50/50"
                    if proof:
                        return Resolution(cid, RESOLVED, dict(payouts), winning_label=winner,
                                          detail=f"{chain_detail}; {proof_detail}")
                    unproven = (dict(payouts), winner)
                    chain_detail = (f"{chain_detail}, but which token each slot pays is not proven "
                                    f"({proof_detail}): waiting for the CLOB + Gamma mirrors to confirm it")
                elif chain_detail == "not resolved on chain yet":
                    return Resolution(cid, PENDING, {}, detail=chain_detail)
        now = self._clock()
        with self._lock:
            due = now - self._api_at.get(cid, -1e18) >= self.api_every_s
            if due:
                self._api_at[cid] = now
        if not due:
            return Resolution(cid, PENDING, {}, detail=(
                f"{chain_detail}; API mirrors next in {self.api_every_s:g}s" if chain_detail
                else "awaiting the API mirrors"))
        mirrors = fetch(cid, self.timeout, chain=False)
        if unproven is not None:
            payouts, winner = unproven
            if not mirrors.resolved:
                return Resolution(cid, PENDING, {}, detail=f"{chain_detail}; mirrors: {mirrors.detail}"[:300])
            same = (set(mirrors.payouts) == set(payouts) and all(
                abs(float(mirrors.payouts[t]) - float(payouts[t])) <= 1e-9 for t in payouts))
            if not same:
                return Resolution(cid, UNKNOWN, {}, detail=(
                    f"RESULT_MISMATCH: on chain {payouts} (slot order unproven) but the CLOB + Gamma "
                    f"mirrors say {mirrors.payouts}: not settled")[:300])
            return Resolution(cid, RESOLVED, dict(payouts), winning_label=winner,
                              detail="on-chain result confirmed by the CLOB + Gamma mirrors (slot order unproven)")
        if mirrors.resolved or not chain_detail:
            return mirrors
        return Resolution(cid, mirrors.status, {}, detail=f"{chain_detail}; mirrors: {mirrors.detail}"[:300])
