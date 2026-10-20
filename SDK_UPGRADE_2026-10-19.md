# Unified SDK migration

Trading integration now targets the official `polymarket-client==0.13.0`
package (`polymarket` imports). The retired `py-clob-client-v2` dependency and
runtime imports have been removed. This is a local source update, not a VPS
deployment or a verified live execution result.

Sources checked:

- https://github.com/Polymarket/py-sdk/tree/f1e2ab2c86d04573c304ca7afe8f1e36fcbd6237
- https://pypi.org/project/polymarket-client/0.13.0/
- https://docs.polymarket.com/migrate/clob-sdk-to-unified-sdk

## Integration

`clob_signing.py` uses the SDK's amount calculations, exchange selection,
unsigned orders, typed data, signature wrapping, canonical order hashes,
HMAC, and POST serialization. It supplies already validated rules, so signing
does not fetch metadata or server time. Protocol-domain selection follows the
SDK's asset routing, including its position-ID handling. The old custom
digest/signing implementation has been replaced; local timing must be
benchmarked again before claiming a performance improvement.

`clob_client.py` preserves the bot's dictionary interfaces for its existing
accounting and reconciler. Balances remain integer base units; execution
amounts remain explicit decimal quantities. Account timestamps and statuses
are normalized, incomplete pagination remains unknown, and a missing order
stays `None` so existing trade-based reconciliation can proceed.

Orders post once through the SDK's request builder and authenticated
transport. Raw acceptance evidence is retained because the SDK's higher-level
response normalization can discard conflicting fields. No automatic order
retry, allowance transaction, or wallet deployment is introduced.

Legacy market orders with `price=0` omit the SDK's optional price bound. This
preserves default SELL exits, which otherwise fail the SDK's strictly positive
`min_price` validation. Explicit positive floors and ceilings remain unchanged;
negative, non-finite and boolean bounds are rejected before SDK access.

Offline protected market BUYs also retain the SDK's final amount-ratio check.
An amount whose rounded shares could lift the next finer supported ask is
rejected before signing, including equality at that next ask.

The configured wallet is explicit, preserving the existing EOA/proxy/Safe
choice. Unknown signer/funder combinations and session keys are refused.
The bot's existing `POLY_SIGNATURE_TYPE=3` restriction remains until a real
deposit-wallet integration is verified. The adapter currently supports the
production Polygon CLOB origin; custom authenticated hosts are refused.

Warm HTTP/2 dispatch, optional `SNIPER_WARM_ONLY=1`, timestamp validation,
durable intent, order caps and ambiguous-order reconciliation remain enabled.
SDK migration does not guarantee acknowledgements or fills below 200 ms.

## Validation and installation

The focused checks cover the original execution fixes plus typed-response
conversion, single POST behavior, conflicting evidence, SDK helper delegation,
dependency pins and HMAC inputs. Native SDK payload/hash/HMAC checks are
explicitly skipped when the SDK is unavailable. Compilation succeeds.

The review environment does not have the pinned SDK or its signing dependencies.
A fresh public-package metadata request failed with `Operation not permitted`;
package installation could not be completed. Native SDK and cryptographic
integration, dependency resolution, full socket-based tests and live performance
have **not** been verified here. Relevant cryptographic pins
follow the published SDK's dependency lock; they require installation checks.

On the intended host, use an isolated environment and verify before live use:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock.txt
.venv/bin/python -m pip check
.venv/bin/python run_tests.py --require-native-sdk unified_sdk
.venv/bin/python run_tests.py
```

The three native tests in `tests_unified_sdk` must run, with no SDK-related
skips. They check the SDK order wire payload, V2/V3 typed-data order hashes, and
HMAC request authentication. The strict runner exits unsuccessfully when the
SDK is missing, its installed version differs from the pin, an import fails,
or the SDK suite still reports skipped checks. It never substitutes mocked
cryptography for these native checks.

The follow-up review fixed share-sized minimum-order skips and aligned paper's
signed quantity, reserved stake and simulated fill with live preparation. It also
fixed hybrid reference initialization races and retained realized equity peaks
through durable transactions and journal replay. The signing benchmark now measures
the production typed-account path without claiming an unverified backend comparison.
The current focused checks passed 338 tests, with the same three native SDK checks
skipped; 188 existing accounting checks passed. Strict native validation correctly
fails in this environment until the installed SDK and dependencies are available.

No uploaded trading records, credentials or `.env` are included in the
source package. The account state and wallet configuration are not migrated
by copying source files.

**Update, October 20:** the pinned lock file installed cleanly on Linux with
Python 3.12 and `pip check` passed. `run_tests.py --require-native-sdk
unified_sdk` now passes with the three native checks run (no skips), and the
whole offline suite passes. Authenticated exchange integration and live
latency are still unverified. See `AUDIT_FIXES_20261020.md`.
