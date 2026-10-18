#!/usr/bin/env bash
# Diagnostic archive of both bots' records - run on the VPS. Read-only on the bots.
#
#   bash export_records.sh            # -> /root/all-records-<UTC time>.tgz
#   PRINTS_DAYS=1 bash export_records.sh
#
# Takes, by name only (never .env / .evn / keys): journals (*.jsonl), ledgers,
# canary state, paper account, and the persisted public trade prints
# (market_prints/market_prints-YYYYMMDD.jsonl, last PRINTS_DAYS days, default 2).
# Prints written before 2026-10-18 never existed on disk: none can be exported.
set -euo pipefail
cd /opt
OUT="/root/all-records-$(date -u +%Y%m%dT%H%MZ).tgz"
DAYS="${PRINTS_DAYS:-2}"
LIST="$(mktemp)"
find sniperbtc sniperbtc-paper50 -maxdepth 2 -type f \( -name "*.jsonl" -o -name "*ledger*.json*" \
    -o -name "live_canary_state.json*" -o -name "paper_account.json" \) \
    -not -path "*/.venv/*" -not -path "*/data/*" -not -path "*/market_prints/*" -print0 > "$LIST" 2>/dev/null || true
find sniperbtc/market_prints sniperbtc-paper50/market_prints -maxdepth 1 -type f -name "market_prints-*.jsonl" \
    -mtime "-$DAYS" -print0 >> "$LIST" 2>/dev/null || true
tar czf "$OUT" --null -T "$LIST"
rm -f "$LIST"
if tar tzf "$OUT" | grep -qiE '(^|/)\.env|\.evn|private|secret'; then
    echo "REFUSING: a credential-like file got into $OUT - deleting it" >&2
    rm -f "$OUT"
    exit 1
fi
ls -lh "$OUT"
echo "download on the PC:  scp root@<VPS-IP>:$OUT \"\$HOME\\Downloads\\\""
