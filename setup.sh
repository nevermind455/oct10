#!/usr/bin/env bash
# Install the BTC sniper on the VPS beside the old bot, without touching it.
#
#   ./setup.sh                      code + .venv + .env + the live journal: ready for paper
#   ./setup.sh --with-live-ledger   also take over the old bot's live ledger, for --live
#                                   (refuses while the old bot - or this one - is running)
#
# Run it as the user that pulls /opt/btcbot (the GitHub deploy key is that user's).
# Re-running is safe: it updates the code and keeps your .env, journals and ledgers.
# It never prints a .env value, never starts or stops a bot, never places an order.
# Paper state (paper_*.json*) is NOT copied: paper starts fresh here.
set -euo pipefail

OLD="${OLD_DIR:-/opt/btcbot}"
APP="${APP_DIR:-/opt/sniperbtc}"
BRANCH="${BRANCH:-sniperbtc}"
WITH_LEDGER=0
for arg in "$@"; do
    case "$arg" in
        --with-live-ledger) WITH_LEDGER=1 ;;
        -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg (see --help)" >&2; exit 2 ;;
    esac
done

say()  { printf '\n== %s\n' "$*"; }
ok()   { printf '   ok    %s\n' "$*"; }
warn() { printf '   WARN  %s\n' "$*"; }
die()  { printf '   FAIL  %s\n' "$*" >&2; exit 1; }

# A bot holding its lock file is running (run.py takes an flock on it).
lock_held() { [ -e "$1" ] && ! flock -n "$1" true 2>/dev/null; }

old_bot_running() {
    if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet btcbot 2>/dev/null; then
        echo "systemd unit btcbot is active"; return 0
    fi
    if pgrep -f "run_feeds.py" >/dev/null 2>&1; then
        echo "a run_feeds.py process is running (pgrep -af run_feeds.py)"; return 0
    fi
    for f in "$OLD"/.btc_bot_live.lock "$OLD"/.btc_bot_paper.lock; do
        if lock_held "$f"; then echo "$f is locked"; return 0; fi
    done
    return 1
}

# ---------------------------------------------------------------- 1. code
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$HERE/run.py" ] && [ -d "$HERE/.git" ]; then
    APP="$HERE"                         # run from a checkout: that checkout is the install
fi
say "1. code: branch $BRANCH in $APP"
if [ -d "$APP/.git" ]; then
    remote="$(git -C "$APP" remote | head -n 1)"
    current="$(git -C "$APP" rev-parse --abbrev-ref HEAD)"
    [ "$current" = "$BRANCH" ] || die "$APP is on branch $current, not $BRANCH"
    if [ -z "$remote" ]; then
        warn "$APP has no git remote: code not updated"
    elif ! git -C "$APP" fetch --quiet "$remote" "$BRANCH"; then
        warn "could not fetch $BRANCH from $remote: code not updated"
    elif git -C "$APP" diff --quiet && git -C "$APP" diff --cached --quiet; then
        git -C "$APP" merge --ff-only --quiet "$remote/$BRANCH" || die "cannot fast-forward $APP to $remote/$BRANCH"
        ok "at $(git -C "$APP" rev-parse --short HEAD)"
    else
        warn "local changes in $APP: code not updated (git -C $APP status)"
    fi
else
    if [ -e "$APP" ]; then die "$APP exists but is not a git checkout"; fi
    url="${REPO_URL:-}"
    if [ -z "$url" ] && [ -d "$OLD/.git" ]; then
        url="$(git -C "$OLD" remote get-url "$(git -C "$OLD" remote | head -n 1)")"
    fi
    [ -n "$url" ] || die "no repository URL: set REPO_URL=<the spe24 URL> and run again"
    git clone --quiet --branch "$BRANCH" --single-branch "$url" "$APP"
    ok "cloned $(git -C "$APP" rev-parse --short HEAD)"
fi
cd "$APP"
if lock_held "$APP/.btc_bot_paper.lock" || lock_held "$APP/.btc_bot_live.lock"; then
    warn "this bot is running: restart it to use updated code"
fi

# ---------------------------------------------------------------- 2. python
say "2. python environment"
PY="${PYTHON:-python3}"
"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' || die "$PY is older than 3.11"
[ -x .venv/bin/python ] || "$PY" -m venv .venv
.venv/bin/python -m pip install --quiet --upgrade pip
.venv/bin/python -m pip install --quiet -r requirements.txt
ok "$(.venv/bin/python --version) + requirements.txt"

# Paths are not secrets: resolve a state path the way run.py does (KEY from
# an .env file, else the default; relative to the install it belongs to).
state_path() {   # state_path <env file> <base dir> <default> KEY [KEY...]
    .venv/bin/python - "$@" <<'PY'
import os, sys
from dotenv import dotenv_values
env_file, base, default, *keys = sys.argv[1:]
values = dotenv_values(env_file) if os.path.exists(env_file) else {}
raw = next((values[k] for k in keys if values.get(k)), default)
print(raw if os.path.isabs(raw) else os.path.join(base, raw))
PY
}

# ---------------------------------------------------------------- 3. .env
say "3. .env (values are never printed)"
if [ -f .env ]; then
    ok "kept the existing $APP/.env"
elif [ -f "$OLD/.env" ]; then
    cp "$OLD/.env" .env
    ok "copied $OLD/.env"
else
    cp .env.example .env
    warn "no $OLD/.env: started from .env.example - add the wallet keys before --live"
fi
chmod 600 .env
# A setting that points into the old install would make this bot write the
# old bot's ledger or journal. Re-point it here (same file name).
old_re="$(printf '%s' "$OLD" | sed 's/[][\.*^$]/\\&/g')"
pointing="$(grep -E "^[A-Za-z_][A-Za-z0-9_]*=.*${old_re}" .env | cut -d= -f1 | tr '\n' ' ' || true)"
if [ -n "$pointing" ]; then
    sed -i -E "/^[A-Za-z_][A-Za-z0-9_]*=.*${old_re}/ s#${old_re}#${APP}#g" .env
    ok "re-pointed from $OLD to $APP: $pointing"
fi
grep -qE '^SNIPER_ONLY=1[[:space:]]*$' .env || warn "SNIPER_ONLY=1 is not set: run.py refuses to start without it"
missing=""
for key in $(grep -oE '^[A-Z_][A-Z0-9_]*=' .env.example | tr -d '='); do
    case "$key" in POLY_PRIVATE_KEY|POLY_FUNDER|POLY_SIGNATURE_TYPE|POLYGON_RPC) continue ;; esac
    grep -qE "^${key}=" .env || missing="$missing $key"
done
if [ -z "$missing" ]; then ok "every setting named in .env.example is set"
else warn "not set (built-in defaults apply; .env.example has the values used so far):$missing"; fi
if grep -qE '^(POLY_PRIVATE_KEY|PRIVATE_KEY)=.+' .env; then ok "wallet key present (not shown)"
else warn "no wallet key in .env: paper only"; fi
for k in STOP_LOSS_ENABLED FLIP_EXIT_ENABLED BOOK_SOURCE BTC_FEED; do
    if grep -qE "^${k}=" .env; then
        warn "$k is an old main-strategy setting: this bot ignores it (run.py names it at start)"
    fi
done

# ---------------------------------------------------------------- 4. live journal
say "4. live journal (paper draws its order delays from >= 30 real live orders)"
old_journal="$(state_path "$OLD/.env" "$OLD" live_sniper.jsonl LIVE_SNIPER_JOURNAL)"
new_journal="$(state_path .env "$APP" live_sniper.jsonl SNIPER_DELAY_JOURNAL LIVE_SNIPER_JOURNAL)"
if [ -f "$new_journal" ]; then
    ok "kept the existing $new_journal"
elif [ -f "$old_journal" ]; then
    cp -p "$old_journal" "$new_journal"
    ok "copied $old_journal -> $new_journal"
else
    warn "no live journal at $old_journal"
fi
if [ -f "$new_journal" ]; then
    n="$(.venv/bin/python -c 'import sys, sniper_mode as S; print(len(S.live_delays(sys.argv[1])[0]))' "$new_journal")"
    if [ "$n" -ge 30 ]; then ok "$n live orders with a venue reply (paper needs >= 30)"
    else warn "only $n live orders: with SNIPER_DELAY_SOURCE=live paper refuses to start (need >= 30)"; fi
fi

# ---------------------------------------------------------------- 5. live ledger
say "5. live ledger"
if [ "$WITH_LEDGER" = 1 ]; then
    if why="$(old_bot_running)"; then
        die "the old bot is running ($why): stop it first (sudo systemctl stop btcbot, or Ctrl+C in its tmux window)"
    fi
    if lock_held "$APP/.btc_bot_live.lock"; then die "this bot is running LIVE: stop it first"; fi
    old_ledger="$(state_path "$OLD/.env" "$OLD" ledger.json LEDGER_PATH)"
    new_ledger="$(state_path .env "$APP" ledger.json LEDGER_PATH)"
    [ -f "$old_ledger" ] || die "no live ledger at $old_ledger (LEDGER_PATH in $OLD/.env)"
    if [ -e "$new_ledger" ]; then
        die "$new_ledger already exists - it may hold this bot's own live orders. Move it away yourself to replace it."
    fi
    .venv/bin/python -c 'import json, sys; json.load(open(sys.argv[1], encoding="utf-8"))' "$old_ledger" \
        || die "$old_ledger is not valid JSON"
    cp -p "$old_ledger" "$new_ledger"
    chmod 600 "$new_ledger"
    ok "copied $old_ledger -> $new_ledger (authorized orders and positions carry over)"
    ok "from now on run LIVE only from $APP: the old ledger is frozen at this copy"
else
    ok "skipped (paper needs none). Before the first --live: stop the old bot, then ./setup.sh --with-live-ledger"
fi

# ---------------------------------------------------------------- 6. owner
if [ "$(id -u)" = 0 ] && [ -d "$OLD" ]; then
    owner="$(stat -c %U "$OLD")"
    if [ "$owner" != root ]; then
        chown -R "$owner": "$APP"
        ok "files owned by $owner, like $OLD"
    fi
fi

# ---------------------------------------------------------------- status
say "old bot"
if why="$(old_bot_running)"; then
    warn "RUNNING ($why). Stop it before starting this one: one bot per VPS."
else
    ok "not running"
fi

cat <<EOF

next:
  cd $APP
  .venv/bin/python run_tests.py                   # every suite, no network, ~2 min
  .venv/bin/python run.py --health                # public feeds for 30s, no wallet
  tmux new -s sniper '.venv/bin/python -u run.py --paper --dash'
  .venv/bin/python sniper_why.py                  # is it trading, and if not, why
live canary (after a clean paper hour; stop the paper bot first):
  ./setup.sh --with-live-ledger                   # old bot stopped
  .venv/bin/python live_preflight.py              # must end with READY
  tmux new -s live '.venv/bin/python -u run.py --live --dash'
  touch STOP_LIVE                                 # emergency brake: no new orders at once
EOF
