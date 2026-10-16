#!/usr/bin/env bash
# Put this PC's checkout of the bot on a VPS - first install or update - over SSH.
# No GitHub key is needed on the server: the code travels as a git bundle.
# Never copies .env, wallet keys or any state file; never starts or stops a bot.
#
#   bash push_to_vps.sh ubuntu@1.2.3.4 ~/Downloads/london.pem            # branch repair/2026-10-14
#   bash push_to_vps.sh ubuntu@1.2.3.4 ~/Downloads/london.pem main
#
# Update: fast-forward only (a server with its own local commits is left alone).
# Afterwards restart the bot on the server yourself (tmux: Ctrl+C, start again).
set -euo pipefail

target=${1:?usage: bash push_to_vps.sh user@host key.pem [branch]}
key=${2:?usage: bash push_to_vps.sh user@host key.pem [branch]}
branch=${3:-repair/2026-10-14}
dir=/opt/sniperbtc

cd "$(dirname "$0")"
git rev-parse --verify --quiet "$branch" >/dev/null || { echo "no local branch $branch"; exit 1; }
sha=$(git rev-parse --short "$branch")
bundle="$(mktemp -d)/sniperbtc.bundle"
git bundle create "$bundle" "$branch" >/dev/null 2>&1
echo "bundle: $branch @ $sha ($(du -h "$bundle" | cut -f1))"

ssh_opts=(-i "$key" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
scp -q "${ssh_opts[@]}" "$bundle" "$target:/tmp/sniperbtc.bundle"
rm -f "$bundle"

ssh "${ssh_opts[@]}" "$target" "bash -s" -- "$dir" "$branch" <<'REMOTE'
set -euo pipefail
dir=$1; branch=$2; bundle=/tmp/sniperbtc.bundle
git bundle verify "$bundle" >/dev/null
if [ ! -d "$dir/.git" ]; then
    sudo mkdir -p "$dir"
    sudo chown "$(id -u):$(id -g)" "$dir"
    git clone -q -b "$branch" "$bundle" "$dir"
    echo "installed $dir"
else
    cd "$dir"
    git fetch -q "$bundle" "$branch:refs/remotes/bundle/$branch"
    if [ "$(git rev-parse --abbrev-ref HEAD)" != "$branch" ]; then
        git checkout -q "$branch" 2>/dev/null || git checkout -q -b "$branch" "refs/remotes/bundle/$branch"
    fi
    git merge -q --ff-only "refs/remotes/bundle/$branch" || {
        echo "NOT updated: the server's $branch has commits this PC does not (left as it is)"; exit 1; }
fi
rm -f "$bundle"
cd "$dir"
echo "server now at: $(git log --oneline -1)"
if [ ! -x .venv/bin/python ]; then
    echo
    echo "no .venv yet - set up Python once (as on the old London box: Python 3.12 via uv):"
    echo "  curl -LsSf https://astral.sh/uv/install.sh | sh && source ~/.local/bin/env"
    echo "  cd $dir && uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -r requirements.lock.txt"
fi
[ -f .env ] || echo "no .env yet - copy .env.example to .env and fill it in on the server (never commit it)"
REMOTE
