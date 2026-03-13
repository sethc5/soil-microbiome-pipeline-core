#!/usr/bin/env bash
# check_sync.sh — Verify local, GitHub (origin), and Hetzner server are at the same commit.
#
# Usage:
#   bash scripts/ops/check_sync.sh            # full check (fetches + SSH)
#   bash scripts/ops/check_sync.sh --no-ssh   # skip server SSH (offline/fast mode)
#
# Exit codes:
#   0 — all three in sync
#   1 — drift detected (prints what's out of sync and how to fix it)
#
# SSH cache: server commit is cached in .git/sync_server_cache for 5 min to
# avoid re-SSHing on every small operation during an active session.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "ERROR: not inside a git repo" >&2; exit 1
}
cd "$REPO_ROOT"

SERVER_HOST="deploy@144.76.222.125"
SERVER_PATH="/opt/pipeline"
SSH_KEY="${HOME}/.ssh/id_ed25519_personal"
CACHE_FILE="${REPO_ROOT}/.git/sync_server_cache"
CACHE_TTL=300   # seconds (5 min)
NO_SSH=false

for arg in "$@"; do
  [[ "$arg" == "--no-ssh" ]] && NO_SSH=true
done

echo ""
echo "═══════════════════════════════════════════════"
echo "  Git Sync Check — soil-microbiome-pipeline-core"
echo "═══════════════════════════════════════════════"

# ── 1. Fetch origin (silent) ─────────────────────────────────────────────────
printf "  Fetching origin...    "
git fetch origin --quiet 2>/dev/null && echo "done" || echo "WARN: fetch failed (offline?)"

# ── 2. Get commits ───────────────────────────────────────────────────────────
LOCAL_COMMIT=$(git rev-parse HEAD)
ORIGIN_COMMIT=$(git rev-parse origin/main 2>/dev/null || echo "UNKNOWN")
LOCAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)
LOCAL_SHORT="${LOCAL_COMMIT:0:9}"
ORIGIN_SHORT="${ORIGIN_COMMIT:0:9}"

# ── 3. Server commit (with 5-min cache) ──────────────────────────────────────
SERVER_COMMIT="SKIPPED"
SERVER_SHORT="SKIPPED"

if [[ "$NO_SSH" == false ]]; then
  # Check cache freshness
  USE_CACHE=false
  if [[ -f "$CACHE_FILE" ]]; then
    CACHE_AGE=$(( $(date +%s) - $(stat -c %Y "$CACHE_FILE" 2>/dev/null || stat -f %m "$CACHE_FILE" 2>/dev/null || echo 0) ))
    if [[ $CACHE_AGE -lt $CACHE_TTL ]]; then
      USE_CACHE=true
      SERVER_COMMIT=$(cat "$CACHE_FILE")
    fi
  fi

  if [[ "$USE_CACHE" == false ]]; then
    printf "  Checking server...     "
    SERVER_COMMIT=$(ssh -o BatchMode=yes -o ConnectTimeout=8 -i "$SSH_KEY" \
      "$SERVER_HOST" "git -C ${SERVER_PATH} rev-parse HEAD 2>/dev/null" 2>/dev/null) || SERVER_COMMIT="SSH_FAILED"
    echo "$SERVER_COMMIT" > "$CACHE_FILE"
    echo "done"
  else
    printf "  Server (cached <5min): "
    echo "ok"
  fi
  SERVER_SHORT="${SERVER_COMMIT:0:9}"
fi

# ── 4. Report ────────────────────────────────────────────────────────────────
DRIFT=false

echo ""
echo "  Repository            Commit       Status"
echo "  ─────────────────────────────────────────────"

# Local vs origin
if [[ "$LOCAL_COMMIT" == "$ORIGIN_COMMIT" ]]; then
  printf "  %-22s ${GREEN}%-12s [OK]${NC}\n" "local ($LOCAL_BRANCH)" "$LOCAL_SHORT"
  printf "  %-22s ${GREEN}%-12s [OK]${NC}\n" "github (origin/main)" "$ORIGIN_SHORT"
else
  DRIFT=true
  # Determine direction
  AHEAD=$(git rev-list --count "origin/main..HEAD" 2>/dev/null || echo "?")
  BEHIND=$(git rev-list --count "HEAD..origin/main" 2>/dev/null || echo "?")
  if [[ "$AHEAD" -gt 0 ]] 2>/dev/null; then
    printf "  %-22s ${YELLOW}%-12s [LOCAL AHEAD +%s]${NC}\n" "local ($LOCAL_BRANCH)" "$LOCAL_SHORT" "$AHEAD"
    printf "  %-22s ${RED}%-12s [BEHIND -%s]${NC}\n" "github (origin/main)" "$ORIGIN_SHORT" "$AHEAD"
  else
    printf "  %-22s ${RED}%-12s [BEHIND -%s]${NC}\n" "local ($LOCAL_BRANCH)" "$LOCAL_SHORT" "$BEHIND"
    printf "  %-22s ${YELLOW}%-12s [AHEAD +%s]${NC}\n" "github (origin/main)" "$ORIGIN_SHORT" "$BEHIND"
  fi
fi

# Server vs origin
if [[ "$NO_SSH" == true ]]; then
  printf "  %-22s %-12s [SKIPPED --no-ssh]\n" "hetzner server" ""
elif [[ "$SERVER_COMMIT" == "SSH_FAILED" ]]; then
  printf "  %-22s ${YELLOW}%-12s [SSH UNREACHABLE]${NC}\n" "hetzner server" "?"
  DRIFT=true
elif [[ "$SERVER_COMMIT" == "UNKNOWN" ]]; then
  printf "  %-22s ${YELLOW}%-12s [UNKNOWN]${NC}\n" "hetzner server" "?"
elif [[ "$SERVER_COMMIT" == "$ORIGIN_COMMIT" ]]; then
  printf "  %-22s ${GREEN}%-12s [OK]${NC}\n" "hetzner server" "$SERVER_SHORT"
else
  DRIFT=true
  printf "  %-22s ${RED}%-12s [DRIFT — origin=%s]${NC}\n" "hetzner server" "$SERVER_SHORT" "$ORIGIN_SHORT"
fi

echo ""

# ── 5. Fix suggestions ───────────────────────────────────────────────────────
if [[ "$DRIFT" == true ]]; then
  echo "  ⚠  DRIFT DETECTED — resolve before proceeding:"
  echo ""

  AHEAD=$(git rev-list --count "origin/main..HEAD" 2>/dev/null || echo 0)
  BEHIND=$(git rev-list --count "HEAD..origin/main" 2>/dev/null || echo 0)

  if [[ "$AHEAD" -gt 0 ]] 2>/dev/null; then
    echo "  • Local has unpushed commits:"
    echo "      git push origin main"
  fi
  if [[ "$BEHIND" -gt 0 ]] 2>/dev/null; then
    echo "  • Local is behind origin:"
    echo "      git pull --rebase origin main"
  fi
  if [[ "$SERVER_COMMIT" != "$ORIGIN_COMMIT" && "$SERVER_COMMIT" != "SSH_FAILED" && "$SERVER_COMMIT" != "SKIPPED" ]]; then
    echo "  • Server is not at origin/main:"
    echo "      ssh -i ~/.ssh/id_ed25519_personal deploy@144.76.222.125 \\"
    echo "        'cd /opt/pipeline && git pull'"
  fi
  if [[ "$SERVER_COMMIT" == "SSH_FAILED" ]]; then
    echo "  • Could not reach server — check connectivity or VPN."
  fi
  echo ""
  exit 1
else
  echo "  ✓ All repositories in sync."
  echo ""
  exit 0
fi
