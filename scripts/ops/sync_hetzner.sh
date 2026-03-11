#!/bin/bash
# sync_hetzner.sh — bidirectional sync between dell5 (local) and hetzner2 (/opt/pipeline)
#
# Run from the repo root on dell5.
# 
# FLOW:
#   1. Push any uncommitted local changes first (warns if dirty)
#   2. Pull runtime outputs from hetzner2 → local (results/, FINDINGS.md)
#   3. Pull new scripts from hetzner2 → local (excludes files git manages)
#   4. git pull on hetzner2 to bring code current
#   5. Report divergences
#
# Usage:
#   ./sync_hetzner.sh          — full sync
#   ./sync_hetzner.sh --push   — local → hetzner2 only (code update)
#   ./sync_hetzner.sh --pull   — hetzner2 → local only (results + new scripts)
#   ./sync_hetzner.sh --check  — show status without syncing

set -euo pipefail

REMOTE="hetzner2"
REMOTE_DIR="/opt/pipeline"
REMOTE_DB="/data/pipeline/db/soil_microbiome.db"
LOCAL_DIR="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
MODE="${1:-all}"

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; NC='\033[0m'
info()  { echo -e "${GREEN}▶ $*${NC}"; }
warn()  { echo -e "${YELLOW}⚠ $*${NC}"; }
error() { echo -e "${RED}✗ $*${NC}" >&2; }

cd "$LOCAL_DIR"

# ── 0. Connectivity check ──────────────────────────────────────────────────
ssh -q -o ConnectTimeout=6 "$REMOTE" true || { error "Cannot reach $REMOTE"; exit 1; }

# ── Check mode ────────────────────────────────────────────────────────────
if [[ "$MODE" == "--check" ]]; then
    info "=== Local git status ==="
    git status --short
    echo ""
    info "=== hetzner2 git status ==="
    ssh "$REMOTE" "cd $REMOTE_DIR && git log --oneline -3 && echo '---' && git status --short"
    echo ""
    info "=== Commits local has that hetzner2 doesn't ==="
    HETZ_HEAD=$(ssh "$REMOTE" "cd $REMOTE_DIR && git rev-parse HEAD")
    git log --oneline "${HETZ_HEAD}..HEAD" || echo "(none)"
    echo ""
    info "=== DB state on hetzner2 ==="
    ssh "$REMOTE" "sqlite3 $REMOTE_DB \"
      SELECT 'total_runs:' || COUNT(*) FROM runs;
      SELECT 't0_pass:' || SUM(t0_pass) FROM runs;
      SELECT 't1_pass:' || SUM(t1_pass) FROM runs;
      SELECT 't2_pass:' || SUM(t2_pass) FROM runs;
      SELECT 'last_run:' || MAX(run_date) FROM runs;
    \" 2>/dev/null"
    echo ""
    info "=== Running on hetzner2 ==="
    ssh "$REMOTE" "pgrep -a -f 'pipeline_core|dfba|batch_runner' 2>/dev/null || echo 'idle'"
    exit 0
fi

# ── 1. Warn if local is dirty ──────────────────────────────────────────────
if [[ -n "$(git status --porcelain)" ]]; then
    warn "Local repo has uncommitted changes — commit or stash before syncing code to hetzner2"
    git status --short
    echo ""
fi

# ── 2. Pull: hetzner2 runtime outputs → local ─────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--pull" ]]; then
    info "Pulling runtime outputs from hetzner2..."

    # FINDINGS.md
    scp -q "$REMOTE:$REMOTE_DIR/FINDINGS.md" "$LOCAL_DIR/FINDINGS.md" && \
        info "  FINDINGS.md" || warn "  FINDINGS.md not found"

    # results/ (CSVs, maps, reports) — skip large binary model files
    rsync -az --exclude='*.png' --exclude='*.pdf' \
        "$REMOTE:$REMOTE_DIR/results/" "$LOCAL_DIR/results/" && \
        info "  results/ synced" || warn "  results/ rsync failed"

    # Spatial maps separately (they're useful to keep)
    rsync -az "$REMOTE:$REMOTE_DIR/results/spatial/" \
        "$LOCAL_DIR/results/spatial/" 2>/dev/null && \
        info "  results/spatial/ synced" || true

    # New scripts from hetzner2 (only pull files git doesn't know about)
    # Uses --ignore-existing so git-managed files are never overwritten
    info "Pulling new scripts from hetzner2 (non-destructive)..."
    rsync -az --ignore-existing \
        --exclude='__pycache__/' --exclude='*.pyc' \
        "$REMOTE:$REMOTE_DIR/scripts/" "$LOCAL_DIR/scripts/" && \
        info "  scripts/ synced (new files only)" || warn "  scripts/ rsync failed"

    # reference/ new files
    rsync -az --ignore-existing \
        "$REMOTE:$REMOTE_DIR/reference/" "$LOCAL_DIR/reference/" 2>/dev/null && \
        info "  reference/ synced" || true

    # Show what changed
    NEW=$(git status --short)
    if [[ -n "$NEW" ]]; then
        warn "New/modified files pulled from hetzner2 — review and commit:"
        echo "$NEW"
    else
        info "No new files pulled."
    fi
fi

# ── 3. Push: code update on hetzner2 via git pull ─────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--push" ]]; then
    info "Updating hetzner2 from git..."
    LOCAL_HEAD=$(git rev-parse HEAD)
    HETZ_HEAD=$(ssh "$REMOTE" "cd $REMOTE_DIR && git rev-parse HEAD")

    if [[ "$LOCAL_HEAD" == "$HETZ_HEAD" ]]; then
        info "  hetzner2 already at HEAD ($LOCAL_HEAD) — nothing to pull"
    else
        COMMITS_AHEAD=$(git rev-list --count "${HETZ_HEAD}..HEAD" 2>/dev/null || echo "?")
        warn "  hetzner2 is $COMMITS_AHEAD commits behind — running git pull..."
        ssh "$REMOTE" "cd $REMOTE_DIR && git pull --ff-only origin main 2>&1" && \
            info "  hetzner2 updated to $(git rev-parse HEAD)" || \
            error "  git pull failed — hetzner2 may have local modifications blocking it"

        # Install any new requirements
        ssh "$REMOTE" "cd $REMOTE_DIR && .venv/bin/pip install -q -r requirements.txt 2>&1 | tail -3" && \
            info "  requirements installed" || warn "  pip install had issues"
    fi
fi

# ── 4. Summary ─────────────────────────────────────────────────────────────
echo ""
info "=== Sync complete ==="
echo "  Local HEAD : $(git rev-parse --short HEAD)"
echo "  Hetzner2   : $(ssh "$REMOTE" "cd $REMOTE_DIR && git rev-parse --short HEAD" 2>/dev/null)"
echo ""
info "Run './sync_hetzner.sh --check' for full status at any time."
