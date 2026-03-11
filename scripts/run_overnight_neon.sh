#!/usr/bin/env bash
# run_overnight_neon.sh — chained overnight NEON 16S → T1 FBA run
# Usage: nohup bash scripts/run_overnight_neon.sh > /dev/null 2>&1 &
# Logs: /data/pipeline/logs/overnight_YYYYMMDD_HHMM.log
set -uo pipefail  # u=treat unset vars as errors, NO -e (steps handle their own errors)

LOG_DIR="/data/pipeline/logs"
STAMP="$(date +%Y%m%d_%H%M)"
LOG="${LOG_DIR}/overnight_${STAMP}.log"
DB="/data/pipeline/db/soil_microbiome.db"
STAGING="/data/pipeline/staging"
SILVA="/data/pipeline/ref/16S_ref.fasta"
PYTHON="/opt/pipeline/.venv/bin/python3"
SCRIPTS="/opt/pipeline/scripts"

mkdir -p "$LOG_DIR"

log() { echo "$(date +%H:%M:%S) $*" | tee -a "$LOG"; }

log "=== Overnight NEON pipeline run started ==="
log "Log file: $LOG"

# ---------------------------------------------------------------------------
# Step 1: Patch missing FASTQ URLs into communities.notes
# ---------------------------------------------------------------------------
log ""
log "=== STEP 1: patch_neon_notes.py ==="
if $PYTHON "$SCRIPTS/patch_neon_notes.py" \
    --db "$DB" \
    --staging "$STAGING" \
    --all-sites \
    --years 2021 2022 2023 2024 \
    2>&1 | tee -a "$LOG"; then
    PATCHED=$(grep "Communities patched" "$LOG" | tail -1 | grep -oP d+ || echo "?")
    log "Step 1 OK — patched: $PATCHED communities"
else
    log "Step 1 WARNING: patch_neon_notes exited non-zero — continuing anyway"
fi

# ---------------------------------------------------------------------------
# Step 2: 16S classification via vsearch
# ---------------------------------------------------------------------------
log ""
log "=== STEP 2: process_neon_16s.py ==="
if $PYTHON "$SCRIPTS/process_neon_16s.py" \
    --db "$DB" \
    --staging "${STAGING}/neon_16s" \
    --silva "$SILVA" \
    --all-sites \
    --workers 8 \
    2>&1 | tee -a "$LOG"; then
    log "Step 2 OK"
else
    log "Step 2 WARNING: process_neon_16s exited non-zero — continuing anyway"
fi

# Count T1-eligible communities after 16S classification
CLASSIFIED=$($PYTHON - <<PYEOF 2>/dev/null || echo "0"
import sqlite3
db = sqlite3.connect("$DB")
n = db.execute("""
    SELECT COUNT(*) FROM communities c
    JOIN runs r ON c.sample_id=r.sample_id
    JOIN samples s ON r.sample_id=s.sample_id
    WHERE s.source=neon AND r.t0_pass=1 AND r.t1_pass IS NULL
      AND c.top_genera NOT IN ([],{},null,) AND c.top_genera IS NOT NULL
""").fetchone()[0]
print(n)
PYEOF
)
log "NEON communities eligible for T1 after 16S: $CLASSIFIED"

# ---------------------------------------------------------------------------
# Step 3: T1 FBA (real-mode on NEON communities)
# ---------------------------------------------------------------------------
log ""
log "=== STEP 3: t1_fba_batch.py --real-mode ==="
if [[ "${CLASSIFIED:-0}" -gt 0 ]]; then
    $PYTHON "$SCRIPTS/t1_fba_batch.py" \
        --real-mode \
        --n-communities 10000 \
        --workers 36 \
        --model-dir /data/pipeline/models \
        --proteome-dir /data/pipeline/proteome_cache \
        2>&1 | tee -a "$LOG" || log "Step 3 WARNING: t1_fba_batch exited non-zero"
else
    log "No T1-eligible communities after 16S — skipping T1 FBA."
    log "Review Step 2 output in $LOG to diagnose 16S classification results."
fi

log ""
log "=== Overnight run COMPLETE ==="
