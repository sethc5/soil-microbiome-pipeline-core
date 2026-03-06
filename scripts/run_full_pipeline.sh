#!/usr/bin/env bash
# =============================================================================
# run_full_pipeline.sh — Master orchestrator for soil microbiome pipeline
# =============================================================================
# Chains all pipeline phases in dependency order.
# Designed to run in a tmux session on the Hetzner server.
#
# Usage:
#   cd /opt/pipeline
#   source .venv/bin/activate
#   bash scripts/run_full_pipeline.sh [--skip-t1] [--skip-bootstrap] [--workers N]
#
# Each phase is idempotent — re-running skips already-completed work.
# =============================================================================
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
DB="/data/pipeline/db/soil_microbiome.db"
LOG_DIR="/var/log/pipeline"
WORKERS=36
SKIP_BOOTSTRAP=false
SKIP_T1=false
SKIP_DFBA=false
SKIP_CLIMATE=false

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)            DB="$2"; shift 2 ;;
    --workers)       WORKERS="$2"; shift 2 ;;
    --skip-bootstrap) SKIP_BOOTSTRAP=true; shift ;;
    --skip-t1)       SKIP_T1=true; shift ;;
    --skip-dfba)     SKIP_DFBA=true; shift ;;
    --skip-climate)  SKIP_CLIMATE=true; shift ;;
    --help|-h)
      echo "Usage: $0 [--db PATH] [--workers N] [--skip-bootstrap] [--skip-t1] [--skip-dfba] [--skip-climate]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

mkdir -p "$LOG_DIR"

MASTER_LOG="$LOG_DIR/full_pipeline_$(date +%Y%m%d_%H%M%S).log"

# ── Logging helper ────────────────────────────────────────────────────────────
log() {
  local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg" | tee -a "$MASTER_LOG"
}

run_phase() {
  local phase_name="$1"
  shift
  log "━━━ PHASE: $phase_name ━━━"
  log "Command: $*"
  local start_ts
  start_ts=$(date +%s)

  if "$@" 2>&1 | tee -a "$MASTER_LOG"; then
    local end_ts
    end_ts=$(date +%s)
    local elapsed=$(( end_ts - start_ts ))
    log "✓ $phase_name completed in ${elapsed}s"
    return 0
  else
    local rc=$?
    log "✗ $phase_name FAILED (exit code $rc)"
    log "Check logs in $LOG_DIR"
    return $rc
  fi
}

# ── Banner ────────────────────────────────────────────────────────────────────
log "╔══════════════════════════════════════════════════════════════╗"
log "║      SOIL MICROBIOME PIPELINE — FULL EXECUTION             ║"
log "╠══════════════════════════════════════════════════════════════╣"
log "║  DB:      $DB"
log "║  Workers: $WORKERS"
log "║  Log:     $MASTER_LOG"
log "║  Skip:    bootstrap=$SKIP_BOOTSTRAP t1=$SKIP_T1 dfba=$SKIP_DFBA climate=$SKIP_CLIMATE"
log "╚══════════════════════════════════════════════════════════════╝"

PIPELINE_START=$(date +%s)

# ══════════════════════════════════════════════════════════════════════════════
# Phase -1: Initialize DB (schema creation + WAL mode)
# Creates all 8 tables if they don't exist. Idempotent.
# ══════════════════════════════════════════════════════════════════════════════
log "━━━ PHASE: Initialize Database ━━━"
mkdir -p "$(dirname "$DB")"
python -c "
import sys; sys.path.insert(0, '.')
from db_utils import SoilDB
with SoilDB('$DB') as db:
    tables = [r[0] for r in db.conn.execute(\"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name\").fetchall()]
    print(f'Schema OK — tables: {tables}')
" 2>&1 | tee -a "$MASTER_LOG"
log "✓ Database initialized"

# ══════════════════════════════════════════════════════════════════════════════
# Phase 0: Synthetic Bootstrap (T0 + T0.25)
# Generate synthetic communities, train ML predictor, produce reference BIOM.
# Already completed: 220K communities. Skippable.
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_BOOTSTRAP" == "false" ]]; then
  run_phase "Synthetic Bootstrap" \
    python scripts/synthetic_bootstrap.py \
      --db "$DB" \
      --n-communities 220000 \
      --workers "$WORKERS" \
      --batch-size 2000
else
  log "⏭ Skipping synthetic bootstrap (already completed)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Populate Supporting Tables
# Fills targets, taxa, and receipts tables. Fast (<1 min).
# ══════════════════════════════════════════════════════════════════════════════
run_phase "Populate Tables (targets/taxa/receipts)" \
  python scripts/populate_tables.py --db "$DB"

# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: T2 dFBA Batch (dynamic FBA ODE simulations)
# Runs batch dFBA on T0.25-passed communities. Already completed: 108K T2-passed.
# Idempotent — skips communities already processed.
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_DFBA" == "false" ]]; then
  run_phase "dFBA Batch (T2 ODE simulations)" \
    python scripts/dfba_batch.py \
      --db "$DB" \
      --n-communities 10000 \
      --workers "$WORKERS" \
      --batch-size 100
else
  log "⏭ Skipping dFBA batch (already completed)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Climate dFBA Projections
# Runs 5 climate scenario simulations per T2-passed community.
# Already completed: 542K rows. Idempotent.
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_CLIMATE" == "false" ]]; then
  run_phase "Climate dFBA Projections" \
    python scripts/climate_dfba.py \
      --db "$DB" \
      --n-communities 120000 \
      --workers "$WORKERS" \
      --batch-size 20
else
  log "⏭ Skipping climate dFBA (already completed)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 4: T1 FBA Batch (CarveMe + COBRApy community FBA)
# Two-phase: (A) build per-genus metabolic models, (B) community FBA + keystone.
# HEAVY — hours of compute. Uses GLPK solver (unlimited model size).
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_T1" == "false" ]]; then
  run_phase "T1 FBA Batch (CarveMe + COBRApy)" \
    python scripts/t1_fba_batch.py \
      --db "$DB" \
      --workers "$WORKERS" \
      --batch-size 20
else
  log "⏭ Skipping T1 FBA (--skip-t1 set)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 5: Intervention Screening (T2)
# Screens bioinoculants + amendments + management practices.
# Runs on T1-passed communities only.
# ══════════════════════════════════════════════════════════════════════════════
run_phase "Intervention Screening" \
  python scripts/intervention_batch.py \
    --db "$DB" \
    --workers "$WORKERS" \
    --batch-size 50

# ══════════════════════════════════════════════════════════════════════════════
# Phase 6: Analysis Pipeline
# Post-simulation analysis: correlations, ranking, spatial, climate resilience.
# Produces JSON/CSV outputs in results/.
# ══════════════════════════════════════════════════════════════════════════════
run_phase "Analysis Pipeline" \
  python scripts/analysis_pipeline.py \
    --db "$DB" \
    --out-dir results/

# ══════════════════════════════════════════════════════════════════════════════
# Phase 7: Generate Findings
# Populates findings table from analysis outputs.
# ══════════════════════════════════════════════════════════════════════════════
run_phase "Generate Findings" \
  python scripts/generate_findings.py \
    --db "$DB" \
    --results-dir results/

# ══════════════════════════════════════════════════════════════════════════════
# Phase 8: Reports
# Generate FINDINGS.md and intervention report.
# ══════════════════════════════════════════════════════════════════════════════
log "━━━ PHASE: Generate Reports ━━━"

if command -v python &>/dev/null; then
  # Findings markdown
  if [[ -f findings_generator.py ]]; then
    run_phase "FINDINGS.md" \
      python findings_generator.py --db "$DB"
  fi

  # Intervention report
  if [[ -f intervention_report.py ]]; then
    run_phase "Intervention Report" \
      python intervention_report.py --db "$DB" --top 50
  fi
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase 9: Validation (optional — requires reference data)
# Runs known-community recovery test if reference files exist.
# ══════════════════════════════════════════════════════════════════════════════
if [[ -f reference/high_bnf_communities.biom ]] && [[ -f reference/bnf_measurements.csv ]]; then
  run_phase "Validation (known community recovery)" \
    python validate_pipeline.py \
      --config config.example.yaml \
      --reference-communities reference/high_bnf_communities.biom \
      --measured-function reference/bnf_measurements.csv \
      --db "$DB"
else
  log "⏭ Skipping validation (no reference data in reference/)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════════
PIPELINE_END=$(date +%s)
TOTAL_ELAPSED=$(( PIPELINE_END - PIPELINE_START ))
HOURS=$(( TOTAL_ELAPSED / 3600 ))
MINUTES=$(( (TOTAL_ELAPSED % 3600) / 60 ))
SECONDS=$(( TOTAL_ELAPSED % 60 ))

log ""
log "╔══════════════════════════════════════════════════════════════╗"
log "║  PIPELINE COMPLETE                                          ║"
log "║  Total time: ${HOURS}h ${MINUTES}m ${SECONDS}s              ║"
log "╚══════════════════════════════════════════════════════════════╝"

# Quick DB stats
log ""
log "── Database Stats ──"
sqlite3 "$DB" "
  SELECT 'samples'        AS tbl, COUNT(*) FROM samples
  UNION ALL SELECT 'communities',   COUNT(*) FROM communities
  UNION ALL SELECT 'targets',       COUNT(*) FROM targets
  UNION ALL SELECT 'runs',          COUNT(*) FROM runs
  UNION ALL SELECT 'interventions', COUNT(*) FROM interventions
  UNION ALL SELECT 'taxa',          COUNT(*) FROM taxa
  UNION ALL SELECT 'findings',      COUNT(*) FROM findings
  UNION ALL SELECT 'receipts',      COUNT(*) FROM receipts;
" 2>/dev/null | while IFS='|' read -r tbl cnt; do
  log "  $tbl: $cnt"
done

log ""
log "Master log: $MASTER_LOG"
log "Done."
