#!/usr/bin/env bash
# =============================================================================
# run_real_data_funnel.sh — Real-data funnel: T0.25 → T1 → T2 → analysis
# =============================================================================
# Picks up after bulk NEON 16S ingest is complete.
# All phases are idempotent — re-running safely skips completed work.
#
# Usage:
#   cd /opt/pipeline && source .venv/bin/activate
#   bash scripts/ops/run_real_data_funnel.sh [--skip-t025] [--skip-t1] [--skip-t1-synth] [--skip-t2] [--workers N]
#
# Phases:
#   T0.25      — score all t0_pass=1 communities missing function_score
#   T1         — community FBA on real neon/mgnify amplicon communities (--real-mode)
#   T1-SYNTH   — bootstrap T1 on 220K synthetic communities via --t025-mode
#                (breaks T2→T1 circular dependency; must run before T2 on synthetics)
#   T2         — dFBA dynamics on all T1-pass communities (real + synthetic)
#   ANALYSIS   — ranked_candidates, spatial, enrichment, findings (parallel)
# =============================================================================
set -euo pipefail

DB="/data/pipeline/db/soil_microbiome.db"
LOG_DIR="/opt/pipeline/logs"
WORKERS=36
SKIP_T025=false
SKIP_T1=false
SKIP_T1_SYNTH=false
SKIP_T2=false
SKIP_ANALYSIS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db)            DB="$2"; shift 2 ;;
    --workers)       WORKERS="$2"; shift 2 ;;
    --skip-t025)     SKIP_T025=true; shift ;;
    --skip-t1)       SKIP_T1=true; shift ;;
    --skip-t1-synth) SKIP_T1_SYNTH=true; shift ;;
    --skip-t2)       SKIP_T2=true; shift ;;
    --skip-analysis) SKIP_ANALYSIS=true; shift ;;
    --help|-h)
      echo "Usage: $0 [--db PATH] [--workers N] [--skip-t025] [--skip-t1] [--skip-t1-synth] [--skip-t2] [--skip-analysis]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

mkdir -p "$LOG_DIR"
RUN_LOG="$LOG_DIR/real_data_funnel_$(date +%Y%m%d_%H%M%S).log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$RUN_LOG"; }

run_phase() {
  local name="$1"; shift
  log "━━━ PHASE: $name ━━━"
  local t0; t0=$(date +%s)
  if "$@" 2>&1 | tee -a "$RUN_LOG"; then
    log "✓ $name — $(($(date +%s) - t0))s"
  else
    local rc=$?
    log "✗ $name FAILED (exit=$rc) — continuing"
    return $rc
  fi
}

log "╔══════════════════════════════════════════════════════╗"
log "║  REAL DATA FUNNEL — T0.25 → T1 → T1-SYNTH → T2 → OUT  ║"
log "║  DB:      $DB"
log "║  Workers: $WORKERS"
log "║  Log:     $RUN_LOG"
log "╚══════════════════════════════════════════════════════╝"

# ── DB stats at start ─────────────────────────────────────────────────────────
log "── DB state at start ──"
sqlite3 "$DB" "
  SELECT 't0_pass:' || SUM(t0_pass) FROM runs;
  SELECT 't025_scored:' || COUNT(*) FROM runs WHERE t025_function_score IS NOT NULL;
  SELECT 't025_pending:' || COUNT(*) FROM runs WHERE t0_pass=1 AND t025_pass IS NULL;
  SELECT 't1_done:' || COUNT(*) FROM runs WHERE t1_pass IS NOT NULL;
  SELECT 't2_done:' || COUNT(*) FROM runs WHERE t2_pass IS NOT NULL;
" 2>/dev/null | tee -a "$RUN_LOG"

# ══════════════════════════════════════════════════════════════════════════════
# Phase T0.25 — Score all t0_pass=1 communities missing function_score
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_T025" == "false" ]]; then
  run_phase "T0.25 scoring (neon)" \
    python apps/bnf/scripts/run_neon_t025.py \
      --db "$DB" \
      --workers "$WORKERS" \
      --source neon

  # Also score mgnify communities if any are pending
  MGNIFY_PENDING=$(sqlite3 "$DB" "
    SELECT COUNT(*) FROM samples s JOIN runs r ON s.sample_id=r.sample_id
    WHERE s.source='mgnify' AND r.t0_pass=1 AND r.t025_pass IS NULL;" 2>/dev/null || echo 0)
  if [[ "$MGNIFY_PENDING" -gt 0 ]]; then
    run_phase "T0.25 scoring (mgnify)" \
      python apps/bnf/scripts/run_neon_t025.py \
        --db "$DB" \
        --workers "$WORKERS" \
        --source mgnify
  else
    log "⏭ No mgnify communities pending T0.25"
  fi
else
  log "⏭ Skipping T0.25 (--skip-t025)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase T1 — Community FBA on real amplicon communities
# Uses --real-mode: processes t0_pass=1 neon/mgnify communities (not t2_pass synthetic)
# Requires genus-level data in top_genera column.
# Note: 16S amplicon gives ~99% Unclassified at genus level — most communities
#       will be filtered. This runs on those that DO have genus assignments.
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_T1" == "false" ]]; then
  run_phase "T1 FBA (real amplicon communities)" \
    python scripts/legacy/t1_fba_batch.py \
      --db "$DB" \
      --workers "$WORKERS" \
      --real-mode \
      --n-communities 50000 \
      --model-dir /data/pipeline/models \
      --batch-size 20
else
  log "⏭ Skipping T1 (--skip-t1)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase T1-SYNTH — Bootstrap T1 on 220K synthetic communities
# Uses --t025-mode: processes t025_pass=1 synthetic communities regardless of
# t2_pass. This breaks the circular dependency where T1 requires t2_pass=1
# and T2 requires t1_pass=1. After this phase, T2 can run on t1_pass synthetics.
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_T1_SYNTH" == "false" ]]; then
  SYNTH_PENDING=$(sqlite3 "$DB" "
    SELECT COUNT(*) FROM runs r
    JOIN samples s ON r.sample_id = s.sample_id
    WHERE r.t025_pass=1 AND r.t1_pass IS NULL AND s.source='synthetic';" 2>/dev/null || echo 0)
  if [[ "$SYNTH_PENDING" -gt 0 ]]; then
    log "T1-SYNTH: $SYNTH_PENDING synthetic communities pending T1 bootstrap"
    run_phase "T1 FBA bootstrap (synthetic --t025-mode)" \
      python scripts/legacy/t1_fba_batch.py \
        --db "$DB" \
        --workers "$WORKERS" \
        --t025-mode \
        --n-communities 250000 \
        --model-dir /data/pipeline/models \
        --batch-size 20
  else
    log "⏭ No synthetic communities pending T1 bootstrap"
  fi
else
  log "⏭ Skipping T1-SYNTH (--skip-t1-synth)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase T2 — dFBA dynamics on all T1-pass communities
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_T2" == "false" ]]; then
  run_phase "T2 dFBA dynamics" \
    python scripts/legacy/t2_dfba_batch.py \
      --db "$DB" \
      --workers "$WORKERS" \
      --model-dir /data/pipeline/models \
      --days 45 \
      --batch-size 64
else
  log "⏭ Skipping T2 (--skip-t2)"
fi

# ══════════════════════════════════════════════════════════════════════════════
# Phase ANALYSIS — Parallel analysis + findings + FINDINGS.md
# ══════════════════════════════════════════════════════════════════════════════
if [[ "$SKIP_ANALYSIS" == "false" ]]; then
  log "━━━ PHASE: Analysis (parallel) ━━━"
  mkdir -p results/spatial

  python rank_candidates.py rank \
    --db "$DB" --config config.example.yaml \
    --output results/ranked_candidates.csv --top 1000 \
    2>&1 | tee -a "$RUN_LOG" &
  _PID_RANK=$!

  python spatial_analysis.py analyze \
    --db "$DB" --output-dir results/spatial/ --top 1000 --n-clusters 20 \
    2>&1 | tee -a "$RUN_LOG" &
  _PID_SPAT=$!

  python taxa_enrichment.py enrich \
    --db "$DB" --output results/taxa_enrichment.csv \
    2>&1 | tee -a "$RUN_LOG" &
  _PID_ENRICH=$!

  wait $_PID_RANK   && log "✓ rank_candidates" || log "⚠ rank_candidates failed (non-fatal)"
  wait $_PID_SPAT   && log "✓ spatial_analysis" || log "⚠ spatial_analysis failed (non-fatal)"
  wait $_PID_ENRICH && log "✓ taxa_enrichment" || log "⚠ taxa_enrichment failed (non-fatal)"

  run_phase "Generate Findings" \
    python scripts/legacy/generate_findings.py \
      --db "$DB" --results-dir results/

  if [[ -f findings_generator.py ]]; then
    run_phase "FINDINGS.md" python findings_generator.py --db "$DB"
  fi
else
  log "⏭ Skipping analysis (--skip-analysis)"
fi

# ── DB stats at end ───────────────────────────────────────────────────────────
log ""
log "── DB state after run ──"
sqlite3 "$DB" "
  SELECT 't025_scored:' || COUNT(*) FROM runs WHERE t025_function_score IS NOT NULL;
  SELECT 't1_done:' || COUNT(*) FROM runs WHERE t1_pass IS NOT NULL;
  SELECT 't2_done:' || COUNT(*) FROM runs WHERE t2_pass IS NOT NULL;
  SELECT 'findings:' || COUNT(*) FROM findings;
" 2>/dev/null | tee -a "$RUN_LOG"

log ""
log "Run log: $RUN_LOG"
log "Done."
