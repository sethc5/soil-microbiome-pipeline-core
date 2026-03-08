#!/usr/bin/env bash
# continue_after_t1.sh — Run pipeline phases 5-8 after T1 FBA completes
# Usage: bash scripts/continue_after_t1.sh
set -euo pipefail

DB="/data/pipeline/db/soil_microbiome.db"
CONFIG="/opt/pipeline/config.example.yaml"
WORKERS=36
LOG="/var/log/pipeline/continue_$(date +%Y%m%d_%H%M%S).log"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG"; }

cd /opt/pipeline
source .venv/bin/activate

# ── Phase 5: Intervention Screening ──────────────────────────────────────────
log "━━━ PHASE 5: Intervention Screening ━━━"
python scripts/intervention_batch.py \
  --db "$DB" \
  --workers "$WORKERS" \
  --batch-size 50 \
  2>&1 | tee -a "$LOG"
log "✓ Intervention screening complete"

# ── Phase 6: Analysis Pipeline (4 modules in parallel) ───────────────────────
log "━━━ PHASE 6: Analysis Pipeline (parallel) ━━━"
mkdir -p results/spatial

python correlation_scanner.py \
  --config "$CONFIG" \
  --db "$DB" \
  --output results/correlation_findings.json \
  2>&1 | tee -a "$LOG" &
_PID_CORR=$!

python rank_candidates.py \
  --config "$CONFIG" \
  --db "$DB" \
  --top 1000 \
  --output results/ranked_candidates.csv \
  2>&1 | tee -a "$LOG" &
_PID_RANK=$!

python spatial_analysis.py \
  --db "$DB" \
  --top 1000 \
  --n-clusters 20 \
  --output-dir results/spatial/ \
  2>&1 | tee -a "$LOG" &
_PID_SPAT=$!

python taxa_enrichment.py \
  --db "$DB" \
  --output results/taxa_enrichment.csv \
  2>&1 | tee -a "$LOG" &
_PID_ENRICH=$!

log "Waiting for parallel analysis modules..."
wait $_PID_CORR   && log "✓ Correlation scan complete"  || log "✗ Correlation scan FAILED"
wait $_PID_RANK   && log "✓ Rank candidates complete"   || log "✗ Rank candidates FAILED"
wait $_PID_SPAT   && log "✓ Spatial analysis complete"  || log "✗ Spatial analysis FAILED"
wait $_PID_ENRICH && log "✓ Taxa enrichment complete"   || log "✗ Taxa enrichment FAILED"
log "✓ Analysis Pipeline parallel phase complete"

# Climate resilience + master summary
python scripts/analysis_pipeline.py \
  --db "$DB" \
  --out-dir results/ \
  --skip-correlations \
  --skip-ranking \
  --skip-spatial \
  --skip-enrichment \
  2>&1 | tee -a "$LOG"
log "✓ Climate resilience + summary complete"

# ── Phase 7: Generate Findings ───────────────────────────────────────────────
log "━━━ PHASE 7: Generate Findings ━━━"
python scripts/generate_findings.py \
  --db "$DB" \
  --results-dir results/ \
  2>&1 | tee -a "$LOG"
log "✓ Findings generated"

# ── Phase 8: Reports ─────────────────────────────────────────────────────────
log "━━━ PHASE 8: Generate Reports ━━━"
python findings_generator.py \
  --config "$CONFIG" \
  --db "$DB" \
  2>&1 | tee -a "$LOG" || log "⚠ FINDINGS.md generation failed"

python intervention_report.py \
  --config "$CONFIG" \
  --db "$DB" \
  --top 50 \
  2>&1 | tee -a "$LOG" || log "⚠ Intervention report failed"

log "━━━ ALL PHASES COMPLETE ━━━"
log "Results at: results/"
log "Log at: $LOG"
