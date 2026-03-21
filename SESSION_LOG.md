# Session Log — Soil Microbiome Pipeline

---

## 2026-03-21 (Evening) — Circular Deadlock Diagnosed + T1-SYNTH Bootstrap

### What Was Done

**Discovery: T2→T1 circular deadlock in synthetic pipeline**
- `t1_fba_batch.py` default mode queries `WHERE t2_pass=1 AND t1_pass IS NULL`
- `t2_dfba_batch.py` queries `WHERE t1_pass=1 AND t2_pass IS NULL`
- The 220K synthetics have neither T1 nor T2 → neither script queues them → permanent deadlock
- The 3,378 that already have both T1+T2 were bootstrapped before this query structure was set
- Designed intent (from T1 docstring): initial T1 → T2 stability filter → refined T1 keystone

**Fix: `--t025-mode` added to `t1_fba_batch.py`**
- New `_fetch_communities` branch: `WHERE t025_pass=1 AND t1_pass IS NULL AND source='synthetic'`
- Ordered by `t025_function_score DESC` (highest-confidence communities first)
- Returns directly (no SILVA artifact filter — synthetics have clean top_genera)
- CLI flag: `--t025-mode`; mode label: "T0.25-seeded synthetic (bootstrap)"
- After this runs, T2 sees `t1_pass=1` communities and proceeds normally

**`run_real_data_funnel.sh` updated**
- New Phase: `T1-SYNTH` (between T1-real and T2)
- Checks pending count before running; skip flag `--skip-t1-synth`
- Runs `t1_fba_batch.py --t025-mode --n-communities 250000`
- New pipeline sequence: T0.25 → T1(real) → T1-SYNTH(synthetic) → T2 → analysis

**Geographic analysis: CONUS focus recommended**
- Queried site-level T0.25 score distribution across all 48 NEON sites
- **Non-CONUS dominates the top spots**: PUUM (Hawaii, avg 0.80), GUAN (Puerto Rico, avg 0.68), LAJA (Puerto Rico), Alaska sites (BARR, HEAL, DEJU, BONA, TOOL)
- PUUM scores 1.000 because: (1) tropical volcanic soils genuinely high BNF, (2) far outside training distribution → ML extrapolation at feature boundary
- Top CONUS sites by avg T0.25: NOGP/ND (0.414), DSNY/FL (0.408), ABBY/WA (0.391), LENO/MS (0.385), OAES/OK (0.378)
- Decision: focus reporting on 48 contiguous states (lat 24–50°N, lon 65–125°W)
- Rationale: model trained on continental literature data; PUUM/PR/ALASKA scores are extrapolation artifacts, not calibrated predictions; CONUS has direct agricultural relevance

### Bugs Fixed

| Bug | Fix | Commit |
|-----|-----|--------|
| T2→T1 circular deadlock (220K synthetics stuck) | `--t025-mode` in t1_fba_batch.py | (this session) |

### Git State
- See latest commit hash after push

### Next Run (unlock the synthetics)
```bash
ssh hetzner2 'cd /opt/pipeline && source .venv/bin/activate && \
  nohup bash scripts/ops/run_real_data_funnel.sh \
    --workers 36 \
    --skip-t025 --skip-t1 \
  > logs/synth_bootstrap_$(date +%Y%m%d_%H%M%S).log 2>&1 &'
```
This runs T1-SYNTH on 220K synthetics, then T2 on resulting T1-pass. Expected: several hours.

---

## 2026-03-21 — Bulk NEON Ingest + T0.25 Complete

### What Was Done

**Phase 1: Two-phase NEON 16S ingestion pipeline**
- Built `bulk_download.py`: downloads first 5MB of each R1 FASTQ via HTTP Range header, 50 workers, writes manifest TSV
- Modified `process_neon_16s.py`: added `--from-manifest` mode to skip download and read from local staged files
- Ran bulk download on hetzner2: 4,169 NEON samples → `/data/staging/neon_fastq/` (21GB, ~5MB each)
- Ran `process_neon_16s.py --from-manifest`: all 4,169 processed, 0 failures
- Result: NEON community count grew from ~13K → 17,567

**Phase 2: T0.25 ML scoring — fixed and ran**
- Discovered `run_neon_t025.py` was calling a broken stub (`run_t025_batch` in `core/engine.py`)
- Rewrote `run_neon_t025.py` to do real inference: loads `bnf_surrogate_regressor_v3.joblib`, builds phylum + env feature vectors, CLR-transforms, batch-predicts, writes `t025_function_score` + `t025_pass=1`
- Ran on 7,664 pending NEON communities → scored in 4 seconds (vectorized batch inference)
- Result: T0.25 coverage 99.9% (231,228/231,296 T0-pass)

**Phase 3: Funnel run script**
- Built `scripts/ops/run_real_data_funnel.sh`: chains T0.25 → T1(real-mode) → T2 → parallel analysis → findings
- Idempotent `--skip-t025`, `--skip-t1`, `--skip-t2`, `--skip-analysis` flags
- Ran end-to-end; T1 attempted 122 real communities (most genera unknown, 21/116 models built)
- T2 ran on 339 T1-pass with no T2 → 0 new (all were already saturated)
- Findings generated: 6 entries (5 top candidates + 1 summary)

### Bugs Fixed

| Bug | Fix | Commit |
|-----|-----|--------|
| `run_neon_t025.py` wrong sys.path (`parents[1]` = apps/bnf, not repo root) | `parents[3]` | 5f01d7d |
| `db_utils.py` shims excluded `_db_connect` (private, not in `*` exports) | explicit re-export | f31acba |
| `_db_connect()` missing `timeout` param (needed by `generate_findings.py`) | added default | 0bf68d6 |
| Server SSH key is read-only — can't push from server | rsync to local → commit → push from local | — |

### Git State
- Local + origin/main: `0bf68d6`
- Server: `0bf68d6` (in sync)
- DB: `/data/pipeline/db/soil_microbiome.db` on hetzner2

### What Remains (see FINDINGS.md Gaps 1–5)
1. **T2 at scale on 220K synthetic communities** — next big run (hours, not days)
2. T1 on resulting T2-pass synthetics
3. Real NEON T1 — architecture decision needed (AGORA2 vs accept T0.25-only)
4. FVA bounds verification
5. FINDINGS.md markdown auto-gen fix

---

## 2026-03-20 — Bootstrap + NEON 16S Processing

- Ran synthetic bootstrap generating 220,000 synthetic communities
- Initial NEON 16S processing: vsearch classification pipeline
- T0.25 partial run on 3,659/11,122 NEON communities (old run)

---
