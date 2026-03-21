# Session Log — Soil Microbiome Pipeline

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
