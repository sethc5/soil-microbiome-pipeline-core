# Pipeline Findings — Soil Microbiome BNF Screen
_Last updated: 2026-03-21 (post bulk-NEON ingest + T0.25 rerun)_

---

## Pipeline Funnel State

| Stage | Count | % of prior | Notes |
|-------|-------|-----------|-------|
| Total communities | 237,662 | — | 220K synthetic + 17.5K NEON + 95 mgnify |
| T0 pass | 231,296 | 97.3% | quality + metadata filter |
| T0.25 scored | 231,228 | 99.9% | ML BNF surrogate v3 ✓ complete |
| T0.25 score ≥ 0.7 | 70,628 | 30.6% | top-third by predicted BNF |
| T1 FBA done | 6,298 | 2.7% | metabolic models built + flux solved |
| T1 pass | 4,830 | 76.7% of T1 | feasible BNF flux |
| T2 dFBA done | 3,378 | 69.9% of T1-pass | dynamics simulated |
| **T1 pending** | **225,039** | — | **← the next major run** |

### By Source
| Source | Total | T0.25 scored | T1 done | T1 pending |
|--------|-------|-------------|---------|-----------|
| Synthetic | 220,000 | ~220K | ~6,200 | ~214,000 |
| NEON | 17,567 | 17,567 | ~122 attempted | ~17,400 |
| mgnify | 95 | 95 | <10 | ~85 |

---

## Top Candidates (T1 + T2 validated)

All top candidates are real NEON amplicon communities with metabolic flux (T1) and dFBA stability (T2) ≥ 0.97.

| community_id | sample_id | site | T0.25 | T1 flux | T2 stability |
|---|---|---|---|---|---|
| 445137 | GUAN_042-M-20220907 | GUAN | 0.500 | 50.0 | **0.977** |
| 442242 | KONZ_042-M-20180717 | KONZ | 0.500 | 50.0 | 0.972 |
| 443844 | BONA_084-O-20200916 | BONA | 0.500 | 50.0 | 0.972 |
| 442342 | WOOD_045-O-20180723 | WOOD | 0.500 | 50.0 | 0.970 |
| 442553 | SJER_048-M-20190313 | SJER | 0.500 | 50.0 | 0.968 |

**Note on T1 flux ceiling**: all top candidates hit the 50.0 cap — T1 is flux-bound at the model constraint limit. FVA (flux variability analysis) is needed to get meaningful bounds. See Gaps section.

---

## Top NEON Communities by T0.25 ML Score (not yet T1-validated)

| sample_id | site | T0.25 | lat/lon | note |
|---|---|---|---|---|
| PUUM_031-O-9-28 | PUUM | **1.000** | 19.55°N 155.32°W | Hawaii — tropical volcanic |
| PUUM_031-M-22.5-2.5 | PUUM | **1.000** | 19.55°N 155.32°W | |
| PUUM_014-O-18-30 | PUUM | **1.000** | 19.55°N 155.32°W | |
| *(+hundreds more PUUM)* | PUUM | 1.000 | | |
| KONZ, TALL, BLAN, OSBS… | various | 0.7–0.9 | CONUS | |

**PUUM = Pu'u Maka'ala Natural Area Reserve, Hawai'i Island** — tropical volcanic soil, known for exceptionally high free-living BNF (literature: 5–50 kg N/ha/yr). The ML model strongly flags this. These communities cannot advance to T1 until genus-level taxonomy is resolved (see Gaps).

---

## T0.25 Model Summary

- **Model**: Random Forest regressor v3, 68 features (59 phyla + 9 env covariates)
- **Training**: 237,567 communities, labels from published BNF rates (Smercina 2019)
- **Top features**: `organic_matter_pct`, `soil_ph`, `Acidobacteria`, `precipitation_mm`, `temperature_c`
- **CLR transform**: applied; `apps/bnf/models/retrain_report_v3.json` has full metrics

Phyla most enriched in top-10% high-scoring communities:
- **Nitrososphaerota** (8.15×), **Deinococcota** (6.02×), **Thermomicrobiota** (5.91×)
- **Nitrospirota** (2.05×), **Gemmatimonadota** (1.76×)

Phyla depleted in high-scoring communities:
- **Acidobacteriota** (0.37×), **Proteobacteria/Pseudomonadota** (0.44×), **Bacteroidota** (0.57×)

---

## Key Gaps and Next Runs

### Gap 1 — T1/T2 at scale on synthetic communities (~214,000 pending) ← NEXT RUN
Synthetic communities have T0.25 scores but T2 hasn't run on most (only 3,378/220K done).
The pipeline order for synthetics is: **T2 dFBA first → T1 FBA on T2-pass**.

```bash
# Run T2 on all T0.25-scored synthetics, then T1 on new T2-pass:
ssh hetzner2 'cd /opt/pipeline && source .venv/bin/activate && \
  nohup bash scripts/ops/run_real_data_funnel.sh --workers 36 --skip-t025 \
  > logs/real_data_funnel_scale.log 2>&1 &'
```
**Expected yield**: 10–30% T2 pass rate → 20–60K new T1/T2 validated candidates.

### Gap 2 — T2 on 339 existing T1-pass lacking T2 ← quick win
Already handled by `run_real_data_funnel.sh --skip-t025 --skip-t1`.

### Gap 3 — Real NEON T1 (BLOCKED — needs architecture decision)
16S amplicon classifies to phylum. T1 FBA requires genus-level metabolic models (21 pre-built SBML in `/data/pipeline/models/`). Options ranked by effort:
1. **Accept T0.25-only reporting** for NEON communities — scientifically valid for screening
2. **AGORA2/MICOM** — 16S-compatible community metabolic modeling, no genus needed
3. **Paired metagenomics** — some NEON sites have shotgun data for true genus assignment

### Gap 4 — FVA bounds for T1 flux confidence
All current T1 top candidates hit the 50.0 flux ceiling. `t1_fba_batch.py` has `--fva` flag — needs verification that it runs and populates `t1_flux_lower_bound`/`t1_flux_upper_bound`.

### Gap 5 — FINDINGS.md auto-generation from DB
`generate_findings.py` writes to DB but `findings_generator.py` (markdown report module) is not importable from legacy path. Fix: add to `scripts/legacy/` or update the import path in `generate_findings.py`.

---

## Infrastructure Built 2026-03-21

| Script | Purpose |
|--------|---------|
| `scripts/ingest/bulk_download.py` | Bulk partial FASTQ downloader (Range header, 5MB/file, 50 workers) |
| `scripts/ingest/process_neon_16s.py` | Added `--from-manifest` mode — reads local FASTQs, skips download |
| `apps/bnf/scripts/run_neon_t025.py` | **Rewrote**: real ML inference with v3 surrogate (was a no-op stub) |
| `scripts/ops/run_real_data_funnel.sh` | Chains T0.25→T1→T2→analysis; idempotent `--skip-*` flags |

**Bugs fixed (all on `main`)**:
- `run_neon_t025.py`: `parents[1]` → `parents[3]` for correct repo root
- `db_utils.py` root + scripts shims: explicit `_db_connect` re-export (private, excluded from `*`)
- `core/db_utils._db_connect()`: added `timeout` parameter

---

## Confidence Summary

| Layer | Coverage | Confidence | Status |
|-------|----------|-----------|--------|
| T0 filter | 97.3% | HIGH | ✓ done |
| T0.25 ML score | 99.9% | MEDIUM | ✓ done |
| T1 FBA | 2.7% | MEDIUM (flux cap) | ⏳ scale-up needed |
| T2 dFBA | 1.4% | MEDIUM | ⏳ scale-up needed |
| Real NEON T1 | ~0% | — | 🚫 genus data blocked |

**Not over** — pipeline is at the T0.25 → scale-up inflection point. The T1/T2 machinery is working; it just needs to be run on the 214K synthetic communities that have passed T0.25.
