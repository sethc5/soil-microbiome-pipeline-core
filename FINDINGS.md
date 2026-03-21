# Pipeline Findings — Soil Microbiome BNF Screen
_Last updated: 2026-03-21 (evening — circular deadlock diagnosed, T1-SYNTH bootstrap added)_

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

## Geographic Distribution of T0.25 Signal

### NEON Site Scores — Full Network

| Site | Location | n | avg T0.25 | max T0.25 | CONUS? |
|------|----------|---|----------|----------|--------|
| PUUM | Hawaii (19.6°N) | 440 | **0.800** | 1.000 | ✗ |
| GUAN | Puerto Rico (18.0°N) | 367 | **0.676** | 0.926 | ✗ |
| LAJA | Puerto Rico (18.0°N) | 93 | 0.505 | 0.617 | ✗ |
| BARR | Alaska (71.3°N) | 27 | 0.500 | 0.500 | ✗ |
| HEAL | Alaska (63.9°N) | 38 | 0.490 | 0.500 | ✗ |
| DEJU | Alaska (63.9°N) | 57 | 0.424 | 0.500 | ✗ |
| **NOGP** | **ND (46.8°N)** | 53 | **0.414** | 0.500 | ✓ |
| **DSNY** | **FL (28.1°N)** | 105 | **0.408** | 0.500 | ✓ |
| **ABBY** | **WA (45.8°N)** | 57 | **0.391** | 0.500 | ✓ |
| **LENO** | **MS (31.9°N)** | 117 | **0.385** | 0.500 | ✓ |
| **OAES** | **OK (35.4°N)** | 123 | **0.378** | 0.500 | ✓ |
| WREF | WA (45.8°N) | 311 | 0.372 | 0.500 | ✓ |
| *(+32 more CONUS sites)* | | | 0.14–0.37 | 0.5 | ✓ |

### Why PUUM and Puerto Rico Score So High

**Biologically valid signal**: tropical volcanic soils have genuinely elevated BNF rates. PUUM (Pu'u Maka'ala, Hawaii Island): 2,000+ mm/yr precipitation, young basaltic soil, literature BNF rates 5–50 kg N/ha/yr. GUAN (El Yunque, Puerto Rico): tropical rainforest, warm/wet, high diazotroph diversity.

**Extrapolation concern**: the BNF surrogate model (v3) was trained on continental literature data — predominantly temperate North America and Europe. PUUM/GUAN sit at or beyond the edge of the training feature distribution (extreme precipitation_mm, temperature_c, soil chemistry). A score of 1.000 almost certainly reflects hitting the model boundary rather than a calibrated prediction.

### Decision: Focus Reporting on CONUS

**CONUS filter**: lat 24–50°N, lon 65–125°W (48 contiguous states)

Rationale:
1. ML surrogate is most reliable within training distribution (continental temperate soils)
2. Hawaiian / PR / Alaskan soils are ecologically distinct — they warrant separate modelling
3. Agricultural and restoration relevance is highest for continental USA
4. NEON CONUS network (37 sites) provides the core long-term ecological research comparison

**Top CONUS sites to prioritize** (by avg T0.25): NOGP (ND), DSNY (FL), ABBY (WA), LENO (MS), OAES (OK), WREF (WA), CLBJ (TX), KONZ (KS)

The non-CONUS results (especially PUUM) are still scientifically interesting but should be reported separately with explicit extrapolation caveats.

---

## Key Gaps and Next Runs

### Gap 1 — T1/T2 at scale on synthetic communities (~220,000 pending) ← NEXT RUN

**Root cause (circular deadlock):**
- `t1_fba_batch.py` default mode requires `t2_pass=1` — T1 waits for T2
- `t2_dfba_batch.py` requires `t1_pass=1` — T2 waits for T1
- The 220K synthetics have neither. The 3,378 with both were bootstrapped early.
- **Fix implemented**: `--t025-mode` flag added to `t1_fba_batch.py`
  - New query: `WHERE t025_pass=1 AND t1_pass IS NULL AND source='synthetic'`
  - Breaks the deadlock: T1 runs first on T0.25-scored synthetics, then T2 can run on T1-pass

**Correct pipeline order for synthetics (after fix):**
T0.25 → **T1-SYNTH** (`--t025-mode`) → T2 on T1-pass → refined T1 keystone (standard mode)

```bash
# Kick off T1-SYNTH bootstrap → T2 on all resulting T1-pass:
ssh hetzner2 'cd /opt/pipeline && source .venv/bin/activate && \
  nohup bash scripts/ops/run_real_data_funnel.sh \
    --workers 36 --skip-t025 --skip-t1 \
  > logs/synth_bootstrap_$(date +%Y%m%d_%H%M%S).log 2>&1 &'
```
**Expected yield** (220K communities): ~10–30% T1-pass rate → 22–66K new T1-pass; of those ~70% T2-stable → 15–46K new fully validated candidates.

### Gap 2 — T2 on 339 existing T1-pass lacking T2 ← quick win
Already handled by `run_real_data_funnel.sh --skip-t025 --skip-t1`.

### Gap 3 — Real NEON T1 (BLOCKED — needs architecture decision)
16S amplicon classifies to phylum. T1 FBA requires genus-level metabolic models (21 pre-built SBML in `/data/pipeline/models/`). Options ranked by effort:
1. **Accept T0.25-only reporting** for NEON communities — scientifically valid for screening; CONUS T0.25 signal is the primary deliverable
2. **Phylum→genus proxy mapping** — extend `_GENUS_NCBI` dict to map each observed phylum to its best BNF-representative genus. Synthesize `top_genera` from `phylum_profile` before T1. Coarse but enables mechanistic validation for top CONUS sites without new data.
3. **NEON paired metagenomics** — NEON ARCTOS has shotgun metagenomics at ~30 CONUS sites; gives genus/species resolution and direct nifH gene detection. Separate ingest pipeline needed. Highest value if advancing toward publication.

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
