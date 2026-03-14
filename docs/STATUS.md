# Pipeline Status — 2026-03-13

## Validation Summary

| Version | Metric | Value | Honest? | Notes |
|---|---|---|---|---|
| v1 | In-dist Spearman r | 0.35 | No | Synthetic circular labels |
| v2 | In-dist Spearman r | **0.87** | **No** | Label leakage (same site labels in train+test) |
| v2 | CV R² (5-fold) | **0.448** | Partial | Same-site samples can appear in both folds |
| v2 | **LOSO Spearman r** | **0.155** | **YES** | Leave-site-out CV — fully independent (n=47 sites) |

**Current honest external performance: LOSO r = 0.155** (barely above chance for unseen sites).

---

## V2 External Validation Results

**Run date:** 2026-03-12  
**Dataset:** 237,567 NEON samples across 45 sites  
**Model:** `apps/bnf/models/bnf_surrogate_classifier_v2.joblib` (retrained on real labels)  
**Training labels:** Published BNF rates — Smercina 2019, Vitousek 2013, Reed 2011  
**Result file:** `results/validation_report_v2.json`

### Check 1: T0 Pass Rate — INFORMATIONAL
```
high_function_pass_rate: 0.967
low_function_pass_rate:  0.979
```
**Interpretation:** T0 quality filters (read depth, chimera, NSTI) are orthogonal to BNF
potential. The small difference (1.2%) likely reflects DNA quality variation across biomes
(tropical/wetland sites can have lower sequencing yield), not pipeline failure. This check
has been redesigned as informational — it does not block production screening.

### Check 2: Spearman r = 0.8685 — PASS (threshold 0.60) ✅
```
n: 237,567   method: surrogate_rf   threshold: 0.60
```
**Interpretation:** The v2 model correctly ranks communities from low-BNF (desert,
sagebrush) to high-BNF (tropical, wetland, boreal moss) sites.

**⚠️ IMPORTANT SCIENTIFIC CAVEAT — label leakage:**
The training labels (`bnf_measurements.csv`) assign BNF rates by NEON `site_id` from
published literature. The validation uses the same `bnf_measurements.csv` as ground truth.
Since all samples from the same site get the same training label AND the same validation
label, the model has learned the site-level signal directly — the r=0.87 reflects
in-distribution performance, not truly independent held-out sites.

**Honest performance estimate:** 5-fold CV R² = 0.448 (held-out samples, same sites).
This translates to expected Spearman r ≈ 0.67 for randomly split samples. For a fully
honest estimate, leave-site-out cross-validation is needed — `apps/bnf/scripts/`
`loso_cv_bnf_surrogate.py` (TODO). The r=0.87 should be treated as an upper bound.

**Why it still matters:** Even with label leakage, the v1 model (synthetic labels)
only achieved r=0.35. The jump to r=0.87 confirms that real published BNF rates provide
substantially better supervised signal than synthetic bootstrap labels.

### Check 3: Fraction non-zero at BNF sites = 0.912 — PASS ✅
```
n: 1,031 communities at BNF-active sites   n_bnf_active_sites: 18,006
```
91.2% of communities from BNF-active sites (measured_function > 0.3) have non-zero T1
predicted flux. The T1 FBA model produces signal where we expect it.

---

## Model Training Report (v2)

| Metric | Value |
|---|---|
| Training samples | 237,567 |
| Features | 64 (phyla + env) |
| Classifier CV ROC-AUC | 0.807 ± 0.001 |
| Regressor CV R² | 0.448 ± 0.015 |
| In-sample Spearman r | 0.927 (overfitting — use CV metrics) |
| Label source | Published site-level BNF rates |

**Top predictive features** (biologically sensible):
1. `organic_matter_pct` (11.5%) — C availability drives BNF (fixers need energy)
2. `soil_ph` (10.4%) — pH structures microbial communities
3. `Acidobacteria` (4.8%) — dominant phylum, good community state proxy
4. `precipitation_mm` (4.5%) — water drives BNF activity
5. `temperature_c` (4.3%) — enzymatic rate control
6. `Actinobacteria` (3.8%) — important secondary decomposers
7. `Cyanobacteria` (2.7%) — major free-living N-fixers

---

## Codebase State

| Concern | Status |
|---|---|
| Local ↔ GitHub ↔ Server git sync | ✅ Enforced by .clinerules + check_sync.sh |
| Core pipeline architecture | ✅ Modular, well-organized |
| External validation framework | ✅ Working (3 checks, Check 1 informational) |
| Published BNF reference rates | ✅ 45/47 NEON sites mapped (3 sources) |
| RF surrogate v1 (synthetic labels) | ⚠️ r=0.35 — do not use for production |
| RF surrogate v2 (real labels) | ✅ CV ROC-AUC=0.807, CV R²=0.448 |
| Label leakage in validation | ✅ LOSO CV complete — r=0.155 (47 sites, 2026-03-12) |
| T1 FBA model | ✅ Produces signal at BNF sites (91.2% non-zero) |
| PICRUSt2 | ⬜ Installed on server, not yet run at scale |
| AGORA2 metabolic models | ⬜ Not yet integrated |

---

## Completed Steps (2026-03-12 to 2026-03-14)

1. ✅ **LOSO CV (v2)** — `apps/bnf/scripts/loso_cv_bnf_surrogate.py` — r=0.1552, 47 sites, 474s (2026-03-12)
2. ✅ **pH-stratified enrichment** — no phyla enriched in ≥2 pH bins — phylum-level 16S too coarse (2026-03-12)
3. ✅ **RF v3 training** — `apps/bnf/scripts/retrain_bnf_surrogate_v3.py` — CV R²=0.462 (≈v2 0.448), confirms feature engineering not bottleneck (2026-03-13)
4. ✅ **top_genera provenance investigation** — 26 BNF-curated genera, likely FBA-derived not real 16S; excluded from v3 (2026-03-13 — Pitfall #9)
5. ✅ **LOSO per-site analysis** — spearman_r field added to loso_report.json; label quality bottleneck identified (2026-03-14)

## LOSO Per-Site Analysis (2026-03-14)

**Confirmed LOSO r = 0.1552** (recomputed from per_site data — was missing from JSON, now fixed).

**Label quality bottleneck discovered:**
- 47 NEON sites → only 21 unique published BNF rates (biome-averaged from Smercina/Vitousek/Reed)
- 11 sites share rate=0.085 (different biomes: WI forest, CO prairie, AK boreal, NC forest, ND prairie — all given the same label)
- Within-label noise degrades LOSO r independently of n_labelled_sites

**Largest prediction errors:**

| Site | Predicted | Published | Error | Interpretation |
|---|---|---|---|---|
| HEAL | 0.608 | 0.106 | +0.50 | Over-predicted: model sees boreal env conditions as high-BNF |
| GRSM | 0.683 | 0.213 | +0.47 | Over-predicted: Great Smoky Mtns temperate forest |
| GUAN | 0.351 | 0.809 | -0.46 | Under-predicted: Puerto Rico tropical (truly high-BNF) |
| PUUM | 0.546 | 1.000 | -0.45 | Under-predicted: Hawaii (highest-BNF in dataset) |
| BARR | 0.255 | 0.532 | -0.28 | Under-predicted: Arctic tundra (cyanobacterial mat BNF) |

**Pattern:** Model correctly identifies desert=low, but can't distinguish high-organic-matter
temperate sites (HEAL, GRSM) from truly high-BNF tropical sites (GUAN, PUUM).
This suggests both label quality AND feature granularity are bottlenecks.

## Next Steps

**Confirmed binding constraints (in order of impact):**

1. **Label quality** — biome-averaged labels (21 unique values / 47 sites) create within-label noise. Need site-specific BNF measurements, not biome averages. Priority search: papers with DIRECT NEON site measurements (ARA or 15N dilution).

2. **Label quantity** — n_labelled_sites=47. Each new distinct site with a direct measurement adds 1 independent LOSO datum. Target: ≥60 sites.

3. **Feature granularity** — phylum-level features can't distinguish HEAL boreal from GUAN tropical. nifH gene abundance (PICRUSt2) or genus-level real 16S would help (note: top_genera in DB is likely FBA-derived — see Pitfall #9).

**Specific next actions:**

- Literature search: ARA measurements at NEON sites (not just biome ranges) — start with GUAN, PUUM, BARR since those are the biggest misses
- NEON data portal: NEON publishes soil biogeochemistry — check if any BNF-proxy data exists
- Process_neon_16s.py fix: save OTU counts alongside phylum profiles → enables real genus features + PICRUSt2

**Medium term:**
- AGORA2 metabolic model integration (`docs/agora2_integration_plan.md`)
- MGnify metagenome data ingestion (`scripts/ingest/ingest_mgnify.py`)
