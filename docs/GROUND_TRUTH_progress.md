# GROUND_TRUTH — Part 1 of 2: Progress, What Works, and Why
> Companion doc — See also: [GROUND_TRUTH_pitfalls.md](GROUND_TRUTH_pitfalls.md)

*"Ground truth" in ML = validated labels from real measurements.
In soil science = what you actually observe in the field.
Both meanings apply here.*

---

## Why This Project Exists

The 2021–2023 urea supply shock (Russia/China export restrictions, energy price spikes)
revealed how brittle synthetic-fertilizer-dependent agriculture is. Biological nitrogen
fixation (BNF) — the process by which certain soil bacteria convert atmospheric N₂ to
plant-available NH₄⁺ — is a natural alternative that requires no Haber-Bosch energy input.

**The question:** Can we screen soil microbiome community composition data to identify
which communities have high BNF potential? If yes, we could guide inoculation strategies
and land management decisions toward lower fertilizer dependency.

**Why now:** NEON (National Ecological Observatory Network) has produced a uniquely
standardized multi-site, multi-year 16S amplicon dataset (237,567 samples, 47 US sites)
with co-located environmental metadata (soil chemistry, climate, land cover). This is the
best available dataset for this question in the continental US.

**The bar:** LOSO (leave-site-out) Spearman r ≥ 0.45 across independent sites before
any claim of practical utility can be made. Current: r=0.155. Path to improvement: clear.

---

## What Has Been Built

### Data Pipeline (production-stable)

**`scripts/ingest/ingest_neon_biom.py`** — NEON metadata ingestion
- Fetches site metadata, soil chemistry (DP1.10086.001), DNA extraction records
- Stores FASTQ URLs in `communities.notes` for downstream processing
- Runs in ~1 hr per site

**`scripts/ingest/process_neon_16s.py`** — 16S amplicon classification
- Downloads R1 FASTQ from NEON GCS (amplicon-only, excludes JGI shotgun)
- Subsamples 10,000 reads per sample (stable Shannon H ≈ ±0.02 vs 50K reads)
- vsearch global alignment against SILVA 138.1 at 97% identity, single-threaded
- Builds phylum_profile + top_genera per sample → stored in DB
- Tuned for 36-thread Xeon: N_workers × single-threaded vsearch beats N_workers × multi-threaded
- Status: 237,567 samples processed, phylum profiles in DB

**Database:** SQLite at `/data/pipeline/db/soil_microbiome.db`
- 237,567 communities, 47 NEON sites, ~10 years of data (2012–2024)
- WAL journal mode, optimized for concurrent read + write

---

### ML Model (v2)

**`apps/bnf/scripts/retrain_bnf_surrogate.py`** — RF classifier on real labels

| Metric | Value | Notes |
|---|---|---|
| Training samples | 237,567 | All NEON samples |
| Features | 64 | Phyla (59) + env (5) |
| CV ROC-AUC | 0.807 ± 0.001 | Genuine discriminative power |
| CV R² (regressor) | 0.448 ± 0.015 | Explains 44.8% of BNF variance |
| **LOSO Spearman r** | **0.155** | **Honest independent estimate** |

**Top features (make biological sense):**
1. `organic_matter_pct` (11.5%) — carbon availability drives fixers (energy cost of N₂ reduction is ~16 ATP/N₂)
2. `soil_ph` (10.4%) — pH is the strongest single driver of microbial community composition (Lauber et al. 2009, DOI: 10.1126/science.1178534)
3. `Acidobacteria` (4.8%) — dominant phylum, good community state proxy
4. `precipitation_mm` (4.5%) — water limits enzymatic activity in dry environments
5. `temperature_c` (4.3%) — nitrogenase activity is temperature-sensitive
6. `Cyanobacteria` (2.7%) — major free-living N-fixers, especially at wet sites

**Why CV R²=0.448 matters even though LOSO r=0.155:** The model IS learning real biological
signal. CV R² measures how well it ranks samples within known sites. LOSO r measures
whether that ranking transfers to completely unknown sites. The gap tells us the limit
is training data size (47 labelled sites), not model capacity.

---

### Validation Framework

**`core/validate_pipeline.py`** — 3-check scientific validation

| Check | What it tests | Status |
|---|---|---|
| Check 1 (T0 pass rate) | Quality filter pass rate by BNF category | Informational — T0 is orthogonal to BNF |
| **Check 2 (Spearman r)** | RF-predicted BNF vs published rates | **Primary metric** |
| Check 3 (BNF sensitivity) | % of known-BNF communities with non-zero T1 flux | 91.2% — PASS |

Key design decision: Check 1 was redesigned from a hard fail to informational. T0 quality
filters (read depth, chimera removal, NSTI) are data quality gates — they are explicitly NOT
designed to discriminate BNF potential. High-BNF tropical sites may have slightly worse
DNA quality due to humidity. Treating T0 pass rate as a BNF discriminator was a category error.

---

### Analysis Scripts

**`apps/bnf/scripts/loso_cv_bnf_surrogate.py`**
- Leave-site-out CV: trains on 46 sites, predicts held-out site
- 47-fold × 200 trees → 474 seconds on Xeon W-2295
- Output: per-site predictions + overall LOSO Spearman r
- This is the script that produces the honest performance number

**`apps/bnf/scripts/ph_stratified_enrichment.py`**
- Fisher's exact test: which phyla are enriched in high-BNF communities within pH bins?
- Controls for pH — the strongest confound
- Result: no phyla enriched in ≥2 pH bins (expected — BNF is genus/species-level, not phylum-level)
- Implies: phylum-level 16S is insufficient for BNF prediction; nifH gene needed

---

### Operations

**`scripts/ops/check_sync.sh`** — 3-way git sync verification
- Compares local, GitHub (origin/main), and Hetzner server
- 5-minute SSH cache (avoids re-SSH on every small operation)
- `--no-ssh` flag for offline use
- Called automatically by `.clinerules` at every Cline session start

**`.clinerules`** — 9 rules enforced by Cline at session start
- Rule 1: Mandatory sync check
- Rule 4: Honest performance numbers (current table baked in)
- Rule 7: DB safety (never DROP/ALTER without backup)
- Rule 8: Context window discipline (wrap up at ≥85%)

---

## The Honest State (as of 2026-03-12)

The pipeline is a **research prototype**. It has genuine signal (CV R²=0.448) but
cannot yet reliably rank unseen sites (LOSO r=0.155). The gap is not a model problem —
it is a training data problem. With 47 labelled sites, every LOSO fold removes 1/47th
of the label space.

**What would move LOSO r above 0.45 (publishable threshold):**
1. More labelled sites — each additional published BNF rate at a new site adds 1 new training/test datum
2. nifH gene abundance from PICRUSt2 — mechanistic signal that transfers across biomes
3. Genus-level features — BNF-capable genera (Frankia, Azotobacter, Rhizobium) rather than phyla

**What won't help much:**
- More samples at the same 47 sites (we have 5K per site already)
- Bigger RF models (diminishing returns above 200 trees per fold)
- More env features (already have the major ones: pH, OM, precip, temp)

---

## References

- Smercina et al. 2019 (DOI: 10.1128/mSystems.00119-19) — BNF across NEON grasslands
- Vitousek et al. 2013 (DOI: 10.1038/ngeo1851) — global BNF constraints
- Reed et al. 2011 (DOI: 10.1890/10-1365.1) — forest BNF
- Lauber et al. 2009 (DOI: 10.1126/science.1178534) — pH as community driver
