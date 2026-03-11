# hetzner2 Server Inventory

**Generated:** 2026-03-11 17:17 CST  
**Host:** `144.76.222.125` (alias `hetzner2`)  
**Commit in sync:** `2db435a` — verified on both local and `/opt/pipeline`

---

## 1 — System

| Property | Value |
|---|---|
| OS | Ubuntu 24.04 LTS (`Ubuntu-2404-noble-amd64-base`) |
| Kernel | 6.8.0-90-generic |
| CPU | Intel Xeon W-2295 @ 3.00 GHz (18 cores) |
| RAM | 251 GB total · 3.0 GB used · 248 GB available |
| Swap | 4.0 GB · 25 MB used |
| Root filesystem | `/dev/md2` (RAID) — 875 GB total · 21 GB used · 810 GB free (3%) |
| Uptime | 4 days 21 hours (booted ~2026-03-06) |
| Load avg | 0.00 / 0.31 / 3.05 (idle since overnight run completed) |

---

## 2 — Code Sync

| Location | HEAD commit | Status |
|---|---|---|
| Local `/home/seth/dev/soil_microbiome_core` | `2db435a` | ✅ |
| hetzner2 `/opt/pipeline` | `2db435a` | ✅ in sync |

Pipeline runs from `/opt/pipeline` using `/opt/pipeline/.venv/bin/python3` (Python 3.12.3).

---

## 3 — Database

**File:** `/data/pipeline/db/soil_microbiome.db`  
**Size:** 528 MB (135,049 pages × 4,096 B)

### Table row counts

| Table | Rows |
|---|---|
| `communities` | 237,662 |
| `runs` | 237,662 |
| `interventions` | 100,000 |
| `receipts` | 3 |
| `t1_results` | (not a separate table — stored in communities) |
| `t2_results` | (not a separate table — stored in communities) |

### Pass funnel (as of 2026-03-11 ~17:00 CST)

| Source | Total | t0_pass | t025_pass | t1_pass | t2_pass |
|---|---|---|---|---|---|
| NEON | ~17,567 | 11,026 | — | ~4,830 | ~3,378 |
| MGnify | ~95 | 95 | — | — | — |
| Synthetic | ~220,000 | 220,000 | — | — | — |
| **Total** | **237,662** | **231,121** | **—** | **~4,830** | **~3,378** |

> t025_pass not yet populated — `run_t025_batch()` is wired in `pipeline_core.py:1070` but `functional_predictor.joblib` must be deployed to `/opt/pipeline/models/` for it to run (see §5).

---

## 4 — Reference Data  `/data/pipeline/ref/`

| Resource | Size | Purpose |
|---|---|---|
| `16S_blast_db/` | 358 MB | vsearch SILVA 138 BLAST-format DB for 16S classification |
| `16S_raw.fasta` | 41 MB | SILVA 138 SSU raw sequences |
| `16S_ref.fasta` | 40 MB | Trimmed SILVA 138 V4 reference sequences |
| `16S_taxids.tsv` | 516 KB | SILVA accession → taxonomy mapping |
| `taxdump/` | 1.1 GB | NCBI taxonomy dump (nodes.dmp, names.dmp, etc.) used by genome_fetcher and NCBI mapping |
| **Total** | **~1.5 GB** | |

---

## 5 — Models

### `/data/pipeline/models/` — AGORA2 SBML metabolic models (247 MB)

20 genera on disk (+ `.bak` copies of patched diazotroph models):

| Genus | Size | Patched (nitrogenase added) |
|---|---|---|
| Acidobacterium | 6.9 MB | — |
| Azoarcus | 7.9 MB | ✅ (Mar 10) |
| Azospirillum | 8.7 MB | ✅ (Mar 10) |
| Azotobacter | 8.8 MB | ✅ (Mar 10) |
| Bacillus | 5.7 MB | — |
| Bradyrhizobium | 9.4 MB | ✅ (Mar 10) |
| Burkholderia | 11 MB | ✅ (Mar 10) |
| Caulobacter | 6.3 MB | — |
| Ellin | 6.9 MB | — |
| Gemmata | 7.0 MB | — |
| Gluconacetobacter | 8.3 MB | — |
| Herbaspirillum | 6.1 MB | ✅ (Mar 10) |
| Mesorhizobium | 9.2 MB | ✅ (Mar 10) |
| Nitrosomonas | 6.0 MB | — |
| Nitrospira | 6.5 MB | — |
| Pseudomonas | 8.9 MB | — |
| Rhizobium | 9.4 MB | ✅ (Mar 10) |
| Sinorhizobium | 9.2 MB | ✅ (Mar 10) |
| Sphingomonas | 9.5 MB | — |
| Streptomyces | 9.0 MB | — |
| Variovorax | 9.2 MB | — |

> **Gap:** `_GENUS_NCBI` in `t1_fba_batch.py` now lists 100+ genera, but only these 20 have SBML files on disk. NEON genera not here fall through to CarveMe (which requires NCBI taxon ID — now populated). CarveMe models will be written to `/data/pipeline/models/` as they are built.

### `/opt/pipeline/models/` — Surrogate ML models (66 MB)

| File | Size | Contents |
|---|---|---|
| `bnf_surrogate_classifier.joblib` | 16 MB | RF classifier gate (ROC-AUC 0.812, OOB 0.772) |
| `bnf_surrogate_regressor.joblib` | 18 MB | RF regressor (R² 0.465, OOB 0.469) |
| `functional_predictor.joblib` | 33 MB | Combined `FunctionalPredictor` wrapper (loaded by `pipeline_core.py:940`) |
| `README.md` | — | Model provenance notes |

> Trained on 5,907 real communities (`cf5e081`). Top features: soil_pH (42%), Nitrososphaerota (19%), Nitrospirota (12%). `pipeline_core.py` loads from `models/functional_predictor.joblib` relative to CWD (`/opt/pipeline`) — path resolves correctly.

---

## 6 — Proteome Cache  `/data/pipeline/proteome_cache/`  (63 MB, 42 files)

21 genera with `.faa` (FASTA proteome) + `.tsv` (annotation table):

Acidobacterium, Azoarcus, Azospirillum, Azotobacter, Bacillus, Bradyrhizobium, Burkholderia, Caulobacter, Ellin, Gemmata, Gluconacetobacter, Herbaspirillum, Mesorhizobium, Nitrosomonas, Nitrospira, Pseudomonas, Rhizobium, Sinorhizobium, Sphingomonas, Streptomyces, Variovorax

Used by `genome_fetcher.py` to avoid repeated NCBI proteome downloads during CarveMe model building.

---

## 7 — Results `/data/pipeline/results/`  (12 MB total)

| File | Size | Generated | Contents |
|---|---|---|---|
| `ranked_candidates.csv` | 169 KB | Mar 8 17:08 | Top-100 BNF communities with scores |
| `spatial/spatial_communities.csv` | 126 KB | Mar 8 17:08 | Per-community GPS + BNF flux for kriging |
| `spatial/spatial_clusters.csv` | 1.2 KB | Mar 8 17:08 | Cluster assignments |
| `spatial/spatial_map.png` | 73 KB | Mar 8 17:08 | CONUS kriging heatmap (6,413-point grid) |
| `keystone_analysis.csv` | 1.4 MB | Mar 8 17:53 | Keystone taxa per community |
| `keystone_organism_summary.csv` | 493 B | Mar 8 17:53 | Cross-community keystone frequency |
| `intervention_portfolio.csv` | 1.7 MB | Mar 8 17:53 | Per-community intervention assignments |
| `intervention_type_summary.csv` | 383 B | Mar 8 17:53 | Intervention type breakdown |
| `intervention_report.json` | 3.5 KB | Mar 8 17:17 | 11 intervention recommendations (JSON) |
| `intervention_report.md` | 3.4 KB | Mar 8 17:17 | 11 intervention recommendations (Markdown) |
| `fva_uncertainty.csv` | 2.3 MB | Mar 8 17:53 | FVA min/max per community |
| `bnf_trajectory_summary.csv` | 3.4 MB | Mar 8 17:33 | dFBA 90-day trajectories |
| `taxa_enrichment.csv` | 2.9 KB | Mar 8 17:29 | Enriched taxa in BNF vs non-BNF communities |
| `climate_resilience.csv` | 2.5 MB | Mar 8 07:13 | Climate perturbation stability scores |
| `correlation_findings.json` | 669 B | Mar 8 17:11 | Correlation scan results |
| `funnel_analysis.json` | 1.0 KB | Mar 8 17:53 | Pass/fail counts per tier |
| `analysis_summary.json` | 340 B | Mar 8 07:13 | Summary stats |
| `mgnify_ftp_checkpoint.json` | 12 KB | Mar 9 15:30 | MGnify FTP ingest progress |
| `mgnify_ftp_ingest.log` | 2.4 KB | Mar 9 15:30 | MGnify FTP ingest log |

---

## 8 — Staging `/data/pipeline/staging/`  (~5 MB)

| Path | Contents |
|---|---|
| `combined_samples_1773106289.json` (2.4 MB) | Cached NEON API sample manifest (Mar 10) |
| `combined_samples_1773193239.json` (2.4 MB) | Cached NEON API sample manifest (Mar 11) |
| `neon_16s/` | 12 NEON 16S sample working dirs (GUAN, KONZ, OSBS, TALL sites) |
| `neon_cache/` | NEON API JSON response cache |
| `neon.GUAN_*/`, `neon.KONZ_*/`, `neon.OSBS_*/`, `neon.TALL_*/` | 12 metagenomics-product working dirs (JGI shotgun, skipped — not 16S amplicon) |

> The 222 "failed" 16S samples in the overnight run were mostly Yellowstone (YELL) metagenomics-only sites with JGI shotgun URLs only — correctly skipped by `process_neon_16s.py`.

---

## 9 — Reference Configs `/opt/pipeline/configs/`

Three pipeline instantiation YAML files demonstrating generality:

| File | Application | Key targets |
|---|---|---|
| `soil_carbon.yaml` | BNF / soil carbon | nifH, nifD, nifK; NITROGENASE_MO flux ≥ 0.01 mmol/gDW/h |
| `bioremediation.yaml` | Hydrocarbon bioremediation | alkB, xylE, catA; contaminated soils (inverted context) |
| `carbon_sequestration.yaml` | C-sequestration | CBB cycle genes; CO₂ fixation flux targets |

---

## 10 — Active Services

| Service | PID | Command | Status |
|---|---|---|---|
| uvicorn REST API | 595903 | `/opt/pipeline/.venv/bin/python3 uvicorn api.main:app --host 127.0.0.1 --port 8000 --workers 2` | ✅ Running |
| API worker 1 | 595927 | multiprocessing fork | ✅ Running |
| API worker 2 | 595928 | multiprocessing fork | ✅ Running |

No pipeline jobs currently running. No cron jobs configured.

---

## 11 — Logs

### `/data/pipeline/logs/` (current run logs)

| File | Size | Coverage |
|---|---|---|
| `overnight_20260311_0618.log` | 195 KB | Mar 11 00:00–07:11 — patch_neon_notes + process_neon_16s (826 OK, 222 failed) |
| `t1_real_20260311_1647.log` | 64 KB | Mar 11 10:47–16:55 — T1 FBA real-mode, NEON 420 eligible |

### `/opt/pipeline/logs/` (historical)

| File | Size | Coverage |
|---|---|---|
| `neon_16s_all.log` | 297 KB | Mar 9 — bulk NEON 16S classification |
| `neon_16s_20260309.log` | 95 KB | Mar 9 — 16S retry run |
| `neon_16s_retry.log` | 17 KB | Mar 9 — targeted retry |
| `neon_t025.log` | 538 B | Mar 9 — T0.25 test attempt |
| `neon_16s_harv.log` | 8.2 KB | Mar 8 — HARV site 16S classification |
| `neon_chem_backfill.log` | 16 KB | Mar 8 — chemical metadata backfill |
| `silva_download.log` | 1.5 KB | Mar 8 — SILVA 138 reference download |
| `install_vsearch.log` | 8.9 KB | Mar 8 — vsearch install |
| `install_sra.log` | 22 KB | Mar 8 — SRA toolkit install |
| `neon_ingest.log` | 3.5 KB | Mar 8 — initial NEON adapter ingest |
| `install_picrust2.log` | 1.6 KB | Mar 8 — PICRUSt2 install attempt |

---

## 12 — Key Python Dependencies (venv)

| Package | Version |
|---|---|
| cobra | 0.30.0 |
| geopandas | 1.1.2 |
| joblib | 1.5.3 |
| numpy | 2.4.2 |
| pandas | 2.3.3 |
| scipy | 1.17.1 |
| scikit-learn | (installed — joblib present) |

> `sklearn` not shown in grep output by default name; `scikit-learn` confirmed at install time. PICRUSt2 and HUMAnN3 were stub-installed (see `install_picrust2.log`).

---

## 13 — Last Run Summary

### Overnight run (2026-03-11 00:00–07:11)
- **Step 1 (patch_neon_notes):** backfilled FASTQ URLs for NEON communities with empty notes
- **Step 2 (process_neon_16s):** 826 OK classified, 222 failed (YELL/JGI shotgun-only — expected)
- **Step 3 (t1_fba_batch):** skipped — eligibility check still returned 0 (bug under investigation; likely timing of t0_pass flag update)

### Manual T1 run (2026-03-11 10:47–16:55)
- **Input:** 420 T1-eligible communities (9 cached genera × ~47 communities each)
- **Phase A:** 9/147 models from SBML cache; 138 fell through (no CarveMe NCBI mapping — fixed in `2db435a`)
- **Phase B:** 79 batches × 5 communities × 36 workers → **391 written, 339 T1-passed in 8.0 min**
- **Cumulative t1_pass:** ~4,830 (3,491 prior + 339 new NEON)

---

## 14 — Gaps / Next Actions

| Item | Detail |
|---|---|
| **SBML coverage** | Only 20 genera have AGORA2 SBML on disk; `_GENUS_NCBI` now has 100+ entries — CarveMe will build new models on next T1 run for all newly mapped genera |
| **T025 surrogate** | `functional_predictor.joblib` is at `/opt/pipeline/models/` (correct path); `predict_with_gate()` now called (`2db435a`); next `run_t025_batch()` call will populate `t025_pass` |
| **Overnight Step 3 bug** | Step 3 T1-eligibility returned 0 despite 826 being classified — needs diagnosis (likely `t0_pass` update timing or query field mismatch) |
| **PICRUSt2 ref** | `/data/pipeline/picrust2_ref/` is empty — PICRUSt2 functional profiling not yet operational |
| **YELL site backfill** | ~222 Yellowstone samples have JGI shotgun URLs only — need amplicon sequencing data from NEON or separate 16S product |
| **T2 re-run** | After expanded T1 (339 new NEON t1_pass), T2 dFBA should be re-run to add the new communities to stability scoring |
| **Cron / automation** | No cron jobs; pipeline runs are fully manual — consider scheduling nightly patch+classify+t1 chain |
