# hetzner2 Server State

**Snapshot:** 2026-03-11 17:30 CST  
**Host:** `144.76.222.125` (alias `hetzner2`)  
**Code commit in sync:** `dc08bae`

---

## System

| Property | Value |
|---|---|
| OS | Ubuntu 24.04 LTS |
| Kernel | 6.8.0-90-generic |
| CPU | Intel Xeon W-2295 @ 3.00 GHz (18 cores) |
| RAM | 251 GB total · 3.0 GB used · 248 GB available |
| Disk | `/dev/md2` RAID · 875 GB · 21 GB used · 810 GB free |
| Uptime | 4 days 21 h (booted ~2026-03-06) |
| Load | 0.00 / 0.31 / 3.05 (idle) |
| Pipeline root | `/opt/pipeline` |
| Python | 3.12.3 · `/opt/pipeline/.venv` |
| API | uvicorn PID 595903 · `127.0.0.1:8000` · 2 workers |

---

## Database `/data/pipeline/db/soil_microbiome.db`  (528 MB)

### Pass funnel by source (live query 2026-03-11 17:25)

| Source | Total | t0_pass | t025_pass | t1_pass | t2_pass |
|---|---|---|---|---|---|
| synthetic | 220,000 | 220,000 | 220,000 | 0 | 0 |
| neon | 17,567 | 11,026 | 3,564 | 4,768 | 3,323 |
| mgnify | 95 | 95 | 95 | 62 | 55 |
| **Total** | **237,662** | **231,121** | **223,659** | **4,830** | **3,378** |

### T1 flux stats (t1_pass communities)

| Metric | Value |
|---|---|
| Count | 4,830 |
| Mean flux | 62.267 mmol NH₄-equiv/gDW/h |
| Max flux | 378.406 |
| Min flux (threshold) | 12.244 |

> Mean/max are above expected biological ceiling (~45 mmol/gDW/h per diazotroph). Cap constant in `t1_fba_batch.py` is set at 50.0 — the DB max of 378 suggests earlier uncapped runs are still in DB. Needs re-examination before publication.

### T2 stability stats (t2_pass communities)

| Metric | Value |
|---|---|
| Count | 3,378 |
| Mean stability score | 0.959 |
| Threshold | ≥ 0.30 |

---

## Data Resources

### Reference sequences  `/data/pipeline/ref/`  (~1.5 GB)

| Resource | Size | Details |
|---|---|---|
| `16S_blast_db/` | 358 MB | vsearch SILVA 138 BLAST-format DB — used by `process_neon_16s.py` |
| `16S_raw.fasta` | 41 MB | SILVA 138 SSU full sequences |
| `16S_ref.fasta` | 40 MB | SILVA 138 V4 region trimmed (primer-bounded) |
| `16S_taxids.tsv` | 516 KB | SILVA accession → NCBI taxonomy ID mapping |
| `taxdump/` | 1.1 GB | NCBI taxonomy nodes.dmp / names.dmp — used by genome_fetcher NCBI taxon lookup |

### AGORA2 metabolic models  `/data/pipeline/models/`  (247 MB)

20 genera on disk; 9 patched with `NITROGENASE_MO` reaction (commit `90f0e92`):

| Genus | Patched |
|---|---|
| Acidobacterium, Bacillus, Caulobacter, Ellin, Gemmata, Gluconacetobacter, Nitrosomonas, Nitrospira, Pseudomonas, Sphingomonas, Streptomyces, Variovorax | — |
| Azoarcus, Azospirillum, Azotobacter, Bradyrhizobium, Burkholderia, Herbaspirillum, Mesorhizobium, Rhizobium, Sinorhizobium | ✅ |

`.bak` copies preserved for all patched models.

### Surrogate ML models  `/opt/pipeline/models/`  (66 MB)

| File | Size | Description |
|---|---|---|
| `bnf_surrogate_classifier.joblib` | 16 MB | RF classifier gate — ROC-AUC 0.812 OOB 0.772 |
| `bnf_surrogate_regressor.joblib` | 18 MB | RF regressor — R² 0.465 OOB 0.469 |
| `functional_predictor.joblib` | 33 MB | Combined `FunctionalPredictor` wrapper |

Trained on 5,907 real communities (`cf5e081`). Top features: soil_pH 42%, Nitrososphaerota 19%, Nitrospirota 12%. `pipeline_core.py` loads relative to CWD (`/opt/pipeline/models/…`) — path resolves correctly.

### Proteome cache  `/data/pipeline/proteome_cache/`  (63 MB, 42 files)

21 genera with `.faa` + `.tsv`: Acidobacterium, Azoarcus, Azospirillum, Azotobacter, Bacillus, Bradyrhizobium, Burkholderia, Caulobacter, Ellin, Gemmata, Gluconacetobacter, Herbaspirillum, Mesorhizobium, Nitrosomonas, Nitrospira, Pseudomonas, Rhizobium, Sinorhizobium, Sphingomonas, Streptomyces, Variovorax.

### Staging  `/data/pipeline/staging/`  (~5 MB)

| Item | Detail |
|---|---|
| `neon_16s/` | 12 NEON 16S working dirs (GUAN, KONZ, OSBS, TALL sites) |
| `combined_samples_*.json` | NEON API manifests (Mar 10 + Mar 11) |
| `neon.GUAN_*/` etc. | 12 JGI-shotgun-only metagenomics dirs — correctly skipped by 16S classifier |

### Results  `/data/pipeline/results/`  (12 MB)

| File | Date | Contents |
|---|---|---|
| `ranked_candidates.csv` | Mar 8 | Top-100 BNF communities |
| `spatial/spatial_map.png` | Mar 8 | CONUS kriging heatmap (6,413-pt grid) |
| `spatial/spatial_communities.csv` | Mar 8 | Per-community GPS + BNF flux |
| `bnf_trajectory_summary.csv` | Mar 8 | dFBA 90-day trajectories |
| `keystone_analysis.csv` | Mar 8 | Keystone taxa per community |
| `intervention_portfolio.csv` | Mar 8 | Per-community intervention assignments |
| `intervention_report.md/.json` | Mar 8 | 11 field recommendations |
| `fva_uncertainty.csv` | Mar 8 | FVA min/max bounds |
| `taxa_enrichment.csv` | Mar 8 | Enriched taxa in BNF-pass communities |
| `climate_resilience.csv` | Mar 8 | Climate perturbation stability scores |

### PICRUSt2 reference  `/data/pipeline/picrust2_ref/`  (empty)

Not yet populated — see Gap 3 below.

---

## Installed Bioinformatics Tools

| Tool | Status | Notes |
|---|---|---|
| vsearch | ✅ Installed | 16S chimera removal + SILVA classification |
| CarveMe 1.6.6 | ✅ pip package | `carveme` binary not in `$PATH` — invoke via venv; generates SBML from proteomes |
| CPLEX (CarveMe solver) | ❓ Unknown | CarveMe prefers CPLEX; falls back to GLPK |
| SRA toolkit | ✅ Installed | `fasterq-dump` available; no SRA jobs run yet |
| scikit-learn | ✅ | RF surrogate training + inference |
| cobra 0.30.0 | ✅ | FBA/FVA; GLPK enforced (OSQP unsafe) |
| geopandas 1.1.2 | ✅ | Spatial kriging (Addition F) |
| DRAM / Prokka | ❌ Not installed | Genome annotation — needed for reference T1 |
| MetaBat2 / SemiBin | ❌ Not installed | MAG binning — needed for reference T1 |
| CheckM | ❌ Not installed | Genome completeness — needed for reference T1 |
| PICRUSt2 | ⚠️ Stub only | pip stub installed; no reference DB; empty `/data/pipeline/picrust2_ref/` |
| HUMAnN3 | ❌ Not installed | Shotgun-only; not applicable until NCBI SRA connected |
| FaProTax | ❌ Not installed | Taxonomy → function DB; needed for T0.25 profiling |

---

## Reference Model Data Source Gap Analysis

Comparing what the **reference model (Diagram 1)** requires vs what is currently available on hetzner2. Ordered by pipeline tier.

### Feed streams

| Feed | Reference model expectation | Current state | Gap severity |
|---|---|---|---|
| **NCBI SRA shotgun reads** | ~2M samples; primary metagenome source | SRA toolkit installed; adapter (`ncbi_sra_adapter.py`) written; **0 samples ingested** | 🔴 High — entire shotgun pathway blocked |
| **EBI MGnify assemblies** | 500k+ studies, processed assemblies | 95 amplicon samples ingested (BIOM via `mgnify_adapter.py`); not shotgun assemblies | 🟡 Partial — amplicon only, not assemblies |
| **NEON amplicon** | Multi-site amplicon + rich metadata | 17,567 communities in DB; 826 with 16S classified (remaining ~16,741 lack amplicon URLs — metagenomics product only) | 🟡 Partial — most sites are JGI-shotgun-only |
| **EMP** | Global amplicon survey | Adapter (`emp_adapter.py`) written; **0 samples ingested** | 🔴 Not started |
| **Qiita** | Broad amplicon repository | Adapter (`qiita_adapter.py`) written; **0 samples ingested** | 🔴 Not started |

### T0 — Quality & Composition Filter

| Resource needed | Reference expectation | Current state | Gap |
|---|---|---|---|
| SILVA 138 V4 classifier | 97% identity vsearch | ✅ On disk — `/data/pipeline/ref/16S_ref.fasta` + `16S_blast_db/` | None |
| nifH gene reference DB | Detect functional BNF genes in reads | `has_nifh` field in schema; detection is genus-taxonomy-proxy, **not direct HMM/BLAST scan** | 🟡 Functional gene detection not sequence-level |
| Contamination + chimera DB | Decontam / PhiX / host reads | vsearch chimera removal only; no host decontamination DB | 🟡 Low risk for amplicon; critical for shotgun |
| Faith PD phylogenetic tree | Phylogenetic diversity metric | Faith PD column in schema; tree source not confirmed | 🟡 May be placeholder value |

### T0.25 — ML Functional Predictor

| Resource needed | Reference expectation | Current state | Gap |
|---|---|---|---|
| PICRUSt2 reference DB | 16S OTU → KEGG KO pathway abundances | Directory empty (`/data/pipeline/picrust2_ref/`); pip stub installed | 🔴 Cannot run PICRUSt2 pathway inference |
| HUMAnN3 UniRef + ChocoPhlAn | Shotgun reads → MetaCyc pathways | Not installed; requires shotgun input anyway | 🔴 Blocked on SRA connection |
| FaProTax reference | Taxonomy → functional guild mapping | Not present on hetzner2 | 🔴 Not installed |
| Reference BNF community library | Bray-Curtis + UniFrac similarity search (min 0.30) | `reference/high_bnf_communities.meta.json` exists locally; reference BIOM files not checked in or deployed | 🟡 Similarity search not operational — replaced by ML surrogate |
| ML surrogate | RF classifier + regressor on T1 FVA outputs | ✅ `functional_predictor.joblib` trained + deployed. ROC-AUC 0.812. `predict_with_gate()` now called (`2db435a`) | ✅ Exceeds reference design on BNF-specificity |

### T1 — Metabolic Network Reactor

| Resource needed | Reference expectation | Current state | Gap |
|---|---|---|---|
| Shotgun MAGs (MetaBat2/SemiBin) | Per-sample genome bins from shotgun reads | Not installed; requires shotgun input | 🔴 Replaced by genus-proxy AGORA2 approach |
| DRAM / Prokka annotation | KEGG + MetaCyc per genome bin | Not installed; not applicable without MAGs | 🔴 Replaced by AGORA2 pre-built models |
| Complete AGORA2 genus library | ~900 genera available | **20 of ~900 genera on disk** — remaining 880 not fetched; CarveMe now has 100+ NCBI entries to build from | 🟡 Coverage expands each T1 run as CarveMe builds new models |
| CarveMe SBML synthesis | Per-sample genome-scale model from annotation | CarveMe 1.6.6 installed (pip); NCBI proteome cache for 21 genera; 100+ NCBI entries now in `_GENUS_NCBI` table | 🟡 Operational but slow — 8–15 min per new genus model |
| NCBI taxdump | Taxon ID → species name for CarveMe | ✅ 1.1 GB on disk at `/data/pipeline/ref/taxdump/` | None |
| CPLEX solver | CarveMe preferred solver (faster than GLPK) | Status unknown on hetzner2; GLPK available as fallback | 🟡 GLPK works; CPLEX would be ~5× faster |

### T2 — Community Dynamics Reactor

| Resource needed | Reference expectation | Current state | Gap |
|---|---|---|---|
| Perturbation climate panel | Drought/heat/flood/pH shift parameter sets | ✅ Implemented in `t2_dfba_batch.py`; climate_projections table populated | None |
| AGORA2 models for intervention screener | Mechanistic niche scoring in `intervention_screener.py` | Wired but awaits broader AGORA2 coverage (currently 20 genera) | 🟡 Falls back to metadata-driven picker |

### Additions (Diagram 3)

| Addition | Data needed | Current state | Gap |
|---|---|---|---|
| **A — Metatranscriptomics** | Paired mRNA from NEON/SRA; nifH expression ratios | No metatranscriptome samples in DB; `mrna_to_dna_ratio` field exists in schema | 🔴 No data; NEON does not routinely publish metatranscriptomes |
| **B — 15N field measurements** | Isotope dilution assay results paired to NEON sites | Not ingested; `reference/bnf_measurements.csv` placeholder noted in README | 🔴 Would require collaboration with field measurement programs |
| **C — Surrogate training loop** | Progressive retraining as T1 FVA results accumulate | ✅ Functional — train_bnf_surrogate.py operational; 5,907 training samples; model deployed | ✅ Active |
| **D — Metabolic exchange map** | Cross-feeding network from community FBA | `metabolic_exchange.py` exists in compute/; not integrated into T1 batch | 🟡 Code present; not wired |
| **E — Agent-based model** | iDynoMiCS individual-based simulation | `agent_based_sim.py` exists; iDynoMiCS not installed | 🟡 Stub present |
| **F — Spatial kriging** | NEON GPS coordinates + BNF flux | ✅ Complete Mar 8 — `/data/pipeline/results/spatial/`; 6,413-pt CONUS grid | ✅ Done |
| **G — Time-series tracking** | Multi-visit NEON sample pairing | NEON has multi-visit data; not queried or matched yet | 🟡 Data exists, not extracted |
| **H — Cross-pipeline optimizer** | BNF + C-seq + pathogen suppression joint ranking | `carbon_sequestration.yaml` + `bioremediation.yaml` configs present; no joint scoring | 🟡 Configs ready; joint run not implemented |

---

## Priority Gap Summary

| Priority | Gap | Effort | Impact |
|---|---|---|---|
| 1 | **PICRUSt2 reference DB** — install `picrust2 install` to populate `/data/pipeline/picrust2_ref/` | ~30 min, ~2 GB download | Unlocks real pathway-based T0.25 profiling |
| 2 | **NCBI SRA connection** — trigger `ncbi_sra_adapter.py` on soil metagenome studies | Hours–days download | 100× more training data; enables reference T1 MAG path |
| 3 | **EMP + Qiita ingestion** — trigger existing adapters | 1–2 days | Increases NEON-independent amplicon coverage |
| 4 | **Remaining AGORA2 SBML** — pre-fetch full genus library from AGORA2 repository | ~4 h download script | Eliminates per-genus CarveMe wait during T1 runs |
| 5 | **FaProTax** — install + wire into T0.25 profiler | ~1 h | Adds taxonomy→function guild annotations without PICRUSt2 |
| 6 | **NEON multi-visit pairing (Addition G)** — query NEON API for repeat visits per plot | ~1 day | Time-series BNF trajectory without new data downloads |
| 7 | **T2 re-run** — 339 new NEON t1_pass not yet through T2 | ~20 min | Adds ~300 communities to stability pool |
| 8 | **Flux ceiling audit** — DB max of 378 mmol/gDW/h exceeds biological expectation; review cap enforcement | ~2 h | Data quality for publication |
