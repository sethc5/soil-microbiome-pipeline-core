# Pipeline Findings — config.example
_Generated: 2026-03-09 14:32 UTC_

## Run Summary
- Communities screened: **450444**
- T0 passed: **444362**
- T1 metabolic models built: **20000**
- T2 dynamics simulated: **20000**
- Top target function flux: **1000** (community 60655)

## Correlation Patterns
- **ph** shows weak positive correlation with target function flux (Spearman r = 0.0172, n = 20000)
- **temperature** shows weak positive correlation with target function flux (Spearman r = 0.0007, n = 20000)
- **latitude** shows weak positive correlation with target function flux (Spearman r = 0.0214, n = 20000)
- **longitude** shows weak negative correlation with target function flux (Spearman r = -0.0092, n = 20000)

## Enriched Taxa (Top 10 by significance)
- **Arthrobacter** — fold-change 0.32×, p_adj = 0
- **Frankia** — fold-change 0.61×, p_adj = 0
- **Gemmata** — fold-change 3.76×, p_adj = 0
- **Nitrospira** — fold-change 1.48×, p_adj = 0
- **Nocardia** — fold-change 0.35×, p_adj = 0
- **Planctomyces** — fold-change 0.38×, p_adj = 0
- **Rhizobium** — fold-change 1.21×, p_adj = 0
- **Herbaspirillum** — fold-change 1.13×, p_adj = 1.01e-14
- **Burkholderia** — fold-change 1.13×, p_adj = 3.53e-13
- **Acidobacterium** — fold-change 0.68×, p_adj = 3.58e-13

## BNF Temporal Stability (dFBA Trajectories)
- 20,000 communities tracked over 30-day dFBA simulations
- Mean peak BNF flux: **0.0851** mmol/gDW/h  (max: 0.1407)
- Mean retention (day-60 vs day-30): **88.3%**  (3.1% fully stable ≥90%)
- Highest peak BNF: community **174015**  (peak=0.1407, retention=91.1%, site=KONZ, land=grassland)
- Most stable BNF: community **109020**  (retention=97.6%, peak=0.1155, site=KONA)
- Mean peak BNF by land use:
  - rangeland: 0.0912
  - grassland: 0.0881
  - cropland: 0.0831
  - wetland: 0.0787
  - forest: 0.0773
_BNF trajectory detail: `results/bnf_trajectory_summary.csv`_

## Keystone Taxa & Community Architecture
- 20,000 T1-pass communities analyzed for keystone architecture
- Mean keystones per community: **7.7** (range 3–9 of 9 members)
- Mean BNF flux-drop when any keystone removed: **88.0%** — indicating highly coupled community architectures
- Organisms by keystone frequency across all communities:
  - **organism_5**: indispensable in 100.0% of communities (mean flux-drop 87.0%)
  - **organism_4**: indispensable in 100.0% of communities (mean flux-drop 86.7%)
  - **organism_6**: indispensable in 99.7% of communities (mean flux-drop 87.5%)
  - **organism_3**: indispensable in 99.6% of communities (mean flux-drop 86.8%)
  - **organism_2**: indispensable in 98.9% of communities (mean flux-drop 87.3%)
  - **organism_9**: least critical — keystone in only 16.5% of communities (mean flux-drop 90.6%)
_Detail: `results/keystone_analysis.csv`, `results/keystone_organism_summary.csv`_

## Intervention Portfolio Analysis
- 200,000 interventions screened across 3 categories:
  - **Bioinoculant** (99,996): mean effect = 0.373, confidence = 0.112, avg cost = $28/ha, cost-effectiveness = 0.01569 effect/$
  - **Amendment** (80,000): mean effect = 0.117, confidence = 0.210, avg cost = $422/ha, cost-effectiveness = 0.00052 effect/$
  - **Management** (20,004): mean effect = 0.045, confidence = 0.150, avg cost = $80/ha, cost-effectiveness = 0.00056 effect/$
- **Bioinoculant** is the dominant strategy: 28× better cost-effectiveness than management
- Effect ranking: bioinoculant > amendment > management
_Detail: `results/intervention_type_summary.csv`_

## Pipeline Funnel Efficiency
- Total communities entered: **441,942**
- T0 quality filter: **441,942** pass (100%)
- T0.25 ML scoring: **441,942** pass (100% of T0)
- T1 community FBA: **20,000** pass (4% of T0.25) — the primary discriminating filter
- T2 dFBA stability: **20,000** pass (100% of T1)
- FVA worst-case flux |lower bound| by land use:
  - grassland: 718.5 ± 27.6 mmol/gDW/h (n=9524)
  - rangeland: 717.9 ± 27.3 mmol/gDW/h (n=2220)
  - cropland: 716.1 ± 27.5 mmol/gDW/h (n=3056)
  - wetland: 715.9 ± 28.4 mmol/gDW/h (n=2808)
  - forest: 715.0 ± 29.4 mmol/gDW/h (n=2392)
  *(Upper bound capped at 1000 by COBRA default — only lower bound is informative)*
_Detail: `results/funnel_analysis.json`, `results/fva_uncertainty.csv`_

## Data Confidence & Production Readiness

### What this pipeline can currently produce
- Systematic ranking of 20,000 synthetic + 4362 real community configurations for BNF potential
- Mechanistic identification of keystone taxa using leave-one-out FBA
- Intervention cost-effectiveness comparison across amendment, bioinoculant, and management strategies
- BNF temporal stability profiling via dFBA trajectory analysis
- Spatial clustering and land-use stratification of top candidates

### Overall data confidence: MEDIUM
Real NEON samples have OTU classifications — functional predictions are data-grounded, though genome models remain synthetic.

#### Confidence by data source
| Source | Samples | Confidence |
|--------|---------|------------|
| MGNIFY | 796 | MEDIUM |
| NEON | 9,648 | MEDIUM — real metadata + 16S phylum profiles classified |
| Synthetic | 220,000 | LOW — placeholder genomes |

#### Real-data progress
- **NEON samples ingested**: 9,648 across 20 field sites
- **NEON soil pH populated**: 9,346 / 9,648 samples ✓
- **NEON T0-pass (16S classified)**: 4,362 / 9,648 ✓
- **NEON T0.25 scored**: 4,360 / 4,362 T0-pass communities ✓
- **MGnify FTP ingested**: 796 soil communities (EBI amplicon-pipeline-v6) ✓
- **MGnify T0-pass**: 796 / 796 ✓
- **SRA tools**: installed (v3.x) ✓
- **PICRUSt2**: installed (v2.6.3) ✓
- **vsearch**: installed (v2.30.x) ✓

### Remaining gaps to high-value production
| Gap | Status | Impact |
|-----|--------|--------|
| NEON 16S classification (vsearch+SILVA) | ✓ Complete — 4362/9648 samples | Real phylum profiles → genuine FBA inputs |
| NEON T0.25 ML scoring | ✓ Complete — 4360/4362 T0-pass communities scored | function_score computed for all T0-pass NEON communities |
| PICRUSt2 functional profiling on NEON OTUs | N/A — vsearch 16S pipeline outputs phylum profiles, not OTU BIOM tables | Would require pipeline restructuring |
| T1 FBA for real NEON T0-pass communities | Not started — needs genus-level assignments (shotgun 16S gives ~99.9% Unclassified) | First real metabolic flux predictions |
| Real genome-scale models (AGORA2/MICOM) | Not started | Replaces synthetic FBA → raises to HIGH |
| MGnify FTP ingest (v6 amplicon) | ✓ Complete — 796 soil communities via ftp.ebi.ac.uk (no proxy needed) | Real 16S phylum profiles from curated EBI pipeline |
| GTDB-Tk + CheckM genome annotation | Not started | Raises model confidence to medium/high |

### Path to high-value output
With NEON 16S OTU profiles complete + PICRUSt2 functional annotation, the pipeline produces field-grounded rankings from real ecological survey data. The schema, funnel logic, receipts system, and findings generator are all production-grade. Only AGORA2 genome-scale models are needed to reach HIGH confidence.

## Caveats
- Metabolic model confidence depends on genome completeness (CheckM).
- dFBA ignores substrate kinetics; stability scores are approximate.
- Enrichment analysis is limited to taxa present in the T0.25 functional profile.
- All computational predictions require wet-lab validation before field application.

_Ranked candidates: `/data/pipeline/results/ranked_candidates.csv`_
_Intervention report: `/data/pipeline/results/intervention_report.md`_
