# Pipeline Status Log

**Last updated**: 2026-03-09 (session 2)  
**Repo**: `sethc5/soil-microbiome-pipeline-core` — branch `main` @ `82de70a`  
**Server**: `deploy@<HETZNER2_HOST>` (Hetzner AX41, `/opt/pipeline/`, `/data/pipeline/`)

---

## Database State

| Source | Communities | T0-pass | T0.25-pass | T1 (flux) | T2 (stability) |
|--------|-------------|---------|-----------|-----------|----------------|
| **Synthetic** | 440,000 | 440,000 | 440,000 | 20,000 | 20,000 |
| **NEON** | 9,648 | 3,566 | 3,564 | 0 | 0 |
| **MGnify (FTP)** | 95 | 95 | 95 | 0 | 0 |
| **Total** | **449,743** | **444,101** | **444,099** | **20,000** | **20,000** |

- NEON: 9,346 / 9,648 samples have `soil_ph` populated; **0 have genus-level data** (shotgun metagenome product only)
- MGnify: **95 real soil communities** from 4 ERP studies in `mgnify_results/` old pipeline:
  - ERP122862, ERP139415, ERP159279, ERP172057 (Acidobacteriota 25–44%, Cyanobacteriota ≈0)
  - All 95 have genus-level data (avg 18.8 genera/community) ← key improvement
  - Previous 796 rows were marine (Prochlorococcus-dominated, misclassified) — **purged**
- Synthetic: 20k communities through full T1+T2 (FBA + dFBA trajectory)

---

## Critical Data Issues Resolved This Session

| Issue | Resolution |
|-------|-----------|
| 796 "MGnify" communities were marine (Cyanobacteriota 60–67%) | Purged; fix soil filter |
| Soil filter was presence-based — admitted marine Planctomycetota | Changed to abundance-based: Acidobacteriota >5% AND Cyanobacteriota <5% |
| ingest_mgnify_ftp.py only supported v6 amplicon-pipeline FTP tree | Added `--ftp-tree old` for `mgnify_results/` tree (different URL layout) |
| ERR sub-bucket computed as `err[:6]` — wrong for 10-char accessions | Fixed to `err[:-3]` (e.g. ERR2640150 → ERR2640/) |
| NEON has 0 genus-level data — shotgun metagenome product | NEON DP1.10108.001 (16S amplicon, 47 sites) identified as path forward |

---

## Findings in DB (9 rows)

| ID | Title |
|----|-------|
| 1 | Pipeline run summary |
| 2 | Climate-resilient community 145109 |
| 3 | Climate-resilient community 177788 |
| 4 | Climate-resilient community 361051 |
| 5 | BNF × land use: rangeland/grassland highest flux |
| 6 | BNF stability: 3.1% stable, 96.9% moderate |
| 7 | Top BNF sites: CLBJ, OAES, NOGP |
| 8 | Best combined BNF candidate: community 174015 (site=KONZ, peak=0.1407, retention=0.911) |
| 9 | BNF trajectory summary: 20,000 communities, mean peak=0.0851 |

---

## Analysis Outputs (`/data/pipeline/results/`)

| File | Description | Status |
|------|-------------|--------|
| `bnf_trajectory_summary.csv` | dFBA trajectories for 20k synthetic communities (3.4 MB) | ✓ |
| `ranked_candidates.csv` | Top 50 BNF candidates, all metadata + scores (169 KB) | ✓ |
| `keystone_analysis.csv` | Leave-one-out keystone taxa, 7.7 mean keystones/community (1.4 MB) | ✓ |
| `keystone_organism_summary.csv` | Keystone taxa ranked by flux impact, 88% mean flux-drop | ✓ |
| `taxa_enrichment.csv` | 21/26 taxa significantly enriched; Gemmata 3.76×, Nitrospira 1.48× | ✓ |
| `funnel_analysis.json` | FVA funnel efficiency by land use | ✓ |
| `fva_uncertainty.csv` | FVA lower/upper bounds per community (2.3 MB) | ✓ |
| `intervention_portfolio.csv` | Bioinoculant 30× more cost-effective than amendment (1.7 MB) | ✓ |
| `intervention_type_summary.csv` | Per-type cost-effectiveness summary | ✓ |
| `intervention_report.md` | Intervention portfolio narrative | ✓ |
| `correlation_findings.json` | 4 correlations: pH (r=0.017), lat (r=0.021), temp, lon | ✓ |
| `climate_resilience.csv` | Top climate-resilient communities (2.5 MB) | ✓ |
| `spatial/` | Spatial clustering outputs | ✓ |
| `mgnify_ftp_checkpoint.json` | 796 ERR accessions processed (resumable) | ✓ |

---

## Key Analysis Results

### BNF by Land Use (synthetic T1/T2)
| Land Use | Mean BNF Flux |
|----------|---------------|
| Rangeland | 0.0912 |
| Grassland | 0.0881 |
| Forest | 0.0773 |

### Trajectory Stability (20k communities)
- Stable (≥0.9 retention): **3.1%** of communities
- Moderate: **96.9%**
- Unstable: **0%**

### Top BNF Sites (synthetic)
CLBJ (Texas savanna) > OAES (Oklahoma grassland) > NOGP (North Dakota prairie)

### Best Candidate
Community 174015 — site KONZ (Kansas tallgrass prairie), grassland, peak BNF = 0.1407, temporal retention = 0.911

### Intervention Portfolio
Bioinoculant is ~30× more cost-effective per unit BNF effect vs. mineral amendment

### Taxa Enriched in High-BNF Communities
21 / 26 tested taxa significant; top: Gemmata (3.76×), Nitrospira (1.48×)

---

## Infrastructure

| Component | Status |
|-----------|--------|
| Server (Hetzner AX41) | ✓ Running — uvicorn API on port 8000 |
| SQLite DB (WAL mode) | ✓ `/data/pipeline/db/soil_microbiome.db` |
| Python venv | ✓ `/opt/pipeline/.venv` |
| vsearch + SILVA | ✓ Installed, used for 16S classification |
| PICRUSt2 | ✓ Installed (v2.6.3) — not yet applied to NEON OTUs |
| SRA-tools | ✓ v3.x installed |
| EBI FTP access | ✓ `ftp.ebi.ac.uk` accessible from Hetzner (no WAF block) |
| EBI MGnify API | ✗ Blocked (Hetzner ASN / EBI WAF) — FTP path used instead |

---

## Scripts Inventory

### Ingestion
| Script | Purpose | Status |
|--------|---------|--------|
| `scripts/ingest_neon_biom.py` | Fetch NEON BIOM/FASTQ, populate samples + communities | ✓ |
| `scripts/ingest_mgnify.py` | MGnify REST API ingest (requires SOCKS proxy) | Superseded by FTP |
| `scripts/ingest_mgnify_ftp.py` | **Direct EBI FTP ingest — v6 and old tree, abundance-based soil filter** | ✓ Active |
| `scripts/ingest_sra.py` | SRA public metagenomes | Available |
| `adapters/neon_adapter.py` | NEON API adapter | ✓ |
| `adapters/mgnify_adapter.py` | MGnify REST adapter (proxy-aware) | Available |

### Pipeline Execution
| Script | Purpose | Status |
|--------|---------|--------|
| `pipeline_core.py` | Main pipeline orchestrator (T0→T2) | ✓ |
| `scripts/run_neon_t025.py` | Batch T0.25 ML scoring for NEON T0-pass communities | ✓ (3,564 scored) |
| `scripts/run_16s_classifier.py` | vsearch + SILVA 16S classification → T0-pass | ✓ (3,566 classified) |
| `compute/diversity_metrics.py` | Alpha/beta diversity | ✓ |
| `compute/functional_predictor.py` | T0.25 function score | ✓ |
| `compute/community_fba.py` | T1 FBA (COBRApy) | ✓ — blocked on genus resolution for real data |
| `compute/dfba_runner.py` | T2 dFBA trajectory | ✓ |
| `compute/stability_analyzer.py` | T2 stability scoring | ✓ |

### Analysis
| Script | Purpose | Run |
|--------|---------|-----|
| `scripts/bnf_trajectory_analysis.py` | Parse dFBA trajectories, write 5 findings | ✓ 2026-03-08 |
| `scripts/fva_funnel_analysis.py` | FVA lower bounds + funnel by land use | ✓ 2026-03-08 |
| `scripts/keystone_analysis.py` | Leave-one-out keystone identification | ✓ 2026-03-08 |
| `correlation_scanner.py` | Environment × BNF correlations | ✓ 2026-03-08 |
| `taxa_enrichment.py` | Mann-Whitney enrichment vs high-BNF communities | ✓ 2026-03-08 |
| `rank_candidates.py` | Composite score ranking, top-50 CSV | ✓ 2026-03-08 |
| `scripts/intervention_portfolio.py` | Cost-effectiveness across intervention types | ✓ 2026-03-08 |
| `findings_generator.py` | Renders `FINDINGS.md` from DB + results | ✓ |

---

## Open Gaps / Blockers

| Gap | Blocker | Impact |
|-----|---------|--------|
| T1 FBA for NEON communities | NEON shotgun product has 0 genera; need DP1.10108.001 (16S amplicon, 47 sites) download + DADA2 | First real metabolic flux values |
| NEON DP1.10108.001 ingest | Raw FASTQ on SRA (PRJNA393362); need QIIME2/DADA2 pipeline or redbiom lookup for pre-computed OTUs | Genus-level NEON data |
| T1/T2 for MGnify 95 communities | 95 communities with genus data — need genome-scale models (AGORA2) to run FBA | Expand real-data FBA |
| More MGnify old-tree studies | 7 studies had `no_SILVA-SSU`; `V6/unknown/` variant found — re-run classifier | Scale to 300+ more communities |
| AGORA2/MICOM genome-scale models | Not downloaded | Replaces synthetic FBA → HIGH confidence |
| PICRUSt2 on MGnify genera | 95 communities ready (avg 18.8 genera) | Functional profiling of real communities |
| ENA portal geo metadata | Not fetched for MGnify communities | lat/lon for spatial analysis |
| GTDB-Tk + CheckM annotation | Not started | Raises genome model confidence |

---

## Recent Commits (HEAD → `82de70a`)

```
82de70a  fix(ingest): abundance-based soil filter + mgnify_results/ old-tree support
563e1e8  feat: findings_generator tracks MGnify FTP counts; FINDINGS.md updated
72853c5  feat: add ingest_mgnify_ftp.py — direct EBI FTP bulk ingest (no API/proxy)
1121f5b  feat: findings_generator tracks n_real_t025; updates gaps table
0f85e35  feat: bnf_trajectory_analysis --write-findings writes 5 key findings to DB
45b7d66  fix: run_t025_batch call uses server signature (config, db, workers)
45aa181  feat: T0.25 batch runner script for NEON t0_pass samples
```

---

## Next Priorities (suggested)

1. **NEON 16S amplicon path** — DP1.10108.001 (47 sites), sequences on SRA PRJNA393362. Download via SRA-tools, run DADA2, load OTU table for genus-level resolution → unblocks T1 FBA for real communities.
2. **Expand MGnify FTP coverage** — re-run `classify_mgnify_studies.py` with ERP115193 etc. (the `no_SILVA-SSU` group, which use `V6/unknown/` path now supported); then ingest confirmed soil studies.
3. **ENA geo metadata for MGnify** — re-run ingest without `--no-ena-meta` to populate lat/lon for 95 communities.
4. **Download AGORA2 models** — Zenodo release ~500 MB; enables real genome-scale T1 FBA for MGnify genera.
5. **PICRUSt2 on MGnify 95** — apply functional inference to the 95 real soil communities.

