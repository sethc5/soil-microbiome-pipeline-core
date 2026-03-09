# Pipeline Status Log

**Last updated**: 2026-03-09  
**Repo**: `sethc5/soil-microbiome-pipeline-core` — branch `main` @ `563e1e8`  
**Server**: `deploy@144.76.222.125` (Hetzner AX41, `/opt/pipeline/`, `/data/pipeline/`)

---

## Database State

| Source | Communities | T0-pass | T0.25-pass | T1 (flux) | T2 (stability) |
|--------|-------------|---------|-----------|-----------|----------------|
| **Synthetic** | 440,000 | 440,000 | 440,000 | 20,000 | 20,000 |
| **NEON** | 9,648 | 3,566 | 3,564 | 0 | 0 |
| **MGnify (FTP)** | 796 | 796 | 796 | 0 | 0 |
| **Total** | **450,444** | **444,362** | **444,360** | **20,000** | **20,000** |

- NEON: 9,346 / 9,648 samples have `soil_ph` populated
- MGnify: 796 soil communities from EBI amplicon-pipeline-v6 (9 ERP studies), all t0/t025 pass
- Synthetic: 20k communities through full T1+T2 (FBA + dFBA trajectory)

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
| `scripts/ingest_mgnify_ftp.py` | **Direct EBI FTP ingest — 796 communities in <2 min** | ✓ Active |
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
| T1 FBA for real NEON communities | 16S vsearch gives ~99.9% Unclassified at genus level; need shotgun or ITS | First real metabolic flux values |
| T1/T2 for MGnify communities | Same — FTP data only has phylum/genus profiles, no genome models | Expand to 796+ real communities |
| AGORA2/MICOM genome-scale models | Not downloaded | Replaces synthetic FBA → HIGH confidence |
| PICRUSt2 on NEON OTU table | vsearch pipeline outputs phylum profiles, not OTU BIOM | Functional profiling of real communities |
| NEON BONA site 16S | S3 transfer timeouts on NEON Alaska storage | 2 samples uncovered |
| More MGnify FTP studies | `mgnify_results/` tree has 27 study groups not yet ingested | Scale from 796 → potentially 5,000+ communities |
| ENA portal geo metadata | Not fetched (--no-ena-meta flag used) | lat/lon for MGnify communities |
| GTDB-Tk + CheckM annotation | Not started | Raises genome model confidence |

---

## Recent Commits (HEAD → `563e1e8`)

```
563e1e8  feat: findings_generator tracks MGnify FTP counts; FINDINGS.md updated
72853c5  feat: add ingest_mgnify_ftp.py — direct EBI FTP bulk ingest (no API/proxy)
1121f5b  feat: findings_generator tracks n_real_t025; updates gaps table
0f85e35  feat: bnf_trajectory_analysis --write-findings writes 5 key findings to DB
45b7d66  fix: run_t025_batch call uses server signature (config, db, workers)
45aa181  feat: T0.25 batch runner script for NEON t0_pass samples
5ea4d1e  feat: MGnify proxy support + JSONL offline-transfer ingest + tunnel script
c23959e  fix: barcode URL fallback for 16S; update findings confidence/gaps status
cb57855  Rewrite 16S pipeline: subsample 50K reads, skip R2/fastp/merge
```

---

## Next Priorities (suggested)

1. **Expand MGnify FTP coverage** — run `ingest_mgnify_ftp.py` against `mgnify_results/` study groups (27 groups, potentially 5k+ runs). Add `--studies` targeting soil-biome studies.
2. **Add ENA geo metadata** — re-run with `--no-ena-meta` removed to populate lat/lon for 796 MGnify communities.
3. **T1 FBA path for real data** — options: (a) use MGnify metagenome assemblies via FTP for genus resolution, (b) use SILVA genus-level classification with stricter thresholds, (c) integrate MICOM with AGORA2.
4. **Download AGORA2 models** — `http://bigg.ucsd.edu/` or AGORA2 Zenodo release; ~500 MB. Enables real genome-scale T1 FBA.
5. **Run T0.25 on MGnify communities** — already `t025_pass=1` by default; need to re-run ML scorer with actual phylum_profiles.
