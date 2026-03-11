# Pipeline Status Log

**Last updated**: 2026-03-10 (session 3)  
**Repo**: `sethc5/soil-microbiome-pipeline-core` — branch `main` @ `25de5c8`  
**Server**: `deploy@<HETZNER2_HOST>` (Hetzner AX41, `/opt/pipeline/`, `/data/pipeline/`)

---

## Database State

| Source | Communities | T0-pass | T1-pass | T2-pass |
|--------|-------------|---------|---------|--------|
| **NEON** | 9,648 | 5,907 | 4,491 | 3,378 |
| **MGnify** | 95 | 95 | 0 | 0 |
| **Synthetic** | 440,000 | 440,000 | 0 | 0 |
| **Total runs** | 457,662 | — | 4,491 | 3,378 |

- NEON 16S amplicon (DP1.10108.001): 9,346 / 9,648 samples have `soil_ph`; genus-level profiles loaded
- MGnify: 95 real soil communities from 4 ERP studies, all T0-pass, not yet through T1
- T1 complete: 4,491 total (3,378 BNF-pass, max flux=50.0, avg=36.23 mmol NH₄/gDW/h)
- T2 complete: 3,378 t2_pass (stability ≥ 0.30), 23,378 dFBA trajectory records in DB

---

## Completed This Session (commits cf9ef98 → 25de5c8)

| Commit | What |
|--------|------|
| cf9ef98 | findings: refresh from real DB (23,378 communities, NEON taxa enrichment) |
| f8f0995 | findings: Spatial Distribution & Kriging section; make_spatial_map.py |
| 828d893 | docs: Diagram 2 refresh to current state; licence fix in CONTRIBUTING.md |
| cf5e081 | feat(t025): train BNF surrogate RF predictor from 5,907 real samples; classifier gate |
| 6ee5d79 | feat(validate): upgrade Check 2 to RF-surrogate Spearman test; make_reference_bnf.py |
| 1429734 | feat(items 5-8): AGORA2 plan, SOC config, T2 metadata enrichment, site BNF tracker |
| 25de5c8 | chore: tidy and organise repo structure |

---

## 8-Item Audit — Status

| # | Item | Status | Commit |
|---|------|--------|--------|
| 1 | Fix Diagram 2 to current state | ✅ Done | 828d893 |
| 2 | Fix CONTRIBUTING.md licence (MIT → PolyForm NC) | ✅ Done | 828d893 |
| 3 | Train T0.25 surrogate RF predictor (Addition C) | ✅ Done | cf5e081 |
| 4 | validate_pipeline.py vs real BNF data | ✅ Done | 6ee5d79 |
| 5 | AGORA2 integration plan | ✅ Done | 1429734 |
| 6 | Carbon sequestration config instantiation | ✅ Done | 1429734 |
| 7 | Wire intervention_screener full metadata (13 fields) | ✅ Done | 1429734 |
| 8 | Time-series visit tracking | ✅ Done | 1429734 |

---

## Surrogate Predictor (T0.25 — Addition C)

| Metric | Classifier gate | Regressor |
|--------|-----------------|-----------|
| Algorithm | RandomForestClassifier (balanced) | RandomForestRegressor |
| Training set | 5,907 NEON communities | 4,491 BNF-pass communities |
| OOB accuracy / R² | 0.772 | 0.469 |
| CV score | ROC-AUC 0.812 ± 0.012 | R² 0.465 ± 0.025 |
| Top features | soil_ph (42%), Nitrososphaerota (19%), Nitrospirota (12%) | — |
| Model files | `models/functional_predictor.joblib` (canonical, embedded classifier) | |
| API | `predict_with_gate(features, gate_threshold=0.4)` → (flux, unc, pass) | |

---

## Findings in DB

FINDINGS.md fully refreshed from real DB (commit f8f0995). Full content:

| Section | Data |
|---------|------|
| Pipeline run summary | 23,378 communities, 4,491 T1-pass, 3,378 T2-pass |
| BNF × land use | Rangeland/grassland highest flux |
| BNF stability analysis | 90% mean retention, CLBJ + GUAN top sites |
| Taxa enrichment | 27/122 NEON phyla significant; Proteobacteria (3.2×) enriched |
| Spatial clusters | 7 clusters, Puerto Rico cluster mean 311.0, 6,413-point CONUS kriging grid |
| Ranked candidates | Top 100 communities |
| Interventions | 11 recommendations |

---

## Analysis Outputs (`results/`)

| File | Description | Status |
|------|-------------|--------|
| `bnf_trajectory_summary.csv` | dFBA trajectories, 23,378 rows | ✓ |
| `ranked_candidates.csv` | Top 100 ranked BNF communities | ✓ |
| `keystone_analysis.csv` | Leave-one-out keystone taxa | ✓ |
| `taxa_enrichment.csv` | 27/122 NEON phyla significant | ✓ |
| `spatial/bnf_spatial_map.png` | Dark-background kriging heatmap + cluster scatter | ✓ |
| `intervention_portfolio.csv` | 11 interventions, cost-effectiveness ranking | ✓ |
| `site_bnf_timeseries.csv` | Per-site BNF trajectory (multi-visit NEON) | Runnable |
| `results/validation_report.json` | validate_pipeline.py output | Runnable |

---

## Key Analysis Results (Real BNF Data)

### BNF Trajectory
- 23,378 communities with dFBA trajectories in DB
- Top sites: CLBJ (Texas savanna), GUAN (Puerto Rico)
- Mean retention: 90%
- Max flux: 50.0 mmol NH₄/gDW/h · Avg: 36.23

### Spatial Distribution
- 7 geographic clusters; 6,413-point CONUS kriging grid
- Puerto Rico cluster: mean BNF 311.0 (highest)
- Midwest prairie cluster: mean BNF 28.4

### Taxa Enrichment
- 27/122 NEON phyla significant (Mann-Whitney, FDR corrected)
- Proteobacteria: 3.2× enriched in high-BNF communities
- Nitrososphaerota: 19% of surrogate predictor importance

---

## Infrastructure

| Component | Status |
|-----------|--------|
| Server (Hetzner AX41) | ✓ Running — uvicorn API on port 8000 |
| SQLite DB (WAL mode) | ✓ `/data/pipeline/db/soil_microbiome.db` |
| Python venv | ✓ `/opt/pipeline/.venv` |
| vsearch + SILVA 138 | ✓ Installed, used for 16S classification |
| PICRUSt2 | ✓ Installed (v2.6.3) — not yet applied to NEON OTUs |
| SRA-tools | ✓ v3.x installed |
| Surrogate RF predictor | ✓ `models/functional_predictor.joblib` (5,907-sample training) |

---

## Open Gaps / Next Steps

| Gap | Impact | Notes |
|-----|--------|-------|
| AGORA2 genus-level SBML models | High | Plan: `docs/agora2_integration_plan.md`; replaces 20 synthetic stubs |
| validate_pipeline.py forward-validation run | Medium | Generate `reference/bnf_measurements.csv` via `scripts/make_reference_bnf.py` then run |
| NEON multi-visit BNF time-series | Medium | `scripts/track_site_bnf.py` ready; requires `visit_number` data in DB |
| PICRUSt2 on NEON 16S communities | Medium | 5,907 classified communities ready |
| SOC pipeline first run | Medium | `configs/soil_carbon.yaml` instantiated; need SOC-specific SBML models |
| Field validation package | Low | 15N measurement protocol + site selection map |
| ENA geo metadata for MGnify 95 | Low | Populate lat/lon for spatial analysis |

---

## Recent Commits (HEAD → `25de5c8`)

```
25de5c8  chore: tidy and organise repo structure
1429734  feat(items 5-8): AGORA2 plan, SOC config, T2 metadata enrichment, site BNF tracker
6ee5d79  feat(validate): upgrade Check 2 to RF-surrogate Spearman test; add make_reference_bnf.py
cf5e081  feat(t025): train BNF surrogate RF predictor from 5907 real samples; wire classifier gate
828d893  docs: update Diagram 2 to current state; fix licence in CONTRIBUTING
f8f0995  feat(findings): add Spatial Distribution & Kriging section; add make_spatial_map.py
cf9ef98  findings: refresh from real DB (23,378 communities, NEON taxa enrichment)
```

---
