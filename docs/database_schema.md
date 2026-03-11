# Database Schema Reference

SQLite database — schema v2. DDL source: [`db_utils.py`](../db_utils.py).  
Connect: `sqlite3 soil_microbiome.db` or `SoilDB("soil_microbiome.db")` from `db_utils`.

---

## Tables

| Table | Rows (current) | Purpose |
|-------|---------------|---------|
| `samples` | ~11,748 | Raw sample metadata from all adapters |
| `communities` | ~11,748 | Computed community composition + functional gene flags |
| `targets` | 1+ | Pipeline run target definitions (per config) |
| `runs` | 457,662 | Per–(sample, community, target) tier results |
| `interventions` | ~11 | Screened interventions from T2 |
| `taxa` | varies | Taxon reference rows (genus-level and above) |
| `findings` | ~7 | FINDINGS.md-backing analytical results |
| `receipts` | varies | Batch job accounting records |

---

## `samples`

One row per ingested sample (NEON, MGnify, SRA, local, etc.)

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `sample_id` | TEXT PK | — | Stable identifier (`NEON:CPER.2019.16S.001`, `SRA:SRR123456`, …) |
| `source` | TEXT | ✓ | `sra` · `mgnify` · `emp` · `qiita` · `neon` · `local` |
| `source_id` | TEXT | ✓ | Source-native ID |
| `project_id` | TEXT | ✓ | ENA project / SRA BioProject / NEON domain |
| `biome` | TEXT | ✓ | ENVO biome term (`ENVO:00000428`) |
| `feature` | TEXT | ✓ | ENVO environmental feature |
| `material` | TEXT | ✓ | ENVO environmental material |
| `sequencing_type` | TEXT | ✓ | `16S` · `ITS` · `shotgun_metagenome` · `metatranscriptome` |
| `sequencing_depth` | INTEGER | ✓ | Read count after QC |
| `n_taxa` | INTEGER | ✓ | Number of distinct taxa detected |
| `latitude` | REAL | ✓ | Decimal degrees |
| `longitude` | REAL | ✓ | Decimal degrees |
| `country` | TEXT | ✓ | ISO-3166 country |
| `climate_zone` | TEXT | ✓ | Koppen-Geiger classification (e.g. `Cfa`) |
| `soil_ph` | REAL | ✓ | Soil pH |
| `soil_texture` | TEXT | ✓ | USDA texture class (`clay loam`, `sandy loam`, …) |
| `clay_pct` | REAL | ✓ | % clay by mass |
| `sand_pct` | REAL | ✓ | % sand |
| `silt_pct` | REAL | ✓ | % silt |
| `bulk_density` | REAL | ✓ | g cm⁻³ |
| `organic_matter_pct` | REAL | ✓ | % OM |
| `total_nitrogen_ppm` | REAL | ✓ | Total N (ppm) |
| `available_p_ppm` | REAL | ✓ | Bray/Olsen P (ppm) |
| `cec` | REAL | ✓ | Cation exchange capacity (cmol kg⁻¹) |
| `moisture_pct` | REAL | ✓ | Gravimetric soil moisture (%) |
| `temperature_c` | REAL | ✓ | Mean annual or sampling-day temperature (°C) |
| `precipitation_mm` | REAL | ✓ | Mean annual precipitation (mm) |
| `land_use` | TEXT | ✓ | `cropland` · `grassland` · `forest` · `urban` · … |
| `management` | TEXT (JSON) | ✓ | Free-form management history dict |
| `sampling_depth_cm` | REAL | ✓ | Soil core depth (cm) |
| `sampling_season` | TEXT | ✓ | `spring` · `summer` · `fall` · `winter` |
| `sampling_date` | TEXT | ✓ | ISO date string |
| `site_id` | TEXT | ✓ | Stable site code for multi-visit grouping (e.g. `NEON:CPER`) |
| `visit_number` | INTEGER | ✓ | Chronological visit index at `site_id` |
| `sampling_fraction` | TEXT | ✓ | `rhizosphere` · `endosphere` · `bulk` · `litter` |
| `created_at` | TIMESTAMP | ✓ | Row insertion time |

**Indices:** `idx_samples_site` (site_id), `idx_samples_source` (source)

---

## `communities`

One row per sample. Holds computed diversity metrics, functional gene flags, and taxonomic profiles.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `community_id` | INTEGER PK | — | Auto-increment |
| `sample_id` | TEXT FK → samples | ✓ | |
| `shannon_diversity` | REAL | ✓ | Shannon H′ |
| `simpson_diversity` | REAL | ✓ | Simpson 1−D |
| `chao1_richness` | REAL | ✓ | Chao1 species richness estimate |
| `observed_otus` | INTEGER | ✓ | Observed OTU/ASV count |
| `pielou_evenness` | REAL | ✓ | Pielou J′ |
| `faith_pd` | REAL | ✓ | Faith's phylogenetic diversity |
| `has_nifh` | BOOLEAN | ✓ | nifH (nitrogen fixation) detected |
| `has_dsrab` | BOOLEAN | ✓ | dsrAB (sulfate reduction) detected |
| `has_mcra` | BOOLEAN | ✓ | mcrA (methanogenesis) detected |
| `has_mmox` | BOOLEAN | ✓ | Methane oxidation marker detected |
| `has_amoa_bacterial` | BOOLEAN | ✓ | Bacterial amoA (nitrification) |
| `has_amoa_archaeal` | BOOLEAN | ✓ | Archaeal amoA / AOA nitrification |
| `has_laccase` | BOOLEAN | ✓ | Laccase (lignin degradation / C-seq) |
| `has_peroxidase` | BOOLEAN | ✓ | Peroxidase (lignin degradation / C-seq) |
| `nifh_is_hgt_flagged` | BOOLEAN | ✓ | nifH present but flagged as likely HGT/non-functional |
| `functional_genes` | TEXT (JSON) | ✓ | Full gene profile with per-gene abundances |
| `fungal_bacterial_ratio` | REAL | ✓ | ITS:16S proxy ratio |
| `its_profile` | TEXT (JSON) | ✓ | ITS fungal taxonomy → relative abundance |
| `mrna_to_dna_ratio` | REAL | ✓ | Expression activity proxy (paired metatranscriptome) |
| `phylum_profile` | TEXT (JSON) | ✓ | Phylum → relative abundance |
| `top_genera` | TEXT (JSON) | ✓ | Top 50 genera → relative abundance |
| `otu_table_path` | TEXT | ✓ | Path to full OTU/ASV table file |
| `notes` | TEXT (JSON) | ✓ | Arbitrary provenance metadata (e.g. FASTQ URLs) |
| `created_at` | TIMESTAMP | ✓ | |

**Index:** `idx_communities_sample` (sample_id)

---

## `targets`

One row per pipeline run target (defined in config YAML, loaded at startup).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `target_id` | TEXT PK | — | Slug from config (e.g. `bnf_neon_cper`) |
| `application` | TEXT | ✓ | Free-text (e.g. `Fertilizer reduction`) |
| `description` | TEXT | ✓ | |
| `target_function` | TEXT | ✓ | Functional outcome name (`bnf_flux`) |
| `target_flux` | TEXT (JSON) | ✓ | `{min, optimal, units}` — see `TargetFluxSpec` |
| `soil_context` | TEXT (JSON) | ✓ | `{ph_range, texture, climate_zone, land_use, crop}` |
| `crop_context` | TEXT | ✓ | Target crop (if any) |
| `intervention_types` | TEXT (JSON) | ✓ | Allowed intervention categories |
| `off_targets` | TEXT (JSON) | ✓ | Functions that must not be harmed |
| `reference_communities` | TEXT (JSON) | ✓ | Known high-performing community IDs |

---

## `runs`

One row per (sample × community × target) combination processed through any tier. The primary result table.

### T0 columns

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | INTEGER PK | Auto-increment |
| `sample_id` | TEXT FK | |
| `community_id` | INTEGER FK | |
| `target_id` | TEXT FK | |
| `t0_pass` | BOOLEAN | Passed all T0 filters |
| `t0_reject_reason` | TEXT | Why rejected (null if passed) |
| `t0_depth_ok` | BOOLEAN | Sequencing depth gate |
| `t0_metadata_ok` | BOOLEAN | Required metadata present |
| `t0_functional_genes_ok` | BOOLEAN | Required gene flags present |

### T0.25 columns

| Column | Type | Description |
|--------|------|-------------|
| `t025_pass` | BOOLEAN | Passed ML/similarity screen |
| `t025_model` | TEXT | Model name used |
| `t025_function_score` | REAL | Surrogate-predicted BNF flux score |
| `t025_similarity_hit` | TEXT | Best reference community hit |
| `t025_similarity_score` | REAL | Bray-Curtis or UniFrac similarity |
| `t025_uncertainty` | REAL | Prediction uncertainty estimate |
| `t025_n_pathways` | INTEGER | PICRUSt2 pathway count (v2 migration) |
| `t025_nsti_mean` | REAL | Mean NSTI from PICRUSt2 (v2 migration) |

### T1 columns

| Column | Type | Description |
|--------|------|-------------|
| `t1_pass` | BOOLEAN | FVA flux ≥ threshold |
| `t1_model_size` | INTEGER | Number of organisms in community FBA model |
| `t1_target_flux` | REAL | FVA max flux for target reaction (mmol/gDW/h) |
| `t1_flux_lower_bound` | REAL | FVA lower bound (v2) |
| `t1_flux_upper_bound` | REAL | FVA upper bound (v2) |
| `t1_flux_units` | TEXT | Units (e.g. `mmol NH4/gDW/h`) |
| `t1_feasible` | BOOLEAN | LP feasibility flag |
| `t1_keystone_taxa` | TEXT (JSON) | Taxa most responsible for target flux |
| `t1_genome_completeness_mean` | REAL | Mean CheckM completeness % across genera (v2) |
| `t1_genome_contamination_mean` | REAL | Mean CheckM contamination % (v2) |
| `t1_model_confidence` | TEXT | `high` · `medium` · `low` based on genome quality (v2) |
| `t1_metabolic_exchanges` | TEXT (JSON) | Cross-feeding exchange fluxes (v2 migration) |
| `t1_walltime_s` | REAL | FBA wall-clock time (seconds) |

### T2 columns

| Column | Type | Description |
|--------|------|-------------|
| `t2_pass` | BOOLEAN | Stability score ≥ threshold |
| `t2_stability_score` | REAL | Community resilience + resistance score |
| `t2_best_intervention` | TEXT (JSON) | Top-ranked intervention (type, detail, effect) |
| `t2_intervention_effect` | REAL | Predicted fractional flux improvement |
| `t2_establishment_prob` | REAL | Inoculant establishment probability |
| `t2_off_target_impact` | TEXT (JSON) | Off-target function changes |
| `t2_confidence` | TEXT | Propagated from `t1_model_confidence` |
| `t2_resistance` | REAL | Resistance to acute perturbation (v2) |
| `t2_resilience` | REAL | Recovery rate after perturbation (v2) |
| `t2_functional_redundancy` | REAL | Functional redundancy across keystone taxa (v2) |
| `t2_interventions` | TEXT (JSON) | Full ranked intervention list (v2) |
| `t2_walltime_s` | REAL | dFBA wall-clock time (seconds) |

### Provenance

| Column | Type | Description |
|--------|------|-------------|
| `tier_reached` | INTEGER | Highest tier completed (0, 25, 1, 2) |
| `run_date` | TIMESTAMP | |
| `machine_id` | TEXT | Hetzner node or local hostname |

**Indices:** `idx_runs_target`, `idx_runs_tier`, `idx_runs_t1`, `idx_runs_t2`, `idx_runs_t025`, `idx_runs_community`

---

## `interventions`

One row per screened intervention record (T2 output). The `runs.t2_best_intervention` JSON covers the top recommendation; this table holds the full panel.

| Column | Type | Description |
|--------|------|-------------|
| `intervention_id` | INTEGER PK | |
| `run_id` | INTEGER FK → runs | |
| `intervention_type` | TEXT | `bioinoculant` · `amendment` · `management` |
| `intervention_detail` | TEXT (JSON) | Organism/compound/practice specifics |
| `predicted_effect` | REAL | Fractional flux change predicted |
| `confidence` | REAL | Model confidence score |
| `stability_under_perturbation` | REAL | Stability of effect under climate perturbation panel |
| `cost_estimate` | TEXT (JSON) | Rough cost breakdown |
| `created_at` | TIMESTAMP | |

---

## `taxa`

Reference table for taxa encountered in the pipeline. Populated by `tax_profiler.py` and `genome_fetcher.py`.

| Column | Type | Description |
|--------|------|-------------|
| `taxon_id` | TEXT PK | NCBI taxonomy ID or internal slug |
| `name` | TEXT | Scientific name |
| `rank` | TEXT | `phylum` · `class` · `order` · `family` · `genus` · `species` |
| `phylum` | TEXT | Lineage |
| `class` | TEXT | |
| `order_name` | TEXT | (`order` is a SQL keyword — stored as `order_name`) |
| `family` | TEXT | |
| `genus` | TEXT | |
| `species` | TEXT | |
| `functional_roles` | TEXT (JSON) | Known functional roles (`nitrogen_fixer`, `AOA`, …) |
| `genome_accession` | TEXT | Representative genome accession (BV-BRC / NCBI) |
| `created_at` | TIMESTAMP | |

---

## `findings`

Backing store for FINDINGS.md entries generated by `findings_generator.py`.

| Column | Type | Description |
|--------|------|-------------|
| `finding_id` | INTEGER PK | |
| `title` | TEXT | Short finding title |
| `description` | TEXT | Full prose description |
| `sample_ids` | TEXT (JSON) | Array of relevant `sample_id` values |
| `taxa_ids` | TEXT (JSON) | Array of relevant `taxon_id` values |
| `statistical_support` | TEXT (JSON) | `{p_value, effect_size, n, model_confidence_dist}` |
| `created_at` | TIMESTAMP | |

---

## `receipts`

Batch job accounting records written by remote workers and merged by `merge_receipts.py`.

| Column | Type | Description |
|--------|------|-------------|
| `receipt_id` | TEXT PK | UUID assigned by worker |
| `machine_id` | TEXT | Hetzner node hostname |
| `batch_start` | TIMESTAMP | |
| `batch_end` | TIMESTAMP | |
| `n_samples_processed` | INTEGER | |
| `n_fba_runs` | INTEGER | |
| `n_dynamics_runs` | INTEGER | |
| `status` | TEXT | `complete` · `partial` · `error` |
| `filepath` | TEXT | Path to JSON receipt file |

---

## Schema Versions

| Version | Change | Commit |
|---------|--------|--------|
| v1 | Initial schema: samples, communities, targets, runs, interventions, taxa, findings, receipts | baseline |
| v2 | Added: `samples.site_id`, `samples.visit_number`, `samples.sampling_fraction`; archaeal/fungal/expression columns to `communities`; FVA bounds, genome quality, confidence, dynamics columns to `runs` | 1429734 |

To migrate an existing v1 database, `SoilDB.__init__` applies `MIGRATION_SQL` automatically (idempotent `ALTER TABLE` with `try/except`).

---

## Common Query Patterns

```sql
-- T1+T2 pass communities with flux and stability
SELECT r.run_id, s.sample_id, s.soil_ph, s.climate_zone,
       r.t1_target_flux, r.t2_stability_score, r.t2_best_intervention
FROM runs r
JOIN samples s ON s.sample_id = r.sample_id
WHERE r.t1_pass AND r.t2_pass
ORDER BY r.t1_target_flux DESC
LIMIT 100;

-- Surrogate score distribution across T0-pass runs
SELECT ROUND(t025_function_score, 1) AS score_bin, COUNT(*) AS n
FROM runs
WHERE t0_pass AND t025_function_score IS NOT NULL
GROUP BY score_bin
ORDER BY score_bin;

-- NEON site BNF trajectory (multi-visit)
SELECT s.site_id, s.visit_number, s.sampling_date,
       AVG(r.t1_target_flux) AS mean_bnf_flux
FROM runs r
JOIN samples s ON s.sample_id = r.sample_id
WHERE r.t1_pass AND s.source = 'neon'
GROUP BY s.site_id, s.visit_number
ORDER BY s.site_id, s.visit_number;

-- Keystone taxa frequency in BNF-passing communities
SELECT json_each.value AS genus, COUNT(*) AS n_communities
FROM runs, json_each(runs.t1_keystone_taxa)
WHERE runs.t1_pass
GROUP BY genus
ORDER BY n_communities DESC
LIMIT 20;

-- Intervention type breakdown
SELECT intervention_type, COUNT(*) AS n,
       ROUND(AVG(predicted_effect), 3) AS mean_effect
FROM interventions
GROUP BY intervention_type;
```

---

## JSON Column Schemas

### `communities.functional_genes`
```json
{
  "nifH": {"present": true, "relative_abundance": 0.003},
  "amoA_bacterial": {"present": false},
  "dsrAB": {"present": true, "relative_abundance": 0.001}
}
```

### `runs.t1_keystone_taxa`
```json
["Bradyrhizobium", "Azotobacter", "Burkholderia"]
```

### `runs.t2_best_intervention`
```json
{
  "type": "bioinoculant",
  "organism": "Bradyrhizobium japonicum USDA 110",
  "dose_rate": "1e8 CFU/g soil",
  "predicted_effect": 0.22,
  "confidence": 0.71
}
```

### `targets.target_flux`
```json
{"min": 0.01, "optimal": "maximize", "units": "mmol NH4/gDW/h"}
```
