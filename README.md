# soil-microbiome-pipeline-core

**A reusable, systematic computational screening pipeline for soil microbiome research — built on open-source metagenomics, metabolic modeling, and community ecology tools, designed for deployment across any soil health, agriculture, carbon sequestration, or bioremediation target.**

A single gram of healthy soil contains ~10,000 bacterial species, ~200 meters of fungal hyphae, and more metabolic diversity than any other ecosystem on Earth. The Earth Microbiome Project has catalogued ~300,000 unique microbial taxa from 97 countries. NCBI SRA contains millions of soil metagenome samples. EBI MGnify has processed 500,000+ metagenomes. The functional potential encoded in these communities — nitrogen fixation, carbon sequestration, plant growth promotion, pathogen suppression, pollutant degradation — is almost entirely unmapped at the mechanistic level.

This pipeline provides systematic infrastructure to scan that space. The "candidates" here are not molecules or crystal structures — they are **microbial community compositions, functional guilds, and intervention strategies** (bioinoculants, amendments, land management practices) predicted to shift soil microbiome function toward a defined target state. The same 4-tier funnel logic from [biochem-pipeline-core](https://github.com/sethc5/biochem-pipeline-core), [materials-pipeline-core](https://github.com/sethc5/materials-pipeline-core), and [genomics-pipeline-core](https://github.com/sethc5/genomics-pipeline-core) applies — cheap filters first, expensive simulation last, everything logged to a database that accumulates scientific value over time.

> **Status**: T1 + T2 complete · 4,491 real communities screened · BNF surrogate RF trained (ROC-AUC 0.812) · AGORA2 integration planned · [Contributors welcome](CONTRIBUTING.md)

---

## What Problem This Solves

Soil science has a severe infrastructure gap between data richness and analytical capability:

**The data exists.** Millions of 16S amplicon and shotgun metagenome samples are publicly available, spanning every soil type, climate, land use, and management history on Earth. The sequencing revolution has been extraordinarily productive.

**The interpretation pipeline doesn't exist.** Most soil microbiome studies stop at community composition — which taxa are present, in what relative abundance. The hard questions — which communities produce which functional outcomes, which interventions reliably shift communities toward target states, which microbial interactions are mechanistically responsible for measured ecosystem services — are almost never answered systematically. The tools to answer them exist individually (COBRApy, QIIME2, Bracken, HUMAnN3) but nobody has built the systematic scanning infrastructure that orchestrates them into reproducible, resumable, database-backed pipelines.

**The stakes are enormous.** Agriculture accounts for 70% of global freshwater use and 25% of greenhouse gas emissions. Soil microbiome function underlies all of it — nitrogen cycling reduces synthetic fertilizer need, carbon sequestration in soil is one of the largest potential carbon sinks on Earth, and pathogen suppression reduces pesticide use. A 10% improvement in nitrogen fixation efficiency across global agriculture would eliminate the need for ~18 million tons of synthetic nitrogen fertilizer annually.

This pipeline builds the systematic interpretation layer. It is domain-agnostic at the infrastructure level and instantiated per application via config YAML.

---

## Why Soil Microbiome Is a Distinct Codebase

Unlike biochem (compound vs protein target) and materials (composition vs property target), soil microbiome research involves a fundamentally different kind of search space:

**The candidate is a community, not a molecule.** A "hit" is a microbial consortium — a set of interacting species with defined functional roles — not a single compound or crystal structure. Community composition is high-dimensional (thousands of taxa), context-dependent (the same taxa behave differently in different soils), and dynamic (communities shift over time and in response to perturbation).

**The property target is emergent.** Nitrogen fixation rate, carbon sequestration potential, crop yield — these properties emerge from network-level interactions between community members, not from individual organism properties. This requires metabolic network modeling tools (COBRApy community FBA) that have no analog in biochem or materials pipelines.

**The intervention space is different.** In biochem you add a compound. In materials you synthesize a composition. In soil you add organisms (bioinoculants), organic amendments (compost, biochar), or change management practices (tillage, cover crops, irrigation). The pipeline must model intervention effects on community dynamics, not just screen static candidates.

**Environmental metadata is load-bearing.** A microbial community that works in sandy loam at pH 6.5 may fail completely in clay-heavy soil at pH 5.0. Soil pH, texture, organic matter content, temperature, moisture, and land use history are not peripheral context — they are primary determinants of whether a candidate community can establish and function. The database schema must treat metadata as first-class.

These differences justify a separate codebase. The architectural pattern (4-tier funnel, SQLite, receipts, findings generator) is shared; the domain-specific compute layer is entirely different.

---

## Architecture

### The 4-Tier Screening Funnel

```
T0   (milliseconds/sample): Community composition + metadata filters
  └─ SKIP if: sequencing depth below threshold (unreliable diversity estimates)
  └─ SKIP if: soil metadata outside target range (pH, texture, climate zone)
  └─ SKIP if: target functional genes absent (e.g. nifH for nitrogen fixation)
  └─ SKIP if: diversity indices outside empirical bounds
  └─ COMPUTE: Shannon diversity, Chao1 richness, functional gene presence/absence

T0.25 (seconds/sample):    ML functional outcome prediction + fast similarity search
  └─ PROMOTE if: ML-predicted target function score above threshold
  └─ Uses: random forest / gradient boosting on OTU table + metadata features
  └─ Fast sample similarity search against reference community database
  └─ Functional profile prediction (HUMAnN3 shortcut models)

T1   (minutes/sample):     Metabolic network modeling + community flux analysis
  └─ Engine: COBRApy community FBA (flux balance analysis)
  └─ Build community metabolic model from member genome annotations
  └─ Score by: predicted flux through target pathway (N fixation, C sequestration)
  └─ PROMOTE if: predicted flux exceeds threshold AND community is metabolically feasible

T2   (hours/sample):       Community dynamics simulation + intervention modeling
  └─ Engine: dFBA (dynamic FBA) or agent-based community simulation
  └─ Model community response to proposed intervention (amendment, inoculant)
  └─ Score by: stability of target function under perturbation, establishment probability
  └─ Output: ranked communities + ranked interventions with mechanistic explanation
```

### What "Candidate" Means at Each Tier

**T0 candidate:** A soil metagenome sample from a database (NCBI SRA, MGnify, EMP) that passes basic quality and metadata filters. At scale, this means scanning hundreds of thousands of samples.

**T0.25 candidate:** A sample predicted by ML to have high target functional activity. The functional prediction is based on community composition features (OTU relative abundances, diversity indices) and environmental metadata — not direct measurement.

**T1 candidate:** A community whose metabolic network model predicts feasible flux through the target pathway. This is where the biology gets mechanistic — you're modeling which metabolic exchanges between community members enable the target function.

**T2 candidate:** A community that maintains target function under simulated perturbation AND for which a specific intervention (addition of a known nitrogen fixer, amendment with biochar to adjust pH) predictably improves function. T2 output is the actionable recommendation: "in soils with these characteristics, add this organism at this concentration."

### Screening Funnel at Scale

**Example: nitrogen fixation enhancement for dryland wheat systems**
```
2,000,000  soil metagenome samples in NCBI SRA
  └─ ~200,000   pass T0 quality + dryland wheat metadata filter (10%)
      └─ ~20,000    pass T0.25 ML nitrogen fixation potential prediction (10%)
          └─ ~2,000     pass T1 COBRApy community FBA flux threshold (10%)
              └─ ~200      pass T2 stability + intervention modeling (10%)
                  └─ top 20-50 communities + intervention strategies for field validation
```

---

## Instantiation Model

```
soil-microbiome-pipeline-core/    ← this repo (the template)
  pipeline_core.py
  db_utils.py
  receipt_system.py
  config_schema.py
  adapters/
    ncbi_sra_adapter.py           ← NCBI SRA metagenome download + metadata
    mgnify_adapter.py             ← EBI MGnify API
    emp_adapter.py                ← Earth Microbiome Project
    qiita_adapter.py              ← Qiita public microbiome database
    local_biom_adapter.py         ← local BIOM/FASTA input
    agp_adapter.py                ← American Gut Project
    neon_adapter.py               ← NEON soil data portal

nitrogen-fixation-pipeline/
  config.yaml                     ← dryland wheat, nifH gene target, N flux target
  nitrogen_landscape.db
  FINDINGS.md

carbon-sequestration-pipeline/
  config.yaml                     ← SOC accumulation target, grassland/forest context
  carbon_landscape.db
  FINDINGS.md

bioremediation-pipeline/
  config.yaml                     ← petroleum hydrocarbon degradation, contaminated sites
  remediation_landscape.db
  FINDINGS.md

plant-growth-promotion-pipeline/
  config.yaml                     ← PGPR taxa, crop yield proxy, rhizosphere context
  pgp_landscape.db
  FINDINGS.md

pathogen-suppression-pipeline/
  config.yaml                     ← Fusarium / Rhizoctonia suppression, suppressiveness score
  suppression_landscape.db
  FINDINGS.md
```

---

## Database Schema

The schema must treat environmental metadata as first-class — it is not peripheral annotation but primary determinant of community function.

```sql
-- Core tables
CREATE TABLE samples (
    sample_id           TEXT PRIMARY KEY,   -- SRA accession or local ID
    source              TEXT,               -- 'sra', 'mgnify', 'emp', 'qiita', 'local'
    source_id           TEXT,               -- ID in source database
    project_id          TEXT,               -- study/project accession
    biome               TEXT,               -- ENVO biome term e.g. 'cropland biome'
    feature             TEXT,               -- ENVO feature e.g. 'wheat field'
    material            TEXT,               -- ENVO material e.g. 'soil'
    sequencing_type     TEXT,               -- '16S', 'ITS', 'shotgun_metagenome', 'metatranscriptome'
    sequencing_depth    INTEGER,            -- total reads
    n_taxa              INTEGER,            -- observed taxa count

    -- Environmental metadata (load-bearing)
    latitude            REAL,
    longitude           REAL,
    country             TEXT,
    climate_zone        TEXT,               -- Koppen-Geiger classification
    soil_ph             REAL,
    soil_texture        TEXT,               -- 'sand', 'silt', 'clay', 'loam', etc.
    clay_pct            REAL,
    sand_pct            REAL,
    silt_pct            REAL,
    bulk_density        REAL,               -- g/cm³
    organic_matter_pct  REAL,               -- SOC proxy
    total_nitrogen_ppm  REAL,
    available_p_ppm     REAL,
    cec                 REAL,               -- cation exchange capacity
    moisture_pct        REAL,
    temperature_c       REAL,               -- mean annual or sampling temp
    precipitation_mm    REAL,               -- mean annual
    land_use            TEXT,               -- 'cropland', 'forest', 'grassland', 'wetland'
    management          TEXT,               -- JSON: tillage, fertilization, crop history
    sampling_depth_cm   REAL,               -- soil horizon sampled
    sampling_season     TEXT,
    sampling_date       TEXT,

    -- Extended spatial / visit tracking (Phase 8)
    site_id             TEXT,               -- persistent site identifier across visits
    visit_number        INTEGER,            -- sequential visit to the same site
    sampling_fraction   REAL,               -- fraction of site area sampled (0–1)

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE communities (
    community_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,

    -- Diversity metrics (computed at T0)
    shannon_diversity   REAL,
    simpson_diversity   REAL,
    chao1_richness      REAL,
    observed_otus       INTEGER,
    pielou_evenness     REAL,
    faith_pd            REAL,               -- phylogenetic diversity

    -- Functional gene presence (T0 filter)
    has_nifh            BOOLEAN,            -- nitrogen fixation
    has_dsrab           BOOLEAN,            -- sulfate reduction
    has_mcra            BOOLEAN,            -- methanogenesis
    has_mmox            BOOLEAN,            -- methane oxidation
    has_amoa            BOOLEAN,            -- ammonia oxidation (nitrification)
    functional_genes    TEXT,               -- JSON: full functional gene profile

    -- Taxonomic composition (stored as compressed profile)
    phylum_profile      TEXT,               -- JSON: phylum → relative abundance
    top_genera          TEXT,               -- JSON: top 50 genera by abundance
    otu_table_path      TEXT,               -- path to full OTU/ASV table file

    -- Extended community metrics (Phase 8)
    fungal_bacterial_ratio REAL,            -- ITS/16S ratio (requires paired sequencing)
    has_amoa_bacterial  BOOLEAN,            -- bacterial amoA (Nitrosomonas/Nitrosospira)
    has_amoa_archaeal   BOOLEAN,            -- archaeal amoA (Thaumarchaeota)
    its_profile         TEXT,               -- JSON: fungal phylum → relative abundance
    mrna_to_dna_ratio   REAL,               -- metatranscriptomic activity proxy

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE targets (
    target_id           TEXT PRIMARY KEY,   -- e.g. 'nitrogen_fixation_dryland'
    application         TEXT,               -- e.g. 'nitrogen_fixation'
    description         TEXT,
    target_function     TEXT,               -- primary ecosystem service
    target_flux         TEXT,               -- JSON: pathway → {min_flux, units}
    soil_context        TEXT,               -- JSON: pH range, texture, climate
    crop_context        TEXT,               -- target crop or vegetation type
    intervention_types  TEXT,               -- JSON: allowed intervention strategies
    off_targets         TEXT,               -- JSON: functions to avoid disrupting
    reference_communities TEXT              -- JSON: known high-performing communities
);

CREATE TABLE runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,
    community_id        INTEGER REFERENCES communities,
    target_id           TEXT REFERENCES targets,

    -- T0 results
    t0_pass             BOOLEAN,
    t0_reject_reason    TEXT,
    t0_depth_ok         BOOLEAN,
    t0_metadata_ok      BOOLEAN,
    t0_functional_genes_ok BOOLEAN,

    -- T0.25 results (ML prediction)
    t025_pass           BOOLEAN,
    t025_model          TEXT,
    t025_function_score REAL,               -- predicted target function score
    t025_similarity_hit TEXT,               -- nearest reference community
    t025_similarity_score REAL,
    t025_uncertainty    REAL,

    -- T1 results (metabolic modeling)
    t1_pass             BOOLEAN,
    t1_model_size       INTEGER,            -- number of organisms in community model
    t1_target_flux      REAL,               -- predicted flux through target pathway
    t1_flux_units       TEXT,               -- e.g. 'mmol N / g soil / day'
    t1_feasible         BOOLEAN,            -- is community metabolically feasible?
    t1_keystone_taxa    TEXT,               -- JSON: taxa most responsible for target flux
    t1_walltime_s       REAL,

    -- T2 results (dynamics + intervention)
    t2_pass             BOOLEAN,
    t2_stability_score  REAL,               -- community stability under perturbation
    t2_best_intervention TEXT,              -- JSON: recommended intervention
    t2_intervention_effect REAL,            -- predicted improvement in target function
    t2_establishment_prob REAL,             -- probability inoculant establishes
    t2_off_target_impact TEXT,              -- JSON: effects on non-target functions
    t2_walltime_s       REAL,

    -- Confidence propagation (Phase 8.4)
    t1_model_confidence TEXT,               -- 'high'/'medium'/'low' from CheckM genome completeness
    t1_flux_lower_bound REAL,               -- FVA lower bound on target flux
    t1_flux_upper_bound REAL,               -- FVA upper bound on target flux
    t2_confidence       TEXT,               -- propagated model_confidence from dFBA run

    tier_reached        INTEGER,
    run_date            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    machine_id          TEXT
);

CREATE TABLE interventions (
    intervention_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              INTEGER REFERENCES runs,
    intervention_type   TEXT,               -- 'bioinoculant', 'amendment', 'management'
    intervention_detail TEXT,               -- JSON: organism IDs / amendment specs
    predicted_effect    REAL,               -- predicted change in target function
    confidence          REAL,
    stability_under_perturbation REAL,
    cost_estimate       TEXT,               -- JSON: rough cost per hectare
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE taxa (
    taxon_id            TEXT PRIMARY KEY,   -- NCBI taxonomy ID or OTU hash
    name                TEXT,
    rank                TEXT,               -- 'species', 'genus', 'family', etc.
    phylum              TEXT,
    class               TEXT,
    order_name          TEXT,
    family              TEXT,
    genus               TEXT,
    species             TEXT,
    functional_roles    TEXT,               -- JSON: known functional roles
    genome_accession    TEXT,               -- NCBI genome accession if available
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE findings (
    finding_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title               TEXT,
    description         TEXT,
    sample_ids          TEXT,               -- JSON array
    taxa_ids            TEXT,               -- JSON array
    statistical_support TEXT,               -- JSON: p_value, effect_size, n
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE receipts (
    receipt_id          TEXT PRIMARY KEY,
    machine_id          TEXT,
    batch_start         TIMESTAMP,
    batch_end           TIMESTAMP,
    n_samples_processed INTEGER,
    n_fba_runs          INTEGER,            -- metabolic modeling is expensive, track
    n_dynamics_runs     INTEGER,            -- T2 dynamics, most expensive
    status              TEXT,
    filepath            TEXT
);
```

---

## Config Schema

```yaml
# nitrogen-fixation-pipeline/config.yaml — example

project:
  name: "nitrogen-fixation-pipeline"
  application: "nitrogen_fixation"
  description: "Identify soil communities with high biological nitrogen fixation potential for dryland wheat systems. Target: reduce synthetic N fertilizer dependency."
  version: "0.1.0"

target:
  id: "nitrogen_fixation_dryland_wheat"
  application: "nitrogen_fixation"
  target_function: "biological_nitrogen_fixation"
  target_flux:
    nifH_pathway:
      min: 0.5                             # mmol N / g soil / day (threshold)
      optimal: ">2.0"
      units: "mmol_N_per_g_soil_per_day"
  off_targets:
    - "denitrification"                    # don't want N loss
    - "methane_production"                 # don't want GHG increase
  soil_context:
    ph_range: [5.5, 7.5]
    texture: ["sandy_loam", "loam", "silt_loam"]
    climate_zone: ["BSk", "BWk", "Csa"]   # semi-arid, dryland Koppen codes
    land_use: ["cropland"]
    crop: "wheat"
  intervention_types:
    - "bioinoculant"                       # add N-fixing organisms
    - "amendment"                          # biochar, compost to improve habitat
    - "management"                         # reduced tillage, cover crops

sequence_source:
  primary: "sra"
  sra_query:
    biome: "cropland"
    sequencing_type: ["shotgun_metagenome", "16S"]
    min_depth: 50000
    metadata_required: ["soil_ph", "latitude", "longitude", "land_use"]
  supplementary:
    - source: "mgnify"
      biome_lineage: "root:Environmental:Terrestrial:Agricultural soil"
    - source: "emp"
      empo_3: "Soil (non-saline)"

filters:
  t0:
    min_sequencing_depth: 50000            # reads
    min_observed_otus: 500
    ph_range: [5.0, 8.5]                   # slightly looser than target
    required_functional_genes: ["nifH"]    # must have N fixation genes
    exclude_contaminated: true             # remove samples with human/animal contamination
    min_soil_organic_matter: 0.5          # % — extremely degraded soils excluded
    exclude_flooded: true                  # anaerobic soils behave differently
  t025:
    ml_models: ["random_forest_bnf", "gradient_boost_functional"]
    min_bnf_score: 0.5                     # ML-predicted BNF activity threshold
    min_nifh_abundance: 0.001              # nifH relative abundance in functional profile
    reference_db: "reference_communities/high_bnf_communities.biom"
    min_similarity: 0.3                    # similarity to reference high-BNF communities
  t1:
    fba_engine: "cobrapy"
    community_size_limit: 20              # max organisms in community FBA model
    genome_db: "bv-brc"                   # source for member genome annotations (BV-BRC, formerly PATRIC)
    min_target_flux: 0.5
    max_fba_walltime_min: 30
  t2:
    dynamics_engine: "dfba"              # dynamic FBA for time-course modeling
    simulation_time_days: 90             # growing season
    perturbations:
      - type: "drought"
        severity: 0.5
      - type: "fertilizer_addition"
        amount_kg_ha: 50
    intervention_screen:
      bioinoculants:
        - "Azospirillum_brasilense"
        - "Herbaspirillum_seropedicae"
        - "Gluconacetobacter_diazotrophicus"
        - "Paenibacillus_polymyxa"
      amendments:
        - type: "biochar"
          rates_t_ha: [1, 2, 5]
        - type: "compost"
          rates_t_ha: [2, 5, 10]
    min_stability_score: 0.6
    min_establishment_prob: 0.4

compute:
  workers: 8
  fba_workers: 4                           # COBRApy is CPU-bound
  batch_size: 1000
  t1_batch_size: 50
  t2_batch_size: 5                         # dynamics runs are slow
  checkpoint_interval: 100

output:
  db_path: "nitrogen_landscape.db"
  receipts_dir: "receipts/"
  results_dir: "results/"
  top_n: 50
  export_community_profiles: true          # BIOM format for top communities
  export_intervention_report: true         # actionable recommendations
```

---

## Pipeline Scripts

```
# Core pipeline
pipeline_core.py                  — 4-tier funnel, config-driven, receipt output
db_utils.py                       — SQLite layer (SoilDB class)
config_schema.py                  — Pydantic config validation
receipt_system.py                 — JSON receipts, FBA cost tracking
correlation_scanner.py            — Automated findings generation

# Data ingestion
adapters/
  ncbi_sra_adapter.py             — SRA Toolkit wrapper, metadata + sequence download
  mgnify_adapter.py               — EBI MGnify REST API, study/sample/analysis retrieval
  emp_adapter.py                  — Earth Microbiome Project BIOM tables
  qiita_adapter.py                — Qiita public study database
  neon_adapter.py                 — NEON Soil Microbiome data portal
  agp_adapter.py                  — American Gut Project
  local_biom_adapter.py           — Local BIOM / FASTA / FASTQ ingestion
  redbiom_adapter.py              — Redbiom (Qiita search layer)

# T0 — community composition + metadata filtering
compute/
  diversity_metrics.py            — Shannon, Simpson, Chao1, Faith PD (scikit-bio)
  metadata_validator.py           — Soil metadata parsing, ENVO term standardization
  functional_gene_scanner.py      — nifH, dsrAB, mcrA, amoA presence detection
  quality_filter.py               — Sequencing depth, chimera detection, contamination
  tax_profiler.py                 — Taxonomic profile from 16S (QIIME2) or shotgun (Bracken)

# T0.25 — ML prediction + fast similarity
compute/
  functional_predictor.py         — RF/GBM functional outcome prediction
  humann3_shortcut.py             — Fast functional profile from marker genes
  community_similarity.py         — Bray-Curtis + UniFrac similarity to reference DB
  picrust2_runner.py              — PICRUSt2 functional prediction from 16S
  tax_function_mapper.py          — Map taxonomy → predicted function via FaProTax

# T1 — metabolic network modeling
compute/
  genome_fetcher.py               — Fetch representative genomes from BV-BRC/NCBI
  genome_annotator.py             — Prokka annotation for novel genomes
  model_builder.py                — CarveMe / ModelSEED genome-scale model construction
  community_fba.py                — COBRApy community FBA, flux variability analysis
  keystone_analyzer.py            — Identify taxa driving target flux
  metabolic_exchange.py           — Cross-feeding interaction network analysis

# T2 — community dynamics + intervention modeling
compute/
  dfba_runner.py                  — Dynamic FBA time-course simulation
  agent_based_sim.py              — iDynoMiCS / custom agent-based model (optional)
  intervention_screener.py        — Screen bioinoculants + amendments vs community
  stability_analyzer.py           — Community resilience + resistance under perturbation
  establishment_predictor.py      — Inoculant establishment probability model
  amendment_effect_model.py       — Biochar/compost effect on pH, moisture, community

# Remote compute
merge_receipts.py                 — Ingest receipts, FBA cost accounting
batch_runner.py                   — Hetzner batch job launcher

# Analysis
rank_candidates.py                — Score communities, rank interventions
taxa_enrichment.py                — Which taxa are enriched in high-performing communities
spatial_analysis.py               — Geographic distribution of top communities
intervention_report.py            — Generate actionable field recommendations
findings_generator.py             — Anomaly detection, FINDINGS.md writer
validate_pipeline.py              — Known community recovery test

# ML surrogate + BNF utilities
scripts/
  train_bnf_surrogate.py          — Train RF surrogate BNF predictor from real FBA results (T0.25 Addition C)
  make_reference_bnf.py           — Generate reference/bnf_measurements.csv from FBA flux data
  track_site_bnf.py               — Time-series BNF trajectory per NEON site (multi-visit)
  make_spatial_map.py             — CONUS BNF kriging heatmap + cluster scatter

# Config instantiations
configs/
  config.example.yaml             — BNF reference config (template for new instantiations)
  soil_carbon.yaml                — SOC sequestration instantiation
  bioremediation.yaml             — Hydrocarbon bioremediation instantiation
  carbon_sequestration.yaml       — Carbon sequestration instantiation

# Curated literature knowledge base
knowledge/                        — 28 curated domain reference files (see knowledge/INDEX.md)

# Trained models
models/
  README.md                       — Feature documentation, training metrics, retrain schedule
  bnf_surrogate_classifier.joblib — RF gate classifier (ROC-AUC 0.812, threshold=0.4)
  bnf_surrogate_regressor.joblib  — RF flux regressor (R² 0.465)
```

---

## Key Design Decisions

**Why community FBA (COBRApy) at T1 rather than just taxonomic correlation?**
Most soil microbiome studies stop at correlating community composition with measured outcomes (high diversity → higher N fixation). Correlation is cheap but mechanistically empty — it cannot predict how a novel community will perform or what intervention will improve function. Genome-scale metabolic modeling via COBRApy provides mechanistic grounding: you're predicting flux through the nitrogen fixation pathway based on which metabolic reactions are present in community members and which cross-feeding interactions are feasible. The predictions are imperfect but far more extrapolatable than correlation. This is the core scientific contribution of the T1 layer.

**Why limit community FBA to 20 organisms?**
Full community metabolic models with 100+ organisms are computationally intractable for systematic screening. The 20-organism limit is a pragmatic compromise — representative organisms from the dominant functional guilds, not every taxon. The keystone_analyzer identifies which taxa actually drive target flux, and most communities have 3-8 genuinely keystone taxa for any given function. Including 20 covers the functional guild structure without computational explosion.

**Why CarveMe for genome-scale model construction?**
Building genome-scale metabolic models manually is weeks of work per organism. CarveMe automates this from genome sequences using a universal template model, producing draft models in minutes. Quality is lower than manually curated models (like iJO1366 for E. coli) but sufficient for community-level flux screening. ModelSEED is an alternative with similar throughput. For keystone taxa identified at T1, manual model curation at T2 is worthwhile — hence the T1/T2 quality escalation.

**Why treat environmental metadata as first-class database columns?**
The same microbial community transplanted from a sandy loam at pH 6.5 to a clay soil at pH 5.0 may completely fail to establish. pH is one of the strongest determinants of soil microbial community structure — more predictive of community composition than geography, land use, or most other factors. A pipeline that treats metadata as optional annotation will produce recommendations that fail in field application. Making pH, texture, climate zone, and organic matter explicit database columns enables systematic filtering at T0 and correct interpretation of T1/T2 predictions.

**Why include intervention screening at T2 rather than just ranking communities?**
The scientific output of ranking communities ("this community has high N fixation potential") is interesting but not actionable — you can't transplant a soil community wholesale. What's actionable is: "adding organism X at concentration Y to soils with these characteristics increases N fixation by Z% because it fills this functional guild gap." T2 intervention screening provides the actionable recommendation layer. Without it, the pipeline produces an interesting academic result; with it, it produces field recommendations.

**Why dFBA over static FBA for T2?**
Static FBA gives a single steady-state flux prediction. dFBA (dynamic FBA) models how community composition and function change over time — a growing season, a drought event, a fertilizer application. For agricultural applications where you need to know whether a recommended intervention is stable across the growing season and resilient to common perturbations, time-course modeling is necessary. dFBA is more expensive but the 10x cost is justified for final-tier screening of hundreds of candidates.

**Why store OTU tables as file paths rather than inline?**
A full OTU table for a soil metagenome with 50,000 taxa × 1 sample is ~2MB as sparse matrix. For 200,000 samples at T0, that's 400GB inline in the database — impractical. The pipeline stores the compressed OTU table as a file and keeps only summary statistics and top-genus profiles in the database. Downstream analysis fetches the full table via file path when needed for T1+ processing.

---

## The Metabolic Modeling Layer (T1) In Detail

T1 is the scientific core of this pipeline and the place most distinct from all other pipeline-core repos. Understanding it is essential for contributors.

### Step 1: Community Composition to Representative Genomes

From the T0.25 community profile (OTU table + taxonomy), select representative organisms for the community FBA model:

1. Filter to taxa with relative abundance > 0.1% (covers ~80-90% of community function)
2. Apply functional guild constraints — ensure coverage of target functional guild (N fixers, C cyclers, etc.)
3. For each representative taxon, fetch the best available reference genome from BV-BRC or NCBI RefSeq
4. For taxa with no reference genome (common in soil — 40-60% of soil taxa are uncharacterized), use a phylogenetic neighbor genome as proxy

### Step 2: Genome-Scale Model Construction

For each representative genome:
1. Run CarveMe with the universal template model to generate a draft genome-scale metabolic model
2. Gap-fill the draft model against the universal template for reactions with strong genomic evidence
3. Validate the model can produce biomass under standard conditions (basic sanity check)

### Step 3: Community FBA

Combine individual models into a community model using COBRApy's community modeling framework:
1. Define shared metabolite pools (carbon sources, nitrogen sources, mineral nutrients)
2. Set environmental constraints from sample metadata (pH-adjusted reaction bounds, temperature-adjusted kinetics)
3. Run FBA to maximize community biomass while tracking flux through target pathways
4. Run flux variability analysis (FVA) to identify which fluxes are essential vs. optional

### Step 4: Keystone Analysis

1. Knockout each community member sequentially — measure change in target pathway flux
2. Taxa whose removal causes >20% reduction in target flux are keystone taxa
3. Report keystone taxa with their specific functional contribution
4. Identify metabolic exchange interactions between keystone taxa (cross-feeding networks)

The keystone analysis output feeds directly into T2 intervention design — bioinoculant candidates are organisms that fill keystone functional roles in low-performing communities.

---

## The Intervention Screening Layer (T2) In Detail

T2 answers a different question than T1. T1 asks: "does this community have the metabolic capacity for the target function?" T2 asks: "what intervention would most reliably improve function in a community that's currently underperforming?"

### Bioinoculant Screening

For each candidate bioinoculant organism:
1. Add the organism to the community model at realistic inoculation density
2. Run dFBA to simulate community dynamics over the growing season
3. Track establishment probability (does the inoculant persist or get outcompeted?)
4. Track effect on target function flux over time
5. Track off-target effects (does the inoculant disrupt existing community function?)

The establishment probability model uses competitive exclusion theory — an inoculant establishes if it fills a functional niche not already occupied at saturation. In communities with a functional guild gap (no nifH-containing organisms), establishment is near-certain. In communities with high N fixer abundance, establishment is near-zero.

### Amendment Screening

For biochar, compost, and other amendments:
1. Translate amendment properties to soil parameter changes (biochar raises pH 0.5-1.5 units, increases moisture retention, reduces bulk density)
2. Re-run T1 FBA with adjusted environmental constraints
3. Compare target flux before and after amendment
4. Identify which community members benefit from the amendment (pH-sensitive taxa)

### Management Practice Screening

For reduced tillage, cover crops, irrigation scheduling:
1. Translate management practice to temporal environmental parameter changes
2. Run dFBA with time-varying constraints (seasonal moisture and temperature profiles)
3. Identify management practices that favor keystone taxa for target function

---

## Application-Specific Notes

### Biological Nitrogen Fixation (BNF)
**Target:** Reduce synthetic N fertilizer use in dryland and smallholder farming systems. Synthetic nitrogen production (Haber-Bosch) accounts for 1-2% of global energy consumption and 1% of CO₂ emissions. BNF replaces atmospheric N₂ with plant-available NH₄⁺ via the nitrogenase enzyme encoded by nifH.

**Key T0 filter:** nifH gene presence (detection in functional gene profile). Samples without nifH have zero BNF capacity by definition.

**Key T1 output:** predicted BNF flux in mmol N / g soil / day, keystone diazotroph taxa, metabolic dependencies (what carbon sources fuel N fixation?).

**Key T2 intervention:** associative N fixers (Azospirillum, Herbaspirillum, Gluconacetobacter) establish in the rhizosphere and provide N to plant roots. Screening for the right organism for a specific soil × crop combination is the core T2 output.

**Validation reference:** Drinkwater & Snapp (2007) — "Nutrients in agroecosystems: rethinking the management paradigm" — empirical BNF rates for validation of T1 predictions.

### Soil Carbon Sequestration
**Target:** Identify communities with high potential for stable soil organic carbon (SOC) accumulation. Soil holds ~2,500 Gt carbon — more than the atmosphere and all vegetation combined. Even a 0.4% annual increase in global SOC would offset all anthropogenic CO₂ emissions (the 4 per 1000 initiative).

**Key distinction:** not all carbon cycling is equal. Mineralization (decomposing organic matter to CO₂) is the opposite of sequestration. The target is communities that promote stable aggregate formation and microbial necromass incorporation rather than rapid turnover.

**Key T0 filters:** functional genes for lignin degradation (laccase, peroxidase) and fungal presence (ITS amplicon or fungal marker genes) — fungi are disproportionately important for carbon stabilization via aggregate formation.

**Key T1 output:** predicted net carbon flux direction (sequestration vs. mineralization), fungal:bacterial ratio as proxy, predicted aggregate formation potential.

**Key T2 simulation:** soil aggregate stability under wetting/drying cycles. Stable aggregates physically protect SOC from decomposition — the T2 model tracks whether community interventions improve aggregate formation.

### Bioremediation
**Target:** Identify communities capable of degrading petroleum hydrocarbons, heavy metals, or persistent organic pollutants (POPs) in contaminated soils.

**Key difference from other applications:** the "soil context" filter is inverted — you want contaminated soils, not clean agricultural soils. The metadata filter at T0 selects for sites with documented contamination history.

**Key T0 filters:** functional genes for specific degradation pathways (alkB for alkane degradation, phn genes for PAH degradation, mer genes for mercury resistance).

**Key T1 output:** predicted degradation flux for the target contaminant, predicted degradation products (are they more or less toxic than the parent compound?).

**Key T2 consideration:** biostimulation (adding nutrients to enhance native degraders) vs. bioaugmentation (adding organisms from elsewhere). The intervention screener tests both strategies and models which is more likely to succeed given existing community composition.

**Validation database:** NCBI BioProject for contaminated site metagenomes, KEGG pathway database for degradation pathway completeness.

### Plant Growth Promotion (PGP)
**Target:** Identify rhizosphere communities with high plant growth promotion potential — phosphate solubilization, phytohormone production, iron siderophore production, induced systemic resistance.

**Key complication:** rhizosphere communities are strongly shaped by plant root exudates — the community that promotes growth of wheat may be entirely different from the community that promotes tomato. The config must specify crop context, and T1 must model the root exudate composition for that crop as a carbon source input.

**Key T1 addition:** root exudate metabolite profile as environmental constraint. Different crops exude different proportions of sugars, organic acids, and amino acids — these selectively enrich different microbial functional guilds.

### Pathogen Suppression
**Target:** Identify communities with natural disease suppressiveness — ability to prevent establishment of soil-borne plant pathogens (Fusarium, Rhizoctonia, Pythium, Phytophthora).

**Key T0 filter:** presence of known biocontrol taxa (Trichoderma, Bacillus, Pseudomonas fluorescens group, Streptomyces) at threshold abundance.

**Key T2 addition:** pathogen competition model — add pathogen to community model, check whether community metabolically excludes it via resource competition or antibiotic production. Predict antibiotic biosynthetic gene cluster expression using antiSMASH output for community members.

---

## Validation Strategy

**Known community recovery test** — the mandatory first step. Take 20-50 soil samples with published BNF measurements (or other target function measurements), process through T0-T0.25, check that high-BNF samples are enriched in T0.25 survivors. Target: Spearman correlation > 0.6 between measured function and ML-predicted function score.

**FBA flux calibration** — compare T1 COBRApy predicted fluxes against published in situ measurements for well-characterized communities (e.g. predicted vs. measured acetylene reduction assay values for BNF). If predicted flux is off by more than 2 orders of magnitude from measured, the model construction or constraints are wrong.

**Cross-study validation** — run the pipeline on samples from a study with published experimental outcomes (fertilizer reduction trial with microbiome data). Check that communities ranked highly by the pipeline correspond to plots with better outcomes. If not, the T1/T2 model needs recalibration.

**Negative control** — run non-target samples (aquatic metagenomes, gut metagenomes) through T0. They should fail T0 at >99% rate. If soil-specific filters aren't working, metadata handling is broken.

---

## Findings & Correlation Scanner

`correlation_scanner.py` surfaces patterns automatically across the accumulated database:

**Taxonomic enrichment** — which genera, families, or phyla are significantly enriched in high-performing communities vs. low-performing communities for the target function. The most important finding type — often identifies undercharacterized taxa worth investigating as bioinoculant candidates.

**Metadata correlations** — which soil properties (pH, organic matter, clay content) most strongly predict T0.25 functional scores. Expected results: pH is usually strongest predictor. Surprises (unexpected metadata correlations) are flags for new hypotheses.

**Geographic clustering** — do high-performing communities cluster geographically? If 70% of top nitrogen-fixing communities come from a specific climate zone or soil type, that constrains where interventions will be most effective.

**Keystone taxa network** — which taxa consistently appear as keystone in T1 metabolic models? Cross-study consistency of keystone identification strengthens the case for bioinoculant development from those taxa.

**Intervention success rate by soil type** — does biochar amendment work better in sandy vs clay soils? Does Azospirillum establish better at pH > 6? These interaction effects are invisible in single-study analyses but emerge from the accumulated multi-study database.

**Loser analysis** — samples with good T0 metadata that consistently fail T1 metabolic feasibility checks. Often reveals structural issues with the community model construction for specific phylogenetic groups — documents tool limitations worth fixing.

---

## Known Pitfalls & Gotchas

| # | Issue | Impact |
|---|-------|--------|
| — | SRA metadata is notoriously inconsistent — soil pH reported in 10 different formats (pH, ph, acidity, reaction) — requires aggressive normalization | False metadata filter failures |
| — | 16S V4 region vs V3-V4 region give different diversity estimates — never compare directly | Biased diversity metrics across studies |
| — | PICRUSt2 functional prediction accuracy degrades for taxa with no close reference genome — common in soil | T0.25 false negatives for novel lineages |
| — | CarveMe model quality varies enormously by genome completeness — incomplete MAGs give unreliable FBA | Silent T1 errors |
| — | COBRApy community FBA is sensitive to biomass objective function choice — different choices give different flux predictions | Non-reproducible T1 results across runs |
| — | nifH gene family is paraphyletic — not all nifH-containing organisms fix N₂ under all conditions | False T0 positives for N fixation |
| — | Biochar effect on soil pH is highly heterogeneous — feedstock and pyrolysis temperature matter — use conservative estimates | T2 amendment predictions overconfident |
| — | Relative abundance data from 16S is compositional — log-ratio transforms required before ML input | ML model biased by sequencing depth |
| — | NCBI SRA download throttling — use Aspera/aws sync for large batches, not direct HTTP | Pipeline bottlenecked on data ingestion |
| — | EBI MGnify API rate limits at 100 requests/minute — implement request queuing | Silent failures during bulk metadata retrieval |

*Document bugs before workarounds. This table is expected to grow.*

---

## Compute Infrastructure Notes

**T0 — metadata filtering + diversity metrics:** CPU-only, fast. QIIME2 diversity metrics on 1000-sample batches complete in minutes on a standard Hetzner CCX23. The SRA download is often the actual bottleneck — use Aspera protocol and bulk downloads rather than per-sample HTTP.

**T0.25 — ML prediction + PICRUSt2:** CPU-bound, moderately fast. PICRUSt2 takes ~30 seconds per sample on 4 cores. For 20,000 T0 survivors, budget ~3 GPU-hours or ~170 CPU-hours. Parallelize aggressively.

**T1 — COBRApy community FBA:** CPU-bound, the primary compute bottleneck. A community FBA model with 20 organisms and flux variability analysis takes 5-30 minutes depending on model complexity. For 2,000 T1 candidates: 200-1,000 CPU-hours. Use Hetzner CCX53 (32 cores) nodes with 4 parallel FBA workers.

**T2 — dFBA dynamics:** Most expensive tier. A 90-day dFBA simulation with perturbation tests takes 2-8 hours per community. For 200 T2 candidates: 400-1,600 CPU-hours. Budget a full Hetzner CCX63 (48 cores) for a week for a complete T2 run.

**Data storage:** SRA metagenome files are large (1-10GB per sample as FASTQ). For bulk pipeline runs, don't store raw reads — process to OTU tables immediately and discard raw files. The SQLite database + OTU table files for 200,000 processed samples is ~500GB total, manageable on a dedicated Hetzner storage volume.

---

## Quick Start

```bash
git clone https://github.com/sethc5/soil-microbiome-pipeline-core.git
cd soil-microbiome-pipeline-core
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Install non-Python dependencies
conda install -c bioconda qiime2 picrust2 mmseqs2 prokka

# Install CarveMe for metabolic model construction
pip install carveme
diamond makedb --in /path/to/uniprot_sprot.fasta -d diamond_db

# Validate config
python config_schema.py --validate path/to/config.yaml

# Run known community validation (mandatory first step)
python validate_pipeline.py \
  --config config.yaml \
  --reference_communities reference/high_bnf_communities.biom \
  --measured_function reference/bnf_measurements.csv \
  --model-path models/bnf_surrogate_classifier.joblib

# Run T0 + T0.25 (fast, no metabolic modeling)
python pipeline_core.py --config config.yaml --tier 025 -w 8

# Run full pipeline
python pipeline_core.py --config config.yaml -w 4 --fba-workers 4

# Merge receipts from remote runs
python merge_receipts.py --list
python merge_receipts.py

# Generate findings and intervention report
python findings_generator.py --config config.yaml
python intervention_report.py --config config.yaml --top 20
```

---

## Tool Stack

| Layer | Tool | Why |
|-------|------|-----|
| 16S processing | `qiime2` | Standard amplicon pipeline |
| Shotgun taxonomy | `bracken` + `kraken2` | Fast, accurate read classification |
| Shotgun function | `humann3` | Functional profile from shotgun metagenomes |
| Fast function pred | `picrust2` | Functional prediction from 16S |
| Diversity metrics | `scikit-bio` | Shannon, Faith PD, UniFrac |
| Sequence alignment | `mmseqs2` | Fast homology for functional gene detection |
| ML prediction | `scikit-learn` | RF/GBM for T0.25 functional prediction |
| Genome annotation | `prokka` | Fast bacterial genome annotation |
| Genome completeness | `checkm-genome` | Completeness/contamination for model confidence |
| Metabolic models | `carveme` | Automated genome-scale model construction |
| Community FBA | `cobrapy` | Community flux balance analysis |
| dFBA dynamics | `cobra` + `scipy` | Dynamic FBA time-course simulation |
| Phylogenetics | `ete3` | Phylogenetic tree construction and analysis |
| Motif/biosynthesis | `antismash` | Biosynthetic gene cluster detection |
| Data format | `biom-format` | Standard microbiome table format |
| Sequence I/O | `biopython` | FASTA/FASTQ handling |
| Metagenome assembly | `megahit` | De novo assembly for novel MAGs (optional) |
| Binning | `metabat2` | MAG binning from assembled contigs (optional) |
| Geospatial | `geopandas` | Geographic distribution analysis |
| Config validation | `pydantic` | Schema validation |
| CLI | `typer` | Clean CLI |
| Progress/logging | `rich` | Readable output |
| Database | `sqlite3` (stdlib) | Zero infrastructure |
| Parallel processing | `concurrent.futures` | T0 batch parallelism |

---

## Instantiation Roadmap

| Repo | Target Function | Application | Status |
|------|----------------|-------------|--------|
| [nitrogen-fixation-pipeline](https://github.com/sethc5/nitrogen-fixation-pipeline) | BNF flux, nifH activity | Fertilizer reduction | ✅ T1+T2 complete · 4,491 communities · surrogate trained |
| carbon-sequestration-pipeline | SOC accumulation rate | Climate mitigation | 🔶 Config instantiated (`configs/soil_carbon.yaml`) |
| bioremediation-pipeline | Hydrocarbon degradation flux | Contaminated site cleanup | 📋 Planned |
| plant-growth-promotion-pipeline | Rhizosphere PGP activity | Crop yield improvement | 📋 Planned |
| pathogen-suppression-pipeline | Disease suppressiveness score | Pesticide reduction | 📋 Planned |
| wetland-methane-pipeline | CH₄ emission flux | GHG mitigation | 📋 Planned |

---

## Relationship to Other Pipelines

```
Athanor (literature mining + hypothesis generation)
    │
    ├── genomics-pipeline-core        ← sequence space: novel organisms from metagenomes
    │     └── antimicrobial-peptide   ← soil metagenomes are a primary AMP source
    │
    ├── biochem-pipeline-core         ← compound space: soil-derived natural products
    │     └── [natural product pipelines]
    │
    ├── materials-pipeline-core       ← composition space
    │
    └── soil-microbiome-pipeline-core ← community space: ecosystem function
          ├── nitrogen-fixation
          ├── carbon-sequestration
          └── bioremediation ─────────┐
                                      │ composes: bioremediation identifies
                                      │ degrading organisms → genomics pipeline
                                      │ sequences them → biochem pipeline
                                      │ screens degradation products
```

Soil is the domain most richly connected to the other pipelines:
- Genomics pipeline uses soil metagenomes as a primary sequence source (AMPs, novel enzymes)
- Biochem pipeline screens compounds from soil-derived natural products
- The bioremediation application composes all three: identify degrading communities (soil pipeline) → sequence keystone taxa (genomics pipeline) → screen degradation products for toxicity (biochem pipeline)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Highest value contributions:

1. **New application instantiations** — nitrogen fixation is first; carbon sequestration and bioremediation have the clearest pipeline structure next
2. **Metabolic model quality** — improving CarveMe model construction for soil-specific phylogenetic groups (Acidobacteria, Verrucomicrobia are notoriously hard to model)
3. **Database adapters** — NEON soil data portal, JGI IMG/M for metagenomes, EMP for amplicon data
4. **T2 dynamics engines** — iDynoMiCS integration for spatially explicit community modeling
5. **Bug documentation** — SRA metadata normalization issues especially
6. **Validation datasets** — curated datasets with both metagenome data and measured functional outcomes (BNF assays, SOC measurements, disease suppression trials)

---

## References

- [Earth Microbiome Project](https://earthmicrobiome.org/) — 97-country soil microbiome atlas
- [EBI MGnify](https://www.ebi.ac.uk/metagenomics/) — 500,000+ processed metagenomes
- [NCBI SRA](https://www.ncbi.nlm.nih.gov/sra) — sequence archive, millions of metagenomes
- [BV-BRC](https://www.bv-brc.org/) — bacterial genome database for model construction (formerly PATRIC)
- [COBRApy](https://github.com/opencobra/cobrapy) — constraint-based metabolic modeling
- [CarveMe](https://github.com/cdanielmachado/carveme) — automated genome-scale model reconstruction
- [QIIME2](https://qiime2.org/) — amplicon microbiome analysis
- [HUMAnN3](https://github.com/biobakery/humann) — functional profiling of metagenomes
- [PICRUSt2](https://github.com/picrust/picrust2) — functional prediction from marker genes
- [antiSMASH](https://antismash.secondarymetabolites.org/) — biosynthetic gene cluster detection
- Thompson et al. (2017) — "A communal catalogue reveals Earth's multiscale microbial diversity" — EMP paper
- Drinkwater & Snapp (2007) — "Nutrients in agroecosystems" — empirical BNF benchmarks
- Fierer (2017) — "Embracing the unknown: disentangling the complexities of the soil microbiome"
- [biochem-pipeline-core](https://github.com/sethc5/biochem-pipeline-core) — parallel drug discovery pipeline
- [materials-pipeline-core](https://github.com/sethc5/materials-pipeline-core) — parallel materials pipeline
- [genomics-pipeline-core](https://github.com/sethc5/genomics-pipeline-core) — parallel genomics pipeline
- [cytools_project](https://github.com/sethc5/cytools_project) — the pipeline architecture origin

## License

This repository is licensed under the PolyForm Noncommercial License 1.0.0.
Commercial use requires a separate agreement with the author. See the LICENSE file in this repository
for details.
