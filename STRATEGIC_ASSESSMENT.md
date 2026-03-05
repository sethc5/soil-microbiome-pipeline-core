# Strategic Assessment: soil-microbiome-pipeline-core

_Generated March 5, 2026 — based on full README review against current compute context (i9-9900K, 128GB RAM, 2×500GB SSD)_

---

## What the Scope Gets Right

The architecture is genuinely well-designed. The 4-tier funnel logic is correct, environmental metadata as first-class DB columns is the right call (pH is the single strongest predictor of soil community structure — most pipelines bury it in a JSON blob), and the CarveMe → COBRApy → keystone analysis chain is a reasonable mechanistic backbone for a solo/small team. The receipts + SQLite approach is appropriate for the computational budget. The decision to treat environmental metadata as load-bearing rather than annotation is unusual in open-source microbiome tools and is the single biggest differentiator from existing pipelines.

---

## Gaps in Scope

### 1. Fungi are second-class citizens — and that's a real scientific problem

The README mentions ITS amplicon and fungal presence as relevant to carbon sequestration but there is no ITS-specific adapter, no `has_its` flag in the communities schema, no dedicated fungal metabolic modeling path, and no fungal reference database (MaarjAM for AMF, UNITE for ITS taxonomy). Arbuscular mycorrhizal fungi (AMF) are the primary mechanism for phosphorus acquisition and aggregate formation in most agricultural soils. For carbon sequestration especially, ignoring fungi makes T1/T2 predictions fundamentally incomplete. Needs its own T0 filter track (`sequencing_type = 'ITS'`) and at minimum a `fungal_bacterial_ratio` column in `communities`.

### 2. Archaea are almost entirely absent

Ammonia-oxidizing archaea (AOA, Thaumarchaeota) carrying archaeal *amoA* are more abundant than their bacterial counterparts in many pH-neutral and slightly acidic soils — precisely the dryland wheat context named in the config example. The `has_amoa` flag in the schema doesn't distinguish bacterial vs. archaeal amoA, which matters enormously for nitrification modeling. Methanogens (Euryarchaeota) are relevant for wetland methane and rice paddy applications listed in the instantiation roadmap. The schema and functional gene scanner need archaeal markers as explicitly named fields.

### 3. No metatranscriptomic path

The `sequencing_type` column lists `metatranscriptome` but no compute module handles it and nothing in T0.25 or T1 accounts for active expression vs. gene presence. The gap between what genes *exist* (DNA) and what's actually being *expressed* (mRNA) is one of the largest sources of false positives in functional prediction — a community can carry nifH and never express it if oxygen or carbon conditions don't favor it. MG-RAST and MGnify both have metatranscriptomic datasets. Even a simple `mrna_to_dna_ratio` column for shared samples would anchor predictions to actual activity.

### 4. Rhizosphere vs. bulk soil conflation

The config has `crop: wheat` but the pipeline treats rhizosphere and bulk soil communities as interchangeable. They are not — rhizosphere communities are shaped almost entirely by root exudate composition (plant genotype effect can explain 20-40% of rhizosphere community variance). The PGP application listed in the roadmap is essentially impossible to do correctly without distinguishing these. `sampling_fraction` (rhizosphere / endosphere / bulk) should be a required metadata field, not buried in free-text `management` JSON.

### 5. MAG quality is listed as optional but is load-bearing for T1

40-60% of soil taxa lack reference genomes. MetaBAT2 and MEGAHIT are listed as "(optional)" — but if they're optional, T1 community FBA for novel lineages silently falls back to phylogenetic neighbor genomes with unknown fitness. CheckM (genome completeness scoring) is not listed in the tool stack at all, which means CarveMe is building models from potentially 30%-complete genomes without the pipeline ever catching it. This produces silent T1 errors, which the pitfalls table flags elsewhere but doesn't connect to a systematic fix.

### 6. PATRIC is now BV-BRC

PATRIC (Pathosystems Resource Integration Center) was absorbed into USDA's BV-BRC (Bacterial and Viral Bioinformatics Resource Center) in 2022. The tool stack, schema comments, and T1 modeling section all reference PATRIC. The API endpoints are different. Any code using the PATRIC API silently fails or hits deprecated endpoints.

### 7. Horizontal gene transfer for functional genes is unhandled

nifH can be transferred between taxa via HGT and is found in phylogenetically diverse organisms, including ones where it is not constitutively expressed. The README notes "nifH gene family is paraphyletic — not all nifH-containing organisms fix N₂ under all conditions" but doesn't follow this through to what T0 does about it. A community could pass the `has_nifH` filter entirely on HGT-acquired, non-functional gene copies. This is a known false-positive mechanism for the primary T0 filter in the BNF application.

### 8. Time-series / longitudinal data is architecturally invisible

NEON has multi-year time-series metagenomes from fixed plots. The database schema has no concept of repeated samples from the same site — there is no `site_id` field linking multiple `sample_id` rows from the same location at different time points. The `run` and `community` tables would silently re-process these as independent samples rather than time-series, losing the most scientifically valuable structure in some of the best-curated datasets available.

### 9. No uncertainty propagation through the funnel

The `runs` table tracks `t025_uncertainty` but this doesn't flow into T1 or T2. The confidence interval on a CarveMe-built model for a 70%-complete MAG is completely different from the confidence interval on a manually curated model — but the pipeline emits a single `t1_target_flux REAL` with no error bounds. Intervention recommendations derived from uncertain FBA predictions should carry that uncertainty forward explicitly; currently the T2 output looks equally confident regardless of model quality.

### 10. Confidence of T2 claims slightly outpaces current science

dFBA for a 20-organism community over 90 simulated days is technically feasible but produces predictions whose accuracy for real soil systems has not been systematically validated against field outcomes. The config example presents specific thresholds (`min_stability_score: 0.6`, `min_establishment_prob: 0.4`) with implied precision that doesn't yet have empirical grounding. This should be communicated prominently in the findings generator output — not just in the pitfalls table — to prevent over-confident interpretation of early results.

---

## Easy Wins

These are high-payoff, achievable quickly without institutional support or new science.

### 1. SRA metadata normalization library

The README explicitly names this as the #1 known pain point — "soil pH reported in 10 different formats." A focused, well-tested normalization module with a regex/synonym mapping table for all common inconsistencies (`pH`, `ph`, `acidity`, `reaction_class`, pH as string "6.5-7.0") immediately unblocks T0 at scale. This is pure ETL work, no new science, probably a 2-3 day task with enormous downstream value. It is also the kind of contribution other soil microbiome researchers would immediately use outside this project.

### 2. FaProTax is fast — promote it to T0

`tax_function_mapper.py` using FaProTax is currently listed under T0.25 but it takes seconds per sample and has no ML complexity. It maps taxonomy → predicted functional role from a curated database of ~80 functional groups. Running it at T0 before ML gives a cheap second functional gene check that catches communities with no FaProTax-identifiable nitrogen cyclers even if PCR-detected nifH is present. Zero additional dependencies.

### 3. Implement the NEON adapter first

NEON provides: structured well-documented metadata, longitudinal sampling (multiple years), paired sequencing + soil chemistry measurements, and known geographic coordinates with climate data. It is the highest-quality labeled dataset for validation that is publicly available. Everything in the NEON data model maps cleanly to the existing DB schema. Getting the NEON adapter working and running `validate_pipeline.py` on NEON samples provides real validation numbers instead of placeholders, achievable within a week.

### 4. CheckM integration — not optional

One function call wrapping CheckM on every MAG before passing to CarveMe makes T1 model quality auditable. Flag genomes below 70% completeness and above 10% contamination as "low-confidence" — store that flag in the T1 results row. The `t1_pass` boolean becomes meaningfully calibrated rather than uniformly optimistic.

### 5. BV-BRC migration for the genome database layer

The PATRIC → BV-BRC API migration is already necessary for correctness. BV-BRC has a REST API with good documentation. This is a targeted endpoint update in `genome_fetcher.py` — probably a half-day task that immediately fixes a broken dependency.

### 6. Add `site_id` and `visit_number` columns to the samples table

Two columns added to the schema enables time-series analysis without redesigning anything else. Link to NEON's official site naming convention. Then `spatial_analysis.py` can answer "did top-performing communities' functional scores change across seasons?" — a compelling analysis that comes nearly free once the schema supports it.

### 7. Write the carbon sequestration `config.yaml`

The pipeline architecture is done. A second instantiated config is a forcing function that surfaces any config schema gaps (fungal ITS handling, SOC-specific functional genes: laccase, peroxidase, fungal markers), and produces the second concrete pipeline instance. It makes the "instantiation model" section of the README concrete rather than aspirational. A day of work.

### 8. Generate the first spatial map from real data

`geopandas` is in the tool stack, `spatial_analysis.py` is in the file list, geographic coordinates are in the schema. A world map of top N communities colored by T0.25 score — generated from NEON or EMP data — is a genuinely compelling visual for the repository. It requires no new science, just connecting existing modules.

### 9. Tune batch sizes for the actual hardware

The config example has `batch_size: 1000`. On 128GB RAM you can push T0 to 5,000–10,000 samples in memory without paging, reducing checkpoint round-trips significantly. The FBA batch size can stay conservative (50), but T2 should be throttled to 2 parallel jobs (dFBA state is 8–16GB per run) rather than 4.

### 10. Reduce default T2 simulation window for initial runs

`simulation_time_days: 90` with multiple perturbation types is the right target but wrong starting point. Cut to 45 days and 1–2 perturbation conditions for first runs to get faster iteration cycles during model quality calibration. Scientific value loss is minimal; cycle time improvement is 2–4×.

---

## Needs Large Institution / External Support

These are items where the blocker is not engineering skill or compute — it is infrastructure, institutional authority, or experimental capacity that a solo developer cannot provide.

### 1. Experimental validation of T1 FBA predictions

The pipeline predicts BNF flux in mmol N / g soil / day. To know whether that number is meaningful, you need to compare it against acetylene reduction assay (ARA) measurements on real communities. ARA requires a wet lab. The validation strategy is described correctly in the README, but the labeled dataset for "T1 predicted flux vs. measured flux" calibration does not exist in public databases at the required resolution. Building it requires a coordinated field-sampling and lab-measurement campaign — minimum a graduate student in a university soil science or microbiology lab, or a USDA ARS collaboration.

### 2. T2 compute at full scale

The README estimates 400–1,600 CPU-hours for T2 alone. On the i9-9900K, at 2 parallel dFBA jobs, 200 communities at 2–8 hours each ranges from 8 to 33 days of continuous compute. This is manageable with selective T2 targeting (top 50–100 communities only), but any full re-processing as models improve — which will happen repeatedly during development — compounds quickly. A DOE/NSF ACCESS allocation or XSEDE successor account provides 10–100× the core count needed to make this fast. Without it, T2 remains a queue-management problem rather than a blocker, but iteration speed is the sacrifice.

### 3. Human-curated metabolic models for Acidobacteria and Verrucomicrobia

The README explicitly names these as "notoriously hard to model." These two phyla are often the *dominant* taxa in soil metagenomes by relative abundance — Acidobacteria can be 20–50% of soil 16S reads. CarveMe quality for them is poor because reference genomes are sparse and their metabolisms are unusual compared to better-characterized organisms. Manual curation of even a handful of representative models for each phylum would dramatically improve T1 accuracy for the majority of soil samples — but it is months of specialized biochemistry and metabolic modeling labor. This is the kind of contribution that comes from DOE JGI partnerships or groups like the KBase team (KBase has draft models for some Acidobacteria representatives that could be imported).

### 4. T2 field validation trials

Taking the pipeline's ranked intervention output ("add *Azospirillum brasilense* at 10⁷ CFU/g to dryland wheat in pH 6.5–7.0 loam") and running a randomized controlled field trial requires multiple farm plot replicates, 1–3 growing seasons, agronomist collaboration, and ideally agricultural extension service partnership. USDA ARS, CGIAR, or a university agronomy department is the minimum institutional partner. Without field validation, T2 output remains a hypothesis rather than a recommendation regardless of pipeline quality.

### 5. Improving SRA/MGnify metadata compliance at source

The normalization library (easy win #1) fixes the symptom on ingest. Fixing the upstream problem — enforcing MIxS (Minimum Information about any Sequence) schema compliance for new submissions — requires working with NCBI through the Genomic Standards Consortium. This is a standards-body process that requires institutional membership and influence. Individual researchers can submit comments, but sustained change requires an institutional voice.

### 6. The commercial bioinoculant pathway

If the pipeline output is compelling enough to act on, translating "add organism X to fields of type Y" into an actual product requires EPA FIFRA registration (for bioinoculants used on food crops), GMP production scale-up, stability testing, and distribution infrastructure. This is well outside the computational scope of this repository. Noting it explicitly ensures collaborators with that expertise understand it is a downstream step and can plan accordingly.

---

## Hardware-Specific Notes (i9-9900K / 128GB / 2×500GB SSD)

| Tier | Wall-time estimate | Notes |
|------|--------------------|-------|
| T0 (200k samples) | ~4 hours | Not a bottleneck. Push batch_size to 8,000. |
| T0.25 (20k samples, PICRUSt2) | ~40 hours | Primary scale bottleneck. Run 8 parallel PICRUSt2 instances at 2 threads each. |
| T1 (2k communities, COBRApy) | ~5 days | 6–8 parallel FBA workers. i9 5GHz clock is an advantage. |
| T2 (top 100 communities, dFBA 45d) | ~8–16 days | Max 2 parallel jobs. Budget as a dedicated week-long batch. |
| Storage | ~600GB at peak | 500GB SSD 1: DB + OTU tables. 500GB SSD 2: Rolling FASTQ staging + T2 outputs. Delete raw reads after processing. |

The machine handles everything through T1 production runs without architectural compromise. T2 is the one tier where scheduling discipline (targeted top-N selection, phased runs) matters more than hardware.

---

## Priority Order for Current Development Phase

1. **BV-BRC migration** — fixes a broken dependency, unblocks genome fetching (half day)
2. **SRA metadata normalization** — unblocks T0 at scale with real data (2–3 days)
3. **CheckM integration** — makes T1 quality auditable (1 day)
4. **NEON adapter** — provides real labeled validation data (1 week)
5. **`site_id` / `visit_number` schema columns** — enables NEON time-series analysis (half day)
6. **FaProTax at T0** — cheap functional double-check (1 day)
7. **Carbon sequestration config.yaml** — second instantiation, surfaces schema gaps (1 day)
8. **First spatial map** — compelling repository artifact (1 day, after NEON adapter)
9. **Fungi / ITS track** — scientifically necessary for carbon sequestration application (1 week)
10. **Longitudinal uncertainty propagation** — important for scientific credibility, can be deferred until T1 is running on real data
