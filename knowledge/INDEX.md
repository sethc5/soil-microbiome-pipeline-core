# References Index

_Built 2026-03-05 via Semantic Scholar API_

| File | Topic | Papers |
|------|-------|--------|
| [nitrogen_fixation.md](nitrogen_fixation.md) | Biological Nitrogen Fixation in Soil | 12 |
| [carbon_sequestration.md](carbon_sequestration.md) | Soil Carbon Sequestration & Microbial Drivers | 12 |
| [metagenomics_methods.md](metagenomics_methods.md) | Soil Metagenomics Methods (16S, Shotgun, QIIME2, HUMAnN3) | 12 |
| [metabolic_modeling_fba.md](metabolic_modeling_fba.md) | Community Flux Balance Analysis & Metabolic Modeling | 10 |
| [functional_prediction.md](functional_prediction.md) | Functional Prediction from Metagenomes (PICRUSt2, HUMAnN3) | 8 |
| [keystone_taxa.md](keystone_taxa.md) | Keystone Taxa & Microbial Interaction Networks | 10 |
| [community_dynamics_sim.md](community_dynamics_sim.md) | Microbial Community Dynamics & Stability | 10 |
| [plant_growth_promoting.md](plant_growth_promoting.md) | Plant Growth Promoting Rhizobacteria | 10 |
| [soil_health_indicators.md](soil_health_indicators.md) | Soil Health Biological Indicators | 10 |
| [pathogen_suppression.md](pathogen_suppression.md) | Soil Suppressiveness & Pathogen Suppression | 8 |
| [bioinoculants_amendments.md](bioinoculants_amendments.md) | Bioinoculants, Biochar & Organic Amendments | 8 |
| [land_management.md](land_management.md) | Land Management & Soil Microbiome | 10 |
| [dfba_agent_simulation.md](dfba_agent_simulation.md) | Dynamic FBA & Agent-Based Microbial Simulation | 8 |
| [diversity_metrics.md](diversity_metrics.md) | Microbial Diversity Metrics & Ecology | 8 |
| [earth_microbiome.md](earth_microbiome.md) | Earth Microbiome Project & Global Surveys | 8 |

---

## Usage in Pipeline

These files provide curated literature backing for each major module:

| Module | Reference file(s) |
|--------|-------------------|
| `compute/tax_profiler.py` | `metagenomics_methods.md` |
| `compute/community_fba.py` | `metabolic_modeling_fba.md`, `dfba_agent_simulation.md` |
| `compute/functional_predictor.py` | `functional_prediction.md` |
| `compute/keystone_analyzer.py` | `keystone_taxa.md` |
| `compute/stability_analyzer.py` | `community_dynamics_sim.md` |
| `compute/establishment_predictor.py` | `bioinoculants_amendments.md` |
| `compute/diversity_metrics.py` | `diversity_metrics.md` |
| `compute/intervention_screener.py` | `land_management.md`, `bioinoculants_amendments.md` |
| `compute/amendment_effect_model.py` | `bioinoculants_amendments.md` |
| `taxa_enrichment.py` | `earth_microbiome.md`, `nitrogen_fixation.md` |
| `validate_pipeline.py` | `soil_health_indicators.md` |
| Application: N-fixation | `nitrogen_fixation.md` |
| Application: Carbon seq | `carbon_sequestration.md` |
| Application: Pathogen suppression | `pathogen_suppression.md` |
| Application: PGPR / inoculants | `plant_growth_promoting.md` |

---

## Gap-Fill References (Strategic Assessment)

_Added 2026-03-05 — topics identified as gaps in STRATEGIC_ASSESSMENT.md and REBUILD_PLAN.md_

| File | Topic | Papers | Addresses |
|------|-------|--------|-----------|
| [fungal_ecology_its.md](fungal_ecology_its.md) | Soil Fungal Ecology, ITS Methods & Mycorrhizal Networks | 10 | Gap 1 |
| [amf_phosphorus_aggregates.md](amf_phosphorus_aggregates.md) | Arbuscular Mycorrhizal Fungi: Phosphorus Acquisition & Soil Aggregation | 8 | Gap 1 |
| [soil_archaea_aoa.md](soil_archaea_aoa.md) | Soil Archaea: Ammonia-Oxidizing Archaea (AOA) & Methanogens | 10 | Gap 2 |
| [metatranscriptomics_soil.md](metatranscriptomics_soil.md) | Soil Metatranscriptomics: Gene Expression vs. Gene Presence | 8 | Gap 3 |
| [rhizosphere_ecology.md](rhizosphere_ecology.md) | Rhizosphere Microbiome Assembly & Root Exudate Effects | 10 | Gap 4 |
| [mag_quality_checkm.md](mag_quality_checkm.md) | Metagenome-Assembled Genomes (MAGs): Quality Standards & CheckM | 8 | Gap 5 |
| [compositional_data_microbiome.md](compositional_data_microbiome.md) | Compositional Data Analysis for Microbiome (CLR, ALR, ILR Transforms) | 8 | T0.25 pitfall |
| [genome_scale_model_reconstruction.md](genome_scale_model_reconstruction.md) | Automated Genome-Scale Metabolic Model Reconstruction (CarveMe, ModelSEED) | 8 | Phase 3.4 model_builder.py |
| [metadata_standards_mixs.md](metadata_standards_mixs.md) | Microbiome Metadata Standards: MIxS, ENVO & Sample Annotation | 8 | Easy Win #1 |
| [soil_ph_community_driver.md](soil_ph_community_driver.md) | Soil pH as Primary Driver of Microbial Community Structure | 8 | Core design decision |
| [bioremediation_hydrocarbon.md](bioremediation_hydrocarbon.md) | Soil Bioremediation: Microbial Hydrocarbon Degradation & Bioaugmentation | 10 | Phase 7.4 |
| [niche_competition_inoculant.md](niche_competition_inoculant.md) | Microbial Niche Competition & Inoculant Establishment in Soil | 8 | Phase 4.3 establishment_predictor.py |
| [unite_fungal_taxonomy.md](unite_fungal_taxonomy.md) | UNITE Database & Fungal Taxonomic Classification from ITS | 6 | Gap 1 |

---

## Gap-Fill → Module Mapping

| Module / Phase | Gap-fill reference(s) |
|---------------|----------------------|
| Phase 0.1: Schema (fungi, archaea) | `fungal_ecology_its.md`, `amf_phosphorus_aggregates.md`, `soil_archaea_aoa.md` |
| Phase 0.4: MetadataNormalizer | `metadata_standards_mixs.md` |
| Phase 1.4: functional_gene_scanner.py | `soil_archaea_aoa.md` (archaeal amoA split) |
| Phase 1.5: tax_profiler.py (ITS) | `fungal_ecology_its.md`, `unite_fungal_taxonomy.md` |
| Phase 2.4: functional_predictor.py | `compositional_data_microbiome.md` (CLR transform) |
| Phase 3.1: genome_fetcher.py | `mag_quality_checkm.md` |
| Phase 3.2: genome_quality.py | `mag_quality_checkm.md` |
| Phase 3.4: model_builder.py | `genome_scale_model_reconstruction.md` |
| Phase 4.3: establishment_predictor.py | `niche_competition_inoculant.md` |
| Phase 7.1: C-seq config | `fungal_ecology_its.md`, `amf_phosphorus_aggregates.md` |
| Phase 7.4: bioremediation config | `bioremediation_hydrocarbon.md` |
| T0 filter design | `soil_ph_community_driver.md` |
| Gap 3: metatranscriptomics | `metatranscriptomics_soil.md` |
| Gap 4: rhizosphere | `rhizosphere_ecology.md` |
