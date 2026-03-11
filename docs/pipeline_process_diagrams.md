# Pipeline Process Diagrams

Three chemical process flow diagrams for the BNF soil microbiome pipeline. Read in order:

1. **Reference Model** — ideal 4-tier process derived from foundational design docs. The standard we are seeking to achieve.
2. **Current Implementation** — what is actually running as of 2026-03-10. Divergences from reference are annotated with reasons.
3. **High-Value Additions** — reference model plus eight prioritised additions that would materially increase scientific output.

Stream labels show population throughput (samples passing between unit operations). Unit operations are shown with key operating parameters.

---

## 1 — Reference Model

Standard we are seeking to achieve. Four-tier funnel from raw public metagenomes to field-ready intervention recommendations. Each tier reduces the candidate pool ~10× while increasing mechanistic resolution. All numbers are design targets, not actuals.

```mermaid
flowchart TB
    classDef feed fill:#0d2b4e,stroke:#4a90d9,color:#c9e6f8
    classDef unit fill:#0a2a0a,stroke:#2ecc71,color:#c8f0c8
    classDef sep fill:#2a1a00,stroke:#f39c12,color:#fdeaa7
    classDef prod fill:#2d1b4e,stroke:#9b59b6,color:#dab8f5
    classDef waste fill:#1a0a0a,stroke:#666,color:#999
    classDef recycle fill:#1a1a2e,stroke:#e74c3c,color:#fab1b1

    F1[/"NCBI SRA\nshotgun reads\n~2M samples"/]:::feed
    F2[/"EBI MGnify\nprocessed assemblies\n500k+ studies"/]:::feed
    F3[/"NEON · EMP · Qiita\namplicon + metadata"/]:::feed

    subgraph T0["① T0 — QUALITY & COMPOSITION FILTER   µs–ms per sample"]
        U0A["SEQUENCING QC\nmin 50k reads · chimera removal\ncontamination screen"]:::unit
        U0B["METADATA CLASSIFIER\nsoil pH · texture · land use · climate zone\nnifH functional gene presence/absence"]:::unit
        U0C["DIVERSITY ESTIMATOR\nShannon H · Chao1 · Faith PD\nPielou evenness"]:::unit
        S0{{"SEPARATOR\nt0_pass?"}}:::sep
    end

    subgraph T025["② T0.25 — ML FUNCTIONAL PREDICTOR   seconds per sample"]
        U025A["FUNCTIONAL PROFILER\nPICRUSt2 16S→KO pathway\nHUMAnN3 shotgun→MetaCyc\nFaProTax taxonomy→function"]:::unit
        U025B["ML CLASSIFIER\nRF/GBM on OTU table + metadata\nBNF activity score\nfeatures: top genera + pH + OM%"]:::unit
        U025C["SIMILARITY SEARCH\nBray-Curtis + UniFrac vs reference BNF DB\nmin similarity 0.3"]:::unit
        S025{{"SEPARATOR\nt025_pass?"}}:::sep
    end

    subgraph T1["③ T1 — METABOLIC NETWORK REACTOR   minutes per sample"]
        U1A["MAG BINNING\nMetaBat2/SemiBin from shotgun reads\nCheckM completeness filter\nper-sample genome bins"]:::unit
        U1B["GENOME ANNOTATOR\nDRAM/Prokka → KEGG + MetaCyc\nnifH · nifD · nifK confirmed per bin"]:::unit
        U1C["MODEL SYNTHESIZER\nCarveMe genome-scale SBML\nN-limited minimal medium\nMo-nitrogenase added if nifHDK+"]:::unit
        U1D["COMMUNITY FBA REACTOR\nisolated intracellular pools per organism\nshared extracellular pool\nbiomass objective\n90% growth constraint for FVA"]:::unit
        U1E["FVA ANALYZER\nNITROGENASE_MO reactions\nfva_max x2 = NH4-equiv flux\nunits: mmol NH4-equiv per gDW per h"]:::unit
        S1{{"SEPARATOR\nflux >= 0.01\nmmol NH4/gDW/h?"}}:::sep
    end

    subgraph T2["④ T2 — COMMUNITY DYNAMICS REACTOR   hours per sample"]
        U2A["dFBA TIME COURSE\n90-day growing season\nC/N/P depletion kinetics\nclimate perturbation panel"]:::unit
        U2B["PERTURBATION SCREEN\ndrought · heat · flood · pH shift\nstability score · resilience"]:::unit
        U2C["INTERVENTION REACTOR\nbioinoculant candidate screen\namendment effect model\nbiochar pH · compost N-release"]:::unit
        U2D["ESTABLISHMENT PREDICTOR\ncompetitive exclusion model\ninoculant survival vs resident community\nmin_establishment_prob 0.4"]:::unit
        S2{{"SEPARATOR\nstability >= 0.6\nestab >= 0.4?"}}:::sep
    end

    P1[/"TOP-50 BNF COMMUNITIES\nsoil context envelope\nkeystone taxa identified"/]:::prod
    P2[/"INTERVENTION REPORT\norganism · dose · timing\ncost per hectare"/]:::prod
    P3[/"FIELD VALIDATION PACKAGE\nsite selection map\n15N measurement protocol"/]:::prod
    REC(["FINDINGS FEEDBACK\nauto-detected patterns\ncalibrate filters"]):::recycle

    W0["discard ~90%"]:::waste
    W025["discard ~90%"]:::waste
    W1["discard ~90%"]:::waste
    W2["discard ~90%"]:::waste

    F1 & F2 & F3 --> U0A --> U0B --> U0C --> S0
    S0 -->|"~10% pass\n~200k samples"| U025A
    S0 -->|"~90% reject"| W0

    U025A --> U025B --> U025C --> S025
    S025 -->|"~10% pass\n~20k samples"| U1A
    S025 -->|"~90% reject"| W025

    U1A --> U1B --> U1C --> U1D --> U1E --> S1
    S1 -->|"~10% pass\n~2k samples"| U2A
    S1 -->|"~90% reject"| W1

    U2A --> U2B --> U2C --> U2D --> S2
    S2 -->|"~10% pass\ntop 200"| P1
    S2 -->|"~90% reject"| W2

    P1 --> P2 --> P3
    P1 -.->|recycle| REC
    REC -.->|calibrate| U0B
```

---

## 2 — Current Implementation

What is actually running as of 2026-03-11 (latest commit). Orange = skipped or constrained. Red = bugs (all fixed). Green = complete. Numbers from live DB query (`soil_microbiome.db`, 237,662 total runs).

```mermaid
flowchart TB
    classDef feed fill:#0d2b4e,stroke:#4a90d9,color:#c9e6f8
    classDef unit fill:#0a2a0a,stroke:#2ecc71,color:#c8f0c8
    classDef sep fill:#2a1a00,stroke:#f39c12,color:#fdeaa7
    classDef prod fill:#2d1b4e,stroke:#9b59b6,color:#dab8f5
    classDef waste fill:#1a0a0a,stroke:#666,color:#999
    classDef skip fill:#2a1800,stroke:#f39c12,color:#fdeaa7
    classDef warn fill:#1a0d00,stroke:#e67e22,color:#f0c080
    classDef done fill:#0a2a12,stroke:#27ae60,color:#a9dfbf
    classDef bug fill:#2a0000,stroke:#c0392b,color:#e8b0b0

    F1[/"NEON amplicon portal\n16S V4 · 17,567 samples\nneon_adapter.py"/]:::feed
    F2[/"EBI MGnify API\n16S amplicon · 95 studies\nmgnify_adapter.py"/]:::feed
    F3[/"Synthetic communities\n220,000 generated\nmodel validation"/]:::feed
    FW["NO SHOTGUN INPUT\nSRA adapter exists but not triggered\nReason: 16S APIs available first"]:::warn

    subgraph T0["① T0 — 16S QUALITY & CLASSIFICATION   COMPLETE"]
        U0A["16S CLASSIFIER\nvsearch vs SILVA 138 · 97% identity\nSUBSAMPLE_N=10k · threads=1 · maxrejects=8\nprocess_neon_16s.py"]:::unit
        U0B["METADATA FILTER\nsoil pH · land use · depth\nsequencing depth threshold"]:::unit
        U0C["DIVERSITY ESTIMATOR\nShannon H · Chao1\nquality_filter.py"]:::unit
        S0{{"SEPARATOR\nt0_pass?"}}:::sep
        T0R["11,026 NEON pass\n95 MGnify pass\n220k synthetic pass\n→ 231,121 total t0_pass"]:::done
    end

    subgraph T025["② T0.25 — ML PREDICTOR   SURROGATE TRAINED — RF classifier ROC-AUC 0.812"]
        T025S["functional_predictor.py · predict_with_gate()\npicrust2_runner.py · humann3_shortcut.py\nSurrogate RF TRAINED on 5,907 real communities (cf5e081)\nClassifier gate: ROC-AUC 0.812 ± 0.012 · OOB 0.772\nRegressor: R² 0.465 ± 0.025 · OOB 0.469\nTop features: soil_ph (42%) · Nitrososphaerota (19%) · Nitrospirota (12%)\nModels: models/bnf_surrogate_classifier.joblib + bnf_surrogate_regressor.joblib\ncalled via run_t025_batch() (pipeline_core.py:1070)\nsurrogate not yet deployed to /data/pipeline/models/"]:::skip
    end

    subgraph T1["③ T1 — METABOLIC NETWORK REACTOR   COMPLETE — 4,491 communities · 117 min"]
        U1A["GENUS CLASSIFIER\n16S taxonomy → genus names\nvsearch top-hit\nReplaces: MAG binning"]:::unit
        U1B["MODEL LOOKUP\nPre-built AGORA2 SBML per genus\n20 genera on disk\nReplaces: CarveMe per-sample\nDivergence: genus-level proxy only"]:::unit
        U1C["NITROGENASE PATCHER\npatch_diazotroph_models.py\n9 diazotroph genera patched\nNITROGENASE_MO stoichiometry added\ncommit 90f0e92"]:::unit
        U1D["COMMUNITY FBA REACTOR\nN-limited minimal medium · 28/357 exchanges open\nisolated intracellular pools per organism\nshared extracellular pool\nbiomass objective · glpk solver (OSQP unsafe at frac=0.0)\ncommits metabolite-ns + c78a0bd"]:::unit
        U1E["FVA ANALYZER\nNITROGENASE_MO reactions\n90% growth constraint · processes=1\nfva_max x2 = NH4-equiv/gDW/h"]:::unit
        S1{{"SEPARATOR\nflux >= 0.01\nmmol NH4/gDW/h?"}}:::sep
        T1R["4,830+ real t1_pass (2026-03-11, in progress)\n3,378+ BNF-pass (max=50.0, avg=36.23 mmol NH4/gDW/h)\nnon-BNF remainder (biomass proxy)\nkeystone taxa stored per community in DB"]:::done
        T1B["BUG HISTORY — 4 ITERATIONS + SOLVER DOC\n1 Biomass proxy — no nitrogenase in AGORA2 models\n2 EX_nh4_e objective — LP saturation at 1000\n3 Complete AGORA2 medium — ATP-unbounded FVA 100-400\n  Fixed: minimal medium closes 329 exchanges (ad31e7b)\n4 Shared intracellular metabolite pools — max 108 mmol/gDW/h\n  Fixed: namespace met ids per organism (metabolite-ns)\n5 OSQP/hybrid FVA hangs on unconstrained problems\n  Fixed: force glpk · documented (c78a0bd)"]:::bug
    end

    subgraph T2["④ T2 — COMMUNITY DYNAMICS REACTOR   COMPLETE — 117.5 min · 0 errors"]
        U2A["dFBA TIME COURSE\nt2_dfba_batch.py · 4,491 communities\nglpk enforced · climate perturbation panel\n23,378 trajectory records in DB"]:::unit
        U2B["STABILITY SCORER\nstability_analyzer.py\n3,378 t2_pass (stability ≥ 0.30)\nmean stability=0.87 · mean estab_prob=0.93"]:::unit
        T2W["INTERVENTION ASSIGNMENT\nmetadata-driven picker (bbf03a4)\npH-amendment 58% · direct-inoculant 29%\ndiversity-enhancement 8% · drought-tolerant 5%\nT2 metadata enriched: 13 fields (1429734)\nsoil_ph, temp_c, OM%, clay%, precipitation_mm,\nland_use, management, sampling_fraction, site_id, climate_zone\nFlux-based t1_confidence: 0.30→0.85 linear scale\nMechanistic screener wired but AGORA2 models pending"]:::warn
        S2{{"SEPARATOR\nstability >= 0.30?"}}:::sep
    end

    P1[/"3,378 BNF t2_pass communities\nmax flux=50.0 avg=36.23 mmol NH4/gDW/h\nkeystone taxa + dFBA trajectories in DB\n6,413-point CONUS kriging grid"/]:::prod
    P2[/"FINDINGS.md committed 25de5c8\ncorrelation · taxa enrichment · spatial\n100 ranked candidates · 11 interventions"/]:::done
    P3["INTERVENTION REPORT generated (11 recs)\nFIELD PACKAGE: not yet built\nNext: mechanistic bioinoculant screen (T2 gap)"]:::warn

    W0["NEON: 6,541 fail\nMGnify: 0 fail\nSynthetic: 0 fail"]:::waste
    W1["~6,341 communities\nno matching SBML genus"]:::waste
    W2["1,113 communities\nstability < 0.30"]:::waste

    F1 & F2 & F3 --> U0A
    FW -.->|"missing"| F1
    U0A --> U0B --> U0C --> S0
    S0 -->|"pass"| T025S
    S0 -->|"fail"| W0
    S0 -.-> T0R

    T025S -->|"skipped — all pass to T1"| U1A

    U1A --> U1B --> U1C --> U1D --> U1E --> S1
    T1B -.->|"all 5 bugs fixed"| U1D
    S1 -->|"pass"| U2A
    S1 -->|"fail"| W1
    S1 -.-> T1R

    U2A --> U2B --> S2
    T2W -.->|"wired: metadata-driven\nnot yet: mechanistic screen"| U2B
    S2 -->|"pass"| P1
    S2 -->|"fail"| W2

    P1 --> P2
    P2 --> P3
```

---

## 3 — Modular Pipeline Architecture (Rebuild)

Refactored architecture where the **Core Engine** is decoupled from the **Biological Intent**. Applications (BNF, Carbon, etc.) are implemented as plugins.

```mermaid
flowchart TB
    classDef core fill:#0a2a0a,stroke:#2ecc71,color:#c8f0c8
    classDef app  fill:#2d1b4e,stroke:#9b59b6,color:#dab8f5
    classDef db   fill:#2a1a00,stroke:#f39c12,color:#fdeaa7

    YAML[/"config.yaml\n(defines application)"/]:::app
    INTENT["AbstractIntent\n(Stoichiometry, Media, Targets)"]:::app
    
    subgraph CoreEngine["UNIFIED PIPELINE ENGINE"]
        T0["Tier 0\nNormalization"]:::core
        T1["Tier 1\nFBA Funnel"]:::core
        T2["Tier 2\ndFBA Funnel"]:::core
    end

    SoilDB[("SoilDB v3\nDynamic Annotations")]:::db

    YAML --> INTENT
    INTENT --> CoreEngine
    CoreEngine <--> SoilDB
    
    BNF["apps/bnf/intent.py"]:::app
    CARB["apps/carbon/intent.py"]:::app
    
    BNF -.-> INTENT
    CARB -.-> INTENT
```

## 4 — High-Value Additions to Reference Model

Reference model (Diagram 1) plus eight additions (lettered A–H, purple) that would materially increase scientific value. Two feedback loops (blue) close the gap between computational predictions and real-world measurement.

| Addition | Unit Operation | Scientific Value |
|---|---|---|
| **A** — Metatranscriptomics | Expression Filter at T0 | Confirms nifH genes are actively transcribed, not just present; eliminates genomically-capable but transcriptionally-silent communities |
| **B** — 15N isotope dilution | Validation feedback loop | Ground-truth BNF rate from field; recalibrates the 0.01 mmol/gDW/h threshold against measured data |
| **C** — Surrogate FBA predictor | ML unit at T0.25 + training loop | After ~4k FBA runs, train ML to predict NITROGENASE_MO flux from 16S taxonomy; skips FBA for obvious pass/fail; improves over time |
| **D** — Metabolic exchange map | Cross-feeding network at T1 | Maps N/C/energy coupling between community members; identifies syntrophic pairs responsible for high BNF; improves keystone taxa precision |
| **E** — Agent-based model | ABM at T2 | Individual-based spatial dynamics (iDynoMiCS); strain-level competition for establishment; more accurate than population-level dFBA for inoculant survival |
| **F** — Spatial kriging | Post-processing | Kriging interpolation across NEON site coordinates → continuous BNF potential field map; enables site-specific field recommendations |
| **G** — Time-series tracking | Post-processing | Multi-visit NEON data → community BNF trajectory over seasons and years; detects stable vs transient high-BNF communities |
| **H** — Cross-pipeline optimizer | Post-processing | Joint ranking across BNF + C-sequestration + pathogen-suppression pipelines; identifies communities that excel at multiple soil health functions |

```mermaid
flowchart TB
    classDef feed fill:#0d2b4e,stroke:#4a90d9,color:#c9e6f8
    classDef unit fill:#0a2a0a,stroke:#2ecc71,color:#c8f0c8
    classDef sep fill:#2a1a00,stroke:#f39c12,color:#fdeaa7
    classDef prod fill:#2d1b4e,stroke:#9b59b6,color:#dab8f5
    classDef waste fill:#1a0a0a,stroke:#666,color:#999
    classDef add fill:#1a0d2e,stroke:#8e44ad,color:#d7bde2
    classDef done fill:#0a2a12,stroke:#27ae60,color:#a9dfbf
    classDef val fill:#0d1f2e,stroke:#2980b9,color:#aed6f1

    F1[/"NCBI SRA\nshotgun reads\n~2M samples"/]:::feed
    F2[/"EBI MGnify\nprocessed assemblies\n500k+ studies"/]:::feed
    F3[/"NEON · EMP · Qiita\namplicon + metadata"/]:::feed
    FA1[/"METATRANSCRIPTOMICS\nmRNA from paired sites\nnifH expression ratios\nADDITION A"/]:::add
    FA2[/"15N FIELD MEASUREMENTS\nisotope dilution assay\nground-truth BNF rate/gDW/h\nADDITION B"/]:::add

    subgraph T0["① T0 — QUALITY & COMPOSITION FILTER"]
        U0A["SEQUENCING QC\nmin 50k reads · chimera removal"]:::unit
        U0B["METADATA CLASSIFIER\nsoil pH · texture · land use · nifH presence"]:::unit
        U0C["DIVERSITY ESTIMATOR\nShannon H · Chao1 · Faith PD"]:::unit
        U0D["EXPRESSION FILTER\nmrna_to_dna_ratio for nifH\nactive transcription required\nADDITION A"]:::add
        S0{{"SEPARATOR\nt0_pass?"}}:::sep
    end

    subgraph T025["② T0.25 — ML FUNCTIONAL PREDICTOR"]
        U025A["FUNCTIONAL PROFILER\nPICRUSt2 16S→KO\nHUMAnN3 shotgun→MetaCyc"]:::unit
        U025B["ML CLASSIFIER\nRF/GBM: OTU + metadata → BNF score"]:::unit
        U025C["SIMILARITY SEARCH\nBray-Curtis + UniFrac vs reference BNF DB"]:::unit
        U025D["SURROGATE FBA PREDICTOR\nML trained on T1 FVA outputs\npredict NITROGENASE_MO flux from 16S alone\nfast-lane: skip FBA for obvious cases\nADDITION C"]:::add
        S025{{"SEPARATOR\nt025_pass?"}}:::sep
    end

    subgraph T1["③ T1 — METABOLIC NETWORK REACTOR"]
        U1A["MAG BINNING\nMetaBat2/SemiBin · CheckM\nper-sample genome bins"]:::unit
        U1B["GENOME ANNOTATOR\nDRAM/Prokka → KEGG + MetaCyc\nnifH · nifD · nifK per bin"]:::unit
        U1C["MODEL SYNTHESIZER\nCarveMe genome-scale SBML\nMo-nitrogenase from annotation"]:::unit
        U1D["COMMUNITY FBA REACTOR\nN-limited minimal medium\nisolated intracellular pools per organism\nshared extracellular pool\nbiomass objective"]:::unit
        U1E["FVA ANALYZER\nNITROGENASE_MO · 90% growth"]:::unit
        U1F["METABOLIC EXCHANGE MAP\ncross-feeding network analysis\nN/C/energy coupling per pair\nidentify syntrophic keystones\nADDITION D"]:::add
        S1{{"SEPARATOR\nflux >= 0.01\nmmol NH4/gDW/h?"}}:::sep
    end

    subgraph T2["④ T2 — COMMUNITY DYNAMICS REACTOR"]
        U2A["dFBA TIME COURSE\n90-day season · C/N/P kinetics"]:::unit
        U2B["PERTURBATION SCREEN\ndrought · heat · pH shift"]:::unit
        U2C["INTERVENTION REACTOR\nbioinoculant screen · amendment model"]:::unit
        U2D["ESTABLISHMENT PREDICTOR\ncompetitive exclusion · population-level"]:::unit
        U2E["AGENT-BASED COMPETITION\niDynoMiCS individual-based model\nstrain-level spatial dynamics\nhigher accuracy for inoculant survival\nADDITION E"]:::add
        S2{{"SEPARATOR\nstability >= 0.6\nestab >= 0.4?"}}:::sep
    end

    subgraph POST["⑤ POST-PROCESSING — ADDITIONS F · G · H"]
        PP1["SPATIAL INTERPOLATOR — COMPLETE Mar-8\nKriging on NEON GPS coordinates\nBNF potential field map · 6,413-point CONUS grid\nresults: /data/pipeline/results/spatial/\nADDITION F"]:::done
        PP2["TIME-SERIES TRACKER\nmulti-visit NEON sites\ncommunity BNF trajectory\nstable vs transient high-BNF\nADDITION G"]:::add
        PP3["CROSS-PIPELINE OPTIMIZER\nBNF x C-sequestration x pathogen suppression\njoint community ranking\nsoil health index\nADDITION H"]:::add
    end

    P1[/"TOP-50 BNF COMMUNITIES\nsoil context envelope\nkeystone taxa + exchange network"/]:::prod
    P2[/"INTERVENTION REPORT\norganism · dose · timing\ncost per hectare + site map"/]:::prod
    P3[/"FIELD VALIDATION PACKAGE\nsite selection · 15N protocol\nground-truth measurement plan"/]:::prod

    VL["15N VALIDATION LOOP\nfield BNF rate vs predicted flux\nrecalibrate 0.01 threshold\nADDITION B"]:::val
    SL["SURROGATE TRAINING LOOP\nT1 FVA results → ML training set\napprox 4k runs = usable model\nimproves continuously\nADDITION C"]:::val

    W0["discard ~90%"]:::waste
    W025["discard ~90%"]:::waste
    W1["discard ~90%"]:::waste
    W2["discard ~90%"]:::waste

    FA1 --> U0D
    F1 & F2 & F3 --> U0A --> U0B --> U0C --> U0D --> S0
    S0 -->|"~10% pass"| U025A
    S0 -->|"~90% reject"| W0

    U025A --> U025B --> U025C --> U025D --> S025
    S025 -->|"~10% pass"| U1A
    S025 -->|"~90% reject"| W025

    U1A --> U1B --> U1C --> U1D --> U1E --> U1F --> S1
    S1 -->|"~10% pass"| U2A
    S1 -->|"~90% reject"| W1

    U2A --> U2B --> U2C --> U2D --> U2E --> S2
    S2 -->|"~10% pass"| P1
    S2 -->|"~90% reject"| W2

    P1 --> PP1 & PP2 & PP3
    PP1 & PP2 & PP3 --> P2 --> P3

    U1E -.->|"FVA training data"| SL
    SL -.->|"retrain surrogate"| U025D
    FA2 -.->|"measured field rates"| VL
    P1 -.->|"predicted flux"| VL
    VL -.->|"recalibrate threshold"| S1
```

---

## Divergence Summary — Reference vs Current

| Step | Reference | Current | Reason |
|---|---|---|---|
| **Input** | Shotgun metagenomes from SRA (millions) | 16S amplicon: NEON 17,567 + MGnify 95 + 220k synthetic | 16S APIs available first; SRA shotgun not yet triggered |
| **T0 method** | Multi-source QC + functional gene scan | vsearch 16S → SILVA 138 classification only | Sufficient for 16S; functional gene scan deferred to T1 genus lookup |
| **T0.25 ML** | PICRUSt2 → RF/GBM BNF score → similarity search | Surrogate **trained**: RF classifier gate (ROC-AUC 0.812) + regressor (R² 0.465) on 5,907 real samples. `predict_with_gate()` wired. Not yet called by `pipeline_core.py` T0.25 batch | HUMAnN3 is shotgun-only; PICRUSt2 not wired into batch; surrogate awaits pipeline integration |
| **T1 genome models** | CarveMe from per-sample MAG bins | Pre-built AGORA2 SBML, 20 genera on disk | CarveMe requires shotgun MAGs; genus-level proxy loses strain variation |
| **T1 nitrogenase** | Present from annotation-driven model build | Patched into 9 genera via patch_diazotroph_models.py | AGORA2 template omits nitrogenase; not a catalogued AGORA2 reaction |
| **T1 medium** | N-limited minimal medium from the start | 3 iterations to reach correct medium (commits 90f0e92 → 13ee41d → ad31e7b) | AGORA2 ships with complete medium; LP saturation and ATP-unbounded FVA not obvious until empirically observed |
| **T1 results** | ~2,000 high-confidence metabolic hits | 4,830+ real t1_pass (3,378+ BNF) — **COMPLETE** | max=50.0, avg=36.23 mmol NH₄/gDW/h; metabolite-ns namespace fix confirmed cap |
| **T2 real** | Run after T1 completes | **COMPLETE** — 4,491 communities in 117.5 min, 0 errors | `scripts/t2_dfba_batch.py` built (f46a1a4); 3,378 t2_pass (stability ≥ 0.30) |
| **T2 intervention** | Full bioinoculant + amendment screen | Metadata-driven picker deployed (bbf03a4); 13-field metadata now passed (1429734): temp, OM%, clay%, precipitation, land_use, management, sampling_fraction, site_id, climate_zone; flux-based t1_confidence | `intervention_screener.py` wired with full metadata; mechanistic niche scoring awaits AGORA2 genus models |
| **Output** | Ranked communities + intervention report + field package | 100 ranked candidates · 11 interventions · FINDINGS.md committed (25de5c8) | Field validation package not yet built; mechanistic intervention screening is next gap |
| **BNF flux ceiling** | Theoretical max ~45 mmol/gDW/h per diazotroph at 10 mmol glucose | max 50.0 (within expected range); avg 36.23 | Metabolite-ns namespace fix confirmed: shared atp_c was root cause; ceiling now capped at 50.0 by FVA cap constant |
| **Solver safety** | Any FBA solver | glpk enforced everywhere (c78a0bd) | OSQP/hybrid hangs on AGORA2 models at fraction_of_optimum=0.0 (1,183 lb=-1000 reactions); documented and enforced |
