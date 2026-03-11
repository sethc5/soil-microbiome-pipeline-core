# models/

Pre-trained ML model files live here.  
Not tracked in git (binary, large).  Reproduce with:

```bash
python scripts/train_bnf_surrogate.py \
  --db /data/pipeline/db/soil_microbiome.db \
  --out-dir models/
```

## Files

| File | Description | CV metric |
|------|-------------|-----------|
| `functional_predictor.joblib` | Canonical regressor loaded by `compute/functional_predictor.py` at runtime. Predicts mmol NH₄/gDW/h from phylum profile + soil metadata. Includes embedded classifier gate. | R² 0.465 ± 0.025 |
| `bnf_surrogate_classifier.joblib` | Standalone pass/fail gate classifier. | ROC-AUC 0.812 ± 0.012 |
| `bnf_surrogate_regressor.joblib` | Standalone flux regressor (BNF-pass communities only). | R² 0.465 ± 0.025 |

## Feature set (17 features)

| Feature | Type |
|---------|------|
| Proteobacteria, Actinobacteria, Acidobacteria, Firmicutes, Bacteroidetes, Verrucomicrobia, Planctomycetes, Chloroflexota, Gemmatimonadota, Nitrospirota, Cyanobacteria, Nitrososphaerota | Phylum relative abundance from `phylum_profile` column |
| soil_ph, organic_matter_pct, clay_pct, temperature_c, precipitation_mm | Soil/climate metadata |

## Top predictors (2026-03-10 training)

Trained on **5,907 real samples** (4,491 BNF-pass) from NEON + MGnify + synthetic via the live DB.

- `soil_ph` — 42% importance (classifier), 46% (regressor). pH is the dominant control on BNF community composition.
- `Nitrososphaerota` — 19%/23%. Archaeal ammonia oxidizers compete for N substrate; their abundance inversely tracks BNF potential.
- `Nitrospirota` — 12%/10%. Nitrite oxidizers; similar competitive dynamic.
- `Chloroflexota` — 11%. Photoheterotrophs; carbon cycling context.
- `Gemmatimonadota` — 9%/10%. Slow-growing oligotrophs; co-vary with stable high-BNF communities.

Retrain after any major DB expansion (>2× more real T1 results).
