# AGORA2 Integration Plan

## Background

**AGORA2** (Assembly of Gut and Oral microbiome for Metabolic Analysis, version 2.0) contains
7,302 genus-level genome-scale metabolic models (GSMMs) for bacteria and archaea found in the
human gut, oral cavity, and soil microbiomes.  The models are available in SBML format from the
Virtual Metabolic Human database (VMH) and were published in:

> Heinken et al. (2023) *Genome-scale metabolic reconstruction of 7,302 human microorganisms
> for personalized medicine*. Nature Biotechnology, 41, 1320–1331.

Although built with a gut focus, ~620 genera overlap with NEON soil microbiome top_genera.

**Current state**: `compute/model_builder.py` generates 20 synthetic SBML stubs.  These are
placeholders with minimal reaction content, which limits FBA precision and niche-overlap
scoring in `compute/establishment_predictor.py`.

---

## Scope

Replace the synthetic SBML stubs with real AGORA2 genus-level models for any genus present
in the pipeline's communities.  Unmatched genera retain the synthetic stub.

---

## Data Sources

| Resource | URL | Format |
|---|---|---|
| AGORA2 full model archive | https://www.vmh.life/files/reconstructions/AGORA2/Version1.0/mat/AGORA2_microbiome.zip | MATLAB .mat |
| AGORA2 SBML (individual files) | https://www.vmh.life/files/reconstructions/AGORA2/Version1.0/sbml/ | SBML .xml |
| AGORA2 metadata table | https://www.vmh.life/files/reconstructions/AGORA2/AGORA2_info.xlsx | XLSX |
| VMH search API | https://www.vmh.life/api/microbes/?format=json | JSON REST |

Download the SBML archive (~4 GB uncompressed) or individual models via the VMH API.

---

## Implementation Plan

### Phase 0 — Dependency check (1 day)

```bash
pip install cobra lxml  # COBRApy for SBML parsing
# Verify: python -c "import cobra; print(cobra.__version__)"
```

### Phase 1 — Download and index AGORA2 (2 days)

Create `scripts/fetch_agora2.py`:

```python
"""
Download AGORA2 SBML models for genera present in the pipeline DB.

Usage:
    python scripts/fetch_agora2.py \
        --db /data/pipeline/db/soil_microbiome.db \
        --out-dir models/agora2/ \
        --top-genera 200  # only fetch genera present in ≥1 community
"""
```

Logic:
1. Query `SELECT DISTINCT top_genera FROM communities LIMIT 50000` from the DB
2. Parse all `top_genera` JSON arrays to extract unique genus names
3. Match against AGORA2 metadata table (fuzzy match on genus name, ≥0.85 similarity)
4. Download matched SBML files to `models/agora2/<genus_name>.xml`
5. Log match rate and unmatched genera

Expected match rate: ~40–60% of top genera (based on AGORA2 soil-relevant subset).

### Phase 2 — Wire into model_builder.py (3 days)

Update `compute/model_builder.py`:

```python
AGORA2_DIR = Path("models/agora2/")

def _load_agora2_model(genus_name: str) -> cobra.Model | None:
    """Load an AGORA2 SBML model for the given genus, or return None."""
    safe = genus_name.replace(" ", "_")
    candidates = [
        AGORA2_DIR / f"{safe}.xml",
        AGORA2_DIR / f"{safe.lower()}.xml",
    ]
    for path in candidates:
        if path.exists():
            try:
                return cobra.io.read_sbml_model(str(path))
            except Exception as e:
                logger.warning("Failed to load AGORA2 model for %s: %s", genus_name, e)
    return None

def build_community_model(top_genera: list[dict], metadata: dict) -> cobra.Model:
    """
    Build a community metabolic model by merging AGORA2 genus-level models.
    Falls back to synthetic stub for unmatched genera.
    """
    member_models = []
    for entry in top_genera[:20]:  # cap at 20 members (T1 config limit)
        genus = entry["name"]
        model = _load_agora2_model(genus) or _build_synthetic_stub(genus, metadata)
        member_models.append((genus, model, entry["rel_abundance"]))
    return _merge_models(member_models, metadata)
```

### Phase 3 — Integrate with T1 batch (2 days)

In `pipeline_core.py` `_process_one_t1()`:
- Call `build_community_model(top_genera, metadata)` for real AGORA2-backed model
- Pass the model to `screen_interventions()` (ends the `community_model=None` workaround)
- `_score_niche_overlap()` will now use real reaction sets for establishment estimation

### Phase 4 — Validation (2 days)

- Run `validate_pipeline.py` with `--model-path models/functional_predictor.joblib`
  on 50 AGORA2-backed vs 50 synthetic-stub communities
- Check: Spearman r improves from current baseline by ≥0.05
- Check: niche overlap scores become genus-specific (not always 0.5)

---

## File Additions

```
scripts/fetch_agora2.py             — download + index AGORA2 models
models/agora2/                      — SBML files (gitignored via models/*)
models/agora2/MANIFEST.tsv          — genus → file path mapping (tracked)
compute/model_builder.py            — updated to load AGORA2 first
```

`.gitignore` already ignores `models/*` except `models/README.md`.  Add:
```
!models/agora2/MANIFEST.tsv
```

---

## Expected Impact

| Metric | Before (synthetic) | After (AGORA2) |
|---|---|---|
| Reaction count per member | ~30 stubs | 500–2000 real reactions |
| Niche overlap resolution | 0.5 default | genus-specific |
| T1 FBA flux precision | ±20% | ±5–10% (estimated) |
| Establishment scoring | pH + temp only | pH + temp + niche + guild |

---

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Low genus match rate in soil communities | Retain synthetic stubs for unmatched genera; log match rate |
| AGORA2 models not validated for soil conditions | Use only reaction topology; flux bounds remain metadata-driven |
| COBRApy memory usage for 20-member community | Use `cobra.Model.merge()` with shared metabolite namespace; cap at 20 members |
| VMH download rate limits | Implement polite scraping (1 req/s, exponential backoff) |

---

## Timeline

| Week | Tasks |
|---|---|
| W1 | Phase 0–1: deps + download script + genus matching |
| W2 | Phase 2: wire model_builder.py, unit tests |
| W3 | Phase 3: T1 batch integration, end-to-end run |
| W4 | Phase 4: validation + performance comparison |

---
*Last updated: this session. Implementation Owner: pipeline maintainer.*
