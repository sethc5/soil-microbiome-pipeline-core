# Sim Model

This folder is for the smallest useful executable version of the soil microbiome pipeline's intent.

The goal is not to replace the full pipeline. The goal is to create a compact model that:

- fits in a small context window
- expresses the real scientific intent of the project
- produces known, testable outcomes
- gives the larger pipeline a stable conceptual core
- lets us validate funnel logic in a world where ground truth is known

## Why this exists

The full soil microbiome pipeline is trying to do several hard things at once:

- represent communities
- represent environments
- represent interventions
- predict target function
- predict stability under perturbation
- rank actionable recommendations

That is a lot of surface area for one codebase and one context window.

A small simulation model gives us a "compressed theory of the project":

- what entities matter
- what state variables matter
- what counts as success
- what must remain true as the big system grows

If the large pipeline is realism, this folder is clarity.

## What the sim model should contain

The first version should be deliberately simple and pure Python.

It should model:

- `Community`
- `Environment`
- `Intervention`
- `SimulationResult`

With a very small number of variables:

- community guild abundances
  - diazotrophs
  - decomposers
  - competitors
  - stress-tolerant taxa
- environment
  - `soil_ph`
  - `organic_matter_pct`
  - `moisture`
  - `temperature_c`
- intervention knobs
  - inoculation strength
  - amendment strength
  - management shift

And a very small number of outputs:

- `target_flux`
- `stability_score`
- `establishment_probability`
- `best_intervention_class`

## What this sim model is for

This folder should support four jobs:

1. Define the intent of the project in a compact form.
2. Generate synthetic worlds where the true causal structure is known.
3. Test whether the funnel recovers useful candidates in those worlds.
4. Provide training data for fast surrogate models.

## Design principles

- Keep it small enough to understand in one sitting.
- Prefer explicit equations over hidden complexity.
- Prefer stable contracts over realism.
- Keep the output schema aligned with the full pipeline.
- Every variable should earn its place.

## First implementation target

The first implementation does not need FBA, dFBA, PICRUSt2, or external tools.

It only needs a toy ecology model with a few sensible assumptions:

- diazotroph abundance raises BNF potential
- organic matter supports activity up to a point
- pH away from the preferred band reduces function
- moisture and temperature alter both flux and stability
- competitors can suppress establishment
- interventions improve or worsen outcomes depending on environment

That gives us a tiny world with known rules.

## Minimal ladder

We should build upward in this order:

1. `schema.py`
   Defines the entities and outputs.
2. `dynamics.py`
   Implements the toy equations.
3. `scenarios.py`
   Stores hand-designed test cases.
4. `simulate.py`
   Runs one community + environment + intervention.
5. `surrogate.py`
   Trains a small predictive model on synthetic outputs.
6. `tests/`
   Verifies known qualitative behavior.

## Example qualitative invariants

These should become tests early:

- More diazotroph abundance should usually increase `target_flux`.
- Extreme `soil_ph` should reduce both flux and stability.
- Very low organic matter should cap achievable function.
- Some interventions should help only in compatible environments.
- A community can have high raw flux but low stability.
- The simulator should produce both easy and ambiguous cases.

## Relationship to the full pipeline

The large pipeline should eventually be judged against this small model, not just against prose.

The small model gives us:

- a compact statement of intent
- a shared output contract
- a source of synthetic benchmark data
- a place to test ranking logic before expensive compute

The full pipeline then becomes the realism layer that replaces toy assumptions with:

- real taxonomic profiles
- real functional predictors
- real metabolic models
- real dynamics
- real validation data

But it should preserve the same basic concepts and outputs.

## Current state

All steps in the minimal ladder are complete, plus 5 additional capability gaps:

| Module | Tests | What it does |
|---|---|---|
| `schema.py` | — | Community, Environment, Intervention, SimulationResult |
| `dynamics.py` | — | Toy equations with guild budget normalization + resource consumption |
| `scenarios.py` | — | 5 hand-designed test cases |
| `simulate.py` | 3 | CLI runner with scenario selection + overrides |
| `surrogate.py` | 1 | RF regressor + classifier on synthetic data |
| `population.py` | 20 | T0 filtering simulation (raw samples → quality/metadata/gene filters) |
| `targets.py` | 11 | 4 application targets (BNF, carbon, pathogen, phosphorus) |
| `interventions.py` | 20 | 8 named interventions with pH/moisture compatibility |
| `spatial.py` | 16 | 5 US geographic regions, clustering, cluster ranking |
| `adapter.py` | 4 | Pipeline metadata → sim input mapping |
| `benchmark.py` | 3 | Multi-world ranking benchmark (funnel vs random vs oracle) |
| `benchmark_gate.py` | 8 | CI gate with trend detection |
| `calibration.py` | 3 | Drift detection (9 qualitative checks) |
| `history_health.py` | 2 | Benchmark baseline staleness checker |
| **Total** | **113** | |

**Gates:** benchmark gate ✅, calibration gate 9/9 ✅

**Rigor fixes applied:**
- Guild budget: abundances normalized to sum to 1.0
- Resource consumption: high primary guild + low OM → reduced flux
- Perturbation modeling: drought/heat/fertilizer/acid_rain presets

## Near-term success criterion

This folder is useful once we can:

- [x] run a toy simulation from a few inputs
- [x] generate a small synthetic dataset
- [x] train a surrogate on that dataset
- [x] recover known high-value communities better than random
- [x] explain why a candidate passed or failed
- [x] simulate T0 filtering on raw sample populations
- [x] switch between 4 application targets via config
- [x] rank specific named interventions (not just classes)
- [x] cluster communities geographically and rank clusters
- [x] model resource consumption and guild budget constraints

The project now has a small executable theory that covers the full funnel from raw samples through spatial ranking.

## CI benchmark gate

Ranking logic now has a CI gate that enforces minimum lift over random on synthetic worlds.

The same check can be run locally:

```bash
python3 -m sim_model.benchmark_gate \
  --seeds 7,13,29 \
  --worlds 180 \
  --candidates 10 \
  --top-k 3 \
  --min-top1-lift 0.03 \
  --min-topk-lift 0.02 \
  --min-regret-reduction 0.25 \
  --min-hit-rate-margin 0.15 \
  --json
```

## Calibration checks

Calibration checks validate that key qualitative and quantitative behavior stays stable as equations evolve.

Default config: `configs/sim_model_calibration.yaml`

Run:

```bash
python3 -m sim_model.calibration --config configs/sim_model_calibration.yaml --json
```

The command exits non-zero when drift thresholds are violated.

## CI lanes

- Fast lane: `.github/workflows/sim-model-benchmark-gate.yml` on PR/push with:
  - calibration gate (`sim_model.calibration`)
  - benchmark gate (moderate settings)
- Nightly stress lane: `.github/workflows/sim-model-benchmark-nightly.yml` with larger worlds/seeds and trend assertion against `reference/sim_model_benchmark_history.jsonl`.
- Both lanes write report artifacts:
  - `results/sim_model_benchmark_latest.json`
  - `results/sim_model_benchmark_summary.md`
