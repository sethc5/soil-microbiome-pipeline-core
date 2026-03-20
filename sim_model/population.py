"""T0 filtering simulation — model how raw samples survive quality + metadata + gene filters."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence, Tuple

from .schema import Community, Environment


@dataclass(frozen=True)
class RawSample:
    """A sample as it enters the pipeline before any filtering."""

    community: Community
    environment: Environment
    sequencing_depth: int
    nifh_read_count: int
    is_contaminated: bool
    is_flooded: bool
    observed_otus: int


@dataclass(frozen=True)
class T0FilterResult:
    """Outcome of T0 quality + metadata + functional gene filtering."""

    passed: bool
    reject_reason: str
    sample: RawSample


# ---------------------------------------------------------------------------
# Default T0 thresholds (mirrors config.example.yaml t0 section)
# ---------------------------------------------------------------------------

DEFAULT_T0_THRESHOLDS: Dict[str, Any] = {
    "min_sequencing_depth": 50000,
    "min_observed_otus": 500,
    "ph_range": [5.0, 8.5],
    "min_nifh_reads": 5,
    "exclude_contaminated": True,
    "exclude_flooded": True,
    "min_organic_matter_pct": 0.5,
}


# ---------------------------------------------------------------------------
# Population generation
# ---------------------------------------------------------------------------

def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def generate_raw_sample(
    rng: random.Random,
    nifh_prevalence: float = 0.30,
    contamination_rate: float = 0.05,
    flooded_rate: float = 0.08,
) -> RawSample:
    """Generate one synthetic raw sample with realistic metadata noise."""

    diazotrophs = rng.uniform(0.0, 0.90)
    decomposers = rng.uniform(0.0, 0.90)
    competitors = rng.uniform(0.0, 0.90)
    stress_tolerant = rng.uniform(0.0, 0.90)

    community = Community(
        diazotrophs=diazotrophs,
        decomposers=decomposers,
        competitors=competitors,
        stress_tolerant_taxa=stress_tolerant,
    )

    soil_ph = rng.uniform(3.5, 9.5)
    om = rng.uniform(0.0, 18.0)
    moisture = rng.uniform(0.0, 1.0)
    temperature = rng.uniform(-5.0, 45.0)

    environment = Environment(
        soil_ph=soil_ph,
        organic_matter_pct=om,
        moisture=moisture,
        temperature_c=temperature,
    )

    depth = int(rng.lognormvariate(10.5, 1.2))  # median ~37K, long tail
    depth = max(depth, 100)

    # nifH reads correlate with diazotroph abundance but with noise
    has_nifh = rng.random() < nifh_prevalence
    if has_nifh:
        nifh_reads = max(0, int(rng.gauss(25, 15) + 40 * diazotrophs))
    else:
        nifh_reads = 0

    is_contaminated = rng.random() < contamination_rate
    is_flooded = rng.random() < flooded_rate

    # observed OTUs correlate with depth and diversity
    diversity_proxy = (diazotrophs + decomposers + competitors + stress_tolerant) / 4.0
    otus = max(10, int(rng.gauss(800, 400) * (0.5 + diversity_proxy)))
    otus = min(otus, 50000)

    return RawSample(
        community=community,
        environment=environment,
        sequencing_depth=depth,
        nifh_read_count=nifh_reads,
        is_contaminated=is_contaminated,
        is_flooded=is_flooded,
        observed_otus=otus,
    )


def generate_sample_population(
    n: int = 1000,
    random_state: int = 42,
    **kwargs: Any,
) -> List[RawSample]:
    """Generate a population of raw samples."""
    rng = random.Random(random_state)
    return [generate_raw_sample(rng, **kwargs) for _ in range(n)]


# ---------------------------------------------------------------------------
# T0 filtering
# ---------------------------------------------------------------------------

def t0_filter(
    sample: RawSample,
    thresholds: Dict[str, Any] | None = None,
) -> T0FilterResult:
    """Apply T0 quality + metadata + functional gene filters to one sample."""

    t = {**DEFAULT_T0_THRESHOLDS, **(thresholds or {})}

    if sample.sequencing_depth < t["min_sequencing_depth"]:
        return T0FilterResult(False, f"depth={sample.sequencing_depth}<{t['min_sequencing_depth']}", sample)

    if sample.observed_otus < t["min_observed_otus"]:
        return T0FilterResult(False, f"otus={sample.observed_otus}<{t['min_observed_otus']}", sample)

    ph_lo, ph_hi = t["ph_range"]
    if not (ph_lo <= sample.environment.soil_ph <= ph_hi):
        return T0FilterResult(False, f"ph={sample.environment.soil_ph:.1f} outside [{ph_lo},{ph_hi}]", sample)

    if sample.nifh_read_count < t["min_nifh_reads"]:
        return T0FilterResult(False, f"nifh={sample.nifh_read_count}<{t['min_nifh_reads']}", sample)

    if t["exclude_contaminated"] and sample.is_contaminated:
        return T0FilterResult(False, "contaminated", sample)

    if t["exclude_flooded"] and sample.is_flooded:
        return T0FilterResult(False, "flooded", sample)

    if sample.environment.organic_matter_pct < t["min_organic_matter_pct"]:
        return T0FilterResult(False, f"om={sample.environment.organic_matter_pct:.1f}<{t['min_organic_matter_pct']}", sample)

    return T0FilterResult(True, "", sample)


def filter_population(
    samples: List[RawSample],
    thresholds: Dict[str, Any] | None = None,
) -> List[T0FilterResult]:
    """Apply T0 filters to a population of raw samples."""
    return [t0_filter(s, thresholds) for s in samples]


def population_filter_summary(
    results: List[T0FilterResult],
) -> Dict[str, Any]:
    """Summarize T0 filter outcomes across a population."""

    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed

    reject_counts: Dict[str, int] = {}
    for r in results:
        if not r.passed:
            # extract the reason prefix (before the '=' or the full string)
            reason_key = r.reject_reason.split("=")[0]
            reject_counts[reason_key] = reject_counts.get(reason_key, 0) + 1

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "pass_rate": passed / total if total > 0 else 0.0,
        "reject_counts": dict(sorted(reject_counts.items(), key=lambda x: -x[1])),
    }