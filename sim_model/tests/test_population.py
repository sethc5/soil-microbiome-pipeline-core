"""Tests for T0 filtering simulation."""

from __future__ import annotations

import random

from sim_model.population import (
    RawSample,
    T0FilterResult,
    filter_population,
    generate_raw_sample,
    generate_sample_population,
    population_filter_summary,
    t0_filter,
)
from sim_model.schema import Community, Environment


def _make_sample(**overrides) -> RawSample:
    """Helper to build a RawSample with sensible defaults that pass T0."""
    defaults = dict(
        community=Community(diazotrophs=0.5, decomposers=0.4, competitors=0.2, stress_tolerant_taxa=0.3),
        environment=Environment(soil_ph=6.5, organic_matter_pct=5.0, moisture=0.6, temperature_c=22.0),
        sequencing_depth=80000,
        nifh_read_count=20,
        is_contaminated=False,
        is_flooded=False,
        observed_otus=1200,
    )
    defaults.update(overrides)
    return RawSample(**defaults)


class TestT0Filter:
    def test_default_sample_passes(self):
        sample = _make_sample()
        result = t0_filter(sample)
        assert result.passed is True
        assert result.reject_reason == ""

    def test_low_depth_rejected(self):
        sample = _make_sample(sequencing_depth=10000)
        result = t0_filter(sample)
        assert result.passed is False
        assert "depth" in result.reject_reason

    def test_low_otus_rejected(self):
        sample = _make_sample(observed_otus=100)
        result = t0_filter(sample)
        assert result.passed is False
        assert "otus" in result.reject_reason

    def test_extreme_ph_rejected(self):
        sample = _make_sample(environment=Environment(soil_ph=3.0, organic_matter_pct=5.0, moisture=0.6, temperature_c=22.0))
        result = t0_filter(sample)
        assert result.passed is False
        assert "ph" in result.reject_reason

    def test_high_ph_rejected(self):
        sample = _make_sample(environment=Environment(soil_ph=9.5, organic_matter_pct=5.0, moisture=0.6, temperature_c=22.0))
        result = t0_filter(sample)
        assert result.passed is False
        assert "ph" in result.reject_reason

    def test_no_nifh_rejected(self):
        sample = _make_sample(nifh_read_count=0)
        result = t0_filter(sample)
        assert result.passed is False
        assert "nifh" in result.reject_reason

    def test_contaminated_rejected(self):
        sample = _make_sample(is_contaminated=True)
        result = t0_filter(sample)
        assert result.passed is False
        assert "contaminated" in result.reject_reason

    def test_flooded_rejected(self):
        sample = _make_sample(is_flooded=True)
        result = t0_filter(sample)
        assert result.passed is False
        assert "flooded" in result.reject_reason

    def test_low_organic_matter_rejected(self):
        sample = _make_sample(environment=Environment(soil_ph=6.5, organic_matter_pct=0.1, moisture=0.6, temperature_c=22.0))
        result = t0_filter(sample)
        assert result.passed is False
        assert "om" in result.reject_reason

    def test_custom_thresholds(self):
        sample = _make_sample(sequencing_depth=20000)
        # With default threshold (50000), this fails
        assert t0_filter(sample).passed is False
        # With relaxed threshold, it passes depth check (but may fail others)
        result = t0_filter(sample, thresholds={"min_sequencing_depth": 10000})
        assert result.passed is True

    def test_depth_check_before_ph(self):
        """Filter order: depth is checked first, so a sample with bad depth
        gets rejected for depth even if pH is also bad."""
        sample = _make_sample(
            sequencing_depth=1000,
            environment=Environment(soil_ph=2.0, organic_matter_pct=5.0, moisture=0.6, temperature_c=22.0),
        )
        result = t0_filter(sample)
        assert result.passed is False
        assert "depth" in result.reject_reason


class TestPopulationGeneration:
    def test_generate_population_size(self):
        pop = generate_sample_population(n=50, random_state=7)
        assert len(pop) == 50

    def test_generate_population_reproducible(self):
        pop1 = generate_sample_population(n=20, random_state=99)
        pop2 = generate_sample_population(n=20, random_state=99)
        assert pop1 == pop2

    def test_generate_population_variety(self):
        pop = generate_sample_population(n=200, random_state=42)
        depths = {s.sequencing_depth for s in pop}
        assert len(depths) > 10  # should have many unique depths

    def test_nifh_prevalence_affects_pass_rate(self):
        """Higher nifH prevalence should yield higher T0 pass rate."""
        pop_low = generate_sample_population(n=500, random_state=1, nifh_prevalence=0.10)
        pop_high = generate_sample_population(n=500, random_state=1, nifh_prevalence=0.80)

        results_low = filter_population(pop_low)
        results_high = filter_population(pop_high)

        summary_low = population_filter_summary(results_low)
        summary_high = population_filter_summary(results_high)

        assert summary_high["pass_rate"] > summary_low["pass_rate"]


class TestPopulationFilterSummary:
    def test_summary_counts(self):
        pop = generate_sample_population(n=100, random_state=42)
        results = filter_population(pop)
        summary = population_filter_summary(results)

        assert summary["total"] == 100
        assert summary["passed"] + summary["failed"] == 100
        assert 0.0 <= summary["pass_rate"] <= 1.0
        assert isinstance(summary["reject_counts"], dict)

    def test_all_pass_population(self):
        """A population where every sample should pass."""
        samples = [_make_sample() for _ in range(10)]
        results = filter_population(samples)
        summary = population_filter_summary(results)

        assert summary["passed"] == 10
        assert summary["failed"] == 0
        assert summary["pass_rate"] == 1.0
        assert len(summary["reject_counts"]) == 0

    def test_all_fail_population(self):
        """A population where every sample should fail (low depth)."""
        samples = [_make_sample(sequencing_depth=100) for _ in range(10)]
        results = filter_population(samples)
        summary = population_filter_summary(results)

        assert summary["passed"] == 0
        assert summary["failed"] == 10
        assert summary["pass_rate"] == 0.0
        assert "depth" in summary["reject_counts"]


class TestNifhPrevalenceEffect:
    """Verify the key qualitative invariant: nifH prevalence drives T0 pass rate."""

    def test_nifh_is_top_reject_reason_at_low_prevalence(self):
        pop = generate_sample_population(n=2000, random_state=42, nifh_prevalence=0.10)
        results = filter_population(pop)
        summary = population_filter_summary(results)

        # nifh should be a major reject reason when prevalence is low
        reject_counts = summary["reject_counts"]
        assert "nifh" in reject_counts
        # nifh rejects should exceed 10% of total
        assert reject_counts["nifh"] > summary["total"] * 0.10

    def test_pass_rate_scales_with_nifh_prevalence(self):
        rates = []
        for prev in [0.10, 0.30, 0.50, 0.70, 0.90]:
            pop = generate_sample_population(n=1000, random_state=42, nifh_prevalence=prev)
            results = filter_population(pop)
            summary = population_filter_summary(results)
            rates.append(summary["pass_rate"])

        # pass rate should be monotonically increasing with nifh prevalence
        for i in range(len(rates) - 1):
            assert rates[i] <= rates[i + 1], f"pass_rate not monotonic: {rates}"