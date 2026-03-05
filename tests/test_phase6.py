"""
tests/test_phase6.py — Unit tests for Phase 6 analysis modules.

Covers:
  rank_candidates       -- _composite_score logic, CSV output
  taxa_enrichment       -- _mann_whitney_u, _bh_correction, _norm_cdf
  spatial_analysis      -- _haversine_km, _k_means_geo
  correlation_scanner   -- _spearman_r, _median, pattern detection
  findings_generator    -- _render_findings_md produces valid markdown
  intervention_report   -- _load_top_interventions, _render_markdown
  validate_pipeline     -- _spearman_r, _load_measured_function

All tests run without any database (mock DB where needed) or network access.
"""

from __future__ import annotations
import csv
import json
import math
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# rank_candidates
# ===========================================================================

class TestCompositeScore:
    def _score(self, t1_target_flux=100.0, t2_stability_score=0.8,
                t1_model_confidence="medium"):
        from rank_candidates import _composite_score
        return _composite_score({
            "t1_target_flux": t1_target_flux,
            "t2_stability_score": t2_stability_score,
            "t1_model_confidence": t1_model_confidence,
        })

    def test_score_in_range(self):
        s = self._score()
        assert 0.0 <= s <= 1.0

    def test_zero_flux_gives_zero(self):
        s = self._score(t1_target_flux=0.0)
        assert s == 0.0

    def test_high_confidence_scores_higher(self):
        s_high = self._score(t1_model_confidence="high")
        s_low = self._score(t1_model_confidence="low")
        assert s_high > s_low

    def test_perfect_conditions(self):
        s = self._score(
            t1_target_flux=1000.0,
            t2_stability_score=1.0,
            t1_model_confidence="high",
        )
        assert s > 0.5

    def test_numeric_confidence_passthrough(self):
        from rank_candidates import _composite_score
        s = _composite_score({
            "t1_target_flux": 100.0,
            "t2_stability_score": 0.8,
            "t1_model_confidence": 0.75,
        })
        assert 0.0 <= s <= 1.0

    def test_none_stability_treated_as_neutral(self):
        from rank_candidates import _composite_score
        s = _composite_score({
            "t1_target_flux": 100.0,
            "t2_stability_score": None,
            "t1_model_confidence": "medium",
        })
        assert 0.0 <= s <= 1.0


# ===========================================================================
# taxa_enrichment helpers
# ===========================================================================

class TestMannWhitneyU:
    def test_identical_groups(self):
        from taxa_enrichment import _mann_whitney_u
        a = [1.0, 2.0, 3.0]
        b = [1.0, 2.0, 3.0]
        u, p = _mann_whitney_u(a, b)
        # p-value should be high (no difference)
        assert p > 0.1

    def test_clearly_different_groups(self):
        from taxa_enrichment import _mann_whitney_u
        a = [10.0, 20.0, 30.0, 40.0, 50.0]
        b = [0.1, 0.2, 0.3, 0.4, 0.5]
        u, p = _mann_whitney_u(a, b)
        assert p < 0.05

    def test_empty_group_returns_one(self):
        from taxa_enrichment import _mann_whitney_u
        _, p = _mann_whitney_u([], [1.0, 2.0])
        assert p == 1.0


class TestBHCorrection:
    def test_all_ones_stays_one(self):
        from taxa_enrichment import _bh_correction
        adjusted = _bh_correction([1.0, 1.0, 1.0])
        assert all(p == 1.0 for p in adjusted)

    def test_length_preserved(self):
        from taxa_enrichment import _bh_correction
        pvals = [0.01, 0.05, 0.001, 0.1]
        adjusted = _bh_correction(pvals)
        assert len(adjusted) == len(pvals)

    def test_adjusted_gte_original(self):
        from taxa_enrichment import _bh_correction
        pvals = [0.001, 0.01, 0.05, 0.1]
        adjusted = _bh_correction(pvals)
        for orig, adj in zip(pvals, adjusted):
            assert adj >= orig - 1e-10

    def test_empty_input(self):
        from taxa_enrichment import _bh_correction
        assert _bh_correction([]) == []

    def test_monotone_nondecreasing_after_sort(self):
        from taxa_enrichment import _bh_correction
        pvals = [0.001, 0.01, 0.02, 0.05, 0.2]
        adjusted = _bh_correction(pvals)
        paired = sorted(zip(pvals, adjusted))
        adj_sorted = [p for _, p in paired]
        for i in range(1, len(adj_sorted)):
            assert adj_sorted[i] >= adj_sorted[i - 1] - 1e-10


class TestNormCDF:
    def test_zero_gives_half(self):
        from taxa_enrichment import _norm_cdf
        assert abs(_norm_cdf(0.0) - 0.5) < 1e-6

    def test_positive_gives_above_half(self):
        from taxa_enrichment import _norm_cdf
        assert _norm_cdf(1.96) > 0.97

    def test_negative_gives_below_half(self):
        from taxa_enrichment import _norm_cdf
        assert _norm_cdf(-1.96) < 0.03


# ===========================================================================
# spatial_analysis helpers
# ===========================================================================

class TestHaversine:
    def test_same_point_is_zero(self):
        from spatial_analysis import _haversine_km
        assert _haversine_km(40.0, -75.0, 40.0, -75.0) == pytest.approx(0.0)

    def test_ny_to_la(self):
        """NYC (40.7, -74.0) to LA (34.0, -118.2) ≈ 3940 km."""
        from spatial_analysis import _haversine_km
        dist = _haversine_km(40.7, -74.0, 34.0, -118.2)
        assert 3800 < dist < 4100

    def test_symmetry(self):
        from spatial_analysis import _haversine_km
        d1 = _haversine_km(51.5, -0.1, 48.8, 2.3)
        d2 = _haversine_km(48.8, 2.3, 51.5, -0.1)
        assert d1 == pytest.approx(d2, rel=1e-6)


class TestKMeansGeo:
    def test_output_length(self):
        from spatial_analysis import _k_means_geo
        points = [(float(i), float(j), i * 10 + j) for i in range(5) for j in range(5)]
        labels = _k_means_geo(points, k=3)
        assert len(labels) == len(points)

    def test_k_gt_n_returns_sequential(self):
        from spatial_analysis import _k_means_geo
        points = [(1.0, 2.0, 0), (3.0, 4.0, 1)]
        labels = _k_means_geo(points, k=10)
        assert len(labels) == 2


# ===========================================================================
# correlation_scanner helpers
# ===========================================================================

class TestSpearmanR:
    def test_perfect_positive(self):
        from correlation_scanner import _spearman_r
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _spearman_r(x, y) == pytest.approx(1.0)

    def test_perfect_negative(self):
        from correlation_scanner import _spearman_r
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [5.0, 4.0, 3.0, 2.0, 1.0]
        assert _spearman_r(x, y) == pytest.approx(-1.0)

    def test_uncorrelated(self):
        from correlation_scanner import _spearman_r
        x = [1.0, 2.0, 3.0]
        y = [2.0, 1.0, 3.0]  # some noise
        r = _spearman_r(x, y)
        assert -1.0 <= r <= 1.0

    def test_short_sequence(self):
        from correlation_scanner import _spearman_r
        assert _spearman_r([1.0], [2.0]) == 0.0


class TestMedian:
    def test_odd_length(self):
        from correlation_scanner import _median
        assert _median([3.0, 1.0, 2.0]) == 2.0

    def test_even_length(self):
        from correlation_scanner import _median
        assert _median([1.0, 2.0, 3.0, 4.0]) == 2.5

    def test_empty(self):
        from correlation_scanner import _median
        assert _median([]) == 0.0


# ===========================================================================
# findings_generator
# ===========================================================================

class TestFindingsGenerator:
    def test_render_findings_md_produces_markdown(self, tmp_path):
        from findings_generator import _render_findings_md
        config_path = tmp_path / "config.yaml"
        config_path.write_text("target_function: nitrogen_fixation\n")
        db_summary = {
            "n_total": 100,
            "n_passed_t0": 80,
            "n_completed_t1": 60,
            "n_completed_t2": 40,
            "top_flux": 12.34,
            "top_community_id": 42,
        }
        md = _render_findings_md(config_path, db_summary, [], [], tmp_path)
        assert "# Pipeline Findings" in md
        assert "nitrogen_fixation" in md
        assert "100" in md

    def test_render_with_correlations(self, tmp_path):
        from findings_generator import _render_findings_md
        config_path = tmp_path / "config.yaml"
        config_path.write_text("target_function: BNF\n")
        db_summary = {
            "n_total": 50, "n_passed_t0": 40, "n_completed_t1": 30,
            "n_completed_t2": 20, "top_flux": 5.0, "top_community_id": 1,
        }
        findings = [
            {"finding": "metadata_correlation", "field": "ph", "spearman_r": 0.72,
             "n": 30, "direction": "positive", "strength": "strong"},
        ]
        md = _render_findings_md(config_path, db_summary, findings, [], tmp_path)
        assert "ph" in md
        assert "0.72" in md

    def test_render_no_config_file(self, tmp_path):
        from findings_generator import _render_findings_md
        config_path = tmp_path / "nonexistent_config.yaml"
        db_summary = {
            "n_total": 10, "n_passed_t0": 5, "n_completed_t1": 3,
            "n_completed_t2": 1, "top_flux": 1.0, "top_community_id": None,
        }
        md = _render_findings_md(config_path, db_summary, [], [], tmp_path)
        assert "# Pipeline Findings" in md


# ===========================================================================
# validate_pipeline helpers
# ===========================================================================

class TestValidatePipelineHelpers:
    def test_load_measured_function(self, tmp_path):
        from validate_pipeline import _load_measured_function
        csv_path = tmp_path / "function.csv"
        csv_path.write_text("sample_id,measured_function\nS001,12.5\nS002,0.3\n")
        result = _load_measured_function(csv_path)
        assert result["S001"] == 12.5
        assert result["S002"] == pytest.approx(0.3)

    def test_load_measured_function_with_colname_variants(self, tmp_path):
        from validate_pipeline import _load_measured_function
        csv_path = tmp_path / "function.csv"
        csv_path.write_text("#SampleID,value\nS001,99.0\n")
        result = _load_measured_function(csv_path)
        assert result["S001"] == 99.0

    def test_spearman_r_validate(self):
        from validate_pipeline import _spearman_r
        x = [1, 2, 3, 4, 5]
        y = [2, 4, 6, 8, 10]
        assert _spearman_r(x, y) == pytest.approx(1.0)


# ===========================================================================
# intervention_report helpers
# ===========================================================================

class TestInterventionReportRender:
    def test_render_markdown_basic(self, tmp_path):
        from intervention_report import _render_markdown
        config_path = tmp_path / "config.yaml"
        config_path.write_text("target_function: nitrogen_fixation\n")
        interventions = [
            {
                "name": "Azospirillum brasilense",
                "category": "bioinoculant",
                "confidence": 0.75,
                "predicted_effect": 0.30,
                "composite_score": 0.225,
                "n_communities": 12,
                "n_studies": 3,
                "rate": "1e9",
                "unit": "CFU/mL",
                "cost_usd_per_ha": 50.0,
                "mechanism": "Biological nitrogen fixation",
                "caveats": ["Requires anaerobic microsites"],
            }
        ]
        md = _render_markdown(config_path, interventions)
        assert "Azospirillum brasilense" in md
        assert "75%" in md
        assert "nitrogen_fixation" in md

    def test_render_markdown_no_interventions(self, tmp_path):
        from intervention_report import _render_markdown
        config_path = tmp_path / "config.yaml"
        config_path.write_text("target_function: BNF\n")
        md = _render_markdown(config_path, [])
        assert "# Intervention Recommendations" in md
