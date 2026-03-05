"""
tests/test_phase4.py — Unit tests for Phase 4 (T2) dynamics and intervention modules.

Covers:
  dfba_runner           -- graceful cobra fallback, trajectory structure
  stability_analyzer    -- resistance/resilience from synthetic trajectory
  establishment_predictor -- output in [0,1], component scores
  amendment_effect_model  -- all 6 amendment types, pH delta direction
  intervention_screener   -- returns ranked list, scores in [0,1]

All tests run without COBRApy installed.
"""

from __future__ import annotations
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# dfba_runner
# ===========================================================================

class TestDFBARunner:
    def test_cobra_not_available_returns_empty(self, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "cobra":
                raise ImportError("cobra not installed")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        from compute.dfba_runner import run_dfba
        result = run_dfba(community_model=None, metadata={})
        assert isinstance(result, dict)
        # Either empty result or graceful degradation
        assert "time_points" in result or "stability_score" in result

    def test_no_model_returns_graceful(self):
        from compute.dfba_runner import run_dfba
        result = run_dfba(community_model=None, metadata={})
        assert isinstance(result, dict)


# ===========================================================================
# stability_analyzer
# ===========================================================================

class TestStabilityAnalyzer:
    def _make_trajectory(self, n_days=30, perturb_day=15, drop=0.5, recovery=0.3):
        """Return a simple flux trajectory with a perturbation at perturb_day."""
        import numpy as np
        t = list(range(n_days))
        flux = []
        for day in t:
            if day < perturb_day:
                flux.append(1.0)
            elif day < perturb_day + 5:
                flux.append(1.0 - drop)
            else:
                flux.append(1.0 - drop + recovery * min((day - perturb_day - 5) / 5, 1.0))
        return {"time_points": t, "target_flux_trajectory": flux}

    def test_score_in_range(self):
        from compute.stability_analyzer import compute_stability_score
        traj = self._make_trajectory()
        score = compute_stability_score(traj, perturbation_days=[])
        assert 0.0 <= score <= 1.0

    def test_stable_trajectory_scores_high(self):
        """Constant trajectory should score near 1.0."""
        from compute.stability_analyzer import compute_stability_score
        traj = {
            "time_points": list(range(30)),
            "target_flux_trajectory": [1.0] * 30,
        }
        score = compute_stability_score(traj, perturbation_days=[])
        assert score > 0.7

    def test_chaotic_trajectory_scores_lower(self):
        """Highly variable trajectory should score below a stable one."""
        import math
        from compute.stability_analyzer import compute_stability_score
        t = list(range(30))
        flux_stable = [1.0] * 30
        flux_chaotic = [1.0 + (i % 3 - 1) * 0.9 for i in range(30)]
        s1 = compute_stability_score({"time_points": t, "target_flux_trajectory": flux_stable}, perturbation_days=[])
        s2 = compute_stability_score({"time_points": t, "target_flux_trajectory": flux_chaotic}, perturbation_days=[])
        assert s1 > s2

    def test_functional_redundancy_range(self):
        from compute.stability_analyzer import compute_functional_redundancy
        member_keystones = [
            {"is_keystone": True},
            {"is_keystone": False},
            {"is_keystone": False},
        ]
        r = compute_functional_redundancy(member_keystones)
        assert 0.0 <= r <= 1.0

    def test_functional_redundancy_all_keystones(self):
        from compute.stability_analyzer import compute_functional_redundancy
        member_keystones = [{"is_keystone": True}] * 5
        r = compute_functional_redundancy(member_keystones)
        assert r == 0.0  # no redundancy when all are keystone

    def test_functional_redundancy_none_keystones(self):
        from compute.stability_analyzer import compute_functional_redundancy
        member_keystones = [{"is_keystone": False}] * 5
        r = compute_functional_redundancy(member_keystones)
        assert r == 1.0  # complete redundancy

    def test_full_stability_report_keys(self):
        from compute.stability_analyzer import full_stability_report
        traj = {
            "time_points": list(range(20)),
            "target_flux_trajectory": [1.0] * 20,
        }
        report = full_stability_report(traj, perturbation_days=[], member_keystones=[])
        assert "stability_score" in report
        assert "resistance" in report
        assert "resilience" in report
        assert "functional_redundancy" in report


# ===========================================================================
# establishment_predictor
# ===========================================================================

class TestEstablishmentPredictor:
    def _make_taxon(self, **kwargs):
        defaults = {
            "taxon_name": "Azospirillum brasilense",
            "functional_guild": "nitrogen_fixation",
            "ph_range": [5.5, 7.5],
            "temp_range": [15, 35],
        }
        defaults.update(kwargs)
        return defaults

    def _make_metadata(self, **kwargs):
        defaults = {"soil_ph": 6.5, "soil_temp_c": 20.0}
        defaults.update(kwargs)
        return defaults

    def test_output_in_range(self):
        from compute.establishment_predictor import predict_establishment
        score = predict_establishment(
            inoculant_taxon=self._make_taxon(),
            community_model=None,
            metadata=self._make_metadata(),
        )
        assert 0.0 <= score <= 1.0

    def test_optimal_ph_scores_high(self):
        from compute.establishment_predictor import predict_establishment
        score_good = predict_establishment(
            inoculant_taxon=self._make_taxon(ph_range=[6.0, 7.0]),
            community_model=None,
            metadata=self._make_metadata(soil_ph=6.5),
        )
        score_bad = predict_establishment(
            inoculant_taxon=self._make_taxon(ph_range=[6.0, 7.0]),
            community_model=None,
            metadata=self._make_metadata(soil_ph=4.0),  # far outside range
        )
        assert score_good > score_bad

    def test_detailed_output_keys(self):
        from compute.establishment_predictor import predict_establishment_detailed
        result = predict_establishment_detailed(
            inoculant_taxon=self._make_taxon(),
            community_model=None,
            metadata=self._make_metadata(),
        )
        assert "establishment_prob" in result
        assert "ph_compatibility" in result
        assert "temperature_compatibility" in result

    def test_out_of_range_ph_lowers_score(self):
        from compute.establishment_predictor import predict_establishment
        score_in = predict_establishment(
            inoculant_taxon=self._make_taxon(ph_range=[6.0, 7.0]),
            community_model=None,
            metadata=self._make_metadata(soil_ph=6.5),
        )
        score_out = predict_establishment(
            inoculant_taxon=self._make_taxon(ph_range=[6.0, 7.0]),
            community_model=None,
            metadata=self._make_metadata(soil_ph=9.0),
        )
        assert score_in > score_out


# ===========================================================================
# amendment_effect_model
# ===========================================================================

class TestAmendmentEffectModel:
    @pytest.mark.parametrize("amendment_type", [
        "biochar", "compost", "lime", "sulfur", "rock_phosphate", "vermicompost"
    ])
    def test_all_amendment_types_run(self, amendment_type):
        from compute.amendment_effect_model import compute_amendment_effect
        metadata = {"ph": 6.0, "temperature": 20.0}
        result = compute_amendment_effect(
            metadata=metadata,
            amendment_type=amendment_type,
            rate_t_ha=2.0,
        )
        assert "updated_metadata" in result
        assert "amendment_type" in result
        assert result["amendment_type"] == amendment_type

    def test_lime_raises_ph(self):
        from compute.amendment_effect_model import compute_amendment_effect
        result = compute_amendment_effect(
            metadata={"ph": 5.0, "temperature": 20.0},
            amendment_type="lime",
            rate_t_ha=2.0,
        )
        assert result["predicted_ph_change"] > 0

    def test_sulfur_lowers_ph(self):
        from compute.amendment_effect_model import compute_amendment_effect
        result = compute_amendment_effect(
            metadata={"ph": 7.5, "temperature": 20.0},
            amendment_type="sulfur",
            rate_t_ha=1.0,
        )
        assert result["predicted_ph_change"] < 0

    def test_cost_estimate_positive(self):
        from compute.amendment_effect_model import compute_amendment_effect
        result = compute_amendment_effect(
            metadata={"ph": 6.0},
            amendment_type="compost",
            rate_t_ha=3.0,
        )
        assert result["cost_estimate_usd_per_ha"] > 0

    def test_conservative_vs_optimistic(self):
        from compute.amendment_effect_model import compute_amendment_effect
        conservative = compute_amendment_effect(
            metadata={"ph": 6.0},
            amendment_type="biochar",
            rate_t_ha=2.0,
            use_conservative=True,
        )
        optimistic = compute_amendment_effect(
            metadata={"ph": 6.0},
            amendment_type="biochar",
            rate_t_ha=2.0,
            use_conservative=False,
        )
        # Conservative N change should be smaller in magnitude
        assert abs(conservative["predicted_n_change_ppm"]) <= abs(optimistic["predicted_n_change_ppm"])


# ===========================================================================
# intervention_screener
# ===========================================================================

class TestInterventionScreener:
    def test_returns_list(self):
        from compute.intervention_screener import screen_interventions
        results = screen_interventions(
            community_model=None,
            metadata={"soil_ph": 6.5, "soil_temp_c": 20.0},
            t2_config={},
        )
        assert isinstance(results, list)

    def test_results_non_empty_by_default(self):
        from compute.intervention_screener import screen_interventions
        results = screen_interventions(
            community_model=None,
            metadata={"soil_ph": 6.5, "soil_temp_c": 20.0},
            t2_config={},
        )
        assert len(results) > 0

    def test_result_has_required_keys(self):
        from compute.intervention_screener import screen_interventions
        results = screen_interventions(
            community_model=None,
            metadata={"soil_ph": 6.5, "soil_temp_c": 20.0},
            t2_config={},
        )
        for r in results:
            assert "intervention_type" in r
            assert "intervention_detail" in r
            assert "confidence" in r
            assert "predicted_effect" in r

    def test_scores_in_range(self):
        from compute.intervention_screener import screen_interventions
        results = screen_interventions(
            community_model=None,
            metadata={"soil_ph": 6.5, "soil_temp_c": 20.0},
            t2_config={},
        )
        for r in results:
            assert 0.0 <= r["confidence"] <= 1.0
            assert 0.0 <= r["predicted_effect"] <= 1.0

    def test_sorted_by_composite_score_descending(self):
        from compute.intervention_screener import screen_interventions
        results = screen_interventions(
            community_model=None,
            metadata={"soil_ph": 6.5, "soil_temp_c": 20.0},
            t2_config={},
        )
        scores = [r["confidence"] * r["predicted_effect"] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_disable_bioinoculants(self):
        from compute.intervention_screener import screen_interventions
        results_all = screen_interventions(
            community_model=None, metadata={"soil_ph": 6.5},
            t2_config={}, include_bioinoculants=True,
        )
        results_no_bio = screen_interventions(
            community_model=None, metadata={"soil_ph": 6.5},
            t2_config={}, include_bioinoculants=False,
        )
        bio_types = [r["intervention_type"] for r in results_no_bio]
        assert "bioinoculant" not in bio_types

    def test_disable_amendments(self):
        from compute.intervention_screener import screen_interventions
        results = screen_interventions(
            community_model=None, metadata={"soil_ph": 6.5},
            t2_config={}, include_amendments=False,
        )
        types = [r["intervention_type"] for r in results]
        assert "amendment" not in types
