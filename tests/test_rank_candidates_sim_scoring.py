from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.analysis.rank_candidates import (
    _extract_top_intervention_candidate,
    _legacy_composite_score,
    _score_row,
    _sim_composite_score,
)


def _base_row() -> dict:
    return {
        "community_id": 101,
        "t1_target_flux": 80.0,
        "t1_model_confidence": "high",
        "t2_stability_score": 0.72,
        "soil_ph": 6.7,
        "temperature_c": 24.0,
        "organic_matter_pct": 5.2,
        "management": '{"moisture_pct": 61}',
        "t2_interventions": '[{"intervention_type":"bioinoculant","predicted_effect":0.7,"establishment_prob":0.6}]',
    }


def test_extract_top_intervention_candidate_list():
    raw = '[{"intervention_type":"amendment","predicted_effect":0.4},{"intervention_type":"management"}]'
    candidate = _extract_top_intervention_candidate(raw)
    assert candidate is not None
    assert candidate["intervention_type"] == "amendment"


def test_sim_composite_score_runs_from_pipeline_row():
    sim_score, sim_result = _sim_composite_score(_base_row())
    assert sim_score is not None
    assert 0.0 <= sim_score <= 1.0
    assert sim_result is not None
    assert "target_flux" in sim_result


def test_score_row_modes_are_consistent():
    row = _base_row()
    legacy_data = _score_row(row, scoring_mode="legacy", legacy_weight=0.5)
    assert legacy_data["composite_score"] == legacy_data["legacy_score"]
    assert legacy_data["sim_score"] is None

    sim_data = _score_row(row, scoring_mode="sim", legacy_weight=0.5)
    assert sim_data["sim_score"] is not None
    assert sim_data["composite_score"] == sim_data["sim_score"]

    hybrid_data = _score_row(row, scoring_mode="hybrid", legacy_weight=0.25)
    assert hybrid_data["sim_score"] is not None
    expected = 0.25 * hybrid_data["legacy_score"] + 0.75 * hybrid_data["sim_score"]
    assert abs(hybrid_data["composite_score"] - expected) < 1e-12


def test_legacy_score_still_available():
    score = _legacy_composite_score(_base_row())
    assert 0.0 <= score <= 1.0


def test_uncertainty_outputs_present_for_sim_mode():
    data = _score_row(
        _base_row(),
        scoring_mode="sim",
        legacy_weight=0.5,
        uncertainty_samples=24,
        risk_aversion=1.2,
        uncertainty_seed=123,
    )
    assert data["uncertainty_samples_used"] == 24
    assert data["score_std"] >= 0.0
    assert data["risk_adjusted_score"] <= data["score_mean"] + 1e-12


def test_uncertainty_disabled_defaults_to_composite():
    data = _score_row(
        _base_row(),
        scoring_mode="hybrid",
        legacy_weight=0.4,
        uncertainty_samples=0,
        risk_aversion=1.5,
        uncertainty_seed=123,
    )
    assert data["score_mean"] == data["composite_score"]
    assert data["score_std"] == 0.0
    assert data["risk_adjusted_score"] == data["composite_score"]
