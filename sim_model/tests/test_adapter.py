from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.adapter import map_pipeline_record_to_sim_inputs, simulate_from_pipeline_record


def test_maps_canonical_fields_without_imputation():
    record = {
        "diazotroph_abundance": 0.61,
        "decomposer_abundance": 0.42,
        "competitor_abundance": 0.19,
        "stress_tolerant_abundance": 0.36,
        "soil_ph": 6.7,
        "organic_matter_pct": 5.5,
        "moisture": 0.63,
        "temperature_c": 24.5,
        "inoculation_strength": 0.4,
        "amendment_strength": 0.2,
        "management_shift": 0.1,
    }
    bundle = map_pipeline_record_to_sim_inputs(record)

    assert bundle.community.diazotrophs == 0.61
    assert bundle.environment.soil_ph == 6.7
    assert bundle.intervention.inoculation_strength == 0.4
    assert bundle.diagnostics["imputed_fields"] == []


def test_aliases_and_unit_normalization_are_applied():
    record = {
        "metadata": {
            "ph": 6.2,
            "soc": 0.038,
            "moisture_pct": 48,
            "soil_temp_c": 26,
        },
        "features": {
            "nifH": 22,
            "saprotroph_abundance": 34,
            "competition_index": 45,
            "stress_tolerance_index": 18,
        },
    }

    bundle = map_pipeline_record_to_sim_inputs(record)

    assert bundle.environment.soil_ph == 6.2
    assert bundle.environment.organic_matter_pct == 3.8
    assert bundle.environment.moisture == 0.48
    assert bundle.community.diazotrophs == 0.22
    assert bundle.community.decomposers == 0.34
    assert bundle.community.competitors == 0.45
    assert bundle.community.stress_tolerant_taxa == 0.18
    assert "inoculation_strength" in bundle.diagnostics["imputed_fields"]


def test_intervention_candidate_is_mapped():
    record = {"soil_ph": 6.8}
    candidate = {
        "intervention_type": "bioinoculant",
        "predicted_effect": 0.8,
        "establishment_prob": 0.5,
    }
    bundle = map_pipeline_record_to_sim_inputs(record, intervention_candidate=candidate)

    assert bundle.intervention.inoculation_strength == 0.4
    assert bundle.intervention.amendment_strength == 0.0
    assert bundle.diagnostics["used_fields"]["inoculation_strength"] == "intervention_candidate"


def test_simulate_from_pipeline_record_runs_end_to_end():
    record = {
        "metadata": {"ph": 6.9, "organic_matter_pct": 4.9, "moisture_pct": 56, "temperature_c": 23.5},
        "features": {
            "diazotroph_rel_abundance": 0.48,
            "decomposer_abundance": 0.41,
            "competitive_exclusion_index": 0.23,
            "stress_tolerant_abundance": 0.28,
        },
    }
    payload = simulate_from_pipeline_record(
        record=record,
        intervention_candidate={"intervention_type": "amendment", "rate_t_ha": 2.5, "predicted_effect": 0.3},
    )

    assert payload["target_flux"] > 0.0
    assert 0.0 <= payload["stability_score"] <= 1.0
    assert payload["inputs"]["intervention"]["amendment_strength"] == 0.5
    assert "adapter_diagnostics" in payload
