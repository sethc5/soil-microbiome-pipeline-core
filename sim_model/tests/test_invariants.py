from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.dynamics import simulate_dynamics
from sim_model.scenarios import get_scenario
from sim_model.schema import Community, Environment, Intervention


def test_more_diazotrophs_usually_increases_flux():
    env = Environment(soil_ph=6.8, organic_matter_pct=6.0, moisture=0.62, temperature_c=24.0)
    intervention = Intervention(0.0, 0.0, 0.0)
    low = Community(diazotrophs=0.12, decomposers=0.35, competitors=0.22, stress_tolerant_taxa=0.30)
    high = Community(diazotrophs=0.58, decomposers=0.35, competitors=0.22, stress_tolerant_taxa=0.30)

    low_result = simulate_dynamics(low, env, intervention)
    high_result = simulate_dynamics(high, env, intervention)

    assert high_result.target_flux > low_result.target_flux


def test_extreme_ph_reduces_flux_and_stability():
    community = Community(diazotrophs=0.52, decomposers=0.42, competitors=0.20, stress_tolerant_taxa=0.35)
    intervention = Intervention(0.0, 0.0, 0.0)
    neutral = Environment(soil_ph=6.8, organic_matter_pct=5.2, moisture=0.62, temperature_c=24.0)
    extreme = Environment(soil_ph=4.2, organic_matter_pct=5.2, moisture=0.62, temperature_c=24.0)

    neutral_result = simulate_dynamics(community, neutral, intervention)
    extreme_result = simulate_dynamics(community, extreme, intervention)

    assert extreme_result.target_flux < neutral_result.target_flux
    assert extreme_result.stability_score < neutral_result.stability_score


def test_low_organic_matter_caps_flux():
    community = Community(diazotrophs=0.60, decomposers=0.40, competitors=0.15, stress_tolerant_taxa=0.30)
    intervention = Intervention(0.0, 0.0, 0.0)
    low_om = Environment(soil_ph=6.8, organic_matter_pct=0.5, moisture=0.62, temperature_c=24.0)
    higher_om = Environment(soil_ph=6.8, organic_matter_pct=6.0, moisture=0.62, temperature_c=24.0)

    low_result = simulate_dynamics(community, low_om, intervention)
    high_result = simulate_dynamics(community, higher_om, intervention)

    assert low_result.target_flux < high_result.target_flux


def test_intervention_compatibility_effect():
    community = Community(diazotrophs=0.45, decomposers=0.32, competitors=0.26, stress_tolerant_taxa=0.34)
    inoculation = Intervention(inoculation_strength=0.85, amendment_strength=0.0, management_shift=0.0)
    baseline = Intervention(0.0, 0.0, 0.0)

    compatible_env = Environment(soil_ph=6.7, organic_matter_pct=5.0, moisture=0.63, temperature_c=23.0)
    incompatible_env = Environment(soil_ph=4.5, organic_matter_pct=5.0, moisture=0.22, temperature_c=35.0)

    compatible_delta = (
        simulate_dynamics(community, compatible_env, inoculation).target_flux
        - simulate_dynamics(community, compatible_env, baseline).target_flux
    )
    incompatible_delta = (
        simulate_dynamics(community, incompatible_env, inoculation).target_flux
        - simulate_dynamics(community, incompatible_env, baseline).target_flux
    )

    assert compatible_delta > incompatible_delta


def test_can_have_high_flux_but_low_stability():
    scenario = get_scenario("high_flux_low_stability")
    result = simulate_dynamics(scenario.community, scenario.environment, scenario.intervention)

    assert result.target_flux >= 7.0
    assert result.stability_score < 0.45


def test_easy_and_ambiguous_cases_exist():
    easy = get_scenario("easy_win")
    ambiguous = get_scenario("ambiguous_tradeoff")
    easy_result = simulate_dynamics(easy.community, easy.environment, easy.intervention)
    ambiguous_result = simulate_dynamics(
        ambiguous.community,
        ambiguous.environment,
        ambiguous.intervention,
    )

    assert easy_result.target_flux > ambiguous_result.target_flux
    assert easy_result.stability_score > ambiguous_result.stability_score
    assert 0.35 <= ambiguous_result.establishment_probability <= 0.75
