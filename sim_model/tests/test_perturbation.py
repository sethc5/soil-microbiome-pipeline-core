"""Tests for perturbation modeling in dynamics."""

from __future__ import annotations

from sim_model.dynamics import (
    PERTURBATION_PRESETS,
    _apply_perturbation,
    simulate_with_perturbation,
)
from sim_model.schema import Community, Environment, Intervention


def _default_community() -> Community:
    return Community(diazotrophs=0.5, decomposers=0.4, competitors=0.2, stress_tolerant_taxa=0.35)


def _default_environment() -> Environment:
    return Environment(soil_ph=6.8, organic_matter_pct=5.0, moisture=0.62, temperature_c=24.0)


def _no_intervention() -> Intervention:
    return Intervention(0.0, 0.0, 0.0)


class TestApplyPerturbation:
    def test_drought_reduces_moisture(self):
        env = _default_environment()
        perturbed = _apply_perturbation(env, {"preset": "drought", "severity": 1.0})
        assert perturbed.moisture < env.moisture

    def test_heat_wave_increases_temperature(self):
        env = _default_environment()
        perturbed = _apply_perturbation(env, {"preset": "heat_wave", "severity": 1.0})
        assert perturbed.temperature_c > env.temperature_c

    def test_acid_rain_reduces_ph(self):
        env = _default_environment()
        perturbed = _apply_perturbation(env, {"preset": "acid_rain", "severity": 1.0})
        assert perturbed.soil_ph < env.soil_ph

    def test_fertilizer_increases_organic_matter(self):
        env = _default_environment()
        perturbed = _apply_perturbation(env, {"preset": "fertilizer_addition", "severity": 1.0})
        assert perturbed.organic_matter_pct > env.organic_matter_pct

    def test_severity_scales_effect(self):
        env = _default_environment()
        mild = _apply_perturbation(env, {"preset": "drought", "severity": 0.3})
        severe = _apply_perturbation(env, {"preset": "drought", "severity": 1.0})
        # severe drought should reduce moisture more
        assert mild.moisture > severe.moisture

    def test_custom_deltas_override_preset(self):
        env = _default_environment()
        perturbed = _apply_perturbation(env, {
            "preset": "drought",
            "severity": 1.0,
            "moisture_delta": -0.05,  # override the preset's -0.35
        })
        # custom delta * severity = -0.05 * 1.0 = -0.05
        assert abs(perturbed.moisture - (env.moisture - 0.05)) < 0.01

    def test_no_preset_uses_custom_deltas(self):
        env = _default_environment()
        perturbed = _apply_perturbation(env, {
            "ph_delta": -0.5,
            "severity": 1.0,
        })
        assert abs(perturbed.soil_ph - (env.soil_ph - 0.5)) < 0.01

    def test_clamping_prevents_extreme_values(self):
        env = Environment(soil_ph=3.5, organic_matter_pct=0.1, moisture=0.05, temperature_c=-3.0)
        perturbed = _apply_perturbation(env, {
            "ph_delta": -5.0,
            "moisture_delta": -1.0,
            "temperature_delta": -20.0,
            "severity": 1.0,
        })
        assert perturbed.soil_ph >= 2.0
        assert perturbed.moisture >= 0.0
        assert perturbed.temperature_c >= -5.0


class TestSimulateWithPerturbation:
    def test_returns_baseline_and_perturbed(self):
        result = simulate_with_perturbation(
            _default_community(), _default_environment(), _no_intervention(),
            {"preset": "drought", "severity": 0.5},
        )
        assert "baseline" in result
        assert "perturbed" in result
        assert "delta_flux" in result
        assert "delta_stability" in result
        assert "flux_resilience" in result

    def test_drought_reduces_flux(self):
        result = simulate_with_perturbation(
            _default_community(), _default_environment(), _no_intervention(),
            {"preset": "drought", "severity": 0.8},
        )
        assert result["delta_flux"] < 0

    def test_drought_reduces_stability(self):
        result = simulate_with_perturbation(
            _default_community(), _default_environment(), _no_intervention(),
            {"preset": "drought", "severity": 0.8},
        )
        assert result["delta_stability"] < 0

    def test_flux_resilience_bounded(self):
        result = simulate_with_perturbation(
            _default_community(), _default_environment(), _no_intervention(),
            {"preset": "drought", "severity": 0.5},
        )
        assert 0.0 <= result["flux_resilience"] <= 1.0

    def test_stress_tolerant_community_more_stable_under_drought(self):
        """A community with high stress_tolerance should maintain higher stability under drought."""
        env = _default_environment()
        perturbation = {"preset": "drought", "severity": 0.7}

        resilient = Community(diazotrophs=0.4, decomposers=0.3, competitors=0.1, stress_tolerant_taxa=0.8)
        fragile = Community(diazotrophs=0.4, decomposers=0.3, competitors=0.8, stress_tolerant_taxa=0.1)

        res_result = simulate_with_perturbation(resilient, env, _no_intervention(), perturbation)
        frag_result = simulate_with_perturbation(fragile, env, _no_intervention(), perturbation)

        # stress-tolerant community should lose less stability
        assert res_result["delta_stability"] > frag_result["delta_stability"]

    def test_low_moisture_env_hit_harder_by_drought(self):
        """An already-dry environment should suffer more from further drought."""
        moist_env = _default_environment()
        dry_env = Environment(soil_ph=6.8, organic_matter_pct=5.0, moisture=0.30, temperature_c=24.0)
        perturbation = {"preset": "drought", "severity": 0.5}

        moist_result = simulate_with_perturbation(_default_community(), moist_env, _no_intervention(), perturbation)
        dry_result = simulate_with_perturbation(_default_community(), dry_env, _no_intervention(), perturbation)

        # dry environment should lose a larger fraction of flux
        assert dry_result["flux_resilience"] < moist_result["flux_resilience"]


class TestPerturbationPresets:
    def test_all_presets_exist(self):
        expected = {"drought", "heat_wave", "fertilizer_addition", "acid_rain"}
        assert set(PERTURBATION_PRESETS.keys()) == expected

    def test_preset_keys_consistent(self):
        required_keys = {"moisture_delta", "temperature_delta", "ph_delta", "om_delta"}
        for name, preset in PERTURBATION_PRESETS.items():
            assert set(preset.keys()) == required_keys, f"{name} has unexpected keys"