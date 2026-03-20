"""Tests for application switching via target configs."""

from __future__ import annotations

from sim_model.dynamics import simulate_dynamics, simulate_dynamics_with_target
from sim_model.schema import Community, Environment, Intervention
from sim_model.targets import TARGET_REGISTRY, get_target, list_targets


def _community(guild: str, value: float) -> Community:
    """Build a community with one guild dominant."""
    base = {"diazotrophs": 0.1, "decomposers": 0.1, "competitors": 0.1, "stress_tolerant_taxa": 0.1}
    base[guild] = value
    return Community(**base)


def _default_env() -> Environment:
    return Environment(soil_ph=6.8, organic_matter_pct=5.0, moisture=0.62, temperature_c=24.0)


def _no_intervention() -> Intervention:
    return Intervention(0.0, 0.0, 0.0)


class TestTargetRegistry:
    def test_all_targets_registered(self):
        expected = {"bnf", "carbon_sequestration", "pathogen_suppression", "phosphorus_solubilization"}
        assert set(TARGET_REGISTRY.keys()) == expected

    def test_get_target_valid(self):
        target = get_target("bnf")
        assert target["name"] == "bnf"
        assert target["primary_guild"] == "diazotrophs"

    def test_get_target_invalid_raises(self):
        try:
            get_target("nonexistent")
            assert False, "Should have raised KeyError"
        except KeyError as e:
            assert "nonexistent" in str(e)

    def test_list_targets_returns_descriptions(self):
        targets = list_targets()
        assert len(targets) == 4
        for name, desc in targets.items():
            assert isinstance(desc, str)
            assert len(desc) > 0

    def test_all_targets_have_required_keys(self):
        required = {"name", "primary_guild", "secondary_guilds", "antagonist_guilds",
                     "base_biotic", "optimal_ph", "ph_sigma", "optimal_moisture",
                     "moisture_sigma", "optimal_temperature", "temperature_sigma", "flux_scale", "primary_weight"}
        for name, target in TARGET_REGISTRY.items():
            assert set(target.keys()) >= required, f"{name} missing keys: {required - set(target.keys())}"


class TestSimulateDynamicsWithTarget:
    def test_bnf_target_matches_original(self):
        """With target='bnf', simulate_dynamics_with_target should match simulate_dynamics."""
        c = Community(diazotrophs=0.5, decomposers=0.4, competitors=0.2, stress_tolerant_taxa=0.35)
        e = _default_env()
        i = _no_intervention()

        original = simulate_dynamics(c, e, i)
        from_target = simulate_dynamics_with_target(c, e, i, target="bnf")

        assert abs(original.target_flux - from_target.target_flux) < 0.01
        assert abs(original.stability_score - from_target.stability_score) < 0.01

    def test_diazotroph_dominant_community_best_for_bnf(self):
        c = _community("diazotrophs", 0.8)
        e = _default_env()

        bnf = simulate_dynamics_with_target(c, e, _no_intervention(), target="bnf")
        carbon = simulate_dynamics_with_target(c, e, _no_intervention(), target="carbon_sequestration")

        assert bnf.target_flux > carbon.target_flux

    def test_decomposer_dominant_community_best_for_carbon(self):
        """A decomposer-dominant community should score higher on carbon than a diazotroph-only community."""
        decomposer_heavy = Community(diazotrophs=0.05, decomposers=0.85, competitors=0.1, stress_tolerant_taxa=0.3)
        diazotroph_heavy = Community(diazotrophs=0.85, decomposers=0.05, competitors=0.1, stress_tolerant_taxa=0.3)
        e = _default_env()

        # Carbon target should prefer decomposer-heavy community
        carbon_decomp = simulate_dynamics_with_target(decomposer_heavy, e, _no_intervention(), target="carbon_sequestration")
        carbon_diaz = simulate_dynamics_with_target(diazotroph_heavy, e, _no_intervention(), target="carbon_sequestration")
        assert carbon_decomp.target_flux > carbon_diaz.target_flux

        # BNF target should prefer diazotroph-heavy community
        bnf_diaz = simulate_dynamics_with_target(diazotroph_heavy, e, _no_intervention(), target="bnf")
        bnf_decomp = simulate_dynamics_with_target(decomposer_heavy, e, _no_intervention(), target="bnf")
        assert bnf_diaz.target_flux > bnf_decomp.target_flux

    def test_stress_tolerant_community_best_for_pathogen_suppression(self):
        c = _community("stress_tolerant_taxa", 0.8)
        e = _default_env()

        path = simulate_dynamics_with_target(c, e, _no_intervention(), target="pathogen_suppression")
        bnf = simulate_dynamics_with_target(c, e, _no_intervention(), target="bnf")

        assert path.target_flux > bnf.target_flux

    def test_optimal_ph_differs_by_target(self):
        """Different targets have different optimal pH, so the same pH affects them differently."""
        c = Community(diazotrophs=0.5, decomposers=0.5, competitors=0.2, stress_tolerant_taxa=0.3)

        # pH 6.0 is optimal for phosphorus_solubilization (6.0) but not BNF (6.8)
        acidic = Environment(soil_ph=6.0, organic_matter_pct=5.0, moisture=0.62, temperature_c=24.0)
        neutral = Environment(soil_ph=6.8, organic_matter_pct=5.0, moisture=0.62, temperature_c=24.0)

        bnf_acidic = simulate_dynamics_with_target(c, acidic, _no_intervention(), target="bnf")
        bnf_neutral = simulate_dynamics_with_target(c, neutral, _no_intervention(), target="bnf")
        phos_acidic = simulate_dynamics_with_target(c, acidic, _no_intervention(), target="phosphorus_solubilization")
        phos_neutral = simulate_dynamics_with_target(c, neutral, _no_intervention(), target="phosphorus_solubilization")

        # BNF prefers neutral over acidic
        assert bnf_neutral.target_flux > bnf_acidic.target_flux
        # Phosphorus should be closer between acidic and neutral (pH 6.0 is near its optimum)
        ratio_phos = phos_acidic.target_flux / phos_neutral.target_flux if phos_neutral.target_flux > 0 else 0
        ratio_bnf = bnf_acidic.target_flux / bnf_neutral.target_flux if bnf_neutral.target_flux > 0 else 0
        assert ratio_phos > ratio_bnf

    def test_different_targets_give_different_fluxes(self):
        """Same community + env should produce different fluxes for different targets."""
        c = Community(diazotrophs=0.5, decomposers=0.5, competitors=0.2, stress_tolerant_taxa=0.3)
        e = _default_env()

        bnf = simulate_dynamics_with_target(c, e, _no_intervention(), target="bnf")
        carbon = simulate_dynamics_with_target(c, e, _no_intervention(), target="carbon_sequestration")
        pathogen = simulate_dynamics_with_target(c, e, _no_intervention(), target="pathogen_suppression")

        fluxes = {bnf.target_flux, carbon.target_flux, pathogen.target_flux}
        assert len(fluxes) > 1, "Different targets should produce different fluxes"
