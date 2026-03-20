"""Tests for intervention specificity."""

from __future__ import annotations

from sim_model.interventions import (
    INTERVENTION_CATALOG,
    apply_intervention,
    get_intervention,
    list_interventions,
    rank_interventions,
    simulate_with_named_intervention,
)
from sim_model.schema import Community, Environment


def _base_community() -> Community:
    return Community(diazotrophs=0.3, decomposers=0.3, competitors=0.2, stress_tolerant_taxa=0.3)


def _neutral_env() -> Environment:
    return Environment(soil_ph=6.5, organic_matter_pct=5.0, moisture=0.60, temperature_c=24.0)


def _acidic_dry_env() -> Environment:
    return Environment(soil_ph=4.5, organic_matter_pct=3.0, moisture=0.25, temperature_c=22.0)


class TestCatalog:
    def test_all_interventions_registered(self):
        assert len(INTERVENTION_CATALOG) == 8

    def test_get_intervention_valid(self):
        spec = get_intervention("azospirillum_brasilense")
        assert spec["type"] == "inoculation"
        assert spec["strength"] > 0

    def test_get_intervention_invalid_raises(self):
        try:
            get_intervention("fake_organism")
            assert False, "Should raise KeyError"
        except KeyError:
            pass

    def test_list_interventions(self):
        listing = list_interventions()
        assert len(listing) == 8
        for name, display in listing.items():
            assert isinstance(display, str)
            assert len(display) > 0

    def test_all_have_required_keys(self):
        required = {"type", "display_name", "strength", "guild_boost", "competitor_resistance"}
        for name, spec in INTERVENTION_CATALOG.items():
            assert set(spec.keys()) >= required, f"{name} missing: {required - set(spec.keys())}"

    def test_inoculants_have_ph_preference(self):
        for name, spec in INTERVENTION_CATALOG.items():
            if spec["type"] == "inoculation":
                assert spec.get("ph_preference") is not None, f"{name} missing ph_preference"
                assert spec.get("moisture_floor") is not None, f"{name} missing moisture_floor"


class TestApplyIntervention:
    def test_azospirillum_boosts_diazotrophs(self):
        c = _base_community()
        mod_c, _, _ = apply_intervention(c, _neutral_env(), "azospirillum_brasilense")
        assert mod_c.diazotrophs > c.diazotrophs

    def test_biochar_raises_ph(self):
        e = _neutral_env()
        _, mod_e, _ = apply_intervention(_base_community(), e, "biochar_5t_ha")
        assert mod_e.soil_ph > e.soil_ph

    def test_compost_raises_om_more_than_biochar(self):
        e = _neutral_env()
        _, biochar_e, _ = apply_intervention(_base_community(), e, "biochar_5t_ha")
        _, compost_e, _ = apply_intervention(_base_community(), e, "compost_10t_ha")
        assert compost_e.organic_matter_pct > biochar_e.organic_matter_pct

    def test_azospirillum_reduced_effect_at_acidic_ph(self):
        c = _base_community()
        _, _, strength_neutral = apply_intervention(c, _neutral_env(), "azospirillum_brasilense")
        _, _, strength_acidic = apply_intervention(c, _acidic_dry_env(), "azospirillum_brasilense")
        assert strength_neutral > strength_acidic

    def test_herbaspirillum_reduced_effect_at_low_moisture(self):
        c = _base_community()
        dry = Environment(soil_ph=6.5, organic_matter_pct=5.0, moisture=0.25, temperature_c=24.0)
        wet = Environment(soil_ph=6.5, organic_matter_pct=5.0, moisture=0.65, temperature_c=24.0)
        _, _, strength_dry = apply_intervention(c, dry, "herbaspirillum_seropedicae")
        _, _, strength_wet = apply_intervention(c, wet, "herbaspirillum_seropedicae")
        assert strength_wet > strength_dry

    def test_biochar_no_moisture_floor_penalty(self):
        """Amendments have no moisture_floor, so effective strength equals base strength."""
        c = _base_community()
        _, _, strength = apply_intervention(c, _acidic_dry_env(), "biochar_5t_ha")
        assert strength == get_intervention("biochar_5t_ha")["strength"]


class TestRankInterventions:
    def test_returns_top_k(self):
        results = rank_interventions(_base_community(), _neutral_env(), target="bnf", top_k=3)
        assert len(results) == 3

    def test_results_sorted_descending(self):
        results = rank_interventions(_base_community(), _neutral_env(), target="bnf", top_k=5)
        scores = [r["composite_score"] for r in results]
        assert scores == sorted(scores, reverse=True)

    def test_azospirillum_ranks_high_for_bnf_neutral(self):
        results = rank_interventions(_base_community(), _neutral_env(), target="bnf", top_k=8)
        names = [r["intervention_name"] for r in results]
        azo_rank = names.index("azospirillum_brasilense")
        no_till_rank = names.index("no_till")
        assert azo_rank < no_till_rank

    def test_ranking_changes_with_environment(self):
        """Different environments should produce different top interventions."""
        neutral = rank_interventions(_base_community(), _neutral_env(), target="bnf", top_k=3)
        acidic = rank_interventions(_base_community(), _acidic_dry_env(), target="bnf", top_k=3)
        neutral_names = [r["intervention_name"] for r in neutral]
        acidic_names = [r["intervention_name"] for r in acidic]
        # At least one intervention should be ranked differently
        assert neutral_names != acidic_names


class TestSimulateWithNamedIntervention:
    def test_returns_expected_keys(self):
        result = simulate_with_named_intervention(
            _base_community(), _neutral_env(), "azospirillum_brasilense", target="bnf",
        )
        expected = {"intervention_name", "display_name", "intervention_type",
                     "effective_strength", "target_flux", "stability_score",
                     "establishment_probability", "best_intervention_class"}
        assert set(result.keys()) >= expected

    def test_azospirillum_improves_bnf_flux(self):
        no_int = simulate_with_named_intervention(
            _base_community(), _neutral_env(), "no_till", target="bnf",
        )
        azo = simulate_with_named_intervention(
            _base_community(), _neutral_env(), "azospirillum_brasilense", target="bnf",
        )
        assert azo["target_flux"] > no_int["target_flux"]

    def test_cover_crop_boosts_diazotrophs_for_bnf(self):
        no_int = simulate_with_named_intervention(
            _base_community(), _neutral_env(), "no_till", target="bnf",
        )
        cover = simulate_with_named_intervention(
            _base_community(), _neutral_env(), "cover_crop_legume", target="bnf",
        )
        assert cover["target_flux"] > no_int["target_flux"]

    def test_bacillus_best_for_pathogen_suppression(self):
        """Bacillus boosts stress_tolerant_taxa, should rank high for pathogen suppression."""
        results = rank_interventions(_base_community(), _neutral_env(), target="pathogen_suppression", top_k=8)
        names = [r["intervention_name"] for r in results]
        bac_rank = names.index("bacillus_subtilis")
        azo_rank = names.index("azospirillum_brasilense")
        assert bac_rank < azo_rank