"""Specific intervention catalog — named organisms, amendments, and management practices."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from .dynamics import simulate_dynamics_with_target
from .schema import Community, Environment, Intervention


INTERVENTION_CATALOG: Dict[str, Dict[str, Any]] = {
    "azospirillum_brasilense": {
        "type": "inoculation",
        "display_name": "Azospirillum brasilense",
        "strength": 0.75,
        "ph_preference": 6.5,
        "moisture_floor": 0.40,
        "guild_boost": {"diazotrophs": 0.15},
        "competitor_resistance": 0.6,
    },
    "herbaspirillum_seropedicae": {
        "type": "inoculation",
        "display_name": "Herbaspirillum seropedicae",
        "strength": 0.70,
        "ph_preference": 6.2,
        "moisture_floor": 0.50,
        "guild_boost": {"diazotrophs": 0.20},
        "competitor_resistance": 0.4,
    },
    "rhizobium_leguminosarum": {
        "type": "inoculation",
        "display_name": "Rhizobium leguminosarum",
        "strength": 0.65,
        "ph_preference": 6.8,
        "moisture_floor": 0.45,
        "guild_boost": {"diazotrophs": 0.25},
        "competitor_resistance": 0.3,
    },
    "bacillus_subtilis": {
        "type": "inoculation",
        "display_name": "Bacillus subtilis",
        "strength": 0.55,
        "ph_preference": 6.5,
        "moisture_floor": 0.35,
        "guild_boost": {"stress_tolerant_taxa": 0.20},
        "competitor_resistance": 0.7,
    },
    "biochar_5t_ha": {
        "type": "amendment",
        "display_name": "Biochar 5 t/ha",
        "strength": 0.60,
        "ph_preference": None,
        "moisture_floor": None,
        "ph_shift": 0.8,
        "om_shift": 2.0,
        "moisture_shift": 0.05,
        "guild_boost": {},
        "competitor_resistance": 0.0,
    },
    "compost_10t_ha": {
        "type": "amendment",
        "display_name": "Compost 10 t/ha",
        "strength": 0.80,
        "ph_preference": None,
        "moisture_floor": None,
        "ph_shift": 0.2,
        "om_shift": 5.0,
        "moisture_shift": 0.08,
        "guild_boost": {},
        "competitor_resistance": 0.0,
    },
    "no_till": {
        "type": "management",
        "display_name": "No-till",
        "strength": 0.30,
        "ph_preference": None,
        "moisture_floor": None,
        "ph_shift": 0.0,
        "om_shift": 1.0,
        "moisture_shift": 0.03,
        "guild_boost": {},
        "competitor_resistance": 0.0,
        "management_shift": 0.4,
    },
    "cover_crop_legume": {
        "type": "management",
        "display_name": "Legume cover crop",
        "strength": 0.50,
        "ph_preference": None,
        "moisture_floor": None,
        "ph_shift": 0.0,
        "om_shift": 1.5,
        "moisture_shift": 0.02,
        "guild_boost": {"diazotrophs": 0.10},
        "competitor_resistance": 0.0,
        "management_shift": 0.3,
    },
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def get_intervention(name: str) -> Dict[str, Any]:
    if name not in INTERVENTION_CATALOG:
        known = ", ".join(sorted(INTERVENTION_CATALOG.keys()))
        raise KeyError(f"Unknown intervention '{name}'. Known: {known}")
    return INTERVENTION_CATALOG[name]


def list_interventions() -> Dict[str, str]:
    return {name: spec["display_name"] for name, spec in INTERVENTION_CATALOG.items()}


def apply_intervention(
    community: Community,
    environment: Environment,
    intervention_name: str,
) -> Tuple[Community, Environment, float]:
    """Apply a named intervention, returning modified community, environment, and effective strength.

    Effective strength accounts for environment compatibility (pH preference, moisture floor).
    """
    spec = get_intervention(intervention_name)

    # Environment modifications (amendments/management)
    new_ph = environment.soil_ph + spec.get("ph_shift", 0.0)
    new_om = environment.organic_matter_pct + spec.get("om_shift", 0.0)
    new_moisture = environment.moisture + spec.get("moisture_shift", 0.0)
    new_temp = environment.temperature_c

    modified_env = Environment(
        soil_ph=new_ph,
        organic_matter_pct=new_om,
        moisture=new_moisture,
        temperature_c=new_temp,
    ).clamped()

    # Community modifications (guild boosts)
    boosts = spec.get("guild_boost", {})
    new_diaz = community.diazotrophs + boosts.get("diazotrophs", 0.0)
    new_decomp = community.decomposers + boosts.get("decomposers", 0.0)
    new_comp = community.competitors + boosts.get("competitors", 0.0)
    new_stress = community.stress_tolerant_taxa + boosts.get("stress_tolerant_taxa", 0.0)

    modified_community = Community(
        diazotrophs=new_diaz,
        decomposers=new_decomp,
        competitors=new_comp,
        stress_tolerant_taxa=new_stress,
    ).clamped()

    # Effective strength based on environment compatibility
    base_strength = spec["strength"]

    ph_pref = spec.get("ph_preference")
    if ph_pref is not None:
        ph_compat = max(0.0, 1.0 - abs(modified_env.soil_ph - ph_pref) / 2.0)
    else:
        ph_compat = 1.0

    moisture_floor = spec.get("moisture_floor")
    if moisture_floor is not None:
        moisture_compat = _clamp((modified_env.moisture - moisture_floor + 0.1) / 0.3, 0.0, 1.0)
    else:
        moisture_compat = 1.0

    effective_strength = base_strength * ph_compat * moisture_compat

    return modified_community, modified_env, effective_strength


def simulate_with_named_intervention(
    community: Community,
    environment: Environment,
    intervention_name: str,
    target: str = "bnf",
) -> Dict[str, Any]:
    """Run simulation with a specific named intervention applied."""

    modified_c, modified_e, effective_strength = apply_intervention(
        community, environment, intervention_name,
    )

    # Build a generic Intervention from effective strength + type
    spec = get_intervention(intervention_name)
    if spec["type"] == "inoculation":
        intervention = Intervention(inoculation_strength=effective_strength, amendment_strength=0.0, management_shift=0.0)
    elif spec["type"] == "amendment":
        intervention = Intervention(inoculation_strength=0.0, amendment_strength=effective_strength, management_shift=0.0)
    else:
        mgmt = spec.get("management_shift", 0.0) * effective_strength
        intervention = Intervention(inoculation_strength=0.0, amendment_strength=0.0, management_shift=mgmt)

    result = simulate_dynamics_with_target(modified_c, modified_e, intervention, target=target)

    return {
        "intervention_name": intervention_name,
        "display_name": spec["display_name"],
        "intervention_type": spec["type"],
        "effective_strength": round(effective_strength, 4),
        "target_flux": result.target_flux,
        "stability_score": result.stability_score,
        "establishment_probability": result.establishment_probability,
        "best_intervention_class": result.best_intervention_class,
    }


def rank_interventions(
    community: Community,
    environment: Environment,
    target: str = "bnf",
    top_k: int = 3,
) -> List[Dict[str, Any]]:
    """Score all interventions in the catalog and return top_k ranked by composite score."""

    scored: List[Dict[str, Any]] = []
    for name in INTERVENTION_CATALOG:
        result = simulate_with_named_intervention(community, environment, name, target=target)
        composite = result["target_flux"] + 50.0 * result["stability_score"] + 30.0 * result["establishment_probability"]
        result["composite_score"] = round(composite, 4)
        scored.append(result)

    scored.sort(key=lambda r: r["composite_score"], reverse=True)
    return scored[:top_k]