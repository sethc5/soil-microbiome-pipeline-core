from __future__ import annotations

import math
from typing import Any, Dict, Tuple

from .schema import Community, Environment, Intervention, SimulationResult
from .targets import BNF_DEFAULT, get_target


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _gaussian_response(value: float, center: float, sigma: float) -> float:
    return math.exp(-((value - center) ** 2) / (2.0 * sigma * sigma))


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def _evaluate_core(
    community: Community,
    environment: Environment,
    intervention: Intervention,
) -> Tuple[float, float, float, Dict[str, float]]:
    c = community.clamped()
    e = environment.clamped()
    i = intervention.clamped()

    ph_factor = _gaussian_response(e.soil_ph, center=6.8, sigma=1.25)
    moisture_factor = _gaussian_response(e.moisture, center=0.62, sigma=0.20)
    temperature_factor = _gaussian_response(e.temperature_c, center=24.0, sigma=8.0)

    om_support = e.organic_matter_pct / (e.organic_matter_pct + 3.5)
    om_excess_penalty = math.exp(-max(0.0, e.organic_matter_pct - 13.0) / 12.0)
    env_factor = ph_factor * moisture_factor * temperature_factor * om_support * om_excess_penalty

    # Guild budget: normalize abundances so they sum to 1.0
    guild_total = c.diazotrophs + c.decomposers + c.competitors + c.stress_tolerant_taxa
    if guild_total > 1e-9:
        d = c.diazotrophs / guild_total
        dec = c.decomposers / guild_total
        comp = c.competitors / guild_total
        st = c.stress_tolerant_taxa / guild_total
    else:
        d, dec, comp, st = 0.25, 0.25, 0.25, 0.25

    biotic_potential = (
        0.25
        + 1.2 * d
        + 0.35 * dec
        + 0.15 * st
        - 0.75 * comp
    )
    biotic_potential = max(0.0, biotic_potential)

    # Resource consumption: BNF activity consumes organic carbon
    # High diazotroph activity depletes OM, reducing future flux
    energy_available = om_support
    bnf_energy_cost = 0.20 * d  # diazotroph activity draws on OM
    effective_energy_ratio = max(0.0, 1.0 - bnf_energy_cost / max(energy_available, 0.01))
    biotic_potential *= (0.3 + 0.7 * effective_energy_ratio)  # floor at 30% to avoid zero

    inoc_compatibility = (ph_factor + moisture_factor + temperature_factor) / 3.0
    inoc_adjust = 0.55 * i.inoculation_strength * (inoc_compatibility - 0.35)

    amendment_need = 1.0 - om_support
    amendment_risk = max(0.0, e.moisture - 0.78) + max(0.0, e.temperature_c - 32.0) / 16.0
    amendment_adjust = 0.45 * i.amendment_strength * (amendment_need - 0.6 * amendment_risk)

    management_adjust_flux = 0.12 * i.management_shift * (c.stress_tolerant_taxa - 0.45 * c.competitors)
    flux_multiplier = _clamp(1.0 + inoc_adjust + amendment_adjust + management_adjust_flux, 0.2, 1.9)

    target_flux = max(0.0, 100.0 * env_factor * biotic_potential * flux_multiplier)

    # Negative feedback: product inhibition — accumulated product slows production
    # Real nitrogenase is inhibited by NH4+ accumulation (product inhibition)
    product_inhibition = 1.0 / (1.0 + target_flux / 40.0)
    target_flux *= product_inhibition

    env_stress = (
        (1.0 - ph_factor)
        + (abs(e.moisture - 0.62) / 0.62) * 0.45
        + (abs(e.temperature_c - 24.0) / 24.0) * 0.30
    )
    community_resilience = (
        0.40 + 0.75 * c.stress_tolerant_taxa + 0.25 * c.decomposers - 0.80 * c.competitors
    )
    management_adjust_stability = (
        0.35 * i.management_shift * (0.65 + c.stress_tolerant_taxa - c.competitors)
    )
    stability_linear = (
        0.80 * community_resilience
        + 0.35 * env_factor
        - 0.55 * env_stress
        + management_adjust_stability
    )
    stability_score = _clamp(_sigmoid(stability_linear), 0.0, 1.0)

    establishment_linear = (
        -0.20
        + 0.90 * c.diazotrophs
        + 0.45 * c.stress_tolerant_taxa
        - 0.95 * c.competitors
        + 0.55 * inoc_adjust
        + 0.25 * amendment_adjust
        - 0.40 * (1.0 - ph_factor)
    )
    establishment_probability = _clamp(_sigmoid(establishment_linear), 0.0, 1.0)

    diagnostics = {
        "ph_factor": ph_factor,
        "moisture_factor": moisture_factor,
        "temperature_factor": temperature_factor,
        "organic_matter_support": om_support,
        "environment_factor": env_factor,
        "biotic_potential": biotic_potential,
        "inoc_adjust": inoc_adjust,
        "amendment_adjust": amendment_adjust,
        "management_adjust_flux": management_adjust_flux,
        "flux_multiplier": flux_multiplier,
        "community_resilience": community_resilience,
        "environment_stress": env_stress,
    }

    return target_flux, stability_score, establishment_probability, diagnostics


def _best_intervention_class(community: Community, environment: Environment) -> str:
    intervention_classes = {
        "none": Intervention(0.0, 0.0, 0.0),
        "inoculation": Intervention(0.80, 0.0, 0.0),
        "amendment": Intervention(0.0, 0.80, 0.0),
        "management": Intervention(0.0, 0.0, 0.80),
    }
    best_name = "none"
    best_score = float("-inf")
    for name, candidate in intervention_classes.items():
        flux, stability, establishment, _ = _evaluate_core(community, environment, candidate)
        score = flux + 50.0 * stability + 30.0 * establishment
        if score > best_score:
            best_score = score
            best_name = name
    return best_name


def simulate_dynamics(
    community: Community,
    environment: Environment,
    intervention: Intervention,
) -> SimulationResult:
    target_flux, stability_score, establishment_probability, diagnostics = _evaluate_core(
        community=community,
        environment=environment,
        intervention=intervention,
    )
    best_intervention_class = _best_intervention_class(community, environment)
    return SimulationResult(
        target_flux=round(target_flux, 4),
        stability_score=round(stability_score, 4),
        establishment_probability=round(establishment_probability, 4),
        best_intervention_class=best_intervention_class,
        diagnostics={k: round(v, 6) for k, v in diagnostics.items()},
    )


# ---------------------------------------------------------------------------
# Perturbation presets
# ---------------------------------------------------------------------------

PERTURBATION_PRESETS: Dict[str, Dict[str, Any]] = {
    "drought": {
        "moisture_delta": -0.35,
        "temperature_delta": 4.0,
        "ph_delta": 0.0,
        "om_delta": 0.0,
    },
    "heat_wave": {
        "moisture_delta": -0.15,
        "temperature_delta": 10.0,
        "ph_delta": 0.0,
        "om_delta": 0.0,
    },
    "fertilizer_addition": {
        "moisture_delta": 0.0,
        "temperature_delta": 0.0,
        "ph_delta": -0.3,
        "om_delta": 1.5,
    },
    "acid_rain": {
        "moisture_delta": 0.05,
        "temperature_delta": 0.0,
        "ph_delta": -0.8,
        "om_delta": 0.0,
    },
}


def _apply_perturbation(
    environment: Environment,
    perturbation: Dict[str, Any],
) -> Environment:
    """Return a new Environment with perturbation deltas applied."""

    if "preset" in perturbation:
        preset = PERTURBATION_PRESETS.get(perturbation["preset"], {})
    else:
        preset = {}

    severity = perturbation.get("severity", 1.0)

    ph_delta = (perturbation.get("ph_delta", preset.get("ph_delta", 0.0))) * severity
    om_delta = (perturbation.get("om_delta", preset.get("om_delta", 0.0))) * severity
    moisture_delta = (perturbation.get("moisture_delta", preset.get("moisture_delta", 0.0))) * severity
    temp_delta = (perturbation.get("temperature_delta", preset.get("temperature_delta", 0.0))) * severity

    return Environment(
        soil_ph=environment.soil_ph + ph_delta,
        organic_matter_pct=environment.organic_matter_pct + om_delta,
        moisture=environment.moisture + moisture_delta,
        temperature_c=environment.temperature_c + temp_delta,
    ).clamped()


def simulate_with_perturbation(
    community: Community,
    environment: Environment,
    intervention: Intervention,
    perturbation: Dict[str, Any],
) -> Dict[str, Any]:
    """Run simulation before and after applying a perturbation.

    Returns baseline result, perturbed result, and resilience metrics.
    """

    baseline = simulate_dynamics(community, environment, intervention)
    perturbed_env = _apply_perturbation(environment, perturbation)
    perturbed = simulate_dynamics(community, perturbed_env, intervention)

    delta_flux = perturbed.target_flux - baseline.target_flux
    delta_stability = perturbed.stability_score - baseline.stability_score
    flux_resilience = (
        perturbed.target_flux / baseline.target_flux if baseline.target_flux > 1e-9 else 0.0
    )

    return {
        "baseline": baseline.to_dict(),
        "perturbed": perturbed.to_dict(),
        "perturbed_environment": {
            "soil_ph": round(perturbed_env.soil_ph, 4),
            "organic_matter_pct": round(perturbed_env.organic_matter_pct, 4),
            "moisture": round(perturbed_env.moisture, 4),
            "temperature_c": round(perturbed_env.temperature_c, 4),
        },
        "delta_flux": round(delta_flux, 4),
        "delta_stability": round(delta_stability, 4),
        "flux_resilience": round(flux_resilience, 4),
        "perturbation": perturbation,
    }


# ---------------------------------------------------------------------------
# Target-aware evaluation (Gap 2: Application switching)
# ---------------------------------------------------------------------------

def _guild_value(community: Community, guild_name: str) -> float:
    mapping = {
        "diazotrophs": community.diazotrophs,
        "decomposers": community.decomposers,
        "competitors": community.competitors,
        "stress_tolerant_taxa": community.stress_tolerant_taxa,
    }
    return mapping.get(guild_name, 0.0)


def _evaluate_core_with_target(
    community: Community,
    environment: Environment,
    intervention: Intervention,
    target: Dict[str, Any],
) -> Tuple[float, float, float, Dict[str, float]]:
    c = community.clamped()
    e = environment.clamped()
    i = intervention.clamped()

    ph_factor = _gaussian_response(e.soil_ph, center=target["optimal_ph"], sigma=target["ph_sigma"])
    moisture_factor = _gaussian_response(e.moisture, center=target["optimal_moisture"], sigma=target["moisture_sigma"])
    temperature_factor = _gaussian_response(e.temperature_c, center=target["optimal_temperature"], sigma=target["temperature_sigma"])

    om_support = e.organic_matter_pct / (e.organic_matter_pct + 3.5)
    om_excess_penalty = math.exp(-max(0.0, e.organic_matter_pct - 13.0) / 12.0)
    env_factor = ph_factor * moisture_factor * temperature_factor * om_support * om_excess_penalty

    # Guild budget: normalize
    guild_total = c.diazotrophs + c.decomposers + c.competitors + c.stress_tolerant_taxa
    if guild_total > 1e-9:
        cn = Community(
            diazotrophs=c.diazotrophs / guild_total,
            decomposers=c.decomposers / guild_total,
            competitors=c.competitors / guild_total,
            stress_tolerant_taxa=c.stress_tolerant_taxa / guild_total,
        )
    else:
        cn = Community(diazotrophs=0.25, decomposers=0.25, competitors=0.25, stress_tolerant_taxa=0.25)

    biotic_potential = target["base_biotic"] + target["primary_weight"] * _guild_value(cn, target["primary_guild"])
    for guild, weight in target["secondary_guilds"].items():
        biotic_potential += weight * _guild_value(cn, guild)
    for guild, weight in target["antagonist_guilds"].items():
        biotic_potential -= weight * _guild_value(cn, guild)
    biotic_potential = max(0.0, biotic_potential)

    # Resource consumption: primary guild activity draws on OM
    primary_frac = _guild_value(cn, target["primary_guild"])
    energy_available = om_support
    resource_cost = 0.20 * primary_frac
    effective_energy_ratio = max(0.0, 1.0 - resource_cost / max(energy_available, 0.01))
    biotic_potential *= (0.3 + 0.7 * effective_energy_ratio)

    inoc_compatibility = (ph_factor + moisture_factor + temperature_factor) / 3.0
    inoc_adjust = 0.55 * i.inoculation_strength * (inoc_compatibility - 0.35)

    amendment_need = 1.0 - om_support
    amendment_risk = max(0.0, e.moisture - 0.78) + max(0.0, e.temperature_c - 32.0) / 16.0
    amendment_adjust = 0.45 * i.amendment_strength * (amendment_need - 0.6 * amendment_risk)

    management_adjust_flux = 0.12 * i.management_shift * (c.stress_tolerant_taxa - 0.45 * c.competitors)
    flux_multiplier = _clamp(1.0 + inoc_adjust + amendment_adjust + management_adjust_flux, 0.2, 1.9)

    target_flux = max(0.0, target["flux_scale"] * env_factor * biotic_potential * flux_multiplier)

    # Negative feedback: product inhibition
    product_inhibition = 1.0 / (1.0 + target_flux / 40.0)
    target_flux *= product_inhibition

    env_stress = (
        (1.0 - ph_factor)
        + (abs(e.moisture - target["optimal_moisture"]) / max(target["optimal_moisture"], 0.01)) * 0.45
        + (abs(e.temperature_c - target["optimal_temperature"]) / max(target["optimal_temperature"], 0.01)) * 0.30
    )
    community_resilience = (
        0.40 + 0.75 * c.stress_tolerant_taxa + 0.25 * c.decomposers - 0.80 * c.competitors
    )
    management_adjust_stability = (
        0.35 * i.management_shift * (0.65 + c.stress_tolerant_taxa - c.competitors)
    )
    stability_linear = (
        0.80 * community_resilience
        + 0.35 * env_factor
        - 0.55 * env_stress
        + management_adjust_stability
    )
    stability_score = _clamp(_sigmoid(stability_linear), 0.0, 1.0)

    establishment_linear = (
        -0.20
        + 0.90 * _guild_value(c, target["primary_guild"])
        + 0.45 * c.stress_tolerant_taxa
        - 0.95 * c.competitors
        + 0.55 * inoc_adjust
        + 0.25 * amendment_adjust
        - 0.40 * (1.0 - ph_factor)
    )
    establishment_probability = _clamp(_sigmoid(establishment_linear), 0.0, 1.0)

    diagnostics = {
        "ph_factor": ph_factor,
        "moisture_factor": moisture_factor,
        "temperature_factor": temperature_factor,
        "organic_matter_support": om_support,
        "environment_factor": env_factor,
        "biotic_potential": biotic_potential,
        "inoc_adjust": inoc_adjust,
        "amendment_adjust": amendment_adjust,
        "management_adjust_flux": management_adjust_flux,
        "flux_multiplier": flux_multiplier,
        "community_resilience": community_resilience,
        "environment_stress": env_stress,
    }

    return target_flux, stability_score, establishment_probability, diagnostics


def simulate_dynamics_with_target(
    community: Community,
    environment: Environment,
    intervention: Intervention,
    target: str | Dict[str, Any] = "bnf",
) -> SimulationResult:
    target_cfg = get_target(target) if isinstance(target, str) else target

    target_flux, stability_score, establishment_probability, diagnostics = _evaluate_core_with_target(
        community=community,
        environment=environment,
        intervention=intervention,
        target=target_cfg,
    )

    intervention_classes = {
        "none": Intervention(0.0, 0.0, 0.0),
        "inoculation": Intervention(0.80, 0.0, 0.0),
        "amendment": Intervention(0.0, 0.80, 0.0),
        "management": Intervention(0.0, 0.0, 0.80),
    }
    best_name = "none"
    best_score = float("-inf")
    for name, candidate in intervention_classes.items():
        flux, stability, establishment, _ = _evaluate_core_with_target(
            community, environment, candidate, target_cfg,
        )
        score = flux + 50.0 * stability + 30.0 * establishment
        if score > best_score:
            best_score = score
            best_name = name

    return SimulationResult(
        target_flux=round(target_flux, 4),
        stability_score=round(stability_score, 4),
        establishment_probability=round(establishment_probability, 4),
        best_intervention_class=best_name,
        diagnostics={k: round(v, 6) if isinstance(v, float) else v for k, v in diagnostics.items()},
    )
