from __future__ import annotations

import math
from typing import Dict, Tuple

from .schema import Community, Environment, Intervention, SimulationResult


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

    biotic_potential = (
        0.25
        + 1.2 * c.diazotrophs
        + 0.35 * c.decomposers
        + 0.15 * c.stress_tolerant_taxa
        - 0.75 * c.competitors
    )
    biotic_potential = max(0.0, biotic_potential)

    inoc_compatibility = (ph_factor + moisture_factor + temperature_factor) / 3.0
    inoc_adjust = 0.55 * i.inoculation_strength * (inoc_compatibility - 0.35)

    amendment_need = 1.0 - om_support
    amendment_risk = max(0.0, e.moisture - 0.78) + max(0.0, e.temperature_c - 32.0) / 16.0
    amendment_adjust = 0.45 * i.amendment_strength * (amendment_need - 0.6 * amendment_risk)

    management_adjust_flux = 0.12 * i.management_shift * (c.stress_tolerant_taxa - 0.45 * c.competitors)
    flux_multiplier = _clamp(1.0 + inoc_adjust + amendment_adjust + management_adjust_flux, 0.2, 1.9)

    target_flux = max(0.0, 100.0 * env_factor * biotic_potential * flux_multiplier)

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
