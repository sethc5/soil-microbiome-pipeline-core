from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from .schema import Community, Environment, Intervention


@dataclass(frozen=True)
class Scenario:
    name: str
    community: Community
    environment: Environment
    intervention: Intervention
    note: str


def get_scenarios() -> Dict[str, Scenario]:
    scenarios: List[Scenario] = [
        Scenario(
            name="easy_win",
            community=Community(
                diazotrophs=0.62,
                decomposers=0.48,
                competitors=0.12,
                stress_tolerant_taxa=0.44,
            ),
            environment=Environment(
                soil_ph=6.8,
                organic_matter_pct=6.5,
                moisture=0.62,
                temperature_c=24.0,
            ),
            intervention=Intervention(
                inoculation_strength=0.35,
                amendment_strength=0.30,
                management_shift=0.20,
            ),
            note="High compatibility baseline that should be easy to rank.",
        ),
        Scenario(
            name="acidic_stress",
            community=Community(
                diazotrophs=0.46,
                decomposers=0.38,
                competitors=0.32,
                stress_tolerant_taxa=0.51,
            ),
            environment=Environment(
                soil_ph=4.4,
                organic_matter_pct=5.8,
                moisture=0.52,
                temperature_c=21.0,
            ),
            intervention=Intervention(
                inoculation_strength=0.70,
                amendment_strength=0.20,
                management_shift=0.35,
            ),
            note="Acidic conditions should suppress function and establishment.",
        ),
        Scenario(
            name="low_organic_cap",
            community=Community(
                diazotrophs=0.58,
                decomposers=0.29,
                competitors=0.18,
                stress_tolerant_taxa=0.36,
            ),
            environment=Environment(
                soil_ph=6.9,
                organic_matter_pct=0.8,
                moisture=0.60,
                temperature_c=24.0,
            ),
            intervention=Intervention(
                inoculation_strength=0.30,
                amendment_strength=0.80,
                management_shift=0.10,
            ),
            note="Low organic matter should cap achievable function.",
        ),
        Scenario(
            name="high_flux_low_stability",
            community=Community(
                diazotrophs=0.88,
                decomposers=0.52,
                competitors=0.68,
                stress_tolerant_taxa=0.08,
            ),
            environment=Environment(
                soil_ph=5.8,
                organic_matter_pct=10.0,
                moisture=0.38,
                temperature_c=31.0,
            ),
            intervention=Intervention(
                inoculation_strength=0.55,
                amendment_strength=0.15,
                management_shift=-0.90,
            ),
            note="Strong flux potential but unstable under perturbation pressure.",
        ),
        Scenario(
            name="ambiguous_tradeoff",
            community=Community(
                diazotrophs=0.43,
                decomposers=0.40,
                competitors=0.31,
                stress_tolerant_taxa=0.34,
            ),
            environment=Environment(
                soil_ph=6.1,
                organic_matter_pct=3.2,
                moisture=0.50,
                temperature_c=28.5,
            ),
            intervention=Intervention(
                inoculation_strength=0.55,
                amendment_strength=0.65,
                management_shift=0.05,
            ),
            note="Borderline case with mixed interventions and uncertain ranking.",
        ),
    ]
    return {scenario.name: scenario for scenario in scenarios}


def get_scenario(name: str) -> Scenario:
    scenarios = get_scenarios()
    if name not in scenarios:
        known = ", ".join(sorted(scenarios.keys()))
        raise KeyError(f"Unknown scenario '{name}'. Known scenarios: {known}")
    return scenarios[name]
