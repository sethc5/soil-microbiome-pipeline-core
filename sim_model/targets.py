"""Target configurations — parameterize dynamics for different applications."""

from __future__ import annotations

from typing import Any, Dict


BNF_DEFAULT: Dict[str, Any] = {
    "name": "bnf",
    "description": "Biological nitrogen fixation",
    "primary_guild": "diazotrophs",
    "secondary_guilds": {"decomposers": 0.35, "stress_tolerant_taxa": 0.15},
    "antagonist_guilds": {"competitors": 0.75},
    "base_biotic": 0.25,
    "optimal_ph": 6.8,
    "ph_sigma": 1.25,
    "optimal_moisture": 0.62,
    "moisture_sigma": 0.20,
    "optimal_temperature": 24.0,
    "temperature_sigma": 8.0,
    "flux_scale": 100.0,
    "primary_weight": 1.2,
}

CARBON_SEQUESTRATION: Dict[str, Any] = {
    "name": "carbon_sequestration",
    "description": "SOC accumulation — decomposer-driven",
    "primary_guild": "decomposers",
    "secondary_guilds": {"stress_tolerant_taxa": 0.40},
    "antagonist_guilds": {"competitors": 0.50},
    "base_biotic": 0.20,
    "optimal_ph": 6.2,
    "ph_sigma": 1.0,
    "optimal_moisture": 0.55,
    "moisture_sigma": 0.18,
    "optimal_temperature": 20.0,
    "temperature_sigma": 7.0,
    "flux_scale": 80.0,
    "primary_weight": 0.8,
}

PATHOGEN_SUPPRESSION: Dict[str, Any] = {
    "name": "pathogen_suppression",
    "description": "Disease suppressiveness — stress-tolerant biocontrol",
    "primary_guild": "stress_tolerant_taxa",
    "secondary_guilds": {"decomposers": 0.30, "diazotrophs": 0.10},
    "antagonist_guilds": {"competitors": 0.20},
    "base_biotic": 0.30,
    "optimal_ph": 6.5,
    "ph_sigma": 1.3,
    "optimal_moisture": 0.58,
    "moisture_sigma": 0.22,
    "optimal_temperature": 22.0,
    "temperature_sigma": 9.0,
    "flux_scale": 90.0,
    "primary_weight": 1.1,
}

PHOSPHORUS_SOLUBILIZATION: Dict[str, Any] = {
    "name": "phosphorus_solubilization",
    "description": "P solubilization — organic acid producers",
    "primary_guild": "decomposers",
    "secondary_guilds": {"diazotrophs": 0.20, "stress_tolerant_taxa": 0.10},
    "antagonist_guilds": {"competitors": 0.40},
    "base_biotic": 0.22,
    "optimal_ph": 6.0,
    "ph_sigma": 1.1,
    "optimal_moisture": 0.60,
    "moisture_sigma": 0.19,
    "optimal_temperature": 25.0,
    "temperature_sigma": 7.5,
    "flux_scale": 85.0,
    "primary_weight": 1.0,
}


TARGET_REGISTRY: Dict[str, Dict[str, Any]] = {
    "bnf": BNF_DEFAULT,
    "carbon_sequestration": CARBON_SEQUESTRATION,
    "pathogen_suppression": PATHOGEN_SUPPRESSION,
    "phosphorus_solubilization": PHOSPHORUS_SOLUBILIZATION,
}


def get_target(name: str) -> Dict[str, Any]:
    if name not in TARGET_REGISTRY:
        known = ", ".join(sorted(TARGET_REGISTRY.keys()))
        raise KeyError(f"Unknown target '{name}'. Known targets: {known}")
    return TARGET_REGISTRY[name]


def list_targets() -> Dict[str, str]:
    return {name: cfg["description"] for name, cfg in TARGET_REGISTRY.items()}