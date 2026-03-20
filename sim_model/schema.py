from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class Community:
    diazotrophs: float
    decomposers: float
    competitors: float
    stress_tolerant_taxa: float

    def clamped(self) -> "Community":
        return Community(
            diazotrophs=_clamp(self.diazotrophs, 0.0, 1.0),
            decomposers=_clamp(self.decomposers, 0.0, 1.0),
            competitors=_clamp(self.competitors, 0.0, 1.0),
            stress_tolerant_taxa=_clamp(self.stress_tolerant_taxa, 0.0, 1.0),
        )


@dataclass(frozen=True)
class Environment:
    soil_ph: float
    organic_matter_pct: float
    moisture: float
    temperature_c: float

    def clamped(self) -> "Environment":
        return Environment(
            soil_ph=_clamp(self.soil_ph, 2.0, 10.0),
            organic_matter_pct=_clamp(self.organic_matter_pct, 0.0, 20.0),
            moisture=_clamp(self.moisture, 0.0, 1.0),
            temperature_c=_clamp(self.temperature_c, -5.0, 45.0),
        )


@dataclass(frozen=True)
class Intervention:
    inoculation_strength: float = 0.0
    amendment_strength: float = 0.0
    management_shift: float = 0.0

    def clamped(self) -> "Intervention":
        return Intervention(
            inoculation_strength=_clamp(self.inoculation_strength, 0.0, 1.0),
            amendment_strength=_clamp(self.amendment_strength, 0.0, 1.0),
            management_shift=_clamp(self.management_shift, -1.0, 1.0),
        )


@dataclass(frozen=True)
class SimulationResult:
    target_flux: float
    stability_score: float
    establishment_probability: float
    best_intervention_class: str
    diagnostics: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
