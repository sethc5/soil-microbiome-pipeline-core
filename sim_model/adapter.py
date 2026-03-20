from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping

from .dynamics import simulate_dynamics
from .schema import Community, Environment, Intervention


@dataclass(frozen=True)
class SimInputBundle:
    community: Community
    environment: Environment
    intervention: Intervention
    diagnostics: Dict[str, Any]


_COMMUNITY_DEFAULTS = {
    "diazotrophs": 0.35,
    "decomposers": 0.35,
    "competitors": 0.25,
    "stress_tolerant_taxa": 0.30,
}

_ENVIRONMENT_DEFAULTS = {
    "soil_ph": 6.5,
    "organic_matter_pct": 4.0,
    "moisture": 0.55,
    "temperature_c": 22.0,
}

_INTERVENTION_DEFAULTS = {
    "inoculation_strength": 0.0,
    "amendment_strength": 0.0,
    "management_shift": 0.0,
}

_COMMUNITY_ALIASES: Dict[str, List[str]] = {
    "diazotrophs": [
        "diazotrophs",
        "diazotroph_abundance",
        "diazotroph_rel_abundance",
        "nitrogen_fixer_abundance",
        "nifh",
        "nif_h",
    ],
    "decomposers": [
        "decomposers",
        "decomposer_abundance",
        "decomposition_guild_abundance",
        "saprotroph_abundance",
    ],
    "competitors": [
        "competitors",
        "competitor_abundance",
        "competitive_exclusion_index",
        "competition_index",
        "niche_overlap",
    ],
    "stress_tolerant_taxa": [
        "stress_tolerant_taxa",
        "stress_tolerant_abundance",
        "stress_tolerance_index",
        "dormancy_index",
    ],
}

_ENVIRONMENT_ALIASES: Dict[str, List[str]] = {
    "soil_ph": [
        "soil_ph",
        "ph",
        "ph_h2o",
        "ph_cacl2",
    ],
    "organic_matter_pct": [
        "organic_matter_pct",
        "organic_matter",
        "soil_organic_carbon",
        "organic_carbon",
        "soc",
        "toc",
    ],
    "moisture": [
        "moisture",
        "soil_moisture",
        "moisture_fraction",
        "moisture_pct",
        "soil_moisture_pct",
    ],
    "temperature_c": [
        "temperature_c",
        "soil_temp_c",
        "soil_temperature_c",
        "temperature",
    ],
}

_INTERVENTION_ALIASES: Dict[str, List[str]] = {
    "inoculation_strength": [
        "inoculation_strength",
        "inoculant_strength",
        "bioinoculant_strength",
    ],
    "amendment_strength": [
        "amendment_strength",
        "amendment_effect_strength",
    ],
    "management_shift": [
        "management_shift",
        "management_intensity_shift",
    ],
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalize_unit_interval(value: Any) -> float | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    if parsed > 1.0:
        parsed = parsed / 100.0
    return _clamp(parsed, 0.0, 1.0)


def _normalize_organic_matter_pct(value: Any) -> float | None:
    parsed = _to_float(value)
    if parsed is None:
        return None
    if 0.0 <= parsed <= 1.0:
        parsed *= 100.0
    return _clamp(parsed, 0.0, 20.0)


def _extract_case_insensitive(
    source: Mapping[str, Any],
    aliases: Iterable[str],
) -> tuple[float | None, str | None]:
    if not isinstance(source, Mapping):
        return None, None
    lowered = {str(key).lower(): key for key in source.keys()}
    for alias in aliases:
        key = lowered.get(alias.lower())
        if key is None:
            continue
        parsed = _to_float(source.get(key))
        if parsed is not None:
            return parsed, str(key)
    return None, None


def _extract_by_aliases(
    sources: List[Mapping[str, Any]],
    aliases: Iterable[str],
) -> tuple[float | None, str | None]:
    for source in sources:
        parsed, key = _extract_case_insensitive(source, aliases)
        if parsed is not None:
            return parsed, key
    return None, None


def _to_source_list(record: Mapping[str, Any], intervention_candidate: Mapping[str, Any] | None) -> List[Mapping[str, Any]]:
    sources: List[Mapping[str, Any]] = [record]
    for key in (
        "metadata",
        "normalised",
        "normalized",
        "features",
        "community",
        "community_profile",
        "functional_profile",
        "intervention",
    ):
        nested = record.get(key)
        if isinstance(nested, Mapping):
            sources.append(nested)
    if intervention_candidate is not None:
        sources.append(intervention_candidate)
    return sources


def _map_community(
    sources: List[Mapping[str, Any]],
    diagnostics: Dict[str, Any],
) -> Community:
    used_fields: Dict[str, str] = diagnostics.setdefault("used_fields", {})
    imputed: List[str] = diagnostics.setdefault("imputed_fields", [])

    out: Dict[str, float] = {}
    for target, aliases in _COMMUNITY_ALIASES.items():
        raw, key = _extract_by_aliases(sources, aliases)
        normalized = _normalize_unit_interval(raw) if raw is not None else None
        if normalized is None:
            out[target] = _COMMUNITY_DEFAULTS[target]
            imputed.append(target)
        else:
            out[target] = normalized
            if key:
                used_fields[target] = key
    return Community(**out)


def _map_environment(
    sources: List[Mapping[str, Any]],
    diagnostics: Dict[str, Any],
) -> Environment:
    used_fields: Dict[str, str] = diagnostics.setdefault("used_fields", {})
    imputed: List[str] = diagnostics.setdefault("imputed_fields", [])

    out: Dict[str, float] = {}
    for target, aliases in _ENVIRONMENT_ALIASES.items():
        raw, key = _extract_by_aliases(sources, aliases)
        if target == "soil_ph":
            normalized = _clamp(raw, 0.0, 14.0) if raw is not None else None
        elif target == "organic_matter_pct":
            normalized = _normalize_organic_matter_pct(raw) if raw is not None else None
        elif target == "moisture":
            normalized = _normalize_unit_interval(raw) if raw is not None else None
        else:
            normalized = raw

        if normalized is None:
            out[target] = _ENVIRONMENT_DEFAULTS[target]
            imputed.append(target)
        else:
            out[target] = normalized
            if key:
                used_fields[target] = key
    return Environment(**out)


def _derive_intervention_from_candidate(candidate: Mapping[str, Any]) -> Dict[str, float]:
    intervention_type = str(candidate.get("intervention_type", "")).strip().lower()
    predicted_effect = _normalize_unit_interval(candidate.get("predicted_effect"))
    if predicted_effect is None:
        predicted_effect = 0.0

    establishment_prob = _normalize_unit_interval(candidate.get("establishment_prob"))
    confidence = _normalize_unit_interval(candidate.get("confidence"))

    if intervention_type == "bioinoculant":
        if establishment_prob is not None:
            strength = predicted_effect * establishment_prob
        else:
            strength = predicted_effect
        return {"inoculation_strength": strength}

    if intervention_type == "amendment":
        rate = _to_float(candidate.get("rate_t_ha"))
        rate_strength = _clamp((rate or 0.0) / 5.0, 0.0, 1.0)
        return {"amendment_strength": max(predicted_effect, rate_strength)}

    if intervention_type == "management":
        base_shift = predicted_effect
        if confidence is not None:
            base_shift = base_shift * (0.5 + 0.5 * confidence)
        practice = str(candidate.get("practice", "")).lower()
        if "disturb" in practice or "intensive" in practice:
            base_shift = -base_shift
        return {"management_shift": _clamp(base_shift, -1.0, 1.0)}

    return {}


def _map_intervention(
    record_sources: List[Mapping[str, Any]],
    intervention_candidate: Mapping[str, Any] | None,
    diagnostics: Dict[str, Any],
) -> Intervention:
    used_fields: Dict[str, str] = diagnostics.setdefault("used_fields", {})
    imputed: List[str] = diagnostics.setdefault("imputed_fields", [])

    out: Dict[str, float] = dict(_INTERVENTION_DEFAULTS)
    for target, aliases in _INTERVENTION_ALIASES.items():
        raw, key = _extract_by_aliases(record_sources, aliases)
        normalized = _normalize_unit_interval(raw) if target != "management_shift" else _to_float(raw)
        if normalized is not None:
            out[target] = _clamp(normalized, -1.0 if target == "management_shift" else 0.0, 1.0)
            if key:
                used_fields[target] = key

    if intervention_candidate is not None:
        derived = _derive_intervention_from_candidate(intervention_candidate)
        for key, value in derived.items():
            if key in out and out[key] == _INTERVENTION_DEFAULTS[key]:
                out[key] = value
                used_fields[key] = "intervention_candidate"

    for target, default_value in _INTERVENTION_DEFAULTS.items():
        if out[target] == default_value:
            imputed.append(target)

    return Intervention(
        inoculation_strength=_clamp(out["inoculation_strength"], 0.0, 1.0),
        amendment_strength=_clamp(out["amendment_strength"], 0.0, 1.0),
        management_shift=_clamp(out["management_shift"], -1.0, 1.0),
    )


def map_pipeline_record_to_sim_inputs(
    record: Mapping[str, Any],
    intervention_candidate: Mapping[str, Any] | None = None,
) -> SimInputBundle:
    """
    Map pipeline-style metadata/features to sim model inputs.

    Returns Community + Environment + Intervention objects plus diagnostics about
    where values came from and which fields were imputed.
    """
    diagnostics: Dict[str, Any] = {"used_fields": {}, "imputed_fields": []}
    sources = _to_source_list(record, intervention_candidate)

    community = _map_community(sources, diagnostics)
    environment = _map_environment(sources, diagnostics)
    intervention = _map_intervention(sources, intervention_candidate, diagnostics)

    diagnostics["imputed_fields"] = sorted(set(diagnostics["imputed_fields"]))
    return SimInputBundle(
        community=community,
        environment=environment,
        intervention=intervention,
        diagnostics=diagnostics,
    )


def simulate_from_pipeline_record(
    record: Mapping[str, Any],
    intervention_candidate: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    bundle = map_pipeline_record_to_sim_inputs(
        record=record,
        intervention_candidate=intervention_candidate,
    )
    result = simulate_dynamics(
        community=bundle.community,
        environment=bundle.environment,
        intervention=bundle.intervention,
    )
    payload = result.to_dict()
    payload["inputs"] = {
        "community": bundle.community.__dict__,
        "environment": bundle.environment.__dict__,
        "intervention": bundle.intervention.__dict__,
    }
    payload["adapter_diagnostics"] = bundle.diagnostics
    return payload
