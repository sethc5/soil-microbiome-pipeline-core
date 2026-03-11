"""
compute/metadata_validator.py -- T0 soil metadata parsing and ENVO standardization.

Handles:
  - Parsing pH from heterogeneous SRA field names and formats
  - Koppen-Geiger climate zone lookup from lat/lon
  - ENVO biome / feature / material term normalization
  - Soil texture class assignment from sand/silt/clay percentages (USDA triangle)
  - Missing-value imputation flags (never silently impute load-bearing fields)

Usage:
  from core.compute.metadata_validator import validate_sample_metadata
  result = validate_sample_metadata(raw_metadata_dict, config_t0)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# SRA uses many synonyms for the same field -- normalize them
PH_FIELD_ALIASES = ["ph", "soil_ph", "acidity", "reaction", "ph_value", "pH"]

_PH_PLAUSIBLE_MIN = 2.0
_PH_PLAUSIBLE_MAX = 11.0


# ---------------------------------------------------------------------------
# USDA texture triangle (pure Python)
# Rules follow USDA NRCS Soil Survey Manual (2017).
# ---------------------------------------------------------------------------

def texture_class_from_fractions(
    sand_pct: float | None,
    silt_pct: float | None,
    clay_pct: float | None,
) -> str | None:
    """
    Return USDA soil texture class from particle size fractions (%).
    Returns None when any input is missing or fractions sum far from 100.
    """
    if sand_pct is None or silt_pct is None or clay_pct is None:
        return None
    total = sand_pct + silt_pct + clay_pct
    if abs(total - 100) > 5:
        logger.debug("Texture fractions sum to %.1f -- cannot classify", total)
        return None
    f = 100 / total
    s, si, c = sand_pct * f, silt_pct * f, clay_pct * f

    if s >= 85 and c <= 10:
        return "sand"
    if s >= 70 and c <= 15:
        return "loamy sand"
    if s >= 52 and c < 20:
        return "sandy loam"
    if c >= 40 and s >= 45:
        return "sandy clay"
    if c >= 40 and si >= 40:
        return "silty clay"
    if c >= 40:
        return "clay"
    if c >= 27 and s < 20:
        return "silty clay loam"
    if c >= 27 and s < 45:
        return "clay loam"
    if c >= 20 and s >= 45:
        return "sandy clay loam"
    if si >= 80:
        return "silt"
    if si >= 50:
        return "silt loam"
    if s >= 23 and c < 28:
        return "loam"
    return "loam"  # centre-of-triangle fallback


# ---------------------------------------------------------------------------
# Koppen-Geiger climate zone lookup
# ---------------------------------------------------------------------------

def climate_zone_from_coords(lat: float | None, lon: float | None) -> str | None:
    """
    Return approximate Koppen-Geiger 3-letter climate code for lat/lon.

    Tries the `climate_zones` package first; falls back to a simplified
    latitude-band heuristic that is accurate enough for coarse filtering.
    """
    if lat is None or lon is None:
        return None

    try:
        from climate_zones import koppen_geiger  # type: ignore
        return koppen_geiger(lat, lon)
    except ImportError:
        pass

    abs_lat = abs(lat)
    if abs_lat >= 70:
        return "ET"
    if abs_lat >= 55:
        return "Dfc"
    if abs_lat >= 40:
        if (-130 <= lon <= -60) or (0 <= lon <= 30):
            return "Dfb"
        return "Dfa"
    if abs_lat >= 30:
        if -70 <= lon <= -30:
            return "Cfa"
        if lon >= 100:
            return "Cfa"
        if -30 <= lon <= 60:
            return "Csa"
        return "BSk"
    if abs_lat >= 10:
        return "Aw"
    return "Af"


# ---------------------------------------------------------------------------
# validate_sample_metadata
# ---------------------------------------------------------------------------

def validate_sample_metadata(
    raw: dict,
    config_t0=None,
) -> dict:
    """
    Validate and normalise raw metadata, delegating parsing to MetadataNormalizer.

    Parameters
    ----------
    raw       : raw metadata dict from any source adapter.
    config_t0 : optional T0Filters instance for threshold gates.

    Returns
    -------
    dict:
      passed           bool
      reject_reasons   list[str]
      warnings         list[str]
      normalized       dict -- canonical fields after normalisation
      missing_fields   list[str]
      texture_class    str | None -- USDA class derived from fractions
    """
    from core.compute.metadata_normalizer import MetadataNormalizer

    norm = MetadataNormalizer()
    normalized: dict[str, Any] = norm.normalize_sample(
        raw, source=raw.get("source", "unknown")
    )

    reject_reasons: list[str] = []
    warnings: list[str] = []
    missing_fields: list[str] = []

    # --- pH ---
    ph = normalized.get("soil_ph")
    if ph is None:
        missing_fields.append("soil_ph")
    elif not (_PH_PLAUSIBLE_MIN <= ph <= _PH_PLAUSIBLE_MAX):
        reject_reasons.append(
            f"soil_ph {ph} outside plausible range "
            f"[{_PH_PLAUSIBLE_MIN},{_PH_PLAUSIBLE_MAX}]"
        )
        normalized["soil_ph"] = None

    if config_t0 and ph is not None:
        lo, hi = config_t0.ph_range[0], config_t0.ph_range[1]
        if not (lo <= ph <= hi):
            reject_reasons.append(
                f"soil_ph {ph:.1f} outside target range [{lo},{hi}]"
            )

    # --- Coordinates ---
    lat = normalized.get("latitude")
    lon = normalized.get("longitude")
    if lat is None:
        missing_fields.append("latitude")
    if lon is None:
        missing_fields.append("longitude")

    # --- Derive climate zone from coords if missing ---
    if not normalized.get("climate_zone") and lat is not None and lon is not None:
        try:
            normalized["climate_zone"] = climate_zone_from_coords(lat, lon)
        except Exception as exc:
            warnings.append(f"climate zone lookup failed: {exc}")

    # --- Texture: derive from fractions if missing ---
    derived_tex: str | None = None
    if not normalized.get("soil_texture"):
        derived_tex = texture_class_from_fractions(
            normalized.get("sand_pct"),
            normalized.get("silt_pct"),
            normalized.get("clay_pct"),
        )
        if derived_tex:
            normalized["soil_texture"] = derived_tex
        else:
            missing_fields.append("soil_texture")

    # --- Sampling fraction gate ---
    raw_frac = normalized.get("sampling_fraction") or raw.get("sampling_fraction")
    if raw_frac is not None:
        try:
            frac_val = float(raw_frac)
            if frac_val > 1.0:
                reject_reasons.append(
                    f"sampling_fraction {frac_val} > 1.0 (must be 0-1)"
                )
        except (TypeError, ValueError):
            pass
    if config_t0 and getattr(config_t0, "required_sampling_fraction", None):
        frac = normalized.get("sampling_fraction")
        allowed = config_t0.required_sampling_fraction
        if frac and frac not in allowed:
            reject_reasons.append(
                f"sampling_fraction '{frac}' not in required {allowed}"
            )

    # --- ITS data gate ---
    if config_t0 and getattr(config_t0, "required_its_data", False):
        has_its = (
            raw.get("its_profile")
            or normalized.get("sequencing_type") == "ITS"
        )
        if not has_its:
            reject_reasons.append(
                "required_its_data=True but no ITS profile available"
            )

    # --- Fungal:bacterial ratio gate ---
    if config_t0 and getattr(config_t0, "min_fungal_bacterial_ratio", None):
        fb = raw.get("fungal_bacterial_ratio") or normalized.get("fungal_bacterial_ratio")
        if fb is not None and fb < config_t0.min_fungal_bacterial_ratio:
            reject_reasons.append(
                f"fungal_bacterial_ratio {fb} < minimum "
                f"{config_t0.min_fungal_bacterial_ratio}"
            )

    return {
        "passed":         len(reject_reasons) == 0,
        "reject_reasons": reject_reasons,
        "warnings":       warnings,
        "normalized":     normalized,
        "missing_fields": missing_fields,
        "texture_class":  derived_tex,
    }
