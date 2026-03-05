"""
compute/amendment_effect_model.py — T2 biochar/compost/amendment effect modeling.

Translates physical amendment properties to soil parameter changes
that are used to adjust environmental constraints in T1/T2 FBA:

  Biochar:
    - pH increase:  +0.5 to +1.5 pH units (feedstock and temperature dependent)
    - Moisture:     +5-15% water holding capacity
    - Bulk density: -0.1 to -0.3 g/cm³
    - CEC:          +2-5 cmol/kg

  Compost:
    - Organic matter: +0.5-2% per t/ha
    - Available N:    +10-30 ppm per t/ha
    - pH:             small buffering effect

Use conservative estimates — amendment effects are heterogeneous (see README gotchas).

Usage:
  from compute.amendment_effect_model import compute_amendment_effect
  new_metadata = compute_amendment_effect(metadata, amendment_type="biochar", rate_t_ha=2)
"""

from __future__ import annotations
import logging
from typing import Any

logger = logging.getLogger(__name__)

AMENDMENT_DEFAULTS: dict[str, dict[str, Any]] = {
    "biochar": {
        "ph_delta_low": 0.5,
        "ph_delta_high": 1.5,
        "ph_delta_conservative": 0.6,
        "moisture_delta_pct": 10.0,
        "bulk_density_delta": -0.15,
        "cec_delta": 3.0,
        "description": "Pyrolyzed organic matter — raises pH, improves WHC, increases CEC",
        "cost_usd_per_t": 300,
    },
    "compost": {
        "organic_matter_delta_per_t_ha": 0.8,
        "available_n_delta_ppm_per_t_ha": 15.0,
        "ph_buffer_delta": 0.1,
        "description": "Decomposed organic matter — boosts OM, N availability, buffers pH",
        "cost_usd_per_t": 50,
    },
    "lime": {
        "ph_delta_per_t_ha": 0.5,
        "ph_delta_conservative": 0.4,
        "ca_delta_cmol_per_t_ha": 10.0,
        "description": "Agricultural lime (CaCO3) — raises pH in acidic soils",
        "cost_usd_per_t": 40,
    },
    "sulfur": {
        "ph_delta_per_t_ha": -0.3,
        "description": "Elemental sulfur — lowers pH in alkaline soils",
        "cost_usd_per_t": 80,
    },
    "rock_phosphate": {
        "available_p_delta_ppm_per_t_ha": 5.0,
        "ph_delta_per_t_ha": -0.05,
        "description": "Slow-release P source — raises plant-available P",
        "cost_usd_per_t": 200,
    },
    "vermicompost": {
        "organic_matter_delta_per_t_ha": 1.2,
        "available_n_delta_ppm_per_t_ha": 25.0,
        "microbial_activity_boost_factor": 1.3,
        "description": "Worm castings — high microbial activity, concentrated nutrients",
        "cost_usd_per_t": 400,
    },
}


def compute_amendment_effect(
    metadata: dict,
    amendment_type: str,
    rate_t_ha: float,
    use_conservative: bool = True,
    run_fba: bool = False,
    community_model: Any = None,
) -> dict[str, Any]:
    """
    Return updated metadata dict with amendment effects applied.

    Uses conservative estimates by default — see README gotchas:
    amendment effects are heterogeneous (± 50% in field conditions).

    Keys returned:
      updated_metadata (dict): metadata with amendment effects applied
      amendment_type (str): amendment applied
      rate_t_ha (float): application rate
      predicted_ph_change (float): pH change
      predicted_n_change_ppm (float): available N change
      cost_estimate_usd_per_ha (float): estimated cost
      caveats (list[str]): known uncertainty sources

    If run_fba=True and community_model is provided, also runs a quick
    FBA with updated constraints (experimental).
    """
    if amendment_type not in AMENDMENT_DEFAULTS:
        raise ValueError(
            f"Unknown amendment_type {amendment_type!r}. "
            f"Available: {list(AMENDMENT_DEFAULTS)}"
        )

    defaults = AMENDMENT_DEFAULTS[amendment_type]
    updated = dict(metadata)  # copy
    caveats: list[str] = []

    predicted_ph_change = 0.0
    predicted_n_change = 0.0
    cost_per_ha = 0.0

    if amendment_type == "biochar":
        ph_delta = (
            defaults["ph_delta_conservative"]
            if use_conservative
            else (defaults["ph_delta_low"] + defaults["ph_delta_high"]) / 2
        )
        predicted_ph_change = ph_delta  # per tonne; does not scale linearly with rate
        ph_change = ph_delta * min(rate_t_ha / 5.0, 1.5)  # diminishing returns above 5 t/ha
        old_ph = float(updated.get("soil_ph", 7.0))
        updated["soil_ph"] = round(min(old_ph + ph_change, 9.0), 2)
        updated["moisture_pct"] = float(updated.get("moisture_pct", 20.0)) + defaults["moisture_delta_pct"]
        cost_per_ha = defaults["cost_usd_per_t"] * rate_t_ha
        caveats.append("pH effect depends heavily on biochar feedstock and pyrolysis temperature")
        caveats.append("Moisture increase assumes uniform incorporation — field heterogeneity ±50%")

    elif amendment_type == "compost":
        om_delta = defaults["organic_matter_delta_per_t_ha"] * rate_t_ha
        n_delta = defaults["available_n_delta_ppm_per_t_ha"] * rate_t_ha
        updated["organic_matter_pct"] = float(updated.get("organic_matter_pct", 2.0)) + om_delta
        updated["available_n_ppm"] = float(updated.get("available_n_ppm", 20.0)) + n_delta
        predicted_ph_change = defaults["ph_buffer_delta"] * rate_t_ha * 0.1  # small buffering
        predicted_n_change = n_delta
        cost_per_ha = defaults["cost_usd_per_t"] * rate_t_ha
        caveats.append("Compost N availability varies by maturity and C:N ratio")

    elif amendment_type == "lime":
        ph_delta_per_t = defaults.get("ph_delta_per_t_ha", 0.5) if not use_conservative else defaults.get("ph_delta_conservative", 0.4)
        ph_change = ph_delta_per_t * rate_t_ha
        old_ph = float(updated.get("soil_ph", 7.0))
        updated["soil_ph"] = round(min(old_ph + ph_change, 8.5), 2)
        predicted_ph_change = ph_change
        cost_per_ha = defaults["cost_usd_per_t"] * rate_t_ha
        caveats.append("Lime reaction rate depends on fineness and soil moisture")

    elif amendment_type == "sulfur":
        ph_delta = defaults["ph_delta_per_t_ha"] * rate_t_ha
        old_ph = float(updated.get("soil_ph", 7.0))
        updated["soil_ph"] = round(max(old_ph + ph_delta, 4.0), 2)
        predicted_ph_change = ph_delta
        cost_per_ha = defaults["cost_usd_per_t"] * rate_t_ha
        caveats.append("Sulfur acidification requires active sulfur-oxidizing bacteria")

    elif amendment_type == "rock_phosphate":
        p_delta = defaults["available_p_delta_ppm_per_t_ha"] * rate_t_ha
        updated["available_p_ppm"] = float(updated.get("available_p_ppm", 10.0)) + p_delta
        cost_per_ha = defaults["cost_usd_per_t"] * rate_t_ha
        caveats.append("Rock phosphate dissolution is slow in high-pH soils")

    elif amendment_type == "vermicompost":
        om_delta = defaults["organic_matter_delta_per_t_ha"] * rate_t_ha
        n_delta = defaults["available_n_delta_ppm_per_t_ha"] * rate_t_ha
        updated["organic_matter_pct"] = float(updated.get("organic_matter_pct", 2.0)) + om_delta
        updated["available_n_ppm"] = float(updated.get("available_n_ppm", 20.0)) + n_delta
        predicted_n_change = n_delta
        cost_per_ha = defaults["cost_usd_per_t"] * rate_t_ha
        caveats.append("Vermicompost microbial effects depend on application timing")

    # Optional quick FBA rerun
    fba_result: dict[str, Any] = {}
    if run_fba and community_model is not None:
        from compute.community_fba import run_community_fba
        try:
            fba_result = run_community_fba(
                [community_model], updated,
                target_pathway="nifH_pathway",
                fva=False,
            )
        except Exception as exc:
            logger.debug("FBA rerun after amendment failed: %s", exc)

    logger.info(
        "Amendment effect: %s @%.1f t/ha → pH%.2f, N+%.1f ppm, cost $%.0f/ha",
        amendment_type, rate_t_ha, predicted_ph_change, predicted_n_change, cost_per_ha,
    )
    return {
        "updated_metadata": updated,
        "amendment_type": amendment_type,
        "rate_t_ha": rate_t_ha,
        "predicted_ph_change": predicted_ph_change,
        "predicted_n_change_ppm": predicted_n_change,
        "cost_estimate_usd_per_ha": cost_per_ha,
        "caveats": caveats,
        "fba_result": fba_result,
    }
