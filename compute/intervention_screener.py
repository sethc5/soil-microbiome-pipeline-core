"""
compute/intervention_screener.py — T2 bioinoculant and amendment screening.

For each candidate intervention (bioinoculant organism, amendment type/rate,
management practice):
  1. Apply intervention to community model (add organism, adjust constraints)
  2. Run dFBA trajectory
  3. Measure: target flux improvement, establishment probability, off-target effects

Bioinoculant establishment uses competitive exclusion theory —
an inoculant establishes when it fills an unoccupied functional niche.

Usage:
  from compute.intervention_screener import screen_interventions
  results = screen_interventions(community_model, metadata, config["filters"]["t2"])
"""

from __future__ import annotations
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Default bioinoculant candidates to screen (overridden by config)
_DEFAULT_BIOINOCULANTS = [
    {"taxon_name": "Azospirillum brasilense", "functional_guild": "nitrogen_fixation",
     "ph_range": (5.5, 8.0), "cost_usd_per_ha": 25},
    {"taxon_name": "Bradyrhizobium japonicum", "functional_guild": "nitrogen_fixation",
     "ph_range": (5.5, 7.5), "cost_usd_per_ha": 30},
    {"taxon_name": "Pseudomonas fluorescens", "functional_guild": "phosphate_solubilization",
     "ph_range": (5.0, 8.5), "cost_usd_per_ha": 20},
    {"taxon_name": "Trichoderma harzianum", "functional_guild": "biocontrol",
     "ph_range": (4.5, 8.0), "cost_usd_per_ha": 50},
    {"taxon_name": "Bacillus subtilis", "functional_guild": "general_pgpr",
     "ph_range": (5.0, 8.5), "cost_usd_per_ha": 15},
]

# Default amendments to screen
_DEFAULT_AMENDMENTS = [
    {"type": "biochar", "rate_t_ha": 2.0},
    {"type": "compost", "rate_t_ha": 5.0},
    {"type": "lime", "rate_t_ha": 1.0},
    {"type": "rock_phosphate", "rate_t_ha": 0.5},
    {"type": "vermicompost", "rate_t_ha": 2.0},
]

# Management practice screeners
_DEFAULT_MANAGEMENT: list[dict] = [
    {"practice": "cover_cropping", "predicted_om_increase_pct": 0.3, "cost_usd_per_ha": 80},
    {"practice": "reduced_tillage", "predicted_om_increase_pct": 0.2, "cost_usd_per_ha": -30},
    {"practice": "irrigation_adjustment", "predicted_yield_mult": 1.1, "cost_usd_per_ha": 50},
]


def _screen_bioinoculants(
    community_model: Any,
    metadata: dict,
    t2_config: dict,
    t1_confidence: float = 0.5,
) -> list[dict]:
    """Screen bioinoculant candidates and return scored results."""
    from compute.establishment_predictor import predict_establishment_detailed

    candidates = t2_config.get("bioinoculants", _DEFAULT_BIOINOCULANTS)
    results = []
    for candidate in candidates:
        try:
            detail = predict_establishment_detailed(candidate, community_model, metadata)
        except Exception as exc:
            logger.debug("Establishment prediction failed for %s: %s", candidate.get("taxon_name"), exc)
            detail = {"establishment_prob": 0.0, "niche_overlap": 1.0, "competitive_advantage": 0.0}

        establishment_prob = detail.get("establishment_prob", 0.0)
        confidence = establishment_prob * t1_confidence

        results.append({
            "intervention_type": "bioinoculant",
            "intervention_detail": candidate.get("taxon_name", "unknown"),
            "taxon_name": candidate.get("taxon_name", "unknown"),
            "functional_guild": candidate.get("functional_guild", "unknown"),
            "predicted_effect": establishment_prob,
            "establishment_prob": establishment_prob,
            "confidence": confidence,
            "stability_under_perturbation": None,  # requires dFBA — set by caller
            "off_target_impact": 0.0,  # requires detailed simulation
            "cost_estimate": candidate.get("cost_usd_per_ha", 0),
            "establishment_detail": detail,
        })
    return results


def _screen_amendments(
    metadata: dict,
    t2_config: dict,
    t1_confidence: float = 0.5,
) -> list[dict]:
    """Screen soil amendment candidates and return scored results."""
    from compute.amendment_effect_model import compute_amendment_effect

    candidates = t2_config.get("amendments", _DEFAULT_AMENDMENTS)
    results = []
    for candidate in candidates:
        amendment_type = candidate.get("type", "compost")
        rate_t_ha = float(candidate.get("rate_t_ha", 1.0))
        try:
            effect = compute_amendment_effect(metadata, amendment_type, rate_t_ha)
        except Exception as exc:
            logger.debug("Amendment effect failed for %s: %s", amendment_type, exc)
            effect = {"predicted_ph_change": 0.0, "cost_estimate_usd_per_ha": 0.0}

        ph_change = abs(effect.get("predicted_ph_change", 0.0))
        n_change = abs(effect.get("predicted_n_change_ppm", 0.0))
        predicted_effect = min((ph_change * 0.3 + n_change * 0.01) * t1_confidence, 1.0)

        results.append({
            "intervention_type": "amendment",
            "intervention_detail": f"{amendment_type} @{rate_t_ha} t/ha",
            "amendment_type": amendment_type,
            "rate_t_ha": rate_t_ha,
            "predicted_effect": predicted_effect,
            "establishment_prob": 1.0,  # amendments always "establish"
            "confidence": t1_confidence * 0.7,  # amendments have lower mechanistic confidence
            "stability_under_perturbation": None,
            "off_target_impact": 0.0,
            "cost_estimate": effect.get("cost_estimate_usd_per_ha", 0.0),
            "amendment_effect": effect,
        })
    return results


def _screen_management(
    metadata: dict,
    t2_config: dict,
    t1_confidence: float = 0.5,
) -> list[dict]:
    """Screen management practices."""
    practices = t2_config.get("management_practices", _DEFAULT_MANAGEMENT)
    results = []
    for practice in practices:
        practice_name = practice.get("practice", "unknown")
        predicted_om = practice.get("predicted_om_increase_pct", 0.0)
        cost = practice.get("cost_usd_per_ha", 0)
        predicted_effect = min(predicted_om * t1_confidence * 0.5, 1.0)

        results.append({
            "intervention_type": "management",
            "intervention_detail": practice_name,
            "practice": practice_name,
            "predicted_effect": predicted_effect,
            "establishment_prob": 1.0,
            "confidence": t1_confidence * 0.5,
            "stability_under_perturbation": None,
            "off_target_impact": 0.0,
            "cost_estimate": cost,
        })
    return results


def screen_interventions(
    community_model: Any,
    metadata: dict,
    t2_config: dict,
    t1_model_confidence: float = 0.5,
    include_bioinoculants: bool = True,
    include_amendments: bool = True,
    include_management: bool = True,
) -> list[dict]:
    """
    Screen all configured interventions and return ranked list.

    Each result dict:
      intervention_type (str): 'bioinoculant', 'amendment', or 'management'
      intervention_detail (str): Name / description of the intervention
      predicted_effect (float): Predicted improvement in target function score [0, 1]
      confidence (float): Propagated confidence from T1 model × establishment probability
      stability_under_perturbation (float | None): Stability score (None if dFBA skipped)
      establishment_prob (float): For bioinoculants; 1.0 for amendments/management
      off_target_impact (float): Estimated negative side-effects [0, 1]
      cost_estimate (float): USD per hectare

    Results are sorted by (confidence × predicted_effect) descending.
    """
    all_results: list[dict] = []

    if include_bioinoculants:
        try:
            bio_results = _screen_bioinoculants(community_model, metadata, t2_config, t1_model_confidence)
            all_results.extend(bio_results)
            logger.info("Screened %d bioinoculant candidates", len(bio_results))
        except Exception as exc:
            logger.warning("Bioinoculant screening failed: %s", exc)

    if include_amendments:
        try:
            amend_results = _screen_amendments(metadata, t2_config, t1_model_confidence)
            all_results.extend(amend_results)
            logger.info("Screened %d amendment candidates", len(amend_results))
        except Exception as exc:
            logger.warning("Amendment screening failed: %s", exc)

    if include_management:
        try:
            mgmt_results = _screen_management(metadata, t2_config, t1_model_confidence)
            all_results.extend(mgmt_results)
            logger.info("Screened %d management practices", len(mgmt_results))
        except Exception as exc:
            logger.warning("Management screening failed: %s", exc)

    # Rank by confidence × predicted_effect
    all_results.sort(
        key=lambda r: r.get("confidence", 0.0) * r.get("predicted_effect", 0.0),
        reverse=True,
    )

    logger.info(
        "Intervention screening complete: %d interventions ranked", len(all_results)
    )
    return all_results
