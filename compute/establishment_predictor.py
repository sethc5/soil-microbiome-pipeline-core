"""
compute/establishment_predictor.py — T2 inoculant establishment probability model.

Predicts the probability that a proposed bioinoculant will persist in
the target community, based on:
  - Functional guild saturation (is the niche already occupied?)
  - Competitive exclusion index (abundance gradient)
  - pH and temperature tolerance match
  - Antibiotic susceptibility vs. community antibiotic production

Usage:
  from compute.establishment_predictor import predict_establishment
  prob = predict_establishment(inoculant_taxon, community_model, metadata)
"""

from __future__ import annotations
import logging
import math
from typing import Any

logger = logging.getLogger(__name__)

# Tolerance ranges for inoculant establishment
_PH_OPTIMAL_HALFWIDTH = 1.5
_TEMP_OPTIMAL_CENTER = 28.0  # °C
_TEMP_OPTIMAL_HALFWIDTH = 12.0

# Niche saturation threshold: if guild_occupancy > SATURATED, poor establishment
_NICHE_SATURATION_THRESHOLD = 0.8


def _score_ph_tolerance(soil_ph: float, inoculant_ph_range: tuple[float, float]) -> float:
    """Return 0–1 pH compatibility score."""
    lo, hi = inoculant_ph_range
    if lo <= soil_ph <= hi:
        return 1.0
    # Decay outside range
    distance = min(abs(soil_ph - lo), abs(soil_ph - hi))
    return float(max(0.0, 1.0 - distance / 2.0))


def _score_temperature(soil_temp_c: float | None) -> float:
    """Score temperature suitability (mesophile default center 28°C)."""
    if soil_temp_c is None:
        return 0.75  # unknown — assume moderate compatibility
    dist = abs(soil_temp_c - _TEMP_OPTIMAL_CENTER)
    return float(max(0.0, 1.0 - dist / _TEMP_OPTIMAL_HALFWIDTH))


def _score_niche_overlap(
    inoculant_taxon: dict,
    community_model: Any,
) -> tuple[float, float]:
    """Estimate niche overlap and competitive advantage.

    Returns (niche_overlap, competitive_advantage) both in [0, 1].
    Niche overlap: fraction of functional guild already occupied.
    Competitive advantage: 1 - overlap (simplified).
    """
    guild = inoculant_taxon.get("functional_guild", "general")

    if community_model is None:
        # No model → neutral estimates
        return 0.5, 0.5

    # Proxy: count reactions related to the guild
    guild_keywords = guild.upper().replace("_", " ").split()
    guild_rxn_count = 0
    total_rxn_count = len(getattr(community_model, "reactions", []))
    if total_rxn_count == 0:
        return 0.5, 0.5

    for rxn in getattr(community_model, "reactions", []):
        rxn_name = (rxn.name or rxn.id or "").upper()
        if any(kw in rxn_name for kw in guild_keywords):
            guild_rxn_count += 1

    guild_occupancy = guild_rxn_count / max(total_rxn_count, 1)
    niche_overlap = min(guild_occupancy * 5, 1.0)  # scale up (reactions are sparse proxy)
    competitive_advantage = 1.0 - niche_overlap
    return niche_overlap, competitive_advantage


def predict_establishment(
    inoculant_taxon: dict,
    community_model: Any,
    metadata: dict,
) -> float:
    """
    Return probability in [0, 1] that the inoculant establishes in this community.

    0 = certainly outcompeted, 1 = certainly establishes.

    Scores combined in a multiplicative framework:
      P(establish) = P(pH_ok) × P(temp_ok) × P(niche_available) × P(no_competitive_exclusion)
    """
    # pH compatibility
    soil_ph = float(metadata.get("soil_ph", 7.0))
    ph_range = inoculant_taxon.get("ph_range", (5.5, 8.5))
    ph_score = _score_ph_tolerance(soil_ph, ph_range)

    # Temperature
    soil_temp = metadata.get("soil_temp_c", None)
    temp_score = _score_temperature(soil_temp)

    # Niche overlap
    niche_overlap, competitive_advantage = _score_niche_overlap(inoculant_taxon, community_model)

    # Suppression by antibiotic producers (if metadata indicates)
    antibiotic_suppression = float(metadata.get("antibiotic_suppression_index", 0.0))
    antibiotic_score = 1.0 - min(antibiotic_suppression, 1.0)

    # Combined probability (multiplicative — conservative)
    establishment_prob = ph_score * temp_score * competitive_advantage * antibiotic_score

    # Hard cap based on niche saturation
    if niche_overlap >= _NICHE_SATURATION_THRESHOLD:
        establishment_prob *= 0.2  # severe penalty for saturated niche

    establishment_prob = float(max(0.0, min(establishment_prob, 1.0)))

    logger.debug(
        "Establishment prediction for %r: ph=%.2f, temp=%.2f, niche_avail=%.2f, "
        "antibiotic=%.2f → P=%.3f",
        inoculant_taxon.get("taxon_name", "unknown"),
        ph_score, temp_score, competitive_advantage, antibiotic_score,
        establishment_prob,
    )
    return establishment_prob


def predict_establishment_detailed(
    inoculant_taxon: dict,
    community_model: Any,
    metadata: dict,
) -> dict[str, Any]:
    """Return a detailed breakdown of the establishment prediction."""
    soil_ph = float(metadata.get("soil_ph", 7.0))
    ph_range = inoculant_taxon.get("ph_range", (5.5, 8.5))
    ph_score = _score_ph_tolerance(soil_ph, ph_range)
    soil_temp = metadata.get("soil_temp_c", None)
    temp_score = _score_temperature(soil_temp)
    niche_overlap, competitive_advantage = _score_niche_overlap(inoculant_taxon, community_model)
    antibiotic_suppression = float(metadata.get("antibiotic_suppression_index", 0.0))
    antibiotic_score = 1.0 - min(antibiotic_suppression, 1.0)
    prob = predict_establishment(inoculant_taxon, community_model, metadata)

    return {
        "establishment_prob": prob,
        "niche_overlap": niche_overlap,
        "competitive_advantage": competitive_advantage,
        "ph_compatibility": ph_score,
        "temperature_compatibility": temp_score,
        "antibiotic_compatibility": antibiotic_score,
        "limiting_resource": "niche" if niche_overlap > 0.5 else "pH" if ph_score < 0.5 else "none",
    }
