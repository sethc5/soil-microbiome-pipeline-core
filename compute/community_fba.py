"""
compute/community_fba.py — T1 COBRApy community flux balance analysis.

Combines individual organism SBML models into a community metabolic model
and runs FBA to predict flux through the target pathway.

Key steps:
  1. Merge member models via shared metabolite pools
  2. Apply environmental constraints from sample metadata (pH-adjusted bounds)
  3. Maximize community biomass as primary objective
  4. Record flux through target pathway reactions
  5. Run FVA to bound uncertainty on target flux prediction

See README: COBRApy community FBA is sensitive to biomass objective choice.
Document the chosen objective function clearly in the run record.

Usage:
  from compute.community_fba import run_community_fba
  result = run_community_fba(member_models, metadata, target_pathway="nifH_pathway")
"""

from __future__ import annotations
import logging
import re
import time
from typing import Any

logger = logging.getLogger(__name__)

# Reaction ID patterns that match target pathways.
# NOTE: nifH_pathway uses EX_nh4_e as an informational proxy — these models
# lack explicit nitrogenase reactions (rxn00006 maps to catalase in AGORA2-
# style genomes; N2 is not a metabolite). The actual T1 pass criterion uses
# the biomass objective value rather than any single reaction flux.
_PATHWAY_PATTERNS: dict[str, list[str]] = {
    "nifH_pathway": ["EX_NH4", "NH4", "AMMO"],  # NH4 exchange as N-fixation proxy
    "carbon_sequestration": ["RBC", "RUBISCO", "CARBONFIX"],
    "methane_production": ["MCR", "METHCOGEN", "rxn09173"],
    "hydrocarbon_degradation": ["ALKB", "ALMO", "rxn00541"],
    "denitrification": ["NITRRED", "NOS", "NOR", "rxn13825"],
    "nitrification": ["AMOO", "rxn00083"],
}

# pH-based environmental exchange bound modifiers
_PH_EXCHANGE_BOUNDS: list[tuple[float, float, float]] = [
    # (ph_lo, ph_hi, bound_multiplier)
    (0.0, 5.0, 0.3),
    (5.0, 5.5, 0.6),
    (5.5, 7.5, 1.0),
    (7.5, 8.5, 0.8),
    (8.5, 14.0, 0.4),
]


def _ph_multiplier(ph: float) -> float:
    for lo, hi, mult in _PH_EXCHANGE_BOUNDS:
        if lo <= ph < hi:
            return mult
    return 0.5


def _apply_environmental_constraints(model: Any, metadata: dict) -> None:
    """Apply pH and moisture-derived bounds to environmental exchange reactions."""
    try:
        ph = float(metadata.get("soil_ph", 7.0))
    except (TypeError, ValueError):
        ph = 7.0

    mult = _ph_multiplier(ph)

    # Scale uptake bounds for key exchange metabolites
    # Typical exchange reaction IDs in BIGG/CarveMe models:
    env_exchanges = ["EX_o2_e", "EX_glc__D_e", "EX_nh4_e", "EX_pi_e", "EX_so4_e"]
    for rxn_id in env_exchanges:
        try:
            rxn = model.reactions.get_by_id(rxn_id)
        except KeyError:
            continue
        if rxn.lower_bound < 0:
            rxn.lower_bound = rxn.lower_bound * mult


def _find_target_reactions(model: Any, target_pathway: str) -> list[Any]:
    """Find reactions in the model that match the target pathway."""
    patterns = _PATHWAY_PATTERNS.get(target_pathway, [target_pathway.upper()])
    matched = []
    for rxn in model.reactions:
        rxn_id_upper = rxn.id.upper()
        rxn_name_upper = (rxn.name or "").upper()
        if any(p in rxn_id_upper or p in rxn_name_upper for p in patterns):
            matched.append(rxn)
    return matched


def _is_exchange_rxn(rxn: Any) -> bool:
    """Return True if rxn is an exchange/boundary reaction (shared extracellular pool).

    Exchange reactions represent nutrient flux across the community boundary and
    must NOT be namespaced per organism — they are shared across all members.
    Criteria: BIGG 'EX_' prefix, or single-metabolite with extracellular compartment.
    """
    if rxn.id.startswith("EX_"):
        return True
    mets = list(rxn.metabolites.keys())
    if len(mets) == 1:
        compartment = getattr(mets[0], "compartment", "") or ""
        return compartment in ("e", "e0", "[e]", "extracellular")
    return False


def _merge_community_models(member_models: list, max_size: int) -> Any | None:
    """Merge member COBRApy models into a community model via compartment namespacing.

    Exchange reactions (EX_*) are shared across all organisms — they represent
    the common extracellular nutrient pool. Only intracellular reactions are
    suffixed per organism. This eliminates LP inflation and EX_* duplicate warnings.
    """
    try:
        import cobra
    except ImportError:
        logger.warning("cobra not installed — install via: pip install cobra")
        return None

    models = [m for m in member_models if m is not None][:max_size]
    if not models:
        return None

    if len(models) == 1:
        return models[0].copy()

    community = models[0].copy()
    community.id = "community_model"

    # Track exchange IDs already in the community (from org0).
    # These are shared — subsequent organisms should not re-add them.
    added_exchange_ids: set[str] = {
        rxn.id for rxn in community.reactions if _is_exchange_rxn(rxn)
    }

    for i, m in enumerate(models[1:], start=1):
        suffix = f"__org{i}"
        rxns_to_add = []
        for rxn in m.reactions:
            new_rxn = rxn.copy()
            if _is_exchange_rxn(rxn):
                # Shared extracellular pool: add once, skip duplicates silently
                if rxn.id not in added_exchange_ids:
                    added_exchange_ids.add(rxn.id)
                    rxns_to_add.append(new_rxn)
            else:
                # Intracellular: namespace per organism
                new_rxn.id = f"{rxn.id}{suffix}"
                rxns_to_add.append(new_rxn)
        community.add_reactions(rxns_to_add)

    logger.debug(
        "Community model: %d reactions (%d exchange shared, %d intracellular namespaced)",
        len(community.reactions),
        len(added_exchange_ids),
        len(community.reactions) - len(added_exchange_ids),
    )
    return community


def _extract_genome_quality_stats(member_models: list) -> dict[str, float]:
    """Aggregate genome quality metadata from model.notes across community members."""
    completeness_vals, contamination_vals, confidence_vals = [], [], []
    for m in member_models:
        if m is None:
            continue
        gq = (m.notes or {}).get("genome_quality", {})
        if gq:
            completeness_vals.append(gq.get("completeness", 0.0))
            contamination_vals.append(gq.get("contamination", 100.0))
            confidence_vals.append(gq.get("model_confidence", 0.35))
    return {
        "genome_completeness_mean": (
            sum(completeness_vals) / len(completeness_vals) if completeness_vals else 0.0
        ),
        "genome_contamination_mean": (
            sum(contamination_vals) / len(contamination_vals) if contamination_vals else 100.0
        ),
        "model_confidence": (
            sum(confidence_vals) / len(confidence_vals) if confidence_vals else 0.35
        ),
    }


def run_community_fba(
    member_models: list,  # list[cobra.Model]
    metadata: dict,
    target_pathway: str,
    max_community_size: int = 20,
    fva: bool = True,
) -> dict[str, Any]:
    """
    Run community FBA and return result dict.

    Returns keys:
      target_flux (float): Mean flux through target pathway reactions
      flux_units (str): "mmol/gDW/h" (COBRApy default)
      feasible (bool): Whether FBA optimal solution was found
      fva_min (float): FVA lower bound on target flux (if fva=True)
      fva_max (float): FVA upper bound on target flux (if fva=True)
      member_fluxes (dict): {model_id: target_flux} for each member
      model_confidence (float): Mean genome quality confidence [0, 1]
      genome_completeness_mean (float): Mean CheckM completeness %
      genome_contamination_mean (float): Mean CheckM contamination %
      walltime_s (float): Elapsed wall time
    """
    t_start = time.perf_counter()

    quality_stats = _extract_genome_quality_stats(member_models)

    try:
        import cobra
    except ImportError:
        logger.warning("cobra not installed — FBA skipped. pip install cobra")
        return {
            "target_flux": 0.0, "flux_units": "mmol/gDW/h", "feasible": False,
            "fva_min": 0.0, "fva_max": 0.0, "member_fluxes": {},
            **quality_stats, "walltime_s": time.perf_counter() - t_start,
        }

    community = _merge_community_models(member_models, max_community_size)
    if community is None:
        return {
            "target_flux": 0.0, "flux_units": "mmol/gDW/h", "feasible": False,
            "fva_min": 0.0, "fva_max": 0.0, "member_fluxes": {},
            **quality_stats, "walltime_s": time.perf_counter() - t_start,
        }

    # Apply environmental constraints
    _apply_environmental_constraints(community, metadata)

    # Solve community FBA
    solution = community.optimize()
    feasible = solution.status == "optimal"
    if not feasible:
        logger.warning("Community FBA infeasible for target_pathway=%s", target_pathway)
        return {
            "target_flux": 0.0, "flux_units": "mmol/gDW/h", "feasible": False,
            "fva_min": 0.0, "fva_max": 0.0, "member_fluxes": {},
            **quality_stats, "walltime_s": time.perf_counter() - t_start,
        }

    # Extract target pathway flux
    target_rxns = _find_target_reactions(community, target_pathway)
    if target_rxns:
        target_fluxes = [abs(solution.fluxes.get(rxn.id, 0.0)) for rxn in target_rxns]
        target_flux = sum(target_fluxes) / len(target_fluxes)
    else:
        logger.warning("No reactions found for target_pathway=%r", target_pathway)
        target_flux = 0.0

    # FVA bounds
    fva_min, fva_max = 0.0, 0.0
    if fva and target_rxns:
        try:
            fva_result = cobra.flux_analysis.flux_variability_analysis(
                community,
                reaction_list=target_rxns,
                fraction_of_optimum=0.9,
            )
            fva_min = float(fva_result["minimum"].mean())
            fva_max = float(fva_result["maximum"].mean())
        except Exception as exc:
            logger.debug("FVA failed: %s", exc)

    # Per-member flux contributions — look up suffixed reaction IDs in community solution
    member_fluxes: dict[str, float] = {}
    for i, m in enumerate(member_models):
        if m is None:
            continue
        m_rxns = _find_target_reactions(m, target_pathway)
        if m_rxns:
            suffix = f"__org{i}" if i > 0 else ""
            member_fluxes[m.id] = sum(
                solution.fluxes.get(f"{rxn.id}{suffix}", 0.0)
                for rxn in m_rxns
            )

    walltime_s = time.perf_counter() - t_start
    logger.info(
        "Community FBA: pathway=%s flux=%.4f feasible=%s wall=%.1fs",
        target_pathway, target_flux, feasible, walltime_s,
    )
    return {
        "target_flux": target_flux,
        "flux_units": "mmol/gDW/h",
        "feasible": feasible,
        "fva_min": fva_min,
        "fva_max": fva_max,
        "member_fluxes": member_fluxes,
        **quality_stats,
        "walltime_s": walltime_s,
    }
