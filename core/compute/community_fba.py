"""
compute/community_fba.py — Generic T1 COBRApy community flux balance analysis.

Refactored to be application-agnostic. Constraints and target reactions
are provided via the 'Intent' plugin.
"""

from __future__ import annotations
import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# pH-based environmental exchange bound modifiers
_PH_EXCHANGE_BOUNDS: list[tuple[float, float, float]] = [
    (0.0, 5.0, 0.3),
    (5.0, 5.5, 0.6),
    (5.5, 7.5, 1.0),
    (7.5, 8.5, 0.8),
    (8.5, 14.0, 0.4),
]

def get_ph_multiplier(ph: float) -> float:
    for lo, hi, mult in _PH_EXCHANGE_BOUNDS:
        if lo <= ph < hi:
            return mult
    return 0.5

def apply_environmental_constraints(model: Any, metadata: Dict[str, Any]) -> None:
    """Apply pH-derived bounds to environmental exchange reactions."""
    ph = float(metadata.get("soil_ph", 7.0))
    mult = get_ph_multiplier(ph)

    env_exchanges = ["EX_o2_e", "EX_glc__D_e", "EX_nh4_e", "EX_pi_e", "EX_so4_e"]
    for rxn_id in env_exchanges:
        try:
            rxn = model.reactions.get_by_id(rxn_id)
            if rxn.lower_bound < 0:
                rxn.lower_bound = rxn.lower_bound * mult
        except KeyError:
            continue

def apply_medium_constraints(model: Any, constraints: Dict[str, Any]) -> None:
    """Apply application-specific medium constraints defined by an Intent."""
    if not constraints:
        return

    # 1. Global Reset (if specified)
    if constraints.get("medium_type") == "N-limited-minimal":
        for rxn in model.reactions:
            if rxn.id.startswith("EX_") and rxn.lower_bound < 0:
                rxn.lower_bound = 0.0

    # 2. Re-open Whitelist
    whitelist = constraints.get("inorganic_whitelist", [])
    for rxn_id in whitelist:
        try:
            rxn = model.reactions.get_by_id(rxn_id)
            rxn.lower_bound = -1000.0
        except KeyError:
            continue

    # 3. Open primary carbon source
    c_sources = constraints.get("preferred_carbon_sources", [])
    c_bound = constraints.get("carbon_uptake_bound", -10.0)
    for c_id in c_sources:
        try:
            rxn = model.reactions.get_by_id(c_id)
            rxn.lower_bound = c_bound
            break 
        except KeyError:
            continue

def _merge_community_models(member_models: list, max_size: int) -> Any | None:
    """
    Merge member COBRApy models into a community model via full compartment namespacing.
    Shared extracellular pool for EX_* reactions.
    """
    try:
        import cobra
    except ImportError:
        return None

    models = [m for m in member_models if m is not None][:max_size]
    if not models: return None
    if len(models) == 1: return models[0].copy()

    community = models[0].copy()
    community.id = "community_model"

    added_exchange_ids: set[str] = {
        rxn.id for rxn in community.reactions if rxn.id.startswith("EX_")
    }

    for i, m in enumerate(models[1:], start=1):
        suffix = f"__org{i}"
        rxns_to_add = []
        for rxn in m.reactions:
            if rxn.id.startswith("EX_"):
                if rxn.id not in added_exchange_ids:
                    added_exchange_ids.add(rxn.id)
                    rxns_to_add.append(rxn.copy())
                continue

            new_rxn = rxn.copy()
            new_rxn.id = f"{rxn.id}{suffix}"
            # Stoichiometry update for intracellular metabolites
            new_metabolites = {}
            for met, coeff in rxn.metabolites.items():
                if met.id.endswith("_e"):
                    # Shared extracellular metabolite
                    if community.metabolites.has_id(met.id):
                        new_metabolites[community.metabolites.get_by_id(met.id)] = coeff
                    else:
                        new_metabolites[met.copy()] = coeff
                else:
                    # Namespaced intracellular metabolite
                    new_met_id = f"{met.id}{suffix}"
                    if community.metabolites.has_id(new_met_id):
                        new_metabolites[community.metabolites.get_by_id(new_met_id)] = coeff
                    else:
                        new_met = met.copy()
                        new_met.id = new_met_id
                        new_metabolites[new_met] = coeff
            
            new_rxn.add_metabolites(new_metabolites, combine=False)
            rxns_to_add.append(new_rxn)
        
        community.add_reactions(rxns_to_add)

    return community

def run_community_fba(
    member_models: List[Any],
    metadata: Dict[str, Any],
    intent: Any,
    fva: bool = True,
    solver: str = None
) -> Dict[str, Any]:
    """
    Unified community FBA powered by Intent.
    
    Args:
        member_models: List of COBRApy models for community members
        metadata: Sample metadata (must include soil_ph)
        intent: Application intent defining constraints and targets
        fva: Whether to run flux variability analysis
        solver: Solver to use ('hybrid', 'glpk', 'scipy'). 
                If None, uses model default.
    """
    community = _merge_community_models(member_models, max_size=20)
    if community is None:
        return {"feasible": False, "error": "merge_failed"}

    # Set solver if specified
    if solver:
        community.solver = solver

    # Standard soil physics
    apply_environmental_constraints(community, metadata)

    # Application specific constraints
    constraints = intent.get_t1_constraints(metadata)
    apply_medium_constraints(community, constraints)

    solution = community.optimize()
    feasible = solution.status == "optimal"

    target_rxns = intent.get_t1_target_reactions(community)
    target_flux = 0.0
    if feasible and target_rxns:
        target_flux = sum(abs(solution.fluxes.get(r.id, 0.0)) for r in target_rxns) / len(target_rxns)

    return {
        "target_flux": target_flux,
        "feasible": feasible,
        "status": solution.status,
        "model_size": len(community.reactions)
    }
