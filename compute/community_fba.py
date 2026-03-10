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



# ── BNF minimal medium ────────────────────────────────────────────────────────
# AGORA2-style models ship with a "complete medium" where every possible
# metabolite exchange is open at ±1000 mmol/gDW/h.  Running FVA on
# NITROGENASE_MO in that context gives ATP-saturated values that far exceed
# any biological rate.  For BNF mode we need a soil-relevant N-limited medium:
#   • Close ALL EX_* uptake fluxes (lb → 0).
#   • Reopen safe inorganic exchanges (water, protons, O2, Pi, SO4, trace
#     metals, molybdate — the MoFe nitrogenase cofactor requirement).
#   • Reopen one primary carbon source at ≤10 mmol/gDW/h (glucose preferred).
#   • Keep ammonium AND all other N sources CLOSED → forces N2 as sole N supply.
#   • The calling code (t1_fba_batch.py) handles EX_n2_e separately.
#
# Under this minimal medium NITROGENASE_MO rates are bounded by the carbon/ATP
# budget, giving values comparable to Reed 2011 (0.01-5 mmol NH4-equiv/gDW/h).

# EX_* IDs that are inorganic / non-N-source and should remain open.
# Molybdate (mobd) is listed explicitly — it is the cofactor for MoFe
# nitrogenase and must be available for the reaction to carry flux.
_BNF_INORGANIC_EXCHANGES: frozenset[str] = frozenset({
    "EX_h2o_e",      # water
    "EX_h_e",        # protons
    "EX_co2_e",      # CO2 (product of C catabolism)
    "EX_hco3_e",     # bicarbonate
    "EX_o2_e",       # O2 (aerobic zones; lb bounded by env constraints)
    "EX_pi_e",       # phosphate (P source)
    "EX_ppi_e",      # pyrophosphate
    "EX_so4_e",      # sulfate (S source)
    "EX_h2s_e",      # hydrogen sulfide (alt S source)
    "EX_fe2_e",      # Fe²⁺
    "EX_fe3_e",      # Fe³⁺
    "EX_mg2_e",      # Mg²⁺ (Mg-ATP cofactor)
    "EX_k_e",        # K⁺
    "EX_na1_e",      # Na⁺
    "EX_ca2_e",      # Ca²⁺
    "EX_zn2_e",      # Zn²⁺
    "EX_mn2_e",      # Mn²⁺
    "EX_cu2_e",      # Cu²⁺
    "EX_mobd_e",     # molybdate — MoFe nitrogenase cofactor (critical!)
    "EX_cobalt2_e",  # Co²⁺
    "EX_cl_e",       # Cl⁻
    "EX_sel_e",      # selenium (selenocysteine)
    "EX_ni2_e",      # Ni²⁺
    "EX_n2_e",       # N₂ (opened separately in t1_fba_batch.py)
})

# Organic cofactors / vitamins that contain carbon but are needed in trace amounts.
# These are kept separate from _BNF_INORGANIC_EXCHANGES so they can be reopened
# at a tiny biological uptake rate (-0.001 mmol/gDW/h) rather than the AGORA2
# complete-medium value (-1000 mmol/gDW/h).  At -1000 the LP catabolises them as
# bulk carbon sources, inflating NITROGENASE_MO flux far above the glucose ceiling.
# At -0.001 they satisfy biosynthetic cofactor demand without providing net ATP.
_BNF_COFACTOR_EXCHANGES: frozenset[str] = frozenset({
    "EX_thm_e",      # thiamine   (C12H17N4OS)
    "EX_ribflv_e",   # riboflavin (C17H20N4O6)
    "EX_btn_e",      # biotin     (C10H16N2O3S)
    "EX_fol_e",      # folate     (C19H17N7O6)
    "EX_pnto__R_e",  # pantothenate (C9H17NO5)
})
# Maximum uptake rate for cofactors in BNF minimal medium (mmol/gDW/h).
# Small enough to cover biosynthetic demand; too small to drive net catabolism.
_BNF_COFACTOR_UPTAKE_BOUND: float = -0.001

# Ordered list of BIGG carbon source EX_ IDs to try for the primary C slot.
# Only the FIRST one found in the model is opened; all others remain closed.
_BNF_PREFERRED_CARBON_SOURCES: list[str] = [
    "EX_glc__D_e",   # D-glucose (canonical minimal-medium C source)
    "EX_sucr_e",     # sucrose (common soil exudate)
    "EX_fru_e",      # fructose
    "EX_malt_e",     # maltose
    "EX_ac_e",       # acetate (very common in AGORA2 models)
    "EX_succ_e",     # succinate
    "EX_mal__L_e",   # L-malate
    "EX_pyr_e",      # pyruvate
    "EX_lac__L_e",   # L-lactate
    "EX_etoh_e",     # ethanol
]

# N-containing exchange IDs that must stay CLOSED in BNF mode.
# Ammonium, nitrate, nitrite, urea — block all exogenous inorganic N.
# Amino-acid/nucleoside N sources are blockaded by the "close everything
# not on the inorganic whitelist" step and are not individually listed here.
_BNF_N_SOURCE_PREFIXES: tuple[str, ...] = (
    "EX_nh4_e", "EX_no3_e", "EX_no2_e", "EX_urea_e",
    "EX_gln__L_e", "EX_glu__L_e", "EX_asn__L_e", "EX_asp__L_e",
)

# Maximum carbon uptake in BNF minimal medium (mmol/gDW/h).
# Chosen to match standard AGORA2 monoculture minimal-medium glucose rate.
# ATP budget: 10 mmol glucose × ~36 ATP/glucose → 360 mmol ATP/gDW/h.
# NITROGENASE_MO ceiling: 360 / 16 × 2 ≈ 45 mmol NH4-equiv/gDW/h —
# already a high-end estimate; most communities will be far below this.
_BNF_CARBON_UPTAKE_BOUND: float = -10.0


def _apply_bnf_minimal_medium(community: Any) -> None:
    """Constrain the community model to a soil-relevant N-limited minimal medium.

    This MUST be called before FBA/FVA in BNF mode to prevent ATP-saturated
    (unrealistically high) N2-fixation flux predictions.

    Strategy
    --------
    1.  Close ALL EX_* exchanges (set lb = 0 for any with lb < 0).
    2a. Re-open truly inorganic exchanges (_BNF_INORGANIC_EXCHANGES) at their
        existing bounds (already pH-scaled by _apply_environmental_constraints).
    2b. Re-open organic cofactor exchanges (_BNF_COFACTOR_EXCHANGES) at a trace
        cap (_BNF_COFACTOR_UPTAKE_BOUND = -0.001 mmol/gDW/h).  These vitamins
        contain carbon but must NOT be used as bulk C sources — capping prevents
        the LP from catabolising them for ATP (which inflated FVA to 100–400).
    3.  Re-open the first available primary carbon source at
        _BNF_CARBON_UPTAKE_BOUND (-10 mmol/gDW/h).
    4.  Leave all N-source exchanges CLOSED (nh4, no3, urea, amino-acid N) so
        the community must rely on the atmospheric N2 supply via nitrogenase.

    The calling code (t1_fba_batch.py) opens EX_n2_e after this call.
    """
    # Step 1: close ALL EX_* uptake fluxes
    original_bounds: dict[str, float] = {}
    for rxn in community.reactions:
        if rxn.id.startswith("EX_") and rxn.lower_bound < 0:
            original_bounds[rxn.id] = rxn.lower_bound
            rxn.lower_bound = 0.0

    # Step 2a: re-open truly inorganic exchanges at env-scaled bounds
    for rxn in community.reactions:
        base_id = rxn.id.split("__org")[0]  # strip organism suffix if any
        if base_id in _BNF_INORGANIC_EXCHANGES:
            orig = original_bounds.get(rxn.id, -1000.0)
            rxn.lower_bound = max(orig, -1000.0)  # retain env-constraint scaling

    # Step 2b: re-open organic cofactors at trace cap only — NOT as bulk C sources
    for rxn in community.reactions:
        base_id = rxn.id.split("__org")[0]
        if base_id in _BNF_COFACTOR_EXCHANGES:
            rxn.lower_bound = _BNF_COFACTOR_UPTAKE_BOUND

    # Step 3: open first available primary carbon source at ≤10 mmol/gDW/h.
    # IMPORTANT: open the single community-level exchange (rxn.id == c_id) if
    # present; otherwise open only the FIRST per-organism variant found.  Do NOT
    # open all per-organism variants — each starts with (c_id + "__org") and
    # opening all N of them multiplies the effective carbon budget by N, inflating
    # NITROGENASE_MO FVA proportionally (the per-organism-stack bug).
    carbon_opened = False
    for c_id in _BNF_PREFERRED_CARBON_SOURCES:
        if carbon_opened:
            break
        for rxn in community.reactions:
            if rxn.id == c_id or rxn.id.startswith(c_id + "__org"):
                rxn.lower_bound = _BNF_CARBON_UPTAKE_BOUND
                carbon_opened = True
                break  # open exactly one exchange; prevents N-organism glucose stacking

    if not carbon_opened:
        # Fallback: use the first carbon-containing exchange that was open
        # originally.  Skip inorganic whitelist, cofactor set, and N sources —
        # only open genuine bulk organic carbon sources.
        for rxn_id, orig_lb in original_bounds.items():
            base_id = rxn_id.split("__org")[0]
            if base_id in _BNF_INORGANIC_EXCHANGES:
                continue
            if base_id in _BNF_COFACTOR_EXCHANGES:  # skip C-containing cofactors
                continue
            if any(base_id.startswith(p) for p in _BNF_N_SOURCE_PREFIXES):
                continue
            rxn = community.reactions.get_by_id(rxn_id)
            rxn.lower_bound = _BNF_CARBON_UPTAKE_BOUND
            carbon_opened = True
            break

    if not carbon_opened:
        logger.warning(
            "BNF minimal medium: no carbon source found in community model; "
            "FVA may be infeasible."
        )

    # Step 4 is implicit: N sources remain closed (lb=0) from Step 1.
    # EX_n2_e is opened by the calling code after this function returns.


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
