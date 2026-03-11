"""
compute/keystone_analyzer.py — T1 keystone taxon identification via sequential knockout.

For each community member, removes that organism from the metabolic model
and measures the change in target pathway flux. Taxa whose removal causes
>20% reduction in flux are classified as keystone taxa.

Also identifies metabolic exchange interactions between keystone taxa
(cross-feeding networks) by tracking shared metabolite pools.

Usage:
  from core.compute.keystone_analyzer import identify_keystone_taxa
  keystones = identify_keystone_taxa(community_model, baseline_flux, threshold=0.2)
"""

from __future__ import annotations
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Flux drop threshold default: 20% → keystone
_DEFAULT_THRESHOLD = 0.20


def _org_reaction_ids(model: Any, org_suffix: str | None) -> list[str]:
    """Return all reaction IDs that belong to a given organism suffix."""
    if org_suffix is None:
        return [rxn.id for rxn in model.reactions]
    return [rxn.id for rxn in model.reactions if rxn.id.endswith(org_suffix)]


def _flux_through_target_rxns(solution: Any, target_rxn_ids: list[str]) -> float:
    """Sum absolute fluxes through target reactions from a COBRApy Solution."""
    if not target_rxn_ids or solution is None:
        return 0.0
    return sum(abs(solution.fluxes.get(rxn_id, 0.0)) for rxn_id in target_rxn_ids) / max(len(target_rxn_ids), 1)


def identify_keystone_taxa(
    community_model: Any,  # cobra.Model assembled community model
    baseline_target_flux: float,
    flux_drop_threshold: float = _DEFAULT_THRESHOLD,
    target_rxn_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Run sequential single-knockout analysis and return keystone taxa list.

    For each organism in the community model (identified by __org{i} suffix):
      1. Delete all of that organism's reactions
      2. Re-optimize the community model
      3. Measure flux through target pathway
      4. Classify as keystone if flux_drop_pct ≥ flux_drop_threshold

    Each result entry:
      {taxon_id, taxon_name, flux_without (float), flux_drop_pct (float),
       is_keystone (bool), functional_contribution (str)}

    If COBRApy is not installed, returns an empty list with a warning.
    """
    try:
        import cobra
    except ImportError:
        logger.warning("cobra not installed — keystone analysis skipped. pip install cobra")
        return []

    if community_model is None:
        return []

    # Detect organism suffixes present in the model
    org_suffixes: list[str | None] = []
    suffix_pattern = re.compile(r"__org(\d+)$")
    seen_suffixes: set[str] = set()
    for rxn in community_model.reactions:
        m = suffix_pattern.search(rxn.id)
        if m:
            suf = m.group(0)
            seen_suffixes.add(suf)

    if not seen_suffixes:
        # Single-organism model — no knockouts to perform
        logger.debug("identify_keystone_taxa: single-organism community, skipping knockouts")
        return []

    org_suffixes = sorted(seen_suffixes)

    # Identify target reactions (use all reactions if not specified)
    if target_rxn_ids is None:
        # Best-effort: use any reactions flagged as target in the model
        target_rxn_ids = [
            rxn.id for rxn in community_model.reactions
            if getattr(rxn, "_is_target", False)
        ]

    keystones = []
    for suffix in org_suffixes:
        org_num = suffix.removeprefix("__org")
        org_rxn_ids = _org_reaction_ids(community_model, suffix)

        with community_model:  # ← context manager reverts all changes
            # Zero-out the organism's reactions (single-knockout)
            for rxn_id in org_rxn_ids:
                rxn = community_model.reactions.get_by_id(rxn_id)
                rxn.lower_bound = 0.0
                rxn.upper_bound = 0.0

            solution = community_model.optimize()
            if solution.status != "optimal":
                flux_without = 0.0
            else:
                flux_without = (
                    _flux_through_target_rxns(solution, target_rxn_ids)
                    if target_rxn_ids
                    else float(solution.objective_value)
                )

        flux_drop_pct = (
            (baseline_target_flux - flux_without) / baseline_target_flux
            if baseline_target_flux > 1e-12
            else 0.0
        )
        is_keystone = flux_drop_pct >= flux_drop_threshold

        contribution_label = (
            "critical" if flux_drop_pct >= 0.50
            else "keystone" if is_keystone
            else "redundant"
        )

        keystones.append({
            "taxon_id": f"org_{org_num}",
            "taxon_name": f"organism_{org_num}",
            "flux_without": float(flux_without),
            "flux_drop_pct": float(flux_drop_pct),
            "is_keystone": bool(is_keystone),
            "functional_contribution": contribution_label,
        })

        logger.debug(
            "Knockout org%s: flux=%.4f → %.4f (drop=%.1f%%) keystone=%s",
            org_num, baseline_target_flux, flux_without,
            flux_drop_pct * 100, is_keystone,
        )

    keystones.sort(key=lambda x: x["flux_drop_pct"], reverse=True)
    logger.info(
        "Keystone analysis complete: %d/%d organisms classified as keystone",
        sum(1 for k in keystones if k["is_keystone"]), len(keystones),
    )
    return keystones
