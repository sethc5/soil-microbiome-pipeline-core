"""
compute/metabolic_exchange.py — T1 cross-feeding interaction network analysis.

Maps the shared metabolite pools in a community FBA model to identify
which organisms donate and which consume each exchanged metabolite.

Output: a directed bipartite exchange graph (organism → metabolite → organism)
as a NetworkX DiGraph, plus a summary table of the most significant exchanges.

Usage:
  from compute.metabolic_exchange import analyze_metabolic_exchanges
  graph, summary = analyze_metabolic_exchanges(community_model, fba_solution)
"""

from __future__ import annotations
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Metabolites known to be uninteresting for cross-feeding analysis
_IGNORE_METABOLITES = frozenset([
    "h_e", "h2o_e", "co2_e", "o2_e", "pi_e", "fe2_e", "fe3_e",
    "hco3_e", "h_c", "h2o_c",
])


def _org_of_reaction(rxn_id: str) -> str | None:
    """Extract '__org{i}' suffix → organism label, or None."""
    m = re.search(r"__org(\d+)$", rxn_id)
    return f"org_{m.group(1)}" if m else None


def analyze_metabolic_exchanges(
    community_model: Any,  # cobra.Model
    fba_solution: Any,     # cobra.Solution
    min_flux: float = 1e-6,
) -> tuple[Any, list[dict]]:
    """
    Build metabolic exchange network and return (nx.DiGraph | None, exchange_list).

    The exchange_list is a list of dicts:
      {donor, acceptor, metabolite_id, metabolite_name, flux, direction}

    exchange_list is always returned (even if networkx unavailable).
    nx.DiGraph is returned if networkx is installed, otherwise None.

    Filters out metabolite exchanges below min_flux threshold and
    uninformative metabolites (H₂O, CO₂, H⁺, phosphate, etc.).
    """
    if community_model is None or fba_solution is None:
        return None, []

    if fba_solution.status != "optimal":
        logger.debug("FBA solution not optimal — no exchange analysis")
        return None, []

    # Map metabolite → {org: net_flux} from FBA fluxes
    # A positive flux in a reaction consuming met_e means import (donation from pool)
    # Shared pool metabolites are in the "_e" compartment
    met_flux_map: dict[str, dict[str, float]] = {}  # met_id → {org_id: net_flux}

    for rxn in community_model.reactions:
        flux = fba_solution.fluxes.get(rxn.id, 0.0)
        if abs(flux) < min_flux:
            continue
        org = _org_of_reaction(rxn.id)
        if org is None:
            continue
        for met, stoich in rxn.metabolites.items():
            if not met.id.endswith("_e"):
                continue  # only shared extracellular metabolites
            if met.id in _IGNORE_METABOLITES:
                continue
            net = stoich * flux  # positive = produced, negative = consumed
            if abs(net) < min_flux:
                continue
            if met.id not in met_flux_map:
                met_flux_map[met.id] = {}
            met_flux_map[met.id][org] = met_flux_map[met.id].get(org, 0.0) + net

    # Build directed exchange edges: producer (net > 0) → consumer (net < 0)
    exchange_list: list[dict] = []
    for met_id, org_fluxes in met_flux_map.items():
        producers = {org: f for org, f in org_fluxes.items() if f > min_flux}
        consumers = {org: f for org, f in org_fluxes.items() if f < -min_flux}
        # Find the metabolite object for its name
        met_obj = community_model.metabolites.get_by_id(met_id)
        met_name = met_obj.name if met_obj else met_id
        for donor, donor_flux in producers.items():
            for acceptor, consumer_flux in consumers.items():
                exchange_flux = min(donor_flux, abs(consumer_flux))
                exchange_list.append({
                    "donor": donor,
                    "acceptor": acceptor,
                    "metabolite_id": met_id,
                    "metabolite_name": met_name,
                    "flux": exchange_flux,
                    "direction": f"{donor} → {acceptor}",
                })

    # Build networkx DiGraph if available
    graph = None
    try:
        import networkx as nx
        G = nx.DiGraph()
        for edge in exchange_list:
            if G.has_edge(edge["donor"], edge["acceptor"]):
                G[edge["donor"]][edge["acceptor"]]["metabolites"].append(edge["metabolite_id"])
                G[edge["donor"]][edge["acceptor"]]["total_flux"] += edge["flux"]
            else:
                G.add_edge(
                    edge["donor"],
                    edge["acceptor"],
                    metabolites=[edge["metabolite_id"]],
                    total_flux=edge["flux"],
                )
        graph = G
    except ImportError:
        logger.debug("networkx not installed — returning exchange_list only. pip install networkx")

    logger.info(
        "Metabolic exchange analysis: %d exchange edges across %d metabolites",
        len(exchange_list),
        len(met_flux_map),
    )
    return graph, exchange_list
