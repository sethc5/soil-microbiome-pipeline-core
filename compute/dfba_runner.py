"""
compute/dfba_runner.py — T2 dynamic FBA time-course simulation.

Runs dFBA (dynamic flux balance analysis) to model community composition
and function over a complete growing season, including imposed perturbation
events (drought, fertilizer pulse, temperature shock).

Built on COBRApy + scipy ODE integrators.

Usage:
  from compute.dfba_runner import run_dfba
  trajectory = run_dfba(community_model, metadata, simulation_days=90,
                         perturbations=[{"type": "drought", "day": 45, "severity": 0.5}])
"""

from __future__ import annotations
import logging
import math
import time
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# Simulation parameters
_DEFAULT_DAYS = 45
_DEFAULT_DT_HOURS = 6.0


def _apply_perturbation(bounds: dict[str, tuple], perturbation: dict) -> dict[str, tuple]:
    """Apply a perturbation event to exchange bounds.

    Supported types:
      drought   — reduces H2O import by severity fraction
      fertilizer_pulse — boosts NH4 uptake for one time step
      temperature_shock — reduces all flux bounds by severity fraction
    """
    ptype = perturbation.get("type", "")
    severity = float(perturbation.get("severity", 0.5))
    modified = dict(bounds)

    if ptype == "drought":
        for rxn_id in ("EX_h2o_e", "EX_o2_e"):
            if rxn_id in modified:
                lo, hi = modified[rxn_id]
                modified[rxn_id] = (lo * (1 - severity), hi)
    elif ptype == "fertilizer_pulse":
        for rxn_id in ("EX_nh4_e", "EX_no3_e"):
            if rxn_id in modified:
                lo, hi = modified[rxn_id]
                modified[rxn_id] = (lo * (1 + severity * 2), hi)
    elif ptype == "temperature_shock":
        for rxn_id in list(modified):
            lo, hi = modified[rxn_id]
            modified[rxn_id] = (lo * (1 - severity * 0.3), hi * (1 - severity * 0.3))
    else:
        logger.debug("Unknown perturbation type: %r — skipped", ptype)

    return modified


def _get_exchange_bounds(model: Any) -> dict[str, tuple[float, float]]:
    """Extract current lower/upper exchange bounds."""
    return {rxn.id: (rxn.lower_bound, rxn.upper_bound) for rxn in model.reactions}


def _restore_bounds(model: Any, original_bounds: dict[str, tuple]) -> None:
    for rxn in model.reactions:
        if rxn.id in original_bounds:
            lo, hi = original_bounds[rxn.id]
            rxn.lower_bound = lo
            rxn.upper_bound = hi


def run_dfba(
    community_model: Any,
    metadata: dict,
    simulation_days: int = _DEFAULT_DAYS,
    dt_hours: float = _DEFAULT_DT_HOURS,
    perturbations: list[dict] | None = None,
    target_rxn_ids: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run dFBA (dynamic flux balance analysis) over a simulated growing season.

    Approach: Euler integration of biomass ODE coupled with FBA at each time step.
      dX/dt = μ * X   where μ = growth_rate from FBA
      Environmental exchange bounds are updated at each step from biomass state.

    Returns:
      time_points (list[float]): Time in days
      target_flux_trajectory (list[float]): Target flux at each time point
      biomass_trajectory (dict[str, list[float]]): Per-organism biomass trajectories
      stability_score (float): Coefficient of variation of target flux [0, 1]
      perturbation_responses (list[dict]): Flux at each perturbation event
      walltime_s (float): Elapsed real time

    If COBRApy is not installed, returns empty trajectories with walltime.
    """
    t_start = time.perf_counter()

    try:
        import cobra
    except ImportError:
        logger.warning("cobra not installed — dFBA skipped. pip install cobra")
        return {
            "time_points": [], "target_flux_trajectory": [],
            "biomass_trajectory": {}, "stability_score": 0.0,
            "perturbation_responses": [], "walltime_s": time.perf_counter() - t_start,
        }

    if community_model is None:
        return {
            "time_points": [], "target_flux_trajectory": [],
            "biomass_trajectory": {}, "stability_score": 0.0,
            "perturbation_responses": [], "walltime_s": time.perf_counter() - t_start,
        }

    n_steps = int(simulation_days * 24 / dt_hours)
    dt_days = dt_hours / 24.0

    # Index perturbation events by time step
    perturbation_index: dict[int, list[dict]] = {}
    for p in (perturbations or []):
        step = int(p.get("day", 0) * 24 / dt_hours)
        perturbation_index.setdefault(step, []).append(p)

    # Track biomass for each organism (keyed by model_id or index)
    organism_ids = [f"org_{i}" for i in range(len(getattr(community_model, "_member_models", [None])))]
    if not organism_ids:
        organism_ids = ["community"]
    biomass: dict[str, float] = {org: 1.0 for org in organism_ids}

    original_bounds = _get_exchange_bounds(community_model)
    current_bounds = dict(original_bounds)

    time_points: list[float] = []
    target_flux_trajectory: list[float] = []
    biomass_trajectory: dict[str, list[float]] = {org: [] for org in organism_ids}
    perturbation_responses: list[dict] = []

    for step in range(n_steps):
        t_day = step * dt_days
        time_points.append(t_day)

        # Apply any perturbations at this step
        if step in perturbation_index:
            for p in perturbation_index[step]:
                current_bounds = _apply_perturbation(current_bounds, p)
                logger.debug("Perturbation at day %.1f: %s", t_day, p.get("type"))

        # Set current bounds on model
        for rxn in community_model.reactions:
            if rxn.id in current_bounds:
                lo, hi = current_bounds[rxn.id]
                rxn.lower_bound = lo
                rxn.upper_bound = hi

        # Solve FBA
        solution = community_model.optimize()
        if solution.status != "optimal":
            target_flux = 0.0
            growth_rate = 0.0
        else:
            obj_val = float(solution.objective_value)
            growth_rate = max(obj_val, 0.0)
            # Target flux
            if target_rxn_ids:
                fluxes = [abs(solution.fluxes.get(rxn_id, 0.0)) for rxn_id in target_rxn_ids]
                target_flux = sum(fluxes) / max(len(fluxes), 1)
            else:
                target_flux = growth_rate

        target_flux_trajectory.append(target_flux)

        # Track perturbation response
        if step in perturbation_index:
            perturbation_responses.append({
                "day": t_day,
                "perturbation": [p.get("type") for p in perturbation_index[step]],
                "target_flux": target_flux,
            })

        # Euler step for biomass
        for org in organism_ids:
            old_bm = biomass[org]
            new_bm = old_bm + growth_rate * old_bm * dt_days
            new_bm = max(new_bm, 1e-12)
            biomass[org] = new_bm
            biomass_trajectory[org].append(new_bm)

        # Scale exchange bounds by total biomass (Michaelis-Menten-like)
        # Keep perturbation modifications, but scale by biomass
        total_bm = sum(biomass.values())
        if total_bm > 1e-9:
            scale = min(total_bm / len(organism_ids), 5.0)  # cap at 5× to prevent runaway
            for rxn_id in list(current_bounds):
                lo, hi = current_bounds[rxn_id]
                if lo < 0:
                    current_bounds[rxn_id] = (lo * scale, hi)

    _restore_bounds(community_model, original_bounds)

    # Stability score: 1 - CV of target flux (lower CV = more stable)
    arr = np.array(target_flux_trajectory)
    if arr.mean() > 1e-12:
        cv = arr.std() / arr.mean()
        stability_score = float(max(0.0, 1.0 - min(cv, 1.0)))
    else:
        stability_score = 0.0

    walltime_s = time.perf_counter() - t_start
    logger.info(
        "dFBA complete: %d steps, final_target_flux=%.4f, stability=%.3f, wall=%.1fs",
        n_steps,
        target_flux_trajectory[-1] if target_flux_trajectory else 0.0,
        stability_score,
        walltime_s,
    )
    return {
        "time_points": time_points,
        "target_flux_trajectory": target_flux_trajectory,
        "biomass_trajectory": biomass_trajectory,
        "stability_score": stability_score,
        "perturbation_responses": perturbation_responses,
        "walltime_s": walltime_s,
    }
