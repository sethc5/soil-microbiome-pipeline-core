"""
compute/stability_analyzer.py — T2 community resilience and resistance analysis.

Measures two components of ecological stability:
  - Resistance: how much does target function drop immediately after perturbation?
  - Resilience: how quickly does target function return to baseline after perturbation?

Stability score = weighted combination of resistance and resilience indices.

Uses dFBA trajectories from dfba_runner.py as input.

Usage:
  from core.compute.stability_analyzer import core.compute_stability_score
  score = compute_stability_score(trajectory, perturbation_days=[45])
"""

from __future__ import annotations
import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def _find_perturbation_step(time_points: list[float], perturb_day: float) -> int:
    """Return the index of the time step closest to perturb_day."""
    diffs = [abs(t - perturb_day) for t in time_points]
    return diffs.index(min(diffs))


def compute_stability_score(
    trajectory: dict,
    perturbation_days: list[int],
    resistance_weight: float = 0.5,
    resilience_weight: float = 0.5,
    recovery_window_days: float = 10.0,
) -> float:
    """
    Compute composite ecological stability score from a dFBA trajectory.

    Definitions:
      Resistance  = 1 - (max_flux_drop / baseline_flux)
                    Measures how much function drops immediately at perturbation.
      Resilience  = fraction of recovery within recovery_window_days.
                    0 = no recovery, 1 = full recovery.

    Returns: float in [0, 1] — higher is more stable under perturbation.
    """
    flux = trajectory.get("target_flux_trajectory", [])
    time = trajectory.get("time_points", [])

    if not flux or not time:
        return 0.0

    flux_arr = np.array(flux, dtype=float)
    time_arr = np.array(time, dtype=float)

    # Estimate pre-perturbation baseline (mean of first 20% of trajectory)
    n = len(flux_arr)
    baseline_end = max(1, n // 5)
    baseline = float(flux_arr[:baseline_end].mean())
    if baseline < 1e-12:
        return 0.0

    if not perturbation_days:
        # No perturbations — use overall CV-based stability
        cv = float(flux_arr.std() / baseline)
        return float(max(0.0, 1.0 - min(cv, 1.0)))

    resistance_scores = []
    resilience_scores = []

    for p_day in perturbation_days:
        p_idx = _find_perturbation_step(list(time_arr), float(p_day))

        # Resistance: minimum flux in the 24h window after perturbation
        window_end_day = p_day + 1.0
        post_idxs = [i for i, t in enumerate(time_arr) if p_day <= t <= window_end_day]
        if not post_idxs:
            post_idxs = [p_idx]
        min_flux = float(flux_arr[post_idxs].min())
        drop_fraction = max(0.0, (baseline - min_flux) / baseline)
        resistance = 1.0 - min(drop_fraction, 1.0)
        resistance_scores.append(resistance)

        # Resilience: how much of the drop is recovered within recovery_window
        recovery_end_day = p_day + recovery_window_days
        recovery_idxs = [
            i for i, t in enumerate(time_arr)
            if p_day <= t <= recovery_end_day
        ]
        if not recovery_idxs:
            resilience_scores.append(0.0)
            continue

        recovery_flux = float(flux_arr[recovery_idxs[-1]])
        if (baseline - min_flux) < 1e-12:
            resilience = 1.0
        else:
            recovered = (recovery_flux - min_flux) / (baseline - min_flux)
            resilience = float(max(0.0, min(recovered, 1.0)))
        resilience_scores.append(resilience)

    mean_resistance = sum(resistance_scores) / len(resistance_scores)
    mean_resilience = sum(resilience_scores) / len(resilience_scores) if resilience_scores else 0.0

    stability = (
        resistance_weight * mean_resistance
        + resilience_weight * mean_resilience
    )
    logger.info(
        "Stability: resistance=%.3f, resilience=%.3f → composite=%.3f",
        mean_resistance, mean_resilience, stability,
    )
    return float(stability)


def compute_functional_redundancy(
    member_keystones: list[dict],
) -> float:
    """
    Estimate functional redundancy from keystone analysis.

    Redundancy = fraction of community members that are NOT keystone taxa.
    High redundancy → community can lose taxa without function collapse.
    Returns float in [0, 1].
    """
    if not member_keystones:
        return 0.5  # no info — neutral estimate
    n_keystone = sum(1 for k in member_keystones if k.get("is_keystone", False))
    n_total = len(member_keystones)
    redundancy = 1.0 - (n_keystone / n_total)
    return float(redundancy)


def full_stability_report(
    trajectory: dict,
    perturbation_days: list[int],
    member_keystones: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Compute all stability metrics and return as a dict.

    Returns:
      stability_score (float): Composite stability
      resistance (float): Resistance component
      resilience (float): Resilience component
      functional_redundancy (float): Fraction of non-keystone taxa
    """
    flux = trajectory.get("target_flux_trajectory", [])
    time = trajectory.get("time_points", [])
    flux_arr = np.array(flux, dtype=float)
    n = len(flux_arr)
    baseline_end = max(1, n // 5)
    baseline = float(flux_arr[:baseline_end].mean()) if n else 0.0

    if not perturbation_days or baseline < 1e-12:
        stability = compute_stability_score(trajectory, perturbation_days)
        return {
            "stability_score": stability,
            "resistance": stability,
            "resilience": stability,
            "functional_redundancy": compute_functional_redundancy(member_keystones or []),
        }

    # Separate component computation
    resistance_scores = []
    resilience_scores = []
    time_arr = np.array(time, dtype=float)
    for p_day in perturbation_days:
        p_idx = _find_perturbation_step(list(time_arr), float(p_day))
        window_end_day = p_day + 1.0
        post_idxs = [i for i, t in enumerate(time_arr) if p_day <= t <= window_end_day]
        if not post_idxs:
            post_idxs = [p_idx]
        min_flux = float(flux_arr[post_idxs].min())
        drop_fraction = max(0.0, (baseline - min_flux) / baseline)
        resistance_scores.append(1.0 - min(drop_fraction, 1.0))

        recovery_end_day = p_day + 10.0
        recovery_idxs = [i for i, t in enumerate(time_arr) if p_day <= t <= recovery_end_day]
        if recovery_idxs:
            recovery_flux = float(flux_arr[recovery_idxs[-1]])
            if (baseline - min_flux) < 1e-12:
                resilience_scores.append(1.0)
            else:
                recovered = (recovery_flux - min_flux) / (baseline - min_flux)
                resilience_scores.append(float(max(0.0, min(recovered, 1.0))))
        else:
            resilience_scores.append(0.0)

    resistance = sum(resistance_scores) / len(resistance_scores)
    resilience = sum(resilience_scores) / len(resilience_scores)
    stability = 0.5 * resistance + 0.5 * resilience
    redundancy = compute_functional_redundancy(member_keystones or [])

    return {
        "stability_score": float(stability),
        "resistance": float(resistance),
        "resilience": float(resilience),
        "functional_redundancy": float(redundancy),
    }
