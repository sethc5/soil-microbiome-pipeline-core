"""
compute/stability_analyzer.py — T2 community resilience and resistance analysis.

Measures two components of ecological stability:
  - Resistance: how much does target function drop immediately after perturbation?
  - Resilience: how quickly does target function return to baseline after perturbation?

Stability score = weighted combination of resistance and resilience indices.

Uses dFBA trajectories from dfba_runner.py as input.

Usage:
  from compute.stability_analyzer import compute_stability_score
  score = compute_stability_score(trajectory, perturbation_days=[45])
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


def compute_stability_score(
    trajectory: dict,
    perturbation_days: list[int],
    resistance_weight: float = 0.5,
    resilience_weight: float = 0.5,
) -> float:
    """
    Compute composite stability score from a dFBA trajectory.

    Returns float in [0, 1] — higher is more stable under perturbation.
    """
    raise NotImplementedError
