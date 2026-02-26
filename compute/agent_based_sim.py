"""
compute/agent_based_sim.py — T2 optional agent-based community simulation.

Wraps iDynoMiCS (individual-based microbial community simulator) for
spatially explicit modeling of community dynamics. Use when spatial
structure matters (biofilm formation, aggregate colonization).

This is optional — dfba_runner.py is the default T2 engine.
iDynoMiCS is a Java-based tool; this module manages the subprocess
invocation and parses XML output.

Usage:
  from compute.agent_based_sim import run_idynomics
  result = run_idynomics(community_model, metadata, simulation_days=30)
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


def run_idynomics(
    community_model,
    metadata: dict,
    simulation_days: int = 30,
    idynomics_jar: str = "iDynoMiCS.jar",
) -> dict:
    """
    Run iDynoMiCS agent-based simulation (optional T2 engine).

    Returns keys: stability_score, spatial_community_profile, walltime_s
    """
    raise NotImplementedError
