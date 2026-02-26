"""
compute/model_builder.py — T1 genome-scale metabolic model construction via CarveMe.

CarveMe builds draft genome-scale metabolic models from annotated protein FASTAs
using a universal bacterial template. Gap-filling is performed against the
template to ensure models can produce biomass.

See README gotchas: model quality varies enormously by genome completeness.
Incomplete MAGs must be flagged and treated with lower confidence in T1.

Usage:
  from compute.model_builder import build_metabolic_model
  model = build_metabolic_model("annotations/GCF_12345.faa", outdir="models/")
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def build_metabolic_model(
    proteins_fasta: str | Path,
    outdir: str | Path = "models/",
    gap_fill: bool = True,
    diamond_db: str | None = None,
) -> "cobra.Model":  # type: ignore[name-defined]
    """
    Build and return a COBRApy Model for a single organism.

    Wraps CarveMe CLI. The model is also saved as SBML to outdir.
    Returns None and logs a warning if model construction fails.
    """
    raise NotImplementedError
