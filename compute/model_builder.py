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
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _carveme_available() -> bool:
    return shutil.which("carve") is not None


def _cobra_available() -> bool:
    try:
        import cobra  # noqa: F401
        return True
    except ImportError:
        return False


def build_metabolic_model(
    proteins_fasta: str | Path,
    outdir: str | Path = "models/",
    gap_fill: bool = True,
    diamond_db: str | None = None,
    genome_quality: dict[str, Any] | None = None,
    force: bool = False,
) -> Any:  # cobra.Model or None
    """
    Build a COBRApy genome-scale metabolic model using CarveMe.

    Steps:
      1. Run CarveMe CLI to reconstruct draft model from protein FASTA
      2. Load SBML into COBRApy
      3. Validate biomass reaction can carry flux
      4. Attach genome_quality metadata to model.notes

    Returns:
      cobra.Model on success, None on failure (with warning logged).

    If CarveMe or cobra is not installed, returns None with instructions.
    """
    proteins_fasta = Path(proteins_fasta)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stem = re.sub(r"[^a-zA-Z0-9_]", "_", proteins_fasta.stem)[:30]
    sbml_path = outdir / f"{stem}.xml"

    # Check for cached model
    if not force and sbml_path.exists() and sbml_path.stat().st_size > 1000:
        logger.debug("CarveMe model already exists: %s", sbml_path)
        return _load_and_annotate(sbml_path, genome_quality)

    if not _carveme_available():
        logger.warning(
            "CarveMe not found in PATH — model construction skipped. "
            "Install via: pip install carveme && diamond --version"
        )
        return None

    cmd = ["carve", "--output", str(sbml_path)]
    if gap_fill:
        cmd += ["--gapfill", "M9"]
    if diamond_db:
        cmd += ["--diamond-db", diamond_db]
    cmd.append(str(proteins_fasta))

    logger.info("Running CarveMe: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            logger.error("CarveMe stderr: %s", result.stderr[-1000:])
            logger.warning("CarveMe failed for %s — returning None", proteins_fasta)
            return None
    except subprocess.TimeoutExpired:
        logger.error("CarveMe timed out for %s", proteins_fasta)
        return None

    if not sbml_path.exists():
        logger.warning("CarveMe did not produce output at %s", sbml_path)
        return None

    return _load_and_annotate(sbml_path, genome_quality)


def _load_and_annotate(sbml_path: Path, genome_quality: dict | None) -> Any:
    """Load SBML into COBRApy, validate biomass, attach quality metadata."""
    if not _cobra_available():
        logger.warning("cobra not installed — returning SBML path as str. pip install cobra")
        return str(sbml_path)

    import cobra
    try:
        model = cobra.io.read_sbml_model(str(sbml_path))
    except Exception as exc:
        logger.error("Failed to load SBML %s: %s", sbml_path, exc)
        return None

    # Biomass validation
    with model:
        try:
            sol = model.optimize()
            biomass_flux = sol.objective_value if sol.status == "optimal" else 0.0
        except Exception:
            biomass_flux = 0.0

    if biomass_flux <= 1e-9:
        logger.warning(
            "Model %s cannot produce biomass (flux=%.2e). "
            "Consider increasing gap-fill or checking annotation quality.",
            sbml_path.name, biomass_flux,
        )

    # Attach genome quality metadata to model.notes
    if genome_quality:
        model.notes["genome_quality"] = {
            "completeness": genome_quality.get("completeness", 0.0),
            "contamination": genome_quality.get("contamination", 100.0),
            "tier": genome_quality.get("tier", "low"),
            "model_confidence": genome_quality.get("model_confidence", 0.35),
        }
        model.notes["biomass_flux"] = biomass_flux

    logger.info(
        "Model loaded: %d reactions, %d metabolites, biomass_flux=%.4f",
        len(model.reactions), len(model.metabolites), biomass_flux,
    )
    return model
