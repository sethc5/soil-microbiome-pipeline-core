"""
compute/genome_quality.py — T1 genome completeness and contamination assessment.

Wraps CheckM for genome quality assessment or falls back to a lightweight
metadata-based estimate when CheckM is not available.

Quality tiers (see REBUILD_PLAN):
  high   — completeness ≥ 90%, contamination ≤ 5%
  medium — completeness ≥ 70%, contamination ≤ 10%
  low    — below medium thresholds

Model confidence is propagated from genome quality through T1 FBA results
(see community_fba.py) so downstream analyses can weight predictions accordingly.

Usage:
  from compute.genome_quality import assess_genome_quality, batch_assess
  quality = assess_genome_quality("genome_cache/GCF_12345.fasta")
"""

from __future__ import annotations
import logging
import shutil
import subprocess
import tempfile
import json
from pathlib import Path
from typing import Any

from compute._tool_resolver import resolve_tool

logger = logging.getLogger(__name__)

# Quality tier thresholds
HIGH_COMPLETENESS = 90.0
HIGH_CONTAMINATION = 5.0
MEDIUM_COMPLETENESS = 70.0
MEDIUM_CONTAMINATION = 10.0


def _checkm_cmd() -> str | None:
    """Return path to checkm executable, checking conda envs."""
    return resolve_tool("checkm") or shutil.which("checkm")


def _checkm_available() -> bool:
    return _checkm_cmd() is not None


def _assign_tier(completeness: float, contamination: float) -> str:
    if completeness >= HIGH_COMPLETENESS and contamination <= HIGH_CONTAMINATION:
        return "high"
    if completeness >= MEDIUM_COMPLETENESS and contamination <= MEDIUM_CONTAMINATION:
        return "medium"
    return "low"


def _model_confidence_from_tier(tier: str) -> float:
    """Map quality tier to a model confidence scalar in [0, 1]."""
    return {"high": 0.90, "medium": 0.65, "low": 0.35}.get(tier, 0.35)


def _parse_checkm_qa_output(qa_file: Path) -> dict[str, Any]:
    """Parse CheckM qa TSV output and return quality metrics."""
    if not qa_file.exists():
        return {}
    with qa_file.open() as fh:
        lines = [l.strip() for l in fh if l.strip() and not l.startswith("-")]
    if len(lines) < 2:
        return {}
    # CheckM QA output: header + data rows
    header = [h.strip() for h in lines[0].split("\t")]
    result_rows = []
    for row_line in lines[1:]:
        parts = [p.strip() for p in row_line.split("\t")]
        if len(parts) < len(header):
            continue
        row = dict(zip(header, parts))
        try:
            completeness = float(row.get("Completeness", 0))
            contamination = float(row.get("Contamination", 100))
        except (ValueError, KeyError):
            continue
        result_rows.append({
            "bin_id": row.get("Bin Id", row.get("Bin", "unknown")),
            "completeness": completeness,
            "contamination": contamination,
            "marker_lineage": row.get("Marker lineage", "unknown"),
            "tier": _assign_tier(completeness, contamination),
            "model_confidence": _model_confidence_from_tier(
                _assign_tier(completeness, contamination)
            ),
        })
    return result_rows[0] if result_rows else {}


def assess_genome_quality(
    genome_fasta: str | Path,
    outdir: str | Path | None = None,
    threads: int = 4,
) -> dict[str, Any]:
    """
    Assess genome completeness and contamination using CheckM.

    Returns dict with keys:
      completeness (float), contamination (float), tier (str),
      model_confidence (float), marker_lineage (str), checkm_available (bool)

    Falls back to pessimistic defaults if CheckM is not installed.
    """
    genome_fasta = Path(genome_fasta)
    if outdir is None:
        _tmp_dir = tempfile.mkdtemp(prefix="checkm_")
        outdir = Path(_tmp_dir)
    else:
        outdir = Path(outdir)
        outdir.mkdir(parents=True, exist_ok=True)

    if not _checkm_available():
        logger.warning(
            "CheckM not found in PATH — falling back to pessimistic genome quality estimate. "
            "Install via: conda install -c bioconda checkm-genome"
        )
        return {
            "completeness": 70.0,
            "contamination": 10.0,
            "tier": "medium",
            "model_confidence": _model_confidence_from_tier("medium"),
            "marker_lineage": "unknown",
            "checkm_available": False,
            "genome_path": str(genome_fasta),
        }

    # CheckM requires a directory of bins, not a single file
    bins_dir = outdir / "bins"
    bins_dir.mkdir(exist_ok=True)
    import shutil as _shutil
    _shutil.copy2(genome_fasta, bins_dir / genome_fasta.name)

    qa_out = outdir / "qa_output.tsv"
    lineage_out = outdir / "lineage.ms"

    try:
        # Step 1: lineage_wf
        subprocess.run(
            [
                _checkm_cmd(), "lineage_wf",
                "--tab_table",
                "-t", str(threads),
                "-x", genome_fasta.suffix.lstrip(".") or "fasta",
                str(bins_dir),
                str(outdir),
            ],
            capture_output=True,
            text=True,
            timeout=3600,
            check=True,
        )
        # Step 2: qa
        result = subprocess.run(
            [
                _checkm_cmd(), "qa",
                str(lineage_out),
                str(outdir),
                "-o", "2",
                "--tab_table",
                "-f", str(qa_out),
            ],
            capture_output=True,
            text=True,
            timeout=600,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.error("CheckM failed: %s", exc.stderr[-1000:])
        return {
            "completeness": 0.0,
            "contamination": 100.0,
            "tier": "low",
            "model_confidence": _model_confidence_from_tier("low"),
            "marker_lineage": "unknown",
            "checkm_available": True,
            "error": str(exc),
        }
    except subprocess.TimeoutExpired:
        raise RuntimeError("CheckM timed out")

    parsed = _parse_checkm_qa_output(qa_out)
    if not parsed:
        logger.warning("CheckM produced no parseable output for %s", genome_fasta)
        return {
            "completeness": 0.0,
            "contamination": 100.0,
            "tier": "low",
            "model_confidence": _model_confidence_from_tier("low"),
            "marker_lineage": "unknown",
            "checkm_available": True,
        }

    parsed["checkm_available"] = True
    parsed["genome_path"] = str(genome_fasta)
    logger.info(
        "CheckM quality: completeness=%.1f%%, contamination=%.1f%%, tier=%s",
        parsed["completeness"], parsed["contamination"], parsed["tier"],
    )
    return parsed


def batch_assess(
    genome_dirs: list[str | Path],
    outdir: str | Path = "checkm_batch/",
    threads_per_genome: int = 2,
) -> list[dict[str, Any]]:
    """
    Assess quality for multiple genome FASTAs in sequence.

    Returns list of quality dicts (same format as assess_genome_quality).
    """
    results = []
    for genome_path in genome_dirs:
        sub_out = Path(outdir) / Path(genome_path).stem
        quality = assess_genome_quality(genome_path, outdir=sub_out, threads=threads_per_genome)
        results.append(quality)
    return results
