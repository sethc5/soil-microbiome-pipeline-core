"""
compute/humann3_shortcut.py — T0.25 fast functional profiling via HUMAnN3.

Wraps the HUMAnN3 pipeline to generate MetaCyc pathway and gene family
abundance profiles from shotgun metagenomes. For 16S samples, falls back
to PICRUSt2 predictions (see picrust2_runner.py).

Usage:
  from core.compute.humann3_shortcut import run_humann3
  profile = run_humann3(fastq_path, threads=8, outdir="humann3_out/")
"""

from __future__ import annotations
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _humann3_available() -> bool:
    return shutil.which("humann") is not None or shutil.which("humann3") is not None


def _parse_tsv(path: Path) -> dict[str, float]:
    """Parse a HUMAnN3 stratified or unstratified TSV → {feature: abundance}."""
    if not path.exists():
        return {}
    result: dict[str, float] = {}
    with path.open() as fh:
        header = None
        for line in fh:
            if line.startswith("#"):
                continue
            parts = line.rstrip("\n").split("\t")
            if header is None:
                header = parts
                continue
            feature = parts[0]
            # skip stratified rows (contain |)
            if "|" in feature:
                continue
            try:
                result[feature] = float(parts[1]) if len(parts) > 1 else 0.0
            except ValueError:
                pass
    return result


def run_humann3(
    fastq_path: str | Path,
    threads: int = 8,
    outdir: str | Path = "humann3_out/",
    bypass_nucleotide_search: bool = False,
) -> dict[str, Any]:
    """
    Run HUMAnN3 and return parsed pathway abundance dict.

    Returns keys:
      pathway_abundances (dict[str, float]),
      gene_families (dict[str, float]),
      pathway_coverage (dict[str, float]),
      pathway_abundance_path (str),
      gene_families_path (str)

    If HUMAnN3 is not installed, logs a warning and returns empty dicts
    so the pipeline continues gracefully.
    """
    fastq_path = Path(fastq_path)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    humann_cmd = "humann" if shutil.which("humann") else "humann3"

    if not _humann3_available():
        logger.warning(
            "HUMAnN3 not found in PATH — returning empty functional profile. "
            "Install via: conda install -c biobakery humann"
        )
        return {
            "pathway_abundances": {},
            "gene_families": {},
            "pathway_coverage": {},
            "pathway_abundance_path": "",
            "gene_families_path": "",
        }

    cmd = [
        humann_cmd,
        "--input", str(fastq_path),
        "--output", str(outdir),
        "--threads", str(threads),
        "--output-format", "tsv",
        "--remove-temp-output",
    ]
    if bypass_nucleotide_search:
        cmd.append("--bypass-nucleotide-search")

    logger.info("Running HUMAnN3: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200,
        )
        if result.returncode != 0:
            logger.error("HUMAnN3 stderr: %s", result.stderr[-2000:])
            raise RuntimeError(f"HUMAnN3 exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        raise RuntimeError("HUMAnN3 timed out after 7200 seconds")

    stem = fastq_path.stem.replace(".fastq", "").replace(".fq", "")
    pathway_file = outdir / f"{stem}_pathabundance.tsv"
    gene_families_file = outdir / f"{stem}_genefamilies.tsv"
    pathway_coverage_file = outdir / f"{stem}_pathcoverage.tsv"

    pathway_abundances = _parse_tsv(pathway_file)
    gene_families = _parse_tsv(gene_families_file)
    pathway_coverage = _parse_tsv(pathway_coverage_file)

    logger.info(
        "HUMAnN3 complete: %d pathways, %d gene families",
        len(pathway_abundances),
        len(gene_families),
    )
    return {
        "pathway_abundances": pathway_abundances,
        "gene_families": gene_families,
        "pathway_coverage": pathway_coverage,
        "pathway_abundance_path": str(pathway_file),
        "gene_families_path": str(gene_families_file),
    }
