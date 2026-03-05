"""
compute/genome_annotator.py — T1 Prokka genome annotation wrapper.

Runs Prokka on unannotated genome FASTA files to produce:
  - GFF3 annotation file
  - Annotated protein FASTA
  - GenBank file for downstream use

Used when a genome is downloaded without pre-existing annotation,
or when a MAG assembled from the metagenome needs annotation before
metabolic model construction.

Usage:
  from compute.genome_annotator import annotate_genome
  result = annotate_genome("genome_cache/GCF_12345.fasta", outdir="annotations/")
"""

from __future__ import annotations
import logging
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _prokka_available() -> bool:
    return shutil.which("prokka") is not None


def _count_genes_from_txt(txt_path: Path) -> dict[str, int]:
    """Parse Prokka *.txt summary file → gene count dict."""
    counts: dict[str, int] = {}
    if not txt_path.exists():
        return counts
    with txt_path.open() as fh:
        for line in fh:
            m = re.match(r"^(\d+)\s+(.+)$", line.strip())
            if m:
                counts[m.group(2)] = int(m.group(1))
    return counts


def annotate_genome(
    genome_fasta: str | Path,
    outdir: str | Path = "annotations/",
    threads: int = 4,
    kingdom: str = "Bacteria",
    prefix: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """
    Run Prokka annotation on a genome FASTA.

    Returns:
      gff_path (str): Path to GFF3 annotation
      proteins_fasta (str): Path to annotated protein FASTA (.faa)
      genbank_path (str): Path to GenBank output (.gbk)
      summary (dict): Prokka gene count summary
      prokka_available (bool): Whether Prokka was found in PATH

    If Prokka is not installed, returns empty paths with a warning so downstream
    steps (CarveMe model building) can detect missing annotation gracefully.
    """
    genome_fasta = Path(genome_fasta)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    stem = prefix or re.sub(r"[^a-zA-Z0-9_]", "_", genome_fasta.stem)[:20]

    # Check if annotation already exists
    gff_path = outdir / f"{stem}.gff"
    faa_path = outdir / f"{stem}.faa"
    gbk_path = outdir / f"{stem}.gbk"
    txt_path = outdir / f"{stem}.txt"

    if not force and gff_path.exists() and faa_path.exists():
        logger.debug("Prokka output already exists for %s — skipping", genome_fasta)
        return {
            "gff_path": str(gff_path),
            "proteins_fasta": str(faa_path),
            "genbank_path": str(gbk_path),
            "summary": _count_genes_from_txt(txt_path),
            "prokka_available": True,
        }

    if not _prokka_available():
        logger.warning(
            "Prokka not found in PATH — annotation skipped for %s. "
            "Install via: conda install -c bioconda prokka",
            genome_fasta,
        )
        return {
            "gff_path": "",
            "proteins_fasta": "",
            "genbank_path": "",
            "summary": {},
            "prokka_available": False,
        }

    cmd = [
        "prokka",
        "--outdir", str(outdir),
        "--prefix", stem,
        "--kingdom", kingdom,
        "--cpus", str(threads),
        "--rfam",
        "--quiet",
        str(genome_fasta),
    ]
    if force:
        cmd.append("--force")

    logger.info("Running Prokka: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            logger.error("Prokka stderr: %s", result.stderr[-1500:])
            raise RuntimeError(f"Prokka exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        raise RuntimeError("Prokka timed out after 3600 seconds")

    summary = _count_genes_from_txt(txt_path)
    logger.info("Prokka complete for %s: %s", genome_fasta.name, summary)

    return {
        "gff_path": str(gff_path),
        "proteins_fasta": str(faa_path),
        "genbank_path": str(gbk_path),
        "summary": summary,
        "prokka_available": True,
    }
