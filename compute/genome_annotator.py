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
from pathlib import Path

logger = logging.getLogger(__name__)


def annotate_genome(
    genome_fasta: str | Path,
    outdir: str | Path = "annotations/",
    threads: int = 4,
    kingdom: str = "Bacteria",
) -> dict:
    """
    Run Prokka annotation on a genome FASTA.

    Returns keys: gff_path, proteins_fasta, summary (gene counts)
    """
    raise NotImplementedError
