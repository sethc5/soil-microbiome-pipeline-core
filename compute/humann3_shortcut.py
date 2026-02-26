"""
compute/humann3_shortcut.py — T0.25 fast functional profiling via HUMAnN3.

Wraps the HUMAnN3 pipeline to generate MetaCyc pathway and gene family
abundance profiles from shotgun metagenomes. For 16S samples, falls back
to PICRUSt2 predictions (see picrust2_runner.py).

Usage:
  from compute.humann3_shortcut import run_humann3
  profile = run_humann3(fastq_path, threads=8, outdir="humann3_out/")
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def run_humann3(
    fastq_path: str | Path,
    threads: int = 8,
    outdir: str | Path = "humann3_out/",
    bypass_nucleotide_search: bool = False,
) -> dict:
    """
    Run HUMAnN3 and return parsed pathway abundance dict.

    Returns keys: pathway_abundances (dict), gene_families_path (str)
    """
    raise NotImplementedError
