"""
compute/tax_profiler.py — T0 taxonomic profiling.

Two paths depending on sequencing type:
  16S (amplicon) — QIIME2 / DADA2 denoising → SILVA classifier → OTU/ASV table
  Shotgun        — Kraken2 + Bracken for read-level taxonomic classification

Outputs a standardized taxonomy dict (phylum → genus breakdown) and
compressed OTU table written to disk, with path stored in the DB.

Usage:
  from compute.tax_profiler import profile_taxonomy
  result = profile_taxonomy(fastq_paths, seq_type="16S", outdir="profiles/")
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def profile_taxonomy(
    fastq_paths: list[str | Path],
    seq_type: str,  # "16S", "ITS", "shotgun_metagenome"
    outdir: str | Path = "profiles/",
    threads: int = 4,
) -> dict:
    """
    Run taxonomic profiling and return summary + file paths.

    Returns keys: phylum_profile, top_genera, otu_table_path, n_taxa
    """
    raise NotImplementedError
