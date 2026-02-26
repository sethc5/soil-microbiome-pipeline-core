"""
compute/picrust2_runner.py — T0.25 functional prediction from 16S (PICRUSt2).

PICRUSt2 predicts MetaCyc pathway abundances from 16S ASV tables by placing
sequences on a reference phylogenetic tree and propagating known functional
annotations via ancestral state reconstruction.

Accuracy degrades for taxa with no close reference — common in soil (see README).

Usage:
  from compute.picrust2_runner import run_picrust2
  pred = run_picrust2(asv_table_biom, rep_seqs_fasta, outdir="picrust2_out/", threads=4)
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def run_picrust2(
    asv_table_biom: str | Path,
    rep_seqs_fasta: str | Path,
    outdir: str | Path = "picrust2_out/",
    threads: int = 4,
) -> dict:
    """
    Run PICRUSt2 pipeline and return pathway predictions.

    Returns keys: pathway_abundances (dict), ko_abundances (dict), nsti_mean (float)
    nsti_mean is the mean Nearest Sequenced Taxon Index — higher = less reliable.
    """
    raise NotImplementedError
