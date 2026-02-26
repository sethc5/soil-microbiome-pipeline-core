"""
compute/functional_gene_scanner.py — T0 functional gene presence/absence detection.

Uses MMseqs2 for fast sequence homology searches against curated HMM profiles
or reference sequence databases for target functional genes:

  nifH   — nitrogenase Fe protein (nitrogen fixation)
  dsrAB  — dissimilatory sulfite reductase (sulfate reduction)
  mcrA   — methyl-coenzyme M reductase (methanogenesis)
  mmox   — methane monooxygenase (methane oxidation)
  amoA   — ammonia monooxygenase (nitrification)
  alkB   — alkane 1-monooxygenase (hydrocarbon degradation)

Note: nifH is paraphyletic — not all nifH-containing organisms fix N₂ under
all conditions. Use abundance threshold AND phylogenetic placement filter
to avoid false positives (see README gotchas).

Usage:
  from compute.functional_gene_scanner import scan_functional_genes
  profile = scan_functional_genes(fasta_path, genes=["nifH", "amoA"])
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SUPPORTED_GENES = ["nifH", "dsrAB", "mcrA", "mmox", "amoA", "alkB", "phn", "mer"]


def scan_functional_genes(
    fasta_path: str | Path,
    genes: list[str] | None = None,
    mmseqs_threads: int = 4,
    min_identity: float = 0.5,
    min_coverage: float = 0.7,
) -> dict[str, bool | float]:
    """
    Detect functional gene presence in a metagenome FASTA.

    Returns dict with boolean presence and relative abundance estimate per gene.
    """
    raise NotImplementedError
