"""
compute/genome_fetcher.py — T1 representative genome retrieval from PATRIC / NCBI RefSeq.

For each representative taxon selected for the community FBA model:
  1. Search PATRIC by taxonomy ID for best available reference genome
  2. Fall back to NCBI RefSeq assembly if PATRIC lacks coverage
  3. For taxa with no reference genome (40-60% of soil taxa), use
     the closest phylogenetic neighbor by 16S similarity

Downloaded genomes are cached locally — repeated runs never re-download.

Usage:
  from compute.genome_fetcher import GenomeFetcher
  fetcher = GenomeFetcher(genome_db="patric", cache_dir="genome_cache/")
  genome_path = fetcher.fetch(taxon_id="1234", taxon_name="Azospirillum brasilense")
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class GenomeFetcher:
    def __init__(self, genome_db: str = "patric", cache_dir: str | Path = "genome_cache/"):
        self.genome_db = genome_db
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, taxon_id: str, taxon_name: str) -> Path:
        """
        Return path to a representative genome FASTA for the taxon.
        Downloads and caches if not already present.
        """
        raise NotImplementedError

    def _fetch_patric(self, taxon_id: str) -> Path | None:
        raise NotImplementedError

    def _fetch_ncbi_refseq(self, taxon_id: str) -> Path | None:
        raise NotImplementedError

    def _nearest_phylogenetic_neighbor(self, taxon_name: str) -> Path | None:
        """Fall back to phylogenetic neighbor genome when no reference exists."""
        raise NotImplementedError
