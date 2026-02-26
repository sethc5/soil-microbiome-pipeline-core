"""
ncbi_sra_adapter.py — NCBI SRA metagenome download and metadata retrieval.

Wraps the SRA Toolkit (prefetch + fasterq-dump) and the Entrez API to:
  1. Search SRA for soil metagenome samples matching config filters
  2. Download metadata (biosample attributes, environmental context)
  3. Download or stream FASTQ for processing
  4. Write results to the samples table via SoilDB

Prefer Aspera (ascp) over HTTP for bulk downloads — see README gotchas.

Usage (as a library):
  adapter = NCBISRAAdapter(config)
  for sample in adapter.search(biome="cropland", sequencing_type="16S"):
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)


class NCBISRAAdapter:
    SOURCE = "sra"

    def __init__(self, config: dict):
        self.config = config

    def search(self, **filters) -> Iterator[dict]:
        """Yield sample metadata dicts matching SRA query filters."""
        raise NotImplementedError

    def download_metadata(self, accession: str) -> dict:
        """Fetch BioSample metadata for a single SRA accession."""
        raise NotImplementedError

    def download_fastq(self, accession: str, outdir: str) -> list[str]:
        """Download FASTQ files via prefetch + fasterq-dump. Returns file paths."""
        raise NotImplementedError
