"""
local_biom_adapter.py — Local BIOM / FASTA / FASTQ ingestion adapter.

For privately held datasets, in-house sequencing runs, or any case where
data is already present on disk rather than sourced from a public database.

Accepts:
  - BIOM format OTU/ASV tables (biom-format)
  - Raw FASTQ (paired-end or single-end)
  - Pre-computed taxonomy TSV files

Usage:
  adapter = LocalBIOMAdapter(config)
  for sample in adapter.from_biom("data/my_study.biom", metadata_csv="data/metadata.csv"):
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)


class LocalBIOMAdapter:
    SOURCE = "local"

    def __init__(self, config: dict):
        self.config = config

    def from_biom(self, biom_path: str, metadata_csv: str | None = None) -> Iterator[dict]:
        """Yield sample dicts from a local BIOM table."""
        raise NotImplementedError

    def from_fastq(self, fastq_dir: str, metadata_csv: str | None = None) -> Iterator[dict]:
        """Yield sample dicts from a directory of FASTQ files."""
        raise NotImplementedError
