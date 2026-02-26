"""
redbiom_adapter.py — Redbiom (Qiita search layer) adapter.

Redbiom provides fast feature-centric search across all public Qiita data:
  https://github.com/biocore/redbiom

Query by ASV / OTU, taxon name, or metadata field. Used here to pull
all Qiita samples that contain a target taxon (e.g., known nifH-containing
genera) regardless of the originating study.

Usage:
  adapter = RedbiomAdapter(config)
  samples = adapter.search_by_taxon("Azospirillum")
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


class RedbiomAdapter:
    SOURCE = "redbiom"

    def __init__(self, config: dict):
        self.config = config

    def search_by_taxon(self, taxon_name: str, context: str = "Deblur_2021.09-Illumina-16S-V4-150nt-780653") -> list[str]:
        """Return sample IDs containing the specified taxon in Qiita."""
        raise NotImplementedError

    def fetch_samples(self, sample_ids: list[str], outdir: str) -> str:
        """Fetch BIOM subset for given sample IDs. Returns file path."""
        raise NotImplementedError
