"""
mgnify_adapter.py — EBI MGnify REST API adapter.

Retrieving metagenome studies, samples, and analysis results from:
  https://www.ebi.ac.uk/metagenomics/api/v1/

Rate limit: 100 requests/minute — request queuing is mandatory (see README gotchas).

Usage:
  adapter = MGnifyAdapter(config)
  for sample in adapter.search_samples(biome_lineage="root:Environmental:Terrestrial:Agricultural soil"):
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)

MGNIFY_API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1"


class MGnifyAdapter:
    SOURCE = "mgnify"

    def __init__(self, config: dict):
        self.config = config

    def search_samples(self, biome_lineage: str, **filters) -> Iterator[dict]:
        """Yield sample metadata from MGnify matching biome lineage."""
        raise NotImplementedError

    def get_analysis(self, analysis_accession: str) -> dict:
        """Retrieve a processed MGnify analysis result."""
        raise NotImplementedError

    def get_taxonomic_profile(self, analysis_accession: str) -> dict:
        """Retrieve OTU taxonomy summary for an analysis."""
        raise NotImplementedError
