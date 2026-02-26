"""
neon_adapter.py — NEON Soil Microbiome data portal adapter.

NEON (National Ecological Observatory Network) publishes standardized soil
microbiome data across 47 US sites with full environmental metadata.

Data products:
  DP1.10107.001 — Soil microbiome: marker gene sequences
  DP1.10086.001 — Soil physical and chemical properties

Usage:
  adapter = NEONAdapter(config)
  for sample in adapter.iter_samples(site_ids=["HARV", "OSBS"]):
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)

NEON_API_BASE = "https://data.neonscience.org/api/v0"


class NEONAdapter:
    SOURCE = "neon"

    def __init__(self, config: dict):
        self.config = config

    def iter_samples(self, site_ids: list[str] | None = None) -> Iterator[dict]:
        """Yield soil microbiome samples from NEON sites."""
        raise NotImplementedError

    def get_soil_chemistry(self, sample_id: str) -> dict:
        """Return soil chemistry metadata for sample."""
        raise NotImplementedError
