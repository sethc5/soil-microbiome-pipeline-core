"""
agp_adapter.py — American Gut Project adapter.

The AGP (now part of The Microsetta Initiative) released the world's
largest human microbiome dataset but also includes environmental samples.
This adapter targets the soil subset of AGP public data.

Usage:
  adapter = AGPAdapter(config)
  for sample in adapter.iter_soil_samples():
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)


class AGPAdapter:
    SOURCE = "agp"

    def __init__(self, config: dict):
        self.config = config

    def iter_soil_samples(self) -> Iterator[dict]:
        """Yield AGP soil samples with metadata."""
        raise NotImplementedError
