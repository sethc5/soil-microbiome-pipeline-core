"""
emp_adapter.py — Earth Microbiome Project BIOM table adapter.

Downloads and parses EMP BIOM tables (97% OTU clustered 16S V4 amplicon data)
from the EMP FTP or Qiita project 164.

Dataset:
  Thompson et al. (2017) — "A communal catalogue reveals Earth's multiscale
  microbial diversity" — Nature 551, 457–463.

Usage:
  adapter = EMPAdapter(config)
  for sample in adapter.iter_soil_samples():
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)

EMP_BIOM_URL = "https://ftp.microbio.me/emp/release1/otu_tables/"


class EMPAdapter:
    SOURCE = "emp"

    def __init__(self, config: dict):
        self.config = config

    def download_biom(self, outdir: str) -> str:
        """Download the EMP 16S BIOM table. Returns local file path."""
        raise NotImplementedError

    def iter_soil_samples(self, biom_path: str) -> Iterator[dict]:
        """Yield soil sample metadata rows from the EMP mapping file."""
        raise NotImplementedError
