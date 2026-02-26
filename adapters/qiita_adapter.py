"""
qiita_adapter.py — Qiita public microbiome database adapter.

Qiita (https://qiita.ucsd.edu/) hosts thousands of amplicon and shotgun
metagenome studies with rich metadata. Access is via the Qiita REST API.

Usage:
  adapter = QiitaAdapter(config)
  for sample in adapter.search(study_type="soil"):
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)

QIITA_API_BASE = "https://qiita.ucsd.edu"


class QiitaAdapter:
    SOURCE = "qiita"

    def __init__(self, config: dict):
        self.config = config

    def search(self, study_type: str = "soil", **filters) -> Iterator[dict]:
        """Yield sample metadata matching Qiita search criteria."""
        raise NotImplementedError

    def get_biom(self, study_id: str, prep_id: str, outdir: str) -> str:
        """Download BIOM table for a study/prep. Returns file path."""
        raise NotImplementedError
