"""
compute/community_similarity.py — T0.25 community similarity search.

Computes Bray-Curtis dissimilarity and UniFrac distances between
query communities and a reference database of known high-performing communities.

Returns the nearest-neighbor reference community and similarity score.
Uses scipy.spatial + skbio for metric computation.

Usage:
  from compute.community_similarity import CommunitySimilaritySearch
  searcher = CommunitySimilaritySearch.from_biom("reference/high_bnf_communities.biom")
  hit, score = searcher.query(otu_vector)
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class CommunitySimilaritySearch:
    def __init__(self, reference_biom_path: str | Path):
        self.reference_biom_path = reference_biom_path
        self._index = None

    @classmethod
    def from_biom(cls, biom_path: str | Path) -> "CommunitySimilaritySearch":
        instance = cls(biom_path)
        instance._load_index()
        return instance

    def _load_index(self) -> None:
        raise NotImplementedError

    def query(self, otu_vector, metric: str = "braycurtis") -> tuple[str, float]:
        """Return (nearest_reference_id, similarity_score) for a query OTU vector."""
        raise NotImplementedError
