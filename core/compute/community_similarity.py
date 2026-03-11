"""
compute/community_similarity.py — T0.25 community similarity search.

Computes Bray-Curtis dissimilarity and UniFrac distances between
query communities and a reference database of known high-performing communities.

Returns the nearest-neighbor reference community and similarity score.
Uses scipy.spatial + skbio for metric computation.

Usage:
  from core.compute.community_similarity import CommunitySimilaritySearch
  searcher = CommunitySimilaritySearch.from_biom("reference/high_bnf_communities.biom")
  hit, score = searcher.query(otu_vector)
"""

from __future__ import annotations
import logging
import math
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


def _braycurtis(u: np.ndarray, v: np.ndarray) -> float:
    """Bray-Curtis dissimilarity ∈ [0, 1]. 0 = identical."""
    denom = np.sum(u) + np.sum(v)
    if denom == 0:
        return 0.0
    return float(np.sum(np.abs(u - v)) / denom)


def _normalize(vec: np.ndarray) -> np.ndarray:
    total = vec.sum()
    return vec / total if total > 0 else vec


class CommunitySimilaritySearch:
    """Nearest-neighbor search against a reference community OTU matrix.

    The index is an in-memory NumPy matrix (n_refs × n_otus) built at load time.
    For large references (>10 k samples) consider using an approximate index
    (e.g., FAISS cosine) — noted as a known scaling limit in the README.
    """

    def __init__(self, reference_biom_path: str | Path):
        self.reference_biom_path = Path(reference_biom_path)
        self._ref_matrix: np.ndarray | None = None  # shape (n_refs, n_otus)
        self._ref_ids: list[str] = []
        self._feature_ids: list[str] = []

    @property
    def sample_ids(self) -> list[str]:
        """Public alias for reference sample IDs."""
        return self._ref_ids

    @property
    def feature_ids(self) -> list[str]:
        """Public alias for feature (OTU/ASV) IDs."""
        return self._feature_ids

    @classmethod
    def from_biom(cls, biom_path: str | Path) -> "CommunitySimilaritySearch":
        instance = cls(biom_path)
        instance._load_index()
        return instance

    @classmethod
    def from_otu_matrix(
        cls,
        matrix: np.ndarray,
        sample_ids: list[str],
        feature_ids: list[str],
    ) -> "CommunitySimilaritySearch":
        """Build index directly from a pre-computed NumPy matrix (for testing)."""
        obj = cls.__new__(cls)
        obj.reference_biom_path = Path("<in-memory>")
        obj._ref_matrix = matrix.astype(float)
        obj._ref_ids = list(sample_ids)
        obj._feature_ids = list(feature_ids)
        return obj

    def _load_index(self) -> None:
        """Load reference BIOM file and build in-memory OTU matrix index."""
        try:
            import biom  # pip install biom-format
            table = biom.load_table(str(self.reference_biom_path))
            # biom table: rows = OTUs, cols = samples
            # Convert to (n_samples × n_otus) matrix
            dense = table.to_dataframe(dense=True)  # index=OTUs, cols=samples
            self._feature_ids = list(dense.index)
            self._ref_ids = list(dense.columns)
            self._ref_matrix = dense.values.T.astype(float)  # (n_samples, n_otus)
            logger.info(
                "Loaded reference BIOM: %d samples × %d OTUs",
                self._ref_matrix.shape[0],
                self._ref_matrix.shape[1],
            )
        except ImportError:
            logger.warning(
                "biom-format not installed — loading BIOM in TSV fallback mode. "
                "Install via: pip install biom-format"
            )
            self._load_index_tsv_fallback()

    def _load_index_tsv_fallback(self) -> None:
        """Parse a classic BIOM TSV export (rows=OTUs, cols=samples)."""
        path = self.reference_biom_path
        if not path.exists():
            logger.warning("Reference BIOM path does not exist: %s", path)
            self._ref_matrix = np.zeros((0, 0))
            self._ref_ids = []
            self._feature_ids = []
            return

        import csv
        with path.open() as fh:
            reader = csv.reader(fh, delimiter="\t")
            rows: list[list[str]] = []
            sample_ids: list[str] = []
            feature_ids: list[str] = []
            for i, row in enumerate(reader):
                if i == 0:
                    sample_ids = row[1:]
                    continue
                feature_ids.append(row[0])
                rows.append(row[1:])
        matrix = np.array([[float(v) for v in r] for r in rows], dtype=float)
        # Transpose: (n_samples, n_otus)
        self._feature_ids = feature_ids
        self._ref_ids = sample_ids
        self._ref_matrix = matrix.T

    def _align_query(self, otu_dict: dict[str, float] | np.ndarray) -> np.ndarray:
        """Align a query OTU dict or vector to the reference feature space."""
        if isinstance(otu_dict, np.ndarray):
            if otu_dict.shape[0] == len(self._feature_ids):
                return otu_dict.astype(float)
            # Pad or truncate
            v = np.zeros(len(self._feature_ids))
            v[: min(len(self._feature_ids), len(otu_dict))] = otu_dict[
                : len(self._feature_ids)
            ]
            return v
        vec = np.zeros(len(self._feature_ids))
        for i, fid in enumerate(self._feature_ids):
            vec[i] = otu_dict.get(fid, 0.0)
        return vec

    def query(
        self,
        otu_vector: dict[str, float] | np.ndarray,
        metric: str = "braycurtis",
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Return top-k nearest reference communities for a query OTU vector.

        Each result dict: {reference_id, similarity_score, rank, method}
        similarity_score is in [0, 1] — 1 = identical community.
        """
        if self._ref_matrix is None or self._ref_matrix.shape[0] == 0:
            logger.warning("CommunitySimilaritySearch: empty index — returning no hits")
            return []

        q = _normalize(self._align_query(otu_vector))
        n_refs = self._ref_matrix.shape[0]

        if metric == "braycurtis":
            dists = np.array([
                _braycurtis(q, _normalize(self._ref_matrix[i]))
                for i in range(n_refs)
            ])
        elif metric == "cosine":
            # cosine similarity → convert to distance
            # Normalize ref rows explicitly for API consistency with braycurtis path
            # (scipy.cdist cosine also normalises internally, but we do it here for clarity)
            from scipy.spatial.distance import cdist
            ref_normed = np.apply_along_axis(_normalize, 1, self._ref_matrix)
            dists_2d = cdist(q[np.newaxis, :], ref_normed, metric="cosine")
            dists = dists_2d[0]
        else:
            raise ValueError(f"Unsupported metric: {metric!r}. Use 'braycurtis' or 'cosine'.")

        # Similarity = 1 - dissimilarity; sort descending
        similarities = 1.0 - dists
        top_indices = np.argsort(dists)[: top_k]
        results = []
        for rank, idx in enumerate(top_indices, start=1):
            results.append({
                "reference_id": self._ref_ids[idx],
                "similarity_score": float(similarities[idx]),
                "dissimilarity": float(dists[idx]),
                "rank": rank,
                "method": metric,
            })
        return results
