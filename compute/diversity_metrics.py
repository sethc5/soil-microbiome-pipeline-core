"""
compute/diversity_metrics.py — T0 alpha-diversity metrics (scikit-bio).

Computes:
  - Shannon entropy (H')
  - Simpson diversity (1 - D)
  - Chao1 richness estimator
  - Observed OTU / ASV count
  - Pielou's evenness (J)
  - Faith's phylogenetic diversity (PD) — requires tree

Input: OTU count vector (numpy array) and optionally a skbio.TreeNode.

Usage:
  from compute.diversity_metrics import compute_alpha_diversity
  metrics = compute_alpha_diversity(counts_array, tree=None)
"""

from __future__ import annotations
import numpy as np


def compute_alpha_diversity(counts: np.ndarray, tree=None) -> dict[str, float]:
    """
    Compute alpha-diversity metrics from a 1-D OTU count array.

    Parameters
    ----------
    counts : np.ndarray
        Integer count vector (one value per OTU/ASV).
    tree : skbio.TreeNode, optional
        Phylogenetic tree for Faith's PD calculation.

    Returns
    -------
    dict with keys: shannon, simpson, chao1, observed_otus, pielou_evenness, faith_pd
    """
    raise NotImplementedError
