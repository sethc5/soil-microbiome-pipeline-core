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

import logging
import math
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_alpha_diversity(
    counts: "np.ndarray | list",
    tree=None,
    otu_ids: list[str] | None = None,
) -> dict[str, Any]:
    """
    Compute alpha-diversity metrics from a 1-D OTU/ASV count vector.

    Parameters
    ----------
    counts  : array-like of non-negative integers (one entry per OTU/ASV).
    tree    : skbio.TreeNode, optional — required for Faith's PD.
    otu_ids : list of OTU ID strings, required when tree is provided.

    Returns
    -------
    dict:
      shannon          float   Shannon entropy H' (nats)
      simpson          float   Simpson diversity (1 - D)
      chao1            float   Chao1 richness estimator
      observed_otus    int     OTUs with abundance > 0
      pielou_evenness  float   Pielou J' (0–1)
      faith_pd         float | None  Faith's PD (None without tree)
    """
    counts = np.asarray(counts, dtype=float)
    counts = counts[counts > 0]

    result: dict[str, Any] = {
        "shannon": None,
        "simpson": None,
        "chao1": None,
        "observed_otus": 0,
        "pielou_evenness": None,
        "faith_pd": None,
    }

    if counts.size == 0:
        return result

    try:
        import skbio.diversity.alpha as ska
        int_counts = counts.astype(int)
        result["shannon"]       = float(ska.shannon(int_counts, base=math.e))
        result["simpson"]       = float(ska.simpson(int_counts))
        result["chao1"]         = float(ska.chao1(int_counts))
        result["observed_otus"] = int(ska.observed_features(int_counts))
        h = result["shannon"]
        s = result["observed_otus"]
        result["pielou_evenness"] = float(h / math.log(s)) if s > 1 else 0.0

        if tree is not None and otu_ids is not None:
            try:
                from skbio.diversity import alpha_diversity
                pd_arr = alpha_diversity(
                    "faith_pd",
                    int_counts.reshape(1, -1),
                    otu_ids,
                    tree=tree,
                )
                result["faith_pd"] = float(pd_arr[0])
            except Exception as exc:
                logger.warning("Faith PD calculation failed: %s", exc)

    except ImportError:
        logger.debug("scikit-bio not installed — using numpy fallback for diversity")
        result.update(_numpy_diversity(counts))

    return result


def diversity_from_profile(
    phylum_profile: dict[str, float],
    top_genera: list[str] | None = None,
) -> dict[str, Any]:
    """
    Compute diversity metrics from a relative-abundance profile dict.

    Useful when only pre-computed taxonomy profiles are available (no BIOM table).
    Chao1 and Faith PD require raw counts/tree and are returned as None.

    Parameters
    ----------
    phylum_profile : phylum → relative abundance (values should sum ≈ 1).
    top_genera     : optional list of genus strings for observed OTU count.
    """
    if not phylum_profile:
        return {"shannon": None, "simpson": None, "chao1": None,
                "observed_otus": 0, "pielou_evenness": None, "faith_pd": None}

    abundances = np.array(list(phylum_profile.values()), dtype=float)
    abundances = abundances[abundances > 0]
    total = abundances.sum()
    if total == 0:
        return {"shannon": None, "simpson": None, "chao1": None,
                "observed_otus": 0, "pielou_evenness": None, "faith_pd": None}

    p = abundances / total
    shannon = float(-np.sum(p * np.log(p)))
    simpson = float(1 - np.sum(p ** 2))
    s = len(p)
    pielou = float(shannon / math.log(s)) if s > 1 else 0.0
    observed = len(top_genera) if top_genera else s

    return {
        "shannon":         shannon,
        "simpson":         simpson,
        "chao1":           None,
        "observed_otus":   observed,
        "pielou_evenness": pielou,
        "faith_pd":        None,
    }


# ---------------------------------------------------------------------------
# Pure-numpy fallback (no scikit-bio)
# ---------------------------------------------------------------------------

def _numpy_diversity(counts: np.ndarray) -> dict[str, Any]:
    """Diversity metrics using only numpy."""
    total = counts.sum()
    if total == 0:
        return {"shannon": 0.0, "simpson": 0.0, "chao1": None,
                "observed_otus": 0, "pielou_evenness": 0.0, "faith_pd": None}
    p = counts / total
    p = p[p > 0]
    shannon = float(-np.sum(p * np.log(p)))
    simpson = float(1 - np.sum(p ** 2))
    s = int((counts > 0).sum())
    pielou = float(shannon / math.log(s)) if s > 1 else 0.0

    int_counts = counts.astype(int)
    f1 = int((int_counts == 1).sum())
    f2 = int((int_counts == 2).sum())
    s_obs = int((int_counts > 0).sum())
    chao1 = float(s_obs + f1 ** 2 / (2 * f2)) if f2 > 0 else float(s_obs + f1 * (f1 - 1) / 2)

    return {
        "shannon":         shannon,
        "simpson":         simpson,
        "chao1":           chao1,
        "observed_otus":   s,
        "pielou_evenness": pielou,
        "faith_pd":        None,
    }
