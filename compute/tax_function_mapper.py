"""
compute/tax_function_mapper.py — T0.25 taxonomy → predicted function via FaProTax.

FaProTax maps 16S taxonomy to functional role predictions using a curated
database of ~80 functional groups (nitrogen fixation, nitrification, denitrification,
methanogenesis, etc.).

Reference: Louca et al. (2016) "Decoupling function and taxonomy in the global
ocean microbiome" — Science 353, 1272-1277.

Usage:
  from compute.tax_function_mapper import map_taxonomy_to_function
  functional_profile = map_taxonomy_to_function(taxonomy_df)
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


def map_taxonomy_to_function(taxonomy_df, normalize: bool = True) -> dict[str, float]:
    """
    Map a taxonomy abundance table to functional group abundances via FaProTax.

    Parameters
    ----------
    taxonomy_df : pd.DataFrame
        Indexed by taxon string, one column of relative abundances.
    normalize : bool
        If True, return relative functional group abundances (sum to 1).

    Returns
    -------
    dict mapping FaProTax functional group → abundance.
    """
    raise NotImplementedError
