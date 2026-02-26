"""
compute/metadata_validator.py — T0 soil metadata parsing and ENVO standardization.

Handles:
  - Parsing pH from heterogeneous SRA field names and formats
  - Koppen-Geiger climate zone lookup from lat/lon
  - ENVO biome / feature / material term normalization
  - Soil texture class assignment from sand/silt/clay percentages (USDA triangle)
  - Missing-value imputation flags (never silently impute load-bearing fields)

Usage:
  from compute.metadata_validator import validate_sample_metadata
  normalized = validate_sample_metadata(raw_metadata_dict)
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# SRA uses many synonyms for the same field — normalize them
PH_FIELD_ALIASES = ["ph", "soil_ph", "acidity", "reaction", "ph_value", "pH"]


def validate_sample_metadata(raw: dict) -> dict:
    """
    Normalize and validate raw metadata from any source adapter.

    Missing load-bearing fields (ph, texture, lat/lon) are set to None
    with an explicit flag — they must be handled by T0 filter logic.
    """
    raise NotImplementedError


def texture_class_from_fractions(sand_pct: float, silt_pct: float, clay_pct: float) -> str:
    """Return USDA soil texture class string from particle size fractions."""
    raise NotImplementedError


def climate_zone_from_coords(lat: float, lon: float) -> str:
    """Return Koppen-Geiger 3-letter code for a lat/lon coordinate."""
    raise NotImplementedError
