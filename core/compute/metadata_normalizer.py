"""
compute/metadata_normalizer.py — Field-name and value harmonization.

Normalizes heterogeneous metadata from EMP, NEON, MGnify, NCBI SRA, etc.
into the canonical column set used by db_utils.py (samples table).

Key responsibilities:
  - Alias resolution for field names (pH, organic_matter, etc.)
  - Unit conversion (ppm ↔ mg/kg, % → fraction, etc.)
  - Controlled vocabulary for land_use, soil_texture, sampling_fraction
  - GPS coordinate parsing (DMS → decimal)
  - Depth string parsing ("0-15 cm" → mid-point float)
  - Koppen-Geiger climate zone assignment (lat/lon lookup)
  - Sampling fraction detection from sample name / description keywords

See also: compute/metadata_synonyms.yaml for the full synonym tables.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_SYNONYMS_PATH = Path(__file__).parent / "metadata_synonyms.yaml"


# ---------------------------------------------------------------------------
# Inline defaults — overridden by metadata_synonyms.yaml if present
# ---------------------------------------------------------------------------

_DEFAULT_PH_ALIASES: list[str] = [
    "soil_ph", "ph", "ph_h2o", "ph_cacl2", "soil_water_ph",
    "ph_measured_cacl2_soil", "ph_measured_h2o_soil",
]

_DEFAULT_TEXTURE_ALIASES: list[str] = [
    "soil_texture", "texture", "soil_texture_class", "usda_texture_class",
]

_DEFAULT_OM_ALIASES: list[str] = [
    "organic_matter_pct", "total_organic_carbon", "soil_organic_carbon",
    "organic_carbon", "toc", "soc", "org_c_pct", "organic_matter",
]

_DEFAULT_LAND_USE_MAP: dict[str, str] = {
    "cropland": "cropland", "agricultural": "cropland", "agriculture": "cropland",
    "arable": "cropland", "farm": "cropland", "cultivated": "cropland",
    "prairie": "grassland", "pasture": "grassland", "meadow": "grassland",
    "grassland": "grassland", "rangeland": "grassland",
    "forest": "forest", "woodland": "forest", "boreal": "forest",
    "temperate forest": "forest", "tropical forest": "forest",
    "shrubland": "shrubland", "scrub": "shrubland", "chaparral": "shrubland",
    "wetland": "wetland", "marsh": "wetland", "bog": "wetland", "fen": "wetland",
    "urban": "urban", "city": "urban", "suburban": "urban",
    "mine": "disturbed", "remediation": "disturbed", "landfill": "disturbed",
}

_DEFAULT_TEXTURE_MAP: dict[str, str] = {
    "sand": "sand", "sandy": "sand",
    "loamy sand": "loamy sand", "loamy_sand": "loamy sand",
    "sandy loam": "sandy loam", "sandy_loam": "sandy loam",
    "loam": "loam",
    "silt loam": "silt loam", "silt_loam": "silt loam",
    "silt": "silt",
    "sandy clay loam": "sandy clay loam",
    "clay loam": "clay loam", "clay_loam": "clay loam",
    "silty clay loam": "silty clay loam",
    "silty clay": "silty clay",
    "sandy clay": "sandy clay",
    "clay": "clay",
}

_DEFAULT_FRACTION_KEYWORDS: dict[str, list[str]] = {
    "rhizosphere": ["rhizosphere", "rhizo", "root-associated", "root adhering"],
    "endosphere": ["endosphere", "endophyte", "endorhiza", "inside root"],
    "bulk": ["bulk soil", "bulk_soil", "non-rhizosphere", "bare soil"],
    "litter": ["litter", "organic horizon", "o_horizon", "o-horizon"],
}


def _load_synonyms() -> dict:
    if _SYNONYMS_PATH.exists():
        with open(_SYNONYMS_PATH) as fh:
            return yaml.safe_load(fh) or {}
    return {}


class MetadataNormalizer:
    """
    Normalizes raw metadata dicts from any adapter into the canonical sample schema.

    Usage:
        norm = MetadataNormalizer()
        canonical = norm.normalize_sample(raw_dict, source="neon")
    """

    def __init__(self):
        synonyms = _load_synonyms()
        self.ph_aliases: list[str] = synonyms.get("ph_aliases", _DEFAULT_PH_ALIASES)
        self.texture_aliases: list[str] = synonyms.get("texture_aliases", _DEFAULT_TEXTURE_ALIASES)
        self.om_aliases: list[str] = synonyms.get("om_aliases", _DEFAULT_OM_ALIASES)
        self.land_use_map: dict[str, str] = {
            **_DEFAULT_LAND_USE_MAP,
            **synonyms.get("land_use_map", {}),
        }
        self.texture_map: dict[str, str] = {
            **_DEFAULT_TEXTURE_MAP,
            **synonyms.get("texture_map", {}),
        }
        self.fraction_keywords: dict[str, list[str]] = {
            **_DEFAULT_FRACTION_KEYWORDS,
            **synonyms.get("fraction_keywords", {}),
        }

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def normalize_sample(self, raw: dict[str, Any], source: str = "unknown") -> dict[str, Any]:
        """
        Convert a raw metadata dict (any adapter) into canonical sample columns.

        Returns a flat dict keyed by db_utils.py samples column names.
        Unknown fields are not included (caller can merge extras into management JSON).
        """
        r = dict(raw)
        out: dict[str, Any] = {}

        # Pass through canonical fields directly
        _passthrough = {
            "sample_id", "source_id", "project_id", "biome", "feature", "material",
            "sequencing_type", "sequencing_depth", "n_taxa", "country",
            "clay_pct", "sand_pct", "silt_pct", "bulk_density", "total_nitrogen_ppm",
            "available_p_ppm", "cec", "moisture_pct", "temperature_c",
            "precipitation_mm", "sampling_date", "sampling_season",
            "site_id", "visit_number",
        }
        for col in _passthrough:
            if col in r:
                out[col] = r[col]

        out["source"] = source

        # Coordinates
        lat = self._extract_first(r, ["latitude", "lat", "decimallatitude", "decimalLatitude", "lat_lon"])
        lon = self._extract_first(r, ["longitude", "lon", "decimallongitude", "decimalLongitude"])
        if lat is not None:
            out["latitude"] = self.parse_coordinate(lat)
        if lon is not None:
            out["longitude"] = self.parse_coordinate(lon)

        # pH
        ph_val = self._extract_first(r, self.ph_aliases)
        if ph_val is not None:
            out["soil_ph"] = self.parse_ph(ph_val)

        # Organic matter / TOC
        om_val = self._extract_first(r, self.om_aliases)
        if om_val is not None:
            out["organic_matter_pct"] = self._to_float(om_val)

        # Texture
        tex_val = self._extract_first(r, self.texture_aliases)
        if tex_val is not None:
            out["soil_texture"] = self.normalize_texture(tex_val)

        # Land use
        lu_val = self._extract_first(r, ["land_use", "landuse", "env_biome", "land_cover"])
        if lu_val is not None:
            out["land_use"] = self.normalize_land_use(lu_val)

        # Depth
        depth_val = self._extract_first(r, [
            "sampling_depth_cm", "depth", "soil_depth", "depth_in_core",
            "collection_depth", "depth_to_sample_m",
        ])
        if depth_val is not None:
            out["sampling_depth_cm"] = self.parse_depth(depth_val)

        # Sampling fraction
        frac_val = self._extract_first(r, [
            "sampling_fraction", "sample_type", "sampleType",
            "env_material", "env_local_scale", "isolation_source",
        ])
        if frac_val is not None:
            out["sampling_fraction"] = self.detect_sampling_fraction(frac_val)
        else:
            # Try detecting from sample_id or description
            for field in ("sample_id", "description", "title"):
                if field in r and r[field]:
                    frac = self.detect_sampling_fraction(str(r[field]))
                    if frac:
                        out["sampling_fraction"] = frac
                        break

        # Remaining fields as management JSON (caller may merge)
        known = _passthrough | set(out.keys()) | {"latitude", "longitude"}
        extras = {k: v for k, v in r.items() if k not in known}
        if extras:
            import json
            out.setdefault("management", json.dumps(extras))

        return out

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def parse_ph(self, value: Any) -> float | None:
        """Parse pH from string or numeric. Clamps to [0, 14]."""
        try:
            ph = float(value)
            return max(0.0, min(14.0, ph))
        except (TypeError, ValueError):
            # Try extracting first float from string like "6.8 (H2O)"
            m = re.search(r"(\d+\.?\d*)", str(value))
            if m:
                ph = float(m.group(1))
                return max(0.0, min(14.0, ph))
        return None

    def parse_depth(self, value: Any) -> float | None:
        """
        Parse depth to a single float (cm).

        Handles:
          "0-15 cm" → 7.5
          "15cm"    → 15.0
          "0.15 m"  → 15.0
          15        → 15.0
        """
        if value is None:
            return None
        s = str(value).strip().lower()

        # Range "0-15 cm" or "0 to 15 cm"
        m = re.match(r"(\d+\.?\d*)\s*[-–to]+\s*(\d+\.?\d*)\s*(cm|m)?", s)
        if m:
            lo, hi = float(m.group(1)), float(m.group(2))
            mid = (lo + hi) / 2
            unit = m.group(3) or "cm"
            return mid * 100 if unit == "m" else mid

        # Single value
        m = re.match(r"(\d+\.?\d*)\s*(cm|m)?", s)
        if m:
            val = float(m.group(1))
            unit = m.group(2) or "cm"
            return val * 100 if unit == "m" else val

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def parse_coordinate(self, value: Any) -> float | None:
        """
        Parse a geographic coordinate (decimal or DMS) to decimal degrees.

        Handles:
          "-43.2105"
          "43°12'37.8\"N"
          "43 12 37.8 S"
        """
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            pass
        s = str(value).strip()
        # DMS pattern
        m = re.match(
            r"(\d+)[°\s]+(\d+)['\s]+(\d+\.?\d*)[\"\s]*([NSEW]?)",
            s, re.IGNORECASE
        )
        if m:
            deg = float(m.group(1))
            mins = float(m.group(2))
            secs = float(m.group(3))
            direction = m.group(4).upper()
            decimal = deg + mins / 60 + secs / 3600
            if direction in ("S", "W"):
                decimal = -decimal
            return decimal
        return None

    def normalize_land_use(self, value: Any) -> str | None:
        """Map free-text land-use descriptions to controlled vocabulary."""
        if not value:
            return None
        s = str(value).lower().strip()
        for key, canonical in self.land_use_map.items():
            if key in s:
                return canonical
        return s  # return as-is if no match

    def normalize_texture(self, value: Any) -> str | None:
        """Map free-text texture descriptions to USDA texture class terms."""
        if not value:
            return None
        s = str(value).lower().strip()
        return self.texture_map.get(s, s)

    def detect_sampling_fraction(self, value: Any) -> str | None:
        """
        Detect sampling fraction (rhizosphere / endosphere / bulk / litter)
        from a string (sample_id, env_material, description, etc.).
        Returns None if no match.
        """
        if not value:
            return None
        s = str(value).lower()
        for fraction, keywords in self.fraction_keywords.items():
            if any(kw in s for kw in keywords):
                return fraction
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_first(d: dict, keys: list[str]) -> Any:
        """Return value of first matching key in d (case-insensitive)."""
        # Build lowercase → original key map for case-insensitive lookup
        lower_map = {k.lower(): v for k, v in d.items()}
        for k in keys:
            if k in d and d[k] is not None and d[k] != "":
                return d[k]
            # case-insensitive fallback
            lk = k.lower()
            if lk in lower_map and lower_map[lk] is not None and lower_map[lk] != "":
                return lower_map[lk]
        return None

    @staticmethod
    def _to_float(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            m = re.search(r"(\d+\.?\d*)", str(value))
            return float(m.group(1)) if m else None
