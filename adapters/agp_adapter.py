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
import csv
import logging
import urllib.request
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

# The American Gut Project soil samples were released as part of the EBI ENA
# study ERP012803 / Qiita study 10317. We access via the EBI metadata endpoint.
_AGP_ENA_STUDY = "ERP012803"
_ENA_BASE = "https://www.ebi.ac.uk/ena/portal/api"


def _safe_float(val: str | None) -> float | None:
    try:
        return float(val) if val not in (None, "", "nan", "NA", "N/A") else None
    except (ValueError, TypeError):
        return None


class AGPAdapter:
    SOURCE = "agp"

    def __init__(self, config: dict):
        self.config = config
        self._env_material = config.get("env_material", "soil")
        self._cache_dir = Path(config.get("cache_dir", "/tmp/agp_cache"))
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _fetch_ena_metadata(self) -> Path | None:
        """Download ENA sample metadata TSV for the AGP study."""
        cache_file = self._cache_dir / "agp_ena_samples.tsv"
        if cache_file.exists():
            return cache_file

        url = (
            f"{_ENA_BASE}/filereport"
            f"?accession={_AGP_ENA_STUDY}"
            f"&result=sample"
            f"&fields=sample_accession,sample_description,"
            f"collection_date,geo_loc_name,lat_lon,env_material,"
            f"ph,temperature,env_biome"
            f"&format=tsv&download=true"
        )
        logger.info("Fetching AGP metadata from ENA ...")
        try:
            urllib.request.urlretrieve(url, str(cache_file))
            return cache_file
        except Exception as exc:
            logger.warning("AGP ENA metadata download failed: %s", exc)
            return None

    def iter_soil_samples(self) -> Iterator[dict]:
        """Yield AGP soil samples with metadata from ENA study ERP012803."""
        meta_path = self._fetch_ena_metadata()
        if not meta_path:
            logger.error("AGP metadata unavailable — no samples yielded")
            return

        with open(meta_path, "r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                env_mat = row.get("env_material", "").lower()
                if self._env_material and self._env_material not in env_mat:
                    continue

                lat = lon = None
                lat_lon_str = row.get("lat_lon", "")
                if lat_lon_str and " " in lat_lon_str:
                    parts = lat_lon_str.split()
                    lat = _safe_float(parts[0])
                    lon = _safe_float(parts[-1]) if len(parts) > 1 else None

                yield {
                    "sample_id": row.get("sample_accession", ""),
                    "source": "agp",
                    "biome": "soil",
                    "ph": _safe_float(row.get("ph")),
                    "temperature": _safe_float(row.get("temperature")),
                    "latitude": lat,
                    "longitude": lon,
                    "country": row.get("geo_loc_name"),
                    "collection_date": row.get("collection_date"),
                    "env_material": row.get("env_material"),
                    "description": row.get("sample_description"),
                }
