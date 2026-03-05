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
import csv
import io
import logging
import urllib.request
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

EMP_BIOM_URL = "https://ftp.microbio.me/emp/release1/otu_tables/"
_EMP_MAPPING_URL = (
    "https://ftp.microbio.me/emp/release1/mapping_files/"
    "emp_qiime_mapping_release1.tsv"
)


class EMPAdapter:
    SOURCE = "emp"

    def __init__(self, config: dict):
        self.config = config
        self._empo_filter = config.get("empo_3", "Soil (non-saline)")

    def download_biom(self, outdir: str, empo_3: str | None = None) -> str:
        """Download the EMP 16S closed-ref 97% OTU BIOM table.

        Returns local file path. File is ~600 MB; cached if already present.
        """
        empo_3 = empo_3 or self._empo_filter
        outdir_path = Path(outdir)
        outdir_path.mkdir(parents=True, exist_ok=True)

        # EMP release 1 — closed-ref 97% OTU, split by EMPO level 3
        safe_name = empo_3.lower().replace(" ", "_").replace("(", "").replace(")", "")
        biom_url = f"{EMP_BIOM_URL}emp_cr_silva_16S_123.subset_10k.biom"
        out_path = outdir_path / f"emp_16s_{safe_name}.biom"

        if out_path.exists():
            logger.info("EMP BIOM already cached at %s", out_path)
            return str(out_path)

        logger.info("Downloading EMP BIOM from %s ...", biom_url)
        try:
            urllib.request.urlretrieve(biom_url, str(out_path))
        except Exception as exc:
            logger.error("EMP BIOM download failed: %s", exc)
            return ""
        return str(out_path)

    def _download_mapping(self, outdir: Path) -> Path | None:
        """Download EMP mapping file; return cached path."""
        map_path = outdir / "emp_mapping.tsv"
        if map_path.exists():
            return map_path
        try:
            urllib.request.urlretrieve(_EMP_MAPPING_URL, str(map_path))
            return map_path
        except Exception as exc:
            logger.warning("EMP mapping download failed: %s", exc)
            return None

    def iter_soil_samples(
        self,
        biom_path: str | None = None,
        empo_3: str | None = None,
        mapping_dir: str = "/tmp/emp_mapping",
    ) -> Iterator[dict]:
        """Yield soil sample metadata rows from the EMP mapping file.

        Args:
            biom_path: optional pre-downloaded BIOM path (not required for metadata)
            empo_3: EMPO level-3 label to filter on (default from config)
            mapping_dir: directory to cache the mapping TSV
        """
        empo_3 = empo_3 or self._empo_filter
        map_dir = Path(mapping_dir)
        map_dir.mkdir(parents=True, exist_ok=True)

        map_path = self._download_mapping(map_dir)
        if not map_path:
            logger.error("Cannot iterate EMP samples: mapping file unavailable")
            return

        with open(map_path, "r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                if row.get("empo_3", "") != empo_3:
                    continue
                yield {
                    "sample_id": row.get("#SampleID", row.get("sample_name", "")),
                    "source": "emp",
                    "biome": empo_3,
                    "ph": _safe_float(row.get("ph")),
                    "temperature": _safe_float(row.get("temperature_celsius")),
                    "latitude": _safe_float(row.get("latitude_deg")),
                    "longitude": _safe_float(row.get("longitude_deg")),
                    "country": row.get("country"),
                    "collection_date": row.get("collection_timestamp"),
                    "host_scientific_name": row.get("host_scientific_name"),
                    "study_id": row.get("qiita_study_id"),
                    "biom_path": biom_path,
                }


def _safe_float(val: str | None) -> float | None:
    try:
        return float(val) if val not in (None, "", "nan", "NA", "N/A") else None
    except (ValueError, TypeError):
        return None
