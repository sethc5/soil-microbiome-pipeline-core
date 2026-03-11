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
import json
import logging
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

QIITA_API_BASE = "https://qiita.ucsd.edu"
_REQUEST_DELAY = 0.5


class QiitaAdapter:
    """Qiita REST API adapter for public microbiome studies."""
    SOURCE = "qiita"

    def __init__(self, config: dict):
        self.config = config
        self._token = config.get("qiita_token", "")
        self._biome_keyword = config.get("biome", "soil")
        self._max_results = int(config.get("max_results", 200))

    def _get(self, path: str, params: dict | None = None) -> dict | None:
        url = f"{QIITA_API_BASE}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)
        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            time.sleep(_REQUEST_DELAY)
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception as exc:
            logger.debug("Qiita API request failed for %s: %s", path, exc)
            return None

    def search(self, study_type: str = "soil", **filters) -> Iterator[dict]:
        """Yield sample metadata matching Qiita search criteria.

        Note: Qiita's public API is rate-limited; token recommended for bulk access.
        """
        # Qiita REST: /api/v1/study/list
        data = self._get("/api/v1/study/list")
        if not data:
            logger.warning("Qiita study list unavailable — no samples yielded")
            return

        yielded = 0
        for study_entry in data.get("data", []):
            if yielded >= self._max_results:
                break
            study_id = study_entry.get("study_id")
            title = study_entry.get("metadata", {}).get("study_title", "")
            if study_type.lower() not in title.lower() and \
               study_type.lower() not in study_entry.get("metadata", {}).get("study_abstract", "").lower():
                continue

            sample_data = self._get(f"/api/v1/study/{study_id}/samples")
            if not sample_data:
                continue
            for sample_id, attrs in (sample_data.get("data") or {}).items():
                yield {
                    "sample_id": sample_id,
                    "source": "qiita",
                    "study_id": study_id,
                    "ph": _safe_float(attrs.get("ph")),
                    "temperature": _safe_float(attrs.get("temperature")),
                    "latitude": _safe_float(attrs.get("latitude")),
                    "longitude": _safe_float(attrs.get("longitude")),
                    "country": attrs.get("geo_loc_name"),
                    "collection_date": attrs.get("collection_timestamp"),
                    "env_material": attrs.get("env_material"),
                    **attrs,
                }
                yielded += 1
                if yielded >= self._max_results:
                    break

    def get_biom(self, study_id: str, prep_id: str, outdir: str) -> str:
        """Download BIOM table for a study/prep combination. Returns local file path."""
        out_dir = Path(outdir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"qiita_study{study_id}_prep{prep_id}.biom"

        if out_path.exists():
            logger.info("Qiita BIOM already cached: %s", out_path)
            return str(out_path)

        # Qiita provides artifact download via: /api/v1/artifact/{artifact_id}/
        # First, find artifact id for the prep
        prep_data = self._get(f"/api/v1/prep/{prep_id}/")
        if not prep_data:
            logger.warning("Could not retrieve prep info for prep_id=%s", prep_id)
            return ""

        artifacts = prep_data.get("data", {}).get("artifacts", [])
        biom_artifact_id = None
        for art in artifacts:
            if "biom" in art.get("type", "").lower():
                biom_artifact_id = art.get("artifact_id")
                break
        if not biom_artifact_id:
            logger.warning("No BIOM artifact found for prep %s", prep_id)
            return ""

        download_url = f"{QIITA_API_BASE}/api/v1/artifact/{biom_artifact_id}/"
        try:
            time.sleep(_REQUEST_DELAY)
            headers = {"Accept": "application/json"}
            if self._token:
                headers["Authorization"] = f"Bearer {self._token}"
            req = urllib.request.Request(download_url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as resp:
                art_data = json.loads(resp.read())
            biom_url = art_data.get("data", {}).get("files", {}).get("BIOM", [])
            if not biom_url:
                return ""
            actual_url = biom_url[0] if isinstance(biom_url, list) else biom_url
            urllib.request.urlretrieve(actual_url, str(out_path))
            return str(out_path)
        except Exception as exc:
            logger.error("Qiita BIOM download failed: %s", exc)
            return ""


def _safe_float(val: str | None) -> float | None:
    try:
        return float(val) if val not in (None, "", "nan", "NA", "N/A") else None
    except (ValueError, TypeError):
        return None
