"""
mgnify_adapter.py — EBI MGnify REST API adapter.

Retrieving metagenome studies, samples, and analysis results from:
  https://www.ebi.ac.uk/metagenomics/api/v1/

Rate limit: 100 requests/minute — request queuing is mandatory (see README gotchas).

Usage:
  adapter = MGnifyAdapter(config)
  for sample in adapter.search_samples(biome_lineage="root:Environmental:Terrestrial:Agricultural soil"):
      yield sample
"""

from __future__ import annotations
import logging
from typing import Iterator

logger = logging.getLogger(__name__)

MGNIFY_API_BASE = "https://www.ebi.ac.uk/metagenomics/api/v1"


class MGnifyAdapter:
    SOURCE = "mgnify"

    _PAGE_SIZE = 50
    _MIN_INTERVAL = 0.62  # ~96 req/min to stay under 100 req/min rate limit

    def __init__(self, config: dict):
        self.config = config
        self._last_request = 0.0
        self._token = config.get("mgnify_token")  # optional OAuth token

    def _get(self, url: str, params: dict | None = None) -> dict | None:
        """Rate-limited GET to MGnify API."""
        import json
        import time
        import urllib.request
        import urllib.parse

        elapsed = time.monotonic() - self._last_request
        if elapsed < self._MIN_INTERVAL:
            time.sleep(self._MIN_INTERVAL - elapsed)

        if params:
            url = url + "?" + urllib.parse.urlencode(params)

        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Token {self._token}"

        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                self._last_request = time.monotonic()
                return json.loads(resp.read())
        except Exception as exc:
            logger.debug("MGnify API request failed: %s", exc)
            self._last_request = time.monotonic()
            return None

    def search_samples(self, biome_lineage: str = "root:Environmental:Terrestrial:Soil", **filters) -> Iterator[dict]:
        """Yield sample metadata from MGnify matching biome lineage.

        Args:
            biome_lineage: MGnify biome lineage string
            **filters: extra query params (e.g. experiment_type='amplicon')
        """
        url = f"{MGNIFY_API_BASE}/samples"
        page = 1
        while True:
            params = {
                "biome_name": biome_lineage,
                "page_size": self._PAGE_SIZE,
                "page": page,
                **filters,
            }
            data = self._get(url, params)
            if not data:
                break

            results = data.get("data", [])
            for item in results:
                attrs = item.get("attributes", {})
                meta = {
                    "sample_id": item.get("id", ""),
                    "source": "mgnify",
                    "biome": biome_lineage,
                    "geographic_location": attrs.get("geo-loc-name"),
                    "latitude": attrs.get("latitude"),
                    "longitude": attrs.get("longitude"),
                    "collection_date": attrs.get("collection-date"),
                    "environment_material": attrs.get("environment-material"),
                    "study_id": (
                        lambda d: d[0].get("id") if d else None
                    )(
                        item.get("relationships", {})
                            .get("studies", {})
                            .get("data", [])
                    ),
                }
                yield meta

            next_url = data.get("links", {}).get("next")
            if not next_url:
                break
            page += 1

    def get_analysis(self, analysis_accession: str) -> dict:
        """Retrieve a processed MGnify analysis result."""
        url = f"{MGNIFY_API_BASE}/analyses/{analysis_accession}"
        data = self._get(url)
        if not data:
            return {}
        attrs = data.get("data", {}).get("attributes", {})
        return {
            "accession": analysis_accession,
            "source": "mgnify",
            "pipeline_version": attrs.get("pipeline-version"),
            "experiment_type": attrs.get("experiment-type"),
            "instrument_model": attrs.get("instrument-model"),
            **attrs,
        }

    def get_taxonomic_profile(self, analysis_accession: str) -> dict:
        """Retrieve OTU/SSU taxonomy summary for an analysis.

        Returns dict of {taxon_name: abundance}.
        """
        url = f"{MGNIFY_API_BASE}/analyses/{analysis_accession}/taxonomy"
        data = self._get(url)
        if not data:
            return {}

        profile: dict[str, float] = {}
        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            lineage = attrs.get("description", item.get("id", "unknown"))
            count = attrs.get("count", 0)
            profile[lineage] = float(count)
        return profile
