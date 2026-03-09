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
import os
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
        # Proxy for routing through a local SOCKS tunnel when running from
        # a data-centre IP that EBI's WAF silently drops (e.g. Hetzner ASN).
        # Set via env var:  MGNIFY_PROXY=socks5://localhost:1080
        # or in config:     mgnify_proxy: socks5://localhost:1080
        self._proxy = (
            config.get("mgnify_proxy")
            or os.environ.get("MGNIFY_PROXY")
            or os.environ.get("HTTPS_PROXY")
        )
        if self._proxy:
            logger.info("MGnifyAdapter: routing via proxy %s", self._proxy)

    def _get(self, url: str, params: dict | None = None) -> dict | None:
        """Rate-limited GET to MGnify API (uses httpx for TLS compatibility).

        If self._proxy is set, all requests are routed through that proxy.
        Typical use: SOCKS5 reverse tunnel to local machine to bypass
        data-centre IP blocks on EBI's metagenomics backend.
        """
        import time
        try:
            import httpx as _httpx
        except ImportError:
            import requests as _requests
            _httpx = None

        elapsed = time.monotonic() - self._last_request
        if elapsed < self._MIN_INTERVAL:
            time.sleep(self._MIN_INTERVAL - elapsed)

        headers = {"Accept": "application/json", "User-Agent": "soil-microbiome-pipeline/1.0"}
        if self._token:
            headers["Authorization"] = f"Token {self._token}"

        proxy_kwargs: dict = {}
        if self._proxy:
            if _httpx is not None:
                proxy_kwargs["proxy"] = self._proxy
            else:
                proxy_kwargs["proxies"] = {"https": self._proxy, "http": self._proxy}

        for attempt in range(3):
            try:
                if _httpx is not None:
                    resp = _httpx.get(url, params=params, headers=headers, timeout=90, **proxy_kwargs)
                else:
                    resp = _requests.get(url, params=params, headers=headers, timeout=90, **proxy_kwargs)
                self._last_request = time.monotonic()
                if resp.status_code == 200:
                    return resp.json()
                logger.warning("MGnify API %s → HTTP %s (attempt %d)", url, resp.status_code, attempt + 1)
                if resp.status_code in (500, 502, 503):
                    time.sleep(10 * (attempt + 1))  # backoff on server errors
                    continue
                return None
            except Exception as exc:
                logger.warning("MGnify API request failed (attempt %d): %s", attempt + 1, exc)
                self._last_request = time.monotonic()
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
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

        Uses the /taxonomy/ssu endpoint (pipeline v4+).  Falls back to
        /taxonomy/lsu if SSU returns empty, then bare /taxonomy.

        Returns dict of {lineage_id: count} where lineage_id is the MGnify
        organism ID string (e.g. 'Bacteria:::Firmicutes:Bacilli').
        """
        for suffix in ("ssu", "lsu", ""):
            path = f"{MGNIFY_API_BASE}/analyses/{analysis_accession}/taxonomy"
            if suffix:
                path = f"{path}/{suffix}"

            all_items: list[dict] = []
            page = 1
            while True:
                data = self._get(path, {"page_size": 200, "page": page})
                if not data:
                    break
                items = data.get("data", [])
                if not items:
                    break
                all_items.extend(items)
                if not data.get("links", {}).get("next"):
                    break
                page += 1

            if all_items:
                profile: dict[str, float] = {}
                for item in all_items:
                    attrs = item.get("attributes", {})
                    lineage = item.get("id", attrs.get("lineage", "unknown"))
                    count = attrs.get("count", 0)
                    profile[lineage] = float(count)
                return profile

        return {}

    def get_taxonomic_profile_structured(self, accession: str) -> dict:
        """
        Return phylum_profile and top_genera dicts suitable for the communities table.

        Uses the MGnify SSU taxonomy endpoint which provides a pre-parsed
        hierarchy dict per organism (no semicolon parsing needed).

        Returns:
          {
            "phylum_profile": {phylum: rel_abundance, ...},
            "top_genera":     [{"name": genus, "rel_abundance": float}, ...],
          }
        """
        phylum_totals: dict[str, float] = {}
        genus_totals:  dict[str, float] = {}
        total_count: float = 0.0

        for suffix in ("ssu", "lsu", ""):
            path = f"{MGNIFY_API_BASE}/analyses/{accession}/taxonomy"
            if suffix:
                path = f"{path}/{suffix}"

            page = 1
            got_any = False
            while True:
                data = self._get(path, {"page_size": 200, "page": page})
                if not data:
                    break
                items = data.get("data", [])
                if not items:
                    break
                got_any = True
                for item in items:
                    attrs = item.get("attributes", {})
                    count = float(attrs.get("count", 0))
                    total_count += count
                    hierarchy = attrs.get("hierarchy", {})
                    rank = attrs.get("rank", "")

                    # Use pre-parsed hierarchy fields where available
                    phylum = (
                        hierarchy.get("phylum")
                        or hierarchy.get("division")
                        or ""
                    )
                    genus = hierarchy.get("genus") or ""

                    if phylum:
                        phylum_totals[phylum] = phylum_totals.get(phylum, 0.0) + count
                    if genus:
                        genus_totals[genus] = genus_totals.get(genus, 0.0) + count
                if not data.get("links", {}).get("next"):
                    break
                page += 1

            if got_any:
                break  # found data at this suffix, don't try next

        if not total_count:
            return {"phylum_profile": {}, "top_genera": []}

        phylum_profile = {
            k: round(v / total_count, 6)
            for k, v in sorted(phylum_totals.items(), key=lambda x: -x[1])
            if k
        }
        top_genera = sorted(
            [{"name": g, "rel_abundance": round(a / total_count, 6)}
             for g, a in genus_totals.items() if g],
            key=lambda x: -x["rel_abundance"],
        )[:50]

        return {"phylum_profile": phylum_profile, "top_genera": top_genera}

    def search_analyses(
        self,
        biome: str = "root:Environmental:Terrestrial:Soil",
        experiment_type: str = "amplicon",
        max_results: int = 5000,
        pipeline_version: str | None = None,
    ) -> Iterator[dict]:
        """
        Yield analysis-level records from MGnify (one per processed sample run).

        Each record has:  accession, sample_accession, study_accession,
        experiment_type, pipeline_version, instrument_platform, biome_lineage.

        This is the primary entry point for bulk ingestion — analyses already have
        QC'd taxonomy and functional profiles, no FASTQ processing needed.
        """
        from urllib.parse import quote
        # Strategy: enumerate soil studies via /biomes/{lineage}/studies, then
        # fetch analyses per study via /studies/{id}/analyses.
        # This avoids the faulty /analyses flat endpoint and the broken biome_name filter.
        biome_path = quote(biome, safe="")  # encode colons → %3A
        studies_url = f"{MGNIFY_API_BASE}/biomes/{biome_path}/studies"
        logger.info("search_analyses: fetching studies from %s", studies_url)

        study_page = 1
        seen = 0
        while seen < max_results:
            sdata = self._get(studies_url, {"page_size": self._PAGE_SIZE, "page": study_page})
            if not sdata:
                logger.warning("search_analyses: no data from studies endpoint (API may be down)")
                break
            studies = sdata.get("data", [])
            if not studies:
                break

            for study in studies:
                if seen >= max_results:
                    return
                study_id = study.get("id", "")
                analyses_url = f"{MGNIFY_API_BASE}/studies/{study_id}/analyses"
                a_params: dict = {
                    "experiment_type": experiment_type,
                    "page_size": self._PAGE_SIZE,
                    "page": 1,
                }
                if pipeline_version:
                    a_params["pipeline_version"] = pipeline_version

                a_page = 1
                while seen < max_results:
                    a_params["page"] = a_page
                    adata = self._get(analyses_url, a_params)
                    if not adata:
                        break
                    items = adata.get("data", [])
                    if not items:
                        break
                    for item in items:
                        if seen >= max_results:
                            return
                        attrs = item.get("attributes", {})
                        rels  = item.get("relationships", {})
                        yield {
                            "accession":           item.get("id", ""),
                            "sample_accession":    (
                                rels.get("sample", {}).get("data", {}) or {}
                            ).get("id", ""),
                            "study_accession":     study_id,
                            "experiment_type":     attrs.get("experiment-type", ""),
                            "pipeline_version":    attrs.get("pipeline-version", ""),
                            "instrument_platform": attrs.get("instrument-platform", ""),
                            "biome_lineage":       biome,
                        }
                        seen += 1
                    if not adata.get("links", {}).get("next"):
                        break
                    a_page += 1

            if not sdata.get("links", {}).get("next"):
                break
            study_page += 1

    def get_analysis_metadata(self, accession: str) -> dict:
        """
        Return the MGnify /analyses/{id} record including linked sample metadata.

        Returns a flat dict with analysis attrs merged with the sample's
        geographic + environmental attributes.
        """
        url = f"{MGNIFY_API_BASE}/analyses/{accession}"
        data = self._get(url)
        if not data:
            return {}
        item  = data.get("data", {})
        attrs = item.get("attributes", {})

        # Fetch linked sample for geo/env metadata
        sample_href = (
            item.get("relationships", {})
                .get("sample", {})
                .get("links", {})
                .get("related", "")
        )
        sample_attrs: dict = {}
        if sample_href:
            sample_data = self._get(sample_href)
            if sample_data:
                sample_attrs = (
                    sample_data.get("data", {}).get("attributes", {})
                )

        return {
            "accession":           accession,
            "pipeline_version":    attrs.get("pipeline-version"),
            "experiment_type":     attrs.get("experiment-type"),
            "instrument_model":    attrs.get("instrument-model"),
            "instrument_platform": attrs.get("instrument-platform"),
            "latitude":            sample_attrs.get("latitude"),
            "longitude":           sample_attrs.get("longitude"),
            "country":             sample_attrs.get("geo-loc-name"),
            "biome":               sample_attrs.get("biome"),
            "collection_date":     sample_attrs.get("collection-date"),
            # Environmental sample data (may be present in metadata entries)
            "soil_ph":             _safe_float(sample_attrs.get("soil pH")),
            "temperature_c":       _safe_float(sample_attrs.get("temperature")),
            "depth_cm":            _safe_float(sample_attrs.get("depth")),
            "environment_material":sample_attrs.get("environment-material"),
            "environment_feature": sample_attrs.get("environment-feature"),
        }

    def get_functional_profile(self, accession: str) -> dict:
        """
        Return MetaCyc pathway abundances and KEGG KO abundances for an analysis.

        This is what populates runs.t025_model.  Returns:
          {
            "pathways":    {pathway_id: abundance, ...},   # MetaCyc
            "kegg":        {ko_id: abundance, ...},        # KEGG KO
            "go_terms":    {go_id: count, ...},            # Gene Ontology
            "n_pathways":  int,
            "source":      "mgnify",
          }
        """
        pathways: dict[str, float] = {}
        kegg:     dict[str, float] = {}
        go_terms: dict[str, float] = {}

        # MetaCyc / InterPro pathway annotations
        for endpoint, target in [
            (f"{MGNIFY_API_BASE}/analyses/{accession}/pathways/metacyc", pathways),
            (f"{MGNIFY_API_BASE}/analyses/{accession}/pathways/kegg",     kegg),
            (f"{MGNIFY_API_BASE}/analyses/{accession}/go-terms",           go_terms),
        ]:
            data = self._get(endpoint)
            if not data:
                continue
            for item in data.get("data", []):
                attrs = item.get("attributes", {})
                feat_id = item.get("id") or attrs.get("accession", "unknown")
                count = attrs.get("count") or attrs.get("abundance", 0)
                try:
                    target[feat_id] = float(count)
                except (TypeError, ValueError):
                    pass

        return {
            "pathways":   pathways,
            "kegg":       kegg,
            "go_terms":   go_terms,
            "n_pathways": len(pathways),
            "source":     "mgnify",
        }



def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
