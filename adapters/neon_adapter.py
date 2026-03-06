"""
adapters/neon_adapter.py — NEON Ecological Observatory Network data adapter.

Provides soil microbiome samples from the highest-quality, fully-labeled
US ecological sites. NEON data is geo-typed, time-stamped, multi-visit,
and includes paired soil chemistry — ideal for time-series validation
of pipeline outputs.

NEON data products used:
  DP1.10107.001  — Soil microbiome marker gene survey (16S rRNA, ITS)
  DP1.10086.001  — Soil physical and chemical properties (periodic)

API docs: https://data.neonscience.org/data-api/explorer/

Usage:
    from adapters.neon_adapter import NEONAdapter
    adapter = NEONAdapter(token="your-neon-token")
    for sample in adapter.iter_samples(sites=["HARV","ORNL"]):
        db.upsert_sample(sample)

Token: register free at https://data.neonscience.org
"""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

NEON_API_BASE = "https://data.neonscience.org/api/v0"

# NEON data product IDs
PRODUCT_MICROBIOME = "DP1.10107.001"
PRODUCT_SOIL_CHEM  = "DP1.10086.001"

# Microbiome sample types present in DP1.10107.001
NEON_PHENOPHASE_TO_FRACTION: dict[str, str] = {
    "rhizosphere": "rhizosphere",
    "endorhiza":   "endosphere",
    "bulk":        "bulk",
    "O horizon":   "litter",
    "M horizon":   "bulk",
}

# NEON domain → ENVO biome (simplified)
NEON_DOMAIN_BIOME: dict[str, str] = {
    "D01": "temperate forest biome",
    "D02": "temperate forest biome",
    "D03": "subtropical moist broadleaf forest biome",
    "D04": "subtropical moist broadleaf forest biome",
    "D05": "temperate broadleaf forest biome",
    "D06": "temperate grassland biome",
    "D07": "boreal forest biome",
    "D08": "temperate grassland biome",
    "D09": "temperate grassland biome",
    "D10": "temperate grassland biome",
    "D11": "temperate grassland biome",
    "D12": "mediterranean shrubland biome",
    "D13": "montane shrubland biome",
    "D14": "desert biome",
    "D15": "montane grassland biome",
    "D16": "montane shrubland biome",
    "D17": "mediterranean shrubland biome",
    "D18": "tundra biome",
    "D19": "taiga biome",
    "D20": "tropical moist broadleaf forest biome",
}


class NEONAdapter:
    """
    Fetches soil microbiome samples from NEON Data Portal.

    Parameters
    ----------
    token : str | None
        NEON API token (optional but recommended for higher rate limits).
        Set via NEON_API_TOKEN env var if not passed directly.
    data_dir : str | Path | None
        Local cache directory for downloaded FASTQ/BIOM files.
    request_timeout : int
        Seconds before HTTP request timeout.
    config : dict | None
        Legacy config dict (ignored; kept for adapter interface compatibility).
    """

    SOURCE = "neon"

    def __init__(
        self,
        token: str | None = None,
        data_dir: str | Path | None = None,
        request_timeout: int = 30,
        config: dict | None = None,
    ):
        self.token = token or os.environ.get("NEON_API_TOKEN", "")
        self.data_dir = Path(data_dir) if data_dir else Path("data/neon_cache")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = request_timeout
        self._session = requests.Session()
        if self.token:
            self._session.headers.update({"X-API-Token": self.token})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def iter_sites(self) -> Iterator[dict]:
        """
        Yield metadata for every NEON site that has microbiome data.

        Yields dicts: site_id, site_name, domain_id, biome, latitude,
        longitude, state.
        """
        data = self._get(f"{NEON_API_BASE}/sites")
        for site in data.get("data", []):
            products = [p["dataProductCode"] for p in site.get("dataProducts", [])]
            if PRODUCT_MICROBIOME not in products:
                continue
            domain = site.get("domainCode", "")
            yield {
                "site_id":   site["siteCode"],
                "site_name": site.get("siteName", ""),
                "domain_id": domain,
                "biome":     NEON_DOMAIN_BIOME.get(domain, "terrestrial biome"),
                "latitude":  site.get("siteLatitude"),
                "longitude": site.get("siteLongitude"),
                "state":     site.get("stateCode", ""),
            }

    def iter_samples(
        self,
        sites: list[str] | None = None,
        years: list[int] | None = None,
        site_ids: list[str] | None = None,   # legacy alias
    ) -> Iterator[dict]:
        """
        Yield canonical sample dicts for each NEON microbiome sample.

        Parameters
        ----------
        sites : list of NEON site codes, e.g. ["HARV","ORNL"].
                If None, fetches all sites with microbiome data.
        years : list of years to include, e.g. [2019, 2020, 2021].
                If None, fetches all available years.
        site_ids : legacy alias for sites (deprecated, use sites=).
        """
        from compute.metadata_normalizer import MetadataNormalizer
        norm = MetadataNormalizer()

        effective_sites = sites or site_ids or [
            s["site_id"] for s in self.iter_sites()
        ]

        for site_code in effective_sites:
            logger.info("NEON: fetching microbiome availability for %s", site_code)
            avail = self._get_product_availability(PRODUCT_MICROBIOME, site_code)

            for release_entry in avail:
                for ym in release_entry.get("availableMonths", []):
                    year = int(ym[:4])
                    if years and year not in years:
                        continue
                    visit_number = self._visit_number(site_code, ym)
                    samples_raw = self._fetch_sample_table(
                        PRODUCT_MICROBIOME, site_code, ym
                    )
                    soil_chem = self.get_soil_chemistry(site_code, ym)

                    for raw in samples_raw:
                        merged = {**raw, **soil_chem}
                        canonical = norm.normalize_sample(merged, source="neon")
                        canonical.update(
                            self._map_neon_fields(raw, site_code, ym, visit_number)
                        )
                        yield canonical

    def get_soil_chemistry(
        self,
        site_code_or_sample_id: str,
        year_month: str | None = None,
    ) -> dict:
        """
        Return soil chemistry metadata for a site/month from DP1.10086.001.

        Signatures:
          get_soil_chemistry("HARV", "2021-06")   → canonical chemistry dict
          get_soil_chemistry("neon.DNA123")        → {}  (legacy call, no month given)

        Returns averaged canonical field dict, or {} if unavailable.
        """
        site_code = site_code_or_sample_id
        if year_month is None:
            return {}

        try:
            rows = self._fetch_sample_table(PRODUCT_SOIL_CHEM, site_code, year_month)
        except Exception as exc:
            logger.warning(
                "NEON soil chem unavailable for %s %s: %s", site_code, year_month, exc
            )
            return {}

        if not rows:
            return {}

        accum: dict[str, list[float]] = defaultdict(list)
        field_map = {
            "soilpH":           "soil_ph",
            "pHH2O":            "soil_ph",
            "organicCPercent":  "organic_matter_pct",
            "clayPercent":      "clay_pct",
            "sandPercent":      "sand_pct",
            "siltPercent":      "silt_pct",
            "bulkDensity":      "bulk_density",
            "totalNitrogen":    "total_nitrogen_ppm",
            "extractableP":     "available_p_ppm",
            "CECbuffer":        "cec",
            "soilMoisture":     "moisture_pct",
        }
        texture_val: str | None = None
        for row in rows:
            for src_field, dst_field in field_map.items():
                val = row.get(src_field)
                if val is None:
                    continue
                try:
                    accum[dst_field].append(float(val))
                except (TypeError, ValueError):
                    pass
            for tex_key in ("textureclss", "textureClass", "texturClassPercent"):
                if row.get(tex_key):
                    texture_val = str(row[tex_key])
                    break

        out: dict = {}
        for col, vals in accum.items():
            if vals:
                out[col] = sum(vals) / len(vals)
        if texture_val:
            out["soil_texture"] = texture_val
        return out

    def download_sequence_data(
        self,
        site_code: str,
        year_month: str,
        seq_type: str = "16S",
    ) -> list[Path]:
        """
        Download FASTQ/BIOM files for a site/month. Returns local paths.

        Parameters
        ----------
        seq_type : '16S' or 'ITS'
        """
        files_meta = self._fetch_file_list(PRODUCT_MICROBIOME, site_code, year_month)
        keyword = "16S" if seq_type == "16S" else "ITS"
        downloaded: list[Path] = []

        for f in files_meta:
            url   = f.get("url", "")
            name  = f.get("name", "")
            if keyword not in name:
                continue
            dest = self.data_dir / site_code / year_month / name
            if dest.exists():
                logger.debug("NEON: cache hit %s", dest)
                downloaded.append(dest)
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            logger.info("NEON: downloading %s", name)
            resp = self._session.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    fh.write(chunk)
            downloaded.append(dest)
            time.sleep(0.1)

        return downloaded

    # ------------------------------------------------------------------
    # NEON field mapping helpers
    # ------------------------------------------------------------------

    def _map_neon_fields(
        self,
        raw: dict,
        site_code: str,
        year_month: str,
        visit_number: int,
    ) -> dict:
        """Map NEON-specific field names → canonical sample schema fields."""
        out: dict = {
            "site_id":      site_code,
            "visit_number": visit_number,
            "source":       "neon",
            "source_id":    raw.get("dnaSampleID") or raw.get("sampleID") or "",
            "sampling_date": year_month + "-01",
        }

        dna_id = raw.get("dnaSampleID") or raw.get("sampleID") or ""
        if dna_id:
            out["sample_id"] = f"neon.{dna_id}"

        seq_type = raw.get("genomicsSampleType", "")
        if "ITS" in seq_type or "fungi" in seq_type.lower():
            out["sequencing_type"] = "ITS"
        elif "16S" in seq_type or "bacteria" in seq_type.lower():
            out["sequencing_type"] = "16S"

        sample_type = raw.get("sampleType", "")
        for neon_key, canonical in NEON_PHENOPHASE_TO_FRACTION.items():
            if neon_key.lower() in sample_type.lower():
                out["sampling_fraction"] = canonical
                break

        top = raw.get("sampleTopDepth")
        bot = raw.get("sampleBottomDepth")
        if top is not None and bot is not None:
            try:
                out["sampling_depth_cm"] = (float(top) + float(bot)) / 2
            except (TypeError, ValueError):
                pass

        domain = raw.get("domainID", "")
        if domain:
            out["biome"] = NEON_DOMAIN_BIOME.get(domain, "terrestrial biome")

        return out

    # ------------------------------------------------------------------
    # NEON API helpers (internal)
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        """GET JSON from NEON API with retries and rate-limit handling."""
        for attempt in range(3):
            try:
                resp = self._session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning("NEON: rate-limited, waiting %ds", wait)
                    time.sleep(wait)
                    if attempt == 2:
                        logger.error("NEON: rate-limited 3 times for %s — giving up", url)
                else:
                    raise
            except requests.RequestException as exc:
                if attempt == 2:
                    raise
                logger.warning("NEON: request error (attempt %d/3): %s", attempt + 1, exc)
                time.sleep(2 ** attempt)
        return {}

    def _get_product_availability(self, product_id: str, site_code: str) -> list[dict]:
        """Return availability entries for a product at a site."""
        url = f"{NEON_API_BASE}/products/{product_id}"
        data = self._get(url)
        for s in data.get("data", {}).get("siteCodes", []):
            if s.get("siteCode") == site_code:
                return [s]
        return []

    def _fetch_sample_table(
        self,
        product_id: str,
        site_code: str,
        year_month: str,
    ) -> list[dict]:
        """Fetch and parse sample-level CSV rows for a product/site/month."""
        url = f"{NEON_API_BASE}/data/{product_id}/{site_code}/{year_month}"
        package_data = self._get(url)
        files = package_data.get("data", {}).get("files", [])

        csv_files = [
            f for f in files
            if f.get("name", "").endswith(".csv")
            and "_readme" not in f.get("name", "").lower()
            and "_variables" not in f.get("name", "").lower()
            and "_validation" not in f.get("name", "").lower()
        ]
        rows: list[dict] = []
        for file_meta in csv_files:
            file_url = file_meta.get("url")
            if not file_url:
                continue
            try:
                resp = self._session.get(file_url, timeout=60)
                resp.raise_for_status()
                rows.extend(self._parse_csv(resp.text))
            except Exception as exc:
                logger.warning("NEON: could not fetch %s: %s", file_url, exc)
        return rows

    def _fetch_file_list(
        self,
        product_id: str,
        site_code: str,
        year_month: str,
    ) -> list[dict]:
        """Return all file metadata for a product/site/month."""
        url = f"{NEON_API_BASE}/data/{product_id}/{site_code}/{year_month}"
        data = self._get(url)
        return data.get("data", {}).get("files", [])

    @staticmethod
    def _parse_csv(text: str) -> list[dict]:
        import csv
        import io
        return [dict(row) for row in csv.DictReader(io.StringIO(text))]

    def _visit_number(self, site_code: str, year_month: str) -> int:
        """Chronological visit index (1-based) for site × month, in-memory."""
        if not hasattr(self, "_visit_counter"):
            self._visit_counter: dict[str, list[str]] = {}
        visits = self._visit_counter.setdefault(site_code, [])
        if year_month not in visits:
            visits.append(year_month)
            visits.sort()
        return visits.index(year_month) + 1
