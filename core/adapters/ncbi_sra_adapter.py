"""
ncbi_sra_adapter.py — NCBI SRA metagenome download and metadata retrieval.

Wraps the SRA Toolkit (prefetch + fasterq-dump) and the Entrez API to:
  1. Search SRA for soil metagenome samples matching config filters
  2. Download metadata (biosample attributes, environmental context)
  3. Download or stream FASTQ for processing
  4. Write results to the samples table via SoilDB

Prefer Aspera (ascp) over HTTP for bulk downloads — see README gotchas.

Usage (as a library):
  adapter = NCBISRAAdapter(config)
  for sample in adapter.search(biome="cropland", sequencing_type="16S"):
      yield sample
"""

from __future__ import annotations
import json
import logging
import shutil
import subprocess
import time
import urllib.request
import urllib.parse
from pathlib import Path
from typing import Iterator

from ..compute.metadata_normalizer import MetadataNormalizer

logger = logging.getLogger(__name__)

_ENTREZ_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_REQUEST_DELAY = 0.4  # seconds between Entrez requests (≤3 req/sec without key)
_normalizer = MetadataNormalizer()


def _get_json(url: str, timeout: int = 30) -> dict | list | None:
    try:
        time.sleep(_REQUEST_DELAY)
        req = urllib.request.Request(
            url, headers={"User-Agent": "soil-microbiome-pipeline/1.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        logger.debug("SRA API request failed: %s", exc)
        return None


class NCBISRAAdapter:
    """NCBI SRA adapter for searching and downloading soil metagenome samples."""
    SOURCE = "sra"

    def __init__(self, config: dict):
        self.config = config
        self._retmax = int(config.get("max_results", 500))
        self._api_key = config.get("ncbi_api_key", "")

    def _build_query(self, filters: dict) -> str:
        """Build Entrez query string from filter dict."""
        parts = []
        biome = filters.get("biome", self.config.get("biome", ""))
        if biome:
            parts.append(f"{biome}[All Fields]")
        seq_type = filters.get("sequencing_type", self.config.get("sequencing_type", ""))
        if seq_type == "16S":
            parts.append("16S ribosomal RNA[All Fields]")
        elif seq_type == "shotgun":
            parts.append("WGS[Strategy]")
        parts.append("SOIL[Source]")
        parts.append("metagenomic[Filter]")
        return " AND ".join(parts) if parts else "soil metagenome[All Fields]"

    def search(self, **filters) -> Iterator[dict]:
        """Yield sample metadata dicts matching SRA query filters.

        Each dict is normalized via MetadataNormalizer before yielding.
        """
        query = self._build_query(filters)
        url = (
            f"{_ENTREZ_BASE}/esearch.fcgi"
            f"?db=sra&term={urllib.parse.quote(query)}"
            f"&retmax={self._retmax}&retmode=json"
        )
        if self._api_key:
            url += f"&api_key={self._api_key}"

        data = _get_json(url)
        if not data:
            return

        id_list = data.get("esearchresult", {}).get("idlist", [])
        logger.info("SRA search returned %d IDs for query: %r", len(id_list), query)

        for sra_id in id_list:
            try:
                meta = self.download_metadata(sra_id)
                if meta:
                    yield meta
            except Exception as exc:
                logger.debug("Metadata fetch failed for SRA ID %s: %s", sra_id, exc)

    def download_metadata(self, accession: str) -> dict:
        """Fetch BioSample metadata for a single SRA accession."""
        url = (
            f"{_ENTREZ_BASE}/efetch.fcgi"
            f"?db=sra&id={urllib.parse.quote(accession)}&rettype=runinfo&retmode=json"
        )
        if self._api_key:
            url += f"&api_key={self._api_key}"

        data = _get_json(url)
        if not data:
            return {}

        # Try to extract sample attributes from efetch result
        raw_meta = {
            "sample_id": accession,
            "source": "sra",
        }
        if isinstance(data, dict):
            raw_meta.update(data)
        elif isinstance(data, list) and data:
            if isinstance(data[0], dict):
                raw_meta.update(data[0])

        # Normalize metadata through MetadataNormalizer
        normalized = _normalizer.normalize_sample(raw_meta)
        return normalized

    def download_fastq(
        self,
        accession: str,
        outdir: str,
        method: str = "fasterq-dump",
    ) -> list[str]:
        """Download FASTQ files via SRA Toolkit. Returns list of FASTQ paths.

        Requires: sra-tools (prefetch + fasterq-dump) in PATH.
        Install via: conda install -c bioconda sra-tools
        """
        import re as _re
        if not _re.match(r'^[A-Z]{1,3}\d{5,}$', accession):
            raise ValueError(f"Invalid SRA accession format: {accession!r}")

        outdir_path = Path(outdir)
        outdir_path.mkdir(parents=True, exist_ok=True)

        if not shutil.which("prefetch") or not shutil.which("fasterq-dump"):
            logger.warning(
                "SRA Toolkit not found — cannot download FASTQ for %s. "
                "Install via: conda install -c bioconda sra-tools",
                accession,
            )
            return []

        # Step 1: prefetch
        logger.info("Prefetching %s ...", accession)
        subprocess.run(
            ["prefetch", "--output-directory", str(outdir_path), accession],
            check=True, timeout=3600, capture_output=True,
        )

        # Step 2: fasterq-dump
        logger.info("Running fasterq-dump for %s ...", accession)
        subprocess.run(
            ["fasterq-dump", "--outdir", str(outdir_path),
             "--split-files", accession],
            check=True, timeout=7200, capture_output=True,
        )

        fastq_files = sorted(outdir_path.glob(f"{accession}*.fastq"))
        return [str(f) for f in fastq_files]
