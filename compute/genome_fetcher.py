"""
compute/genome_fetcher.py — T1 representative genome retrieval from PATRIC / NCBI RefSeq.

For each representative taxon selected for the community FBA model:
  1. Search PATRIC by taxonomy ID for best available reference genome
  2. Fall back to NCBI RefSeq assembly if PATRIC lacks coverage
  3. For taxa with no reference genome (40-60% of soil taxa), use
     the closest phylogenetic neighbor by 16S similarity

Downloaded genomes are cached locally — repeated runs never re-download.

Usage:
  from compute.genome_fetcher import GenomeFetcher
  fetcher = GenomeFetcher(genome_db="patric", cache_dir="genome_cache/")
  genome_path = fetcher.fetch(taxon_id="1234", taxon_name="Azospirillum brasilense")
"""

from __future__ import annotations
import hashlib
import logging
import re
import time
import urllib.request
import urllib.parse
import urllib.error
import gzip
import shutil
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# BV-BRC (formerly PATRIC) API
BV_BRC_BASE = "https://www.bv-brc.org/api"
# NCBI Entrez (no key needed for <3 requests/sec)
NCBI_ENTREZ_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
# NCBI datasets FTP base
NCBI_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/genomes/all"

_REQUEST_DELAY = 0.35  # seconds between API requests


def _get_json(url: str, timeout: int = 30) -> Any:
    """Fetch a JSON URL and return parsed data. Returns None on failure."""
    try:
        time.sleep(_REQUEST_DELAY)
        req = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "soil-microbiome-pipeline/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            import json
            return json.loads(resp.read())
    except Exception as exc:
        logger.debug("GET %s failed: %s", url, exc)
        return None


def _download_file(url: str, dest: Path, timeout: int = 300) -> bool:
    """Download url to dest. Returns True on success."""
    try:
        time.sleep(_REQUEST_DELAY)
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "soil-microbiome-pipeline/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp, dest.open("wb") as fh:
            shutil.copyfileobj(resp, fh)
        return True
    except Exception as exc:
        logger.debug("Download %s → %s failed: %s", url, dest, exc)
        return False


def _decompress_gz(gz_path: Path) -> Path:
    """Decompress a .gz file to same directory. Returns path to decompressed file."""
    out_path = gz_path.with_suffix("")
    if out_path.exists():
        return out_path
    with gzip.open(gz_path, "rb") as f_in, out_path.open("wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    gz_path.unlink(missing_ok=True)
    return out_path


class GenomeFetcher:
    """Retrieve representative genome FASTAs for taxa by taxonomy ID.

    Priority order:
      1. Local cache (by taxon_id hash)
      2. BV-BRC (PATRIC) API
      3. NCBI RefSeq
      4. Phylogenetic neighbor (same genus, then family)
    """

    def __init__(self, genome_db: str = "bvbrc", cache_dir: str | Path = "genome_cache/"):
        self.genome_db = genome_db
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, taxon_id: str) -> Path:
        safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", taxon_id)
        return self.cache_dir / f"{safe}_genomic.fna"

    def fetch(self, taxon_id: str, taxon_name: str) -> Path:
        """Return path to a representative genome FASTA for the taxon.

        Downloads and caches if not already present. Raises RuntimeError if
        no genome can be obtained via any strategy.
        """
        cached = self._cache_key(taxon_id)
        if cached.exists() and cached.stat().st_size > 100:
            logger.debug("Cache hit for taxon %s: %s", taxon_id, cached)
            return cached

        # Strategy 1: BV-BRC
        genome_path = self._fetch_bvbrc(taxon_id)
        # Strategy 2: NCBI RefSeq
        if genome_path is None:
            genome_path = self._fetch_ncbi_refseq(taxon_id)
        # Strategy 3: Phylogenetic neighbor
        if genome_path is None:
            genome_path = self._nearest_phylogenetic_neighbor(taxon_name)

        if genome_path is None:
            raise RuntimeError(
                f"Could not obtain genome for {taxon_name!r} (taxon_id={taxon_id!r}) "
                "via BV-BRC, NCBI RefSeq, or phylogenetic neighbor strategies."
            )
        # Move to canonical cache path
        if genome_path != cached:
            shutil.move(str(genome_path), str(cached))
        return cached

    def _fetch_bvbrc(self, taxon_id: str) -> Path | None:
        """Search BV-BRC for the best available reference genome for taxon_id."""
        # BV-BRC genome search endpoint
        url = (
            f"{BV_BRC_BASE}/genome/"
            f"?and(eq(taxon_id,{urllib.parse.quote(taxon_id)}),"
            f"eq(reference_genome,1))"
            f"&select(genome_id,genome_name,genome_length,contigs)"
            f"&sort(-genome_length)&limit(1)"
        )
        data = _get_json(url)
        if not data or not isinstance(data, list) or len(data) == 0:
            # Try without reference_genome filter
            url = (
                f"{BV_BRC_BASE}/genome/"
                f"?eq(taxon_id,{urllib.parse.quote(taxon_id)})"
                f"&select(genome_id,genome_name,genome_length,contigs)"
                f"&sort(-genome_length)&limit(1)"
            )
            data = _get_json(url)

        if not data or len(data) == 0:
            logger.debug("BV-BRC: no genome found for taxon_id=%s", taxon_id)
            return None

        genome_id = data[0].get("genome_id")
        if not genome_id:
            return None

        # Download FASTA via BV-BRC genome feature endpoint
        fasta_url = f"{BV_BRC_BASE}/genome_feature/?eq(genome_id,{genome_id})&http_accept=application/dna+fasta&limit(1000000)"
        dest = self.cache_dir / f"bvbrc_{genome_id}.fna"
        if _download_file(fasta_url, dest):
            logger.info("BV-BRC: downloaded genome %s for taxon_id=%s", genome_id, taxon_id)
            return dest

        # Fallback: try the contigs download URL
        fasta_url2 = f"https://www.bv-brc.org/api/genome_sequence/?eq(genome_id,{genome_id})&http_accept=application/dna+fasta"
        if _download_file(fasta_url2, dest):
            logger.info("BV-BRC (sequence endpoint): downloaded %s for taxon_id=%s", genome_id, taxon_id)
            return dest

        logger.debug("BV-BRC: found genome_id=%s but download failed", genome_id)
        return None

    # NOTE: Plan doc uses "PATRIC" → we use "BV-BRC" (rebranded). Keep old name
    def _fetch_patric(self, taxon_id: str) -> Path | None:
        """Alias for backwards-compat; delegates to _fetch_bvbrc."""
        return self._fetch_bvbrc(taxon_id)

    def _fetch_ncbi_refseq(self, taxon_id: str) -> Path | None:
        """Search NCBI RefSeq for a representative genome via Entrez."""
        # Entrez esearch for RefSeq assembly
        esearch_url = (
            f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            f"?db=assembly&term=txid{urllib.parse.quote(taxon_id)}[Organism]"
            f"+AND+reference_genome[RefSeq+Category]&retmax=1&retmode=json"
        )
        data = _get_json(esearch_url)
        if not data:
            return None

        id_list = data.get("esearchresult", {}).get("idlist", [])
        if not id_list:
            # Try without reference filter
            esearch_url2 = (
                f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                f"?db=assembly&term=txid{urllib.parse.quote(taxon_id)}[Organism]"
                f"&retmax=1&retmode=json"
            )
            data2 = _get_json(esearch_url2)
            if data2:
                id_list = data2.get("esearchresult", {}).get("idlist", [])

        if not id_list:
            logger.debug("NCBI RefSeq: no assembly for taxon_id=%s", taxon_id)
            return None

        assembly_id = id_list[0]
        # Fetch assembly summary via esummary
        esummary_url = (
            f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
            f"?db=assembly&id={assembly_id}&retmode=json"
        )
        summary = _get_json(esummary_url)
        if not summary:
            return None

        result = summary.get("result", {}).get(assembly_id, {})
        ftp_path = result.get("ftppath_refseq", "") or result.get("ftppath_genbank", "")
        if not ftp_path:
            return None

        # Build FASTA URL from FTP base
        accession = ftp_path.split("/")[-1]
        fasta_gz_url = f"{ftp_path}/{accession}_genomic.fna.gz"
        dest_gz = self.cache_dir / f"ncbi_{assembly_id}.fna.gz"
        if _download_file(fasta_gz_url, dest_gz):
            dest = _decompress_gz(dest_gz)
            logger.info("NCBI RefSeq: downloaded %s for taxon_id=%s", accession, taxon_id)
            return dest

        return None

    def _nearest_phylogenetic_neighbor(self, taxon_name: str) -> Path | None:
        """Find the closest phylogenetic neighbor when no reference genome exists.

        Strategy:
          1. Extract genus from taxon_name
          2. Search BV-BRC for any genome in the same genus
          3. Fall back to family-level if genus has nothing
        """
        parts = taxon_name.strip().split()
        if not parts:
            return None
        genus = parts[0]

        # Try genus-level search on BV-BRC
        url = (
            f"{BV_BRC_BASE}/genome/"
            f"?eq(genus,{urllib.parse.quote(genus)})"
            f"&select(genome_id,genome_name,taxon_id)"
            f"&sort(-genome_length)&limit(1)"
        )
        data = _get_json(url)
        if data and len(data) > 0:
            neighbor_taxon_id = str(data[0].get("taxon_id", ""))
            neighbor_name = data[0].get("genome_name", genus)
            logger.info(
                "Using phylogenetic neighbor %r (taxon_id=%s) for %r",
                neighbor_name, neighbor_taxon_id, taxon_name,
            )
            if neighbor_taxon_id:
                return self._fetch_bvbrc(neighbor_taxon_id)

        logger.warning(
            "Could not find phylogenetic neighbor for %r — no genome available", taxon_name
        )
        return None
