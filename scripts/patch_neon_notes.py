"""
scripts/patch_neon_notes.py — Backfill FASTQ URLs into communities.notes for
existing NEON communities that were ingested without marker-gene amplicon URLs.

Problem addressed:
  Communities ingested via the legacy metagenomics product (DP1.10107.001) have
  communities.notes = '{}' or '[]' with no fastq_urls.  process_neon_16s.py
  skips them because (a) t0_pass is already 1 and (b) notes has no URLs.

Solution:
  1. Re-query the NEON API using the marker-gene product (DP1.10108.001) for
     each site/year to get 16S amplicon FASTQ URLs.
  2. UPDATE communities.notes with {"fastq_urls": [...]} for matching rows.
  3. RESET runs.t0_pass = 0 so process_neon_16s.py will re-classify them.

After this script completes, run:
  python scripts/process_neon_16s.py --all-sites --workers 8 \\
      --silva /data/pipeline/ref/16S_ref.fasta

Usage:
  python scripts/patch_neon_notes.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --staging /data/pipeline/staging \\
      --all-sites --years 2021 2022 2023 2024

  # Dry run (show what would change, no DB writes):
  python scripts/patch_neon_notes.py ... --dry-run

  # Specific sites only:
  python scripts/patch_neon_notes.py ... --sites HARV ORNL KONZ
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import csv
import io
import requests

from adapters.neon_adapter import NEONAdapter, PRODUCT_MARKER_GENE


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _load_patchable_sample_ids(conn: sqlite3.Connection) -> set[str]:
    """Return sample_ids that need their notes patched.

    Criteria: NEON source, t0_pass=1, top_genera is empty ([] or {}),
    phylum_profile is empty ({}).
    """
    rows = conn.execute("""
        SELECT s.sample_id
        FROM samples s
        JOIN communities c ON c.sample_id = s.sample_id
        JOIN runs r        ON r.sample_id  = s.sample_id
        WHERE s.source = 'neon'
          AND r.t0_pass = 1
          AND (c.top_genera   IN ('[]', '{}', '', 'null') OR c.top_genera IS NULL)
          AND (c.phylum_profile IN ('{}', '[]', '', 'null') OR c.phylum_profile IS NULL)
    """).fetchall()
    return {row["sample_id"] for row in rows}


def _patch_community_notes(
    conn: sqlite3.Connection,
    sample_id: str,
    fastq_urls: list[str],
    dry_run: bool,
) -> bool:
    """Update communities.notes and reset t0_pass=0.  Returns True if changed."""
    if not fastq_urls:
        return False

    notes_json = json.dumps({"fastq_urls": fastq_urls})

    if dry_run:
        logger.info("[DRY RUN] %s: would set %d FASTQ URLs, reset t0_pass=0",
                    sample_id, len(fastq_urls))
        return True

    conn.execute(
        "UPDATE communities SET notes = ? WHERE sample_id = ?",
        (notes_json, sample_id),
    )
    conn.execute(
        "UPDATE runs SET t0_pass = 0 WHERE sample_id = ? AND t0_pass = 1",
        (sample_id,),
    )
    return True


# ---------------------------------------------------------------------------
# NEON CSV parsing helpers (mirrors ingest_neon_biom.py logic)
# ---------------------------------------------------------------------------

def _fetch_csv(adapter: NEONAdapter, site: str,
               year_month: str, keyword: str) -> list[dict]:
    """Download and parse a NEON marker-gene CSV identified by keyword.

    Mirrors the logic in ingest_neon_biom.py: prefers 'expanded' package
    files but falls back to any matching file.
    """
    try:
        files = adapter._fetch_file_list(PRODUCT_MARKER_GENE, site, year_month)
    except Exception as exc:
        logger.warning("File list failed for %s %s: %s", site, year_month, exc)
        return []

    expanded = [f for f in files
                if keyword in f.get("name", "") and "expanded" in f.get("name", "")]
    fallback  = [f for f in files
                 if keyword in f.get("name", "") and f not in expanded]
    candidates = expanded or fallback

    for f in candidates:
        try:
            r = requests.get(f.get("url", ""), timeout=30)
            r.raise_for_status()
            return list(csv.DictReader(io.StringIO(r.text)))
        except Exception as exc:
            logger.warning("Fetch failed for %s: %s", f.get("name", ""), exc)
    return []


def _parse_raw_file_rows(rows: list[dict]) -> dict[str, list[str]]:
    """Return {dnaSampleID: [fastq_url, ...]} — same as ingest_neon_biom.py."""
    out: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        dna_id = row.get("dnaSampleID") or ""
        url    = row.get("rawDataFilePath") or ""
        if dna_id and url:
            out[dna_id].append(url)
    return dict(out)


def _parse_extraction_rows(rows: list[dict]) -> dict[str, str]:
    """Return {dnaSampleID: sample_id} using the same convention as ingest_neon_biom.py.

    sample_id = "neon." + dnaSampleID  (matches ingest_neon_biom._parse_extraction_rows)
    """
    result = {}
    for row in rows:
        dna_id = row.get("dnaSampleID") or row.get("sampleID") or ""
        if not dna_id:
            continue
        sample_id = f"neon.{dna_id}"  # no slash replacement — matches ingest
        result[dna_id] = sample_id
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",      required=True,
                   help="Path to soil_microbiome.db")
    p.add_argument("--staging", required=True,
                   help="Cache dir for downloaded NEON files")
    p.add_argument("--token",   default=os.environ.get("NEON_API_TOKEN", ""),
                   help="NEON API token (optional; increases rate limits)")
    p.add_argument("--sites",   nargs="+",
                   default=["HARV", "ORNL", "KONZ", "WOOD", "CPER",
                             "UNDE", "STEI", "DCFS", "NOGP", "CLBJ", "OAES"],
                   help="NEON site codes to process (default: 11 key sites)")
    p.add_argument("--all-sites", action="store_true",
                   help="Process ALL NEON sites found via the API")
    p.add_argument("--years",   type=int, nargs="+",
                   default=[2021, 2022, 2023, 2024],
                   help="Calendar years to scan (default: 2021-2024)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would change without writing to DB")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    conn = _db_connect(args.db)
    patchable = _load_patchable_sample_ids(conn)
    logger.info("Communities needing FASTQ URL patch: %d", len(patchable))

    if not patchable:
        logger.info("Nothing to do.")
        return

    adapter = NEONAdapter(token=args.token, data_dir=args.staging)

    # Resolve site list
    if args.all_sites:
        try:
            sites = [s["site_id"] for s in adapter.iter_sites()]
            logger.info("All-sites mode: found %d NEON sites", len(sites))
        except Exception as exc:
            logger.error("Could not fetch site list: %s", exc)
            sys.exit(1)
    else:
        sites = args.sites

    n_patched = n_no_url = n_not_in_db = 0

    for site_code in sites:
        logger.info("── Site: %s", site_code)
        try:
            avail = adapter._get_product_availability(PRODUCT_MARKER_GENE, site_code)
        except Exception as exc:
            logger.warning("  Availability check failed: %s", exc)
            continue

        for release_entry in avail:
            for year_month in release_entry.get("availableMonths", []):
                year = int(year_month[:4])
                if args.years and year not in args.years:
                    continue

                extraction_rows = _fetch_csv(adapter, site_code, year_month, "soilDnaExtraction")
                raw_rows        = _fetch_csv(adapter, site_code, year_month, "soilRawDataFiles")

                if not extraction_rows:
                    continue

                dna_to_sample = _parse_extraction_rows(extraction_rows)
                fastq_map     = _parse_raw_file_rows(raw_rows)

                for dna_id, sample_id in dna_to_sample.items():
                    if sample_id not in patchable:
                        n_not_in_db += 1
                        continue

                    fastq_urls = fastq_map.get(dna_id, [])
                    if not fastq_urls:
                        logger.debug("%s: no FASTQ URLs in rawDataFiles", sample_id)
                        n_no_url += 1
                        continue

                    changed = _patch_community_notes(conn, sample_id, fastq_urls,
                                                     args.dry_run)
                    if changed:
                        n_patched += 1
                        patchable.discard(sample_id)  # don't re-patch if seen again

        if not args.dry_run:
            conn.commit()

    if not args.dry_run:
        conn.commit()

    print("\n=== patch_neon_notes complete ===")
    print(f"  Communities patched     : {n_patched}")
    print(f"  No amplicon URL found   : {n_no_url}")
    print(f"  Not in patchable set    : {n_not_in_db}")
    remaining = len(patchable)
    print(f"  Still unpatched in DB   : {remaining}")
    if remaining and not args.dry_run:
        logger.warning("%d communities remain without FASTQ URLs (sites not in "
                       "marker-gene product). They cannot be 16S-classified.", remaining)
    if n_patched > 0 and not args.dry_run:
        print("\nNext step:")
        print("  python scripts/process_neon_16s.py --all-sites --workers 8 "
              "--silva /data/pipeline/ref/16S_ref.fasta")


if __name__ == "__main__":
    main()
