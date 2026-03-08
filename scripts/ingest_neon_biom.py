"""
scripts/ingest_neon_biom.py — Ingest NEON soil microbiome sample metadata.

NEON DP1.10107.001 distributes sample metadata as CSV files + raw FASTQ links
(not pre-processed BIOM files). This script:

  1. Fetches NEON site metadata (lat/lon, biome) via the NEON Data API
  2. Fetches soil chemistry data (DP1.10086.001) per site/month
  3. Parses mms_metagenomeDnaExtraction CSV -> creates sample records with real geo/env data
  4. Parses mms_rawDataFiles CSV -> stores FASTQ URLs as notes for future SRA ingest
  5. Creates placeholder community records (phylum_profile populated after processing)
  6. Sets t0_pass=0 — pending FASTQ processing by QIIME2/DADA2

Raw FASTQ files for 16S community processing live at:
  https://storage.neonscience.org/neon-microbial-raw-seq-files/

Usage:
  python scripts/ingest_neon_biom.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --staging /data/pipeline/staging \\
      --sites HARV ORNL KONZ KONA WOOD CPER SJER TALL OSBS \\
      --years 2019 2020 2021

  python scripts/ingest_neon_biom.py --db ... --staging ... --all-sites --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sys
from pathlib import Path
from typing import Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adapters.neon_adapter import NEONAdapter, PRODUCT_MICROBIOME


# ---------------------------------------------------------------------------
# CSV parsing helpers
# ---------------------------------------------------------------------------

def _fetch_csv(adapter: NEONAdapter, product: str, site: str,
               year_month: str, keyword: str) -> list[dict]:
    """Download and parse a NEON CSV file identified by keyword in filename."""
    import requests
    try:
        files = adapter._fetch_file_list(product, site, year_month)
    except Exception as exc:
        logger.warning("  File list failed for %s %s %s: %s", product, site, year_month, exc)
        return []
    for f in files:
        name = f.get("name", "")
        if keyword in name and "expanded" in name:
            try:
                r = requests.get(f.get("url", ""), timeout=30)
                r.raise_for_status()
                return list(csv.DictReader(io.StringIO(r.text)))
            except Exception as exc:
                logger.warning("  Fetch failed for %s: %s", name, exc)
                return []
    return []


def _parse_extraction_rows(rows: list[dict], site_meta: dict, year_month: str) -> Iterator[dict]:
    """Convert NEON mms_metagenomeDnaExtraction rows -> canonical sample dicts."""
    for row in rows:
        dna_id = row.get("dnaSampleID") or row.get("sampleID") or ""
        if not dna_id:
            continue
        sample_type = row.get("sampleType", "").lower()
        fraction = "bulk"
        if "rhizo" in sample_type:
            fraction = "rhizosphere"
        elif "litter" in sample_type or "o horizon" in sample_type:
            fraction = "litter"
        collect_date = row.get("collectDate") or row.get("endDate") or (year_month + "-01")
        yield {
            "sample_id":         f"neon.{dna_id}",
            "source_id":         dna_id,
            "biome":             site_meta.get("biome", "terrestrial biome"),
            "latitude":          site_meta.get("latitude"),
            "longitude":         site_meta.get("longitude"),
            "country":           "USA",
            "site_id":           row.get("siteID") or site_meta.get("site_id", ""),
            "visit_number":      1,
            "sampling_date":     collect_date[:10] if collect_date else year_month + "-01",
            "sampling_fraction": fraction,
        }


def _parse_sequencing_rows(rows: list[dict]) -> dict[str, dict]:
    """Return {dnaSampleID: {ncbi_project_id, ...}} from sequencing CSV."""
    return {
        (row.get("dnaSampleID") or ""): {
            "ncbi_project_id": row.get("ncbiProjectID"),
            "instrument":      row.get("instrument_model"),
        }
        for row in rows if row.get("dnaSampleID")
    }


def _parse_raw_file_rows(rows: list[dict]) -> dict[str, list[str]]:
    """Return {dnaSampleID: [fastq_url, ...]} from rawDataFiles CSV."""
    out: dict[str, list[str]] = {}
    for row in rows:
        dna_id = row.get("dnaSampleID") or ""
        url = row.get("rawDataFilePath") or ""
        if dna_id and url:
            out.setdefault(dna_id, []).append(url)
    return out


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_connect(db_path: str):
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _upsert_sample(conn, sample_id: str, meta: dict) -> None:
    conn.execute(
        """
        INSERT INTO samples (
            sample_id, source, source_id, biome, sequencing_type,
            latitude, longitude, country, site_id, visit_number,
            soil_ph, temperature_c, precipitation_mm, organic_matter_pct,
            clay_pct, sand_pct, bulk_density, total_nitrogen_ppm,
            available_p_ppm, cec, moisture_pct, soil_texture,
            sampling_fraction, sampling_date
        ) VALUES (
            :sample_id, 'neon', :source_id, :biome, '16S',
            :latitude, :longitude, :country, :site_id, :visit_number,
            :soil_ph, :temperature_c, :precipitation_mm, :organic_matter_pct,
            :clay_pct, :sand_pct, :bulk_density, :total_nitrogen_ppm,
            :available_p_ppm, :cec, :moisture_pct, :soil_texture,
            :sampling_fraction, :sampling_date
        )
        ON CONFLICT(sample_id) DO UPDATE SET
            soil_ph       = COALESCE(excluded.soil_ph, samples.soil_ph),
            temperature_c = COALESCE(excluded.temperature_c, samples.temperature_c),
            latitude      = COALESCE(excluded.latitude, samples.latitude),
            longitude     = COALESCE(excluded.longitude, samples.longitude),
            sampling_date = excluded.sampling_date
        """,
        {
            "sample_id":          sample_id,
            "source_id":          meta.get("source_id", ""),
            "biome":              meta.get("biome", "terrestrial biome"),
            "latitude":           meta.get("latitude"),
            "longitude":          meta.get("longitude"),
            "country":            meta.get("country", "USA"),
            "site_id":            meta.get("site_id"),
            "visit_number":       meta.get("visit_number", 1),
            "soil_ph":            meta.get("soil_ph"),
            "temperature_c":      meta.get("temperature_c"),
            "precipitation_mm":   meta.get("precipitation_mm"),
            "organic_matter_pct": meta.get("organic_matter_pct"),
            "clay_pct":           meta.get("clay_pct"),
            "sand_pct":           meta.get("sand_pct"),
            "bulk_density":       meta.get("bulk_density"),
            "total_nitrogen_ppm": meta.get("total_nitrogen_ppm"),
            "available_p_ppm":    meta.get("available_p_ppm"),
            "cec":                meta.get("cec"),
            "moisture_pct":       meta.get("moisture_pct"),
            "soil_texture":       meta.get("soil_texture"),
            "sampling_fraction":  meta.get("sampling_fraction", "bulk"),
            "sampling_date":      meta.get("sampling_date"),
        },
    )


def _upsert_community_placeholder(conn, sample_id: str, fastq_urls: list[str]) -> int:
    """Create a community record with empty profiles (to be updated after FASTQ processing)."""
    row = conn.execute(
        "SELECT community_id FROM communities WHERE sample_id = ?", (sample_id,)
    ).fetchone()
    if row:
        return row[0]
    notes = json.dumps({"fastq_urls": fastq_urls}) if fastq_urls else None
    cursor = conn.execute(
        """INSERT INTO communities (sample_id, phylum_profile, top_genera, notes)
           VALUES (?, '{}', '[]', ?)""",
        (sample_id, notes),
    )
    return cursor.lastrowid


def _upsert_run_pending(conn, sample_id: str, community_id: int,
                         target_id: str, machine_id: str) -> None:
    """Create a runs row with t0_pass=0 — pending FASTQ processing."""
    if not conn.execute(
        "SELECT run_id FROM runs WHERE sample_id = ? LIMIT 1", (sample_id,)
    ).fetchone():
        conn.execute(
            """INSERT INTO runs (sample_id, community_id, target_id, t0_pass, tier_reached, machine_id)
               VALUES (?, ?, ?, 0, 0, ?)""",
            (sample_id, community_id, target_id, machine_id),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",        required=True)
    p.add_argument("--staging",   required=True, help="Cache dir for downloaded files")
    p.add_argument("--token",     default=os.environ.get("NEON_API_TOKEN", ""),
                   help="NEON API token (free at neonscience.org — increases rate limits)")
    p.add_argument("--sites",     nargs="+",
                   default=["HARV", "ORNL", "KONZ", "KONA", "WOOD", "CPER",
                            "SJER", "TALL", "OSBS", "SCBI", "BLAN", "BART"])
    p.add_argument("--all-sites", action="store_true",
                   help="Process all NEON sites with microbiome data")
    p.add_argument("--years",     nargs="+", type=int,
                   help="Restrict to specific years (e.g. 2019 2020 2021)")
    p.add_argument("--target-id", default="neon_soil_microbiome")
    p.add_argument("--dry-run",   action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    adapter = NEONAdapter(token=args.token, data_dir=args.staging)
    machine_id = os.uname().nodename
    conn = None if args.dry_run else _db_connect(args.db)

    logger.info("Loading NEON site metadata...")
    site_index: dict[str, dict] = {}
    try:
        for s in adapter.iter_sites():
            site_index[s["site_id"]] = s
    except Exception as exc:
        logger.warning("Could not load site metadata: %s", exc)

    sites = list(site_index.keys()) if args.all_sites else args.sites
    logger.info("Processing %d NEON sites: %s", len(sites), sites)

    n_samples = 0
    n_inserted = 0
    n_skipped = 0

    for site_code in sites:
        logger.info("── Site: %s", site_code)
        site_meta = site_index.get(site_code, {"site_id": site_code})

        try:
            avail = adapter._get_product_availability(PRODUCT_MICROBIOME, site_code)
        except Exception as exc:
            logger.warning("  Availability check failed for %s: %s", site_code, exc)
            continue

        for release_entry in avail:
            for year_month in release_entry.get("availableMonths", []):
                year = int(year_month[:4])
                if args.years and year not in args.years:
                    continue
                logger.info("  %s — processing %s", site_code, year_month)

                try:
                    soil_chem = adapter.get_soil_chemistry(site_code, year_month)
                except Exception:
                    soil_chem = {}

                extraction_rows = _fetch_csv(
                    adapter, PRODUCT_MICROBIOME, site_code, year_month,
                    "metagenomeDnaExtraction"
                )
                if not extraction_rows:
                    logger.info("  No extraction rows for %s %s", site_code, year_month)
                    continue

                seq_rows  = _fetch_csv(adapter, PRODUCT_MICROBIOME, site_code, year_month, "metagenomeSequencing")
                raw_rows  = _fetch_csv(adapter, PRODUCT_MICROBIOME, site_code, year_month, "rawDataFiles")
                seq_meta  = _parse_sequencing_rows(seq_rows)
                fastq_map = _parse_raw_file_rows(raw_rows)

                for sample in _parse_extraction_rows(extraction_rows, site_meta, year_month):
                    n_samples += 1
                    dna_id = sample["source_id"]
                    sample.update({k: v for k, v in soil_chem.items() if v is not None})
                    sm = seq_meta.get(dna_id, {})
                    fastq_urls = fastq_map.get(dna_id, [])

                    if args.dry_run:
                        print(f"[DRY RUN] {sample['sample_id']}")
                        print(f"  site={site_code}  date={sample['sampling_date']}")
                        print(f"  lat={sample.get('latitude')}  lon={sample.get('longitude')}")
                        print(f"  soil_ph={sample.get('soil_ph')}  fraction={sample['sampling_fraction']}")
                        print(f"  fastq_urls={len(fastq_urls)}  ncbi_project={sm.get('ncbi_project_id')}")
                        n_inserted += 1
                        continue

                    try:
                        _upsert_sample(conn, sample["sample_id"], sample)
                        cid = _upsert_community_placeholder(conn, sample["sample_id"], fastq_urls)
                        _upsert_run_pending(conn, sample["sample_id"], cid, args.target_id, machine_id)
                        conn.commit()
                        n_inserted += 1
                    except Exception as exc:
                        logger.error("  DB insert failed for %s: %s", sample["sample_id"], exc)
                        if conn:
                            conn.rollback()
                        n_skipped += 1

    print(f"\n=== NEON metadata ingest complete ===")
    print(f"  Sites processed    : {len(sites)}")
    print(f"  Samples found      : {n_samples}")
    print(f"  Inserted / dry-run : {n_inserted}")
    print(f"  Skipped (errors)   : {n_skipped}")
    if not args.dry_run and n_inserted > 0:
        print(f"\nNote: Community profiles are empty (t0_pass=0).")
        print(f"Process raw FASTQs with QIIME2/DADA2 to populate 16S OTU tables.")


if __name__ == "__main__":
    main()
