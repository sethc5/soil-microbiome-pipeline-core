"""
scripts/ingest_neon_biom.py — Download NEON BIOM files and populate the DB.

NEON DP1.10107.001 ships pre-demultiplexed, OTU-clustered BIOM files for
every site × month combination. Unlike raw FASTQ, these need no QIIME2 —
biom-format (already in venv) reads them directly.

For each NEON site/month this script:
  1. Downloads BIOM file(s) via NEONAdapter.download_sequence_data()
  2. Parses OTU table with biom-format: extracts phylum/genus profiles
  3. Upserts samples table (geo + soil chemistry from DP1.10086.001)
  4. Updates communities table: otu_table_path, phylum_profile, top_genera
  5. Sets t0_pass = 1 in runs table (ready for T1 FBA)

Already-downloaded files are cached in --staging-dir and skipped on re-run.

Usage:
  python scripts/ingest_neon_biom.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --staging /data/pipeline/staging \\
      --sites HARV ORNL KONZ KONA WOOD CPER SJER TALL OSBS \\
      --token $NEON_API_TOKEN

  # All sites with microbiome data (slow — ~47 sites):
  python scripts/ingest_neon_biom.py --db ... --staging ... --all-sites
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adapters.neon_adapter import NEONAdapter


# ---------------------------------------------------------------------------
# BIOM parsing helpers
# ---------------------------------------------------------------------------

def _parse_biom_file(biom_path: Path) -> dict:
    """
    Parse a BIOM file (v1 JSON or v2 HDF5) and return:
      {
        otu_counts:     {otu_id: total_count},
        taxonomy:       {otu_id: lineage_string},
        phylum_profile: {phylum: rel_abundance},
        top_genera:     [{"name": str, "rel_abundance": float}, ...],
        n_otus:         int,
        total_seqs:     int,
      }
    """
    try:
        import biom
        table = biom.load_table(str(biom_path))
    except Exception as exc:
        logger.error("Failed to load BIOM file %s: %s", biom_path, exc)
        return {}

    # Total per-OTU counts (sum across all samples in file)
    otu_counts: dict[str, float] = {}
    def _collect(val, otu_id, _): otu_counts[otu_id] = float(val.sum())
    table.iterate(_collect, axis="observation")  # type: ignore[arg-type]

    total = sum(otu_counts.values()) or 1.0

    # Extract taxonomy from observation metadata
    otu_taxonomy: dict[str, str] = {}
    for otu_id in table.ids(axis="observation"):
        meta = table.metadata(otu_id, axis="observation") or {}
        tax = meta.get("taxonomy") or meta.get("Taxonomy") or meta.get("taxon") or []
        if isinstance(tax, (list, tuple)):
            lineage = "; ".join(str(t).strip() for t in tax)
        else:
            lineage = str(tax)
        otu_taxonomy[otu_id] = lineage

    # Roll up into phylum_profile and genus totals
    phylum_totals: dict[str, float] = {}
    genus_totals:  dict[str, float] = {}

    for otu_id, count in otu_counts.items():
        rel = count / total
        lineage = otu_taxonomy.get(otu_id, "")
        parts = [p.strip() for p in lineage.split(";")]

        # Phyloseq-style:  k__Bacteria; p__Proteobacteria; c__...; o__; f__; g__; s__
        def _strip_prefix(s: str) -> str:
            return s.split("__", 1)[-1].strip() if "__" in s else s.strip()

        phylum = _strip_prefix(parts[1]) if len(parts) > 1 and parts[1] else "Unknown"
        genus  = _strip_prefix(parts[5]) if len(parts) > 5 and parts[5] else None

        phylum_totals[phylum] = phylum_totals.get(phylum, 0.0) + rel
        if genus and genus not in ("", "g__"):
            genus_totals[genus] = genus_totals.get(genus, 0.0) + rel

    phylum_sum = sum(phylum_totals.values()) or 1.0
    phylum_profile = {k: round(v / phylum_sum, 6) for k, v in phylum_totals.items()}
    top_genera = sorted(
        [{"name": g, "rel_abundance": round(a, 6)} for g, a in genus_totals.items()],
        key=lambda x: -x["rel_abundance"],
    )[:50]

    return {
        "otu_counts":     otu_counts,
        "taxonomy":       otu_taxonomy,
        "phylum_profile": phylum_profile,
        "top_genera":     top_genera,
        "n_otus":         len(otu_counts),
        "total_seqs":     int(total),
    }


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
            latitude, longitude, country, climate_zone, site_id, visit_number,
            soil_ph, temperature_c, precipitation_mm, organic_matter_pct,
            clay_pct, sand_pct, bulk_density, total_nitrogen_ppm,
            available_p_ppm, cec, moisture_pct, soil_texture,
            sampling_fraction, sampling_date
        ) VALUES (
            :sample_id, 'neon', :source_id, :biome, '16S',
            :latitude, :longitude, :country, :climate_zone, :site_id, :visit_number,
            :soil_ph, :temperature_c, :precipitation_mm, :organic_matter_pct,
            :clay_pct, :sand_pct, :bulk_density, :total_nitrogen_ppm,
            :available_p_ppm, :cec, :moisture_pct, :soil_texture,
            :sampling_fraction, :sampling_date
        )
        ON CONFLICT(sample_id) DO UPDATE SET
            soil_ph            = excluded.soil_ph,
            temperature_c      = excluded.temperature_c,
            latitude           = excluded.latitude,
            longitude          = excluded.longitude
        """,
        {
            "sample_id":          sample_id,
            "source_id":          meta.get("source_id", ""),
            "biome":              meta.get("biome", "terrestrial biome"),
            "latitude":           meta.get("latitude"),
            "longitude":          meta.get("longitude"),
            "country":            meta.get("country", "USA"),
            "climate_zone":       meta.get("climate_zone"),
            "site_id":            meta.get("site_id"),
            "visit_number":       meta.get("visit_number"),
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


def _upsert_community(conn, sample_id: str, biom_data: dict, biom_path: Path) -> int:
    row = conn.execute(
        "SELECT community_id FROM communities WHERE sample_id = ?", (sample_id,)
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE communities SET
                otu_table_path = ?,
                phylum_profile = ?,
                top_genera     = ?,
                observed_otus  = ?
            WHERE community_id = ?
            """,
            (
                str(biom_path),
                json.dumps(biom_data.get("phylum_profile", {})),
                json.dumps(biom_data.get("top_genera", [])),
                biom_data.get("n_otus", 0),
                row[0],
            ),
        )
        return row[0]
    cursor = conn.execute(
        """
        INSERT INTO communities (
            sample_id, otu_table_path, phylum_profile, top_genera,
            observed_otus
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            sample_id,
            str(biom_path),
            json.dumps(biom_data.get("phylum_profile", {})),
            json.dumps(biom_data.get("top_genera", [])),
            biom_data.get("n_otus", 0),
        ),
    )
    return cursor.lastrowid


def _upsert_run_t0(conn, sample_id: str, community_id: int, target_id: str, machine_id: str) -> None:
    existing = conn.execute(
        "SELECT run_id FROM runs WHERE sample_id = ? LIMIT 1",
        (sample_id,),
    ).fetchone()
    if not existing:
        conn.execute(
            """
            INSERT INTO runs (
                sample_id, community_id, target_id,
                t0_pass, tier_reached, machine_id
            ) VALUES (?, ?, ?, 1, 0, ?)
            """,
            (sample_id, community_id, target_id, machine_id),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",           required=True)
    p.add_argument("--staging",      required=True,
                   help="Local directory for downloaded BIOM files")
    p.add_argument("--token",        default=os.environ.get("NEON_API_TOKEN", ""),
                   help="NEON API token (or set NEON_API_TOKEN env var)")
    p.add_argument("--sites",        nargs="+",
                   default=["HARV", "ORNL", "KONZ", "KONA", "WOOD", "CPER",
                             "SJER", "TALL", "OSBS", "SCBI", "MLBS", "SERC",
                             "GUAN", "LENO", "DSNY"],
                   help="NEON site codes to ingest")
    p.add_argument("--all-sites",    action="store_true",
                   help="Discover and ingest all sites with microbiome data")
    p.add_argument("--years",        nargs="+", type=int,
                   default=None, help="Limit to specific years")
    p.add_argument("--seq-type",     default="16S", choices=["16S", "ITS"])
    p.add_argument("--target-id",    default="neon_soil_bnf")
    p.add_argument("--dry-run",      action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    adapter = NEONAdapter(
        token=args.token,
        data_dir=args.staging,
    )
    machine_id = os.uname().nodename

    if not args.dry_run:
        conn = _db_connect(args.db)
    else:
        conn = None  # type: ignore[assignment]

    sites = [s["site_id"] for s in adapter.iter_sites()] if args.all_sites else args.sites
    logger.info("Processing %d NEON sites: %s", len(sites), sites)

    n_files    = 0
    n_otus_total = 0
    n_inserted = 0

    for site_code in sites:
        logger.info("── Site: %s", site_code)
        try:
            avail = adapter._get_product_availability("DP1.10107.001", site_code)
        except Exception as exc:
            logger.warning("  Availability check failed: %s", exc)
            continue

        for release_entry in avail:
            for year_month in release_entry.get("availableMonths", []):
                year = int(year_month[:4])
                if args.years and year not in args.years:
                    continue

                logger.info("  %s — downloading %s", site_code, year_month)
                try:
                    biom_paths = adapter.download_sequence_data(
                        site_code, year_month, seq_type=args.seq_type
                    )
                except Exception as exc:
                    logger.warning("  Download failed: %s", exc)
                    continue

                biom_files = [p for p in biom_paths if p.suffix in (".biom", ".hdf5")]
                if not biom_files:
                    logger.info("  No BIOM files found for %s %s", site_code, year_month)
                    continue

                # Get soil chemistry for this site/month
                try:
                    soil_chem = adapter.get_soil_chemistry(site_code, year_month)
                except Exception:
                    soil_chem = {}

                # Get site lat/lon from iter_sites metadata (cached)
                site_meta: dict = {}
                for s in adapter.iter_sites():
                    if s["site_id"] == site_code:
                        site_meta = s
                        break

                for biom_path in biom_files:
                    logger.info("  Parsing %s", biom_path.name)
                    biom_data = _parse_biom_file(biom_path)
                    if not biom_data:
                        continue

                    n_files    += 1
                    n_otus_total += biom_data.get("n_otus", 0)

                    sample_id = f"neon.{site_code}.{year_month}.{biom_path.stem}"
                    meta = {
                        "source_id":         f"{site_code}.{year_month}",
                        "biome":             site_meta.get("biome", "terrestrial biome"),
                        "latitude":          site_meta.get("latitude"),
                        "longitude":         site_meta.get("longitude"),
                        "site_id":           site_code,
                        "visit_number":      1,
                        "sampling_date":     year_month + "-01",
                        "sampling_fraction": "bulk",
                        **soil_chem,
                    }

                    if args.dry_run:
                        print(f"[DRY RUN] {sample_id}")
                        print(f"  n_otus={biom_data['n_otus']}  total_seqs={biom_data['total_seqs']}")
                        print(f"  phyla={list(biom_data['phylum_profile'].keys())[:5]}")
                        print(f"  genera={[g['name'] for g in biom_data['top_genera'][:5]]}")
                        n_inserted += 1
                        continue

                    try:
                        _upsert_sample(conn, sample_id, meta)
                        cid = _upsert_community(conn, sample_id, biom_data, biom_path)
                        _upsert_run_t0(conn, sample_id, cid, args.target_id, machine_id)
                        conn.commit()
                        n_inserted += 1
                    except Exception as exc:
                        logger.error("  DB insert failed for %s: %s", sample_id, exc)
                        if conn:
                            conn.rollback()

    print(f"\n=== NEON BIOM ingest complete ===")
    print(f"  BIOM files parsed: {n_files}")
    print(f"  Total OTUs seen  : {n_otus_total:,}")
    print(f"  Samples inserted : {n_inserted}")


if __name__ == "__main__":
    main()
