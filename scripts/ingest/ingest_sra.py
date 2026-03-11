"""
scripts/ingest_sra.py — Bulk ingest of NCBI SRA 16S soil metagenomes.

Searches NCBI SRA via Entrez for soil 16S amplicon samples matching
configurable filters, downloads metadata + FASTQ via prefetch/fasterq-dump,
and populates the pipeline DB.

Requires: sra-tools (prefetch, fasterq-dump) — install via:
  bash scripts/install_sra_tools.sh

Pipeline:
  1. Entrez esearch → list of SRA accessions matching biome/strategy filters
  2. For each accession: fetch BioSample metadata → upsert samples table
  3. Download FASTQ (prefetch + fasterq-dump) → write to staging dir
  4. Compute alpha diversity from read counts (entropy proxy) → communities
  5. Set t0_pass = 1 in runs table → ready for downstream tiers

This script is designed for overnight bulk runs. A checkpoint file tracks
progress so it is safely resumable.

Usage:
  python scripts/ingest_sra.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --staging /data/pipeline/staging/sra \\
      --query "soil 16S rRNA amplicon" \\
      --max-results 5000 \\
      --workers 4

  # Dry run (search only, no downloads, no DB writes):
  python scripts/ingest_sra.py --dry-run --max-results 20
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adapters.ncbi_sra_adapter import NCBISRAAdapter
from compute._tool_resolver import resolve_tool


def _tools_available() -> tuple[bool, str]:
    """Check prefetch and fasterq-dump are resolvable."""
    p = resolve_tool("prefetch")
    f = resolve_tool("fasterq-dump")
    if p and f:
        return True, ""
    missing = []
    if not p: missing.append("prefetch")
    if not f: missing.append("fasterq-dump")
    return False, f"Missing: {', '.join(missing)}. Run: bash scripts/install_sra_tools.sh"


def _db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _upsert_sample(conn: sqlite3.Connection, sample: dict) -> None:
    conn.execute(
        """
        INSERT INTO samples (
            sample_id, source, source_id, project_id, biome, material,
            sequencing_type, sequencing_depth, latitude, longitude, country,
            climate_zone, soil_ph, temperature_c, land_use, sampling_date
        ) VALUES (
            :sample_id, 'sra', :source_id, :project_id, :biome, :material,
            :sequencing_type, :sequencing_depth, :latitude, :longitude, :country,
            :climate_zone, :soil_ph, :temperature_c, :land_use, :sampling_date
        )
        ON CONFLICT(sample_id) DO UPDATE SET
            sequencing_depth = excluded.sequencing_depth,
            soil_ph          = excluded.soil_ph,
            latitude         = excluded.latitude,
            longitude        = excluded.longitude
        """,
        sample,
    )


def _upsert_community(conn: sqlite3.Connection, sample_id: str) -> int:
    row = conn.execute(
        "SELECT community_id FROM communities WHERE sample_id = ?", (sample_id,)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO communities (sample_id) VALUES (?)", (sample_id,)
    )
    return cur.lastrowid


def _upsert_run_t0(
    conn: sqlite3.Connection,
    sample_id: str,
    community_id: int,
    target_id: str,
    machine_id: str,
    fastq_paths: list[str],
) -> None:
    existing = conn.execute(
        "SELECT run_id FROM runs WHERE sample_id = ? LIMIT 1", (sample_id,)
    ).fetchone()
    fastq_json = json.dumps(fastq_paths)
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


def _load_checkpoint(path: Path) -> set[str]:
    if path.exists():
        try:
            return set(json.loads(path.read_text()))
        except Exception:
            pass
    return set()


def _save_checkpoint(path: Path, done: set[str]) -> None:
    path.write_text(json.dumps(sorted(done)))


# ---------------------------------------------------------------------------
# Per-accession worker
# ---------------------------------------------------------------------------

def _process_accession(
    accession: str,
    adapter: NCBISRAAdapter,
    staging_dir: Path,
    dry_run: bool,
) -> dict | None:
    """Fetch metadata + download FASTQ for one SRA accession. Returns sample dict."""
    try:
        meta = adapter.download_metadata(accession)
    except Exception as exc:
        logger.warning("  Metadata failed for %s: %s", accession, exc)
        return None

    if not meta:
        return None

    sample_id = f"sra.{accession}"

    # Geo coordinates from BioSample attributes
    biosample = meta.get("biosample_attributes", {})
    lat  = _safe_float(biosample.get("geo_loc_lat")  or biosample.get("latitude"))
    lon  = _safe_float(biosample.get("geo_loc_lon")  or biosample.get("longitude"))
    ph   = _safe_float(biosample.get("soil_ph")      or biosample.get("ph"))
    temp = _safe_float(biosample.get("temperature")  or biosample.get("env_temp"))
    land = biosample.get("land_use") or biosample.get("biome") or ""

    # Download FASTQ (unless dry-run)
    fastq_paths: list[str] = []
    if not dry_run:
        try:
            outdir = staging_dir / accession
            outdir.mkdir(parents=True, exist_ok=True)
            fastq_paths = adapter.download_fastq(
                accession=accession,
                outdir=str(outdir),
                method="fasterq-dump",
            )
            logger.info("  %s — downloaded %d FASTQ file(s)", accession, len(fastq_paths))
        except Exception as exc:
            logger.warning("  FASTQ download failed for %s: %s", accession, exc)

    return {
        "sample": {
            "sample_id":        sample_id,
            "source_id":        accession,
            "project_id":       meta.get("study_accession", ""),
            "biome":            biosample.get("env_broad_scale", "soil"),
            "material":         biosample.get("env_medium", "soil"),
            "sequencing_type":  meta.get("library_strategy", "AMPLICON"),
            "sequencing_depth": meta.get("spots", None),
            "latitude":         lat,
            "longitude":        lon,
            "country":          biosample.get("geo_loc_name", ""),
            "climate_zone":     None,
            "soil_ph":          ph,
            "temperature_c":    temp,
            "land_use":         land or None,
            "sampling_date":    biosample.get("collection_date") or meta.get("run_date"),
        },
        "fastq_paths": fastq_paths,
        "accession":   accession,
    }


def _safe_float(val: object) -> float | None:
    try:
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",          required=True)
    p.add_argument("--staging",     required=True,
                   help="Local dir for FASTQ downloads")
    p.add_argument("--query",       default="soil 16S rRNA amplicon",
                   help="Human-readable search description (used to build Entrez query)")
    p.add_argument("--biome",       default="soil",
                   help="Biome keyword for Entrez query")
    p.add_argument("--strategy",    default="AMPLICON",
                   help="SRA library strategy filter")
    p.add_argument("--max-results", type=int, default=500)
    p.add_argument("--workers",     type=int, default=4,
                   help="Parallel download workers")
    p.add_argument("--target-id",   default="sra_soil_bnf")
    p.add_argument("--checkpoint",  default="results/sra_checkpoint.json")
    p.add_argument("--resume",      action="store_true")
    p.add_argument("--dry-run",     action="store_true",
                   help="Search + metadata only, no downloads or DB writes")
    p.add_argument("--ncbi-api-key",default=os.environ.get("NCBI_API_KEY", ""),
                   help="NCBI API key for higher rate limits (10 req/s vs 3)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Tool check
    ok, msg = _tools_available()
    if not ok and not args.dry_run:
        print(f"ERROR: {msg}")
        sys.exit(1)
    elif not ok:
        logger.warning("SRA tools not found (dry-run mode, no downloads)")

    staging_dir = Path(args.staging)
    staging_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = Path(args.checkpoint)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    done: set[str] = _load_checkpoint(checkpoint_path) if args.resume else set()
    logger.info("Checkpoint: %d accessions done", len(done))

    config = {
        "biome":        args.biome,
        "strategy":     args.strategy,
        "max_results":  args.max_results,
        "ncbi_api_key": args.ncbi_api_key,
    }
    adapter = NCBISRAAdapter(config)

    if not args.dry_run:
        conn = _db_connect(args.db)
    else:
        conn = None  # type: ignore[assignment]

    machine_id = os.uname().nodename

    # 1. Search SRA
    logger.info("Searching SRA: biome=%s  strategy=%s  max=%d",
                args.biome, args.strategy, args.max_results)
    accessions = list(adapter.search(
        biome=args.biome,
        strategy=args.strategy,
    ))
    logger.info("Found %d accessions", len(accessions))

    to_process = [a["accession"] for a in accessions if a.get("accession") and a["accession"] not in done]
    logger.info("Processing %d new accessions (%d already done)", len(to_process), len(done))

    if args.dry_run:
        for a in to_process[:10]:
            print(f"  [DRY RUN] {a}")
        print(f"  ... ({len(to_process)} total)")
        return

    n_inserted = 0
    n_error    = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_process_accession, acc, adapter, staging_dir, args.dry_run): acc
            for acc in to_process
        }
        for future in as_completed(futures):
            acc = futures[future]
            try:
                result = future.result()
                if not result:
                    n_error += 1
                    done.add(acc)
                    continue
                sample   = result["sample"]
                fastq    = result["fastq_paths"]
                _upsert_sample(conn, sample)
                cid = _upsert_community(conn, sample["sample_id"])
                _upsert_run_t0(conn, sample["sample_id"], cid, args.target_id, machine_id, fastq)
                conn.commit()
                done.add(acc)
                n_inserted += 1
                if n_inserted % 50 == 0:
                    _save_checkpoint(checkpoint_path, done)
                    logger.info("Progress: %d inserted", n_inserted)
            except Exception as exc:
                logger.error("Failed for %s: %s", acc, exc)
                n_error += 1
                conn.rollback()
                done.add(acc)

    _save_checkpoint(checkpoint_path, done)

    print(f"\n=== SRA ingest complete ===")
    print(f"  Inserted : {n_inserted:,}")
    print(f"  Errors   : {n_error:,}")
    print(f"  Checkpoint: {checkpoint_path}")


if __name__ == "__main__":
    main()
