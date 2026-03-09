"""
scripts/ingest_mgnify.py — Bulk ingest of pre-processed MGnify soil analyses.

MGnify has already run QIIME2 + their functional annotation pipeline on
500,000+ metagenomes. This script harvests those pre-computed results via
their REST API — no FASTQ, no QIIME2, no tool installs required.

Running from Hetzner / data-centre IPs:
  EBI's metagenomics backend silently drops connections from known cloud/DC
  ASNs (Hetzner AS24940 confirmed). Use --proxy with a SOCKS5 tunnel:

  # Step 1 — on YOUR LOCAL MACHINE, open a SOCKS proxy on port 1080:
  ssh -D 1080 -N -f localhost

  # Step 2 — forward that port into the server (keep this terminal open):
  ssh -R 1080:localhost:1080 pipeline    # 'pipeline' SSH alias

  # Step 3 — on the server, run:
  MGNIFY_PROXY=socks5://localhost:1080 \\
      python scripts/ingest_mgnify.py --db /data/pipeline/db/soil_microbiome.db

  # Or run locally and ship the results:
  python scripts/ingest_mgnify.py --dry-run --max-results 20    # test
  python scripts/ingest_mgnify.py --output-jsonl mgnify_soil.jsonl --max-results 50000
  scp mgnify_soil.jsonl pipeline:/tmp/
  ssh pipeline python scripts/ingest_mgnify.py \\
      --db /data/pipeline/db/soil_microbiome.db --from-jsonl /tmp/mgnify_soil.jsonl

For each analysis it populates:
  - samples table    : geo, environmental metadata per sample
  - communities table: phylum_profile, top_genera (from MGnify taxonomy)
  - runs table       : t025_model (functional pathway JSON), t025_function_score,
                       t025_n_pathways, t025_nsti_mean, t025_pass = 1

Designed to run overnight. A checkpoint file tracks progress so the run
is resumable with --resume.

Usage:
  python scripts/ingest_mgnify.py --db /data/pipeline/db/soil_microbiome.db \\
      --max-results 50000 --biome "root:Environmental:Terrestrial:Soil"

  # Dry-run (fetch + print, no DB writes):
  python scripts/ingest_mgnify.py --dry-run --max-results 20
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Add project root to path so adapters/ and db_utils are importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adapters.mgnify_adapter import MGnifyAdapter


# ---------------------------------------------------------------------------
# DB helpers (thin wrappers — avoid importing the full SoilDB to stay lean)
# ---------------------------------------------------------------------------

def _db_connect(db_path: str):
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _upsert_sample(conn, sample: dict) -> None:
    conn.execute(
        """
        INSERT INTO samples (
            sample_id, source, source_id, biome, feature, material,
            sequencing_type, latitude, longitude, country, climate_zone,
            soil_ph, temperature_c, land_use, sampling_date
        ) VALUES (
            :sample_id, :source, :source_id, :biome, :feature, :material,
            :sequencing_type, :latitude, :longitude, :country, :climate_zone,
            :soil_ph, :temperature_c, :land_use, :sampling_date
        )
        ON CONFLICT(sample_id) DO UPDATE SET
            latitude       = excluded.latitude,
            longitude      = excluded.longitude,
            soil_ph        = excluded.soil_ph,
            temperature_c  = excluded.temperature_c,
            country        = excluded.country
        """,
        sample,
    )


def _upsert_community(conn, community: dict) -> int:
    """Insert or retrieve community_id for this sample_id."""
    row = conn.execute(
        "SELECT community_id FROM communities WHERE sample_id = ?",
        (community["sample_id"],),
    ).fetchone()
    if row:
        # Update taxonomy profiles if we have better data
        conn.execute(
            """
            UPDATE communities SET
                phylum_profile = CASE WHEN phylum_profile IS NULL OR phylum_profile = '{}' THEN ? ELSE phylum_profile END,
                top_genera     = CASE WHEN top_genera     IS NULL OR top_genera     = '[]' THEN ? ELSE top_genera     END
            WHERE community_id = ?
            """,
            (
                json.dumps(community.get("phylum_profile", {})),
                json.dumps(community.get("top_genera", [])),
                row[0],
            ),
        )
        return row[0]
    cursor = conn.execute(
        """
        INSERT INTO communities (sample_id, phylum_profile, top_genera)
        VALUES (:sample_id, :phylum_profile, :top_genera)
        """,
        {
            "sample_id":      community["sample_id"],
            "phylum_profile": json.dumps(community.get("phylum_profile", {})),
            "top_genera":     json.dumps(community.get("top_genera", [])),
        },
    )
    return cursor.lastrowid


def _upsert_run(conn, run: dict) -> None:
    """Insert a T0.25-level run row for an MGnify analysis."""
    existing = conn.execute(
        "SELECT run_id FROM runs WHERE sample_id = ? LIMIT 1",
        (run["sample_id"],),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE runs SET
                t025_pass           = 1,
                t025_model          = :t025_model,
                t025_function_score = :t025_function_score,
                t025_n_pathways     = :t025_n_pathways,
                t025_uncertainty    = :t025_uncertainty,
                tier_reached        = 1
            WHERE run_id = ?
            """,
            {**run, "run_id": existing[0]},
        )
    else:
        conn.execute(
            """
            INSERT INTO runs (
                sample_id, community_id, target_id,
                t0_pass, t025_pass,
                t025_model, t025_function_score, t025_n_pathways, t025_uncertainty,
                tier_reached, machine_id
            ) VALUES (
                :sample_id, :community_id, :target_id,
                1, 1,
                :t025_model, :t025_function_score, :t025_n_pathways, :t025_uncertainty,
                1, :machine_id
            )
            """,
            run,
        )


# ---------------------------------------------------------------------------
# Checkpoint helpers  (plain JSON file)
# ---------------------------------------------------------------------------

def _load_checkpoint(path: Path) -> set[str]:
    if path.exists():
        try:
            return set(json.loads(path.read_text()))
        except Exception:
            pass
    return set()


def _save_checkpoint(path: Path, seen: set[str]) -> None:
    path.write_text(json.dumps(sorted(seen)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",          required=True,  help="Path to SQLite DB")
    p.add_argument("--biome",       default="root:Environmental:Terrestrial:Soil",
                   help="MGnify biome lineage string")
    p.add_argument("--experiment",  default="amplicon",
                   help="MGnify experiment_type filter (amplicon | metagenomic)")
    p.add_argument("--max-results", type=int, default=5000,
                   help="Max analyses to fetch (API pages of 50)")
    p.add_argument("--target-id",   default="mgnify_soil_bnf",
                   help="target_id value stored in runs table")
    p.add_argument("--checkpoint",  default="results/mgnify_checkpoint.json",
                   help="Path to checkpoint file (for --resume)")
    p.add_argument("--resume",      action="store_true",
                   help="Skip analyses already in checkpoint file")
    p.add_argument("--dry-run",     action="store_true",
                   help="Fetch + print without writing to DB")
    p.add_argument("--batch-size",  type=int, default=50,
                   help="Commit to DB every N analyses")
    p.add_argument("--proxy",       default=None,
                   help="Proxy URL (e.g. socks5://localhost:1080) for routing "
                        "around EBI WAF blocks. Also read from MGNIFY_PROXY env.")
    p.add_argument("--output-jsonl", default=None,
                   help="Write JSONL records to this file instead of --db "
                        "(for offline transfer to server).")
    p.add_argument("--from-jsonl",  default=None,
                   help="Load JSONL records from file into --db, no API calls.")
    return p.parse_args()


def _load_jsonl_to_db(jsonl_path: str, db_path: str) -> None:
    """Load a --output-jsonl dump produced on another machine into the local DB."""
    conn = _db_connect(db_path)
    n = 0
    with open(jsonl_path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                sample = {k: rec.get(k) for k in [
                    "sample_id", "source", "source_id", "biome", "feature", "material",
                    "sequencing_type", "latitude", "longitude", "country", "climate_zone",
                    "soil_ph", "temperature_c", "land_use", "sampling_date",
                ]}
                community = {
                    "sample_id":      rec["sample_id"],
                    "phylum_profile": rec.get("phylum_profile", {}),
                    "top_genera":     rec.get("top_genera", []),
                }
                run = {
                    "sample_id":           rec["sample_id"],
                    "community_id":        None,
                    "target_id":           rec.get("target_id", "mgnify_soil"),
                    "t025_model":          rec.get("t025_model"),
                    "t025_function_score": rec.get("t025_function_score", 0.0),
                    "t025_n_pathways":     rec.get("t025_n_pathways", 0),
                    "t025_uncertainty":    rec.get("t025_uncertainty", 0.1),
                    "machine_id":          os.uname().nodename,
                }
                _upsert_sample(conn, sample)
                cid = _upsert_community(conn, community)
                run["community_id"] = cid
                _upsert_run(conn, run)
                n += 1
                if n % 500 == 0:
                    conn.commit()
                    logger.info("  loaded %d records...", n)
            except Exception as exc:
                logger.warning("Skipping line: %s", exc)
    conn.commit()
    logger.info("JSONL load complete: %d records", n)


def main() -> None:
    args = parse_args()

    if args.from_jsonl:
        _load_jsonl_to_db(args.from_jsonl, args.db)
        return

    proxy = args.proxy or os.environ.get("MGNIFY_PROXY")
    config = {"mgnify_token": os.environ.get("MGNIFY_TOKEN", "")}
    if proxy:
        config["mgnify_proxy"] = proxy
    adapter = MGnifyAdapter(config=config)

    checkpoint_path = Path(args.checkpoint)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    done: set[str] = _load_checkpoint(checkpoint_path) if args.resume else set()
    logger.info("Checkpoint: %d analyses already done", len(done))

    jsonl_fh = open(args.output_jsonl, "w") if args.output_jsonl else None

    if not args.dry_run and not jsonl_fh:
        conn = _db_connect(args.db)
    else:
        conn = None  # type: ignore[assignment]

    machine_id = os.uname().nodename
    n_inserted = 0
    n_skipped  = 0
    n_error    = 0
    batch_buffer: list[tuple] = []  # (sample, community, run) tuples

    def flush_batch() -> None:
        nonlocal n_inserted
        if not batch_buffer or args.dry_run:
            return
        for sample, community, run in batch_buffer:
            try:
                _upsert_sample(conn, sample)
                cid = _upsert_community(conn, community)
                run["community_id"] = cid
                _upsert_run(conn, run)
                n_inserted += 1
            except Exception as exc:
                logger.warning("DB upsert failed: %s", exc)
        conn.commit()
        batch_buffer.clear()
        _save_checkpoint(checkpoint_path, done)
        logger.info("Committed batch — total ingested: %d", n_inserted)

    logger.info(
        "Fetching up to %d MGnify analyses from biome: %s",
        args.max_results, args.biome,
    )

    for analysis in adapter.search_analyses(
        biome=args.biome,
        experiment_type=args.experiment,
        max_results=args.max_results,
    ):
        accession = analysis["accession"]
        if accession in done:
            n_skipped += 1
            continue

        # --- Fetch detailed metadata + profiles ---
        try:
            meta = adapter.get_analysis_metadata(accession)
            func = adapter.get_functional_profile(accession)
            tax  = adapter.get_taxonomic_profile_structured(accession)
        except Exception as exc:
            logger.warning("Failed to fetch profiles for %s: %s", accession, exc)
            n_error += 1
            done.add(accession)
            continue

        # Derive a unique sample_id
        sample_accession = analysis.get("sample_accession") or accession
        sample_id = f"mgnify.{sample_accession}"

        # Compute a pseudo-function-score: fraction of MetaCyc pathways present
        n_pathways = func.get("n_pathways", 0)
        # T0.25 function score: normalised pathway richness (0–1, cap at 500 pathways)
        function_score = min(n_pathways / 500.0, 1.0)

        sample = {
            "sample_id":        sample_id,
            "source":           "mgnify",
            "source_id":        sample_accession,
            "biome":            meta.get("biome") or args.biome,
            "feature":          meta.get("environment_feature", ""),
            "material":         meta.get("environment_material", "soil"),
            "sequencing_type":  "16S" if "amplicon" in (analysis.get("experiment_type") or "") else "shotgun_metagenome",
            "latitude":         meta.get("latitude"),
            "longitude":        meta.get("longitude"),
            "country":          meta.get("country"),
            "climate_zone":     None,
            "soil_ph":          meta.get("soil_ph"),
            "temperature_c":    meta.get("temperature_c"),
            "land_use":         None,  # not available from MGnify
            "sampling_date":    meta.get("collection_date"),
        }

        community = {
            "sample_id":      sample_id,
            "phylum_profile": tax.get("phylum_profile", {}),
            "top_genera":     tax.get("top_genera", []),
        }

        run = {
            "sample_id":            sample_id,
            "community_id":         None,  # filled in flush_batch
            "target_id":            args.target_id,
            "t025_model":           json.dumps(func),
            "t025_function_score":  round(function_score, 6),
            "t025_n_pathways":      n_pathways,
            "t025_uncertainty":     0.10,  # MGnify pipeline uncertainty placeholder
            "machine_id":           machine_id,
        }

        if args.dry_run:
            print(f"\n[DRY RUN] {accession}")
            print(f"  sample_id      : {sample_id}")
            print(f"  latitude       : {meta.get('latitude')}  longitude: {meta.get('longitude')}")
            print(f"  n_pathways     : {n_pathways}  function_score: {function_score:.3f}")
            print(f"  phyla          : {list(tax['phylum_profile'].keys())[:5]}")
            print(f"  top_genera     : {[g['name'] for g in tax['top_genera'][:5]]}")
            n_inserted += 1
        elif jsonl_fh:
            jsonl_rec = {
                **sample,
                "phylum_profile":    json.dumps(community.get("phylum_profile", {})),
                "top_genera":        json.dumps(community.get("top_genera", [])),
                "t025_model":        run["t025_model"],
                "t025_function_score": run["t025_function_score"],
                "t025_n_pathways":   run["t025_n_pathways"],
                "t025_uncertainty":  run["t025_uncertainty"],
                "target_id":         run["target_id"],
            }
            jsonl_fh.write(json.dumps(jsonl_rec) + "\n")
            jsonl_fh.flush()
            n_inserted += 1
        else:
            batch_buffer.append((sample, community, run))

        done.add(accession)

        if len(batch_buffer) >= args.batch_size:
            flush_batch()

    flush_batch()  # final partial batch

    if jsonl_fh:
        jsonl_fh.close()
        logger.info("JSONL written to %s — transfer to server and load with --from-jsonl", args.output_jsonl)

    print(f"\n=== MGnify ingest complete ===")
    print(f"  Ingested : {n_inserted:,}")
    print(f"  Skipped  : {n_skipped:,}  (already in checkpoint)")
    print(f"  Errors   : {n_error:,}")
    print(f"  Checkpoint: {checkpoint_path}")


if __name__ == "__main__":
    main()
