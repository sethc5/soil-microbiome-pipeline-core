#!/usr/bin/env python3
"""Run T0.25 scoring for all NEON samples that have t0_pass=1.

T0.25 uses phylum profiles + optional reference BIOM + ML model to assign
a function_score and similarity_score per community. Communities without
phylum profiles still get t025_pass=True with score=0 (not rejected).

Usage:
    python scripts/run_neon_t025.py [--db PATH] [--workers N] [--dry-run]
"""
import argparse
import logging
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config_schema import PipelineConfig
from db_utils import SoilDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="/data/pipeline/db/soil_microbiome.db",
                   help="Path to SQLite DB")
    p.add_argument("--workers", type=int, default=8,
                   help="Parallel workers")
    p.add_argument("--source", default="neon",
                   help="samples.source to filter (default: neon)")
    p.add_argument("--dry-run", action="store_true",
                   help="Count communities without running T0.25")
    p.add_argument("--limit", type=int, default=0,
                   help="Process at most N communities (0=all)")
    args = p.parse_args()

    db = SoilDB(args.db)

    # Find all community_ids for source samples that have t0_pass=1 but no T0.25 yet
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(
        """
        SELECT DISTINCT r.community_id
        FROM samples s
        JOIN runs r ON s.sample_id = r.sample_id
        WHERE s.source = ?
          AND r.t0_pass = 1
          AND r.t025_pass IS NULL
          AND r.community_id IS NOT NULL
        ORDER BY r.community_id
        """,
        (args.source,),
    )
    community_ids = [row[0] for row in c.fetchall()]
    conn.close()

    if args.limit:
        community_ids = community_ids[: args.limit]

    logger.info(
        "Found %d %s communities with t0_pass=1 awaiting T0.25",
        len(community_ids),
        args.source,
    )

    if args.dry_run:
        logger.info("[dry-run] Exit without running.")
        return

    if not community_ids:
        logger.info("Nothing to do.")
        return

    # Minimal PipelineConfig — no ML model or reference BIOM on this run.
    # function_score will be 0 for communities without phylum profiles or models.
    config = PipelineConfig(
        project={"name": "neon-t025", "version": "1.0"},
        target={"taxa": ["soil_microbiome"], "functional_gene": "nifH"},
        filters={
            "t0": {},
            "t025": {
                "min_function_score": 0.0,
                "min_similarity": 0.0,
                "model_path": "",
                "reference_db": "",
            },
        },
    )

    from pipeline_core import run_t025_batch

    logger.info("Starting T0.25 batch for %d communities (workers=%d)...",
                len(community_ids), args.workers)

    # Note: server pipeline_core has run_t025_batch(config, db, workers, ...)
    # which auto-queries all t0_pass=1 communities where t025_pass IS NULL.
    result = run_t025_batch(
        config=config,
        db=db,
        workers=args.workers,
    )

    logger.info(
        "T0.25 complete: n_processed=%s  n_passed=%s  n_failed=%s",
        result.get("n_processed"),
        result.get("n_passed"),
        result.get("n_failed"),
    )
    if result.get("errors"):
        logger.warning("First 5 errors: %s", result["errors"][:5])
    if result.get("receipt_path"):
        logger.info("Receipt: %s", result["receipt_path"])


if __name__ == "__main__":
    main()
