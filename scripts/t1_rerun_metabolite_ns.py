"""
scripts/t1_rerun_metabolite_ns.py — Full T1 rerun for ALL BNF communities after
the metabolite-namespacing fix (commit ea2257f).

Root cause (fixed in ea2257f):
    _merge_community_models namespaced reaction IDs (PGK__org1) but NOT metabolite
    IDs, so all N organisms shared the same atp_c / nadph_c / pyr_c pools.  The LP
    could route N organisms-worth of intracellular catabolism into the shared pool,
    giving NITROGENASE_MO an N× ATP budget and inflating BNF flux to a max of 108
    mmol NH4-equiv/gDW/h (theoretical ceiling is ≤45 for a single diazotroph at 10
    mmol glucose).

Affected signal: ALL runs with t1_flux_units = 'mmol_nh4_equiv/gDW/h' AND t1_pass = 1.
    (Unlike the cofactor fix which only hit communities with fva_ub ≥ 100, the metabolite
    pool bug inflates flux proportionally to community size across the entire BNF tier.)

This script:
    1. Queries the DB for every BNF t1_pass community (t1_pass=1, mmol_nh4_equiv units).
    2. Loads top_genera and sample metadata for each community.
    3. Reruns community FBA via _worker_batch (importing the fixed community_fba.py).
    4. Writes corrected t1_* values back unconditionally.

Usage (on server):
    cd /opt/pipeline
    .venv/bin/python scripts/t1_rerun_metabolite_ns.py \\
        --db /data/pipeline/db/soil_microbiome.db \\
        --model-dir /data/pipeline/models \\
        --workers 32

    # Dry run (count only):
    .venv/bin/python scripts/t1_rerun_metabolite_ns.py \\
        --db /data/pipeline/db/soil_microbiome.db --dry-run
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False)


def _fetch_all_bnf_communities(db_path: str) -> list[tuple]:
    """Return (community_id, top_genera_json, meta_json) for all BNF t1_pass communities.

    Broadened from cofactor-fix script: we rerun EVERY community that passed T1
    in BNF mode, since the metabolite pool bug inflates flux proportionally to
    community size — not just those above an fva_ub threshold.
    """
    conn = _db_connect(db_path)
    rows = conn.execute(
        """
        SELECT DISTINCT
            c.community_id,
            c.top_genera,
            json_object(
                'soil_ph',            COALESCE(s.soil_ph, 6.5),
                'organic_matter_pct', COALESCE(s.organic_matter_pct, 2.0),
                'clay_pct',           COALESCE(s.clay_pct, 25.0),
                'temperature_c',      COALESCE(s.temperature_c, 12.0),
                'precipitation_mm',   COALESCE(s.precipitation_mm, 600.0)
            ) AS meta_json
        FROM runs r
        JOIN communities c ON c.community_id = r.community_id
        JOIN samples s ON s.sample_id = r.sample_id
        WHERE r.t1_flux_units = 'mmol_nh4_equiv/gDW/h'
          AND r.t1_pass = 1
          AND c.top_genera IS NOT NULL
          AND c.top_genera NOT IN ('[]', '{}', 'null', '')
        """,
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2]) for r in rows]


def _write_rerun_results(db_path: str, results: list[dict]) -> tuple[int, int]:
    """Write corrected T1 results — UNCONDITIONAL update for all BNF t1_pass runs."""
    conn = _db_connect(db_path, timeout=60)
    conn.execute("PRAGMA synchronous=OFF")
    n_written, n_passed = 0, 0

    for r in results:
        cid = r.get("community_id")
        if r.get("error"):
            logger.warning("Rerun error cid=%s: %s", cid, r["error"])
            # Mark as failed so this community doesn't appear as good data.
            conn.execute(
                """UPDATE runs SET
                       t1_pass = 0,
                       t1_model_confidence = 'failed_rerun_metabolite_ns'
                   WHERE community_id = ?
                     AND t1_flux_units = 'mmol_nh4_equiv/gDW/h'
                     AND t1_pass = 1""",
                (cid,),
            )
            n_written += 1
            continue

        conn.execute(
            """UPDATE runs SET
                   t1_pass                      = ?,
                   t1_model_size                = ?,
                   t1_target_flux               = ?,
                   t1_flux_lower_bound          = ?,
                   t1_flux_upper_bound          = ?,
                   t1_flux_units                = ?,
                   t1_feasible                  = ?,
                   t1_keystone_taxa             = ?,
                   t1_genome_completeness_mean  = ?,
                   t1_genome_contamination_mean = ?,
                   t1_model_confidence          = ?,
                   t1_walltime_s                = ?
               WHERE community_id = ?
                 AND t1_flux_units = 'mmol_nh4_equiv/gDW/h'
                 AND t1_pass = 1""",
            (
                1 if r["t1_pass"] else 0,
                r.get("t1_model_size"),
                r.get("t1_target_flux"),
                r.get("t1_flux_lower_bound"),
                r.get("t1_flux_upper_bound"),
                r.get("t1_flux_units"),
                1 if r.get("t1_feasible") else 0,
                r.get("t1_keystone_taxa"),
                r.get("t1_genome_completeness_mean"),
                r.get("t1_genome_contamination_mean"),
                r.get("t1_model_confidence"),
                r.get("t1_walltime_s"),
                cid,
            ),
        )
        n_written += 1
        if r["t1_pass"]:
            n_passed += 1

    conn.commit()
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.close()
    logger.info("DB write: %d communities updated, %d now t1_pass", n_written, n_passed)
    return n_written, n_passed


@app.command()
def main(
    db: str = typer.Option(..., help="Path to SQLite database"),
    model_dir: str = typer.Option("/data/pipeline/models", help="Directory with SBML genus models"),
    workers: int = typer.Option(32, help="Number of parallel worker processes"),
    batch_size: int = typer.Option(50, help="Communities per worker batch"),
    dry_run: bool = typer.Option(False, help="Query and report count only, don't rerun"),
) -> None:
    """Rerun T1 FBA for ALL BNF communities after metabolite-namespacing fix (ea2257f)."""
    logger.info("Querying all BNF t1_pass communities from %s ...", db)
    communities = _fetch_all_bnf_communities(db)
    logger.info("Found %d unique BNF communities to reprocess", len(communities))

    if dry_run:
        logger.info("Dry run — exiting without processing")
        raise typer.Exit(0)

    if not communities:
        logger.info("No communities found — nothing to do")
        raise typer.Exit(0)

    # Import worker from t1_fba_batch (which uses the updated community_fba.py)
    sys.path.insert(0, str(_PROJ_ROOT / "scripts"))
    from t1_fba_batch import _worker_batch  # noqa: E402

    batches = [communities[i : i + batch_size] for i in range(0, len(communities), batch_size)]
    logger.info(
        "Processing %d batches of up to %d communities, %d workers",
        len(batches), batch_size, workers,
    )

    all_results: list[dict] = []
    t_start = time.perf_counter()
    completed = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker_batch, b, model_dir): b for b in batches}
        for fut in as_completed(futures):
            try:
                batch_results = fut.result()
                all_results.extend(batch_results)
                completed += len(batch_results)
                elapsed = time.perf_counter() - t_start
                rate = completed / elapsed if elapsed > 0 else 0
                remain = len(communities) - completed
                eta = remain / rate if rate > 0 else 0
                n_pass = sum(1 for r in all_results if r.get("t1_pass"))
                logger.info(
                    "[%d/%d] %.0f/min  ETA %.0f min  t1_pass so far: %d",
                    completed, len(communities), rate * 60, eta / 60, n_pass,
                )
            except Exception as exc:
                logger.error("Batch failed: %s", exc)

    elapsed_total = time.perf_counter() - t_start
    logger.info(
        "Rerun complete: %d results collected in %.1f min",
        len(all_results), elapsed_total / 60,
    )

    # Summary before writing
    n_pass = sum(1 for r in all_results if r.get("t1_pass"))
    n_err = sum(1 for r in all_results if r.get("error"))
    flux_vals = [
        r["t1_target_flux"]
        for r in all_results
        if r.get("t1_pass") and r.get("t1_flux_units") == "mmol_nh4_equiv/gDW/h"
    ]
    if flux_vals:
        avg_flux = sum(flux_vals) / len(flux_vals)
        max_flux = max(flux_vals)
        logger.info(
            "Corrected BNF flux: n=%d  avg=%.2f  max=%.2f mmol NH4-equiv/gDW/h",
            len(flux_vals), avg_flux, max_flux,
        )
        if max_flux > 45.0:
            logger.warning(
                "max=%.2f still exceeds theoretical ceiling of 45 — investigate "
                "multi-diazotroph communities or unconstrained photon exchanges.",
                max_flux,
            )
        else:
            logger.info("max=%.2f ≤ 45 — fix confirmed successful.", max_flux)

    logger.info("Pass: %d  Errors: %d  Writing to DB ...", n_pass, n_err)
    _write_rerun_results(db, all_results)
    logger.info("Done.")


if __name__ == "__main__":
    app()
