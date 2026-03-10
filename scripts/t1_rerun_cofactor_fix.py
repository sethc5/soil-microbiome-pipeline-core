"""
scripts/t1_rerun_cofactor_fix.py — Targeted T1 rerun for communities affected by
the organic-cofactor carbon-source leak in _apply_bnf_minimal_medium.

Root cause (fixed commit 4499927):
    EX_thm_e / EX_ribflv_e / EX_btn_e / EX_fol_e / EX_pnto__R_e were listed in
    _BNF_INORGANIC_EXCHANGES and reopened at -1000 mmol/gDW/h.  AGORA2 models can
    catabolise these vitamins for ATP, making NITROGENASE_MO FVA LP-unconstrained.

Affected signal: t1_flux_upper_bound >= 100 AND t1_flux_units = 'mmol_nh4_equiv/gDW/h'
    (fva_ub=1000 is the clearest indicator that the LP hit the uncapped bound)

This script:
    1. Queries the DB for all affected run_ids and their community_ids.
    2. For each unique community, loads top_genera and sample metadata.
    3. Reruns community FBA via _worker_batch (which imports the fixed code).
    4. Writes corrected t1_* values back with an unconditional UPDATE.

Usage (on server):
    cd /opt/pipeline
    .venv/bin/python scripts/t1_rerun_cofactor_fix.py \\
        --db /data/pipeline/db/soil_microbiome.db \\
        --model-dir /opt/pipeline/models \\
        --workers 32
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

# ---------------------------------------------------------------------------
# Threshold for "affected" communities.
# fva_ub >= 100 is the clear signal the LP was unconstrained.
# We also include any run where the stored fva_ub is clearly wrong even if
# target_flux looks moderate (could have been partially constrained).
# ---------------------------------------------------------------------------
_ARTIFACT_FVA_UB_THRESHOLD: float = 100.0


def _fetch_affected_communities(db_path: str) -> list[tuple]:
    """Return (community_id, top_genera_json, meta_json) for affected communities.

    A community is "affected" if ANY of its runs has fva_ub >= threshold in BNF mode.
    We also include communities where t1_target_flux > 45 as a belt-and-suspenders check
    (45 = theoretical ceiling for 10 mmol glucose, single diazotroph).
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
            ) as meta_json
        FROM runs r
        JOIN communities c ON c.community_id = r.community_id
        JOIN samples s ON s.sample_id = r.sample_id
        WHERE r.t1_flux_units = 'mmol_nh4_equiv/gDW/h'
          AND (r.t1_flux_upper_bound >= ? OR r.t1_target_flux > 45)
          AND c.top_genera IS NOT NULL
          AND c.top_genera NOT IN ('[]', '{}', 'null', '')
        """,
        (_ARTIFACT_FVA_UB_THRESHOLD,),
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2]) for r in rows]


def _write_rerun_results(db_path: str, results: list[dict]) -> tuple[int, int]:
    """Write corrected T1 results — UNCONDITIONAL update (t1_pass may already be set)."""
    conn = _db_connect(db_path, timeout=60)
    conn.execute("PRAGMA synchronous=OFF")
    n_written, n_passed = 0, 0

    for r in results:
        cid = r.get("community_id")
        if r.get("error"):
            logger.warning("Rerun error cid=%s: %s", cid, r["error"])
            # Mark affected runs as failed so they don't look like good data.
            conn.execute(
                """UPDATE runs SET
                       t1_pass = 0,
                       t1_model_confidence = 'failed_rerun'
                   WHERE community_id = ?
                     AND t1_flux_units = 'mmol_nh4_equiv/gDW/h'
                     AND (t1_flux_upper_bound >= ? OR t1_target_flux > 45)""",
                (cid, _ARTIFACT_FVA_UB_THRESHOLD),
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
                 AND (t1_flux_upper_bound >= ? OR t1_target_flux > 45)""",
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
                _ARTIFACT_FVA_UB_THRESHOLD,
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
    model_dir: str = typer.Option("/opt/pipeline/models", help="Directory with SBML genus models"),
    workers: int = typer.Option(32, help="Number of parallel worker processes"),
    batch_size: int = typer.Option(50, help="Communities per worker batch"),
    dry_run: bool = typer.Option(False, help="Query and report count only, don't rerun"),
) -> None:
    """Rerun T1 FBA for communities affected by cofactor C-source leak (commit 4499927 fix)."""
    logger.info("Querying affected communities from %s ...", db)
    affected = _fetch_affected_communities(db)
    logger.info("Found %d unique communities to reprocess", len(affected))

    if dry_run:
        logger.info("Dry run — exiting without processing")
        raise typer.Exit(0)

    if not affected:
        logger.info("No affected communities found — nothing to do")
        raise typer.Exit(0)

    # Import worker from t1_fba_batch (which uses the updated community_fba.py)
    sys.path.insert(0, str(_PROJ_ROOT / "scripts"))
    from t1_fba_batch import _worker_batch  # noqa: E402 — imported here to avoid circular

    # Split into batches
    batches = [affected[i : i + batch_size] for i in range(0, len(affected), batch_size)]
    logger.info("Processing %d batches of up to %d communities, %d workers",
                len(batches), batch_size, workers)

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
                remain = len(affected) - completed
                eta = remain / rate if rate > 0 else 0
                n_pass = sum(1 for r in all_results if r.get("t1_pass"))
                logger.info(
                    "[%d/%d] %.0f/min  ETA %.0f min  t1_pass so far: %d",
                    completed, len(affected), rate * 60, eta / 60, n_pass,
                )
            except Exception as exc:
                logger.error("Batch failed: %s", exc)

    logger.info("Rerun complete: %d results collected in %.1f min",
                len(all_results), (time.perf_counter() - t_start) / 60)

    # Summary before writing
    n_pass = sum(1 for r in all_results if r.get("t1_pass"))
    n_err  = sum(1 for r in all_results if r.get("error"))
    flux_vals = [r["t1_target_flux"] for r in all_results
                 if r.get("t1_pass") and r.get("t1_flux_units") == "mmol_nh4_equiv/gDW/h"]
    if flux_vals:
        avg_flux = sum(flux_vals) / len(flux_vals)
        max_flux = max(flux_vals)
        logger.info("Corrected BNF flux: n=%d avg=%.2f max=%.2f mmol_NH4/gDW/h",
                    len(flux_vals), avg_flux, max_flux)

    logger.info("Pass: %d  Errors: %d  Writing to DB ...", n_pass, n_err)
    _write_rerun_results(db, all_results)

    logger.info("Done. Max flux should now be ≤45 mmol NH4-equiv/gDW/h.")


if __name__ == "__main__":
    app()
