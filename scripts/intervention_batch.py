"""
scripts/intervention_batch.py — Phase 15: Intervention screening over T1-passed communities.

For each T1-passed community:
  1. Load metadata + T1 confidence
  2. Screen bioinoculants (Azospirillum, Bradyrhizobium, Pseudomonas, ...)
  3. Screen amendments (biochar, compost, lime, ...)
  4. Screen management practices (cover cropping, reduced tillage, ...)
  5. Rank all interventions by (confidence × predicted_effect)
  6. Write top interventions to ``interventions`` table
  7. Update runs.t2_best_intervention with the top-ranked intervention

Usage:
  python scripts/intervention_batch.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --workers 36
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect  # noqa: E402

logger = logging.getLogger(__name__)
app = typer.Typer(
    help="Batch intervention screening for T1-passed communities",
    add_completion=False,
    invoke_without_command=True,
)


# ---------------------------------------------------------------------------
# Worker: intervention screening (runs in child process)
# ---------------------------------------------------------------------------

def _worker_batch(batch: list[tuple]) -> list[dict]:
    """
    Screen interventions for a batch of communities.

    Each tuple: (run_id, community_id, metadata_json, t1_model_confidence)
    """
    try:
        from compute.intervention_screener import screen_interventions
    except ImportError as exc:
        return [{"run_id": t[0], "community_id": t[1], "error": f"Import: {exc}"} for t in batch]

    t2_config: dict = {}  # use defaults from intervention_screener

    results = []
    for run_id, community_id, meta_json, t1_confidence_label in batch:
        t0 = time.perf_counter()
        try:
            metadata = json.loads(meta_json or "{}")

            # Map label to numeric confidence
            conf_map = {"high": 0.85, "medium": 0.55, "low": 0.30, "failed": 0.0}
            t1_conf = conf_map.get(t1_confidence_label or "low", 0.35)

            if t1_conf < 0.01:
                results.append({
                    "run_id": run_id,
                    "community_id": community_id,
                    "interventions": [],
                    "error": "t1_confidence_zero",
                    "walltime_s": time.perf_counter() - t0,
                })
                continue

            # Screen (community_model=None → bioinoculant screening uses metadata only)
            interventions = screen_interventions(
                community_model=None,
                metadata=metadata,
                t2_config=t2_config,
                t1_model_confidence=t1_conf,
                include_bioinoculants=True,
                include_amendments=True,
                include_management=True,
            )

            results.append({
                "run_id": run_id,
                "community_id": community_id,
                "interventions": interventions,
                "error": None,
                "walltime_s": time.perf_counter() - t0,
            })

        except Exception as exc:
            results.append({
                "run_id": run_id,
                "community_id": community_id,
                "interventions": [],
                "error": str(exc),
                "walltime_s": time.perf_counter() - t0,
            })

    return results


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _fetch_t1_communities(db_path: str, n_max: int) -> list[tuple]:
    """
    Load T1-passed communities that haven't been intervention-screened yet.

    Returns list of (run_id, community_id, metadata_json, t1_model_confidence).
    """
    conn = _db_connect(db_path)
    rows = conn.execute(
        """SELECT r.run_id, r.community_id,
                  json_object(
                    'soil_ph',           COALESCE(s.soil_ph, 6.5),
                    'organic_matter_pct',COALESCE(s.organic_matter_pct, 2.0),
                    'clay_pct',          COALESCE(s.clay_pct, 25.0),
                    'temperature_c',     COALESCE(s.temperature_c, 12.0),
                    'precipitation_mm',  COALESCE(s.precipitation_mm, 600.0),
                    'latitude',          s.latitude,
                    'longitude',         s.longitude,
                    'land_use',          s.land_use,
                    'site_id',           s.site_id
                  ),
                  r.t1_model_confidence
           FROM runs r
           JOIN samples s ON r.sample_id = s.sample_id
           WHERE r.t1_pass = 1
             AND r.t2_best_intervention IS NULL
           ORDER BY r.t1_target_flux DESC
           LIMIT ?""",
        (n_max,),
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def _write_interventions(db_path: str, results: list[dict]) -> tuple[int, int]:
    """
    Write intervention results to DB.

    Inserts rows into ``interventions`` table and updates
    ``runs.t2_best_intervention``.

    Returns (n_interventions_written, n_runs_updated).
    """
    conn = _db_connect(db_path, timeout=60)
    conn.execute("PRAGMA synchronous=OFF")  # safe in WAL; restore before commit
    n_interventions, n_runs = 0, 0

    for r in results:
        if r.get("error") and r["error"] != "t1_confidence_zero":
            continue

        run_id = r["run_id"]
        interventions = r.get("interventions", [])

        # Insert intervention rows
        for iv in interventions[:10]:  # cap at top 10 per community
            try:
                conn.execute(
                    """INSERT INTO interventions
                       (run_id, intervention_type, intervention_detail,
                        predicted_effect, confidence,
                        stability_under_perturbation, cost_estimate)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        iv.get("intervention_type", "unknown"),
                        json.dumps({
                            k: v for k, v in iv.items()
                            if k not in ("intervention_type", "predicted_effect",
                                         "confidence", "stability_under_perturbation",
                                         "cost_estimate")
                        }),
                        iv.get("predicted_effect", 0.0),
                        iv.get("confidence", 0.0),
                        iv.get("stability_under_perturbation"),
                        json.dumps({"usd_per_ha": iv.get("cost_estimate", 0)}),
                    ),
                )
                n_interventions += 1
            except Exception as exc:
                logger.debug("Intervention insert failed: %s", exc)

        # Update runs with best intervention
        if interventions:
            best = interventions[0]
            try:
                conn.execute(
                    """UPDATE runs SET
                           t2_best_intervention = ?,
                           t2_intervention_effect = ?,
                           t2_establishment_prob = ?
                       WHERE run_id = ?""",
                    (
                        json.dumps({
                            "type": best.get("intervention_type"),
                            "detail": best.get("intervention_detail"),
                        }),
                        best.get("predicted_effect", 0.0),
                        best.get("establishment_prob", 0.0),
                        run_id,
                    ),
                )
                n_runs += 1
            except Exception as exc:
                logger.debug("Run update failed for run_id=%s: %s", run_id, exc)

    conn.commit()
    conn.execute("PRAGMA synchronous=NORMAL")  # restore after commit (can't change inside tx)
    conn.close()
    return n_interventions, n_runs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main(
    ctx:           typer.Context,
    db_path:       Path          = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    n_communities: int           = typer.Option(50_000, "--n-communities", "-n"),
    workers:       int           = typer.Option(36,    "--workers", "-w"),
    batch_size:    int           = typer.Option(200,   "--batch-size"),
    log_path:      Optional[Path] = typer.Option(
        Path("/var/log/pipeline/intervention_batch.log"), "--log"
    ),
):
    """Screen interventions for T1-passed communities."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=handlers,
        force=True,
    )

    logger.info("=== Intervention screening starting: n=%d, workers=%d ===",
                n_communities, workers)

    communities = _fetch_t1_communities(str(db_path), n_communities)
    logger.info("Found %d T1-passed communities needing intervention screening", len(communities))

    if not communities:
        logger.warning("No T1-passed communities found — run t1_fba_batch.py first")
        raise typer.Exit(0)

    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    batches = list(_chunks(communities, batch_size))
    logger.info("Submitting %d batches × %d to %d workers",
                len(batches), batch_size, workers)

    t_start = time.time()
    total_interventions, total_runs = 0, 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_worker_batch, batch): idx for idx, batch in enumerate(batches)}
        for fut in as_completed(futures):
            batch_idx = futures[fut]
            try:
                batch_results = fut.result()
                n_i, n_r = _write_interventions(str(db_path), batch_results)
                total_interventions += n_i
                total_runs += n_r
                elapsed = time.time() - t_start
                rate = total_runs / elapsed if elapsed > 0 else 0
                logger.info(
                    "Batch %4d/%d done — %6d interventions, %5d runs updated "
                    "(%.1f/s, %.1f min elapsed)",
                    batch_idx + 1, len(batches),
                    total_interventions, total_runs,
                    rate, elapsed / 60,
                )
            except Exception as exc:
                logger.error("Batch %d failed: %s", batch_idx, exc)

    elapsed = time.time() - t_start
    logger.info(
        "=== Intervention screening complete: %d interventions, %d runs in %.1f min ===",
        total_interventions, total_runs, elapsed / 60,
    )


if __name__ == "__main__":
    app()
