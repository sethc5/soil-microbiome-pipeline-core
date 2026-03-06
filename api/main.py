"""
api/main.py — FastAPI results API for the soil microbiome pipeline.

Exposes read-only endpoints over the pipeline SQLite database and results
directory. Designed to run on port 8000 behind nginx.

Endpoints
---------
GET /candidates              top ranked communities (from results/ranked_candidates.csv)
GET /interventions/{cid}     screened interventions for a community
GET /findings                recent findings from the findings table
GET /stats                   funnel counts + DB row totals

Usage (dev)
-----------
    pip install -r api/requirements.txt
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

Production (systemd)
--------------------
    see deploy/pipeline-api.service
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------

DB_PATH     = Path(os.getenv("PIPELINE_DB",      "/data/pipeline/db/soil_microbiome.db"))
RESULTS_DIR = Path(os.getenv("PIPELINE_RESULTS", "/opt/pipeline/results"))
TARGET_ID   = os.getenv("PIPELINE_TARGET_ID",    "nitrogen-fixation-pipeline")

app = FastAPI(
    title="Soil Microbiome Pipeline API",
    description="Read-only access to pipeline candidates, interventions, findings, and stats.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect() -> sqlite3.Connection:
    """Return an open sqlite3 connection in read-only mode."""
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Database not found at {DB_PATH}. Pipeline may not have run yet.",
        )
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=15)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(r) for r in rows]


def _parse_json_fields(record: dict, fields: list[str]) -> dict:
    """Deserialise JSON-encoded columns so the API response is native JSON."""
    for f in fields:
        if f in record and isinstance(record[f], str):
            try:
                record[f] = json.loads(record[f])
            except (json.JSONDecodeError, TypeError):
                pass
    return record


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def root():
    return {"service": "soil-microbiome-pipeline-api", "version": "1.0.0"}


@app.get("/candidates", summary="Top ranked communities")
def get_candidates(
    limit: int = Query(100, ge=1, le=5000, description="Max rows to return"),
    min_score: float = Query(0.0, ge=0.0, le=1.0, description="Minimum composite_score filter"),
):
    """
    Return the top-ranked communities from ``results/ranked_candidates.csv``.
    Produced by Phase 6 (rank_candidates.py). Falls back to a live DB query
    if the CSV hasn't been written yet.
    """
    csv_path = RESULTS_DIR / "ranked_candidates.csv"

    if csv_path.exists():
        results: list[dict] = []
        with open(csv_path, newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                score = float(row.get("composite_score", 0) or 0)
                if score < min_score:
                    continue
                # Coerce numeric strings
                for col in ("rank", "community_id"):
                    if col in row:
                        try:
                            row[col] = int(row[col])
                        except (ValueError, TypeError):
                            pass
                for col in ("composite_score", "t1_target_flux", "t2_stability_score",
                            "bnf_score", "ph", "organic_matter", "temperature_c",
                            "precipitation_mm", "latitude", "longitude"):
                    if col in row:
                        try:
                            row[col] = float(row[col])
                        except (ValueError, TypeError):
                            pass
                results.append(row)
                if len(results) >= limit:
                    break
        return {"source": "csv", "n": len(results), "candidates": results}

    # Fallback: live DB query
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT r.community_id,
                   r.t1_target_flux,
                   r.t2_stability_score,
                   r.t025_function_score AS bnf_score,
                   s.soil_ph AS ph,
                   s.organic_matter_pct AS organic_matter,
                   s.temperature_c,
                   s.latitude,
                   s.longitude,
                   s.site_id,
                   s.land_use
            FROM runs r
            JOIN communities c ON r.community_id = c.community_id
            JOIN samples     s ON r.sample_id   = s.sample_id
            WHERE r.t1_target_flux IS NOT NULL
            ORDER BY r.t1_target_flux DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    return {"source": "db", "n": len(rows), "candidates": _rows_to_dicts(rows)}


@app.get("/interventions/{community_id}", summary="Interventions for a community")
def get_interventions(community_id: int):
    """
    Return all screened interventions for *community_id*, ordered by
    ``predicted_effect`` descending.
    """
    conn = _connect()
    try:
        # Resolve community → most recent run
        run_row = conn.execute(
            "SELECT run_id FROM runs WHERE community_id = ? ORDER BY run_id DESC LIMIT 1",
            (community_id,),
        ).fetchone()
        if run_row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No runs found for community_id={community_id}",
            )
        run_id = run_row["run_id"]

        rows = conn.execute(
            """
            SELECT intervention_id, run_id, intervention_type,
                   intervention_detail, predicted_effect, confidence,
                   stability_under_perturbation, cost_estimate, created_at
            FROM interventions
            WHERE run_id = ?
            ORDER BY predicted_effect DESC
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    data = [
        _parse_json_fields(dict(r), ["intervention_detail", "cost_estimate"])
        for r in rows
    ]

    return {
        "community_id": community_id,
        "run_id": run_id,
        "n_interventions": len(data),
        "interventions": data,
    }


@app.get("/findings", summary="Recent pipeline findings")
def get_findings(
    limit: int = Query(50, ge=1, le=500, description="Max findings to return"),
):
    """Return the most recent findings, newest first."""
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT finding_id, title, description,
                   sample_ids, taxa_ids, statistical_support, created_at
            FROM findings
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    data = [
        _parse_json_fields(dict(r), ["sample_ids", "taxa_ids", "statistical_support"])
        for r in rows
    ]
    return {"n": len(data), "findings": data}


@app.get("/stats", summary="Pipeline funnel stats and DB row counts")
def get_stats():
    """
    Return high-level pipeline statistics:
    - Row counts for every table
    - T0→T0.25→T1→T2 funnel pass counts for the active target
    - Top site by mean T1 flux
    """
    conn = _connect()
    try:
        tables = ["samples", "communities", "targets", "runs",
                  "interventions", "taxa", "findings", "receipts"]
        counts: dict[str, int] = {}
        for tbl in tables:
            try:
                n = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]  # noqa: S608
                counts[tbl] = n
            except sqlite3.OperationalError:
                counts[tbl] = -1

        # Funnel
        funnel: dict[str, int] = {}
        for tier_col in ("t0_pass", "t025_pass", "t1_pass", "t2_pass"):
            try:
                n = conn.execute(
                    f"SELECT COUNT(*) FROM runs WHERE {tier_col} = 1"  # noqa: S608
                ).fetchone()[0]
                funnel[tier_col] = n
            except sqlite3.OperationalError:
                funnel[tier_col] = -1

        # Top 5 sites by mean T1 flux
        top_sites: list[dict] = []
        try:
            site_rows = conn.execute(
                """
                SELECT s.site_id,
                       COUNT(*) AS n_communities,
                       AVG(r.t1_target_flux) AS mean_flux,
                       AVG(r.t2_stability_score) AS mean_stability
                FROM runs r
                JOIN communities c ON r.community_id = c.community_id
                JOIN samples     s ON r.sample_id    = s.sample_id
                WHERE r.t1_target_flux IS NOT NULL
                  AND s.site_id IS NOT NULL
                GROUP BY s.site_id
                ORDER BY mean_flux DESC
                LIMIT 5
                """
            ).fetchall()
            top_sites = _rows_to_dicts(site_rows)
        except sqlite3.OperationalError:
            pass

        # Latest receipt
        latest_receipt: dict | None = None
        try:
            rec = conn.execute(
                "SELECT * FROM receipts ORDER BY batch_end DESC LIMIT 1"
            ).fetchone()
            if rec:
                latest_receipt = dict(rec)
        except sqlite3.OperationalError:
            pass

    finally:
        conn.close()

    return {
        "target_id":      TARGET_ID,
        "db_path":        str(DB_PATH),
        "table_counts":   counts,
        "funnel":         funnel,
        "top_sites":      top_sites,
        "latest_receipt": latest_receipt,
    }


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
