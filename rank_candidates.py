"""
rank_candidates.py — Score T1/T2 communities and rank intervention strategies.

Reads run results from the database and produces a ranked list of communities
and associated interventions ordered by composite score:
  target_flux × stability × establishment_probability

Usage:
  python rank_candidates.py --config config.yaml --db nitrogen_landscape.db --top 50
"""

from __future__ import annotations
import csv
import json
import logging
import math
from pathlib import Path

import typer

from db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _composite_score(row: dict) -> float:
    """Compute composite ranking score from a runs-table row dict.

    Score = normalised_flux * stability * confidence
    All factors clamped to [0, 1]; undefined values treated as 0.5 (neutral).
    """
    # Target flux — log-normalise to [0,1] using soft cap of 1000 mmol/gDW/h
    flux = float(row.get("t1_target_flux") or 0.0)
    flux_score = min(1.0, math.log1p(max(flux, 0)) / math.log1p(1000.0))

    # Stability score from T2
    stability = float(row.get("t2_stability_score") or 0.5)
    stability = max(0.0, min(1.0, stability))

    # Model confidence: high=0.90, medium=0.65, low=0.35, numeric passthrough
    conf_raw = row.get("t1_model_confidence", "medium")
    if isinstance(conf_raw, str):
        conf = {"high": 0.90, "medium": 0.65, "low": 0.35}.get(conf_raw.lower(), 0.5)
    else:
        conf = max(0.0, min(1.0, float(conf_raw or 0.5)))

    return flux_score * stability * conf


@app.command()
def rank(
    config: Path = typer.Option(..., help="Pipeline config YAML"),
    db: Path = typer.Option(Path("landscape.db"), help="SQLite database path"),
    top: int = typer.Option(50, help="Number of top candidates to report"),
    output: Path = typer.Option(Path("results/ranked_candidates.csv")),
):
    """Rank communities and interventions from accumulated run results."""
    logging.basicConfig(level=logging.INFO)
    database = SoilDB(str(db))

    # Retrieve all completed T1/T2 runs
    with database._connect() as conn:
        rows = conn.execute(
            """
            SELECT r.run_id, r.community_id, r.run_date,
                   r.t0_pass, r.t0_depth_ok,
                   r.t025_model, r.t025_n_pathways, r.t025_nsti_mean,
                   r.t1_target_flux, r.t1_model_confidence, r.t1_metabolic_exchanges,
                   r.t2_stability_score, r.t2_resistance, r.t2_resilience,
                   r.t2_functional_redundancy, r.t2_interventions,
                   c.sample_id, s.site_id, s.latitude, s.longitude,
                   s.soil_ph, s.temperature_c
            FROM runs r
            JOIN communities c ON r.community_id = c.community_id
            JOIN samples s ON c.sample_id = s.sample_id
            WHERE r.t1_target_flux IS NOT NULL
            ORDER BY r.community_id
            """
        ).fetchall()

    if not rows:
        logger.warning("No T1 results found in %s — nothing to rank.", db)
        raise typer.Exit(1)

    col_names = [
        "run_id", "community_id", "run_date", "t0_pass", "t0_depth_ok",
        "t025_model", "t025_n_pathways", "t025_nsti_mean",
        "t1_target_flux", "t1_model_confidence", "t1_metabolic_exchanges",
        "t2_stability_score", "t2_resistance", "t2_resilience",
        "t2_functional_redundancy", "t2_interventions",
        "sample_id", "site_id", "latitude", "longitude", "soil_ph", "temperature_c",
    ]

    records = []
    for row in rows:
        d = dict(zip(col_names, row))
        d["composite_score"] = _composite_score(d)
        # Decode intervention list if stored as JSON
        if isinstance(d.get("t2_interventions"), str):
            try:
                d["top_intervention"] = json.loads(d["t2_interventions"])[0].get("name", "")
            except Exception:
                d["top_intervention"] = ""
        else:
            d["top_intervention"] = ""
        records.append(d)

    records.sort(key=lambda r: r["composite_score"], reverse=True)
    top_records = records[:top]

    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank", "community_id", "sample_id", "study_id",
        "composite_score", "t1_target_flux", "t1_model_confidence",
        "t2_stability_score", "t2_resistance", "t2_resilience",
        "t2_functional_redundancy", "top_intervention",
        "latitude", "longitude", "soil_ph", "temperature_c",
    ]
    with open(output, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for i, rec in enumerate(top_records, start=1):
            rec["rank"] = i
            writer.writerow(rec)

    logger.info("Ranked %d candidates → %s", len(top_records), output)
    typer.echo(f"Top {len(top_records)} candidates written to {output}")


if __name__ == "__main__":
    app()
