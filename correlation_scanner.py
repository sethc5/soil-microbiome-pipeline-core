"""
correlation_scanner.py — Automated pattern scanning from accumulated database.

Surfaces patterns across runs:
  - Metadata correlations with T0.25 functional scores
  - Geographic clustering of top communities
  - Keystone taxa consistency across studies
  - Intervention success rate stratified by soil type
  - Loser analysis (good metadata, failed T1)

Usage:
  python correlation_scanner.py --config config.yaml --db nitrogen_landscape.db
"""

from __future__ import annotations
import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import typer

from db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _spearman_r(x: list[float], y: list[float]) -> float:
    """Compute Spearman rank correlation between two equal-length sequences."""
    n = len(x)
    if n < 3:
        return 0.0

    def _rank(seq: list[float]) -> list[float]:
        sorted_vals = sorted(enumerate(seq), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and sorted_vals[j][1] == sorted_vals[j + 1][1]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                ranks[sorted_vals[k][0]] = avg
            i = j + 1
        return ranks

    rx = _rank(x)
    ry = _rank(y)
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1 - 6 * d2 / (n * (n * n - 1))


def _scan_metadata_correlations(rows: list[dict]) -> list[dict]:
    """Spearman correlation of metadata fields vs target flux."""
    numeric_fields = ["ph", "temperature", "latitude", "longitude"]
    results = []
    fluxes = [r["t1_target_flux"] for r in rows if r.get("t1_target_flux") is not None]

    for field in numeric_fields:
        paired = [
            (r[field], r["t1_target_flux"])
            for r in rows
            if r.get(field) is not None and r.get("t1_target_flux") is not None
        ]
        if len(paired) < 5:
            continue
        xs, ys = zip(*paired)
        r = _spearman_r(list(xs), list(ys))
        results.append({
            "finding": f"metadata_correlation",
            "field": field,
            "spearman_r": round(r, 4),
            "n": len(paired),
            "direction": "positive" if r > 0 else "negative",
            "strength": "strong" if abs(r) > 0.5 else "moderate" if abs(r) > 0.3 else "weak",
        })

    return results


def _scan_intervention_rates(rows: list[dict]) -> list[dict]:
    """Stratify intervention success rate by pH category."""
    ph_bins: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        if r.get("ph") is None or r.get("t2_interventions") is None:
            continue
        ph = r["ph"]
        cat = "acidic (< 5.5)" if ph < 5.5 else "neutral (5.5–7)" if ph < 7 else "alkaline (> 7)"
        try:
            interventions = json.loads(r["t2_interventions"])
            if interventions:
                top_score = float(interventions[0].get("confidence", 0))
                ph_bins[cat].append(top_score)
        except Exception:
            pass

    return [
        {
            "finding": "intervention_by_ph",
            "ph_category": cat,
            "mean_top_confidence": round(sum(scores) / len(scores), 4),
            "n": len(scores),
        }
        for cat, scores in ph_bins.items()
    ]


def _scan_loser_analysis(rows: list[dict]) -> list[dict]:
    """Identify samples with good metadata but low T1 flux (failed model)."""
    median_flux = _median([r["t1_target_flux"] for r in rows if r.get("t1_target_flux")])
    losers = [
        r for r in rows
        if r.get("ph") and r.get("latitude") and
           r.get("t1_target_flux", float("inf")) < median_flux * 0.1
    ]
    if not losers:
        return []
    return [{
        "finding": "loser_analysis",
        "n_low_flux_with_good_metadata": len(losers),
        "potential_cause": "CarveMe model gap-fill failure or uncommon metabolism",
        "example_community_ids": [r["community_id"] for r in losers[:5]],
    }]


def _median(vals: list[float]) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    n = len(s)
    return (s[n // 2] + s[(n - 1) // 2]) / 2


@app.command()
def scan(
    config: Path = typer.Option(..., help="Pipeline config YAML"),
    db: Path = typer.Option(..., help="SQLite database path"),
    output: Path = typer.Option(Path("results/correlation_scan.json")),
):
    """Run correlation scanner against an existing landscape database."""
    logging.basicConfig(level=logging.INFO)
    database = SoilDB(str(db))

    with database._connect() as conn:
        raw_rows = conn.execute(
            """
            SELECT r.community_id, r.t1_target_flux, r.t2_stability_score,
                   r.t2_interventions, c.ph, c.temperature, c.latitude, c.longitude,
                   c.study_id, c.sample_id
            FROM runs r
            JOIN communities c ON r.community_id = c.id
            WHERE r.t0_passed = 1
            """
        ).fetchall()

    cols = ["community_id", "t1_target_flux", "t2_stability_score",
            "t2_interventions", "ph", "temperature", "latitude", "longitude",
            "study_id", "sample_id"]
    rows = [dict(zip(cols, r)) for r in raw_rows]

    if not rows:
        logger.warning("No T0-passed runs found — nothing to scan.")
        raise typer.Exit(1)

    findings: list[dict] = []
    findings.extend(_scan_metadata_correlations(rows))
    findings.extend(_scan_intervention_rates(rows))
    findings.extend(_scan_loser_analysis(rows))

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(findings, indent=2))
    logger.info("Correlation scan: %d findings → %s", len(findings), output)
    typer.echo(f"{len(findings)} pattern findings → {output}")


if __name__ == "__main__":
    app()
