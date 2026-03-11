"""
scripts/make_reference_bnf.py — Generate reference BNF measurements CSV from real DB data.

Pulls two balanced cohorts from the pipeline DB:
  - HIGH BNF: top N communities by t1_target_flux (real FBA-derived BNF rate)
  - LOW  BNF: communities that passed T0 but failed T1 (flux = 0.0)

Saves reference/bnf_measurements.csv with columns:
  sample_id, measured_function

The "measured_function" column represents BNF rate in mmol N2/gDW/h.
For HIGH cohort it is the actual t1_target_flux from the DB.
For LOW cohort it is 0.0 (community did not support nitrogen fixation in FBA).

This file is gitignored (reference/*.csv) but reproducible from the DB.
Run this once on the server before calling validate_pipeline.py.

Usage:
    python scripts/make_reference_bnf.py \
        --db /data/pipeline/db/soil_microbiome.db \
        --out reference/bnf_measurements.csv \
        --n-high 150 \
        --n-low 150
"""

from __future__ import annotations

import csv
import logging
import sqlite3
from pathlib import Path

import typer

app = typer.Typer(add_completion=False)
logger = logging.getLogger(__name__)


def _query_high_bnf(conn: sqlite3.Connection, n: int) -> list[tuple[str, float]]:
    """Return (sample_id, t1_target_flux) for top-N communities by BNF flux."""
    rows = conn.execute(
        """
        SELECT c.sample_id, r.t1_target_flux
        FROM runs r
        JOIN communities c ON r.community_id = c.community_id
        WHERE r.t1_pass = 1
          AND r.t1_target_flux IS NOT NULL
          AND c.sample_id IS NOT NULL
        ORDER BY r.t1_target_flux DESC
        LIMIT ?
        """,
        (n,),
    ).fetchall()
    return [(row[0], float(row[1])) for row in rows]


def _query_low_bnf(conn: sqlite3.Connection, n: int, exclude_ids: set[str]) -> list[tuple[str, float]]:
    """Return (sample_id, 0.0) for T0-pass/T1-fail communities (low BNF potential)."""
    rows = conn.execute(
        """
        SELECT DISTINCT c.sample_id
        FROM runs r
        JOIN communities c ON r.community_id = c.community_id
        WHERE r.t0_pass = 1
          AND (r.t1_pass = 0 OR r.t1_pass IS NULL)
          AND c.sample_id IS NOT NULL
        ORDER BY RANDOM()
        LIMIT ?
        """,
        (n * 3,),  # over-sample to allow exclusion of high-BNF ids
    ).fetchall()

    result = []
    for (sid,) in rows:
        if sid not in exclude_ids:
            result.append((sid, 0.0))
        if len(result) >= n:
            break
    return result


@app.command()
def make_reference(
    db: Path = typer.Option(..., help="Path to soil_microbiome.db"),
    out: Path = typer.Option(Path("reference/bnf_measurements.csv"), help="Output CSV path"),
    n_high: int = typer.Option(150, help="Number of high-BNF communities to include"),
    n_low: int = typer.Option(150, help="Number of low-BNF communities to include"),
) -> None:
    """Generate reference/bnf_measurements.csv from real pipeline DB FBA results."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not db.exists():
        typer.echo(f"ERROR: DB not found at {db}", err=True)
        raise typer.Exit(1)

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row

    logger.info("Querying high-BNF cohort (top %d by t1_target_flux)...", n_high)
    high_rows = _query_high_bnf(conn, n_high)
    logger.info("  Found %d high-BNF records", len(high_rows))

    high_ids = {sid for sid, _ in high_rows}

    logger.info("Querying low-BNF cohort (T0-pass / T1-fail, n=%d)...", n_low)
    low_rows = _query_low_bnf(conn, n_low, exclude_ids=high_ids)
    logger.info("  Found %d low-BNF records", len(low_rows))

    conn.close()

    all_rows = high_rows + low_rows

    # Deduplicate sample_id (keep first occurrence = highest flux)
    seen: set[str] = set()
    deduped: list[tuple[str, float]] = []
    for sid, val in all_rows:
        if sid not in seen:
            seen.add(sid)
            deduped.append((sid, val))

    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["sample_id", "measured_function"])
        for sid, val in deduped:
            writer.writerow([sid, f"{val:.6f}"])

    n_high_written = sum(1 for _, v in deduped if v > 0)
    n_low_written = sum(1 for _, v in deduped if v == 0.0)

    logger.info(
        "Wrote %d communities to %s  (%d high-BNF, %d low-BNF)",
        len(deduped), out, n_high_written, n_low_written,
    )
    typer.echo(f"Reference CSV written: {out}  ({len(deduped)} communities)")
    typer.echo(f"  High-BNF (t1 flux > 0): {n_high_written}")
    typer.echo(f"  Low-BNF  (flux = 0.0) : {n_low_written}")
    typer.echo(
        "\nThis CSV represents FBA-derived BNF flux, not wet-lab ARA measurements.\n"
        "It validates that the T0.25 surrogate predictor is consistent with\n"
        "the T1 FBA stage (internal forward-validation loop)."
    )


if __name__ == "__main__":
    app()
