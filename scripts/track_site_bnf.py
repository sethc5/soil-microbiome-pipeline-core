"""
scripts/track_site_bnf.py — Time-series BNF trajectory per NEON site.

For each site with ≥2 visits, queries all communities and their T1 BNF
fluxes in chronological visit order, then outputs a CSV suitable for
temporal trend analysis.

Schema used:
  samples.site_id          -- stable NEON site identifier (e.g. "CLBJ")
  samples.visit_number     -- chronological order at site (1 = first visit)
  samples.sampling_date    -- ISO date string
  communities.community_id -- for run lookup
  runs.t1_target_flux      -- BNF flux (mmol N2/gDW/h)
  runs.t1_pass             -- tier 1 passed flag

Output CSV columns:
  site_id, visit_number, sampling_date, n_communities, n_t1_pass,
  mean_bnf_flux, max_bnf_flux, std_bnf_flux

Usage:
    python scripts/track_site_bnf.py \\
        --db /data/pipeline/db/soil_microbiome.db \\
        --out results/site_bnf_timeseries.csv \\
        --min-visits 2

"""
from __future__ import annotations

import csv
import logging
import sqlite3
import statistics
from pathlib import Path

import typer

app = typer.Typer(add_completion=False)
logger = logging.getLogger(__name__)


def _query_multi_visit_sites(conn: sqlite3.Connection, min_visits: int) -> list[str]:
    """Return site_ids with at least min_visits distinct visit_number values."""
    rows = conn.execute(
        """
        SELECT site_id, COUNT(DISTINCT visit_number) AS n_visits
        FROM samples
        WHERE site_id IS NOT NULL
          AND visit_number IS NOT NULL
        GROUP BY site_id
        HAVING n_visits >= ?
        ORDER BY n_visits DESC
        """,
        (min_visits,),
    ).fetchall()
    return [r[0] for r in rows]


def _query_site_visit_bnf(conn: sqlite3.Connection, site_id: str) -> list[dict]:
    """
    Return per-visit BNF summary for a site.

    Joins samples → communities → runs and aggregates by visit_number.
    """
    rows = conn.execute(
        """
        SELECT
            s.visit_number,
            s.sampling_date,
            COUNT(DISTINCT c.community_id)          AS n_communities,
            SUM(CASE WHEN r.t1_pass = 1 THEN 1 ELSE 0 END) AS n_t1_pass,
            AVG(CASE WHEN r.t1_pass = 1 THEN r.t1_target_flux ELSE NULL END) AS mean_bnf_flux,
            MAX(r.t1_target_flux)                   AS max_bnf_flux,
            GROUP_CONCAT(r.t1_target_flux)          AS all_fluxes
        FROM samples s
        JOIN communities c ON c.sample_id = s.sample_id
        LEFT JOIN runs r    ON r.community_id = c.community_id
        WHERE s.site_id = ?
          AND s.visit_number IS NOT NULL
        GROUP BY s.visit_number, s.sampling_date
        ORDER BY s.visit_number ASC
        """,
        (site_id,),
    ).fetchall()

    results = []
    for row in rows:
        visit_number, sampling_date, n_comm, n_pass, mean_flux, max_flux, all_str = row

        # Compute std_dev from GROUP_CONCAT list
        flux_vals: list[float] = []
        if all_str:
            for v in all_str.split(","):
                try:
                    flux_vals.append(float(v))
                except ValueError:
                    pass
        std_flux = statistics.stdev(flux_vals) if len(flux_vals) >= 2 else 0.0

        results.append({
            "visit_number": visit_number,
            "sampling_date": sampling_date or "",
            "n_communities": n_comm,
            "n_t1_pass": n_pass or 0,
            "mean_bnf_flux": round(mean_flux, 4) if mean_flux else 0.0,
            "max_bnf_flux": round(max_flux, 4) if max_flux else 0.0,
            "std_bnf_flux": round(std_flux, 4),
        })
    return results


@app.command()
def track(
    db: Path = typer.Option(..., help="Path to soil_microbiome.db"),
    out: Path = typer.Option(
        Path("results/site_bnf_timeseries.csv"),
        help="Output CSV path",
    ),
    min_visits: int = typer.Option(2, help="Minimum number of visits to include a site"),
) -> None:
    """Generate per-site BNF time-series from multi-visit NEON communities."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if not db.exists():
        typer.echo(f"ERROR: DB not found at {db}", err=True)
        raise typer.Exit(1)

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row

    logger.info("Finding sites with >= %d visits...", min_visits)
    sites = _query_multi_visit_sites(conn, min_visits)
    logger.info("  Found %d qualifying sites", len(sites))

    if not sites:
        typer.echo(
            f"No sites found with >= {min_visits} visits.  "
            "Ensure samples.site_id and samples.visit_number are populated."
        )
        conn.close()
        raise typer.Exit(0)

    out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "site_id", "visit_number", "sampling_date",
        "n_communities", "n_t1_pass",
        "mean_bnf_flux", "max_bnf_flux", "std_bnf_flux",
    ]
    total_rows = 0

    with open(out, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for site_id in sites:
            visits = _query_site_visit_bnf(conn, site_id)
            for visit in visits:
                writer.writerow({"site_id": site_id, **visit})
                total_rows += 1
            logger.info(
                "Site %-8s: %d visits, mean flux range [%.2f – %.2f]",
                site_id,
                len(visits),
                min((v["mean_bnf_flux"] for v in visits), default=0.0),
                max((v["mean_bnf_flux"] for v in visits), default=0.0),
            )

    conn.close()

    typer.echo(f"\nTime-series CSV written: {out}")
    typer.echo(f"  Sites: {len(sites)}   Rows: {total_rows}")
    typer.echo(
        "\nTip: plot site_id vs visit_number coloured by mean_bnf_flux "
        "to visualise BNF trajectory across NEON revisits."
    )


if __name__ == "__main__":
    app()
