"""
scripts/bnf_trajectory_analysis.py — Analyse dFBA BNF time-series stored in
runs.t2_bnf_trajectory.

For each community with a trajectory compute:
  peak_bnf      – day-30 flux (first measured point)
  final_bnf     – day-60 flux (last measured point)
  retention     – final_bnf / peak_bnf  (1 = fully stable, 0 = collapsed)
  decline_rate  – (peak - final) / peak
  auc           – trapezoidal area under days [30,50,60] curve

Writes results/bnf_trajectory_summary.csv and prints top findings.

Usage:
  python scripts/bnf_trajectory_analysis.py \
      --db /data/pipeline/db/soil_microbiome.db \
      --output results/bnf_trajectory_summary.csv
"""
from __future__ import annotations

import csv
import json
import logging
import sqlite3
import sys
from pathlib import Path
from statistics import mean, median, stdev

import typer

app = typer.Typer(add_completion=False)
logger = logging.getLogger(__name__)


def _trapz(xs: list[float], ys: list[float]) -> float:
    """Trapezoidal integration."""
    total = 0.0
    for i in range(1, len(xs)):
        total += (xs[i] - xs[i - 1]) * (ys[i] + ys[i - 1]) / 2
    return total


def _parse_trajectory(json_str: str) -> list[dict]:
    try:
        pts = json.loads(json_str)
        if isinstance(pts, list) and pts:
            return sorted(pts, key=lambda p: p.get("day", 0))
    except Exception:
        pass
    return []


@app.command()
def main(
    db: Path = typer.Option(Path("/data/pipeline/db/soil_microbiome.db")),
    output: Path = typer.Option(Path("results/bnf_trajectory_summary.csv")),
    top_n: int = typer.Option(20, help="Number of top communities to highlight"),
):
    """Analyse BNF dFBA trajectories and write summary CSV + findings."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT r.community_id, r.t2_bnf_trajectory, r.t1_target_flux,
               s.site_id, s.latitude, s.longitude, s.soil_ph,
               s.land_use, s.climate_zone, s.temperature_c, s.precipitation_mm
        FROM runs r
        JOIN samples s ON r.sample_id = s.sample_id
        WHERE r.t2_bnf_trajectory IS NOT NULL
          AND r.t2_bnf_trajectory != ''
        """
    ).fetchall()
    conn.close()

    records = []
    skipped = 0
    for row in rows:
        pts = _parse_trajectory(row["t2_bnf_trajectory"])
        if len(pts) < 2:
            skipped += 1
            continue

        days = [p["day"] for p in pts]
        fluxes = [p["bnf_flux"] for p in pts]

        peak = max(fluxes)
        final = fluxes[-1]
        peak_day = days[fluxes.index(peak)]
        retention = final / peak if peak > 0 else 0.0
        decline_rate = 1.0 - retention
        auc = _trapz(days, fluxes)

        records.append({
            "community_id": row["community_id"],
            "t1_target_flux": row["t1_target_flux"],
            "peak_bnf": round(peak, 6),
            "peak_day": peak_day,
            "final_bnf": round(final, 6),
            "retention": round(retention, 4),
            "decline_rate": round(decline_rate, 4),
            "auc": round(auc, 4),
            "n_timepoints": len(pts),
            "site_id": row["site_id"] or "",
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "soil_ph": row["soil_ph"],
            "land_use": row["land_use"] or "",
            "climate_zone": row["climate_zone"] or "",
            "temperature_c": row["temperature_c"],
            "precipitation_mm": row["precipitation_mm"],
        })

    if not records:
        logger.error("No trajectory records found.")
        raise typer.Exit(1)

    logger.info("Parsed %d trajectories, skipped %d", len(records), skipped)

    # Write CSV
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(records[0].keys())
    with open(output, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    logger.info("Wrote %s", output)

    # ── Summary stats ─────────────────────────────────────────────────────
    peaks       = [r["peak_bnf"] for r in records]
    retentions  = [r["retention"] for r in records]
    aucs        = [r["auc"] for r in records]

    print(f"\n{'='*60}")
    print(f"BNF Trajectory Analysis — {len(records):,} communities")
    print(f"{'='*60}")
    print(f"Peak BNF flux:    mean={mean(peaks):.4f}  median={median(peaks):.4f}  "
          f"max={max(peaks):.4f}  min={min(peaks):.4f}  stdev={stdev(peaks):.4f}")
    print(f"Retention (d60/d30): mean={mean(retentions):.3f}  "
          f"median={median(retentions):.3f}  stdev={stdev(retentions):.4f}")
    print(f"AUC:              mean={mean(aucs):.3f}  max={max(aucs):.3f}")

    # Top by peak BNF
    top_peak = sorted(records, key=lambda r: r["peak_bnf"], reverse=True)[:top_n]
    print(f"\nTop {top_n} by peak BNF flux:")
    for i, r in enumerate(top_peak, 1):
        print(f"  {i:2d}. cid={r['community_id']:6d}  peak={r['peak_bnf']:.4f}  "
              f"retention={r['retention']:.3f}  site={r['site_id']}  land={r['land_use']}")

    # Top by retention (most stable)
    top_stable = sorted(records, key=lambda r: r["retention"], reverse=True)[:top_n]
    print(f"\nTop {top_n} by retention (most stable BNF):")
    for i, r in enumerate(top_stable, 1):
        print(f"  {i:2d}. cid={r['community_id']:6d}  retention={r['retention']:.4f}  "
              f"peak={r['peak_bnf']:.4f}  site={r['site_id']}  land={r['land_use']}")

    # Land use breakdown
    from collections import defaultdict
    by_land: dict[str, list] = defaultdict(list)
    for r in records:
        by_land[r["land_use"] or "unknown"].append(r["peak_bnf"])

    print(f"\nMean peak BNF by land use:")
    for lu, vals in sorted(by_land.items(), key=lambda x: -mean(x[1])):
        print(f"  {lu:20s}  n={len(vals):5d}  mean_peak={mean(vals):.4f}")

    # Retention distribution
    stable = sum(1 for r in records if r["retention"] >= 0.9)
    unstable = sum(1 for r in records if r["retention"] < 0.7)
    print(f"\nRetention distribution:")
    print(f"  Stable (≥0.9):   {stable:5d} ({100*stable/len(records):.1f}%)")
    print(f"  Moderate (0.7–0.9): {len(records)-stable-unstable:5d} "
          f"({100*(len(records)-stable-unstable)/len(records):.1f}%)")
    print(f"  Unstable (<0.7): {unstable:5d} ({100*unstable/len(records):.1f}%)")

    print(f"\nOutput: {output}")


if __name__ == "__main__":
    app()
