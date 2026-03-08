"""
scripts/fva_funnel_analysis.py — FVA flux uncertainty + funnel efficiency analysis.

Two analyses in one script:

1. FVA uncertainty: uses t1_flux_lower_bound to characterize prediction intervals.
   (Upper bound = 1000 in all cases due to COBRA default cap — only lower bound varies.)
   Reports: mean/std lower bound by land_use and climate_zone.

2. Funnel analysis: counts communities at each pipeline tier (T0, T0.25, T1, T2),
   computes pass rates, and analyzes how t025_function_score separates T1-pass
   from T1-fail communities.

Output: results/fva_uncertainty.csv
        results/funnel_analysis.json
        stdout summary

Usage:
  python scripts/fva_funnel_analysis.py --db /data/pipeline/db/soil_microbiome.db
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--output-dir", default="results")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)

    # ── 1. FVA UNCERTAINTY ──────────────────────────────────────────────────
    fva_rows = conn.execute(
        """
        SELECT r.community_id, r.t1_target_flux,
               r.t1_flux_lower_bound, r.t1_flux_upper_bound,
               r.t1_model_confidence,
               s.land_use, s.climate_zone, s.soil_ph,
               s.temperature_c, s.precipitation_mm
        FROM runs r
        JOIN samples s ON r.sample_id = s.sample_id
        WHERE r.t1_pass = 1
          AND r.t1_flux_lower_bound IS NOT NULL
        """
    ).fetchall()

    fva_out_rows: list[dict] = []
    by_land_use: dict[str, list[float]] = defaultdict(list)
    by_climate: dict[str, list[float]] = defaultdict(list)

    for (cid, t1_flux, lb, ub, conf, land_use, climate, ph, temp, precip) in fva_rows:
        # Upper bound is always 1000.0 (COBRA cap) — use lb as uncertainty indicator.
        # Lower bound is negative (FVA feasibility range).
        lb_abs = abs(lb) if lb is not None else None
        # "Flux feasibility window": how wide is the range around the actual flux?
        window_low = t1_flux - abs(lb) if (t1_flux and lb) else None  # distance from worst-case
        window_high = ub - t1_flux if (ub and t1_flux) else None       # distance to cap (uninformative)

        fva_out_rows.append(
            {
                "community_id": cid,
                "land_use": land_use or "",
                "climate_zone": climate or "",
                "soil_ph": ph or "",
                "temperature_c": temp or "",
                "precipitation_mm": precip or "",
                "t1_target_flux": round(t1_flux, 4) if t1_flux else "",
                "t1_flux_lower_bound": round(lb, 4) if lb else "",
                "t1_flux_upper_bound": round(ub, 4) if ub else "",
                "model_confidence": conf or "",
                "abs_lower_bound": round(lb_abs, 4) if lb_abs else "",
                "flux_vs_lower_margin": round(window_low, 4) if window_low else "",
            }
        )
        if land_use and lb is not None:
            by_land_use[land_use].append(abs(lb))
        if climate and lb is not None:
            by_climate[climate].append(abs(lb))

    fva_file = out_dir / "fva_uncertainty.csv"
    with open(fva_file, "w", newline="") as f:
        if fva_out_rows:
            w = csv.DictWriter(f, fieldnames=fva_out_rows[0].keys())
            w.writeheader()
            w.writerows(fva_out_rows)
    print(f"Wrote {len(fva_out_rows)} FVA rows to {fva_file}")

    # ── 2. FUNNEL ANALYSIS ─────────────────────────────────────────────────
    funnel = conn.execute(
        """
        SELECT
          COUNT(*)                                         AS total_runs,
          SUM(CASE WHEN t0_pass = 1 THEN 1 ELSE 0 END)    AS t0_pass,
          SUM(CASE WHEN t025_pass = 1 THEN 1 ELSE 0 END)   AS t025_pass,
          SUM(CASE WHEN t1_pass = 1 THEN 1 ELSE 0 END)     AS t1_pass,
          SUM(CASE WHEN t2_pass = 1 THEN 1 ELSE 0 END)     AS t2_pass
        FROM runs
        """
    ).fetchone()

    total, n_t0, n_t025, n_t1, n_t2 = funnel

    # t025 function score: T1-pass vs T1-fail
    score_t1_pass = conn.execute(
        "SELECT t025_function_score FROM runs WHERE t1_pass=1 AND t025_function_score IS NOT NULL"
    ).fetchall()
    score_t1_fail = conn.execute(
        "SELECT t025_function_score FROM runs WHERE t1_pass=0 AND t025_function_score IS NOT NULL LIMIT 50000"
    ).fetchall()

    score_pass = [r[0] for r in score_t1_pass]
    score_fail = [r[0] for r in score_t1_fail]

    conn.close()

    funnel_result = {
        "total_runs": total,
        "t0_pass": n_t0,
        "t025_pass": n_t025,
        "t1_pass": n_t1,
        "t2_pass": n_t2,
        "t0_pass_rate_pct": round(100 * n_t0 / total, 1) if total else 0,
        "t025_pass_rate_pct": round(100 * (n_t025 or 0) / (n_t0 or 1), 1),
        "t1_pass_rate_pct": round(100 * (n_t1 or 0) / (n_t025 or n_t0 or 1), 1),
        "t2_pass_rate_pct": round(100 * (n_t2 or 0) / (n_t1 or 1), 1),
        "t025_score_t1pass": {
            "mean": round(statistics.mean(score_pass), 4) if score_pass else None,
            "median": round(statistics.median(score_pass), 4) if score_pass else None,
            "stdev": round(statistics.stdev(score_pass), 4) if len(score_pass) > 1 else None,
            "min": round(min(score_pass), 4) if score_pass else None,
            "max": round(max(score_pass), 4) if score_pass else None,
        },
        "t025_score_t1fail": {
            "mean": round(statistics.mean(score_fail), 4) if score_fail else None,
            "median": round(statistics.median(score_fail), 4) if score_fail else None,
            "stdev": round(statistics.stdev(score_fail), 4) if len(score_fail) > 1 else None,
            "min": round(min(score_fail), 4) if score_fail else None,
            "max": round(max(score_fail), 4) if score_fail else None,
        },
        "fva_lower_bound_by_land_use": {
            lu: {
                "mean_abs_lb": round(statistics.mean(vals), 2),
                "stdev": round(statistics.stdev(vals), 2) if len(vals) > 1 else 0,
                "n": len(vals),
            }
            for lu, vals in sorted(by_land_use.items(), key=lambda kv: -statistics.mean(kv[1]))
        },
        "fva_lower_bound_by_climate": {
            cl: {
                "mean_abs_lb": round(statistics.mean(vals), 2),
                "stdev": round(statistics.stdev(vals), 2) if len(vals) > 1 else 0,
                "n": len(vals),
            }
            for cl, vals in sorted(by_climate.items(), key=lambda kv: -statistics.mean(kv[1]))
        },
    }

    funnel_file = out_dir / "funnel_analysis.json"
    funnel_file.write_text(json.dumps(funnel_result, indent=2))
    print(f"Wrote {funnel_file}")

    # ── STDOUT SUMMARY ─────────────────────────────────────────────────────
    print("\n=== PIPELINE FUNNEL ANALYSIS ===")
    print(f"  Total runs entered         : {total:,}")
    print(f"  T0  pass (quality filter)  : {n_t0 or 0:,}  ({funnel_result['t0_pass_rate_pct']:.0f}%)")
    print(f"  T0.25 pass (ML score)      : {n_t025 or 0:,}  ({funnel_result['t025_pass_rate_pct']:.0f}% of T0-pass)")
    print(f"  T1  pass (FBA flux)        : {n_t1 or 0:,}  ({funnel_result['t1_pass_rate_pct']:.0f}% of T0.25-pass)")
    print(f"  T2  pass (dFBA stability)  : {n_t2 or 0:,}  ({funnel_result['t2_pass_rate_pct']:.0f}% of T1-pass)")

    if score_pass and score_fail:
        print(f"\n  T0.25 function score — T1-PASS communities:")
        sp = funnel_result["t025_score_t1pass"]
        print(f"    mean={sp['mean']:.3f}  median={sp['median']:.3f}  stdev={sp['stdev']:.3f}")
        print(f"  T0.25 function score — T1-FAIL communities:")
        sf = funnel_result["t025_score_t1fail"]
        print(f"    mean={sf['mean']:.3f}  median={sf['median']:.3f}  stdev={sf['stdev']:.3f}")
        separation = (sp["mean"] - sf["mean"]) / sf["stdev"] if sf["stdev"] else 0
        print(f"  Score separation (Cohen's d proxy): {separation:.2f}σ")

    print("\n  FVA flux lower bound (|lb|) by land_use:")
    for lu, stats in funnel_result["fva_lower_bound_by_land_use"].items():
        print(f"    {lu:12s}: mean_|LB|={stats['mean_abs_lb']:.1f} ± {stats['stdev']:.1f}")


if __name__ == "__main__":
    main()
