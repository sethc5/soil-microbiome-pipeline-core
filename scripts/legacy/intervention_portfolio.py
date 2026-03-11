"""
scripts/intervention_portfolio.py — Full analysis of the interventions table.

Analyzes 200,000 screened interventions across 20,000 T2-pass communities,
comparing effectiveness, confidence, stability, and cost across:
  - bioinoculant interventions
  - amendment interventions
  - management interventions

Output: results/intervention_portfolio.csv  (per-intervention, sampled/aggregated)
        results/intervention_type_summary.csv  (3-row type summary)
        stdout narrative summary

Usage:
  python scripts/intervention_portfolio.py --db /data/pipeline/db/soil_microbiome.db
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


def _safe_cost(cost_json: str | None) -> float | None:
    """Return cost in USD/ha from cost_estimate JSON or None."""
    if not cost_json:
        return None
    try:
        d = json.loads(cost_json)
        return float(d.get("usd_per_ha", d.get("usd_per_treatment", 0)))
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def main() -> None:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)

    # Full intervention table joined with community metadata
    rows = conn.execute(
        """
        SELECT i.intervention_type, i.intervention_detail, i.predicted_effect,
               i.confidence, i.stability_under_perturbation, i.cost_estimate,
               s.land_use, s.climate_zone, s.soil_ph, s.temperature_c,
               r.t1_target_flux, r.t2_stability_score
        FROM interventions i
        JOIN runs r ON i.run_id = r.run_id
        JOIN samples s ON r.sample_id = s.sample_id
        """
    ).fetchall()
    conn.close()

    print(f"Total interventions: {len(rows):,}")

    # Bucket by type
    by_type: dict[str, list[dict]] = defaultdict(list)
    for (itype, idetail, effect, conf, stab, cost_json,
         land_use, climate, ph, temp_c, t1_flux, t2_stab) in rows:
        cost = _safe_cost(cost_json)
        by_type[itype or "unknown"].append(
            {
                "intervention_type": itype or "unknown",
                "intervention_detail": idetail or "",
                "predicted_effect": effect,
                "confidence": conf,
                "stability_under_perturbation": stab,
                "cost_usd_per_ha": cost,
                "land_use": land_use or "",
                "climate_zone": climate or "",
                "soil_ph": ph,
                "temperature_c": temp_c,
                "t1_target_flux": t1_flux,
                "t2_stability_score": t2_stab,
            }
        )

    # Per-type summary
    type_summary_rows: list[dict] = []
    for itype, records in sorted(by_type.items()):
        effects = [r["predicted_effect"] for r in records if r["predicted_effect"] is not None]
        confs = [r["confidence"] for r in records if r["confidence"] is not None]
        stabs = [r["stability_under_perturbation"] for r in records if r["stability_under_perturbation"] is not None]
        costs = [r["cost_usd_per_ha"] for r in records if r["cost_usd_per_ha"] is not None]

        # Cost-effectiveness: effect per dollar (if costs available)
        cost_eff_vals = [
            r["predicted_effect"] / r["cost_usd_per_ha"]
            for r in records
            if r["predicted_effect"] and r["cost_usd_per_ha"] and r["cost_usd_per_ha"] > 0
        ]

        type_summary_rows.append(
            {
                "intervention_type": itype,
                "n_interventions": len(records),
                "mean_predicted_effect": round(statistics.mean(effects), 4) if effects else "",
                "max_predicted_effect": round(max(effects), 4) if effects else "",
                "mean_confidence": round(statistics.mean(confs), 4) if confs else "",
                "mean_stability": round(statistics.mean(stabs), 4) if stabs else "",
                "mean_cost_usd_per_ha": round(statistics.mean(costs), 2) if costs else "",
                "min_cost_usd_per_ha": round(min(costs), 2) if costs else "",
                "max_cost_usd_per_ha": round(max(costs), 2) if costs else "",
                "mean_cost_effectiveness": round(statistics.mean(cost_eff_vals), 6) if cost_eff_vals else "",
            }
        )

    type_out = out_dir / "intervention_type_summary.csv"
    with open(type_out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=type_summary_rows[0].keys())
        w.writeheader()
        w.writerows(type_summary_rows)
    print(f"Wrote {type_out}")

    # Best intervention per land_use × type combination
    combo_rows: list[dict] = []
    land_use_type: dict[tuple, list[dict]] = defaultdict(list)
    for itype, records in by_type.items():
        for r in records:
            land_use_type[(r["land_use"], itype)].append(r)

    best_combos: list[dict] = []
    for (lu, itype), records in land_use_type.items():
        effects = [r["predicted_effect"] for r in records if r["predicted_effect"] is not None]
        if not effects:
            continue
        best_eff = max(effects)
        best = next(r for r in records if r["predicted_effect"] == best_eff)
        best_combos.append(
            {
                "land_use": lu,
                "intervention_type": itype,
                "n_interventions": len(records),
                "mean_effect": round(statistics.mean(effects), 4),
                "best_effect": round(best_eff, 4),
                "best_detail": best["intervention_detail"],
                "confidence": round(best["confidence"], 3) if best["confidence"] else "",
                "cost_usd_per_ha": best["cost_usd_per_ha"],
            }
        )

    # Overall portfolio CSV (sampled — up to 1000 rows per type for manageability)
    all_records = []
    for records in by_type.values():
        # sample top-effect rows
        by_effect = sorted(records, key=lambda r: r.get("predicted_effect") or 0, reverse=True)
        all_records.extend(by_effect[:1000])

    portfolio_out = out_dir / "intervention_portfolio.csv"
    with open(portfolio_out, "w", newline="") as f:
        if all_records:
            w = csv.DictWriter(f, fieldnames=all_records[0].keys())
            w.writeheader()
            w.writerows(all_records)
    print(f"Wrote {len(all_records)} sample rows to {portfolio_out}")

    # Stdout summary
    print("\n=== INTERVENTION PORTFOLIO SUMMARY ===")
    for r in type_summary_rows:
        print(f"\n  [{r['intervention_type'].upper()}]")
        print(f"    Count             : {r['n_interventions']:,}")
        print(f"    Mean effect       : {r['mean_predicted_effect']}")
        print(f"    Max effect        : {r['max_predicted_effect']}")
        print(f"    Mean confidence   : {r['mean_confidence']}")
        print(f"    Mean stability    : {r['mean_stability']}")
        if r['mean_cost_usd_per_ha']:
            print(f"    Cost range ($/ha) : {r['min_cost_usd_per_ha']} — {r['max_cost_usd_per_ha']}")
            print(f"    Cost-effectiveness: {r['mean_cost_effectiveness']:.5f} effect/dollar")

    print("\n  Best intervention by land_use × type:")
    for c in sorted(best_combos, key=lambda x: -x["mean_effect"]):
        cost_str = f"${c['cost_usd_per_ha']}/ha" if c["cost_usd_per_ha"] else "N/A"
        print(
            f"    {c['land_use']:12s} × {c['intervention_type']:14s} — "
            f"mean_effect={c['mean_effect']:.3f}  cost={cost_str}"
        )


if __name__ == "__main__":
    main()
