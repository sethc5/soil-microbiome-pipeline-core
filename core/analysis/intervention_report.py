"""
intervention_report.py — Generate actionable field recommendations from T2 results.

Aggregates the top-ranked interventions from the database and writes a structured
report: which organisms/amendments to apply, at what concentration/rate, in which
soil context, with predicted outcome and confidence.

Output: results/intervention_report.md (human-readable) + .json (machine-readable)

Usage:
  python intervention_report.py --config config.yaml --db nitrogen_landscape.db --top 20
"""

from __future__ import annotations
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import typer
import yaml

from core.db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _load_top_interventions(db: SoilDB, top: int) -> list[dict]:
    """Aggregate interventions from the interventions table, grouped by type/detail."""
    with db._connect() as conn:
        rows = conn.execute(
            """
            SELECT i.intervention_type, i.intervention_detail,
                   i.predicted_effect, i.confidence, i.cost_estimate,
                   s.site_id
            FROM interventions i
            JOIN runs r ON i.run_id = r.run_id
            JOIN communities c ON r.community_id = c.community_id
            JOIN samples s ON c.sample_id = s.sample_id
            WHERE i.confidence IS NOT NULL
            ORDER BY (i.confidence * i.predicted_effect) DESC
            """,
        ).fetchall()

    if not rows:
        return []

    tally: dict[str, dict] = {}
    for itype, detail_json, effect, confidence, cost_json, site_id in rows:
        try:
            detail = json.loads(detail_json) if detail_json else {}
        except Exception:
            detail = {}
        name = detail.get("intervention_detail", itype)
        if name not in tally:
            tally[name] = {
                "name": name,
                "category": itype or "unknown",
                "confidence": float(confidence or 0),
                "predicted_effect": float(effect or 0),
                "sum_score": 0.0,
                "n_communities": 0,
                "study_ids": set(),
                "cost_usd_per_ha": None,
                "mechanism": detail.get("functional_guild", ""),
                "caveats": detail.get("caveats", []),
                "rate": detail.get("rate_t_ha"),
                "unit": "t/ha" if detail.get("rate_t_ha") else None,
            }
        tally[name]["n_communities"] += 1
        score = float(confidence or 0) * float(effect or 0)
        tally[name]["sum_score"] += score
        if site_id:
            tally[name]["study_ids"].add(site_id)
        if tally[name]["cost_usd_per_ha"] is None:
            try:
                cost = json.loads(cost_json) if cost_json else {}
                tally[name]["cost_usd_per_ha"] = cost.get("usd_per_ha")
            except Exception:
                pass

    results = []
    for name, data in tally.items():
        avg_score = data["sum_score"] / max(data["n_communities"], 1)
        results.append({
            "name": name,
            "category": data["category"],
            "confidence": data["confidence"],
            "predicted_effect": data["predicted_effect"],
            "composite_score": round(avg_score, 4),
            "n_communities": data["n_communities"],
            "n_studies": len(data["study_ids"]),
            "rate": data.get("rate"),
            "unit": data.get("unit"),
            "cost_usd_per_ha": data.get("cost_usd_per_ha"),
            "mechanism": data.get("mechanism", ""),
            "caveats": data.get("caveats", []),
        })

    results.sort(key=lambda r: r["composite_score"], reverse=True)
    return results[:top]


def _render_markdown(config_path: Path, interventions: list[dict]) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cfg = yaml.safe_load(config_path.read_text()) if config_path.exists() else {}
    target = cfg.get("target_function", "target function")
    project = config_path.stem

    lines = [
        f"# Intervention Recommendations — {project}",
        f"_Generated: {now}_",
        "",
        f"Ranked by composite score (confidence × predicted {target} effect),",
        f"aggregated across all T2 simulations.",
        "",
    ]

    category_groups: dict[str, list[dict]] = defaultdict(list)
    for item in interventions:
        category_groups[item["category"]].append(item)

    category_order = ["bioinoculant", "amendment", "management", "unknown"]
    rank = 0
    for cat in category_order:
        items = category_groups.get(cat, [])
        if not items:
            continue
        lines.append(f"## {cat.title()} Interventions\n")
        for item in items:
            rank += 1
            lines.append(f"### {rank}. {item['name']}")
            lines.append(f"- **Category:** {item['category']}")
            lines.append(f"- **Confidence:** {item['confidence']:.0%}")
            lines.append(f"- **Predicted effect:** {item['predicted_effect']:.0%} improvement in {target}")
            lines.append(f"- **Composite score:** {item['composite_score']:.4f}")
            lines.append(f"- **Supported by:** {item['n_communities']} communities, {item['n_studies']} studies")
            if item.get("rate"):
                lines.append(f"- **Recommended rate:** {item['rate']} {item.get('unit', '')}")
            if item.get("cost_usd_per_ha"):
                lines.append(f"- **Estimated cost:** ${item['cost_usd_per_ha']:.0f} USD/ha")
            if item.get("mechanism"):
                lines.append(f"- **Mechanism:** {item['mechanism']}")
            caveats = item.get("caveats") or []
            if caveats:
                lines.append(f"- **Caveats:** {'; '.join(str(c) for c in caveats)}")
            lines.append("")

    lines += [
        "---",
        "> All recommendations are computational predictions requiring wet-lab and field validation.",
        "> Confidence scores reflect model quality, not probability of agronomic success.",
    ]
    return "\n".join(lines) + "\n"


@app.command()
def report(
    config: Path = typer.Option(..., help="Pipeline config YAML"),
    db: Path = typer.Option(Path("landscape.db")),
    top: int = typer.Option(20, help="Number of top interventions to report"),
    output_dir: Path = typer.Option(Path("results/")),
):
    """Write intervention report for top T2 candidates."""
    logging.basicConfig(level=logging.INFO)
    output_dir.mkdir(parents=True, exist_ok=True)
    database = SoilDB(str(db))

    interventions = _load_top_interventions(database, top)
    if not interventions:
        logger.warning("No T2 intervention data found in %s", db)
        raise typer.Exit(1)

    # Write JSON
    json_path = output_dir / "intervention_report.json"
    json_path.write_text(json.dumps(interventions, indent=2))

    # Write Markdown
    md_path = output_dir / "intervention_report.md"
    md_path.write_text(_render_markdown(config, interventions))

    logger.info("Intervention report: %d recommendations → %s", len(interventions), output_dir)
    typer.echo(f"{len(interventions)} interventions → {md_path} + {json_path}")


if __name__ == "__main__":
    app()
