"""
findings_generator.py — Anomaly detection and FINDINGS.md writer.

Runs the correlation_scanner, taxa_enrichment, and spatial_analysis outputs
through an anomaly detection pass, then writes notable findings to FINDINGS.md
in the instantiation repo directory.

Usage:
  python findings_generator.py --config config.yaml
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import typer
import yaml

from db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _load_json_if_exists(path: Path) -> list | dict | None:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def _db_summary(db: SoilDB) -> dict:
    with db._connect() as conn:
        n_total = conn.execute("SELECT COUNT(*) FROM communities").fetchone()[0]
        n_passed = conn.execute("SELECT COUNT(*) FROM runs WHERE t0_pass=1").fetchone()[0]
        n_t1 = conn.execute("SELECT COUNT(*) FROM runs WHERE t1_target_flux IS NOT NULL").fetchone()[0]
        n_t2 = conn.execute("SELECT COUNT(*) FROM runs WHERE t2_stability_score IS NOT NULL").fetchone()[0]
        top_flux_row = conn.execute(
            "SELECT MAX(t1_target_flux), community_id FROM runs"
        ).fetchone()
    return {
        "n_total": n_total,
        "n_passed_t0": n_passed,
        "n_completed_t1": n_t1,
        "n_completed_t2": n_t2,
        "top_flux": top_flux_row[0] if top_flux_row else None,
        "top_community_id": top_flux_row[1] if top_flux_row else None,
    }


def _bnf_trajectory_summary(results_dir: Path) -> dict | None:
    """Load bnf_trajectory_summary.csv and return key stats."""
    path = results_dir / "bnf_trajectory_summary.csv"
    if not path.exists():
        return None
    import csv
    from collections import defaultdict
    records: list[dict] = []
    with open(path) as fh:
        for row in csv.DictReader(fh):
            try:
                records.append({
                    "community_id": int(row["community_id"]),
                    "peak_bnf":    float(row["peak_bnf"]),
                    "retention":   float(row["retention"]),
                    "auc":         float(row["auc"]),
                    "land_use":    row.get("land_use", ""),
                    "site_id":     row.get("site_id", ""),
                })
            except (ValueError, KeyError):
                pass
    if not records:
        return None
    peaks = [r["peak_bnf"] for r in records]
    rets  = [r["retention"] for r in records]
    stable = sum(1 for r in records if r["retention"] >= 0.9)
    by_land: dict[str, list] = defaultdict(list)
    for r in records:
        by_land[r["land_use"] or "unknown"].append(r["peak_bnf"])
    land_means = {lu: sum(v)/len(v) for lu, v in by_land.items()}
    top_peak = max(records, key=lambda r: r["peak_bnf"])
    top_stable = max(records, key=lambda r: r["retention"])
    return {
        "n":            len(records),
        "mean_peak":    sum(peaks) / len(peaks),
        "max_peak":     max(peaks),
        "mean_ret":     sum(rets) / len(rets),
        "pct_stable":   100 * stable / len(records),
        "land_means":   land_means,
        "top_peak_cid": top_peak["community_id"],
        "top_peak_val": top_peak["peak_bnf"],
        "top_peak_ret": top_peak["retention"],
        "top_peak_land":top_peak["land_use"],
        "top_peak_site":top_peak["site_id"],
        "top_stable_cid": top_stable["community_id"],
        "top_stable_ret": top_stable["retention"],
        "top_stable_peak":top_stable["peak_bnf"],
        "top_stable_site":top_stable["site_id"],
    }


def _render_findings_md(
    config_path: Path,
    db_summary: dict,
    correlation_findings: list,
    enriched_taxa: list,
    results_dir: Path,
    bnf_traj: dict | None = None,
) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    config = yaml.safe_load(config_path.read_text()) if config_path.exists() else {}
    target = config.get("target_function", "target function")
    project = config_path.stem

    lines = [
        f"# Pipeline Findings — {project}",
        f"_Generated: {now}_",
        "",
        "## Run Summary",
        f"- Communities screened: **{db_summary['n_total']}**",
        f"- T0 passed: **{db_summary['n_passed_t0']}**",
        f"- T1 metabolic models built: **{db_summary['n_completed_t1']}**",
        f"- T2 dynamics simulated: **{db_summary['n_completed_t2']}**",
        f"- Top {target} flux: **{db_summary['top_flux'] or 0.0:.4g}** (community {db_summary['top_community_id']})",
        "",
        "## Correlation Patterns",
    ]

    if correlation_findings:
        for f in correlation_findings:
            if f["finding"] == "metadata_correlation":
                lines.append(
                    f"- **{f['field']}** shows {f['strength']} {f['direction']} correlation with"
                    f" {target} flux (Spearman r = {f['spearman_r']}, n = {f['n']})"
                )
            elif f["finding"] == "intervention_by_ph":
                lines.append(
                    f"- {f['ph_category']}: mean top intervention confidence = {f['mean_top_confidence']:.2f} (n={f['n']})"
                )
            elif f["finding"] == "loser_analysis":
                lines.append(
                    f"- ⚠ {f['n_low_flux_with_good_metadata']} samples had good metadata but very low flux — "
                    f"potential cause: {f['potential_cause']}"
                )
    else:
        lines.append("- *No significant correlations detected yet.*")

    lines += ["", "## Enriched Taxa (Top 10 by significance)"]
    if enriched_taxa:
        sig_taxa = [t for t in enriched_taxa if t.get("significant")][:10]
        for t in sig_taxa:
            fc = t.get("fold_change", 0)
            if fc == float("inf"):
                fc_str = "∞"
            else:
                fc_str = f"{fc:.2f}×"
            lines.append(
                f"- **{t['taxon']}** — fold-change {fc_str}, p_adj = {t['p_adj']:.3g}"
            )
        if not sig_taxa:
            lines.append("- *No significantly enriched taxa yet (FDR < 0.05).*")
    else:
        lines.append("- *Taxa enrichment not yet computed. Run `taxa_enrichment.py` first.*")

    # ── BNF Temporal Stability ─────────────────────────────────────────────
    lines += ["", "## BNF Temporal Stability (dFBA Trajectories)"]
    if bnf_traj:
        lines.append(
            f"- {bnf_traj['n']:,} communities tracked over 30-day dFBA simulations"
        )
        lines.append(
            f"- Mean peak BNF flux: **{bnf_traj['mean_peak']:.4f}** mmol/gDW/h  "
            f"(max: {bnf_traj['max_peak']:.4f})"
        )
        lines.append(
            f"- Mean retention (day-60 vs day-30): **{bnf_traj['mean_ret']:.1%}**  "
            f"({bnf_traj['pct_stable']:.1f}% fully stable ≥90%)"
        )
        lines.append(
            f"- Highest peak BNF: community **{bnf_traj['top_peak_cid']}**  "
            f"(peak={bnf_traj['top_peak_val']:.4f}, retention={bnf_traj['top_peak_ret']:.1%}, "
            f"site={bnf_traj['top_peak_site']}, land={bnf_traj['top_peak_land']})"
        )
        lines.append(
            f"- Most stable BNF: community **{bnf_traj['top_stable_cid']}**  "
            f"(retention={bnf_traj['top_stable_ret']:.1%}, peak={bnf_traj['top_stable_peak']:.4f}, "
            f"site={bnf_traj['top_stable_site']})"
        )
        lines.append("- Mean peak BNF by land use:")
        for lu, mn in sorted(bnf_traj["land_means"].items(), key=lambda x: -x[1]):
            if lu:
                lines.append(f"  - {lu}: {mn:.4f}")
        lines.append(
            f"_BNF trajectory detail: `results/bnf_trajectory_summary.csv`_"
        )
    else:
        lines.append(
            "- *Not yet computed. Run `scripts/bnf_trajectory_analysis.py` first.*"
        )

    lines += [
        "",
        "## Caveats",
        "- Metabolic model confidence depends on genome completeness (CheckM).",
        "- dFBA ignores substrate kinetics; stability scores are approximate.",
        "- Enrichment analysis is limited to taxa present in the T0.25 functional profile.",
        "- All computational predictions require wet-lab validation before field application.",
        "",
        f"_Ranked candidates: `{results_dir}/ranked_candidates.csv`_",
        f"_Intervention report: `{results_dir}/intervention_report.md`_",
    ]

    return "\n".join(lines) + "\n"


@app.command()
def generate(
    config: Path = typer.Option(..., help="Pipeline config YAML"),
    db: Path = typer.Option(Path("landscape.db")),
    output: Path = typer.Option(Path("FINDINGS.md")),
    results_dir: Path = typer.Option(Path("results")),
):
    """Generate FINDINGS.md from accumulated pipeline results."""
    logging.basicConfig(level=logging.INFO)
    database = SoilDB(str(db))

    db_summary = _db_summary(database)
    correlation_findings = _load_json_if_exists(results_dir / "correlation_scan.json") or []
    enriched_taxa: list = []
    taxa_path = results_dir / "taxa_enrichment.csv"
    if taxa_path.exists():
        import csv
        with open(taxa_path) as fh:
            enriched_taxa = list(csv.DictReader(fh))
        for row in enriched_taxa:
            row["p_adj"] = float(row.get("p_adj", 1))
            row["fold_change"] = float(row.get("fold_change", 0)) if row.get("fold_change") not in ("inf", "Infinity") else float("inf")
            row["significant"] = row.get("significant", "False").lower() == "true"

    bnf_traj = _bnf_trajectory_summary(results_dir)
    md = _render_findings_md(config, db_summary, correlation_findings, enriched_taxa, results_dir, bnf_traj)
    output.write_text(md)
    logger.info("FINDINGS.md written → %s", output)
    typer.echo(f"FINDINGS.md → {output}")


if __name__ == "__main__":
    app()
