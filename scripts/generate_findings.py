"""
scripts/generate_findings.py — Phase 16: Populate ``findings`` table from analysis outputs.

Reads the JSON/CSV outputs produced by ``analysis_pipeline.py`` and the
existing ``findings_generator.py`` library, then inserts structured finding
rows into the ``findings`` DB table.

Also generates / regenerates ``FINDINGS.md`` from the accumulated data.

Usage:
  python scripts/generate_findings.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --results-dir /opt/pipeline/results
"""
from __future__ import annotations

import csv
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect  # noqa: E402

logger = logging.getLogger(__name__)
app = typer.Typer(
    help="Populate findings table from analysis pipeline outputs",
    add_completion=False,
    invoke_without_command=True,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> list | dict | None:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None


def _load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# Finding extractors
# ---------------------------------------------------------------------------

def _extract_correlation_findings(results_dir: Path) -> list[dict]:
    """Convert correlation_findings.json → list of DB finding dicts."""
    data = _load_json(results_dir / "correlation_findings.json")
    if not data or not isinstance(data, list):
        return []

    findings = []
    for f in data:
        ftype = f.get("type", "")
        if ftype == "env_correlation":
            findings.append({
                "title": f"Environmental predictor: {f.get('predictor', '?')}",
                "description": (
                    f"{f.get('predictor')} shows {f.get('direction', '?')} correlation "
                    f"with target flux (Spearman r={f.get('spearman_r', 0):.3f}, "
                    f"p={f.get('p_value', 1):.2e}, n={f.get('n', 0)})"
                ),
                "statistical_support": json.dumps({
                    "spearman_r": f.get("spearman_r"),
                    "p_value": f.get("p_value"),
                    "n": f.get("n"),
                    "method": "spearman_rank_correlation",
                }),
            })
        elif ftype == "phylum_correlation":
            findings.append({
                "title": f"Phylum predictor: {f.get('predictor', '?')}",
                "description": (
                    f"{f.get('predictor')} abundance shows {f.get('direction', '?')} "
                    f"correlation with target flux "
                    f"(Spearman r={f.get('spearman_r', 0):.3f}, "
                    f"p={f.get('p_value', 1):.2e})"
                ),
                "statistical_support": json.dumps({
                    "spearman_r": f.get("spearman_r"),
                    "p_value": f.get("p_value"),
                    "n": f.get("n"),
                    "method": "spearman_rank_correlation",
                }),
            })
    return findings


def _extract_phylum_importance(results_dir: Path) -> list[dict]:
    """Convert phylum_importance.json → finding rows."""
    data = _load_json(results_dir / "phylum_importance.json")
    if not data or not isinstance(data, list):
        return []

    findings = []
    for entry in data[:5]:  # top 5 phyla
        findings.append({
            "title": f"Phylum driver: {entry.get('phylum', '?')}",
            "description": (
                f"{entry.get('phylum')} is a key driver of target flux variability "
                f"(importance={entry.get('importance_score', 0):.3f}, "
                f"mean_abundance_top_q={entry.get('mean_abundance_top_q', 0):.3f})"
            ),
            "statistical_support": json.dumps({
                "importance_score": entry.get("importance_score"),
                "mean_abundance_top_q": entry.get("mean_abundance_top_q"),
                "method": "variance_importance",
            }),
        })
    return findings


def _extract_spatial_findings(results_dir: Path) -> list[dict]:
    """Convert spatial_clusters.json → finding rows."""
    data = _load_json(results_dir / "spatial_clusters.json")
    if not data or not isinstance(data, list):
        return []

    findings = []
    for cluster in data[:5]:
        cluster_id = cluster.get("cluster_id", "?")
        n = cluster.get("n_communities", 0)
        findings.append({
            "title": f"Spatial cluster {cluster_id}",
            "description": (
                f"Cluster {cluster_id}: {n} communities, "
                f"centroid=({cluster.get('centroid_lat', 0):.2f}, "
                f"{cluster.get('centroid_lon', 0):.2f}), "
                f"mean_flux={cluster.get('mean_flux', 0):.4f}"
            ),
            "statistical_support": json.dumps({
                "n_communities": n,
                "centroid": [cluster.get("centroid_lat"), cluster.get("centroid_lon")],
                "method": "kmeans_spatial_clustering",
            }),
        })
    return findings


def _extract_climate_findings(results_dir: Path) -> list[dict]:
    """Convert climate_resilience.csv → finding rows."""
    rows = _load_csv(results_dir / "climate_resilience.csv")
    if not rows:
        return []

    findings = []
    # Top 3 climate-resilient communities
    for row in rows[:3]:
        findings.append({
            "title": f"Climate-resilient community {row.get('community_id', '?')}",
            "description": (
                f"Community {row.get('community_id')} shows high climate robustness "
                f"({row.get('climate_robustness', '?')}), "
                f"baseline_flux={row.get('baseline_flux', '?')}, "
                f"RCP8.5_flux={row.get('rcp85_flux', '?')}"
            ),
            "statistical_support": json.dumps({
                "climate_robustness": row.get("climate_robustness"),
                "max_sensitivity": row.get("max_sensitivity"),
                "n_scenarios": row.get("n_scenarios"),
                "method": "climate_scenario_projection",
            }),
        })
    return findings


def _extract_rank_findings(results_dir: Path) -> list[dict]:
    """Convert ranked_candidates.csv → finding rows for top 5."""
    rows = _load_csv(results_dir / "ranked_candidates.csv")
    if not rows:
        return []

    findings = []
    for row in rows[:5]:
        findings.append({
            "title": f"Top candidate: community {row.get('community_id', '?')}",
            "description": (
                f"Rank {row.get('rank', '?')}: composite_score={row.get('composite_score', '?')}, "
                f"t1_flux={row.get('t1_target_flux', '?')}, "
                f"stability={row.get('t2_stability_score', '?')}, "
                f"site={row.get('site_id', '?')}, land_use={row.get('land_use', '?')}"
            ),
            "statistical_support": json.dumps({
                "composite_score": row.get("composite_score"),
                "rank": row.get("rank"),
                "method": "multi_criteria_ranking",
            }),
        })
    return findings


# ---------------------------------------------------------------------------
# DB summary finding
# ---------------------------------------------------------------------------

def _extract_summary_finding(db_path: str) -> dict:
    """Generate a pipeline summary finding."""
    conn = _db_connect(db_path)
    n_total = conn.execute("SELECT COUNT(*) FROM communities").fetchone()[0]
    n_t0 = conn.execute("SELECT COUNT(*) FROM runs WHERE t0_pass = 1").fetchone()[0]
    n_t1 = conn.execute("SELECT COUNT(*) FROM runs WHERE t1_pass = 1").fetchone()[0]
    n_t2 = conn.execute("SELECT COUNT(*) FROM runs WHERE t2_pass = 1").fetchone()[0]
    top_row = conn.execute(
        "SELECT t1_target_flux, community_id FROM runs WHERE t1_pass = 1 ORDER BY t1_target_flux DESC LIMIT 1"
    ).fetchone()
    n_interventions = conn.execute("SELECT COUNT(*) FROM interventions").fetchone()[0]
    conn.close()

    return {
        "title": "Pipeline run summary",
        "description": (
            f"Communities screened: {n_total}. "
            f"T0 passed: {n_t0}. T1 metabolic models: {n_t1}. T2 dynamics: {n_t2}. "
            f"Interventions screened: {n_interventions}. "
            f"Top T1 flux: {top_row[0] or 0:.4g} (community {top_row[1] or '?'})"
        ),
        "statistical_support": json.dumps({
            "n_total": n_total,
            "n_t0": n_t0,
            "n_t1": n_t1,
            "n_t2": n_t2,
            "n_interventions": n_interventions,
            "top_flux": top_row[0] if top_row else None,
            "top_community": top_row[1] if top_row else None,
        }),
    }


# ---------------------------------------------------------------------------
# Main: populate findings table + regenerate FINDINGS.md
# ---------------------------------------------------------------------------

def _insert_findings(conn: sqlite3.Connection, findings: list[dict]) -> int:
    """Insert findings into DB. Returns count inserted."""
    n = 0
    for f in findings:
        try:
            conn.execute(
                """INSERT INTO findings (title, description, statistical_support)
                   VALUES (?, ?, ?)""",
                (f.get("title", ""), f.get("description", ""),
                 f.get("statistical_support", "{}")),
            )
            n += 1
        except Exception as exc:
            logger.debug("Finding insert failed: %s", exc)
    return n


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main(
    ctx:         typer.Context,
    db_path:     Path          = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    results_dir: Path          = typer.Option(Path("/opt/pipeline/results"), "--results-dir"),
    config_path: Path          = typer.Option(Path("/opt/pipeline/config.example.yaml"), "--config"),
    output_md:   Path          = typer.Option(Path("/opt/pipeline/FINDINGS.md"), "--output"),
    log_path:    Optional[Path] = typer.Option(
        Path("/var/log/pipeline/generate_findings.log"), "--log"
    ),
):
    """Generate findings from analysis outputs and populate DB."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=handlers,
        force=True,
    )

    t0 = time.time()
    logger.info("=== Findings generation starting ===")

    # Collect all findings
    all_findings: list[dict] = []

    summary = _extract_summary_finding(str(db_path))
    all_findings.append(summary)
    logger.info("Summary finding: %s", summary["description"][:100])

    corr = _extract_correlation_findings(results_dir)
    all_findings.extend(corr)
    logger.info("Correlation findings: %d", len(corr))

    phylum = _extract_phylum_importance(results_dir)
    all_findings.extend(phylum)
    logger.info("Phylum importance findings: %d", len(phylum))

    spatial = _extract_spatial_findings(results_dir)
    all_findings.extend(spatial)
    logger.info("Spatial cluster findings: %d", len(spatial))

    climate = _extract_climate_findings(results_dir)
    all_findings.extend(climate)
    logger.info("Climate resilience findings: %d", len(climate))

    rank = _extract_rank_findings(results_dir)
    all_findings.extend(rank)
    logger.info("Top-candidate findings: %d", len(rank))

    # Write to DB
    conn = _db_connect(str(db_path), timeout=60)
    conn.execute("PRAGMA synchronous=OFF")  # write path; restored before commit

    # Clear old findings and repopulate
    existing = conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0]
    if existing > 0:
        conn.execute("DELETE FROM findings")
        logger.info("Cleared %d old findings", existing)

    n = _insert_findings(conn, all_findings)
    conn.execute("PRAGMA synchronous=NORMAL")  # restore safe default
    conn.commit()
    conn.close()
    logger.info("Inserted %d findings into DB", n)

    # Generate FINDINGS.md using findings_generator if config exists
    try:
        from findings_generator import _db_summary, _render_findings_md
        from db_utils import SoilDB

        with SoilDB(str(db_path)) as db:
            db_sum = _db_summary(db)
        enriched_taxa: list = []
        taxa_csv = results_dir / "taxa_enrichment.csv"
        if taxa_csv.exists():
            with open(taxa_csv) as fh:
                enriched_taxa = list(csv.DictReader(fh))
            for row in enriched_taxa:
                row["p_adj"] = float(row.get("p_adj", 1))
                fc = row.get("fold_change", 0)
                row["fold_change"] = float(fc) if fc not in ("inf", "Infinity") else float("inf")
                row["significant"] = str(row.get("significant", "False")).lower() == "true"

        md = _render_findings_md(config_path, db_sum, corr, enriched_taxa, results_dir)
        output_md.write_text(md)
        logger.info("FINDINGS.md written → %s", output_md)
    except Exception as exc:
        logger.warning("FINDINGS.md generation failed: %s — DB findings still written", exc)

    logger.info("=== Findings generation complete in %.1f s (%d findings) ===",
                time.time() - t0, n)


if __name__ == "__main__":
    app()
