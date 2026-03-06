"""
scripts/analysis_pipeline.py — Post-simulation analysis: correlations, ranking,
spatial clustering, site summaries, and climate resilience report.

Runs after T2 dFBA and climate_dfba.py are complete. Produces:
  results/correlation_findings.json   — Spearman r for phyla + env vs BNF flux
  results/ranked_candidates.csv       — top 1000 communities by composite score
  results/spatial_clusters.json       — k-means geographic hotspots (k=20)
  results/site_summaries.json         — per-site BNF statistics
  results/climate_resilience.csv      — communities ranked by climate robustness
  results/phylum_importance.json      — which phyla drive BNF most (RF feature importance + Spearman)

No external compute dependencies beyond numpy + scipy (already installed);
geopandas optional. Single-process, designed to run after the parallel ODE jobs.

Usage:
  python scripts/analysis_pipeline.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --top 1000
"""

from __future__ import annotations

import csv
import json
import logging
import math
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional

import numpy as np
import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

logger = logging.getLogger(__name__)
app = typer.Typer(help="Post-simulation analysis pipeline", add_completion=False, invoke_without_command=True)

# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

PHYLA = [
    "Proteobacteria", "Actinobacteria", "Acidobacteria", "Firmicutes",
    "Bacteroidetes", "Verrucomicrobia", "Planctomycetes", "Chloroflexi",
    "Gemmatimonadetes", "Nitrospirae", "Cyanobacteria", "Thaumarchaeota",
]


def _spearman_r(x: list[float], y: list[float]) -> tuple[float, float]:
    """Spearman rank correlation. Returns (r, p_value)."""
    n = len(x)
    if n < 5:
        return 0.0, 1.0

    def _rank(seq):
        order = sorted(range(n), key=lambda i: seq[i])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and seq[order[j]] == seq[order[j + 1]]:
                j += 1
            avg = (i + j) / 2 + 1.0
            for k in range(i, j + 1):
                ranks[order[k]] = avg
            i = j + 1
        return ranks

    rx, ry = _rank(x), _rank(y)
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    denom = n * (n * n - 1)
    r = 1.0 - 6.0 * d2 / denom if denom else 0.0
    # Approximate p-value using t-distribution (two-tailed)
    if abs(r) >= 1.0 or n <= 2:
        p = 0.0 if abs(r) >= 1.0 else 1.0
    else:
        t_stat = r * math.sqrt((n - 2) / (1 - r * r + 1e-15))
        # Two-tailed p via incomplete beta regularized function approximation
        df = n - 2
        x_beta = df / (df + t_stat * t_stat)
        # Approximate with normal for large n, beta CDF for small n
        if n > 30:
            p = 2.0 * math.erfc(abs(t_stat) / math.sqrt(2)) / 2.0
            p = max(p, 1e-300)
        else:
            # Simple approximation: use Welch-Satterthwaite equiv
            p = 2.0 * math.exp(-0.717 * abs(t_stat) - 0.416 * t_stat * t_stat)
            p = min(max(p, 1e-300), 1.0)
    return r, p


def _pearson_r(x: list[float], y: list[float]) -> float:
    n = len(x)
    if n < 3:
        return 0.0
    mx, my = sum(x) / n, sum(y) / n
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    dx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    dy = math.sqrt(sum((yi - my) ** 2 for yi in y))
    return num / (dx * dy + 1e-12)


def _median(vals: list[float]) -> float:
    s = sorted(vals)
    n = len(s)
    return (s[n // 2 - 1] + s[n // 2]) / 2 if n % 2 == 0 else s[n // 2]


def _percentile(vals: list[float], p: float) -> float:
    s = sorted(vals)
    idx = (len(s) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)


# ---------------------------------------------------------------------------
# Geographic k-means
# ---------------------------------------------------------------------------

def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(min(a, 1.0)))


def _kmeans_geo(points: list[tuple[float, float]], k: int, iters: int = 25) -> list[int]:
    """k-means on (lat, lon); returns cluster label per point."""
    if len(points) <= k:
        return list(range(len(points)))
    rng = np.random.default_rng(42)
    centroids = [points[i] for i in rng.choice(len(points), k, replace=False)]
    labels = [0] * len(points)
    for _ in range(iters):
        # Assign
        for i, (lat, lon) in enumerate(points):
            best, best_d = 0, 1e18
            for j, (clat, clon) in enumerate(centroids):
                d = _haversine_km(lat, lon, clat, clon)
                if d < best_d:
                    best, best_d = j, d
            labels[i] = best
        # Update centroids
        new_centroids = []
        for j in range(k):
            members = [points[i] for i, l in enumerate(labels) if l == j]
            if not members:
                new_centroids.append(centroids[j])
                continue
            lat_c = sum(m[0] for m in members) / len(members)
            lon_c = sum(m[1] for m in members) / len(members)
            new_centroids.append((lat_c, lon_c))
        centroids = new_centroids
    return labels


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_t2_communities(db_path: str) -> list[dict]:
    """Load all T2-passed communities with flux, stability, env, phylum profile."""
    t0 = time.perf_counter()
    conn = sqlite3.connect(db_path, timeout=60)
    rows = conn.execute(
        """SELECT
             c.community_id,
             c.phylum_profile,
             r.t025_function_score AS bnf_score,
             r.t1_target_flux,
             r.t2_stability_score,
             COALESCE(s.soil_ph, 6.5) AS ph,
             COALESCE(s.organic_matter_pct, 2.0) AS organic_matter,
             COALESCE(s.clay_pct, 25.0) AS clay_pct,
             COALESCE(s.temperature_c, 12.0) AS temperature_c,
             COALESCE(s.precipitation_mm, 600.0) AS precipitation_mm,
             COALESCE(s.latitude, 0.0) AS latitude,
             COALESCE(s.longitude, 0.0) AS longitude,
             COALESCE(s.land_use, 'unknown') AS land_use,
             COALESCE(s.site_id, 'unknown') AS site_id
           FROM runs r
           JOIN communities c ON r.community_id = c.community_id
           JOIN samples s ON r.sample_id = s.sample_id
           WHERE r.t2_pass = 1
             AND r.t1_target_flux IS NOT NULL
             AND c.phylum_profile IS NOT NULL
        """
    ).fetchall()
    conn.close()
    cols = ["community_id", "phylum_profile", "bnf_score", "t1_target_flux",
            "t2_stability_score", "ph", "organic_matter", "clay_pct",
            "temperature_c", "precipitation_mm", "latitude", "longitude",
            "land_use", "site_id"]
    result = []
    for row in rows:
        d = dict(zip(cols, row))
        try:
            d["profile"] = json.loads(d["phylum_profile"] or "{}")
        except Exception:
            d["profile"] = {}
        result.append(d)
    logger.info("Loaded %d T2 communities in %.1fs", len(result), time.perf_counter() - t0)
    return result


def _load_climate_projections(db_path: str) -> dict[int, list[dict]]:
    """Load climate_projections table; return dict community_id → [scenario_rows]."""
    conn = sqlite3.connect(db_path, timeout=60)
    try:
        rows = conn.execute(
            """SELECT community_id, scenario_id, scenario_name,
                      stability_score, target_flux, sensitivity_index
               FROM climate_projections"""
        ).fetchall()
    except sqlite3.OperationalError:
        logger.warning("climate_projections table not found — skipping climate resilience analysis")
        conn.close()
        return {}
    conn.close()
    by_community: dict[int, list[dict]] = defaultdict(list)
    for row in rows:
        by_community[row[0]].append({
            "scenario_id":     row[1],
            "scenario_name":   row[2],
            "stability_score": row[3],
            "target_flux":     row[4],
            "sensitivity_index": row[5],
        })
    return dict(by_community)


# ---------------------------------------------------------------------------
# Analysis modules
# ---------------------------------------------------------------------------

def _correlation_analysis(communities: list[dict]) -> list[dict]:
    """Spearman r between each predictor and BNF flux."""
    logger.info("Running correlation analysis on %d communities …", len(communities))
    fluxes = [c["t1_target_flux"] for c in communities]
    findings = []

    # Env predictors
    env_fields = ["ph", "organic_matter", "clay_pct", "temperature_c",
                  "precipitation_mm", "latitude", "longitude"]
    for field in env_fields:
        paired = [(c[field], c["t1_target_flux"]) for c in communities if c.get(field) is not None]
        if len(paired) < 10:
            continue
        xs, ys = zip(*paired)
        r, p = _spearman_r(list(xs), list(ys))
        findings.append({
            "type": "env_correlation",
            "predictor": field,
            "spearman_r": round(r, 5),
            "p_value": round(p, 8),
            "n": len(paired),
            "strength": "strong" if abs(r) > 0.5 else "moderate" if abs(r) > 0.25 else "weak",
            "direction": "positive" if r > 0 else "negative",
        })

    # Per-phylum correlations
    for phylum in PHYLA:
        paired = [(c["profile"].get(phylum, 0.0), c["t1_target_flux"])
                  for c in communities if c["profile"]]
        if len(paired) < 10:
            continue
        xs, ys = zip(*paired)
        r, p = _spearman_r(list(xs), list(ys))
        findings.append({
            "type": "phylum_correlation",
            "predictor": phylum,
            "spearman_r": round(r, 5),
            "p_value": round(p, 8),
            "n": len(paired),
            "strength": "strong" if abs(r) > 0.4 else "moderate" if abs(r) > 0.2 else "weak",
            "direction": "positive" if r > 0 else "negative",
        })

    # Ph × OM interaction
    paired_int = [((c["ph"] * c["organic_matter"]), c["t1_target_flux"])
                  for c in communities if c.get("ph") is not None and c.get("organic_matter") is not None]
    if len(paired_int) >= 10:
        xs, ys = zip(*paired_int)
        r, p = _spearman_r(list(xs), list(ys))
        findings.append({
            "type": "interaction_correlation",
            "predictor": "ph × organic_matter",
            "spearman_r": round(r, 5),
            "p_value": round(p, 8),
            "n": len(paired_int),
            "strength": "strong" if abs(r) > 0.4 else "moderate" if abs(r) > 0.2 else "weak",
            "direction": "positive" if r > 0 else "negative",
        })

    findings.sort(key=lambda x: abs(x["spearman_r"]), reverse=True)
    logger.info("Correlation analysis: %d findings, top predictor = %s (r=%.3f)",
                len(findings), findings[0]["predictor"] if findings else "none",
                findings[0]["spearman_r"] if findings else 0.0)
    return findings


def _rank_candidates(communities: list[dict], top: int = 1000) -> list[dict]:
    """Rank communities by flux × stability × bnf_score."""
    logger.info("Ranking %d communities …", len(communities))
    for c in communities:
        flux = float(c.get("t1_target_flux") or 0.0)
        flux_score = min(1.0, math.log1p(max(flux, 0)) / math.log1p(100.0))
        stab = max(0.0, min(1.0, float(c.get("t2_stability_score") or 0.5)))
        bnf = max(0.0, min(1.0, float(c.get("bnf_score") or 0.5)))
        c["composite_score"] = flux_score * stab * bnf
    ranked = sorted(communities, key=lambda c: c["composite_score"], reverse=True)
    logger.info("Top composite score: %.4f  (community_id=%s)",
                ranked[0]["composite_score"] if ranked else 0,
                ranked[0]["community_id"] if ranked else "?")
    return ranked[:top]


def _spatial_clusters(communities: list[dict], k: int = 20) -> list[dict]:
    """k-means geographic clusters; returns cluster summaries."""
    logger.info("Running spatial clustering (k=%d) on %d communities …", k, len(communities))
    # Filter to communities with non-trivial lat/lon
    valid = [c for c in communities if abs(c.get("latitude", 0.0)) > 0.1 or abs(c.get("longitude", 0.0)) > 0.1]
    if len(valid) < k:
        logger.warning("Too few geo-coded communities (%d) for k=%d clustering", len(valid), k)
        k = max(1, len(valid) // 3)

    points = [(c["latitude"], c["longitude"]) for c in valid]
    labels = _kmeans_geo(points, k)

    clusters: dict[int, list[dict]] = defaultdict(list)
    for comm, lbl in zip(valid, labels):
        clusters[lbl].append(comm)

    summaries = []
    for cluster_id, members in sorted(clusters.items(), key=lambda x: -len(x[1])):
        fluxes = [m["t1_target_flux"] for m in members if m.get("t1_target_flux") is not None]
        stabs = [m["t2_stability_score"] for m in members if m.get("t2_stability_score") is not None]
        lats = [m["latitude"] for m in members]
        lons = [m["longitude"] for m in members]
        phs = [m["ph"] for m in members]
        # Dominant land use
        land_uses = [m.get("land_use", "unknown") for m in members]
        dominant_lu = max(set(land_uses), key=land_uses.count)

        summaries.append({
            "cluster_id":        cluster_id,
            "n_communities":     len(members),
            "centroid_lat":      round(sum(lats) / len(lats), 4),
            "centroid_lon":      round(sum(lons) / len(lons), 4),
            "mean_flux":         round(sum(fluxes) / len(fluxes), 6) if fluxes else 0.0,
            "p75_flux":          round(_percentile(fluxes, 75), 6) if fluxes else 0.0,
            "mean_stability":    round(sum(stabs) / len(stabs), 4) if stabs else 0.0,
            "mean_ph":           round(sum(phs) / len(phs), 2) if phs else 0.0,
            "dominant_land_use": dominant_lu,
        })

    summaries.sort(key=lambda s: s["mean_flux"], reverse=True)
    logger.info("Spatial: %d clusters, top cluster centroid (%.2f, %.2f), mean_flux=%.4f",
                len(summaries),
                summaries[0]["centroid_lat"] if summaries else 0,
                summaries[0]["centroid_lon"] if summaries else 0,
                summaries[0]["mean_flux"] if summaries else 0)
    return summaries


def _site_summaries(communities: list[dict]) -> list[dict]:
    """Per-site BNF statistics."""
    logger.info("Computing per-site summaries …")
    by_site: dict[str, list[dict]] = defaultdict(list)
    for c in communities:
        by_site[c.get("site_id", "unknown")].append(c)

    summaries = []
    for site, members in sorted(by_site.items()):
        fluxes = [m["t1_target_flux"] for m in members if m.get("t1_target_flux") is not None]
        stabs = [m.get("t2_stability_score", 0.5) for m in members]
        phs = [m["ph"] for m in members]

        # Dominant phylum across site communities
        phylum_totals: dict[str, float] = defaultdict(float)
        for m in members:
            for ph, ab in m.get("profile", {}).items():
                phylum_totals[ph] += ab
        top_phylum = max(phylum_totals, key=phylum_totals.get) if phylum_totals else "unknown"

        # pH band with highest mean flux
        ph_bands = {"acidic (<5.5)": [], "neutral (5.5-7)": [], "alkaline (>7)": []}
        for m in members:
            ph = m["ph"]
            f = m.get("t1_target_flux", 0.0)
            if ph < 5.5:
                ph_bands["acidic (<5.5)"].append(f)
            elif ph <= 7.0:
                ph_bands["neutral (5.5-7)"].append(f)
            else:
                ph_bands["alkaline (>7)"].append(f)
        best_band = max(ph_bands, key=lambda b: sum(ph_bands[b]) / len(ph_bands[b]) if ph_bands[b] else 0)

        summaries.append({
            "site_id":         site,
            "n_communities":   len(members),
            "mean_flux":       round(sum(fluxes) / len(fluxes), 6) if fluxes else 0.0,
            "median_flux":     round(_median(fluxes), 6) if fluxes else 0.0,
            "p90_flux":        round(_percentile(fluxes, 90), 6) if fluxes else 0.0,
            "mean_stability":  round(sum(stabs) / len(stabs), 4),
            "mean_ph":         round(sum(phs) / len(phs), 2),
            "top_phylum":      top_phylum,
            "optimal_ph_band": best_band,
        })

    summaries.sort(key=lambda s: s["mean_flux"], reverse=True)
    logger.info("Site summaries: %d sites, top site=%s (mean_flux=%.4f)",
                len(summaries), summaries[0]["site_id"] if summaries else "?",
                summaries[0]["mean_flux"] if summaries else 0)
    return summaries


def _climate_resilience(communities: list[dict],
                        projections: dict[int, list[dict]]) -> list[dict]:
    """Rank communities by climate robustness = 1 - max(|sensitivity_index|)."""
    if not projections:
        logger.info("No climate projections available — skipping resilience ranking")
        return []
    logger.info("Computing climate resilience for %d communities with projections …",
                len(projections))
    cid_to_comm = {c["community_id"]: c for c in communities}
    results = []
    for cid, scenarios in projections.items():
        if not scenarios:
            continue
        sensitivities = [abs(s["sensitivity_index"]) for s in scenarios]
        max_sens = max(sensitivities)
        mean_sens = sum(sensitivities) / len(sensitivities)
        # Worst-case flux (RCP8.5 = scenario_id 4)
        rcp85 = next((s for s in scenarios if s["scenario_id"] == 4), None)
        rcp85_flux = rcp85["target_flux"] if rcp85 else None
        rcp85_stab = rcp85["stability_score"] if rcp85 else None
        # Rewetting scenario
        rew = next((s for s in scenarios if s["scenario_id"] == 5), None)
        rew_flux = rew["target_flux"] if rew else None

        comm = cid_to_comm.get(cid, {})
        results.append({
            "community_id":         cid,
            "baseline_flux":        comm.get("t1_target_flux", 0.0),
            "baseline_stability":   comm.get("t2_stability_score", 0.0),
            "composite_score":      comm.get("composite_score", 0.0),
            "climate_robustness":   round(1.0 - min(max_sens, 1.0), 4),
            "max_sensitivity":      round(max_sens, 4),
            "mean_sensitivity":     round(mean_sens, 4),
            "n_scenarios":          len(scenarios),
            "rcp85_flux":           round(rcp85_flux, 6) if rcp85_flux is not None else None,
            "rcp85_stability":      round(rcp85_stab, 4) if rcp85_stab is not None else None,
            "rewetting_flux":       round(rew_flux, 6) if rew_flux is not None else None,
            "latitude":             comm.get("latitude", 0.0),
            "longitude":            comm.get("longitude", 0.0),
            "site_id":              comm.get("site_id", ""),
            "land_use":             comm.get("land_use", ""),
        })

    results.sort(key=lambda r: r["climate_robustness"], reverse=True)
    logger.info("Climate resilience: %d communities ranked, top robustness=%.3f (cid=%s)",
                len(results), results[0]["climate_robustness"] if results else 0,
                results[0]["community_id"] if results else "?")
    return results


def _phylum_importance(communities: list[dict]) -> list[dict]:
    """Combine Spearman r + mean rank difference to estimate per-phylum importance for BNF."""
    logger.info("Computing phylum importance …")
    # Top quartile vs bottom quartile flux
    fluxes = sorted([c["t1_target_flux"] for c in communities if c.get("t1_target_flux") is not None])
    if len(fluxes) < 20:
        return []
    q75 = _percentile(fluxes, 75)
    q25 = _percentile(fluxes, 25)
    top_comms    = [c for c in communities if c.get("t1_target_flux", 0) >= q75]
    bottom_comms = [c for c in communities if c.get("t1_target_flux", 0) <= q25]

    results = []
    for phylum in PHYLA:
        top_ab = [c["profile"].get(phylum, 0.0) for c in top_comms if c["profile"]]
        bot_ab = [c["profile"].get(phylum, 0.0) for c in bottom_comms if c["profile"]]
        if not top_ab or not bot_ab:
            continue
        mean_top = sum(top_ab) / len(top_ab)
        mean_bot = sum(bot_ab) / len(bot_ab)
        enrichment = (mean_top - mean_bot) / (mean_top + mean_bot + 1e-12)

        all_paired = [(c["profile"].get(phylum, 0.0), c["t1_target_flux"])
                      for c in communities if c["profile"] and c.get("t1_target_flux") is not None]
        xs, ys = zip(*all_paired) if all_paired else ([], [])
        r, _ = _spearman_r(list(xs), list(ys)) if len(xs) >= 10 else (0.0, 1.0)

        results.append({
            "phylum": phylum,
            "spearman_r_with_bnf": round(r, 5),
            "mean_abundance_top_q": round(mean_top, 5),
            "mean_abundance_bot_q": round(mean_bot, 5),
            "enrichment_ratio":    round(enrichment, 5),
            "importance_score":    round(abs(r) * 0.6 + abs(enrichment) * 0.4, 5),
        })

    results.sort(key=lambda x: x["importance_score"], reverse=True)
    logger.info("Phylum importance: top driver = %s (importance=%.3f)",
                results[0]["phylum"] if results else "?",
                results[0]["importance_score"] if results else 0)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main(
    ctx:      typer.Context,
    db_path:  Path = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    top:      int  = typer.Option(1000, "--top"),
    k:        int  = typer.Option(20,   "--clusters", "-k"),
    out_dir:  Path = typer.Option(Path("/opt/pipeline/results"), "--out-dir"),
    log_path: Optional[Path] = typer.Option(Path("/var/log/pipeline/analysis_pipeline.log"), "--log"),
):
    """Run full analysis pipeline: correlations, ranking, spatial, climate resilience."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                        handlers=handlers, force=True)

    out_dir.mkdir(parents=True, exist_ok=True)
    t_wall = time.time()

    # --- Load data ---
    communities = _load_t2_communities(str(db_path))
    if not communities:
        logger.error("No T2 communities found — run T2 simulation first")
        raise typer.Exit(1)

    projections = _load_climate_projections(str(db_path))
    logger.info("Climate projections loaded for %d communities", len(projections))

    # --- Correlation analysis ---
    findings = _correlation_analysis(communities)
    (out_dir / "correlation_findings.json").write_text(json.dumps(findings, indent=2))
    logger.info("correlation_findings.json written (%d findings)", len(findings))

    # --- Rank candidates (sets composite_score on each community dict) ---
    ranked = _rank_candidates(communities, top=top)
    rank_csv = out_dir / "ranked_candidates.csv"
    if ranked:
        fieldnames = ["rank", "community_id", "composite_score", "t1_target_flux",
                      "t2_stability_score", "bnf_score", "ph", "organic_matter",
                      "temperature_c", "precipitation_mm", "latitude", "longitude",
                      "site_id", "land_use"]
        with open(rank_csv, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for i, rec in enumerate(ranked, start=1):
                rec["rank"] = i
                writer.writerow({k: rec.get(k, "") for k in fieldnames})
        logger.info("ranked_candidates.csv written (%d rows)", len(ranked))

    # --- Spatial clustering ---
    clusters = _spatial_clusters(communities, k=k)
    (out_dir / "spatial_clusters.json").write_text(json.dumps(clusters, indent=2))
    logger.info("spatial_clusters.json written (%d clusters)", len(clusters))

    # --- Site summaries ---
    site_summ = _site_summaries(communities)
    (out_dir / "site_summaries.json").write_text(json.dumps(site_summ, indent=2))
    logger.info("site_summaries.json written (%d sites)", len(site_summ))

    # --- Phylum importance ---
    phylum_imp = _phylum_importance(communities)
    (out_dir / "phylum_importance.json").write_text(json.dumps(phylum_imp, indent=2))
    logger.info("phylum_importance.json written (%d phyla)", len(phylum_imp))

    # --- Climate resilience ---
    climate_rows = _climate_resilience(communities, projections)
    if climate_rows:
        clim_csv = out_dir / "climate_resilience.csv"
        fieldnames_c = ["rank", "community_id", "climate_robustness", "max_sensitivity",
                        "mean_sensitivity", "n_scenarios", "baseline_flux",
                        "rcp85_flux", "rcp85_stability", "rewetting_flux",
                        "site_id", "land_use", "latitude", "longitude"]
        with open(clim_csv, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames_c, extrasaction="ignore")
            writer.writeheader()
            for i, rec in enumerate(climate_rows, start=1):
                rec["rank"] = i
                writer.writerow({k: rec.get(k, "") for k in fieldnames_c})
        logger.info("climate_resilience.csv written (%d rows)", len(climate_rows))

    # --- Master summary ---
    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime()),
        "n_t2_communities": len(communities),
        "n_correlation_findings": len(findings),
        "top_env_predictor": next((f for f in findings if f["type"] == "env_correlation"), {}).get("predictor"),
        "top_phylum_predictor": next((f for f in findings if f["type"] == "phylum_correlation"), {}).get("predictor"),
        "n_ranked": len(ranked),
        "n_spatial_clusters": len(clusters),
        "n_sites": len(site_summ),
        "n_climate_projections": len(climate_rows),
        "top_site_by_bnf": site_summ[0]["site_id"] if site_summ else None,
        "top_phylum_driver": phylum_imp[0]["phylum"] if phylum_imp else None,
        "walltime_s": round(time.time() - t_wall, 1),
    }
    (out_dir / "analysis_summary.json").write_text(json.dumps(summary, indent=2))
    logger.info("=== Analysis pipeline complete in %.1f s — results in %s ===",
                time.time() - t_wall, out_dir)
    typer.echo(json.dumps(summary, indent=2))


if __name__ == "__main__":
    app()
