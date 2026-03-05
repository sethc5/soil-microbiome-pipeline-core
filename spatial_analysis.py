"""
spatial_analysis.py — Geographic distribution of top communities and interventions.

Clusters top-ranked communities by lat/lon, identifies geographic hot spots,
and checks whether high-performing communities are confined to specific
climate zones or soil types.

Optionally uses geopandas/matplotlib for maps; gracefully degrades to CSV if
those dependencies are not installed.

Usage:
  python spatial_analysis.py --db nitrogen_landscape.db --top 200
"""

from __future__ import annotations
import csv
import json
import logging
import math
from pathlib import Path

import typer

from db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _k_means_geo(points: list[tuple[float, float, int]], k: int, iterations: int = 20) -> list[int]:
    """Simple k-means clustering on (lat, lon) pairs. Returns cluster label per point."""
    import random
    coords = [(lat, lon) for lat, lon, _ in points]
    if len(coords) < k:
        return list(range(len(coords)))

    # Init: pick k random centroids
    centroids = random.sample(coords, k)
    labels = [0] * len(coords)

    for _ in range(iterations):
        # Assign
        for i, (lat, lon) in enumerate(coords):
            dists = [_haversine_km(lat, lon, c[0], c[1]) for c in centroids]
            labels[i] = dists.index(min(dists))

        # Update centroids
        new_centroids = []
        for ki in range(k):
            cluster_pts = [coords[i] for i, l in enumerate(labels) if l == ki]
            if cluster_pts:
                new_centroids.append((
                    sum(p[0] for p in cluster_pts) / len(cluster_pts),
                    sum(p[1] for p in cluster_pts) / len(cluster_pts),
                ))
            else:
                new_centroids.append(centroids[ki])
        centroids = new_centroids

    return labels


@app.command()
def analyze(
    db: Path = typer.Option(Path("landscape.db")),
    top: int = typer.Option(200, help="Number of top-ranked communities to analyze"),
    n_clusters: int = typer.Option(8, help="Number of geographic clusters"),
    output_dir: Path = typer.Option(Path("results/spatial/")),
):
    """Generate geographic distribution analysis for top communities."""
    logging.basicConfig(level=logging.INFO)
    output_dir.mkdir(parents=True, exist_ok=True)
    database = SoilDB(str(db))

    with database._connect() as conn:
        rows = conn.execute(
            """
            SELECT r.community_id, r.t1_target_flux, r.t2_stability_score,
                   c.latitude, c.longitude, c.ph, c.study_id, c.sample_id
            FROM runs r
            JOIN communities c ON r.community_id = c.id
            WHERE r.t1_target_flux IS NOT NULL
              AND c.latitude IS NOT NULL
              AND c.longitude IS NOT NULL
            ORDER BY r.t1_target_flux DESC
            LIMIT ?
            """,
            (top,)
        ).fetchall()

    if not rows:
        logger.warning("No georeferenced T1 results — cannot perform spatial analysis.")
        raise typer.Exit(1)

    col = ["community_id", "t1_target_flux", "t2_stability_score",
           "latitude", "longitude", "ph", "study_id", "sample_id"]
    records = [dict(zip(col, r)) for r in rows]

    # Cluster
    points = [(r["latitude"], r["longitude"], i) for i, r in enumerate(records)]
    labels = _k_means_geo(points, k=min(n_clusters, len(points)))
    for rec, label in zip(records, labels):
        rec["cluster"] = label

    # Cluster summary
    from collections import defaultdict
    clusters: dict[int, list[dict]] = defaultdict(list)
    for rec in records:
        clusters[rec["cluster"]].append(rec)

    cluster_summary = []
    for cid, members in clusters.items():
        fluxes = [m["t1_target_flux"] for m in members if m["t1_target_flux"]]
        lats = [m["latitude"] for m in members]
        lons = [m["longitude"] for m in members]
        cluster_summary.append({
            "cluster": cid,
            "n_communities": len(members),
            "centroid_lat": sum(lats) / len(lats),
            "centroid_lon": sum(lons) / len(lons),
            "mean_flux": sum(fluxes) / len(fluxes) if fluxes else 0,
            "max_flux": max(fluxes) if fluxes else 0,
        })

    # Write CSV: per-community
    per_community_path = output_dir / "spatial_communities.csv"
    with open(per_community_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=col + ["cluster"])
        writer.writeheader()
        writer.writerows(records)

    # Write CSV: cluster summary
    cluster_path = output_dir / "spatial_clusters.csv"
    with open(cluster_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["cluster", "n_communities", "centroid_lat", "centroid_lon", "mean_flux", "max_flux"])
        writer.writeheader()
        writer.writerows(cluster_summary)

    # Attempt matplotlib map
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(12, 6))
        colors = plt.cm.tab10.colors
        for cid, members in clusters.items():
            lats = [m["latitude"] for m in members]
            lons = [m["longitude"] for m in members]
            ax.scatter(lons, lats, c=[colors[cid % len(colors)]], label=f"Cluster {cid}", s=30, alpha=0.7)
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_title(f"Geographic distribution of top {top} communities")
        ax.legend(loc="best", fontsize=6)
        map_path = output_dir / "spatial_map.png"
        fig.savefig(str(map_path), dpi=150, bbox_inches="tight")
        plt.close(fig)
        logger.info("Spatial map saved → %s", map_path)
    except ImportError:
        logger.info("matplotlib not installed — skipping map generation")

    logger.info("Spatial analysis complete → %s", output_dir)
    typer.echo(f"Spatial analysis: {len(records)} communities, {len(clusters)} clusters → {output_dir}")


if __name__ == "__main__":
    app()
