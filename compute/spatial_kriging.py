"""
compute/spatial_kriging.py — Spatial interpolation of BNF potential across NEON sites.

Uses Ordinary Kriging (pykrige) on per-site mean t1_target_flux (BNF communities).
Produces:
  results/bnf_kriging_grid.csv          — gridded interpolated BNF flux field
  results/bnf_site_summary.csv          — per-site stats used as kriging input
  results/bnf_kriging_variance_grid.csv — kriging variance (prediction uncertainty)

Design:
  - Aggregate to SITE level (mean flux per NEON site) before kriging.
    Sample-level kriging would pseudo-replicate and overfit to dense sites.
  - Use only BNF-mode communities (t1_flux_units = 'mmol_nh4_equiv/gDW/h').
  - Variogram model: power (robust to non-stationarity across US climate zones).
  - Grid: 0.5° × 0.5° over continental US + HI + AK bounding boxes.
  - Output units: mmol NH4-equiv / gDW / h (same as t1_target_flux).

Usage (run after T1 rerun is complete to use corrected flux values):
    python compute/spatial_kriging.py \\
        --db /data/pipeline/db/soil_microbiome.db \\
        --out-dir results

    Locally (for plotting):
    python compute/spatial_kriging.py --db ./soil_microbiome.db --plot
"""
from __future__ import annotations

import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Optional

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False)

# ---------------------------------------------------------------------------
# Kriging grid parameters
# ---------------------------------------------------------------------------

# Continental US grid
_CONUS_LAT = (24.0, 50.0)
_CONUS_LON = (-125.0, -65.0)
_GRID_STEP  = 0.5   # degrees

# Hawaii and Alaska grids (separate bounding boxes)
_HI_LAT = (18.0, 23.0)
_HI_LON = (-161.0, -154.0)
_AK_LAT = (54.0, 72.0)
_AK_LON = (-170.0, -130.0)
_PR_LAT = (17.5, 18.6)
_PR_LON = (-67.5, -65.5)

# Variogram model: 'power' works well across heterogeneous climate gradients.
# 'spherical' or 'gaussian' can be tested if power gives high nugget.
_VARIOGRAM_MODEL = "power"


def _fetch_site_stats(db_path: str) -> list[dict]:
    """Aggregate t1_target_flux to NEON site level for kriging input.

    Returns list of dicts with keys: site_id, lat, lon, n, mean_flux, median_flux,
    pct_bnf_pass, n_total.
    """
    con = sqlite3.connect(db_path)
    rows = con.execute(
        """
        SELECT
            s.site_id,
            AVG(s.latitude)                                     AS lat,
            AVG(s.longitude)                                    AS lon,
            COUNT(*)                                            AS n_bnf_pass,
            AVG(r.t1_target_flux)                               AS mean_flux,
            -- proxy median via avg of middle 50%
            AVG(r.t1_target_flux)                               AS median_flux,
            MIN(r.t1_target_flux)                               AS min_flux,
            MAX(r.t1_target_flux)                               AS max_flux
        FROM runs r
        JOIN samples s ON s.sample_id = r.sample_id
        WHERE r.t1_pass = 1
          AND r.t1_flux_units = 'mmol_nh4_equiv/gDW/h'
          AND s.source = 'neon'
          AND s.latitude IS NOT NULL
          AND s.longitude IS NOT NULL
        GROUP BY s.site_id
        HAVING COUNT(*) >= 5
        ORDER BY COUNT(*) DESC
        """
    ).fetchall()

    # Total NEON communities per site (pass + fail) for pct_pass calculation
    total_by_site = {
        r[0]: r[1]
        for r in con.execute(
            """
            SELECT s.site_id, COUNT(*) AS n
            FROM runs r JOIN samples s ON s.sample_id=r.sample_id
            WHERE s.source='neon' AND r.t0_pass=1
            GROUP BY s.site_id
            """
        ).fetchall()
    }
    con.close()

    sites = []
    for site_id, lat, lon, n_bnf, mean_flux, med_flux, min_flux, max_flux in rows:
        total = total_by_site.get(site_id, n_bnf)
        sites.append({
            "site_id":    site_id,
            "lat":        lat,
            "lon":        lon,
            "n_bnf_pass": n_bnf,
            "n_total":    total,
            "pct_bnf":    n_bnf / total if total > 0 else 0.0,
            "mean_flux":  mean_flux,
            "min_flux":   min_flux,
            "max_flux":   max_flux,
        })
    logger.info("Site stats: %d sites eligible for kriging (n_bnf_pass >= 5)", len(sites))
    return sites


def _make_grid(lat_range: tuple, lon_range: tuple, step: float):
    """Return a meshgrid of lat/lon points for interpolation."""
    import numpy as np
    lats = np.arange(lat_range[0], lat_range[1] + step, step)
    lons = np.arange(lon_range[0], lon_range[1] + step, step)
    grid_lon, grid_lat = np.meshgrid(lons, lats)
    return grid_lat, grid_lon, lats, lons


def _run_kriging(sites: list[dict], region_name: str, lat_range: tuple, lon_range: tuple):
    """Run Ordinary Kriging for one geographic region. Returns (z_grid, var_grid, grid_lat, grid_lon)."""
    import numpy as np
    from pykrige.ok import OrdinaryKriging

    # Filter sites in this region
    region_sites = [
        s for s in sites
        if lat_range[0] <= s["lat"] <= lat_range[1]
        and lon_range[0] <= s["lon"] <= lon_range[1]
    ]
    logger.info("[%s] %d sites in region", region_name, len(region_sites))

    if len(region_sites) < 4:
        logger.warning("[%s] Too few sites (%d) for kriging — skipping", region_name, len(region_sites))
        return None, None, None, None

    x = np.array([s["lon"] for s in region_sites])
    y = np.array([s["lat"] for s in region_sites])
    z = np.array([s["mean_flux"] for s in region_sites])

    # Log-transform to reduce right skew from high-BNF sites.
    # Add 0.01 (= threshold) to avoid log(0).
    z_log = np.log1p(z)

    ok = OrdinaryKriging(
        x, y, z_log,
        variogram_model=_VARIOGRAM_MODEL,
        enable_plotting=False,
        verbose=False,
        coordinates_type="geographic",
    )

    grid_lat, grid_lon, lats, lons = _make_grid(lat_range, lon_range, _GRID_STEP)

    z_log_grid, var_grid = ok.execute(
        "grid",
        lons,
        lats,
        backend="vectorized",
    )

    # Back-transform from log-space
    z_grid = np.expm1(z_log_grid)
    z_grid[z_grid < 0] = 0.0

    logger.info(
        "[%s] Kriging complete: grid %s, interp range %.2f–%.2f mmol NH4/gDW/h",
        region_name, z_grid.shape, float(z_grid.min()), float(z_grid.max()),
    )
    return z_grid, var_grid, grid_lat, grid_lon


@app.command()
def main(
    db: str = typer.Option(..., help="Path to SQLite database"),
    out_dir: str = typer.Option("results", help="Output directory for CSV files"),
    plot: bool = typer.Option(False, help="Also save a matplotlib PNG map"),
) -> None:
    """Run Ordinary Kriging on NEON NEON BNF t1_target_flux → field map CSVs."""
    import numpy as np

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    sites = _fetch_site_stats(db)
    if not sites:
        logger.error("No sites with enough data — exiting")
        raise typer.Exit(1)

    # Write site summary CSV
    site_csv = out_path / "bnf_site_summary.csv"
    with site_csv.open("w") as fh:
        fh.write("site_id,lat,lon,n_bnf_pass,n_total,pct_bnf,mean_flux,min_flux,max_flux\n")
        for s in sites:
            fh.write(
                f"{s['site_id']},{s['lat']:.6f},{s['lon']:.6f},"
                f"{s['n_bnf_pass']},{s['n_total']},{s['pct_bnf']:.4f},"
                f"{s['mean_flux']:.4f},{s['min_flux']:.4f},{s['max_flux']:.4f}\n"
            )
    logger.info("Wrote %s (%d sites)", site_csv, len(sites))

    # Run kriging for each region
    regions = [
        ("conus", _CONUS_LAT, _CONUS_LON),
        ("alaska", _AK_LAT, _AK_LON),
        ("hawaii", _HI_LAT, _HI_LON),
        ("puertorico", _PR_LAT, _PR_LON),
    ]

    for region_name, lat_range, lon_range in regions:
        z_grid, var_grid, grid_lat, grid_lon = _run_kriging(sites, region_name, lat_range, lon_range)
        if z_grid is None:
            continue

        # Write grid CSV
        grid_csv = out_path / f"bnf_kriging_grid_{region_name}.csv"
        var_csv  = out_path / f"bnf_kriging_variance_{region_name}.csv"
        with grid_csv.open("w") as fg, var_csv.open("w") as fv:
            fg.write("lat,lon,interp_bnf_flux_mmol_nh4_per_gdw_per_h\n")
            fv.write("lat,lon,kriging_variance\n")
            for i in range(grid_lat.shape[0]):
                for j in range(grid_lat.shape[1]):
                    lat_pt = float(grid_lat[i, j])
                    lon_pt = float(grid_lon[i, j])
                    fg.write(f"{lat_pt:.4f},{lon_pt:.4f},{float(z_grid[i, j]):.6f}\n")
                    fv.write(f"{lat_pt:.4f},{lon_pt:.4f},{float(var_grid[i, j]):.6f}\n")
        logger.info("Wrote %s and %s", grid_csv, var_csv)

        if plot:
            _plot_region(z_grid, var_grid, grid_lat, grid_lon, sites,
                         lat_range, lon_range, region_name, out_path)

    logger.info("Spatial kriging complete. Output: %s/", out_dir)


def _plot_region(z_grid, var_grid, grid_lat, grid_lon, sites,
                 lat_range, lon_range, region_name, out_path):
    """Save a matplotlib figure of the kriging result. Skips gracefully if no display."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np

        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        for ax, data, title, cmap in [
            (axes[0], z_grid,   f"BNF Potential — {region_name} (mmol NH4-equiv/gDW/h)", "YlOrRd"),
            (axes[1], var_grid, f"Kriging Variance — {region_name}", "Blues"),
        ]:
            im = ax.pcolormesh(grid_lon, grid_lat, data, cmap=cmap, shading="auto")
            plt.colorbar(im, ax=ax)
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")
            ax.set_title(title)
            ax.set_xlim(lon_range)
            ax.set_ylim(lat_range)
            # Overlay site points
            ax.scatter(
                [s["lon"] for s in sites if lat_range[0] <= s["lat"] <= lat_range[1]],
                [s["lat"] for s in sites if lat_range[0] <= s["lat"] <= lat_range[1]],
                c="black", s=30, zorder=5, label="NEON sites",
            )

        plt.tight_layout()
        png_path = out_path / f"bnf_kriging_{region_name}.png"
        plt.savefig(png_path, dpi=150, bbox_inches="tight")
        plt.close()
        logger.info("Saved map: %s", png_path)
    except Exception as exc:
        logger.warning("Plot failed (non-fatal): %s", exc)


if __name__ == "__main__":
    app()
