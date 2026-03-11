"""
scripts/make_spatial_map.py
Produce a publication-quality BNF spatial map:
  - Background kriging interpolation heatmap (CONUS)
  - Community sample points coloured by spatial cluster
  - Top-site labels
Output: results/spatial/bnf_spatial_map.png
"""

import csv
import pathlib
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as cm
import numpy as np

ROOT = pathlib.Path(__file__).parent.parent
RESULTS = ROOT / "results"

# ── Load kriging grid ────────────────────────────────────────────────────────
grid_path = RESULTS / "bnf_kriging_grid_conus.csv"
lats, lons, fluxes = [], [], []
with open(grid_path) as fh:
    for row in csv.DictReader(fh):
        lats.append(float(row["lat"]))
        lons.append(float(row["lon"]))
        fluxes.append(float(row["interp_bnf_flux_mmol_nh4_per_gdw_per_h"]))

# Build 2D grid
u_lats = sorted(set(lats))
u_lons = sorted(set(lons))
lat_idx = {v: i for i, v in enumerate(u_lats)}
lon_idx = {v: i for i, v in enumerate(u_lons)}
grid = np.full((len(u_lats), len(u_lons)), np.nan)
for lat, lon, flux in zip(lats, lons, fluxes):
    grid[lat_idx[lat], lon_idx[lon]] = flux

# ── Load community points ────────────────────────────────────────────────────
comm_path = RESULTS / "spatial" / "spatial_communities.csv"
comm_lats, comm_lons, comm_fluxes, comm_clusters = [], [], [], []
comm_sites = []
with open(comm_path) as fh:
    for row in csv.DictReader(fh):
        try:
            clat = float(row["latitude"])
            clon = float(row["longitude"])
        except (ValueError, KeyError):
            continue
        comm_lats.append(clat)
        comm_lons.append(clon)
        comm_fluxes.append(float(row["t1_target_flux"]))
        comm_clusters.append(int(row["cluster"]))
        comm_sites.append(row.get("study_id", ""))

# ── Load cluster summary ─────────────────────────────────────────────────────
clusters_path = RESULTS / "spatial" / "spatial_clusters.csv"
cluster_info = {}
with open(clusters_path) as fh:
    for row in csv.DictReader(fh):
        cid = int(row["cluster"])
        cluster_info[cid] = row

# ── Build figure ─────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 7))
fig.patch.set_facecolor("#0e1117")
ax.set_facecolor("#0e1117")

# Kriging heatmap
extent = [min(u_lons), max(u_lons), min(u_lats), max(u_lats)]
im = ax.imshow(
    grid,
    origin="lower",
    extent=extent,
    aspect="auto",
    cmap="YlOrRd",
    alpha=0.75,
    vmin=0,
    vmax=max(fluxes) * 0.85,
)
cbar = fig.colorbar(im, ax=ax, pad=0.01, fraction=0.025)
cbar.set_label("Kriging BNF flux (mmol NH₄/gDW/h)", color="white", fontsize=9)
cbar.ax.yaxis.set_tick_params(color="white")
plt.setp(plt.getp(cbar.ax.axes, "yticklabels"), color="white")

# Community scatter, coloured by cluster
unique_clusters = sorted(set(comm_clusters))
palette = plt.get_cmap("tab10")
cluster_colors = {c: palette(i % 10) for i, c in enumerate(unique_clusters)}

legend_handles = []
for cid in unique_clusters:
    mask = [i for i, cl in enumerate(comm_clusters) if cl == cid]
    cx = [comm_lons[i] for i in mask]
    cy = [comm_lats[i] for i in mask]
    cf = [comm_fluxes[i] for i in mask]
    n = cluster_info.get(cid, {}).get("n_communities", len(mask))
    mean_f = cluster_info.get(cid, {}).get("mean_flux", "?")
    try:
        mean_f = f"{float(mean_f):.0f}"
    except (ValueError, TypeError):
        mean_f = "?"
    sc = ax.scatter(
        cx, cy, c=[cluster_colors[cid]] * len(cx),
        s=28, alpha=0.9, edgecolors="black", linewidths=0.3, zorder=3,
        label=f"Cluster {cid}  n={n}  mean={mean_f}",
    )
    legend_handles.append(sc)

# Label top sites (highest flux per unique study_id)
seen_sites = set()
site_top: dict[str, tuple] = {}
for i, (lat, lon, flux, site) in enumerate(zip(comm_lats, comm_lons, comm_fluxes, comm_sites)):
    if not site:
        continue
    if site not in site_top or flux > site_top[site][2]:
        site_top[site] = (lat, lon, flux)

top_sites = sorted(site_top.items(), key=lambda x: -x[1][2])[:8]
for site, (lat, lon, flux) in top_sites:
    ax.annotate(
        f" {site}\n {flux:.0f}",
        xy=(lon, lat), xytext=(lon + 0.4, lat + 0.4),
        fontsize=6.5, color="white", zorder=5,
        arrowprops=dict(arrowstyle="-", color="white", lw=0.6),
    )

# Map boundaries
ax.set_xlim(-130, -60)
ax.set_ylim(23, 52)
ax.set_xlabel("Longitude", color="white", fontsize=9)
ax.set_ylabel("Latitude", color="white", fontsize=9)
ax.tick_params(colors="white", labelsize=7)
for spine in ax.spines.values():
    spine.set_edgecolor("#555")

ax.set_title(
    "Biological Nitrogen Fixation — Kriging Interpolation + Community Clusters (CONUS)",
    color="white", fontsize=11, pad=10,
)

legend = ax.legend(
    handles=legend_handles,
    loc="lower right",
    fontsize=7,
    framealpha=0.3,
    facecolor="#222",
    edgecolor="#555",
    labelcolor="white",
)

plt.tight_layout()
out_path = RESULTS / "spatial" / "bnf_spatial_map.png"
out_path.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out_path, dpi=180, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Saved → {out_path}")
