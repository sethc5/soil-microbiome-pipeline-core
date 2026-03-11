"""
taxa_enrichment.py — Identify taxa enriched in high-performing communities.

Compares taxonomic composition between top-ranked communities (by target
function score) vs. all others. Reports genera/families/phyla with
statistically significant enrichment.

Uses: Mann-Whitney U, FDR correction (statsmodels or manual BH), effect size.

Usage:
  python taxa_enrichment.py --db nitrogen_landscape.db --top-pct 10
"""

from __future__ import annotations
import csv
import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Literal

import typer

from core.db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _mann_whitney_u(group_a: list[float], group_b: list[float]) -> tuple[float, float]:
    """Compute Mann-Whitney U statistic and approximate p-value (normal approx, tie-corrected)."""
    import statistics
    na, nb = len(group_a), len(group_b)
    if na == 0 or nb == 0:
        return 0.0, 1.0

    combined = sorted([(v, 0) for v in group_a] + [(v, 1) for v in group_b])
    ranks: list[float] = []
    tie_term = 0  # sum of (t^3 - t) for each tie group
    i = 0
    while i < len(combined):
        j = i
        while j < len(combined) - 1 and combined[j][0] == combined[j + 1][0]:
            j += 1
        avg_rank = (i + j) / 2 + 1
        t = j - i + 1  # group size
        if t > 1:
            tie_term += t ** 3 - t
        for _ in range(t):
            ranks.append(avg_rank)
        i = j + 1

    rank_sum_a = sum(r for r, (_, g) in zip(ranks, combined) if g == 0)
    u = rank_sum_a - na * (na + 1) / 2
    n = na + nb
    mean_u = na * nb / 2
    # Tie-corrected standard deviation (Hollander & Wolfe 1999)
    numerator = na * nb * ((n ** 3 - n) - tie_term)
    if numerator <= 0 or n <= 1:
        return u, 1.0
    std_u = math.sqrt(numerator / (12 * n * (n - 1)))
    if std_u == 0:
        return u, 1.0
    z = (u - mean_u) / std_u
    # Approximate p-value via standard normal CDF (two-tailed)
    p = 2 * (1 - _norm_cdf(abs(z)))
    return u, p


def _norm_cdf(x: float) -> float:
    """Approximation of standard normal CDF using error function."""
    import math
    return (1.0 + math.erf(x / math.sqrt(2))) / 2


def _bh_correction(pvalues: list[float]) -> list[float]:
    """Benjamini-Hochberg FDR correction. Returns adjusted p-values."""
    n = len(pvalues)
    if n == 0:
        return []
    indexed = sorted(enumerate(pvalues), key=lambda x: x[1])
    adjusted = [1.0] * n
    prev = 1.0
    for rank_i, (orig_i, p) in enumerate(reversed(indexed)):
        adj = p * n / (n - rank_i)
        prev = min(prev, adj)
        adjusted[orig_i] = prev
    return adjusted


def _load_community_taxa(db: SoilDB) -> dict[int, dict[str, float]]:
    """Load taxon abundance data keyed by community_id.

    Preference order for taxon abundances:
    1. runs.t025_model — PICRUSt2/HUMAnN3 pathway abundances (dict JSON)
    2. communities.top_genera — genus-level relative abundances (list JSON)
    3. communities.phylum_profile — phylum-level relative abundances (dict JSON)
    """
    with db._connect() as conn:
        rows = conn.execute(
            """
            SELECT r.community_id, r.t025_model, r.t1_target_flux,
                   c.top_genera, c.phylum_profile
            FROM runs r
            JOIN communities c ON r.community_id = c.community_id
            WHERE r.t1_pass = 1
            """
        ).fetchall()

    result = {}
    for community_id, model_json, flux, genera_json, phylum_json in rows:
        taxa: dict[str, float] = {}
        # 1. t025_model (PICRUSt2 pathway dict)
        if model_json and len(model_json) > 2:
            try:
                data = json.loads(model_json)
                if isinstance(data, dict) and data:
                    taxa = {k: float(v) for k, v in data.items()}
            except Exception:
                pass
        # 2. top_genera (list of {name, rel_abundance})
        if not taxa and genera_json and len(genera_json) > 2:
            try:
                data = json.loads(genera_json)
                if isinstance(data, list):
                    taxa = {item["name"]: float(item["rel_abundance"])
                            for item in data if "name" in item and "rel_abundance" in item}
            except Exception:
                pass
        # 3. phylum_profile (dict)
        if not taxa and phylum_json and len(phylum_json) > 2:
            try:
                data = json.loads(phylum_json)
                if isinstance(data, dict) and data:
                    taxa = {k: float(v) for k, v in data.items()}
            except Exception:
                pass

        if taxa:
            result[community_id] = {"_flux": float(flux or 0), **taxa}
    return result


@app.command()
def enrich(
    db: Path = typer.Option(Path("landscape.db")),
    top_pct: float = typer.Option(10.0, help="Top percentile to treat as high-performing"),
    rank: str = typer.Option("genus", help="Taxonomic rank: genus, family, phylum"),
    output: Path = typer.Option(Path("results/taxa_enrichment.csv")),
):
    """Compute taxonomic enrichment in high-performing communities."""
    logging.basicConfig(level=logging.INFO)
    database = SoilDB(str(db))

    community_taxa = _load_community_taxa(database)
    if not community_taxa:
        logger.error("No taxa abundance data found in %s", db)
        raise typer.Exit(1)

    fluxes = [v["_flux"] for v in community_taxa.values()]
    fluxes_sorted = sorted(fluxes)
    threshold_idx = int(len(fluxes_sorted) * (1 - top_pct / 100))
    flux_threshold = fluxes_sorted[threshold_idx] if threshold_idx < len(fluxes_sorted) else float("inf")

    high_group = {cid: taxa for cid, taxa in community_taxa.items() if taxa["_flux"] >= flux_threshold}
    low_group = {cid: taxa for cid, taxa in community_taxa.items() if taxa["_flux"] < flux_threshold}
    logger.info("High group: %d communities, Low group: %d", len(high_group), len(low_group))

    # Collect all taxon keys (excluding _flux)
    all_taxa: set[str] = set()
    for taxa in community_taxa.values():
        all_taxa.update(k for k in taxa if k != "_flux")

    results = []
    for taxon in sorted(all_taxa):
        hi_vals = [v.get(taxon, 0.0) for v in high_group.values()]
        lo_vals = [v.get(taxon, 0.0) for v in low_group.values()]
        _, p = _mann_whitney_u(hi_vals, lo_vals)

        hi_mean = sum(hi_vals) / len(hi_vals) if hi_vals else 0
        lo_mean = sum(lo_vals) / len(lo_vals) if lo_vals else 0
        fold_change = hi_mean / lo_mean if lo_mean > 0 else float("inf")

        results.append({
            "taxon": taxon,
            "p_value": p,
            "mean_high": hi_mean,
            "mean_low": lo_mean,
            "fold_change": fold_change,
        })

    # BH correction
    pvals = [r["p_value"] for r in results]
    adjusted = _bh_correction(pvals)
    for r, adj in zip(results, adjusted):
        r["p_adj"] = adj
        r["significant"] = adj < 0.05

    results.sort(key=lambda r: r["p_adj"])

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["taxon", "p_value", "p_adj", "significant", "mean_high", "mean_low", "fold_change"])
        writer.writeheader()
        writer.writerows(results)

    n_sig = sum(1 for r in results if r["significant"])
    logger.info("Taxa enrichment: %d / %d taxa significant (FDR<0.05) → %s", n_sig, len(results), output)
    typer.echo(f"{n_sig} significantly enriched taxa → {output}")


if __name__ == "__main__":
    app()
