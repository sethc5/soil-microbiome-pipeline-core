"""
taxa_enrichment.py — Identify taxa enriched in high-performing communities.

Compares taxonomic composition between top-ranked communities (by target
function score) vs. all others. Reports genera/families/phyla with
statistically significant enrichment.

Uses: Mann-Whitney U, FDR correction (statsmodels), effect size (Cohen's d).

Usage:
  python taxa_enrichment.py --db nitrogen_landscape.db --top-pct 10
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def enrich(
    db: Path = typer.Option(Path("landscape.db")),
    top_pct: float = typer.Option(10.0, help="Top percentile to treat as 'high-performing'"),
    rank: str = typer.Option("genus", help="Taxonomic rank: genus, family, phylum"),
    output: Path = typer.Option(Path("results/taxa_enrichment.csv")),
):
    """Compute taxonomic enrichment in high-performing communities."""
    raise NotImplementedError("taxa_enrichment.enrich not yet implemented")


if __name__ == "__main__":
    app()
