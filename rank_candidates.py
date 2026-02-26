"""
rank_candidates.py — Score T1/T2 communities and rank intervention strategies.

Reads run results from the database and produces a ranked list of communities
and associated interventions ordered by composite score:
  target_flux × stability × establishment_probability

Usage:
  python rank_candidates.py --config config.yaml --db nitrogen_landscape.db --top 50
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def rank(
    config: Path = typer.Option(...),
    db: Path = typer.Option(Path("landscape.db")),
    top: int = typer.Option(50, help="Number of top candidates to report"),
    output: Path = typer.Option(Path("results/ranked_candidates.csv")),
):
    """Rank communities and interventions from accumulated run results."""
    raise NotImplementedError("rank_candidates.rank not yet implemented")


if __name__ == "__main__":
    app()
