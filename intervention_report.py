"""
intervention_report.py — Generate actionable field recommendations from T2 results.

Aggregates the top-ranked interventions from the database and writes a structured
report: which organisms/amendments to apply, at what concentration/rate, in which
soil context, with predicted outcome and confidence.

Output: results/intervention_report.md (human-readable) + .json (machine-readable)

Usage:
  python intervention_report.py --config config.yaml --db nitrogen_landscape.db --top 20
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def report(
    config: Path = typer.Option(...),
    db: Path = typer.Option(Path("landscape.db")),
    top: int = typer.Option(20),
    output_dir: Path = typer.Option(Path("results/")),
):
    """Write intervention report for top T2 candidates."""
    raise NotImplementedError("intervention_report.report not yet implemented")


if __name__ == "__main__":
    app()
