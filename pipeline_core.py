"""
pipeline_core.py — 4-tier screening funnel for soil microbiome candidates.

Tiers:
  T0    — community composition + metadata filters (milliseconds/sample)
  T0.25 — ML functional outcome prediction + fast similarity search (seconds/sample)
  T1    — metabolic network modeling + community flux analysis (minutes/sample)
  T2    — community dynamics simulation + intervention modeling (hours/sample)

Everything is config-driven via a YAML file validated by config_schema.py.
All runs write JSON receipts and persist results to SQLite via db_utils.py.

Usage:
  python pipeline_core.py --config config.yaml --tier 025 -w 8
  python pipeline_core.py --config config.yaml -w 4 --fba-workers 4
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def run(
    config: Path = typer.Option(..., help="Path to config YAML"),
    tier: str = typer.Option("2", help="Maximum tier to run: 0, 025, 1, 2"),
    workers: int = typer.Option(4, "-w", help="General worker count"),
    fba_workers: int = typer.Option(2, help="Parallel COBRApy FBA workers"),
):
    """Run the soil microbiome screening pipeline up to the specified tier."""
    raise NotImplementedError("pipeline_core.run not yet implemented")


if __name__ == "__main__":
    app()
