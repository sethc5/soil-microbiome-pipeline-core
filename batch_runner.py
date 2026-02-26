"""
batch_runner.py — Remote batch job launcher (Hetzner / SLURM).

Splits a full sample list into batches, submits each batch as a
separate compute job, and tracks job IDs for later receipt collection.

Usage:
  python batch_runner.py --config config.yaml --n-batches 20
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def launch(
    config: Path = typer.Option(...),
    n_batches: int = typer.Option(10, help="Number of parallel batch jobs"),
    dry_run: bool = typer.Option(False, help="Print commands without executing"),
):
    """Launch remote batch jobs for a pipeline run."""
    raise NotImplementedError("batch_runner.launch not yet implemented")


if __name__ == "__main__":
    app()
