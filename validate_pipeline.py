"""
validate_pipeline.py — Known community recovery test (mandatory first step).

Takes a set of soil samples with published target-function measurements
and verifies that:
  1. High-function samples pass T0 more often than low-function samples
  2. T0.25 ML scores correlate with measured function (Spearman r > 0.6 target)
  3. T1 predicted fluxes are within 2 orders of magnitude of measured values

If validation fails, the pipeline is not ready for production screening.

Usage:
  python validate_pipeline.py \
    --config config.yaml \
    --reference-communities reference/high_bnf_communities.biom \
    --measured-function reference/bnf_measurements.csv
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def validate(
    config: Path = typer.Option(...),
    reference_communities: Path = typer.Option(...),
    measured_function: Path = typer.Option(...),
    spearman_threshold: float = typer.Option(0.6, help="Minimum acceptable Spearman r"),
):
    """Run known-community recovery validation."""
    raise NotImplementedError("validate_pipeline.validate not yet implemented")


if __name__ == "__main__":
    app()
