"""
findings_generator.py — Anomaly detection and FINDINGS.md writer.

Runs the correlation_scanner, taxa_enrichment, and spatial_analysis outputs
through an anomaly detection pass, then writes notable findings to FINDINGS.md
in the instantiation repo directory.

Usage:
  python findings_generator.py --config config.yaml
"""

import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def generate(
    config: Path = typer.Option(...),
    db: Path = typer.Option(Path("landscape.db")),
    output: Path = typer.Option(Path("FINDINGS.md")),
):
    """Generate FINDINGS.md from accumulated pipeline results."""
    raise NotImplementedError("findings_generator.generate not yet implemented")


if __name__ == "__main__":
    app()
