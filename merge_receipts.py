"""
merge_receipts.py — Ingest remote batch receipts into the SQLite receipts table.

Scans receipts/ directory for JSON files not yet loaded into the database
and inserts them, providing an audit trail and FBA cost accounting.

Usage:
  python merge_receipts.py --list           # show unmerged receipts
  python merge_receipts.py                  # merge all unmerged receipts
  python merge_receipts.py --db my.db
"""

import json
import logging
import typer
from pathlib import Path

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.command()
def merge(
    receipts_dir: Path = typer.Option(Path("receipts/"), help="Receipts directory"),
    db: Path = typer.Option(Path("landscape.db"), help="SQLite database path"),
    list_only: bool = typer.Option(False, "--list", help="List unmerged receipts only"),
):
    """Ingest JSON receipts into the SQLite receipts table."""
    raise NotImplementedError("merge_receipts.merge not yet implemented")


if __name__ == "__main__":
    app()
