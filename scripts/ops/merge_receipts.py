"""
merge_receipts.py — Ingest remote batch receipts into the SQLite receipts table.

Scans receipts/ directory for JSON files not yet loaded into the database
and inserts them, providing an audit trail and FBA cost accounting.

Usage:
  python merge_receipts.py --list           # show unmerged receipts
  python merge_receipts.py                  # merge all unmerged receipts
  python merge_receipts.py --db my.db
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_receipt_json(path: Path) -> dict | None:
    """Parse a receipt JSON file; return None on error."""
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not parse %s: %s", path, exc)
        return None


def _get_known_receipt_ids(db: SoilDB) -> set[str]:
    """Return set of receipt_ids already present in the DB receipts table."""
    with db._connect() as conn:
        rows = conn.execute("SELECT receipt_id FROM receipts").fetchall()
    return {row[0] for row in rows}


def _insert_receipt(db: SoilDB, payload: dict) -> None:
    """Insert one receipt dict into the receipts table via SoilDB._insert."""
    db._insert("receipts", {
        "receipt_id":          payload.get("receipt_id", ""),
        "machine_id":          payload.get("machine_id", ""),
        "batch_start":         payload.get("batch_start"),
        "batch_end":           payload.get("batch_end"),
        "n_samples_processed": payload.get("n_samples_processed", 0),
        "n_fba_runs":          payload.get("n_fba_runs", 0),
        "n_dynamics_runs":     payload.get("n_dynamics_runs", 0),
        "status":              payload.get("status", "unknown"),
        "filepath":            payload.get("filepath", ""),
    })


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.command()
def merge(
    receipts_dir: Path = typer.Option(Path("receipts/"),         help="Receipts directory"),
    db:           Path = typer.Option(Path("soil_microbiome.db"), help="SQLite database path"),
    list_only:    bool = typer.Option(False, "--list",            help="List unmerged receipts, do not merge"),
):
    """Ingest JSON receipts into the SQLite receipts table."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if not receipts_dir.exists():
        typer.echo(f"Receipts directory not found: {receipts_dir}")
        raise typer.Exit(code=0)

    all_json = sorted(receipts_dir.glob("*.json"))
    if not all_json:
        typer.echo("No receipt files found.")
        return

    with SoilDB(db) as soil_db:
        known_ids = _get_known_receipt_ids(soil_db)

        # Determine which files are new
        unmerged: list[tuple[Path, dict]] = []
        for path in all_json:
            payload = _load_receipt_json(path)
            if payload and payload.get("receipt_id") not in known_ids:
                unmerged.append((path, payload))

        typer.echo(
            f"Found {len(all_json)} receipt file(s) | "
            f"{len(known_ids)} already in DB | "
            f"{len(unmerged)} new"
        )

        # --list: print table and exit
        if list_only:
            table = Table(
                "Receipt ID", "Machine", "Status",
                "Samples", "FBA runs", "Dynamics runs",
            )
            for _, r in unmerged:
                table.add_row(
                    r.get("receipt_id", "?")[:20],
                    r.get("machine_id",  "?"),
                    r.get("status",      "?"),
                    str(r.get("n_samples_processed", 0)),
                    str(r.get("n_fba_runs",          0)),
                    str(r.get("n_dynamics_runs",     0)),
                )
            console.print(table)
            return

        # --- Merge ---
        merged        = 0
        total_samples = 0
        total_fba     = 0
        total_dynamics= 0
        errors        = 0

        for path, payload in unmerged:
            try:
                _insert_receipt(soil_db, payload)
                merged         += 1
                total_samples  += payload.get("n_samples_processed", 0)
                total_fba      += payload.get("n_fba_runs",          0)
                total_dynamics += payload.get("n_dynamics_runs",     0)
                logger.debug("Merged receipt %s", payload.get("receipt_id"))
            except Exception as exc:          # noqa: BLE001
                logger.error("Failed to insert %s: %s", path.name, exc)
                errors += 1

    # --- Cost accounting summary ---
    table = Table(title="Receipt Merge Summary")
    table.add_column("Metric",  style="bold")
    table.add_column("Value",   justify="right")
    table.add_row("Receipts merged",         str(merged))
    table.add_row("Errors",                  str(errors))
    table.add_row("Samples processed",       str(total_samples))
    table.add_row("FBA runs (expensive)",    str(total_fba))
    table.add_row("Dynamics runs (costly)",  str(total_dynamics))
    console.print(table)

    if errors:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
