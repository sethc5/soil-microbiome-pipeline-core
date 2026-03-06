"""
storage_manager.py — Phase 8.5: Storage management utilities
=============================================================
Designed for the Hetzner i9-9900K node with 2×500GB SSDs (~900 GiB usable after RAID-1).

Usage
-----
    python scripts/storage_manager.py estimate --db /path/to/soil.db
    python scripts/storage_manager.py cleanup  --staging /path/to/staging --dry-run
    python scripts/storage_manager.py estimate --staging /path/to/staging --json
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

app = typer.Typer(help="Soil-microbiome pipeline storage management utilities.")
console = Console()

_FASTQ_SUFFIXES: tuple[str, ...] = (".fastq", ".fastq.gz", ".fq", ".fq.gz")


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _human(size_bytes: int) -> str:
    """Return a human-readable byte count string."""
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:6.1f} {unit}"
        size_bytes /= 1024.0  # type: ignore[assignment]
    return f"{size_bytes:.1f} PiB"


def _dir_size_bytes(path: Path) -> int:
    """Recursively sum the size of all files under *path*."""
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file() and not entry.is_symlink():  # 3.11-safe (no follow_symlinks kwarg)
            try:
                total += entry.stat().st_size
            except OSError:
                pass
    return total


# ---------------------------------------------------------------------------
# cleanup_fastq: delete old FASTQ files from staging after OTU confirmation
# ---------------------------------------------------------------------------

def cleanup_fastq(
    staging_dir: Path,
    max_age_days: float = 7.0,
    dry_run: bool = False,
) -> dict:
    """
    Delete FASTQ files from *staging_dir* older than *max_age_days*.

    Only files whose age ≥ max_age_days AND whose parent sample directory
    contains an OTU table (``*.biom`` or ``*_otu_table.tsv``) are removed.
    This ensures FASTQ is only purged after successful OTU processing.

    Parameters
    ----------
    staging_dir : Path
        Root directory where raw FASTQ files are staged.
    max_age_days : float
        Delete files older than this many days. Default: 7.
    dry_run : bool
        If True, report what *would* be deleted without actually deleting.

    Returns
    -------
    dict with keys:
        deleted_count   int     number of files deleted (or would-be deleted)
        freed_bytes     int     total bytes freed (or that would be freed)
        skipped_count   int     files skipped due to missing OTU confirmation
        errors          list[str]  any OSError messages encountered
    """
    if not staging_dir.is_dir():
        raise FileNotFoundError(f"Staging directory not found: {staging_dir}")

    cutoff = time.time() - max_age_days * 86400
    deleted = 0
    freed = 0
    skipped = 0
    errors: list[str] = []

    for fastq_path in staging_dir.rglob("*"):
        if not fastq_path.is_file():
            continue
        if fastq_path.suffix not in _FASTQ_SUFFIXES and not fastq_path.name.endswith(
            tuple(s + ".gz" for s in _FASTQ_SUFFIXES)
        ):
            # Check full suffix chain (e.g. .fastq.gz)
            name = fastq_path.name.lower()
            if not any(name.endswith(s.lstrip(".")) for s in _FASTQ_SUFFIXES):
                continue

        # Age check
        try:
            mtime = fastq_path.stat().st_mtime
        except OSError as exc:
            errors.append(str(exc))
            continue

        if mtime >= cutoff:
            skipped += 1
            continue

        # OTU confirmation check — look for .biom or *_otu_table.tsv in same dir
        parent = fastq_path.parent
        has_otu = any(
            (parent / name).exists()
            for name in os.listdir(parent)
            if name.endswith(".biom") or name.endswith("_otu_table.tsv")
        )
        if not has_otu:
            skipped += 1
            continue

        # Remove
        size = fastq_path.stat().st_size
        if not dry_run:
            try:
                fastq_path.unlink()
            except OSError as exc:
                errors.append(str(exc))
                continue
        deleted += 1
        freed += size

    return {
        "deleted_count": deleted,
        "freed_bytes": freed,
        "skipped_count": skipped,
        "errors": errors,
        "dry_run": dry_run,
    }


# ---------------------------------------------------------------------------
# estimate_storage: summarise current disk usage per pipeline artifact class
# ---------------------------------------------------------------------------

def estimate_storage(
    db_path: Path | None = None,
    staging_dir: Path | None = None,
    results_dir: Path | None = None,
) -> dict:
    """
    Estimate storage breakdown across pipeline artifact classes.

    Parameters
    ----------
    db_path : Path | None
        Path to the SQLite database (soil.db or similar).
    staging_dir : Path | None
        FASTQ staging directory.
    results_dir : Path | None
        Pipeline results directory (T1/T2 outputs, receipts, etc.).

    Returns
    -------
    dict with keys:
        db_bytes         int
        db_row_counts    dict[str, int]   {table_name: row_count}
        staging_bytes    int
        results_bytes    int
        total_bytes      int
        budget_bytes     int   hard-coded 900 GiB for 2×500GB RAID-1 node
        pct_used         float
    """
    BUDGET = 900 * 1024 ** 3  # 900 GiB in bytes

    db_bytes = 0
    db_row_counts: dict[str, int] = {}
    if db_path and db_path.is_file():
        db_bytes = db_path.stat().st_size
        try:
            with sqlite3.connect(db_path) as con:
                tables = [
                    r[0]
                    for r in con.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                ]
                for t in tables:
                    row = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()  # noqa: S608
                    db_row_counts[t] = row[0] if row else 0
        except sqlite3.Error:
            pass

    staging_bytes = _dir_size_bytes(staging_dir) if staging_dir and staging_dir.is_dir() else 0
    results_bytes = _dir_size_bytes(results_dir) if results_dir and results_dir.is_dir() else 0
    total = db_bytes + staging_bytes + results_bytes

    return {
        "db_bytes": db_bytes,
        "db_row_counts": db_row_counts,
        "staging_bytes": staging_bytes,
        "results_bytes": results_bytes,
        "total_bytes": total,
        "budget_bytes": BUDGET,
        "pct_used": round(100.0 * total / BUDGET, 2) if BUDGET else 0.0,
    }


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

@app.command()
def estimate(
    db: Path = typer.Option(None, help="Path to pipeline SQLite database."),
    staging: Path = typer.Option(None, help="FASTQ staging directory."),
    results: Path = typer.Option(None, help="Pipeline results directory."),
    as_json: bool = typer.Option(False, "--json", help="Output raw JSON instead of table."),
) -> None:
    """Report current storage usage across pipeline artifact classes."""
    info = estimate_storage(db_path=db, staging_dir=staging, results_dir=results)

    if as_json:
        typer.echo(json.dumps(info, indent=2))
        return

    table = Table(title="Pipeline Storage Estimate", show_lines=True)
    table.add_column("Category", style="bold")
    table.add_column("Size", justify="right")
    table.add_column("Detail", style="dim")

    def db_detail() -> str:
        if not info["db_row_counts"]:
            return "-"
        parts = [f"{t}:{n}" for t, n in sorted(info["db_row_counts"].items())]
        return ", ".join(parts[:6]) + ("…" if len(parts) > 6 else "")

    table.add_row("Database (SQLite)", _human(info["db_bytes"]), db_detail())
    table.add_row("Staging (FASTQ)", _human(info["staging_bytes"]), str(staging or "-"))
    table.add_row("Results / T1+T2", _human(info["results_bytes"]), str(results or "-"))
    table.add_row(
        "TOTAL",
        _human(info["total_bytes"]),
        f"{info['pct_used']}% of 900 GiB budget",
    )
    console.print(table)


@app.command()
def cleanup(
    staging: Path = typer.Argument(..., help="FASTQ staging directory to clean."),
    max_age_days: float = typer.Option(7.0, help="Delete FASTQs older than N days after OTU confirmation."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be deleted without deleting."),
    as_json: bool = typer.Option(False, "--json", help="Output raw JSON instead of table."),
) -> None:
    """Delete old FASTQ files after confirmed OTU processing."""
    result = cleanup_fastq(staging, max_age_days=max_age_days, dry_run=dry_run)

    if as_json:
        typer.echo(json.dumps(result, indent=2))
        return

    mode = "[DRY RUN] " if dry_run else ""
    console.print(
        f"{mode}Cleaned {result['deleted_count']} FASTQ files, "
        f"freed {_human(result['freed_bytes'])}. "
        f"Skipped {result['skipped_count']} (no OTU confirmation or too recent)."
    )
    if result["errors"]:
        console.print("[red]Errors:[/red]")
        for err in result["errors"]:
            console.print(f"  {err}")


if __name__ == "__main__":
    app()
