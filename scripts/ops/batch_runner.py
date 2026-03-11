"""
batch_runner.py — Remote/local batch job launcher (Hetzner / SLURM).

Splits a full sample list into N batches, runs each as an independent
pipeline_core subprocess, and tracks job status via receipts.

Usage (local, 20 batches):
  python batch_runner.py launch --config config.yaml --samples-json samples.json --n-batches 20

Usage (remote Hetzner SSH):
  python batch_runner.py launch --config config.yaml --samples-json samples.json \\
      --n-batches 20 --remote-host user@10.0.0.1 --remote-dir /home/pipeline/run01

Usage (dry-run to preview commands):
  python batch_runner.py launch --config config.yaml --samples-json samples.json --dry-run
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import typer
import yaml

from config_schema import PipelineConfig

app = typer.Typer()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _split_samples(samples: list[dict], n_batches: int) -> list[list[dict]]:
    """Partition samples into n_batches roughly equal chunks."""
    if not samples:
        return []
    chunk_size = math.ceil(len(samples) / max(1, n_batches))
    return [samples[i : i + chunk_size] for i in range(0, len(samples), chunk_size)]


def _python_exe() -> str:
    return sys.executable


def _launch_local_batch(
    config_path: Path,
    samples_chunk: list[dict],
    batch_idx: int,
    tier: str,
    workers: int,
    db_path: Path,
    receipts_dir: Path,
    tmp_dir: Path,
    dry_run: bool,
) -> "subprocess.Popen | None":
    """Write chunk JSON to tmp_dir and launch pipeline_core subprocess."""
    tmp_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = tmp_dir / f"batch_{batch_idx:04d}.json"
    chunk_path.write_text(json.dumps(samples_chunk))

    cmd = [
        _python_exe(), "pipeline_core.py",
        "--config",       str(config_path),
        "--tier",         tier,
        "--workers",      str(workers),
        "--db-path",      str(db_path),
        "--receipts-dir", str(receipts_dir),
        "--samples-json", str(chunk_path),
        "--target-id",    f"batch_{batch_idx:04d}",
    ]

    if dry_run:
        typer.echo(f"[dry-run] batch {batch_idx}: {' '.join(cmd)}")
        return None

    log_path = tmp_dir / f"batch_{batch_idx:04d}.log"
    logger.info("Launching batch %d → log: %s", batch_idx, log_path)
    return subprocess.Popen(
        cmd,
        stdout=open(log_path, "w"),  # noqa: SIM115
        stderr=subprocess.STDOUT,
        cwd=str(Path.cwd()),
    )


def _rsync_to_remote(remote_host: str, remote_dir: str, local_paths: list[str]) -> None:
    """rsync specific local paths to remote host:remote_dir."""
    subprocess.run(
        ["rsync", "-avz", "--mkpath"] + local_paths + [f"{remote_host}:{remote_dir}/"],
        check=True,
    )


def _tail_receipts(receipts_dir: Path, expected: int, poll_interval: int = 10) -> None:
    """Block until expected number of receipt JSON files appear in receipts_dir."""
    logger.info("Waiting for %d receipts in %s ...", expected, receipts_dir)
    while True:
        found = len(list(receipts_dir.glob("*.json")))
        logger.info("  %d / %d receipts collected", found, expected)
        if found >= expected:
            break
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.command()
def launch(
    config:       Path          = typer.Option(...,                    help="Path to pipeline config YAML"),
    samples_json: Optional[Path]= typer.Option(None,                   help="JSON file containing list of sample dicts"),
    n_batches:    int           = typer.Option(10,                     help="Number of parallel batch jobs"),
    tier:         str           = typer.Option("0",                    help="Max pipeline tier: 0, 025, 1, 2"),
    workers:      int           = typer.Option(4, "-w",                help="Worker processes per batch subprocess"),
    db_path:      Path          = typer.Option(Path("soil_microbiome.db"), help="SQLite DB path"),
    receipts_dir: Path          = typer.Option(Path("receipts/"),      help="Receipts directory"),
    tmp_dir:      Path          = typer.Option(Path(".batch_tmp/"),    help="Temp dir for chunk files + logs"),
    remote_host:  Optional[str] = typer.Option(None,                   help="SSH host (user@hostname) for remote launch"),
    remote_dir:   str           = typer.Option("/home/pipeline/run",   help="Remote working directory"),
    max_parallel: int           = typer.Option(0,                      help="Max simultaneous local subprocesses (0=all)"),
    dry_run:      bool          = typer.Option(False,                  help="Print commands without executing"),
):
    """
    Split samples into N batches and launch pipeline_core subprocesses.

    Local mode (default): parallel subprocesses on this machine, one per batch.
    Remote mode (--remote-host): rsyncs code + samples to SSH host, launches
    each batch with nohup and monitors via receipt file counts.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # --- Validate config ---
    cfg = PipelineConfig.from_yaml(config)
    logger.info("Config loaded: project=%s", cfg.project.get("name"))

    # --- Load samples ---
    if samples_json is None or not samples_json.exists():
        typer.echo("ERROR: --samples-json is required (JSON list of sample dicts)", err=True)
        raise typer.Exit(code=1)
    samples: list[dict] = json.loads(samples_json.read_text())
    typer.echo(f"Total samples: {len(samples)} → splitting into {n_batches} batch(es)")

    chunks = _split_samples(samples, n_batches)
    typer.echo(f"Chunk sizes: {[len(c) for c in chunks]}")

    receipts_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ remote
    if remote_host:
        if not dry_run:
            typer.echo(f"Syncing code to {remote_host}:{remote_dir} ...")
            local_src = [
                str(p) for p in Path(".").iterdir()
                if p.name not in {".git", ".venv", "__pycache__", ".batch_tmp", "receipts"}
                and not p.name.startswith(".")
            ]
            _rsync_to_remote(remote_host, remote_dir, local_src)
        else:
            typer.echo(f"[dry-run] Would rsync code to {remote_host}:{remote_dir}")

        for i, chunk in enumerate(chunks):
            tmp_dir.mkdir(parents=True, exist_ok=True)
            chunk_path = tmp_dir / f"batch_{i:04d}.json"
            chunk_path.write_text(json.dumps(chunk))
            remote_chunk = f"{remote_dir}/.batch_tmp/batch_{i:04d}.json"
            remote_log   = f"{remote_dir}/.batch_tmp/batch_{i:04d}.log"

            remote_cmd = (
                f"mkdir -p {remote_dir}/.batch_tmp && "
                f"nohup python {remote_dir}/pipeline_core.py "
                f"--config {remote_dir}/{config.name} "
                f"--tier {tier} -w {workers} "
                f"--db-path {remote_dir}/{db_path.name} "
                f"--receipts-dir {remote_dir}/receipts "
                f"--samples-json {remote_chunk} "
                f"--target-id batch_{i:04d} "
                f"> {remote_log} 2>&1 &"
            )
            ssh_cmd = ["ssh", remote_host, f"cd {remote_dir} && {remote_cmd}"]

            if dry_run:
                typer.echo(f"[dry-run] batch {i}: {' '.join(ssh_cmd)}")
            else:
                subprocess.run(
                    ["scp", str(chunk_path), f"{remote_host}:{remote_chunk}"],
                    check=True,
                )
                logger.info("Launching remote batch %d", i)
                subprocess.run(ssh_cmd, check=True)

        if not dry_run:
            typer.echo(f"All {len(chunks)} remote batches launched. Monitor with:")
            typer.echo(f"  ssh {remote_host} 'ls {remote_dir}/receipts/*.json | wc -l'")
        return

    # ------------------------------------------------------------------ local
    processes: list[subprocess.Popen] = []
    limit = max_parallel if max_parallel > 0 else len(chunks)

    for i, chunk in enumerate(chunks):
        proc = _launch_local_batch(
            config_path=config,
            samples_chunk=chunk,
            batch_idx=i,
            tier=tier,
            workers=workers,
            db_path=db_path,
            receipts_dir=receipts_dir,
            tmp_dir=tmp_dir,
            dry_run=dry_run,
        )
        if proc:
            processes.append(proc)
        # Throttle: wait for a slot before launching next batch
        while max_parallel > 0 and sum(1 for p in processes if p.poll() is None) >= limit:
            time.sleep(2)

    if dry_run:
        typer.echo(f"[dry-run] Would launch {len(chunks)} batch(es).")
        return

    typer.echo(f"Launched {len(processes)} batch subprocesses. Waiting for completion ...")
    failed = 0
    for proc in processes:
        rc = proc.wait()
        if rc != 0:
            failed += 1
            logger.error("Batch PID %d exited with return code %d", proc.pid, rc)

    if failed:
        typer.echo(f"WARNING: {failed}/{len(processes)} batches failed. Check .batch_tmp/*.log")
        raise typer.Exit(code=1)

    typer.echo(f"All {len(processes)} batches completed successfully.")
    typer.echo(f"Run:  python merge_receipts.py --db {db_path}  to consolidate receipts.")


if __name__ == "__main__":
    app()
