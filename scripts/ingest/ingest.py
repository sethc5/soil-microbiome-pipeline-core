"""
scripts/ingest.py — Full data ingest orchestrator: NEON/SRA → T0 → T0.25 → DB.

Fetches real soil microbiome samples from NEON and/or NCBI SRA, runs them
through the T0 (and optionally T0.25) pipeline tiers, and stores results
in the SQLite database.

Designed to run as a long background job on the Hetzner server:
  tmux new -s ingest
  python scripts/ingest.py neon --workers 36 --years 2019 2020 2021 2022

Usage:
  python scripts/ingest.py neon
      [--sites HARV ORNL STER ...]       # subset of NEON sites
      [--years 2019 2020 2021]           # year filter
      [--workers 36]                     # T0 parallel workers
      [--run-t025]                       # also run T0.25 on T0 passers
      [--db /data/pipeline/db/soil_microbiome.db]
      [--config config.example.yaml]
      [--neon-token <token>]             # optional; higher rate limits

  python scripts/ingest.py sra
      [--query "cropland nitrogen fixation 16S"]
      [--max-results 1000]
      [--workers 36]

  python scripts/ingest.py both          # fetch NEON + SRA, deduplicate, run T0
"""

from __future__ import annotations

import json
import logging
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import typer
import yaml

# Ensure project root is importable when run as script/subprocess
_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from config_schema import PipelineConfig
from db_utils import SoilDB, _db_connect
from pipeline_core import run_t0_batch, run_t025_batch

logger = logging.getLogger(__name__)
app = typer.Typer(help="Ingest real soil data from NEON / SRA and run T0 pipeline", add_completion=False)

# ---------------------------------------------------------------------------
# NEON agricultural / grassland / cropland-adjacent sites to prioritise
# ---------------------------------------------------------------------------
_PRIORITY_NEON_SITES = [
    "STER",  # Sterling, CO — row crop agriculture
    "CPER",  # Central Plains Experimental Range, CO — shortgrass prairie
    "NOGP",  # Northern Great Plains Research Laboratory, ND — mixed grass
    "DCFS",  # Dakota Coteau Field School, ND — wetland/prairie
    "KONA",  # Konza Prairie LTER, KS — tallgrass prairie
    "OAES",  # Goldsby, OK — pasture
    "CLBJ",  # Lyndon B. Johnson National Grassland, TX — rangeland
    "WOOD",  # Chase Lake Wetlands, ND — wetland-edge
    "LENO",  # Lenoir Landing, AL — floodplain
    "UKFS",  # University of Kansas Field Station, KS  — forest/grass transition
    "ORNL",  # Oak Ridge National Lab, TN — managed forest
    "HARV",  # Harvard Forest, MA — temperate deciduous
    "SCBI",  # Smithsonian Conservation Biology Institute, VA
    "UNDE",  # University of Notre Dame Environmental Research Center, MI
    "KONZ",  # Konza Prairie (variant code)
]

_RECENT_YEARS = [2019, 2020, 2021, 2022, 2023]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _setup_logging(log_path: Optional[Path] = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=handlers,
        force=True,
    )


def _load_config(config_path: Path) -> PipelineConfig:
    cfg_dict = yaml.safe_load(config_path.read_text())
    return PipelineConfig(**cfg_dict)


def _samples_seen(db: SoilDB) -> set[str]:
    """Return sample_ids already in the DB to avoid re-processing."""
    try:
        with db._connect() as conn:
            rows = conn.execute("SELECT sample_id FROM samples").fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _write_checkpoint(samples: list[dict], path: Path) -> None:
    """Write fetched samples to a checkpoint JSON so work isn't lost."""
    path.write_text(json.dumps(samples, indent=2))
    logger.info("Checkpoint written: %d samples → %s", len(samples), path)


def _run_pipeline(
    samples: list[dict],
    config: PipelineConfig,
    db: SoilDB,
    workers: int,
    run_t025: bool,
    db_path: Path,
    receipts_dir: Path,
    target_id: str,
) -> dict:
    """Run T0 (and optionally T0.25) on samples, return summary."""
    logger.info("Running T0 on %d samples with %d workers", len(samples), workers)
    t0_summary = run_t0_batch(
        samples=samples,
        config=config,
        db=db,
        workers=workers,
        receipts_dir=receipts_dir,
        target_id=target_id,
    )
    logger.info(
        "T0 complete: %d passed / %d processed",
        t0_summary["n_passed"], t0_summary["n_processed"],
    )

    t025_summary: dict = {}
    if run_t025 and t0_summary["n_passed"] > 0:
        logger.info("Running T0.25 on %d T0 passers …", t0_summary["n_passed"])
        try:
            t025_summary = run_t025_batch(
                config=config,
                db=db,
                workers=workers,
                target_id=target_id,
                receipts_dir=receipts_dir,
                batch_run_label=t0_summary["batch_run_label"],
            )
            logger.info(
                "T0.25 complete: %d passed / %d processed",
                t025_summary.get("n_passed", 0),
                t025_summary.get("n_processed", 0),
            )
        except Exception as exc:
            logger.warning("T0.25 batch failed (model may not be trained yet): %s", exc)

    return {"t0": t0_summary, "t025": t025_summary}


# ---------------------------------------------------------------------------
# NEON subcommand
# ---------------------------------------------------------------------------

@app.command()
def neon(
    sites:       Optional[list[str]] = typer.Option(None, "--sites",   help="NEON site codes (default: priority agricultural list)"),
    years:       Optional[list[int]] = typer.Option(None, "--years",   help="Years to include (default: 2019-2023)"),
    workers:     int                 = typer.Option(36,   "--workers", "-w"),
    run_t025:    bool                = typer.Option(False, "--run-t025", help="Also run T0.25 on T0 passers"),
    config_path: Path                = typer.Option(Path("config.example.yaml"), "--config"),
    db_path:     Path                = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    receipts_dir:Path                = typer.Option(Path("/data/pipeline/receipts"), "--receipts-dir"),
    log_path:    Optional[Path]      = typer.Option(Path("/var/log/pipeline/ingest-neon.log"), "--log"),
    neon_token:  Optional[str]       = typer.Option(None, "--neon-token", envvar="NEON_API_TOKEN"),
    staging_dir: Path                = typer.Option(Path("/data/pipeline/staging"), "--staging-dir"),
    max_samples: int                 = typer.Option(0, "--max-samples", help="Cap total samples (0=unlimited)"),
    checkpoint:  bool                = typer.Option(True,  "--checkpoint/--no-checkpoint"),
):
    """Fetch NEON soil microbiome samples and run through T0 pipeline."""
    _setup_logging(log_path)
    logger.info("=== NEON ingest starting ===")

    staging_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)

    config = _load_config(config_path)

    from adapters.neon_adapter import NEONAdapter
    adapter = NEONAdapter(
        token=neon_token or "",
        data_dir=str(staging_dir / "neon_cache"),
    )

    effective_sites = sites or _PRIORITY_NEON_SITES
    effective_years = years or _RECENT_YEARS
    logger.info("Fetching from %d sites: %s", len(effective_sites), effective_sites)
    logger.info("Years: %s", effective_years)

    # Load checkpoint if available
    checkpoint_path = staging_dir / "neon_checkpoint.json"
    if checkpoint and checkpoint_path.exists():
        existing = json.loads(checkpoint_path.read_text())
        logger.info("Resuming from checkpoint: %d samples already fetched", len(existing))
        all_samples = existing
        seen_sites = {s.get("site") or s.get("site_id") or "" for s in existing}
    else:
        all_samples = []
        seen_sites: set[str] = set()

    with SoilDB(str(db_path)) as db:
        already_in_db = _samples_seen(db)
        logger.info("DB already has %d samples — will skip duplicates", len(already_in_db))

        for site_code in effective_sites:
            if site_code in seen_sites:
                logger.info("Skipping %s (already in checkpoint)", site_code)
                continue

            site_samples: list[dict] = []
            site_start = time.monotonic()
            try:
                for sample in adapter.iter_samples(sites=[site_code], years=effective_years):
                    sid = sample.get("sample_id", "")
                    if sid in already_in_db:
                        continue
                    site_samples.append(sample)
                    if max_samples and (len(all_samples) + len(site_samples)) >= max_samples:
                        break
                elapsed = time.monotonic() - site_start
                logger.info(
                    "Site %s: fetched %d samples in %.1fs",
                    site_code, len(site_samples), elapsed,
                )
            except Exception as exc:
                logger.warning("NEON fetch failed for site %s: %s", site_code, exc)
                continue

            all_samples.extend(site_samples)
            if checkpoint:
                _write_checkpoint(all_samples, checkpoint_path)

            if max_samples and len(all_samples) >= max_samples:
                logger.info("Hit --max-samples %d cap, stopping fetch", max_samples)
                break

        logger.info("Total samples to process: %d", len(all_samples))
        if not all_samples:
            logger.warning("No samples fetched — check site codes and NEON API connectivity")
            raise typer.Exit(code=1)

        # Also dump full sample set for batch_runner if needed
        all_samples_path = staging_dir / f"neon_samples_{int(time.time())}.json"
        _write_checkpoint(all_samples, all_samples_path)

        summary = _run_pipeline(
            samples=all_samples,
            config=config,
            db=db,
            workers=workers,
            run_t025=run_t025,
            db_path=db_path,
            receipts_dir=receipts_dir,
            target_id="neon_ingest",
        )

    # Remove checkpoint on clean completion
    if checkpoint and checkpoint_path.exists():
        checkpoint_path.unlink()
        logger.info("Checkpoint removed (clean completion)")

    logger.info("=== NEON ingest complete ===")
    typer.echo(json.dumps(summary, indent=2))


# ---------------------------------------------------------------------------
# SRA subcommand
# ---------------------------------------------------------------------------

@app.command()
def sra(
    query:       str              = typer.Option("cropland soil metagenome 16S", "--query"),
    max_results: int              = typer.Option(500, "--max-results"),
    workers:     int              = typer.Option(36, "--workers", "-w"),
    run_t025:    bool             = typer.Option(False, "--run-t025"),
    config_path: Path             = typer.Option(Path("config.example.yaml"), "--config"),
    db_path:     Path             = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    receipts_dir:Path             = typer.Option(Path("/data/pipeline/receipts"), "--receipts-dir"),
    log_path:    Optional[Path]   = typer.Option(Path("/var/log/pipeline/ingest-sra.log"), "--log"),
    staging_dir: Path             = typer.Option(Path("/data/pipeline/staging"), "--staging-dir"),
    ncbi_api_key:Optional[str]    = typer.Option(None, "--ncbi-api-key", envvar="NCBI_API_KEY"),
):
    """Search NCBI SRA for soil metagenomes and run through T0 pipeline."""
    _setup_logging(log_path)
    logger.info("=== SRA ingest starting — query: %r ===", query)

    staging_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)

    config = _load_config(config_path)

    from adapters.ncbi_sra_adapter import NCBISRAAdapter
    sra_config = {
        "max_results": max_results,
        "ncbi_api_key": ncbi_api_key or "",
        "biome": "cropland",
        "sequencing_type": "16S",
    }
    adapter = NCBISRAAdapter(sra_config)

    samples: list[dict] = []
    with SoilDB(str(db_path)) as db:
        already_in_db = _samples_seen(db)
        logger.info("DB already has %d samples", len(already_in_db))

        logger.info("Searching SRA …")
        for sample in adapter.search(biome="cropland", sequencing_type="16S"):
            sid = sample.get("sample_id", "")
            if sid in already_in_db:
                continue
            samples.append(sample)

        logger.info("%d new SRA samples fetched", len(samples))
        if not samples:
            logger.warning("No new SRA samples found")
            raise typer.Exit(code=1)

        checkpoint_path = staging_dir / f"sra_samples_{int(time.time())}.json"
        _write_checkpoint(samples, checkpoint_path)

        summary = _run_pipeline(
            samples=samples,
            config=config,
            db=db,
            workers=workers,
            run_t025=run_t025,
            db_path=db_path,
            receipts_dir=receipts_dir,
            target_id="sra_ingest",
        )

    logger.info("=== SRA ingest complete ===")
    typer.echo(json.dumps(summary, indent=2))


# ---------------------------------------------------------------------------
# Both sources
# ---------------------------------------------------------------------------

@app.command()
def both(
    workers:     int            = typer.Option(36, "--workers", "-w"),
    run_t025:    bool           = typer.Option(False, "--run-t025"),
    config_path: Path           = typer.Option(Path("config.example.yaml"), "--config"),
    db_path:     Path           = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    receipts_dir:Path           = typer.Option(Path("/data/pipeline/receipts"), "--receipts-dir"),
    log_path:    Optional[Path] = typer.Option(Path("/var/log/pipeline/ingest-both.log"), "--log"),
    staging_dir: Path           = typer.Option(Path("/data/pipeline/staging"), "--staging-dir"),
    neon_token:  Optional[str]  = typer.Option(None, "--neon-token", envvar="NEON_API_TOKEN"),
    ncbi_api_key:Optional[str]  = typer.Option(None, "--ncbi-api-key", envvar="NCBI_API_KEY"),
    neon_sites:  Optional[list[str]] = typer.Option(None, "--neon-sites"),
    years:       Optional[list[int]] = typer.Option(None, "--years"),
    sra_query:   str            = typer.Option("cropland soil nitrogen 16S", "--sra-query"),
    sra_max:     int            = typer.Option(500, "--sra-max"),
):
    """Fetch from both NEON and SRA, deduplicate, run T0 (and optionally T0.25)."""
    _setup_logging(log_path)
    logger.info("=== Combined NEON+SRA ingest starting ===")

    staging_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)
    config = _load_config(config_path)

    all_samples: list[dict] = []
    seen_ids: set[str] = set()

    # --- NEON (parallel site fetch) ---
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
        from adapters.neon_adapter import NEONAdapter

        effective_sites = neon_sites or _PRIORITY_NEON_SITES
        effective_years = years or _RECENT_YEARS
        _neon_cache = str(staging_dir / "neon_cache")
        _neon_token = neon_token or ""

        def _fetch_one_site(site_code: str) -> tuple[str, list[dict]]:
            """Fetch a single NEON site in a thread (each call gets its own adapter)."""
            adapter = NEONAdapter(token=_neon_token, data_dir=_neon_cache)
            return site_code, list(adapter.iter_samples(sites=[site_code], years=effective_years))

        logger.info(
            "Fetching NEON: %d sites in parallel (up to 8 concurrent), years %s",
            len(effective_sites), effective_years,
        )
        _site_samples: dict[str, list[dict]] = {}
        with ThreadPoolExecutor(max_workers=min(len(effective_sites), 8)) as _pool:
            _futs = {_pool.submit(_fetch_one_site, s): s for s in effective_sites}
            for _fut in _as_completed(_futs):
                _site = _futs[_fut]
                try:
                    _, _samples = _fut.result()
                    _site_samples[_site] = _samples
                    logger.info("NEON site %s: %d samples fetched", _site, len(_samples))
                except Exception as _exc:
                    logger.warning("NEON fetch failed for site %s: %s", _site, _exc)

        for _samples in _site_samples.values():
            for sample in _samples:
                sid = sample.get("sample_id", "")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    all_samples.append(sample)
        logger.info("NEON contributed %d samples", len(all_samples))
    except Exception as exc:
        logger.warning("NEON fetch failed: %s — continuing with SRA only", exc)

    # --- SRA ---
    try:
        from adapters.ncbi_sra_adapter import NCBISRAAdapter
        sra_cfg = {"max_results": sra_max, "ncbi_api_key": ncbi_api_key or "", "biome": "cropland"}
        adapter_sra = NCBISRAAdapter(sra_cfg)
        logger.info("Fetching SRA: query=%r max=%d", sra_query, sra_max)
        for sample in adapter_sra.search(biome="cropland", sequencing_type="16S"):
            sid = sample.get("sample_id", "")
            if sid and sid not in seen_ids:
                seen_ids.add(sid)
                all_samples.append(sample)
        logger.info("Total after SRA merge: %d samples", len(all_samples))
    except Exception as exc:
        logger.warning("SRA fetch failed: %s — continuing with NEON only", exc)

    if not all_samples:
        logger.error("No samples fetched from any source")
        raise typer.Exit(code=1)

    checkpoint_path = staging_dir / f"combined_samples_{int(time.time())}.json"
    _write_checkpoint(all_samples, checkpoint_path)

    with SoilDB(str(db_path)) as db:
        already = _samples_seen(db)
        filtered = [s for s in all_samples if s.get("sample_id") not in already]
        logger.info("After DB dedup: %d new samples to process (skipped %d)", len(filtered), len(all_samples) - len(filtered))
        if not filtered:
            logger.info("All samples already in DB — nothing to do")
            raise typer.Exit(code=0)

        summary = _run_pipeline(
            samples=filtered,
            config=config,
            db=db,
            workers=workers,
            run_t025=run_t025,
            db_path=db_path,
            receipts_dir=receipts_dir,
            target_id="combined_ingest",
        )

    logger.info("=== Combined ingest complete ===")
    typer.echo(json.dumps(summary, indent=2))


# ---------------------------------------------------------------------------
# Status subcommand — show how a previously-launched ingest is going
# ---------------------------------------------------------------------------

@app.command()
def status(
    db_path:     Path           = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    log_path:    Optional[Path] = typer.Option(None, "--log"),
    receipts_dir:Path           = typer.Option(Path("/data/pipeline/receipts"), "--receipts-dir"),
):
    """Print current DB counts and receipt progress."""
    import sqlite3, os
    if not db_path.exists():
        typer.echo(f"DB not found: {db_path}")
        raise typer.Exit(1)
    con = _db_connect(db_path)
    out = {"db_path": str(db_path), "db_size_mb": round(os.path.getsize(db_path) / 1e6, 2)}
    for table in ["samples", "communities", "runs"]:
        try:
            n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            out[table] = n
        except Exception:
            out[table] = "table not found"
    con.close()
    n_receipts = len(list(receipts_dir.glob("*.json"))) if receipts_dir.exists() else 0
    out["receipts"] = n_receipts
    if log_path and log_path.exists():
        with open(log_path) as f:
            lines = f.readlines()
        out["last_log_lines"] = [l.rstrip() for l in lines[-10:]]
    typer.echo(json.dumps(out, indent=2))


if __name__ == "__main__":
    app()
