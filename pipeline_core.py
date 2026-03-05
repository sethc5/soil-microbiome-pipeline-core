"""
pipeline_core.py -- 4-tier screening funnel for soil microbiome candidates.

Tiers:
  T0    -- community composition + metadata filters (milliseconds/sample)
  T0.25 -- ML functional outcome prediction + fast similarity search (seconds/sample)
  T1    -- metabolic network modeling + community flux analysis (minutes/sample)
  T2    -- community dynamics simulation + intervention modeling (hours/sample)

Everything is config-driven via a YAML file validated by config_schema.py.
All runs write JSON receipts and persist results to SQLite via db_utils.py.

Usage:
  python pipeline_core.py --config config.yaml --tier 025 -w 8
  python pipeline_core.py --config config.yaml -w 4 --fba-workers 4

Programmatic usage:
  from pipeline_core import run_t0_batch
  results = run_t0_batch(samples, config, db)
"""

from __future__ import annotations

import json
import logging
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import typer
import yaml

from config_schema import PipelineConfig
from db_utils import SoilDB
from receipt_system import Receipt

# T0 compute modules
from compute.quality_filter import run_quality_filter
from compute.metadata_validator import validate_sample_metadata
from compute.diversity_metrics import compute_alpha_diversity, diversity_from_profile
from compute.functional_gene_scanner import scan_functional_genes, make_community_flags
from compute.tax_profiler import profile_taxonomy
from compute.tax_function_mapper import map_taxonomy_to_function, get_functional_summary

app = typer.Typer()
logger = logging.getLogger(__name__)

_DEFAULT_BATCH_SIZE = 8_000


# ---------------------------------------------------------------------------
# Public batch runner (importable, no Typer required)
# ---------------------------------------------------------------------------

def run_t0_batch(
    samples: list[dict],
    config: PipelineConfig,
    db: SoilDB,
    workers: int = 4,
    receipts_dir: str | Path = "receipts/",
    batch_size: int = _DEFAULT_BATCH_SIZE,
    target_id: str = "default",
) -> dict[str, Any]:
    """
    Run all samples through the T0 compute layer in parallel.

    Parameters
    ----------
    samples      : list of sample dicts (raw metadata + optional fastq_paths).
    config       : validated PipelineConfig.
    db           : open SoilDB connection.
    workers      : ProcessPoolExecutor max_workers.
    receipts_dir : directory to write batch receipts.
    batch_size   : max samples per parallel batch (memory guard).
    target_id    : label stored in the runs table.

    Returns
    -------
    {
        n_processed  int
        n_passed     int
        n_failed     int
        run_id       int   (DB run record ID)
        receipt_path str
        errors       list[dict]
    }
    """
    receipt = Receipt(receipts_dir=receipts_dir).start()
    run_id = db.insert_run({
        "target_id":     target_id,
        "tier":          0,
        "status":        "running",
        "n_samples_in":  len(samples),
        "config_yaml":   yaml.dump(config.model_dump()),
    })

    t0_cfg = config.t0

    n_passed = 0
    n_failed = 0
    errors: list[dict] = []

    # Process in batches to bound memory
    for batch_start_idx in range(0, len(samples), batch_size):
        batch = samples[batch_start_idx : batch_start_idx + batch_size]
        logger.info(
            "T0 batch %d-%d / %d",
            batch_start_idx, batch_start_idx + len(batch), len(samples)
        )

        if workers > 1:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {
                    pool.submit(_process_one_sample_t0, sample, t0_cfg.model_dump()): sample
                    for sample in batch
                }
                for future in as_completed(futures):
                    sample = futures[future]
                    sid = sample.get("sample_id", "<unknown>")
                    try:
                        result = future.result()
                        _persist_t0_result(result, db, run_id)
                        if result.get("passed_t0", False):
                            n_passed += 1
                        else:
                            n_failed += 1
                    except Exception as exc:
                        n_failed += 1
                        errors.append({
                            "sample_id": sid,
                            "error": str(exc),
                            "traceback": traceback.format_exc(),
                        })
                        logger.warning("T0 failed for sample %s: %s", sid, exc)
        else:
            # Single-process mode (testing / debugging)
            for sample in batch:
                sid = sample.get("sample_id", "<unknown>")
                try:
                    result = _process_one_sample_t0(sample, t0_cfg.model_dump())
                    _persist_t0_result(result, db, run_id)
                    if result.get("passed_t0", False):
                        n_passed += 1
                    else:
                        n_failed += 1
                except Exception as exc:
                    n_failed += 1
                    errors.append({
                        "sample_id": sid,
                        "error": str(exc),
                        "traceback": traceback.format_exc(),
                    })
                    logger.warning("T0 failed for sample %s: %s", sid, exc)

    receipt.n_samples_processed = n_passed + n_failed
    status = "completed" if not errors else "completed_with_errors"
    receipt_path = receipt.finish(status=status)

    db.update_run(run_id, {
        "status":         status,
        "n_samples_out":  n_passed,
        "receipt_path":   str(receipt_path),
    })

    return {
        "n_processed":  n_passed + n_failed,
        "n_passed":     n_passed,
        "n_failed":     n_failed,
        "run_id":       run_id,
        "receipt_path": str(receipt_path),
        "errors":       errors,
    }


# ---------------------------------------------------------------------------
# Single-sample T0 processing (runs in subprocess worker)
# ---------------------------------------------------------------------------

def _process_one_sample_t0(sample: dict, t0_cfg_dict: dict) -> dict:
    """
    Process a single sample through the full T0 stack.

    This function is intentionally free of DB calls so it can safely run
    in a subprocess (ProcessPoolExecutor).

    Returns a result dict with everything needed by _persist_t0_result().
    """
    from config_schema import T0Filters  # re-import in subprocess context
    t0_cfg = T0Filters(**t0_cfg_dict)

    sample_id   = sample.get("sample_id", "unknown")
    fastq_paths = sample.get("fastq_paths", [])
    fasta_path  = sample.get("fasta_path")

    result: dict[str, Any] = {
        "sample_id":      sample_id,
        "raw_sample":     sample,
        "passed_t0":      False,
        "reject_reasons": [],
        "warnings":       [],
    }

    # ---- 1. Metadata validation ----
    try:
        meta = validate_sample_metadata(sample, t0_cfg)
        result["metadata"]       = meta.get("normalised", {})
        result["meta_warnings"]  = meta.get("warnings", [])
        result["meta_reject"]    = meta.get("reject_reasons", [])
        result["reject_reasons"] += meta.get("reject_reasons", [])
    except Exception as exc:
        result["reject_reasons"].append(f"metadata_validator error: {exc}")
        result["metadata"] = {}

    # ---- 2. Quality filter (FASTQ-based) ----
    if fastq_paths:
        try:
            qf = run_quality_filter(
                fastq_paths          = fastq_paths,
                min_depth            = getattr(t0_cfg, "min_depth", 5_000),
                min_read_length      = getattr(t0_cfg, "min_read_length", 100),
                max_n_fraction       = getattr(t0_cfg, "max_n_fraction", 0.05),
                remove_host          = getattr(t0_cfg, "remove_host", False),
                host_genome_index    = getattr(t0_cfg, "host_genome_index", None),
                metadata             = result.get("metadata", {}),
            )
            result["quality_filter"] = qf
            if not qf.get("passed", True):
                result["reject_reasons"] += qf.get("reject_reasons", [])
        except Exception as exc:
            result["warnings"].append(f"quality_filter error: {exc}")
    else:
        result["quality_filter"] = {"passed": True, "note": "no_fastq_provided"}

    # ---- 3. Taxonomy profiling ----
    seq_type         = sample.get("sequencing_type", "16S")
    precomputed_prof = sample.get("precomputed_profile")
    try:
        tax = profile_taxonomy(
            fastq_paths         = fastq_paths,
            seq_type            = seq_type,
            precomputed_profile = precomputed_prof,
            classifier_path     = sample.get("classifier_path"),
            kraken_db           = sample.get("kraken_db"),
        )
        result["taxonomy"] = tax
    except Exception as exc:
        result["warnings"].append(f"tax_profiler error: {exc}")
        result["taxonomy"] = {
            "phylum_profile": sample.get("phylum_profile", {}),
            "top_genera":     sample.get("top_genera", []),
            "n_taxa":         0,
            "profiler_used":  "failed",
        }

    # ---- 4. Alpha diversity ----
    phylum_profile = result["taxonomy"].get("phylum_profile", {})
    top_genera     = result["taxonomy"].get("top_genera", [])
    try:
        div = diversity_from_profile(phylum_profile, top_genera)
        result["diversity"] = div
    except Exception as exc:
        result["warnings"].append(f"diversity_metrics error: {exc}")
        result["diversity"] = {}

    # Min diversity gate
    min_div = getattr(t0_cfg, "min_shannon_diversity", None)
    if min_div is not None:
        shannon = result["diversity"].get("shannon", None)
        if shannon is not None and shannon < min_div:
            result["reject_reasons"].append(
                f"shannon_diversity {shannon:.3f} < threshold {min_div}"
            )

    # ---- 5. Functional gene scan ----
    try:
        gene_scan = scan_functional_genes(
            fasta_path       = fasta_path,
            community_data   = sample.get("community_data"),
        )
        result["gene_scan"]    = gene_scan
        result["community_flags"] = make_community_flags(gene_scan)
    except Exception as exc:
        result["warnings"].append(f"functional_gene_scanner error: {exc}")
        result["gene_scan"]    = {}
        result["community_flags"] = {}

    # ---- 6. Taxonomy -> function mapping ----
    try:
        genus_profile = {
            g["name"]: g["rel_abundance"]
            for g in top_genera
            if g.get("rel_abundance", 0) > 0
        }
        fn_map = map_taxonomy_to_function(genus_profile or phylum_profile)
        result["function_map"]     = fn_map
        result["function_summary"] = get_functional_summary(fn_map)
    except Exception as exc:
        result["warnings"].append(f"tax_function_mapper error: {exc}")
        result["function_map"]     = {}
        result["function_summary"] = {}

    # ---- T0 pass/fail decision ----
    result["passed_t0"] = len(result["reject_reasons"]) == 0
    return result


# ---------------------------------------------------------------------------
# DB persistence (main-process only -- uses DB connection)
# ---------------------------------------------------------------------------

def _persist_t0_result(result: dict, db: SoilDB, run_id: int) -> None:
    """Write T0 results from a single sample into the database."""
    sample_id = result["sample_id"]
    meta      = result.get("metadata", {})
    taxonomy  = result.get("taxonomy", {})
    diversity = result.get("diversity", {})
    flags     = result.get("community_flags", {})
    fn_sum    = result.get("function_summary", {})

    # Upsert sample record
    db.upsert_sample({
        "sample_id":         sample_id,
        "source":            meta.get("source", "unknown"),
        "site_id":           meta.get("site_id"),
        "visit_number":      meta.get("visit_number"),
        "lat":               meta.get("lat"),
        "lon":               meta.get("lon"),
        "depth_cm":          meta.get("depth_cm"),
        "ph":                meta.get("ph"),
        "sampling_fraction": meta.get("sampling_fraction"),
        "sequencing_type":   meta.get("sequencing_type", taxonomy.get("seq_type")),
        "land_use":          meta.get("land_use"),
        "texture_class":     meta.get("texture_class"),
        "climate_zone":      meta.get("climate_zone"),
        "metadata_json":     json.dumps(meta),
    })

    # Upsert community record
    db.upsert_community({
        "sample_id":             sample_id,
        "phylum_profile_json":   json.dumps(taxonomy.get("phylum_profile", {})),
        "top_genera_json":       json.dumps(taxonomy.get("top_genera", [])),
        "n_taxa":                taxonomy.get("n_taxa", 0),
        "shannon_diversity":     diversity.get("shannon"),
        "simpson_diversity":     diversity.get("simpson"),
        "chao1":                 diversity.get("chao1"),
        "pielou_evenness":       diversity.get("pielou_evenness"),
        "fungal_bacterial_ratio": taxonomy.get("fungal_bacterial_ratio"),
        "its_profile_json":      json.dumps(taxonomy.get("its_profile", {})),
        "has_amoa_bacterial":    flags.get("has_amoa_bacterial", False),
        "has_amoa_archaeal":     flags.get("has_amoa_archaeal", False),
        "functional_genes_json": flags.get("functional_genes", "{}"),
        "n_functions_detected":  fn_sum.get("n_functions_detected", 0),
        "has_n_cycling":         fn_sum.get("has_n_cycling", False),
        "has_c_cycling":         fn_sum.get("has_c_cycling", False),
        "has_mycorrhizal":       fn_sum.get("has_mycorrhizal", False),
        "t0_passed":             result.get("passed_t0", False),
        "t0_reject_reasons":     json.dumps(result.get("reject_reasons", [])),
        "run_id":                run_id,
    })


# ---------------------------------------------------------------------------
# Typer CLI
# ---------------------------------------------------------------------------

@app.command()
def run(
    config: Path = typer.Option(..., help="Path to config YAML"),
    tier: str  = typer.Option("2", help="Maximum tier to run: 0, 025, 1, 2"),
    workers: int = typer.Option(4, "-w", help="General worker count"),
    fba_workers: int = typer.Option(2, help="Parallel COBRApy FBA workers"),
    db_path: Path = typer.Option(Path("soil_microbiome.db"), help="SQLite DB path"),
    receipts_dir: Path = typer.Option(Path("receipts/"), help="Receipts directory"),
    target_id: str = typer.Option("default", help="Target ID label for DB runs"),
    samples_json: Path = typer.Option(None, help="JSON file with sample list"),
):
    """Run the soil microbiome screening pipeline up to the specified tier."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
    )

    # Load and validate config
    cfg_dict = yaml.safe_load(config.read_text())
    pipeline_cfg = PipelineConfig(**cfg_dict)

    # Load samples
    if samples_json and samples_json.exists():
        samples = json.loads(samples_json.read_text())
    else:
        logger.error(
            "No --samples-json provided. Supply a JSON file with a list of sample dicts."
        )
        raise typer.Exit(code=1)

    logger.info(
        "Starting pipeline | tier=%s | %d samples | %d workers", tier, len(samples), workers
    )

    with SoilDB(db_path) as db:
        if tier in ("0", "all"):
            summary = run_t0_batch(
                samples      = samples,
                config       = pipeline_cfg,
                db           = db,
                workers      = workers,
                receipts_dir = receipts_dir,
                target_id    = target_id,
            )
            typer.echo(json.dumps(summary, indent=2))

        if tier in ("025", "1", "2", "all"):
            typer.echo("T0.25 / T1 / T2 tiers not yet implemented. Run --tier 0 for now.")


if __name__ == "__main__":
    app()
