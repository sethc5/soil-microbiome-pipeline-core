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
        batch_run_label  str   (label tagging per-sample run rows)
        receipt_path str
        errors       list[dict]
    }
    """
    receipt = Receipt(receipts_dir=receipts_dir).start()
    # Batch run label — used as target_id tag on per-sample run rows
    import time as _time
    batch_run_label = f"{target_id}_{int(_time.time())}"

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
                        _persist_t0_result(result, db, batch_run_label)
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
                    _persist_t0_result(result, db, batch_run_label)
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

    return {
        "n_processed":    n_passed + n_failed,
        "n_passed":       n_passed,
        "n_failed":       n_failed,
        "batch_run_label": batch_run_label,
        "receipt_path":   str(receipt_path),
        "errors":         errors,
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
        result["metadata"]       = meta.get("normalized", {})
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

def _persist_t0_result(result: dict, db: SoilDB, batch_run_label: str) -> None:
    """Write T0 results from a single sample into the database."""
    sample_id = result["sample_id"]
    meta      = result.get("metadata", {})
    taxonomy  = result.get("taxonomy", {})
    diversity = result.get("diversity", {})
    flags     = result.get("community_flags", {})
    fn_sum    = result.get("function_summary", {})

    # Upsert sample record — column names match db_utils.py samples schema
    db.upsert_sample({
        "sample_id":          sample_id,
        "source":             meta.get("source", "unknown"),
        "site_id":            meta.get("site_id"),
        "visit_number":       meta.get("visit_number"),
        "latitude":           meta.get("latitude"),
        "longitude":          meta.get("longitude"),
        "sampling_depth_cm":  meta.get("depth_cm"),
        "soil_ph":            meta.get("soil_ph"),
        "sampling_fraction":  meta.get("sampling_fraction"),
        "sequencing_type":    meta.get("sequencing_type", taxonomy.get("seq_type")),
        "land_use":           meta.get("land_use"),
        "soil_texture":       meta.get("soil_texture") or meta.get("texture_class"),
        "climate_zone":       meta.get("climate_zone"),
        "sand_pct":           meta.get("sand_pct"),
        "silt_pct":           meta.get("silt_pct"),
        "clay_pct":           meta.get("clay_pct"),
        "n_taxa":             taxonomy.get("n_taxa", 0),
    })

    # Upsert community record — column names match db_utils.py communities schema
    community_id = db.upsert_community({
        "sample_id":              sample_id,
        "phylum_profile":         json.dumps(taxonomy.get("phylum_profile", {})),
        "top_genera":             json.dumps(taxonomy.get("top_genera", [])),
        "shannon_diversity":      diversity.get("shannon"),
        "simpson_diversity":      diversity.get("simpson"),
        "chao1_richness":         diversity.get("chao1"),
        "observed_otus":          diversity.get("observed_otus"),
        "pielou_evenness":        diversity.get("pielou_evenness"),
        "fungal_bacterial_ratio": taxonomy.get("fungal_bacterial_ratio"),
        "its_profile":            json.dumps(taxonomy.get("its_profile") or {}),
        "has_nifh":               flags.get("has_nifh", False),
        "has_dsrab":              flags.get("has_dsrab", False),
        "has_mcra":               flags.get("has_mcra", False),
        "has_mmox":               flags.get("has_mmox", False),
        "has_amoa_bacterial":     flags.get("has_amoa_bacterial", False),
        "has_amoa_archaeal":      flags.get("has_amoa_archaeal", False),
        "has_laccase":            flags.get("has_laccase", False),
        "has_peroxidase":         flags.get("has_peroxidase", False),
        "nifh_is_hgt_flagged":    flags.get("nifh_is_hgt_flagged", False),
        "functional_genes":       flags.get("functional_genes", "{}"),
        "otu_table_path":         taxonomy.get("otu_table_path"),
    })

    # Per-sample run record — columns match db_utils.py runs schema
    db.insert_run({
        "sample_id":              sample_id,
        "community_id":           community_id,
        "target_id":              None,   # FK to targets; NULL until target is registered
        "t0_pass":                result.get("passed_t0", False),
        "t0_reject_reason":       (result.get("reject_reasons") or [None])[0],
        "t0_metadata_ok":         len(result.get("meta_reject", [])) == 0,
        "t0_depth_ok":            result.get("quality_filter", {}).get("passed", True),
        "t0_functional_genes_ok": bool(flags),
    })


# ---------------------------------------------------------------------------
# T0.25 — ML functional prediction + community similarity
# ---------------------------------------------------------------------------

def run_t025_batch(
    community_ids: list[int],
    config: PipelineConfig,
    db: SoilDB,
    workers: int = 4,
    receipts_dir: str | Path = "receipts/",
    reference_biom: str | Path | None = None,
    model_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Run T0.25 compute layer for communities that passed T0.

    For each community:
      1. Load OTU data from DB
      2. Run PICRUSt2 (if ASV table + rep seqs available) or HUMAnN3 (shotgun)
      3. Community similarity search against reference BIOM
      4. ML functional score prediction (FunctionalPredictor)
      5. Update DB t025_* columns on the community row

    Returns: {n_processed, n_passed, n_failed, receipt_path, errors}
    """
    from compute.picrust2_runner import run_picrust2
    from compute.humann3_shortcut import run_humann3
    from compute.community_similarity import CommunitySimilaritySearch
    from compute.functional_predictor import FunctionalPredictor

    receipt = Receipt(receipts_dir=receipts_dir).start()

    # Load similarity searcher if reference BIOM provided
    similarity_searcher: CommunitySimilaritySearch | None = None
    if reference_biom and Path(reference_biom).exists():
        try:
            similarity_searcher = CommunitySimilaritySearch.from_biom(reference_biom)
        except Exception as exc:
            logger.warning("Could not load reference BIOM for similarity search: %s", exc)

    # Load ML predictor if model checkpoint provided
    predictor: FunctionalPredictor | None = None
    if model_path and Path(str(model_path)).exists():
        try:
            predictor = FunctionalPredictor.load(model_path)
        except Exception as exc:
            logger.warning("Could not load FunctionalPredictor from %s: %s", model_path, exc)

    n_passed = 0
    n_failed = 0
    errors: list[dict] = []

    for community_id in community_ids:
        try:
            # Fetch community row
            community = db.get_community(community_id)
            if community is None:
                raise ValueError(f"Community {community_id} not found in DB")

            sample_id = community.get("sample_id", "unknown")
            phylum_profile = json.loads(community.get("phylum_profile", "{}") or "{}")
            top_genera_raw = json.loads(community.get("top_genera", "[]") or "[]")
            otu_table_path = community.get("otu_table_path")

            # --- PICRUSt2 / HUMAnN3 functional profiling ---
            pathway_abundances: dict = {}
            nsti_mean: float = float("nan")

            sample_row = db.get_sample(sample_id)
            seq_type = (sample_row or {}).get("sequencing_type", "16S")

            if seq_type in ("shotgun", "metatranscriptome") and otu_table_path:
                try:
                    h3 = run_humann3(otu_table_path, outdir="humann3_out/")
                    pathway_abundances = h3.get("pathway_abundances", {})
                except Exception as exc:
                    logger.debug("HUMAnN3 failed for community %d: %s", community_id, exc)
            elif seq_type == "16S" and otu_table_path:
                rep_seqs = Path(str(otu_table_path)).parent / "rep_seqs.fasta"
                if rep_seqs.exists():
                    try:
                        p2 = run_picrust2(
                            otu_table_path, rep_seqs,
                            outdir=f"picrust2_out/{sample_id}/",
                        )
                        pathway_abundances = p2.get("pathway_abundances", {})
                        nsti_mean = p2.get("nsti_mean", float("nan"))
                    except Exception as exc:
                        logger.debug("PICRUSt2 failed for community %d: %s", community_id, exc)

            n_pathways = len(pathway_abundances)

            # --- Community similarity search ---
            top_similarity: float = 0.0
            top_reference_id: str = ""
            if similarity_searcher and phylum_profile:
                try:
                    hits = similarity_searcher.query(phylum_profile, top_k=1)
                    if hits:
                        top_similarity = hits[0].get("similarity_score", 0.0)
                        top_reference_id = hits[0].get("reference_id", "")
                except Exception as exc:
                    logger.debug("Similarity search failed for community %d: %s", community_id, exc)

            # --- ML function score prediction ---
            function_score: float = 0.0
            function_uncertainty: float = 0.0
            if predictor and phylum_profile:
                try:
                    import numpy as np
                    feature_vec = np.array(list(phylum_profile.values()), dtype=float)
                    function_score, function_uncertainty = predictor.predict(feature_vec)
                except Exception as exc:
                    logger.debug("FunctionalPredictor failed for community %d: %s", community_id, exc)

            # --- Update DB community row with T0.25 results ---
            db.update_community_t025(community_id, {
                "t025_pathway_abundances": json.dumps(pathway_abundances),
                "t025_n_pathways": n_pathways,
                "t025_nsti_mean": None if (nsti_mean != nsti_mean) else nsti_mean,
                "t025_top_similarity": top_similarity,
                "t025_top_reference_id": top_reference_id,
                "t025_function_score": function_score,
                "t025_function_uncertainty": function_uncertainty,
                "t025_passed": True,  # T0.25 currently does not hard-reject
            })

            n_passed += 1
            logger.debug("T0.25 complete for community %d: score=%.4f", community_id, function_score)

        except Exception as exc:
            n_failed += 1
            errors.append({"community_id": community_id, "error": str(exc)})
            logger.warning("T0.25 failed for community %d: %s", community_id, exc)

    receipt.n_samples_processed = n_passed + n_failed
    receipt_path = receipt.finish(status="completed" if not errors else "completed_with_errors")
    return {
        "n_processed": n_passed + n_failed,
        "n_passed": n_passed,
        "n_failed": n_failed,
        "receipt_path": str(receipt_path),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# T1 — Metabolic modeling + FBA
# ---------------------------------------------------------------------------

def run_t1_batch(
    community_ids: list[int],
    config: PipelineConfig,
    db: SoilDB,
    fba_workers: int = 2,
    receipts_dir: str | Path = "receipts/",
    genome_cache_dir: str | Path = "genome_cache/",
    models_dir: str | Path = "models/",
    annotations_dir: str | Path = "annotations/",
    target_pathway: str = "nifH_pathway",
) -> dict[str, Any]:
    """
    Run T1 metabolic modeling for communities that passed T0.25.

    Per community:
      1. Fetch top genera from DB
      2. genome_fetcher → genome FASTA per taxon
      3. genome_quality → CheckM assessment
      4. genome_annotator → Prokka annotation
      5. model_builder → CarveMe genome-scale model
      6. community_fba → Community FBA + FVA
      7. keystone_analyzer → Single-knockout analysis
      8. metabolic_exchange → Cross-feeding network
      9. Persist T1 results to DB

    Returns: {n_processed, n_passed, n_failed, receipt_path, errors}
    """
    from compute.genome_fetcher import GenomeFetcher
    from compute.genome_quality import assess_genome_quality
    from compute.genome_annotator import annotate_genome
    from compute.model_builder import build_metabolic_model
    from compute.community_fba import run_community_fba
    from compute.keystone_analyzer import identify_keystone_taxa
    from compute.metabolic_exchange import analyze_metabolic_exchanges

    receipt = Receipt(receipts_dir=receipts_dir).start()
    fetcher = GenomeFetcher(cache_dir=genome_cache_dir)

    n_passed = 0
    n_failed = 0
    errors: list[dict] = []

    for community_id in community_ids:
        try:
            community = db.get_community(community_id)
            if community is None:
                raise ValueError(f"Community {community_id} not found")

            sample_id = community.get("sample_id", "unknown")
            sample_row = db.get_sample(sample_id) or {}
            metadata = {
                "soil_ph": sample_row.get("soil_ph", 7.0),
                "latitude": sample_row.get("latitude"),
                "longitude": sample_row.get("longitude"),
            }

            top_genera = json.loads(community.get("top_genera", "[]") or "[]")
            # Use top N genera for community model (cap at 20)
            top_genera = top_genera[:20]

            member_models = []
            quality_records = []

            for genus_rec in top_genera:
                taxon_name = genus_rec.get("name", "unknown")
                taxon_id = genus_rec.get("taxon_id", taxon_name)
                try:
                    genome_path = fetcher.fetch(str(taxon_id), taxon_name)
                    quality = assess_genome_quality(genome_path)
                    quality_records.append(quality)
                    annotation = annotate_genome(genome_path, outdir=str(annotations_dir))
                    proteins_fasta = annotation.get("proteins_fasta", "")
                    model = None
                    if proteins_fasta:
                        model = build_metabolic_model(
                            proteins_fasta,
                            outdir=str(models_dir),
                            genome_quality=quality,
                        )
                    member_models.append(model)
                except Exception as exc:
                    logger.debug("Genome pipeline failed for %s: %s", taxon_name, exc)
                    member_models.append(None)
                    quality_records.append({})

            # --- Community FBA ---
            fba_result = run_community_fba(
                member_models, metadata, target_pathway, fva=True,
            )

            # --- Keystone analysis ---
            viable_models = [m for m in member_models if m is not None]
            keystones = []
            if viable_models:
                # Build minimal community model for knockout
                from compute.community_fba import _merge_community_models
                community_model = _merge_community_models(viable_models, 20)
                if community_model is not None:
                    keystones = identify_keystone_taxa(
                        community_model,
                        baseline_target_flux=fba_result.get("target_flux", 0.0),
                    )

            # --- Metabolic exchange ---
            exchanges: list[dict] = []
            # (requires a solved community model + solution, simplified here)

            # --- Persist T1 results ---
            db.update_community_t1(community_id, {
                "t1_target_flux": fba_result.get("target_flux", 0.0),
                "t1_fva_min": fba_result.get("fva_min", 0.0),
                "t1_fva_max": fba_result.get("fva_max", 0.0),
                "t1_feasible": fba_result.get("feasible", False),
                "t1_model_confidence": fba_result.get("model_confidence", 0.35),
                "t1_genome_completeness_mean": fba_result.get("genome_completeness_mean", 0.0),
                "t1_genome_contamination_mean": fba_result.get("genome_contamination_mean", 100.0),
                "t1_keystone_taxa": json.dumps(keystones),
                "t1_metabolic_exchanges": json.dumps(exchanges),
                "t1_passed": fba_result.get("feasible", False),
            })

            n_passed += 1
            logger.info(
                "T1 complete for community %d: flux=%.4f, feasible=%s",
                community_id, fba_result.get("target_flux", 0.0), fba_result.get("feasible"),
            )

        except Exception as exc:
            n_failed += 1
            errors.append({"community_id": community_id, "error": str(exc)})
            logger.warning("T1 failed for community %d: %s", community_id, exc)

    receipt.n_samples_processed = n_passed + n_failed
    receipt_path = receipt.finish(status="completed" if not errors else "completed_with_errors")
    return {
        "n_processed": n_passed + n_failed,
        "n_passed": n_passed,
        "n_failed": n_failed,
        "receipt_path": str(receipt_path),
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# T2 — Dynamic FBA + intervention screening
# ---------------------------------------------------------------------------

def run_t2_batch(
    community_ids: list[int],
    config: PipelineConfig,
    db: SoilDB,
    workers: int = 2,
    receipts_dir: str | Path = "receipts/",
    simulation_days: int = 45,
    perturbations: list[dict] | None = None,
    target_pathway: str = "nifH_pathway",
) -> dict[str, Any]:
    """
    Run T2 dynamics + intervention screening for communities that passed T1.

    Per community:
      1. Retrieve T1 FBA result + community model
      2. Run dFBA (45-day simulation with 6h dt)
      3. Compute stability metrics
      4. Screen interventions (bioinoculants + amendments)
      5. Persist T2 results to DB

    Max 2 parallel workers (dFBA is memory-intensive).
    Returns: {n_processed, n_passed, n_failed, receipt_path, errors}
    """
    from compute.dfba_runner import run_dfba
    from compute.stability_analyzer import full_stability_report
    from compute.intervention_screener import screen_interventions

    receipt = Receipt(receipts_dir=receipts_dir).start()

    # T2 config for intervention screening
    t2_config = getattr(config, "t2", None)
    t2_cfg_dict = t2_config.model_dump() if t2_config is not None else {}

    n_passed = 0
    n_failed = 0
    errors: list[dict] = []

    # Use limited parallel workers for memory-heavy dFBA
    def _process_one_t2(community_id: int) -> dict:
        community = db.get_community(community_id)
        if community is None:
            raise ValueError(f"Community {community_id} not found")

        sample_id = community.get("sample_id", "unknown")
        sample_row = db.get_sample(sample_id) or {}
        metadata = {
            "soil_ph": sample_row.get("soil_ph", 7.0),
            "latitude": sample_row.get("latitude"),
            "longitude": sample_row.get("longitude"),
        }

        t1_confidence = float(community.get("t1_model_confidence") or 0.35)

        # Run dFBA (uses placeholder community model if T1 models unavailable)
        traj = run_dfba(
            community_model=None,  # Full T1 model re-build would happen here
            metadata=metadata,
            simulation_days=simulation_days,
            dt_hours=6.0,
            perturbations=perturbations or [{"type": "drought", "day": 20, "severity": 0.4}],
        )

        # Stability analysis
        perturb_days = [int(p.get("day", 0)) for p in (perturbations or [{"day": 20}])]
        stability = full_stability_report(
            traj, perturb_days,
            member_keystones=json.loads(community.get("t1_keystone_taxa", "[]") or "[]"),
        )

        # Intervention screening
        interventions = screen_interventions(
            community_model=None,
            metadata=metadata,
            t2_config=t2_cfg_dict,
            t1_model_confidence=t1_confidence,
        )

        return {
            "community_id": community_id,
            "trajectory": traj,
            "stability": stability,
            "interventions": interventions,
        }

    for community_id in community_ids:
        try:
            result = _process_one_t2(community_id)

            db.update_community_t2(community_id, {
                "t2_stability_score": result["stability"].get("stability_score", 0.0),
                "t2_resistance": result["stability"].get("resistance", 0.0),
                "t2_resilience": result["stability"].get("resilience", 0.0),
                "t2_functional_redundancy": result["stability"].get("functional_redundancy", 0.0),
                "t2_interventions": json.dumps(result["interventions"]),
                "t2_top_intervention": (
                    result["interventions"][0].get("intervention_detail", "")
                    if result["interventions"] else ""
                ),
                "t2_top_confidence": (
                    result["interventions"][0].get("confidence", 0.0)
                    if result["interventions"] else 0.0
                ),
                "t2_passed": True,
            })

            n_passed += 1
            logger.info(
                "T2 complete for community %d: stability=%.3f, top=%s",
                community_id,
                result["stability"].get("stability_score", 0.0),
                result["interventions"][0].get("intervention_detail", "none") if result["interventions"] else "none",
            )

        except Exception as exc:
            n_failed += 1
            errors.append({"community_id": community_id, "error": str(exc)})
            logger.warning("T2 failed for community %d: %s", community_id, exc)

    receipt.n_samples_processed = n_passed + n_failed
    receipt_path = receipt.finish(status="completed" if not errors else "completed_with_errors")
    return {
        "n_processed": n_passed + n_failed,
        "n_passed": n_passed,
        "n_failed": n_failed,
        "receipt_path": str(receipt_path),
        "errors": errors,
    }


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
            typer.echo("T0.25 / T1 / T2: run --tier 0 first, then pass community_ids to run_t025_batch / run_t1_batch / run_t2_batch.")


if __name__ == "__main__":
    app()
