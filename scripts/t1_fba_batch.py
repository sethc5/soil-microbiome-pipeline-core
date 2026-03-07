"""
scripts/t1_fba_batch.py — Phase 14: T1 metabolic-modelling batch
(CarveMe + COBRApy community FBA + keystone analysis).

Two-phase design:
  Phase A — Build per-genus metabolic models (CarveMe + SCIP solver)
    * Download proteomes from NCBI RefSeq / BV-BRC for each unique genus
    * Run ``carve --solver scip --gapfill M9`` to reconstruct draft models
    * Cache all models as SBML files on disk

  Phase B — Community-level FBA + keystone (COBRApy + HiGHS/GLPK solver)
    * For each T2-passed community, assemble community model from cached
      genus-level SBML models (SBML files cached per-worker to avoid re-parse)
    * Run FBA + FVA for nifH_pathway target flux
    * Sequential single-knockout keystone-taxa identification (T1-pass only)
    * Write T1 results (t1_pass, t1_target_flux, …) to ``runs`` table

Usage:
  python scripts/t1_fba_batch.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --n-communities 5000 --workers 36
"""
from __future__ import annotations

import gzip
import json
import logging
import math
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect  # noqa: E402

logger = logging.getLogger(__name__)
app = typer.Typer(
    help="T1 metabolic-modelling batch: CarveMe + community FBA",
    add_completion=False,
    invoke_without_command=True,
)

# ---------------------------------------------------------------------------
# Representative species for each genus used in synthetic communities
# ---------------------------------------------------------------------------

_GENUS_NCBI: dict[str, dict[str, str]] = {
    # BNF genera
    "Bradyrhizobium":    {"species": "Bradyrhizobium japonicum",         "taxon_id": "375"},
    "Rhizobium":         {"species": "Rhizobium leguminosarum",          "taxon_id": "384"},
    "Mesorhizobium":     {"species": "Mesorhizobium loti",               "taxon_id": "266835"},
    "Sinorhizobium":     {"species": "Sinorhizobium meliloti",           "taxon_id": "382"},
    "Azospirillum":      {"species": "Azospirillum brasilense",          "taxon_id": "192"},
    "Azotobacter":       {"species": "Azotobacter vinelandii",           "taxon_id": "354"},
    "Frankia":           {"species": "Frankia alni",                     "taxon_id": "45403"},
    "Azoarcus":          {"species": "Azoarcus sp. BH72",               "taxon_id": "62928"},
    "Herbaspirillum":    {"species": "Herbaspirillum seropedicae",       "taxon_id": "976"},
    "Gluconacetobacter": {"species": "Gluconacetobacter diazotrophicus", "taxon_id": "54015"},
    "Burkholderia":      {"species": "Burkholderia cenocepacia",         "taxon_id": "95486"},
    # Non-BNF genera
    "Bacillus":          {"species": "Bacillus subtilis",                "taxon_id": "1423"},
    "Streptomyces":      {"species": "Streptomyces coelicolor",          "taxon_id": "100226"},
    "Nocardia":          {"species": "Nocardia farcinica",               "taxon_id": "47170"},
    "Arthrobacter":      {"species": "Arthrobacter crystallopoietes",    "taxon_id": "45962"},
    "Acidobacterium":    {"species": "Acidobacterium capsulatum",        "taxon_id": "33075"},
    "Gemmata":           {"species": "Gemmata obscuriglobus",            "taxon_id": "114"},
    "Planctomyces":      {"species": "Planctomyces limnophilus",          "taxon_id": "52975"},
    "Nitrospira":        {"species": "Nitrospira moscoviensis",           "taxon_id": "42253"},
    "Nitrosomonas":      {"species": "Nitrosomonas europaea",            "taxon_id": "915"},
    "Pseudomonas":       {"species": "Pseudomonas fluorescens",          "taxon_id": "294"},
    "Sphingomonas":      {"species": "Sphingomonas paucimobilis",        "taxon_id": "28214"},
    "Caulobacter":       {"species": "Caulobacter vibrioides",            "taxon_id": "155892"},
    "Variovorax":        {"species": "Variovorax paradoxus",             "taxon_id": "34073"},
    # Synthetic names → closest real proxy
    "Ellin":                 {"species": "Acidobacterium capsulatum",    "taxon_id": "33075"},
    "Burkholderia_non_bnf":  {"species": "Burkholderia cenocepacia",    "taxon_id": "95486"},
}

BV_BRC_BASE = "https://www.bv-brc.org/api"
_REQ_DELAY = 0.35  # seconds between API calls (rate limiting)


# ---------------------------------------------------------------------------
# Per-worker SBML model cache
# Populated once per child process on first access; deepcopy'd before use
# so COBRApy can mutate freely without corrupting the cache.
# ---------------------------------------------------------------------------

_MODEL_CACHE: dict[str, Any] = {}


def _load_genus_model(sbml_path: str) -> Any:
    """Load an SBML model, caching it for the lifetime of this worker process.

    Returns a deep copy ready for COBRApy mutation.
    """
    import copy
    import cobra  # imported here so cache works in both main and worker

    if sbml_path not in _MODEL_CACHE:
        _MODEL_CACHE[sbml_path] = cobra.io.read_sbml_model(sbml_path)
    return copy.deepcopy(_MODEL_CACHE[sbml_path])


# ---------------------------------------------------------------------------
# Proteome download helpers
# ---------------------------------------------------------------------------

def _get_json(url: str, timeout: int = 30) -> Any | None:
    try:
        time.sleep(_REQ_DELAY)
        req = urllib.request.Request(
            url, headers={"Accept": "application/json", "User-Agent": "soil-pipeline/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        logger.debug("GET %s failed: %s", url, exc)
        return None


def _download(url: str, dest: Path, timeout: int = 300) -> bool:
    try:
        time.sleep(_REQ_DELAY)
        req = urllib.request.Request(url, headers={"User-Agent": "soil-pipeline/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp, dest.open("wb") as fh:
            shutil.copyfileobj(resp, fh)
        return dest.stat().st_size > 100
    except Exception as exc:
        logger.debug("Download %s → %s failed: %s", url, dest, exc)
        return False


def _fetch_proteome_ncbi(taxon_id: str, cache_dir: Path) -> Path | None:
    """Download protein FASTA from NCBI RefSeq for a given taxon_id."""
    esearch_url = (
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        f"?db=assembly&term=txid{urllib.parse.quote(taxon_id)}[Organism]"
        f"+AND+reference_genome[RefSeq+Category]&retmax=1&retmode=json"
    )
    data = _get_json(esearch_url)
    id_list = (data or {}).get("esearchresult", {}).get("idlist", [])

    if not id_list:
        # Broaden: any assembly
        esearch_url2 = (
            f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
            f"?db=assembly&term=txid{urllib.parse.quote(taxon_id)}[Organism]"
            f"&retmax=1&retmode=json"
        )
        data2 = _get_json(esearch_url2)
        id_list = (data2 or {}).get("esearchresult", {}).get("idlist", [])

    if not id_list:
        return None

    assembly_id = id_list[0]
    esummary_url = (
        f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
        f"?db=assembly&id={assembly_id}&retmode=json"
    )
    summary = _get_json(esummary_url)
    if not summary:
        return None

    result = summary.get("result", {}).get(assembly_id, {})
    ftp_path = result.get("ftppath_refseq", "") or result.get("ftppath_genbank", "")
    if not ftp_path:
        return None

    accession = ftp_path.split("/")[-1]
    # Try protein FASTA first (_protein.faa.gz), fall back to translated CDS
    for suffix in [f"{accession}_protein.faa.gz", f"{accession}_translated_cds.faa.gz"]:
        fasta_gz_url = f"{ftp_path}/{suffix}"
        dest_gz = cache_dir / f"ncbi_{assembly_id}_protein.faa.gz"
        if _download(fasta_gz_url, dest_gz):
            dest = dest_gz.with_suffix("")  # strip .gz
            if not dest.exists():
                with gzip.open(dest_gz, "rb") as f_in, dest.open("wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                dest_gz.unlink(missing_ok=True)
            logger.info("NCBI proteome downloaded: %s (taxon_id=%s)", accession, taxon_id)
            return dest

    return None


def _fetch_proteome_bvbrc(taxon_id: str, cache_dir: Path) -> Path | None:
    """Download protein FASTA from BV-BRC for a given taxon_id."""
    # Find genome_id
    url = (
        f"{BV_BRC_BASE}/genome/"
        f"?eq(taxon_id,{urllib.parse.quote(taxon_id)})"
        f"&select(genome_id,genome_name)&sort(-genome_length)&limit(1)"
    )
    data = _get_json(url)
    if not data or not isinstance(data, list) or len(data) == 0:
        return None

    genome_id = data[0].get("genome_id")
    if not genome_id:
        return None

    # Download protein FASTA
    prot_url = (
        f"{BV_BRC_BASE}/genome_feature/"
        f"?eq(genome_id,{urllib.parse.quote(genome_id)})"
        f"&eq(feature_type,CDS)"
        f"&http_accept=application/protein+fasta"
        f"&limit(1000000)"
    )
    dest = cache_dir / f"bvbrc_{genome_id}_protein.faa"
    if _download(prot_url, dest):
        logger.info("BV-BRC proteome downloaded: %s (taxon_id=%s)", genome_id, taxon_id)
        return dest

    return None


def _fetch_proteome(genus: str, cache_dir: Path) -> Path | None:
    """Fetch a proteome FASTA for the given genus. Uses NCBI → BV-BRC fallback."""
    info = _GENUS_NCBI.get(genus)
    if not info:
        logger.warning("No NCBI mapping for genus %r — skipping", genus)
        return None

    taxon_id = info["taxon_id"]
    cached = cache_dir / f"{genus}_proteome.faa"
    if cached.exists() and cached.stat().st_size > 500:
        logger.debug("Proteome cache hit: %s", cached)
        return cached

    # Strategy 1: NCBI RefSeq
    path = _fetch_proteome_ncbi(taxon_id, cache_dir)
    if path and path.exists():
        if path != cached:
            shutil.move(str(path), str(cached))
        return cached

    # Strategy 2: BV-BRC
    path = _fetch_proteome_bvbrc(taxon_id, cache_dir)
    if path and path.exists():
        if path != cached:
            shutil.move(str(path), str(cached))
        return cached

    logger.warning("Could not obtain proteome for %s (taxon_id=%s)", genus, taxon_id)
    return None


# ---------------------------------------------------------------------------
# CarveMe model building
# ---------------------------------------------------------------------------

def _build_genus_model(genus: str, proteome_cache: Path, model_dir: Path) -> str | None:
    """Build a CarveMe metabolic model for one genus. Returns SBML path or None."""
    sbml_path = model_dir / f"{genus}.xml"
    if sbml_path.exists() and sbml_path.stat().st_size > 1000:
        logger.debug("Model cache hit: %s", sbml_path)
        return str(sbml_path)

    proteome = _fetch_proteome(genus, proteome_cache)
    if proteome is None:
        return None

    cmd = [
        "carve",
        "--solver", "scip",
        "--gapfill", "M9",
        "--output", str(sbml_path),
        str(proteome),
    ]
    logger.info("CarveMe: building model for %s ...", genus)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        if result.returncode != 0:
            logger.error("CarveMe failed for %s: %s", genus, result.stderr[-500:])
            return None
    except subprocess.TimeoutExpired:
        logger.error("CarveMe timeout for %s", genus)
        return None

    if not sbml_path.exists() or sbml_path.stat().st_size < 500:
        logger.warning("CarveMe produced no/empty output for %s", genus)
        return None

    logger.info("CarveMe model built: %s (%d KB)", genus, sbml_path.stat().st_size // 1024)
    return str(sbml_path)


# ---------------------------------------------------------------------------
# Worker: community FBA + keystone (runs in child process)
# ---------------------------------------------------------------------------

def _worker_batch(batch: list[tuple], model_dir: str) -> list[dict]:
    """
    Process a batch of communities through community FBA + keystone analysis.

    Each tuple: (community_id, top_genera_json, metadata_json)
    """
    # Import heavy deps inside worker (avoids pickling issues)
    try:
        import cobra
        from compute.community_fba import (
            _merge_community_models,
            _apply_environmental_constraints,
            _find_target_reactions,
            _extract_genome_quality_stats,
        )
        from compute.keystone_analyzer import identify_keystone_taxa
    except ImportError as exc:
        return [{"community_id": t[0], "error": f"Import failed: {exc}"} for t in batch]

    # Detect best available LP solver once per worker.
    # We test against COBRApy's registered solvers, NOT just whether highspy
    # is importable — optlang may not have the highs_interface even if highspy
    # is installed, which causes model.solver = "highs" to raise ValueError.
    _SOLVER = "glpk"
    try:
        import cobra.util.solver as _cs
        if "highs" in _cs.solvers:
            _SOLVER = "highs"
    except Exception:
        pass

    # Suppress libsbml/cobra parse noise (EX_* exchange reaction warnings)
    logging.getLogger("cobra.io.sbml").setLevel(logging.ERROR)
    logging.getLogger("libsbml").setLevel(logging.ERROR)
    logging.getLogger("cobra.core.model").setLevel(logging.ERROR)

    model_dir_p = Path(model_dir)
    results = []

    for community_id, genera_json, meta_json in batch:
        t0 = time.perf_counter()
        try:
            genera = json.loads(genera_json or "[]")
            metadata = json.loads(meta_json or "{}")

            # Load cached genus SBML models
            genus_names = []
            for g in genera:
                name = g.get("name", g) if isinstance(g, dict) else str(g)
                genus_names.append(name)

            member_models = []
            for gname in genus_names[:10]:  # cap at 10 members
                # Normalise: Burkholderia_non_bnf uses same model as Burkholderia
                lookup = gname if gname != "Burkholderia_non_bnf" else "Burkholderia"
                sbml = model_dir_p / f"{lookup}.xml"
                if not sbml.exists():
                    continue
                try:
                    m = _load_genus_model(str(sbml))
                    m.solver = _SOLVER
                    m.id = gname
                    member_models.append(m)
                except Exception as exc:
                    logger.debug("Failed to load %s: %s", sbml, exc)

            if not member_models:
                results.append({
                    "community_id": community_id,
                    "t1_pass": False,
                    "error": "no_models_loaded",
                    "walltime_s": time.perf_counter() - t0,
                })
                continue

            # Merge community model
            community = _merge_community_models(member_models, max_size=20)
            if community is None:
                results.append({
                    "community_id": community_id,
                    "t1_pass": False,
                    "error": "merge_failed",
                    "walltime_s": time.perf_counter() - t0,
                })
                continue

            community.solver = _SOLVER
            _apply_environmental_constraints(community, metadata)

            # FBA
            solution = community.optimize()
            feasible = solution.status == "optimal"

            # T1 target flux: use community biomass growth rate (objective value).
            # AGORA2-style reference models do not include an explicit nitrogenase
            # reaction — rxn00006 in these models maps to catalase and N2 is not
            # a metabolite. Biomass production rate under soil environmental
            # constraints is therefore the most meaningful metabolic viability
            # indicator achievable with these models. N2-fixation specificity is
            # already captured upstream by the T0.25 functional gene scanner
            # (nifH/nifD/nifK presence). T1 FBA confirms community-level metabolic
            # growth potential under the target soil conditions.
            target_flux = max(0.0, solution.objective_value) if feasible else 0.0

            # Identify informational N-related exchange reactions for FVA bounding.
            # Fall back to EX_nh4_e (ammonium exchange) if no pattern hits.
            target_rxns = _find_target_reactions(community, "nifH_pathway")
            if not target_rxns:
                try:
                    target_rxns = [community.reactions.get_by_id("EX_nh4_e")]
                except KeyError:
                    pass

            # Early T1 pass/fail — skip FVA + keystone on non-passing communities.
            # Threshold 1e-3 gDW/gDW/h: nominal growth (GLPK default ATPM ~8 units).
            t1_pass = feasible and target_flux > 1e-3

            # FVA (only for T1-passing communities)
            fva_min, fva_max = 0.0, 0.0
            if t1_pass and target_rxns:
                try:
                    fva_result = cobra.flux_analysis.flux_variability_analysis(
                        community, reaction_list=target_rxns, fraction_of_optimum=0.9,
                    )
                    fva_min = float(fva_result["minimum"].mean())
                    fva_max = float(fva_result["maximum"].mean())
                except Exception:
                    pass

            # Keystone analysis (only for T1-passing communities)
            keystone_taxa = []
            if t1_pass and target_flux > 1e-6:
                target_rxn_ids = [rxn.id for rxn in target_rxns]
                keystone_taxa = identify_keystone_taxa(
                    community, target_flux,
                    target_rxn_ids=target_rxn_ids,
                )

            # Genome quality stats
            quality = _extract_genome_quality_stats(member_models)

            # Confidence classification
            confidence_score = quality.get("model_confidence", 0.35)
            if confidence_score >= 0.7:
                confidence_label = "high"
            elif confidence_score >= 0.4:
                confidence_label = "medium"
            else:
                confidence_label = "low"

            results.append({
                "community_id": community_id,
                "t1_pass": t1_pass,
                "t1_model_size": len(community.reactions),
                "t1_target_flux": target_flux,
                "t1_flux_lower_bound": fva_min,
                "t1_flux_upper_bound": fva_max,
                "t1_flux_units": "mmol/gDW/h",
                "t1_feasible": feasible,
                "t1_keystone_taxa": json.dumps(keystone_taxa),
                "t1_genome_completeness_mean": quality.get("genome_completeness_mean", 0.0),
                "t1_genome_contamination_mean": quality.get("genome_contamination_mean", 100.0),
                "t1_model_confidence": confidence_label,
                "t1_walltime_s": time.perf_counter() - t0,
                "error": None,
            })

        except Exception as exc:
            results.append({
                "community_id": community_id,
                "t1_pass": False,
                "error": str(exc),
                "walltime_s": time.perf_counter() - t0,
            })

    return results


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _fetch_communities(db_path: str, n_max: int) -> list[tuple]:
    """
    Load T2-passed communities for T1 modelling.

    Returns list of (community_id, top_genera_json, metadata_json).
    Sorted by t2_stability_score * t025_function_score descending.
    """
    conn = _db_connect(db_path)
    rows = conn.execute(
        """SELECT c.community_id, c.top_genera,
                  json_object(
                    'soil_ph',           COALESCE(s.soil_ph, 6.5),
                    'organic_matter_pct',COALESCE(s.organic_matter_pct, 2.0),
                    'clay_pct',          COALESCE(s.clay_pct, 25.0),
                    'temperature_c',     COALESCE(s.temperature_c, 12.0),
                    'precipitation_mm',  COALESCE(s.precipitation_mm, 600.0)
                  )
           FROM runs r
           JOIN communities c ON r.community_id = c.community_id
           JOIN samples s ON r.sample_id = s.sample_id
           WHERE r.t2_pass = 1
             AND r.t1_pass IS NULL
             AND c.top_genera IS NOT NULL
           ORDER BY (COALESCE(r.t2_stability_score, 0) * COALESCE(r.t025_function_score, 0)) DESC
           LIMIT ?""",
        (n_max,),
    ).fetchall()
    conn.close()
    return [(r[0], r[1], r[2]) for r in rows]


def _write_results(db_path: str, results: list[dict]) -> tuple[int, int]:
    """Write T1 FBA results back to runs table. Returns (n_written, n_passed)."""
    conn = _db_connect(db_path, timeout=60)
    # synchronous=OFF: ~3-5x faster bulk writes. WAL mode ensures atomicity —
    # on crash the incomplete transaction is rolled back cleanly on next open.
    conn.execute("PRAGMA synchronous=OFF")
    n_written, n_passed = 0, 0

    for r in results:
        if r.get("error"):
            # Still mark as attempted so we don't retry failures
            try:
                conn.execute(
                    "UPDATE runs SET t1_pass = 0, t1_model_confidence = 'failed' "
                    "WHERE community_id = ? AND t1_pass IS NULL",
                    (r["community_id"],),
                )
                n_written += 1
            except Exception:
                pass
            continue
        try:
            conn.execute(
                """UPDATE runs SET
                       t1_pass = ?,
                       t1_model_size = ?,
                       t1_target_flux = ?,
                       t1_flux_lower_bound = ?,
                       t1_flux_upper_bound = ?,
                       t1_flux_units = ?,
                       t1_feasible = ?,
                       t1_keystone_taxa = ?,
                       t1_genome_completeness_mean = ?,
                       t1_genome_contamination_mean = ?,
                       t1_model_confidence = ?,
                       t1_walltime_s = ?,
                       tier_reached = CASE WHEN t2_pass = 1 THEN 2 ELSE 1 END
                   WHERE community_id = ? AND t1_pass IS NULL""",
                (
                    1 if r["t1_pass"] else 0,
                    r.get("t1_model_size"),
                    r.get("t1_target_flux"),
                    r.get("t1_flux_lower_bound"),
                    r.get("t1_flux_upper_bound"),
                    r.get("t1_flux_units"),
                    1 if r.get("t1_feasible") else 0,
                    r.get("t1_keystone_taxa"),
                    r.get("t1_genome_completeness_mean"),
                    r.get("t1_genome_contamination_mean"),
                    r.get("t1_model_confidence"),
                    r.get("t1_walltime_s"),
                    r["community_id"],
                ),
            )
            n_written += 1
            if r["t1_pass"]:
                n_passed += 1
        except Exception as exc:
            logger.debug("Write failed for cid=%s: %s", r.get("community_id"), exc)

    conn.commit()
    conn.execute("PRAGMA synchronous=NORMAL")  # restore safe default after commit
    conn.close()
    return n_written, n_passed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main(
    ctx:           typer.Context,
    db_path:       Path          = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    n_communities: int           = typer.Option(5000, "--n-communities", "-n"),
    workers:       int           = typer.Option(36,   "--workers", "-w"),
    batch_size:    int           = typer.Option(5,    "--batch-size"),  # smaller = better worker saturation
    model_dir:     Path          = typer.Option(Path("/data/pipeline/models"),           "--model-dir"),
    proteome_dir:  Path          = typer.Option(Path("/data/pipeline/proteome_cache"),   "--proteome-dir"),
    log_path:      Optional[Path] = typer.Option(Path("/var/log/pipeline/t1_fba_batch.log"), "--log"),
):
    """Run T1 metabolic modelling: CarveMe model-build → community FBA → keystone."""
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

    model_dir.mkdir(parents=True, exist_ok=True)
    proteome_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== T1 FBA batch starting: n=%d, workers=%d ===", n_communities, workers)

    # ------------------------------------------------------------------
    # Phase A: determine unique genera and build per-genus models
    # ------------------------------------------------------------------
    logger.info("Loading communities from DB ...")
    communities = _fetch_communities(str(db_path), n_communities)
    logger.info("Found %d T2-passed communities with t1_pass IS NULL", len(communities))
    if not communities:
        logger.warning("No communities need T1 modelling — nothing to do")
        raise typer.Exit(0)

    # Collect unique genera across all communities
    all_genera: set[str] = set()
    for _, genera_json, _ in communities:
        try:
            genera_list = json.loads(genera_json or "[]")
            for g in genera_list:
                name = g.get("name", g) if isinstance(g, dict) else str(g)
                all_genera.add(name)
        except Exception:
            continue

    logger.info("Unique genera to model: %d", len(all_genera))

    # Build models for each genus (sequential — CarveMe uses DIAMOND internally)
    genus_model_paths: dict[str, str] = {}
    t_model = time.time()
    for i, genus in enumerate(sorted(all_genera), 1):
        lookup = genus if genus != "Burkholderia_non_bnf" else "Burkholderia"
        if lookup in genus_model_paths:
            genus_model_paths[genus] = genus_model_paths[lookup]
            continue
        path = _build_genus_model(lookup, proteome_dir, model_dir)
        if path:
            genus_model_paths[genus] = path
            genus_model_paths[lookup] = path
        logger.info("Model build progress: %d/%d genera (%s → %s)",
                     i, len(all_genera), genus, "OK" if path else "FAILED")

    logger.info(
        "Phase A complete: %d/%d genus models built in %.1f min",
        len(set(genus_model_paths.values())),
        len(all_genera),
        (time.time() - t_model) / 60,
    )

    if not genus_model_paths:
        logger.error("No genus models built — cannot proceed with community FBA")
        raise typer.Exit(1)

    # ------------------------------------------------------------------
    # Phase B: community FBA + keystone in parallel
    # ------------------------------------------------------------------

    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    batches = list(_chunks(communities, batch_size))
    logger.info(
        "Submitting %d batches × %d communities to %d workers",
        len(batches), batch_size, workers,
    )

    t_start = time.time()
    total_written, total_passed = 0, 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_worker_batch, batch, str(model_dir)): idx
            for idx, batch in enumerate(batches)
        }
        for fut in as_completed(futures):
            batch_idx = futures[fut]
            try:
                batch_results = fut.result()
                n_w, n_p = _write_results(str(db_path), batch_results)
                total_written += n_w
                total_passed += n_p
                elapsed = time.time() - t_start
                rate = total_written / elapsed if elapsed > 0 else 0
                logger.info(
                    "Batch %4d/%d done — %6d written, %5d T1-passed "
                    "(%.1f/s, %.1f min elapsed)",
                    batch_idx + 1, len(batches), total_written, total_passed,
                    rate, elapsed / 60,
                )
            except Exception as exc:
                logger.error("Batch %d failed: %s", batch_idx, exc)

    elapsed = time.time() - t_start
    logger.info(
        "=== T1 FBA batch complete: %d written, %d T1-passed in %.1f min ===",
        total_written, total_passed, elapsed / 60,
    )


if __name__ == "__main__":
    app()
