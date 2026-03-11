"""
scripts/t2_dfba_batch.py — Phase 15: T2 dynamic FBA batch.

Runs dFBA (dynamic flux balance analysis) over a simulated 45-day growing
season for every T1-pass community.  Derives per-community:
  • stability_score     — how consistently the target pathway operates (1 - CV)
  • resistance          — flux retention at peak perturbation
  • resilience          — recovery speed after perturbation
  • functional_redundancy — fraction of community genera with on-disk models
  • establishment_prob  — probability a community sustains function in the field
  • best_intervention   — perturbation type that most improved flux, or amendment
  • intervention_effect — effect size (Δflux / baseline)
  • t2_bnf_trajectory   — JSON time-series of target flux (for plotting)
  • t2_interventions    — JSON list of all perturbation responses

Parallelism: ProcessPoolExecutor (same design as t1_fba_batch.py).
Solver:      glpk forced for all inner FBA steps — see solver safety note in
             t1_fba_batch.py.  OSQP/hybrid must NOT be used here because each
             dFBA step calls optimize() on a model whose bounds may transiently
             make reaction ranges very wide.

Usage:
  python scripts/t2_dfba_batch.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --model-dir /data/pipeline/models \\
      --workers 36 \\
      --days 45 \\
      --batch-size 64
"""
from __future__ import annotations

import copy
import json
import logging
import math
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import typer

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect  # noqa: E402

logger = logging.getLogger(__name__)
app = typer.Typer(
    help="T2 dynamic FBA batch: dFBA + stability + intervention analysis",
    add_completion=False,
    invoke_without_command=True,
)

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

_T2_STABILITY_PASS_THRESHOLD: float = 0.30   # stability_score >= this to pass
_T2_CONFIDENCE_HIGH_THRESHOLD: float = 0.70  # stability >= this → "high"
_T2_CONFIDENCE_MED_THRESHOLD: float = 0.45   # stability >= this → "medium"

# Perturbation schedule applied to every community (relative to metadata):
#   • drought at day 15 (severity derived from inverted moisture_pct)
#   • fertilizer_pulse at day 20 (tests N-fixation community response)
#   • temperature_shock at day 35 (severity 0.2 — mild universal stress)
_DEFAULT_PERTURBATIONS = [
    {"type": "drought",            "day": 15, "severity": 0.5},
    {"type": "fertilizer_pulse",   "day": 20, "severity": 0.4},
    {"type": "temperature_shock",  "day": 35, "severity": 0.2},
]


# ---------------------------------------------------------------------------
# Per-worker SBML model cache (same strategy as t1_fba_batch.py)
# ---------------------------------------------------------------------------

_MODEL_CACHE: dict[str, Any] = {}


def _load_model_cached(genus: str, model_dir: Path) -> Any | None:
    """Load and cache an SBML model by genus name.  Returns None if not found."""
    if genus in _MODEL_CACHE:
        return _MODEL_CACHE[genus]

    try:
        import cobra
    except ImportError:
        return None

    for suffix in (genus, genus.lower(), genus.capitalize()):
        sbml_path = model_dir / f"{suffix}.xml"
        if sbml_path.exists():
            try:
                m = cobra.io.read_sbml_model(str(sbml_path))
                m.solver = "glpk"  # SAFETY: glpk only — see module docstring
                _MODEL_CACHE[genus] = m
                return m
            except Exception as exc:
                logger.debug("Failed to load %s: %s", sbml_path, exc)
                return None
    return None


# ---------------------------------------------------------------------------
# Helpers for deriving perturbations and metrics from metadata
# ---------------------------------------------------------------------------

def _build_perturbations(meta: dict) -> list[dict]:
    """Customise perturbation severity from site metadata where available."""
    perts = copy.deepcopy(_DEFAULT_PERTURBATIONS)

    moisture = meta.get("moisture_pct")
    if moisture is not None:
        # Dry sites (low moisture) get a stronger drought
        drought_severity = max(0.2, min(0.9, 1.0 - float(moisture)))
        perts[0]["severity"] = round(drought_severity, 2)

    temp = meta.get("temperature_c")
    if temp is not None:
        # Hot sites get a slightly stronger thermal shock
        temp_severity = max(0.1, min(0.5, float(temp) / 50.0))
        perts[2]["severity"] = round(temp_severity, 2)

    return perts


def _compute_resistance(trajectory: list[float], perturbation_responses: list[dict]) -> float:
    """Ratio of minimum flux at perturbation events to the pre-perturbation mean.

    Returns 1.0 (perfect resistance) when there are no perturbation responses.
    """
    if not trajectory or not perturbation_responses:
        return 1.0
    # Use first quarter of trajectory as baseline
    baseline_window = trajectory[: max(1, len(trajectory) // 4)]
    baseline = sum(baseline_window) / len(baseline_window)
    if baseline < 1e-12:
        return 0.0
    perturb_fluxes = [r["target_flux"] for r in perturbation_responses]
    min_perturb = min(perturb_fluxes)
    return float(max(0.0, min(1.0, min_perturb / baseline)))


def _compute_resilience(trajectory: list[float], perturbation_responses: list[dict]) -> float:
    """Fraction of post-perturbation time steps where flux is back to ≥90% of pre-perturbation mean.

    Returns 1.0 when no perturbations occurred.
    """
    if not trajectory or not perturbation_responses:
        return 1.0
    # Identify the last perturbation step index (approx from day / total time)
    n = len(trajectory)
    total_days = 45.0
    first_perturb_day = min(r["day"] for r in perturbation_responses)
    split = int(first_perturb_day / total_days * n)

    pre = trajectory[:split] if split > 0 else trajectory[:1]
    post = trajectory[split:]
    if not pre or not post:
        return 1.0

    baseline = sum(pre) / len(pre)
    if baseline < 1e-12:
        return 0.0

    recovered = sum(1 for v in post if v >= 0.9 * baseline)
    return float(recovered / len(post))


def _compute_functional_redundancy(top_genera: dict, model_dir: Path) -> float:
    """Fraction of community genera that have an on-disk SBML model."""
    if not top_genera:
        return 0.0
    on_disk = sum(
        1 for g in top_genera
        if (model_dir / f"{g}.xml").exists() or (model_dir / f"{g.lower()}.xml").exists()
    )
    return round(on_disk / len(top_genera), 4)


def _recommend_intervention(
    meta: dict,
    stability: float,
    resistance: float,
    resilience: float,
    func_redundancy: float,
) -> tuple[str, float]:
    """Recommend a field intervention based on site metadata and dFBA metrics.

    Previous approach (pick perturbation type with highest flux response) was
    non-discriminating: drought universally "won" because reducing O2 import
    marginally relieves nitrogenase O2-inhibition, giving every community the
    same recommendation regardless of site conditions.

    This approach instead maps site constraints to actionable levers:
      pH suboptimal (< 5.5 or > 8.0)  → pH-amendment (lime/sulfur)
      Very dry site (moisture < 0.05)  → drought-tolerant-inoculant
      Poor recovery (resilience < 0.7) → moisture-management
      Low model coverage (redundancy < 0.2) → diversity-enhancement
      Robust community (stab ≥ 0.95, resist ≥ 0.9) → direct-inoculant
      Moderate community                → consortium-optimization

    intervention_effect = expected gain in establishment_prob from the
    recommended action; approximated as (1 - current_establishment_headroom).
    """
    ph = meta.get("soil_ph")
    moisture = meta.get("moisture_pct")

    intervention = "consortium-optimization"  # default

    if ph is not None and (float(ph) < 5.5 or float(ph) > 8.0):
        intervention = "pH-amendment"
    elif moisture is not None and float(moisture) < 0.05:
        intervention = "drought-tolerant-inoculant"
    elif resilience < 0.70:
        intervention = "moisture-management"
    elif func_redundancy < 0.20:
        intervention = "diversity-enhancement"
    elif stability >= 0.95 and resistance >= 0.90:
        intervention = "direct-inoculant"

    # Effect size: approximate marginal improvement from intervention.
    # For pH/moisture issues, assume fix restores ~25% of establishment headroom.
    # For robust communities, direct inoculation has ~10% expected yield gain.
    headroom = max(0.0, 1.0 - (stability * 0.5 + resistance * 0.3 + resilience * 0.2))
    if intervention in ("pH-amendment", "drought-tolerant-inoculant", "moisture-management"):
        effect = round(min(headroom * 0.25 + 0.05, 0.40), 4)
    elif intervention == "direct-inoculant":
        effect = round(0.08 + stability * 0.05, 4)
    elif intervention == "diversity-enhancement":
        effect = round(min(headroom * 0.15 + 0.03, 0.25), 4)
    else:
        effect = round(min(headroom * 0.10 + 0.02, 0.20), 4)

    return intervention, effect


def _compute_establishment_prob(
    stability_score: float,
    resistance: float,
    resilience: float,
    meta: dict,
) -> float:
    """Estimate field establishment probability.

    Sigmoid combining stability, resistance, resilience, and two key soil
    covariates (pH optimum 6.0–7.5 for most diazotrophs; moisture ≥ 0.05).
    """
    # Base score from dFBA metrics
    base = (stability_score * 0.5 + resistance * 0.3 + resilience * 0.2)

    # pH bonus/penalty (optimal range 6.0–7.5)
    ph = meta.get("soil_ph")
    ph_factor = 1.0
    if ph is not None:
        ph = float(ph)
        if 6.0 <= ph <= 7.5:
            ph_factor = 1.1
        elif ph < 5.0 or ph > 8.5:
            ph_factor = 0.75

    # Moisture bonus (very dry sites are risky)
    moisture = meta.get("moisture_pct")
    moist_factor = 1.0
    if moisture is not None:
        moisture = float(moisture)
        if moisture < 0.05:
            moist_factor = 0.7
        elif moisture > 0.15:
            moist_factor = 1.05

    prob = base * ph_factor * moist_factor
    return round(float(max(0.0, min(1.0, prob))), 4)


def _confidence_label(stability_score: float, n_genera_modelled: int, n_genera_total: int) -> str:
    coverage = n_genera_modelled / max(n_genera_total, 1)
    if stability_score >= _T2_CONFIDENCE_HIGH_THRESHOLD and coverage >= 0.4:
        return "high"
    if stability_score >= _T2_CONFIDENCE_MED_THRESHOLD and coverage >= 0.2:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Worker function (runs in child process via ProcessPoolExecutor)
# ---------------------------------------------------------------------------

def _worker_batch(
    batch: list[tuple],
    model_dir: str,
    simulation_days: int,
    dt_hours: float,
) -> list[dict]:
    """Process a batch of (community_id, t1_flux, top_genera_json, meta_json) tuples.

    Returns list of result dicts (one per community) for DB writing.
    """
    try:
        import cobra
        from compute.dfba_runner import run_dfba
    except ImportError as exc:
        return [{"community_id": t[0], "error": f"Import failed: {exc}"} for t in batch]

    # Suppress parse noise from libsbml
    logging.getLogger("cobra.io.sbml").setLevel(logging.ERROR)
    logging.getLogger("libsbml").setLevel(logging.ERROR)
    logging.getLogger("cobra.core.model").setLevel(logging.ERROR)

    model_dir_p = Path(model_dir)
    results = []

    for community_id, t1_flux, top_genera_json, meta_json in batch:
        t_start = time.perf_counter()
        result: dict[str, Any] = {"community_id": community_id, "error": None}

        try:
            top_genera: dict = json.loads(top_genera_json) if top_genera_json else {}
            meta: dict = json.loads(meta_json) if meta_json else {}

            # -----------------------------------------------------------------
            # Build community model (same approach as T1: merge genus models)
            # -----------------------------------------------------------------
            member_models: list[Any] = []
            for genus in top_genera:
                m = _load_model_cached(genus, model_dir_p)
                if m is not None:
                    member_models.append(copy.deepcopy(m))

            if not member_models:
                result.update({
                    "t2_pass": False,
                    "t2_stability_score": 0.0,
                    "t2_confidence": "low",
                    "t2_walltime_s": time.perf_counter() - t_start,
                    "error": "no_on_disk_models",
                    "tier_reached": 1,
                })
                results.append(result)
                continue

            # Merge member models into a single community model
            # (simplified additive merge — same approach as run_community_fba)
            from compute.community_fba import _apply_bnf_minimal_medium  # type: ignore

            if len(member_models) == 1:
                community = member_models[0]
            else:
                try:
                    community = member_models[0].copy()
                    for other in member_models[1:]:
                        for rxn in other.reactions:
                            if rxn.id not in community.reactions:
                                try:
                                    community.add_reactions([rxn.copy()])
                                except Exception:
                                    pass
                except Exception as exc:
                    logger.debug("cid=%s merge error: %s", community_id, exc)
                    community = member_models[0]

            # Apply BNF minimal medium if appropriate (has_nifh already filtered by T1)
            try:
                _apply_bnf_minimal_medium(community)
            except Exception:
                pass

            # Force glpk — OSQP/hybrid must not be used for inner dFBA FBA steps
            community.solver = "glpk"

            # -----------------------------------------------------------------
            # Identify target reactions (nitrogenase / BNF pathway)
            # -----------------------------------------------------------------
            target_rxn_ids = [
                r.id for r in community.reactions
                if "nitrogenase" in r.id.lower() or "NITROGENASE" in r.id
            ]
            if not target_rxn_ids:
                target_rxn_ids = [
                    r.id for r in community.reactions
                    if "nifh" in r.id.lower() or "bnf" in r.name.lower()
                ]

            # -----------------------------------------------------------------
            # Build perturbation schedule from metadata
            # -----------------------------------------------------------------
            perturbations = _build_perturbations(meta)

            # -----------------------------------------------------------------
            # Run dFBA
            # -----------------------------------------------------------------
            dfba_result = run_dfba(
                community,
                metadata=meta,
                simulation_days=simulation_days,
                dt_hours=dt_hours,
                perturbations=perturbations,
                target_rxn_ids=target_rxn_ids or None,
            )

            trajectory = dfba_result.get("target_flux_trajectory", [])
            perturb_responses = dfba_result.get("perturbation_responses", [])
            stability_score = float(dfba_result.get("stability_score", 0.0))
            walltime_s = float(dfba_result.get("walltime_s", 0.0))

            # -----------------------------------------------------------------
            # Derived T2 metrics
            # -----------------------------------------------------------------
            baseline_flux = (
                sum(trajectory[: max(1, len(trajectory) // 4)]) /
                max(1, len(trajectory) // 4)
            ) if trajectory else float(t1_flux or 0.0)

            resistance = _compute_resistance(trajectory, perturb_responses)
            resilience = _compute_resilience(trajectory, perturb_responses)

            n_genera_total = len(top_genera)
            n_genera_modelled = len(member_models)
            func_redundancy = _compute_functional_redundancy(top_genera, model_dir_p)

            best_intervention, intervention_effect = _recommend_intervention(
                meta, stability_score, resistance, resilience, func_redundancy
            )
            establishment_prob = _compute_establishment_prob(
                stability_score, resistance, resilience, meta
            )
            confidence = _confidence_label(stability_score, n_genera_modelled, n_genera_total)
            t2_pass = stability_score >= _T2_STABILITY_PASS_THRESHOLD

            # Off-target impact: high functional redundancy = lower off-target risk
            off_target = "low" if func_redundancy >= 0.4 else ("medium" if func_redundancy >= 0.2 else "high")

            result.update({
                "t2_pass": t2_pass,
                "t2_stability_score": round(stability_score, 6),
                "t2_best_intervention": best_intervention,
                "t2_intervention_effect": round(intervention_effect, 6),
                "t2_establishment_prob": establishment_prob,
                "t2_off_target_impact": off_target,
                "t2_confidence": confidence,
                "t2_walltime_s": round(walltime_s, 3),
                "t2_resistance": round(resistance, 6),
                "t2_resilience": round(resilience, 6),
                "t2_functional_redundancy": round(func_redundancy, 6),
                "t2_interventions": json.dumps(perturb_responses),
                "t2_bnf_trajectory": json.dumps([round(v, 4) for v in trajectory]),
                "tier_reached": 2,
            })

        except Exception as exc:
            result.update({
                "t2_pass": False,
                "t2_stability_score": 0.0,
                "t2_confidence": "low",
                "t2_walltime_s": time.perf_counter() - t_start,
                "error": str(exc)[:200],
                "tier_reached": 1,
            })

        results.append(result)

    return results


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

_FETCH_SQL = """
    SELECT
        r.community_id,
        r.t1_target_flux,
        c.top_genera,
        json_object(
            'moisture_pct',   s.moisture_pct,
            'temperature_c',  s.temperature_c,
            'soil_ph',        s.soil_ph,
            'latitude',       s.latitude,
            'longitude',      s.longitude
        ) AS meta_json
    FROM runs r
    JOIN communities c ON c.community_id = r.community_id
    JOIN samples     s ON s.sample_id    = c.sample_id
    WHERE r.t1_pass = 1
      AND r.t2_pass IS NULL
    ORDER BY r.community_id
    LIMIT ?
"""

_UPDATE_SQL = """
    UPDATE runs SET
        t2_pass                  = :t2_pass,
        t2_stability_score       = :t2_stability_score,
        t2_best_intervention     = :t2_best_intervention,
        t2_intervention_effect   = :t2_intervention_effect,
        t2_establishment_prob    = :t2_establishment_prob,
        t2_off_target_impact     = :t2_off_target_impact,
        t2_confidence            = :t2_confidence,
        t2_walltime_s            = :t2_walltime_s,
        t2_resistance            = :t2_resistance,
        t2_resilience            = :t2_resilience,
        t2_functional_redundancy = :t2_functional_redundancy,
        t2_interventions         = :t2_interventions,
        t2_bnf_trajectory        = :t2_bnf_trajectory,
        tier_reached             = :tier_reached
    WHERE community_id = :community_id
"""


def _write_results(db_path: str, results: list[dict]) -> tuple[int, int]:
    """Upsert T2 result rows; return (n_written, n_errors)."""
    n_written = n_errors = 0
    con = sqlite3.connect(db_path, timeout=30)
    try:
        for r in results:
            if r.get("error") and "t2_pass" not in r:
                n_errors += 1
                continue
            try:
                row = {
                    "community_id":            r["community_id"],
                    "t2_pass":                 int(bool(r.get("t2_pass", False))),
                    "t2_stability_score":      r.get("t2_stability_score", 0.0),
                    "t2_best_intervention":    r.get("t2_best_intervention", "none"),
                    "t2_intervention_effect":  r.get("t2_intervention_effect", 0.0),
                    "t2_establishment_prob":   r.get("t2_establishment_prob", 0.0),
                    "t2_off_target_impact":    r.get("t2_off_target_impact", "unknown"),
                    "t2_confidence":           r.get("t2_confidence", "low"),
                    "t2_walltime_s":           r.get("t2_walltime_s", 0.0),
                    "t2_resistance":           r.get("t2_resistance", 0.0),
                    "t2_resilience":           r.get("t2_resilience", 0.0),
                    "t2_functional_redundancy":r.get("t2_functional_redundancy", 0.0),
                    "t2_interventions":        r.get("t2_interventions", "[]"),
                    "t2_bnf_trajectory":       r.get("t2_bnf_trajectory", "[]"),
                    "tier_reached":            r.get("tier_reached", 1),
                }
                con.execute(_UPDATE_SQL, row)
                n_written += 1
            except Exception as exc:
                logger.warning("DB write error cid=%s: %s", r.get("community_id"), exc)
                n_errors += 1
        con.commit()
    finally:
        con.close()
    return n_written, n_errors


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

@app.callback()
def main(
    db: str = typer.Option(..., help="Path to soil_microbiome.db"),
    model_dir: str = typer.Option("/data/pipeline/models", help="Directory with SBML model files"),
    workers: int = typer.Option(36, help="Parallel worker processes"),
    days: int = typer.Option(45, help="dFBA simulation length in days"),
    dt_hours: float = typer.Option(6.0, help="dFBA time step in hours"),
    batch_size: int = typer.Option(64, help="Communities per worker batch"),
    n_communities: int = typer.Option(0, help="Limit total communities (0 = all)"),
    log_level: str = typer.Option("INFO", help="Logging level"),
) -> None:
    """Run T2 dFBA on all T1-pass communities that have not yet been processed."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # -----------------------------------------------------------------
    # Fetch work queue
    # -----------------------------------------------------------------
    limit = n_communities if n_communities > 0 else 999_999_999
    con = sqlite3.connect(db, timeout=30)
    rows = con.execute(_FETCH_SQL, (limit,)).fetchall()
    con.close()

    total = len(rows)
    if total == 0:
        logger.info("No T1-pass communities pending T2. Done.")
        raise typer.Exit()

    logger.info("T2 dFBA: %d communities to process | workers=%d days=%d batch_size=%d",
                total, workers, days, batch_size)

    # -----------------------------------------------------------------
    # Split into batches
    # -----------------------------------------------------------------
    batches = [rows[i: i + batch_size] for i in range(0, total, batch_size)]
    logger.info("%d batches queued", len(batches))

    t_wall_start = time.perf_counter()
    n_written_total = 0
    n_errors_total = 0
    n_pass = 0
    n_completed_batches = 0

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_worker_batch, batch, model_dir, days, dt_hours): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                batch_results = future.result()
            except Exception as exc:
                logger.error("Batch %d raised: %s", batch_idx, exc)
                n_errors_total += len(batches[batch_idx])
                n_completed_batches += 1
                continue

            n_pass_batch = sum(1 for r in batch_results if r.get("t2_pass"))
            n_err_batch = sum(1 for r in batch_results if r.get("error") and "t2_pass" not in r)

            n_written, n_db_err = _write_results(db, batch_results)
            n_written_total += n_written
            n_errors_total += n_err_batch + n_db_err
            n_pass += n_pass_batch
            n_completed_batches += 1

            elapsed = time.perf_counter() - t_wall_start
            rate = n_written_total / elapsed if elapsed > 0 else 0
            eta_s = (total - n_written_total) / rate if rate > 0 else 0
            logger.info(
                "Batch %4d/%d done | written=%d pass=%d errors=%d | "
                "%.1f comm/s | ETA %.0fm",
                n_completed_batches, len(batches),
                n_written_total, n_pass, n_errors_total,
                rate, eta_s / 60,
            )

    # -----------------------------------------------------------------
    # Final summary
    # -----------------------------------------------------------------
    elapsed_total = time.perf_counter() - t_wall_start
    logger.info(
        "T2 dFBA complete. total=%d written=%d t2_pass=%d errors=%d wall=%.1fm",
        total, n_written_total, n_pass, n_errors_total, elapsed_total / 60,
    )

    # Quick DB summary
    con = sqlite3.connect(db, timeout=30)
    summary = con.execute(
        "SELECT COUNT(*), AVG(t2_stability_score), AVG(t2_establishment_prob) "
        "FROM runs WHERE t2_pass=1"
    ).fetchone()
    con.close()
    logger.info(
        "DB: t2_pass=%s  avg_stability=%.3f  avg_establishment_prob=%.3f",
        summary[0], summary[1] or 0.0, summary[2] or 0.0,
    )


if __name__ == "__main__":
    app()
