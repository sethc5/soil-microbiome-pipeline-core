"""
rank_candidates.py — Score T1/T2 communities and rank intervention strategies.

Reads run results from the database and produces a ranked list of communities
and associated interventions ordered by composite score:
  target_flux × stability × establishment_probability

Usage:
  python rank_candidates.py --config config.yaml --db nitrogen_landscape.db --top 50
"""

from __future__ import annotations
import csv
import json
import logging
import math
import random
import statistics
from copy import deepcopy
from pathlib import Path
from typing import Any

import typer

from core.db_utils import SoilDB
from sim_model.adapter import simulate_from_pipeline_record

app = typer.Typer()
logger = logging.getLogger(__name__)


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _legacy_composite_score(row: dict[str, Any]) -> float:
    """Compute composite ranking score from a runs-table row dict.

    Score = normalised_flux * stability * confidence
    All factors clamped to [0, 1]; undefined values treated as 0.5 (neutral).
    """
    # Target flux — log-normalise to [0,1] using soft cap of 1000 mmol/gDW/h
    flux = float(row.get("t1_target_flux") or 0.0)
    flux_score = min(1.0, math.log1p(max(flux, 0)) / math.log1p(1000.0))

    # Stability score from T2
    stability = float(row.get("t2_stability_score") or 0.5)
    stability = max(0.0, min(1.0, stability))

    # Model confidence: high=0.90, medium=0.65, low=0.35, numeric passthrough
    conf_raw = row.get("t1_model_confidence", "medium")
    if isinstance(conf_raw, str):
        conf = {"high": 0.90, "medium": 0.65, "low": 0.35}.get(conf_raw.lower(), 0.5)
    else:
        conf = max(0.0, min(1.0, float(conf_raw or 0.5)))

    return flux_score * stability * conf


def _sim_composite_score_from_result(sim_result: dict[str, Any]) -> float:
    flux = float(sim_result.get("target_flux") or 0.0)
    flux_score = min(1.0, math.log1p(max(flux, 0.0)) / math.log1p(100.0))
    stability = max(0.0, min(1.0, float(sim_result.get("stability_score") or 0.0)))
    establishment = max(0.0, min(1.0, float(sim_result.get("establishment_probability") or 0.0)))
    return flux_score * stability * establishment


def _extract_top_intervention_candidate(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        # v3 schema may store a plain-text best intervention label.
        return {"intervention_detail": raw.strip()}

    if isinstance(parsed, list) and parsed:
        first = parsed[0]
        if isinstance(first, dict):
            return first
    if isinstance(parsed, dict):
        return parsed
    return None


def _table_columns(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    if not rows:
        return set()
    out: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            name = row.get("name")
        else:
            try:
                name = row["name"]
            except Exception:
                name = row[1] if len(row) > 1 else None
        if isinstance(name, str):
            out.add(name)
    return out


def _coalesced_column_expr(
    alias: str,
    columns: set[str],
    preferred: list[str],
    fallback_sql: str = "NULL",
) -> tuple[str, str | None]:
    for col in preferred:
        if col in columns:
            return f"{alias}.{col}", col
    return fallback_sql, None


def _build_rank_query(conn: Any) -> str:
    run_cols = _table_columns(conn, "runs")
    community_cols = _table_columns(conn, "communities")
    sample_cols = _table_columns(conn, "samples")

    if not run_cols or not community_cols or not sample_cols:
        raise RuntimeError("Database schema is missing one or more required tables: runs, communities, samples.")
    if "community_id" not in run_cols:
        raise RuntimeError("runs.community_id is required for ranking query.")
    if "sample_id" not in community_cols:
        raise RuntimeError("communities.sample_id is required for ranking query.")
    if "sample_id" not in sample_cols:
        raise RuntimeError("samples.sample_id is required for ranking query.")

    t1_flux_expr, t1_flux_col = _coalesced_column_expr("r", run_cols, ["t1_target_flux", "t1_flux"])
    if t1_flux_col is None:
        raise RuntimeError("runs table must contain either t1_target_flux (legacy) or t1_flux (v3).")

    t1_conf_expr, _ = _coalesced_column_expr("r", run_cols, ["t1_model_confidence", "t1_confidence"], "'medium'")
    t2_stability_expr, _ = _coalesced_column_expr("r", run_cols, ["t2_stability_score", "t2_stability"], "0.5")
    t2_interventions_expr, _ = _coalesced_column_expr("r", run_cols, ["t2_interventions", "t2_best_intervention"])

    run_date_expr = "r.run_date" if "run_date" in run_cols else "NULL"
    t0_pass_expr = "r.t0_pass" if "t0_pass" in run_cols else "NULL"
    t0_depth_ok_expr = "r.t0_depth_ok" if "t0_depth_ok" in run_cols else "NULL"
    t025_model_expr = "r.t025_model" if "t025_model" in run_cols else "NULL"
    t025_n_pathways_expr = "r.t025_n_pathways" if "t025_n_pathways" in run_cols else "NULL"
    t025_nsti_expr = "r.t025_nsti_mean" if "t025_nsti_mean" in run_cols else "NULL"
    t1_exchanges_expr = "r.t1_metabolic_exchanges" if "t1_metabolic_exchanges" in run_cols else "NULL"
    t2_resistance_expr = "r.t2_resistance" if "t2_resistance" in run_cols else "NULL"
    t2_resilience_expr = "r.t2_resilience" if "t2_resilience" in run_cols else "NULL"
    t2_func_red_expr = "r.t2_functional_redundancy" if "t2_functional_redundancy" in run_cols else "NULL"

    site_id_expr = "s.site_id" if "site_id" in sample_cols else "NULL"
    latitude_expr = "s.latitude" if "latitude" in sample_cols else "NULL"
    longitude_expr = "s.longitude" if "longitude" in sample_cols else "NULL"
    soil_ph_expr = "s.soil_ph" if "soil_ph" in sample_cols else "NULL"
    temperature_expr = "s.temperature_c" if "temperature_c" in sample_cols else "NULL"
    om_expr = "s.organic_matter_pct" if "organic_matter_pct" in sample_cols else "NULL"
    management_expr = "s.management" if "management" in sample_cols else "NULL"

    where_clause = f"{t1_flux_expr} IS NOT NULL"
    return f"""
        SELECT
            r.run_id AS run_id,
            r.community_id AS community_id,
            {run_date_expr} AS run_date,
            {t0_pass_expr} AS t0_pass,
            {t0_depth_ok_expr} AS t0_depth_ok,
            {t025_model_expr} AS t025_model,
            {t025_n_pathways_expr} AS t025_n_pathways,
            {t025_nsti_expr} AS t025_nsti_mean,
            {t1_flux_expr} AS t1_target_flux,
            {t1_conf_expr} AS t1_model_confidence,
            {t1_exchanges_expr} AS t1_metabolic_exchanges,
            {t2_stability_expr} AS t2_stability_score,
            {t2_resistance_expr} AS t2_resistance,
            {t2_resilience_expr} AS t2_resilience,
            {t2_func_red_expr} AS t2_functional_redundancy,
            {t2_interventions_expr} AS t2_interventions,
            c.sample_id AS sample_id,
            {site_id_expr} AS site_id,
            {latitude_expr} AS latitude,
            {longitude_expr} AS longitude,
            {soil_ph_expr} AS soil_ph,
            {temperature_expr} AS temperature_c,
            {om_expr} AS organic_matter_pct,
            {management_expr} AS management
        FROM runs r
        JOIN communities c ON r.community_id = c.community_id
        JOIN samples s ON c.sample_id = s.sample_id
        WHERE {where_clause}
        ORDER BY r.community_id
    """


def _fetch_rank_rows(database: SoilDB) -> list[dict[str, Any]]:
    query = _build_rank_query(database.conn)
    rows = database.conn.execute(query).fetchall()
    return [dict(row) for row in rows]


def _extract_management_metadata(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _build_sim_inputs(row: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    record: dict[str, Any] = {
        "soil_ph": row.get("soil_ph"),
        "organic_matter_pct": row.get("organic_matter_pct"),
        "temperature_c": row.get("temperature_c"),
    }
    management_metadata = _extract_management_metadata(row.get("management"))
    if management_metadata:
        record["metadata"] = management_metadata
    candidate = _extract_top_intervention_candidate(row.get("t2_interventions"))
    return record, candidate


def _derive_top_intervention_label(candidate: dict[str, Any] | None) -> str:
    if not candidate:
        return ""
    for key in (
        "intervention_detail",
        "name",
        "taxon_name",
        "practice",
        "amendment_type",
        "intervention_type",
    ):
        value = candidate.get(key)
        if value:
            return str(value)
    return ""


def _sim_composite_score(
    row: dict[str, Any],
    record_override: dict[str, Any] | None = None,
    candidate_override: dict[str, Any] | None = None,
) -> tuple[float | None, dict[str, Any] | None]:
    if record_override is None and candidate_override is None:
        record, candidate = _build_sim_inputs(row)
    else:
        record = record_override if record_override is not None else {}
        candidate = candidate_override
    try:
        sim_result = simulate_from_pipeline_record(
            record=record,
            intervention_candidate=candidate,
        )
    except Exception as exc:
        logger.debug("sim_model scoring failed for community=%s: %s", row.get("community_id"), exc)
        return None, None

    return _sim_composite_score_from_result(sim_result), sim_result


def _perturb_sim_inputs(
    row: dict[str, Any],
    rng: random.Random,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    base_record, base_candidate = _build_sim_inputs(row)
    record = deepcopy(base_record)
    candidate = deepcopy(base_candidate) if isinstance(base_candidate, dict) else None

    ph = _to_float(record.get("soil_ph"))
    if ph is not None:
        record["soil_ph"] = _clamp(ph + rng.gauss(0.0, 0.22), 3.5, 9.2)

    om = _to_float(record.get("organic_matter_pct"))
    if om is not None:
        record["organic_matter_pct"] = _clamp(om + rng.gauss(0.0, 0.45), 0.0, 20.0)

    temp = _to_float(record.get("temperature_c"))
    if temp is not None:
        record["temperature_c"] = _clamp(temp + rng.gauss(0.0, 0.9), -5.0, 45.0)

    metadata = record.get("metadata")
    if isinstance(metadata, dict):
        moisture = _to_float(metadata.get("moisture"))
        if moisture is None:
            moisture_pct = _to_float(metadata.get("moisture_pct"))
            if moisture_pct is not None:
                moisture = moisture_pct / 100.0 if moisture_pct > 1.0 else moisture_pct
        if moisture is not None:
            metadata["moisture"] = _clamp(moisture + rng.gauss(0.0, 0.05), 0.0, 1.0)

    if isinstance(candidate, dict):
        predicted_effect = _to_float(candidate.get("predicted_effect"))
        if predicted_effect is not None:
            candidate["predicted_effect"] = _clamp(predicted_effect + rng.gauss(0.0, 0.10), 0.0, 1.0)
        establishment = _to_float(candidate.get("establishment_prob"))
        if establishment is not None:
            candidate["establishment_prob"] = _clamp(establishment + rng.gauss(0.0, 0.10), 0.0, 1.0)
        confidence = _to_float(candidate.get("confidence"))
        if confidence is not None:
            candidate["confidence"] = _clamp(confidence + rng.gauss(0.0, 0.08), 0.0, 1.0)

    return record, candidate


def _uncertainty_scores(
    row: dict[str, Any],
    scoring_mode: str,
    legacy_score: float,
    legacy_weight: float,
    uncertainty_samples: int,
    risk_aversion: float,
    uncertainty_seed: int,
    fallback_score: float,
) -> tuple[float, float, float, int]:
    if uncertainty_samples <= 0 or scoring_mode == "legacy":
        return fallback_score, 0.0, fallback_score, 0

    community_id = int(row.get("community_id") or 0)
    rng = random.Random(uncertainty_seed + (community_id * 9973))
    sampled_scores: list[float] = []
    for _ in range(uncertainty_samples):
        record_override, candidate_override = _perturb_sim_inputs(row, rng)
        sim_score, _ = _sim_composite_score(
            row,
            record_override=record_override,
            candidate_override=candidate_override,
        )
        if sim_score is None:
            continue

        if scoring_mode == "sim":
            sampled_scores.append(sim_score)
        elif scoring_mode == "hybrid":
            sampled_scores.append(legacy_weight * legacy_score + (1.0 - legacy_weight) * sim_score)
        else:
            sampled_scores.append(fallback_score)

    if not sampled_scores:
        return fallback_score, 0.0, fallback_score, 0

    score_mean = float(statistics.fmean(sampled_scores))
    score_std = float(statistics.pstdev(sampled_scores)) if len(sampled_scores) > 1 else 0.0
    risk_adjusted_score = score_mean - risk_aversion * score_std
    return score_mean, score_std, risk_adjusted_score, len(sampled_scores)


def _derive_risk_reason(
    score_data: dict[str, Any],
    scoring_mode: str,
    uncertainty_samples: int,
    risk_aversion: float,
) -> str:
    sim_score = score_data.get("sim_score")
    score_std = float(score_data.get("score_std") or 0.0)
    score_mean = float(score_data.get("score_mean") or 0.0)
    risk_adjusted_score = float(score_data.get("risk_adjusted_score") or 0.0)
    samples_used = int(score_data.get("uncertainty_samples_used") or 0)

    if scoring_mode == "legacy":
        return "legacy_mode"
    if sim_score is None:
        return "sim_unavailable_legacy_fallback"
    if uncertainty_samples <= 0:
        return "uncertainty_disabled"
    if samples_used < max(1, uncertainty_samples // 2):
        return "uncertainty_samples_too_sparse"
    if samples_used < uncertainty_samples:
        return "uncertainty_samples_partial"
    if score_std < 0.010:
        return "low_uncertainty"
    if score_std < 0.030:
        return "moderate_uncertainty"
    penalty = score_mean - risk_adjusted_score
    if penalty >= max(0.05, 1.5 * risk_aversion * 0.03):
        return "high_uncertainty_penalized"
    return "high_uncertainty"


def _score_row(
    row: dict[str, Any],
    scoring_mode: str,
    legacy_weight: float,
    uncertainty_samples: int = 0,
    risk_aversion: float = 1.0,
    uncertainty_seed: int = 42,
) -> dict[str, Any]:
    legacy_score = _legacy_composite_score(row)
    if scoring_mode == "legacy":
        score_mean, score_std, risk_adjusted_score, samples_used = _uncertainty_scores(
            row=row,
            scoring_mode=scoring_mode,
            legacy_score=legacy_score,
            legacy_weight=legacy_weight,
            uncertainty_samples=uncertainty_samples,
            risk_aversion=risk_aversion,
            uncertainty_seed=uncertainty_seed,
            fallback_score=legacy_score,
        )
        return {
            "composite_score": legacy_score,
            "legacy_score": legacy_score,
            "sim_score": None,
            "sim_result": None,
            "score_mean": score_mean,
            "score_std": score_std,
            "risk_adjusted_score": risk_adjusted_score,
            "uncertainty_samples_used": samples_used,
        }

    sim_score, sim_result = _sim_composite_score(row)
    if sim_score is None:
        # Safe fallback keeps existing ranking available even if sim scoring cannot run.
        score_mean, score_std, risk_adjusted_score, samples_used = _uncertainty_scores(
            row=row,
            scoring_mode="legacy",
            legacy_score=legacy_score,
            legacy_weight=legacy_weight,
            uncertainty_samples=uncertainty_samples,
            risk_aversion=risk_aversion,
            uncertainty_seed=uncertainty_seed,
            fallback_score=legacy_score,
        )
        return {
            "composite_score": legacy_score,
            "legacy_score": legacy_score,
            "sim_score": None,
            "sim_result": None,
            "score_mean": score_mean,
            "score_std": score_std,
            "risk_adjusted_score": risk_adjusted_score,
            "uncertainty_samples_used": samples_used,
        }

    if scoring_mode == "sim":
        score_mean, score_std, risk_adjusted_score, samples_used = _uncertainty_scores(
            row=row,
            scoring_mode=scoring_mode,
            legacy_score=legacy_score,
            legacy_weight=legacy_weight,
            uncertainty_samples=uncertainty_samples,
            risk_aversion=risk_aversion,
            uncertainty_seed=uncertainty_seed,
            fallback_score=sim_score,
        )
        return {
            "composite_score": sim_score,
            "legacy_score": legacy_score,
            "sim_score": sim_score,
            "sim_result": sim_result,
            "score_mean": score_mean,
            "score_std": score_std,
            "risk_adjusted_score": risk_adjusted_score,
            "uncertainty_samples_used": samples_used,
        }

    if scoring_mode == "hybrid":
        final = legacy_weight * legacy_score + (1.0 - legacy_weight) * sim_score
        score_mean, score_std, risk_adjusted_score, samples_used = _uncertainty_scores(
            row=row,
            scoring_mode=scoring_mode,
            legacy_score=legacy_score,
            legacy_weight=legacy_weight,
            uncertainty_samples=uncertainty_samples,
            risk_aversion=risk_aversion,
            uncertainty_seed=uncertainty_seed,
            fallback_score=final,
        )
        return {
            "composite_score": final,
            "legacy_score": legacy_score,
            "sim_score": sim_score,
            "sim_result": sim_result,
            "score_mean": score_mean,
            "score_std": score_std,
            "risk_adjusted_score": risk_adjusted_score,
            "uncertainty_samples_used": samples_used,
        }

    raise ValueError(f"Unknown scoring mode: {scoring_mode}")


@app.command()
def rank(
    config: Path = typer.Option(..., help="Pipeline config YAML"),
    db: Path = typer.Option(Path("landscape.db"), help="SQLite database path"),
    top: int = typer.Option(50, help="Number of top candidates to report"),
    output: Path = typer.Option(Path("results/ranked_candidates.csv")),
    scoring_mode: str = typer.Option(
        "legacy",
        help="Ranking score mode: legacy, sim, or hybrid.",
    ),
    legacy_weight: float = typer.Option(
        0.50,
        help="Hybrid mode only: weight on legacy score in [0, 1].",
    ),
    uncertainty_samples: int = typer.Option(
        0,
        help="If >0, run sim uncertainty sampling and produce score_mean/score_std/risk_adjusted_score.",
    ),
    risk_aversion: float = typer.Option(
        1.0,
        help="Risk penalty multiplier for risk_adjusted_score = score_mean - risk_aversion*score_std.",
    ),
    uncertainty_seed: int = typer.Option(
        42,
        help="Random seed for uncertainty sampling.",
    ),
):
    """Rank communities and interventions from accumulated run results."""
    logging.basicConfig(level=logging.INFO)
    scoring_mode = scoring_mode.lower().strip()
    valid_modes = {"legacy", "sim", "hybrid"}
    if scoring_mode not in valid_modes:
        raise typer.BadParameter(f"scoring_mode must be one of: {', '.join(sorted(valid_modes))}")
    if not (0.0 <= legacy_weight <= 1.0):
        raise typer.BadParameter("legacy_weight must be in [0, 1].")
    if uncertainty_samples < 0:
        raise typer.BadParameter("uncertainty_samples must be >= 0.")
    if risk_aversion < 0.0:
        raise typer.BadParameter("risk_aversion must be >= 0.")

    # Retrieve all completed T1/T2 runs
    with SoilDB(str(db)) as database:
        rows = _fetch_rank_rows(database)

    if not rows:
        logger.warning("No T1 results found in %s — nothing to rank.", db)
        raise typer.Exit(1)

    records = []
    for row in rows:
        d = dict(row)
        score_data = _score_row(
            d,
            scoring_mode=scoring_mode,
            legacy_weight=legacy_weight,
            uncertainty_samples=uncertainty_samples,
            risk_aversion=risk_aversion,
            uncertainty_seed=uncertainty_seed,
        )
        d["composite_score"] = score_data["composite_score"]
        d["legacy_score"] = score_data["legacy_score"]
        d["sim_score"] = score_data["sim_score"]
        d["score_mean"] = score_data["score_mean"]
        d["score_std"] = score_data["score_std"]
        d["risk_adjusted_score"] = score_data["risk_adjusted_score"]
        d["uncertainty_samples_used"] = score_data["uncertainty_samples_used"]
        d["risk_reason"] = _derive_risk_reason(
            score_data=score_data,
            scoring_mode=scoring_mode,
            uncertainty_samples=uncertainty_samples,
            risk_aversion=risk_aversion,
        )
        d["scoring_mode"] = scoring_mode

        top_candidate = _extract_top_intervention_candidate(d.get("t2_interventions"))
        d["top_intervention"] = _derive_top_intervention_label(top_candidate)
        sim_result = score_data["sim_result"]
        if sim_result is not None:
            d["sim_target_flux"] = sim_result.get("target_flux")
            d["sim_stability_score"] = sim_result.get("stability_score")
            d["sim_establishment_probability"] = sim_result.get("establishment_probability")
        records.append(d)

    sort_metric = "risk_adjusted_score" if uncertainty_samples > 0 else "composite_score"
    records.sort(key=lambda r: float(r.get(sort_metric) or 0.0), reverse=True)
    top_records = records[:top]

    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "rank", "community_id", "sample_id",
        "scoring_mode", "composite_score", "legacy_score", "sim_score",
        "score_mean", "score_std", "risk_adjusted_score", "uncertainty_samples_used",
        "risk_reason",
        "t1_target_flux", "t1_model_confidence",
        "t2_stability_score", "t2_resistance", "t2_resilience",
        "t2_functional_redundancy", "top_intervention",
        "sim_target_flux", "sim_stability_score", "sim_establishment_probability",
        "latitude", "longitude", "soil_ph", "temperature_c", "organic_matter_pct",
    ]
    with open(output, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for i, rec in enumerate(top_records, start=1):
            rec["rank"] = i
            writer.writerow(rec)

    logger.info(
        "Ranked %d candidates with mode=%s (legacy_weight=%.2f, uncertainty_samples=%d, risk_aversion=%.2f, sort=%s) → %s",
        len(top_records),
        scoring_mode,
        legacy_weight,
        uncertainty_samples,
        risk_aversion,
        sort_metric,
        output,
    )
    typer.echo(
        f"Top {len(top_records)} candidates written to {output} "
        f"(mode={scoring_mode}, uncertainty_samples={uncertainty_samples}, sort={sort_metric})"
    )


if __name__ == "__main__":
    app()
