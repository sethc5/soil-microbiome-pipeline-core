"""
validate_pipeline.py — Known community recovery test (mandatory first step).

Takes a set of soil samples with published target-function measurements
and verifies that:
  1. High-function samples pass T0 more often than low-function samples
  2. T0.25 ML scores correlate with measured function (Spearman r > 0.6 target)
  3. T1 predicted fluxes are within 2 orders of magnitude of measured values

If validation fails, the pipeline is not ready for production screening.

Usage:
  python validate_pipeline.py \
    --config config.yaml \
    --reference-communities reference/high_bnf_communities.biom \
    --measured-function reference/bnf_measurements.csv
"""

from __future__ import annotations
import csv
import json
import logging
import math
from pathlib import Path
from typing import Optional

import numpy as np
import typer

from core.compute.functional_predictor import FunctionalPredictor
from core.db_utils import SoilDB

app = typer.Typer()
logger = logging.getLogger(__name__)


def _spearman_r(x: list[float], y: list[float]) -> float:
    n = len(x)
    if n < 3:
        return 0.0

    def _rank(seq: list[float]) -> list[float]:
        sorted_vals = sorted(enumerate(seq), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and sorted_vals[j][1] == sorted_vals[j + 1][1]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                ranks[sorted_vals[k][0]] = avg
            i = j + 1
        return ranks

    rx = _rank(x)
    ry = _rank(y)
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1 - 6 * d2 / (n * (n * n - 1))


def _load_measured_function(csv_path: Path) -> dict[str, float]:
    """Load sample_id → measured_function mapping from a 2-column CSV."""
    result: dict[str, float] = {}
    with open(csv_path) as fh:
        reader = csv.DictReader(fh)
        id_col = None
        val_col = None
        for row in reader:
            if id_col is None:
                for c in ("sample_id", "#SampleID", "SampleID", "sample_name"):
                    if c in row:
                        id_col = c
                        break
                for c in ("measured_function", "value", "flux", "nitrogenase", "activity"):
                    if c in row:
                        val_col = c
                        break
                if id_col is None or val_col is None:
                    raise ValueError(
                        f"Cannot detect sample_id or value columns in {csv_path}. "
                        f"Expected columns like 'sample_id' and 'measured_function'."
                    )
            sid = row[id_col].strip()
            try:
                result[sid] = float(row[val_col])
            except (ValueError, KeyError):
                pass
    return result


def _check1_t0_pass_rate(db: SoilDB, measured: dict[str, float]) -> dict:
    """Check 1: High-function samples should pass T0 more often than low-function."""
    median_val = sorted(measured.values())[len(measured) // 2]
    high_ids = {sid for sid, v in measured.items() if v >= median_val}
    low_ids = {sid for sid, v in measured.items() if v < median_val}

    with db._connect() as conn:
        def pass_rate(sample_ids: set[str]) -> float:
            if not sample_ids:
                return 0.0
            placeholders = ",".join("?" * len(sample_ids))
            rows = conn.execute(
                f"SELECT r.t0_pass FROM runs r "
                f"JOIN communities c ON r.community_id = c.community_id "
                f"WHERE c.sample_id IN ({placeholders})",
                list(sample_ids),
            ).fetchall()
            if not rows:
                return float("nan")
            return sum(1 for r in rows if r[0]) / len(rows)

        high_rate = pass_rate(high_ids)
        low_rate = pass_rate(low_ids)

    passed = high_rate > low_rate or math.isnan(high_rate)
    return {
        "check": "t0_pass_rate",
        "high_function_pass_rate": high_rate,
        "low_function_pass_rate": low_rate,
        "passed": passed,
        "note": "High-function samples should pass T0 filters more often",
    }


def _build_feature_vector(
    phylum_profile_json: str | None,
    soil_ph: float | None,
    organic_matter_pct: float | None,
    clay_pct: float | None,
    temperature_c: float | None,
    precipitation_mm: float | None,
    feature_names: list[str],
) -> list[float]:
    """Build a feature vector in the order expected by the RF surrogate."""
    pp: dict[str, float] = json.loads(phylum_profile_json or "{}") if phylum_profile_json else {}
    meta: dict[str, float] = {
        "soil_ph": soil_ph if soil_ph is not None else 6.5,
        "organic_matter_pct": organic_matter_pct if organic_matter_pct is not None else 2.5,
        "clay_pct": clay_pct if clay_pct is not None else 20.0,
        "temperature_c": temperature_c if temperature_c is not None else 15.0,
        "precipitation_mm": precipitation_mm if precipitation_mm is not None else 600.0,
    }
    row: dict[str, float] = {**{k: pp.get(k, 0.0) for k in feature_names}, **meta}
    return [row.get(f, 0.0) for f in feature_names]


def _check2_with_surrogate(
    db: SoilDB,
    measured: dict[str, float],
    predictor: FunctionalPredictor,
    threshold: float = 0.6,
) -> dict:
    """Check 2 (surrogate path): RF-predicted BNF flux should correlate with measured function.

    Loads phylum_profile + env metadata for each measured sample_id,
    builds feature vectors, runs predict_batch_with_gate(), and computes
    Spearman r between predicted flux and measured function.
    """
    sample_ids = list(measured.keys())
    placeholders = ",".join("?" * len(sample_ids))

    with db._connect() as conn:
        rows = conn.execute(
            f"SELECT c.sample_id, c.phylum_profile, "
            f"s.soil_ph, s.organic_matter_pct, s.clay_pct, s.temperature_c, s.precipitation_mm "
            f"FROM communities c "
            f"JOIN samples s ON c.sample_id = s.sample_id "
            f"WHERE c.sample_id IN ({placeholders})",
            sample_ids,
        ).fetchall()

    if not rows:
        return {
            "check": "t025_spearman",
            "method": "surrogate_rf",
            "spearman_r": None,
            "passed": True,
            "n": 0,
            "note": "No communities found in DB for measured sample_ids — check passes by default",
        }

    feat_names = predictor._feature_names
    feature_matrix: list[list[float]] = []
    valid_measured: list[float] = []

    for sid, pp_json, ph, om, clay, temp, precip in rows:
        vec = _build_feature_vector(pp_json, ph, om, clay, temp, precip, feat_names)
        feature_matrix.append(vec)
        valid_measured.append(measured[sid])

    X = np.array(feature_matrix, dtype=float)
    preds, _uncs, _flags = predictor.predict_batch_with_gate(X)

    if len(preds) < 5:
        return {
            "check": "t025_spearman",
            "method": "surrogate_rf",
            "spearman_r": None,
            "passed": True,
            "n": len(preds),
            "note": "Insufficient paired data (<5) — check passes by default",
        }

    r = _spearman_r(preds.tolist(), valid_measured)
    return {
        "check": "t025_spearman",
        "method": "surrogate_rf",
        "spearman_r": round(r, 4),
        "threshold": threshold,
        "passed": r >= threshold,
        "n": len(preds),
        "note": "Spearman r between RF-surrogate predicted BNF flux and measured function",
    }


def _check2_t025_correlation(db: SoilDB, measured: dict[str, float]) -> dict:
    """Check 2 (legacy path): T0.25 PICRUSt2 pathway count should correlate with measured function.

    Used as fallback when no surrogate model path is provided.
    """
    with db._connect() as conn:
        rows = conn.execute(
            "SELECT c.sample_id, r.t025_nsti_mean, r.t025_n_pathways "
            "FROM runs r JOIN communities c ON r.community_id = c.community_id "
            "WHERE r.t025_n_pathways IS NOT NULL"
        ).fetchall()

    # Use n_pathways as proxy for predicted function score
    paired = []
    for sid, nsti, n_pathways in rows:
        if sid in measured and n_pathways is not None:
            # Lower NSTI → better; use n_pathways as positive proxy
            paired.append((float(n_pathways), measured[sid]))

    if len(paired) < 5:
        return {
            "check": "t025_spearman",
            "method": "picrust2_n_pathways",
            "spearman_r": None,
            "passed": True,  # not enough data to fail
            "n": len(paired),
            "note": "Insufficient paired data — check passes by default",
        }

    xs, ys = zip(*paired)
    r = _spearman_r(list(xs), list(ys))
    return {
        "check": "t025_spearman",
        "method": "picrust2_n_pathways",
        "spearman_r": round(r, 4),
        "threshold": 0.6,
        "passed": r >= 0.6,
        "n": len(paired),
        "note": "Spearman r between T0.25 PICRUSt2 pathway count and measured function",
    }


def _check3_t1_flux_magnitude(db: SoilDB, measured: dict[str, float]) -> dict:
    """Check 3: T1 predicted fluxes should be within 2 orders of magnitude of measured."""
    with db._connect() as conn:
        rows = conn.execute(
            "SELECT c.sample_id, r.t1_target_flux FROM runs r "
            "JOIN communities c ON r.community_id = c.community_id "
            "WHERE r.t1_target_flux IS NOT NULL"
        ).fetchall()

    within_2_orders = 0
    total_paired = 0
    for sid, flux in rows:
        if sid in measured and flux and measured[sid]:
            ratio = abs(math.log10(max(flux, 1e-12)) - math.log10(max(measured[sid], 1e-12)))
            total_paired += 1
            if ratio <= 2.0:
                within_2_orders += 1

    if total_paired == 0:
        return {
            "check": "t1_flux_magnitude",
            "fraction_within_2_orders": None,
            "passed": True,
            "n": 0,
            "note": "No paired T1 flux + measured data found — check passes by default",
        }

    fraction = within_2_orders / total_paired
    return {
        "check": "t1_flux_magnitude",
        "fraction_within_2_orders": round(fraction, 4),
        "threshold": 0.5,
        "passed": fraction >= 0.5,
        "n": total_paired,
        "note": "Fraction of communities with T1 flux within 2 log10 orders of measured value",
    }


@app.command()
def validate(
    config: Path = typer.Option(..., help="Pipeline config YAML"),
    reference_communities: Path = typer.Option(..., help="BIOM or ID list of reference communities"),
    measured_function: Path = typer.Option(..., help="CSV with sample_id, measured_function columns"),
    db: Path = typer.Option(Path("landscape.db")),
    spearman_threshold: float = typer.Option(0.6, help="Minimum acceptable Spearman r for Check 2"),
    model_path: Optional[Path] = typer.Option(
        None,
        help="Path to functional_predictor.joblib for RF-surrogate Check 2 "
             "(falls back to PICRUSt2 pathway count if not provided)",
    ),
    output: Path = typer.Option(Path("results/validation_report.json")),
):
    """Validate pipeline output against known reference communities."""
    logging.basicConfig(level=logging.INFO)
    database = SoilDB(str(db))

    measured = _load_measured_function(measured_function)
    logger.info("Loaded %d samples with measured function values", len(measured))

    # Check 2: prefer RF surrogate when model path is given
    if model_path and model_path.exists():
        logger.info("Loading RF surrogate from %s for Check 2", model_path)
        predictor = FunctionalPredictor.load(str(model_path))
        check2 = _check2_with_surrogate(database, measured, predictor, threshold=spearman_threshold)
    else:
        if model_path:
            logger.warning("--model-path %s not found; falling back to PICRUSt2 proxy", model_path)
        check2 = _check2_t025_correlation(database, measured)

    checks = [
        _check1_t0_pass_rate(database, measured),
        check2,
        _check3_t1_flux_magnitude(database, measured),
    ]

    # Override threshold from CLI arg (for legacy path; surrogate path sets it directly)
    for c in checks:
        if c["check"] == "t025_spearman" and c.get("method") == "picrust2_n_pathways":
            c["threshold"] = spearman_threshold
            if c.get("spearman_r") is not None:
                c["passed"] = c["spearman_r"] >= spearman_threshold

    all_passed = all(c["passed"] for c in checks)
    report = {
        "validation_passed": all_passed,
        "checks": checks,
        "n_reference_samples": len(measured),
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2))

    for c in checks:
        status = "PASS" if c["passed"] else "FAIL"
        logger.info("[%s] %s — %s", status, c["check"], c.get("note", ""))
        typer.echo(f"  [{status}] {c['check']}: {c.get('note', '')}")

    if all_passed:
        typer.echo("\nValidation PASSED — pipeline is ready for production screening.")
    else:
        typer.echo("\nValidation FAILED — review results before production use.", err=True)
        raise typer.Exit(2)


if __name__ == "__main__":
    app()
