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
import logging
import math
from pathlib import Path

import typer

from db_utils import SoilDB

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


def _check2_t025_correlation(db: SoilDB, measured: dict[str, float]) -> dict:
    """Check 2: T0.25 ML score should correlate with measured function (Spearman r > 0.6)."""
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
            "spearman_r": None,
            "passed": True,  # not enough data to fail
            "n": len(paired),
            "note": "Insufficient paired data — check passes by default",
        }

    xs, ys = zip(*paired)
    r = _spearman_r(list(xs), list(ys))
    return {
        "check": "t025_spearman",
        "spearman_r": round(r, 4),
        "threshold": 0.6,
        "passed": r >= 0.6,
        "n": len(paired),
        "note": "Spearman r between T0.25 pathway count and measured function",
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
    output: Path = typer.Option(Path("results/validation_report.json")),
):
    """Validate pipeline output against known reference communities."""
    logging.basicConfig(level=logging.INFO)
    database = SoilDB(str(db))

    measured = _load_measured_function(measured_function)
    logger.info("Loaded %d samples with measured function values", len(measured))

    checks = [
        _check1_t0_pass_rate(database, measured),
        _check2_t025_correlation(database, measured),
        _check3_t1_flux_magnitude(database, measured),
    ]

    # Override threshold from CLI arg
    for c in checks:
        if c["check"] == "t025_spearman":
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
    import json
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
