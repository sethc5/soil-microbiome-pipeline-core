from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from .dynamics import simulate_dynamics
from .schema import Community, Environment, Intervention


DEFAULT_CONFIG_PATH = Path("configs/sim_model_calibration.yaml")


def _safe_div(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return numerator / denominator


def _average(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def _load_structured_text(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        return json.loads(text)

    try:
        import yaml  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            f"YAML config '{path}' requires PyYAML. Install pyyaml or use JSON config."
        ) from exc
    parsed = yaml.safe_load(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"Config '{path}' must parse to a mapping.")
    return parsed


def load_calibration_config(path: str | Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Calibration config not found: {config_path}")
    return _load_structured_text(config_path)


def _coerce_inputs(raw: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    return {
        "community": dict(raw.get("community", {})),
        "environment": dict(raw.get("environment", {})),
        "intervention": dict(raw.get("intervention", {})),
    }


def _to_dataclasses(inputs: Dict[str, Dict[str, float]]) -> tuple[Community, Environment, Intervention]:
    community = Community(**inputs["community"]).clamped()
    environment = Environment(**inputs["environment"]).clamped()
    intervention = Intervention(**inputs["intervention"]).clamped()
    return community, environment, intervention


def _set_path(target: Dict[str, Dict[str, float]], dotted_path: str, value: float) -> None:
    parts = dotted_path.split(".")
    if len(parts) != 2:
        raise ValueError(f"Invalid path '{dotted_path}'. Expected 'section.field'.")
    section, field = parts
    if section not in target:
        raise ValueError(f"Unknown section '{section}' for path '{dotted_path}'.")
    target[section][field] = value


def _get_metric(result: Any, metric: str) -> float:
    if not hasattr(result, metric):
        raise ValueError(f"Unknown metric '{metric}'.")
    return float(getattr(result, metric))


def _run_with_inputs(inputs: Dict[str, Dict[str, float]]) -> Any:
    community, environment, intervention = _to_dataclasses(inputs)
    return simulate_dynamics(community, environment, intervention)


def _check_drift(stat_value: float, check: Dict[str, Any], details: Dict[str, Any]) -> bool:
    expected_value = check.get("expected_value")
    max_abs_drift = check.get("max_abs_drift")
    if expected_value is None or max_abs_drift is None:
        details["drift_checked"] = False
        return True

    drift = abs(stat_value - float(expected_value))
    details["drift_checked"] = True
    details["expected_value"] = float(expected_value)
    details["max_abs_drift"] = float(max_abs_drift)
    details["abs_drift"] = drift
    return drift <= float(max_abs_drift)


def _evaluate_monotonic_sweep(check: Dict[str, Any], base_inputs: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    sweep_path = str(check["sweep_path"])
    values = [float(v) for v in check["values"]]
    metric = str(check["metric"])
    direction = str(check.get("direction", "increasing")).lower().strip()
    tolerance = float(check.get("tolerance", 1e-9))
    min_total_change = float(check.get("min_total_change", 0.0))

    metric_values: List[float] = []
    for value in values:
        working = deepcopy(base_inputs)
        _set_path(working, sweep_path, value)
        result = _run_with_inputs(working)
        metric_values.append(_get_metric(result, metric))

    deltas = [b - a for a, b in zip(metric_values, metric_values[1:])]
    if direction == "increasing":
        monotonic_ok = all(delta >= -tolerance for delta in deltas)
        total_change = metric_values[-1] - metric_values[0]
        threshold_ok = total_change >= min_total_change
    elif direction == "decreasing":
        monotonic_ok = all(delta <= tolerance for delta in deltas)
        total_change = metric_values[0] - metric_values[-1]
        threshold_ok = total_change >= min_total_change
    else:
        raise ValueError(f"Unsupported monotonic direction: {direction}")

    details: Dict[str, Any] = {
        "type": "monotonic_sweep",
        "metric": metric,
        "sweep_path": sweep_path,
        "values": values,
        "metric_values": metric_values,
        "direction": direction,
        "monotonic_ok": monotonic_ok,
        "total_change": total_change,
        "min_total_change": min_total_change,
    }
    drift_ok = _check_drift(stat_value=total_change, check=check, details=details)
    passed = monotonic_ok and threshold_ok and drift_ok
    details["passed"] = passed
    return details


def _evaluate_band_comparison(check: Dict[str, Any], base_inputs: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    sweep_path = str(check["sweep_path"])
    low_values = [float(v) for v in check["low_values"]]
    high_values = [float(v) for v in check["high_values"]]
    metric = str(check["metric"])
    expectation = str(check.get("expectation", "high_gt_low")).lower().strip()
    min_ratio = float(check.get("min_ratio", 1.0))
    max_ratio = float(check.get("max_ratio", 1.0))
    max_gap = float(check.get("max_gap", 0.0))

    def _sample(values: List[float]) -> List[float]:
        out: List[float] = []
        for value in values:
            working = deepcopy(base_inputs)
            _set_path(working, sweep_path, value)
            result = _run_with_inputs(working)
            out.append(_get_metric(result, metric))
        return out

    low_metrics = _sample(low_values)
    high_metrics = _sample(high_values)

    low_mean = _average(low_metrics)
    high_mean = _average(high_metrics)
    ratio = _safe_div(high_mean, low_mean)
    gap = high_mean - low_mean

    if expectation == "high_gt_low":
        expectation_ok = ratio >= min_ratio
        drift_stat = ratio
    elif expectation == "low_gt_high":
        expectation_ok = _safe_div(low_mean, high_mean) >= min_ratio
        drift_stat = _safe_div(low_mean, high_mean)
    elif expectation == "similar":
        expectation_ok = abs(gap) <= max_gap and (ratio <= max_ratio if max_ratio > 0 else True)
        drift_stat = gap
    else:
        raise ValueError(f"Unsupported expectation: {expectation}")

    details: Dict[str, Any] = {
        "type": "band_comparison",
        "metric": metric,
        "sweep_path": sweep_path,
        "low_values": low_values,
        "high_values": high_values,
        "low_metrics": low_metrics,
        "high_metrics": high_metrics,
        "low_mean": low_mean,
        "high_mean": high_mean,
        "ratio": ratio,
        "gap": gap,
        "expectation": expectation,
        "min_ratio": min_ratio,
        "max_ratio": max_ratio,
        "max_gap": max_gap,
    }
    drift_ok = _check_drift(stat_value=drift_stat, check=check, details=details)
    passed = expectation_ok and drift_ok
    details["passed"] = passed
    return details


def evaluate_calibration_config(config: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _coerce_inputs(config.get("defaults", {}))
    checks = list(config.get("checks", []))
    if not checks:
        raise ValueError("Calibration config must include at least one check.")

    check_results: List[Dict[str, Any]] = []
    failures: List[str] = []
    for check in checks:
        check_id = str(check.get("id", "unnamed_check"))
        check_type = str(check.get("type", "")).strip().lower()
        try:
            if check_type == "monotonic_sweep":
                details = _evaluate_monotonic_sweep(check, defaults)
            elif check_type == "band_comparison":
                details = _evaluate_band_comparison(check, defaults)
            else:
                raise ValueError(f"Unsupported check type: {check_type}")
        except Exception as exc:
            details = {
                "id": check_id,
                "type": check_type,
                "passed": False,
                "error": str(exc),
            }
        details["id"] = check_id
        check_results.append(details)
        if not details.get("passed", False):
            failures.append(check_id)

    drift_thresholds = dict(config.get("drift_thresholds", {}))
    max_failed_checks = int(drift_thresholds.get("max_failed_checks", 0))
    min_pass_rate = float(drift_thresholds.get("min_pass_rate", 1.0))

    total_checks = len(check_results)
    failed_checks = len(failures)
    pass_rate = _safe_div(total_checks - failed_checks, total_checks)

    passed = failed_checks <= max_failed_checks and pass_rate >= min_pass_rate
    return {
        "passed": passed,
        "summary": {
            "total_checks": total_checks,
            "failed_checks": failed_checks,
            "pass_rate": pass_rate,
            "max_failed_checks": max_failed_checks,
            "min_pass_rate": min_pass_rate,
        },
        "failed_check_ids": failures,
        "checks": check_results,
    }


def run_calibration(config_path: str | Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    config = load_calibration_config(config_path)
    return evaluate_calibration_config(config)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run sim_model calibration checks.")
    parser.add_argument("--config", type=str, default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    payload = run_calibration(args.config)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"passed: {payload['passed']}")
        print(f"summary: {payload['summary']}")
        if payload["failed_check_ids"]:
            print("failed_check_ids:")
            for check_id in payload["failed_check_ids"]:
                print(f"- {check_id}")
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
