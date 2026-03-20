from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Sequence


DEFAULT_CONFIG_PATH = Path("configs/bnf_loso_regression.yaml")


def _load_structured_text(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json":
        parsed = json.loads(text)
    else:
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


def evaluate_loso_regression(
    report: Dict[str, Any],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []

    loso_r_raw = report.get("loso_spearman_r")
    n_sites_raw = report.get("n_sites")

    if loso_r_raw is None:
        failures.append("missing field 'loso_spearman_r' in LOSO report")
        loso_r = 0.0
    else:
        loso_r = float(loso_r_raw)

    if n_sites_raw is None:
        failures.append("missing field 'n_sites' in LOSO report")
        n_sites = 0
    else:
        n_sites = int(n_sites_raw)

    baseline = float(config.get("baseline_loso_spearman_r", 0.1552))
    min_r = float(config.get("min_loso_spearman_r", 0.12))
    max_drop = float(config.get("max_allowed_drop_from_baseline", 0.03))
    min_sites = int(config.get("min_sites", 40))

    if loso_r < min_r:
        failures.append(f"loso_spearman_r={loso_r:.4f} < min_loso_spearman_r={min_r:.4f}")

    drop = baseline - loso_r
    if drop > max_drop:
        failures.append(
            f"baseline_drop={drop:.4f} exceeds max_allowed_drop_from_baseline={max_drop:.4f} "
            f"(baseline={baseline:.4f}, current={loso_r:.4f})"
        )

    if n_sites < min_sites:
        failures.append(f"n_sites={n_sites} < min_sites={min_sites}")
    elif n_sites < max(min_sites + 5, 45):
        warnings.append(f"n_sites={n_sites} is near floor; consider increasing site coverage for stability.")

    payload: Dict[str, Any] = {
        "passed": len(failures) == 0,
        "loso_spearman_r": loso_r,
        "n_sites": n_sites,
        "checks": {
            "baseline_loso_spearman_r": baseline,
            "min_loso_spearman_r": min_r,
            "max_allowed_drop_from_baseline": max_drop,
            "min_sites": min_sites,
        },
        "baseline_drop": drop,
        "failures": failures,
        "warnings": warnings,
    }
    return payload


def render_markdown(payload: Dict[str, Any]) -> str:
    checks = payload.get("checks", {})
    lines = [
        "## BNF LOSO Regression Gate",
        "",
        f"- Passed: **{'yes' if payload.get('passed') else 'no'}**",
        f"- LOSO Spearman r: `{float(payload.get('loso_spearman_r', 0.0)):.4f}`",
        f"- Sites: `{int(payload.get('n_sites', 0))}`",
        "",
        "| Check | Value | Threshold |",
        "|---|---:|---:|",
        f"| LOSO r | {float(payload.get('loso_spearman_r', 0.0)):.4f} | >= {float(checks.get('min_loso_spearman_r', 0.0)):.4f} |",
        f"| Baseline drop | {float(payload.get('baseline_drop', 0.0)):.4f} | <= {float(checks.get('max_allowed_drop_from_baseline', 0.0)):.4f} |",
        f"| Site count | {int(payload.get('n_sites', 0))} | >= {int(checks.get('min_sites', 0))} |",
    ]
    warnings = payload.get("warnings", [])
    failures = payload.get("failures", [])
    if warnings:
        lines.extend(["", "### Warnings"])
        for warning in warnings:
            lines.append(f"- {warning}")
    if failures:
        lines.extend(["", "### Failures"])
        for failure in failures:
            lines.append(f"- {failure}")
    return "\n".join(lines) + "\n"


def _append_step_summary(markdown: str) -> str | None:
    summary_path_raw = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path_raw:
        return None
    path = Path(summary_path_raw)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(markdown)
    return str(path.resolve())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail CI when LOSO performance regresses below configured thresholds.")
    parser.add_argument("--report", required=True, help="Path to LOSO report JSON from loso_cv_bnf_surrogate.py")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Gate config YAML/JSON")
    parser.add_argument("--append-step-summary", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    report_path = Path(args.report)
    config_path = Path(args.config)
    if not report_path.exists():
        raise FileNotFoundError(f"LOSO report not found: {report_path}")
    if not config_path.exists():
        raise FileNotFoundError(f"Gate config not found: {config_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise ValueError("LOSO report must parse to a JSON object.")
    config = _load_structured_text(config_path)
    payload = evaluate_loso_regression(report=report, config=config)

    markdown = render_markdown(payload)
    if args.append_step_summary:
        _append_step_summary(markdown)

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(markdown)
    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
