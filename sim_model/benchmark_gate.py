from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from .benchmark import load_benchmark_history, run_ranking_benchmark


@dataclass(frozen=True)
class GateThresholds:
    min_top1_lift: float = 0.03
    min_topk_lift: float = 0.02
    min_regret_reduction: float = 0.25
    min_hit_rate_margin: float = 0.15


@dataclass(frozen=True)
class TrendThresholds:
    max_top1_lift_drop: float = 0.04
    max_topk_lift_drop: float = 0.04
    max_regret_reduction_drop: float = 0.08
    max_hit_rate_margin_drop: float = 0.10


@dataclass(frozen=True)
class TrendConfig:
    history_path: str | None = None
    trend_window: int = 20
    min_history_entries: int = 1
    fail_on_missing_history: bool = False
    thresholds: TrendThresholds = TrendThresholds()


@dataclass(frozen=True)
class GateConfig:
    seeds: List[int]
    worlds: int = 180
    candidates: int = 10
    top_k: int = 3
    thresholds: GateThresholds = GateThresholds()
    trend: TrendConfig = TrendConfig()


def _parse_seeds(raw: str) -> List[int]:
    seeds = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not seeds:
        raise ValueError("At least one seed is required.")
    return seeds


def _avg(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def _extract_history_metrics(entry: Dict[str, Any]) -> Dict[str, float] | None:
    if not isinstance(entry, dict):
        return None

    if isinstance(entry.get("aggregate"), dict):
        aggregate = entry["aggregate"]
        top1 = aggregate.get("avg_top1_lift")
        topk = aggregate.get("avg_topk_lift")
        regret = aggregate.get("avg_regret_reduction")
        hit = aggregate.get("avg_hit_rate_margin")
        if top1 is None or topk is None or regret is None:
            return None
        out = {
            "top1_lift": float(top1),
            "topk_lift": float(topk),
            "regret_reduction": float(regret),
        }
        if hit is not None:
            out["hit_rate_margin"] = float(hit)
        return out

    lifts = entry.get("lifts")
    if not isinstance(lifts, dict):
        return None
    top1 = lifts.get("funnel_vs_random_top1_lift")
    topk = lifts.get("funnel_vs_random_topk_lift")
    regret = lifts.get("funnel_vs_random_regret_reduction")
    if top1 is None or topk is None or regret is None:
        return None
    out = {
        "top1_lift": float(top1),
        "topk_lift": float(topk),
        "regret_reduction": float(regret),
    }
    summary = entry.get("summary")
    if isinstance(summary, dict):
        funnel = summary.get("funnel", {})
        random_ = summary.get("random", {})
        if isinstance(funnel, dict) and isinstance(random_, dict):
            fh = funnel.get("hit_optimal")
            rh = random_.get("hit_optimal")
            if fh is not None and rh is not None:
                out["hit_rate_margin"] = float(fh) - float(rh)
    return out


def _evaluate_trend(config: TrendConfig, aggregate: Dict[str, float]) -> Dict[str, Any]:
    trend_payload: Dict[str, Any] = {
        "enabled": bool(config.history_path),
        "history_path": config.history_path,
        "trend_window": config.trend_window,
        "min_history_entries": config.min_history_entries,
        "fail_on_missing_history": config.fail_on_missing_history,
        "passed": True,
        "failures": [],
    }
    if not config.history_path:
        trend_payload["enabled"] = False
        return trend_payload

    entries = load_benchmark_history(config.history_path)
    history_metrics = [m for m in (_extract_history_metrics(entry) for entry in entries) if m is not None]
    trend_payload["history_entries_total"] = len(history_metrics)

    if len(history_metrics) < config.min_history_entries:
        msg = (
            f"history entries {len(history_metrics)} < required {config.min_history_entries} "
            f"for trend check ({config.history_path})"
        )
        if config.fail_on_missing_history:
            trend_payload["passed"] = False
            trend_payload["failures"].append(msg)
        else:
            trend_payload["skipped_reason"] = msg
        return trend_payload

    considered = history_metrics[-max(1, config.trend_window) :]
    trend_payload["history_entries_considered"] = len(considered)
    baseline = {
        "top1_lift": _avg(item["top1_lift"] for item in considered),
        "topk_lift": _avg(item["topk_lift"] for item in considered),
        "regret_reduction": _avg(item["regret_reduction"] for item in considered),
    }
    hit_values = [item["hit_rate_margin"] for item in considered if "hit_rate_margin" in item]
    if hit_values:
        baseline["hit_rate_margin"] = _avg(hit_values)
    trend_payload["baseline"] = baseline

    current = {
        "top1_lift": float(aggregate["avg_top1_lift"]),
        "topk_lift": float(aggregate["avg_topk_lift"]),
        "regret_reduction": float(aggregate["avg_regret_reduction"]),
        "hit_rate_margin": float(aggregate["avg_hit_rate_margin"]),
    }
    trend_payload["current"] = current

    checks = [
        ("top1_lift", config.thresholds.max_top1_lift_drop),
        ("topk_lift", config.thresholds.max_topk_lift_drop),
        ("regret_reduction", config.thresholds.max_regret_reduction_drop),
    ]
    if "hit_rate_margin" in baseline:
        checks.append(("hit_rate_margin", config.thresholds.max_hit_rate_margin_drop))

    for metric, max_drop in checks:
        baseline_value = float(baseline[metric])
        current_value = float(current[metric])
        drop = baseline_value - current_value
        if drop > max_drop:
            trend_payload["passed"] = False
            trend_payload["failures"].append(
                f"trend {metric}: current={current_value:.4f}, baseline={baseline_value:.4f}, "
                f"drop={drop:.4f} > max_drop={max_drop:.4f}"
            )
    return trend_payload


def append_gate_history(payload: Dict[str, Any], history_path: str | Path) -> Dict[str, Any]:
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "gate_config": payload.get("gate_config", {}),
        "aggregate": payload.get("aggregate", {}),
        "passed": payload.get("passed", False),
        "trend": payload.get("trend", {}),
    }
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True))
        handle.write("\n")
    return entry


def _fmt(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.4f}"


def render_markdown_summary(payload: Dict[str, Any]) -> str:
    passed = bool(payload.get("passed", False))
    gate_config = payload.get("gate_config", {})
    aggregate = payload.get("aggregate", {})
    thresholds = gate_config.get("thresholds", {})
    trend = payload.get("trend", {}) if isinstance(payload.get("trend"), dict) else {}
    failures = payload.get("failures", [])

    lines = [
        "## Sim Model Benchmark Gate",
        "",
        f"- Status: **{'PASS' if passed else 'FAIL'}**",
        f"- Worlds: `{gate_config.get('worlds')}` | Candidates: `{gate_config.get('candidates')}` | "
        f"Top-k: `{gate_config.get('top_k')}` | Seeds: `{gate_config.get('seeds')}`",
        "",
        "| Metric | Current Avg | Threshold |",
        "|---|---:|---:|",
        f"| top1 lift | {_fmt(aggregate.get('avg_top1_lift'))} | >= {_fmt(thresholds.get('min_top1_lift'))} |",
        f"| topk lift | {_fmt(aggregate.get('avg_topk_lift'))} | >= {_fmt(thresholds.get('min_topk_lift'))} |",
        f"| regret reduction | {_fmt(aggregate.get('avg_regret_reduction'))} | >= {_fmt(thresholds.get('min_regret_reduction'))} |",
        f"| hit-rate margin | {_fmt(aggregate.get('avg_hit_rate_margin'))} | >= {_fmt(thresholds.get('min_hit_rate_margin'))} |",
    ]

    if trend.get("enabled"):
        baseline = trend.get("baseline", {})
        current = trend.get("current", {})
        trend_thresholds = gate_config.get("trend", {}).get("thresholds", {})
        lines.extend(
            [
                "",
                "### Trend Check",
                f"- Passed: **{'yes' if trend.get('passed') else 'no'}**",
                "| Metric | Current | Baseline | Max Drop |",
                "|---|---:|---:|---:|",
                f"| top1 lift | {_fmt(current.get('top1_lift'))} | {_fmt(baseline.get('top1_lift'))} | {_fmt(trend_thresholds.get('max_top1_lift_drop'))} |",
                f"| topk lift | {_fmt(current.get('topk_lift'))} | {_fmt(baseline.get('topk_lift'))} | {_fmt(trend_thresholds.get('max_topk_lift_drop'))} |",
                f"| regret reduction | {_fmt(current.get('regret_reduction'))} | {_fmt(baseline.get('regret_reduction'))} | {_fmt(trend_thresholds.get('max_regret_reduction_drop'))} |",
                f"| hit-rate margin | {_fmt(current.get('hit_rate_margin'))} | {_fmt(baseline.get('hit_rate_margin'))} | {_fmt(trend_thresholds.get('max_hit_rate_margin_drop'))} |",
            ]
        )

    if failures:
        lines.extend(["", "### Failures"])
        for failure in failures[:12]:
            lines.append(f"- {failure}")
    return "\n".join(lines) + "\n"


def write_report_artifacts(
    payload: Dict[str, Any],
    report_json_path: str | Path | None = None,
    report_md_path: str | Path | None = None,
    append_step_summary: bool = False,
) -> Dict[str, str]:
    outputs: Dict[str, str] = {}
    markdown = render_markdown_summary(payload)

    if report_json_path:
        json_path = Path(report_json_path)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        outputs["json"] = str(json_path.resolve())

    if report_md_path:
        md_path = Path(report_md_path)
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(markdown, encoding="utf-8")
        outputs["markdown"] = str(md_path.resolve())

    if append_step_summary:
        summary_path_raw = os.getenv("GITHUB_STEP_SUMMARY")
        if summary_path_raw:
            summary_path = Path(summary_path_raw)
            summary_path.parent.mkdir(parents=True, exist_ok=True)
            with summary_path.open("a", encoding="utf-8") as handle:
                handle.write(markdown)
            outputs["step_summary"] = str(summary_path.resolve())
    return outputs


def evaluate_benchmark_gate(config: GateConfig) -> Dict[str, Any]:
    per_seed: List[Dict[str, Any]] = []
    failures: List[str] = []

    for seed in config.seeds:
        result = run_ranking_benchmark(
            n_worlds=config.worlds,
            n_candidates=config.candidates,
            top_k=config.top_k,
            random_state=seed,
        )
        lifts = result["lifts"]
        summary = result["summary"]
        funnel_hit = summary["funnel"]["hit_optimal"]
        random_hit = summary["random"]["hit_optimal"]
        hit_margin = funnel_hit - random_hit

        run_row = {
            "seed": seed,
            "config": result["config"],
            "lifts": lifts,
            "summary": summary,
            "hit_rate_margin": hit_margin,
        }
        per_seed.append(run_row)

        if lifts["funnel_vs_random_top1_lift"] < config.thresholds.min_top1_lift:
            failures.append(
                f"seed {seed}: funnel_vs_random_top1_lift={lifts['funnel_vs_random_top1_lift']:.4f} "
                f"< {config.thresholds.min_top1_lift:.4f}"
            )
        if lifts["funnel_vs_random_topk_lift"] < config.thresholds.min_topk_lift:
            failures.append(
                f"seed {seed}: funnel_vs_random_topk_lift={lifts['funnel_vs_random_topk_lift']:.4f} "
                f"< {config.thresholds.min_topk_lift:.4f}"
            )
        if lifts["funnel_vs_random_regret_reduction"] < config.thresholds.min_regret_reduction:
            failures.append(
                f"seed {seed}: funnel_vs_random_regret_reduction={lifts['funnel_vs_random_regret_reduction']:.4f} "
                f"< {config.thresholds.min_regret_reduction:.4f}"
            )
        if hit_margin < config.thresholds.min_hit_rate_margin:
            failures.append(
                f"seed {seed}: funnel_hit_optimal - random_hit_optimal={hit_margin:.4f} "
                f"< {config.thresholds.min_hit_rate_margin:.4f}"
            )

    top1_lifts = [row["lifts"]["funnel_vs_random_top1_lift"] for row in per_seed]
    topk_lifts = [row["lifts"]["funnel_vs_random_topk_lift"] for row in per_seed]
    regret_lifts = [row["lifts"]["funnel_vs_random_regret_reduction"] for row in per_seed]
    hit_margins = [row["hit_rate_margin"] for row in per_seed]

    aggregate = {
        "avg_top1_lift": _avg(top1_lifts),
        "min_top1_lift": min(top1_lifts),
        "avg_topk_lift": _avg(topk_lifts),
        "min_topk_lift": min(topk_lifts),
        "avg_regret_reduction": _avg(regret_lifts),
        "min_regret_reduction": min(regret_lifts),
        "avg_hit_rate_margin": _avg(hit_margins),
        "min_hit_rate_margin": min(hit_margins),
    }

    trend = _evaluate_trend(config.trend, aggregate)
    failures.extend(trend.get("failures", []))

    return {
        "passed": len(failures) == 0,
        "gate_config": {
            "seeds": config.seeds,
            "worlds": config.worlds,
            "candidates": config.candidates,
            "top_k": config.top_k,
            "thresholds": {
                "min_top1_lift": config.thresholds.min_top1_lift,
                "min_topk_lift": config.thresholds.min_topk_lift,
                "min_regret_reduction": config.thresholds.min_regret_reduction,
                "min_hit_rate_margin": config.thresholds.min_hit_rate_margin,
            },
            "trend": {
                "history_path": config.trend.history_path,
                "trend_window": config.trend.trend_window,
                "min_history_entries": config.trend.min_history_entries,
                "fail_on_missing_history": config.trend.fail_on_missing_history,
                "thresholds": {
                    "max_top1_lift_drop": config.trend.thresholds.max_top1_lift_drop,
                    "max_topk_lift_drop": config.trend.thresholds.max_topk_lift_drop,
                    "max_regret_reduction_drop": config.trend.thresholds.max_regret_reduction_drop,
                    "max_hit_rate_margin_drop": config.trend.thresholds.max_hit_rate_margin_drop,
                },
            },
        },
        "aggregate": aggregate,
        "per_seed": per_seed,
        "trend": trend,
        "failures": failures,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CI gate: ensure ranking benchmark lift remains above minimum thresholds."
    )
    parser.add_argument("--seeds", type=str, default="7,13,29")
    parser.add_argument("--worlds", type=int, default=180)
    parser.add_argument("--candidates", type=int, default=10)
    parser.add_argument("--top-k", type=int, default=3)
    parser.add_argument("--min-top1-lift", type=float, default=0.03)
    parser.add_argument("--min-topk-lift", type=float, default=0.02)
    parser.add_argument("--min-regret-reduction", type=float, default=0.25)
    parser.add_argument("--min-hit-rate-margin", type=float, default=0.15)
    parser.add_argument("--history-path", type=str, default=None)
    parser.add_argument("--trend-window", type=int, default=20)
    parser.add_argument("--min-history-entries", type=int, default=1)
    parser.add_argument("--fail-on-missing-history", action="store_true")
    parser.add_argument("--max-top1-lift-drop", type=float, default=0.04)
    parser.add_argument("--max-topk-lift-drop", type=float, default=0.04)
    parser.add_argument("--max-regret-reduction-drop", type=float, default=0.08)
    parser.add_argument("--max-hit-rate-margin-drop", type=float, default=0.10)
    parser.add_argument("--append-history-path", type=str, default=None)
    parser.add_argument("--report-json-path", type=str, default=None)
    parser.add_argument("--report-md-path", type=str, default=None)
    parser.add_argument("--append-step-summary", action="store_true")
    parser.add_argument("--json", action="store_true", help="Print full payload as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = GateConfig(
        seeds=_parse_seeds(args.seeds),
        worlds=args.worlds,
        candidates=args.candidates,
        top_k=args.top_k,
        thresholds=GateThresholds(
            min_top1_lift=args.min_top1_lift,
            min_topk_lift=args.min_topk_lift,
            min_regret_reduction=args.min_regret_reduction,
            min_hit_rate_margin=args.min_hit_rate_margin,
        ),
        trend=TrendConfig(
            history_path=args.history_path,
            trend_window=args.trend_window,
            min_history_entries=args.min_history_entries,
            fail_on_missing_history=args.fail_on_missing_history,
            thresholds=TrendThresholds(
                max_top1_lift_drop=args.max_top1_lift_drop,
                max_topk_lift_drop=args.max_topk_lift_drop,
                max_regret_reduction_drop=args.max_regret_reduction_drop,
                max_hit_rate_margin_drop=args.max_hit_rate_margin_drop,
            ),
        ),
    )
    payload = evaluate_benchmark_gate(config)
    if args.append_history_path:
        appended = append_gate_history(payload, args.append_history_path)
        payload["history_appended"] = {
            "path": str(Path(args.append_history_path).resolve()),
            "timestamp": appended["run_timestamp_utc"],
        }
    if args.report_json_path or args.report_md_path or args.append_step_summary:
        artifacts = write_report_artifacts(
            payload=payload,
            report_json_path=args.report_json_path,
            report_md_path=args.report_md_path,
            append_step_summary=args.append_step_summary,
        )
        if artifacts:
            payload["report_artifacts"] = artifacts

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"passed: {payload['passed']}")
        print(f"aggregate: {payload['aggregate']}")
        if payload["failures"]:
            print("failures:")
            for line in payload["failures"]:
                print(f"- {line}")

    return 0 if payload["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
