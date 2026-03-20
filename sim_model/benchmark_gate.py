from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence

from .benchmark import run_ranking_benchmark


@dataclass(frozen=True)
class GateThresholds:
    min_top1_lift: float = 0.03
    min_topk_lift: float = 0.02
    min_regret_reduction: float = 0.25
    min_hit_rate_margin: float = 0.15


@dataclass(frozen=True)
class GateConfig:
    seeds: List[int]
    worlds: int = 180
    candidates: int = 10
    top_k: int = 3
    thresholds: GateThresholds = GateThresholds()


def _parse_seeds(raw: str) -> List[int]:
    seeds = [int(part.strip()) for part in raw.split(",") if part.strip()]
    if not seeds:
        raise ValueError("At least one seed is required.")
    return seeds


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

    def _avg(values: Iterable[float]) -> float:
        values = list(values)
        return sum(values) / len(values) if values else 0.0

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
        },
        "aggregate": aggregate,
        "per_seed": per_seed,
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
    )
    payload = evaluate_benchmark_gate(config)

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
