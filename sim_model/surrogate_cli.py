from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Sequence

from .dynamics import simulate_dynamics
from .scenarios import get_scenarios
from .schema import Community, Environment, Intervention
from .surrogate import (
    evaluate_surrogate,
    load_surrogate_artifacts,
    predict_with_surrogate,
    save_surrogate_artifacts,
    train_surrogate,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train, evaluate, and use surrogate models.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    train_cmd = subparsers.add_parser("train", help="Train and save surrogate artifacts.")
    train_cmd.add_argument("--samples", type=int, default=1500)
    train_cmd.add_argument("--random-state", type=int, default=42)
    train_cmd.add_argument("--test-size", type=float, default=0.20)
    train_cmd.add_argument("--output-dir", type=str, required=True)
    train_cmd.add_argument("--json", action="store_true")

    eval_cmd = subparsers.add_parser("eval", help="Evaluate saved artifacts on fresh synthetic data.")
    eval_cmd.add_argument("--artifacts-dir", type=str, required=True)
    eval_cmd.add_argument("--samples", type=int, default=500)
    eval_cmd.add_argument("--random-state", type=int, default=1337)
    eval_cmd.add_argument("--json", action="store_true")

    predict_cmd = subparsers.add_parser("predict", help="Predict outcomes from one input point.")
    predict_cmd.add_argument("--artifacts-dir", type=str, required=True)
    predict_cmd.add_argument("--scenario", type=str, default="easy_win")
    predict_cmd.add_argument("--include-truth", action="store_true")
    predict_cmd.add_argument("--json", action="store_true")
    _add_input_override_args(predict_cmd)

    return parser


def _add_input_override_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--diazotrophs", type=float, default=None)
    parser.add_argument("--decomposers", type=float, default=None)
    parser.add_argument("--competitors", type=float, default=None)
    parser.add_argument("--stress-tolerant-taxa", dest="stress_tolerant_taxa", type=float, default=None)

    parser.add_argument("--soil-ph", dest="soil_ph", type=float, default=None)
    parser.add_argument("--organic-matter-pct", dest="organic_matter_pct", type=float, default=None)
    parser.add_argument("--moisture", type=float, default=None)
    parser.add_argument("--temperature-c", dest="temperature_c", type=float, default=None)

    parser.add_argument("--inoculation-strength", dest="inoculation_strength", type=float, default=None)
    parser.add_argument("--amendment-strength", dest="amendment_strength", type=float, default=None)
    parser.add_argument("--management-shift", dest="management_shift", type=float, default=None)


def _collect_overrides(args: argparse.Namespace) -> Dict[str, Dict[str, float | None]]:
    return {
        "community": {
            "diazotrophs": args.diazotrophs,
            "decomposers": args.decomposers,
            "competitors": args.competitors,
            "stress_tolerant_taxa": args.stress_tolerant_taxa,
        },
        "environment": {
            "soil_ph": args.soil_ph,
            "organic_matter_pct": args.organic_matter_pct,
            "moisture": args.moisture,
            "temperature_c": args.temperature_c,
        },
        "intervention": {
            "inoculation_strength": args.inoculation_strength,
            "amendment_strength": args.amendment_strength,
            "management_shift": args.management_shift,
        },
    }


def _apply_overrides(base: object, overrides: Dict[str, float | None], cls: type[object]) -> object:
    data = dict(base.__dict__)
    for key, value in overrides.items():
        if value is not None:
            data[key] = value
    return cls(**data)


def _print_payload(payload: Dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    for key, value in payload.items():
        print(f"{key}: {value}")


def _run_train(args: argparse.Namespace) -> int:
    trained = train_surrogate(
        n_samples=args.samples,
        random_state=args.random_state,
        test_size=args.test_size,
    )
    saved = save_surrogate_artifacts(trained, output_dir=args.output_dir)
    payload = {
        "command": "train",
        "output_dir": str(Path(args.output_dir).resolve()),
        "saved_files": saved,
        "training_metrics": trained.metrics,
        "training_config": trained.training_config,
    }
    _print_payload(payload, as_json=args.json)
    return 0


def _run_eval(args: argparse.Namespace) -> int:
    loaded = load_surrogate_artifacts(args.artifacts_dir)
    metrics = evaluate_surrogate(
        loaded,
        n_samples=args.samples,
        random_state=args.random_state,
    )
    payload = {
        "command": "eval",
        "artifacts_dir": str(Path(args.artifacts_dir).resolve()),
        "evaluation_metrics": metrics,
        "training_metrics": loaded.metrics,
    }
    _print_payload(payload, as_json=args.json)
    return 0


def _run_predict(args: argparse.Namespace) -> int:
    scenarios = get_scenarios()
    if args.scenario not in scenarios:
        known = ", ".join(sorted(scenarios.keys()))
        raise SystemExit(f"Unknown scenario '{args.scenario}'. Known scenarios: {known}")

    overrides = _collect_overrides(args)
    scenario = scenarios[args.scenario]
    community = _apply_overrides(scenario.community, overrides["community"], Community)
    environment = _apply_overrides(scenario.environment, overrides["environment"], Environment)
    intervention = _apply_overrides(scenario.intervention, overrides["intervention"], Intervention)

    surrogate = load_surrogate_artifacts(args.artifacts_dir)
    surrogate_prediction = predict_with_surrogate(
        surrogate=surrogate,
        community=community,
        environment=environment,
        intervention=intervention,
    )

    payload: Dict[str, object] = {
        "command": "predict",
        "artifacts_dir": str(Path(args.artifacts_dir).resolve()),
        "scenario": args.scenario,
        "inputs": {
            "community": community.__dict__,
            "environment": environment.__dict__,
            "intervention": intervention.__dict__,
        },
        "surrogate_prediction": surrogate_prediction,
    }
    if args.include_truth:
        payload["sim_truth"] = simulate_dynamics(community, environment, intervention).to_dict()

    _print_payload(payload, as_json=args.json)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "train":
        return _run_train(args)
    if args.command == "eval":
        return _run_eval(args)
    if args.command == "predict":
        return _run_predict(args)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
