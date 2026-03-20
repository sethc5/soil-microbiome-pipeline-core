from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Sequence

from .dynamics import simulate_dynamics
from .scenarios import get_scenario, get_scenarios
from .schema import Community, Environment, Intervention


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run toy soil microbiome simulations.")
    parser.add_argument("--scenario", type=str, default="easy_win", help="Scenario name to run.")
    parser.add_argument(
        "--list-scenarios",
        action="store_true",
        help="List built-in scenarios and exit.",
    )
    parser.add_argument("--json", action="store_true", help="Print output as JSON.")

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
    parser.add_argument(
        "--note",
        type=str,
        default=None,
        help="Optional note to include in output (useful for custom runs).",
    )
    return parser


def _print_human_readable(result: dict) -> None:
    print(f"target_flux: {result['target_flux']:.4f}")
    print(f"stability_score: {result['stability_score']:.4f}")
    print(f"establishment_probability: {result['establishment_probability']:.4f}")
    print(f"best_intervention_class: {result['best_intervention_class']}")


def _apply_overrides(base: Any, overrides: Dict[str, float | None], cls: Any) -> Any:
    data = dict(base.__dict__)
    for key, value in overrides.items():
        if value is not None:
            data[key] = value
    return cls(**data)


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


def run_simulation_for_scenario(scenario_name: str) -> dict:
    scenario = get_scenario(scenario_name)
    return run_simulation(
        scenario_name=scenario.name,
        community_overrides={},
        environment_overrides={},
        intervention_overrides={},
        note_override=scenario.note,
    )


def run_simulation(
    scenario_name: str,
    community_overrides: Dict[str, float | None],
    environment_overrides: Dict[str, float | None],
    intervention_overrides: Dict[str, float | None],
    note_override: str | None = None,
) -> dict:
    scenario = get_scenario(scenario_name)
    community = _apply_overrides(scenario.community, community_overrides, Community)
    environment = _apply_overrides(scenario.environment, environment_overrides, Environment)
    intervention = _apply_overrides(scenario.intervention, intervention_overrides, Intervention)

    result = simulate_dynamics(
        community=community,
        environment=environment,
        intervention=intervention,
    )

    applied_overrides = sorted(
        [
            key
            for group in [community_overrides, environment_overrides, intervention_overrides]
            for key, value in group.items()
            if value is not None
        ]
    )

    output = result.to_dict()
    output["scenario"] = scenario.name
    output["note"] = note_override if note_override is not None else scenario.note
    output["inputs"] = {
        "community": community.__dict__,
        "environment": environment.__dict__,
        "intervention": intervention.__dict__,
    }
    output["applied_overrides"] = applied_overrides
    return output


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    scenarios = get_scenarios()

    if args.list_scenarios:
        for name in sorted(scenarios.keys()):
            print(f"{name}: {scenarios[name].note}")
        return 0

    if args.scenario not in scenarios:
        parser.error(
            f"Unknown scenario '{args.scenario}'. "
            f"Known scenarios: {', '.join(sorted(scenarios.keys()))}"
        )

    overrides = _collect_overrides(args)
    output = run_simulation(
        scenario_name=args.scenario,
        community_overrides=overrides["community"],
        environment_overrides=overrides["environment"],
        intervention_overrides=overrides["intervention"],
        note_override=args.note,
    )
    if args.json:
        print(json.dumps(output, indent=2, sort_keys=True))
    else:
        _print_human_readable(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
