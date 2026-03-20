from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .dynamics import simulate_dynamics
from .schema import Community, Environment, Intervention


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _gaussian_response(value: float, center: float, sigma: float) -> float:
    return math.exp(-((value - center) ** 2) / (2.0 * sigma * sigma))


@dataclass(frozen=True)
class CandidateRecord:
    community: Community
    environment: Environment
    intervention: Intervention
    true_score: float
    true_target_flux: float
    true_stability: float
    true_establishment: float


def _sample_community(rng: random.Random) -> Community:
    return Community(
        diazotrophs=rng.uniform(0.05, 0.85),
        decomposers=rng.uniform(0.05, 0.85),
        competitors=rng.uniform(0.05, 0.85),
        stress_tolerant_taxa=rng.uniform(0.05, 0.85),
    )


def _sample_environment(rng: random.Random) -> Environment:
    return Environment(
        soil_ph=rng.uniform(4.1, 8.5),
        organic_matter_pct=rng.uniform(0.2, 13.5),
        moisture=rng.uniform(0.08, 0.92),
        temperature_c=rng.uniform(7.0, 35.0),
    )


def _sample_intervention(rng: random.Random) -> Intervention:
    return Intervention(
        inoculation_strength=rng.uniform(0.0, 1.0),
        amendment_strength=rng.uniform(0.0, 1.0),
        management_shift=rng.uniform(-1.0, 1.0),
    )


def _true_score(target_flux: float, stability: float, establishment: float) -> float:
    flux_norm = _clamp(math.log1p(max(target_flux, 0.0)) / math.log1p(100.0), 0.0, 1.0)
    return flux_norm * _clamp(stability, 0.0, 1.0) * _clamp(establishment, 0.0, 1.0)


def _funnel_predicted_score(
    community: Community,
    environment: Environment,
    intervention: Intervention,
) -> float:
    ph_factor = _gaussian_response(environment.soil_ph, 6.8, 1.4)
    moisture_factor = _gaussian_response(environment.moisture, 0.62, 0.24)
    temperature_factor = _gaussian_response(environment.temperature_c, 24.0, 9.0)
    om_support = environment.organic_matter_pct / (environment.organic_matter_pct + 4.0)
    env_suitability = ph_factor * moisture_factor * temperature_factor * om_support

    base_flux = _clamp(
        0.20
        + 1.0 * community.diazotrophs
        + 0.25 * community.decomposers
        + 0.10 * community.stress_tolerant_taxa
        - 0.45 * community.competitors,
        0.0,
        1.8,
    )
    intervention_gain = _clamp(
        1.0
        + 0.18 * intervention.inoculation_strength * (0.5 + ph_factor)
        + 0.14 * intervention.amendment_strength * (1.0 - 0.6 * environment.moisture)
        + 0.10 * intervention.management_shift * (community.stress_tolerant_taxa - 0.5 * community.competitors),
        0.3,
        1.8,
    )
    predicted_flux_norm = _clamp(env_suitability * base_flux * intervention_gain, 0.0, 1.0)

    predicted_stability = _clamp(
        0.45
        + 0.35 * community.stress_tolerant_taxa
        + 0.10 * community.decomposers
        - 0.40 * community.competitors
        + 0.20 * intervention.management_shift
        - 0.15 * abs(environment.moisture - 0.62),
        0.0,
        1.0,
    )

    compatibility = _clamp((ph_factor + moisture_factor + temperature_factor) / 3.0, 0.0, 1.0)
    confidence = _clamp(
        0.40
        + 0.45 * compatibility
        + 0.20 * intervention.inoculation_strength
        - 0.15 * community.competitors,
        0.05,
        1.0,
    )
    return predicted_flux_norm * predicted_stability * confidence


def _heuristic_predicted_score(
    community: Community,
    environment: Environment,
    intervention: Intervention,
) -> float:
    _ = environment
    return _clamp(
        0.55 * intervention.inoculation_strength
        + 0.20 * intervention.amendment_strength
        + 0.10 * (intervention.management_shift + 1.0) / 2.0
        + 0.10 * community.diazotrophs
        - 0.10 * community.competitors,
        0.0,
        1.0,
    )


def _average(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _safe_div(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-12:
        return 0.0
    return numerator / denominator


def _build_world(rng: random.Random, n_candidates: int) -> List[CandidateRecord]:
    community = _sample_community(rng)
    environment = _sample_environment(rng)
    world: List[CandidateRecord] = []

    for _ in range(n_candidates):
        intervention = _sample_intervention(rng)
        result = simulate_dynamics(community, environment, intervention)
        true_score = _true_score(
            target_flux=result.target_flux,
            stability=result.stability_score,
            establishment=result.establishment_probability,
        )
        world.append(
            CandidateRecord(
                community=community,
                environment=environment,
                intervention=intervention,
                true_score=true_score,
                true_target_flux=result.target_flux,
                true_stability=result.stability_score,
                true_establishment=result.establishment_probability,
            )
        )
    return world


def _evaluate_world(
    world: List[CandidateRecord],
    top_k: int,
    rng: random.Random,
) -> Dict[str, Dict[str, float]]:
    ordered_truth = sorted(world, key=lambda item: item.true_score, reverse=True)
    best_true = ordered_truth[0].true_score
    k = min(top_k, len(world))

    def _scores_for_method(name: str) -> List[float]:
        if name == "oracle":
            ranked = ordered_truth
        elif name == "funnel":
            ranked = sorted(
                world,
                key=lambda item: _funnel_predicted_score(item.community, item.environment, item.intervention),
                reverse=True,
            )
        elif name == "heuristic":
            ranked = sorted(
                world,
                key=lambda item: _heuristic_predicted_score(item.community, item.environment, item.intervention),
                reverse=True,
            )
        elif name == "random":
            ranked = list(world)
            rng.shuffle(ranked)
        else:
            raise ValueError(f"Unknown method: {name}")
        return [item.true_score for item in ranked]

    methods = ["oracle", "funnel", "heuristic", "random"]
    out: Dict[str, Dict[str, float]] = {}
    for method in methods:
        ranked_scores = _scores_for_method(method)
        top1 = ranked_scores[0]
        topk_avg = _average(ranked_scores[:k])
        out[method] = {
            "top1_true_score": top1,
            "topk_avg_true_score": topk_avg,
            "top1_regret": best_true - top1,
            "hit_optimal": 1.0 if abs(top1 - best_true) < 1e-12 else 0.0,
        }
    return out


def run_ranking_benchmark(
    n_worlds: int = 200,
    n_candidates: int = 12,
    top_k: int = 3,
    random_state: int = 42,
) -> Dict[str, Any]:
    if n_worlds <= 0:
        raise ValueError("n_worlds must be > 0")
    if n_candidates < 2:
        raise ValueError("n_candidates must be >= 2")
    if top_k <= 0:
        raise ValueError("top_k must be > 0")

    rng = random.Random(random_state)
    per_method: Dict[str, Dict[str, List[float]]] = {
        "oracle": {"top1_true_score": [], "topk_avg_true_score": [], "top1_regret": [], "hit_optimal": []},
        "funnel": {"top1_true_score": [], "topk_avg_true_score": [], "top1_regret": [], "hit_optimal": []},
        "heuristic": {"top1_true_score": [], "topk_avg_true_score": [], "top1_regret": [], "hit_optimal": []},
        "random": {"top1_true_score": [], "topk_avg_true_score": [], "top1_regret": [], "hit_optimal": []},
    }

    for _ in range(n_worlds):
        world = _build_world(rng, n_candidates=n_candidates)
        world_metrics = _evaluate_world(world, top_k=top_k, rng=rng)
        for method, metrics in world_metrics.items():
            for key, value in metrics.items():
                per_method[method][key].append(value)

    summary: Dict[str, Dict[str, float]] = {}
    for method, metric_lists in per_method.items():
        summary[method] = {key: _average(values) for key, values in metric_lists.items()}

    random_top1 = summary["random"]["top1_true_score"]
    random_topk = summary["random"]["topk_avg_true_score"]
    random_regret = summary["random"]["top1_regret"]

    lifts = {
        "funnel_vs_random_top1_lift": _safe_div(
            summary["funnel"]["top1_true_score"] - random_top1,
            random_top1,
        ),
        "funnel_vs_random_topk_lift": _safe_div(
            summary["funnel"]["topk_avg_true_score"] - random_topk,
            random_topk,
        ),
        "funnel_vs_random_regret_reduction": _safe_div(
            random_regret - summary["funnel"]["top1_regret"],
            random_regret,
        ),
        "heuristic_vs_random_top1_lift": _safe_div(
            summary["heuristic"]["top1_true_score"] - random_top1,
            random_top1,
        ),
    }

    return {
        "config": {
            "n_worlds": n_worlds,
            "n_candidates": n_candidates,
            "top_k": top_k,
            "random_state": random_state,
        },
        "summary": summary,
        "lifts": lifts,
    }


def append_benchmark_history(
    benchmark_result: Dict[str, Any],
    history_path: str | Path,
) -> Dict[str, Any]:
    entry = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "config": benchmark_result.get("config", {}),
        "lifts": benchmark_result.get("lifts", {}),
        "summary": benchmark_result.get("summary", {}),
    }
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True))
        handle.write("\n")
    return entry


def load_benchmark_history(history_path: str | Path) -> List[Dict[str, Any]]:
    path = Path(history_path)
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows
