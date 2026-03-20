"""Small executable simulation model for pipeline intent validation."""

from .adapter import SimInputBundle, map_pipeline_record_to_sim_inputs, simulate_from_pipeline_record
from .benchmark import (
    append_benchmark_history,
    load_benchmark_history,
    run_ranking_benchmark,
)
from .dynamics import simulate_dynamics
from .schema import Community, Environment, Intervention, SimulationResult

__all__ = [
    "SimInputBundle",
    "Community",
    "Environment",
    "Intervention",
    "SimulationResult",
    "simulate_dynamics",
    "map_pipeline_record_to_sim_inputs",
    "simulate_from_pipeline_record",
    "run_ranking_benchmark",
    "append_benchmark_history",
    "load_benchmark_history",
    "load_calibration_config",
    "evaluate_calibration_config",
    "run_calibration",
]


def load_calibration_config(*args, **kwargs):
    from .calibration import load_calibration_config as _fn

    return _fn(*args, **kwargs)


def evaluate_calibration_config(*args, **kwargs):
    from .calibration import evaluate_calibration_config as _fn

    return _fn(*args, **kwargs)


def run_calibration(*args, **kwargs):
    from .calibration import run_calibration as _fn

    return _fn(*args, **kwargs)


try:
    from .surrogate import (
        evaluate_surrogate,
        load_surrogate_artifacts,
        predict_with_surrogate,
        save_surrogate_artifacts,
        train_surrogate,
    )

    __all__.extend(
        [
            "train_surrogate",
            "evaluate_surrogate",
            "save_surrogate_artifacts",
            "load_surrogate_artifacts",
            "predict_with_surrogate",
        ]
    )
except ModuleNotFoundError:
    # Keep the core sim importable when optional ML deps are unavailable.
    pass
