from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.benchmark_gate import GateConfig, GateThresholds, evaluate_benchmark_gate, main


def test_benchmark_gate_passes_with_reasonable_thresholds():
    config = GateConfig(
        seeds=[7, 13],
        worlds=80,
        candidates=10,
        top_k=3,
        thresholds=GateThresholds(
            min_top1_lift=0.02,
            min_topk_lift=0.01,
            min_regret_reduction=0.20,
            min_hit_rate_margin=0.10,
        ),
    )
    payload = evaluate_benchmark_gate(config)

    assert payload["passed"] is True
    assert payload["aggregate"]["min_top1_lift"] >= 0.02


def test_benchmark_gate_fails_with_unrealistic_thresholds():
    config = GateConfig(
        seeds=[7],
        worlds=50,
        candidates=8,
        top_k=2,
        thresholds=GateThresholds(
            min_top1_lift=0.50,
            min_topk_lift=0.40,
            min_regret_reduction=0.95,
            min_hit_rate_margin=0.95,
        ),
    )
    payload = evaluate_benchmark_gate(config)

    assert payload["passed"] is False
    assert len(payload["failures"]) >= 1


def test_benchmark_gate_cli_json():
    out = io.StringIO()
    with redirect_stdout(out):
        rc = main(
            [
                "--seeds",
                "7",
                "--worlds",
                "40",
                "--candidates",
                "8",
                "--top-k",
                "2",
                "--min-top1-lift",
                "0.01",
                "--min-topk-lift",
                "0.01",
                "--min-regret-reduction",
                "0.10",
                "--min-hit-rate-margin",
                "0.05",
                "--json",
            ]
        )
    payload = json.loads(out.getvalue())

    assert rc == 0
    assert payload["passed"] is True
    assert "aggregate" in payload
