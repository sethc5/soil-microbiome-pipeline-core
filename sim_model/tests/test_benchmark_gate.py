from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.benchmark_gate import (
    GateConfig,
    GateThresholds,
    TrendConfig,
    TrendThresholds,
    append_gate_history,
    evaluate_benchmark_gate,
    main,
    render_markdown_summary,
    write_report_artifacts,
)


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


def test_benchmark_gate_trend_passes_against_similar_history(tmp_path):
    history_path = tmp_path / "history.jsonl"
    baseline_entry = {
        "aggregate": {
            "avg_top1_lift": 0.09,
            "avg_topk_lift": 0.07,
            "avg_regret_reduction": 0.80,
            "avg_hit_rate_margin": 0.45,
        }
    }
    history_path.write_text(json.dumps(baseline_entry) + "\n", encoding="utf-8")

    config = GateConfig(
        seeds=[7],
        worlds=60,
        candidates=9,
        top_k=3,
        thresholds=GateThresholds(
            min_top1_lift=0.01,
            min_topk_lift=0.01,
            min_regret_reduction=0.1,
            min_hit_rate_margin=0.05,
        ),
        trend=TrendConfig(
            history_path=str(history_path),
            trend_window=5,
            min_history_entries=1,
            fail_on_missing_history=True,
            thresholds=TrendThresholds(
                max_top1_lift_drop=0.10,
                max_topk_lift_drop=0.10,
                max_regret_reduction_drop=0.20,
                max_hit_rate_margin_drop=0.20,
            ),
        ),
    )
    payload = evaluate_benchmark_gate(config)
    assert payload["trend"]["passed"] is True
    assert payload["passed"] is True


def test_benchmark_gate_trend_fails_for_large_drop(tmp_path):
    history_path = tmp_path / "history.jsonl"
    strict_baseline = {
        "aggregate": {
            "avg_top1_lift": 0.30,
            "avg_topk_lift": 0.25,
            "avg_regret_reduction": 0.99,
            "avg_hit_rate_margin": 0.90,
        }
    }
    history_path.write_text(json.dumps(strict_baseline) + "\n", encoding="utf-8")

    config = GateConfig(
        seeds=[7],
        worlds=60,
        candidates=9,
        top_k=3,
        thresholds=GateThresholds(
            min_top1_lift=0.01,
            min_topk_lift=0.01,
            min_regret_reduction=0.1,
            min_hit_rate_margin=0.05,
        ),
        trend=TrendConfig(
            history_path=str(history_path),
            trend_window=5,
            min_history_entries=1,
            fail_on_missing_history=True,
            thresholds=TrendThresholds(
                max_top1_lift_drop=0.01,
                max_topk_lift_drop=0.01,
                max_regret_reduction_drop=0.01,
                max_hit_rate_margin_drop=0.01,
            ),
        ),
    )
    payload = evaluate_benchmark_gate(config)
    assert payload["trend"]["passed"] is False
    assert payload["passed"] is False
    assert len(payload["trend"]["failures"]) >= 1


def test_append_gate_history(tmp_path):
    history_path = tmp_path / "gate_history.jsonl"
    payload = {
        "passed": True,
        "gate_config": {"seeds": [7]},
        "aggregate": {"avg_top1_lift": 0.11},
        "trend": {"passed": True},
    }
    entry = append_gate_history(payload, history_path)
    assert history_path.exists()
    text = history_path.read_text(encoding="utf-8").strip()
    loaded = json.loads(text)
    assert loaded["aggregate"]["avg_top1_lift"] == 0.11
    assert loaded["run_timestamp_utc"] == entry["run_timestamp_utc"]


def test_write_report_artifacts(tmp_path):
    payload = {
        "passed": True,
        "gate_config": {
            "worlds": 100,
            "candidates": 10,
            "top_k": 3,
            "seeds": [7, 13],
            "thresholds": {
                "min_top1_lift": 0.03,
                "min_topk_lift": 0.02,
                "min_regret_reduction": 0.25,
                "min_hit_rate_margin": 0.15,
            },
            "trend": {"thresholds": {}},
        },
        "aggregate": {
            "avg_top1_lift": 0.12,
            "avg_topk_lift": 0.09,
            "avg_regret_reduction": 0.93,
            "avg_hit_rate_margin": 0.54,
        },
        "trend": {"enabled": False, "passed": True},
        "failures": [],
    }

    json_path = tmp_path / "latest.json"
    md_path = tmp_path / "summary.md"
    outputs = write_report_artifacts(payload, report_json_path=json_path, report_md_path=md_path)
    assert "json" in outputs
    assert "markdown" in outputs
    assert json_path.exists()
    assert md_path.exists()

    loaded_json = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded_json["aggregate"]["avg_top1_lift"] == 0.12
    md_text = md_path.read_text(encoding="utf-8")
    assert "Sim Model Benchmark Gate" in md_text
    assert "PASS" in md_text


def test_render_markdown_summary_includes_failures():
    payload = {
        "passed": False,
        "gate_config": {
            "worlds": 50,
            "candidates": 8,
            "top_k": 2,
            "seeds": [7],
            "thresholds": {
                "min_top1_lift": 0.03,
                "min_topk_lift": 0.02,
                "min_regret_reduction": 0.25,
                "min_hit_rate_margin": 0.15,
            },
            "trend": {"thresholds": {}},
        },
        "aggregate": {
            "avg_top1_lift": 0.01,
            "avg_topk_lift": 0.01,
            "avg_regret_reduction": 0.1,
            "avg_hit_rate_margin": 0.02,
        },
        "trend": {"enabled": False, "passed": True},
        "failures": ["seed 7: funnel_vs_random_top1_lift=0.0100 < 0.0300"],
    }
    md = render_markdown_summary(payload)
    assert "FAIL" in md
    assert "Failures" in md
    assert "seed 7" in md
