from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.benchmark import append_benchmark_history, load_benchmark_history, run_ranking_benchmark
from sim_model.benchmark_cli import main as benchmark_cli_main


def test_benchmark_reports_positive_funnel_lift():
    result = run_ranking_benchmark(
        n_worlds=120,
        n_candidates=10,
        top_k=3,
        random_state=7,
    )
    lifts = result["lifts"]

    assert lifts["funnel_vs_random_top1_lift"] > 0.0
    assert lifts["funnel_vs_random_topk_lift"] > 0.0
    assert lifts["funnel_vs_random_regret_reduction"] > 0.0


def test_benchmark_history_append_and_load(tmp_path):
    history_path = tmp_path / "sim_benchmark_history.jsonl"
    result = run_ranking_benchmark(n_worlds=50, n_candidates=8, top_k=2, random_state=11)

    entry = append_benchmark_history(result, history_path)
    loaded = load_benchmark_history(history_path)

    assert history_path.exists()
    assert len(loaded) == 1
    assert loaded[0]["run_timestamp_utc"] == entry["run_timestamp_utc"]
    assert "funnel_vs_random_top1_lift" in loaded[0]["lifts"]


def test_benchmark_cli_run_and_history(tmp_path):
    history_path = tmp_path / "history.jsonl"
    run_out = io.StringIO()
    with redirect_stdout(run_out):
        code = benchmark_cli_main(
            [
                "run",
                "--worlds",
                "40",
                "--candidates",
                "8",
                "--top-k",
                "2",
                "--seed",
                "99",
                "--history-path",
                str(history_path),
                "--json",
            ]
        )
    assert code == 0
    run_payload = json.loads(run_out.getvalue())
    assert run_payload["history_appended"]["path"].endswith("history.jsonl")

    hist_out = io.StringIO()
    with redirect_stdout(hist_out):
        code = benchmark_cli_main(
            [
                "history",
                "--history-path",
                str(history_path),
                "--last",
                "1",
                "--json",
            ]
        )
    assert code == 0
    hist_payload = json.loads(hist_out.getvalue())
    assert hist_payload["entries_total"] == 1
    assert "avg_funnel_vs_random_top1_lift" in hist_payload
