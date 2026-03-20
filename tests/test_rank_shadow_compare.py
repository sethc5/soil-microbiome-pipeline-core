from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.ops.rank_shadow_compare import compute_shadow_metrics


def _rows(ids: list[str]) -> list[dict]:
    return [{"community_id": cid, "rank": i + 1} for i, cid in enumerate(ids)]


def test_compute_shadow_metrics_reports_overlap_and_shift():
    legacy = _rows(["1", "2", "3", "4"])
    hybrid = _rows(["2", "1", "3", "5"])
    payload = compute_shadow_metrics(legacy, hybrid, top_k=3)

    assert payload["top_k_overlap"]["count"] == 3
    assert abs(payload["top_k_overlap"]["ratio"] - 1.0) < 1e-12
    assert abs(payload["top_k_overlap"]["jaccard"] - 1.0) < 1e-12
    assert abs(payload["rank_displacement"]["mean_abs_rank_shift"] - (2.0 / 3.0)) < 1e-12
    assert payload["rank_displacement"]["max_abs_rank_shift"] == 1


def test_compute_shadow_metrics_handles_disjoint_top_k():
    legacy = _rows(["1", "2", "3", "4"])
    hybrid = _rows(["8", "9", "10", "1"])
    payload = compute_shadow_metrics(legacy, hybrid, top_k=3)

    assert payload["top_k_overlap"]["count"] == 0
    assert payload["top_k_overlap"]["ratio"] == 0.0
    assert payload["top_k_overlap"]["jaccard"] == 0.0
    assert payload["rank_displacement"]["mean_abs_rank_shift"] == 3.0
    assert payload["rank_displacement"]["max_abs_rank_shift"] == 3
