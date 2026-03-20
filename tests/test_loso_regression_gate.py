from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from apps.bnf.scripts.loso_regression_gate import evaluate_loso_regression


def _base_config() -> dict:
    return {
        "baseline_loso_spearman_r": 0.1552,
        "min_loso_spearman_r": 0.12,
        "max_allowed_drop_from_baseline": 0.03,
        "min_sites": 40,
    }


def test_loso_regression_passes_when_above_thresholds():
    report = {"loso_spearman_r": 0.14, "n_sites": 47}
    payload = evaluate_loso_regression(report, _base_config())
    assert payload["passed"] is True
    assert payload["failures"] == []


def test_loso_regression_fails_on_large_drop():
    report = {"loso_spearman_r": 0.10, "n_sites": 47}
    payload = evaluate_loso_regression(report, _base_config())
    assert payload["passed"] is False
    assert any("baseline_drop" in f for f in payload["failures"])


def test_loso_regression_fails_on_low_site_count():
    report = {"loso_spearman_r": 0.14, "n_sites": 25}
    payload = evaluate_loso_regression(report, _base_config())
    assert payload["passed"] is False
    assert any("n_sites" in f for f in payload["failures"])
