from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.calibration import evaluate_calibration_config, main


def _base_config() -> dict:
    return {
        "defaults": {
            "community": {
                "diazotrophs": 0.45,
                "decomposers": 0.38,
                "competitors": 0.22,
                "stress_tolerant_taxa": 0.32,
            },
            "environment": {
                "soil_ph": 6.8,
                "organic_matter_pct": 5.2,
                "moisture": 0.62,
                "temperature_c": 24.0,
            },
            "intervention": {
                "inoculation_strength": 0.35,
                "amendment_strength": 0.25,
                "management_shift": 0.10,
            },
        },
        "drift_thresholds": {"max_failed_checks": 0, "min_pass_rate": 1.0},
        "checks": [
            {
                "id": "diazotroph_flux_monotonic",
                "type": "monotonic_sweep",
                "sweep_path": "community.diazotrophs",
                "values": [0.10, 0.30, 0.50, 0.70],
                "metric": "target_flux",
                "direction": "increasing",
                "min_total_change": 5.0,
            },
            {
                "id": "ph_band_flux_penalty",
                "type": "band_comparison",
                "sweep_path": "environment.soil_ph",
                "low_values": [4.2, 4.6, 5.0],
                "high_values": [6.4, 6.8, 7.2],
                "metric": "target_flux",
                "expectation": "high_gt_low",
                "min_ratio": 1.5,
            },
        ],
    }


def test_evaluate_calibration_config_passes():
    payload = evaluate_calibration_config(_base_config())
    assert payload["passed"] is True
    assert payload["summary"]["failed_checks"] == 0


def test_evaluate_calibration_config_fails_with_unrealistic_threshold():
    config = _base_config()
    config["checks"][0]["min_total_change"] = 300.0
    payload = evaluate_calibration_config(config)
    assert payload["passed"] is False
    assert payload["summary"]["failed_checks"] >= 1


def test_calibration_cli_with_json_config(tmp_path):
    config_path = tmp_path / "calibration.json"
    config_path.write_text(json.dumps(_base_config()), encoding="utf-8")

    out = io.StringIO()
    with redirect_stdout(out):
        code = main(["--config", str(config_path), "--json"])

    assert code == 0
    payload = json.loads(out.getvalue())
    assert payload["passed"] is True
    assert payload["summary"]["failed_checks"] == 0
