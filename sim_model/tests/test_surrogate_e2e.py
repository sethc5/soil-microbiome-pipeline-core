from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.schema import Community, Environment, Intervention
from sim_model.surrogate import evaluate_surrogate, load_surrogate_artifacts, predict_with_surrogate
from sim_model.surrogate_cli import main as surrogate_cli_main


def test_surrogate_train_save_load_eval_predict_e2e(tmp_path):
    artifact_dir = tmp_path / "artifacts"

    train_buffer = io.StringIO()
    with redirect_stdout(train_buffer):
        code = surrogate_cli_main(
            [
                "train",
                "--samples",
                "800",
                "--random-state",
                "17",
                "--output-dir",
                str(artifact_dir),
                "--json",
            ]
        )
    assert code == 0
    train_payload = json.loads(train_buffer.getvalue())
    assert Path(train_payload["saved_files"]["regressor"]).exists()
    assert Path(train_payload["saved_files"]["classifier"]).exists()
    assert Path(train_payload["saved_files"]["metadata"]).exists()

    loaded = load_surrogate_artifacts(artifact_dir)
    eval_metrics = evaluate_surrogate(loaded, n_samples=350, random_state=23)
    assert eval_metrics["r2_target_flux"] >= 0.30
    assert eval_metrics["r2_stability_score"] >= 0.20
    assert eval_metrics["r2_establishment_probability"] >= 0.10
    assert eval_metrics["best_intervention_accuracy"] >= 0.55

    prediction = predict_with_surrogate(
        surrogate=loaded,
        community=Community(0.58, 0.36, 0.22, 0.30),
        environment=Environment(6.7, 5.6, 0.61, 24.2),
        intervention=Intervention(0.45, 0.25, 0.20),
    )
    assert prediction["predicted_best_intervention_class"] in loaded.class_labels
    assert pytest.approx(sum(prediction["class_probabilities"].values()), abs=1e-9) == 1.0
