from __future__ import annotations

import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

from .dynamics import simulate_dynamics
from .schema import Community, Environment, Intervention

FEATURE_NAMES = [
    "diazotrophs",
    "decomposers",
    "competitors",
    "stress_tolerant_taxa",
    "soil_ph",
    "organic_matter_pct",
    "moisture",
    "temperature_c",
    "inoculation_strength",
    "amendment_strength",
    "management_shift",
]

CLASS_LABELS = ["none", "inoculation", "amendment", "management"]
REGRESSOR_FILENAME = "surrogate_regressor.joblib"
CLASSIFIER_FILENAME = "surrogate_classifier.joblib"
METADATA_FILENAME = "surrogate_metadata.json"


@dataclass(frozen=True)
class SurrogateTrainingResult:
    regressor: RandomForestRegressor
    classifier: RandomForestClassifier
    feature_names: List[str]
    class_labels: List[str]
    metrics: Dict[str, float]
    training_config: Dict[str, Any]


def _sample_case(rng: random.Random) -> Tuple[Community, Environment, Intervention]:
    community = Community(
        diazotrophs=rng.uniform(0.02, 0.85),
        decomposers=rng.uniform(0.02, 0.85),
        competitors=rng.uniform(0.02, 0.85),
        stress_tolerant_taxa=rng.uniform(0.02, 0.85),
    )
    environment = Environment(
        soil_ph=rng.uniform(3.8, 8.8),
        organic_matter_pct=rng.uniform(0.1, 14.0),
        moisture=rng.uniform(0.05, 0.95),
        temperature_c=rng.uniform(5.0, 36.0),
    )
    intervention = Intervention(
        inoculation_strength=rng.uniform(0.0, 1.0),
        amendment_strength=rng.uniform(0.0, 1.0),
        management_shift=rng.uniform(-1.0, 1.0),
    )
    return community, environment, intervention


def _to_features(community: Community, environment: Environment, intervention: Intervention) -> List[float]:
    return [
        community.diazotrophs,
        community.decomposers,
        community.competitors,
        community.stress_tolerant_taxa,
        environment.soil_ph,
        environment.organic_matter_pct,
        environment.moisture,
        environment.temperature_c,
        intervention.inoculation_strength,
        intervention.amendment_strength,
        intervention.management_shift,
    ]


def generate_synthetic_dataset(
    n_samples: int = 1000,
    random_state: int = 42,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = random.Random(random_state)
    features: List[List[float]] = []
    targets: List[List[float]] = []
    classes: List[int] = []

    label_to_index = {label: idx for idx, label in enumerate(CLASS_LABELS)}

    for _ in range(n_samples):
        community, environment, intervention = _sample_case(rng)
        result = simulate_dynamics(community, environment, intervention)
        features.append(_to_features(community, environment, intervention))
        targets.append(
            [
                result.target_flux,
                result.stability_score,
                result.establishment_probability,
            ]
        )
        classes.append(label_to_index[result.best_intervention_class])

    return np.asarray(features), np.asarray(targets), np.asarray(classes)


def _compute_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    c_true: np.ndarray,
    c_pred: np.ndarray,
) -> Dict[str, float]:
    return {
        "r2_target_flux": float(r2_score(y_true[:, 0], y_pred[:, 0])),
        "r2_stability_score": float(r2_score(y_true[:, 1], y_pred[:, 1])),
        "r2_establishment_probability": float(r2_score(y_true[:, 2], y_pred[:, 2])),
        "mae_target_flux": float(mean_absolute_error(y_true[:, 0], y_pred[:, 0])),
        "mae_stability_score": float(mean_absolute_error(y_true[:, 1], y_pred[:, 1])),
        "mae_establishment_probability": float(mean_absolute_error(y_true[:, 2], y_pred[:, 2])),
        "best_intervention_accuracy": float((c_pred == c_true).mean()),
    }


def train_surrogate(
    n_samples: int = 1500,
    random_state: int = 42,
    test_size: float = 0.20,
) -> SurrogateTrainingResult:
    features, targets, classes = generate_synthetic_dataset(
        n_samples=n_samples,
        random_state=random_state,
    )
    x_train, x_test, y_train, y_test, c_train, c_test = train_test_split(
        features,
        targets,
        classes,
        test_size=test_size,
        random_state=random_state,
    )

    regressor = RandomForestRegressor(
        n_estimators=300,
        min_samples_leaf=3,
        random_state=random_state,
        n_jobs=-1,
    )
    regressor.fit(x_train, y_train)
    y_pred = regressor.predict(x_test)

    classifier = RandomForestClassifier(
        n_estimators=300,
        min_samples_leaf=2,
        random_state=random_state,
        n_jobs=-1,
    )
    classifier.fit(x_train, c_train)
    c_pred = classifier.predict(x_test)

    metrics = _compute_metrics(y_true=y_test, y_pred=y_pred, c_true=c_test, c_pred=c_pred)

    return SurrogateTrainingResult(
        regressor=regressor,
        classifier=classifier,
        feature_names=list(FEATURE_NAMES),
        class_labels=list(CLASS_LABELS),
        metrics=metrics,
        training_config={
            "n_samples": n_samples,
            "random_state": random_state,
            "test_size": test_size,
        },
    )


def evaluate_surrogate(
    surrogate: SurrogateTrainingResult,
    n_samples: int = 500,
    random_state: int = 1337,
) -> Dict[str, float]:
    features, targets, classes = generate_synthetic_dataset(
        n_samples=n_samples,
        random_state=random_state,
    )
    y_pred = surrogate.regressor.predict(features)
    c_pred = surrogate.classifier.predict(features)
    return _compute_metrics(y_true=targets, y_pred=y_pred, c_true=classes, c_pred=c_pred)


def save_surrogate_artifacts(
    surrogate: SurrogateTrainingResult,
    output_dir: str | Path,
) -> Dict[str, str]:
    artifact_dir = Path(output_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    regressor_path = artifact_dir / REGRESSOR_FILENAME
    classifier_path = artifact_dir / CLASSIFIER_FILENAME
    metadata_path = artifact_dir / METADATA_FILENAME

    joblib.dump(surrogate.regressor, regressor_path)
    joblib.dump(surrogate.classifier, classifier_path)

    metadata = {
        "feature_names": surrogate.feature_names,
        "class_labels": surrogate.class_labels,
        "training_metrics": surrogate.metrics,
        "training_config": surrogate.training_config,
        "artifact_version": 1,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "regressor": str(regressor_path),
        "classifier": str(classifier_path),
        "metadata": str(metadata_path),
    }


def load_surrogate_artifacts(artifact_dir: str | Path) -> SurrogateTrainingResult:
    base = Path(artifact_dir)
    regressor_path = base / REGRESSOR_FILENAME
    classifier_path = base / CLASSIFIER_FILENAME
    metadata_path = base / METADATA_FILENAME

    if not regressor_path.exists() or not classifier_path.exists() or not metadata_path.exists():
        raise FileNotFoundError(
            "Missing surrogate artifacts. "
            f"Expected {regressor_path}, {classifier_path}, and {metadata_path}."
        )

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    feature_names = metadata.get("feature_names", list(FEATURE_NAMES))
    class_labels = metadata.get("class_labels", list(CLASS_LABELS))
    training_metrics = metadata.get("training_metrics", {})
    training_config = metadata.get("training_config", {})

    return SurrogateTrainingResult(
        regressor=joblib.load(regressor_path),
        classifier=joblib.load(classifier_path),
        feature_names=list(feature_names),
        class_labels=list(class_labels),
        metrics=dict(training_metrics),
        training_config=dict(training_config),
    )


def predict_with_surrogate(
    surrogate: SurrogateTrainingResult,
    community: Community,
    environment: Environment,
    intervention: Intervention,
) -> Dict[str, Any]:
    features = np.asarray([_to_features(community, environment, intervention)])
    reg_prediction = surrogate.regressor.predict(features)[0]

    predicted_class_id = int(surrogate.classifier.predict(features)[0])
    class_label = surrogate.class_labels[predicted_class_id]

    probabilities = surrogate.classifier.predict_proba(features)[0]
    class_probabilities = {label: 0.0 for label in surrogate.class_labels}
    for idx, class_id in enumerate(surrogate.classifier.classes_):
        label = surrogate.class_labels[int(class_id)]
        class_probabilities[label] = float(probabilities[idx])

    return {
        "predicted_target_flux": float(reg_prediction[0]),
        "predicted_stability_score": float(reg_prediction[1]),
        "predicted_establishment_probability": float(reg_prediction[2]),
        "predicted_best_intervention_class": class_label,
        "class_probabilities": class_probabilities,
    }
