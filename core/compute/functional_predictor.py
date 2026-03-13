"""
compute/functional_predictor.py — T0.25 ML functional outcome prediction.

Trains or loads random forest / gradient boosting models that predict
target function scores (e.g. BNF rate, SOC accumulation) from:
  - OTU relative abundance features (log-ratio transformed — see README gotchas)
  - Environmental metadata features (pH, texture, climate)
  - Derived diversity metrics

Models are persisted as joblib files alongside the database.

Usage:
  from core.compute.functional_predictor import FunctionalPredictor
  predictor = FunctionalPredictor.load("models/random_forest_bnf.joblib")
  score, uncertainty = predictor.predict(community_feature_vector)
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# Pseudo-count for CLR transform (avoids log(0))
_CLR_PSEUDOCOUNT = 1e-9


def clr_transform(X: np.ndarray) -> np.ndarray:
    """Centered log-ratio transform for compositional OTU data.

    X: (n_samples, n_features) relative abundances in [0, 1].
    Returns CLR-transformed matrix of the same shape.
    """
    X = np.where(X == 0, _CLR_PSEUDOCOUNT, X)
    log_X = np.log(X)
    geometric_mean = log_X.mean(axis=1, keepdims=True)
    return log_X - geometric_mean


class FunctionalPredictor:
    """ML model for predicting target function scores from community + env features.

    Supported model_type values: 'random_forest', 'gradient_boost'.
    OTU features should be CLR-transformed before training (see clr_transform()).

    The canonical models/functional_predictor.joblib may also include a
    RandomForestClassifier gate (key ``classifier``) trained to screen obvious
    BNF non-passers at T0.25 before the regressor runs.  Use
    ``predict_with_gate()`` to get the full two-stage prediction.
    """

    VALID_MODEL_TYPES = ("random_forest", "gradient_boost")

    def __init__(self, model_type: str = "random_forest"):
        if model_type not in self.VALID_MODEL_TYPES:
            raise ValueError(
                f"model_type must be one of {self.VALID_MODEL_TYPES}, got {model_type!r}"
            )
        self.model_type = model_type
        self._model: Any = None  # scikit-learn estimator
        self._feature_names: list[str] = []
        self._apply_clr: bool = True  # whether OTU features need CLR transform
        self._classifier: Any = None  # optional pass/fail gate pipeline

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: list[str] | None = None,
        apply_clr: bool = True,
    ) -> "FunctionalPredictor":
        """Train on feature matrix X (n_samples × n_features) and target y.

        X: Combined OTU + environmental features. OTU columns are CLR-transformed
           if apply_clr=True (set False if already transformed upstream).
        y: Continuous functional score (e.g. BNF rate).

        Returns self for chaining.
        """
        try:
            from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
            from sklearn.pipeline import Pipeline
            from sklearn.preprocessing import StandardScaler
        except ImportError as exc:
            raise ImportError(
                "scikit-learn is required for FunctionalPredictor. "
                "Install via: pip install scikit-learn"
            ) from exc

        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float)
        self._apply_clr = apply_clr
        self._feature_names = feature_names or [f"feature_{i}" for i in range(X.shape[1])]

        X_processed = clr_transform(X) if apply_clr else X

        if self.model_type == "random_forest":
            estimator = RandomForestRegressor(
                n_estimators=200,
                max_features="sqrt",
                min_samples_leaf=2,
                random_state=42,
                n_jobs=-1,
            )
        else:  # gradient_boost
            estimator = GradientBoostingRegressor(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=4,
                subsample=0.8,
                random_state=42,
            )

        self._model = Pipeline([
            ("scaler", StandardScaler()),
            ("est", estimator),
        ])
        self._model.fit(X_processed, y)
        logger.info(
            "FunctionalPredictor trained: model=%s, n_samples=%d, n_features=%d",
            self.model_type, X.shape[0], X.shape[1],
        )
        return self

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(
        self,
        features: np.ndarray | list,
    ) -> tuple[float, float]:
        """Return (predicted_score, uncertainty_estimate) for a single sample.

        uncertainty_estimate is the std-dev of individual tree predictions
        (for RandomForest) or a proxy uncertainty (0.0) for GradientBoost.

        Does NOT apply the classifier gate — use predict_with_gate() for that.
        """
        if self._model is None:
            raise RuntimeError("No model loaded or trained — call train() or load() first.")

        X = np.asarray(features, dtype=float)
        if X.ndim == 1:
            X = X[np.newaxis, :]

        X_processed = clr_transform(X) if self._apply_clr else X

        # Use pipeline.predict() directly — handles any step names robustly
        point_estimate = float(self._model.predict(X_processed)[0])

        # Uncertainty via tree ensemble variance (RF only)
        # Access the last pipeline step regardless of its name
        estimator = list(self._model.named_steps.values())[-1]
        uncertainty = 0.0
        if hasattr(estimator, "estimators_"):
            scaler = self._model.named_steps.get("scaler")
            X_scaled = scaler.transform(X_processed) if scaler else X_processed
            tree_preds = np.array([
                tree.predict(X_scaled)[0] for tree in estimator.estimators_
            ])
            uncertainty = float(tree_preds.std())

        return point_estimate, uncertainty

    def predict_with_gate(
        self,
        features: np.ndarray | list,
        gate_threshold: float = 0.4,
    ) -> tuple[float, float, bool]:
        """Two-stage prediction: classifier gate then regressor.

        First runs the pass/fail classifier (if available). If the predicted
        BNF-pass probability is below ``gate_threshold``, returns (0.0, 0.0,
        False) immediately — no regressor call needed.

        Returns:
            (predicted_flux, uncertainty, predicted_pass)
        """
        X = np.asarray(features, dtype=float)
        if X.ndim == 1:
            X = X[np.newaxis, :]
        X_proc = clr_transform(X) if self._apply_clr else X

        if self._classifier is not None:
            pass_prob = float(self._classifier.predict_proba(X_proc)[0, 1])
            if pass_prob < gate_threshold:
                return 0.0, 0.0, False

        flux, unc = self.predict(features)
        return flux, unc, True

    def predict_batch_with_gate(
        self,
        X: np.ndarray,
        gate_threshold: float = 0.4,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Batch two-stage prediction.

        Returns:
            (predictions, uncertainties, pass_flags)  each shape (n_samples,)
        """
        X = np.asarray(X, dtype=float)
        X_proc = clr_transform(X) if self._apply_clr else X

        pass_flags = np.ones(len(X), dtype=bool)
        if self._classifier is not None:
            pass_probs = self._classifier.predict_proba(X_proc)[:, 1]
            pass_flags = pass_probs >= gate_threshold

        predictions  = np.zeros(len(X), dtype=float)
        uncertainties = np.zeros(len(X), dtype=float)
        if pass_flags.any():
            preds, uncs = self.predict_batch(X[pass_flags])
            predictions[pass_flags]   = preds
            uncertainties[pass_flags] = uncs

        return predictions, uncertainties, pass_flags

    def predict_batch(
        self,
        X: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Predict for a batch of samples.

        Returns (predictions, uncertainties) each of shape (n_samples,).
        """
        if self._model is None:
            raise RuntimeError("No model loaded or trained.")
        X = np.asarray(X, dtype=float)
        X_processed = clr_transform(X) if self._apply_clr else X

        # Use pipeline.predict() directly — handles any step names robustly.
        # If the model is a classifier, use predict_proba()[:,1] for continuous scores
        # (better Spearman correlation than binary class labels for rank-order validation).
        estimator = list(self._model.named_steps.values())[-1]
        if hasattr(estimator, "predict_proba") and not hasattr(estimator, "n_outputs_"):
            # Classifier — use probability of positive class as continuous score
            predictions = self._model.predict_proba(X_processed)[:, 1]
        else:
            predictions = self._model.predict(X_processed)

        if hasattr(estimator, "estimators_"):
            scaler = self._model.named_steps.get("scaler")
            X_scaled = scaler.transform(X_processed) if scaler else X_processed
            tree_matrix = np.vstack([t.predict(X_scaled) for t in estimator.estimators_])
            uncertainties = tree_matrix.std(axis=0)
        else:
            uncertainties = np.zeros(len(predictions))

        return predictions, uncertainties

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def save(self, model_path: str | Path) -> None:
        """Persist the trained model to disk (joblib format)."""
        if self._model is None:
            raise RuntimeError("No model to save — train first.")
        try:
            import joblib
        except ImportError as exc:
            raise ImportError("joblib is required for model serialization: pip install joblib") from exc

        model_path = Path(model_path)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "model": self._model,
            "model_type": self.model_type,
            "feature_names": self._feature_names,
            "apply_clr": self._apply_clr,
        }
        joblib.dump(payload, model_path)
        logger.info("FunctionalPredictor saved to %s", model_path)

    @classmethod
    def load(cls, model_path: str | Path) -> "FunctionalPredictor":
        """Load a previously saved FunctionalPredictor from disk."""
        try:
            import joblib
        except ImportError as exc:
            raise ImportError("joblib is required: pip install joblib") from exc

        payload = joblib.load(str(model_path))
        obj = cls(model_type=payload["model_type"] if payload.get("model_type") in cls.VALID_MODEL_TYPES else "random_forest")
        obj._model = payload["model"]
        obj._feature_names = payload.get("feature_names", [])
        obj._apply_clr = payload.get("apply_clr", True)
        # Load classifier gate if present (set by train_bnf_surrogate.py)
        obj._classifier = payload.get("classifier", None)
        if obj._classifier is not None:
            logger.info("FunctionalPredictor: classifier gate loaded")
        logger.info("FunctionalPredictor loaded from %s", model_path)
        return obj

    # ------------------------------------------------------------------
    # Feature importance
    # ------------------------------------------------------------------

    def feature_importances(self) -> dict[str, float]:
        """Return feature importance dict {feature_name: importance}."""
        if self._model is None:
            return {}
        estimator = list(self._model.named_steps.values())[-1]
        if not hasattr(estimator, "feature_importances_"):
            return {}
        importances = estimator.feature_importances_
        names = self._feature_names or [f"feature_{i}" for i in range(len(importances))]
        return dict(zip(names, importances.tolist()))
