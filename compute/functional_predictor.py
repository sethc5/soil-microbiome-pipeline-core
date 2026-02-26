"""
compute/functional_predictor.py — T0.25 ML functional outcome prediction.

Trains or loads random forest / gradient boosting models that predict
target function scores (e.g. BNF rate, SOC accumulation) from:
  - OTU relative abundance features (log-ratio transformed — see README gotchas)
  - Environmental metadata features (pH, texture, climate)
  - Derived diversity metrics

Models are persisted as joblib files alongside the database.

Usage:
  from compute.functional_predictor import FunctionalPredictor
  predictor = FunctionalPredictor.load("models/random_forest_bnf.joblib")
  score, uncertainty = predictor.predict(community_feature_vector)
"""

from __future__ import annotations
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class FunctionalPredictor:
    def __init__(self, model_type: str = "random_forest"):
        self.model_type = model_type
        self._model = None

    @classmethod
    def load(cls, model_path: str | Path) -> "FunctionalPredictor":
        raise NotImplementedError

    def train(self, X, y) -> "FunctionalPredictor":
        """Train on feature matrix X and target labels y."""
        raise NotImplementedError

    def predict(self, features) -> tuple[float, float]:
        """Return (predicted_score, uncertainty_estimate)."""
        raise NotImplementedError

    def save(self, model_path: str | Path) -> None:
        raise NotImplementedError
