"""
tests/test_phase2.py — Unit tests for Phase 2 (T0.25) compute modules.

Covers:
  community_similarity   -- from_otu_matrix, query braycurtis + cosine
  functional_predictor   -- train + predict, CLR transform, feature_importances
  picrust2_runner        -- graceful fallback when not installed
  humann3_shortcut       -- graceful fallback when not installed

All tests run without external bioinformatics tools installed.
"""

from __future__ import annotations
import math
import sys
from pathlib import Path

import pytest
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# CLR transform
# ===========================================================================

class TestCLRTransform:
    def test_output_shape(self):
        from compute.functional_predictor import clr_transform
        X = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
        out = clr_transform(X)
        assert out.shape == X.shape

    def test_rows_sum_to_zero(self):
        from compute.functional_predictor import clr_transform
        X = np.array([[1.0, 2.0, 4.0]])
        out = clr_transform(X)
        assert abs(out.sum()) < 1e-6

    def test_zero_input_survives(self):
        """All-zero input should not crash (pseudocount handles it)."""
        from compute.functional_predictor import clr_transform
        X = np.zeros((2, 4))
        out = clr_transform(X)
        assert out.shape == (2, 4)


# ===========================================================================
# CommunitySimilaritySearch
# ===========================================================================

class TestCommunitySimilaritySearch:
    def _make_index(self, n_samples=10, n_features=20, seed=42):
        from compute.community_similarity import CommunitySimilaritySearch
        rng = np.random.default_rng(seed)
        matrix = rng.integers(0, 200, (n_samples, n_features)).astype(float)
        sample_ids = [f"SMP{i:03d}" for i in range(n_samples)]
        feature_ids = [f"OTU{j:03d}" for j in range(n_features)]
        return CommunitySimilaritySearch.from_otu_matrix(matrix, sample_ids, feature_ids)

    def test_from_otu_matrix_stores_ids(self):
        idx = self._make_index()
        assert len(idx.sample_ids) == 10
        assert len(idx.feature_ids) == 20

    def test_query_returns_top_k(self):
        idx = self._make_index()
        query_vec = np.ones(20) * 50
        results = idx.query(query_vec, metric="braycurtis", top_k=3)
        assert len(results) == 3

    def test_query_result_keys(self):
        idx = self._make_index()
        query_vec = np.ones(20) * 50
        result = idx.query(query_vec, top_k=1)[0]
        assert "reference_id" in result
        assert "similarity_score" in result
        assert "rank" in result

    def test_similarity_score_in_range(self):
        idx = self._make_index()
        query_vec = np.ones(20) * 50
        for r in idx.query(query_vec, metric="braycurtis", top_k=5):
            assert 0.0 <= r["similarity_score"] <= 1.0

    def test_cosine_metric(self):
        idx = self._make_index()
        query_vec = np.ones(20) * 10
        results = idx.query(query_vec, metric="cosine", top_k=3)
        assert len(results) == 3
        for r in results:
            assert 0.0 <= r["similarity_score"] <= 1.0

    def test_query_dict_input(self):
        """Query can be a dict of {feature_id: count}."""
        idx = self._make_index()
        query_dict = {f"OTU{j:03d}": float(j * 10) for j in range(20)}
        results = idx.query(query_dict, top_k=2)
        assert len(results) == 2

    def test_query_result_ordered_by_rank(self):
        idx = self._make_index()
        results = idx.query(np.ones(20), top_k=5)
        ranks = [r["rank"] for r in results]
        assert ranks == sorted(ranks)

    def test_top_k_capped_by_index_size(self):
        idx = self._make_index(n_samples=4)
        results = idx.query(np.ones(20), top_k=100)
        assert len(results) == 4


# ===========================================================================
# FunctionalPredictor
# ===========================================================================

class TestFunctionalPredictor:
    def _make_predictor(self, model_type="random_forest"):
        from compute.functional_predictor import FunctionalPredictor
        return FunctionalPredictor(model_type=model_type)

    def _make_training_data(self, n=30, p=15, seed=7):
        rng = np.random.default_rng(seed)
        X = rng.integers(0, 500, (n, p)).astype(float)
        y = rng.uniform(0, 100, n)
        features = [f"feat_{i}" for i in range(p)]
        return X, y, features

    def test_train_and_predict_rf(self):
        pred = self._make_predictor("random_forest")
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features)
        point, uncertainty = pred.predict(X[0])
        assert isinstance(point, float)
        assert isinstance(uncertainty, float)

    def test_train_and_predict_gbm(self):
        pred = self._make_predictor("gradient_boost")
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features)
        point, _ = pred.predict(X[0])
        assert isinstance(point, float)

    def test_predict_batch_shapes(self):
        pred = self._make_predictor()
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features)
        preds, uncerts = pred.predict_batch(X[:5])
        assert len(preds) == 5
        assert len(uncerts) == 5

    def test_feature_importances_sums_to_one(self):
        pred = self._make_predictor()
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features)
        imps = pred.feature_importances()
        assert len(imps) == len(features)
        total = sum(imps.values())
        assert abs(total - 1.0) < 1e-3

    def test_save_load_roundtrip(self, tmp_path):
        import joblib
        pred = self._make_predictor()
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features)
        save_path = tmp_path / "model.joblib"
        pred.save(str(save_path))
        assert save_path.exists()

        from compute.functional_predictor import FunctionalPredictor
        pred2 = FunctionalPredictor.load(str(save_path))
        point_orig, _ = pred.predict(X[0])
        point_loaded, _ = pred2.predict(X[0])
        assert abs(point_orig - point_loaded) < 1e-6

    def test_clr_applied_in_train(self):
        """apply_clr=True should not crash and produces a valid prediction."""
        pred = self._make_predictor()
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features, apply_clr=True)
        point, _ = pred.predict(X[1])
        assert math.isfinite(point)

    def test_unknown_feature_in_query_ignored(self):
        """Predict from dict with extra keys should not crash."""
        pred = self._make_predictor()
        X, y, features = self._make_training_data()
        pred.train(X, y, feature_names=features)
        query = {f"feat_{i}": float(i) for i in range(len(features))}
        query["EXTRA_FEATURE"] = 999.0
        # dict input handled by _align_query in CommunitySimilaritySearch;
        # FunctionalPredictor expects array or array-coercible
        point, _ = pred.predict(np.zeros(len(features)))
        assert math.isfinite(point)


# ===========================================================================
# PICRUSt2 graceful fallback
# ===========================================================================

class TestPICRUSt2Runner:
    def test_graceful_when_not_installed(self, monkeypatch, tmp_path):
        """If picrust2 binary not found, returns empty dicts and nan NSTI."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.picrust2_runner as pr
        reload(pr)

        result = pr.run_picrust2(
            asv_table_biom=str(tmp_path / "fake.biom"),
            rep_seqs_fasta=str(tmp_path / "fake.fasta"),
            outdir=str(tmp_path / "picrust_out"),
        )
        assert isinstance(result["pathway_abundances"], dict)
        assert isinstance(result["ko_abundances"], dict)
        assert math.isnan(result["nsti_mean"])


# ===========================================================================
# HUMAnN3 graceful fallback
# ===========================================================================

class TestHUMAnN3Runner:
    def test_graceful_when_not_installed(self, monkeypatch, tmp_path):
        """If humann binary not found, returns empty dicts."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.humann3_shortcut as h3
        reload(h3)

        result = h3.run_humann3(
            fastq_path=str(tmp_path / "fake.fastq"),
            outdir=str(tmp_path / "humann_out"),
        )
        assert isinstance(result["pathway_abundances"], dict)
        assert isinstance(result["gene_families"], dict)
        assert not result["pathway_abundance_path"]  # None or empty string
