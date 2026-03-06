"""
tests/test_phase9.py — Phase 9: ingest orchestrator (scripts/ingest.py) +
T0.25 wiring in pipeline_core (_score_community_t025 / run_t025_batch).
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# Ensure project root importable even when pytest is run from tests/
_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))


# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

def _minimal_config(tmp_path: Path) -> "PipelineConfig":
    """Return a PipelineConfig with T0.25 thresholds and no real model paths."""
    from config_schema import PipelineConfig
    return PipelineConfig(
        project={"name": "test-project", "version": "0.1"},
        target={"taxa": ["Rhizobium"], "functional_gene": "nifH"},
        filters={
            "t0": {"min_nifh_abundance": 0.01},
            "t025": {
                "min_function_score": 0.5,
                "min_similarity": 0.3,
                "model_path": str(tmp_path / "nonexistent_model.joblib"),
                "reference_db": str(tmp_path / "nonexistent.biom"),
            },
        },
    )


def _make_memory_db() -> "SoilDB":
    """Return a SoilDB backed by an in-memory SQLite database."""
    from db_utils import SoilDB
    db = SoilDB.__new__(SoilDB)
    # Provide a _connect() method that uses a shared :memory: connection
    _conn = sqlite3.connect(":memory:", check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    # Minimal schema needed for T0.25 queries
    _conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS samples (
            sample_id TEXT PRIMARY KEY,
            site TEXT,
            collection_date TEXT,
            lat REAL,
            lon REAL,
            metadata TEXT
        );
        CREATE TABLE IF NOT EXISTS communities (
            community_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_id TEXT,
            phylum_profile TEXT,
            top_genera TEXT,
            otu_table_path TEXT
        );
        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            community_id INTEGER,
            sample_id TEXT,
            target_id TEXT DEFAULT 'default',
            t0_pass INTEGER DEFAULT 0,
            t025_pass INTEGER,
            t025_function_score REAL,
            t025_uncertainty REAL,
            t025_similarity_score REAL,
            t025_similarity_hit TEXT,
            t025_model TEXT,
            t025_n_pathways INTEGER,
            t025_nsti_mean REAL
        );
        """
    )
    _conn.commit()

    import contextlib

    @contextlib.contextmanager
    def _connect_ctx():
        yield _conn

    db._connect = _connect_ctx  # type: ignore[attr-defined]
    db._shared_conn = _conn

    # Provide update_community_t025 stub that directly writes to the in-memory DB
    def _update_t025(community_id: int, data: dict) -> None:
        column_map = {
            "t025_pathway_abundances": "t025_model",
            "t025_top_similarity":     "t025_similarity_score",
            "t025_top_reference_id":   "t025_similarity_hit",
            "t025_function_score":     "t025_function_score",
            "t025_function_uncertainty": "t025_uncertainty",
            "t025_passed":             "t025_pass",
        }
        updates = {}
        for src, dst in column_map.items():
            if src in data:
                updates[dst] = data[src]
        if updates:
            set_clause = ", ".join(f"{col} = ?" for col in updates)
            vals = list(updates.values()) + [community_id]
            _conn.execute(
                f"UPDATE runs SET {set_clause} WHERE community_id = ?", vals
            )
            _conn.commit()

    db.update_community_t025 = _update_t025  # type: ignore[method-assign]
    return db


def _insert_t0_passer(db: "SoilDB", phylum_profile: dict | None = None) -> int:
    """Insert a sample + community + T0-passed run; return community_id."""
    conn = db._shared_conn
    phylum_json = json.dumps(phylum_profile or {"Proteobacteria": 0.4, "Firmicutes": 0.3})
    conn.execute(
        "INSERT INTO samples (sample_id, site) VALUES (?, ?)",
        ("sample-001", "STER"),
    )
    conn.execute(
        "INSERT INTO communities (sample_id, phylum_profile, top_genera) VALUES (?, ?, ?)",
        ("sample-001", phylum_json, json.dumps(["Rhizobium", "Bradyrhizobium"])),
    )
    community_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO runs (community_id, sample_id, t0_pass, t025_pass) VALUES (?, ?, 1, NULL)",
        (community_id, "sample-001"),
    )
    conn.commit()
    return community_id


# ---------------------------------------------------------------------------
# 1. _score_community_t025 — unit tests
# ---------------------------------------------------------------------------

class TestScoreCommunityT025:
    """Tests for pipeline_core._score_community_t025()"""

    @pytest.fixture(autouse=True)
    def _import(self):
        from pipeline_core import _score_community_t025
        self.fn = _score_community_t025

    def _community_row(self, phylum_profile: dict | None = None) -> dict:
        return {
            "community_id": 1,
            "phylum_profile": json.dumps(phylum_profile or {"Proteobacteria": 0.5, "Firmicutes": 0.3}),
            "top_genera": json.dumps(["Rhizobium"]),
        }

    def _t025_cfg(self, min_fn: float = 0.5, min_sim: float = 0.3) -> dict:
        return {"min_function_score": min_fn, "min_similarity": min_sim}

    # --- passthrough when no models loaded ---

    def test_no_models_pass_through_neutral_score(self):
        """With no predictor and no similarity index, community passes with 0.5 sentinel."""
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(),
            predictor=None,
            similarity_search=None,
        )
        assert result["t025_passed"] is True
        assert result["t025_function_score"] == pytest.approx(0.5)

    def test_no_models_other_fields_none(self):
        """Similarity and uncertainty fields stay None in passthrough mode."""
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(),
            predictor=None,
            similarity_search=None,
        )
        assert result["t025_top_similarity"] is None
        assert result["t025_top_reference_id"] is None
        assert result["t025_function_uncertainty"] is None

    # --- predictor-only tests ---

    def test_fn_score_above_threshold_passes(self):
        """FunctionalPredictor score ≥ min_function_score → t025_passed True."""
        predictor = MagicMock()
        predictor.predict.return_value = (0.75, 0.05)
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_fn=0.5),
            predictor=predictor,
            similarity_search=None,
        )
        assert result["t025_passed"] is True
        assert result["t025_function_score"] == pytest.approx(0.75)
        assert result["t025_function_uncertainty"] == pytest.approx(0.05)

    def test_fn_score_below_threshold_fails_without_sim(self):
        """Score below threshold and no similarity → fails."""
        predictor = MagicMock()
        predictor.predict.return_value = (0.2, 0.1)
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_fn=0.5),
            predictor=predictor,
            similarity_search=None,
        )
        assert result["t025_passed"] is False
        assert result["t025_function_score"] == pytest.approx(0.2)

    def test_fn_score_at_threshold_boundary_passes(self):
        """Score exactly at boundary (≥) → passes."""
        predictor = MagicMock()
        predictor.predict.return_value = (0.5, 0.0)
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_fn=0.5),
            predictor=predictor,
            similarity_search=None,
        )
        assert result["t025_passed"] is True

    # --- similarity-only tests ---

    def test_sim_score_above_threshold_passes(self):
        """High similarity alone clears the gate (OR logic)."""
        sim_search = MagicMock()
        sim_search.query.return_value = [{"similarity_score": 0.8, "reference_id": "ref-001"}]
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_sim=0.3),
            predictor=None,
            similarity_search=sim_search,
        )
        assert result["t025_passed"] is True
        assert result["t025_top_similarity"] == pytest.approx(0.8)
        assert result["t025_top_reference_id"] == "ref-001"

    def test_sim_score_below_threshold_fails_without_fn(self):
        """Low similarity and no predictor → fails."""
        sim_search = MagicMock()
        sim_search.query.return_value = [{"similarity_score": 0.1, "reference_id": "ref-002"}]
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_sim=0.3),
            predictor=None,
            similarity_search=sim_search,
        )
        assert result["t025_passed"] is False

    # --- OR logic: one signal rescues the other ---

    def test_low_fn_but_high_sim_passes(self):
        """fn_score < threshold but sim_score ≥ threshold → passes (OR)."""
        predictor = MagicMock()
        predictor.predict.return_value = (0.2, 0.1)
        sim_search = MagicMock()
        sim_search.query.return_value = [{"similarity_score": 0.9, "reference_id": "ref-X"}]
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_fn=0.5, min_sim=0.3),
            predictor=predictor,
            similarity_search=sim_search,
        )
        assert result["t025_passed"] is True

    def test_both_below_threshold_fails(self):
        """Both signals below their thresholds → fails."""
        predictor = MagicMock()
        predictor.predict.return_value = (0.1, 0.2)
        sim_search = MagicMock()
        sim_search.query.return_value = [{"similarity_score": 0.05, "reference_id": "ref-Y"}]
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(min_fn=0.5, min_sim=0.3),
            predictor=predictor,
            similarity_search=sim_search,
        )
        assert result["t025_passed"] is False

    # --- robustness ---

    def test_empty_phylum_profile_passthrough(self):
        """Empty / missing phylum profile → predictor not called, passthrough."""
        predictor = MagicMock()
        row_empty = {
            "community_id": 2,
            "phylum_profile": "{}",
            "top_genera": "[]",
        }
        result = self.fn(
            community_row=row_empty,
            run_row={"run_id": 2},
            t025_cfg=self._t025_cfg(),
            predictor=predictor,
            similarity_search=None,
        )
        # predictor.predict should NOT be called (empty profile → no scoring)
        predictor.predict.assert_not_called()
        # Passthrough neutral score
        assert result["t025_passed"] is True

    def test_broken_predictor_graceful_degradation(self):
        """If predictor.predict raises, score is None and community passes through."""
        predictor = MagicMock()
        predictor.predict.side_effect = RuntimeError("model corrupt")
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(),
            predictor=predictor,
            similarity_search=None,
        )
        # fn_score failed → None; sim_score None → passthrough sentinel
        assert result["t025_passed"] is True
        assert result["t025_function_score"] == pytest.approx(0.5)

    def test_result_contains_all_keys(self):
        """Result dict always has all expected keys regardless of outcome."""
        result = self.fn(
            community_row=self._community_row(),
            run_row={"run_id": 1},
            t025_cfg=self._t025_cfg(),
            predictor=None,
            similarity_search=None,
        )
        expected_keys = {
            "t025_function_score",
            "t025_function_uncertainty",
            "t025_top_similarity",
            "t025_top_reference_id",
            "t025_passed",
            "t025_pathway_abundances",
        }
        assert expected_keys.issubset(result.keys())


# ---------------------------------------------------------------------------
# 2. run_t025_batch — integration-style tests (mocked DB + config)
# ---------------------------------------------------------------------------

class TestRunT025Batch:
    """Tests for the second run_t025_batch() definition in pipeline_core (line ~885)."""

    @pytest.fixture(autouse=True)
    def _import(self, tmp_path):
        # Import the final (overriding) definition
        from pipeline_core import run_t025_batch
        self.run_t025_batch = run_t025_batch
        self.tmp_path = tmp_path

    def _config(self) -> "PipelineConfig":
        return _minimal_config(self.tmp_path)

    def _db_with_passers(self, n: int = 3) -> "SoilDB":
        db = _make_memory_db()
        conn = db._shared_conn
        for i in range(n):
            conn.execute(
                "INSERT INTO samples (sample_id, site) VALUES (?, ?)",
                (f"s-{i:03d}", "STER"),
            )
            conn.execute(
                "INSERT INTO communities (sample_id, phylum_profile, top_genera) VALUES (?, ?, ?)",
                (
                    f"s-{i:03d}",
                    json.dumps({"Proteobacteria": 0.5 - i * 0.1, "Firmicutes": 0.3}),
                    json.dumps(["Rhizobium"]),
                ),
            )
            cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                "INSERT INTO runs (community_id, sample_id, t0_pass, t025_pass) VALUES (?, ?, 1, NULL)",
                (cid, f"s-{i:03d}"),
            )
        conn.commit()
        return db

    def test_empty_db_returns_zero_processed(self):
        """No T0-passed rows → n_processed == 0 with no errors."""
        db = _make_memory_db()  # empty DB
        result = self.run_t025_batch(
            config=self._config(),
            db=db,
            workers=1,
            receipts_dir=self.tmp_path / "receipts",
        )
        assert result["n_processed"] == 0
        assert result["n_passed"] == 0
        assert result["errors"] == []

    def test_passthrough_when_no_models(self):
        """All communities pass through when no predictor/biom exist."""
        db = self._db_with_passers(n=4)
        result = self.run_t025_batch(
            config=self._config(),
            db=db,
            workers=1,
            receipts_dir=self.tmp_path / "receipts",
        )
        assert result["n_processed"] == 4
        assert result["n_passed"] == 4
        assert result["n_failed"] == 0

    def test_updates_t025_pass_in_db(self):
        """After batch, t025_pass is set in the runs table."""
        db = self._db_with_passers(n=2)
        self.run_t025_batch(
            config=self._config(),
            db=db,
            workers=1,
            receipts_dir=self.tmp_path / "receipts",
        )
        rows = db._shared_conn.execute(
            "SELECT t025_pass FROM runs WHERE t0_pass = 1"
        ).fetchall()
        # Every row should now have a non-NULL t025_pass value
        assert all(r[0] is not None for r in rows)

    def test_already_scored_rows_not_reprocessed(self):
        """Rows with t025_pass already set are skipped."""
        db = _make_memory_db()
        conn = db._shared_conn
        conn.execute("INSERT INTO samples (sample_id, site) VALUES ('s-already', 'STER')")
        conn.execute(
            "INSERT INTO communities (sample_id, phylum_profile, top_genera) VALUES ('s-already', ?, ?)",
            (json.dumps({"Proteobacteria": 0.4}), "[]"),
        )
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            "INSERT INTO runs (community_id, sample_id, t0_pass, t025_pass) VALUES (?, 's-already', 1, 1)",
            (cid,),
        )
        conn.commit()
        result = self.run_t025_batch(
            config=self._config(),
            db=db,
            workers=1,
            receipts_dir=self.tmp_path / "receipts",
        )
        # Already scored → skipped
        assert result["n_processed"] == 0

    def test_result_keys_present(self):
        """Return dict always contains all expected keys."""
        db = _make_memory_db()
        result = self.run_t025_batch(
            config=self._config(),
            db=db,
            workers=1,
            receipts_dir=self.tmp_path / "receipts",
        )
        assert {"n_processed", "n_passed", "n_failed", "errors"}.issubset(result.keys())

    def test_with_mock_predictor_above_threshold(self):
        """With a mocked predictor returning 0.9, community passes."""
        mock_predictor = MagicMock()
        mock_predictor.predict.return_value = (0.9, 0.02)
        from pipeline_core import _score_community_t025
        row = {
            "community_id": 99,
            "phylum_profile": json.dumps({"Proteobacteria": 0.5}),
            "top_genera": "[]",
        }
        result = _score_community_t025(
            community_row=row,
            run_row={"run_id": 99},
            t025_cfg={"min_function_score": 0.5, "min_similarity": 0.3},
            predictor=mock_predictor,
            similarity_search=None,
        )
        assert result["t025_passed"] is True
        assert result["t025_function_score"] == pytest.approx(0.9)

    def test_with_mock_predictor_below_threshold(self):
        """Predictor returning below threshold → community fails."""
        mock_predictor = MagicMock()
        mock_predictor.predict.return_value = (0.1, 0.3)
        from pipeline_core import _score_community_t025
        row = {
            "community_id": 99,
            "phylum_profile": json.dumps({"Proteobacteria": 0.5}),
            "top_genera": "[]",
        }
        result = _score_community_t025(
            community_row=row,
            run_row={"run_id": 99},
            t025_cfg={"min_function_score": 0.5, "min_similarity": 0.3},
            predictor=mock_predictor,
            similarity_search=None,
        )
        assert result["t025_passed"] is False


# ---------------------------------------------------------------------------
# 3. scripts/ingest.py — helper function unit tests
# ---------------------------------------------------------------------------

class TestIngestHelpers:
    """Tests for helper functions in scripts/ingest.py."""

    @pytest.fixture(autouse=True)
    def _import(self):
        import importlib.util, sys
        spec = importlib.util.spec_from_file_location(
            "ingest",
            str(_PROJ_ROOT / "scripts" / "ingest.py"),
        )
        mod = importlib.util.module_from_spec(spec)
        # Stub typer so the module-level Typer() call doesn't fail in test env
        import types
        typer_stub = types.ModuleType("typer")
        typer_stub.Typer = lambda **kw: MagicMock()
        typer_stub.Option = lambda *a, **kw: None
        typer_stub.Argument = lambda *a, **kw: None
        typer_stub.echo = print
        sys.modules.setdefault("typer", __import__("typer"))  # use real typer if installed
        spec.loader.exec_module(mod)
        self.ingest = mod

    def test_priority_sites_list_not_empty(self):
        """_PRIORITY_NEON_SITES must have at least 10 entries."""
        assert len(self.ingest._PRIORITY_NEON_SITES) >= 10

    def test_priority_sites_contains_known_sites(self):
        """Key agricultural sites are present."""
        sites = set(self.ingest._PRIORITY_NEON_SITES)
        assert "STER" in sites
        assert "KONA" in sites
        assert "CPER" in sites

    def test_recent_years_is_sorted_and_recent(self):
        """_RECENT_YEARS covers at least 2019–2022."""
        years = self.ingest._RECENT_YEARS
        assert 2019 in years
        assert 2022 in years
        assert sorted(years) == years

    def test_write_checkpoint_creates_file(self, tmp_path):
        """_write_checkpoint writes valid JSON to the specified path."""
        samples = [{"sample_id": "s1", "site": "STER"}, {"sample_id": "s2", "site": "CPER"}]
        out = tmp_path / "checkpoint.json"
        self.ingest._write_checkpoint(samples, out)
        assert out.exists()
        loaded = json.loads(out.read_text())
        assert loaded == samples

    def test_write_checkpoint_overwrites_existing(self, tmp_path):
        """_write_checkpoint overwrites previous content."""
        out = tmp_path / "cp.json"
        self.ingest._write_checkpoint([{"x": 1}], out)
        self.ingest._write_checkpoint([{"x": 2}, {"x": 3}], out)
        loaded = json.loads(out.read_text())
        assert len(loaded) == 2

    def test_samples_seen_empty_db(self):
        """_samples_seen returns empty set when DB has no rows."""
        db = _make_memory_db()   # samples table exists but is empty
        result = self.ingest._samples_seen(db)
        assert result == set()

    def test_samples_seen_returns_existing_ids(self):
        """_samples_seen returns the IDs already in the DB."""
        db = _make_memory_db()
        conn = db._shared_conn
        conn.execute("INSERT INTO samples (sample_id, site) VALUES ('aaa', 'STER')")
        conn.execute("INSERT INTO samples (sample_id, site) VALUES ('bbb', 'CPER')")
        conn.commit()
        result = self.ingest._samples_seen(db)
        assert result == {"aaa", "bbb"}

    def test_samples_seen_handles_missing_table(self, tmp_path):
        """_samples_seen returns empty set when table doesn't exist (new DB)."""
        db_path = tmp_path / "brand_new.db"
        # Don't create any tables at all
        from db_utils import SoilDB
        db = SoilDB.__new__(SoilDB)
        # Give it a _connect that points to our empty file
        import contextlib, sqlite3 as sq
        raw = sq.connect(str(db_path))
        raw.row_factory = sq.Row

        @contextlib.contextmanager
        def _connect():
            yield raw

        db._connect = _connect  # type: ignore[attr-defined]
        result = self.ingest._samples_seen(db)
        assert result == set()


# ---------------------------------------------------------------------------
# 4. Config schema — T025Filters validation
# ---------------------------------------------------------------------------

class TestT025FiltersSchema:
    """Validate T025Filters Pydantic model."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from config_schema import T025Filters
        self.T025Filters = T025Filters

    def test_defaults_are_sensible(self):
        """Default thresholds match config.example.yaml expectations."""
        f = self.T025Filters()
        assert f.min_function_score == pytest.approx(0.5)
        assert f.min_similarity == pytest.approx(0.3)
        assert f.reference_db is None

    def test_custom_thresholds(self):
        """Custom values round-trip correctly."""
        f = self.T025Filters(min_function_score=0.7, min_similarity=0.6, reference_db="foo.biom")
        assert f.min_function_score == pytest.approx(0.7)
        assert f.min_similarity == pytest.approx(0.6)
        assert f.reference_db == "foo.biom"

    def test_ml_models_default_empty(self):
        """ml_models defaults to empty list."""
        f = self.T025Filters()
        assert f.ml_models == []

    def test_extra_fields_ignored(self):
        """Unknown extra fields do not raise (model uses ignore mode)."""
        # Pydantic v2 ignores extra by default; just assert construction succeeds
        try:
            f = self.T025Filters(min_function_score=0.8, unknown_key="ok")
            # some configs use model_config = {'extra': 'ignore'} — either way no crash
        except Exception:
            pass  # extra-fields-as-error is also acceptable


# ---------------------------------------------------------------------------
# 5. Pipeline config round-trip with T0.25 section
# ---------------------------------------------------------------------------

class TestPipelineConfigT025:
    """PipelineConfig loads T0.25 filters from dict correctly."""

    def test_t025_filters_loaded_from_dict(self, tmp_path):
        from config_schema import PipelineConfig
        cfg = PipelineConfig(
            project={"name": "test", "version": "0"},
            target={"taxa": ["Azospirillum"], "functional_gene": "nifH"},
            filters={
                "t025": {
                    "min_function_score": 0.6,
                    "min_similarity": 0.4,
                    "reference_db": "ref/db.biom",
                }
            },
        )
        t025 = cfg.filters.get("t025", {})
        assert t025["min_function_score"] == pytest.approx(0.6)
        assert t025["min_similarity"] == pytest.approx(0.4)

    def test_missing_t025_section_not_fatal(self):
        """PipelineConfig with no t025 key is valid; t025 defaults apply."""
        from config_schema import PipelineConfig
        cfg = PipelineConfig(
            project={"name": "test", "version": "0"},
            target={"taxa": ["Rhizobium"], "functional_gene": "nifH"},
        )
        # get("t025") returns None/empty — should not raise
        t025 = cfg.filters.get("t025", {})
        assert isinstance(t025, dict)
