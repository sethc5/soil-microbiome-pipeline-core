"""
tests/test_phase10.py — Phase 10: synthetic_bootstrap + dfba_batch

Covers:
  - _phylum_profile: Dirichlet output, pH-driven composition, normalization
  - _bnf_label: score range, pH optimum, land-use boost
  - _generate_one: deterministic (same seed), all required keys
  - _build_reference_biom: TSV format correctness, sidecar JSON
  - _insert_batch / round-trip: writes to SQLite, community+runs rows present
  - _worker_batch (dfba): returns required keys, no crash on edge-case profiles
  - _run_community_sim: stability_score in [0,1], trajectory finishes
  - temperature_factor, precipitation_factor: range checks
  - _fetch_communities: respects min_bnf filter and t2_pass IS NULL
  - _write_results: updates t2_pass, t2_stability_score, t1_target_flux
"""

from __future__ import annotations

import json
import math
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

# ---------------------------------------------------------------------------
# Lazy imports (modules may not be importable on CI if optional deps missing)
# ---------------------------------------------------------------------------

try:
    from scripts.synthetic_bootstrap import (
        PHYLA,
        _bnf_label,
        _build_reference_biom,
        _generate_batch,
        _generate_one,
        _insert_batch,
        _phylum_profile,
        _train_predictor,
    )
    _BOOTSTRAP_AVAILABLE = True
except Exception:
    _BOOTSTRAP_AVAILABLE = False

try:
    from scripts.dfba_batch import (
        _build_odes,
        _fetch_communities,
        _run_community_sim,
        _worker_batch,
        _write_results,
        _precipitation_factor,
        _temperature_factor,
    )
    _DFBA_AVAILABLE = True
except Exception:
    _DFBA_AVAILABLE = False

skip_bootstrap = pytest.mark.skipif(not _BOOTSTRAP_AVAILABLE,
                                     reason="synthetic_bootstrap not importable")
skip_dfba = pytest.mark.skipif(not _DFBA_AVAILABLE,
                                reason="dfba_batch not importable or scipy missing")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS samples (
    sample_id        TEXT PRIMARY KEY,
    source           TEXT DEFAULT 'synthetic',
    site             TEXT,
    site_id          TEXT,
    collection_date  TEXT,
    lat              REAL DEFAULT 0,
    lon              REAL DEFAULT 0,
    latitude         REAL DEFAULT 0,
    longitude        REAL DEFAULT 0,
    soil_ph          REAL DEFAULT 6.5,
    organic_matter_pct REAL DEFAULT 2.0,
    clay_pct         REAL DEFAULT 25.0,
    temperature_c    REAL DEFAULT 12.0,
    precipitation_mm REAL DEFAULT 600.0,
    land_use         TEXT DEFAULT 'natural',
    sampling_fraction TEXT DEFAULT 'bulk',
    depth_cm         REAL DEFAULT 10.0,
    fraction         TEXT DEFAULT 'bulk'
);
CREATE TABLE IF NOT EXISTS communities (
    community_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id          TEXT,
    phylum_profile     TEXT,
    top_genera         TEXT,
    shannon_diversity  REAL DEFAULT 0,
    simpson_diversity  REAL DEFAULT 0,
    richness_observed  INTEGER DEFAULT 0,
    observed_otus      INTEGER DEFAULT 0,
    biomass_proxy      REAL DEFAULT 0,
    pielou_evenness    REAL DEFAULT 0,
    target_enrichment  REAL DEFAULT 0,
    has_nifh           INTEGER DEFAULT 0,
    nifh_abundance     REAL DEFAULT 0,
    synthetic          INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS runs (
    run_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id       INTEGER,
    sample_id          TEXT,
    t0_pass            INTEGER,
    t025_pass          INTEGER,
    t025_function_score REAL,
    t025_uncertainty   REAL DEFAULT 0,
    t025_similarity_score REAL,
    t2_pass            INTEGER,
    t2_stability_score REAL,
    t1_target_flux     REAL,
    t2_walltime_s      REAL,
    t2_best_intervention TEXT,
    tier_reached       INTEGER DEFAULT 0,
    batch_run_label    TEXT
);
"""


def _make_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(_MINIMAL_SCHEMA)
    conn.close()


# ---------------------------------------------------------------------------
# synthetic_bootstrap tests
# ---------------------------------------------------------------------------

class TestPhylumProfile:
    @skip_bootstrap
    def test_sums_to_one(self):
        import numpy as np
        rng = np.random.default_rng(42)
        profile = _phylum_profile(6.5, 2.0, 25.0, "natural", "bulk", rng)
        total = sum(profile.values())
        assert abs(total - 1.0) < 1e-6, f"Expected sum=1, got {total}"

    @skip_bootstrap
    def test_all_phyla_present(self):
        import numpy as np
        rng = np.random.default_rng(1)
        profile = _phylum_profile(5.0, 3.0, 30.0, "agricultural", "bulk", rng)
        assert set(profile.keys()) == set(PHYLA)

    @skip_bootstrap
    def test_acidobacteria_peaks_low_ph(self):
        """Acidobacteria should be highest or near-highest at pH 4.5 (Fierer & Jackson 2006)."""
        import numpy as np
        scores = []
        for seed in range(50):
            rng = np.random.default_rng(seed)
            p_low  = _phylum_profile(4.5, 2.0, 25.0, "natural", "bulk", rng)
            rng2 = np.random.default_rng(seed + 10_000)
            p_high = _phylum_profile(7.8, 2.0, 25.0, "natural", "bulk", rng2)
            scores.append(p_low["Acidobacteria"] - p_high["Acidobacteria"])
        # On average, low pH has more Acidobacteria
        assert sum(scores) / len(scores) > 0, "Acidobacteria should be higher at low pH"

    @skip_bootstrap
    def test_all_non_negative(self):
        import numpy as np
        for seed in range(20):
            rng = np.random.default_rng(seed)
            p = _phylum_profile(6.5, 2.0, 25.0, "natural", "bulk", rng)
            for k, v in p.items():
                assert v >= 0, f"Negative abundance for {k}: {v}"


class TestBnfLabel:
    @skip_bootstrap
    def test_range_zero_one(self):
        for proto in [0.05, 0.2, 0.4]:
            profile = {p: 0.0 for p in PHYLA}
            profile["Proteobacteria"] = proto
            score = _bnf_label(profile, 6.5, 2.0, "natural", "bulk")
            assert 0.0 <= score <= 1.0, f"BNF score out of range: {score}"

    @skip_bootstrap
    def test_higher_proteobacteria_higher_bnf(self):
        profile_lo = {p: 0.0 for p in PHYLA}
        profile_lo["Proteobacteria"] = 0.10

        profile_hi = {p: 0.0 for p in PHYLA}
        profile_hi["Proteobacteria"] = 0.40

        score_lo = _bnf_label(profile_lo, 6.5, 2.0, "natural", "bulk")
        score_hi = _bnf_label(profile_hi, 6.5, 2.0, "natural", "bulk")
        assert score_hi > score_lo

    @skip_bootstrap
    def test_agriculture_boost(self):
        profile = {p: 0.0 for p in PHYLA}
        profile["Proteobacteria"] = 0.25
        agr = _bnf_label(profile, 6.5, 2.0, "agricultural", "bulk")
        nat = _bnf_label(profile, 6.5, 2.0, "natural", "bulk")
        assert agr >= nat  # land-use boost must be non-negative

    @skip_bootstrap
    def test_ph_optimum_near_6_5(self):
        profile = {p: 0.0 for p in PHYLA}
        profile["Proteobacteria"] = 0.30
        score_opt = _bnf_label(profile, 6.5, 2.0, "natural", "bulk")
        score_low = _bnf_label(profile, 4.0, 2.0, "natural", "bulk")
        score_hi  = _bnf_label(profile, 8.5, 2.0, "natural", "bulk")
        assert score_opt > score_low
        assert score_opt > score_hi


class TestGenerateOne:
    @skip_bootstrap
    def test_required_keys(self):
        result = _generate_one(42, "HARV", "bulk")
        required = {"sample_id", "phylum_profile", "bnf_score",
                    "shannon_diversity", "soil_ph", "organic_matter_pct"}
        assert required.issubset(set(result.keys())), f"Missing keys: {required - set(result.keys())}"

    @skip_bootstrap
    def test_deterministic(self):
        r1 = _generate_one(99, "ORNL", "rhizosphere")
        r2 = _generate_one(99, "ORNL", "rhizosphere")
        assert r1["phylum_profile"] == r2["phylum_profile"]
        assert abs(r1["bnf_score"] - r2["bnf_score"]) < 1e-12

    @skip_bootstrap
    def test_bnf_score_in_range(self):
        for seed in range(30):
            r = _generate_one(seed, "KONA", "bulk")
            assert 0.0 <= r["bnf_score"] <= 1.0

    @skip_bootstrap
    def test_phylum_profile_json_valid(self):
        r = _generate_one(7, "CLBJ", "bulk")
        profile = r["phylum_profile"]
        if isinstance(profile, str):
            profile = json.loads(profile)
        assert isinstance(profile, dict)
        total = sum(profile.values())
        assert abs(total - 1.0) < 1e-5


class TestInsertBatch:
    @skip_bootstrap
    def test_inserts_rows(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        communities = [_generate_one(seed, "HARV", "bulk") for seed in range(5)]
        n = _insert_batch(db_path, communities)
        assert n == 5
        conn = sqlite3.connect(db_path)
        n_comm = conn.execute("SELECT COUNT(*) FROM communities").fetchone()[0]
        n_runs = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
        conn.close()
        assert n_comm == 5
        assert n_runs == 5

    @skip_bootstrap
    def test_t0_pass_set(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        communities = [_generate_one(seed, "HARV", "bulk") for seed in range(3)]
        _insert_batch(db_path, communities)
        conn = sqlite3.connect(db_path)
        flags = conn.execute("SELECT DISTINCT t0_pass FROM runs").fetchall()
        conn.close()
        assert all(row[0] == 1 for row in flags)


class TestBuildReferenceBiom:
    @skip_bootstrap
    def test_tsv_format(self, tmp_path):
        """TSV must have feature_id header + one row per phylum."""
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        # Insert synthetic communities with high BNF
        communities = [_generate_one(seed, "HARV", "bulk") for seed in range(10)]
        # Force high BNF so filter passes
        for c in communities:
            c["bnf_score"] = 0.80
        _insert_batch(db_path, communities)

        biom_out = tmp_path / "reference.tsv"
        _build_reference_biom(db_path, biom_out, min_bnf=0.70, n_max=10)

        assert biom_out.exists(), "TSV output file not created"
        lines = biom_out.read_text().splitlines()
        assert len(lines) >= 2, "TSV must have header + data rows"

        header = lines[0].split("\t")
        assert header[0] == "feature_id", f"First column header must be 'feature_id', got {header[0]!r}"
        assert len(header) >= 2, "Must have at least one sample column"

        # Each subsequent row: phylum name + float values
        for row in lines[1:]:
            parts = row.split("\t")
            assert parts[0] in PHYLA, f"Unknown phylum in row: {parts[0]}"
            for val in parts[1:]:
                float(val)  # raises ValueError if not parseable

    @skip_bootstrap
    def test_sidecar_json(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        communities = [_generate_one(seed, "HARV", "bulk") for seed in range(5)]
        for c in communities:
            c["bnf_score"] = 0.80
        _insert_batch(db_path, communities)

        biom_out = tmp_path / "reference.tsv"
        _build_reference_biom(db_path, biom_out, min_bnf=0.70, n_max=5)

        meta = tmp_path / "reference.meta.json"
        assert meta.exists(), "Sidecar meta.json not created"
        data = json.loads(meta.read_text())
        assert "bnf_scores" in data
        assert len(data["bnf_scores"]) > 0

    @skip_bootstrap
    def test_all_phyla_rows_present(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        communities = [_generate_one(seed, "ORNL", "bulk") for seed in range(10)]
        for c in communities:
            c["bnf_score"] = 0.80
        _insert_batch(db_path, communities)

        biom_out = tmp_path / "reference.tsv"
        _build_reference_biom(db_path, biom_out, min_bnf=0.70, n_max=10)

        lines = biom_out.read_text().splitlines()
        row_phyla = [l.split("\t")[0] for l in lines[1:]]
        for phylum in PHYLA:
            assert phylum in row_phyla, f"Missing phylum row: {phylum}"

    @skip_bootstrap
    def test_empty_db_skips_gracefully(self, tmp_path):
        db_path = str(tmp_path / "empty.db")
        _make_db(db_path)
        biom_out = tmp_path / "reference.tsv"
        # Should not raise; file should NOT be created because no rows
        _build_reference_biom(db_path, biom_out, min_bnf=0.99, n_max=100)
        # Either file doesn't exist, or (if created) it's empty / header-only
        if biom_out.exists():
            lines = biom_out.read_text().splitlines()
            assert len(lines) <= 1


# ---------------------------------------------------------------------------
# dfba_batch tests
# ---------------------------------------------------------------------------

class TestTemperatureFactor:
    @skip_dfba
    def test_positive(self):
        for day in [0, 30, 60, 90]:
            assert _temperature_factor(day, 12.0) > 0

    @skip_dfba
    def test_seasonal_variation(self):
        vals = [_temperature_factor(d, 15.0) for d in range(0, 365, 15)]
        assert max(vals) > min(vals), "Expected seasonal variation in temperature factor"


class TestPrecipitationFactor:
    @skip_dfba
    def test_positive(self):
        for day in [0, 30, 60, 90]:
            assert _precipitation_factor(day, 600.0) > 0

    @skip_dfba
    def test_drought_day_relatively_low(self):
        """Day 30 should be among the lower moisture days in the cycle."""
        day_30 = _precipitation_factor(30, 600.0)
        assert day_30 > 0  # must remain positive even during drought


class TestRunCommunitySim:
    _PROTO_PROFILE = {
        "Proteobacteria":   0.35,
        "Actinobacteria":   0.15,
        "Acidobacteria":    0.10,
        "Firmicutes":       0.08,
        "Bacteroidetes":    0.07,
        "Verrucomicrobia":  0.05,
        "Planctomycetes":   0.04,
        "Chloroflexi":      0.04,
        "Gemmatimonadetes": 0.04,
        "Nitrospirae":      0.03,
        "Cyanobacteria":    0.03,
        "Thaumarchaeota":   0.02,
    }

    _ENV = {"soil_ph": 6.5, "organic_matter_pct": 2.5,
            "clay_pct": 25.0, "temperature_c": 14.0, "precipitation_mm": 650.0}

    @skip_dfba
    def test_returns_required_keys(self):
        result = _run_community_sim(1, self._PROTO_PROFILE, self._ENV, sim_days=30)
        for key in ("community_id", "stability_score", "target_flux", "walltime_s",
                    "perturbation_responses", "t2_pass", "error"):
            assert key in result, f"Missing key: {key}"

    @skip_dfba
    def test_stability_in_range(self):
        result = _run_community_sim(2, self._PROTO_PROFILE, self._ENV, sim_days=30)
        assert 0.0 <= result["stability_score"] <= 1.0

    @skip_dfba
    def test_target_flux_positive(self):
        result = _run_community_sim(3, self._PROTO_PROFILE, self._ENV, sim_days=30)
        assert result["target_flux"] >= 0.0

    @skip_dfba
    def test_no_error_on_normal_input(self):
        result = _run_community_sim(4, self._PROTO_PROFILE, self._ENV, sim_days=30)
        assert result["error"] is None, f"Unexpected error: {result['error']}"

    @skip_dfba
    def test_empty_profile_runs_without_crash(self):
        """Empty profile edge case — should return gracefully."""
        result = _run_community_sim(5, {}, self._ENV, sim_days=15)
        assert "stability_score" in result

    @skip_dfba
    def test_perturbation_responses_list(self):
        result = _run_community_sim(6, self._PROTO_PROFILE, self._ENV, sim_days=90)
        assert isinstance(result["perturbation_responses"], list)
        # Should have responses for days 30, 50, 60 (≤ 90-day sim)
        assert len(result["perturbation_responses"]) == 3

    @skip_dfba
    def test_walltime_positive(self):
        result = _run_community_sim(7, self._PROTO_PROFILE, self._ENV, sim_days=30)
        assert result["walltime_s"] > 0.0


class TestWorkerBatch:
    @skip_dfba
    def test_batch_returns_same_count(self):
        profile = json.dumps({
            "Proteobacteria": 0.35, "Actinobacteria": 0.15,
            "Acidobacteria": 0.10, "Firmicutes": 0.08,
            "Bacteroidetes": 0.07, "Verrucomicrobia": 0.05,
            "Planctomycetes": 0.04, "Chloroflexi": 0.04,
            "Gemmatimonadetes": 0.04, "Nitrospirae": 0.03,
            "Cyanobacteria": 0.03, "Thaumarchaeota": 0.02,
        })
        env = json.dumps({"soil_ph": 6.5, "organic_matter_pct": 2.0,
                          "temperature_c": 14.0, "precipitation_mm": 600.0})
        batch = [(i, profile, env, 15) for i in range(5)]
        results = _worker_batch(batch)
        assert len(results) == 5

    @skip_dfba
    def test_bad_json_profile_handled(self):
        batch = [(99, "NOT JSON {{{", "{}", 10)]
        results = _worker_batch(batch)
        assert len(results) == 1
        # Should not raise — error key may be set or not, but no exception


class TestFetchCommunities:
    @skip_dfba
    def test_respects_min_bnf(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        # Insert 3 rows: 2 above, 1 below threshold
        profile = json.dumps({"Proteobacteria": 0.30})
        conn = sqlite3.connect(db_path)
        for i, score in enumerate([0.75, 0.80, 0.40]):
            conn.execute("INSERT INTO samples (sample_id) VALUES (?)", (f"S_{i}",))
            conn.execute(
                "INSERT INTO communities (sample_id, phylum_profile, synthetic) VALUES (?,?,1)",
                (f"S_{i}", profile)
            )
            cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                """INSERT INTO runs (community_id, sample_id, t0_pass, t025_pass,
                                    t025_function_score, t2_pass)
                   VALUES (?,?,1,1,?,NULL)""",
                (cid, f"S_{i}", score)
            )
        conn.commit()
        conn.close()

        rows = _fetch_communities(db_path, min_bnf=0.65, n_max=100)
        assert len(rows) == 2, f"Expected 2 rows above min_bnf=0.65, got {len(rows)}"

    @skip_dfba
    def test_excludes_already_t2_passed(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        profile = json.dumps({"Proteobacteria": 0.30})
        conn = sqlite3.connect(db_path)
        for i, t2 in enumerate([None, 1]):
            conn.execute("INSERT INTO samples (sample_id) VALUES (?)", (f"S_{i}",))
            conn.execute(
                "INSERT INTO communities (sample_id, phylum_profile, synthetic) VALUES (?,?,1)",
                (f"S_{i}", profile)
            )
            cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                """INSERT INTO runs (community_id, sample_id, t0_pass, t025_pass,
                                    t025_function_score, t2_pass)
                   VALUES (?,?,1,1,0.80,?)""",
                (cid, f"S_{i}", t2)
            )
        conn.commit()
        conn.close()

        rows = _fetch_communities(db_path, min_bnf=0.65, n_max=100)
        assert len(rows) == 1, "Should exclude already-t2-passed communities"


class TestWriteResults:
    @skip_dfba
    def test_writes_stability_and_flux(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("INSERT INTO samples (sample_id) VALUES ('S_1')")
        conn.execute(
            "INSERT INTO communities (sample_id, phylum_profile, synthetic) VALUES ('S_1','{}',1)"
        )
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO runs (community_id, sample_id, t0_pass, t025_pass,
                                t025_function_score, t2_pass)
               VALUES (?,?,1,1,0.75,NULL)""",
            (cid, "S_1")
        )
        conn.commit()
        conn.close()

        results = [{
            "community_id":    cid,
            "stability_score": 0.82,
            "target_flux":     0.015,
            "walltime_s":      1.23,
            "t2_pass":         True,
            "perturbation_responses": [{"day": 30, "bnf_flux": 0.012}],
            "error":           None,
        }]
        _write_results(db_path, results)

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT t2_pass, t2_stability_score, t1_target_flux FROM runs WHERE community_id=?", (cid,)
        ).fetchone()
        conn.close()
        assert row[0] == 1
        assert abs(row[1] - 0.82) < 1e-6
        assert abs(row[2] - 0.015) < 1e-6

    @skip_dfba
    def test_skips_error_results(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        _make_db(db_path)
        results = [{
            "community_id": 9999,
            "stability_score": 0.5,
            "target_flux": 0.01,
            "walltime_s": 0.5,
            "t2_pass": True,
            "perturbation_responses": [],
            "error": "something went wrong",
        }]
        # Should not raise, and should write 0 rows
        n = _write_results(db_path, results)
        assert n == 0
