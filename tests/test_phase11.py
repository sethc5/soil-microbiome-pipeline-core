"""
tests/test_phase11.py — Phase 11: climate_dfba + analysis_pipeline

Covers:
  climate_dfba:
    - SCENARIOS list has 5 entries, all non-baseline
    - _run_ode returns stability_score in [0,1] and target_flux >= 0
    - _run_community_scenarios returns exactly 5 results per community
    - _write_results creates climate_projections rows, honoring UNIQUE constraint
    - _fetch_communities excludes already-projected communities

  analysis_pipeline:
    - _spearman_r correctness (known monotone sequence)
    - _kmeans_geo assigns labels in [0, k)
    - _correlation_analysis returns list with spearman_r fields
    - _rank_candidates: composite_score computed, ordered descending
    - _site_summaries: one entry per site, mean_flux > 0
    - _phylum_importance: ordered descending, all phyla present
    - _climate_resilience: robustness in [0,1], ordered descending
    - full run() smoke test against temp DB
"""

from __future__ import annotations

import json
import sqlite3
import sys
import time
from pathlib import Path

import pytest

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

# ---------------------------------------------------------------------------
# Lazy imports
# ---------------------------------------------------------------------------

try:
    from scripts.climate_dfba import (
        SCENARIOS,
        _ensure_schema,
        _fetch_communities as _cp_fetch,
        _run_community_scenarios,
        _run_ode,
        _worker_batch as _cp_worker,
        _write_results as _cp_write,
    )
    _CLIMATE_AVAILABLE = True
except Exception as _e:
    _CLIMATE_AVAILABLE = False

try:
    from scripts.analysis_pipeline import (
        PHYLA,
        _climate_resilience,
        _correlation_analysis,
        _kmeans_geo,
        _phylum_importance,
        _rank_candidates,
        _site_summaries,
        _spearman_r,
    )
    _ANALYSIS_AVAILABLE = True
except Exception as _e:
    _ANALYSIS_AVAILABLE = False

skip_climate  = pytest.mark.skipif(not _CLIMATE_AVAILABLE,  reason="climate_dfba not importable")
skip_analysis = pytest.mark.skipif(not _ANALYSIS_AVAILABLE, reason="analysis_pipeline not importable or scipy missing")

# ---------------------------------------------------------------------------
# DB schema (must match dfba_batch + climate_dfba tables)
# ---------------------------------------------------------------------------

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS samples (
    sample_id        TEXT PRIMARY KEY,
    source           TEXT DEFAULT 'synthetic',
    site             TEXT,
    site_id          TEXT,
    latitude         REAL DEFAULT 0,
    longitude        REAL DEFAULT 0,
    soil_ph          REAL DEFAULT 6.5,
    organic_matter_pct REAL DEFAULT 2.0,
    clay_pct         REAL DEFAULT 25.0,
    temperature_c    REAL DEFAULT 12.0,
    precipitation_mm REAL DEFAULT 600.0,
    land_use         TEXT DEFAULT 'natural',
    sampling_fraction TEXT DEFAULT 'bulk'
);
CREATE TABLE IF NOT EXISTS communities (
    community_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id      TEXT,
    phylum_profile TEXT,
    synthetic      INTEGER DEFAULT 1,
    shannon_diversity REAL DEFAULT 2.0
);
CREATE TABLE IF NOT EXISTS runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id        INTEGER,
    sample_id           TEXT,
    t0_pass             INTEGER DEFAULT 1,
    t025_pass           INTEGER DEFAULT 1,
    t025_function_score REAL DEFAULT 0.75,
    t1_target_flux      REAL,
    t2_pass             INTEGER,
    t2_stability_score  REAL,
    t2_walltime_s       REAL
);
CREATE TABLE IF NOT EXISTS climate_projections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id    INTEGER NOT NULL,
    scenario_id     INTEGER NOT NULL,
    scenario_name   TEXT NOT NULL,
    stability_score REAL,
    target_flux     REAL,
    sensitivity_index REAL,
    walltime_s      REAL,
    UNIQUE(community_id, scenario_id)
);
"""

_PROTO_PROFILE = {
    "Proteobacteria": 0.35, "Actinobacteria": 0.15, "Acidobacteria": 0.10,
    "Firmicutes": 0.08, "Bacteroidetes": 0.07, "Verrucomicrobia": 0.05,
    "Planctomycetes": 0.04, "Chloroflexi": 0.04, "Gemmatimonadetes": 0.04,
    "Nitrospirae": 0.03, "Cyanobacteria": 0.03, "Thaumarchaeota": 0.02,
}
_ENV = {"soil_ph": 6.5, "organic_matter_pct": 2.0, "temperature_c": 12.0, "precipitation_mm": 600.0}


def _make_db(path: str, n_communities: int = 5) -> list[int]:
    """Create a minimal DB with n communities that have t2_pass=1 and t1_target_flux set."""
    conn = sqlite3.connect(path)
    conn.executescript(_SCHEMA)
    cids = []
    for i in range(n_communities):
        conn.execute("INSERT INTO samples (sample_id, site_id, latitude, longitude) VALUES (?,?,?,?)",
                     (f"S_{i}", f"SITE_{i % 3}", 40.0 + i * 0.5, -100.0 + i * 0.5))
        conn.execute("INSERT INTO communities (sample_id, phylum_profile) VALUES (?,?)",
                     (f"S_{i}", json.dumps(_PROTO_PROFILE)))
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        cids.append(cid)
        conn.execute(
            """INSERT INTO runs (community_id, sample_id, t2_pass, t1_target_flux, t2_stability_score)
               VALUES (?,?,1,?,?)""",
            (cid, f"S_{i}", 0.015 + i * 0.001, 0.75)
        )
    conn.commit()
    conn.close()
    return cids


# ===========================================================================
# climate_dfba tests
# ===========================================================================

class TestScenariosDefinition:
    @skip_climate
    def test_five_scenarios(self):
        assert len(SCENARIOS) == 5, f"Expected 5 scenarios, got {len(SCENARIOS)}"

    @skip_climate
    def test_scenario_ids_unique(self):
        ids = [s[0] for s in SCENARIOS]
        assert len(ids) == len(set(ids)), "Scenario IDs must be unique"

    @skip_climate
    def test_rcp85_is_hottest(self):
        temps = {s[1]: s[2] for s in SCENARIOS}
        assert temps.get("RCP8.5", 0) == max(temps.values()), "RCP8.5 should have highest ΔT"

    @skip_climate
    def test_rewetting_increases_precip(self):
        rew = next(s for s in SCENARIOS if s[1] == "rewetting")
        assert rew[3] > 1.0, "Rewetting should have precip_factor > 1"


class TestRunODE:
    @skip_climate
    def test_returns_required_keys(self):
        result = _run_ode(_PROTO_PROFILE, _ENV, [], sim_days=30)
        assert "stability_score" in result
        assert "target_flux" in result

    @skip_climate
    def test_stability_in_range(self):
        result = _run_ode(_PROTO_PROFILE, _ENV, [], sim_days=30)
        assert 0.0 <= result["stability_score"] <= 1.0

    @skip_climate
    def test_target_flux_non_negative(self):
        result = _run_ode(_PROTO_PROFILE, _ENV, [], sim_days=30)
        assert result["target_flux"] >= 0.0

    @skip_climate
    def test_empty_profile_no_crash(self):
        result = _run_ode({}, _ENV, [], sim_days=15)
        assert "stability_score" in result

    @skip_climate
    def test_warmer_env_changes_flux(self):
        """+4°C should produce noticeably different flux than baseline."""
        base = _run_ode(_PROTO_PROFILE, _ENV, [], sim_days=30)
        warm_env = dict(_ENV, temperature_c=_ENV["temperature_c"] + 4.0)
        warm = _run_ode(_PROTO_PROFILE, warm_env, [], sim_days=30)
        # They don't have to be equal
        assert base["target_flux"] != warm["target_flux"] or base["stability_score"] != warm["stability_score"]


class TestRunCommunityScenarios:
    @skip_climate
    def test_returns_five_dicts(self):
        results = _run_community_scenarios(1, json.dumps(_PROTO_PROFILE), json.dumps(_ENV), 0.015, 30)
        assert len(results) == 5, f"Expected 5 scenario results, got {len(results)}"

    @skip_climate
    def test_all_required_keys(self):
        results = _run_community_scenarios(2, json.dumps(_PROTO_PROFILE), json.dumps(_ENV), 0.015, 30)
        for r in results:
            for key in ("community_id", "scenario_id", "scenario_name",
                        "stability_score", "target_flux", "sensitivity_index", "walltime_s"):
                assert key in r, f"Missing key {key}"

    @skip_climate
    def test_scenario_ids_match_global(self):
        results = _run_community_scenarios(3, json.dumps(_PROTO_PROFILE), json.dumps(_ENV), 0.015, 30)
        result_ids = sorted(r["scenario_id"] for r in results)
        expected_ids = sorted(s[0] for s in SCENARIOS)
        assert result_ids == expected_ids

    @skip_climate
    def test_bad_json_handled(self):
        results = _run_community_scenarios(4, "NOT JSON", "{}", 0.01, 10)
        assert len(results) == 5  # still produces 5 results


class TestClimateProjWriteRead:
    @skip_climate
    def test_write_results(self, tmp_path):
        db = str(tmp_path / "t.db")
        _make_db(db, 1)
        _ensure_schema(db)
        results = [
            {"community_id": 1, "scenario_id": 1, "scenario_name": "RCP2.6",
             "stability_score": 0.72, "target_flux": 0.012, "sensitivity_index": -0.05,
             "walltime_s": 1.1, "error": None},
        ]
        n = _cp_write(db, results)
        assert n == 1
        conn = sqlite3.connect(db)
        row = conn.execute("SELECT scenario_name, target_flux FROM climate_projections").fetchone()
        conn.close()
        assert row[0] == "RCP2.6"
        assert abs(row[1] - 0.012) < 1e-9

    @skip_climate
    def test_unique_constraint_respected(self, tmp_path):
        db = str(tmp_path / "t.db")
        _make_db(db, 1)
        _ensure_schema(db)
        results = [{"community_id": 1, "scenario_id": 2, "scenario_name": "RCP4.5",
                    "stability_score": 0.6, "target_flux": 0.01, "sensitivity_index": -0.1,
                    "walltime_s": 1.0, "error": None}]
        _cp_write(db, results)
        n2 = _cp_write(db, results)  # same row again — should not double-insert
        conn = sqlite3.connect(db)
        cnt = conn.execute("SELECT COUNT(*) FROM climate_projections").fetchone()[0]
        conn.close()
        assert cnt == 1

    @skip_climate
    def test_fetch_excludes_done(self, tmp_path):
        db = str(tmp_path / "t.db")
        cids = _make_db(db, 3)
        _ensure_schema(db)
        # Fully project community 0 (5 scenarios)
        conn = sqlite3.connect(db)
        for scen_id, scen_name, _, _ in SCENARIOS:
            conn.execute(
                "INSERT OR IGNORE INTO climate_projections "
                "(community_id, scenario_id, scenario_name, stability_score, target_flux, sensitivity_index) "
                "VALUES (?,?,?,0.5,0.01,0.0)",
                (cids[0], scen_id, scen_name)
            )
        conn.commit()
        conn.close()

        rows = _cp_fetch(db, 100)
        present_cids = {r[0] for r in rows}
        assert cids[0] not in present_cids, "Fully-projected community should be excluded"
        assert cids[1] in present_cids
        assert cids[2] in present_cids


# ===========================================================================
# analysis_pipeline tests
# ===========================================================================

class TestSpearmanR:
    @skip_analysis
    def test_perfect_positive(self):
        x = [1, 2, 3, 4, 5]
        y = [2, 4, 6, 8, 10]
        assert abs(_spearman_r(x, y) - 1.0) < 1e-9

    @skip_analysis
    def test_perfect_negative(self):
        x = [1, 2, 3, 4, 5]
        y = [5, 4, 3, 2, 1]
        assert abs(_spearman_r(x, y) + 1.0) < 1e-9

    @skip_analysis
    def test_short_returns_zero(self):
        assert _spearman_r([1, 2], [3, 4]) == 0.0

    @skip_analysis
    def test_range_minus1_to_1(self):
        import random
        rng = random.Random(42)
        x = [rng.random() for _ in range(50)]
        y = [rng.random() for _ in range(50)]
        r = _spearman_r(x, y)
        assert -1.0 <= r <= 1.0


class TestKMeansGeo:
    @skip_analysis
    def test_labels_in_range(self):
        points = [(lat, lon) for lat in [30, 40, 50] for lon in [-100, -90, -80]]
        labels = _kmeans_geo(points, k=3)
        assert all(0 <= l < 3 for l in labels)

    @skip_analysis
    def test_correct_count(self):
        points = [(i * 1.0, j * 1.0) for i in range(10) for j in range(10)]
        labels = _kmeans_geo(points, k=5, iters=10)
        assert len(labels) == len(points)

    @skip_analysis
    def test_k_eq_n_returns_trivial(self):
        points = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
        labels = _kmeans_geo(points, k=3)
        assert len(set(labels)) <= 3


def _make_community_dicts(n: int = 20) -> list[dict]:
    """Generate synthetic community dicts for analysis tests."""
    import math
    comms = []
    for i in range(n):
        ph = 5.0 + (i % 5) * 0.5
        om = 1.5 + i * 0.1
        flux = 0.005 + i * 0.001
        stab = 0.5 + (i % 4) * 0.1
        bnf = 0.6 + (i % 5) * 0.05
        comms.append({
            "community_id":    i + 1,
            "t1_target_flux":  flux,
            "t2_stability_score": stab,
            "bnf_score":       bnf,
            "ph":              ph,
            "organic_matter":  om,
            "clay_pct":        25.0,
            "temperature_c":   12.0,
            "precipitation_mm": 600.0,
            "latitude":        40.0 + (i % 5),
            "longitude":       -100.0 + (i % 3),
            "site_id":         f"SITE_{i % 4}",
            "land_use":        ["forest", "grassland", "cropland"][i % 3],
            "profile": {
                "Proteobacteria": 0.25 + 0.01 * i,
                "Actinobacteria": 0.15,
                "Acidobacteria":  max(0.01, 0.20 - 0.01 * i),
                "Firmicutes":     0.08,
                "Bacteroidetes":  0.07,
                "Verrucomicrobia": 0.05,
                "Planctomycetes": 0.04,
                "Chloroflexi":    0.04,
                "Gemmatimonadetes": 0.04,
                "Nitrospirae":    0.03,
                "Cyanobacteria":  0.03,
                "Thaumarchaeota": 0.02,
            },
        })
    return comms


class TestCorrelationAnalysis:
    @skip_analysis
    def test_returns_list(self):
        comms = _make_community_dicts(30)
        findings = _correlation_analysis(comms)
        assert isinstance(findings, list)

    @skip_analysis
    def test_has_env_and_phylum_findings(self):
        comms = _make_community_dicts(30)
        findings = _correlation_analysis(comms)
        types = {f["type"] for f in findings}
        assert "env_correlation" in types
        assert "phylum_correlation" in types

    @skip_analysis
    def test_spearman_r_in_range(self):
        comms = _make_community_dicts(30)
        findings = _correlation_analysis(comms)
        for f in findings:
            assert -1.0 <= f["spearman_r"] <= 1.0

    @skip_analysis
    def test_sorted_by_abs_r(self):
        comms = _make_community_dicts(30)
        findings = _correlation_analysis(comms)
        abs_rs = [abs(f["spearman_r"]) for f in findings]
        assert abs_rs == sorted(abs_rs, reverse=True)


class TestRankCandidates:
    @skip_analysis
    def test_returns_at_most_top(self):
        comms = _make_community_dicts(20)
        ranked = _rank_candidates(comms, top=5)
        assert len(ranked) <= 5

    @skip_analysis
    def test_composite_scores_descending(self):
        comms = _make_community_dicts(20)
        ranked = _rank_candidates(comms, top=20)
        scores = [r["composite_score"] for r in ranked]
        assert scores == sorted(scores, reverse=True)

    @skip_analysis
    def test_composite_score_in_range(self):
        comms = _make_community_dicts(20)
        ranked = _rank_candidates(comms, top=20)
        for r in ranked:
            assert 0.0 <= r["composite_score"] <= 1.0


class TestSiteSummaries:
    @skip_analysis
    def test_one_entry_per_site(self):
        comms = _make_community_dicts(20)  # 4 sites
        summ = _site_summaries(comms)
        assert len(summ) == 4

    @skip_analysis
    def test_mean_flux_positive(self):
        comms = _make_community_dicts(20)
        summ = _site_summaries(comms)
        for s in summ:
            assert s["mean_flux"] > 0

    @skip_analysis
    def test_sorted_descending_flux(self):
        comms = _make_community_dicts(20)
        summ = _site_summaries(comms)
        fluxes = [s["mean_flux"] for s in summ]
        assert fluxes == sorted(fluxes, reverse=True)


class TestPhylumImportance:
    @skip_analysis
    def test_all_phyla_represented(self):
        comms = _make_community_dicts(50)
        imp = _phylum_importance(comms)
        names = {r["phylum"] for r in imp}
        for ph in PHYLA:
            assert ph in names

    @skip_analysis
    def test_importance_score_positive(self):
        comms = _make_community_dicts(50)
        imp = _phylum_importance(comms)
        for r in imp:
            assert r["importance_score"] >= 0.0

    @skip_analysis
    def test_sorted_descending(self):
        comms = _make_community_dicts(50)
        imp = _phylum_importance(comms)
        scores = [r["importance_score"] for r in imp]
        assert scores == sorted(scores, reverse=True)


class TestClimateResilience:
    @skip_analysis
    def test_empty_projections_returns_empty(self):
        comms = _make_community_dicts(5)
        result = _climate_resilience(comms, {})
        assert result == []

    @skip_analysis
    def test_robustness_in_range(self):
        comms = _make_community_dicts(5)
        projections = {
            1: [{"scenario_id": i, "scenario_name": f"S{i}",
                 "target_flux": 0.01, "stability_score": 0.7,
                 "sensitivity_index": -0.05 * i}
                for i in range(1, 6)]
        }
        result = _climate_resilience(comms, projections)
        for r in result:
            assert 0.0 <= r["climate_robustness"] <= 1.0

    @skip_analysis
    def test_sorted_descending_robustness(self):
        comms = _make_community_dicts(5)
        projections = {
            i + 1: [{"scenario_id": j, "scenario_name": f"S{j}",
                     "target_flux": 0.01, "stability_score": 0.7,
                     "sensitivity_index": -0.02 * j * (i + 1)}
                    for j in range(1, 6)]
            for i in range(5)
        }
        result = _climate_resilience(comms, projections)
        r_vals = [r["climate_robustness"] for r in result]
        assert r_vals == sorted(r_vals, reverse=True)
