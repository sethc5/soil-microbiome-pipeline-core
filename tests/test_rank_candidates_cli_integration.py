from __future__ import annotations

import csv
import importlib.util
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest


if importlib.util.find_spec("typer") is None:
    pytestmark = pytest.mark.skip(reason="typer is required for rank_candidates CLI integration tests")


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(REPO_ROOT) if not existing else f"{str(REPO_ROOT)}:{existing}"
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_rank(db_path: Path, output_path: Path, uncertainty_seed: int = 123) -> None:
    result = _run(
        [
            sys.executable,
            "-m",
            "core.analysis.rank_candidates",
            "--config",
            "configs/config.example.yaml",
            "--db",
            str(db_path),
            "--top",
            "3",
            "--output",
            str(output_path),
            "--scoring-mode",
            "hybrid",
            "--legacy-weight",
            "0.40",
            "--uncertainty-samples",
            "20",
            "--risk-aversion",
            "1.10",
            "--uncertainty-seed",
            str(uncertainty_seed),
        ]
    )
    assert result.returncode == 0, f"rank_candidates failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"


def _load_csv(path: Path) -> list[dict[str, str]]:
    assert path.exists(), f"missing output CSV: {path}"
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows, f"empty output CSV: {path}"
    return rows


def _create_v3_fixture_db(path: Path) -> None:
    if path.exists():
        path.unlink()
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE samples (
                sample_id TEXT PRIMARY KEY,
                site_id TEXT,
                latitude REAL,
                longitude REAL,
                soil_ph REAL,
                organic_matter_pct REAL,
                management TEXT
            );

            CREATE TABLE communities (
                community_id INTEGER PRIMARY KEY,
                sample_id TEXT
            );

            CREATE TABLE runs (
                run_id INTEGER PRIMARY KEY,
                community_id INTEGER,
                run_date TEXT,
                t0_pass BOOLEAN,
                t1_flux REAL,
                t1_confidence TEXT,
                t2_stability REAL,
                t2_best_intervention TEXT
            );
            """
        )

        rows = [
            ("S1", "SITE_A", 40.1, -104.9, 6.8, 5.4, '{"moisture_pct": 60}', 1, 401, 82.0, "high", 0.72, "Azospirillum"),
            ("S2", "SITE_B", 40.3, -105.1, 5.3, 2.1, '{"moisture_pct": 48}', 2, 402, 46.0, "medium", 0.56, "Compost"),
            ("S3", "SITE_C", 40.2, -104.7, 7.4, 3.8, '{"moisture_pct": 55}', 3, 403, 64.0, "high", 0.61, "Reduced tillage"),
        ]
        for (
            sample_id,
            site_id,
            latitude,
            longitude,
            soil_ph,
            organic_matter_pct,
            management,
            community_id,
            run_id,
            t1_flux,
            t1_confidence,
            t2_stability,
            t2_best_intervention,
        ) in rows:
            conn.execute(
                "INSERT INTO samples (sample_id, site_id, latitude, longitude, soil_ph, organic_matter_pct, management) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (sample_id, site_id, latitude, longitude, soil_ph, organic_matter_pct, management),
            )
            conn.execute(
                "INSERT INTO communities (community_id, sample_id) VALUES (?, ?)",
                (community_id, sample_id),
            )
            conn.execute(
                "INSERT INTO runs (run_id, community_id, run_date, t0_pass, t1_flux, t1_confidence, t2_stability, t2_best_intervention) VALUES (?, ?, datetime('now'), 1, ?, ?, ?, ?)",
                (run_id, community_id, t1_flux, t1_confidence, t2_stability, t2_best_intervention),
            )
        conn.commit()


def test_rank_cli_produces_stable_order_for_fixture(tmp_path: Path):
    db_path = tmp_path / "fixture.db"
    out_a = tmp_path / "ranked_a.csv"
    out_b = tmp_path / "ranked_b.csv"

    create_db = _run([sys.executable, "scripts/ops/create_rank_fixture_db.py", "--db", str(db_path)])
    assert create_db.returncode == 0, f"fixture creation failed:\n{create_db.stdout}\n{create_db.stderr}"

    _run_rank(db_path, out_a, uncertainty_seed=123)
    _run_rank(db_path, out_b, uncertainty_seed=123)

    rows_a = _load_csv(out_a)
    rows_b = _load_csv(out_b)

    required = {
        "composite_score",
        "score_mean",
        "score_std",
        "risk_adjusted_score",
        "uncertainty_samples_used",
        "risk_reason",
        "scoring_mode",
    }
    missing = required - set(rows_a[0].keys())
    assert not missing, f"missing columns: {sorted(missing)}"

    order_a = [row["community_id"] for row in rows_a]
    order_b = [row["community_id"] for row in rows_b]
    assert order_a == order_b, "ranking order should be deterministic for fixed seed"

    scores = [float(row["risk_adjusted_score"]) for row in rows_a]
    assert scores == sorted(scores, reverse=True)


def test_rank_cli_supports_v3_schema_layout(tmp_path: Path):
    db_path = tmp_path / "v3_layout.db"
    out_path = tmp_path / "v3_ranked.csv"
    _create_v3_fixture_db(db_path)

    _run_rank(db_path, out_path, uncertainty_seed=321)
    rows = _load_csv(out_path)

    assert len(rows) == 3
    assert rows[0]["top_intervention"] != ""
    assert all(row["t1_target_flux"] != "" for row in rows)
