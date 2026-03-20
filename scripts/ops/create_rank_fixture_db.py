from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    site_id TEXT,
    latitude REAL,
    longitude REAL,
    soil_ph REAL,
    temperature_c REAL,
    organic_matter_pct REAL,
    management TEXT
);

CREATE TABLE IF NOT EXISTS communities (
    community_id INTEGER PRIMARY KEY,
    sample_id TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY,
    community_id INTEGER,
    run_date TEXT,
    t0_pass BOOLEAN,
    t0_depth_ok BOOLEAN,
    t025_model TEXT,
    t025_n_pathways INTEGER,
    t025_nsti_mean REAL,
    t1_target_flux REAL,
    t1_model_confidence TEXT,
    t1_metabolic_exchanges TEXT,
    t2_stability_score REAL,
    t2_resistance REAL,
    t2_resilience REAL,
    t2_functional_redundancy REAL,
    t2_interventions TEXT
);
"""


def create_fixture_db(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    rows = [
        {
            "sample_id": "S1",
            "site_id": "SITE_A",
            "latitude": 40.10,
            "longitude": -104.95,
            "soil_ph": 6.8,
            "temperature_c": 24.0,
            "organic_matter_pct": 5.8,
            "management": '{"moisture_pct": 62, "land_use": "cropland"}',
            "community_id": 1,
            "run_id": 101,
            "t1_target_flux": 82.0,
            "t1_model_confidence": "high",
            "t2_stability_score": 0.71,
            "t2_resistance": 0.65,
            "t2_resilience": 0.63,
            "t2_functional_redundancy": 0.58,
            "t2_interventions": '[{"intervention_type":"bioinoculant","intervention_detail":"Azospirillum","predicted_effect":0.74,"establishment_prob":0.66}]',
        },
        {
            "sample_id": "S2",
            "site_id": "SITE_B",
            "latitude": 39.82,
            "longitude": -105.21,
            "soil_ph": 5.2,
            "temperature_c": 22.0,
            "organic_matter_pct": 2.2,
            "management": '{"moisture_pct": 46, "land_use": "grassland"}',
            "community_id": 2,
            "run_id": 102,
            "t1_target_flux": 45.0,
            "t1_model_confidence": "medium",
            "t2_stability_score": 0.56,
            "t2_resistance": 0.54,
            "t2_resilience": 0.50,
            "t2_functional_redundancy": 0.47,
            "t2_interventions": '[{"intervention_type":"amendment","intervention_detail":"compost @5t/ha","predicted_effect":0.53,"rate_t_ha":5.0}]',
        },
        {
            "sample_id": "S3",
            "site_id": "SITE_C",
            "latitude": 41.02,
            "longitude": -104.50,
            "soil_ph": 7.5,
            "temperature_c": 27.5,
            "organic_matter_pct": 3.9,
            "management": '{"moisture_pct": 58, "land_use": "forest"}',
            "community_id": 3,
            "run_id": 103,
            "t1_target_flux": 64.0,
            "t1_model_confidence": "high",
            "t2_stability_score": 0.61,
            "t2_resistance": 0.58,
            "t2_resilience": 0.57,
            "t2_functional_redundancy": 0.52,
            "t2_interventions": '[{"intervention_type":"management","intervention_detail":"reduced_tillage","predicted_effect":0.41,"confidence":0.72}]',
        },
    ]

    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA_SQL)
        for row in rows:
            conn.execute(
                """
                INSERT INTO samples (
                    sample_id, site_id, latitude, longitude, soil_ph, temperature_c,
                    organic_matter_pct, management
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["sample_id"],
                    row["site_id"],
                    row["latitude"],
                    row["longitude"],
                    row["soil_ph"],
                    row["temperature_c"],
                    row["organic_matter_pct"],
                    row["management"],
                ),
            )
            conn.execute(
                "INSERT INTO communities (community_id, sample_id) VALUES (?, ?)",
                (row["community_id"], row["sample_id"]),
            )
            conn.execute(
                """
                INSERT INTO runs (
                    run_id, community_id, run_date, t0_pass, t0_depth_ok,
                    t025_model, t025_n_pathways, t025_nsti_mean,
                    t1_target_flux, t1_model_confidence, t1_metabolic_exchanges,
                    t2_stability_score, t2_resistance, t2_resilience,
                    t2_functional_redundancy, t2_interventions
                ) VALUES (?, ?, datetime('now'), 1, 1, 'picrust2', 12, 0.15, ?, ?, '{}', ?, ?, ?, ?, ?)
                """,
                (
                    row["run_id"],
                    row["community_id"],
                    row["t1_target_flux"],
                    row["t1_model_confidence"],
                    row["t2_stability_score"],
                    row["t2_resistance"],
                    row["t2_resilience"],
                    row["t2_functional_redundancy"],
                    row["t2_interventions"],
                ),
            )
        conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a tiny fixture DB for rank_candidates CI checks.")
    parser.add_argument("--db", type=str, required=True, help="Output SQLite DB path.")
    args = parser.parse_args()
    create_fixture_db(args.db)
    print(f"Fixture DB created at {args.db}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
