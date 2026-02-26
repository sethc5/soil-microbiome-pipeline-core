"""
db_utils.py — SQLite persistence layer (SoilDB class).

All schema DDL lives here. Tables:
  samples, communities, targets, runs, interventions, taxa, findings, receipts

Usage:
  from db_utils import SoilDB
  db = SoilDB("nitrogen_landscape.db")
  db.upsert_sample(...)
"""

import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    sample_id           TEXT PRIMARY KEY,
    source              TEXT,
    source_id           TEXT,
    project_id          TEXT,
    biome               TEXT,
    feature             TEXT,
    material            TEXT,
    sequencing_type     TEXT,
    sequencing_depth    INTEGER,
    n_taxa              INTEGER,
    latitude            REAL,
    longitude           REAL,
    country             TEXT,
    climate_zone        TEXT,
    soil_ph             REAL,
    soil_texture        TEXT,
    clay_pct            REAL,
    sand_pct            REAL,
    silt_pct            REAL,
    bulk_density        REAL,
    organic_matter_pct  REAL,
    total_nitrogen_ppm  REAL,
    available_p_ppm     REAL,
    cec                 REAL,
    moisture_pct        REAL,
    temperature_c       REAL,
    precipitation_mm    REAL,
    land_use            TEXT,
    management          TEXT,
    sampling_depth_cm   REAL,
    sampling_season     TEXT,
    sampling_date       TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS communities (
    community_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,
    shannon_diversity   REAL,
    simpson_diversity   REAL,
    chao1_richness      REAL,
    observed_otus       INTEGER,
    pielou_evenness     REAL,
    faith_pd            REAL,
    has_nifh            BOOLEAN,
    has_dsrab           BOOLEAN,
    has_mcra            BOOLEAN,
    has_mmox            BOOLEAN,
    has_amoa            BOOLEAN,
    functional_genes    TEXT,
    phylum_profile      TEXT,
    top_genera          TEXT,
    otu_table_path      TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS targets (
    target_id           TEXT PRIMARY KEY,
    application         TEXT,
    description         TEXT,
    target_function     TEXT,
    target_flux         TEXT,
    soil_context        TEXT,
    crop_context        TEXT,
    intervention_types  TEXT,
    off_targets         TEXT,
    reference_communities TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,
    community_id        INTEGER REFERENCES communities,
    target_id           TEXT REFERENCES targets,
    t0_pass             BOOLEAN,
    t0_reject_reason    TEXT,
    t0_depth_ok         BOOLEAN,
    t0_metadata_ok      BOOLEAN,
    t0_functional_genes_ok BOOLEAN,
    t025_pass           BOOLEAN,
    t025_model          TEXT,
    t025_function_score REAL,
    t025_similarity_hit TEXT,
    t025_similarity_score REAL,
    t025_uncertainty    REAL,
    t1_pass             BOOLEAN,
    t1_model_size       INTEGER,
    t1_target_flux      REAL,
    t1_flux_units       TEXT,
    t1_feasible         BOOLEAN,
    t1_keystone_taxa    TEXT,
    t1_walltime_s       REAL,
    t2_pass             BOOLEAN,
    t2_stability_score  REAL,
    t2_best_intervention TEXT,
    t2_intervention_effect REAL,
    t2_establishment_prob REAL,
    t2_off_target_impact TEXT,
    t2_walltime_s       REAL,
    tier_reached        INTEGER,
    run_date            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    machine_id          TEXT
);

CREATE TABLE IF NOT EXISTS interventions (
    intervention_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              INTEGER REFERENCES runs,
    intervention_type   TEXT,
    intervention_detail TEXT,
    predicted_effect    REAL,
    confidence          REAL,
    stability_under_perturbation REAL,
    cost_estimate       TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS taxa (
    taxon_id            TEXT PRIMARY KEY,
    name                TEXT,
    rank                TEXT,
    phylum              TEXT,
    class               TEXT,
    order_name          TEXT,
    family              TEXT,
    genus               TEXT,
    species             TEXT,
    functional_roles    TEXT,
    genome_accession    TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS findings (
    finding_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title               TEXT,
    description         TEXT,
    sample_ids          TEXT,
    taxa_ids            TEXT,
    statistical_support TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS receipts (
    receipt_id          TEXT PRIMARY KEY,
    machine_id          TEXT,
    batch_start         TIMESTAMP,
    batch_end           TIMESTAMP,
    n_samples_processed INTEGER,
    n_fba_runs          INTEGER,
    n_dynamics_runs     INTEGER,
    status              TEXT,
    filepath            TEXT
);
"""


class SoilDB:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> "SoilDB":
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA_SQL)
        self._conn.commit()
        return self

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        if self._conn:
            self._conn.close()

    # --- stub CRUD methods (to be implemented) ---

    def upsert_sample(self, record: dict) -> None:
        raise NotImplementedError

    def upsert_community(self, record: dict) -> int:
        raise NotImplementedError

    def insert_run(self, record: dict) -> int:
        raise NotImplementedError

    def update_run(self, run_id: int, updates: dict) -> None:
        raise NotImplementedError

    def insert_finding(self, record: dict) -> int:
        raise NotImplementedError

    def insert_intervention(self, record: dict) -> int:
        raise NotImplementedError
