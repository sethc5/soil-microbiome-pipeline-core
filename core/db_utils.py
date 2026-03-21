"""
db_utils.py — Unified Persistence Layer (SoilDB).

Normalized v3 schema:
  - Decouples functional traits into 'annotations' table.
  - Removes hardcoded application-specific columns.
  - Retains high-performance WAL mode and PRAGMA settings.
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    sample_id           TEXT PRIMARY KEY,
    source              TEXT,               -- 'sra','mgnify','neon','local'
    site_id             TEXT,               -- stable site identifier
    visit_number        INTEGER,            -- chronological order at site
    latitude            REAL,
    longitude           REAL,
    soil_ph             REAL,
    soil_texture        TEXT,
    organic_matter_pct  REAL,
    climate_zone        TEXT,
    land_use            TEXT,
    sampling_date       TEXT,
    management          TEXT,               -- Catch-all for extra source-specific fields
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS communities (
    community_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,
    phylum_profile      TEXT,               -- JSON: phylum -> rel abundance
    top_genera          TEXT,               -- JSON: top 50 genera
    otu_table_path      TEXT,
    shannon_diversity   REAL,
    pielou_evenness     REAL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS annotations (
    annotation_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id       INTEGER REFERENCES communities,
    trait_name         TEXT,                -- 'nifH', 'alkB', 'laccase'
    value              REAL,                -- e.g., relative abundance
    is_present         BOOLEAN,
    method             TEXT,                -- 'mmseqs2', 'keyword'
    meta_json          TEXT,                -- extra flags (e.g., hgt_flagged)
    UNIQUE(community_id, trait_name)
);

CREATE TABLE IF NOT EXISTS targets (
    target_id           TEXT PRIMARY KEY,
    application         TEXT,
    config_json         TEXT                -- Serialized PipelineConfig for this target
);

CREATE TABLE IF NOT EXISTS runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    community_id        INTEGER REFERENCES communities,
    target_id           TEXT REFERENCES targets,
    tier_reached        INTEGER,             -- 0, 1, 2
    t0_pass             BOOLEAN,
    
    -- T1 results
    t1_pass             BOOLEAN,
    t1_flux             REAL,
    t1_confidence       TEXT,                -- 'high','medium','low'
    
    -- T2 results
    t2_pass             BOOLEAN,
    t2_stability        REAL,
    t2_best_intervention TEXT,
    
    run_date            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_samples_site ON samples(site_id);
CREATE INDEX IF NOT EXISTS idx_annotations_trait ON annotations(trait_name);
"""

class SoilDB:
    def __init__(self, db_path: str | Path = ":memory:"):
        self.db_path = str(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> "SoilDB":
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA_SQL)
        return self

    def __enter__(self) -> "SoilDB":
        return self.connect()

    def __exit__(self, *args) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.connect()
        return self._conn # type: ignore

    def upsert_sample(self, record: Dict[str, Any]) -> None:
        cols = list(record.keys())
        placeholders = ", ".join("?" * len(cols))
        sql = f"INSERT OR REPLACE INTO samples ({', '.join(cols)}) VALUES ({placeholders})"
        self.conn.execute(sql, list(record.values()))
        self.conn.commit()

    def add_annotation(self, community_id: int, trait: str, value: float, present: bool, method: str, meta: Optional[Dict] = None):
        self.conn.execute(
            "INSERT OR REPLACE INTO annotations (community_id, trait_name, value, is_present, method, meta_json) VALUES (?, ?, ?, ?, ?, ?)",
            (community_id, trait, value, 1 if present else 0, method, json.dumps(meta or {}))
        )
        self.conn.commit()

    def update_community_t1(self, community_id: int, result: Dict[str, Any]):
        # Simplified update for the new engine
        self.conn.execute(
            "UPDATE runs SET t1_pass = ?, t1_flux = ?, tier_reached = 1 WHERE community_id = ?",
            (1 if result.get("t1_pass") else 0, result.get("target_flux"), community_id)
        )
        self.conn.commit()

    # Convenience getters
    def get_community(self, community_id: int) -> Dict[str, Any]:
        row = self.conn.execute("SELECT * FROM communities WHERE community_id = ?", (community_id,)).fetchone()
        return dict(row) if row else {}

    def get_sample_metadata(self, sample_id: str) -> Dict[str, Any]:
        row = self.conn.execute("SELECT * FROM samples WHERE sample_id = ?", (sample_id,)).fetchone()
        return dict(row) if row else {}

def _db_connect(db_path):
    """Legacy compatibility: return a raw sqlite3 connection."""
    import sqlite3
    return sqlite3.connect(db_path)

