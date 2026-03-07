"""
db_utils.py — SQLite persistence layer (SoilDB class).

All schema DDL lives here. Tables:
  samples, communities, targets, runs, interventions, taxa, findings, receipts

Schema version: 2 — adds fungi/ITS, archaea, rhizosphere fraction,
time-series site linkage, and T1 confidence columns (see REBUILD_PLAN.md Phase 0.1).

Usage:
  from db_utils import SoilDB
  with SoilDB("nitrogen_landscape.db") as db:
      db.upsert_sample({...})
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL — v2
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    sample_id           TEXT PRIMARY KEY,
    source              TEXT,               -- 'sra','mgnify','emp','qiita','neon','local'
    source_id           TEXT,
    project_id          TEXT,
    biome               TEXT,               -- ENVO biome term
    feature             TEXT,
    material            TEXT,
    sequencing_type     TEXT,               -- '16S','ITS','shotgun_metagenome','metatranscriptome'
    sequencing_depth    INTEGER,
    n_taxa              INTEGER,

    -- Geographic / temporal
    latitude            REAL,
    longitude           REAL,
    country             TEXT,
    climate_zone        TEXT,               -- Koppen-Geiger

    -- Environmental metadata (load-bearing — see REBUILD_PLAN.md Gap 4)
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
    management          TEXT,               -- JSON
    sampling_depth_cm   REAL,
    sampling_season     TEXT,
    sampling_date       TEXT,

    -- Time-series support (Gap 8 — NEON multi-visit sites)
    site_id             TEXT,               -- stable site identifier across visits
    visit_number        INTEGER,            -- chronological order at site

    -- Rhizosphere / bulk distinction (Gap 4)
    sampling_fraction   TEXT,               -- 'rhizosphere','endosphere','bulk','litter'

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_samples_site ON samples(site_id);
CREATE INDEX IF NOT EXISTS idx_samples_source ON samples(source);

CREATE TABLE IF NOT EXISTS communities (
    community_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,

    -- Alpha diversity (T0)
    shannon_diversity   REAL,
    simpson_diversity   REAL,
    chao1_richness      REAL,
    observed_otus       INTEGER,
    pielou_evenness     REAL,
    faith_pd            REAL,

    -- Bacterial functional genes (T0 filter)
    has_nifh            BOOLEAN,            -- nitrogen fixation
    has_dsrab           BOOLEAN,            -- sulfate reduction
    has_mcra            BOOLEAN,            -- methanogenesis
    has_mmox            BOOLEAN,            -- methane oxidation
    has_amoa_bacterial  BOOLEAN,            -- bacterial nitrification (Gap 2 — split from has_amoa)
    has_amoa_archaeal   BOOLEAN,            -- archaeal AOA nitrification (Gap 2)
    has_laccase         BOOLEAN,            -- lignin degradation / C-sequestration
    has_peroxidase      BOOLEAN,            -- lignin degradation / C-sequestration
    nifh_is_hgt_flagged BOOLEAN,            -- nifH present but HGT/non-functional risk (Gap 7)
    functional_genes    TEXT,               -- JSON: full gene profile with abundances

    -- Fungal profile (Gap 1)
    fungal_bacterial_ratio REAL,            -- ITS/16S ratio proxy
    its_profile         TEXT,               -- JSON: ITS fungal taxonomy profile

    -- Expression ratio (Gap 3 — when paired metatranscriptome exists)
    mrna_to_dna_ratio   REAL,

    -- Taxonomic profiles
    phylum_profile      TEXT,               -- JSON: phylum -> rel abundance
    top_genera          TEXT,               -- JSON: top 50 genera
    otu_table_path      TEXT,               -- path to full OTU/ASV table file

    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_communities_sample ON communities(sample_id);

CREATE TABLE IF NOT EXISTS targets (
    target_id           TEXT PRIMARY KEY,
    application         TEXT,
    description         TEXT,
    target_function     TEXT,
    target_flux         TEXT,               -- JSON
    soil_context        TEXT,               -- JSON
    crop_context        TEXT,
    intervention_types  TEXT,               -- JSON
    off_targets         TEXT,               -- JSON
    reference_communities TEXT              -- JSON
);

CREATE TABLE IF NOT EXISTS runs (
    run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id           TEXT REFERENCES samples,
    community_id        INTEGER REFERENCES communities,
    target_id           TEXT REFERENCES targets,

    -- T0
    t0_pass             BOOLEAN,
    t0_reject_reason    TEXT,
    t0_depth_ok         BOOLEAN,
    t0_metadata_ok      BOOLEAN,
    t0_functional_genes_ok BOOLEAN,

    -- T0.25
    t025_pass           BOOLEAN,
    t025_model          TEXT,
    t025_function_score REAL,
    t025_similarity_hit TEXT,
    t025_similarity_score REAL,
    t025_uncertainty    REAL,

    -- T1 — metabolic modeling with confidence (Gap 5, Gap 9)
    t1_pass             BOOLEAN,
    t1_model_size       INTEGER,
    t1_target_flux      REAL,
    t1_flux_lower_bound REAL,               -- FVA lower bound (Gap 9)
    t1_flux_upper_bound REAL,               -- FVA upper bound (Gap 9)
    t1_flux_units       TEXT,
    t1_feasible         BOOLEAN,
    t1_keystone_taxa    TEXT,               -- JSON
    t1_genome_completeness_mean REAL,       -- mean CheckM completeness (Gap 5)
    t1_genome_contamination_mean REAL,      -- mean CheckM contamination (Gap 5)
    t1_model_confidence TEXT,               -- 'high','medium','low' (Gap 9)
    t1_walltime_s       REAL,

    -- T2 — dynamics + intervention with propagated confidence (Gap 9)
    t2_pass             BOOLEAN,
    t2_stability_score  REAL,
    t2_best_intervention TEXT,              -- JSON
    t2_intervention_effect REAL,
    t2_establishment_prob REAL,
    t2_off_target_impact TEXT,              -- JSON
    t2_confidence       TEXT,               -- propagated from t1_model_confidence
    t2_walltime_s       REAL,

    tier_reached        INTEGER,
    run_date            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    machine_id          TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_target    ON runs(target_id);
CREATE INDEX IF NOT EXISTS idx_runs_tier      ON runs(tier_reached, target_id);
CREATE INDEX IF NOT EXISTS idx_runs_t1        ON runs(t1_pass, t1_target_flux DESC);
CREATE INDEX IF NOT EXISTS idx_runs_t2        ON runs(t2_pass, t2_stability_score DESC);
CREATE INDEX IF NOT EXISTS idx_runs_t025      ON runs(t025_pass, t025_function_score DESC);
CREATE INDEX IF NOT EXISTS idx_runs_community ON runs(community_id);

CREATE TABLE IF NOT EXISTS interventions (
    intervention_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id              INTEGER REFERENCES runs,
    intervention_type   TEXT,               -- 'bioinoculant','amendment','management'
    intervention_detail TEXT,               -- JSON
    predicted_effect    REAL,
    confidence          REAL,
    stability_under_perturbation REAL,
    cost_estimate       TEXT,               -- JSON
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
    functional_roles    TEXT,               -- JSON
    genome_accession    TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS findings (
    finding_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title               TEXT,
    description         TEXT,
    sample_ids          TEXT,               -- JSON array
    taxa_ids            TEXT,               -- JSON array
    statistical_support TEXT,               -- JSON: {p_value, effect_size, n, model_confidence_dist}
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

# ---------------------------------------------------------------------------
# Migration DDL — adds v2 columns to existing v1 databases gracefully
# ---------------------------------------------------------------------------

MIGRATION_SQL: list[tuple[str, str]] = [
    # (table, column_def) — ALTER TABLE is idempotent via try/except
    ("samples",      "site_id TEXT"),
    ("samples",      "visit_number INTEGER"),
    ("samples",      "sampling_fraction TEXT"),
    ("communities",  "has_amoa_bacterial BOOLEAN"),
    ("communities",  "has_amoa_archaeal BOOLEAN"),
    ("communities",  "has_laccase BOOLEAN"),
    ("communities",  "has_peroxidase BOOLEAN"),
    ("communities",  "nifh_is_hgt_flagged BOOLEAN"),
    ("communities",  "fungal_bacterial_ratio REAL"),
    ("communities",  "its_profile TEXT"),
    ("communities",  "mrna_to_dna_ratio REAL"),
    ("runs",         "t1_flux_lower_bound REAL"),
    ("runs",         "t1_flux_upper_bound REAL"),
    ("runs",         "t1_genome_completeness_mean REAL"),
    ("runs",         "t1_genome_contamination_mean REAL"),
    ("runs",         "t1_model_confidence TEXT"),
    ("runs",         "t2_confidence TEXT"),
    ("runs",         "t025_n_pathways INTEGER"),
    ("runs",         "t025_nsti_mean REAL"),
    ("runs",         "t1_metabolic_exchanges TEXT"),
    ("runs",         "t2_resistance REAL"),
    ("runs",         "t2_resilience REAL"),
    ("runs",         "t2_functional_redundancy REAL"),
    ("runs",         "t2_interventions TEXT"),
]



# ---------------------------------------------------------------------------
# Module-level helper — use instead of bare sqlite3.connect()
# ---------------------------------------------------------------------------


def _db_connect(db_path: str | Path, timeout: int = 30) -> sqlite3.Connection:
    """Open a raw sqlite3 connection with all pipeline performance PRAGMAs applied.

    Use this everywhere instead of bare ``sqlite3.connect()`` to ensure
    consistent WAL mode, 512 MB page cache, mmap, and temp_store settings.
    """
    conn = sqlite3.connect(str(db_path), timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-524288")   # 512 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")    # temp tables in RAM
    conn.execute("PRAGMA mmap_size=536870912")  # 512 MB memory-mapped I/O
    return conn


# ---------------------------------------------------------------------------
# SoilDB — main database class
# ---------------------------------------------------------------------------


class SoilDB:
    """SQLite persistence layer for the soil microbiome pipeline."""

    def __init__(self, db_path: str | Path = ":memory:"):
        self.db_path = Path(db_path) if db_path != ":memory:" else db_path
        self._conn: sqlite3.Connection | None = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> "SoilDB":
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # Performance tuning — safe on server with 251 GB RAM
        self._conn.execute("PRAGMA cache_size=-524288")   # 512 MB page cache
        self._conn.execute("PRAGMA temp_store=MEMORY")    # temp tables in RAM
        self._conn.execute("PRAGMA mmap_size=536870912")  # 512 MB memory-mapped I/O
        self._conn.executescript(SCHEMA_SQL)
        self._apply_migrations()
        self._conn.commit()
        return self

    def _apply_migrations(self) -> None:
        """Add v2 columns to existing v1 databases without data loss."""
        for table, col_def in MIGRATION_SQL:
            try:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
            except sqlite3.OperationalError:
                pass  # column already exists

    def __enter__(self) -> "SoilDB":
        return self.connect()

    def __exit__(self, *_) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SoilDB not connected — use as context manager or call connect()")
        return self._conn

    def _connect(self):
        """Return a context-manager that yields the raw sqlite3.Connection.

        Usage by analysis modules::
            with db._connect() as conn:
                rows = conn.execute("SELECT ...").fetchall()
        """
        import contextlib

        if self._conn is None:
            self.connect()

        @contextlib.contextmanager
        def _ctx():
            yield self.conn

        return _ctx()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    _SAFE_COL_RE = __import__("re").compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def _validate_col_names(self, names: list[str]) -> None:
        """Reject column names that don't look like valid SQL identifiers."""
        for name in names:
            if not self._SAFE_COL_RE.match(name):
                raise ValueError(f"Invalid column name: {name!r}")

    def _insert(self, table: str, record: dict, *, or_replace: bool = False) -> int:
        """INSERT (OR REPLACE) a dict into table. Returns lastrowid."""
        verb = "INSERT OR REPLACE" if or_replace else "INSERT"
        cols = list(record.keys())
        self._validate_col_names(cols)
        placeholders = ", ".join("?" * len(cols))
        col_names = ", ".join(cols)
        values = [
            json.dumps(v) if isinstance(v, (dict, list)) else v
            for v in record.values()
        ]
        cur = self.conn.execute(
            f"{verb} INTO {table} ({col_names}) VALUES ({placeholders})",
            values,
        )
        self.conn.commit()
        return cur.lastrowid  # type: ignore[return-value]

    def _update(self, table: str, pk_col: str, pk_val: Any, updates: dict) -> None:
        """UPDATE table SET col=? WHERE pk_col=pk_val."""
        cols = list(updates.keys())
        self._validate_col_names(cols)
        self._validate_col_names([pk_col])
        set_clause = ", ".join(f"{k} = ?" for k in cols)
        values = [
            json.dumps(v) if isinstance(v, (dict, list)) else v
            for v in updates.values()
        ]
        values.append(pk_val)
        self.conn.execute(
            f"UPDATE {table} SET {set_clause} WHERE {pk_col} = ?",
            values,
        )
        self.conn.commit()

    # ------------------------------------------------------------------
    # CRUD — samples
    # ------------------------------------------------------------------

    def upsert_sample(self, record: dict) -> None:
        """Insert or replace a sample record. Expects keys matching samples columns."""
        self._insert("samples", record, or_replace=True)
        logger.debug("upsert_sample: %s", record.get("sample_id"))

    def get_sample(self, sample_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM samples WHERE sample_id = ?", (sample_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_samples_by_site(self, site_id: str) -> list[dict]:
        """Return all samples from a site, ordered by visit number (time-series)."""
        rows = self.conn.execute(
            "SELECT * FROM samples WHERE site_id = ? ORDER BY visit_number ASC",
            (site_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def count_samples(self, source: str | None = None) -> int:
        if source:
            return self.conn.execute(
                "SELECT COUNT(*) FROM samples WHERE source = ?", (source,)
            ).fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM samples").fetchone()[0]

    # ------------------------------------------------------------------
    # CRUD — communities
    # ------------------------------------------------------------------

    def upsert_community(self, record: dict) -> int:
        """Insert or replace a community record. Returns community_id."""
        return self._insert("communities", record, or_replace=True)

    def get_community(self, community_id: int) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM communities WHERE community_id = ?", (community_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_community_for_sample(self, sample_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM communities WHERE sample_id = ? ORDER BY community_id DESC LIMIT 1",
            (sample_id,),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # CRUD — runs
    # ------------------------------------------------------------------

    def insert_run(self, record: dict) -> int:
        """Insert a run record. Returns run_id."""
        return self._insert("runs", record)

    def update_run(self, run_id: int, updates: dict) -> None:
        """Update arbitrary columns on an existing run row."""
        self._update("runs", "run_id", run_id, updates)

    def get_runs_by_tier(self, tier: int, target_id: str) -> list[dict]:
        """Return all runs that reached at least `tier` for a given target."""
        rows = self.conn.execute(
            "SELECT * FROM runs WHERE tier_reached >= ? AND target_id = ?",
            (tier, target_id),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_t1_confidence_distribution(self, target_id: str) -> dict:
        """Distribution of model confidence tiers for T1 runs."""
        rows = self.conn.execute(
            """SELECT t1_model_confidence, COUNT(*) as n
               FROM runs
               WHERE target_id = ? AND t1_pass = 1
               GROUP BY t1_model_confidence""",
            (target_id,),
        ).fetchall()
        return {r["t1_model_confidence"]: r["n"] for r in rows}

    def count_by_tier(self, target_id: str) -> dict:
        """Funnel summary: how many runs passed each tier."""
        result = {}
        for tier in (0, 1, 2, 3):
            col = {0: "t0_pass", 1: "t025_pass", 2: "t1_pass", 3: "t2_pass"}.get(tier)
            if col:
                n = self.conn.execute(
                    f"SELECT COUNT(*) FROM runs WHERE target_id = ? AND {col} = 1",
                    (target_id,),
                ).fetchone()[0]
                result[f"t{tier}_pass"] = n
        return result

    # ------------------------------------------------------------------
    # CRUD — interventions
    # ------------------------------------------------------------------

    def insert_intervention(self, record: dict) -> int:
        """Insert an intervention record. Returns intervention_id."""
        return self._insert("interventions", record)

    def get_interventions_for_run(self, run_id: int) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM interventions WHERE run_id = ? ORDER BY predicted_effect DESC",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # CRUD — findings
    # ------------------------------------------------------------------

    def insert_finding(self, record: dict) -> int:
        """Insert a finding record. Returns finding_id."""
        return self._insert("findings", record)

    def get_findings(self, limit: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM findings ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # CRUD — taxa
    # ------------------------------------------------------------------

    def upsert_taxon(self, record: dict) -> None:
        self._insert("taxa", record, or_replace=True)

    def get_taxon(self, taxon_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM taxa WHERE taxon_id = ?", (taxon_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_run_for_community(self, community_id: int) -> dict | None:
        """Return the most recent run row for a given community_id."""
        row = self.conn.execute(
            "SELECT * FROM runs WHERE community_id = ? ORDER BY run_id DESC LIMIT 1",
            (community_id,),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Tier update convenience wrappers
    # ------------------------------------------------------------------

    def update_community_t025(self, community_id: int, data: dict) -> None:
        """Update T0.25 results on the most recent run for this community."""
        run = self.get_run_for_community(community_id)
        column_map = {
            "t025_pathway_abundances": "t025_model",   # store as JSON in t025_model column
            "t025_n_pathways": "t025_n_pathways",
            "t025_nsti_mean": "t025_nsti_mean",
            "t025_top_similarity": "t025_similarity_score",
            "t025_top_reference_id": "t025_similarity_hit",
            "t025_function_score": "t025_function_score",
            "t025_function_uncertainty": "t025_uncertainty",
            "t025_passed": "t025_pass",
        }
        updates: dict = {}
        for src, dst in column_map.items():
            if src in data:
                updates[dst] = data[src]
        # Also store pathway abundances JSON in t025_model field if present
        if "t025_pathway_abundances" in data:
            updates["t025_model"] = data["t025_pathway_abundances"][:4000] if data["t025_pathway_abundances"] else ""

        if run is not None:
            self.update_run(run["run_id"], updates)
        else:
            # Insert minimal run row
            sample_id = self.conn.execute(
                "SELECT sample_id FROM communities WHERE community_id = ?", (community_id,)
            ).fetchone()
            if sample_id:
                row_id = self.insert_run({
                    "sample_id": sample_id[0], "community_id": community_id,
                    "target_id": None, "t0_pass": True,
                })
                self.update_run(row_id, updates)

    def update_community_t1(self, community_id: int, data: dict) -> None:
        """Update T1 FBA results on the most recent run for this community."""
        run = self.get_run_for_community(community_id)
        column_map = {
            "t1_target_flux": "t1_target_flux",
            "t1_fva_min": "t1_flux_lower_bound",
            "t1_fva_max": "t1_flux_upper_bound",
            "t1_feasible": "t1_feasible",
            "t1_model_confidence": "t1_model_confidence",
            "t1_genome_completeness_mean": "t1_genome_completeness_mean",
            "t1_genome_contamination_mean": "t1_genome_contamination_mean",
            "t1_keystone_taxa": "t1_keystone_taxa",
            "t1_passed": "t1_pass",
        }
        updates: dict = {}
        for src, dst in column_map.items():
            if src in data:
                val = data[src]
                # Convert model_confidence float → tier string for TEXT column
                if dst == "t1_model_confidence" and isinstance(val, float):
                    val = "high" if val >= 0.85 else "medium" if val >= 0.60 else "low"
                updates[dst] = val

        if run is not None:
            self.update_run(run["run_id"], updates)
        else:
            sample_id = self.conn.execute(
                "SELECT sample_id FROM communities WHERE community_id = ?", (community_id,)
            ).fetchone()
            if sample_id:
                row_id = self.insert_run({
                    "sample_id": sample_id[0], "community_id": community_id,
                    "target_id": None, "t0_pass": True,
                })
                self.update_run(row_id, updates)

    def update_community_t2(self, community_id: int, data: dict) -> None:
        """Update T2 dynamics results on the most recent run for this community."""
        run = self.get_run_for_community(community_id)
        column_map = {
            "t2_stability_score": "t2_stability_score",
            "t2_resistance": "t2_resistance",
            "t2_resilience": "t2_resilience",
            "t2_functional_redundancy": "t2_functional_redundancy",
            "t2_interventions": "t2_interventions",
            "t2_top_intervention": "t2_best_intervention",
            "t2_top_confidence": "t2_intervention_effect",
            "t2_establishment_prob": "t2_establishment_prob",
            "t2_passed": "t2_pass",
        }
        updates: dict = {}
        for src, dst in column_map.items():
            if src in data:
                updates[dst] = data[src]

        if run is not None:
            self.update_run(run["run_id"], updates)
        else:
            sample_id = self.conn.execute(
                "SELECT sample_id FROM communities WHERE community_id = ?", (community_id,)
            ).fetchone()
            if sample_id:
                row_id = self.insert_run({
                    "sample_id": sample_id[0], "community_id": community_id,
                    "target_id": None, "t0_pass": True,
                })
                self.update_run(row_id, updates)

    # ------------------------------------------------------------------
    # Reporting helpers
    # ------------------------------------------------------------------

    def top_candidates(self, target_id: str, n: int = 50) -> list[dict]:
        """Return top N T2-passing runs ranked by stability × effect."""
        rows = self.conn.execute(
            """SELECT r.*, s.soil_ph, s.soil_texture, s.climate_zone,
                      s.latitude, s.longitude, s.site_id
               FROM runs r
               JOIN samples s ON r.sample_id = s.sample_id
               WHERE r.target_id = ?
                 AND r.t2_pass = 1
               ORDER BY (r.t2_stability_score * r.t2_intervention_effect) DESC
               LIMIT ?""",
            (target_id, n),
        ).fetchall()
        return [dict(r) for r in rows]

    _VALID_METADATA_COLS = {
        "soil_ph", "soil_texture", "clay_pct", "sand_pct", "silt_pct",
        "organic_matter_pct", "total_nitrogen_ppm", "available_p_ppm",
        "climate_zone", "land_use", "country", "biome", "moisture_pct",
        "temperature_c", "precipitation_mm", "sampling_season",
        "sampling_fraction", "cec", "bulk_density", "sampling_depth_cm",
        "latitude", "longitude",
    }

    def metadata_correlation(self, target_id: str, metadata_col: str) -> list[dict]:
        """Return (metadata_value, mean_t025_score) for correlation analysis."""
        if metadata_col not in self._VALID_METADATA_COLS:
            raise ValueError(
                f"Invalid metadata column: {metadata_col!r}. "
                f"Allowed: {sorted(self._VALID_METADATA_COLS)}"
            )
        rows = self.conn.execute(
            f"""SELECT s.{metadata_col}, AVG(r.t025_function_score) as mean_score,
                       COUNT(*) as n
                FROM runs r
                JOIN samples s ON r.sample_id = s.sample_id
                WHERE r.target_id = ? AND r.t025_pass = 1
                  AND s.{metadata_col} IS NOT NULL
                GROUP BY s.{metadata_col}
                ORDER BY mean_score DESC""",
            (target_id,),
        ).fetchall()
        return [dict(r) for r in rows]
