"""
tests/test_rebuild.py — Tests for the modular pipeline rebuild.
"""

import json
import pytest
from pathlib import Path

from db_utils import SoilDB
from core.samples import SampleManager
from core.engine import PipelineEngine
from apps.bnf.intent import BNFIntent
from compute.community_fba import run_community_fba

@pytest.fixture
def db():
    with SoilDB(":memory:") as d:
        yield d

@pytest.fixture
def bnf_intent():
    return BNFIntent()

def test_dynamic_annotations(db):
    """Test v3 dynamic annotation schema."""
    db.connect()
    # Insert sample first (FK requirement)
    db.conn.execute("INSERT INTO samples (sample_id) VALUES ('sample1')")
    
    # Insert community
    db.conn.execute(
        "INSERT INTO communities (sample_id, phylum_profile) VALUES (?, ?)",
        ("sample1", json.dumps({"Proteobacteria": 0.5}))
    )
    db.conn.commit()
    
    # Add custom annotation
    db.add_annotation(
        community_id=1,
        trait="nifH",
        value=0.035,
        present=True,
        method="keyword",
        meta={"hgt_flagged": False}
    )
    
    # Retrieve annotation
    row = db.conn.execute("SELECT * FROM annotations WHERE community_id = 1").fetchone()
    assert row["trait_name"] == "nifH"
    assert row["value"] == 0.035
    assert row["is_present"] == 1
    meta = json.loads(row["meta_json"])
    assert meta["hgt_flagged"] is False

def test_sample_manager_normalization(db):
    """Test unified sample manager and metadata normalization."""
    db.connect()
    manager = SampleManager(db)
    raw = {
        "sample_id": "test1",
        "ph": "6.8",
        "latitude": 40.0,
        "longitude": -105.0
    }
    sid = manager.ingest_sample(raw, source="neon")
    
    meta = db.get_sample_metadata(sid)
    assert meta["sample_id"] == "test1"
    assert meta["soil_ph"] == 6.8
    assert meta["latitude"] == 40.0

def test_bnf_intent_parameters(bnf_intent):
    """Test that BNFIntent provides correct biological parameters."""
    filters = bnf_intent.get_t0_filters()
    assert "nifH" in filters["required_functional_genes"]
    
    constraints = bnf_intent.get_t1_constraints({})
    assert constraints["medium_type"] == "N-limited-minimal"
    assert "EX_n2_e" in constraints["inorganic_whitelist"]

def test_engine_t1_dispatch(db, bnf_intent):
    """Test engine orchestration of T1 FBA."""
    db.connect()
    # Mock sample, community and run
    db.conn.execute("INSERT INTO samples (sample_id, soil_ph) VALUES ('S1', 7.0)")
    db.conn.execute("INSERT INTO communities (sample_id) VALUES ('S1')")
    db.conn.execute("INSERT INTO runs (community_id, t0_pass) VALUES (1, 1)")
    db.conn.commit()
    
    engine = PipelineEngine(bnf_intent, db, {})
    
    mock_result = {
        "community_id": 1,
        "t1_pass": True,
        "target_flux": 1.23,
        "status": "optimal"
    }
    engine._persist_t1_result(mock_result)
    
    run_row = db.conn.execute("SELECT * FROM runs WHERE community_id = 1").fetchone()
    assert run_row["t1_pass"] == 1
    assert run_row["t1_flux"] == 1.23
