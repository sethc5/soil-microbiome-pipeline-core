"""
pipeline_core.py — Authoritative entry point for the soil microbiome pipeline.

Usage:
    python pipeline_core.py --config configs/bnf.yaml --tier 1 --workers 36
"""

from __future__ import annotations
import logging
import json
from pathlib import Path
from typing import Any, Dict, Optional

import typer
import yaml

from core.db_utils import SoilDB
from core.samples import SampleManager
from core.engine import PipelineEngine
from apps.bnf.intent import BNFIntent

app = typer.Typer(help="Unified Soil Microbiome Pipeline Core")
logger = logging.getLogger("pipeline")

def resolve_intent(app_name: str) -> Any:
    """Resolve the biological intent module."""
    if app_name == "nitrogen_fixation":
        return BNFIntent()
    # Add other intents here (Carbon, Biorem, etc.)
    raise ValueError(f"Unknown application: {app_name}")

@app.command()
def run(
    config_path: Path = typer.Option(..., "--config", help="Path to config YAML"),
    tier: int = typer.Option(2, help="Target funnel tier (0, 1, 2)"),
    workers: int = typer.Option(4, "-w", help="Number of parallel workers"),
    db_path: Path = typer.Option(Path("soil_microbiome.db"), help="SQLite DB path"),
):
    """Run the pipeline funnel up to the specified tier."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    # 1. Load Config and Intent
    with open(config_path) as f:
        cfg_dict = yaml.safe_load(f)
    
    app_name = cfg_dict.get("project", {}).get("application")
    intent = resolve_intent(app_name)
    
    logger.info("Initializing Pipeline | Intent: %s | Target Tier: T%d", intent.target_id, tier)

    # 2. Initialize Core Components
    db = SoilDB(db_path).connect()
    engine = PipelineEngine(intent, db, cfg_dict)
    
    # 3. Funnel Execution
    if tier >= 0:
        logger.info("Executing Tier 0: Quality & Metadata")
        # SampleManager logic would be called here during ingest
    
    if tier >= 1:
        logger.info("Executing Tier 1: Metabolic Network Modeling")
        # Query T0 passers from DB
        with db.conn:
            rows = db.conn.execute(
                "SELECT community_id FROM runs WHERE t0_pass = 1 AND t1_pass IS NULL"
            ).fetchall()
        
        community_ids = [r[0] for r in rows]
        if community_ids:
            logger.info("Found %d communities for T1 modeling", len(community_ids))
            results = engine.run_t1_batch(community_ids, workers=workers)
            logger.info("T1 Batch Complete: %s", results)
        else:
            logger.info("No pending communities for T1.")

    if tier >= 2:
        logger.info("Executing Tier 2: Community Dynamics")
        # ... logic for T2 ...

    logger.info("Pipeline execution complete.")

if __name__ == "__main__":
    app()
