"""
config_schema.py — Pydantic v2 schema for pipeline config YAML.

Usage:
  from config_schema import PipelineConfig
  cfg = PipelineConfig.from_yaml("config.yaml")

  # or validate from CLI:
  python config_schema.py --validate path/to/config.yaml
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import typer
import yaml
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class TargetFluxSpec(BaseModel):
    min: float
    optimal: str | float | None = None
    units: str


class SoilContext(BaseModel):
    ph_range: list[float] = Field(default_factory=lambda: [4.0, 9.0])
    texture: list[str] = Field(default_factory=list)
    climate_zone: list[str] = Field(default_factory=list)
    land_use: list[str] = Field(default_factory=list)
    crop: str | None = None


class T0Filters(BaseModel):
    min_sequencing_depth: int = 50_000
    min_observed_otus: int = 500
    ph_range: list[float] = Field(default_factory=lambda: [4.0, 9.0])
    required_functional_genes: list[str] = Field(default_factory=list)
    exclude_contaminated: bool = True
    min_soil_organic_matter: float | None = None
    exclude_flooded: bool = False


class T025Filters(BaseModel):
    ml_models: list[str] = Field(default_factory=list)
    min_function_score: float = 0.5
    min_target_gene_abundance: float = 0.001
    reference_db: str | None = None
    min_similarity: float = 0.3


class T1Filters(BaseModel):
    fba_engine: str = "cobrapy"
    community_size_limit: int = 20
    genome_db: str = "patric"
    min_target_flux: float = 0.5
    max_fba_walltime_min: int = 30


class T2Filters(BaseModel):
    dynamics_engine: str = "dfba"
    simulation_time_days: int = 90
    perturbations: list[dict[str, Any]] = Field(default_factory=list)
    intervention_screen: dict[str, Any] = Field(default_factory=dict)
    min_stability_score: float = 0.6
    min_establishment_prob: float = 0.4


class ComputeConfig(BaseModel):
    workers: int = 4
    fba_workers: int = 2
    batch_size: int = 1000
    t1_batch_size: int = 50
    t2_batch_size: int = 5
    checkpoint_interval: int = 100


class OutputConfig(BaseModel):
    db_path: str = "landscape.db"
    receipts_dir: str = "receipts/"
    results_dir: str = "results/"
    top_n: int = 50
    export_community_profiles: bool = True
    export_intervention_report: bool = True


# ---------------------------------------------------------------------------
# Root config model
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    project: dict[str, Any]
    target: dict[str, Any]
    sequence_source: dict[str, Any] = Field(default_factory=dict)
    filters: dict[str, Any] = Field(default_factory=dict)
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "PipelineConfig":
        with open(path) as fh:
            raw = yaml.safe_load(fh)
        return cls(**raw)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

app = typer.Typer()


@app.command()
def validate(config: Path = typer.Option(..., help="Path to config YAML")):
    """Validate a pipeline config YAML file."""
    cfg = PipelineConfig.from_yaml(config)
    typer.echo(f"Config valid: project={cfg.project.get('name')}")


if __name__ == "__main__":
    app()
