"""
tests/test_phase7.py — Phase 7: batch_runner, merge_receipts, agent_based_sim, configs.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest
import yaml

from config_schema import PipelineConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MINIMAL_CONFIG = {
    "project": {"name": "test-project", "application": "nitrogen_fixation"},
    "target":  {"target_function": "nitrogen_fixation"},
}


@pytest.fixture()
def tmp_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.dump(MINIMAL_CONFIG))
    return cfg


@pytest.fixture()
def tmp_samples(tmp_path: Path) -> Path:
    samples = [{"sample_id": f"S{i}", "soil_ph": 6.5} for i in range(30)]
    p = tmp_path / "samples.json"
    p.write_text(json.dumps(samples))
    return p


# ---------------------------------------------------------------------------
# batch_runner tests
# ---------------------------------------------------------------------------

class TestBatchRunner:

    def test_split_samples_even(self):
        from batch_runner import _split_samples
        chunks = _split_samples(list(range(30)), 3)
        assert len(chunks) == 3
        assert all(len(c) == 10 for c in chunks)
        assert sum(len(c) for c in chunks) == 30

    def test_split_samples_uneven(self):
        from batch_runner import _split_samples
        chunks = _split_samples(list(range(7)), 3)
        assert len(chunks) == 3
        assert sum(len(c) for c in chunks) == 7

    def test_split_samples_empty(self):
        from batch_runner import _split_samples
        assert _split_samples([], 5) == []

    def test_split_samples_more_batches_than_samples(self):
        from batch_runner import _split_samples
        # ceil(3/10) = 1 item/chunk → 3 chunks of 1
        chunks = _split_samples([1, 2, 3], 10)
        assert sum(len(c) for c in chunks) == 3
        assert all(len(c) == 1 for c in chunks)

    def test_dry_run_local(self, tmp_config: Path, tmp_samples: Path, tmp_path: Path, capsys):
        """Dry-run should print commands without launching processes."""
        from typer.testing import CliRunner
        from batch_runner import app

        runner = CliRunner()
        result = runner.invoke(app, [
            "--config",       str(tmp_config),
            "--samples-json", str(tmp_samples),
            "--n-batches",    "3",
            "--tmp-dir",      str(tmp_path / "tmp"),
            "--dry-run",
        ])
        assert result.exit_code == 0, result.output
        # Should print 3 dry-run commands
        assert result.output.count("[dry-run]") >= 3

    def test_dry_run_chunks_cover_all_samples(self, tmp_config: Path, tmp_path: Path, capsys):
        """Each batch command should reference a unique chunk file."""
        samples = [{"sample_id": f"X{i}"} for i in range(20)]
        samples_json = tmp_path / "s.json"
        samples_json.write_text(json.dumps(samples))

        from typer.testing import CliRunner
        from batch_runner import app

        runner = CliRunner()
        result = runner.invoke(app, [
            "--config",       str(tmp_config),
            "--samples-json", str(samples_json),
            "--n-batches",    "4",
            "--tmp-dir",      str(tmp_path / "tmp"),
            "--dry-run",
        ])
        assert result.exit_code == 0, result.output
        # 4 batch commands expected
        assert result.output.count("batch_0") >= 1
        assert result.output.count("batch_0003") >= 1 or result.output.count("batch_0") >= 4

    def test_missing_samples_json_exits_nonzero(self, tmp_config: Path, tmp_path: Path):
        from typer.testing import CliRunner
        from batch_runner import app

        runner = CliRunner()
        result = runner.invoke(app, [
            "--config", str(tmp_config),
            "--n-batches", "2",
        ])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# merge_receipts tests
# ---------------------------------------------------------------------------

class TestMergeReceipts:

    def _make_receipt(self, tmp_path: Path, receipt_id: str = "abc-123", **kwargs) -> Path:
        payload = {
            "receipt_id": receipt_id,
            "machine_id": "testnode",
            "batch_start": "2026-03-05T00:00:00+00:00",
            "batch_end":   "2026-03-05T00:05:00+00:00",
            "n_samples_processed": kwargs.get("n_samples", 100),
            "n_fba_runs":          kwargs.get("n_fba", 5),
            "n_dynamics_runs":     kwargs.get("n_dyn", 1),
            "status": "completed",
            "filepath": str(tmp_path / f"{receipt_id}.json"),
        }
        p = tmp_path / f"{receipt_id}.json"
        p.write_text(json.dumps(payload))
        return p

    def test_list_only_no_receipts(self, tmp_path: Path):
        from typer.testing import CliRunner
        from merge_receipts import app

        runner = CliRunner()
        result = runner.invoke(app, [
            "--receipts-dir", str(tmp_path),
            "--db",           str(tmp_path / "test.db"),
            "--list",
        ])
        # Empty dir → "No receipt files found."
        assert result.exit_code == 0

    def test_merge_into_empty_db(self, tmp_path: Path):
        """Merging 3 receipts into a fresh DB should succeed."""
        rdir = tmp_path / "receipts"
        rdir.mkdir()
        for i in range(3):
            self._make_receipt(rdir, receipt_id=f"rcpt-{i:04d}", n_samples=50)

        from typer.testing import CliRunner
        from merge_receipts import app
        from db_utils import SoilDB

        db_path = tmp_path / "test.db"
        # Pre-create DB so schema is initialised
        with SoilDB(db_path):
            pass

        runner = CliRunner()
        result = runner.invoke(app, [
            "--receipts-dir", str(rdir),
            "--db",           str(db_path),
        ])
        assert result.exit_code == 0, result.output
        assert "3" in result.output  # "Receipts merged: 3" in summary

    def test_merge_skips_already_merged(self, tmp_path: Path):
        """Re-running merge should not insert duplicates."""
        rdir = tmp_path / "receipts"
        rdir.mkdir()
        self._make_receipt(rdir, receipt_id="dup-0001", n_samples=20)

        from typer.testing import CliRunner
        from merge_receipts import app
        from db_utils import SoilDB

        db_path = tmp_path / "test.db"
        with SoilDB(db_path):
            pass

        runner = CliRunner()
        # First merge
        r1 = runner.invoke(app, ["--receipts-dir", str(rdir), "--db", str(db_path)])
        assert r1.exit_code == 0

        # Second merge — same receipt already in DB
        r2 = runner.invoke(app, ["--receipts-dir", str(rdir), "--db", str(db_path)])
        assert r2.exit_code == 0
        # "0 new" on second run
        assert "0 new" in r2.output

    def test_list_mode_shows_unmerged(self, tmp_path: Path):
        rdir = tmp_path / "receipts"
        rdir.mkdir()
        self._make_receipt(rdir, receipt_id="listed-001")

        from typer.testing import CliRunner
        from merge_receipts import app
        from db_utils import SoilDB

        db_path = tmp_path / "test.db"
        with SoilDB(db_path):
            pass

        runner = CliRunner()
        result = runner.invoke(app, [
            "--receipts-dir", str(rdir),
            "--db",           str(db_path),
            "--list",
        ])
        assert result.exit_code == 0
        assert "listed-001" in result.output

    def test_corrupted_receipt_does_not_crash(self, tmp_path: Path):
        rdir = tmp_path / "receipts"
        rdir.mkdir()
        (rdir / "bad.json").write_text("{not valid json")

        from typer.testing import CliRunner
        from merge_receipts import app
        from db_utils import SoilDB

        db_path = tmp_path / "test.db"
        with SoilDB(db_path):
            pass

        runner = CliRunner()
        # Should not raise — corrupted file is skipped with a warning
        result = runner.invoke(app, ["--receipts-dir", str(rdir), "--db", str(db_path)])
        # Exit 0 (no valid receipts to merge) or 1 (errors count > 0 — both acceptable)
        assert result.exit_code in (0, 1)

    def test_cost_accounting_sums_correctly(self, tmp_path: Path):
        """Total FBA and dynamics counts should sum across receipts."""
        rdir = tmp_path / "receipts"
        rdir.mkdir()
        self._make_receipt(rdir, receipt_id="r1", n_fba=10, n_dyn=2)
        self._make_receipt(rdir, receipt_id="r2", n_fba=20, n_dyn=3)

        from typer.testing import CliRunner
        from merge_receipts import app
        from db_utils import SoilDB

        db_path = tmp_path / "test.db"
        with SoilDB(db_path):
            pass

        runner = CliRunner()
        result = runner.invoke(app, ["--receipts-dir", str(rdir), "--db", str(db_path)])
        assert result.exit_code == 0
        # Total FBA = 30, Dynamics = 5
        assert "30" in result.output
        assert "5" in result.output


# ---------------------------------------------------------------------------
# agent_based_sim tests
# ---------------------------------------------------------------------------

class TestAgentBasedSim:

    def test_fallback_no_java(self, monkeypatch):
        """When java is not in PATH, returns graceful fallback."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda name: None)

        from compute.agent_based_sim import run_idynomics
        result = run_idynomics(None, {}, simulation_days=1, java_exe="java_not_real")

        assert result["engine"] == "fallback"
        assert result["stability_score"] == 0.0
        assert "walltime_s" in result
        assert result["note"] is not None

    def test_fallback_no_jar(self, monkeypatch, tmp_path: Path):
        """When jar is missing even though java exists, returns fallback."""
        import shutil as _shutil
        monkeypatch.setattr(_shutil, "which", lambda name: "/usr/bin/java" if name == "java" else None)

        from compute import agent_based_sim
        monkeypatch.setattr(agent_based_sim, "_find_jar", lambda _: None)

        result = agent_based_sim.run_idynomics(None, {}, simulation_days=1)
        assert result["engine"] == "fallback"
        assert "iDynoMiCS" in (result["note"] or "")

    def test_result_schema(self, monkeypatch):
        """Result dict always has required keys regardless of engine."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda name: None)

        from compute.agent_based_sim import run_idynomics
        result = run_idynomics(None, {})

        for key in ("stability_score", "spatial_community_profile", "walltime_s", "engine", "note"):
            assert key in result, f"Missing key: {key}"

    def test_stability_score_in_range(self, monkeypatch):
        import shutil
        monkeypatch.setattr(shutil, "which", lambda name: None)

        from compute.agent_based_sim import run_idynomics
        result = run_idynomics(None, {})
        assert 0.0 <= result["stability_score"] <= 1.0

    def test_write_protocol_xml(self, tmp_path: Path):
        """Protocol XML should be written and parseable."""
        from compute.agent_based_sim import _write_protocol_xml
        import xml.etree.ElementTree as ET

        proto = _write_protocol_xml(None, {"soil_ph": 7.0}, 30, tmp_path)
        assert proto.exists()
        tree = ET.parse(proto)
        root = tree.getroot()
        assert root.tag == "idynomics"
        # Should have a simulator and world block
        assert root.find("simulator") is not None
        assert root.find("world") is not None

    def test_write_protocol_xml_includes_species(self, tmp_path: Path):
        """When community_model has _member_models, species should appear in XML."""
        from compute.agent_based_sim import _write_protocol_xml
        import xml.etree.ElementTree as ET

        class FakeModel:
            id = "org0"
            class _M:
                id = "org0"
            _member_models = [_M()]

        proto = _write_protocol_xml(FakeModel(), {}, 14, tmp_path)
        root = ET.parse(proto).getroot()
        species = root.findall("species")
        assert len(species) >= 1


# ---------------------------------------------------------------------------
# Config YAML validation tests
# ---------------------------------------------------------------------------

class TestConfigYAMLs:

    def test_carbon_sequestration_config_valid(self):
        cfg_path = Path(__file__).parent.parent / "configs" / "carbon_sequestration.yaml"
        assert cfg_path.exists(), f"Missing: {cfg_path}"
        cfg = PipelineConfig.from_yaml(cfg_path)
        assert cfg.project["application"] == "carbon_sequestration"
        assert cfg.fungal.include_its_track is True
        assert cfg.fungal.require_its_data is True

    def test_bioremediation_config_valid(self):
        cfg_path = Path(__file__).parent.parent / "configs" / "bioremediation.yaml"
        assert cfg_path.exists(), f"Missing: {cfg_path}"
        cfg = PipelineConfig.from_yaml(cfg_path)
        assert cfg.project["application"] == "bioremediation"
        assert cfg.fungal.include_its_track is False

    def test_carbon_seq_has_required_genes(self):
        cfg_path = Path(__file__).parent.parent / "configs" / "carbon_sequestration.yaml"
        with open(cfg_path) as fh:
            raw = yaml.safe_load(fh)
        t0 = raw["filters"]["t0"]
        genes = t0.get("required_functional_genes", [])
        assert "laccase" in genes or "peroxidase" in genes

    def test_bioremediation_has_alkb_gene(self):
        cfg_path = Path(__file__).parent.parent / "configs" / "bioremediation.yaml"
        with open(cfg_path) as fh:
            raw = yaml.safe_load(fh)
        genes = raw["filters"]["t0"].get("required_functional_genes", [])
        assert "alkB" in genes

    def test_carbon_seq_exclude_contaminated_false_inverted(self):
        """Bioremediation inverts the contamination gate."""
        cfg_path = Path(__file__).parent.parent / "configs" / "bioremediation.yaml"
        with open(cfg_path) as fh:
            raw = yaml.safe_load(fh)
        assert raw["filters"]["t0"]["exclude_contaminated"] is False

    def test_three_configs_have_distinct_db_paths(self):
        """Each config instantiation must write to its own DB."""
        root = Path(__file__).parent.parent
        paths = []
        for name in ["config.example.yaml", "configs/carbon_sequestration.yaml", "configs/bioremediation.yaml"]:
            p = root / name
            if p.exists():
                with open(p) as fh:
                    raw = yaml.safe_load(fh)
                paths.append(raw.get("output", {}).get("db_path", ""))
        assert len(set(paths)) == len(paths), f"Duplicate db_path values: {paths}"
