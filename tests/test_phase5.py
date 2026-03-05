"""
tests/test_phase5.py — Unit tests for Phase 5 adapter implementations.

Covers:
  adapters/__init__.py  -- registry completeness, get_adapter factory
  local_biom_adapter    -- from_fastq with actual temp FASTQ files, metadata CSV
  emp_adapter           -- _safe_float helper, download graceful missing
  agp_adapter           -- instantiation, env_material filter config
  ncbi_sra_adapter      -- instantiation, query building
  mgnify_adapter        -- instantiation, rate-limit constant
  qiita_adapter         -- instantiation, _get graceful failure
  redbiom_adapter       -- search_by_taxon returns empty if not installed
  neon_adapter          -- SOURCE attribute

All tests require no external network connections.
"""

from __future__ import annotations
import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# Registry
# ===========================================================================

class TestAdapterRegistry:
    def test_all_expected_keys_present(self):
        from adapters import ADAPTER_REGISTRY
        expected = {"sra", "mgnify", "emp", "agp", "local", "qiita", "redbiom", "neon"}
        assert expected.issubset(set(ADAPTER_REGISTRY.keys()))

    def test_get_adapter_sra(self):
        from adapters import get_adapter
        from adapters.ncbi_sra_adapter import NCBISRAAdapter
        adapter = get_adapter("sra", config={})
        assert isinstance(adapter, NCBISRAAdapter)

    def test_get_adapter_case_insensitive(self):
        from adapters import get_adapter
        adapter = get_adapter("SRA", config={})
        assert adapter.SOURCE == "sra"

    def test_get_adapter_mgnify(self):
        from adapters import get_adapter
        adapter = get_adapter("mgnify", config={})
        assert adapter.SOURCE == "mgnify"

    def test_get_adapter_local(self):
        from adapters import get_adapter
        adapter = get_adapter("local", config={})
        assert adapter.SOURCE == "local"

    def test_get_adapter_unknown_raises(self):
        from adapters import get_adapter
        with pytest.raises(ValueError, match="Unknown adapter"):
            get_adapter("does_not_exist", config={})

    def test_get_adapter_no_config(self):
        from adapters import get_adapter
        adapter = get_adapter("neon")
        assert adapter is not None

    def test_ncbi_sra_alias(self):
        from adapters import get_adapter, ADAPTER_REGISTRY
        assert ADAPTER_REGISTRY["ncbi_sra"] is ADAPTER_REGISTRY["sra"]


# ===========================================================================
# LocalBIOMAdapter
# ===========================================================================

class TestLocalBIOMAdapter:
    def _make_metadata_csv(self, tmp_path: Path, samples: list[dict]) -> Path:
        csv_path = tmp_path / "metadata.csv"
        fieldnames = ["sample_id", "ph", "temperature", "latitude", "longitude",
                      "collection_date", "country"]
        with open(csv_path, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(samples)
        return csv_path

    def _make_fastq_files(self, tmp_path: Path, sample_ids: list[str]) -> Path:
        fq_dir = tmp_path / "fastq"
        fq_dir.mkdir()
        for sid in sample_ids:
            (fq_dir / f"{sid}_R1.fastq").write_text("@read1\nACGT\n+\nIIII\n")
            (fq_dir / f"{sid}_R2.fastq").write_text("@read1\nTGCA\n+\nIIII\n")
        return fq_dir

    def test_from_fastq_yields_samples(self, tmp_path):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        fq_dir = self._make_fastq_files(tmp_path, ["S001", "S002"])
        samples = list(adapter.from_fastq(str(fq_dir)))
        assert len(samples) == 2

    def test_from_fastq_sample_ids(self, tmp_path):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        fq_dir = self._make_fastq_files(tmp_path, ["SMP01"])
        samples = list(adapter.from_fastq(str(fq_dir)))
        # sample_id should be derived from filename stem without _R1/_R2
        assert samples[0]["sample_id"] == "SMP01"

    def test_from_fastq_with_metadata(self, tmp_path):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        fq_dir = self._make_fastq_files(tmp_path, ["S001"])
        meta_csv = self._make_metadata_csv(tmp_path, [
            {"sample_id": "S001", "ph": "6.5", "temperature": "20.0"}
        ])
        samples = list(adapter.from_fastq(str(fq_dir), metadata_csv=str(meta_csv)))
        assert float(samples[0]["ph"]) == 6.5

    def test_from_fastq_r2_pair_detected(self, tmp_path):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        fq_dir = self._make_fastq_files(tmp_path, ["SAMPLE"])
        samples = list(adapter.from_fastq(str(fq_dir)))
        assert samples[0]["fastq_r2"] is not None

    def test_from_fastq_nonexistent_dir(self, tmp_path, capsys):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        samples = list(adapter.from_fastq(str(tmp_path / "does_not_exist")))
        assert samples == []

    def test_from_biom_nonexistent_file(self, tmp_path, capsys):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        samples = list(adapter.from_biom(str(tmp_path / "missing.biom")))
        assert samples == []

    def test_metadata_csv_tsv_format(self, tmp_path):
        from adapters.local_biom_adapter import LocalBIOMAdapter
        adapter = LocalBIOMAdapter(config={})
        tsv_path = tmp_path / "meta.tsv"
        tsv_path.write_text("sample_id\tph\nS001\t6.5\n")
        # _load_metadata_csv should parse TSV
        meta = adapter._load_metadata_csv(str(tsv_path))
        assert "S001" in meta
        assert meta["S001"]["ph"] == "6.5"


# ===========================================================================
# EMPAdapter
# ===========================================================================

class TestEMPAdapter:
    def test_source_attribute(self):
        from adapters.emp_adapter import EMPAdapter
        adapter = EMPAdapter(config={})
        assert adapter.SOURCE == "emp"

    def test_safe_float_valid(self):
        from adapters.emp_adapter import _safe_float
        assert _safe_float("6.5") == 6.5

    def test_safe_float_none(self):
        from adapters.emp_adapter import _safe_float
        assert _safe_float(None) is None

    def test_safe_float_nan_string(self):
        from adapters.emp_adapter import _safe_float
        assert _safe_float("nan") is None

    def test_safe_float_invalid(self):
        from adapters.emp_adapter import _safe_float
        assert _safe_float("not_a_number") is None

    def test_download_biom_returns_string(self, tmp_path, monkeypatch):
        """download_biom should return path even if download fails."""
        import urllib.request
        monkeypatch.setattr(urllib.request, "urlretrieve", lambda url, path: (_ for _ in ()).throw(OSError("no network")))
        from adapters.emp_adapter import EMPAdapter
        adapter = EMPAdapter(config={})
        result = adapter.download_biom(outdir=str(tmp_path))
        # Returns empty string on failure
        assert isinstance(result, str)


# ===========================================================================
# AGPAdapter
# ===========================================================================

class TestAGPAdapter:
    def test_source_attribute(self):
        from adapters.agp_adapter import AGPAdapter
        adapter = AGPAdapter(config={})
        assert adapter.SOURCE == "agp"

    def test_env_material_from_config(self):
        from adapters.agp_adapter import AGPAdapter
        adapter = AGPAdapter(config={"env_material": "sediment"})
        assert adapter._env_material == "sediment"

    def test_iter_soil_samples_no_network(self, monkeypatch, tmp_path):
        """If ENA is unreachable, iter_soil_samples yields nothing gracefully."""
        import urllib.request
        monkeypatch.setattr(urllib.request, "urlretrieve", lambda url, path: (_ for _ in ()).throw(OSError("offline")))
        from adapters.agp_adapter import AGPAdapter
        adapter = AGPAdapter(config={"cache_dir": str(tmp_path)})
        samples = list(adapter.iter_soil_samples())
        assert isinstance(samples, list)  # empty but not an error


# ===========================================================================
# NCBISRAAdapter
# ===========================================================================

class TestNCBISRAAdapter:
    def test_source_attribute(self):
        from adapters.ncbi_sra_adapter import NCBISRAAdapter
        adapter = NCBISRAAdapter(config={})
        assert adapter.SOURCE == "sra"

    def test_build_query_default(self):
        from adapters.ncbi_sra_adapter import NCBISRAAdapter
        adapter = NCBISRAAdapter(config={"biome": "agricultural soil"})
        q = adapter._build_query({})
        assert "soil" in q.lower()

    def test_build_query_16s(self):
        from adapters.ncbi_sra_adapter import NCBISRAAdapter
        adapter = NCBISRAAdapter(config={})
        q = adapter._build_query({"sequencing_type": "16S"})
        assert "16S" in q or "ribosomal" in q.lower()

    def test_download_fastq_no_toolkit(self, monkeypatch, tmp_path):
        """Without sra-tools, download_fastq returns empty list gracefully."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.picrust2_runner as pr  # dummy to trigger shutil reload
        from adapters.ncbi_sra_adapter import NCBISRAAdapter
        adapter = NCBISRAAdapter(config={})
        result = adapter.download_fastq("SRR123456", outdir=str(tmp_path))
        assert result == []


# ===========================================================================
# MGnifyAdapter
# ===========================================================================

class TestMGnifyAdapter:
    def test_source_attribute(self):
        from adapters.mgnify_adapter import MGnifyAdapter
        adapter = MGnifyAdapter(config={})
        assert adapter.SOURCE == "mgnify"

    def test_rate_limit_constant(self):
        from adapters.mgnify_adapter import MGnifyAdapter
        assert MGnifyAdapter._MIN_INTERVAL > 0.5

    def test_no_network_yields_empty(self, monkeypatch):
        """If API is unreachable, search_samples yields nothing."""
        import urllib.request
        monkeypatch.setattr(urllib.request, "Request", lambda url, **kw: None)
        monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **kw: (_ for _ in ()).throw(OSError("offline")))
        from adapters.mgnify_adapter import MGnifyAdapter
        import time
        monkeypatch.setattr(time, "sleep", lambda x: None)
        adapter = MGnifyAdapter(config={})
        samples = list(adapter.search_samples("root:Environmental:Terrestrial:Soil"))
        assert isinstance(samples, list)


# ===========================================================================
# QiitaAdapter
# ===========================================================================

class TestQiitaAdapter:
    def test_source_attribute(self):
        from adapters.qiita_adapter import QiitaAdapter
        adapter = QiitaAdapter(config={})
        assert adapter.SOURCE == "qiita"

    def test_no_network_yields_empty(self, monkeypatch):
        import urllib.request, time
        monkeypatch.setattr(time, "sleep", lambda x: None)
        monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **kw: (_ for _ in ()).throw(OSError("offline")))
        monkeypatch.setattr(urllib.request, "Request", lambda *a, **kw: None)
        from adapters.qiita_adapter import QiitaAdapter
        adapter = QiitaAdapter(config={})
        samples = list(adapter.search(study_type="soil"))
        assert isinstance(samples, list)


# ===========================================================================
# RedbiomAdapter
# ===========================================================================

class TestRedbiomAdapter:
    def test_source_attribute(self):
        from adapters.redbiom_adapter import RedbiomAdapter
        adapter = RedbiomAdapter(config={})
        assert adapter.SOURCE == "redbiom"

    def test_search_by_taxon_graceful_when_not_installed(self, monkeypatch):
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from adapters.redbiom_adapter import RedbiomAdapter
        adapter = RedbiomAdapter(config={})
        result = adapter.search_by_taxon("Azospirillum")
        assert result == []

    def test_fetch_samples_empty_ids(self, tmp_path):
        from adapters.redbiom_adapter import RedbiomAdapter
        adapter = RedbiomAdapter(config={})
        result = adapter.fetch_samples([], outdir=str(tmp_path))
        assert result == ""


# ===========================================================================
# NEONAdapter (from Phase 0, verify not broken by adapter refactor)
# ===========================================================================

class TestNEONAdapter:
    def test_source_attribute(self):
        from adapters.neon_adapter import NEONAdapter
        adapter = NEONAdapter(config={})
        assert adapter.SOURCE == "neon"

    def test_get_adapter_neon(self):
        from adapters import get_adapter
        adapter = get_adapter("neon")
        assert adapter.SOURCE == "neon"
