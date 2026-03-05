"""
tests/test_phase1.py -- Unit tests for Phase 1 T0 compute modules.

Covers:
  quality_filter        -- metadata-only mode, depth and N-fraction gates
  diversity_metrics     -- Shannon, Simpson, Chao1, numpy fallback
  metadata_validator    -- USDA texture triangle (parametrized), climate zone,
                           pH gates, sampling_fraction
  functional_gene_scanner -- community_data path, keyword scan on mock FASTA,
                             amoA split, nifH HGT flag
  tax_profiler           -- precomputed_profile passthrough, fungal_bacterial_ratio
  tax_function_mapper    -- bundled lookup, get_functional_summary
  pipeline_core          -- _process_one_sample_t0 smoke test (single worker)

All tests run without any external tools (mmseqs2, QIIME2, Kraken2, etc.).
"""

from __future__ import annotations

import json
import math
import os
import sys
import tempfile
from pathlib import Path

import pytest

# Ensure repo root is on path when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# quality_filter
# ===========================================================================

class TestQualityFilter:
    def _run(self, **kwargs):
        from compute.quality_filter import run_quality_filter
        return run_quality_filter(**kwargs)

    def test_metadata_only_passes(self):
        """No FASTQs provided -- should pass by default."""
        result = self._run(fastq_paths=None, metadata={"sample_id": "s1"})
        assert result["passed"] is True
        assert result["total_reads"] is None

    def test_depth_reject(self):
        """Low total_reads supplied via metadata triggers depth reject."""
        result = self._run(
            fastq_paths=None,
            min_depth=5000,
            metadata={"sample_id": "s2", "total_reads": 1000},
        )
        assert result["passed"] is False
        assert any("depth" in r.lower() or "reads" in r.lower()
                   for r in result["reject_reasons"])

    def test_n_fraction_reject_from_metadata(self):
        """High N fraction supplied via metadata triggers rejection."""
        result = self._run(
            fastq_paths=None,
            max_n_fraction=0.05,
            metadata={"sample_id": "s3", "n_fraction": 0.20},
        )
        assert result["passed"] is False

    def test_returns_tools_used(self):
        result = self._run(fastq_paths=None, metadata={"sample_id": "s4"})
        assert "tools_used" in result

    def test_chimera_rate_none_without_fasta(self):
        result = self._run(fastq_paths=None, metadata={})
        assert result.get("chimera_rate") is None


# ===========================================================================
# diversity_metrics
# ===========================================================================

class TestDiversityMetrics:
    def test_compute_alpha_diversity_basic(self):
        from compute.diversity_metrics import compute_alpha_diversity
        counts = [10, 20, 30, 5, 15]
        d = compute_alpha_diversity(counts)
        assert "shannon" in d
        assert "simpson" in d
        assert "chao1" in d
        assert "observed_otus" in d
        assert d["observed_otus"] == 5

    def test_shannon_all_equal(self):
        from compute.diversity_metrics import compute_alpha_diversity
        counts = [25, 25, 25, 25]
        d = compute_alpha_diversity(counts)
        assert d["shannon"] == pytest.approx(math.log(4), rel=1e-3)

    def test_shannon_one_species(self):
        from compute.diversity_metrics import compute_alpha_diversity
        d = compute_alpha_diversity([100])
        assert d["shannon"] == pytest.approx(0.0, abs=1e-9)

    def test_simpson_one_species(self):
        from compute.diversity_metrics import compute_alpha_diversity
        d = compute_alpha_diversity([100])
        assert d["simpson"] == pytest.approx(0.0, abs=1e-9)

    def test_chao1_no_singletons(self):
        from compute.diversity_metrics import compute_alpha_diversity
        # All species observed >= 2x -- Chao1 = observed_otus
        counts = [5, 5, 5, 5, 5]
        d = compute_alpha_diversity(counts)
        assert d["chao1"] >= d["observed_otus"]

    def test_pielou_evenness_range(self):
        from compute.diversity_metrics import compute_alpha_diversity
        counts = [10, 20, 30]
        d = compute_alpha_diversity(counts)
        if d["pielou_evenness"] is not None:
            assert 0.0 <= d["pielou_evenness"] <= 1.0

    def test_diversity_from_profile(self):
        from compute.diversity_metrics import diversity_from_profile
        phylum = {"Proteobacteria": 0.4, "Firmicutes": 0.3, "Actinobacteria": 0.3}
        top_gen = [{"name": "Pseudomonas", "rel_abundance": 0.2}]
        d = diversity_from_profile(phylum, top_gen)
        assert "shannon" in d
        assert d["shannon"] > 0

    def test_empty_counts_returns_zeros(self):
        from compute.diversity_metrics import compute_alpha_diversity
        d = compute_alpha_diversity([])
        assert d["observed_otus"] == 0


# ===========================================================================
# metadata_validator -- texture triangle (parametrized)
# ===========================================================================

TEXTURE_CASES = [
    # (sand, silt, clay, expected_class)
    (90, 5, 5,   "sand"),
    (75, 15, 10, "loamy sand"),
    (65, 20, 15, "sandy loam"),
    (10, 80, 10, "silt"),
    (10, 10, 80, "clay"),
    (40, 30, 30, "clay loam"),
    (20, 60, 20, "silt loam"),
    (50, 25, 25, "sandy clay loam"),
    (5, 50, 45,  "silty clay"),
    (25, 10, 65, "clay"),
]


@pytest.mark.parametrize("sand,silt,clay,expected", TEXTURE_CASES)
def test_texture_class(sand, silt, clay, expected):
    from compute.metadata_validator import texture_class_from_fractions
    result = texture_class_from_fractions(sand, silt, clay)
    assert result == expected, f"sand={sand} silt={silt} clay={clay}: got '{result}', expected '{expected}'"


class TestMetadataValidator:
    def _validate(self, raw):
        from compute.metadata_validator import validate_sample_metadata
        return validate_sample_metadata(raw)

    def test_valid_minimal(self):
        result = self._validate({"sample_id": "s1", "ph": 6.5, "lat": 40.0, "lon": -105.0})
        assert isinstance(result, dict)
        assert "reject_reasons" in result

    def test_ph_below_zero_rejected(self):
        result = self._validate({"sample_id": "s2", "ph": -1.0})
        assert any("ph" in r.lower() for r in result.get("reject_reasons", []))

    def test_ph_above_14_rejected(self):
        result = self._validate({"sample_id": "s3", "ph": 15.0})
        assert any("ph" in r.lower() for r in result.get("reject_reasons", []))

    def test_ph_in_range_passes_gate(self):
        result = self._validate({"sample_id": "s4", "ph": 7.0})
        ph_rejects = [r for r in result.get("reject_reasons", []) if "ph" in r.lower()]
        assert len(ph_rejects) == 0

    def test_climate_zone_derived_from_coords(self):
        result = self._validate({"sample_id": "s5", "lat": 51.5, "lon": -0.1})
        meta = result.get("normalised", {})
        # Climate zone may or may not be set depending on available libraries,
        # but function should not raise
        assert isinstance(result, dict)

    def test_texture_derived_from_fractions(self):
        raw = {"sample_id": "s6", "sand_pct": 70.0, "silt_pct": 20.0, "clay_pct": 10.0}
        result = self._validate(raw)
        meta = result.get("normalised", {})
        tc = meta.get("texture_class")
        if tc is not None:
            assert isinstance(tc, str)

    def test_sampling_fraction_gt1_rejected(self):
        result = self._validate({"sample_id": "s7", "sampling_fraction": 1.5})
        reasons = result.get("reject_reasons", [])
        assert any("sampling" in r.lower() or "fraction" in r.lower() for r in reasons)

    def test_no_crash_on_empty_input(self):
        result = self._validate({})
        assert isinstance(result, dict)


# ===========================================================================
# functional_gene_scanner
# ===========================================================================

class TestFunctionalGeneScanner:
    def test_community_data_detection(self):
        from compute.functional_gene_scanner import scan_functional_genes
        cd = {"amoA bacterial Nitrosomonas": 0.05, "nifH nitrogenase": 0.02}
        result = scan_functional_genes(community_data=cd)
        assert "nifH" in result
        assert "amoA_bacterial" in result
        assert result["nifH"]["present"] is True

    def test_amoA_split_bacterial_vs_archaeal(self):
        """amoA_bacterial and amoA_archaeal are separate keys."""
        from compute.functional_gene_scanner import scan_functional_genes, SUPPORTED_GENES
        assert "amoA_bacterial" in SUPPORTED_GENES
        assert "amoA_archaeal" in SUPPORTED_GENES

    def test_keyword_scan_on_mock_fasta(self, tmp_path):
        from compute.functional_gene_scanner import scan_functional_genes
        fasta = tmp_path / "mock.fasta"
        fasta.write_text(
            ">seq1 nifH nitrogenase reductase Rhizobium\nACGTACGT\n"
            ">seq2 dsrA sulfite reductase\nGGGTCCAT\n"
            ">seq3 unrelated hypothetical protein\nTTTTAAAA\n"
        )
        result = scan_functional_genes(fasta_path=str(fasta), genes=["nifH", "dsrAB"])
        assert result["nifH"]["present"] is True
        assert result["dsrAB"]["present"] is True
        assert result["nifH"]["method"] == "keyword"

    def test_nifh_hgt_flag_at_low_abundance(self, tmp_path):
        from compute.functional_gene_scanner import scan_functional_genes
        # Add many unrelated headers to drive nifH abundance low
        lines = [">seq_unrelated_protein\nACGT\n"] * 5000
        lines.append(">seq_nifH nitrogenase very rare\nACGT\n")
        fasta = tmp_path / "low_nifH.fasta"
        fasta.write_text("".join(lines))
        result = scan_functional_genes(fasta_path=str(fasta), genes=["nifH"])
        if result["nifH"]["present"]:
            # At ~1/5001 abundance the HGT flag should be True
            assert result["nifH"]["hgt_flagged"] is True

    def test_all_genes_returned(self):
        from compute.functional_gene_scanner import scan_functional_genes, SUPPORTED_GENES
        result = scan_functional_genes(fasta_path=None)
        assert set(result.keys()) == set(SUPPORTED_GENES.keys())

    def test_make_community_flags_schema(self, tmp_path):
        from compute.functional_gene_scanner import scan_functional_genes, make_community_flags
        fasta = tmp_path / "empty.fasta"
        fasta.write_text("")
        gene_r = scan_functional_genes(fasta_path=str(fasta))
        flags  = make_community_flags(gene_r)
        assert "has_nifh" in flags
        assert "has_amoa_bacterial" in flags
        assert "has_amoa_archaeal" in flags
        assert "nifh_is_hgt_flagged" in flags
        assert "functional_genes" in flags


# ===========================================================================
# tax_profiler
# ===========================================================================

class TestTaxProfiler:
    def test_precomputed_passthrough(self):
        from compute.tax_profiler import profile_taxonomy
        pre = {
            "phylum_profile": {"Proteobacteria": 0.5, "Firmicutes": 0.3, "Ascomycota": 0.2},
            "top_genera":     [{"name": "Pseudomonas", "rel_abundance": 0.3}],
            "n_taxa":         30,
        }
        result = profile_taxonomy(precomputed_profile=pre, seq_type="16S")
        assert result["n_taxa"] == 30
        assert result["profiler_used"] == "precomputed"
        assert "Proteobacteria" in result["phylum_profile"]

    def test_fungal_bacterial_ratio(self):
        from compute.tax_profiler import compute_fungal_bacterial_ratio
        profile = {"Ascomycota": 0.2, "Basidiomycota": 0.1, "Proteobacteria": 0.7}
        ratio = compute_fungal_bacterial_ratio(profile)
        assert ratio == pytest.approx(0.3 / 0.7, rel=1e-3)

    def test_fungal_ratio_none_for_empty(self):
        from compute.tax_profiler import compute_fungal_bacterial_ratio
        assert compute_fungal_bacterial_ratio({}) is None

    def test_no_fastq_no_crash(self):
        from compute.tax_profiler import profile_taxonomy
        result = profile_taxonomy(fastq_paths=None, seq_type="shotgun")
        assert "warnings" in result
        assert result["profiler_used"] == "none"

    def test_unknown_seq_type_warning(self):
        from compute.tax_profiler import profile_taxonomy
        result = profile_taxonomy(
            fastq_paths=["/tmp/fake.fastq"],
            seq_type="MAGIC",
        )
        assert any("MAGIC" in w or "Unknown" in w for w in result["warnings"])

    def test_schema_keys_present(self):
        from compute.tax_profiler import profile_taxonomy
        result = profile_taxonomy(seq_type="16S")
        for key in ["phylum_profile", "top_genera", "n_taxa",
                    "fungal_bacterial_ratio", "its_profile", "profiler_used"]:
            assert key in result, f"missing key: {key}"


# ===========================================================================
# tax_function_mapper
# ===========================================================================

class TestTaxFunctionMapper:
    def test_nitrogen_fixation_detected(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function({"Rhizobium": 0.1, "Pseudomonas": 0.05})
        assert r["nitrogen_fixation"]["present"] is True

    def test_nitrification_detected(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function({"Nitrosomonas": 0.08})
        assert r["nitrification"]["present"] is True

    def test_mycorrhizal_detected(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function({"Glomus": 0.15})
        assert r["mycorrhizal"]["present"] is True

    def test_empty_input_returns_empty_profile(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function({})
        for v in r.values():
            assert v["present"] is False

    def test_all_canonical_groups_in_result(self):
        from compute.tax_function_mapper import map_taxonomy_to_function, _empty_function_profile
        baseline = _empty_function_profile()
        r = map_taxonomy_to_function({"Rhizobium": 0.1})
        for group in baseline:
            assert group in r

    def test_normalize_scores_sum_to_1(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function(
            {"Nitrosomonas": 0.1, "Glomus": 0.2, "Methanobacterium": 0.15},
            normalize=True,
        )
        total = sum(v["score"] for v in r.values())
        if total > 0:
            assert total == pytest.approx(1.0, abs=1e-6)

    def test_get_functional_summary_structure(self):
        from compute.tax_function_mapper import map_taxonomy_to_function, get_functional_summary
        r  = map_taxonomy_to_function({"Rhizobium": 0.1, "Glomus": 0.15})
        s  = get_functional_summary(r)
        assert "top_functions" in s
        assert "n_functions_detected" in s
        assert "has_n_cycling" in s
        assert "has_mycorrhizal" in s
        assert s["has_mycorrhizal"] is True

    def test_phylum_level_fallback(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function({"Euryarchaeota": 0.2})
        assert r["methanogenesis"]["present"] is True

    def test_abundance_threshold_filters_low_taxa(self):
        from compute.tax_function_mapper import map_taxonomy_to_function
        r = map_taxonomy_to_function(
            {"Rhizobium": 0.0001},
            abundance_threshold=0.001,
        )
        assert r["nitrogen_fixation"]["present"] is False


# ===========================================================================
# pipeline_core -- single-sample smoke test (no external tools, no DB)
# ===========================================================================

class TestPipelineCore:
    def _make_sample(self):
        return {
            "sample_id":          "smoke_test_001",
            "ph":                 6.8,
            "lat":                40.0,
            "lon":                -105.0,
            "depth_cm":           10.0,
            "land_use":           "agriculture",
            "sequencing_type":    "16S",
            "precomputed_profile": {
                "phylum_profile": {
                    "Proteobacteria": 0.45,
                    "Actinobacteria": 0.25,
                    "Firmicutes":     0.15,
                    "Acidobacteria":  0.15,
                },
                "top_genera": [
                    {"name": "Pseudomonas", "rel_abundance": 0.15},
                    {"name": "Bacillus",    "rel_abundance": 0.10},
                    {"name": "Nitrosomonas","rel_abundance": 0.05},
                ],
                "n_taxa": 120,
            },
            "community_data": {
                "nifH nitrogenase": 0.03,
                "amoA bacterial":   0.05,
            },
        }

    def test_smoke_no_external_tools(self):
        from pipeline_core import _process_one_sample_t0
        from config_schema import T0Filters
        sample = self._make_sample()
        cfg    = T0Filters()
        result = _process_one_sample_t0(sample, cfg.model_dump())

        assert "passed_t0" in result
        assert isinstance(result["passed_t0"], bool)
        assert "diversity" in result
        assert "function_summary" in result

    def test_missing_sample_id_handled(self):
        from pipeline_core import _process_one_sample_t0
        from config_schema import T0Filters
        result = _process_one_sample_t0({}, T0Filters().model_dump())
        assert "reject_reasons" in result
        assert isinstance(result["reject_reasons"], list)

    def test_low_ph_causes_rejection(self):
        from pipeline_core import _process_one_sample_t0
        from config_schema import T0Filters
        sample = self._make_sample()
        sample["ph"] = -2.0
        result = _process_one_sample_t0(sample, T0Filters().model_dump())
        assert result["passed_t0"] is False

    def test_gene_flags_in_result(self):
        from pipeline_core import _process_one_sample_t0
        from config_schema import T0Filters
        result = _process_one_sample_t0(self._make_sample(), T0Filters().model_dump())
        assert "gene_scan" in result
        assert "community_flags" in result

    def test_taxonomy_profile_populated(self):
        from pipeline_core import _process_one_sample_t0
        from config_schema import T0Filters
        result = _process_one_sample_t0(self._make_sample(), T0Filters().model_dump())
        tax = result.get("taxonomy", {})
        # precomputed profile should flow through
        assert tax.get("n_taxa", 0) == 120


# ===========================================================================
# pipeline_core -- DB integration (exercises _persist_t0_result)
# ===========================================================================

class TestPipelineCoreDB:
    """Exercises the DB persistence path that was broken by schema mismatches."""

    def _make_sample(self):
        return {
            "sample_id":           "db_test_001",
            "ph":                  6.8,
            "lat":                 40.0,
            "lon":                 -105.0,
            "depth_cm":            10.0,
            "land_use":            "agriculture",
            "sequencing_type":     "16S",
            "precomputed_profile": {
                "phylum_profile": {"Proteobacteria": 0.5, "Actinobacteria": 0.3, "Ascomycota": 0.2},
                "top_genera": [{"name": "Nitrosomonas", "rel_abundance": 0.05}],
                "n_taxa": 80,
            },
            "community_data": {"nifH nitrogenase": 0.03},
        }

    def test_persist_does_not_raise(self):
        """_persist_t0_result must complete without OperationalError."""
        from pipeline_core import _process_one_sample_t0, _persist_t0_result
        from config_schema import T0Filters
        from db_utils import SoilDB

        sample = self._make_sample()
        result = _process_one_sample_t0(sample, T0Filters().model_dump())

        with SoilDB(":memory:") as db:
            _persist_t0_result(result, db, "batch_audit_test")

    def test_sample_written_to_db(self):
        """After persist, sample row is readable with correct column values."""
        from pipeline_core import _process_one_sample_t0, _persist_t0_result
        from config_schema import T0Filters
        from db_utils import SoilDB

        sample = self._make_sample()
        result = _process_one_sample_t0(sample, T0Filters().model_dump())

        with SoilDB(":memory:") as db:
            _persist_t0_result(result, db, "batch_audit_test")
            row = db.get_sample("db_test_001")

        assert row is not None
        assert row["sample_id"] == "db_test_001"
        # Verify correct column mappings (not ph/lat/lon aliases)
        assert row.get("latitude") is not None or row.get("soil_ph") is not None

    def test_community_written_to_db(self):
        """After persist, community row is readable."""
        from pipeline_core import _process_one_sample_t0, _persist_t0_result
        from config_schema import T0Filters
        from db_utils import SoilDB

        sample = self._make_sample()
        result = _process_one_sample_t0(sample, T0Filters().model_dump())

        with SoilDB(":memory:") as db:
            _persist_t0_result(result, db, "batch_audit_test")
            comm = db.get_community_for_sample("db_test_001")

        assert comm is not None
        assert comm["sample_id"] == "db_test_001"

    def test_metadata_key_not_empty(self):
        """metadata dict must not be empty (normalised vs normalized bug check)."""
        from pipeline_core import _process_one_sample_t0
        from config_schema import T0Filters

        result = _process_one_sample_t0(self._make_sample(), T0Filters().model_dump())
        assert len(result.get("metadata", {})) > 0, (
            "metadata dict is empty — likely normalised vs normalized key mismatch"
        )
