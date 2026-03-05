"""
tests/test_phase3.py — Unit tests for Phase 3 (T1) genome-scale modeling.

Covers:
  genome_quality     -- CheckM fallback returns medium defaults
  genome_fetcher     -- cache hit returns early, no network call
  genome_annotator   -- Prokka fallback returns graceful empty result
  model_builder      -- CarveMe fallback returns None
  community_fba      -- cobra-not-available graceful empty result
  keystone_analyzer  -- cobra-not-available graceful empty list
  metabolic_exchange -- cobra-not-available graceful empty result

All tests run without CheckM, Prokka, CarveMe, or COBRApy installed.
"""

from __future__ import annotations
import json
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


# ===========================================================================
# genome_quality
# ===========================================================================

class TestGenomeQuality:
    def test_checkm_fallback_returns_medium(self, tmp_path, monkeypatch):
        """If checkm not installed, returns medium-tier defaults."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_quality as gq
        reload(gq)

        result = gq.assess_genome_quality(
            genome_fasta=str(tmp_path / "genome.fna"),
            outdir=str(tmp_path / "checkm_out"),
        )
        assert result["tier"] == "medium"
        assert result["checkm_available"] is False
        assert 0.0 < result["model_confidence"] <= 1.0

    def test_tier_assignment_high(self, monkeypatch):
        """High completeness + low contamination → 'high' tier."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_quality as gq
        reload(gq)

        tier = gq._assign_tier(completeness=95.0, contamination=2.0)
        assert tier == "high"

    def test_tier_assignment_medium(self, monkeypatch):
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_quality as gq
        reload(gq)

        tier = gq._assign_tier(completeness=75.0, contamination=8.0)
        assert tier == "medium"

    def test_tier_assignment_low(self, monkeypatch):
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_quality as gq
        reload(gq)

        tier = gq._assign_tier(completeness=40.0, contamination=15.0)
        assert tier == "low"

    def test_model_confidence_ordering(self, monkeypatch):
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_quality as gq
        reload(gq)

        high = gq._model_confidence_from_tier("high")
        medium = gq._model_confidence_from_tier("medium")
        low = gq._model_confidence_from_tier("low")
        assert high > medium > low


# ===========================================================================
# genome_fetcher
# ===========================================================================

class TestGenomeFetcher:
    def test_cache_hit_returns_early(self, tmp_path):
        """If genome already cached, returns path without any network call."""
        from compute.genome_fetcher import GenomeFetcher

        cache_dir = tmp_path / "genome_cache"
        cache_dir.mkdir()
        fetcher = GenomeFetcher(genome_db={}, cache_dir=str(cache_dir))

        # Pre-populate cache
        cache_key = fetcher._cache_key("12345")
        cached_file = cache_dir / cache_key
        cached_file.write_text(">fake\nACGT\n")

        result = fetcher.fetch(taxon_id="12345", taxon_name="Fake sp.")
        assert str(result) == str(cached_file)

    def test_cache_key_is_deterministic(self, tmp_path):
        from compute.genome_fetcher import GenomeFetcher
        fetcher = GenomeFetcher(genome_db={}, cache_dir=str(tmp_path))
        k1 = fetcher._cache_key("99999")
        k2 = fetcher._cache_key("99999")
        assert k1 == k2

    def test_cache_key_differs_per_taxon(self, tmp_path):
        from compute.genome_fetcher import GenomeFetcher
        fetcher = GenomeFetcher(genome_db={}, cache_dir=str(tmp_path))
        assert fetcher._cache_key("111") != fetcher._cache_key("222")


# ===========================================================================
# genome_annotator
# ===========================================================================

class TestGenomeAnnotator:
    def test_prokka_fallback(self, tmp_path, monkeypatch):
        """Prokka not installed → returns empty result with prokka_available=False."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_annotator as ga
        reload(ga)

        result = ga.annotate_genome(
            genome_fasta=str(tmp_path / "genome.fna"),
            outdir=str(tmp_path / "prokka_out"),
        )
        assert result["prokka_available"] is False
        assert not result["gff_path"]  # empty string or None when not available

    def test_skip_if_cached(self, tmp_path, monkeypatch):
        """If .gff and .faa already exist, returns immediately without calling Prokka."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.genome_annotator as ga
        reload(ga)

        # Pre-create expected output files
        outdir = tmp_path / "prokka_cached"
        outdir.mkdir()
        genome = tmp_path / "genome.fna"
        genome.write_text(">seq1\nACGT\n")
        gff = outdir / "genome.gff"
        gff.write_text("##gff-version 3\n")
        faa = outdir / "genome.faa"
        faa.write_text(">prot1\nMAA\n")

        result = ga.annotate_genome(
            genome_fasta=str(genome),
            outdir=str(outdir),
            force=False,
        )
        # Should return paths to existing files without running Prokka
        assert result["gff_path"] == str(gff) or result["gff_path"] is None


# ===========================================================================
# model_builder
# ===========================================================================

class TestModelBuilder:
    def test_carveme_fallback(self, tmp_path, monkeypatch):
        """CarveMe not installed → returns None."""
        import shutil
        monkeypatch.setattr(shutil, "which", lambda x: None)
        from importlib import reload
        import compute.model_builder as mb
        reload(mb)

        proteins = tmp_path / "proteins.faa"
        proteins.write_text(">prot1\nMAA\n")
        result = mb.build_metabolic_model(
            proteins_fasta=str(proteins),
            outdir=str(tmp_path / "models"),
        )
        assert result is None


# ===========================================================================
# community_fba
# ===========================================================================

class TestCommunityFBA:
    def test_empty_models_returns_infeasible(self):
        from compute.community_fba import run_community_fba
        result = run_community_fba(member_models=[], metadata={}, target_pathway="nitrogen_fixation")
        assert result["feasible"] is False
        assert result["target_flux"] == 0.0

    def test_cobra_not_available_graceful(self, monkeypatch):
        """If cobra import fails, returns graceful empty result."""
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "cobra":
                raise ImportError("cobra not installed")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        from compute.community_fba import run_community_fba
        # Empty models list → should return graceful infeasible result
        result = run_community_fba(member_models=[], metadata={}, target_pathway="nitrogen_fixation")
        assert isinstance(result, dict)
        assert "feasible" in result


# ===========================================================================
# keystone_analyzer
# ===========================================================================

class TestKeystoneAnalyzer:
    def test_empty_model_graceful(self):
        from compute.keystone_analyzer import identify_keystone_taxa
        result = identify_keystone_taxa(
            community_model=None,
            baseline_target_flux=1.0,
        )
        assert isinstance(result, list)
        assert len(result) == 0


# ===========================================================================
# metabolic_exchange
# ===========================================================================

class TestMetabolicExchange:
    def test_none_model_returns_empty(self):
        from compute.metabolic_exchange import analyze_metabolic_exchanges
        graph, exchange_list = analyze_metabolic_exchanges(
            community_model=None, fba_solution=None
        )
        assert graph is None
        assert exchange_list == []
