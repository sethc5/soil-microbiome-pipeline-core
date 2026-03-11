"""
compute/tax_profiler.py -- T0 taxonomy profiling router.

Routes sequencing data to the appropriate profiler based on sequencing type:

  16S / 18S / ITS  -->  QIIME2 CLI (subprocess)
      ITS uses UNITE ARB reference; 16S/18S use Greengenes2 or SILVA
  shotgun           -->  Kraken2 + Bracken
  metatranscriptome -->  MetaPhlAn 4

Returns a unified profile dict consumed by diversity_metrics.py and
tax_function_mapper.py.

Usage:
  from core.compute.tax_profiler import profile_taxonomy
  result = profile_taxonomy(fastq_paths=["R1.fq.gz", "R2.fq.gz"],
                            seq_type="shotgun", outdir="/tmp/kraken_out")
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def profile_taxonomy(
    fastq_paths: list[str | Path] | None = None,
    seq_type: str = "16S",
    outdir: str | Path | None = None,
    threads: int = 8,
    classifier_path: str | Path | None = None,
    kraken_db: str | Path | None = None,
    metaphlan_db: str | Path | None = None,
    precomputed_profile: dict | None = None,
) -> dict[str, Any]:
    """
    Profile taxonomy for a single sample.

    Parameters
    ----------
    fastq_paths        : list of R1 (and optionally R2) FASTQ files.
    seq_type           : one of ['16S','18S','ITS','shotgun','metatranscriptome'].
    outdir             : working directory for intermediate files (tmpdir if None).
    threads            : parallel threads for profiler call.
    classifier_path    : path to QIIME2-format sklearn classifier .qza.
    kraken_db          : path to Kraken2 database directory.
    metaphlan_db       : path to MetaPhlAn marker database directory.
    precomputed_profile: dict with keys phylum_profile / top_genera / etc.
                         When provided, this bypasses all tool calls.

    Returns
    -------
    dict with:
      phylum_profile        dict[str,float]  phylum -> relative abundance (0-1)
      top_genera            list[dict]       [{name, rel_abundance}] sorted desc
      otu_table_path        str | None
      n_taxa                int
      fungal_bacterial_ratio  float | None   (ITS only)
      its_profile           dict | None      (ITS only -- genus-level fungal profile)
      seq_type              str
      profiler_used         str
      warnings              list[str]
    """
    if precomputed_profile:
        return _normalise_precomputed(precomputed_profile, seq_type)

    seq_type = seq_type.upper() if seq_type else "16S"
    fastq_paths = [Path(p) for p in (fastq_paths or [])]

    base_result: dict[str, Any] = {
        "phylum_profile":        {},
        "top_genera":            [],
        "otu_table_path":        None,
        "n_taxa":                0,
        "fungal_bacterial_ratio": None,
        "its_profile":           None,
        "seq_type":              seq_type,
        "profiler_used":         "none",
        "warnings":              [],
    }

    if not fastq_paths:
        base_result["warnings"].append("No FASTQ paths provided; empty profile returned.")
        return base_result

    with tempfile.TemporaryDirectory(prefix="tax_profiler_") as _tmpdir:
        work_dir = Path(outdir) if outdir else Path(_tmpdir)
        work_dir.mkdir(parents=True, exist_ok=True)

        if seq_type in ("16S", "18S"):
            return _run_qiime2(
                fastq_paths, work_dir, threads, classifier_path, seq_type, base_result
            )
        elif seq_type == "ITS":
            return _run_qiime2_its(
                fastq_paths, work_dir, threads, classifier_path, base_result
            )
        elif seq_type == "SHOTGUN":
            return _run_kraken2(
                fastq_paths, work_dir, threads, kraken_db, base_result
            )
        elif seq_type in ("METATRANSCRIPTOME", "RNA"):
            return _run_metaphlan(
                fastq_paths, work_dir, threads, metaphlan_db, base_result
            )
        else:
            base_result["warnings"].append(f"Unknown seq_type '{seq_type}'; no profiling done.")
            return base_result


def compute_fungal_bacterial_ratio(phylum_profile: dict[str, float]) -> float | None:
    """
    Compute the ratio of fungal reads to bacterial reads from a phylum profile.

    Fungal phyla: Ascomycota, Basidiomycota, Chytridiomycota, Mucoromycota,
                  Mortierellomycota, Glomeromycota, Blastocladiomycota
    Bacterial phyla: everything else (when sum > 0)
    """
    if not phylum_profile:
        return None

    fungal_phyla = {
        "ascomycota", "basidiomycota", "chytridiomycota", "mucoromycota",
        "mortierellomycota", "glomeromycota", "blastocladiomycota",
        "fungi",
    }
    fungal_abund  = sum(v for k, v in phylum_profile.items() if k.lower() in fungal_phyla)
    total_abund   = sum(phylum_profile.values())
    bacterial_abund = total_abund - fungal_abund

    if bacterial_abund <= 0:
        return None
    return fungal_abund / bacterial_abund


# ---------------------------------------------------------------------------
# QIIME2 backend (16S / 18S)
# ---------------------------------------------------------------------------

def _run_qiime2(fastq_paths, work_dir, threads, classifier_path, seq_type, result):
    """Run QIIME2 dada2 + feature-classifier (16S or 18S)."""
    if not shutil.which("qiime"):
        result["warnings"].append(
            "qiime2 CLI not found; taxonomy profiling skipped. "
            "Install QIIME2 or provide precomputed_profile."
        )
        result["profiler_used"] = "qiime2_missing"
        return result

    if not classifier_path:
        result["warnings"].append(
            "No classifier_path provided for QIIME2; profiling skipped."
        )
        result["profiler_used"] = "qiime2_no_classifier"
        return result

    manifest = work_dir / "manifest.tsv"
    _write_qiime_manifest(manifest, fastq_paths)

    # Import reads
    imported = work_dir / "imported.qza"
    paired = len(fastq_paths) >= 2
    import_type = "SampleData[PairedEndSequencesWithQuality]" if paired else \
                  "SampleData[SequencesWithQuality]"
    import_fmt  = "PairedEndFastqManifestPhred33V2" if paired else \
                  "SingleEndFastqManifestPhred33V2"

    _qiime_cmd(["qiime", "tools", "import",
                "--type", import_type,
                "--input-path", str(manifest),
                "--output-path", str(imported),
                "--input-format", import_fmt], result)

    # DADA2 denoising
    denoised = work_dir / "table.qza"
    rep_seqs  = work_dir / "rep-seqs.qza"
    if paired:
        _qiime_cmd(["qiime", "dada2", "denoise-paired",
                    "--i-demultiplexed-seqs", str(imported),
                    "--p-trunc-len-f", "250",
                    "--p-trunc-len-r", "250",
                    "--p-n-threads", str(threads),
                    "--o-table", str(denoised),
                    "--o-representative-sequences", str(rep_seqs),
                    "--o-denoising-stats", str(work_dir / "dada2-stats.qza"),
                    "--verbose"], result)
    else:
        _qiime_cmd(["qiime", "dada2", "denoise-single",
                    "--i-demultiplexed-seqs", str(imported),
                    "--p-trunc-len", "250",
                    "--p-n-threads", str(threads),
                    "--o-table", str(denoised),
                    "--o-representative-sequences", str(rep_seqs),
                    "--o-denoising-stats", str(work_dir / "dada2-stats.qza"),
                    "--verbose"], result)

    # Feature classification
    taxonomy_qza = work_dir / "taxonomy.qza"
    _qiime_cmd(["qiime", "feature-classifier", "classify-sklearn",
                "--i-classifier", str(classifier_path),
                "--i-reads", str(rep_seqs),
                "--o-classification", str(taxonomy_qza),
                "--p-n-jobs", str(threads)], result)

    # Export taxonomy to TSV and parse
    taxonomy_dir = work_dir / "taxonomy_export"
    _qiime_cmd(["qiime", "tools", "export",
                "--input-path", str(taxonomy_qza),
                "--output-path", str(taxonomy_dir)], result)

    tsv = taxonomy_dir / "taxonomy.tsv"
    if tsv.exists():
        phylum_profile, top_genera, n_taxa = _parse_taxonomy_tsv(tsv)
        result.update({
            "phylum_profile":    phylum_profile,
            "top_genera":        top_genera,
            "n_taxa":            n_taxa,
            "otu_table_path":    str(denoised),
            "profiler_used":     "qiime2",
            "fungal_bacterial_ratio": compute_fungal_bacterial_ratio(phylum_profile),
        })

    return result


def _run_qiime2_its(fastq_paths, work_dir, threads, classifier_path, result):
    """Run QIIME2 for ITS amplicons using UNITE-trained classifier."""
    # ITS uses same QIIME2 pipeline but with different trimming (no fixed trunc-len)
    if not shutil.which("qiime"):
        result["warnings"].append("qiime2 CLI not found; ITS profiling skipped.")
        result["profiler_used"] = "qiime2_missing"
        return result

    # Re-use 16S path; ITS-specific: use --p-trunc-len 0 (variable length amplificate)
    result = _run_qiime2(fastq_paths, work_dir, threads, classifier_path, "ITS", result)

    # Tag ITS-specific fields
    if result.get("phylum_profile"):
        result["its_profile"] = {
            g["name"]: g["rel_abundance"]
            for g in result.get("top_genera", [])
        }
        result["fungal_bacterial_ratio"] = compute_fungal_bacterial_ratio(
            result["phylum_profile"]
        )
    result["profiler_used"] = "qiime2_unite"
    return result


# ---------------------------------------------------------------------------
# Kraken2 + Bracken backend (shotgun)
# ---------------------------------------------------------------------------

def _run_kraken2(fastq_paths, work_dir, threads, kraken_db, result):
    """Run Kraken2 taxonomic classification followed by Bracken re-estimation."""
    if not shutil.which("kraken2"):
        result["warnings"].append("kraken2 not found; shotgun profiling skipped.")
        result["profiler_used"] = "kraken2_missing"
        return result

    if not kraken_db:
        result["warnings"].append(
            "No kraken_db path provided; shotgun profiling skipped."
        )
        result["profiler_used"] = "kraken2_no_db"
        return result

    report = work_dir / "kraken2_report.txt"
    output = work_dir / "kraken2_output.txt"

    cmd = [
        "kraken2", "--db", str(kraken_db),
        "--threads", str(threads),
        "--report", str(report),
        "--output", str(output),
    ]
    if len(fastq_paths) >= 2:
        cmd += ["--paired", str(fastq_paths[0]), str(fastq_paths[1])]
    else:
        cmd += [str(fastq_paths[0])]

    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        result["warnings"].append(f"kraken2 failed: {exc.stderr.decode()[:200]}")
        result["profiler_used"] = "kraken2_error"
        return result

    # Bracken re-estimation at genus level
    bracken_output = work_dir / "bracken_genus.txt"
    if shutil.which("bracken"):
        try:
            subprocess.run([
                "bracken", "-d", str(kraken_db),
                "-i", str(report),
                "-o", str(bracken_output),
                "-l", "G",
                "-r", "150",
            ], check=True, capture_output=True)
            phylum_profile, top_genera, n_taxa = _parse_bracken_output(bracken_output, report)
        except subprocess.CalledProcessError:
            phylum_profile, top_genera, n_taxa = _parse_kraken_report(report)
    else:
        phylum_profile, top_genera, n_taxa = _parse_kraken_report(report)

    result.update({
        "phylum_profile":    phylum_profile,
        "top_genera":        top_genera,
        "n_taxa":            n_taxa,
        "otu_table_path":    str(bracken_output if bracken_output.exists() else report),
        "profiler_used":     "kraken2_bracken",
        "fungal_bacterial_ratio": compute_fungal_bacterial_ratio(phylum_profile),
    })
    return result


# ---------------------------------------------------------------------------
# MetaPhlAn backend (metatranscriptome)
# ---------------------------------------------------------------------------

def _run_metaphlan(fastq_paths, work_dir, threads, metaphlan_db, result):
    """Run MetaPhlAn 4 for metatranscriptome profiling."""
    if not shutil.which("metaphlan"):
        result["warnings"].append("metaphlan not found; RNA profiling skipped.")
        result["profiler_used"] = "metaphlan_missing"
        return result

    profile_txt = work_dir / "metaphlan_profile.txt"
    cmd = [
        "metaphlan",
        str(fastq_paths[0]),
        "--input_type", "fastq",
        "--nproc", str(threads),
        "-o", str(profile_txt),
    ]
    if metaphlan_db:
        cmd += ["--bowtie2db", str(metaphlan_db)]
    if len(fastq_paths) >= 2:
        cmd += ["--nproc", str(threads)]

    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        result["warnings"].append(f"metaphlan failed: {exc.stderr.decode()[:200]}")
        result["profiler_used"] = "metaphlan_error"
        return result

    phylum_profile, top_genera, n_taxa = _parse_metaphlan_output(profile_txt)
    result.update({
        "phylum_profile": phylum_profile,
        "top_genera":     top_genera,
        "n_taxa":         n_taxa,
        "otu_table_path": str(profile_txt),
        "profiler_used":  "metaphlan4",
        "fungal_bacterial_ratio": compute_fungal_bacterial_ratio(phylum_profile),
    })
    return result


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_taxonomy_tsv(tsv_path: Path) -> tuple[dict, list, int]:
    """Parse QIIME2 exported taxonomy.tsv -> (phylum_profile, top_genera, n_taxa)."""
    phylum_counts: dict[str, int] = {}
    genus_counts:  dict[str, int] = {}

    with open(tsv_path) as fh:
        for line in fh:
            if line.startswith("Feature") or line.startswith("#"):
                continue
            parts = line.strip().split("\t")
            if len(parts) < 2:
                continue
            lineage = parts[1]
            phylum = _extract_rank(lineage, "p__")
            genus  = _extract_rank(lineage, "g__")
            if phylum:
                phylum_counts[phylum] = phylum_counts.get(phylum, 0) + 1
            if genus:
                genus_counts[genus]  = genus_counts.get(genus, 0) + 1

    total = max(sum(phylum_counts.values()), 1)
    phylum_profile = {k: v / total for k, v in phylum_counts.items()}
    top_genera = sorted(
        [{"name": g, "rel_abundance": c / total} for g, c in genus_counts.items()],
        key=lambda x: x["rel_abundance"],
        reverse=True,
    )[:50]
    return phylum_profile, top_genera, len(genus_counts)


def _parse_kraken_report(report: Path) -> tuple[dict, list, int]:
    """Parse Kraken2 report file -> (phylum_profile, top_genera, n_taxa)."""
    phylum: dict[str, float] = {}
    genera: dict[str, float] = {}

    with open(report) as fh:
        for line in fh:
            cols = line.strip().split("\t")
            if len(cols) < 6:
                continue
            pct   = float(cols[0])
            rank  = cols[3].strip()
            name  = cols[5].strip()
            if rank == "P":
                phylum[name] = pct / 100
            elif rank == "G":
                genera[name] = pct / 100

    top_genera = sorted(
        [{"name": g, "rel_abundance": v} for g, v in genera.items()],
        key=lambda x: x["rel_abundance"], reverse=True
    )[:50]
    return phylum, top_genera, len(genera)


def _parse_bracken_output(bracken_path: Path, kraken_report: Path) -> tuple[dict, list, int]:
    """Parse Bracken genus-level output and use Kraken2 report for phylum."""
    phylum, _, _ = _parse_kraken_report(kraken_report)
    genera: dict[str, float] = {}
    total_reads = 0

    with open(bracken_path) as fh:
        next(fh, None)  # header
        for line in fh:
            cols = line.strip().split("\t")
            if len(cols) < 7:
                continue
            name  = cols[0]
            reads = int(cols[5])
            genera[name] = reads
            total_reads  += reads

    if total_reads:
        genera = {k: v / total_reads for k, v in genera.items()}

    top_genera = sorted(
        [{"name": g, "rel_abundance": v} for g, v in genera.items()],
        key=lambda x: x["rel_abundance"], reverse=True
    )[:50]
    return phylum, top_genera, len(genera)


def _parse_metaphlan_output(profile_path: Path) -> tuple[dict, list, int]:
    """Parse MetaPhlAn profile output."""
    phylum: dict[str, float] = {}
    genera: dict[str, float] = {}

    with open(profile_path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            cols = line.strip().split("\t")
            if len(cols) < 2:
                continue
            lineage = cols[0]
            pct     = float(cols[1]) / 100

            if "|p__" in lineage and "|c__" not in lineage:
                name = lineage.split("|p__")[-1].split("|")[0]
                phylum[name] = pct
            elif "|g__" in lineage and "|s__" not in lineage:
                name = lineage.split("|g__")[-1].split("|")[0]
                genera[name] = pct

    top_genera = sorted(
        [{"name": g, "rel_abundance": v} for g, v in genera.items()],
        key=lambda x: x["rel_abundance"], reverse=True
    )[:50]
    return phylum, top_genera, len(genera)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _extract_rank(lineage: str, prefix: str) -> str | None:
    """Extract a taxonomic rank name from a QIIME2 semicolon-delimited lineage string."""
    for part in lineage.split(";"):
        part = part.strip()
        if part.startswith(prefix):
            name = part[len(prefix):]
            return name if name else None
    return None


def _write_qiime_manifest(manifest_path: Path, fastq_paths: list[Path]) -> None:
    """Write a QIIME2 manifest TSV for a single sample."""
    paired = len(fastq_paths) >= 2
    with open(manifest_path, "w") as fh:
        if paired:
            fh.write("sample-id\tforward-absolute-filepath\treverse-absolute-filepath\n")
            fh.write(f"sample1\t{fastq_paths[0]}\t{fastq_paths[1]}\n")
        else:
            fh.write("sample-id\tabsolute-filepath\n")
            fh.write(f"sample1\t{fastq_paths[0]}\n")


def _qiime_cmd(cmd: list, result: dict) -> None:
    """Run a QIIME2 CLI command, appending warnings on failure."""
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as exc:
        msg = f"QIIME2 command failed: {' '.join(cmd[:3])} — {exc.stderr.decode()[:200]}"
        logger.warning(msg)
        result.setdefault("warnings", []).append(msg)


def _normalise_precomputed(profile: dict, seq_type: str) -> dict:
    """Normalise a precomputed profile dict to the standard result schema."""
    return {
        "phylum_profile":        profile.get("phylum_profile", {}),
        "top_genera":            profile.get("top_genera", []),
        "otu_table_path":        profile.get("otu_table_path"),
        "n_taxa":                profile.get("n_taxa", len(profile.get("phylum_profile", {}))),
        "fungal_bacterial_ratio": profile.get("fungal_bacterial_ratio",
                                              compute_fungal_bacterial_ratio(
                                                  profile.get("phylum_profile", {}))),
        "its_profile":           profile.get("its_profile"),
        "seq_type":              seq_type,
        "profiler_used":         profile.get("profiler_used", "precomputed"),
        "warnings":              profile.get("warnings", []),
    }
