"""
compute/functional_gene_scanner.py -- T0 functional gene presence/absence detection.

Detects functional genes via MMseqs2 (fast sequence homology against curated
reference sequence databases) or a fallback header-keyword scan for FASTA
files with annotated sequence names.

Gene catalogue (v2 -- split amoA bacterial/archaeal per REBUILD_PLAN Gap 2):

  nifH            nitrogenase reductase (nitrogen fixation)
  dsrAB           dissimilatory sulfite reductase (sulfate reduction)
  mcrA            methyl-coenzyme M reductase (methanogenesis)
  mmox            particulate methane monooxygenase (methane oxidation)
  amoA_bacterial  bacterial ammonia monooxygenase subunit A (nitrification)
  amoA_archaeal   archaeal ammonia monooxygenase subunit A (AOA nitrification)
  laccase         multicopper oxidase (lignin degradation / C sequestration)
  peroxidase      ligninolytic peroxidase -- MnP/LiP/VP (C sequestration)
  alkB            alkane 1-monooxygenase (bioremediation)
  phn             phosphonate lyase (P cycling)
  mer             mercuric reductase (Hg detoxification)

Usage:
  from core.compute.functional_gene_scanner import scan_functional_genes
  profile = scan_functional_genes(fasta_path, genes=["nifH","amoA_bacterial"])
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from core.compute._tool_resolver import resolve_tool

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gene catalogue
# ---------------------------------------------------------------------------

SUPPORTED_GENES: dict[str, dict] = {
    "nifH": {
        "description": "nitrogenase reductase",
        "keywords": ["nifH", "nitrogenase", "nif"],
        "hgt_risk": True,   # nifH can be laterally transferred, flag for review
    },
    "dsrAB": {
        "description": "dissimilatory sulfite reductase",
        "keywords": ["dsrA", "dsrB", "sulfite reductase", "dsr"],
        "hgt_risk": False,
    },
    "mcrA": {
        "description": "methyl-coenzyme M reductase alpha (methanogenesis)",
        "keywords": ["mcrA", "methyl-coenzyme M reductase", "mcr"],
        "hgt_risk": False,
    },
    "mmox": {
        "description": "particulate methane monooxygenase",
        "keywords": ["pmoA", "mmoX", "methane monooxygenase", "pmo"],
        "hgt_risk": False,
    },
    "amoA_bacterial": {
        "description": "bacterial ammonia monooxygenase subunit A",
        "keywords": ["amoA", "ammonia monooxygenase"],
        "lineage_filter": ["Nitrosomonas", "Nitrosospira", "Nitrosovibrio",
                           "Nitrosolobus", "Nitrosococcus", "Betaproteobacteria",
                           "Gammaproteobacteria"],
        "hgt_risk": False,
    },
    "amoA_archaeal": {
        "description": "archaeal ammonia monooxygenase subunit A (AOA)",
        "keywords": ["amoA", "ammonia monooxygenase"],
        "lineage_filter": ["Thaumarchaeota", "Crenarchaeota", "Archaea",
                           "Nitrosopumilales", "Nitrososphaera",
                           "Candidatus Nitrosocosmicus"],
        "hgt_risk": False,
    },
    "laccase": {
        "description": "multicopper oxidase (lignin degradation)",
        "keywords": ["laccase", "multicopper oxidase", "MCO", "CotA"],
        "hgt_risk": False,
    },
    "peroxidase": {
        "description": "ligninolytic peroxidase",
        "keywords": ["lignin peroxidase", "manganese peroxidase", "versatile peroxidase",
                     "MnP", "LiP", "VP", "DyP"],
        "hgt_risk": False,
    },
    "alkB": {
        "description": "alkane 1-monooxygenase (hydrocarbon degradation)",
        "keywords": ["alkB", "alkane monooxygenase", "alkane hydroxylase"],
        "hgt_risk": False,
    },
    "phn": {
        "description": "phosphonate lyase (phosphorus cycling)",
        "keywords": ["phnJ", "phosphonate lyase", "C-P lyase"],
        "hgt_risk": False,
    },
    "mer": {
        "description": "mercuric reductase (mercury detoxification)",
        "keywords": ["merA", "mercuric reductase"],
        "hgt_risk": False,
    },
}

# ---------------------------------------------------------------------------
# Curated verified diazotroph lineages (HGT-aware nifH validation — Gap 7)
# Source: Gaby & Buckley 2012 (Environ Microbiol); Dos Santos et al 2012 (BMC Genomics)
# ---------------------------------------------------------------------------

_VERIFIED_DIAZOTROPH_GENERA: frozenset[str] = frozenset({
    # Free-living N-fixers (well-documented)
    "Azotobacter", "Azoarcus", "Azospirillum", "Herbaspirillum", "Gluconacetobacter",
    "Burkholderia", "Paenibacillus", "Bacillus", "Clostridium", "Desulfovibrio",
    "Frankia",  # actinorhizal symbiont
    # Cyanobacterial N-fixers
    "Anabaena", "Nostoc", "Aphanizomenon", "Cylindrospermum", "Fischerella",
    "Trichodesmium", "Cyanothece", "Calothrix",
    # Rhizobial / legume symbionts
    "Rhizobium", "Bradyrhizobium", "Sinorhizobium", "Mesorhizobium", "Azorhizobium",
    "Cupriavidus", "Phyllobacterium",
    # Methanotrophic / anoxygenic phototroph
    "Rhodospirillum", "Rhodopseudomonas", "Rhodobacter", "Chlorobaculum",
    # Diazotrophic sulfate reducers
    "Desulfobacter", "Desulfosporosinus",
})

# Genera known to carry non-functional nifH via HGT without regulatory context
_KNOWN_HGT_ONLY_GENERA: frozenset[str] = frozenset({
    "Geodermatophilus",      # nifH present but no active N-fixation detected
    "Methylobacterium",      # some strains have HGT-acquired nifH; verify
    "Microcoleus",           # some strains non-functional
    "Magnetospirillum",      # nifH present, functional status unclear
})


def validate_nifh_functional(
    nifh_data: dict,
    taxonomy: dict | None = None,
) -> dict:
    """
    Post-process nifH hits to flag probable HGT-acquired non-functional copies.

    Cross-references detected genera (from taxonomy dict or nifH hit annotation)
    against a curated list of verified diazotroph lineages.

    Parameters
    ----------
    nifh_data : dict
        The nifH result dict from scan_functional_genes() for the 'nifH' gene.
    taxonomy : dict | None
        Community taxonomy dict (genus → relative_abundance). If provided,
        checks which genera carrying nifH are verified diazotrophs.

    Returns
    -------
    dict: updated nifH result dict with added keys:
        functional_confidence        'high' | 'medium' | 'low'
        verified_diazotroph_genera   list[str]  genera confirmed as diazotrophs
        hgt_flagged                  bool       True if likely non-functional HGT
        functional_note              str        human-readable explanation
    """
    result = dict(nifh_data)  # make a copy, don't mutate

    if not result.get("present"):
        return result

    # If no taxonomy available, fall through to abundance heuristic only
    if taxonomy:
        genera_in_community = set(taxonomy.keys())
        verified = genera_in_community & _VERIFIED_DIAZOTROPH_GENERA
        hgt_only = genera_in_community & _KNOWN_HGT_ONLY_GENERA
        non_diazotroph_burden = len(genera_in_community - _VERIFIED_DIAZOTROPH_GENERA - _KNOWN_HGT_ONLY_GENERA)

        if verified:
            confidence = "high"
            hgt_flag = False
            note = f"Verified diazotroph genera present: {sorted(verified)}"
        elif hgt_only and not verified:
            confidence = "low"
            hgt_flag = True
            note = (
                f"nifH detected but only in HGT-known genera {sorted(hgt_only)} — "
                "functional N-fixation unlikely. Cross-validate with acetylene reduction assay."
            )
        else:
            # Presence in community but no verified genera — medium confidence
            confidence = "medium"
            hgt_flag = result.get("hgt_flagged", False)
            note = (
                "nifH detected; no verified diazotroph genera identified in taxonomy. "
                "May reflect HGT-acquired copies or rare/unresolved diazotrophs."
            )
    else:
        # Abundance-only heuristic (no taxonomy available)
        abundance = result.get("abundance") or 0.0
        if abundance >= 0.01:
            confidence = "medium"
            hgt_flag = False
            note = "nifH detected at meaningful abundance; verify with taxonomy cross-reference."
        else:
            confidence = "low"
            hgt_flag = True
            note = (
                f"nifH abundance {abundance:.2e} is below functional threshold (0.01). "
                "Likely spurious or HGT-acquired."
            )

    result["functional_confidence"] = confidence
    result["verified_diazotroph_genera"] = sorted(verified) if taxonomy else []
    result["hgt_flagged"] = hgt_flag
    result["functional_note"] = note
    return result

def scan_functional_genes(
    fasta_path: str | Path | None = None,
    genes: list[str] | None = None,
    mmseqs_threads: int = 4,
    min_identity: float = 0.5,
    min_coverage: float = 0.7,
    db_dir: str | Path | None = None,
    community_data: dict | None = None,
) -> dict[str, Any]:
    """
    Detect functional gene presence and abundance in a metagenome or
    pre-computed community profile.

    Strategy (in order of preference):
      A. MMseqs2 against curated reference DBs (if mmseqs2 in PATH and fasta given)
      B. FASTA header keyword scan (fast, lower precision)
      C. community_data dict scan (for pre-processed samples, e.g. PICRUSt2 output)

    Parameters
    ----------
    fasta_path      : path to assembled metagenome FASTA. May be None for C-path.
    genes           : subset of SUPPORTED_GENES to scan. None = all genes.
    mmseqs_threads  : threads for MMseqs2.
    min_identity    : minimum sequence identity threshold (0–1).
    min_coverage    : minimum query coverage threshold (0–1).
    db_dir          : directory containing pre-built MMseqs2 reference DBs per gene.
    community_data  : pre-computed profile dict (e.g. from PICRUSt2 output) with
                      gene names as keys and relative abundance as values.

    Returns
    -------
    dict: gene_name -> {
        present          bool
        abundance        float | None  (relative, 0-1; None if not quantified)
        hits             int | None    (read count; None if not counted)
        hgt_flagged      bool          (nifH HGT risk flag)
        method           str           ('mmseqs2'|'keyword'|'community_data')
    }
    """
    target_genes = genes or list(SUPPORTED_GENES.keys())
    # Validate requested genes
    unknown = [g for g in target_genes if g not in SUPPORTED_GENES]
    if unknown:
        raise ValueError(f"Unknown genes: {unknown}. Supported: {list(SUPPORTED_GENES)}")

    # Initialise results
    results: dict[str, Any] = {
        gene: {
            "present": False,
            "abundance": None,
            "hits": None,
            "hgt_flagged": False,
            "method": "not_run",
        }
        for gene in target_genes
    }

    # --- Path C: community_data dict ---
    if community_data:
        _scan_community_data(results, community_data, target_genes)
        return results

    if fasta_path is None:
        return results

    path = Path(fasta_path)
    if not path.exists():
        logger.warning("FASTA not found: %s", path)
        return results

    # --- Path A: MMseqs2 ---
    _mmseqs = resolve_tool("mmseqs") or shutil.which("mmseqs")
    if _mmseqs and db_dir:
        try:
            _mmseqs_scan(results, path, target_genes, db_dir,
                         mmseqs_threads, min_identity, min_coverage,
                         mmseqs_cmd=_mmseqs)
            return results
        except Exception as exc:
            logger.warning("MMseqs2 scan failed (%s), falling back to keyword scan", exc)

    # --- Path B: FASTA header keyword scan ---
    _keyword_scan(results, path, target_genes)
    return results


def make_community_flags(gene_results: dict) -> dict[str, bool | None]:
    """
    Convert scan_functional_genes output to flat boolean flags for DB storage.

    Returns dict matching communities table column names:
      has_nifh, has_dsrab, has_mcra, has_mmox, has_amoa_bacterial,
      has_amoa_archaeal, has_laccase, has_peroxidase, nifh_is_hgt_flagged
    """
    def get(gene: str) -> bool:
        return gene_results.get(gene, {}).get("present", False)

    return {
        "has_nifh":              get("nifH"),
        "has_dsrab":             get("dsrAB"),
        "has_mcra":              get("mcrA"),
        "has_mmox":              get("mmox"),
        "has_amoa_bacterial":    get("amoA_bacterial"),
        "has_amoa_archaeal":     get("amoA_archaeal"),
        "has_laccase":           get("laccase"),
        "has_peroxidase":        get("peroxidase"),
        "nifh_is_hgt_flagged":   gene_results.get("nifH", {}).get("hgt_flagged", False),
        "functional_genes":      json.dumps({
            g: {k: v for k, v in d.items() if k != "method"}
            for g, d in gene_results.items()
        }),
    }


# ---------------------------------------------------------------------------
# Internal scan implementations
# ---------------------------------------------------------------------------

def _scan_community_data(
    results: dict,
    community_data: dict,
    target_genes: list[str],
) -> None:
    """Detect genes from a pre-computed abundance dict (PICRUSt2 / HUMAnN3 output)."""
    cd_lower = {k.lower(): v for k, v in community_data.items()}

    for gene in target_genes:
        gene_info = SUPPORTED_GENES[gene]
        lineage_filter = gene_info.get("lineage_filter")
        # Match by keyword against community_data keys
        total_abundance = 0.0
        hits = 0
        for kw in gene_info["keywords"]:
            for cd_key, val in cd_lower.items():
                if kw.lower() in cd_key:
                    # If lineage_filter defined, require lineage match in key
                    if lineage_filter:
                        if not any(lf.lower() in cd_key for lf in lineage_filter):
                            continue
                    try:
                        total_abundance += float(val)
                        hits += 1
                    except (TypeError, ValueError):
                        pass

        if total_abundance > 0:
            results[gene]["present"] = True
            results[gene]["abundance"] = min(total_abundance, 1.0)
            results[gene]["hits"] = hits
            results[gene]["method"] = "community_data"
            if gene == "nifH" and SUPPORTED_GENES[gene].get("hgt_risk"):
                results[gene]["hgt_flagged"] = total_abundance < 0.001


def _keyword_scan(results: dict, fasta_path: Path, target_genes: list[str]) -> None:
    """
    Fast FASTA header keyword scan.

    Low precision (no alignment) but works without any external tools.
    Reads only the header lines (lines starting with '>') for speed.
    """
    import gzip

    opener = gzip.open if fasta_path.suffix == ".gz" else open
    gene_counts: dict[str, int] = {g: 0 for g in target_genes}
    total_seqs = 0

    try:
        with opener(fasta_path, "rt", errors="ignore") as fh:
            for line in fh:
                if not line.startswith(">"):
                    continue
                total_seqs += 1
                header_lower = line.lower()
                for gene in target_genes:
                    gene_info = SUPPORTED_GENES[gene]
                    lineage_filter = gene_info.get("lineage_filter")
                    for kw in gene_info["keywords"]:
                        if kw.lower() in header_lower:
                            # If lineage_filter defined, require lineage in header
                            if lineage_filter:
                                if not any(lf.lower() in header_lower for lf in lineage_filter):
                                    continue
                            gene_counts[gene] += 1
                            break
    except Exception as exc:
        logger.warning("keyword scan failed on %s: %s", fasta_path, exc)
        return

    for gene in target_genes:
        hits = gene_counts[gene]
        if hits > 0:
            results[gene]["present"] = True
            results[gene]["hits"] = hits
            results[gene]["abundance"] = hits / total_seqs if total_seqs else None
            results[gene]["method"] = "keyword"
            if gene == "nifH" and SUPPORTED_GENES[gene].get("hgt_risk"):
                # nifH HGT flag: present but at very low abundance
                results[gene]["hgt_flagged"] = (
                    results[gene]["abundance"] is not None
                    and results[gene]["abundance"] < 0.001
                )


def _mmseqs_scan(
    results: dict,
    fasta_path: Path,
    target_genes: list[str],
    db_dir: Path | str,
    threads: int,
    min_identity: float,
    min_coverage: float,
    mmseqs_cmd: str = "mmseqs",
) -> None:
    """
    Run MMseqs2 easy-search against per-gene reference databases.

    Expects one MMseqs2 database per gene at:
      {db_dir}/{gene_name}/db
    """
    db_dir = Path(db_dir)

    with tempfile.TemporaryDirectory(prefix="fgs_mmseqs_") as tmpdir:
        tmp = Path(tmpdir)
        query_db = tmp / "query"

        # Build query DB
        subprocess.run(
            [mmseqs_cmd, "createdb", str(fasta_path), str(query_db)],
            check=True, capture_output=True,
        )

        for gene in target_genes:
            ref_db = db_dir / gene / "db"
            if not ref_db.exists():
                logger.debug("No MMseqs2 DB for gene %s at %s, skipping", gene, ref_db)
                continue

            result_db = tmp / f"result_{gene}"
            aln_file  = tmp / f"aln_{gene}.tsv"

            try:
                subprocess.run(
                    [
                        mmseqs_cmd, "search",
                        str(query_db), str(ref_db), str(result_db), str(tmp),
                        "--threads", str(threads),
                        "--min-seq-id", str(min_identity),
                        "-c", str(min_coverage),
                        "--cov-mode", "0",
                        "-s", "5",
                    ],
                    check=True, capture_output=True,
                )
                subprocess.run(
                    [mmseqs_cmd, "convertalis", str(query_db), str(ref_db),
                     str(result_db), str(aln_file)],
                    check=True, capture_output=True,
                )

                hits = sum(1 for _ in open(aln_file))
                if hits > 0:
                    results[gene]["present"] = True
                    results[gene]["hits"] = hits
                    results[gene]["method"] = "mmseqs2"
                    if gene == "nifH" and SUPPORTED_GENES[gene].get("hgt_risk"):
                        results[gene]["hgt_flagged"] = False  # proper alignment, lower risk

            except subprocess.CalledProcessError as exc:
                logger.warning("MMseqs2 failed for gene %s: %s", gene, exc)
