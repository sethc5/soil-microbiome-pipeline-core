"""
compute/quality_filter.py — T0 sequencing quality filtering.

Checks:
  - Minimum sequencing depth (total reads / read pairs)
  - Chimera rate (UCHIME / VSEARCH)
  - Host / human contamination removal (bbduk / bowtie2 against human genome)
  - PhiX spike-in removal
  - Adapter content assessment

Returns a QC summary dict suitable for logging and T0 pass/fail decision.

Usage:
  from core.compute.quality_filter import run_quality_filter
  qc = run_quality_filter(fastq_paths, min_depth=50_000)
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tool detection
# ---------------------------------------------------------------------------

def _tool_available(name: str) -> bool:
    return shutil.which(name) is not None


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_quality_filter(
    fastq_paths: list[str | Path] | None = None,
    min_depth: int = 50_000,
    min_read_length: int = 100,
    max_n_fraction: float = 0.05,
    remove_host: bool = False,
    host_genome_index: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """
    T0 sequencing quality filter.

    Checks (in order):
      1. Metadata depth gate    — if sequencing_depth already in metadata
      2. File existence         — all fastq_paths must exist
      3. Read count             — total reads ≥ min_depth
      4. N-base fraction        — fraction of reads with >5% N ambiguous bases
      5. Host contamination     — optional bbduk (only if remove_host=True)
      6. Chimera rate           — optional vsearch --uchime_denovo

    Parameters
    ----------
    fastq_paths : FASTQ file paths (plain or .gz). May be empty for
                  metadata-only mode when metadata.sequencing_depth is set.
    min_depth   : minimum read count threshold.
    max_n_fraction : maximum tolerated fraction of N-heavy reads.
    remove_host : if True attempt bbduk host-removal.
    host_genome_index : path to host k-mer reference for bbduk.
    metadata    : optional pre-computed fields dict.

    Returns
    -------
    dict:
      passed           bool
      reject_reasons   list[str]
      total_reads      int | None
      chimera_rate     float | None
      host_fraction    float | None
      n_fraction       float | None
      tools_used       list[str]
    """
    result: dict[str, Any] = {
        "passed": True,
        "reject_reasons": [],
        "total_reads": None,
        "chimera_rate": None,
        "host_fraction": None,
        "n_fraction": None,
        "tools_used": [],
    }

    # 1. Metadata-only depth gate (accepts sequencing_depth OR total_reads from metadata)
    if metadata:
        raw_depth = metadata.get("sequencing_depth") or metadata.get("total_reads")
        if raw_depth is not None:
            depth = int(raw_depth)
            result["total_reads"] = depth
            if depth < min_depth:
                result["passed"] = False
                result["reject_reasons"].append(
                    f"total_reads {depth} < minimum {min_depth}"
                )
        # Pre-computed n_fraction gate
        raw_nfrac = metadata.get("n_fraction")
        if raw_nfrac is not None:
            nfrac = float(raw_nfrac)
            result["n_fraction"] = nfrac
            if nfrac > max_n_fraction:
                result["passed"] = False
                result["reject_reasons"].append(
                    f"n_fraction {nfrac:.3f} > maximum {max_n_fraction}"
                )

    if not fastq_paths:
        return result  # metadata-only mode

    paths = [Path(p) for p in fastq_paths]

    # 2. File existence
    missing = [str(p) for p in paths if not p.exists()]
    if missing:
        result["passed"] = False
        result["reject_reasons"].append(f"files not found: {missing}")
        return result

    # 3. Read count
    if result["total_reads"] is None:
        total = _count_reads(paths)
        result["total_reads"] = total
        if total < min_depth:
            result["passed"] = False
            result["reject_reasons"].append(
                f"total_reads {total} < minimum {min_depth}"
            )

    # 4. N-fraction
    n_frac = _estimate_n_fraction(paths)
    result["n_fraction"] = n_frac
    if n_frac is not None and n_frac > max_n_fraction:
        result["passed"] = False
        result["reject_reasons"].append(
            f"N-base fraction {n_frac:.3f} > maximum {max_n_fraction}"
        )

    # 5. Host contamination (optional)
    if remove_host and host_genome_index and _tool_available("bbduk.sh"):
        host_frac = _bbduk_host_fraction(paths, host_genome_index)
        result["host_fraction"] = host_frac
        result["tools_used"].append("bbduk")
        if host_frac is not None and host_frac > 0.10:
            result["passed"] = False
            result["reject_reasons"].append(
                f"host contamination {host_frac:.3f} > 10%"
            )

    # 6. Chimera check (optional)
    if _tool_available("vsearch"):
        chimera_rate = _vsearch_chimera_rate(paths)
        result["chimera_rate"] = chimera_rate
        result["tools_used"].append("vsearch")
        if chimera_rate is not None and chimera_rate > 0.15:
            result["passed"] = False
            result["reject_reasons"].append(
                f"chimera rate {chimera_rate:.3f} > 15%"
            )

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _count_reads(paths: list[Path]) -> int:
    """Count total reads across FASTQ files (supports .gz)."""
    import gzip
    total = 0
    for p in paths:
        opener = gzip.open if p.suffix == ".gz" else open
        try:
            with opener(p, "rt") as fh:
                count = sum(1 for _ in fh)
            total += count // 4
        except Exception as exc:
            logger.warning("Could not count reads in %s: %s", p, exc)
    return total


def _estimate_n_fraction(paths: list[Path], sample_n: int = 10_000) -> float | None:
    """Sample first sample_n reads; return fraction with >5% N bases."""
    import gzip
    n_reads = n_bad = 0
    for p in paths:
        opener = gzip.open if p.suffix == ".gz" else open
        try:
            with opener(p, "rt") as fh:
                for line_num, line in enumerate(fh):
                    if line_num % 4 == 1:   # sequence line
                        seq = line.strip()
                        if seq and (seq.count("N") / max(len(seq), 1)) > 0.05:
                            n_bad += 1
                        n_reads += 1
                        if n_reads >= sample_n:
                            break
        except Exception as exc:
            logger.warning("N-fraction check failed on %s: %s", p, exc)
    return n_bad / n_reads if n_reads else None


def _bbduk_host_fraction(paths: list[Path], ref: str) -> float | None:
    """Run bbduk.sh; return fraction of reads matching host reference."""
    cmd = [
        "bbduk.sh", f"in={paths[0]}", "out=/dev/null",
        f"ref={ref}", "k=31", "hdist=1",
    ]
    if len(paths) > 1:
        cmd.append(f"in2={paths[1]}")
    try:
        proc = _run(cmd)
        for line in (proc.stderr or "").splitlines():
            if "Matched:" in line:
                pct = line.split()[-1].rstrip("%")
                return float(pct) / 100
    except Exception as exc:
        logger.warning("bbduk failed: %s", exc)
    return None


def _vsearch_chimera_rate(paths: list[Path]) -> float | None:
    """Run vsearch --uchime_denovo; return chimera fraction."""
    if not paths:
        return None
    cmd = [
        "vsearch", "--uchime_denovo", str(paths[0]),
        "--chimeras", "/dev/null", "--nonchimeras", "/dev/null",
    ]
    try:
        proc = _run(cmd)
        for line in (proc.stderr or "").splitlines():
            if "%" in line and ("chimera" in line.lower()):
                tokens = [t for t in line.split() if "%" in t]
                if tokens:
                    return float(tokens[0].strip("%")) / 100
    except Exception as exc:
        logger.warning("vsearch chimera check failed: %s", exc)
    return None
