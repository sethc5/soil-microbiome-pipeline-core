"""
compute/picrust2_runner.py — T0.25 functional prediction from 16S (PICRUSt2).

PICRUSt2 predicts MetaCyc pathway abundances from 16S ASV tables by placing
sequences on a reference phylogenetic tree and propagating known functional
annotations via ancestral state reconstruction.

Accuracy degrades for taxa with no close reference — common in soil (see README).

Usage:
  from compute.picrust2_runner import run_picrust2
  pred = run_picrust2(asv_table_biom, rep_seqs_fasta, outdir="picrust2_out/", threads=4)
"""

from __future__ import annotations
import logging
import math
import shutil
import subprocess
from pathlib import Path
from typing import Any

from compute._tool_resolver import resolve_tool

logger = logging.getLogger(__name__)


def _picrust2_cmd() -> str | None:
    """Return path to picrust2_pipeline executable, checking conda envs."""
    return (
        resolve_tool("picrust2_pipeline.py")
        or resolve_tool("picrust2_pipeline")
        or shutil.which("picrust2_pipeline.py")
        or shutil.which("picrust2_pipeline")
    )


def _picrust2_available() -> bool:
    return _picrust2_cmd() is not None


def _parse_abundance_table(path: Path) -> dict[str, float]:
    """Parse a TSV PICRUSt2 output table → {feature_id: mean_abundance_across_samples}."""
    if not path.exists():
        return {}
    import gzip, io

    opener = gzip.open if str(path).endswith(".gz") else open
    totals: dict[str, float] = {}
    n_samples = 0
    with opener(path, "rt") as fh:  # type: ignore[call-overload]
        header_done = False
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith("#"):
                continue
            parts = line.split("\t")
            if not header_done:
                n_samples = len(parts) - 1
                header_done = True
                continue
            feature_id = parts[0]
            values = [float(v) for v in parts[1:] if v]
            if values:
                totals[feature_id] = sum(values) / len(values)
    return totals


def _parse_nsti(nsti_path: Path) -> float:
    """Parse NSTI TSV → mean NSTI value."""
    if not nsti_path.exists():
        return float("nan")
    values: list[float] = []
    with nsti_path.open() as fh:
        for line in fh:
            if line.startswith("#") or line.startswith("sequence"):
                continue
            parts = line.strip().split("\t")
            if len(parts) >= 2:
                try:
                    values.append(float(parts[1]))
                except ValueError:
                    pass
    return sum(values) / len(values) if values else float("nan")


def run_picrust2(
    asv_table_biom: str | Path,
    rep_seqs_fasta: str | Path,
    outdir: str | Path = "picrust2_out/",
    threads: int = 4,
) -> dict[str, Any]:
    """
    Run PICRUSt2 pipeline and return pathway predictions.

    Returns keys: pathway_abundances (dict), ko_abundances (dict), nsti_mean (float)
    nsti_mean is the mean Nearest Sequenced Taxon Index — higher = less reliable.

    If PICRUSt2 is not installed, logs a warning and returns empty dicts with
    nsti_mean=nan so the pipeline continues gracefully.
    """
    asv_table_biom = Path(asv_table_biom)
    rep_seqs_fasta = Path(rep_seqs_fasta)
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if not _picrust2_available():
        logger.warning(
            "PICRUSt2 not found in PATH — returning empty functional predictions. "
            "Install via: conda install -c bioconda picrust2"
        )
        return {"pathway_abundances": {}, "ko_abundances": {}, "nsti_mean": float("nan")}

    cmd = [
        _picrust2_cmd(),
        "-s", str(rep_seqs_fasta),
        "-i", str(asv_table_biom),
        "-o", str(outdir),
        "-p", str(threads),
        "--in_traits", "COG,EC,KO,PFAM,TIGRFAM",
        "--skip_minpath",  # skip MinPath for speed; enable in production
    ]

    logger.info("Running PICRUSt2: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            logger.error("PICRUSt2 stderr: %s", result.stderr[-2000:])
            raise RuntimeError(f"PICRUSt2 exited with code {result.returncode}")
    except subprocess.TimeoutExpired:
        raise RuntimeError("PICRUSt2 timed out after 3600 seconds")

    # Locate output files (compressed or not)
    pathway_file = outdir / "pathways_out" / "path_abun_unstrat.tsv.gz"
    if not pathway_file.exists():
        pathway_file = outdir / "pathways_out" / "path_abun_unstrat.tsv"

    ko_file = outdir / "KO_predicted.tsv.gz"
    if not ko_file.exists():
        ko_file = outdir / "KO_predicted.tsv"

    nsti_files = list(outdir.rglob("marker_nsti_predicted.tsv"))
    nsti_mean = _parse_nsti(nsti_files[0]) if nsti_files else float("nan")

    pathway_abundances = _parse_abundance_table(pathway_file)
    ko_abundances = _parse_abundance_table(ko_file)

    logger.info(
        "PICRUSt2 complete: %d pathways, %d KOs, NSTI mean=%.4f",
        len(pathway_abundances),
        len(ko_abundances),
        nsti_mean if not math.isnan(nsti_mean) else -1,
    )
    return {
        "pathway_abundances": pathway_abundances,
        "ko_abundances": ko_abundances,
        "nsti_mean": nsti_mean,
    }
