"""
scripts/process_neon_16s.py — Download and classify NEON 16S amplicons.

For each NEON sample that has fastq_urls in communities.notes this script:
  1. Downloads R1 + R2 FASTQ from NEON GCS (or NEON storage)
  2. Trims with fastp (adapter removal, quality trim)
  3. Merges paired-end reads with vsearch
  4. Classifies against SILVA 16S ref (vsearch global_search)
  5. Builds phylum_profile + top_genera dicts
  6. Updates communities table: phylum_profile, top_genera, shannon_diversity
  7. Sets runs.t0_pass = 1 for the sample

Requires:
  conda env: bioinfo  (vsearch, fastp, cutadapt)
  SILVA ref: /data/pipeline/ref/SILVA_138.1_SSURef_Nr99_tax_silva.fasta.gz
  Download ref first: bash scripts/download_silva.sh

Usage:
  python scripts/process_neon_16s.py \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --staging /data/pipeline/staging/neon_16s \\
      --silva /data/pipeline/ref/SILVA_138.1_SSURef.fasta \\
      --sites HARV ORNL KONZ \\
      --workers 4

  # Dry run (print actions only):
  python scripts/process_neon_16s.py --dry-run --sites HARV

  # All sites (overnight run):
  python scripts/process_neon_16s.py --all-sites --workers 8
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import log
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

BIOINFO_BIN = "/home/deploy/miniforge3/envs/bioinfo/bin"
VSEARCH    = f"{BIOINFO_BIN}/vsearch"
FASTP      = f"{BIOINFO_BIN}/fastp"
CUTADAPT   = f"{BIOINFO_BIN}/cutadapt"

# 16S primers (515F / 806R — Earth Microbiome Project standard)
PRIMER_FWD = "GTGYCAGCMGCCGCGGTAA"
PRIMER_REV = "GGACTACNVGGGTWTCTAAT"

# vsearch taxonomy parsing
VSEARCH_ID  = 0.97   # OTU identity threshold for SILVA search
TOP_N_GENERA = 10
TOP_N_PHYLA  = 25

# ---------------------------------------------------------------------------
# SILVA taxonomy utilities
# ---------------------------------------------------------------------------

def _parse_silva_taxonomy(taxstr: str) -> tuple[str, str]:
    """
    Parse taxonomy string from NCBI 16S db or SILVA format:
      NCBI: 'Bacteria; Firmicutes; Bacilli; Lactobacillales; ...'
      SILVA: 'Bacteria;Firmicutes;Bacilli;...'
      NCBI defline: 'Bacillus subtilis strain NRRL NRS-744 16S rRNA [...]'
    → (phylum, genus)
    """
    # Handle NCBI lineage format (semicolon+space separated)
    taxstr = taxstr.strip()
    if "; " in taxstr:
        parts = [p.strip() for p in taxstr.split(";") if p.strip()]
    elif ";" in taxstr:
        parts = [p.strip() for p in taxstr.split(";") if p.strip()]
    else:
        # NCBI defline-style: 'Bacillus subtilis strain...' → genus is first word
        words = taxstr.replace("[", "").replace("]", "").split()
        phylum = "Unknown"
        genus = words[0] if words else "Unknown"
        return phylum, genus

    # Skip 'cellular organisms' and 'root' prefixes common in NCBI lineage
    parts = [p for p in parts if p.lower() not in ("cellular organisms", "root", "")]
    domain = parts[0] if parts else "Unknown"
    phylum = parts[1] if len(parts) > 1 else "Unknown"
    genus  = parts[5] if len(parts) > 5 else (parts[-1] if parts else "Unknown")
    # Filter out numeric clade codes like D_0__, D_1__ in older SILVA builds
    if phylum.startswith("D_"):
        phylum = parts[2] if len(parts) > 2 else "Unknown"
    if genus.startswith("D_"):
        genus = parts[-1] if parts else "Unknown"
    return phylum, genus


def _build_profiles(hits_file: Path) -> tuple[dict, dict, float]:
    """
    Read vsearch UC-format output and build taxonomy profiles.
    UC format columns: type, cluster, size, pct_id, strand, ., ., cigar, query, centroid
    Type H=hit, N=no_match.
    Centroid column (index 9) contains the full reference label including taxonomy.
    """
    phylum_counts: dict[str, int] = defaultdict(int)
    genus_counts:  dict[str, int] = defaultdict(int)
    total = 0

    try:
        with hits_file.open() as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")

                if parts[0] == "N":  # no hit
                    total += 1
                    phylum_counts["Unclassified"] += 1
                    continue
                if parts[0] != "H":  # skip non-hit rows (S=seed, C=cluster)
                    continue

                # Column 9 (index 9) = full centroid label: "accession taxonomy"
                target_label = parts[9] if len(parts) > 9 else ""
                # Strip the accession (first word) to get tax string
                label_parts = target_label.split(None, 1)
                taxstr = label_parts[1] if len(label_parts) > 1 else label_parts[0] if label_parts else "Unknown"

                phylum, genus = _parse_silva_taxonomy(taxstr)
                phylum_counts[phylum] += 1
                genus_counts[genus]   += 1
                total += 1
    except Exception as e:
        logger.warning("Could not parse UC hits file %s: %s", hits_file, e)

    if total == 0:
        return {}, {}, 0.0

    phylum_freq = {k: round(v / total, 6)
                   for k, v in sorted(phylum_counts.items(),
                                       key=lambda x: -x[1])[:TOP_N_PHYLA]}
    genus_freq  = {k: round(v / total, 6)
                   for k, v in sorted(genus_counts.items(),
                                       key=lambda x: -x[1])[:TOP_N_GENERA]}

    # Shannon diversity over phyla
    shannon = 0.0
    for p in phylum_freq.values():
        if p > 0:
            shannon -= p * log(p)

    return phylum_freq, genus_freq, round(shannon, 4)


# ---------------------------------------------------------------------------
# Per-sample worker
# ---------------------------------------------------------------------------

def _download_file(url: str, dest: Path) -> bool:
    """Download a file via curl (handles GCS redirect)."""
    if dest.exists() and dest.stat().st_size > 1_000:
        logger.debug("Cache hit: %s", dest.name)
        return True
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            ["curl", "-fsSL", "--retry", "3", "--retry-delay", "5",
             "-o", str(dest), url],
            timeout=600, capture_output=True,
        )
        if result.returncode != 0:
            logger.warning("curl failed for %s: %s", url, result.stderr.decode()[:200])
            return False
        return dest.exists() and dest.stat().st_size > 1_000
    except subprocess.TimeoutExpired:
        logger.warning("Download timeout: %s", url)
        return False


def _run_fastp(r1: Path, r2: Path, out1: Path, out2: Path, workdir: Path) -> bool:
    """Trim adapters and quality filter with fastp."""
    log_path = workdir / "fastp.json"
    cmd = [
        FASTP, "-i", str(r1), "-I", str(r2),
        "-o", str(out1), "-O", str(out2),
        "--json", str(log_path),
        "--disable_length_filtering",
        "--thread", "2",
        "--quiet",
    ]
    try:
        result = subprocess.run(cmd, timeout=300, capture_output=True)
        return result.returncode == 0
    except Exception as e:
        logger.warning("fastp failed: %s", e)
        return False


def _merge_and_classify(
    r1_trimmed: Path,
    r2_trimmed: Path,
    silva_db: str,
    workdir: Path,
    sample_id: str,
) -> Path | None:
    """
    Merge pairs with vsearch, then classify against SILVA.
    Returns path to hits TSV or None.
    """
    merged = workdir / "merged.fasta"
    hits   = workdir / "hits.uc"
    notmerged = workdir / "notmerged.fasta"

    # Step 1: merge paired reads
    cmd_merge = [
        VSEARCH, "--fastq_mergepairs", str(r1_trimmed),
        "--reverse", str(r2_trimmed),
        "--fastaout", str(merged),
        "--fastaout_notmerged_fwd", str(notmerged),
        "--fastq_minovlen", "10",
        "--threads", "4",
        "--quiet",
    ]
    try:
        result = subprocess.run(cmd_merge, timeout=600, capture_output=True)
        if result.returncode != 0 or not merged.exists():
            # Fall back to R1 only
            logger.debug("%s: merge failed, using R1 only", sample_id)
            shutil.copy(str(r1_trimmed), str(merged))
    except Exception as e:
        logger.warning("merge failed: %s", e)
        shutil.copy(str(r1_trimmed), str(merged))

    if not merged.exists() or merged.stat().st_size == 0:
        return None

    # Step 2: classify against 16S reference using UC output format
    # --notrunclabels: use full reference header (includes taxonomy string)
    # --uc: UC-format output includes full centroid label in column 9
    cmd_class = [
        VSEARCH, "--usearch_global", str(merged),
        "--db", silva_db,
        "--id", str(VSEARCH_ID),
        "--uc", str(hits),
        "--notrunclabels",
        "--threads", "4",
        "--quiet",
        "--maxaccepts", "1",
        "--maxrejects", "32",
    ]
    try:
        result = subprocess.run(cmd_class, timeout=1800, capture_output=True)
        if result.returncode == 0 and hits.exists():
            return hits
        logger.warning("%s classification stderr: %s",
                       sample_id, result.stderr.decode()[:300])
        return None
    except subprocess.TimeoutExpired:
        logger.warning("%s: vsearch classification timed out", sample_id)
        return None


def _process_one_sample(
    sample_id: str,
    community_id: int,
    fastq_urls: list[str],
    silva_db: str,
    staging_dir: Path,
    dry_run: bool,
    target_id: str,
) -> dict | None:
    """Full pipeline for one NEON sample. Returns result dict or None on failure."""
    r1_urls = [u for u in fastq_urls if "_R1" in u or ".1.fastq" in u]
    r2_urls = [u for u in fastq_urls if "_R2" in u or ".2.fastq" in u]
    if not r1_urls:
        logger.warning("%s: no R1 URL found in %s", sample_id, fastq_urls)
        return None

    r1_url = r1_urls[0]
    r2_url = r2_urls[0] if r2_urls else None

    workdir = staging_dir / sample_id.replace("/", "_")
    workdir.mkdir(parents=True, exist_ok=True)

    r1_raw = workdir / "R1.fastq.gz"
    r2_raw = workdir / "R2.fastq.gz" if r2_url else None

    if dry_run:
        logger.info("[DRY RUN] %s: would download %s + classify", sample_id, r1_url[:60])
        return {"sample_id": sample_id, "skipped": True}

    # --- Download ---
    logger.info("%s: downloading R1...", sample_id)
    if not _download_file(r1_url, r1_raw):
        logger.error("%s: R1 download failed", sample_id)
        return None

    if r2_url:
        logger.info("%s: downloading R2...", sample_id)
        _download_file(r2_url, r2_raw)

    # --- Trim ---
    r1_trim = workdir / "R1.trimmed.fastq.gz"
    r2_trim = workdir / "R2.trimmed.fastq.gz"
    if r2_raw and r2_raw.exists():
        ok = _run_fastp(r1_raw, r2_raw, r1_trim, r2_trim, workdir)
        if not ok:
            logger.warning("%s: fastp failed, falling back to untrimmed reads", sample_id)
            shutil.copy(str(r1_raw), str(r1_trim))
            shutil.copy(str(r2_raw), str(r2_trim))
    else:
        # single-end fallback
        shutil.copy(str(r1_raw), str(r1_trim))
        r2_trim = r1_trim  # dummy

    # --- Classify ---
    hits_file = _merge_and_classify(r1_trim, r2_trim, silva_db, workdir, sample_id)
    if not hits_file:
        logger.warning("%s: classification failed", sample_id)
        return None

    # --- Parse profiles ---
    phylum_profile, top_genera, shannon = _build_profiles(hits_file)
    if not phylum_profile:
        logger.warning("%s: empty taxonomy result", sample_id)
        return None

    # --- Cleanup raw files (keep hits for audit) ---
    for f in [r1_raw, r2_raw, r1_trim, r2_trim]:
        if f and f.exists() and f != r1_trim:
            try:
                f.unlink()
            except Exception:
                pass

    return {
        "sample_id":      sample_id,
        "community_id":   community_id,
        "phylum_profile": phylum_profile,
        "top_genera":     top_genera,
        "shannon":        shannon,
    }


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _update_community(conn: sqlite3.Connection, result: dict) -> None:
    conn.execute(
        """
        UPDATE communities SET
            phylum_profile    = ?,
            top_genera        = ?,
            shannon_diversity = ?
        WHERE community_id = ?
        """,
        (
            json.dumps(result["phylum_profile"]),
            json.dumps(result["top_genera"]),
            result["shannon"],
            result["community_id"],
        ),
    )
    conn.execute(
        "UPDATE runs SET t0_pass=1 WHERE sample_id=? AND t0_pass=0",
        (result["sample_id"],),
    )
    conn.commit()


def _fetch_pending_samples(
    conn: sqlite3.Connection, sites: list[str] | None
) -> list[tuple]:
    """Return (sample_id, community_id, notes) for pending NEON samples."""
    site_filter = ""
    params: list = ["neon"]
    if sites:
        placeholders = ",".join("?" * len(sites))
        site_filter = f" AND s.site_id IN ({placeholders})"
        params.extend(sites)

    return conn.execute(
        f"""
        SELECT s.sample_id, c.community_id, c.notes
        FROM communities c
        JOIN samples s ON c.sample_id = s.sample_id
        JOIN runs r ON r.sample_id = s.sample_id
        WHERE s.source = ?
          AND (r.t0_pass = 0 OR r.t0_pass IS NULL)
          AND c.notes IS NOT NULL
          AND c.notes != '[]'
          {site_filter}
        ORDER BY s.site_id, s.sample_id
        """,
        params,
    ).fetchall()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db",      default="/data/pipeline/db/soil_microbiome.db")
    p.add_argument("--staging", default="/data/pipeline/staging/neon_16s")
    p.add_argument("--silva",   default="/data/pipeline/ref/16S_ref.fasta",
                   help="16S FASTA reference for vsearch. Build with: bash scripts/download_silva.sh")
    p.add_argument("--sites",   nargs="*",
                   default=["HARV","ORNL","KONZ","WOOD","CPER","UNDE","STEI",
                             "DCFS","NOGP","CLBJ","OAES"],
                   help="NEON site codes to process (default: 11 key sites).")
    p.add_argument("--all-sites", action="store_true",
                   help="Process all NEON sites in the DB.")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--target-id", default="carbon_sequestration")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Tool checks
    for tool in [VSEARCH, FASTP]:
        if not Path(tool).exists():
            logger.error("Required tool not found: %s", tool)
            logger.error("Run: conda install -n bioinfo -c bioconda vsearch fastp")
            sys.exit(1)

    silva_db = args.silva
    if not Path(silva_db).exists():
        logger.error("SILVA reference not found: %s", silva_db)
        logger.error("Run: bash scripts/download_silva.sh")
        sys.exit(1)

    staging = Path(args.staging)
    staging.mkdir(parents=True, exist_ok=True)

    conn = _db_connect(args.db)
    sites = None if args.all_sites else args.sites
    pending = _fetch_pending_samples(conn, sites)

    if not pending:
        logger.info("No pending NEON samples with FASTQ URLs.")
        return

    logger.info("Processing %d NEON samples (workers=%d)", len(pending), args.workers)

    n_ok = n_fail = n_skip = 0

    def _worker(row):
        sample_id, community_id, notes_raw = row
        try:
            notes = json.loads(notes_raw) if notes_raw else {}
        except Exception:
            return None
        # notes can be list (old) or dict {"fastq_urls": [...]}
        if isinstance(notes, list):
            urls = notes
        else:
            urls = notes.get("fastq_urls", [])
        if not urls:
            return None
        return _process_one_sample(
            sample_id, community_id, urls,
            silva_db, staging, args.dry_run, args.target_id,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_worker, row): row[0] for row in pending}
        for fut in as_completed(futures):
            sid = futures[fut]
            try:
                result = fut.result()
            except Exception as exc:
                logger.error("%s: unexpected error: %s", sid, exc)
                n_fail += 1
                continue

            if result is None:
                n_fail += 1
                continue
            if result.get("skipped"):
                n_skip += 1
                continue

            if not args.dry_run:
                try:
                    _update_community(conn, result)
                    n_ok += 1
                    logger.info("✓ %s  phyla=%d  shannon=%.2f",
                               sid, len(result["phylum_profile"]), result["shannon"])
                except Exception as exc:
                    logger.error("%s: DB update failed: %s", sid, exc)
                    n_fail += 1

    print("\n=== NEON 16S processing complete ===")
    print(f"  Processed OK  : {n_ok}")
    print(f"  Failed        : {n_fail}")
    print(f"  Dry-run skip  : {n_skip}")


if __name__ == "__main__":
    main()
