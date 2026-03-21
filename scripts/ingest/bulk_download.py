#!/usr/bin/env python3
"""Bulk download NEON 16S FASTQ files with partial download support.

Downloads only the first ~5MB of each R1 FASTQ (enough for 10K reads)
using HTTP Range headers. Saves to staging directory for later processing
by process_neon_16s.py --from-local-dir.

Usage:
    python scripts/ingest/bulk_download.py \
        --db /data/pipeline/db/soil_microbiome.db \
        --staging /data/staging/neon_fastq \
        --workers 50 \
        --max-bytes 5242880
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_MAX_BYTES = 5 * 1024 * 1024  # 5 MB — enough for ~10K reads


def _prefer_amplicon_r1(fastq_urls: list[str]) -> str | None:
    """Pick the best R1 URL from a list of FASTQ URLs."""
    for u in fastq_urls:
        if "_R1_" in u or "_R1." in u or ".1.fastq" in u:
            return u
    # Fallback: first URL that isn't obviously shotgun
    for u in fastq_urls:
        if "JGI" not in u and "shotgun" not in u.lower():
            return u
    return fastq_urls[0] if fastq_urls else None


def _download_partial(url: str, dest: Path, max_bytes: int, timeout: int = 300) -> bool:
    """Download first max_bytes of a URL using Range header."""
    if dest.exists() and dest.stat().st_size > 1000:
        return True  # cache hit

    dest.parent.mkdir(parents=True, exist_ok=True)

    # Use curl with Range header for partial download
    cmd = [
        "curl", "-fsSL",
        "--retry", "3", "--retry-delay", "5",
        "-r", f"0-{max_bytes - 1}",
        "-o", str(dest),
        url,
    ]
    try:
        result = subprocess.run(cmd, timeout=timeout, capture_output=True)
        if result.returncode != 0:
            # Range might not be supported — fall back to full download
            logger.debug("Range not supported for %s, trying full download", url[:60])
            cmd_full = ["curl", "-fsSL", "--retry", "3", "--retry-delay", "5",
                        "-o", str(dest), url]
            result = subprocess.run(cmd_full, timeout=timeout, capture_output=True)
            if result.returncode != 0:
                logger.warning("curl failed for %s: %s", url, result.stderr.decode()[:200])
                return False
        return dest.exists() and dest.stat().st_size > 100
    except subprocess.TimeoutExpired:
        logger.warning("Download timeout: %s", url)
        return False


def _fetch_pending(db_path: str) -> list[tuple[str, int, list[str]]]:
    """Fetch samples that need OTU profiles and have fastq_urls."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT c.sample_id, c.community_id, c.notes
        FROM communities c
        WHERE (c.otu_profile IS NULL OR c.otu_profile = '{}')
        AND c.notes IS NOT NULL
        AND c.notes LIKE '%fastq%'
    """).fetchall()
    conn.close()

    pending = []
    for row in rows:
        notes = json.loads(row["notes"])
        if isinstance(notes, list):
            urls = notes
        else:
            urls = notes.get("fastq_urls", [])
        if not urls:
            continue
        r1 = _prefer_amplicon_r1(urls)
        if r1:
            pending.append((row["sample_id"], row["community_id"], r1))

    return pending


def main():
    parser = argparse.ArgumentParser(description="Bulk download NEON 16S FASTQ files")
    parser.add_argument("--db", default="/data/pipeline/db/soil_microbiome.db")
    parser.add_argument("--staging", default="/data/staging/neon_fastq",
                        help="Directory to save downloaded FASTQs")
    parser.add_argument("--workers", type=int, default=50,
                        help="Parallel download workers")
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES,
                        help="Max bytes to download per file (default: 5MB)")
    parser.add_argument("--timeout", type=int, default=300,
                        help="Per-file download timeout in seconds")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manifest", default=None,
                        help="Write manifest CSV (sample_id, fastq_path) for process_neon_16s.py")
    args = parser.parse_args()

    pending = _fetch_pending(args.db)
    logger.info("Found %d samples needing download", len(pending))

    if not pending:
        logger.info("Nothing to download")
        return 0

    staging = Path(args.staging)
    staging.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for sid, _, url in pending[:10]:
            logger.info("[DRY RUN] %s: %s", sid, url[:80])
        logger.info("[DRY RUN] ... and %d more", max(0, len(pending) - 10))
        return 0

    n_ok = 0
    n_fail = 0
    manifest_rows = []

    def _worker(item: tuple[str, int, str]) -> tuple[str, int, bool]:
        sid, cid, url = item
        dest = staging / f"{sid}.R1.fastq.gz"
        ok = _download_partial(url, dest, args.max_bytes, args.timeout)
        return sid, cid, ok

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_worker, item): item[0] for item in pending}
        for i, fut in enumerate(as_completed(futures)):
            sid = futures[fut]
            try:
                _, cid, ok = fut.result()
            except Exception as exc:
                logger.error("%s: %s", sid, exc)
                ok = False
                cid = None

            if ok:
                n_ok += 1
                fastq_path = str(staging / f"{sid}.R1.fastq.gz")
                manifest_rows.append((sid, cid, fastq_path))
            else:
                n_fail += 1

            if (n_ok + n_fail) % 100 == 0:
                logger.info("Progress: %d/%d (ok=%d, fail=%d)",
                            n_ok + n_fail, len(pending), n_ok, n_fail)

    logger.info("Download complete: %d ok, %d fail out of %d", n_ok, n_fail, len(pending))

    # Write manifest for process_neon_16s.py --from-manifest
    if args.manifest and manifest_rows:
        manifest_path = Path(args.manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        with manifest_path.open("w") as f:
            f.write("sample_id\tcommunity_id\tfastq_path\n")
            for sid, cid, fpath in manifest_rows:
                f.write(f"{sid}\t{cid}\t{fpath}\n")
        logger.info("Manifest written: %s (%d entries)", manifest_path, len(manifest_rows))

    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())