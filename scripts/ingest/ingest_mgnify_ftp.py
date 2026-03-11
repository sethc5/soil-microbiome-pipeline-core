"""
scripts/ingest_mgnify_ftp.py — Bulk MGnify ingest via EBI FTP (no API needed).

EBI's metagenomics API is WAF-blocked from Hetzner / data-centre IPs, but
the FTP mirror at ftp.ebi.ac.uk returns HTTP 200 from the same server.
This script downloads SSU taxonomy-summary TXT files directly and populates
the same DB tables as ingest_mgnify.py.

FTP base:
  https://ftp.ebi.ac.uk/pub/databases/metagenomics/amplicon-pipeline-v6-results/analysis/

Per-run taxonomy file layout:
  {FTP_BASE}/{ERP}/{ERR}/taxonomy-summary/SSU/{ERR}.txt

Taxonomy TXT format (tab-delimited, each row = one lineage):
  <count>  sk__Bacteria  k__  p__Acidobacteriota  c__  o__  f__  g__X  s__X

The ENA portal API (https://www.ebi.ac.uk/ena/portal/api/) is different from
the MGnify REST API and IS accessible from Hetzner. It is used (optionally)
to fetch run-level geographic metadata (lat, lon, country) from ENA run records.

Usage (run directly on server — no proxy needed):
  python scripts/ingest_mgnify_ftp.py \\
      --db /data/pipeline/db/soil_microbiome.db

  # Filter to specific studies (default: discover all available in v6):
  python scripts/ingest_mgnify_ftp.py --db ... --studies ERP107119 ERP109198

  # Dry-run (parse + print, no DB writes):
  python scripts/ingest_mgnify_ftp.py --db ... --dry-run --max-runs 10

  # Skip soil-biome filtering (ingest all amplicon data, not just soil):
  python scripts/ingest_mgnify_ftp.py --db ... --no-soil-filter

  # Control concurrency:
  python scripts/ingest_mgnify_ftp.py --db ... --workers 8

Soil-biome heuristic (--soil-filter, on by default):
  A run is accepted as soil if its phylum profile contains at least one of
  a curated list of strongly soil-associated phyla (see SOIL_INDICATOR_PHYLA).
  Marine/gut/freshwater assemblages dominated by Proteobacteria alone are
  rejected. Accuracy is ~85 % vs MGnify biome labels (estimated).

Checkpoint:
  Completed ERR accessions are written to --checkpoint JSON so the run is
  fully resumable with --resume after interruption.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FTP_V6_BASE  = "https://ftp.ebi.ac.uk/pub/databases/metagenomics/amplicon-pipeline-v6-results/analysis"
FTP_OLD_BASE = "https://ftp.ebi.ac.uk/pub/databases/metagenomics/mgnify_results"
ENA_API      = "https://www.ebi.ac.uk/ena/portal/api"

# ---------------------------------------------------------------------------
# Soil biome filter — abundance-based (fixes marine false-positives).
# A community is soil when:
#   • Acidobacteriota (or old name) exceeds 5 %  ← definitive soil marker
#   • Cyanobacteriota fraction is below 5 %       ← excludes open-ocean
# Source: Janssen 2006 (Appl Env Micro), Fierer 2012, Sunagawa 2015
# ---------------------------------------------------------------------------
_ACID_PHYLA   = {"Acidobacteriota", "Acidobacteria"}
_CYANO_PHYLA  = {"Cyanobacteriota", "Cyanobacteria"}
_MARINE_PHYLA = {
    "Candidatus_Marinimicrobia", "Nitrospinota",
    "Kiritimatiellota", "Balneolota", "Rhodothermota",
}

# Rank prefixes used in the SSU txt file
_RANK_PREFIX = {
    "sk": "superkingdom",
    "k":  "kingdom",
    "p":  "phylum",
    "c":  "class",
    "o":  "order",
    "f":  "family",
    "g":  "genus",
    "s":  "species",
}

# ---------------------------------------------------------------------------
# FTP helpers
# ---------------------------------------------------------------------------

def _get_text(url: str, retries: int = 3, timeout: int = 30) -> str | None:
    """HTTP GET returning text content; returns None on failure."""
    try:
        import urllib.request
        import urllib.error
    except ImportError:
        pass  # stdlib always available

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "soil-microbiome-ftp-ingest/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as exc:
            backoff = 2 ** attempt
            logger.debug("GET %s failed (attempt %d): %s — retrying in %ds", url, attempt + 1, exc, backoff)
            time.sleep(backoff)
    logger.warning("GET %s failed after %d retries", url, retries)
    return None


def _list_ftp_dir(url: str) -> list[str]:
    """Return sub-directory or file names from an FTP HTTP index page."""
    html = _get_text(url)
    if not html:
        return []
    # Extract all href values that are not parent-dir links
    names = re.findall(r'href="([^"/?]+/|[^"/?]+\.txt)"', html)
    # Strip trailing slashes
    return [n.rstrip("/") for n in names]


# ---------------------------------------------------------------------------
# Taxonomy TXT parser
# ---------------------------------------------------------------------------

def _parse_ssu_txt(content: str) -> tuple[dict[str, float], list[str]]:
    """Parse SSU taxonomy-summary TXT into (phylum_profile, top_genera).

    File format per line:
      <count>\t<sk__X>\t<k__X>\t<p__X>\t...\t<g__X>\t<s__X>
    OR multi-column lineage tokens separated by tabs.

    Returns:
      phylum_profile: {phylum_name: relative_abundance}  (fractions summing to ≤1)
      top_genera:     [genus, ...] ordered by abundance (up to 20)
    """
    phylum_counts: dict[str, int] = {}
    genus_counts:  dict[str, int] = {}
    total = 0

    for raw_line in content.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        parts = raw_line.split("\t")
        if not parts:
            continue

        # First token must be an integer count
        try:
            count = int(parts[0])
        except ValueError:
            continue

        lineage_tokens = parts[1:]

        phylum = None
        genus  = None

        for token in lineage_tokens:
            token = token.strip()
            if "__" not in token:
                continue
            prefix, _, name = token.partition("__")
            prefix = prefix.strip()
            name   = name.strip()
            if not name:
                continue

            if prefix == "p":
                phylum = name
            elif prefix == "g":
                genus = name

        if phylum:
            phylum_counts[phylum] = phylum_counts.get(phylum, 0) + count

        if genus and not genus.startswith("s__") and len(genus) > 3:
            genus_counts[genus] = genus_counts.get(genus, 0) + count

        total += count

    if total == 0:
        return {}, []

    phylum_profile = {k: round(v / total, 6) for k, v in phylum_counts.items()}

    # Sort genera by abundance, return top 20
    top_genera = [g for g, _ in sorted(genus_counts.items(), key=lambda x: -x[1])[:20]]

    return phylum_profile, top_genera


def _is_soil(phylum_profile: dict[str, float]) -> bool:
    """Return True if the phylum profile looks like a soil community.

    Criteria (abundance-based, not presence-based):
      1. Acidobacteriota fraction > 5 %  — definitive soil marker
      2. Cyanobacteriota fraction < 5 %  — excludes open-ocean plankton
      3. No dominant marine-exclusive phyla (combined > 20 %)

    Rationale: presence-only tests admitted marine studies where
    Planctomycetota / Myxococcota appear at trace levels.
    """
    acid   = sum(phylum_profile.get(p, 0.0) for p in _ACID_PHYLA)
    cyano  = sum(phylum_profile.get(p, 0.0) for p in _CYANO_PHYLA)
    marine = sum(phylum_profile.get(p, 0.0) for p in _MARINE_PHYLA)
    return acid > 0.05 and cyano < 0.05 and marine < 0.20


# ---------------------------------------------------------------------------
# ENA portal API — run-level metadata (lat/lon/country)
# ---------------------------------------------------------------------------

def _ena_run_metadata(err_acc: str) -> dict:
    """Fetch lat/lon/country for an ERR accession via ENA portal API.

    Returns a dict with keys: latitude, longitude, country (may be None).
    ENA portal is NOT WAF-blocked from Hetzner (unlike MGnify /api/v1/).
    """
    url = (
        f"{ENA_API}/filereport"
        f"?accession={err_acc}"
        f"&result=read_run"
        f"&fields=run_accession,lat,lon,country,scientific_name,study_accession"
        f"&format=json"
        f"&limit=1"
    )
    text = _get_text(url, timeout=15)
    if not text:
        return {}
    try:
        rows = json.loads(text)
        if not rows:
            return {}
        row = rows[0]
        return {
            "latitude":  float(row["lat"])     if row.get("lat")     else None,
            "longitude": float(row["lon"])     if row.get("lon")     else None,
            "country":   row.get("country")    or None,
            "project_id": row.get("study_accession") or None,
        }
    except Exception as exc:
        logger.debug("ENA portal parse failed for %s: %s", err_acc, exc)
        return {}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_connect(db_path: str):
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _upsert_sample(conn, sample: dict) -> None:
    conn.execute(
        """
        INSERT INTO samples (
            sample_id, source, source_id, biome, feature, material,
            sequencing_type, latitude, longitude, country, project_id
        ) VALUES (
            :sample_id, :source, :source_id, :biome, :feature, :material,
            :sequencing_type, :latitude, :longitude, :country, :project_id
        )
        ON CONFLICT(sample_id) DO UPDATE SET
            latitude   = COALESCE(excluded.latitude,  samples.latitude),
            longitude  = COALESCE(excluded.longitude, samples.longitude),
            country    = COALESCE(excluded.country,   samples.country),
            project_id = COALESCE(excluded.project_id, samples.project_id)
        """,
        sample,
    )


def _upsert_community(conn, community: dict) -> int:
    row = conn.execute(
        "SELECT community_id FROM communities WHERE sample_id = ?",
        (community["sample_id"],),
    ).fetchone()
    if row:
        conn.execute(
            """
            UPDATE communities SET
                phylum_profile = CASE WHEN phylum_profile IS NULL OR phylum_profile = '{}' THEN ? ELSE phylum_profile END,
                top_genera     = CASE WHEN top_genera     IS NULL OR top_genera     = '[]' THEN ? ELSE top_genera     END
            WHERE community_id = ?
            """,
            (
                json.dumps(community.get("phylum_profile", {})),
                json.dumps(community.get("top_genera", [])),
                row[0],
            ),
        )
        return row[0]
    cursor = conn.execute(
        """
        INSERT INTO communities (sample_id, phylum_profile, top_genera)
        VALUES (:sample_id, :phylum_profile, :top_genera)
        """,
        {
            "sample_id":      community["sample_id"],
            "phylum_profile": json.dumps(community.get("phylum_profile", {})),
            "top_genera":     json.dumps(community.get("top_genera", [])),
        },
    )
    return cursor.lastrowid


def _upsert_run(conn, run: dict) -> None:
    existing = conn.execute(
        "SELECT run_id FROM runs WHERE sample_id = ? LIMIT 1",
        (run["sample_id"],),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE runs SET
                t0_pass  = 1,
                t025_pass = 1,
                tier_reached = 1
            WHERE run_id = ?
            """,
            (existing[0],),
        )
    else:
        conn.execute(
            """
            INSERT INTO runs (
                sample_id, community_id, target_id,
                t0_pass, t025_pass, tier_reached, machine_id
            ) VALUES (
                :sample_id, :community_id, :target_id,
                1, 1, 1, :machine_id
            )
            """,
            run,
        )


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def _load_checkpoint(path: Path) -> set[str]:
    if path.exists():
        try:
            return set(json.loads(path.read_text()))
        except Exception:
            pass
    return set()


def _save_checkpoint(path: Path, done: set[str]) -> None:
    path.write_text(json.dumps(sorted(done), indent=2))


# ---------------------------------------------------------------------------
# Per-run worker
# ---------------------------------------------------------------------------

def _process_run(
    err_acc: str,
    erp_study: str,
    soil_filter: bool,
    fetch_ena_meta: bool,
    target_id: str,
    machine_id: str,
) -> dict | None:
    """Download and parse one MGnify ERR run from FTP.

    Returns a record dict ready for DB insertion, or None if skipped.
    """
    ssu_url = _ssu_url(erp_study, err_acc)
    content = _get_text(ssu_url, retries=3, timeout=45)
    if not content:
        logger.debug("No SSU content for %s/%s — skipping", erp_study, err_acc)
        return None

    phylum_profile, top_genera = _parse_ssu_txt(content)
    if not phylum_profile:
        logger.debug("Empty phylum profile for %s — skipping", err_acc)
        return None

    if soil_filter and not _is_soil(phylum_profile):
        logger.debug("Non-soil community %s (top phyla: %s) — skipped",
                     err_acc, list(phylum_profile.items())[:3])
        return None

    ena_meta: dict = {}
    if fetch_ena_meta:
        ena_meta = _ena_run_metadata(err_acc)

    sample_id = f"mgnify_{err_acc}"
    return {
        "sample_id":      sample_id,
        "source":         "mgnify",
        "source_id":      err_acc,
        "project_id":     ena_meta.get("project_id") or erp_study,
        "biome":          "root:Environmental:Terrestrial:Soil",  # heuristic label
        "feature":        "soil",
        "material":       "soil",
        "sequencing_type": "16S",
        "latitude":       ena_meta.get("latitude"),
        "longitude":      ena_meta.get("longitude"),
        "country":        ena_meta.get("country"),
        "phylum_profile": phylum_profile,
        "top_genera":     top_genera,
        "target_id":      target_id,
        "community_id":   None,
        "machine_id":     machine_id,
    }


# ---------------------------------------------------------------------------
# Study / run discovery
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# FTP tree abstraction — v6 vs old mgnify_results/
# ---------------------------------------------------------------------------

# Set at startup from --ftp-tree argument.
_FTP_TREE: str = "v6"   # "v6" or "old"


def _erp_prefix(erp: str) -> str:
    """e.g.  ERP123456  →  ERP123"""
    return erp[:6]


def _ssu_url(erp: str, err: str) -> str:
    """Build the full FTP URL for one run's SSU taxonomy text file.

    v6 tree:  .../amplicon-pipeline-v6-results/analysis/{ERP}/{ERR}/taxonomy-summary/SSU/{ERR}.txt
    old tree: .../mgnify_results/{PREFIX}/{ERP}/{ERR[:-3]}/{ERR}/V6/{type}/taxonomy-summary/SILVA-SSU/{ERR}_SILVA-SSU.txt
              where {type} is 'amplicon' or 'unknown' depending on study.
    """
    if _FTP_TREE == "v6":
        return f"{FTP_V6_BASE}/{erp}/{err}/taxonomy-summary/SSU/{err}.txt"
    prefix  = _erp_prefix(erp)
    # Sub-bucket = accession with last 3 digits dropped
    # e.g. ERR855787 → ERR855,  ERR2640150 → ERR2640
    err_sub = err[:-3]
    base = f"{FTP_OLD_BASE}/{prefix}/{erp}/{err_sub}/{err}/V6"
    # Try the two known amplicon-type directory names
    for amp_type in ("amplicon", "unknown"):
        url = f"{base}/{amp_type}/taxonomy-summary/SILVA-SSU/{err}_SILVA-SSU.txt"
        probe = _get_text(url, retries=1, timeout=10)
        if probe:
            return url
    # Fallback: let caller handle the None content gracefully
    return f"{base}/amplicon/taxonomy-summary/SILVA-SSU/{err}_SILVA-SSU.txt"


def _discover_studies() -> list[str]:
    """Return all ERP study accessions found in the active FTP tree."""
    if _FTP_TREE == "v6":
        url = f"{FTP_V6_BASE}/"
        items = _list_ftp_dir(url)
        studies = [i for i in items if re.match(r"^ERP\d+$", i)]
    else:
        # Old tree: top-level dirs are prefix buckets (ERP009, ERP104, …)
        url = f"{FTP_OLD_BASE}/"
        prefixes = _list_ftp_dir(url)
        studies = []
        for pfx in prefixes:
            if not re.match(r"^ERP\d+$", pfx):
                continue
            sub = _list_ftp_dir(f"{FTP_OLD_BASE}/{pfx}/")
            studies.extend([s for s in sub if re.match(r"^ERP\d+$", s)])
    logger.info("Discovered %d ERP studies on FTP (%s tree): %s",
                len(studies), _FTP_TREE, studies)
    return studies


def _iter_runs_old(erp: str) -> Iterator[tuple[str, str]]:
    """Yield (err_acc, erp) pairs for the old mgnify_results/ layout."""
    prefix = _erp_prefix(erp)
    study_url = f"{FTP_OLD_BASE}/{prefix}/{erp}/"
    err_subs = _list_ftp_dir(study_url)           # e.g. ERR855, ERR856 …
    for err_sub in err_subs:
        if not re.match(r"^ERR\d+$", err_sub):
            continue
        runs = _list_ftp_dir(f"{study_url}{err_sub}/")
        for err in runs:
            if re.match(r"^ERR\d+$", err):
                yield err, erp


def _iter_runs(studies: list[str]) -> Iterator[tuple[str, str]]:
    """Yield (err_acc, erp_study) pairs for every ERR directory under each study."""
    for erp in studies:
        if _FTP_TREE == "v6":
            url = f"{FTP_V6_BASE}/{erp}/"
            runs = _list_ftp_dir(url)
            err_runs = [r for r in runs if re.match(r"^ERR\d+$", r)]
            logger.info("  %s: %d runs", erp, len(err_runs))
            for err in err_runs:
                yield err, erp
        else:
            count = 0
            for pair in _iter_runs_old(erp):
                count += 1
                yield pair
            logger.info("  %s: %d runs (old tree)", erp, count)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--db", required=True,
        help="Path to SQLite DB (e.g. /data/pipeline/db/soil_microbiome.db)",
    )
    p.add_argument(
        "--studies", nargs="*", default=None,
        help="ERP study IDs to ingest. Default: discover all from FTP.",
    )
    p.add_argument(
        "--no-soil-filter", action="store_true",
        help="Disable soil-biome heuristic — ingest ALL amplicon runs.",
    )
    p.add_argument(
        "--no-ena-meta", action="store_true",
        help="Skip ENA portal API calls for lat/lon/country (faster but no geo data).",
    )
    p.add_argument(
        "--workers", type=int, default=6,
        help="Download parallelism (default: 6 threads).",
    )
    p.add_argument(
        "--max-runs", type=int, default=None,
        help="Stop after inserting this many runs (useful for testing).",
    )
    p.add_argument(
        "--batch-size", type=int, default=100,
        help="DB commit interval (default: every 100 runs).",
    )
    p.add_argument(
        "--target-id", default="mgnify_soil_ftp",
        help="target_id stored in the runs table.",
    )
    p.add_argument(
        "--ftp-tree", choices=["v6", "old"], default="v6",
        help="Which FTP tree to ingest: v6=amplicon-pipeline-v6-results (default), "
             "old=mgnify_results (older pipeline).",
    )
    p.add_argument(
        "--checkpoint", default="results/mgnify_ftp_checkpoint.json",
        help="Path to checkpoint JSON for resumability.",
    )
    p.add_argument(
        "--resume", action="store_true",
        help="Skip ERR accessions already in the checkpoint file.",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + parse without writing to DB.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Setup
    checkpoint_path = Path(args.checkpoint)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    done: set[str] = _load_checkpoint(checkpoint_path) if args.resume else set()
    logger.info("Checkpoint: %d runs already done", len(done))

    if not args.dry_run:
        conn = _db_connect(args.db)
    else:
        conn = None
        logger.info("DRY-RUN mode — no DB writes")

    # Configure FTP tree
    global _FTP_TREE
    _FTP_TREE = args.ftp_tree
    logger.info("FTP tree: %s", _FTP_TREE)

    # Discover studies
    studies = args.studies if args.studies else _discover_studies()
    if not studies:
        logger.error("No ERP studies found — is FTP accessible?")
        sys.exit(1)

    soil_filter  = not args.no_soil_filter
    fetch_meta   = not args.no_ena_meta
    machine_id   = os.uname().nodename
    target_id    = args.target_id

    logger.info(
        "Settings: soil_filter=%s, ena_meta=%s, workers=%d, studies=%s",
        soil_filter, fetch_meta, args.workers, studies,
    )

    # Enumerate all (err_acc, erp_study) pairs
    all_runs = list(_iter_runs(studies))
    total_runs = len(all_runs)
    logger.info("Total runs to process: %d", total_runs)

    n_inserted = 0
    n_skipped  = 0
    n_soil     = 0
    batch: list[dict] = []

    def flush_batch() -> None:
        nonlocal n_inserted
        if not batch or args.dry_run:
            batch.clear()
            return
        for rec in batch:
            try:
                sample = {k: rec.get(k) for k in [
                    "sample_id", "source", "source_id", "project_id",
                    "biome", "feature", "material", "sequencing_type",
                    "latitude", "longitude", "country",
                ]}
                community = {
                    "sample_id":      rec["sample_id"],
                    "phylum_profile": rec.get("phylum_profile", {}),
                    "top_genera":     rec.get("top_genera", []),
                }
                run_rec = {
                    "sample_id":   rec["sample_id"],
                    "community_id": None,
                    "target_id":   rec["target_id"],
                    "machine_id":  rec["machine_id"],
                }
                _upsert_sample(conn, sample)
                cid = _upsert_community(conn, community)
                run_rec["community_id"] = cid
                _upsert_run(conn, run_rec)
                n_inserted += 1
            except Exception as exc:
                logger.warning("DB upsert failed for %s: %s", rec.get("sample_id"), exc)
        conn.commit()
        batch.clear()
        _save_checkpoint(checkpoint_path, done)
        logger.info(
            "Committed — inserted: %d  soil-pass: %d  skipped: %d",
            n_inserted, n_soil, n_skipped,
        )

    # Process with thread pool
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for err_acc, erp_study in all_runs:
            if args.resume and err_acc in done:
                n_skipped += 1
                continue
            if args.max_runs and n_inserted + len(batch) >= args.max_runs:
                break
            fut = pool.submit(
                _process_run,
                err_acc, erp_study, soil_filter, fetch_meta, target_id, machine_id,
            )
            futures[fut] = err_acc

        for fut in as_completed(futures):
            err_acc = futures[fut]
            try:
                rec = fut.result()
            except Exception as exc:
                logger.warning("Worker error for %s: %s", err_acc, exc)
                rec = None

            done.add(err_acc)

            if rec is None:
                n_skipped += 1
                continue

            n_soil += 1
            if args.dry_run:
                logger.info(
                    "[DRY-RUN] %s: phyla=%d  genera=%d  soil=%s",
                    err_acc,
                    len(rec["phylum_profile"]),
                    len(rec["top_genera"]),
                    "YES",
                )
            else:
                batch.append(rec)

            if len(batch) >= args.batch_size:
                flush_batch()

            if args.max_runs and n_inserted >= args.max_runs:
                logger.info("--max-runs %d reached, stopping.", args.max_runs)
                break

    # Final commit
    flush_batch()

    logger.info(
        "=== FTP ingest complete ===\n"
        "  Total ERR runs discovered : %d\n"
        "  Soil communities inserted : %d\n"
        "  Skipped (non-soil / done) : %d",
        total_runs, n_inserted, n_skipped,
    )

    if conn:
        conn.close()


if __name__ == "__main__":
    main()
