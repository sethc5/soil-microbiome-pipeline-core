"""
local_biom_adapter.py — Local BIOM / FASTA / FASTQ ingestion adapter.

For privately held datasets, in-house sequencing runs, or any case where
data is already present on disk rather than sourced from a public database.

Accepts:
  - BIOM format OTU/ASV tables (biom-format)
  - Raw FASTQ (paired-end or single-end)
  - Pre-computed taxonomy TSV files

Usage:
  adapter = LocalBIOMAdapter(config)
  for sample in adapter.from_biom("data/my_study.biom", metadata_csv="data/metadata.csv"):
      yield sample
"""

from __future__ import annotations
import csv
import logging
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


def _safe_float(val: str | None) -> float | None:
    try:
        return float(val) if val not in (None, "", "nan", "NA", "N/A") else None
    except (ValueError, TypeError):
        return None


class LocalBIOMAdapter:
    """Adapter for locally stored BIOM tables, FASTA, or FASTQ data."""
    SOURCE = "local"

    def __init__(self, config: dict):
        self.config = config
        self._metadata_fields = config.get("metadata_fields", [
            "ph", "temperature", "latitude", "longitude",
            "collection_date", "country", "description",
        ])

    def _load_metadata_csv(self, metadata_csv: str | None) -> dict[str, dict]:
        """Parse metadata CSV/TSV into {sample_id: attrs} dict."""
        if not metadata_csv:
            return {}
        path = Path(metadata_csv)
        if not path.exists():
            logger.warning("Metadata file not found: %s", metadata_csv)
            return {}

        sep = "\t" if path.suffix.lower() in (".tsv", ".txt") else ","
        meta: dict[str, dict] = {}
        with open(path, "r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter=sep)
            id_col = None
            for row in reader:
                if id_col is None:
                    for col in ("sample_id", "#SampleID", "SampleID", "sample_name"):
                        if col in row:
                            id_col = col
                            break
                    if id_col is None:
                        id_col = reader.fieldnames[0] if reader.fieldnames else "sample_id"
                sid = row.get(id_col, "").strip()
                meta[sid] = dict(row)
        return meta

    def from_biom(self, biom_path: str, metadata_csv: str | None = None) -> Iterator[dict]:
        """Yield sample metadata dicts from a local BIOM table.

        Requires biom-format; falls back to logging a warning if not installed.
        """
        path = Path(biom_path)
        if not path.exists():
            logger.error("BIOM file not found: %s", biom_path)
            return

        metadata_lookup = self._load_metadata_csv(metadata_csv)

        try:
            import biom
            table = biom.load_table(str(path))
            sample_ids = list(table.ids(axis="sample"))
        except ImportError:
            logger.warning(
                "biom-format not installed; cannot parse sample IDs from BIOM. "
                "Install with: pip install biom-format"
            )
            # Yield from metadata CSV only if available
            for sid, attrs in metadata_lookup.items():
                record = {"sample_id": sid, "source": "local", "biom_path": biom_path}
                record.update(attrs)
                yield record
            return

        for sid in sample_ids:
            meta_attrs = metadata_lookup.get(sid, {})
            record = {
                "sample_id": sid,
                "source": "local",
                "biom_path": biom_path,
                # Pull standard fields, falling back to None
                "ph": _safe_float(meta_attrs.get("ph")),
                "temperature": _safe_float(meta_attrs.get("temperature")),
                "latitude": _safe_float(meta_attrs.get("latitude")),
                "longitude": _safe_float(meta_attrs.get("longitude")),
                "collection_date": meta_attrs.get("collection_date"),
                "country": meta_attrs.get("country"),
                "description": meta_attrs.get("description"),
            }
            record.update(meta_attrs)  # include all extra metadata fields
            yield record

    def from_fastq(
        self,
        fastq_dir: str,
        metadata_csv: str | None = None,
        pattern: str = "*.fastq{,.gz}",
    ) -> Iterator[dict]:
        """Yield sample dicts from a directory of FASTQ files.

        Each dict points to the FASTQ file path for downstream processing.
        """
        fastq_dir_path = Path(fastq_dir)
        if not fastq_dir_path.is_dir():
            logger.error("FASTQ directory not found: %s", fastq_dir)
            return

        metadata_lookup = self._load_metadata_csv(metadata_csv)

        # Collect FASTQ files — support .fastq and .fastq.gz
        fastq_files: list[Path] = sorted([
            *fastq_dir_path.glob("*.fastq"),
            *fastq_dir_path.glob("*.fastq.gz"),
            *fastq_dir_path.glob("*.fq"),
            *fastq_dir_path.glob("*.fq.gz"),
        ])

        seen_samples: set[str] = set()
        for fq in fastq_files:
            # Derive sample_id: strip _R1/_R2/_1/_2 suffixes using endswith (not rstrip)
            stem = fq.name
            for ext in (".fastq.gz", ".fastq", ".fq.gz", ".fq"):
                if stem.endswith(ext):
                    stem = stem[: -len(ext)]
                    break

            # Skip R2/second-read files — they'll be picked up as paired below
            is_r2 = False
            for r2_sfx in ("_R2", ".R2", "_2"):
                if stem.endswith(r2_sfx):
                    is_r2 = True
                    break
            if is_r2:
                continue

            # Strip R1/first-read suffix if present
            sample_id = stem
            for r1_sfx in ("_R1", ".R1", "_1"):
                if sample_id.endswith(r1_sfx):
                    sample_id = sample_id[: -len(r1_sfx)]
                    break

            if sample_id in seen_samples:
                continue
            seen_samples.add(sample_id)

            meta_attrs = metadata_lookup.get(sample_id, {})
            # Try to find paired R2 file
            r2_candidates = [
                fastq_dir_path / fq.name.replace("_R1", "_R2"),
                fastq_dir_path / fq.name.replace("_1.", "_2."),
            ]
            r2 = next((p for p in r2_candidates if p.exists() and p != fq), None)
            record = {
                "sample_id": sample_id,
                "source": "local",
                "fastq_r1": str(fq),
                "fastq_r2": str(r2) if r2 is not None else None,
                "ph": _safe_float(meta_attrs.get("ph")),
                "temperature": _safe_float(meta_attrs.get("temperature")),
                "latitude": _safe_float(meta_attrs.get("latitude")),
                "longitude": _safe_float(meta_attrs.get("longitude")),
                "collection_date": meta_attrs.get("collection_date"),
                "country": meta_attrs.get("country"),
            }
            record.update(meta_attrs)
            yield record
