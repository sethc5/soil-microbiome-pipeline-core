from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
from pathlib import Path

from compute.metadata_normalizer import MetadataNormalizer
from db_utils import SoilDB

logger = logging.getLogger(__name__)

class SampleManager:
    """
    Handles sample ingestion, normalization, and persistence.
    Unifies logic from various adapters and ingestion scripts.
    """

    def __init__(self, db: SoilDB):
        self.db = db
        self.normalizer = MetadataNormalizer()

    def ingest_sample(self, raw_data: Dict[str, Any], source: str) -> str:
        """
        Normalize and store a single sample.
        """
        normalized = self.normalizer.normalize_sample(raw_data, source=source)
        
        # Ensure sample_id is present
        sample_id = normalized.get("sample_id")
        if not sample_id:
            # Fallback ID generation if missing
            import uuid
            sample_id = f"{source}.{uuid.uuid4().hex[:8]}"
            normalized["sample_id"] = sample_id

        self.db.upsert_sample(normalized)
        return sample_id

    def batch_ingest(self, samples: List[Dict[str, Any]], source: str) -> List[str]:
        """
        Ingest a batch of samples efficiently.
        """
        ids = []
        for s in samples:
            try:
                sid = self.ingest_sample(s, source)
                ids.append(sid)
            except Exception as exc:
                logger.warning("Failed to ingest sample from %s: %s", source, exc)
        return ids
