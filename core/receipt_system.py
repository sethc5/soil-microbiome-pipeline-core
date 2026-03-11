"""
receipt_system.py — JSON receipt writer and FBA cost tracking.

A receipt is a JSON file written at the end of each pipeline batch recording:
  - machine ID, timestamps, sample counts
  - FBA run counts (expensive, need tracking)
  - dynamic simulation run counts (most expensive)
  - overall batch status

Receipts are ingested later by merge_receipts.py into the SQLite receipts table.
"""

from __future__ import annotations

import json
import platform
import uuid
from datetime import datetime, timezone
from pathlib import Path


def _machine_id() -> str:
    return platform.node()


class Receipt:
    def __init__(self, receipts_dir: str | Path = "receipts/"):
        self.receipts_dir = Path(receipts_dir)
        self.receipts_dir.mkdir(parents=True, exist_ok=True)
        self.receipt_id = str(uuid.uuid4())
        self.machine_id = _machine_id()
        self.batch_start: str | None = None
        self.batch_end: str | None = None
        self.n_samples_processed = 0
        self.n_fba_runs = 0
        self.n_dynamics_runs = 0
        self.status = "started"

    def start(self) -> "Receipt":
        self.batch_start = datetime.now(timezone.utc).isoformat()
        return self

    def finish(self, status: str = "completed") -> Path:
        self.batch_end = datetime.now(timezone.utc).isoformat()
        self.status = status
        return self._write()

    def _write(self) -> Path:
        filepath = self.receipts_dir / f"{self.receipt_id}.json"
        payload = {
            "receipt_id": self.receipt_id,
            "machine_id": self.machine_id,
            "batch_start": self.batch_start,
            "batch_end": self.batch_end,
            "n_samples_processed": self.n_samples_processed,
            "n_fba_runs": self.n_fba_runs,
            "n_dynamics_runs": self.n_dynamics_runs,
            "status": self.status,
            "filepath": str(filepath),
        }
        filepath.write_text(json.dumps(payload, indent=2))
        return filepath
