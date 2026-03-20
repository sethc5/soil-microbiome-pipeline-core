from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sim_model.history_health import assess_history_freshness, render_history_markdown


def test_history_freshness_detects_fresh(tmp_path):
    path = tmp_path / "history.jsonl"
    now = datetime.now(timezone.utc)
    path.write_text(
        json.dumps({"run_timestamp_utc": now.isoformat(), "aggregate": {"avg_top1_lift": 0.1}}) + "\n",
        encoding="utf-8",
    )
    payload = assess_history_freshness(path, warn_if_older_than_days=7)
    assert payload["stale"] is False
    assert payload["has_timestamp"] is True


def test_history_freshness_detects_stale(tmp_path):
    path = tmp_path / "history.jsonl"
    old = datetime.now(timezone.utc) - timedelta(days=40)
    path.write_text(
        json.dumps({"run_timestamp_utc": old.isoformat(), "aggregate": {"avg_top1_lift": 0.1}}) + "\n",
        encoding="utf-8",
    )
    payload = assess_history_freshness(path, warn_if_older_than_days=14)
    assert payload["stale"] is True
    md = render_history_markdown(payload)
    assert "STALE" in md
