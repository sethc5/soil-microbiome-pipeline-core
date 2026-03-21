#!/usr/bin/env python3
"""Run T0.25 scoring for all t0_pass=1 communities that are missing function_score.

Loads the trained v3 BNF surrogate (bnf_surrogate_regressor_v3.joblib), builds
phylum + env feature vectors for each pending community, and writes
t025_function_score + t025_pass=1 to the runs table.

Usage:
    python apps/bnf/scripts/run_neon_t025.py [--db PATH] [--workers N] [--dry-run]
"""
import argparse
import json
import logging
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT))

from core.compute.functional_predictor import clr_transform  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_META_FEATURES = [
    "soil_ph", "organic_matter_pct", "clay_pct", "temperature_c", "precipitation_mm",
    "total_nitrogen_ppm", "available_p_ppm", "moisture_pct", "bulk_density",
]
_META_DEFAULTS = {
    "soil_ph": 6.5, "organic_matter_pct": 2.5, "clay_pct": 20.0,
    "temperature_c": 15.0, "precipitation_mm": 600.0,
    "total_nitrogen_ppm": 2000.0, "available_p_ppm": 15.0,
    "moisture_pct": 30.0, "bulk_density": 1.2,
}
_BATCH = 2000


def _load_model(model_dir: Path) -> dict:
    try:
        import joblib
    except ImportError:
        sys.exit("joblib required: pip install joblib")
    path = model_dir / "bnf_surrogate_regressor_v3.joblib"
    if not path.exists():
        sys.exit(f"Model not found: {path}")
    payload = joblib.load(path)
    logger.info("Loaded model v%s  features=%d  apply_clr=%s",
                payload.get("version", "?"), len(payload["feature_names"]), payload["apply_clr"])
    return payload


def _fetch_pending(db_path: str, source: str, limit: int) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    q = """
        SELECT r.run_id, r.community_id,
               c.phylum_profile,
               s.soil_ph, s.organic_matter_pct, s.clay_pct,
               s.temperature_c, s.precipitation_mm,
               s.total_nitrogen_ppm, s.available_p_ppm,
               s.moisture_pct, s.bulk_density
        FROM runs r
        JOIN communities c ON r.community_id = c.community_id
        JOIN samples s ON r.sample_id = s.sample_id
        WHERE s.source = ?
          AND r.t0_pass = 1
          AND r.t025_pass IS NULL
        ORDER BY r.run_id
    """
    if limit:
        q += f" LIMIT {limit}"
    rows = [dict(r) for r in conn.execute(q, (source,)).fetchall()]
    conn.close()
    return rows


def _build_X(rows: list[dict], feature_names: list[str]) -> np.ndarray:
    phylum_features = [f for f in feature_names if f not in _META_FEATURES]
    X = []
    for r in rows:
        pp: dict[str, float] = {}
        if r["phylum_profile"]:
            try:
                pp = json.loads(r["phylum_profile"])
            except Exception:
                pass
        vec = [pp.get(p, 0.0) for p in phylum_features]
        for m in _META_FEATURES:
            val = r.get(m)
            vec.append(float(val) if val is not None else _META_DEFAULTS[m])
        X.append(vec)
    return np.array(X, dtype=float)


def _write_scores(db_path: str, run_ids: list[int], scores: np.ndarray) -> None:
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "UPDATE runs SET t025_function_score=?, t025_pass=1, t025_model='bnf_surrogate_v3' "
        "WHERE run_id=?",
        [(float(score), rid) for score, rid in zip(scores, run_ids)],
    )
    conn.commit()
    conn.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default="/data/pipeline/db/soil_microbiome.db")
    p.add_argument("--model-dir", default=str(_REPO_ROOT / "apps/bnf/models"))
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--source", default="neon")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    payload = _load_model(Path(args.model_dir))
    feature_names: list[str] = payload["feature_names"]
    model = payload["model"]
    apply_clr: bool = payload["apply_clr"]

    rows = _fetch_pending(args.db, args.source, args.limit)
    logger.info("Found %d %s communities pending T0.25", len(rows), args.source)

    if args.dry_run or not rows:
        logger.info("[dry-run] Exit without scoring." if args.dry_run else "Nothing to do.")
        return

    n_ok = n_err = 0
    for i in range(0, len(rows), _BATCH):
        batch = rows[i : i + _BATCH]
        X = _build_X(batch, feature_names)
        if apply_clr:
            X = clr_transform(X)
        scores = model.predict(X)
        # Clip to [0, 1] range
        scores = np.clip(scores, 0.0, 1.0)
        run_ids = [r["run_id"] for r in batch]
        _write_scores(args.db, run_ids, scores)
        n_ok += len(batch)
        logger.info("T0.25 scored %d/%d  (batch mean=%.3f)", n_ok, len(rows), float(scores.mean()))

    logger.info("T0.25 complete: n_scored=%d  n_err=%d", n_ok, n_err)


if __name__ == "__main__":
    main()
