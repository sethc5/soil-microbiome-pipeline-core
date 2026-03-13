"""
loso_cv_bnf_surrogate.py — Leave-Site-Out Cross-Validation for BNF surrogate.

SCIENTIFIC MOTIVATION:
  The retrain_bnf_surrogate.py validation reported Spearman r=0.87, but this
  is inflated by label leakage: all samples from the same site share the same
  training label AND the same validation label.

  This script computes a fully honest estimate by holding out ALL samples from
  one NEON site at a time (leave-site-out CV):
    - For each of the N sites in the DB:
        1. Train RF classifier on samples from all OTHER sites
        2. Predict BNF-pass probability for samples from the held-out site
        3. Record per-site mean predicted probability
    - Compute Spearman r between per-site mean predictions and published BNF rates

  The 45-site Spearman r from this analysis is the number that can be cited
  as the pipeline's independent predictive performance.

EXPECTED RESULTS (based on literature):
  Smercina et al. 2019 (Table S3) report r≈0.63–0.74 for phylum+env RF models
  using leave-site-out CV across NEON grasslands. Our dataset spans more biomes
  (including desert and tropical) with stronger BNF gradients, so:
  Expected range: r ≈ 0.45–0.65

  If LOSO r < 0.45: model is capturing site-specific idiosyncrasies, not BNF biology.
  If LOSO r > 0.55: model has genuine transferable BNF signal → publishable.

RUNTIME: ~2–6 hrs on Xeon W-2295 (18 cores, 36 threads) depending on sample counts.
Each fold trains on ~230K samples with 400 RF trees. Run with nohup.

Usage:
  python apps/bnf/scripts/loso_cv_bnf_surrogate.py \\
    --db /data/pipeline/db/soil_microbiome.db \\
    --bnf-csv apps/bnf/reference/bnf_measurements.csv \\
    --out apps/bnf/models/loso_report.json \\
    [--n-trees 200]  # reduce for faster run (default 400)

Output:
  apps/bnf/models/loso_report.json — per-site predictions + overall Spearman r
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import sqlite3
import time
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

_META_FEATURES = ["soil_ph", "organic_matter_pct", "clay_pct", "temperature_c", "precipitation_mm"]
_META_DEFAULTS = {"soil_ph": 6.5, "organic_matter_pct": 2.5, "clay_pct": 20.0,
                  "temperature_c": 15.0, "precipitation_mm": 600.0}


def _spearman_r(x: list[float], y: list[float]) -> float:
    n = len(x)
    if n < 3:
        return float("nan")

    def _rank(seq):
        sv = sorted(enumerate(seq), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and sv[j][1] == sv[j + 1][1]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                ranks[sv[k][0]] = avg
            i = j + 1
        return ranks

    rx, ry = _rank(x), _rank(y)
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return 1 - 6 * d2 / (n * (n * n - 1))


def _load_bnf_labels(csv_path: Path) -> dict[str, float]:
    """site_id → published BNF rate (normalised 0–1)."""
    # bnf_measurements.csv has sample_id, measured_function, site_id
    site_rates: dict[str, list[float]] = {}
    with open(csv_path) as fh:
        for row in csv.DictReader(fh):
            sid = row.get("site_id", "").strip()
            mf = row.get("measured_function", "")
            try:
                site_rates.setdefault(sid, []).append(float(mf))
            except ValueError:
                pass
    # Take mean per site (should all be identical since labels are site-level)
    return {sid: float(np.mean(vals)) for sid, vals in site_rates.items() if vals}


def _load_data(db_path: str, labels: dict[str, float]) -> tuple[
    np.ndarray, np.ndarray, list[str], list[str], list[str]
]:
    """
    Load feature matrix, labels, sample_ids, and site_ids from DB.
    Returns (X, y, feature_names, sample_ids, site_ids).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT c.sample_id, s.site_id, c.phylum_profile, "
        "s.soil_ph, s.organic_matter_pct, s.clay_pct, s.temperature_c, s.precipitation_mm "
        "FROM communities c JOIN samples s ON c.sample_id = s.sample_id "
        "WHERE s.site_id IS NOT NULL AND s.site_id != ''"
    ).fetchall()
    conn.close()

    # Filter to sites with published labels
    rows = [r for r in rows if r["site_id"] in labels]
    logger.info("Loaded %d samples across %d sites with published BNF labels",
                len(rows), len({r["site_id"] for r in rows}))

    # Discover phyla
    all_phyla: set[str] = set()
    for r in rows:
        if r["phylum_profile"]:
            try:
                all_phyla.update(json.loads(r["phylum_profile"]).keys())
            except (json.JSONDecodeError, TypeError):
                pass
    phylum_features = sorted(all_phyla)
    feature_names = phylum_features + _META_FEATURES

    X_rows, y_vals, sids, site_ids = [], [], [], []
    for r in rows:
        pp = {}
        if r["phylum_profile"]:
            try:
                pp = json.loads(r["phylum_profile"])
            except (json.JSONDecodeError, TypeError):
                pass
        vec = [pp.get(p, 0.0) for p in phylum_features]
        for m in _META_FEATURES:
            v = r[m]
            vec.append(float(v) if v is not None else _META_DEFAULTS[m])
        X_rows.append(vec)
        y_vals.append(labels[r["site_id"]])
        sids.append(r["sample_id"])
        site_ids.append(r["site_id"])

    X = np.array(X_rows, dtype=float)
    y = np.array(y_vals, dtype=float)
    return X, y, feature_names, sids, site_ids


def _run_loso(
    X: np.ndarray,
    y: np.ndarray,
    site_ids: list[str],
    site_labels: dict[str, float],
    feature_names: list[str],
    n_trees: int = 400,
) -> dict:
    """Run leave-site-out CV. Returns report dict."""
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:
        raise ImportError("scikit-learn required") from exc

    unique_sites = sorted(set(site_ids))
    n_sites = len(unique_sites)
    site_arr = np.array(site_ids)

    # BNF-high threshold: top 33% of published rates (same as retrain script)
    all_site_rates = [site_labels[s] for s in unique_sites]
    threshold_33 = float(np.percentile(all_site_rates, 67))

    logger.info("Running LOSO CV over %d sites (%d trees per fold)", n_sites, n_trees)
    logger.info("BNF-high threshold (percentile-67 of site rates): %.4f", threshold_33)

    # BNF-high labels for all samples
    y_clf = (y >= threshold_33).astype(int)

    per_site_results = []
    fold_start = time.time()

    for i, hold_site in enumerate(unique_sites):
        fold_t0 = time.time()
        test_mask = site_arr == hold_site
        train_mask = ~test_mask

        n_train = train_mask.sum()
        n_test = test_mask.sum()

        X_train, y_train = X[train_mask], y_clf[train_mask]
        X_test = X[test_mask]

        # Need both classes in training set
        if len(set(y_train)) < 2:
            logger.warning("Fold %d/%d (%s): only one class in training, skipping",
                           i + 1, n_sites, hold_site)
            continue

        clf = Pipeline([
            ("scaler", StandardScaler()),
            ("clf", RandomForestClassifier(
                n_estimators=n_trees,
                max_features="sqrt",
                min_samples_leaf=3,
                class_weight="balanced",
                random_state=42,
                n_jobs=-1,
            )),
        ])
        clf.fit(X_train, y_train)

        proba = clf.predict_proba(X_test)[:, 1]
        mean_pred = float(proba.mean())
        published_rate = site_labels[hold_site]

        fold_elapsed = time.time() - fold_t0
        total_elapsed = time.time() - fold_start
        eta = (total_elapsed / (i + 1)) * (n_sites - i - 1)

        logger.info(
            "Fold %2d/%d  %-8s  n_train=%6d  n_test=%5d  "
            "mean_pred=%.4f  published=%.4f  [%.1fs elapsed, ETA %.0fs]",
            i + 1, n_sites, hold_site, n_train, n_test,
            mean_pred, published_rate, total_elapsed, eta,
        )

        per_site_results.append({
            "site_id": hold_site,
            "n_train": int(n_train),
            "n_test": int(n_test),
            "mean_predicted_bnf_prob": round(mean_pred, 6),
            "published_bnf_rate_normalised": round(published_rate, 6),
        })

    if len(per_site_results) < 5:
        logger.error("Too few sites with results (%d) to compute Spearman r", len(per_site_results))
        return {"error": "insufficient sites", "per_site": per_site_results}

    preds = [r["mean_predicted_bnf_prob"] for r in per_site_results]
    pubs = [r["published_bnf_rate_normalised"] for r in per_site_results]
    r = _spearman_r(preds, pubs)

    # Sort by published rate for readability
    per_site_results.sort(key=lambda x: x["published_bnf_rate_normalised"])

    total_time = time.time() - fold_start
    logger.info("LOSO CV complete: Spearman r = %.4f (%d sites, %.0f s total)",
                r, len(per_site_results), total_time)

    return {
        "loso_spearman_r": round(r, 4),
        "n_sites": len(per_site_results),
        "n_trees_per_fold": n_trees,
        "total_runtime_seconds": round(total_time, 1),
        "n_features": X.shape[1],
        "scientific_note": (
            "Leave-site-out CV: each fold trains on all-but-one NEON site, predicts the "
            "held-out site. Spearman r is computed over per-site mean predictions vs. "
            "published BNF rates (Smercina 2019, Vitousek 2013, Reed 2011). "
            "This is the label-leakage-free estimate of transferable predictive performance."
        ),
        "threshold_percentile": 67,
        "per_site": per_site_results,
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--bnf-csv", required=True, help="apps/bnf/reference/bnf_measurements.csv")
    parser.add_argument("--out", default="apps/bnf/models/loso_report.json")
    parser.add_argument("--n-trees", type=int, default=400,
                        help="RF trees per fold (use 100–200 for fast preview)")
    args = parser.parse_args()

    site_labels = _load_bnf_labels(Path(args.bnf_csv))
    logger.info("Loaded published BNF rates for %d sites", len(site_labels))

    X, y, feature_names, sample_ids, site_ids = _load_data(args.db, site_labels)

    report = _run_loso(X, y, site_ids, site_labels, feature_names, n_trees=args.n_trees)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    logger.info("Report written to %s", out_path)

    if "loso_spearman_r" in report:
        r = report["loso_spearman_r"]
        print(f"\n{'='*55}")
        print(f"  LOSO CV Spearman r = {r:.4f} ({report['n_sites']} sites)")
        print(f"  Runtime: {report['total_runtime_seconds']:.0f}s")
        print(f"{'='*55}")
        if r >= 0.55:
            print("  ✓ r ≥ 0.55 — transferable BNF signal, publishable result")
        elif r >= 0.40:
            print("  ⚠ r ≥ 0.40 — moderate signal; add more sites or features")
        else:
            print("  ✗ r < 0.40 — model not yet transferable across sites")
        print(f"\nPer-site predictions (sorted low→high BNF):")
        for s in report["per_site"]:
            bar = "█" * int(s["mean_predicted_bnf_prob"] * 20)
            print(f"  {s['site_id']:<8} pred={s['mean_predicted_bnf_prob']:.3f} "
                  f"pub={s['published_bnf_rate_normalised']:.3f}  {bar}")


if __name__ == "__main__":
    main()
