"""
retrain_bnf_surrogate_v3.py — RF v3: expanded env features (no top_genera — provenance unclear).

CHANGES FROM v2:
  - Adds 4 more env features: total_nitrogen_ppm, available_p_ppm, moisture_pct, bulk_density
  - Does NOT add top_genera features (provenance unclear — may be FBA-predicted, not real 16S)
  - Same phylum_profile features as v2 (59 phyla)
  - Total: 64 → up to 68 features (only populated features added)

WHY NOT top_genera:
  The communities.top_genera column contains only 26 genera with a
  [{"name": ..., "rel_abundance": ...}] format — different from the flat dict that
  process_neon_16s.py (vsearch) generates. All 26 are BNF-curated genera with
  suspiciously uniform ~45% frequency across samples. This strongly suggests they
  were populated by the legacy FBA annotation pipeline (t1_fba_batch.py) rather
  than real 16S classification. Using FBA-derived genus predictions as RF training
  features would be partially circular. See docs/GROUND_TRUTH_pitfalls.md Pitfall #9.

  To safely add genus features, process_neon_16s.py must be confirmed as the source
  OR the vsearch hits.uc must be re-extracted to get genuine genus-level taxonomy.

SCIENTIFIC CAVEAT:
  Additional soil chemistry features (N, P, moisture, bulk_density) are independent
  of the RF model. If available for more samples, they may improve cross-site transfer
  because nutrient status is a known BNF co-variate (P-limitation of BNF:
  Vitousek & Field 1999, DOI: 10.1046/j.1365-2745.1999.00353.x).

Usage:
  python apps/bnf/scripts/retrain_bnf_surrogate_v3.py \\
    --db /data/pipeline/db/soil_microbiome.db \\
    --bnf-csv apps/bnf/reference/bnf_measurements.csv \\
    --out-dir apps/bnf/models/

Output:
  apps/bnf/models/bnf_surrogate_classifier_v3.joblib
  apps/bnf/models/bnf_surrogate_regressor_v3.joblib
  apps/bnf/models/retrain_report_v3.json
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import sqlite3
from pathlib import Path

import numpy as np

logger = logging.getLogger(__name__)

# v3 expands env features from 5 → 9
_META_FEATURES = [
    "soil_ph",
    "organic_matter_pct",
    "clay_pct",
    "temperature_c",
    "precipitation_mm",
    # NEW in v3:
    "total_nitrogen_ppm",
    "available_p_ppm",
    "moisture_pct",
    "bulk_density",
]
_META_DEFAULTS = {
    "soil_ph": 6.5,
    "organic_matter_pct": 2.5,
    "clay_pct": 20.0,
    "temperature_c": 15.0,
    "precipitation_mm": 600.0,
    "total_nitrogen_ppm": 2000.0,   # ~2 g/kg typical mineral soil
    "available_p_ppm": 15.0,        # mg/kg typical
    "moisture_pct": 30.0,           # volumetric
    "bulk_density": 1.2,            # g/cm³ typical mineral soil
}


def _load_bnf_labels(csv_path: Path) -> dict[str, float]:
    labels: dict[str, float] = {}
    with open(csv_path) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            sid = row.get("sample_id", "").strip()
            mf = row.get("measured_function", "")
            try:
                labels[sid] = float(mf)
            except (ValueError, KeyError):
                pass
    logger.info("Loaded %d BNF labels from %s", len(labels), csv_path)
    return labels


def _build_feature_matrix(
    db_path: str,
    labels: dict[str, float],
    min_samples: int = 50,
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """
    Query phylum profiles + expanded env metadata for labelled samples.
    Returns (X, y, feature_names).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT c.sample_id, c.phylum_profile, "
        "s.soil_ph, s.organic_matter_pct, s.clay_pct, s.temperature_c, s.precipitation_mm, "
        "s.total_nitrogen_ppm, s.available_p_ppm, s.moisture_pct, s.bulk_density "
        "FROM communities c "
        "JOIN samples s ON c.sample_id = s.sample_id "
    ).fetchall()
    conn.close()

    rows = [r for r in rows if r["sample_id"] in labels]
    if len(rows) < min_samples:
        raise ValueError(f"Only {len(rows)} labelled samples (need ≥{min_samples}).")
    logger.info("Found %d labelled samples", len(rows))

    # Count how many samples have each new env feature
    for feat in ["total_nitrogen_ppm", "available_p_ppm", "moisture_pct", "bulk_density"]:
        n_filled = sum(1 for r in rows if r[feat] is not None)
        logger.info("  %s: %d/%d samples populated (%.1f%%)",
                    feat, n_filled, len(rows), 100 * n_filled / len(rows))

    # Discover phylum features
    all_phyla: set[str] = set()
    for r in rows:
        if r["phylum_profile"]:
            try:
                pp = json.loads(r["phylum_profile"])
                all_phyla.update(pp.keys())
            except Exception:
                pass
    phylum_features = sorted(all_phyla)
    feature_names = phylum_features + _META_FEATURES
    logger.info("Feature matrix: %d phyla + %d env = %d features",
                len(phylum_features), len(_META_FEATURES), len(feature_names))

    X_rows: list[list[float]] = []
    y_vals: list[float] = []

    for r in rows:
        pp: dict[str, float] = {}
        if r["phylum_profile"]:
            try:
                pp = json.loads(r["phylum_profile"])
            except Exception:
                pass

        row_vec = [pp.get(p, 0.0) for p in phylum_features]
        for meta in _META_FEATURES:
            val = r[meta]
            row_vec.append(float(val) if val is not None else _META_DEFAULTS[meta])

        X_rows.append(row_vec)
        y_vals.append(labels[r["sample_id"]])

    X = np.array(X_rows, dtype=float)
    y = np.array(y_vals, dtype=float)
    return X, y, feature_names


def _train_and_save(
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    out_dir: Path,
    apply_clr: bool = False,
) -> dict:
    try:
        import joblib
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        from sklearn.model_selection import cross_val_score, StratifiedKFold, KFold
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:
        raise ImportError("scikit-learn and joblib required") from exc

    from core.compute.functional_predictor import clr_transform

    out_dir.mkdir(parents=True, exist_ok=True)
    n, nf = X.shape
    logger.info("Training v3 on %d samples × %d features", n, nf)

    X_proc = clr_transform(X) if apply_clr else X

    # Classifier
    threshold_33 = float(np.percentile(y, 67))
    y_clf = (y >= threshold_33).astype(int)
    clf_pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", RandomForestClassifier(
            n_estimators=400, max_features="sqrt", min_samples_leaf=3,
            class_weight="balanced", oob_score=True, random_state=42, n_jobs=-1,
        )),
    ])
    cv_clf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_roc = cross_val_score(clf_pipeline, X_proc, y_clf, cv=cv_clf, scoring="roc_auc", n_jobs=-1)
    clf_pipeline.fit(X_proc, y_clf)
    oob_acc = clf_pipeline.named_steps["clf"].oob_score_
    logger.info("Classifier CV ROC-AUC: %.4f ± %.4f; OOB: %.4f",
                cv_roc.mean(), cv_roc.std(), oob_acc)

    clf_payload = {
        "model": clf_pipeline, "model_type": "random_forest",
        "feature_names": feature_names, "apply_clr": apply_clr,
        "training_n": n, "cv_roc_auc": float(cv_roc.mean()),
        "label_source": "published_bnf_rates", "version": "v3",
        "feature_importances": dict(zip(
            feature_names, clf_pipeline.named_steps["clf"].feature_importances_.tolist())),
    }
    clf_path = out_dir / "bnf_surrogate_classifier_v3.joblib"
    joblib.dump(clf_payload, clf_path)
    logger.info("Saved classifier v3 → %s", clf_path)

    # Regressor
    reg_pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("reg", RandomForestRegressor(
            n_estimators=400, max_features="sqrt", min_samples_leaf=3,
            oob_score=True, random_state=42, n_jobs=-1,
        )),
    ])
    cv_reg = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_r2 = cross_val_score(reg_pipeline, X_proc, y, cv=cv_reg, scoring="r2", n_jobs=-1)
    reg_pipeline.fit(X_proc, y)
    oob_r2 = reg_pipeline.named_steps["reg"].oob_score_
    logger.info("Regressor CV R²: %.4f ± %.4f; OOB: %.4f",
                cv_r2.mean(), cv_r2.std(), oob_r2)

    y_pred = reg_pipeline.predict(X_proc)
    from core.validate_pipeline import _spearman_r
    sp_r = _spearman_r(y_pred.tolist(), y.tolist())
    logger.info("In-sample Spearman r: %.4f", sp_r)

    reg_payload = {
        "model": reg_pipeline, "model_type": "random_forest",
        "feature_names": feature_names, "apply_clr": apply_clr,
        "training_n": n, "cv_r2": float(cv_r2.mean()),
        "label_source": "published_bnf_rates", "version": "v3",
        "feature_importances": dict(zip(
            feature_names, reg_pipeline.named_steps["reg"].feature_importances_.tolist())),
    }
    reg_path = out_dir / "bnf_surrogate_regressor_v3.joblib"
    joblib.dump(reg_payload, reg_path)
    logger.info("Saved regressor v3 → %s", reg_path)

    top_features = sorted(
        clf_payload["feature_importances"].items(), key=lambda x: x[1], reverse=True)[:20]

    report = {
        "version": "v3",
        "changes_from_v2": [
            "Added env features: total_nitrogen_ppm, available_p_ppm, moisture_pct, bulk_density",
            "Did NOT add top_genera (provenance unclear — may be FBA-derived, not real 16S)",
        ],
        "training_n": n,
        "n_features": nf,
        "classifier": {
            "cv_roc_auc": float(cv_roc.mean()), "cv_roc_auc_std": float(cv_roc.std()),
            "oob_accuracy": float(oob_acc),
        },
        "regressor": {
            "cv_r2": float(cv_r2.mean()), "cv_r2_std": float(cv_r2.std()),
            "oob_r2": float(oob_r2), "insample_spearman_r": float(sp_r),
        },
        "top_features": top_features,
        "scientific_note": (
            "v3 expands environmental covariates. Nitrogen and phosphorus availability are "
            "known BNF modulators (P-limitation: Vitousek & Field 1999 DOI:10.1046/j.1365-2745.1999.00353.x). "
            "CV metrics are within-site (same-site leakage possible). "
            "Run loso_cv_bnf_surrogate_v3.py for honest cross-site performance."
        ),
    }
    report_path = out_dir / "retrain_report_v3.json"
    report_path.write_text(json.dumps(report, indent=2))
    logger.info("Report → %s", report_path)
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", required=True)
    p.add_argument("--bnf-csv", required=True)
    p.add_argument("--out-dir", default="apps/bnf/models/")
    p.add_argument("--no-clr", action="store_true")
    args = p.parse_args()

    labels = _load_bnf_labels(Path(args.bnf_csv))
    X, y, feature_names = _build_feature_matrix(args.db, labels)
    report = _train_and_save(X, y, feature_names, Path(args.out_dir), apply_clr=not args.no_clr)

    print(f"\n=== v3 Retraining Complete ===")
    print(f"  Training samples : {report['training_n']:,}")
    print(f"  Features         : {report['n_features']}")
    print(f"  Classifier ROC-AUC (5-fold CV): {report['classifier']['cv_roc_auc']:.4f}")
    print(f"  Regressor R² (5-fold CV)      : {report['regressor']['cv_r2']:.4f}")
    print(f"  In-sample Spearman r          : {report['regressor']['insample_spearman_r']:.4f}")
    print(f"\nTop 10 predictive features:")
    for feat, imp in report["top_features"][:10]:
        print(f"  {feat:<35} {imp:.4f}")
    print(f"\nNext: run loso_cv_bnf_surrogate.py --model-version v3 for honest LOSO r")
    print(f"Compare LOSO r vs v2 baseline of 0.155")


if __name__ == "__main__":
    main()
