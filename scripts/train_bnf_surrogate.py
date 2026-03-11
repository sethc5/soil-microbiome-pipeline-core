"""
scripts/train_bnf_surrogate.py — Train T0.25 BNF surrogate predictor from real FBA results.

Addition C from pipeline_process_diagrams.md:  
  "After ~4k FBA runs, train ML to predict NITROGENASE_MO flux from 16S taxonomy;
   skips FBA for obvious pass/fail; improves over time."

We now have 4,491 real T1-pass communities. This script:
  1. Extracts phylum profiles + soil metadata + T1 results from the DB
  2. Trains a RandomForestClassifier (BNF pass/fail gate)
     and a RandomForestRegressor (predicted mmol NH4/gDW/h for passers)
  3. Cross-validates both to get honest performance estimates
  4. Saves models/bnf_surrogate_rf.joblib  (same format as functional_predictor.joblib)
  5. Prints feature importances

Usage:
  python scripts/train_bnf_surrogate.py --db /data/pipeline/db/soil_microbiome.db
"""

from __future__ import annotations
import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Feature definition (matches existing functional_predictor.joblib) ────────
PHYLA = [
    "Proteobacteria", "Actinobacteria", "Acidobacteria", "Firmicutes",
    "Bacteroidetes", "Verrucomicrobia", "Planctomycetes", "Chloroflexota",
    "Gemmatimonadota", "Nitrospirota", "Cyanobacteria", "Nitrososphaerota",
]
META_COLS = ["soil_ph", "organic_matter_pct", "clay_pct", "temperature_c", "precipitation_mm"]
FEATURE_NAMES = PHYLA + META_COLS
BNF_FLUX_THRESHOLD = 0.01  # mmol NH4/gDW/h — same as T1 separator


def _extract_features(db_path: str) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns:
        X       (n, 17)  phylum rel-abundances + metadata
        y_pass  (n,)     0/1 BNF pass label
        y_flux  (n,)     actual flux value (0.0 for non-pass rows)
    """
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT
            c.phylum_profile,
            s.soil_ph,
            s.organic_matter_pct,
            s.clay_pct,
            s.temperature_c,
            s.precipitation_mm,
            r.t1_pass,
            COALESCE(r.t1_target_flux, 0.0) AS flux
        FROM runs r
        JOIN communities c ON r.community_id = c.community_id
        JOIN samples s ON r.sample_id = s.sample_id
        WHERE r.t0_pass = 1
          AND c.phylum_profile IS NOT NULL
          AND r.t1_pass IS NOT NULL
        ORDER BY r.run_id
    """).fetchall()
    conn.close()

    log.info("Loaded %d rows from DB", len(rows))

    X_list, y_pass_list, y_flux_list = [], [], []
    skipped = 0
    for row in rows:
        try:
            phyla_d = json.loads(row["phylum_profile"])
        except Exception:
            skipped += 1
            continue

        # Phylum features — safe default 0.0 if phylum absent
        phyla_feats = [float(phyla_d.get(p, 0.0)) for p in PHYLA]

        meta_feats = [
            float(row["soil_ph"])              if row["soil_ph"]              is not None else 6.5,
            float(row["organic_matter_pct"])   if row["organic_matter_pct"]   is not None else 2.0,
            float(row["clay_pct"])             if row["clay_pct"]             is not None else 20.0,
            float(row["temperature_c"])        if row["temperature_c"]        is not None else 15.0,
            float(row["precipitation_mm"])     if row["precipitation_mm"]     is not None else 600.0,
        ]

        X_list.append(phyla_feats + meta_feats)
        y_pass_list.append(1 if row["t1_pass"] else 0)
        y_flux_list.append(max(0.0, float(row["flux"])))

    if skipped:
        log.warning("Skipped %d rows with unparseable phylum_profile", skipped)

    X       = np.array(X_list, dtype=float)
    y_pass  = np.array(y_pass_list, dtype=int)
    y_flux  = np.array(y_flux_list, dtype=float)
    return X, y_pass, y_flux


def train(db_path: str, out_dir: Path) -> None:
    try:
        from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
        from sklearn.model_selection import cross_val_score, StratifiedKFold, KFold
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
        from sklearn.metrics import classification_report
        import joblib
    except ImportError:
        log.error("scikit-learn + joblib required: pip install scikit-learn joblib")
        sys.exit(1)

    X, y_pass, y_flux = _extract_features(db_path)
    n_total = len(X)
    n_pass  = y_pass.sum()
    log.info("Dataset: %d samples, %d BNF-pass (%.1f%%)", n_total, n_pass, 100 * n_pass / max(n_total, 1))

    # ── Classifier: predict BNF pass/fail ────────────────────────────────────
    log.info("Training BNF pass/fail classifier …")
    clf_pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", RandomForestClassifier(
            n_estimators=200,
            max_features="sqrt",
            min_samples_leaf=2,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
            oob_score=True,
        )),
    ])
    clf_pipe.fit(X, y_pass)
    clf = clf_pipe.named_steps["clf"]
    log.info("Classifier OOB accuracy: %.3f", clf.oob_score_)

    cv5 = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(clf_pipe, X, y_pass, cv=cv5, scoring="roc_auc", n_jobs=-1)
    log.info("Classifier 5-fold CV ROC-AUC: %.3f ± %.3f", cv_scores.mean(), cv_scores.std())

    # Feature importances — classifier
    importances = dict(zip(FEATURE_NAMES, clf.feature_importances_))
    log.info("Top classifier features: %s", sorted(importances.items(), key=lambda x: -x[1])[:5])

    # ── Regressor: predict flux for BNF-pass communities ─────────────────────
    mask_pass = y_pass == 1
    X_pass  = X[mask_pass]
    y_flux_pass = y_flux[mask_pass]
    log.info("Training BNF flux regressor on %d pass samples …", mask_pass.sum())

    reg_pipe = Pipeline([
        ("scaler", StandardScaler()),
        ("est", RandomForestRegressor(
            n_estimators=200,
            max_features="sqrt",
            min_samples_leaf=2,
            random_state=42,
            n_jobs=-1,
            oob_score=True,
        )),
    ])
    reg_pipe.fit(X_pass, y_flux_pass)
    reg = reg_pipe.named_steps["est"]
    log.info("Regressor OOB R²: %.3f", reg.oob_score_)

    cv5r = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_r2 = cross_val_score(reg_pipe, X_pass, y_flux_pass, cv=cv5r, scoring="r2", n_jobs=-1)
    log.info("Regressor 5-fold CV R²: %.3f ± %.3f", cv_r2.mean(), cv_r2.std())

    reg_importances = dict(zip(FEATURE_NAMES, reg.feature_importances_))
    log.info("Top regressor features: %s", sorted(reg_importances.items(), key=lambda x: -x[1])[:5])

    # ── Save ─────────────────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)

    # Classifier model (gate)
    clf_payload = {
        "model":          clf_pipe,
        "model_type":     "random_forest_classifier",
        "feature_names":  FEATURE_NAMES,
        "apply_clr":      False,
        "training_n":     n_total,
        "pass_n":         int(n_pass),
        "cv_roc_auc":     float(cv_scores.mean()),
        "oob_accuracy":   float(clf.oob_score_),
        "feature_importances": importances,
    }
    clf_path = out_dir / "bnf_surrogate_classifier.joblib"
    joblib.dump(clf_payload, clf_path)
    log.info("Classifier saved → %s", clf_path)

    # Regressor model (flux predictor)
    reg_payload = {
        "model":          reg_pipe,
        "model_type":     "random_forest",
        "feature_names":  FEATURE_NAMES,
        "apply_clr":      False,
        "training_n":     int(mask_pass.sum()),
        "cv_r2":          float(cv_r2.mean()),
        "oob_r2":         float(reg.oob_score_),
        "feature_importances": reg_importances,
    }
    reg_path = out_dir / "bnf_surrogate_regressor.joblib"
    joblib.dump(reg_payload, reg_path)
    log.info("Regressor saved → %s", reg_path)

    # Also overwrite the canonical functional_predictor.joblib that
    # compute/functional_predictor.py loads at runtime
    canonical_payload = dict(reg_payload)  # regressor is the runtime scorer
    canonical_payload["classifier"] = clf_payload["model"]  # attach gate
    joblib.dump(canonical_payload, out_dir / "functional_predictor.joblib")
    log.info("Canonical model overwritten → %s/functional_predictor.joblib", out_dir)

    print("\n=== SURROGATE TRAINING COMPLETE ===")
    print(f"  Samples: {n_total} total, {n_pass} BNF-pass")
    print(f"  Classifier  ROC-AUC (CV): {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
    print(f"  Regressor   R²      (CV): {cv_r2.mean():.3f} ± {cv_r2.std():.3f}")
    print(f"  Top classifier features:")
    for feat, imp in sorted(importances.items(), key=lambda x: -x[1])[:5]:
        print(f"    {feat:30s}  {imp:.4f}")
    print(f"  Models saved to {out_dir}/")


def main() -> None:
    ap = argparse.ArgumentParser(description="Train BNF surrogate predictor from real FBA results")
    ap.add_argument("--db",      required=True, help="Path to soil_microbiome.db")
    ap.add_argument("--out-dir", default="models", help="Output directory for model files")
    args = ap.parse_args()
    train(args.db, Path(args.out_dir))


if __name__ == "__main__":
    main()
