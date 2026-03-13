"""
ph_stratified_enrichment.py — pH-stratified BNF taxa enrichment analysis.

SCIENTIFIC RATIONALE:
  Soil pH is the strongest single predictor of microbial community composition
  (Lauber et al. 2009, Science 326:1480) AND of BNF rates (soil_ph = top 2
  feature in v2 RF model, importance 10.4%).

  Without controlling for pH, any taxa-BNF association could be a pH artifact:
  acidic soils have both lower BNF AND different communities (Acidobacteria-rich)
  compared to neutral soils. This script asks the pH-corrected question:

    "Within each pH bin, which phyla are enriched in high-BNF vs. low-BNF
     communities?"

  Associations that appear in multiple pH bins are most likely to be genuine
  BNF-associated taxa rather than pH artifacts.

SOURCES:
  - Lauber et al. 2009 (DOI: 10.1126/science.1178534) — pH as primary driver
  - Rousk et al. 2010 (DOI: 10.1038/ismej.2010.94) — pH controls bacterial communities
  - Smercina et al. 2019 (DOI: 10.1128/mSystems.00119-19) — BNF correlates

STATISTICAL METHOD:
  For each (pH bin × phylum): contingency table test
  - High-BNF communities: measured_function ≥ site-level 67th percentile
  - Low-BNF communities: measured_function < 33rd percentile
  - Middle tertile excluded (reduces noise)
  - Fisher's exact test (or chi-squared for large n) on presence/absence
  - Bonferroni correction for multiple comparisons across phyla

Usage:
  python apps/bnf/scripts/ph_stratified_enrichment.py \\
    --db /data/pipeline/db/soil_microbiome.db \\
    --bnf-csv apps/bnf/reference/bnf_measurements.csv \\
    --out results/ph_enrichment.json

Output:
  results/ph_enrichment.json — enriched/depleted phyla per pH bin
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import math
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# pH bin boundaries — scientifically meaningful breakpoints
PH_BINS = [
    ("acidic",   0.0,  5.5),
    ("mod_acid", 5.5,  6.5),
    ("neutral",  6.5,  7.5),
    ("alkaline", 7.5, 14.0),
]


def _fisher_exact_2x2(a: int, b: int, c: int, d: int) -> float:
    """
    One-sided Fisher's exact p-value for:
      [[a, b],   <- high-BNF: phylum present (a) vs. absent (b)
       [c, d]]   <- low-BNF:  phylum present (c) vs. absent (d)

    Returns p-value for the hypothesis: phylum is enriched in high-BNF.
    Uses hypergeometric distribution.
    """
    n = a + b + c + d
    if n == 0:
        return 1.0
    # Log hypergeometric probability for each table at least as extreme
    # p = sum P(X = k) for k >= a, where X ~ hypergeometric
    # P(X=k) = C(a+c, k) * C(b+d, (a+b)-k) / C(n, a+b)
    k1 = a + c  # marginal: phylum present count
    k2 = a + b  # marginal: high-BNF count

    def log_comb(n: int, k: int) -> float:
        if k < 0 or k > n:
            return -math.inf
        return math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1)

    log_denom = log_comb(n, k2)
    if log_denom == -math.inf:
        return 1.0

    p = 0.0
    for k in range(a, min(k1, k2) + 1):
        lp = log_comb(k1, k) + log_comb(n - k1, k2 - k) - log_denom
        p += math.exp(lp) if lp > -700 else 0.0
    return min(p, 1.0)


def _load_bnf_sample_labels(csv_path: Path) -> dict[str, float]:
    """Load sample_id → measured_function (0–1 normalised)."""
    labels: dict[str, float] = {}
    with open(csv_path) as fh:
        for row in csv.DictReader(fh):
            sid = row.get("sample_id", "").strip()
            mf = row.get("measured_function", "")
            try:
                labels[sid] = float(mf)
            except ValueError:
                pass
    return labels


def _load_communities(db_path: str) -> list[dict]:
    """Load sample_id, soil_ph, phylum_profile from DB."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT c.sample_id, s.soil_ph, c.phylum_profile "
        "FROM communities c JOIN samples s ON c.sample_id = s.sample_id "
        "WHERE s.soil_ph IS NOT NULL AND c.phylum_profile IS NOT NULL"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def run_enrichment(
    communities: list[dict],
    sample_labels: dict[str, float],
    min_prevalence: float = 0.05,
    alpha: float = 0.05,
) -> dict:
    """
    Run pH-stratified enrichment analysis.

    Parameters:
        min_prevalence: minimum fraction of communities a phylum must appear
                        in (within the bin) to be tested
        alpha: Bonferroni-corrected significance threshold
    """
    # Get BNF tertile thresholds
    all_vals = sorted(v for v in sample_labels.values())
    if len(all_vals) < 10:
        return {"error": "insufficient labelled samples"}
    low_cutoff = all_vals[len(all_vals) // 3]
    high_cutoff = all_vals[2 * len(all_vals) // 3]
    logger.info("BNF tertile thresholds: low=%.4f high=%.4f", low_cutoff, high_cutoff)

    # Collect all phyla across the dataset
    all_phyla: set[str] = set()
    for comm in communities:
        if comm["phylum_profile"]:
            try:
                all_phyla.update(json.loads(comm["phylum_profile"]).keys())
            except (json.JSONDecodeError, TypeError):
                pass
    all_phyla = sorted(all_phyla)
    logger.info("Testing %d phyla across %d pH bins", len(all_phyla), len(PH_BINS))

    results_by_bin: dict[str, list] = {}
    summary: list[dict] = []

    for bin_name, ph_lo, ph_hi in PH_BINS:
        # Filter to this pH bin
        bin_comms = [
            c for c in communities
            if c["soil_ph"] is not None and ph_lo <= float(c["soil_ph"]) < ph_hi
            and c["sample_id"] in sample_labels
        ]
        if not bin_comms:
            logger.info("pH bin %s: no communities", bin_name)
            continue

        high_bnf = [c for c in bin_comms if sample_labels[c["sample_id"]] >= high_cutoff]
        low_bnf  = [c for c in bin_comms if sample_labels[c["sample_id"]] <= low_cutoff]
        n_high, n_low = len(high_bnf), len(low_bnf)

        logger.info("pH bin %s (%.1f–%.1f): %d total, %d high-BNF, %d low-BNF",
                    bin_name, ph_lo, ph_hi, len(bin_comms), n_high, n_low)

        if n_high < 5 or n_low < 5:
            logger.info("  Skipping bin %s — insufficient high or low communities", bin_name)
            continue

        # Parse phylum profiles once per bin
        def get_pp(comm: dict) -> dict[str, float]:
            try:
                return json.loads(comm["phylum_profile"]) if comm["phylum_profile"] else {}
            except (json.JSONDecodeError, TypeError):
                return {}

        high_pps = [get_pp(c) for c in high_bnf]
        low_pps  = [get_pp(c) for c in low_bnf]

        n_tests = 0
        bin_results = []

        for phylum in all_phyla:
            # Presence/absence (relative abundance > 0.001)
            hi_present = sum(1 for pp in high_pps if pp.get(phylum, 0) > 0.001)
            hi_absent  = n_high - hi_present
            lo_present = sum(1 for pp in low_pps  if pp.get(phylum, 0) > 0.001)
            lo_absent  = n_low  - lo_present

            # Minimum prevalence filter
            total_prev = (hi_present + lo_present) / (n_high + n_low)
            if total_prev < min_prevalence:
                continue

            n_tests += 1
            p_enriched  = _fisher_exact_2x2(hi_present, hi_absent, lo_present, lo_absent)
            p_depleted  = _fisher_exact_2x2(lo_present, lo_absent, hi_present, hi_absent)
            hi_frac = hi_present / n_high
            lo_frac = lo_present / n_low
            fold_change = (hi_frac + 1e-6) / (lo_frac + 1e-6)

            bin_results.append({
                "phylum": phylum,
                "hi_prevalence": round(hi_frac, 4),
                "lo_prevalence": round(lo_frac, 4),
                "fold_change_hi_vs_lo": round(fold_change, 3),
                "p_enriched": round(p_enriched, 6),
                "p_depleted": round(p_depleted, 6),
                "hi_n": n_high, "lo_n": n_low,
            })

        # Bonferroni correction
        bonf_threshold = alpha / max(n_tests, 1)
        significant = [
            r for r in bin_results
            if r["p_enriched"] < bonf_threshold or r["p_depleted"] < bonf_threshold
        ]
        significant.sort(key=lambda x: min(x["p_enriched"], x["p_depleted"]))

        # Add direction + significance flag
        for r in significant:
            r["direction"] = "enriched" if r["p_enriched"] < r["p_depleted"] else "depleted"
            r["significant"] = True
            r["bonferroni_threshold"] = round(bonf_threshold, 8)

        results_by_bin[bin_name] = significant[:30]  # top 30 per bin
        logger.info("  %s: %d significant (Bonferroni p<%.6f, %d tests)",
                    bin_name, len(significant), bonf_threshold, n_tests)

        # Build cross-bin summary entries
        for r in significant[:10]:
            summary.append({
                "ph_bin": bin_name,
                "ph_range": f"{ph_lo}–{ph_hi}",
                "phylum": r["phylum"],
                "direction": r["direction"],
                "fold_change": r["fold_change_hi_vs_lo"],
                "p": min(r["p_enriched"], r["p_depleted"]),
            })

    # Cross-bin consistency: which phyla are significant in multiple pH bins?
    from collections import Counter
    phylum_bin_counts = Counter(r["phylum"] for r in summary if r["direction"] == "enriched")
    consistent_enriched = [
        {"phylum": p, "n_bins": c}
        for p, c in phylum_bin_counts.most_common(15)
        if c >= 2
    ]
    phylum_bin_counts_dep = Counter(r["phylum"] for r in summary if r["direction"] == "depleted")
    consistent_depleted = [
        {"phylum": p, "n_bins": c}
        for p, c in phylum_bin_counts_dep.most_common(15)
        if c >= 2
    ]

    return {
        "method": "Fisher's exact test, Bonferroni corrected, presence/absence per phylum",
        "ph_bins": {b: f"{lo}–{hi}" for b, lo, hi in PH_BINS},
        "bnf_tertile_thresholds": {"low_cutoff": round(low_cutoff, 4), "high_cutoff": round(high_cutoff, 4)},
        "n_communities_total": len(communities),
        "n_labelled": len([c for c in communities if c["sample_id"] in sample_labels]),
        "results_by_bin": results_by_bin,
        "consistent_enriched_across_bins": consistent_enriched,
        "consistent_depleted_across_bins": consistent_depleted,
        "interpretation": (
            "Phyla enriched in high-BNF communities WITHIN pH bins are less likely to be "
            "pH confounds. Phyla appearing significant in ≥2 pH bins are strongest BNF candidates."
        ),
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--bnf-csv", required=True)
    parser.add_argument("--out", default="results/ph_enrichment.json")
    parser.add_argument("--min-prevalence", type=float, default=0.05,
                        help="Minimum within-bin prevalence to test (default 0.05)")
    args = parser.parse_args()

    sample_labels = _load_bnf_sample_labels(Path(args.bnf_csv))
    logger.info("Loaded %d sample BNF labels", len(sample_labels))

    communities = _load_communities(args.db)
    logger.info("Loaded %d communities with pH + phylum profile", len(communities))

    report = run_enrichment(communities, sample_labels, min_prevalence=args.min_prevalence)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    logger.info("Results written to %s", out_path)

    print(f"\n=== pH-Stratified BNF Enrichment ===")
    print(f"  Communities analysed: {report['n_labelled']:,}")
    print(f"\nConsistent enrichments (≥2 pH bins):")
    for e in report.get("consistent_enriched_across_bins", []):
        print(f"  + {e['phylum']:<30} ({e['n_bins']} bins)")
    print(f"\nConsistent depletions (≥2 pH bins):")
    for d in report.get("consistent_depleted_across_bins", []):
        print(f"  - {d['phylum']:<30} ({d['n_bins']} bins)")
    if not report.get("consistent_enriched_across_bins"):
        print("  (no phylum significant in ≥2 bins — label leakage may dominate signal)")
    print(f"\nFull results: {args.out}")


if __name__ == "__main__":
    main()
