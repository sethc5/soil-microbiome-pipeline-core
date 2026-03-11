"""
scripts/keystone_analysis.py — Analyze t1_keystone_taxa across T1-pass communities.

For each T1-pass community, parses the keystone_taxa JSON to compute:
  - keystone count per community
  - mean / max flux_drop_pct per community
  - per-organism frequency of being keystone across all communities
  - per-organism mean flux_drop_pct

Output: results/keystone_analysis.csv
        results/keystone_organism_summary.csv
        stdout summary

Usage:
  python scripts/keystone_analysis.py --db /data/pipeline/db/soil_microbiome.db
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--output-dir", default="results")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    rows = conn.execute(
        """
        SELECT r.community_id, r.t1_keystone_taxa, r.t1_target_flux,
               s.land_use, s.climate_zone
        FROM runs r
        JOIN communities c ON r.community_id = c.community_id
        JOIN samples s ON r.sample_id = s.sample_id
        WHERE r.t1_pass = 1
          AND r.t1_keystone_taxa IS NOT NULL
          AND r.t1_keystone_taxa NOT IN ('', 'null', '[]', '{}')
        """
    ).fetchall()
    conn.close()

    print(f"Communities with keystone data: {len(rows)}")

    community_rows: list[dict] = []
    org_flux_drops: dict[str, list[float]] = defaultdict(list)
    org_keystone_count: dict[str, int] = defaultdict(int)
    org_total_seen: dict[str, int] = defaultdict(int)  # how many communities it appears in

    for cid, taxa_json, flux, land_use, climate in rows:
        try:
            taxa = json.loads(taxa_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(taxa, list) or len(taxa) == 0:
            continue

        keystones = [t for t in taxa if t.get("is_keystone")]
        n_keystones = len(keystones)
        drop_pcts = [t["flux_drop_pct"] for t in keystones if "flux_drop_pct" in t]
        mean_drop = statistics.mean(drop_pcts) if drop_pcts else None
        max_drop = max(drop_pcts) if drop_pcts else None
        top_org = max(taxa, key=lambda t: t.get("flux_drop_pct", 0), default={})

        community_rows.append(
            {
                "community_id": cid,
                "land_use": land_use or "",
                "climate_zone": climate or "",
                "t1_target_flux": flux,
                "n_keystones": n_keystones,
                "n_total_taxa": len(taxa),
                "mean_flux_drop_pct": round(mean_drop, 4) if mean_drop is not None else "",
                "max_flux_drop_pct": round(max_drop, 4) if max_drop is not None else "",
                "top_keystone": top_org.get("taxon_name", ""),
                "top_keystone_drop": round(top_org.get("flux_drop_pct", 0), 4),
            }
        )

        for t in taxa:
            name = t.get("taxon_name", "unknown")
            drop = t.get("flux_drop_pct")
            org_total_seen[name] += 1
            if t.get("is_keystone") and drop is not None:
                org_keystone_count[name] += 1
                org_flux_drops[name].append(drop)

    # Write per-community CSV
    community_out = out_dir / "keystone_analysis.csv"
    with open(community_out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=community_rows[0].keys())
        w.writeheader()
        w.writerows(community_rows)
    print(f"Wrote {len(community_rows)} rows to {community_out}")

    # Per-organism summary
    org_rows: list[dict] = []
    n_communities = len(community_rows)
    for name in sorted(org_keystone_count, key=lambda n: -org_keystone_count[n]):
        drops = org_flux_drops[name]
        org_rows.append(
            {
                "organism": name,
                "times_keystone": org_keystone_count[name],
                "pct_of_communities": round(100 * org_keystone_count[name] / n_communities, 1),
                "mean_flux_drop_pct": round(statistics.mean(drops), 4) if drops else "",
                "max_flux_drop_pct": round(max(drops), 4) if drops else "",
                "min_flux_drop_pct": round(min(drops), 4) if drops else "",
            }
        )

    org_out = out_dir / "keystone_organism_summary.csv"
    with open(org_out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=org_rows[0].keys())
        w.writeheader()
        w.writerows(org_rows)
    print(f"Wrote {len(org_rows)} organisms to {org_out}")

    # Summary stats
    n_ks = [r["n_keystones"] for r in community_rows]
    mean_ks = statistics.mean(n_ks)
    max_ks = max(n_ks)
    min_ks = min(n_ks)
    all_drops = [t for rows in org_flux_drops.values() for t in rows]
    mean_drop_all = statistics.mean(all_drops) if all_drops else 0

    # Keystone count by land_use
    land_ks: dict[str, list[int]] = defaultdict(list)
    for r in community_rows:
        if r["land_use"]:
            land_ks[r["land_use"]].append(r["n_keystones"])

    print("\n=== KEYSTONE TAXA SUMMARY ===")
    print(f"Communities analyzed: {n_communities:,}")
    print(f"Mean keystone taxa per community: {mean_ks:.1f} (range {min_ks}–{max_ks})")
    print(f"Mean flux-drop-pct across all keystone assignments: {mean_drop_all:.3f}")
    print(f"\nTop organisms by keystone frequency:")
    for r in org_rows[:10]:
        print(
            f"  {r['organism']:15s} — keystone in {r['pct_of_communities']:5.1f}% of communities,"
            f" mean flux_drop {r['mean_flux_drop_pct']:.3f}"
        )
    print(f"\nKeystone count by land_use:")
    for lu, vals in sorted(land_ks.items(), key=lambda kv: -statistics.mean(kv[1])):
        print(f"  {lu:12s}: mean {statistics.mean(vals):.1f}")


if __name__ == "__main__":
    main()
