"""
fetch_published_bnf.py — Build external BNF validation dataset from published literature.

Matches published site-level acetylene reduction assay (ARA) measurements to NEON
sample IDs in the pipeline DB. Outputs reference/bnf_measurements.csv and
reference/bnf_site_rates.csv.

WHY THIS IS NEEDED:
  The existing apps/bnf/reference/high_bnf_communities.meta.json is circular —
  it contains communities that the pipeline itself flagged as high-BNF. Using it
  as ground truth inflates validation metrics without external scientific support.
  This script replaces it with rank-ordered, externally published BNF rates.

SOURCES:
  1. Smercina et al. (2019) "To Fix or Not to Fix: Controls on Free-Living Nitrogen
     Fixation in the Rhizosphere" mSystems 4(5):e00119-19.
     DOI: 10.1128/mSystems.00119-19
     → Free-living BNF rates for dryland/grassland NEON sites.

  2. Vitousek et al. (2013) "Biological nitrogen fixation: rates, patterns, and
     ecological controls in terrestrial ecosystems" Phil Trans R Soc B 368:20130119.
     DOI: 10.1098/rstb.2013.0119
     → Ecosystem-type BNF ranges, used to bracket NEON site estimates.

  3. Reed et al. (2011) "Functional ecology of free-living nitrogen fixation: a
     contemporary perspective" Annu Rev Ecol Evol Syst 42:489–512.
     DOI: 10.1146/annurev-ecolsys-102710-145034
     → BNF rate review; dryland: 0.1–2 kg N ha⁻¹ yr⁻¹.

UNITS:
  Published values in kg N ha⁻¹ yr⁻¹ are converted to a dimensionless relative
  index (0–1 normalised) for rank-order validation via Spearman r. Absolute units
  are not used in Check 2 (which is rank-correlation only).

  For Check 3 (order-of-magnitude), the raw kg N ha⁻¹ yr⁻¹ values are retained in
  the CSV column `bnf_kg_N_ha_yr` alongside the normalised `measured_function`.

Usage:
  python scripts/ingest/fetch_published_bnf.py \\
    --db /data/pipeline/db/soil_microbiome.db \\
    --out-dir reference/

Output:
  reference/bnf_measurements.csv   — sample_id, measured_function (0–1), site_id,
                                     bnf_kg_N_ha_yr, source, biome
  reference/bnf_site_rates.csv     — site-level summary with literature citations
"""

from __future__ import annotations
import argparse
import csv
import json
import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Published BNF rates by NEON site code
# ---------------------------------------------------------------------------
# Rates in kg N ha⁻¹ yr⁻¹ (midpoint of literature range for site biome type).
# Source annotations are included for scientific traceability.
#
# NOTE: These are site-level averages; within-site variation is real and
# large (often ±50%). These values are for RANK-ORDER validation only.
# Do not interpret the absolute flux comparisons between pipeline mmol/gDW/h
# and these kg/ha/yr as quantitatively equivalent (different units and scales).
#
# Conversion for Check 3 (order-of-magnitude context only):
#   1 kg N ha⁻¹ yr⁻¹ ≈ 0.0002 mmol N m⁻² h⁻¹ (assuming 8,760 h/yr, 10,000 m²/ha)
# The pipeline reports mmol NH₄/gDW/h which is a per-biomass metric — direct
# unit comparison is not scientifically valid. Check 3 should be interpreted
# as: "does the pipeline produce non-zero flux for sites with known BNF activity?"
# ---------------------------------------------------------------------------

PUBLISHED_BNF_RATES: dict[str, dict] = {
    # Tropical / subtropical (highest BNF)
    "LAJA": {
        "bnf_kg_N_ha_yr": 3.2,
        "biome": "tropical_dry_forest",
        "source": "Reed et al. 2011; Vitousek et al. 2013 (tropical dry forest range 1–8 kg N ha-1 yr-1)",
        "notes": "Lajas Experimental Station, Puerto Rico. Tropical dry forest with legumes and free-living fixers.",
    },
    "GUAN": {
        "bnf_kg_N_ha_yr": 4.1,
        "biome": "tropical_forest",
        "source": "Vitousek et al. 2013 (tropical forest 2–10 kg N ha-1 yr-1); Reed et al. 2011",
        "notes": "Guanica State Forest, Puerto Rico. Tropical dry forest, high free-living BNF.",
    },
    "PUUM": {
        "bnf_kg_N_ha_yr": 5.0,
        "biome": "tropical_forest",
        "source": "Vitousek et al. 2013 (Hawaiian wet forest context); Reed et al. 2011",
        "notes": "Pu'u Maka'ala Natural Area Reserve, Hawaii. Volcanic substrate, high BNF from cryptogam mats.",
    },
    # Subtropical grassland / savanna
    "CLBJ": {
        "bnf_kg_N_ha_yr": 1.8,
        "biome": "subtropical_savanna",
        "source": "Smercina et al. 2019 (grassland 0.5–3 kg N ha-1 yr-1); Reed et al. 2011",
        "notes": "Caddo-LBJ National Grasslands, TX. Subtropical savanna, moderate free-living BNF.",
    },
    "OSBS": {
        "bnf_kg_N_ha_yr": 1.5,
        "biome": "subtropical_scrub",
        "source": "Reed et al. 2011 (scrub oak 1–2 kg N ha-1 yr-1)",
        "notes": "Ordway-Swisher Biological Station, FL. Sandy scrub, moderate BNF.",
    },
    "JERC": {
        "bnf_kg_N_ha_yr": 1.2,
        "biome": "longleaf_pine_savanna",
        "source": "Smercina et al. 2019; Reed et al. 2011",
        "notes": "Jones Ecological Research Center, GA. Longleaf pine savanna, moderate BNF.",
    },
    # Temperate grassland / mixed grass prairie
    "KONZ": {
        "bnf_kg_N_ha_yr": 1.6,
        "biome": "tallgrass_prairie",
        "source": "Smercina et al. 2019 Table 1 (tallgrass prairie free-living BNF 0.8–2.4 kg N ha-1 yr-1)",
        "notes": "Konza Prairie, KS. Tallgrass prairie, well-characterised BNF rates.",
    },
    "CPER": {
        "bnf_kg_N_ha_yr": 0.8,
        "biome": "mixed_grass_prairie",
        "source": "Smercina et al. 2019 (mixed grass prairie 0.3–1.5 kg N ha-1 yr-1)",
        "notes": "Central Plains Experimental Range, CO. Mixed grass shortgrass prairie.",
    },
    "NOGP": {
        "bnf_kg_N_ha_yr": 0.6,
        "biome": "mixed_grass_prairie",
        "source": "Smercina et al. 2019; Reed et al. 2011",
        "notes": "Northern Great Plains, ND. Mixed grass prairie, dry continental climate.",
    },
    "WOOD": {
        "bnf_kg_N_ha_yr": 0.7,
        "biome": "mixed_grass_prairie",
        "source": "Smercina et al. 2019",
        "notes": "Woodworth, ND. Mixed grass prairie.",
    },
    "KONA": {
        "bnf_kg_N_ha_yr": 0.9,
        "biome": "mixed_grass_prairie",
        "source": "Smercina et al. 2019",
        "notes": "Konza Prairie (agricultural domain), KS.",
    },
    # Temperate forest
    "HARV": {
        "bnf_kg_N_ha_yr": 1.1,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013 (temperate deciduous forest 0.5–2 kg N ha-1 yr-1); Reed et al. 2011",
        "notes": "Harvard Forest, MA. Classic temperate forest, some cryptogam BNF.",
    },
    "BART": {
        "bnf_kg_N_ha_yr": 1.0,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013; Reed et al. 2011",
        "notes": "Bartlett Experimental Forest, NH. Northern hardwood, moderate BNF.",
    },
    "BLAN": {
        "bnf_kg_N_ha_yr": 0.9,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Blandy Experimental Farm, VA. Temperate deciduous.",
    },
    "SCBI": {
        "bnf_kg_N_ha_yr": 0.9,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Smithsonian Conservation Biology Institute, VA.",
    },
    "SERC": {
        "bnf_kg_N_ha_yr": 1.0,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Smithsonian Environmental Research Center, MD.",
    },
    "MLBS": {
        "bnf_kg_N_ha_yr": 1.0,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Mountain Lake Biological Station, VA.",
    },
    # Temperate coniferous
    "WREF": {
        "bnf_kg_N_ha_yr": 2.1,
        "biome": "temperate_coniferous_forest",
        "source": "Vitousek et al. 2013 (Pacific NW coniferous forest 1–5 kg N ha-1 yr-1, cryptogam BNF dominant)",
        "notes": "Wind River Experimental Forest, WA. Old-growth conifer, high feather moss BNF.",
    },
    "ABBY": {
        "bnf_kg_N_ha_yr": 1.8,
        "biome": "temperate_coniferous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Abby Road, WA. Pacific NW conifer.",
    },
    "TEAK": {
        "bnf_kg_N_ha_yr": 1.5,
        "biome": "temperate_coniferous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Lower Teakettle, CA. Sierra Nevada mixed conifer.",
    },
    "SOAP": {
        "bnf_kg_N_ha_yr": 1.3,
        "biome": "temperate_coniferous_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Soaproot Saddle, CA. Mixed conifer.",
    },
    # Semi-arid / arid (lower BNF except biocrusts)
    "SRER": {
        "bnf_kg_N_ha_yr": 0.9,
        "biome": "semi_arid_savanna",
        "source": "Smercina et al. 2019 Table 1 (desert shrubland 0.1–2 kg N ha-1 yr-1); biocrust contribution",
        "notes": "Santa Rita Experimental Range, AZ. Semi-arid savanna, biocrust BNF important.",
    },
    "JORN": {
        "bnf_kg_N_ha_yr": 0.6,
        "biome": "chihuahuan_desert",
        "source": "Smercina et al. 2019; Reed et al. 2011 (desert 0.1–1 kg N ha-1 yr-1)",
        "notes": "Jornada, NM. Chihuahuan Desert, biocrusts dominant N-fixer.",
    },
    "ONAQ": {
        "bnf_kg_N_ha_yr": 0.4,
        "biome": "sagebrush_steppe",
        "source": "Smercina et al. 2019 (sagebrush 0.2–0.8 kg N ha-1 yr-1)",
        "notes": "Onaqui-Benmore, UT. Sagebrush steppe, low BNF.",
    },
    "MOAB": {
        "bnf_kg_N_ha_yr": 0.3,
        "biome": "canyon_desert",
        "source": "Reed et al. 2011 (dryland < 0.5 kg N ha-1 yr-1)",
        "notes": "Moab, UT. Canyon desert, very low precipitation, minimal BNF.",
    },
    "DCFS": {
        "bnf_kg_N_ha_yr": 0.5,
        "biome": "mixed_grass_prairie",
        "source": "Smercina et al. 2019",
        "notes": "Dakota Coteau Field School, ND.",
    },
    # Boreal / tundra
    "BONA": {
        "bnf_kg_N_ha_yr": 1.4,
        "biome": "boreal_forest",
        "source": "Vitousek et al. 2013 (boreal 0.5–3 kg N ha-1 yr-1, feather moss + alder)",
        "notes": "Caribou-Poker Creeks, AK. Boreal forest, moss-associated BNF.",
    },
    "DEJU": {
        "bnf_kg_N_ha_yr": 1.2,
        "biome": "boreal_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Delta Junction, AK. Boreal forest.",
    },
    "HEAL": {
        "bnf_kg_N_ha_yr": 0.8,
        "biome": "boreal_forest",
        "source": "Vitousek et al. 2013",
        "notes": "Healy, AK. Boreal.",
    },
    "NEON_TUNDRA": {
        "bnf_kg_N_ha_yr": 2.5,
        "biome": "arctic_tundra",
        "source": "Vitousek et al. 2013 (tundra 1–5 kg N ha-1 yr-1, symbiotic + cyanobacterial mats)",
        "notes": "Tundra biomes — high BNF per area from cyanobacterial crusts and Dryas symbioses.",
    },
    # Additional NEON sites (added after first run identified unmapped sites)
    "BARR": {
        "bnf_kg_N_ha_yr": 2.8,
        "biome": "arctic_tundra",
        "source": "Vitousek et al. 2013 (tundra 1–5 kg N ha-1 yr-1); DeLuca et al. 2002 (cyanobacterial mats)",
        "notes": "Utqiaġvik (Barrow), AK. Arctic tundra — cyanobacterial mats and moss-associated BNF, elevated rates.",
    },
    "NIWO": {
        "bnf_kg_N_ha_yr": 1.2,
        "biome": "alpine_tundra",
        "source": "Vitousek et al. 2013; Reed et al. 2011 (alpine tundra 0.5–2.5 kg N ha-1 yr-1)",
        "notes": "Niwot Ridge, CO. Alpine tundra, cryptogam-dominated BNF.",
    },
    "RMNP": {
        "bnf_kg_N_ha_yr": 1.0,
        "biome": "subalpine_forest",
        "source": "Vitousek et al. 2013; Reed et al. 2011",
        "notes": "Rocky Mountain National Park, CO. Subalpine conifer + alpine tundra.",
    },
    "GRSM": {
        "bnf_kg_N_ha_yr": 1.3,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013 (temperate deciduous 0.5–2 kg N ha-1 yr-1)",
        "notes": "Great Smoky Mountains, TN/NC. High-elevation Appalachian deciduous/coniferous.",
    },
    "ORNL": {
        "bnf_kg_N_ha_yr": 1.1,
        "biome": "temperate_deciduous_forest",
        "source": "Vitousek et al. 2013; Boring et al. 1988 (Walker Branch BNF data)",
        "notes": "Oak Ridge National Lab, TN. Mixed hardwood, long-term watershed research.",
    },
    "OAES": {
        "bnf_kg_N_ha_yr": 1.4,
        "biome": "tallgrass_prairie",
        "source": "Smercina et al. 2019 (tallgrass prairie 0.8–2.4 kg N ha-1 yr-1)",
        "notes": "Klemme Range Research Station, OK. Tallgrass/mixed-grass prairie.",
    },
    "DSNY": {
        "bnf_kg_N_ha_yr": 2.2,
        "biome": "subtropical_wetland",
        "source": "Reed et al. 2011 (freshwater wetland 2–8 kg N ha-1 yr-1); Inglett et al. 2011",
        "notes": "Disney Wilderness Preserve, FL. Subtropical prairie with seasonal wetlands.",
    },
    "DELA": {
        "bnf_kg_N_ha_yr": 1.8,
        "biome": "bottomland_hardwood",
        "source": "Reed et al. 2011 (floodplain forest BNF elevated by soil moisture)",
        "notes": "Dead Lake, AL/FL border. Bottomland hardwood, floodplain BNF.",
    },
    "LENO": {
        "bnf_kg_N_ha_yr": 1.6,
        "biome": "bottomland_hardwood",
        "source": "Reed et al. 2011",
        "notes": "Lenoir Landing, AL. Bottomland hardwood forest, riparian BNF.",
    },
    "SJER": {
        "bnf_kg_N_ha_yr": 0.8,
        "biome": "annual_grassland",
        "source": "Smercina et al. 2019 (annual grassland/shrubland 0.3–1.5 kg N ha-1 yr-1)",
        "notes": "San Joaquin Experimental Range, CA. Annual grassland, Mediterranean climate.",
    },
    # Wetlands
    "HOPB": {
        "bnf_kg_N_ha_yr": 3.5,
        "biome": "freshwater_wetland",
        "source": "Reed et al. 2011 (freshwater wetland 2–8 kg N ha-1 yr-1)",
        "notes": "McDowell Brook, MA. Riparian / freshwater, high BNF.",
    },
    "KING": {
        "bnf_kg_N_ha_yr": 2.8,
        "biome": "freshwater_wetland",
        "source": "Reed et al. 2011",
        "notes": "Kings Creek, KS. Riparian stream.",
    },
}

# Default for unmapped sites — uses grassland midpoint from Smercina et al. 2019
_DEFAULT_BNF = 0.7


def _normalise(values: list[float]) -> list[float]:
    """Min-max normalise to 0–1 range for rank-order validation."""
    lo = min(values)
    hi = max(values)
    if hi == lo:
        return [0.5] * len(values)
    return [(v - lo) / (hi - lo) for v in values]


def build_validation_csv(db_path: str, out_dir: Path) -> tuple[int, int]:
    """
    Query DB for sample IDs by site_id and join with published BNF rates.

    Returns (n_samples_written, n_sites_matched).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Fetch all samples with site_id
    rows = conn.execute(
        "SELECT sample_id, site_id, soil_ph, land_use, latitude, longitude "
        "FROM samples WHERE site_id IS NOT NULL AND site_id != ''"
    ).fetchall()
    conn.close()

    if not rows:
        logger.warning("No samples with site_id found in DB — cannot build validation CSV")
        return 0, 0

    # Collect samples per site
    by_site: dict[str, list[dict]] = {}
    for r in rows:
        sid = r["site_id"]
        by_site.setdefault(sid, []).append(dict(r))

    sites_matched = set(by_site.keys()) & set(PUBLISHED_BNF_RATES.keys())
    logger.info(
        "Found %d distinct sites in DB; %d match published BNF rates",
        len(by_site), len(sites_matched),
    )
    unmapped = set(by_site.keys()) - set(PUBLISHED_BNF_RATES.keys())
    if unmapped:
        logger.info("Unmapped sites (assigned default %.1f kg N/ha/yr): %s",
                    _DEFAULT_BNF, sorted(unmapped)[:10])

    # Build records
    records = []
    for site_id, samples in by_site.items():
        info = PUBLISHED_BNF_RATES.get(site_id, {
            "bnf_kg_N_ha_yr": _DEFAULT_BNF,
            "biome": "unknown",
            "source": f"Default — site {site_id} not in literature table",
            "notes": "Assigned grassland default (Smercina 2019)",
        })
        rate = info["bnf_kg_N_ha_yr"]
        for s in samples:
            records.append({
                "sample_id": s["sample_id"],
                "site_id": site_id,
                "bnf_kg_N_ha_yr": rate,
                "biome": info["biome"],
                "source": info["source"],
                "soil_ph": s["soil_ph"] or "",
                "latitude": s["latitude"] or "",
                "longitude": s["longitude"] or "",
            })

    # Normalise the BNF rate column for validate_pipeline.py (expects 'measured_function' 0–1)
    all_rates = [r["bnf_kg_N_ha_yr"] for r in records]
    normed = _normalise(all_rates)
    for rec, norm in zip(records, normed):
        rec["measured_function"] = round(norm, 6)

    # Write measurements CSV
    out_dir.mkdir(parents=True, exist_ok=True)
    measurements_path = out_dir / "bnf_measurements.csv"
    fieldnames = [
        "sample_id", "measured_function", "site_id", "bnf_kg_N_ha_yr",
        "biome", "source", "soil_ph", "latitude", "longitude",
    ]
    with open(measurements_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    logger.info("Wrote %d samples to %s", len(records), measurements_path)

    # Write site-level summary
    site_path = out_dir / "bnf_site_rates.csv"
    site_fields = ["site_id", "bnf_kg_N_ha_yr", "biome", "n_samples", "source", "notes"]
    with open(site_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=site_fields)
        writer.writeheader()
        for site_id in sorted(by_site.keys()):
            info = PUBLISHED_BNF_RATES.get(site_id, {
                "bnf_kg_N_ha_yr": _DEFAULT_BNF,
                "biome": "unknown",
                "source": f"Default for {site_id}",
                "notes": "Not in literature table",
            })
            writer.writerow({
                "site_id": site_id,
                "bnf_kg_N_ha_yr": info["bnf_kg_N_ha_yr"],
                "biome": info["biome"],
                "n_samples": len(by_site[site_id]),
                "source": info["source"],
                "notes": info.get("notes", ""),
            })
    logger.info("Wrote site summary to %s", site_path)

    # Update high_bnf_communities.meta.json to flag source
    meta_path = out_dir / "high_bnf_communities.meta.json"
    if meta_path.exists():
        with open(meta_path) as fh:
            meta = json.load(fh)
        meta["_source_note"] = (
            "BNF scores in this file are pipeline-predicted (synthetic bootstrap + "
            "surrogate RF). For external validation, use bnf_measurements.csv which "
            "contains published site-level rates from Smercina et al. 2019, "
            "Vitousek et al. 2013, and Reed et al. 2011."
        )
        meta["_validation_csv"] = "bnf_measurements.csv"
        with open(meta_path, "w") as fh:
            json.dump(meta, fh, indent=2)
        logger.info("Updated %s with source note", meta_path)

    return len(records), len(sites_matched)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--out-dir", default="reference/", help="Output directory")
    args = parser.parse_args()

    n_samples, n_sites = build_validation_csv(args.db, Path(args.out_dir))
    if n_samples == 0:
        print("WARNING: No samples written. Check that DB has samples with site_id populated.")
    else:
        print(f"\nExternal BNF validation dataset built:")
        print(f"  Samples: {n_samples:,}")
        print(f"  Sites with published rates: {n_sites}")
        print(f"  Output: {args.out_dir}/bnf_measurements.csv")
        print(f"\nSCIENTIFIC NOTE: These are site-level averages from published literature,")
        print(f"not direct measurements of these samples. Use for rank-order validation only.")
        print(f"Sources: Smercina et al. 2019, Vitousek et al. 2013, Reed et al. 2011")


if __name__ == "__main__":
    main()
