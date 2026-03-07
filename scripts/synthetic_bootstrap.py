"""
scripts/synthetic_bootstrap.py — Ecologically-parameterized synthetic community
bootstrap for the soil microbiome pipeline.

Generates N realistic communities using known soil ecology priors
(Fierer & Jackson 2006, Lauber et al. 2009, Barberan et al. 2012),
computes diversity metrics, assigns BNF labels, trains FunctionalPredictor,
builds reference BIOM, and re-scores T0.25 rows with the real model.

Scientific grounding:
  - Phylum ratios parameterized by soil pH, organic matter, land use
    from published gradient studies across NEON, LTER, and EMP sites
  - BNF proxy label: function of Proteobacteria abundance, pH proximity
    to optimum (~6.5), organic matter, and sampling fraction
  - CLR-transformed phylum features + standardized env features for ML

Usage:
  python scripts/synthetic_bootstrap.py \\
      --n-communities 100000 \\
      --workers 36 \\
      --db /data/pipeline/db/soil_microbiome.db \\
      --model-out models/functional_predictor.joblib \\
      --biom-out reference/high_bnf_communities.biom
"""

from __future__ import annotations

import json
import logging
import math
import os
import random
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import numpy as np
import typer
import yaml

_PROJ_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJ_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJ_ROOT))

from db_utils import _db_connect  # noqa: E402

logger = logging.getLogger(__name__)
app = typer.Typer(help="Generate synthetic communities and train FunctionalPredictor", add_completion=False, invoke_without_command=True)

# ---------------------------------------------------------------------------
# Phyla in the model (12 major bacterial/archaeal phyla found in soil)
# ---------------------------------------------------------------------------
PHYLA = [
    "Proteobacteria",
    "Actinobacteria",
    "Acidobacteria",
    "Firmicutes",
    "Bacteroidetes",
    "Verrucomicrobia",
    "Planctomycetes",
    "Chloroflexi",
    "Gemmatimonadetes",
    "Nitrospirae",
    "Cyanobacteria",
    "Thaumarchaeota",
]

# Key BNF-capable genera (for top_genera field)
BNF_GENERA_POOL = [
    "Bradyrhizobium", "Rhizobium", "Mesorhizobium", "Sinorhizobium",
    "Azospirillum", "Azotobacter", "Frankia", "Azoarcus",
    "Herbaspirillum", "Gluconacetobacter", "Burkholderia",
]
NON_BNF_GENERA_POOL = [
    "Bacillus", "Streptomyces", "Nocardia", "Arthrobacter",
    "Acidobacterium", "Ellin", "Gemmata", "Planctomyces",
    "Nitrospira", "Nitrosomonas", "Pseudomonas", "Sphingomonas",
    "Caulobacter", "Variovorax", "Burkholderia_non_bnf",
]

# NEON sites with approximate environmental characteristics
SITE_ENV = {
    "STER": {"biome": "cropland", "lat": 40.46, "lon": -104.75, "mean_ph": 7.1, "mean_om": 2.1},
    "CPER": {"biome": "grassland", "lat": 40.82, "lon": -104.75, "mean_ph": 6.9, "mean_om": 1.8},
    "NOGP": {"biome": "grassland", "lat": 46.77, "lon": -100.92, "mean_ph": 6.7, "mean_om": 2.4},
    "DCFS": {"biome": "wetland",   "lat": 47.16, "lon": -99.11,  "mean_ph": 7.3, "mean_om": 3.8},
    "KONA": {"biome": "grassland", "lat": 39.11, "lon": -96.61,  "mean_ph": 6.2, "mean_om": 3.1},
    "OAES": {"biome": "grassland", "lat": 35.41, "lon": -99.06,  "mean_ph": 6.4, "mean_om": 1.4},
    "CLBJ": {"biome": "rangeland", "lat": 33.40, "lon": -97.57,  "mean_ph": 6.8, "mean_om": 1.6},
    "WOOD": {"biome": "wetland",   "lat": 47.13, "lon": -99.24,  "mean_ph": 7.5, "mean_om": 4.2},
    "LENO": {"biome": "forest",    "lat": 31.85, "lon": -88.16,  "mean_ph": 5.5, "mean_om": 2.9},
    "UKFS": {"biome": "forest",    "lat": 39.04, "lon": -95.19,  "mean_ph": 5.8, "mean_om": 3.4},
    "ORNL": {"biome": "forest",    "lat": 35.96, "lon": -84.28,  "mean_ph": 5.2, "mean_om": 2.7},
    "HARV": {"biome": "forest",    "lat": 42.54, "lon": -72.17,  "mean_ph": 4.8, "mean_om": 4.1},
    "SCBI": {"biome": "forest",    "lat": 38.89, "lon": -78.14,  "mean_ph": 5.1, "mean_om": 3.2},
    "UNDE": {"biome": "forest",    "lat": 46.23, "lon": -89.54,  "mean_ph": 4.6, "mean_om": 3.9},
    "KONZ": {"biome": "grassland", "lat": 39.10, "lon": -96.56,  "mean_ph": 6.3, "mean_om": 3.0},
}

LAND_USE_ENCODE = {"cropland": 1.0, "grassland": 0.7, "rangeland": 0.6,
                   "wetland": 0.5, "forest": 0.2}
FRACTION_BNF_BOOST = {"rhizosphere": 0.25, "bulk": 0.0, "endosphere": 0.15, "litter": -0.1}


# ---------------------------------------------------------------------------
# Ecological simulation of phylum profiles
# ---------------------------------------------------------------------------

def _phylum_profile(ph: float, organic_matter: float, clay_pct: float,
                    land_use: str, fraction: str, rng: np.random.Generator) -> dict[str, float]:
    """
    Generate a phylum-level relative abundance profile using empirical
    pH and organic matter gradients from published NEON / EMP studies.

    Key relationships (Fierer & Jackson 2006, Lauber et al. 2009):
      Acidobacteria:    decreases ~8%/pH-unit from pH 4 to 8
      Actinobacteria:   increases ~6%/pH-unit from pH 4 to 8
      Proteobacteria:   bell-shaped, peaks at pH 6-7
      Firmicutes:       relatively flat, slight increase with OM
      Bacteroidetes:    moderate, increases with OM and neutral pH
    """
    # Mean abundances as function of pH (normalized to start at pH 5)
    ph_c = max(4.0, min(9.0, ph))  # clamp

    # Acidobacteria: 45% at pH 4 → 5% at pH 8
    acid_mean = 0.50 - 0.10 * (ph_c - 4.0)  # decreasing
    acid_mean = max(0.03, min(0.55, acid_mean))

    # Actinobacteria: 8% at pH 4 → 35% at pH 8
    actin_mean = 0.06 + 0.07 * (ph_c - 4.0)
    actin_mean = max(0.05, min(0.40, actin_mean))

    # Proteobacteria: bell peak at pH 6.5
    proto_mean = 0.18 + 0.08 * math.exp(-0.5 * ((ph_c - 6.5) / 1.0) ** 2)
    if land_use in ("cropland", "rangeland"):
        proto_mean += 0.05   # agricultural soils enriched
    proto_mean = max(0.10, min(0.40, proto_mean))

    # Rest: fill the remaining fraction with noise
    firmicutes_mean = 0.05 + 0.02 * organic_matter / 4.0
    bacteroidetes_mean = 0.04 + 0.01 * (ph_c - 4.0) + 0.01 * organic_matter
    verrucomicrobia_mean = 0.03 + 0.01 * rng.random()
    planctomycetes_mean = 0.025 + 0.005 * rng.random()
    chloroflexi_mean = 0.03 - 0.004 * (ph_c - 4.0) + 0.005 * rng.random()
    chloroflexi_mean = max(0.005, chloroflexi_mean)
    gemmatimonadetes_mean = 0.015 + 0.003 * (ph_c - 4.0)
    nitrospirae_mean = 0.012 + 0.001 * rng.random()
    cyanobacteria_mean = 0.005 + 0.002 * rng.random()
    thaumarchaeota_mean = 0.02 + 0.005 * (8.0 - ph_c)  # AOA prefer acidic-neutral

    raw = np.array([
        proto_mean,       # Proteobacteria
        actin_mean,       # Actinobacteria
        acid_mean,        # Acidobacteria
        firmicutes_mean,  # Firmicutes
        bacteroidetes_mean,
        verrucomicrobia_mean,
        planctomycetes_mean,
        chloroflexi_mean,
        gemmatimonadetes_mean,
        nitrospirae_mean,
        cyanobacteria_mean,
        thaumarchaeota_mean,
    ])

    # Add Dirichlet noise to simulate natural variation (concentration ≈ 10)
    concentration = 10.0
    alpha = raw * concentration
    sample = rng.dirichlet(alpha)

    return {phylum: float(v) for phylum, v in zip(PHYLA, sample)}


def _bnf_label(phylum_profile: dict, ph: float, organic_matter: float,
               land_use: str, fraction: str) -> float:
    """
    Compute a continuous BNF activity score in [0, 1].

    Ecological basis:
      - Proteobacteria abundance is the primary signal (contains Rhizobiales)
      - pH proximity to optimum (6.0-7.0 for most diazotrophs)
      - Organic matter drives C:N, which stimulates N fixation
      - Rhizosphere and cropland (legume) association boosts score
    """
    proto = phylum_profile.get("Proteobacteria", 0.0)

    # pH optimality for diazotrophs (peaks at 6.5)
    ph_factor = math.exp(-0.5 * ((ph - 6.5) / 1.2) ** 2)

    # Organic matter factor (0-1, saturates at 5%)
    om_factor = min(organic_matter / 5.0, 1.0)

    # Land use / fraction boost
    lu_boost = LAND_USE_ENCODE.get(land_use, 0.4)
    frac_boost = FRACTION_BNF_BOOST.get(fraction, 0.0)

    # Composite score
    score = (
        0.45 * proto / 0.35  # normalise proteobacteria (typical max ~35%)
        + 0.25 * ph_factor
        + 0.15 * om_factor
        + 0.10 * lu_boost
        + 0.05 * max(0.0, frac_boost)
    )
    return float(min(1.0, max(0.0, score)))


def _top_genera(phylum_profile: dict, bnf_score: float,
                rng: np.random.Generator) -> list[dict]:
    """Generate a plausible top-20 genera list from phylum profile + BNF score."""
    n_bnf = int(round(bnf_score * 8))  # 0-8 BNF genera
    n_other = 12 - n_bnf
    selected_bnf   = rng.choice(BNF_GENERA_POOL,   size=n_bnf,   replace=False).tolist()
    selected_other = rng.choice(NON_BNF_GENERA_POOL, size=n_other, replace=False).tolist()
    all_genera = selected_bnf + selected_other
    rng.shuffle(all_genera)
    # Assign random rel_abundances summing to 1
    abund = rng.dirichlet(np.ones(len(all_genera)))
    return [{"name": g, "rel_abundance": float(a)} for g, a in zip(all_genera, abund)]


def _generate_one(seed: int, site_code: str, fraction: str) -> dict:
    """Generate a single synthetic community. Pure function — process-safe."""
    rng = np.random.default_rng(seed)
    env = SITE_ENV[site_code]

    ph = float(rng.normal(env["mean_ph"], 0.4))
    ph = max(4.0, min(9.5, ph))
    om = float(rng.normal(env["mean_om"], 0.5))
    om = max(0.2, min(8.0, om))
    clay_pct = float(rng.uniform(10, 45))
    temp_c = float(rng.normal(12 - abs(env["lat"] - 38) * 0.4, 2.5))
    precip_mm = float(rng.normal(650 - abs(env["lon"] + 95) * 5, 80))
    precip_mm = max(200.0, precip_mm)
    land_use = env["biome"]

    profile = _phylum_profile(ph, om, clay_pct, land_use, fraction, rng)
    bnf = _bnf_label(profile, ph, om, land_use, fraction)
    genera = _top_genera(profile, bnf, rng)

    # Shannon diversity from phylum profile
    vals = np.array(list(profile.values()))
    vals = vals[vals > 0]
    shannon = float(-np.sum(vals * np.log(vals)))
    simpson = float(1 - np.sum(vals ** 2))
    observed_otus = int(rng.integers(300, 1200))

    has_nifh = bnf > 0.45 and rng.random() < 0.85
    nifh_abundance = float(bnf * 0.08 * rng.uniform(0.5, 1.5)) if has_nifh else 0.0

    sample_id = f"synth_{site_code}_{seed:010d}"

    return {
        "sample_id":        sample_id,
        "site_code":        site_code,
        "land_use":         land_use,
        "sampling_fraction": fraction,
        "soil_ph":          ph,
        "organic_matter_pct": om,
        "clay_pct":         clay_pct,
        "temperature_c":    temp_c,
        "precipitation_mm": precip_mm,
        "latitude":         env["lat"] + float(rng.normal(0, 0.05)),
        "longitude":        env["lon"] + float(rng.normal(0, 0.05)),
        "phylum_profile":   profile,
        "top_genera":       genera,
        "shannon_diversity": shannon,
        "simpson_diversity": simpson,
        "observed_otus":    observed_otus,
        "pielou_evenness":  shannon / math.log(max(2, observed_otus)),
        "has_nifh":         bool(has_nifh),
        "nifh_abundance":   nifh_abundance,
        "bnf_score":        bnf,
    }


def _generate_batch(args: list[tuple]) -> list[dict]:
    """Worker: generate a batch of communities. Returns list of result dicts."""
    return [_generate_one(seed, site_code, fraction) for seed, site_code, fraction in args]


# ---------------------------------------------------------------------------
# DB insertion helpers
# ---------------------------------------------------------------------------

def _insert_batch(db_path: str, communities: list[dict]) -> int:
    """Insert a batch of synthetic communities into the DB. Returns n inserted."""
    conn = _db_connect(db_path, timeout=60)
    conn.execute("PRAGMA synchronous=OFF")  # write path; restored before commit
    inserted = 0
    try:
        for c in communities:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO samples
                       (sample_id, source, site_id, latitude, longitude,
                        soil_ph, organic_matter_pct, clay_pct, temperature_c,
                        precipitation_mm, land_use, sampling_fraction)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (c["sample_id"], "synthetic", c["site_code"],
                     c["latitude"], c["longitude"],
                     c["soil_ph"], c["organic_matter_pct"], c["clay_pct"],
                     c["temperature_c"], c["precipitation_mm"],
                     c["land_use"], c["sampling_fraction"]),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO communities
                       (sample_id, phylum_profile, top_genera,
                        shannon_diversity, simpson_diversity,
                        observed_otus, pielou_evenness,
                        has_nifh)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (c["sample_id"],
                     json.dumps(c["phylum_profile"]),
                     json.dumps(c["top_genera"]),
                     c["shannon_diversity"],
                     c["simpson_diversity"],
                     c["observed_otus"],
                     c["pielou_evenness"],
                     1 if c["has_nifh"] else 0),
                )
                row = conn.execute(
                    "SELECT community_id FROM communities WHERE sample_id=? ORDER BY community_id DESC LIMIT 1",
                    (c["sample_id"],)
                ).fetchone()
                if row:
                    cid = row[0]
                    conn.execute(
                        """INSERT OR IGNORE INTO runs
                           (sample_id, community_id, t0_pass, t025_pass,
                            t025_function_score, t025_uncertainty)
                           VALUES (?,?,1,1,?,0.05)""",
                        (c["sample_id"], cid, c["bnf_score"]),
                    )
                inserted += 1
            except Exception as exc:
                logger.debug("Insert failed for %s: %s", c["sample_id"], exc)
        conn.execute("PRAGMA synchronous=NORMAL")  # restore safe default
        conn.commit()
    finally:
        conn.close()
    return inserted


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

def _train_predictor(db_path: str, model_out: Path, n_max: int = 80_000) -> None:
    """Train FunctionalPredictor on synthetic communities in the DB."""
    from compute.functional_predictor import FunctionalPredictor, clr_transform

    logger.info("Loading training data from DB …")
    conn = _db_connect(db_path)
    rows = conn.execute(
        """SELECT c.phylum_profile, s.soil_ph, s.organic_matter_pct,
                  s.clay_pct, s.temperature_c, s.precipitation_mm,
                  r.t025_function_score
           FROM runs r
           JOIN communities c ON r.community_id = c.community_id
           JOIN samples s ON r.sample_id = s.sample_id
           WHERE r.t025_function_score IS NOT NULL
             AND s.source = 'synthetic'
           ORDER BY RANDOM()
           LIMIT ?""",
        (n_max,)
    ).fetchall()
    conn.close()

    if len(rows) < 500:
        logger.warning("Only %d training rows — need at least 500. Skipping.", len(rows))
        return

    logger.info("Training on %d samples …", len(rows))

    X_rows = []
    y = []
    feature_names = PHYLA + ["soil_ph", "organic_matter_pct", "clay_pct",
                              "temperature_c", "precipitation_mm"]

    for row in rows:
        try:
            profile = json.loads(row[0] or "{}")
            phylum_vec = [profile.get(p, 0.0) for p in PHYLA]
            env_vec = [
                float(row[1] or 6.5),   # soil_ph
                float(row[2] or 2.0),   # organic_matter_pct
                float(row[3] or 25.0),  # clay_pct
                float(row[4] or 12.0),  # temperature_c
                float(row[5] or 600.0), # precipitation_mm
            ]
            X_rows.append(phylum_vec + env_vec)
            y.append(float(row[6]))
        except Exception:
            continue

    X = np.array(X_rows, dtype=float)
    y = np.array(y, dtype=float)

    # Separate OTU and env features; CLR only on OTU columns
    X_otu = X[:, :len(PHYLA)]
    X_env = X[:, len(PHYLA):]
    X_otu_clr = clr_transform(X_otu)
    X_full = np.hstack([X_otu_clr, X_env])

    predictor = FunctionalPredictor(model_type="random_forest")
    predictor.train(X_full, y, feature_names=feature_names, apply_clr=False)

    model_out.parent.mkdir(parents=True, exist_ok=True)
    predictor.save(str(model_out))
    logger.info("Model saved → %s", model_out)


# ---------------------------------------------------------------------------
# Reference BIOM construction
# ---------------------------------------------------------------------------

def _build_reference_biom(db_path: str, biom_out: Path, min_bnf: float = 0.65,
                           n_max: int = 5000) -> None:
    """
    Build a reference OTU table in TSV format (rows=phyla, cols=samples).

    CommunitySimilaritySearch._load_index_tsv_fallback() expects:
      feature_id\\tref_1\\tref_2\\t...\\n
      Proteobacteria\\t0.35\\t0.22\\t...\\n
      Actinobacteria\\t0.12\\t0.28\\t...\\n
      ...

    A paired JSON sidecar (<biom_out>.meta.json) stores bnf_scores per column.
    """
    logger.info("Building reference OTU TSV (min_bnf=%.2f) …", min_bnf)
    conn = _db_connect(db_path)
    rows = conn.execute(
        """SELECT r.community_id, c.phylum_profile, r.t025_function_score
           FROM runs r
           JOIN communities c ON r.community_id = c.community_id
           WHERE r.t025_function_score >= ?
             AND c.phylum_profile IS NOT NULL
           ORDER BY r.t025_function_score DESC
           LIMIT ?""",
        (min_bnf, n_max)
    ).fetchall()
    conn.close()

    if not rows:
        logger.warning("No rows above min_bnf=%.2f — skipping reference BIOM build", min_bnf)
        return

    # Collect per-column abundances keyed by phylum
    col_ids: list[str] = []
    col_profiles: list[dict] = []
    bnf_scores: dict[str, float] = {}

    for cid, profile_json, bnf in rows:
        profile = json.loads(profile_json or "{}")
        col_id = f"ref_{cid}"
        col_ids.append(col_id)
        col_profiles.append(profile)
        bnf_scores[col_id] = float(bnf)

    biom_out.parent.mkdir(parents=True, exist_ok=True)

    # Write TSV: feature_id header + one row per phylum
    with open(biom_out, "w", encoding="utf-8") as fh:
        # Header row
        fh.write("feature_id\t" + "\t".join(col_ids) + "\n")
        # Data rows
        for phylum in PHYLA:
            vals = [f"{prof.get(phylum, 0.0):.6f}" for prof in col_profiles]
            fh.write(phylum + "\t" + "\t".join(vals) + "\n")

    # Sidecar metadata (bnf scores) for downstream use
    meta_path = biom_out.with_suffix(".meta.json")
    meta_path.write_text(json.dumps({"bnf_scores": bnf_scores}, indent=2))

    logger.info("Reference OTU TSV written: %d samples, %d phyla → %s",
                len(col_ids), len(PHYLA), biom_out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    n_communities:  int           = typer.Option(100_000, "--n-communities", "-n"),
    workers:        int           = typer.Option(36,      "--workers",       "-w"),
    db_path:        Path          = typer.Option(Path("/data/pipeline/db/soil_microbiome.db"), "--db"),
    model_out:      Path          = typer.Option(Path("models/functional_predictor.joblib"),   "--model-out"),
    biom_out:       Path          = typer.Option(Path("reference/high_bnf_communities.biom"),  "--biom-out"),
    batch_size:     int           = typer.Option(2000,    "--batch-size"),
    min_bnf_biom:   float         = typer.Option(0.65,    "--min-bnf-biom"),
    log_path:       Optional[Path] = typer.Option(Path("/var/log/pipeline/synthetic_bootstrap.log"), "--log"),
):
    """Generate synthetic communities, train FunctionalPredictor, build reference BIOM."""
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(str(log_path)))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=handlers, force=True
    )

    site_codes = list(SITE_ENV.keys())
    fractions  = ["bulk", "rhizosphere", "bulk", "bulk"]  # 50% bulk, 25% rhizo

    logger.info("=== Synthetic bootstrap starting ===")
    logger.info("Target: %d communities, %d workers, batch %d", n_communities, workers, batch_size)

    # Build job list: distribute evenly across sites and fractions
    rng_global = np.random.default_rng(42)
    seeds = rng_global.integers(0, 2**32, size=n_communities).tolist()

    jobs: list[tuple] = []
    for i, seed in enumerate(seeds):
        site = site_codes[i % len(site_codes)]
        frac = fractions[i % len(fractions)]
        jobs.append((seed, site, frac))

    # Batch jobs
    def _chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    batched_args = [
        ([(s, site, frac) for s, site, frac in chunk])
        for chunk in _chunks(jobs, batch_size)
    ]

    t_start = time.time()
    n_total_inserted = 0
    n_batches = len(batched_args)

    logger.info("Submitting %d batches to %d workers …", n_batches, workers)

    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_generate_batch, arg): i for i, arg in enumerate(batched_args)}
        for fut in as_completed(futures):
            batch_idx = futures[fut]
            try:
                community_list = fut.result()
                n_ins = _insert_batch(str(db_path), community_list)
                n_total_inserted += n_ins
                elapsed = time.time() - t_start
                rate = n_total_inserted / elapsed if elapsed > 0 else 0
                logger.info(
                    "Batch %4d/%d done — %6d inserted so far (%.0f/s, %.1f min elapsed)",
                    batch_idx + 1, n_batches, n_total_inserted, rate, elapsed / 60
                )
            except Exception as exc:
                logger.error("Batch %d failed: %s", batch_idx, exc)

    elapsed = time.time() - t_start
    logger.info("Generation complete: %d communities in %.1f min", n_total_inserted, elapsed / 60)

    # Train model
    logger.info("--- Training FunctionalPredictor ---")
    try:
        _train_predictor(str(db_path), model_out)
    except Exception as exc:
        logger.error("Model training failed: %s", exc)

    # Build reference BIOM
    logger.info("--- Building reference BIOM ---")
    try:
        _build_reference_biom(str(db_path), biom_out, min_bnf=min_bnf_biom)
    except Exception as exc:
        logger.error("Reference BIOM build failed: %s", exc)

    # Rescore T0.25 with the real model (all rows with neutral 0.5 score)
    logger.info("--- Re-running T0.25 with trained model ---")
    try:
        import yaml
        from config_schema import PipelineConfig
        from db_utils import SoilDB

        config_path = _PROJ_ROOT / "config.example.yaml"
        cfg = PipelineConfig(**yaml.safe_load(config_path.read_text()))

        # Override model_path and reference_db in the config
        if "t025" not in cfg.filters:
            cfg.filters["t025"] = {}
        cfg.filters["t025"]["model_path"]   = str(model_out)
        cfg.filters["t025"]["reference_db"] = str(biom_out)

        from pipeline_core import run_t025_batch
        with SoilDB(str(db_path)) as db:
            result = run_t025_batch(config=cfg, db=db, workers=workers,
                                    receipts_dir=str(_PROJ_ROOT / "receipts"))
        logger.info("T0.25 rescore: %s", json.dumps(result))
    except Exception as exc:
        logger.error("T0.25 rescore failed: %s", exc)

    logger.info("=== Bootstrap complete ===")


if __name__ == "__main__":
    app()
