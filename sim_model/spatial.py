"""Spatial population generation and geographic clustering."""

from __future__ import annotations

import random
from typing import Any, Dict, List, Tuple

from .dynamics import simulate_dynamics_with_target
from .population import RawSample, generate_raw_sample
from .schema import Community, Environment, Intervention


# ---------------------------------------------------------------------------
# Geographic regions — each has a characteristic environment profile
# ---------------------------------------------------------------------------

REGION_PROFILES: Dict[str, Dict[str, Any]] = {
    "southeast_us": {
        "display_name": "Southeast US (warm, humid, acidic soils)",
        "lat_center": 33.0,
        "lon_center": -84.0,
        "lat_spread": 3.0,
        "lon_spread": 4.0,
        "ph_mean": 5.5, "ph_std": 0.6,
        "om_mean": 3.5, "om_std": 1.5,
        "moisture_mean": 0.70, "moisture_std": 0.10,
        "temp_mean": 22.0, "temp_std": 3.0,
        "nifh_prevalence": 0.35,
    },
    "great_plains": {
        "display_name": "Great Plains (semi-arid, neutral pH)",
        "lat_center": 40.0,
        "lon_center": -100.0,
        "lat_spread": 4.0,
        "lon_spread": 5.0,
        "ph_mean": 7.0, "ph_std": 0.5,
        "om_mean": 2.5, "om_std": 1.0,
        "moisture_mean": 0.35, "moisture_std": 0.12,
        "temp_mean": 18.0, "temp_std": 5.0,
        "nifh_prevalence": 0.20,
    },
    "pacific_northwest": {
        "display_name": "Pacific Northwest (cool, wet, acidic)",
        "lat_center": 47.0,
        "lon_center": -122.0,
        "lat_spread": 2.5,
        "lon_spread": 3.0,
        "ph_mean": 5.8, "ph_std": 0.5,
        "om_mean": 6.0, "om_std": 2.0,
        "moisture_mean": 0.75, "moisture_std": 0.08,
        "temp_mean": 14.0, "temp_std": 3.0,
        "nifh_prevalence": 0.30,
    },
    "midwest_corn_belt": {
        "display_name": "Midwest Corn Belt (fertile, neutral)",
        "lat_center": 41.0,
        "lon_center": -89.0,
        "lat_spread": 2.0,
        "lon_spread": 4.0,
        "ph_mean": 6.5, "ph_std": 0.4,
        "om_mean": 4.5, "om_std": 1.2,
        "moisture_mean": 0.55, "moisture_std": 0.10,
        "temp_mean": 20.0, "temp_std": 4.0,
        "nifh_prevalence": 0.25,
    },
    "desert_southwest": {
        "display_name": "Desert Southwest (hot, dry, alkaline)",
        "lat_center": 34.0,
        "lon_center": -112.0,
        "lat_spread": 2.0,
        "lon_spread": 3.0,
        "ph_mean": 8.0, "ph_std": 0.4,
        "om_mean": 0.8, "om_std": 0.4,
        "moisture_mean": 0.15, "moisture_std": 0.08,
        "temp_mean": 32.0, "temp_std": 4.0,
        "nifh_prevalence": 0.10,
    },
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def generate_spatial_population(
    n: int = 500,
    regions: List[str] | None = None,
    random_state: int = 42,
) -> List[Dict[str, Any]]:
    """Generate samples with lat/lon clustered in geographic regions.

    Returns list of dicts with: sample (RawSample), lat, lon, region.
    """
    rng = random.Random(random_state)
    if regions is None:
        regions = list(REGION_PROFILES.keys())

    samples_per_region = n // len(regions)
    remainder = n % len(regions)

    output: List[Dict[str, Any]] = []
    for i, region_name in enumerate(regions):
        count = samples_per_region + (1 if i < remainder else 0)
        profile = REGION_PROFILES[region_name]

        for _ in range(count):
            lat = rng.gauss(profile["lat_center"], profile["lat_spread"])
            lon = rng.gauss(profile["lon_center"], profile["lon_spread"])

            ph = _clamp(rng.gauss(profile["ph_mean"], profile["ph_std"]), 2.0, 10.0)
            om = _clamp(rng.gauss(profile["om_mean"], profile["om_std"]), 0.0, 20.0)
            moisture = _clamp(rng.gauss(profile["moisture_mean"], profile["moisture_std"]), 0.0, 1.0)
            temp = _clamp(rng.gauss(profile["temp_mean"], profile["temp_std"]), -5.0, 45.0)

            sample = generate_raw_sample(rng, nifh_prevalence=profile["nifh_prevalence"])

            # Override environment with region-specific values
            env = Environment(soil_ph=ph, organic_matter_pct=om, moisture=moisture, temperature_c=temp)
            sample = RawSample(
                community=sample.community,
                environment=env,
                sequencing_depth=sample.sequencing_depth,
                nifh_read_count=sample.nifh_read_count,
                is_contaminated=sample.is_contaminated,
                is_flooded=sample.is_flooded,
                observed_otus=sample.observed_otus,
            )

            output.append({
                "sample": sample,
                "lat": round(lat, 4),
                "lon": round(lon, 4),
                "region": region_name,
            })

    return output


def cluster_communities(
    samples: List[Dict[str, Any]],
    k: int = 5,
) -> List[Dict[str, Any]]:
    """Assign samples to k clusters based on lat/lon using simple binning."""
    if not samples:
        return []

    lats = [s["lat"] for s in samples]
    lons = [s["lon"] for s in samples]
    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)

    lat_range = max_lat - min_lat if max_lat > min_lat else 1.0
    lon_range = max_lon - min_lon if max_lon > min_lon else 1.0

    for s in samples:
        lat_bin = int((s["lat"] - min_lat) / lat_range * (k - 0.001))
        lon_bin = int((s["lon"] - min_lon) / lon_range * (k - 0.001))
        s["cluster"] = min(lat_bin * 3 + lon_bin, k - 1)

    return samples


def rank_clusters(
    samples: List[Dict[str, Any]],
    target: str = "bnf",
    top_k: int = 5,
) -> List[Dict[str, Any]]:
    """Simulate each sample, compute mean flux per cluster, rank clusters."""

    cluster_data: Dict[int, List[Dict[str, Any]]] = {}
    for s in samples:
        cluster_id = s.get("cluster", 0)
        if cluster_id not in cluster_data:
            cluster_data[cluster_id] = []

        result = simulate_dynamics_with_target(
            s["sample"].community,
            s["sample"].environment,
            Intervention(0.0, 0.0, 0.0),
            target=target,
        )
        s["target_flux"] = result.target_flux
        s["stability_score"] = result.stability_score
        cluster_data[cluster_id].append(s)

    ranked: List[Dict[str, Any]] = []
    for cluster_id, members in cluster_data.items():
        fluxes = [m["target_flux"] for m in members]
        stabilities = [m["stability_score"] for m in members]
        regions = [m["region"] for m in members]
        lats = [m["lat"] for m in members]
        lons = [m["lon"] for m in members]

        from collections import Counter
        region_counts = Counter(regions)
        dominant_region = region_counts.most_common(1)[0][0]

        ranked.append({
            "cluster": cluster_id,
            "n_samples": len(members),
            "mean_target_flux": round(sum(fluxes) / len(fluxes), 4),
            "mean_stability": round(sum(stabilities) / len(stabilities), 4),
            "mean_lat": round(sum(lats) / len(lats), 2),
            "mean_lon": round(sum(lons) / len(lons), 2),
            "dominant_region": dominant_region,
            "dominant_region_display": REGION_PROFILES[dominant_region]["display_name"],
        })

    ranked.sort(key=lambda c: c["mean_target_flux"], reverse=True)
    return ranked[:top_k]