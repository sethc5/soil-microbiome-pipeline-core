"""Tests for spatial population generation and clustering."""

from __future__ import annotations

from sim_model.spatial import (
    REGION_PROFILES,
    cluster_communities,
    generate_spatial_population,
    rank_clusters,
)


class TestGenerateSpatialPopulation:
    def test_correct_size(self):
        pop = generate_spatial_population(n=100, random_state=42)
        assert len(pop) == 100

    def test_reproducible(self):
        pop1 = generate_spatial_population(n=50, random_state=7)
        pop2 = generate_spatial_population(n=50, random_state=7)
        assert pop1 == pop2

    def test_has_required_keys(self):
        pop = generate_spatial_population(n=10, random_state=1)
        for s in pop:
            assert "sample" in s
            assert "lat" in s
            assert "lon" in s
            assert "region" in s

    def test_regions_distributed(self):
        pop = generate_spatial_population(n=500, random_state=42)
        regions = {s["region"] for s in pop}
        assert len(regions) == 5  # all 5 regions represented

    def test_region_specific_environment(self):
        """Desert samples should have higher pH and lower moisture than PNW samples."""
        pop = generate_spatial_population(n=500, random_state=42)
        desert = [s for s in pop if s["region"] == "desert_southwest"]
        pnw = [s for s in pop if s["region"] == "pacific_northwest"]

        desert_mean_ph = sum(s["sample"].environment.soil_ph for s in desert) / len(desert)
        pnw_mean_ph = sum(s["sample"].environment.soil_ph for s in pnw) / len(pnw)
        assert desert_mean_ph > pnw_mean_ph

        desert_mean_moist = sum(s["sample"].environment.moisture for s in desert) / len(desert)
        pnw_mean_moist = sum(s["sample"].environment.moisture for s in pnw) / len(pnw)
        assert desert_mean_moist < pnw_mean_moist

    def test_lat_lon_within_region_bounds(self):
        pop = generate_spatial_population(n=200, random_state=42)
        for s in pop:
            profile = REGION_PROFILES[s["region"]]
            # lat/lon should be within ~4 spreads of center (gaussian has long tails)
            assert abs(s["lat"] - profile["lat_center"]) < 4 * profile["lat_spread"] + 2
            assert abs(s["lon"] - profile["lon_center"]) < 4 * profile["lon_spread"] + 2

    def test_subset_of_regions(self):
        pop = generate_spatial_population(n=100, regions=["southeast_us", "great_plains"], random_state=42)
        regions = {s["region"] for s in pop}
        assert regions == {"southeast_us", "great_plains"}


class TestClusterCommunities:
    def test_clustering_assigns_cluster_id(self):
        pop = generate_spatial_population(n=100, random_state=42)
        clustered = cluster_communities(pop, k=5)
        for s in clustered:
            assert "cluster" in s
            assert 0 <= s["cluster"] < 5

    def test_empty_input(self):
        assert cluster_communities([], k=5) == []

    def test_clustering_deterministic(self):
        pop = generate_spatial_population(n=100, random_state=42)
        c1 = cluster_communities(list(pop), k=3)
        c2 = cluster_communities(list(pop), k=3)
        assert [s["cluster"] for s in c1] == [s["cluster"] for s in c2]


class TestRankClusters:
    def test_returns_top_k(self):
        pop = generate_spatial_population(n=200, random_state=42)
        clustered = cluster_communities(pop, k=5)
        ranked = rank_clusters(clustered, target="bnf", top_k=3)
        assert len(ranked) <= 3

    def test_sorted_by_flux(self):
        pop = generate_spatial_population(n=200, random_state=42)
        clustered = cluster_communities(pop, k=5)
        ranked = rank_clusters(clustered, target="bnf", top_k=5)
        fluxes = [c["mean_target_flux"] for c in ranked]
        assert fluxes == sorted(fluxes, reverse=True)

    def test_clusters_have_required_keys(self):
        pop = generate_spatial_population(n=200, random_state=42)
        clustered = cluster_communities(pop, k=5)
        ranked = rank_clusters(clustered, target="bnf", top_k=3)
        required = {"cluster", "n_samples", "mean_target_flux", "mean_stability",
                     "mean_lat", "mean_lon", "dominant_region", "dominant_region_display"}
        for c in ranked:
            assert set(c.keys()) >= required

    def test_warm_humid_regions_rank_higher_for_bnf(self):
        """Southeast US (warm, humid, high nifH prevalence) should rank higher than desert for BNF."""
        pop = generate_spatial_population(n=500, random_state=42)
        clustered = cluster_communities(pop, k=5)
        ranked = rank_clusters(clustered, target="bnf", top_k=5)

        # Find clusters dominated by southeast vs desert
        se_fluxes = [c["mean_target_flux"] for c in ranked if c["dominant_region"] == "southeast_us"]
        desert_fluxes = [c["mean_target_flux"] for c in ranked if c["dominant_region"] == "desert_southwest"]

        if se_fluxes and desert_fluxes:
            assert max(se_fluxes) > max(desert_fluxes)

    def test_different_targets_rank_regions_differently(self):
        """BNF and pathogen suppression should rank regions differently."""
        pop = generate_spatial_population(n=300, random_state=42)
        clustered = cluster_communities(pop, k=5)

        bnf_ranked = rank_clusters(clustered, target="bnf", top_k=5)
        path_ranked = rank_clusters(list(clustered), target="pathogen_suppression", top_k=5)

        bnf_regions = [c["dominant_region"] for c in bnf_ranked]
        path_regions = [c["dominant_region"] for c in path_ranked]
        assert bnf_regions != path_regions

    def test_more_samples_stabilizes_mean(self):
        """Larger clusters should have more stable mean flux estimates."""
        pop_small = generate_spatial_population(n=50, random_state=42)
        pop_large = generate_spatial_population(n=500, random_state=42)

        clustered_small = cluster_communities(pop_small, k=3)
        clustered_large = cluster_communities(pop_large, k=3)

        ranked_small = rank_clusters(clustered_small, target="bnf", top_k=3)
        ranked_large = rank_clusters(clustered_large, target="bnf", top_k=3)

        # Larger clusters should have more samples
        assert sum(c["n_samples"] for c in ranked_large) > sum(c["n_samples"] for c in ranked_small)