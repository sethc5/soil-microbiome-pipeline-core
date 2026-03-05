"""
tests/test_metadata_normalizer.py — Unit tests for MetadataNormalizer.

Run with:
    python -m pytest tests/test_metadata_normalizer.py -v
"""

import pytest

from compute.metadata_normalizer import MetadataNormalizer


@pytest.fixture
def norm():
    return MetadataNormalizer()


# ---------------------------------------------------------------------------
# parse_ph
# ---------------------------------------------------------------------------

class TestParsePH:
    def test_numeric(self, norm):
        assert norm.parse_ph(6.5) == pytest.approx(6.5)

    def test_string_numeric(self, norm):
        assert norm.parse_ph("7.2") == pytest.approx(7.2)

    def test_string_with_suffix(self, norm):
        assert norm.parse_ph("6.8 (H2O)") == pytest.approx(6.8)

    def test_clamp_high(self, norm):
        assert norm.parse_ph(15.0) == pytest.approx(14.0)

    def test_clamp_low(self, norm):
        assert norm.parse_ph(-1.0) == pytest.approx(0.0)

    def test_none(self, norm):
        assert norm.parse_ph(None) is None

    def test_garbage(self, norm):
        assert norm.parse_ph("no ph here") is None


# ---------------------------------------------------------------------------
# parse_depth
# ---------------------------------------------------------------------------

class TestParseDepth:
    def test_numeric(self, norm):
        assert norm.parse_depth(15) == pytest.approx(15.0)

    def test_range_cm(self, norm):
        assert norm.parse_depth("0-15 cm") == pytest.approx(7.5)

    def test_range_nodash(self, norm):
        assert norm.parse_depth("5 to 20 cm") == pytest.approx(12.5)

    def test_single_cm(self, norm):
        assert norm.parse_depth("15cm") == pytest.approx(15.0)

    def test_meters(self, norm):
        assert norm.parse_depth("0.15 m") == pytest.approx(15.0)

    def test_range_meters(self, norm):
        assert norm.parse_depth("0-0.15 m") == pytest.approx(7.5)

    def test_none(self, norm):
        assert norm.parse_depth(None) is None


# ---------------------------------------------------------------------------
# parse_coordinate
# ---------------------------------------------------------------------------

class TestParseCoordinate:
    def test_decimal(self, norm):
        assert norm.parse_coordinate("-43.2105") == pytest.approx(-43.2105)

    def test_float(self, norm):
        assert norm.parse_coordinate(42.37) == pytest.approx(42.37)

    def test_dms_north(self, norm):
        # 42°12'37.8"N → 42 + 12/60 + 37.8/3600 ≈ 42.2105
        val = norm.parse_coordinate("42°12'37.8\"N")
        assert val == pytest.approx(42.2105, abs=0.001)

    def test_dms_south(self, norm):
        val = norm.parse_coordinate("43°12'37.8\"S")
        assert val < 0

    def test_none(self, norm):
        assert norm.parse_coordinate(None) is None


# ---------------------------------------------------------------------------
# normalize_land_use
# ---------------------------------------------------------------------------

class TestNormalizeLandUse:
    @pytest.mark.parametrize("raw, expected", [
        ("cropland",          "cropland"),
        ("agricultural",      "cropland"),
        ("row crop",          "cropland"),
        ("temperate forest",  "forest"),
        ("Prairie",           "grassland"),
        ("Wetland",           "wetland"),
        ("Urban core",        "urban"),
        ("mine tailings",     "disturbed"),
    ])
    def test_controlled_vocab(self, norm, raw, expected):
        assert norm.normalize_land_use(raw) == expected

    def test_unknown_passthrough(self, norm):
        assert norm.normalize_land_use("tundra") == "tundra"

    def test_none(self, norm):
        assert norm.normalize_land_use(None) is None


# ---------------------------------------------------------------------------
# normalize_texture
# ---------------------------------------------------------------------------

class TestNormalizeTexture:
    @pytest.mark.parametrize("raw, expected", [
        ("clay loam",  "clay loam"),
        ("clay_loam",  "clay loam"),
        ("cl",         "clay loam"),
        ("sl",         "sandy loam"),
        ("loam",       "loam"),
        ("CLAY",       "clay"),          # case insensitive
    ])
    def test_mapping(self, norm, raw, expected):
        assert norm.normalize_texture(raw) == expected

    def test_unknown_passthrough(self, norm):
        assert norm.normalize_texture("volcanic") == "volcanic"


# ---------------------------------------------------------------------------
# detect_sampling_fraction
# ---------------------------------------------------------------------------

class TestDetectSamplingFraction:
    @pytest.mark.parametrize("text, expected", [
        ("rhizosphere soil",     "rhizosphere"),
        ("bulk_soil_sample",     "bulk"),
        ("bulk",                 "bulk"),
        ("endosphere fraction",  "endosphere"),
        ("forest floor litter",  "litter"),
        ("O-horizon",            "litter"),
        ("rhizo extract",        "rhizosphere"),
    ])
    def test_detection(self, norm, text, expected):
        assert norm.detect_sampling_fraction(text) == expected

    def test_no_match(self, norm):
        assert norm.detect_sampling_fraction("random text") is None

    def test_none(self, norm):
        assert norm.detect_sampling_fraction(None) is None


# ---------------------------------------------------------------------------
# normalize_sample (integration)
# ---------------------------------------------------------------------------

class TestNormalizeSample:
    def test_neon_style_dict(self, norm):
        raw = {
            "dnaSampleID":    "DNA-HARV-2021-001",
            "siteID":         "HARV",
            "decimalLatitude": 42.537,
            "decimalLongitude": -72.172,
            "soilpH":         5.8,
            "organicCPercent": 4.2,
            "clayPercent":    22.0,
            "sampleType":     "rhizosphere soil",
            "sampleTopDepth": 0,
            "sampleBottomDepth": 15,
        }
        out = norm.normalize_sample(raw, source="neon")
        assert out["source"] == "neon"
        assert out["latitude"] == pytest.approx(42.537)
        assert out["soil_ph"] == pytest.approx(5.8)
        assert out["organic_matter_pct"] == pytest.approx(4.2)
        assert out["sampling_fraction"] == "rhizosphere"

    def test_emp_style_dict(self, norm):
        raw = {
            "sample_id":   "EMP.12345",
            "ph":          "6.5",
            "latitude":    "37.9",
            "longitude":   "-122.3",
            "land_use":    "grassland",
            "depth":       "0-10 cm",
        }
        out = norm.normalize_sample(raw, source="emp")
        assert out["soil_ph"] == pytest.approx(6.5)
        assert out["latitude"] == pytest.approx(37.9)
        assert out["land_use"] == "grassland"
        assert out["sampling_depth_cm"] == pytest.approx(5.0)

    def test_passthrough_canonical_fields(self, norm):
        raw = {
            "sample_id":       "SRA.001",
            "site_id":         "HARV",
            "visit_number":    3,
            "sequencing_type": "shotgun_metagenome",
        }
        out = norm.normalize_sample(raw, source="sra")
        assert out["site_id"] == "HARV"
        assert out["visit_number"] == 3
        assert out["sequencing_type"] == "shotgun_metagenome"

    def test_extras_go_to_management(self, norm):
        raw = {
            "sample_id": "X.001",
            "unknown_field_abc": "some_value",
        }
        out = norm.normalize_sample(raw)
        import json
        mgmt = json.loads(out["management"])
        assert "unknown_field_abc" in mgmt
