"""Feature catalog / metadata (definitions-driven)."""
from src.features.definitions import FEATURE_DEFINITIONS
from src.features.metadata import feature_catalog_payload, list_public_features


def test_list_public_features_aligns_with_definitions():
    public = list_public_features()
    assert len(public) == len(FEATURE_DEFINITIONS)
    for p, d in zip(public, FEATURE_DEFINITIONS, strict=True):
        assert p["name"] == d["name"]
        assert p["dtype"] == d["dtype"]
        assert p["kind"] == d["kind"]
        assert "description" in p


def test_feature_catalog_payload_shape():
    cat = feature_catalog_payload()
    assert cat["feature_count"] == len(FEATURE_DEFINITIONS)
    assert "features" in cat
    assert cat["feature_table_version"]
