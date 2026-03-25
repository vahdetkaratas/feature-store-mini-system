"""Feature registry: build order follows definitions.py."""
from src.features.definitions import FEATURE_DEFINITIONS

# Build order: same as definitions (dependencies handled in transformations)
FEATURE_BUILD_ORDER = [d["name"] for d in FEATURE_DEFINITIONS]


def get_feature_names():
    """Return ordered list of feature names."""
    return FEATURE_BUILD_ORDER.copy()
