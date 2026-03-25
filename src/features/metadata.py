"""
Public feature metadata for API / docs — derived from FEATURE_DEFINITIONS only.

`kind` is a lightweight, honest category (not a full feature-store taxonomy).
"""
from __future__ import annotations

from src.features.definitions import FEATURE_DEFINITIONS, FEATURE_VERSION


def list_public_features() -> list[dict]:
    """
    One dict per registered feature for reviewers / GET /features.

    Fields mirror definitions.py; no invented lineage or scheduling metadata.
    """
    out: list[dict] = []
    for f in FEATURE_DEFINITIONS:
        entry = {
            "name": f["name"],
            "dtype": f["dtype"],
            "kind": f["kind"],
            "description": f["description"],
            "input_columns": list(f.get("input_columns") or []),
        }
        out.append(entry)
    return out


def feature_catalog_payload() -> dict:
    """Wrapper for API: catalog + global version string."""
    return {
        "feature_table_version": FEATURE_VERSION,
        "feature_count": len(FEATURE_DEFINITIONS),
        "features": list_public_features(),
    }
