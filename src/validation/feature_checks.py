"""Feature table validation: duplicates, expected columns, numeric sanity, all-null columns."""
from pathlib import Path

import pandas as pd

from src.features.registry import get_feature_names

FEATURE_TABLE_PATH = Path("artifacts/feature_table/customer_feature_table.csv")


def check_duplicate_customer_id(df: pd.DataFrame) -> dict:
    """Return duplicate count and whether OK (no duplicates)."""
    if "customer_id" not in df.columns:
        return {"ok": False, "duplicate_count": 0, "message": "customer_id column missing"}
    dup = df["customer_id"].duplicated(keep=False)
    n_dup = dup.sum()
    return {
        "ok": bool(n_dup == 0),
        "duplicate_count": int(n_dup),
        "message": "No duplicate customer_id" if n_dup == 0 else f"{n_dup} rows with duplicate customer_id",
    }


def check_missing_features(df: pd.DataFrame) -> dict:
    """Check that all expected feature columns exist."""
    expected = set(get_feature_names())
    expected.add("customer_id")
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected
    return {
        "ok": bool(len(missing) == 0),
        "missing": sorted(missing),
        "extra": sorted(extra),
        "message": "All expected features present" if not missing else f"Missing: {sorted(missing)}",
    }


def check_numeric_sanity(df: pd.DataFrame) -> dict:
    """Basic numeric checks: num_active_services >= 0, charge_per_tenure >= 0."""
    issues = []
    if "num_active_services" in df.columns:
        neg = (df["num_active_services"] < 0).sum()
        if neg > 0:
            issues.append(f"num_active_services has {neg} negative values")
    if "charge_per_tenure" in df.columns:
        neg = (df["charge_per_tenure"] < 0).sum()
        if neg > 0:
            issues.append(f"charge_per_tenure has {neg} negative values")
    return {
        "ok": bool(len(issues) == 0),
        "issues": issues,
        "message": "Numeric sanity OK" if not issues else "; ".join(issues),
    }


def check_distribution(df: pd.DataFrame) -> dict:
    """Lightweight check: no expected feature column is entirely null."""
    issues = []
    for col in ["customer_id"] + get_feature_names():
        if col not in df.columns:
            continue
        if df[col].isna().all():
            issues.append(f"Column '{col}' is entirely null")
    return {
        "ok": bool(len(issues) == 0),
        "issues": issues,
        "message": "Distribution OK" if not issues else "; ".join(issues),
    }


def summarize_validation(result: dict) -> dict:
    """
    Compact summary for API clients (human-readable + machine counts).

    Expects the dict returned by validate_feature_table (excluding load errors).
    """
    if result.get("error"):
        return {
            "all_ok": False,
            "error": result["error"],
            "checks_passed": 0,
            "checks_failed": 0,
            "checks": [],
        }

    checks = [
        {"name": "duplicate_customer_id", "ok": result["duplicate_check"].get("ok", False)},
        {"name": "expected_columns", "ok": result["missing_features"].get("ok", False)},
        {"name": "numeric_sanity", "ok": result["numeric_sanity"].get("ok", False)},
        {"name": "no_all_null_columns", "ok": result["distribution"].get("ok", False)},
    ]
    passed = sum(1 for c in checks if c["ok"])
    failed = len(checks) - passed
    return {
        "all_ok": bool(result.get("all_ok", False)),
        "checks_passed": passed,
        "checks_failed": failed,
        "checks": checks,
        "headline": "All checks passed" if result.get("all_ok") else "One or more checks failed",
    }


def validate_feature_table(df: pd.DataFrame | None = None, path: str | Path | None = None) -> dict:
    """
    Run all checks. If df is None, load from path (default FEATURE_TABLE_PATH).
    Returns dict with keys: duplicate_check, missing_features, numeric_sanity, distribution, all_ok.
    """
    if df is None:
        path = Path(path or FEATURE_TABLE_PATH)
        if not path.exists():
            err = {
                "all_ok": False,
                "error": f"Feature table not found: {path}",
                "duplicate_check": {},
                "missing_features": {},
                "numeric_sanity": {},
                "distribution": {},
            }
            err["summary"] = summarize_validation(err)
            return err
        df = pd.read_csv(path)

    dup = check_duplicate_customer_id(df)
    missing = check_missing_features(df)
    numeric = check_numeric_sanity(df)
    dist = check_distribution(df)
    all_ok = bool(dup["ok"] and missing["ok"] and numeric["ok"] and dist["ok"])
    full = {
        "all_ok": all_ok,
        "duplicate_check": dup,
        "missing_features": missing,
        "numeric_sanity": numeric,
        "distribution": dist,
    }
    full["summary"] = summarize_validation(full)
    return full
