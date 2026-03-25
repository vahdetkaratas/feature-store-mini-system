"""
Feature transformation functions. Raw columns: Churn-style (see README and definitions.py).
"""
import pandas as pd

from src.features.definitions import FEATURE_VERSION

# Must match input_columns for num_active_services in definitions.py (single logical source: definitions).
SERVICE_COLUMNS = [
    "PhoneService", "MultipleLines", "OnlineSecurity", "OnlineBackup",
    "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies",
]

# MonthlyCharges bands (percentile-based or fixed)
MONTHLY_CHARGE_LOW = 35.0
MONTHLY_CHARGE_HIGH = 70.0


def _ensure_numeric_total_charges(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure TotalCharges is numeric (coerce errors to NaN)."""
    out = df.copy()
    if "TotalCharges" in out.columns:
        out["TotalCharges"] = pd.to_numeric(out["TotalCharges"], errors="coerce")
    return out


def num_active_services(df: pd.DataFrame) -> pd.Series:
    """Count of services with Yes (No / No phone service / No internet service -> 0)."""
    out = df.copy()
    for col in SERVICE_COLUMNS:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).str.strip().str.lower()
    count = pd.Series(0, index=df.index)
    for col in SERVICE_COLUMNS:
        if col in out.columns:
            count += (out[col] == "yes").astype(int)
    return count


def is_long_term_contract(df: pd.DataFrame) -> pd.Series:
    """True if Contract is One year or Two year."""
    if "Contract" not in df.columns:
        return pd.Series(False, index=df.index)
    c = df["Contract"].fillna("").astype(str).str.strip()
    return c.isin(["One year", "Two year"])


def monthly_charge_band(df: pd.DataFrame) -> pd.Series:
    """Low / Medium / High from MonthlyCharges."""
    if "MonthlyCharges" not in df.columns:
        return pd.Series("Unknown", index=df.index, dtype=object)
    mc = pd.to_numeric(df["MonthlyCharges"], errors="coerce").fillna(0)
    band = pd.Series("Medium", index=df.index, dtype=object)
    band[mc <= MONTHLY_CHARGE_LOW] = "Low"
    band[mc > MONTHLY_CHARGE_HIGH] = "High"
    return band


def charge_per_tenure(df: pd.DataFrame) -> pd.Series:
    """TotalCharges / tenure; tenure 0 -> 0."""
    out = _ensure_numeric_total_charges(df)
    total = out.get("TotalCharges", pd.Series(0.0, index=df.index))
    tenure = out.get("tenure", pd.Series(0, index=df.index))
    tenure = pd.to_numeric(tenure, errors="coerce").fillna(0)
    result = total / tenure.replace(0, float("nan"))
    return result.fillna(0.0)


def has_tech_support(df: pd.DataFrame) -> pd.Series:
    """True if TechSupport == Yes."""
    if "TechSupport" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["TechSupport"].fillna("").astype(str).str.strip().str.lower() == "yes"


def is_fiber_user(df: pd.DataFrame) -> pd.Series:
    """True if InternetService == Fiber optic."""
    if "InternetService" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["InternetService"].fillna("").astype(str).str.strip() == "Fiber optic"


def has_streaming_bundle(df: pd.DataFrame) -> pd.Series:
    """True if both StreamingTV and StreamingMovies are Yes."""
    if "StreamingTV" not in df.columns or "StreamingMovies" not in df.columns:
        return pd.Series(False, index=df.index)
    tv = df["StreamingTV"].fillna("").astype(str).str.strip().str.lower() == "yes"
    mov = df["StreamingMovies"].fillna("").astype(str).str.strip().str.lower() == "yes"
    return tv & mov


def feature_version_column(df: pd.DataFrame) -> pd.Series:
    """Constant feature_version string."""
    return pd.Series(FEATURE_VERSION, index=df.index, dtype=object)


# Map feature name -> transformation function (for registry)
TRANSFORM_FUNCTIONS = {
    "num_active_services": num_active_services,
    "is_long_term_contract": is_long_term_contract,
    "monthly_charge_band": monthly_charge_band,
    "charge_per_tenure": charge_per_tenure,
    "has_tech_support": has_tech_support,
    "is_fiber_user": is_fiber_user,
    "has_streaming_bundle": has_streaming_bundle,
    "feature_version": feature_version_column,
}
