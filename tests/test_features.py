"""Tests for feature transformations, pipeline, and validation."""
import pandas as pd
import pytest

from src.features.transformations import (
    TRANSFORM_FUNCTIONS,
    num_active_services,
    is_long_term_contract,
    monthly_charge_band,
    charge_per_tenure,
    has_tech_support,
    is_fiber_user,
    has_streaming_bundle,
)
from src.features.registry import get_feature_names
from src.pipeline.build_feature_table import build_feature_table
from src.pipeline.errors import PipelineInputError
from src.validation.feature_checks import (
    check_duplicate_customer_id,
    check_missing_features,
    validate_feature_table,
)


@pytest.fixture
def sample_raw_df():
    """Churn-style raw DataFrame for tests."""
    return pd.DataFrame({
        "customerID": ["c1", "c2", "c3"],
        "tenure": [12, 0, 24],
        "MonthlyCharges": [25.0, 80.0, 55.0],
        "TotalCharges": [300.0, 0.0, 1320.0],
        "Contract": ["Month-to-month", "Two year", "One year"],
        "InternetService": ["DSL", "Fiber optic", "DSL"],
        "PhoneService": ["Yes", "Yes", "No"],
        "MultipleLines": ["No", "Yes", "No phone service"],
        "OnlineSecurity": ["No", "Yes", "No"],
        "OnlineBackup": ["No", "No", "Yes"],
        "DeviceProtection": ["No", "Yes", "No"],
        "TechSupport": ["No", "Yes", "No"],
        "StreamingTV": ["No", "Yes", "Yes"],
        "StreamingMovies": ["No", "Yes", "No"],
    })


def test_num_active_services(sample_raw_df):
    """num_active_services: Yes = 1, count per row."""
    out = num_active_services(sample_raw_df)
    assert len(out) == 3
    # c1: Phone=Yes -> 1
    assert out.iloc[0] == 1
    # c2: Phone, MultipleLines, OnlineSecurity, DeviceProtection, TechSupport, StreamingTV, StreamingMovies = 7
    assert out.iloc[1] == 7
    # c3: OnlineBackup, StreamingTV = 2
    assert out.iloc[2] == 2


def test_is_long_term_contract(sample_raw_df):
    """is_long_term_contract: One year / Two year -> True."""
    out = is_long_term_contract(sample_raw_df)
    assert out.iloc[0] == False  # Month-to-month
    assert out.iloc[1] == True   # Two year
    assert out.iloc[2] == True   # One year


def test_monthly_charge_band(sample_raw_df):
    """monthly_charge_band: Low <= 35, High > 70."""
    out = monthly_charge_band(sample_raw_df)
    assert out.iloc[0] == "Low"    # 25
    assert out.iloc[1] == "High"  # 80
    assert out.iloc[2] == "Medium"  # 55


def test_charge_per_tenure(sample_raw_df):
    """charge_per_tenure: TotalCharges / tenure; tenure 0 -> 0."""
    out = charge_per_tenure(sample_raw_df)
    assert out.iloc[0] == 25.0   # 300/12
    assert out.iloc[1] == 0.0    # tenure 0
    assert out.iloc[2] == 55.0   # 1320/24


def test_has_tech_support(sample_raw_df):
    """has_tech_support: TechSupport == Yes."""
    out = has_tech_support(sample_raw_df)
    assert out.iloc[0] == False
    assert out.iloc[1] == True
    assert out.iloc[2] == False


def test_is_fiber_user(sample_raw_df):
    """is_fiber_user: InternetService == Fiber optic."""
    out = is_fiber_user(sample_raw_df)
    assert out.iloc[0] == False
    assert out.iloc[1] == True
    assert out.iloc[2] == False


def test_has_streaming_bundle(sample_raw_df):
    """has_streaming_bundle: both StreamingTV and StreamingMovies Yes."""
    out = has_streaming_bundle(sample_raw_df)
    assert out.iloc[0] == False   # TV No, Movies No
    assert out.iloc[1] == True    # TV Yes, Movies Yes
    assert out.iloc[2] == False   # TV Yes, Movies No


def test_feature_build_order():
    """Registry returns non-empty feature list."""
    names = get_feature_names()
    assert len(names) >= 5
    assert "num_active_services" in names
    assert "feature_version" in names


def test_build_feature_table_not_empty(sample_raw_df):
    """build_feature_table produces non-empty table with customer_id and features."""
    ft = build_feature_table(sample_raw_df)
    assert not ft.empty
    assert "customer_id" in ft.columns
    assert ft["customer_id"].tolist() == ["c1", "c2", "c3"]
    assert "num_active_services" in ft.columns
    assert "is_long_term_contract" in ft.columns
    assert "feature_version" in ft.columns


def test_build_feature_table_num_active_services(sample_raw_df):
    """Pipeline num_active_services matches direct transform."""
    ft = build_feature_table(sample_raw_df)
    direct = num_active_services(sample_raw_df)
    assert ft["num_active_services"].tolist() == direct.tolist()


def test_build_feature_table_is_long_term_contract(sample_raw_df):
    """Pipeline is_long_term_contract matches direct transform."""
    ft = build_feature_table(sample_raw_df)
    direct = is_long_term_contract(sample_raw_df)
    assert ft["is_long_term_contract"].tolist() == direct.tolist()


def test_check_duplicate_customer_id_no_dup():
    """No duplicate customer_id -> ok."""
    df = pd.DataFrame({"customer_id": ["a", "b"], "num_active_services": [1, 2]})
    r = check_duplicate_customer_id(df)
    assert r["ok"] == True
    assert r["duplicate_count"] == 0


def test_check_duplicate_customer_id_with_dup():
    """Duplicate customer_id -> not ok."""
    df = pd.DataFrame({"customer_id": ["a", "a"], "num_active_services": [1, 2]})
    r = check_duplicate_customer_id(df)
    assert r["ok"] == False
    assert r["duplicate_count"] == 2


def test_check_missing_features():
    """All expected columns present -> ok."""
    names = get_feature_names()
    df = pd.DataFrame({c: [1] for c in ["customer_id"] + names})
    r = check_missing_features(df)
    assert r["ok"] == True


def test_validate_feature_table_in_memory(sample_raw_df):
    """validate_feature_table on DataFrame from build_feature_table."""
    ft = build_feature_table(sample_raw_df)
    r = validate_feature_table(df=ft)
    assert "duplicate_check" in r
    assert "missing_features" in r
    assert "numeric_sanity" in r
    assert "distribution" in r
    assert "summary" in r
    assert r["summary"]["checks_passed"] == 4
    assert r["duplicate_check"]["ok"] == True
    assert r["missing_features"]["ok"] == True
    assert r["numeric_sanity"]["ok"] == True
    assert r["distribution"]["ok"] == True
    assert r["all_ok"] == True


def test_build_feature_table_missing_raw_columns_raises(sample_raw_df):
    """Pipeline must fail fast when required raw columns are missing."""
    df = sample_raw_df.drop(columns=["TechSupport"])
    with pytest.raises(PipelineInputError) as e:
        build_feature_table(df)
    assert e.value.code == "MISSING_RAW_COLUMNS"
    assert "TechSupport" in e.value.missing_columns


def test_build_feature_table_strict_rejects_all_null_required_column(sample_raw_df):
    """Strict mode rejects required columns that exist but are entirely null."""
    df = sample_raw_df.copy()
    df["MonthlyCharges"] = float("nan")
    with pytest.raises(PipelineInputError) as e:
        build_feature_table(df, strict=True)
    assert e.value.code == "STRICT_RAW_ALL_NULL"
    assert "MonthlyCharges" in e.value.details.get("entirely_null_columns", [])


def test_registry_has_transform_for_every_feature():
    """No silent skips: every registered feature name maps to a transform."""
    names = get_feature_names()
    missing = [n for n in names if n not in TRANSFORM_FUNCTIONS]
    assert not missing, f"Missing TRANSFORM_FUNCTIONS for: {missing}"
    extra = [k for k in TRANSFORM_FUNCTIONS if k not in names]
    assert not extra, f"TRANSFORM_FUNCTIONS keys not in registry: {extra}"
