"""Pipeline: raw CSV -> feature table (see README)."""
import argparse
from pathlib import Path

import pandas as pd

from src.features.registry import get_feature_names
from src.features.definitions import FEATURE_DEFINITIONS
from src.features.transformations import TRANSFORM_FUNCTIONS, _ensure_numeric_total_charges
from src.pipeline.errors import PipelineInputError

RAW_DEFAULT = Path("data/raw/sample_raw.csv")
FEATURE_TABLE_PATH = Path("artifacts/feature_table/customer_feature_table.csv")

def _expected_raw_feature_columns() -> set[str]:
    """Union of raw columns required by all registered features."""
    cols: set[str] = set()
    for f in FEATURE_DEFINITIONS:
        for c in f.get("input_columns", []):
            if c:
                cols.add(c)
    return cols


def _check_raw_columns(raw_df: pd.DataFrame, *, strict: bool = False) -> None:
    """
    Fail-fast validation for raw input schema.

    If required raw columns are missing, some transforms could silently emit
    zeros/False; the pipeline raises instead.
    """
    if "customer_id" not in raw_df.columns and "customerID" not in raw_df.columns:
        raise PipelineInputError(
            "Raw data must include a customer id column: `customer_id` or `customerID`.",
            code="MISSING_CUSTOMER_ID",
        )

    expected = _expected_raw_feature_columns()
    missing = sorted(expected - set(raw_df.columns))
    if missing:
        raise PipelineInputError(
            f"Raw data is missing {len(missing)} required column(s) for registered features.",
            code="MISSING_RAW_COLUMNS",
            missing_columns=missing,
            details={"required_for_features": missing},
        )

    if strict:
        # Reject inputs where required feature-driving columns are present but useless (all null).
        entirely_null = sorted(
            c for c in expected if c in raw_df.columns and raw_df[c].isna().all()
        )
        if entirely_null:
            raise PipelineInputError(
                "Strict mode: one or more required raw columns are entirely null.",
                code="STRICT_RAW_ALL_NULL",
                missing_columns=[],
                details={"entirely_null_columns": entirely_null},
            )


def _ensure_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize customer id column to customer_id."""
    if "customer_id" in df.columns:
        return df
    if "customerID" in df.columns:
        out = df.copy()
        out["customer_id"] = out["customerID"].astype(str)
        return out
    return df


def build_feature_table(raw_df: pd.DataFrame, *, strict: bool = False) -> pd.DataFrame:
    """
    From raw Churn-style DataFrame, compute all registered features.
    Returns DataFrame with customer_id + feature columns.

    strict: if True, also fail when required raw columns exist but are 100% null.
    """
    _check_raw_columns(raw_df, strict=strict)
    df = _ensure_numeric_total_charges(raw_df)
    df = _ensure_customer_id(df)

    result = df[["customer_id"]].copy() if "customer_id" in df.columns else pd.DataFrame()
    if result.empty and "customerID" in df.columns:
        result = pd.DataFrame({"customer_id": df["customerID"].astype(str)})

    for name in get_feature_names():
        try:
            fn = TRANSFORM_FUNCTIONS[name]
        except KeyError as e:
            raise PipelineInputError(
                f"Feature '{name}' is registered but has no implementation in "
                f"TRANSFORM_FUNCTIONS (src/features/transformations.py).",
                code="MISSING_TRANSFORM",
                details={"feature_name": name},
            ) from e
        result[name] = fn(df)

    return result


def run_build_feature_table(
    raw_path: str | Path | None = None,
    output_path: str | Path | None = None,
    *,
    strict: bool = False,
) -> Path:
    """
    Load raw CSV, build feature table, save to artifacts/feature_table/customer_feature_table.csv.
    Returns path to written file.
    """
    raw_path = Path(raw_path or RAW_DEFAULT)
    output_path = Path(output_path or FEATURE_TABLE_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data not found: {raw_path}")

    raw_df = pd.read_csv(raw_path)
    ft = build_feature_table(raw_df, strict=strict)
    ft.to_csv(output_path, index=False)
    print(f"Feature table saved: {len(ft)} rows -> {output_path}")
    return output_path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build feature table from raw Churn-style CSV (repo root = CWD).",
    )
    parser.add_argument(
        "--raw",
        type=Path,
        default=None,
        help=f"Path to raw CSV (default: {RAW_DEFAULT})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=f"Output feature table CSV (default: {FEATURE_TABLE_PATH})",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if required raw columns exist but are 100%% null.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run_build_feature_table(raw_path=args.raw, output_path=args.out, strict=args.strict)
