"""
HTTP demo: feature catalog, health, and CSV upload → structured pipeline result.

Not a feature store product — batch pipeline + validation only.
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.features.definitions import FEATURE_VERSION
from src.features.metadata import feature_catalog_payload
from src.pipeline.build_feature_table import build_feature_table
from src.pipeline.errors import PipelineInputError
from src.validation.feature_checks import validate_feature_table

MAX_UPLOAD_BYTES = 2 * 1024 * 1024
PREVIEW_ROWS = 20

# Project root (parent of src/) — main.py lives in src/api/
_ROOT = Path(__file__).resolve().parent.parent.parent
SAMPLE_RAW_CSV = _ROOT / "data" / "raw" / "sample_raw.csv"
LAYOUT_SHELL_INDEX = _ROOT / "layout-shell" / "index.html"

app = FastAPI(
    title="Feature Store Mini — demo API",
    version="1.0.0",
    description=(
        "Churn-style CSV → feature table + validation. "
        "Exposes `/features` metadata. Batch-only; no online store or training."
    ),
)

# Lets the demo page work when opened from disk (file://) or another origin while hitting this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount(
    "/layout-shell",
    StaticFiles(directory=str(_ROOT / "layout-shell")),
    name="layout_shell",
)


@app.get("/")
def root_demo_page() -> FileResponse:
    """Serve the demo HTML at / so the URL bar stays on the site root (assets under /layout-shell/)."""
    if not LAYOUT_SHELL_INDEX.is_file():
        raise HTTPException(
            status_code=404,
            detail="Demo page missing (expected layout-shell/index.html).",
        )
    return FileResponse(
        path=str(LAYOUT_SHELL_INDEX),
        media_type="text/html; charset=utf-8",
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/features")
def list_features() -> dict[str, Any]:
    """Registered feature definitions (dtype, kind, inputs) — same source as the pipeline."""
    return jsonable_encoder(feature_catalog_payload())


@app.get("/demo/sample-raw.csv")
def demo_sample_raw_csv() -> FileResponse:
    """Committed churn-style demo input — same file the CLI uses by default."""
    if not SAMPLE_RAW_CSV.is_file():
        raise HTTPException(
            status_code=404,
            detail="Bundled sample_raw.csv not found (expected data/raw/sample_raw.csv).",
        )
    return FileResponse(
        path=str(SAMPLE_RAW_CSV),
        filename="sample_raw.csv",
        media_type="text/csv",
    )


@app.post("/demo/transform")
async def demo_transform(
    file: UploadFile = File(..., description="Raw CSV with required customer columns"),
    strict: bool = Query(
        False,
        description=(
            "If true, reject uploads where required raw columns exist but are 100% null "
            "(stricter than default schema checks)."
        ),
    ),
) -> dict[str, Any]:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Expected a .csv upload.")

    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File too large (max {MAX_UPLOAD_BYTES // (1024 * 1024)} MB).",
        )

    try:
        raw_df = pd.read_csv(io.BytesIO(raw))
    except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {e}") from e

    try:
        feature_df = build_feature_table(raw_df, strict=strict)
    except PipelineInputError as e:
        raise HTTPException(
            status_code=422,
            detail={
                "code": e.code,
                "message": str(e),
                "missing_columns": e.missing_columns,
                "details": e.details,
            },
        ) from e

    validation_full = validate_feature_table(df=feature_df)
    preview_df = feature_df.head(PREVIEW_ROWS)
    feature_cols = [c for c in feature_df.columns if c != "customer_id"]

    return {
        "meta": {
            "pipeline_version": app.version,
            "feature_table_version": FEATURE_VERSION,
            "strict_mode": strict,
            "preview_row_limit": PREVIEW_ROWS,
        },
        "input": {
            "filename": file.filename,
            "row_count": len(raw_df),
            "column_count": raw_df.shape[1],
            "columns": list(raw_df.columns),
        },
        "output": {
            "row_count": len(feature_df),
            "column_count": feature_df.shape[1],
            "id_column": "customer_id",
            "feature_columns": feature_cols,
            "columns": list(feature_df.columns),
        },
        "validation": {
            "summary": validation_full.get("summary", {}),
            "details": {
                "all_ok": validation_full.get("all_ok"),
                "duplicate_check": validation_full.get("duplicate_check", {}),
                "missing_features": validation_full.get("missing_features", {}),
                "numeric_sanity": validation_full.get("numeric_sanity", {}),
                "distribution": validation_full.get("distribution", {}),
            },
        },
        "preview": {
            "limit": PREVIEW_ROWS,
            "row_count_returned": len(preview_df),
            "rows": jsonable_encoder(preview_df.to_dict(orient="records")),
        },
    }
