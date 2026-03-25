# Feature Store Mini System

A **small, honest batch pipeline** that turns **raw customer rows** (Telco-style / churn schema) into a **reusable feature table** (CSV). It centralizes **feature definitions**, **transform code**, and **lightweight validation** so training and batch scoring can share the same logic.

## What this is

- A **mini “feature-store-style” engineering pattern**: definitions → transforms → one build step → validated output.
- A **batch pipeline** you can run locally, test with `pytest`, and (optionally) expose via a **small FastAPI app**: a **minimal HTML demo page** (upload + bundled sample) plus **JSON endpoints** (`/features`, `/demo/transform`, etc.).
- **Portfolio-sized**: enough structure to discuss feature consistency in interviews without claiming a full platform.

## What this is not

- Not Feast / Tecton / a real feature platform (no online serving, no materialized history, no point-in-time joins).
- Not a training or model-serving repo.
- Not a large UI product.

## Repository layout

```
data/raw/sample_raw.csv          # Synthetic demo input (~250 rows; regenerate optional)
scripts/generate_sample_raw.py   # Regenerates sample_raw.csv (deterministic seed)
layout-shell/                    # Demo UI: index.html + styles.css + demo-content.css (served at /layout-shell/)
src/features/                    # definitions, transforms, registry, metadata (API catalog)
src/pipeline/build_feature_table.py
src/pipeline/errors.py           # Structured input errors (used by API 422 responses)
src/validation/feature_checks.py
src/api/main.py                  # FastAPI: /layout-shell static demo; / , /features , /demo/sample-raw.csv , /demo/transform
tests/
.github/workflows/ci.yml         # pytest + smoke pipeline (GitHub Actions)
```

## Requirements

- **Python 3.11 or 3.12** (these are the versions CI runs on GitHub Actions).
- Dependencies are **pinned** in `requirements.txt`. Newer Python (e.g. 3.13) may work locally but is not CI-guaranteed.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run the pipeline (CLI)

From the **repository root** (paths are relative to CWD):

```bash
python -m src.pipeline.build_feature_table
```

- **Input (default):** `data/raw/sample_raw.csv`
- **Output:** `artifacts/feature_table/customer_feature_table.csv`

Optional flags:

```bash
python -m src.pipeline.build_feature_table --raw data/raw/sample_raw.csv --out artifacts/feature_table/out.csv
python -m src.pipeline.build_feature_table --strict
```

| Flag | Meaning |
|------|---------|
| `--raw` | Path to raw CSV (defaults to `data/raw/sample_raw.csv`) |
| `--out` | Output feature table path (defaults to `artifacts/feature_table/customer_feature_table.csv`) |
| `--strict` | Fail if a required raw column exists but is 100% null |

Custom paths (Python):

```python
from src.pipeline.build_feature_table import run_build_feature_table

run_build_feature_table(
    raw_path="data/raw/sample_raw.csv",
    output_path="artifacts/feature_table/customer_feature_table.csv",
    strict=False,
)
```

### Sample data

- Committed `sample_raw.csv` is **synthetic** (not real customers), generated for a **non-trivial row count** so demos and quick stats are meaningful.
- Regenerate anytime:

```bash
python scripts/generate_sample_raw.py
```

## Raw input schema

The pipeline expects **Churn-style** columns (names must match). At minimum, every column referenced in `src/features/definitions.py` must be present, plus a customer id:

- `customerID` or `customer_id`
- `tenure`, `MonthlyCharges`, `TotalCharges`, `Contract`, `InternetService`, `TechSupport`
- `PhoneService`, `MultipleLines`, `OnlineSecurity`, `OnlineBackup`, `DeviceProtection`, `StreamingTV`, `StreamingMovies`

If required columns are missing, the pipeline raises **`PipelineInputError`** (subclass of `ValueError`) with a stable `code` (e.g. `MISSING_RAW_COLUMNS`) instead of silently producing zeros/false features.

Optional **`strict=True`** (CLI programmatic use or API query `strict=true`): also rejects inputs where a required raw column **exists but is 100% null**.

## Output schema

`customer_id` plus eight features:

| Column | Role |
|--------|------|
| `num_active_services` | int |
| `is_long_term_contract` | bool |
| `monthly_charge_band` | Low / Medium / High |
| `charge_per_tenure` | float |
| `has_tech_support` | bool |
| `is_fiber_user` | bool |
| `has_streaming_bundle` | bool |
| `feature_version` | string (e.g. `v1.0`) |

## Validation

```python
from src.validation.feature_checks import validate_feature_table

result = validate_feature_table(path="artifacts/feature_table/customer_feature_table.csv")
print(result["all_ok"], result["summary"], result["duplicate_check"])
```

Checks include duplicate `customer_id`, missing expected columns, basic numeric sanity, and “column entirely null”. Each full result includes a compact **`summary`** block (`checks_passed`, per-check `ok`, headline) for APIs and reviewers.

## Tests

```bash
python -m pytest tests/ -v
```

## Optional: minimal live demo (FastAPI)

A **small FastAPI** app serves (1) a **minimal HTML demo** and (2) **JSON APIs** for the same pipeline. The page lets you upload a raw CSV or run the **bundled** `sample_raw.csv` without a local file; **`POST /demo/transform`** returns the same structured JSON you can also obtain from **curl** or **Swagger**.

```bash
uvicorn src.api.main:app --reload --host 127.0.0.1 --port 8000
```

**HTML demo:** **`/`** serves the same **`layout-shell/index.html`** (URL stays on `/`). The **`/layout-shell/`** mount still serves that folder for static assets (**`/layout-shell/styles.css`**, **`/layout-shell/demo-content.css`**) and direct access to **`/layout-shell/index.html`** if needed.

**Interactive API docs:** **`/docs`** (Swagger UI) or **`/redoc`** (ReDoc), e.g. `http://127.0.0.1:8000/docs`.

| Endpoint | Purpose |
|----------|---------|
| `GET /` | Demo HTML (same file as `layout-shell/index.html`; URL remains `/`) |
| `GET /health` | Liveness |
| `GET /features` | Feature **catalog**: name, `dtype`, `kind`, description, `input_columns` (from `definitions.py`) |
| `GET /demo/sample-raw.csv` | Returns the committed **`data/raw/sample_raw.csv`** (same default input as the CLI); used by the demo page and handy for curl |
| `POST /demo/transform` | Multipart `file` = raw `.csv` (max 2 MB). Query `strict=true` for stricter raw checks |

**`POST /demo/transform` response shape** (high level):

- `meta` — `pipeline_version` (API app version), `feature_table_version` (from `FEATURE_VERSION` in definitions), `strict_mode`, `preview_row_limit`  
- `input` — filename, row/column counts, column names  
- `output` — row/column counts, `id_column`, `feature_columns`, full column list  
- `validation` — `summary` (pass counts + checklist) + `details` (full check dicts)  
- `preview` — first N rows as JSON records  

**422 errors** for schema/strict failures return a JSON body in `detail` with: `code`, `message`, `missing_columns`, `details`.

Common `detail.code` values:

| `code` | Meaning |
|--------|---------|
| `MISSING_CUSTOMER_ID` | No `customer_id` or `customerID` column in the upload. |
| `MISSING_RAW_COLUMNS` | One or more raw columns required by registered features are absent (`missing_columns` lists them). |
| `STRICT_RAW_ALL_NULL` | `strict=true` / `--strict`: a required raw column is present but entirely null (`details.entirely_null_columns`). |
| `MISSING_TRANSFORM` | Registry/feature wiring bug: a feature name has no implementation in `TRANSFORM_FUNCTIONS`. |

**Reviewer quick path (terminal):** with the API running locally, scan the structured response without scrolling the full JSON (requires [jq](https://jqlang.org/)):

```bash
curl -s -X POST "http://127.0.0.1:8000/demo/transform" -F "file=@data/raw/sample_raw.csv" \
  | jq '.meta, .validation.summary, .output.feature_columns'
```

Example (curl, full body):

```bash
curl -s -X POST "http://127.0.0.1:8000/demo/transform" -F "file=@data/raw/sample_raw.csv"
curl -s "http://127.0.0.1:8000/features"
```

This is intentionally minimal: **definitions + batch build + validation** exposed as a small HTML demo and JSON APIs, not a production feature store.

## CI (GitHub Actions)

On push/PR to `main` or `master`, CI runs on Python **3.11** and **3.12**: `pip install -r requirements.txt`, `pytest`, then a **smoke** run of `python -m src.pipeline.build_feature_table`.

## Why this matters (portfolio framing)

Interviewers often see models and APIs; fewer candidates show **where features come from** and how to avoid **train/serve skew**. This repo demonstrates a **single source of truth** for feature logic (definitions + transforms + one build entrypoint) and **basic output validation** — at a scope that stays truthful.

## License / data

Synthetic sample data is generated by `scripts/generate_sample_raw.py` for demonstration only.
