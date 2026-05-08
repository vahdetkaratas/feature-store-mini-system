# Feature Store Mini System

This repository is a **compact batch pipeline** that turns **Telco-style (churn) customer rows** in a raw CSV into a **single feature table** you can reuse for analysis, training prep, or batch scoring. The idea is to keep **feature meaning, transform code, and validation** in one place so nothing drifts between a notebook, a script, and a downstream consumer.

**How it is organized.** Each engineered column is described in a **definition registry** (name, dtypes, which raw fields it needs, a short description). A **single build step** reads the raw table, runs the registered transforms in order, and writes a table with `customer_id` plus the derived features. A small **validation layer** then checks the result (for example duplicate ids, expected columns, reasonable numerics, entirely-null columns) and returns a compact summary that is easy to surface in an API or a log.

**What it is not.** This is **not** a hosted feature store (no Feast/Tecton-style online serving, materialized history, or point-in-time joins). It is **not** a training or model-serving project. Scope stays deliberately small: **one honest batch pattern** with tests and an optional HTTP layer so someone else can see the same behavior without digging through only notebooks.

**Stack.** Python, **pandas**, **pytest**, and optional **FastAPI** for JSON endpoints and an optional local **HTML demo** when **`layout-shell/index.html`** is present (recruiter shell is generated from **`shell/`** and committed as **`layout-shell/`** — re-run `render-shell.mjs` after copy changes). **GitHub Actions** runs tests and a one-line CLI smoke build on Python **3.11** and **3.12**.

---

## Repository layout

At a glance: **`src/features`** holds definitions and transforms; **`src/pipeline`** exposes `build_feature_table` and structured **`PipelineInputError`** codes for bad input; **`src/validation`** runs checks on the built frame; **`data/raw`** holds a synthetic **`sample_raw.csv`**; **`src/api`** wires the same build into **`POST /demo/transform`** and a **`/features`** catalog. See the tree below for file-level navigation.

```
data/raw/sample_raw.csv       # synthetic sample (~250 rows)
scripts/generate_sample_raw.py
src/features/
src/pipeline/
src/validation/
src/api/main.py
tests/
.github/workflows/ci.yml
```

If **`layout-shell/`** is missing, **`GET /`** redirects to **`/docs`** (API-only deploy). If it exists, **`GET /`** redirects to **`/layout-shell/index.html`** and static files are served under **`/layout-shell/`**.

---

## Input and output

The pipeline expects **known column names** compatible with a typical churn-style export (customer id, tenure, charges, contract, internet/phone/add-on flags). The exact set is driven by **`src/features/definitions.py`**. Missing required columns do not fail silently: the code raises **`PipelineInputError`** with a stable **`code`** such as `MISSING_RAW_COLUMNS`.

The built table has **`customer_id`** plus eight derived columns, including bands, booleans, counts, **`charge_per_tenure`**, and a **`feature_version`** stamp. Validation can run from disk or on the DataFrame in memory and produces a **`summary`** suitable for APIs alongside detailed check blocks.

---

## Getting started

Use **Python 3.11 or 3.12** (what CI runs). Create a venv, install dependencies from **`requirements.txt`**, then from the **repository root** run the default build:

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m src.pipeline.build_feature_table
```

By default this reads **`data/raw/sample_raw.csv`** and writes **`artifacts/feature_table/customer_feature_table.csv`**. You can point **`--raw`** and **`--out`** elsewhere; **`--strict`** tightens rules when a required raw column exists but is entirely null. To regenerate the sample CSV, run **`python scripts/generate_sample_raw.py`**. To run the test suite: **`python -m pytest tests/ -v`**.

---

## Optional HTTP demo

The same build runs behind **FastAPI**: start the app with **`uvicorn src.api.main:app --reload --host 127.0.0.1 --port 8000`**, then open **`/docs`** for interactive documentation. Endpoints include **`/features`** (catalog), **`/demo/sample-raw.csv`** (bundled input), and **`/demo/transform`** (upload a CSV, optional **`strict=true`**). Successful responses include **`meta`**, **`input`**, **`output`**, **`validation`**, and **`preview`**; validation failures typically return **422** with a structured **`detail`** object (**`code`**, **`message`**, **`missing_columns`**, **`details`**).

---

## Data note

**`sample_raw.csv`** is **synthetic** and generated for demonstration only (**`scripts/generate_sample_raw.py`**), not real customers.
