"""Smoke tests for the optional FastAPI demo (upload → structured JSON)."""
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.api.main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_root_serves_demo_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")
    body = r.text
    assert "Feature Store Mini" in body
    assert "/layout-shell/styles.css" in body
    assert "/layout-shell/demo-content.css" in body


def test_demo_sample_raw_csv_served(client):
    r = client.get("/demo/sample-raw.csv")
    assert r.status_code == 200
    assert "text/csv" in r.headers.get("content-type", "")
    body = r.text
    assert "customerID" in body
    assert len(body) > 100


def test_demo_page_renders(client):
    r = client.get("/layout-shell/index.html")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("content-type", "")
    body = r.text
    assert "Feature Store Mini" in body
    assert "/demo/transform" in body
    assert 'href="/layout-shell/styles.css"' in body
    assert 'href="/layout-shell/demo-content.css"' in body


def test_features_catalog(client):
    r = client.get("/features")
    assert r.status_code == 200
    body = r.json()
    assert body["feature_count"] >= 5
    assert isinstance(body["features"], list)
    assert body["features"][0]["name"]
    assert body["features"][0]["dtype"]
    assert body["features"][0]["kind"]
    assert "input_columns" in body["features"][0]


def test_demo_transform_sample_csv(client):
    root = Path(__file__).resolve().parents[1]
    csv_path = root / "data" / "raw" / "sample_raw.csv"
    assert csv_path.exists(), "sample_raw.csv missing — run scripts/generate_sample_raw.py"

    with csv_path.open("rb") as f:
        r = client.post(
            "/demo/transform",
            files={"file": ("sample_raw.csv", f, "text/csv")},
        )
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["input"]["row_count"] >= 1
    assert body["output"]["row_count"] == body["input"]["row_count"]
    assert body["output"]["id_column"] == "customer_id"
    assert "customer_id" in body["output"]["columns"]
    assert body["validation"]["details"]["all_ok"] is True
    assert body["validation"]["summary"]["all_ok"] is True
    assert body["validation"]["summary"]["checks_passed"] == 4

    assert "feature_columns" in body["output"]
    assert "customer_id" not in body["output"]["feature_columns"]

    assert body["preview"]["limit"] == 20
    assert isinstance(body["preview"]["rows"], list)
    assert len(body["preview"]["rows"]) <= 20

    assert body["meta"]["pipeline_version"] == "1.0.0"
    assert body["meta"]["feature_table_version"]


def test_demo_transform_strict_query_passes(client):
    root = Path(__file__).resolve().parents[1]
    csv_path = root / "data" / "raw" / "sample_raw.csv"
    with csv_path.open("rb") as f:
        r = client.post(
            "/demo/transform?strict=true",
            files={"file": ("sample_raw.csv", f, "text/csv")},
        )
    assert r.status_code == 200
    assert r.json()["meta"]["strict_mode"] is True
