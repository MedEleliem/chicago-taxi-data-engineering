import json

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.api.main import app
from src.api import service


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("CHICAGO_DATA_ROOT", str(tmp_path))
    root = tmp_path / "gold/chicago_taxi/processing_date=2023-06-01"
    (root / "trips").mkdir(parents=True)
    (root / "_manifest.json").write_text(json.dumps({"status": "SUCCESS", "run_id": "GLD_TEST", "processing_date": "2023-06-01"}))
    (root / "_validation.json").write_text('{"passed":true}')
    base = {"trip_id": "a", "taxi_id": "t1", "trip_start_timestamp": pd.Timestamp("2023-06-01T10:00:00"),
        "trip_date": "2023-06-01", "trip_hour": 10, "company": "Flash Cab", "payment_type": "Cash",
        "trip_total": 12., "tips": 2., "has_tip": True, "trip_duration_minutes": 10., "trip_miles": 2.5,
        "pickup_community_area": 8, "pickup_centroid_latitude": 41.88, "pickup_centroid_longitude": -87.63,
        "dropoff_centroid_latitude": 41.9, "dropoff_centroid_longitude": -87.6}
    pd.DataFrame([base, {**base, "trip_id": "b", "company": "Other", "trip_total": 20., "pickup_centroid_latitude": None}]).to_parquet(root / "trips/part.parquet")
    service.read_snapshot.cache_clear()
    return TestClient(app)


def test_health_filters_aggregates_and_limits(client):
    assert client.get("/api/health").json()["ready"]
    assert client.get("/api/kpis").json()["total_trips"] == 2
    assert client.get("/api/kpis?company=Flash%20Cab").json()["total_revenue"] == 12
    assert client.get("/api/daily").json()[0]["trip_count"] == 2
    assert client.get("/api/trips?limit=1").json()["total"] == 2
    assert client.get("/api/trips?limit=1001").status_code == 422
    assert client.get("/api/kpis?start_date=2023-06-02&end_date=2023-06-01").status_code == 422
    assert client.get("/api/trips/a").json()["trip"]["trip_id"] == "a"
    assert client.get("/api/trips/missing").status_code == 404


def test_geo_and_optional_places(client, monkeypatch):
    assert len(client.get("/api/geo/pickups").json()["features"]) == 1
    assert client.get("/api/geo/trips").json()["features"][0]["geometry"]["type"] == "LineString"
    monkeypatch.delenv("FOURSQUARE_API_KEY", raising=False)
    assert client.get("/api/places/nearby?lat=41&lon=-87").json()["available"] is False
    assert client.get("/api/places/nearby?lat=100&lon=-87").status_code == 422


def test_empty_filter_serialization(client):
    result = client.get("/api/kpis?company=missing")
    assert result.status_code == 200 and result.json()["total_trips"] == 0
    assert result.json()["avg_trip_total"] is None


def test_no_gold_is_explicit(tmp_path, monkeypatch):
    monkeypatch.setenv("CHICAGO_DATA_ROOT", str(tmp_path))
    client = TestClient(app)
    assert client.get("/api/health").json()["ready"] is False
    assert client.get("/api/kpis").status_code == 503


def test_quality_follows_gold_lineage(client):
    root = service.data_root()
    marker = root / "gold/chicago_taxi/processing_date=2023-06-01/_manifest.json"
    payload = json.loads(marker.read_text())
    payload["source_run_id"] = "SLV_TEST"
    marker.write_text(json.dumps(payload))
    silver = root / "reports/silver/SLV_TEST"
    bronze = root / "reports/bronze/BRZ_TEST"
    silver.mkdir(parents=True)
    bronze.mkdir(parents=True)
    (silver / "manifest.json").write_text(json.dumps({"run_id": "SLV_TEST", "source_run_id": "BRZ_TEST",
        "input_rows": 3, "valid_rows": 2, "rejected_rows": 1, "warning_rows": 2}))
    (bronze / "manifest.json").write_text(json.dumps({"run_id": "BRZ_TEST", "rows_downloaded": 3, "pagination_complete": True}))
    (silver / "errors_report.csv").write_text("rule,count,rate\nINVALID_DURATION,1,0.333333\n")
    (silver / "warnings_report.csv").write_text("rule,count,rate\nZERO_DISTANCE,2,0.666667\n")
    assert client.get("/api/data-quality/summary").json()["rejection_rate"] == pytest.approx(1 / 3)
    assert client.get("/api/data-quality/errors").json()[0]["count"] == 1
    assert client.get("/api/data-quality/warnings").json()[0]["count"] == 2
    assert client.get("/api/data-quality/summary?company=Other").status_code == 422


def test_places_failure_is_nonblocking(client, monkeypatch):
    import requests
    monkeypatch.setenv("FOURSQUARE_API_KEY", "test-value")
    def fail(*args, **kwargs):
        raise requests.Timeout()
    monkeypatch.setattr(service.requests, "get", fail)
    assert client.get("/api/places/nearby?lat=41&lon=-87").json()["reason"] == "unavailable"
