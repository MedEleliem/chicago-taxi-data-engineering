import json
import logging
from copy import deepcopy

import pytest
import requests

from src.bronze import __main__ as bronze
from src.common import load_config


def local_config(tmp_path):
    config = deepcopy(load_config("config/chicago_taxi.yml"))
    for key in ("path", "reports_path", "logs_path"):
        config["bronze"]["output"][key] = str(tmp_path / {"path": "bronze", "reports_path": "reports", "logs_path": "logs"}[key])
    for key, directory in {"input_path": "bronze", "output_path": "silver", "quarantine_path": "quarantine",
                           "reports_path": "reports", "logs_path": "logs"}.items():
        config["silver"][key] = str(tmp_path / directory)
    return config


def test_date_window_and_where():
    assert bronze.build_date_window("2023-12-31") == ("2023-12-31T00:00:00", "2024-01-01T00:00:00")
    assert bronze.build_where_clause("2023-06-01") == "trip_start_timestamp >= '2023-06-01T00:00:00' AND trip_start_timestamp < '2023-06-02T00:00:00'"
    with pytest.raises(ValueError):
        bronze.build_date_window("2023-02-30")


def test_pagination_and_cap(monkeypatch):
    offsets = []
    def page(source, ingestion, where, offset):
        offsets.append(offset)
        return [{"trip_id": str(offset)}, {"trip_id": str(offset + 1)}] if offset == 0 else []
    monkeypatch.setattr(bronze, "fetch_page", page)
    _, metadata = bronze.fetch_all_pages({}, {"page_size": 2}, "2023-06-01", logging.getLogger("test"))
    assert offsets == [0, 2]
    assert metadata == {"pages_downloaded": 2, "rows_downloaded": 2, "pagination_complete": True}
    _, metadata = bronze.fetch_all_pages({}, {"page_size": 2, "max_pages": 1}, "2023-06-01", logging.getLogger("test"))
    assert metadata["pagination_complete"] is False


@pytest.mark.parametrize("status,expected_attempts", [(429, 3), (503, 3), (400, 1)])
def test_retry_is_limited(monkeypatch, status, expected_attempts):
    calls = []
    def fail(*args, **kwargs):
        calls.append(1)
        response = requests.Response()
        response.status_code = status
        raise requests.HTTPError(response=response)
    monkeypatch.setattr(bronze.requests, "get", fail)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: None)
    with pytest.raises(requests.HTTPError):
        bronze.fetch_page({"domain": "example.test", "dataset_id": "fixture"}, {"max_retries": 3}, "test", 0)
    assert len(calls) == expected_attempts


def test_bronze_quality(spark):
    records = [{"trip_id": "same", "fare": "0"}, {"trip_id": "same", "fare": "0"}, {"trip_id": None, "fare": None}]
    columns = load_config("config/chicago_taxi.yml")["bronze"]["ingestion"]["expected_columns"]
    report = bronze.bronze_quality(bronze.records_to_frame(spark, records, columns))
    assert report["row_count"] == 3
    assert report["duplicate_rows"]["count"] == 2
    assert report["duplicate_trip_ids"]["count"] == 2
    assert report["missing_trip_id"]["count"] == 1
    assert report["missing_pickup_coordinates"]["count"] == 3


def test_bronze_manifest_and_partition(spark, tmp_path, monkeypatch):
    config = local_config(tmp_path)
    def page(*args):
        manifests = list((tmp_path / "reports/bronze").glob("*/manifest.json"))
        assert json.loads(manifests[0].read_text())["status"] == "RUNNING"
        return [{"trip_id": "fixture"}]
    monkeypatch.setattr(bronze, "fetch_page", page)
    manifest = bronze.run_bronze(spark, config, "2023-06-01")
    assert manifest["status"] == "SUCCESS"
    assert manifest["rows_downloaded"] == 1
    from pathlib import Path
    partition = Path(manifest["output_path"])
    assert spark.read.parquet(str(partition)).count() == 1
    assert json.loads((partition / "_bronze_run.json").read_text())["run_id"] == manifest["run_id"]
    report_dir = tmp_path / "reports/bronze" / manifest["run_id"]
    assert (report_dir / "bronze_dq_report.json").exists()
    assert (report_dir / "column_profile.csv").exists()
    assert (tmp_path / "logs" / (manifest["run_id"] + ".log")).exists()
