import json

import pytest

from src.bronze import __main__ as bronze
from src.silver import __main__ as silver
from src.common import load_config, promote, write_staging
from test_bronze import local_config


ROW = {"trip_id": "one", "trip_start_timestamp": "2023-06-01T10:00:00.000",
       "trip_end_timestamp": "2023-06-01T10:10:00.000", "trip_seconds": "600",
       "trip_miles": "2.5", "fare": "10", "tips": "2", "tolls": "0", "extras": "0", "trip_total": "12"}


def checked_frame(spark, overrides):
    raw = bronze.records_to_frame(spark, [{**ROW, **row} for row in overrides], list(silver.SCHEMA))
    return silver.apply_quality_rules(silver.add_derived_columns(silver.cast_columns(raw)))


def test_casting_errors_warnings_and_split(spark):
    frame = checked_frame(spark, [{}, {"trip_id": "bad", "trip_seconds": "0", "trip_miles": "-1",
                                     "trip_end_timestamp": "2023-06-01T09:00:00"},
                                    {"trip_id": "warn", "trip_miles": "0", "pickup_centroid_latitude": "91"}]).cache()
    try:
        rows = {row.trip_id: row for row in frame.collect()}
        assert dict(frame.dtypes)["fare"] == "double"
        assert rows["one"].tip_rate == pytest.approx(0.2)
        assert rows["one"].trip_duration_minutes == 10
        assert {"INVALID_DURATION", "NEGATIVE_DISTANCE", "END_BEFORE_START"} <= set(rows["bad"].quality_errors)
        assert {"ZERO_DISTANCE", "INVALID_PICKUP_LATITUDE", "PARTIAL_PICKUP_COORDINATES"} <= set(rows["warn"].quality_warnings)
        valid, quarantine = silver.split_valid_and_quarantine(frame)
        report = silver.quality_report(frame, valid, quarantine)
        assert (report["input_rows"], report["valid_rows"], report["rejected_rows"], report["warning_rows"]) == (3, 2, 1, 2)
    finally:
        frame.unpersist()


def test_null_policy_duplicates_and_all_money_rules(spark):
    money = {name: "-1" for name in ("fare", "tips", "tolls", "extras", "trip_total")}
    rows = checked_frame(spark, [{"trip_id": None, "trip_start_timestamp": "bad", "trip_seconds": "ABC", **money}] * 2).collect()
    assert len(rows) == 2 and all(row.is_exact_duplicate for row in rows)
    assert rows[0].trip_seconds is None
    assert "INVALID_DURATION" not in rows[0].quality_errors
    assert {"MISSING_TRIP_ID", "INVALID_START_TIMESTAMP", *(f"NEGATIVE_{name.upper()}" for name in money)} <= set(rows[0].quality_errors)


def test_reconciliation_failure():
    with pytest.raises(ValueError, match="Reconciliation"):
        silver.validate_reconciliation({"input_rows": 3, "valid_rows": 1, "rejected_rows": 1, "warning_rows": 0})


def test_deduplication_prefers_valid_and_is_stable(spark):
    checked = checked_frame(spark, [{"trip_id": "same", "trip_seconds": "0"},
                                    {"trip_id": "same", "trip_total": "15"},
                                    {"trip_id": "same", "trip_total": "12"}])
    for frame in (checked, checked.repartition(2)):
        result = silver.deduplicate_trips(frame)
        valid, quarantine = silver.split_valid_and_quarantine(result)
        assert valid.count() == 1 and quarantine.count() == 2
        assert valid.first().trip_total == 12
        assert all("DUPLICATE_TRIP_ID_EXCLUDED" in row.quality_errors for row in quarantine.collect())


def test_staging_failure_keeps_old_partition(spark, tmp_path):
    final = tmp_path / "final"
    final.mkdir()
    (final / "old.txt").write_text("old")
    with pytest.raises(ValueError):
        write_staging(spark.range(1), tmp_path / "staging", 2)
    assert (final / "old.txt").read_text() == "old"


def test_bronze_to_silver_and_rerun(spark, tmp_path, monkeypatch):
    config = local_config(tmp_path)
    monkeypatch.setattr(bronze, "fetch_page", lambda *args: [ROW, {**ROW, "trip_id": "bad", "trip_seconds": "0"}])
    source = bronze.run_bronze(spark, config, "2023-06-01")
    result = silver.run_silver(spark, config, "2023-06-01")
    assert result["status"] == "SUCCESS" and result["source_run_id"] == source["run_id"]
    assert (result["input_rows"], result["valid_rows"], result["rejected_rows"]) == (2, 1, 1)
    for name in ("dq_report.json", "errors_report.csv", "warnings_report.csv", "manifest.json"):
        assert (tmp_path / "reports/silver" / result["run_id"] / name).exists()
    rows = spark.read.parquet(result["output_path"]).collect()
    assert rows[0]._source_run_id == source["run_id"]
    second = silver.run_silver(spark, config, "2023-06-01")
    assert second["run_id"] != result["run_id"]
    assert spark.read.parquet(second["output_path"]).count() == 1
    assert spark.read.parquet(second["quarantine_path"]).count() == 1


def test_empty_bronze_to_silver(spark, tmp_path, monkeypatch):
    config = local_config(tmp_path)
    monkeypatch.setattr(bronze, "fetch_page", lambda *args: [])
    bronze.run_bronze(spark, config, "2023-06-02")
    result = silver.run_silver(spark, config, "2023-06-02")
    assert result["status"] == "SUCCESS"
    assert result["input_rows"] == result["valid_rows"] == result["rejected_rows"] == 0
    assert result["warning_rate"] == result["rejection_rate"] == 0


def test_missing_partition_records_failure(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        silver.run_silver(spark, local_config(tmp_path), "2023-06-03")
    manifest = next((tmp_path / "reports/silver").glob("*/manifest.json"))
    assert json.loads(manifest.read_text())["status"] == "FAILED"
