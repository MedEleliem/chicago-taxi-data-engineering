import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.gold.__main__ import build_tables, run_gold, validate_tables
from test_silver import checked_frame


def test_gold_metrics_and_geo(spark):
    silver = checked_frame(spark, [{"trip_id": "a", "taxi_id": "t1", "company": None, "payment_type": None,
                                    "pickup_centroid_latitude": "41.88", "pickup_centroid_longitude": "-87.63",
                                    "dropoff_centroid_latitude": "41.9", "dropoff_centroid_longitude": "-87.62"},
                                   {"trip_id": "b", "taxi_id": "t2", "trip_total": "20", "tips": "0"}]).cache()
    try:
        tables = build_tables(silver)
        assert validate_tables(tables, 2)["passed"]
        assert tables["daily"].first().total_revenue == 32.0
        assert tables["daily"].first().tipped_trip_rate == .5
        assert tables["payments"].first().payment_type == "UNKNOWN"
        assert tables["geo"].count() == 1
        assert tables["kpi_summary"].first().unique_taxis == 2
        assert not validate_tables(tables, 3)["passed"]
    finally:
        silver.unpersist()


def test_gold_rejects_error_rows(spark):
    with pytest.raises(ValueError, match="Trusted"):
        build_tables(checked_frame(spark, [{"trip_seconds": "0"}]))


def test_gold_publication_and_failure_preserves_previous(spark, tmp_path, monkeypatch):
    from src.gold import __main__ as gold
    source = tmp_path / "silver/processing_date=2023-06-01"
    df = checked_frame(spark, [{}]).withColumn("_run_id", F.lit("SLV_FIXTURE"))
    df.write.parquet(str(source))
    report = tmp_path / "reports/silver/SLV_FIXTURE"
    report.mkdir(parents=True)
    (report / "manifest.json").write_text(json.dumps({"run_id": "SLV_FIXTURE", "source_run_id": "BRZ_FIXTURE",
        "status": "SUCCESS", "processing_date": "2023-06-01", "output_path": str(source), "finished_at": "2026-09-13"}))
    config = {"gold": {"input_path": str(tmp_path / "silver"), "output_path": str(tmp_path / "gold"),
                       "reports_path": str(tmp_path / "reports"), "logs_path": str(tmp_path / "logs")}}
    result = run_gold(spark, config, "2023-06-01")
    output = Path(result["output_path"])
    assert result["status"] == "SUCCESS" and result["source_run_id"] == "SLV_FIXTURE"
    assert spark.read.parquet(str(output / "daily")).first().trip_count == 1
    monkeypatch.setattr(gold, "validate_tables", lambda *args: {"passed": False})
    with pytest.raises(ValueError, match="reconciliation"):
        run_gold(spark, config, "2023-06-01")
    assert json.loads((output / "_manifest.json").read_text())["run_id"] == result["run_id"]
