"""Analytical Gold tables from one already-validated Silver partition."""
import json
from pathlib import Path

from pyspark.sql import functions as F

from src.common import (cleanup, close_logger, create_logger, create_spark, load_config,
                        new_run_id, parse_args, partition_path, promote, utc_now, write_json, write_staging)

TRIP_COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_date", "trip_hour",
    "trip_duration_minutes", "trip_miles", "fare", "tips", "trip_total", "has_tip",
    "payment_type", "company", "pickup_community_area", "dropoff_community_area",
    "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude",
]


def aggregate_metrics(df, keys):
    return (df.groupBy(*keys).agg(
        F.count("*").alias("trip_count"), F.coalesce(F.sum("trip_total"), F.lit(0.0)).alias("total_revenue"),
        F.avg("trip_total").alias("avg_trip_total"), F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.avg("trip_miles").alias("avg_trip_miles"), F.coalesce(F.sum("trip_miles"), F.lit(0.0)).alias("total_trip_miles"),
        F.coalesce(F.sum("tips"), F.lit(0.0)).alias("total_tips"), F.avg("tips").alias("avg_tip"),
        F.sum(F.when(F.col("has_tip"), 1).otherwise(0)).alias("tipped_trip_count"))
        .withColumn("tipped_trip_rate", F.col("tipped_trip_count") / F.col("trip_count")))


def build_tables(silver):
    if silver.filter(F.col("has_error") | F.col("has_error").isNull()).limit(1).count():
        raise ValueError("Gold requires Silver Trusted: error rows found")
    trips = silver.select(*TRIP_COLUMNS).fillna({"company": "UNKNOWN", "payment_type": "UNKNOWN"})
    tables = {
        "daily": aggregate_metrics(trips, ["trip_date"]).withColumnRenamed("avg_trip_total", "avg_revenue_per_trip"),
        "hourly": aggregate_metrics(trips, ["trip_date", "trip_hour"]),
        "zones": aggregate_metrics(trips, ["pickup_community_area"]),
        "payments": aggregate_metrics(trips, ["payment_type"]),
        "companies": aggregate_metrics(trips, ["company"]),
        "trips": trips,
    }
    coordinates = F.lit(True)
    for side in ("pickup", "dropoff"):
        coordinates = coordinates & F.col(f"{side}_centroid_latitude").between(-90, 90) & F.col(f"{side}_centroid_longitude").between(-180, 180)
    tables["geo"] = trips.filter(coordinates)
    tables["kpi_summary"] = silver.agg(
        F.count("*").alias("total_trips"), F.coalesce(F.sum("trip_total"), F.lit(0.0)).alias("total_revenue"),
        F.avg("trip_total").alias("avg_trip_total"), F.avg("trip_miles").alias("avg_trip_miles"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        F.coalesce(F.sum("tips"), F.lit(0.0)).alias("total_tips"),
        F.coalesce(F.avg(F.when(F.col("has_tip"), 1.0).otherwise(0.0)), F.lit(0.0)).alias("tipped_trip_rate"),
        F.count_distinct("taxi_id").alias("unique_taxis"), F.count_distinct("company").alias("unique_companies"))
    return tables


def validate_tables(tables, input_rows):
    counts = {name: int(tables[name].agg(F.sum("trip_count")).first()[0] or 0)
              for name in ("daily", "hourly", "zones", "payments", "companies")}
    checks = {name: count == input_rows for name, count in counts.items()}
    checks["kpi_summary"] = tables["kpi_summary"].first().total_trips == input_rows
    return {"input_rows": input_rows, "counts": counts, "checks": checks, "passed": all(checks.values())}


def source_manifest(silver, reports_root, processing_date, input_path):
    run_ids = [row[0] for row in silver.select("_run_id").distinct().limit(2).collect()]
    if len(run_ids) > 1:
        raise ValueError("Silver partition mixes runs")
    manifests = []
    for path in (Path(reports_root) / "silver").glob("*/manifest.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (payload.get("status") == "SUCCESS" and payload.get("processing_date") == processing_date
                and Path(payload.get("output_path", ".")).resolve() == input_path
                and (not run_ids or payload["run_id"] == run_ids[0])):
            manifests.append(payload)
    if not manifests:
        raise ValueError("No successful source Silver manifest")
    return max(manifests, key=lambda item: item["finished_at"])


def run_gold(spark, config, processing_date):
    settings = config["gold"]
    input_path = partition_path(settings["input_path"], processing_date)
    final = partition_path(settings["output_path"], processing_date)
    if input_path == final:
        raise ValueError("Gold output must differ from Silver input")
    run_id = new_run_id("GLD")
    staging = final.parent / "_tmp" / run_id
    reports = Path(settings["reports_path"]) / "gold" / run_id
    logger = create_logger(run_id, settings["logs_path"])
    manifest = {"run_id": run_id, "source_run_id": None, "processing_date": processing_date, "status": "RUNNING",
                "started_at": utc_now(), "finished_at": None, "input_path": str(input_path), "output_path": str(final),
                "input_rows": 0, "output_rows": {}, "error_message": None}
    silver = None
    try:
        write_json(manifest, reports / "manifest.json")
        silver = spark.read.parquet(str(input_path)).cache()
        source = source_manifest(silver, settings["reports_path"], processing_date, input_path)
        manifest.update(source_run_id=source["run_id"], bronze_run_id=source["source_run_id"], input_rows=silver.count())
        tables = build_tables(silver)
        validation = validate_tables(tables, manifest["input_rows"])
        write_json(validation, reports / "gold_validation_report.json")
        if not validation["passed"]:
            raise ValueError("Critical Gold reconciliation failed")
        for name, table in tables.items():
            count = table.count()
            write_staging(table, staging / name, count)
            manifest["output_rows"][name] = count
        manifest.update(status="SUCCESS", finished_at=utc_now())
        write_json(manifest, staging / "_manifest.json")
        write_json(validation, staging / "_validation.json")
        promote(staging, final)
        logger.info("Gold success | input_rows=%s | tables=%s", manifest["input_rows"], manifest["output_rows"])
    except Exception as exc:
        manifest.update(status="FAILED", error_message=str(exc))
        logger.exception("Gold failure")
        raise
    finally:
        manifest["finished_at"] = utc_now()
        write_json(manifest, reports / "manifest.json")
        if silver is not None:
            silver.unpersist()
        cleanup(staging)
        close_logger(logger)
    return manifest


def main():
    args = parse_args("Chicago Taxi Trips: Gold")
    spark = create_spark("chicago-taxi-gold")
    try:
        run_gold(spark, load_config(args.config), args.processing_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
