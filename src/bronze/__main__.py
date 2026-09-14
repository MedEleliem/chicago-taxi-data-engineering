"""Download one day from SODA, measure quality, and write Bronze Parquet."""
import json
import os
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from pyspark.sql import functions as F, types as T

from src.common import (cleanup, close_logger, column_profile, create_logger, create_spark,
                        load_config, new_run_id, parse_args, partition_path, promote,
                        utc_now, write_csv, write_json, write_staging)


def build_date_window(processing_date):
    partition_path(".", processing_date)
    start = date.fromisoformat(processing_date)
    return f"{start}T00:00:00", f"{start + timedelta(days=1)}T00:00:00"


def build_where_clause(processing_date):
    start, end = build_date_window(processing_date)
    return f"trip_start_timestamp >= '{start}' AND trip_start_timestamp < '{end}'"


def fetch_page(source, ingestion, where, offset):
    attempts = int(ingestion.get("max_retries", 3))
    if attempts < 1:
        raise ValueError("max_retries must be at least 1")
    token = os.getenv(source.get("app_token_env", "CHICAGO_APP_TOKEN"))
    headers = {"X-App-Token": token} if token else {}
    url = f"https://{source['domain']}/resource/{source['dataset_id']}.json"
    for attempt in range(attempts):
        try:
            with requests.get(url, headers=headers, timeout=ingestion.get("timeout_seconds", 120),
                              params={"$where": where, "$order": "trip_start_timestamp, trip_id",
                                      "$limit": ingestion.get("page_size", 50000), "$offset": offset}) as response:
                response.raise_for_status()
                records = response.json()
                if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
                    raise ValueError("SODA response must be a list of records")
                return records
        except (requests.Timeout, requests.HTTPError) as exc:
            status = exc.response.status_code if exc.response is not None else None
            retryable = isinstance(exc, requests.Timeout) or status in {429, 500, 502, 503, 504}
            if not retryable or attempt == attempts - 1:
                raise
            time.sleep(ingestion.get("retry_backoff_seconds", 2))


def fetch_all_pages(source, ingestion, processing_date, logger):
    limit = int(ingestion.get("page_size", 50000))
    maximum = ingestion.get("max_pages")
    if limit <= 0 or (maximum is not None and maximum <= 0):
        raise ValueError("page_size and max_pages must be positive")
    records, pages, complete = [], 0, False
    while maximum is None or pages < maximum:
        page = fetch_page(source, ingestion, build_where_clause(processing_date), len(records))
        pages += 1
        records.extend(page)
        logger.info("page downloaded | page=%s | rows=%s | total=%s", pages, len(page), len(records))
        if len(page) < limit:
            complete = True
            break
    return records, {"pages_downloaded": pages, "rows_downloaded": len(records),
                     "pagination_complete": complete}


def records_to_frame(spark, records, expected_columns):
    columns = list(dict.fromkeys(expected_columns + [key for row in records for key in row]))
    schema = T.StructType([T.StructField(name, T.StringType(), True) for name in columns])
    # SODA locations may be JSON objects; Bronze stores their representation without business casting.
    rows = [{key: (json.dumps(value, sort_keys=True) if isinstance(value, (dict, list))
                   else str(value) if value is not None else None)
             for key, value in row.items()} for row in records]
    return spark.createDataFrame(rows, schema)


def bronze_quality(df):
    def missing(name):
        return F.lit(True) if name not in df.columns else F.col(name).isNull() | (F.trim(F.col(name)) == "")

    checks = {f"missing_{label}": missing(name) for label, name in {
        "trip_id": "trip_id", "start_timestamp": "trip_start_timestamp",
        "end_timestamp": "trip_end_timestamp", "trip_seconds": "trip_seconds",
        "trip_miles": "trip_miles", "fare": "fare", "trip_total": "trip_total"}.items()}
    for side in ("pickup", "dropoff"):
        checks[f"missing_{side}_coordinates"] = missing(f"{side}_centroid_latitude") | missing(f"{side}_centroid_longitude")
    values = df.agg(F.count("*").alias("rows"), *[
        F.count(F.when(condition, 1)).alias(name) for name, condition in checks.items()]).first()
    count = values.rows
    report = {"row_count": count, "column_count": len(df.columns)}
    for name in checks:
        report[name] = {"count": values[name], "rate": values[name] / count if count else 0.0}
    for name, frame, keys in [("duplicate_rows", df, df.columns),
                              ("duplicate_trip_ids", df.filter(~missing("trip_id")), ["trip_id"])]:
        duplicates = frame.groupBy(*keys).count().filter("count > 1").agg(F.sum("count")).first()[0] or 0
        report[name] = {"count": duplicates, "rate": duplicates / count if count else 0.0}
    return report


def run_bronze(spark, config, processing_date):
    settings = config["bronze"]
    output = settings["output"]
    final = partition_path(output["path"], processing_date)
    run_id = new_run_id("BRZ")
    staging = final.parent / "_tmp" / run_id
    reports = Path(output["reports_path"]) / "bronze" / run_id
    logger = create_logger(run_id, output["logs_path"])
    manifest = {"run_id": run_id, "processing_date": processing_date, "status": "RUNNING",
                "started_at": utc_now(), "finished_at": None, "source": settings["source"],
                "rows_downloaded": 0, "pages_downloaded": 0, "pagination_complete": False,
                "output_path": str(final), "column_count": 0, "error_message": None}
    frame = None
    try:
        write_json(manifest, reports / "manifest.json")
        logger.info("starting ingestion | processing_date=%s", processing_date)
        records, pagination = fetch_all_pages(settings["source"], settings["ingestion"], processing_date, logger)
        manifest.update(pagination)
        frame = records_to_frame(spark, records, settings["ingestion"]["expected_columns"]).cache()
        manifest["column_count"] = len(frame.columns)
        write_json(bronze_quality(frame), reports / "bronze_dq_report.json")
        write_csv(column_profile(frame), ["column", "dtype", "null_count", "null_rate", "distinct_count"], reports / "column_profile.csv")
        logger.info("writing parquet | rows downloaded=%s", len(records))
        write_staging(frame, staging, len(records))
        write_json(records, staging / "_raw/records.json")
        logger.info("validation complete")
        write_json({"run_id": run_id}, staging / "_bronze_run.json")
        promote(staging, final)
        manifest["status"] = "SUCCESS"
        logger.info("success | output=%s | pagination_complete=%s", final, pagination["pagination_complete"])
    except Exception as exc:
        manifest.update(status="FAILED", error_message=str(exc))
        logger.exception("failure")
        raise
    finally:
        manifest["finished_at"] = utc_now()
        write_json(manifest, reports / "manifest.json")
        if frame is not None:
            frame.unpersist()
        cleanup(staging)
        close_logger(logger)
    return manifest


def main():
    args = parse_args("Chicago Taxi Trips: Bronze")
    config = load_config(args.config)
    spark = create_spark("chicago-taxi-bronze")
    try:
        run_bronze(spark, config, args.processing_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
