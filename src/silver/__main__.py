"""Read one Bronze partition, apply notebook rules, write Silver and quarantine."""
import json
import re
from pathlib import Path

from pyspark.sql import Window, functions as F

from src.common import (cleanup, close_logger, column_profile, create_logger, create_spark,
                        load_config, new_run_id, parse_args, partition_path, promote,
                        utc_now, write_csv, write_json, write_staging)


SCHEMA = {
    "trip_id": "string", "taxi_id": "string",
    "trip_start_timestamp": "timestamp", "trip_end_timestamp": "timestamp",
    "trip_seconds": "long", "trip_miles": "double",
    "pickup_census_tract": "string", "dropoff_census_tract": "string",
    "pickup_community_area": "int", "dropoff_community_area": "int",
    "fare": "double", "tips": "double", "tolls": "double", "extras": "double", "trip_total": "double",
    "payment_type": "string", "company": "string",
    "pickup_centroid_latitude": "double", "pickup_centroid_longitude": "double",
    "dropoff_centroid_latitude": "double", "dropoff_centroid_longitude": "double",
    "pickup_centroid_location": "string", "dropoff_centroid_location": "string",
}


def read_bronze(spark, root, processing_date):
    path = partition_path(root, processing_date)
    if not path.exists() or not any(path.glob("*.parquet")):
        raise FileNotFoundError(f"Bronze partition missing or without Parquet: {path}")
    return spark.read.parquet(str(path))


def cast_columns(df):
    names = [re.sub(r"\s+", "_", name.strip().lower()) for name in df.columns]
    if len(names) != len(set(names)):
        raise ValueError("Column names collide after normalization")
    df = df.toDF(*names)
    for name, dtype in SCHEMA.items():
        if name not in df.columns:
            df = df.withColumn(name, F.lit(None).cast("string"))
        if dtype == "timestamp":
            value = F.coalesce(F.try_to_timestamp(name),
                               F.try_to_timestamp(name, F.lit("MM/dd/yyyy hh:mm:ss a")))
        else:
            value = F.expr(f"try_cast(`{name}` as {dtype})")
            if dtype == "double":
                value = F.when(F.isnan(value), None).otherwise(value)
        df = df.withColumn(name, value)
    return df.select(*SCHEMA)


def add_derived_columns(df):
    return (df.withColumn("trip_date", F.to_date("trip_start_timestamp"))
            .withColumn("trip_year", F.year("trip_start_timestamp"))
            .withColumn("trip_month", F.month("trip_start_timestamp"))
            .withColumn("trip_hour", F.hour("trip_start_timestamp"))
            .withColumn("calculated_trip_seconds", F.col("trip_end_timestamp").cast("double") - F.col("trip_start_timestamp").cast("double"))
            .withColumn("trip_duration_minutes", F.col("trip_seconds") / 60)
            .withColumn("has_tip", F.coalesce(F.col("tips") > 0, F.lit(False)))
            .withColumn("tip_rate", F.when(F.col("fare") > 0, F.col("tips") / F.col("fare"))))


def rule_names(rules):
    return F.filter(F.array(*[F.when(condition, F.lit(name)) for name, condition in rules.items()]),
                    lambda name: name.isNotNull())


def apply_quality_rules(df):
    df = df.withColumn("is_exact_duplicate", F.count("*").over(Window.partitionBy(*df.columns)) > 1)
    df = df.withColumn("is_trip_id_duplicate", F.count("*").over(Window.partitionBy("trip_id")) > 1)
    errors = {
        "MISSING_TRIP_ID": F.col("trip_id").isNull() | (F.trim("trip_id") == ""),
        "INVALID_START_TIMESTAMP": F.col("trip_start_timestamp").isNull(),
        "INVALID_END_TIMESTAMP": F.col("trip_end_timestamp").isNull(),
        "END_BEFORE_START": F.col("trip_end_timestamp") < F.col("trip_start_timestamp"),
        "INVALID_DURATION": F.col("trip_seconds") <= 0,
        "NEGATIVE_DISTANCE": F.col("trip_miles") < 0,
    }
    for name in ("fare", "tips", "tolls", "extras", "trip_total"):
        errors[f"NEGATIVE_{name.upper()}"] = F.col(name) < 0
    warnings = {
        "ZERO_DISTANCE": F.col("trip_miles") == 0,
        "DURATION_MISMATCH": F.abs(F.col("trip_seconds") - F.col("calculated_trip_seconds")) > 60,
        "DUPLICATED_TRIP_ID": F.col("is_trip_id_duplicate"),
    }
    for side in ("pickup", "dropoff"):
        lat, lon = F.col(f"{side}_centroid_latitude"), F.col(f"{side}_centroid_longitude")
        warnings[f"PARTIAL_{side.upper()}_COORDINATES"] = lat.isNull() != lon.isNull()
        warnings[f"INVALID_{side.upper()}_LATITUDE"] = ~lat.between(-90, 90)
        warnings[f"INVALID_{side.upper()}_LONGITUDE"] = ~lon.between(-180, 180)
    return (df.withColumn("quality_errors", rule_names(errors))
            .withColumn("quality_warnings", rule_names(warnings))
            .withColumn("has_error", F.size("quality_errors") > 0)
            .withColumn("has_warning", F.size("quality_warnings") > 0))


def split_valid_and_quarantine(df):
    return df.filter(~F.col("has_error")), df.filter(F.col("has_error"))


def deduplicate_trips(df):
    # Prefer a valid row, then a stable ordering of business values. Exact ties are equivalent.
    values = F.to_json(F.struct(*[F.col(name) for name in SCHEMA]), options={"ignoreNullFields": "false"})
    order = Window.partitionBy("trip_id").orderBy(F.col("has_error").asc(), values.asc())
    duplicate = F.col("trip_id").isNotNull() & (F.col("_duplicate_rank") > 1)
    return (df.withColumn("_duplicate_rank", F.row_number().over(order))
            .withColumn("quality_errors", F.when(duplicate, F.array_union("quality_errors", F.array(F.lit("DUPLICATE_TRIP_ID_EXCLUDED"))))
                        .otherwise(F.col("quality_errors")))
            .withColumn("has_error", F.size("quality_errors") > 0).drop("_duplicate_rank"))


def quality_report(df, valid, quarantine):
    report = {"input_rows": df.count(), "valid_rows": valid.count(), "rejected_rows": quarantine.count(),
              "warning_rows": df.filter("has_warning").count(),
              "duplicate_trip_rows": df.filter("is_trip_id_duplicate").count(),
              "exact_duplicate_rows": df.filter("is_exact_duplicate").count()}
    for rate, count in [("rejection_rate", "rejected_rows"), ("warning_rate", "warning_rows")]:
        report[rate] = report[count] / report["input_rows"] if report["input_rows"] else 0.0
    validate_reconciliation(report)
    return report


def validate_reconciliation(report):
    if report["input_rows"] != report["valid_rows"] + report["rejected_rows"]:
        raise ValueError("Reconciliation failed: input != valid + rejected")
    if report["warning_rows"] > report["input_rows"]:
        raise ValueError("Reconciliation failed: warnings exceed input")


def rule_report(df, column, input_rows):
    counts = df.select(F.explode(column).alias("rule")).groupBy("rule").count().orderBy("rule")
    return [{"rule": row.rule, "count": row["count"], "rate": row["count"] / input_rows}
            for row in counts.collect()]


def find_source_run(input_path, reports_root, processing_date):
    marker = input_path / "_bronze_run.json"
    if marker.exists():
        return json.loads(marker.read_text(encoding="utf-8"))["run_id"]
    # Compatibility with Bronze partitions written before the local provenance marker.
    candidates = []
    for path in (Path(reports_root) / "bronze").glob("*/manifest.json"):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if (manifest.get("status") == "SUCCESS" and manifest.get("processing_date") == processing_date
                and Path(manifest.get("output_path", ".")).resolve() == input_path):
            candidates.append((manifest.get("finished_at", ""), manifest["run_id"]))
    if not candidates:
        raise ValueError("Bronze lineage missing: run Bronze for this partition first")
    return max(candidates)[1]


def run_silver(spark, config, processing_date):
    settings = config["silver"]
    input_path = partition_path(settings["input_path"], processing_date)
    final = partition_path(settings["output_path"], processing_date)
    quarantine_path = partition_path(settings["quarantine_path"], processing_date)
    if len({input_path, final, quarantine_path}) != 3:
        raise ValueError("Bronze, Silver and quarantine must have different roots")
    run_id = new_run_id("SLV")
    reports = Path(settings["reports_path"]) / "silver" / run_id
    staging = final.parent / "_tmp" / run_id
    rejected_staging = quarantine_path.parent / "_tmp" / run_id
    logger = create_logger(run_id, settings["logs_path"])
    manifest = {"run_id": run_id, "source_run_id": None, "processing_date": processing_date,
                "status": "RUNNING", "input_rows": 0, "valid_rows": 0, "rejected_rows": 0,
                "warning_rows": 0, "rejection_rate": 0.0, "warning_rate": 0.0,
                "input_path": str(input_path), "output_path": str(final), "quarantine_path": str(quarantine_path),
                "started_at": utc_now(), "finished_at": None, "error_message": None}
    frame = None
    try:
        write_json(manifest, reports / "manifest.json")
        logger.info("starting Silver | processing_date=%s", processing_date)
        bronze = read_bronze(spark, settings["input_path"], processing_date)
        source_run = find_source_run(input_path, settings["reports_path"], processing_date)
        manifest["source_run_id"] = source_run
        typed = cast_columns(bronze)
        derived = add_derived_columns(typed)
        checked = deduplicate_trips(apply_quality_rules(derived))
        frame = (checked.withColumn("_run_id", F.lit(run_id))
                 .withColumn("_source_run_id", F.lit(source_run))
                 .withColumn("_processing_date", F.lit(processing_date).cast("date"))
                 .withColumn("_processing_timestamp", F.lit(manifest["started_at"]).cast("timestamp"))).cache()
        valid, quarantine = split_valid_and_quarantine(frame)
        report = quality_report(frame, valid, quarantine)
        manifest.update(report)
        write_json(report, reports / "dq_report.json")
        for column, filename in [("quality_errors", "errors_report.csv"), ("quality_warnings", "warnings_report.csv")]:
            write_csv(rule_report(frame, column, report["input_rows"]), ["rule", "count", "rate"], reports / filename)
        write_csv(column_profile(frame), ["column", "dtype", "null_count", "null_rate", "distinct_count"], reports / "column_profile.csv")
        write_staging(valid, staging, report["valid_rows"])
        write_staging(quarantine, rejected_staging, report["rejected_rows"])
        promote(staging, final)
        promote(rejected_staging, quarantine_path)
        manifest["status"] = "SUCCESS"
        logger.info("success | source_run_id=%s | quality=%s", source_run, report)
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
        cleanup(rejected_staging)
        close_logger(logger)
    return manifest


def main():
    args = parse_args("Chicago Taxi Trips: Silver")
    config = load_config(args.config)
    spark = create_spark("chicago-taxi-silver")
    try:
        run_silver(spark, config, args.processing_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
