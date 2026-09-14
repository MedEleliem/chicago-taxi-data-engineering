# Code complet de la version active

Instantane genere depuis les sources. Les fichiers executables restent la reference.

Lire les guides architecture, bronze, silver, gold, api, frontend et dataops pour
l'explication des fonctions. Les secrets et les modules historiques sont exclus.

## src/common.py

```python
"""Small local helpers shared by the Bronze and Silver command-line jobs."""
import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import yaml
from pyspark.sql import SparkSession, functions as F


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def new_run_id(layer):
    return f"{layer}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid4().hex[:8]}"


def parse_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    return parser.parse_args()


def load_config(path):
    with Path(path).open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def partition_path(root, processing_date):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", processing_date):
        raise ValueError("processing_date must be YYYY-MM-DD")
    datetime.strptime(processing_date, "%Y-%m-%d")
    return Path(root).resolve() / f"processing_date={processing_date}"


def create_spark(name):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    hadoop = Path(__file__).resolve().parents[1] / ".hadoop"
    if (hadoop / "bin/winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["PATH"] = str(hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")
    master = os.getenv("SPARK_MASTER", "local[1]" if os.name == "nt" else "local[*]")
    return (SparkSession.builder.master(master).appName(name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8").getOrCreate())


def create_logger(run_id, root):
    Path(root).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(run_id)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    for handler in [logging.FileHandler(Path(root) / f"{run_id}.log", encoding="utf-8"),
                    logging.StreamHandler()]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def close_logger(logger):
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)


def write_json(payload, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temporary.replace(path)


def write_csv(rows, columns, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def column_profile(df):
    expressions = [F.count("*").alias("rows")]
    for i, name in enumerate(df.columns):
        expressions.extend([F.count(F.when(F.col(name).isNull(), 1)).alias(f"n{i}"),
                            F.count_distinct(name).alias(f"d{i}")])
    result = df.agg(*expressions).first()
    return [{"column": name, "dtype": dtype, "null_count": result[f"n{i}"],
             "null_rate": result[f"n{i}"] / result.rows if result.rows else 0.0,
             "distinct_count": result[f"d{i}"]}
            for i, (name, dtype) in enumerate(df.dtypes)]


def write_staging(df, path, expected_rows):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("errorifexists").parquet(str(path))
    restored = df.sparkSession.read.parquet(str(path))
    if restored.count() != expected_rows or restored.dtypes != df.dtypes:
        raise ValueError(f"Parquet validation failed: {path}")


def promote(staging, final):
    staging, final = Path(staging).resolve(), Path(final).resolve()
    backup = staging.with_name(staging.name + ".previous")
    if backup.exists():
        raise FileExistsError(f"Recovery directory already exists: {backup}")
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        final.rename(backup)
    try:
        staging.rename(final)
    except Exception:
        if backup.exists():
            backup.rename(final)
        raise
    if backup.exists():
        shutil.rmtree(backup)


def cleanup(path):
    path = Path(path).resolve()
    if path.exists():
        shutil.rmtree(path)
```

## src/object_store.py

```python
"""S3 publications: immutable run files, checksums, and one conditional latest pointer."""
import hashlib
import json
import os
import re
import shutil
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"],
                        region_name="us-east-1", config=Config(signature_version="s3v4",
                        s3={"addressing_style": "path"}, retries={"max_attempts": 3},
                        connect_timeout=5, read_timeout=60))


def bucket():
    return os.environ.get("S3_BUCKET", "chicago-taxi")


def pointer_key(layer, day):
    if layer not in ("bronze", "silver", "gold") or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        raise ValueError("Invalid publication identifier")
    return f"{layer}/processing_date={day}/latest.json"


def read_pointer(s3, layer, day):
    try:
        response = s3.get_object(Bucket=bucket(), Key=pointer_key(layer, day))
        with response["Body"] as stream:
            return json.loads(stream.read()), response["ETag"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None, None
        raise


def publish(manifest, layer, root):
    root = Path(root).resolve()
    s3 = client()
    day, run_id = manifest["processing_date"], manifest["run_id"]
    _, previous_etag = read_pointer(s3, layer, day)
    directories = [Path(manifest["output_path"])]
    if layer == "silver":
        directories.append(Path(manifest["quarantine_path"]))
    lineage = [(layer, run_id)]
    if layer in ("silver", "gold"):
        lineage.append(("bronze" if layer == "silver" else "silver", manifest["source_run_id"]))
    if layer == "gold":
        lineage.append(("bronze", manifest["bronze_run_id"]))
    directories += [root / "reports" / name / identifier for name, identifier in lineage]
    files = {file.resolve() for folder in directories for file in folder.rglob("*") if file.is_file()}
    for _, identifier in lineage:
        for file in (root / "reports/audit" / f"{identifier}.json", root / "logs" / f"{identifier}.log"):
            if file.exists():
                files.add(file.resolve())
    prefix = f"{layer}/processing_date={day}/runs/{run_id}/"
    inventory = []
    for file in sorted(files):
        relative = file.relative_to(root).as_posix()
        with file.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        with file.open("rb") as stream:
            s3.put_object(Bucket=bucket(), Key=prefix + relative, Body=stream,
                          Metadata={"sha256": digest}, IfNoneMatch="*")
        head = s3.head_object(Bucket=bucket(), Key=prefix + relative)
        if head["ContentLength"] != file.stat().st_size or head["Metadata"].get("sha256") != digest:
            raise ValueError("S3 upload verification failed")
        inventory.append({"path": relative, "size": file.stat().st_size, "sha256": digest})
    publication = {"layer": layer, "processing_date": day, "run_id": run_id,
                   "source_run_id": manifest.get("source_run_id"), "prefix": prefix,
                   "original_root": str(root), "files": inventory,
                   "output": Path(manifest["output_path"]).resolve().relative_to(root).as_posix()}
    if layer == "silver":
        publication["quarantine"] = Path(manifest["quarantine_path"]).resolve().relative_to(root).as_posix()
    body = json.dumps(publication).encode()
    s3.put_object(Bucket=bucket(), Key=prefix + "publication.json", Body=body, IfNoneMatch="*")
    condition = {"IfMatch": previous_etag} if previous_etag else {"IfNoneMatch": "*"}
    s3.put_object(Bucket=bucket(), Key=pointer_key(layer, day), Body=body, **condition)
    return publication


def contained(root, relative):
    path = (Path(root) / relative).resolve()
    if not path.is_relative_to(Path(root).resolve()) or path == Path(root).resolve():
        raise ValueError("Object path escapes snapshot directory")
    return path


def download(publication, cache_root):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", publication["run_id"]):
        raise ValueError("Invalid run_id")
    final = Path(cache_root).resolve() / publication["run_id"]
    if (final / "_download_complete.json").exists():
        return final
    staging = final.with_name(final.name + "-" + uuid4().hex)
    staging.mkdir(parents=True)
    s3 = client()
    try:
        for item in publication["files"]:
            target = contained(staging, item["path"])
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket(), publication["prefix"] + item["path"], str(target))
            with target.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if target.stat().st_size != item["size"] or digest != item["sha256"]:
                raise ValueError("S3 download checksum mismatch")
        (staging / "_download_complete.json").write_text(json.dumps(publication), encoding="utf-8")
        staging.rename(final)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return final


def restore(layer, day, root):
    from src.common import promote
    root = Path(root).resolve()
    publication, _ = read_pointer(client(), layer, day)
    if publication is None:
        raise FileNotFoundError(f"No S3 publication: {layer} {day}")
    snapshot = download(publication, root / "object-cache")
    for key in ("output", "quarantine"):
        if key in publication:
            target = contained(root, publication[key])
            staging = root / "_restore" / uuid4().hex
            shutil.copytree(contained(snapshot, publication[key]), staging)
            promote(staging, target)
    for folder in ("reports", "logs"):
        if (snapshot / folder).exists():
            shutil.copytree(snapshot / folder, root / folder, dirs_exist_ok=True)
    # Local workspaces can differ; only metadata paths are rebased, never business values.
    for item in publication["files"]:
        if Path(item["path"]).name in ("manifest.json", "_manifest.json"):
            path = contained(root, item["path"])
            payload = json.loads(path.read_text(encoding="utf-8"))
            for key in ("input_path", "output_path", "quarantine_path"):
                value = payload.get(key)
                old = publication["original_root"]
                if value and value.startswith(old + os.sep):
                    payload[key] = str(root / value[len(old) + 1:])
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return publication


def gold_publications(cache_root):
    s3 = client()
    result = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket(), Prefix="gold/"):
        for item in page.get("Contents", []):
            if re.fullmatch(r"gold/processing_date=\d{4}-\d{2}-\d{2}/latest.json", item["Key"]):
                day = item["Key"].split("/")[1].split("=")[1]
                publication, _ = read_pointer(s3, "gold", day)
                snapshot = download(publication, cache_root)
                path = contained(snapshot, publication["output"]) / "_manifest.json"
                manifest = json.loads(path.read_text(encoding="utf-8"))
                validation = json.loads((path.parent / "_validation.json").read_text(encoding="utf-8"))
                if manifest["run_id"] != publication["run_id"] or not validation.get("passed"):
                    raise ValueError("Invalid Gold object publication")
                result.append((path, manifest))
    return sorted(result, key=lambda item: item[1]["processing_date"])
```

## src/bronze/__main__.py

```python
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
```

## src/silver/__main__.py

```python
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
```

## src/gold/__main__.py

```python
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
```

## src/dataops/__main__.py

```python
"""Publication checks, configurable Silver gate and small JSON run audit."""
import argparse
import json
import math
from pathlib import Path

import pyarrow.parquet as pq

from src.common import load_config, partition_path, utc_now, write_json


def quality_gate(report, policy):
    warning, failure = policy["warning_threshold"], policy["failure_threshold"]
    if not 0 <= warning < failure <= 1:
        raise ValueError("Expected 0 <= warning_threshold < failure_threshold <= 1")
    counts = [report[name] for name in ("input_rows", "valid_rows", "rejected_rows")]
    if any(type(value) is not int or value < 0 for value in counts):
        raise ValueError("Row counts must be nonnegative integers")
    total, valid, rejected = counts
    if total != valid + rejected:
        raise ValueError("Silver reconciliation failed")
    rate = report["rejection_rate"]
    expected = rejected / total if total else 0.0
    if not math.isfinite(rate) or not math.isclose(rate, expected, abs_tol=1e-12):
        raise ValueError("Silver rejection rate disagrees with row counts")
    if (not total and not policy["allow_empty"]) or rate >= failure:
        return "FAIL"
    return "WARNING" if rate >= warning else "PASS"


def latest_manifest(reports, layer, processing_date):
    partition_path("data", processing_date)
    candidates = []
    for path in (Path(reports) / layer).glob("*/manifest.json"):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest["processing_date"] == processing_date:
            candidates.append(manifest)
    if not candidates:
        raise ValueError(f"No {layer} run for {processing_date}")
    return max(candidates, key=lambda item: item["started_at"])


def audit_run(manifest, layer, reports, check_status, message=None):
    output = manifest.get("output_rows", manifest.get("valid_rows", manifest.get("rows_downloaded", 0)))
    if isinstance(output, dict):
        output = output["trips"]
    record = {name: manifest.get(name) for name in
              ("run_id", "processing_date", "source_run_id", "started_at", "finished_at", "status")}
    record.update(layer=layer, input_rows=manifest.get("input_rows", manifest.get("rows_downloaded", 0)),
                  output_rows=output, check_status=check_status, checked_at=utc_now(), message=message)
    write_json(record, Path(reports) / "audit" / f"{manifest['run_id']}.json")
    return record


def parquet_rows(path, run_id=None):
    files = list(Path(path).glob("*.parquet"))
    if not files:
        raise ValueError(f"No Parquet files: {path}")
    count = 0
    for file in files:
        parquet = pq.ParquetFile(file)
        count += parquet.metadata.num_rows
        if run_id is not None:
            ids = parquet.read(columns=["_run_id"]).column("_run_id").unique().to_pylist()
            if any(value != run_id for value in ids):
                raise ValueError("Published Silver does not match this run")
    return count


def validate_publication(manifest, layer, policy):
    if manifest["status"] != "SUCCESS":
        raise ValueError(f"Latest {layer} run is {manifest['status']}")
    output = Path(manifest["output_path"])
    if layer == "bronze":
        marker = json.loads((output / "_bronze_run.json").read_text(encoding="utf-8"))
        if marker["run_id"] != manifest["run_id"]:
            raise ValueError("Bronze publication belongs to another run")
        if parquet_rows(output) != manifest["rows_downloaded"]:
            raise ValueError("Bronze count mismatch")
        if policy["require_complete_bronze"] and not manifest["pagination_complete"]:
            raise ValueError("Bronze pagination is incomplete; raise max_pages before publishing")
    elif layer == "silver":
        if parquet_rows(output, manifest["run_id"]) != manifest["valid_rows"]:
            raise ValueError("Silver count mismatch")
        if parquet_rows(manifest["quarantine_path"], manifest["run_id"]) != manifest["rejected_rows"]:
            raise ValueError("Quarantine count mismatch")
    else:
        marker = json.loads((output / "_manifest.json").read_text(encoding="utf-8"))
        validation = json.loads((output / "_validation.json").read_text(encoding="utf-8"))
        if marker["run_id"] != manifest["run_id"] or validation.get("passed") is not True:
            raise ValueError("Gold publication is not validated for this run")
        for table, expected in manifest["output_rows"].items():
            if parquet_rows(output / table) != expected:
                raise ValueError(f"Gold {table} count mismatch")


def run_check(config, layer, processing_date, reports_root=None):
    settings = config["bronze"]["output"] if layer == "bronze" else config[layer]
    reports = Path(reports_root or settings["reports_path"])
    manifest = latest_manifest(reports, layer, processing_date)
    try:
        validate_publication(manifest, layer, config["dataops"])
        status = quality_gate(manifest, config["dataops"]) if layer == "silver" else "PASS"
        if layer != "bronze":
            source_layer = "bronze" if layer == "silver" else "silver"
            source = latest_manifest(reports, source_layer, processing_date)
            if source["status"] != "SUCCESS" or source["run_id"] != manifest["source_run_id"]:
                raise ValueError("Source lineage is not the latest successful publication")
        record = audit_run(manifest, layer, reports, status)
    except Exception as exc:
        audit_run(manifest, layer, reports, "FAIL", str(exc))
        raise
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    parser.add_argument("--reports-root", help="Optional demo reports directory")
    args = parser.parse_args()
    record = run_check(load_config(args.config), args.layer, args.processing_date, args.reports_root)
    print(json.dumps(record, indent=2))
    if record["check_status"] == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
```

## src/api/main.py

```python
from datetime import date
import os
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
import requests
from botocore.exceptions import BotoCoreError, ClientError

from src.api import service

app = FastAPI(title="Chicago Taxi Intelligence", version="1.0.0")


@app.exception_handler(BotoCoreError)
@app.exception_handler(ClientError)
def unavailable_storage(request, exc):
    return JSONResponse(status_code=503, content={"detail": "Object storage is temporarily unavailable."})


@app.exception_handler(FileNotFoundError)
def missing_data(request, exc):
    return JSONResponse(status_code=503, content={"detail": "Published data or lineage reports are unavailable."})


def filters(start_date: date | None = None, end_date: date | None = None, company: str | None = None,
            payment_type: str | None = None, pickup_area: int | None = None):
    if start_date and end_date and start_date > end_date:
        raise HTTPException(422, "start_date must not exceed end_date")
    return dict(start_date=start_date, end_date=end_date, company=company, payment_type=payment_type, pickup_area=pickup_area)


@app.get("/api/health")
def health():
    published = service.publications()
    options = {"companies": [], "payments": []}
    if published:
        df = service.trips({})
        options = {"companies": sorted(df.company.unique().tolist()), "payments": sorted(df.payment_type.unique().tolist())}
    return {"status": "ok", "ready": bool(published), "mode": "demo" if (service.data_root() / "_demo.json").exists() else "live",
            "dates": [item[1]["processing_date"] for item in published], **options}


@app.get("/api/kpis")
def get_kpis(selected: dict = Depends(filters)):
    return service.kpis(service.trips(selected))


@app.get("/api/daily")
@app.get("/api/hourly")
@app.get("/api/zones")
@app.get("/api/payments")
@app.get("/api/companies")
def get_metrics(request: Request, selected: dict = Depends(filters)):
    return service.metrics(service.trips(selected), request.url.path.rsplit("/", 1)[-1])


@app.get("/api/trips")
def get_trips(selected: dict = Depends(filters), limit: int = Query(50, ge=1, le=1000), offset: int = Query(0, ge=0),
              search: str = "", sort: str = "trip_start_timestamp", descending: bool = True):
    if sort not in {"trip_start_timestamp", "trip_total", "trip_miles", "company"}:
        raise HTTPException(422, "Unsupported sort column")
    df = service.trips(selected)
    if search:
        df = df[df.trip_id.str.contains(search, case=False, regex=False) | df.company.str.contains(search, case=False, regex=False)]
    return {"total": len(df), "offset": offset, "limit": limit,
            "items": service.records(df.sort_values([sort, "trip_id"], ascending=not descending).iloc[offset:offset + limit])}


@app.get("/api/trips/{trip_id}")
def get_trip(trip_id: str, selected: dict = Depends(filters)):
    df = service.trips(selected)
    rows = df[df.trip_id == trip_id]
    if rows.empty:
        raise HTTPException(404, "Trip not found")
    return {"trip": service.records(rows.iloc[:1])[0], "matching_rows": len(rows)}


@app.get("/api/geo/pickups")
@app.get("/api/geo/trips")
def get_geo(request: Request, selected: dict = Depends(filters), limit: int = Query(2000, ge=1, le=5000)):
    return service.geojson(service.trips(selected), request.url.path.endswith("/trips"), limit)


@app.get("/api/data-quality/summary")
@app.get("/api/data-quality/errors")
@app.get("/api/data-quality/warnings")
def get_quality(request: Request, selected: dict = Depends(filters)):
    if selected["company"] or selected["payment_type"] or selected["pickup_area"] is not None:
        raise HTTPException(422, "Quality reports support date filters only")
    return service.quality(selected)[request.url.path.rsplit("/", 1)[-1]]


@app.get("/api/places/nearby")
def get_places(lat: float = Query(ge=-90, le=90), lon: float = Query(ge=-180, le=180)):
    return service.nearby_places(lat, lon)


@app.get("/api/map-style")
def map_style():
    carto = bool(os.getenv("CARTO_BASEMAP_KEY"))
    return {"version": 8, "sources": {"basemap": {"type": "raster", "tileSize": 256,
        "tiles": ["/api/tiles/{z}/{x}/{y}.png"] if carto else ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        "attribution": '<a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors' + (' &copy; <a href="https://carto.com/attributions">CARTO</a>' if carto else '')}},
        "layers": [{"id": "basemap", "type": "raster", "source": "basemap"}]}


@app.get("/api/tiles/{z}/{x}/{y}.png")
def tile(z: int, x: int, y: int):
    if not (0 <= z <= 19 and 0 <= x < 2 ** z and 0 <= y < 2 ** z):
        raise HTTPException(422, "Invalid tile coordinates")
    try:
        return Response(service.carto_tile(z, x, y), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
    except requests.RequestException:
        raise HTTPException(502, "Basemap temporarily unavailable") from None


FRONTEND = Path(__file__).resolve().parents[2] / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND), name="frontend")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(FRONTEND / "index.html")
```

## src/api/service.py

```python
"""Read published Gold only; keep HTTP concerns in main.py."""
import json
import os
from functools import lru_cache
from pathlib import Path
from threading import Lock

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
CACHE_LOCK = Lock()


def data_root():
    return Path(os.getenv("CHICAGO_DATA_ROOT", str(ROOT / "data"))).resolve()


def publications():
    if os.getenv("S3_ENDPOINT_URL"):
        from src.object_store import gold_publications
        with CACHE_LOCK:
            return gold_publications(data_root() / "object-cache")
    result = []
    for path in sorted((data_root() / "gold/chicago_taxi").glob("processing_date=*/_manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        validation = json.loads((path.parent / "_validation.json").read_text(encoding="utf-8"))
        if manifest["status"] == "SUCCESS" and validation.get("passed"):
            result.append((path, manifest))
    return result


@lru_cache(maxsize=1)
def read_snapshot(signature):
    frames = [pd.read_parquet(Path(path).parent / "trips") for path, run_id in signature]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def trips(filters):
    signature = tuple((str(path), manifest["run_id"]) for path, manifest in publications())
    if not signature:
        raise FileNotFoundError("No validated Gold publication. Run Bronze, Silver and Gold first.")
    df = read_snapshot(signature)
    mask = pd.Series(True, index=df.index)
    dates = pd.to_datetime(df["trip_start_timestamp"]).dt.date
    for key, operator in [("start_date", "ge"), ("end_date", "le")]:
        if filters.get(key):
            mask &= getattr(dates, operator)(filters[key])
    for key, column in [("company", "company"), ("payment_type", "payment_type"), ("pickup_area", "pickup_community_area")]:
        if filters.get(key) is not None:
            mask &= df[column] == filters[key]
    return df.loc[mask].copy()


def records(df):
    return json.loads(df.to_json(orient="records", date_format="iso"))


def kpis(df):
    values = {"total_trips": len(df), "total_revenue": df.trip_total.sum(), "avg_trip_total": df.trip_total.mean(),
              "avg_trip_miles": df.trip_miles.mean(), "avg_trip_duration_minutes": df.trip_duration_minutes.mean(),
              "total_tips": df.tips.sum(), "tipped_trip_rate": df.has_tip.mean() if len(df) else 0,
              "unique_taxis": df.taxi_id.nunique(), "unique_companies": df.loc[df.company != "UNKNOWN", "company"].nunique()}
    return records(pd.DataFrame([values]))[0]


def metrics(df, table):
    keys = {"daily": ["trip_date"], "hourly": ["trip_date", "trip_hour"], "zones": ["pickup_community_area"],
            "payments": ["payment_type"], "companies": ["company"]}[table]
    result = df.groupby(keys, dropna=False).agg(trip_count=("trip_id", "size"), total_revenue=("trip_total", "sum"),
        avg_trip_total=("trip_total", "mean"), avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
        avg_trip_miles=("trip_miles", "mean"), total_tips=("tips", "sum"), avg_tip=("tips", "mean"),
        tipped_trip_rate=("has_tip", "mean")).reset_index()
    if table == "daily":
        result = result.rename(columns={"avg_trip_total": "avg_revenue_per_trip"})
    if table == "zones":
        located = df[df.pickup_centroid_latitude.between(-90, 90) & df.pickup_centroid_longitude.between(-180, 180)]
        centers = located.groupby("pickup_community_area", dropna=False).agg(latitude=("pickup_centroid_latitude", "mean"), longitude=("pickup_centroid_longitude", "mean")).reset_index()
        result = result.merge(centers, how="left", on="pickup_community_area")
    return records(result)


def geo_frame(df):
    mask = pd.Series(True, index=df.index)
    for side in ("pickup", "dropoff"):
        mask &= df[f"{side}_centroid_latitude"].between(-90, 90) & df[f"{side}_centroid_longitude"].between(-180, 180)
    return df.loc[mask]


def geojson(df, lines=False, limit=2000):
    source = geo_frame(df).sort_values(["trip_start_timestamp", "trip_id"])
    # Evenly spaced deterministic sample, bounded before serialization.
    if len(source) > limit:
        source = source.iloc[[int(i * len(source) / limit) for i in range(limit)]]
    features = []
    for row in records(source):
        pickup = [row["pickup_centroid_longitude"], row["pickup_centroid_latitude"]]
        dropoff = [row["dropoff_centroid_longitude"], row["dropoff_centroid_latitude"]]
        features.append({"type": "Feature", "geometry": {"type": "LineString" if lines else "Point",
                         "coordinates": [pickup, dropoff] if lines else pickup}, "properties": row})
    return {"type": "FeatureCollection", "features": features, "total": len(geo_frame(df)), "returned": len(features)}


def quality(filters):
    runs, errors, warnings = [], [], []
    for gold_path, gold in publications():
        day = pd.Timestamp(gold["processing_date"]).date()
        if filters.get("start_date") and day < filters["start_date"] or filters.get("end_date") and day > filters["end_date"]:
            continue
        report_root = gold_path.parents[3] / "reports"
        silver_dir = report_root / "silver" / gold["source_run_id"]
        silver = json.loads((silver_dir / "manifest.json").read_text(encoding="utf-8"))
        bronze_dir = report_root / "bronze" / silver["source_run_id"]
        bronze = json.loads((bronze_dir / "manifest.json").read_text(encoding="utf-8"))
        runs.append({"processing_date": gold["processing_date"], "bronze_run_id": bronze["run_id"],
                     "silver_run_id": silver["run_id"], "gold_run_id": gold["run_id"], "status": gold["status"],
                     "bronze_rows": bronze["rows_downloaded"], "input_rows": silver["input_rows"],
                     "valid_rows": silver["valid_rows"], "rejected_rows": silver["rejected_rows"],
                     "warning_rows": silver["warning_rows"], "pagination_complete": bronze["pagination_complete"]})
        errors.extend(pd.read_csv(silver_dir / "errors_report.csv").to_dict("records"))
        warnings.extend(pd.read_csv(silver_dir / "warnings_report.csv").to_dict("records"))
    summary = {key: sum(row[key] for row in runs) for key in ["bronze_rows", "input_rows", "valid_rows", "rejected_rows", "warning_rows"]}
    summary.update(rejection_rate=summary["rejected_rows"] / summary["input_rows"] if summary["input_rows"] else 0,
                   warning_rate=summary["warning_rows"] / summary["input_rows"] if summary["input_rows"] else 0, runs=runs)
    def combine(rows):
        if not rows:
            return []
        result = pd.DataFrame(rows).groupby("rule", as_index=False)["count"].sum()
        result["rate"] = result["count"] / summary["input_rows"] if summary["input_rows"] else 0.0
        return records(result.sort_values("count", ascending=False))
    return {"summary": summary, "errors": combine(errors), "warnings": combine(warnings)}


def nearby_places(lat, lon):
    key = os.getenv("FOURSQUARE_API_KEY")
    if not key:
        return {"available": False, "reason": "not_configured", "places": []}
    cache_path = data_root() / "cache/foursquare.json"
    coordinate_key = f"{lat:.4f},{lon:.4f}"
    try:
        with CACHE_LOCK:
            cached = json.loads(cache_path.read_text()) if cache_path.exists() else {}
            if coordinate_key in cached:
                return {"available": True, "cached": True, "places": cached[coordinate_key]}
            response = requests.get("https://places-api.foursquare.com/places/search",
                headers={"Authorization": f"Bearer {key}", "X-Places-Api-Version": "2025-06-17"},
                params={"ll": coordinate_key, "radius": 500, "limit": 5}, timeout=5)
            response.raise_for_status()
            places = [{"name": p.get("name"), "category": (p.get("categories") or [{}])[0].get("name"),
                       "distance": p.get("distance"), "address": p.get("location", {}).get("formatted_address")}
                      for p in response.json().get("results", [])[:5]]
            cached[coordinate_key] = places
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            temp = cache_path.with_suffix(".tmp")
            temp.write_text(json.dumps(cached), encoding="utf-8")
            temp.replace(cache_path)
        return {"available": True, "cached": False, "places": places}
    except (requests.RequestException, OSError, ValueError, KeyError, TypeError):
        return {"available": False, "reason": "unavailable", "places": []}


@lru_cache(maxsize=256)
def carto_tile(z, x, y):
    key = os.getenv("CARTO_BASEMAP_KEY")
    if not key:
        raise FileNotFoundError("CARTO key is not configured")
    response = requests.get(f"https://basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png",
                            params={"key": key}, timeout=10)
    response.raise_for_status()
    return response.content
```

## frontend/index.html

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Chicago Taxi Intelligence</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.6.1/dist/maplibre-gl.css">
  <link rel="stylesheet" href="/static/css/style.css">
  <script defer src="https://cdn.plot.ly/plotly-3.0.1.min.js"></script>
  <script defer src="https://unpkg.com/maplibre-gl@5.6.1/dist/maplibre-gl.js"></script>
  <script defer src="https://unpkg.com/lucide@0.468.0/dist/umd/lucide.min.js"></script>
  <script defer src="/static/js/app.js"></script>
</head>
<body>
<a class="skip" href="#content">Skip to content</a>
<aside class="sidebar">
  <a class="brand" href="#overview"><span class="brand-mark"><i data-lucide="route"></i></span><span>CHICAGO TAXI<span class="brand-sub">INTELLIGENCE</span></span></a>
  <div class="workspace"><span class="workspace-mark">CT</span><div>Mobility workspace<small>Chicago, Illinois</small></div><i data-lucide="chevrons-up-down"></i></div>
  <div class="nav-label">ANALYTICS</div>
  <nav aria-label="Main navigation">
    <a href="#overview" data-page="overview"><i data-lucide="layout-dashboard"></i>Overview</a>
    <a href="#operations" data-page="operations"><i data-lucide="chart-no-axes-combined"></i>Operations</a>
    <a href="#geography" data-page="geography"><i data-lucide="map"></i>Geography</a>
    <a href="#quality" data-page="quality"><i data-lucide="shield-check"></i>Data Quality</a>
  </nav>
  <div class="sidebar-bottom"><div class="source-label"><span class="status-dot"></span>Local data platform</div><small>Chicago SODA · Parquet</small><a href="/docs" target="_blank" rel="noreferrer">API documentation<i data-lucide="arrow-up-right"></i></a></div>
</aside>
<main id="content">
  <div class="topbar"><div class="breadcrumb">Workspace <span>/</span> Analytics <span>/</span> <strong id="crumb">Overview</strong></div><div class="connection" id="connection">Connecting</div></div>
  <header class="page-header"><div><div class="eyebrow">MOBILITY INTELLIGENCE</div><h1 id="page-title">Chicago Taxi Intelligence</h1><p id="page-subtitle">Operational &amp; Revenue Analytics</p></div><button class="icon-button" id="refresh" aria-label="Refresh data" title="Refresh data"><i data-lucide="refresh-cw"></i></button></header>
  <div id="demo-banner" class="demo-banner" hidden><i data-lucide="flask-conical"></i>Synthetic demonstration data · June 1–7, 2023</div>
  <form id="filters" class="filters">
    <label>From<input type="date" id="start-date" name="start_date" required></label>
    <label>To<input type="date" id="end-date" name="end_date" required></label>
    <label>Company<select id="company"><option value="">All companies</option></select></label>
    <label>Payment<select id="payment"><option value="">All payments</option></select></label>
    <button class="primary-button" type="submit"><i data-lucide="list-filter"></i>Apply filters</button>
    <button type="button" class="icon-button" id="reset" aria-label="Reset filters" title="Reset filters"><i data-lucide="rotate-ccw"></i></button>
  </form>
  <div id="feedback" class="feedback" role="status" hidden></div>
  <div id="loading" class="loading-line" hidden><span></span></div>
  <section id="overview" class="page" aria-label="Overview">
    <div class="section-heading"><h2>Performance at a glance</h2><span class="period-label"></span></div>
    <div id="overview-kpis" class="kpi-grid"></div>
    <div class="chart-grid overview-grid"><section class="chart-section"><div class="section-heading"><h2>Daily trips</h2><span>Trip volume</span></div><div id="daily-trips" class="chart"></div></section><section class="chart-section"><div class="section-heading"><h2>Daily revenue</h2><span>USD</span></div><div id="daily-revenue" class="chart"></div></section></div>
    <section class="chart-section"><div class="section-heading"><h2>Trips by hour</h2><span>Published trip start time</span></div><div id="overview-hourly" class="chart short-chart"></div></section>
  </section>
  <section id="operations" class="page" aria-label="Operations" hidden>
    <div class="section-heading"><h2>Operating performance</h2><span class="period-label"></span></div>
    <div id="operations-kpis" class="kpi-grid compact-kpis"></div>
    <div class="chart-grid"><section class="chart-section"><h2>Activity by day &amp; hour</h2><div id="activity-heatmap" class="chart"></div></section><section class="chart-section"><h2>Revenue by company</h2><div id="company-revenue" class="chart"></div></section><section class="chart-section"><h2>Company ranking</h2><div id="company-ranking" class="chart short-chart"></div></section><section class="chart-section"><h2>Payment mix</h2><div id="payment-mix" class="chart short-chart"></div></section></div>
    <section class="trips-section"><div class="section-heading"><h2>Trip records</h2><label class="search"><i data-lucide="search"></i><input id="trip-search" type="search" placeholder="Search trip ID or company" aria-label="Search trips"></label></div><div class="table-scroll"><table><thead><tr><th>Trip ID</th><th><button class="sort" data-sort="trip_start_timestamp">Start <i data-lucide="arrow-down-up"></i></button></th><th><button class="sort" data-sort="company">Company <i data-lucide="arrow-down-up"></i></button></th><th>Payment</th><th>Distance</th><th><button class="sort" data-sort="trip_total">Total <i data-lucide="arrow-down-up"></i></button></th></tr></thead><tbody id="trip-rows"></tbody></table></div><div class="pagination"><span id="trip-count"></span><div><button class="icon-button" id="prev" aria-label="Previous page" title="Previous page"><i data-lucide="chevron-left"></i></button><button class="icon-button" id="next" aria-label="Next page" title="Next page"><i data-lucide="chevron-right"></i></button></div></div></section>
  </section>
  <section id="geography" class="page" aria-label="Geography" hidden>
    <div class="map-toolbar"><div class="segmented" role="group" aria-label="Map mode"><button data-mode="density" aria-pressed="true">Pickup Density</button><button data-mode="trips-area" aria-pressed="false">Trips by Area</button><button data-mode="revenue-area" aria-pressed="false">Revenue by Area</button><button data-mode="explorer" aria-pressed="false">Trip Explorer</button></div><span id="map-count"></span></div>
    <div class="map-layout"><div class="map-wrap"><div id="map" aria-label="Chicago taxi map"></div><div class="map-legend"><span class="legend-dot"></span><span id="map-legend-text">Pickup concentration</span></div><button id="fit-map" class="icon-button map-fit" title="Fit all locations" aria-label="Fit all locations"><i data-lucide="scan"></i></button><div id="map-error" class="map-error" hidden></div></div><aside class="trip-detail" id="trip-detail"><span class="eyebrow">TRIP EXPLORER</span><h2>Trip details</h2><div class="detail-empty"><i data-lucide="mouse-pointer-2"></i><p>No trip selected</p></div></aside></div>
    <p class="map-note">Straight line between published pickup/dropoff centroids. Not the actual driven route.</p>
  </section>
  <section id="quality" class="page" aria-label="Data Quality" hidden>
    <div class="section-heading"><h2>Pipeline quality</h2><span>All companies · Selected dates</span></div><div id="quality-kpis" class="kpi-grid"></div>
    <section class="funnel-section"><h2>From source to trusted data</h2><div id="quality-funnel" class="funnel"></div></section>
    <div class="chart-grid"><section class="chart-section"><h2>Top blocking rules</h2><div id="error-chart" class="chart"></div></section><section class="chart-section"><h2>Warning rules</h2><div id="warning-chart" class="chart"></div></section></div>
    <section class="trips-section"><div class="section-heading"><h2>Run lineage</h2><span>Published Gold runs</span></div><div class="table-scroll"><table><thead><tr><th>Processing date</th><th>Bronze run</th><th>Silver run</th><th>Gold run</th><th>Status</th><th>Source complete</th></tr></thead><tbody id="run-rows"></tbody></table></div></section>
  </section>
  <footer class="page-footer"><span>Chicago Taxi Intelligence</span><span id="updated">Local analytics workspace</span></footer>
</main>
</body>
</html>
```

## frontend/css/style.css

```css
:root{--bg:#f6f8fb;--surface:#fff;--text:#101828;--muted:#667085;--border:#e5e9f0;--primary:#4f46e5;--success:#16866d;--warning:#b77919;--error:#c65353;--sidebar:228px}
*{box-sizing:border-box}body{margin:0;font-family:Inter,system-ui,sans-serif;font-size:14px;line-height:1.5;color:var(--text);background:var(--bg);letter-spacing:0}button,input,select{font:inherit}button,a,input,select{-webkit-tap-highlight-color:transparent}button{cursor:pointer}button:disabled{cursor:default;opacity:.45}a{color:inherit;text-decoration:none}button{transition:background .18s,color .18s,border-color .18s}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible{outline:3px solid #aaa4ff;outline-offset:3px}[hidden]{display:none!important}svg.lucide{width:19px;height:19px;flex:none;stroke-width:1.7}h1,h2,p{margin:0}h1{font-size:28px;font-weight:600;line-height:1.25}h2{font-size:15px;font-weight:600}small{font-size:11px;color:var(--muted)}.skip{position:fixed;top:-60px;z-index:99;background:#fff;padding:12px}.skip:focus{top:0}
.sidebar{position:fixed;inset:0 auto 0 0;width:var(--sidebar);background:#fff;border-right:1px solid var(--border);padding:30px 18px 20px;display:flex;flex-direction:column;z-index:10}.brand{display:flex;gap:10px;align-items:center;font-size:12px;font-weight:700;line-height:1.5}.brand-mark{background:#f3c94f;width:36px;height:38px;display:grid;place-items:center;border-radius:6px;color:#292b30}.brand-mark svg{width:24px;height:24px}.brand-sub{display:block;font-size:10px;color:var(--muted);font-weight:500}.workspace{display:flex;align-items:center;gap:9px;border-block:1px solid var(--border);margin:30px 0 26px;padding:18px 0;font-size:11px;font-weight:500}.workspace small{display:block;font-size:10px}.workspace>svg{margin-left:auto;width:12px}.workspace-mark{display:grid;place-items:center;width:30px;height:30px;background:#f2f4f7;border:1px solid var(--border);border-radius:5px;font-size:10px}.nav-label{font-size:10px;color:#98a2b3;padding:0 13px 12px;font-weight:600}.sidebar nav{display:grid;gap:6px}.sidebar nav a{display:flex;align-items:center;gap:13px;padding:11px 14px;border-radius:6px;font-size:13px;color:var(--muted);font-weight:500}.sidebar nav a:hover{background:#f7f8fa}.sidebar nav a.active{background:#eeedff;color:var(--primary)}.sidebar-bottom{margin-top:auto;border-top:1px solid var(--border);padding:18px 8px 0;font-size:11px}.sidebar-bottom>a{display:flex;justify-content:space-between;align-items:center;margin-top:20px;color:var(--muted)}.sidebar-bottom>a svg{width:14px}.source-label{display:flex;align-items:center;gap:7px;margin-bottom:4px}.status-dot{width:6px;height:6px;border-radius:50%;background:var(--success)}main{margin-left:var(--sidebar);padding:0 34px;max-width:1900px}.topbar{height:66px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:10px;font-size:11px;color:var(--muted)}.breadcrumb{display:flex;gap:12px}.breadcrumb strong{font-weight:500;color:var(--text)}.breadcrumb span{color:#c4cbd5}.connection{font-size:10px;color:var(--success);background:#edf8f3;border:1px solid #d4e9df;border-radius:4px;padding:4px 8px}.page-header{display:flex;justify-content:space-between;align-items:center;padding:28px 0 24px;gap:16px}.eyebrow{font-size:10px;color:var(--muted);font-weight:600;margin-bottom:8px}.page-header p{color:var(--muted);font-size:12px;margin-top:7px}.icon-button{display:inline-flex;align-items:center;justify-content:center;width:36px;height:36px;flex:none;border:1px solid var(--border);background:#fff;border-radius:5px;color:var(--muted)}.icon-button:hover{background:#eef0f6;color:var(--text)}.demo-banner{display:flex;align-items:center;gap:8px;background:#fff8e6;border:1px solid #efdfb2;border-radius:5px;color:#87651d;font-size:11px;padding:8px 12px;margin-bottom:16px}.demo-banner svg{width:15px;height:15px}.filters{display:flex;align-items:flex-end;gap:12px;padding:0 0 24px;border-bottom:1px solid var(--border);flex-wrap:wrap}.filters label{display:grid;gap:6px;font-size:10px;color:var(--muted);font-weight:500;flex:1;min-width:130px;max-width:220px}.filters input,.filters select{height:37px;width:100%;border:1px solid #dce1e9;border-radius:5px;color:#344054;background:#fff;padding:7px 10px;font-size:12px;min-width:0}.primary-button{height:37px;display:flex;align-items:center;justify-content:center;gap:8px;padding:0 15px;border:1px solid var(--primary);background:var(--primary);color:#fff;border-radius:5px;font-size:11px;font-weight:500;white-space:nowrap}.primary-button:hover{background:#4038c7}.primary-button svg{width:15px;height:15px}.section-heading{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:16px}.section-heading>span{font-size:10px;color:var(--muted)}.page{padding-top:25px}.kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px;margin-bottom:28px}.kpi{background:#fff;border:1px solid var(--border);border-radius:6px;padding:17px 14px;min-height:112px;overflow:hidden}.kpi-label{font-size:10px;color:var(--muted);display:flex;justify-content:space-between;align-items:center;gap:4px;min-height:18px}.kpi-label svg{width:14px;height:14px;color:#929bad}.kpi-value{font-size:25px;font-weight:600;font-variant-numeric:tabular-nums;margin-top:12px;line-height:1.1;overflow-wrap:anywhere}.kpi-unit{font-size:10px;color:#98a2b3;margin-top:7px}.compact-kpis{grid-template-columns:repeat(3,minmax(0,1fr))}.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:28px}.chart-section{min-width:0;padding:20px 0 8px;border-top:1px solid var(--border)}.chart-section h2{margin-bottom:4px}.chart-section .section-heading{margin-bottom:0}.chart{height:275px;width:100%;min-width:0}.short-chart{height:225px}.feedback{padding:14px;border:1px solid #edcbca;background:#fff2f0;border-radius:5px;color:#964949;margin-top:18px;font-size:12px}.feedback.neutral{background:#f0f3f8;border-color:var(--border);color:var(--muted)}.loading-line{height:2px;overflow:hidden;background:#e5e2fc;margin-top:8px}.loading-line span{display:block;height:100%;width:30%;background:var(--primary);animation:progress 1s infinite ease-in-out}@keyframes progress{from{transform:translateX(-100%)}to{transform:translateX(440%)}}.trips-section{border-top:1px solid var(--border);padding-top:22px;margin-top:18px}.table-scroll{overflow:auto;width:100%}table{border-collapse:collapse;width:100%;font-size:11px;text-align:left;white-space:nowrap}th{color:var(--muted);font-weight:500;background:#f0f3f8;padding:12px 14px;border-block:1px solid var(--border)}td{padding:13px 14px;border-bottom:1px solid var(--border);font-variant-numeric:tabular-nums}tbody tr:hover{background:#f0f2f7}.trip-link{background:none;border:0;padding:0;color:var(--primary);font-size:11px;text-decoration:underline;text-underline-offset:3px}.sort{border:0;background:none;padding:0;color:inherit;display:flex;align-items:center;gap:8px;font-size:11px}.sort svg{width:12px}.search{display:flex;align-items:center;gap:7px;border:1px solid var(--border);background:#fff;border-radius:5px;padding:6px 10px}.search input{border:0;background:none;outline:none;width:190px;font-size:11px}.search svg{width:14px}.pagination{display:flex;align-items:center;justify-content:space-between;padding:15px 0;color:var(--muted);font-size:11px}.pagination>div{display:flex;gap:8px}.funnel-section{border-block:1px solid var(--border);padding:22px 0 28px;margin-bottom:20px}.funnel{display:grid;grid-template-columns:1fr 35px 1fr 35px 1fr 1fr;align-items:center;gap:10px;margin-top:22px}.funnel-node{border-left:3px solid var(--primary);padding:8px 14px;background:#efeffb}.funnel-node.valid{border-color:var(--success);background:#eaf5ef}.funnel-node.rejected{border-color:var(--error);background:#fbeeed}.funnel-node span{display:block;font-size:10px;color:var(--muted)}.funnel-node strong{font-size:22px;font-weight:600}.funnel>svg{color:#adb5c3;width:20px}.run-id{max-width:160px;overflow:hidden;text-overflow:ellipsis}.status-tag{color:var(--success);background:#eaf5ef;font-size:9px;padding:4px 7px;border-radius:4px}.page-footer{display:flex;justify-content:space-between;gap:15px;border-top:1px solid var(--border);padding:18px 0;margin-top:28px;color:#98a2b3;font-size:10px}.map-toolbar{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:18px;flex-wrap:wrap}.segmented{display:flex;padding:3px;border:1px solid var(--border);background:#edeff4;border-radius:6px;gap:3px;flex-wrap:wrap}.segmented button{border:0;background:none;padding:8px 12px;font-size:11px;color:var(--muted);border-radius:4px}.segmented button[aria-pressed=true]{background:#fff;color:var(--text);box-shadow:0 1px 3px #10182812}.map-toolbar>span{font-size:10px;color:var(--muted)}.map-layout{display:grid;grid-template-columns:minmax(0,1fr) 280px;min-height:560px;border-block:1px solid var(--border);background:#fff}.map-wrap{position:relative;min-width:0}#map{width:100%;height:560px}.trip-detail{padding:22px;border-left:1px solid var(--border);min-width:0;max-height:560px;overflow:auto}.trip-detail h2{font-size:18px}.detail-empty{display:grid;justify-items:center;gap:15px;padding:90px 0;color:#98a2b3;font-size:12px}.detail-empty svg{width:30px;height:30px}.trip-detail .trip-id{font-size:10px;color:var(--muted);overflow-wrap:anywhere;padding:8px 0 18px}.trip-detail dl{margin:0;display:grid;grid-template-columns:1fr 1fr;gap:17px 10px}.trip-detail dt{font-size:10px;color:var(--muted);margin-bottom:4px}.trip-detail dd{margin:0;font-size:12px;overflow-wrap:anywhere}.trip-detail .wide{grid-column:1/-1}.places{border-top:1px solid var(--border);margin-top:20px;padding-top:15px}.places h3{font-size:12px;font-weight:600;margin:0 0 10px}.places button{margin-bottom:10px}.places p{font-size:11px;color:var(--muted)}.place-item{padding:9px 0;border-bottom:1px solid var(--border);font-size:11px}.place-item small{display:block}.map-note{margin-top:12px;font-size:10px;color:var(--muted)}.map-fit{position:absolute;right:10px;bottom:50px;box-shadow:0 1px 4px #10182820}.map-legend{position:absolute;left:15px;bottom:25px;background:#ffffffed;border:1px solid var(--border);border-radius:4px;padding:8px 12px;display:flex;gap:8px;align-items:center;font-size:10px}.legend-dot{width:8px;height:8px;border-radius:50%;background:var(--primary)}.map-error{position:absolute;top:15px;left:15px;right:15px;background:#fff8e6;color:#87651d;padding:10px;font-size:11px}.maplibregl-ctrl-attrib{font-size:9px}.maplibregl-canvas:focus{outline:none}
@media(min-width:1600px){main{padding-inline:48px}.chart{height:310px}.kpi-value{font-size:29px}.map-layout{min-height:650px}#map{height:650px}.trip-detail{max-height:650px}}
@media(max-width:1150px){:root{--sidebar:200px}main{padding-inline:24px}.sidebar{padding-inline:13px}.kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.map-layout{grid-template-columns:minmax(0,1fr) 240px}.chart-grid{gap:18px}.filters label{min-width:120px}.brand{font-size:11px}}
@media(max-width:800px){:root{--sidebar:72px}.sidebar{padding:24px 10px}.brand>span:not(.brand-mark),.workspace,.nav-label,.sidebar-bottom{display:none}.brand{justify-content:center}.sidebar nav{margin-top:30px;gap:14px}.sidebar nav a{font-size:0;justify-content:center;padding:12px}.sidebar nav a svg{width:22px;height:22px}.chart-grid{grid-template-columns:1fr}.map-layout{grid-template-columns:1fr}.trip-detail{border-left:0;border-top:1px solid var(--border);max-height:none}#map{height:460px}.detail-empty{padding:20px}.trip-detail dl{grid-template-columns:repeat(3,1fr)}.funnel{grid-template-columns:1fr 25px 1fr}.funnel-node.valid,.funnel-node.rejected{grid-column:auto}.topbar{height:55px}.breadcrumb{gap:6px}.page-header{padding-top:24px}}
@media(max-width:520px){:root{--sidebar:0px}.sidebar{position:fixed;inset:auto 0 0 0;width:auto;height:64px;padding:0 10px;border-top:1px solid var(--border);border-right:0}.brand{display:none}.sidebar nav{display:flex;margin:0;gap:0;justify-content:space-around;height:100%}.sidebar nav a{flex-direction:column;gap:3px;font-size:9px;padding:9px 12px;border-radius:0;flex:1}.sidebar nav a svg{width:19px;height:19px}main{padding-inline:17px;padding-bottom:65px}.breadcrumb{font-size:10px}.breadcrumb>span:first-of-type{display:none}h1{font-size:24px}.kpi-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.kpi{padding:14px;min-height:105px}.kpi-value{font-size:26px}.filters{gap:10px}.filters label{min-width:calc(50% - 6px);max-width:none}.filters .primary-button{flex:1}.page-header{align-items:flex-start}.section-heading{align-items:flex-start}.section-heading>span{font-size:9px;text-align:right}.segmented{width:100%;display:grid;grid-template-columns:1fr 1fr}.search input{width:140px}.search{max-width:60%}.map-note{font-size:10px}.trip-detail dl{grid-template-columns:1fr 1fr}.funnel{gap:8px}.page-footer{font-size:9px}.chart{height:260px}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
@media(max-width:800px){.funnel{grid-template-columns:repeat(2,minmax(0,1fr))}.funnel>svg{display:none}.funnel-node{min-width:0}.chart{overflow:hidden}}
```

## frontend/js/app.js

```javascript
"use strict";
const API_BASE = "/api";
const $ = id => document.getElementById(id);
const state = { page: "overview", health: null, offset: 0, sort: "trip_start_timestamp", descending: true,
  map: null, mapReady: null, mode: "density", geo: null, zones: [], selectedTrip: null, request: 0 };
const palette = { primary: "#6257d6", green: "#21987d", gray: "#a3aabc", red: "#c96c6b", amber: "#c39241" };
const titles = { overview: ["Chicago Taxi Intelligence", "Operational & Revenue Analytics"], operations: ["Operations", "Trip activity, operators & payment patterns"],
  geography: ["Geography", "The geography of Chicago mobility"], quality: ["Data Quality", "From source records to trusted analytics"] };
const fmt = (value, digits = 0) => value == null ? "—" : Number(value).toLocaleString("en-US", { maximumFractionDigits: digits });
const money = value => value == null ? "—" : Number(value).toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
const moneyExact = value => value == null ? "—" : Number(value).toLocaleString("en-US", { style: "currency", currency: "USD" });
const percent = value => value == null ? "—" : `${fmt(value * 100, 1)}%`;
const escapeHTML = value => String(value ?? "—").replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
const icons = () => window.lucide?.createIcons();

function selectedParams(quality = false) {
  const params = new URLSearchParams();
  if ($("start-date").value) params.set("start_date", $("start-date").value);
  if ($("end-date").value) params.set("end_date", $("end-date").value);
  if (!quality && $("company").value) params.set("company", $("company").value);
  if (!quality && $("payment").value) params.set("payment_type", $("payment").value);
  return params;
}
async function api(path, params = selectedParams()) {
  const response = await fetch(`${API_BASE}${path}?${params}`, { cache: "no-store" });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(typeof body.detail === "string" ? body.detail : `Request failed (${response.status}).`);
  }
  return response.json();
}
const fetchKPIs = () => api("/kpis");
const fetchDaily = () => api("/daily");
const fetchHourly = () => api("/hourly");
const fetchZones = () => api("/zones");
const fetchDQ = () => Promise.all(["summary", "errors", "warnings"].map(kind => api(`/data-quality/${kind}`, selectedParams(true))));
async function fetchTrips() {
  const params = selectedParams();
  params.set("limit", "12"); params.set("offset", state.offset); params.set("sort", state.sort);
  params.set("descending", state.descending); params.set("search", $("trip-search").value);
  return api("/trips", params);
}
function feedback(message, neutral = false) {
  $("feedback").textContent = message;
  $("feedback").classList.toggle("neutral", neutral);
  $("feedback").hidden = !message;
}
function kpis(target, items) {
  $(target).innerHTML = items.map(([label, value, unit, icon]) => `<article class="kpi"><div class="kpi-label"><span>${label}</span><i data-lucide="${icon}"></i></div><div class="kpi-value">${value}</div><div class="kpi-unit">${unit}</div></article>`).join("");
  icons();
}
function chart(id, traces, options = {}) {
  if (!window.Plotly) { $(id).textContent = "Charts unavailable. Check your connection and refresh."; return; }
  const empty = !traces.length || traces.every(trace => !(trace.x?.length || trace.z?.length));
  const layout = { margin: { l: 44, r: 12, t: 26, b: 36 }, paper_bgcolor: "transparent", plot_bgcolor: "transparent",
    font: { family: "Inter, sans-serif", size: 10, color: "#667085" }, hoverlabel: { bgcolor: "#fff", font: { size: 12 }, bordercolor: "#e5e9f0" },
    showlegend: false, hovermode: "closest", xaxis: { showgrid: false, zeroline: false, fixedrange: true, automargin: true },
    yaxis: { gridcolor: "#e9edf3", zeroline: false, fixedrange: true, automargin: true },
    annotations: empty ? [{ text: "No data for this selection", showarrow: false, xref: "paper", yref: "paper", x: .5, y: .5 }] : [], ...options,
    width: $(id).clientWidth, autosize: true };
  layout.margin.l = Math.min(layout.margin.l, Math.round($(id).clientWidth * .46));
  return Plotly.react(id, traces, layout, { responsive: true, displayModeBar: false, displaylogo: false });
}
function hours(rows) {
  const totals = Array(24).fill(0);
  rows.forEach(row => { totals[row.trip_hour] += row.trip_count; });
  return totals;
}
function bar(x, y, color = palette.primary, horizontal = false) {
  return { type: "bar", x, y, orientation: horizontal ? "h" : "v", marker: { color, line: { width: 0 } },
    hovertemplate: horizontal ? "%{y}<br>%{x:,.0f}<extra></extra>" : "%{x}<br>%{y:,.0f}<extra></extra>" };
}
function drawOverview(kpi, daily, hourly) {
  kpis("overview-kpis", [["Total trips", fmt(kpi.total_trips), "Validated trips", "car-front"], ["Revenue", money(kpi.total_revenue), "Total trip revenue", "wallet"],
    ["Avg trip value", moneyExact(kpi.avg_trip_total), "Per validated trip", "badge-dollar-sign"], ["Avg distance", fmt(kpi.avg_trip_miles, 1), "Miles per trip", "route"],
    ["Avg duration", fmt(kpi.avg_trip_duration_minutes, 1), "Minutes per trip", "clock-3"], ["Tip rate", percent(kpi.tipped_trip_rate), "Trips with a tip", "heart-handshake"]]);
  chart("daily-trips", [{ type: "scatter", mode: "lines+markers", x: daily.map(r => r.trip_date), y: daily.map(r => r.trip_count),
    line: { color: palette.primary, width: 2.5, shape: "linear" }, marker: { size: 6, color: palette.primary, line: { color: "white", width: 2 } },
    fill: "tozeroy", fillcolor: "rgba(98,87,214,.065)", hovertemplate: "%{x|%b %d}<br>%{y:,.0f} trips<extra></extra>" }], { xaxis: { type: "date", tickformat: "%b %d", showgrid: false, fixedrange: true } });
  chart("daily-revenue", [bar(daily.map(r => r.trip_date), daily.map(r => r.total_revenue), palette.green)], { bargap: .55, yaxis: { tickprefix: "$", gridcolor: "#e9edf3", fixedrange: true }, xaxis: { type: "date", tickformat: "%b %d", showgrid: false, fixedrange: true } });
  chart("overview-hourly", [bar(Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`), hours(hourly))], { bargap: .4 });
}
function drawOperations(kpi, hourly, companies, payments) {
  kpis("operations-kpis", [["Average duration", `${fmt(kpi.avg_trip_duration_minutes, 1)} min`, "Per validated trip", "clock-3"],
    ["Average distance", `${fmt(kpi.avg_trip_miles, 1)} mi`, "Per validated trip", "route"], ["Active taxis", fmt(kpi.unique_taxis), "Distinct taxi identifiers", "car-front"]]);
  const z = Array.from({ length: 7 }, () => Array(24).fill(0));
  hourly.forEach(row => { const day = (new Date(`${String(row.trip_date).slice(0, 10)}T12:00:00Z`).getUTCDay() + 6) % 7; z[day][row.trip_hour] += row.trip_count; });
  chart("activity-heatmap", [{ type: "heatmap", x: Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`),
    y: ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], z, xgap: 3, ygap: 3,
    colorscale: [[0, "#f1f0fb"], [.3, "#d2cef0"], [.65, "#9489df"], [1, "#5546c1"]], showscale: false,
    hovertemplate: "%{y}, %{x}<br>%{z} trips<extra></extra>" }], { yaxis: { autorange: "reversed", showgrid: false, fixedrange: true } });
  const companyRevenue = [...companies].sort((a, b) => a.total_revenue - b.total_revenue).slice(-8);
  chart("company-revenue", [bar(companyRevenue.map(r => r.total_revenue), companyRevenue.map(r => r.company), palette.green, true)], { margin: { l: 115, r: 20, t: 25, b: 35 } });
  const ranked = [...companies].sort((a, b) => a.trip_count - b.trip_count).slice(-8);
  chart("company-ranking", [bar(ranked.map(r => r.trip_count), ranked.map(r => r.company), palette.primary, true)], { margin: { l: 115, r: 20, t: 25, b: 35 } });
  chart("payment-mix", [bar(payments.map(r => r.payment_type), payments.map(r => r.trip_count), "#8793a8")]);
}
function drawTrips(result) {
  $("trip-rows").innerHTML = result.items.map(row => `<tr><td><button class="trip-link" data-trip="${escapeHTML(row.trip_id)}">${escapeHTML(row.trip_id.slice(0, 16))}</button></td><td>${escapeHTML(row.trip_start_timestamp?.replace("T", " ").slice(0, 16))}</td><td>${escapeHTML(row.company)}</td><td>${escapeHTML(row.payment_type)}</td><td>${fmt(row.trip_miles, 1)} mi</td><td>${moneyExact(row.trip_total)}</td></tr>`).join("") || '<tr><td colspan="6">No matching trips</td></tr>';
  $("trip-count").textContent = result.total ? `${fmt(result.offset + 1)}–${fmt(Math.min(result.total, result.offset + result.limit))} of ${fmt(result.total)} trips` : "0 trips";
  $("prev").disabled = state.offset === 0; $("next").disabled = state.offset + result.limit >= result.total;
  $("trip-rows").querySelectorAll("[data-trip]").forEach(button => button.addEventListener("click", () => selectTrip(button.dataset.trip)));
}
function drawQuality(summary, errors, warnings) {
  kpis("quality-kpis", [["Bronze rows", fmt(summary.bronze_rows), "Source records", "database"], ["Silver valid", fmt(summary.valid_rows), "Trusted records", "shield-check"],
    ["Rejected", fmt(summary.rejected_rows), "Quarantined records", "shield-alert"], ["Warning rows", fmt(summary.warning_rows), "Includes rejected rows", "triangle-alert"],
    ["Rejection rate", percent(summary.rejection_rate), "Of Silver input", "circle-x"], ["Warning rate", percent(summary.warning_rate), "Of Silver input", "flag"]]);
  $("quality-funnel").innerHTML = `<div class="funnel-node"><span>BRONZE</span><strong>${fmt(summary.bronze_rows)}</strong></div><i data-lucide="arrow-right"></i><div class="funnel-node"><span>SILVER INPUT</span><strong>${fmt(summary.input_rows)}</strong></div><i data-lucide="git-fork"></i><div class="funnel-node valid"><span>VALID</span><strong>${fmt(summary.valid_rows)}</strong></div><div class="funnel-node rejected"><span>QUARANTINE</span><strong>${fmt(summary.rejected_rows)}</strong></div>`;
  const rules = (id, rows, color) => { const selected = rows.slice(0, 7).reverse(); chart(id, [bar(selected.map(r => r.count), selected.map(r => r.rule.replaceAll("_", " ")), color, true)], { margin: { l: 190, r: 25, t: 25, b: 35 } }); };
  rules("error-chart", errors, palette.red); rules("warning-chart", warnings, palette.amber);
  $("run-rows").innerHTML = summary.runs.map(row => `<tr><td>${row.processing_date}</td>${["bronze_run_id", "silver_run_id", "gold_run_id"].map(key => `<td class="run-id" title="${escapeHTML(row[key])}">${escapeHTML(row[key])}</td>`).join("")}<td><span class="status-tag">${row.status}</span></td><td>${row.pagination_complete ? "Complete" : "Partial"}</td></tr>`).join("") || '<tr><td colspan="6">No published runs in this date range</td></tr>';
  icons();
}

async function ensureMap() {
  if (state.mapReady) return state.mapReady;
  state.mapReady = (async () => {
    if (!window.maplibregl) throw new Error("Map library unavailable. Check your connection and refresh.");
    const style = await api("/map-style", new URLSearchParams());
    const map = new maplibregl.Map({ container: "map", style, center: [-87.649, 41.901], zoom: 11, attributionControl: true });
    state.map = map;
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.on("error", () => { $("map-error").textContent = "Some map tiles could not be loaded. Data overlays remain available."; $("map-error").hidden = false; });
    await new Promise((resolve, reject) => { const timer = setTimeout(() => reject(new Error("Map loading timed out.")), 20000); map.once("load", () => { clearTimeout(timer); resolve(); }); });
    const empty = { type: "FeatureCollection", features: [] };
    ["pickups", "routes", "areas"].forEach(name => map.addSource(name, { type: "geojson", data: empty }));
    map.addLayer({ id: "density", type: "heatmap", source: "pickups", paint: { "heatmap-radius": 25, "heatmap-opacity": .7,
      "heatmap-color": ["interpolate", ["linear"], ["heatmap-density"], 0, "rgba(98,87,214,0)", .2, "#ced4eb", .5, "#9b93d8", .8, "#7566c0", 1, "#43388b"] } });
    map.addLayer({ id: "routes", type: "line", source: "routes", paint: { "line-color": "#6651c6", "line-width": 1.5, "line-opacity": .22 } });
    map.addLayer({ id: "pickup-points", type: "circle", source: "pickups", paint: { "circle-radius": 4, "circle-color": palette.primary, "circle-stroke-color": "#fff", "circle-stroke-width": 1 } });
    map.addLayer({ id: "area-circles", type: "circle", source: "areas", paint: { "circle-radius": 12, "circle-color": palette.primary, "circle-opacity": .65, "circle-stroke-width": 2, "circle-stroke-color": "#fff" } });
    map.on("click", "pickup-points", event => showTrip(event.features[0].properties));
    map.on("click", "routes", event => showTrip(event.features[0].properties));
    map.on("click", "area-circles", event => { const feature = event.features[0]; const div = document.createElement("div"); div.textContent = `Pickup area ${feature.properties.area}: ${state.mode === "revenue-area" ? money(feature.properties.value) : fmt(feature.properties.value) + " trips"}`; new maplibregl.Popup().setLngLat(feature.geometry.coordinates).setDOMContent(div).addTo(map); });
    ["pickup-points", "routes", "area-circles"].forEach(layer => { map.on("mouseenter", layer, () => { map.getCanvas().style.cursor = "pointer"; }); map.on("mouseleave", layer, () => { map.getCanvas().style.cursor = ""; }); });
    return map;
  })();
  return state.mapReady;
}
async function drawMap(geo, zones) {
  state.geo = geo; state.zones = zones;
  await ensureMap();
  state.map.getSource("routes").setData(geo);
  state.map.getSource("pickups").setData({ type: "FeatureCollection", features: geo.features.map(feature => ({ ...feature, geometry: { type: "Point", coordinates: feature.geometry.coordinates[0] } })) });
  $("map-count").textContent = `${fmt(geo.returned)} of ${fmt(geo.total)} geolocated trips`;
  updateMapMode(); fitMap();
}
function updateMapMode() {
  document.querySelectorAll("[data-mode]").forEach(button => button.setAttribute("aria-pressed", button.dataset.mode === state.mode));
  if (!state.map?.getLayer("density")) return;
  const areaMode = state.mode.endsWith("area");
  const visibility = { density: state.mode === "density", routes: state.mode === "explorer", "pickup-points": !areaMode, "area-circles": areaMode };
  for (const [layer, visible] of Object.entries(visibility)) state.map.setLayoutProperty(layer, "visibility", visible ? "visible" : "none");
  state.map.setPaintProperty("pickup-points", "circle-opacity", state.mode === "density" ? .15 : .85);
  state.map.setPaintProperty("pickup-points", "circle-stroke-opacity", state.mode === "density" ? .1 : 1);
  const points = state.zones.filter(row => row.latitude != null && row.longitude != null).map(row => ({ type: "Feature", geometry: { type: "Point", coordinates: [row.longitude, row.latitude] },
    properties: { area: row.pickup_community_area ?? "Unknown", value: state.mode === "revenue-area" ? row.total_revenue : row.trip_count } }));
  state.map.getSource("areas").setData({ type: "FeatureCollection", features: points });
  const max = Math.max(1, ...points.map(point => point.properties.value));
  state.map.setPaintProperty("area-circles", "circle-radius", ["interpolate", ["linear"], ["get", "value"], 0, 8, max, 36]);
  state.map.setPaintProperty("area-circles", "circle-color", state.mode === "revenue-area" ? palette.green : palette.primary);
  $("map-legend-text").textContent = { density: "Pickup concentration", "trips-area": "Area trip totals · proportional circles", "revenue-area": "Area revenue · proportional circles", explorer: "Published centroid connections" }[state.mode];
}
function fitMap() {
  if (!state.map || !state.geo?.features.length) return;
  const bounds = new maplibregl.LngLatBounds();
  state.geo.features.forEach(feature => feature.geometry.coordinates.forEach(point => bounds.extend(point)));
  state.map.fitBounds(bounds, { padding: 50, maxZoom: 13, duration: 350 });
}
function showTrip(row) {
  state.selectedTrip = row;
  const fields = [["Company", row.company], ["Payment", row.payment_type], ["Start", row.trip_start_timestamp?.replace("T", " ").slice(0, 19)],
    ["End", row.trip_end_timestamp?.replace("T", " ").slice(0, 19)], ["Duration", `${fmt(row.trip_duration_minutes, 1)} min`], ["Distance", `${fmt(row.trip_miles, 1)} mi`],
    ["Fare", moneyExact(row.fare)], ["Tip", moneyExact(row.tips)], ["Total", moneyExact(row.trip_total)], ["Pickup area", row.pickup_community_area], ["Dropoff area", row.dropoff_community_area]];
  $("trip-detail").innerHTML = `<span class="eyebrow">SELECTED TRIP</span><h2>Trip details</h2><div class="trip-id">${escapeHTML(row.trip_id)}</div><dl>${fields.map(([label, value]) => `<div class="${["Start", "End"].includes(label) ? "wide" : ""}"><dt>${label}</dt><dd>${escapeHTML(value)}</dd></div>`).join("")}</dl><div class="places"><h3>Nearby places</h3><button class="primary-button" id="nearby"><i data-lucide="map-pin"></i>At pickup</button><div id="place-results"></div></div>`;
  const lat = row.pickup_centroid_latitude, lon = row.pickup_centroid_longitude;
  $("nearby").disabled = lat == null || lon == null;
  $("nearby").addEventListener("click", async () => {
    $("nearby").disabled = true; $("place-results").textContent = "Loading nearby places…";
    try { const result = await api("/places/nearby", new URLSearchParams({ lat, lon }));
      $("place-results").innerHTML = result.available ? result.places.map(place => `<div class="place-item">${escapeHTML(place.name)}<small>${escapeHTML(place.category)} · ${fmt(place.distance)} m</small><small>${escapeHTML(place.address)}</small></div>`).join("") || "No nearby places found." : "Nearby places are currently unavailable.";
    } catch { $("place-results").textContent = "Nearby places are currently unavailable."; }
    finally { if ($("nearby")) $("nearby").disabled = false; }
  }); icons();
}
async function selectTrip(id) {
  try { const result = await api(`/trips/${encodeURIComponent(id)}`); showTrip(result.trip); state.mode = "explorer"; location.hash = "geography"; }
  catch (error) { feedback(error.message); }
}
async function reload() {
  const ticket = ++state.request;
  feedback(""); $("loading").hidden = false; $("content").setAttribute("aria-busy", "true");
  document.querySelectorAll(".period-label").forEach(el => { el.textContent = `${$("start-date").value} — ${$("end-date").value}`; });
  try {
    if (state.page === "overview") {
      const results = await Promise.all([fetchKPIs(), fetchDaily(), fetchHourly()]);
      if (ticket !== state.request) return;
      drawOverview(...results); if (!results[0].total_trips) feedback("No trips match the selected filters.", true);
    } else if (state.page === "operations") {
      const results = await Promise.all([fetchKPIs(), fetchHourly(), api("/companies"), api("/payments"), fetchTrips()]);
      if (ticket !== state.request) return;
      drawOperations(...results); drawTrips(results[4]);
    } else if (state.page === "quality") {
      const results = await fetchDQ(); if (ticket !== state.request) return; drawQuality(...results);
    } else {
      const results = await Promise.all([api("/geo/trips"), fetchZones()]); if (ticket !== state.request) return; await drawMap(...results);
    }
    $("updated").textContent = `Updated ${new Date().toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" })}`;
  } catch (error) { if (ticket === state.request) feedback(error.message); }
  finally { if (ticket === state.request) { $("loading").hidden = true; $("content").setAttribute("aria-busy", "false"); } }
}
function navigate() {
  state.page = Object.hasOwn(titles, location.hash.slice(1)) ? location.hash.slice(1) : "overview";
  document.querySelectorAll(".page").forEach(section => { section.hidden = section.id !== state.page; });
  document.querySelectorAll("[data-page]").forEach(link => { link.classList.toggle("active", link.dataset.page === state.page); link.setAttribute("aria-current", link.dataset.page === state.page ? "page" : "false"); });
  $("page-title").textContent = titles[state.page][0]; $("page-subtitle").textContent = titles[state.page][1];
  $("crumb").textContent = state.page === "quality" ? "Data Quality" : state.page[0].toUpperCase() + state.page.slice(1);
  $("company").disabled = $("payment").disabled = state.page === "quality";
  state.offset = 0;
  if (state.health?.ready) reload();
  if (state.map && state.page === "geography") setTimeout(() => state.map.resize(), 0);
}
async function boot() {
  icons();
  try {
    state.health = await api("/health", new URLSearchParams());
    $("connection").textContent = state.health.ready ? "Gold connected" : "No published data";
    $("demo-banner").hidden = state.health.mode !== "demo";
    $("start-date").value = state.health.dates[0] || ""; $("end-date").value = state.health.dates.at(-1) || "";
    for (const [id, key, label] of [["company", "companies", "All companies"], ["payment", "payments", "All payments"]]) {
      $(id).replaceChildren(new Option(label, ""), ...state.health[key].map(value => new Option(value, value)));
    }
    navigate();
    if (!state.health.ready) feedback("No validated Gold data is available for this workspace.", true);
  } catch (error) { feedback(error.message); $("connection").textContent = "Disconnected"; }
}
$("filters").addEventListener("submit", event => { event.preventDefault(); if ($("start-date").value > $("end-date").value) return feedback("The start date must be before the end date."); state.offset = 0; reload(); });
$("reset").addEventListener("click", () => { $("company").value = $("payment").value = ""; $("start-date").value = state.health?.dates[0] || ""; $("end-date").value = state.health?.dates.at(-1) || ""; state.offset = 0; reload(); });
$("refresh").addEventListener("click", () => state.health?.ready ? reload() : boot());
$("prev").addEventListener("click", () => { state.offset = Math.max(0, state.offset - 12); reload(); });
$("next").addEventListener("click", () => { state.offset += 12; reload(); });
let searchTimer;
$("trip-search").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.offset = 0; reload(); }, 250); });
document.querySelectorAll("[data-sort]").forEach(button => button.addEventListener("click", () => { state.descending = state.sort === button.dataset.sort ? !state.descending : true; state.sort = button.dataset.sort; state.offset = 0; reload(); }));
document.querySelectorAll("[data-mode]").forEach(button => button.addEventListener("click", () => { state.mode = button.dataset.mode; updateMapMode(); }));
$("fit-map").addEventListener("click", fitMap);
window.addEventListener("hashchange", navigate);
window.addEventListener("resize", () => {
  if (window.Plotly) document.querySelectorAll(".page:not([hidden]) .js-plotly-plot").forEach(el => Plotly.relayout(el, { width: el.clientWidth }));
});
boot();
```

## config/chicago_taxi.yml

```yaml
dataset: chicago_taxi

dataops:
  warning_threshold: 0.02
  failure_threshold: 0.05
  allow_empty: false
  require_complete_bronze: true

bronze:
  source:
    domain: data.cityofchicago.org
    dataset_id: wrvz-psew
    app_token_env: CHICAGO_APP_TOKEN
  ingestion:
    timestamp_column: trip_start_timestamp
    order_by: trip_start_timestamp, trip_id
    page_size: 50000
    max_pages: 2
    timeout_seconds: 120
    max_retries: 3
    retry_backoff_seconds: 2
    expected_columns:
      - trip_id
      - taxi_id
      - trip_start_timestamp
      - trip_end_timestamp
      - trip_seconds
      - trip_miles
      - pickup_census_tract
      - dropoff_census_tract
      - pickup_community_area
      - dropoff_community_area
      - fare
      - tips
      - tolls
      - extras
      - trip_total
      - payment_type
      - company
      - pickup_centroid_latitude
      - pickup_centroid_longitude
      - pickup_centroid_location
      - dropoff_centroid_latitude
      - dropoff_centroid_longitude
      - dropoff_centroid_location
  output:
    path: data/bronze/chicago_taxi
    reports_path: data/reports
    logs_path: data/logs
    format: parquet

silver:
  input_path: data/bronze/chicago_taxi
  output_path: data/silver/chicago_taxi
  quarantine_path: data/quarantine/chicago_taxi
  reports_path: data/reports
  logs_path: data/logs
  format: parquet

gold:
  input_path: data/silver/chicago_taxi
  output_path: data/gold/chicago_taxi
  reports_path: data/reports
  logs_path: data/logs
```

## dags/chicago_taxi_batch_pipeline.py

```python
"""One manual batch DAG; business logic lives in the standalone modules."""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="chicago_taxi_batch_pipeline",
    start_date=datetime(2023, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"processing_date": Param("2023-06-01", type="string", format="date",
                                     pattern=r"^\d{4}-\d{2}-\d{2}$")},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1),
                  "retry_exponential_backoff": True, "max_retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(hours=2)},
    tags=["chicago-taxi", "batch"],
) as dag:
    start = EmptyOperator(task_id="start")
    previous = start
    commands = {
        "bronze": "python -m scripts.run_stage --layer bronze",
        "validate_bronze": "python -m src.dataops --layer bronze",
        "silver": "python -m scripts.run_stage --layer silver",
        "quality_gate": "python -m src.dataops --layer silver",
        "gold": "python -m scripts.run_stage --layer gold",
        "validate_gold": "python -m src.dataops --layer gold",
    }
    for task_id, command in commands.items():
        task = BashOperator(
            task_id=task_id,
            bash_command=command + ' --processing-date "$PROCESSING_DATE" --config "$PIPELINE_CONFIG"',
            cwd=os.environ.get("CHICAGO_PROJECT_ROOT", "/opt/chicago"),
            env={"PROCESSING_DATE": "{{ params.processing_date }}",
                 "PIPELINE_CONFIG": os.environ.get("CHICAGO_CONFIG", "config/chicago_taxi.yml")},
            append_env=True,
            do_xcom_push=False,
        )
        previous >> task
        previous = task
    end = EmptyOperator(task_id="end")
    previous >> end
```

## scripts/run_pipeline.py

```python
"""Run the standalone jobs in order, stopping on the first failed job or gate."""
import argparse
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    args = parser.parse_args()
    options = ["--processing-date", args.processing_date, "--config", args.config]
    for layer in ("bronze", "silver", "gold"):
        result = subprocess.run([sys.executable, "-m", "scripts.run_stage", "--layer", layer, *options])
        if result.returncode:
            raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
```

## scripts/run_stage.py

```python
"""Run one layer, checking and publishing its output to S3 when configured."""
import argparse
import importlib
import os
from pathlib import Path

from src.common import create_spark, load_config
from src.dataops.__main__ import run_check


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"])
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    args = parser.parse_args()
    config = load_config(args.config)
    settings = config["bronze"]["output"] if args.layer == "bronze" else config[args.layer]
    root = Path(settings["reports_path"]).resolve().parent
    use_s3 = bool(os.getenv("S3_ENDPOINT_URL"))
    if use_s3:
        from src import object_store
        if args.layer != "bronze":
            object_store.restore("bronze" if args.layer == "silver" else "silver", args.processing_date, root)
    module = importlib.import_module(f"src.{args.layer}.__main__")
    spark = create_spark(f"chicago-{args.layer}")
    try:
        manifest = getattr(module, f"run_{args.layer}")(spark, config, args.processing_date)
    finally:
        spark.stop()
    check = run_check(config, args.layer, args.processing_date)
    if check["check_status"] == "FAIL":
        raise SystemExit("Quality Gate FAIL: publication stopped")
    if use_s3:
        publication = object_store.publish(manifest, args.layer, root)
        print(f"S3 published: {publication['prefix']}")


if __name__ == "__main__":
    main()
```

## scripts/start_airflow.py

```python
"""Local-only Airflow launcher with a predictable development login."""
import json
import os
from pathlib import Path

path = Path(os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"])
if not path.exists():
    path.write_text(json.dumps({"artefact": os.environ["CHICAGO_AIRFLOW_PASSWORD"]}), encoding="utf-8")
    path.chmod(0o600)
os.execvp("airflow", ["airflow", "standalone"])
```

## scripts/serve.ps1

```powershell
param([int]$Port = 8000, [switch]$Live)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
if (Test-Path -LiteralPath '.env.local') {
    foreach ($line in Get-Content -LiteralPath '.env.local') {
        if ($line.Trim() -and -not $line.Trim().StartsWith('#')) {
            $pair = $line.Split('=', 2)
            if ($pair.Length -eq 2) {
                [Environment]::SetEnvironmentVariable($pair[0].Trim(), $pair[1].Trim(), 'Process')
            }
        }
    }
}
if ($Live) { $env:CHICAGO_DATA_ROOT = 'data' }
python -m uvicorn src.api.main:app --host 127.0.0.1 --port $Port
```

## scripts/build_demo.py

```python
"""Build explicitly synthetic fixtures through the real Bronze/Silver/Gold jobs."""
import math
import random
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from src.common import create_spark, load_config, write_json
from src.bronze.__main__ import run_bronze
from src.silver.__main__ import run_silver
from src.gold.__main__ import run_gold


def main():
    root = Path("data/demo").resolve()
    config = deepcopy(load_config("config/chicago_taxi.yml"))
    for layer in ("bronze", "silver", "gold"):
        settings = config[layer]["output"] if layer == "bronze" else config[layer]
        settings["path" if layer == "bronze" else "output_path"] = str(root / layer / "chicago_taxi")
        settings["reports_path"], settings["logs_path"] = str(root / "reports"), str(root / "logs")
        if layer != "bronze":
            settings["input_path"] = str(root / ("bronze" if layer == "silver" else "silver") / "chicago_taxi")
    write_json({"synthetic": True, "purpose": "UI and integration demonstration, not Chicago source observations"}, root / "_demo.json")
    spark = create_spark("chicago-taxi-demo")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    rng = random.Random(42)
    areas = [(8, 41.900, -87.634), (32, 41.881, -87.629), (28, 41.876, -87.666), (6, 41.943, -87.654), (7, 41.921, -87.650)]
    try:
        for day in range(7):
            date = datetime(2023, 6, 1) + timedelta(days=day)
            rows = []
            for i in range(240 + day * 19 + (90 if day in (1, 2) else 0)):
                hour = rng.choices(range(24), weights=[2 + 8 * math.exp(-((h - 17) / 4) ** 2) + 4 * math.exp(-((h - 8) / 2) ** 2) for h in range(24)])[0]
                start = date + timedelta(hours=hour, minutes=rng.randrange(60))
                seconds = rng.randrange(180, 2400)
                pickup, dropoff = rng.choice(areas), rng.choice(areas)
                fare = round(5 + seconds / 90 + rng.random() * 8, 2)
                tips = round(fare * .18, 2) if rng.random() < .64 else 0
                rows.append({"trip_id": f"DEMO-{day}-{i:05}", "taxi_id": f"DEMO-TAXI-{i % 83}",
                    "trip_start_timestamp": start.isoformat(), "trip_end_timestamp": (start + timedelta(seconds=seconds)).isoformat(),
                    "trip_seconds": "0" if i % 59 == 0 else str(seconds), "trip_miles": "0" if i % 31 == 0 else str(round(seconds / 240 + rng.random(), 2)),
                    "fare": str(fare), "tips": str(tips), "tolls": "0", "extras": "1", "trip_total": str(round(fare + tips + 1, 2)),
                    "payment_type": rng.choice(["Credit Card", "Credit Card", "Cash", "Mobile"]),
                    "company": rng.choice(["Flash Cab", "City Service", "Sun Taxi", "Chicago Carriage"]),
                    "pickup_community_area": str(pickup[0]), "dropoff_community_area": str(dropoff[0]),
                    "pickup_centroid_latitude": str(pickup[1] + rng.uniform(-.004, .004)), "pickup_centroid_longitude": str(pickup[2] + rng.uniform(-.004, .004)),
                    "dropoff_centroid_latitude": str(dropoff[1] + rng.uniform(-.004, .004)), "dropoff_centroid_longitude": str(dropoff[2] + rng.uniform(-.004, .004))})
            with patch("src.bronze.__main__.fetch_page", return_value=rows):
                run_bronze(spark, config, date.date().isoformat())
            run_silver(spark, config, date.date().isoformat())
            run_gold(spark, config, date.date().isoformat())
            print(f"Demo date ready: {date.date()}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

## scripts/prepare_validation.py

```python
"""Create an explicitly limited real-API validation config, separate from normal data."""
from pathlib import Path

import yaml

from src.common import load_config


def main():
    config = load_config("config/chicago_taxi.yml")
    root = Path("data/validation")
    for layer in ("bronze", "silver", "gold"):
        settings = config[layer]["output"] if layer == "bronze" else config[layer]
        settings["path" if layer == "bronze" else "output_path"] = str(root / layer / "chicago_taxi")
        settings["reports_path"] = str(root / "reports")
        settings["logs_path"] = str(root / "logs")
        if layer != "bronze":
            settings["input_path"] = str(root / ("bronze" if layer == "silver" else "silver") / "chicago_taxi")
    config["silver"]["quarantine_path"] = str(root / "quarantine/chicago_taxi")
    config["bronze"]["ingestion"].update(page_size=500, max_pages=1)
    config["dataops"]["require_complete_bronze"] = False
    root.mkdir(parents=True, exist_ok=True)
    Path("data/validation.yml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    print("data/validation.yml: real Chicago sample, at most 500 rows; not a complete day")


if __name__ == "__main__":
    main()
```

## scripts/check_frontend.py

```python
"""Local browser acceptance checks; needs Playwright and a running demo API."""
import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright


def main():
    output = Path("docs/reports")
    output.mkdir(parents=True, exist_ok=True)
    errors = []
    checks = {}
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1060}, device_scale_factor=1)
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(os.getenv("CHICAGO_TEST_URL", "http://127.0.0.1:8000"), wait_until="networkidle")
        page.wait_for_selector("#overview-kpis .kpi")
        checks["overview"] = page.locator("#overview-kpis .kpi").count() == 6
        checks["charts"] = page.locator("#overview .js-plotly-plot").count() == 3
        page.screenshot(path=str(output / "ui-overview-desktop.png"), full_page=True)
        before = page.locator("#overview-kpis .kpi-value").first.inner_text()
        page.select_option("#company", index=1)
        page.get_by_role("button", name="Apply filters").click()
        page.wait_for_function("document.querySelector('#content').getAttribute('aria-busy') === 'false'")
        checks["filters"] = page.locator("#overview-kpis .kpi-value").first.inner_text() != before
        page.get_by_role("button", name="Reset filters").click()
        page.get_by_role("link", name="Operations", exact=True).click()
        page.wait_for_selector("#trip-rows .trip-link")
        checks["operations"] = page.locator("#operations .js-plotly-plot").count() == 4
        page.screenshot(path=str(output / "ui-operations-desktop.png"), full_page=True)
        first = page.locator("#trip-rows .trip-link").first.inner_text()
        page.get_by_role("button", name="Next page").click()
        page.wait_for_function("document.querySelector('#content').getAttribute('aria-busy') === 'false'")
        checks["pagination"] = page.locator("#trip-rows .trip-link").first.inner_text() != first
        page.locator("#trip-rows .trip-link").first.click()
        page.wait_for_selector(".maplibregl-canvas")
        page.wait_for_function("state.map && state.map.loaded()", timeout=60000)
        checks["map_features"] = page.evaluate("state.geo.features.length > 0")
        checks["trip_detail"] = page.locator("#trip-detail dl").count() == 1
        checks["map_tiles"] = page.evaluate("state.map.isSourceLoaded('basemap')")
        page.get_by_role("button", name="Revenue by Area", exact=True).click()
        checks["area_mode"] = page.evaluate("state.map.getLayoutProperty('area-circles', 'visibility') === 'visible'")
        page.get_by_role("button", name="Pickup Density", exact=True).click()
        page.screenshot(path=str(output / "ui-geography-desktop.png"), full_page=True)
        page.get_by_role("button", name="At pickup").click()
        page.wait_for_function("!document.querySelector('#nearby').disabled")
        checks["optional_places"] = "unavailable" in page.locator("#place-results").inner_text()
        page.get_by_role("link", name="Data Quality", exact=True).click()
        page.wait_for_selector("#quality-kpis .kpi")
        checks["quality"] = page.locator("#quality-kpis .kpi").count() == 6
        checks["quality_filter_scope"] = page.locator("#company").is_disabled()
        page.screenshot(path=str(output / "ui-quality-desktop.png"), full_page=True)
        for width, height in [(390, 844), (768, 1024)]:
            page.set_viewport_size({"width": width, "height": height})
            for section in ("overview", "geography", "quality"):
                page.locator(f'nav a[data-page="{section}"]').click()
                page.wait_for_function("document.querySelector('#content').getAttribute('aria-busy') === 'false'")
                checks[f"no_overflow_{section}_{width}"] = page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                page.screenshot(path=str(output / f"ui-{section}-{width}.png"), full_page=True)
        checks["no_js_errors"] = not errors
        browser.close()
    (output / "frontend-checks.json").write_text(json.dumps({"checks": checks, "errors": errors}, indent=2), encoding="utf-8")
    print(json.dumps(checks, indent=2))
    if not all(checks.values()):
        raise SystemExit("Frontend checks failed")


if __name__ == "__main__":
    main()
```

## scripts/check_release.py

```python
"""Acceptance against the real Compose stack; run twice to verify replacement safety."""
import argparse
import json
import math
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests


def docker(*args):
    executable = shutil.which("docker")
    if not executable and os.name == "nt":
        executable = r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"
    result = subprocess.run([executable or "docker", "compose", *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--processing-date", default="2023-06-01")
    parser.add_argument("--runs", type=int, choices=[1, 2], default=2)
    parser.add_argument("--api-url", default="http://127.0.0.1:18000")
    args = parser.parse_args()
    datetime.strptime(args.processing_date, "%Y-%m-%d")
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "processing_date": args.processing_date,
              "runs": [], "checks": {}}
    try:
        previous = None
        previous_snapshot = None
        for number in range(args.runs):
            run_id = "acceptance_" + uuid4().hex
            docker("exec", "-T", "airflow", "airflow", "dags", "trigger", "chicago_taxi_batch_pipeline",
                   "--run-id", run_id, "--conf", json.dumps({"processing_date": args.processing_date}))
            print(f"Started DAG {number + 1}: {run_id}", flush=True)
            deadline = time.monotonic() + 1800
            state = ""
            while time.monotonic() < deadline:
                state = docker("exec", "-T", "postgres", "psql", "-U", "airflow", "-d", "airflow", "-At", "-c",
                               f"SELECT state FROM dag_run WHERE run_id='{run_id}'")
                if state in ("success", "failed"):
                    break
                time.sleep(10)
            if state != "success":
                raise RuntimeError(f"DAG did not succeed: {state or 'timeout'}")
            params = {"start_date": args.processing_date, "end_date": args.processing_date}
            kpi_response = requests.get(args.api_url + "/api/kpis", params=params, timeout=60)
            kpi_response.raise_for_status()
            kpis = kpi_response.json()
            dq_response = requests.get(args.api_url + "/api/data-quality/summary", params=params, timeout=60)
            dq_response.raise_for_status()
            dq = dq_response.json()
            snapshot = json.loads(docker("exec", "-T", "airflow", "python", "-m", "scripts.inspect_release",
                                         "--processing-date", args.processing_date))
            report["runs"].append({"dag_run_id": run_id, "state": state, "kpis": kpis, "quality": dq, "snapshot": snapshot})
            report["checks"]["unique_gold_trip_ids"] = snapshot["trip_rows"] == snapshot["unique_trip_ids"]
            if previous is not None:
                report["checks"]["rerun_kpis_identical"] = all(
                    math.isclose(value, previous[key], rel_tol=1e-10, abs_tol=1e-8)
                    if isinstance(value, float) else value == previous[key] for key, value in kpis.items())
                report["checks"]["rerun_content_identical"] = snapshot["content_sha256"] == previous_snapshot["content_sha256"]
                report["checks"]["rerun_new_run_id"] = snapshot["gold_run_id"] != previous_snapshot["gold_run_id"]
            previous = kpis
            previous_snapshot = snapshot
            report["checks"]["reconciliation"] = dq["input_rows"] == dq["valid_rows"] + dq["rejected_rows"]
            report["checks"]["gold_matches_silver"] = kpis["total_trips"] == dq["valid_rows"]
            report["checks"]["complete_source"] = all(run["pagination_complete"] for run in dq["runs"])
            print(f"DAG succeeded: {dq['input_rows']} input rows, {kpis['total_trips']} Gold trips", flush=True)
        for path in ("/api/health", "/", "/api/daily", "/api/hourly", "/api/geo/trips", "/api/data-quality/errors"):
            response = requests.get(args.api_url + path, timeout=60)
            report["checks"][path] = response.status_code == 200
        if not all(report["checks"].values()):
            raise AssertionError("Acceptance checks failed")
        report["status"] = "PASS"
    except Exception as exc:
        report.update(status="FAIL", error=str(exc))
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        destination = Path("docs/reports/compose-acceptance.json")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
```

## scripts/inspect_release.py

```python
"""Inspect a Gold snapshot downloaded from S3, without reading the job workspace."""
import argparse
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.object_store import client, download, read_pointer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--processing-date", required=True)
    args = parser.parse_args()
    publication, _ = read_pointer(client(), "gold", args.processing_date)
    if publication is None:
        raise FileNotFoundError("No published Gold snapshot")
    snapshot = download(publication, Path("data/release-check"))
    tables = [pq.ParquetFile(file).read() for file in (snapshot / publication["output"] / "trips").glob("*.parquet")]
    rows = pa.concat_tables(tables).to_pylist()
    ordered = sorted(rows, key=lambda row: row["trip_id"])
    digest = hashlib.sha256(json.dumps(ordered, sort_keys=True, default=str).encode()).hexdigest()
    print(json.dumps({"gold_run_id": publication["run_id"], "trip_rows": len(rows),
                      "unique_trip_ids": len({row["trip_id"] for row in rows}),
                      "content_sha256": digest, "verified_object_files": len(publication["files"])}))


if __name__ == "__main__":
    main()
```

## tests/conftest.py

```python
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    hadoop_home = Path(__file__).resolve().parents[1] / ".hadoop"
    if (hadoop_home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] = f"{hadoop_home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"

    session = (
        SparkSession.builder.master("local[1]")
        .appName("chicago-taxi-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def silver_df(spark: SparkSession):
    rows = [
        ("1", "taxi-1", "2026-01-01 14:00:00", "2026-01-01 15:00:00", 8, 32, 3600, 60.0, 10.0, "10.00", "2.00", "12.00", "CREDIT CARD", "A", True, 0.2),
        ("2", "taxi-2", "2026-01-01 15:00:00", "2026-01-01 15:30:00", 8, 33, 1800, 30.0, 5.0, "20.00", "0.00", "20.00", "CASH", "B", False, 0.0),
        ("3", "taxi-3", "2026-01-02 10:00:00", "2026-01-02 13:30:00", 9, 33, 12600, 210.0, 0.0, "350.00", "200.00", "550.00", "CREDIT CARD", "A", True, 0.5714),
    ]
    columns = [
        "trip_id",
        "taxi_id",
        "trip_start_timestamp",
        "trip_end_timestamp",
        "pickup_community_area",
        "dropoff_community_area",
        "trip_seconds",
        "trip_duration_minutes",
        "trip_miles",
        "fare",
        "tips",
        "trip_total",
        "payment_type",
        "company",
        "has_tip",
        "tip_rate",
    ]
    df = spark.createDataFrame(rows, columns)
    return (
        df.withColumn("trip_start_timestamp", F.to_timestamp("trip_start_timestamp"))
        .withColumn("trip_end_timestamp", F.to_timestamp("trip_end_timestamp"))
        .withColumn("trip_date", F.to_date("trip_start_timestamp"))
        .withColumn("trip_year", F.year("trip_start_timestamp"))
        .withColumn("trip_month", F.month("trip_start_timestamp"))
        .withColumn("trip_hour", F.hour("trip_start_timestamp"))
        .withColumn("fare", F.col("fare").cast("decimal(12,2)"))
        .withColumn("tips", F.col("tips").cast("decimal(12,2)"))
        .withColumn("trip_total", F.col("trip_total").cast("decimal(12,2)"))
    )
```

## tests/test_bronze.py

```python
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
```

## tests/test_silver.py

```python
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
```

## tests/test_gold.py

```python
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
```

## tests/test_api.py

```python
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
```

## tests/test_dataops.py

```python
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.dataops.__main__ import audit_run, latest_manifest, quality_gate, run_check

POLICY = {"warning_threshold": 0.02, "failure_threshold": 0.05,
          "allow_empty": False, "require_complete_bronze": True}


@pytest.mark.parametrize("rejected,expected", [(0, "PASS"), (1, "PASS"), (2, "WARNING"),
                                              (4, "WARNING"), (5, "FAIL"), (100, "FAIL")])
def test_gate_boundaries(rejected, expected):
    assert quality_gate({"input_rows": 100, "valid_rows": 100-rejected,
                         "rejected_rows": rejected, "rejection_rate": rejected/100}, POLICY) == expected


def test_gate_empty_and_invalid_metrics():
    report = {"input_rows": 0, "valid_rows": 0, "rejected_rows": 0, "rejection_rate": 0}
    assert quality_gate(report, POLICY) == "FAIL"
    assert quality_gate(report, {**POLICY, "allow_empty": True}) == "PASS"
    for changes in ({"rejection_rate": float("nan")}, {"valid_rows": 1}, {"input_rows": -1}):
        with pytest.raises(ValueError):
            quality_gate({**report, **changes}, POLICY)
    with pytest.raises(ValueError):
        quality_gate(report, {**POLICY, "warning_threshold": 0.1})


def test_latest_failure_is_not_hidden(tmp_path):
    for number, status in [(1, "SUCCESS"), (2, "FAILED")]:
        folder = tmp_path / "bronze" / str(number)
        folder.mkdir(parents=True)
        (folder / "manifest.json").write_text(json.dumps({"processing_date": "2023-06-01",
                                                         "started_at": str(number), "status": status}))
    assert latest_manifest(tmp_path, "bronze", "2023-06-01")["status"] == "FAILED"


def test_publication_validation_and_audit(tmp_path):
    output = tmp_path / "partition"
    output.mkdir()
    pq.write_table(pa.table({"trip_id": ["a", "b"]}), output / "part.parquet")
    (output / "_bronze_run.json").write_text('{"run_id":"BRZ_test"}')
    reports = tmp_path / "reports"
    folder = reports / "bronze" / "BRZ_test"
    folder.mkdir(parents=True)
    manifest = {"run_id": "BRZ_test", "processing_date": "2023-06-01", "started_at": "1",
                "finished_at": "2", "status": "SUCCESS", "output_path": str(output),
                "rows_downloaded": 2, "pagination_complete": True}
    path = folder / "manifest.json"
    path.write_text(json.dumps(manifest))
    config = {"bronze": {"output": {"reports_path": str(reports)}}, "dataops": POLICY}
    assert run_check(config, "bronze", "2023-06-01")["check_status"] == "PASS"
    manifest["pagination_complete"] = False
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="incomplete"):
        run_check(config, "bronze", "2023-06-01")
    audit = json.loads((reports / "audit" / "BRZ_test.json").read_text())
    assert audit["check_status"] == "FAIL" and audit["input_rows"] == 2


def test_gold_audit_counts_trips_not_aggregate_rows(tmp_path):
    record = audit_run({"run_id": "GLD_test", "output_rows": {"trips": 100, "daily": 1}},
                       "gold", tmp_path, "PASS")
    assert record["output_rows"] == 100
```

## tests/test_object_store.py

```python
import hashlib
import io
import json

import pytest
from botocore.exceptions import ClientError

from src import object_store as store


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.fail_upload = False

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body, metadata = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": hashlib.md5(body).hexdigest()}

    def put_object(self, Bucket, Key, Body, Metadata=None, **conditions):
        if self.fail_upload and Key.endswith("part.parquet"):
            raise RuntimeError("Simulated interrupted upload")
        if conditions.get("IfNoneMatch") and Key in self.objects:
            raise RuntimeError("Precondition failed")
        if conditions.get("IfMatch") and self.get_object(Bucket, Key)["ETag"] != conditions["IfMatch"]:
            raise RuntimeError("Concurrent publication")
        self.objects[Key] = (Body.read() if hasattr(Body, "read") else Body, Metadata or {})

    def head_object(self, Bucket, Key):
        body, metadata = self.objects[Key]
        return {"ContentLength": len(body), "Metadata": metadata}

    def download_file(self, Bucket, Key, Filename):
        from pathlib import Path
        Path(Filename).write_bytes(self.objects[Key][0])


def test_publication_download_and_failed_replacement(tmp_path, monkeypatch):
    s3 = MemoryS3()
    monkeypatch.setattr(store, "client", lambda: s3)
    output = tmp_path / "bronze/chicago_taxi/processing_date=2023-06-01"
    output.mkdir(parents=True)
    (output / "part.parquet").write_bytes(b"parquet-fixture")
    manifest = {"run_id": "BRZ_first", "processing_date": "2023-06-01", "output_path": str(output)}
    first = store.publish(manifest, "bronze", tmp_path)
    snapshot = store.download(first, tmp_path / "cache")
    assert (snapshot / first["output"] / "part.parquet").read_bytes() == b"parquet-fixture"
    s3.fail_upload = True
    with pytest.raises(RuntimeError, match="interrupted"):
        store.publish({**manifest, "run_id": "BRZ_second"}, "bronze", tmp_path)
    assert store.read_pointer(s3, "bronze", "2023-06-01")[0]["run_id"] == "BRZ_first"


def test_checksum_and_path_protection(tmp_path, monkeypatch):
    s3 = MemoryS3()
    monkeypatch.setattr(store, "client", lambda: s3)
    s3.objects["run/file"] = (b"corrupted", {})
    publication = {"run_id": "BRZ_bad", "prefix": "run/", "files": [
        {"path": "file", "size": 9, "sha256": "incorrect"}]}
    with pytest.raises(ValueError, match="checksum"):
        store.download(publication, tmp_path)
    assert not (tmp_path / "BRZ_bad").exists()
    with pytest.raises(ValueError, match="escapes"):
        store.contained(tmp_path, "../outside")


def test_restore_rebases_lineage_and_removes_old_parts(tmp_path, monkeypatch):
    s3 = MemoryS3()
    monkeypatch.setattr(store, "client", lambda: s3)
    source = tmp_path / "source"
    output = source / "bronze/chicago_taxi/processing_date=2023-06-01"
    output.mkdir(parents=True)
    (output / "part.parquet").write_bytes(b"new")
    report = source / "reports/bronze/BRZ_one"
    report.mkdir(parents=True)
    manifest = {"run_id": "BRZ_one", "processing_date": "2023-06-01", "output_path": str(output)}
    (report / "manifest.json").write_text(json.dumps(manifest))
    store.publish(manifest, "bronze", source)
    target = tmp_path / "target"
    old = target / output.relative_to(source)
    old.mkdir(parents=True)
    (old / "stale.parquet").write_bytes(b"old")
    store.restore("bronze", "2023-06-01", target)
    assert not (old / "stale.parquet").exists()
    restored = json.loads((target / "reports/bronze/BRZ_one/manifest.json").read_text())
    assert restored["output_path"] == str(old)
```

## tests/test_s3_integration.py

```python
"""Opt-in checks against the real local S3 server, in an isolated temporary bucket."""
import os
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from src import object_store as store


@pytest.mark.skipif(os.getenv("CHICAGO_S3_TESTS") != "1", reason="Requires the local Compose object store")
def test_real_s3_publication_and_preconditions(tmp_path, monkeypatch):
    name = "chicago-test-" + uuid4().hex
    monkeypatch.setenv("S3_BUCKET", name)
    s3 = store.client()
    s3.create_bucket(Bucket=name)
    try:
        directory = tmp_path / "bronze/chicago_taxi/processing_date=2023-06-01"
        directory.mkdir(parents=True)
        (directory / "part.parquet").write_bytes(b"storage test fixture, not business data")
        manifest = {"run_id": "BRZ_first", "processing_date": "2023-06-01", "output_path": str(directory)}
        first = store.publish(manifest, "bronze", tmp_path)
        before, etag = store.read_pointer(s3, "bronze", "2023-06-01")
        assert before["run_id"] == "BRZ_first"
        with pytest.raises(ClientError):
            s3.put_object(Bucket=name, Key=store.pointer_key("bronze", "2023-06-01"),
                          Body=b"invalid", IfNoneMatch="*")
        second = store.publish({**manifest, "run_id": "BRZ_second"}, "bronze", tmp_path)
        assert store.read_pointer(s3, "bronze", "2023-06-01")[0]["run_id"] == "BRZ_second"
        with pytest.raises(ClientError):
            s3.put_object(Bucket=name, Key=store.pointer_key("bronze", "2023-06-01"), Body=b"stale", IfMatch=etag)
        snapshot = store.download(second, tmp_path / "cache")
        assert (snapshot / second["output"] / "part.parquet").read_bytes() == (directory / "part.parquet").read_bytes()
        assert s3.head_object(Bucket=name, Key=first["prefix"] + "publication.json")["ContentLength"] > 0
    finally:
        # Only the freshly-created test bucket is removed, never the application bucket.
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=name):
            keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=name, Delete={"Objects": keys})
        s3.delete_bucket(Bucket=name)
```

## compose.yml

```yaml
name: chicago-taxi

x-storage: &storage
  S3_ENDPOINT_URL: http://object-store:8333
  S3_BUCKET: chicago-taxi
  AWS_ACCESS_KEY_ID: ${CHICAGO_S3_USER:-chicago-local}
  AWS_SECRET_ACCESS_KEY: ${CHICAGO_S3_PASSWORD:-chicago-local-storage-only}
  AWS_DEFAULT_REGION: us-east-1

services:
  object-store:
    image: chrislusf/seaweedfs:4.46
    command: ["mini", "-dir=/data", "-master.volumeSizeLimitMB=128", "-volume.max=8"]
    environment: *storage
    volumes:
      - object-data:/data
    ports:
      - "127.0.0.1:${CHICAGO_S3_PORT:-18333}:8333"
      - "127.0.0.1:${CHICAGO_STORAGE_UI_PORT:-18888}:8888"
    healthcheck:
      test: ["CMD", "wget", "-q", "-O", "/dev/null", "http://localhost:9333/cluster/status"]
      interval: 10s
      timeout: 5s
      retries: 20
    mem_limit: 512m

  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: ${CHICAGO_POSTGRES_PASSWORD:-chicago-local-db-only}
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow"]
      interval: 5s
      retries: 20
    mem_limit: 256m

  airflow:
    build:
      context: .
      dockerfile: docker/airflow.Dockerfile
    environment:
      <<: *storage
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:${CHICAGO_POSTGRES_PASSWORD:-chicago-local-db-only}@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/chicago/dags
      AIRFLOW__CORE__PARALLELISM: "1"
      AIRFLOW__CORE__AUTH_MANAGER: airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager
      AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: artefact:admin
      AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE: /opt/airflow/simple_auth_manager_passwords.json
      AIRFLOW__API__WORKERS: "1"
      AIRFLOW__API_AUTH__JWT_SECRET: ${CHICAGO_JWT_SECRET:-chicago-local-jwt-change-before-sharing}
      AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES: "1"
      CHICAGO_AIRFLOW_PASSWORD: ${CHICAGO_AIRFLOW_PASSWORD:-chicago-local}
      CHICAGO_PROJECT_ROOT: /opt/chicago
      CHICAGO_CONFIG: config/chicago_taxi.yml
      SPARK_MASTER: local[1]
      SPARK_LOCAL_IP: 127.0.0.1
      PYTHONPATH: /opt/chicago
    command: ["python", "/opt/chicago/scripts/start_airflow.py"]
    ports:
      - "127.0.0.1:${CHICAGO_AIRFLOW_PORT:-18080}:8080"
    volumes:
      - airflow-home:/opt/airflow
      - ./data/docker:/opt/chicago/data
    depends_on:
      postgres:
        condition: service_healthy
      object-store:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/v2/monitor/health', timeout=5)"]
      interval: 20s
      timeout: 10s
      retries: 20
      start_period: 120s
    mem_limit: 3g
    cpus: 2

  api:
    build:
      context: .
      dockerfile: docker/api.Dockerfile
    env_file:
      - path: .env.local
        required: false
    environment:
      <<: *storage
      CHICAGO_DATA_ROOT: /opt/chicago/data
    ports:
      - "127.0.0.1:${CHICAGO_API_PORT:-18000}:8000"
    volumes:
      - api-cache:/opt/chicago/data
    depends_on:
      object-store:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health', timeout=10)"]
      interval: 20s
      timeout: 15s
      retries: 10
    mem_limit: 768m

volumes:
  object-data:
  postgres-data:
  airflow-home:
  api-cache:
```

## docker/api.Dockerfile

```Dockerfile
FROM python:3.12-slim-bookworm
WORKDIR /opt/chicago
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt
COPY src/__init__.py src/__init__.py
COPY src/api/ src/api/
COPY src/object_store.py src/object_store.py
COPY frontend/ frontend/
RUN useradd --uid 10001 --create-home app && mkdir -p data && chown app:app data
USER app
CMD ["python", "-m", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## docker/airflow.Dockerfile

```Dockerfile
FROM apache/airflow:3.2.1-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
RUN install -d -o airflow -g root /opt/chicago/data
USER airflow
COPY requirements-pipeline.txt /tmp/requirements-pipeline.txt
RUN pip install --no-cache-dir "apache-airflow==3.2.1" -r /tmp/requirements-pipeline.txt
WORKDIR /opt/chicago
COPY --chown=airflow:root src/ src/
COPY --chown=airflow:root scripts/ scripts/
COPY --chown=airflow:root dags/ dags/
COPY --chown=airflow:root config/ config/
COPY --chown=airflow:root frontend/ frontend/
COPY --chown=airflow:root tests/ tests/
```
