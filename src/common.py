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
