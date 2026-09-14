# Bronze

Bronze ingests one daily partition from the Chicago Taxi Trips SODA API. It preserves the source before applying business transformations.

## Input

- SODA dataset: `wrvz-psew`
- Lower bound: `trip_start_timestamp >= processing_date`
- Upper bound: start of the following day, excluded
- Stable pagination order: `trip_start_timestamp, trip_id`
- Configuration: `config/chicago_taxi.yml`

## Processing

1. Build the daily time window.
2. Download JSON pages with timeout, connection retries and exponential backoff.
3. Preserve all source values as strings.
4. Measure missing values, duplicates and column profiles.
5. Write to staging and read the Parquet data back.
6. Promote the validated partition.
7. Publish a versioned S3 snapshot.

## Outputs

```text
data/bronze/chicago_taxi/processing_date=YYYY-MM-DD/
  part-*.parquet
  _raw/records.json
  _bronze_run.json

data/reports/bronze/<run_id>/
  manifest.json
  bronze_dq_report.json
  column_profile.csv
```

JSON is the retained API response. The all-string Parquet dataset provides an efficient Spark input while leaving typing decisions to Silver.

## Partition behavior

One directory represents one calendar day. The validated `2023-06-01`
partition contains 24,337 rows, 6 files and uses 27.64 MB locally. Bronze size
varies with source volume and is larger than the other layers because both raw
JSON and Parquet are retained.

The job writes to `_tmp/<run_id>` first. It replaces the visible daily
partition only after the row count and files are validated.

## Blocking checks

- valid SODA response;
- expected source schema;
- Parquet row count equal to the downloaded count;
- consistent lineage marker;
- complete pagination when required by the DataOps policy.

## Run

```powershell
docker compose exec airflow python -m scripts.run_stage --layer bronze --processing-date 2023-06-01
```

Rerunning a date creates a new `run_id`. The visible partition changes only after validation, while the previous S3 snapshot remains versioned.
