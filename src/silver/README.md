# Silver

Silver converts one Bronze partition into typed, quality-controlled data. Trusted and rejected records are stored separately.

## Input

```text
data/bronze/chicago_taxi/processing_date=YYYY-MM-DD/
```

The `_bronze_run.json` marker supplies the `source_run_id` used for lineage.

## Processing

1. Normalize column names.
2. Cast timestamps, numeric values, areas and identifiers with an explicit schema.
3. Derive trip date, hour, duration, tip flag and tip rate.
4. Apply blocking errors and non-blocking warnings.
5. Deduplicate deterministically by `trip_id`.
6. Reconcile `input_rows = valid_rows + rejected_rows`.
7. Validate and promote the trusted and quarantine partitions.

## Main rules

Blocking errors include a missing trip ID, invalid timestamps, an end before the start, non-positive duration, negative distance or negative monetary values.

Warnings include zero distance, duration mismatch, duplicate trip IDs and partial or invalid coordinates.

## Outputs

```text
data/silver/chicago_taxi/processing_date=YYYY-MM-DD/
data/quarantine/chicago_taxi/processing_date=YYYY-MM-DD/

data/reports/silver/<run_id>/
  manifest.json
  dq_report.json
  errors_report.csv
  warnings_report.csv
  column_profile.csv
```

Quarantine keeps rejected records with their `quality_errors`. Warnings do not remove a record from Silver.

## Partition behavior

Silver and Quarantine use the same `processing_date` as Bronze. For
`2023-06-01`, Silver contains 23,882 rows in 18 files and uses 3.52 MB;
Quarantine contains 455 rows in 18 files and uses 0.22 MB. The small files are
a consequence of local Spark output and are not fixed sizing targets.

Both outputs are validated in staging before promotion. A retry only replaces
that day's trusted and rejected partitions and records a new lineage chain.

## Quality gate

- rejection rate below 2%: `PASS`;
- 2% to less than 5%: `WARNING`;
- 5% or more, empty input or invalid reconciliation: `FAIL`.

A failure prevents Gold from starting. After PASS or WARNING, Airflow requests a Data Quality and Business Quality human approval.

## Run

```powershell
docker compose exec airflow python -m scripts.run_stage --layer silver --processing-date 2023-06-01
```

A rerun replaces only this date after validation and retains a separate S3 snapshot for every `run_id`.
