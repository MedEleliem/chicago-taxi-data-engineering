# Gold

Gold builds datasets ready for FastAPI, the dashboard or a BI tool. It reads only a validated Silver partition.

## Input

```text
data/silver/chicago_taxi/processing_date=YYYY-MM-DD/
```

Gold rejects a partition containing error records or data from multiple Silver `run_id` values.

## Datasets

| Dataset | Purpose |
|---|---|
| `kpi_summary` | daily headline metrics |
| `daily` | metrics grouped by date |
| `hourly` | activity by date and hour |
| `zones` | pickup-area performance |
| `companies` | company performance |
| `payments` | payment-type distribution |
| `trips` | curated trip-level projection |
| `geo` | trips with valid map coordinates |

Metrics include trip count, revenue, distance, duration, tips and tipped-trip rate.

## Validation and output

Trip counts are reconciled between Silver, `kpi_summary` and every aggregate. All datasets are written to staging, read back, and promoted together.

```text
data/gold/chicago_taxi/processing_date=YYYY-MM-DD/
  kpi_summary/
  daily/
  hourly/
  zones/
  companies/
  payments/
  trips/
  geo/
  _manifest.json
  _validation.json
```

FastAPI downloads Gold snapshots from S3. With no date filter it reads only the
latest partition; a date range loads only matching partitions. Web requests
never start Spark.

## Partition behavior

Each Gold date is an independent directory containing all eight datasets. The
validated `2023-06-01` partition stores 23,882 curated trips in 34 files and
uses 3.29 MB locally. Aggregate tables are small; most bytes belong to `trips`
and `geo`.

The complete Gold directory is promoted as one partition. S3 keeps immutable
`runs/<run_id>` snapshots and changes `latest.json` only after checksum
verification, so API readers never select a partial publication.

## Run

```powershell
docker compose exec airflow python -m scripts.run_stage --layer gold --processing-date 2023-06-01
```

In Airflow, Gold starts only after the automated gates and an **Approve** click in the Human-in-the-Loop task.
