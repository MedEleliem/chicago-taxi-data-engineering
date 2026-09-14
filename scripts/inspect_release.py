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
