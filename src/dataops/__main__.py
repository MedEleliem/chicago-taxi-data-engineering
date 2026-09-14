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
