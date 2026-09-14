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
