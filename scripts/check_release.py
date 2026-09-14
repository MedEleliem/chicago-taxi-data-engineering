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
