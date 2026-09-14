"""Run the daily pipeline sequentially for an inclusive date range."""
import argparse
import subprocess
import sys

from src.date_range import processing_dates


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    args = parser.parse_args()
    for day in processing_dates(args.start_date, args.end_date):
        command = [sys.executable, "-m", "scripts.run_pipeline", "--processing-date", day,
                   "--config", args.config]
        print(f"Running partition {day}", flush=True)
        result = subprocess.run(command)
        if result.returncode:
            raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
