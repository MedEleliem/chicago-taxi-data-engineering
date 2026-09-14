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
