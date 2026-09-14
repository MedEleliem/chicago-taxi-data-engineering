"""Run daily partitions for an inclusive date range with one Spark session per layer."""
import argparse

from src.common import create_spark, load_config
from src.date_range import processing_dates
from scripts.run_stage import run_stage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    args = parser.parse_args()
    dates = processing_dates(args.start_date, args.end_date)
    config = load_config(args.config)
    for layer in ("bronze", "silver", "gold"):
        spark = create_spark(f"chicago-range-{layer}")
        try:
            for index, day in enumerate(dates, start=1):
                print(f"[{layer}] partition {index}/{len(dates)}: {day}", flush=True)
                run_stage(layer, day, config, spark)
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
