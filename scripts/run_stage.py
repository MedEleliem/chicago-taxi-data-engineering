"""Run one layer, checking and publishing its output to S3 when configured."""
import argparse
import importlib
import os
from pathlib import Path

from src.common import create_spark, load_config
from src.dataops.__main__ import run_check


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"])
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    args = parser.parse_args()
    config = load_config(args.config)
    settings = config["bronze"]["output"] if args.layer == "bronze" else config[args.layer]
    root = Path(settings["reports_path"]).resolve().parent
    use_s3 = bool(os.getenv("S3_ENDPOINT_URL"))
    if use_s3:
        from src import object_store
        if args.layer != "bronze":
            object_store.restore("bronze" if args.layer == "silver" else "silver", args.processing_date, root)
    module = importlib.import_module(f"src.{args.layer}.__main__")
    spark = create_spark(f"chicago-{args.layer}")
    try:
        manifest = getattr(module, f"run_{args.layer}")(spark, config, args.processing_date)
    finally:
        spark.stop()
    check = run_check(config, args.layer, args.processing_date)
    if check["check_status"] == "FAIL":
        raise SystemExit("Quality Gate FAIL: publication stopped")
    if use_s3:
        publication = object_store.publish(manifest, args.layer, root)
        print(f"S3 published: {publication['prefix']}")


if __name__ == "__main__":
    main()
