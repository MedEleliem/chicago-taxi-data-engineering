"""Run one layer, checking and publishing its output to S3 when configured."""
import argparse
import importlib
import os
from pathlib import Path

from src.common import create_spark, load_config
from src.dataops.__main__ import run_check


def run_stage(layer, processing_date, config, spark):
    settings = config["bronze"]["output"] if layer == "bronze" else config[layer]
    root = Path(settings["reports_path"]).resolve().parent
    use_s3 = bool(os.getenv("S3_ENDPOINT_URL"))
    if use_s3:
        from src import object_store
        if layer != "bronze":
            object_store.restore("bronze" if layer == "silver" else "silver",
                                 processing_date, root)
    module = importlib.import_module(f"src.{layer}.__main__")
    manifest = getattr(module, f"run_{layer}")(spark, config, processing_date)
    check = run_check(config, layer, processing_date)
    if check["check_status"] == "FAIL":
        raise RuntimeError("Quality Gate FAIL: publication stopped")
    if use_s3:
        publication = object_store.publish(manifest, layer, root)
        print(f"S3 published: {publication['prefix']}")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"])
    parser.add_argument("--processing-date", required=True)
    parser.add_argument("--config", default="config/chicago_taxi.yml")
    args = parser.parse_args()
    config = load_config(args.config)
    spark = create_spark(f"chicago-{args.layer}")
    try:
        run_stage(args.layer, args.processing_date, config, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
