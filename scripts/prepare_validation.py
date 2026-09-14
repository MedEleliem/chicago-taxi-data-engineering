"""Create an explicitly limited real-API validation config, separate from normal data."""
from pathlib import Path

import yaml

from src.common import load_config


def main():
    config = load_config("config/chicago_taxi.yml")
    root = Path("data/validation")
    for layer in ("bronze", "silver", "gold"):
        settings = config[layer]["output"] if layer == "bronze" else config[layer]
        settings["path" if layer == "bronze" else "output_path"] = str(root / layer / "chicago_taxi")
        settings["reports_path"] = str(root / "reports")
        settings["logs_path"] = str(root / "logs")
        if layer != "bronze":
            settings["input_path"] = str(root / ("bronze" if layer == "silver" else "silver") / "chicago_taxi")
    config["silver"]["quarantine_path"] = str(root / "quarantine/chicago_taxi")
    config["bronze"]["ingestion"].update(page_size=500, max_pages=1)
    config["dataops"]["require_complete_bronze"] = False
    root.mkdir(parents=True, exist_ok=True)
    Path("data/validation.yml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    print("data/validation.yml: real Chicago sample, at most 500 rows; not a complete day")


if __name__ == "__main__":
    main()
