"""Build explicitly synthetic fixtures through the real Bronze/Silver/Gold jobs."""
import math
import random
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from src.common import create_spark, load_config, write_json
from src.bronze.__main__ import run_bronze
from src.silver.__main__ import run_silver
from src.gold.__main__ import run_gold


def main():
    root = Path("data/demo").resolve()
    config = deepcopy(load_config("config/chicago_taxi.yml"))
    for layer in ("bronze", "silver", "gold"):
        settings = config[layer]["output"] if layer == "bronze" else config[layer]
        settings["path" if layer == "bronze" else "output_path"] = str(root / layer / "chicago_taxi")
        settings["reports_path"], settings["logs_path"] = str(root / "reports"), str(root / "logs")
        if layer != "bronze":
            settings["input_path"] = str(root / ("bronze" if layer == "silver" else "silver") / "chicago_taxi")
    write_json({"synthetic": True, "purpose": "UI and integration demonstration, not Chicago source observations"}, root / "_demo.json")
    spark = create_spark("chicago-taxi-demo")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    rng = random.Random(42)
    areas = [(8, 41.900, -87.634), (32, 41.881, -87.629), (28, 41.876, -87.666), (6, 41.943, -87.654), (7, 41.921, -87.650)]
    try:
        for day in range(7):
            date = datetime(2023, 6, 1) + timedelta(days=day)
            rows = []
            for i in range(240 + day * 19 + (90 if day in (1, 2) else 0)):
                hour = rng.choices(range(24), weights=[2 + 8 * math.exp(-((h - 17) / 4) ** 2) + 4 * math.exp(-((h - 8) / 2) ** 2) for h in range(24)])[0]
                start = date + timedelta(hours=hour, minutes=rng.randrange(60))
                seconds = rng.randrange(180, 2400)
                pickup, dropoff = rng.choice(areas), rng.choice(areas)
                fare = round(5 + seconds / 90 + rng.random() * 8, 2)
                tips = round(fare * .18, 2) if rng.random() < .64 else 0
                rows.append({"trip_id": f"DEMO-{day}-{i:05}", "taxi_id": f"DEMO-TAXI-{i % 83}",
                    "trip_start_timestamp": start.isoformat(), "trip_end_timestamp": (start + timedelta(seconds=seconds)).isoformat(),
                    "trip_seconds": "0" if i % 59 == 0 else str(seconds), "trip_miles": "0" if i % 31 == 0 else str(round(seconds / 240 + rng.random(), 2)),
                    "fare": str(fare), "tips": str(tips), "tolls": "0", "extras": "1", "trip_total": str(round(fare + tips + 1, 2)),
                    "payment_type": rng.choice(["Credit Card", "Credit Card", "Cash", "Mobile"]),
                    "company": rng.choice(["Flash Cab", "City Service", "Sun Taxi", "Chicago Carriage"]),
                    "pickup_community_area": str(pickup[0]), "dropoff_community_area": str(dropoff[0]),
                    "pickup_centroid_latitude": str(pickup[1] + rng.uniform(-.004, .004)), "pickup_centroid_longitude": str(pickup[2] + rng.uniform(-.004, .004)),
                    "dropoff_centroid_latitude": str(dropoff[1] + rng.uniform(-.004, .004)), "dropoff_centroid_longitude": str(dropoff[2] + rng.uniform(-.004, .004))})
            with patch("src.bronze.__main__.fetch_page", return_value=rows):
                run_bronze(spark, config, date.date().isoformat())
            run_silver(spark, config, date.date().isoformat())
            run_gold(spark, config, date.date().isoformat())
            print(f"Demo date ready: {date.date()}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
