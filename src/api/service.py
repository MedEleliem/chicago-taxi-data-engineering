"""Read published Gold only; keep HTTP concerns in main.py."""
import json
import os
from functools import lru_cache
from pathlib import Path
from threading import Lock

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
CACHE_LOCK = Lock()


def data_root():
    return Path(os.getenv("CHICAGO_DATA_ROOT", str(ROOT / "data"))).resolve()


def publications():
    if os.getenv("S3_ENDPOINT_URL"):
        from src.object_store import gold_publications
        with CACHE_LOCK:
            return gold_publications(data_root() / "object-cache")
    result = []
    for path in sorted((data_root() / "gold/chicago_taxi").glob("processing_date=*/_manifest.json")):
        manifest = json.loads(path.read_text(encoding="utf-8"))
        validation = json.loads((path.parent / "_validation.json").read_text(encoding="utf-8"))
        if manifest["status"] == "SUCCESS" and validation.get("passed"):
            result.append((path, manifest))
    return result


@lru_cache(maxsize=4)
def read_snapshot(signature):
    frames = [pd.read_parquet(Path(path).parent / "trips") for path, run_id in signature]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def trips(filters):
    published = publications()
    start, end = filters.get("start_date"), filters.get("end_date")
    if not start and not end and published:
        published = published[-1:]
    else:
        published = [(path, manifest) for path, manifest in published
                     if (not start or pd.Timestamp(manifest["processing_date"]).date() >= start)
                     and (not end or pd.Timestamp(manifest["processing_date"]).date() <= end)]
    signature = tuple((str(path), manifest["run_id"]) for path, manifest in published)
    if not signature:
        raise FileNotFoundError("No validated Gold publication. Run Bronze, Silver and Gold first.")
    df = read_snapshot(signature)
    mask = pd.Series(True, index=df.index)
    dates = pd.to_datetime(df["trip_start_timestamp"]).dt.date
    for key, operator in [("start_date", "ge"), ("end_date", "le")]:
        if filters.get(key):
            mask &= getattr(dates, operator)(filters[key])
    for key, column in [("company", "company"), ("payment_type", "payment_type"), ("pickup_area", "pickup_community_area")]:
        if filters.get(key) is not None:
            mask &= df[column] == filters[key]
    return df.loc[mask].copy()


def records(df):
    return json.loads(df.to_json(orient="records", date_format="iso"))


def kpis(df):
    values = {"total_trips": len(df), "total_revenue": df.trip_total.sum(), "avg_trip_total": df.trip_total.mean(),
              "avg_trip_miles": df.trip_miles.mean(), "avg_trip_duration_minutes": df.trip_duration_minutes.mean(),
              "total_tips": df.tips.sum(), "tipped_trip_rate": df.has_tip.mean() if len(df) else 0,
              "unique_taxis": df.taxi_id.nunique(), "unique_companies": df.loc[df.company != "UNKNOWN", "company"].nunique()}
    return records(pd.DataFrame([values]))[0]


def metrics(df, table):
    keys = {"daily": ["trip_date"], "hourly": ["trip_date", "trip_hour"], "zones": ["pickup_community_area"],
            "payments": ["payment_type"], "companies": ["company"]}[table]
    result = df.groupby(keys, dropna=False).agg(trip_count=("trip_id", "size"), total_revenue=("trip_total", "sum"),
        avg_trip_total=("trip_total", "mean"), avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
        avg_trip_miles=("trip_miles", "mean"), total_tips=("tips", "sum"), avg_tip=("tips", "mean"),
        tipped_trip_rate=("has_tip", "mean")).reset_index()
    if table == "daily":
        result = result.rename(columns={"avg_trip_total": "avg_revenue_per_trip"})
    if table == "zones":
        located = df[df.pickup_centroid_latitude.between(-90, 90) & df.pickup_centroid_longitude.between(-180, 180)]
        centers = located.groupby("pickup_community_area", dropna=False).agg(latitude=("pickup_centroid_latitude", "mean"), longitude=("pickup_centroid_longitude", "mean")).reset_index()
        result = result.merge(centers, how="left", on="pickup_community_area")
    return records(result)


def geo_frame(df):
    mask = pd.Series(True, index=df.index)
    for side in ("pickup", "dropoff"):
        mask &= df[f"{side}_centroid_latitude"].between(-90, 90) & df[f"{side}_centroid_longitude"].between(-180, 180)
    return df.loc[mask]


def geojson(df, lines=False, limit=2000):
    source = geo_frame(df).sort_values(["trip_start_timestamp", "trip_id"])
    # Evenly spaced deterministic sample, bounded before serialization.
    if len(source) > limit:
        source = source.iloc[[int(i * len(source) / limit) for i in range(limit)]]
    features = []
    for row in records(source):
        pickup = [row["pickup_centroid_longitude"], row["pickup_centroid_latitude"]]
        dropoff = [row["dropoff_centroid_longitude"], row["dropoff_centroid_latitude"]]
        features.append({"type": "Feature", "geometry": {"type": "LineString" if lines else "Point",
                         "coordinates": [pickup, dropoff] if lines else pickup}, "properties": row})
    return {"type": "FeatureCollection", "features": features, "total": len(geo_frame(df)), "returned": len(features)}


def quality(filters):
    runs, errors, warnings = [], [], []
    for gold_path, gold in publications():
        day = pd.Timestamp(gold["processing_date"]).date()
        if filters.get("start_date") and day < filters["start_date"] or filters.get("end_date") and day > filters["end_date"]:
            continue
        report_root = gold_path.parents[3] / "reports"
        silver_dir = report_root / "silver" / gold["source_run_id"]
        silver = json.loads((silver_dir / "manifest.json").read_text(encoding="utf-8"))
        bronze_dir = report_root / "bronze" / silver["source_run_id"]
        bronze = json.loads((bronze_dir / "manifest.json").read_text(encoding="utf-8"))
        runs.append({"processing_date": gold["processing_date"], "bronze_run_id": bronze["run_id"],
                     "silver_run_id": silver["run_id"], "gold_run_id": gold["run_id"], "status": gold["status"],
                     "bronze_rows": bronze["rows_downloaded"], "input_rows": silver["input_rows"],
                     "valid_rows": silver["valid_rows"], "rejected_rows": silver["rejected_rows"],
                     "warning_rows": silver["warning_rows"], "pagination_complete": bronze["pagination_complete"]})
        errors.extend(pd.read_csv(silver_dir / "errors_report.csv").to_dict("records"))
        warnings.extend(pd.read_csv(silver_dir / "warnings_report.csv").to_dict("records"))
    summary = {key: sum(row[key] for row in runs) for key in ["bronze_rows", "input_rows", "valid_rows", "rejected_rows", "warning_rows"]}
    summary.update(rejection_rate=summary["rejected_rows"] / summary["input_rows"] if summary["input_rows"] else 0,
                   warning_rate=summary["warning_rows"] / summary["input_rows"] if summary["input_rows"] else 0, runs=runs)
    def combine(rows):
        if not rows:
            return []
        result = pd.DataFrame(rows).groupby("rule", as_index=False)["count"].sum()
        result["rate"] = result["count"] / summary["input_rows"] if summary["input_rows"] else 0.0
        return records(result.sort_values("count", ascending=False))
    return {"summary": summary, "errors": combine(errors), "warnings": combine(warnings)}


def nearby_places(lat, lon):
    key = os.getenv("FOURSQUARE_API_KEY")
    if not key:
        return {"available": False, "reason": "not_configured", "places": []}
    cache_path = data_root() / "cache/foursquare.json"
    coordinate_key = f"{lat:.4f},{lon:.4f}"
    try:
        with CACHE_LOCK:
            cached = json.loads(cache_path.read_text()) if cache_path.exists() else {}
            if coordinate_key in cached:
                return {"available": True, "cached": True, "places": cached[coordinate_key]}
            response = requests.get("https://places-api.foursquare.com/places/search",
                headers={"Authorization": f"Bearer {key}", "X-Places-Api-Version": "2025-06-17"},
                params={"ll": coordinate_key, "radius": 500, "limit": 5}, timeout=5)
            response.raise_for_status()
            places = [{"name": p.get("name"), "category": (p.get("categories") or [{}])[0].get("name"),
                       "distance": p.get("distance"), "address": p.get("location", {}).get("formatted_address")}
                      for p in response.json().get("results", [])[:5]]
            cached[coordinate_key] = places
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            temp = cache_path.with_suffix(".tmp")
            temp.write_text(json.dumps(cached), encoding="utf-8")
            temp.replace(cache_path)
        return {"available": True, "cached": False, "places": places}
    except (requests.RequestException, OSError, ValueError, KeyError, TypeError):
        return {"available": False, "reason": "unavailable", "places": []}


@lru_cache(maxsize=256)
def carto_tile(z, x, y):
    key = os.getenv("CARTO_BASEMAP_KEY")
    if not key:
        raise FileNotFoundError("CARTO key is not configured")
    response = requests.get(f"https://basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png",
                            params={"key": key}, timeout=10)
    response.raise_for_status()
    return response.content
