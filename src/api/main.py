from datetime import date
import os
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response, FileResponse
from fastapi.staticfiles import StaticFiles
import requests
from botocore.exceptions import BotoCoreError, ClientError

from src.api import service

app = FastAPI(title="Chicago Taxi Intelligence", version="1.0.0")


@app.exception_handler(BotoCoreError)
@app.exception_handler(ClientError)
def unavailable_storage(request, exc):
    return JSONResponse(status_code=503, content={"detail": "Object storage is temporarily unavailable."})


@app.exception_handler(FileNotFoundError)
def missing_data(request, exc):
    return JSONResponse(status_code=503, content={"detail": "Published data or lineage reports are unavailable."})


def filters(start_date: date | None = None, end_date: date | None = None, company: str | None = None,
            payment_type: str | None = None, pickup_area: int | None = None):
    if start_date and end_date and start_date > end_date:
        raise HTTPException(422, "start_date must not exceed end_date")
    return dict(start_date=start_date, end_date=end_date, company=company, payment_type=payment_type, pickup_area=pickup_area)


@app.get("/api/health")
def health():
    published = service.publications()
    options = {"companies": [], "payments": []}
    if published:
        df = service.trips({})
        options = {"companies": sorted(df.company.unique().tolist()), "payments": sorted(df.payment_type.unique().tolist())}
    return {"status": "ok", "ready": bool(published), "mode": "demo" if (service.data_root() / "_demo.json").exists() else "live",
            "dates": [item[1]["processing_date"] for item in published], **options}


@app.get("/api/kpis")
def get_kpis(selected: dict = Depends(filters)):
    return service.kpis(service.trips(selected))


@app.get("/api/daily")
@app.get("/api/hourly")
@app.get("/api/zones")
@app.get("/api/payments")
@app.get("/api/companies")
def get_metrics(request: Request, selected: dict = Depends(filters)):
    return service.metrics(service.trips(selected), request.url.path.rsplit("/", 1)[-1])


@app.get("/api/trips")
def get_trips(selected: dict = Depends(filters), limit: int = Query(50, ge=1, le=1000), offset: int = Query(0, ge=0),
              search: str = "", sort: str = "trip_start_timestamp", descending: bool = True):
    if sort not in {"trip_start_timestamp", "trip_total", "trip_miles", "company"}:
        raise HTTPException(422, "Unsupported sort column")
    df = service.trips(selected)
    if search:
        df = df[df.trip_id.str.contains(search, case=False, regex=False) | df.company.str.contains(search, case=False, regex=False)]
    return {"total": len(df), "offset": offset, "limit": limit,
            "items": service.records(df.sort_values([sort, "trip_id"], ascending=not descending).iloc[offset:offset + limit])}


@app.get("/api/trips/{trip_id}")
def get_trip(trip_id: str, selected: dict = Depends(filters)):
    df = service.trips(selected)
    rows = df[df.trip_id == trip_id]
    if rows.empty:
        raise HTTPException(404, "Trip not found")
    return {"trip": service.records(rows.iloc[:1])[0], "matching_rows": len(rows)}


@app.get("/api/geo/pickups")
@app.get("/api/geo/trips")
def get_geo(request: Request, selected: dict = Depends(filters), limit: int = Query(2000, ge=1, le=5000)):
    return service.geojson(service.trips(selected), request.url.path.endswith("/trips"), limit)


@app.get("/api/data-quality/summary")
@app.get("/api/data-quality/errors")
@app.get("/api/data-quality/warnings")
def get_quality(request: Request, selected: dict = Depends(filters)):
    if selected["company"] or selected["payment_type"] or selected["pickup_area"] is not None:
        raise HTTPException(422, "Quality reports support date filters only")
    return service.quality(selected)[request.url.path.rsplit("/", 1)[-1]]


@app.get("/api/places/nearby")
def get_places(lat: float = Query(ge=-90, le=90), lon: float = Query(ge=-180, le=180)):
    return service.nearby_places(lat, lon)


@app.get("/api/map-style")
def map_style():
    carto = bool(os.getenv("CARTO_BASEMAP_KEY"))
    return {"version": 8, "sources": {"basemap": {"type": "raster", "tileSize": 256,
        "tiles": ["/api/tiles/{z}/{x}/{y}.png"] if carto else ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        "attribution": '<a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors' + (' &copy; <a href="https://carto.com/attributions">CARTO</a>' if carto else '')}},
        "layers": [{"id": "basemap", "type": "raster", "source": "basemap"}]}


@app.get("/api/tiles/{z}/{x}/{y}.png")
def tile(z: int, x: int, y: int):
    if not (0 <= z <= 19 and 0 <= x < 2 ** z and 0 <= y < 2 ** z):
        raise HTTPException(422, "Invalid tile coordinates")
    try:
        return Response(service.carto_tile(z, x, y), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})
    except requests.RequestException:
        raise HTTPException(502, "Basemap temporarily unavailable") from None


FRONTEND = Path(__file__).resolve().parents[2] / "frontend"
app.mount("/static", StaticFiles(directory=FRONTEND), name="frontend")


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(FRONTEND / "index.html")
