from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import asyncio
import httpx
import os

app = FastAPI(title="Mexico Mileage API")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

if not GOOGLE_API_KEY:
    print("WARNING: GOOGLE_API_KEY environment variable not set")

# Very simple country detection based on whether the "Mexico" columns are filled
# Excel columns expected:
# - Mexico Origin City and State
# - Origin City, Origin State, Origin Zip
# - Mexico Dest City and State
# - Destination City, Destination State, Destination Zip

def norm(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return str(v).strip()

def build_address(city_state_str, zip_code=None, country="Mexico"):
    """
    city_state_str like 'JUAREZ,NL' or 'CHIHUAHUA,CH'
    Turn into a full address string for geocoding.
    """
    if not city_state_str:
        return None
    city_state_str = city_state_str.replace(",", ", ")
    parts = [city_state_str]
    if zip_code:
        parts.append(str(zip_code))
    parts.append(country)
    return ", ".join(parts)

def build_us_address(city, state, zip_code):
    if not city or not state:
        return None
    parts = [city.strip(), state.strip()]
    if zip_code:
        parts.append(str(zip_code))
    parts.append("USA")
    return ", ".join(parts)

def classify_and_build_addresses(row):
    """
    Implements your 4 routing rules and returns (origin_address, destination_address)
    for the Mexico leg only. If no Mexico leg, returns (None, None).
    """
    mx_origin_cs = norm(row.get("Mexico Origin City and State"))
    mx_dest_cs   = norm(row.get("Mexico Dest City and State"))

    orig_city  = norm(row.get("Origin City"))
    orig_state = norm(row.get("Origin State"))
    orig_zip   = norm(row.get("Origin Zip"))

    dest_city  = norm(row.get("Destination City"))
    dest_state = norm(row.get("Destination State"))
    dest_zip   = norm(row.get("Destination Zip"))

    # Rule 1: Origin in Mexico and Destination in the U.S.
    if mx_origin_cs and not mx_dest_cs:
        # Mexico origin address
        mx_origin_addr = build_address(mx_origin_cs, None, "Mexico")
        # Border crossing on US side: use the U.S. destination columns
        border_addr = build_us_address(dest_city, dest_state, dest_zip)
        if not border_addr:
            return None, None
        return mx_origin_addr, border_addr

    # Rule 2: Origin in the U.S. and Destination in Mexico
    if not mx_origin_cs and mx_dest_cs:
        # Border on US side: use U.S. origin columns
        border_addr = build_us_address(orig_city, orig_state, orig_zip)
        # Mexico destination
        mx_dest_addr = build_address(mx_dest_cs, None, "Mexico")
        if not border_addr or not mx_dest_addr:
            return None, None
        return border_addr, mx_dest_addr

    # Rule 3: Origin and Destination both in Mexico
    if mx_origin_cs and mx_dest_cs:
        mx_origin_addr = build_address(mx_origin_cs, None, "Mexico")
        mx_dest_addr   = build_address(mx_dest_cs, None, "Mexico")
        return mx_origin_addr, mx_dest_addr

    # Rule 4: neither side in Mexico → no Mexico miles
    return None, None

async def google_route_distance_miles(origin_address, destination_address):
    """
    Call Google Routes API: returns distance in miles or None.
    """
    if not GOOGLE_API_KEY:
        return None

    url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": "routes.distanceMeters"
    }
    payload = {
        "origin": {
            "address": origin_address
        },
        "destination": {
            "address": destination_address
        },
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE_OPTIMAL"
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(url, headers=headers, json=payload)
    if resp.status_code != 200:
        print("Google API error:", resp.status_code, resp.text[:200])
        return None

    data = resp.json()
    routes = data.get("routes", [])
    if not routes:
        return None
    meters = routes[0].get("distanceMeters")
    if meters is None:
        return None
    miles = meters * 0.000621371
    return round(miles, 1)

@app.post("/api/calculate-mileage")
async def calculate_mileage(file: UploadFile = File(...)):
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, detail="Please upload an Excel .xlsx or .xls file")

    content = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(content))
    except Exception:
        raise HTTPException(400, detail="Could not read Excel file")

    required_cols = [
        "Mexico Origin City and State",
        "Mexico Dest City and State",
        "Origin City",
        "Origin State",
        "Origin Zip",
        "Destination City",
        "Destination State",
        "Destination Zip",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(400, detail=f"Missing columns: {missing}")

    sem = asyncio.Semaphore(20)

    async def process_one(idx, row):
        async with sem:
            origin_addr, dest_addr = classify_and_build_addresses(row)
            if not origin_addr or not dest_addr:
                return idx, None
            miles = await google_route_distance_miles(origin_addr, dest_addr)
            return idx, miles

    tasks = [process_one(idx, row) for idx, row in df.iterrows()]
    results = await asyncio.gather(*tasks)

    mexico_miles = [None] * len(df)
    for idx, miles in results:
        mexico_miles[idx] = miles

    df["Mexico Miles"] = mexico_miles

    out_buf = io.BytesIO()
    df.to_excel(out_buf, index=False, engine="openpyxl")
    out_buf.seek(0)

    return StreamingResponse(
        out_buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=mexico_miles_{file.filename}"
        },
    )

@app.get("/api/health")
async def health():
    return {"status": "ok"}
