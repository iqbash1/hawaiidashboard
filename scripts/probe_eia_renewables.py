#!/usr/bin/env python3
"""
Purpose: Probe EIA API v2 for HI annual generation by fuel, compute renewables share, and emit a probe CSV.
Why: Validate the correct route+facets to unblock metric 'energy_renewables_share_generation'.
Prev error: Using 'stateid' on this route and omitting 'sectorid' triggered HTTP 500. Fix: use facets[location] and facets[sectorid].
Future-proofing: Trap HTTP errors and retry with sectorid=98 if 99 fails; classify renewables by description; exclude pumped storage.
"""
import os, sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import requests

API = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
OUT = Path("site/data/v1/csv/_probe_energy_renewables_share_generation_hi.csv")

def load_key():
    key = os.getenv("EIA_API_KEY", "")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("EIA_API_KEY="):
                key = line.split("=",1)[1].strip().strip('"').strip("'")
                break
    return key

def fetch_rows(api_key, sector_code):
    # Request all fuels for HI to classify locally. Annual since 2010.
    params = {
        "api_key": api_key,
        "frequency": "annual",
        "data[]": "generation",
        "facets[location][]": "HI",
        "facets[sectorid][]": sector_code,   # '99' = All sectors; '98' = Electric power
        "start": "2010",
        "end": str(datetime.utcnow().year),
        "length": "5000",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    r = requests.get(API, params=params, timeout=60)
    r.raise_for_status()
    j = r.json()
    return j.get("response", {}).get("data", [])

def classify_and_aggregate(rows):
    # year -> dict fuel -> MWh
    gen = defaultdict(lambda: defaultdict(float))
    for r in rows:
        y = str(r.get("period"))
        fuel_id = (r.get("fueltypeid") or "").strip()
        fuel_desc = (r.get("fuelTypeDescription") or "").lower()
        val = r.get("generation")
        try:
            mwh = float(val) if val not in (None, "", "NA") else 0.0
        except Exception:
            mwh = 0.0
        if not (y.isdigit() and fuel_id):
            continue
        gen[y][fuel_id] += mwh

    # identify renewables and exclusions by description keywords across any row
    renew_kw = ("solar","wind","hydro","water","geothermal","biomass","wood",
                "landfill","municipal solid waste","msw","black liquor","bagasse","waste","bio","biogas")
    exclude_kw = ("pumped",)  # exclude pumped storage from totals

    # build lookup fuel_id -> flags using any description encountered
    desc_by_id = {}
    for r in rows:
        fid = (r.get("fueltypeid") or "").strip()
        fd = (r.get("fuelTypeDescription") or "").lower()
        if fid:
            desc_by_id.setdefault(fid, set()).add(fd)

    is_renew = set()
    is_excluded = set()
    for fid, descs in desc_by_id.items():
        d = " ".join(descs)
        if any(k in d for k in renew_kw): is_renew.add(fid)
        if any(k in d for k in exclude_kw): is_excluded.add(fid)

    # compute shares for last 10 years available
    years = sorted([y for y in gen.keys() if y.isdigit()])
    last10 = years[-10:] if len(years) > 10 else years
    out = []
    for y in last10:
        fuels = gen[y]
        total = sum(v for f,v in fuels.items() if f not in is_excluded)
        ren = sum(v for f,v in fuels.items() if f in is_renew)
        share = (ren/total*100.0) if total > 0 else None
        out.append((y, share))
    return out, is_renew, is_excluded

def main():
    key = load_key()
    if not key:
        print("EIA_API_KEY not set. Create .env with EIA_API_KEY=... or export it. Exiting 0.")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("state,year,share\n")
        return 0

    # Try sector 99 (All sectors) first, then 98 (Electric power)
    sector_try = ["99","98"]
    rows = []
    tried = []
    for s in sector_try:
        try:
            rows = fetch_rows(key, s)
            tried.append((s, "OK", len(rows)))
            if rows:
                break
        except requests.HTTPError as e:
            tried.append((s, f"HTTP {getattr(e.response,'status_code',None)}", 0))
        except Exception as e:
            tried.append((s, f"ERR {e.__class__.__name__}", 0))
    if not rows:
        print("EIA returned no rows. Tried:", tried)
        print("Avoiding past mistake: we now always include facets[location] and facets[sectorid]. If EIA is down, re-run later.")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("state,year,share\n")
        return 0

    series, ren_ids, ex_ids = classify_and_aggregate(rows)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w") as f:
        f.write("state,year,share\n")
        for y, s in series:
            f.write(f"Hawaii,{y},{'' if s is None else f'{s:.1f}'}\n")

    print("Route:", API)
    print("Tried sectors:", tried)
    print("Renewable fuel IDs:", sorted(ren_ids))
    print("Excluded fuel IDs:", sorted(ex_ids))
    print(f"Wrote {OUT}")
    for y, s in series:
        print(f"{y}: {'' if s is None else f'{s:.1f}'}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
