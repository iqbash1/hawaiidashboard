#!/usr/bin/env python3
"""
Purpose: Probe EIA v2 'electric-power-operational-data' to compute Hawaii renewables share using row-driven code classification.
Why: Facet-name-based whitelist failed. Row-driven rules are concrete and reproducible.
Avoiding past mistakes:
  - Prefer sector=98 (Electric power) to avoid DPV entirely; fall back to 99 only if needed.
  - Exclude any code with 'total' in description (aggregators SUN/WNT/TPV/TSN etc.) to prevent double-counting.
  - Exclude 'pumped' from denominator.
  - If sector=99, also exclude DPV (distributed PV) from both numerator and denominator.
  - Renewables = solar PV (utility-scale), wind, hydro (conventional), geothermal, biomass family (wood, landfill gas, MSW, black liquor, bagasse, biogas).
"""
import os, sys, json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import requests

API = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
OUT = Path("site/data/v1/csv/_probe_energy_renewables_share_generation_hi.csv")
MAP = Path("site/data/v1/csv/_probe_eia_fuel_code_map.json")

def load_key():
    key = os.getenv("EIA_API_KEY", "")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("EIA_API_KEY="):
                key = line.split("=",1)[1].strip().strip('"').strip("'")
                break
    return key

def fetch_rows(api_key, sector_code):
    params = {
        "api_key": api_key,
        "frequency": "annual",
        "data[]": "generation",
        "facets[location][]": "HI",
        "facets[sectorid][]": sector_code,  # 98=Electric power preferred, 99=All sectors fallback
        "start": "2010",
        "end": str(datetime.utcnow().year),
        "length": "5000",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    r = requests.get(API, params=params, timeout=60)
    r.raise_for_status()
    return r.json().get("response", {}).get("data", [])

def get_desc(row):
    for k in ("fuelTypeDescription","fueltypeDescription","fuelType","fueltype","fuelDescription","fueldescription"):
        v = row.get(k)
        if v:
            return str(v)
    return ""

def classify_codes(rows, sector_used):
    # Aggregate descriptions per code
    descs = defaultdict(set)
    for r in rows:
        code = str(r.get("fueltypeid") or r.get("fueltype") or "").upper().strip()
        if code:
            descs[code].add(get_desc(r).lower().strip())

    pos = (
        "solar","photovoltaic","pv",
        "wind",
        "hydro","water",
        "geothermal",
        "biomass","wood","landfill","municipal solid waste","msw",
        "black liquor","bagasse","biogas","waste wood"
    )
    neg = (
        "coal","natural gas","petroleum","oil","diesel","naphtha",
        "nuclear","uranium"
    )

    renewables = set()
    exclude_from_total = set()
    exclude_everywhere = set()  # aggregators and DPV (if sector=99)

    for code, ds in descs.items():
        d = " ".join(sorted(ds))
        dl = d.lower()
        if "total" in dl:
            exclude_everywhere.add(code)
            continue
        if "pumped" in dl:
            exclude_from_total.add(code)
            continue
        if sector_used == "99" and (code == "DPV" or "distributed" in dl or "behind-the-meter" in dl):
            exclude_everywhere.add(code)
            continue

        has_pos = any(tok in dl for tok in pos)
        has_neg = any(tok in dl for tok in neg)

        if has_pos and not has_neg:
            renewables.add(code)

    # If both BIO and its subcodes appear, drop BIO to avoid double-count
    bio_like = {c for c in renewables if c in {"BIO","WOO","WWW","WAS","MLG","MSB","OBW","OB2"}}
    if "BIO" in bio_like and len(bio_like) > 1:
        renewables.discard("BIO")

    # Save map for inspection
    MAP.parent.mkdir(parents=True, exist_ok=True)
    MAP.write_text(json.dumps({
        "sector_used": sector_used,
        "codes": {c: sorted(list(descs[c])) for c in sorted(descs.keys())},
        "renewables": sorted(list(renewables)),
        "exclude_from_total": sorted(list(exclude_from_total)),
        "exclude_everywhere": sorted(list(exclude_everywhere)),
    }, indent=2))

    return renewables, exclude_from_total, exclude_everywhere, descs

def compute_series(rows, renewables, exclude_from_total, exclude_everywhere):
    by_year = defaultdict(lambda: defaultdict(float))  # year -> code -> MWh
    for r in rows:
        y = str(r.get("period"))
        code = str(r.get("fueltypeid") or r.get("fueltype") or "").upper().strip()
        val = r.get("generation")
        try:
            mwh = float(val) if val not in (None, "", "NA") else 0.0
        except Exception:
            mwh = 0.0
        if not (y.isdigit() and code):
            continue
        by_year[y][code] += mwh

    years = sorted([y for y in by_year.keys() if y.isdigit()])
    last10 = years[-10:] if len(years) > 10 else years

    out = []
    for y in last10:
        fuels = by_year[y]
        total = sum(v for c,v in fuels.items() if c not in exclude_everywhere and c not in exclude_from_total)
        ren = sum(v for c,v in fuels.items() if c in renewables and c not in exclude_everywhere and c not in exclude_from_total)
        share = (ren/total*100.0) if total > 0 else None
        out.append((y, share))
    return out

def main():
    key = load_key()
    if not key:
        print("EIA_API_KEY not set. Create .env with EIA_API_KEY=... or export it. Exiting 0.")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("state,year,share\n")
        return 0

    tried = []
    rows = []
    sector_used = None
    for sector in ("98","99"):  # prefer 98 (utility-scale)
        try:
            rows = fetch_rows(key, sector)
            tried.append((sector, "OK", len(rows)))
            if rows:
                sector_used = sector
                break
        except requests.HTTPError as e:
            tried.append((sector, f"HTTP {getattr(e.response,'status_code',None)}", 0))
        except Exception as e:
            tried.append((sector, f"ERR {e.__class__.__name__}", 0))

    if not rows:
        print("EIA returned no rows. Tried:", tried)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text("state,year,share\n")
        return 0

    renewables, excl_total, excl_every, descs = classify_codes(rows, sector_used)
    series = compute_series(rows, renewables, excl_total, excl_every)

    # Emit CSV
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w") as f:
        f.write("state,year,share\n")
        for y, s in series:
            f.write(f"Hawaii,{y},{'' if s is None else f'{s:.1f}'}\n")

    # Print summary
    def label(c):
        ds = " | ".join(sorted(descs.get(c, [])))
        return f"{c} :: {ds}"

    print("ROUTE:", API)
    print("SECTOR tried:", tried, "USED:", sector_used)
    print("RENEWABLE codes:")
    for c in sorted(renewables): print("  +", label(c))
    print("EXCLUDED FROM TOTAL:")
    for c in sorted(excl_total): print("  -", label(c))
    print("EXCLUDED EVERYWHERE (aggregators/DPV):")
    for c in sorted(excl_every): print("  -", label(c))
    print("CSV:", OUT)
    for y, s in series:
        print(f"{y}: {'' if s is None else f'{s:.1f}'}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
