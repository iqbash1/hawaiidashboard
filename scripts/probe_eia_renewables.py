#!/usr/bin/env python3
"""
Purpose: Probe EIA v2 'electric-power-operational-data' to compute Hawaii renewables share with an explicit whitelist.
Why: Prevent false positives like 'COL' (coal) and exclude small-scale PV (DPV) per v1 scope.
How we avoid past mistake: Build the renewables whitelist from facet names using positive and negative keyword rules, and print the final include/exclude lists for review.
"""
import os, sys, json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import requests

API_BASE = "https://api.eia.gov/v2"
ROUTE = "electricity/electric-power-operational-data"
DATA_URL = f"{API_BASE}/{ROUTE}/data/"
FACET_URL = f"{API_BASE}/{ROUTE}/facet/fueltypeid"
OUT_CSV = Path("site/data/v1/csv/_probe_energy_renewables_share_generation_hi.csv")
OUT_FACETS = Path("site/data/v1/csv/_probe_eia_fuel_facets.json")

def load_key():
    key = os.getenv("EIA_API_KEY", "")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("EIA_API_KEY="):
                key = line.split("=",1)[1].strip().strip('"').strip("'")
                break
    return key

def eia_get(url, params):
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_fuel_facets(api_key):
    j = eia_get(FACET_URL, {"api_key": api_key})
    vals = j.get("response", {}).get("values")
    if isinstance(vals, list):
        pass
    else:
        facets = j.get("response", {}).get("facets", [])
        vals = facets[0].get("values", []) if facets else []
    out = []
    for v in vals or []:
        code = str(v.get("code") or v.get("id") or "").strip()
        name = (v.get("name") or v.get("description") or "").strip()
        if code:
            out.append({"code": code, "name": name})
    return out

def build_whitelist(facets):
    # Positive signals for renewables
    pos = (
        "solar", "photovoltaic", "pv",  # solar utility-scale
        "wind",
        "hydro", "water",               # conventional hydro
        "geothermal",
        "biomass", "wood", "landfill", "municipal solid waste", "msw",
        "black liquor", "bagasse", "biogas", "waste wood"
    )
    # Hard negatives
    neg = (
        "coal", "natural gas", "petroleum", "oil", "diesel", "naphtha",
        "nuclear", "uranium", "pumped", "storage", "behind-the-meter",
        "distributed"
    )
    # Exclude known small-scale PV and aggregators that double-count
    explicit_exclude = {"DPV", "SUN"}  # DPV=distributed PV, SUN=total solar (often SPV+DPV)
    include, exclude_from_total, allcodes = set(), set(), set()

    for f in facets:
        code = f["code"].upper()
        name = f["name"].lower()
        allcodes.add(code)

        # Mark pumped storage to exclude from totals
        if "pumped" in name:
            exclude_from_total.add(code)
            continue

        # Decide renewables
        has_pos = any(tok in name for tok in pos)
        has_neg = any(tok in name for tok in neg)
        if code in explicit_exclude:
            has_neg = True
        if has_pos and not has_neg:
            include.add(code)

    # Tighten whitelist to typical utility-scale renewables if present
    # Prefer specific subcodes when available (e.g., SPV over SUN).
    preferred = set()
    for code in include:
        if code in {"SPV","WND","HYC","GEO","BIO","WOO","WWW","WAS","MLG","MSB","OBW","OB2"}:
            preferred.add(code)
    if preferred:
        include = preferred

    return include, exclude_from_total, allcodes

def fetch_hi_generation(api_key, sector="99"):
    params = {
        "api_key": api_key,
        "frequency": "annual",
        "data[]": "generation",
        "facets[location][]": "HI",
        "facets[sectorid][]": sector,  # 99=All sectors; 98=Electric power if needed
        "start": "2010",
        "end": str(datetime.utcnow().year),
        "length": "5000",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    j = eia_get(DATA_URL, params)
    return j.get("response", {}).get("data", [])

def compute_share(rows, whitelist, excluded_from_total):
    # year -> fuel -> MWh
    gen = defaultdict(lambda: defaultdict(float))
    for r in rows:
        y = str(r.get("period"))
        fid = str(r.get("fueltypeid") or "").upper()
        val = r.get("generation")
        try:
            mwh = float(val) if val not in (None, "", "NA") else 0.0
        except Exception:
            mwh = 0.0
        if y.isdigit() and fid:
            gen[y][fid] += mwh

    years = sorted([y for y in gen.keys() if y.isdigit()])
    last10 = years[-10:] if len(years) > 10 else years
    series = []
    for y in last10:
        fuels = gen[y]
        total = sum(v for f,v in fuels.items() if f not in excluded_from_total)
        ren = sum(v for f,v in fuels.items() if f in whitelist)
        share = (ren/total*100.0) if total > 0 else None
        series.append((y, share))
    return series

def main():
    key = load_key()
    if not key:
        print("EIA_API_KEY not set. Create .env with EIA_API_KEY=... or export it. Exiting 0.")
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        OUT_CSV.write_text("state,year,share\n")
        return 0

    facets = fetch_fuel_facets(key)
    OUT_FACETS.parent.mkdir(parents=True, exist_ok=True)
    OUT_FACETS.write_text(json.dumps(facets, indent=2))

    whitelist, excluded_from_total, allcodes = build_whitelist(facets)

    # Try 99 then 98
    tried = []
    rows = []
    for sector in ("99","98"):
        try:
            rows = fetch_hi_generation(key, sector=sector)
            tried.append((sector, "OK", len(rows)))
            if rows: break
        except requests.HTTPError as e:
            tried.append((sector, f"HTTP {getattr(e.response,'status_code',None)}", 0))

    if not rows:
        print("EIA returned no rows; tried sectors:", tried)
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        OUT_CSV.write_text("state,year,share\n")
        return 0

    series = compute_share(rows, whitelist, excluded_from_total)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w") as f:
        f.write("state,year,share\n")
        for y, s in series:
            f.write(f"Hawaii,{y},{'' if s is None else f'{s:.1f}'}\n")

    # Print mapping summary for review
    def label(c): 
        name = next((v["name"] for v in facets if str(v.get("code") or "").upper()==c), "")
        return f"{c} :: {name}"

    print("ROUTE:", DATA_URL)
    print("SECTOR tries:", tried)
    print("RENEWABLE whitelist (utility-scale focus):")
    for c in sorted(whitelist):
        print("  +", label(c))
    print("EXCLUDED FROM TOTAL (pumped/storage):")
    for c in sorted(excluded_from_total):
        print("  -", label(c))
    print("CSV:", OUT_CSV)

    for y, s in series:
        print(f"{y}: {'' if s is None else f'{s:.1f}'}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
