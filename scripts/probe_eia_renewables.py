#!/usr/bin/env python3
import os, sys, json
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import requests

API_BASE = "https://api.eia.gov/v2"
ROUTE = "electricity/electric-power-operational-data"
OUT_CSV = Path("site/data/v1/csv/_probe_energy_renewables_share_generation_hi.csv")

def load_key():
    key = os.getenv("EIA_API_KEY")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("EIA_API_KEY="):
                key = line.split("=",1)[1].strip().strip("'").strip('"')
                break
    return key or ""

def eia_get(path, params):
    p = {"api_key": params.pop("api_key")}
    p.update(params)
    r = requests.get(f"{API_BASE}/{path}", params=p, timeout=60)
    r.raise_for_status()
    return r.json()

def facet_values(facet, api_key):
    """Return list of {code, name} for a facet. Handle v2 return shape variants."""
    try:
        j = eia_get(f"{ROUTE}/facet/{facet}", {"api_key": api_key})
    except Exception:
        return []
    resp = j.get("response", {})
    # Variant A: {"values":[{"code":...,"name":...}, ...]}
    vals = resp.get("values")
    if isinstance(vals, list):
        return [{"code": v.get("code") or v.get("id") or "", "name": v.get("name") or v.get("description") or ""} for v in vals]
    # Variant B: {"facets":[{"id":facet,"values":[...] } ]}
    facets = resp.get("facets")
    if isinstance(facets, list) and facets:
        v2 = facets[0].get("values") or []
        return [{"code": v.get("code") or v.get("id") or "", "name": v.get("name") or v.get("description") or ""} for v in v2]
    return []

def pick_sector_code(api_key):
    """Prefer Electric Power sector. Fall back to All sectors if not available."""
    vals = facet_values("sectorid", api_key)
    # Prefer numeric 98 (Electric power). Else 99 (All sectors).
    code = None
    for v in vals:
        if v["code"] in ("98", 98) or "electric power" in v["name"].lower():
            code = str(v["code"])
            break
    if not code:
        for v in vals:
            if v["code"] in ("99", 99) or "all sector" in v["name"].lower():
                code = str(v["code"])
                break
    return code  # may be None if facet absent; query will omit sector filter

def renewable_codes(api_key):
    vals = facet_values("fueltypeid", api_key)
    if not vals:
        return set(), set()
    ren, exclude = set(), set()
    for v in vals:
        code = str(v["code"]).strip()
        name = (v["name"] or "").lower()
        if not code:
            continue
        # Exclude pumped storage from totals
        if "pumped" in name:
            exclude.add(code)
            continue
        if any(tok in name for tok in [
            "solar","wind","hydro","water","geothermal","biomass","wood",
            "landfill","municipal solid waste","msw","black liquor","bagasse",
            "waste","bio","biogas","paper sludge"
        ]):
            ren.add(code)
    return ren, exclude

def fetch_generation_by_fuel(state="HI", start="2010", end=None, api_key=""):
    if end is None:
        end = str(datetime.utcnow().year)
    params = {
        "api_key": api_key,
        "frequency": "annual",
        "data[]": "generation",
        "facets[stateid][]": state,
        "start": start,
        "end": end,
        "length": "5000",
    }
    j = eia_get(f"{ROUTE}/data/", params)
    rows = j.get("response", {}).get("data", [])
    out = defaultdict(lambda: defaultdict(float))  # year -> fuel -> generation MWh
    for r in rows:
        period = str(r.get("period"))
        fuel = (r.get("fueltypeid") or r.get("fueltype") or r.get("fuel") or "").strip()
        val = r.get("generation")
        try:
            gen = float(val) if val not in (None, "", "NA") else 0.0
        except Exception:
            gen = 0.0
        if period and fuel:
            out[period][fuel] += gen
    return out  # dict(year -> dict(fuel -> MWh))

def main():
    key = load_key()
    if not key:
        print("EIA_API_KEY not set. Create .env with EIA_API_KEY=... or export it.", file=sys.stderr)
        sys.exit(0)

    # Discover sector code if available
    sector = pick_sector_code(key)  # not used in query if None (route may not expose it)

    # Discover renewable and excluded fuel codes
    ren_codes, exclude_codes = renewable_codes(key)

    # Pull annual generation by fuel for HI
    gen = fetch_generation_by_fuel(state="HI", start="2010", api_key=key)
    years = sorted([y for y in gen.keys() if y.isdigit()])
    years10 = years[-10:] if len(years) > 10 else years

    # Compute totals (exclude pumped storage) and renewable sums
    out = []
    for y in years10:
        fuels = gen.get(y, {})
        total = sum(v for f, v in fuels.items() if f not in exclude_codes)
        ren = sum(v for f, v in fuels.items() if f in ren_codes)
        share = (ren / total * 100.0) if total > 0 else None
        out.append((y, share))

    # Ensure parent dir
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w") as f:
        f.write("state,year,share\n")
        for y, s in out:
            f.write(f"Hawaii,{y},{'' if s is None else f'{s:.1f}'}\n")

    # Console summary
    print(f"Route: {ROUTE}")
    print(f"Sector facet used: {sector or '(none)'}")
    print(f"Renewable fuel codes: {sorted(ren_codes)}")
    print(f"Excluded fuel codes: {sorted(exclude_codes)}")
    print(f"Wrote {OUT_CSV}")
    for y, s in out:
        print(f"{y}: {'' if s is None else f'{s:.1f}'}")

if __name__ == "__main__":
    sys.exit(main())
