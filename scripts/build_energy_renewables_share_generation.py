#!/usr/bin/env python3
"""
Build 'energy_renewables_share_generation' for all states using EIA v2 electric-power-operational-data (sector=98).
Utility-scale only. Exclude pumped storage from denominator. Exclude aggregator codes (SUN total solar, WND parent) when subcodes exist.
Outputs:
- site/data/v1/energy_renewables_share_generation.json (HI vs other-states average, last 10 years)
- site/data/v1/csv/energy_renewables_share_generation.csv (tidy: state,year,value)
Why: Provide production-ready files for the dashboard without touching the main pipeline yet.
"""
import os, sys, json, time
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
import requests

API = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
OUT_JSON = Path("site/data/v1/energy_renewables_share_generation.json")
OUT_CSV  = Path("site/data/v1/csv/energy_renewables_share_generation.csv")

STATE_CODES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","ID","IL","IN","IA","KS","KY","LA",
  "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH",
  "OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","HI"
]  # 50 states, DC excluded

STATE_NAMES = {
  "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado",
  "CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho",
  "IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana",
  "ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi",
  "MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey",
  "NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
  "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota",
  "TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington",
  "WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"
}

def load_key():
    key = os.getenv("EIA_API_KEY", "")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("EIA_API_KEY="):
                key = line.split("=",1)[1].strip().strip('"').strip("'")
                break
    return key

def eia_fetch(state, start_year="2010"):
    params = {
        "api_key": load_key(),
        "frequency": "annual",
        "data[]": "generation",
        "facets[location][]": state,
        "facets[sectorid][]": "98",  # Electric power (utility-scale)
        "start": start_year,
        "end": str(datetime.utcnow().year),
        "length": "5000",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    for attempt in range(3):
        r = requests.get(API, params=params, timeout=60)
        if r.status_code >= 500:
            time.sleep(1.5 * (attempt+1)); continue
        r.raise_for_status()
        return r.json().get("response", {}).get("data", [])
    r.raise_for_status()  # will throw last error

def get_desc(row):
    for k in ("fuelTypeDescription","fueltypeDescription","fuelType","fueltype","fuelDescription","fueldescription"):
        v = row.get(k)
        if v: return str(v)
    return ""

def classify_codes_all(rows_all_states):
    """
    Build global classification sets from all returned rows across states.
    Avoid past mistakes:
      - Exclude aggregator codes containing 'total' in description (e.g., SUN total solar).
      - Exclude DPV entirely (utility-scale only).
      - Exclude pumped storage from denominator.
      - Prefer subcodes over parent aggregators: if both SUN and SPV exist -> drop SUN; if both WND and WNT exist -> drop WND; if BIO and subcodes exist -> drop BIO.
    """
    descs = defaultdict(set)
    for r in rows_all_states:
        code = str(r.get("fueltypeid") or r.get("fueltype") or "").upper().strip()
        if code: descs[code].add(get_desc(r).lower().strip())

    def has_kw(s, kws): return any(k in s for k in kws)

    renew_pos = ("solar","photovoltaic","pv","wind","hydro","water","geothermal","biomass","wood","landfill","municipal solid waste","msw","black liquor","bagasse","biogas","waste wood")
    fossils_neg = ("coal","natural gas","petroleum","oil","diesel","naphtha","nuclear","uranium")
    renewables = set()
    excl_total = set()
    excl_every = set()

    # First pass
    for code, ds in descs.items():
        d = " ".join(sorted(ds))
        dl = d.lower()
        if "total" in dl:
            excl_every.add(code); continue
        if "distributed" in dl or "behind-the-meter" in dl or code == "DPV":
            excl_every.add(code); continue
        if "pumped" in dl:
            excl_total.add(code); continue
        if has_kw(dl, renew_pos) and not has_kw(dl, fossils_neg):
            renewables.add(code)

    # Prefer specific subcodes
    # Solar: drop SUN if SPV present
    if "SUN" in renewables and "SPV" in renewables:
        renewables.discard("SUN"); excl_every.add("SUN")
    # Wind: drop WND if WNT present
    if "WND" in renewables and "WNT" in renewables:
        renewables.discard("WND"); excl_every.add("WND")
    # Biomass: drop BIO if subcodes present
    bio_subs = {"WOO","WWW","WAS","MLG","MSB","OBW","OB2"}
    if "BIO" in renewables and len(renewables.intersection(bio_subs))>0:
        renewables.discard("BIO"); excl_every.add("BIO")

    return renewables, excl_total, excl_every, descs

def compute_shares_for_state(rows, renewables, excl_total, excl_every):
    by_year = defaultdict(lambda: defaultdict(float))
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

    shares = {}
    for y, fuels in by_year.items():
        total = sum(v for c,v in fuels.items() if c not in excl_every and c not in excl_total)
        ren   = sum(v for c,v in fuels.items() if c in renewables and c not in excl_every and c not in excl_total)
        shares[y] = (ren/total*100.0) if total>0 else None
    return shares  # {year: percent}

def main():
    key = load_key()
    if not key:
        print("EIA_API_KEY not set. Create .env with EIA_API_KEY=... or export it.")
        return 0

    # Fetch rows for all states; build global classification once
    all_rows = []
    rows_by_state = {}
    for i, st in enumerate(STATE_CODES, 1):
        try:
            rows = eia_fetch(st)
            rows_by_state[st] = rows
            all_rows.extend(rows)
            print(f"[{i:02d}/50] {st}: {len(rows)} rows")
            time.sleep(0.15)
        except Exception as e:
            print(f"[WARN] {st} fetch failed: {e}")
            rows_by_state[st] = []

    renewables, excl_total, excl_every, descs = classify_codes_all(all_rows)
    print("Renewable codes:", sorted(renewables))
    print("Exclude-from-total codes:", sorted(excl_total))
    print("Exclude-everywhere codes:", sorted(excl_every))

    # Compute shares per state-year
    shares = { st: compute_shares_for_state(rows_by_state[st], renewables, excl_total, excl_every)
               for st in STATE_CODES }

    # Determine last 10 years across all states
    all_years = sorted({ y for st in STATE_CODES for y in shares[st].keys() if y.isdigit() })
    years10 = all_years[-10:] if len(all_years) > 10 else all_years

    # Build tidy CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w") as f:
        f.write("state,year,value\n")
        for st in STATE_CODES:
            name = STATE_NAMES[st]
            for y in years10:
                v = shares[st].get(y, None)
                if v is None:
                    f.write(f"{name},{y},\n")
                else:
                    f.write(f"{name},{y},{v:.6f}\n")  # keep precision; chart will round

    # Build JSON HI vs others
    def avg_others(year):
        vals = [shares[st].get(year) for st in STATE_CODES if st != "HI"]
        vals = [v for v in vals if v is not None]
        return (sum(vals)/len(vals)) if vals else None

    hi_series = [ shares["HI"].get(y, None) for y in years10 ]
    oth_series = [ avg_others(y) for y in years10 ]

    notes = [
        "Utility-scale generation only (EIA sectorid=98). Distributed PV (DPV) excluded.",
        "Pumped storage generation excluded from denominator.",
        "Aggregator fuel codes (e.g., SUN total solar, WND parent wind) dropped when specific subcodes present.",
    ]

    payload = {
        "metric_id": "energy_renewables_share_generation",
        "title": "Renewables — share of electricity generation",
        "unit": "percent",
        "years": [int(y) for y in years10],
        "hawaii": hi_series,
        "other_states_avg": oth_series,
        "notes": notes,
        "source": {
            "name": "EIA electric power operational data (v2, annual, utility-scale)",
            "url": "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
        },
        "last_updated_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w") as jf:
        json.dump(payload, jf, indent=2)

    print(f"Wrote {OUT_JSON} and {OUT_CSV}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
