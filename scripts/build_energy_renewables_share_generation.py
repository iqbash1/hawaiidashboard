#!/usr/bin/env python3
import os, sys, json, time
from pathlib import Path
from collections import defaultdict
from datetime import datetime
import requests

API = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
OUT_JSON = Path("site/data/v1/energy_renewables_share_generation.json")
OUT_CSV  = Path("site/data/v1/csv/energy_renewables_share_generation.csv")

STATE_CODES = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","HI"]
STATE_NAMES = {"AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"}

def load_key():
    key = os.getenv("EIA_API_KEY", "")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("EIA_API_KEY="):
                key = line.split("=",1)[1].strip().strip('"').strip("'")
                break
    if not key:
        print("EIA_API_KEY missing. Create .env with EIA_API_KEY=... and re-run.", file=sys.stderr)
        sys.exit(0)
    return key

def eia_fetch(state, key):
    params = {
        "api_key": key,
        "frequency": "annual",
        "data[]": "generation",
        "facets[location][]": state,
        "facets[sectorid][]": "98",  # utility-scale
        "start": "2010",
        "end": str(datetime.utcnow().year),
        "length": "5000",
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
    }
    for attempt in range(3):
        r = requests.get(API, params=params, timeout=60)
        if r.status_code >= 500:
            time.sleep(1.5*(attempt+1)); continue
        r.raise_for_status()
        return r.json().get("response", {}).get("data", [])
    r.raise_for_status()

def get_desc(row):
    for k in ("fuelTypeDescription","fueltypeDescription","fuelType","fueltype","fuelDescription"):
        v = row.get(k)
        if v: return str(v).lower()
    return ""

def classify(all_rows):
    descs = {}
    for r in all_rows:
        code = str(r.get("fueltypeid") or r.get("fueltype") or "").upper().strip()
        if not code: continue
        descs.setdefault(code, set()).add(get_desc(r))
    pos = ("solar","photovoltaic","pv","wind","hydro","water","geothermal","biomass","wood","landfill","municipal solid waste","msw","black liquor","bagasse","biogas","waste wood")
    neg = ("coal","natural gas","petroleum","oil","diesel","naphtha","nuclear","uranium")
    renew, excl_tot, excl_all = set(), set(), set()
    for code, ds in descs.items():
        d = " ".join(sorted(ds))
        if "total" in d: excl_all.add(code); continue
        if "distributed" in d or "behind-the-meter" in d or code == "DPV": excl_all.add(code); continue
        if "pumped" in d: excl_tot.add(code); continue
        if any(k in d for k in pos) and not any(k in d for k in neg): renew.add(code)
    # prefer subcodes
    if "SUN" in renew and "SPV" in renew: renew.discard("SUN"); excl_all.add("SUN")
    if "WND" in renew and "WNT" in renew: renew.discard("WND"); excl_all.add("WND")
    bio_subs = {"WOO","WWW","WAS","MLG","MSB","OBW","OB2","LFG","STH","WNS"}
    if "BIO" in renew and renew.intersection(bio_subs): renew.discard("BIO"); excl_all.add("BIO")
    return renew, excl_tot, excl_all

def shares(rows, renew, excl_tot, excl_all):
    by_year = defaultdict(lambda: defaultdict(float))
    for r in rows:
        y = str(r.get("period")); code = str(r.get("fueltypeid") or r.get("fueltype") or "").upper().strip()
        val = r.get("generation")
        try: mwh = float(val) if val not in (None,"","NA") else 0.0
        except: mwh = 0.0
        if y.isdigit() and code: by_year[y][code] += mwh
    out = {}
    for y, fuels in by_year.items():
        total = sum(v for c,v in fuels.items() if c not in excl_all and c not in excl_tot)
        ren   = sum(v for c,v in fuels.items() if c in renew and c not in excl_all and c not in excl_tot)
        out[int(y)] = (ren/total*100.0) if total>0 else None
    return out

def main():
    key = load_key()
    all_rows, by_state = [], {}
    for st in STATE_CODES:
        rows = eia_fetch(st, key)
        by_state[st] = rows
        all_rows.extend(rows)
        time.sleep(0.12)
    renew, excl_tot, excl_all = classify(all_rows)
    state_shares = { st: shares(by_state[st], renew, excl_tot, excl_all) for st in STATE_CODES }
    all_years = sorted({ y for st in STATE_CODES for y in state_shares[st].keys() })
    years10 = all_years[-10:] if len(all_years)>10 else all_years
    # CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w") as f:
        f.write("state,year,value\n")
        for st in STATE_CODES:
            for y in years10:
                v = state_shares[st].get(y)
                name = STATE_NAMES[st]
                f.write(f"{name},{y},{'' if v is None else f'{v:.6f}'}\n")
    # JSON
    def avg_others(year):
        vals = [state_shares[st].get(year) for st in STATE_CODES if st != "HI"]
        vals = [v for v in vals if v is not None]
        return (sum(vals)/len(vals)) if vals else None
    payload = {
      "metric_id":"energy_renewables_share_generation",
      "title":"Renewables — share of electricity generation",
      "unit":"percent",
      "years": years10,
      "hawaii":[ state_shares["HI"].get(y) for y in years10 ],
      "other_states_avg":[ avg_others(y) for y in years10 ],
      "notes":[
        "Utility-scale generation only (EIA sectorid=98); distributed PV excluded.",
        "Pumped storage generation excluded from denominator.",
        "Aggregator fuel codes dropped when specific subcodes present."
      ],
      "source":{
        "name":"EIA electric power operational data (v2, annual, utility-scale)",
        "url":"https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
      },
      "last_updated_utc": datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2))
    print("Years included:", years10[0], "to", years10[-1])
    print("Hawaii last 3 years:", [state_shares['HI'].get(y) for y in years10[-3:]])
    print("Wrote", OUT_JSON, "and", OUT_CSV)
if __name__=="__main__":
    sys.exit(main())
