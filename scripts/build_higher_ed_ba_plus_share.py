#!/usr/bin/env python3
import os, sys, json, time
from pathlib import Path
from datetime import datetime
import requests

OUT_JSON = Path("site/data/v1/higher_ed_ba_plus_share.json")
OUT_CSV  = Path("site/data/v1/csv/higher_ed_ba_plus_share.csv")

STATE_FIPS = {
 "01":"Alabama","02":"Alaska","04":"Arizona","05":"Arkansas","06":"California","08":"Colorado","09":"Connecticut",
 "10":"Delaware","12":"Florida","13":"Georgia","15":"Hawaii","16":"Idaho","17":"Illinois","18":"Indiana","19":"Iowa",
 "20":"Kansas","21":"Kentucky","22":"Louisiana","23":"Maine","24":"Maryland","25":"Massachusetts","26":"Michigan",
 "27":"Minnesota","28":"Mississippi","29":"Missouri","30":"Montana","31":"Nebraska","32":"Nevada","33":"New Hampshire",
 "34":"New Jersey","35":"New Mexico","36":"New York","37":"North Carolina","38":"North Dakota","39":"Ohio","40":"Oklahoma",
 "41":"Oregon","42":"Pennsylvania","44":"Rhode Island","45":"South Carolina","46":"South Dakota","47":"Tennessee","48":"Texas",
 "49":"Utah","50":"Vermont","51":"Virginia","53":"Washington","54":"West Virginia","55":"Wisconsin","56":"Wyoming"
}
EXCLUDE_FIPS = {"11"}  # DC excluded

def load_key():
    key = os.getenv("CENSUS_API_KEY", "")
    if not key and Path(".env").exists():
        for line in Path(".env").read_text().splitlines():
            if line.strip().startswith("CENSUS_API_KEY="):
                key = line.split("=",1)[1].strip().strip('"').strip("'")
                break
    return key  # optional

def year_available(y, key):
    url = f"https://api.census.gov/data/{y}/acs/acs1/subject"
    params = {"get":"NAME,S1501_C02_015E","for":"state:*"}
    if key: params["key"]=key
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 404:
        return False
    r.raise_for_status()
    return True

def compute_years():
    # Build candidate years from 2010..current-1, probe from newest backwards
    now = datetime.utcnow().year
    candidates = list(range(2010, now))
    key = load_key()
    latest = None
    for y in range(candidates[-1], candidates[0]-1, -1):
        try:
            if year_available(y, key):
                latest = y
                break
        except requests.HTTPError:
            continue
        except Exception:
            continue
    if latest is None:
        raise SystemExit("No ACS subject years available.")
    # Build 10-year window ending at 'latest', skip 2020; backfill earlier to keep 10 points
    years = []
    y = latest
    while len(years) < 10 and y >= 2010:
        if y != 2020:
            years.append(y)
        y -= 1
    years = sorted(years[-10:])
    return years, key

def fetch_year(year, key):
    url = f"https://api.census.gov/data/{year}/acs/acs1/subject"
    params = {"get":"NAME,S1501_C02_015E","for":"state:*"}
    if key: params["key"]=key
    r = requests.get(url, params=params, timeout=60)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    rows = r.json()
    hdr = rows[0]
    idx_name = hdr.index("NAME")
    idx_val  = hdr.index("S1501_C02_015E")
    idx_fips = hdr.index("state")
    out = {}
    for rec in rows[1:]:
        fips = rec[idx_fips].zfill(2)
        if fips in EXCLUDE_FIPS or fips not in STATE_FIPS:
            continue
        name = STATE_FIPS[fips]
        try:
            v = float(rec[idx_val])
        except Exception:
            v = None
        out[name] = v
    return out

def main():
    years, key = compute_years()
    data = {}
    for y in years:
        try:
            data[y] = fetch_year(y, key)
        except Exception:
            data[y] = {}

    # CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w") as f:
        f.write("state,year,value\n")
        for y in years:
            for st in sorted(STATE_FIPS.values()):
                if st == "District of Columbia": continue
                v = (data.get(y) or {}).get(st, None)
                f.write(f"{st},{y},{'' if v is None else f'{v:.6f}'}\n")

    # JSON HI vs others
    def avg_others(y):
        vals = [v for st,v in (data.get(y) or {}).items() if st != "Hawaii" and v is not None]
        return (sum(vals)/len(vals)) if vals else None

    hi_series  = [ (data.get(y) or {}).get("Hawaii") for y in years ]
    oth_series = [ avg_others(y) for y in years ]

    notes = [
        "ACS 1-year subject table S1501_C02_015E (Adults 25+ with bachelor’s or higher).",
        "2020 ACS 1-year (experimental) skipped for comparability.",
        "If the latest release year is not yet available, the series backfills from earlier years to keep 10 points.",
        "Comparator is simple average of other 49 states; DC excluded."
    ]
    payload = {
      "metric_id": "higher_ed_ba_plus_share",
      "title": "Adults 25+ with BA+ (%)",
      "unit": "percent",
      "years": years,
      "hawaii": hi_series,
      "other_states_avg": oth_series,
      "notes": notes,
      "source": {
        "name": "Census ACS 1-year S1501",
        "url": f"https://api.census.gov/data/{years[-1]}/acs/acs1/subject"
      },
      "last_updated_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2))
    print("Years included:", years[0], "to", years[-1], "(2020 skipped)")
    print("HI last 3:", hi_series[-3:])
    print("Wrote", OUT_JSON, "and", OUT_CSV)

if __name__ == "__main__":
    sys.exit(main())
