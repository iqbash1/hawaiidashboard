#!/usr/bin/env python3
import os, sys, csv, io, json, time
from pathlib import Path
from datetime import datetime
import requests

OUT_JSON = Path("site/data/v1/public_health_ypll75_rate_per_100k.json")
OUT_CSV  = Path("site/data/v1/csv/public_health_ypll75_rate_per_100k.csv")
FALLBACK = Path("data/raw/wisqars_ypll75_state.csv")

STATE_NAMES = {
 "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia",
 "Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts",
 "Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
 "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
 "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia",
 "Wisconsin","Wyoming","District of Columbia"
}
EXCLUDE_DC = True

CANDIDATE_URLS = [
  "https://wisqars.cdc.gov/data-export/ypll_75/state",
  "https://wisqars.cdc.gov/data-export/ypll/state?ypll_age=75&geography=state",
  "https://wisqars.cdc.gov/data-export/ypll_75/state?format=csv"
]

def try_remote():
    hdr = {"User-Agent":"Mozilla/5.0","Accept":"text/csv,application/octet-stream,application/json"}
    for u in CANDIDATE_URLS:
        try:
            r = requests.get(u, headers=hdr, timeout=60)
            if r.status_code != 200 or not r.content:
                continue
            text = r.content.decode("utf-8", errors="replace")
            if ("State" in text or "state" in text) and ("Year" in text or "Data Year" in text):
                return text, u
        except Exception:
            time.sleep(0.8)
            continue
    return None, None

def try_local():
    if FALLBACK.exists():
        return FALLBACK.read_text(encoding="utf-8", errors="replace"), str(FALLBACK)
    return None, None

def pick_rate_col(headers):
    # Prefer age-adjusted YPLL rate columns
    keys = [h.strip() for h in headers]
    # Rank candidates
    prefs = [
        "age-adjusted ypll rate", "age adjusted ypll rate", "ypll rate age-adjusted",
        "age-adjusted rate", "age adjusted rate", "ypll rate", "rate"
    ]
    kl = [k.lower() for k in keys]
    for p in prefs:
        for i,h in enumerate(kl):
            if p in h and "per" in h and "100" in h:
                return keys[i]
    # fallback: any header containing "rate"
    for i,h in enumerate(kl):
        if "rate" in h:
            return keys[i]
    return None

def parse_csv(text):
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    headers = reader.fieldnames or []
    # Identify key columns
    state_col = next((h for h in headers if h.lower() in ("state","location","geography")), None)
    year_col  = next((h for h in headers if h.lower() in ("year","data year","data_year","yearcode")), None)
    rate_col  = pick_rate_col(headers)
    if not (state_col and year_col and rate_col):
        raise RuntimeError(f"Could not identify required columns. Headers: {headers}")

    # Collect
    data = {}  # (state -> {year:int -> value:float|None})
    notes = set()
    for row in reader:
        st = (row.get(state_col) or "").strip()
        if st not in STATE_NAMES: 
            continue
        if EXCLUDE_DC and st == "District of Columbia":
            continue
        yraw = (row.get(year_col) or "").strip()
        y = None
        # Allow values like "2023", "2023*", "2023 (provisional?)" etc.
        for tok in yraw.replace("("," ").replace(")"," ").replace("*"," ").split():
            if tok.isdigit() and len(tok) == 4:
                y = int(tok); break
        if y is None:
            continue

        vraw = (row.get(rate_col) or "").strip()
        # Suppression/unstable: '--', '**', '--*' => None
        if vraw in ("","--","**","--*"):
            v = None
            if vraw != "": notes.add("Suppressed/unstable values set to null.")
        else:
            try:
                v = float(vraw.replace(",",""))
            except Exception:
                v = None
                notes.add("Non-numeric values set to null.")
        data.setdefault(st, {})[y] = v

    # Determine years union
    years = sorted({yr for m in data.values() for yr in m.keys()})
    if not years:
        raise RuntimeError("No usable rows parsed.")
    years10 = years[-10:] if len(years) > 10 else years

    # Write tidy CSV
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w") as fcsv:
        fcsv.write("state,year,value\n")
        for st in sorted(s for s in data.keys()):
            for y in years10:
                v = data[st].get(y, None)
                fcsv.write(f"{st},{y},{'' if v is None else f'{v:.6f}'}\n")

    # Build HI vs others avg
    def avg_others(y):
        vals = [data[s].get(y) for s in data.keys() if s != "Hawaii"]
        vals = [x for x in vals if x is not None]
        return (sum(vals)/len(vals)) if vals else None

    hi_series = [ data.get("Hawaii", {}).get(y) for y in years10 ]
    oth_series = [ avg_others(y) for y in years10 ]

    meta_notes = [
        "Age-adjusted Years of Potential Life Lost before age 75 per 100,000.",
        "Suppressed/unstable values (\"--\", \"**\", \"--*\") are null.",
    ]
    meta_notes.extend(sorted(notes))
    return years10, hi_series, oth_series, meta_notes

def main():
    used = None
    text, used = try_remote()
    remote_ok = text is not None
    if not remote_ok:
        text, used = try_local()
    if text is None:
        # Fail-soft: emit placeholder JSON/CSV with clear note
        years = list(range(datetime.utcnow().year-10, datetime.utcnow().year))
        OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        with OUT_CSV.open("w") as fcsv:
            fcsv.write("state,year,value\n")
        payload = {
            "metric_id":"public_health_ypll75_rate_per_100k",
            "title":"Premature deaths, YPLL<75 (per 100,000)",
            "unit":"per 100,000",
            "years": years,
            "hawaii": [None]*len(years),
            "other_states_avg": [None]*len(years),
            "notes":[
              "Automated WISQARS export not reachable; place CSV at data/raw/wisqars_ypll75_state.csv and re-run.",
              "Age-adjusted YPLL before age 75; suppressed values should be null."
            ],
            "source":{"name":"CDC WISQARS — YPLL export (age 75, state)","url":"https://wisqars.cdc.gov/data-export/ypll_75/state"},
            "last_updated_utc": datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
        }
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(json.dumps(payload, indent=2))
        print("WARN: Could not fetch WISQARS and no local fallback CSV found; wrote placeholders.")
        print("Place a file at", FALLBACK, "then rerun.")
        return 0

    years, hi, oth, notes = parse_csv(text)

    payload = {
      "metric_id":"public_health_ypll75_rate_per_100k",
      "title":"Premature deaths, YPLL<75 (per 100,000)",
      "unit":"per 100,000",
      "years": years,
      "hawaii": hi,
      "other_states_avg": oth,
      "notes": notes,
      "source":{"name":"CDC WISQARS — YPLL export (age 75, state)","url": used or "https://wisqars.cdc.gov/data-export/ypll_75/state"},
      "last_updated_utc": datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2))
    print("Years:", years[0], "to", years[-1])
    print("HI last 3:", hi[-3:])
    print("Wrote", OUT_JSON, "and", OUT_CSV)
    return 0

if __name__=="__main__":
    sys.exit(main())
