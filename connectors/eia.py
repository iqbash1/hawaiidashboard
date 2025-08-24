import os
import requests
import pandas as pd
from typing import List, Tuple

# Reads your key from .env via pipeline.py -> load_dotenv()
EIA_API_KEY = os.getenv("FFsf7F17guAaB6ClmCeckdIippnW8ElrDrLEb236", "")
# EIA API v2 route for electric power operations (annual/monthly)
BASE = "https://api.eia.gov/v2/electricity/electric-power-operational-data"

# State code -> full name
STATE_NAMES = {
    'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado',
    'CT':'Connecticut','DE':'Delaware','FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho',
    'IL':'Illinois','IN':'Indiana','IA':'Iowa','KS':'Kansas','KY':'Kentucky','LA':'Louisiana',
    'ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan','MN':'Minnesota',
    'MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada',
    'NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina',
    'ND':'North Dakota','OH':'Ohio','OK':'Oklahoma','OR':'Oregon','PA':'Pennsylvania',
    'RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee','TX':'Texas',
    'UT':'Utah','VT':'Vermont','VA':'Virginia','WA':'Washington','WV':'West Virginia',
    'WI':'Wisconsin','WY':'Wyoming','DC':'District of Columbia'
}

# Fuel codes in this dataset (facet: fueltypeid):
#   SUN (solar), WND (wind), HYC (conventional hydro), GEO (geothermal), BIO (biomass)
#   HPS = hydro pumped storage (exclude from numerator), ALL = all fuels (denominator)
RENEWABLE_CODES = ["SUN", "WND", "HYC", "GEO", "BIO"]
TOTAL_CODE = "ALL"
PUMPED_STORAGE_CODE = "HPS"

def _eia_fetch_all(params: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Fetches all pages from EIA API v2 for the given param list.
    `params` must be a list of (key, value) tuples to support repeated keys like facets[location][].
    """
    if not EIA_API_KEY:
        raise RuntimeError("EIA_API_KEY not set. Add it to your .env")

    frames = []
    offset = 0
    limit = 5000

    while True:
        # EIA v2 supports pagination via offset/limit; API key must be in the URL (per docs)
        query = list(params) + [("api_key", EIA_API_KEY), ("offset", str(offset)), ("limit", str(limit))]
        r = requests.get(f"{BASE}/data/", params=query, timeout=60)
        r.raise_for_status()
        js = r.json()
        data = js.get("response", {}).get("data", [])
        if not data:
            break
        frames.append(pd.DataFrame(data))
        if len(data) < limit:
            break
        offset += limit

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def _fetch_generation_yearly(states: List[str], start_year: int, end_year: int, fuel_codes: List[str]) -> pd.DataFrame:
    """
    Pull annual 'generation' for specified states and fueltypeid codes over [start_year, end_year].
    """
    params: List[Tuple[str, str]] = []
    params.append(("frequency", "annual"))
    params.append(("start", str(start_year)))
    params.append(("end", str(end_year)))
    params.append(("data[0]", "generation"))  # column name per EIA v2 for this route

    # facet arrays must be repeated keys
    for st in states:
        params.append(("facets[location][]", st))
    for fc in fuel_codes:
        params.append(("facets[fueltypeid][]", fc))

    return _eia_fetch_all(params)

def fetch_renewables_share_by_state(start_year: int, end_year: int, exclude_dc: bool = True) -> pd.DataFrame:
    """
    Returns a tidy DataFrame:
      [state (abbr), state_name, year, renewables_share_pct]

    renewables_share_pct = (SUN + WND + HYC + GEO + BIO) / ALL * 100
    (HPS pumped-storage hydro is explicitly excluded from the numerator.)
    """
    # 50 states + DC (drop DC if exclude_dc)
    states = list(STATE_NAMES.keys())
    if exclude_dc:
        states = [s for s in states if s != "DC"]

    # Pull the minimum set of fuels we need to compute the share
    fuel_codes = sorted(set(RENEWABLE_CODES + [TOTAL_CODE, PUMPED_STORAGE_CODE]))

    df = _fetch_generation_yearly(states, start_year, end_year, fuel_codes)
    if df.empty:
        return pd.DataFrame(columns=["state", "state_name", "year", "renewables_share_pct"])

    # Normalize & reshape
    # Period can be YYYY or YYYY-MM; take the year part
    df["year"] = df["period"].astype(str).str.slice(0, 4).astype(int)
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")

    # Keep only what we need and pivot: (state, year) x fueltypeid
    df = df[["location", "year", "fueltypeid", "generation"]]
    wide = df.pivot_table(index=["location", "year"], columns="fueltypeid", values="generation", aggfunc="sum")

    # Ensure all codes exist, fill missing with 0 for safe arithmetic
    for code in RENEWABLE_CODES + [TOTAL_CODE, PUMPED_STORAGE_CODE]:
        if code not in wide.columns:
            wide[code] = 0.0

    wide = wide.reset_index()

    # Numerator: sum of conventional renewables (exclude pumped storage)
    wide["renewable_net_generation_mwh"] = wide[RENEWABLE_CODES].fillna(0.0).sum(axis=1)

    # Denominator: total generation (ALL)
    denom = wide[TOTAL_CODE].replace({0: pd.NA})  # avoid div-by-zero

    wide["renewables_share_pct"] = (wide["renewable_net_generation_mwh"] / denom) * 100.0
    wide["state"] = wide["location"]
    wide["state_name"] = wide["state"].map(STATE_NAMES)

    out = (
        wide[["state", "state_name", "year", "renewables_share_pct"]]
        .dropna(subset=["renewables_share_pct"])
        .sort_values(["state", "year"])
        .reset_index(drop=True)
    )
    return out

