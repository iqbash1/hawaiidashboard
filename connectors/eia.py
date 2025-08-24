import os
import requests
import pandas as pd
from typing import List, Tuple, Iterable

# EIA API v2 route for electric power operations (annual/monthly)
BASE = "https://api.eia.gov/v2/electricity/electric-power-operational-data"

# USPS -> full state name (for labels)
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

# EIA v2 fuel codes we expect in this dataset:
#  - Solar: SUN
#  - Wind:  WND
#  - Hydro (conventional): HYC (some feeds show WAT; we accept either)
#  - Geothermal: GEO
#  - Biomass: BIO (some feeds split; we'll accept BIO*)
#  - Pumped storage: HPS (exclude from numerator)
#  - Total: ALL (denominator)
RENEWABLE_CODE_GROUPS = [
    ["SUN"],          # solar
    ["WND"],          # wind
    ["HYC", "WAT"],   # hydro conventional (accept either)
    ["GEO"],          # geothermal
    ["BIO"]           # biomass (base code)
]
TOTAL_CODE = "ALL"
PUMPED_STORAGE_CODE = "HPS"

def _states_list(exclude_dc: bool) -> List[str]:
    states = list(STATE_NAMES.keys())
    return [s for s in states if exclude_dc and s != "DC" or not exclude_dc]

def _eia_fetch_all(params: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Fetch all pages from EIA v2. Reads API key at *call time*.
    """
    api_key = os.getenv("EIA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("EIA_API_KEY not set. Add it to your .env")

    frames = []
    offset = 0
    limit = 5000
    while True:
        query = list(params) + [("api_key", api_key), ("offset", str(offset)), ("limit", str(limit))]
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

def _fetch_generation_yearly(states: Iterable[str], start_year: int, end_year: int, fuel_codes: Iterable[str]) -> pd.DataFrame:
    """
    Annual 'generation' for given states (facet: stateid) and fueltypeid codes over [start_year, end_year].
    """
    params: List[Tuple[str, str]] = []
    params.append(("frequency", "annual"))
    params.append(("start", str(start_year)))
    params.append(("end", str(end_year)))
    params.append(("data[0]", "generation"))  # field name in this dataset

    # repeated keys for array facets per EIA v2
    for st in states:
        params.append(("facets[stateid][]", st))
    for fc in fuel_codes:
        params.append(("facets[fueltypeid][]", fc))

    return _eia_fetch_all(params)

def fetch_renewables_share_by_state(start_year: int, end_year: int, exclude_dc: bool = True) -> pd.DataFrame:
    """
    Returns tidy DataFrame: [state, state_name, year, renewables_share_pct]
    renewables_share_pct = (SUN + WND + HYC/WAT + GEO + BIO*) / ALL * 100
    (Pumped storage HPS is *not* included in numerator.)
    """
    states = _states_list(exclude_dc=exclude_dc)
    # Build a set of fuels we need
    fuels_needed = set([TOTAL_CODE, PUMPED_STORAGE_CODE])
    for group in RENEWABLE_CODE_GROUPS:
        fuels_needed.update(group)
    fuel_codes = sorted(fuels_needed)

    df = _fetch_generation_yearly(states, start_year, end_year, fuel_codes)
    if df.empty:
        return pd.DataFrame(columns=["state", "state_name", "year", "renewables_share_pct"])

    # Normalize columns
    # Expected columns include: period, stateid, fueltypeid, generation
    df = df.rename(columns={"stateid": "state"})
    df["year"] = df["period"].astype(str).str[:4].astype(int)
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")

    # Pivot to columns per fuel code: (state, year) x fueltypeid
    wide = df.pivot_table(index=["state", "year"], columns="fueltypeid", values="generation", aggfunc="sum")
    wide = wide.fillna(0.0).reset_index()

    # Helper: sum any present codes in a group
    def sum_group(row, codes: Iterable[str]) -> float:
        return float(sum(row.get(c, 0.0) for c in codes if c in row.index))

    # Numerator = sum of renewable groups
    renew_sum = []
    for _, r in wide.iterrows():
        total = 0.0
        for grp in RENEWABLE_CODE_GROUPS:
            total += sum_group(r, grp)
        renew_sum.append(total)
    wide["renewable_net_generation_mwh"] = renew_sum

    # Denominator = ALL
    denom = wide.get(TOTAL_CODE)
    if denom is None:
        # If 'ALL' is absent (shouldn't be), bail gracefully
        return pd.DataFrame(columns=["state", "state_name", "year", "renewables_share_pct"])

    # Share (%)
    wide["renewables_share_pct"] = (wide["renewable_net_generation_mwh"] / wide[TOTAL_CODE].replace({0: pd.NA})) * 100.0

    # Attach names and tidy
    wide["state_name"] = wide["state"].map(STATE_NAMES)
    out = wide[["state", "state_name", "year", "renewables_share_pct"]].dropna(subset=["renewables_share_pct"])
    return out.sort_values(["state", "year"]).reset_index(drop=True)
