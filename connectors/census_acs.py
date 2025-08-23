import os
import requests
import pandas as pd

CENSUS_API_KEY = os.getenv("CENSUS_API_KEY", "")
BASE = "https://api.census.gov/data"

def fetch_broadband_adoption_by_state(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Returns a DataFrame with columns:
      state (FIPS as string), NAME (state name from Census), year, broadband_adoption_share (percent)
    Skips 2020 (standard ACS 1-year was not published).
    """
    frames = []
    for year in range(start_year, end_year + 1):
        if year == 2020:
            continue
        url = f"{BASE}/{year}/acs/acs1"
        params = {
            "get": "NAME,B28002_001E,B28002_004E",
            "for": "state:*",
        }
        if CENSUS_API_KEY:
            params["key"] = CENSUS_API_KEY
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        cols, rows = data[0], data[1:]
        df = pd.DataFrame(rows, columns=cols)
        df["year"] = year
        # numeric cleanup
        for c in ["B28002_001E", "B28002_004E", "state"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        # percent of households with a broadband subscription
        df["broadband_adoption_share"] = (df["B28002_004E"] / df["B28002_001E"]) * 100.0
        frames.append(df[["state", "NAME", "year", "broadband_adoption_share"]])

    return pd.concat(frames, ignore_index=True)

