import os
import requests
import pandas as pd
from typing import List

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
BASE = "https://api.eia.gov/v2"

def _eia_get(endpoint: str, params: dict) -> dict:
    params = {**params, "api_key": EIA_API_KEY}
    url = f"{BASE}/{endpoint}/data/"
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def _states() -> List[str]:
    # 50 states + DC (we’ll drop DC later if exclude_dc=True)
    return [
        'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN',
        'MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA',
        'WA','WV','WI','WY','DC'
    ]

def fetch_renewables_share_by_state(start_year: int, end_year: int, exclude_dc: bool = True) -> pd.DataFrame:
    """
    Returns: DataFrame with columns [state, state_name, year, renewables_share_pct]
    renewables_share_pct = (hydro + solar + wind + geothermal + biomass) / total * 100
    (Pumped-storage hydro is excluded from the numerator.)
    """
    energy_sources = ['total','hydroelectric','solar','wind','geothermal','biomass','hydroelectric_pumped_storage']
    frames = []
    for es in energy_sources:
        params = {
            "frequency": "annual",
            "data[0]": "value",
            "facets[state][]": _states(),
            "facets[energy-source][]": es,
            "start": start_year,
            "end": end_year,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        }
        js = _eia_get("electricity/state-generation", params)
        rows = js.get("response", {}).get("data", [])
        if not rows:
            continue
        df = pd.DataFrame(rows)[["state", "period", "energy-source", "value"]]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.rename(columns={"period": "year", "energy-source": "energy_source", "value": f"value_{es}"}, inplace=True)
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["state","state_name","year","renewables_share_pct"])

    from functools import reduce
    df = reduce(lambda l, r: pd.merge(l, r, on=["state", "year"], how="outer"), frames)

    # Numerator = sum of conventional renewables (exclude pumped storage)
    ren_cols = [c for c in df.columns if c in [f"value_{s}" for s in ['hydroelectric','solar','wind','geothermal','biomass']]]
    df["renewable_net_generation_mwh"] = df[ren_cols].sum(axis=1, skipna=True)

    # Denominator = total net generation
    df.rename(columns={"value_total": "total_net_generation_mwh"}, inplace=True)

    # Share (%)
    df["renewables_share_pct"] = (df["renewable_net_generation_mwh"] / df["total_net_generation_mwh"]) * 100.0

    # Map state codes to names
    state_names = {
        'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado','CT':'Connecticut',
        'DE':'Delaware','FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa',
        'KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine','MD':'Maryland','MA':'Massachusetts','MI':'Michigan',
        'MN':'Minnesota','MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada','NH':'New Hampshire',
        'NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma',
        'OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee','TX':'Texas',
        'UT':'Utah','VT':'Vermont','VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming','DC':'District of Columbia'
    }
    df["state_name"] = df["state"].map(state_names)

    # Keep only valid rows; drop DC if requested
    df = df[df["state"].isin(state_names.keys())]
    if exclude_dc:
        df = df[df["state"] != "DC"]

    return df[["state", "state_name", "year", "renewables_share_pct"]]

