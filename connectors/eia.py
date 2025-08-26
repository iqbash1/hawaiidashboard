import os, requests, pandas as pd
from typing import List

EIA_BASE = "https://api.eia.gov/v2/electricity/electric-power-operational-data"
RENEWABLE_CODES = ["WND","SUN","GEO","BIO","HYC"]  # exclude pumped storage (HPS)

def _api_key():
    k = os.getenv("EIA_API_KEY") or os.getenv("EIA_KEY")
    if not k:
        raise RuntimeError("EIA_API_KEY not set. Add it to your .env")
    return k

def _fetch_generation(start_year:int, end_year:int, fuel_codes:List[str]) -> pd.DataFrame:
    url = f"{EIA_BASE}/data/"
    params = {
        "api_key": _api_key(),
        "frequency": "annual",
        "start": str(start_year),
        "end": str(end_year),
        "data[0]": "generation",
        "length": 5000,
    }
    for fc in fuel_codes:
        params.setdefault("facets[fueltypeid][]", [])
        params["facets[fueltypeid][]"].append(fc)
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    js = r.json()
    rows = js.get("data", [])
    if not rows:
        return pd.DataFrame(columns=["stateid","state_name","year","fueltypeid","generation"])
    df = pd.DataFrame(rows).rename(columns={"stateDescription":"state_name","period":"year"})
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["generation"] = pd.to_numeric(df["generation"], errors="coerce")
    return df[["stateid","state_name","year","fueltypeid","generation"]]

def fetch_renewables_share_by_state(start_year:int, end_year:int, exclude_dc:bool=True) -> pd.DataFrame:
    df_total = _fetch_generation(start_year, end_year, ["ALL"])
    df_ren   = _fetch_generation(start_year, end_year, RENEWABLE_CODES)
    if df_total.empty:
        raise RuntimeError("EIA returned no rows for total generation.")
    g_total = (df_total.groupby(["stateid","state_name","year"], as_index=False)["generation"]
               .sum().rename(columns={"generation":"gen_total"}))
    if df_ren.empty:
        g_ren = g_total[["stateid","state_name","year"]].copy(); g_ren["gen_ren"] = 0.0
    else:
        g_ren = (df_ren.groupby(["stateid","state_name","year"], as_index=False)["generation"]
                 .sum().rename(columns={"generation":"gen_ren"}))
    out = g_total.merge(g_ren, on=["stateid","state_name","year"], how="left")
    out["gen_ren"] = out["gen_ren"].fillna(0.0)
    out["value"] = (out["gen_ren"] / out["gen_total"].replace({0: pd.NA})) * 100.0
    if exclude_dc:
        out = out[out["stateid"]!="DC"]
    return out[["state_name","year","value"]].sort_values(["state_name","year"]).reset_index(drop=True)
