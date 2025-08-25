import io, re, zipfile
import requests
import pandas as pd
from typing import Optional

WISQARS_YPLL_STATE = "https://wisqars.cdc.gov/data-export/ypll_75/state"

def _norm(s: str) -> str:
    s = re.sub(r"[^\w]+", "_", str(s).strip().lower())
    return re.sub(r"_+", "_", s).strip("_")

def _pick(df: pd.DataFrame, candidates) -> Optional[str]:
    cols = {_norm(c): c for c in df.columns}
    for want in candidates:
        if want in cols:
            return cols[want]
    return None

def _read_wisqars_csv_bytes(b: bytes) -> pd.DataFrame:
    # ZIP or plain CSV
    if len(b) >= 2 and b[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(b)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                raise RuntimeError("WISQARS ZIP had no CSV")
            with zf.open(csv_names[0]) as f:
                return pd.read_csv(f)
    return pd.read_csv(io.BytesIO(b))

def fetch_ypll75_rate_by_state(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Returns tidy: [state_name, year, value] where value = YPLL under 75 per 100,000.
    Excludes DC and PR; averages duplicate breakdown rows if present.
    """
    r = requests.get(
        WISQARS_YPLL_STATE,
        headers={"Accept": "text/csv, application/zip, */*"},
        timeout=90,
        allow_redirects=True
    )
    r.raise_for_status()

    try:
        df = _read_wisqars_csv_bytes(r.content)
    except Exception as e:
        # some servers might return JSON; try once
        try:
            js = r.json()
            df = pd.DataFrame(js)
        except Exception:
            raise RuntimeError(f"Could not parse WISQARS export: {e}")

    state_col = _pick(df, ["state", "location", "state_territory", "state_name", "jurisdiction"])
    year_col  = _pick(df, ["year", "data_year", "year_code", "year_start"])
    value_col = _pick(df, [
        "ypll_rate", "ypll_rate_per_100000", "years_of_potential_life_lost_rate",
        "years_of_potential_life_lost_ypll_rate", "ypll_under_age_75_rate"
    ])
    if not (state_col and year_col and value_col):
        raise RuntimeError("WISQARS columns not found in export")

    out = df[[state_col, year_col, value_col]].rename(columns={
        state_col: "state_name", year_col: "year", value_col: "value"
    })
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["year"]).astype({"year": int})

    out = out[(out["year"] >= start_year) & (out["year"] <= end_year)]
    out = out[~out["state_name"].isin(["District of Columbia", "Puerto Rico"])]

    # If multiple rows per state/year (e.g., by race/ethnicity), average them
    out = (out.groupby(["state_name", "year"], as_index=False)["value"]
             .mean(numeric_only=True)
             .sort_values(["state_name", "year"]))
    return out
