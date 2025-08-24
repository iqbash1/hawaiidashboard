import os
import requests
import pandas as pd
from typing import Dict

CENSUS_API_KEY = os.getenv('CENSUS_API_KEY', '')
CENSUS_BASE = 'https://api.census.gov/data'

def fetch_broadband_adoption_by_state(start_year: int, end_year: int) -> pd.DataFrame:
    frames = []
    for year in range(start_year, end_year + 1):
        if year == 2020:  # standard ACS 1-year was not published
            continue
        url = f"{CENSUS_BASE}/{year}/acs/acs1"
        params = {'get': 'NAME,B28002_001E,B28002_004E', 'for': 'state:*'}
        if CENSUS_API_KEY:
            params['key'] = CENSUS_API_KEY
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        cols, *rows = resp.json()
        df = pd.DataFrame(rows, columns=cols)
        df['year'] = year
        for c in ['B28002_001E','B28002_004E','state']:
            df[c] = pd.to_numeric(df[c], errors='coerce')
        df['broadband_adoption_share'] = (df['B28002_004E'] / df['B28002_001E']) * 100.0
        frames.append(df[['state','NAME','year','broadband_adoption_share']])
    out = pd.concat(frames, ignore_index=True)
    out['state_abbr'] = out['state'].map(lambda s: _state_fips_to_abbr().get(int(s), None))
    return out

def fetch_uninsured_share_by_state(start_year: int, end_year: int) -> pd.DataFrame:
    """
    Returns columns: state (FIPS), NAME (state name), year, uninsured_share (%)
    Pulls ACS 1-year SUBJECT dataset S2701_C05_001 (Percent uninsured).
    Skips 2020 standard release.
    """
    frames = []
    for year in range(start_year, end_year + 1):
        if year == 2020:
            continue
        base = f"{CENSUS_BASE}/{year}/acs/acs1/subject"
        # Try variable without 'E' first, then with 'E' (var names differ across years)
        vars_to_try = ["S2701_C05_001", "S2701_C05_001E"]
        got = None
        for var in vars_to_try:
            params = {'get': f'NAME,{var}', 'for': 'state:*'}
            if CENSUS_API_KEY:
                params['key'] = CENSUS_API_KEY
            r = requests.get(base, params=params, timeout=60)
            if r.status_code == 200:
                cols, *rows = r.json()
                df = pd.DataFrame(rows, columns=cols)
                if var in df.columns:
                    df['year'] = year
                    df['uninsured_share'] = pd.to_numeric(df[var], errors='coerce')
                    frames.append(df[['state','NAME','year','uninsured_share']])
                    got = True
                    break
        if not got:
            raise RuntimeError(f"ACS S2701 variable not found for year {year}")
    return pd.concat(frames, ignore_index=True)

def _state_fips_to_abbr() -> Dict[int, str]:
    return {1:'AL',2:'AK',4:'AZ',5:'AR',6:'CA',8:'CO',9:'CT',10:'DE',12:'FL',13:'GA',15:'HI',
            16:'ID',17:'IL',18:'IN',19:'IA',20:'KS',21:'KY',22:'LA',23:'ME',24:'MD',25:'MA',
            26:'MI',27:'MN',28:'MS',29:'MO',30:'MT',31:'NE',32:'NV',33:'NH',34:'NJ',35:'NM',
            36:'NY',37:'NC',38:'ND',39:'OH',40:'OK',41:'OR',42:'PA',44:'RI',45:'SC',46:'SD',
            47:'TN',48:'TX',49:'UT',50:'VT',51:'VA',53:'WA',54:'WV',55:'WI',56:'WY',11:'DC',72:'PR'}
