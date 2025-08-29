import io, csv, time, requests, pandas as pd
from pathlib import Path

CANDIDATE_URLS = [
  "https://wisqars.cdc.gov/data-export/ypll_75/state",
  "https://wisqars.cdc.gov/data-export/ypll/state?ypll_age=75&geography=state",
  "https://wisqars.cdc.gov/data-export/ypll_75/state?format=csv"
]
FALLBACK = Path("data/raw/wisqars_ypll75_state.csv")
STATE_NAMES = {
 "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia",
 "Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts",
 "Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
 "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
 "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia",
 "Wisconsin","Wyoming","District of Columbia"
}
def _try_remote():
    hdr={"User-Agent":"Mozilla/5.0","Accept":"text/csv,application/octet-stream,application/json"}
    for u in CANDIDATE_URLS:
        try:
            r = requests.get(u, headers=hdr, timeout=60)
            if r.status_code==200 and r.content:
                return r.content.decode("utf-8","replace")
        except Exception:
            time.sleep(0.8)
            continue
    return None
def _try_local():
    return FALLBACK.read_text("utf-8","replace") if FALLBACK.exists() else None
def _pick_rate_col(headers):
    keys=[h.strip() for h in headers]; kl=[k.lower() for k in keys]
    prefs=["age-adjusted ypll rate","age adjusted ypll rate","ypll rate age-adjusted","age-adjusted rate","age adjusted rate","ypll rate","rate"]
    for p in prefs:
        for i,h in enumerate(kl):
            if p in h and "per" in h and "100" in h:
                return keys[i]
    for i,h in enumerate(kl):
        if "rate" in h: return keys[i]
    return None
def ypll75_rate_per_100k():
    text = _try_remote()
    if text is None:
        text = _try_local()
    if text is None:
        return pd.DataFrame(columns=["state","year","value"])
    f=io.StringIO(text)
    rdr=csv.DictReader(f)
    hdr=rdr.fieldnames or []
    state_col=next((h for h in hdr if h.lower() in ("state","location","geography")), None)
    year_col =next((h for h in hdr if h.lower() in ("year","data year","data_year","yearcode")), None)
    rate_col=_pick_rate_col(hdr)
    if not (state_col and year_col and rate_col):
        return pd.DataFrame(columns=["state","year","value"])
    rows=[]
    for r in rdr:
        st=(r.get(state_col) or "").strip()
        if st not in STATE_NAMES or st=="District of Columbia": continue
        yraw=(r.get(year_col) or "").strip()
        y=None
        for tok in yraw.replace("("," ").replace(")"," ").replace("*"," ").split():
            if tok.isdigit() and len(tok)==4:
                y=int(tok); break
        if y is None: continue
        vraw=(r.get(rate_col) or "").strip()
        if vraw in ("","--","**","--*"):
            v=None
        else:
            try: v=float(vraw.replace(",",""))
            except: v=None
        rows.append((st,y,v))
    import pandas as pd
    df=pd.DataFrame(rows, columns=["state","year","value"])
    if df.empty: return df
    years=sorted(df["year"].unique())
    years10=years[-10:] if len(years)>10 else years
    return df[df["year"].isin(years10)].sort_values(["state","year"]).reset_index(drop=True)
