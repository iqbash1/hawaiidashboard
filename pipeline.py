import os, json
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

# Silence macOS LibreSSL warning locally (no-op on CI)
try:
    import warnings, urllib3
    warnings.filterwarnings("ignore", category=urllib3.exceptions.NotOpenSSLWarning)
except Exception:
    pass

import yaml
import pandas as pd

from connectors.census_acs import fetch_broadband_adoption_by_state, fetch_uninsured_share_by_state
from connectors.eia import fetch_renewables_share_by_state
from connectors.cdc_wisqars import fetch_ypll75_rate_by_state
from utils import long_to_wide, compute_other_states_simple_average
from excel_io.excel_writer import write_metric_sheet


def write_site_json(out_dir: Path, metric_cfg: dict, years, hi_vals, other_vals):
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "metric_id": metric_cfg["id"],
        "title": metric_cfg.get("title", metric_cfg["id"]),
        "unit": metric_cfg.get("unit", ""),
        "years": years,
        "hawaii": [None if pd.isna(v) else float(v) for v in hi_vals],
        "other_states_avg": [None if pd.isna(v) else float(v) for v in other_vals],
        "notes": metric_cfg.get("annotations", []),
        "source": metric_cfg.get("source", {}),
        "last_updated_utc": datetime.now(timezone.utc).isoformat(),
    }
    (out_dir / f"{metric_cfg[id]}.json").write_text(json.dumps(payload, indent=2))


def write_site_csv(out_dir: Path, metric_cfg: dict, df_tidy: pd.DataFrame):
    """
    Write a CSV for the website: columns [state, year, value].
    Uses state full names (Hawaii, etc.) so it is human-friendly.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df_tidy[["state_name", "year", "value"]].rename(columns={"state_name": "state"})
    (out_dir / f"{metric_cfg[id]}.csv").write_text(df.to_csv(index=False))


def main():
    root = Path(__file__).resolve().parent
    cfg = yaml.safe_load((root / "config" / "metrics.yml").read_text())

    out_dir = root / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_xlsx = out_dir / "HI_dashboard_auto_demo.xlsx"
    writer = pd.ExcelWriter(out_xlsx, engine="xlsxwriter")

    site_json_dir = root / "site" / "data" / "v1"
    site_csv_dir  = root / "site" / "data" / "v1" / "csv"

    for m in cfg:
        mid = m["id"]
        start_year = m["years"]["start"]
        end_year = m["years"]["end"]
        responsibility = m.get("responsibility", "")
        title = m.get("title", mid)
        notes = m.get("annotations", [])

        # --- Fetch tidy (state, year, value) ---
        if mid == "broadband_adoption_households_share":
            df = fetch_broadband_adoption_by_state(start_year, end_year)
            df["state_name"] = df["NAME"]
            df = df[~df["state_name"].isin(["District of Columbia", "Puerto Rico"])]
            df = df.rename(columns={"broadband_adoption_share": "value"})

        elif mid == "electricity_renewables_generation_share":
            if not os.getenv("EIA_API_KEY"):
                print("EIA_API_KEY not set -> skipping renewables metric.")
                continue
            try:
                df = fetch_renewables_share_by_state(start_year, end_year, exclude_dc=True)
                df = df.rename(columns={"renewables_share_pct": "value"})
            except Exception as e:
                print(f"Renewables metric failed: {e} -> skipping.")
                continue

        elif mid == "public_health_uninsured_share":
            df = fetch_uninsured_share_by_state(start_year, end_year)
            df["state_name"] = df["NAME"]
            df = df[~df["state_name"].isin(["District of Columbia", "Puerto Rico"])]
            df = df.rename(columns={"uninsured_share": "value"})

        elif mid == "public_health_ypll75_rate":
            try:
                df = fetch_ypll75_rate_by_state(start_year, end_year)
            except Exception as e:
                print(f"YPLL metric failed: {e} -> skipping.")
                continue
            # df already has [state_name, year, value]

        else:
            continue

        # --- Wide matrix (states x years) ---
        wide = long_to_wide(df, state_col="state_name", year_col="year", value_col="value")

        # Comparator: simple average of other US states (exclude Hawaii + DC)
        exclude_states = ["Hawaii", "District of Columbia"]
        avg = compute_other_states_simple_average(wide, exclude_states=exclude_states)
        wide.loc["Other US States Average"] = avg

        if "Hawaii" not in wide.index:
            raise RuntimeError(f"Hawaii not found for metric {mid}")

        # --- Excel sheet ---
        sheet_name = {
            "broadband_adoption_households_share": "Infra-Broadband (auto)",
            "electricity_renewables_generation_share": "Env-Renewables (auto)",
            "public_health_uninsured_share": "Health-Uninsured (auto)",
            "public_health_ypll75_rate": "Health-YPLL<75 (auto)",
        }.get(mid, mid[:31])

        write_metric_sheet(writer, sheet_name, wide,
                           title_cells={"responsibility": responsibility, "metric": title},
                           notes=notes)

        # --- Curated CSV for archive ---
        (root / "data" / "curated").mkdir(parents=True, exist_ok=True)
        df[["state_name", "year", "value"]].rename(columns={"state_name": "state"}) \
          .to_csv(root / "data" / "curated" / f"{mid}.csv", index=False)

        # --- Website JSON (last 10 yrs) + CSV (full period) ---
        # JSON: last 10 years (for small payloads)
        all_years = [int(y) for y in list(wide.columns) if isinstance(y, (int, float))]
        all_years = sorted(all_years)
        years = all_years[-10:] if len(all_years) > 10 else all_years
        hi_vals = [wide.loc["Hawaii", y] if y in wide.columns else None for y in years]
        other_vals = [wide.loc["Other US States Average", y] if y in wide.columns else None for y in years]
        write_site_json(site_json_dir, m, years, hi_vals, other_vals)

        # CSV: full period, tidy
        write_site_csv(site_csv_dir, m, df)

    writer.close()
    print(f"Wrote {out_xlsx}")


if __name__ == "__main__":
    main()
