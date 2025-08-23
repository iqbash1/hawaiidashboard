import yaml
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
from pathlib import Path

from connectors.census_acs import fetch_broadband_adoption_by_state
from connectors.eia import fetch_renewables_share_by_state
from utils import long_to_wide, compute_other_states_simple_average
from io.excel_writer import write_metric_sheet

def main():
    root = Path(__file__).resolve().parent
    cfg = yaml.safe_load((root / "config" / "metrics.yml").read_text())

    out_dir = root / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_xlsx = out_dir / "HI_dashboard_auto_demo.xlsx"
    writer = pd.ExcelWriter(out_xlsx, engine="xlsxwriter")

    for m in cfg:
        mid = m["id"]
        start_year = m["years"]["start"]
        end_year = m["years"]["end"]
        responsibility = m.get("responsibility", "")
        title = m.get("title", mid)
        notes = m.get("annotations", [])

        if mid == "broadband_adoption_households_share":
            df = fetch_broadband_adoption_by_state(start_year, end_year)
            df["state_name"] = df["NAME"]
            # Drop DC and PR
            df = df[~df["state_name"].isin(["District of Columbia", "Puerto Rico"])]
            df = df.rename(columns={"broadband_adoption_share": "value"})

        elif mid == "electricity_renewables_generation_share":
            df = fetch_renewables_share_by_state(start_year, end_year, exclude_dc=True)
            df = df.rename(columns={"renewables_share_pct": "value"})

        else:
            # Unknown metric in this minimal starter; skip
            continue

        # wide matrix: states x years
        wide = long_to_wide(df, state_col="state_name", year_col="year", value_col="value")

        # Comparator: simple average of other US states (exclude Hawaii and DC)
        exclude_states = ["Hawaii", "District of Columbia"]
        avg = compute_other_states_simple_average(wide, exclude_states=exclude_states)
        wide.loc["Other US States Average"] = avg

        if "Hawaii" not in wide.index:
            raise RuntimeError(f"Hawaii not found for metric {mid}")

        # Sheet name mapping
        sheet_name = {
            "broadband_adoption_households_share": "Infrastructure- Broadband (auto)",
            "electricity_renewables_generation_share": "Environment-Renewables Share (auto)",
        }.get(mid, mid[:31])

        # Write worksheet + chart
        write_metric_sheet(
            writer,
            sheet_name,
            wide,
            title_cells={"responsibility": responsibility, "metric": title},
            notes=notes,
        )

        # Save tidy CSV for the website later
        (root / "data" / "curated").mkdir(parents=True, exist_ok=True)
        df[["state_name", "year", "value"]].rename(columns={"state_name": "state"}).to_csv(
            root / "data" / "curated" / f"{mid}.csv", index=False
        )

    writer.close()
    print(f"Wrote {out_xlsx}")

if __name__ == "__main__":
    main()

