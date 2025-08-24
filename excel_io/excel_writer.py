import re
import pandas as pd

def _safe_sheet_name(name: str) -> str:
    """
    Excel limits: name <= 31 chars; cannot contain : \ / ? * [ ]
    """
    s = str(name)
    s = re.sub(r'[:\\/?*\[\]]', '-', s)  # replace illegal chars
    s = s.strip() or "Sheet"
    return s[:31]  # Excel hard limit

def write_metric_sheet(writer, sheet_name: str, wide_df: pd.DataFrame, title_cells: dict = None, notes: list = None):
    """
    Writes one worksheet:
      - header rows with responsibility + metric title
      - a wide matrix: State row + year columns
      - a line chart: Hawaii vs Other US States Average
    """
    # Make the sheet name Excel-safe
    sheet = _safe_sheet_name(sheet_name)

    years = list(wide_df.columns)
    ws = writer.book.add_worksheet(sheet)

    # Header rows
    responsibility = (title_cells or {}).get("responsibility", "")
    metric_title   = (title_cells or {}).get("metric", sheet)
    ws.write_row(0, 0, [responsibility] + [""] * (3 + len(years)))
    ws.write_row(1, 0, [metric_title]   + [""] * (3 + len(years)))

    # Column headers
    ws.write_row(2, 0, ["", "", "", "State"] + years)

    # Data rows
    row_start = 3
    for i, (state, row) in enumerate(wide_df.iterrows(), start=row_start):
        ws.write(i, 3, state)
        for j, y in enumerate(years, start=4):
            v = row.get(y)
            if pd.notna(v):
                try:
                    ws.write_number(i, j, float(v))
                except Exception:
                    ws.write(i, j, v)
            else:
                ws.write(i, j, None)

    # Notes footer
    if notes:
        r = row_start + len(wide_df) + 2
        for note in notes:
            ws.write(r, 0, f"Note: {note}")
            r += 1

    # Chart: Hawaii vs Other US States Average
    try:
        idx = list(wide_df.index)
        if "Hawaii" in idx and "Other US States Average" in idx and years:
            n = len(years)
            hi_row  = idx.index("Hawaii")
            avg_row = idx.index("Other US States Average")
            chart = writer.book.add_chart({"type": "line"})
            chart.add_series({
                "name": "Hawaii",
                "categories": [sheet, 2, 4, 2, 4 + n - 1],
                "values":     [sheet, row_start + hi_row, 4, row_start + hi_row, 4 + n - 1],
            })
            chart.add_series({
                "name": "Other US States Average",
                "categories": [sheet, 2, 4, 2, 4 + n - 1],
                "values":     [sheet, row_start + avg_row, 4, row_start + avg_row, 4 + n - 1],
            })
            chart.set_title({"name": metric_title[:31]})
            chart.set_legend({"position": "bottom"})
            ws.insert_chart(row_start, 1, chart)
    except Exception:
        # If chart creation fails, leave the data-only sheet
        pass
