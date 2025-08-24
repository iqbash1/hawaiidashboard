import pandas as pd

def write_metric_sheet(writer, sheet_name: str, wide_df: pd.DataFrame, title_cells: dict = None, notes: list = None):
    """
    Writes one worksheet:
      - header rows with responsibility + metric title
      - a wide matrix: State row + year columns
      - a line chart: Hawaii vs Other US States Average
    """
    years = list(wide_df.columns)

    ws = writer.book.add_worksheet(sheet_name)

    # Header rows
    header1 = [title_cells.get("responsibility", "") if title_cells else ""] + [""] * (3 + len(years))
    header2 = [title_cells.get("metric", "") if title_cells else ""] + [""] * (3 + len(years))
    ws.write_row(0, 0, header1)
    ws.write_row(1, 0, header2)

    # Column headers
    ws.write_row(2, 0, ["", "", "", "State"] + years)

    # Data rows
    row_start = 3
    for i, (state, row) in enumerate(wide_df.iterrows(), start=row_start):
        ws.write(i, 3, state)
        for j, y in enumerate(years, start=4):
            val = row[y]
            if pd.notna(val):
                try:
                    ws.write_number(i, j, float(val))
                except Exception:
                    ws.write(i, j, val)
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
        index_list = list(wide_df.index)
        if "Hawaii" in index_list and "Other US States Average" in index_list:
            n_years = len(years)
            hi_row = index_list.index("Hawaii")
            avg_row = index_list.index("Other US States Average")

            chart = writer.book.add_chart({"type": "line"})
            chart.add_series({
                "name": "Hawaii",
                "categories": [sheet_name, 2, 4, 2, 4 + n_years - 1],
                "values": [sheet_name, row_start + hi_row, 4, row_start + hi_row, 4 + n_years - 1],
            })
            chart.add_series({
                "name": "Other US States Average",
                "categories": [sheet_name, 2, 4, 2, 4 + n_years - 1],
                "values": [sheet_name, row_start + avg_row, 4, row_start + avg_row, 4 + n_years - 1],
            })
            chart.set_title({"name": (title_cells or {}).get("metric", sheet_name)})
            chart.set_legend({"position": "bottom"})
            ws.insert_chart(row_start, 1, chart)
    except Exception:
        # If chart creation fails for any reason, leave the data-only sheet.
        pass

