import pandas as pd
from typing import Sequence

def long_to_wide(long_df: pd.DataFrame, state_col: str, year_col: str, value_col: str) -> pd.DataFrame:
    """
    Convert a tidy table (state, year, value) to a wide matrix with states as rows and years as columns.
    Columns (years) are sorted ascending; rows (states) are sorted alphabetically.
    """
    w = long_df.pivot_table(index=state_col, columns=year_col, values=value_col, aggfunc="mean")
    w = w.reindex(sorted(w.columns), axis=1)  # sort year columns
    w = w.sort_index()                        # sort states
    return w

def compute_other_states_simple_average(wide: pd.DataFrame, exclude_states: Sequence[str]) -> pd.Series:
    """
    Equal-weight average across states, excluding the ones provided (e.g., ['Hawaii', 'District of Columbia']).
    Returns a 1D series indexed by year (the wide columns).
    """
    mask = ~wide.index.isin(exclude_states)
    return wide.loc[mask].mean(axis=0, skipna=True)

