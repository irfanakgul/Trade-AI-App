from __future__ import annotations

from typing import List

import pandas as pd


def calculate_camarilla_pivots(
    df: pd.DataFrame,
    high_col: str = "HIGH",
    low_col: str = "LOW",
    close_col: str = "CLOSE",
) -> pd.DataFrame:
    """
    Legacy Camarilla Pivot logic preserved.

    Expected columns:
    - EXCHANGE
    - SYMBOL
    - TIMESTAMP
    - HIGH
    - LOW
    - CLOSE

    Logic:
    - Input dataframe is assumed to be pre-filtered (e.g. last N days).
    - For each symbol:
        * current_year = max year in filtered data
        * prev_year = current_year - 1
        * pivots are calculated from prev_year OHLC
        * only current_year rows receive pivot values
    - Returns all current_year rows for symbols where prev_year exists.
    """

    required_cols = {"EXCHANGE", "SYMBOL", "TIMESTAMP", high_col, low_col, close_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for pivot calculation: {sorted(missing)}")

    if df.empty:
        return df.copy()

    work = df.copy()
    work["TIMESTAMP"] = pd.to_datetime(work["TIMESTAMP"], errors="coerce")
    work[high_col] = pd.to_numeric(work[high_col], errors="coerce")
    work[low_col] = pd.to_numeric(work[low_col], errors="coerce")
    work[close_col] = pd.to_numeric(work[close_col], errors="coerce")

    work = work.dropna(subset=["EXCHANGE", "SYMBOL", "TIMESTAMP", high_col, low_col, close_col])
    if work.empty:
        return work.copy()

    result_frames: List[pd.DataFrame] = []

    for symbol, group in work.groupby("SYMBOL", sort=False):
        g = group.sort_values("TIMESTAMP").reset_index(drop=True).copy()
        g["SYMBOL"] = symbol

        g["YEAR"] = pd.to_datetime(g["TIMESTAMP"]).dt.year
        current_year = int(g["YEAR"].max())
        prev_year = current_year - 1

        prev = g[g["YEAR"] == prev_year].copy()
        if prev.empty:
            continue

        prev_high = prev[high_col].max()
        prev_low = prev[low_col].min()
        prev_close = prev.iloc[-1][close_col]

        hl = prev_high - prev_low

        pivot = (prev_high + prev_low + prev_close) / 3
        r1 = prev_close + 1.1 * hl / 12
        r2 = prev_close + 1.1 * hl / 6
        r3 = prev_close + 1.1 * hl / 4
        r4 = prev_close + 1.1 * hl / 2
        r5 = (prev_high / prev_low) * prev_close

        s1 = prev_close - 1.1 * hl / 12
        s2 = prev_close - 1.1 * hl / 6
        s3 = prev_close - 1.1 * hl / 4
        s4 = prev_close - 1.1 * hl / 2
        s5 = prev_close - (r5 - prev_close)

        current = g[g["YEAR"] == current_year].copy()
        if current.empty:
            continue

        current["PVT_YEAR"] = current_year
        current["PIVOT"] = pivot

        current["PVT_R1"] = r1
        current["PVT_R2"] = r2
        current["PVT_R3"] = r3
        current["PVT_R4"] = r4
        current["PVT_R5"] = r5

        current["PVT_S1"] = s1
        current["PVT_S2"] = s2
        current["PVT_S3"] = s3
        current["PVT_S4"] = s4
        current["PVT_S5"] = s5

        result_frames.append(current)

    if not result_frames:
        return pd.DataFrame(columns=list(work.columns) + [
            "PVT_YEAR", "PIVOT",
            "PVT_R1", "PVT_R2", "PVT_R3", "PVT_R4", "PVT_R5",
            "PVT_S1", "PVT_S2", "PVT_S3", "PVT_S4", "PVT_S5",
        ])

    return pd.concat(result_frames, ignore_index=True)