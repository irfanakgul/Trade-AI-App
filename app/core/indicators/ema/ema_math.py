from __future__ import annotations

from typing import Optional, List

import pandas as pd


def calculate_ema_cross(
    df: pd.DataFrame,
    price_col: str = "CLOSE",
    signal_lookback_days: int = 20,
) -> pd.DataFrame:
    """
    Legacy pandas EMA logic preserved exactly.

    Expected input columns:
    - SYMBOL
    - TIMESTAMP
    - CLOSE (or price_col)

    Notes:
    - EMA values are calculated on the FULL input dataframe passed in.
    - Therefore caller should provide enough history (warmup) so EMA20 matches legacy project.
    - Final output is filtered by DAYS_SINCE_CROSS between 0 and signal_lookback_days.
    """

    required_cols = {"SYMBOL", "TIMESTAMP", price_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for EMA calculation: {sorted(missing)}")

    if df.empty:
        return df.copy()

    work = df.copy()
    work["TIMESTAMP"] = pd.to_datetime(work["TIMESTAMP"], errors="coerce")
    work[price_col] = pd.to_numeric(work[price_col], errors="coerce")
    work = work.dropna(subset=["SYMBOL", "TIMESTAMP", price_col])

    if work.empty:
        return work.copy()

    result_frames: List[pd.DataFrame] = []

    for symbol, group in work.groupby("SYMBOL", sort=False):
        g = group.sort_values("TIMESTAMP").reset_index(drop=True).copy()
        g["SYMBOL"] = symbol

        # SAME legacy math
        g["EMA5"] = g[price_col].ewm(span=5, adjust=False).mean()
        g["EMA20"] = g[price_col].ewm(span=20, adjust=False).mean()
        g["EMA_Status"] = (g["EMA5"] > g["EMA20"]).astype(int)
        g["EMA_Cross"] = g["EMA_Status"].diff().fillna(0).astype(int)

        last_cross_pos: Optional[int] = None
        days_since = []

        for pos, row in g.iterrows():
            if row["EMA_Cross"] == 1:
                last_cross_pos = pos

            if row["EMA_Status"] == 0 or last_cross_pos is None:
                days_since.append(0)
            else:
                days_since.append(pos - last_cross_pos)

        g["DAYS_SINCE_CROSS"] = days_since
        result_frames.append(g)

    if not result_frames:
        return pd.DataFrame(columns=list(work.columns) + [
            "EMA5", "EMA20", "EMA_Status", "EMA_Cross", "DAYS_SINCE_CROSS"
        ])

    result = pd.concat(result_frames, ignore_index=True)

    # SAME final filter logic
    return result[result["DAYS_SINCE_CROSS"].between(0, signal_lookback_days)].reset_index(drop=True)