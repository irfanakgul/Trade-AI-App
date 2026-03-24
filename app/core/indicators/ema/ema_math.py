from __future__ import annotations

from typing import Optional, List

import pandas as pd


def calculate_ema_cross(
    df: pd.DataFrame,
    price_col: str = "CLOSE",
    signal_lookback_days: int = 20,
) -> pd.DataFrame:
    """
    Legacy EMA logic preserved exactly.

    Expected input columns:
    - SYMBOL
    - TIMESTAMP
    - CLOSE (or price_col)

    Output columns:
    - EMA3, EMA5, EMA14, EMA20
    - EMA_Status_5_20, EMA_Cross_5_20
    - EMA_Status_3_20, EMA_Cross_3_20
    - EMA_Status_3_14, EMA_Cross_3_14
    - DAYS_SINCE_CROSS_5_20
    - DAYS_SINCE_CROSS_3_20
    - DAYS_SINCE_CROSS_3_14

    Final filter:
    - keep rows where all three DAYS_SINCE_CROSS_* values are <= signal_lookback_days
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

        # EMA calculations
        g["EMA3"] = g[price_col].ewm(span=3, adjust=False).mean()
        g["EMA5"] = g[price_col].ewm(span=5, adjust=False).mean()
        g["EMA14"] = g[price_col].ewm(span=14, adjust=False).mean()
        g["EMA20"] = g[price_col].ewm(span=20, adjust=False).mean()

        # 5 / 20
        g["EMA_Status_5_20"] = (g["EMA5"] > g["EMA20"]).astype(int)
        g["EMA_Cross_5_20"] = g["EMA_Status_5_20"].diff().fillna(0).astype(int)

        # 3 / 20
        g["EMA_Status_3_20"] = (g["EMA3"] > g["EMA20"]).astype(int)
        g["EMA_Cross_3_20"] = g["EMA_Status_3_20"].diff().fillna(0).astype(int)

        # 3 / 14
        g["EMA_Status_3_14"] = (g["EMA3"] > g["EMA14"]).astype(int)
        g["EMA_Cross_3_14"] = g["EMA_Status_3_14"].diff().fillna(0).astype(int)

        # DAYS_SINCE_CROSS per pair
        for suffix, status_col, cross_col in [
            ("5_20", "EMA_Status_5_20", "EMA_Cross_5_20"),
            ("3_20", "EMA_Status_3_20", "EMA_Cross_3_20"),
            ("3_14", "EMA_Status_3_14", "EMA_Cross_3_14"),
        ]:
            last_cross_pos: Optional[int] = None
            days_since = []

            for pos, row in g.iterrows():
                if row[cross_col] == 1:
                    last_cross_pos = pos

                if row[status_col] == 0 or last_cross_pos is None:
                    days_since.append(0)
                else:
                    days_since.append(pos - last_cross_pos)

            g[f"DAYS_SINCE_CROSS_{suffix}"] = days_since

        result_frames.append(g)

    if not result_frames:
        return pd.DataFrame(columns=list(work.columns) + [
            "EMA3", "EMA5", "EMA14", "EMA20",
            "EMA_Status_5_20", "EMA_Cross_5_20",
            "EMA_Status_3_20", "EMA_Cross_3_20",
            "EMA_Status_3_14", "EMA_Cross_3_14",
            "DAYS_SINCE_CROSS_5_20", "DAYS_SINCE_CROSS_3_20", "DAYS_SINCE_CROSS_3_14",
        ])

    result = pd.concat(result_frames, ignore_index=True)

    mask = (
        (result["DAYS_SINCE_CROSS_5_20"] <= signal_lookback_days) &
        (result["DAYS_SINCE_CROSS_3_20"] <= signal_lookback_days) &
        (result["DAYS_SINCE_CROSS_3_14"] <= signal_lookback_days)
    )

    return result[mask].reset_index(drop=True)