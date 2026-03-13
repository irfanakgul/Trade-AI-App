from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd


def calculate_mfi(
    df: pd.DataFrame,
    length: int = 14,
    price_col: str = "CLOSE",
    volume_col: str = "VOLUME",
    high_col: str = "HIGH",
    low_col: str = "LOW",
) -> pd.DataFrame:
    """
    Legacy pandas MFI logic preserved as closely as possible.

    Expected input columns:
    - SYMBOL
    - TIMESTAMP
    - CLOSE
    - VOLUME
    - optionally HIGH / LOW

    If HIGH / LOW are missing:
    - HIGH = CLOSE * 1.01
    - LOW  = CLOSE * 0.99

    Output columns added:
    - MFI
    - MF_TODAY
    - MF_YESTERDAY
    - MF_12DAY_AVG
    - MF_DIRECTION

    IMPORTANT:
    - Unlike the old screening-style code, this version does NOT filter only Upward rows.
    - It returns all rows so the latest feature can always be stored.
    """
    required_cols = {"SYMBOL", "TIMESTAMP", price_col, volume_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for MFI calculation: {sorted(missing)}")

    if df.empty:
        return df.copy()

    work = df.copy()
    work["TIMESTAMP"] = pd.to_datetime(work["TIMESTAMP"], errors="coerce")
    work[price_col] = pd.to_numeric(work[price_col], errors="coerce")
    work[volume_col] = pd.to_numeric(work[volume_col], errors="coerce")

    if high_col in work.columns:
        work[high_col] = pd.to_numeric(work[high_col], errors="coerce")
    if low_col in work.columns:
        work[low_col] = pd.to_numeric(work[low_col], errors="coerce")

    work = work.dropna(subset=["SYMBOL", "TIMESTAMP", price_col, volume_col])

    if work.empty:
        return work.copy()

    result_frames: List[pd.DataFrame] = []

    for symbol, group in work.groupby("SYMBOL", sort=False):
        g = group.sort_values("TIMESTAMP").reset_index(drop=True).copy()
        g["SYMBOL"] = symbol

        # If HIGH / LOW missing, derive from CLOSE
        h = g[high_col] if high_col in g.columns else g[price_col] * 1.01
        l = g[low_col] if low_col in g.columns else g[price_col] * 0.99

        typical_price = (h + l + g[price_col]) / 3.0
        raw_money_flow = typical_price * g[volume_col]

        price_change = typical_price.diff()
        positive_mf = raw_money_flow.where(price_change > 0, 0)
        negative_mf = raw_money_flow.where(price_change < 0, 0)

        pos_sum = positive_mf.rolling(window=length, min_periods=length).sum()
        neg_sum = negative_mf.rolling(window=length, min_periods=length).sum()

        g["MFI"] = 100 * pos_sum / (pos_sum + neg_sum + 1e-10)

        g["MF_TODAY"] = raw_money_flow
        g["MF_YESTERDAY"] = raw_money_flow.shift(1)
        g["MF_12DAY_AVG"] = (
            raw_money_flow.rolling(window=length, min_periods=length).sum()
            - g["MF_TODAY"]
            - g["MF_YESTERDAY"]
        ) / 12

        g["MF_DIRECTION"] = np.where(
            g["MF_TODAY"] > g["MF_YESTERDAY"],
            "Upward",
            "Downward"
        )

        result_frames.append(g)

    if not result_frames:
        return pd.DataFrame(columns=list(work.columns) + [
            "MFI", "MF_TODAY", "MF_YESTERDAY", "MF_12DAY_AVG", "MF_DIRECTION"
        ])

    return pd.concat(result_frames, ignore_index=True).reset_index(drop=True)