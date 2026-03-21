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
    Legacy MFI logic preserved exactly.

    If HIGH / LOW columns do not exist:
    - HIGH = CLOSE * 1.01
    - LOW  = CLOSE * 0.99

    Output columns:
    - MFI
    - MFI_YESTERDAY
    - MFI_12DAY_AVG
    - MFI_DIRECTION

    Final filter:
    - keep only rows where MFI_DIRECTION == "Upward"
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

        # Use HIGH / LOW if present, otherwise derive from CLOSE
        h = g[high_col] if high_col in g.columns else g[price_col] * 1.01
        l = g[low_col] if low_col in g.columns else g[price_col] * 0.99

        # Step 1: Typical Price
        typical_price = (h + l + g[price_col]) / 3

        # Step 2: Raw Money Flow
        raw_money_flow = typical_price * g[volume_col]

        # Step 2b: Positive / Negative MF
        price_change = typical_price.diff()
        positive_mf = np.where(price_change > 0, raw_money_flow, 0)
        negative_mf = np.where(price_change < 0, raw_money_flow, 0)

        # Step 3: Rolling sums
        pos_sum = pd.Series(positive_mf).rolling(window=length, min_periods=length).sum()
        neg_sum = pd.Series(negative_mf).rolling(window=length, min_periods=length).sum()

        # Step 3b: Money Ratio
        mf_ratio = pos_sum / (neg_sum + 1e-10)

        # Step 4: MFI
        g["MFI"] = 100 - (100 / (1 + mf_ratio))
        g["MFI_YESTERDAY"] = g["MFI"].shift(1)

        # Last 12-day average excluding today and yesterday
        g["MFI_12DAY_AVG"] = g["MFI"].shift(2).rolling(window=12, min_periods=12).mean()

        # Direction
        g["MFI_DIRECTION"] = np.where(g["MFI"] > g["MFI_YESTERDAY"], "Upward", "Downward")

        result_frames.append(g)

    if not result_frames:
        return pd.DataFrame(columns=list(work.columns) + [
            "MFI", "MFI_YESTERDAY", "MFI_12DAY_AVG", "MFI_DIRECTION"
        ])

    result = pd.concat(result_frames, ignore_index=True).reset_index(drop=True)

    return result