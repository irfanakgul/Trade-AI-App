from __future__ import annotations

from typing import List, Optional

import pandas as pd
import numpy as np


def calculate_rsi_features(
    df: pd.DataFrame,
    rsi_length: int = 14,
    ma_length: int = 14,
    price_col: str = "CLOSE",
    signal_lookback_days: int = 20,
) -> pd.DataFrame:
    """
    Legacy pandas RSI logic preserved exactly.

    Expected input columns:
    - SYMBOL
    - TIMESTAMP
    - CLOSE (or given price_col)

    Logic preserved:
    - delta = price.diff()
    - gain = delta.clip(lower=0)
    - loss = -delta.clip(upper=0)
    - avg_gain = gain.ewm(alpha=1/rsi_length, adjust=False).mean()
    - avg_loss = loss.ewm(alpha=1/rsi_length, adjust=False).mean()
    - rs = avg_gain / avg_loss
    - RSI = 100 - (100 / (1 + rs))
    - RSI_MA = RSI.rolling(window=ma_length, min_periods=ma_length).mean()
    - RSI_Status = (RSI > RSI_MA).astype(int)
    - RSI_Cross = RSI_Status.diff().fillna(0).astype(int)
    - RSI_Cross_Days_Ago follows the exact loop logic
    - Final filter: between(1, signal_lookback_days)
    """
    required_cols = {"SYMBOL", "TIMESTAMP", price_col}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for RSI calculation: {sorted(missing)}")

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

        delta = g[price_col].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(alpha=1 / rsi_length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / rsi_length, adjust=False).mean()

        rs = avg_gain / avg_loss
        g["RSI"] = 100 - (100 / (1 + rs))
        g["RSI_MA"] = g["RSI"].rolling(window=ma_length, min_periods=ma_length).mean()

        g["RSI_Status"] = (g["RSI"] > g["RSI_MA"]).astype(int)
        g["RSI_Cross"] = g["RSI_Status"].diff().fillna(0).astype(int)

        last_cross_pos: Optional[int] = None
        days_since = []

        for pos, row in g.iterrows():
            if row["RSI_Cross"] == 1:
                last_cross_pos = pos

            if row["RSI_Status"] == 0 or last_cross_pos is None:
                days_since.append(0)
            else:
                days_since.append(pos - last_cross_pos)

        g["RSI_Cross_Days_Ago"] = days_since
        result_frames.append(g)

    if not result_frames:
        return pd.DataFrame(columns=list(work.columns) + [
            "RSI", "RSI_MA", "RSI_Status", "RSI_Cross", "RSI_Cross_Days_Ago"
        ])

    result = pd.concat(result_frames, ignore_index=True)

    return result[result["RSI_Cross_Days_Ago"].between(1, signal_lookback_days)].reset_index(drop=True)