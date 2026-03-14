from __future__ import annotations

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = ["EXCHANGE", "SYMBOL", "TIMESTAMP", "HIGH", "VOLUME"]


def clean_and_prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    data = df.copy()
    data["TIMESTAMP"] = pd.to_datetime(data["TIMESTAMP"], errors="coerce")
    data["HIGH"] = pd.to_numeric(data["HIGH"], errors="coerce")
    data["VOLUME"] = pd.to_numeric(data["VOLUME"], errors="coerce")

    data = data.dropna(subset=["EXCHANGE", "SYMBOL", "TIMESTAMP", "HIGH", "VOLUME"])
    data = data[data["VOLUME"] >= 0]
    data = data.sort_values(["SYMBOL", "TIMESTAMP"]).reset_index(drop=True)
    return data


def calculate_anchored_vwap_from_arrays(high: np.ndarray, volume: np.ndarray) -> float:
    pv = high * volume
    cumulative_pv = np.cumsum(pv)
    cumulative_volume = np.cumsum(volume)

    cumulative_volume = np.where(cumulative_volume == 0, np.nan, cumulative_volume)
    vwap_series = cumulative_pv / cumulative_volume
    return float(vwap_series[-1])


def _safe_tail_mean(series: pd.Series, n: int) -> float | None:
    if series.empty:
        return None
    tail = series.tail(n)
    if tail.empty:
        return None
    val = tail.mean()
    if pd.isna(val):
        return None
    return float(val)


def build_anchored_vwap_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input df is already filtered to the last N calendar months per symbol.
    VWAP math and filtering logic remain EXACTLY the same:
    - sort by TIMESTAMP
    - highest HIGH in filtered window
    - earliest TIMESTAMP if tie
    - anchored dataset = TIMESTAMP >= highest_timestamp
    - VWAP source = HIGH

    Added only:
    - AVG_VOLUME_10D
    - AVG_VOLUME_20D
    - AVG_VOLUME_30D
    These are computed from the symbol's filtered source window (not anchored slice),
    using the latest 10/20/30 trading rows.
    """
    data = clean_and_prepare_data(df)

    results = []

    for (exchange, symbol), symbol_df in data.groupby(["EXCHANGE", "SYMBOL"], sort=True):
        symbol_df = symbol_df.sort_values("TIMESTAMP").copy()
        if symbol_df.empty:
            continue

        # NEW: volume averages from latest trading rows in the filtered source window
        avg_volume_10d = _safe_tail_mean(symbol_df["VOLUME"], 10)
        avg_volume_20d = _safe_tail_mean(symbol_df["VOLUME"], 20)
        avg_volume_30d = _safe_tail_mean(symbol_df["VOLUME"], 30)

        # OLD LOGIC: highest HIGH in filtered window
        max_high = symbol_df["HIGH"].max()
        highest_rows = symbol_df[symbol_df["HIGH"] == max_high].sort_values("TIMESTAMP")
        if highest_rows.empty:
            continue

        highest_timestamp = highest_rows.iloc[0]["TIMESTAMP"]

        # OLD LOGIC: anchored dataset starts from highest timestamp
        anchored_df = symbol_df[symbol_df["TIMESTAMP"] >= highest_timestamp].copy()
        if anchored_df.empty:
            continue

        high = anchored_df["HIGH"].to_numpy(dtype=float)
        volume = anchored_df["VOLUME"].to_numpy(dtype=float)

        # OLD LOGIC: VWAP source = HIGH
        vwap_value = calculate_anchored_vwap_from_arrays(high, volume)

        results.append(
            {
                "EXCHANGE": exchange,
                "SYMBOL": symbol,
                "START_TIME": anchored_df["TIMESTAMP"].min().to_pydatetime(),
                "END_TIME": anchored_df["TIMESTAMP"].max().to_pydatetime(),
                "HIGHEST_VALUE": float(max_high),
                "HIGHEST_TIMESTAMP": pd.to_datetime(highest_timestamp).to_pydatetime(),
                "VWAP": round(vwap_value, 2),

                "AVG_VOLUME_10D": avg_volume_10d,
                "AVG_VOLUME_20D": avg_volume_20d,
                "AVG_VOLUME_30D": avg_volume_30d,
            }
        )

    result_df = pd.DataFrame(results)

    if not result_df.empty:
        result_df = result_df.sort_values(["EXCHANGE", "SYMBOL"]).reset_index(drop=True)

    return result_df