from __future__ import annotations

from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta


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


def period_to_delta(period: str) -> relativedelta:
    p = period.strip().lower().replace(" ", "")
    if "year" in p:
        n = int("".join([c for c in p if c.isdigit()]) or "1")
        return relativedelta(years=n)
    if "month" in p:
        n = int("".join([c for c in p if c.isdigit()]) or "1")
        return relativedelta(months=n)
    if "week" in p:
        n = int("".join([c for c in p if c.isdigit()]) or "1")
        return relativedelta(weeks=n)
    if "day" in p:
        n = int("".join([c for c in p if c.isdigit()]) or "1")
        return relativedelta(days=n)
    raise ValueError(f"Unsupported period: {period}")


def calculate_anchored_vwap_from_arrays(high: np.ndarray, volume: np.ndarray) -> float:
    pv = high * volume
    cumulative_pv = np.cumsum(pv)
    cumulative_volume = np.cumsum(volume)
    cumulative_volume = np.where(cumulative_volume == 0, np.nan, cumulative_volume)
    vwap_series = cumulative_pv / cumulative_volume
    return float(vwap_series[-1])


def _daily_volume_summary(df: pd.DataFrame) -> pd.DataFrame:
    # daily total volume
    d = df.copy()
    d["TRADE_DATE"] = d["TIMESTAMP"].dt.date
    out = d.groupby("TRADE_DATE", as_index=False)["VOLUME"].sum()
    out = out.sort_values("TRADE_DATE").reset_index(drop=True)
    return out


def _avg_or_none(values: np.ndarray, n: int) -> Optional[float]:
    if len(values) < n:
        return None
    return float(np.mean(values[-n:]))


def _get_avg_volume_fields(period_df: pd.DataFrame, highest_timestamp: pd.Timestamp) -> Dict[str, Any]:
    daily = _daily_volume_summary(period_df)
    daily_vols = daily["VOLUME"].to_numpy(dtype=float)

    avg_5d = _avg_or_none(daily_vols, 5)
    avg_10d = _avg_or_none(daily_vols, 10)
    avg_20d = _avg_or_none(daily_vols, 20)

    lastday = float(daily_vols[-1]) if len(daily_vols) >= 1 else None
    yesterday = float(daily_vols[-2]) if len(daily_vols) >= 2 else None

    # highest in last 20 trade day?
    last_20_dates = set(daily["TRADE_DATE"].tail(20).tolist())
    highest_in_last_20 = highest_timestamp.date() in last_20_dates

    if highest_in_last_20:
        status = "failed AVG VOL | highest in last 20 day"
    else:
        if (
            lastday is not None
            and yesterday is not None
            and avg_5d is not None
            and avg_10d is not None
            and avg_20d is not None
            and (lastday > yesterday > avg_5d > avg_10d > avg_20d)
        ):
            status = "positive"
        else:
            status = "negative"

    return {
        "AVG_VOLUME_5D": avg_5d,
        "AVG_VOLUME_10D": avg_10d,
        "AVG_VOLUME_20D": avg_20d,
        "AVG_VOLUME_YESTERDAY": yesterday,
        "AVG_VOLUME_LASTDAY": lastday,
        "AVG_VOL_STATUS": status,
    }


def build_anchored_vwap_summary(df: pd.DataFrame, periods: List[str]) -> pd.DataFrame:
    data = clean_and_prepare_data(df)
    results: List[Dict[str, Any]] = []

    for (exchange, symbol), symbol_df in data.groupby(["EXCHANGE", "SYMBOL"], sort=True):
        symbol_df = symbol_df.sort_values("TIMESTAMP").copy()
        if symbol_df.empty:
            continue

        end_ts = symbol_df["TIMESTAMP"].max()

        for period in periods:
            delta = period_to_delta(period)
            raw_cutoff = end_ts - delta

            # important rule:
            # if cutoff becomes 2025-02-03 13:00, include from that day's first bar
            cutoff_day = raw_cutoff.date()
            period_df = symbol_df[symbol_df["TIMESTAMP"].dt.date >= cutoff_day].copy()

            if period_df.empty:
                continue

            start_time = period_df["TIMESTAMP"].min()
            end_time = period_df["TIMESTAMP"].max()

            max_high = period_df["HIGH"].max()
            highest_rows = period_df[period_df["HIGH"] == max_high].sort_values("TIMESTAMP")
            if highest_rows.empty:
                continue

            highest_timestamp = highest_rows.iloc[0]["TIMESTAMP"]

            anchored_df = period_df[period_df["TIMESTAMP"] >= highest_timestamp].copy()
            if anchored_df.empty:
                continue

            high = anchored_df["HIGH"].to_numpy(dtype=float)
            volume = anchored_df["VOLUME"].to_numpy(dtype=float)

            vwap_value = calculate_anchored_vwap_from_arrays(high, volume)
            avg_fields = _get_avg_volume_fields(period_df, pd.Timestamp(highest_timestamp))

            results.append(
                {
                    "EXCHANGE": exchange,
                    "SYMBOL": symbol,
                    "VWAP_PERIOD": period,
                    "START_TIME": pd.Timestamp(start_time).to_pydatetime(),
                    "END_TIME": pd.Timestamp(end_time).to_pydatetime(),
                    "HIGHEST_VALUE": float(max_high),
                    "HIGHEST_TIMESTAMP": pd.Timestamp(highest_timestamp).to_pydatetime(),
                    "VWAP": round(vwap_value, 2),
                    **avg_fields,
                }
            )

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values(["EXCHANGE", "SYMBOL", "VWAP_PERIOD"]).reset_index(drop=True)
    return result_df