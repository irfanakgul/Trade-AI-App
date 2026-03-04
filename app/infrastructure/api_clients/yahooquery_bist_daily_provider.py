from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from yahooquery import Ticker


@dataclass(frozen=True)
class YahooQueryBistDailyConfig:
    interval: str = "1d"              # daily
    exchange_suffix: str = ".IS"      # Yahoo uses .IS for BIST


class YahooQueryBistDailyProvider:
    """
    Daily provider for BIST via yahooquery.

    Contract:
      fetch_daily_df(symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame

    Returns a DataFrame with at least:
      TS (timezone-normalized naive UTC), OPEN,HIGH,LOW,CLOSE,VOLUME
    """
    def __init__(self, cfg: YahooQueryBistDailyConfig = YahooQueryBistDailyConfig()):
        self.cfg = cfg

    def fetch_daily_df(self, symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame:
        yahoo_symbol = f"{symbol}{self.cfg.exchange_suffix}"
        t = Ticker(yahoo_symbol)

        # YahooQuery "end" can behave like exclusive; include end date by adding 1 day.
        start_s = start_dt.isoformat()
        end_s = (end_dt + timedelta(days=1)).isoformat()

        df = t.history(
            interval=self.cfg.interval,
            start=start_s,
            end=end_s,
        )

        if df is None:
            return pd.DataFrame()

        # yahooquery can return multiindex (symbol, date/datetime)
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
        else:
            df = df.reset_index()

        # Normalize column names
        df.columns = [c.upper().strip() for c in df.columns]

        # Common variants: DATE / DATETIME column after reset_index
        ts_col = None
        for cand in ("DATE", "DATETIME", "TIMESTAMP"):
            if cand in df.columns:
                ts_col = cand
                break
        if ts_col is None:
            return pd.DataFrame()

        # Make TS: parse -> force UTC -> drop tz (naive)
        ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        df["TS"] = ts.dt.tz_convert("UTC").dt.tz_localize(None)

        # Make sure OHLCV exist (yahooquery uses lowercase sometimes, but we uppercased)
        need = ["OPEN", "HIGH", "LOW", "CLOSE"]
        for c in need:
            if c not in df.columns:
                raise ValueError(f"Yahoo daily DF missing column: {c}")
        if "VOLUME" not in df.columns:
            df["VOLUME"] = 0.0

        # Keep only needed fields for the service conversion step
        return df[["TS", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]].dropna(subset=["TS"])