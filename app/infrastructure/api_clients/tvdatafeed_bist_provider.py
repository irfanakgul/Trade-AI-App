from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, date
from typing import AsyncIterator, List

import pandas as pd
from tvDatafeed import TvDatafeed, Interval

from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class TvDatafeedBistConfig:
    username: str
    password: str
    n_bars: int = 500


class TvDatafeedBistProvider(MarketDataProvider):
    """
    BIST provider using tvDatafeed. No '.IS' suffix needed.
    """

    def __init__(self, cfg: TvDatafeedBistConfig):
        self.cfg = cfg
        self._tv = TvDatafeed(username=cfg.username, password=cfg.password)

    async def fetch_1min_aggs(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[List[AggBar]]:
        df = self._tv.get_hist(
            symbol=symbol,
            exchange="",
            interval=Interval.in_1_minute,
            n_bars=self.cfg.n_bars,
        )

        if df is None or df.empty:
            yield []
            return

        df = df.reset_index().copy()

        # normalize datetime FIRST and keep only one DATETIME column
        if "datetime" in df.columns:
            dt_series = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        elif "DATETIME" in df.columns:
            dt_series = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce")
        else:
            dt_series = pd.to_datetime(df.iloc[:, 0], utc=True, errors="coerce")

        # drop possible duplicate datetime-like columns before assigning normalized one
        cols_to_drop = [c for c in df.columns if c.lower() == "datetime"]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop, errors="ignore")

        df["DATETIME"] = dt_series

        df = df.dropna(subset=["DATETIME"]).sort_values("DATETIME")


        if df.empty:
            yield []
            return

        df.columns = [str(c).upper() for c in df.columns]

        for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        batch: List[AggBar] = []

        for row in df.itertuples(index=False):
            dt = getattr(row, "DATETIME", None)
            if dt is None or pd.isna(dt):
                continue

            ts_ms = int(pd.Timestamp(dt).timestamp() * 1000)

            open_v = getattr(row, "OPEN", 0.0)
            high_v = getattr(row, "HIGH", 0.0)
            low_v = getattr(row, "LOW", 0.0)
            close_v = getattr(row, "CLOSE", 0.0)
            volume_v = getattr(row, "VOLUME", 0.0)

            batch.append(
                AggBar(
                    ts_ms=ts_ms,
                    open=float(0.0 if pd.isna(open_v) else open_v),
                    high=float(0.0 if pd.isna(high_v) else high_v),
                    low=float(0.0 if pd.isna(low_v) else low_v),
                    close=float(0.0 if pd.isna(close_v) else close_v),
                    volume=float(0.0 if pd.isna(volume_v) else volume_v),
                )
            )

        yield batch