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
    n_bars: int = 200000  # can be tuned


class TvDatafeedBistProvider(MarketDataProvider):
    """
    Fallback BIST provider using tvDatafeed. No '.IS' suffix needed.
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
            exchange="BIST",
            interval=Interval.in_1_minute,
            n_bars=self.cfg.n_bars,
        )

        if df is None or len(df) == 0:
            yield []
            return

        df = df.reset_index()
        # tvDatafeed uses 'datetime' index; normalize
        if "datetime" in df.columns:
            df["DATETIME"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
        elif "DATETIME" in df.columns:
            df["DATETIME"] = pd.to_datetime(df["DATETIME"], utc=True, errors="coerce")
        else:
            df["DATETIME"] = pd.to_datetime(df.iloc[:, 0], utc=True, errors="coerce")

        df = df.dropna(subset=["DATETIME"]).sort_values("DATETIME")

        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        df = df[(df["DATETIME"] >= start_dt) & (df["DATETIME"] <= end_dt)]

        # Normalize columns
        df.columns = [c.upper() for c in df.columns]
        for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        batch: List[AggBar] = []
        for _, r in df.iterrows():
            dt = r["DATETIME"]
            if pd.isna(dt):
                continue
            ts_ms = int(dt.timestamp() * 1000)
            batch.append(
                AggBar(
                    ts_ms=ts_ms,
                    open=float(r.get("OPEN", 0.0) or 0.0),
                    high=float(r.get("HIGH", 0.0) or 0.0),
                    low=float(r.get("LOW", 0.0) or 0.0),
                    close=float(r.get("CLOSE", 0.0) or 0.0),
                    volume=float(r.get("VOLUME", 0.0) or 0.0),
                )
            )

        yield batch