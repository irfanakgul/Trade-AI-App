from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import AsyncGenerator, List, Optional

import pandas as pd
from tvDatafeed import TvDatafeed, Interval  # Interval.in_daily is supported  [oai_citation:1‡GitHub](https://github.com/rongardF/tvdatafeed)

from app.infrastructure.api_clients.market_data_provider import AggBar, MarketDataProvider


@dataclass(frozen=True)
class TvDatafeedBistDailyConfig:
    username: Optional[str] = None
    password: Optional[str] = None
    interval: Interval = Interval.in_daily
    interval_tag: str = "daily"
    source: str = "tvDatafeed"
    n_bars: int = 5000


class TvDatafeedBistDailyProvider(MarketDataProvider):
    def __init__(self, cfg: TvDatafeedBistDailyConfig):
        self.cfg = cfg
        self._tv = TvDatafeed(cfg.username, cfg.password) if cfg.username and cfg.password else TvDatafeed()

    async def fetch_1min_aggs(self, symbol: str, start_dt, end_dt) -> AsyncGenerator[List[AggBar], None]:
        df = self._tv.get_hist(
            symbol=symbol,
            exchange="BIST",
            interval=self.cfg.interval,
            n_bars=self.cfg.n_bars,
        )
        if df is None or len(df) == 0:
            yield []
            return

        df = df.reset_index().rename(columns={"datetime": "DATETIME"})
        df.columns = [c.upper() for c in df.columns]
        df["DATETIME"] = pd.to_datetime(df["DATETIME"], errors="coerce")
        df = df.dropna(subset=["DATETIME"]).sort_values("DATETIME")

        # Filter by [start_dt, end_dt]
        df = df[(df["DATETIME"].dt.date >= start_dt) & (df["DATETIME"].dt.date <= end_dt)]

        out: List[AggBar] = []
        for _, r in df.iterrows():
            dt = r["DATETIME"].to_pydatetime()
            dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
            ts_ms = int(dt.timestamp() * 1000)

            out.append(
                AggBar(
                    ts_ms=ts_ms,
                    open=float(r.get("OPEN", 0.0)) if pd.notna(r.get("OPEN")) else None,
                    high=float(r.get("HIGH", 0.0)) if pd.notna(r.get("HIGH")) else None,
                    low=float(r.get("LOW", 0.0)) if pd.notna(r.get("LOW")) else None,
                    close=float(r.get("CLOSE", 0.0)) if pd.notna(r.get("CLOSE")) else None,
                    volume=float(r.get("VOLUME", 0.0)) if pd.notna(r.get("VOLUME")) else None,
                )
            )

        yield out