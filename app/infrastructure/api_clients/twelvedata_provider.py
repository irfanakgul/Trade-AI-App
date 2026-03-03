# app/infrastructure/api_clients/twelvedata_provider.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import AsyncIterator, List, Optional

import pandas as pd
from twelvedata import TDClient

from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class TwelveDataConfig:
    api_key: str
    interval: str = "1min"
    timezone: str = "UTC"
    outputsize: int = 5000  # provider limit


class TwelveDataProvider(MarketDataProvider):
    """
    Fallback provider (TwelveData). Intended for short lookback ranges (e.g., last 7 days),
    because outputsize and historical depth can be plan-limited.
    """

    def __init__(self, cfg: TwelveDataConfig):
        self.cfg = cfg
        self._client = TDClient(apikey=cfg.api_key)

    async def fetch_1min_aggs(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[List[AggBar]]:
        # TwelveData SDK is sync; we keep interface async but run sync call inside.
        # For now, we yield a single batch (sufficient for "last week" fallback use case).

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=7)

        ts = self._client.time_series(
            symbol=symbol,
            interval=self.cfg.interval,
            outputsize=self.cfg.outputsize,
            timezone=self.cfg.timezone,
        ).as_pandas()

        if ts is None or len(ts) == 0:
            yield []
            return

        # Index is datetime-like (string); normalize to UTC
        ts.index = pd.to_datetime(ts.index, utc=True)
        ts = ts.sort_index()

        ts = ts[(ts.index >= start) & (ts.index <= end)]
        if ts.empty:
            yield []
            return

        # TwelveData returns strings; normalize numeric columns
        # Common column names: open, high, low, close, volume
        for c in ["open", "high", "low", "close", "volume"]:
            if c in ts.columns:
                ts[c] = pd.to_numeric(ts[c], errors="coerce")

        batch: List[AggBar] = []
        for dt, row in ts.iterrows():
            # dt is timezone-aware UTC; convert to epoch ms
            ts_ms = int(dt.timestamp() * 1000)

            batch.append(
                AggBar(
                    ts_ms=ts_ms,
                    open=float(row.get("open", 0.0) or 0.0),
                    high=float(row.get("high", 0.0) or 0.0),
                    low=float(row.get("low", 0.0) or 0.0),
                    close=float(row.get("close", 0.0) or 0.0),
                    volume=float(row.get("volume", 0.0) or 0.0),
                )
            )

        yield batch