from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import AsyncIterator, List

import pandas as pd
from yahooquery import Ticker

from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class YahooQueryBistConfig:
    interval: str = "1m"
    chunk_days: int = 7


class YahooQueryBistProvider(MarketDataProvider):
    """
    Primary BIST provider using yahooquery.
    Note: Yahoo BIST symbols require '.IS' suffix.
    """

    def __init__(self, cfg: YahooQueryBistConfig = YahooQueryBistConfig()):
        self.cfg = cfg

    async def fetch_1min_aggs(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> AsyncIterator[List[AggBar]]:
        yahoo_symbol = f"{symbol}.IS"

        # YahooQuery returns timezone-aware timestamps; normalize to UTC.
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=timezone.utc)

        cur = start_dt
        while cur <= end_dt:
            chunk_end = min(cur + timedelta(days=self.cfg.chunk_days), end_dt)

            df = self._fetch_chunk(yahoo_symbol, cur, chunk_end)
            batch = self._df_to_aggbars(df)

            yield batch
            cur = chunk_end + timedelta(seconds=1)

    def _fetch_chunk(self, yahoo_symbol: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        t = Ticker(yahoo_symbol)

        # IMPORTANT:
        # YahooQuery's "end" date can behave like an exclusive boundary (or same-day edge case),
        # so we pass end_date + 1 day and then post-filter precisely by timestamps.
        start_s = start_dt.date().isoformat()
        end_s = (end_dt.date() + timedelta(days=1)).isoformat()

        df = t.history(
            interval=self.cfg.interval,
            start=start_s,
            end=end_s,
        )

        if df is None or len(df) == 0:
            return pd.DataFrame()

        df = df.reset_index()

        # --- Normalize datetime column robustly ---
        if "date" in df.columns:
            dt = pd.to_datetime(df["date"], errors="coerce")
        elif "datetime" in df.columns:
            dt = pd.to_datetime(df["datetime"], errors="coerce")
        else:
            dt = pd.to_datetime(df.iloc[:, 0], errors="coerce")

        # If timestamps are tz-naive, assume BIST exchange time then convert to UTC
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize(ZoneInfo("Europe/Istanbul"), ambiguous="infer", nonexistent="shift_forward")

        dt = dt.dt.tz_convert(timezone.utc)
        df["DATETIME"] = dt

        df = df.dropna(subset=["DATETIME"]).sort_values("DATETIME")

        # Strict filter inside chunk window (UTC)
        df = df[(df["DATETIME"] >= start_dt) & (df["DATETIME"] <= end_dt)]

        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        return df

    def _df_to_aggbars(self, df: pd.DataFrame) -> List[AggBar]:
        if df is None or df.empty:
            return []

        out: List[AggBar] = []
        for _, r in df.iterrows():
            dt = r["DATETIME"]
            if pd.isna(dt):
                continue
            ts_ms = int(dt.timestamp() * 1000)

            out.append(
                AggBar(
                    ts_ms=ts_ms,
                    open=float(r.get("open", 0.0) or 0.0),
                    high=float(r.get("high", 0.0) or 0.0),
                    low=float(r.get("low", 0.0) or 0.0),
                    close=float(r.get("close", 0.0) or 0.0),
                    volume=float(r.get("volume", 0.0) or 0.0),
                )
            )
        return out