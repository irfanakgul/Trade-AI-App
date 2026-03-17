from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math

import pandas as pd
from yahooquery import Ticker


@dataclass(frozen=True)
class YahooQueryHourlyConfig:
    source_name: str = "yahooquery"


class YahooQueryHourlyProvider:
    """
    Generic hourly provider for YahooQuery.
    No timezone conversion is applied.
    """

    def __init__(self, cfg: YahooQueryHourlyConfig = YahooQueryHourlyConfig()):
        self.cfg = cfg

    def fetch(
        self,
        exchange: str,
        symbol: str,
        safe_start_dt: datetime,
    ) -> pd.DataFrame:
        now_dt = datetime.now()
        diff_days = max(1, math.ceil((now_dt - safe_start_dt).total_seconds() / 86400))
        period_days = diff_days + 1

        yahoo_symbol = self._map_symbol(exchange, symbol)

        ticker = Ticker(yahoo_symbol)
        df = ticker.history(
            period=f"{period_days}d",
            interval="1h",
        )

        if df is None or len(df) == 0:
            return pd.DataFrame()

        df = df.reset_index()
        df.columns = [c.upper() for c in df.columns]

        if "DATE" in df.columns:
            df["TIMESTAMP"] = pd.to_datetime(df["DATE"], errors="coerce")
        elif "DATETIME" in df.columns:
            df["TIMESTAMP"] = pd.to_datetime(df["DATETIME"], errors="coerce")
        else:
            raise ValueError(f"YahooQuery returned no DATE/DATETIME column for {exchange}:{symbol}")

        df = df.dropna(subset=["TIMESTAMP"]).copy()

        df["TS"] = df["TIMESTAMP"]

        ts_series = df["TS"]
        safe_start_cmp = pd.Timestamp(safe_start_dt)

        # Make comparison-compatible without converting provider timestamps
        if getattr(ts_series.dt, "tz", None) is not None and safe_start_cmp.tzinfo is None:
            safe_start_cmp = safe_start_cmp.tz_localize(ts_series.dt.tz)

        df = df[df["TS"] >= safe_start_cmp].copy()

        if df.empty:
            return pd.DataFrame()

        df["EXCHANGE"] = exchange
        df["SYMBOL"] = symbol
        df["SOURCE"] = self.cfg.source_name

        for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = self._add_row_id(df)

        needed = [
            "EXCHANGE",
            "SYMBOL",
            "TIMESTAMP",
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "VOLUME",
            "SOURCE",
            "ROW_ID",
            "TS",
        ]
        return df[needed].copy()

    def _map_symbol(self, exchange: str, symbol: str) -> str:
        ex = exchange.upper().strip()

        if ex == "BIST":
            return f"{symbol}.IS"
        if ex == "EURONEXT":
            return f"{symbol}.AS"

        return symbol

    def _add_row_id(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()

        out["TIMESTAMP"] = pd.to_datetime(out["TIMESTAMP"], errors="coerce")

        date_part = out["TIMESTAMP"].dt.strftime("%Y%m%d")
        time_part = out["TIMESTAMP"].dt.strftime("%H%M")

        out["ROW_ID"] = (
            "id_"
            + out["EXCHANGE"].astype(str).str.lower()
            + "_"
            + out["SYMBOL"].astype(str).str.lower()
            + "_"
            + date_part
            + "_"
            + time_part
            + "_1hour"
        ).str.lower()

        return out