from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math

import pandas as pd
from tvDatafeed import TvDatafeed, Interval


@dataclass(frozen=True)
class TvDatafeedHourlyConfig:
    username: str
    password: str
    source_name: str = "tvDatafeed"


class TvDatafeedHourlyProvider:
    """
    Generic hourly provider for exchanges supported by tvDatafeed.
    No timezone conversion is applied. Timestamps stay as returned by the provider.
    """

    def __init__(self, cfg: TvDatafeedHourlyConfig):
        self.cfg = cfg
        self.tv = TvDatafeed(username=cfg.username, password=cfg.password)

    def fetch(
        self,
        exchange: str,
        symbol: str,
        safe_start_dt: datetime,
    ) -> pd.DataFrame:
        """
        Fetch hourly data using n_bars computed from safe_start_dt until now.
        """

        now_dt = datetime.now()
        diff_hours = max(1, math.ceil((now_dt - safe_start_dt).total_seconds() / 3600))
        n_bars = diff_hours + 24  # extra safety window

        request_symbol = symbol
        if exchange.upper() == "BINANCE":
            request_symbol = f"{symbol.upper()}USDT"

        df = self.tv.get_hist(
            symbol=request_symbol,
            exchange=exchange,
            interval=Interval.in_1_hour,
            n_bars=n_bars,
        )

        if df is None or len(df) == 0:
            return pd.DataFrame()

        df = df.reset_index()
        df.columns = [c.upper() for c in df.columns]

        if "DATETIME" not in df.columns:
            raise ValueError(f"tvDatafeed returned no DATETIME column for {exchange}:{symbol}")

        df["TIMESTAMP"] = pd.to_datetime(df["DATETIME"], errors="coerce")
        df = df.dropna(subset=["TIMESTAMP"]).copy()

        # Keep provider time exactly as returned
        df["TS"] = df["TIMESTAMP"]

        # Filter by safe start
        df = df[df["TS"] >= safe_start_dt].copy()

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