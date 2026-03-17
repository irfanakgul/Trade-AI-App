from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
import requests
import pandas as pd


@dataclass(frozen=True)
class BinanceHourlyConfig:
    source_name: str = "binance_api"
    timeout_s: int = 30
    base_url: str = "https://api.binance.com/api/v3/klines"


class BinanceHourlyProvider:
    """
    Generic BINANCE hourly provider.
    No timezone conversion is applied beyond pandas timestamp parsing from open_time ms.
    """

    def __init__(self, cfg: BinanceHourlyConfig = BinanceHourlyConfig()):
        self.cfg = cfg

    def fetch(
        self,
        exchange: str,
        symbol: str,
        safe_start_dt: datetime,
    ) -> pd.DataFrame:
        if exchange.upper() != "BINANCE":
            raise ValueError(f"BinanceHourlyProvider only supports BINANCE exchange, got {exchange}")

        request_symbol = f"{symbol.upper()}USDT"

        now_dt = datetime.now()
        diff_hours = max(1, math.ceil((now_dt - safe_start_dt).total_seconds() / 3600))
        limit = min(diff_hours + 24, 1000)  # Binance max limit protection

        params = {
            "symbol": request_symbol,
            "interval": "1h",
            "limit": limit,
        }

        resp = requests.get(self.cfg.base_url, params=params, timeout=self.cfg.timeout_s)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list) or len(data) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(
            data,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "num_trades",
                "taker_buy_base_vol",
                "taker_buy_quote_vol",
                "ignore",
            ],
        )

        df = df[["open_time", "open", "high", "low", "close", "volume"]].copy()

        df["TIMESTAMP"] = pd.to_datetime(df["open_time"], unit="ms", errors="coerce")
        df = df.dropna(subset=["TIMESTAMP"]).copy()

        # Keep provider time as-is
        df["TS"] = df["TIMESTAMP"]

        df = df[df["TS"] >= safe_start_dt].copy()
        if df.empty:
            return pd.DataFrame()

        df["EXCHANGE"] = "BINANCE"
        df["SYMBOL"] = symbol.upper()
        df["SOURCE"] = self.cfg.source_name

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.rename(
            columns={
                "open": "OPEN",
                "high": "HIGH",
                "low": "LOW",
                "close": "CLOSE",
                "volume": "VOLUME",
            }
        )

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