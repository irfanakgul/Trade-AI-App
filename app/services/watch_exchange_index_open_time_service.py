from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

import certifi
import pandas as pd

from app.infrastructure.database.repository import PostgresRepository


# Pipeline exchange -> index candidates
# Each entry: (display_label, [(symbol, exchange), ...] tried in order until one works)
INDEX_CANDIDATES: dict[str, tuple[str, list[tuple[str, str]]]] = {
    "BIST": ("BIST 100", [("XU100", "BIST"), ("XU100", "TVC")]),
    "EURONEXT": ("AEX", [("AEX", "EURONEXT"), ("AEX", "TVC")]),
    "NASDAQ": ("NASDAQ", [("IXIC", "NASDAQ"), ("IXIC", "TVC"), ("NDX", "NASDAQ")]),
    "NYSE": ("NYSE", [("NYA", "NYSE"), ("NYA", "TVC"), ("DJI", "DJ")]),
}


@dataclass(frozen=True)
class WatchExchangeIndexOpenTimeConfig:
    job_name: str = "watch_exchange_index_open_time"
    tv_n_bars: int = 1000


class WatchExchangeIndexOpenTimeService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: WatchExchangeIndexOpenTimeConfig = WatchExchangeIndexOpenTimeConfig(),
    ):
        self.repo = repo
        self.cfg = cfg

    def run(
        self,
        exchange: str,
        index_schema: str,
        index_table: str,
        output_schema: str,
        output_table: str,
        log_schema: str,
        log_table: str,
        open_hour: int,
        open_minute: int,
        mid_close_hour: int,
        mid_close_minute: int,
        is_truncate_exchange: bool = True,
    ) -> Dict[str, Any]:
        exchange = exchange.upper().strip()

        if exchange not in INDEX_CANDIDATES:
            raise ValueError(
                f"No index candidate configured for exchange={exchange}. "
                f"Available={list(INDEX_CANDIDATES.keys())}"
            )

        label, candidates = INDEX_CANDIDATES[exchange]

        if is_truncate_exchange:
            deleted_index = self.repo.delete_indexes_by_exchange(
                schema=index_schema,
                table=index_table,
                exchange=exchange,
            )
            deleted_output = self.repo.delete_exchange_index_on_open_time_by_exchange(
                schema=output_schema,
                table=output_table,
                exchange=exchange,
            )
        else:
            deleted_index = 0
            deleted_output = 0

        index_rows = self._fetch_index_rows(
            exchange=exchange,
            label=label,
            candidates=candidates,
        )

        if not index_rows:
            print(f"[INDEX-OPEN] {exchange}: no index data")
            return {
                "exchange": exchange,
                "deleted_index_rows": deleted_index,
                "deleted_output_rows": deleted_output,
                "index_rows": 0,
                "output_rows": 0,
                "log_rows": 0,
                "trend_status": None,
            }

        inserted_index = self.repo.insert_index_rows(
            schema=index_schema,
            table=index_table,
            rows=index_rows,
        )

        result_row = self.repo.build_exchange_index_open_time_row(
            exchange=exchange,
            index_schema=index_schema,
            index_table=index_table,
            open_hour=int(open_hour),
            open_minute=int(open_minute),
            mid_close_hour=int(mid_close_hour),
            mid_close_minute=int(mid_close_minute),
        )

        if not result_row:
            print(f"[INDEX-OPEN] {exchange}: no open/mid-close row")
            return {
                "exchange": exchange,
                "deleted_index_rows": deleted_index,
                "deleted_output_rows": deleted_output,
                "index_rows": inserted_index,
                "output_rows": 0,
                "log_rows": 0,
                "trend_status": None,
            }

        inserted_output = self.repo.insert_exchange_index_open_time_rows(
            schema=output_schema,
            table=output_table,
            rows=[result_row],
            on_conflict_do_nothing=False,
        )

        inserted_log = self.repo.insert_exchange_index_open_time_rows(
            schema=log_schema,
            table=log_table,
            rows=[result_row],
            on_conflict_do_nothing=True,
        )

        print(
            f"[INDEX-OPEN] {exchange} | "
            f"OPEN_INDEX={result_row['OPEN_INDEX']} | "
            f"MID_CLOSE_INDEX={result_row['MID_CLOSE_INDEX']} | "
            f"TREND={result_row['TREND_STATUS']}"
        )

        return {
            "exchange": exchange,
            "deleted_index_rows": deleted_index,
            "deleted_output_rows": deleted_output,
            "index_rows": inserted_index,
            "output_rows": inserted_output,
            "log_rows": inserted_log,
            "trend_status": result_row["TREND_STATUS"],
        }

    def _fetch_index_rows(
        self,
        exchange: str,
        label: str,
        candidates: list[tuple[str, str]],
    ) -> List[Dict[str, Any]]:
        from tvDatafeed import Interval, TvDatafeed

        logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)
        os.environ["SSL_CERT_FILE"] = certifi.where()
        os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

        tv = TvDatafeed()

        last_reason = "no data"

        for symbol, source_exchange in candidates:
            try:
                df = tv.get_hist(
                    symbol=symbol,
                    exchange=source_exchange,
                    interval=Interval.in_1_minute,
                    n_bars=self.cfg.tv_n_bars,
                )
            except Exception as e:
                last_reason = f"{source_exchange}:{symbol} -> {e}"
                continue

            if df is None or df.empty:
                last_reason = f"{source_exchange}:{symbol} -> empty dataframe"
                continue

            df = df.reset_index()
            df.columns = [str(c).upper() for c in df.columns]

            if "DATETIME" in df.columns:
                ts_col = "DATETIME"
            elif "TIMESTAMP" in df.columns:
                ts_col = "TIMESTAMP"
            else:
                ts_col = df.columns[0]

            created_at = datetime.now()
            out: List[Dict[str, Any]] = []

            for _, r in df.iterrows():
                out.append(
                    {
                        "EXCHANGE": exchange,
                        "INDEX_LABEL": label,
                        "INDEX_SYMBOL": symbol,
                        "SOURCE_EXCHANGE": source_exchange,
                        "TIMESTAMP": pd.to_datetime(r[ts_col]).to_pydatetime(),
                        "OPEN": float(r["OPEN"]) if "OPEN" in df.columns and pd.notna(r.get("OPEN")) else None,
                        "HIGH": float(r["HIGH"]) if "HIGH" in df.columns and pd.notna(r.get("HIGH")) else None,
                        "LOW": float(r["LOW"]) if "LOW" in df.columns and pd.notna(r.get("LOW")) else None,
                        "CLOSE": float(r["CLOSE"]) if "CLOSE" in df.columns and pd.notna(r.get("CLOSE")) else None,
                        "VOLUME": float(r["VOLUME"]) if "VOLUME" in df.columns and pd.notna(r.get("VOLUME")) else None,
                        "CREATED_AT": created_at,
                    }
                )

            if out:
                return out

        print(f"[INDEX-OPEN] {exchange}: failed. reason={last_reason}")
        return []