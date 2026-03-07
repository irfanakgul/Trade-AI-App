from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class BistDailyIngestionConfig:
    job_name: str = "bist_daily_historical_ingestion"

    target_schema: str = "raw"
    target_table: str = "bist_daily_archive"

    last_ts_schema: str = "raw"
    last_ts_table: str = "bist_daily_archive"
    last_ts_column: str = "TS"  # typed timestamp column

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    interval_tag: str = "daily"
    source: str = "yahooquery"

    max_concurrent_symbols: int = 6
    symbol_level_retries: int = 2
    retry_backoff_s: float = 3.0


class BistDailyHistoricalIngestionService:
    """
    Provider contract (daily):
      - provider.fetch_daily_df(symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame
        Expected columns (case-insensitive): OPEN,HIGH,LOW,CLOSE,VOLUME and a datetime column:
          - TS (preferred) or TIMESTAMP or DATETIME or DATE
    """

    def __init__(self, repo: PostgresRepository, provider: Any, config: BistDailyIngestionConfig = BistDailyIngestionConfig()):
        self.repo = repo
        self.provider = provider
        self.cfg = config
        self._sem = asyncio.Semaphore(self.cfg.max_concurrent_symbols)
        self.permanently_failed_symbols: List[str] = []

    async def run(self, use_db_last_timestamp: bool, start_date: str, end_date: Optional[str] = None) -> None:
        symbols = self.repo.get_in_scope_symbols(exchange="BIST", schema="silver")
        if not symbols:
            print("[BIST-DAILY] No in-scope BIST symbols found.")
            return

        self.permanently_failed_symbols = []

        total = len(symbols)
        default_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()

        remaining: List[Tuple[int, str]] = [(i + 1, s) for i, s in enumerate(symbols)]
        round_idx = 0

        while True:
            round_idx += 1
            failed: List[Tuple[int, str]] = []

            tasks = [
                asyncio.create_task(
                    self._process_symbol(
                        idx=idx,
                        total=total,
                        symbol=symbol,
                        use_db_last_timestamp=use_db_last_timestamp,
                        default_start=default_start,
                        end_dt=end_dt,
                        failed_out=failed,
                    )
                )
                for (idx, symbol) in remaining
            ]
            await asyncio.gather(*tasks)

            if not failed:
                print(f"[BIST-DAILY] All symbols completed. rounds={round_idx}")
                break

            print(f"[BIST-DAILY] Round {round_idx} finished. failed_symbols={len(failed)}")

            if round_idx > (1 + self.cfg.symbol_level_retries):
                self.permanently_failed_symbols = [s for _, s in failed]
                print(f"[BIST-DAILY] Exhausted retries. permanently_failed={len(self.permanently_failed_symbols)}")
                break

            remaining = failed
            await asyncio.sleep(self.cfg.retry_backoff_s * round_idx)

    async def _process_symbol(
        self,
        idx: int,
        total: int,
        symbol: str,
        use_db_last_timestamp: bool,
        default_start: date,
        end_dt: date,
        failed_out: List[Tuple[int, str]],
    ) -> None:
        async with self._sem:
            attempted_rows = 0
            inserted_rows = 0

            try:
                start_dt = default_start
                if use_db_last_timestamp:
                    last_ts = self.repo.get_last_timestamp(
                        symbol=symbol,
                        schema=self.cfg.last_ts_schema,
                        table=self.cfg.last_ts_table,
                        ts_column=self.cfg.last_ts_column,
                    )
                    if last_ts:
                        start_dt = last_ts.date()

                df: pd.DataFrame = await asyncio.to_thread(self.provider.fetch_daily_df, symbol, start_dt, end_dt)
                if df is None or df.empty:
                    print(f"[BIST-DAILY {idx}/{total}] {symbol}: done. attempted_rows=0 inserted_rows=0 start={start_dt} end={end_dt}")
                    return

                rows = self._df_to_rows(symbol, df)
                attempted_rows = len(rows)

                inserted_rows = self.repo.bulk_insert_on_conflict_do_nothing(
                    schema=self.cfg.target_schema,
                    table=self.cfg.target_table,
                    rows=rows,
                    conflict_column="ROW_ID",
                )

                print(
                    f"[BIST-DAILY {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows} "
                    f"start={start_dt} end={end_dt}"
                )

            except Exception as e:
                failed_out.append((idx, symbol))
                print(f"[BIST-DAILY {idx}/{total}] {symbol}: failed with error: {repr(e)}")
                try:
                    self.repo.log_ingestion_error(
                        schema=self.cfg.error_schema,
                        table=self.cfg.error_table,
                        job_name=self.cfg.job_name,
                        symbol=symbol,
                        exchange="BIST",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                except Exception:
                    pass

    def _df_to_rows(self, symbol: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        d = df.copy()
        d.columns = [c.upper().strip() for c in d.columns]

        ts_col = None
        for cand in ("TS", "TIMESTAMP", "DATETIME", "DATE"):
            if cand in d.columns:
                ts_col = cand
                break
        if ts_col is None:
            raise ValueError("Daily DF has no TS/TIMESTAMP/DATETIME/DATE column")

        ts = pd.to_datetime(d[ts_col], utc=True, errors="coerce")
        if ts.isna().any():
            raise ValueError("Daily DF contains invalid timestamps after parsing (NaT)")

        ts_naive = ts.dt.tz_convert("UTC").dt.tz_localize(None)
        d["TS"] = ts_naive
        d["TIMESTAMP"] = ts_naive.dt.strftime("%Y-%m-%d %H:%M:%S")

        for need in ("OPEN", "HIGH", "LOW", "CLOSE"):
            if need not in d.columns:
                raise ValueError(f"Daily DF missing column: {need}")
        if "VOLUME" not in d.columns:
            d["VOLUME"] = 0.0

        d["SYMBOL"] = symbol
        d["SOURCE"] = self.cfg.source
        d["ROW_ID"] = d["TS"].dt.strftime(f"ID_{symbol}_%Y%m%d_0000_{self.cfg.interval_tag}")

        out_cols = ["SYMBOL", "TIMESTAMP", "TS", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SOURCE", "ROW_ID"]
        return d[out_cols].to_dict("records")