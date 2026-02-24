# app/services/usa_historical_ingestion_service.py

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, Any, List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class UsaIngestionConfig:
    target_schema: str = "bronze"
    target_table: str = "usa_1min_high_filtered"
    last_ts_schema: str = "bronze"
    last_ts_table: str = "usa_1min_high_filtered"
    last_ts_column: str = "TIMESTAMP"
    interval_tag: str = "1min"
    source: str = "polygon"
    max_concurrent_symbols: int = 8
    symbol_level_retries: int = 2          # number of extra rounds after the first
    retry_backoff_s: float = 3.0
    error_schema: str = "logs"
    error_table: str = "ingestion_errors"
    job_name: str = "usa_historical_ingestion"


class UsaHistoricalIngestionService:
    def __init__(
        self,
        repo: PostgresRepository,
        provider: MarketDataProvider,
        config: UsaIngestionConfig = UsaIngestionConfig(),
    ):
        self.repo = repo
        self.provider = provider
        self.cfg = config
        self._sem = asyncio.Semaphore(self.cfg.max_concurrent_symbols)
        self.permanently_failed_symbols: List[str] = []

    async def run(
        self,
        use_db_last_timestamp: bool,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> None:
        symbols = self.repo.get_in_scope_symbols(exchange="USA", schema="silver")
        if not symbols:
            print("No in-scope USA symbols found.")
            return

        # Reset permanently failed list on each run
        self.permanently_failed_symbols = []

        total = len(symbols)
        start_dt_default = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()

        remaining = [(i + 1, s) for i, s in enumerate(symbols)]
        round_idx = 0

        while True:
            round_idx += 1
            failed: List[tuple[int, str]] = []

            tasks = [
                asyncio.create_task(
                    self._process_symbol(
                        idx=idx,
                        total=total,
                        symbol=s,
                        use_db_last_timestamp=use_db_last_timestamp,
                        default_start=start_dt_default,
                        end_dt=end_dt,
                        failed_out=failed,
                    )
                )
                for (idx, s) in remaining
            ]
            await asyncio.gather(*tasks)

            if not failed:
                print(f"[USA] All symbols completed. rounds={round_idx}")
                break

            print(f"[USA] Round {round_idx} finished. failed_symbols={len(failed)}")

            # Retry exhausted
            if round_idx > (1 + self.cfg.symbol_level_retries):
                print(f"[USA] Exhausted retries. permanently_failed={len(failed)}")

                # Store permanently failed symbols for fallback usage
                self.permanently_failed_symbols = [s for (_, s) in failed]

                for idx, s in failed:
                    self._log_permanent_failure(
                        symbol=s,
                        error_type="SymbolRetryExhausted",
                        error_message="Symbol failed after retry rounds.",
                    )

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
        failed_out: List[tuple[int, str]],
        attempted_rows = 0,
        inserted_rows = 0
    ) -> None:
        async with self._sem:
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

                total_attempted = 0
                async for batch in self.provider.fetch_1min_aggs(symbol, start_dt, end_dt):
                    rows = self._to_rows(symbol, batch)
                    total_attempted += self.repo.bulk_insert_on_conflict_do_nothing(
                        schema=self.cfg.target_schema,
                        table=self.cfg.target_table,
                        rows=rows,
                        conflict_column="ROW_ID",
                    )

                print(
                    f"[USA {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows} "
                    f"start={start_dt} end={end_dt}"
                )

            except Exception as e:
                failed_out.append(symbol)
                print(f"[USA {idx}/{total}] {symbol}: failed with error: {repr(e)}")
                # Optional: log every failure attempt (not only final)
                try:
                    self.repo.log_ingestion_error(
                        schema=self.cfg.error_schema,
                        table=self.cfg.error_table,
                        job_name=self.cfg.job_name,
                        symbol=symbol,
                        exchange="USA",
                        error_type=type(e).__name__,
                        error_message=str(e),
                    )
                except Exception:
                    pass

    def _to_rows(self, symbol: str, batch: List[AggBar]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for b in batch:
            ts = datetime.utcfromtimestamp(b.ts_ms / 1000.0)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            row_id = f"ID_{symbol}_{ts.strftime('%Y%m%d_%H%M')}_{self.cfg.interval_tag}"

            rows.append(
                {
                    "SYMBOL": symbol,
                    "TIMESTAMP": ts_str,
                    "OPEN": b.open,
                    "HIGH": b.high,
                    "LOW": b.low,
                    "CLOSE": b.close,
                    "VOLUME": b.volume,
                    "SOURCE": self.cfg.source,
                    "ROW_ID": row_id,
                }
            )
        return rows