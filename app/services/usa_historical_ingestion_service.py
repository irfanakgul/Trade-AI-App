from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import Dict, Any, List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class UsaIngestionConfig:
    target_schema: str = "bronze"
    target_table: str = "usa_1min_high_filtered"
    last_ts_schema: str = "bronze"
    last_ts_table: str = "usa_1min_high_filtered"
    last_ts_column: str = "TS"  # Use typed TS for speed and correctness
    interval_tag: str = "1min"
    source: str = "polygon"
    max_concurrent_symbols: int = 8
    symbol_level_retries: int = 2
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

        self.permanently_failed_symbols = []

        total = len(symbols)
        start_dt_default = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()

        remaining: List[tuple[int, str]] = [(i + 1, s) for i, s in enumerate(symbols)]
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
                for idx, s in remaining
            ]
            await asyncio.gather(*tasks)

            if not failed:
                print(f"[USA] All symbols completed. rounds={round_idx}")
                break

            print(f"[USA] Round {round_idx} finished. failed_symbols={len(failed)}")

            if round_idx > (1 + self.cfg.symbol_level_retries):
                print(f"[USA] Exhausted retries. permanently_failed={len(failed)}")
                self.permanently_failed_symbols = [s for (_, s) in failed]

                for _, s in failed:
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
                        ts_column=self.cfg.last_ts_column,  # TS
                    )
                    if last_ts:
                        start_dt = last_ts.date()

                async for batch in self.provider.fetch_1min_aggs(symbol, start_dt, end_dt):
                    rows = self._to_rows(symbol, batch)
                    attempted_rows += len(rows)
                    inserted_rows += self.repo.bulk_insert_on_conflict_do_nothing(
                        schema=self.cfg.target_schema,
                        table=self.cfg.target_table,
                        rows=rows,
                        conflict_column="ROW_ID",
                    )

                # Success => clear active error record (if any)
                try:
                    self.repo.clear_ingestion_error(
                        schema=self.cfg.error_schema,
                        table=self.cfg.error_table,
                        job_name=self.cfg.job_name,
                        symbol=symbol,
                        exchange="USA",
                    )
                except Exception:
                    pass

                print(
                    f"[USA {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows} "
                    f"start={start_dt} end={end_dt}"
                )

            except Exception as e:
                failed_out.append((idx, symbol))
                print(f"[USA {idx}/{total}] {symbol}: failed with error: {repr(e)}")

                # Record/refresh active error on every failure attempt (cleared on success later)
                try:
                    self.repo.upsert_ingestion_error(
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
            dt_utc = datetime.fromtimestamp(b.ts_ms / 1000.0, tz=timezone.utc)
            ts_naive = dt_utc.replace(tzinfo=None)
            ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
            row_id = f"ID_{symbol}_{dt_utc.strftime('%Y%m%d_%H%M')}_{self.cfg.interval_tag}"

            rows.append(
                {
                    "SYMBOL": symbol,
                    "TIMESTAMP": ts_str,
                    "TS": ts_naive,
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