from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class UsaYahooFallbackConfig:
    job_name: str = "usa_historical_ingestion"
    target_schema: str = "bronze"
    target_table: str = "usa_1min_high_filtered"
    last_ts_schema: str = "bronze"
    last_ts_table: str = "usa_1min_high_filtered"
    last_ts_column: str = "TS"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    interval_tag: str = "1min"
    source: str = "yahooquery"
    max_concurrent_symbols: int = 4


class UsaHistoricalYahooFallbackService:
    def __init__(self, repo: PostgresRepository, provider: MarketDataProvider, cfg: UsaYahooFallbackConfig = UsaYahooFallbackConfig()):
        self.repo = repo
        self.provider = provider
        self.cfg = cfg
        self._sem = asyncio.Semaphore(cfg.max_concurrent_symbols)
        self.permanently_failed_symbols: list[str] = []

    async def run(
        self,
        symbols: List[str],
        use_db_last_timestamp: bool,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> None:
        self.permanently_failed_symbols = []
        if not symbols:
            print("[USA-YH] No symbols to fetch.")
            return

        start_dt_default = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()

        failed_local: list[str] = []

        tasks = [
            asyncio.create_task(self._process_symbol(i + 1, len(symbols), s, use_db_last_timestamp, start_dt_default, end_dt, failed_local))
            for i, s in enumerate(symbols)
        ]
        await asyncio.gather(*tasks)

        self.permanently_failed_symbols = failed_local

    async def _process_symbol(
        self,
        idx: int,
        total: int,
        symbol: str,
        use_db_last_timestamp: bool,
        default_start: date,
        end_dt: date,
        failed_out: list[str],
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

                async for batch in self.provider.fetch_1min_aggs(symbol, start_dt, end_dt):
                    rows = self._to_rows(symbol, batch)
                    attempted_rows += len(rows)
                    inserted_rows += self.repo.bulk_insert_on_conflict_do_nothing(
                        schema=self.cfg.target_schema,
                        table=self.cfg.target_table,
                        rows=rows,
                        conflict_column="ROW_ID",
                    )

                # Clear active error if success later
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
                    f"[USA-YH {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows} "
                    f"start={start_dt} end={end_dt}"
                )

            except Exception as e:
                failed_out.append((idx,symbol))

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

                print(f"[USA-YH {idx}/{total}] {symbol}: failed with error: {repr(e)}")

    def _to_rows(self, symbol: str, batch: List[AggBar]) -> List[dict]:
        out: List[dict] = []
        for b in batch:
            dt_utc = datetime.fromtimestamp(b.ts_ms / 1000.0, tz=timezone.utc)
            ts_naive = dt_utc.replace(tzinfo=None)
            ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

            row_id = f"ID_{symbol}_{dt_utc.strftime('%Y%m%d_%H%M')}_{self.cfg.interval_tag}"

            out.append(
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
        return out