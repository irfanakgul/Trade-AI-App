from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import List, Optional, Dict, Any

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class UsaFallbackConfig:
    job_name: str = "usa_historical_ingestion"
    target_schema: str = "raw"
    target_table: str = "usa_1min_archive"

    last_ts_schema: str = "bronze"
    last_ts_table: str = "usa_1min_high_filtered"
    last_ts_column: str = "TS"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    interval_tag: str = "1min"
    source: str = "twelvedata"

    max_concurrent_symbols: int = 6


class UsaHistoricalFallbackService:
    def __init__(
        self,
        repo: PostgresRepository,
        provider: MarketDataProvider,
        cfg: UsaFallbackConfig = UsaFallbackConfig(),
    ):
        self.repo = repo
        self.provider = provider
        self.cfg = cfg
        self._sem = asyncio.Semaphore(self.cfg.max_concurrent_symbols)

    async def run_last_week(self, symbols: List[str]) -> None:
        """
        Fetch last ~7 days for the given symbols via TwelveData and write idempotently.
        Clears logs.ingestion_errors on success, upserts on failure.
        """
        if not symbols:
            print("[USA-FB] No symbols to fetch.")
            return

        end_dt = date.today()
        start_dt = end_dt - timedelta(days=7)

        tasks = [
            asyncio.create_task(self._process_symbol(i + 1, len(symbols), s, start_dt, end_dt))
            for i, s in enumerate(symbols)
        ]
        await asyncio.gather(*tasks)

    async def _process_symbol(
        self,
        idx: int,
        total: int,
        symbol: str,
        start_dt: date,
        end_dt: date,
    ) -> None:
        async with self._sem:
            attempted_rows = 0
            inserted_rows = 0

            try:
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
                    f"[USA-FB {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows} "
                    f"start={start_dt} end={end_dt}"
                )

            except Exception as e:
                # Keep it as an active failure until a later success clears it
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

                print(f"[USA-FB {idx}/{total}] {symbol}: failed with error: {repr(e)}")

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