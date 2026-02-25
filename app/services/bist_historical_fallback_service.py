from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class BistFallbackConfig:
    job_name: str = "bist_historical_ingestion"
    target_schema: str = "bronze"
    target_table: str = "bist_1min_tv_past"
    last_ts_schema: str = "bronze"
    last_ts_table: str = "bist_1min_tv_past"
    last_ts_column: str = "TS"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    interval_tag: str = "1min"
    source: str = "tvDatafeed"
    max_concurrent_symbols: int = 4


class BistHistoricalFallbackService:
    def __init__(self, repo: PostgresRepository, provider: MarketDataProvider, cfg: BistFallbackConfig = BistFallbackConfig()):
        self.repo = repo
        self.provider = provider
        self.cfg = cfg
        self._sem = asyncio.Semaphore(cfg.max_concurrent_symbols)

    async def run(
        self,
        symbols: List[str],
        use_db_last_timestamp: bool,
        start_date: str,
        end_date: Optional[str] = None,
    ) -> None:
        if not symbols:
            print("[BIST-FB] No symbols to fetch.")
            return

        start_dt_default = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()

        tasks = [
            asyncio.create_task(self._process_symbol(i + 1, len(symbols), s, use_db_last_timestamp, start_dt_default, end_dt))
            for i, s in enumerate(symbols)
        ]
        await asyncio.gather(*tasks)

    async def _process_symbol(
        self,
        idx: int,
        total: int,
        symbol: str,
        use_db_last_timestamp: bool,
        default_start: date,
        end_dt: date,
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

                # Success => clear active error record
                try:
                    self.repo.clear_ingestion_error(
                        schema=self.cfg.error_schema,
                        table=self.cfg.error_table,
                        job_name=self.cfg.job_name,
                        symbol=symbol,
                        exchange="BIST",
                    )
                except Exception:
                    pass

                print(
                    f"[BIST-FB {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows} "
                    f"start={start_dt} end={end_dt}"
                )

            except Exception as e:
                # Keep it in logs table as active failure
                try:
                    self.repo.upsert_ingestion_error(
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

                print(f"[BIST-FB {idx}/{total}] {symbol}: failed with error: {repr(e)}")

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