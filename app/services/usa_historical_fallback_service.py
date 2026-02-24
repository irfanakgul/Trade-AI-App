# app/services/usa_historical_fallback_service.py

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timezone
from typing import List, Optional

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.market_data_provider import MarketDataProvider, AggBar


@dataclass(frozen=True)
class UsaFallbackConfig:
    target_schema: str = "bronze"
    target_table: str = "usa_1min_high_filtered"
    interval_tag: str = "1min"
    source: str = "twelvedata"
    max_concurrent_symbols: int = 4


class UsaHistoricalFallbackService:
    def __init__(self, repo: PostgresRepository, provider: MarketDataProvider, cfg: UsaFallbackConfig = UsaFallbackConfig()):
        self.repo = repo
        self.provider = provider
        self.cfg = cfg
        self._sem = asyncio.Semaphore(cfg.max_concurrent_symbols)

    async def run_last_week(self, symbols: List[str]) -> None:
        if not symbols:
            print("[USA-FB] No symbols to fetch.")
            return

        tasks = [
            asyncio.create_task(self._process_symbol(symbol=s, idx=i + 1, total=len(symbols)))
            for i, s in enumerate(symbols)
        ]
        await asyncio.gather(*tasks)

    async def _process_symbol(self, symbol: str, idx: int, total: int) -> None:
        async with self._sem:
            try:
                # We keep the interface signature but provider fetches last week internally
                start_dt = date.today()
                end_dt = date.today()

                inserted_rows = 0
                attempted_rows = 0

                async for batch in self.provider.fetch_1min_aggs(symbol, start_dt, end_dt):
                    rows = self._to_rows(symbol, batch)
                    attempted_rows += len(rows)
                    inserted_rows += self.repo.bulk_insert_on_conflict_do_nothing(
                        schema=self.cfg.target_schema,
                        table=self.cfg.target_table,
                        rows=rows,
                        conflict_column="ROW_ID",
                    )

                print(
                    f"[USA-FB {idx}/{total}] {symbol}: done. "
                    f"attempted_rows={attempted_rows} inserted_rows={inserted_rows}"
                )

            except Exception as e:
                print(f"[USA-FB {idx}/{total}] {symbol}: failed with error: {repr(e)}")
                # We can log to logs.ingestion_errors if needed (same mechanism as primary)

    def _to_rows(self, symbol: str, batch: List[AggBar]) -> List[dict]:
        out: List[dict] = []
        for b in batch:
            dt_utc = datetime.fromtimestamp(b.ts_ms / 1000.0, tz=timezone.utc)
            # TS is typed timestamp column (UTC naive for timestamp without tz)
            ts_naive = dt_utc.replace(tzinfo=None)
            ts_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

            row_id = f"ID_{symbol}_{dt_utc.strftime('%Y%m%d_%H%M')}_{self.cfg.interval_tag}"

            out.append(
                {
                    "SYMBOL": symbol,
                    "TIMESTAMP": ts_str,     # keep legacy text column consistent
                    "TS": ts_naive,          # typed timestamp column for fast trim/index
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