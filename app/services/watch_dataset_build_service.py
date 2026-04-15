from __future__ import annotations

import asyncio
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Any

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import os

from app.infrastructure.database.repository import PostgresRepository
from app.infrastructure.api_clients.tvdatafeed_bist_provider import (
    TvDatafeedBistProvider,
    TvDatafeedBistConfig,
)


@dataclass(frozen=True)
class WatchDatasetBuildConfig:
    job_name: str = "watch_dataset_build"
    exchange: str = ""

    source_schema: str = ""
    source_table: str = ""
    source_symbol_col: str = "SYMBOL"
    source_exchange_col: str = "EXCHANGE"
    source_where_sql: str | None = None

    target_schema: str = ""
    target_table: str = ""

    provider_source_name: str = "tvDatafeed"
    max_retries: int = 3
    retry_wait_seconds: int = 2

    tv_n_bars: int = 120

    # bugunluk minute data istiyoruz.
    # provider mevcut implementasyonda start/end aliyor ama get_hist n_bars ile calisiyor.
    # bu nedenle service sadece "today UTC" window'unu filtreletiyor.
    batch_insert_size: int = 5000
    top_n:int = 10




class WatchDatasetBuildService:
    def __init__(
        self,
        repo: PostgresRepository,
        cfg: WatchDatasetBuildConfig = WatchDatasetBuildConfig(),
    ):
        self.repo = repo
        self.cfg = cfg

        tv_username = os.getenv("TV_USERNAME")
        tv_password = os.getenv("TV_PASSWORD")

        if not tv_username or not tv_password:
            raise ValueError(
                "❌ TVDATAFEED_USERNAME / TVDATAFEED_PASSWORD not found in env!"
            )

        self.provider = TvDatafeedBistProvider(
            TvDatafeedBistConfig(
                username=tv_username,
                password=tv_password,
                n_bars=cfg.tv_n_bars,
            )
        )

    async def run(
        self,
        exchange: str | None = None,
        source_schema: str | None = None,
        source_table: str | None = None,
        source_symbol_col: str | None = None,
        source_exchange_col: str | None = None,
        source_where_sql: str | None = None,
        target_schema: str | None = None,
        target_table: str | None = None,
        provider_source_name: str | None = None,
        max_retries: int | None = None,
        retry_wait_seconds: int | None = None,
        batch_insert_size: int | None = None,
        top_n: int | None = None,
    ) -> dict:
        exchange = (exchange or self.cfg.exchange).upper().strip()
        source_schema = source_schema or self.cfg.source_schema
        source_table = source_table or self.cfg.source_table
        source_symbol_col = source_symbol_col or self.cfg.source_symbol_col
        source_exchange_col = source_exchange_col or self.cfg.source_exchange_col
        source_where_sql = source_where_sql if source_where_sql is not None else self.cfg.source_where_sql
        target_schema = target_schema or self.cfg.target_schema
        target_table = target_table or self.cfg.target_table
        provider_source_name = provider_source_name or self.cfg.provider_source_name
        max_retries = max_retries or self.cfg.max_retries
        retry_wait_seconds = retry_wait_seconds or self.cfg.retry_wait_seconds
        batch_insert_size = batch_insert_size or self.cfg.batch_insert_size
        top_n = top_n if top_n is not None else self.cfg.top_n

        symbols = self.repo.get_symbols_from_table(
            source_schema=source_schema,
            source_table=source_table,
            exchange=exchange,
            symbol_col=source_symbol_col,
            exchange_col=source_exchange_col,
            where_sql=source_where_sql,
            top_n = top_n
        )
        print(symbols)

        print(
            f"[{self.cfg.job_name}] started | exchange={exchange} | "
            f"source={source_schema}.{source_table} | target={target_schema}.{target_table} | "
            f"symbol_count={len(symbols)}"
        )

        self.repo.truncate_table(
            schema_name=target_schema,
            table_name=target_table,
        )
        print(f"[{self.cfg.job_name}] target truncated -> {target_schema}.{target_table}")

        total_inserted = 0
        failed_symbols: List[str] = []

        for idx, symbol in enumerate(symbols, start=1):
            try:
                rows = await self._fetch_symbol_rows_with_retry(
                    symbol=symbol,
                    exchange=exchange,
                    provider_source_name=provider_source_name,
                    max_retries=max_retries,
                    retry_wait_seconds=retry_wait_seconds,
                )

                inserted = 0
                for i in range(0, len(rows), batch_insert_size):
                    chunk = rows[i:i + batch_insert_size]
                    inserted += self.repo.bulk_insert_watch_dataset(
                        rows=chunk,
                        target_schema=target_schema,
                        target_table=target_table,
                    )

                total_inserted += inserted

                print(
                    f"[{exchange}-WATCH {idx}/{len(symbols)}] {symbol} | "
                    f"{provider_source_name} | SUCCESS | rows={len(rows)} inserted={inserted}"
                )

            except Exception as e:
                failed_symbols.append(symbol)
                print(
                    f"[{exchange}-WATCH {idx}/{len(symbols)}] {symbol} | "
                    f"{provider_source_name} | FINAL FAIL | error={e}"
                )
                print(traceback.format_exc())

        result = {
            "job_name": self.cfg.job_name,
            "exchange": exchange,
            "source_table": f"{source_schema}.{source_table}",
            "target_table": f"{target_schema}.{target_table}",
            "symbol_count": len(symbols),
            "failed_symbol_count": len(failed_symbols),
            "failed_symbols": failed_symbols,
            "inserted_row_count": total_inserted,
        }

        print(f"[{self.cfg.job_name}] finished | result={result}")
        return result

    async def _fetch_symbol_rows_with_retry(
        self,
        symbol: str,
        exchange: str,
        provider_source_name: str,
        max_retries: int,
        retry_wait_seconds: int,
    ) -> List[Dict[str, Any]]:
        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_fixed(retry_wait_seconds),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )
        async def _inner() -> List[Dict[str, Any]]:
            rows = await self._fetch_symbol_rows_once(
                symbol=symbol,
                exchange=exchange,
                provider_source_name=provider_source_name,
            )
            if not rows:
                raise ValueError("EMPTY DATAFRAME / EMPTY RESULT")
            return rows

        return await _inner()

    async def _fetch_symbol_rows_once(
        self,
        symbol: str,
        exchange: str,
        provider_source_name: str,
    ) -> List[Dict[str, Any]]:
        today_utc = datetime.now(timezone.utc).date()

        rows: List[Dict[str, Any]] = []

        async for batch in self.provider.fetch_1min_aggs(
            symbol=symbol,
            start_date=today_utc,
            end_date=today_utc,
        ):
            if not batch:
                continue

            created_at = datetime.now()

            for bar in batch:
                bar_dt = datetime.fromtimestamp(bar.ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
                row_id = f"{exchange}_{symbol}_{bar.ts_ms}"

                rows.append(
                    {
                        "EXCHANGE": exchange,
                        "SYMBOL": symbol,
                        "TIMESTAMP": bar_dt,
                        "OPEN": bar.open,
                        "HIGH": bar.high,
                        "LOW": bar.low,
                        "CLOSE": bar.close,
                        "VOLUME": bar.volume,
                        "ROW_ID": row_id,
                        "SOURCE": provider_source_name,
                        "CREATED_AT": created_at,
                    }
                )

        return rows