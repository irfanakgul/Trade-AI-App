from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.infrastructure.api_clients.tvdatafeed_hourly_provider import (
    TvDatafeedHourlyProvider,
    TvDatafeedHourlyConfig,
)
from app.services.exchange_hourly_ingestion_service import (
    ExchangeHourlyIngestionService,
    ExchangeHourlyIngestionConfig,
)


@dataclass(frozen=True)
class OandaHourlyDataPipelineFlags:
    ingest: bool = True

    main_provider: str = "tvdatafeed"
    alternative_provider: str = "not_implemented"
    enable_fallback: bool = True

    use_db_last_timestamp: bool = True
    start_date: Optional[str] = "2024-01-01"

    safe_days_back: int = 1
    main_provider_retries: int = 2
    max_concurrent_symbols: int = 8

    symbol_schema: str = "prod"
    symbol_table: str = "FOCUS_SYMBOLS_ALL"

    target_schema: str = "raw"
    target_table: str = "assets_hourly_archive"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    cleanup_last_days: int = 1


def _build_main_provider(name: str):
    name = name.lower().strip()

    if name == "tvdatafeed":
        return TvDatafeedHourlyProvider(
            TvDatafeedHourlyConfig(
                username=os.environ["TV_USERNAME"],
                password=os.environ["TV_PASSWORD"],
                source_name="tvDatafeed",
            )
        )

    raise ValueError(f"Unsupported main provider: {name}")


async def run_oanda_hourly_data_pipeline(repo, flags: OandaHourlyDataPipelineFlags):
    print(
        "\n[OANDA-HOURLY] pipeline started... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    if not flags.ingest:
        print("[OANDA-HOURLY] ingestion skipped")
        return

    repo.delete_recent_days_by_last_ts(
        schema=flags.target_schema,
        table=flags.target_table,
        ts_col="TS",
        days_back=flags.cleanup_last_days,
    )

    symbols = repo.get_in_scope_symbols_from_table(
        schema=flags.symbol_schema,
        table=flags.symbol_table,
        exchange="OANDA",
        symbol_col="SYMBOL",
        exchange_col="EXCHANGE",
        in_scope_col="IN_SCOPE",
    )

    print(f"[OANDA-HOURLY] symbol_count={len(symbols)}")

    main_provider = _build_main_provider(flags.main_provider)

    # Fallback reserved for future implementation
    alternative_provider = None

    svc = ExchangeHourlyIngestionService(
        repo=repo,
        main_provider=main_provider,
        alternative_provider=alternative_provider,
        cfg=ExchangeHourlyIngestionConfig(
            job_name="oanda_hourly_ingestion",
            exchange="OANDA",
            target_schema=flags.target_schema,
            target_table=flags.target_table,
            last_ts_schema=flags.target_schema,
            last_ts_table=flags.target_table,
            last_ts_column="TS",
            error_schema=flags.error_schema,
            error_table=flags.error_table,
            safe_days_back=flags.safe_days_back,
            main_provider_retries=flags.main_provider_retries,
            enable_fallback=flags.enable_fallback,
            max_concurrent_symbols=flags.max_concurrent_symbols,
        ),
    )

    await svc.run(
        symbols=symbols,
        use_db_last_timestamp=flags.use_db_last_timestamp,
        start_date=flags.start_date,
    )

    print(
        "\n[OANDA-HOURLY] pipeline finished... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )