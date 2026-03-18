from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.infrastructure.api_clients.binance_hourly_provider import (
    BinanceHourlyProvider,
    BinanceHourlyConfig,
)
from app.infrastructure.api_clients.tvdatafeed_hourly_provider import (
    TvDatafeedHourlyProvider,
    TvDatafeedHourlyConfig,
)
from app.services.exchange_hourly_ingestion_service import (
    ExchangeHourlyIngestionService,
    ExchangeHourlyIngestionConfig,
)


@dataclass(frozen=True)
class BinanceHourlyDataPipelineFlags:
    # Step-1: ingestion
    ingest: bool = True

    main_provider: str = "binance_api"
    alternative_provider: str = "tvdatafeed"
    enable_fallback: bool = True

    use_db_last_timestamp: bool = True
    start_date: Optional[str] = "2024-01-01"

    safe_days_back: int = 1
    main_provider_retries: int = 2
    max_concurrent_symbols: int = 8

    symbol_schema: str = "prod"
    symbol_table: str = "FOCUS_SYMBOLS_ALL"

    target_schema: str = "raw"
    target_table: str = "crypto_hourly_archive"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    cleanup_last_days: int = 1

    # Step-2 and beyond
    sync_archive_to_working: bool = False
    trim_history: bool = False
    build_focus_dataset: bool = False
    build_sample_dataset: bool = False
    run_dq: bool = False


def _build_provider(name: str):
    name = name.lower().strip()

    if name == "binance_api":
        return BinanceHourlyProvider(
            BinanceHourlyConfig(
                source_name="binance_api",
            )
        )

    if name == "tvdatafeed":
        return TvDatafeedHourlyProvider(
            TvDatafeedHourlyConfig(
                username=os.environ["TV_USERNAME"],
                password=os.environ["TV_PASSWORD"],
                source_name="tvDatafeed",
            )
        )

    raise ValueError(f"Unsupported provider: {name}")


async def run_binance_hourly_data_pipeline(repo, flags: BinanceHourlyDataPipelineFlags):
    print(
        "\n[BINANCE-HOURLY] pipeline started... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    # ----------------------------------------------------------
    # 1) INGESTION
    # ----------------------------------------------------------
    if flags.ingest:
        repo.delete_recent_days_by_last_ts(
            schema=flags.target_schema,
            table=flags.target_table,
            ts_col="TS",
            days_back=flags.cleanup_last_days,
        )

        symbols = repo.get_in_scope_symbols_from_table(
            schema=flags.symbol_schema,
            table=flags.symbol_table,
            exchange="BINANCE",
            symbol_col="SYMBOL",
            exchange_col="EXCHANGE",
            in_scope_col="IN_SCOPE",
        )

        print(f"[BINANCE-HOURLY] symbol_count={len(symbols)}")

        main_provider = _build_provider(flags.main_provider)
        alternative_provider = _build_provider(flags.alternative_provider) if flags.enable_fallback else None

        svc = ExchangeHourlyIngestionService(
            repo=repo,
            main_provider=main_provider,
            alternative_provider=alternative_provider,
            cfg=ExchangeHourlyIngestionConfig(
                job_name="binance_hourly_ingestion",
                exchange="BINANCE",
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
    else:
        print("[BINANCE-HOURLY] ingestion skipped")

    # ----------------------------------------------------------
    # 2) SYNC raw -> bronze/working
    # ----------------------------------------------------------
    if flags.sync_archive_to_working:
        print(F"[SYNC] CRYPTO sync implementation started...\n")
        ins = repo.sync_archive_to_working(
            archive_schema=flags.target_schema,
            archive_table=flags.target_table,
            working_schema="bronze",
            working_table="synced_working_crypto_hourly",
            ts_col="TS",
            safety_days=1,
            interval = "hourly",
        )
        print(
            f"[SYNC] CRYPTO sync completed. inserted_rows={ins} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n")
    else:
        print("[SYNC] CRYPTO skipped")

    # ----------------------------------------------------------
    # 3) TRIM
    # ----------------------------------------------------------
    if flags.trim_history:
        print("[BINANCE-HOURLY] trim step placeholder")
        # later:
        # repo.trim_history_by_peak_or_lookback_ts(...)
    else:
        print("[BINANCE-HOURLY] trim skipped")

    # ----------------------------------------------------------
    # 4) BUILD FOCUS DATASET
    # ----------------------------------------------------------
    if flags.build_focus_dataset:
        print("[BINANCE-HOURLY] focus dataset step placeholder")
        # later:
        # repo.build_frvp_focus_dataset(...)
    else:
        print("[BINANCE-HOURLY] focus dataset skipped")

    # ----------------------------------------------------------
    # 5) BUILD SAMPLE DATASET
    # ----------------------------------------------------------
    if flags.build_sample_dataset:
        print("[BINANCE-HOURLY] sample dataset step placeholder")
        # later:
        # repo.rebuild_symbol_sample_dataset(...)
    else:
        print("[BINANCE-HOURLY] sample dataset skipped")

    # ----------------------------------------------------------
    # 6) DQ
    # ----------------------------------------------------------
    if flags.run_dq:
        print("[BINANCE-HOURLY] dq step placeholder")
        # later:
        # dq.run(...)
    else:
        print("[BINANCE-HOURLY] dq skipped")

    print(
        "\n[BINANCE-HOURLY] pipeline finished... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )