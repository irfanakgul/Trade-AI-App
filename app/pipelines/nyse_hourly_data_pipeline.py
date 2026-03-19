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
from datetime import datetime,date
from app.services.dq_v2_service import DQV2Service, DQRunConfig, DQTableConfig


@dataclass(frozen=True)
class NyseHourlyDataPipelineFlags:
    # Step-1: ingestion
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
    target_table: str = "nyse_hourly_archive"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    cleanup_last_days: int = 1

    # Step-2 and beyond
    sync_archive_to_working: bool = False
    trim_history: bool = False
    build_focus_dataset: bool = False
    build_sample_dataset: bool = False
    run_dq: bool = False


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


async def run_nyse_hourly_data_pipeline(repo, flags: NyseHourlyDataPipelineFlags,exchange):
    print(
        "\n►►►►►►►►[nyse-HOURLY] pipeline started... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    # ----------------------------------------------------------
    # 1) INGESTION
    # ----------------------------------------------------------
    if flags.ingest:
        print('[INGESTION] nyse started...')
        repo.delete_recent_days_by_last_ts(
            schema=flags.target_schema,
            table=flags.target_table,
            ts_col="TS",
            days_back=flags.cleanup_last_days,
        )

        symbols = repo.get_in_scope_symbols_from_table(
            schema=flags.symbol_schema,
            table=flags.symbol_table,
            exchange="NYSE",
            symbol_col="SYMBOL",
            exchange_col="EXCHANGE",
            in_scope_col="IN_SCOPE",
        )

        print(f"[NYSE] INGEST symbol_count={len(symbols)}")

        main_provider = _build_main_provider(flags.main_provider)

        # Fallback is intentionally reserved for future implementation
        alternative_provider = None

        svc = ExchangeHourlyIngestionService(
            repo=repo,
            main_provider=main_provider,
            alternative_provider=alternative_provider,
            cfg=ExchangeHourlyIngestionConfig(
                job_name="nyse_hourly_ingestion",
                exchange="NYSE",
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
        print("❌ [INGESTION] NYSE skipped")

    # ----------------------------------------------------------
    # 2) SYNC raw -> bronze/working
    # ----------------------------------------------------------
    if flags.sync_archive_to_working:
        print(F"[SYNC] NYSE implementation started...\n")
        ins = repo.sync_archive_to_working(
            archive_schema=flags.target_schema,
            archive_table=flags.target_table,
            working_schema="bronze",
            working_table="synced_working_nyse_hourly",
            ts_col="TS",
            safety_days=1,
            interval = "hourly",
        )
        print(
            f"[SYNC] NYSE completed. inserted_rows={ins} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n")
    else:
        print("❌ [SYNC] NYSE skipped")

    # ----------------------------------------------------------
    # 3) TRIM
    # ----------------------------------------------------------
    if flags.trim_history:
        print(F"[TRIM365] nyse started...\n")

        before = repo.count_rows(schema="bronze", table="synced_working_nyse_hourly")
        print(f"[TRIM365] nyse rows before trim: {before}")

        deleted = repo.trim_history_by_peak_or_lookback_ts(
            schema="bronze",
            table="synced_working_nyse_hourly",
            symbol_col="SYMBOL",
            ts_typed_col="TS",
            high_col="HIGH",
            lookback_days=365,
            reference_days_ago=1,
        )
        print(
            f"[TRIM365] NYSE completed. deleted_rows={deleted} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}"
        )

        after = repo.count_rows(schema="bronze", table="synced_working_nyse_hourly")
        print(f"[TRIM365] NYSE rows after trim: {after}")
    else:
        print("❌ [TRIM365] NYSE skipped!")

    # ----------------------------------------------------------
    # 4) BUILD FOCUS DATASET
    # ----------------------------------------------------------
    if flags.build_focus_dataset:
        print("[IND-FOCUS] NYE dataset build started...")

        stats = repo.build_frvp_focus_dataset(
            source_schema="bronze",
            source_table="synced_working_nyse_hourly",
            target_schema="silver",
            target_table="indicators_nyse_focus_dataset",
            ts_col="TS",
            high_col="HIGH",
            exchange=exchange,
            min_trading_days=15,
        )

        #adding elemination reason
        repo.update_focus_symbol_scope(
            exchange=exchange,
            compare_schema = 'silver',
            compare_table='indicators_nyse_focus_dataset',
            reason='Highest HIGH Value falled in last 15 days',
            main_symbol_schema = 'prod',
            main_symbol_table = 'FOCUS_SYMBOLS_ALL',
            drop_and_recreate = False
        )

        print(
            f'[IND-FOCUS] NYSE Focus dataset built. '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows: {stats["before_rows"]} -> {stats["after_rows"]} '
            f'{datetime.now().strftime("%d-%m-%Y %H:%M")}')
        
    else:
        print("❌ [IND-FOCUS] NYSE indicator focus dataset build skipped!")

    # ----------------------------------------------------------
    # 5) DQ
    # ----------------------------------------------------------
    if flags.run_dq:
        print("[DQ] NYSE starting...")

        dq = DQV2Service(repo)

        dq_run_cfg = DQRunConfig(
            job_name="nyse_hourly_data_pipeline",
            active_table="dq_check_overview_nyse",
            specific_trading_calendar=False,   # later True when calendar table is ready
            known_holidays=(),                 # later fill for USA holidays
            as_of_date=date.today(),
        )

        tables = [
            DQTableConfig(
                exchange="NYSE",
                schema_name="raw",
                table_name="nyse_hourly_archive",
                interval="hourly",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=21,        # change if your market-close convention differs
                expected_close_minute=30,
                end_tolerance_minutes=65,
                bar_threshold=24,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
            DQTableConfig(
                exchange="NYSE",
                schema_name="bronze",
                table_name="synced_working_nyse_hourly",
                interval="hourly",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=21,
                expected_close_minute=30,
                end_tolerance_minutes=65,
                bar_threshold=24,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
            DQTableConfig(
                exchange="NYSE",
                schema_name="silver",
                table_name="indicators_nyse_focus_dataset",
                interval="hourly",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=21,
                expected_close_minute=30,
                end_tolerance_minutes=65,
                bar_threshold=24,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
        ]

        dq_run_id = dq.run_exchange_checks(dq_run_cfg, tables)
        print(f"[DQ] NYSE completed. DQ_RUN_ID={dq_run_id}")
    else:
        print("❌ [DQ] NYSE skipped")