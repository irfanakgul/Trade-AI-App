import os
from dataclasses import dataclass
from typing import Optional
from datetime import date
from app.services.dq_v2_service import DQV2Service, DQRunConfig, DQTableConfig
from app.infrastructure.api_clients.polygon_provider import PolygonProvider
from app.services.usa_historical_ingestion_service import UsaHistoricalIngestionService
from app.infrastructure.api_clients.twelvedata_provider import TwelveDataProvider, TwelveDataConfig
from app.services.usa_historical_fallback_service import UsaHistoricalFallbackService
from app.infrastructure.api_clients.yahooquery_usa_provider import YahooQueryUsaProvider
from app.services.usa_historical_yahoo_fallback_service import UsaHistoricalYahooFallbackService
from app.infrastructure.database.repository import PostgresRepository
from datetime import datetime

@dataclass(frozen=True)
class UsaDataPipelineFlags:
    ingest: bool = True
    fallback_twelvedata: bool = True
    fallback_yahoo: bool = True
    sync_archive_to_working: bool = True
    interval: str = '1min'
    sync_start_date: str = None  
    trim365: bool = True
    build_focus_dataset: bool = True
    dq: bool = True
    apply_dq_out_scope: bool = True # dq failed symbols will be out of scope for ema and further.False include, True exclude

    use_db_last_timestamp: bool = True
    start_date: str = "2026-01-01"
    end_date: Optional[str] = None

    safety_days: int = 1
    lookback_days: int = 365
    reference_days_ago: int = 1
    min_trading_days: int = 15

    err_schema: str = "logs"
    err_table: str = "ingestion_errors"
    job_name: str = "usa_historical_ingestion"

    # auto sample flags
    auto_sample_run: bool = True
    smpl_source_schema: str ="silver"
    smpl_source_table: str ="FRVP_USA_FOCUS_DATASET"
    smpl_target_schema: str ="test"
    smpl_target_table: str ="sample_usa_1min"
    smpl_symbol_col:str = "SYMBOL"
    smpl_ts_col:str = "TS"
    smpl_trading_days_back:int = 30

async def run_usa_data_pipeline(repo: PostgresRepository, flags: UsaDataPipelineFlags) -> None:
    print(
        f"\n[USA] Data pipeline started... "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
    )

    # 1) Ingestion (Polygon)
    if flags.ingest:
        repo.delete_recent_days_by_last_ts(schema='raw',table='usa_1min_archive',ts_col= "TS",days_back = 1)
        repo.delete_recent_days_by_last_ts(schema='bronze',table='usa_1min_high_filtered',ts_col= "TS",days_back = 1)

        usa_provider = PolygonProvider(api_key=os.environ["POLYGON_API_KEY"])
        usa_svc = UsaHistoricalIngestionService(repo=repo, provider=usa_provider)

        await usa_svc.run(
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )
    else:
        print("[USA] ingest skipped")

    # After Polygon (or if ingest skipped), use error table as source of truth
    still_failed = repo.get_active_error_symbols(
        schema=flags.err_schema,
        table=flags.err_table,
        job_name=flags.job_name,
        exchange="USA",
    )
    print(f"\n[USA] After Polygon: still_failed={len(still_failed)}\n")

    # 2) Fallback-1 (TwelveData)
    if flags.fallback_twelvedata and still_failed:
        print(f"\n[USA-TD] TwelveData fallback started. symbols={len(still_failed)}\n")
        td_provider = TwelveDataProvider(TwelveDataConfig(api_key=os.environ["TWELVEDATA_API_KEY"]))
        usa_td_svc = UsaHistoricalFallbackService(repo=repo, provider=td_provider)

        await usa_td_svc.run_last_week(still_failed)

        still_failed = repo.get_active_error_symbols(
            schema=flags.err_schema,
            table=flags.err_table,
            job_name=flags.job_name,
            exchange="USA",
        )
        print(f"\n[USA] After TwelveData: still_failed={len(still_failed)}\n")
    elif flags.fallback_twelvedata:
        print("[USA-TD] fallback skipped (no failed symbols)")
    else:
        print("[USA-TD] fallback disabled")

    # 3) Fallback-2 (YahooQuery)
    if flags.fallback_yahoo and still_failed:
        print(f"\n[USA-YH] YahooQuery fallback started. symbols={len(still_failed)}\n")
        yh_provider = YahooQueryUsaProvider()
        usa_yh_svc = UsaHistoricalYahooFallbackService(repo=repo, provider=yh_provider)

        await usa_yh_svc.run(
            symbols=still_failed,
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )

        still_failed = repo.get_active_error_symbols(
            schema=flags.err_schema,
            table=flags.err_table,
            job_name=flags.job_name,
            exchange="USA",
        )
        print(f"\n[USA] After YahooQuery: still_failed={len(still_failed)}\n")
    elif flags.fallback_yahoo:
        print("[USA-YH] fallback skipped (no failed symbols)")
    else:
        print("[USA-YH] fallback disabled")

    print(
        f"[USA] data update completed "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}"
    )
    # 4) Sync raw -> bronze working
    if flags.sync_archive_to_working:
        print("\n[USA-SYNC] Sync archive -> working started...\n")
        ins_usa = repo.sync_archive_to_working(
            archive_schema="raw",
            archive_table="usa_1min_archive",
            working_schema="bronze",
            working_table="usa_1min_high_filtered",
            sync_start_date= None,
            interval='1min',
            ts_col="TS",
            safety_days=flags.safety_days,
        )
        print(f"[USA-SYNC] Sync completed. inserted_rows={ins_usa}\n")
    else:
        print("[USA-SYNC] sync skipped")

    print(
        f"[USA-SYNC] Data cloned into bronze from raw "
        f"|RT: {datetime.now().strftime('%d-%m-%Y %H:%M')}"
    )
    # 5) Trim
    if flags.trim365:
        before_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
        print(f"[USA] rows before trim: {before_usa}")

        deleted_usa = repo.trim_history_by_peak_or_lookback_ts(
            schema="bronze",
            table="usa_1min_high_filtered",
            symbol_col="SYMBOL",
            ts_typed_col="TS",
            high_col="HIGH",
            lookback_days=flags.lookback_days,
            reference_days_ago=flags.reference_days_ago,
        )
        print(
        f"[USA] trim365 completed. deleted_rows={deleted_usa} "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}"
            )

        after_usa = repo.count_rows(schema="bronze", table="usa_1min_high_filtered")
        print(f"[USA] rows after trim: {after_usa}")
    else:
        print("[USA] trim skipped")

    # 6) Focus dataset + focus symbol list rebuild for indicators
    if flags.build_focus_dataset:
        stats_usa = repo.build_frvp_focus_dataset(
            source_schema="bronze",
            source_table="usa_1min_high_filtered",
            target_schema="silver",
            target_table="FRVP_USA_FOCUS_DATASET",
            ts_col="TS",
            high_col="HIGH",
            exchange="USA",
            min_trading_days=flags.min_trading_days,
        )
        print(
            f'[USA] Focus dataset built. '
            f'symbols: {stats_usa["before_symbols"]} -> {stats_usa["after_symbols"]}, '
            f'rows: {stats_usa["before_rows"]} -> {stats_usa["after_rows"]}'
        )
    else:
        print("[USA] focus dataset build skipped")

    # Optional: final remaining errors count
    final_failed = repo.get_active_error_symbols(
        schema=flags.err_schema,
        table=flags.err_table,
        job_name=flags.job_name,
        exchange="USA",
    )
    print(f"\n[USA] Final remaining failed symbols in logs: {len(final_failed)}\n")

    # ----------------------------------------------------------
    # 6) SAMPLE AUTO DATASET
    # ----------------------------------------------------------

    if flags.auto_sample_run:
        symbols = os.getenv("USA_SAMPLE_SYMBOLS", "")
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
        print(f'[SAMPLE-USA-1MIN] | Sample symbols {len(symbols)} > {symbols}')

        repo.rebuild_symbol_sample_dataset(
            source_schema=flags.smpl_source_schema,
            source_table=flags.smpl_source_table,
            target_schema=flags.smpl_target_schema,
            target_table=flags.smpl_target_table,
            symbols=symbols,
            symbol_col=flags.smpl_symbol_col,
            ts_col=flags.smpl_ts_col,
            trading_days_back=flags.smpl_trading_days_back,
        )
    else: 
        print(f'⏭️[SAMPLE-USA-1MIN] SKIPPED!')

    # ----------------------------------------------------------
    # 7) DQ CHECKS
    # ----------------------------------------------------------
    if flags.dq:
        print("[USA-DQ] starting...")

        dq = DQV2Service(repo)

        usa_dq_run_cfg = DQRunConfig(
            job_name="usa_1min_data_pipeline",
            active_table="usa_dq_check",
            specific_trading_calendar=False,   # later True when calendar table is ready
            known_holidays=(),                 # later fill for USA holidays
            as_of_date=date.today(),
        )

        usa_tables = [
            DQTableConfig(
                exchange="USA",
                schema_name="raw",
                table_name="usa_1min_archive",
                interval="1min",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=23,        # change if your market-close convention differs
                expected_close_minute=59,
                end_tolerance_minutes=60,
                bar_threshold=360,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
            DQTableConfig(
                exchange="USA",
                schema_name="bronze",
                table_name="usa_1min_high_filtered",
                interval="1min",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=23,
                expected_close_minute=59,
                end_tolerance_minutes=60,
                bar_threshold=360,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
            DQTableConfig(
                exchange="USA",
                schema_name="silver",
                table_name="FRVP_USA_FOCUS_DATASET",
                interval="1min",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=23,
                expected_close_minute=59,
                end_tolerance_minutes=60,
                bar_threshold=360,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
        ]

        usa_dq_run_id = dq.run_exchange_checks(usa_dq_run_cfg, usa_tables)
        print(f"[USA-DQ] completed. DQ_RUN_ID={usa_dq_run_id}")