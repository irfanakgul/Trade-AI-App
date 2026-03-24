from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from app.infrastructure.api_clients.tvdatafeed_hourly_provider import (
    TvDatafeedHourlyProvider,
    TvDatafeedHourlyConfig,
)
from app.infrastructure.api_clients.yahooquery_hourly_provider import (
    YahooQueryHourlyProvider,
    YahooQueryHourlyConfig,
)
from app.services.exchange_hourly_ingestion_service import (
    ExchangeHourlyIngestionService,
    ExchangeHourlyIngestionConfig,
)
from datetime import datetime,date
from app.services.dq_v2_service import DQV2Service, DQRunConfig, DQTableConfig

# indicator modules
from app.infrastructure.database.repository import PostgresRepository
from app.services.ind_frv_poc_profile_service import IndFrvPocProfileService
from app.services.ind_ema_focus_service import IndEmaFocusService
from app.services.ind_vwap_focus_service import IndVwapFocusService # type: ignore
from app.services.email_service import send_email
from app.services.ind_bar_status_service import IndBarStatusService # type: ignore
from app.services.ind_rsi_focus_service import IndRsiFocusService # type: ignore # type: ignore
from app.services.ind_mfi_focus_service import IndMfiFocusService # type: ignore
from app.services.ind_master_combined_indicators_service import IndMasterCombinedIndicatorsService # type: ignore


@dataclass(frozen=True)
class EuronextHourlyDataPipelineFlags:
    # Step-1: ingestion
    ingest: bool = True

    main_provider: str = "tvdatafeed"
    alternative_provider: str = "yahooquery"
    enable_fallback: bool = True

    use_db_last_timestamp: bool = True
    start_date: Optional[str] = "2024-01-01"

    safe_days_back: int = 1
    main_provider_retries: int = 2
    max_concurrent_symbols: int = 8

    symbol_schema: str = "prod"
    symbol_table: str = "FOCUS_SYMBOLS_ALL"

    target_schema: str = "raw"
    target_table: str = "ams_hourly_archive"

    error_schema: str = "logs"
    error_table: str = "ingestion_errors"

    cleanup_last_days: int = 1

    # Step-2 and beyond
    sync_archive_to_working: bool = False
    trim_history: bool = False
    build_focus_dataset: bool = False
    build_sample_dataset: bool = False
    run_dq: bool = False
    dq_elemination: bool = False

    #-------------------------------------
    # indicator flags
    #-------------------------------------
    bar_status: bool = True
    run_frvp: bool = True
    run_convert_daily: bool = True
    run_ema_ind: bool = True
    converted_schema:str = 'silver'
    converted_table:str = 'converted_daily_dataset_ams'
    run_vwap_ind:bool = True
    run_rsi_ind:bool = True
    run_mfi_ind:bool = True
    run_combined_indicators:bool = True


def _build_provider(name: str):
    name = name.lower().strip()

    if name == "tvdatafeed":
        return TvDatafeedHourlyProvider(
            TvDatafeedHourlyConfig(
                username=os.environ["TV_USERNAME"],
                password=os.environ["TV_PASSWORD"],
                source_name="tvDatafeed",
            )
        )

    if name == "yahooquery":
        return YahooQueryHourlyProvider(
            YahooQueryHourlyConfig(source_name="yahooquery")
        )

    raise ValueError(f"Unsupported provider: {name}")


async def run_euronext_hourly_data_pipeline(repo, flags: EuronextHourlyDataPipelineFlags,exchange):
    print(
        "\n[EURONEXT-HOURLY] pipeline started... "
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
            exchange=exchange,
            symbol_col="SYMBOL",
            exchange_col="EXCHANGE",
            in_scope_col="IN_SCOPE",
        )

        print(f"[INGESTION] AMS symbol_count={len(symbols)}")

        main_provider = _build_provider(flags.main_provider)
        alternative_provider = _build_provider(flags.alternative_provider) if flags.enable_fallback else None

        svc = ExchangeHourlyIngestionService(
            repo=repo,
            main_provider=main_provider,
            alternative_provider=alternative_provider,
            cfg=ExchangeHourlyIngestionConfig(
                job_name="euronext_hourly_ingestion",
                exchange=exchange,
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
        print("❌ [INGESTION] AMS ingestion skipped")

    # ----------------------------------------------------------
    # 2) SYNC raw -> raw/bronze/working
    # ----------------------------------------------------------
    if flags.sync_archive_to_working:
        print(F"[SYNC] AMS sync implementation started...\n")
        ins = repo.sync_archive_to_working(
            archive_schema=flags.target_schema,
            archive_table=flags.target_table,
            working_schema="bronze",
            working_table="synced_working_ams_hourly",
            ts_col="TS",
            safety_days=1,
            interval = "hourly",
        )
        print(
            f"[SYNC] AMS sync completed. inserted_rows={ins} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n")
    else:
        print("❌ [SYNC] AMS skipped")

    # ----------------------------------------------------------
    # 3) TRIM
    # ----------------------------------------------------------
    if flags.trim_history:
        print(F"[TRIM365] AMS TRIM365 started...\n")

        before = repo.count_rows(schema="bronze", table="synced_working_ams_hourly")
        print(f"[TRIM365] AMS rows before trim: {before}")

        deleted = repo.trim_history_by_peak_or_lookback_ts(
            schema="bronze",
            table="synced_working_ams_hourly",
            symbol_col="SYMBOL",
            ts_typed_col="TS",
            high_col="HIGH",
            lookback_days=365,
            reference_days_ago=1,
        )
        print(
            f"[TRIM365] AMS trim365 completed. deleted_rows={deleted} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}"
        )

        after = repo.count_rows(schema="bronze", table="synced_working_ams_hourly")
        print(f"[TRIM365] AMS rows after trim: {after}")
    else:
        print("❌ [TRIM365] AMS trim skipped")

    # ----------------------------------------------------------
    # 4) BUILD IND FOCUS DATASET
    # ----------------------------------------------------------
    if flags.build_focus_dataset:
        print("[IND-FOCUS] AMS dataset build started...")

        stats = repo.build_frvp_focus_dataset(
            source_schema="bronze",
            source_table="synced_working_ams_hourly",
            target_schema="silver",
            target_table="indicators_ams_focus_dataset",
            ts_col="TS",
            high_col="HIGH",
            exchange=exchange,
            min_trading_days=15,
        )

        #adding elemination reason
        repo.update_focus_symbol_scope(
            exchange=exchange,
            compare_schema = 'silver',
            compare_table='indicators_ams_focus_dataset',
            reason='Highest HIGH Value falled in last 15 days',
            main_symbol_schema = 'prod',
            main_symbol_table = 'FOCUS_SYMBOLS_ALL',
            drop_and_recreate = False
        )

        print(
            f'[IND-FOCUS] AMS Focus dataset built. '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows: {stats["before_rows"]} -> {stats["after_rows"]} '
            f'{datetime.now().strftime("%d-%m-%Y %H:%M")}')
        
    else:
        print("❌ [IND-FOCUS] AMS dataset build skipped!")

    # ----------------------------------------------------------
    # 5) DQ
    # ----------------------------------------------------------
    if flags.run_dq:
        print("[DQ] AMS starting...")

        expected_bar_count = 6

        dq = DQV2Service(repo)
        dq_run_cfg = DQRunConfig(
            job_name="ams_hourly_data_pipeline",
            active_table="dq_check_overview_ams",
            specific_trading_calendar=False,   # later True when calendar table is ready
            known_holidays=(),                 # later fill for USA holidays
            as_of_date=date.today(),
        )

        tables = [
            DQTableConfig(
                exchange="EURONEXT",
                schema_name="raw",
                table_name="ams_hourly_archive",
                interval="hourly",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=17,        # change if your market-close convention differs
                expected_close_minute=00,
                end_tolerance_minutes=0,
                bar_threshold=expected_bar_count,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
            DQTableConfig(
                exchange="EURONEXT",
                schema_name="bronze",
                table_name="synced_working_ams_hourly",
                interval="hourly",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=17,
                expected_close_minute=00,
                end_tolerance_minutes=0,
                bar_threshold=expected_bar_count,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
            DQTableConfig(
                exchange="EURONEXT",
                schema_name="silver",
                table_name="indicators_ams_focus_dataset",
                interval="hourly",
                ts_col="TS",
                timestamp_col="TIMESTAMP",
                symbol_col="SYMBOL",
                row_id_col="ROW_ID",
                expected_close_hour=17,
                expected_close_minute=00,
                end_tolerance_minutes=0,
                bar_threshold=expected_bar_count,
                checks=(
                    "END_DATE_CHECK",
                    "NULL_CHECK",
                    "DUPLICATE_CHECK",
                    "BAR_CHECK",
                ),
            ),
        ]

        dq_run_id = dq.run_exchange_checks(dq_run_cfg, tables)
        print(f"[DQ] AMS completed. DQ_RUN_ID={dq_run_id}")
        

        if flags.dq_elemination:
            repo.update_focus_symbol_scope(
                exchange=exchange,
                compare_schema='logs',
                compare_table='dq_check_overview_ams',
                reason='DQ FAILED',
                main_symbol_schema = 'prod',
                main_symbol_table = 'FOCUS_SYMBOLS_ALL',
                drop_and_recreate = False
            )
    else:
        print("❌ [DQ] AMS skipped")


    # ============================================================================================
    # INDICATOR CHAPTER
    # ============================================================================================

    #--------------------------
    # IND-1) IND BAR STATUS CALC
    #--------------------------
    if flags.bar_status:
        print("[IND-BAR_STATUS] ams started for exchange={exchange}")

        svc = IndBarStatusService(repo=repo)
        svc.run(
            exchange = exchange,
            source_schema=flags.target_schema,
            source_table=flags.target_table,
            bs_target_schema='silver',
            bs_target_table='IND_BAR_STATUS',
            is_truncate_scope=True,
        )
        # adding elemination reason
        repo.update_focus_symbol_scope_filtered(
            exchange=exchange,
            compare_schema = 'silver',
            compare_table='IND_BAR_STATUS',
            comp_col='BAR_STATUS',
            comp_value='GREEN', # green olmayanlari scope disi birakacak!
            reason='bar status red | close is lower than open',
            main_symbol_schema = 'prod',
            main_symbol_table = 'FOCUS_SYMBOLS_ALL',
            drop_and_recreate = False
        )
    else:
        print(f"❌ [IND-BAR_STATUS] ams skipped for exchange={exchange}")
    
    #---------------------------------
    # IND-2) IND FRV_ POC VAL VAH CALC
    #---------------------------------
    
    if flags.run_frvp:
        print(f"[IND-FRVP] ams started ({exchange})...")

        svc = IndFrvPocProfileService(repo=repo)
        svc.run(
            exchange=exchange,
            periods=["2year", "1year", "6months", "4months"],
            frvp_source_schema='silver',
            frvp_source_table='indicators_ams_focus_dataset',
            frvp_target_schema='silver',
            frvp_target_table='IND_FRV_POC_PROFILE',
            cutt_off_date=None
            )
        
        #adding elemination reason
        repo.update_focus_symbol_scope_filtered(
            exchange=exchange,
            compare_schema = 'silver',
            compare_table='IND_FRV_POC_PROFILE',
            comp_col="IN_SCOPE_FOR_EMA_RSI",
            comp_value='True', # true olmayanlar scope disi kalacak demek unutma!
            reason='4 poc values are lower than latest close price',
            main_symbol_schema = 'prod',
            main_symbol_table = 'FOCUS_SYMBOLS_ALL',
            drop_and_recreate = False
        )
    else:
        print(f"❌ [IND-FRVP] ams skipped for exchange={exchange}")

    #---------------------------------
    # IND-3) run_convert_daily
    #---------------------------------
    if flags.run_convert_daily:
        print(f"[IND-CONVERT_DAILY] ams started ({exchange})...")

        stats = repo.build_converted_daily_for_ema_rsi_scope(
            exchange=exchange,
            interval='hourly',  
            start_trading_days_back=130,
            source_schema='silver',
            source_table='indicators_ams_focus_dataset',
            ts_col="TS",
            high_col="HIGH",
            target_schema=flags.converted_schema,
            target_table=flags.converted_table,#'converted_daily_dataset_ams',
        )
        print(
            f'[IND-CONVERT] Converted-daily built. exchange={stats["exchange"]} '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows={stats["after_rows"]} target={stats["target"]}',
            flush=True
        )
    else:
        print(f"❌ [IND-CONVERT] ams skipped for exchange={exchange}", flush=True)


    #---------------------------------
    # IND-4) EMA CALC
    #---------------------------------
    if flags.run_ema_ind:
        print(f"[IND-EMA] ams started ({exchange})...")

        svc = IndEmaFocusService(repo=repo)
        svc.run(
            exchange=exchange,
            input_schema=flags.converted_schema,
            input_table=flags.converted_table,
            ema_calc_history_days=120,
            ema_signal_lookback_days=20,
        )
        
    else:
        print('❌ [IND-EMA] ams skipped!')

    #---------------------------------
    # IND-5) WVAP CALC
    #---------------------------------
    if flags.run_vwap_ind:
        print(f"[IND-VWAP] ams started ({exchange})...")

        svc = IndVwapFocusService(repo=repo)
        svc.run(
            exchange=exchange,
            source_schema=flags.target_schema,
            source_table=flags.target_table,
            target_schema='silver',
            target_table='IND_VWAP_FOCUS',
            periods=["2year", "1year", "6months", "4months"],
            is_truncate_scope=True,
        )
        
    else:
        print('❌ [IND-VWAP] ams skipped!')

    #---------------------------------
    # IND-6) RSI CALC
    #---------------------------------
    if flags.run_rsi_ind:
        print(f"[IND-RSI] ams started ({exchange})...")

        svc = IndRsiFocusService(repo=repo)
        svc.run(
            exchange=exchange,
            input_schema=flags.converted_schema,
            input_table=flags.converted_table,
            rsi_calc_history_days=120,
            rsi_signal_lookback_days=20,
        )
    else:
        print('❌ [IND-RSI] ams skipped!')

    #---------------------------------
    # IND-7) MFI CALC
    #---------------------------------
    if flags.run_mfi_ind:
        print(f"[IND-MFI] ams started ({exchange})...")
        svc = IndMfiFocusService(repo=repo)
        
        svc.run(
            exchange=exchange,
            input_schema=flags.converted_schema,
            input_table=flags.converted_table,
            mfi_calc_history_days=120,
        )
        
    else:
        print('❌ [IND-MFI] ams skipped!')

    #---------------------------------
    # IND-8) MASTER IND FILE
    #---------------------------------
    
    if flags.run_combined_indicators:
        svc = IndMasterCombinedIndicatorsService(repo=repo)

        svc.run(
            exchange=exchange,
            target_schema='gold',
            target_table='ams_master_combined_indicators',
            log_schema='logs',
            log_table='log_ams_master_combined_indicators',

            frvp_table="IND_FRV_POC_PROFILE",
            bs_table="IND_BAR_STATUS",
            ema_table="IND_EMA_FOCUS",
            rsi_table="IND_RSI_FOCUS",
            mfi_table="IND_MFI_FOCUS",
            vwap_table="IND_VWAP_FOCUS",
        )

        # write to gg
        repo.fn_repo_write_to_google_generic(
            schema='gold',
            table='ams_master_combined_indicators',
            sheet_name= 'MASTER_IND_AMS',
            replace_append = os.getenv("MASTERFILE_APPEND_REPLACE"))
        
        # write to gg
        repo.fn_repo_write_to_google_generic(
            schema='silver',
            table='cloned_focus_symbol_list',
            sheet_name= 'ALL_SYMBOLS_STATUS',
            replace_append = 'replace')
        
        print(f"✅✅✅  [IND-MASTER] AMS DONE SUCCESFULLY! | exchange={exchange} ✅✅✅")
    else:
        print('❌ [MASTERFILE] skipped!')


    