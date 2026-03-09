import os
from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from app.services.dq_generic_service import DQGenericService, DQConfig
import uuid


from app.infrastructure.api_clients.yahooquery_bist_provider import YahooQueryBistProvider
from app.infrastructure.api_clients.tvdatafeed_bist_provider import TvDatafeedBistProvider, TvDatafeedBistConfig
from app.services.bist_historical_ingestion_service import BistHistoricalIngestionService
from app.services.bist_historical_fallback_service import BistHistoricalFallbackService
from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class BistDataPipelineFlags:
    ingest: bool = True
    fallback: bool = True
    sync_archive_to_working: bool = False
    trim365: bool = False
    build_focus_dataset: bool = False
    dq: bool = False
    apply_dq_out_scope: bool = False # dq failed symbols will be out of scope for ema and further.False include, True exclude

    use_db_last_timestamp: bool = True
    start_date: str = "2025-02-01"
    end_date: Optional[str] = None

    safety_days: int = 1
    lookback_days: int = 365
    reference_days_ago: int = 1
    min_trading_days: int = 15

    #auto sample flags
    auto_sample_run: bool = True
    smpl_source_schema: str ="raw"
    smpl_source_table: str ="bist_1min_archive"
    smpl_target_schema: str ="test"
    smpl_target_table: str ="sample_bist_1min"
    smpl_symbol_col:str = "SYMBOL"
    smpl_ts_col:str = "TS"
    smpl_trading_days_back:int = 30


async def run_bist_data_pipeline(repo: PostgresRepository, flags: BistDataPipelineFlags) -> None:

    print(
        "\n[BIST] Data pipeline started... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    failed_bist: list[str] = []

    # 1) Ingestion (yahooquery)
    if flags.ingest:
        #remove last n days data from raw/bronze tables
        repo.delete_recent_days_by_last_ts(schema='raw',table='bist_1min_archive',ts_col= "TS",days_back = 1)
        repo.delete_recent_days_by_last_ts(schema='bronze',table='bist_1min_tv_past',ts_col= "TS",days_back = 1)

        bist_primary_provider = YahooQueryBistProvider()
        bist_svc = BistHistoricalIngestionService(repo=repo, provider=bist_primary_provider)

        await bist_svc.run(
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )

        failed_bist = getattr(bist_svc, "permanently_failed_symbols", [])
    else:
        print("[BIST] ingest skipped")

    # 2) Fallback (tvDatafeed)
    if flags.fallback and failed_bist:
        print(f"\n[BIST-FB] tvDatafeed fallback started. failed_symbols={len(failed_bist)}\n")

        tv_provider = TvDatafeedBistProvider(
            TvDatafeedBistConfig(
                username=os.environ["TV_USERNAME"],
                password=os.environ["TV_PASSWORD"],
            )
        )
        bist_fb_svc = BistHistoricalFallbackService(repo=repo, provider=tv_provider)

        await bist_fb_svc.run(
            symbols=failed_bist,
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )

        print("\n[BIST-FB] fallback completed\n")
        print(
            "\n[BIST] Data update completed.. "
            + datetime.now().strftime("%d-%m-%Y %H:%M")
            + "\n")
    elif flags.fallback:
        print("[BIST-FB] fallback skipped (no failed symbols)")
        print(
            "\n[BIST] Data update completed.. "
            + datetime.now().strftime("%d-%m-%Y %H:%M")
            + "\n")
    else:
        print("[BIST-FB] fallback disabled")
        print(
            "\n[BIST] Data update completed.. "
            + datetime.now().strftime("%d-%m-%Y %H:%M")
            + "\n")

    

    # 3) Sync raw -> bronze working
    if flags.sync_archive_to_working:
        print("\n[BIST] Sync archive -> working started...\n")
        ins_bist = repo.sync_archive_to_working(
            archive_schema="raw",
            archive_table="bist_1min_archive",
            working_schema="bronze",
            working_table="bist_1min_tv_past",
            ts_col="TS",
            safety_days=flags.safety_days,
        )
        print(
            f"[BIST] Sync completed. inserted_rows={ins_bist} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
        )
    else:
        print("[BIST] sync skipped")

    # 4) Trim
    if flags.trim365:
        before_bist = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
        print(f"[BIST] rows before trim: {before_bist}")

        deleted_bist = repo.trim_history_by_peak_or_lookback_ts(
            schema="bronze",
            table="bist_1min_tv_past",
            symbol_col="SYMBOL",
            ts_typed_col="TS",
            high_col="HIGH",
            lookback_days=flags.lookback_days,
            reference_days_ago=flags.reference_days_ago,
        )
        print(
            f"[BIST] trim365 completed. deleted_rows={deleted_bist} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}"
        )

        after_bist = repo.count_rows(schema="bronze", table="bist_1min_tv_past")
        print(f"[BIST] rows after trim: {after_bist}")
    else:
        print("[BIST] trim skipped")


    # 5) Focus dataset
    if flags.build_focus_dataset:
        stats_bist = repo.build_frvp_focus_dataset(
            source_schema="bronze",
            source_table="bist_1min_tv_past",
            target_schema="silver",
            target_table="FRVP_BIST_FOCUS_DATASET",
            ts_col="TS",
            high_col="HIGH",
            exchange="BIST",
            min_trading_days=flags.min_trading_days,
        )
        print(
            f'[BIST] Focus dataset built. '
            f'symbols: {stats_bist["before_symbols"]} -> {stats_bist["after_symbols"]}, '
            f'rows: {stats_bist["before_rows"]} -> {stats_bist["after_rows"]} '
            f'{datetime.now().strftime("%d-%m-%Y %H:%M")}'
        )
    else:
        print("[BIST] focus dataset build skipped")

    print(
        f"\n[BIST] Data pipeline finished. "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
    )
    # ----------------------------------------------------------
    # 6) SAMPLE AUTO DATASET
    # ----------------------------------------------------------

    if flags.auto_sample_run:
        symbols = os.getenv("BIST_SAMPLE_SYMBOLS", "")
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
        print(f'[SAMPLE-BIST-1min] | Sample symbols {len(symbols)} > {symbols}')

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
        print(f'⏭️[SAMPLE-BIST-1min] SKIPPED!')


    # ----------------------------------------------------------
    # 7) DQ CHECKS
    # ----------------------------------------------------------
    if flags.dq:
        run_id = uuid.uuid4()
        deleted = repo.clear_dq_for_exchange(schema="logs", table="DQ_generic_check", exchange="BIST")
        print(f"[DQ] cleared previous DQ logs for BIST. deleted_rows={deleted}")


        dq = DQGenericService(repo=repo, config=DQConfig(job_name="bist_daily_data_pipeline"))
        dq.truncate_logs()

        cols = ["SYMBOL", "TIMESTAMP", "OPEN", "LOW", "HIGH", "CLOSE", "VOLUME"]

        # BIST focus
        dq.run_for_table(
            run_id=run_id,
            exchange="BIST",
            schema="silver",
            table="FRVP_BIST_FOCUS_DATASET",
            interval="1min",
            ts_col="TS",  # if exists; otherwise "TIMESTAMP"
            columns=cols,
        )
        print(f"[DQ - BIST] Completed. run_id={run_id}")

        if flags.apply_dq_out_scope:
            repo.apply_dq_to_poc_profile(reset_in_scope=True)  # or False if you don't want to reset IN_SCOPE