from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import uuid
from app.services.dq_generic_service import DQGenericService, DQConfig # type: ignore

from app.infrastructure.database.repository import PostgresRepository

from app.infrastructure.api_clients.yahooquery_bist_daily_provider import (
    YahooQueryBistDailyProvider,
)

from app.infrastructure.api_clients.tvdatafeed_bist_daily_provider import (
    TvDatafeedBistDailyProvider,
    TvDatafeedBistDailyConfig,
)

from app.services.bist_daily_historical_ingestion_service import (
    BistDailyHistoricalIngestionService,
)

from app.services.bist_daily_historical_fallback_service import (
    BistDailyHistoricalFallbackService,
)


@dataclass(frozen=True)
class BistDailyDataPipelineFlags:

    ingest: bool = False
    fallback: bool = False

    sync_archive_to_working: bool = True
    interval: str = 'daily'
    sync_start_date: str = ''
    trim365: bool = False

    build_focus_dataset: bool = False

    use_db_last_timestamp: bool = False
    start_date: str = "2020-01-01"
    end_date: Optional[str] = None

    archive_schema: str = "raw"
    archive_table: str = "bist_daily_archive"

    working_schema: str = "bronze"
    working_table: str = "bist_daily_high_filtered"

    ts_col: str = "TS"

    safety_days: int = 1
    lookback_days: int = 365
    reference_days_ago: int = 1

    focus_schema: str = "silver"
    focus_table: str = "FRVP_BIST_FOCUS_DATASET"
    high_col: str = "HIGH"
    min_trading_days: int = 15

    dq: bool = True
    apply_dq_out_scope: bool = True # dq failed symbols will be out of scope for ema and further.False include, True exclude


async def run_bist_daily_data_pipeline(repo: PostgresRepository, flags: BistDailyDataPipelineFlags):

    print(
        "\n[BIST-DAILY] pipeline started... "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    failed_symbols: list[str] = []

    # ----------------------------------------------------------
    # 1) INGESTION (YahooQuery)
    # ----------------------------------------------------------

    if flags.ingest:
        repo.delete_recent_days_by_last_ts(schema='raw',table='bist_daily_archive',ts_col= "TS",days_back = 1)
        repo.delete_recent_days_by_last_ts(schema='bronze',table='bist_daily_high_filtered',ts_col= "TS",days_back = 1)


        provider = YahooQueryBistDailyProvider()

        svc = BistDailyHistoricalIngestionService(
            repo=repo,
            provider=provider,
        )

        await svc.run(
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )

        failed_symbols = getattr(svc, "permanently_failed_symbols", [])

    else:
        print("[BIST-DAILY] ingestion skipped")

    # ----------------------------------------------------------
    # 2) FALLBACK (tvDatafeed)
    # ----------------------------------------------------------

    if flags.fallback and failed_symbols:

        print(
            f"\n[BIST-DAILY-FB] tvDatafeed fallback started. "
            f"failed_symbols={len(failed_symbols)}\n"
        )

        tv_provider = TvDatafeedBistDailyProvider(
            TvDatafeedBistDailyConfig(
                username=os.environ["TV_USERNAME"],
                password=os.environ["TV_PASSWORD"],
            )
        )

        fb_svc = BistDailyHistoricalFallbackService(
            repo=repo,
            provider=tv_provider,
        )

        await fb_svc.run(
            symbols=failed_symbols,
            use_db_last_timestamp=flags.use_db_last_timestamp,
            start_date=flags.start_date,
            end_date=flags.end_date,
        )

        print("\n[BIST-DAILY-FB] fallback completed\n")

    elif flags.fallback:
        print("[BIST-DAILY-FB] fallback skipped (no failed symbols)")
    else:
        print("[BIST-DAILY-FB] fallback disabled")

    print(
        "\n[BIST-DAILY] archive update completed.. "
        + datetime.now().strftime("%d-%m-%Y %H:%M")
        + "\n"
    )

    # ----------------------------------------------------------
    # 3) SYNC raw → bronze
    # ----------------------------------------------------------

    if flags.sync_archive_to_working:

        print("\n[BIST-DAILY] Sync archive -> working started...\n")

        inserted = repo.sync_archive_to_working(
            archive_schema=flags.archive_schema,
            archive_table=flags.archive_table,
            working_schema=flags.working_schema,
            working_table=flags.working_table,
            ts_col=flags.ts_col,
            safety_days=flags.safety_days,
            interval="daily",
            sync_start_date="2024-03-05", # if daily, then truncate and take data from start_date
        )

        print(
            f"[BIST-DAILY] Sync completed. inserted_rows={inserted} "
            f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
        )

    else:
        print("[BIST-DAILY] sync skipped")

    # ----------------------------------------------------------
    # 4) TRIM
    # ----------------------------------------------------------

    if flags.trim365:

        before = repo.count_rows(
            schema=flags.working_schema,
            table=flags.working_table,
        )

        print(f"[BIST-DAILY] rows before trim: {before}")

        deleted = repo.trim_history_by_peak_or_lookback_ts(
            schema=flags.working_schema,
            table=flags.working_table,
            symbol_col="SYMBOL",
            ts_typed_col=flags.ts_col,
            high_col=flags.high_col,
            lookback_days=flags.lookback_days,
            reference_days_ago=flags.reference_days_ago,
        )

        after = repo.count_rows(
            schema=flags.working_schema,
            table=flags.working_table,
        )

        print(f"[BIST-DAILY] trim completed. deleted_rows={deleted}")
        print(f"[BIST-DAILY] rows after trim: {after}")

    else:
        print("[BIST-DAILY] trim skipped")

    # ----------------------------------------------------------
    # 5) BUILD FOCUS DATASET
    # ----------------------------------------------------------

    if flags.build_focus_dataset:

        stats = repo.build_frvp_focus_dataset(
            source_schema=flags.working_schema,
            source_table=flags.working_table,
            target_schema=flags.focus_schema,
            target_table=flags.focus_table,
            ts_col=flags.ts_col,
            high_col=flags.high_col,
            exchange="BIST",
            min_trading_days=flags.min_trading_days,
        )

        print(
            f'[BIST-DAILY] Focus dataset built. '
            f'symbols: {stats["before_symbols"]} -> {stats["after_symbols"]}, '
            f'rows: {stats["before_rows"]} -> {stats["after_rows"]} '
            f'{datetime.now().strftime("%d-%m-%Y %H:%M")}'
        )

    else:
        print("[BIST-DAILY] focus dataset build skipped")

    print(
        f"\n[BIST-DAILY] pipeline finished. "
        f"{datetime.now().strftime('%d-%m-%Y %H:%M')}\n"
    )

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
            interval="daily",
            ts_col="TS",  # if exists; otherwise "TIMESTAMP"
            columns=cols,
        )
        print(f"[DQ - BIST] Completed. run_id={run_id}")
        
        if flags.apply_dq_out_scope:
            repo.apply_dq_to_poc_profile(reset_in_scope=True)  # or False if you don't want to reset IN_SCOPE  # or False if you don't want to reset IN_SCOPE