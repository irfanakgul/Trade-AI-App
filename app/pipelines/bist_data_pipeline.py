import os
from dataclasses import dataclass
from typing import Optional

from app.infrastructure.api_clients.yahooquery_bist_provider import YahooQueryBistProvider
from app.infrastructure.api_clients.tvdatafeed_bist_provider import TvDatafeedBistProvider, TvDatafeedBistConfig
from app.services.bist_historical_ingestion_service import BistHistoricalIngestionService
from app.services.bist_historical_fallback_service import BistHistoricalFallbackService
from app.infrastructure.database.repository import PostgresRepository


@dataclass(frozen=True)
class BistDataPipelineFlags:
    ingest: bool = True
    fallback: bool = True
    sync_archive_to_working: bool = True
    trim365: bool = True
    build_focus_dataset: bool = True

    use_db_last_timestamp: bool = True
    start_date: str = "2026-01-01"
    end_date: Optional[str] = None

    safety_days: int = 1
    lookback_days: int = 365
    reference_days_ago: int = 1
    min_trading_days: int = 15


async def run_bist_data_pipeline(repo: PostgresRepository, flags: BistDataPipelineFlags) -> None:
    print("\n[BIST] Data pipeline started...\n")

    failed_bist: list[str] = []

    # 1) Ingestion (yahooquery)
    if flags.ingest:
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
    elif flags.fallback:
        print("[BIST-FB] fallback skipped (no failed symbols)")
    else:
        print("[BIST-FB] fallback disabled")

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
        print(f"[BIST] Sync completed. inserted_rows={ins_bist}\n")
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
        print(f"[BIST] trim completed. deleted_rows={deleted_bist}")

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
            f'rows: {stats_bist["before_rows"]} -> {stats_bist["after_rows"]}'
        )
    else:
        print("[BIST] focus dataset build skipped")

    print("\n[BIST] Data pipeline finished.\n")