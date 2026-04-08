from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.infrastructure.database.repository import PostgresRepository
from app.services.watch_dataset_build_service import (
    WatchDatasetBuildService,
    WatchDatasetBuildConfig,
)


@dataclass(frozen=True)
class WatchPipelineFlags:
    run_watch_ingestion: bool = True
    run_watch_calc_1: bool = False
    run_watch_calc_2: bool = False
    run_watch_calc_3: bool = False


async def run_watch_pipeline(
    repo: PostgresRepository,
    flags: WatchPipelineFlags,
    exchange: str,
    exc_name: str,
):
    exchange = exchange.upper().strip()
    exc_name = exc_name.lower().strip()

    print(
        f"\n►►►►►►►►►►►►[WATCH PIPELINE {exc_name.upper()}] started ◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎◀︎\n"
        + datetime.now().strftime("%d-%m-%Y %H:%M")
    )

    # ----------------------------------------------------------
    # 1) WATCH INGESTION
    # ----------------------------------------------------------
    if flags.run_watch_ingestion:
        print(f"[WATCH-INGESTION] {exchange} started...")

        svc = WatchDatasetBuildService(
            repo=repo,
            cfg=WatchDatasetBuildConfig(
                job_name=f"watch_{exc_name}_pipeline",
                exchange=exchange,
                source_schema="gold",
                source_table=f"{exc_name}_evaluation_master_score",
                source_where_sql=None,
                target_schema="prod",
                target_table=f"{exc_name}_watch_dataset",
                max_retries=3,
                retry_wait_seconds=3,
                tv_n_bars=120,
            ),
        )

        result = await svc.run(exchange=exchange)

        print(
            f"[WATCH-INGESTION] {exchange} completed. "
            f"symbol_count={result['symbol_count']} "
            f"inserted_row_count={result['inserted_row_count']} "
            f"failed_symbol_count={result['failed_symbol_count']}"
        )
    else:
        print(f"❌ [WATCH-INGESTION] {exchange} skipped!")

    # ----------------------------------------------------------
    # 2) WATCH CALC 1
    # ----------------------------------------------------------
    if flags.run_watch_calc_1:
        print(f"[WATCH-CALC-1] {exchange} started...")
        print(f"[WATCH-CALC-1] {exchange} completed.")
    else:
        print(f"❌ [WATCH-CALC-1] {exchange} skipped!")

    # ----------------------------------------------------------
    # 3) WATCH CALC 2
    # ----------------------------------------------------------
    if flags.run_watch_calc_2:
        print(f"[WATCH-CALC-2] {exchange} started...")
        print(f"[WATCH-CALC-2] {exchange} completed.")
    else:
        print(f"❌ [WATCH-CALC-2] {exchange} skipped!")

    # ----------------------------------------------------------
    # 4) WATCH CALC 3
    # ----------------------------------------------------------
    if flags.run_watch_calc_3:
        print(f"[WATCH-CALC-3] {exchange} started...")
        print(f"[WATCH-CALC-3] {exchange} completed.")
    else:
        print(f"❌ [WATCH-CALC-3] {exchange} skipped!")