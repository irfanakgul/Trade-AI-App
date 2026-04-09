from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from app.services.telegram_bot_chat_service import telegram_send_message  # type: ignore

from app.infrastructure.database.repository import PostgresRepository
from app.services.watch_dataset_build_service import (
    WatchDatasetBuildService,
    WatchDatasetBuildConfig,
)

from app.services.watch_signal_check_service import (
    WatchSignalCheckService,
    WatchSignalCheckConfig,
)

@dataclass(frozen=True)
class WatchPipelineFlags:
    run_watch_ingestion: bool = True
    run_watch_signal_check: bool = False
    run_watch_calc_2: bool = False
    run_watch_calc_3: bool = False
    send_telegram_buy_signal: bool = False


async def run_watch_pipeline(
    repo: PostgresRepository,
    flags: WatchPipelineFlags,
    exchange: str,
    exc_name: str,
    signal_open_hour: int,
    signal_open_minute: int,
    signal_close_hour: int,
    signal_close_minute: int,
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
                tv_n_bars=1000,
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
    # 2) BUY SIGNAL CHECK 
    # ----------------------------------------------------------
    if flags.run_watch_signal_check:
        print(f"[SIGNAL-CHECK] {exchange} started...")

        svc = WatchSignalCheckService(
            repo=repo,
            cfg=WatchSignalCheckConfig(
                job_name=f"watch_signal_check_{exc_name}",
            ),
        )

        result = svc.run(
            exchange=exchange,
            input_schema="prod",
            input_table=f"{exc_name}_watch_dataset",
            output_schema="prod",
            output_table=f"watch_signal_check_{exc_name}",
            log_schema="logs",
            log_table="watch_signal_check_all",
            open_hour=signal_open_hour,
            open_minute=signal_open_minute,
            close_hour=signal_close_hour,
            close_minute=signal_close_minute,
            is_truncate_output=True,
        )

        if flags.send_telegram_buy_signal and result["telegram_text"]:
            telegram_send_message(
                title=f"💸 BUY SIGNAL: {exc_name.upper()}",
                text=result["telegram_text"],
                channel='trades'
            )

        print(
            f"[BUY-SIGNAL] {exchange} completed. "
            f"deleted_output_rows={result['deleted_output_rows']} "
            f"inserted_output_rows={result['inserted_output_rows']} "
            f"inserted_log_rows={result['inserted_log_rows']} "
            f"buy_count={result['buy_count']} "
            f"total_rows={result['total_rows']}"
        )
    else:
        print(f"❌ [SIGNAL-CHECK] {exchange} skipped!")

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