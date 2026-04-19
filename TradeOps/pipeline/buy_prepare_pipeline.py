from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.device import get_runtime_device_id
from config.exchange_loader import get_enabled_exchanges
from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.budget_service import (
    BudgetService,
    ExchangeAllocationInput,
)
from service.log_service import LogEvent, LogService


@dataclass
class BuyPreparePipelineResult:
    SUCCESS: bool
    MESSAGE: str
    TRADE_DATE: str | None = None
    AVAILABLE_FUNDS: float | None = None
    GLOBAL_CURRENT_OPEN_POSITION_COUNT: int | None = None
    GLOBAL_REMAINING_OPEN_POSITION_SLOTS: int | None = None
    TODAY_BUY_COUNT_USED: int | None = None
    TODAY_BUY_COUNT_REMAINING: int | None = None
    TOTAL_PLANNED_BUY_COUNT: int | None = None
    EXCHANGE_PLANNED_SUMMARY: dict | None = None


def run_buy_prepare_pipeline(settings: AppSettings) -> BuyPreparePipelineResult:
    settings.DEVICE_ID = get_runtime_device_id(settings.USERNAME)
    now = datetime.now(ZoneInfo(settings.APP_TIMEZONE))
    trade_date = now.date()

    engine = create_db_engine(settings)
    repository = Repository(engine)
    log_service = LogService(settings)
    budget_service = BudgetService()

    print("-" * 60)
    print("BUY PREPARE PIPELINE START")
    print(f"TRADE_DATE               : {trade_date.isoformat()}")

    log_service.log(
        LogEvent(
            EVENT_TYPE="PIPELINE_RUN",
            EVENT_STATUS="STARTED",
            MESSAGE="Buy prepare pipeline started.",
            PIPELINE_NAME="BUY_PREPARE_PIPELINE",
            DETAILS_JSON={
                "TRADE_DATE": trade_date.isoformat(),
            },
        )
    )

    try:
        actual_wallet = repository.get_user_actual_wallet(settings.USERNAME)
        if not actual_wallet:
            raise RuntimeError("Actual wallet record not found. Run wallet snapshot first.")

        available_funds = float(actual_wallet.get("AVAILABLE_FUNDS") or 0.0)
        global_current_open_position_count = int(actual_wallet.get("OPEN_POSITION_COUNT") or 0)

        today_buy_count_used = repository.get_today_buy_count_used(
            USERNAME=settings.USERNAME,
            TRADE_DATE=trade_date.isoformat(),
        )

        enabled_exchanges = get_enabled_exchanges()

        exchange_inputs: list[ExchangeAllocationInput] = []
        for exchange_config in enabled_exchanges:
            current_open_position_count = repository.get_latest_open_position_count_by_exchange(
                USERNAME=settings.USERNAME,
                EXCHANGE=exchange_config.EXCHANGE,
            )

            exchange_inputs.append(
                ExchangeAllocationInput(
                    EXCHANGE=exchange_config.EXCHANGE,
                    EXCHANGE_MAX_OPEN_POSITIONS=exchange_config.MAX_OPEN_POSITIONS,
                    CURRENT_OPEN_POSITION_COUNT=current_open_position_count,
                    ALLOCATED_BUDGET_PCT=exchange_config.BUDGET_PCT,
                )
            )

        calculation = budget_service.calculate_global_buy_preparation(
            TOTAL_MAX_OPEN_POSITIONS=settings.TOTAL_MAX_OPEN_POSITIONS,
            MAX_DAILY_TRADE_COUNT=settings.MAX_DAILY_TRADE_COUNT,
            GLOBAL_CURRENT_OPEN_POSITION_COUNT=global_current_open_position_count,
            TODAY_BUY_COUNT_USED=today_buy_count_used,
            AVAILABLE_FUNDS=available_funds,
            EXCHANGE_INPUTS=exchange_inputs,
            EXCHANGE_PRIORITY=settings.EXCHANGE_PRIORITY,
        )

        exchange_planned_summary: dict[str, int] = {}

        for exchange_result in calculation.EXCHANGE_RESULTS:
            exchange_planned_summary[exchange_result.EXCHANGE] = exchange_result.PLANNED_BUY_COUNT

            repository.upsert_buy_limits(
                USERNAME=settings.USERNAME,
                DEVICE_ID=settings.DEVICE_ID,
                IBKR_MODE=settings.IBKR_MODE,
                TRADE_DATE=trade_date,
                EXCHANGE=exchange_result.EXCHANGE,
                TOTAL_MAX_OPEN_POSITIONS=calculation.TOTAL_MAX_OPEN_POSITIONS,
                MAX_DAILY_TRADE_COUNT=calculation.MAX_DAILY_TRADE_COUNT,
                EXCHANGE_MAX_OPEN_POSITIONS=exchange_result.EXCHANGE_MAX_OPEN_POSITIONS,
                CURRENT_OPEN_POSITION_COUNT=exchange_result.CURRENT_OPEN_POSITION_COUNT,
                REMAINING_OPEN_POSITION_SLOTS=exchange_result.REMAINING_OPEN_POSITION_SLOTS,
                TODAY_BUY_COUNT_USED=calculation.TODAY_BUY_COUNT_USED,
                TODAY_BUY_COUNT_REMAINING=calculation.TODAY_BUY_COUNT_REMAINING,
                AVAILABLE_FUNDS=calculation.AVAILABLE_FUNDS,
                ALLOCATED_BUDGET_PCT=exchange_result.ALLOCATED_BUDGET_PCT,
                ALLOCATED_BUDGET_AMOUNT=exchange_result.ALLOCATED_BUDGET_AMOUNT,
                SLOT_BUDGET_AMOUNT=exchange_result.SLOT_BUDGET_AMOUNT,
                PLANNED_BUY_COUNT=exchange_result.PLANNED_BUY_COUNT,
                IS_ENABLED=exchange_result.IS_ENABLED,
                PREPARED_AT=now,
                UPDATED_AT=now,
            )

        total_planned_buy_count = sum(exchange_planned_summary.values())

        print("[BUY PREPARE] buy_limits updated successfully")
        print(f"[BUY PREPARE] TOTAL_PLANNED_BUY_COUNT : {total_planned_buy_count}")
        print(f"[BUY PREPARE] EXCHANGE_PLANNED_SUMMARY: {exchange_planned_summary}")

        log_service.log(
            LogEvent(
                EVENT_TYPE="PIPELINE_RUN",
                EVENT_STATUS="SUCCESS",
                MESSAGE="Buy prepare pipeline completed successfully.",
                PIPELINE_NAME="BUY_PREPARE_PIPELINE",
                DETAILS_JSON={
                    "TRADE_DATE": trade_date.isoformat(),
                    "AVAILABLE_FUNDS": calculation.AVAILABLE_FUNDS,
                    "GLOBAL_CURRENT_OPEN_POSITION_COUNT": calculation.GLOBAL_CURRENT_OPEN_POSITION_COUNT,
                    "GLOBAL_REMAINING_OPEN_POSITION_SLOTS": calculation.GLOBAL_REMAINING_OPEN_POSITION_SLOTS,
                    "TODAY_BUY_COUNT_USED": calculation.TODAY_BUY_COUNT_USED,
                    "TODAY_BUY_COUNT_REMAINING": calculation.TODAY_BUY_COUNT_REMAINING,
                    "TOTAL_PLANNED_BUY_COUNT": total_planned_buy_count,
                    "EXCHANGE_PLANNED_SUMMARY": exchange_planned_summary,
                },
            )
        )

        return BuyPreparePipelineResult(
            SUCCESS=True,
            MESSAGE="Buy prepare pipeline completed successfully.",
            TRADE_DATE=trade_date.isoformat(),
            AVAILABLE_FUNDS=calculation.AVAILABLE_FUNDS,
            GLOBAL_CURRENT_OPEN_POSITION_COUNT=calculation.GLOBAL_CURRENT_OPEN_POSITION_COUNT,
            GLOBAL_REMAINING_OPEN_POSITION_SLOTS=calculation.GLOBAL_REMAINING_OPEN_POSITION_SLOTS,
            TODAY_BUY_COUNT_USED=calculation.TODAY_BUY_COUNT_USED,
            TODAY_BUY_COUNT_REMAINING=calculation.TODAY_BUY_COUNT_REMAINING,
            TOTAL_PLANNED_BUY_COUNT=total_planned_buy_count,
            EXCHANGE_PLANNED_SUMMARY=exchange_planned_summary,
        )

    except Exception as exc:
        log_service.log(
            LogEvent(
                EVENT_TYPE="PIPELINE_RUN",
                EVENT_STATUS="FAILED",
                MESSAGE=f"Buy prepare pipeline failed: {exc}",
                PIPELINE_NAME="BUY_PREPARE_PIPELINE",
                DETAILS_JSON={
                    "TRADE_DATE": trade_date.isoformat(),
                    "ERROR": str(exc),
                },
            )
        )

        return BuyPreparePipelineResult(
            SUCCESS=False,
            MESSAGE=f"Buy prepare pipeline failed: {exc}",
            TRADE_DATE=trade_date.isoformat(),
        )

    finally:
        print("BUY PREPARE PIPELINE END")
        print("-" * 60)