from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import floor
from zoneinfo import ZoneInfo

from config.device import get_runtime_device_id
from config.exchange_loader import get_exchange_config
from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.ibkr_connection_service import IbkrConnectionService
from service.ibkr_order_service import IbkrOrderService
from service.log_service import LogEvent, LogService
from service.position_runtime_service import PositionRuntimeService
from service.scope_service import ScopeService
from service.signal_service import SignalService
from service.trade_log_service import TradeLogService


@dataclass
class BuyExecutionPipelineResult:
    SUCCESS: bool
    MESSAGE: str
    EXCHANGE: str
    SIGNAL_COUNT: int = 0
    PLANNED_BUY_COUNT: int = 0
    EXECUTED_BUY_COUNT: int = 0
    SKIPPED_SCOPE_COUNT: int = 0
    SKIPPED_ALREADY_OPEN_COUNT: int = 0
    SKIPPED_SAME_DAY_COUNT: int = 0
    SKIPPED_BUDGET_COUNT: int = 0


def run_buy_execution_pipeline(
    settings: AppSettings,
    exchange_code: str,
) -> BuyExecutionPipelineResult:
    settings.DEVICE_ID = get_runtime_device_id(settings.USERNAME)

    now = datetime.now(ZoneInfo(settings.APP_TIMEZONE))
    trade_date = now.date()
    exchange_config = get_exchange_config(exchange_code)

    repository = Repository(create_db_engine(settings))
    log_service = LogService(settings)
    signal_service = SignalService(settings)
    scope_service = ScopeService(settings)
    trade_log_service = TradeLogService(settings)
    position_runtime_service = PositionRuntimeService(settings)

    executed_buy_count = 0
    skipped_scope_count = 0
    skipped_already_open_count = 0
    skipped_same_day_count = 0
    skipped_budget_count = 0

    print("-" * 60)
    print(f"BUY EXECUTION PIPELINE START | EXCHANGE={exchange_code}")
    print(f"TRADE_DATE               : {trade_date.isoformat()}")

    log_service.log(
        LogEvent(
            EVENT_TYPE="BUY_EXECUTION",
            EVENT_STATUS="STARTED",
            MESSAGE="Buy execution pipeline started.",
            PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
            EXCHANGE=exchange_code,
            DETAILS_JSON={
                "TRADE_DATE": trade_date.isoformat(),
            },
        )
    )

    try:
        buy_limits = repository.get_buy_limits(
            USERNAME=settings.USERNAME,
            TRADE_DATE=trade_date,
            EXCHANGE=exchange_code,
        )

        if not buy_limits:
            raise RuntimeError("buy_limits row not found. Run BUY_PREPARE first.")

        if not bool(buy_limits["IS_ENABLED"]):
            message = "Buy execution skipped because buy_limits is disabled."
            return BuyExecutionPipelineResult(
                SUCCESS=True,
                MESSAGE=message,
                EXCHANGE=exchange_code,
                PLANNED_BUY_COUNT=int(buy_limits["PLANNED_BUY_COUNT"] or 0),
            )

        planned_buy_count = int(buy_limits["PLANNED_BUY_COUNT"] or 0)
        slot_budget_amount = float(buy_limits["SLOT_BUDGET_AMOUNT"] or 0.0)

        if planned_buy_count <= 0:
            message = "Buy execution skipped because planned buy count is 0."
            return BuyExecutionPipelineResult(
                SUCCESS=True,
                MESSAGE=message,
                EXCHANGE=exchange_code,
                PLANNED_BUY_COUNT=planned_buy_count,
            )

        signal_result = signal_service.read_latest_signals_for_execution_exchange(exchange_code)
        if not signal_result.SUCCESS:
            raise RuntimeError(signal_result.MESSAGE)

        if signal_result.SIGNAL_COUNT <= 0:
            message = "Buy execution completed successfully. No signals found."
            return BuyExecutionPipelineResult(
                SUCCESS=True,
                MESSAGE=message,
                EXCHANGE=exchange_code,
                SIGNAL_COUNT=signal_result.SIGNAL_COUNT,
                PLANNED_BUY_COUNT=planned_buy_count,
                EXECUTED_BUY_COUNT=0,
                SKIPPED_SCOPE_COUNT=0,
                SKIPPED_ALREADY_OPEN_COUNT=0,
                SKIPPED_SAME_DAY_COUNT=0,
                SKIPPED_BUDGET_COUNT=0,
            )

        connection_service = IbkrConnectionService(settings)
        connect_result = connection_service.connect(readonly=False)
        if not connect_result.SUCCESS:
            raise RuntimeError(f"IBKR connection failed: {connect_result.MESSAGE}")

        try:
            ib_client = connection_service.get_client()
            order_service = IbkrOrderService(ib_client)

            for signal in signal_result.SIGNALS:
                if executed_buy_count >= planned_buy_count:
                    break

                symbol = signal.SYMBOL
                storage_exchange = signal.EXCHANGE

                log_service.log(
                    LogEvent(
                        EVENT_TYPE="BUY_EXECUTION",
                        EVENT_STATUS="INFO",
                        MESSAGE="Evaluating buy candidate.",
                        PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                        EXCHANGE=storage_exchange,
                        SYMBOL=symbol,
                        DETAILS_JSON={
                            "RANK": signal.RANK,
                            "TARGET_PRICE": signal.TARGET_PRICE,
                            "SLOT_BUDGET_AMOUNT": slot_budget_amount,
                        },
                    )
                )

                scope_result = scope_service.is_symbol_in_scope(
                    exchange=storage_exchange,
                    symbol=symbol,
                )
                if not scope_result.SUCCESS or not scope_result.USER_IN_SCOPE:
                    skipped_scope_count += 1
                    continue

                same_day_buy_exists = repository.exists_same_day_trade_log(
                    USERNAME=settings.USERNAME,
                    EXCHANGE=storage_exchange,
                    SYMBOL=symbol,
                    ACTION="BUY",
                    TRADE_DATE=trade_date.isoformat(),
                )
                if same_day_buy_exists:
                    skipped_same_day_count += 1

                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="BUY_EXECUTION",
                            EVENT_STATUS="WARNING",
                            MESSAGE="Symbol skipped because same-day buy already exists.",
                            PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                            EXCHANGE=storage_exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={},
                        )
                    )
                    continue

                runtime_row = repository.get_user_position_runtime(
                    USERNAME=settings.USERNAME,
                    EXCHANGE=storage_exchange,
                    SYMBOL=symbol,
                )

                if runtime_row and bool(runtime_row["IS_OPEN"]):
                    skipped_already_open_count += 1

                    position_runtime_service.apply_signal_refresh_plus_one(
                        EXCHANGE=storage_exchange,
                        SYMBOL=symbol,
                        SIGNAL_DATE=trade_date,
                        FORCED_SELL_MODE=exchange_config.FORCED_SELL_MODE,
                    )

                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="BUY_EXECUTION",
                            EVENT_STATUS="INFO",
                            MESSAGE="Symbol already open. Applied holding +1 refresh and skipped buy.",
                            PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                            EXCHANGE=storage_exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={},
                        )
                    )
                    continue

                market_price = order_service.get_market_price(
                    symbol=symbol,
                    exchange=storage_exchange,
                    currency=exchange_config.CURRENCY,
                )

                if market_price is None or market_price <= 0:
                    skipped_budget_count += 1

                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="BUY_EXECUTION",
                            EVENT_STATUS="WARNING",
                            MESSAGE="Symbol skipped because market price could not be resolved.",
                            PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                            EXCHANGE=storage_exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={},
                        )
                    )
                    continue

                quantity = floor(slot_budget_amount / market_price)
                if quantity <= 0:
                    skipped_budget_count += 1

                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="BUY_EXECUTION",
                            EVENT_STATUS="WARNING",
                            MESSAGE="Symbol skipped because calculated quantity is 0.",
                            PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                            EXCHANGE=storage_exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={
                                "SLOT_BUDGET_AMOUNT": slot_budget_amount,
                                "MARKET_PRICE": market_price,
                            },
                        )
                    )
                    continue

                buy_result = order_service.place_market_buy(
                    symbol=symbol,
                    exchange=storage_exchange,
                    currency=exchange_config.CURRENCY,
                    quantity=quantity,
                )

                if not buy_result.SUCCESS:
                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="BUY_ORDER",
                            EVENT_STATUS="FAILED",
                            MESSAGE=buy_result.MESSAGE,
                            PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                            EXCHANGE=storage_exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={
                                "QUANTITY": quantity,
                            },
                        )
                    )
                    continue

                filled_qty = float(buy_result.FILLED_QTY or quantity)
                avg_fill_price = float(buy_result.AVG_FILL_PRICE or market_price)

                oca_group = order_service.build_oca_group()

                trailing_result = order_service.place_trailing_stop_sell(
                    symbol=symbol,
                    exchange=storage_exchange,
                    currency=exchange_config.CURRENCY,
                    quantity=filled_qty,
                    trailing_percent=exchange_config.STOP_LOSS_PCT,
                    oca_group=oca_group,
                )

                if settings.EXIT_MODE == "TARGET_PRICE" and signal.TARGET_PRICE:
                    order_service.place_target_limit_sell(
                        symbol=symbol,
                        exchange=storage_exchange,
                        currency=exchange_config.CURRENCY,
                        quantity=filled_qty,
                        target_price=float(signal.TARGET_PRICE),
                        oca_group=oca_group,
                    )

                trade_log_service.write_trade(
                    EXCHANGE=storage_exchange,
                    SYMBOL=symbol,
                    ACTION="BUY",
                    QUANTITY=filled_qty,
                    PRICE=avg_fill_price,
                    TRADE_SOURCE="SYSTEM",
                    DETECTION_SOURCE="LIVE",
                    TRADE_TIME=now,
                    ORDER_ID=str(buy_result.ORDER_ID) if buy_result.ORDER_ID is not None else None,
                    CLIENT_ORDER_ID=None,
                    NOTES=f"BUY_EXECUTION_PIPELINE | EXIT_MODE={settings.EXIT_MODE}",
                )

                position_runtime_service.open_new_position(
                    EXCHANGE=storage_exchange,
                    SYMBOL=symbol,
                    ENTRY_DATE=trade_date,
                    MAX_HOLDING_DAY=int(exchange_config.MAX_HOLDING_DAY),
                    LAST_PRICE=avg_fill_price,
                )

                executed_buy_count += 1

                log_service.log(
                    LogEvent(
                        EVENT_TYPE="BUY_ORDER",
                        EVENT_STATUS="SUCCESS",
                        MESSAGE="Buy order executed successfully.",
                        PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                        EXCHANGE=storage_exchange,
                        SYMBOL=symbol,
                        DETAILS_JSON={
                            "QUANTITY": filled_qty,
                            "AVG_FILL_PRICE": avg_fill_price,
                            "TRAILING_STOP_ORDER_SUCCESS": trailing_result.SUCCESS,
                            "TARGET_PRICE": signal.TARGET_PRICE,
                        },
                    )
                )

        finally:
            connection_service.disconnect()

        message = "Buy execution pipeline completed successfully."

        log_service.log(
            LogEvent(
                EVENT_TYPE="BUY_EXECUTION",
                EVENT_STATUS="SUCCESS",
                MESSAGE=message,
                PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                EXCHANGE=exchange_code,
                DETAILS_JSON={
                    "SIGNAL_COUNT": signal_result.SIGNAL_COUNT,
                    "PLANNED_BUY_COUNT": planned_buy_count,
                    "EXECUTED_BUY_COUNT": executed_buy_count,
                    "SKIPPED_SCOPE_COUNT": skipped_scope_count,
                    "SKIPPED_ALREADY_OPEN_COUNT": skipped_already_open_count,
                    "SKIPPED_SAME_DAY_COUNT": skipped_same_day_count,
                    "SKIPPED_BUDGET_COUNT": skipped_budget_count,
                },
            )
        )

        return BuyExecutionPipelineResult(
            SUCCESS=True,
            MESSAGE=message,
            EXCHANGE=exchange_code,
            SIGNAL_COUNT=signal_result.SIGNAL_COUNT,
            PLANNED_BUY_COUNT=planned_buy_count,
            EXECUTED_BUY_COUNT=executed_buy_count,
            SKIPPED_SCOPE_COUNT=skipped_scope_count,
            SKIPPED_ALREADY_OPEN_COUNT=skipped_already_open_count,
            SKIPPED_SAME_DAY_COUNT=skipped_same_day_count,
            SKIPPED_BUDGET_COUNT=skipped_budget_count,
        )

    except Exception as exc:
        message = f"Buy execution pipeline failed: {exc}"

        log_service.log(
            LogEvent(
                EVENT_TYPE="BUY_EXECUTION",
                EVENT_STATUS="FAILED",
                MESSAGE=message,
                PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                EXCHANGE=exchange_code,
                DETAILS_JSON={
                    "ERROR": str(exc),
                },
            )
        )

        return BuyExecutionPipelineResult(
            SUCCESS=False,
            MESSAGE=message,
            EXCHANGE=exchange_code,
            EXECUTED_BUY_COUNT=executed_buy_count,
            SKIPPED_SCOPE_COUNT=skipped_scope_count,
            SKIPPED_ALREADY_OPEN_COUNT=skipped_already_open_count,
            SKIPPED_SAME_DAY_COUNT=skipped_same_day_count,
            SKIPPED_BUDGET_COUNT=skipped_budget_count,
        )

    finally:
        print(f"BUY EXECUTION PIPELINE END | EXCHANGE={exchange_code}")
        print("-" * 60)