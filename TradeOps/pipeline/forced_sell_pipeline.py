from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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
from service.trade_log_service import TradeLogService


@dataclass
class ForcedSellPipelineResult:
    SUCCESS: bool
    MESSAGE: str
    EXCHANGE: str
    AGED_POSITION_COUNT: int = 0
    ELIGIBLE_FORCED_SELL_COUNT: int = 0
    EXECUTED_FORCED_SELL_COUNT: int = 0


def run_forced_sell_pipeline(
    settings: AppSettings,
    exchange_code: str,
) -> ForcedSellPipelineResult:
    settings.DEVICE_ID = get_runtime_device_id(settings.USERNAME)

    now = datetime.now(ZoneInfo(settings.APP_TIMEZONE))
    trade_date = now.date()
    exchange_config = get_exchange_config(exchange_code)

    repository = Repository(create_db_engine(settings))
    log_service = LogService(settings)
    trade_log_service = TradeLogService(settings)
    position_runtime_service = PositionRuntimeService(settings)

    print("-" * 60)
    print(f"FORCED SELL PIPELINE START | EXCHANGE={exchange_code}")
    print(f"TRADE_DATE               : {trade_date.isoformat()}")

    log_service.log(
        LogEvent(
            EVENT_TYPE="FORCED_SELL",
            EVENT_STATUS="STARTED",
            MESSAGE="Forced sell pipeline started.",
            PIPELINE_NAME="FORCED_SELL_PIPELINE",
            EXCHANGE=exchange_code,
            DETAILS_JSON={
                "TRADE_DATE": trade_date.isoformat(),
                "FORCED_SELL_MODE": exchange_config.FORCED_SELL_MODE,
            },
        )
    )

    try:
        aging_results = position_runtime_service.apply_daily_aging_for_exchange(
            EXCHANGE=exchange_code,
            CURRENT_DATE=trade_date,
        )
        aged_position_count = len(aging_results)

        open_runtime_rows = repository.get_open_position_runtime_rows_by_exchange(
            USERNAME=settings.USERNAME,
            EXCHANGE=exchange_code,
        )

        eligible_rows = [
            row for row in open_runtime_rows
            if int(row["REMAINING_HOLDING_DAYS"]) <= 0
        ]

        eligible_forced_sell_count = len(eligible_rows)
        if eligible_forced_sell_count <= 0:
            return ForcedSellPipelineResult(
                SUCCESS=True,
                MESSAGE="Forced sell pipeline completed successfully. No eligible positions.",
                EXCHANGE=exchange_code,
                AGED_POSITION_COUNT=aged_position_count,
                ELIGIBLE_FORCED_SELL_COUNT=0,
                EXECUTED_FORCED_SELL_COUNT=0,
            )

        latest_position_details = repository.get_latest_open_position_details(settings.USERNAME)
        detail_map = {
            (str(row["EXCHANGE"]).upper(), str(row["SYMBOL"]).upper()): row
            for row in latest_position_details
        }

        connection_service = IbkrConnectionService(settings)
        connect_result = connection_service.connect(readonly=False)
        if not connect_result.SUCCESS:
            raise RuntimeError(f"IBKR connection failed: {connect_result.MESSAGE}")

        executed_forced_sell_count = 0

        try:
            ib_client = connection_service.get_client()
            order_service = IbkrOrderService(ib_client)

            for runtime_row in eligible_rows:
                symbol = str(runtime_row["SYMBOL"]).upper()
                exchange = str(runtime_row["EXCHANGE"]).upper()

                detail = detail_map.get((exchange, symbol))
                if not detail:
                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="FORCED_SELL",
                            EVENT_STATUS="WARNING",
                            MESSAGE="Position detail not found for forced sell candidate.",
                            PIPELINE_NAME="FORCED_SELL_PIPELINE",
                            EXCHANGE=exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={},
                        )
                    )
                    continue

                quantity = int(float(detail.get("POSITION_QTY") or 0.0))
                market_price = detail.get("MARKET_PRICE")

                if quantity <= 0:
                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="FORCED_SELL",
                            EVENT_STATUS="WARNING",
                            MESSAGE="Forced sell skipped because quantity is 0.",
                            PIPELINE_NAME="FORCED_SELL_PIPELINE",
                            EXCHANGE=exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={},
                        )
                    )
                    continue

                sell_result = order_service.place_market_sell(
                    symbol=symbol,
                    exchange=exchange,
                    currency=exchange_config.CURRENCY,
                    quantity=quantity,
                )

                if not sell_result.SUCCESS:
                    log_service.log(
                        LogEvent(
                            EVENT_TYPE="FORCED_SELL",
                            EVENT_STATUS="FAILED",
                            MESSAGE=sell_result.MESSAGE,
                            PIPELINE_NAME="FORCED_SELL_PIPELINE",
                            EXCHANGE=exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={
                                "QUANTITY": quantity,
                            },
                        )
                    )
                    continue

                trade_log_service.write_trade(
                    EXCHANGE=exchange,
                    SYMBOL=symbol,
                    ACTION="SELL",
                    QUANTITY=float(quantity),
                    PRICE=float(sell_result.AVG_FILL_PRICE or market_price) if (sell_result.AVG_FILL_PRICE or market_price) is not None else None,
                    TRADE_SOURCE="SYSTEM",
                    DETECTION_SOURCE="LIVE",
                    TRADE_TIME=now,
                    ORDER_ID=str(sell_result.ORDER_ID) if sell_result.ORDER_ID is not None else None,
                    CLIENT_ORDER_ID=None,
                    NOTES="FORCED_SELL_PIPELINE",
                )

                position_runtime_service.close_position(
                    EXCHANGE=exchange,
                    SYMBOL=symbol,
                    LAST_PRICE=float(sell_result.AVG_FILL_PRICE or market_price) if (sell_result.AVG_FILL_PRICE or market_price) is not None else None,
                )

                executed_forced_sell_count += 1

                log_service.log(
                    LogEvent(
                        EVENT_TYPE="FORCED_SELL",
                        EVENT_STATUS="SUCCESS",
                        MESSAGE="Forced sell executed successfully.",
                        PIPELINE_NAME="FORCED_SELL_PIPELINE",
                        EXCHANGE=exchange,
                        SYMBOL=symbol,
                        DETAILS_JSON={
                            "QUANTITY": quantity,
                            "AVG_FILL_PRICE": sell_result.AVG_FILL_PRICE,
                        },
                    )
                )

        finally:
            connection_service.disconnect()

        return ForcedSellPipelineResult(
            SUCCESS=True,
            MESSAGE="Forced sell pipeline completed successfully.",
            EXCHANGE=exchange_code,
            AGED_POSITION_COUNT=aged_position_count,
            ELIGIBLE_FORCED_SELL_COUNT=eligible_forced_sell_count,
            EXECUTED_FORCED_SELL_COUNT=executed_forced_sell_count,
        )

    except Exception as exc:
        log_service.log(
            LogEvent(
                EVENT_TYPE="FORCED_SELL",
                EVENT_STATUS="FAILED",
                MESSAGE=f"Forced sell pipeline failed: {exc}",
                PIPELINE_NAME="FORCED_SELL_PIPELINE",
                EXCHANGE=exchange_code,
                DETAILS_JSON={
                    "ERROR": str(exc),
                },
            )
        )

        return ForcedSellPipelineResult(
            SUCCESS=False,
            MESSAGE=f"Forced sell pipeline failed: {exc}",
            EXCHANGE=exchange_code,
        )

    finally:
        print(f"FORCED SELL PIPELINE END | EXCHANGE={exchange_code}")
        print("-" * 60)