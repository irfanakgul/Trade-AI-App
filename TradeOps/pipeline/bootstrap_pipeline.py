from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.device import get_runtime_device_id
from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.ibkr_account_service import IbkrAccountService
from service.ibkr_connection_service import IbkrConnectionService
from service.ibkr_order_service import IbkrOrderService
from service.ibkr_position_service import IbkrPositionService
from service.log_service import LogEvent, LogService


@dataclass
class BootstrapPipelineResult:
    SUCCESS: bool
    MESSAGE: str
    FETCHED_AT: str | None = None
    ACCOUNT_ID: str | None = None
    OPEN_POSITION_COUNT: int | None = None
    OPEN_SYMBOLS: str | None = None
    ORDER_DETAIL_ROW_COUNT: int | None = None
    POSITION_DETAIL_ROW_COUNT: int | None = None


def run_bootstrap_pipeline(settings: AppSettings) -> BootstrapPipelineResult:
    settings.DEVICE_ID = get_runtime_device_id(settings.USERNAME)
    fetched_at = datetime.now(ZoneInfo(settings.APP_TIMEZONE))

    engine = create_db_engine(settings)
    repository = Repository(engine)
    connection_service = IbkrConnectionService(settings)
    log_service = LogService(settings)

    print("-" * 60)
    print("BOOTSTRAP PIPELINE START")
    print(f"FETCHED_AT               : {fetched_at.isoformat()}")

    log_service.log(
        LogEvent(
            EVENT_TYPE="PIPELINE_RUN",
            EVENT_STATUS="STARTED",
            MESSAGE="Bootstrap pipeline started.",
            PIPELINE_NAME="BOOTSTRAP_PIPELINE",
            DETAILS_JSON={
                "FETCHED_AT": fetched_at.isoformat(),
            },
        )
    )

    connect_result = connection_service.connect(readonly=False)
    if not connect_result.SUCCESS:
        log_service.log(
            LogEvent(
                EVENT_TYPE="IBKR_CONNECTION",
                EVENT_STATUS="FAILED",
                MESSAGE=f"IBKR connection failed: {connect_result.MESSAGE}",
                PIPELINE_NAME="BOOTSTRAP_PIPELINE",
                DETAILS_JSON={
                    "FETCHED_AT": fetched_at.isoformat(),
                },
            )
        )
        return BootstrapPipelineResult(
            SUCCESS=False,
            MESSAGE=f"IBKR connection failed: {connect_result.MESSAGE}",
            FETCHED_AT=fetched_at.isoformat(),
        )

    try:
        ib_client = connection_service.get_client()

        account_service = IbkrAccountService(ib_client)
        position_service = IbkrPositionService(ib_client)
        order_service = IbkrOrderService(ib_client)

        print("[PIPELINE] Fetching IBKR account metrics...")
        metrics = account_service.get_account_metrics()

        print("[PIPELINE] Fetching raw account summary...")
        raw_summary = account_service.get_raw_account_summary_map()

        print("[PIPELINE] Fetching position snapshot...")
        position_snapshot = position_service.get_position_snapshot()

        print("[PIPELINE] Fetching position details...")
        position_details = position_service.get_position_details()

        print("[PIPELINE] Fetching open order details...")
        order_details = order_service.get_order_details()

        print("[PIPELINE] Writing wallet snapshot history to DB...")
        repository.insert_user_ibkr_wallet_details(
            USERNAME=settings.USERNAME,
            DEVICE_ID=settings.DEVICE_ID,
            IBKR_MODE=settings.IBKR_MODE,
            ACCOUNT_ID=metrics.ACCOUNT_ID,
            AVAILABLE_FUNDS=metrics.AVAILABLE_FUNDS,
            NET_LIQUIDATION=metrics.NET_LIQUIDATION,
            TOTAL_CASH_VALUE=metrics.TOTAL_CASH_VALUE,
            SETTLED_CASH=metrics.SETTLED_CASH,
            BUYING_POWER=metrics.BUYING_POWER,
            EXCESS_LIQUIDITY=metrics.EXCESS_LIQUIDITY,
            GROSS_POSITION_VALUE=metrics.GROSS_POSITION_VALUE,
            CASH_BALANCE_BASE=metrics.CASH_BALANCE_BASE,
            CASH_BALANCE_EUR=metrics.CASH_BALANCE_EUR,
            CASH_BALANCE_USD=metrics.CASH_BALANCE_USD,
            OPEN_POSITION_COUNT=position_snapshot.OPEN_POSITION_COUNT,
            OPEN_SYMBOLS=position_snapshot.OPEN_SYMBOLS,
            RAW_ACCOUNT_SUMMARY_JSON=raw_summary,
            FETCHED_AT=fetched_at,
        )

        print("[PIPELINE] Updating actual wallet to DB...")
        repository.upsert_user_actual_wallet(
            USERNAME=settings.USERNAME,
            DEVICE_ID=settings.DEVICE_ID,
            IBKR_MODE=settings.IBKR_MODE,
            ACCOUNT_ID=metrics.ACCOUNT_ID,
            AVAILABLE_FUNDS=metrics.AVAILABLE_FUNDS,
            NET_LIQUIDATION=metrics.NET_LIQUIDATION,
            TOTAL_CASH_VALUE=metrics.TOTAL_CASH_VALUE,
            SETTLED_CASH=metrics.SETTLED_CASH,
            BUYING_POWER=metrics.BUYING_POWER,
            EXCESS_LIQUIDITY=metrics.EXCESS_LIQUIDITY,
            GROSS_POSITION_VALUE=metrics.GROSS_POSITION_VALUE,
            CASH_BALANCE_BASE=metrics.CASH_BALANCE_BASE,
            CASH_BALANCE_EUR=metrics.CASH_BALANCE_EUR,
            CASH_BALANCE_USD=metrics.CASH_BALANCE_USD,
            OPEN_POSITION_COUNT=position_snapshot.OPEN_POSITION_COUNT,
            OPEN_SYMBOLS=position_snapshot.OPEN_SYMBOLS,
            RAW_ACCOUNT_SUMMARY_JSON=raw_summary,
            FETCHED_AT=fetched_at,
        )

        print("[PIPELINE] Writing position detail snapshots to DB...")
        for position_detail in position_details:
            repository.insert_user_open_position_details(
                USERNAME=settings.USERNAME,
                DEVICE_ID=settings.DEVICE_ID,
                IBKR_MODE=settings.IBKR_MODE,
                ACCOUNT_ID=position_detail.ACCOUNT_ID,
                SYMBOL=position_detail.SYMBOL,
                LOCAL_SYMBOL=position_detail.LOCAL_SYMBOL,
                SEC_TYPE=position_detail.SEC_TYPE,
                EXCHANGE=position_detail.EXCHANGE,
                PRIMARY_EXCHANGE=position_detail.PRIMARY_EXCHANGE,
                CURRENCY=position_detail.CURRENCY,
                POSITION_QTY=position_detail.POSITION_QTY,
                AVG_COST=position_detail.AVG_COST,
                MARKET_PRICE=position_detail.MARKET_PRICE,
                MARKET_VALUE=position_detail.MARKET_VALUE,
                UNREALIZED_PNL=position_detail.UNREALIZED_PNL,
                REALIZED_PNL=position_detail.REALIZED_PNL,
                RAW_POSITION_JSON=position_detail.RAW_POSITION_JSON,
                FETCHED_AT=fetched_at,
            )

        print("[PIPELINE] Writing open order snapshots to DB...")
        for order_detail in order_details:
            repository.insert_user_order_details(
                USERNAME=settings.USERNAME,
                DEVICE_ID=settings.DEVICE_ID,
                IBKR_MODE=settings.IBKR_MODE,
                ACCOUNT_ID=order_detail.ACCOUNT_ID,
                ORDER_ID=order_detail.ORDER_ID,
                PERM_ID=order_detail.PERM_ID,
                CLIENT_ID=order_detail.CLIENT_ID,
                PARENT_ID=order_detail.PARENT_ID,
                SYMBOL=order_detail.SYMBOL,
                LOCAL_SYMBOL=order_detail.LOCAL_SYMBOL,
                SEC_TYPE=order_detail.SEC_TYPE,
                EXCHANGE=order_detail.EXCHANGE,
                PRIMARY_EXCHANGE=order_detail.PRIMARY_EXCHANGE,
                CURRENCY=order_detail.CURRENCY,
                ACTION=order_detail.ACTION,
                TOTAL_QUANTITY=order_detail.TOTAL_QUANTITY,
                ORDER_TYPE=order_detail.ORDER_TYPE,
                LMT_PRICE=order_detail.LMT_PRICE,
                AUX_PRICE=order_detail.AUX_PRICE,
                TIF=order_detail.TIF,
                OUTSIDE_RTH=order_detail.OUTSIDE_RTH,
                TRANSMIT=order_detail.TRANSMIT,
                ORDER_REF=order_detail.ORDER_REF,
                ORDER_STATUS=order_detail.ORDER_STATUS,
                FILLED=order_detail.FILLED,
                REMAINING=order_detail.REMAINING,
                AVG_FILL_PRICE=order_detail.AVG_FILL_PRICE,
                LAST_FILL_PRICE=order_detail.LAST_FILL_PRICE,
                WHY_HELD=order_detail.WHY_HELD,
                MKT_CAP_PRICE=order_detail.MKT_CAP_PRICE,
                RAW_ORDER_JSON=order_detail.RAW_ORDER_JSON,
                FETCHED_AT=fetched_at,
            )

        print("[PIPELINE] Wallet snapshot inserted successfully")
        print("[PIPELINE] Actual wallet updated successfully")
        print("[PIPELINE] Position detail snapshots inserted successfully")
        print("[PIPELINE] Order detail snapshots inserted successfully")

        log_service.log(
            LogEvent(
                EVENT_TYPE="PIPELINE_RUN",
                EVENT_STATUS="SUCCESS",
                MESSAGE="Bootstrap pipeline completed successfully.",
                PIPELINE_NAME="BOOTSTRAP_PIPELINE",
                DETAILS_JSON={
                    "FETCHED_AT": fetched_at.isoformat(),
                    "ACCOUNT_ID": metrics.ACCOUNT_ID,
                    "AVAILABLE_FUNDS": metrics.AVAILABLE_FUNDS,
                    "OPEN_POSITION_COUNT": position_snapshot.OPEN_POSITION_COUNT,
                    "OPEN_SYMBOLS": position_snapshot.OPEN_SYMBOLS,
                    "ORDER_DETAIL_ROW_COUNT": len(order_details),
                    "POSITION_DETAIL_ROW_COUNT": len(position_details),
                },
            )
        )

        return BootstrapPipelineResult(
            SUCCESS=True,
            MESSAGE="Bootstrap pipeline completed successfully.",
            FETCHED_AT=fetched_at.isoformat(),
            ACCOUNT_ID=metrics.ACCOUNT_ID,
            OPEN_POSITION_COUNT=position_snapshot.OPEN_POSITION_COUNT,
            OPEN_SYMBOLS=position_snapshot.OPEN_SYMBOLS,
            ORDER_DETAIL_ROW_COUNT=len(order_details),
            POSITION_DETAIL_ROW_COUNT=len(position_details),
        )

    except Exception as exc:
        log_service.log(
            LogEvent(
                EVENT_TYPE="PIPELINE_RUN",
                EVENT_STATUS="FAILED",
                MESSAGE=f"Bootstrap pipeline failed: {exc}",
                PIPELINE_NAME="BOOTSTRAP_PIPELINE",
                DETAILS_JSON={
                    "FETCHED_AT": fetched_at.isoformat(),
                    "ERROR": str(exc),
                },
            )
        )

        return BootstrapPipelineResult(
            SUCCESS=False,
            MESSAGE=f"Bootstrap pipeline failed: {exc}",
            FETCHED_AT=fetched_at.isoformat(),
        )

    finally:
        connection_service.disconnect()
        print("BOOTSTRAP PIPELINE END")
        print("-" * 60)