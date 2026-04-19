from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.exchange_loader import get_exchange_config
from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.log_service import LogEvent, LogService
from service.position_runtime_service import PositionRuntimeService
from service.trade_log_service import TradeLogService


@dataclass
class ReconcileSummary:
    MANUAL_BUY_DETECTED_COUNT: int
    MANUAL_SELL_DETECTED_COUNT: int
    RUNTIME_CLOSED_COUNT: int
    RUNTIME_OPENED_COUNT: int


class ReconcileService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.engine = create_db_engine(settings)
        self.repository = Repository(self.engine)
        self.log_service = LogService(settings)
        self.trade_log_service = TradeLogService(settings)
        self.position_runtime_service = PositionRuntimeService(settings)

    def reconcile_end_of_day(self) -> ReconcileSummary:
        now = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))

        runtime_rows = self.repository.get_open_position_runtime_rows(self.settings.USERNAME)
        broker_rows = self.repository.get_latest_open_position_details(self.settings.USERNAME)

        runtime_map = {
            (str(row["EXCHANGE"]).upper(), str(row["SYMBOL"]).upper()): row
            for row in runtime_rows
        }

        broker_map = {
            (
                self._normalize_broker_exchange_to_execution_exchange(row),
                str(row["SYMBOL"]).upper(),
            ): row
            for row in broker_rows
        }

        runtime_keys = set(runtime_map.keys())
        broker_keys = set(broker_map.keys())

        manual_buy_keys = broker_keys - runtime_keys
        manual_sell_keys = runtime_keys - broker_keys

        manual_buy_detected_count = 0
        manual_sell_detected_count = 0
        runtime_closed_count = 0
        runtime_opened_count = 0

        # Manual buys: broker has it, runtime does not
        for exchange, symbol in sorted(manual_buy_keys):
            broker_row = broker_map[(exchange, symbol)]

            qty = float(broker_row.get("POSITION_QTY") or 0.0)
            avg_cost = broker_row.get("AVG_COST")
            fetched_at = broker_row.get("FETCHED_AT") or now

            self.trade_log_service.write_trade(
                EXCHANGE=exchange,
                SYMBOL=symbol,
                ACTION="BUY",
                QUANTITY=qty,
                PRICE=float(avg_cost) if avg_cost is not None else None,
                TRADE_SOURCE="MANUAL",
                DETECTION_SOURCE="END_OF_DAY",
                TRADE_TIME=fetched_at,
                ORDER_ID=None,
                CLIENT_ORDER_ID=None,
                NOTES="Detected by end_of_day_reconcile_pipeline as manual buy.",
            )

            try:
                exchange_config = get_exchange_config(exchange)

                open_result = self.position_runtime_service.open_new_position(
                    EXCHANGE=exchange,
                    SYMBOL=symbol,
                    ENTRY_DATE=fetched_at.date(),
                    MAX_HOLDING_DAY=int(exchange_config.MAX_HOLDING_DAY),
                    LAST_PRICE=float(avg_cost) if avg_cost is not None else None,
                )

                if open_result.SUCCESS:
                    runtime_opened_count += 1

            except Exception as exc:
                self.log_service.log(
                    LogEvent(
                        EVENT_TYPE="RECONCILE",
                        EVENT_STATUS="FAILED",
                        MESSAGE=f"Manual buy detected but runtime open failed: {exc}",
                        PIPELINE_NAME="END_OF_DAY_RECONCILE_PIPELINE",
                        EXCHANGE=exchange,
                        SYMBOL=symbol,
                        DETAILS_JSON={
                            "POSITION_QTY": qty,
                            "AVG_COST": avg_cost,
                            "BROKER_EXCHANGE": broker_row.get("EXCHANGE"),
                            "PRIMARY_EXCHANGE": broker_row.get("PRIMARY_EXCHANGE"),
                            "ERROR": str(exc),
                        },
                    )
                )

            manual_buy_detected_count += 1

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="RECONCILE",
                    EVENT_STATUS="WARNING",
                    MESSAGE="Manual buy detected in broker state.",
                    PIPELINE_NAME="END_OF_DAY_RECONCILE_PIPELINE",
                    EXCHANGE=exchange,
                    SYMBOL=symbol,
                    DETAILS_JSON={
                        "POSITION_QTY": qty,
                        "AVG_COST": avg_cost,
                        "BROKER_EXCHANGE": broker_row.get("EXCHANGE"),
                        "PRIMARY_EXCHANGE": broker_row.get("PRIMARY_EXCHANGE"),
                    },
                )
            )

        # Manual sells: runtime has it, broker does not
        for exchange, symbol in sorted(manual_sell_keys):
            runtime_row = runtime_map[(exchange, symbol)]

            self.trade_log_service.write_trade(
                EXCHANGE=exchange,
                SYMBOL=symbol,
                ACTION="SELL",
                QUANTITY=1.0,
                PRICE=float(runtime_row["LAST_PRICE"]) if runtime_row.get("LAST_PRICE") is not None else None,
                TRADE_SOURCE="MANUAL",
                DETECTION_SOURCE="END_OF_DAY",
                TRADE_TIME=now,
                ORDER_ID=None,
                CLIENT_ORDER_ID=None,
                NOTES="Detected by end_of_day_reconcile_pipeline as manual sell.",
            )

            close_result = self.position_runtime_service.close_position(
                EXCHANGE=exchange,
                SYMBOL=symbol,
                LAST_PRICE=float(runtime_row["LAST_PRICE"]) if runtime_row.get("LAST_PRICE") is not None else None,
            )

            if close_result.SUCCESS:
                runtime_closed_count += 1

            manual_sell_detected_count += 1

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="RECONCILE",
                    EVENT_STATUS="WARNING",
                    MESSAGE="Manual sell detected in broker state.",
                    PIPELINE_NAME="END_OF_DAY_RECONCILE_PIPELINE",
                    EXCHANGE=exchange,
                    SYMBOL=symbol,
                    DETAILS_JSON={},
                )
            )

        return ReconcileSummary(
            MANUAL_BUY_DETECTED_COUNT=manual_buy_detected_count,
            MANUAL_SELL_DETECTED_COUNT=manual_sell_detected_count,
            RUNTIME_CLOSED_COUNT=runtime_closed_count,
            RUNTIME_OPENED_COUNT=runtime_opened_count,
        )

    def _normalize_broker_exchange_to_execution_exchange(self, broker_row: dict) -> str:
        primary_exchange = str(broker_row.get("PRIMARY_EXCHANGE") or "").upper()
        exchange = str(broker_row.get("EXCHANGE") or "").upper()

        if primary_exchange in {"NASDAQ"}:
            return "NASDAQ"

        if primary_exchange in {"NYSE"}:
            return "NYSE"

        if primary_exchange in {"AEB", "ENEXT", "EURONEXT", "AEBE"}:
            return "AEB"

        if exchange in {"NASDAQ", "NYSE", "AEB"}:
            return exchange

        if exchange == "SMART" and primary_exchange:
            return primary_exchange

        return exchange