from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.log_service import LogEvent, LogService


@dataclass
class TradeLogWriteResult:
    SUCCESS: bool
    MESSAGE: str


class TradeLogService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.engine = create_db_engine(settings)
        self.repository = Repository(self.engine)
        self.log_service = LogService(settings)

    def write_trade(
        self,
        EXCHANGE: str,
        SYMBOL: str,
        ACTION: str,
        QUANTITY: float,
        PRICE: float | None,
        TRADE_SOURCE: str,
        DETECTION_SOURCE: str,
        TRADE_TIME: datetime | None = None,
        ORDER_ID: str | None = None,
        CLIENT_ORDER_ID: str | None = None,
        NOTES: str | None = None,
    ) -> TradeLogWriteResult:
        normalized_exchange = EXCHANGE.strip().upper()
        normalized_symbol = SYMBOL.strip().upper()
        normalized_action = ACTION.strip().upper()

        if TRADE_TIME is None:
            TRADE_TIME = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))

        total_amount = None
        if PRICE is not None:
            total_amount = float(PRICE) * float(QUANTITY)

        try:
            self.repository.insert_user_trade_log(
                USERNAME=self.settings.USERNAME,
                DEVICE_ID=self.settings.DEVICE_ID,
                IBKR_MODE=self.settings.IBKR_MODE,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                ACTION=normalized_action,
                QUANTITY=float(QUANTITY),
                PRICE=float(PRICE) if PRICE is not None else None,
                TOTAL_AMOUNT=total_amount,
                TRADE_TIME=TRADE_TIME,
                TRADE_SOURCE=TRADE_SOURCE,
                DETECTION_SOURCE=DETECTION_SOURCE,
                ORDER_ID=ORDER_ID,
                CLIENT_ORDER_ID=CLIENT_ORDER_ID,
                NOTES=NOTES,
            )

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="TRADE_LOG_WRITE",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE="Trade log row inserted successfully.",
                    PIPELINE_NAME="TRADE_LOG_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "ACTION": normalized_action,
                        "QUANTITY": float(QUANTITY),
                        "PRICE": float(PRICE) if PRICE is not None else None,
                        "TRADE_SOURCE": TRADE_SOURCE,
                        "DETECTION_SOURCE": DETECTION_SOURCE,
                    },
                )
            )

            return TradeLogWriteResult(
                SUCCESS=True,
                MESSAGE="Trade log row inserted successfully.",
            )

        except Exception as exc:
            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="TRADE_LOG_WRITE",
                    EVENT_STATUS="FAILED",
                    MESSAGE=f"Trade log insert failed: {exc}",
                    PIPELINE_NAME="TRADE_LOG_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "ACTION": normalized_action,
                        "ERROR": str(exc),
                    },
                )
            )

            return TradeLogWriteResult(
                SUCCESS=False,
                MESSAGE=f"Trade log insert failed: {exc}",
            )