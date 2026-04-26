from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from zoneinfo import ZoneInfo

from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.log_service import LogEvent, LogService


@dataclass
class PositionRuntimeResult:
    SUCCESS: bool
    MESSAGE: str
    EXCHANGE: str
    SYMBOL: str
    HOLDING_DAYS_USED: int | None = None
    REMAINING_HOLDING_DAYS: int | None = None
    MAX_HOLDING_DAY: int | None = None
    IS_OPEN: bool | None = None


class PositionRuntimeService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.engine = create_db_engine(settings)
        self.repository = Repository(self.engine)
        self.log_service = LogService(settings)

    def open_new_position(
        self,
        EXCHANGE: str,
        SYMBOL: str,
        ENTRY_DATE,
        MAX_HOLDING_DAY: int,
        LAST_PRICE: float | None = None,
    ) -> PositionRuntimeResult:
        now = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))
        normalized_exchange = EXCHANGE.strip().upper()
        normalized_symbol = SYMBOL.strip().upper()

        holding_days_used = 1
        remaining_holding_days = max(MAX_HOLDING_DAY - 1, 0)

        try:
            self.repository.upsert_user_position_runtime(
                USERNAME=self.settings.USERNAME,
                DEVICE_ID=self.settings.DEVICE_ID,
                IBKR_MODE=self.settings.IBKR_MODE,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                IS_OPEN=True,
                ENTRY_DATE=ENTRY_DATE,
                LAST_SIGNAL_DATE=ENTRY_DATE,
                HOLDING_DAYS_USED=holding_days_used,
                REMAINING_HOLDING_DAYS=remaining_holding_days,
                MAX_HOLDING_DAY=MAX_HOLDING_DAY,
                LAST_PRICE=LAST_PRICE,
                LAST_AGING_DATE=ENTRY_DATE,
                LAST_UPDATED_AT=now,
            )

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE="New position runtime opened.",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "ENTRY_DATE": str(ENTRY_DATE),
                        "MAX_HOLDING_DAY": MAX_HOLDING_DAY,
                        "HOLDING_DAYS_USED": holding_days_used,
                        "REMAINING_HOLDING_DAYS": remaining_holding_days,
                    },
                )
            )

            return PositionRuntimeResult(
                SUCCESS=True,
                MESSAGE="New position runtime opened.",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                HOLDING_DAYS_USED=holding_days_used,
                REMAINING_HOLDING_DAYS=remaining_holding_days,
                MAX_HOLDING_DAY=MAX_HOLDING_DAY,
                IS_OPEN=True,
            )

        except Exception as exc:
            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME",
                    EVENT_STATUS="FAILED",
                    MESSAGE=f"Open new position runtime failed: {exc}",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "ERROR": str(exc),
                    },
                )
            )

            return PositionRuntimeResult(
                SUCCESS=False,
                MESSAGE=f"Open new position runtime failed: {exc}",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
            )

    def apply_signal_refresh_plus_one(
        self,
        EXCHANGE: str,
        SYMBOL: str,
        SIGNAL_DATE,
        FORCED_SELL_MODE: str,
    ) -> PositionRuntimeResult:
        now = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))
        normalized_exchange = EXCHANGE.strip().upper()
        normalized_symbol = SYMBOL.strip().upper()
        normalized_mode = FORCED_SELL_MODE.strip().upper()

        try:
            row = self.repository.get_user_position_runtime(
                USERNAME=self.settings.USERNAME,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
            )

            if not row:
                return PositionRuntimeResult(
                    SUCCESS=False,
                    MESSAGE="Position runtime row not found.",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                )

            max_holding_day = int(row["MAX_HOLDING_DAY"])
            remaining_holding_days = int(row["REMAINING_HOLDING_DAYS"])
            holding_days_used = int(row["HOLDING_DAYS_USED"])
            last_aging_date = row.get("LAST_AGING_DATE") or row["ENTRY_DATE"]

            if normalized_mode == "FIXED":
                new_max = max_holding_day
                new_remaining = remaining_holding_days
                new_used = holding_days_used
            else:
                new_max = max_holding_day + 1
                new_remaining = remaining_holding_days + 1
                new_used = max(new_max - new_remaining, 0)

            self.repository.upsert_user_position_runtime(
                USERNAME=self.settings.USERNAME,
                DEVICE_ID=self.settings.DEVICE_ID,
                IBKR_MODE=self.settings.IBKR_MODE,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                IS_OPEN=bool(row["IS_OPEN"]),
                ENTRY_DATE=row["ENTRY_DATE"],
                LAST_SIGNAL_DATE=SIGNAL_DATE,
                HOLDING_DAYS_USED=new_used,
                REMAINING_HOLDING_DAYS=new_remaining,
                MAX_HOLDING_DAY=new_max,
                LAST_PRICE=row.get("LAST_PRICE"),
                LAST_AGING_DATE=last_aging_date,
                LAST_UPDATED_AT=now,
            )

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME_REFRESH",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE="Signal refresh applied to position runtime.",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "SIGNAL_DATE": str(SIGNAL_DATE),
                        "FORCED_SELL_MODE": normalized_mode,
                        "OLD_HOLDING_DAYS_USED": holding_days_used,
                        "OLD_REMAINING_HOLDING_DAYS": remaining_holding_days,
                        "OLD_MAX_HOLDING_DAY": max_holding_day,
                        "NEW_HOLDING_DAYS_USED": new_used,
                        "NEW_REMAINING_HOLDING_DAYS": new_remaining,
                        "NEW_MAX_HOLDING_DAY": new_max,
                    },
                )
            )

            return PositionRuntimeResult(
                SUCCESS=True,
                MESSAGE="Signal refresh applied.",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                HOLDING_DAYS_USED=new_used,
                REMAINING_HOLDING_DAYS=new_remaining,
                MAX_HOLDING_DAY=new_max,
                IS_OPEN=bool(row["IS_OPEN"]),
            )

        except Exception as exc:
            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME_REFRESH",
                    EVENT_STATUS="FAILED",
                    MESSAGE=f"Signal refresh failed: {exc}",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "FORCED_SELL_MODE": normalized_mode,
                        "ERROR": str(exc),
                    },
                )
            )

            return PositionRuntimeResult(
                SUCCESS=False,
                MESSAGE=f"Signal refresh failed: {exc}",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
            )

    def apply_daily_aging_for_exchange(
        self,
        EXCHANGE: str,
        CURRENT_DATE: date,
    ) -> list[PositionRuntimeResult]:
        normalized_exchange = EXCHANGE.strip().upper()
        now = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))

        rows = self.repository.get_open_position_runtime_rows_by_exchange(
            USERNAME=self.settings.USERNAME,
            EXCHANGE=normalized_exchange,
        )

        results: list[PositionRuntimeResult] = []

        for row in rows:
            symbol = str(row["SYMBOL"]).upper()
            last_aging_date = row.get("LAST_AGING_DATE") or row["ENTRY_DATE"]

            if isinstance(last_aging_date, datetime):
                last_aging_date = last_aging_date.date()

            days_elapsed = (CURRENT_DATE - last_aging_date).days
            if days_elapsed <= 0:
                if row.get("LAST_AGING_DATE") is None:
                    self.repository.upsert_user_position_runtime(
                        USERNAME=self.settings.USERNAME,
                        DEVICE_ID=self.settings.DEVICE_ID,
                        IBKR_MODE=self.settings.IBKR_MODE,
                        EXCHANGE=normalized_exchange,
                        SYMBOL=symbol,
                        IS_OPEN=bool(row["IS_OPEN"]),
                        ENTRY_DATE=row["ENTRY_DATE"],
                        LAST_SIGNAL_DATE=row.get("LAST_SIGNAL_DATE"),
                        HOLDING_DAYS_USED=int(row["HOLDING_DAYS_USED"]),
                        REMAINING_HOLDING_DAYS=int(row["REMAINING_HOLDING_DAYS"]),
                        MAX_HOLDING_DAY=int(row["MAX_HOLDING_DAY"]),
                        LAST_PRICE=row.get("LAST_PRICE"),
                        LAST_AGING_DATE=CURRENT_DATE,
                        LAST_UPDATED_AT=now,
                    )

                    self.log_service.log(
                        LogEvent(
                            EVENT_TYPE="POSITION_RUNTIME_AGING",
                            EVENT_STATUS="SUCCESS",
                            MESSAGE="LAST_AGING_DATE initialized without aging.",
                            PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                            EXCHANGE=normalized_exchange,
                            SYMBOL=symbol,
                            DETAILS_JSON={
                                "CURRENT_DATE": str(CURRENT_DATE),
                            },
                        )
                    )

                results.append(
                    PositionRuntimeResult(
                        SUCCESS=True,
                        MESSAGE="No aging needed.",
                        EXCHANGE=normalized_exchange,
                        SYMBOL=symbol,
                        HOLDING_DAYS_USED=int(row["HOLDING_DAYS_USED"]),
                        REMAINING_HOLDING_DAYS=int(row["REMAINING_HOLDING_DAYS"]),
                        MAX_HOLDING_DAY=int(row["MAX_HOLDING_DAY"]),
                        IS_OPEN=bool(row["IS_OPEN"]),
                    )
                )
                continue

            old_used = int(row["HOLDING_DAYS_USED"])
            old_remaining = int(row["REMAINING_HOLDING_DAYS"])
            max_holding_day = int(row["MAX_HOLDING_DAY"])

            new_used = old_used + days_elapsed
            new_remaining = old_remaining - days_elapsed

            self.repository.upsert_user_position_runtime(
                USERNAME=self.settings.USERNAME,
                DEVICE_ID=self.settings.DEVICE_ID,
                IBKR_MODE=self.settings.IBKR_MODE,
                EXCHANGE=normalized_exchange,
                SYMBOL=symbol,
                IS_OPEN=bool(row["IS_OPEN"]),
                ENTRY_DATE=row["ENTRY_DATE"],
                LAST_SIGNAL_DATE=row.get("LAST_SIGNAL_DATE"),
                HOLDING_DAYS_USED=new_used,
                REMAINING_HOLDING_DAYS=new_remaining,
                MAX_HOLDING_DAY=max_holding_day,
                LAST_PRICE=row.get("LAST_PRICE"),
                LAST_AGING_DATE=CURRENT_DATE,
                LAST_UPDATED_AT=now,
            )

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME_AGING",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE="Daily aging applied to position runtime.",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=symbol,
                    DETAILS_JSON={
                        "DAYS_ELAPSED": days_elapsed,
                        "OLD_HOLDING_DAYS_USED": old_used,
                        "OLD_REMAINING_HOLDING_DAYS": old_remaining,
                        "NEW_HOLDING_DAYS_USED": new_used,
                        "NEW_REMAINING_HOLDING_DAYS": new_remaining,
                    },
                )
            )

            results.append(
                PositionRuntimeResult(
                    SUCCESS=True,
                    MESSAGE="Daily aging applied.",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=symbol,
                    HOLDING_DAYS_USED=new_used,
                    REMAINING_HOLDING_DAYS=new_remaining,
                    MAX_HOLDING_DAY=max_holding_day,
                    IS_OPEN=True,
                )
            )

        return results

    def close_position(
        self,
        EXCHANGE: str,
        SYMBOL: str,
        LAST_PRICE: float | None = None,
    ) -> PositionRuntimeResult:
        now = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))
        normalized_exchange = EXCHANGE.strip().upper()
        normalized_symbol = SYMBOL.strip().upper()

        try:
            row = self.repository.get_user_position_runtime(
                USERNAME=self.settings.USERNAME,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
            )

            if not row:
                return PositionRuntimeResult(
                    SUCCESS=False,
                    MESSAGE="Position runtime row not found.",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                )

            self.repository.close_user_position_runtime(
                USERNAME=self.settings.USERNAME,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                LAST_UPDATED_AT=now,
                LAST_PRICE=LAST_PRICE,
            )

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE="Position runtime closed.",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "LAST_PRICE": LAST_PRICE,
                    },
                )
            )

            return PositionRuntimeResult(
                SUCCESS=True,
                MESSAGE="Position runtime closed.",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                HOLDING_DAYS_USED=int(row["HOLDING_DAYS_USED"]),
                REMAINING_HOLDING_DAYS=int(row["REMAINING_HOLDING_DAYS"]),
                MAX_HOLDING_DAY=int(row["MAX_HOLDING_DAY"]),
                IS_OPEN=False,
            )

        except Exception as exc:
            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="POSITION_RUNTIME",
                    EVENT_STATUS="FAILED",
                    MESSAGE=f"Close position runtime failed: {exc}",
                    PIPELINE_NAME="POSITION_RUNTIME_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "ERROR": str(exc),
                    },
                )
            )

            return PositionRuntimeResult(
                SUCCESS=False,
                MESSAGE=f"Close position runtime failed: {exc}",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
            )