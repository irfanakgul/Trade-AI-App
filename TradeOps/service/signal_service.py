from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.log_service import LogEvent, LogService


@dataclass
class BuySignalRow:
    DATE: object
    EXCHANGE: str
    SYMBOL: str
    RANK: int | None
    TARGET_PRICE: float | None
    RAW_ROW: dict


@dataclass
class SignalReadResult:
    SUCCESS: bool
    MESSAGE: str
    EXECUTION_EXCHANGE: str
    SOURCE_EXCHANGE: str
    SIGNAL_DATE: object | None
    SIGNAL_COUNT: int
    SIGNALS: List[BuySignalRow]


class SignalService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.engine = create_db_engine(settings)
        self.repository = Repository(self.engine)
        self.log_service = LogService(settings)

    def read_latest_signals_for_execution_exchange(
        self,
        execution_exchange: str,
    ) -> SignalReadResult:
        source_exchange = self._map_execution_exchange_to_source_exchange(execution_exchange)

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SIGNAL_READ",
                EVENT_STATUS="STARTED",
                MESSAGE="Signal read started.",
                PIPELINE_NAME="SIGNAL_SERVICE",
                EXCHANGE=execution_exchange,
                DETAILS_JSON={
                    "SOURCE_EXCHANGE": source_exchange,
                },
            )
        )

        try:
            latest_signal_date = self.repository.get_latest_signal_date_for_exchange(
                SOURCE_EXCHANGE=source_exchange
            )

            if latest_signal_date is None:
                message = "No signal date found for exchange."
                self.log_service.log(
                    LogEvent(
                        EVENT_TYPE="SIGNAL_READ",
                        EVENT_STATUS="WARNING",
                        MESSAGE=message,
                        PIPELINE_NAME="SIGNAL_SERVICE",
                        EXCHANGE=execution_exchange,
                        DETAILS_JSON={
                            "SOURCE_EXCHANGE": source_exchange,
                        },
                    )
                )

                return SignalReadResult(
                    SUCCESS=True,
                    MESSAGE=message,
                    EXECUTION_EXCHANGE=execution_exchange,
                    SOURCE_EXCHANGE=source_exchange,
                    SIGNAL_DATE=None,
                    SIGNAL_COUNT=0,
                    SIGNALS=[],
                )

            raw_rows = self.repository.get_daily_buy_signals_for_exchange_and_date(
                SOURCE_EXCHANGE=source_exchange,
                SIGNAL_DATE=latest_signal_date,
            )

            signals = [self._build_signal_row(row) for row in raw_rows]

            message = f"{len(signals)} signals loaded successfully."

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="SIGNAL_READ",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE=message,
                    PIPELINE_NAME="SIGNAL_SERVICE",
                    EXCHANGE=execution_exchange,
                    DETAILS_JSON={
                        "SOURCE_EXCHANGE": source_exchange,
                        "SIGNAL_DATE": str(latest_signal_date),
                        "SIGNAL_COUNT": len(signals),
                    },
                )
            )

            return SignalReadResult(
                SUCCESS=True,
                MESSAGE=message,
                EXECUTION_EXCHANGE=execution_exchange,
                SOURCE_EXCHANGE=source_exchange,
                SIGNAL_DATE=latest_signal_date,
                SIGNAL_COUNT=len(signals),
                SIGNALS=signals,
            )

        except Exception as exc:
            message = f"Signal read failed: {exc}"

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="SIGNAL_READ",
                    EVENT_STATUS="FAILED",
                    MESSAGE=message,
                    PIPELINE_NAME="SIGNAL_SERVICE",
                    EXCHANGE=execution_exchange,
                    DETAILS_JSON={
                        "SOURCE_EXCHANGE": source_exchange,
                        "ERROR": str(exc),
                    },
                )
            )

            return SignalReadResult(
                SUCCESS=False,
                MESSAGE=message,
                EXECUTION_EXCHANGE=execution_exchange,
                SOURCE_EXCHANGE=source_exchange,
                SIGNAL_DATE=None,
                SIGNAL_COUNT=0,
                SIGNALS=[],
            )

    def _map_execution_exchange_to_source_exchange(self, execution_exchange: str) -> str:
        normalized = execution_exchange.strip().upper()

        if normalized == "AEB":
            return "EURONEXT"

        if normalized in {"NASDAQ", "NYSE"}:
            return normalized

        return normalized

    def _build_signal_row(self, row: dict) -> BuySignalRow:
        return BuySignalRow(
            DATE=row.get("DATE"),
            EXCHANGE=str(row.get("EXCHANGE") or "").upper(),
            SYMBOL=str(row.get("SYMBOL") or "").upper(),
            RANK=self._safe_int(row.get("RANK")),
            TARGET_PRICE=self._safe_float(row.get("TARGET_PRICE")),
            RAW_ROW=row,
        )

    def _safe_int(self, value) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _safe_float(self, value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
        
    def get_user_symbol_scope(
        self,
        USERNAME: str,
        EXCHANGE: str,
        SYMBOL: str,
    ) -> dict | None:
        sql = """
        SELECT
            USERNAME,
            EXCHANGE,
            SYMBOL,
            USER_IN_SCOPE
        FROM live.user_symbols_all
        WHERE USERNAME = :USERNAME
          AND EXCHANGE = :EXCHANGE
          AND SYMBOL = :SYMBOL
        LIMIT 1;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "EXCHANGE": EXCHANGE,
                    "SYMBOL": SYMBOL,
                },
            ).mappings().first()

            if not row:
                return None

            row_dict = dict(row)
            return {str(key).upper(): value for key, value in row_dict.items()}