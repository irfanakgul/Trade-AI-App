from __future__ import annotations

from dataclasses import dataclass

from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository
from service.log_service import LogEvent, LogService


@dataclass
class ScopeCheckResult:
    SUCCESS: bool
    MESSAGE: str
    EXCHANGE: str
    SYMBOL: str
    USER_IN_SCOPE: bool


class ScopeService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.engine = create_db_engine(settings)
        self.repository = Repository(self.engine)
        self.log_service = LogService(settings)

    def is_symbol_in_scope(
        self,
        exchange: str,
        symbol: str,
    ) -> ScopeCheckResult:
        normalized_exchange = self._map_exchange_for_storage(exchange)
        normalized_symbol = symbol.strip().upper()

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCOPE_CHECK",
                EVENT_STATUS="STARTED",
                MESSAGE="Scope check started.",
                PIPELINE_NAME="SCOPE_SERVICE",
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                DETAILS_JSON={},
            )
        )

        try:
            row = self.repository.get_user_symbol_scope(
                USERNAME=self.settings.USERNAME,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
            )

            if row is None:
                message = "Scope row not found. Symbol treated as out of scope."

                self.log_service.log(
                    LogEvent(
                        EVENT_TYPE="SCOPE_CHECK",
                        EVENT_STATUS="WARNING",
                        MESSAGE=message,
                        PIPELINE_NAME="SCOPE_SERVICE",
                        EXCHANGE=normalized_exchange,
                        SYMBOL=normalized_symbol,
                        DETAILS_JSON={},
                    )
                )

                return ScopeCheckResult(
                    SUCCESS=True,
                    MESSAGE=message,
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    USER_IN_SCOPE=False,
                )

            user_in_scope = bool(row.get("USER_IN_SCOPE"))

            if user_in_scope:
                message = "Symbol is in scope."

                self.log_service.log(
                    LogEvent(
                        EVENT_TYPE="SCOPE_CHECK",
                        EVENT_STATUS="SUCCESS",
                        MESSAGE=message,
                        PIPELINE_NAME="SCOPE_SERVICE",
                        EXCHANGE=normalized_exchange,
                        SYMBOL=normalized_symbol,
                        DETAILS_JSON={
                            "USER_IN_SCOPE": True,
                        },
                    )
                )

                return ScopeCheckResult(
                    SUCCESS=True,
                    MESSAGE=message,
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    USER_IN_SCOPE=True,
                )

            message = "Symbol is out of scope."

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="SCOPE_CHECK",
                    EVENT_STATUS="WARNING",
                    MESSAGE=message,
                    PIPELINE_NAME="SCOPE_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "USER_IN_SCOPE": False,
                    },
                )
            )

            return ScopeCheckResult(
                SUCCESS=True,
                MESSAGE=message,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                USER_IN_SCOPE=False,
            )

        except Exception as exc:
            message = f"Scope check failed: {exc}"

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="SCOPE_CHECK",
                    EVENT_STATUS="FAILED",
                    MESSAGE=message,
                    PIPELINE_NAME="SCOPE_SERVICE",
                    EXCHANGE=normalized_exchange,
                    SYMBOL=normalized_symbol,
                    DETAILS_JSON={
                        "ERROR": str(exc),
                    },
                )
            )

            return ScopeCheckResult(
                SUCCESS=False,
                MESSAGE=message,
                EXCHANGE=normalized_exchange,
                SYMBOL=normalized_symbol,
                USER_IN_SCOPE=False,
            )

    def _map_exchange_for_storage(self, exchange: str) -> str:
        normalized = exchange.strip().upper()
        if normalized == "AEB":
            return "EURONEXT"
        return normalized