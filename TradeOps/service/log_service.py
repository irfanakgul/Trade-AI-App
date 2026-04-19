from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from config.settings import AppSettings
from repository.db import create_db_engine
from repository.repository import Repository


@dataclass
class LogEvent:
    EVENT_TYPE: str
    EVENT_STATUS: str
    MESSAGE: str
    PIPELINE_NAME: str | None = None
    EXCHANGE: str | None = None
    SYMBOL: str | None = None
    DETAILS_JSON: dict | None = None


class LogService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.engine = create_db_engine(settings)
        self.repository = Repository(self.engine)

    def log(self, event: LogEvent) -> None:
        created_at = datetime.now(ZoneInfo(self.settings.APP_TIMEZONE))

        self.repository.insert_app_log(
            USERNAME=self.settings.USERNAME,
            DEVICE_ID=self.settings.DEVICE_ID,
            IBKR_MODE=self.settings.IBKR_MODE,
            PIPELINE_NAME=event.PIPELINE_NAME,
            EXCHANGE=event.EXCHANGE,
            SYMBOL=event.SYMBOL,
            EVENT_TYPE=event.EVENT_TYPE,
            EVENT_STATUS=event.EVENT_STATUS,
            MESSAGE=event.MESSAGE,
            DETAILS_JSON=event.DETAILS_JSON or {},
            CREATED_AT=created_at,
        )