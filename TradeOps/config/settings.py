from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
ENV_LOCAL_PATH = BASE_DIR / ".env_local"
CONFIG_ENV_PATH = BASE_DIR / "config.env"


def _load_env_files() -> None:
    if CONFIG_ENV_PATH.exists():
        load_dotenv(CONFIG_ENV_PATH, override=True)

    if ENV_LOCAL_PATH.exists():
        load_dotenv(ENV_LOCAL_PATH, override=True)


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    value = os.getenv(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: Optional[int] = None, required: bool = False) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        if required and default is None:
            raise ValueError(f"Missing required integer environment variable: {name}")
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be integer, got: {value}") from exc


def _get_list(name: str, default: Optional[List[str]] = None) -> List[str]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default or []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


@dataclass
class AppSettings:
    USERNAME: str
    EMAIL: str
    DEVICE_ID: Optional[str]
    APP_TIMEZONE: str

    IBKR_MODE: str
    IBKR_HOST: str
    IBKR_PORT: int
    IBKR_CLIENT_ID: int

    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    TELEGRAM_BOT_TOKEN: Optional[str]
    TELEGRAM_CHAT_ID: Optional[str]

    TOTAL_MAX_OPEN_POSITIONS: int
    MAX_DAILY_TRADE_COUNT: int
    EXIT_MODE: str

    ACCOUNT_SNAPSHOT_POST_EU_BUY_TIME: str
    ACCOUNT_SNAPSHOT_POST_EU_CLOSE_TIME: str
    ACCOUNT_SNAPSHOT_EOD_TIME: str

    ACCOUNT_SNAPSHOT_ENABLED: bool
    END_OF_DAY_RECONCILE_ENABLED: bool
    FORCED_SELL_ENABLED: bool
    TELEGRAM_ENABLED: bool
    MAIL_ENABLED: bool

    EXCHANGE_PRIORITY: List[str]

    RUN_ON_SCHEDULE: bool
    MANUAL_TRIGGER_PIPELINES: List[str]


def load_settings() -> AppSettings:
    _load_env_files()

    settings = AppSettings(
        USERNAME=_get_env("USERNAME", required=True),
        EMAIL=_get_env("EMAIL", required=True),
        DEVICE_ID=_get_env("DEVICE_ID", default=None, required=False),
        APP_TIMEZONE=_get_env("APP_TIMEZONE", default="Europe/Amsterdam", required=True),

        IBKR_MODE=_get_env("IBKR_MODE", default="PAPER", required=True),
        IBKR_HOST=_get_env("IBKR_HOST", default="127.0.0.1", required=True),
        IBKR_PORT=_get_int("IBKR_PORT", default=7497, required=True),
        IBKR_CLIENT_ID=_get_int("IBKR_CLIENT_ID", default=1, required=True),

        DB_HOST=_get_env("DB_HOST", required=True),
        DB_PORT=_get_int("DB_PORT", default=5432, required=True),
        DB_NAME=_get_env("DB_NAME", required=True),
        DB_USER=_get_env("DB_USER", required=True),
        DB_PASSWORD=_get_env("DB_PASSWORD", required=True),

        TELEGRAM_BOT_TOKEN=_get_env("TELEGRAM_BOT_TOKEN", default=None),
        TELEGRAM_CHAT_ID=_get_env("TELEGRAM_CHAT_ID", default=None),

        TOTAL_MAX_OPEN_POSITIONS=_get_int("TOTAL_MAX_OPEN_POSITIONS", required=True),
        MAX_DAILY_TRADE_COUNT=_get_int("MAX_DAILY_TRADE_COUNT", required=True),
        EXIT_MODE=_get_env("EXIT_MODE", required=True),

        ACCOUNT_SNAPSHOT_POST_EU_BUY_TIME=_get_env("ACCOUNT_SNAPSHOT_POST_EU_BUY_TIME", default="11:00", required=True),
        ACCOUNT_SNAPSHOT_POST_EU_CLOSE_TIME=_get_env("ACCOUNT_SNAPSHOT_POST_EU_CLOSE_TIME", default="18:00", required=True),
        ACCOUNT_SNAPSHOT_EOD_TIME=_get_env("ACCOUNT_SNAPSHOT_EOD_TIME", default="23:30", required=True),

        ACCOUNT_SNAPSHOT_ENABLED=_get_bool("ACCOUNT_SNAPSHOT_ENABLED", default=True),
        END_OF_DAY_RECONCILE_ENABLED=_get_bool("END_OF_DAY_RECONCILE_ENABLED", default=True),
        FORCED_SELL_ENABLED=_get_bool("FORCED_SELL_ENABLED", default=True),
        TELEGRAM_ENABLED=_get_bool("TELEGRAM_ENABLED", default=False),
        MAIL_ENABLED=_get_bool("MAIL_ENABLED", default=False),

        EXCHANGE_PRIORITY=_get_list("EXCHANGE_PRIORITY", default=[]),

        RUN_ON_SCHEDULE=_get_bool("RUN_ON_SCHEDULE", default=False),
        MANUAL_TRIGGER_PIPELINES=_get_list("MANUAL_TRIGGER_PIPELINES", default=[]),
    )

    _validate_settings(settings)
    return settings


def _validate_settings(settings: AppSettings) -> None:
    allowed_exit_modes = {"TARGET_PRICE", "INCREMENTAL"}
    if settings.EXIT_MODE not in allowed_exit_modes:
        raise ValueError(
            f"EXIT_MODE must be one of {allowed_exit_modes}, got: {settings.EXIT_MODE}"
        )

    allowed_ibkr_modes = {"PAPER", "LIVE"}
    if settings.IBKR_MODE not in allowed_ibkr_modes:
        raise ValueError(
            f"IBKR_MODE must be one of {allowed_ibkr_modes}, got: {settings.IBKR_MODE}"
        )

    if settings.TOTAL_MAX_OPEN_POSITIONS <= 0:
        raise ValueError("TOTAL_MAX_OPEN_POSITIONS must be > 0")

    if settings.MAX_DAILY_TRADE_COUNT <= 0:
        raise ValueError("MAX_DAILY_TRADE_COUNT must be > 0")

    if not settings.APP_TIMEZONE.strip():
        raise ValueError("APP_TIMEZONE cannot be empty")

    for name in [
        "ACCOUNT_SNAPSHOT_POST_EU_BUY_TIME",
        "ACCOUNT_SNAPSHOT_POST_EU_CLOSE_TIME",
        "ACCOUNT_SNAPSHOT_EOD_TIME",
    ]:
        value = getattr(settings, name)
        if ":" not in value:
            raise ValueError(f"{name} must be in HH:MM format")

    allowed_manual_pipelines = {
        "ACCOUNT_SNAPSHOT_POST_EU_BUY",
        "ACCOUNT_SNAPSHOT_POST_EU_CLOSE",
        "ACCOUNT_SNAPSHOT_EOD",
        "BUY_PREPARE",
        "BUY_EXECUTION",
        "FORCED_SELL",
        "END_OF_DAY_RECONCILE",
}

    invalid = [x for x in settings.MANUAL_TRIGGER_PIPELINES if x not in allowed_manual_pipelines]
    if invalid:
        raise ValueError(
            f"Invalid MANUAL_TRIGGER_PIPELINES values: {invalid}. "
            f"Allowed: {sorted(allowed_manual_pipelines)}"
        )