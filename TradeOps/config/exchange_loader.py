from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import yaml


BASE_DIR = Path(__file__).resolve().parent.parent
EXCHANGES_YAML_PATH = BASE_DIR / "config" / "exchanges.yaml"


@dataclass
class ExchangeConfig:
    EXCHANGE: str
    ENABLED: bool
    BUY_PREPARE_TIME: str
    SIGNAL_TIME: str
    EOD_RECONCILE_TIME: str
    FORCED_SELL_TIME: str
    MAX_OPEN_POSITIONS: int
    STOP_LOSS_PCT: float
    BUDGET_PCT: float
    MAX_HOLDING_DAY: int
    FORCED_SELL_MODE: str
    CURRENCY: str


def load_exchange_configs() -> List[ExchangeConfig]:
    if not EXCHANGES_YAML_PATH.exists():
        raise FileNotFoundError(f"Missing exchanges config file: {EXCHANGES_YAML_PATH}")

    with open(EXCHANGES_YAML_PATH, "r", encoding="utf-8") as file:
        raw_data = yaml.safe_load(file) or {}

    exchanges = raw_data.get("EXCHANGES", [])
    if not isinstance(exchanges, list):
        raise ValueError("config/exchanges.yaml must contain EXCHANGES as a list")

    configs: List[ExchangeConfig] = []

    for item in exchanges:
        config = ExchangeConfig(
            EXCHANGE=item["EXCHANGE"],
            ENABLED=bool(item["ENABLED"]),
            BUY_PREPARE_TIME=str(item["BUY_PREPARE_TIME"]),
            SIGNAL_TIME=str(item["SIGNAL_TIME"]),
            EOD_RECONCILE_TIME=str(item["EOD_RECONCILE_TIME"]),
            FORCED_SELL_TIME=str(item["FORCED_SELL_TIME"]),
            MAX_OPEN_POSITIONS=int(item["MAX_OPEN_POSITIONS"]),
            STOP_LOSS_PCT=float(item["STOP_LOSS_PCT"]),
            BUDGET_PCT=float(item["BUDGET_PCT"]),
            MAX_HOLDING_DAY=int(item["MAX_HOLDING_DAY"]),
            FORCED_SELL_MODE=str(item["FORCED_SELL_MODE"]).upper(),
            CURRENCY=str(item["CURRENCY"]),
        )
        _validate_exchange_config(config)
        configs.append(config)

    return configs


def get_enabled_exchanges() -> List[ExchangeConfig]:
    return [exchange for exchange in load_exchange_configs() if exchange.ENABLED]


def get_exchange_config(exchange_code: str) -> ExchangeConfig:
    normalized = exchange_code.strip().upper()
    for exchange in load_exchange_configs():
        if exchange.EXCHANGE.strip().upper() == normalized:
            return exchange
    raise ValueError(f"Exchange config not found: {exchange_code}")


def _validate_exchange_config(config: ExchangeConfig) -> None:
    if not config.EXCHANGE.strip():
        raise ValueError("EXCHANGE cannot be empty")

    if config.MAX_OPEN_POSITIONS < 0:
        raise ValueError(f"{config.EXCHANGE}: MAX_OPEN_POSITIONS cannot be negative")

    if config.STOP_LOSS_PCT <= 0:
        raise ValueError(f"{config.EXCHANGE}: STOP_LOSS_PCT must be > 0")

    if config.BUDGET_PCT < 0:
        raise ValueError(f"{config.EXCHANGE}: BUDGET_PCT cannot be negative")

    if config.MAX_HOLDING_DAY <= 0:
        raise ValueError(f"{config.EXCHANGE}: MAX_HOLDING_DAY must be > 0")

    if config.FORCED_SELL_MODE not in {"FIXED", "FLEXIBLE"}:
        raise ValueError(
            f"{config.EXCHANGE}: FORCED_SELL_MODE must be FIXED or FLEXIBLE"
        )

    for field_name in [
        "BUY_PREPARE_TIME",
        "SIGNAL_TIME",
        "EOD_RECONCILE_TIME",
        "FORCED_SELL_TIME",
    ]:
        value = getattr(config, field_name)
        if ":" not in value:
            raise ValueError(f"{config.EXCHANGE}: {field_name} must be in HH:MM format")