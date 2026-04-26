from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ib_insync import IB


@dataclass
class PositionSnapshot:
    OPEN_POSITION_COUNT: int
    OPEN_SYMBOLS: str


@dataclass
class PositionDetail:
    ACCOUNT_ID: Optional[str]
    SYMBOL: Optional[str]
    LOCAL_SYMBOL: Optional[str]
    SEC_TYPE: Optional[str]
    EXCHANGE: Optional[str]
    PRIMARY_EXCHANGE: Optional[str]
    CURRENCY: Optional[str]
    POSITION_QTY: Optional[float]
    AVG_COST: Optional[float]
    MARKET_PRICE: Optional[float]
    MARKET_VALUE: Optional[float]
    UNREALIZED_PNL: Optional[float]
    REALIZED_PNL: Optional[float]
    RAW_POSITION_JSON: Dict[str, Any]


class IbkrPositionService:
    def __init__(self, ib: IB) -> None:
        self.ib = ib

    def get_position_snapshot(self) -> PositionSnapshot:
        positions = self.ib.positions()

        symbols: List[str] = []
        for position in positions:
            contract = getattr(position, "contract", None)
            symbol = getattr(contract, "symbol", None) if contract else None
            qty = getattr(position, "position", None)

            if symbol and qty and float(qty) != 0:
                symbols.append(symbol)

        unique_symbols = sorted(set(symbols))

        return PositionSnapshot(
            OPEN_POSITION_COUNT=len(unique_symbols),
            OPEN_SYMBOLS=",".join(unique_symbols),
        )

    def get_position_details(self) -> List[PositionDetail]:
        positions = self.ib.positions()
        results: List[PositionDetail] = []

        for position in positions:
            contract = getattr(position, "contract", None)
            qty = getattr(position, "position", None)

            if qty is None or float(qty) == 0:
                continue

            raw_json = {
                "account": getattr(position, "account", None),
                "position": self._safe_float(getattr(position, "position", None)),
                "avgCost": self._safe_float(getattr(position, "avgCost", None)),
                "contract": self._to_serializable_dict(contract),
            }

            results.append(
                PositionDetail(
                    ACCOUNT_ID=getattr(position, "account", None),
                    SYMBOL=getattr(contract, "symbol", None) if contract else None,
                    LOCAL_SYMBOL=getattr(contract, "localSymbol", None) if contract else None,
                    SEC_TYPE=getattr(contract, "secType", None) if contract else None,
                    EXCHANGE=getattr(contract, "exchange", None) if contract else None,
                    PRIMARY_EXCHANGE=getattr(contract, "primaryExchange", None) if contract else None,
                    CURRENCY=getattr(contract, "currency", None) if contract else None,
                    POSITION_QTY=self._safe_float(getattr(position, "position", None)),
                    AVG_COST=self._safe_float(getattr(position, "avgCost", None)),
                    MARKET_PRICE=None,
                    MARKET_VALUE=None,
                    UNREALIZED_PNL=None,
                    REALIZED_PNL=None,
                    RAW_POSITION_JSON=raw_json,
                )
            )

        return results

    def _safe_float(self, value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_serializable_dict(self, obj: object) -> Dict[str, Any]:
        if obj is None:
            return {}

        result: Dict[str, Any] = {}
        for key, value in vars(obj).items():
            if key.startswith("_"):
                continue
            result[key] = self._normalize_value(value)
        return result

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, list):
            return [self._normalize_value(v) for v in value]
        if isinstance(value, tuple):
            return [self._normalize_value(v) for v in value]
        if isinstance(value, dict):
            return {str(k): self._normalize_value(v) for k, v in value.items()}
        return str(value)