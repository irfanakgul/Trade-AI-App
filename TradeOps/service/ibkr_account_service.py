from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from ib_insync import IB


@dataclass
class AccountMetrics:
    ACCOUNT_ID: Optional[str]
    AVAILABLE_FUNDS: Optional[float]
    NET_LIQUIDATION: Optional[float]
    TOTAL_CASH_VALUE: Optional[float]
    SETTLED_CASH: Optional[float]
    BUYING_POWER: Optional[float]
    EXCESS_LIQUIDITY: Optional[float]
    GROSS_POSITION_VALUE: Optional[float]
    CASH_BALANCE_BASE: Optional[float]
    CASH_BALANCE_EUR: Optional[float]
    CASH_BALANCE_USD: Optional[float]


class IbkrAccountService:
    def __init__(self, ib: IB) -> None:
        self.ib = ib

    def get_account_metrics(self) -> AccountMetrics:
        summary_rows = self.ib.accountSummary()
        if not summary_rows:
            raise RuntimeError("IBKR account summary is empty.")

        return AccountMetrics(
            ACCOUNT_ID=self._extract_account_id(summary_rows),
            AVAILABLE_FUNDS=self._get_summary_value(summary_rows, "AvailableFunds"),
            NET_LIQUIDATION=self._get_summary_value(summary_rows, "NetLiquidation"),
            TOTAL_CASH_VALUE=self._get_summary_value(summary_rows, "TotalCashValue"),
            SETTLED_CASH=self._get_summary_value(summary_rows, "SettledCash"),
            BUYING_POWER=self._get_summary_value(summary_rows, "BuyingPower"),
            EXCESS_LIQUIDITY=self._get_summary_value(summary_rows, "ExcessLiquidity"),
            GROSS_POSITION_VALUE=self._get_summary_value(summary_rows, "GrossPositionValue"),
            CASH_BALANCE_BASE=self._get_summary_value(summary_rows, "CashBalance"),
            CASH_BALANCE_EUR=self._get_summary_value(summary_rows, "CashBalance", currency="EUR"),
            CASH_BALANCE_USD=self._get_summary_value(summary_rows, "CashBalance", currency="USD"),
        )

    def get_raw_account_summary_map(self) -> Dict[str, Dict[str, Optional[float]]]:
        rows = self.ib.accountSummary()
        result: Dict[str, Dict[str, Optional[float]]] = {}

        for row in rows:
            tag = getattr(row, "tag", None)
            currency = getattr(row, "currency", None) or "BASE"
            value = self._safe_float(getattr(row, "value", None))

            if not tag:
                continue

            if tag not in result:
                result[tag] = {}

            result[tag][currency] = value

        return result

    def _extract_account_id(self, summary_rows: List) -> Optional[str]:
        for row in summary_rows:
            account = getattr(row, "account", None)
            if account:
                return account
        return None

    def _get_summary_value(
        self,
        summary_rows: List,
        tag: str,
        currency: Optional[str] = None,
    ) -> Optional[float]:
        for row in summary_rows:
            row_tag = getattr(row, "tag", None)
            row_currency = getattr(row, "currency", None)

            if row_tag != tag:
                continue

            if currency is not None and row_currency != currency:
                continue

            return self._safe_float(getattr(row, "value", None))

        return None

    def _safe_float(self, value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None