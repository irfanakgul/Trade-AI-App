from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ib_insync import IB, LimitOrder, MarketOrder, Order, Stock


@dataclass
class OrderDetail:
    ACCOUNT_ID: Optional[str]
    ORDER_ID: Optional[int]
    PERM_ID: Optional[int]
    CLIENT_ID: Optional[int]
    PARENT_ID: Optional[int]
    SYMBOL: Optional[str]
    LOCAL_SYMBOL: Optional[str]
    SEC_TYPE: Optional[str]
    EXCHANGE: Optional[str]
    PRIMARY_EXCHANGE: Optional[str]
    CURRENCY: Optional[str]
    ACTION: Optional[str]
    TOTAL_QUANTITY: Optional[float]
    ORDER_TYPE: Optional[str]
    LMT_PRICE: Optional[float]
    AUX_PRICE: Optional[float]
    TIF: Optional[str]
    OUTSIDE_RTH: Optional[bool]
    TRANSMIT: Optional[bool]
    ORDER_REF: Optional[str]
    ORDER_STATUS: Optional[str]
    FILLED: Optional[float]
    REMAINING: Optional[float]
    AVG_FILL_PRICE: Optional[float]
    LAST_FILL_PRICE: Optional[float]
    WHY_HELD: Optional[str]
    MKT_CAP_PRICE: Optional[float]
    RAW_ORDER_JSON: Dict[str, Any]


@dataclass
class MarketBuyResult:
    SUCCESS: bool
    MESSAGE: str
    ORDER_ID: Optional[int] = None
    FILLED_QTY: Optional[float] = None
    AVG_FILL_PRICE: Optional[float] = None


@dataclass
class ExitOrderResult:
    SUCCESS: bool
    MESSAGE: str
    ORDER_ID: Optional[int] = None


class IbkrOrderService:
    def __init__(self, ib: IB) -> None:
        self.ib = ib

    def get_order_details(self) -> List[OrderDetail]:
        trades = self.ib.openTrades()
        results: List[OrderDetail] = []

        for trade in trades:
            contract = getattr(trade, "contract", None)
            order = getattr(trade, "order", None)
            status = getattr(trade, "orderStatus", None)

            raw_json = {
                "contract": self._to_serializable_dict(contract),
                "order": self._to_serializable_dict(order),
                "orderStatus": self._to_serializable_dict(status),
            }

            results.append(
                OrderDetail(
                    ACCOUNT_ID=getattr(order, "account", None) if order else None,
                    ORDER_ID=getattr(order, "orderId", None) if order else None,
                    PERM_ID=getattr(order, "permId", None) if order else None,
                    CLIENT_ID=getattr(order, "clientId", None) if order else None,
                    PARENT_ID=getattr(order, "parentId", None) if order else None,
                    SYMBOL=getattr(contract, "symbol", None) if contract else None,
                    LOCAL_SYMBOL=getattr(contract, "localSymbol", None) if contract else None,
                    SEC_TYPE=getattr(contract, "secType", None) if contract else None,
                    EXCHANGE=getattr(contract, "exchange", None) if contract else None,
                    PRIMARY_EXCHANGE=getattr(contract, "primaryExchange", None) if contract else None,
                    CURRENCY=getattr(contract, "currency", None) if contract else None,
                    ACTION=getattr(order, "action", None) if order else None,
                    TOTAL_QUANTITY=self._safe_float(getattr(order, "totalQuantity", None) if order else None),
                    ORDER_TYPE=getattr(order, "orderType", None) if order else None,
                    LMT_PRICE=self._safe_float(getattr(order, "lmtPrice", None) if order else None),
                    AUX_PRICE=self._safe_float(getattr(order, "auxPrice", None) if order else None),
                    TIF=getattr(order, "tif", None) if order else None,
                    OUTSIDE_RTH=getattr(order, "outsideRth", None) if order else None,
                    TRANSMIT=getattr(order, "transmit", None) if order else None,
                    ORDER_REF=getattr(order, "orderRef", None) if order else None,
                    ORDER_STATUS=getattr(status, "status", None) if status else None,
                    FILLED=self._safe_float(getattr(status, "filled", None) if status else None),
                    REMAINING=self._safe_float(getattr(status, "remaining", None) if status else None),
                    AVG_FILL_PRICE=self._safe_float(getattr(status, "avgFillPrice", None) if status else None),
                    LAST_FILL_PRICE=self._safe_float(getattr(status, "lastFillPrice", None) if status else None),
                    WHY_HELD=getattr(status, "whyHeld", None) if status else None,
                    MKT_CAP_PRICE=self._safe_float(getattr(status, "mktCapPrice", None) if status else None),
                    RAW_ORDER_JSON=raw_json,
                )
            )

        return results

    def get_market_price(
        self,
        symbol: str,
        exchange: str,
        currency: str,
    ) -> Optional[float]:
        contract = self._build_stock_contract(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        tickers = self.ib.reqTickers(contract)
        if not tickers:
            return None

        ticker = tickers[0]
        market_price = ticker.marketPrice()

        if market_price is None or market_price <= 0:
            market_price = getattr(ticker, "last", None) or getattr(ticker, "close", None)

        return self._safe_float(market_price)

    def place_market_buy(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        quantity: int,
    ) -> MarketBuyResult:
        if quantity <= 0:
            return MarketBuyResult(
                SUCCESS=False,
                MESSAGE="Quantity must be > 0.",
            )

        contract = self._build_stock_contract(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        order = MarketOrder("BUY", quantity)
        trade = self.ib.placeOrder(contract, order)

        success = self._wait_for_terminal_status(trade, timeout_seconds=20)

        status = getattr(trade.orderStatus, "status", None)
        if not success or status not in {"Filled", "Submitted", "PreSubmitted"}:
            return MarketBuyResult(
                SUCCESS=False,
                MESSAGE=f"Market buy failed. Final status: {status}",
                ORDER_ID=getattr(order, "orderId", None),
            )

        return MarketBuyResult(
            SUCCESS=True,
            MESSAGE=f"Market buy sent successfully. Final status: {status}",
            ORDER_ID=getattr(order, "orderId", None),
            FILLED_QTY=self._safe_float(getattr(trade.orderStatus, "filled", None)),
            AVG_FILL_PRICE=self._safe_float(getattr(trade.orderStatus, "avgFillPrice", None)),
        )

    def place_trailing_stop_sell(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        quantity: float,
        trailing_percent: float,
        oca_group: str | None = None,
    ) -> ExitOrderResult:
        contract = self._build_stock_contract(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        order = Order(
            action="SELL",
            orderType="TRAIL",
            totalQuantity=quantity,
            trailingPercent=trailing_percent,
            tif="GTC",
            outsideRth=True,
        )

        if oca_group:
            order.ocaGroup = oca_group
            order.ocaType = 1

        trade = self.ib.placeOrder(contract, order)
        self.ib.sleep(1)

        status = getattr(trade.orderStatus, "status", None)
        if status in {"Submitted", "PreSubmitted", "Filled"}:
            return ExitOrderResult(
                SUCCESS=True,
                MESSAGE=f"Trailing stop order placed. Status: {status}",
                ORDER_ID=getattr(order, "orderId", None),
            )

        return ExitOrderResult(
            SUCCESS=False,
            MESSAGE=f"Trailing stop order failed. Status: {status}",
            ORDER_ID=getattr(order, "orderId", None),
        )

    def place_target_limit_sell(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        quantity: float,
        target_price: float,
        oca_group: str | None = None,
    ) -> ExitOrderResult:
        contract = self._build_stock_contract(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        order = LimitOrder("SELL", quantity, target_price)
        order.tif = "GTC"
        order.outsideRth = True

        if oca_group:
            order.ocaGroup = oca_group
            order.ocaType = 1

        trade = self.ib.placeOrder(contract, order)
        self.ib.sleep(1)

        status = getattr(trade.orderStatus, "status", None)
        if status in {"Submitted", "PreSubmitted", "Filled"}:
            return ExitOrderResult(
                SUCCESS=True,
                MESSAGE=f"Target limit order placed. Status: {status}",
                ORDER_ID=getattr(order, "orderId", None),
            )

        return ExitOrderResult(
            SUCCESS=False,
            MESSAGE=f"Target limit order failed. Status: {status}",
            ORDER_ID=getattr(order, "orderId", None),
        )

    def build_oca_group(self) -> str:
        return f"OCA_{uuid.uuid4().hex[:12].upper()}"

    def _build_stock_contract(
        self,
        symbol: str,
        exchange: str,
        currency: str,
    ) -> Stock:
        normalized_exchange = self._map_exchange_for_ibkr(exchange)
        return Stock(symbol, normalized_exchange, currency)

    def _map_exchange_for_ibkr(self, exchange: str) -> str:
        normalized = exchange.strip().upper()
        if normalized == "EURONEXT":
            return "AEB"
        return normalized

    def _wait_for_terminal_status(self, trade, timeout_seconds: int = 20) -> bool:
        start = time.time()
        while time.time() - start < timeout_seconds:
            status = getattr(trade.orderStatus, "status", None)
            if status in {"Filled", "Cancelled", "Inactive"}:
                return True
            self.ib.sleep(0.5)
        return False

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
    
    def place_market_sell(
        self,
        symbol: str,
        exchange: str,
        currency: str,
        quantity: int,
    ) -> MarketBuyResult:
        if quantity <= 0:
            return MarketBuyResult(
                SUCCESS=False,
                MESSAGE="Quantity must be > 0.",
            )

        contract = self._build_stock_contract(symbol, exchange, currency)
        self.ib.qualifyContracts(contract)

        order = MarketOrder("SELL", quantity)
        trade = self.ib.placeOrder(contract, order)

        success = self._wait_for_terminal_status(trade, timeout_seconds=20)

        status = getattr(trade.orderStatus, "status", None)
        if not success or status not in {"Filled", "Submitted", "PreSubmitted"}:
            return MarketBuyResult(
                SUCCESS=False,
                MESSAGE=f"Market sell failed. Final status: {status}",
                ORDER_ID=getattr(order, "orderId", None),
            )

        return MarketBuyResult(
            SUCCESS=True,
            MESSAGE=f"Market sell sent successfully. Final status: {status}",
            ORDER_ID=getattr(order, "orderId", None),
            FILLED_QTY=self._safe_float(getattr(trade.orderStatus, "filled", None)),
            AVG_FILL_PRICE=self._safe_float(getattr(trade.orderStatus, "avgFillPrice", None)),
        )