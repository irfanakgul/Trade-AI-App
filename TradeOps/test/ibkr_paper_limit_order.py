import time
import threading
from dataclasses import dataclass
from typing import Optional

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


@dataclass
class TradeRequest:
    symbol: str
    side: str  # BUY or SELL
    quantity: float
    limit_price: float
    exchange: str = "SMART"
    currency: str = "USD"
    sec_type: str = "STK"
    primary_exchange: Optional[str] = None
    transmit: bool = False


class IBKRApp(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, self)
        self.next_order_id: Optional[int] = None
        self.connected_event = threading.Event()
        self.order_event = threading.Event()
        self.error_messages: list[str] = []

    def nextValidId(self, orderId: int) -> None:
        super().nextValidId(orderId)
        self.next_order_id = orderId
        print(f"[IBKR] nextValidId received: {orderId}")
        self.connected_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="") -> None:
        info_codes = {2103, 2104, 2106, 2108, 2158}
        prefix = "[IBKR][INFO]" if errorCode in info_codes else "[IBKR][ERROR]"

        msg = (
            f"{prefix} reqId={reqId} code={errorCode} "
            f"message={errorString}"
        )
        if advancedOrderRejectJson:
            msg += f" | reject_json={advancedOrderRejectJson}"

        self.error_messages.append(msg)
        print(msg)

    def openOrder(self, orderId, contract, order, orderState) -> None:
        print(
            f"[IBKR] openOrder | "
            f"orderId={orderId} "
            f"symbol={contract.symbol} "
            f"action={order.action} "
            f"type={order.orderType} "
            f"qty={order.totalQuantity} "
            f"limit_price={getattr(order, 'lmtPrice', None)} "
            f"status={orderState.status}"
        )
        self.order_event.set()

    def openOrderEnd(self) -> None:
        print("[IBKR] openOrderEnd")

    def orderStatus(
        self,
        orderId,
        status,
        filled,
        remaining,
        avgFillPrice,
        permId,
        parentId,
        lastFillPrice,
        clientId,
        whyHeld,
        mktCapPrice,
    ) -> None:
        print(
            f"[IBKR] orderStatus | "
            f"orderId={orderId} "
            f"status={status} "
            f"filled={filled} "
            f"remaining={remaining} "
            f"avgFillPrice={avgFillPrice}"
        )
        self.order_event.set()


def build_stock_contract(req: TradeRequest) -> Contract:
    contract = Contract()
    contract.symbol = req.symbol
    contract.secType = req.sec_type
    contract.exchange = req.exchange
    contract.currency = req.currency

    if req.primary_exchange:
        contract.primaryExchange = req.primary_exchange

    return contract


def build_limit_order(req: TradeRequest) -> Order:
    order = Order()
    order.action = req.side.upper()
    order.orderType = "LMT"
    order.totalQuantity = req.quantity
    order.lmtPrice = req.limit_price
    order.transmit = req.transmit
    order.tif = "DAY"

    # Compatibility for some TWS/API combinations
    try:
        order.eTradeOnly = False
    except Exception:
        pass

    try:
        order.firmQuoteOnly = False
    except Exception:
        pass

    return order


def place_limit_order(
    host: str,
    port: int,
    client_id: int,
    trade_req: TradeRequest,
) -> None:
    app = IBKRApp()
    app.connect(host, port, client_id)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        app.disconnect()
        raise TimeoutError(
            "Could not receive nextValidId from TWS. "
            "Check TWS API settings, port, and login state."
        )

    if app.next_order_id is None:
        app.disconnect()
        raise RuntimeError("next_order_id is None.")

    contract = build_stock_contract(trade_req)
    order = build_limit_order(trade_req)

    print(
        f"[IBKR] placing order | "
        f"symbol={trade_req.symbol} "
        f"side={trade_req.side} "
        f"qty={trade_req.quantity} "
        f"limit={trade_req.limit_price} "
        f"transmit={trade_req.transmit}"
    )

    app.placeOrder(app.next_order_id, contract, order)

    # Ask TWS to send open order callbacks
    app.reqOpenOrders()

    # Wait a bit for callbacks
    app.order_event.wait(timeout=20)
    time.sleep(5)

    app.disconnect()
    print("[IBKR] disconnected.")


def main() -> None:
    # =====================================================
    # TEST INPUTS
    # =====================================================
    HOST = "127.0.0.1"
    PORT = 7497          # TWS paper port
    CLIENT_ID = 17

    SYMBOL = "AAPL"
    SIDE = "SELL"         # BUY or SELL
    QUANTITY = 1
    LIMIT_PRICE = 254.00

    EXCHANGE = "SMART"
    CURRENCY = "USD"
    PRIMARY_EXCHANGE = "NASDAQ"

    # First keep this False
    # After successful test, switch to True for real paper order
    TRANSMIT = True
    # =====================================================

    trade_req = TradeRequest(
        symbol=SYMBOL,
        side=SIDE,
        quantity=QUANTITY,
        limit_price=LIMIT_PRICE,
        exchange=EXCHANGE,
        currency=CURRENCY,
        primary_exchange=PRIMARY_EXCHANGE,
        transmit=TRANSMIT,
    )

    place_limit_order(
        host=HOST,
        port=PORT,
        client_id=CLIENT_ID,
        trade_req=trade_req,
    )


if __name__ == "__main__":
    main()