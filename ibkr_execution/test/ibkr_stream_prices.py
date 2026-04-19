import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional


from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper


@dataclass
class StreamRequest:
    req_id: int
    symbol: str
    exchange: str
    currency: str
    sec_type: str = "STK"
    primary_exchange: Optional[str] = None


class IBKRMultiExchangeStreamApp(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, self)

        self.connected_event = threading.Event()
        self.symbol_map: Dict[int, StreamRequest] = {}
        self.market_data_type_map: Dict[int, int] = {}
        self.data: Dict[str, Dict[str, object]] = {}

    def nextValidId(self, orderId: int) -> None:
        super().nextValidId(orderId)
        print(f"[IBKR] connected | nextValidId={orderId}")
        self.connected_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="") -> None:
        info_codes = {
            2103, 2104, 2106, 2108, 2158,
            10167, 10089
        }
        prefix = "[IBKR][INFO]" if errorCode in info_codes else "[IBKR][ERROR]"

        msg = f"{prefix} reqId={reqId} code={errorCode} message={errorString}"
        if advancedOrderRejectJson:
            msg += f" | reject_json={advancedOrderRejectJson}"

        print(msg)

    def marketDataType(self, reqId: int, marketDataType: int) -> None:
        """
        1 = live
        2 = frozen
        3 = delayed
        4 = delayed frozen
        """
        self.market_data_type_map[reqId] = marketDataType

        req = self.symbol_map.get(reqId)
        if req is None:
            symbol_text = f"REQ_{reqId}"
        else:
            symbol_text = f"{req.exchange}:{req.symbol}"

        type_name = {
            1: "LIVE",
            2: "FROZEN",
            3: "DELAYED",
            4: "DELAYED_FROZEN",
        }.get(marketDataType, str(marketDataType))

        print(f"[IBKR] marketDataType | req_id={reqId} symbol={symbol_text} type={type_name}")

    def _symbol_key(self, req: StreamRequest) -> str:
        return f"{req.exchange}:{req.symbol}"

    def _ensure_symbol(self, symbol_key: str) -> None:
        if symbol_key not in self.data:
            self.data[symbol_key] = {}

    def _format_unix_seconds(self, ts_value: object) -> Optional[str]:
        try:
            ts_int = int(float(str(ts_value)))
            return datetime.fromtimestamp(ts_int).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _format_unix_millis(self, ts_value: object) -> Optional[str]:
        try:
            ts_ms = int(float(str(ts_value)))
            return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _print_snapshot(self, reqId: int, updated_field: str) -> None:
        req = self.symbol_map.get(reqId)
        if req is None:
            return

        symbol_key = self._symbol_key(req)
        row = self.data[symbol_key]

        bid = row.get("bid")
        if bid is None:
            bid = row.get("delayed_bid")

        ask = row.get("ask")
        if ask is None:
            ask = row.get("delayed_ask")

        last = row.get("last")
        if last is None:
            last = row.get("delayed_last")

        volume = row.get("volume")
        if volume is None:
            volume = row.get("delayed_volume")

        trade_ts = row.get("last_timestamp")
        if trade_ts is None:
            trade_ts = row.get("rt_trade_time")

        data_type = self.market_data_type_map.get(reqId)
        data_type_name = {
            1: "LIVE",
            2: "FROZEN",
            3: "DELAYED",
            4: "DELAYED_FROZEN",
        }.get(data_type, "UNKNOWN")

        now_str = time.strftime("%H:%M:%S")

        print(
            f"[{now_str}] "
            f"{req.exchange:<8} "
            f"{req.symbol:<8} "
            f"type={data_type_name:<14} "
            f"last={last} bid={bid} ask={ask} "
            f"volume={volume} "
            f"trade_ts={trade_ts} "
            f"updated={updated_field}"
        )

    def tickPrice(self, reqId: int, tickType: int, price: float, attrib) -> None:
        req = self.symbol_map.get(reqId)
        if req is None:
            return

        symbol_key = self._symbol_key(req)
        self._ensure_symbol(symbol_key)

        tick_name = {
            # live/default
            1: "bid",
            2: "ask",
            4: "last",
            6: "high",
            7: "low",
            9: "close",
            14: "open",

            # delayed
            66: "delayed_bid",
            67: "delayed_ask",
            68: "delayed_last",
            72: "delayed_high",
            73: "delayed_low",
            75: "delayed_close",
        }.get(tickType, f"tick_{tickType}")

        self.data[symbol_key][tick_name] = price

        if tick_name in {
            "bid", "ask", "last",
            "delayed_bid", "delayed_ask", "delayed_last",
        }:
            self._print_snapshot(reqId, tick_name)

    def tickSize(self, reqId: int, tickType: int, size: int) -> None:
        req = self.symbol_map.get(reqId)
        if req is None:
            return

        symbol_key = self._symbol_key(req)
        self._ensure_symbol(symbol_key)

        tick_name = {
            # live/default
            0: "bid_size",
            3: "ask_size",
            5: "last_size",
            8: "volume",

            # delayed
            69: "delayed_bid_size",
            70: "delayed_ask_size",
            71: "delayed_last_size",
            74: "delayed_volume",
        }.get(tickType, f"size_{tickType}")

        self.data[symbol_key][tick_name] = size

        if tick_name in {"volume", "delayed_volume"}:
            self._print_snapshot(reqId, tick_name)

    def tickString(self, reqId: int, tickType: int, value: str) -> None:
        req = self.symbol_map.get(reqId)
        if req is None:
            return

        symbol_key = self._symbol_key(req)
        self._ensure_symbol(symbol_key)

        if tickType == 45:
            # Last Timestamp (unix seconds)
            formatted = self._format_unix_seconds(value)
            self.data[symbol_key]["last_timestamp_raw"] = value
            self.data[symbol_key]["last_timestamp"] = formatted if formatted else value
            self._print_snapshot(reqId, "last_timestamp")

        elif tickType == 48:
            # RTVolume:
            # lastPrice;lastSize;lastTime;totalVolume;VWAP;singleTradeFlag
            parts = value.split(";")
            if len(parts) >= 6:
                last_price = parts[0]
                last_size = parts[1]
                last_time = parts[2]
                total_volume = parts[3]
                vwap = parts[4]

                self.data[symbol_key]["rt_last_price"] = last_price
                self.data[symbol_key]["rt_last_size"] = last_size
                self.data[symbol_key]["rt_total_volume"] = total_volume
                self.data[symbol_key]["rt_vwap"] = vwap

                formatted_time = self._format_unix_millis(last_time)
                self.data[symbol_key]["rt_trade_time"] = formatted_time if formatted_time else last_time
                self.data[symbol_key]["volume"] = total_volume

                try:
                    self.data[symbol_key]["last"] = float(last_price)
                except Exception:
                    self.data[symbol_key]["last"] = last_price

                self._print_snapshot(reqId, "rtvolume")

        elif tickType == 77:
            # RTTradeVolume
            parts = value.split(";")
            if len(parts) >= 6:
                last_price = parts[0]
                last_size = parts[1]
                last_time = parts[2]
                total_volume = parts[3]
                vwap = parts[4]

                self.data[symbol_key]["rt_trade_last_price"] = last_price
                self.data[symbol_key]["rt_trade_last_size"] = last_size
                self.data[symbol_key]["rt_trade_total_volume"] = total_volume
                self.data[symbol_key]["rt_trade_vwap"] = vwap

                formatted_time = self._format_unix_millis(last_time)
                self.data[symbol_key]["rt_trade_time"] = formatted_time if formatted_time else last_time
                self._print_snapshot(reqId, "rt_trade_volume")


def build_stock_contract(req: StreamRequest) -> Contract:
    contract = Contract()
    contract.symbol = req.symbol
    contract.secType = req.sec_type
    contract.exchange = req.exchange
    contract.currency = req.currency

    if req.primary_exchange:
        contract.primaryExchange = req.primary_exchange

    return contract


def build_requests_from_input(
    exchange_symbol_map: Dict[str, List[str]],
    start_req_id: int = 1001,
) -> List[StreamRequest]:
    """
    Input example:
    {
        "NASDAQ": ["AAPL", "NVDA", "MSFT"],
        "NYSE": ["KO", "IBM"],
        "AEB": ["ASM", "PHIA"]
    }
    """

    requests: List[StreamRequest] = []
    req_id = start_req_id

    for exchange, symbols in exchange_symbol_map.items():
        for symbol in symbols:
            if exchange == "NASDAQ":
                requests.append(
                    StreamRequest(
                        req_id=req_id,
                        symbol=symbol,
                        exchange="NASDAQ",
                        primary_exchange="NASDAQ",
                        currency="USD",
                    )
                )
            elif exchange == "NYSE":
                requests.append(
                    StreamRequest(
                        req_id=req_id,
                        symbol=symbol,
                        exchange="NYSE",
                        primary_exchange="NYSE",
                        currency="USD",
                    )
                )
            elif exchange == "AEB":
                requests.append(
                    StreamRequest(
                        req_id=req_id,
                        symbol=symbol,
                        exchange="AEB",
                        primary_exchange="AEB",
                        currency="EUR",
                    )
                )
            else:
                raise ValueError(
                    f"Unsupported exchange: {exchange}. "
                    f"Supported values: NASDAQ, NYSE, AEB"
                )

            req_id += 1

    return requests


def main() -> None:
    # =====================================================
    # CONNECTION
    # =====================================================
    HOST = "127.0.0.1"
    PORT = 7497
    CLIENT_ID = 31

    # 1 = live
    # 2 = frozen
    # 3 = delayed
    # 4 = delayed frozen
    MARKET_DATA_TYPE = 3

    # =====================================================
    # INPUT: EXCHANGE -> SYMBOL LIST
    # =====================================================
    exchange_symbol_map = {
        "NASDAQ": ["GRAB", "NTAP", "BKNG"],
        "NYSE": ["DOC", "HPQ","MA","MRSH","UBER"],
        "AEB": ["CVC", "EXO","WKL"],
    }
    # =====================================================

    requests = build_requests_from_input(exchange_symbol_map)

    app = IBKRMultiExchangeStreamApp()
    app.connect(HOST, PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        app.disconnect()
        raise TimeoutError(
            "Could not connect to TWS. "
            "Check TWS API settings, port, and login state."
        )

    app.reqMarketDataType(MARKET_DATA_TYPE)

    for req in requests:
        contract = build_stock_contract(req)
        app.symbol_map[req.req_id] = req

        print(
            f"[IBKR] subscribing | "
            f"req_id={req.req_id} "
            f"exchange={req.exchange} "
            f"symbol={req.symbol}"
        )

        # generic ticks:
        # 233 = RTVolume
        generic_ticks = "233"

        app.reqMktData(
            req.req_id,
            contract,
            generic_ticks,
            False,
            False,
            [],
        )

    print("[IBKR] multi-exchange streaming started. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[IBKR] stopping subscriptions...")

        for req in requests:
            app.cancelMktData(req.req_id)

        time.sleep(1)
        app.disconnect()
        print("[IBKR] disconnected.")


if __name__ == "__main__":
    main()