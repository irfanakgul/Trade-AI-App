import threading
import time
from typing import Dict, Optional, List, Tuple

from ibapi.client import EClient
from ibapi.wrapper import EWrapper


class IBKRAccountApp(EWrapper, EClient):
    def __init__(self) -> None:
        EClient.__init__(self, self)

        self.connected_event = threading.Event()
        self.account_ready_event = threading.Event()

        self.next_order_id: Optional[int] = None
        self.managed_accounts: list[str] = []

        # account -> tag -> currency -> value
        self.account_values: Dict[str, Dict[str, Dict[str, str]]] = {}

    def nextValidId(self, orderId: int) -> None:
        super().nextValidId(orderId)
        self.next_order_id = orderId
        print(f"[IBKR] connected | nextValidId={orderId}")
        self.connected_event.set()

    def managedAccounts(self, accountsList: str) -> None:
        self.managed_accounts = [acc.strip() for acc in accountsList.split(",") if acc.strip()]
        print(f"[IBKR] managedAccounts | {self.managed_accounts}")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson="") -> None:
        info_codes = {2103, 2104, 2106, 2108, 2158}
        prefix = "[IBKR][INFO]" if errorCode in info_codes else "[IBKR][ERROR]"

        msg = f"{prefix} reqId={reqId} code={errorCode} message={errorString}"
        if advancedOrderRejectJson:
            msg += f" | reject_json={advancedOrderRejectJson}"
        print(msg)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str) -> None:
        if account not in self.account_values:
            self.account_values[account] = {}

        if tag not in self.account_values[account]:
            self.account_values[account][tag] = {}

        ccy_key = currency if currency is not None else ""
        self.account_values[account][tag][ccy_key] = value

        print(
            f"[IBKR] accountSummary | reqId={reqId} "
            f"account={account} tag={tag} value={value} currency={ccy_key}"
        )

    def accountSummaryEnd(self, reqId: int) -> None:
        print(f"[IBKR] accountSummaryEnd | reqId={reqId}")
        self.account_ready_event.set()


def get_all_currencies_for_tag(
    data: Dict[str, Dict[str, Dict[str, str]]],
    account: str,
    tag: str,
) -> Dict[str, str]:
    return data.get(account, {}).get(tag, {})


def get_best_value(
    data: Dict[str, Dict[str, Dict[str, str]]],
    account: str,
    tag: str,
    preferred_order: Optional[List[str]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns:
        (value, currency_used)
    """
    if preferred_order is None:
        preferred_order = ["BASE", "EUR", "USD", ""]

    tag_map = get_all_currencies_for_tag(data, account, tag)
    if not tag_map:
        return None, None

    for ccy in preferred_order:
        if ccy in tag_map:
            return tag_map[ccy], ccy

    # Fallback: return first available
    first_ccy = next(iter(tag_map.keys()))
    return tag_map[first_ccy], first_ccy


def print_tag_block(
    app: IBKRAccountApp,
    account: str,
    tag: str,
    label: str,
) -> None:
    raw_map = get_all_currencies_for_tag(app.account_values, account, tag)
    best_value, best_ccy = get_best_value(app.account_values, account, tag)

    print(f"{label:<30} | tag={tag}")
    print(f"  resolved_value              = {best_value}")
    print(f"  resolved_currency           = {best_ccy}")
    print(f"  raw_values                  = {raw_map if raw_map else None}")


def print_account_report(app: IBKRAccountApp) -> None:
    if not app.managed_accounts:
        print("[IBKR] No managed accounts found.")
        return

    for account in app.managed_accounts:
        print("\n" + "=" * 70)
        print(f"ACCOUNT REPORT | account={account}")
        print("=" * 70)

        main_fields = [
            ("AvailableFunds", "Available funds"),
            ("FullAvailableFunds", "Full available funds"),
            ("NetLiquidation", "Net liquidation"),
            ("TotalCashValue", "Total cash value"),
            ("SettledCash", "Settled cash"),
            ("BuyingPower", "Buying power"),
            ("ExcessLiquidity", "Excess liquidity"),
            ("FullExcessLiquidity", "Full excess liquidity"),
            ("LookAheadAvailableFunds", "Look-ahead available funds"),
            ("LookAheadExcessLiquidity", "Look-ahead excess liquidity"),
            ("GrossPositionValue", "Gross position value"),
            ("MaintMarginReq", "Maintenance margin req"),
            ("InitMarginReq", "Initial margin req"),
            ("CashBalance", "Cash balance"),
        ]

        print("\n--- Main Account Fields ---\n")
        for tag, label in main_fields:
            print_tag_block(app, account, tag, label)
            print()

        print("\n--- Currency / Ledger Oriented Fields ---\n")
        for ccy in ["BASE", "EUR", "USD"]:
            total_cash = get_all_currencies_for_tag(app.account_values, account, "TotalCashValue").get(ccy)
            cash_balance = get_all_currencies_for_tag(app.account_values, account, "CashBalance").get(ccy)
            settled_cash = get_all_currencies_for_tag(app.account_values, account, "SettledCash").get(ccy)

            if total_cash is not None or cash_balance is not None or settled_cash is not None:
                print(f"Currency = {ccy}")
                if total_cash is not None:
                    print(f"  TotalCashValue[{ccy}]      = {total_cash}")
                if cash_balance is not None:
                    print(f"  CashBalance[{ccy}]         = {cash_balance}")
                if settled_cash is not None:
                    print(f"  SettledCash[{ccy}]         = {settled_cash}")
                print()

        print("\n--- Suggested Operational Reads ---\n")

        available_funds, available_funds_ccy = get_best_value(app.account_values, account, "AvailableFunds")
        net_liq, net_liq_ccy = get_best_value(app.account_values, account, "NetLiquidation")
        total_cash, total_cash_ccy = get_best_value(app.account_values, account, "TotalCashValue")
        settled_cash, settled_cash_ccy = get_best_value(app.account_values, account, "SettledCash")
        cash_balance, cash_balance_ccy = get_best_value(app.account_values, account, "CashBalance")

        print(f"AvailableFunds              = {available_funds} ({available_funds_ccy})")
        print(f"NetLiquidation              = {net_liq} ({net_liq_ccy})")
        print(f"TotalCashValue              = {total_cash} ({total_cash_ccy})")
        print(f"SettledCash                 = {settled_cash} ({settled_cash_ccy})")
        print(f"CashBalance                 = {cash_balance} ({cash_balance_ccy})")

        print("\nInterpretation:")
        print("- AvailableFunds: new trade açmak için en önemli alan")
        print("- NetLiquidation: toplam portföy büyüklüğü")
        print("- TotalCashValue: toplam nakit görünümü")
        print("- SettledCash: settle olmuş nakit")
        print("- CashBalance: para birimi bazlı mevcut nakit bakiyesi")

        print("=" * 70 + "\n")


def main() -> None:
    HOST = "127.0.0.1"
    PORT = 7496   # live için 7496, paper için 7497
    CLIENT_ID = 41

    app = IBKRAccountApp()
    app.connect(HOST, PORT, CLIENT_ID)

    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()

    if not app.connected_event.wait(timeout=10):
        app.disconnect()
        raise TimeoutError("Could not connect to TWS.")

    app.reqManagedAccts()
    time.sleep(2)

    summary_tags = ",".join(
        [
            "NetLiquidation",
            "TotalCashValue",
            "SettledCash",
            "AvailableFunds",
            "FullAvailableFunds",
            "BuyingPower",
            "ExcessLiquidity",
            "FullExcessLiquidity",
            "LookAheadAvailableFunds",
            "LookAheadExcessLiquidity",
            "MaintMarginReq",
            "InitMarginReq",
            "GrossPositionValue",
            "CashBalance",
            "$LEDGER",
            "$LEDGER:EUR",
            "$LEDGER:USD",
        ]
    )

    app.reqAccountSummary(9001, "All", summary_tags)

    if not app.account_ready_event.wait(timeout=15):
        print("[IBKR] Warning: account summary end not received in time.")

    time.sleep(2)
    print_account_report(app)

    app.cancelAccountSummary(9001)
    time.sleep(1)
    app.disconnect()
    print("[IBKR] disconnected.")


if __name__ == "__main__":
    main()