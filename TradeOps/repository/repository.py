from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


class Repository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def insert_user_ibkr_wallet_details(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        ACCOUNT_ID: Optional[str],
        AVAILABLE_FUNDS: Optional[float],
        NET_LIQUIDATION: Optional[float],
        TOTAL_CASH_VALUE: Optional[float],
        SETTLED_CASH: Optional[float],
        BUYING_POWER: Optional[float],
        EXCESS_LIQUIDITY: Optional[float],
        GROSS_POSITION_VALUE: Optional[float],
        CASH_BALANCE_BASE: Optional[float],
        CASH_BALANCE_EUR: Optional[float],
        CASH_BALANCE_USD: Optional[float],
        OPEN_POSITION_COUNT: int,
        OPEN_SYMBOLS: str,
        RAW_ACCOUNT_SUMMARY_JSON: Dict[str, Any],
        FETCHED_AT: datetime,
    ) -> None:
        sql = """
        INSERT INTO live.user_ibkr_wallet_details (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            ACCOUNT_ID,
            AVAILABLE_FUNDS,
            NET_LIQUIDATION,
            TOTAL_CASH_VALUE,
            SETTLED_CASH,
            BUYING_POWER,
            EXCESS_LIQUIDITY,
            GROSS_POSITION_VALUE,
            CASH_BALANCE_BASE,
            CASH_BALANCE_EUR,
            CASH_BALANCE_USD,
            OPEN_POSITION_COUNT,
            OPEN_SYMBOLS,
            RAW_ACCOUNT_SUMMARY_JSON,
            FETCHED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :ACCOUNT_ID,
            :AVAILABLE_FUNDS,
            :NET_LIQUIDATION,
            :TOTAL_CASH_VALUE,
            :SETTLED_CASH,
            :BUYING_POWER,
            :EXCESS_LIQUIDITY,
            :GROSS_POSITION_VALUE,
            :CASH_BALANCE_BASE,
            :CASH_BALANCE_EUR,
            :CASH_BALANCE_USD,
            :OPEN_POSITION_COUNT,
            :OPEN_SYMBOLS,
            CAST(:RAW_ACCOUNT_SUMMARY_JSON AS JSONB),
            :FETCHED_AT
        );
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "ACCOUNT_ID": ACCOUNT_ID,
            "AVAILABLE_FUNDS": AVAILABLE_FUNDS,
            "NET_LIQUIDATION": NET_LIQUIDATION,
            "TOTAL_CASH_VALUE": TOTAL_CASH_VALUE,
            "SETTLED_CASH": SETTLED_CASH,
            "BUYING_POWER": BUYING_POWER,
            "EXCESS_LIQUIDITY": EXCESS_LIQUIDITY,
            "GROSS_POSITION_VALUE": GROSS_POSITION_VALUE,
            "CASH_BALANCE_BASE": CASH_BALANCE_BASE,
            "CASH_BALANCE_EUR": CASH_BALANCE_EUR,
            "CASH_BALANCE_USD": CASH_BALANCE_USD,
            "OPEN_POSITION_COUNT": OPEN_POSITION_COUNT,
            "OPEN_SYMBOLS": OPEN_SYMBOLS,
            "RAW_ACCOUNT_SUMMARY_JSON": json.dumps(RAW_ACCOUNT_SUMMARY_JSON),
            "FETCHED_AT": FETCHED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def insert_user_order_details(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        ACCOUNT_ID: Optional[str],
        ORDER_ID: Optional[int],
        PERM_ID: Optional[int],
        CLIENT_ID: Optional[int],
        PARENT_ID: Optional[int],
        SYMBOL: Optional[str],
        LOCAL_SYMBOL: Optional[str],
        SEC_TYPE: Optional[str],
        EXCHANGE: Optional[str],
        PRIMARY_EXCHANGE: Optional[str],
        CURRENCY: Optional[str],
        ACTION: Optional[str],
        TOTAL_QUANTITY: Optional[float],
        ORDER_TYPE: Optional[str],
        LMT_PRICE: Optional[float],
        AUX_PRICE: Optional[float],
        TIF: Optional[str],
        OUTSIDE_RTH: Optional[bool],
        TRANSMIT: Optional[bool],
        ORDER_REF: Optional[str],
        ORDER_STATUS: Optional[str],
        FILLED: Optional[float],
        REMAINING: Optional[float],
        AVG_FILL_PRICE: Optional[float],
        LAST_FILL_PRICE: Optional[float],
        WHY_HELD: Optional[str],
        MKT_CAP_PRICE: Optional[float],
        RAW_ORDER_JSON: Dict[str, Any],
        FETCHED_AT: datetime,
    ) -> None:
        sql = """
        INSERT INTO live.user_order_details (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            ACCOUNT_ID,
            ORDER_ID,
            PERM_ID,
            CLIENT_ID,
            PARENT_ID,
            SYMBOL,
            LOCAL_SYMBOL,
            SEC_TYPE,
            EXCHANGE,
            PRIMARY_EXCHANGE,
            CURRENCY,
            ACTION,
            TOTAL_QUANTITY,
            ORDER_TYPE,
            LMT_PRICE,
            AUX_PRICE,
            TIF,
            OUTSIDE_RTH,
            TRANSMIT,
            ORDER_REF,
            ORDER_STATUS,
            FILLED,
            REMAINING,
            AVG_FILL_PRICE,
            LAST_FILL_PRICE,
            WHY_HELD,
            MKT_CAP_PRICE,
            RAW_ORDER_JSON,
            FETCHED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :ACCOUNT_ID,
            :ORDER_ID,
            :PERM_ID,
            :CLIENT_ID,
            :PARENT_ID,
            :SYMBOL,
            :LOCAL_SYMBOL,
            :SEC_TYPE,
            :EXCHANGE,
            :PRIMARY_EXCHANGE,
            :CURRENCY,
            :ACTION,
            :TOTAL_QUANTITY,
            :ORDER_TYPE,
            :LMT_PRICE,
            :AUX_PRICE,
            :TIF,
            :OUTSIDE_RTH,
            :TRANSMIT,
            :ORDER_REF,
            :ORDER_STATUS,
            :FILLED,
            :REMAINING,
            :AVG_FILL_PRICE,
            :LAST_FILL_PRICE,
            :WHY_HELD,
            :MKT_CAP_PRICE,
            CAST(:RAW_ORDER_JSON AS JSONB),
            :FETCHED_AT
        );
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "ACCOUNT_ID": ACCOUNT_ID,
            "ORDER_ID": ORDER_ID,
            "PERM_ID": PERM_ID,
            "CLIENT_ID": CLIENT_ID,
            "PARENT_ID": PARENT_ID,
            "SYMBOL": SYMBOL,
            "LOCAL_SYMBOL": LOCAL_SYMBOL,
            "SEC_TYPE": SEC_TYPE,
            "EXCHANGE": EXCHANGE,
            "PRIMARY_EXCHANGE": PRIMARY_EXCHANGE,
            "CURRENCY": CURRENCY,
            "ACTION": ACTION,
            "TOTAL_QUANTITY": TOTAL_QUANTITY,
            "ORDER_TYPE": ORDER_TYPE,
            "LMT_PRICE": LMT_PRICE,
            "AUX_PRICE": AUX_PRICE,
            "TIF": TIF,
            "OUTSIDE_RTH": OUTSIDE_RTH,
            "TRANSMIT": TRANSMIT,
            "ORDER_REF": ORDER_REF,
            "ORDER_STATUS": ORDER_STATUS,
            "FILLED": FILLED,
            "REMAINING": REMAINING,
            "AVG_FILL_PRICE": AVG_FILL_PRICE,
            "LAST_FILL_PRICE": LAST_FILL_PRICE,
            "WHY_HELD": WHY_HELD,
            "MKT_CAP_PRICE": MKT_CAP_PRICE,
            "RAW_ORDER_JSON": json.dumps(RAW_ORDER_JSON),
            "FETCHED_AT": FETCHED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def insert_user_open_position_details(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        ACCOUNT_ID: Optional[str],
        SYMBOL: Optional[str],
        LOCAL_SYMBOL: Optional[str],
        SEC_TYPE: Optional[str],
        EXCHANGE: Optional[str],
        PRIMARY_EXCHANGE: Optional[str],
        CURRENCY: Optional[str],
        POSITION_QTY: Optional[float],
        AVG_COST: Optional[float],
        MARKET_PRICE: Optional[float],
        MARKET_VALUE: Optional[float],
        UNREALIZED_PNL: Optional[float],
        REALIZED_PNL: Optional[float],
        RAW_POSITION_JSON: Dict[str, Any],
        FETCHED_AT: datetime,
    ) -> None:
        sql = """
        INSERT INTO live.user_open_position_details (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            ACCOUNT_ID,
            SYMBOL,
            LOCAL_SYMBOL,
            SEC_TYPE,
            EXCHANGE,
            PRIMARY_EXCHANGE,
            CURRENCY,
            POSITION_QTY,
            AVG_COST,
            MARKET_PRICE,
            MARKET_VALUE,
            UNREALIZED_PNL,
            REALIZED_PNL,
            RAW_POSITION_JSON,
            FETCHED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :ACCOUNT_ID,
            :SYMBOL,
            :LOCAL_SYMBOL,
            :SEC_TYPE,
            :EXCHANGE,
            :PRIMARY_EXCHANGE,
            :CURRENCY,
            :POSITION_QTY,
            :AVG_COST,
            :MARKET_PRICE,
            :MARKET_VALUE,
            :UNREALIZED_PNL,
            :REALIZED_PNL,
            CAST(:RAW_POSITION_JSON AS JSONB),
            :FETCHED_AT
        );
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "ACCOUNT_ID": ACCOUNT_ID,
            "SYMBOL": SYMBOL,
            "LOCAL_SYMBOL": LOCAL_SYMBOL,
            "SEC_TYPE": SEC_TYPE,
            "EXCHANGE": EXCHANGE,
            "PRIMARY_EXCHANGE": PRIMARY_EXCHANGE,
            "CURRENCY": CURRENCY,
            "POSITION_QTY": POSITION_QTY,
            "AVG_COST": AVG_COST,
            "MARKET_PRICE": MARKET_PRICE,
            "MARKET_VALUE": MARKET_VALUE,
            "UNREALIZED_PNL": UNREALIZED_PNL,
            "REALIZED_PNL": REALIZED_PNL,
            "RAW_POSITION_JSON": json.dumps(RAW_POSITION_JSON),
            "FETCHED_AT": FETCHED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def insert_app_log(
        self,
        USERNAME: str,
        DEVICE_ID: str | None,
        IBKR_MODE: str | None,
        PIPELINE_NAME: str | None,
        EXCHANGE: str | None,
        SYMBOL: str | None,
        EVENT_TYPE: str,
        EVENT_STATUS: str,
        MESSAGE: str,
        DETAILS_JSON: dict | None,
        CREATED_AT: datetime,
    ) -> None:
        sql = """
        INSERT INTO live.app_logs (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            PIPELINE_NAME,
            EXCHANGE,
            SYMBOL,
            EVENT_TYPE,
            EVENT_STATUS,
            MESSAGE,
            DETAILS_JSON,
            CREATED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :PIPELINE_NAME,
            :EXCHANGE,
            :SYMBOL,
            :EVENT_TYPE,
            :EVENT_STATUS,
            :MESSAGE,
            CAST(:DETAILS_JSON AS JSONB),
            :CREATED_AT
        );
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "PIPELINE_NAME": PIPELINE_NAME,
            "EXCHANGE": EXCHANGE,
            "SYMBOL": SYMBOL,
            "EVENT_TYPE": EVENT_TYPE,
            "EVENT_STATUS": EVENT_STATUS,
            "MESSAGE": MESSAGE,
            "DETAILS_JSON": json.dumps(DETAILS_JSON or {}),
            "CREATED_AT": CREATED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def upsert_user_actual_wallet(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        ACCOUNT_ID: Optional[str],
        AVAILABLE_FUNDS: Optional[float],
        NET_LIQUIDATION: Optional[float],
        TOTAL_CASH_VALUE: Optional[float],
        SETTLED_CASH: Optional[float],
        BUYING_POWER: Optional[float],
        EXCESS_LIQUIDITY: Optional[float],
        GROSS_POSITION_VALUE: Optional[float],
        CASH_BALANCE_BASE: Optional[float],
        CASH_BALANCE_EUR: Optional[float],
        CASH_BALANCE_USD: Optional[float],
        OPEN_POSITION_COUNT: int,
        OPEN_SYMBOLS: str,
        RAW_ACCOUNT_SUMMARY_JSON: Dict[str, Any],
        FETCHED_AT: datetime,
    ) -> None:
        sql = """
        INSERT INTO live.user_actual_wallet (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            ACCOUNT_ID,
            AVAILABLE_FUNDS,
            NET_LIQUIDATION,
            TOTAL_CASH_VALUE,
            SETTLED_CASH,
            BUYING_POWER,
            EXCESS_LIQUIDITY,
            GROSS_POSITION_VALUE,
            CASH_BALANCE_BASE,
            CASH_BALANCE_EUR,
            CASH_BALANCE_USD,
            OPEN_POSITION_COUNT,
            OPEN_SYMBOLS,
            RAW_ACCOUNT_SUMMARY_JSON,
            FETCHED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :ACCOUNT_ID,
            :AVAILABLE_FUNDS,
            :NET_LIQUIDATION,
            :TOTAL_CASH_VALUE,
            :SETTLED_CASH,
            :BUYING_POWER,
            :EXCESS_LIQUIDITY,
            :GROSS_POSITION_VALUE,
            :CASH_BALANCE_BASE,
            :CASH_BALANCE_EUR,
            :CASH_BALANCE_USD,
            :OPEN_POSITION_COUNT,
            :OPEN_SYMBOLS,
            CAST(:RAW_ACCOUNT_SUMMARY_JSON AS JSONB),
            :FETCHED_AT
        )
        ON CONFLICT (USERNAME)
        DO UPDATE SET
            DEVICE_ID = EXCLUDED.DEVICE_ID,
            IBKR_MODE = EXCLUDED.IBKR_MODE,
            ACCOUNT_ID = EXCLUDED.ACCOUNT_ID,
            AVAILABLE_FUNDS = EXCLUDED.AVAILABLE_FUNDS,
            NET_LIQUIDATION = EXCLUDED.NET_LIQUIDATION,
            TOTAL_CASH_VALUE = EXCLUDED.TOTAL_CASH_VALUE,
            SETTLED_CASH = EXCLUDED.SETTLED_CASH,
            BUYING_POWER = EXCLUDED.BUYING_POWER,
            EXCESS_LIQUIDITY = EXCLUDED.EXCESS_LIQUIDITY,
            GROSS_POSITION_VALUE = EXCLUDED.GROSS_POSITION_VALUE,
            CASH_BALANCE_BASE = EXCLUDED.CASH_BALANCE_BASE,
            CASH_BALANCE_EUR = EXCLUDED.CASH_BALANCE_EUR,
            CASH_BALANCE_USD = EXCLUDED.CASH_BALANCE_USD,
            OPEN_POSITION_COUNT = EXCLUDED.OPEN_POSITION_COUNT,
            OPEN_SYMBOLS = EXCLUDED.OPEN_SYMBOLS,
            RAW_ACCOUNT_SUMMARY_JSON = EXCLUDED.RAW_ACCOUNT_SUMMARY_JSON,
            FETCHED_AT = EXCLUDED.FETCHED_AT;
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "ACCOUNT_ID": ACCOUNT_ID,
            "AVAILABLE_FUNDS": AVAILABLE_FUNDS,
            "NET_LIQUIDATION": NET_LIQUIDATION,
            "TOTAL_CASH_VALUE": TOTAL_CASH_VALUE,
            "SETTLED_CASH": SETTLED_CASH,
            "BUYING_POWER": BUYING_POWER,
            "EXCESS_LIQUIDITY": EXCESS_LIQUIDITY,
            "GROSS_POSITION_VALUE": GROSS_POSITION_VALUE,
            "CASH_BALANCE_BASE": CASH_BALANCE_BASE,
            "CASH_BALANCE_EUR": CASH_BALANCE_EUR,
            "CASH_BALANCE_USD": CASH_BALANCE_USD,
            "OPEN_POSITION_COUNT": OPEN_POSITION_COUNT,
            "OPEN_SYMBOLS": OPEN_SYMBOLS,
            "RAW_ACCOUNT_SUMMARY_JSON": json.dumps(RAW_ACCOUNT_SUMMARY_JSON),
            "FETCHED_AT": FETCHED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def get_user_actual_wallet(self, USERNAME: str) -> dict | None:
        sql = """
        SELECT
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            ACCOUNT_ID,
            AVAILABLE_FUNDS,
            NET_LIQUIDATION,
            TOTAL_CASH_VALUE,
            SETTLED_CASH,
            BUYING_POWER,
            EXCESS_LIQUIDITY,
            GROSS_POSITION_VALUE,
            CASH_BALANCE_BASE,
            CASH_BALANCE_EUR,
            CASH_BALANCE_USD,
            OPEN_POSITION_COUNT,
            OPEN_SYMBOLS,
            RAW_ACCOUNT_SUMMARY_JSON,
            FETCHED_AT
        FROM live.user_actual_wallet
        WHERE USERNAME = :USERNAME
        LIMIT 1;
        """

        with self.engine.begin() as conn:
            row = conn.execute(text(sql), {"USERNAME": USERNAME}).mappings().first()
            if not row:
                return None

            row_dict = dict(row)
            return {str(key).upper(): value for key, value in row_dict.items()}

    def get_latest_open_position_count_by_exchange(
        self,
        USERNAME: str,
        EXCHANGE: str,
    ) -> int:
        sql = """
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT DISTINCT SYMBOL
            FROM live.user_open_position_details
            WHERE USERNAME = :USERNAME
              AND EXCHANGE = :EXCHANGE
              AND FETCHED_AT = (
                  SELECT MAX(FETCHED_AT)
                  FROM live.user_open_position_details
                  WHERE USERNAME = :USERNAME
              )
        ) t;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "EXCHANGE": EXCHANGE,
                },
            ).mappings().first()

            return int(row["cnt"]) if row and row["cnt"] is not None else 0

    def get_today_buy_count_used(
        self,
        USERNAME: str,
        TRADE_DATE: str,
    ) -> int:
        sql = """
        SELECT COUNT(*) AS cnt
        FROM live.user_order_details
        WHERE USERNAME = :USERNAME
          AND ACTION = 'BUY'
          AND ORDER_STATUS IN ('Submitted', 'PreSubmitted', 'Filled')
          AND DATE(FETCHED_AT) = :TRADE_DATE;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "TRADE_DATE": TRADE_DATE,
                },
            ).mappings().first()

            return int(row["cnt"]) if row and row["cnt"] is not None else 0

    def upsert_buy_limits(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        TRADE_DATE,
        EXCHANGE: str,
        TOTAL_MAX_OPEN_POSITIONS: int,
        MAX_DAILY_TRADE_COUNT: int,
        EXCHANGE_MAX_OPEN_POSITIONS: int,
        CURRENT_OPEN_POSITION_COUNT: int,
        REMAINING_OPEN_POSITION_SLOTS: int,
        TODAY_BUY_COUNT_USED: int,
        TODAY_BUY_COUNT_REMAINING: int,
        AVAILABLE_FUNDS: float | None,
        ALLOCATED_BUDGET_PCT: float,
        ALLOCATED_BUDGET_AMOUNT: float | None,
        SLOT_BUDGET_AMOUNT: float | None,
        PLANNED_BUY_COUNT: int,
        IS_ENABLED: bool,
        PREPARED_AT,
        UPDATED_AT,
    ) -> None:
        sql = """
        INSERT INTO live.buy_limits (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            TRADE_DATE,
            EXCHANGE,
            TOTAL_MAX_OPEN_POSITIONS,
            MAX_DAILY_TRADE_COUNT,
            EXCHANGE_MAX_OPEN_POSITIONS,
            CURRENT_OPEN_POSITION_COUNT,
            REMAINING_OPEN_POSITION_SLOTS,
            TODAY_BUY_COUNT_USED,
            TODAY_BUY_COUNT_REMAINING,
            AVAILABLE_FUNDS,
            ALLOCATED_BUDGET_PCT,
            ALLOCATED_BUDGET_AMOUNT,
            SLOT_BUDGET_AMOUNT,
            PLANNED_BUY_COUNT,
            IS_ENABLED,
            PREPARED_AT,
            UPDATED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :TRADE_DATE,
            :EXCHANGE,
            :TOTAL_MAX_OPEN_POSITIONS,
            :MAX_DAILY_TRADE_COUNT,
            :EXCHANGE_MAX_OPEN_POSITIONS,
            :CURRENT_OPEN_POSITION_COUNT,
            :REMAINING_OPEN_POSITION_SLOTS,
            :TODAY_BUY_COUNT_USED,
            :TODAY_BUY_COUNT_REMAINING,
            :AVAILABLE_FUNDS,
            :ALLOCATED_BUDGET_PCT,
            :ALLOCATED_BUDGET_AMOUNT,
            :SLOT_BUDGET_AMOUNT,
            :PLANNED_BUY_COUNT,
            :IS_ENABLED,
            :PREPARED_AT,
            :UPDATED_AT
        )
        ON CONFLICT (USERNAME, TRADE_DATE, EXCHANGE)
        DO UPDATE SET
            DEVICE_ID = EXCLUDED.DEVICE_ID,
            IBKR_MODE = EXCLUDED.IBKR_MODE,
            TOTAL_MAX_OPEN_POSITIONS = EXCLUDED.TOTAL_MAX_OPEN_POSITIONS,
            MAX_DAILY_TRADE_COUNT = EXCLUDED.MAX_DAILY_TRADE_COUNT,
            EXCHANGE_MAX_OPEN_POSITIONS = EXCLUDED.EXCHANGE_MAX_OPEN_POSITIONS,
            CURRENT_OPEN_POSITION_COUNT = EXCLUDED.CURRENT_OPEN_POSITION_COUNT,
            REMAINING_OPEN_POSITION_SLOTS = EXCLUDED.REMAINING_OPEN_POSITION_SLOTS,
            TODAY_BUY_COUNT_USED = EXCLUDED.TODAY_BUY_COUNT_USED,
            TODAY_BUY_COUNT_REMAINING = EXCLUDED.TODAY_BUY_COUNT_REMAINING,
            AVAILABLE_FUNDS = EXCLUDED.AVAILABLE_FUNDS,
            ALLOCATED_BUDGET_PCT = EXCLUDED.ALLOCATED_BUDGET_PCT,
            ALLOCATED_BUDGET_AMOUNT = EXCLUDED.ALLOCATED_BUDGET_AMOUNT,
            SLOT_BUDGET_AMOUNT = EXCLUDED.SLOT_BUDGET_AMOUNT,
            PLANNED_BUY_COUNT = EXCLUDED.PLANNED_BUY_COUNT,
            IS_ENABLED = EXCLUDED.IS_ENABLED,
            PREPARED_AT = EXCLUDED.PREPARED_AT,
            UPDATED_AT = EXCLUDED.UPDATED_AT;
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "TRADE_DATE": TRADE_DATE,
            "EXCHANGE": EXCHANGE,
            "TOTAL_MAX_OPEN_POSITIONS": TOTAL_MAX_OPEN_POSITIONS,
            "MAX_DAILY_TRADE_COUNT": MAX_DAILY_TRADE_COUNT,
            "EXCHANGE_MAX_OPEN_POSITIONS": EXCHANGE_MAX_OPEN_POSITIONS,
            "CURRENT_OPEN_POSITION_COUNT": CURRENT_OPEN_POSITION_COUNT,
            "REMAINING_OPEN_POSITION_SLOTS": REMAINING_OPEN_POSITION_SLOTS,
            "TODAY_BUY_COUNT_USED": TODAY_BUY_COUNT_USED,
            "TODAY_BUY_COUNT_REMAINING": TODAY_BUY_COUNT_REMAINING,
            "AVAILABLE_FUNDS": AVAILABLE_FUNDS,
            "ALLOCATED_BUDGET_PCT": ALLOCATED_BUDGET_PCT,
            "ALLOCATED_BUDGET_AMOUNT": ALLOCATED_BUDGET_AMOUNT,
            "SLOT_BUDGET_AMOUNT": SLOT_BUDGET_AMOUNT,
            "PLANNED_BUY_COUNT": PLANNED_BUY_COUNT,
            "IS_ENABLED": IS_ENABLED,
            "PREPARED_AT": PREPARED_AT,
            "UPDATED_AT": UPDATED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)
    

    def get_latest_signal_date_for_exchange(
        self,
        SOURCE_EXCHANGE: str,
    ):
        sql = """
        SELECT MAX("DATE") AS latest_date
        FROM live.daily_buy_signals_all
        WHERE "EXCHANGE" = :SOURCE_EXCHANGE;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "SOURCE_EXCHANGE": SOURCE_EXCHANGE,
                },
            ).mappings().first()

            if not row or row["latest_date"] is None:
                return None

            return row["latest_date"]

    def get_daily_buy_signals_for_exchange_and_date(
        self,
        SOURCE_EXCHANGE: str,
        SIGNAL_DATE,
    ) -> list[dict]:
        sql = """
        SELECT *
        FROM live.daily_buy_signals_all
        WHERE "EXCHANGE" = :SOURCE_EXCHANGE
          AND "DATE" = :SIGNAL_DATE
        ORDER BY "RANK" ASC;
        """

        with self.engine.begin() as conn:
            rows = conn.execute(
                text(sql),
                {
                    "SOURCE_EXCHANGE": SOURCE_EXCHANGE,
                    "SIGNAL_DATE": SIGNAL_DATE,
                },
            ).mappings().all()

            result = []
            for row in rows:
                row_dict = dict(row)
                result.append({str(key).upper(): value for key, value in row_dict.items()})
            return result
    
    def insert_user_trade_log(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        EXCHANGE: str,
        SYMBOL: str,
        ACTION: str,
        QUANTITY: float,
        PRICE: float | None,
        TOTAL_AMOUNT: float | None,
        TRADE_TIME,
        TRADE_SOURCE: str,
        DETECTION_SOURCE: str,
        ORDER_ID: str | None,
        CLIENT_ORDER_ID: str | None,
        NOTES: str | None,
    ) -> None:
        sql = """
        INSERT INTO live.user_trade_log (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            EXCHANGE,
            SYMBOL,
            ACTION,
            QUANTITY,
            PRICE,
            TOTAL_AMOUNT,
            TRADE_TIME,
            TRADE_SOURCE,
            DETECTION_SOURCE,
            ORDER_ID,
            CLIENT_ORDER_ID,
            NOTES
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :EXCHANGE,
            :SYMBOL,
            :ACTION,
            :QUANTITY,
            :PRICE,
            :TOTAL_AMOUNT,
            :TRADE_TIME,
            :TRADE_SOURCE,
            :DETECTION_SOURCE,
            :ORDER_ID,
            :CLIENT_ORDER_ID,
            :NOTES
        );
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "EXCHANGE": EXCHANGE,
            "SYMBOL": SYMBOL,
            "ACTION": ACTION,
            "QUANTITY": QUANTITY,
            "PRICE": PRICE,
            "TOTAL_AMOUNT": TOTAL_AMOUNT,
            "TRADE_TIME": TRADE_TIME,
            "TRADE_SOURCE": TRADE_SOURCE,
            "DETECTION_SOURCE": DETECTION_SOURCE,
            "ORDER_ID": ORDER_ID,
            "CLIENT_ORDER_ID": CLIENT_ORDER_ID,
            "NOTES": NOTES,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def get_user_position_runtime(
        self,
        USERNAME: str,
        EXCHANGE: str,
        SYMBOL: str,
    ) -> dict | None:
        sql = """
        SELECT
            ID,
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            EXCHANGE,
            SYMBOL,
            IS_OPEN,
            ENTRY_DATE,
            LAST_SIGNAL_DATE,
            HOLDING_DAYS_USED,
            REMAINING_HOLDING_DAYS,
            MAX_HOLDING_DAY,
            LAST_PRICE,
            LAST_AGING_DATE,
            LAST_UPDATED_AT,
            CREATED_AT
        FROM live.user_position_runtime
        WHERE USERNAME = :USERNAME
          AND EXCHANGE = :EXCHANGE
          AND SYMBOL = :SYMBOL
        LIMIT 1;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "EXCHANGE": EXCHANGE,
                    "SYMBOL": SYMBOL,
                },
            ).mappings().first()

            if not row:
                return None

            row_dict = dict(row)
            return {str(key).upper(): value for key, value in row_dict.items()}

    def upsert_user_position_runtime(
        self,
        USERNAME: str,
        DEVICE_ID: str,
        IBKR_MODE: str,
        EXCHANGE: str,
        SYMBOL: str,
        IS_OPEN: bool,
        ENTRY_DATE,
        LAST_SIGNAL_DATE,
        HOLDING_DAYS_USED: int,
        REMAINING_HOLDING_DAYS: int,
        MAX_HOLDING_DAY: int,
        LAST_PRICE: float | None,
        LAST_AGING_DATE,
        LAST_UPDATED_AT,
    ) -> None:
        sql = """
        INSERT INTO live.user_position_runtime (
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            EXCHANGE,
            SYMBOL,
            IS_OPEN,
            ENTRY_DATE,
            LAST_SIGNAL_DATE,
            HOLDING_DAYS_USED,
            REMAINING_HOLDING_DAYS,
            MAX_HOLDING_DAY,
            LAST_PRICE,
            LAST_AGING_DATE,
            LAST_UPDATED_AT
        )
        VALUES (
            :USERNAME,
            :DEVICE_ID,
            :IBKR_MODE,
            :EXCHANGE,
            :SYMBOL,
            :IS_OPEN,
            :ENTRY_DATE,
            :LAST_SIGNAL_DATE,
            :HOLDING_DAYS_USED,
            :REMAINING_HOLDING_DAYS,
            :MAX_HOLDING_DAY,
            :LAST_PRICE,
            :LAST_AGING_DATE,
            :LAST_UPDATED_AT
        )
        ON CONFLICT (USERNAME, EXCHANGE, SYMBOL)
        DO UPDATE SET
            DEVICE_ID = EXCLUDED.DEVICE_ID,
            IBKR_MODE = EXCLUDED.IBKR_MODE,
            IS_OPEN = EXCLUDED.IS_OPEN,
            ENTRY_DATE = EXCLUDED.ENTRY_DATE,
            LAST_SIGNAL_DATE = EXCLUDED.LAST_SIGNAL_DATE,
            HOLDING_DAYS_USED = EXCLUDED.HOLDING_DAYS_USED,
            REMAINING_HOLDING_DAYS = EXCLUDED.REMAINING_HOLDING_DAYS,
            MAX_HOLDING_DAY = EXCLUDED.MAX_HOLDING_DAY,
            LAST_PRICE = EXCLUDED.LAST_PRICE,
            LAST_AGING_DATE = EXCLUDED.LAST_AGING_DATE,
            LAST_UPDATED_AT = EXCLUDED.LAST_UPDATED_AT;
        """

        params = {
            "USERNAME": USERNAME,
            "DEVICE_ID": DEVICE_ID,
            "IBKR_MODE": IBKR_MODE,
            "EXCHANGE": EXCHANGE,
            "SYMBOL": SYMBOL,
            "IS_OPEN": IS_OPEN,
            "ENTRY_DATE": ENTRY_DATE,
            "LAST_SIGNAL_DATE": LAST_SIGNAL_DATE,
            "HOLDING_DAYS_USED": HOLDING_DAYS_USED,
            "REMAINING_HOLDING_DAYS": REMAINING_HOLDING_DAYS,
            "MAX_HOLDING_DAY": MAX_HOLDING_DAY,
            "LAST_PRICE": LAST_PRICE,
            "LAST_AGING_DATE": LAST_AGING_DATE,
            "LAST_UPDATED_AT": LAST_UPDATED_AT,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def close_user_position_runtime(
        self,
        USERNAME: str,
        EXCHANGE: str,
        SYMBOL: str,
        LAST_UPDATED_AT,
        LAST_PRICE: float | None = None,
    ) -> None:
        sql = """
        UPDATE live.user_position_runtime
        SET
            IS_OPEN = FALSE,
            LAST_PRICE = COALESCE(:LAST_PRICE, LAST_PRICE),
            LAST_UPDATED_AT = :LAST_UPDATED_AT
        WHERE USERNAME = :USERNAME
          AND EXCHANGE = :EXCHANGE
          AND SYMBOL = :SYMBOL;
        """

        params = {
            "USERNAME": USERNAME,
            "EXCHANGE": EXCHANGE,
            "SYMBOL": SYMBOL,
            "LAST_UPDATED_AT": LAST_UPDATED_AT,
            "LAST_PRICE": LAST_PRICE,
        }

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def get_buy_limits(
        self,
        USERNAME: str,
        TRADE_DATE,
        EXCHANGE: str,
    ) -> dict | None:
        sql = """
        SELECT
            ID,
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            TRADE_DATE,
            EXCHANGE,
            TOTAL_MAX_OPEN_POSITIONS,
            MAX_DAILY_TRADE_COUNT,
            EXCHANGE_MAX_OPEN_POSITIONS,
            CURRENT_OPEN_POSITION_COUNT,
            REMAINING_OPEN_POSITION_SLOTS,
            TODAY_BUY_COUNT_USED,
            TODAY_BUY_COUNT_REMAINING,
            AVAILABLE_FUNDS,
            ALLOCATED_BUDGET_PCT,
            ALLOCATED_BUDGET_AMOUNT,
            SLOT_BUDGET_AMOUNT,
            PLANNED_BUY_COUNT,
            IS_ENABLED,
            PREPARED_AT,
            UPDATED_AT
        FROM live.buy_limits
        WHERE USERNAME = :USERNAME
          AND TRADE_DATE = :TRADE_DATE
          AND EXCHANGE = :EXCHANGE
        LIMIT 1;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "TRADE_DATE": TRADE_DATE,
                    "EXCHANGE": EXCHANGE,
                },
            ).mappings().first()

            if not row:
                return None

            row_dict = dict(row)
            return {str(key).upper(): value for key, value in row_dict.items()}

    def exists_same_day_trade_log(
        self,
        USERNAME: str,
        EXCHANGE: str,
        SYMBOL: str,
        ACTION: str,
        TRADE_DATE: str,
    ) -> bool:
        sql = """
        SELECT COUNT(*) AS cnt
        FROM live.user_trade_log
        WHERE USERNAME = :USERNAME
          AND EXCHANGE = :EXCHANGE
          AND SYMBOL = :SYMBOL
          AND ACTION = :ACTION
          AND DATE(TRADE_TIME) = :TRADE_DATE;
        """

        with self.engine.begin() as conn:
            row = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "EXCHANGE": EXCHANGE,
                    "SYMBOL": SYMBOL,
                    "ACTION": ACTION,
                    "TRADE_DATE": TRADE_DATE,
                },
            ).mappings().first()

            return bool(row and int(row["cnt"]) > 0)
        
    def get_open_position_runtime_rows(self, USERNAME: str) -> list[dict]:
        sql = """
        SELECT
            ID,
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            EXCHANGE,
            SYMBOL,
            IS_OPEN,
            ENTRY_DATE,
            LAST_SIGNAL_DATE,
            HOLDING_DAYS_USED,
            REMAINING_HOLDING_DAYS,
            MAX_HOLDING_DAY,
            LAST_PRICE,
            LAST_AGING_DATE,
            LAST_UPDATED_AT,
            CREATED_AT
        FROM live.user_position_runtime
        WHERE USERNAME = :USERNAME
          AND IS_OPEN = TRUE;
        """

        with self.engine.begin() as conn:
            rows = conn.execute(
                text(sql),
                {"USERNAME": USERNAME},
            ).mappings().all()

            result = []
            for row in rows:
                row_dict = dict(row)
                result.append({str(key).upper(): value for key, value in row_dict.items()})
            return result

    def get_open_position_runtime_rows_by_exchange(
        self,
        USERNAME: str,
        EXCHANGE: str,
    ) -> list[dict]:
        sql = """
        SELECT
            ID,
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            EXCHANGE,
            SYMBOL,
            IS_OPEN,
            ENTRY_DATE,
            LAST_SIGNAL_DATE,
            HOLDING_DAYS_USED,
            REMAINING_HOLDING_DAYS,
            MAX_HOLDING_DAY,
            LAST_PRICE,
            LAST_AGING_DATE,
            LAST_UPDATED_AT,
            CREATED_AT
        FROM live.user_position_runtime
        WHERE USERNAME = :USERNAME
          AND EXCHANGE = :EXCHANGE
          AND IS_OPEN = TRUE;
        """

        with self.engine.begin() as conn:
            rows = conn.execute(
                text(sql),
                {
                    "USERNAME": USERNAME,
                    "EXCHANGE": EXCHANGE,
                },
            ).mappings().all()

            result = []
            for row in rows:
                row_dict = dict(row)
                result.append({str(key).upper(): value for key, value in row_dict.items()})
            return result
        

    def get_latest_open_position_details(self, USERNAME: str) -> list[dict]:
        sql = """
        SELECT
            USERNAME,
            DEVICE_ID,
            IBKR_MODE,
            ACCOUNT_ID,
            SYMBOL,
            LOCAL_SYMBOL,
            SEC_TYPE,
            EXCHANGE,
            PRIMARY_EXCHANGE,
            CURRENCY,
            POSITION_QTY,
            AVG_COST,
            MARKET_PRICE,
            MARKET_VALUE,
            UNREALIZED_PNL,
            REALIZED_PNL,
            RAW_POSITION_JSON,
            FETCHED_AT
        FROM live.user_open_position_details
        WHERE USERNAME = :USERNAME
          AND FETCHED_AT = (
              SELECT MAX(FETCHED_AT)
              FROM live.user_open_position_details
              WHERE USERNAME = :USERNAME
          );
        """

        with self.engine.begin() as conn:
            rows = conn.execute(
                text(sql),
                {"USERNAME": USERNAME},
            ).mappings().all()

            result = []
            for row in rows:
                row_dict = dict(row)
                result.append({str(key).upper(): value for key, value in row_dict.items()})
            return result