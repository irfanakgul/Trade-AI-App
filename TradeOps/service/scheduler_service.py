from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from config.exchange_loader import ExchangeConfig, get_enabled_exchanges
from config.settings import AppSettings
from pipeline.account_snapshot_pipeline import run_account_snapshot_pipeline
from pipeline.buy_execution_pipeline import run_buy_execution_pipeline
from pipeline.buy_prepare_pipeline import run_buy_prepare_pipeline
from pipeline.end_of_day_reconcile_pipeline import run_end_of_day_reconcile_pipeline
from service.log_service import LogEvent, LogService
from pipeline.forced_sell_pipeline import run_forced_sell_pipeline

@dataclass
class SchedulerState:
    IS_RUNNING: bool = False
    LAST_TRIGGERED_KEYS: set[str] = field(default_factory=set)


class SchedulerService:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.state = SchedulerState()
        self.timezone = ZoneInfo(settings.APP_TIMEZONE)
        self.log_service = LogService(settings)

    def start(self) -> None:
        if self.state.IS_RUNNING:
            print("[SCHEDULER] Already running")
            return

        self.state.IS_RUNNING = True
        print("[SCHEDULER] STARTED")

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="STARTED",
                MESSAGE="Scheduler started.",
                DETAILS_JSON={},
            )
        )

        try:
            while self.state.IS_RUNNING:
                self._tick()
                time.sleep(15)
        except KeyboardInterrupt:
            print("[SCHEDULER] KeyboardInterrupt received")
        finally:
            self.state.IS_RUNNING = False

            self.log_service.log(
                LogEvent(
                    EVENT_TYPE="SCHEDULER",
                    EVENT_STATUS="SUCCESS",
                    MESSAGE="Scheduler stopped.",
                    DETAILS_JSON={},
                )
            )

            print("[SCHEDULER] STOPPED")

    def stop(self) -> None:
        self.state.IS_RUNNING = False
        print("[SCHEDULER] STOP REQUESTED")

    def _handle_forced_sell_trigger(
        self,
        exchange: ExchangeConfig,
        current_hhmm: str,
        current_date: str,
    ) -> None:
        trigger_time = exchange.FORCED_SELL_TIME
        if current_hhmm != trigger_time:
            return

        trigger_key = f"{current_date}|FORCED_SELL|{exchange.EXCHANGE}|{trigger_time}"
        if trigger_key in self.state.LAST_TRIGGERED_KEYS:
            return

        print(
            f"[SCHEDULER] FORCED SELL TRIGGERED | "
            f"EXCHANGE={exchange.EXCHANGE} | TIME={trigger_time}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="INFO",
                MESSAGE="Forced sell trigger fired.",
                PIPELINE_NAME="FORCED_SELL_PIPELINE",
                EXCHANGE=exchange.EXCHANGE,
                DETAILS_JSON={
                    "TRIGGER_TIME": trigger_time,
                    "CURRENT_DATE": current_date,
                },
            )
        )

        result = run_forced_sell_pipeline(
            settings=self.settings,
            exchange_code=exchange.EXCHANGE,
        )

        print(
            f"[SCHEDULER] FORCED SELL RESULT | "
            f"EXCHANGE={exchange.EXCHANGE} | SUCCESS={result.SUCCESS} | MESSAGE={result.MESSAGE}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="SUCCESS" if result.SUCCESS else "FAILED",
                MESSAGE=result.MESSAGE,
                PIPELINE_NAME="FORCED_SELL_PIPELINE",
                EXCHANGE=exchange.EXCHANGE,
                DETAILS_JSON={
                    "AGED_POSITION_COUNT": result.AGED_POSITION_COUNT,
                    "ELIGIBLE_FORCED_SELL_COUNT": result.ELIGIBLE_FORCED_SELL_COUNT,
                    "EXECUTED_FORCED_SELL_COUNT": result.EXECUTED_FORCED_SELL_COUNT,
                },
            )
        )

        self.state.LAST_TRIGGERED_KEYS.add(trigger_key)


    def _tick(self) -> None:
        now = datetime.now(self.timezone)
        current_hhmm = now.strftime("%H:%M")
        current_date = now.strftime("%Y-%m-%d")

        enabled_exchanges = get_enabled_exchanges()

        print(
            f"[SCHEDULER] TICK | NOW={now.isoformat()} | "
            f"ENABLED_EXCHANGES={','.join([x.EXCHANGE for x in enabled_exchanges])}"
        )

        self._handle_buy_prepare_global_trigger(
            enabled_exchanges=enabled_exchanges,
            current_hhmm=current_hhmm,
            current_date=current_date,
        )

        for exchange in enabled_exchanges:
            self._handle_buy_execution_trigger(
                exchange=exchange,
                current_hhmm=current_hhmm,
                current_date=current_date,
            )

            self._handle_forced_sell_trigger(
                exchange=exchange,
                current_hhmm=current_hhmm,
                current_date=current_date,
            )

        self._handle_account_snapshot_trigger(
            current_hhmm=current_hhmm,
            current_date=current_date,
            snapshot_type="POST_EU_BUY",
            trigger_time=self.settings.ACCOUNT_SNAPSHOT_POST_EU_BUY_TIME,
        )

        self._handle_account_snapshot_trigger(
            current_hhmm=current_hhmm,
            current_date=current_date,
            snapshot_type="POST_EU_CLOSE",
            trigger_time=self.settings.ACCOUNT_SNAPSHOT_POST_EU_CLOSE_TIME,
        )

        self._handle_account_snapshot_trigger(
            current_hhmm=current_hhmm,
            current_date=current_date,
            snapshot_type="EOD",
            trigger_time=self.settings.ACCOUNT_SNAPSHOT_EOD_TIME,
        )

        self._handle_end_of_day_reconcile_global_trigger(
            enabled_exchanges=enabled_exchanges,
            current_hhmm=current_hhmm,
            current_date=current_date,
        )

        self._cleanup_old_trigger_keys(current_date=current_date)

    def _handle_buy_prepare_global_trigger(
        self,
        enabled_exchanges: list[ExchangeConfig],
        current_hhmm: str,
        current_date: str,
    ) -> None:
        matching_exchanges = [
            exchange for exchange in enabled_exchanges
            if exchange.BUY_PREPARE_TIME == current_hhmm
        ]

        if not matching_exchanges:
            return

        trigger_key = f"{current_date}|BUY_PREPARE|GLOBAL|{current_hhmm}"
        if trigger_key in self.state.LAST_TRIGGERED_KEYS:
            return

        exchange_names = [exchange.EXCHANGE for exchange in matching_exchanges]

        print(
            f"[SCHEDULER] BUY PREPARE TRIGGERED | "
            f"EXCHANGES={','.join(exchange_names)} | TIME={current_hhmm}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="INFO",
                MESSAGE="Buy prepare trigger fired.",
                PIPELINE_NAME="BUY_PREPARE_PIPELINE",
                DETAILS_JSON={
                    "TRIGGER_TIME": current_hhmm,
                    "CURRENT_DATE": current_date,
                    "MATCHING_EXCHANGES": exchange_names,
                },
            )
        )

        result = run_buy_prepare_pipeline(settings=self.settings)

        print(
            f"[SCHEDULER] BUY PREPARE RESULT | "
            f"SUCCESS={result.SUCCESS} | MESSAGE={result.MESSAGE}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="SUCCESS" if result.SUCCESS else "FAILED",
                MESSAGE=result.MESSAGE,
                PIPELINE_NAME="BUY_PREPARE_PIPELINE",
                DETAILS_JSON={
                    "TRADE_DATE": result.TRADE_DATE,
                    "TOTAL_PLANNED_BUY_COUNT": result.TOTAL_PLANNED_BUY_COUNT,
                    "EXCHANGE_PLANNED_SUMMARY": result.EXCHANGE_PLANNED_SUMMARY,
                },
            )
        )

        self.state.LAST_TRIGGERED_KEYS.add(trigger_key)

    def _handle_buy_execution_trigger(
        self,
        exchange: ExchangeConfig,
        current_hhmm: str,
        current_date: str,
    ) -> None:
        trigger_time = exchange.SIGNAL_TIME
        if current_hhmm != trigger_time:
            return

        trigger_key = f"{current_date}|BUY_EXECUTION|{exchange.EXCHANGE}|{trigger_time}"
        if trigger_key in self.state.LAST_TRIGGERED_KEYS:
            return

        print(
            f"[SCHEDULER] BUY EXECUTION TRIGGERED | "
            f"EXCHANGE={exchange.EXCHANGE} | TIME={trigger_time}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="INFO",
                MESSAGE="Buy execution trigger fired.",
                PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                EXCHANGE=exchange.EXCHANGE,
                DETAILS_JSON={
                    "TRIGGER_TIME": trigger_time,
                    "CURRENT_DATE": current_date,
                },
            )
        )

        result = run_buy_execution_pipeline(
            settings=self.settings,
            exchange_code=exchange.EXCHANGE,
        )

        print(
            f"[SCHEDULER] BUY EXECUTION RESULT | "
            f"EXCHANGE={exchange.EXCHANGE} | SUCCESS={result.SUCCESS} | MESSAGE={result.MESSAGE}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="SUCCESS" if result.SUCCESS else "FAILED",
                MESSAGE=result.MESSAGE,
                PIPELINE_NAME="BUY_EXECUTION_PIPELINE",
                EXCHANGE=exchange.EXCHANGE,
                DETAILS_JSON={
                    "SIGNAL_COUNT": result.SIGNAL_COUNT,
                    "PLANNED_BUY_COUNT": result.PLANNED_BUY_COUNT,
                    "EXECUTED_BUY_COUNT": result.EXECUTED_BUY_COUNT,
                    "SKIPPED_SCOPE_COUNT": result.SKIPPED_SCOPE_COUNT,
                    "SKIPPED_ALREADY_OPEN_COUNT": result.SKIPPED_ALREADY_OPEN_COUNT,
                    "SKIPPED_SAME_DAY_COUNT": result.SKIPPED_SAME_DAY_COUNT,
                    "SKIPPED_BUDGET_COUNT": result.SKIPPED_BUDGET_COUNT,
                },
            )
        )

        self.state.LAST_TRIGGERED_KEYS.add(trigger_key)

    def _handle_account_snapshot_trigger(
        self,
        current_hhmm: str,
        current_date: str,
        snapshot_type: str,
        trigger_time: str,
    ) -> None:
        if current_hhmm != trigger_time:
            return

        trigger_key = f"{current_date}|ACCOUNT_SNAPSHOT|{snapshot_type}|{trigger_time}"
        if trigger_key in self.state.LAST_TRIGGERED_KEYS:
            return

        print(
            f"[SCHEDULER] ACCOUNT SNAPSHOT TRIGGERED | "
            f"SNAPSHOT_TYPE={snapshot_type} | TIME={trigger_time}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="INFO",
                MESSAGE="Account snapshot trigger fired.",
                PIPELINE_NAME="ACCOUNT_SNAPSHOT_PIPELINE",
                DETAILS_JSON={
                    "SNAPSHOT_TYPE": snapshot_type,
                    "TRIGGER_TIME": trigger_time,
                    "CURRENT_DATE": current_date,
                },
            )
        )

        result = run_account_snapshot_pipeline(
            settings=self.settings,
            snapshot_type=snapshot_type,
        )

        print(
            f"[SCHEDULER] ACCOUNT SNAPSHOT RESULT | "
            f"SNAPSHOT_TYPE={snapshot_type} | SUCCESS={result.SUCCESS} | MESSAGE={result.MESSAGE}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="SUCCESS" if result.SUCCESS else "FAILED",
                MESSAGE=result.MESSAGE,
                PIPELINE_NAME="ACCOUNT_SNAPSHOT_PIPELINE",
                DETAILS_JSON={
                    "SNAPSHOT_TYPE": result.SNAPSHOT_TYPE,
                    "FETCHED_AT": result.FETCHED_AT,
                    "OPEN_POSITION_COUNT": result.OPEN_POSITION_COUNT,
                    "OPEN_SYMBOLS": result.OPEN_SYMBOLS,
                    "ORDER_DETAIL_ROW_COUNT": result.ORDER_DETAIL_ROW_COUNT,
                    "POSITION_DETAIL_ROW_COUNT": result.POSITION_DETAIL_ROW_COUNT,
                },
            )
        )

        self.state.LAST_TRIGGERED_KEYS.add(trigger_key)

    def _handle_end_of_day_reconcile_global_trigger(
        self,
        enabled_exchanges: list[ExchangeConfig],
        current_hhmm: str,
        current_date: str,
    ) -> None:
        matching_exchanges = [
            exchange for exchange in enabled_exchanges
            if exchange.EOD_RECONCILE_TIME == current_hhmm
        ]

        if not matching_exchanges:
            return

        trigger_key = f"{current_date}|END_OF_DAY_RECONCILE|GLOBAL|{current_hhmm}"
        if trigger_key in self.state.LAST_TRIGGERED_KEYS:
            return

        exchange_names = [exchange.EXCHANGE for exchange in matching_exchanges]

        print(
            f"[SCHEDULER] END OF DAY RECONCILE TRIGGERED | "
            f"EXCHANGES={','.join(exchange_names)} | TIME={current_hhmm}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="INFO",
                MESSAGE="End of day reconcile trigger fired.",
                PIPELINE_NAME="END_OF_DAY_RECONCILE_PIPELINE",
                DETAILS_JSON={
                    "TRIGGER_TIME": current_hhmm,
                    "CURRENT_DATE": current_date,
                    "MATCHING_EXCHANGES": exchange_names,
                },
            )
        )

        result = run_end_of_day_reconcile_pipeline(settings=self.settings)

        print(
            f"[SCHEDULER] END OF DAY RECONCILE RESULT | "
            f"SUCCESS={result.SUCCESS} | MESSAGE={result.MESSAGE}"
        )

        self.log_service.log(
            LogEvent(
                EVENT_TYPE="SCHEDULER",
                EVENT_STATUS="SUCCESS" if result.SUCCESS else "FAILED",
                MESSAGE=result.MESSAGE,
                PIPELINE_NAME="END_OF_DAY_RECONCILE_PIPELINE",
                DETAILS_JSON={
                    "FETCHED_AT": result.FETCHED_AT,
                    "MANUAL_BUY_DETECTED_COUNT": result.MANUAL_BUY_DETECTED_COUNT,
                    "MANUAL_SELL_DETECTED_COUNT": result.MANUAL_SELL_DETECTED_COUNT,
                    "RUNTIME_CLOSED_COUNT": result.RUNTIME_CLOSED_COUNT,
                    "RUNTIME_OPENED_COUNT": result.RUNTIME_OPENED_COUNT,
                },
            )
        )

        self.state.LAST_TRIGGERED_KEYS.add(trigger_key)

    def _cleanup_old_trigger_keys(self, current_date: str) -> None:
        self.state.LAST_TRIGGERED_KEYS = {
            key for key in self.state.LAST_TRIGGERED_KEYS
            if key.startswith(current_date)
        }