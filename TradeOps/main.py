from __future__ import annotations

from config.device import get_runtime_device_id
from config.exchange_loader import get_enabled_exchanges
from config.settings import load_settings
from pipeline.account_snapshot_pipeline import run_account_snapshot_pipeline
from pipeline.bootstrap_pipeline import run_bootstrap_pipeline
from pipeline.buy_execution_pipeline import run_buy_execution_pipeline
from pipeline.buy_prepare_pipeline import run_buy_prepare_pipeline
from pipeline.end_of_day_reconcile_pipeline import run_end_of_day_reconcile_pipeline
from service.log_service import LogEvent, LogService
from service.scheduler_service import SchedulerService
from pipeline.forced_sell_pipeline import run_forced_sell_pipeline

def _run_manual_pipelines(settings) -> None:
    if not settings.MANUAL_TRIGGER_PIPELINES:
        print("[MAIN] No manual pipelines configured")
        return

    print("[MAIN] Running manual pipelines...")

    for pipeline_name in settings.MANUAL_TRIGGER_PIPELINES:
        if pipeline_name == "ACCOUNT_SNAPSHOT_POST_EU_BUY":
            result = run_account_snapshot_pipeline(
                settings=settings,
                snapshot_type="POST_EU_BUY",
            )
            print(
                f"[MAIN] MANUAL PIPELINE RESULT | "
                f"PIPELINE={pipeline_name} | "
                f"SUCCESS={result.SUCCESS} | "
                f"MESSAGE={result.MESSAGE}"
            )

        elif pipeline_name == "ACCOUNT_SNAPSHOT_POST_EU_CLOSE":
            result = run_account_snapshot_pipeline(
                settings=settings,
                snapshot_type="POST_EU_CLOSE",
            )
            print(
                f"[MAIN] MANUAL PIPELINE RESULT | "
                f"PIPELINE={pipeline_name} | "
                f"SUCCESS={result.SUCCESS} | "
                f"MESSAGE={result.MESSAGE}"
            )

        elif pipeline_name == "ACCOUNT_SNAPSHOT_EOD":
            result = run_account_snapshot_pipeline(
                settings=settings,
                snapshot_type="EOD",
            )
            print(
                f"[MAIN] MANUAL PIPELINE RESULT | "
                f"PIPELINE={pipeline_name} | "
                f"SUCCESS={result.SUCCESS} | "
                f"MESSAGE={result.MESSAGE}"
            )

        elif pipeline_name == "BUY_PREPARE":
            result = run_buy_prepare_pipeline(settings=settings)
            print(
                f"[MAIN] MANUAL PIPELINE RESULT | "
                f"PIPELINE={pipeline_name} | "
                f"SUCCESS={result.SUCCESS} | "
                f"MESSAGE={result.MESSAGE} | "
                f"TOTAL_PLANNED_BUY_COUNT={result.TOTAL_PLANNED_BUY_COUNT} | "
                f"EXCHANGE_PLANNED_SUMMARY={result.EXCHANGE_PLANNED_SUMMARY}"
            )

        elif pipeline_name == "BUY_EXECUTION":
            for exchange in get_enabled_exchanges():
                result = run_buy_execution_pipeline(
                    settings=settings,
                    exchange_code=exchange.EXCHANGE,
                )
                print(
                    f"[MAIN] MANUAL PIPELINE RESULT | "
                    f"PIPELINE={pipeline_name} | "
                    f"EXCHANGE={exchange.EXCHANGE} | "
                    f"SUCCESS={result.SUCCESS} | "
                    f"MESSAGE={result.MESSAGE} | "
                    f"SIGNAL_COUNT={result.SIGNAL_COUNT} | "
                    f"PLANNED_BUY_COUNT={result.PLANNED_BUY_COUNT} | "
                    f"EXECUTED_BUY_COUNT={result.EXECUTED_BUY_COUNT} | "
                    f"SKIPPED_SCOPE_COUNT={result.SKIPPED_SCOPE_COUNT} | "
                    f"SKIPPED_ALREADY_OPEN_COUNT={result.SKIPPED_ALREADY_OPEN_COUNT} | "
                    f"SKIPPED_SAME_DAY_COUNT={result.SKIPPED_SAME_DAY_COUNT} | "
                    f"SKIPPED_BUDGET_COUNT={result.SKIPPED_BUDGET_COUNT}"
                )

        elif pipeline_name == "END_OF_DAY_RECONCILE":
            result = run_end_of_day_reconcile_pipeline(settings=settings)
            print(
                f"[MAIN] MANUAL PIPELINE RESULT | "
                f"PIPELINE={pipeline_name} | "
                f"SUCCESS={result.SUCCESS} | "
                f"MESSAGE={result.MESSAGE} | "
                f"MANUAL_BUY_DETECTED_COUNT={result.MANUAL_BUY_DETECTED_COUNT} | "
                f"MANUAL_SELL_DETECTED_COUNT={result.MANUAL_SELL_DETECTED_COUNT} | "
                f"RUNTIME_CLOSED_COUNT={result.RUNTIME_CLOSED_COUNT} | "
                f"RUNTIME_OPENED_COUNT={result.RUNTIME_OPENED_COUNT}"
            )

        elif pipeline_name == "FORCED_SELL":
            for exchange in get_enabled_exchanges():
                result = run_forced_sell_pipeline(
                    settings=settings,
                    exchange_code=exchange.EXCHANGE,
                )
                print(
                    f"[MAIN] MANUAL PIPELINE RESULT | "
                    f"PIPELINE={pipeline_name} | "
                    f"EXCHANGE={exchange.EXCHANGE} | "
                    f"SUCCESS={result.SUCCESS} | "
                    f"MESSAGE={result.MESSAGE} | "
                    f"AGED_POSITION_COUNT={result.AGED_POSITION_COUNT} | "
                    f"ELIGIBLE_FORCED_SELL_COUNT={result.ELIGIBLE_FORCED_SELL_COUNT} | "
                    f"EXECUTED_FORCED_SELL_COUNT={result.EXECUTED_FORCED_SELL_COUNT}"
                )


def main() -> None:
    settings = load_settings()
    settings.DEVICE_ID = get_runtime_device_id(settings.USERNAME)
    log_service = LogService(settings)

    enabled_exchanges = get_enabled_exchanges()

    log_service.log(
        LogEvent(
            EVENT_TYPE="APP_BOOT",
            EVENT_STATUS="STARTED",
            MESSAGE="Application boot started.",
            DETAILS_JSON={},
        )
    )

    print("=" * 60)
    print("IBKR EXECUTION BOOT")
    print("=" * 60)
    print(f"USERNAME                 : {settings.USERNAME}")
    print(f"EMAIL                    : {settings.EMAIL}")
    print(f"APP_TIMEZONE             : {settings.APP_TIMEZONE}")
    print(f"IBKR_MODE                : {settings.IBKR_MODE}")
    print(f"IBKR_HOST                : {settings.IBKR_HOST}")
    print(f"IBKR_PORT                : {settings.IBKR_PORT}")
    print(f"IBKR_CLIENT_ID           : {settings.IBKR_CLIENT_ID}")
    print(f"TOTAL_MAX_OPEN_POSITIONS : {settings.TOTAL_MAX_OPEN_POSITIONS}")
    print(f"MAX_DAILY_TRADE_COUNT    : {settings.MAX_DAILY_TRADE_COUNT}")
    print(f"EXIT_MODE                : {settings.EXIT_MODE}")
    print(f"EXCHANGE_PRIORITY        : {settings.EXCHANGE_PRIORITY}")
    print(f"RUN_ON_SCHEDULE          : {settings.RUN_ON_SCHEDULE}")
    print(f"MANUAL_TRIGGER_PIPELINES : {settings.MANUAL_TRIGGER_PIPELINES}")
    print("-" * 60)
    print("ENABLED EXCHANGES")

    for exchange in enabled_exchanges:
        print(
            f"- {exchange.EXCHANGE} | "
            f"BUY_PREPARE={exchange.BUY_PREPARE_TIME} | "
            f"SIGNAL={exchange.SIGNAL_TIME} | "
            f"EOD={exchange.EOD_RECONCILE_TIME} | "
            f"FORCED_SELL={exchange.FORCED_SELL_TIME} | "
            f"MAX_OPEN_POSITIONS={exchange.MAX_OPEN_POSITIONS} | "
            f"BUDGET_PCT={exchange.BUDGET_PCT} | "
            f"CURRENCY={exchange.CURRENCY}"
        )

    print("-" * 60)
    print("ACCOUNT SNAPSHOT TIMES")
    print(f"POST_EU_BUY             : {settings.ACCOUNT_SNAPSHOT_POST_EU_BUY_TIME}")
    print(f"POST_EU_CLOSE           : {settings.ACCOUNT_SNAPSHOT_POST_EU_CLOSE_TIME}")
    print(f"EOD                     : {settings.ACCOUNT_SNAPSHOT_EOD_TIME}")

    bootstrap_result = run_bootstrap_pipeline(settings)

    print("=" * 60)
    print("BOOTSTRAP RESULT")
    print("=" * 60)
    print(f"SUCCESS                  : {bootstrap_result.SUCCESS}")
    print(f"MESSAGE                  : {bootstrap_result.MESSAGE}")
    print(f"FETCHED_AT               : {bootstrap_result.FETCHED_AT}")
    print(f"ACCOUNT_ID               : {bootstrap_result.ACCOUNT_ID}")
    print(f"OPEN_POSITION_COUNT      : {bootstrap_result.OPEN_POSITION_COUNT}")
    print(f"OPEN_SYMBOLS             : {bootstrap_result.OPEN_SYMBOLS}")
    print(f"ORDER_DETAIL_ROW_COUNT   : {bootstrap_result.ORDER_DETAIL_ROW_COUNT}")
    print(f"POSITION_DETAIL_ROW_COUNT: {bootstrap_result.POSITION_DETAIL_ROW_COUNT}")
    print("=" * 60)

    if not bootstrap_result.SUCCESS:
        log_service.log(
            LogEvent(
                EVENT_TYPE="APP_BOOT",
                EVENT_STATUS="FAILED",
                MESSAGE="Application boot failed.",
                DETAILS_JSON={
                    "BOOTSTRAP_MESSAGE": bootstrap_result.MESSAGE,
                },
            )
        )
        raise SystemExit(1)

    _run_manual_pipelines(settings)

    log_service.log(
        LogEvent(
            EVENT_TYPE="APP_BOOT",
            EVENT_STATUS="SUCCESS",
            MESSAGE="Application boot completed successfully.",
            DETAILS_JSON={
                "RUN_ON_SCHEDULE": settings.RUN_ON_SCHEDULE,
                "MANUAL_TRIGGER_PIPELINES": settings.MANUAL_TRIGGER_PIPELINES,
            },
        )
    )

    if settings.RUN_ON_SCHEDULE:
        print("[MAIN] Starting scheduler...")
        scheduler = SchedulerService(settings)
        scheduler.start()
    else:
        print("[MAIN] RUN_ON_SCHEDULE is false. Exiting after manual/test runs.")


if __name__ == "__main__":
    main()