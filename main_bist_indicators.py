# main_bist_indicators.py

from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.indicators_pipeline import run_indicators_for_exchange, IndicatorsFlags


def main():
    load_dotenv()

    db = Database()
    db.print_connection_info()
    engine = db.connect()
    repo = PostgresRepository(engine)

    flags = IndicatorsFlags(
        #flags for FRVP/POC
        frvp=False,
        truncate_scope=True,
        periods=["2year", "1year", "6months", "4months"],
        cutt_off_date=None,

        #FLAGS: converting from min to daily for 2e indicator calc
        build_converted_daily=False,
        converted_daily_input_schema="silver",
        converted_daily_input_table="FRVP_BIST_FOCUS_DATASET",
        converted_daily_input_interval="daily",
        converted_daily_output_schema="silver",
        converted_daily_output_table="bist_focus_2e_indicators_converted_daily",
        converted_daily_start_trading_days_back=130,

        # Flags for EMA and other 2e indicators 
        ema_calc = False, #True calc is active False: skip ema calc
        ema_input_table = 'bist_focus_2e_indicators_converted_daily',

        # VWAP flags
        build_vwap_focus = False,
        vwap_source_table = "bist_focus_2e_indicators_converted_daily",

        # RSI flags
        build_rsi_focus = False,
        rsi_source_table='bist_focus_2e_indicators_converted_daily',

        # MFI FLAGS
        build_mfi_focus = True,
        mfi_source_table='bist_focus_2e_indicators_converted_daily',


        # bar status identification
        build_bar_status = False,
        bar_status_source_schema = "raw",
        bar_status_source_table = "bist_1min_archive"

    )
    

    run_indicators_for_exchange(repo, "BIST", flags)

if __name__ == "__main__":
    main()