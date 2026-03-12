from dotenv import load_dotenv

from app.infrastructure.database.connection import Database
from app.infrastructure.database.repository import PostgresRepository
from app.pipelines.indicators_pipeline import run_indicators_for_exchange, IndicatorsFlags


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

        # --- FRVP ---
        frvp=False,

        # --- Converted Daily (for EMA/RSI input) ---
        build_converted_daily=True, #should be true to generate sample data based on converted 1min data
        converted_daily_input_schema="silver",
        converted_daily_input_table="FRVP_USA_FOCUS_DATASET",
        converted_daily_input_interval="1min",
        converted_daily_output_schema="silver",
        converted_daily_output_table="usa_focus_2e_indicators_converted_daily",
        converted_daily_start_trading_days_back=150,


        # auto sample genarete on converted daily
        auto_sample_run = True, #use ema_exchange for exchange

        #ema usa flags
        ema_calc = True,
        ema_input_schema = 'silver',
        ema_input_table = 'usa_focus_2e_indicators_converted_daily',

        # VWAP flags
        build_vwap_focus = True,
        vwap_source_table = "usa_focus_2e_indicators_converted_daily",
  
    )

    run_indicators_for_exchange(repo, "USA", flags)


if __name__ == "__main__":
    main()